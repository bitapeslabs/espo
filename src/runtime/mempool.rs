use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoTrace, extract_alkane_storage,
    prettyify_protobuf_trace_json,
};
use crate::config::{get_bitcoind_rpc_client, get_metashrew_rpc_url};
use crate::runtime::mdb::Mdb;
use crate::schemas::EspoOutpoint;
use anyhow::{Context, Result};
use bitcoin::block::Version as BlockVersion;
use bitcoin::blockdata::block::Header;
use bitcoin::blockdata::transaction::Version as TxVersion;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::{
    Address, Amount, Block, CompactTarget, Network, Sequence, Transaction, TxIn, TxMerkleNode,
    TxOut, Txid, Witness,
};
use bitcoincore_rpc::{Client as CoreClient, RpcApi};
use borsh::{BorshDeserialize, BorshSerialize, to_vec};
use futures::{StreamExt, stream};
use ordinals::{Artifact, Runestone};
use prost::Message;
use protorune_support::protostone::Protostone;
use protorune_support::proto::protorune;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// --- Tunables (edit as needed) ---
pub const MEMPOOL_POLL_SECS: u64 = 5;
pub const MEMPOOL_PREVIEW_BATCH_SIZE: usize = 10;
pub const MEMPOOL_PREVIEW_TX_CONCURRENCY: usize = 6;
pub const MEMPOOL_LOG_STEP: usize = 100;
pub const MEMPOOL_MAX_TXS: usize = 50_000;
pub const MEMPOOL_MIN_FEE_RATE_SATS_VBYTE: f64 = 0.5;
/// --- End tunables ---

#[derive(Clone, Debug)]
pub struct MempoolEntry {
    pub txid: Txid,
    pub tx: Transaction,
    pub traces: Option<Vec<EspoTrace>>,
    pub first_seen: u64,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
struct PersistedTrace {
    protobuf: Vec<u8>,
    outpoint: Vec<u8>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
struct PersistedMempoolTx {
    raw_tx: Vec<u8>,
    traces: Vec<PersistedTrace>,
    first_seen: u64,
    addresses: Vec<String>,
}

struct ProcessedEntry {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
    first_seen: u64,
    addresses: Vec<String>,
}

static MEMPOOL_MDB: OnceLock<Mdb> = OnceLock::new();

pub fn get_mempool_mdb() -> &'static Mdb {
    MEMPOOL_MDB.get_or_init(|| Mdb::from_db(crate::config::get_espo_db(), b"mempool:"))
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn protostones_for_tx(tx: &Transaction) -> Vec<Protostone> {
    match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => Protostone::from_runestone(&r).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn shadow_base(tx: &Transaction) -> u32 {
    tx.output.len() as u32 + 1
}

fn encode_outpoint_hex(txid: &Txid, vout: u32) -> String {
    let mut outpoint = protorune::Outpoint::default();
    outpoint.txid = txid.to_byte_array().to_vec();
    outpoint.vout = vout;
    let bytes = outpoint.encode_to_vec();
    format!("0x{}", hex::encode(bytes))
}

fn build_preview_block_hex(tx: &Transaction) -> Result<String> {
    let coinbase = Transaction {
        version: TxVersion::TWO,
        lock_time: bitcoin::locktime::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: bitcoin::OutPoint::null(),
            script_sig: bitcoin::ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::from_slice(&[vec![0u8; 32]]),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_00000000),
            script_pubkey: bitcoin::ScriptBuf::new(),
        }],
    };

    let mut txs = Vec::with_capacity(2);
    txs.push(coinbase);
    txs.push(tx.clone());

    let txids: Vec<Txid> = txs.iter().map(|t| t.compute_txid()).collect();
    let merkle_root_txid =
        bitcoin::merkle_tree::calculate_root(txids.into_iter()).unwrap_or_else(Txid::all_zeros);

    let header = Header {
        version: BlockVersion::TWO,
        prev_blockhash: bitcoin::BlockHash::all_zeros(),
        merkle_root: TxMerkleNode::from(merkle_root_txid),
        time: now_ts() as u32,
        bits: CompactTarget::from_consensus(0x1d00ffff),
        nonce: 0,
    };

    let block = Block { header, txdata: txs };

    let mut buf = Vec::new();
    block.consensus_encode(&mut buf)?;
    Ok(hex::encode(buf))
}

fn decode_trace_hex(
    data_hex: &str,
    txid: &Txid,
    tx: &Transaction,
    vout: u32,
) -> Result<EspoTrace> {
    let trimmed = data_hex.strip_prefix("0x").unwrap_or(data_hex);
    let bytes = hex::decode(trimmed)?;
    let protobuf_trace =
        alkanes_support::proto::alkanes::AlkanesTrace::decode(bytes.as_slice()).with_context(
            || "failed to decode preview trace protobuf",
        )?;
    let events_json_str = prettyify_protobuf_trace_json(&protobuf_trace)?;
    let events: Vec<EspoSandshrewLikeTraceEvent> = serde_json::from_str(&events_json_str)
        .context("deserialize preview trace events")?;

    let sandshrew_trace =
        EspoSandshrewLikeTrace { outpoint: format!("{}:{}", txid, vout), events };
    let storage_changes = extract_alkane_storage(&protobuf_trace, tx)?;
    let outpoint =
        EspoOutpoint { txid: txid.to_byte_array().to_vec(), vout, tx_spent: None };

    Ok(EspoTrace {
        sandshrew_trace,
        protobuf_trace,
        storage_changes,
        outpoint,
    })
}

fn collect_addresses(
    tx: &Transaction,
    network: Network,
    rpc: &CoreClient,
    prev_cache: &mut HashMap<Txid, Transaction>,
) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();

    for o in &tx.output {
        if let Ok(addr) = Address::from_script(o.script_pubkey.as_script(), network) {
            out.insert(addr.to_string());
        }
    }

    for vin in &tx.input {
        if vin.previous_output.is_null() {
            continue;
        }
        let prev_txid = vin.previous_output.txid;
        let prev = if let Some(t) = prev_cache.get(&prev_txid) {
            Some(t.clone())
        } else {
            rpc.get_raw_transaction_hex(&prev_txid, None)
                .ok()
                .and_then(|hex_str| hex::decode(hex_str).ok())
                .and_then(|raw| deserialize::<Transaction>(&raw).ok())
                .map(|ptx| {
                    prev_cache.insert(prev_txid, ptx.clone());
                    ptx
                })
        };

        if let Some(prev_tx) = prev {
            if let Some(prev_out) = prev_tx.output.get(vin.previous_output.vout as usize) {
                if let Ok(addr) = Address::from_script(prev_out.script_pubkey.as_script(), network)
                {
                    out.insert(addr.to_string());
                }
            }
        }
    }

    out
}

fn k_tx(txid: &Txid) -> Vec<u8> {
    let mut v = Vec::with_capacity(3 + 64);
    v.extend_from_slice(b"tx/");
    v.extend_from_slice(txid.to_string().as_bytes());
    v
}

fn k_seen(first_seen: u64, txid: &Txid) -> Vec<u8> {
    let mut v = Vec::with_capacity(5 + 8 + 64);
    v.extend_from_slice(b"seen/");
    v.extend_from_slice(&first_seen.to_be_bytes());
    v.push(b'/');
    v.extend_from_slice(txid.to_string().as_bytes());
    v
}

fn k_addr(addr: &str, first_seen: u64, txid: &Txid) -> Vec<u8> {
    let mut v = Vec::with_capacity(6 + addr.len() + 8 + 64);
    v.extend_from_slice(b"addr/");
    v.extend_from_slice(addr.as_bytes());
    v.push(b'/');
    v.extend_from_slice(&first_seen.to_be_bytes());
    v.push(b'/');
    v.extend_from_slice(txid.to_string().as_bytes());
    v
}

fn k_count() -> &'static [u8] {
    b"stats/count"
}

fn load_count(mdb: &Mdb) -> u64 {
    mdb.get(k_count())
        .ok()
        .flatten()
        .and_then(|b| {
            if b.len() >= 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&b[..8]);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn decode_persisted_tx(_txid: &Txid, raw: &[u8]) -> Option<PersistedMempoolTx> {
    PersistedMempoolTx::try_from_slice(raw).ok().or_else(|| {
        // attempt serde_json fallback if format changes
        serde_json::from_slice(raw).ok()
    })
}

fn build_persisted_traces(traces: &Option<Vec<EspoTrace>>) -> Vec<PersistedTrace> {
    let mut out = Vec::new();
    let Some(traces) = traces else { return out };
    for t in traces {
        let mut buf = Vec::new();
        if let Err(e) = t.protobuf_trace.encode(&mut buf) {
            eprintln!("[mempool] encode trace for {} failed: {:?}", t.sandshrew_trace.outpoint, e);
            continue;
        }
        // outpoint bytes: txid (le) + vout (le)
        let mut outp = t.outpoint.txid.clone();
        outp.extend_from_slice(&t.outpoint.vout.to_le_bytes());
        out.push(PersistedTrace { protobuf: buf, outpoint: outp });
    }
    out
}

fn reconstruct_traces(
    txid: &Txid,
    tx: &Transaction,
    persisted: &[PersistedTrace],
) -> Option<Vec<EspoTrace>> {
    if persisted.is_empty() {
        return None;
    }
    let mut traces = Vec::with_capacity(persisted.len());
    let tx_hex = txid.to_string();
    for p in persisted {
        let proto = match alkanes_support::proto::alkanes::AlkanesTrace::decode(p.protobuf.as_slice()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[mempool] decode persisted trace for {} failed: {:?}", txid, e);
                continue;
            }
        };
        let outpoint_bytes = &p.outpoint;
        if outpoint_bytes.len() < 36 {
            eprintln!("[mempool] persisted outpoint too short for {}", txid);
            continue;
        }
        let (txid_le, vout_le) = outpoint_bytes.split_at(outpoint_bytes.len() - 4);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();
        let vout = u32::from_le_bytes(vout_le.try_into().unwrap_or_default());

        let events_json_str = match prettyify_protobuf_trace_json(&proto) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[mempool] pretty trace json failed for {}: {:?}", txid, e);
                continue;
            }
        };
        let events: Vec<EspoSandshrewLikeTraceEvent> = match serde_json::from_str(&events_json_str)
        {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!("[mempool] decode trace events failed for {}: {:?}", txid, e);
                continue;
            }
        };

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = match extract_alkane_storage(&proto, tx) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[mempool] extract storage failed for {}: {:?}", txid, e);
                continue;
            }
        };

        traces.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: proto.clone(),
            storage_changes,
            outpoint: EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    if traces.is_empty() { None } else { Some(traces) }
}

async fn preview_traces_for_tx(
    http: &Client,
    preview_url: &str,
    txid: &Txid,
    tx: &Transaction,
    protostone_count: usize,
) -> Option<Vec<EspoTrace>> {
    if protostone_count == 0 {
        return None;
    }
    let block_hex = match build_preview_block_hex(tx) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("[mempool] build preview block failed for {}: {e:?}", txid);
            return None;
        }
    };
    let base = shadow_base(tx);
    let mut jobs: Vec<(u32, String)> = Vec::with_capacity(protostone_count);
    for idx in 0..protostone_count {
        let vout = base + idx as u32;
        jobs.push((vout, encode_outpoint_hex(txid, vout)));
    }

    let mut traces: Vec<EspoTrace> = Vec::new();
    for batch in jobs.chunks(MEMPOOL_PREVIEW_BATCH_SIZE) {
        let futs = stream::iter(batch.iter().map(|(vout, input_hex)| {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("{}:{}", txid, vout),
                "method": "metashrew_preview",
                "params": [
                    block_hex,
                    "trace",
                    input_hex,
                    "latest",
                ]
            });
            let http = http.clone();
            let preview_url = preview_url.to_string();
            let txid = *txid;
            async move {
                let resp_json: Value = match http.post(&preview_url).json(&body).send().await {
                    Ok(r) => match r.error_for_status() {
                        Ok(ok) => match ok.json().await {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("[mempool] preview decode failed for {}@{}: {:?}", txid, vout, e);
                                return None;
                            }
                        },
                        Err(e) => {
                            eprintln!("[mempool] preview HTTP error for {}@{}: {:?}", txid, vout, e);
                            return None;
                        }
                    },
                    Err(e) => {
                        eprintln!("[mempool] preview POST failed for {}@{}: {:?}", txid, vout, e);
                        return None;
                    }
                };

                let result_hex = resp_json
                    .get("result")
                    .and_then(|v| v.as_str())
                    .or_else(|| resp_json.get("result").and_then(|v| v.get("trace")).and_then(|v| v.as_str()));
                let Some(result_hex) = result_hex else {
                    return None;
                };
                match decode_trace_hex(result_hex, &txid, tx, *vout) {
                    Ok(trace) => Some(trace),
                    Err(e) => {
                        eprintln!("[mempool] decode preview trace {}@{} failed: {:?}", txid, vout, e);
                        None
                    }
                }
            }
        }))
        .buffer_unordered(MEMPOOL_PREVIEW_BATCH_SIZE);

        futures::pin_mut!(futs);
        while let Some(res) = futs.next().await {
            if let Some(t) = res {
                traces.push(t);
            }
        }
    }

    if traces.is_empty() { None } else { Some(traces) }
}

fn load_existing_first_seen(txids: &[Txid]) -> HashMap<Txid, u64> {
    let mdb = get_mempool_mdb();
    let keys: Vec<Vec<u8>> = txids.iter().map(k_tx).collect();
    let mut out = HashMap::new();
    if let Ok(values) = mdb.multi_get(&keys) {
        for (i, maybe) in values.into_iter().enumerate() {
            if let Some(raw) = maybe {
                if let Some(p) = decode_persisted_tx(&txids[i], &raw) {
                    out.insert(txids[i], p.first_seen);
                }
            }
        }
    }
    out
}

pub fn decode_seen_key(raw: &[u8]) -> Option<(u64, Txid)> {
    // raw is relative key, e.g., b"seen/<8 bytes>/<txid>"
    if !raw.starts_with(b"seen/") || raw.len() < 5 + 8 + 1 {
        return None;
    }
    let rest = &raw[5..];
    if rest.len() < 8 + 1 || rest[8] != b'/' {
        return None;
    }
    let mut ts_bytes = [0u8; 8];
    ts_bytes.copy_from_slice(&rest[..8]);
    let ts = u64::from_be_bytes(ts_bytes);
    let txid_bytes = &rest[9..];
    let txid_str = std::str::from_utf8(txid_bytes).ok()?;
    let txid = Txid::from_str(txid_str).ok()?;
    Some((ts, txid))
}

pub fn reset_mempool_store() -> Result<()> {
    let mdb = get_mempool_mdb();
    let keys = mdb.scan_prefix(b"").unwrap_or_default();
    let total = keys.len();
    mdb.bulk_write(|wb| {
        for k in keys {
            wb.delete(&k);
        }
    })?;
    eprintln!("[mempool] reset store: deleted {} keys", total);
    Ok(())
}

async fn build_processed_entries(
    entries: Vec<(Txid, Transaction)>,
    first_seen_map: &HashMap<Txid, u64>,
    http: &Client,
    preview_url: &str,
) -> Vec<ProcessedEntry> {
    let total = entries.len();
    let protostone_jobs: Vec<(Txid, Transaction, usize)> = entries
        .iter()
        .map(|(txid, tx)| {
            let protos = protostones_for_tx(tx);
            (txid.clone(), tx.clone(), protos.len())
        })
        .collect();

    let mut processed: Vec<ProcessedEntry> = Vec::with_capacity(entries.len());

    let mut preview_results: HashMap<Txid, Option<Vec<EspoTrace>>> = HashMap::new();
    for (chunk_idx, chunk) in protostone_jobs.chunks(MEMPOOL_LOG_STEP).enumerate() {
        let chunk_start = chunk_idx * MEMPOOL_LOG_STEP;
        eprintln!("[mempool] preview batch start {}/{}", chunk_start, total);

        let stream = stream::iter(chunk.iter().cloned().map(|(txid, tx, proto_len)| {
            let http = http.clone();
            let preview_url = preview_url.to_string();
            async move {
                let traces =
                    preview_traces_for_tx(&http, &preview_url, &txid, &tx, proto_len).await;
                (txid, traces)
            }
        }))
        .buffer_unordered(MEMPOOL_PREVIEW_TX_CONCURRENCY);

        let mut txs_with_traces = 0usize;
        let mut traces_found = 0usize;
        futures::pin_mut!(stream);
        while let Some((txid, traces)) = stream.next().await {
            if let Some(ref t) = traces {
                if !t.is_empty() {
                    txs_with_traces += 1;
                    traces_found += t.len();
                }
            }
            preview_results.insert(txid, traces);
        }

        eprintln!(
            "[mempool] preview batch done start={} size={} txs_with_traces={} traces={}",
            chunk_start,
            chunk.len(),
            txs_with_traces,
            traces_found
        );
    }

    for (txid, tx) in entries {
        let first_seen = first_seen_map.get(&txid).copied().unwrap_or_else(now_ts);
        let traces = preview_results.remove(&txid).unwrap_or(None);
        processed.push(ProcessedEntry {
            txid,
            tx,
            traces,
            first_seen,
            addresses: Vec::new(), // fill later
        });
    }

    processed
}

fn write_mempool_to_db(entries: &mut [ProcessedEntry], network: Network, rpc: &CoreClient) -> Result<()> {
    let mdb = get_mempool_mdb();

    // Build prev cache for address extraction
    let mut prev_cache: HashMap<Txid, Transaction> =
        entries.iter().map(|e| (e.txid, e.tx.clone())).collect();
    for entry in entries.iter_mut() {
        if entry.addresses.is_empty() {
            let addrs = collect_addresses(&entry.tx, network, rpc, &mut prev_cache);
            entry.addresses = addrs.into_iter().collect();
        }
    }

    // Existing txids (and records) for cleanup
    let existing_keys = mdb.scan_prefix(b"tx/").unwrap_or_default();
    let mut existing_map: HashMap<Txid, PersistedMempoolTx> = HashMap::new();
    for rel in existing_keys {
        if let Some(txid_str) = std::str::from_utf8(&rel[3..]).ok() {
            if let Ok(txid) = Txid::from_str(txid_str) {
                if let Ok(Some(raw)) = mdb.get(&rel) {
                    if let Some(p) = decode_persisted_tx(&txid, &raw) {
                        existing_map.insert(txid, p);
                    }
                }
            }
        }
    }

    let new_set: HashSet<Txid> = entries.iter().map(|e| e.txid).collect();

    // Write batch
    mdb.bulk_write(|wb| {
        // Remove txs that disappeared
        for (txid, rec) in existing_map.iter() {
            if new_set.contains(txid) {
                continue;
            }
            wb.delete(&k_tx(txid));
            wb.delete(&k_seen(rec.first_seen, txid));
            for addr in &rec.addresses {
                wb.delete(&k_addr(addr, rec.first_seen, txid));
            }
        }

        // Upsert current mempool
        for entry in entries.iter() {
            let traces_persisted = build_persisted_traces(&entry.traces);
            let persisted = PersistedMempoolTx {
                raw_tx: serialize(&entry.tx),
                traces: traces_persisted,
                first_seen: entry.first_seen,
                addresses: entry.addresses.clone(),
            };
            let encoded = match to_vec(&persisted) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[mempool] encode persisted tx {} failed: {:?}", entry.txid, e);
                    continue;
                }
            };
            wb.put(&k_tx(&entry.txid), &encoded);
            wb.put(&k_seen(entry.first_seen, &entry.txid), &[]);
            for addr in &entry.addresses {
                wb.put(&k_addr(addr, entry.first_seen, &entry.txid), &[]);
            }
        }

        let count_bytes = (entries.len() as u64).to_le_bytes();
        wb.put(k_count(), &count_bytes);
    })
    .context("write mempool to db")?;

    prune_oversized_mempool(entries.len())?;

    Ok(())
}

fn delete_tx_from_store(
    mdb: &Mdb,
    txid: &Txid,
    persisted: &PersistedMempoolTx,
) -> Result<()> {
    let first_seen = persisted.first_seen;
    mdb.bulk_write(|wb| {
        wb.delete(&k_tx(txid));
        wb.delete(&k_seen(first_seen, txid));
        for addr in &persisted.addresses {
            wb.delete(&k_addr(addr, first_seen, txid));
        }
    })?;
    Ok(())
}

fn prune_oversized_mempool(current_len: usize) -> Result<()> {
    if current_len <= MEMPOOL_MAX_TXS {
        return Ok(());
    }
    let mdb = get_mempool_mdb();
    let mut to_delete = current_len.saturating_sub(MEMPOOL_MAX_TXS);
    eprintln!("[mempool] pruning {} oldest entries ({} -> {})", to_delete, current_len, MEMPOOL_MAX_TXS);

    let mut removed = 0usize;
    let seen_prefix = b"seen/";
    for res in mdb.iter_from(seen_prefix) {
        let (k, _) = res?;
        let rel = &k[mdb.prefix().len()..];
        if !rel.starts_with(seen_prefix) {
            break;
        }
        let Some((first_seen, txid)) = decode_seen_key(rel) else { continue };
        if let Some(raw) = mdb.get(&k_tx(&txid))? {
            if let Some(persisted) = decode_persisted_tx(&txid, &raw) {
                mdb.bulk_write(|wb| {
                    wb.delete(&k_tx(&txid));
                    wb.delete(&k_seen(first_seen, &txid));
                    for addr in &persisted.addresses {
                        wb.delete(&k_addr(addr, first_seen, &txid));
                    }
                })?;
            }
        }
        removed += 1;
        to_delete = to_delete.saturating_sub(1);
        if to_delete == 0 {
            break;
        }
    }

    let new_count = current_len.saturating_sub(removed) as u64;
    mdb.put(k_count(), &new_count.to_le_bytes())?;
    Ok(())
}

fn upsert_mempool_chunk(entries: &mut [ProcessedEntry]) -> Result<()> {
    let mdb = get_mempool_mdb();
    mdb.bulk_write(|wb| {
        for entry in entries.iter() {
            let traces_persisted = build_persisted_traces(&entry.traces);
            let persisted = PersistedMempoolTx {
                raw_tx: serialize(&entry.tx),
                traces: traces_persisted,
                first_seen: entry.first_seen,
                addresses: entry.addresses.clone(),
            };
            let encoded = match to_vec(&persisted) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[mempool] encode persisted tx {} failed: {:?}", entry.txid, e);
                    continue;
                }
            };
            wb.put(&k_tx(&entry.txid), &encoded);
            wb.put(&k_seen(entry.first_seen, &entry.txid), &[]);
            for addr in &entry.addresses {
                wb.put(&k_addr(addr, entry.first_seen, &entry.txid), &[]);
            }
        }
    })?;
    Ok(())
}

async fn refresh_mempool(
    rpc: &CoreClient,
    http: &Client,
    preview_url: &str,
    network: Network,
) -> Result<()> {
    let txids = rpc.get_raw_mempool().context("bitcoind getrawmempool failed")?;
    let tx_total = txids.len();
    eprintln!("[mempool] processing {} transactions", tx_total);

    let mut entries: Vec<(Txid, Transaction)> = Vec::with_capacity(tx_total);
    let mut low_fee_skips: usize = 0;
    for (_idx, txid) in txids.into_iter().enumerate() {
        match rpc.get_raw_transaction_hex(&txid, None) {
            Ok(raw_hex) => {
                let raw_bytes = match hex::decode(raw_hex.trim()) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("[mempool] decode hex failed for {}: {}", txid, e);
                        continue;
                    }
                };
                match deserialize::<Transaction>(&raw_bytes) {
                    Ok(tx) => {
                        // Filter by feerate (sats/vbyte)
                        let vsize = tx.vsize() as f64;
                        if vsize > 0.0 {
                            let out: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
                            let mut input_total = Some(0u64);
                            for vin in &tx.input {
                                if vin.previous_output.is_null() {
                                    input_total = None;
                                    break;
                                }
                                if let Ok(prev_raw) =
                                    rpc.get_raw_transaction_hex(&vin.previous_output.txid, None)
                                {
                                    if let Ok(prev_bytes) = hex::decode(prev_raw.trim()) {
                                        if let Ok(prev_tx) = deserialize::<Transaction>(&prev_bytes)
                                        {
                                            if let Some(prev_out) =
                                                prev_tx.output.get(vin.previous_output.vout as usize)
                                            {
                                                input_total = input_total.and_then(|acc| {
                                                    acc.checked_add(prev_out.value.to_sat())
                                                });
                                            }
                                        }
                                    }
                                }
                                if input_total.is_none() {
                                    break;
                                }
                            }
                            if let Some(inputs) = input_total {
                                    if let Some(fee) = inputs.checked_sub(out) {
                                        let feerate = fee as f64 / vsize;
                                        if feerate < MEMPOOL_MIN_FEE_RATE_SATS_VBYTE {
                                            low_fee_skips += 1;
                                            continue;
                                        }
                                    }
                                }
                            }
                        entries.push((txid, tx))
                    }
                    Err(e) => eprintln!("[mempool] decode tx {} failed: {}", txid, e),
                }
            }
            Err(e) => eprintln!("[mempool] getrawtransaction {} failed: {}", txid, e),
        }

    }
    if low_fee_skips > 0 {
        eprintln!("[mempool] skipped {} low-fee txs (< {:.2} sat/vB)", low_fee_skips, MEMPOOL_MIN_FEE_RATE_SATS_VBYTE);
    }

    let mut processed: Vec<ProcessedEntry> = Vec::new();
    let mut new_entries: Vec<(Txid, Transaction)> = Vec::new();

    for (txid, tx) in entries {
        if let Some(p) = load_tx(&txid) {
            let traces = reconstruct_traces(&txid, &tx, &p.traces);
            processed.push(ProcessedEntry {
                txid,
                tx,
                traces,
                first_seen: p.first_seen,
                addresses: p.addresses.clone(),
            });
        } else {
            new_entries.push((txid, tx));
        }
    }

    if !new_entries.is_empty() {
        let txids_only: Vec<Txid> = new_entries.iter().map(|(t, _)| *t).collect();
        let first_seen_map = load_existing_first_seen(&txids_only);
        let mut new_processed =
            build_processed_entries(new_entries, &first_seen_map, http, preview_url).await;
        processed.append(&mut new_processed);
    }

    // incremental checkpoint every MEMPOOL_LOG_STEP
    for chunk in processed.chunks_mut(MEMPOOL_LOG_STEP) {
        if let Err(e) = upsert_mempool_chunk(chunk) {
            eprintln!("[mempool] checkpoint write failed: {e:?}");
        }
    }

    write_mempool_to_db(&mut processed, network, rpc)?;

    Ok(())
}

pub async fn run_mempool_service(network: Network) -> Result<()> {
    let rpc = get_bitcoind_rpc_client();
    let preview_url = get_metashrew_rpc_url().to_string();
    let http = Client::new();

    eprintln!(
        "[mempool] service starting (poll={}s, preview_url={})",
        MEMPOOL_POLL_SECS, preview_url
    );

    loop {
        eprintln!("[mempool] refresh tick");
        if let Err(e) = refresh_mempool(&rpc, &http, &preview_url, network).await {
            eprintln!("[mempool] refresh failed: {e:?}");
        }

        eprintln!("[mempool] sleeping {}s", MEMPOOL_POLL_SECS);
        tokio::time::sleep(Duration::from_secs(MEMPOOL_POLL_SECS)).await;
    }
}

fn load_tx(txid: &Txid) -> Option<PersistedMempoolTx> {
    let mdb = get_mempool_mdb();
    let raw = mdb.get(&k_tx(txid)).ok()??;
    decode_persisted_tx(txid, &raw)
}

pub fn get_tx_from_mempool(txid: &Txid) -> Option<MempoolEntry> {
    let persisted = load_tx(txid)?;
    let tx: Transaction = deserialize(&persisted.raw_tx).ok()?;
    let traces = reconstruct_traces(txid, &tx, &persisted.traces);
    Some(MempoolEntry { txid: *txid, tx, traces, first_seen: persisted.first_seen })
}

pub fn pending_by_txid(txid: &Txid) -> Option<MempoolEntry> {
    get_tx_from_mempool(txid)
}

pub fn pending_for_address(addr: &str) -> Vec<MempoolEntry> {
    let mdb = get_mempool_mdb();
    let mut out: Vec<MempoolEntry> = Vec::new();

    let mut prefix = Vec::with_capacity(6 + addr.len());
    prefix.extend_from_slice(b"addr/");
    prefix.extend_from_slice(addr.as_bytes());
    prefix.push(b'/');

    for res in mdb.iter_prefix_rev(&mdb.prefixed(&prefix)) {
        let Ok((k_full, _)) = res else { continue };
        let rel = &k_full[mdb.prefix().len()..];
        if !rel.starts_with(&prefix) {
            break;
        }
        // rel format: addr/<addr>/<first_seen_be>/<txid>
        let parts: Vec<&[u8]> = rel.split(|b| *b == b'/').collect();
        if parts.len() < 4 {
            continue;
        }
        let txid_str = match std::str::from_utf8(parts[3]) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let Ok(txid) = Txid::from_str(txid_str) else { continue };
        if let Some(entry) = get_tx_from_mempool(&txid) {
            out.push(entry);
        }
    }

    out
}

pub fn purge_confirmed_txids(txids: &[Txid]) -> Result<usize> {
    let mdb = get_mempool_mdb();
    let mut removed = 0usize;
    for txid in txids {
        if let Some(persisted) = load_tx(txid) {
            delete_tx_from_store(mdb, txid, &persisted)?;
            removed += 1;
        }
    }
    if removed > 0 {
        let current = load_count(mdb);
        let new_count = current.saturating_sub(removed as u64);
        mdb.put(k_count(), &new_count.to_le_bytes())?;
    }
    Ok(removed)
}

pub fn purge_confirmed_from_chain() -> Result<usize> {
    let rpc = get_bitcoind_rpc_client();
    let mdb = get_mempool_mdb();
    let keys = mdb.scan_prefix(b"tx/").unwrap_or_default();
    if keys.is_empty() {
        return Ok(0);
    }

    let mut confirmed: Vec<Txid> = Vec::new();
    for rel in keys {
        if rel.len() <= 3 {
            continue;
        }
        let Some(txid_str) = std::str::from_utf8(&rel[3..]).ok() else { continue };
        let Ok(txid) = Txid::from_str(txid_str) else { continue };
        if let Ok(info) = rpc.get_raw_transaction_info(&txid, None) {
            if info.blockhash.is_some() {
                confirmed.push(txid);
            }
        }
    }

    let removed = purge_confirmed_txids(&confirmed)?;
    if removed > 0 {
        eprintln!("[mempool] purged {} confirmed txs from store", removed);
    }
    Ok(removed)
}
