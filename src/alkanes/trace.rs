use std::collections::{HashMap, HashSet};

use crate::config::{get_electrum_client, get_metashrew_sdb};
use crate::consts::TRACES_BY_BLOCK_PREFIX;
use crate::runtime::sdb::SDB;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::proto::alkanes;
use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::{Context, Result};
use bitcoin::block::Header;
use bitcoin::consensus::deserialize;
use bitcoin::{Transaction, Txid};
use electrum_client::ElectrumApi;
use protobuf::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTrace {
    pub outpoint: String,
    pub events: Vec<EspoSandshrewLikeTraceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", content = "data")]
pub enum EspoSandshrewLikeTraceEvent {
    #[serde(rename = "invoke")]
    Invoke(EspoSandshrewLikeTraceInvokeData),

    #[serde(rename = "return")]
    Return(EspoSandshrewLikeTraceReturnData),

    #[serde(rename = "create")]
    Create(EspoSandshrewLikeTraceCreateData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceInvokeData {
    #[serde(rename = "type")]
    pub typ: String,
    pub context: EspoSandshrewLikeTraceInvokeContext,
    pub fuel: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceInvokeContext {
    pub myself: EspoSandshrewLikeTraceShortId,
    pub caller: EspoSandshrewLikeTraceShortId,
    pub inputs: Vec<String>,
    #[serde(rename = "incomingAlkanes")]
    pub incoming_alkanes: Vec<EspoSandshrewLikeTraceTransfer>,
    pub vout: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceShortId {
    pub block: String,
    pub tx: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceTransfer {
    pub id: EspoSandshrewLikeTraceShortId,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceReturnData {
    pub status: EspoSandshrewLikeTraceStatus,
    pub response: EspoSandshrewLikeTraceReturnResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EspoSandshrewLikeTraceStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceReturnResponse {
    pub alkanes: Vec<EspoSandshrewLikeTraceTransfer>,
    pub data: String,
    pub storage: Vec<EspoSandshrewLikeTraceStorageKV>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceStorageKV {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EspoSandshrewLikeTraceCreateData {
    #[serde(rename = "newAlkane")]
    pub new_alkane: EspoSandshrewLikeTraceShortId,
}
#[derive(Clone)]
pub struct EspoOutpoint {
    pub txid: Vec<u8>,
    pub vout: u32,
}
pub struct PartialEspoTrace {
    pub protobuf_trace: AlkanesTrace,
    pub outpoint: Vec<u8>,
}
#[derive(Clone)]
pub struct EspoAlkanesTransaction {
    pub sandshrew_trace: EspoSandshrewLikeTrace,
    pub protobuf_trace: AlkanesTrace,
    pub storage_changes: AlkaneStorageChanges,
    pub outpoint: EspoOutpoint,
    pub transaction: Transaction,
}
#[derive(Clone)]
pub struct EspoBlock {
    pub height: u32,
    pub block_header: Header,
    pub transactions: Vec<EspoAlkanesTransaction>,
    //All related prevouts for this block
    pub prevouts: HashMap<Txid, Transaction>,
}

/// Map of AlkaneId -> (key -> value), last-write-wins per key within a single trace.
pub type AlkaneStorageChanges = HashMap<SchemaAlkaneId, HashMap<Vec<u8>, Vec<u8>>>;

/// Convert proto uint128 to (hi, lo) as u128
fn u128_from_proto(x: &alkanes::Uint128) -> u128 {
    ((x.hi as u128) << 64) | (x.lo as u128)
}

/// Convert proto AlkaneId (uint128 fields) into your SchemaAlkaneId (u32, u64).
/// Truncates safely assuming values fit (hi==0 for both fields). If not, we clamp.
fn schema_id_from_proto(id: &alkanes::AlkaneId) -> SchemaAlkaneId {
    let b = id.block.as_ref().map(u128_from_proto).unwrap_or(0);
    let t = id.tx.as_ref().map(u128_from_proto).unwrap_or(0);
    // Truncate with saturation (you can assert if you want strictness)
    SchemaAlkaneId { block: (b as u64) as u32, tx: t as u64 }
}

/// Frame carries only the *active storage owner* (codeId is irrelevant for attribution here)
#[derive(Clone, Copy, Debug)]
struct Frame {
    storage_owner: SchemaAlkaneId,
}

/// Extract last-write-wins storage mutations per Alkane from a single protobuf trace.
pub fn extract_alkane_storage(trace: &alkanes::AlkanesTrace) -> AlkaneStorageChanges {
    let mut out: AlkaneStorageChanges = HashMap::new();
    let mut stack: Vec<Frame> = Vec::with_capacity(16);

    for ev in &trace.events {
        use alkanes::alkanes_trace_event::Event;
        if let Some(evt) = &ev.event {
            match evt {
                Event::EnterContext(enter) => {
                    let call_ty = enter.call_type.enum_value_or_default();
                    let ctx = match enter.context.as_ref() {
                        Some(c) => c,
                        None => continue,
                    };
                    let inner = match ctx.inner.as_ref() {
                        Some(i) => i,
                        None => continue,
                    };
                    let myself = match inner.myself.as_ref() {
                        Some(m) => m,
                        None => continue,
                    };

                    // Determine storage owner based on call type
                    let owner = match call_ty {
                        alkanes::AlkanesTraceCallType::CALL => schema_id_from_proto(myself),
                        alkanes::AlkanesTraceCallType::DELEGATECALL
                        | alkanes::AlkanesTraceCallType::STATICCALL => {
                            // inherit from parent if any, otherwise default to self
                            stack
                                .last()
                                .map(|f| f.storage_owner)
                                .unwrap_or_else(|| schema_id_from_proto(myself))
                        }
                        _ => {
                            // Unknown/none: default to self
                            schema_id_from_proto(myself)
                        }
                    };

                    stack.push(Frame { storage_owner: owner });
                }

                Event::ExitContext(exit) => {
                    // Peek or pop depending on your policy
                    let frame = match stack.last().copied() {
                        Some(f) => f,
                        None => continue,
                    };

                    if let Some(resp) = exit.response.as_ref() {
                        // `resp.storage` is the repeated KeyValuePair
                        for kv in &resp.storage {
                            let k = kv.key.clone();
                            let v = kv.value.clone();

                            out.entry(frame.storage_owner)
                                .or_insert_with(HashMap::new)
                                .insert(k, v); // last write wins
                        }
                    }

                    // If you know your traces are 1:1 enter/exit, use:
                    // let _ = stack.pop();
                }

                Event::CreateAlkane(_create) => {
                    // Creation doesn’t by itself mutate storage in the return payload we process here.
                    // If you want to mark new alkanes, you could precreate an empty map entry.
                }

                &_ => {
                    continue;
                }
            }
        }
    }

    out
}

fn trace_block_prefix(block: &u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(7 + 8 + 1);
    v.extend_from_slice(TRACES_BY_BLOCK_PREFIX);
    v.extend_from_slice(&block.to_le_bytes());
    v.push(b'/');
    v
}
fn next_prefix(mut p: Vec<u8>) -> Option<Vec<u8>> {
    for i in (0..p.len()).rev() {
        if p[i] != 0xff {
            p[i] += 1;
            p.truncate(i + 1);
            return Some(p);
        }
    }
    None
}
fn is_length_bucket(key: &[u8], prefix: &[u8]) -> bool {
    if key.len() < prefix.len() + 1 + 4 {
        return false;
    }
    let bucket = &key[prefix.len()..key.len() - 4];
    bucket == b"length"
}
pub fn collect_outpoints_for_block(db: &SDB, block: &u64) -> Result<Vec<Vec<u8>>> {
    let prefix = trace_block_prefix(&block);
    let mut ro = ReadOptions::default();
    if let Some(ub) = next_prefix(prefix.clone()) {
        ro.set_iterate_upper_bound(ub);
    }
    ro.set_total_order_seek(true);

    let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
    let mut out = Vec::new();

    while let Some(Ok((k, v))) = it.next() {
        if !k.starts_with(&prefix) {
            break;
        }
        if is_length_bucket(&k, &prefix) {
            continue;
        }
        if v.len() == 4 && v[..] == [0x01, 0x00, 0x00, 0x00] {
            continue;
        }
        if v.len() < 36 {
            continue;
        }
        out.push(v[..36].to_vec());
    }
    Ok(out)
}

pub fn outpoint_bytes_to_key(bytes: &Vec<u8>) -> Vec<u8> {
    vec![
        TRACES_BY_BLOCK_PREFIX,
        &bytes.to_vec(),
        //extra padding cuz why tf not
        &vec![0x00, 0x00, 0x00, 0x00],
    ]
    .concat()
}

fn fmt_u128_hex(u: &alkanes::Uint128) -> String {
    let v = ((u.hi as u128) << 64) | (u.lo as u128);
    format!("0x{:x}", v)
}

/// Bytes -> 0x-hex
fn fmt_bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

/// Bytes -> String; if not valid UTF-8, fall back to 0x-hex
fn bytes_to_string_or_hex(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => fmt_bytes_hex(b),
    }
}

pub fn prettyify_protobuf_trace_json(trace: &AlkanesTrace) -> Result<String> {
    let mut out: Vec<Value> = Vec::with_capacity(trace.events.len() as usize);

    for ev in &trace.events {
        if let Some(event) = &ev.event {
            use alkanes::alkanes_trace_event::Event;
            match event {
                Event::EnterContext(enter) => {
                    // NOTE: enum wrapper -> unwrap

                    let typ = match enter.call_type.enum_value_or_default() {
                        alkanes::AlkanesTraceCallType::CALL => "call",
                        alkanes::AlkanesTraceCallType::DELEGATECALL => "delegatecall",
                        alkanes::AlkanesTraceCallType::STATICCALL => "staticcall",
                        _ => "unknown",
                    };

                    let ctx = enter.context.as_ref().context("enter.context missing")?;
                    let inner = ctx.inner.as_ref().context("enter.context.inner missing")?;

                    let myself = inner.myself.as_ref();
                    let caller = inner.caller.as_ref();

                    let my_block = myself.and_then(|m| m.block.as_ref());
                    let my_tx = myself.and_then(|m| m.tx.as_ref());
                    let caller_block = caller.and_then(|c| c.block.as_ref());
                    let caller_tx = caller.and_then(|c| c.tx.as_ref());

                    let inputs_hex: Vec<String> = inner.inputs.iter().map(fmt_u128_hex).collect();

                    let incoming_alkanes: Vec<Value> = inner.incoming_alkanes
                        .iter()
                        .map(|t| {
                            let id = t.id.as_ref();
                            json!({
                                "id": {
                                    "block": id.and_then(|x| x.block.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    id.and_then(|x| x.tx.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "value": t.value.as_ref().map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                            })
                        })
                        .collect();

                    out.push(
                        json!({
                        "event": "invoke",
                        "data": {
                            "type": typ,
                            "context": {
                                "myself": {
                                    "block": my_block.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    my_tx.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "caller": {
                                    "block": caller_block.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    caller_tx.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "inputs": inputs_hex,
                                "incomingAlkanes": incoming_alkanes,
                                "vout": inner.vout,
                            },
                            "fuel": ctx.fuel,
                        }
                    })
                    );
                }

                Event::ExitContext(exit) => {
                    let status = match exit.status.enum_value_or_default() {
                        alkanes::AlkanesTraceStatusFlag::FAILURE => "failure",
                        _ => "success",
                    };

                    let resp = exit.response.as_ref();

                    let alkanes_list: Vec<Value> = resp
                        .map(|r| {
                            r.alkanes
                                .iter()
                                .map(|t| {
                                    let id = t.id.as_ref();
                                    json!({
                                    "id": {
                                        "block": id.and_then(|x| x.block.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                        "tx":    id.and_then(|x| x.tx.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    },
                                    "value": t.value.as_ref().map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                })
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let storage_list: Vec<Value> = resp
                        .map(|r| {
                            r.storage
                                .iter()
                                .map(|kv| {
                                    let k = bytes_to_string_or_hex(&kv.key);
                                    let v = fmt_bytes_hex(&kv.value);
                                    json!({ "key": k, "value": v })
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let data_hex =
                        resp.map(|r| fmt_bytes_hex(&r.data)).unwrap_or_else(|| "0x".to_string());

                    out.push(json!({
                        "event": "return",
                        "data": {
                            "status": status,
                            "response": {
                                "alkanes": alkanes_list,
                                "data": data_hex,
                                "storage": storage_list,
                            }
                        }
                    }));
                }

                Event::CreateAlkane(create) => {
                    let id = create.new_alkane.as_ref();
                    let block_hex = id
                        .and_then(|x| x.block.as_ref())
                        .map(fmt_u128_hex)
                        .unwrap_or_else(|| "0x0".to_string());
                    let tx_hex = id
                        .and_then(|x| x.tx.as_ref())
                        .map(fmt_u128_hex)
                        .unwrap_or_else(|| "0x0".to_string());
                    out.push(json!({
                        "event": "create",
                        "data": {
                            "newAlkane": { "block": block_hex, "tx": tx_hex }
                        }
                    }));
                }
                &_ => {}
            }
        }
    }

    Ok(serde_json::to_string(&out).context("serialize normalized events")?)
}
fn outpoint_bytes_to_display(outpoint: &[u8]) -> String {
    let (txid_le, vout_le) = outpoint.split_at(32);
    let mut txid_be = txid_le.to_vec();
    txid_be.reverse();
    let vout = u32::from_le_bytes(vout_le.try_into().expect("vout 4 bytes"));
    format!("{}:{}", hex::encode(txid_be), vout)
}

// parse possibly-tailed trace (strip trailing u32 if needed)
fn parse_trace_maybe_with_trailer(mut bytes: Vec<u8>) -> Option<AlkanesTrace> {
    AlkanesTrace::parse_from_bytes(&bytes).ok().or_else(|| {
        if bytes.len() >= 4 {
            bytes.truncate(bytes.len() - 4);
            AlkanesTrace::parse_from_bytes(&bytes).ok()
        } else {
            None
        }
    })
}

pub fn traces_for_block_as_protobuf(db: &SDB, block: u64) -> Result<Vec<PartialEspoTrace>> {
    let outpoints = collect_outpoints_for_block(db, &block)
        .with_context(|| format!("collect_outpoints_for_block({block}) failed"))?; // Vec<Vec<u8>>

    let keys: Vec<Vec<u8>> = outpoints.iter().map(|op| outpoint_bytes_to_key(op)).collect();

    let values = db.multi_get(keys.iter())?;

    let traces: Vec<PartialEspoTrace> = values
        .into_iter()
        .flat_map(Option::into_iter)
        .map(|bytes| parse_trace_maybe_with_trailer(bytes))
        .flat_map(Option::into_iter)
        .enumerate()
        .map(|(index, protobuf_trace)| PartialEspoTrace {
            protobuf_trace,
            outpoint: outpoints[index].clone(),
        })
        .collect();
    Ok(traces)
}

pub fn traces_for_block_as_json_str(db: &SDB, block: u64) -> Result<String> {
    let partial_traces = traces_for_block_as_protobuf(db, block)?;
    let mut entries: Vec<serde_json::Value> = Vec::new();

    for partial_trace in partial_traces {
        // already-JSON string -> Value
        let events_v: serde_json::Value =
            serde_json::from_str(&prettyify_protobuf_trace_json(&partial_trace.protobuf_trace)?)?;

        entries.push(json!({
            "outpoint": outpoint_bytes_to_display(&partial_trace.outpoint),
            "events": events_v, // <-- Value, not a string
        }));
    }

    let final_json =
        serde_json::to_string(&entries).context("ESPO: failed to serialize final entries array")?;

    Ok(final_json)
}

fn collect_prevouts_txids_for_block(transactions: &Vec<EspoAlkanesTransaction>) -> Vec<Txid> {
    let mut prevout_txid_set = HashSet::new();

    for transaction in transactions {
        for vin in &transaction.transaction.input {
            prevout_txid_set.insert(vin.previous_output.txid);
        }
    }

    prevout_txid_set.into_iter().collect()
}

fn get_bulk_txs_from_electrum(txids: Vec<Txid>) -> Result<HashMap<Txid, Transaction>> {
    let client = get_electrum_client();

    let raw_txs: Vec<Vec<u8>> = client
        .batch_transaction_get_raw(&txids)
        .context("electrum batch_transaction_get_raw failed")?;

    let mut tx_map: HashMap<Txid, Transaction> = HashMap::with_capacity(txids.len());
    for (i, tx_bytes) in raw_txs.into_iter().enumerate() {
        let tx: Transaction = deserialize(&tx_bytes).context("deserialize tx")?;
        tx_map.insert(txids[i], tx);
    }

    Ok(tx_map)
}

pub fn get_espo_block(block: u64) -> Result<EspoBlock> {
    let metashrew_sdb = &get_metashrew_sdb();
    let client = get_electrum_client();

    let partials = traces_for_block_as_protobuf(metashrew_sdb, block)
        .with_context(|| format!("failed traces_for_block_as_protobuf({block})"))?;

    let mut uniq_txids: Vec<Txid> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for p in &partials {
        let (txid_le, _vout_le) = p.outpoint.split_at(32);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();
        let txid_hex = hex::encode(txid_be);
        if seen.insert(txid_hex.clone()) {
            let txid = Txid::from_str(&txid_hex)
                .with_context(|| format!("invalid txid hex: {txid_hex}"))?;
            uniq_txids.push(txid);
        }
    }

    let tx_map = get_bulk_txs_from_electrum(uniq_txids)?;

    let block_header: bitcoin::block::Header =
        client.block_header(block as usize).context("electrum block_header failed")?;

    let mut txs: Vec<EspoAlkanesTransaction> = Vec::with_capacity(partials.len());
    for p in partials {
        // outpoint -> txid (BE hex) + vout
        let (txid_le, vout_le) = p.outpoint.split_at(32);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();
        let txid_hex = hex::encode(&txid_be);
        let txid =
            Txid::from_str(&txid_hex).with_context(|| format!("invalid txid hex: {txid_hex}"))?;
        let vout = u32::from_le_bytes(vout_le.try_into().expect("vout 4 bytes"));

        let events_json_str = prettyify_protobuf_trace_json(&p.protobuf_trace)?;
        let events: Vec<EspoSandshrewLikeTraceEvent> =
            serde_json::from_str(&events_json_str).context("deserialize sandshrew-like events")?;

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{txid_hex}:{vout}"), events };

        let transaction = tx_map
            .get(&txid)
            .cloned()
            .with_context(|| format!("missing raw tx for {txid}"))?;

        let outpoint = EspoOutpoint { txid: txid_be, vout };

        let storage_changes = extract_alkane_storage(&p.protobuf_trace);
        /*
                for (id, kvs) in &storage_changes {
                    let alkane_hex = format!("0x{:x}:0x{:x}", id.block, id.tx); // e.g., 0x4:0xfff2
                    for (k, v) in kvs {
                        println!(
                            "[indexer_debug] storage change {} => {} on {}",
                            fmt_bytes_hex(k),
                            fmt_bytes_hex(v),
                            alkane_hex
                        );
                    }
                }
        */
        txs.push(EspoAlkanesTransaction {
            sandshrew_trace,
            protobuf_trace: p.protobuf_trace,
            storage_changes,
            outpoint,
            transaction,
        });
    }
    let prevout_txids = collect_prevouts_txids_for_block(&txs);

    let prev_txs_map = get_bulk_txs_from_electrum(prevout_txids)?;

    Ok(EspoBlock {
        block_header,
        transactions: txs,
        height: block.try_into()?,
        prevouts: prev_txs_map,
    })
}
