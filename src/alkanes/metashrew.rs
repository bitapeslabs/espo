use crate::alkanes::trace::PartialEspoTrace;
use crate::config::get_metashrew_sdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_cli_common::alkanes_pb::{AlkanesTrace, AlkanesTraceEvent};
use alkanes_support::gz;
use alkanes_support::id::AlkaneId as SupportAlkaneId;
use anyhow::{Context, Result, anyhow};
use bitcoin::Txid;
use bitcoin::hashes::Hash;
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::collections::{BTreeMap, HashMap, HashSet};

fn try_decode_trace_prost(raw: &[u8]) -> Option<AlkanesTrace> {
    AlkanesTrace::decode(raw).ok().or_else(|| {
        if raw.len() >= 4 { AlkanesTrace::decode(&raw[..raw.len() - 4]).ok() } else { None }
    })
}

fn try_decode_trace_event_prost(raw: &[u8]) -> Option<AlkanesTraceEvent> {
    AlkanesTraceEvent::decode(raw).ok().or_else(|| {
        if raw.len() >= 4 { AlkanesTraceEvent::decode(&raw[..raw.len() - 4]).ok() } else { None }
    })
}

/// Traces can be stored as raw protobuf bytes or as UTF-8 "height:HEX" blobs.
/// This helper handles both by decoding any hex payload and stripping the
/// optional 4-byte trailer some entries carry.
pub fn decode_trace_blob(bytes: &[u8]) -> Option<AlkanesTrace> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_block, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                if let Some(trace) = try_decode_trace_prost(&decoded) {
                    return Some(trace);
                }
            }
        }
    }

    try_decode_trace_prost(bytes)
}

pub fn decode_trace_event_blob(bytes: &[u8]) -> Option<AlkanesTraceEvent> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_block, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                if let Some(event) = try_decode_trace_event_prost(&decoded) {
                    return Some(event);
                }
            }
        }
    }

    try_decode_trace_event_prost(bytes)
}

pub struct MetashrewAdapter {
    label: Option<String>,
}

pub trait FromLeBytes<const N: usize>: Sized {
    fn from_le_bytes(bytes: [u8; N]) -> Self;
}

impl FromLeBytes<4> for u32 {
    fn from_le_bytes(bytes: [u8; 4]) -> Self {
        u32::from_le_bytes(bytes)
    }
}

impl FromLeBytes<8> for u64 {
    fn from_le_bytes(bytes: [u8; 8]) -> Self {
        u64::from_le_bytes(bytes)
    }
}

impl FromLeBytes<16> for u128 {
    fn from_le_bytes(bytes: [u8; 16]) -> Self {
        u128::from_le_bytes(bytes)
    }
}

impl MetashrewAdapter {
    pub fn new(label: Option<String>) -> MetashrewAdapter {
        let norm_label = label.and_then(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        });
        MetashrewAdapter { label: norm_label }
    }

    fn next_prefix(&self, mut p: Vec<u8>) -> Option<Vec<u8>> {
        for i in (0..p.len()).rev() {
            if p[i] != 0xff {
                p[i] += 1;
                p.truncate(i + 1);
                return Some(p);
            }
        }
        None
    }

    fn apply_label(&self, key: Vec<u8>) -> Vec<u8> {
        let suffix = b"://";

        match &self.label {
            Some(label) => {
                let mut result: Vec<u8> = vec![];
                result.extend(label.as_str().as_bytes());
                result.extend(suffix);
                result.extend(key);
                result
            }
            None => key.clone(),
        }
    }

    fn read_uint_key<const N: usize, T>(&self, key: Vec<u8>) -> Result<T>
    where
        T: FromLeBytes<N>,
    {
        let metashrew_sdb = get_metashrew_sdb();

        let bytes = metashrew_sdb
            .get(key)?
            .ok_or_else(|| anyhow!("ESPO ERROR: failed to find metashrew key"))?;

        let arr: [u8; N] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("ESPO ERROR: Expected {} bytes, got {}", N, bytes.len()))?;

        Ok(T::from_le_bytes(arr))
    }

    fn load_wasm_inner(
        &self,
        db: &rocksdb::DB,
        block: u128,
        tx: u128,
        seen: &mut HashSet<(u128, u128)>,
        hops: usize,
    ) -> Result<Option<(Vec<u8>, SupportAlkaneId)>> {
        const MAX_HOPS: usize = 64;
        if hops > MAX_HOPS {
            return Err(anyhow!("alias chain too deep (possible cycle)"));
        }
        if !seen.insert((block, tx)) {
            return Err(anyhow!("alias cycle detected at ({block}, {tx})"));
        }

        // Build candidate keys (LE + BE, with optional /0..3 suffixes)
        let mut base_le = b"/alkanes/".to_vec();
        base_le.extend_from_slice(&block.to_le_bytes());
        base_le.extend_from_slice(&tx.to_le_bytes());
        let mut base_be = b"/alkanes/".to_vec();
        base_be.extend_from_slice(&block.to_be_bytes());
        base_be.extend_from_slice(&tx.to_be_bytes());

        let mut candidate_keys: Vec<Vec<u8>> = Vec::new();
        for base in [base_le, base_be] {
            candidate_keys.push(self.apply_label(base.clone()));
            for idx in 0u8..=3u8 {
                let mut k = base.clone();
                k.push(b'/');
                k.push(b'0' + idx);
                candidate_keys.push(self.apply_label(k));
            }
        }

        let mut last_err: Option<anyhow::Error> = None;
        for key in candidate_keys {
            if let Some(raw) = db.get(&key)? {
                if raw.is_empty() {
                    continue;
                }

                // direct alias payload
                if raw.len() == 32 {
                    let alias = SupportAlkaneId::try_from(raw.clone())
                        .map_err(|e| anyhow!("decode alkane alias for ({block}, {tx}): {e}"))?;
                    return self.load_wasm_inner(db, alias.block, alias.tx, seen, hops + 1);
                }

                // pointer string like "height:HEX"
                let mut payload = raw.to_vec();
                if let Ok(s) = std::str::from_utf8(&raw) {
                    if let Some((_h, hex_part)) = s.split_once(':') {
                        if let Ok(decoded) = hex::decode(hex_part) {
                            payload = decoded;
                        }
                    }
                }

                if payload.len() == 32 {
                    if let Ok(alias) = SupportAlkaneId::try_from(payload.clone()) {
                        return self.load_wasm_inner(db, alias.block, alias.tx, seen, hops + 1);
                    }
                }

                match gz::decompress(payload.clone()) {
                    Ok(bytes) => {
                        return Ok(Some((bytes, SupportAlkaneId { block, tx })));
                    }
                    Err(e) => {
                        last_err = Some(anyhow!(e));
                        continue;
                    }
                }
            }
        }

        if let Some(e) = last_err {
            return Err(anyhow!("decompress alkane wasm payload from metashrew: {e}"));
        }

        Ok(None)
    }

    pub fn get_alkane_wasm_bytes_with_db(
        &self,
        db: &rocksdb::DB,
        alkane: &SchemaAlkaneId,
    ) -> Result<Option<(Vec<u8>, SchemaAlkaneId)>> {
        let mut seen = HashSet::new();
        let res =
            self.load_wasm_inner(db, alkane.block as u128, alkane.tx as u128, &mut seen, 0)?;
        if let Some((bytes, sid)) = res {
            let block: u32 = sid
                .block
                .try_into()
                .map_err(|_| anyhow!("factory alkane block does not fit into u32"))?;
            let tx: u64 = sid
                .tx
                .try_into()
                .map_err(|_| anyhow!("factory alkane tx does not fit into u64"))?;
            Ok(Some((bytes, SchemaAlkaneId { block, tx })))
        } else {
            Ok(None)
        }
    }

    pub fn get_alkane_wasm_bytes(
        &self,
        alkane: &SchemaAlkaneId,
    ) -> Result<Option<(Vec<u8>, SchemaAlkaneId)>> {
        let db = get_metashrew_sdb();
        db.catch_up_now().context("metashrew catch_up before wasm fetch")?;
        self.get_alkane_wasm_bytes_with_db(db.as_db(), alkane)
    }
    pub fn get_alkanes_tip_height(&self) -> Result<u32> {
        let tip_height_prefix: Vec<u8> = self.apply_label(b"/__INTERNAL/tip-height".to_vec());

        self.read_uint_key::<4, u32>(tip_height_prefix)
    }


    /// Fetch all traces for a txid directly from the secondary DB, without needing block height.
    pub fn traces_for_tx(&self, txid: &Txid) -> Result<Vec<PartialEspoTrace>> {
        let db = get_metashrew_sdb();
        db.catch_up_now().context("metashrew catch_up before scanning traces_for_tx")?;

        let tx_be = txid.to_byte_array().to_vec();
        let mut tx_le = tx_be.clone();
        tx_le.reverse();

        // Metashrew stores individual events at /trace/<tx_le><vout_le>/<idx>,
        // so we assemble per-trace event lists here.
        let mut traces_by_outpoint: HashMap<
            Vec<u8>,
            BTreeMap<u64, BTreeMap<u64, AlkanesTraceEvent>>,
        > = HashMap::new();

        let parse_index = |bytes: &[u8]| -> Option<u64> {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(v) = s.parse::<u64>() {
                    return Some(v);
                }
            }

            match bytes.len() {
                4 => {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(bytes);
                    Some(u32::from_le_bytes(arr) as u64)
                }
                8 => {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    Some(u64::from_le_bytes(arr))
                }
                _ => None,
            }
        };

        for search_bytes in [&tx_be, &tx_le] {
            let mut prefix = b"/trace/".to_vec();
            prefix.extend_from_slice(search_bytes);
            let prefix = self.apply_label(prefix);

            let mut ro = ReadOptions::default();
            if let Some(ub) = self.next_prefix(prefix.clone()) {
                ro.set_iterate_upper_bound(ub);
            }
            ro.set_total_order_seek(true);

            let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
            while let Some(Ok((k, v))) = it.next() {
                if !k.starts_with(&prefix) {
                    break;
                }

                let suffix = &k[prefix.len()..];
                if suffix.len() < 5 || suffix[4] != b'/' {
                    continue;
                }

                let remainder = &suffix[5..];
                if remainder.is_empty() {
                    continue;
                }

                let segments: Vec<&[u8]> = remainder.split(|b| *b == b'/').collect();
                if segments.is_empty() || segments.iter().any(|s| *s == b"length") {
                    continue;
                }

                let (trace_idx_bytes, event_idx_bytes) = if segments.len() >= 2 {
                    (segments[0], segments[1])
                } else {
                    (&b"0"[..], segments[0])
                };

                let event_idx = match parse_index(event_idx_bytes) {
                    Some(idx) => idx,
                    None => continue,
                };
                let trace_idx = parse_index(trace_idx_bytes).unwrap_or(0);

                let vout_le: [u8; 4] = suffix[..4].try_into().unwrap_or_default();
                let mut outpoint: Vec<u8> = tx_le.clone();
                outpoint.extend_from_slice(&vout_le);

                if let Some(event) = decode_trace_event_blob(&v) {
                    let trace_map =
                        traces_by_outpoint.entry(outpoint.clone()).or_insert_with(BTreeMap::new);
                    let events = trace_map.entry(trace_idx).or_insert_with(BTreeMap::new);
                    events.entry(event_idx).or_insert(event);
                    continue;
                }

                if let Some(trace) = decode_trace_blob(&v) {
                    let trace_map =
                        traces_by_outpoint.entry(outpoint).or_insert_with(BTreeMap::new);
                    let events = trace_map.entry(trace_idx).or_insert_with(BTreeMap::new);
                    for (idx, ev) in trace.events.into_iter().enumerate() {
                        events.entry(idx as u64).or_insert(ev);
                    }
                }
            }
        }

        let mut out: Vec<PartialEspoTrace> = Vec::new();
        for (outpoint, trace_map) in traces_by_outpoint {
            for (_trace_idx, events_map) in trace_map {
                let events: Vec<AlkanesTraceEvent> =
                    events_map.into_iter().map(|(_, ev)| ev).collect();
                if events.is_empty() {
                    continue;
                }

                out.push(PartialEspoTrace {
                    protobuf_trace: AlkanesTrace { events },
                    outpoint: outpoint.clone(),
                });
            }
        }

        Ok(out)
    }

    pub fn get_latest_reserves_for_alkane(
        &self,
        who_alkane: &SchemaAlkaneId,
        what_alkane: &SchemaAlkaneId,
    ) -> Result<Option<u128>> {
        let u128_to_le16 = |x: u128| x.to_le_bytes();

        let enc_alkaneid_raw32 = |id: &SchemaAlkaneId| {
            let mut out = [0u8; 32];
            out[..16].copy_from_slice(&u128_to_le16(id.block as u128));
            out[16..].copy_from_slice(&u128_to_le16(id.tx as u128));
            out
        };

        let balance_prefix = |what: &SchemaAlkaneId, who: &SchemaAlkaneId| {
            let what32 = enc_alkaneid_raw32(what);
            let who32 = enc_alkaneid_raw32(who);
            let mut v = Vec::with_capacity(9 + 32 + 10 + 32);
            v.extend_from_slice(b"/alkanes/");
            v.extend_from_slice(&what32);
            v.extend_from_slice(b"/balances/");
            v.extend_from_slice(&who32);
            self.apply_label(v)
        };

        let prefix = balance_prefix(what_alkane, who_alkane);

        let mut length_key = prefix.clone();
        length_key.extend_from_slice(b"/length");

        let db = get_metashrew_sdb();

        let length_bytes = match db.get(&length_key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let length_str = std::str::from_utf8(&length_bytes)
            .map_err(|e| anyhow!("utf8 decode balances length: {e}"))?;
        let length: u64 = length_str
            .parse()
            .map_err(|e| anyhow!("parse balances length '{length_str}': {e}"))?;

        let Some(latest_idx) = length.checked_sub(1) else { return Ok(None) };

        let mut entry_key = prefix.clone();
        entry_key.push(b'/');
        entry_key.extend_from_slice(latest_idx.to_string().as_bytes());

        let entry_bytes = match db.get(&entry_key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let entry_str = std::str::from_utf8(&entry_bytes)
            .map_err(|e| anyhow!("utf8 decode balance entry: {e}"))?;
        let (height_str, hex_part) =
            entry_str.split_once(':').ok_or_else(|| anyhow!("balance entry missing ':'"))?;

        let _updated_height: u64 = height_str
            .parse()
            .map_err(|e| anyhow!("parse balance height '{height_str}': {e}"))?;

        let raw_balance = hex::decode(hex_part)
            .map_err(|e| anyhow!("hex decode balance payload '{hex_part}': {e}"))?;
        if raw_balance.len() != 16 {
            return Err(anyhow!("balance payload length {}, expected 16 bytes", raw_balance.len()));
        }
        let mut bal_bytes = [0u8; 16];
        bal_bytes.copy_from_slice(&raw_balance);

        Ok(Some(u128::from_le_bytes(bal_bytes)))
    }

    pub fn traces_for_block_as_prost(&self, block: u64) -> Result<Vec<PartialEspoTrace>> {
        let trace_block_prefix = |blk: u64| {
            let mut v = Vec::with_capacity(7 + 8 + 1);
            v.extend_from_slice(b"/trace/");
            v.extend_from_slice(&blk.to_le_bytes());
            v.push(b'/');
            self.apply_label(v)
        };

        let next_prefix = |mut p: Vec<u8>| -> Option<Vec<u8>> {
            for i in (0..p.len()).rev() {
                if p[i] != 0xff {
                    p[i] += 1;
                    p.truncate(i + 1);
                    return Some(p);
                }
            }
            None
        };

        let is_length_bucket = |key: &[u8], prefix: &[u8]| -> bool {
            if key.len() < prefix.len() + 1 + 4 {
                return false;
            }
            let bucket = &key[prefix.len()..key.len() - 4];
            bucket == b"length"
        };

        let db = get_metashrew_sdb();
        // Ensure the secondary view is fresh before scanning traces.
        db.catch_up_now().context("metashrew catch_up before scanning traces")?;
        let prefix = trace_block_prefix(block);

        let mut ro = ReadOptions::default();
        if let Some(ub) = next_prefix(prefix.clone()) {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut outpoints: Vec<Vec<u8>> = Vec::new();

        while let Some(Ok((k, v))) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }

            if is_length_bucket(&k, &prefix) {
                continue;
            }

            let suffix = &k[prefix.len()..];
            let parts: Vec<&[u8]> = suffix.split(|b| *b == b'/').collect();
            if parts.len() != 2 {
                continue;
            }

            if parts[1] == b"length" {
                continue;
            }

            let trace_idx = match std::str::from_utf8(parts[1]) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let val_str = std::str::from_utf8(&v)
                .map_err(|e| anyhow!("utf8 decode trace pointer for block {block}: {e}"))?;
            let (_block_str, hex_part) = val_str
                .split_once(':')
                .ok_or_else(|| anyhow!("trace pointer missing ':' for block {block}"))?;

            let hex_bytes = hex::decode(hex_part)
                .map_err(|e| anyhow!("hex decode trace pointer for block {block}: {e}"))?;
            if hex_bytes.len() < 36 {
                continue;
            }

            let (tx_be, vout) = hex_bytes.split_at(32);

            let mut key = Vec::with_capacity(7 + tx_be.len() + vout.len() + 1 + trace_idx.len());
            key.extend_from_slice(b"/trace/");
            key.extend_from_slice(tx_be);
            key.extend_from_slice(vout);
            key.push(b'/');
            key.extend_from_slice(trace_idx.as_bytes());
            keys.push(self.apply_label(key));

            // Pointer payload stores txid in little-endian already; keep as-is for outpoint.
            let mut outpoint = tx_be.to_vec();
            outpoint.extend_from_slice(vout);
            outpoints.push(outpoint);
        }

        let values = db.multi_get(keys.iter())?;

        let traces: Vec<PartialEspoTrace> = values
            .into_iter()
            .zip(outpoints.iter())
            .filter_map(|(maybe_bytes, outpoint)| {
                maybe_bytes.as_deref().and_then(decode_trace_blob).map(|protobuf_trace| {
                    PartialEspoTrace { protobuf_trace, outpoint: outpoint.clone() }
                })
            })
            .collect();
        if traces.is_empty() {
            eprintln!(
                "[metashrew] block {block}: pointers={} keys={} traces=0",
                outpoints.len(),
                keys.len()
            );
        }
        Ok(traces)
    }
}
