use crate::alkanes::trace::PartialEspoTrace;
use crate::config::get_metashrew_sdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};

#[derive(Debug, Clone, Copy)]
struct DecodedVal {
    balance: Option<u128>,
    updated: Option<u32>,
}
#[inline]
fn decode_balance_value(bytes: &[u8]) -> DecodedVal {
    match bytes.len() {
        20 => {
            let mut bal = [0u8; 16];
            bal.copy_from_slice(&bytes[..16]);
            let mut ts = [0u8; 4];
            ts.copy_from_slice(&bytes[16..20]);
            DecodedVal {
                balance: Some(u128::from_le_bytes(bal)),
                updated: Some(u32::from_le_bytes(ts)),
            }
        }
        16 => {
            let mut bal = [0u8; 16];
            bal.copy_from_slice(bytes);
            DecodedVal { balance: Some(u128::from_le_bytes(bal)), updated: None }
        }
        8 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            DecodedVal { balance: Some(u64::from_le_bytes(a) as u128), updated: None }
        }
        4 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            DecodedVal { balance: Some(u32::from_le_bytes(a) as u128), updated: None }
        }
        _ => DecodedVal { balance: None, updated: None },
    }
}

fn try_decode_prost(raw: &[u8]) -> Option<AlkanesTrace> {
    AlkanesTrace::decode(raw).ok().or_else(|| {
        if raw.len() >= 4 { AlkanesTrace::decode(&raw[..raw.len() - 4]).ok() } else { None }
    })
}

/// Traces can be stored as raw protobuf bytes or as UTF-8 "height:HEX" blobs.
/// This helper handles both by decoding any hex payload and stripping the
/// optional 4-byte trailer some entries carry.
pub fn decode_trace_blob(bytes: &[u8]) -> Option<AlkanesTrace> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_block, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                if let Some(trace) = try_decode_prost(&decoded) {
                    return Some(trace);
                }
            }
        }
    }

    try_decode_prost(bytes)
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
        MetashrewAdapter { label }
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
    pub fn get_alkanes_tip_height(&self) -> Result<u32> {
        let tip_height_prefix: Vec<u8> = self.apply_label(b"/__INTERNAL/tip-height".to_vec());

        self.read_uint_key::<4, u32>(tip_height_prefix)
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

        let prefix = balance_prefix(what_alkane, who_alkane);

        let db = get_metashrew_sdb();

        let mut ro = ReadOptions::default();
        if let Some(ub) = next_prefix(prefix.clone()) {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);

        let mut best_updated: Option<u32> = None;
        let mut best_slot: u32 = 0;
        let mut best_balance: Option<u128> = None;

        while let Some(Ok((k, v))) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }
            // key suffix = slot u32 LE (bytes 32..36 of WHO segment), but we don't need to parse WHO again
            if k.len() < prefix.len() + 4 {
                continue;
            }
            let mut slot_bytes = [0u8; 4];
            slot_bytes.copy_from_slice(&k[prefix.len()..prefix.len() + 4]);
            let slot = u32::from_le_bytes(slot_bytes);

            let dec = decode_balance_value(&v);
            if let Some(bal) = dec.balance {
                // prefer higher updated; if none available, prefer higher slot
                let better = match (best_updated, dec.updated) {
                    (None, Some(_)) => true,
                    (Some(prev), Some(cur)) if cur > prev => true,
                    (Some(prev), Some(cur)) if cur == prev && slot > best_slot => true,
                    (None, None) if slot > best_slot => true,
                    (None, None) => best_balance.is_none(), // first one wins
                    _ => false,
                };
                if better {
                    best_updated = dec.updated;
                    best_slot = slot;
                    best_balance = Some(bal);
                }
            }
        }

        Ok(best_balance)
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
