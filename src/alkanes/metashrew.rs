use crate::alkanes::trace::PartialEspoTrace;
use crate::config::get_metashrew_sdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use bitcoin::hashes::Hash;
use bitcoin::Txid;

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

        let mut out: Vec<PartialEspoTrace> = Vec::new();

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

                let vout_le: [u8; 4] = suffix[..4].try_into().unwrap_or_default();
                let mut outpoint: Vec<u8> = tx_le.clone();
                outpoint.extend_from_slice(&vout_le);

                if let Some(protobuf_trace) = decode_trace_blob(&v) {
                    out.push(PartialEspoTrace { protobuf_trace, outpoint });
                }
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
