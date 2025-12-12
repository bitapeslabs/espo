use crate::alkanes::trace::PartialEspoTrace;
use crate::config::get_metashrew_sdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::{Result, anyhow};
use protobuf::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};

/// Value layout in Metashrew balances:
/// - 20 bytes: [0..16) = balance u128 LE, [16..20) = updated u32 LE
/// - 16 bytes: balance u128 LE (no updated)
/// - 8/4 bytes: occasionally compacted values (we'll decode as u64/u32)
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

    pub fn traces_for_block_as_protobuf(&self, block: u64) -> Result<Vec<PartialEspoTrace>> {
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

        let outpoint_bytes_to_key = |bytes: &Vec<u8>| {
            let mut key = Vec::with_capacity(7 + bytes.len());
            key.extend_from_slice(b"/trace/");
            key.extend_from_slice(bytes);
            self.apply_label(key)
        };
        let parse_trace_maybe_with_trailer = |mut bytes: Vec<u8>| -> Option<AlkanesTrace> {
            AlkanesTrace::parse_from_bytes(&bytes).ok().or_else(|| {
                if bytes.len() >= 4 {
                    bytes.truncate(bytes.len() - 4);
                    AlkanesTrace::parse_from_bytes(&bytes).ok()
                } else {
                    None
                }
            })
        };

        let db = get_metashrew_sdb();
        let prefix = trace_block_prefix(block);

        let mut ro = ReadOptions::default();
        if let Some(ub) = next_prefix(prefix.clone()) {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
        let mut outpoints = Vec::new();

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
            outpoints.push(v[..36].to_vec());
        }

        let keys: Vec<Vec<u8>> = outpoints.iter().map(outpoint_bytes_to_key).collect();

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
}
