use super::schemas::{SchemaPoolSnapshot, SchemaReservesSnapshot};
use crate::modules::ammdata::consts::KEY_INDEX_HEIGHT;
use crate::modules::ammdata::main::AmmData;
use anyhow::Result;
use borsh::BorshDeserialize;
use std::collections::{BTreeMap, HashMap};
impl AmmData {
    pub fn get_key_index_height() -> &'static [u8] {
        KEY_INDEX_HEIGHT
    }
}
use crate::schemas::SchemaAlkaneId;

use crate::modules::ammdata::schemas::{SchemaFullCandleV1, Timeframe};
use borsh::BorshSerialize;

/// Hex without "0x", lowercase
#[inline]
pub fn to_hex_no0x<T: Into<u128>>(x: T) -> String {
    format!("{:x}", x.into())
}

/// Namespace prefix for iterating all candles for (pool, timeframe)
/// Example: "fc1:2:11070:1h:"
pub fn candle_ns_prefix(pool: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
    let blk_hex = format!("{:x}", pool.block);
    let tx_hex = format!("{:x}", pool.tx);
    format!("fc1:{}:{}:{}:", blk_hex, tx_hex, tf.code()).into_bytes()
}

/// Deserialize helper
pub fn decode_full_candle_v1(bytes: &[u8]) -> anyhow::Result<SchemaFullCandleV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaFullCandleV1::try_from_slice(bytes)?)
}

/// Encode helper
pub fn encode_full_candle_v1(v: &SchemaFullCandleV1) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    v.serialize(&mut out)?;
    Ok(out)
}
pub fn candle_key_seq(pool: &SchemaAlkaneId, tf: Timeframe, bucket_ts: u64, seq: u32) -> Vec<u8> {
    // assuming your existing candle_ns_prefix(pool, tf) = b"fc1:<blk>:<tx>:<tf>:"
    // final key: prefix + "<bucket_ts>:<seq>"
    let mut k = candle_ns_prefix(pool, tf);
    k.extend_from_slice(bucket_ts.to_string().as_bytes());
    k.push(b':');
    k.extend_from_slice(seq.to_string().as_bytes());
    k
}

// Backward-compat: keep the old single-snapshot key
pub fn candle_key(pool: &SchemaAlkaneId, tf: Timeframe, bucket_ts: u64) -> Vec<u8> {
    let mut k = candle_ns_prefix(pool, tf);
    k.extend_from_slice(bucket_ts.to_string().as_bytes());
    k
}

pub fn pools_key(pool: &SchemaAlkaneId) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + 4 + 8);
    k.extend_from_slice(b"/pools/");
    k.extend_from_slice(&pool.block.to_be_bytes());
    k.extend_from_slice(&pool.tx.to_be_bytes());
    k
}

// Encode Snapshot -> BORSH (deterministic order via BTreeMap)
pub fn encode_reserves_snapshot(
    map: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
) -> Result<Vec<u8>> {
    let ordered: BTreeMap<SchemaAlkaneId, SchemaPoolSnapshot> =
        map.iter().map(|(k, v)| (*k, v.clone())).collect();
    let snap = SchemaReservesSnapshot { entries: ordered };
    Ok(borsh::to_vec(&snap)?)
}

// Decode BORSH -> Snapshot
pub fn decode_reserves_snapshot(
    bytes: &[u8],
) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let snap = SchemaReservesSnapshot::try_from_slice(bytes)?;
    Ok(snap.entries.into_iter().collect())
}

#[inline]
pub fn reserves_snapshot_key() -> &'static [u8] {
    b"/reserves_snapshot_v1" // v1 as agreed
}
