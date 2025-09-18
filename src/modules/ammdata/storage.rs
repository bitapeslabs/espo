use crate::modules::ammdata::consts::KEY_INDEX_HEIGHT;
use crate::modules::ammdata::main::AmmData;
impl AmmData {
    pub fn get_key_index_height() -> &'static [u8] {
        KEY_INDEX_HEIGHT
    }
}
use crate::modules::ammdata::schemas::{SchemaAlkaneId, SchemaFullCandleV1, Timeframe};
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
