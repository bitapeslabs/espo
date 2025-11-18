use alkanes_support::proto::alkanes::{AlkaneId, Uint128};
use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use protorune_support::balance_sheet::IntoString;
use std::fmt;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Debug,
    Clone,
    Copy,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
)]
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}

/* ---------- helpers ---------- */

#[inline]
fn u128_from_uint128(u: &Uint128) -> u128 {
    // lo = lower 64 bits, hi = upper 64 bits
    ((u.hi as u128) << 64) | (u.lo as u128)
}

#[inline]
fn uint128_from_u128_le(x: u128) -> Uint128 {
    // split using LE bytes: [0..8] => lo, [8..16] => hi
    let bytes = x.to_le_bytes();
    let lo = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    Uint128 { lo, hi }
}

impl TryInto<SchemaAlkaneId> for AlkaneId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<SchemaAlkaneId> {
        let prost_block_u128 = self
            .block
            .as_ref()
            .context("Schema error: missing block on AlkaneId -> SchemaAlkaneId")?;
        let prost_tx_u128 = self
            .tx
            .as_ref()
            .context("Schema error: missing tx on AlkaneId -> SchemaAlkaneId")?;

        let block: u32 = u128_from_uint128(prost_block_u128).try_into().unwrap_or(u32::MAX);
        let tx: u64 = u128_from_uint128(prost_tx_u128).try_into().unwrap_or(u64::MAX);

        Ok(SchemaAlkaneId { block, tx })
    }
}

impl TryFrom<SchemaAlkaneId> for AlkaneId {
    type Error = anyhow::Error;

    fn try_from(value: SchemaAlkaneId) -> Result<Self> {
        // Promote to u128 then split to {lo, hi} via LE
        let block128 = value.block as u128;
        let tx128 = value.tx as u128;

        let block_u = uint128_from_u128_le(block128);
        let tx_u = uint128_from_u128_le(tx128);

        Ok(AlkaneId { block: Some(block_u), tx: Some(tx_u) })
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Eq, Hash, Default)]
pub struct EspoOutpoint {
    pub txid: Vec<u8>, // BE bytes
    pub vout: u32,
}

impl EspoOutpoint {
    pub fn as_outpoint_string(&self) -> String {
        let mut reversed_txid_bytes = self.txid.clone();
        reversed_txid_bytes.reverse();
        format!("{}:{}", reversed_txid_bytes.to_str(), self.vout)
    }
}

impl fmt::Display for EspoOutpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.txid.to_str(), self.vout)
    }
}
impl fmt::Display for SchemaAlkaneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // format as "block:tx" (decimal), e.g. "2:0"
        write!(f, "{}:{}", self.block, self.tx)
    }
}
