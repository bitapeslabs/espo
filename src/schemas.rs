use alkanes_support::proto::alkanes::{AlkaneId, Uint128};
use anyhow::{Context, Result, anyhow};
use borsh::{BorshDeserialize, BorshSerialize};
use protobuf::SpecialFields;

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
    Uint128 { lo, hi, special_fields: SpecialFields::new() }
}

/* ---------- AlkaneId -> SchemaAlkaneId ---------- */

impl TryInto<SchemaAlkaneId> for AlkaneId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<SchemaAlkaneId> {
        let b = self
            .block
            .as_ref()
            .context("Schema error: missing block on AlkaneId -> SchemaAlkaneId")?;
        let t = self
            .tx
            .as_ref()
            .context("Schema error: missing tx on AlkaneId -> SchemaAlkaneId")?;

        // Correct recomposition: (hi << 64) | lo
        let block128 = u128_from_uint128(b);
        let tx128 = u128_from_uint128(t);

        // Enforce fit to schema (u32/u64)
        if block128 > (u32::MAX as u128) {
            return Err(anyhow!("Schema error: block does not fit into u32: {block128}"));
        }
        if tx128 > (u64::MAX as u128) {
            return Err(anyhow!("Schema error: tx does not fit into u64: {tx128}"));
        }

        Ok(SchemaAlkaneId { block: block128 as u32, tx: tx128 as u64 })
    }
}

/* ---------- SchemaAlkaneId -> AlkaneId ---------- */

impl TryFrom<SchemaAlkaneId> for AlkaneId {
    type Error = anyhow::Error;

    fn try_from(value: SchemaAlkaneId) -> Result<Self> {
        // Promote to u128 then split to {lo, hi} via LE
        let block128 = value.block as u128;
        let tx128 = value.tx as u128;

        let block_u = uint128_from_u128_le(block128);
        let tx_u = uint128_from_u128_le(tx128);

        Ok(AlkaneId {
            block: protobuf::MessageField::some(block_u),
            tx: protobuf::MessageField::some(tx_u),
            special_fields: SpecialFields::new(),
        })
    }
}
