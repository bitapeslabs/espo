use alkanes_support::proto::alkanes::{AlkaneId, Uint128};
use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use protobuf::SpecialFields;

#[derive(
    BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy, Eq, PartialOrd, Ord, Hash,
)]
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}

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

        // In your TryFrom, you set:
        //   hi = low 64 bits, lo = high 64 bits (from u128::to_le_bytes).
        // So to reconstruct u128 we do: (lo << 64) | hi.
        let block: u128 = ((b.lo as u128) << 64) | (b.hi as u128);
        let tx: u128 = ((t.lo as u128) << 64) | (t.hi as u128);

        Ok(SchemaAlkaneId {
            block: block
                .try_into()
                .context("Schema error: Failed to parse block in AlkaneId -> SchemaAlkaneId")?,
            tx: tx
                .try_into()
                .context("Schema error: Failed to parse tx in AlkaneId -> SchemaAlkaneId")?,
        })
    }
}
impl TryFrom<SchemaAlkaneId> for AlkaneId {
    type Error = anyhow::Error;
    fn try_from(value: SchemaAlkaneId) -> Result<Self> {
        let hi_block =
            u64::from_le_bytes((value.block as u128).to_le_bytes()[..8].try_into().context(
                "Schema error: Failed to unwrap hi block on SchemaAlkaneId -> AlkaneId",
            )?);
        let lo_block =
            u64::from_le_bytes((value.block as u128).to_le_bytes()[8..16].try_into().context(
                "Schema error: Failed to unwrap lo block on SchemaAlkaneId -> AlkaneId",
            )?);

        let hi_tx = u64::from_le_bytes(
            (value.tx as u128).to_le_bytes()[..8]
                .try_into()
                .context("Schema error: Failed to unwrap hi tx on SchemaAlkaneId -> AlkaneId")?,
        );
        let lo_tx = u64::from_le_bytes(
            (value.tx as u128).to_le_bytes()[..8]
                .try_into()
                .context("Schema error: Failed to unwrap lo tx on SchemaAlkaneId -> AlkaneId")?,
        );

        Ok(AlkaneId {
            block: protobuf::MessageField::some(Uint128 {
                hi: hi_block,
                lo: lo_block,
                special_fields: SpecialFields::new(),
            }),
            tx: protobuf::MessageField::some(Uint128 {
                hi: hi_tx,
                lo: lo_tx,
                special_fields: SpecialFields::new(),
            }),
            special_fields: SpecialFields::new(),
        })
    }
}
