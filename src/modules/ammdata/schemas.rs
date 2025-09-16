use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaCandleV1 {
    pub open: u128,
    pub high: u128,
    pub low: u128,
    pub close: u128,
    pub volume_quote: u128,
    pub volume_base: u128,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaMarketDefs {
    pub base_alkane_id: SchemaAlkaneId,
    pub quote_alkane_id: SchemaAlkaneId,
    pub pool_alkane_id: SchemaAlkaneId,
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct SchemaTradeV1 {
    pub timestamp: u64,
    pub txid: [u8; 32],
    pub address: String,
    pub base_in: u128,
    pub base_out: u128,
    pub quote_in: u128,
    pub quote_out: u128,
    pub price_after: u128,
}
impl SchemaCandleV1 {
    pub fn from_price_and_vol(
        open: u128,
        high: u128,
        low: u128,
        close: u128,
        vol_q: u128,
        vol_b: u128,
    ) -> Self {
        Self { open, high, low, close, volume_quote: vol_q, volume_base: vol_b }
    }
}
