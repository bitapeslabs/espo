use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaCandleV1 {
    open: u128,
    high: u128,
    low: u128,
    close: u128,
    volume_quote: u128,
    volume_base: u128,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaMarketDefs {
    base_alkane_id: u128,
    quote_alkane_id: u128,
    pool_alkane_id: u128,
}
