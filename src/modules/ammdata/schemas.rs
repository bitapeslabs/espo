use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy, Eq, PartialOrd, Ord)]
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
    pub volume: u128,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaFullCandleV1 {
    pub base_candle: SchemaCandleV1,
    pub quote_candle: SchemaCandleV1,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy)]
pub struct SchemaMarketDefs {
    pub base_alkane_id: SchemaAlkaneId,
    pub quote_alkane_id: SchemaAlkaneId,
    pub pool_alkane_id: SchemaAlkaneId,
}
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum TradeSide {
    Sell,
    Buy,
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct SchemaTradeV1 {
    pub timestamp: u64,
    pub txid: [u8; 32],
    pub address: String,
    pub xpubkey: [u8; 32],
    pub base_inflow: i128,
    pub quote_inflow: i128,
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct SchemaFullTradeV1 {
    pub base_trade: SchemaTradeV1,
    pub quote_trade: SchemaTradeV1,
}

impl SchemaCandleV1 {
    pub fn from_price_and_vol(
        open: u128,
        high: u128,
        low: u128,
        close: u128,
        volume: u128,
    ) -> Self {
        Self { open, high, low, close, volume }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Timeframe {
    M10,
    H1,
    D1,
    W1,
    M1,
}

impl Timeframe {
    #[inline]
    pub fn duration_secs(&self) -> u64 {
        match self {
            Timeframe::M10 => 10 * 60,
            Timeframe::H1 => 60 * 60,
            Timeframe::D1 => 24 * 60 * 60,
            Timeframe::W1 => 7 * 24 * 60 * 60,
            Timeframe::M1 => 30 * 24 * 60 * 60, // simple month bucket (30d)
        }
    }
    /// Short ASCII code used in keys (keeps keys compact & lexicographically nice)
    #[inline]
    pub fn code(&self) -> &'static str {
        match self {
            Timeframe::M10 => "10m",
            Timeframe::H1 => "1h",
            Timeframe::D1 => "1d",
            Timeframe::W1 => "1w",
            Timeframe::M1 => "1M",
        }
    }
}
pub fn active_timeframes() -> Vec<Timeframe> {
    vec![Timeframe::M10, Timeframe::H1, Timeframe::D1, Timeframe::W1, Timeframe::M1]
}
