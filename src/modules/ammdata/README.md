## ESPO ammdata module

The ammdata espo module generates OHLCV data and tradehistory from traces from oylamm.

### rocksdb schema

SchemaAlkaneId -> Borsh
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}



ammdata/block_index -> blockNumber(u64 LE) : The blockNumber of the last proccessed block for the ammdata indexer
ammdata/ohlcv/<alkanePoolId>/<interval>/<timestamp> -> ohlcv data
ammdata/tradehistory/<poolId>/<timestamp> -> trade data