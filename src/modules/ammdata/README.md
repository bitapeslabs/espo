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

### TVL lines

Historical TVL is served as a line series, in the same shape as the candle
endpoints, by `ammdata.get_tvl_candles`.

#### How a pool is valued

Each pool contributes one *anchored* value: the side of it we can price without
guessing, doubled, since a constant-product pool holds equal value on both sides.
The side is chosen in this order, quote before base, matching `pool_volume_side`:

| Case | Bucket | Meaning |
| --- | --- | --- |
| Pool has a canonical quote (frBTC; BUSD before height 946500) | `canonical_sats` | Exact, no price feed involved |
| One side has a canonical-rooted sats price | `derived_sats` | Priced through that side's canonical pool |
| Neither | `unanchored_sats` | Both sides priced off the USD token feed and summed |

The three buckets are stored separately so a chart can drop the ones it does not
trust. `sats` in the RPC response excludes `unanchored_sats` unless
`include_unanchored` is passed.

Doubling is exact only while a pool is balanced, so an anchored value will
disagree with `pool_tvl_usd` on a heavily skewed or freshly-donated pool.

#### Why the series is stored in sats

The stored level is canonical sats, never USD. The USD column is produced at read
time by multiplying each bucket against the btc/usd line of the same bucket. That
keeps a move in the BTC price from rewriting history, and it separates the two
questions a TVL chart gets asked: `sats` is liquidity growth on its own, `usd` is
liquidity growth plus BTC appreciation. Buckets the btc/usd line does not cover
report `usd` and `btc_usd` as null rather than zero.

TVL is a level rather than a flow, so a bucket with no write means nothing moved
and the previous value is carried forward, both across interior gaps and from the
last written bucket up to now.

Because the level is in sats, a move in the BTC price does not change a pool's
`canonical_sats` at all, so idle pools need no revaluation. Only `derived_sats`
and `unanchored_sats` are frozen at the valuation of the pool's last touch, since
those depend on a price feed.

#### Keys

```
ammdata/atl1:<tf>:<bucket_ts>                 -> SchemaTvlPointV1  (AMM-wide line)
ammdata/ttl1:<blk_hex>:<tx_hex>:<tf>:<bucket_ts>
                                              -> SchemaTvlPointV1  (per-token line)
ammdata/amm_tvl_total/v1/<height BE u64>      -> SchemaTvlPointV1  (running AMM total)
ammdata/token_tvl_total/v1/<token>/<height BE u64>
                                              -> SchemaTvlPointV1  (running token total)
ammdata/pool_tvl_anchor/v1/<pool>             -> SchemaTvlPointV1  (pool's last contribution)
```

A per-token line counts the full anchored value of each pool the token appears
in, so per-token lines do not sum to the AMM-wide line - the counter-asset is in
both.

#### Maintenance

`index_tvl::prepare_tvl_lines` runs per block and is O(pools touched), not
O(all pools): each pool's last contribution is kept in `pool_tvl_anchor`, so a
block applies deltas to the running totals instead of re-summing. The totals are
keyed by height and read back at-or-before `height - 1` through the versioned
store, the same shape `total_volume_amm` uses, so a reorg that drops a block
falls back to the last surviving total on its own. All writes go out in the
normal `index_finalize` batch.

#### No backfill

The lines are **forward-only**. They begin at the block the index was first
written at and accrue from there; there are no points before that, and nothing
regenerates them. That is deliberate - see the indexer rules in the repo's
CLAUDE.md. A backfill would have to run inside `index_block`, which stalls the
indexer until it completes, and one per index upgrade would be unmaintainable.

Nothing here reads or rewrites an existing keyspace: the series lives entirely in
the new keys above, so deploying it cannot affect an index already on disk.
