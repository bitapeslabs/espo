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
ammdata/backfill/tvl_line/v1                  -> u128 LE           (backfill completion marker)
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

#### Backfill

`backfill_tvl::maybe_backfill_tvl_lines` gives the lines their history **without
re-running traces**. Everything it needs is already indexed:

* essentials keeps a per-height log of which alkanes' balances moved and by how
  much - the same feed the live indexer replays to track reserves;
* ammdata keeps btc/usd per height, the canonical-pool candles that give a token
  its price in sats, and the per-token USD candles.

So it is a single forward walk over the height range carrying the same running
totals the live path maintains. It runs once, guarded by the marker key above,
from inside `index_block` before the block's own work, and writes the marker only
on completion - an interrupted run starts over. Set `tvl_line_backfill: false` in
the ammdata module config to skip it and leave the lines forward-only.

One known gap: the backfill recovers `unanchored_sats` from the per-token USD
candle series, which only exists for tokens that were ever priced. A pool whose
tokens have no USD candle history contributes nothing to the backfilled
`unanchored_sats`, where the live path would have valued it from token metrics.
`canonical_sats` and `derived_sats` are unaffected.
