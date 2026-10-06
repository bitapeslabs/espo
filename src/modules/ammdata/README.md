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
ammdata/atl2:<tf>:<bucket_ts>                 -> SchemaTvlPointV1  (AMM-wide line)
ammdata/ttl2:<blk_hex>:<tx_hex>:<tf>:<bucket_ts>
                                              -> SchemaTvlPointV1  (per-token line)
ammdata/amm_tvl_total/v2/<height BE u64>      -> SchemaTvlPointV1  (running AMM total)
ammdata/token_tvl_total/v2/<token>/<height BE u64>
                                              -> SchemaTvlPointV1  (running token total)
ammdata/pool_tvl_anchor/v2/<pool>             -> SchemaTvlPointV1  (pool's last contribution)
```

**The `v1` namespaces (`atl1:`, `ttl1:`, `/amm_tvl_total/v1/`, `/token_tvl_total/v1/`,
`/pool_tvl_anchor/v1/`, `/backfill/tvl_line/v1`) exist on prod and are stale.** They
were written with the derived leg inflated by 1e8 (a `PRICE_SCALE`-scaled candle
price divided by `AMOUNT_SCALE`). Nothing reads them and nothing deletes them:
deleting keys in prod is dangerous, writing new ones is not, so the fix moved to a
fresh namespace and left the old one in place.

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

#### History and the one-time backfill

The lines are **forward-only** in this codebase: the live path alone populates
them from the block the index is deployed at, and nothing here regenerates
history. That is the repo rule (see "Indexer changes" in CLAUDE.md); a backfill
runs inside `index_block`, stalls the indexer until it finishes, and one per
index upgrade would be unmaintainable.

The v2 series on prod does have history, from a one-time backfill that was
explicitly requested and run once on 2026-09-25. It replayed the essentials
per-height balance log from the ammdata genesis (no traces re-run), took 22
seconds for 63,824 heights, and was validated against live reserves: the
backfilled `canonical_sats` matched `2 x sum(frBTC reserves)` from the reserves
snapshot to within 0.000%. The code was removed afterwards. Two things it left
behind, both harmless and both left alone: the `v1` keyspaces above, and the
marker key `ammdata/backfill/tvl_line/v2` (u128 LE, the height it completed
through). Nothing reads either.

One known gap in that backfilled history: `unanchored_sats` before the deploy
block was recovered from the per-token USD candle series, which only exists for
tokens that were ever priced, so pools whose tokens have no USD candle history
contributed nothing to it. `canonical_sats` and `derived_sats` are unaffected.

### cmcap (circulating market cap)

`get_candles` with `pool: "<token>-cmcap"` (or `<token>-derived_<quote>-cmcap`).

The stored mcap candles (`tmc1:` / `tdmc1:`) are price x the supply essentials
tracks, which counts every minted token - tokens in a vesting contract, a
treasury or a burn sink included. For many tokens that is FDV. cmcap is that
series scaled at request time:

```
cmcap = mcap x (supply - non_circulating) / supply
```

`non_circulating` is the sum of balances held by the holders listed for the
token in `MAINNET_NON_CIRCULATING` (`consts.rs`). It is a network fact - which
contracts are lockers, which address is the treasury - so it lives in consts
next to `canonical_quotes`, not in config. LP pools are not listed: pooled
liquidity is circulating. Mainnet today:

| token | non-circulating alkanes | non-circulating addresses |
| --- | --- | --- |
| DIESEL `2:0` | - | `bc1phqvgwn7...g8umr8` |
| TORTILLA `2:68479` | `2:68478` (deployer) | `1A1zP1eP5Q...DivfNa` (burn) |
| SLICE `4:8888` | `4:53`, `4:54`, `4:55`, `4:56` (lockers) | `bc1pz860hk...mzyjer` (treasury) |

Other networks have no entries, so `-cmcap` equals `-mcusd` there.

#### Why it needs no index

The ratio is a step function: it only moves when a listed holder's balance
moves, and essentials already records exactly when that happens.

| Holder | Trigger heights from | Balance at a trigger from |
| --- | --- | --- |
| alkane | its `balance_by_height` list for the token | the `balance_by_height` row |
| address | its outpoint list: every outpoint carrying the token, at its creation block and (if spent) its spend block | the outpoint's own amount - `+` at creation, `-` at spend. No per-height balance reads; outpoints carrying other tokens are one small decode and nothing more |

The heights are unioned, each holder's balance is carried forward across them
(one cursor per holder), and a height only becomes a step if the total actually
moved - supply is read and the block time looked up per step, not per holder
event. A request is a few small list reads plus one supply read per real change,
not a walk over the chart's history. An address with more than 50,000 outpoints
is refused rather than truncated, since that is not a treasury and a partial
history would make the chart lie.

Why outpoints and not the address's tx ledger: the first version used the
`AlkaneTxs` ledger and a pinned balance read per height. For TORTILLA's burn
address, which receives burns of two dozen other tokens, that meant 1,291 steps
and a 2.7 s request, each step loading a trace-carrying tx blob for a height
where TORTILLA had not moved. The outpoint form only touches outpoints that
carry the token, and needs no historical reads at all.

Applying it: a candle takes the last step at or before the end of its bucket.
Buckets before the first step get ratio 1 (nothing was locked yet). Volume is
never scaled - it is trade volume. Each candle carries `circulating_ratio`; the
response carries `cmcap: { configured, holders, steps }`. With no entry for the
token the candles equal `-mcusd` and the ratio is 1.

#### Known approximation

Supply is only read at trigger heights, so a token that mints continuously
(DIESEL) without any listed holder moving keeps the ratio from its last trigger
while supply grows. The mcap candle itself does reflect the new supply, so the
error is confined to the ratio. If that matters for a token, the fix is to add
the supply-change heights as triggers too - still no index, just more point
reads - or to divide out supply per bucket from the stored price candle.

### Derived-chart forks

`MAINNET_DERIVED_QUOTE_FORKS` (`consts.rs`) lists (token, derived quote, height)
triples. From the height on, the token's `-derived_<quote>-usd` chart no longer
registers the quote's pool as its derived pool, so the quote's price moves stop
being written into new candles and the chart follows the token's direct USD
pricing - the same values `-usd` would give. History before the height is left
exactly as it was indexed; the fork changes what gets written from then on, so
it needs no reindex.

Mechanically it is one exclusion when the derived-pool map is built in
`derive_token_data`. Everything downstream - bucket triggers, the per-bucket
merge, higher-timeframe canonicalization, derived mcap, derived metrics - keys
off that map, and all of it already has a "no derived pool for this pair"
branch that mirrors the direct USD series. The first post-fork candle's open is
still anchored to the last derived close (`apply_open`), so the series is
continuous even though its level steps to the frBTC-only price.

| token | quote | height |
| --- | --- | --- |
| TORTILLA `2:68479` | DIESEL `2:0` | 969393 |

This sits beside the older `BUSD_CANONICAL_QUOTE_FORK_HEIGHT` (946500), which
removed BUSD as a canonical quote the same way.

### Forkable derived liquidity, `-full`, and `derive_sources`

```json
"derived_liquidity": [
  { "alkane": "2:0",     "strategy": "neutral-vwap" },
  { "alkane": "2:68479", "strategy": "neutral-vwap", "height": 969925 }
],
"derive_sources": [
  { "alkane": "2:77", "target": "2:68479" },
  { "alkane": "2:68479", "target": "2:0",
    "drop_forks": [ { "alkane": "2:0", "height": 970000 } ],
    "start_offset": 960000 }
]
```

The first `derived_liquidity` entry is the **default** derived quote.

#### `height` on a derived quote

Without a height an entry acts as derived liquidity from the ammdata genesis, as
before. With one, the alkane is invisible to indexing until that block
(`AmmDataConfig::derived_index_plan(height)` hands `derive_token_data` only the
quotes in force), and from it on it is a derived quote like any other.

It is also a **fork**. A token's chart against the new quote does not start
empty: the first time the pair is about to get a candle, the token's usd and
mcusd history against the default quote is copied into the pair's namespace, on
every timeframe (`AmmDataProvider::fork_token_derived_history`). For the default
quote's own token, which has no chart against itself, its direct usd/mcusd
charts are what gets carried over.

This is the one place the module copies history, and it does so because it was
asked for explicitly. It is held to the rules for that in CLAUDE.md:

* **once per pair**, behind `token_derived_fork/v1/<token>/<quote>`, written in
  the same batch as the copy;
* **new keys only** - a destination that already holds candles is left exactly
  as it is and just marked, and nothing is ever deleted or moved (the default
  quote's series stays and keeps being indexed);
* **lazy** - per token, on its first trade after the height, not a walk over
  every token at the fork block;
* **in-block** - the write lands in the block being indexed, so the rest of that
  block (previous close, anchors, higher-timeframe aggregation, metrics) reads it
  back, and a reorg removes the copy and the marker together.

Cost to expect: one read of the token's default-quote series per timeframe and
the same number of puts, on that token's first post-fork trade. A token with a
DIESEL pool has a candle in most 10m buckets, so that can be tens of thousands
of keys in the block where it first trades; a block where several such tokens
trade for the first time will be noticeably slower than usual, once.

#### `-full`

`<alkane>-full` (usd) and `<alkane>-full-<kind>` for `usd`, `sats`, `mcusd`,
`mcsats`, `cmcap`. It is rewritten before parsing, so it is served by exactly the
code that serves the chart it proxies to:

| situation | `<alkane>-full` serves |
| --- | --- |
| alkane has a `derive_sources` target that is in force, and the pair has forked (or the target has no height) | `<alkane>-derived_<target>-usd` |
| no entry, target not in force yet, or the pair has not forked yet | `<alkane>-derived_<default>-usd` |
| the alkane *is* the quote it would proxy to, or no derived quote is in force | `<alkane>-usd` |

"Has not forked yet" matters for illiquid tokens: a pair only gets candles under
a forked quote on the token's first trade after the height, so until then
`-full` stays on the default quote rather than going blank at the fork height.
The response keeps `pool` as requested and adds `resolved_pool`.
`get_alkanes_quote` orders its derived quotes the same way, and the explorer's
token page takes its market summary and chart source from the same resolver
(`AmmDataProvider::full_chart_target`, remote-aware for client-mode explorers),
so quote, page and chart agree.

A `target` that is not in `derived_liquidity` at all is a config error; one that
is listed but not in force yet is just "not yet". `target` may be omitted on an
entry that exists only for `drop_forks` or `start_offset`. `derived_sources` is
accepted as a spelling of `derive_sources`.

#### `drop_forks`

`{ alkane, height }` on a source entry: from that height, trading on the
source's pool against that alkane is ignored by the source's derived charts,
indefinitely. The chart id does not change. What it does depends on the alkane:

| dropped alkane is | effect |
| --- | --- |
| a derived quote (e.g. `2:0`) | the pool is not registered as the pair's derived pool; the chart follows the canonical leg alone. The config form of `MAINNET_DERIVED_QUOTE_FORKS` - see "Derived-chart forks" above. Both apply |
| a canonical quote (e.g. `32:0`) | the canonical leg (the source's direct USD candle) is left out of its derived charts, which follow the derived pool alone |

The canonical leg is one merged series across all of a token's canonical pools,
so it is only dropped when **every** canonical quote in force is dropped for the
token (`DerivedIndexPlan::canonical_leg_dropped`). In `derive_token_data` that
means the token's own USD candle neither triggers a derived bucket nor feeds the
merge, and the higher-timeframe copy from the direct series is skipped for it.
The token's direct `-usd` / `-mcusd` charts and its token metrics are untouched:
they are the canonical leg.

#### `start_offset`

A height before which none of the alkane's charts return candles - usd, mcusd,
derived, `-full`, `-cmcap`, sats, and its TVL line - as if the earlier trading
had not happened. Read-side only: nothing is deleted, and removing the setting
brings the history back.

It is applied at 10-minute precision (`apply_start_offset_ts`). Buckets that end
before the offset are dropped. On a higher timeframe the one bucket straddling
it is **rebuilt** from the series' own 10m candles from the offset on, because
simply keeping it would leave a launch wick inside that day's or week's open and
high, and simply dropping it would hide up to a month of real data on the 1M
chart. A `start_offset` the index has not reached yet hides the whole chart:
every candle so far happened before it. Charts requested by raw pool id are not
affected - the offset is per alkane.

### Trusted pricing factories

Espo indexes pools from any contract that looks like an AMM factory, and a
factory only guarantees one pool per pair among its own pools - so a second
factory can open a duplicate pool for an existing pair. From mainnet block
`MAINNET_PRICING_FACTORY_FORK_HEIGHT` (970000) only pools created by a factory in
`MAINNET_PRICING_FACTORIES` (`4:65522`, the Oyl AMM) can set a token's price.
Other pools are still indexed, charted, and counted in volume and TVL.

`IndexState::pricing_excluded_pools` is filled once per block, after pool
discovery, by `AmmDataProvider::pools_not_in_factories`, and consulted wherever a
pool can set a price:

| site | effect on an excluded pool |
| --- | --- |
| `process_balance_deltas` | its trade does not add to `canonical_trade_buckets`, so it does not re-price a token |
| `derive_token_data` | not one of a token's canonical pools; not a pair's derived pool; cannot write the canonical-pool pointer |
| `derive_pool_metrics` | not a source for a token's sats price (pool TVL) |
| `get_canonical_pools` | dropped at request time; the token's pool list supplies the trusted pool instead |

A pool belongs to a factory if either `pool_factory` or `factory_pools` says so.
That is espo's own record, not essentials' `inspection.factory_alkane` (the
clone template) - by that field most Oyl pools belong to `4:780993`.

The canonical-pool pointer (`canonical_pool/v2/<token>/<quote>`) holds one pool
per pair and the newest discovered pool overwrote it, so duplicates had already
taken it for several tokens. It is not rewritten; readers stop trusting it from
the fork height. History before the height is untouched.

Background, the mainnet incident that prompted it, and what it changes for
consumers: `docs/2-trusted-pricing-factories.md`.
