# 2. Trusted pricing factories

From mainnet block **970000**, only pools created by an allowlisted AMM factory
can set a token's price. Pools from any other factory are still indexed, charted
and counted - they just cannot move a price.

The allowlist is one factory today: `4:65522`, the Oyl AMM.

---

## Why

Espo does not hard-code which contract is "the" AMM. It indexes pools from any
contract that looks like an AMM factory - directly, or through a proxy that
points at one. On mainnet it recognises several hundred.

A factory guarantees **one pool per pair among its own pools**, and nothing
more. So anyone can deploy a factory and open a second pool for a pair that
already has one, with a few dollars in it.

That happened. Between blocks 967842 and 969911 a second factory, `4:235884`,
opened 18 pools duplicating the main markets, each with dust liquidity:

| pair | real pool (Oyl) | duplicate (`4:235884`) |
| --- | --- | --- |
| DIESEL / frBTC | `2:77087` - 205,447 DIESEL / 92 BTC | `2:96884` - 17.7 DIESEL / 938,061 sats |
| TORTILLA / frBTC | `2:77269` | `2:102386` - 247 TORT / 3,125 sats |
| SLICE / frBTC | `2:96947` - 5,245 SLICE / 0.47 BTC | `2:102392` - 0.47 SLICE / 4,090 sats |
| FIRE / frBTC | (Oyl) | `2:97417` |
| DIESEL / TORTILLA | `2:70020` | `2:96895` |

Of the 196 pools on mainnet at the time, 178 belonged to `4:65522` and these 18
to `4:235884`.

Two things in how espo priced tokens turned that from clutter into a problem.

**A token's price was not merged across its pools.** For a token with more than
one frBTC pool, each pool that traded in a 10-minute bucket *overwrote* the
previous one, and the order came from a hash map. Which pool became the token's
price was arbitrary - not the deeper one. A single trade in a $3 pool could set
the candle.

**The live-price pointer kept one pool per pair, newest wins.** The index behind
`get_canonical_pool_prices` (used by oylapi and tokendata) stores a single pool
per (token, quote), and every newly discovered pool replaced it. So for each
token that got a duplicate, that pointer named the dust pool - for SLICE, a pool
that had never traded.

---

## What changed

A pool is **trusted for pricing** when it was created by a factory on the
allowlist. From the activation height, an untrusted pool:

| | still happens | no longer happens |
| --- | --- | --- |
| its own candles, activity, volume | yes | |
| counted in TVL, listed by `get_pools` | yes | |
| its trades re-price a token | | no - it does not trigger a token USD candle |
| it is one of a token's canonical pools | | no - left out of the direct USD price and of the sats price used for TVL |
| it is a pair's derived pool | | no - left out of derived charts |
| it takes the live-price pointer | | no - it is not written, and readers no longer trust it |

The rule is applied in every place a pool can set a price, not just one:

* `process_balance_deltas` - an untrusted pool's trade does not mark a token for
  re-pricing;
* `derive_token_data` - a token's canonical pools, a pair's derived pool, and
  writes to the canonical-pool pointer;
* `derive_pool_metrics` - the per-token sats price used for pool TVL;
* `get_canonical_pools` - request-time live prices.

### The pointer is not repaired, it is distrusted

The duplicates had already overwritten the live-price pointer for nine tokens.
Rewriting it would mean editing indexed state, so it is left alone. Instead,
from the activation height `get_canonical_pools` treats it as a hint: an entry
naming an untrusted pool is dropped, and the token's own pool list supplies the
trusted pool the pointer lost. Live prices come back the moment the new binary
is serving past the height, with nothing rewritten.

### How trust is decided

A pool's factory is recorded twice when the pool is discovered: `pool_factory`
(pool -> factory) and `factory_pools` (factory -> pool). A pool belongs to a
factory if **either** says so, so an index that only gained one of the two later
cannot make an old pool look foreign.

Note this is espo's own record of who created the pool. It is **not**
essentials' `inspection.factory_alkane`, which is the template a contract was
cloned from: by that field 143 of the Oyl pools belong to `4:780993`, and an
allowlist built on it would have stripped the price from nearly every token.

A read error while working this out fails the block rather than being swallowed.
Treating "could not read" as "nothing is trusted" would silently stop pricing
every token.

---

## What it does not do

* **History is untouched.** Before block 970000 every recognised factory priced
  tokens, and those candles stay exactly as indexed. The duplicate DIESEL/frBTC
  pool traded once before the height; if that set a DIESEL candle, it still does.
* **No reindex, nothing deleted.** It changes how blocks from the height onward
  are priced.
* **It is mainnet-only.** Other networks have no allowlist and behave as before.

## One token loses its direct price

`2:490` had no frBTC pool until `4:235884` opened `2:102217` for it at block
969644, with 14,046 sats of liquidity. That is its only frBTC pool, so from the
height it has no direct frBTC price again and is priced through its DIESEL pool,
as it was before. Every other affected token keeps a trusted frBTC pool.

---

## Adding a factory

Edit `MAINNET_PRICING_FACTORIES` in `src/modules/ammdata/consts.rs`. If the
change should not apply retroactively on a reindex, give it its own height the
way `MAINNET_PRICING_FACTORY_FORK_HEIGHT` does.

With more than one trusted factory a pair can again have several trusted pools,
and the "last pool to trade wins" behaviour described above returns for those
pairs. If that day comes, price from the deepest pool rather than extending the
list alone.

---

## Where it lives

| piece | location |
| --- | --- |
| the allowlist and its height | `MAINNET_PRICING_FACTORIES`, `MAINNET_PRICING_FACTORY_FORK_HEIGHT`, `pricing_factories_at_height` in `src/modules/ammdata/consts.rs` |
| deciding trust | `AmmDataProvider::pools_not_in_factories`, `pool_in_factories` in `src/modules/ammdata/storage.rs` |
| per-block set | `IndexState::pricing_excluded_pools`, filled in `index_block` after pool discovery |
| request-time canonical pools | `trusted_canonical_pools` in `storage.rs` |
