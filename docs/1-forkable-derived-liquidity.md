# 1. Forkable derived liquidity, `-full` charts, and `derive_sources`

This update lets an operator change **which pools price an alkane's chart, from a
chosen block onward**, without reindexing and without the chart's consumers
having to know it happened.

It adds three things to the `ammdata` module:

1. a `height` on `derived_liquidity` entries, so an alkane can *become* a derived
   quote at a block, with every chart against it continuing from existing history;
2. a new chart id, `<alkane>-full`, that always resolves to "the chart this
   alkane is supposed to be read from right now";
3. a `derive_sources` config section that routes `-full` per alkane and can drop
   a pool from an alkane's pricing (`drop_forks`) or hide its early history
   (`start_offset`).

A complete, runnable example lives in
[`example_configs/config.json`](../example_configs/config.json). Its credentials
are placeholders.

---

## Background: the two kinds of quote

Every alkane chart is priced in USD through a **quote**.

| | canonical quote | derived quote |
| --- | --- | --- |
| what it is | an asset whose USD value is known directly | an alkane used as a second route to USD |
| on mainnet | frBTC `32:0` (BUSD too, before height 946500) | whatever `derived_liquidity` lists - DIESEL `2:0` by default |
| chart it produces | `<alkane>-usd` - the **direct** chart | `<alkane>-derived_<quote>-usd` - the **derived** chart |
| defined in | `consts.rs` | `config.json` |

A derived chart merges two legs:

* the **canonical leg** - the alkane's direct USD price, from its frBTC pools;
* the **derived leg** - the alkane's price in the derived quote, from its pool
  against that quote, times the quote's own USD price.

They are combined with the quote's `strategy` (`neutral`, `neutral-vwap`,
`optimistic`, `pessimistic`). An alkane with no pool against a derived quote
still has a derived chart against it; it simply mirrors the direct chart.

The **default derived quote** is the first entry of `derived_liquidity`.

---

## 1. `height` on `derived_liquidity`

```json
"derived_liquidity": [
  { "alkane": "2:0",     "strategy": "neutral-vwap" },
  { "alkane": "2:68479", "strategy": "neutral-vwap", "height": 969931 }
]
```

| field | required | meaning |
| --- | --- | --- |
| `alkane` | yes | the alkane acting as a derived quote |
| `strategy` | yes | how the two legs are merged |
| `height` | no | first block it acts as a derived quote. Omitted: from the ammdata genesis, as before |

Before `height` the entry does not exist as far as indexing is concerned - no
candles are written against it. From `height` on it is a derived quote like any
other.

### It is a fork, not a fresh start

A chart against the new quote does not begin empty. The first time a token is
about to get a candle against it, the token's **entire usd and mcusd history
against the default quote is copied** into the new pair, on every timeframe.
New candles then continue from there.

```
2:77-derived_2:0-usd       ████████████████████████████████▶  keeps being indexed
                                              │ first trade at/after the height
                                              ▼ history copied once
2:77-derived_2:68479-usd   ████████████████████░░░░░░░░░░░░▶  new candles from here
                           └── copied history ─┘└─ indexed ─┘
```

For the default quote's own token there is no chart against itself (`2:0` has no
`2:0-derived_2:0-usd`), so its direct usd/mcusd charts are what is carried over.

How the copy is kept safe:

* **Once per pair.** A marker (`token_derived_fork/v1/<token>/<quote>`) is written
  in the same batch as the copy.
* **New keys only.** If the destination already holds candles it is left exactly
  as it is and only marked. Nothing is deleted or moved - the default quote's
  series stays and keeps being indexed.
* **Lazy.** It happens per token, on that token's first trade after the height -
  not for every token at the fork block.
* **Inside the block.** The copy is written into the block being indexed, so the
  rest of that block already reads it (previous close, anchoring, higher-timeframe
  aggregation, metrics), and a reorg removes the copy and the marker together.

### What it costs

Every token that trades after the height forks against the new quote - not only
tokens that have a pool against it, because every traded token gets a derived
candle against every active derived quote. Each fork is one read of the token's
default-quote series per timeframe and as many writes. A token with a DIESEL pool
has a candle in most 10-minute buckets, so that can be tens of thousands of keys
in the block where it first trades. Expect a handful of noticeably slow blocks,
once per token, and the derived series to roughly double on disk over time.

The indexer logs each one:

```
[AMMDATA] forked 2:77 derived history 2:0 -> 2:68479 at height 969940 (41873 candles)
```

---

## 2. The `-full` chart

`-full` exists so a consumer never has to track which derived quote an alkane is
on, or when it moved.

| id | chart |
| --- | --- |
| `<alkane>-full` | usd |
| `<alkane>-full-usd` | usd |
| `<alkane>-full-sats` | price in sats |
| `<alkane>-full-mcusd` | market cap |
| `<alkane>-full-mcsats` | market cap in sats |
| `<alkane>-full-cmcap` | circulating market cap |

It is rewritten to a real chart id before parsing, so it is served by exactly
the code that serves the chart it points at:

| situation | `<alkane>-full` serves |
| --- | --- |
| the alkane has a `derive_sources` target, the target is in force, and the pair has forked (or the target never had a height) | `<alkane>-derived_<target>-usd` |
| no `derive_sources` entry, target not in force yet, or the pair has not forked yet | `<alkane>-derived_<default>-usd` |
| the alkane *is* the quote it would point at, or no derived quote is in force | `<alkane>-usd` |

"Has not forked yet" is deliberate. A pair only gets candles under a forked quote
on the token's first trade after the height, so an illiquid token would otherwise
go blank at the fork height and stay blank until it traded. Until then `-full`
keeps serving the default quote.

The response reports what was served:

```json
{
  "ok": true,
  "pool": "4:8888-full",
  "resolved_pool": "4:8888-derived_2:68479-usd",
  "timeframe": "1d",
  "candles": [ ... ]
}
```

`ammdata.get_alkanes_quote` orders its derived quotes the same way, so a quote
agrees with the `-full` chart.

---

## 3. `derive_sources`

```json
"derive_sources": [
  {
    "alkane": "4:8888",
    "target": "2:68479",
    "start_offset": 968034,
    "drop_forks": [ { "alkane": "32:0", "height": 969931 } ]
  }
]
```

One entry per alkane.

| field | required | meaning |
| --- | --- | --- |
| `alkane` | yes | the alkane this entry routes |
| `target` | no | the derived quote its `-full` chart points at |
| `drop_forks` | no | pools to stop taking into its derived charts, each from a height |
| `start_offset` | no | height before which its charts return nothing |

`derived_sources` is accepted as a spelling of the key.

### `target`

Must be an alkane listed in `derived_liquidity` - one that is not listed at all is
a config error and espo will not start. One that is listed but whose `height` has
not been reached is fine: `-full` uses the default quote until it is.

`target` may be omitted on an entry that only exists for `drop_forks` or
`start_offset`; `-full` then uses the default quote.

### `drop_forks`

`{ "alkane": X, "height": H }`: from block `H`, trading on **this alkane's pool
against `X`** is ignored by this alkane's derived charts, indefinitely. The chart
id does not change. What it means depends on what `X` is:

| `X` is | effect on the alkane's derived charts |
| --- | --- |
| a **derived quote** (e.g. `2:0`) | the pool against `X` is no longer the pair's derived pool. `X`'s price moves stop being written into new candles; the chart follows the canonical leg alone - the same values `-usd` gives |
| a **canonical quote** (e.g. `32:0`) | the canonical leg is left out. The alkane's own frBTC trades stop moving the chart; it follows the derived pool alone |

Two things to know about dropping a canonical quote:

* The canonical leg is one merged series across all of an alkane's canonical
  pools, so it can only be left out whole. It is dropped only when **every**
  canonical quote in force is dropped for that alkane. On mainnet today frBTC is
  the only one, so dropping `32:0` is enough.
* It applies to the alkane's **derived charts**, which is what `-full` serves.
  Its direct `-usd` / `-mcusd` charts are untouched: they *are* the canonical leg.
  Token metrics (the price shown in listings) also still come from the direct
  chart.

Dropping the canonical quote leaves an alkane priced by its derived pool alone,
so look at that pool's depth before doing it. If the alkane has no pool against
its target at all, nothing is left to price it and the chart goes flat.

The hard-coded mainnet table `MAINNET_DERIVED_QUOTE_FORKS` (`consts.rs`) is the
same mechanism for derived quotes; both apply.

### `start_offset`

A height before which none of the alkane's charts return candles - usd, mcusd,
derived, `-full`, `-cmcap`, sats, and its TVL line - as if the earlier trading
had not happened.

* **Read-side only.** Nothing is deleted. Remove the setting and the history is
  back.
* **10-minute precision.** Buckets that end before the offset's block are dropped.
* **Higher timeframes are repaired, not just trimmed.** The one daily / weekly /
  monthly bucket that straddles the offset is rebuilt from the 10-minute candles
  from the offset on. Simply keeping it would leave a launch wick inside that
  candle's open and high; simply dropping it would hide up to a month of real
  data on the monthly chart.
* **A height the index has not reached hides the whole chart** - every candle so
  far happened before it. Check for typos.
* Charts requested by raw pool id are not affected; the offset is per alkane.

---

## The example, step by step

`example_configs/config.json` configures this for SLICE (`4:8888`):

```json
"derived_liquidity": [
  { "alkane": "2:0",     "strategy": "neutral-vwap" },
  { "alkane": "2:68479", "strategy": "neutral-vwap", "height": 969931 }
],
"derive_sources": [
  { "alkane": "4:8888", "target": "2:68479", "start_offset": 968034,
    "drop_forks": [ { "alkane": "32:0", "height": 969931 } ] }
]
```

| | before 969931 | from 969931 |
| --- | --- | --- |
| derived quotes in force | DIESEL | DIESEL, TORTILLA |
| `4:8888-full` serves | `4:8888-derived_2:0-usd` | `4:8888-derived_2:68479-usd`, once the pair has forked |
| what moves SLICE's derived chart | its frBTC pools (it has no DIESEL pool, so the chart mirrors direct) | its TORTILLA pool only - frBTC is dropped |
| history shown | from height 968034 | from height 968034; before the fork it is the copied DIESEL-quote history |

What happens at the fork, in order:

1. Block 969931 is indexed. TORTILLA is now a derived quote. SLICE's canonical
   leg is dropped, so its own frBTC trades no longer create derived candles.
2. The first time SLICE's TORTILLA pool trades (or TORTILLA's USD price moves),
   a `4:8888-derived_2:68479` candle is due. Its marker is missing, so SLICE's
   whole `4:8888-derived_2:0` history is copied into `4:8888-derived_2:68479`.
3. The new candle is built from the TORTILLA pool price times TORTILLA's USD
   price, opening at the previous close, and written after the copied history.
4. From then on `4:8888-full` resolves to `4:8888-derived_2:68479-usd`.
5. At every read, candles before block 968034 are dropped.

Every other token is untouched by the SLICE entry. Each of them still forks
against TORTILLA on its own first trade after 969931, but with no `derive_sources`
entry its `-full` stays on DIESEL.

---

## Operating notes

* **Forward-only.** These settings change what is indexed from a block onward.
  Nothing already on disk is rewritten, and there is no reindex. If a `height` is
  already behind the tip when the new binary starts, it takes effect at the first
  block that binary indexes; candles for the blocks in between were written under
  the old rules and stay as they are (and are part of what a fork copies).
* **Nothing is deleted.** Forks copy; `start_offset` filters at read time; a
  dropped pool just stops contributing. Every one of these can be reverted by
  editing config, with the caveat that candles indexed while a setting was in
  force are permanent.
* **Reorgs.** Fork copies and markers are written inside a block and leave with
  it.
* **Changing a `height` after the fact.** A pair that has already forked keeps
  its marker and its copied history. Moving the height later does not undo that.

---

## Where it lives

| piece | location |
| --- | --- |
| config types, parsing, `derived_index_plan`, `full_target` | `src/modules/ammdata/config.rs` |
| active quotes, drop forks, the fork trigger | `derive_token_data` in `src/modules/ammdata/utils/index_tokens.rs` |
| the history copy and its marker | `AmmDataProvider::fork_token_derived_history` in `src/modules/ammdata/storage.rs` |
| `-full` resolution | `resolve_full_chart_id` in `storage.rs` |
| `start_offset` | `apply_token_chart_start_cutoff` / `apply_start_offset_ts` in `storage.rs` |
| mainnet hard-coded forks | `MAINNET_DERIVED_QUOTE_FORKS` in `src/modules/ammdata/consts.rs` |
| module reference | `src/modules/ammdata/README.md` |
