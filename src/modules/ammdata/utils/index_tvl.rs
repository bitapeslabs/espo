//! Aggregated TVL lines.
//!
//! Every pool contributes one *anchored* value: the side of the pool we can price
//! without guessing, doubled, because a constant-product pool holds equal value on
//! both sides. Those contributions are summed into a global running total and into
//! a per-token running total, then sampled onto the same timeframe buckets the
//! candle series uses.
//!
//! The totals are held in canonical sats, never USD. A sats series is the honest
//! "did liquidity grow" line, and the USD line is recovered at read time by
//! multiplying against the btc/usd line at the same bucket - so a move in the BTC
//! price never rewrites history.
//!
//! Maintenance is O(pools touched this block): each pool's last contribution is
//! stored, so a block applies deltas rather than re-summing every pool.

use crate::modules::ammdata::consts::{AMOUNT_SCALE, CanonicalQuoteUnit};
use crate::modules::ammdata::schemas::{SchemaMarketDefs, SchemaTvlPointV1, active_timeframes};
use crate::modules::ammdata::storage::{AmmDataProvider, encode_tvl_point_v1};
use crate::modules::ammdata::utils::candles::bucket_start_for;
use crate::modules::ammdata::utils::index_state::IndexState;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use std::collections::HashMap;

/// Per-side valuations for one pool, as `derive_pool_metrics` already computes them.
pub struct PoolAnchorInput {
    pub base_tvl_sats: u128,
    pub quote_tvl_sats: u128,
    pub base_price_sats: u128,
    pub quote_price_sats: u128,
    pub base_tvl_usd: u128,
    pub quote_tvl_usd: u128,
}

/// Pick the side of the pool we can price most confidently and double it.
///
/// Preference order matches `pool_volume_side`: quote first, then base. A pool with
/// a canonical quote (frBTC/BUSD) needs no price feed at all; one whose side has a
/// canonical-rooted sats price is a step less certain; anything else falls back to
/// summing both sides off the USD token feed and is reported separately so a chart
/// can drop it.
pub fn pool_anchor_point(
    defs: &SchemaMarketDefs,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    input: &PoolAnchorInput,
    btc_usd_price: Option<u128>,
) -> SchemaTvlPointV1 {
    let quote_is_canonical = canonical_quote_units.contains_key(&defs.quote_alkane_id);
    let base_is_canonical = canonical_quote_units.contains_key(&defs.base_alkane_id);

    if quote_is_canonical {
        return SchemaTvlPointV1 {
            canonical_sats: input.quote_tvl_sats.saturating_mul(2),
            ..Default::default()
        };
    }
    if base_is_canonical {
        return SchemaTvlPointV1 {
            canonical_sats: input.base_tvl_sats.saturating_mul(2),
            ..Default::default()
        };
    }
    if input.quote_price_sats > 0 {
        return SchemaTvlPointV1 {
            derived_sats: input.quote_tvl_sats.saturating_mul(2),
            ..Default::default()
        };
    }
    if input.base_price_sats > 0 {
        return SchemaTvlPointV1 {
            derived_sats: input.base_tvl_sats.saturating_mul(2),
            ..Default::default()
        };
    }

    // Neither side is reachable from a canonical quote in sats. Fall back to the USD
    // token feed for both sides and convert once, so the pool still shows up somewhere.
    let usd = input.base_tvl_usd.saturating_add(input.quote_tvl_usd);
    let sats = match btc_usd_price {
        Some(price) if price > 0 => usd.saturating_mul(AMOUNT_SCALE).saturating_div(price),
        _ => 0,
    };
    SchemaTvlPointV1 { unanchored_sats: sats, ..Default::default() }
}

#[derive(Default, Clone, Copy)]
struct TvlDelta {
    canonical: i128,
    derived: i128,
    unanchored: i128,
}

impl TvlDelta {
    fn add(&mut self, prev: &SchemaTvlPointV1, next: &SchemaTvlPointV1) {
        self.canonical =
            self.canonical.saturating_add(diff(next.canonical_sats, prev.canonical_sats));
        self.derived = self.derived.saturating_add(diff(next.derived_sats, prev.derived_sats));
        self.unanchored =
            self.unanchored.saturating_add(diff(next.unanchored_sats, prev.unanchored_sats));
    }

    fn is_zero(&self) -> bool {
        self.canonical == 0 && self.derived == 0 && self.unanchored == 0
    }
}

fn clamp_i128(value: u128) -> i128 {
    value.min(i128::MAX as u128) as i128
}

fn diff(next: u128, prev: u128) -> i128 {
    clamp_i128(next).saturating_sub(clamp_i128(prev))
}

fn apply(base: u128, delta: i128) -> u128 {
    if delta >= 0 {
        base.saturating_add(delta as u128)
    } else {
        base.saturating_sub(delta.unsigned_abs())
    }
}

fn apply_point(base: &SchemaTvlPointV1, delta: &TvlDelta) -> SchemaTvlPointV1 {
    SchemaTvlPointV1 {
        canonical_sats: apply(base.canonical_sats, delta.canonical),
        derived_sats: apply(base.derived_sats, delta.derived),
        unanchored_sats: apply(base.unanchored_sats, delta.unanchored),
    }
}

/// Move the global and per-token TVL totals by this block's deltas, then stamp the
/// new totals onto every timeframe bucket that `block_ts` falls in.
pub fn prepare_tvl_lines(
    block_ts: u64,
    height: u32,
    provider: &AmmDataProvider,
    state: &mut IndexState,
) -> Result<()> {
    if state.pool_tvl_anchor_current.is_empty() {
        return Ok(());
    }

    let table = provider.table();
    let anchors: Vec<(SchemaAlkaneId, SchemaTvlPointV1)> = state
        .pool_tvl_anchor_current
        .iter()
        .map(|(pool, point)| (*pool, *point))
        .collect();

    let mut global_delta = TvlDelta::default();
    let mut token_deltas: HashMap<SchemaAlkaneId, TvlDelta> = HashMap::new();

    for (pool, next) in anchors.iter() {
        let prev = provider.get_pool_tvl_anchor(pool).ok().flatten().unwrap_or_default();
        if prev == *next {
            continue;
        }

        global_delta.add(&prev, next);
        if let Some(defs) = state.pools_map.get(pool).copied() {
            token_deltas.entry(defs.base_alkane_id).or_default().add(&prev, next);
            if defs.quote_alkane_id != defs.base_alkane_id {
                token_deltas.entry(defs.quote_alkane_id).or_default().add(&prev, next);
            }
        }

        state
            .pool_tvl_anchor_writes
            .push((table.pool_tvl_anchor_key(pool), encode_tvl_point_v1(next)?));
    }

    if global_delta.is_zero() && token_deltas.is_empty() {
        return Ok(());
    }

    let prev_height = height.saturating_sub(1);
    let timeframes = active_timeframes();

    if !global_delta.is_zero() {
        let prev_total = provider
            .get_amm_tvl_total_at_or_before_height(prev_height)
            .ok()
            .flatten()
            .map(|(_h, point)| point)
            .unwrap_or_default();
        let next_total = apply_point(&prev_total, &global_delta);
        let encoded = encode_tvl_point_v1(&next_total)?;
        state
            .tvl_total_writes
            .push((table.amm_tvl_total_key(u64::from(height)), encoded.clone()));
        for tf in timeframes.iter() {
            let bucket_ts = bucket_start_for(block_ts, *tf);
            state
                .tvl_line_writes
                .push((table.amm_tvl_line_key(*tf, bucket_ts), encoded.clone()));
        }
    }

    for (token, delta) in token_deltas.iter() {
        if delta.is_zero() {
            continue;
        }
        let prev_total = provider
            .get_token_tvl_total_at_or_before_height(token, prev_height)
            .ok()
            .flatten()
            .map(|(_h, point)| point)
            .unwrap_or_default();
        let next_total = apply_point(&prev_total, delta);
        let encoded = encode_tvl_point_v1(&next_total)?;
        state
            .tvl_total_writes
            .push((table.token_tvl_total_key(token, u64::from(height)), encoded.clone()));
        for tf in timeframes.iter() {
            let bucket_ts = bucket_start_for(block_ts, *tf);
            state
                .tvl_line_writes
                .push((table.token_tvl_line_key(token, *tf, bucket_ts), encoded.clone()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::ammdata::consts::{FRBTC_ALKANE_ID, PRICE_SCALE};

    fn defs(base: SchemaAlkaneId, quote: SchemaAlkaneId) -> SchemaMarketDefs {
        SchemaMarketDefs {
            base_alkane_id: base,
            quote_alkane_id: quote,
            pool_alkane_id: SchemaAlkaneId { block: 100, tx: 1 },
        }
    }

    #[test]
    fn canonical_quote_pool_doubles_the_canonical_side() {
        let diesel = SchemaAlkaneId { block: 2, tx: 0 };
        let mut canonical = HashMap::new();
        canonical.insert(FRBTC_ALKANE_ID, CanonicalQuoteUnit::Btc);

        let point = pool_anchor_point(
            &defs(diesel, FRBTC_ALKANE_ID),
            &canonical,
            &PoolAnchorInput {
                base_tvl_sats: 999,
                quote_tvl_sats: 1_000_000,
                base_price_sats: 0,
                quote_price_sats: 0,
                base_tvl_usd: 0,
                quote_tvl_usd: 0,
            },
            Some(PRICE_SCALE),
        );

        // The non-canonical side is ignored entirely; only 2x the canonical leg counts.
        assert_eq!(point.canonical_sats, 2_000_000);
        assert_eq!(point.derived_sats, 0);
        assert_eq!(point.unanchored_sats, 0);
    }

    #[test]
    fn pool_without_canonical_side_uses_the_priced_leg() {
        let tort = SchemaAlkaneId { block: 2, tx: 68479 };
        let diesel = SchemaAlkaneId { block: 2, tx: 0 };
        let canonical = HashMap::new();

        let point = pool_anchor_point(
            &defs(tort, diesel),
            &canonical,
            &PoolAnchorInput {
                base_tvl_sats: 500,
                quote_tvl_sats: 700,
                base_price_sats: 0,
                quote_price_sats: 42,
                base_tvl_usd: 0,
                quote_tvl_usd: 0,
            },
            Some(PRICE_SCALE),
        );

        assert_eq!(point.canonical_sats, 0);
        assert_eq!(point.derived_sats, 1_400);
        assert_eq!(point.unanchored_sats, 0);
    }

    #[test]
    fn unpriceable_pool_falls_back_to_both_usd_legs() {
        let a = SchemaAlkaneId { block: 2, tx: 111 };
        let b = SchemaAlkaneId { block: 2, tx: 222 };
        let canonical = HashMap::new();
        let btc_usd = 50_000u128.saturating_mul(PRICE_SCALE);

        let point = pool_anchor_point(
            &defs(a, b),
            &canonical,
            &PoolAnchorInput {
                base_tvl_sats: 0,
                quote_tvl_sats: 0,
                base_price_sats: 0,
                quote_price_sats: 0,
                base_tvl_usd: 25_000u128.saturating_mul(PRICE_SCALE),
                quote_tvl_usd: 25_000u128.saturating_mul(PRICE_SCALE),
            },
            Some(btc_usd),
        );

        assert_eq!(point.canonical_sats, 0);
        assert_eq!(point.derived_sats, 0);
        // $50k at $50k/BTC is exactly one BTC.
        assert_eq!(point.unanchored_sats, AMOUNT_SCALE);
    }

    #[test]
    fn deltas_can_move_a_total_down() {
        let prev = SchemaTvlPointV1 { canonical_sats: 1_000, ..Default::default() };
        let next = SchemaTvlPointV1 { canonical_sats: 400, ..Default::default() };
        let mut delta = TvlDelta::default();
        delta.add(&prev, &next);

        let total = SchemaTvlPointV1 { canonical_sats: 5_000, ..Default::default() };
        assert_eq!(apply_point(&total, &delta).canonical_sats, 4_400);
    }
}
