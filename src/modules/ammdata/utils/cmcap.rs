//! Circulating market cap ("cmcap"), derived at request time from the stored mcap
//! series.
//!
//! The stored mcap candles are price x circulating supply as essentials tracks it,
//! which counts every minted token - tokens sitting in a vesting contract, a
//! treasury or a burn sink included. For many tokens that number is FDV, not
//! market cap. cmcap scales each mcap candle by
//!
//! ```text
//! (supply - non_circulating) / supply
//! ```
//!
//! where `non_circulating` is the sum of balances held by the addresses and
//! alkanes listed for that token in `consts::MAINNET_NON_CIRCULATING` - a network
//! fact, so it lives in consts rather than config.
//!
//! Nothing new is indexed. The ratio is a step function that only moves when a
//! configured holder's balance moves, and essentials already records exactly when
//! that happens:
//!
//! * an alkane holder has a per-height balance history for the token
//!   (`balance_by_height`), with the list of heights it changed at;
//! * an address holder has a ledger of every tx that touched its alkane balances
//!   (`AlkaneTxs`), and the store is versioned per block, so its balance at any
//!   of those heights is one point read against that block's root.
//!
//! So a request costs a few small list reads plus one point read per trigger
//! height - not a walk over the candles' history. Supply is read at those same
//! trigger heights only; between triggers the ratio is held, so supply growth
//! without a holder movement does not move it. That is a deliberate trade for
//! cheapness and is noted in the docs.

use crate::modules::ammdata::consts::NonCirculatingHolders;
use crate::modules::ammdata::schemas::SchemaCandleV1;
use crate::modules::essentials::storage::{
    AddressIndexListKind, EssentialsProvider, GetCirculatingSupplyParams, GetMultiValuesParams,
    GetRawValueParams, decode_u128_value, get_address_index_list_len, get_address_index_list_range,
    load_tx_pointer_blob_v3_by_id,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};

/// Refuse rather than silently truncate: an address with this many alkane txs is
/// not a treasury or a locker, and a partial ledger would make the chart lie.
const MAX_ADDRESS_LEDGER_ENTRIES: u64 = 50_000;
/// Block summaries are fetched in batches of this many heights.
const BLOCK_TIME_BATCH: usize = 1_000;

/// One point where the circulating ratio changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CirculatingStep {
    pub height: u32,
    pub ts: u64,
    pub non_circulating: u128,
    pub supply: u128,
}

impl CirculatingStep {
    /// Circulating amount, floored at zero if the config over-counts.
    pub fn circulating(&self) -> u128 {
        self.supply.saturating_sub(self.non_circulating)
    }

    pub fn ratio_f64(&self) -> f64 {
        if self.supply == 0 {
            return 1.0;
        }
        self.circulating() as f64 / self.supply as f64
    }
}

/// Build the ratio's step function for one token, oldest first.
pub fn circulating_steps(
    essentials: &EssentialsProvider,
    token: SchemaAlkaneId,
    holders: &NonCirculatingHolders,
) -> Result<Vec<CirculatingStep>> {
    let mut series: Vec<Vec<(u32, u128)>> = Vec::new();
    for owner in holders.alkanes.iter() {
        series.push(alkane_holder_series(essentials, owner, &token)?);
    }
    for address in holders.addresses.iter() {
        series.push(address_holder_series(essentials, address, &token)?);
    }

    let mut heights: BTreeSet<u32> = BTreeSet::new();
    for s in series.iter() {
        for (h, _) in s.iter() {
            heights.insert(*h);
        }
    }
    if heights.is_empty() {
        return Ok(Vec::new());
    }

    let heights: Vec<u32> = heights.into_iter().collect();
    let block_times = block_times_for(essentials, &heights)?;

    // Merge-walk: every holder's balance at height H is its last recorded value
    // at or before H, so one cursor per holder suffices.
    let mut cursors: Vec<usize> = vec![0; series.len()];
    let mut held: Vec<u128> = vec![0; series.len()];
    let mut out: Vec<CirculatingStep> = Vec::with_capacity(heights.len());
    for h in heights {
        for (i, s) in series.iter().enumerate() {
            while cursors[i] < s.len() && s[cursors[i]].0 <= h {
                held[i] = s[cursors[i]].1;
                cursors[i] += 1;
            }
        }
        let non_circulating = held.iter().fold(0u128, |acc, v| acc.saturating_add(*v));
        let supply = supply_at_height(essentials, &token, h)?;
        let Some(ts) = block_times.get(&h).copied() else { continue };
        out.push(CirculatingStep { height: h, ts, non_circulating, supply });
    }
    Ok(out)
}

/// The step in effect for a bucket, i.e. the last one at or before the bucket's end.
pub fn step_for_bucket(
    steps: &[CirculatingStep],
    bucket_ts: u64,
    dur: u64,
) -> Option<&CirculatingStep> {
    let end_inclusive = bucket_ts.saturating_add(dur).saturating_sub(1);
    // Steps are sorted by height and block time is monotone enough for this.
    let idx = steps.partition_point(|s| s.ts <= end_inclusive);
    if idx == 0 { None } else { steps.get(idx - 1) }
}

/// Scale an mcap candle to cmcap. Volume is left alone: it is trade volume.
pub fn scale_candle(candle: SchemaCandleV1, step: &CirculatingStep) -> SchemaCandleV1 {
    if step.supply == 0 {
        return candle;
    }
    let circ = step.circulating();
    let scale = |v: u128| -> u128 {
        let n = U256::from(v) * U256::from(circ) / U256::from(step.supply);
        n.saturating_to::<u128>()
    };
    SchemaCandleV1 {
        open: scale(candle.open),
        high: scale(candle.high),
        low: scale(candle.low),
        close: scale(candle.close),
        volume: candle.volume,
    }
}

/// `(height, balance)` for an alkane holder, from its per-height balance history.
fn alkane_holder_series(
    essentials: &EssentialsProvider,
    owner: &SchemaAlkaneId,
    token: &SchemaAlkaneId,
) -> Result<Vec<(u32, u128)>> {
    let table = essentials.table();
    let len = essentials
        .get_raw_value(GetRawValueParams {
            blockhash: StateAt::Latest,
            key: table.alkane_balance_by_height_list_len_key(owner, token),
        })?
        .value
        .and_then(|b| (b.len() == 4).then(|| u32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        .unwrap_or(0);
    if len == 0 {
        return Ok(Vec::new());
    }

    let idx_keys: Vec<Vec<u8>> = (0..len)
        .map(|i| table.alkane_balance_by_height_list_idx_key(owner, token, i))
        .collect();
    let mut heights: Vec<u32> = essentials
        .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys: idx_keys })?
        .values
        .into_iter()
        .flatten()
        .filter(|b| b.len() == 4)
        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .collect();
    heights.sort_unstable();
    heights.dedup();

    let value_keys: Vec<Vec<u8>> = heights
        .iter()
        .map(|h| table.alkane_balance_by_height_key(owner, token, *h))
        .collect();
    let values = essentials
        .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys: value_keys })?
        .values;

    Ok(heights
        .into_iter()
        .zip(values)
        .map(|(h, v)| (h, v.and_then(|b| decode_u128_value(&b).ok()).unwrap_or(0)))
        .collect())
}

/// `(height, balance)` for an address holder. Heights come from the address's
/// alkane tx ledger; each balance is a point read of the address's balance row
/// against that block's root.
fn address_holder_series(
    essentials: &EssentialsProvider,
    address: &str,
    token: &SchemaAlkaneId,
) -> Result<Vec<(u32, u128)>> {
    let total = get_address_index_list_len(
        essentials,
        StateAt::Latest,
        AddressIndexListKind::AlkaneTxs,
        address,
    )?;
    if total == 0 {
        return Ok(Vec::new());
    }
    if total > MAX_ADDRESS_LEDGER_ENTRIES {
        return Err(anyhow!(
            "non_circulating address {address} has {total} alkane txs, over the {MAX_ADDRESS_LEDGER_ENTRIES} cap"
        ));
    }
    let ids = get_address_index_list_range(
        essentials,
        StateAt::Latest,
        AddressIndexListKind::AlkaneTxs,
        address,
        0,
        total,
    )?;

    let mut heights: BTreeSet<u32> = BTreeSet::new();
    for id in ids {
        if let Some(blob) = load_tx_pointer_blob_v3_by_id(essentials, id) {
            heights.insert(blob.height);
        }
    }

    let table = essentials.table();
    let key = table.address_balance_key(address, token);
    let mut out = Vec::with_capacity(heights.len());
    for h in heights {
        let Some(hash) = essentials.blockhash_for_height(h)? else { continue };
        let at = essentials.with_view_blockhash(Some(hash));
        let balance = at
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: key.clone() })?
            .value
            .and_then(|b| decode_u128_value(&b).ok())
            .unwrap_or(0);
        out.push((h, balance));
    }
    Ok(out)
}

fn supply_at_height(
    essentials: &EssentialsProvider,
    token: &SchemaAlkaneId,
    height: u32,
) -> Result<u128> {
    let Some(hash) = essentials.blockhash_for_height(height)? else { return Ok(0) };
    let at = essentials.with_view_blockhash(Some(hash));
    Ok(at
        .get_circulating_supply(GetCirculatingSupplyParams {
            blockhash: StateAt::Latest,
            alkane: *token,
            height,
        })?
        .supply)
}

fn block_times_for(essentials: &EssentialsProvider, heights: &[u32]) -> Result<BTreeMap<u32, u64>> {
    let mut out = BTreeMap::new();
    for chunk in heights.chunks(BLOCK_TIME_BATCH) {
        let summaries = essentials.get_block_summaries_by_heights(chunk)?;
        for (h, s) in chunk.iter().zip(summaries) {
            if let Some(ts) = s.as_ref().and_then(|s| s.block_time()) {
                out.insert(*h, ts);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::ammdata::consts::PRICE_SCALE;

    fn step(ts: u64, non_circulating: u128, supply: u128) -> CirculatingStep {
        CirculatingStep { height: 0, ts, non_circulating, supply }
    }

    #[test]
    fn scales_by_the_circulating_fraction_without_overflowing() {
        // 1e9 tokens at $1 in PRICE_SCALE units: 1e9 * 1e8 * 1e16 = 1e33, which
        // times a 1e17 supply would overflow u128 if done naively.
        let mcap = 1_000_000_000u128 * 100_000_000 * PRICE_SCALE;
        let supply = 1_000_000_000u128 * 100_000_000;
        let locked = supply / 4;
        let c = SchemaCandleV1 { open: mcap, high: mcap, low: mcap, close: mcap, volume: 7 };
        let out = scale_candle(c, &step(0, locked, supply));
        assert_eq!(out.close, mcap / 4 * 3);
        assert_eq!(out.volume, 7);
    }

    #[test]
    fn over_counted_config_floors_at_zero_rather_than_underflowing() {
        let c = SchemaCandleV1 { open: 100, high: 100, low: 100, close: 100, volume: 0 };
        let out = scale_candle(c, &step(0, 500, 100));
        assert_eq!(out.close, 0);
        assert_eq!(step(0, 500, 100).ratio_f64(), 0.0);
    }

    #[test]
    fn bucket_takes_the_last_step_at_or_before_its_end() {
        let steps = vec![step(1_000, 1, 10), step(2_000, 2, 10), step(3_000, 3, 10)];
        let dur = 600;
        // Before any step: nothing locked yet.
        assert!(step_for_bucket(&steps, 0, dur).is_none());
        // A step landing inside the bucket counts for that bucket.
        assert_eq!(step_for_bucket(&steps, 1_800, dur).map(|s| s.non_circulating), Some(2));
        // Exactly at the end boundary is the next bucket's.
        assert_eq!(step_for_bucket(&steps, 1_400, dur).map(|s| s.non_circulating), Some(1));
        assert_eq!(step_for_bucket(&steps, 9_000, dur).map(|s| s.non_circulating), Some(3));
    }
}
