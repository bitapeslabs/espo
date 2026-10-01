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
//! * an address holder's balance in a token is the sum of its unspent outpoints
//!   carrying that token, and essentials keeps the address's outpoint list. Each
//!   outpoint record is small (no traces) and names its block, so the token's
//!   balance history is: +amount at the outpoint's creation height, -amount at
//!   its spend height. No per-height balance reads, and outpoints that do not
//!   carry the token cost one small decode and nothing else - which matters for
//!   a burn address that receives dozens of other tokens.
//!
//! Holders' histories are then merged and collapsed to the heights where the
//! total actually changed, and supply is read only at those. Between steps the
//! ratio is held, so supply growth without a holder movement does not move it.
//! That is a deliberate trade for cheapness and is noted in the docs.

use crate::modules::ammdata::consts::NonCirculatingHolders;
use crate::modules::ammdata::schemas::SchemaCandleV1;
use crate::modules::essentials::storage::{
    AddressIndexListKind, EssentialsProvider, GetCirculatingSupplyParams, GetMultiValuesParams,
    GetRawValueParams, decode_u128_value, get_address_index_list_len, get_address_index_list_range,
    load_outpoint_pointer_blob_v3_by_id, load_tx_summary_v2, resolve_outpoint_spent_by_id_v2,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Txid};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Refuse rather than silently truncate: an address with this many outpoints is
/// not a treasury or a locker, and a partial history would make the chart lie.
const MAX_ADDRESS_OUTPOINTS: u64 = 50_000;
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
    let mut last_total: Option<u128> = None;
    for h in heights {
        for (i, s) in series.iter().enumerate() {
            while cursors[i] < s.len() && s[cursors[i]].0 <= h {
                held[i] = s[cursors[i]].1;
                cursors[i] += 1;
            }
        }
        let non_circulating = held.iter().fold(0u128, |acc, v| acc.saturating_add(*v));
        // Only a height where the total actually moved is a step; everything
        // downstream (supply read, block time, ratio) is per step.
        if last_total == Some(non_circulating) {
            continue;
        }
        last_total = Some(non_circulating);
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

/// `(height, balance)` for an address holder, from its outpoints.
///
/// An outpoint carrying the token adds its amount at the block it was created in
/// and subtracts it at the block it was spent in. Outpoints that do not carry the
/// token are one small decode and nothing more. Spend heights need the spending
/// tx's summary, which is heavier, but lockers and burn addresses rarely spend.
fn address_holder_series(
    essentials: &EssentialsProvider,
    address: &str,
    token: &SchemaAlkaneId,
) -> Result<Vec<(u32, u128)>> {
    let total = get_address_index_list_len(
        essentials,
        StateAt::Latest,
        AddressIndexListKind::OutpointIdx,
        address,
    )?;
    if total == 0 {
        return Ok(Vec::new());
    }
    if total > MAX_ADDRESS_OUTPOINTS {
        return Err(anyhow!(
            "non_circulating address {address} has {total} outpoints, over the {MAX_ADDRESS_OUTPOINTS} cap"
        ));
    }
    let ids = get_address_index_list_range(
        essentials,
        StateAt::Latest,
        AddressIndexListKind::OutpointIdx,
        address,
        0,
        total,
    )?;

    // Block hash -> height is a tree lookup, not an essentials one.
    let tree = crate::runtime::tree_db::get_global_tree_db().ok_or_else(|| {
        anyhow!("cmcap needs the versioned tree to map outpoint blocks to heights")
    })?;
    let mut height_by_blockhash: HashMap<[u8; 32], Option<u32>> = HashMap::new();
    let mut deltas: Vec<(u32, i128)> = Vec::new();
    for id in ids {
        let Some(blob) = load_outpoint_pointer_blob_v3_by_id(essentials, id) else { continue };
        let amount: u128 = blob
            .balances
            .iter()
            .filter(|b| b.alkane == *token)
            .fold(0u128, |acc, b| acc.saturating_add(b.amount));
        if amount == 0 {
            continue;
        }
        let created = match height_by_blockhash.get(&blob.blockhash) {
            Some(h) => *h,
            None => {
                let h = tree.height_for_blockhash(&BlockHash::from_byte_array(blob.blockhash))?;
                height_by_blockhash.insert(blob.blockhash, h);
                h
            }
        };
        let Some(created) = created else { continue };
        deltas.push((created, clamp_i128(amount)));

        if let Some(spent_txid) = resolve_outpoint_spent_by_id_v2(essentials, StateAt::Latest, id)?
        {
            if let Some(summary) =
                load_tx_summary_v2(essentials, &Txid::from_byte_array(spent_txid))
            {
                deltas.push((summary.height, -clamp_i128(amount)));
            }
        }
    }
    Ok(series_from_deltas(deltas))
}

fn clamp_i128(v: u128) -> i128 {
    v.min(i128::MAX as u128) as i128
}

/// Fold signed per-height deltas into a `(height, balance)` series, one entry per
/// height the balance changed at.
fn series_from_deltas(mut deltas: Vec<(u32, i128)>) -> Vec<(u32, u128)> {
    deltas.sort_by_key(|(h, _)| *h);
    let mut out: Vec<(u32, u128)> = Vec::new();
    let mut balance: i128 = 0;
    let mut i = 0;
    while i < deltas.len() {
        let h = deltas[i].0;
        while i < deltas.len() && deltas[i].0 == h {
            balance = balance.saturating_add(deltas[i].1);
            i += 1;
        }
        let b = balance.max(0) as u128;
        if out.last().map(|(_, prev)| *prev) != Some(b) {
            out.push((h, b));
        }
    }
    out
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
    fn deltas_fold_into_a_balance_series_with_one_entry_per_change() {
        // Outpoints created at 100 and 105; the first spent at 110 while another of
        // equal size lands at 110, so 110 is a no-op; nothing at 120.
        let series =
            series_from_deltas(vec![(105, 50), (100, 100), (110, -100), (110, 100), (120, 0)]);
        assert_eq!(series, vec![(100, 100), (105, 150)]);
    }

    #[test]
    fn deltas_never_drive_a_balance_negative() {
        let series = series_from_deltas(vec![(10, 5), (20, -9)]);
        assert_eq!(series, vec![(10, 5), (20, 0)]);
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
