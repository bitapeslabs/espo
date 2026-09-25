//! One-time backfill of the aggregated TVL lines.
//!
//! This does not re-run traces. Every input it needs is already indexed:
//!
//! * `essentials` keeps a per-height log of which alkanes' balances moved and by
//!   how much, which is the same feed the live indexer replays to track reserves.
//! * `ammdata` keeps the btc/usd price per height, the canonical-pool candles that
//!   give a token's price in sats, and the per-token USD candles.
//!
//! So the backfill is a single forward walk over the height range, carrying the
//! same running totals `index_tvl` maintains block to block. It is guarded by a
//! marker key holding the height it completed through, and it writes that marker
//! only at the end - an interrupted run simply starts over.

use crate::modules::ammdata::consts::{
    AMOUNT_SCALE, CanonicalQuoteUnit, ammdata_genesis_block, canonical_quotes_at_height,
};
use crate::modules::ammdata::schemas::{
    SchemaMarketDefs, SchemaTvlPointV1, Timeframe, active_timeframes,
};
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetListEntriesDescRangeParams, GetRawValueParams, SetBatchParams,
    decode_candle_v1, decode_full_candle_v1, decode_u128_value, encode_tvl_point_v1,
    encode_u128_value,
};
use crate::modules::ammdata::utils::candles::bucket_start_for;
use crate::modules::ammdata::utils::index_snapshot::{
    load_reserves_snapshot, pools_map_from_snapshot,
};
use crate::modules::ammdata::utils::index_tvl::{
    PoolAnchorInput, pool_anchor_point, side_tvl_sats as anchor_side_tvl_sats,
};
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, EssentialsProvider,
    GetListEntriesDescParams as EssentialsGetListEntriesDescParams, decode_pointer_idx_u64,
    load_tx_pointer_blob_v3_by_id,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use std::collections::HashMap;

/// Flush accumulated puts once they reach this many keys.
const FLUSH_EVERY_PUTS: usize = 50_000;
/// Block summaries are fetched in batches of this many heights.
const BLOCK_TIME_BATCH: usize = 1_000;
/// How often to log progress, in heights.
const LOG_EVERY_HEIGHTS: u32 = 5_000;

/// Height the backfill last completed through, or `None` if it has never finished.
pub fn backfill_done_through(provider: &AmmDataProvider) -> Result<Option<u32>> {
    let key = provider.table().tvl_line_backfill_key();
    let raw = provider
        .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
        .value;
    Ok(raw
        .and_then(|bytes| decode_u128_value(&bytes).ok())
        .map(|h| h as u32)
        .filter(|h| *h > 0))
}

/// Returns true when a backfill ran.
///
/// Called from `index_block`, so it only fires when a block actually arrives. A
/// restart at the tip does not trigger it until the next block does; `set_mdb`
/// logs that at startup so nobody waits on a log line that cannot appear yet.
pub fn maybe_backfill_tvl_lines(
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
    network: Network,
    tip_height: u32,
) -> Result<bool> {
    let table = provider.table();
    let marker_key = table.tvl_line_backfill_key();
    if backfill_done_through(provider)?.is_some() {
        return Ok(false);
    }

    let genesis = ammdata_genesis_block(network);
    if tip_height <= genesis {
        return Ok(false);
    }
    // Stop one short of the tip: the live path owns the block currently being indexed.
    let end_height = tip_height.saturating_sub(1);

    let reserves_snapshot = load_reserves_snapshot(provider)?;
    let pools_map = pools_map_from_snapshot(&reserves_snapshot);
    if pools_map.is_empty() {
        return Ok(false);
    }

    eprintln!(
        "[AMMDATA] tvl line backfill starting: heights {genesis}..={end_height}, {} pools",
        pools_map.len()
    );
    let started = std::time::Instant::now();

    let mut walker = BackfillWalker::new(provider, essentials, network, pools_map);
    walker.run(genesis, end_height)?;
    let stats = walker.finish(end_height, &marker_key)?;

    eprintln!(
        "[AMMDATA] tvl line backfill done in {:?}: heights_with_activity={}, points={}, totals={}",
        started.elapsed(),
        stats.heights_with_activity,
        stats.line_points,
        stats.total_points
    );
    Ok(true)
}

#[derive(Default)]
struct BackfillStats {
    heights_with_activity: u64,
    line_points: u64,
    total_points: u64,
}

struct BackfillWalker<'a> {
    provider: &'a AmmDataProvider,
    essentials: &'a EssentialsProvider,
    network: Network,
    pools_map: HashMap<SchemaAlkaneId, SchemaMarketDefs>,
    /// Pools that quote a given token in a canonical asset, for sats pricing.
    canonical_pools_by_token: HashMap<SchemaAlkaneId, Vec<(SchemaAlkaneId, SchemaAlkaneId)>>,
    reserves: HashMap<SchemaAlkaneId, (u128, u128)>,
    anchors: HashMap<SchemaAlkaneId, SchemaTvlPointV1>,
    global: SchemaTvlPointV1,
    token_totals: HashMap<SchemaAlkaneId, SchemaTvlPointV1>,
    block_times: HashMap<u32, u64>,
    block_times_range: Option<(u32, u32)>,
    price_sats_cache: HashMap<(SchemaAlkaneId, u64), u128>,
    price_usd_cache: HashMap<(SchemaAlkaneId, u64), u128>,
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    timeframes: Vec<Timeframe>,
    stats: BackfillStats,
}

impl<'a> BackfillWalker<'a> {
    fn new(
        provider: &'a AmmDataProvider,
        essentials: &'a EssentialsProvider,
        network: Network,
        pools_map: HashMap<SchemaAlkaneId, SchemaMarketDefs>,
    ) -> Self {
        // A token can be priced in sats when some pool pairs it against a canonical
        // quote. Built once from the final pool set; a pool that did not exist yet at
        // a given height simply has no candle there, so it contributes no price.
        let mut canonical_pools_by_token: HashMap<
            SchemaAlkaneId,
            Vec<(SchemaAlkaneId, SchemaAlkaneId)>,
        > = HashMap::new();
        let canonical_ever: Vec<SchemaAlkaneId> =
            canonical_quotes_at_height(network, 0).into_iter().map(|q| q.id).collect();
        for (pool, defs) in pools_map.iter() {
            if canonical_ever.contains(&defs.quote_alkane_id) {
                canonical_pools_by_token
                    .entry(defs.base_alkane_id)
                    .or_default()
                    .push((*pool, defs.quote_alkane_id));
            }
            if canonical_ever.contains(&defs.base_alkane_id) {
                canonical_pools_by_token
                    .entry(defs.quote_alkane_id)
                    .or_default()
                    .push((*pool, defs.base_alkane_id));
            }
        }

        Self {
            provider,
            essentials,
            network,
            pools_map,
            canonical_pools_by_token,
            reserves: HashMap::new(),
            anchors: HashMap::new(),
            global: SchemaTvlPointV1::default(),
            token_totals: HashMap::new(),
            block_times: HashMap::new(),
            block_times_range: None,
            price_sats_cache: HashMap::new(),
            price_usd_cache: HashMap::new(),
            puts: Vec::new(),
            timeframes: active_timeframes(),
            stats: BackfillStats::default(),
        }
    }

    fn run(&mut self, genesis: u32, end_height: u32) -> Result<()> {
        let mut height = genesis;
        while height <= end_height {
            let needs_load = match self.block_times_range {
                Some((from, to)) => height < from || height > to,
                None => true,
            };
            if needs_load {
                self.load_block_times(height, end_height)?;
            }
            self.step(height)?;

            if self.puts.len() >= FLUSH_EVERY_PUTS {
                self.flush()?;
            }
            if height % LOG_EVERY_HEIGHTS == 0 {
                eprintln!(
                    "[AMMDATA] tvl line backfill at height {height}/{end_height} (points={})",
                    self.stats.line_points
                );
            }
            height = height.saturating_add(1);
        }
        Ok(())
    }

    fn load_block_times(&mut self, from: u32, end_height: u32) -> Result<()> {
        let last = from.saturating_add(BLOCK_TIME_BATCH as u32 - 1).min(end_height);
        let heights: Vec<u32> = (from..=last).collect();
        let summaries = self.essentials.get_block_summaries_by_heights(&heights)?;
        self.block_times.clear();
        self.block_times_range = Some((from, last));
        for (height, summary) in heights.into_iter().zip(summaries.into_iter()) {
            if let Some(ts) = summary.as_ref().and_then(|s| s.block_time()) {
                self.block_times.insert(height, ts);
            }
        }
        Ok(())
    }

    /// Apply one height: move reserves, re-anchor the pools that changed, and stamp
    /// the new totals onto this height's buckets.
    fn step(&mut self, height: u32) -> Result<()> {
        let balance_txs =
            load_pool_balance_txs_by_height(self.essentials, height, &self.pools_map)?;
        if balance_txs.is_empty() {
            return Ok(());
        }

        let mut touched: Vec<SchemaAlkaneId> = Vec::new();
        for (owner, entries) in balance_txs {
            let Some(defs) = self.pools_map.get(&owner).copied() else { continue };
            let slot = self.reserves.entry(owner).or_insert((0, 0));
            let mut changed = false;
            for entry in entries {
                let base_delta = crate::modules::ammdata::signed_from_delta(
                    entry.outflow.get(&defs.base_alkane_id),
                );
                let quote_delta = crate::modules::ammdata::signed_from_delta(
                    entry.outflow.get(&defs.quote_alkane_id),
                );
                if base_delta == 0 && quote_delta == 0 {
                    continue;
                }
                slot.0 = crate::modules::ammdata::apply_delta_u128(slot.0, base_delta);
                slot.1 = crate::modules::ammdata::apply_delta_u128(slot.1, quote_delta);
                changed = true;
            }
            if changed && !touched.contains(&owner) {
                touched.push(owner);
            }
        }

        if touched.is_empty() {
            return Ok(());
        }
        let Some(block_ts) = self.block_times.get(&height).copied() else { return Ok(()) };
        self.stats.heights_with_activity = self.stats.heights_with_activity.saturating_add(1);

        let btc_usd = self
            .provider
            .get_btc_usd_price_entry_at_or_before_height(u64::from(height))?
            .map(|(_h, price)| price);
        let mut canonical_quote_units: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for quote in canonical_quotes_at_height(self.network, height) {
            canonical_quote_units.insert(quote.id, quote.unit);
        }

        let mut changed_tokens: Vec<SchemaAlkaneId> = Vec::new();
        for pool in touched.iter() {
            let Some(defs) = self.pools_map.get(pool).copied() else { continue };
            let (base_reserve, quote_reserve) = self.reserves.get(pool).copied().unwrap_or((0, 0));

            let base_price_sats =
                self.token_price_sats(&defs.base_alkane_id, &canonical_quote_units, block_ts);
            let quote_price_sats =
                self.token_price_sats(&defs.quote_alkane_id, &canonical_quote_units, block_ts);
            let base_tvl_sats = self.side_tvl_sats(
                &defs.base_alkane_id,
                base_reserve,
                base_price_sats,
                &canonical_quote_units,
                btc_usd,
            );
            let quote_tvl_sats = self.side_tvl_sats(
                &defs.quote_alkane_id,
                quote_reserve,
                quote_price_sats,
                &canonical_quote_units,
                btc_usd,
            );
            let base_tvl_usd = self.side_tvl_usd(
                &defs.base_alkane_id,
                base_reserve,
                &canonical_quote_units,
                btc_usd,
                block_ts,
            );
            let quote_tvl_usd = self.side_tvl_usd(
                &defs.quote_alkane_id,
                quote_reserve,
                &canonical_quote_units,
                btc_usd,
                block_ts,
            );

            let next = pool_anchor_point(
                &defs,
                &canonical_quote_units,
                &PoolAnchorInput {
                    base_tvl_sats,
                    quote_tvl_sats,
                    base_price_sats,
                    quote_price_sats,
                    base_tvl_usd,
                    quote_tvl_usd,
                },
                btc_usd,
            );
            let prev = self.anchors.get(pool).copied().unwrap_or_default();
            if prev == next {
                continue;
            }
            self.anchors.insert(*pool, next);

            self.global = rebase(&self.global, &prev, &next);
            let mut tokens = vec![defs.base_alkane_id];
            if defs.quote_alkane_id != defs.base_alkane_id {
                tokens.push(defs.quote_alkane_id);
            }
            for token in tokens {
                let entry = self.token_totals.entry(token).or_default();
                *entry = rebase(entry, &prev, &next);
                if !changed_tokens.contains(&token) {
                    changed_tokens.push(token);
                }
            }
        }

        if changed_tokens.is_empty() {
            return Ok(());
        }

        let table = self.provider.table();
        let global_encoded = encode_tvl_point_v1(&self.global)?;
        self.puts
            .push((table.amm_tvl_total_key(u64::from(height)), global_encoded.clone()));
        self.stats.total_points = self.stats.total_points.saturating_add(1);
        for tf in self.timeframes.iter() {
            let bucket_ts = bucket_start_for(block_ts, *tf);
            self.puts.push((table.amm_tvl_line_key(*tf, bucket_ts), global_encoded.clone()));
            self.stats.line_points = self.stats.line_points.saturating_add(1);
        }

        for token in changed_tokens.iter() {
            let point = self.token_totals.get(token).copied().unwrap_or_default();
            let encoded = encode_tvl_point_v1(&point)?;
            self.puts
                .push((table.token_tvl_total_key(token, u64::from(height)), encoded.clone()));
            self.stats.total_points = self.stats.total_points.saturating_add(1);
            for tf in self.timeframes.iter() {
                let bucket_ts = bucket_start_for(block_ts, *tf);
                self.puts
                    .push((table.token_tvl_line_key(token, *tf, bucket_ts), encoded.clone()));
                self.stats.line_points = self.stats.line_points.saturating_add(1);
            }
        }

        Ok(())
    }

    /// Value one side of a pool in sats, through the same helper the live path uses
    /// so the two can never disagree on scaling.
    fn side_tvl_sats(
        &self,
        token: &SchemaAlkaneId,
        amount: u128,
        price_sats: u128,
        canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
        btc_usd: Option<u128>,
    ) -> u128 {
        anchor_side_tvl_sats(token, amount, price_sats, canonical_quote_units, btc_usd)
    }

    fn side_tvl_usd(
        &mut self,
        token: &SchemaAlkaneId,
        amount: u128,
        canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
        btc_usd: Option<u128>,
        block_ts: u64,
    ) -> u128 {
        if let Some(unit) = canonical_quote_units.get(token).copied() {
            return crate::modules::ammdata::canonical_quote_amount_tvl_usd(amount, unit, btc_usd)
                .unwrap_or(0);
        }
        let price_usd = self.token_price_usd(token, block_ts);
        amount.saturating_mul(price_usd).saturating_div(AMOUNT_SCALE)
    }

    /// A token's price in sats at `block_ts`, taken from the 10m candle of a pool
    /// that quotes it against BTC - the same source the live indexer uses.
    fn token_price_sats(
        &mut self,
        token: &SchemaAlkaneId,
        canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
        block_ts: u64,
    ) -> u128 {
        if canonical_quote_units.contains_key(token) {
            return 0;
        }
        let bucket = bucket_start_for(block_ts, Timeframe::M10);
        if let Some(price) = self.price_sats_cache.get(&(*token, bucket)) {
            return *price;
        }

        let mut price = 0u128;
        if let Some(entries) = self.canonical_pools_by_token.get(token).cloned() {
            for (pool, quote_id) in entries {
                if canonical_quote_units.get(&quote_id).copied() != Some(CanonicalQuoteUnit::Btc) {
                    continue;
                }
                let Some(defs) = self.pools_map.get(&pool).copied() else { continue };
                let use_base_price =
                    defs.base_alkane_id == *token && defs.quote_alkane_id == quote_id;
                let use_quote_price =
                    defs.quote_alkane_id == *token && defs.base_alkane_id == quote_id;
                if !use_base_price && !use_quote_price {
                    continue;
                }
                if let Some(candle) = self.load_candle_at_or_before(&pool, bucket) {
                    price = if use_base_price {
                        candle.base_candle.close
                    } else {
                        candle.quote_candle.close
                    };
                    break;
                }
            }
        }
        self.price_sats_cache.insert((*token, bucket), price);
        price
    }

    /// A token's USD price at `block_ts`, from its own USD candle series.
    fn token_price_usd(&mut self, token: &SchemaAlkaneId, block_ts: u64) -> u128 {
        let bucket = bucket_start_for(block_ts, Timeframe::M10);
        if let Some(price) = self.price_usd_cache.get(&(*token, bucket)) {
            return *price;
        }
        let table = self.provider.table();
        let prefix = table.token_usd_candle_ns_prefix(token, Timeframe::M10);
        let price = newest_entry_at_or_before(self.provider, prefix, bucket)
            .and_then(|(_key, value)| decode_candle_v1(&value).ok())
            .map(|candle| candle.close)
            .unwrap_or(0);
        self.price_usd_cache.insert((*token, bucket), price);
        price
    }

    fn load_candle_at_or_before(
        &self,
        pool: &SchemaAlkaneId,
        bucket: u64,
    ) -> Option<crate::modules::ammdata::schemas::SchemaFullCandleV1> {
        let table = self.provider.table();
        let prefix = table.candle_ns_prefix(pool, Timeframe::M10);
        newest_entry_at_or_before(self.provider, prefix, bucket)
            .and_then(|(_key, value)| decode_full_candle_v1(&value).ok())
    }

    fn flush(&mut self) -> Result<()> {
        if self.puts.is_empty() {
            return Ok(());
        }
        let puts = std::mem::take(&mut self.puts);
        self.provider.set_batch(SetBatchParams {
            blockhash: StateAt::Latest,
            puts,
            deletes: Vec::new(),
        })?;
        Ok(())
    }

    /// Persist the final per-pool anchors plus the completion marker, so the live
    /// path can pick up from here with plain deltas.
    fn finish(mut self, end_height: u32, marker_key: &[u8]) -> Result<BackfillStats> {
        let table = self.provider.table();
        let anchors: Vec<(SchemaAlkaneId, SchemaTvlPointV1)> =
            self.anchors.iter().map(|(pool, point)| (*pool, *point)).collect();
        for (pool, point) in anchors {
            if point.is_zero() {
                continue;
            }
            self.puts.push((table.pool_tvl_anchor_key(&pool), encode_tvl_point_v1(&point)?));
        }
        self.puts
            .push((marker_key.to_vec(), encode_u128_value(u128::from(end_height))?));
        self.flush()?;
        Ok(self.stats)
    }
}

/// The by-height balance log, restricted to pool owners.
///
/// Same shape as `load_balance_txs_by_height`, but the owner is part of the log
/// key, so non-pool owners are dropped *before* their pointer blob is loaded. At a
/// busy height most balance changes belong to wallets, not pools, and each blob is
/// a point read - skipping them is most of the remaining per-height cost.
fn load_pool_balance_txs_by_height(
    essentials: &EssentialsProvider,
    height: u32,
    pools: &HashMap<SchemaAlkaneId, SchemaMarketDefs>,
) -> Result<HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>> {
    let table = essentials.table();
    let prefix = table.alkane_balance_txs_by_height_log_prefix(height);
    let entries = essentials
        .get_list_entries_desc(EssentialsGetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix,
        })?
        .entries;

    let mut with_idx: HashMap<SchemaAlkaneId, Vec<(u32, AlkaneBalanceTxEntry)>> = HashMap::new();
    for (key, value) in entries {
        let Some((tx_idx, owner)) = table.parse_alkane_balance_txs_by_height_log_key(height, &key)
        else {
            continue;
        };
        if !pools.contains_key(&owner) {
            continue;
        }
        let Ok(entry_id) = decode_pointer_idx_u64(&value) else { continue };
        let Some(blob) = load_tx_pointer_blob_v3_by_id(essentials, entry_id) else { continue };
        with_idx.entry(owner).or_default().push((
            tx_idx,
            AlkaneBalanceTxEntry {
                txid: blob.txid,
                height: blob.height,
                outflow: blob.outflows.get(&owner).cloned().unwrap_or_default(),
            },
        ));
    }

    let mut out: HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>> = HashMap::new();
    for (owner, mut list) in with_idx {
        // Deltas must apply in intra-block tx order, as the live path does.
        list.sort_by_key(|(tx_idx, _)| *tx_idx);
        out.insert(owner, list.into_iter().map(|(_, e)| e).collect());
    }
    Ok(out)
}

/// Replace a pool's old contribution to a total with its new one.
fn rebase(
    total: &SchemaTvlPointV1,
    prev: &SchemaTvlPointV1,
    next: &SchemaTvlPointV1,
) -> SchemaTvlPointV1 {
    SchemaTvlPointV1 {
        canonical_sats: total
            .canonical_sats
            .saturating_sub(prev.canonical_sats)
            .saturating_add(next.canonical_sats),
        derived_sats: total
            .derived_sats
            .saturating_sub(prev.derived_sats)
            .saturating_add(next.derived_sats),
        unanchored_sats: total
            .unanchored_sats
            .saturating_sub(prev.unanchored_sats)
            .saturating_add(next.unanchored_sats),
    }
}

/// Newest entry in a candle namespace at or before `bucket`, as one bounded scan.
///
/// Candle keys end in the bucket timestamp written as decimal ASCII, so the range
/// bound is the prefix plus `bucket + 1`. Lexicographic order matches numeric order
/// as long as the timestamps share a digit count, which holds for every second from
/// 2001 to 2286. Scanning the whole namespace instead - as an earlier version did -
/// cost a full list read per token per 10-minute bucket and dominated the walk.
fn newest_entry_at_or_before(
    provider: &AmmDataProvider,
    ns_prefix: Vec<u8>,
    bucket: u64,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut end_exclusive = ns_prefix.clone();
    end_exclusive.extend_from_slice(bucket.saturating_add(1).to_string().as_bytes());
    provider
        .get_list_entries_desc_range(GetListEntriesDescRangeParams {
            blockhash: StateAt::Latest,
            start_inclusive: ns_prefix,
            end_exclusive: Some(end_exclusive),
            limit: 1,
        })
        .ok()?
        .entries
        .into_iter()
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebase_swaps_one_pools_contribution() {
        let total =
            SchemaTvlPointV1 { canonical_sats: 1_000, derived_sats: 500, unanchored_sats: 0 };
        let prev = SchemaTvlPointV1 { canonical_sats: 300, ..Default::default() };
        let next = SchemaTvlPointV1 { canonical_sats: 900, ..Default::default() };

        let out = rebase(&total, &prev, &next);
        assert_eq!(out.canonical_sats, 1_600);
        assert_eq!(out.derived_sats, 500);
    }
}
