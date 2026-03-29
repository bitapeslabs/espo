use crate::modules::ammdata::config::{DerivedMergeStrategy, DerivedQuoteConfig};
use crate::modules::ammdata::consts::{AMOUNT_SCALE, CanonicalQuoteUnit, PRICE_SCALE};
use crate::modules::ammdata::price_feeds::{
    PriceFeed, UniswapPriceFeed, get_historical_btc_usd_price,
};
use crate::modules::ammdata::schemas::{
    SchemaCandleV1, SchemaCanonicalPoolEntry, SchemaFullCandleV1, SchemaTokenMetricsV1, Timeframe,
    active_timeframes,
};
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetListEntriesDescParams, GetRawValueParams, SchemaChartChangeSetV1,
    SchemaChartChangeValueV1, SearchIndexField, TokenMetricsIndexField, decode_candle_v1,
    decode_chart_change_set_v1, decode_full_candle_v1, decode_token_metrics, decode_u128_value,
    encode_alkane_id_be, encode_candle_v1, encode_chart_change_set_v1, encode_token_metrics,
    encode_u128_value, parse_change_basis_points,
};
use crate::modules::ammdata::utils::candles::bucket_start_for;
use crate::modules::ammdata::utils::index_state::IndexState;
use crate::modules::ammdata::utils::search::collect_search_prefixes;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationRecordParams, GetRawValueParams as EssentialsGetRawValueParams,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use std::collections::{BTreeMap, HashMap, HashSet};

fn parse_bucket_ts_from_key(key: &[u8]) -> Option<u64> {
    let ts_bytes = key.rsplit(|&b| b == b':').next()?;
    let ts_str = std::str::from_utf8(ts_bytes).ok()?;
    ts_str.parse::<u64>().ok()
}

fn read_m10_candles_map(
    provider: &AmmDataProvider,
    prefix: Vec<u8>,
) -> Result<BTreeMap<u64, SchemaCandleV1>> {
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams { blockhash: StateAt::Latest, prefix })?
        .entries
    {
        let Some(ts) = parse_bucket_ts_from_key(&k) else {
            continue;
        };
        if per_bucket.contains_key(&ts) {
            continue;
        }
        if let Ok(c) = decode_candle_v1(&v) {
            per_bucket.insert(ts, c);
        }
    }
    Ok(per_bucket)
}

fn close_at_or_before(per_bucket: &BTreeMap<u64, SchemaCandleV1>, target_bucket: u64) -> u128 {
    if per_bucket.is_empty() {
        return 0;
    }
    if let Some((_ts, candle)) = per_bucket.range(..=target_bucket).next_back() {
        return candle.close;
    }
    per_bucket.values().next().map(|c| c.close).unwrap_or(0)
}

fn build_chart_change_set(
    now_bucket: u64,
    usd_per_bucket: &BTreeMap<u64, SchemaCandleV1>,
    mcusd_per_bucket: &BTreeMap<u64, SchemaCandleV1>,
) -> SchemaChartChangeSetV1 {
    let latest_usd = close_at_or_before(usd_per_bucket, now_bucket);
    let latest_mcusd = close_at_or_before(mcusd_per_bucket, now_bucket);
    let mut timeframe_changes: BTreeMap<String, SchemaChartChangeValueV1> = BTreeMap::new();
    for tf in active_timeframes() {
        let lookback_target = now_bucket.saturating_sub(tf.duration_secs());
        let prev_usd = close_at_or_before(usd_per_bucket, lookback_target);
        let mut prev_mcusd = close_at_or_before(mcusd_per_bucket, lookback_target);
        let mut now_mcusd = latest_mcusd;
        if now_mcusd == 0 || prev_mcusd == 0 {
            now_mcusd = latest_usd;
            prev_mcusd = prev_usd;
        }
        timeframe_changes.insert(
            tf.code().to_string(),
            SchemaChartChangeValueV1 {
                usd_bp: crate::modules::ammdata::main::percent_change_basis_points(
                    prev_usd, latest_usd,
                ),
                mcusd_bp: crate::modules::ammdata::main::percent_change_basis_points(
                    prev_mcusd, now_mcusd,
                ),
            },
        );
    }
    SchemaChartChangeSetV1 { timeframe_changes }
}

fn diff_chart_change_set(
    prev: Option<&SchemaChartChangeSetV1>,
    next: &SchemaChartChangeSetV1,
) -> SchemaChartChangeSetV1 {
    let mut timeframe_changes: BTreeMap<String, SchemaChartChangeValueV1> = BTreeMap::new();
    for (tf, value) in &next.timeframe_changes {
        let changed = prev
            .and_then(|p| p.timeframe_changes.get(tf))
            .map(|old| old != value)
            .unwrap_or(true);
        if changed {
            timeframe_changes.insert(tf.clone(), value.clone());
        }
    }
    SchemaChartChangeSetV1 { timeframe_changes }
}

fn higher_timeframes() -> [Timeframe; 5] {
    [Timeframe::H1, Timeframe::H4, Timeframe::D1, Timeframe::W1, Timeframe::M1]
}

fn read_m10_full_candles_map(
    provider: &AmmDataProvider,
    prefix: Vec<u8>,
) -> Result<BTreeMap<u64, SchemaFullCandleV1>> {
    let mut per_bucket: BTreeMap<u64, SchemaFullCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams { blockhash: StateAt::Latest, prefix })?
        .entries
    {
        let Some(ts) = parse_bucket_ts_from_key(&k) else {
            continue;
        };
        if per_bucket.contains_key(&ts) {
            continue;
        }
        if let Ok(c) = decode_full_candle_v1(&v) {
            per_bucket.insert(ts, c);
        }
    }
    Ok(per_bucket)
}

fn aggregate_candle_from_m10(
    per_bucket: &BTreeMap<u64, SchemaCandleV1>,
    tf: Timeframe,
    bucket_start: u64,
) -> Option<SchemaCandleV1> {
    if per_bucket.is_empty() {
        return None;
    }
    if tf == Timeframe::M10 {
        return per_bucket.get(&bucket_start).copied();
    }

    let step = Timeframe::M10.duration_secs();
    let bucket_end = bucket_start.saturating_add(tf.duration_secs());
    let mut ts = bucket_start;
    let mut last_close = close_at_or_before(per_bucket, bucket_start.saturating_sub(step));

    let mut out: Option<SchemaCandleV1> = None;
    let mut had_real = false;
    while ts < bucket_end {
        let mut sample = if let Some(c) = per_bucket.get(&ts).copied() {
            had_real = true;
            c
        } else {
            SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            }
        };

        sample.open = last_close;
        if sample.open > sample.high {
            sample.high = sample.open;
        }
        if sample.open < sample.low {
            sample.low = sample.open;
        }
        last_close = sample.close;

        if let Some(acc) = out.as_mut() {
            acc.high = acc.high.max(sample.high);
            acc.low = acc.low.min(sample.low);
            acc.close = sample.close;
            acc.volume = acc.volume.saturating_add(sample.volume);
        } else {
            out = Some(sample);
        }

        ts = match ts.checked_add(step) {
            Some(v) => v,
            None => break,
        };
    }

    if had_real { out } else { None }
}

fn aggregate_full_candle_from_m10(
    per_bucket: &BTreeMap<u64, SchemaFullCandleV1>,
    tf: Timeframe,
    bucket_start: u64,
) -> Option<SchemaFullCandleV1> {
    if per_bucket.is_empty() {
        return None;
    }
    let mut base_map: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut quote_map: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (ts, c) in per_bucket {
        base_map.insert(*ts, c.base_candle);
        quote_map.insert(*ts, c.quote_candle);
    }
    let base = aggregate_candle_from_m10(&base_map, tf, bucket_start)?;
    let quote = aggregate_candle_from_m10(&quote_map, tf, bucket_start)?;
    Some(SchemaFullCandleV1 { base_candle: base, quote_candle: quote })
}

pub fn derive_token_data(
    block_ts: u64,
    height: u32,
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    derived_quotes: &[DerivedQuoteConfig],
    use_historical_backfill: bool,
    search_index_enabled: bool,
    search_prefix_min: usize,
    search_prefix_max: usize,
    state: &mut IndexState,
) -> Result<()> {
    let table = provider.table();
    // Essentials circulating supply is amount-scaled (1e8). Market-cap outputs are price-scaled
    // (1e16), so we multiply by supply and divide by AMOUNT_SCALE.
    let scale_price_by_supply = |price_scaled: u128, supply_amount_scaled: u128| -> u128 {
        price_scaled.saturating_mul(supply_amount_scaled).saturating_div(AMOUNT_SCALE)
    };

    // ---------- btc/usd price ----------
    if state.has_trades {
        let mut price: Option<u128> = None;
        if use_historical_backfill {
            match get_historical_btc_usd_price(height as u64) {
                Ok(Some(v)) => price = Some(v),
                Ok(None) => {}
                Err(e) => {
                    eprintln!(
                        "[AMMDATA] btc/usd historical backfill failed at height {height}: {e:?}"
                    );
                }
            }
        }
        if price.is_none() {
            match UniswapPriceFeed::from_global_config() {
                Ok(feed) => match feed.get_bitcoin_price_usd_at_block_height(height as u64) {
                    Ok(v) => price = Some(v),
                    Err(e) => {
                        eprintln!("[AMMDATA] btc/usd price_feed failed at height {height}: {e:?}");
                    }
                },
                Err(e) => {
                    eprintln!("[AMMDATA] btc/usd price_feed init failed at height {height}: {e:?}")
                }
            };
        }

        if price.is_none() {
            let key = table.btc_usd_price_key(height as u64);
            price = provider
                .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
                .value
                .and_then(|raw| decode_u128_value(&raw).ok());
        }
        if price.is_none() {
            let prefix = table.btc_usd_price_prefix();
            if let Ok(resp) = provider.get_list_entries_desc(GetListEntriesDescParams {
                blockhash: StateAt::Latest,
                prefix,
            }) {
                if let Some((_k, v)) = resp.entries.into_iter().next() {
                    price = decode_u128_value(&v).ok();
                }
            }
        }

        if let Some(p) = price {
            if let Ok(encoded) = encode_u128_value(p) {
                state
                    .btc_usd_price_writes
                    .push((table.btc_usd_price_key(height as u64), encoded));
            }
        }
        state.btc_usd_price = price;

        if let Some(p) = price {
            let tfs = [
                Timeframe::M10,
                Timeframe::H1,
                Timeframe::H4,
                Timeframe::D1,
                Timeframe::W1,
                Timeframe::M1,
            ];
            for tf in tfs {
                let bucket_ts = bucket_start_for(block_ts, tf);
                if let Ok(encoded) = encode_u128_value(p) {
                    state
                        .btc_usd_line_writes
                        .push((table.btc_usd_line_key(tf, bucket_ts), encoded));
                }
            }
        }
    }

    let mut canonical_pools_by_token: HashMap<SchemaAlkaneId, Vec<SchemaCanonicalPoolEntry>> =
        HashMap::new();
    for (pool, defs) in state.pools_map.iter() {
        if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
            canonical_pools_by_token
                .entry(defs.base_alkane_id)
                .or_default()
                .push(SchemaCanonicalPoolEntry { pool_id: *pool, quote_id: defs.quote_alkane_id });
        }
        if canonical_quote_units.contains_key(&defs.base_alkane_id) {
            canonical_pools_by_token
                .entry(defs.quote_alkane_id)
                .or_default()
                .push(SchemaCanonicalPoolEntry { pool_id: *pool, quote_id: defs.base_alkane_id });
        }
    }

    let (candle_writes, candle_entries) =
        std::mem::take(&mut state.candle_cache).into_writes_with_entries(provider)?;
    state.candle_writes = candle_writes;
    state.pool_candle_overrides.clear();
    for (pool, tf, bucket_ts, candle) in candle_entries {
        state.pool_candle_overrides.insert((pool, tf, bucket_ts), candle);
    }

    // Canonicalize higher pool timeframes from 10m candles so all TF highs/lows are
    // aggregation-consistent with 10m.
    let mut pools_with_m10_updates: HashSet<SchemaAlkaneId> = HashSet::new();
    for (pool, tf, _bucket_ts) in state.pool_candle_overrides.keys() {
        if *tf == Timeframe::M10 {
            pools_with_m10_updates.insert(*pool);
        }
    }
    for pool in pools_with_m10_updates {
        let mut per_bucket =
            read_m10_full_candles_map(provider, table.candle_ns_prefix(&pool, Timeframe::M10))?;
        for ((p, tf, bucket_ts), candle) in state.pool_candle_overrides.iter() {
            if *p == pool && *tf == Timeframe::M10 {
                per_bucket.insert(*bucket_ts, *candle);
            }
        }
        for tf in higher_timeframes() {
            let bucket_ts = bucket_start_for(block_ts, tf);
            let Some(agg) = aggregate_full_candle_from_m10(&per_bucket, tf, bucket_ts) else {
                continue;
            };
            state.pool_candle_overrides.insert((pool, tf, bucket_ts), agg);
            state.candle_writes.push((
                table.candle_key(&pool, tf, bucket_ts),
                crate::modules::ammdata::storage::encode_full_candle_v1(&agg)?,
            ));
        }
    }

    let load_pool_candle = |pool: &SchemaAlkaneId,
                            tf: Timeframe,
                            bucket_ts: u64|
     -> Result<Option<SchemaFullCandleV1>> {
        if let Some(c) = state.pool_candle_overrides.get(&(*pool, tf, bucket_ts)) {
            return Ok(Some(*c));
        }
        let key = table.candle_key(pool, tf, bucket_ts);
        if let Some(raw) = provider
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
        {
            return Ok(Some(decode_full_candle_v1(&raw)?));
        }
        Ok(None)
    };

    for (token, new_entries) in state.canonical_pool_updates.iter() {
        for entry in new_entries {
            state.canonical_pool_writes.push((
                table.canonical_pool_quote_key(token, &entry.quote_id),
                encode_alkane_id_be(&entry.pool_id).to_vec(),
            ));
        }
    }

    let mut token_usd_candle_overrides: HashMap<(SchemaAlkaneId, Timeframe, u64), SchemaCandleV1> =
        HashMap::new();
    let mut token_mcusd_candle_overrides: HashMap<
        (SchemaAlkaneId, Timeframe, u64),
        SchemaCandleV1,
    > = HashMap::new();
    let mut token_derived_usd_candle_overrides: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId, Timeframe, u64),
        SchemaCandleV1,
    > = HashMap::new();
    let mut token_derived_mcusd_candle_overrides: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId, Timeframe, u64),
        SchemaCandleV1,
    > = HashMap::new();
    let mut supply_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();

    if !state.canonical_trade_buckets.is_empty() || !derived_quotes.is_empty() {
        for (token, buckets) in state.canonical_trade_buckets.iter() {
            let Some(pools) = canonical_pools_by_token.get(token) else { continue };
            for (tf, bucket_ts) in buckets {
                let mut btc_candle: Option<SchemaCandleV1> = None;
                let mut usd_candle: Option<SchemaCandleV1> = None;

                for entry in pools.iter() {
                    let Some(unit) = canonical_quote_units.get(&entry.quote_id) else {
                        continue;
                    };
                    let Some(pool_candle) = load_pool_candle(&entry.pool_id, *tf, *bucket_ts)?
                    else {
                        continue;
                    };
                    let Some(defs) = state.pools_map.get(&entry.pool_id) else {
                        continue;
                    };

                    let token_is_base = defs.base_alkane_id == *token;
                    let token_is_quote = defs.quote_alkane_id == *token;
                    if !token_is_base && !token_is_quote {
                        continue;
                    }

                    let (price_candle, inverse_candle, canonical_volume) = if token_is_base {
                        if defs.quote_alkane_id != entry.quote_id {
                            continue;
                        }
                        (
                            pool_candle.base_candle,
                            pool_candle.quote_candle,
                            pool_candle.quote_candle.volume,
                        )
                    } else {
                        if defs.base_alkane_id != entry.quote_id {
                            continue;
                        }
                        (
                            pool_candle.quote_candle,
                            pool_candle.base_candle,
                            pool_candle.base_candle.volume,
                        )
                    };

                    let conv = |p: u128, inv: u128| -> Option<u128> {
                        match unit {
                            CanonicalQuoteUnit::Usd => Some(p),
                            CanonicalQuoteUnit::Btc => state.btc_usd_price.map(|btc| {
                                if p != 0 {
                                    p.saturating_mul(btc) / PRICE_SCALE
                                } else if inv != 0 {
                                    btc.saturating_mul(PRICE_SCALE) / inv
                                } else {
                                    0
                                }
                            }),
                        }
                    };
                    let conv_vol = |v: u128| -> Option<u128> {
                        match unit {
                            CanonicalQuoteUnit::Usd => Some(v),
                            CanonicalQuoteUnit::Btc => {
                                state.btc_usd_price.map(|btc| v.saturating_mul(btc) / PRICE_SCALE)
                            }
                        }
                    };

                    let Some(open) = conv(price_candle.open, inverse_candle.open) else { continue };
                    let Some(high) = conv(price_candle.high, inverse_candle.high) else { continue };
                    let Some(low) = conv(price_candle.low, inverse_candle.low) else { continue };
                    let Some(close) = conv(price_candle.close, inverse_candle.close) else {
                        continue;
                    };
                    let Some(volume) = conv_vol(canonical_volume) else { continue };

                    let converted = SchemaCandleV1 { open, high, low, close, volume };
                    match unit {
                        CanonicalQuoteUnit::Usd => {
                            usd_candle = Some(converted);
                        }
                        CanonicalQuoteUnit::Btc => {
                            btc_candle = Some(converted);
                        }
                    }
                }

                let mut derived = match (btc_candle, usd_candle) {
                    (Some(btc), Some(usd)) => SchemaCandleV1 {
                        open: (btc.open.saturating_add(usd.open)) / 2,
                        high: (btc.high.saturating_add(usd.high)) / 2,
                        low: (btc.low.saturating_add(usd.low)) / 2,
                        close: (btc.close.saturating_add(usd.close)) / 2,
                        volume: btc.volume.saturating_add(usd.volume),
                    },
                    (Some(one), None) | (None, Some(one)) => SchemaCandleV1 {
                        open: one.open,
                        high: one.high,
                        low: one.low,
                        close: one.close,
                        volume: one.volume,
                    },
                    _ => continue,
                };

                let existing =
                    if let Some(c) = token_usd_candle_overrides.get(&(*token, *tf, *bucket_ts)) {
                        Some(*c)
                    } else {
                        let key = table.token_usd_candle_key(token, *tf, *bucket_ts);
                        if let Some(raw) = provider
                            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
                            .value
                        {
                            Some(decode_candle_v1(&raw)?)
                        } else {
                            None
                        }
                    };

                let mut open = if let Some(prev) = existing {
                    prev.open
                } else {
                    let prev_bucket =
                        bucket_ts.checked_sub(tf.duration_secs()).unwrap_or(*bucket_ts);
                    if let Some(c) = token_usd_candle_overrides.get(&(*token, *tf, prev_bucket)) {
                        c.close
                    } else {
                        let key = table.token_usd_candle_key(token, *tf, prev_bucket);
                        provider
                            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
                            .value
                            .and_then(|raw| decode_candle_v1(&raw).ok())
                            .map(|c| c.close)
                            .unwrap_or(0)
                    }
                };
                if open == 0 && derived.open != 0 {
                    open = derived.open;
                }
                derived.open = open;
                if derived.open > derived.high {
                    derived.high = derived.open;
                }
                if derived.open < derived.low {
                    derived.low = derived.open;
                }

                token_usd_candle_overrides.insert((*token, *tf, *bucket_ts), derived);
            }
        }

        if !derived_quotes.is_empty() {
            #[derive(Clone, Copy)]
            struct DerivedPoolInfo {
                pool_id: SchemaAlkaneId,
                token_is_base: bool,
            }

            let mut derived_quote_strategies: HashMap<SchemaAlkaneId, DerivedMergeStrategy> =
                HashMap::new();
            for dq in derived_quotes {
                derived_quote_strategies.insert(dq.alkane, dq.strategy.clone());
            }

            let derived_quote_set: HashSet<SchemaAlkaneId> =
                derived_quotes.iter().map(|dq| dq.alkane).collect();

            let mut derived_pool_by_token_quote: HashMap<
                (SchemaAlkaneId, SchemaAlkaneId),
                DerivedPoolInfo,
            > = HashMap::new();

            let mut maybe_insert_pool =
                |token: SchemaAlkaneId,
                 quote: SchemaAlkaneId,
                 pool: SchemaAlkaneId,
                 token_is_base: bool| {
                    let key = (token, quote);
                    match derived_pool_by_token_quote.get(&key) {
                        None => {
                            derived_pool_by_token_quote
                                .insert(key, DerivedPoolInfo { pool_id: pool, token_is_base });
                        }
                        Some(existing) => {
                            let prefer = token_is_base && !existing.token_is_base;
                            let smaller = pool.block < existing.pool_id.block
                                || (pool.block == existing.pool_id.block
                                    && pool.tx < existing.pool_id.tx);
                            if prefer || (existing.token_is_base == token_is_base && smaller) {
                                derived_pool_by_token_quote
                                    .insert(key, DerivedPoolInfo { pool_id: pool, token_is_base });
                            }
                        }
                    }
                };

            for (pool, defs) in state.pools_map.iter() {
                if derived_quote_set.contains(&defs.quote_alkane_id) {
                    maybe_insert_pool(defs.base_alkane_id, defs.quote_alkane_id, *pool, true);
                }
                if derived_quote_set.contains(&defs.base_alkane_id) {
                    maybe_insert_pool(defs.quote_alkane_id, defs.base_alkane_id, *pool, false);
                }
            }

            let mut pool_to_edges: HashMap<
                SchemaAlkaneId,
                Vec<(SchemaAlkaneId, SchemaAlkaneId, bool)>,
            > = HashMap::new();
            let mut quote_to_tokens: HashMap<SchemaAlkaneId, Vec<SchemaAlkaneId>> = HashMap::new();
            let mut token_to_quotes: HashMap<SchemaAlkaneId, Vec<SchemaAlkaneId>> = HashMap::new();
            for ((token, quote), info) in derived_pool_by_token_quote.iter() {
                pool_to_edges.entry(info.pool_id).or_default().push((
                    *token,
                    *quote,
                    info.token_is_base,
                ));
                quote_to_tokens.entry(*quote).or_default().push(*token);
                token_to_quotes.entry(*token).or_default().push(*quote);
            }

            let mut pool_overrides_by_pool_tf: HashMap<
                (SchemaAlkaneId, Timeframe),
                BTreeMap<u64, SchemaFullCandleV1>,
            > = HashMap::new();
            for ((pool, tf, bucket), candle) in state.pool_candle_overrides.iter() {
                pool_overrides_by_pool_tf
                    .entry((*pool, *tf))
                    .or_default()
                    .insert(*bucket, *candle);
            }

            let mut token_usd_overrides_by_token_tf: HashMap<
                (SchemaAlkaneId, Timeframe),
                BTreeMap<u64, SchemaCandleV1>,
            > = HashMap::new();
            for ((token, tf, bucket), candle) in token_usd_candle_overrides.iter() {
                token_usd_overrides_by_token_tf
                    .entry((*token, *tf))
                    .or_default()
                    .insert(*bucket, *candle);
            }

            let mut derived_overrides_by_token_quote_tf: HashMap<
                (SchemaAlkaneId, SchemaAlkaneId, Timeframe),
                BTreeMap<u64, SchemaCandleV1>,
            > = HashMap::new();

            let parse_ts = |key: &[u8]| -> Option<u64> {
                key.rsplit(|&b| b == b':')
                    .next()
                    .and_then(|ts_bytes| std::str::from_utf8(ts_bytes).ok())
                    .and_then(|ts_str| ts_str.parse::<u64>().ok())
            };

            let latest_pool_candle = |pool: &SchemaAlkaneId,
                                      tf: Timeframe,
                                      target: u64|
             -> Option<(u64, SchemaFullCandleV1)> {
                let mut best: Option<(u64, SchemaFullCandleV1)> = None;
                if let Some(map) = pool_overrides_by_pool_tf.get(&(*pool, tf)) {
                    if let Some((&ts, candle)) = map.range(..=target).next_back() {
                        best = Some((ts, *candle));
                    }
                }
                let prefix = table.candle_ns_prefix(pool, tf);
                if let Ok(resp) = provider.get_list_entries_desc(GetListEntriesDescParams {
                    blockhash: StateAt::Latest,
                    prefix,
                }) {
                    for (k, v) in resp.entries {
                        let Some(ts) = parse_ts(&k) else { continue };
                        if ts > target {
                            continue;
                        }
                        if let Ok(c) = decode_full_candle_v1(&v) {
                            match best {
                                Some((best_ts, _)) if best_ts >= ts => {}
                                _ => best = Some((ts, c)),
                            }
                        }
                        break;
                    }
                }
                best
            };

            let latest_token_usd_candle = |token: &SchemaAlkaneId,
                                           tf: Timeframe,
                                           target: u64|
             -> Option<(u64, SchemaCandleV1)> {
                let mut best: Option<(u64, SchemaCandleV1)> = None;
                if let Some(map) = token_usd_overrides_by_token_tf.get(&(*token, tf)) {
                    if let Some((&ts, candle)) = map.range(..=target).next_back() {
                        best = Some((ts, *candle));
                    }
                }
                let prefix = table.token_usd_candle_ns_prefix(token, tf);
                if let Ok(resp) = provider.get_list_entries_desc(GetListEntriesDescParams {
                    blockhash: StateAt::Latest,
                    prefix,
                }) {
                    for (k, v) in resp.entries {
                        let Some(ts) = parse_ts(&k) else { continue };
                        if ts > target {
                            continue;
                        }
                        if let Ok(c) = decode_candle_v1(&v) {
                            match best {
                                Some((best_ts, _)) if best_ts >= ts => {}
                                _ => best = Some((ts, c)),
                            }
                        }
                        break;
                    }
                }
                best
            };

            let latest_derived_candle = |map: &HashMap<
                (SchemaAlkaneId, SchemaAlkaneId, Timeframe),
                BTreeMap<u64, SchemaCandleV1>,
            >,
                                         token: &SchemaAlkaneId,
                                         quote: &SchemaAlkaneId,
                                         tf: Timeframe,
                                         target: u64|
             -> Option<(u64, SchemaCandleV1)> {
                let mut best: Option<(u64, SchemaCandleV1)> = None;
                if let Some(bucket_map) = map.get(&(*token, *quote, tf)) {
                    if let Some((&ts, candle)) = bucket_map.range(..=target).next_back() {
                        best = Some((ts, *candle));
                    }
                }
                let prefix = table.token_derived_usd_candle_ns_prefix(token, quote, tf);
                if let Ok(resp) = provider.get_list_entries_desc(GetListEntriesDescParams {
                    blockhash: StateAt::Latest,
                    prefix,
                }) {
                    for (k, v) in resp.entries {
                        let Some(ts) = parse_ts(&k) else { continue };
                        if ts > target {
                            continue;
                        }
                        if let Ok(c) = decode_candle_v1(&v) {
                            match best {
                                Some((best_ts, _)) if best_ts >= ts => {}
                                _ => best = Some((ts, c)),
                            }
                        }
                        break;
                    }
                }
                best
            };

            let load_token_usd_candle = |token: &SchemaAlkaneId,
                                         tf: Timeframe,
                                         bucket_ts: u64|
             -> Result<Option<SchemaCandleV1>> {
                if let Some(c) = token_usd_candle_overrides.get(&(*token, tf, bucket_ts)) {
                    return Ok(Some(*c));
                }
                let key = table.token_usd_candle_key(token, tf, bucket_ts);
                if let Some(raw) = provider
                    .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
                    .value
                {
                    return Ok(Some(decode_candle_v1(&raw)?));
                }
                Ok(None)
            };

            let mut derived_buckets: HashSet<(SchemaAlkaneId, SchemaAlkaneId, Timeframe, u64)> =
                HashSet::new();
            for ((pool, tf, bucket), _candle) in state.pool_candle_overrides.iter() {
                if *tf != Timeframe::M10 {
                    continue;
                }
                if let Some(edges) = pool_to_edges.get(pool) {
                    for (token, quote, _token_is_base) in edges {
                        derived_buckets.insert((*token, *quote, *tf, *bucket));
                    }
                }
            }
            for ((quote, tf, bucket), _candle) in token_usd_candle_overrides.iter() {
                if *tf != Timeframe::M10 {
                    continue;
                }
                if let Some(tokens) = quote_to_tokens.get(quote) {
                    for token in tokens {
                        derived_buckets.insert((*token, *quote, *tf, *bucket));
                    }
                }
            }
            for ((token, tf, bucket), _candle) in token_usd_candle_overrides.iter() {
                if *tf != Timeframe::M10 {
                    continue;
                }
                if let Some(quotes) = token_to_quotes.get(token) {
                    for quote in quotes {
                        derived_buckets.insert((*token, *quote, *tf, *bucket));
                    }
                }
            }
            for ((token, tf, bucket), _candle) in token_usd_candle_overrides.iter() {
                if *tf != Timeframe::M10 {
                    continue;
                }
                for quote in derived_quote_set.iter() {
                    if token != quote {
                        derived_buckets.insert((*token, *quote, *tf, *bucket));
                    }
                }
            }

            let q_per_t_from_pool = |pool_candle: SchemaFullCandleV1,
                                     token_is_base: bool|
             -> Option<((u128, u128, u128, u128), u128)> {
                if token_is_base {
                    Some((
                        (
                            pool_candle.base_candle.open,
                            pool_candle.base_candle.high,
                            pool_candle.base_candle.low,
                            pool_candle.base_candle.close,
                        ),
                        pool_candle.base_candle.volume,
                    ))
                } else {
                    let inv_open =
                        crate::modules::ammdata::invert_price_value(pool_candle.base_candle.open)?;
                    let inv_close =
                        crate::modules::ammdata::invert_price_value(pool_candle.base_candle.close)?;
                    let inv_high =
                        crate::modules::ammdata::invert_price_value(pool_candle.base_candle.low)?;
                    let inv_low =
                        crate::modules::ammdata::invert_price_value(pool_candle.base_candle.high)?;
                    Some((
                        (inv_open, inv_high, inv_low, inv_close),
                        pool_candle.quote_candle.volume,
                    ))
                }
            };

            let derived_from_pool_with_quote_close = |pool_candle: SchemaFullCandleV1,
                                                      token_is_base: bool,
                                                      quote_close: u128|
             -> Option<SchemaCandleV1> {
                let ((q_open, q_high, q_low, q_close), token_volume) =
                    q_per_t_from_pool(pool_candle, token_is_base)?;
                let conv = |q_usd: u128, q_per_t: u128| -> Option<u128> {
                    if q_per_t == 0 {
                        None
                    } else {
                        Some(q_usd.saturating_mul(q_per_t) / PRICE_SCALE)
                    }
                };
                let open = conv(quote_close, q_open)?;
                let high = conv(quote_close, q_high)?;
                let low = conv(quote_close, q_low)?;
                let close = conv(quote_close, q_close)?;
                let volume = token_volume.saturating_mul(close) / PRICE_SCALE;
                Some(SchemaCandleV1 { open, high, low, close, volume })
            };

            let derived_from_quote_with_q_per_t = |quote_candle: SchemaCandleV1,
                                                   q_per_t: u128|
             -> Option<SchemaCandleV1> {
                if q_per_t == 0 {
                    return None;
                }
                let conv = |q_usd: u128| -> u128 { q_usd.saturating_mul(q_per_t) / PRICE_SCALE };
                Some(SchemaCandleV1 {
                    open: conv(quote_candle.open),
                    high: conv(quote_candle.high),
                    low: conv(quote_candle.low),
                    close: conv(quote_candle.close),
                    volume: 0,
                })
            };

            let apply_open = |mut candle: SchemaCandleV1, open: u128| -> SchemaCandleV1 {
                if open == 0 {
                    return candle;
                }
                candle.open = open;
                if candle.open > candle.high {
                    candle.high = candle.open;
                }
                if candle.open < candle.low {
                    candle.low = candle.open;
                }
                candle
            };

            for (token, quote, tf, bucket_ts) in derived_buckets.into_iter() {
                let info = derived_pool_by_token_quote.get(&(token, quote)).copied();

                let pool_active = info.and_then(|i| {
                    state.pool_candle_overrides.get(&(i.pool_id, tf, bucket_ts)).copied()
                });
                let quote_usd_active =
                    token_usd_candle_overrides.get(&(quote, tf, bucket_ts)).copied();
                let token_usd_active =
                    token_usd_candle_overrides.get(&(token, tf, bucket_ts)).copied();

                let pool_bucket_candle = if let Some(c) = pool_active {
                    Some(c)
                } else if let Some(info) = info {
                    load_pool_candle(&info.pool_id, tf, bucket_ts)?
                } else {
                    None
                };
                let token_usd_bucket_candle = if let Some(c) = token_usd_active {
                    Some(c)
                } else {
                    load_token_usd_candle(&token, tf, bucket_ts)?
                };

                let prev_bucket = bucket_ts.saturating_sub(tf.duration_secs());
                let last_derived_candle = latest_derived_candle(
                    &derived_overrides_by_token_quote_tf,
                    &token,
                    &quote,
                    tf,
                    prev_bucket,
                )
                .map(|(_ts, c)| c);
                let last_derived_close = last_derived_candle.map(|c| c.close).unwrap_or(0);

                if info.is_none() {
                    let mut final_candle = token_usd_bucket_candle.or_else(|| {
                        if last_derived_close != 0 {
                            Some(SchemaCandleV1 {
                                open: last_derived_close,
                                high: last_derived_close,
                                low: last_derived_close,
                                close: last_derived_close,
                                volume: 0,
                            })
                        } else {
                            None
                        }
                    });

                    if let Some(derived) = final_candle.as_mut() {
                        *derived = apply_open(*derived, last_derived_close);
                    }

                    if let Some(derived) = final_candle {
                        token_derived_usd_candle_overrides
                            .insert((token, quote, tf, bucket_ts), derived);
                        derived_overrides_by_token_quote_tf
                            .entry((token, quote, tf))
                            .or_default()
                            .insert(bucket_ts, derived);
                    }
                    continue;
                }
                let info = info.expect("checked above");

                let last_quote_usd_close =
                    latest_token_usd_candle(&quote, tf, bucket_ts).map(|(_ts, c)| c.close);
                let prev_quote_usd_close =
                    latest_token_usd_candle(&quote, tf, prev_bucket).map(|(_ts, c)| c.close);

                let last_pool_q_per_t_close = latest_pool_candle(&info.pool_id, tf, bucket_ts)
                    .and_then(|(_ts, pool_candle)| {
                        q_per_t_from_pool(pool_candle, info.token_is_base)
                            .map(|((_, _, _, close), _)| close)
                    });

                let q_per_t_const = if last_derived_close != 0 {
                    prev_quote_usd_close.and_then(|q_usd| {
                        if q_usd == 0 {
                            None
                        } else {
                            Some(last_derived_close.saturating_mul(PRICE_SCALE) / q_usd)
                        }
                    })
                } else {
                    last_pool_q_per_t_close
                };

                let strategy =
                    derived_quote_strategies.get(&quote).unwrap_or(&DerivedMergeStrategy::Neutral);

                let path_a = match (quote_usd_active, q_per_t_const) {
                    (Some(q_usd), Some(q_per_t)) => derived_from_quote_with_q_per_t(q_usd, q_per_t),
                    _ => None,
                };

                let path_b = match (pool_bucket_candle, last_quote_usd_close) {
                    (Some(pool_candle), Some(q_close)) => {
                        derived_from_pool_with_quote_close(pool_candle, info.token_is_base, q_close)
                    }
                    _ => None,
                };

                let path_c = token_usd_bucket_candle;

                let path_d = if pool_bucket_candle.is_some() && token_usd_bucket_candle.is_some() {
                    if let (Some(b), Some(c)) = (path_b, path_c) {
                        let mut merged = crate::modules::ammdata::merge_candles(b, c, strategy);
                        merged.volume = b.volume.saturating_add(c.volume);
                        Some(merged)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let merge_with_anchor =
                    |anchor_close: u128, candle: SchemaCandleV1| -> SchemaCandleV1 {
                        let anchor = SchemaCandleV1 {
                            open: anchor_close,
                            high: anchor_close,
                            low: anchor_close,
                            close: anchor_close,
                            volume: 0,
                        };
                        let mut merged =
                            crate::modules::ammdata::merge_candles(anchor, candle, strategy);
                        merged.volume = candle.volume;
                        merged
                    };

                let only_token_usd_active = token_usd_active.is_some()
                    && pool_active.is_none()
                    && quote_usd_active.is_none();
                let only_pool_active = pool_active.is_some()
                    && token_usd_active.is_none()
                    && quote_usd_active.is_none();
                let only_quote_usd_active = quote_usd_active.is_some()
                    && token_usd_active.is_none()
                    && pool_active.is_none();

                let use_anchor = matches!(
                    strategy,
                    DerivedMergeStrategy::Neutral | DerivedMergeStrategy::NeutralVwap
                );

                let token_derived = if path_d.is_some() {
                    path_d
                } else if path_b.is_some() {
                    path_b
                } else if path_c.is_some() {
                    path_c
                } else {
                    None
                };

                let token_derived =
                    if use_anchor && only_token_usd_active && last_derived_close != 0 {
                        token_derived.map(|c| merge_with_anchor(last_derived_close, c))
                    } else if use_anchor && only_pool_active && last_derived_close != 0 {
                        token_derived.map(|c| merge_with_anchor(last_derived_close, c))
                    } else {
                        token_derived
                    };

                let path_a = if use_anchor && only_quote_usd_active && last_derived_close != 0 {
                    path_a.map(|c| merge_with_anchor(last_derived_close, c))
                } else {
                    path_a
                };

                let mut final_candle = if quote_usd_active.is_some() && token_derived.is_some() {
                    if let (Some(td), Some(a)) = (token_derived, path_a) {
                        let mut merged = crate::modules::ammdata::merge_candles(td, a, strategy);
                        merged.volume = td.volume.saturating_add(a.volume);
                        Some(merged)
                    } else {
                        token_derived
                    }
                } else if token_derived.is_some() {
                    token_derived
                } else if quote_usd_active.is_some() {
                    path_a
                } else if last_derived_close != 0 {
                    Some(SchemaCandleV1 {
                        open: last_derived_close,
                        high: last_derived_close,
                        low: last_derived_close,
                        close: last_derived_close,
                        volume: 0,
                    })
                } else {
                    None
                };

                if let Some(derived) = final_candle.as_mut() {
                    *derived = apply_open(*derived, last_derived_close);
                }

                if let Some(derived) = final_candle {
                    token_derived_usd_candle_overrides
                        .insert((token, quote, tf, bucket_ts), derived);
                    derived_overrides_by_token_quote_tf
                        .entry((token, quote, tf))
                        .or_default()
                        .insert(bucket_ts, derived);
                }
            }
        }

        for ((token, quote, tf, bucket_ts), candle) in token_derived_usd_candle_overrides.iter() {
            let supply = if let Some(v) = supply_cache.get(token) {
                *v
            } else {
                let table_e = essentials.table();
                let key = table_e.circulating_supply_latest_key(token);
                let v = essentials
                    .get_raw_value(EssentialsGetRawValueParams { blockhash: StateAt::Latest, key })?
                    .value
                    .and_then(|raw| {
                        crate::modules::essentials::storage::decode_u128_value(&raw).ok()
                    })
                    .unwrap_or(0);
                supply_cache.insert(*token, v);
                v
            };
            if supply == 0 {
                continue;
            }
            let scale = |p: u128| -> u128 { scale_price_by_supply(p, supply) };
            let mc_candle = SchemaCandleV1 {
                open: scale(candle.open),
                high: scale(candle.high),
                low: scale(candle.low),
                close: scale(candle.close),
                volume: candle.volume,
            };
            token_derived_mcusd_candle_overrides
                .insert((*token, *quote, *tf, *bucket_ts), mc_candle);
        }

        for ((token, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
            let supply = if let Some(v) = supply_cache.get(token) {
                *v
            } else {
                let table_e = essentials.table();
                let key = table_e.circulating_supply_latest_key(token);
                let v = essentials
                    .get_raw_value(EssentialsGetRawValueParams { blockhash: StateAt::Latest, key })?
                    .value
                    .and_then(|raw| {
                        crate::modules::essentials::storage::decode_u128_value(&raw).ok()
                    })
                    .unwrap_or(0);
                supply_cache.insert(*token, v);
                v
            };
            if supply == 0 {
                continue;
            }
            let scale = |p: u128| -> u128 { scale_price_by_supply(p, supply) };
            let mc_candle = SchemaCandleV1 {
                open: scale(candle.open),
                high: scale(candle.high),
                low: scale(candle.low),
                close: scale(candle.close),
                volume: candle.volume,
            };
            token_mcusd_candle_overrides.insert((*token, *tf, *bucket_ts), mc_candle);
        }

        // Canonicalize higher TFs from 10m for all token/derived chart families.
        let mut tokens_with_usd_m10: HashSet<SchemaAlkaneId> = HashSet::new();
        for (token, tf, _bucket_ts) in token_usd_candle_overrides.keys() {
            if *tf == Timeframe::M10 {
                tokens_with_usd_m10.insert(*token);
            }
        }
        for token in tokens_with_usd_m10 {
            let mut per_bucket = read_m10_candles_map(
                provider,
                table.token_usd_candle_ns_prefix(&token, Timeframe::M10),
            )?;
            for ((tok, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
                if *tok == token && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket_ts, *candle);
                }
            }
            for tf in higher_timeframes() {
                let bucket_ts = bucket_start_for(block_ts, tf);
                let Some(agg) = aggregate_candle_from_m10(&per_bucket, tf, bucket_ts) else {
                    continue;
                };
                token_usd_candle_overrides.insert((token, tf, bucket_ts), agg);
            }
        }

        let mut tokens_with_mcusd_m10: HashSet<SchemaAlkaneId> = HashSet::new();
        for (token, tf, _bucket_ts) in token_mcusd_candle_overrides.keys() {
            if *tf == Timeframe::M10 {
                tokens_with_mcusd_m10.insert(*token);
            }
        }
        for token in tokens_with_mcusd_m10 {
            let mut per_bucket = read_m10_candles_map(
                provider,
                table.token_mcusd_candle_ns_prefix(&token, Timeframe::M10),
            )?;
            for ((tok, tf, bucket_ts), candle) in token_mcusd_candle_overrides.iter() {
                if *tok == token && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket_ts, *candle);
                }
            }
            for tf in higher_timeframes() {
                let bucket_ts = bucket_start_for(block_ts, tf);
                let Some(agg) = aggregate_candle_from_m10(&per_bucket, tf, bucket_ts) else {
                    continue;
                };
                token_mcusd_candle_overrides.insert((token, tf, bucket_ts), agg);
            }
        }

        let mut pairs_with_derived_usd_m10: HashSet<(SchemaAlkaneId, SchemaAlkaneId)> =
            HashSet::new();
        for (token, quote, tf, _bucket_ts) in token_derived_usd_candle_overrides.keys() {
            if *tf == Timeframe::M10 {
                pairs_with_derived_usd_m10.insert((*token, *quote));
            }
        }
        for (token, quote) in pairs_with_derived_usd_m10 {
            let mut per_bucket = read_m10_candles_map(
                provider,
                table.token_derived_usd_candle_ns_prefix(&token, &quote, Timeframe::M10),
            )?;
            for ((tok, q, tf, bucket_ts), candle) in token_derived_usd_candle_overrides.iter() {
                if *tok == token && *q == quote && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket_ts, *candle);
                }
            }
            for tf in higher_timeframes() {
                let bucket_ts = bucket_start_for(block_ts, tf);
                let Some(agg) = aggregate_candle_from_m10(&per_bucket, tf, bucket_ts) else {
                    continue;
                };
                token_derived_usd_candle_overrides.insert((token, quote, tf, bucket_ts), agg);
            }
        }

        let mut pairs_with_derived_mcusd_m10: HashSet<(SchemaAlkaneId, SchemaAlkaneId)> =
            HashSet::new();
        for (token, quote, tf, _bucket_ts) in token_derived_mcusd_candle_overrides.keys() {
            if *tf == Timeframe::M10 {
                pairs_with_derived_mcusd_m10.insert((*token, *quote));
            }
        }
        for (token, quote) in pairs_with_derived_mcusd_m10 {
            let mut per_bucket = read_m10_candles_map(
                provider,
                table.token_derived_mcusd_candle_ns_prefix(&token, &quote, Timeframe::M10),
            )?;
            for ((tok, q, tf, bucket_ts), candle) in token_derived_mcusd_candle_overrides.iter() {
                if *tok == token && *q == quote && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket_ts, *candle);
                }
            }
            for tf in higher_timeframes() {
                let bucket_ts = bucket_start_for(block_ts, tf);
                let Some(agg) = aggregate_candle_from_m10(&per_bucket, tf, bucket_ts) else {
                    continue;
                };
                token_derived_mcusd_candle_overrides.insert((token, quote, tf, bucket_ts), agg);
            }
        }

        for ((token, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
            let key = table.token_usd_candle_key(token, *tf, *bucket_ts);
            let encoded = encode_candle_v1(candle)?;
            state.token_usd_candle_writes.push((key, encoded));
        }

        for ((token, tf, bucket_ts), candle) in token_mcusd_candle_overrides.iter() {
            let key = table.token_mcusd_candle_key(token, *tf, *bucket_ts);
            let encoded = encode_candle_v1(candle)?;
            state.token_mcusd_candle_writes.push((key, encoded));
        }

        for ((token, quote, tf, bucket_ts), candle) in token_derived_usd_candle_overrides.iter() {
            let key = table.token_derived_usd_candle_key(token, quote, *tf, *bucket_ts);
            let encoded = encode_candle_v1(candle)?;
            state.token_derived_usd_candle_writes.push((key, encoded));
        }

        for ((token, quote, tf, bucket_ts), candle) in token_derived_mcusd_candle_overrides.iter() {
            let key = table.token_derived_mcusd_candle_key(token, quote, *tf, *bucket_ts);
            let encoded = encode_candle_v1(candle)?;
            state.token_derived_mcusd_candle_writes.push((key, encoded));
        }

        let mut tokens_for_metrics: HashSet<SchemaAlkaneId> = HashSet::new();
        for token in state.canonical_trade_buckets.keys() {
            tokens_for_metrics.insert(*token);
        }
        for ((token, _tf, _bucket), _candle) in token_usd_candle_overrides.iter() {
            tokens_for_metrics.insert(*token);
        }
        for pool in state.pools_touched.iter() {
            if let Some(defs) = state.pools_map.get(pool) {
                tokens_for_metrics.insert(defs.base_alkane_id);
                tokens_for_metrics.insert(defs.quote_alkane_id);
            }
        }

        let mut pool_trade_window_cache: HashMap<
            SchemaAlkaneId,
            crate::modules::ammdata::PoolTradeWindows,
        > = HashMap::new();

        for token in tokens_for_metrics.iter() {
            let prefix = table.token_usd_candle_ns_prefix(token, Timeframe::M10);
            let mut per_bucket = read_m10_candles_map(provider, prefix.clone())?;

            for ((tok, tf, bucket), candle) in token_usd_candle_overrides.iter() {
                if tok == token && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket, *candle);
                }
            }

            let mc_prefix = table.token_mcusd_candle_ns_prefix(token, Timeframe::M10);
            let mut mc_per_bucket = read_m10_candles_map(provider, mc_prefix.clone())?;
            for ((tok, tf, bucket), candle) in token_mcusd_candle_overrides.iter() {
                if tok == token && *tf == Timeframe::M10 {
                    mc_per_bucket.insert(*bucket, *candle);
                }
            }

            let now_bucket = bucket_start_for(block_ts, Timeframe::M10);
            let earliest_bucket = per_bucket.keys().next().copied().unwrap_or(now_bucket);
            let latest_close = close_at_or_before(&per_bucket, now_bucket);
            let first_close = per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);

            let window_close = |secs: u64| -> u128 {
                let target = now_bucket.saturating_sub(secs);
                close_at_or_before(&per_bucket, target)
            };

            let supply = {
                let table_e = essentials.table();
                let key = table_e.circulating_supply_latest_key(token);
                essentials
                    .get_raw_value(EssentialsGetRawValueParams { blockhash: StateAt::Latest, key })?
                    .value
                    .and_then(|v| crate::modules::essentials::storage::decode_u128_value(&v).ok())
                    .unwrap_or(0)
            };

            let price_usd = latest_close;
            let fdv_usd = scale_price_by_supply(price_usd, supply);
            let marketcap_usd = fdv_usd;

            let metrics_key = table.token_metrics_key(token);
            let prev_raw = provider.get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: metrics_key.clone(),
            })?;
            let prev_metrics =
                prev_raw.value.as_ref().and_then(|raw| decode_token_metrics(raw).ok());

            let full_history = prev_metrics.is_none();
            let token_trade = match crate::modules::ammdata::token_trade_windows(
                provider,
                &state.pools_map,
                token,
                block_ts,
                &state.in_block_trade_volumes,
                &mut pool_trade_window_cache,
                full_history,
            ) {
                Ok(v) => v,
                Err(_) => crate::modules::ammdata::TokenTradeWindows::default(),
            };

            let volume_1d = token_trade.amount_1d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_7d = token_trade.amount_7d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_30d = token_trade.amount_30d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_all_time = if token_trade.has_all_time {
                token_trade.amount_all.saturating_mul(price_usd) / AMOUNT_SCALE
            } else {
                let prev = prev_metrics.as_ref().map(|m| m.volume_all_time).unwrap_or(0);
                let block_usd = token_trade.block_amount.saturating_mul(price_usd) / AMOUNT_SCALE;
                prev.saturating_add(block_usd)
            };

            let metrics = SchemaTokenMetricsV1 {
                price_usd,
                fdv_usd,
                marketcap_usd,
                volume_all_time,
                volume_1d,
                volume_7d,
                volume_30d,
                change_1d: crate::modules::ammdata::percent_change_str(
                    window_close(24 * 60 * 60),
                    latest_close,
                ),
                change_7d: crate::modules::ammdata::percent_change_str(
                    window_close(7 * 24 * 60 * 60),
                    latest_close,
                ),
                change_30d: crate::modules::ammdata::percent_change_str(
                    window_close(30 * 24 * 60 * 60),
                    latest_close,
                ),
                change_all_time: crate::modules::ammdata::percent_change_str(
                    first_close,
                    latest_close,
                ),
            };
            let chart_name = format!("{}:{}-usd", token.block, token.tx);
            let next_chart_set = build_chart_change_set(now_bucket, &per_bucket, &mc_per_bucket);
            let prev_chart_set = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.chart_change_latest_key(&chart_name),
                })?
                .value
                .and_then(|raw| decode_chart_change_set_v1(&raw).ok());
            let delta_set = diff_chart_change_set(prev_chart_set.as_ref(), &next_chart_set);
            if !delta_set.timeframe_changes.is_empty() {
                state.chart_change_writes.push((
                    table.chart_change_event_key(height as u64, &chart_name),
                    encode_chart_change_set_v1(&delta_set)?,
                ));
                state.chart_change_writes.push((
                    table.chart_change_latest_key(&chart_name),
                    encode_chart_change_set_v1(&next_chart_set)?,
                ));
            }
            if prev_raw.value.is_none() {
                state.token_metrics_index_new = state.token_metrics_index_new.saturating_add(1);
            }

            let build_index_keys =
                |m: &SchemaTokenMetricsV1| -> Vec<(TokenMetricsIndexField, Vec<u8>)> {
                    vec![
                        (
                            TokenMetricsIndexField::PriceUsd,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::PriceUsd,
                                m.price_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::MarketcapUsd,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::MarketcapUsd,
                                m.marketcap_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume1d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume1d,
                                m.volume_1d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume7d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume7d,
                                m.volume_7d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume30d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume30d,
                                m.volume_30d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::VolumeAllTime,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::VolumeAllTime,
                                m.volume_all_time,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change1d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change1d,
                                parse_change_basis_points(&m.change_1d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change7d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change7d,
                                parse_change_basis_points(&m.change_7d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change30d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change30d,
                                parse_change_basis_points(&m.change_30d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::ChangeAllTime,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::ChangeAllTime,
                                parse_change_basis_points(&m.change_all_time),
                                token,
                            ),
                        ),
                    ]
                };

            let new_keys = build_index_keys(&metrics);
            if let Some(prev) = prev_metrics.as_ref() {
                let prev_keys = build_index_keys(prev);
                for (idx, (_field, new_key)) in new_keys.iter().enumerate() {
                    if let Some((_pf, prev_key)) = prev_keys.get(idx) {
                        if prev_key != new_key {
                            state.token_metrics_index_deletes.push(prev_key.clone());
                            state.token_metrics_index_writes.push((new_key.clone(), Vec::new()));
                        }
                    }
                }
            } else {
                for (_field, new_key) in new_keys.into_iter() {
                    state.token_metrics_index_writes.push((new_key, Vec::new()));
                }
            }

            if search_index_enabled {
                let rec = essentials
                    .get_creation_record(GetCreationRecordParams {
                        blockhash: StateAt::Latest,
                        alkane: *token,
                    })
                    .ok()
                    .and_then(|resp| resp.record);
                if let Some(rec) = rec {
                    let prefixes = collect_search_prefixes(
                        &rec.names,
                        &rec.symbols,
                        search_prefix_min,
                        search_prefix_max,
                    );
                    if !prefixes.is_empty() {
                        let new_marketcap = metrics.marketcap_usd;
                        let new_volume_7d = metrics.volume_7d;
                        let new_change_7d = parse_change_basis_points(&metrics.change_7d);
                        let new_volume_all = metrics.volume_all_time;

                        let prev_marketcap = prev_metrics.as_ref().map(|m| m.marketcap_usd);
                        let prev_volume_7d = prev_metrics.as_ref().map(|m| m.volume_7d);
                        let prev_change_7d =
                            prev_metrics.as_ref().map(|m| parse_change_basis_points(&m.change_7d));
                        let prev_volume_all = prev_metrics.as_ref().map(|m| m.volume_all_time);

                        for prefix in prefixes {
                            state.token_search_index_writes.push((
                                table.token_search_index_key_u128(
                                    SearchIndexField::Marketcap,
                                    &prefix,
                                    new_marketcap,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.token_search_index_writes.push((
                                table.token_search_index_key_u128(
                                    SearchIndexField::Volume7d,
                                    &prefix,
                                    new_volume_7d,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.token_search_index_writes.push((
                                table.token_search_index_key_i64(
                                    SearchIndexField::Change7d,
                                    &prefix,
                                    new_change_7d,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.token_search_index_writes.push((
                                table.token_search_index_key_u128(
                                    SearchIndexField::VolumeAllTime,
                                    &prefix,
                                    new_volume_all,
                                    token,
                                ),
                                Vec::new(),
                            ));

                            if let Some(prev) = prev_marketcap {
                                if prev != new_marketcap {
                                    state.token_search_index_deletes.push(
                                        table.token_search_index_key_u128(
                                            SearchIndexField::Marketcap,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_volume_7d {
                                if prev != new_volume_7d {
                                    state.token_search_index_deletes.push(
                                        table.token_search_index_key_u128(
                                            SearchIndexField::Volume7d,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_change_7d {
                                if prev != new_change_7d {
                                    state.token_search_index_deletes.push(
                                        table.token_search_index_key_i64(
                                            SearchIndexField::Change7d,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_volume_all {
                                if prev != new_volume_all {
                                    state.token_search_index_deletes.push(
                                        table.token_search_index_key_u128(
                                            SearchIndexField::VolumeAllTime,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            state.token_metrics_cache.insert(*token, metrics.clone());
            let encoded = encode_token_metrics(&metrics)?;
            state.token_metrics_writes.push((metrics_key, encoded));
        }

        let mut derived_tokens_for_metrics: HashSet<(SchemaAlkaneId, SchemaAlkaneId)> =
            HashSet::new();
        for ((token, quote, _tf, _bucket), _candle) in token_derived_usd_candle_overrides.iter() {
            derived_tokens_for_metrics.insert((*token, *quote));
        }

        for (token, quote) in derived_tokens_for_metrics.iter() {
            let prefix = table.token_derived_usd_candle_ns_prefix(token, quote, Timeframe::M10);
            let mut per_bucket = read_m10_candles_map(provider, prefix.clone())?;

            for ((tok, q, tf, bucket), candle) in token_derived_usd_candle_overrides.iter() {
                if tok == token && q == quote && *tf == Timeframe::M10 {
                    per_bucket.insert(*bucket, *candle);
                }
            }
            if per_bucket.is_empty() {
                continue;
            }

            let mc_prefix =
                table.token_derived_mcusd_candle_ns_prefix(token, quote, Timeframe::M10);
            let mut mc_per_bucket = read_m10_candles_map(provider, mc_prefix.clone())?;
            for ((tok, q, tf, bucket), candle) in token_derived_mcusd_candle_overrides.iter() {
                if tok == token && q == quote && *tf == Timeframe::M10 {
                    mc_per_bucket.insert(*bucket, *candle);
                }
            }

            let now_bucket = bucket_start_for(block_ts, Timeframe::M10);
            let earliest_bucket = per_bucket.keys().next().copied().unwrap_or(now_bucket);
            let latest_close = close_at_or_before(&per_bucket, now_bucket);
            let first_close = per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);

            let window_close = |secs: u64| -> u128 {
                let target = now_bucket.saturating_sub(secs);
                close_at_or_before(&per_bucket, target)
            };

            let supply = if let Some(v) = supply_cache.get(token) {
                *v
            } else {
                let table_e = essentials.table();
                let key = table_e.circulating_supply_latest_key(token);
                let v = essentials
                    .get_raw_value(EssentialsGetRawValueParams { blockhash: StateAt::Latest, key })?
                    .value
                    .and_then(|raw| {
                        crate::modules::essentials::storage::decode_u128_value(&raw).ok()
                    })
                    .unwrap_or(0);
                supply_cache.insert(*token, v);
                v
            };

            let price_usd = latest_close;
            let fdv_usd = scale_price_by_supply(price_usd, supply);
            let marketcap_usd = fdv_usd;

            let metrics_key = table.token_derived_metrics_key(token, quote);
            let prev_raw = provider.get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: metrics_key.clone(),
            })?;
            let prev_metrics =
                prev_raw.value.as_ref().and_then(|raw| decode_token_metrics(raw).ok());

            let full_history = prev_metrics.is_none();
            let token_trade = match crate::modules::ammdata::token_trade_windows(
                provider,
                &state.pools_map,
                token,
                block_ts,
                &state.in_block_trade_volumes,
                &mut pool_trade_window_cache,
                full_history,
            ) {
                Ok(v) => v,
                Err(_) => crate::modules::ammdata::TokenTradeWindows::default(),
            };

            let volume_1d = token_trade.amount_1d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_7d = token_trade.amount_7d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_30d = token_trade.amount_30d.saturating_mul(price_usd) / AMOUNT_SCALE;
            let volume_all_time = if token_trade.has_all_time {
                token_trade.amount_all.saturating_mul(price_usd) / AMOUNT_SCALE
            } else {
                let prev = prev_metrics.as_ref().map(|m| m.volume_all_time).unwrap_or(0);
                let block_usd = token_trade.block_amount.saturating_mul(price_usd) / AMOUNT_SCALE;
                prev.saturating_add(block_usd)
            };

            let metrics = SchemaTokenMetricsV1 {
                price_usd,
                fdv_usd,
                marketcap_usd,
                volume_all_time,
                volume_1d,
                volume_7d,
                volume_30d,
                change_1d: crate::modules::ammdata::percent_change_str(
                    window_close(24 * 60 * 60),
                    latest_close,
                ),
                change_7d: crate::modules::ammdata::percent_change_str(
                    window_close(7 * 24 * 60 * 60),
                    latest_close,
                ),
                change_30d: crate::modules::ammdata::percent_change_str(
                    window_close(30 * 24 * 60 * 60),
                    latest_close,
                ),
                change_all_time: crate::modules::ammdata::percent_change_str(
                    first_close,
                    latest_close,
                ),
            };
            let chart_name =
                format!("{}:{}-derived_{}:{}-usd", token.block, token.tx, quote.block, quote.tx);
            let next_chart_set = build_chart_change_set(now_bucket, &per_bucket, &mc_per_bucket);
            let prev_chart_set = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.chart_change_latest_key(&chart_name),
                })?
                .value
                .and_then(|raw| decode_chart_change_set_v1(&raw).ok());
            let delta_set = diff_chart_change_set(prev_chart_set.as_ref(), &next_chart_set);
            if !delta_set.timeframe_changes.is_empty() {
                state.chart_change_writes.push((
                    table.chart_change_event_key(height as u64, &chart_name),
                    encode_chart_change_set_v1(&delta_set)?,
                ));
                state.chart_change_writes.push((
                    table.chart_change_latest_key(&chart_name),
                    encode_chart_change_set_v1(&next_chart_set)?,
                ));
            }
            if prev_raw.value.is_none() {
                let entry = state.derived_metrics_index_new.entry(*quote).or_insert(0);
                *entry = entry.saturating_add(1);
            }

            let build_index_keys =
                |m: &SchemaTokenMetricsV1| -> Vec<(TokenMetricsIndexField, Vec<u8>)> {
                    vec![
                        (
                            TokenMetricsIndexField::PriceUsd,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::PriceUsd,
                                m.price_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::MarketcapUsd,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::MarketcapUsd,
                                m.marketcap_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume1d,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::Volume1d,
                                m.volume_1d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume7d,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::Volume7d,
                                m.volume_7d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume30d,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::Volume30d,
                                m.volume_30d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::VolumeAllTime,
                            table.token_derived_metrics_index_key_u128(
                                quote,
                                TokenMetricsIndexField::VolumeAllTime,
                                m.volume_all_time,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change1d,
                            table.token_derived_metrics_index_key_i64(
                                quote,
                                TokenMetricsIndexField::Change1d,
                                parse_change_basis_points(&m.change_1d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change7d,
                            table.token_derived_metrics_index_key_i64(
                                quote,
                                TokenMetricsIndexField::Change7d,
                                parse_change_basis_points(&m.change_7d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change30d,
                            table.token_derived_metrics_index_key_i64(
                                quote,
                                TokenMetricsIndexField::Change30d,
                                parse_change_basis_points(&m.change_30d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::ChangeAllTime,
                            table.token_derived_metrics_index_key_i64(
                                quote,
                                TokenMetricsIndexField::ChangeAllTime,
                                parse_change_basis_points(&m.change_all_time),
                                token,
                            ),
                        ),
                    ]
                };

            let new_keys = build_index_keys(&metrics);
            if let Some(prev) = prev_metrics.as_ref() {
                let prev_keys = build_index_keys(prev);
                for (idx, (_field, new_key)) in new_keys.iter().enumerate() {
                    if let Some((_pf, prev_key)) = prev_keys.get(idx) {
                        if prev_key != new_key {
                            state.derived_metrics_index_deletes.push(prev_key.clone());
                            state.derived_metrics_index_writes.push((new_key.clone(), Vec::new()));
                        }
                    }
                }
            } else {
                for (_field, new_key) in new_keys.into_iter() {
                    state.derived_metrics_index_writes.push((new_key, Vec::new()));
                }
            }

            if search_index_enabled {
                let rec = essentials
                    .get_creation_record(GetCreationRecordParams {
                        blockhash: StateAt::Latest,
                        alkane: *token,
                    })
                    .ok()
                    .and_then(|resp| resp.record);
                if let Some(rec) = rec {
                    let prefixes = collect_search_prefixes(
                        &rec.names,
                        &rec.symbols,
                        search_prefix_min,
                        search_prefix_max,
                    );
                    if !prefixes.is_empty() {
                        let new_marketcap = metrics.marketcap_usd;
                        let new_volume_7d = metrics.volume_7d;
                        let new_change_7d = parse_change_basis_points(&metrics.change_7d);
                        let new_volume_all = metrics.volume_all_time;

                        let prev_marketcap = prev_metrics.as_ref().map(|m| m.marketcap_usd);
                        let prev_volume_7d = prev_metrics.as_ref().map(|m| m.volume_7d);
                        let prev_change_7d =
                            prev_metrics.as_ref().map(|m| parse_change_basis_points(&m.change_7d));
                        let prev_volume_all = prev_metrics.as_ref().map(|m| m.volume_all_time);

                        for prefix in prefixes {
                            state.derived_search_index_writes.push((
                                table.token_derived_search_index_key_u128(
                                    quote,
                                    SearchIndexField::Marketcap,
                                    &prefix,
                                    new_marketcap,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.derived_search_index_writes.push((
                                table.token_derived_search_index_key_u128(
                                    quote,
                                    SearchIndexField::Volume7d,
                                    &prefix,
                                    new_volume_7d,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.derived_search_index_writes.push((
                                table.token_derived_search_index_key_i64(
                                    quote,
                                    SearchIndexField::Change7d,
                                    &prefix,
                                    new_change_7d,
                                    token,
                                ),
                                Vec::new(),
                            ));
                            state.derived_search_index_writes.push((
                                table.token_derived_search_index_key_u128(
                                    quote,
                                    SearchIndexField::VolumeAllTime,
                                    &prefix,
                                    new_volume_all,
                                    token,
                                ),
                                Vec::new(),
                            ));

                            if let Some(prev) = prev_marketcap {
                                if prev != new_marketcap {
                                    state.derived_search_index_deletes.push(
                                        table.token_derived_search_index_key_u128(
                                            quote,
                                            SearchIndexField::Marketcap,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_volume_7d {
                                if prev != new_volume_7d {
                                    state.derived_search_index_deletes.push(
                                        table.token_derived_search_index_key_u128(
                                            quote,
                                            SearchIndexField::Volume7d,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_change_7d {
                                if prev != new_change_7d {
                                    state.derived_search_index_deletes.push(
                                        table.token_derived_search_index_key_i64(
                                            quote,
                                            SearchIndexField::Change7d,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                            if let Some(prev) = prev_volume_all {
                                if prev != new_volume_all {
                                    state.derived_search_index_deletes.push(
                                        table.token_derived_search_index_key_u128(
                                            quote,
                                            SearchIndexField::VolumeAllTime,
                                            &prefix,
                                            prev,
                                            token,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            let encoded = encode_token_metrics(&metrics)?;
            state.derived_metrics_writes.push((metrics_key, encoded));
        }
    }

    Ok(())
}
