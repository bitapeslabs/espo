use crate::modules::ammdata::consts::{AMOUNT_SCALE, CanonicalQuoteUnit};
use crate::modules::ammdata::schemas::{
    SchemaMarketDefs, SchemaPoolMetricsV1, SchemaPoolMetricsV2, Timeframe,
};
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetListEntriesDescParams, GetPoolCreationInfoParams, GetPoolMetricsV2Params,
    GetTokenMetricsParams, GetTvlVersionedAtOrBeforeParams, PoolMetricsIndexField,
    decode_full_candle_v1, encode_f64_value, encode_pool_metrics, encode_pool_metrics_v2,
    encode_u128_value, parse_change_basis_points,
};
use crate::modules::ammdata::utils::candles::bucket_start_for;
use crate::modules::ammdata::utils::index_state::IndexState;
use crate::modules::essentials::storage::{EssentialsProvider, GetLatestCirculatingSupplyParams};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use serde_json::json;
use std::collections::HashMap;

#[derive(Clone, Copy)]
enum PoolVolumeSide {
    Base,
    Quote,
}

fn pool_volume_side(
    defs: &SchemaMarketDefs,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
) -> PoolVolumeSide {
    if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
        PoolVolumeSide::Quote
    } else {
        PoolVolumeSide::Base
    }
}

fn amount_value(
    amount: u128,
    token: SchemaAlkaneId,
    price_usd: u128,
    price_sats: u128,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    btc_usd_price: Option<u128>,
) -> (u128, u128) {
    if let Some(unit) = canonical_quote_units.get(&token).copied() {
        let usd =
            crate::modules::ammdata::canonical_quote_amount_tvl_usd(amount, unit, btc_usd_price)
                .unwrap_or(0);
        let sats =
            crate::modules::ammdata::canonical_quote_amount_tvl_sats(amount, unit, btc_usd_price)
                .unwrap_or(0);
        return (usd, sats);
    }

    (
        amount.saturating_mul(price_usd).saturating_div(AMOUNT_SCALE),
        amount.saturating_mul(price_sats).saturating_div(AMOUNT_SCALE),
    )
}

pub fn derive_pool_metrics(
    blockhash: StateAt,
    block_ts: u64,
    height: u32,
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
    network: Network,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    state: &mut IndexState,
) -> Result<()> {
    if state.pools_touched.is_empty() {
        return Ok(());
    }

    let table = provider.table();

    let load_pool_candle =
        |pool: &SchemaAlkaneId,
         tf: Timeframe,
         bucket_ts: u64|
         -> Result<Option<crate::modules::ammdata::schemas::SchemaFullCandleV1>> {
            if let Some(c) = state.pool_candle_overrides.get(&(*pool, tf, bucket_ts)) {
                return Ok(Some(*c));
            }
            let key = table.candle_key(pool, tf, bucket_ts);
            if let Some(raw) = provider
                .get_raw_value(crate::modules::ammdata::storage::GetRawValueParams {
                    blockhash: blockhash.clone(),
                    key,
                })?
                .value
            {
                return Ok(Some(decode_full_candle_v1(&raw)?));
            }
            Ok(None)
        };

    let mut canonical_pools_by_token: HashMap<
        SchemaAlkaneId,
        Vec<crate::modules::ammdata::schemas::SchemaCanonicalPoolEntry>,
    > = HashMap::new();
    for (pool, defs) in state.pools_map.iter() {
        if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
            canonical_pools_by_token.entry(defs.base_alkane_id).or_default().push(
                crate::modules::ammdata::schemas::SchemaCanonicalPoolEntry {
                    pool_id: *pool,
                    quote_id: defs.quote_alkane_id,
                },
            );
        }
        if canonical_quote_units.contains_key(&defs.base_alkane_id) {
            canonical_pools_by_token.entry(defs.quote_alkane_id).or_default().push(
                crate::modules::ammdata::schemas::SchemaCanonicalPoolEntry {
                    pool_id: *pool,
                    quote_id: defs.base_alkane_id,
                },
            );
        }
    }

    let mut token_price_usd_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    let mut token_price_sats_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();

    let mut get_token_price_usd = |token: &SchemaAlkaneId| -> u128 {
        if let Some(price) = token_price_usd_cache.get(token) {
            return *price;
        }
        let metrics = state
            .token_metrics_cache
            .get(token)
            .cloned()
            .or_else(|| {
                provider
                    .get_token_metrics(GetTokenMetricsParams {
                        blockhash: blockhash.clone(),
                        token: *token,
                    })
                    .ok()
                    .map(|res| res.metrics)
            })
            .unwrap_or_default();
        let price = metrics.price_usd;
        token_price_usd_cache.insert(*token, price);
        price
    };

    let mut get_token_price_sats = |token: &SchemaAlkaneId| -> u128 {
        if let Some(price) = token_price_sats_cache.get(token) {
            return *price;
        }
        let mut price = 0u128;
        if let Some(pools) = canonical_pools_by_token.get(token) {
            for entry in pools {
                let unit = match canonical_quote_units.get(&entry.quote_id) {
                    Some(u) => *u,
                    None => continue,
                };
                if unit != CanonicalQuoteUnit::Btc {
                    continue;
                }
                let Some(defs) = state.pools_map.get(&entry.pool_id) else { continue };
                let token_is_base = defs.base_alkane_id == *token;
                let token_is_quote = defs.quote_alkane_id == *token;
                if !token_is_base && !token_is_quote {
                    continue;
                }
                let use_base_price = token_is_base && defs.quote_alkane_id == entry.quote_id;
                let use_quote_price = token_is_quote && defs.base_alkane_id == entry.quote_id;
                if !use_base_price && !use_quote_price {
                    continue;
                }
                let bucket = bucket_start_for(block_ts, Timeframe::M10);
                if let Ok(Some(candle)) = load_pool_candle(&entry.pool_id, Timeframe::M10, bucket) {
                    price = if use_base_price {
                        candle.base_candle.close
                    } else {
                        candle.quote_candle.close
                    };
                } else {
                    let prefix = table.candle_ns_prefix(&entry.pool_id, Timeframe::M10);
                    if let Ok(res) = provider.get_list_entries_desc(GetListEntriesDescParams {
                        blockhash: StateAt::Latest,
                        prefix,
                    }) {
                        if let Some((_k, v)) = res.entries.into_iter().next() {
                            if let Ok(candle) = decode_full_candle_v1(&v) {
                                price = if use_base_price {
                                    candle.base_candle.close
                                } else {
                                    candle.quote_candle.close
                                };
                            }
                        }
                    }
                }
                break;
            }
        }
        token_price_sats_cache.insert(*token, price);
        price
    };

    let tvl_at_height = |pool: &SchemaAlkaneId, h: u32| -> u128 {
        provider
            .get_tvl_versioned_at_or_before(GetTvlVersionedAtOrBeforeParams {
                blockhash: StateAt::Latest,
                pool: *pool,
                height: h,
            })
            .ok()
            .and_then(|res| res.value)
            .unwrap_or(0)
    };

    let mut pool_trade_window_cache: HashMap<
        SchemaAlkaneId,
        crate::modules::ammdata::PoolTradeWindows,
    > = HashMap::new();

    for pool in state.pools_touched.iter() {
        let Some(defs) = state.pools_map.get(pool) else { continue };

        let Some(snapshot) = state.reserves_snapshot.get(pool) else { continue };
        let token0_amount = snapshot.base_reserve;
        let token1_amount = snapshot.quote_reserve;

        let token0_price_usd = get_token_price_usd(&defs.base_alkane_id);
        let token1_price_usd = get_token_price_usd(&defs.quote_alkane_id);
        let token0_price_sats = get_token_price_sats(&defs.base_alkane_id);
        let token1_price_sats = get_token_price_sats(&defs.quote_alkane_id);

        let mut token0_tvl_usd = token0_amount.saturating_mul(token0_price_usd) / AMOUNT_SCALE;
        let mut token1_tvl_usd = token1_amount.saturating_mul(token1_price_usd) / AMOUNT_SCALE;
        let mut token0_tvl_sats = token0_amount.saturating_mul(token0_price_sats) / AMOUNT_SCALE;
        let mut token1_tvl_sats = token1_amount.saturating_mul(token1_price_sats) / AMOUNT_SCALE;

        if let Some(unit) = canonical_quote_units.get(&defs.base_alkane_id) {
            if let Some(value) = crate::modules::ammdata::canonical_quote_amount_tvl_usd(
                token0_amount,
                *unit,
                state.btc_usd_price,
            ) {
                token0_tvl_usd = value;
            }
            if let Some(value) = crate::modules::ammdata::canonical_quote_amount_tvl_sats(
                token0_amount,
                *unit,
                state.btc_usd_price,
            ) {
                token0_tvl_sats = value;
            }
        }
        if let Some(unit) = canonical_quote_units.get(&defs.quote_alkane_id) {
            if let Some(value) = crate::modules::ammdata::canonical_quote_amount_tvl_usd(
                token1_amount,
                *unit,
                state.btc_usd_price,
            ) {
                token1_tvl_usd = value;
            }
            if let Some(value) = crate::modules::ammdata::canonical_quote_amount_tvl_sats(
                token1_amount,
                *unit,
                state.btc_usd_price,
            ) {
                token1_tvl_sats = value;
            }
        }

        let pool_tvl_usd = token0_tvl_usd.saturating_add(token1_tvl_usd);
        let pool_tvl_sats = token0_tvl_sats.saturating_add(token1_tvl_sats);

        // Contribution to the aggregated TVL lines: the priceable side, doubled.
        // Kept separate from pool_tvl_* above, which prices both sides independently.
        let anchor_point = crate::modules::ammdata::utils::index_tvl::pool_anchor_point(
            defs,
            canonical_quote_units,
            &crate::modules::ammdata::utils::index_tvl::PoolAnchorInput {
                base_tvl_sats: token0_tvl_sats,
                quote_tvl_sats: token1_tvl_sats,
                base_price_sats: token0_price_sats,
                quote_price_sats: token1_price_sats,
                base_tvl_usd: token0_tvl_usd,
                quote_tvl_usd: token1_tvl_usd,
            },
            state.btc_usd_price,
        );
        state.pool_tvl_anchor_current.insert(*pool, anchor_point);

        let prev_pool_metrics = provider
            .get_pool_metrics_v2(GetPoolMetricsV2Params {
                blockhash: blockhash.clone(),
                pool: *pool,
            })
            .ok()
            .and_then(|res| res.metrics);
        let full_history = prev_pool_metrics.is_none();
        let trade_windows = match crate::modules::ammdata::pool_trade_windows(
            provider,
            pool,
            block_ts,
            &state.in_block_trade_volumes,
            &mut pool_trade_window_cache,
            full_history,
        ) {
            Ok(v) => v,
            Err(_) => crate::modules::ammdata::PoolTradeWindows::default(),
        };

        let token0_volume_1d = trade_windows.token0_1d;
        let token1_volume_1d = trade_windows.token1_1d;
        let token0_volume_7d = trade_windows.token0_7d;
        let token1_volume_7d = trade_windows.token1_7d;
        let token0_volume_30d = trade_windows.token0_30d;
        let token1_volume_30d = trade_windows.token1_30d;
        let volume_side = pool_volume_side(defs, canonical_quote_units);
        let btc_usd_price = state.btc_usd_price;
        let selected_volume_value = |base_amount: u128, quote_amount: u128| -> (u128, u128) {
            match volume_side {
                PoolVolumeSide::Base => amount_value(
                    base_amount,
                    defs.base_alkane_id,
                    token0_price_usd,
                    token0_price_sats,
                    canonical_quote_units,
                    btc_usd_price,
                ),
                PoolVolumeSide::Quote => amount_value(
                    quote_amount,
                    defs.quote_alkane_id,
                    token1_price_usd,
                    token1_price_sats,
                    canonical_quote_units,
                    btc_usd_price,
                ),
            }
        };

        let (pool_volume_1d_usd, pool_volume_1d_sats) =
            selected_volume_value(token0_volume_1d, token1_volume_1d);
        let (pool_volume_7d_usd, pool_volume_7d_sats) =
            selected_volume_value(token0_volume_7d, token1_volume_7d);
        let (pool_volume_30d_usd, pool_volume_30d_sats) =
            selected_volume_value(token0_volume_30d, token1_volume_30d);
        let (block_base, block_quote) =
            state.in_block_trade_volumes.get(pool).copied().unwrap_or((0, 0));
        let (block_volume_usd, block_volume_sats) = selected_volume_value(block_base, block_quote);

        let pool_volume_all_time_usd = if trade_windows.has_all_time {
            selected_volume_value(trade_windows.token0_all, trade_windows.token1_all).0
        } else {
            prev_pool_metrics
                .as_ref()
                .map(|m| m.pool_volume_all_time_usd)
                .unwrap_or(0)
                .saturating_add(block_volume_usd)
        };
        let pool_volume_all_time_sats = if trade_windows.has_all_time {
            selected_volume_value(trade_windows.token0_all, trade_windows.token1_all).1
        } else {
            prev_pool_metrics
                .as_ref()
                .map(|m| m.pool_volume_all_time_sats)
                .unwrap_or(0)
                .saturating_add(block_volume_sats)
        };

        let prev_1d = tvl_at_height(pool, height.saturating_sub(144));
        let prev_7d = tvl_at_height(pool, height.saturating_sub(1008));
        let tvl_change_24h = crate::modules::ammdata::percent_change_str(prev_1d, pool_tvl_usd);
        let tvl_change_7d = crate::modules::ammdata::percent_change_str(prev_7d, pool_tvl_usd);

        let pool_apr = crate::modules::ammdata::apr_30d_str(pool_volume_30d_usd, pool_tvl_usd);

        let metrics = SchemaPoolMetricsV1 {
            token0_volume_1d,
            token1_volume_1d,
            token0_volume_30d,
            token1_volume_30d,
            pool_volume_1d_usd,
            pool_volume_30d_usd,
            pool_volume_1d_sats,
            pool_volume_30d_sats,
            pool_tvl_usd,
            pool_tvl_sats,
            tvl_change_24h: tvl_change_24h.clone(),
            tvl_change_7d: tvl_change_7d.clone(),
            pool_apr: pool_apr.clone(),
        };
        let metrics_v2 = SchemaPoolMetricsV2 {
            token0_volume_1d,
            token1_volume_1d,
            token0_volume_30d,
            token1_volume_30d,
            pool_volume_1d_usd,
            pool_volume_30d_usd,
            pool_volume_1d_sats,
            pool_volume_30d_sats,
            pool_volume_7d_usd,
            pool_volume_all_time_usd,
            pool_volume_7d_sats,
            pool_volume_all_time_sats,
            pool_tvl_usd,
            pool_tvl_sats,
            tvl_change_24h,
            tvl_change_7d,
            pool_apr,
        };

        let build_pool_index_keys =
            |m: &SchemaPoolMetricsV2| -> Vec<(PoolMetricsIndexField, Vec<u8>)> {
                vec![
                    (
                        PoolMetricsIndexField::TvlUsd,
                        table.pool_metrics_index_key_u128(
                            PoolMetricsIndexField::TvlUsd,
                            m.pool_tvl_usd,
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::Volume1dUsd,
                        table.pool_metrics_index_key_u128(
                            PoolMetricsIndexField::Volume1dUsd,
                            m.pool_volume_1d_usd,
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::Volume7dUsd,
                        table.pool_metrics_index_key_u128(
                            PoolMetricsIndexField::Volume7dUsd,
                            m.pool_volume_7d_usd,
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::Volume30dUsd,
                        table.pool_metrics_index_key_u128(
                            PoolMetricsIndexField::Volume30dUsd,
                            m.pool_volume_30d_usd,
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::VolumeAllTimeUsd,
                        table.pool_metrics_index_key_u128(
                            PoolMetricsIndexField::VolumeAllTimeUsd,
                            m.pool_volume_all_time_usd,
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::Apr,
                        table.pool_metrics_index_key_i64(
                            PoolMetricsIndexField::Apr,
                            parse_change_basis_points(&m.pool_apr),
                            pool,
                        ),
                    ),
                    (
                        PoolMetricsIndexField::TvlChange24h,
                        table.pool_metrics_index_key_i64(
                            PoolMetricsIndexField::TvlChange24h,
                            parse_change_basis_points(&m.tvl_change_24h),
                            pool,
                        ),
                    ),
                ]
            };

        if prev_pool_metrics.is_none() {
            state.pool_metrics_index_new = state.pool_metrics_index_new.saturating_add(1);
        }

        let new_pool_index_keys = build_pool_index_keys(&metrics_v2);
        if let Some(prev) = prev_pool_metrics.as_ref() {
            let prev_keys = build_pool_index_keys(prev);
            for (idx, (_field, new_key)) in new_pool_index_keys.iter().enumerate() {
                if let Some((_pf, prev_key)) = prev_keys.get(idx) {
                    if prev_key != new_key {
                        state.pool_metrics_index_deletes.push(prev_key.clone());
                        state.pool_metrics_index_writes.push((new_key.clone(), Vec::new()));
                    }
                }
            }
        } else {
            for (_field, new_key) in new_pool_index_keys.into_iter() {
                state.pool_metrics_index_writes.push((new_key, Vec::new()));
            }
        }

        state
            .pool_metrics_writes
            .push((table.pool_metrics_key(pool), encode_pool_metrics(&metrics)?));
        state
            .pool_metrics_writes
            .push((table.pool_metrics_v2_key(pool), encode_pool_metrics_v2(&metrics_v2)?));

        let lp_supply = essentials
            .get_latest_circulating_supply(GetLatestCirculatingSupplyParams {
                blockhash: blockhash.clone(),
                alkane: *pool,
            })
            .map(|res| res.supply)
            .unwrap_or(0);
        state
            .pool_lp_supply_writes
            .push((table.pool_lp_supply_latest_key(pool), encode_u128_value(lp_supply)?));

        let pool_label =
            crate::modules::ammdata::pool_name_display(&crate::modules::ammdata::strip_lp_suffix(
                &crate::modules::ammdata::utils::index_pools::get_alkane_label(
                    blockhash.clone(),
                    essentials,
                    &mut state.alkane_label_cache,
                    pool,
                ),
            ));

        let creation_info = state.pool_creation_info_cache.get(pool).cloned().or_else(|| {
            provider
                .get_pool_creation_info(GetPoolCreationInfoParams {
                    blockhash: blockhash.clone(),
                    pool: *pool,
                })
                .ok()
                .and_then(|res| res.info)
        });
        let (creator_address, creation_height, initial_token0_amount, initial_token1_amount) =
            if let Some(info) = creation_info {
                let creator = if info.creator_spk.is_empty() {
                    None
                } else {
                    let spk = bitcoin::ScriptBuf::from(info.creator_spk.clone());
                    crate::modules::essentials::storage::spk_to_address_str(&spk, network)
                };
                (
                    creator,
                    Some(info.creation_height),
                    info.initial_token0_amount,
                    info.initial_token1_amount,
                )
            } else {
                (None, None, 0, 0)
            };

        let lp_value_sats =
            if lp_supply == 0 { 0 } else { pool_tvl_sats.saturating_div(lp_supply) };
        let lp_value_usd = if lp_supply == 0 { 0 } else { pool_tvl_usd.saturating_div(lp_supply) };

        let pool_apr = crate::modules::ammdata::parse_change_f64(&metrics.pool_apr);
        let tvl_change_24h = crate::modules::ammdata::parse_change_f64(&metrics.tvl_change_24h);
        let tvl_change_7d = crate::modules::ammdata::parse_change_f64(&metrics.tvl_change_7d);

        let _value = json!({
            "token0": crate::modules::ammdata::alkane_id_json(&defs.base_alkane_id),
            "token1": crate::modules::ammdata::alkane_id_json(&defs.quote_alkane_id),
            "token0Amount": token0_amount.to_string(),
            "token1Amount": token1_amount.to_string(),
            "tokenSupply": lp_supply.to_string(),
            "poolName": pool_label,
            "poolId": crate::modules::ammdata::alkane_id_json(pool),
            "token0TvlInSats": token0_tvl_sats.to_string(),
            "token0TvlInUsd": crate::modules::ammdata::scale_price_u128(token0_tvl_usd),
            "token1TvlInSats": token1_tvl_sats.to_string(),
            "token1TvlInUsd": crate::modules::ammdata::scale_price_u128(token1_tvl_usd),
            "poolVolume30dInSats": metrics.pool_volume_30d_sats.to_string(),
            "poolVolume1dInSats": metrics.pool_volume_1d_sats.to_string(),
            "poolVolume30dInUsd": crate::modules::ammdata::scale_price_u128(metrics.pool_volume_30d_usd),
            "poolVolume1dInUsd": crate::modules::ammdata::scale_price_u128(metrics.pool_volume_1d_usd),
            "token0Volume30d": metrics.token0_volume_30d.to_string(),
            "token1Volume30d": metrics.token1_volume_30d.to_string(),
            "token0Volume1d": metrics.token0_volume_1d.to_string(),
            "token1Volume1d": metrics.token1_volume_1d.to_string(),
            "lPTokenValueInSats": lp_value_sats.to_string(),
            "lPTokenValueInUsd": crate::modules::ammdata::scale_price_u128(lp_value_usd),
            "poolTvlInSats": pool_tvl_sats.to_string(),
            "poolTvlInUsd": crate::modules::ammdata::scale_price_u128(pool_tvl_usd),
            "tvlChange24h": tvl_change_24h,
            "tvlChange7d": tvl_change_7d,
            "totalSupply": lp_supply.to_string(),
            "poolApr": pool_apr,
            "initialToken0Amount": initial_token0_amount.to_string(),
            "initialToken1Amount": initial_token1_amount.to_string(),
            "creatorAddress": creator_address,
            "creationBlockHeight": creation_height,
            "tvl": crate::modules::ammdata::scale_price_u128(pool_tvl_usd),
            "volume1d": crate::modules::ammdata::scale_price_u128(metrics.pool_volume_1d_usd),
            "volume7d": crate::modules::ammdata::scale_price_u128(pool_volume_7d_usd),
            "volume30d": crate::modules::ammdata::scale_price_u128(metrics.pool_volume_30d_usd),
            "volumeAllTime": crate::modules::ammdata::scale_price_u128(pool_volume_all_time_usd),
            "apr": pool_apr,
            "tvlChange": tvl_change_24h,
        });

        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "token0_tvl_usd"),
            encode_u128_value(token0_tvl_usd)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "token1_tvl_usd"),
            encode_u128_value(token1_tvl_usd)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "token0_tvl_sats"),
            encode_u128_value(token0_tvl_sats)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "token1_tvl_sats"),
            encode_u128_value(token1_tvl_sats)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "pool_tvl_usd"),
            encode_u128_value(pool_tvl_usd)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "pool_volume_1d_usd"),
            encode_u128_value(metrics.pool_volume_1d_usd)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "pool_volume_30d_usd"),
            encode_u128_value(metrics.pool_volume_30d_usd)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "pool_apr"),
            encode_f64_value(pool_apr)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "tvl_change_24h"),
            encode_f64_value(tvl_change_24h)?,
        ));
        state.pool_details_snapshot_writes.push((
            table.pool_details_snapshot_field_key(pool, "lp_supply"),
            encode_u128_value(lp_supply)?,
        ));

        state
            .tvl_versioned_writes
            .push((table.tvl_versioned_key(pool, height), encode_u128_value(pool_tvl_usd)?));
    }

    Ok(())
}
