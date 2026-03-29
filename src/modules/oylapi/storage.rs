use crate::config::{debug_enabled, get_config, get_electrum_like, get_network};
use crate::debug;
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::consts::{
    AMOUNT_SCALE, CanonicalQuoteUnit, PRICE_SCALE, canonical_quotes,
};
use crate::modules::ammdata::schemas::{
    ActivityKind, SchemaActivityV1, SchemaMarketDefs, SchemaPoolCreationInfoV1, SchemaPoolSnapshot,
    SchemaTokenMetricsV1, Timeframe,
};
use crate::modules::ammdata::storage::{
    AmmDataProvider, AmmHistoryEntry, GetActivityEntryParams, GetAddressPoolBurnsPageParams,
    GetAddressPoolCreationsPageParams, GetAddressPoolMintsPageParams,
    GetAddressPoolSwapsPageParams, GetAddressTokenSwapsPageParams, GetCanonicalPoolPricesParams,
    GetFactoryPoolsParams, GetLatestBtcUsdPriceParams, GetLatestTokenUsdCloseParams,
    GetListEntriesDescParams, GetPoolActivityEntriesParams, GetPoolCreationInfoParams,
    GetPoolCreationsPageParams, GetPoolDefsParams, GetPoolFactoryParams,
    GetPoolIdsByNamePrefixParams, GetPoolLpSupplyLatestParams, GetPoolMetricsParams,
    GetPoolMetricsV2Params, GetTokenDerivedMetricsByIdParams,
    GetTokenDerivedMetricsIndexCountParams, GetTokenDerivedMetricsIndexPageParams,
    GetTokenDerivedMetricsParams, GetTokenDerivedSearchIndexPageParams, GetTokenMetricsByIdParams,
    GetTokenMetricsIndexCountParams, GetTokenMetricsIndexPageParams, GetTokenMetricsParams,
    GetTokenPoolsParams, GetTokenSearchIndexPageParams, GetTokenSwapsPageParams, SearchIndexField,
    TokenMetricsIndexField, decode_full_candle_v1,
};
use crate::modules::ammdata::utils::candles::bucket_start_for;
use crate::modules::ammdata::utils::pathfinder::plan_exact_in_default_fee;
use crate::modules::ammdata::utils::search::normalize_search_text;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetAlkaneIdsByNamePrefixParams, GetAlkaneIdsBySymbolPrefixParams,
    GetCreationCountParams, GetCreationRecordParams, GetCreationRecordsByIdParams,
    GetCreationRecordsOrderedPageParams, GetCreationRecordsOrderedParams, GetHoldersCountParams,
    GetHoldersCountsByIdParams, GetHoldersOrderedPageParams, GetLatestCirculatingSupplyParams,
    GetLatestTotalMintedParams, spk_to_address_str,
};
use crate::modules::essentials::utils::balances::{
    get_alkane_balances, get_balance_for_address, get_outpoint_balances_with_spent_batch,
};
use crate::modules::oylapi::config::OylApiConfig;
use crate::modules::oylapi::ordinals::{OrdOutput, fetch_ord_outputs};
use crate::modules::subfrost::schemas::SchemaWrapEventV1;
use crate::modules::subfrost::storage::{
    GetUnwrapEventsAllParams, GetUnwrapEventsByAddressParams, GetUnwrapTotalAtOrBeforeParams,
    GetUnwrapTotalLatestParams, GetWrapEventsAllParams, GetWrapEventsByAddressParams,
    SubfrostProvider,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::hashes::sha256;
use bitcoin::script::ScriptBuf;
use bitcoin::{Address, Txid, hashes::Hash as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Deserialize)]
struct EsploraUtxoStatus {
    confirmed: bool,
    block_height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EsploraUtxo {
    txid: String,
    vout: u32,
    value: u64,
    status: EsploraUtxoStatus,
}

#[derive(Debug, Deserialize)]
struct EsploraAddressStats {
    chain_stats: EsploraTxStats,
    mempool_stats: EsploraTxStats,
}

#[derive(Debug, Deserialize)]
struct EsploraTxStats {
    funded_txo_sum: u64,
    spent_txo_sum: u64,
}

#[derive(Debug, Serialize, Clone)]
pub struct AlkanesUtxoEntry {
    pub value: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FormattedUtxo {
    pub tx_id: String,
    pub output_index: u32,
    pub satoshis: u64,
    pub script_pk: String,
    pub address: String,
    pub inscriptions: Vec<String>,
    pub runes: Value,
    pub alkanes: HashMap<String, AlkanesUtxoEntry>,
    pub confirmations: u64,
    pub indexed: bool,
}

#[derive(Clone)]
pub struct OylApiState {
    pub config: OylApiConfig,
    pub essentials: Arc<EssentialsProvider>,
    pub ammdata: Arc<AmmDataProvider>,
    pub subfrost: Arc<SubfrostProvider>,
    pub http_client: Client,
}

pub struct GetAlkanesParams {
    pub limit: u64,
    pub offset: Option<u64>,
    pub sort_by: Option<String>,
    pub order: Option<String>,
    pub search_query: Option<String>,
}

pub async fn get_alkanes_by_address(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
) -> Value {
    crate::debug_timer_log!("get_alkanes_by_address");
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let balances = match get_balance_for_address(blockhash.clone(), &state.essentials, &address) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    if balances.is_empty() {
        return json!({ "statusCode": 200, "data": [] });
    }

    let ids: Vec<SchemaAlkaneId> = balances.keys().copied().collect();
    let records = match state.essentials.get_creation_records_by_id(GetCreationRecordsByIdParams {
        blockhash: blockhash.clone(),
        alkanes: ids,
    }) {
        Ok(r) => r.records,
        Err(err) => return internal_error(err),
    };
    let mut rec_map: HashMap<SchemaAlkaneId, _> = HashMap::new();
    for rec in records.into_iter().flatten() {
        rec_map.insert(rec.alkane, rec);
    }

    let now_ts = now_ts();
    let mut out: Vec<Value> = Vec::new();

    for (alkane, balance) in balances {
        let rec = rec_map.get(&alkane);
        let name = rec.and_then(|r| r.names.first()).cloned().unwrap_or_default();
        let symbol = rec
            .and_then(|r| r.symbols.first())
            .cloned()
            .unwrap_or_default()
            .to_ascii_uppercase();

        let (frbtc_price, busd_price) =
            match canonical_pool_prices(blockhash.clone(), state, &alkane, now_ts) {
                Ok(v) => v,
                Err(err) => return internal_error(err),
            };
        let price_usd = match latest_token_usd_close(blockhash.clone(), state, &alkane) {
            Ok(v) => v.unwrap_or(0),
            Err(err) => return internal_error(err),
        };
        let image = format!("{}/{}-{}.png", state.config.alkane_icon_cdn, alkane.block, alkane.tx);

        out.push(json!({
            "name": name,
            "symbol": symbol,
            "balance": balance.to_string(),
            "alkaneId": alkane_id_json(&alkane),
            "floorPrice": scale_price_u128(price_usd),
            "frbtcPoolPriceInSats": frbtc_price.to_string(),
            "busdPoolPriceInUsd": scale_price_u128(busd_price),
            "priceUsd": scale_price_u128(price_usd),
            "priceInSatoshi": frbtc_price.to_string(),
            "tokenImage": image,
            "idClubMarketplace": false,
        }));
    }

    json!({ "statusCode": 200, "data": out })
}

pub async fn get_bitcoin_price(blockhash: StateAt, state: &OylApiState) -> Value {
    crate::debug_timer_log!("get_bitcoin_price");
    match btc_price_usd_cached(blockhash.clone(), state) {
        Ok(price) => json!({
            "statusCode": 200,
            "data": {
                "bitcoin": {
                    "usd": scale_price_u128(price),
                }
            }
        }),
        Err(err) => internal_error(err),
    }
}

pub async fn get_alkanes_utxo(blockhash: StateAt, state: &OylApiState, address: &str) -> Value {
    crate::debug_timer_log!("get_alkanes_utxo");
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };
    let utxos = match get_address_utxos(
        blockhash.clone(),
        &state.essentials,
        &state.http_client,
        &address,
        state.config.ord_endpoint.as_deref(),
    )
    .await
    {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    json!({ "statusCode": 200, "data": utxos })
}

pub async fn get_address_utxos_portfolio(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    spend_strategy: Option<Value>,
) -> Value {
    crate::debug_timer_log!("get_address_utxos_portfolio");
    if address.trim().is_empty() {
        return json!({
            "statusCode": 200,
            "data": {
                "utxos": [],
                "alkaneUtxos": [],
                "spendableTotalBalance": 0,
                "spendableUtxos": [],
                "runeUtxos": [],
                "ordUtxos": [],
                "pendingUtxos": [],
                "pendingTotalBalance": 0,
                "totalBalance": 0,
            }
        });
    }

    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let mut utxos = match get_address_utxos(
        blockhash.clone(),
        &state.essentials,
        &state.http_client,
        &address,
        state.config.ord_endpoint.as_deref(),
    )
    .await
    {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    if should_sort_greatest_to_least(spend_strategy) {
        utxos.sort_by(|a, b| b.satoshis.cmp(&a.satoshis));
    }

    let (_confirmed_balance, pending_balance, total_balance) =
        match fetch_address_balances(&state.http_client, &address).await {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };

    let mut alkane_utxos = Vec::new();
    let mut rune_utxos = Vec::new();
    let mut ord_utxos = Vec::new();
    let mut spendable_utxos = Vec::new();
    let mut pending_utxos = Vec::new();
    let mut spendable_total: u64 = 0;

    for utxo in &utxos {
        if !utxo.alkanes.is_empty() {
            alkane_utxos.push(utxo.clone());
        }
        if let Value::Object(map) = &utxo.runes {
            if !map.is_empty() {
                rune_utxos.push(utxo.clone());
            }
        }
        if !utxo.inscriptions.is_empty() {
            ord_utxos.push(utxo.clone());
        }
        if utxo.confirmations == 0 {
            pending_utxos.push(utxo.clone());
        }
        if utxo.alkanes.is_empty() && is_clean_btc(utxo) {
            spendable_total = spendable_total.saturating_add(utxo.satoshis);
            spendable_utxos.push(utxo.clone());
        }
    }

    json!({
        "statusCode": 200,
        "data": {
            "utxos": utxos,
            "alkaneUtxos": alkane_utxos,
            "spendableTotalBalance": spendable_total,
            "spendableUtxos": spendable_utxos,
            "runeUtxos": rune_utxos,
            "ordUtxos": ord_utxos,
            "pendingUtxos": pending_utxos,
            "pendingTotalBalance": pending_balance,
            "totalBalance": total_balance,
        }
    })
}

pub async fn get_amm_utxos(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    spend_strategy: Option<Value>,
) -> Value {
    crate::debug_timer_log!("get_amm_utxos");
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let mut utxos = match get_address_utxos(
        blockhash.clone(),
        &state.essentials,
        &state.http_client,
        &address,
        state.config.ord_endpoint.as_deref(),
    )
    .await
    {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    utxos.retain(|u| !u.alkanes.is_empty() || is_clean_btc(u));

    if should_sort_greatest_to_least(spend_strategy) {
        utxos.sort_by(|a, b| b.satoshis.cmp(&a.satoshis));
    }

    json!({ "statusCode": 200, "data": { "utxos": utxos } })
}

pub async fn get_alkanes(
    blockhash: StateAt,
    state: &OylApiState,
    params: GetAlkanesParams,
) -> Value {
    crate::debug_timer_log!("get_alkanes");
    let query_blockhash = blockhash.clone();
    if params.limit == 0 {
        return error_response(400, "limit_required");
    }
    let mut limit = params.limit as usize;
    let offset = params.offset.unwrap_or(0) as usize;
    let sort_by = params.sort_by.unwrap_or_else(|| "volumeAllTime".to_string());
    let order = params.order.unwrap_or_else(|| "desc".to_string());
    let search_cfg = match AmmDataConfig::load_from_global_config() {
        Ok(cfg) => Some(cfg),
        Err(err) => return internal_error(err),
    };
    let search_index_enabled = search_cfg.as_ref().map(|c| c.search_index_enabled).unwrap_or(false);
    let derived_quotes: Vec<SchemaAlkaneId> = search_cfg
        .as_ref()
        .and_then(|c| c.derived_liquidity.as_ref())
        .map(|c| c.derived_quotes.iter().map(|dq| dq.alkane).collect())
        .unwrap_or_default();
    let mut search_prefix_min =
        search_cfg.as_ref().map(|c| c.search_prefix_min_len as usize).unwrap_or(2);
    let mut search_prefix_max =
        search_cfg.as_ref().map(|c| c.search_prefix_max_len as usize).unwrap_or(6);
    let search_scan_cap = search_cfg.as_ref().map(|c| c.search_fallback_scan_cap).unwrap_or(5000);
    let search_limit_cap = search_cfg.as_ref().map(|c| c.search_limit_cap).unwrap_or(20) as usize;
    if search_prefix_min == 0 {
        search_prefix_min = 2;
    }
    if search_prefix_max < search_prefix_min {
        search_prefix_max = search_prefix_min;
    }
    let mut tokens = Vec::new();
    let mut total: usize = 0;

    let query_opt = params.search_query.as_deref().and_then(|q| {
        let trimmed = q.trim();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    });

    if let Some(raw_query) = query_opt {
        let Some(query_norm) = normalize_search_text(raw_query) else {
            return json!({
                "statusCode": 200,
                "data": {
                    "tokens": tokens,
                    "total": total,
                    "count": tokens.len(),
                    "offset": offset,
                    "limit": limit,
                }
            });
        };
        let query_len = query_norm.chars().count();
        if query_len < search_prefix_min {
            return json!({
                "statusCode": 200,
                "data": {
                    "tokens": tokens,
                    "total": total,
                    "count": tokens.len(),
                    "offset": offset,
                    "limit": limit,
                }
            });
        }

        limit = std::cmp::min(limit, search_limit_cap);
        let desc = order.eq_ignore_ascii_case("desc");
        let mut ids: Vec<SchemaAlkaneId> = Vec::new();

        if search_index_enabled && query_len <= search_prefix_max {
            let (field, derived_quote) = search_index_field_for_sort(&sort_by);
            if let Some(quote) = derived_quote {
                if field != SearchIndexField::Holders {
                    ids = match state.ammdata.get_token_derived_search_index_page(
                        GetTokenDerivedSearchIndexPageParams {
                            blockhash: query_blockhash,
                            quote,
                            field,
                            prefix: query_norm.clone(),
                            offset: offset as u64,
                            limit: limit as u64,
                            desc,
                        },
                    ) {
                        Ok(res) => res.ids,
                        Err(err) => return internal_error(err),
                    };
                }
            } else {
                ids =
                    match state.ammdata.get_token_search_index_page(GetTokenSearchIndexPageParams {
                        blockhash: query_blockhash,
                        field,
                        prefix: query_norm.clone(),
                        offset: offset as u64,
                        limit: limit as u64,
                        desc,
                    }) {
                        Ok(res) => res.ids,
                        Err(err) => return internal_error(err),
                    };
            }
        }

        if ids.is_empty() {
            ids = match search_by_holders_scan(
                blockhash.clone(),
                state,
                &query_norm,
                offset as u64,
                limit as u64,
                search_scan_cap,
            ) {
                Ok(v) => v,
                Err(err) => return internal_error(err),
            };
            let (_field, derived_quote) = search_index_field_for_sort(&sort_by);
            if let Some(quote) = derived_quote {
                ids = match filter_ids_with_derived_metrics(blockhash.clone(), state, ids, quote) {
                    Ok(v) => v,
                    Err(err) => return internal_error(err),
                };
            }
        }

        total = offset + ids.len();

        if !ids.is_empty() {
            let records =
                match state.essentials.get_creation_records_by_id(GetCreationRecordsByIdParams {
                    blockhash: query_blockhash,
                    alkanes: ids.clone(),
                }) {
                    Ok(res) => res.records,
                    Err(err) => return internal_error(err),
                };
            let metrics = match token_metrics_multi(blockhash.clone(), state, &ids) {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let holders = match holders_counts_multi(blockhash.clone(), state, &ids) {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };

            for ((rec_opt, metrics), holders) in
                records.into_iter().zip(metrics.into_iter()).zip(holders.into_iter())
            {
                let Some(rec) = rec_opt else { continue };
                let token = match build_alkane_token(
                    blockhash.clone(),
                    state,
                    rec,
                    metrics,
                    holders,
                    &derived_quotes,
                ) {
                    Ok(v) => v,
                    Err(err) => return internal_error(err),
                };
                tokens.push(token);
            }
        }
    } else {
        let desc = order.eq_ignore_ascii_case("desc");
        let sort_index = alkane_sort_index(&sort_by);
        total = match sort_index {
            AlkaneSortIndex::Derived { quote, .. } => {
                match state.ammdata.get_token_derived_metrics_index_count(
                    GetTokenDerivedMetricsIndexCountParams { blockhash: query_blockhash, quote },
                ) {
                    Ok(res) => res.count as usize,
                    Err(err) => return internal_error(err),
                }
            }
            _ => match state
                .essentials
                .get_creation_count(GetCreationCountParams { blockhash: query_blockhash })
            {
                Ok(res) => res.count as usize,
                Err(err) => return internal_error(err),
            },
        };

        if total > 0 && limit > 0 {
            let ids = match fetch_sorted_alkane_ids_no_query(
                blockhash.clone(),
                state,
                sort_index,
                desc,
                offset as u64,
                limit as u64,
                total as u64,
            ) {
                Ok(v) => v,
                Err(err) => return internal_error(err),
            };

            if !ids.is_empty() {
                let records = match state.essentials.get_creation_records_by_id(
                    GetCreationRecordsByIdParams {
                        blockhash: query_blockhash,
                        alkanes: ids.clone(),
                    },
                ) {
                    Ok(res) => res.records,
                    Err(err) => return internal_error(err),
                };
                let metrics = match token_metrics_multi(blockhash.clone(), state, &ids) {
                    Ok(res) => res,
                    Err(err) => return internal_error(err),
                };
                let holders = match holders_counts_multi(blockhash.clone(), state, &ids) {
                    Ok(res) => res,
                    Err(err) => return internal_error(err),
                };

                for ((rec_opt, metrics), holders) in
                    records.into_iter().zip(metrics.into_iter()).zip(holders.into_iter())
                {
                    let Some(rec) = rec_opt else { continue };
                    let token = match build_alkane_token(
                        blockhash.clone(),
                        state,
                        rec,
                        metrics,
                        holders,
                        &derived_quotes,
                    ) {
                        Ok(v) => v,
                        Err(err) => return internal_error(err),
                    };
                    tokens.push(token);
                }
            }
        }
    }

    json!({
        "statusCode": 200,
        "data": {
            "tokens": tokens,
            "total": total,
            "count": tokens.len(),
            "offset": offset,
            "limit": limit,
        }
    })
}

pub async fn get_global_alkanes_search(
    blockhash: StateAt,
    state: &OylApiState,
    query: &str,
) -> Value {
    crate::debug_timer_log!("get_global_alkanes_search");
    let records = match fetch_records_for_query(blockhash.clone(), state, Some(query)) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let derived_quotes = match derived_quotes_from_config() {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let tokens = if records.is_empty() {
        Vec::new()
    } else {
        let ids: Vec<SchemaAlkaneId> = records.iter().map(|rec| rec.alkane).collect();
        let metrics = match token_metrics_multi(blockhash.clone(), state, &ids) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };
        let holders = match holders_counts_multi(blockhash.clone(), state, &ids) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };
        let mut combined = records
            .into_iter()
            .zip(metrics.into_iter())
            .zip(holders.into_iter())
            .map(|((rec, metrics), holders)| (holders, rec, metrics))
            .collect::<Vec<_>>();
        combined.sort_by(|a, b| b.0.cmp(&a.0));

        let mut out = Vec::new();
        for (holders, rec, metrics) in combined {
            let token = match build_alkane_token(
                blockhash.clone(),
                state,
                rec,
                metrics,
                holders,
                &derived_quotes,
            ) {
                Ok(v) => v,
                Err(err) => return internal_error(err),
            };
            out.push(token);
        }
        out
    };

    let pools = match search_pools(blockhash.clone(), state, query) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    json!({ "statusCode": 200, "data": { "tokens": tokens, "pools": pools } })
}

pub async fn get_alkane_details(
    blockhash: StateAt,
    state: &OylApiState,
    block: &str,
    tx: &str,
) -> Value {
    crate::debug_timer_log!("get_alkane_details");
    let Some(alkane) = parse_alkane_id_fields(block, tx) else {
        return error_response(400, "invalid_alkane_id");
    };

    let rec = match state
        .essentials
        .get_creation_record(GetCreationRecordParams { blockhash: blockhash.clone(), alkane })
    {
        Ok(resp) => resp.record,
        Err(err) => return internal_error(err),
    };
    let Some(rec) = rec else {
        return error_response(404, "alkane_not_found");
    };

    let metrics = match token_metrics(blockhash.clone(), state, &alkane) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let holders = match holders_count(blockhash.clone(), state, &alkane) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let derived_quotes = match derived_quotes_from_config() {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let mut token = match build_alkane_token(
        blockhash.clone(),
        state,
        rec,
        metrics,
        holders,
        &derived_quotes,
    ) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    let supply = match latest_circulating_supply(blockhash.clone(), state, &alkane) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let now_ts = now_ts();
    let (frbtc_price, _busd_price) =
        match canonical_pool_prices(blockhash.clone(), state, &alkane, now_ts) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
    let token_image =
        format!("{}/{}-{}.png", state.config.alkane_icon_cdn, alkane.block, alkane.tx);

    if let Value::Object(ref mut map) = token {
        map.insert("decimals".to_string(), json!(8));
        map.insert("supply".to_string(), json!(supply.to_string()));
        map.insert("priceInSatoshi".to_string(), json!(frbtc_price.to_string()));
        map.insert("tokenImage".to_string(), json!(token_image));
    }

    json!({ "statusCode": 200, "data": token })
}

pub async fn get_pools(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Value {
    crate::debug_timer_log!("get_pools");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };

    let pools = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    let total = pools.len();
    let offset = offset.unwrap_or(0) as usize;
    let limit_usize = limit.map(|l| l as usize);

    let slice: Vec<Value> = pools
        .into_iter()
        .skip(offset)
        .take(limit_usize.unwrap_or(total))
        .map(|id| alkane_id_json(&id))
        .collect();

    json!({
        "statusCode": 200,
        "data": slice,
        "total": total,
        "offset": offset,
        "limit": limit,
    })
}

pub async fn get_pool_details(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    pool_block: &str,
    pool_tx: &str,
) -> Value {
    crate::debug_timer_log!("get_pool_details");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };

    let factory_ok = match state
        .ammdata
        .get_pool_factory(GetPoolFactoryParams { blockhash: blockhash.clone(), pool })
    {
        Ok(res) => match res.factory {
            Some(found) => found == factory,
            None => {
                let pools = match state.ammdata.get_factory_pools(GetFactoryPoolsParams {
                    blockhash: blockhash.clone(),
                    factory,
                }) {
                    Ok(res) => res.pools,
                    Err(err) => return internal_error(err),
                };
                pools.contains(&pool)
            }
        },
        Err(err) => return internal_error(err),
    };

    let details = if !factory_ok {
        None
    } else {
        match build_pool_details(blockhash.clone(), state, Some(factory), pool, None) {
            Ok(Some(d)) => Some(d.value),
            Ok(None) => None,
            Err(err) => return internal_error(err),
        }
    };

    json!({ "statusCode": 200, "data": details })
}

pub async fn get_address_positions(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    factory_block: &str,
    factory_tx: &str,
) -> Value {
    crate::debug_timer_log!("get_address_positions");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let balances = match get_balance_for_address(blockhash.clone(), &state.essentials, &address) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    if balances.is_empty() {
        return json!({ "statusCode": 200, "data": [] });
    }

    let pool_ids = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    let pool_set: HashSet<SchemaAlkaneId> = pool_ids.into_iter().collect();

    let mut positions: Vec<(Value, u128)> = Vec::new();

    let mut details_ctx = PoolDetailsContext::new(true);
    for (pool_id, balance) in balances {
        if balance == 0 || !pool_set.contains(&pool_id) {
            continue;
        }
        let details = match build_pool_details(
            blockhash.clone(),
            state,
            Some(factory),
            pool_id,
            Some(&mut details_ctx),
        ) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        let Some(details) = details else { continue };

        let lp_supply = state
            .essentials
            .get_latest_circulating_supply(GetLatestCirculatingSupplyParams {
                blockhash: blockhash.clone(),
                alkane: pool_id,
            })
            .map(|res| res.supply)
            .unwrap_or(details.lp_supply);
        let share_value = if lp_supply == 0 { 0 } else { balance };

        let token0_value_usd = if lp_supply == 0 {
            0
        } else {
            details.token0_tvl_usd.saturating_mul(share_value).saturating_div(lp_supply)
        };
        let token1_value_usd = if lp_supply == 0 {
            0
        } else {
            details.token1_tvl_usd.saturating_mul(share_value).saturating_div(lp_supply)
        };
        let token0_value_sats = if lp_supply == 0 {
            0
        } else {
            details.token0_tvl_sats.saturating_mul(share_value).saturating_div(lp_supply)
        };
        let token1_value_sats = if lp_supply == 0 {
            0
        } else {
            details.token1_tvl_sats.saturating_mul(share_value).saturating_div(lp_supply)
        };

        let total_value_usd = token0_value_usd.saturating_add(token1_value_usd);
        let total_value_sats = token0_value_sats.saturating_add(token1_value_sats);

        let mut value = details.value;
        if let Value::Object(ref mut map) = value {
            map.insert("balance".to_string(), json!(balance.to_string()));
            map.insert("token0ValueInSats".to_string(), json!(token0_value_sats.to_string()));
            map.insert("token1ValueInSats".to_string(), json!(token1_value_sats.to_string()));
            map.insert("token0ValueInUsd".to_string(), json!(scale_price_u128(token0_value_usd)));
            map.insert("token1ValueInUsd".to_string(), json!(scale_price_u128(token1_value_usd)));
            map.insert("totalValueInSats".to_string(), json!(total_value_sats.to_string()));
            map.insert("totalValueInUsd".to_string(), json!(scale_price_u128(total_value_usd)));
        }

        positions.push((value, total_value_usd));
    }

    positions.sort_by(|a, b| b.1.cmp(&a.1));
    let data: Vec<Value> = positions.into_iter().map(|(v, _)| v).collect();

    json!({ "statusCode": 200, "data": data })
}

pub async fn get_all_pools_details(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    limit: Option<u64>,
    offset: Option<u64>,
    sort_by: Option<String>,
    order: Option<String>,
    address: Option<String>,
    search_query: Option<String>,
) -> Value {
    crate::debug_timer_log!("get_all_pools_details");
    let debug = debug_enabled();
    let module = "espo::modules::oylapi::storage";
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };

    let timer = debug::start_if(debug);
    let mut pools = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    debug::log_elapsed(module, "get_all_pools_details_fetch_pools", timer);
    if debug {
        eprintln!(
            "[oylapi] get_all_pools_details pools_initial={} factory={}:{}",
            pools.len(),
            factory.block,
            factory.tx
        );
    }

    if let Some(query) = search_query.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
        let timer = debug::start_if(debug);
        if let Some(id) = parse_alkane_id_str(query) {
            let mut filtered = Vec::new();
            for pool in pools.into_iter() {
                if pool == id {
                    filtered.push(pool);
                    continue;
                }
                let resp = match state
                    .ammdata
                    .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
                {
                    Ok(v) => v,
                    Err(err) => return internal_error(err),
                };
                if let Some(defs) = resp.defs {
                    if defs.base_alkane_id == id || defs.quote_alkane_id == id {
                        filtered.push(pool);
                    }
                }
            }
            pools = filtered;
        } else {
            let norm = normalize_query(query);
            let ids =
                match state.ammdata.get_pool_ids_by_name_prefix(GetPoolIdsByNamePrefixParams {
                    blockhash: blockhash.clone(),
                    prefix: norm,
                }) {
                    Ok(res) => res.ids,
                    Err(err) => return internal_error(err),
                };
            let id_set: HashSet<SchemaAlkaneId> = ids.into_iter().collect();
            pools = pools.into_iter().filter(|p| id_set.contains(p)).collect();
        }
        debug::log_elapsed(module, "get_all_pools_details_search_filter", timer);
        if debug {
            eprintln!(
                "[oylapi] get_all_pools_details pools_after_search={} query=\"{}\"",
                pools.len(),
                query
            );
        }
    }

    if let Some(addr) = address {
        let timer = debug::start_if(debug);
        let Some(addr) = normalize_address(&addr) else {
            return error_response(400, "invalid_address");
        };
        let balances = match get_balance_for_address(blockhash.clone(), &state.essentials, &addr) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        pools = pools
            .into_iter()
            .filter(|p| balances.get(p).copied().unwrap_or(0) > 0)
            .collect();
        debug::log_elapsed(module, "get_all_pools_details_address_filter", timer);
        if debug {
            eprintln!(
                "[oylapi] get_all_pools_details pools_after_address_filter={} addr={}",
                pools.len(),
                addr
            );
        }
    }

    let timer = debug::start_if(debug);
    let mut computed: Vec<PoolDetailsComputed> = Vec::new();
    let mut details_ctx = PoolDetailsContext::new(true);
    let mut pools_scanned: usize = 0;
    let mut pools_kept: usize = 0;
    for pool in pools.into_iter() {
        pools_scanned += 1;
        let details = match build_pool_details(
            blockhash.clone(),
            state,
            Some(factory),
            pool,
            Some(&mut details_ctx),
        ) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        if let Some(details) = details {
            computed.push(details);
            pools_kept += 1;
        }
    }
    debug::log_elapsed(module, "get_all_pools_details_build_details", timer);
    if debug {
        eprintln!(
            "[oylapi] get_all_pools_details pools_scanned={} pools_kept={}",
            pools_scanned, pools_kept
        );
    }

    if computed.is_empty() {
        return json!({
            "statusCode": 200,
            "data": {
                "count": 0,
                "pools": [],
                "total": 0,
                "offset": offset.unwrap_or(0),
                "limit": limit,
                "largestPool": Value::Null,
                "trendingPools": Value::Null,
                "totalTvl": 0.0,
                "totalPoolVolume24hChange": 0.0,
                "totalPoolVolume24h": 0.0,
            }
        });
    }

    let timer = debug::start_if(debug);
    let mut total_tvl: u128 = 0;
    let mut total_volume_1d: u128 = 0;
    let mut largest: Option<PoolDetailsComputed> = None;
    let mut trending: Option<PoolDetailsComputed> = None;

    for details in &computed {
        total_tvl = total_tvl.saturating_add(details.pool_tvl_usd);
        total_volume_1d = total_volume_1d.saturating_add(details.pool_volume_1d_usd);

        if largest.as_ref().map(|d| details.pool_tvl_usd > d.pool_tvl_usd).unwrap_or(true) {
            largest = Some(details.clone());
        }
        if trending
            .as_ref()
            .map(|d| details.tvl_change_24h > d.tvl_change_24h)
            .unwrap_or(true)
        {
            trending = Some(details.clone());
        }
    }
    debug::log_elapsed(module, "get_all_pools_details_aggregate", timer);

    let sort_by = sort_by.unwrap_or_else(|| "tvl".to_string());
    let order = order.unwrap_or_else(|| "desc".to_string());
    let desc = order.eq_ignore_ascii_case("desc");

    let timer = debug::start_if(debug);
    computed.sort_by(|a, b| {
        let (va, vb) = match sort_by.as_str() {
            "volume1d" => (a.pool_volume_1d_usd as f64, b.pool_volume_1d_usd as f64),
            "volume30d" => (a.pool_volume_30d_usd as f64, b.pool_volume_30d_usd as f64),
            "apr" => (a.pool_apr, b.pool_apr),
            "tvlChange" => (a.tvl_change_24h, b.tvl_change_24h),
            _ => (a.pool_tvl_usd as f64, b.pool_tvl_usd as f64),
        };
        if desc {
            vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        }
    });

    let total = computed.len();
    let offset = offset.unwrap_or(0) as usize;
    let limit_usize = limit.map(|l| l as usize);
    let paginated = computed
        .iter()
        .skip(offset)
        .take(limit_usize.unwrap_or(total))
        .map(|d| d.value.clone())
        .collect::<Vec<_>>();
    debug::log_elapsed(module, "get_all_pools_details_sort_paginate", timer);

    let largest_pool = largest.map(|d| {
        let mut val = d.value.clone();
        if let Value::Object(ref mut map) = val {
            map.insert("tvl".to_string(), json!(scale_price_u128(d.pool_tvl_usd)));
        }
        val
    });

    let trending_pool = trending.map(|d| {
        let mut val = d.value.clone();
        if let Value::Object(ref mut map) = val {
            map.insert("trend".to_string(), json!(d.tvl_change_24h));
        }
        json!({ "1d": val })
    });

    json!({
        "statusCode": 200,
        "data": {
            "count": paginated.len(),
            "pools": paginated,
            "total": total,
            "offset": offset,
            "limit": limit,
            "largestPool": largest_pool,
            "trendingPools": trending_pool,
            "totalTvl": scale_price_u128(total_tvl),
            "totalPoolVolume24hChange": 0.0,
            "totalPoolVolume24h": scale_price_u128(total_volume_1d),
        }
    })
}

pub async fn get_pool_swap_history(
    blockhash: StateAt,
    state: &OylApiState,
    pool_block: &str,
    pool_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_pool_swap_history");
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let defs = match state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
    {
        Ok(res) => match res.defs {
            Some(defs) => defs,
            None => {
                return json!({
                    "statusCode": 200,
                    "data": {
                        "items": {
                            "pool": {
                                "poolId": alkane_id_json(&pool),
                                "poolName": "",
                            },
                            "swaps": [],
                            "count": 0,
                            "offset": offset,
                            "total": 0,
                        },
                        "total": 0,
                        "count": 0,
                        "offset": offset,
                    }
                });
            }
        },
        Err(err) => return internal_error(err),
    };

    let resp = match state.ammdata.get_pool_activity_entries(GetPoolActivityEntriesParams {
        blockhash: blockhash.clone(),
        pool,
        offset,
        limit,
        kinds: Some(vec![ActivityKind::TradeBuy, ActivityKind::TradeSell]),
        successful,
        include_total,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let pool_name = match pool_name_from_defs(blockhash.clone(), state, &defs) {
        Ok(v) => pool_name_display(&v),
        Err(err) => return internal_error(err),
    };
    let mut swaps = Vec::new();
    for entry in resp.entries {
        let Some((sold_id, bought_id, sold_amt, bought_amt)) = trade_from_activity(&defs, &entry)
        else {
            continue;
        };
        swaps.push(json!({
            "transactionId": txid_hex(entry.txid),
            "pay": {
                "tokenId": alkane_id_json(&sold_id),
                "amount": sold_amt.to_string(),
            },
            "receive": {
                "tokenId": alkane_id_json(&bought_id),
                "amount": bought_amt.to_string(),
            },
            "address": address_from_spk_bytes(&entry.address_spk),
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    let items = json!({
        "pool": {
            "poolId": alkane_id_json(&pool),
            "poolName": pool_name,
        },
        "swaps": swaps,
        "count": swaps.len(),
        "offset": offset,
        "total": total,
    });

    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": swaps.len(),
            "offset": offset,
        }
    })
}

pub async fn get_token_swap_history(
    blockhash: StateAt,
    state: &OylApiState,
    token_block: &str,
    token_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_token_swap_history");
    let Some(token) = parse_alkane_id_fields(token_block, token_tx) else {
        return error_response(400, "invalid_token_id");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.ammdata.get_token_swaps_page(GetTokenSwapsPageParams {
        blockhash: blockhash.clone(),
        token,
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut swaps = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };
        let Some((sold_id, bought_id, sold_amt, bought_amt)) =
            trade_from_activity(&defs, &activity)
        else {
            continue;
        };
        let address = address_from_spk_bytes(&activity.address_spk);
        swaps.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "soldTokenBlockId": sold_id.block.to_string(),
            "soldTokenTxId": sold_id.tx.to_string(),
            "boughtTokenBlockId": bought_id.block.to_string(),
            "boughtTokenTxId": bought_id.tx.to_string(),
            "soldAmount": sold_amt.to_string(),
            "boughtAmount": bought_amt.to_string(),
            "sellerAddress": address,
            "address": address,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": swaps,
            "total": total,
            "count": swaps.len(),
            "offset": offset,
        }
    })
}

pub async fn get_pool_mint_history(
    blockhash: StateAt,
    state: &OylApiState,
    pool_block: &str,
    pool_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_pool_mint_history");
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let defs = match state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
    {
        Ok(res) => match res.defs {
            Some(defs) => defs,
            None => {
                return json!({
                    "statusCode": 200,
                    "data": { "items": [], "total": 0, "count": 0, "offset": offset }
                });
            }
        },
        Err(err) => return internal_error(err),
    };

    let resp = match state.ammdata.get_pool_activity_entries(GetPoolActivityEntriesParams {
        blockhash: blockhash.clone(),
        pool,
        offset,
        limit,
        kinds: Some(vec![ActivityKind::LiquidityAdd]),
        successful,
        include_total,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let lp_supply = match state.ammdata.get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
        blockhash: blockhash.clone(),
        pool,
    }) {
        Ok(res) => res.supply,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let token0_amt = abs_i128(entry.base_delta);
        let token1_amt = abs_i128(entry.quote_delta);
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "poolBlockId": pool.block.to_string(),
            "poolTxId": pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": token0_amt.to_string(),
            "token1Amount": token1_amt.to_string(),
            "lpTokenAmount": lp_supply.to_string(),
            "minterAddress": address,
            "address": address,
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_pool_burn_history(
    blockhash: StateAt,
    state: &OylApiState,
    pool_block: &str,
    pool_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_pool_burn_history");
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let defs = match state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
    {
        Ok(res) => match res.defs {
            Some(defs) => defs,
            None => {
                return json!({
                    "statusCode": 200,
                    "data": { "items": [], "total": 0, "count": 0, "offset": offset }
                });
            }
        },
        Err(err) => return internal_error(err),
    };

    let resp = match state.ammdata.get_pool_activity_entries(GetPoolActivityEntriesParams {
        blockhash: blockhash.clone(),
        pool,
        offset,
        limit,
        kinds: Some(vec![ActivityKind::LiquidityRemove]),
        successful,
        include_total,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let lp_supply = match state.ammdata.get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
        blockhash: blockhash.clone(),
        pool,
    }) {
        Ok(res) => res.supply,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let token0_amt = abs_i128(entry.base_delta);
        let token1_amt = abs_i128(entry.quote_delta);
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "poolBlockId": pool.block.to_string(),
            "poolTxId": pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": token0_amt.to_string(),
            "token1Amount": token1_amt.to_string(),
            "lpTokenAmount": lp_supply.to_string(),
            "burnerAddress": address,
            "address": address,
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_pool_creation_history(
    blockhash: StateAt,
    state: &OylApiState,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_pool_creation_history");
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.ammdata.get_pool_creations_page(GetPoolCreationsPageParams {
        blockhash: blockhash.clone(),
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };

        let creation_info = match state.ammdata.get_pool_creation_info(GetPoolCreationInfoParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
        }) {
            Ok(res) => res.info,
            Err(err) => return internal_error(err),
        };
        let (token0_amt, token1_amt, token_supply, creator_spk) = if let Some(info) = creation_info
        {
            (
                info.initial_token0_amount,
                info.initial_token1_amount,
                info.initial_lp_supply,
                info.creator_spk,
            )
        } else {
            (0, 0, 0, Vec::new())
        };
        let creator = if creator_spk.is_empty() {
            address_from_spk_bytes(&activity.address_spk)
        } else {
            address_from_spk_bytes(&creator_spk)
        };

        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": token0_amt.to_string(),
            "token1Amount": token1_amt.to_string(),
            "tokenSupply": token_supply.to_string(),
            "creatorAddress": creator,
            "address": creator,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_swap_history_for_pool(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    pool_block: &str,
    pool_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_swap_history_for_pool");
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let defs = match state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
    {
        Ok(res) => match res.defs {
            Some(defs) => defs,
            None => {
                return json!({
                    "statusCode": 200,
                    "data": { "items": [], "total": 0, "count": 0, "offset": offset }
                });
            }
        },
        Err(err) => return internal_error(err),
    };

    let resp = match state.ammdata.get_address_pool_swaps_page(GetAddressPoolSwapsPageParams {
        blockhash: blockhash.clone(),
        address_spk: address_spk.clone(),
        pool,
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let Some((sold_id, bought_id, sold_amt, bought_amt)) =
            trade_from_activity(&defs, &activity)
        else {
            continue;
        };
        let address = address_from_spk_bytes(&activity.address_spk);
        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": pool.block.to_string(),
            "poolTxId": pool.tx.to_string(),
            "soldTokenBlockId": sold_id.block.to_string(),
            "soldTokenTxId": sold_id.tx.to_string(),
            "boughtTokenBlockId": bought_id.block.to_string(),
            "boughtTokenTxId": bought_id.tx.to_string(),
            "soldAmount": sold_amt.to_string(),
            "boughtAmount": bought_amt.to_string(),
            "sellerAddress": address,
            "address": address,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_swap_history_for_token(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    token_block: &str,
    token_tx: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_swap_history_for_token");
    let Some(token) = parse_alkane_id_fields(token_block, token_tx) else {
        return error_response(400, "invalid_token_id");
    };
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.ammdata.get_address_token_swaps_page(GetAddressTokenSwapsPageParams {
        blockhash: blockhash.clone(),
        address_spk: address_spk.clone(),
        token,
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };
        let Some((sold_id, bought_id, sold_amt, bought_amt)) =
            trade_from_activity(&defs, &activity)
        else {
            continue;
        };
        let address = address_from_spk_bytes(&activity.address_spk);
        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "soldTokenBlockId": sold_id.block.to_string(),
            "soldTokenTxId": sold_id.tx.to_string(),
            "boughtTokenBlockId": bought_id.block.to_string(),
            "boughtTokenTxId": bought_id.tx.to_string(),
            "soldAmount": sold_amt.to_string(),
            "boughtAmount": bought_amt.to_string(),
            "sellerAddress": address,
            "address": address,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_pool_creation_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_pool_creation_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp =
        match state
            .ammdata
            .get_address_pool_creations_page(GetAddressPoolCreationsPageParams {
                blockhash: blockhash.clone(),
                address_spk,
                offset,
                limit,
            }) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };

        let creation_info = match state.ammdata.get_pool_creation_info(GetPoolCreationInfoParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
        }) {
            Ok(res) => res.info,
            Err(err) => return internal_error(err),
        };
        let (token0_amt, token1_amt, token_supply, creator_spk) = if let Some(info) = creation_info
        {
            (
                info.initial_token0_amount,
                info.initial_token1_amount,
                info.initial_lp_supply,
                info.creator_spk,
            )
        } else {
            (0, 0, 0, Vec::new())
        };
        let creator = if creator_spk.is_empty() {
            address_from_spk_bytes(&activity.address_spk)
        } else {
            address_from_spk_bytes(&creator_spk)
        };

        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": token0_amt.to_string(),
            "token1Amount": token1_amt.to_string(),
            "tokenSupply": token_supply.to_string(),
            "creatorAddress": creator,
            "address": creator,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_pool_mint_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_pool_mint_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.ammdata.get_address_pool_mints_page(GetAddressPoolMintsPageParams {
        blockhash: blockhash.clone(),
        address_spk,
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };

        let lp_supply = match state.ammdata.get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
        }) {
            Ok(res) => res.supply,
            Err(err) => return internal_error(err),
        };
        let address = address_from_spk_bytes(&activity.address_spk);
        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": abs_i128(activity.base_delta).to_string(),
            "token1Amount": abs_i128(activity.quote_delta).to_string(),
            "lpTokenAmount": lp_supply.to_string(),
            "minterAddress": address,
            "address": address,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_pool_burn_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_pool_burn_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.ammdata.get_address_pool_burns_page(GetAddressPoolBurnsPageParams {
        blockhash: blockhash.clone(),
        address_spk,
        offset,
        limit,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut items = Vec::new();
    for entry in resp.entries {
        let activity = match state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        }) {
            Ok(res) => match res.entry {
                Some(v) => v,
                None => continue,
            },
            Err(err) => return internal_error(err),
        };
        if successful.unwrap_or(false) && !activity.success {
            continue;
        }
        let defs = if let Some(cached) = defs_cache.get(&entry.pool) {
            *cached
        } else {
            let defs_resp = match state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool: entry.pool })
            {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(entry.pool, defs);
            defs
        };

        let lp_supply = match state.ammdata.get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
        }) {
            Ok(res) => res.supply,
            Err(err) => return internal_error(err),
        };
        let address = address_from_spk_bytes(&activity.address_spk);
        items.push(json!({
            "transactionId": txid_hex(activity.txid),
            "poolBlockId": entry.pool.block.to_string(),
            "poolTxId": entry.pool.tx.to_string(),
            "token0BlockId": defs.base_alkane_id.block.to_string(),
            "token0TxId": defs.base_alkane_id.tx.to_string(),
            "token1BlockId": defs.quote_alkane_id.block.to_string(),
            "token1TxId": defs.quote_alkane_id.tx.to_string(),
            "token0Amount": abs_i128(activity.base_delta).to_string(),
            "token1Amount": abs_i128(activity.quote_delta).to_string(),
            "lpTokenAmount": lp_supply.to_string(),
            "burnerAddress": address,
            "address": address,
            "timestamp": iso_timestamp(activity.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_wrap_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_wrap_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.subfrost.get_wrap_events_by_address(GetWrapEventsByAddressParams {
        blockhash: blockhash.clone(),
        address_spk: address_spk.clone(),
        offset,
        limit,
        successful,
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "address": address,
            "amount": entry.amount.to_string(),
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_address_unwrap_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_address_unwrap_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.subfrost.get_unwrap_events_by_address(GetUnwrapEventsByAddressParams {
        blockhash: blockhash.clone(),
        address_spk: address_spk.clone(),
        offset,
        limit,
        successful,
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "address": address,
            "amount": entry.amount.to_string(),
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_all_wrap_history(
    blockhash: StateAt,
    state: &OylApiState,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_all_wrap_history");
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.subfrost.get_wrap_events_all(GetWrapEventsAllParams {
        blockhash: blockhash.clone(),
        offset,
        limit,
        successful,
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "address": address,
            "amount": entry.amount.to_string(),
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_all_unwrap_history(
    blockhash: StateAt,
    state: &OylApiState,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_all_unwrap_history");
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);

    let resp = match state.subfrost.get_unwrap_events_all(GetUnwrapEventsAllParams {
        blockhash: blockhash.clone(),
        offset,
        limit,
        successful,
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    let mut items = Vec::new();
    for entry in resp.entries {
        let address = address_from_spk_bytes(&entry.address_spk);
        items.push(json!({
            "transactionId": txid_hex(entry.txid),
            "address": address,
            "amount": entry.amount.to_string(),
            "timestamp": iso_timestamp(entry.timestamp),
        }));
    }

    let total = if include_total { resp.total } else { 0 };
    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_total_unwrap_amount(
    blockhash: StateAt,
    state: &OylApiState,
    block_height: Option<u32>,
    successful: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_total_unwrap_amount");
    let successful = successful.unwrap_or(false);
    let total = if let Some(height) = block_height {
        match state.subfrost.get_unwrap_total_at_or_before(GetUnwrapTotalAtOrBeforeParams {
            blockhash: blockhash.clone(),
            height,
            successful,
        }) {
            Ok(res) => res.total.unwrap_or(0),
            Err(err) => return internal_error(err),
        }
    } else {
        match state.subfrost.get_unwrap_total_latest(GetUnwrapTotalLatestParams {
            blockhash: blockhash.clone(),
            successful,
            height: None,
            height_present: false,
        }) {
            Ok(res) => res.total,
            Err(err) => return internal_error(err),
        }
    };

    json!({
        "statusCode": 200,
        "data": { "totalAmount": total.to_string() }
    })
}

pub async fn get_all_address_amm_tx_history(
    blockhash: StateAt,
    state: &OylApiState,
    address: &str,
    transaction_type: Option<String>,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_all_address_amm_tx_history");
    let Some(address_spk) = address_spk_bytes(address) else {
        return error_response(400, "invalid_address");
    };
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);
    let successful_only = successful.unwrap_or(false);
    let tx_type = match parse_amm_tx_type(transaction_type.as_deref()) {
        Ok(v) => v,
        Err(msg) => return error_response(400, msg),
    };

    if let Some(AmmTxType::Wrap) = tx_type {
        let resp = match state.subfrost.get_wrap_events_by_address(GetWrapEventsByAddressParams {
            blockhash: blockhash.clone(),
            address_spk: address_spk.clone(),
            offset,
            limit,
            successful: if successful_only { Some(true) } else { None },
            height: None,
            height_present: false,
        }) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };
        let mut items = Vec::new();
        for entry in resp.entries {
            items.push(wrap_event_to_value(&entry, "wrap"));
        }
        let total = if include_total { resp.total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }
    if let Some(AmmTxType::Unwrap) = tx_type {
        let resp =
            match state.subfrost.get_unwrap_events_by_address(GetUnwrapEventsByAddressParams {
                blockhash: blockhash.clone(),
                address_spk: address_spk.clone(),
                offset,
                limit,
                successful: if successful_only { Some(true) } else { None },
                height: None,
                height_present: false,
            }) {
                Ok(res) => res,
                Err(err) => return internal_error(err),
            };
        let mut items = Vec::new();
        for entry in resp.entries {
            items.push(wrap_event_to_value(&entry, "unwrap"));
        }
        let total = if include_total { resp.total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }

    if let Some(kind) = tx_type {
        let kinds = amm_kinds_for_type(kind);
        let (entries, total) = match collect_amm_history_items(
            blockhash.clone(),
            state,
            AmmHistoryScope::Address(&address_spk),
            kinds.as_deref(),
            offset,
            limit,
            successful_only,
            include_total,
        ) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        let items = match amm_history_items_to_values(blockhash.clone(), state, entries) {
            Ok(values) => values.into_iter().map(|item| item.item).collect::<Vec<_>>(),
            Err(err) => return internal_error(err),
        };
        let total = if include_total { total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }

    let combined_limit = offset.saturating_add(limit);
    let (amm_entries, amm_total) = match collect_amm_history_items(
        blockhash.clone(),
        state,
        AmmHistoryScope::Address(&address_spk),
        None,
        0,
        combined_limit,
        successful_only,
        include_total,
    ) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let mut combined = match amm_history_items_to_values(blockhash.clone(), state, amm_entries) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    let wrap_resp = match state.subfrost.get_wrap_events_by_address(GetWrapEventsByAddressParams {
        blockhash: blockhash.clone(),
        address_spk: address_spk.clone(),
        offset: 0,
        limit: combined_limit,
        successful: if successful_only { Some(true) } else { None },
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };
    let unwrap_resp =
        match state.subfrost.get_unwrap_events_by_address(GetUnwrapEventsByAddressParams {
            blockhash: blockhash.clone(),
            address_spk: address_spk.clone(),
            offset: 0,
            limit: combined_limit,
            successful: if successful_only { Some(true) } else { None },
            height: None,
            height_present: false,
        }) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };

    for entry in wrap_resp.entries {
        combined.push(wrap_event_to_history_item(&entry, "wrap"));
    }
    for entry in unwrap_resp.entries {
        combined.push(wrap_event_to_history_item(&entry, "unwrap"));
    }

    combined.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| b.seq.cmp(&a.seq)));
    let items = combined
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|item| item.item)
        .collect::<Vec<_>>();
    let total = if include_total {
        amm_total.saturating_add(wrap_resp.total).saturating_add(unwrap_resp.total)
    } else {
        0
    };

    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_all_amm_tx_history(
    blockhash: StateAt,
    state: &OylApiState,
    transaction_type: Option<String>,
    count: Option<u64>,
    offset: Option<u64>,
    successful: Option<bool>,
    include_total: Option<bool>,
) -> Value {
    crate::debug_timer_log!("get_all_amm_tx_history");
    let limit = clamp_count(count);
    let offset = clamp_offset(offset);
    let include_total = include_total.unwrap_or(true);
    let successful_only = successful.unwrap_or(false);
    let tx_type = match parse_amm_tx_type(transaction_type.as_deref()) {
        Ok(v) => v,
        Err(msg) => return error_response(400, msg),
    };

    if let Some(AmmTxType::Wrap) = tx_type {
        let resp = match state.subfrost.get_wrap_events_all(GetWrapEventsAllParams {
            blockhash: blockhash.clone(),
            offset,
            limit,
            successful: if successful_only { Some(true) } else { None },
            height: None,
            height_present: false,
        }) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };
        let mut items = Vec::new();
        for entry in resp.entries {
            items.push(wrap_event_to_value(&entry, "wrap"));
        }
        let total = if include_total { resp.total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }
    if let Some(AmmTxType::Unwrap) = tx_type {
        let resp = match state.subfrost.get_unwrap_events_all(GetUnwrapEventsAllParams {
            blockhash: blockhash.clone(),
            offset,
            limit,
            successful: if successful_only { Some(true) } else { None },
            height: None,
            height_present: false,
        }) {
            Ok(res) => res,
            Err(err) => return internal_error(err),
        };
        let mut items = Vec::new();
        for entry in resp.entries {
            items.push(wrap_event_to_value(&entry, "unwrap"));
        }
        let total = if include_total { resp.total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }

    if let Some(kind) = tx_type {
        let kinds = amm_kinds_for_type(kind);
        let (entries, total) = match collect_amm_history_items(
            blockhash.clone(),
            state,
            AmmHistoryScope::All,
            kinds.as_deref(),
            offset,
            limit,
            successful_only,
            include_total,
        ) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        let items = match amm_history_items_to_values(blockhash.clone(), state, entries) {
            Ok(values) => values.into_iter().map(|item| item.item).collect::<Vec<_>>(),
            Err(err) => return internal_error(err),
        };
        let total = if include_total { total } else { 0 };
        return json!({
            "statusCode": 200,
            "data": {
                "items": items,
                "total": total,
                "count": items.len(),
                "offset": offset,
            }
        });
    }

    let combined_limit = offset.saturating_add(limit);
    let (amm_entries, amm_total) = match collect_amm_history_items(
        blockhash.clone(),
        state,
        AmmHistoryScope::All,
        None,
        0,
        combined_limit,
        successful_only,
        include_total,
    ) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };
    let mut combined = match amm_history_items_to_values(blockhash.clone(), state, amm_entries) {
        Ok(v) => v,
        Err(err) => return internal_error(err),
    };

    let wrap_resp = match state.subfrost.get_wrap_events_all(GetWrapEventsAllParams {
        blockhash: blockhash.clone(),
        offset: 0,
        limit: combined_limit,
        successful: if successful_only { Some(true) } else { None },
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };
    let unwrap_resp = match state.subfrost.get_unwrap_events_all(GetUnwrapEventsAllParams {
        blockhash: blockhash.clone(),
        offset: 0,
        limit: combined_limit,
        successful: if successful_only { Some(true) } else { None },
        height: None,
        height_present: false,
    }) {
        Ok(res) => res,
        Err(err) => return internal_error(err),
    };

    for entry in wrap_resp.entries {
        combined.push(wrap_event_to_history_item(&entry, "wrap"));
    }
    for entry in unwrap_resp.entries {
        combined.push(wrap_event_to_history_item(&entry, "unwrap"));
    }

    combined.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| b.seq.cmp(&a.seq)));
    let items = combined
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|item| item.item)
        .collect::<Vec<_>>();
    let total = if include_total {
        amm_total.saturating_add(wrap_resp.total).saturating_add(unwrap_resp.total)
    } else {
        0
    };

    json!({
        "statusCode": 200,
        "data": {
            "items": items,
            "total": total,
            "count": items.len(),
            "offset": offset,
        }
    })
}

pub async fn get_all_token_pairs(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
) -> Value {
    crate::debug_timer_log!("get_all_token_pairs");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };

    let pools = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };

    let mut meta_cache: HashMap<SchemaAlkaneId, TokenMeta> = HashMap::new();
    let mut out = Vec::new();
    for pool in pools {
        let pair = match build_token_pair(blockhash.clone(), state, pool, &mut meta_cache) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        if let Some(pair) = pair {
            out.push(pair.value);
        }
    }

    json!({ "statusCode": 200, "data": out })
}

pub async fn get_token_pairs(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    token_block: &str,
    token_tx: &str,
    sort_by: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    search_query: Option<String>,
) -> Value {
    crate::debug_timer_log!("get_token_pairs");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(token) = parse_alkane_id_fields(token_block, token_tx) else {
        return error_response(400, "invalid_token_id");
    };

    let pools = match state
        .ammdata
        .get_token_pools(GetTokenPoolsParams { blockhash: blockhash.clone(), token })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    let factory_pools = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    let factory_set: HashSet<SchemaAlkaneId> = factory_pools.into_iter().collect();

    let mut meta_cache: HashMap<SchemaAlkaneId, TokenMeta> = HashMap::new();
    let mut pairs: Vec<TokenPairComputed> = Vec::new();
    for pool in pools {
        if !factory_set.contains(&pool) {
            continue;
        }
        let pair = match build_token_pair(blockhash.clone(), state, pool, &mut meta_cache) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        if let Some(pair) = pair {
            pairs.push(pair);
        }
    }

    if let Some(query) = search_query.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
        if let Some(id) = parse_alkane_id_str(query) {
            pairs.retain(|pair| pair.pool_id == id || pair.token0_id == id || pair.token1_id == id);
        } else {
            let norm = normalize_query(query);
            pairs.retain(|pair| pair.search.contains(&norm));
        }
    }

    let sort_by = sort_by.unwrap_or_else(|| "tvl".to_string());
    if sort_by.eq_ignore_ascii_case("tvl") {
        pairs.sort_by(|a, b| b.tvl_usd.cmp(&a.tvl_usd));
    }

    let offset = offset.unwrap_or(0) as usize;
    let limit = limit.map(|v| v as usize);
    let total = pairs.len();
    let out = pairs
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(total))
        .map(|pair| pair.value)
        .collect::<Vec<_>>();

    json!({ "statusCode": 200, "data": out })
}

pub async fn get_alkane_swap_pair_details(
    blockhash: StateAt,
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    token_a_block: &str,
    token_a_tx: &str,
    token_b_block: &str,
    token_b_tx: &str,
) -> Value {
    crate::debug_timer_log!("get_alkane_swap_pair_details");
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(token_a) = parse_alkane_id_fields(token_a_block, token_a_tx) else {
        return error_response(400, "invalid_token_a_id");
    };
    let Some(token_b) = parse_alkane_id_fields(token_b_block, token_b_tx) else {
        return error_response(400, "invalid_token_b_id");
    };

    let factory_pools = match state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { blockhash: blockhash.clone(), factory })
    {
        Ok(res) => res.pools,
        Err(err) => return internal_error(err),
    };
    let mut filtered: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = HashMap::new();
    for pool in factory_pools {
        let defs = match state
            .ammdata
            .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })
        {
            Ok(res) => res.defs,
            Err(err) => return internal_error(err),
        };
        let Some(defs) = defs else { continue };

        let mut balances = match get_alkane_balances(blockhash.clone(), &state.essentials, &pool) {
            Ok(v) => v,
            Err(err) => return internal_error(err),
        };
        let base_reserve = balances.remove(&defs.base_alkane_id).unwrap_or(0);
        let quote_reserve = balances.remove(&defs.quote_alkane_id).unwrap_or(0);

        filtered.insert(
            pool,
            SchemaPoolSnapshot {
                base_reserve,
                quote_reserve,
                base_id: defs.base_alkane_id,
                quote_id: defs.quote_alkane_id,
            },
        );
    }

    let mut meta_cache: HashMap<SchemaAlkaneId, TokenMeta> = HashMap::new();
    let mut paths = Vec::new();
    if let Some(quote) = plan_exact_in_default_fee(&filtered, token_a, token_b, 1, 0, 3) {
        let mut path_ids = Vec::new();
        path_ids.push(alkane_id_json(&token_a));
        for hop in &quote.hops {
            path_ids.push(alkane_id_json(&hop.token_out));
        }
        let mut pools = Vec::new();
        for hop in &quote.hops {
            let pair = match build_token_pair(blockhash.clone(), state, hop.pool, &mut meta_cache) {
                Ok(v) => v,
                Err(err) => return internal_error(err),
            };
            if let Some(pair) = pair {
                pools.push(pair.value);
            }
        }
        paths.push(json!({ "path": path_ids, "pools": pools }));
    }

    json!({ "statusCode": 200, "data": paths })
}

pub async fn get_address_utxos(
    blockhash: StateAt,
    essentials: &EssentialsProvider,
    client: &Client,
    address: &str,
    ord_endpoint: Option<&str>,
) -> Result<Vec<FormattedUtxo>> {
    crate::debug_timer_log!("get_address_utxos");
    let network = get_network();
    let addr = Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .ok_or_else(|| anyhow!("invalid address"))?;
    let address_str = addr.to_string();
    let script_pubkey = addr.script_pubkey();
    let script_pk_hex = hex::encode(script_pubkey.as_bytes());

    let script_hash = script_hash_hex(&script_pubkey);
    let electrs_url = get_config()
        .electrs_esplora_url
        .clone()
        .ok_or_else(|| anyhow!("electrs_esplora_url missing"))?;

    let utxos = fetch_scripthash_utxos(client, &electrs_url, &script_hash).await?;
    if utxos.is_empty() {
        return Ok(Vec::new());
    }

    let tip_height = get_electrum_like().tip_height()? as u64;

    let mut outpoints: Vec<(Txid, u32)> = Vec::with_capacity(utxos.len());
    let mut outpoint_strs: Vec<String> = Vec::with_capacity(utxos.len());
    for utxo in &utxos {
        let txid = Txid::from_str(&utxo.txid)?;
        outpoints.push((txid, utxo.vout));
        outpoint_strs.push(format!("{}:{}", utxo.txid, utxo.vout));
    }

    let balances_by_outpoint =
        get_outpoint_balances_with_spent_batch(blockhash.clone(), essentials, &outpoints)?;

    let mut alkane_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    for lookup in balances_by_outpoint.values() {
        for be in &lookup.balances {
            alkane_ids.insert(be.alkane);
        }
    }

    let mut names: HashMap<SchemaAlkaneId, (String, String)> = HashMap::new();
    if !alkane_ids.is_empty() {
        let alkanes: Vec<SchemaAlkaneId> = alkane_ids.iter().copied().collect();
        let records = essentials
            .get_creation_records_by_id(GetCreationRecordsByIdParams {
                blockhash: blockhash.clone(),
                alkanes,
            })?
            .records;
        for rec in records.into_iter().flatten() {
            let name = rec.names.first().cloned().unwrap_or_default();
            let symbol = rec.symbols.first().cloned().unwrap_or_default().to_ascii_uppercase();
            names.insert(rec.alkane, (name, symbol));
        }
    }

    let ord_outputs = if let Some(endpoint) = ord_endpoint {
        fetch_ord_outputs(client, endpoint, &outpoint_strs).await?
    } else {
        HashMap::new()
    };

    let mut formatted: Vec<FormattedUtxo> = Vec::with_capacity(utxos.len());
    for (idx, utxo) in utxos.iter().enumerate() {
        let txid = Txid::from_str(&utxo.txid)?;
        let out_key = (txid, utxo.vout);
        let lookup = balances_by_outpoint.get(&out_key);
        let balances = lookup.map(|l| l.balances.clone()).unwrap_or_default();

        let mut alkanes_map: HashMap<String, AlkanesUtxoEntry> = HashMap::new();
        for be in balances {
            let id_str = format!("{}:{}", be.alkane.block, be.alkane.tx);
            let (name, symbol) = names.get(&be.alkane).cloned().unwrap_or_default();
            alkanes_map
                .insert(id_str, AlkanesUtxoEntry { value: be.amount.to_string(), name, symbol });
        }

        let ord = ord_outputs.get(&outpoint_strs[idx]).cloned().unwrap_or_else(OrdOutput::default);

        let confirmations = if utxo.status.confirmed {
            utxo.status
                .block_height
                .and_then(|h| tip_height.checked_sub(h).map(|d| d + 1))
                .unwrap_or(0)
        } else {
            0
        };

        formatted.push(FormattedUtxo {
            tx_id: utxo.txid.clone(),
            output_index: utxo.vout,
            satoshis: utxo.value,
            script_pk: script_pk_hex.clone(),
            address: address_str.clone(),
            inscriptions: ord.inscriptions,
            runes: ord.runes,
            alkanes: alkanes_map,
            confirmations,
            indexed: true,
        });
    }

    Ok(formatted)
}

fn script_hash_hex(script_pubkey: &ScriptBuf) -> String {
    let hash = sha256::Hash::hash(script_pubkey.as_bytes());
    hex::encode(hash.to_byte_array())
}

async fn fetch_scripthash_utxos(
    client: &Client,
    base_url: &str,
    script_hash: &str,
) -> Result<Vec<EsploraUtxo>> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/scripthash/{script_hash}/utxo");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo request failed: {e}"))?;
    let resp = resp
        .error_for_status()
        .map_err(|e| anyhow!("esplora scripthash utxo status error: {e}"))?;
    let body = resp
        .json::<Vec<EsploraUtxo>>()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo decode failed: {e}"))?;
    Ok(body)
}

async fn fetch_address_balances(client: &Client, address: &str) -> Result<(u64, u64, u64)> {
    let electrs_url = get_config()
        .electrs_esplora_url
        .clone()
        .ok_or_else(|| anyhow!("electrs_esplora_url missing"))?;
    let base = electrs_url.trim_end_matches('/');
    let url = format!("{base}/address/{address}");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow!("esplora address request failed: {e}"))?;
    let resp = resp
        .error_for_status()
        .map_err(|e| anyhow!("esplora address status error: {e}"))?;
    let stats = resp
        .json::<EsploraAddressStats>()
        .await
        .map_err(|e| anyhow!("esplora address decode failed: {e}"))?;
    let confirmed =
        stats.chain_stats.funded_txo_sum.saturating_sub(stats.chain_stats.spent_txo_sum);
    let pending = stats
        .mempool_stats
        .funded_txo_sum
        .saturating_sub(stats.mempool_stats.spent_txo_sum);
    Ok((confirmed, pending, confirmed.saturating_add(pending)))
}

fn normalize_address(address: &str) -> Option<String> {
    let network = get_network();
    Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.to_string())
}

fn address_spk_bytes(address: &str) -> Option<Vec<u8>> {
    let network = get_network();
    Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.script_pubkey().into_bytes())
}

fn error_response(code: u16, msg: &str) -> Value {
    json!({ "statusCode": code, "error": msg })
}

fn internal_error<E: std::fmt::Display>(err: E) -> Value {
    json!({ "statusCode": 500, "error": err.to_string() })
}

fn btc_price_usd_cached(blockhash: StateAt, state: &OylApiState) -> Result<u128> {
    // Read-paths must never call external ETH RPC. Use the latest indexed BTC/USD price.
    state
        .ammdata
        .get_latest_btc_usd_price(GetLatestBtcUsdPriceParams { blockhash: blockhash.clone() })
        .map_err(|e| anyhow!("failed to load btc/usd price from ammdata index: {e}"))?
        .ok_or_else(|| anyhow!("btc/usd price unavailable (ammdata index empty)"))
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[inline]
fn scale_price_u128(value: u128) -> f64 {
    (value as f64) / (PRICE_SCALE as f64)
}

fn pool_candle_volume_sums(
    blockhash: StateAt,
    state: &OylApiState,
    pool: &SchemaAlkaneId,
    now_ts: u64,
) -> Result<(u128, u128, u128, u128)> {
    let bucket_now = bucket_start_for(now_ts, Timeframe::D1);
    let window_start = bucket_now.saturating_sub(6 * Timeframe::D1.duration_secs());
    let table = state.ammdata.table();
    let prefix = table.candle_ns_prefix(pool, Timeframe::D1);
    let entries = state
        .ammdata
        .get_list_entries_desc(GetListEntriesDescParams { blockhash: blockhash.clone(), prefix })?
        .entries;

    let mut token0_volume_7d = 0u128;
    let mut token1_volume_7d = 0u128;
    let mut token0_volume_all = 0u128;
    let mut token1_volume_all = 0u128;

    for (k, v) in entries {
        let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() else { continue };
        let Ok(ts_str) = std::str::from_utf8(ts_bytes) else { continue };
        let Ok(ts) = ts_str.parse::<u64>() else { continue };
        let candle = decode_full_candle_v1(&v)?;
        let base_vol = candle.base_candle.volume;
        let quote_vol = candle.quote_candle.volume;
        token0_volume_all = token0_volume_all.saturating_add(base_vol);
        token1_volume_all = token1_volume_all.saturating_add(quote_vol);
        if ts >= window_start {
            token0_volume_7d = token0_volume_7d.saturating_add(base_vol);
            token1_volume_7d = token1_volume_7d.saturating_add(quote_vol);
        }
    }

    Ok((token0_volume_7d, token1_volume_7d, token0_volume_all, token1_volume_all))
}

#[inline]
fn parse_change_f64(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or(0.0)
}

fn alkane_id_json(alkane: &SchemaAlkaneId) -> Value {
    json!({ "block": alkane.block.to_string(), "tx": alkane.tx.to_string() })
}

fn parse_alkane_id_str(raw: &str) -> Option<SchemaAlkaneId> {
    let mut parts = raw.split(':');
    let block = parts.next()?.parse::<u32>().ok()?;
    let tx = parts.next()?.parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}

fn derived_quotes_from_config() -> Result<Vec<SchemaAlkaneId>> {
    Ok(AmmDataConfig::load_from_global_config()?
        .derived_liquidity
        .map(|cfg| cfg.derived_quotes.into_iter().map(|dq| dq.alkane).collect())
        .unwrap_or_default())
}

fn parse_alkane_id_fields(block: &str, tx: &str) -> Option<SchemaAlkaneId> {
    let block = block.parse::<u32>().ok()?;
    let tx = tx.parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}

fn normalize_query(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn is_clean_btc(utxo: &FormattedUtxo) -> bool {
    if !utxo.inscriptions.is_empty() {
        return false;
    }
    if let Value::Object(map) = &utxo.runes { map.is_empty() } else { utxo.runes.is_null() }
}

fn should_sort_greatest_to_least(spend_strategy: Option<Value>) -> bool {
    let Some(mut value) = spend_strategy else {
        return false;
    };
    if let Value::String(ref s) = value {
        if let Ok(parsed) = serde_json::from_str::<Value>(s) {
            value = parsed;
        }
    }
    if let Value::Object(map) = value {
        map.get("utxoSortGreatestToLeast").and_then(|v| v.as_bool()).unwrap_or(false)
    } else {
        false
    }
}

fn canonical_pool_prices(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
    now_ts: u64,
) -> Result<(u128, u128)> {
    state
        .ammdata
        .get_canonical_pool_prices(GetCanonicalPoolPricesParams {
            blockhash: blockhash.clone(),
            token: *token,
            now_ts,
        })
        .map(|res| (res.frbtc_price, res.busd_price))
}

fn latest_token_usd_close(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
) -> Result<Option<u128>> {
    state
        .ammdata
        .get_latest_token_usd_close(GetLatestTokenUsdCloseParams {
            blockhash: blockhash.clone(),
            token: *token,
            timeframe: Timeframe::M10,
        })
        .map(|res| res.close)
}

fn token_metrics(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
) -> Result<SchemaTokenMetricsV1> {
    let mut metrics = state
        .ammdata
        .get_token_metrics(GetTokenMetricsParams { blockhash: blockhash.clone(), token: *token })?
        .metrics;
    if metrics.change_1d.is_empty() {
        metrics.change_1d = "0".to_string();
    }
    if metrics.change_7d.is_empty() {
        metrics.change_7d = "0".to_string();
    }
    if metrics.change_30d.is_empty() {
        metrics.change_30d = "0".to_string();
    }
    if metrics.change_all_time.is_empty() {
        metrics.change_all_time = "0".to_string();
    }
    Ok(metrics)
}

fn holders_count(blockhash: StateAt, state: &OylApiState, token: &SchemaAlkaneId) -> Result<u64> {
    state
        .essentials
        .get_holders_count(GetHoldersCountParams { blockhash: blockhash.clone(), alkane: *token })
        .map(|res| res.count)
}

fn latest_circulating_supply(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
) -> Result<u128> {
    state
        .essentials
        .get_latest_circulating_supply(GetLatestCirculatingSupplyParams {
            blockhash: blockhash.clone(),
            alkane: *token,
        })
        .map(|res| res.supply)
}

fn latest_total_minted(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
) -> Result<u128> {
    state
        .essentials
        .get_latest_total_minted(GetLatestTotalMintedParams {
            blockhash: blockhash.clone(),
            alkane: *token,
        })
        .map(|res| res.total_minted)
}

fn canonical_quote_amount_tvl_usd(
    blockhash: StateAt,
    state: &OylApiState,
    amount: u128,
    unit: CanonicalQuoteUnit,
) -> Result<u128> {
    match unit {
        CanonicalQuoteUnit::Usd => Ok(amount.saturating_mul(PRICE_SCALE) / AMOUNT_SCALE),
        CanonicalQuoteUnit::Btc => {
            let btc_price = btc_price_usd_cached(blockhash.clone(), state)?;
            Ok(amount.saturating_mul(btc_price) / AMOUNT_SCALE)
        }
    }
}

fn canonical_quote_amount_tvl_sats(
    blockhash: StateAt,
    state: &OylApiState,
    amount: u128,
    unit: CanonicalQuoteUnit,
) -> Result<u128> {
    match unit {
        CanonicalQuoteUnit::Btc => Ok(amount),
        CanonicalQuoteUnit::Usd => {
            let btc_price = btc_price_usd_cached(blockhash.clone(), state)?;
            if btc_price == 0 {
                return Ok(0);
            }
            Ok(amount.saturating_mul(PRICE_SCALE).saturating_div(btc_price))
        }
    }
}

#[derive(Clone, Copy)]
enum AmmTxType {
    Swap,
    Mint,
    Burn,
    Creation,
    Wrap,
    Unwrap,
}

enum AmmHistoryScope<'a> {
    Address(&'a [u8]),
    All,
}

#[derive(Clone)]
struct AmmHistoryItem {
    entry: AmmHistoryEntry,
    activity: SchemaActivityV1,
}

#[derive(Clone)]
struct HistoryItemWithTs {
    ts: u64,
    seq: u32,
    item: Value,
}

#[derive(Clone)]
struct TokenMeta {
    name: String,
    symbol: String,
    label: String,
    image: String,
    decimals: u8,
}

#[derive(Clone)]
struct TokenPairComputed {
    value: Value,
    pool_id: SchemaAlkaneId,
    token0_id: SchemaAlkaneId,
    token1_id: SchemaAlkaneId,
    search: String,
    tvl_usd: u128,
}

fn parse_amm_tx_type(raw: Option<&str>) -> Result<Option<AmmTxType>, &'static str> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let norm = raw.trim().to_ascii_lowercase();
    if norm.is_empty() {
        return Ok(None);
    }
    match norm.as_str() {
        "swap" => Ok(Some(AmmTxType::Swap)),
        "mint" => Ok(Some(AmmTxType::Mint)),
        "burn" => Ok(Some(AmmTxType::Burn)),
        "creation" => Ok(Some(AmmTxType::Creation)),
        "wrap" => Ok(Some(AmmTxType::Wrap)),
        "unwrap" => Ok(Some(AmmTxType::Unwrap)),
        _ => Err("invalid_transaction_type"),
    }
}

fn amm_kinds_for_type(tx_type: AmmTxType) -> Option<Vec<ActivityKind>> {
    match tx_type {
        AmmTxType::Swap => Some(vec![ActivityKind::TradeBuy, ActivityKind::TradeSell]),
        AmmTxType::Mint => Some(vec![ActivityKind::LiquidityAdd]),
        AmmTxType::Burn => Some(vec![ActivityKind::LiquidityRemove]),
        AmmTxType::Creation => Some(vec![ActivityKind::PoolCreate]),
        AmmTxType::Wrap | AmmTxType::Unwrap => None,
    }
}

fn collect_amm_history_items(
    blockhash: StateAt,
    state: &OylApiState,
    scope: AmmHistoryScope<'_>,
    kinds: Option<&[ActivityKind]>,
    offset: usize,
    limit: usize,
    successful_only: bool,
    include_total: bool,
) -> Result<(Vec<AmmHistoryItem>, usize)> {
    let table = state.ammdata.table();
    let prefix = match scope {
        AmmHistoryScope::Address(spk) => table.address_amm_history_prefix(spk),
        AmmHistoryScope::All => table.amm_history_all_prefix(),
    };
    let entries = state
        .ammdata
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: blockhash.clone(),
            prefix: prefix.clone(),
        })?
        .entries;

    let mut total = 0usize;
    let mut out = Vec::new();
    let mut seen = 0usize;
    for (k, _v) in entries {
        let Some(entry) = parse_amm_history_key(&prefix, &k) else {
            continue;
        };
        if let Some(kinds) = kinds {
            if !kinds.contains(&entry.kind) {
                continue;
            }
        }
        let res = state.ammdata.get_activity_entry(GetActivityEntryParams {
            blockhash: blockhash.clone(),
            pool: entry.pool,
            ts: entry.ts,
            seq: entry.seq,
        })?;
        let activity = match res.entry {
            Some(activity) => activity,
            None => continue,
        };
        if successful_only && !activity.success {
            continue;
        }
        total += 1;
        if seen < offset {
            seen += 1;
            continue;
        }
        if out.len() < limit {
            out.push(AmmHistoryItem { entry, activity });
        }
        if !include_total && out.len() >= limit {
            break;
        }
    }

    let total = if include_total { total } else { 0 };
    Ok((out, total))
}

fn amm_history_items_to_values(
    blockhash: StateAt,
    state: &OylApiState,
    items: Vec<AmmHistoryItem>,
) -> Result<Vec<HistoryItemWithTs>> {
    let mut defs_cache: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
    let mut lp_supply_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    let mut creation_cache: HashMap<SchemaAlkaneId, Option<SchemaPoolCreationInfoV1>> =
        HashMap::new();
    let mut out = Vec::new();

    for item in items {
        let pool = item.entry.pool;
        let defs = if let Some(defs) = defs_cache.get(&pool) {
            *defs
        } else {
            let defs_resp = state
                .ammdata
                .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })?;
            let Some(defs) = defs_resp.defs else {
                continue;
            };
            defs_cache.insert(pool, defs);
            defs
        };

        match item.entry.kind {
            ActivityKind::TradeBuy | ActivityKind::TradeSell => {
                let Some((sold_id, bought_id, sold_amt, bought_amt)) =
                    trade_from_activity(&defs, &item.activity)
                else {
                    continue;
                };
                let address = address_from_spk_bytes(&item.activity.address_spk);
                let value = json!({
                    "transactionId": txid_hex(item.activity.txid),
                    "poolBlockId": pool.block.to_string(),
                    "poolTxId": pool.tx.to_string(),
                    "soldTokenBlockId": sold_id.block.to_string(),
                    "soldTokenTxId": sold_id.tx.to_string(),
                    "boughtTokenBlockId": bought_id.block.to_string(),
                    "boughtTokenTxId": bought_id.tx.to_string(),
                    "soldAmount": sold_amt.to_string(),
                    "boughtAmount": bought_amt.to_string(),
                    "sellerAddress": address,
                    "address": address,
                    "timestamp": iso_timestamp(item.activity.timestamp),
                    "type": "swap",
                });
                out.push(HistoryItemWithTs {
                    ts: item.activity.timestamp,
                    seq: item.entry.seq,
                    item: value,
                });
            }
            ActivityKind::LiquidityAdd => {
                let lp_supply = if let Some(supply) = lp_supply_cache.get(&pool) {
                    *supply
                } else {
                    let supply = state
                        .ammdata
                        .get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
                            blockhash: blockhash.clone(),
                            pool,
                        })?
                        .supply;
                    lp_supply_cache.insert(pool, supply);
                    supply
                };
                let token0_amt = abs_i128(item.activity.base_delta);
                let token1_amt = abs_i128(item.activity.quote_delta);
                let address = address_from_spk_bytes(&item.activity.address_spk);
                let value = json!({
                    "transactionId": txid_hex(item.activity.txid),
                    "poolBlockId": pool.block.to_string(),
                    "poolTxId": pool.tx.to_string(),
                    "token0BlockId": defs.base_alkane_id.block.to_string(),
                    "token0TxId": defs.base_alkane_id.tx.to_string(),
                    "token1BlockId": defs.quote_alkane_id.block.to_string(),
                    "token1TxId": defs.quote_alkane_id.tx.to_string(),
                    "token0Amount": token0_amt.to_string(),
                    "token1Amount": token1_amt.to_string(),
                    "lpTokenAmount": lp_supply.to_string(),
                    "minterAddress": address,
                    "address": address,
                    "timestamp": iso_timestamp(item.activity.timestamp),
                    "type": "mint",
                });
                out.push(HistoryItemWithTs {
                    ts: item.activity.timestamp,
                    seq: item.entry.seq,
                    item: value,
                });
            }
            ActivityKind::LiquidityRemove => {
                let lp_supply = if let Some(supply) = lp_supply_cache.get(&pool) {
                    *supply
                } else {
                    let supply = state
                        .ammdata
                        .get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
                            blockhash: blockhash.clone(),
                            pool,
                        })?
                        .supply;
                    lp_supply_cache.insert(pool, supply);
                    supply
                };
                let token0_amt = abs_i128(item.activity.base_delta);
                let token1_amt = abs_i128(item.activity.quote_delta);
                let address = address_from_spk_bytes(&item.activity.address_spk);
                let value = json!({
                    "transactionId": txid_hex(item.activity.txid),
                    "poolBlockId": pool.block.to_string(),
                    "poolTxId": pool.tx.to_string(),
                    "token0BlockId": defs.base_alkane_id.block.to_string(),
                    "token0TxId": defs.base_alkane_id.tx.to_string(),
                    "token1BlockId": defs.quote_alkane_id.block.to_string(),
                    "token1TxId": defs.quote_alkane_id.tx.to_string(),
                    "token0Amount": token0_amt.to_string(),
                    "token1Amount": token1_amt.to_string(),
                    "lpTokenAmount": lp_supply.to_string(),
                    "burnerAddress": address,
                    "address": address,
                    "timestamp": iso_timestamp(item.activity.timestamp),
                    "type": "burn",
                });
                out.push(HistoryItemWithTs {
                    ts: item.activity.timestamp,
                    seq: item.entry.seq,
                    item: value,
                });
            }
            ActivityKind::PoolCreate => {
                let creation = if let Some(info) = creation_cache.get(&pool) {
                    info.clone()
                } else {
                    let info = state
                        .ammdata
                        .get_pool_creation_info(GetPoolCreationInfoParams {
                            blockhash: blockhash.clone(),
                            pool,
                        })?
                        .info;
                    creation_cache.insert(pool, info.clone());
                    info
                };
                let (token0_amt, token1_amt, token_supply, creator_spk) =
                    if let Some(info) = creation {
                        (
                            info.initial_token0_amount,
                            info.initial_token1_amount,
                            info.initial_lp_supply,
                            info.creator_spk,
                        )
                    } else {
                        (0, 0, 0, Vec::new())
                    };
                let creator = if creator_spk.is_empty() {
                    address_from_spk_bytes(&item.activity.address_spk)
                } else {
                    address_from_spk_bytes(&creator_spk)
                };
                let value = json!({
                    "transactionId": txid_hex(item.activity.txid),
                    "poolBlockId": pool.block.to_string(),
                    "poolTxId": pool.tx.to_string(),
                    "token0BlockId": defs.base_alkane_id.block.to_string(),
                    "token0TxId": defs.base_alkane_id.tx.to_string(),
                    "token1BlockId": defs.quote_alkane_id.block.to_string(),
                    "token1TxId": defs.quote_alkane_id.tx.to_string(),
                    "token0Amount": token0_amt.to_string(),
                    "token1Amount": token1_amt.to_string(),
                    "tokenSupply": token_supply.to_string(),
                    "creatorAddress": creator,
                    "address": creator,
                    "timestamp": iso_timestamp(item.activity.timestamp),
                    "type": "creation",
                });
                out.push(HistoryItemWithTs {
                    ts: item.activity.timestamp,
                    seq: item.entry.seq,
                    item: value,
                });
            }
        }
    }

    Ok(out)
}

fn wrap_event_to_value(entry: &SchemaWrapEventV1, kind: &str) -> Value {
    let address = address_from_spk_bytes(&entry.address_spk);
    json!({
        "transactionId": txid_hex(entry.txid),
        "address": address,
        "amount": entry.amount.to_string(),
        "timestamp": iso_timestamp(entry.timestamp),
        "type": kind,
    })
}

fn wrap_event_to_history_item(entry: &SchemaWrapEventV1, kind: &str) -> HistoryItemWithTs {
    HistoryItemWithTs { ts: entry.timestamp, seq: 0, item: wrap_event_to_value(entry, kind) }
}

fn build_token_pair(
    blockhash: StateAt,
    state: &OylApiState,
    pool: SchemaAlkaneId,
    meta_cache: &mut HashMap<SchemaAlkaneId, TokenMeta>,
) -> Result<Option<TokenPairComputed>> {
    let defs = state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })?;
    let Some(defs) = defs.defs else {
        return Ok(None);
    };

    let mut balances = get_alkane_balances(blockhash.clone(), &state.essentials, &pool)?;
    let reserve0 = balances.remove(&defs.base_alkane_id).unwrap_or(0);
    let reserve1 = balances.remove(&defs.quote_alkane_id).unwrap_or(0);

    let metrics = state
        .ammdata
        .get_pool_metrics(GetPoolMetricsParams { blockhash: blockhash.clone(), pool })?
        .metrics;

    let token0_meta = get_token_meta(blockhash.clone(), state, meta_cache, &defs.base_alkane_id)?;
    let token1_meta = get_token_meta(blockhash.clone(), state, meta_cache, &defs.quote_alkane_id)?;
    let pool_name_raw = format!("{} / {}", token0_meta.label.as_str(), token1_meta.label.as_str());
    let pool_name_norm = normalize_query(&pool_name_raw);
    let pool_name = pool_name_display(&pool_name_raw);

    let value = json!({
        "poolId": alkane_id_json(&pool),
        "poolVolume1dInUsd": scale_price_u128(metrics.pool_volume_1d_usd),
        "poolTvlInUsd": scale_price_u128(metrics.pool_tvl_usd),
        "poolName": pool_name,
        "reserve0": reserve0.to_string(),
        "reserve1": reserve1.to_string(),
        "token0": {
            "symbol": token0_meta.symbol.clone(),
            "alkaneId": alkane_id_json(&defs.base_alkane_id),
            "name": token0_meta.name.clone(),
            "decimals": token0_meta.decimals,
            "image": token0_meta.image.clone(),
            "token0Amount": reserve0.to_string(),
        },
        "token1": {
            "symbol": token1_meta.symbol.clone(),
            "alkaneId": alkane_id_json(&defs.quote_alkane_id),
            "name": token1_meta.name.clone(),
            "decimals": token1_meta.decimals,
            "image": token1_meta.image.clone(),
            "token1Amount": reserve1.to_string(),
        }
    });

    let search = format!(
        "{} {} {} {} {}",
        pool_name_norm,
        normalize_query(&token0_meta.symbol),
        normalize_query(&token0_meta.name),
        normalize_query(&token1_meta.symbol),
        normalize_query(&token1_meta.name),
    );

    Ok(Some(TokenPairComputed {
        value,
        pool_id: pool,
        token0_id: defs.base_alkane_id,
        token1_id: defs.quote_alkane_id,
        search,
        tvl_usd: metrics.pool_tvl_usd,
    }))
}

fn get_token_meta(
    blockhash: StateAt,
    state: &OylApiState,
    cache: &mut HashMap<SchemaAlkaneId, TokenMeta>,
    id: &SchemaAlkaneId,
) -> Result<TokenMeta> {
    crate::debug_timer_log!("get_token_meta");
    if let Some(meta) = cache.get(id) {
        return Ok(meta.clone());
    }

    let rec = state
        .essentials
        .get_creation_record(GetCreationRecordParams { blockhash: blockhash.clone(), alkane: *id })?
        .record;
    let name = rec.as_ref().and_then(|r| r.names.first().cloned()).unwrap_or_default();
    let symbol_raw = rec.as_ref().and_then(|r| r.symbols.first().cloned()).unwrap_or_default();
    let label = if !symbol_raw.is_empty() {
        symbol_raw.clone()
    } else if !name.is_empty() {
        name.clone()
    } else {
        format!("{}:{}", id.block, id.tx)
    };
    let display_symbol =
        if symbol_raw.is_empty() { label.clone() } else { symbol_raw }.to_ascii_uppercase();
    let meta = TokenMeta {
        name,
        symbol: display_symbol,
        label,
        image: format!("{}/{}-{}.png", state.config.alkane_icon_cdn, id.block, id.tx),
        decimals: 8,
    };
    cache.insert(*id, meta.clone());
    Ok(meta)
}

fn parse_amm_history_key(prefix: &[u8], key: &[u8]) -> Option<AmmHistoryEntry> {
    if !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    if rest.len() < 25 {
        return None;
    }
    let mut ts_arr = [0u8; 8];
    ts_arr.copy_from_slice(&rest[0..8]);
    let mut seq_arr = [0u8; 4];
    seq_arr.copy_from_slice(&rest[8..12]);
    let kind = activity_kind_from_code(rest[12])?;
    let pool = decode_alkane_id_be(&rest[13..25])?;
    Some(AmmHistoryEntry {
        ts: u64::from_be_bytes(ts_arr),
        seq: u32::from_be_bytes(seq_arr),
        pool,
        kind,
    })
}

fn activity_kind_from_code(code: u8) -> Option<ActivityKind> {
    match code {
        0 => Some(ActivityKind::TradeBuy),
        1 => Some(ActivityKind::TradeSell),
        2 => Some(ActivityKind::LiquidityAdd),
        3 => Some(ActivityKind::LiquidityRemove),
        4 => Some(ActivityKind::PoolCreate),
        _ => None,
    }
}

fn decode_alkane_id_be(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    if bytes.len() != 12 {
        return None;
    }
    let mut block_arr = [0u8; 4];
    block_arr.copy_from_slice(&bytes[..4]);
    let mut tx_arr = [0u8; 8];
    tx_arr.copy_from_slice(&bytes[4..12]);
    Some(SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) })
}

fn fetch_records_for_query(
    blockhash: StateAt,
    state: &OylApiState,
    query: Option<&str>,
) -> Result<Vec<crate::modules::essentials::utils::inspections::AlkaneCreationRecord>> {
    let mut ids: HashSet<SchemaAlkaneId> = HashSet::new();

    if let Some(q) = query {
        let q = q.trim();
        if q.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(id) = parse_alkane_id_str(q) {
            ids.insert(id);
        } else {
            let norm = normalize_query(q);
            let by_name = state
                .essentials
                .get_alkane_ids_by_name_prefix(GetAlkaneIdsByNamePrefixParams {
                    blockhash: blockhash.clone(),
                    prefix: norm.clone(),
                })?
                .ids;
            let by_symbol = state
                .essentials
                .get_alkane_ids_by_symbol_prefix(GetAlkaneIdsBySymbolPrefixParams {
                    blockhash: blockhash.clone(),
                    prefix: norm,
                })?
                .ids;
            for id in by_name.into_iter().chain(by_symbol.into_iter()) {
                ids.insert(id);
            }
        }
    } else {
        return Ok(state
            .essentials
            .get_creation_records_ordered(GetCreationRecordsOrderedParams {
                blockhash: blockhash.clone(),
            })?
            .records);
    }

    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let recs = state
        .essentials
        .get_creation_records_by_id(GetCreationRecordsByIdParams {
            blockhash: blockhash.clone(),
            alkanes: ids.iter().copied().collect(),
        })?
        .records;
    Ok(recs.into_iter().flatten().collect())
}

fn parse_search_index_field(key: &str) -> SearchIndexField {
    match key {
        "marketcap" => SearchIndexField::Marketcap,
        "holders" => SearchIndexField::Holders,
        "volume7d" => SearchIndexField::Volume7d,
        "change7d" => SearchIndexField::Change7d,
        "volumealltime" => SearchIndexField::VolumeAllTime,
        _ => SearchIndexField::Holders,
    }
}

fn search_index_field_for_sort(sort_by: &str) -> (SearchIndexField, Option<SchemaAlkaneId>) {
    let raw = sort_by.trim();
    let key = raw.to_ascii_lowercase();
    if let Some(idx) = key.find("-derived_") {
        let field_part = &key[..idx];
        let quote_part = &raw[idx + "-derived_".len()..];
        let field = parse_search_index_field(field_part);
        let quote = parse_alkane_id_str(quote_part);
        return (field, quote);
    }
    (parse_search_index_field(key.as_str()), None)
}

fn record_matches_query(
    rec: &crate::modules::essentials::utils::inspections::AlkaneCreationRecord,
    query_norm: &str,
) -> bool {
    for name in rec.names.iter().chain(rec.symbols.iter()) {
        if let Some(norm) = normalize_search_text(name) {
            if norm.starts_with(query_norm) {
                return true;
            }
        }
    }
    false
}

fn search_by_holders_scan(
    blockhash: StateAt,
    state: &OylApiState,
    query_norm: &str,
    offset: u64,
    limit: u64,
    scan_cap: u64,
) -> Result<Vec<SchemaAlkaneId>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let scan_limit = if scan_cap == 0 { 5000 } else { scan_cap };
    let ids = state
        .essentials
        .get_holders_ordered_page(GetHoldersOrderedPageParams {
            blockhash: blockhash.clone(),
            offset: 0,
            limit: scan_limit,
            desc: true,
        })?
        .ids;
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let records = state
        .essentials
        .get_creation_records_by_id(GetCreationRecordsByIdParams {
            blockhash: blockhash.clone(),
            alkanes: ids.clone(),
        })?
        .records;

    let mut out: Vec<SchemaAlkaneId> = Vec::new();
    let mut seen = HashSet::new();
    let mut matched: u64 = 0;

    for (idx, rec_opt) in records.into_iter().enumerate() {
        let Some(rec) = rec_opt else { continue };
        if !record_matches_query(&rec, query_norm) {
            continue;
        }
        let id = ids[idx];
        if !seen.insert(id) {
            continue;
        }
        if matched < offset {
            matched += 1;
            continue;
        }
        out.push(id);
        matched += 1;
        if out.len() >= limit as usize {
            break;
        }
    }
    Ok(out)
}

fn filter_ids_with_derived_metrics(
    blockhash: StateAt,
    state: &OylApiState,
    ids: Vec<SchemaAlkaneId>,
    quote: SchemaAlkaneId,
) -> Result<Vec<SchemaAlkaneId>> {
    if ids.is_empty() {
        return Ok(ids);
    }
    let metrics =
        state
            .ammdata
            .get_token_derived_metrics_by_id(GetTokenDerivedMetricsByIdParams {
                blockhash: blockhash.clone(),
                tokens: ids.clone(),
                quote,
            })?;
    let metrics = metrics.metrics;
    if metrics.len() != ids.len() {
        return Ok(Vec::new());
    }
    Ok(ids
        .into_iter()
        .zip(metrics.into_iter())
        .filter_map(|(id, metric)| if metric.is_some() { Some(id) } else { None })
        .collect())
}

#[derive(Clone, Copy)]
enum AlkaneSortIndex {
    Holders,
    Metrics(TokenMetricsIndexField),
    Derived { quote: SchemaAlkaneId, field: TokenMetricsIndexField },
}

fn parse_metrics_field(key: &str) -> Option<TokenMetricsIndexField> {
    match key {
        "price" => Some(TokenMetricsIndexField::PriceUsd),
        "marketcap" => Some(TokenMetricsIndexField::MarketcapUsd),
        "volume1d" => Some(TokenMetricsIndexField::Volume1d),
        "volume7d" => Some(TokenMetricsIndexField::Volume7d),
        "volume30d" => Some(TokenMetricsIndexField::Volume30d),
        "volumealltime" => Some(TokenMetricsIndexField::VolumeAllTime),
        "change1d" => Some(TokenMetricsIndexField::Change1d),
        "change7d" => Some(TokenMetricsIndexField::Change7d),
        "change30d" => Some(TokenMetricsIndexField::Change30d),
        "changealltime" => Some(TokenMetricsIndexField::ChangeAllTime),
        _ => None,
    }
}

fn alkane_sort_index(sort_by: &str) -> AlkaneSortIndex {
    let raw = sort_by.trim();
    let key = raw.to_ascii_lowercase();
    if let Some(idx) = key.find("-derived_") {
        let field_part = &key[..idx];
        let quote_part = &raw[idx + "-derived_".len()..];
        if let (Some(field), Some(quote)) =
            (parse_metrics_field(field_part), parse_alkane_id_str(quote_part))
        {
            return AlkaneSortIndex::Derived { quote, field };
        }
    }
    match key.as_str() {
        "holders" => AlkaneSortIndex::Holders,
        _ => parse_metrics_field(key.as_str())
            .map(AlkaneSortIndex::Metrics)
            .unwrap_or(AlkaneSortIndex::Metrics(TokenMetricsIndexField::VolumeAllTime)),
    }
}

fn token_metrics_multi(
    blockhash: StateAt,
    state: &OylApiState,
    ids: &[SchemaAlkaneId],
) -> Result<Vec<SchemaTokenMetricsV1>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let res = state.ammdata.get_token_metrics_by_id(GetTokenMetricsByIdParams {
        blockhash: blockhash.clone(),
        tokens: ids.to_vec(),
    })?;
    Ok(res.metrics.into_iter().map(|m| m.unwrap_or_default()).collect())
}

fn holders_counts_multi(
    blockhash: StateAt,
    state: &OylApiState,
    ids: &[SchemaAlkaneId],
) -> Result<Vec<u64>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let res = state.essentials.get_holders_counts_by_id(GetHoldersCountsByIdParams {
        blockhash: blockhash.clone(),
        alkanes: ids.to_vec(),
    })?;
    Ok(res.counts)
}

fn collect_missing_tokens(
    blockhash: StateAt,
    state: &OylApiState,
    offset: u64,
    limit: u64,
    desc: bool,
) -> Result<Vec<SchemaAlkaneId>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut out: Vec<SchemaAlkaneId> = Vec::new();
    let mut missing_seen: u64 = 0;
    let mut creation_offset: u64 = 0;
    let chunk = std::cmp::max(200usize, limit as usize);

    loop {
        let page = state
            .essentials
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: blockhash.clone(),
                offset: creation_offset,
                limit: chunk as u64,
                desc,
            })?
            .records;
        if page.is_empty() {
            break;
        }

        let ids: Vec<SchemaAlkaneId> = page.iter().map(|rec| rec.alkane).collect();
        let metrics = state
            .ammdata
            .get_token_metrics_by_id(GetTokenMetricsByIdParams {
                blockhash: blockhash.clone(),
                tokens: ids.clone(),
            })?
            .metrics;
        if metrics.len() != ids.len() {
            break;
        }

        for (idx, id) in ids.into_iter().enumerate() {
            let has_metrics = metrics.get(idx).and_then(|m| m.as_ref()).is_some();
            if has_metrics {
                continue;
            }
            if missing_seen < offset {
                missing_seen += 1;
                continue;
            }
            out.push(id);
            missing_seen += 1;
            if out.len() >= limit as usize {
                return Ok(out);
            }
        }

        creation_offset = creation_offset.saturating_add(page.len() as u64);
    }

    Ok(out)
}

fn fetch_sorted_alkane_ids_no_query(
    blockhash: StateAt,
    state: &OylApiState,
    sort_index: AlkaneSortIndex,
    desc: bool,
    offset: u64,
    limit: u64,
    total: u64,
) -> Result<Vec<SchemaAlkaneId>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    match sort_index {
        AlkaneSortIndex::Holders => {
            let ids = state
                .essentials
                .get_holders_ordered_page(GetHoldersOrderedPageParams {
                    blockhash: blockhash.clone(),
                    offset,
                    limit,
                    desc,
                })?
                .ids;
            Ok(ids)
        }
        AlkaneSortIndex::Derived { quote, field } => {
            let metrics_count = state
                .ammdata
                .get_token_derived_metrics_index_count(GetTokenDerivedMetricsIndexCountParams {
                    blockhash: blockhash.clone(),
                    quote,
                })?
                .count;
            if metrics_count == 0 || offset >= metrics_count {
                return Ok(Vec::new());
            }
            let take = std::cmp::min(limit, metrics_count.saturating_sub(offset));
            let ids = state
                .ammdata
                .get_token_derived_metrics_index_page(GetTokenDerivedMetricsIndexPageParams {
                    blockhash: blockhash.clone(),
                    quote,
                    field,
                    offset,
                    limit: take,
                    desc,
                })?
                .ids;
            Ok(ids)
        }
        AlkaneSortIndex::Metrics(field) => {
            let metrics_count = state
                .ammdata
                .get_token_metrics_index_count(GetTokenMetricsIndexCountParams {
                    blockhash: blockhash.clone(),
                })?
                .count;
            let missing_count = total.saturating_sub(metrics_count);

            let metrics_page = |off: u64, lim: u64| -> Result<Vec<SchemaAlkaneId>> {
                if lim == 0 {
                    return Ok(Vec::new());
                }
                let ids = state
                    .ammdata
                    .get_token_metrics_index_page(GetTokenMetricsIndexPageParams {
                        blockhash: blockhash.clone(),
                        field,
                        offset: off,
                        limit: lim,
                        desc,
                    })?
                    .ids;
                Ok(ids)
            };

            if metrics_count == 0 && total > 0 {
                let probe = metrics_page(offset, limit)?;
                if !probe.is_empty() {
                    return Ok(probe);
                }
                let fallback: Vec<SchemaAlkaneId> = state
                    .essentials
                    .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                        blockhash: blockhash.clone(),
                        offset,
                        limit,
                        desc,
                    })?
                    .records
                    .into_iter()
                    .map(|rec| rec.alkane)
                    .collect();
                if !fallback.is_empty() {
                    return Ok(fallback);
                }
            }

            let mut ids: Vec<SchemaAlkaneId> = Vec::new();

            if desc {
                if offset < metrics_count {
                    let take = std::cmp::min(limit, metrics_count - offset);
                    ids.extend(metrics_page(offset, take)?);
                    let remaining = limit.saturating_sub(ids.len() as u64);
                    if remaining > 0 && missing_count > 0 {
                        ids.extend(collect_missing_tokens(
                            blockhash.clone(),
                            state,
                            0,
                            remaining,
                            true,
                        )?);
                    }
                } else if missing_count > 0 {
                    let miss_off = offset.saturating_sub(metrics_count);
                    ids.extend(collect_missing_tokens(
                        blockhash.clone(),
                        state,
                        miss_off,
                        limit,
                        true,
                    )?);
                }
            } else {
                if missing_count > 0 && offset < missing_count {
                    let take = std::cmp::min(limit, missing_count - offset);
                    ids.extend(collect_missing_tokens(
                        blockhash.clone(),
                        state,
                        offset,
                        take,
                        false,
                    )?);
                    let remaining = limit.saturating_sub(ids.len() as u64);
                    if remaining > 0 {
                        ids.extend(metrics_page(0, remaining)?);
                    }
                } else {
                    let metrics_offset = offset.saturating_sub(missing_count);
                    ids.extend(metrics_page(metrics_offset, limit)?);
                }
            }

            Ok(ids)
        }
    }
}

#[allow(dead_code)]
fn sort_records(
    items: &mut Vec<(
        crate::modules::essentials::utils::inspections::AlkaneCreationRecord,
        SchemaTokenMetricsV1,
        u64,
    )>,
    sort_by: &str,
    order: &str,
) {
    let desc = order.eq_ignore_ascii_case("desc");
    let cmp_f64 = |a: f64, b: f64| {
        if desc {
            b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
        }
    };
    let cmp_u128 = |a: u128, b: u128| if desc { b.cmp(&a) } else { a.cmp(&b) };
    let cmp_u64 = |a: u64, b: u64| if desc { b.cmp(&a) } else { a.cmp(&b) };

    items.sort_by(|a, b| match sort_by {
        "holders" => cmp_u64(a.2, b.2),
        "price" => cmp_u128(a.1.price_usd, b.1.price_usd),
        "marketcap" => cmp_u128(a.1.marketcap_usd, b.1.marketcap_usd),
        "volume1d" => cmp_u128(a.1.volume_1d, b.1.volume_1d),
        "volume7d" => cmp_u128(a.1.volume_7d, b.1.volume_7d),
        "volume30d" => cmp_u128(a.1.volume_30d, b.1.volume_30d),
        "volumeAllTime" => cmp_u128(a.1.volume_all_time, b.1.volume_all_time),
        "change1d" => cmp_f64(
            a.1.change_1d.parse::<f64>().unwrap_or(0.0),
            b.1.change_1d.parse::<f64>().unwrap_or(0.0),
        ),
        "change7d" => cmp_f64(
            a.1.change_7d.parse::<f64>().unwrap_or(0.0),
            b.1.change_7d.parse::<f64>().unwrap_or(0.0),
        ),
        "change30d" => cmp_f64(
            a.1.change_30d.parse::<f64>().unwrap_or(0.0),
            b.1.change_30d.parse::<f64>().unwrap_or(0.0),
        ),
        "changeAllTime" => cmp_f64(
            a.1.change_all_time.parse::<f64>().unwrap_or(0.0),
            b.1.change_all_time.parse::<f64>().unwrap_or(0.0),
        ),
        _ => cmp_u128(a.1.volume_all_time, b.1.volume_all_time),
    });
}

fn build_alkane_token(
    blockhash: StateAt,
    state: &OylApiState,
    rec: crate::modules::essentials::utils::inspections::AlkaneCreationRecord,
    metrics: SchemaTokenMetricsV1,
    holders: u64,
    derived_quotes: &[SchemaAlkaneId],
) -> Result<Value> {
    let supply = latest_circulating_supply(blockhash.clone(), state, &rec.alkane)?;
    let minted = latest_total_minted(blockhash.clone(), state, &rec.alkane)?;
    let max_supply = rec.cap.saturating_mul(rec.mint_amount);
    let mint_active = max_supply > minted;
    let percentage_minted = if max_supply == 0 {
        0
    } else {
        let numerator = minted.saturating_mul(100);
        numerator.saturating_add(max_supply.saturating_sub(1)) / max_supply
    };

    let now_ts = now_ts();
    let (frbtc_price, busd_price) =
        canonical_pool_prices(blockhash.clone(), state, &rec.alkane, now_ts)?;
    let price_usd = latest_token_usd_close(blockhash.clone(), state, &rec.alkane)?.unwrap_or(0);

    let has_busd = busd_price > 0;
    let has_frbtc = frbtc_price > 0;
    let frbtc_fdv = if has_frbtc { frbtc_price.saturating_mul(supply) / PRICE_SCALE } else { 0 };
    let busd_fdv = if has_busd { metrics.fdv_usd } else { 0 };
    let frbtc_mcap = if has_frbtc { frbtc_price.saturating_mul(supply) / PRICE_SCALE } else { 0 };
    let busd_mcap = if has_busd { metrics.marketcap_usd } else { 0 };

    let id_str = format!("{}:{}", rec.alkane.block, rec.alkane.tx);
    let mut name = rec.names.first().cloned().unwrap_or_default();
    if name.is_empty() {
        name = id_str.clone();
    }
    let mut symbol = rec.symbols.first().cloned().unwrap_or_default();
    if symbol.is_empty() {
        symbol = id_str;
    }
    let symbol = symbol.to_ascii_uppercase();
    let change_1d_raw =
        if metrics.change_1d.trim().is_empty() { "0" } else { metrics.change_1d.as_str() };
    let change_7d_raw =
        if metrics.change_7d.trim().is_empty() { "0" } else { metrics.change_7d.as_str() };
    let change_30d_raw =
        if metrics.change_30d.trim().is_empty() { "0" } else { metrics.change_30d.as_str() };
    let change_all_time_raw = if metrics.change_all_time.trim().is_empty() {
        "0"
    } else {
        metrics.change_all_time.as_str()
    };
    let change_1d = parse_change_f64(change_1d_raw);
    let change_7d = parse_change_f64(change_7d_raw);
    let change_30d = parse_change_f64(change_30d_raw);
    let change_all_time = parse_change_f64(change_all_time_raw);
    let percentage_minted_num = percentage_minted as u64;
    let token_volume_1d = scale_price_u128(metrics.volume_1d);
    let token_volume_30d = scale_price_u128(metrics.volume_30d);
    let token_volume_7d = scale_price_u128(metrics.volume_7d);
    let token_volume_all_time = scale_price_u128(metrics.volume_all_time);

    let mut derived_data = serde_json::Map::new();
    for quote in derived_quotes.iter() {
        let derived_metrics = state
            .ammdata
            .get_token_derived_metrics(GetTokenDerivedMetricsParams {
                blockhash: blockhash.clone(),
                token: rec.alkane,
                quote: *quote,
            })?
            .metrics;
        let Some(dm) = derived_metrics else { continue };

        let suffix = format!("derived_{}:{}", quote.block, quote.tx);
        let price_usd = dm.price_usd;
        let token_volume_1d = scale_price_u128(dm.volume_1d);
        let token_volume_30d = scale_price_u128(dm.volume_30d);
        let token_volume_7d = scale_price_u128(dm.volume_7d);
        let token_volume_all_time = scale_price_u128(dm.volume_all_time);

        let change_1d_raw =
            if dm.change_1d.trim().is_empty() { "0" } else { dm.change_1d.as_str() };
        let change_7d_raw =
            if dm.change_7d.trim().is_empty() { "0" } else { dm.change_7d.as_str() };
        let change_30d_raw =
            if dm.change_30d.trim().is_empty() { "0" } else { dm.change_30d.as_str() };
        let change_all_time_raw =
            if dm.change_all_time.trim().is_empty() { "0" } else { dm.change_all_time.as_str() };

        derived_data.insert(format!("priceUsd-{}", suffix), json!(scale_price_u128(price_usd)));
        derived_data.insert(format!("fdvUsd-{}", suffix), json!(scale_price_u128(dm.fdv_usd)));
        derived_data
            .insert(format!("marketcap-{}", suffix), json!(scale_price_u128(dm.marketcap_usd)));
        derived_data.insert(
            format!("tokenPoolsVolume1dInUsd-{}", suffix),
            json!(scale_price_u128(dm.volume_1d)),
        );
        derived_data.insert(
            format!("tokenPoolsVolume7dInUsd-{}", suffix),
            json!(scale_price_u128(dm.volume_7d)),
        );
        derived_data.insert(
            format!("tokenPoolsVolume30dInUsd-{}", suffix),
            json!(scale_price_u128(dm.volume_30d)),
        );
        derived_data.insert(
            format!("tokenPoolsVolumeAllTimeInUsd-{}", suffix),
            json!(scale_price_u128(dm.volume_all_time)),
        );
        derived_data.insert(format!("tokenVolume1d-{}", suffix), json!(token_volume_1d));
        derived_data.insert(format!("tokenVolume7d-{}", suffix), json!(token_volume_7d));
        derived_data.insert(format!("tokenVolume30d-{}", suffix), json!(token_volume_30d));
        derived_data.insert(format!("tokenVolumeAllTime-{}", suffix), json!(token_volume_all_time));
        derived_data
            .insert(format!("priceChange24h-{}", suffix), json!(parse_change_f64(change_1d_raw)));
        derived_data
            .insert(format!("priceChange7d-{}", suffix), json!(parse_change_f64(change_7d_raw)));
        derived_data
            .insert(format!("priceChange30d-{}", suffix), json!(parse_change_f64(change_30d_raw)));
        derived_data.insert(
            format!("priceChangeAllTime-{}", suffix),
            json!(parse_change_f64(change_all_time_raw)),
        );
    }

    Ok(json!({
        "id": alkane_id_json(&rec.alkane),
        "alkaneId": alkane_id_json(&rec.alkane),
        "name": name,
        "symbol": symbol,
        "totalSupply": supply.to_string(),
        "cap": rec.cap.to_string(),
        "minted": minted.to_string(),
        "mintActive": mint_active,
        "percentageMinted": percentage_minted_num,
        "mintAmount": rec.mint_amount.to_string(),
        "image": format!("{}/{}-{}.png", state.config.alkane_icon_cdn, rec.alkane.block, rec.alkane.tx),
        "frbtcPoolPriceInSats": frbtc_price.to_string(),
        "busdPoolPriceInUsd": scale_price_u128(busd_price),
        "maxSupply": max_supply.to_string(),
        "floorPrice": 0.0,
        "fdv": scale_price_u128(metrics.fdv_usd),
        "holders": holders,
        "marketcap": scale_price_u128(metrics.marketcap_usd),
        "idClubMarketplace": false,
        "busdPoolFdvInUsd": scale_price_u128(busd_fdv),
        "frbtcPoolFdvInSats": frbtc_fdv.to_string(),
        "priceUsd": scale_price_u128(price_usd),
        "fdvUsd": scale_price_u128(metrics.fdv_usd),
        "busdPoolMarketcapInUsd": scale_price_u128(busd_mcap),
        "frbtcPoolMarketcapInSats": frbtc_mcap.to_string(),
        "tokenPoolsVolume1dInUsd": scale_price_u128(metrics.volume_1d),
        "tokenPoolsVolume30dInUsd": scale_price_u128(metrics.volume_30d),
        "tokenPoolsVolume7dInUsd": scale_price_u128(metrics.volume_7d),
        "tokenPoolsVolumeAllTimeInUsd": scale_price_u128(metrics.volume_all_time),
        "tokenVolume1d": token_volume_1d,
        "tokenVolume30d": token_volume_30d,
        "tokenVolume7d": token_volume_7d,
        "tokenVolumeAllTime": token_volume_all_time,
        "priceChange24h": change_1d,
        "priceChange7d": change_7d,
        "priceChange30d": change_30d,
        "priceChangeAllTime": change_all_time,
        "derived_data": derived_data,
    }))
}

#[derive(Clone)]
struct PoolDetailsComputed {
    value: Value,
    token0_tvl_usd: u128,
    token1_tvl_usd: u128,
    token0_tvl_sats: u128,
    token1_tvl_sats: u128,
    pool_tvl_usd: u128,
    pool_volume_1d_usd: u128,
    pool_volume_30d_usd: u128,
    pool_apr: f64,
    tvl_change_24h: f64,
    lp_supply: u128,
}

#[derive(Default)]
struct PoolDetailsCache {
    labels: HashMap<SchemaAlkaneId, String>,
    token_metrics: HashMap<SchemaAlkaneId, SchemaTokenMetricsV1>,
    canonical_prices: HashMap<SchemaAlkaneId, (u128, u128)>,
}

struct PoolDetailsContext {
    cache: PoolDetailsCache,
    canonical_units: HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    now_ts: u64,
    skip_factory_check: bool,
}

impl PoolDetailsContext {
    fn new(skip_factory_check: bool) -> Self {
        let mut canonical_units: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for quote in canonical_quotes(get_network()) {
            canonical_units.insert(quote.id, quote.unit);
        }
        Self {
            cache: PoolDetailsCache::default(),
            canonical_units,
            now_ts: now_ts(),
            skip_factory_check,
        }
    }
}

fn alkane_label_cached(
    blockhash: StateAt,
    state: &OylApiState,
    id: &SchemaAlkaneId,
    cache: &mut PoolDetailsCache,
) -> Result<String> {
    if let Some(label) = cache.labels.get(id) {
        return Ok(label.clone());
    }
    let label = alkane_label(blockhash.clone(), state, id)?;
    cache.labels.insert(*id, label.clone());
    Ok(label)
}

fn token_metrics_cached(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
    cache: &mut PoolDetailsCache,
) -> Result<SchemaTokenMetricsV1> {
    if let Some(metrics) = cache.token_metrics.get(token) {
        return Ok(metrics.clone());
    }
    let metrics = token_metrics(blockhash.clone(), state, token)?;
    cache.token_metrics.insert(*token, metrics.clone());
    Ok(metrics)
}

fn canonical_pool_prices_cached(
    blockhash: StateAt,
    state: &OylApiState,
    token: &SchemaAlkaneId,
    now_ts: u64,
    cache: &mut PoolDetailsCache,
) -> Result<(u128, u128)> {
    if let Some(prices) = cache.canonical_prices.get(token) {
        return Ok(*prices);
    }
    let prices = canonical_pool_prices(blockhash.clone(), state, token, now_ts)?;
    cache.canonical_prices.insert(*token, prices);
    Ok(prices)
}

fn build_pool_details(
    blockhash: StateAt,
    state: &OylApiState,
    factory: Option<SchemaAlkaneId>,
    pool: SchemaAlkaneId,
    mut ctx: Option<&mut PoolDetailsContext>,
) -> Result<Option<PoolDetailsComputed>> {
    let skip_factory_check = ctx.as_ref().map(|c| c.skip_factory_check).unwrap_or(false);
    if !skip_factory_check {
        if let Some(factory_id) = factory {
            let factory_match = state
                .ammdata
                .get_pool_factory(GetPoolFactoryParams { blockhash: blockhash.clone(), pool })?
                .factory;
            if let Some(found) = factory_match {
                if found != factory_id {
                    return Ok(None);
                }
            } else {
                let pools = state
                    .ammdata
                    .get_factory_pools(GetFactoryPoolsParams {
                        blockhash: blockhash.clone(),
                        factory: factory_id,
                    })?
                    .pools;
                if !pools.contains(&pool) {
                    return Ok(None);
                }
            }
        }
    }

    let defs = state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })?
        .defs;
    let Some(defs) = defs else {
        return Ok(None);
    };

    let token0 = defs.base_alkane_id;
    let token1 = defs.quote_alkane_id;

    let mut balances = get_alkane_balances(blockhash.clone(), &state.essentials, &pool)?;
    let token0_amount = balances.remove(&token0).unwrap_or(0);
    let token1_amount = balances.remove(&token1).unwrap_or(0);

    let token0_label = match ctx {
        Some(ref mut ctx) => {
            alkane_label_cached(blockhash.clone(), state, &token0, &mut ctx.cache)?
        }
        None => alkane_label(blockhash.clone(), state, &token0)?,
    };
    let token1_label = match ctx {
        Some(ref mut ctx) => {
            alkane_label_cached(blockhash.clone(), state, &token1, &mut ctx.cache)?
        }
        None => alkane_label(blockhash.clone(), state, &token1)?,
    };
    let pool_name = pool_name_display(&format!("{token0_label} / {token1_label}"));

    let mut metrics = state
        .ammdata
        .get_pool_metrics(GetPoolMetricsParams { blockhash: blockhash.clone(), pool })?
        .metrics;
    let pool_metrics_v2 = state
        .ammdata
        .get_pool_metrics_v2(GetPoolMetricsV2Params { blockhash: blockhash.clone(), pool })?
        .metrics;
    if metrics.tvl_change_24h.is_empty() {
        metrics.tvl_change_24h = "0".to_string();
    }
    if metrics.tvl_change_7d.is_empty() {
        metrics.tvl_change_7d = "0".to_string();
    }
    if metrics.pool_apr.is_empty() {
        metrics.pool_apr = "0".to_string();
    }

    let token0_metrics = match ctx {
        Some(ref mut ctx) => {
            token_metrics_cached(blockhash.clone(), state, &token0, &mut ctx.cache)?
        }
        None => token_metrics(blockhash.clone(), state, &token0)?,
    };
    let token1_metrics = match ctx {
        Some(ref mut ctx) => {
            token_metrics_cached(blockhash.clone(), state, &token1, &mut ctx.cache)?
        }
        None => token_metrics(blockhash.clone(), state, &token1)?,
    };
    let token0_price_usd = token0_metrics.price_usd;
    let token1_price_usd = token1_metrics.price_usd;
    let now_ts = ctx.as_ref().map(|c| c.now_ts).unwrap_or_else(now_ts);
    let (token0_price_sats, _) = match ctx {
        Some(ref mut ctx) => {
            canonical_pool_prices_cached(blockhash.clone(), state, &token0, now_ts, &mut ctx.cache)?
        }
        None => canonical_pool_prices(blockhash.clone(), state, &token0, now_ts)?,
    };
    let (token1_price_sats, _) = match ctx {
        Some(ref mut ctx) => {
            canonical_pool_prices_cached(blockhash.clone(), state, &token1, now_ts, &mut ctx.cache)?
        }
        None => canonical_pool_prices(blockhash.clone(), state, &token1, now_ts)?,
    };

    let mut token0_tvl_usd = token0_amount.saturating_mul(token0_price_usd) / AMOUNT_SCALE;
    let mut token1_tvl_usd = token1_amount.saturating_mul(token1_price_usd) / AMOUNT_SCALE;
    let mut token0_tvl_sats = token0_amount.saturating_mul(token0_price_sats) / AMOUNT_SCALE;
    let mut token1_tvl_sats = token1_amount.saturating_mul(token1_price_sats) / AMOUNT_SCALE;

    let local_units = if ctx.is_some() {
        None
    } else {
        let mut map: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for quote in canonical_quotes(get_network()) {
            map.insert(quote.id, quote.unit);
        }
        Some(map)
    };
    let canonical_units = if let Some(ctx) = ctx.as_ref() {
        &ctx.canonical_units
    } else {
        local_units.as_ref().unwrap()
    };
    if let Some(unit) = canonical_units.get(&token0) {
        token0_tvl_usd =
            canonical_quote_amount_tvl_usd(blockhash.clone(), state, token0_amount, *unit)?;
        token0_tvl_sats =
            canonical_quote_amount_tvl_sats(blockhash.clone(), state, token0_amount, *unit)?;
    }
    if let Some(unit) = canonical_units.get(&token1) {
        token1_tvl_usd =
            canonical_quote_amount_tvl_usd(blockhash.clone(), state, token1_amount, *unit)?;
        token1_tvl_sats =
            canonical_quote_amount_tvl_sats(blockhash.clone(), state, token1_amount, *unit)?;
    }

    let pool_tvl_usd = token0_tvl_usd.saturating_add(token1_tvl_usd);
    let pool_tvl_sats = token0_tvl_sats.saturating_add(token1_tvl_sats);

    let mut lp_supply = state
        .ammdata
        .get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams {
            blockhash: blockhash.clone(),
            pool,
        })?
        .supply;
    if lp_supply == 0 {
        lp_supply = latest_circulating_supply(blockhash.clone(), state, &pool)?;
    }

    let lp_value_sats = if lp_supply == 0 { 0 } else { pool_tvl_sats.saturating_div(lp_supply) };
    let lp_value_usd = if lp_supply == 0 { 0 } else { pool_tvl_usd.saturating_div(lp_supply) };

    let creation = state
        .ammdata
        .get_pool_creation_info(GetPoolCreationInfoParams { blockhash: blockhash.clone(), pool })?
        .info;
    let (creator_address, creation_height, initial_token0_amount, initial_token1_amount) =
        if let Some(info) = creation {
            let creator = if info.creator_spk.is_empty() {
                None
            } else {
                let spk = ScriptBuf::from(info.creator_spk.clone());
                spk_to_address_str(&spk, get_network())
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

    let pool_apr = parse_change_f64(&metrics.pool_apr);
    let tvl_change_24h = parse_change_f64(&metrics.tvl_change_24h);
    let tvl_change_7d = parse_change_f64(&metrics.tvl_change_7d);
    let mut pool_volume_7d_usd =
        pool_metrics_v2.as_ref().map(|m| m.pool_volume_7d_usd).unwrap_or(0);
    let mut pool_volume_all_time_usd =
        pool_metrics_v2.as_ref().map(|m| m.pool_volume_all_time_usd).unwrap_or(0);
    if pool_volume_7d_usd == 0 || pool_volume_all_time_usd == 0 {
        if let Ok((token0_volume_7d, token1_volume_7d, token0_volume_all, token1_volume_all)) =
            pool_candle_volume_sums(blockhash.clone(), state, &pool, now_ts)
        {
            let fallback_7d = token0_volume_7d
                .saturating_mul(token0_price_usd)
                .saturating_div(PRICE_SCALE)
                .saturating_add(
                    token1_volume_7d.saturating_mul(token1_price_usd).saturating_div(PRICE_SCALE),
                );
            let fallback_all = token0_volume_all
                .saturating_mul(token0_price_usd)
                .saturating_div(PRICE_SCALE)
                .saturating_add(
                    token1_volume_all.saturating_mul(token1_price_usd).saturating_div(PRICE_SCALE),
                );
            if pool_volume_7d_usd == 0 {
                pool_volume_7d_usd = fallback_7d;
            }
            if pool_volume_all_time_usd == 0 {
                pool_volume_all_time_usd = fallback_all;
            }
        }
    }

    let value = json!({
        "token0": alkane_id_json(&token0),
        "token1": alkane_id_json(&token1),
        "token0Amount": token0_amount.to_string(),
        "token1Amount": token1_amount.to_string(),
        "tokenSupply": lp_supply.to_string(),
        "poolName": pool_name,
        "poolId": alkane_id_json(&pool),
        "token0TvlInSats": token0_tvl_sats.to_string(),
        "token0TvlInUsd": scale_price_u128(token0_tvl_usd),
        "token1TvlInSats": token1_tvl_sats.to_string(),
        "token1TvlInUsd": scale_price_u128(token1_tvl_usd),
        "poolVolume30dInSats": metrics.pool_volume_30d_sats.to_string(),
        "poolVolume1dInSats": metrics.pool_volume_1d_sats.to_string(),
        "poolVolume30dInUsd": scale_price_u128(metrics.pool_volume_30d_usd),
        "poolVolume1dInUsd": scale_price_u128(metrics.pool_volume_1d_usd),
        "token0Volume30d": metrics.token0_volume_30d.to_string(),
        "token1Volume30d": metrics.token1_volume_30d.to_string(),
        "token0Volume1d": metrics.token0_volume_1d.to_string(),
        "token1Volume1d": metrics.token1_volume_1d.to_string(),
        "lPTokenValueInSats": lp_value_sats.to_string(),
        "lPTokenValueInUsd": scale_price_u128(lp_value_usd),
        "poolTvlInSats": pool_tvl_sats.to_string(),
        "poolTvlInUsd": scale_price_u128(pool_tvl_usd),
        "tvlChange24h": tvl_change_24h,
        "tvlChange7d": tvl_change_7d,
        "totalSupply": lp_supply.to_string(),
        "poolApr": pool_apr,
        "initialToken0Amount": initial_token0_amount.to_string(),
        "initialToken1Amount": initial_token1_amount.to_string(),
        "creatorAddress": creator_address,
        "creationBlockHeight": creation_height,
        "tvl": scale_price_u128(pool_tvl_usd),
        "volume1d": scale_price_u128(metrics.pool_volume_1d_usd),
        "volume7d": scale_price_u128(pool_volume_7d_usd),
        "volume30d": scale_price_u128(metrics.pool_volume_30d_usd),
        "volumeAllTime": scale_price_u128(pool_volume_all_time_usd),
        "apr": pool_apr,
        "tvlChange": tvl_change_24h,
    });

    Ok(Some(PoolDetailsComputed {
        value,
        token0_tvl_usd,
        token1_tvl_usd,
        token0_tvl_sats,
        token1_tvl_sats,
        pool_tvl_usd,
        pool_volume_1d_usd: metrics.pool_volume_1d_usd,
        pool_volume_30d_usd: metrics.pool_volume_30d_usd,
        pool_apr,
        tvl_change_24h,
        lp_supply,
    }))
}

fn search_pools(blockhash: StateAt, state: &OylApiState, query: &str) -> Result<Vec<Value>> {
    let mut pool_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    if let Some(id) = parse_alkane_id_str(query) {
        pool_ids.insert(id);
    }
    let norm = normalize_query(query);
    let matches = state
        .ammdata
        .get_pool_ids_by_name_prefix(GetPoolIdsByNamePrefixParams {
            blockhash: blockhash.clone(),
            prefix: norm,
        })?
        .ids;
    for id in matches {
        pool_ids.insert(id);
    }

    if pool_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for pool in pool_ids {
        let defs = state
            .ammdata
            .get_pool_defs(GetPoolDefsParams { blockhash: blockhash.clone(), pool })?
            .defs;
        let Some(defs) = defs else { continue };
        let base = defs.base_alkane_id;
        let quote = defs.quote_alkane_id;
        let mut balances = get_alkane_balances(blockhash.clone(), &state.essentials, &pool)?;
        let base_reserve = balances.remove(&base).unwrap_or(0);
        let quote_reserve = balances.remove(&quote).unwrap_or(0);
        let base_label = alkane_label(blockhash.clone(), state, &base)?;
        let quote_label = alkane_label(blockhash.clone(), state, &quote)?;
        let pool_name = pool_name_display(&format!("{base_label} / {quote_label}"));
        out.push(json!({
            "poolId": alkane_id_json(&pool),
            "token0": alkane_id_json(&base),
            "token1": alkane_id_json(&quote),
            "token0Amount": base_reserve.to_string(),
            "token1Amount": quote_reserve.to_string(),
            "poolName": pool_name,
            "totalSupply": "0",
        }));
    }

    Ok(out)
}

fn alkane_label(blockhash: StateAt, state: &OylApiState, id: &SchemaAlkaneId) -> Result<String> {
    let rec = state
        .essentials
        .get_creation_record(crate::modules::essentials::storage::GetCreationRecordParams {
            blockhash: blockhash.clone(),
            alkane: *id,
        })?
        .record;
    Ok(rec
        .and_then(|r| r.symbols.first().cloned().or_else(|| r.names.first().cloned()))
        .unwrap_or_else(|| format!("{}:{}", id.block, id.tx)))
}

fn clamp_count(count: Option<u64>) -> usize {
    let count = count.unwrap_or(50);
    let count = count.clamp(1, 200);
    count as usize
}

fn clamp_offset(offset: Option<u64>) -> usize {
    offset.unwrap_or(0) as usize
}

fn abs_i128(v: i128) -> u128 {
    if v < 0 { (-v) as u128 } else { v as u128 }
}

fn txid_hex(txid: [u8; 32]) -> String {
    let mut bytes = txid;
    bytes.reverse();
    hex::encode(bytes)
}

fn iso_timestamp(ts: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp(ts as i64)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn address_from_spk_bytes(spk: &[u8]) -> String {
    if spk.is_empty() {
        return String::new();
    }
    let spk = ScriptBuf::from_bytes(spk.to_vec());
    spk_to_address_str(&spk, get_network()).unwrap_or_default()
}

fn pool_name_from_defs(
    blockhash: StateAt,
    state: &OylApiState,
    defs: &SchemaMarketDefs,
) -> Result<String> {
    let token0_label = alkane_label(blockhash.clone(), state, &defs.base_alkane_id)?;
    let token1_label = alkane_label(blockhash.clone(), state, &defs.quote_alkane_id)?;
    Ok(format!("{token0_label} / {token1_label}"))
}

fn pool_name_display(raw: &str) -> String {
    raw.to_ascii_uppercase()
}

fn trade_from_activity(
    defs: &SchemaMarketDefs,
    entry: &crate::modules::ammdata::schemas::SchemaActivityV1,
) -> Option<(SchemaAlkaneId, SchemaAlkaneId, u128, u128)> {
    match entry.kind {
        ActivityKind::TradeSell => {
            let sold = defs.base_alkane_id;
            let bought = defs.quote_alkane_id;
            Some((sold, bought, abs_i128(entry.base_delta), abs_i128(entry.quote_delta)))
        }
        ActivityKind::TradeBuy => {
            let sold = defs.quote_alkane_id;
            let bought = defs.base_alkane_id;
            Some((sold, bought, abs_i128(entry.quote_delta), abs_i128(entry.base_delta)))
        }
        _ => None,
    }
}
