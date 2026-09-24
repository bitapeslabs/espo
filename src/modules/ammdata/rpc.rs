use crate::modules::ammdata::storage::{
    AmmDataProvider, RpcFindBestSwapPathParams, RpcGetActivityParams, RpcGetAlkaneQuoteParams,
    RpcGetAlkanesQuoteParams, RpcGetAmmFactoriesParams, RpcGetBestMevSwapParams,
    RpcGetBtcUsdCandlesParams, RpcGetBtcUsdPriceParams, RpcGetCandlesParams,
    RpcGetChartChangeBlockParams, RpcGetChartChangesBlockParams, RpcGetPoolsParams,
    RpcGetPortfolioStatsParams, RpcGetTokenActivityParams, RpcGetTokenTotalVolumeParams,
    RpcGetTokenVolumeParams, RpcGetTotalVolumeAmmParams, RpcGetTvlCandlesParams, RpcPingParams,
};
use crate::modules::defs::RpcNsRegistrar;
use serde_json::{Value, json};
use std::sync::Arc;

#[allow(dead_code)]
pub fn register_rpc(reg: &RpcNsRegistrar, provider: Arc<AmmDataProvider>) {
    let mdb_ptr = Arc::clone(&provider);

    eprintln!("[RPC::AMMDATA] registering RPC handlers…");

    let reg_candles = reg.clone();
    let mdb_ptr_candles: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        let mdb_for_handler = Arc::clone(&mdb_ptr_candles);
        reg_candles
            .register("get_candles", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_for_handler);
                async move {
                    let params = RpcGetCandlesParams {
                        pool: payload.get("pool").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        timeframe: payload
                            .get("timeframe")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        size: payload.get("size").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        side: payload.get("side").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        now: payload.get("now").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_candles(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_btc_candles = reg.clone();
    let mdb_ptr_btc_candles = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_btc_candles
            .register("get_btc_usd_candles", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_ptr_btc_candles);
                async move {
                    let params = RpcGetBtcUsdCandlesParams {
                        timeframe: payload
                            .get("timeframe")
                            .and_then(|value| value.as_str())
                            .map(str::to_string),
                        limit: payload.get("limit").and_then(|value| value.as_u64()),
                        size: payload.get("size").and_then(|value| value.as_u64()),
                        page: payload.get("page").and_then(|value| value.as_u64()),
                        now: payload.get("now").and_then(|value| value.as_u64()),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|value| value.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(view) => view,
                        Err(error) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": error.to_string()
                            });
                        }
                    };
                    view.rpc_get_btc_usd_candles(params)
                        .map(|response| response.value)
                        .unwrap_or_else(|_| json!({ "ok": false, "error": "internal_error" }))
                }
            })
            .await;
    });

    let reg_tvl_candles = reg.clone();
    let mdb_ptr_tvl_candles = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_tvl_candles
            .register("get_tvl_candles", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_ptr_tvl_candles);
                async move {
                    let params = RpcGetTvlCandlesParams {
                        token: payload
                            .get("token")
                            .and_then(|value| value.as_str())
                            .map(str::to_string),
                        timeframe: payload
                            .get("timeframe")
                            .and_then(|value| value.as_str())
                            .map(str::to_string),
                        limit: payload.get("limit").and_then(|value| value.as_u64()),
                        size: payload.get("size").and_then(|value| value.as_u64()),
                        page: payload.get("page").and_then(|value| value.as_u64()),
                        now: payload.get("now").and_then(|value| value.as_u64()),
                        include_unanchored: payload
                            .get("include_unanchored")
                            .and_then(|value| value.as_bool()),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|value| value.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(view) => view,
                        Err(error) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": error.to_string()
                            });
                        }
                    };
                    view.rpc_get_tvl_candles(params)
                        .map(|response| response.value)
                        .unwrap_or_else(|_| json!({ "ok": false, "error": "internal_error" }))
                }
            })
            .await;
    });

    let reg_alkanes_quote = reg.clone();
    let mdb_alkanes_quote = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_alkanes_quote
            .register("get_alkanes_quote", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_alkanes_quote);
                async move {
                    let assets = payload.get("assets").and_then(|value| {
                        value.as_array().and_then(|items| {
                            items
                                .iter()
                                .map(|item| item.as_str().map(str::to_string))
                                .collect::<Option<Vec<_>>>()
                        })
                    });
                    let params = RpcGetAlkanesQuoteParams {
                        assets,
                        now: payload.get("now").and_then(|value| value.as_u64()),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|value| value.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(view) => view,
                        Err(error) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": error.to_string()
                            });
                        }
                    };
                    view.rpc_get_alkanes_quote(params)
                        .map(|response| response.value)
                        .unwrap_or_else(|_| json!({ "ok": false, "error": "internal_error" }))
                }
            })
            .await;
    });

    let reg_alkane_quote = reg.clone();
    let mdb_alkane_quote = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_alkane_quote
            .register("get_alkane_quote", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_alkane_quote);
                async move {
                    let params = RpcGetAlkaneQuoteParams {
                        asset: payload
                            .get("asset")
                            .or_else(|| payload.get("alkane"))
                            .and_then(|value| value.as_str())
                            .map(str::to_string),
                        now: payload.get("now").and_then(|value| value.as_u64()),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|value| value.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(view) => view,
                        Err(error) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": error.to_string()
                            });
                        }
                    };
                    view.rpc_get_alkane_quote(params)
                        .map(|response| response.value)
                        .unwrap_or_else(|_| json!({ "ok": false, "error": "internal_error" }))
                }
            })
            .await;
    });

    let reg_portfolio = reg.clone();
    let mdb_portfolio = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_portfolio
            .register("get_portfolio_stats", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_portfolio);
                async move {
                    let params = RpcGetPortfolioStatsParams {
                        address: payload
                            .get("address")
                            .and_then(|value| value.as_str())
                            .map(str::to_string),
                    };
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|value| value.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(view) => view,
                        Err(error) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": error.to_string()
                            });
                        }
                    };
                    view.rpc_get_portfolio_stats(params)
                        .map(|response| response.value)
                        .unwrap_or_else(|error| {
                            json!({
                                "ok": false,
                                "error": "internal_error",
                                "detail": error.to_string()
                            })
                        })
                }
            })
            .await;
    });

    let reg_token_volume = reg.clone();
    let mdb_token_volume: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_token_volume
            .register("get_token_volume", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_token_volume);
                async move {
                    let params = RpcGetTokenVolumeParams {
                        token: payload
                            .get("token")
                            .or_else(|| payload.get("alkane"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        timeframe: payload
                            .get("timeframe")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        size: payload.get("size").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        now: payload.get("now").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_token_volume(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_chart_change = reg.clone();
    let mdb_ptr_chart_change: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_chart_change
            .register("get_chart_change_block", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_ptr_chart_change);
                async move {
                    let params = RpcGetChartChangeBlockParams {
                        chart: payload.get("chart").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        height: payload.get("height").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_chart_change_block(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_chart_changes = reg.clone();
    let mdb_ptr_chart_changes: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_chart_changes
            .register("get_chart_changes_block", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_ptr_chart_changes);
                async move {
                    let params = RpcGetChartChangesBlockParams {
                        height: payload.get("height").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_chart_changes_block(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_activity = reg.clone();
    let mdb_ptr_activity: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_activity
            .register("get_activity", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_ptr_activity);
                async move {
                    let params = RpcGetActivityParams {
                        pool: payload.get("pool").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        side: payload.get("side").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        filter_side: payload
                            .get("filter_side")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        activity_type: payload
                            .get("activity_type")
                            .or_else(|| payload.get("type"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        sort: payload.get("sort").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        dir: payload
                            .get("dir")
                            .or_else(|| payload.get("direction"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_activity(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_token_activity = reg.clone();
    let mdb_ptr_token_activity: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_token_activity
            .register("get_token_activity", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_ptr_token_activity);
                async move {
                    let params = RpcGetTokenActivityParams {
                        token: payload
                            .get("token")
                            .or_else(|| payload.get("alkane"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        side: payload.get("side").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        activity_type: payload
                            .get("activity_type")
                            .or_else(|| payload.get("type"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        kind: payload.get("kind").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        sort: payload
                            .get("sort")
                            .or_else(|| payload.get("sort_by"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        dir: payload.get("dir").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_token_activity(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_pools = reg.clone();
    let mdb_for_pools = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_pools
            .register("get_pools", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_for_pools);
                async move {
                    let params = RpcGetPoolsParams {
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_pools(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_factories = reg.clone();
    let mdb_for_factories = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_factories
            .register("get_amm_factories", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_for_factories);
                async move {
                    let params = RpcGetAmmFactoriesParams {
                        page: payload.get("page").and_then(|v| v.as_u64()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_amm_factories(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_path = reg.clone();
    let mdb_for_swap_path: Arc<AmmDataProvider> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_path
            .register("find_best_swap_path", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_for_swap_path);
                async move {
                    let params = RpcFindBestSwapPathParams {
                        mode: payload.get("mode").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        token_in: payload
                            .get("token_in")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        token_out: payload
                            .get("token_out")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        fee_bps: payload.get("fee_bps").and_then(|v| v.as_u64()),
                        max_hops: payload.get("max_hops").and_then(|v| v.as_u64()),
                        amount_in: payload.get("amount_in").cloned(),
                        amount_out_min: payload.get("amount_out_min").cloned(),
                        amount_out: payload.get("amount_out").cloned(),
                        amount_in_max: payload.get("amount_in_max").cloned(),
                        available_in: payload.get("available_in").cloned(),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_find_best_swap_path(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_mev = reg.clone();
    let mdb_mev_swap_ptr = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_mev
            .register("get_best_mev_swap", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_mev_swap_ptr);
                async move {
                    let params = RpcGetBestMevSwapParams {
                        token: payload.get("token").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        fee_bps: payload.get("fee_bps").and_then(|v| v.as_u64()),
                        max_hops: payload.get("max_hops").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_best_mev_swap(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_btc = reg.clone();
    let mdb_btc = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_btc
            .register("get_btc_usd_price", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_btc);
                async move {
                    let params = RpcGetBtcUsdPriceParams {
                        height: payload.get("height").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler
                        .with_height(params.height, payload.get("height").is_some())
                    {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_btc_usd_price(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_total_volume = reg.clone();
    let mdb_total_volume = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_total_volume
            .register("get_total_volume_amm", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_total_volume);
                async move {
                    let params = RpcGetTotalVolumeAmmParams {
                        unit: payload.get("unit").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        range_min: payload.get("range_min").and_then(|v| v.as_u64()),
                        range_max: payload.get("range_max").and_then(|v| v.as_u64()),
                        from_height: payload.get("from_height").and_then(|v| v.as_u64()),
                        to_height: payload.get("to_height").and_then(|v| v.as_u64()),
                        start_height: payload.get("start_height").and_then(|v| v.as_u64()),
                        end_height: payload.get("end_height").and_then(|v| v.as_u64()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_total_volume_amm(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_token_total_volume = reg.clone();
    let mdb_token_total_volume = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_token_total_volume
            .register("get_token_total_volume", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_token_total_volume);
                async move {
                    let params = RpcGetTokenTotalVolumeParams {
                        token: payload
                            .get("token")
                            .or_else(|| payload.get("alkane"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        range_min: payload.get("range_min").and_then(|v| v.as_u64()),
                        range_max: payload.get("range_max").and_then(|v| v.as_u64()),
                        from_height: payload.get("from_height").and_then(|v| v.as_u64()),
                        to_height: payload.get("to_height").and_then(|v| v.as_u64()),
                        start_height: payload.get("start_height").and_then(|v| v.as_u64()),
                        end_height: payload.get("end_height").and_then(|v| v.as_u64()),
                        limit: payload.get("limit").and_then(|v| v.as_u64()),
                        page: payload.get("page").and_then(|v| v.as_u64()),
                    };
                    let view = match mdb_for_handler.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_get_token_total_volume(params)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| json!({"ok": false, "error": "internal_error"}))
                }
            })
            .await;
    });

    let reg_ping = reg.clone();
    let mdb_ping = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_ping
            .register("ping", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_ping);
                async move {
                    let view = match mdb.with_height(
                        payload.get("height").and_then(|v| v.as_u64()),
                        payload.get("height").is_some(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_height",
                                "detail": e.to_string()
                            });
                        }
                    };
                    view.rpc_ping(RpcPingParams)
                        .map(|resp| resp.value)
                        .unwrap_or_else(|_| Value::String("pong".to_string()))
                }
            })
            .await;
    });
}
