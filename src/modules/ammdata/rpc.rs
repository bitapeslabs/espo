use crate::modules::ammdata::storage::{
    AmmDataProvider, RpcFindBestSwapPathParams, RpcGetActivityParams, RpcGetAmmFactoriesParams,
    RpcGetBestMevSwapParams, RpcGetBtcUsdPriceParams, RpcGetCandlesParams,
    RpcGetChartChangeBlockParams, RpcGetChartChangesBlockParams, RpcGetPoolsParams, RpcPingParams,
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
