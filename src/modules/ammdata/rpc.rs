use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::ammdata::utils::candles::{PriceSide, read_candles_v1};
use crate::modules::ammdata::utils::live_reserves::fetch_all_pools;
use crate::modules::ammdata::utils::trades::{
    SortDir, TradePage, TradeSideFilter, TradeSortKey, read_trades_for_pool,
    read_trades_for_pool_sorted,
};
use crate::modules::defs::RpcNsRegistrar;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use std::sync::Arc;

use serde_json::map::Map;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::schemas::{SchemaPoolSnapshot, Timeframe};

// === pathfinder (still needed, but built from LIVE reserves now) ===
use crate::modules::ammdata::utils::pathfinder::{
    DEFAULT_FEE_BPS, plan_best_mev_swap, plan_exact_in_default_fee, plan_exact_out_default_fee,
    plan_implicit_default_fee, plan_swap_exact_tokens_for_tokens,
    plan_swap_exact_tokens_for_tokens_implicit, plan_swap_tokens_for_exact_tokens,
};

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Parse pool/token id string like "2:68441" (decimal) or "0x2:0x10b59" (hex).
fn parse_id_from_str(s: &str) -> Option<SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let parse_u32 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u32::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u32>().ok()
        }
    };
    let parse_u64 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u64::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(parts[0])?, tx: parse_u64(parts[1])? })
}

fn parse_timeframe(s: &str) -> Option<Timeframe> {
    match s {
        "10m" | "m10" => Some(Timeframe::M10),
        "1h" | "h1" => Some(Timeframe::H1),
        "1d" | "d1" => Some(Timeframe::D1),
        "1w" | "w1" => Some(Timeframe::W1),
        "1M" | "m1" => Some(Timeframe::M1),
        _ => None,
    }
}

fn parse_price_side(s: &str) -> Option<PriceSide> {
    match s.to_ascii_lowercase().as_str() {
        "base" | "b" => Some(PriceSide::Base),
        "quote" | "q" => Some(PriceSide::Quote),
        _ => None,
    }
}

fn parse_side_filter(v: Option<&Value>) -> TradeSideFilter {
    if let Some(Value::String(s)) = v {
        return match s.to_ascii_lowercase().as_str() {
            "buy" | "b" => TradeSideFilter::Buy,
            "sell" | "s" => TradeSideFilter::Sell,
            "all" | "a" | "" => TradeSideFilter::All,
            _ => TradeSideFilter::All,
        };
    }
    TradeSideFilter::All
}

#[inline]
fn scale_u128(x: u128) -> f64 {
    (x as f64) / (PRICE_SCALE as f64) // 8 decimals
}

/* ---------- sort parsing helpers (single token only) ---------- */

fn parse_sort_dir(v: Option<&Value>) -> SortDir {
    if let Some(Value::String(s)) = v {
        match s.to_ascii_lowercase().as_str() {
            "asc" | "ascending" => return SortDir::Asc,
            "desc" | "descending" => return SortDir::Desc,
            _ => {}
        }
    }
    SortDir::Desc
}

fn norm_token(s: &str) -> Option<&'static str> {
    match s.to_ascii_lowercase().as_str() {
        // timestamp
        "ts" | "time" | "timestamp" => Some("ts"),
        // generic amount (mapped to base/quote depending on `side`)
        "amt" | "amount" => Some("amount"),
        // side (buy/sell group, then ts)
        "side" | "s" => Some("side"),
        // explicit base/quote amounts
        "absb" | "amount_base" | "base_amount" => Some("absb"),
        "absq" | "amount_quote" | "quote_amount" => Some("absq"),
        _ => None,
    }
}

/// Map a single `sort` token + requested PriceSide to a concrete index label + key.
fn map_sort(side: PriceSide, token: Option<&str>) -> (TradeSortKey, &'static str) {
    if let Some(tok) = token.and_then(norm_token) {
        return match tok {
            "ts" => (TradeSortKey::Timestamp, "ts"),
            "amount" => match side {
                PriceSide::Base => (TradeSortKey::AmountBaseAbs, "absb"),
                PriceSide::Quote => (TradeSortKey::AmountQuoteAbs, "absq"),
            },
            "side" => match side {
                // side ⇒ group by side then ts so paging is stable
                PriceSide::Base => (TradeSortKey::SideBaseTs, "sb_ts"),
                PriceSide::Quote => (TradeSortKey::SideQuoteTs, "sq_ts"),
            },
            "absb" => (TradeSortKey::AmountBaseAbs, "absb"),
            "absq" => (TradeSortKey::AmountQuoteAbs, "absq"),
            _ => (TradeSortKey::Timestamp, "ts"),
        };
    }
    (TradeSortKey::Timestamp, "ts")
}

/* ---------- tiny helpers for this file ---------- */

fn parse_u128_arg(v: Option<&Value>) -> Option<u128> {
    match v {
        Some(Value::String(s)) => s.parse::<u128>().ok(),
        Some(Value::Number(n)) => n.as_u64().map(|x| x as u128), // accept small numeric for convenience
        _ => None,
    }
}

fn id_str(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

/* ================================================================================ */

#[allow(dead_code)]
pub fn register_rpc(reg: &RpcNsRegistrar, mdb: Mdb) {
    let mdb_ptr = Arc::new(mdb);

    eprintln!("[RPC_AMMDATA] registering RPC handlers…");

    /* -------------------- get_candles -------------------- */
    let reg_candles = reg.clone();

    let mdb_ptr_candles: Arc<Mdb> = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        let mdb_for_handler = Arc::clone(&mdb_ptr_candles);
        reg_candles
            .register("get_candles", move |_cx, payload| {
                let mdb = Arc::clone(&mdb_for_handler);
                async move {
                    let tf = payload
                        .get("timeframe")
                        .and_then(|v| v.as_str())
                        .and_then(parse_timeframe)
                        .unwrap_or(Timeframe::H1);

                    // legacy "size" alias
                    let legacy_size = payload
                        .get("size")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize);
                    let limit = payload
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize)
                        .or(legacy_size)
                        .unwrap_or(120);
                    let page = payload
                        .get("page")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize)
                        .unwrap_or(1);

                    let side = payload
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(parse_price_side)
                        .unwrap_or(PriceSide::Base);

                    let now = payload
                        .get("now")
                        .and_then(|v| v.as_u64())
                        .unwrap_or_else(now_ts);

                    let pool =
                        match payload.get("pool").and_then(|v| v.as_str()).and_then(parse_id_from_str)
                        {
                            Some(p) => p,
                            None => {
                                eprintln!("[RPC:get_candles] invalid pool, payload={payload:?}");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_pool",
                                    "hint": "pool should be a string like \"2:68441\""
                                });
                            }
                        };

                    let slice = read_candles_v1(&mdb, pool, tf, /*unused*/ limit, now, side);
                    match slice {
                        Ok(slice) => {
                            let total = slice.candles_newest_first.len();

                            let dur = tf.duration_secs();
                            let newest_ts = slice.newest_ts;

                            let offset = limit.saturating_mul(page.saturating_sub(1));
                            let end = (offset + limit).min(total);
                            let page_slice = if offset >= total {
                                &[][..]
                            } else {
                                &slice.candles_newest_first[offset..end]
                            };

                            let arr: Vec<Value> =
                                page_slice.iter().enumerate().map(|(i, c)| {
                                    let global_idx = offset + i;
                                    let ts = newest_ts.saturating_sub((global_idx as u64) * dur);
                                    json!({
                                        "ts":     ts,
                                        "open":   scale_u128(c.open),
                                        "high":   scale_u128(c.high),
                                        "low":    scale_u128(c.low),
                                        "close":  scale_u128(c.close),
                                        "volume": scale_u128(c.volume),
                                    })
                                }).collect();

                            json!({
                                "ok": true,
                                "pool": id_str(&pool),
                                "timeframe": tf.code(),
                                "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                "page": page,
                                "limit": limit,
                                "total": total,
                                "has_more": end < total,
                                "candles": arr
                            })
                        }
                        Err(e) => json!({ "ok": false, "error": format!("read_failed: {e}") }),
                    }
                }
            })
            .await;
    });

    /* -------------------- get_trades -------------------- */
    let reg_trades = reg.clone();
    let mdb_ptr_trades: Arc<Mdb> = Arc::clone(&mdb_ptr);

    tokio::spawn(async move {
        reg_trades
            .register("get_trades", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_ptr_trades);

                async move {
                    let limit = payload
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize)
                        .unwrap_or(50);
                    let page = payload
                        .get("page")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize)
                        .unwrap_or(1);

                    let side = payload
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(parse_price_side)
                        .unwrap_or(PriceSide::Base);

                    let filter_side = parse_side_filter(payload.get("filter_side"));

                    // parse single sort token + dir
                    let sort_token: Option<String> =
                        payload.get("sort").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let dir = parse_sort_dir(payload.get("dir").or_else(|| payload.get("direction")));
                    let (sort_key, sort_code) = map_sort(side, sort_token.as_deref());

                    let pool =
                        match payload.get("pool").and_then(|v| v.as_str()).and_then(parse_id_from_str)
                        {
                            Some(p) => p,
                            None => {
                                eprintln!("[RPC:get_trades] invalid pool, payload={payload:?}");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_pool",
                                    "hint": "pool should be a string like \"2:68441\""
                                });
                            }
                        };

                    if sort_token.is_some() || !matches!(filter_side, TradeSideFilter::All) {
                        match read_trades_for_pool_sorted(
                            &mdb_for_handler, pool, page, limit, side, sort_key, dir, filter_side,
                        ) {
                            Ok(TradePage { trades, total }) => {
                                json!({
                                    "ok": true,
                                    "pool": id_str(&pool),
                                    "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                    "filter_side": match filter_side {
                                        TradeSideFilter::All => "all",
                                        TradeSideFilter::Buy => "buy",
                                        TradeSideFilter::Sell => "sell"
                                    },
                                    "sort": sort_code,
                                    "dir": match dir { SortDir::Asc => "asc", SortDir::Desc => "desc" },
                                    "page": page,
                                    "limit": limit,
                                    "total": total,
                                    "has_more": page.saturating_mul(limit) < total,
                                    "trades": trades
                                })
                            }
                            Err(e) => json!({ "ok": false, "error": format!("read_failed: {e}") }),
                        }
                    } else {
                        match read_trades_for_pool(&mdb_for_handler, pool, page, limit, side) {
                            Ok(TradePage { trades, total }) => {
                                json!({
                                    "ok": true,
                                    "pool": id_str(&pool),
                                    "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                    "filter_side": "all",
                                    "sort": "ts",
                                    "dir": "desc",
                                    "page": page,
                                    "limit": limit,
                                    "total": total,
                                    "has_more": page.saturating_mul(limit) < total,
                                    "trades": trades
                                })
                            }
                            Err(e) => json!({ "ok": false, "error": format!("read_failed: {e}") }),
                        }
                    }
                }
            })
            .await;
    });

    /* -------------------- get_pools (LIVE ONLY, single call) -------------------- */
    let reg_pools = reg.clone();

    let mdb_for_pools = Arc::clone(&mdb_ptr);
    tokio::spawn(async move {
        reg_pools
            .register("get_pools", move |_cx, payload|  {
                let mdb_for_handler = Arc::clone(&mdb_for_pools);
                // Single live call that returns ALL pools + reserves
                async move{
                let live_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
                    match fetch_all_pools(&mdb_for_handler) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[RPC:get_pools] live reserves fetch failed: {e:?}");
                            return json!({
                                "ok": false,
                                "error": "live_fetch_failed",
                                "hint": "could not load live reserves"
                            });
                        }
                    };

                // Order deterministically by (block, tx) for stable pagination
                let mut rows: Vec<(SchemaAlkaneId, SchemaPoolSnapshot)> =
                    live_map.into_iter().collect();
                rows.sort_by(|(a, _), (b, _)| a.block.cmp(&b.block).then(a.tx.cmp(&b.tx)));

                let total = rows.len();

                // default page size: all (bounded)
                let limit = payload
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(total.max(1))
                    .clamp(1, 20_000);

                let page = payload
                    .get("page")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(1)
                    .max(1);

                let offset = limit.saturating_mul(page.saturating_sub(1));
                let end = (offset + limit).min(total);
                let window = if offset >= total {
                    &[][..]
                } else {
                    &rows[offset..end]
                };
                let has_more = end < total;

                let mut pools_obj: Map<String, Value> = Map::with_capacity(window.len());
                for (pool, snap) in window.iter() {
                    pools_obj.insert(
                        format!("{}:{}", pool.block, pool.tx),
                        json!({
                            "base":          format!("{}:{}", snap.base_id.block,  snap.base_id.tx),
                            "quote":         format!("{}:{}", snap.quote_id.block, snap.quote_id.tx),
                            "base_reserve":  snap.base_reserve.to_string(),
                            "quote_reserve": snap.quote_reserve.to_string(),
                            "source": "live",
                        }),
                    );
                }

                json!({
                    "ok": true,
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "has_more": has_more,
                    "pools": Value::Object(pools_obj)
                })
            }})
            .await;
    });

    /* -------------------- find_best_swap_path (uses LIVE reserves) -------------------- */
    let reg_path = reg.clone();
    let mdb_for_swap_path: Arc<Mdb> = Arc::clone(&mdb_ptr);

    tokio::spawn(async move {
        reg_path
            .register("find_best_swap_path", move |_cx, payload|   {
                // Load LIVE reserves once
                let mdb_for_handler = Arc::clone(&mdb_for_swap_path);
                async move {
                let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
                    match fetch_all_pools(&mdb_for_handler) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[RPC:find_best_swap_path] live fetch failed: {e:?}");
                            return json!({
                                "ok": false,
                                "error": "no_liquidity",
                                "hint": "live reserves unavailable"
                            });
                        }
                    };

                if snapshot_map.is_empty() {
                    return json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves map is empty"
                    });
                }

                // Parse core params
                let mode = payload
                    .get("mode")
                    .and_then(|v| v.as_str())
                    .unwrap_or("exact_in")
                    .to_ascii_lowercase();

                let token_in =
                    match payload.get("token_in").and_then(|v| v.as_str()).and_then(parse_id_from_str)
                    {
                        Some(t) => t,
                        None => return json!({"ok": false, "error": "missing_or_invalid_token_in"}),
                    };
                let token_out =
                    match payload.get("token_out").and_then(|v| v.as_str()).and_then(parse_id_from_str)
                    {
                        Some(t) => t,
                        None => return json!({"ok": false, "error": "missing_or_invalid_token_out"}),
                    };

                let fee_bps = payload
                    .get("fee_bps")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u32)
                    .unwrap_or(DEFAULT_FEE_BPS);
                let max_hops = payload
                    .get("max_hops")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(3)
                    .max(1)
                    .min(6);

                // Run planner by mode
                let plan = match mode.as_str() {
                    // amount_in (req), amount_out_min (optional → default 0)
                    "exact_in" => {
                        let amount_in = match parse_u128_arg(payload.get("amount_in")) {
                            Some(v) => v,
                            None => {
                                return json!({"ok": false, "error": "missing_or_invalid_amount_in"})
                            }
                        };
                        let min_out = parse_u128_arg(payload.get("amount_out_min")).unwrap_or(0u128);

                        if let Some(bps) = payload.get("fee_bps").and_then(|v| v.as_u64()) {
                            plan_swap_exact_tokens_for_tokens(
                                &snapshot_map,
                                token_in,
                                token_out,
                                amount_in,
                                min_out,
                                bps as u32,
                                max_hops,
                            )
                        } else {
                            plan_exact_in_default_fee(
                                &snapshot_map,
                                token_in,
                                token_out,
                                amount_in,
                                min_out,
                                max_hops,
                            )
                        }
                    }

                    // amount_out (req), amount_in_max (optional → u128::MAX)
                    "exact_out" => {
                        let amount_out = match parse_u128_arg(payload.get("amount_out")) {
                            Some(v) => v,
                            None => {
                                return json!({"ok": false, "error": "missing_or_invalid_amount_out"})
                            }
                        };
                        let in_max =
                            parse_u128_arg(payload.get("amount_in_max")).unwrap_or(u128::MAX);

                        if let Some(bps) = payload.get("fee_bps").and_then(|v| v.as_u64()) {
                            plan_swap_tokens_for_exact_tokens(
                                &snapshot_map,
                                token_in,
                                token_out,
                                amount_out,
                                in_max,
                                bps as u32,
                                max_hops,
                            )
                        } else {
                            plan_exact_out_default_fee(
                                &snapshot_map,
                                token_in,
                                token_out,
                                amount_out,
                                in_max,
                                max_hops,
                            )
                        }
                    }

                    // available_in (req), amount_out_min (optional → 0)
                    "implicit" => {
                        let available_in = match parse_u128_arg(
                            payload
                                .get("amount_in")
                                .or_else(|| payload.get("available_in")),
                        ) {
                            Some(v) => v,
                            None => {
                                return json!({"ok": false, "error": "missing_or_invalid_amount_in"})
                            }
                        };
                        let min_out = parse_u128_arg(payload.get("amount_out_min")).unwrap_or(0u128);

                        if let Some(bps) = payload.get("fee_bps").and_then(|v| v.as_u64()) {
                            plan_swap_exact_tokens_for_tokens_implicit(
                                &snapshot_map,
                                token_in,
                                token_out,
                                available_in,
                                min_out,
                                bps as u32,
                                max_hops,
                            )
                        } else {
                            plan_implicit_default_fee(
                                &snapshot_map,
                                token_in,
                                token_out,
                                available_in,
                                min_out,
                                max_hops,
                            )
                        }
                    }

                    _ => {
                        return json!({
                            "ok": false,
                            "error": "invalid_mode",
                            "hint": "use exact_in | exact_out | implicit"
                        })
                    }
                };

                match plan {
                    Some(pq) => {
                        let hops: Vec<Value> = pq
                            .hops
                            .iter()
                            .map(|h| {
                                json!({
                                    "pool":       id_str(&h.pool),
                                    "token_in":   id_str(&h.token_in),
                                    "token_out":  id_str(&h.token_out),
                                    "amount_in":  h.amount_in.to_string(),
                                    "amount_out": h.amount_out.to_string(),
                                })
                            })
                            .collect();

                        json!({
                            "ok": true,
                            "mode": mode,
                            "token_in":  id_str(&token_in),
                            "token_out": id_str(&token_out),
                            "fee_bps": fee_bps,
                            "max_hops": max_hops,
                            "amount_in":  pq.amount_in.to_string(),
                            "amount_out": pq.amount_out.to_string(),
                            "hops": hops
                        })
                    }
                    None => json!({"ok": false, "error": "no_path_found"}),
                }
    }})
            .await;
    });

    /* -------------------- get_best_mev_swap (LIVE reserves, one call) -------------------- */
    let reg_mev = reg.clone();
    let mdb_mev_swap_ptr = Arc::clone(&mdb_ptr);

    tokio::spawn(async move {
        reg_mev
            .register("get_best_mev_swap", move |_cx, payload| {
                let mdb_for_handler = Arc::clone(&mdb_mev_swap_ptr);

                async move {
                // Load LIVE reserves once
                let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
                    match fetch_all_pools(&mdb_for_handler) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[RPC:get_best_mev_swap] live fetch failed: {e:?}");
                            return json!({
                                "ok": false,
                                "error": "no_liquidity",
                                "hint": "live reserves unavailable"
                            });
                        }
                    };

                if snapshot_map.is_empty() {
                    return json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves map is empty"
                    });
                }

                // Parse params
                let token =
                    match payload.get("token").and_then(|v| v.as_str()).and_then(parse_id_from_str)
                    {
                        Some(t) => t,
                        None => return json!({"ok": false, "error": "missing_or_invalid_token"}),
                    };
                let fee_bps = payload
                    .get("fee_bps")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u32)
                    .unwrap_or(DEFAULT_FEE_BPS);
                let max_hops = payload
                    .get("max_hops")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(3)
                    .clamp(2, 6); // cycles require at least 2 hops

                match plan_best_mev_swap(&snapshot_map, token, fee_bps, max_hops) {
                    Some(pq) => {
                        let hops: Vec<Value> = pq
                            .hops
                            .iter()
                            .map(|h| {
                                json!({
                                    "pool":       id_str(&h.pool),
                                    "token_in":   id_str(&h.token_in),
                                    "token_out":  id_str(&h.token_out),
                                    "amount_in":  h.amount_in.to_string(),
                                    "amount_out": h.amount_out.to_string(),
                                })
                            })
                            .collect();

                        json!({
                            "ok": true,
                            "token":   id_str(&token),
                            "fee_bps": fee_bps,
                            "max_hops": max_hops,
                            "amount_in":  pq.amount_in.to_string(),
                            "amount_out": pq.amount_out.to_string(),
                            "profit": (pq.amount_out as i128 - pq.amount_in as i128).to_string(),
                            "hops": hops
                        })
                    }
                    None => json!({"ok": false, "error": "no_profitable_cycle"}),
                }
            }})
            .await;
    });

    /* -------------------- ping -------------------- */
    let reg_ping = reg.clone();
    tokio::spawn(async move {
        reg_ping
            .register("ping", |_cx, _payload| async move {
                eprintln!("[RPC:ping] pong");
                Value::String("pong".to_string())
            })
            .await;
    });
}
