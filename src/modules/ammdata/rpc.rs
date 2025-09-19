use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

use super::schemas::{SchemaAlkaneId, Timeframe};
use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::ammdata::utils::candles::{PriceSide, read_candles_v1};
use crate::modules::ammdata::utils::trades::{
    SortDir, TradePage, TradeSideFilter, TradeSortKey, read_trades_for_pool,
    read_trades_for_pool_sorted,
};
use crate::modules::defs::RpcNsRegistrar;
use crate::runtime::mdb::Mdb;

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Parse pool id string like "2:68441" (decimal) or "0x2:0x10b59" (hex).
fn parse_pool_from_str(s: &str) -> Option<SchemaAlkaneId> {
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

#[allow(dead_code)]
pub fn register_rpc(reg: &RpcNsRegistrar, mdb: Mdb) {
    eprintln!("[RPC_AMMDATA] registering RPC handlers…");

    /* -------------------- get_candles -------------------- */
    let mdb_candles = mdb.clone();
    let reg_candles = reg.clone();
    tokio::spawn(async move {
        reg_candles
            .register("get_candles", move |_cx, payload| {
                let mdb = mdb_candles.clone();
                async move {
                    let tf = payload
                        .get("timeframe").and_then(|v| v.as_str())
                        .and_then(parse_timeframe)
                        .unwrap_or(Timeframe::H1);

                    // support legacy "size" as alias of limit
                    let legacy_size = payload.get("size").and_then(|v| v.as_u64()).map(|n| n as usize);
                    let limit = payload.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize)
                        .or(legacy_size).unwrap_or(120);
                    let page = payload.get("page").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(1);

                    let side = payload.get("side").and_then(|v| v.as_str())
                        .and_then(parse_price_side).unwrap_or(PriceSide::Base);

                    let now = payload.get("now").and_then(|v| v.as_u64()).unwrap_or_else(now_ts);

                    let pool = match payload.get("pool").and_then(|v| v.as_str()).and_then(parse_pool_from_str) {
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

                    let slice = read_candles_v1(&mdb, pool, tf, /*unused*/limit, now, side);
                    match slice {
                        Ok(slice) => {
                            let total = slice.candles_newest_first.len();

                            eprintln!(
                                "[RPC:get_candles] pool={}:{} tf={} side={:?} page={} limit={} total={}",
                                pool.block, pool.tx, tf.code(), side, page, limit, total
                            );

                            let dur = tf.duration_secs();
                            let newest_ts = slice.newest_ts;

                            let offset = limit.saturating_mul(page.saturating_sub(1));
                            let end = (offset + limit).min(total);
                            let page_slice = if offset >= total {
                                &[][..]
                            } else {
                                &slice.candles_newest_first[offset..end]
                            };

                            let arr: Vec<Value> = page_slice.iter().enumerate().map(|(i, c)| {
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
                                "pool": format!("{}:{}", pool.block, pool.tx),
                                "timeframe": tf.code(),
                                "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                "page": page,
                                "limit": limit,
                                "total": total,
                                "has_more": end < total,
                                "candles": arr
                            })
                        }
                        Err(e) => {
                            eprintln!(
                                "[RPC:get_candles] pool={}:{} tf={} side={:?} ERROR={}",
                                pool.block, pool.tx, tf.code(), side, e
                            );
                            json!({ "ok": false, "error": format!("read_failed: {e}") })
                        }
                    }
                }
            })
            .await;
    });

    /* -------------------- get_trades -------------------- */
    // Params:
    //   side:         "base" | "quote"                        (default "base")
    //   filter_side:  "buy" | "sell" | "all"                  (default "all")
    //   sort:         "ts" | "amount" | "side" | "absb"|"absq" (single token; default "ts")
    //   dir:          "asc" | "desc"                          (default "desc")
    //   page, limit
    let mdb_trades = mdb.clone();
    let reg_trades = reg.clone();
    tokio::spawn(async move {
        reg_trades
            .register("get_trades", move |_cx, payload| {
                let mdb = mdb_trades.clone();
                async move {
                    let limit = payload.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(50);
                    let page  = payload.get("page").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(1);

                    let side = payload.get("side").and_then(|v| v.as_str())
                        .and_then(parse_price_side).unwrap_or(PriceSide::Base);

                    let filter_side = parse_side_filter(payload.get("filter_side"));

                    // parse single sort token + dir
                    let sort_token: Option<String> = payload.get("sort").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let dir = parse_sort_dir(payload.get("dir").or_else(|| payload.get("direction")));
                    let (sort_key, sort_code) = map_sort(side, sort_token.as_deref());

                    let pool = match payload.get("pool").and_then(|v| v.as_str()).and_then(parse_pool_from_str) {
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

                    // Use sorted path if sort provided OR a side filter other than "all" is provided.
                    if sort_token.is_some() || !matches!(filter_side, TradeSideFilter::All) {
                        match read_trades_for_pool_sorted(&mdb, pool, page, limit, side, sort_key, dir, filter_side) {
                            Ok(TradePage { trades, total }) => {
                                json!({
                                    "ok": true,
                                    "pool": format!("{}:{}", pool.block, pool.tx),
                                    "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                    "filter_side": match filter_side { TradeSideFilter::All => "all", TradeSideFilter::Buy => "buy", TradeSideFilter::Sell => "sell" },
                                    "sort": sort_code,
                                    "dir": match dir { SortDir::Asc => "asc", SortDir::Desc => "desc" },
                                    "page": page,
                                    "limit": limit,
                                    "total": total,
                                    "has_more": page.saturating_mul(limit) < total,
                                    "trades": trades
                                })
                            }
                            Err(e) => {
                                eprintln!("[RPC:get_trades] pool={}:{} SORTED ERROR={}", pool.block, pool.tx, e);
                                json!({ "ok": false, "error": format!("read_failed: {e}") })
                            }
                        }
                    } else {
                        // Default legacy behavior: timestamp desc (latest first), no filter
                        match read_trades_for_pool(&mdb, pool, page, limit, side) {
                            Ok(TradePage { trades, total }) => {
                                json!({
                                    "ok": true,
                                    "pool": format!("{}:{}", pool.block, pool.tx),
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
                            Err(e) => {
                                eprintln!("[RPC:get_trades] pool={}:{} ERROR={}", pool.block, pool.tx, e);
                                json!({ "ok": false, "error": format!("read_failed: {e}") })
                            }
                        }
                    }
                }
            })
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
