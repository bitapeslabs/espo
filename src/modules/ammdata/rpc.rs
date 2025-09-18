use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

use super::schemas::{SchemaAlkaneId, Timeframe};
use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::ammdata::utils::candles::{PriceSide, read_candles_v1};
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

fn parse_side(s: &str) -> Option<PriceSide> {
    match s.to_ascii_lowercase().as_str() {
        "base" | "b" => Some(PriceSide::Base),
        "quote" | "q" => Some(PriceSide::Quote),
        _ => None,
    }
}

#[inline]
fn scale(x: u128) -> f64 {
    (x as f64) / (PRICE_SCALE as f64)
}

#[allow(dead_code)]
pub fn register_rpc(reg: &RpcNsRegistrar, mdb: Mdb) {
    eprintln!("[RPC_AMMDATA] registering RPC handlers…");

    // GET CANDLES
    let mdb_candles = mdb.clone();
    let reg_candles = reg.clone();
    tokio::spawn(async move {
        reg_candles
            .register("get_candles", move |_cx, payload| {
                let mdb = mdb_candles.clone();
                async move {
                    let tf = payload
                        .get("timeframe")
                        .and_then(|v| v.as_str())
                        .and_then(parse_timeframe)
                        .unwrap_or(Timeframe::H1);

                    // Paging (page/limit). Legacy "size" maps to limit when page not provided.
                    let legacy_size = payload.get("size").and_then(|v| v.as_u64()).map(|n| n as usize);
                    let limit = payload
                        .get("limit").and_then(|v| v.as_u64()).map(|n| n as usize)
                        .or(legacy_size)
                        .unwrap_or(120);
                    let page  = payload
                        .get("page").and_then(|v| v.as_u64()).map(|n| n as usize)
                        .unwrap_or(1);

                    let side = payload
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(parse_side)
                        .unwrap_or(PriceSide::Base);

                    // still read "now" but not used for anchoring anymore; harmless
                    let now = payload.get("now").and_then(|v| v.as_u64()).unwrap_or_else(now_ts);

                    // Pool (string, e.g. "2:68441")
                    let pool = match payload.get("pool").and_then(|v| v.as_str()).and_then(parse_pool_from_str) {
                        Some(p) => p,
                        None => {
                            return json!({
                                "ok": false,
                                "error": "missing_or_invalid_pool",
                                "hint": "pool should be a string like \"2:68441\""
                            });
                        }
                    };

                    // Read *all present* candles newest→oldest anchored to latest present bucket
                    let slice = read_candles_v1(&mdb, pool, tf, /*unused*/limit, now, side);
                    match slice {
                        Ok(slice) => {
                            let total = slice.candles_newest_first.len();
                            if total == 0 {
                                return json!({
                                    "ok": true,
                                    "pool": format!("{}:{}", pool.block, pool.tx),
                                    "timeframe": tf.code(),
                                    "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                                    "page": page,
                                    "limit": limit,
                                    "total": 0,
                                    "has_more": false,
                                    "candles": []
                                });
                            }

                            let dur = tf.duration_secs();
                            let newest_ts = slice.newest_ts;

                            // paging
                            let offset = limit.saturating_mul(page.saturating_sub(1));
                            let end = (offset + limit).min(total);
                            let page_slice = if offset >= total {
                                &[][..]
                            } else {
                                &slice.candles_newest_first[offset..end]
                            };

                            let arr: Vec<Value> = page_slice.iter().enumerate().map(|(i, c)| {
                                let global_idx = offset + i; // index within newest→oldest stream
                                let ts = newest_ts.saturating_sub((global_idx as u64) * dur);
                                json!({
                                    "ts":     ts,
                                    "open":   scale(c.open),
                                    "high":   scale(c.high),
                                    "low":    scale(c.low),
                                    "close":  scale(c.close),
                                    "volume": scale(c.volume),
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
                        Err(e) => json!({ "ok": false, "error": format!("read_failed: {e}") }),
                    }
                }
            })
            .await;
    });

    // PING
    let reg_ping = reg.clone();
    tokio::spawn(async move {
        reg_ping
            .register("ping", |_cx, _payload| async move { Value::String("pong".to_string()) })
            .await;
    });

    eprintln!("[RPC_AMMDATA] RPC handlers ready.");
}
