use crate::config::get_last_safe_tip;
use crate::modules::defs::RpcNsRegistrar;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use serde_json::{Value, json};
use std::sync::Arc;

use super::storage::{
    GetSeriesByAlkaneParams, GetSeriesByAlkanesParams, GetSeriesByIdParams, GetSeriesByIdsParams,
    PizzafunProvider, SeriesEntry, normalize_series_id,
};

#[inline]
fn log_rpc(method: &str, msg: &str) {
    eprintln!("[RPC::PIZZAFUN] {method} - {msg}");
}

fn parse_height_payload(payload: &Value) -> Result<(Option<u64>, bool), ()> {
    let Some(raw) = payload.get("height") else {
        return Ok((None, false));
    };

    if raw.is_null() {
        return Ok((None, false));
    }

    if let Some(height) = raw.as_u64() {
        return Ok((Some(height), true));
    }

    let Some(s) = raw.as_str().map(str::trim) else {
        return Err(());
    };
    if s.is_empty() || s.eq_ignore_ascii_case("latest") {
        return Ok((None, false));
    }
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).map(|h| (Some(h), true)).map_err(|_| ());
    }
    s.parse::<u64>().map(|h| (Some(h), true)).map_err(|_| ())
}

fn resolve_view(provider: &PizzafunProvider, payload: &Value) -> Result<PizzafunProvider, Value> {
    let (height, height_present) = parse_height_payload(payload)
        .map_err(|_| json!({"ok": false, "error": "invalid_height"}))?;
    provider
        .with_height(height, height_present)
        .map_err(|_| json!({"ok": false, "error": "invalid_height"}))
}

fn parse_alkane_id(s: &str) -> Option<SchemaAlkaneId> {
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

fn confirmations_for(creation_height: u32) -> u32 {
    get_last_safe_tip().map(|tip| tip.saturating_sub(creation_height)).unwrap_or(0)
}

fn entry_to_json(entry: &SeriesEntry) -> Value {
    json!({
        "series_id": entry.series_id.clone(),
        "alkane_id": format!("{}:{}", entry.alkane_id.block, entry.alkane_id.tx),
        "confirmations": confirmations_for(entry.creation_height),
    })
}

pub(crate) fn register_rpc(reg: RpcNsRegistrar, provider: Arc<PizzafunProvider>) {
    eprintln!("[RPC::PIZZAFUN] registering RPC handlers...");

    /* -------- get_series_id_from_alkane_id -------- */
    {
        let reg_one = reg.clone();
        let provider_one = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_one
                .register("get_series_id_from_alkane_id", move |_cx, payload| {
                    let provider = Arc::clone(&provider_one);
                    async move {
                        let view = match resolve_view(provider.as_ref(), &payload) {
                            Ok(v) => v,
                            Err(err) => return err,
                        };
                        let alk = match payload
                            .get("alkane_id")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_id)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc(
                                    "get_series_id_from_alkane_id",
                                    "missing_or_invalid_alkane_id",
                                );
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane_id",
                                    "hint": "provide alkane_id as \"<block>:<tx>\" (hex ok)"
                                });
                            }
                        };

                        let entry = match view.get_series_by_alkane(GetSeriesByAlkaneParams {
                            blockhash: StateAt::Latest,
                            alkane: alk,
                        }) {
                            Ok(Some(entry)) => entry,
                            Ok(None) => return json!({"ok": false, "error": "not_found"}),
                            Err(e) => {
                                log_rpc("get_series_id_from_alkane_id", &format!("db_error: {e}"));
                                return json!({"ok": false, "error": "db_error"});
                            }
                        };

                        let mut out = entry_to_json(&entry);
                        if let Value::Object(ref mut map) = out {
                            map.insert("ok".to_string(), Value::Bool(true));
                        }
                        out
                    }
                })
                .await;
        });
    }

    /* -------- get_series_ids_from_alkane_ids -------- */
    {
        let reg_batch = reg.clone();
        let provider_batch = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_batch
                .register("get_series_ids_from_alkane_ids", move |_cx, payload| {
                    let provider = Arc::clone(&provider_batch);
                    async move {
                        let view = match resolve_view(provider.as_ref(), &payload) {
                            Ok(v) => v,
                            Err(err) => return err,
                        };
                        let ids = match payload.get("alkane_ids").and_then(|v| v.as_array()) {
                            Some(v) => v,
                            None => {
                                log_rpc(
                                    "get_series_ids_from_alkane_ids",
                                    "missing_or_invalid_alkane_ids",
                                );
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane_ids",
                                    "hint": "provide alkane_ids as an array of \"<block>:<tx>\""
                                });
                            }
                        };

                        let mut parsed: Vec<Option<SchemaAlkaneId>> = Vec::with_capacity(ids.len());
                        let mut lookup: Vec<SchemaAlkaneId> = Vec::new();
                        for raw in ids {
                            let Some(s) = raw.as_str() else {
                                parsed.push(None);
                                continue;
                            };
                            let Some(alk) = parse_alkane_id(s) else {
                                parsed.push(None);
                                continue;
                            };
                            lookup.push(alk);
                            parsed.push(Some(alk));
                        }

                        let mut out: Vec<Value> = Vec::with_capacity(ids.len());
                        let results = match view.get_series_by_alkanes(GetSeriesByAlkanesParams {
                            blockhash: StateAt::Latest,
                            alkanes: lookup.clone(),
                        }) {
                            Ok(res) => res,
                            Err(e) => {
                                log_rpc(
                                    "get_series_ids_from_alkane_ids",
                                    &format!("db_error: {e}"),
                                );
                                return json!({"ok": false, "error": "db_error"});
                            }
                        };
                        let mut res_iter = results.into_iter();
                        for maybe in parsed {
                            match maybe {
                                None => out.push(Value::Null),
                                Some(_) => {
                                    let entry = res_iter.next().unwrap_or(None);
                                    match entry {
                                        Some(entry) => out.push(entry_to_json(&entry)),
                                        None => out.push(Value::Null),
                                    }
                                }
                            }
                        }

                        json!({
                            "ok": true,
                            "items": out,
                        })
                    }
                })
                .await;
        });
    }

    /* -------- get_alkane_id_from_series_id -------- */
    {
        let reg_one = reg.clone();
        let provider_one = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_one
                .register("get_alkane_id_from_series_id", move |_cx, payload| {
                    let provider = Arc::clone(&provider_one);
                    async move {
                        let view = match resolve_view(provider.as_ref(), &payload) {
                            Ok(v) => v,
                            Err(err) => return err,
                        };
                        let series_id = match payload
                            .get("series_id")
                            .and_then(|v| v.as_str())
                            .and_then(normalize_series_id)
                        {
                            Some(s) => s,
                            None => {
                                log_rpc(
                                    "get_alkane_id_from_series_id",
                                    "missing_or_invalid_series_id",
                                );
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_series_id"
                                });
                            }
                        };

                        let entry = match view.get_series_by_id(GetSeriesByIdParams {
                            blockhash: StateAt::Latest,
                            series_id: series_id.clone(),
                        }) {
                            Ok(Some(entry)) => entry,
                            Ok(None) => return json!({"ok": false, "error": "not_found"}),
                            Err(e) => {
                                log_rpc("get_alkane_id_from_series_id", &format!("db_error: {e}"));
                                return json!({"ok": false, "error": "db_error"});
                            }
                        };

                        let mut out = entry_to_json(&entry);
                        if let Value::Object(ref mut map) = out {
                            map.insert("ok".to_string(), Value::Bool(true));
                        }
                        out
                    }
                })
                .await;
        });
    }

    /* -------- get_alkane_ids_from_series_ids -------- */
    {
        let reg_batch = reg.clone();
        let provider_batch = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_batch
                .register("get_alkane_ids_from_series_ids", move |_cx, payload| {
                    let provider = Arc::clone(&provider_batch);
                    async move {
                        let view = match resolve_view(provider.as_ref(), &payload) {
                            Ok(v) => v,
                            Err(err) => return err,
                        };
                        let ids = match payload.get("series_ids").and_then(|v| v.as_array()) {
                            Some(v) => v,
                            None => {
                                log_rpc(
                                    "get_alkane_ids_from_series_ids",
                                    "missing_or_invalid_series_ids",
                                );
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_series_ids",
                                    "hint": "provide series_ids as an array of strings"
                                });
                            }
                        };

                        let mut parsed: Vec<Option<String>> = Vec::with_capacity(ids.len());
                        let mut lookup: Vec<String> = Vec::new();
                        for raw in ids {
                            let Some(s) = raw.as_str() else {
                                parsed.push(None);
                                continue;
                            };
                            let Some(series_id) = normalize_series_id(s) else {
                                parsed.push(None);
                                continue;
                            };
                            lookup.push(series_id.clone());
                            parsed.push(Some(series_id));
                        }

                        let mut out: Vec<Value> = Vec::with_capacity(ids.len());
                        let results = match view.get_series_by_ids(GetSeriesByIdsParams {
                            blockhash: StateAt::Latest,
                            series_ids: lookup.clone(),
                        }) {
                            Ok(res) => res,
                            Err(e) => {
                                log_rpc(
                                    "get_alkane_ids_from_series_ids",
                                    &format!("db_error: {e}"),
                                );
                                return json!({"ok": false, "error": "db_error"});
                            }
                        };
                        let mut res_iter = results.into_iter();
                        for maybe in parsed {
                            match maybe {
                                None => out.push(Value::Null),
                                Some(_) => {
                                    let entry = res_iter.next().unwrap_or(None);
                                    match entry {
                                        Some(entry) => out.push(entry_to_json(&entry)),
                                        None => out.push(Value::Null),
                                    }
                                }
                            }
                        }

                        json!({
                            "ok": true,
                            "items": out,
                        })
                    }
                })
                .await;
        });
    }
}
