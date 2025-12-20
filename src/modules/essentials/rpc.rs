use crate::alkanes::trace::prettyify_protobuf_trace_json;
use crate::config::{get_metashrew, get_network};
use crate::modules::defs::RpcNsRegistrar;
use crate::modules::essentials::main::Essentials;
use crate::modules::essentials::storage::{
    HoldersCountEntry, holders_count_key, outpoint_addr_key, outpoint_balances_prefix,
};
// for Essentials::k_kv
use crate::runtime::mdb::Mdb;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};

use serde_json::map::Map;
use serde_json::{Value, json};

use borsh::BorshDeserialize;

// <-- use the public helpers & types from balances.rs
use super::storage::BalanceEntry;
use super::utils::balances::{
    get_balance_for_address, get_holders_for_alkane,
    get_outpoint_balances as get_outpoint_balances_index,
};
use super::utils::inspections::{inspection_to_json, load_inspection};

use bitcoin::Address;
use bitcoin::hashes::Hash;
use std::str::FromStr;

/// Local decoder using the public BalanceEntry type
fn decode_balances_vec(bytes: &[u8]) -> anyhow::Result<Vec<BalanceEntry>> {
    Ok(Vec::<BalanceEntry>::try_from_slice(bytes)?)
}

// Normalize and re-encode the address in the canonical (checked) form for the active NETWORK.
// This ensures keys under /balances/{address}/… match what we wrote from the indexer.
fn normalize_address(s: &str) -> Option<String> {
    let network = get_network();
    Address::from_str(s)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.to_string())
}

/* ---------------- register ---------------- */
use std::sync::Arc;
use std::vec;

/// Tiny helper to standardize RPC logs.
#[inline]
fn log_rpc(method: &str, msg: &str) {
    eprintln!("[RPC::ESSENTIALS] {method} — {msg}");
}

pub fn register_rpc(reg: RpcNsRegistrar, mdb: Mdb) {
    // Wrap once; everything else shares this.
    let mdb = Arc::new(mdb);

    eprintln!("[RPC::ESSENTIALS] registering RPC handlers…");

    /* -------- existing: get_keys -------- */
    {
        let reg_get = reg.clone();
        let mdb_get = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_get
                .register("get_keys", move |_cx, payload| {
                    // clone inside the Fn closure, before building async future
                    let mdb = Arc::clone(&mdb_get);
                    async move {
                        // ---- parse alkane id
                        let alk = match payload
                            .get("alkane")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_from_str)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc("get_keys", "missing_or_invalid_alkane");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane",
                                    "hint": "alkane should be a string like \"2:68441\" or \"0x2:0x10b59\""
                                });
                            }
                        };

                        // ---- options
                        let try_decode_utf8 = payload
                            .get("try_decode_utf8")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);

                        let limit_req = payload.get("limit").and_then(|v| v.as_u64()).unwrap_or(100);
                        let limit = limit_req as usize;

                        let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(1).max(1) as usize;

                        log_rpc(
                            "get_keys",
                            &format!(
                                "alkane={}:{}, page={}, limit={}, try_decode_utf8={}",
                                alk.block, alk.tx, page, limit, try_decode_utf8
                            ),
                        );

                        // ---- resolve key set
                        let keys_param = payload.get("keys").and_then(|v| v.as_array());

                        let all_keys: Vec<Vec<u8>> = if let Some(arr) = keys_param {
                            let mut v = Vec::with_capacity(arr.len());
                            for it in arr {
                                if let Some(s) = it.as_str() {
                                    if let Some(bytes) = parse_key_str_to_bytes(s) {
                                        v.push(bytes);
                                    }
                                }
                            }
                            dedup_sort_keys(v)
                        } else {
                            let scan_pref = dir_scan_prefix(&alk);
                            let rel_keys = match mdb.scan_prefix(&scan_pref) {
                                Ok(v) => v,
                                Err(e) => {
                                    log_rpc("get_keys", &format!("scan_prefix failed: {e:?}"));
                                    Vec::new()
                                }
                            };

                            let mut extracted: Vec<Vec<u8>> = Vec::with_capacity(rel_keys.len());
                            for rel in rel_keys {
                                if rel.len() < 1 + 4 + 8 + 2 || rel[0] != 0x03 {
                                    continue;
                                }
                                let key_len = u16::from_be_bytes([rel[13], rel[14]]) as usize;
                                if rel.len() < 1 + 4 + 8 + 2 + key_len {
                                    continue;
                                }
                                extracted.push(rel[15..15 + key_len].to_vec());
                            }
                            dedup_sort_keys(extracted)
                        };

                        // ---- paginate
                        let total = all_keys.len();
                        let offset = limit.saturating_mul(page.saturating_sub(1));
                        let end = (offset + limit).min(total);
                        let window = if offset >= total { &[][..] } else { &all_keys[offset..end] };
                        let has_more = end < total;

                        // ---- build response object
                        let mut items: Map<String, Value> = Map::with_capacity(window.len());

                        for k in window.iter() {
                            let kv_key = Essentials::k_kv(&alk, k);

                            let (last_txid_val, value_hex, value_str_val, value_u128_val) =
                                match mdb.get(&kv_key) {
                                    Ok(Some(v)) => {
                                        let (ltxid_opt, raw) = split_txid_value(&v);
                                        (
                                            ltxid_opt.map(Value::String).unwrap_or(Value::Null),
                                            fmt_bytes_hex(raw),
                                            utf8_or_null(raw),
                                            u128_le_or_null(raw),
                                        )
                                    }
                                    _ => (Value::Null, "0x".to_string(), Value::Null, Value::Null),
                                };

                            let key_hex = fmt_bytes_hex(k);
                            let key_str_val = utf8_or_null(k);

                            let top_key = if try_decode_utf8 {
                                if let Value::String(s) = &key_str_val {
                                    s.clone()
                                } else {
                                    key_hex.clone()
                                }
                            } else {
                                key_hex.clone()
                            };

                            items.insert(
                                top_key,
                                json!({
                                    "key_hex":    key_hex,
                                    "key_str":    key_str_val,
                                    "value_hex":  value_hex,
                                    "value_str":  value_str_val,
                                    "value_u128": value_u128_val,
                                    "last_txid":  last_txid_val
                                }),
                            );
                        }

                        json!({
                            "ok": true,
                            "alkane": format!("{}:{}", alk.block, alk.tx),
                            "page": page,
                            "limit": limit,
                            "total": total,
                            "has_more": has_more,
                            "items": Value::Object(items)
                        })
                    }
                })
                .await;
        });
    }

    /* -------- inspections.lookup -------- */
    {
        let reg_inspection = reg.clone();
        let mdb_inspection = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_inspection
                .register("get_inspection", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_inspection);
                    async move {
                        let alk = match payload
                            .get("alkane")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_from_str)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc("get_inspection", "missing_or_invalid_alkane");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane",
                                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                                });
                            }
                        };

                        match load_inspection(&mdb, &alk) {
                            Ok(Some(record)) => json!({
                                "ok": true,
                                "inspection": inspection_to_json(&record)
                            }),
                            Ok(None) => {
                                json!({"ok": false, "error": "not_found"})
                            }
                            Err(e) => {
                                log_rpc(
                                    "get_inspection",
                                    &format!("load_inspection failed for {}:{}: {e}", alk.block, alk.tx),
                                );
                                json!({"ok": false, "error": "lookup_failed"})
                            }
                        }
                    }
                })
                .await;
        });
    }

    /* -------- NEW: get_holders -------- */
    {
        let reg_holders = reg.clone();
        let mdb_holders = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_holders
                .register("get_holders", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_holders);
                    async move {
                        let alk = match payload
                            .get("alkane")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_from_str)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc("get_holders", "missing_or_invalid_alkane");
                                return json!({"ok": false, "error": "missing_or_invalid_alkane"});
                            }
                        };

                        let limit =
                            payload.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
                        let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(1).max(1)
                            as usize;

                        log_rpc(
                            "get_holders",
                            &format!(
                                "alkane={}:{}, page={}, limit={}",
                                alk.block, alk.tx, page, limit
                            ),
                        );

                        let (total, slice) = match get_holders_for_alkane(&mdb, alk, page, limit) {
                            Ok(tup) => tup,
                            Err(e) => {
                                log_rpc("get_holders", &format!("failed: {e:?}"));
                                return json!({"ok": false, "error": "internal_error"});
                            }
                        };

                        let has_more = page.saturating_mul(limit) < total;

                        let items: Vec<Value> = slice
                            .into_iter()
                            .map(|h| json!({"address": h.address, "amount": h.amount.to_string()}))
                            .collect();

                        json!({
                            "ok": true,
                            "alkane": format!("{}:{}", alk.block, alk.tx),
                            "page": page,
                            "limit": limit,
                            "total": total,
                            "has_more": has_more,
                            "items": items
                        })
                    }
                })
                .await;
        });
    }

    {
        let reg_addr_bal = reg.clone();
        let mdb_addr_bal = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_addr_bal
                .register("get_address_balances", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_addr_bal);
                    async move {
                        let address_raw = match payload.get("address").and_then(|v| v.as_str()) {
                            Some(s) if !s.is_empty() => s.trim(),
                            _ => {
                                log_rpc("get_address_balances", "missing_or_invalid_address");
                                return json!({"ok": false, "error": "missing_or_invalid_address"});
                            }
                        };
                        let address = match normalize_address(address_raw) {
                            Some(a) => a,
                            None => {
                                log_rpc("get_address_balances", "invalid_address_format");
                                return json!({"ok": false, "error": "invalid_address_format"});
                            }
                        };

                        let include_outpoints = payload
                            .get("include_outpoints")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        log_rpc(
                            "get_address_balances",
                            &format!(
                                "address={}, include_outpoints={}",
                                address, include_outpoints
                            ),
                        );

                        let agg = match get_balance_for_address(&mdb, &address) {
                            Ok(m) => m,
                            Err(e) => {
                                log_rpc(
                                    "get_address_balances",
                                    &format!("get_balance_for_address failed: {e:?}"),
                                );
                                return json!({"ok": false, "error": "internal_error"});
                            }
                        };

                        let mut balances: Map<String, Value> = Map::new();
                        for (id, amt) in agg {
                            balances.insert(
                                format!("{}:{}", id.block, id.tx),
                                Value::String(amt.to_string()),
                            );
                        }

                        let mut resp = json!({
                            "ok": true,
                            "address": address,
                            "balances": Value::Object(balances),
                        });

                        if include_outpoints {
                            let mut pref = b"/balances/".to_vec();
                            pref.extend_from_slice(resp["address"].as_str().unwrap().as_bytes());
                            pref.push(b'/');

                            let keys = match mdb.scan_prefix(&pref) {
                                Ok(v) => v,
                                Err(e) => {
                                    log_rpc(
                                        "get_address_balances",
                                        &format!("scan_prefix failed: {e:?}"),
                                    );
                                    Vec::new()
                                }
                            };

                            let mut outpoints = Vec::with_capacity(keys.len());
                            for k in keys {
                                let val = match mdb.get(&k) {
                                    Ok(Some(v)) => v,
                                    _ => continue,
                                };
                                let entries = match decode_balances_vec(&val) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let op = match std::str::from_utf8(&k[pref.len()..]) {
                                    Ok(s) => s.to_string(),
                                    Err(_) => continue,
                                };
                                let entry_list: Vec<Value> = entries.into_iter().map(|be| {
                                    json!({
                                        "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                                        "amount": be.amount.to_string()
                                    })
                                }).collect();

                                outpoints.push(json!({ "outpoint": op, "entries": entry_list }));
                            }

                            resp.as_object_mut()
                                .unwrap()
                                .insert("outpoints".to_string(), Value::Array(outpoints));
                        }

                        resp
                    }
                })
                .await;
        });
    }

    {
        let reg_op_bal = reg.clone();
        let mdb_op_bal = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_op_bal
                .register("get_outpoint_balances", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_op_bal);
                    async move {
                        let outpoint = match payload.get("outpoint").and_then(|v| v.as_str()) {
                            Some(s) if !s.is_empty() => s.trim().to_string(),
                            _ => {
                                log_rpc("get_outpoint_balances", "missing_or_invalid_outpoint");
                                return json!({"ok": false, "error": "missing_or_invalid_outpoint", "hint": "expected \"<txid>:<vout>\""});
                            }
                        };

                        // parse "<txid>:<vout>"
                        let (txid, vout_u32) = match outpoint.split_once(':') {
                            Some((txid_hex, vout_str)) => {
                                let txid = match bitcoin::Txid::from_str(txid_hex) {
                                    Ok(t) => t,
                                    Err(_) => {
                                        log_rpc("get_outpoint_balances", "invalid_txid");
                                        return json!({"ok": false, "error": "invalid_txid"});
                                    }
                                };
                                let vout_u32 = match vout_str.parse::<u32>() {
                                    Ok(n) => n,
                                    Err(_) => {
                                        log_rpc("get_outpoint_balances", "invalid_vout");
                                        return json!({"ok": false, "error": "invalid_vout"});
                                    }
                                };
                                (txid, vout_u32)
                            }
                            None => {
                                log_rpc("get_outpoint_balances", "invalid_outpoint_format");
                                return json!({"ok": false, "error": "invalid_outpoint_format", "hint": "expected \"<txid>:<vout>\""});
                            }
                        };

                        log_rpc("get_outpoint_balances", &format!("outpoint={}", outpoint));

                        let entries = match get_outpoint_balances_index(&mdb, &txid, vout_u32) {
                            Ok(v) => v,
                            Err(e) => {
                                log_rpc("get_outpoint_balances", &format!("index lookup failed: {e:?}"));
                                return json!({"ok": false, "error": "internal_error"});
                            }
                        };

                            let addr = {
                                let pref =
                                    outpoint_balances_prefix(txid.to_byte_array().as_slice(), vout_u32);
                            if let Ok(pref) = pref {
                                if let Ok(keys) = mdb.scan_prefix(&pref) {
                                    if let Some(full_key) = keys.first() {
                                        let raw = &full_key[b"/outpoint_balances/".len()..];
                                        if let Ok(op) = EspoOutpoint::try_from_slice(raw) {
                                            let key_new = outpoint_addr_key(&op).ok();
                                            key_new.and_then(|k| mdb.get(&k).ok().flatten()).and_then(|b| {
                                                std::str::from_utf8(&b).ok().map(|s| s.to_string())
                                            })
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };

                        let entry_list: Vec<Value> = entries.into_iter().map(|be| {
                            json!({
                                "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                                "amount": be.amount.to_string()
                            })
                        }).collect();

                        let mut item = json!({
                            "outpoint": outpoint,
                            "entries": entry_list
                        });
                        if let Some(a) = addr {
                            item.as_object_mut().unwrap().insert("address".to_string(), Value::String(a));
                        }

                        json!({
                            "ok": true,
                            "outpoint": item["outpoint"],
                            "items": [item]
                        })
                    }
                })
                .await;
        });
    }

    {
        let reg_traces = reg.clone();
        tokio::spawn(async move {
            reg_traces
                .register("get_block_traces", move |_cx, payload| async move {
                    let height = match payload.get("height").and_then(|v| v.as_u64()) {
                        Some(h) => h,
                        None => {
                            log_rpc("get_block_traces", "missing_or_invalid_height");
                            return json!({"ok": false, "error": "missing_or_invalid_height", "hint": "expected {\"height\": <u64>}"} );
                        }
                    };

                    log_rpc("get_block_traces", &format!("height={height}"));

                    let partials = match get_metashrew().traces_for_block_as_prost(height) {
                        Ok(v) => v,
                        Err(e) => {
                            log_rpc("get_block_traces", &format!("metashrew fetch failed: {e:?}"));
                            return json!({"ok": false, "error": "metashrew_fetch_failed"});
                        }
                    };

                    let mut traces: Vec<Value> = Vec::with_capacity(partials.len());
                    for p in partials {
                        if p.outpoint.len() < 36 {
                            continue;
                        }
                        let (txid_le, vout_le) = p.outpoint.split_at(32);
                        let mut txid_be = txid_le.to_vec();
                        txid_be.reverse();
                        let txid_hex = hex::encode(&txid_be);
                        let vout = u32::from_le_bytes(vout_le[..4].try_into().expect("vout 4 bytes"));

                        let events_str = match prettyify_protobuf_trace_json(&p.protobuf_trace) {
                            Ok(s) => s,
                            Err(e) => {
                                log_rpc(
                                    "get_block_traces",
                                    &format!("normalize failed for {txid_hex}:{vout}: {e:?}"),
                                );
                                continue;
                            }
                        };
                        let events: Value = serde_json::from_str(&events_str).unwrap_or(Value::Null);

                        traces.push(json!({
                            "outpoint": format!("{txid_hex}:{vout}"),
                            "events": events
                        }));
                    }

                    json!({
                        "ok": true,
                        "height": height,
                        "traces": traces
                    })
                })
                .await;
        });
    }

    {
        let reg_holders_count = reg.clone();
        let mdb_holders_count = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_holders_count
                .register("get_holders_count", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_holders_count);
                    async move {


                        let alkane = match payload
                            .get("alkane")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_from_str)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc("get_holders_count", "missing_or_invalid_alkane");
                                return json!({"ok": false, "error": "missing_or_invalid_alkane"});
                            }
                        };

                        let holders_count_key = holders_count_key(&alkane);


                        let count: u64 = match HoldersCountEntry::try_from_slice(&mdb.get(&holders_count_key).ok().flatten().unwrap_or(vec![] as Vec<u8>)) {
                            Ok(count_value) => count_value.count,
                            Err(_) => {
                                log_rpc("get_holders_count", "missing_or_invalid_outpoint");
                                return json!({"ok": false, "error": "missing_or_invalid_outpoint", "hint": "expected \"<txid>:<vout>\""});
                            }
                        };


                        json!({
                            "ok": true,
                            "count": count,
                        })
                    }
                })
                .await;
        });
    }

    {
        let reg_addr_ops = reg.clone();
        let mdb_addr_ops = Arc::clone(&mdb);
        tokio::spawn(async move {
            reg_addr_ops
                .register("get_address_outpoints", move |_cx, payload| {
                    let mdb = Arc::clone(&mdb_addr_ops);
                    async move {
                        let address_raw = match payload.get("address").and_then(|v| v.as_str()) {
                            Some(s) if !s.is_empty() => s.trim(),
                            _ => {
                                log_rpc("get_address_outpoints", "missing_or_invalid_address");
                                return json!({"ok": false, "error": "missing_or_invalid_address"});
                            }
                        };
                        let address = match normalize_address(address_raw) {
                            Some(a) => a,
                            None => {
                                log_rpc("get_address_outpoints", "invalid_address_format");
                                return json!({"ok": false, "error": "invalid_address_format"});
                            }
                        };

                        log_rpc("get_address_outpoints", &format!("address={address}"));

                        // Prefix for /balances/{address}/
                        let mut pref = b"/balances/".to_vec();
                        pref.extend_from_slice(address.as_bytes());
                        pref.push(b'/');

                        let keys = match mdb.scan_prefix(&pref) {
                            Ok(v) => v,
                            Err(e) => {
                                log_rpc(
                                    "get_address_outpoints",
                                    &format!("scan_prefix failed: {e:?}"),
                                );
                                Vec::new()
                            }
                        };

                        let keys_amount = keys.len();
                        log_rpc(
                            "get_address_outpoints",
                            &format!("address={address}, estimated_keys={keys_amount}"),
                        );

                        let mut outpoints: Vec<Value> = Vec::with_capacity(keys.len());

                        for k in keys {
                            if k.len() <= pref.len() {
                                continue;
                            }

                            let decoded = EspoOutpoint::try_from_slice(&k[pref.len()..]);
                            let espo_out = match decoded {
                                Ok(op) => op,
                                Err(err) => {
                                    log_rpc(
                                        "get_address_outpoints",
                                        &format!("decode failed: {err}"),
                                    );
                                    continue;
                                }
                            };

                            if espo_out.tx_spent.is_some() {
                                continue;
                            }

                            let outpoint_str = espo_out.as_outpoint_string();

                            let (txid, vout) = match outpoint_str.split_once(':') {
                                Some((txid_hex, vout_s)) => {
                                    let tid = match bitcoin::Txid::from_str(txid_hex) {
                                        Ok(t) => t,
                                        Err(_) => continue,
                                    };
                                    let v = match vout_s.parse::<u32>() {
                                        Ok(n) => n,
                                        Err(_) => continue,
                                    };
                                    (tid, v)
                                }
                                None => continue,
                            };

                            let entries_vec = match get_outpoint_balances_index(&mdb, &txid, vout) {
                                Ok(v) => v,
                                Err(e) => {
                                    log_rpc(
                                        "get_address_outpoints",
                                        &format!(
                                            "O(1) index read failed for {outpoint_str}: {e:?}"
                                        ),
                                    );
                                    Vec::new()
                                }
                            };

                            let entry_list: Vec<Value> = entries_vec
                                .into_iter()
                                .map(|be| {
                                    json!({
                                        "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                                        "amount": be.amount.to_string()
                                    })
                                })
                                .collect();

                            outpoints.push(json!({
                                "outpoint": outpoint_str,
                                "entries": entry_list
                            }));
                        }

                        outpoints.sort_by(|a, b| {
                            let sa = a.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
                            let sb = b.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
                            sa.cmp(sb)
                        });
                        outpoints.dedup_by(|a, b| {
                            a.get("outpoint").and_then(|v| v.as_str())
                                == b.get("outpoint").and_then(|v| v.as_str())
                        });

                        json!({
                            "ok": true,
                            "address": address,
                            "outpoints": outpoints
                        })
                    }
                })
                .await;
        });
    }

    // simple ping (doesn't need mdb)
    {
        let reg_ping = reg.clone();
        tokio::spawn(async move {
            reg_ping
                .register("ping", |_cx, _payload| async move {
                    log_rpc("ping", "ok");
                    Value::String("pong".to_string())
                })
                .await;
        });
    }
}

/* ---------------- helpers ---------------- */

fn dir_scan_prefix(alk: &SchemaAlkaneId) -> [u8; 1 + 4 + 8] {
    let mut p = [0u8; 1 + 4 + 8];
    p[0] = 0x03;
    p[1..5].copy_from_slice(&alk.block.to_be_bytes());
    p[5..13].copy_from_slice(&alk.tx.to_be_bytes());
    p
}

fn parse_alkane_from_str(s: &str) -> Option<SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let parse_u32 = |t: &str| {
        if let Some(x) = t.strip_prefix("0x") {
            u32::from_str_radix(x, 16).ok()
        } else {
            t.parse::<u32>().ok()
        }
    };
    let parse_u64 = |t: &str| {
        if let Some(x) = t.strip_prefix("0x") {
            u64::from_str_radix(x, 16).ok()
        } else {
            t.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(parts[0])?, tx: parse_u64(parts[1])? })
}

fn parse_key_str_to_bytes(s: &str) -> Option<Vec<u8>> {
    if let Some(hex) = s.strip_prefix("0x") {
        if hex.len() % 2 == 0 && !hex.is_empty() {
            return hex::decode(hex).ok();
        }
    }
    Some(s.as_bytes().to_vec())
}

fn dedup_sort_keys(mut v: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    v.sort();
    v.dedup();
    v
}

/// Split the stored value row into `(last_txid_be_hex, raw_value_bytes)`.
/// First 32 bytes = txid in LE; we flip to BE for explorers.
/// Returns (Some("deadbeef…"), tail) or (None, whole) if no txid present.
fn split_txid_value(v: &[u8]) -> (Option<String>, &[u8]) {
    if v.len() >= 32 {
        let txid_le = &v[..32];
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();
        (Some(fmt_bytes_hex_noprefix(&txid_be)), &v[32..])
    } else {
        (None, v)
    }
}

// hex with "0x"
pub fn fmt_bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

// hex without "0x"
fn fmt_bytes_hex_noprefix(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

fn utf8_or_null(b: &[u8]) -> Value {
    match std::str::from_utf8(b) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => Value::Null,
    }
}
fn u128_le_or_null(b: &[u8]) -> Value {
    if b.len() > 16 {
        return Value::Null;
    }
    let mut acc: u128 = 0;
    for (i, &byte) in b.iter().enumerate() {
        acc |= (byte as u128) << (i * 8);
    }
    Value::String(acc.to_string())
}
