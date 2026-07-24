//! `internal.*` JSON-RPC methods: raw, read-only storage primitives that let a
//! remote espo explorer (configured with `explorer_espo_rpc_host`) fulfil its
//! SSR data needs against this instance. Every module getter bottoms out in
//! these Mdb primitives, so serving them is sufficient for the whole explorer.
//!
//! Only registered when `enable_internal_rpc` is true in config: the methods
//! expose raw namespace scans, which are cheap-by-construction for the
//! explorer's access patterns but are not meant for untrusted public traffic.

use crate::config::get_espo_db;
use crate::modules::defs::RpcNsRegistrar;
use crate::runtime::mdb::Mdb;
use crate::runtime::tree_db::get_global_tree_db;
use bitcoin::BlockHash;
use bitcoin::hashes::Hash;
use serde_json::{Value, json};

/// Upper bound on entries a single scan call will return; the explorer's
/// getters never legitimately need more in one call.
const MAX_SCAN_ENTRIES: usize = 1_000_000;

fn err(error: &str, hint: &str) -> Value {
    json!({ "ok": false, "error": error, "hint": hint })
}

fn parse_prefix(payload: &Value) -> Result<Vec<u8>, Value> {
    let Some(prefix) = payload.get("prefix").and_then(Value::as_str) else {
        return Err(err("missing_prefix", "provide prefix like \"essentials:\""));
    };
    let valid = prefix.len() >= 2
        && prefix.ends_with(':')
        && prefix[..prefix.len() - 1]
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        return Err(err(
            "invalid_prefix",
            "prefix must be a module namespace like \"essentials:\"",
        ));
    }
    Ok(prefix.as_bytes().to_vec())
}

fn parse_hex_field(payload: &Value, field: &str) -> Result<Vec<u8>, Value> {
    let Some(raw) = payload.get(field).and_then(Value::as_str) else {
        return Err(err("missing_field", &format!("{field} (hex string) is required")));
    };
    hex::decode(raw.strip_prefix("0x").unwrap_or(raw))
        .map_err(|_| err("invalid_hex", &format!("{field} must be hex")))
}

fn parse_opt_blockhash(payload: &Value) -> Result<Option<BlockHash>, Value> {
    let Some(raw) = payload.get("blockhash").and_then(Value::as_str) else {
        return Ok(None);
    };
    let bytes = hex::decode(raw.strip_prefix("0x").unwrap_or(raw))
        .map_err(|_| err("invalid_blockhash", "blockhash must be 32 hex bytes"))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| err("invalid_blockhash", "blockhash must be 32 hex bytes"))?;
    Ok(Some(BlockHash::from_byte_array(arr)))
}

fn entries_json(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Value {
    if entries.len() > MAX_SCAN_ENTRIES {
        return err("scan_too_large", "narrow the scan prefix/range");
    }
    json!({
        "ok": true,
        "entries": entries
            .into_iter()
            .map(|(k, v)| json!([hex::encode(k), hex::encode(v)]))
            .collect::<Vec<_>>(),
    })
}

fn internal_error(e: impl std::fmt::Display) -> Value {
    json!({ "ok": false, "error": "internal_error", "detail": e.to_string() })
}

fn mdb_for(prefix: &[u8]) -> Mdb {
    Mdb::from_db(get_espo_db(), prefix)
}

pub fn register_internal_rpc(reg: RpcNsRegistrar) {
    eprintln!("[RPC::INTERNAL] registering storage-primitive RPC handlers…");

    {
        let reg_get = reg.clone();
        tokio::spawn(async move {
            reg_get
                .register("mdb_get", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let key = match parse_hex_field(&payload, "key") {
                        Ok(k) => k,
                        Err(e) => return e,
                    };
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let mdb = mdb_for(&prefix);
                    let result = match blockhash {
                        Some(bh) => mdb.get_at_blockhash(&bh, &key),
                        None => mdb.get(&key),
                    };
                    match result {
                        Ok(value) => {
                            json!({ "ok": true, "value": value.map(hex::encode) })
                        }
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_multi = reg.clone();
        tokio::spawn(async move {
            reg_multi
                .register("mdb_multi_get", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let Some(raw_keys) = payload.get("keys").and_then(Value::as_array) else {
                        return err("missing_field", "keys (array of hex strings) is required");
                    };
                    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(raw_keys.len());
                    for raw in raw_keys {
                        let Some(s) = raw.as_str() else {
                            return err("invalid_hex", "keys entries must be hex strings");
                        };
                        match hex::decode(s.strip_prefix("0x").unwrap_or(s)) {
                            Ok(k) => keys.push(k),
                            Err(_) => return err("invalid_hex", "keys entries must be hex strings"),
                        }
                    }
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let handle = tokio::task::spawn_blocking(move || {
                        let mdb = mdb_for(&prefix);
                        match blockhash {
                            Some(bh) => mdb.multi_get_at_blockhash(&bh, &keys),
                            None => mdb.multi_get(&keys),
                        }
                    });
                    match handle.await {
                        Ok(Ok(values)) => json!({
                            "ok": true,
                            "values": values
                                .into_iter()
                                .map(|v| v.map(hex::encode).map(Value::String).unwrap_or(Value::Null))
                                .collect::<Vec<_>>(),
                        }),
                        Ok(Err(e)) => internal_error(e),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_scan = reg.clone();
        tokio::spawn(async move {
            reg_scan
                .register("mdb_scan_prefix_entries", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let scan_prefix = match parse_hex_field(&payload, "scan_prefix") {
                        Ok(k) => k,
                        Err(e) => return e,
                    };
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let handle = tokio::task::spawn_blocking(move || {
                        let mdb = mdb_for(&prefix);
                        match blockhash {
                            Some(bh) => mdb.scan_prefix_entries_at_blockhash(&bh, &scan_prefix),
                            None => mdb.scan_prefix_entries(&scan_prefix),
                        }
                    });
                    match handle.await {
                        Ok(Ok(entries)) => entries_json(entries),
                        Ok(Err(e)) => internal_error(e),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_keys = reg.clone();
        tokio::spawn(async move {
            reg_keys
                .register("mdb_scan_prefix_keys", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let scan_prefix = match parse_hex_field(&payload, "scan_prefix") {
                        Ok(k) => k,
                        Err(e) => return e,
                    };
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let handle = tokio::task::spawn_blocking(move || {
                        let mdb = mdb_for(&prefix);
                        match blockhash {
                            Some(bh) => mdb.scan_prefix_keys_at_blockhash(&bh, &scan_prefix),
                            None => mdb.scan_prefix_keys(&scan_prefix),
                        }
                    });
                    match handle.await {
                        Ok(Ok(keys)) => {
                            if keys.len() > MAX_SCAN_ENTRIES {
                                return err("scan_too_large", "narrow the scan prefix");
                            }
                            json!({
                                "ok": true,
                                "keys": keys.into_iter().map(hex::encode).collect::<Vec<_>>(),
                            })
                        }
                        Ok(Err(e)) => internal_error(e),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_range = reg.clone();
        tokio::spawn(async move {
            reg_range
                .register("mdb_scan_range_entries", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let start = match parse_hex_field(&payload, "start") {
                        Ok(k) => k,
                        Err(e) => return e,
                    };
                    let end = if payload.get("end").map(|v| !v.is_null()).unwrap_or(false) {
                        match parse_hex_field(&payload, "end") {
                            Ok(k) => Some(k),
                            Err(e) => return e,
                        }
                    } else {
                        None
                    };
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let handle = tokio::task::spawn_blocking(move || {
                        let mdb = mdb_for(&prefix);
                        match blockhash {
                            Some(bh) => {
                                mdb.scan_range_entries_at_blockhash(&bh, &start, end.as_deref())
                            }
                            None => mdb.scan_range_entries(&start, end.as_deref()),
                        }
                    });
                    match handle.await {
                        Ok(Ok(entries)) => entries_json(entries),
                        Ok(Err(e)) => internal_error(e),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_page = reg.clone();
        tokio::spawn(async move {
            reg_page
                .register("mdb_scan_range_entries_page", move |_cx, payload| async move {
                    let prefix = match parse_prefix(&payload) {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let start = match parse_hex_field(&payload, "start") {
                        Ok(k) => k,
                        Err(e) => return e,
                    };
                    let end = if payload.get("end").map(|v| !v.is_null()).unwrap_or(false) {
                        match parse_hex_field(&payload, "end") {
                            Ok(k) => Some(k),
                            Err(e) => return e,
                        }
                    } else {
                        None
                    };
                    let offset =
                        payload.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
                    let limit = (payload.get("limit").and_then(Value::as_u64).unwrap_or(100)
                        as usize)
                        .min(MAX_SCAN_ENTRIES);
                    let reverse = payload.get("reverse").and_then(Value::as_bool).unwrap_or(false);
                    let blockhash = match parse_opt_blockhash(&payload) {
                        Ok(b) => b,
                        Err(e) => return e,
                    };
                    let handle = tokio::task::spawn_blocking(move || {
                        let mdb = mdb_for(&prefix);
                        match blockhash {
                            Some(bh) => mdb.scan_range_entries_page_at_blockhash(
                                &bh,
                                &start,
                                end.as_deref(),
                                offset,
                                limit,
                                reverse,
                            ),
                            None => mdb.scan_range_entries_page(
                                &start,
                                end.as_deref(),
                                offset,
                                limit,
                                reverse,
                            ),
                        }
                    });
                    match handle.await {
                        Ok(Ok(entries)) => entries_json(entries),
                        Ok(Err(e)) => internal_error(e),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_bh = reg.clone();
        tokio::spawn(async move {
            reg_bh
                .register("tree_blockhash_for_height", move |_cx, payload| async move {
                    let Some(height) = payload.get("height").and_then(Value::as_u64) else {
                        return err("missing_field", "height (u32) is required");
                    };
                    let Ok(height) = u32::try_from(height) else {
                        return err("invalid_height", "height out of range");
                    };
                    let Some(tree) = get_global_tree_db() else {
                        return err("versioned_tree_unavailable", "tree db not initialized");
                    };
                    match tree.blockhash_for_height(height) {
                        Ok(bh) => json!({
                            "ok": true,
                            "height": height,
                            "blockhash": bh.map(|b| hex::encode(b.to_byte_array())),
                        }),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }

    {
        let reg_bounds = reg.clone();
        tokio::spawn(async move {
            reg_bounds
                .register("tree_indexed_height_bounds", move |_cx, _payload| async move {
                    let Some(tree) = get_global_tree_db() else {
                        return json!({ "ok": true, "min": Value::Null, "max": Value::Null });
                    };
                    match tree.indexed_height_bounds() {
                        Ok(Some((min, max))) => json!({ "ok": true, "min": min, "max": max }),
                        Ok(None) => json!({ "ok": true, "min": Value::Null, "max": Value::Null }),
                        Err(e) => internal_error(e),
                    }
                })
                .await;
        });
    }
}
