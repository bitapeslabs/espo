use crate::modules::defs::RpcNsRegistrar;
use crate::modules::essentials::main::Essentials; // for Essentials::k_kv
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use serde_json::map::Map;
use serde_json::{Value, json};

/* ---------------- register ---------------- */

pub fn register_rpc(reg: RpcNsRegistrar, mdb: Mdb) {
    eprintln!("[RPC_ESSENTIALS] registering RPC handlers…");

    let reg_get = reg.clone();
    let mdb_get = mdb.clone();
    tokio::spawn(async move {
        reg_get
            .register("get_keys", move |_cx, payload| {
                let mdb = mdb_get.clone();
                async move {
                    // ---- parse alkane id
                    let alk = match payload
                        .get("alkane")
                        .and_then(|v| v.as_str())
                        .and_then(parse_alkane_from_str)
                    {
                        Some(a) => a,
                        None => {
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
                    let limit = limit_req.min(1000).max(1) as usize;

                    let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(1).max(1) as usize;

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
                                eprintln!("[RPC:essentials.get_keys] scan_prefix failed: {e:?}");
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
                                        u128_be_or_null(raw),
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
                                "last_txid":  last_txid_val   // plain hex string or null
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

    // simple ping
    let reg_ping = reg.clone();
    tokio::spawn(async move {
        reg_ping
            .register("ping", |_cx, _payload| async move { Value::String("pong".to_string()) })
            .await;
    });
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

fn u128_be_or_null(b: &[u8]) -> Value {
    if b.len() > 16 {
        return Value::Null;
    }
    let mut acc: u128 = 0;
    for &byte in b {
        acc = (acc << 8) | (byte as u128);
    }
    Value::String(acc.to_string())
}
