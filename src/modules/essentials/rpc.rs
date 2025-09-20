use crate::modules::defs::RpcNsRegistrar;
use crate::modules::essentials::main::Essentials; // use associated fns like Essentials::k_dir
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use serde_json::map::Map;
use serde_json::{Value, json};

pub fn register_rpc(reg: RpcNsRegistrar, mdb: Mdb) {
    eprintln!("[RPC_ESSENTIALS] registering RPC handlers…");

    // GET all latest keys for an AlkaneId
    // payload: {
    //   "alkane": "2:68441" | "0x2:0x10b59",
    //   "try_decode_utf8": true | false   // optional, default true; controls top-level key names only
    // }
    let reg_get = reg.clone();
    let mdb_get = mdb.clone();
    tokio::spawn(async move {
        reg_get
            .register("get_keys", move |_cx, payload| {
                let mdb = mdb_get.clone();
                async move {
                    // parse alkane id
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

                    // whether to try UTF-8 for top-level key names
                    let try_decode_utf8 = payload
                        .get("try_decode_utf8")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);

                    // Directory of keys for this alkane
                    let dir_key = Essentials::k_dir(&alk);
                    let dir: Vec<Vec<u8>> = match mdb.get(&dir_key) {
                        Ok(Some(b)) => Essentials::dec_dir(&b).unwrap_or_default(),
                        Ok(None) => Vec::<Vec<u8>>::new(),
                        Err(e) => {
                            eprintln!("[RPC:essentials.get_keys] rocksdb get dir failed: {e:?}");
                            Vec::<Vec<u8>>::new()
                        }
                    };

                    // Build an object: { "<key>": {key_hex, key_str, value_hex, value_str, value_u128}, ... }
                    let mut items: Map<String, Value> = Map::with_capacity(dir.len());
                    for k in dir.iter() {
                        let kv_key = Essentials::k_kv(&alk, k);
                        let value_bytes_opt = match mdb.get(&kv_key) {
                            Ok(Some(v)) => Some(v),
                            _ => None,
                        };

                        let key_hex = fmt_bytes_hex(k);
                        let key_str_val = utf8_or_null(k);

                        let (value_hex, value_str_val, value_u128_val) = if let Some(v) = value_bytes_opt.as_deref() {
                            (fmt_bytes_hex(v), utf8_or_null(v), u128_be_or_null(v))
                        } else {
                            ("0x".to_string(), Value::Null, Value::Null)
                        };

                        // choose top-level key name
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
                                "key_hex":   key_hex,
                                "key_str":   key_str_val,
                                "value_hex": value_hex,
                                "value_str": value_str_val,
                                "value_u128": value_u128_val
                            }),
                        );
                    }

                    json!({
                        "ok": true,
                        "alkane": format!("{}:{}", alk.block, alk.tx),
                        "count": items.len(),
                        "items": items
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

// hex "0x..."
pub fn fmt_bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

// UTF-8 if valid else null
fn utf8_or_null(b: &[u8]) -> Value {
    match std::str::from_utf8(b) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => Value::Null,
    }
}

// Big-endian parse into u128 (if len <= 16), returned as decimal string to preserve precision.
// If >16 bytes, return null.
fn u128_be_or_null(b: &[u8]) -> Value {
    if b.len() > 16 {
        return Value::Null;
    }
    let mut acc: u128 = 0;
    for &byte in b {
        acc = (acc << 8) | (byte as u128);
    }
    // represent as decimal string (JSON numbers can’t safely hold full u128)
    Value::String(acc.to_string())
}
