use crate::consts::TRACES_BY_BLOCK_PREFIX;
use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::{Context, Result};
use protobuf::Message;
use protobuf_json_mapping::parse_from_str;
use protobuf_json_mapping::print_to_string;
use rocksdb::{DB, Direction, IteratorMode, ReadOptions}; // bring the trait into scope
use serde_json::{Value, json};

use alkanes_support::proto::alkanes;

fn trace_block_prefix(block: &u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(7 + 8 + 1);
    v.extend_from_slice(TRACES_BY_BLOCK_PREFIX);
    v.extend_from_slice(&block.to_le_bytes());
    v.push(b'/');
    v
}
fn next_prefix(mut p: Vec<u8>) -> Option<Vec<u8>> {
    for i in (0..p.len()).rev() {
        if p[i] != 0xFF {
            p[i] += 1;
            p.truncate(i + 1);
            return Some(p);
        }
    }
    None
}
fn is_length_bucket(key: &[u8], prefix: &[u8]) -> bool {
    if key.len() < prefix.len() + 1 + 4 {
        return false;
    }
    let bucket = &key[prefix.len()..key.len() - 4];
    bucket == b"length"
}
pub fn collect_outpoints_for_block(db: &DB, block: &u64) -> Result<Vec<Vec<u8>>> {
    let prefix = trace_block_prefix(&block);
    let mut ro = ReadOptions::default();
    if let Some(ub) = next_prefix(prefix.clone()) {
        ro.set_iterate_upper_bound(ub);
    }
    ro.set_total_order_seek(true);

    let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
    let mut out = Vec::new();

    while let Some(Ok((k, v))) = it.next() {
        if !k.starts_with(&prefix) {
            break;
        }
        if is_length_bucket(&k, &prefix) {
            continue;
        }
        if v.len() == 4 && v[..] == [0x01, 0x00, 0x00, 0x00] {
            continue;
        }
        if v.len() < 36 {
            continue;
        }
        out.push(v[..36].to_vec());
    }
    Ok(out)
}

pub fn outpoint_bytes_to_key(bytes: &Vec<u8>) -> Vec<u8> {
    vec![
        TRACES_BY_BLOCK_PREFIX,
        &bytes.to_vec(),
        //extra padding cuz why tf not
        &vec![0x00, 0x00, 0x00, 0x00],
    ]
    .concat()
}

fn fmt_u128_hex(u: &alkanes::Uint128) -> String {
    let v = ((u.hi as u128) << 64) | (u.lo as u128);
    format!("0x{:x}", v)
}

/// Bytes -> 0x-hex
fn fmt_bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

/// Bytes -> String; if not valid UTF-8, fall back to 0x-hex
fn bytes_to_string_or_hex(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => fmt_bytes_hex(b),
    }
}

pub fn prettyify_protobuf_trace_json(input_json: &str) -> Result<String> {
    let trace: alkanes::AlkanesTrace =
        parse_from_str(input_json).context("parse AlkanesTrace from JSON")?;

    let mut out: Vec<Value> = Vec::with_capacity(trace.events.len() as usize);

    for ev in &trace.events {
        if let Some(event) = &ev.event {
            use alkanes::alkanes_trace_event::Event;
            match event {
                Event::EnterContext(enter) => {
                    // NOTE: enum wrapper -> unwrap

                    let typ = match enter.call_type.enum_value_or_default() {
                        alkanes::AlkanesTraceCallType::CALL => "call",
                        alkanes::AlkanesTraceCallType::DELEGATECALL => "delegatecall",
                        alkanes::AlkanesTraceCallType::STATICCALL => "staticcall",
                        _ => "unknown",
                    };

                    let ctx = enter.context.as_ref().context("enter.context missing")?;
                    let inner = ctx.inner.as_ref().context("enter.context.inner missing")?;

                    let myself = inner.myself.as_ref();
                    let caller = inner.caller.as_ref();

                    let my_block = myself.and_then(|m| m.block.as_ref());
                    let my_tx = myself.and_then(|m| m.tx.as_ref());
                    let caller_block = caller.and_then(|c| c.block.as_ref());
                    let caller_tx = caller.and_then(|c| c.tx.as_ref());

                    let inputs_hex: Vec<String> = inner.inputs.iter().map(fmt_u128_hex).collect();

                    let incoming_alkanes: Vec<Value> = inner
                        .incoming_alkanes
                        .iter()
                        .map(|t| {
                            let id = t.id.as_ref();
                            json!({
                                "id": {
                                    "block": id.and_then(|x| x.block.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    id.and_then(|x| x.tx.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "value": t.value.as_ref().map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                            })
                        })
                        .collect();

                    out.push(json!({
                        "event": "invoke",
                        "data": {
                            "type": typ,
                            "context": {
                                "myself": {
                                    "block": my_block.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    my_tx.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "caller": {
                                    "block": caller_block.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    "tx":    caller_tx.map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                },
                                "inputs": inputs_hex,
                                "incomingAlkanes": incoming_alkanes,
                                "vout": inner.vout,
                            },
                            "fuel": ctx.fuel,
                        }
                    }));
                }

                Event::ExitContext(exit) => {
                    let status = match exit.status.enum_value_or_default() {
                        alkanes::AlkanesTraceStatusFlag::FAILURE => "failure",
                        _ => "success",
                    };

                    let resp = exit.response.as_ref();

                    let alkanes_list: Vec<Value> = resp
                        .map(|r| {
                            r.alkanes.iter().map(|t| {
                                let id = t.id.as_ref();
                                json!({
                                    "id": {
                                        "block": id.and_then(|x| x.block.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                        "tx":    id.and_then(|x| x.tx.as_ref()).map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                    },
                                    "value": t.value.as_ref().map(fmt_u128_hex).unwrap_or_else(|| "0x0".to_string()),
                                })
                            }).collect()
                        })
                        .unwrap_or_default();

                    let storage_list: Vec<Value> = resp
                        .map(|r| {
                            r.storage
                                .iter()
                                .map(|kv| {
                                    let k = bytes_to_string_or_hex(&kv.key);
                                    let v = fmt_bytes_hex(&kv.value);
                                    json!({ "key": k, "value": v })
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let data_hex = resp
                        .map(|r| fmt_bytes_hex(&r.data))
                        .unwrap_or_else(|| "0x".to_string());

                    out.push(json!({
                        "event": "return",
                        "data": {
                            "status": status,
                            "response": {
                                "alkanes": alkanes_list,
                                "data": data_hex,
                                "storage": storage_list,
                            }
                        }
                    }));
                }

                Event::CreateAlkane(create) => {
                    let id = create.new_alkane.as_ref();
                    let block_hex = id
                        .and_then(|x| x.block.as_ref())
                        .map(fmt_u128_hex)
                        .unwrap_or_else(|| "0x0".to_string());
                    let tx_hex = id
                        .and_then(|x| x.tx.as_ref())
                        .map(fmt_u128_hex)
                        .unwrap_or_else(|| "0x0".to_string());
                    out.push(json!({
                        "event": "create",
                        "data": {
                            "newAlkane": { "block": block_hex, "tx": tx_hex }
                        }
                    }));
                }
                &_ => {} // future-proof for new variants
            }
        }
    }

    Ok(serde_json::to_string(&out).context("serialize normalized events")?)
}
fn outpoint_bytes_to_display(outpoint: &[u8]) -> String {
    let (txid_le, vout_le) = outpoint.split_at(32);
    let mut txid_be = txid_le.to_vec();
    txid_be.reverse();
    let vout = u32::from_le_bytes(vout_le.try_into().expect("vout 4 bytes"));
    format!("{}:{}", hex::encode(txid_be), vout)
}

// parse possibly-tailed trace (strip trailing u32 if needed)
fn parse_trace_maybe_with_trailer(mut bytes: Vec<u8>) -> Option<AlkanesTrace> {
    AlkanesTrace::parse_from_bytes(&bytes).ok().or_else(|| {
        if bytes.len() >= 4 {
            bytes.truncate(bytes.len() - 4);
            AlkanesTrace::parse_from_bytes(&bytes).ok()
        } else {
            None
        }
    })
}

pub fn traces_for_block(db: &DB, block: u64) -> Result<String> {
    // 1) outpoints for block
    let outpoints = collect_outpoints_for_block(db, &block)
        .with_context(|| format!("collect_outpoints_for_block({block}) failed"))?; // Vec<Vec<u8>>

    // 2) keys aligned 1:1 with outpoints
    let keys: Vec<Vec<u8>> = outpoints
        .iter()
        .map(|op| outpoint_bytes_to_key(op))
        .collect();

    // 3) multiget (order matches keys)
    let values = db.multi_get(keys.iter());

    // 4) build [{outpoint, events:[...]}...]
    let mut entries: Vec<Value> = Vec::with_capacity(outpoints.len());
    for (i, res) in values.into_iter().enumerate() {
        // 1. handle RocksDB errors
        let opt = match res {
            Ok(opt) => opt,
            Err(_) => continue, // skip this entry
        };

        // 2. handle missing value
        let dbv = match opt {
            Some(v) => v,
            None => continue, // skip if no value
        };

        // 3. parse trace
        let bytes: Vec<u8> = (&*dbv).to_vec(); // deref to [u8] then to_vec
        let Some(trace) = parse_trace_maybe_with_trailer(bytes) else {
            continue; // skip if unparsable
        };

        // 4. protobuf -> JSON
        let trace_json = print_to_string(&trace).context("ESPO: Failed to serialize trace")?;

        let pretty_events_str =
            prettyify_protobuf_trace_json(&trace_json).context("ESPO: Failed to prettify trace")?;

        let events_val: Value = serde_json::from_str(&pretty_events_str)
            .context("ESPO: prettified events invalid JSON")?;

        let out_label = outpoint_bytes_to_display(&outpoints[i]);

        entries.push(json!({
            "outpoint": out_label,
            "events": events_val
        }));
    }

    // 5) final JSON array string
    let final_json =
        serde_json::to_string(&entries).context("ESPO: failed to serialize final entries array")?;
    Ok(final_json)
}
