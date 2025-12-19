use std::collections::{HashMap, HashSet};

use bitcoin::blockdata::script::Instruction;
use bitcoin::hashes::Hash;
use bitcoin::{opcodes, Address, Amount, Network, ScriptBuf, Transaction, Txid};
use maud::{Markup, PreEscaped, html};

use crate::alkanes::trace::{EspoTrace, prettyify_protobuf_trace_json};
use crate::explorer::components::svg_assets::{icon_arrow_bend_down_right, icon_caret_right};
use crate::explorer::consts::{ALKANE_ICON_BASE, ALKANE_NAME_OVERRIDES};
use crate::explorer::pages::common::{fmt_alkane_amount, fmt_amount};
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::utils::balances::OutpointLookup;
use crate::schemas::SchemaAlkaneId;
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use serde_json::{Value, json};

const ADDR_PREFIX_LEN: usize = 31;
const ADDR_SUFFIX_LEN: usize = 8;
const ADDR_TRUNCATE_MIN_LEN: usize = ADDR_PREFIX_LEN + ADDR_SUFFIX_LEN;

fn should_truncate_addr(addr: &str) -> bool {
    addr.len() > ADDR_TRUNCATE_MIN_LEN
}

fn addr_suffix(addr: &str) -> String {
    if !should_truncate_addr(addr) {
        return String::new();
    }
    let suffix_len = addr.len().saturating_sub(ADDR_PREFIX_LEN).min(ADDR_SUFFIX_LEN);
    if suffix_len == 0 {
        return String::new();
    }
    addr[addr.len().saturating_sub(suffix_len)..].to_string()
}

fn arrow_svg() -> Markup {
    html! {
        svg class="io-arrow-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" aria-hidden="true" {
            path d="M224.49,136.49l-72,72a12,12,0,0,1-17-17L187,140H40a12,12,0,0,1,0-24H187L135.51,64.48a12,12,0,0,1,17-17l72,72A12,12,0,0,1,224.49,136.49Z" fill="currentColor" {}
        }
    }
}

#[derive(Clone, Debug)]
struct OpReturnDecoded {
    data: Vec<u8>,
    has_runestone_magic: bool,
    pushdata_only: bool,
}

fn decode_op_return_payload(spk: &ScriptBuf) -> Option<OpReturnDecoded> {
    let mut instructions = spk.instructions();
    match instructions.next() {
        Some(Ok(Instruction::Op(opcodes::all::OP_RETURN))) => {}
        _ => return None,
    }

    let mut has_runestone_magic = false;
    let mut pushdata_only = true;
    let mut data: Vec<u8> = Vec::new();

    if let Some(next) = instructions.next() {
        match next {
            Ok(Instruction::Op(opcodes::all::OP_PUSHNUM_13)) => {
                has_runestone_magic = true;
            }
            Ok(Instruction::PushBytes(pb)) => data.extend_from_slice(pb.as_bytes()),
            Ok(Instruction::Op(_)) => pushdata_only = false,
            Err(_) => pushdata_only = false,
        }
    } else {
        return Some(OpReturnDecoded { data, has_runestone_magic, pushdata_only });
    }

    for instr in instructions {
        match instr {
            Ok(Instruction::PushBytes(pb)) => data.extend_from_slice(pb.as_bytes()),
            Ok(Instruction::Op(_)) => pushdata_only = false,
            Err(_) => pushdata_only = false,
        }
    }

    Some(OpReturnDecoded { data, has_runestone_magic, pushdata_only })
}

fn runestone_vout_indices(tx: &Transaction) -> HashSet<usize> {
    tx.output
        .iter()
        .enumerate()
        .filter_map(|(i, o)| {
            decode_op_return_payload(&o.script_pubkey)
                .filter(|p| p.has_runestone_magic && p.pushdata_only)
                .map(|_| i)
        })
        .collect()
}

fn protostone_json(tx: &Transaction) -> Option<Value> {
    let runestone = match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => r,
        _ => return None,
    };
    let protostones = Protostone::from_runestone(&runestone).ok()?;
    if protostones.is_empty() {
        return None;
    }

    let view: Vec<_> = protostones
        .into_iter()
        .map(|p| {
            let utf8 = String::from_utf8(p.message.clone()).ok();
            let edicts: Vec<_> = p
                .edicts
                .into_iter()
                .map(|e| {
                    json!({
                        "id": { "block": e.id.block, "tx": e.id.tx },
                        "amount": e.amount,
                        "output": e.output,
                    })
                })
                .collect();
            json!({
                "protocol_tag": p.protocol_tag,
                "burn": p.burn,
                "pointer": p.pointer,
                "refund": p.refund,
                "from": p.from,
                "message_hex": hex::encode(&p.message),
                "message_utf8": utf8,
                "edicts": edicts,
            })
        })
        .collect();
    Some(json!(view))
}

fn opreturn_utf8(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

/// Render a transaction with VINs, VOUTs, and optional trace details.
pub fn render_tx(
    txid: &Txid,
    tx: &Transaction,
    traces: Option<&[EspoTrace]>,
    network: Network,
    prev_map: &HashMap<Txid, Transaction>,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
    outspends_fn: &dyn Fn(&Txid) -> Vec<Option<Txid>>,
) -> Markup {
    let vins_markup = render_vins(tx, network, prev_map, outpoint_fn);
    let outspends = outspends_fn(txid);
    let protostone_json = protostone_json(tx);
    let runestone_vouts = runestone_vout_indices(tx);
    let vouts_markup = render_vouts(
        txid,
        tx,
        network,
        outpoint_fn,
        &outspends,
        traces,
        &protostone_json,
        &runestone_vouts,
    );

    html! {
        div class="card tx-card" {
            span class="mono tx-title" { a class="link" href=(format!("/tx/{}", txid)) { (txid) } }
            div class="tx-io-grid" {
                div class="io-col" { (vins_markup) }
                div class="io-col" { (vouts_markup) }
            }
        }
    }
}

fn render_vins(
    tx: &Transaction,
    network: Network,
    prev_map: &HashMap<Txid, Transaction>,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
) -> Markup {
    html! {
        @if tx.input.is_empty() {
            p class="muted" { "No inputs" }
        } @else {
            div class="io-list" {
                @for vin in tx.input.iter() {
                    @if vin.previous_output.is_null() {
                        div class="io-row" {
                            span class="io-arrow in" title="Coinbase input" { (arrow_svg()) }
                            div class="io-main" {
                                div class="io-addr-row" {
                                    div class="io-addr mono" { "Coinbase" }
                                    div class="io-amount muted" { "—" }
                                }
                            }
                        }
                    } @else {
                        @let prev_txid = vin.previous_output.txid;
                        @let prev_vout = vin.previous_output.vout;
                        @let prevout = prev_map.get(&prev_txid).and_then(|ptx| ptx.output.get(prev_vout as usize));
                        @let prevout_view = outpoint_fn(&prev_txid, prev_vout);
                        div class="io-row" {
                            a class="io-arrow io-arrow-link in" href=(format!("/tx/{}", prev_txid)) title="View previous transaction" { (arrow_svg()) }
                            div class="io-main" {
                                @match prevout {
                                    Some(po) => {
                                        @let addr_opt = Address::from_script(po.script_pubkey.as_script(), network).ok();
                                        div class="io-addr-row" {
                                            div class="io-addr" {
                                                @match addr_opt {
                                                    Some(a) => {
                                                        @let addr = a.to_string();
                                                        a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                            @let truncate_addr = should_truncate_addr(&addr);
                                                            @if truncate_addr {
                                                                @let addr_suffix = addr_suffix(&addr);
                                                                span class="addr-prefix-wrap" {
                                                                    span class="addr-prefix" { (addr) }
                                                                }
                                                                @if !addr_suffix.is_empty() {
                                                                    span class="addr-suffix" { (addr_suffix) }
                                                                }
                                                            } @else {
                                                                span class="addr-full" { (addr) }
                                                            }
                                                        }
                                                    }
                                                    None => span class="mono muted" { "unknown" },
                                                }
                                            }
                                            div class="io-amount muted" { (fmt_amount(po.value)) }
                                        }
                                    }
                                    None => {
                                        div class="io-addr-row" {
                                            div class="io-addr muted" { "prevout unavailable" }
                                            div class="io-amount muted" { "—" }
                                        }
                                    }
                                }
                                (balances_list(&prevout_view.balances))
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_vouts(
    txid: &Txid,
    tx: &Transaction,
    network: Network,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
    outspends: &[Option<Txid>],
    traces: Option<&[EspoTrace]>,
    protostone_json: &Option<Value>,
    runestone_vouts: &HashSet<usize>,
) -> Markup {
    let tx_bytes = txid.to_byte_array();
    let tx_hex = txid.to_string();

    html! {
        @if tx.output.is_empty() {
            p class="muted" { "No outputs" }
        } @else {
            div class="io-list" {
                @for (vout, o) in tx.output.iter().enumerate() {
                    @let OutpointLookup { balances, spent_by: db_spent } = outpoint_fn(txid, vout as u32);
                    @let spent_by = outspends.get(vout).cloned().flatten().or(db_spent);
                    @let opret = decode_op_return_payload(&o.script_pubkey);
                    div class="io-row" {
                        div class="io-main" {
                            @match opret {
                                Some(payload) => {
                                    @let is_protostone = runestone_vouts.contains(&vout) && protostone_json.is_some();
                                    @let traces_for_vout: Vec<&EspoTrace> = traces.map(|ts| {
                                        let matches: Vec<&EspoTrace> = ts
                                            .iter()
                                            .filter(|t| {
                                                if t.outpoint.vout != vout as u32 {
                                                    return false;
                                                }
                                                let bytes_match = t.outpoint.txid.as_slice() == tx_bytes.as_slice();
                                                let parsed_match = Txid::from_slice(&t.outpoint.txid)
                                                    .map(|tid| tid == *txid)
                                                    .unwrap_or(false);
                                                let string_match = t.sandshrew_trace.outpoint == format!("{tx_hex}:{vout}");
                                                bytes_match || parsed_match || string_match
                                            })
                                            .collect();
                                        if !matches.is_empty() { matches } else { ts.iter().collect() }
                                    }).unwrap_or_default();
                                    (render_op_return(&payload, o.value, is_protostone, protostone_json.as_ref(), &traces_for_vout))
                                }
                                None => {
                                    @let addr_opt = Address::from_script(o.script_pubkey.as_script(), network).ok();
                                    div class="io-addr-row" {
                                        div class="io-addr" {
                                            @match addr_opt {
                                                Some(a) => {
                                                    @let addr = a.to_string();
                                                    a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                        @let truncate_addr = should_truncate_addr(&addr);
                                                        @if truncate_addr {
                                                            @let addr_suffix = addr_suffix(&addr);
                                                            span class="addr-prefix-wrap" {
                                                                span class="addr-prefix" { (addr) }
                                                            }
                                                            @if !addr_suffix.is_empty() {
                                                                span class="addr-suffix" { (addr_suffix) }
                                                            }
                                                        } @else {
                                                            span class="addr-full" { (addr) }
                                                        }
                                                    }
                                                }
                                                None => span class="mono muted" { "non-standard" },
                                            }
                                        }
                                        div class="io-amount mono muted" { (fmt_amount(o.value)) }
                                    }
                                }
                            }
                            (balances_list(&balances))
                        }
                        @match spent_by {
                            Some(spender) => a class="io-arrow io-arrow-link out spent" href=(format!("/tx/{}", spender)) title="Spent by transaction" { (arrow_svg()) },
                            None => span class="io-arrow out" title="Unspent output" { (arrow_svg()) },
                        }
                    }
                }
            }
        }
    }
}

fn render_op_return(
    payload: &OpReturnDecoded,
    amount: Amount,
    is_protostone: bool,
    protostone_json: Option<&Value>,
    traces: &[&EspoTrace],
) -> Markup {
    let fallback = opreturn_utf8(&payload.data);
    let trace_views: Vec<(String, Option<Value>)> = traces
        .iter()
        .map(|t| {
            let raw = prettyify_protobuf_trace_json(&t.protobuf_trace).unwrap_or_else(|_| "[]".to_string());
            let parsed = serde_json::from_str::<Value>(&raw).ok();
            (raw, parsed)
        })
        .collect();

    html! {
        div class="io-addr-row opret-row" {
            details class="io-opret" {
                summary class="opret-summary" {
                    span class="opret-left" {
                        span class="opret-caret" aria-hidden="true" { (icon_caret_right()) }
                        span class="opret-title mono" { "OP_RETURN" }
                        @if is_protostone {
                            span class="opret-meta" {
                                "("
                                span class="opret-diamond" aria-hidden="true" {}
                                " Protostone message)"
                            }
                        }
                    }
                    span class="io-amount mono muted" { (fmt_amount(amount)) }
                }
            }
            @if is_protostone {
                div class="opret-body protostone-body" {
                    details class="opret-toggle" open {
                        summary class="opret-toggle-summary" {
                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                            span class="opret-toggle-label" { "Protostone message" }
                        }
                        div class="opret-toggle-body" { (json_viewer(protostone_json, &fallback)) }
                    }
                    @for (idx, (trace_raw, trace_parsed)) in trace_views.iter().enumerate() {
                        @let label = format!("Alkanes Trace #{}", idx + 1);
                        details class="opret-toggle" {
                            summary class="opret-toggle-summary" {
                                span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                span class="opret-toggle-label" { (label) }
                            }
                            div class="opret-toggle-body" { (json_viewer(trace_parsed.as_ref(), trace_raw)) }
                        }
                    }
                }
            } @else {
                pre class="opret-body" { (fallback) }
            }
        }
    }
}

fn balances_list(entries: &[BalanceEntry]) -> Markup {
    if entries.is_empty() {
        return html! {};
    }
    html! {
        div class="io-alkanes" {
            @for be in entries {
                @let (name, sym, icon) = alkane_meta(&be.alkane);
                @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                div class="alk-line" {
                    span class="alk-arrow" aria-hidden="true" { (icon_arrow_bend_down_right()) }
                    img class="alk-icon" src=(icon) alt=(sym.clone()) {}
                    span class="alk-amt mono" { (fmt_alkane_amount(be.amount)) }
                    a class="alk-sym link mono" href=(format!("/alkane/{alk}")) { (name) }
                }
            }
        }
    }
}

fn alkane_meta(id: &SchemaAlkaneId) -> (String, String, String) {
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, name, sym) in ALKANE_NAME_OVERRIDES {
        if *id_s == key {
            let icon = format!("{}/{}_{}.png", ALKANE_ICON_BASE, id.block, id.tx);
            return (name.to_string(), sym.to_string(), icon);
        }
    }
    let icon = format!("{}/{}_{}.png", ALKANE_ICON_BASE, id.block, id.tx);
    (key.clone(), key, icon)
}

fn json_viewer(value: Option<&Value>, raw: &str) -> Markup {
    match value {
        Some(v) => {
            let mut buf = String::new();
            render_json_value(v, 0, &mut buf);
            html! {
                div class="json-viewer json-only" {
                    pre class="json-raw" { (PreEscaped(buf)) }
                }
            }
        }
        None => {
            html! { pre class="json-raw" { (raw) } }
        }
    }
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_json_value(v: &Value, depth: usize, out: &mut String) {
    let indent = "  ".repeat(depth);
    let next_indent = "  ".repeat(depth + 1);
    match v {
        Value::Null => out.push_str(r#"<span class="jv-val null">null</span>"#),
        Value::Bool(b) => {
            out.push_str(r#"<span class="jv-val boolean">"#);
            out.push_str(if *b { "true" } else { "false" });
            out.push_str("</span>");
        }
        Value::Number(n) => {
            out.push_str(r#"<span class="jv-val number">"#);
            out.push_str(&escape_html(&n.to_string()));
            out.push_str("</span>");
        }
        Value::String(s) => {
            let json_str = serde_json::to_string(s).unwrap_or_else(|_| format!("{:?}", s));
            out.push_str(r#"<span class="jv-val string">"#);
            out.push_str(&escape_html(&json_str));
            out.push_str("</span>");
        }
        Value::Array(arr) => {
            out.push_str(r#"<span class="jv-brace">[</span>"#);
            if !arr.is_empty() {
                out.push('\n');
                for (i, item) in arr.iter().enumerate() {
                    out.push_str(&next_indent);
                    render_json_value(item, depth + 1, out);
                    if i + 1 != arr.len() {
                        out.push_str(r#"<span class="jv-comma">,</span>"#);
                    }
                    out.push('\n');
                }
                out.push_str(&indent);
            }
            out.push_str(r#"<span class="jv-brace">]</span>"#);
        }
        Value::Object(map) => {
            out.push_str(r#"<span class="jv-brace">{</span>"#);
            if !map.is_empty() {
                out.push('\n');
                let len = map.len();
                for (idx, (k, val)) in map.iter().enumerate() {
                    out.push_str(&next_indent);
                    let key_escaped =
                        escape_html(&serde_json::to_string(k).unwrap_or_else(|_| format!("{:?}", k)));
                    out.push_str(r#"<span class="jv-key">"#);
                    out.push_str(&key_escaped);
                    out.push_str(r#"</span><span class="jv-sep">: </span>"#);
                    render_json_value(val, depth + 1, out);
                    if idx + 1 != len {
                        out.push_str(r#"<span class="jv-comma">,</span>"#);
                    }
                    out.push('\n');
                }
                out.push_str(&indent);
            }
            out.push_str(r#"<span class="jv-brace">}</span>"#);
        }
    }
}
