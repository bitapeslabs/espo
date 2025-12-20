use std::collections::{HashMap, HashSet};

use bitcoin::blockdata::script::Instruction;
use bitcoin::hashes::Hash;
use bitcoin::{opcodes, Address, Amount, Network, ScriptBuf, Transaction, Txid};
use maud::{Markup, PreEscaped, html};

use crate::alkanes::trace::{
    EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId, EspoSandshrewLikeTraceStatus,
    EspoTrace, prettyify_protobuf_trace_json,
};
use crate::explorer::components::svg_assets::{icon_arrow_bend_down_right, icon_caret_right};
use crate::explorer::consts::{ALKANE_ICON_BASE, ALKANE_ICON_OVERRIDES, ALKANE_NAME_OVERRIDES};
use crate::explorer::pages::common::{fmt_alkane_amount, fmt_amount};
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::utils::balances::OutpointLookup;
use crate::modules::essentials::utils::inspections::{StoredInspectionResult, load_inspection};
use crate::runtime::mdb::Mdb;
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

#[derive(Clone, Debug)]
struct ContractCallSummary {
    contract_id: SchemaAlkaneId,
    contract_name: ResolvedName,
    icon_url: String,
    method_name: Option<String>,
    opcode: Option<u128>,
    call_type: Option<String>,
    response_text: Option<String>,
    success: bool,
}

#[derive(Clone, Debug)]
struct ResolvedName {
    value: String,
    known: bool,
}

impl ResolvedName {
    fn fallback_letter(&self) -> char {
        if !self.known {
            return '?';
        }
        self.value
            .chars()
            .find(|c| !c.is_whitespace())
            .map(|c| c.to_ascii_uppercase())
            .unwrap_or('?')
    }
}

type InspectionCache = HashMap<SchemaAlkaneId, Option<StoredInspectionResult>>;
#[derive(Clone, Debug, Default)]
struct AlkaneKvMeta {
    name: Option<String>,
    symbol: Option<String>,
}
type AlkaneKvCache = HashMap<SchemaAlkaneId, AlkaneKvMeta>;
#[derive(Clone, Debug)]
struct AlkaneMetaDisplay {
    name: ResolvedName,
    symbol: String,
    icon_url: String,
}
const KV_KEY_NAME: &[u8] = b"/name";
const KV_KEY_SYMBOL: &[u8] = b"/symbol";

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

fn parse_u128_from_str(s: &str) -> Option<u128> {
    if let Some(hex) = s.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u128>().ok()
    }
}

fn parse_short_id_to_schema(id: &EspoSandshrewLikeTraceShortId) -> Option<SchemaAlkaneId> {
    fn parse_u32_or_hex(s: &str) -> Option<u32> {
        if let Some(hex) = s.strip_prefix("0x") {
            return u32::from_str_radix(hex, 16).ok();
        }
        s.parse::<u32>().ok()
    }
    fn parse_u64_or_hex(s: &str) -> Option<u64> {
        if let Some(hex) = s.strip_prefix("0x") {
            return u64::from_str_radix(hex, 16).ok();
        }
        s.parse::<u64>().ok()
    }

    let block = parse_u32_or_hex(&id.block)?;
    let tx = parse_u64_or_hex(&id.tx)?;
    Some(SchemaAlkaneId { block, tx })
}

fn trace_opcode(inputs: &[String]) -> Option<u128> {
    inputs.first().and_then(|s| parse_u128_from_str(s))
}

fn decode_trace_response(data_hex: &str) -> Option<String> {
    let hex_str = data_hex.strip_prefix("0x").unwrap_or(data_hex);
    if hex_str.is_empty() {
        return None;
    }
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&bytes).to_string();
    let trimmed = text.trim_matches('\u{0}').to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn kv_row_key(alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4 + 8 + 2 + skey.len());
    v.push(0x01);
    v.extend_from_slice(&alk.block.to_be_bytes());
    v.extend_from_slice(&alk.tx.to_be_bytes());
    let len = u16::try_from(skey.len()).unwrap_or(u16::MAX);
    v.extend_from_slice(&len.to_be_bytes());
    if len as usize != skey.len() {
        v.extend_from_slice(&skey[..(len as usize)]);
    } else {
        v.extend_from_slice(skey);
    }
    v
}

fn kv_utf8_value(alk: &SchemaAlkaneId, skey: &[u8], mdb: &Mdb) -> Option<String> {
    let key = kv_row_key(alk, skey);
    let raw = mdb.get(&key).ok().flatten()?;
    let value = if raw.len() >= 32 { &raw[32..] } else { raw.as_slice() };
    let s = std::str::from_utf8(value).ok()?.trim_matches('\0').to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn kv_meta_for_alkane(id: &SchemaAlkaneId, cache: &mut AlkaneKvCache, mdb: &Mdb) -> AlkaneKvMeta {
    if let Some(meta) = cache.get(id) {
        return meta.clone();
    }
    let meta = AlkaneKvMeta {
        name: kv_utf8_value(id, KV_KEY_NAME, mdb),
        symbol: kv_utf8_value(id, KV_KEY_SYMBOL, mdb),
    };
    cache.insert(*id, meta.clone());
    meta
}

fn lookup_inspection<'a>(
    id: &SchemaAlkaneId,
    cache: &'a mut InspectionCache,
    mdb: &Mdb,
) -> Option<&'a StoredInspectionResult> {
    if !cache.contains_key(id) {
        let loaded = load_inspection(mdb, id).ok().flatten();
        cache.insert(*id, loaded);
    }
    cache.get(id).and_then(|o| o.as_ref())
}

fn contract_display_name(
    id: &SchemaAlkaneId,
    inspection: Option<&StoredInspectionResult>,
    kv_meta: &AlkaneKvMeta,
) -> ResolvedName {
    if let Some(meta) = inspection.and_then(|i| i.metadata.as_ref()) {
        if !meta.name.trim().is_empty() {
            return ResolvedName { value: meta.name.clone(), known: true };
        }
    }
    if let Some(name) = kv_meta.name.as_ref() {
        if !name.trim().is_empty() {
            return ResolvedName { value: name.clone(), known: true };
        }
    }
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, name, _sym) in ALKANE_NAME_OVERRIDES {
        if *id_s == key {
            return ResolvedName { value: name.to_string(), known: true };
        }
    }
    ResolvedName { value: key, known: false }
}

fn method_display_name(opcode: u128, inspection: Option<&StoredInspectionResult>) -> Option<String> {
    let meta = inspection?.metadata.as_ref()?;
    meta.methods
        .iter()
        .find(|m| m.opcode == opcode)
        .map(|m| m.name.clone())
}

fn alkane_icon_url(id: &SchemaAlkaneId) -> String {
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, url) in ALKANE_ICON_OVERRIDES {
        if *id_s == key {
            return url.to_string();
        }
    }
    format!("{}/{}_{}.png", ALKANE_ICON_BASE, id.block, id.tx)
}

fn contract_icon_url(id: &SchemaAlkaneId) -> String {
    alkane_icon_url(id)
}

fn summarize_contract_call(
    trace: &EspoTrace,
    cache: &mut InspectionCache,
    kv_cache: &mut AlkaneKvCache,
    mdb: &Mdb,
) -> Option<ContractCallSummary> {
    let mut contract_id: Option<SchemaAlkaneId> = None;
    let mut opcode: Option<u128> = None;
    let mut call_type: Option<String> = None;
    let mut response_text: Option<String> = None;
    let mut any_failure = false;

    for ev in &trace.sandshrew_trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(data) => {
                if contract_id.is_none() {
                    contract_id = parse_short_id_to_schema(&data.context.myself);
                }
                if opcode.is_none() {
                    opcode = trace_opcode(&data.context.inputs);
                }
                if call_type.is_none() && !data.typ.is_empty() {
                    call_type = Some(data.typ.clone());
                }
            }
            EspoSandshrewLikeTraceEvent::Return(data) => {
                if matches!(data.status, EspoSandshrewLikeTraceStatus::Failure) {
                    any_failure = true;
                }
                if let Some(text) = decode_trace_response(&data.response.data) {
                    response_text = Some(text);
                }
            }
            EspoSandshrewLikeTraceEvent::Create(_) => {}
        }
    }

    let contract_id = contract_id?;
    let inspection = lookup_inspection(&contract_id, cache, mdb);
    let kv_meta = kv_meta_for_alkane(&contract_id, kv_cache, mdb);
    let contract_name = contract_display_name(&contract_id, inspection, &kv_meta);
    let method_name = opcode.and_then(|op| method_display_name(op, inspection));

    Some(ContractCallSummary {
        contract_id,
        contract_name,
        icon_url: contract_icon_url(&contract_id),
        method_name,
        opcode,
        call_type,
        response_text,
        success: !any_failure,
    })
}

fn render_trace_summary(summary: &ContractCallSummary) -> Markup {
    let alkane_path = format!("/alkane/{}:{}", summary.contract_id.block, summary.contract_id.tx);
    let status_class = if summary.success { "success" } else { "failure" };
    let status_text = match (summary.response_text.clone(), summary.success) {
        (Some(t), true) => t,
        (Some(t), false) => format!("Reverted: {t}"),
        (None, true) => "Call successful".to_string(),
        (None, false) => "Call reverted".to_string(),
    };
    let method_label =
        summary.method_name.clone().unwrap_or_else(|| "contract call".to_string());
    let fallback_letter = summary.contract_name.fallback_letter();

    html! {
        div class="trace-summary" {
            span class="trace-summary-label" { "Contract call:" }
            div class="trace-contract-row" {
                div class="trace-contract-icon" aria-hidden="true" {
                    img class="trace-contract-img" src=(summary.icon_url.clone()) alt=(summary.contract_name.value.clone()) loading="lazy" onerror="this.remove()" {}
                    span class="trace-icon-letter" { (fallback_letter) }
                }
                div class="trace-contract-meta" {
                    a class="trace-contract-name link" href=(alkane_path.clone()) { (summary.contract_name.value.clone()) }
                }
                span class="io-arrow" { (arrow_svg()) }
            }
            @if summary.method_name.is_some() || summary.opcode.is_some() {
                div class="trace-method-pill" {
                    span class="trace-method-name" { (method_label) }
                    @if let Some(op) = summary.opcode {
                        span class="trace-opcode" { (format!("opcode {}", op)) }
                    }
                }
            }
            div class=(format!("trace-status {}", status_class)) {
                span class="trace-status-icon" aria-hidden="true" { (icon_arrow_bend_down_right()) }
                span class="trace-status-text" { (status_text) }
            }
        }
    }
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
    essentials_mdb: &Mdb,
) -> Markup {
    let mut alkane_kv_cache: AlkaneKvCache = HashMap::new();
    let vins_markup = render_vins(tx, network, prev_map, outpoint_fn, &mut alkane_kv_cache, essentials_mdb);
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
        &mut alkane_kv_cache,
        essentials_mdb,
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
    alkane_kv_cache: &mut AlkaneKvCache,
    essentials_mdb: &Mdb,
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
                                (balances_list(&prevout_view.balances, alkane_kv_cache, essentials_mdb))
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
    alkane_kv_cache: &mut AlkaneKvCache,
    essentials_mdb: &Mdb,
) -> Markup {
    let tx_bytes = txid.to_byte_array();
    let tx_hex = txid.to_string();
    let mut inspection_cache: InspectionCache = HashMap::new();

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
                                    (render_op_return(&payload, o.value, is_protostone, protostone_json.as_ref(), &traces_for_vout, &mut inspection_cache, alkane_kv_cache, essentials_mdb))
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
                            (balances_list(&balances, alkane_kv_cache, essentials_mdb))
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
    inspection_cache: &mut InspectionCache,
    kv_cache: &mut AlkaneKvCache,
    essentials_mdb: &Mdb,
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
                    @for (idx, ((trace_raw, trace_parsed), trace)) in trace_views.iter().zip(traces.iter()).enumerate() {
                        @let label = format!("Alkanes Trace #{}", idx + 1);
                        @let summary = summarize_contract_call(*trace, inspection_cache, kv_cache, essentials_mdb);
                        div class="trace-view" {
                            @if let Some(s) = summary {
                                (render_trace_summary(&s))
                            }
                            details class="opret-toggle" {
                                summary class="opret-toggle-summary" {
                                    span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                    span class="opret-toggle-label" { (label) }
                                }
                                div class="opret-toggle-body" { (json_viewer(trace_parsed.as_ref(), trace_raw)) }
                            }
                        }
                    }
                    details class="opret-toggle" {
                        summary class="opret-toggle-summary" {
                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                            span class="opret-toggle-label" { "Protostone message" }
                        }
                        div class="opret-toggle-body" { (json_viewer(protostone_json, &fallback)) }
                    }
                }
            } @else {
                pre class="opret-body" { (fallback) }
            }
        }
    }
}

fn balances_list(
    entries: &[BalanceEntry],
    kv_cache: &mut AlkaneKvCache,
    essentials_mdb: &Mdb,
) -> Markup {
    if entries.is_empty() {
        return html! {};
    }
    html! {
        div class="io-alkanes" {
            @for be in entries {
                @let meta = alkane_meta(&be.alkane, kv_cache, essentials_mdb);
                @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                @let fallback_letter = meta.name.fallback_letter();
                div class="alk-line" {
                    span class="alk-arrow" aria-hidden="true" { (icon_arrow_bend_down_right()) }
                    div class="alk-icon-wrap" aria-hidden="true" {
                        img class="alk-icon-img" src=(meta.icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror="this.remove()" {}
                        span class="alk-icon-letter" { (fallback_letter) }
                    }
                    span class="alk-amt mono" { (fmt_alkane_amount(be.amount)) }
                    a class="alk-sym link mono" href=(format!("/alkane/{alk}")) { (meta.name.value.clone()) }
                }
            }
        }
    }
}

fn alkane_meta(
    id: &SchemaAlkaneId,
    kv_cache: &mut AlkaneKvCache,
    essentials_mdb: &Mdb,
) -> AlkaneMetaDisplay {
    let meta = kv_meta_for_alkane(id, kv_cache, essentials_mdb);
    if let Some(name) = meta.name.as_ref() {
        if !name.trim().is_empty() {
            let icon_url = alkane_icon_url(id);
            let sym = meta.symbol.clone().unwrap_or_else(|| name.clone());
            return AlkaneMetaDisplay {
                name: ResolvedName { value: name.clone(), known: true },
                symbol: sym,
                icon_url,
            };
        }
    }
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, name, sym) in ALKANE_NAME_OVERRIDES {
        if *id_s == key {
            let icon_url = alkane_icon_url(id);
            return AlkaneMetaDisplay {
                name: ResolvedName { value: name.to_string(), known: true },
                symbol: sym.to_string(),
                icon_url,
            };
        }
    }
    let icon_url = alkane_icon_url(id);
    AlkaneMetaDisplay {
        name: ResolvedName { value: key.clone(), known: false },
        symbol: key,
        icon_url,
    }
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
