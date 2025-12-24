use std::collections::{HashMap, HashSet};

use bitcoin::blockdata::script::Instruction;
use bitcoin::hashes::Hash;
use bitcoin::{Address, Amount, Network, ScriptBuf, Transaction, Txid, opcodes};
use maud::{Markup, PreEscaped, html};

use crate::alkanes::trace::{
    EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId, EspoSandshrewLikeTraceStatus,
    EspoTrace, prettyify_protobuf_trace_json,
};
use crate::explorer::components::svg_assets::{
    arrow_svg, icon_arrow_bend_down_right, icon_caret_right, icon_magic_wand,
};
use crate::explorer::consts::{
    ALKANE_ICON_BASE, ALKANE_ICON_FALLBACK_BASE, alkane_contract_name_overrides,
    alkane_icon_overrides, alkane_name_overrides,
};
use crate::explorer::pages::common::{fmt_alkane_amount, fmt_amount};
use crate::modules::essentials::storage::{BalanceEntry, load_creation_record};
use crate::modules::essentials::utils::balances::OutpointLookup;
use crate::modules::essentials::utils::inspections::{StoredInspectionResult, load_inspection};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use serde_json::{Value, json};

const ADDR_SUFFIX_LEN: usize = 8;

#[derive(Clone, Debug)]
pub struct TxPill {
    pub label: String,
    pub tone: TxPillTone,
}

#[derive(Clone, Debug)]
pub enum TxPillTone {
    Success,
    Danger,
}

fn addr_prefix_suffix(addr: &str) -> (String, String) {
    let suffix_len = addr.len().min(ADDR_SUFFIX_LEN);
    let split_at = addr.len().saturating_sub(suffix_len);
    let prefix = addr[..split_at].to_string();
    let suffix = addr[split_at..].to_string();
    (prefix, suffix)
}

#[derive(Clone, Debug)]
struct OpReturnDecoded {
    data: Vec<u8>,
    has_runestone_magic: bool,
    pushdata_only: bool,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct ContractCallSummary {
    contract_id: SchemaAlkaneId,
    factory_id: Option<SchemaAlkaneId>,
    factory_template: Option<SchemaAlkaneId>,
    factory_created: Option<SchemaAlkaneId>,
    factory_created_meta: Option<AlkaneMetaDisplay>,
    link_id: SchemaAlkaneId,
    contract_name: ResolvedName,
    icon_url: String,
    method_name: Option<String>,
    opcode: Option<u128>,
    call_type: Option<String>,
    response_text: Option<String>,
    success: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedName {
    pub value: String,
    pub known: bool,
}

impl ResolvedName {
    pub fn fallback_letter(&self) -> char {
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
pub(crate) struct AlkaneImplMeta {
    implementation: Option<Option<SchemaAlkaneId>>,
}
pub(crate) type AlkaneImplCache = HashMap<SchemaAlkaneId, AlkaneImplMeta>;
pub(crate) type AlkaneMetaCache = HashMap<SchemaAlkaneId, AlkaneMetaDisplay>;
#[derive(Clone, Debug)]
pub(crate) struct AlkaneMetaDisplay {
    pub name: ResolvedName,
    pub symbol: String,
    pub icon_url: String,
}
const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
const UPGRADEABLE_METHODS: [(&str, u128); 3] =
    [("initialize", 32767), ("upgrade", 32766), ("forward", 36863)];
const UPGRADEABLE_NAME: &str = "Upgradeable";
const UPGRADEABLE_NAME_ALT: &str = "Upgradable";

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

fn parse_factory_clone(
    inputs: Option<&Vec<String>>,
    created: Option<SchemaAlkaneId>,
) -> Option<(SchemaAlkaneId, Option<SchemaAlkaneId>)> {
    let inputs = inputs?;
    if inputs.len() < 2 {
        return None;
    }
    let header = parse_u128_from_str(&inputs[0])?;
    let n = parse_u128_from_str(&inputs[1])?;
    let template = match header {
        5 => SchemaAlkaneId { block: 2, tx: n as u64 },
        6 => SchemaAlkaneId { block: 3, tx: n as u64 },
        _ => return None,
    };
    Some((template, created))
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
    if trimmed.is_empty() { None } else { Some(trimmed) }
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

fn decode_kv_implementation(raw: &[u8]) -> Option<SchemaAlkaneId> {
    if raw.len() < 32 {
        return None;
    }
    let block_bytes: [u8; 16] = raw[0..16].try_into().ok()?;
    let tx_bytes: [u8; 16] = raw[16..32].try_into().ok()?;
    let block = u128::from_le_bytes(block_bytes);
    let tx = u128::from_le_bytes(tx_bytes);
    if block > u32::MAX as u128 || tx > u64::MAX as u128 {
        return None;
    }
    Some(SchemaAlkaneId { block: block as u32, tx: tx as u64 })
}

fn kv_implementation_value(
    alk: &SchemaAlkaneId,
    cache: &mut AlkaneImplCache,
    mdb: &Mdb,
) -> Option<SchemaAlkaneId> {
    if let Some(meta) = cache.get(alk) {
        if let Some(stored) = &meta.implementation {
            return *stored;
        }
    }
    let mut meta = cache.get(alk).cloned().unwrap_or_default();
    let implementation =
        mdb.get(&kv_row_key(alk, KV_KEY_IMPLEMENTATION)).ok().flatten().and_then(|raw| {
            if raw.len() >= 32 {
                decode_kv_implementation(&raw[32..])
            } else {
                decode_kv_implementation(&raw)
            }
        });
    meta.implementation = Some(implementation);
    cache.insert(*alk, meta.clone());
    implementation
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

fn is_upgradeable_proxy(inspection: &StoredInspectionResult) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    let name_matches = meta.name.eq_ignore_ascii_case(UPGRADEABLE_NAME)
        || meta.name.eq_ignore_ascii_case(UPGRADEABLE_NAME_ALT);
    if !name_matches {
        return false;
    }
    UPGRADEABLE_METHODS.iter().all(|(name, opcode)| {
        meta.methods
            .iter()
            .any(|m| m.name.eq_ignore_ascii_case(name) && m.opcode == *opcode)
    })
}

fn contract_name_override(id: &SchemaAlkaneId) -> Option<String> {
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, name) in alkane_contract_name_overrides() {
        if *id_s == key {
            return Some(name.to_string());
        }
    }
    None
}

fn upgradeable_proxy_target(
    id: &SchemaAlkaneId,
    inspection: Option<&StoredInspectionResult>,
    trace: &EspoTrace,
    impl_cache: &mut AlkaneImplCache,
    mdb: &Mdb,
) -> Option<SchemaAlkaneId> {
    // Prefer storage changes in the trace; fall back to the DB.
    if let Some(kvs) = trace.storage_changes.get(id) {
        for (skey, (_txid, value)) in kvs {
            if skey.as_slice() == KV_KEY_IMPLEMENTATION {
                if let Some(decoded) = decode_kv_implementation(value) {
                    if inspection.map_or(false, is_upgradeable_proxy) {
                        return Some(decoded);
                    }
                }
            }
        }
    }
    let decoded = kv_implementation_value(id, impl_cache, mdb);
    if decoded.is_some() && inspection.map_or(false, is_upgradeable_proxy) {
        return decoded;
    }
    None
}

fn contract_display_name(
    id: &SchemaAlkaneId,
    inspection: Option<&StoredInspectionResult>,
    meta_cache: &mut AlkaneMetaCache,
    mdb: &Mdb,
) -> ResolvedName {
    if let Some(name) = contract_name_override(id) {
        return ResolvedName { value: name, known: true };
    }
    if let Some(meta) = inspection.and_then(|i| i.metadata.as_ref()) {
        if !meta.name.trim().is_empty() {
            return ResolvedName { value: meta.name.clone(), known: true };
        }
    }

    let meta = alkane_meta(id, meta_cache, mdb);
    meta.name
}

fn method_display_name(
    opcode: u128,
    inspection: Option<&StoredInspectionResult>,
) -> Option<String> {
    let meta = inspection?.metadata.as_ref()?;
    meta.methods.iter().find(|m| m.opcode == opcode).map(|m| m.name.clone())
}

pub(crate) fn alkane_icon_url(id: &SchemaAlkaneId) -> String {
    icon_url_with_base(id, ALKANE_ICON_BASE)
}

pub(crate) fn alkane_icon_fallback_url(id: &SchemaAlkaneId) -> String {
    icon_url_with_base(id, ALKANE_ICON_FALLBACK_BASE)
}

fn icon_url_with_base(id: &SchemaAlkaneId, base: &str) -> String {
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, url) in alkane_icon_overrides() {
        if *id_s == key {
            return url.to_string();
        }
    }
    format!("{}/{}_{}", base, id.block, id.tx)
}

pub(crate) fn icon_onerror(fallback_url: &str) -> String {
    format!(
        "if(!this.dataset.fallback){{this.dataset.fallback=1;this.src='{}';}}else{{this.remove();}}",
        fallback_url
    )
}

fn contract_icon_url(id: &SchemaAlkaneId) -> String {
    alkane_icon_url(id)
}

fn summarize_contract_call(
    trace: &EspoTrace,
    cache: &mut InspectionCache,
    meta_cache: &mut AlkaneMetaCache,
    impl_cache: &mut AlkaneImplCache,
    mdb: &Mdb,
) -> Option<ContractCallSummary> {
    let mut contract_id: Option<SchemaAlkaneId> = None;
    let mut first_invoke_inputs: Option<Vec<String>> = None;
    let mut created_alkane: Option<SchemaAlkaneId> = None;
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
                if first_invoke_inputs.is_none() {
                    first_invoke_inputs = Some(data.context.inputs.clone());
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
            EspoSandshrewLikeTraceEvent::Create(c) => {
                if created_alkane.is_none() {
                    created_alkane = parse_short_id_to_schema(&c);
                }
            }
        }
    }

    let contract_id = contract_id?;
    let contract_inspection = lookup_inspection(&contract_id, cache, mdb).cloned();
    let proxy_template = upgradeable_proxy_target(
        &contract_id,
        contract_inspection.as_ref(),
        trace,
        impl_cache,
        mdb,
    );
    let (active_id, active_inspection, using_proxy_template) = match proxy_template {
        Some(target) => {
            let insp = lookup_inspection(&target, cache, mdb).cloned();
            (target, insp, true)
        }
        None => (contract_id, contract_inspection.clone(), false),
    };
    let (inspection_factory_id, contract_name, mut method_name, mut icon_id) = {
        let inspection = active_inspection.as_ref();
        let name = contract_display_name(&active_id, inspection, meta_cache, mdb);
        let mname = opcode.and_then(|op| method_display_name(op, inspection));
        let icon = inspection.and_then(|i| i.factory_alkane).unwrap_or(active_id);
        let factory = inspection.and_then(|i| i.factory_alkane);
        (factory, name, mname, icon)
    };
    if using_proxy_template {
        icon_id = contract_id;
    }
    let mut factory_pair = parse_factory_clone(first_invoke_inputs.as_ref(), created_alkane);
    if factory_pair.is_none() {
        if let (Some(factory_id), Some(created)) = (inspection_factory_id, created_alkane) {
            if factory_id != contract_id || created != contract_id {
                factory_pair = Some((factory_id, Some(created)));
            }
        }
    }
    let mut link_id = active_id;
    let mut effective_name = contract_name;
    let mut created_meta: Option<AlkaneMetaDisplay> = None;
    if let Some((template_id, _)) = factory_pair {
        // Show the factory/template metadata for factory clones
        let template_inspection = lookup_inspection(&template_id, cache, mdb);
        effective_name = contract_display_name(&template_id, template_inspection, meta_cache, mdb);
        let template_method = opcode.and_then(|op| method_display_name(op, template_inspection));
        method_name = template_method.or(method_name);
        icon_id = template_id;
        link_id = template_id;
    }
    if let Some(created) = factory_pair.as_ref().and_then(|(_, c)| *c) {
        created_meta = Some(alkane_meta(&created, meta_cache, mdb));
    }
    if using_proxy_template {
        if let Some(name) = contract_name_override(&contract_id) {
            effective_name = ResolvedName { value: name, known: true };
        }
    }

    Some(ContractCallSummary {
        contract_id,
        factory_id: inspection_factory_id,
        factory_template: factory_pair.as_ref().map(|(t, _)| *t),
        factory_created: factory_pair.as_ref().and_then(|(_, c)| *c),
        factory_created_meta: created_meta,
        link_id,
        contract_name: effective_name,
        icon_url: contract_icon_url(&icon_id),
        method_name,
        opcode,
        call_type,
        response_text,
        success: !any_failure,
    })
}

fn render_trace_summary(summary: &ContractCallSummary) -> Markup {
    let is_factory_clone = summary.factory_template.is_some();
    let link_id = summary.link_id;
    let alkane_path = format!("/alkane/{}:{}", link_id.block, link_id.tx);
    let status_class = if summary.success { "success" } else { "failure" };
    let status_text = match (summary.response_text.clone(), summary.success) {
        (Some(_), true) => "Call successful".to_string(),
        (Some(t), false) => format!("Reverted: {t}"),
        (None, true) => "Call successful".to_string(),
        (None, false) => "Call reverted".to_string(),
    };
    let mut method_label =
        summary.method_name.clone().unwrap_or_else(|| "contract call".to_string());
    if is_factory_clone {
        method_label = "factory clone".to_string();
    }
    let fallback_letter = summary.contract_name.fallback_letter();
    let fallback_icon_url = alkane_icon_fallback_url(&summary.contract_id);

    html! {
        div class="trace-summary" {
            span class="trace-summary-label" { "Contract call:" }
            div class="trace-contract-row" {
                div class="trace-contract-icon" aria-hidden="true" {
                    img class="trace-contract-img" src=(summary.icon_url.clone()) alt=(summary.contract_name.value.clone()) loading="lazy" onerror=(icon_onerror(&fallback_icon_url)) {}
                    span class="trace-icon-letter" { (fallback_letter) }
                }
                div class="trace-contract-meta" {
                    a class="trace-contract-name link" href=(alkane_path.clone()) { (summary.contract_name.value.clone()) }
                }
                span class="io-arrow" { (arrow_svg()) }
            }
            @if summary.method_name.is_some() || (summary.opcode.is_some() && !is_factory_clone) || is_factory_clone {
                div class="trace-method-pill" {
                    @if is_factory_clone {
                        span class="trace-method-icon" aria-hidden="true" { (icon_magic_wand()) }
                    }
                    span class="trace-method-name" { (method_label) }
                    @if let Some(op) = summary.opcode {
                        @if !is_factory_clone {
                        span class="trace-opcode" { (format!("opcode {}", op)) }
                        }
                    }
                }
                @if is_factory_clone && (summary.method_name.is_some() && summary.opcode.is_some()) {
                    div class="trace-method-pill trace-method-pill-secondary" {
                        @if let Some(name) = summary.method_name.as_ref() {
                            span class="trace-method-name" { (name) }
                        }
                        @if let Some(op) = summary.opcode {
                            span class="trace-opcode" { (format!("opcode {}", op)) }
                        }
                    }
                }
            }
            @if let Some(created) = summary.factory_created {
                @let created_path = format!("/alkane/{}:{}", created.block, created.tx);
                @let created_meta = summary.factory_created_meta.as_ref();
                @let created_icon = created_meta.map(|m| m.icon_url.clone()).unwrap_or_else(|| contract_icon_url(&created));
                @let created_name = created_meta.map(|m| m.name.value.clone()).unwrap_or_else(|| format!("{}:{}", created.block, created.tx));
                @let created_letter = created_meta.map(|m| m.name.fallback_letter()).unwrap_or('?');
                @let created_fallback_icon = alkane_icon_fallback_url(&created);
                div class="trace-factory-clone" {
                    span class="factory-clone-arrow muted" { (icon_arrow_bend_down_right()) }
                    a class="trace-contract-name link" href=(created_path.clone()) {
                        div class="trace-contract-icon" aria-hidden="true" {
                            img class="trace-contract-img" src=(created_icon.clone()) alt=(created_name.clone()) loading="lazy" onerror=(icon_onerror(&created_fallback_icon)) {}
                            span class="trace-icon-letter" { (created_letter) }
                        }
                        span class="trace-contract-name" { (created_name) }
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
    pill: Option<TxPill>,
    show_tx_title: bool,
) -> Markup {
    let mut alkane_meta_cache: AlkaneMetaCache = HashMap::new();
    let mut alkane_impl_cache: AlkaneImplCache = HashMap::new();
    let vins_markup =
        render_vins(tx, network, prev_map, outpoint_fn, &mut alkane_meta_cache, essentials_mdb);
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
        &mut alkane_meta_cache,
        &mut alkane_impl_cache,
        essentials_mdb,
    );

    html! {
        div class="card tx-card" {
            @if show_tx_title {
                span class="mono tx-title" { a class="link" href=(format!("/tx/{}", txid)) { (txid) } }
            }
            div class="tx-io-grid" {
                div class="io-col" {
                    div class="io-col-title" { "Inputs" }
                    (vins_markup)
                }
                div class="io-col" {
                    div class="io-col-title" { "Outputs" }
                    (vouts_markup)
                }
            }
            @if let Some(p) = pill {
                @let tone_class = match &p.tone {
                    TxPillTone::Success => "success",
                    TxPillTone::Danger => "danger",
                };
                div class="tx-pill-row" {
                    span class=(format!("pill tx-pill {}", tone_class)) { (p.label.clone()) }
                }
            }
        }

    }
}

fn render_vins(
    tx: &Transaction,
    network: Network,
    prev_map: &HashMap<Txid, Transaction>,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
    alkane_meta_cache: &mut AlkaneMetaCache,
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
                                                        @let (addr_prefix, addr_suffix) = addr_prefix_suffix(&addr);
                                                        a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                            span class="addr-prefix" { (addr_prefix) }
                                                            span class="addr-suffix" { (addr_suffix) }
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
                                (balances_list(&prevout_view.balances, alkane_meta_cache, essentials_mdb, true))
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
    alkane_meta_cache: &mut AlkaneMetaCache,
    alkane_impl_cache: &mut AlkaneImplCache,
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
                    @let is_opret = opret.is_some();
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
                                    (render_op_return(&payload, o.value, is_protostone, protostone_json.as_ref(), &traces_for_vout, &mut inspection_cache, alkane_meta_cache, alkane_impl_cache, essentials_mdb))
                                }
                                None => {
                                    @let addr_opt = Address::from_script(o.script_pubkey.as_script(), network).ok();
                                    div class="io-addr-row" {
                                        div class="io-addr" {
                                            @match addr_opt {
                                                Some(a) => {
                                                    @let addr = a.to_string();
                                                    @let (addr_prefix, addr_suffix) = addr_prefix_suffix(&addr);
                                                    a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                        span class="addr-prefix" { (addr_prefix) }
                                                        span class="addr-suffix" { (addr_suffix) }
                                                    }
                                                }
                                                None => span class="mono muted" { "non-standard" },
                                            }
                                        }
                                        div class="io-amount mono muted" { (fmt_amount(o.value)) }
                                    }
                                }
                            }
                            (balances_list(&balances, alkane_meta_cache, essentials_mdb, true))
                        }
                        @match spent_by {
                            Some(spender) => a class=(if is_opret { "io-arrow io-arrow-link out spent opret-arrow" } else { "io-arrow io-arrow-link out spent" }) href=(format!("/tx/{}", spender)) title="Spent by transaction" { (arrow_svg()) },
                            None => span class=(if is_opret { "io-arrow out opret-arrow" } else { "io-arrow out" }) title="Unspent output" { (arrow_svg()) },
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
    meta_cache: &mut AlkaneMetaCache,
    impl_cache: &mut AlkaneImplCache,
    essentials_mdb: &Mdb,
) -> Markup {
    let fallback = opreturn_utf8(&payload.data);
    let trace_views: Vec<(String, Option<Value>)> = traces
        .iter()
        .map(|t| {
            let raw = prettyify_protobuf_trace_json(&t.protobuf_trace)
                .unwrap_or_else(|_| "[]".to_string());
            let parsed = serde_json::from_str::<Value>(&raw).ok();
            (raw, parsed)
        })
        .collect();

    html! {
        div class="io-addr-row opret-row" {
            details class="io-opret" open {
                summary class="opret-summary" {
                    span class="opret-left" {
                        span class="opret-caret" aria-hidden="true" { (icon_caret_right()) }
                        span class="opret-title mono" {
                            "OP_RETURN"
                            @if is_protostone {
                                " ( "
                                span class="opret-meta" {
                                    span class="opret-diamond" aria-hidden="true" {}
                                    " Protostone message)"
                                }
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
                        @let summary = summarize_contract_call(*trace, inspection_cache, meta_cache, impl_cache, essentials_mdb);
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
    meta_cache: &mut AlkaneMetaCache,
    essentials_mdb: &Mdb,
    show_arrow: bool,
) -> Markup {
    if entries.is_empty() {
        return html! {};
    }
    html! {
        div class="io-alkanes" {
            @for be in entries {
                @let meta = alkane_meta(&be.alkane, meta_cache, essentials_mdb);
                @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                @let fallback_letter = meta.name.fallback_letter();
                @let fallback_icon_url = alkane_icon_fallback_url(&be.alkane);
                @let inner = html! {
                    div class="alk-line" {
                        @if show_arrow {
                            span class="alk-arrow" aria-hidden="true" { (icon_arrow_bend_down_right()) }
                        }
                        div class="alk-icon-wrap" aria-hidden="true" {
                            img class="alk-icon-img" src=(meta.icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror=(icon_onerror(&fallback_icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_alkane_amount(be.amount)) }
                        a class="alk-sym link mono" href=(format!("/alkane/{alk}")) { (meta.name.value.clone()) }
                    }
                };
                (inner)
            }
        }
    }
}

pub fn render_alkane_balances(entries: &[BalanceEntry], essentials_mdb: &Mdb) -> Markup {
    let mut cache: AlkaneMetaCache = HashMap::new();
    balances_list(entries, &mut cache, essentials_mdb, false)
}

pub(crate) fn alkane_meta(
    id: &SchemaAlkaneId,
    meta_cache: &mut AlkaneMetaCache,
    essentials_mdb: &Mdb,
) -> AlkaneMetaDisplay {
    if let Some(meta) = meta_cache.get(id) {
        return meta.clone();
    }

    let key = format!("{}:{}", id.block, id.tx);
    let mut name: Option<String> = None;
    let mut symbol: Option<String> = None;

    if let Ok(Some(rec)) = load_creation_record(essentials_mdb, id) {
        if let Some(n) = rec.names.first().map(|s| s.trim()).filter(|s| !s.is_empty()) {
            name = Some(n.to_string());
        }
        if let Some(s) = rec.symbols.first().map(|s| s.trim()).filter(|s| !s.is_empty()) {
            symbol = Some(s.to_string());
        }
    }

    for (id_s, n, sym) in alkane_name_overrides() {
        if *id_s == key {
            name = Some(n.to_string());
            symbol = Some(sym.to_string());
        }
    }

    let known = name.as_ref().map(|s| !s.trim().is_empty()).unwrap_or(false);
    let value = name.unwrap_or_else(|| key.clone());
    let sym = symbol.unwrap_or_else(|| value.clone());
    let icon_url = alkane_icon_url(id);
    let meta = AlkaneMetaDisplay { name: ResolvedName { value, known }, symbol: sym, icon_url };
    meta_cache.insert(*id, meta.clone());
    meta
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
                    let key_escaped = escape_html(
                        &serde_json::to_string(k).unwrap_or_else(|_| format!("{:?}", k)),
                    );
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
