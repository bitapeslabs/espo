use super::defs::{EspoTraceType, SignedU128, SignedU128MapExt};
use super::utils::{
    Unallocated, compute_nets, is_op_return, parse_protostones, parse_short_id,
    schema_id_from_parts, transfers_to_sheet, tx_has_op_return, u128_to_u32,
};
use crate::alkanes::trace::{
    EspoBlock, EspoHostFunctionValues, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceStatus, EspoTrace,
};
use crate::config::{
    debug_enabled, get_electrum_like, get_espo_module_mdb, get_metashrew, get_metashrew_sdb,
    get_network, strict_check_alkane_balances, strict_check_trace_mismatches, strict_check_utxos,
};
use crate::debug;
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::storage::{AmmDataTable, SearchIndexField};
use crate::modules::ammdata::utils::search::collect_search_prefixes;
use crate::modules::essentials::storage::{
    AddressActivityEntry, AddressAmountEntry, AddressIndexListKind, AlkaneBalanceTxEntry,
    AlkaneTxSummary, BalanceEntry, HolderEntry, HolderId,
    address_index_list_id_alkane_balance_txs_by_token, address_index_list_id_alkane_block_txs,
    append_address_index_values, build_outpoint_pos_versioned_puts,
    build_outpoint_spent_versioned_puts, build_tx_pos_versioned_puts, decode_balances_vec,
    decode_outpoint_pointer_blob_v3, decode_pointer_idx_u64, decode_u128_value,
    encode_outpoint_pointer_blob_v3, encode_pointer_idx_u64, encode_tx_pointer_blob_v3,
    encode_u128_value, encode_vec, get_holders_count_encoded, mk_outpoint, resolve_outpoint_id_v2,
    resolve_outpoint_ids_batch_v2, resolve_outpoint_spent_by_id_v2,
    resolve_outpoint_spent_by_ids_batch_v2, resolve_tx_pointer_ids_batch_v2, spk_to_address_str,
};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetMultiValuesParams, GetRawValueParams, SetBatchParams,
};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::state_at::StateAt;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Context, Result, anyhow};
use bitcoin::block::Header;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{ScriptBuf, Transaction, Txid};
use protorune_support::protostone::{Protostone, ProtostoneEdict};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

static AMMDATA_MDB: OnceLock<Arc<Mdb>> = OnceLock::new();

fn ammdata_mdb() -> Arc<Mdb> {
    AMMDATA_MDB.get_or_init(|| get_espo_module_mdb("ammdata")).clone()
}

#[derive(Default, Clone, Copy)]
struct FamilyWriteProfile {
    raw_put_rows: usize,
    raw_put_key_bytes: usize,
    raw_put_value_bytes: usize,
    dedup_put_rows: usize,
    dedup_put_key_bytes: usize,
    dedup_put_value_bytes: usize,
    raw_delete_rows: usize,
    raw_delete_key_bytes: usize,
    dedup_delete_rows: usize,
    dedup_delete_key_bytes: usize,
}

fn key_starts_with_any(key: &[u8], prefixes: &[&[u8]]) -> bool {
    prefixes.iter().any(|p| !p.is_empty() && key.starts_with(p))
}

fn profile_family_writes(
    puts: &[(Vec<u8>, Vec<u8>)],
    deletes: &[Vec<u8>],
    prefixes: &[&[u8]],
) -> FamilyWriteProfile {
    let mut stats = FamilyWriteProfile::default();

    for (k, v) in puts {
        if !key_starts_with_any(k, prefixes) {
            continue;
        }
        stats.raw_put_rows = stats.raw_put_rows.saturating_add(1);
        stats.raw_put_key_bytes = stats.raw_put_key_bytes.saturating_add(k.len());
        stats.raw_put_value_bytes = stats.raw_put_value_bytes.saturating_add(v.len());
    }
    for k in deletes {
        if !key_starts_with_any(k, prefixes) {
            continue;
        }
        stats.raw_delete_rows = stats.raw_delete_rows.saturating_add(1);
        stats.raw_delete_key_bytes = stats.raw_delete_key_bytes.saturating_add(k.len());
    }

    // Mirror set_batch last-write-wins behavior for per-family profiling.
    let mut dedup_puts: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
    for (k, v) in puts.iter().rev() {
        if !key_starts_with_any(k, prefixes) {
            continue;
        }
        dedup_puts.entry(k.clone()).or_insert((k.len(), v.len()));
    }
    stats.dedup_put_rows = dedup_puts.len();
    for (klen, vlen) in dedup_puts.values() {
        stats.dedup_put_key_bytes = stats.dedup_put_key_bytes.saturating_add(*klen);
        stats.dedup_put_value_bytes = stats.dedup_put_value_bytes.saturating_add(*vlen);
    }

    let mut dedup_deletes_seen: HashSet<Vec<u8>> = HashSet::new();
    for k in deletes {
        if !key_starts_with_any(k, prefixes) {
            continue;
        }
        if dedup_puts.contains_key(k) {
            continue;
        }
        if dedup_deletes_seen.insert(k.clone()) {
            stats.dedup_delete_rows = stats.dedup_delete_rows.saturating_add(1);
            stats.dedup_delete_key_bytes = stats.dedup_delete_key_bytes.saturating_add(k.len());
        }
    }

    stats
}

pub(crate) fn clean_espo_sandshrew_like_trace(
    trace: &EspoSandshrewLikeTrace,
    host_function_values: &EspoHostFunctionValues,
) -> Option<EspoSandshrewLikeTrace> {
    let mut invokes = 0usize;
    let mut returns = 0usize;
    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(_) => invokes += 1,
            EspoSandshrewLikeTraceEvent::Return(_) => returns += 1,
            EspoSandshrewLikeTraceEvent::Create(_) => {}
        }
    }

    if invokes == returns {
        return Some(trace.clone());
    }
    if returns < invokes {
        return None;
    }

    let (header, coinbase, diesel, fee) = host_function_values;
    let host_values: [&[u8]; 4] = [header, coinbase, diesel, fee];
    let mismatch = returns.saturating_sub(invokes);

    let decode_data = |data: &str| -> Option<Vec<u8>> {
        let trimmed = data.strip_prefix("0x").unwrap_or(data);
        if trimmed.is_empty() {
            return Some(Vec::new());
        }
        hex::decode(trimmed).ok()
    };

    let host_match = |data_bytes: &[u8]| -> bool {
        for host_bytes in host_values.iter() {
            if data_bytes == *host_bytes {
                return true;
            }
        }
        false
    };

    let fuzzy_host_match = |data_bytes: &[u8]| -> bool {
        if data_bytes.len() == 80 && deserialize::<Header>(data_bytes).is_ok() {
            return true;
        }
        if let Ok(tx) = deserialize::<Transaction>(data_bytes) {
            if tx.is_coinbase() {
                return true;
            }
        }
        false
    };

    let attempt_clean = |allow_fuzzy: bool| -> Option<EspoSandshrewLikeTrace> {
        let mut remove_indices: HashSet<usize> = HashSet::new();
        let mut candidate_stack: Vec<usize> = Vec::new();
        let mut total_candidates = 0usize;
        let mut depth: isize = 0;

        for (idx, ev) in trace.events.iter().enumerate() {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(_) => {
                    depth += 1;
                }
                EspoSandshrewLikeTraceEvent::Return(ret) => {
                    let mut is_candidate = false;
                    if ret.status == EspoSandshrewLikeTraceStatus::Success
                        && ret.response.alkanes.is_empty()
                        && ret.response.storage.is_empty()
                    {
                        if let Some(data_bytes) = decode_data(&ret.response.data) {
                            if host_match(&data_bytes) {
                                is_candidate = true;
                            } else if allow_fuzzy && fuzzy_host_match(&data_bytes) {
                                is_candidate = true;
                            }
                        }
                    }
                    if is_candidate {
                        total_candidates += 1;
                        candidate_stack.push(idx);
                    }

                    depth -= 1;
                    if depth < 0 {
                        let Some(remove_idx) = candidate_stack.pop() else {
                            return None;
                        };
                        remove_indices.insert(remove_idx);
                        depth += 1;
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
        }

        if total_candidates < mismatch || remove_indices.len() != mismatch {
            return None;
        }

        let mut cleaned_events =
            Vec::with_capacity(trace.events.len().saturating_sub(remove_indices.len()));
        for (idx, ev) in trace.events.iter().enumerate() {
            if !remove_indices.contains(&idx) {
                cleaned_events.push(ev.clone());
            }
        }

        let mut cleaned_invokes = 0usize;
        let mut cleaned_returns = 0usize;
        let mut cleaned_depth: isize = 0;
        for ev in &cleaned_events {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(_) => {
                    cleaned_invokes += 1;
                    cleaned_depth += 1;
                }
                EspoSandshrewLikeTraceEvent::Return(_) => {
                    cleaned_returns += 1;
                    cleaned_depth -= 1;
                    if cleaned_depth < 0 {
                        return None;
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
        }
        if cleaned_invokes != cleaned_returns || cleaned_depth != 0 {
            return None;
        }

        Some(EspoSandshrewLikeTrace { outpoint: trace.outpoint.clone(), events: cleaned_events })
    };

    attempt_clean(false).or_else(|| attempt_clean(true))
}

fn parse_u128_from_str(input: &str) -> Option<u128> {
    if let Some(hex) = input.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        input.parse::<u128>().ok()
    }
}

fn mint_deltas_from_trace(
    trace: &EspoSandshrewLikeTrace,
    host_function_values: &EspoHostFunctionValues,
) -> Option<BTreeMap<SchemaAlkaneId, u128>> {
    let trace = clean_espo_sandshrew_like_trace(trace, host_function_values)?;

    #[derive(Clone)]
    struct Frame {
        owner: Option<SchemaAlkaneId>,
        mint_candidate: bool,
        incoming: Vec<(SchemaAlkaneId, u128)>,
    }

    let mut stack: Vec<Frame> = Vec::new();
    let mut deltas: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                let typ = inv.typ.to_ascii_lowercase();
                let is_static = typ == "staticcall";
                let mut mint_candidate = false;
                if !is_static {
                    let opcode_match = inv
                        .context
                        .inputs
                        .get(2)
                        .and_then(|s| parse_u128_from_str(s))
                        .filter(|op| *op == 77)
                        .is_some()
                        || inv
                            .context
                            .inputs
                            .get(0)
                            .and_then(|s| parse_u128_from_str(s))
                            .filter(|op| *op == 77)
                            .is_some();
                    if opcode_match {
                        mint_candidate = true;
                    }
                }
                let incoming = if mint_candidate {
                    inv.context
                        .incoming_alkanes
                        .iter()
                        .filter_map(|t| {
                            let id = parse_short_id(&t.id)?;
                            let value = parse_u128_from_str(&t.value)?;
                            if value == 0 {
                                return None;
                            }
                            Some((id, value))
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                stack.push(Frame {
                    owner: parse_short_id(&inv.context.myself),
                    mint_candidate,
                    incoming,
                });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                let Some(frame) = stack.pop() else {
                    return None;
                };
                if !frame.mint_candidate {
                    continue;
                }
                if ret.status != EspoSandshrewLikeTraceStatus::Success {
                    continue;
                }
                let Some(owner) = frame.owner else {
                    continue;
                };
                let mut returned: Vec<(SchemaAlkaneId, u128)> = ret
                    .response
                    .alkanes
                    .iter()
                    .filter_map(|t| {
                        let id = parse_short_id(&t.id)?;
                        let value = parse_u128_from_str(&t.value)?;
                        if value == 0 {
                            return None;
                        }
                        Some((id, value))
                    })
                    .collect();
                if !frame.incoming.is_empty() && !returned.is_empty() {
                    for (inc_id, inc_value) in &frame.incoming {
                        if let Some(pos) = returned
                            .iter()
                            .position(|(id, value)| id == inc_id && value == inc_value)
                        {
                            returned.remove(pos);
                        }
                    }
                }
                if let Some((_, value)) = returned.iter().find(|(id, _)| *id == owner) {
                    *deltas.entry(owner).or_default() =
                        deltas.get(&owner).copied().unwrap_or(0).saturating_add(*value);
                }
            }
            EspoSandshrewLikeTraceEvent::Create(_) => {}
        }
    }

    if !stack.is_empty() {
        return None;
    }

    Some(deltas)
}

pub(crate) fn accumulate_alkane_balance_deltas(
    trace: &EspoSandshrewLikeTrace,
    _txid: &Txid,
    host_function_values: &EspoHostFunctionValues,
) -> (bool, HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>) {
    let debug = debug_enabled();
    let module = "essentials.balances";
    let timer = debug::start_if(debug);
    let Some(trace) = clean_espo_sandshrew_like_trace(trace, host_function_values) else {
        if strict_check_trace_mismatches() {
            eprintln!(
                "[balances][strict] dropped trace: failed to clean sandshrew-like events (txid={})",
                _txid
            );
        }
        return (false, HashMap::new());
    };
    debug::log_elapsed(module, "accumulate.clean_trace", timer);
    if std::env::var_os("ESPO_LOG_HOST_FUNCTION_VALUES").is_some() {
        let (header, coinbase, diesel, fee) = host_function_values;
        eprintln!(
            "[balances] host_function_values header={} coinbase={} diesel={} fee={}",
            hex::encode(header),
            hex::encode(coinbase),
            hex::encode(diesel),
            hex::encode(fee),
        );
    }

    // We treat the trace as a call stack (invoke ... return), and only apply balance
    // changes when a frame returns successfully. This lets us drop an entire subtree
    // of effects if a parent frame fails or is static (reverts all children).
    //
    // Rules implemented:
    // - Normal calls: incoming credits go to `myself`, outgoing debits come from `myself`.
    // - Delegate calls: still credit `myself` for incoming, but the "parent" for both
    //   incoming and outgoing is the nearest NORMAL ancestor frame (skip delegates).
    // - Static calls: ignored completely (no effects, children ignored).
    // - Create events: ignored.
    // - Returned alkanes pay to the nearest normal parent (never to a delegate).
    // - We allow negative deltas here; final balance checks happen later.
    // - Self-token deltas are kept for outflow reporting; balances/holders ignore them later.

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum FrameKind {
        Normal,
        Delegate,
        Static,
    }

    #[derive(Clone)]
    struct Frame {
        kind: FrameKind,
        owner: SchemaAlkaneId,
        incoming: BTreeMap<SchemaAlkaneId, u128>,
        parent_normal: Option<SchemaAlkaneId>,
        deltas: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    }

    // Find the nearest NORMAL frame in the current stack (delegates/statics are skipped).
    fn nearest_normal_owner(stack: &[Frame]) -> Option<SchemaAlkaneId> {
        stack.iter().rev().find_map(|frame| {
            if matches!(frame.kind, FrameKind::Normal) { Some(frame.owner) } else { None }
        })
    }

    // Add a signed delta for a (owner, token) pair.
    // Self-token deltas are kept for outflow reporting; balances filter them later.
    fn add_delta(
        outflows: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        owner: SchemaAlkaneId,
        token: SchemaAlkaneId,
        delta: SignedU128,
    ) {
        if delta.is_zero() {
            return;
        }
        let remove = {
            let entry = outflows.entry(owner).or_default();
            entry.add_signed(token, delta);
            entry.is_empty()
        };
        if remove {
            outflows.remove(&owner);
        }
    }

    // Apply a transfer (amount of token) from -> to into a delta map.
    fn apply_transfer(
        outflows: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        from: Option<SchemaAlkaneId>,
        to: Option<SchemaAlkaneId>,
        token: SchemaAlkaneId,
        amount: u128,
    ) {
        if amount == 0 {
            return;
        }
        if let Some(owner) = from {
            add_delta(outflows, owner, token, SignedU128::negative(amount));
        }
        if let Some(owner) = to {
            add_delta(outflows, owner, token, SignedU128::positive(amount));
        }
    }

    // Merge a child's delta map into its parent (used to drop effects on failure/static).
    fn merge_deltas(
        target: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        child: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    ) {
        for (owner, per_token) in child {
            if per_token.is_empty() {
                continue;
            }
            let remove = {
                let entry = target.entry(owner).or_default();
                for (token, delta) in per_token {
                    entry.add_signed(token, delta);
                }
                entry.is_empty()
            };
            if remove {
                target.remove(&owner);
            }
        }
    }

    let mut outflows: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
        HashMap::new();
    let mut stack: Vec<Frame> = Vec::new();
    let mut root_reverted = false;

    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                // Determine call kind and the nearest normal parent BEFORE pushing the frame.
                let kind = match inv.typ.to_ascii_lowercase().as_str() {
                    "delegatecall" => FrameKind::Delegate,
                    "staticcall" => FrameKind::Static,
                    _ => FrameKind::Normal,
                };
                let Some(owner) = parse_short_id(&inv.context.myself) else { continue };
                let parent_normal = nearest_normal_owner(&stack);

                // Static calls are ignored, but we still push a frame to keep stack depth.
                let incoming = if matches!(kind, FrameKind::Static) {
                    BTreeMap::new()
                } else {
                    transfers_to_sheet(&inv.context.incoming_alkanes)
                };

                stack.push(Frame { kind, owner, incoming, parent_normal, deltas: HashMap::new() });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                let Some(mut frame) = stack.pop() else {
                    // Mismatched return: treat as a reverted root.
                    root_reverted = true;
                    continue;
                };

                // Failed frames (and their children) are ignored.
                if ret.status == EspoSandshrewLikeTraceStatus::Failure {
                    if stack.is_empty() && matches!(frame.kind, FrameKind::Normal) {
                        root_reverted = true;
                    }
                    continue;
                }

                // Static calls are ignored completely (including children).
                if matches!(frame.kind, FrameKind::Static) {
                    continue;
                }

                // Incoming: transfer from nearest normal parent -> this frame's owner.
                for (token, amount) in &frame.incoming {
                    apply_transfer(
                        &mut frame.deltas,
                        frame.parent_normal,
                        Some(frame.owner),
                        *token,
                        *amount,
                    );
                }

                // Outgoing: transfer from this frame's owner -> nearest normal parent.
                let outgoing = transfers_to_sheet(&ret.response.alkanes);
                for (token, amount) in &outgoing {
                    apply_transfer(
                        &mut frame.deltas,
                        Some(frame.owner),
                        frame.parent_normal,
                        *token,
                        *amount,
                    );
                }

                // Merge this frame's (successful) subtree effects upward.
                if let Some(parent) = stack.last_mut() {
                    merge_deltas(&mut parent.deltas, frame.deltas);
                } else {
                    merge_deltas(&mut outflows, frame.deltas);
                }
            }
            EspoSandshrewLikeTraceEvent::Create(_) => {
                // Create events are ignored per rules.
            }
        }
    }

    if root_reverted || !stack.is_empty() {
        return (false, HashMap::new());
    }

    (true, outflows)
}

/* -------------------------- Edicts + routing (multi-protostone, per your rules) -------------------------- */

/// Whether `vout` is a valid, spendable, non-OP_RETURN output index for this tx.
fn is_valid_spend_vout(tx: &Transaction, vout: u32) -> bool {
    let i = vout as usize;
    i < tx.output.len() && !is_op_return(&tx.output[i].script_pubkey)
}

fn apply_transfers_multi(
    tx: &Transaction,
    protostones: &[Protostone],
    traces_for_tx: &[EspoTrace],
    mut seed_unalloc: Unallocated, // VIN balances only
) -> Result<HashMap<u32, Vec<BalanceEntry>>> {
    let mut out_map: HashMap<u32, Vec<BalanceEntry>> = HashMap::new();

    let n_outputs: u32 = tx.output.len() as u32;
    let multicast_index: u32 = n_outputs; // runes multicast
    let shadow_base: u32 = n_outputs.saturating_add(1);
    let shadow_end: u32 = shadow_base + protostones.len() as u32 - 1;

    // Spendable (non-OP_RETURN)
    let spendable_vouts: Vec<u32> = tx
        .output
        .iter()
        .enumerate()
        .filter_map(|(i, o)| if is_op_return(&o.script_pubkey) { None } else { Some(i as u32) })
        .collect();

    // Map shadow index -> trace (prefer match by Invoke.vout; fallback by order)
    let mut trace_by_shadow: HashMap<u32, &EspoSandshrewLikeTrace> = HashMap::new();

    for t in traces_for_tx {
        // prefer the vout recorded in the first Invoke; else use the outpoint's vout
        let mut vout_opt: Option<u32> = None;
        for ev in &t.sandshrew_trace.events {
            if let EspoSandshrewLikeTraceEvent::Invoke(inv) = ev {
                vout_opt = Some(inv.context.vout);
                break;
            }
        }
        let vout = vout_opt.unwrap_or(t.outpoint.vout);

        // only keep traces that actually point into this tx's shadow range
        if vout >= shadow_base && vout <= shadow_end {
            trace_by_shadow.insert(vout, &t.sandshrew_trace);
        }
    }

    // Sheet incoming routed explicitly to protostone[i] (from previous pointers/edicts/refunds)
    let mut incoming_shadow: Vec<BTreeMap<SchemaAlkaneId, u128>> =
        vec![BTreeMap::new(); protostones.len()];

    // helpers
    fn push_to_vout(
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        vout: u32,
        delta: &BTreeMap<SchemaAlkaneId, u128>,
    ) {
        if delta.is_empty() {
            return;
        }
        let e = out_map.entry(vout).or_default();
        for (rid, &amt) in delta {
            if amt > 0 {
                e.push(BalanceEntry { alkane: *rid, amount: amt });
            }
        }
    }

    fn route_delta(
        target: u32,
        delta: &BTreeMap<SchemaAlkaneId, u128>,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        tx: &Transaction,
        spendable_vouts: &[u32],
        n_outputs: u32,
        multicast_index: u32,
        shadow_base: u32,
        shadow_end: u32,
    ) {
        if delta.is_empty() {
            return;
        }

        if target == multicast_index {
            if spendable_vouts.is_empty() {
                return;
            }
            let m = spendable_vouts.len() as u128;
            for (rid, &total_amt) in delta.iter() {
                if total_amt == 0 {
                    continue;
                }
                let per = total_amt / m;
                let rem = (total_amt % m) as usize;
                for (i, out_i) in spendable_vouts.iter().enumerate() {
                    let mut amt = per;
                    if i < rem {
                        amt = amt.saturating_add(1);
                    }
                    if amt == 0 {
                        continue;
                    }
                    out_map
                        .entry(*out_i)
                        .or_default()
                        .push(BalanceEntry { alkane: *rid, amount: amt });
                }
            }
            return;
        }

        if target < n_outputs {
            if !is_valid_spend_vout(tx, target) {
                return;
            }
            push_to_vout(out_map, target, delta);
            return;
        }

        if target >= shadow_base && target <= shadow_end {
            let idx = (target - shadow_base) as usize;
            let sheet = &mut incoming_shadow[idx];
            for (rid, &amt) in delta {
                if amt == 0 {
                    continue;
                }
                *sheet.entry(*rid).or_default() =
                    sheet.get(rid).copied().unwrap_or(0).saturating_add(amt);
            }
            return;
        }
        // else burn by omission
    }

    fn apply_single_edict(
        sheet: &mut BTreeMap<SchemaAlkaneId, u128>,
        ed: &ProtostoneEdict,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        tx: &Transaction,
        spendable_vouts: &[u32],
        n_outputs: u32,
        multicast_index: u32,
        shadow_base: u32,
        shadow_end: u32,
    ) -> Result<()> {
        // guard
        if ed.id.block == 0 && ed.id.tx > 0 {
            return Ok(());
        }
        let out_idx = u128_to_u32(ed.output)?;
        let rid = schema_id_from_parts(ed.id.block, ed.id.tx)?;

        // ---- SPECIAL: multicast target (output == n_outputs) ----
        if out_idx == multicast_index {
            if spendable_vouts.is_empty() {
                return Ok(());
            }

            // how much is available on the sheet for this rune
            let entry = sheet.entry(rid).or_default();
            let have = *entry;
            if have == 0 {
                return Ok(());
            }

            if ed.amount == 0 {
                // even split of ALL available (what you already had working)
                let mut delta = BTreeMap::new();
                delta.insert(rid, have);
                // zero it out from the sheet before routing
                *entry = 0;
                sheet.remove(&rid);

                route_delta(
                    out_idx,
                    &delta,
                    out_map,
                    incoming_shadow,
                    tx,
                    spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                );
            } else {
                // amount > 0 → treat ed.amount as PER-VOUT CAP, and use ALL available
                let mut remaining = have;
                let mut used: u128 = 0;

                for v in spendable_vouts {
                    if remaining == 0 {
                        break;
                    }
                    let give = remaining.min(ed.amount);
                    if give == 0 {
                        break;
                    }
                    out_map.entry(*v).or_default().push(BalanceEntry { alkane: rid, amount: give });
                    remaining = remaining.saturating_sub(give);
                    used = used.saturating_add(give);
                }

                // subtract only what we actually allocated; leave any leftover on the sheet
                *entry = entry.saturating_sub(used);
                if *entry == 0 {
                    sheet.remove(&rid);
                }
            }

            return Ok(());
        }

        // ---- normal (non-multicast) targets: original behavior ----
        let have = sheet.get(&rid).copied().unwrap_or(0);
        let need = if ed.amount == 0 { have } else { ed.amount.min(have) };
        if need == 0 {
            return Ok(());
        }

        // take from sheet
        let entry = sheet.entry(rid).or_default();
        let take = (*entry).min(need);
        *entry = entry.saturating_sub(take);
        if *entry == 0 {
            sheet.remove(&rid);
        }
        if take == 0 {
            return Ok(());
        }

        // route normally
        let mut delta = BTreeMap::new();
        delta.insert(rid, take);
        route_delta(
            out_idx,
            &delta,
            out_map,
            incoming_shadow,
            tx,
            spendable_vouts,
            n_outputs,
            multicast_index,
            shadow_base,
            shadow_end,
        );
        Ok(())
    }

    // process in order
    for (i, ps) in protostones.iter().enumerate() {
        let shadow_vout = shadow_base + i as u32;

        // sheet starts with explicitly routed incoming to this shadow.
        let mut sheet: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

        // merge routed-in firstx
        for (rid, amt) in std::mem::take(&mut incoming_shadow[i]) {
            if amt == 0 {
                continue;
            }
            *sheet.entry(rid).or_default() =
                sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
        }

        // if there is a trace for this protostone, compute net_out and status
        let (net_in, net_out, status) = match trace_by_shadow.get(&shadow_vout) {
            Some(trace) => compute_nets(trace),
            None => (None, None, EspoTraceType::NOTRACE),
        };
        // Metashrew can omit incoming_alkanes on reverted traces; fall back to NOTRACE
        // so original VIN balances are still available for edicts/pointers.
        let revert_missing_incoming =
            status == EspoTraceType::REVERT && net_in.as_ref().map_or(true, |m| m.is_empty());
        let status = if revert_missing_incoming { EspoTraceType::NOTRACE } else { status };

        // On success, consume incoming amounts so only returned/minted balances remain.
        if status == EspoTraceType::SUCCESS {
            if let Some(ref net_in_map) = net_in {
                for (rid, amt) in net_in_map {
                    if *amt == 0 {
                        continue;
                    }
                    let entry = sheet.entry(*rid).or_default();
                    *entry = entry.saturating_sub(*amt);
                    if *entry == 0 {
                        sheet.remove(rid);
                    }
                }
            }
        }

        // add net_out to sheet
        if status == EspoTraceType::SUCCESS {
            if let Some(ref net_out_map) = net_out {
                for (rid, amt) in net_out_map {
                    if *amt == 0 {
                        continue;
                    }
                    *sheet.entry(*rid).or_default() =
                        sheet.get(rid).copied().unwrap_or(0).saturating_add(*amt);
                }
            }
        }
        // merge VIN balances ONLY into protostone 0’s sheet
        if i == 0 && status == EspoTraceType::NOTRACE {
            for (rid, amt) in seed_unalloc.drain_all() {
                if amt == 0 {
                    continue;
                }
                *sheet.entry(rid).or_default() =
                    sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
            }
        }

        // If we have a status and it is Failure → refund net_in (only), skip edicts.
        if status == EspoTraceType::REVERT {
            if let Some(ref net_in_map) = net_in {
                if let Some(refund_ptr) = ps.refund {
                    route_delta(
                        refund_ptr,
                        &net_in_map,
                        &mut out_map,
                        &mut incoming_shadow,
                        tx,
                        &spendable_vouts,
                        n_outputs,
                        multicast_index,
                        shadow_base,
                        shadow_end,
                    );
                }
                // if no refund pointer → burn (do nothing)
            }
            // Skip edicts on failure
            continue;
        }

        // Success path (or no status info): apply edicts against the current sheet
        if !ps.edicts.is_empty() {
            for ed in &ps.edicts {
                if let Err(e) = apply_single_edict(
                    &mut sheet,
                    ed,
                    &mut out_map,
                    &mut incoming_shadow,
                    tx,
                    &spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                ) {
                    eprintln!("[ESSENTIALS::balances] WARN edict apply failed: {e:?}");
                }
            }
        }

        // leftovers after edicts:
        if !sheet.is_empty() {
            if let Some(ptr) = ps.pointer {
                route_delta(
                    ptr,
                    &sheet,
                    &mut out_map,
                    &mut incoming_shadow,
                    tx,
                    &spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                );
            } else {
                // per your note: do NOT auto-chain; send to first non-OP_RETURN vout
                if let Some(v) = spendable_vouts.first().copied() {
                    push_to_vout(&mut out_map, v, &sheet);
                }
                // else burn by omission
            }
        }
    }

    Ok(out_map)
}

/* -------------------------- Holders helpers -------------------------- */

fn holder_order_key(id: &HolderId) -> String {
    match id {
        HolderId::Address(a) => format!("addr:{a}"),
        HolderId::Alkane(id) => format!("alkane:{:010}:{:020}", id.block, id.tx),
    }
}

fn sort_address_amount_entries(entries: &mut Vec<AddressAmountEntry>) {
    entries.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => a.address.cmp(&b.address),
        o => o,
    });
}

/* ===========================================================
Public API
=========================================================== */

#[allow(unused_assignments)]
pub fn bulk_update_balances_for_block(
    provider: &EssentialsProvider,
    block: &EspoBlock,
) -> Result<()> {
    crate::debug_timer_log!("bulk_update_balances_for_block.total");
    let debug = debug_enabled();
    let module = "essentials.balances";
    let network = get_network();
    let table = provider.table();
    let blockhash = block.block_header.block_hash().to_byte_array();
    let mut tx_index_by_txid: HashMap<[u8; 32], u32> =
        HashMap::with_capacity(block.transactions.len());
    for (tx_idx, atx) in block.transactions.iter().enumerate() {
        tx_index_by_txid.insert(atx.transaction.compute_txid().to_byte_array(), tx_idx as u32);
    }
    let search_cfg = AmmDataConfig::load_from_global_config().ok();
    let search_index_enabled = search_cfg.as_ref().map(|c| c.search_index_enabled).unwrap_or(false);
    let mut search_prefix_min =
        search_cfg.as_ref().map(|c| c.search_prefix_min_len as usize).unwrap_or(2);
    let mut search_prefix_max =
        search_cfg.as_ref().map(|c| c.search_prefix_max_len as usize).unwrap_or(6);
    if search_prefix_min == 0 {
        search_prefix_min = 2;
    }
    if search_prefix_max < search_prefix_min {
        search_prefix_max = search_prefix_min;
    }
    let mut ammdata_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut ammdata_deletes: Vec<Vec<u8>> = Vec::new();

    eprintln!("[balances] >>> begin block #{} (txs={})", block.height, block.transactions.len());

    // --------- stats ----------
    let mut stat_outpoints_marked_spent: usize = 0;
    let mut stat_outpoints_written: usize = 0;
    let mut stat_minus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut stat_plus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut minted_delta_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut alkane_tx_summaries: Vec<AlkaneTxSummary> = Vec::new();
    let mut alkane_block_txids: Vec<[u8; 32]> = Vec::new();
    let mut alkane_address_txids: HashMap<String, Vec<[u8; 32]>> = HashMap::new();
    let mut latest_trace_txids: Vec<[u8; 32]> = Vec::new();
    let mut transfer_volume_delta: HashMap<SchemaAlkaneId, HashMap<String, u128>> = HashMap::new();
    let mut total_received_delta: HashMap<SchemaAlkaneId, HashMap<String, u128>> = HashMap::new();
    let mut address_activity_transfer_delta: HashMap<String, HashMap<SchemaAlkaneId, u128>> =
        HashMap::new();
    let mut address_activity_received_delta: HashMap<String, HashMap<SchemaAlkaneId, u128>> =
        HashMap::new();
    let mut address_balance_delta: HashMap<String, HashMap<SchemaAlkaneId, SignedU128>> =
        HashMap::new();

    let push_balance_tx_entry = |map: &mut HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>,
                                 alk: SchemaAlkaneId,
                                 entry: AlkaneBalanceTxEntry| {
        let entries = map.entry(alk).or_default();
        if let Some(existing) = entries.iter_mut().find(|e| e.txid == entry.txid) {
            if existing.outflow.is_empty() && !entry.outflow.is_empty() {
                existing.outflow = entry.outflow;
            }
            if existing.height == 0 && entry.height != 0 {
                existing.height = entry.height;
            }
            return;
        }
        entries.push(entry);
    };
    let push_balance_tx_entry_pair =
        |map: &mut HashMap<(SchemaAlkaneId, SchemaAlkaneId), Vec<AlkaneBalanceTxEntry>>,
         owner: SchemaAlkaneId,
         token: SchemaAlkaneId,
         entry: AlkaneBalanceTxEntry| {
            let entries = map.entry((owner, token)).or_default();
            if let Some(existing) = entries.iter_mut().find(|e| e.txid == entry.txid) {
                if existing.outflow.is_empty() && !entry.outflow.is_empty() {
                    existing.outflow = entry.outflow;
                }
                if existing.height == 0 && entry.height != 0 {
                    existing.height = entry.height;
                }
                return;
            }
            entries.push(entry);
        };

    // holders_delta[alk][addr] = SignedU128 delta
    let mut holders_delta: HashMap<SchemaAlkaneId, BTreeMap<HolderId, SignedU128>> = HashMap::new();
    let mut alkane_balance_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
        HashMap::new();
    let mut alkane_balance_tx_entries: HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>> =
        HashMap::new();
    let mut alkane_balance_tx_entries_by_token: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId),
        Vec<AlkaneBalanceTxEntry>,
    > = HashMap::new();
    let mut alkane_balance_delta_src: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId),
        AlkaneBalanceTxEntry,
    > = HashMap::new();

    // Records for inputs spent in this block (for persistence w/ tx_spent)
    #[derive(Clone)]
    struct SpentOutpointRecord {
        outpoint: EspoOutpoint,      // original outpoint (tx_spent = None)
        addr: Option<String>,        // resolved address
        balances: Vec<BalanceEntry>, // balances stored on the outpoint
        spk: Option<ScriptBuf>,      // script (for reverse index)
        spent_by: Vec<u8>,           // BE txid of spending tx
    }
    let mut spent_outpoints: HashMap<String, SpentOutpointRecord> = HashMap::new();

    // Ephemeral state for CPFP within the same block
    let mut ephem_outpoint_balances: HashMap<String, Vec<BalanceEntry>> = HashMap::new();
    let mut ephem_outpoint_addr: HashMap<String, String> = HashMap::new();
    let mut ephem_outpoint_spk: HashMap<String, ScriptBuf> = HashMap::new();
    let mut ephem_outpoint_struct: HashMap<String, EspoOutpoint> = HashMap::new();
    let mut consumed_ephem_outpoints: HashMap<String, Vec<u8>> = HashMap::new(); // outpoint_str -> spender txid

    // ---------- Pass A: collect block-created outpoints & external inputs ----------
    let timer = debug::start_if(debug);
    let mut block_created_outs: HashSet<String> = HashSet::new();
    for atx in &block.transactions {
        let tx = &atx.transaction;
        if !tx_has_op_return(tx) {
            continue; // no OP_RETURN -> no Alkanes activity on its outputs
        }
        let txid = tx.compute_txid();
        for (vout, _o) in tx.output.iter().enumerate() {
            let op = mk_outpoint(txid.as_byte_array().to_vec(), vout as u32, None);
            block_created_outs.insert(op.as_outpoint_string());
        }
    }

    // Collect all non-ephemeral vins across the block (dedup)
    let mut external_inputs_vec: Vec<EspoOutpoint> = Vec::new();
    let mut external_inputs_set: HashSet<(Vec<u8>, u32)> = HashSet::new();

    for atx in &block.transactions {
        for input in &atx.transaction.input {
            let op = mk_outpoint(
                input.previous_output.txid.as_byte_array().to_vec(),
                input.previous_output.vout,
                None,
            );
            let in_str = op.as_outpoint_string();
            if !block_created_outs.contains(&in_str) {
                let key = (op.txid.clone(), op.vout);
                if external_inputs_set.insert(key) {
                    external_inputs_vec.push(op);
                }
            }
        }
    }

    debug::log_elapsed(module, "pass_a_collect_outpoints", timer);
    // ---------- Pass B: fetch external inputs (batch read) ----------
    let timer = debug::start_if(debug);
    let mut balances_by_outpoint: HashMap<(Txid, u32), Vec<BalanceEntry>> = HashMap::new();
    let mut addr_by_outpoint: HashMap<(Txid, u32), String> = HashMap::new();
    let mut spk_by_outpoint: HashMap<(Txid, u32), ScriptBuf> = HashMap::new();

    if !external_inputs_vec.is_empty() {
        // Prefilter external inputs by prev txids that are indexed as alkane txs.
        // If a prev tx never had alkane activity, none of its outpoints can hold alkane balances.
        let mut external_prev_txids: Vec<[u8; 32]> = Vec::new();
        let mut external_prev_txid_set: HashSet<[u8; 32]> = HashSet::new();
        for op in &external_inputs_vec {
            if op.txid.len() != 32 {
                continue;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&op.txid);
            if external_prev_txid_set.insert(arr) {
                external_prev_txids.push(arr);
            }
        }

        let mut indexed_external_prev_txids: HashSet<[u8; 32]> = HashSet::new();
        if !external_prev_txids.is_empty() {
            let resolved = resolve_tx_pointer_ids_batch_v2(
                provider,
                StateAt::Latest,
                external_prev_txids.as_slice(),
            )?;
            for (txid, pointer_id) in external_prev_txids.iter().zip(resolved.into_iter()) {
                if pointer_id.is_some() {
                    indexed_external_prev_txids.insert(*txid);
                }
            }
        }

        let filtered_external_inputs: Vec<&EspoOutpoint> = external_inputs_vec
            .iter()
            .filter(|op| {
                if op.txid.len() != 32 {
                    return false;
                }
                let mut txid_arr = [0u8; 32];
                txid_arr.copy_from_slice(&op.txid);
                indexed_external_prev_txids.contains(&txid_arr)
            })
            .collect();

        let mut lookup_outpoints: Vec<(Txid, u32)> =
            Vec::with_capacity(filtered_external_inputs.len());
        for op in &filtered_external_inputs {
            if op.txid.len() != 32 {
                continue;
            }
            let mut txid_arr = [0u8; 32];
            txid_arr.copy_from_slice(&op.txid);
            lookup_outpoints.push((Txid::from_byte_array(txid_arr), op.vout));
        }
        let lookups =
            get_outpoint_balances_with_spent_batch(StateAt::Latest, provider, &lookup_outpoints)?;
        for (txid, vout) in lookup_outpoints {
            let Some(lookup) = lookups.get(&(txid, vout)) else {
                continue;
            };
            if lookup.spent_by.is_some() {
                continue;
            }
            let key = (txid, vout);
            if !lookup.balances.is_empty() {
                balances_by_outpoint.insert(key, lookup.balances.clone());
            }
            if let Some(addr) = lookup.address.as_ref() {
                if !addr.is_empty() {
                    addr_by_outpoint.insert((txid, vout), addr.clone());
                }
            }
            if let Some(spk) = lookup.spk.as_ref() {
                if !spk.is_empty() {
                    spk_by_outpoint.insert((txid, vout), spk.clone());
                }
            }
        }

        if debug {
            eprintln!(
                "[balances] pass_b prefilter: external_inputs={} unique_prev_txids={} indexed_prev_txids={} candidates={}",
                external_inputs_vec.len(),
                external_prev_txids.len(),
                indexed_external_prev_txids.len(),
                filtered_external_inputs.len()
            );
        }
    }
    debug::log_elapsed(module, "pass_b_fetch_inputs", timer);

    let timer = debug::start_if(debug);
    let mut block_tx_index: HashMap<Txid, usize> = HashMap::new();
    for (idx, atx) in block.transactions.iter().enumerate() {
        let txid = atx.transaction.compute_txid();
        block_tx_index.insert(txid, idx);
    }

    let mut trace_prevout_txids: Vec<Txid> = Vec::new();
    let mut trace_prevout_set: HashSet<Txid> = HashSet::new();
    for atx in &block.transactions {
        let has_traces = atx.traces.as_ref().map_or(false, |t| !t.is_empty());
        if !has_traces {
            continue;
        }
        for input in &atx.transaction.input {
            if input.previous_output.is_null() {
                continue;
            }
            let prev_txid = input.previous_output.txid;
            if block_tx_index.contains_key(&prev_txid) {
                continue;
            }
            if trace_prevout_set.insert(prev_txid) {
                trace_prevout_txids.push(prev_txid);
            }
        }
    }

    // TODO: extend prevout fallback to all alkane txs (not just traced) for full address coverage.
    debug::log_elapsed(module, "trace_prevout_scan", timer);
    let timer = debug::start_if(debug);
    let mut trace_prev_tx_map: HashMap<Txid, Transaction> = HashMap::new();
    if !trace_prevout_txids.is_empty() {
        let electrum_like = get_electrum_like();
        let start = Instant::now();
        let raw_prev = electrum_like
            .batch_transaction_get_raw(&trace_prevout_txids)
            .unwrap_or_default();
        eprintln!(
            "[balances] traced prevout fetch: block={} prevouts={} elapsed_ms={}",
            block.height,
            trace_prevout_txids.len(),
            start.elapsed().as_millis()
        );
        for (i, raw_prev) in raw_prev.into_iter().enumerate() {
            if raw_prev.is_empty() {
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                trace_prev_tx_map.insert(trace_prevout_txids[i], prev_tx);
            }
        }
    }
    debug::log_elapsed(module, "trace_prevout_fetch", timer);

    // ---------- Main per-tx loop ----------
    let process_timer = debug::start_if(debug);
    for atx in &block.transactions {
        let tx = &atx.transaction;
        let txid = tx.compute_txid();
        let txid_bytes = txid.to_byte_array();
        let mut tx_addrs: HashSet<String> = HashSet::new();
        let mut tx_transfer_amounts_by_alkane: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
        let mut tx_transfer_participants_by_alkane: HashMap<SchemaAlkaneId, HashSet<String>> =
            HashMap::new();
        let mut has_alkane_vin = false;
        let has_traces = atx.traces.as_ref().map_or(false, |t| !t.is_empty());
        let mut holder_alkanes_changed: HashSet<SchemaAlkaneId> = HashSet::new();
        let mut local_alkane_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
            HashMap::new();
        let mut tx_mint_deltas: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

        let mut add_holder_delta =
            |alk: SchemaAlkaneId,
             holder: HolderId,
             delta: SignedU128,
             holder_changed: &mut HashSet<SchemaAlkaneId>| {
                if delta.is_zero() {
                    return;
                }
                if let HolderId::Alkane(a) = holder {
                    holder_changed.insert(a);
                }
                let entry = holders_delta.entry(alk).or_default();
                let slot = entry.entry(holder.clone()).or_insert_with(SignedU128::zero);
                *slot += delta;
                if slot.is_zero() {
                    entry.remove(&holder);
                }
            };

        // Seed from VIN balances only
        let mut seed_unalloc = Unallocated::default();

        // Gather ephemerals for this tx & apply; for externals, use prefetched maps
        for input in &tx.input {
            let in_op = mk_outpoint(
                input.previous_output.txid.as_byte_array().to_vec(),
                input.previous_output.vout,
                None,
            );
            let in_key = (input.previous_output.txid, input.previous_output.vout);
            let in_str = in_op.as_outpoint_string();

            if !input.previous_output.is_null() {
                let mut input_addr: Option<String> = None;
                if let Some(idx) = block_tx_index.get(&input.previous_output.txid) {
                    if let Some(prev_out) = block.transactions[*idx]
                        .transaction
                        .output
                        .get(input.previous_output.vout as usize)
                    {
                        input_addr = spk_to_address_str(&prev_out.script_pubkey, network);
                    }
                }
                if input_addr.is_none() {
                    if let Some(addr) = addr_by_outpoint.get(&in_key) {
                        input_addr = Some(addr.clone());
                    } else if let Some(spk) = spk_by_outpoint.get(&in_key) {
                        input_addr = spk_to_address_str(spk, network);
                    }
                }
                if input_addr.is_none() && has_traces {
                    if let Some(prev_tx) = trace_prev_tx_map.get(&input.previous_output.txid) {
                        if let Some(prev_out) =
                            prev_tx.output.get(input.previous_output.vout as usize)
                        {
                            input_addr = spk_to_address_str(&prev_out.script_pubkey, network);
                        }
                    }
                }
                if let Some(addr) = input_addr {
                    tx_addrs.insert(addr.clone());
                }
            }

            // 1) Ephemeral? (created earlier in this same block)
            if let Some(bals) = ephem_outpoint_balances.get(&in_str) {
                consumed_ephem_outpoints.insert(in_str.clone(), txid.as_byte_array().to_vec());
                has_alkane_vin = true;

                if let Some(addr) = ephem_outpoint_addr.get(&in_str) {
                    tx_addrs.insert(addr.clone());
                    for be in bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            SignedU128::negative(be.amount),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
                        let per_addr = address_balance_delta.entry(addr.clone()).or_default();
                        let slot = per_addr.entry(be.alkane).or_insert_with(SignedU128::zero);
                        *slot += SignedU128::negative(be.amount);
                        if slot.is_zero() {
                            per_addr.remove(&be.alkane);
                        }
                        tx_transfer_participants_by_alkane
                            .entry(be.alkane)
                            .or_default()
                            .insert(addr.clone());
                    }
                    // we only track addr-row deletes for DB-resident rows; ephemerals were not persisted yet
                }
                for be in bals {
                    seed_unalloc.add(be.alkane, be.amount);
                }
                // record for persistence as spent
                let rec = SpentOutpointRecord {
                    outpoint: in_op.clone(),
                    addr: ephem_outpoint_addr.get(&in_str).cloned(),
                    balances: bals.clone(),
                    spk: ephem_outpoint_spk.get(&in_str).cloned(),
                    spent_by: txid.to_byte_array().to_vec(),
                };
                spent_outpoints.entry(in_str.clone()).or_insert(rec);
                continue;
            }

            // 2) External input: resolve from prefetched maps (no DB calls here)
            if let Some(bals) = balances_by_outpoint.get(&in_key).cloned() {
                has_alkane_vin = true;
                // resolve address: /outpoint_addr first, else /utxo_spk → address
                let mut resolved_addr = addr_by_outpoint.get(&in_key).cloned();
                if resolved_addr.is_none() {
                    if let Some(spk) = spk_by_outpoint.get(&in_key) {
                        resolved_addr = spk_to_address_str(spk, network);
                    }
                }

                if let Some(ref addr) = resolved_addr {
                    tx_addrs.insert(addr.clone());
                    // holders-- and mark legacy addr-row delete
                    for be in &bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            SignedU128::negative(be.amount),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
                        let per_addr = address_balance_delta.entry(addr.clone()).or_default();
                        let slot = per_addr.entry(be.alkane).or_insert_with(SignedU128::zero);
                        *slot += SignedU128::negative(be.amount);
                        if slot.is_zero() {
                            per_addr.remove(&be.alkane);
                        }
                        tx_transfer_participants_by_alkane
                            .entry(be.alkane)
                            .or_default()
                            .insert(addr.clone());
                    }
                }

                for be in &bals {
                    seed_unalloc.add(be.alkane, be.amount);
                }

                // record for persistence with spend metadata
                let rec = SpentOutpointRecord {
                    outpoint: in_op.clone(),
                    addr: resolved_addr.clone(),
                    balances: bals.clone(),
                    spk: spk_by_outpoint.get(&in_key).cloned(),
                    spent_by: txid.to_byte_array().to_vec(),
                };
                spent_outpoints.entry(in_str.clone()).or_insert(rec);
            }
            // else: no balances row → nothing to do for this vin
        }

        // apply transfers with your semantics
        let traces_for_tx: Vec<EspoTrace> = atx.traces.clone().unwrap_or_default();
        if !traces_for_tx.is_empty() {
            for t in &traces_for_tx {
                let (ok, deltas) = accumulate_alkane_balance_deltas(
                    &t.sandshrew_trace,
                    &txid,
                    &block.host_function_values,
                );
                if !ok {
                    // Trace-level failure should not discard deltas from other traces in this tx.
                    continue;
                }
                if let Some(mints) =
                    mint_deltas_from_trace(&t.sandshrew_trace, &block.host_function_values)
                {
                    for (alkane, delta) in mints {
                        if delta == 0 {
                            continue;
                        }
                        *tx_mint_deltas.entry(alkane).or_default() =
                            tx_mint_deltas.get(&alkane).copied().unwrap_or(0).saturating_add(delta);
                    }
                }
                for (owner, per_token) in deltas {
                    let entry = local_alkane_delta.entry(owner).or_default();
                    for (tok, delta) in per_token {
                        let slot = entry.entry(tok).or_insert_with(SignedU128::zero);
                        *slot += delta;
                        if slot.is_zero() {
                            entry.remove(&tok);
                        }
                    }
                }
            }
        }
        if !tx_mint_deltas.is_empty() {
            for (alkane, delta) in tx_mint_deltas {
                *minted_delta_by_alk.entry(alkane).or_default() =
                    minted_delta_by_alk.get(&alkane).copied().unwrap_or(0).saturating_add(delta);
            }
        }

        let allocations = if tx_has_op_return(tx) {
            let protostones = parse_protostones(tx)?;
            // apply transfers only when there’s a proto/runestone carrier
            apply_transfers_multi(tx, &protostones, &traces_for_tx, seed_unalloc)?
        } else {
            // No OP_RETURN → no Alkanes allocations (but we already did VIN cleanup/holders--)
            HashMap::<u32, Vec<BalanceEntry>>::new()
        };
        // record outputs ephemerally (for same-block spends)
        for (vout_idx, entries_for_vout) in allocations {
            if entries_for_vout.is_empty() || vout_idx as usize >= tx.output.len() {
                continue;
            }
            let output = &tx.output[vout_idx as usize];
            if is_op_return(&output.script_pubkey) {
                continue;
            }

            if let Some(address_str) = spk_to_address_str(&output.script_pubkey, network) {
                tx_addrs.insert(address_str.clone());
                // Combine duplicates
                let mut amounts_by_alkane: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
                for entry in entries_for_vout {
                    *amounts_by_alkane.entry(entry.alkane).or_default() = amounts_by_alkane
                        .get(&entry.alkane)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(entry.amount);
                }

                let balances_for_outpoint: Vec<BalanceEntry> = amounts_by_alkane
                    .iter()
                    .map(|(alkane_id, amount)| BalanceEntry { alkane: *alkane_id, amount: *amount })
                    .collect();

                let created_outpoint = mk_outpoint(txid.as_byte_array().to_vec(), vout_idx, None);
                let outpoint_str = created_outpoint.as_outpoint_string();

                // cache for same-block spends
                ephem_outpoint_balances.insert(outpoint_str.clone(), balances_for_outpoint.clone());
                ephem_outpoint_addr.insert(outpoint_str.clone(), address_str.clone());
                ephem_outpoint_spk.insert(outpoint_str.clone(), output.script_pubkey.clone());
                ephem_outpoint_struct.insert(outpoint_str.clone(), created_outpoint.clone());

                // holders++ stats
                for (alkane_id, delta_amount) in amounts_by_alkane {
                    *tx_transfer_amounts_by_alkane.entry(alkane_id).or_default() =
                        tx_transfer_amounts_by_alkane
                            .get(&alkane_id)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(delta_amount);
                    let total_by_addr = total_received_delta.entry(alkane_id).or_default();
                    *total_by_addr.entry(address_str.clone()).or_default() = total_by_addr
                        .get(&address_str)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta_amount);
                    let per_addr = address_balance_delta.entry(address_str.clone()).or_default();
                    let slot = per_addr.entry(alkane_id).or_insert_with(SignedU128::zero);
                    *slot += SignedU128::positive(delta_amount);
                    if slot.is_zero() {
                        per_addr.remove(&alkane_id);
                    }
                    let activity_by_addr =
                        address_activity_received_delta.entry(address_str.clone()).or_default();
                    *activity_by_addr.entry(alkane_id).or_default() = activity_by_addr
                        .get(&alkane_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta_amount);
                    tx_transfer_participants_by_alkane
                        .entry(alkane_id)
                        .or_default()
                        .insert(address_str.clone());
                    add_holder_delta(
                        alkane_id,
                        HolderId::Address(address_str.clone()),
                        SignedU128::positive(delta_amount),
                        &mut holder_alkanes_changed,
                    );
                    *stat_plus_by_alk.entry(alkane_id).or_default() = stat_plus_by_alk
                        .get(&alkane_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta_amount);
                }

                stat_outpoints_written += 1;
            }
        }

        if !tx_transfer_amounts_by_alkane.is_empty() {
            for (alkane_id, amount) in tx_transfer_amounts_by_alkane {
                let Some(participants) = tx_transfer_participants_by_alkane.get(&alkane_id) else {
                    continue;
                };
                if participants.is_empty() {
                    continue;
                }
                let per_addr = transfer_volume_delta.entry(alkane_id).or_default();
                for addr in participants {
                    *per_addr.entry(addr.clone()).or_default() =
                        per_addr.get(addr).copied().unwrap_or(0).saturating_add(amount);
                    let activity = address_activity_transfer_delta.entry(addr.clone()).or_default();
                    *activity.entry(alkane_id).or_default() =
                        activity.get(&alkane_id).copied().unwrap_or(0).saturating_add(amount);
                }
            }
        }
        for (holder_alk, per_token) in &local_alkane_delta {
            let entry_outflow = AlkaneBalanceTxEntry {
                txid: txid_bytes,
                height: block.height,
                outflow: per_token.clone(),
            };
            for (token, delta) in per_token {
                if delta.is_zero() {
                    continue;
                }
                alkane_balance_delta_src.insert((*holder_alk, *token), entry_outflow.clone());
                push_balance_tx_entry_pair(
                    &mut alkane_balance_tx_entries_by_token,
                    *holder_alk,
                    *token,
                    entry_outflow.clone(),
                );
                if *token == *holder_alk {
                    // Keep self-token outflows for summaries/ammdata, but don't persist balances.
                    continue;
                }
                add_holder_delta(
                    *token,
                    HolderId::Alkane(*holder_alk),
                    *delta,
                    &mut holder_alkanes_changed,
                );
                let (is_negative, mag) = delta.as_parts();
                if is_negative {
                    *stat_minus_by_alk.entry(*token).or_default() =
                        stat_minus_by_alk.get(token).copied().unwrap_or(0).saturating_add(mag);
                } else {
                    *stat_plus_by_alk.entry(*token).or_default() =
                        stat_plus_by_alk.get(token).copied().unwrap_or(0).saturating_add(mag);
                }
                let entry = alkane_balance_delta.entry(*holder_alk).or_default();
                let slot = entry.entry(*token).or_insert_with(SignedU128::zero);
                *slot += *delta;
                if slot.is_zero() {
                    entry.remove(token);
                }
            }
        }

        for owner in &holder_alkanes_changed {
            let outflow = local_alkane_delta.get(owner).cloned().unwrap_or_else(BTreeMap::new);
            let entry = AlkaneBalanceTxEntry { txid: txid_bytes, height: block.height, outflow };
            push_balance_tx_entry(&mut alkane_balance_tx_entries, *owner, entry);
        }

        let is_alkane_tx = has_alkane_vin || has_traces;
        if is_alkane_tx {
            for output in &tx.output {
                if is_op_return(&output.script_pubkey) {
                    continue;
                }
                if let Some(addr) = spk_to_address_str(&output.script_pubkey, network) {
                    tx_addrs.insert(addr);
                }
            }

            let mut outflows: Vec<AlkaneBalanceTxEntry> = Vec::new();
            for (_owner, per_token) in &local_alkane_delta {
                if per_token.is_empty() {
                    continue;
                }
                outflows.push(AlkaneBalanceTxEntry {
                    txid: txid_bytes,
                    height: block.height,
                    outflow: per_token.clone(),
                });
            }

            let traces: Vec<EspoSandshrewLikeTrace> = if has_traces {
                atx.traces
                    .as_ref()
                    .map(|list| list.iter().map(|t| t.sandshrew_trace.clone()).collect())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            alkane_tx_summaries.push(AlkaneTxSummary {
                txid: txid_bytes,
                traces,
                outflows,
                height: block.height,
            });
            alkane_block_txids.push(txid_bytes);
            for addr in &tx_addrs {
                alkane_address_txids.entry(addr.clone()).or_default().push(txid_bytes);
            }
            if has_traces {
                latest_trace_txids.push(txid_bytes);
            }
        }
    }

    debug::log_elapsed(module, "process_transactions_loop", process_timer);
    if debug {
        eprintln!(
            "[balances] process loop stats: spent_outpoints={} ephem_outpoints={} consumed_ephem={} addr_balance_delta_addrs={} holders_delta_alkanes={} alkane_balance_delta_owners={} tx_summaries={} block_txids={} latest_trace_txids={}",
            spent_outpoints.len(),
            ephem_outpoint_balances.len(),
            consumed_ephem_outpoints.len(),
            address_balance_delta.len(),
            holders_delta.len(),
            alkane_balance_delta.len(),
            alkane_tx_summaries.len(),
            alkane_block_txids.len(),
            latest_trace_txids.len()
        );
    }

    // Ensure txid indexes are recorded for every alkane/token delta we are about to persist.
    for ((owner, token), entry) in &alkane_balance_delta_src {
        push_balance_tx_entry(&mut alkane_balance_tx_entries, *owner, entry.clone());
        push_balance_tx_entry_pair(
            &mut alkane_balance_tx_entries_by_token,
            *owner,
            *token,
            entry.clone(),
        );
    }

    // Accumulate alkane holder deltas (alkane -> token) and prepare rows for persistence.
    let timer = debug::start_if(debug);
    let mut alkane_balances_rows: HashMap<SchemaAlkaneId, Vec<BalanceEntry>> = HashMap::new();
    if !alkane_balance_delta.is_empty() {
        let mut owners: Vec<SchemaAlkaneId> = alkane_balance_delta.keys().copied().collect();
        owners.sort();
        for owner in owners.iter() {
            let mut amounts: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
            let bal_len = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.alkane_balance_list_len_key(owner),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if bal_len > 0 {
                let mut idx_keys = Vec::with_capacity(bal_len as usize);
                for idx in 0..bal_len {
                    idx_keys.push(table.alkane_balance_list_idx_key(owner, idx));
                }
                let idx_vals = provider
                    .get_multi_values(GetMultiValuesParams {
                        blockhash: StateAt::Latest,
                        keys: idx_keys,
                    })?
                    .values;
                let mut tokens = Vec::new();
                let mut bal_keys = Vec::new();
                for idx_val in idx_vals {
                    let Some(raw) = idx_val else { continue };
                    if raw.len() != 12 {
                        continue;
                    }
                    let token = SchemaAlkaneId {
                        block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                        tx: u64::from_be_bytes([
                            raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                        ]),
                    };
                    bal_keys.push(table.alkane_balance_key(owner, &token));
                    tokens.push(token);
                }
                let vals = provider
                    .get_multi_values(GetMultiValuesParams {
                        blockhash: StateAt::Latest,
                        keys: bal_keys,
                    })?
                    .values;
                for (token, value) in tokens.into_iter().zip(vals.into_iter()) {
                    let Some(bytes) = value else { continue };
                    let Ok(amount) = decode_u128_value(&bytes) else {
                        continue;
                    };
                    if amount == 0 {
                        continue;
                    }
                    amounts.insert(token, amount);
                }
            }

            if let Some(delta_map) = alkane_balance_delta.get(owner) {
                for (token, delta) in delta_map {
                    let (is_negative, mag) = delta.as_parts();
                    if mag == 0 {
                        continue;
                    }
                    let cur = amounts.get(token).copied().unwrap_or(0);
                    let updated = if !is_negative {
                        cur.saturating_add(mag)
                    } else {
                        if mag > cur {
                            let txid_str = alkane_balance_delta_src
                                .get(&(*owner, *token))
                                .map(|entry| Txid::from_byte_array(entry.txid))
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            if strict_check_alkane_balances() {
                                panic!(
                                    "[balances] negative alkane balance detected (txid={}, owner={}:{}, token={}:{}, cur={}, sub={})",
                                    txid_str, owner.block, owner.tx, token.block, token.tx, cur, mag
                                );
                            } else {
                                eprintln!(
                                    "[balances][warn] negative alkane balance clamped to 0 (txid={}, owner={}:{}, token={}:{}, cur={}, sub={})",
                                    txid_str, owner.block, owner.tx, token.block, token.tx, cur, mag
                                );
                            }
                            0
                        } else {
                            cur - mag
                        }
                    };
                    if updated == 0 {
                        amounts.remove(token);
                    } else {
                        amounts.insert(*token, updated);
                    }
                }
            }

            let mut vec_entries: Vec<BalanceEntry> = amounts
                .into_iter()
                .map(|(alkane, amount)| BalanceEntry { alkane, amount })
                .collect();
            vec_entries
                .sort_by(|a, b| b.amount.cmp(&a.amount).then_with(|| a.alkane.cmp(&b.alkane)));
            alkane_balances_rows.insert(*owner, vec_entries);
        }
    }
    debug::log_elapsed(module, "process_transactions_build_balance_rows", timer);
    if debug {
        eprintln!(
            "[balances] balance rows stats: owners={} tx_entries_owner={} tx_entries_pair={} delta_src={}",
            alkane_balances_rows.len(),
            alkane_balance_tx_entries.len(),
            alkane_balance_tx_entries_by_token.len(),
            alkane_balance_delta_src.len()
        );
    }

    let timer = debug::start_if(debug);
    debug::log_elapsed(module, "process_transactions_address_offsets", timer);

    let timer = debug::start_if(debug);
    let latest_traces_prev_len = provider
        .get_raw_value(GetRawValueParams {
            blockhash: StateAt::Latest,
            key: table.latest_traces_length_key(),
        })?
        .value
        .and_then(|b| {
            if b.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&b);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut latest_traces: Vec<[u8; 32]> = Vec::with_capacity(latest_traces_prev_len as usize);
    if latest_traces_prev_len > 0 {
        let mut keys = Vec::with_capacity(latest_traces_prev_len as usize);
        for idx in 0..latest_traces_prev_len {
            keys.push(table.latest_traces_idx_key(idx));
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
            .values;
        for val in vals.into_iter().flatten() {
            if val.len() != 32 {
                continue;
            }
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&val);
            latest_traces.push(txid);
        }
    }
    if !latest_trace_txids.is_empty() {
        for txid in latest_trace_txids {
            latest_traces.insert(0, txid);
        }
        if latest_traces.len() > 20 {
            latest_traces.truncate(20);
        }
    }
    debug::log_elapsed(module, "process_transactions_latest_traces", timer);

    debug::log_elapsed(module, "process_transactions", process_timer);

    // logging metric
    stat_outpoints_marked_spent = spent_outpoints.len();

    // Build unified rows (new outputs + spent inputs)
    let timer = debug::start_if(debug);
    struct NewRow {
        outpoint: EspoOutpoint,
        addr: String,
        enc_balances: Vec<u8>,
        uspk_val: Option<Vec<u8>>, // spk bytes
    }
    let mut new_rows: Vec<NewRow> = Vec::new();

    // map outpoint string -> row data
    let mut row_map: HashMap<String, NewRow> = HashMap::new();

    // Persist block-created outputs (mark as spent if consumed within same block)
    for (out_str, vec_out) in &ephem_outpoint_balances {
        let addr = match ephem_outpoint_addr.get(out_str) {
            Some(a) => a.clone(),
            None => continue,
        };
        let mut op = match ephem_outpoint_struct.get(out_str) {
            Some(o) => o.clone(),
            None => continue,
        };

        if let Some(spender) = consumed_ephem_outpoints.get(out_str) {
            op.tx_spent = Some(spender.clone());
        }

        let enc_balances = encode_vec(vec_out)?;
        let uspk_val = ephem_outpoint_spk.get(out_str).map(|spk| spk.as_bytes().to_vec());

        row_map.insert(out_str.clone(), NewRow { outpoint: op, addr, enc_balances, uspk_val });
    }

    // Persist external inputs (spent) and any ephemerals consumed in-block
    for (out_str, rec) in &spent_outpoints {
        let addr = match &rec.addr {
            Some(a) => a.clone(),
            None => continue,
        };
        let mut op = rec.outpoint.clone();
        op.tx_spent = Some(rec.spent_by.clone());
        let enc_balances = encode_vec(&rec.balances)?;
        let uspk_val = rec.spk.as_ref().map(|spk| spk.as_bytes().to_vec());

        row_map
            .entry(out_str.clone())
            .and_modify(|row| {
                row.outpoint.tx_spent = Some(rec.spent_by.clone());
                if row.uspk_val.is_none() {
                    row.uspk_val = uspk_val.clone();
                }
            })
            .or_insert(NewRow { outpoint: op, addr, enc_balances, uspk_val });
    }

    for (_, row) in row_map {
        new_rows.push(row);
    }
    debug::log_elapsed(module, "process_transactions_build_new_rows", timer);
    if debug {
        let spent_rows = new_rows.iter().filter(|r| r.outpoint.tx_spent.is_some()).count();
        eprintln!(
            "[balances] new_rows stats: total={} spent={} unspent={}",
            new_rows.len(),
            spent_rows,
            new_rows.len().saturating_sub(spent_rows)
        );
    }

    // ---- single write-batch ----
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut blob_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut section_start_puts = 0usize;
    let mut section_start_deletes = 0usize;

    // A) Persist outpoint pointer blobs + COW id locators/spend edges.
    let outpoint_counter_key = table.outpoint_pointer_counter_key();
    let mut next_outpoint_id = provider
        .blob_mdb()
        .get(&outpoint_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);
    let outpoint_idx_chunk_counter_key =
        table.address_index_chunk_counter_key(AddressIndexListKind::OutpointIdx);
    let mut next_outpoint_idx_chunk_id = provider
        .blob_mdb()
        .get(&outpoint_idx_chunk_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);

    let mut created_outpoint_ids: HashMap<(Txid, u32), u64> = HashMap::new();
    let mut created_outpoint_txidx: HashMap<(Txid, u32), u32> = HashMap::new();
    for row in &new_rows {
        if row.outpoint.txid.len() != 32 {
            continue;
        }
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&row.outpoint.txid);
        let Some(tx_idx) = tx_index_by_txid.get(&txid_arr).copied() else {
            continue;
        };
        let txid = Txid::from_byte_array(txid_arr);
        let key = (txid, row.outpoint.vout);
        if created_outpoint_ids.contains_key(&key) {
            continue;
        }
        created_outpoint_ids.insert(key, next_outpoint_id);
        created_outpoint_txidx.insert((txid, row.outpoint.vout), tx_idx);
        next_outpoint_id = next_outpoint_id.saturating_add(1);
    }

    let mut outpoint_idx_appends: HashMap<String, Vec<u64>> = HashMap::new();
    let mut outpoint_pos_updates: HashMap<([u8; 32], u32), u64> = HashMap::new();
    let mut outpoint_spent_updates: HashMap<u64, [u8; 32]> = HashMap::new();

    let mut external_spent_candidates: Vec<(Txid, u32)> = Vec::new();
    let mut external_spent_set: HashSet<(Txid, u32)> = HashSet::new();
    for row in &new_rows {
        if row.outpoint.tx_spent.is_none() || row.outpoint.txid.len() != 32 {
            continue;
        }
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&row.outpoint.txid);
        let txid = Txid::from_byte_array(txid_arr);
        let key = (txid, row.outpoint.vout);
        if created_outpoint_ids.contains_key(&key) {
            continue;
        }
        if external_spent_set.insert(key) {
            external_spent_candidates.push((txid, row.outpoint.vout));
        }
    }
    let mut external_spent_ids: HashMap<(Txid, u32), u64> = HashMap::new();
    if !external_spent_candidates.is_empty() {
        let resolved = resolve_outpoint_ids_batch_v2(
            provider,
            StateAt::Latest,
            external_spent_candidates.as_slice(),
        )?;
        for ((txid, vout), id) in external_spent_candidates.iter().zip(resolved.into_iter()) {
            if let Some(id) = id {
                external_spent_ids.insert((*txid, *vout), id);
            }
        }
    }

    let mut addr_spk_updates: HashMap<String, Vec<u8>> = HashMap::new();
    for row in &new_rows {
        if row.outpoint.txid.len() != 32 {
            continue;
        }
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&row.outpoint.txid);
        let txid = Txid::from_byte_array(txid_arr);
        let key = (txid, row.outpoint.vout);

        if let Some(outpoint_id) = created_outpoint_ids.get(&key).copied() {
            let Some(tx_idx) = created_outpoint_txidx.get(&key).copied() else {
                continue;
            };
            let balances = decode_balances_vec(&row.enc_balances).unwrap_or_default();
            let spk_bytes = row.uspk_val.clone().unwrap_or_default();
            let row_blob = match encode_outpoint_pointer_blob_v3(
                &txid_arr,
                row.outpoint.vout,
                &blockhash,
                tx_idx,
                &row.addr,
                &spk_bytes,
                &balances,
            ) {
                Ok(v) => v,
                Err(_) => continue,
            };
            blob_puts.push((table.outpoint_pointer_blob_key(outpoint_id), row_blob));
            outpoint_pos_updates.insert((txid_arr, row.outpoint.vout), outpoint_id);
            if row.outpoint.tx_spent.is_none() {
                outpoint_idx_appends.entry(row.addr.clone()).or_default().push(outpoint_id);
            }
        }

        if let Some(ref spent_by) = row.outpoint.tx_spent {
            if spent_by.len() == 32 {
                let outpoint_id = created_outpoint_ids
                    .get(&key)
                    .copied()
                    .or_else(|| external_spent_ids.get(&key).copied());
                if let Some(id) = outpoint_id {
                    let mut spender_arr = [0u8; 32];
                    spender_arr.copy_from_slice(spent_by);
                    outpoint_spent_updates.insert(id, spender_arr);
                }
            }
        }
        if let Some(ref spk_bytes) = row.uspk_val {
            addr_spk_updates.entry(row.addr.clone()).or_insert_with(|| spk_bytes.clone());
        }
    }

    if !outpoint_idx_appends.is_empty() {
        let mut addrs: Vec<String> = outpoint_idx_appends.keys().cloned().collect();
        addrs.sort();
        for addr in addrs {
            let Some(values) = outpoint_idx_appends.get(&addr) else {
                continue;
            };
            append_address_index_values(
                provider,
                AddressIndexListKind::OutpointIdx,
                &addr,
                values,
                &mut next_outpoint_idx_chunk_id,
                &mut puts,
                &mut blob_puts,
            )?;
        }
    }

    if !addr_spk_updates.is_empty() {
        let mut addrs: Vec<String> = addr_spk_updates.keys().cloned().collect();
        addrs.sort();
        let keys: Vec<Vec<u8>> = addrs.iter().map(|a| table.addr_spk_key(a)).collect();
        let existing = provider
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: keys.clone(),
            })?
            .values;
        for (idx, addr) in addrs.into_iter().enumerate() {
            let Some(next) = addr_spk_updates.remove(&addr) else {
                continue;
            };
            let same = existing
                .get(idx)
                .and_then(|v| v.as_ref())
                .map(|cur| cur.as_slice() == next.as_slice())
                .unwrap_or(false);
            if !same {
                puts.push((keys[idx].clone(), next));
            }
        }
    }
    if debug {
        eprintln!(
            "[balances] writes A/outpoints: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
    }
    section_start_puts = puts.len();
    section_start_deletes = deletes.len();

    // B) Persist address/token balances as signed deltas.
    let mut address_added_tokens: HashMap<String, HashSet<SchemaAlkaneId>> = HashMap::new();
    let mut address_removed_tokens: HashMap<String, u32> = HashMap::new();
    let mut address_full_rebuilds = 0usize;
    let mut address_full_rebuild_entries = 0usize;
    for (address, per_token) in &address_balance_delta {
        for (token, delta) in per_token {
            let key = table.address_balance_key(address, token);
            let current_raw = provider
                .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: key.clone() })?
                .value;
            let current =
                current_raw.as_ref().and_then(|raw| decode_u128_value(raw).ok()).unwrap_or(0);
            let (is_negative, mag) = delta.as_parts();
            let next = if is_negative {
                if mag > current {
                    panic!(
                        "[balances] negative address balance detected (addr={}, token={}:{}, cur={}, sub={})",
                        address, token.block, token.tx, current, mag
                    );
                }
                current - mag
            } else {
                current.saturating_add(mag)
            };
            puts.push((key, encode_u128_value(next)?));
            if current == 0 && next > 0 {
                address_added_tokens.entry(address.clone()).or_default().insert(*token);
            } else if current > 0 && next == 0 {
                let counter = address_removed_tokens.entry(address.clone()).or_insert(0);
                *counter = counter.saturating_add(1);
            }
        }
    }
    let mut address_membership_touched: HashSet<String> = HashSet::new();
    address_membership_touched.extend(address_added_tokens.keys().cloned());
    address_membership_touched.extend(address_removed_tokens.keys().cloned());
    for address in address_membership_touched {
        let added_tokens = address_added_tokens.remove(&address).unwrap_or_default();
        let removed_count = address_removed_tokens.get(&address).copied().unwrap_or(0);
        if added_tokens.is_empty() && removed_count == 0 {
            continue;
        }
        let len = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.address_balance_list_len_key(&address),
            })?
            .value
            .and_then(|bytes| {
                if bytes.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&bytes);
                    Some(u32::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        let mut existing_tokens: Vec<SchemaAlkaneId> = Vec::new();
        if len > 0 {
            let mut idx_keys = Vec::with_capacity(len as usize);
            for idx in 0..len {
                idx_keys.push(table.address_balance_list_idx_key(&address, idx));
            }
            let idx_vals = provider
                .get_multi_values(GetMultiValuesParams {
                    blockhash: StateAt::Latest,
                    keys: idx_keys,
                })?
                .values;
            for idx_val in idx_vals {
                let Some(raw) = idx_val else { continue };
                if raw.len() != 12 {
                    continue;
                }
                existing_tokens.push(SchemaAlkaneId {
                    block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                    tx: u64::from_be_bytes([
                        raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                    ]),
                });
            }
        }

        let mut existing_nonzero: Vec<SchemaAlkaneId> = Vec::new();
        let mut had_zero_in_existing_list = false;
        if !existing_tokens.is_empty() {
            let mut balance_keys = Vec::with_capacity(existing_tokens.len());
            for token in &existing_tokens {
                balance_keys.push(table.address_balance_key(&address, token));
            }
            let balance_vals = provider
                .get_multi_values(GetMultiValuesParams {
                    blockhash: StateAt::Latest,
                    keys: balance_keys,
                })?
                .values;
            for (token, val) in existing_tokens.into_iter().zip(balance_vals.into_iter()) {
                let amount = val.as_ref().and_then(|raw| decode_u128_value(raw).ok()).unwrap_or(0);
                if amount == 0 {
                    had_zero_in_existing_list = true;
                } else {
                    existing_nonzero.push(token);
                }
            }
        }

        if !had_zero_in_existing_list && removed_count == 0 {
            if added_tokens.is_empty() {
                continue;
            }
            let appended_count = added_tokens.len() as u32;
            let mut appended: Vec<SchemaAlkaneId> = added_tokens.into_iter().collect();
            appended.sort();
            let base = len;
            for (offset, token) in appended.into_iter().enumerate() {
                let mut token_bytes = Vec::with_capacity(12);
                token_bytes.extend_from_slice(&token.block.to_be_bytes());
                token_bytes.extend_from_slice(&token.tx.to_be_bytes());
                puts.push((
                    table
                        .address_balance_list_idx_key(&address, base.saturating_add(offset as u32)),
                    token_bytes,
                ));
            }
            let new_len = base.saturating_add(appended_count);
            puts.push((
                table.address_balance_list_len_key(&address),
                new_len.to_le_bytes().to_vec(),
            ));
            continue;
        }

        let mut token_set: HashSet<SchemaAlkaneId> = existing_nonzero.into_iter().collect();
        token_set.extend(added_tokens.into_iter());
        let mut final_tokens: Vec<SchemaAlkaneId> = token_set.into_iter().collect();
        final_tokens.sort();
        let new_len = final_tokens.len() as u32;
        address_full_rebuilds = address_full_rebuilds.saturating_add(1);
        address_full_rebuild_entries =
            address_full_rebuild_entries.saturating_add(new_len as usize);
        puts.push((table.address_balance_list_len_key(&address), new_len.to_le_bytes().to_vec()));
        for (idx, token) in final_tokens.into_iter().enumerate() {
            let mut token_bytes = Vec::with_capacity(12);
            token_bytes.extend_from_slice(&token.block.to_be_bytes());
            token_bytes.extend_from_slice(&token.tx.to_be_bytes());
            puts.push((table.address_balance_list_idx_key(&address, idx as u32), token_bytes));
        }
        if len > new_len {
            for idx in new_len..len {
                deletes.push(table.address_balance_list_idx_key(&address, idx));
            }
        }
    }
    if debug {
        eprintln!(
            "[balances] writes B/address_balances: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
        eprintln!(
            "[balances] writes B/address_balances details: full_rebuilds={} rebuilt_entries={}",
            address_full_rebuilds, address_full_rebuild_entries
        );
    }
    section_start_puts = puts.len();
    section_start_deletes = deletes.len();

    // C) Persist alkane holder balances as per-token rows.
    let mut alkane_balance_full_rebuilds = 0usize;
    let mut alkane_balance_full_rebuild_entries = 0usize;
    for (owner, entries) in alkane_balances_rows.iter() {
        let mut final_amounts: HashMap<SchemaAlkaneId, u128> =
            HashMap::with_capacity(entries.len());
        for be in entries {
            final_amounts.insert(be.alkane, be.amount);
        }

        let mut changed_tokens: Vec<SchemaAlkaneId> =
            if let Some(delta_map) = alkane_balance_delta.get(owner) {
                delta_map.keys().copied().collect()
            } else {
                final_amounts.keys().copied().collect()
            };
        changed_tokens.sort();
        changed_tokens.dedup();

        for token in &changed_tokens {
            let amount = final_amounts.get(token).copied().unwrap_or(0);
            puts.push((table.alkane_balance_key(owner, token), encode_u128_value(amount)?));
        }

        if !changed_tokens.is_empty() {
            let mut added_tokens: HashSet<SchemaAlkaneId> = HashSet::new();
            let mut removed_tokens: u32 = 0;
            for token in &changed_tokens {
                let current = provider
                    .get_raw_value(GetRawValueParams {
                        blockhash: StateAt::Latest,
                        key: table.alkane_balance_key(owner, token),
                    })?
                    .value
                    .and_then(|raw| decode_u128_value(&raw).ok())
                    .unwrap_or(0);
                let next = final_amounts.get(token).copied().unwrap_or(0);
                if current == 0 && next > 0 {
                    added_tokens.insert(*token);
                } else if current > 0 && next == 0 {
                    removed_tokens = removed_tokens.saturating_add(1);
                }
            }

            let len = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.alkane_balance_list_len_key(owner),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            let mut existing_tokens: Vec<SchemaAlkaneId> = Vec::new();
            if len > 0 {
                let mut idx_keys = Vec::with_capacity(len as usize);
                for idx in 0..len {
                    idx_keys.push(table.alkane_balance_list_idx_key(owner, idx));
                }
                let idx_vals = provider
                    .get_multi_values(GetMultiValuesParams {
                        blockhash: StateAt::Latest,
                        keys: idx_keys,
                    })?
                    .values;
                for idx_val in idx_vals {
                    let Some(raw) = idx_val else { continue };
                    if raw.len() != 12 {
                        continue;
                    }
                    existing_tokens.push(SchemaAlkaneId {
                        block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                        tx: u64::from_be_bytes([
                            raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                        ]),
                    });
                }
            }

            let mut existing_nonzero: Vec<SchemaAlkaneId> = Vec::new();
            let mut had_zero_in_existing_list = false;
            if !existing_tokens.is_empty() {
                let mut balance_keys = Vec::with_capacity(existing_tokens.len());
                for token in &existing_tokens {
                    balance_keys.push(table.alkane_balance_key(owner, token));
                }
                let balance_vals = provider
                    .get_multi_values(GetMultiValuesParams {
                        blockhash: StateAt::Latest,
                        keys: balance_keys,
                    })?
                    .values;
                for (token, val) in existing_tokens.into_iter().zip(balance_vals.into_iter()) {
                    let amount =
                        val.as_ref().and_then(|raw| decode_u128_value(raw).ok()).unwrap_or(0);
                    if amount == 0 {
                        had_zero_in_existing_list = true;
                    } else {
                        existing_nonzero.push(token);
                    }
                }
            }

            if !had_zero_in_existing_list && removed_tokens == 0 {
                if !added_tokens.is_empty() {
                    let appended_count = added_tokens.len() as u32;
                    let mut appended: Vec<SchemaAlkaneId> = added_tokens.into_iter().collect();
                    appended.sort();
                    let base = len;
                    for (offset, token) in appended.into_iter().enumerate() {
                        let mut token_bytes = Vec::with_capacity(12);
                        token_bytes.extend_from_slice(&token.block.to_be_bytes());
                        token_bytes.extend_from_slice(&token.tx.to_be_bytes());
                        puts.push((
                            table.alkane_balance_list_idx_key(
                                owner,
                                base.saturating_add(offset as u32),
                            ),
                            token_bytes,
                        ));
                    }
                    let new_len = base.saturating_add(appended_count);
                    puts.push((
                        table.alkane_balance_list_len_key(owner),
                        new_len.to_le_bytes().to_vec(),
                    ));
                }
            } else {
                let mut final_tokens: Vec<SchemaAlkaneId> = final_amounts.keys().copied().collect();
                final_tokens.sort();
                let new_len = final_tokens.len() as u32;
                alkane_balance_full_rebuilds = alkane_balance_full_rebuilds.saturating_add(1);
                alkane_balance_full_rebuild_entries =
                    alkane_balance_full_rebuild_entries.saturating_add(new_len as usize);
                puts.push((
                    table.alkane_balance_list_len_key(owner),
                    new_len.to_le_bytes().to_vec(),
                ));
                for (idx, token) in final_tokens.into_iter().enumerate() {
                    let mut token_bytes = Vec::with_capacity(12);
                    token_bytes.extend_from_slice(&token.block.to_be_bytes());
                    token_bytes.extend_from_slice(&token.tx.to_be_bytes());
                    puts.push((table.alkane_balance_list_idx_key(owner, idx as u32), token_bytes));
                }
                if len > new_len {
                    for idx in new_len..len {
                        deletes.push(table.alkane_balance_list_idx_key(owner, idx));
                    }
                }
            }
        }

        for token in changed_tokens {
            let amount = final_amounts.get(&token).copied().unwrap_or(0);
            puts.push((
                table.alkane_balance_by_height_key(owner, &token, block.height),
                encode_u128_value(amount)?,
            ));
            let height_len_key = table.alkane_balance_by_height_list_len_key(owner, &token);
            let height_len = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: height_len_key.clone(),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            puts.push((
                table.alkane_balance_by_height_list_idx_key(owner, &token, height_len),
                block.height.to_be_bytes().to_vec(),
            ));
            puts.push((height_len_key, (height_len.saturating_add(1)).to_le_bytes().to_vec()));
        }
    }
    if debug {
        eprintln!(
            "[balances] writes C/alkane_balances: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
        eprintln!(
            "[balances] writes C/alkane_balances details: full_rebuilds={} rebuilt_entries={}",
            alkane_balance_full_rebuilds, alkane_balance_full_rebuild_entries
        );
    }
    section_start_puts = puts.len();
    section_start_deletes = deletes.len();

    // D) Persist append-only alkane balance tx logs + tx summaries.
    let timer = debug::start_if(debug);
    let mut log_rows_owner: usize = 0;
    let mut log_rows_height: usize = 0;
    let mut log_rows_pair: usize = 0;

    let tx_pointer_counter_key = table.tx_pointer_counter_key();
    let mut next_tx_pointer_id = provider
        .blob_mdb()
        .get(&tx_pointer_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);
    let by_token_chunk_counter_key =
        table.address_index_chunk_counter_key(AddressIndexListKind::AlkaneBalanceTxsByToken);
    let mut next_by_token_chunk_id = provider
        .blob_mdb()
        .get(&by_token_chunk_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);
    let alkane_block_chunk_counter_key =
        table.address_index_chunk_counter_key(AddressIndexListKind::AlkaneBlockTxs);
    let mut next_alkane_block_chunk_id = provider
        .blob_mdb()
        .get(&alkane_block_chunk_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);
    let alkane_addr_chunk_counter_key =
        table.address_index_chunk_counter_key(AddressIndexListKind::AlkaneTxs);
    let mut next_alkane_addr_chunk_id = provider
        .blob_mdb()
        .get(&alkane_addr_chunk_counter_key)?
        .and_then(|bytes| decode_pointer_idx_u64(&bytes).ok())
        .unwrap_or(0);
    let mut tx_pointer_id_by_txid: HashMap<[u8; 32], u64> = HashMap::new();
    for summary in &alkane_tx_summaries {
        if !tx_index_by_txid.contains_key(&summary.txid) {
            continue;
        }
        if tx_pointer_id_by_txid.insert(summary.txid, next_tx_pointer_id).is_none() {
            next_tx_pointer_id = next_tx_pointer_id.saturating_add(1);
        }
    }

    if !alkane_balance_tx_entries.is_empty() {
        let mut tokens: Vec<SchemaAlkaneId> = alkane_balance_tx_entries.keys().copied().collect();
        tokens.sort();
        for tok in &tokens {
            if let Some(new_entries) = alkane_balance_tx_entries.get(tok) {
                for entry in new_entries {
                    let Some(tx_idx) = tx_index_by_txid.get(&entry.txid).copied() else {
                        continue;
                    };
                    let Some(entry_id) = tx_pointer_id_by_txid.get(&entry.txid).copied() else {
                        continue;
                    };
                    let key = table.alkane_balance_txs_log_key(tok, entry.height, tx_idx, entry_id);
                    puts.push((key, Vec::new()));
                    log_rows_owner = log_rows_owner.saturating_add(1);

                    let by_height_key =
                        table.alkane_balance_txs_by_height_log_key(entry.height, tx_idx, tok);
                    puts.push((by_height_key, encode_pointer_idx_u64(entry_id)));
                    log_rows_height = log_rows_height.saturating_add(1);
                }
            }
        }
    }

    let mut by_token_pointer_appends: HashMap<String, Vec<u64>> = HashMap::new();
    if !alkane_balance_tx_entries_by_token.is_empty() {
        let mut pairs: Vec<(SchemaAlkaneId, SchemaAlkaneId)> =
            alkane_balance_tx_entries_by_token.keys().copied().collect();
        pairs.sort();
        for (owner, token) in &pairs {
            if let Some(new_entries) = alkane_balance_tx_entries_by_token.get(&(*owner, *token)) {
                for entry in new_entries {
                    let Some(entry_id) = tx_pointer_id_by_txid.get(&entry.txid).copied() else {
                        continue;
                    };
                    let list_id = address_index_list_id_alkane_balance_txs_by_token(owner, token);
                    by_token_pointer_appends.entry(list_id).or_default().push(entry_id);
                    log_rows_pair = log_rows_pair.saturating_add(1);
                }
            }
        }
    }
    if !by_token_pointer_appends.is_empty() {
        let mut list_ids: Vec<String> = by_token_pointer_appends.keys().cloned().collect();
        list_ids.sort();
        for list_id in list_ids {
            let Some(values) = by_token_pointer_appends.get(&list_id) else {
                continue;
            };
            append_address_index_values(
                provider,
                AddressIndexListKind::AlkaneBalanceTxsByToken,
                &list_id,
                values,
                &mut next_by_token_chunk_id,
                &mut puts,
                &mut blob_puts,
            )?;
        }
    }

    if debug {
        eprintln!(
            "[balances] append_balance_txs stats: owner_rows={} by_height_rows={} owner_token_rows={}",
            log_rows_owner, log_rows_height, log_rows_pair
        );
    }
    debug::log_elapsed(module, "process_transactions_update_balance_txs_append", timer);

    // D2) Persist alkane tx pointer blobs (txid -> id, id -> immutable blob).
    let mut packed_outflows_by_tx: HashMap<
        [u8; 32],
        BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    > = HashMap::new();
    let mut tx_pos_updates: HashMap<[u8; 32], u64> = HashMap::new();
    for (owner, entries) in &alkane_balance_tx_entries {
        for entry in entries {
            packed_outflows_by_tx
                .entry(entry.txid)
                .or_default()
                .entry(*owner)
                .or_insert_with(|| entry.outflow.clone());
        }
    }
    for summary in &alkane_tx_summaries {
        let Some(tx_idx) = tx_index_by_txid.get(&summary.txid).copied() else {
            continue;
        };
        let Some(pointer_id) = tx_pointer_id_by_txid.get(&summary.txid).copied() else {
            continue;
        };
        tx_pos_updates.insert(summary.txid, pointer_id);

        let packed = packed_outflows_by_tx.remove(&summary.txid).unwrap_or_default();
        let Ok(row_value) = encode_tx_pointer_blob_v3(
            &summary.txid,
            &blockhash,
            tx_idx,
            summary.height,
            &summary.traces,
            &packed,
        ) else {
            continue;
        };
        blob_puts.push((table.tx_pointer_blob_key(pointer_id), row_value));
    }

    blob_puts.extend(build_outpoint_pos_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &outpoint_pos_updates,
    )?);
    blob_puts.extend(build_outpoint_spent_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &outpoint_spent_updates,
    )?);
    blob_puts.extend(build_tx_pos_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &tx_pos_updates,
    )?);

    let mut block_pointer_ids: Vec<u64> = Vec::with_capacity(alkane_block_txids.len());
    for txid_bytes in &alkane_block_txids {
        let Some(pointer_id) = tx_pointer_id_by_txid.get(txid_bytes).copied() else {
            continue;
        };
        block_pointer_ids.push(pointer_id);
    }
    if !block_pointer_ids.is_empty() {
        let list_id = address_index_list_id_alkane_block_txs(block.height as u64);
        append_address_index_values(
            provider,
            AddressIndexListKind::AlkaneBlockTxs,
            &list_id,
            &block_pointer_ids,
            &mut next_alkane_block_chunk_id,
            &mut puts,
            &mut blob_puts,
        )?;
    }

    for (addr, txids) in alkane_address_txids.iter() {
        let mut pointer_ids: Vec<u64> = Vec::with_capacity(txids.len());
        for txid_bytes in txids {
            let Some(pointer_id) = tx_pointer_id_by_txid.get(txid_bytes).copied() else {
                continue;
            };
            pointer_ids.push(pointer_id);
        }
        if pointer_ids.is_empty() {
            continue;
        }
        append_address_index_values(
            provider,
            AddressIndexListKind::AlkaneTxs,
            addr,
            &pointer_ids,
            &mut next_alkane_addr_chunk_id,
            &mut puts,
            &mut blob_puts,
        )?;
    }

    let latest_traces_new_len = latest_traces.len() as u32;
    if latest_traces_new_len == 0 {
        deletes.push(table.latest_traces_length_key());
    } else {
        puts.push((table.latest_traces_length_key(), latest_traces_new_len.to_le_bytes().to_vec()));
    }
    for (idx, txid) in latest_traces.iter().enumerate() {
        puts.push((table.latest_traces_idx_key(idx as u32), txid.to_vec()));
    }
    if latest_traces_prev_len > latest_traces_new_len {
        for idx in latest_traces_new_len..latest_traces_prev_len {
            deletes.push(table.latest_traces_idx_key(idx));
        }
    }

    if debug {
        eprintln!(
            "[balances] writes D/tx_indexes: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
    }
    section_start_puts = puts.len();
    section_start_deletes = deletes.len();

    // E) Holders deltas
    let mut holders_full_rebuilds = 0usize;
    let mut holders_full_rebuild_entries = 0usize;
    for (alkane, per_holder) in holders_delta.iter() {
        let holders_count_key = table.holders_count_key(alkane);
        let mut holder_amounts: BTreeMap<HolderId, u128> = BTreeMap::new();
        let mut had_zero_in_existing_list = false;
        let holder_len = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.holder_list_len_key(alkane),
            })?
            .value
            .and_then(|bytes| {
                if bytes.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&bytes);
                    Some(u32::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        if holder_len > 0 {
            let mut idx_keys = Vec::with_capacity(holder_len as usize);
            for idx in 0..holder_len {
                idx_keys.push(table.holder_list_idx_key(alkane, idx));
            }
            let idx_vals = provider
                .get_multi_values(GetMultiValuesParams {
                    blockhash: StateAt::Latest,
                    keys: idx_keys,
                })?
                .values;
            let mut holders = Vec::new();
            let mut holder_keys = Vec::new();
            for idx_val in idx_vals {
                let Some(raw) = idx_val else { continue };
                let holder = if raw.is_empty() {
                    continue;
                } else if raw[0] == b'a' {
                    let Ok(addr) = std::str::from_utf8(&raw[1..]).map(|s| s.to_string()) else {
                        continue;
                    };
                    HolderId::Address(addr)
                } else if raw[0] == b'k' && raw.len() == 13 {
                    HolderId::Alkane(SchemaAlkaneId {
                        block: u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]),
                        tx: u64::from_be_bytes([
                            raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12],
                        ]),
                    })
                } else {
                    continue;
                };
                holder_keys.push(table.holder_key(alkane, &holder));
                holders.push(holder);
            }
            let holder_vals = provider
                .get_multi_values(GetMultiValuesParams {
                    blockhash: StateAt::Latest,
                    keys: holder_keys,
                })?
                .values;
            for (holder, value) in holders.into_iter().zip(holder_vals.into_iter()) {
                let Some(bytes) = value else { continue };
                let Ok(amount) = decode_u128_value(&bytes) else {
                    continue;
                };
                if amount == 0 {
                    had_zero_in_existing_list = true;
                }
                holder_amounts.insert(holder, amount);
            }
        }

        let prev_count = holder_amounts.values().filter(|amt| **amt > 0).count() as u64;
        let mut changed_holders: Vec<(HolderId, u128)> = Vec::with_capacity(per_holder.len());
        let mut added_holders: Vec<HolderId> = Vec::new();
        let mut removed_holders: u32 = 0;
        let mut membership_changed = had_zero_in_existing_list;
        for (holder, delta) in per_holder {
            let cur = holder_amounts.get(holder).copied().unwrap_or(0);
            let (is_negative, mag) = delta.as_parts();
            let next = if is_negative {
                if mag > cur {
                    panic!(
                        "[balances] negative holder balance detected (alkane={}:{}, holder={:?}, cur={}, sub={})",
                        alkane.block, alkane.tx, holder, cur, mag
                    );
                }
                cur - mag
            } else {
                cur.saturating_add(mag)
            };
            if (cur > 0) != (next > 0) {
                membership_changed = true;
                if cur == 0 && next > 0 {
                    added_holders.push(holder.clone());
                } else if cur > 0 && next == 0 {
                    removed_holders = removed_holders.saturating_add(1);
                }
            }
            holder_amounts.insert(holder.clone(), next);
            changed_holders.push((holder.clone(), next));
        }
        holder_amounts.retain(|_, amount| *amount > 0);
        let new_count = holder_amounts.len() as u64;
        if search_index_enabled {
            let rec = provider
                .get_creation_record(crate::modules::essentials::storage::GetCreationRecordParams {
                    blockhash: StateAt::Latest,
                    alkane: *alkane,
                })
                .ok()
                .and_then(|resp| resp.record);
            if let Some(rec) = rec {
                let prefixes = collect_search_prefixes(
                    &rec.names,
                    &rec.symbols,
                    search_prefix_min,
                    search_prefix_max,
                );
                if !prefixes.is_empty() {
                    let mdb = ammdata_mdb();
                    let table_amm = AmmDataTable::new(mdb.as_ref());
                    for prefix in prefixes {
                        ammdata_puts.push((
                            table_amm.token_search_index_key_u64(
                                SearchIndexField::Holders,
                                &prefix,
                                new_count,
                                alkane,
                            ),
                            Vec::new(),
                        ));
                        if prev_count != new_count {
                            ammdata_deletes.push(table_amm.token_search_index_key_u64(
                                SearchIndexField::Holders,
                                &prefix,
                                prev_count,
                                alkane,
                            ));
                        }
                    }
                }
            }
        }
        let new_index_key = table.alkane_holders_ordered_key(new_count, alkane);
        if prev_count != new_count {
            let prev_index_key = table.alkane_holders_ordered_key(prev_count, alkane);
            deletes.push(prev_index_key);
        }
        puts.push((new_index_key, Vec::new()));

        let supply: u128 = holder_amounts.values().copied().sum();
        let supply_latest_key = table.circulating_supply_latest_key(alkane);
        let prev_supply = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: supply_latest_key.clone(),
            })?
            .value
            .and_then(|v| decode_u128_value(&v).ok())
            .unwrap_or(0);
        if supply != prev_supply {
            let encoded = encode_u128_value(supply)?;
            puts.push((table.circulating_supply_key(alkane, block.height), encoded.clone()));
            puts.push((supply_latest_key, encoded));
        }

        for (holder, amount) in changed_holders {
            let holder_key = table.holder_key(alkane, &holder);
            if amount == 0 {
                deletes.push(holder_key);
            } else {
                puts.push((holder_key, encode_u128_value(amount)?));
            }
        }

        if membership_changed {
            if !had_zero_in_existing_list && removed_holders == 0 {
                let added_count = added_holders.len() as u32;
                let mut added_holder_keys: Vec<Vec<u8>> = added_holders
                    .into_iter()
                    .map(|holder| match holder {
                        HolderId::Address(addr) => {
                            let mut out = Vec::with_capacity(1 + addr.len());
                            out.push(b'a');
                            out.extend_from_slice(addr.as_bytes());
                            out
                        }
                        HolderId::Alkane(id) => {
                            let mut out = Vec::with_capacity(13);
                            out.push(b'k');
                            out.extend_from_slice(&id.block.to_be_bytes());
                            out.extend_from_slice(&id.tx.to_be_bytes());
                            out
                        }
                    })
                    .collect();
                added_holder_keys.sort();
                let base = holder_len;
                for (offset, holder_key_bytes) in added_holder_keys.into_iter().enumerate() {
                    puts.push((
                        table.holder_list_idx_key(alkane, base.saturating_add(offset as u32)),
                        holder_key_bytes,
                    ));
                }
                let new_len = holder_len.saturating_add(added_count);
                puts.push((table.holder_list_len_key(alkane), new_len.to_le_bytes().to_vec()));
            } else {
                let mut holder_keys_for_idx: Vec<Vec<u8>> = holder_amounts
                    .keys()
                    .map(|holder| match holder {
                        HolderId::Address(addr) => {
                            let mut out = Vec::with_capacity(1 + addr.len());
                            out.push(b'a');
                            out.extend_from_slice(addr.as_bytes());
                            out
                        }
                        HolderId::Alkane(id) => {
                            let mut out = Vec::with_capacity(13);
                            out.push(b'k');
                            out.extend_from_slice(&id.block.to_be_bytes());
                            out.extend_from_slice(&id.tx.to_be_bytes());
                            out
                        }
                    })
                    .collect();
                holder_keys_for_idx.sort();
                let new_len = holder_keys_for_idx.len() as u32;
                holders_full_rebuilds = holders_full_rebuilds.saturating_add(1);
                holders_full_rebuild_entries =
                    holders_full_rebuild_entries.saturating_add(new_len as usize);
                puts.push((table.holder_list_len_key(alkane), new_len.to_le_bytes().to_vec()));
                for (idx, holder_key_bytes) in holder_keys_for_idx.into_iter().enumerate() {
                    puts.push((table.holder_list_idx_key(alkane, idx as u32), holder_key_bytes));
                }
                if holder_len > new_len {
                    for idx in new_len..holder_len {
                        deletes.push(table.holder_list_idx_key(alkane, idx));
                    }
                }
            }
        }

        puts.push((holders_count_key, get_holders_count_encoded(new_count)?));
    }
    if debug {
        eprintln!(
            "[balances] writes F/holders: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
        eprintln!(
            "[balances] writes F/holders details: full_rebuilds={} rebuilt_entries={}",
            holders_full_rebuilds, holders_full_rebuild_entries
        );
    }
    section_start_puts = puts.len();
    section_start_deletes = deletes.len();

    // G) Transfer volume + total received + address activity rows.
    let mut transfer_new_addrs: HashMap<SchemaAlkaneId, HashSet<String>> = HashMap::new();
    for (alkane, per_addr) in transfer_volume_delta.iter() {
        for (addr, delta) in per_addr {
            let key = table.transfer_volume_entry_key(alkane, addr);
            let prev_raw = provider
                .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: key.clone() })?
                .value;
            let had_row = prev_raw.is_some();
            let prev =
                prev_raw.as_ref().and_then(|bytes| decode_u128_value(bytes).ok()).unwrap_or(0);
            puts.push((key, encode_u128_value(prev.saturating_add(*delta))?));
            if !had_row {
                transfer_new_addrs.entry(*alkane).or_default().insert(addr.clone());
            }
        }
    }
    for (alkane, new_addrs) in transfer_new_addrs {
        if new_addrs.is_empty() {
            continue;
        }
        let len = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.transfer_volume_list_len_key(&alkane),
            })?
            .value
            .and_then(|bytes| {
                if bytes.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&bytes);
                    Some(u32::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let appended_count = new_addrs.len() as u32;
        let mut appended: Vec<String> = new_addrs.into_iter().collect();
        appended.sort();
        let base = len;
        for (offset, addr) in appended.into_iter().enumerate() {
            puts.push((
                table.transfer_volume_list_idx_key(&alkane, base.saturating_add(offset as u32)),
                addr.into_bytes(),
            ));
        }
        let new_len = len.saturating_add(appended_count);
        puts.push((table.transfer_volume_list_len_key(&alkane), new_len.to_le_bytes().to_vec()));
    }

    let mut received_new_addrs: HashMap<SchemaAlkaneId, HashSet<String>> = HashMap::new();
    for (alkane, per_addr) in total_received_delta.iter() {
        for (addr, delta) in per_addr {
            let key = table.total_received_entry_key(alkane, addr);
            let prev_raw = provider
                .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: key.clone() })?
                .value;
            let had_row = prev_raw.is_some();
            let prev =
                prev_raw.as_ref().and_then(|bytes| decode_u128_value(bytes).ok()).unwrap_or(0);
            puts.push((key, encode_u128_value(prev.saturating_add(*delta))?));
            if !had_row {
                received_new_addrs.entry(*alkane).or_default().insert(addr.clone());
            }
        }
    }
    for (alkane, new_addrs) in received_new_addrs {
        if new_addrs.is_empty() {
            continue;
        }
        let len = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.total_received_list_len_key(&alkane),
            })?
            .value
            .and_then(|bytes| {
                if bytes.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&bytes);
                    Some(u32::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let appended_count = new_addrs.len() as u32;
        let mut appended: Vec<String> = new_addrs.into_iter().collect();
        appended.sort();
        let base = len;
        for (offset, addr) in appended.into_iter().enumerate() {
            puts.push((
                table.total_received_list_idx_key(&alkane, base.saturating_add(offset as u32)),
                addr.into_bytes(),
            ));
        }
        let new_len = len.saturating_add(appended_count);
        puts.push((table.total_received_list_len_key(&alkane), new_len.to_le_bytes().to_vec()));
    }

    if !address_activity_transfer_delta.is_empty() || !address_activity_received_delta.is_empty() {
        let mut activity_transfer_new: HashMap<String, HashSet<SchemaAlkaneId>> = HashMap::new();
        let mut activity_received_new: HashMap<String, HashSet<SchemaAlkaneId>> = HashMap::new();
        let mut addr_keys: HashSet<String> = HashSet::new();
        addr_keys.extend(address_activity_transfer_delta.keys().cloned());
        addr_keys.extend(address_activity_received_delta.keys().cloned());
        for addr in addr_keys {
            if let Some(per_alk) = address_activity_transfer_delta.get(&addr) {
                for (alk, delta) in per_alk {
                    let key = table.address_activity_transfer_key(&addr, alk);
                    let prev_raw = provider
                        .get_raw_value(GetRawValueParams {
                            blockhash: StateAt::Latest,
                            key: key.clone(),
                        })?
                        .value;
                    let had_row = prev_raw.is_some();
                    let prev = prev_raw
                        .as_ref()
                        .and_then(|bytes| decode_u128_value(bytes).ok())
                        .unwrap_or(0);
                    puts.push((key, encode_u128_value(prev.saturating_add(*delta))?));
                    if !had_row {
                        activity_transfer_new.entry(addr.clone()).or_default().insert(*alk);
                    }
                }
            }
            if let Some(per_alk) = address_activity_received_delta.get(&addr) {
                for (alk, delta) in per_alk {
                    let key = table.address_activity_total_received_key(&addr, alk);
                    let prev_raw = provider
                        .get_raw_value(GetRawValueParams {
                            blockhash: StateAt::Latest,
                            key: key.clone(),
                        })?
                        .value;
                    let had_row = prev_raw.is_some();
                    let prev = prev_raw
                        .as_ref()
                        .and_then(|bytes| decode_u128_value(bytes).ok())
                        .unwrap_or(0);
                    puts.push((key, encode_u128_value(prev.saturating_add(*delta))?));
                    if !had_row {
                        activity_received_new.entry(addr.clone()).or_default().insert(*alk);
                    }
                }
            }
        }
        for (addr, new_tokens) in activity_transfer_new {
            if new_tokens.is_empty() {
                continue;
            }
            let len = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.address_activity_transfer_list_len_key(&addr),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let appended_count = new_tokens.len() as u32;
            let mut appended: Vec<SchemaAlkaneId> = new_tokens.into_iter().collect();
            appended.sort();
            let base = len;
            for (offset, token) in appended.into_iter().enumerate() {
                let mut token_bytes = Vec::with_capacity(12);
                token_bytes.extend_from_slice(&token.block.to_be_bytes());
                token_bytes.extend_from_slice(&token.tx.to_be_bytes());
                puts.push((
                    table.address_activity_transfer_list_idx_key(
                        &addr,
                        base.saturating_add(offset as u32),
                    ),
                    token_bytes,
                ));
            }
            let new_len = len.saturating_add(appended_count);
            puts.push((
                table.address_activity_transfer_list_len_key(&addr),
                new_len.to_le_bytes().to_vec(),
            ));
        }
        for (addr, new_tokens) in activity_received_new {
            if new_tokens.is_empty() {
                continue;
            }
            let len = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.address_activity_total_received_list_len_key(&addr),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let appended_count = new_tokens.len() as u32;
            let mut appended: Vec<SchemaAlkaneId> = new_tokens.into_iter().collect();
            appended.sort();
            let base = len;
            for (offset, token) in appended.into_iter().enumerate() {
                let mut token_bytes = Vec::with_capacity(12);
                token_bytes.extend_from_slice(&token.block.to_be_bytes());
                token_bytes.extend_from_slice(&token.tx.to_be_bytes());
                puts.push((
                    table.address_activity_total_received_list_idx_key(
                        &addr,
                        base.saturating_add(offset as u32),
                    ),
                    token_bytes,
                ));
            }
            let new_len = len.saturating_add(appended_count);
            puts.push((
                table.address_activity_total_received_list_len_key(&addr),
                new_len.to_le_bytes().to_vec(),
            ));
        }
    }

    for (alkane, delta) in minted_delta_by_alk.iter() {
        if *delta == 0 {
            continue;
        }
        let latest_key = table.total_minted_latest_key(alkane);
        let prev_total = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: latest_key.clone(),
            })?
            .value
            .and_then(|v| decode_u128_value(&v).ok())
            .unwrap_or(0);
        let new_total = prev_total.saturating_add(*delta);
        let encoded = encode_u128_value(new_total)?;
        puts.push((table.total_minted_key(alkane, block.height), encoded.clone()));
        puts.push((latest_key, encoded));
    }
    if debug {
        eprintln!(
            "[balances] writes G/activity+minted: puts+{} deletes+{}",
            puts.len().saturating_sub(section_start_puts),
            deletes.len().saturating_sub(section_start_deletes)
        );
        eprintln!("[balances] writes TOTAL: puts={} deletes={}", puts.len(), deletes.len());
    }

    debug::log_elapsed(module, "build_writes", timer);
    let timer = debug::start_if(debug);
    let check_balances = strict_check_alkane_balances();
    let check_utxos = strict_check_utxos();
    if check_balances || check_utxos {
        let mut changed_pairs: Vec<(SchemaAlkaneId, SchemaAlkaneId)> = Vec::new();
        if check_balances {
            for (owner, per_token) in &alkane_balance_delta {
                for (token, delta) in per_token {
                    if delta.is_zero() {
                        continue;
                    }
                    changed_pairs.push((*owner, *token));
                }
            }
            changed_pairs.sort();
            changed_pairs.dedup();
        }
        let unspent_rows_count = if check_utxos {
            new_rows.iter().filter(|row| row.outpoint.tx_spent.is_none()).count()
        } else {
            0
        };
        let check_balances_now = check_balances && !changed_pairs.is_empty();
        let check_utxos_now = check_utxos && unspent_rows_count > 0;
        if !check_balances_now && !check_utxos_now {
            if debug {
                eprintln!(
                    "[balances][strict] skipped: changed_pairs={} unspent_rows={}",
                    changed_pairs.len(),
                    unspent_rows_count
                );
            }
        } else {
            let metashrew = get_metashrew();
            let height_u64 = block.height as u64;
            let metashrew_sdb = get_metashrew_sdb();
            metashrew_sdb
                .catch_up_now()
                .context("metashrew catch_up before strict checks")?;
            let sdb = metashrew_sdb.as_ref();

            let mut balance_mismatches: Vec<(SchemaAlkaneId, SchemaAlkaneId, u128, u128)> =
                Vec::new();
            if check_balances_now {
                let balances_from_rows = |owner: &SchemaAlkaneId| -> HashMap<SchemaAlkaneId, u128> {
                    let entries = alkane_balances_rows.get(owner).unwrap_or_else(|| {
                        panic!(
                            "[balances][strict] missing prewrite balances (owner={}:{})",
                            owner.block, owner.tx
                        )
                    });
                    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
                    for entry in entries {
                        if entry.amount == 0 {
                            continue;
                        }
                        *agg.entry(entry.alkane).or_default() = agg
                            .get(&entry.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(entry.amount);
                    }
                    if let Some(self_balance) = lookup_self_balance(owner) {
                        if self_balance == 0 {
                            agg.remove(owner);
                        } else {
                            agg.insert(*owner, self_balance);
                        }
                    }
                    agg
                };

                let mut local_cache: HashMap<SchemaAlkaneId, HashMap<SchemaAlkaneId, u128>> =
                    HashMap::new();

                for (owner, token) in changed_pairs {
                    if !local_cache.contains_key(&owner) {
                        let balances = balances_from_rows(&owner);
                        local_cache.insert(owner, balances);
                    }
                    let local_balance =
                        local_cache.get(&owner).and_then(|m| m.get(&token).copied()).unwrap_or(0);

                    let metashrew_balance = match metashrew.get_reserves_for_alkane_with_db(
                        sdb,
                        &owner,
                        &token,
                        Some(height_u64),
                    ) {
                        Ok(Some(bal)) => bal,
                        Ok(None) => 0,
                        Err(e) => {
                            panic!(
                                "[balances][strict] metashrew lookup failed (owner={}:{}, token={}:{}, height={}): {e:?}",
                                owner.block, owner.tx, token.block, token.tx, height_u64
                            );
                        }
                    };

                    if local_balance != metashrew_balance {
                        balance_mismatches.push((owner, token, local_balance, metashrew_balance));
                    }
                }
            }

            struct UtxoMismatch {
                outpoint: EspoOutpoint,
                addr: String,
                local: BTreeMap<SchemaAlkaneId, u128>,
                metashrew: BTreeMap<SchemaAlkaneId, u128>,
            }
            let mut utxo_mismatches: Vec<UtxoMismatch> = Vec::new();
            if check_utxos_now {
                let to_balance_map = |entries: &[BalanceEntry]| -> BTreeMap<SchemaAlkaneId, u128> {
                    let mut out = BTreeMap::new();
                    for entry in entries {
                        if entry.amount == 0 {
                            continue;
                        }
                        *out.entry(entry.alkane).or_default() = out
                            .get(&entry.alkane)
                            .copied()
                            .unwrap_or(0u128)
                            .saturating_add(entry.amount);
                    }
                    out
                };
                let parse_txid = |txid_bytes: &[u8]| -> Result<Txid> {
                    if txid_bytes.len() != 32 {
                        return Err(anyhow!("invalid txid length {}", txid_bytes.len()));
                    }
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(txid_bytes);
                    Ok(Txid::from_byte_array(arr))
                };

                for row in &new_rows {
                    if row.outpoint.tx_spent.is_some() {
                        continue;
                    }
                    let txid = parse_txid(&row.outpoint.txid).unwrap_or_else(|e| {
                        panic!(
                            "[balances][strict] invalid outpoint txid bytes ({}:{}): {e}",
                            row.outpoint.as_outpoint_string(),
                            row.outpoint.vout
                        )
                    });
                    let local_entries = decode_balances_vec(&row.enc_balances).unwrap_or_default();
                    let local_map = to_balance_map(&local_entries);

                    let meta_entries = metashrew
                        .get_outpoint_alkane_balances_with_db(sdb, &txid, row.outpoint.vout)
                        .unwrap_or_else(|e| {
                            panic!(
                            "[balances][strict] metashrew outpoint lookup failed ({}:{}): {e:?}",
                            row.outpoint.as_outpoint_string(),
                            row.outpoint.vout
                        )
                        });
                    let mut meta_map = BTreeMap::new();
                    for (id, amount) in meta_entries {
                        if amount == 0 {
                            continue;
                        }
                        let schema = schema_id_from_parts(id.block, id.tx).unwrap_or_else(|e| {
                            panic!(
                                "[balances][strict] invalid metashrew alkane id ({}:{}): {e:?}",
                                id.block, id.tx
                            )
                        });
                        *meta_map.entry(schema).or_default() =
                            meta_map.get(&schema).copied().unwrap_or(0u128).saturating_add(amount);
                    }

                    if local_map != meta_map {
                        utxo_mismatches.push(UtxoMismatch {
                            outpoint: row.outpoint.clone(),
                            addr: row.addr.clone(),
                            local: local_map,
                            metashrew: meta_map,
                        });
                    }
                }
            }

            if !balance_mismatches.is_empty() || !utxo_mismatches.is_empty() {
                if check_balances_now {
                    let mut height_history_cache: HashMap<
                        (SchemaAlkaneId, SchemaAlkaneId),
                        Vec<(u32, u128)>,
                    > = HashMap::new();

                    let mut find_mismatch_origin =
                        |owner: &SchemaAlkaneId,
                         token: &SchemaAlkaneId,
                         current_balance: u128|
                         -> Option<(u32, u128, u128, bool)> {
                            let history = if let Some(cached) =
                                height_history_cache.get(&(*owner, *token))
                            {
                                cached.clone()
                            } else {
                                let hlen = match provider.get_raw_value(GetRawValueParams {
                                    blockhash: StateAt::Latest,
                                    key: table.alkane_balance_by_height_list_len_key(owner, token),
                                }) {
                                    Ok(v) => v
                                        .value
                                        .and_then(|bytes| {
                                            if bytes.len() == 4 {
                                                let mut arr = [0u8; 4];
                                                arr.copy_from_slice(&bytes);
                                                Some(u32::from_le_bytes(arr))
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or(0),
                                    Err(_) => 0,
                                };
                                if hlen == 0 {
                                    return None;
                                }
                                let mut hidx_keys = Vec::with_capacity(hlen as usize);
                                for idx in 0..hlen {
                                    hidx_keys.push(
                                        table.alkane_balance_by_height_list_idx_key(
                                            owner, token, idx,
                                        ),
                                    );
                                }
                                let hidx_vals =
                                    match provider.get_multi_values(GetMultiValuesParams {
                                        blockhash: StateAt::Latest,
                                        keys: hidx_keys,
                                    }) {
                                        Ok(v) => v.values,
                                        Err(_) => Vec::new(),
                                    };
                                if hidx_vals.is_empty() {
                                    return None;
                                }
                                let mut heights: Vec<u32> = Vec::new();
                                for hraw in hidx_vals.into_iter().flatten() {
                                    if hraw.len() != 4 {
                                        continue;
                                    }
                                    heights.push(u32::from_be_bytes([
                                        hraw[0], hraw[1], hraw[2], hraw[3],
                                    ]));
                                }
                                if heights.is_empty() {
                                    return None;
                                }
                                heights.sort_unstable();
                                heights.dedup();
                                let value_keys: Vec<Vec<u8>> = heights
                                    .iter()
                                    .map(|h| table.alkane_balance_by_height_key(owner, token, *h))
                                    .collect();
                                let value_rows =
                                    match provider.get_multi_values(GetMultiValuesParams {
                                        blockhash: StateAt::Latest,
                                        keys: value_keys,
                                    }) {
                                        Ok(v) => v.values,
                                        Err(_) => Vec::new(),
                                    };
                                let mut entries_by_height: Vec<(u32, u128)> = Vec::new();
                                for (height, value) in
                                    heights.iter().copied().zip(value_rows.into_iter())
                                {
                                    let Some(bytes) = value else {
                                        continue;
                                    };
                                    if let Ok(amount) = decode_u128_value(&bytes) {
                                        entries_by_height.push((height, amount));
                                    }
                                }
                                if entries_by_height.is_empty() {
                                    return None;
                                }
                                entries_by_height.sort_by_key(|(h, _)| *h);
                                entries_by_height.dedup_by_key(|(h, _)| *h);
                                height_history_cache
                                    .insert((*owner, *token), entries_by_height.clone());
                                entries_by_height
                            };

                            if history.is_empty() {
                                return None;
                            }
                            let mut snapshots = history;
                            let current_height = block.height;
                            snapshots.retain(|(h, _)| *h <= current_height);
                            if snapshots.is_empty() {
                                return None;
                            }

                            if let Some(last) = snapshots.last_mut() {
                                if last.0 == current_height {
                                    last.1 = current_balance;
                                } else if last.0 < current_height {
                                    snapshots.push((current_height, current_balance));
                                }
                            } else {
                                return None;
                            }

                            #[derive(Clone, Copy)]
                            struct Segment {
                                start: u32,
                                end: u32,
                                balance: u128,
                            }

                            let mut segments: Vec<Segment> = Vec::with_capacity(snapshots.len());
                            for idx in 0..snapshots.len() {
                                let (start, balance) = snapshots[idx];
                                let end = if idx + 1 < snapshots.len() {
                                    let next_start = snapshots[idx + 1].0;
                                    if next_start == 0 { 0 } else { next_start.saturating_sub(1) }
                                } else {
                                    current_height
                                };
                                if end < start {
                                    continue;
                                }
                                segments.push(Segment { start, end, balance });
                            }

                            if segments.is_empty() {
                                return None;
                            }

                            let mut meta_cache: HashMap<u32, u128> = HashMap::new();
                            let mut metashrew_at = |height: u32| -> u128 {
                                if let Some(val) = meta_cache.get(&height).copied() {
                                    return val;
                                }
                                let height_u64 = height as u64;
                                let value = match metashrew.get_reserves_for_alkane_with_db(
                                    sdb,
                                    owner,
                                    token,
                                    Some(height_u64),
                                ) {
                                    Ok(Some(bal)) => bal,
                                    Ok(None) => 0,
                                    Err(e) => {
                                        panic!(
                                            "[balances][strict] metashrew lookup failed (owner={}:{}, token={}:{}, height={}): {e:?}",
                                            owner.block,
                                            owner.tx,
                                            token.block,
                                            token.tx,
                                            height_u64
                                        );
                                    }
                                };
                                meta_cache.insert(height, value);
                                value
                            };

                            for idx in (0..segments.len()).rev() {
                                let seg = segments[idx];
                                let meta_start = metashrew_at(seg.start);
                                if meta_start == seg.balance {
                                    let mut lo = seg.start;
                                    let mut hi = seg.end;
                                    while lo < hi {
                                        let mid = lo + (hi - lo) / 2;
                                        let meta_mid = metashrew_at(mid);
                                        if meta_mid == seg.balance {
                                            lo = mid + 1;
                                        } else {
                                            hi = mid;
                                        }
                                    }
                                    let meta_at = metashrew_at(lo);
                                    return Some((lo, seg.balance, meta_at, true));
                                }

                                if idx == 0 || seg.start == 0 {
                                    return Some((seg.start, seg.balance, meta_start, false));
                                }

                                let prev_end = seg.start - 1;
                                let prev_balance = segments[idx - 1].balance;
                                let meta_prev_end = metashrew_at(prev_end);
                                if meta_prev_end == prev_balance {
                                    return Some((seg.start, seg.balance, meta_start, true));
                                }
                            }

                            None
                        };

                    for (owner, token, local_balance, metashrew_balance) in &balance_mismatches {
                        eprintln!(
                            "[balances][strict] mismatch height={} owner={}:{} token={}:{} local={} metashrew={}",
                            height_u64,
                            owner.block,
                            owner.tx,
                            token.block,
                            token.tx,
                            local_balance,
                            metashrew_balance
                        );

                        let mut txids: Vec<String> = alkane_balance_tx_entries_by_token
                            .get(&(*owner, *token))
                            .map(|entries| {
                                entries
                                    .iter()
                                    .map(|entry| Txid::from_byte_array(entry.txid).to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        txids.sort();
                        txids.dedup();

                        if txids.is_empty() {
                            eprintln!(
                                "[balances][strict] balance-change txids: none (owner={}:{}, token={}:{})",
                                owner.block, owner.tx, token.block, token.tx
                            );
                        } else {
                            eprintln!(
                                "[balances][strict] balance-change txids: {}",
                                txids.join(",")
                            );
                        }

                        if let Some((first_height, local_at, meta_at, exact)) =
                            find_mismatch_origin(owner, token, *local_balance)
                        {
                            if exact {
                                eprintln!(
                                    "[balances][strict] mismatch origin height={} owner={}:{} token={}:{} local={} metashrew={}",
                                    first_height,
                                    owner.block,
                                    owner.tx,
                                    token.block,
                                    token.tx,
                                    local_at,
                                    meta_at
                                );
                            } else {
                                eprintln!(
                                    "[balances][strict] mismatch origin at or before height={} owner={}:{} token={}:{} local={} metashrew={}",
                                    first_height,
                                    owner.block,
                                    owner.tx,
                                    token.block,
                                    token.tx,
                                    local_at,
                                    meta_at
                                );
                            }
                        }
                    }
                }

                if check_utxos_now {
                    let fmt_sheet = |sheet: &BTreeMap<SchemaAlkaneId, u128>| -> String {
                        if sheet.is_empty() {
                            return "empty".to_string();
                        }
                        sheet
                            .iter()
                            .map(|(id, amt)| format!("{}:{}={}", id.block, id.tx, amt))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    for mismatch in &utxo_mismatches {
                        eprintln!(
                            "[balances][strict] utxo mismatch outpoint={} addr={} local=[{}] metashrew=[{}]",
                            mismatch.outpoint.as_outpoint_string(),
                            mismatch.addr,
                            fmt_sheet(&mismatch.local),
                            fmt_sheet(&mismatch.metashrew)
                        );
                    }
                }

                panic!(
                    "[balances][strict] metashrew mismatch at height {} (alkanes={} utxos={})",
                    height_u64,
                    balance_mismatches.len(),
                    utxo_mismatches.len()
                );
            }
        }
    }
    debug::log_elapsed(module, "strict_mode_checks", timer);

    let timer = debug::start_if(debug);
    if debug {
        let put_payload_bytes: usize =
            puts.iter().map(|(k, v)| k.len().saturating_add(v.len())).sum();
        let blob_put_payload_bytes: usize =
            blob_puts.iter().map(|(k, v)| k.len().saturating_add(v.len())).sum();
        let delete_key_bytes: usize = deletes.iter().map(|k| k.len()).sum();
        eprintln!(
            "[balances] write_batch prepare: puts={} blob_puts={} deletes={} put_payload_bytes={} blob_put_payload_bytes={} delete_key_bytes={}",
            puts.len(),
            blob_puts.len(),
            deletes.len(),
            put_payload_bytes,
            blob_put_payload_bytes,
            delete_key_bytes
        );

        let outpoint_pos_prefix = table.outpoint_pos_point_family_prefix();
        let outpoint_sid_prefix = table.outpoint_spent_by_id_point_family_prefix();
        let tx_pos_prefix = table.tx_packed_outflow_pos_point_family_prefix();
        let empty_deletes: &[Vec<u8>] = &[];

        {
            let prefixes = [outpoint_pos_prefix.as_slice(), outpoint_sid_prefix.as_slice()];
            let prof = profile_family_writes(&blob_puts, empty_deletes, &prefixes);
            eprintln!(
                "[balances][non_cow_profile] block={} family=essentials.outpoint.v2.other raw_put_rows={} raw_put_bytes={} dedup_put_rows={} dedup_put_bytes={} raw_delete_rows={} raw_delete_key_bytes={} dedup_delete_rows={} dedup_delete_key_bytes={}",
                block.height,
                prof.raw_put_rows,
                prof.raw_put_key_bytes.saturating_add(prof.raw_put_value_bytes),
                prof.dedup_put_rows,
                prof.dedup_put_key_bytes.saturating_add(prof.dedup_put_value_bytes),
                prof.raw_delete_rows,
                prof.raw_delete_key_bytes,
                prof.dedup_delete_rows,
                prof.dedup_delete_key_bytes
            );
        }

        {
            let prof =
                profile_family_writes(&blob_puts, empty_deletes, &[outpoint_pos_prefix.as_slice()]);
            eprintln!(
                "[balances][non_cow_profile] block={} family=essentials.outpoint.v2.p raw_put_rows={} raw_put_bytes={} dedup_put_rows={} dedup_put_bytes={} raw_delete_rows={} raw_delete_key_bytes={} dedup_delete_rows={} dedup_delete_key_bytes={}",
                block.height,
                prof.raw_put_rows,
                prof.raw_put_key_bytes.saturating_add(prof.raw_put_value_bytes),
                prof.dedup_put_rows,
                prof.dedup_put_key_bytes.saturating_add(prof.dedup_put_value_bytes),
                prof.raw_delete_rows,
                prof.raw_delete_key_bytes,
                prof.dedup_delete_rows,
                prof.dedup_delete_key_bytes
            );
        }

        {
            let prof =
                profile_family_writes(&blob_puts, empty_deletes, &[outpoint_sid_prefix.as_slice()]);
            eprintln!(
                "[balances][non_cow_profile] block={} family=essentials.outpoint.v2.sid raw_put_rows={} raw_put_bytes={} dedup_put_rows={} dedup_put_bytes={} raw_delete_rows={} raw_delete_key_bytes={} dedup_delete_rows={} dedup_delete_key_bytes={}",
                block.height,
                prof.raw_put_rows,
                prof.raw_put_key_bytes.saturating_add(prof.raw_put_value_bytes),
                prof.dedup_put_rows,
                prof.dedup_put_key_bytes.saturating_add(prof.dedup_put_value_bytes),
                prof.raw_delete_rows,
                prof.raw_delete_key_bytes,
                prof.dedup_delete_rows,
                prof.dedup_delete_key_bytes
            );
        }

        {
            let prof =
                profile_family_writes(&blob_puts, empty_deletes, &[tx_pos_prefix.as_slice()]);
            eprintln!(
                "[balances][non_cow_profile] block={} family=essentials.tx.packed_outflow_pos raw_put_rows={} raw_put_bytes={} dedup_put_rows={} dedup_put_bytes={} raw_delete_rows={} raw_delete_key_bytes={} dedup_delete_rows={} dedup_delete_key_bytes={}",
                block.height,
                prof.raw_put_rows,
                prof.raw_put_key_bytes.saturating_add(prof.raw_put_value_bytes),
                prof.dedup_put_rows,
                prof.dedup_put_key_bytes.saturating_add(prof.dedup_put_value_bytes),
                prof.raw_delete_rows,
                prof.raw_delete_key_bytes,
                prof.dedup_delete_rows,
                prof.dedup_delete_key_bytes
            );
        }
    }
    blob_puts.push((outpoint_counter_key.clone(), encode_pointer_idx_u64(next_outpoint_id)));
    blob_puts.push((
        outpoint_idx_chunk_counter_key.clone(),
        encode_pointer_idx_u64(next_outpoint_idx_chunk_id),
    ));
    blob_puts.push((tx_pointer_counter_key.clone(), encode_pointer_idx_u64(next_tx_pointer_id)));
    blob_puts
        .push((by_token_chunk_counter_key.clone(), encode_pointer_idx_u64(next_by_token_chunk_id)));
    blob_puts.push((
        alkane_block_chunk_counter_key.clone(),
        encode_pointer_idx_u64(next_alkane_block_chunk_id),
    ));
    blob_puts.push((
        alkane_addr_chunk_counter_key.clone(),
        encode_pointer_idx_u64(next_alkane_addr_chunk_id),
    ));
    provider.blob_mdb().bulk_write(|wb: &mut MdbBatch<'_>| {
        for (key, value) in &blob_puts {
            wb.put(key, value);
        }
    })?;
    provider.set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes })?;
    debug::log_elapsed(module, "write_batch", timer);

    let search_index_timer = debug::start_if(debug);
    if search_index_enabled && (!ammdata_puts.is_empty() || !ammdata_deletes.is_empty()) {
        let mdb = ammdata_mdb();
        let res = mdb.bulk_write(|wb| {
            for key in &ammdata_deletes {
                wb.delete(key);
            }
            for (key, value) in &ammdata_puts {
                wb.put(key, value);
            }
        });
        if let Err(e) = res {
            eprintln!(
                "[balances] ammdata search index write failed at height {}: {e}",
                block.height
            );
        }
    }
    debug::log_elapsed(module, "write_ammdata_search_index", search_index_timer);

    let minus_total: u128 = stat_minus_by_alk.values().copied().sum();
    let plus_total: u128 = stat_plus_by_alk.values().copied().sum();

    eprintln!(
        "[balances] block #{}, txs={}, outpoints_written={}, outpoints_marked_spent={}, alkanes_added={}, alkanes_removed={}, unique_add={}, unique_remove={}",
        block.height,
        block.transactions.len(),
        stat_outpoints_written,
        stat_outpoints_marked_spent,
        plus_total,
        minus_total,
        stat_plus_by_alk.len(),
        stat_minus_by_alk.len()
    );
    eprintln!("[balances] <<< end   block #{}", block.height);

    Ok(())
}

fn lookup_self_balance(alk: &SchemaAlkaneId) -> Option<u128> {
    match get_metashrew().get_reserves_for_alkane(alk, alk, None) {
        Ok(val) => val,
        Err(e) => {
            eprintln!(
                "[balances] WARN: self-balance lookup failed for {}:{} ({e:?})",
                alk.block, alk.tx
            );
            None
        }
    }
}

pub fn get_balance_for_address(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    address: &str,
) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let table = provider.table();
    let len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.address_balance_list_len_key(address),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if len == 0 {
        return Ok(HashMap::new());
    }

    let mut idx_keys = Vec::with_capacity(len as usize);
    for idx in 0..len {
        idx_keys.push(table.address_balance_list_idx_key(address, idx));
    }
    let idx_vals = provider
        .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
        .values;
    let mut tokens = Vec::new();
    let mut bal_keys = Vec::new();
    for idx_val in idx_vals {
        let Some(raw) = idx_val else { continue };
        if raw.len() != 12 {
            continue;
        }
        let token = SchemaAlkaneId {
            block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
            tx: u64::from_be_bytes([
                raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
            ]),
        };
        bal_keys.push(table.address_balance_key(address, &token));
        tokens.push(token);
    }
    let vals = provider
        .get_multi_values(GetMultiValuesParams { blockhash, keys: bal_keys })?
        .values;

    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    for (token, v) in tokens.into_iter().zip(vals.into_iter()) {
        let Some(bytes) = v else { continue };
        let Ok(amount) = decode_u128_value(&bytes) else {
            continue;
        };
        if amount == 0 {
            continue;
        }
        agg.insert(token, amount);
    }
    Ok(agg)
}

pub fn get_alkane_balances(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    owner: &SchemaAlkaneId,
) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let table = provider.table();
    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    let len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.alkane_balance_list_len_key(owner),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys.push(table.alkane_balance_list_idx_key(owner, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut tokens = Vec::new();
        let mut bal_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            if raw.len() != 12 {
                continue;
            }
            let token = SchemaAlkaneId {
                block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                tx: u64::from_be_bytes([
                    raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                ]),
            };
            bal_keys.push(table.alkane_balance_key(owner, &token));
            tokens.push(token);
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: bal_keys })?
            .values;
        for (token, value) in tokens.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount == 0 {
                continue;
            }
            agg.insert(token, amount);
        }
    }

    /*
     * Keep metashrew self-balance override behavior for parity with existing API semantics.
     */
    if let Some(self_balance) = lookup_self_balance(owner) {
        if self_balance == 0 {
            agg.remove(owner);
        } else {
            agg.insert(*owner, self_balance);
        }
    }

    Ok(agg)
}

pub fn get_alkane_balances_at_or_before(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    owner: &SchemaAlkaneId,
    height: u32,
) -> Result<(HashMap<SchemaAlkaneId, u128>, Option<u32>)> {
    let table = provider.table();
    let mut agg = HashMap::new();
    let mut resolved_height: Option<u32> = None;
    let token_len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.alkane_balance_list_len_key(owner),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if token_len > 0 {
        let mut token_idx_keys = Vec::with_capacity(token_len as usize);
        for idx in 0..token_len {
            token_idx_keys.push(table.alkane_balance_list_idx_key(owner, idx));
        }
        let token_idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: token_idx_keys })?
            .values;
        let mut tokens = Vec::new();
        for token_raw in token_idx_vals.into_iter().flatten() {
            if token_raw.len() != 12 {
                continue;
            }
            tokens.push(SchemaAlkaneId {
                block: u32::from_be_bytes([token_raw[0], token_raw[1], token_raw[2], token_raw[3]]),
                tx: u64::from_be_bytes([
                    token_raw[4],
                    token_raw[5],
                    token_raw[6],
                    token_raw[7],
                    token_raw[8],
                    token_raw[9],
                    token_raw[10],
                    token_raw[11],
                ]),
            });
        }

        for token in tokens {
            let hlen = provider
                .get_raw_value(GetRawValueParams {
                    blockhash,
                    key: table.alkane_balance_by_height_list_len_key(owner, &token),
                })?
                .value
                .and_then(|bytes| {
                    if bytes.len() == 4 {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes);
                        Some(u32::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if hlen == 0 {
                continue;
            }

            let mut hidx_keys = Vec::with_capacity(hlen as usize);
            for idx in 0..hlen {
                hidx_keys.push(table.alkane_balance_by_height_list_idx_key(owner, &token, idx));
            }
            let hidx_vals = provider
                .get_multi_values(GetMultiValuesParams { blockhash, keys: hidx_keys })?
                .values;
            let mut best_height: Option<u32> = None;
            for hraw in hidx_vals.into_iter().flatten() {
                if hraw.len() != 4 {
                    continue;
                }
                let h = u32::from_be_bytes([hraw[0], hraw[1], hraw[2], hraw[3]]);
                if h <= height {
                    best_height = Some(best_height.map(|cur| cur.max(h)).unwrap_or(h));
                }
            }

            let Some(found_height) = best_height else {
                continue;
            };
            let amount = provider
                .get_raw_value(GetRawValueParams {
                    blockhash,
                    key: table.alkane_balance_by_height_key(owner, &token, found_height),
                })?
                .value
                .and_then(|bytes| decode_u128_value(&bytes).ok())
                .unwrap_or(0);
            resolved_height =
                Some(resolved_height.map(|cur| cur.max(found_height)).unwrap_or(found_height));
            if amount > 0 {
                agg.insert(token, amount);
            }
        }
    }

    Ok((agg, resolved_height))
}

#[derive(Default, Clone, Debug)]
pub struct OutpointLookup {
    pub balances: Vec<BalanceEntry>,
    pub spent_by: Option<Txid>,
    pub address: Option<String>,
    pub spk: Option<ScriptBuf>,
}

fn resolve_outpoint_spent_by_v2(
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
    blockhash: StateAt,
) -> Result<Option<Txid>> {
    let outpoint_id = resolve_outpoint_id_v2(provider, blockhash, txid.as_byte_array(), vout)?;
    if let Some(outpoint_id) = outpoint_id {
        if let Some(raw_txid) = resolve_outpoint_spent_by_id_v2(provider, blockhash, outpoint_id)? {
            if let Ok(txid) = Txid::from_slice(&raw_txid) {
                return Ok(Some(txid));
            }
        }
    }
    Ok(None)
}

fn load_outpoint_row_v2(
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
    blockhash: StateAt,
) -> Result<Option<crate::modules::essentials::storage::OutpointPointerBlobV3>> {
    let table = provider.table();
    if let Some(outpoint_id) =
        resolve_outpoint_id_v2(provider, blockhash, txid.as_byte_array(), vout)?
    {
        let row_key = table.outpoint_pointer_blob_key(outpoint_id);
        if let Some(row_raw) = provider
            .get_blob_raw_value(GetRawValueParams { blockhash, key: row_key })?
            .value
        {
            if let Ok(row) = decode_outpoint_pointer_blob_v3(&row_raw) {
                return Ok(Some(row));
            }
        }
    }
    Ok(None)
}

pub fn get_outpoint_balances(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<Vec<BalanceEntry>> {
    Ok(load_outpoint_row_v2(provider, txid, vout, blockhash)?
        .map(|row| row.balances)
        .unwrap_or_default())
}

pub fn get_outpoint_address(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<Option<String>> {
    Ok(load_outpoint_row_v2(provider, txid, vout, blockhash)?
        .map(|row| row.address)
        .filter(|addr| !addr.is_empty()))
}

pub fn get_outpoint_balances_with_spent(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<OutpointLookup> {
    let spent_by = resolve_outpoint_spent_by_v2(provider, txid, vout, blockhash)?;
    let row = load_outpoint_row_v2(provider, txid, vout, blockhash)?;
    let balances = row.as_ref().map(|r| r.balances.clone()).unwrap_or_default();
    let address = row.as_ref().map(|r| r.address.clone()).filter(|s| !s.is_empty());
    let spk = row
        .as_ref()
        .and_then(|r| if r.spk.is_empty() { None } else { Some(ScriptBuf::from(r.spk.clone())) });
    Ok(OutpointLookup { balances, spent_by, address, spk })
}

pub fn get_outpoint_rows_batch(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    outpoints: &[(Txid, u32)],
) -> Result<HashMap<(Txid, u32), OutpointLookup>> {
    let table = provider.table();
    let ids = resolve_outpoint_ids_batch_v2(provider, blockhash, outpoints)?;
    let mut unique_ids: Vec<u64> = Vec::new();
    let mut seen_ids: HashSet<u64> = HashSet::new();
    for id in ids.iter().flatten() {
        if seen_ids.insert(*id) {
            unique_ids.push(*id);
        }
    }
    let mut row_by_id: HashMap<u64, crate::modules::essentials::storage::OutpointPointerBlobV3> =
        HashMap::new();
    if !unique_ids.is_empty() {
        let row_keys: Vec<Vec<u8>> =
            unique_ids.iter().map(|id| table.outpoint_pointer_blob_key(*id)).collect();
        let row_vals = provider
            .get_blob_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: row_keys,
            })?
            .values;
        for (id, row_raw) in unique_ids.iter().copied().zip(row_vals.into_iter()) {
            let Some(row_raw) = row_raw else { continue };
            let Ok(row) = decode_outpoint_pointer_blob_v3(&row_raw) else {
                continue;
            };
            row_by_id.insert(id, row);
        }
    }

    let mut out: HashMap<(Txid, u32), OutpointLookup> = HashMap::new();
    for ((txid, vout), id) in outpoints.iter().zip(ids.into_iter()) {
        let row = id.and_then(|rid| row_by_id.get(&rid));
        let balances = row.map(|r| r.balances.clone()).unwrap_or_default();
        let address = row.map(|r| r.address.clone()).filter(|s| !s.is_empty());
        let spk = row.and_then(|r| {
            if r.spk.is_empty() { None } else { Some(ScriptBuf::from(r.spk.clone())) }
        });
        out.insert((*txid, *vout), OutpointLookup { balances, spent_by: None, address, spk });
    }
    Ok(out)
}

pub fn get_outpoint_balances_with_spent_batch(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    outpoints: &[(Txid, u32)],
) -> Result<HashMap<(Txid, u32), OutpointLookup>> {
    let table = provider.table();
    let ids = resolve_outpoint_ids_batch_v2(provider, blockhash, outpoints)?;
    let mut unique_ids: Vec<u64> = Vec::new();
    let mut seen_ids: HashSet<u64> = HashSet::new();
    for id in ids.iter().flatten() {
        if seen_ids.insert(*id) {
            unique_ids.push(*id);
        }
    }
    let mut row_by_id: HashMap<u64, crate::modules::essentials::storage::OutpointPointerBlobV3> =
        HashMap::new();
    if !unique_ids.is_empty() {
        let row_keys: Vec<Vec<u8>> =
            unique_ids.iter().map(|id| table.outpoint_pointer_blob_key(*id)).collect();
        let row_vals = provider
            .get_blob_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: row_keys,
            })?
            .values;
        for (id, row_raw) in unique_ids.iter().copied().zip(row_vals.into_iter()) {
            let Some(row_raw) = row_raw else { continue };
            let Ok(row) = decode_outpoint_pointer_blob_v3(&row_raw) else {
                continue;
            };
            row_by_id.insert(id, row);
        }
    }
    let mut spent_by_id: HashMap<u64, Option<Txid>> = HashMap::new();
    if !unique_ids.is_empty() {
        let spent_vals =
            resolve_outpoint_spent_by_ids_batch_v2(provider, blockhash, unique_ids.as_slice())?;
        for (id, spent_raw) in unique_ids.iter().copied().zip(spent_vals.into_iter()) {
            let spent = spent_raw.and_then(|arr| Txid::from_slice(&arr).ok());
            spent_by_id.insert(id, spent);
        }
    }

    let mut out: HashMap<(Txid, u32), OutpointLookup> = HashMap::new();
    for ((txid, vout), id) in outpoints.iter().zip(ids.into_iter()) {
        let spent_by = id.and_then(|rid| spent_by_id.get(&rid).cloned().flatten());
        let row = id.and_then(|rid| row_by_id.get(&rid));
        let balances = row.map(|r| r.balances.clone()).unwrap_or_default();
        let address = row.map(|r| r.address.clone()).filter(|s| !s.is_empty());
        let spk = row.and_then(|r| {
            if r.spk.is_empty() { None } else { Some(ScriptBuf::from(r.spk.clone())) }
        });
        out.insert((*txid, *vout), OutpointLookup { balances, spent_by, address, spk });
    }
    Ok(out)
}

pub fn get_holders_for_alkane(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize /*total*/, u128 /*supply*/, Vec<HolderEntry>)> {
    let table = provider.table();
    let len = provider
        .get_raw_value(GetRawValueParams { blockhash, key: table.holder_list_len_key(&alk) })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);

    let mut all: Vec<HolderEntry> = Vec::new();
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys.push(table.holder_list_idx_key(&alk, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut holders = Vec::new();
        let mut holder_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            let holder = if raw.is_empty() {
                continue;
            } else if raw[0] == b'a' {
                let Ok(addr) = std::str::from_utf8(&raw[1..]).map(|s| s.to_string()) else {
                    continue;
                };
                HolderId::Address(addr)
            } else if raw[0] == b'k' && raw.len() == 13 {
                HolderId::Alkane(SchemaAlkaneId {
                    block: u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]),
                    tx: u64::from_be_bytes([
                        raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12],
                    ]),
                })
            } else {
                continue;
            };
            holder_keys.push(table.holder_key(&alk, &holder));
            holders.push(holder);
        }

        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: holder_keys })?
            .values;
        for (holder, value) in holders.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount == 0 {
                continue;
            }
            all.push(HolderEntry { holder, amount });
        }
    }
    if let Some(self_balance) = lookup_self_balance(&alk) {
        if self_balance > 0 {
            if let Some(existing) = all.iter_mut().find(|h| h.holder == HolderId::Alkane(alk)) {
                existing.amount = self_balance;
            } else {
                all.push(HolderEntry { holder: HolderId::Alkane(alk), amount: self_balance });
            }
        } else {
            all.retain(|h| h.holder != HolderId::Alkane(alk));
        }
    }

    all.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => holder_order_key(&a.holder).cmp(&holder_order_key(&b.holder)),
        o => o,
    });
    let total = all.len();
    let supply: u128 = all.iter().map(|h| h.amount).sum();
    let p = page.max(1);
    let l = limit.max(1);
    let off = l.saturating_mul(p - 1);
    let end = (off + l).min(total);
    let slice = if off >= total { vec![] } else { all[off..end].to_vec() };
    Ok((total, supply, slice))
}

pub fn get_transfer_volume_for_alkane(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let table = provider.table();
    let len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.transfer_volume_list_len_key(&alk),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut all = Vec::new();
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys.push(table.transfer_volume_list_idx_key(&alk, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut addrs = Vec::new();
        let mut value_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            let Ok(addr) = std::str::from_utf8(&raw).map(|s| s.to_string()) else {
                continue;
            };
            value_keys.push(table.transfer_volume_entry_key(&alk, &addr));
            addrs.push(addr);
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: value_keys })?
            .values;
        for (address, value) in addrs.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount == 0 {
                continue;
            }
            all.push(AddressAmountEntry { address, amount });
        }
    }
    sort_address_amount_entries(&mut all);
    let total = all.len();
    let p = page.max(1);
    let l = limit.max(1);
    let off = l.saturating_mul(p - 1);
    let end = (off + l).min(total);
    let slice = if off >= total { vec![] } else { all[off..end].to_vec() };
    Ok((total, slice))
}

pub fn get_total_received_for_alkane(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let table = provider.table();
    let len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.total_received_list_len_key(&alk),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut all = Vec::new();
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys.push(table.total_received_list_idx_key(&alk, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut addrs = Vec::new();
        let mut value_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            let Ok(addr) = std::str::from_utf8(&raw).map(|s| s.to_string()) else {
                continue;
            };
            value_keys.push(table.total_received_entry_key(&alk, &addr));
            addrs.push(addr);
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: value_keys })?
            .values;
        for (address, value) in addrs.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount == 0 {
                continue;
            }
            all.push(AddressAmountEntry { address, amount });
        }
    }
    sort_address_amount_entries(&mut all);
    let total = all.len();
    let p = page.max(1);
    let l = limit.max(1);
    let off = l.saturating_mul(p - 1);
    let end = (off + l).min(total);
    let slice = if off >= total { vec![] } else { all[off..end].to_vec() };
    Ok((total, slice))
}

pub fn get_address_activity_for_address(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    address: &str,
) -> Result<AddressActivityEntry> {
    let table = provider.table();
    let mut entry = AddressActivityEntry::default();

    let transfer_len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.address_activity_transfer_list_len_key(address),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if transfer_len > 0 {
        let mut idx_keys = Vec::with_capacity(transfer_len as usize);
        for idx in 0..transfer_len {
            idx_keys.push(table.address_activity_transfer_list_idx_key(address, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut tokens = Vec::new();
        let mut value_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            if raw.len() != 12 {
                continue;
            }
            let alk = SchemaAlkaneId {
                block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                tx: u64::from_be_bytes([
                    raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                ]),
            };
            value_keys.push(table.address_activity_transfer_key(address, &alk));
            tokens.push(alk);
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: value_keys })?
            .values;
        for (alk, value) in tokens.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount > 0 {
                entry.transfer_volume.insert(alk, amount);
            }
        }
    }

    let received_len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.address_activity_total_received_list_len_key(address),
        })?
        .value
        .and_then(|bytes| {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if received_len > 0 {
        let mut idx_keys = Vec::with_capacity(received_len as usize);
        for idx in 0..received_len {
            idx_keys.push(table.address_activity_total_received_list_idx_key(address, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut tokens = Vec::new();
        let mut value_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            if raw.len() != 12 {
                continue;
            }
            let alk = SchemaAlkaneId {
                block: u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
                tx: u64::from_be_bytes([
                    raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                ]),
            };
            value_keys.push(table.address_activity_total_received_key(address, &alk));
            tokens.push(alk);
        }
        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: value_keys })?
            .values;
        for (alk, value) in tokens.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(amount) = decode_u128_value(&bytes) else {
                continue;
            };
            if amount > 0 {
                entry.total_received.insert(alk, amount);
            }
        }
    }
    Ok(entry)
}

pub fn get_scriptpubkey_for_address(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    addr: &str,
) -> Result<Option<ScriptBuf>> {
    let table = provider.table();
    let key = table.addr_spk_key(addr);
    let v = provider.get_raw_value(GetRawValueParams { blockhash, key })?.value;
    Ok(v.map(ScriptBuf::from))
}
