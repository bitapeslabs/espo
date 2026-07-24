use super::defs::{EspoTraceType, SignedU128, SignedU128MapExt};
use super::utils::{
    Unallocated, compute_nets, is_op_return, parse_protostones, parse_short_id,
    schema_id_from_parts, transfers_to_sheet, tx_has_op_return, u128_to_u32,
};
use crate::alkanes::trace::{
    EspoBlock, EspoHostFunctionValues, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceStatus, EspoTrace,
};
use crate::alkanes::utils::clean_espo_sandshrew_like_trace;
use crate::config::{
    debug_enabled, get_electrum_like, get_espo_db, get_metashrew, get_metashrew_sdb, get_network,
    strict_check_alkane_balances, strict_check_trace_mismatches, strict_check_utxos,
};
use crate::debug;
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::storage::{AmmDataTable, SearchIndexField};
use crate::modules::ammdata::utils::search::collect_search_prefixes;
use crate::modules::essentials::storage::{
    AddressActivityEntry, AddressAmountEntry, AddressContractAmountEntry, AddressIndexListKind,
    AddressOrbitalBalanceEntry, AlkaneBalanceTxEntry, AlkaneTxSummary, BalanceEntry, HolderEntry,
    HolderId, HoldersCountEntry, OrbitalHolderEntry,
    address_index_list_id_alkane_balance_txs_by_token, address_index_list_id_alkane_block_txs,
    append_address_index_values, build_new_outpoint_pos_versioned_puts,
    build_new_outpoint_spent_versioned_puts, build_new_tx_pos_versioned_puts,
    build_outpoint_spent_versioned_puts, decode_address_contract_amount_entries,
    decode_address_orbital_balance_entries, decode_balances_vec, decode_orbital_holder_entry,
    decode_outpoint_pointer_blob_v3, decode_pointer_idx_u64, decode_u128_value,
    encode_address_contract_amount_entries, encode_address_orbital_balance_entries,
    encode_orbital_holder_entry, encode_outpoint_pointer_blob_v3, encode_pointer_idx_u64,
    encode_tx_pointer_blob_v3, encode_u128_value, encode_vec, get_holders_count_encoded,
    mk_outpoint, resolve_outpoint_id_v2, resolve_outpoint_ids_batch_v2,
    resolve_outpoint_spent_by_id_v2, resolve_outpoint_spent_by_ids_batch_v2,
    resolve_tx_pointer_ids_batch_v2, spk_to_address_str,
};
use crate::modules::essentials::storage::{
    EssentialsProvider, EssentialsTable, GetCreationRecordParams, GetFactoryChildrenParams,
    GetListEntriesDescParams, GetMultiValuesParams, GetRawValueParams, SetBatchParams,
};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::state_at::StateAt;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Context, Result, anyhow};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{ScriptBuf, Transaction, Txid};
use borsh::BorshDeserialize;
use protorune_support::protostone::{Protostone, ProtostoneEdict};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

static AMMDATA_MDB: OnceLock<Arc<Mdb>> = OnceLock::new();
const ALKANES_V217_EDICT_FIX_HEIGHT: u64 = 943_500;

pub(crate) type ProjectionSheet = BTreeMap<SchemaAlkaneId, u128>;
type AlkaneDeltaMap = HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>;
type RootDebitCandidateMap = HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, u128>>;

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) struct ContractProjectionContext<'a> {
    pub tx: &'a Transaction,
    pub protostone: &'a Protostone,
    pub protostone_index: usize,
    pub shadow_vout: u32,
    pub incoming: &'a ProjectionSheet,
}

pub(crate) struct ContractProjection {
    pub output: ProjectionSheet,
}

pub(crate) trait MempoolContractProjector {
    fn project(&mut self, ctx: ContractProjectionContext<'_>) -> Option<ContractProjection>;

    fn project_from_inputs(
        &mut self,
        _ctx: ContractProjectionContext<'_>,
    ) -> Option<ContractProjection> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum AttributionSource {
    Address(String),
    Contract(SchemaAlkaneId),
}

type SourceAmounts = VecDeque<(AttributionSource, u128)>;
type SourcedSheet = BTreeMap<SchemaAlkaneId, SourceAmounts>;
type ContractTokenAmounts = BTreeMap<(SchemaAlkaneId, SchemaAlkaneId), u128>;
type AddressContractAmounts = HashMap<String, ContractTokenAmounts>;
type SourceTokenAddressAmounts = HashMap<(SchemaAlkaneId, SchemaAlkaneId), HashMap<String, u128>>;
type OrbitalChildHolderDeltas =
    HashMap<SchemaAlkaneId, BTreeMap<HolderId, BTreeMap<SchemaAlkaneId, SignedU128>>>;

#[derive(Default)]
struct TransferApplication {
    allocations: HashMap<u32, Vec<BalanceEntry>>,
    send_contracts: AddressContractAmounts,
    receive_contracts_by_vout: HashMap<u32, ContractTokenAmounts>,
}

#[derive(Default)]
struct TraceSourceFlow {
    returned: SourcedSheet,
    send_contracts: AddressContractAmounts,
}

fn ammdata_mdb() -> Arc<Mdb> {
    AMMDATA_MDB
        .get_or_init(|| Arc::new(Mdb::from_db(get_espo_db(), b"ammdata:")))
        .clone()
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

fn apply_signed_balance_delta_or_panic(
    current: u128,
    delta: SignedU128,
    check_negative_balances: bool,
    panic_message: impl FnOnce() -> String,
) -> u128 {
    let (is_negative, amount) = delta.as_parts();
    if is_negative {
        if check_negative_balances && amount > current {
            panic!("{}", panic_message());
        }
        current.saturating_sub(amount)
    } else {
        current.saturating_add(amount)
    }
}

#[cfg(test)]
mod balance_delta_tests {
    use super::*;

    #[test]
    fn negative_balance_clamps_when_check_disabled() {
        let next = apply_signed_balance_delta_or_panic(5, SignedU128::negative(8), false, || {
            "should not panic".to_string()
        });

        assert_eq!(next, 0);
    }

    #[test]
    #[should_panic(expected = "[balances] negative holder balance detected")]
    fn negative_balance_panics_when_check_enabled() {
        let _ = apply_signed_balance_delta_or_panic(5, SignedU128::negative(8), true, || {
            "[balances] negative holder balance detected".to_string()
        });
    }
}

// clean_espo_sandshrew_like_trace moved to crate::alkanes::utils so every
// module that walks trace events as a call stack shares one implementation.
fn parse_u128_from_str(input: &str) -> Option<u128> {
    if let Some(hex) = input.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        input.parse::<u128>().ok()
    }
}

pub(crate) fn mint_deltas_from_trace(
    trace: &EspoSandshrewLikeTrace,
    host_function_values: &EspoHostFunctionValues,
) -> Option<BTreeMap<SchemaAlkaneId, u128>> {
    let trace = clean_espo_sandshrew_like_trace(trace, host_function_values)?;

    #[derive(Clone)]
    struct Frame {
        owner: Option<SchemaAlkaneId>,
        mint_candidate: bool,
        incoming: Vec<(SchemaAlkaneId, u128)>,
        nested_mints: BTreeMap<SchemaAlkaneId, u128>,
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
                    nested_mints: BTreeMap::new(),
                });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                let Some(frame) = stack.pop() else {
                    return None;
                };
                let mut frame_mints = frame.nested_mints;
                if frame.mint_candidate && ret.status == EspoSandshrewLikeTraceStatus::Success {
                    if let Some(owner) = frame.owner {
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
                            let nested = frame_mints.get(&owner).copied().unwrap_or(0);
                            let delta = value.saturating_sub(nested);
                            if delta > 0 {
                                *deltas.entry(owner).or_default() =
                                    deltas.get(&owner).copied().unwrap_or(0).saturating_add(delta);
                                *frame_mints.entry(owner).or_default() = frame_mints
                                    .get(&owner)
                                    .copied()
                                    .unwrap_or(0)
                                    .saturating_add(delta);
                            }
                        }
                    }
                }
                if let Some(parent) = stack.last_mut() {
                    for (alkane, amount) in frame_mints {
                        *parent.nested_mints.entry(alkane).or_default() = parent
                            .nested_mints
                            .get(&alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(amount);
                    }
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
) -> (bool, AlkaneDeltaMap, RootDebitCandidateMap) {
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
        return (false, HashMap::new(), HashMap::new());
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
        deltas: AlkaneDeltaMap,
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
        outflows: &mut AlkaneDeltaMap,
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
        outflows: &mut AlkaneDeltaMap,
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

    fn cancel_positive_delta(
        outflows: &mut AlkaneDeltaMap,
        owner: SchemaAlkaneId,
        token: SchemaAlkaneId,
        amount: u128,
    ) -> u128 {
        let available = outflows
            .get(&owner)
            .and_then(|per_token| per_token.get(&token))
            .map(|delta| {
                let (negative, amount) = delta.as_parts();
                if negative { 0 } else { amount }
            })
            .unwrap_or(0);
        let amount = available.min(amount);
        if amount > 0 {
            add_delta(outflows, owner, token, SignedU128::negative(amount));
        }
        amount
    }

    fn add_root_debit_candidate(
        candidates: &mut RootDebitCandidateMap,
        owner: SchemaAlkaneId,
        token: SchemaAlkaneId,
        amount: u128,
    ) {
        if amount == 0 {
            return;
        }
        let entry = candidates.entry(owner).or_default();
        *entry.entry(token).or_default() =
            entry.get(&token).copied().unwrap_or(0).saturating_add(amount);
    }

    // Merge a child's delta map into its parent (used to drop effects on failure/static).
    fn merge_deltas(target: &mut AlkaneDeltaMap, child: AlkaneDeltaMap) {
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

    let mut outflows: AlkaneDeltaMap = HashMap::new();
    let mut root_debit_candidates: RootDebitCandidateMap = HashMap::new();
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
                if frame.parent_normal.is_some() {
                    for (token, amount) in &outgoing {
                        apply_transfer(
                            &mut frame.deltas,
                            Some(frame.owner),
                            frame.parent_normal,
                            *token,
                            *amount,
                        );
                    }
                } else {
                    // Root returns have no alkane parent, but they still consume alkanes
                    // credited to the root by this trace. Only cancel observed positive
                    // trace-local deltas here; any remaining response amount is a bounded
                    // debit candidate that is validated against the persisted balance before
                    // the block writes are built.
                    for (token, amount) in &outgoing {
                        let canceled =
                            cancel_positive_delta(&mut frame.deltas, frame.owner, *token, *amount);
                        let remaining = amount.saturating_sub(canceled);
                        if remaining > 0 && *token != frame.owner {
                            add_delta(
                                &mut frame.deltas,
                                frame.owner,
                                *token,
                                SignedU128::negative(remaining),
                            );
                            add_root_debit_candidate(
                                &mut root_debit_candidates,
                                frame.owner,
                                *token,
                                remaining,
                            );
                        }
                    }
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
        return (false, HashMap::new(), HashMap::new());
    }

    (true, outflows, root_debit_candidates)
}

fn source_amounts_total(sources: &SourceAmounts) -> u128 {
    sources.iter().map(|(_, amount)| *amount).fold(0u128, u128::saturating_add)
}

fn add_source_amount(sources: &mut SourceAmounts, source: AttributionSource, amount: u128) {
    if amount == 0 {
        return;
    }
    if let Some((last_source, last_amount)) = sources.back_mut() {
        if *last_source == source {
            *last_amount = last_amount.saturating_add(amount);
            return;
        }
    }
    sources.push_back((source, amount));
}

fn source_amount(source: AttributionSource, amount: u128) -> SourceAmounts {
    let mut sources = SourceAmounts::new();
    add_source_amount(&mut sources, source, amount);
    sources
}

fn add_sources_to_sheet(sheet: &mut SourcedSheet, token: SchemaAlkaneId, sources: SourceAmounts) {
    if sources.is_empty() {
        return;
    }
    let entry = sheet.entry(token).or_default();
    for (source, amount) in sources {
        add_source_amount(entry, source, amount);
    }
    if entry.is_empty() {
        sheet.remove(&token);
    }
}

fn merge_sourced_sheet(target: &mut SourcedSheet, source: SourcedSheet) {
    for (token, sources) in source {
        add_sources_to_sheet(target, token, sources);
    }
}

fn add_contract_token_amount(
    map: &mut ContractTokenAmounts,
    contract: SchemaAlkaneId,
    token: SchemaAlkaneId,
    amount: u128,
) {
    if amount == 0 {
        return;
    }
    let key = (contract, token);
    let entry = map.entry(key).or_default();
    *entry = entry.saturating_add(amount);
}

fn add_address_contract_amount(
    map: &mut AddressContractAmounts,
    address: String,
    contract: SchemaAlkaneId,
    token: SchemaAlkaneId,
    amount: u128,
) {
    if amount == 0 {
        return;
    }
    add_contract_token_amount(map.entry(address).or_default(), contract, token, amount);
}

fn merge_address_contract_amounts(
    target: &mut AddressContractAmounts,
    source: AddressContractAmounts,
) {
    for (address, entries) in source {
        let target_entries = target.entry(address).or_default();
        for ((contract, token), amount) in entries {
            add_contract_token_amount(target_entries, contract, token, amount);
        }
    }
}

fn add_contract_receives_from_sources(
    receives: &mut ContractTokenAmounts,
    token: SchemaAlkaneId,
    sources: &SourceAmounts,
) {
    for (source, amount) in sources.iter() {
        if let AttributionSource::Contract(contract) = source {
            add_contract_token_amount(receives, *contract, token, *amount);
        }
    }
}

fn take_sources_from_amounts(sources: &mut SourceAmounts, amount: u128) -> SourceAmounts {
    let mut remaining = amount;
    let mut taken = SourceAmounts::new();
    while remaining > 0 {
        let remove_front = {
            let Some((source, available)) = sources.front_mut() else {
                break;
            };
            let take = (*available).min(remaining);
            add_source_amount(&mut taken, source.clone(), take);
            *available = available.saturating_sub(take);
            remaining = remaining.saturating_sub(take);
            *available == 0
        };
        if remove_front {
            sources.pop_front();
        }
    }
    taken
}

fn take_sources_from_sheet(
    sheet: &mut SourcedSheet,
    token: &SchemaAlkaneId,
    amount: u128,
) -> SourceAmounts {
    if amount == 0 {
        return SourceAmounts::new();
    }
    let Some(sources) = sheet.get_mut(token) else {
        return SourceAmounts::new();
    };
    let taken = take_sources_from_amounts(sources, amount);
    if sources.is_empty() {
        sheet.remove(token);
    }
    taken
}

fn take_sources_for_delta(
    source_sheet: &mut SourcedSheet,
    delta: &BTreeMap<SchemaAlkaneId, u128>,
) -> SourcedSheet {
    let mut out = SourcedSheet::new();
    for (token, amount) in delta {
        let sources = take_sources_from_sheet(source_sheet, token, *amount);
        add_sources_to_sheet(&mut out, *token, sources);
    }
    out
}

fn trace_source_flow(
    trace: &EspoSandshrewLikeTrace,
    incoming_sources: SourcedSheet,
    host_function_values: &EspoHostFunctionValues,
) -> Option<TraceSourceFlow> {
    let trace = clean_espo_sandshrew_like_trace(trace, host_function_values)?;

    #[derive(Copy, Clone, Eq, PartialEq)]
    enum FrameKind {
        Normal,
        Delegate,
        Static,
    }

    #[derive(Clone)]
    struct Frame {
        kind: FrameKind,
        owner: SchemaAlkaneId,
        parent_normal_index: Option<usize>,
        incoming: SourcedSheet,
        held: SourcedSheet,
    }

    fn nearest_normal_index(stack: &[Frame]) -> Option<usize> {
        stack.iter().rposition(|frame| matches!(frame.kind, FrameKind::Normal))
    }

    let mut stack: Vec<Frame> = Vec::new();
    let mut top_sources = incoming_sources;
    let mut out = TraceSourceFlow::default();

    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                let Some(owner) = parse_short_id(&inv.context.myself) else {
                    return None;
                };
                let in_static_subtree =
                    stack.iter().any(|frame| matches!(frame.kind, FrameKind::Static));
                let kind = if in_static_subtree {
                    FrameKind::Static
                } else {
                    match inv.typ.to_ascii_lowercase().as_str() {
                        "delegatecall" => FrameKind::Delegate,
                        "staticcall" => FrameKind::Static,
                        _ => FrameKind::Normal,
                    }
                };
                let parent_normal_index = nearest_normal_index(&stack);
                let mut incoming = SourcedSheet::new();

                if !matches!(kind, FrameKind::Static) {
                    let incoming_amounts = transfers_to_sheet(&inv.context.incoming_alkanes);
                    for (token, amount) in incoming_amounts {
                        let mut sources = if let Some(parent_idx) = parent_normal_index {
                            take_sources_from_sheet(&mut stack[parent_idx].held, &token, amount)
                        } else {
                            take_sources_from_sheet(&mut top_sources, &token, amount)
                        };
                        let sourced = source_amounts_total(&sources);
                        if sourced < amount {
                            if let Some(parent_idx) = parent_normal_index {
                                add_source_amount(
                                    &mut sources,
                                    AttributionSource::Contract(stack[parent_idx].owner),
                                    amount - sourced,
                                );
                            }
                        }
                        add_sources_to_sheet(&mut incoming, token, sources);
                    }
                }

                stack.push(Frame {
                    kind,
                    owner,
                    parent_normal_index,
                    held: incoming.clone(),
                    incoming,
                });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                let Some(mut frame) = stack.pop() else {
                    return None;
                };

                if matches!(frame.kind, FrameKind::Static) {
                    continue;
                }

                if ret.status == EspoSandshrewLikeTraceStatus::Failure {
                    if let Some(parent_idx) = frame.parent_normal_index {
                        merge_sourced_sheet(&mut stack[parent_idx].held, frame.incoming);
                    }
                    continue;
                }

                let outgoing = transfers_to_sheet(&ret.response.alkanes);
                let mut returned = SourcedSheet::new();
                for (token, amount) in outgoing {
                    let mut sources = take_sources_from_sheet(&mut frame.held, &token, amount);
                    let sourced = source_amounts_total(&sources);
                    if sourced < amount {
                        add_source_amount(
                            &mut sources,
                            AttributionSource::Contract(frame.owner),
                            amount - sourced,
                        );
                    }
                    add_sources_to_sheet(&mut returned, token, sources);
                }

                for (token, sources) in frame.held {
                    for (source, amount) in sources {
                        if let AttributionSource::Address(address) = source {
                            add_address_contract_amount(
                                &mut out.send_contracts,
                                address,
                                frame.owner,
                                token,
                                amount,
                            );
                        }
                    }
                }

                if let Some(parent_idx) = frame.parent_normal_index {
                    merge_sourced_sheet(&mut stack[parent_idx].held, returned);
                } else {
                    merge_sourced_sheet(&mut out.returned, returned);
                }
            }
            EspoSandshrewLikeTraceEvent::Create(_) => {}
        }
    }

    if !stack.is_empty() {
        return None;
    }

    Some(out)
}

fn trace_root_owner(
    trace: &EspoSandshrewLikeTrace,
    host_function_values: &EspoHostFunctionValues,
) -> Option<SchemaAlkaneId> {
    let cleaned = clean_espo_sandshrew_like_trace(trace, host_function_values);
    let events = cleaned.as_ref().map(|t| t.events.as_slice()).unwrap_or(&trace.events);
    events.iter().find_map(|ev| {
        if let EspoSandshrewLikeTraceEvent::Invoke(inv) = ev {
            parse_short_id(&inv.context.myself)
        } else {
            None
        }
    })
}

fn final_trace_return_sources(
    root_owner: SchemaAlkaneId,
    mut incoming_sources: SourcedSheet,
    net_out: &BTreeMap<SchemaAlkaneId, u128>,
) -> SourcedSheet {
    let mut out = SourcedSheet::new();
    for (token, amount) in net_out {
        if *amount == 0 {
            continue;
        }
        let mut sources = take_sources_from_sheet(&mut incoming_sources, token, *amount);
        let sourced = source_amounts_total(&sources);
        if sourced < *amount {
            add_source_amount(
                &mut sources,
                AttributionSource::Contract(root_owner),
                amount.saturating_sub(sourced),
            );
        }
        add_sources_to_sheet(&mut out, *token, sources);
    }
    out
}

/* -------------------------- Edicts + routing (multi-protostone, per your rules) -------------------------- */

/// Whether `vout` is a valid, spendable, non-OP_RETURN output index for this tx.
fn is_valid_spend_vout(tx: &Transaction, vout: u32) -> bool {
    let i = vout as usize;
    i < tx.output.len() && !is_op_return(&tx.output[i].script_pubkey)
}

fn apply_transfers_multi_attributed(
    tx: &Transaction,
    protostones: &[Protostone],
    traces_for_tx: &[EspoTrace],
    block_height: u64,
    host_function_values: &EspoHostFunctionValues,
    mut seed_unalloc: Unallocated, // VIN balances only
    mut seed_sources: SourcedSheet,
    mut contract_projector: Option<&mut dyn MempoolContractProjector>,
) -> Result<TransferApplication> {
    let mut out_map: HashMap<u32, Vec<BalanceEntry>> = HashMap::new();
    let mut receive_contracts_by_vout: HashMap<u32, ContractTokenAmounts> = HashMap::new();
    let mut send_contracts: AddressContractAmounts = HashMap::new();

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
    let mut incoming_shadow_sources: Vec<SourcedSheet> =
        vec![SourcedSheet::new(); protostones.len()];

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

    fn push_sources_to_vout(
        receive_contracts_by_vout: &mut HashMap<u32, ContractTokenAmounts>,
        vout: u32,
        source_delta: &SourcedSheet,
    ) {
        for (token, sources) in source_delta {
            add_contract_receives_from_sources(
                receive_contracts_by_vout.entry(vout).or_default(),
                *token,
                sources,
            );
        }
    }

    fn route_delta(
        target: u32,
        delta: &BTreeMap<SchemaAlkaneId, u128>,
        source_delta: &mut SourcedSheet,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        receive_contracts_by_vout: &mut HashMap<u32, ContractTokenAmounts>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        incoming_shadow_sources: &mut [SourcedSheet],
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
                    let mut routed_sources = SourcedSheet::new();
                    let sources = take_sources_from_sheet(source_delta, rid, amt);
                    add_sources_to_sheet(&mut routed_sources, *rid, sources);
                    out_map
                        .entry(*out_i)
                        .or_default()
                        .push(BalanceEntry { alkane: *rid, amount: amt });
                    push_sources_to_vout(receive_contracts_by_vout, *out_i, &routed_sources);
                }
            }
            return;
        }

        if target < n_outputs {
            if !is_valid_spend_vout(tx, target) {
                return;
            }
            push_to_vout(out_map, target, delta);
            push_sources_to_vout(receive_contracts_by_vout, target, source_delta);
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
            merge_sourced_sheet(&mut incoming_shadow_sources[idx], std::mem::take(source_delta));
            return;
        }
        // else burn by omission
    }

    fn apply_single_edict(
        sheet: &mut BTreeMap<SchemaAlkaneId, u128>,
        source_sheet: &mut SourcedSheet,
        ed: &ProtostoneEdict,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        receive_contracts_by_vout: &mut HashMap<u32, ContractTokenAmounts>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        incoming_shadow_sources: &mut [SourcedSheet],
        tx: &Transaction,
        spendable_vouts: &[u32],
        n_outputs: u32,
        multicast_index: u32,
        shadow_base: u32,
        shadow_end: u32,
        block_height: u64,
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
                let mut source_delta = SourcedSheet::new();
                let sources = take_sources_from_sheet(source_sheet, &rid, have);
                add_sources_to_sheet(&mut source_delta, rid, sources);
                // zero it out from the sheet before routing
                *entry = 0;
                sheet.remove(&rid);

                route_delta(
                    out_idx,
                    &delta,
                    &mut source_delta,
                    out_map,
                    receive_contracts_by_vout,
                    incoming_shadow,
                    incoming_shadow_sources,
                    tx,
                    spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                );
            } else if block_height < ALKANES_V217_EDICT_FIX_HEIGHT
                && have <= (spendable_vouts.len() as u128).saturating_mul(ed.amount)
            {
                // Pre-v2.1.7 canonical metashrew behavior for amount-capped multicast:
                // an OP_RETURN consumes one amount slot, but that slot is deferred and
                // appended to the next spendable output after the regular walk stops.
                let mut remaining = have;
                let mut used: u128 = 0;
                let mut deferred: Vec<(u128, SourcedSheet)> = Vec::new();
                let mut last_consumed_idx: Option<usize> = None;

                for (idx, output) in tx.output.iter().enumerate() {
                    if remaining == 0 {
                        break;
                    }
                    let give = remaining.min(ed.amount);
                    if give == 0 {
                        break;
                    }
                    let mut routed_sources = SourcedSheet::new();
                    let sources = take_sources_from_sheet(source_sheet, &rid, give);
                    add_sources_to_sheet(&mut routed_sources, rid, sources);
                    remaining = remaining.saturating_sub(give);
                    used = used.saturating_add(give);
                    last_consumed_idx = Some(idx);

                    if is_op_return(&output.script_pubkey) {
                        deferred.push((give, routed_sources));
                    } else {
                        out_map
                            .entry(idx as u32)
                            .or_default()
                            .push(BalanceEntry { alkane: rid, amount: give });
                        push_sources_to_vout(
                            receive_contracts_by_vout,
                            idx as u32,
                            &routed_sources,
                        );
                    }
                }

                let mut search_from =
                    last_consumed_idx.map(|idx| idx.saturating_add(1)).unwrap_or(0);
                for (amount, routed_sources) in deferred {
                    let Some(next_idx) =
                        tx.output.iter().enumerate().skip(search_from).find_map(|(idx, output)| {
                            if is_op_return(&output.script_pubkey) { None } else { Some(idx) }
                        })
                    else {
                        continue;
                    };
                    out_map
                        .entry(next_idx as u32)
                        .or_default()
                        .push(BalanceEntry { alkane: rid, amount });
                    push_sources_to_vout(
                        receive_contracts_by_vout,
                        next_idx as u32,
                        &routed_sources,
                    );
                    search_from = next_idx.saturating_add(1);
                }

                *entry = entry.saturating_sub(used);
                if *entry == 0 {
                    sheet.remove(&rid);
                }
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
                    let mut routed_sources = SourcedSheet::new();
                    let sources = take_sources_from_sheet(source_sheet, &rid, give);
                    add_sources_to_sheet(&mut routed_sources, rid, sources);
                    out_map.entry(*v).or_default().push(BalanceEntry { alkane: rid, amount: give });
                    push_sources_to_vout(receive_contracts_by_vout, *v, &routed_sources);
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
        let mut source_delta = SourcedSheet::new();
        let sources = take_sources_from_sheet(source_sheet, &rid, take);
        add_sources_to_sheet(&mut source_delta, rid, sources);
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
            &mut source_delta,
            out_map,
            receive_contracts_by_vout,
            incoming_shadow,
            incoming_shadow_sources,
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
        let mut source_sheet: SourcedSheet = SourcedSheet::new();

        // merge routed-in firstx
        for (rid, amt) in std::mem::take(&mut incoming_shadow[i]) {
            if amt == 0 {
                continue;
            }
            *sheet.entry(rid).or_default() =
                sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
        }
        merge_sourced_sheet(&mut source_sheet, std::mem::take(&mut incoming_shadow_sources[i]));

        // if there is a trace for this protostone, compute net_out and status
        let trace_for_shadow = trace_by_shadow.get(&shadow_vout).copied();
        let (net_in, net_out, status) = match trace_for_shadow {
            Some(trace) => compute_nets(trace),
            None => (None, None, EspoTraceType::NOTRACE),
        };
        // Metashrew can omit incoming_alkanes on reverted traces; fall back to NOTRACE
        // so original VIN balances are still available for edicts/pointers.
        let revert_missing_incoming =
            status == EspoTraceType::REVERT && net_in.as_ref().map_or(true, |m| m.is_empty());
        let status = if revert_missing_incoming { EspoTraceType::NOTRACE } else { status };

        // AMM and frBTC mempool projections are derived from the actual VIN sheet and
        // transaction outputs. Prefer them over a successful view trace, which was
        // evaluated without unconfirmed ancestors and can therefore be stale.
        let input_projection = if status != EspoTraceType::REVERT {
            contract_projector.as_deref_mut().and_then(|projector| {
                let mut incoming = sheet.clone();
                if i == 0 {
                    for (rid, amt) in seed_unalloc.clone().drain_all() {
                        if amt == 0 {
                            continue;
                        }
                        *incoming.entry(rid).or_default() =
                            incoming.get(&rid).copied().unwrap_or(0).saturating_add(amt);
                    }
                }
                projector.project_from_inputs(ContractProjectionContext {
                    tx,
                    protostone: ps,
                    protostone_index: i,
                    shadow_vout,
                    incoming: &incoming,
                })
            })
        } else {
            None
        };
        let used_input_projection = input_projection.is_some();
        if let Some(projection) = input_projection {
            if i == 0 {
                seed_unalloc.drain_all();
                merge_sourced_sheet(&mut source_sheet, std::mem::take(&mut seed_sources));
            }
            sheet = projection.output;
        }

        // On success, consume incoming amounts so only returned/minted balances remain.
        if !used_input_projection && status == EspoTraceType::SUCCESS {
            if i == 0 {
                merge_sourced_sheet(&mut source_sheet, std::mem::take(&mut seed_sources));
            }
            let mut trace_in_sources = SourcedSheet::new();
            if let Some(ref net_in_map) = net_in {
                for (rid, amt) in net_in_map {
                    if *amt == 0 {
                        continue;
                    }
                    let sources = take_sources_from_sheet(&mut source_sheet, rid, *amt);
                    add_sources_to_sheet(&mut trace_in_sources, *rid, sources);
                    let entry = sheet.entry(*rid).or_default();
                    *entry = entry.saturating_sub(*amt);
                    if *entry == 0 {
                        sheet.remove(rid);
                    }
                }
            }
            let trace_in_sources_for_receive = trace_in_sources.clone();
            if let Some(trace) = trace_for_shadow {
                if let Some(flow) = trace_source_flow(trace, trace_in_sources, host_function_values)
                {
                    merge_address_contract_amounts(&mut send_contracts, flow.send_contracts);
                }
            }
            if let (Some(trace), Some(ref net_out_map)) = (trace_for_shadow, net_out.as_ref()) {
                if let Some(root_owner) = trace_root_owner(trace, host_function_values) {
                    let final_sources = final_trace_return_sources(
                        root_owner,
                        trace_in_sources_for_receive,
                        net_out_map,
                    );
                    merge_sourced_sheet(&mut source_sheet, final_sources);
                }
            }
        }

        // add net_out to sheet
        if !used_input_projection && status == EspoTraceType::SUCCESS {
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
        if !used_input_projection && i == 0 && status == EspoTraceType::NOTRACE {
            for (rid, amt) in seed_unalloc.drain_all() {
                if amt == 0 {
                    continue;
                }
                *sheet.entry(rid).or_default() =
                    sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
            }
            merge_sourced_sheet(&mut source_sheet, std::mem::take(&mut seed_sources));
        }

        if !used_input_projection && status == EspoTraceType::NOTRACE {
            if let Some(projector) = contract_projector.as_deref_mut() {
                if let Some(projection) = projector.project(ContractProjectionContext {
                    tx,
                    protostone: ps,
                    protostone_index: i,
                    shadow_vout,
                    incoming: &sheet,
                }) {
                    sheet = projection.output;
                }
            }
        }

        // If we have a status and it is Failure → refund net_in (only), skip edicts.
        if status == EspoTraceType::REVERT {
            if i == 0 {
                merge_sourced_sheet(&mut source_sheet, std::mem::take(&mut seed_sources));
            }
            if let Some(ref net_in_map) = net_in {
                if let Some(refund_ptr) = ps.refund {
                    let mut source_delta = take_sources_for_delta(&mut source_sheet, net_in_map);
                    route_delta(
                        refund_ptr,
                        &net_in_map,
                        &mut source_delta,
                        &mut out_map,
                        &mut receive_contracts_by_vout,
                        &mut incoming_shadow,
                        &mut incoming_shadow_sources,
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
                    &mut source_sheet,
                    ed,
                    &mut out_map,
                    &mut receive_contracts_by_vout,
                    &mut incoming_shadow,
                    &mut incoming_shadow_sources,
                    tx,
                    &spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                    block_height,
                ) {
                    eprintln!("[ESSENTIALS::balances] WARN edict apply failed: {e:?}");
                }
            }
        }

        // leftovers after edicts:
        if !sheet.is_empty() {
            if let Some(ptr) = ps.pointer {
                let mut source_delta = take_sources_for_delta(&mut source_sheet, &sheet);
                route_delta(
                    ptr,
                    &sheet,
                    &mut source_delta,
                    &mut out_map,
                    &mut receive_contracts_by_vout,
                    &mut incoming_shadow,
                    &mut incoming_shadow_sources,
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
                    let source_delta = take_sources_for_delta(&mut source_sheet, &sheet);
                    push_to_vout(&mut out_map, v, &sheet);
                    push_sources_to_vout(&mut receive_contracts_by_vout, v, &source_delta);
                }
                // else burn by omission
            }
        }
    }

    Ok(TransferApplication { allocations: out_map, send_contracts, receive_contracts_by_vout })
}

fn apply_transfers_multi(
    tx: &Transaction,
    protostones: &[Protostone],
    traces_for_tx: &[EspoTrace],
    block_height: u64,
    seed_unalloc: Unallocated,
    contract_projector: Option<&mut dyn MempoolContractProjector>,
) -> Result<HashMap<u32, Vec<BalanceEntry>>> {
    let host_function_values = EspoHostFunctionValues::default();
    Ok(apply_transfers_multi_attributed(
        tx,
        protostones,
        traces_for_tx,
        block_height,
        &host_function_values,
        seed_unalloc,
        SourcedSheet::new(),
        contract_projector,
    )?
    .allocations)
}

pub(crate) fn project_tx_output_balances_from_traces(
    tx: &Transaction,
    traces_for_tx: &[EspoTrace],
    input_balances: Vec<BalanceEntry>,
) -> HashMap<u32, Vec<BalanceEntry>> {
    project_tx_output_balances_from_traces_with_projector(tx, traces_for_tx, input_balances, None)
}

pub(crate) fn project_tx_output_balances_from_traces_with_projector(
    tx: &Transaction,
    traces_for_tx: &[EspoTrace],
    input_balances: Vec<BalanceEntry>,
    contract_projector: Option<&mut dyn MempoolContractProjector>,
) -> HashMap<u32, Vec<BalanceEntry>> {
    if !tx_has_op_return(tx) {
        return HashMap::new();
    }

    let protostones = match parse_protostones(tx) {
        Ok(protostones) => protostones,
        Err(_) => return HashMap::new(),
    };
    if protostones.is_empty() {
        return HashMap::new();
    }

    let mut seed_unalloc = Unallocated::default();
    for entry in input_balances {
        if entry.amount > 0 {
            seed_unalloc.add(entry.alkane, entry.amount);
        }
    }

    apply_transfers_multi(
        tx,
        &protostones,
        traces_for_tx,
        ALKANES_V217_EDICT_FIX_HEIGHT,
        seed_unalloc,
        contract_projector,
    )
    .unwrap_or_default()
}

#[cfg(test)]
mod attribution_tests {
    use super::*;

    fn alkane(block: u32, tx: u64) -> SchemaAlkaneId {
        SchemaAlkaneId { block, tx }
    }

    fn address_sources(token: SchemaAlkaneId, address: &str, amount: u128) -> SourcedSheet {
        address_source_chunks(token, &[(address, amount)])
    }

    fn address_source_chunks(token: SchemaAlkaneId, chunks: &[(&str, u128)]) -> SourcedSheet {
        let mut sheet = SourcedSheet::new();
        let mut sources = SourceAmounts::new();
        for (address, amount) in chunks {
            add_source_amount(
                &mut sources,
                AttributionSource::Address((*address).to_string()),
                *amount,
            );
        }
        add_sources_to_sheet(&mut sheet, token, sources);
        sheet
    }

    #[test]
    fn root_return_without_parent_marks_bounded_debit_candidate() {
        let owner = alkane(2, 91332);
        let token = alkane(2, 0);
        let trace_json = r#"
{
  "outpoint": "9198a67bb28d11dc616e81ef3bed4e7949dd09f04f7592fcd31ea9f2228f6231:4",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x164c4"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": ["0x23", "0x2", "0x0", "0x989680", "0x0", "0x0", "0x0", "0x0"],
          "incomingAlkanes": [],
          "vout": 4
        },
        "fuel": 4835967
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x989680"},
            {"id": {"block": "0x2", "tx": "0x164c4"}, "value": "0x1"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let txid = Txid::from_byte_array([0u8; 32]);
        let (ok, deltas, root_debits) =
            accumulate_alkane_balance_deltas(&trace, &txid, &EspoHostFunctionValues::default());

        assert!(ok);
        assert_eq!(
            deltas.get(&owner).and_then(|per_token| per_token.get(&token)).copied(),
            Some(SignedU128::negative(10_000_000)),
            "root response debits are modeled when they can be covered by persisted balance"
        );
        assert_eq!(
            root_debits.get(&owner).and_then(|per_token| per_token.get(&token)).copied(),
            Some(10_000_000),
            "negative-only root response debits must stay identifiable as bounded candidates"
        );
        assert!(
            deltas.get(&owner).and_then(|per_token| per_token.get(&owner)).is_none(),
            "self-token root returns should not create owner balance deltas"
        );
    }

    #[test]
    fn root_return_cancels_trace_local_root_credit() {
        let owner = alkane(2, 77627);
        let token = alkane(2, 77087);
        let trace_json = r#"
{
  "outpoint": "2670291efdeca65b1b8329841635be0acb63bbf272c587313b775c50fc7d2640:3",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x12f3b"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x12d1f"}, "value": "0x64"}
          ],
          "vout": 3
        },
        "fuel": 1
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x12d1f"}, "value": "0x64"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let txid = Txid::from_byte_array([0u8; 32]);
        let (ok, deltas, root_debits) =
            accumulate_alkane_balance_deltas(&trace, &txid, &EspoHostFunctionValues::default());

        assert!(ok);
        assert!(root_debits.is_empty());
        assert!(
            deltas.get(&owner).and_then(|per_token| per_token.get(&token)).is_none(),
            "root returns should consume trace-local root credits"
        );
    }

    fn source_total(sources: &SourceAmounts, source: AttributionSource) -> u128 {
        sources
            .iter()
            .filter_map(
                |(candidate, amount)| if candidate == &source { Some(*amount) } else { None },
            )
            .fold(0u128, u128::saturating_add)
    }

    #[test]
    fn receive_contract_sources_flow_fifo_across_outputs() {
        let token = alkane(2, 0);
        let contract = token;
        let trace_json = r#"
{
  "outpoint": "test",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x0"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x1bee"}
          ],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x28a0"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let host_values = EspoHostFunctionValues::default();
        let flow = trace_source_flow(&trace, address_sources(token, "addr_a", 7_150), &host_values)
            .expect("source flow");

        let mut returned = flow.returned;
        let first = take_sources_from_sheet(&mut returned, &token, 10_000);
        let second = take_sources_from_sheet(&mut returned, &token, 400);

        assert_eq!(source_total(&first, AttributionSource::Address("addr_a".to_string())), 7_150);
        assert_eq!(source_total(&first, AttributionSource::Contract(contract)), 2_850);
        assert_eq!(source_total(&second, AttributionSource::Contract(contract)), 400);
    }

    #[test]
    fn onchain_height_946000_trace_returns_created_chunk_after_vin_fifo() {
        let token = alkane(2, 0);
        let trace_json = r#"
{
  "outpoint": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8:3",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x0"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": ["0x4d", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0"],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x2330ba25"}
          ],
          "vout": 3
        },
        "fuel": 25382538
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x26000000000000000000000000000000", "storage": []}
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x5f69d712000000000000000000000000", "storage": []}
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x2330ba25"},
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x7c08f8"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let host_values = (
            Vec::new(),
            Vec::new(),
            hex::decode("26000000000000000000000000000000").expect("diesel host value"),
            hex::decode("5f69d712000000000000000000000000").expect("fee host value"),
        );
        let flow =
            trace_source_flow(&trace, address_sources(token, "addr_a", 590_395_941), &host_values)
                .expect("source flow");

        let mut returned = flow.returned;
        let vin_chunk = take_sources_from_sheet(&mut returned, &token, 590_395_941);
        let created_chunk = take_sources_from_sheet(&mut returned, &token, 8_128_760);

        assert_eq!(
            source_total(&vin_chunk, AttributionSource::Address("addr_a".to_string())),
            590_395_941
        );
        assert_eq!(source_total(&vin_chunk, AttributionSource::Contract(token)), 0);
        assert_eq!(source_total(&created_chunk, AttributionSource::Contract(token)), 8_128_760);
    }

    #[test]
    fn final_receive_sources_use_outermost_frame_not_inner_return_source() {
        let token = alkane(2, 77_623);
        let outer = alkane(2, 77_631);
        let inner = token;
        let trace_json = r#"
{
  "outpoint": "test",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x12f3f"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": [],
          "incomingAlkanes": [],
          "vout": 6
        },
        "fuel": 1
      }
    },
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x2", "tx": "0x12f37"},
          "caller": {"block": "0x2", "tx": "0x12f3f"},
          "inputs": [],
          "incomingAlkanes": [],
          "vout": 6
        },
        "fuel": 1
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x12f37"}, "value": "0x5"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {
          "alkanes": [
            {"id": {"block": "0x2", "tx": "0x12f37"}, "value": "0x5"}
          ],
          "data": "0x",
          "storage": []
        }
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let host_values = EspoHostFunctionValues::default();

        let flow =
            trace_source_flow(&trace, SourcedSheet::new(), &host_values).expect("source flow");
        let mut nested_return_sources = flow.returned;
        let nested_chunk = take_sources_from_sheet(&mut nested_return_sources, &token, 5);
        assert_eq!(source_total(&nested_chunk, AttributionSource::Contract(inner)), 5);

        let root_owner = trace_root_owner(&trace, &host_values).expect("root owner");
        assert_eq!(root_owner, outer);
        let (_net_in, net_out, status) = compute_nets(&trace);
        assert_eq!(status, EspoTraceType::SUCCESS);
        let mut final_sources =
            final_trace_return_sources(root_owner, SourcedSheet::new(), net_out.as_ref().unwrap());
        let final_chunk = take_sources_from_sheet(&mut final_sources, &token, 5);

        assert_eq!(source_total(&final_chunk, AttributionSource::Contract(outer)), 5);
        assert_eq!(source_total(&final_chunk, AttributionSource::Contract(inner)), 0);
    }

    #[test]
    fn final_receive_sources_keep_vin_fifo_before_outer_created_excess() {
        let token = alkane(2, 0);
        let outer = alkane(4, 1);
        let net_out = BTreeMap::from([(token, 10)]);

        let mut final_sources =
            final_trace_return_sources(outer, address_sources(token, "addr_a", 7), &net_out);
        let chunk = take_sources_from_sheet(&mut final_sources, &token, 10);

        assert_eq!(source_total(&chunk, AttributionSource::Address("addr_a".to_string())), 7);
        assert_eq!(source_total(&chunk, AttributionSource::Contract(outer)), 3);
    }

    #[test]
    fn send_contract_sources_consume_vin_chunks_fifo() {
        let token = alkane(2, 0);
        let parent = alkane(4, 1);
        let child = alkane(4, 2);
        let trace_json = r#"
{
  "outpoint": "test",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x4", "tx": "0x1"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x64"}
          ],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x4", "tx": "0x2"},
          "caller": {"block": "0x4", "tx": "0x1"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x50"}
          ],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x", "storage": []}
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x", "storage": []}
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let host_values = EspoHostFunctionValues::default();
        let flow = trace_source_flow(
            &trace,
            address_source_chunks(token, &[("addr_a", 60), ("addr_b", 40)]),
            &host_values,
        )
        .expect("source flow");

        let addr_a = flow.send_contracts.get("addr_a").expect("addr_a sends");
        let addr_b = flow.send_contracts.get("addr_b").expect("addr_b sends");
        assert_eq!(addr_a.get(&(child, token)).copied(), Some(60));
        assert_eq!(addr_a.get(&(parent, token)).copied(), None);
        assert_eq!(addr_b.get(&(child, token)).copied(), Some(20));
        assert_eq!(addr_b.get(&(parent, token)).copied(), Some(20));
    }

    #[test]
    fn static_subtrees_do_not_attribute_sends() {
        let token = alkane(2, 0);
        let trace_json = r#"
{
  "outpoint": "test",
  "events": [
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x4", "tx": "0x1"},
          "caller": {"block": "0x0", "tx": "0x0"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x64"}
          ],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "invoke",
      "data": {
        "type": "staticcall",
        "context": {
          "myself": {"block": "0x4", "tx": "0x2"},
          "caller": {"block": "0x4", "tx": "0x1"},
          "inputs": [],
          "incomingAlkanes": [],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "invoke",
      "data": {
        "type": "call",
        "context": {
          "myself": {"block": "0x4", "tx": "0x3"},
          "caller": {"block": "0x4", "tx": "0x2"},
          "inputs": [],
          "incomingAlkanes": [
            {"id": {"block": "0x2", "tx": "0x0"}, "value": "0x50"}
          ],
          "vout": 2
        },
        "fuel": 1
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x", "storage": []}
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x", "storage": []}
      }
    },
    {
      "event": "return",
      "data": {
        "status": "success",
        "response": {"alkanes": [], "data": "0x", "storage": []}
      }
    }
  ]
}
"#;
        let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("trace json");
        let host_values = EspoHostFunctionValues::default();
        let flow = trace_source_flow(&trace, address_sources(token, "addr_a", 100), &host_values)
            .expect("source flow");

        let addr = flow.send_contracts.get("addr_a").expect("parent send");
        assert_eq!(addr.len(), 1);
        assert_eq!(addr.get(&(alkane(4, 1), token)).copied(), Some(100));
    }

    #[test]
    fn orbital_rollup_aggregates_child_contract_deltas_by_factory() {
        let factory = alkane(2, 0);
        let child_a = alkane(2, 1);
        let child_b = alkane(2, 2);
        let unrelated = alkane(2, 3);
        let token = alkane(4, 3);

        let mut deltas = AddressContractAmounts::new();
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), child_a, token, 1);
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), child_b, token, 2);
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), unrelated, token, 5);

        let factory_by_child = HashMap::from([(child_a, factory), (child_b, factory)]);
        let rolled = rollup_contract_amounts_to_orbitals(&deltas, &factory_by_child);
        let addr = rolled.get("addr_a").expect("rolled address");

        assert_eq!(addr.len(), 1);
        assert_eq!(addr.get(&(factory, token)).copied(), Some(3));
    }

    #[test]
    fn volume_deltas_are_scoped_by_source_and_token() {
        let factory = alkane(2, 0);
        let other_factory = alkane(2, 1);
        let token_a = alkane(4, 3);
        let token_b = alkane(4, 4);

        let mut deltas = AddressContractAmounts::new();
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), factory, token_a, 7);
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), factory, token_b, 5);
        add_address_contract_amount(&mut deltas, "addr_b".to_string(), factory, token_a, 3);
        add_address_contract_amount(&mut deltas, "addr_a".to_string(), other_factory, token_a, 11);

        let volumes = volume_deltas_from_address_amounts(&deltas);

        assert_eq!(
            volumes.get(&(factory, token_a)).and_then(|m| m.get("addr_a")).copied(),
            Some(7)
        );
        assert_eq!(
            volumes.get(&(factory, token_b)).and_then(|m| m.get("addr_a")).copied(),
            Some(5)
        );
        assert_eq!(
            volumes.get(&(factory, token_a)).and_then(|m| m.get("addr_b")).copied(),
            Some(3)
        );
        assert_eq!(
            volumes.get(&(other_factory, token_a)).and_then(|m| m.get("addr_a")).copied(),
            Some(11)
        );
    }

    #[test]
    fn orbital_holder_delta_accumulator_nets_zero_crossings() {
        let factory = alkane(2, 0);
        let child_a = alkane(2, 1);
        let child_b = alkane(2, 2);
        let addr_a = HolderId::Address("addr_a".to_string());
        let addr_b = HolderId::Address("addr_b".to_string());

        let mut deltas: OrbitalChildHolderDeltas = HashMap::new();
        add_orbital_child_holder_delta(
            &mut deltas,
            factory,
            addr_a.clone(),
            child_a,
            SignedU128::positive(1),
        );
        add_orbital_child_holder_delta(
            &mut deltas,
            factory,
            addr_a.clone(),
            child_b,
            SignedU128::positive(1),
        );
        add_orbital_child_holder_delta(
            &mut deltas,
            factory,
            addr_a.clone(),
            child_b,
            SignedU128::negative(1),
        );
        add_orbital_child_holder_delta(
            &mut deltas,
            factory,
            addr_b.clone(),
            child_a,
            SignedU128::positive(1),
        );
        add_orbital_child_holder_delta(
            &mut deltas,
            factory,
            addr_b.clone(),
            child_a,
            SignedU128::negative(1),
        );

        let entry = deltas.get(&factory).expect("factory deltas");
        assert_eq!(entry.len(), 1);
        let addr_a_children = entry.get(&addr_a).expect("addr_a child deltas");
        assert_eq!(addr_a_children.len(), 1);
        assert_eq!(addr_a_children.get(&child_a).map(SignedU128::as_parts), Some((false, 1)));
        assert!(!entry.contains_key(&addr_b));
    }

    #[test]
    fn factory_hints_resolve_orbital_children_without_name_dependency() {
        let factory = alkane(4, 520);
        let fire_position = alkane(2, 77_635);
        let unrelated = alkane(2, 1);
        let children = HashSet::from([fire_position, unrelated]);
        let hints = HashMap::from([(fire_position, factory)]);

        let resolved = resolve_factory_by_child_from_hints(&children, &hints);

        assert_eq!(resolved.get(&fire_position).copied(), Some(factory));
        assert!(!resolved.contains_key(&unrelated));
    }
}

#[cfg(test)]
mod edict_fork_tests {
    use super::*;
    use bitcoin::{Amount, TxOut, locktime::absolute, opcodes, transaction};
    use protorune_support::balance_sheet::ProtoruneRuneId;

    struct InputPreferredSwapProjector {
        token_in: SchemaAlkaneId,
        token_out: SchemaAlkaneId,
        amount_out: u128,
    }

    impl MempoolContractProjector for InputPreferredSwapProjector {
        fn project(&mut self, _ctx: ContractProjectionContext<'_>) -> Option<ContractProjection> {
            None
        }

        fn project_from_inputs(
            &mut self,
            ctx: ContractProjectionContext<'_>,
        ) -> Option<ContractProjection> {
            if ctx.incoming.get(&self.token_in).copied().unwrap_or_default() == 0 {
                return None;
            }
            Some(ContractProjection { output: BTreeMap::from([(self.token_out, self.amount_out)]) })
        }
    }

    fn tx_with_middle_op_return() -> Transaction {
        Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![],
            output: vec![
                TxOut { value: Amount::ZERO, script_pubkey: ScriptBuf::new() },
                TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::builder()
                        .push_opcode(opcodes::all::OP_RETURN)
                        .into_script(),
                },
                TxOut { value: Amount::ZERO, script_pubkey: ScriptBuf::new() },
            ],
        }
    }

    fn tx_with_leading_op_return(output_count: usize) -> Transaction {
        let mut outputs = Vec::with_capacity(output_count);
        outputs.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::builder().push_opcode(opcodes::all::OP_RETURN).into_script(),
        });
        for _ in 1..output_count {
            outputs.push(TxOut { value: Amount::ZERO, script_pubkey: ScriptBuf::new() });
        }

        Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![],
            output: outputs,
        }
    }

    fn multicast_fixture(height: u64) -> HashMap<u32, Vec<BalanceEntry>> {
        let alkane = SchemaAlkaneId { block: 2, tx: 1 };
        let mut seed = Unallocated::default();
        seed.add(alkane, 500);
        let protostone = Protostone {
            burn: None,
            message: vec![],
            edicts: vec![ProtostoneEdict {
                id: ProtoruneRuneId { block: 2, tx: 1 },
                amount: 300,
                output: 3,
            }],
            refund: None,
            pointer: Some(0),
            from: None,
            protocol_tag: 1,
        };

        apply_transfers_multi(&tx_with_middle_op_return(), &[protostone], &[], height, seed, None)
            .expect("apply transfers")
    }

    fn amount_at(map: &HashMap<u32, Vec<BalanceEntry>>, vout: u32) -> u128 {
        map.get(&vout).into_iter().flatten().map(|entry| entry.amount).sum()
    }

    fn token_amount_at(
        map: &HashMap<u32, Vec<BalanceEntry>>,
        vout: u32,
        token: SchemaAlkaneId,
    ) -> u128 {
        map.get(&vout)
            .into_iter()
            .flatten()
            .filter(|entry| entry.alkane == token)
            .map(|entry| entry.amount)
            .sum()
    }

    #[test]
    fn input_projection_overrides_stale_success_trace_before_edict_routing() {
        let token_in = SchemaAlkaneId { block: 32, tx: 0 };
        let token_out = SchemaAlkaneId { block: 2, tx: 77_269 };
        let stale_trace_token = SchemaAlkaneId { block: 2, tx: 1 };
        let tx = tx_with_leading_op_return(3);
        let shadow_vout = tx.output.len() as u32 + 1;
        let trace_json = format!(
            r#"{{
  "outpoint": "test",
  "events": [
    {{
      "event": "invoke",
      "data": {{
        "type": "call",
        "context": {{
          "myself": {{"block": "0x4", "tx": "0x1"}},
          "caller": {{"block": "0x0", "tx": "0x0"}},
          "inputs": ["0x3"],
          "incomingAlkanes": [
            {{"id": {{"block": "0x20", "tx": "0x0"}}, "value": "0x64"}}
          ],
          "vout": {shadow_vout}
        }},
        "fuel": 1
      }}
    }},
    {{
      "event": "return",
      "data": {{
        "status": "success",
        "response": {{
          "alkanes": [
            {{"id": {{"block": "0x2", "tx": "0x1"}}, "value": "0x1"}}
          ],
          "data": "0x",
          "storage": []
        }}
      }}
    }}
  ]
}}"#
        );
        let sandshrew_trace: EspoSandshrewLikeTrace =
            serde_json::from_str(&trace_json).expect("trace json");
        let trace = EspoTrace {
            sandshrew_trace,
            protobuf_trace: Default::default(),
            storage_changes: Default::default(),
            outpoint: EspoOutpoint { txid: vec![0; 32], vout: shadow_vout, tx_spent: None },
        };
        let protostone = Protostone {
            burn: None,
            message: vec![1],
            edicts: vec![ProtostoneEdict {
                id: ProtoruneRuneId {
                    block: u128::from(token_out.block),
                    tx: u128::from(token_out.tx),
                },
                amount: 80,
                output: 1,
            }],
            refund: None,
            pointer: Some(2),
            from: None,
            protocol_tag: 1,
        };
        let mut seed = Unallocated::default();
        seed.add(token_in, 100);
        let mut projector = InputPreferredSwapProjector { token_in, token_out, amount_out: 90 };

        let allocations = apply_transfers_multi(
            &tx,
            &[protostone],
            &[trace],
            ALKANES_V217_EDICT_FIX_HEIGHT,
            seed,
            Some(&mut projector),
        )
        .expect("projected allocations");

        assert_eq!(token_amount_at(&allocations, 1, token_out), 80);
        assert_eq!(token_amount_at(&allocations, 2, token_out), 10);
        assert_eq!(token_amount_at(&allocations, 1, stale_trace_token), 0);
        assert_eq!(token_amount_at(&allocations, 2, stale_trace_token), 0);
    }

    #[test]
    fn multicast_edicts_match_legacy_before_v217_fix() {
        let allocations = multicast_fixture(ALKANES_V217_EDICT_FIX_HEIGHT - 1);
        assert_eq!(amount_at(&allocations, 0), 300);
        assert_eq!(amount_at(&allocations, 1), 0);
        assert_eq!(amount_at(&allocations, 2), 200);
    }

    #[test]
    fn multicast_edicts_skip_op_return_after_v217_fix() {
        let allocations = multicast_fixture(ALKANES_V217_EDICT_FIX_HEIGHT);
        assert_eq!(amount_at(&allocations, 0), 300);
        assert_eq!(amount_at(&allocations, 1), 0);
        assert_eq!(amount_at(&allocations, 2), 200);
    }

    #[test]
    fn pre_v217_multicast_skips_op_return_before_consuming_amount() {
        let alkane = SchemaAlkaneId { block: 2, tx: 0 };
        let mut seed = Unallocated::default();
        seed.add(alkane, 312_500_000);
        let protostone = Protostone {
            burn: None,
            message: vec![],
            edicts: vec![ProtostoneEdict {
                id: ProtoruneRuneId { block: 2, tx: 0 },
                amount: 312_500_000,
                output: 34,
            }],
            refund: None,
            pointer: Some(31),
            from: None,
            protocol_tag: 1,
        };

        let allocations = apply_transfers_multi(
            &tx_with_leading_op_return(34),
            &[protostone],
            &[],
            ALKANES_V217_EDICT_FIX_HEIGHT - 1,
            seed,
            None,
        )
        .expect("apply transfers");

        assert_eq!(amount_at(&allocations, 1), 312_500_000);
        assert_eq!(amount_at(&allocations, 31), 0);
    }

    #[test]
    fn pre_v217_multicast_leftover_pointer_matches_metashrew_skip_behavior() {
        let alkane = SchemaAlkaneId { block: 2, tx: 16 };
        let mut seed = Unallocated::default();
        seed.add(alkane, 299_500_000_000);
        let protostone = Protostone {
            burn: None,
            message: vec![],
            edicts: vec![ProtostoneEdict {
                id: ProtoruneRuneId { block: 2, tx: 16 },
                amount: 8_000_000_000,
                output: 34,
            }],
            refund: None,
            pointer: Some(31),
            from: None,
            protocol_tag: 1,
        };

        let allocations = apply_transfers_multi(
            &tx_with_leading_op_return(34),
            &[protostone],
            &[],
            ALKANES_V217_EDICT_FIX_HEIGHT - 1,
            seed,
            None,
        )
        .expect("apply transfers");

        assert_eq!(amount_at(&allocations, 31), 43_500_000_000);
    }

    #[test]
    fn pre_v217_multicast_defers_leading_op_return_slot_before_partial_output() {
        let alkane = SchemaAlkaneId { block: 2, tx: 25_720 };
        let mut seed = Unallocated::default();
        seed.add(alkane, 835_695_545_699);
        let protostone = Protostone {
            burn: None,
            message: vec![],
            edicts: vec![ProtostoneEdict {
                id: ProtoruneRuneId { block: 2, tx: 25_720 },
                amount: 50_000_000_000,
                output: 93,
            }],
            refund: None,
            pointer: Some(90),
            from: None,
            protocol_tag: 1,
        };

        let allocations = apply_transfers_multi(
            &tx_with_leading_op_return(93),
            &[protostone],
            &[],
            ALKANES_V217_EDICT_FIX_HEIGHT - 1,
            seed,
            None,
        )
        .expect("apply transfers");

        assert_eq!(amount_at(&allocations, 15), 50_000_000_000);
        assert_eq!(amount_at(&allocations, 16), 35_695_545_699);
        assert_eq!(amount_at(&allocations, 17), 50_000_000_000);
    }
}

/* -------------------------- Holders helpers -------------------------- */

fn holder_order_key(id: &HolderId) -> String {
    match id {
        HolderId::Address(a) => format!("addr:{a}"),
        HolderId::Alkane(id) => format!("alkane:{:010}:{:020}", id.block, id.tx),
    }
}

fn holder_id_index_bytes(holder: &HolderId) -> Vec<u8> {
    match holder {
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
    }
}

fn parse_holder_id_index_bytes(raw: &[u8]) -> Option<HolderId> {
    if raw.is_empty() {
        return None;
    }
    match raw[0] {
        b'a' => std::str::from_utf8(&raw[1..])
            .ok()
            .map(|addr| HolderId::Address(addr.to_string())),
        b'k' if raw.len() == 13 => Some(HolderId::Alkane(SchemaAlkaneId {
            block: u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]),
            tx: u64::from_be_bytes([
                raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12],
            ]),
        })),
        _ => None,
    }
}

fn sort_address_amount_entries(entries: &mut Vec<AddressAmountEntry>) {
    entries.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => a.address.cmp(&b.address),
        o => o,
    });
}

fn sort_address_contract_amount_entries(entries: &mut Vec<AddressContractAmountEntry>) {
    entries.sort_by(|a, b| {
        b.amount
            .cmp(&a.amount)
            .then_with(|| a.contract.cmp(&b.contract))
            .then_with(|| a.alkane.cmp(&b.alkane))
    });
}

fn apply_contract_amount_delta(
    current: Vec<AddressContractAmountEntry>,
    delta: &ContractTokenAmounts,
) -> Vec<AddressContractAmountEntry> {
    let mut amounts: BTreeMap<(SchemaAlkaneId, SchemaAlkaneId), u128> = BTreeMap::new();
    for entry in current {
        if entry.amount == 0 {
            continue;
        }
        let key = (entry.contract, entry.alkane);
        *amounts.entry(key).or_default() =
            amounts.get(&key).copied().unwrap_or(0).saturating_add(entry.amount);
    }
    for ((contract, token), amount) in delta {
        if *amount == 0 {
            continue;
        }
        let key = (*contract, *token);
        *amounts.entry(key).or_default() =
            amounts.get(&key).copied().unwrap_or(0).saturating_add(*amount);
    }
    let mut entries: Vec<AddressContractAmountEntry> = amounts
        .into_iter()
        .filter_map(|((contract, alkane), amount)| {
            if amount == 0 {
                None
            } else {
                Some(AddressContractAmountEntry { contract, alkane, amount })
            }
        })
        .collect();
    sort_address_contract_amount_entries(&mut entries);
    entries
}

fn rollup_contract_amounts_to_orbitals(
    deltas: &AddressContractAmounts,
    factory_by_child: &HashMap<SchemaAlkaneId, SchemaAlkaneId>,
) -> AddressContractAmounts {
    let mut rolled = AddressContractAmounts::new();
    for (address, entries) in deltas {
        for ((child, token), amount) in entries {
            let Some(factory) = factory_by_child.get(child) else {
                continue;
            };
            add_address_contract_amount(&mut rolled, address.clone(), *factory, *token, *amount);
        }
    }
    rolled
}

fn volume_deltas_from_address_amounts(
    deltas: &AddressContractAmounts,
) -> SourceTokenAddressAmounts {
    let mut out: SourceTokenAddressAmounts = HashMap::new();
    for (address, entries) in deltas {
        for ((source, token), amount) in entries {
            if *amount == 0 {
                continue;
            }
            let slot =
                out.entry((*source, *token)).or_default().entry(address.clone()).or_default();
            *slot = slot.saturating_add(*amount);
        }
    }
    out
}

fn add_orbital_child_holder_delta(
    deltas: &mut OrbitalChildHolderDeltas,
    factory: SchemaAlkaneId,
    holder: HolderId,
    child: SchemaAlkaneId,
    delta: SignedU128,
) {
    if delta.is_zero() {
        return;
    }
    let remove_holder = {
        let entry = deltas.entry(factory).or_default().entry(holder.clone()).or_default();
        let slot = entry.entry(child).or_insert_with(SignedU128::zero);
        *slot += delta;
        if slot.is_zero() {
            entry.remove(&child);
        }
        entry.is_empty()
    };
    if remove_holder {
        let remove_factory = if let Some(per_holder) = deltas.get_mut(&factory) {
            per_holder.remove(&holder);
            per_holder.is_empty()
        } else {
            false
        };
        if remove_factory {
            deltas.remove(&factory);
        }
    }
}

fn hydrate_orbital_children_from_holder_index(
    provider: &EssentialsProvider,
    table: &EssentialsTable<'_>,
    factory: &SchemaAlkaneId,
    holder: &HolderId,
) -> Result<BTreeSet<SchemaAlkaneId>> {
    let children = provider
        .get_factory_children(GetFactoryChildrenParams {
            blockhash: StateAt::Latest,
            factory: *factory,
        })?
        .children;
    if children.is_empty() {
        return Ok(BTreeSet::new());
    }

    let keys: Vec<Vec<u8>> = children.iter().map(|child| table.holder_key(child, holder)).collect();
    let values = provider
        .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
        .values;

    let mut held = BTreeSet::new();
    for (child, value) in children.into_iter().zip(values.into_iter()) {
        let amount = value.as_ref().and_then(|bytes| decode_u128_value(bytes).ok()).unwrap_or(0);
        if amount > 0
            || matches!(holder, HolderId::Alkane(id) if *id == child)
                && lookup_self_balance(&child).unwrap_or(0) > 0
        {
            held.insert(child);
        }
    }
    Ok(held)
}

fn add_address_orbital_balance_delta(
    deltas: &mut HashMap<String, BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>>,
    address: &str,
    factory: SchemaAlkaneId,
    child: SchemaAlkaneId,
    delta: SignedU128,
) {
    if delta.is_zero() {
        return;
    }
    let remove_child = {
        let slot = deltas
            .entry(address.to_string())
            .or_default()
            .entry(factory)
            .or_default()
            .entry(child)
            .or_insert_with(SignedU128::zero);
        *slot += delta;
        slot.is_zero()
    };
    if remove_child {
        let remove_address = if let Some(per_factory) = deltas.get_mut(address) {
            if let Some(per_child) = per_factory.get_mut(&factory) {
                per_child.remove(&child);
                if per_child.is_empty() {
                    per_factory.remove(&factory);
                }
            }
            per_factory.is_empty()
        } else {
            false
        };
        if remove_address {
            deltas.remove(address);
        }
    }
}

#[derive(Clone, Copy)]
enum SourceVolumeIndex {
    Alkane,
    Orbital,
}

fn source_volume_list_len_key(
    table: &EssentialsTable<'_>,
    index: SourceVolumeIndex,
    source: &SchemaAlkaneId,
    alkane: &SchemaAlkaneId,
    receive: bool,
) -> Vec<u8> {
    match (index, receive) {
        (SourceVolumeIndex::Alkane, false) => table.alkane_send_volume_list_len_key(source, alkane),
        (SourceVolumeIndex::Alkane, true) => {
            table.alkane_receive_volume_list_len_key(source, alkane)
        }
        (SourceVolumeIndex::Orbital, false) => {
            table.orbital_send_volume_list_len_key(source, alkane)
        }
        (SourceVolumeIndex::Orbital, true) => {
            table.orbital_receive_volume_list_len_key(source, alkane)
        }
    }
}

fn source_volume_list_idx_key(
    table: &EssentialsTable<'_>,
    index: SourceVolumeIndex,
    source: &SchemaAlkaneId,
    alkane: &SchemaAlkaneId,
    idx: u32,
    receive: bool,
) -> Vec<u8> {
    match (index, receive) {
        (SourceVolumeIndex::Alkane, false) => {
            table.alkane_send_volume_list_idx_key(source, alkane, idx)
        }
        (SourceVolumeIndex::Alkane, true) => {
            table.alkane_receive_volume_list_idx_key(source, alkane, idx)
        }
        (SourceVolumeIndex::Orbital, false) => {
            table.orbital_send_volume_list_idx_key(source, alkane, idx)
        }
        (SourceVolumeIndex::Orbital, true) => {
            table.orbital_receive_volume_list_idx_key(source, alkane, idx)
        }
    }
}

fn source_volume_entry_key(
    table: &EssentialsTable<'_>,
    index: SourceVolumeIndex,
    source: &SchemaAlkaneId,
    alkane: &SchemaAlkaneId,
    address: &str,
    receive: bool,
) -> Vec<u8> {
    match (index, receive) {
        (SourceVolumeIndex::Alkane, false) => {
            table.alkane_send_volume_entry_key(source, alkane, address)
        }
        (SourceVolumeIndex::Alkane, true) => {
            table.alkane_receive_volume_entry_key(source, alkane, address)
        }
        (SourceVolumeIndex::Orbital, false) => {
            table.orbital_send_volume_entry_key(source, alkane, address)
        }
        (SourceVolumeIndex::Orbital, true) => {
            table.orbital_receive_volume_entry_key(source, alkane, address)
        }
    }
}

fn apply_source_volume_deltas(
    provider: &EssentialsProvider,
    table: &EssentialsTable<'_>,
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    deltas: &SourceTokenAddressAmounts,
    index: SourceVolumeIndex,
    receive: bool,
) -> Result<()> {
    for ((source, alkane), per_addr) in deltas {
        if per_addr.is_empty() {
            continue;
        }
        let len_key = source_volume_list_len_key(table, index, source, alkane, receive);
        let len = provider
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: len_key.clone() })?
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

        let mut addresses: Vec<String> = per_addr.keys().cloned().collect();
        addresses.sort();
        let entry_keys: Vec<Vec<u8>> = addresses
            .iter()
            .map(|address| source_volume_entry_key(table, index, source, alkane, address, receive))
            .collect();
        let current_values = provider
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: entry_keys.clone(),
            })?
            .values;

        let mut appended: Vec<String> = Vec::new();
        for ((address, key), current_raw) in
            addresses.iter().zip(entry_keys.into_iter()).zip(current_values.into_iter())
        {
            let delta = per_addr.get(address).copied().unwrap_or(0);
            if delta == 0 {
                continue;
            }
            let prev =
                current_raw.as_ref().and_then(|raw| decode_u128_value(raw).ok()).unwrap_or(0);
            let next = prev.saturating_add(delta);
            puts.push((key, encode_u128_value(next)?));
            if prev == 0 && next > 0 && current_raw.is_none() {
                appended.push(address.clone());
            }
        }

        if !appended.is_empty() {
            let base = len;
            for (offset, address) in appended.iter().enumerate() {
                let idx_key = source_volume_list_idx_key(
                    table,
                    index,
                    source,
                    alkane,
                    base.saturating_add(offset as u32),
                    receive,
                );
                puts.push((idx_key, address.as_bytes().to_vec()));
            }
            let new_len = len.saturating_add(appended.len() as u32);
            puts.push((len_key, new_len.to_le_bytes().to_vec()));
        }
    }
    Ok(())
}

fn contracts_in_address_amounts(deltas: &AddressContractAmounts) -> HashSet<SchemaAlkaneId> {
    let mut contracts = HashSet::new();
    for entries in deltas.values() {
        for ((contract, _), amount) in entries {
            if *amount > 0 {
                contracts.insert(*contract);
            }
        }
    }
    contracts
}

fn resolve_factory_by_child_from_hints(
    children: &HashSet<SchemaAlkaneId>,
    factory_hints: &HashMap<SchemaAlkaneId, SchemaAlkaneId>,
) -> HashMap<SchemaAlkaneId, SchemaAlkaneId> {
    let mut out = HashMap::new();
    for child in children {
        if let Some(factory) = factory_hints.get(child) {
            out.insert(*child, *factory);
        }
    }
    out
}

fn resolve_factory_by_child(
    provider: &EssentialsProvider,
    children: HashSet<SchemaAlkaneId>,
    factory_hints: &HashMap<SchemaAlkaneId, SchemaAlkaneId>,
) -> Result<HashMap<SchemaAlkaneId, SchemaAlkaneId>> {
    let mut out = resolve_factory_by_child_from_hints(&children, factory_hints);
    let mut children: Vec<SchemaAlkaneId> = children.into_iter().collect();
    children.sort();
    for child in children {
        if out.contains_key(&child) {
            continue;
        }
        let Some(record) = provider
            .get_creation_record(GetCreationRecordParams {
                blockhash: StateAt::Latest,
                alkane: child,
            })?
            .record
        else {
            continue;
        };
        let Some(factory) = record.inspection.and_then(|inspection| inspection.factory_alkane)
        else {
            continue;
        };
        out.insert(child, factory);
    }
    Ok(out)
}

fn read_address_amount_prefix_page(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    prefix: Vec<u8>,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let entries = provider
        .get_list_entries_desc(GetListEntriesDescParams { blockhash, prefix: prefix.clone() })?
        .entries;
    let mut all = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let Some(address_raw) = key.strip_prefix(prefix.as_slice()) else {
            continue;
        };
        let Ok(address) = std::str::from_utf8(address_raw).map(|s| s.to_string()) else {
            continue;
        };
        let Ok(amount) = decode_u128_value(&value) else {
            continue;
        };
        if amount == 0 {
            continue;
        }
        all.push(AddressAmountEntry { address, amount });
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

/* ===========================================================
Public API
=========================================================== */

#[allow(unused_assignments)]
pub fn bulk_update_balances_for_block(
    provider: &EssentialsProvider,
    block: &EspoBlock,
) -> Result<()> {
    bulk_update_balances_for_block_with_factory_hints(provider, block, &HashMap::new())
}

#[allow(unused_assignments)]
pub fn bulk_update_balances_for_block_with_factory_hints(
    provider: &EssentialsProvider,
    block: &EspoBlock,
    factory_hints: &HashMap<SchemaAlkaneId, SchemaAlkaneId>,
) -> Result<()> {
    crate::debug_timer_log!("bulk_update_balances_for_block.total");
    let debug = debug_enabled();
    let check_negative_balances = strict_check_alkane_balances();
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
    let mut address_contract_send_delta: AddressContractAmounts = HashMap::new();
    let mut address_contract_receive_delta: AddressContractAmounts = HashMap::new();

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

        let lookup_outpoints =
            lookup_pairs_from_outpoints(filtered_external_inputs.iter().copied());

        let lookups =
            get_outpoint_balances_with_spent_batch(StateAt::Latest, provider, &lookup_outpoints)?;
        populate_outpoint_lookup_maps(
            lookup_outpoints,
            lookups,
            &mut balances_by_outpoint,
            &mut addr_by_outpoint,
            &mut spk_by_outpoint,
        );

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
            let prev_key = (prev_txid, input.previous_output.vout);
            if addr_by_outpoint.contains_key(&prev_key) || spk_by_outpoint.contains_key(&prev_key) {
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
        let mut local_alkane_delta: AlkaneDeltaMap = HashMap::new();
        let mut local_root_debit_candidates: RootDebitCandidateMap = HashMap::new();
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
        let mut seed_sources: SourcedSheet = SourcedSheet::new();

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
                        add_sources_to_sheet(
                            &mut seed_sources,
                            be.alkane,
                            source_amount(AttributionSource::Address(addr.clone()), be.amount),
                        );
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
                        add_sources_to_sheet(
                            &mut seed_sources,
                            be.alkane,
                            source_amount(AttributionSource::Address(addr.clone()), be.amount),
                        );
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
                let (ok, deltas, root_debits) = accumulate_alkane_balance_deltas(
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
                for (owner, per_token) in root_debits {
                    let entry = local_root_debit_candidates.entry(owner).or_default();
                    for (token, amount) in per_token {
                        *entry.entry(token).or_default() =
                            entry.get(&token).copied().unwrap_or(0).saturating_add(amount);
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
        // NOTE: a protostone that fails with "all fuel consumed by WebAssembly" only
        // rolls back its own message (protorune checkpoints/rollbacks per protostone
        // and refunds that protostone's inputs). Successful sibling protostones in the
        // same transaction keep their commits — including root response debits — so an
        // out-of-fuel failure in one protostone must NOT cancel debit candidates
        // accumulated from the other traces of the same tx.
        if !local_root_debit_candidates.is_empty() {
            let mut empty_owners: Vec<SchemaAlkaneId> = Vec::new();
            for (owner, per_token_candidates) in &local_root_debit_candidates {
                let Some(per_token_delta) = local_alkane_delta.get_mut(owner) else {
                    continue;
                };
                let mut remove_tokens: Vec<SchemaAlkaneId> = Vec::new();
                for (token, candidate_amount) in per_token_candidates {
                    let Some(delta) = per_token_delta.get(token).copied() else {
                        continue;
                    };
                    let (is_negative, mag) = delta.as_parts();
                    if !is_negative || mag == 0 {
                        continue;
                    }
                    let candidate_amount = (*candidate_amount).min(mag);
                    if candidate_amount == 0 {
                        continue;
                    }

                    let current_raw = provider
                        .get_raw_value(GetRawValueParams {
                            blockhash: StateAt::Latest,
                            key: table.alkane_balance_key(owner, token),
                        })?
                        .value;
                    let mut effective_current = current_raw
                        .as_ref()
                        .and_then(|raw| decode_u128_value(raw).ok())
                        .unwrap_or(0);
                    if let Some(prior_delta) =
                        alkane_balance_delta.get(owner).and_then(|per_token| per_token.get(token))
                    {
                        effective_current = apply_signed_balance_delta_or_panic(
                            effective_current,
                            *prior_delta,
                            false,
                            || String::new(),
                        );
                    }
                    if mag <= effective_current {
                        continue;
                    }

                    let non_candidate = mag.saturating_sub(candidate_amount);
                    if non_candidate > effective_current {
                        continue;
                    }

                    if debug {
                        eprintln!(
                            "[balances] skipped uncovered root response debit: txid={} owner={}:{} token={}:{} current={} requested={} skipped={}",
                            txid,
                            owner.block,
                            owner.tx,
                            token.block,
                            token.tx,
                            effective_current,
                            mag,
                            candidate_amount,
                        );
                    }
                    if non_candidate == 0 {
                        remove_tokens.push(*token);
                    } else {
                        per_token_delta.insert(*token, SignedU128::negative(non_candidate));
                    }
                }
                for token in remove_tokens {
                    per_token_delta.remove(&token);
                }
                if per_token_delta.is_empty() {
                    empty_owners.push(*owner);
                }
            }
            for owner in empty_owners {
                local_alkane_delta.remove(&owner);
            }
        }
        if !tx_mint_deltas.is_empty() {
            for (alkane, delta) in tx_mint_deltas {
                *minted_delta_by_alk.entry(alkane).or_default() =
                    minted_delta_by_alk.get(&alkane).copied().unwrap_or(0).saturating_add(delta);
            }
        }

        let transfer_application = if tx_has_op_return(tx) {
            let protostones = match parse_protostones(tx) {
                Ok(protostones) => protostones,
                Err(e) => {
                    if debug {
                        eprintln!(
                            "[balances] skipping malformed protostones: height={} txid={} err={e:#}",
                            block.height, txid
                        );
                    }
                    Vec::new()
                }
            };
            if protostones.is_empty() {
                TransferApplication::default()
            } else {
                // apply transfers only when there’s a proto/runestone carrier
                apply_transfers_multi_attributed(
                    tx,
                    &protostones,
                    &traces_for_tx,
                    u64::from(block.height),
                    &block.host_function_values,
                    seed_unalloc,
                    seed_sources,
                    None,
                )?
            }
        } else {
            // No OP_RETURN → no Alkanes allocations (but we already did VIN cleanup/holders--)
            TransferApplication::default()
        };
        merge_address_contract_amounts(
            &mut address_contract_send_delta,
            transfer_application.send_contracts,
        );
        let mut receive_contracts_by_vout = transfer_application.receive_contracts_by_vout;
        let allocations = transfer_application.allocations;
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
                if let Some(receives) = receive_contracts_by_vout.remove(&vout_idx) {
                    let addr_receives =
                        address_contract_receive_delta.entry(address_str.clone()).or_default();
                    for ((contract, token), amount) in receives {
                        add_contract_token_amount(addr_receives, contract, token, amount);
                    }
                }
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
                    let (_, mag) = delta.as_parts();
                    if mag == 0 {
                        continue;
                    }
                    let cur = amounts.get(token).copied().unwrap_or(0);
                    let updated = apply_signed_balance_delta_or_panic(
                        cur,
                        *delta,
                        check_negative_balances,
                        || {
                            let txid_str = alkane_balance_delta_src
                                .get(&(*owner, *token))
                                .map(|entry| Txid::from_byte_array(entry.txid))
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            format!(
                                "[balances] negative alkane balance detected (txid={}, owner={}:{}, token={}:{}, cur={}, sub={})",
                                txid_str, owner.block, owner.tx, token.block, token.tx, cur, mag
                            )
                        },
                    );
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
    let mut new_outpoint_spent_updates: HashMap<u64, [u8; 32]> = HashMap::new();
    let mut existing_outpoint_spent_updates: HashMap<u64, [u8; 32]> = HashMap::new();

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
                let mut spender_arr = [0u8; 32];
                spender_arr.copy_from_slice(spent_by);
                if let Some(id) = created_outpoint_ids.get(&key).copied() {
                    new_outpoint_spent_updates.insert(id, spender_arr);
                } else if let Some(id) = external_spent_ids.get(&key).copied() {
                    existing_outpoint_spent_updates.insert(id, spender_arr);
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
            let (_, mag) = delta.as_parts();
            let next = apply_signed_balance_delta_or_panic(
                current,
                *delta,
                check_negative_balances,
                || {
                    format!(
                        "[balances] negative address balance detected (addr={}, token={}:{}, cur={}, sub={})",
                        address, token.block, token.tx, current, mag
                    )
                },
            );
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

    blob_puts.extend(build_new_outpoint_pos_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &outpoint_pos_updates,
    )?);
    blob_puts.extend(build_new_outpoint_spent_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &new_outpoint_spent_updates,
    )?);
    blob_puts.extend(build_outpoint_spent_versioned_puts(
        provider,
        block.height,
        &blockhash,
        &existing_outpoint_spent_updates,
    )?);
    blob_puts.extend(build_new_tx_pos_versioned_puts(
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
    let holders_full_rebuilds = 0usize;
    let holders_full_rebuild_entries = 0usize;
    let holder_child_factories =
        resolve_factory_by_child(provider, holders_delta.keys().copied().collect(), factory_hints)?;
    let mut orbital_holders_delta: OrbitalChildHolderDeltas = HashMap::new();
    for (alkane, per_holder) in holders_delta.iter() {
        let holders_count_key = table.holders_count_key(alkane);
        let prev_count = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: holders_count_key.clone(),
            })?
            .value
            .and_then(|raw| HoldersCountEntry::try_from_slice(&raw).ok())
            .map(|entry| entry.count)
            .unwrap_or(0);
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

        let mut holder_keys: Vec<Vec<u8>> = Vec::with_capacity(per_holder.len());
        let mut holders: Vec<HolderId> = Vec::with_capacity(per_holder.len());
        for holder in per_holder.keys() {
            holder_keys.push(table.holder_key(alkane, holder));
            holders.push(holder.clone());
        }
        let current_holder_values = provider
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: holder_keys.clone(),
            })?
            .values;

        let supply_latest_key = table.circulating_supply_latest_key(alkane);
        let prev_supply = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: supply_latest_key.clone(),
            })?
            .value
            .and_then(|v| decode_u128_value(&v).ok())
            .unwrap_or(0);
        let mut supply = prev_supply;
        let mut changed_holders: Vec<(Vec<u8>, u128)> = Vec::with_capacity(per_holder.len());
        let mut added_count: u64 = 0;
        let mut appended_holders: Vec<HolderId> = Vec::new();
        let mut removed_holders: u32 = 0;
        for ((holder, holder_key), current_raw) in holders
            .into_iter()
            .zip(holder_keys.into_iter())
            .zip(current_holder_values.into_iter())
        {
            let cur = current_raw.as_ref().and_then(|raw| decode_u128_value(raw).ok()).unwrap_or(0);
            let Some(delta) = per_holder.get(&holder) else {
                continue;
            };
            let (_, mag) = delta.as_parts();
            let next = apply_signed_balance_delta_or_panic(
                cur,
                *delta,
                check_negative_balances,
                || {
                    format!(
                        "[balances] negative holder balance detected (alkane={}:{}, holder={:?}, cur={}, sub={})",
                        alkane.block, alkane.tx, holder, cur, mag
                    )
                },
            );
            if (cur > 0) != (next > 0) {
                if cur == 0 && next > 0 {
                    added_count = added_count.saturating_add(1);
                    if current_raw.is_none() {
                        appended_holders.push(holder.clone());
                    }
                    if let Some(factory) = holder_child_factories.get(alkane) {
                        add_orbital_child_holder_delta(
                            &mut orbital_holders_delta,
                            *factory,
                            holder.clone(),
                            *alkane,
                            SignedU128::positive(1),
                        );
                    }
                } else if cur > 0 && next == 0 {
                    removed_holders = removed_holders.saturating_add(1);
                    if let Some(factory) = holder_child_factories.get(alkane) {
                        add_orbital_child_holder_delta(
                            &mut orbital_holders_delta,
                            *factory,
                            holder.clone(),
                            *alkane,
                            SignedU128::negative(1),
                        );
                    }
                }
            }
            if next >= cur {
                supply = supply.saturating_add(next - cur);
            } else {
                supply = supply.saturating_sub(cur - next);
            }
            changed_holders.push((holder_key, next));
        }
        let new_count =
            prev_count.saturating_add(added_count).saturating_sub(removed_holders as u64);
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

        if supply != prev_supply {
            let encoded = encode_u128_value(supply)?;
            puts.push((table.circulating_supply_key(alkane, block.height), encoded.clone()));
            puts.push((supply_latest_key, encoded));
        }

        for (holder_key, amount) in changed_holders {
            puts.push((holder_key, encode_u128_value(amount)?));
        }

        let mut added_holder_keys: Vec<Vec<u8>> = appended_holders
            .into_iter()
            .map(|holder| holder_id_index_bytes(&holder))
            .collect();
        added_holder_keys.sort();
        if !added_holder_keys.is_empty() {
            let base = holder_len;
            for (offset, holder_key_bytes) in added_holder_keys.iter().enumerate() {
                puts.push((
                    table.holder_list_idx_key(alkane, base.saturating_add(offset as u32)),
                    holder_key_bytes.clone(),
                ));
            }
            let new_len = holder_len.saturating_add(added_holder_keys.len() as u32);
            puts.push((table.holder_list_len_key(alkane), new_len.to_le_bytes().to_vec()));
        }

        puts.push((holders_count_key, get_holders_count_encoded(new_count)?));
    }

    let mut address_orbital_balance_deltas: HashMap<
        String,
        BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    > = HashMap::new();
    for (factory, per_holder) in orbital_holders_delta.iter() {
        let holder_len = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.orbital_holder_v2_list_len_key(factory),
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

        let mut holder_keys: Vec<Vec<u8>> = Vec::with_capacity(per_holder.len());
        let mut holders: Vec<HolderId> = Vec::with_capacity(per_holder.len());
        for holder in per_holder.keys() {
            holder_keys.push(table.orbital_holder_v2_key(factory, holder));
            holders.push(holder.clone());
        }
        let current_holder_values = provider
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: holder_keys.clone(),
            })?
            .values;

        let mut changed_holders: Vec<(Vec<u8>, OrbitalHolderEntry)> =
            Vec::with_capacity(per_holder.len());
        let mut appended_holders: Vec<HolderId> = Vec::new();
        for ((holder, holder_key), current_raw) in holders
            .into_iter()
            .zip(holder_keys.into_iter())
            .zip(current_holder_values.into_iter())
        {
            let had_v2_row = current_raw.is_some();
            let current_entry =
                current_raw.as_ref().and_then(|raw| decode_orbital_holder_entry(raw).ok());
            let mut children: BTreeSet<SchemaAlkaneId> = if let Some(entry) = current_entry {
                entry.alkanes.into_iter().collect()
            } else {
                hydrate_orbital_children_from_holder_index(provider, &table, factory, &holder)?
            };
            if !had_v2_row {
                if let HolderId::Address(address) = &holder {
                    for child in &children {
                        add_address_orbital_balance_delta(
                            &mut address_orbital_balance_deltas,
                            address,
                            *factory,
                            *child,
                            SignedU128::positive(1),
                        );
                    }
                }
            }
            let Some(child_deltas) = per_holder.get(&holder) else {
                continue;
            };
            for (child, delta) in child_deltas {
                let (is_negative, mag) = delta.as_parts();
                if mag == 0 {
                    continue;
                }
                if is_negative {
                    children.remove(child);
                } else {
                    children.insert(*child);
                }
                if let HolderId::Address(address) = &holder {
                    add_address_orbital_balance_delta(
                        &mut address_orbital_balance_deltas,
                        address,
                        *factory,
                        *child,
                        *delta,
                    );
                }
            }
            let alkanes: Vec<SchemaAlkaneId> = children.into_iter().collect();
            let next = alkanes.len() as u128;
            if next == 0 && !had_v2_row {
                continue;
            }
            if next > 0 && !had_v2_row {
                appended_holders.push(holder.clone());
            }
            changed_holders
                .push((holder_key, OrbitalHolderEntry { holder, amount: next, alkanes }));
        }

        for (holder_key, entry) in changed_holders {
            puts.push((holder_key, encode_orbital_holder_entry(entry)?));
        }

        let mut added_holder_keys: Vec<Vec<u8>> = appended_holders
            .into_iter()
            .map(|holder| holder_id_index_bytes(&holder))
            .collect();
        added_holder_keys.sort();
        if !added_holder_keys.is_empty() {
            let base = holder_len;
            for (offset, holder_key_bytes) in added_holder_keys.iter().enumerate() {
                puts.push((
                    table.orbital_holder_v2_list_idx_key(
                        factory,
                        base.saturating_add(offset as u32),
                    ),
                    holder_key_bytes.clone(),
                ));
            }
            let new_len = holder_len.saturating_add(added_holder_keys.len() as u32);
            puts.push((
                table.orbital_holder_v2_list_len_key(factory),
                new_len.to_le_bytes().to_vec(),
            ));
        }
    }

    for (address, per_factory_delta) in address_orbital_balance_deltas {
        let key = table.address_orbital_balances_v2_key(&address);
        let current = provider
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: key.clone() })?
            .value
            .and_then(|raw| decode_address_orbital_balance_entries(&raw).ok())
            .unwrap_or_default();
        let mut by_factory: BTreeMap<SchemaAlkaneId, BTreeSet<SchemaAlkaneId>> = current
            .into_iter()
            .map(|entry| (entry.factory, entry.alkanes.into_iter().collect()))
            .collect();

        for (factory, per_child_delta) in per_factory_delta {
            let children = by_factory.entry(factory).or_default();
            for (child, delta) in per_child_delta {
                let (is_negative, mag) = delta.as_parts();
                if mag == 0 {
                    continue;
                }
                if is_negative {
                    children.remove(&child);
                } else {
                    children.insert(child);
                }
            }
            if children.is_empty() {
                by_factory.remove(&factory);
            }
        }

        let entries: Vec<AddressOrbitalBalanceEntry> = by_factory
            .into_iter()
            .map(|(factory, children)| {
                let alkanes: Vec<SchemaAlkaneId> = children.into_iter().collect();
                AddressOrbitalBalanceEntry { factory, amount: alkanes.len() as u128, alkanes }
            })
            .collect();
        puts.push((key, encode_address_orbital_balance_entries(entries)?));
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

    if !address_contract_send_delta.is_empty() || !address_contract_receive_delta.is_empty() {
        let mut child_contracts = contracts_in_address_amounts(&address_contract_send_delta);
        child_contracts.extend(contracts_in_address_amounts(&address_contract_receive_delta));
        let factory_by_child = resolve_factory_by_child(provider, child_contracts, factory_hints)?;
        let address_orbital_send_delta =
            rollup_contract_amounts_to_orbitals(&address_contract_send_delta, &factory_by_child);
        let address_orbital_receive_delta =
            rollup_contract_amounts_to_orbitals(&address_contract_receive_delta, &factory_by_child);
        let alkane_send_volume_delta =
            volume_deltas_from_address_amounts(&address_contract_send_delta);
        let alkane_receive_volume_delta =
            volume_deltas_from_address_amounts(&address_contract_receive_delta);
        let orbital_send_volume_delta =
            volume_deltas_from_address_amounts(&address_orbital_send_delta);
        let orbital_receive_volume_delta =
            volume_deltas_from_address_amounts(&address_orbital_receive_delta);

        let mut addresses: HashSet<String> = HashSet::new();
        addresses.extend(address_contract_send_delta.keys().cloned());
        addresses.extend(address_contract_receive_delta.keys().cloned());
        addresses.extend(address_orbital_send_delta.keys().cloned());
        addresses.extend(address_orbital_receive_delta.keys().cloned());
        let mut addresses: Vec<String> = addresses.into_iter().collect();
        addresses.sort();

        for addr in &addresses {
            if let Some(delta) = address_contract_send_delta.get(addr) {
                let key = table.address_cumulative_send_alkanes_key(addr);
                let current = provider
                    .get_raw_value(GetRawValueParams {
                        blockhash: StateAt::Latest,
                        key: key.clone(),
                    })?
                    .value
                    .and_then(|bytes| decode_address_contract_amount_entries(&bytes).ok())
                    .unwrap_or_default();
                let entries = apply_contract_amount_delta(current, delta);
                puts.push((key, encode_address_contract_amount_entries(&entries)?));
            }

            if let Some(delta) = address_contract_receive_delta.get(addr) {
                let key = table.address_cumulative_receive_alkanes_key(addr);
                let current = provider
                    .get_raw_value(GetRawValueParams {
                        blockhash: StateAt::Latest,
                        key: key.clone(),
                    })?
                    .value
                    .and_then(|bytes| decode_address_contract_amount_entries(&bytes).ok())
                    .unwrap_or_default();
                let entries = apply_contract_amount_delta(current, delta);
                puts.push((key, encode_address_contract_amount_entries(&entries)?));
            }

            if let Some(delta) = address_orbital_send_delta.get(addr) {
                let key = table.address_cumulative_send_orbitals_key(addr);
                let current = provider
                    .get_raw_value(GetRawValueParams {
                        blockhash: StateAt::Latest,
                        key: key.clone(),
                    })?
                    .value
                    .and_then(|bytes| decode_address_contract_amount_entries(&bytes).ok())
                    .unwrap_or_default();
                let entries = apply_contract_amount_delta(current, delta);
                puts.push((key, encode_address_contract_amount_entries(&entries)?));
            }

            if let Some(delta) = address_orbital_receive_delta.get(addr) {
                let key = table.address_cumulative_receive_orbitals_key(addr);
                let current = provider
                    .get_raw_value(GetRawValueParams {
                        blockhash: StateAt::Latest,
                        key: key.clone(),
                    })?
                    .value
                    .and_then(|bytes| decode_address_contract_amount_entries(&bytes).ok())
                    .unwrap_or_default();
                let entries = apply_contract_amount_delta(current, delta);
                puts.push((key, encode_address_contract_amount_entries(&entries)?));
            }
        }

        apply_source_volume_deltas(
            provider,
            &table,
            &mut puts,
            &alkane_send_volume_delta,
            SourceVolumeIndex::Alkane,
            false,
        )?;
        apply_source_volume_deltas(
            provider,
            &table,
            &mut puts,
            &alkane_receive_volume_delta,
            SourceVolumeIndex::Alkane,
            true,
        )?;
        apply_source_volume_deltas(
            provider,
            &table,
            &mut puts,
            &orbital_send_volume_delta,
            SourceVolumeIndex::Orbital,
            false,
        )?;
        apply_source_volume_deltas(
            provider,
            &table,
            &mut puts,
            &orbital_receive_volume_delta,
            SourceVolumeIndex::Orbital,
            true,
        )?;
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
    let check_balances = check_negative_balances;
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
                    "[balances][strict] metashrew mismatch at height {} (alkanes={:#?} utxos={})",
                    height_u64,
                    balance_mismatches,
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
    // Remote-explorer deployments have no local metashrew; ask the remote
    // espo's equivalent RPC for the self-balance instead.
    if crate::config::try_get_metashrew_sdb().is_none() {
        let client = crate::config::get_explorer_remote_mdb_client()?;
        let id = format!("{}:{}", alk.block, alk.tx);
        return match client.call(
            "essentials.get_alkane_balance_metashrew",
            serde_json::json!({ "owner": id, "alkane": id }),
        ) {
            Ok(result) => result
                .get("balance")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u128>().ok()),
            Err(e) => {
                eprintln!(
                    "[balances] WARN: remote self-balance lookup failed for {}:{} ({e})",
                    alk.block, alk.tx
                );
                None
            }
        };
    }
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

fn lookup_pairs_from_outpoints<'a, I>(outpoints: I) -> Vec<(Txid, u32)>
where
    I: IntoIterator<Item = &'a EspoOutpoint>,
{
    let mut lookup_outpoints = Vec::new();
    for op in outpoints {
        if op.txid.len() != 32 {
            continue;
        }
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&op.txid);
        lookup_outpoints.push((Txid::from_byte_array(txid_arr), op.vout));
    }
    lookup_outpoints
}

fn populate_outpoint_lookup_maps(
    lookup_outpoints: Vec<(Txid, u32)>,
    mut lookups: HashMap<(Txid, u32), OutpointLookup>,
    balances_by_outpoint: &mut HashMap<(Txid, u32), Vec<BalanceEntry>>,
    addr_by_outpoint: &mut HashMap<(Txid, u32), String>,
    spk_by_outpoint: &mut HashMap<(Txid, u32), ScriptBuf>,
) -> usize {
    let mut hits = 0usize;
    for (txid, vout) in lookup_outpoints {
        let key = (txid, vout);
        let Some(lookup) = lookups.remove(&key) else {
            continue;
        };
        if lookup.spent_by.is_some() {
            continue;
        }

        let mut inserted = false;
        if !lookup.balances.is_empty() {
            balances_by_outpoint.insert(key, lookup.balances);
            inserted = true;
        }
        if let Some(addr) = lookup.address {
            if !addr.is_empty() {
                addr_by_outpoint.insert(key, addr);
                inserted = true;
            }
        }
        if let Some(spk) = lookup.spk {
            if !spk.is_empty() {
                spk_by_outpoint.insert(key, spk);
                inserted = true;
            }
        }
        if inserted {
            hits = hits.saturating_add(1);
        }
    }
    hits
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

pub fn get_orbital_holders_for_factory(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    factory: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize /*total*/, u128 /*counted children*/, Vec<OrbitalHolderEntry>)> {
    let table = provider.table();
    let len = provider
        .get_raw_value(GetRawValueParams {
            blockhash,
            key: table.orbital_holder_v2_list_len_key(&factory),
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

    let mut all: Vec<OrbitalHolderEntry> = Vec::new();
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys.push(table.orbital_holder_v2_list_idx_key(&factory, idx));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut holders = Vec::new();
        let mut holder_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            let Some(holder) = parse_holder_id_index_bytes(&raw) else { continue };
            holder_keys.push(table.orbital_holder_v2_key(&factory, &holder));
            holders.push(holder);
        }

        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: holder_keys })?
            .values;
        for (holder, value) in holders.into_iter().zip(vals.into_iter()) {
            let Some(bytes) = value else { continue };
            let Ok(mut entry) = decode_orbital_holder_entry(&bytes) else {
                continue;
            };
            if entry.amount == 0 {
                continue;
            }
            entry.holder = holder;
            all.push(entry);
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

fn get_source_volume(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    source: SchemaAlkaneId,
    alkane: SchemaAlkaneId,
    index: SourceVolumeIndex,
    receive: bool,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let table = provider.table();
    let len_key = source_volume_list_len_key(&table, index, &source, &alkane, receive);
    let len = provider
        .get_raw_value(GetRawValueParams { blockhash, key: len_key })?
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

    let mut all: Vec<AddressAmountEntry> = Vec::new();
    if len > 0 {
        let mut idx_keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            idx_keys
                .push(source_volume_list_idx_key(&table, index, &source, &alkane, idx, receive));
        }
        let idx_vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: idx_keys })?
            .values;
        let mut addresses = Vec::new();
        let mut amount_keys = Vec::new();
        for idx_val in idx_vals {
            let Some(raw) = idx_val else { continue };
            let Ok(address) = std::str::from_utf8(&raw).map(|s| s.to_string()) else {
                continue;
            };
            amount_keys
                .push(source_volume_entry_key(&table, index, &source, &alkane, &address, receive));
            addresses.push(address);
        }

        let vals = provider
            .get_multi_values(GetMultiValuesParams { blockhash, keys: amount_keys })?
            .values;
        for (address, value) in addresses.into_iter().zip(vals.into_iter()) {
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

pub fn get_orbital_volume_for_factory(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    factory: SchemaAlkaneId,
    alkane: SchemaAlkaneId,
    receive: bool,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    get_source_volume(
        blockhash,
        provider,
        factory,
        alkane,
        SourceVolumeIndex::Orbital,
        receive,
        page,
        limit,
    )
}

pub fn get_alkane_volume_for_source(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    source: SchemaAlkaneId,
    alkane: SchemaAlkaneId,
    receive: bool,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    get_source_volume(
        blockhash,
        provider,
        source,
        alkane,
        SourceVolumeIndex::Alkane,
        receive,
        page,
        limit,
    )
}

pub fn get_transfer_volume_for_alkane(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let table = provider.table();
    read_address_amount_prefix_page(
        blockhash,
        provider,
        table.transfer_volume_prefix(&alk),
        page,
        limit,
    )
}

pub fn get_total_received_for_alkane(
    blockhash: StateAt,
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize, Vec<AddressAmountEntry>)> {
    let table = provider.table();
    read_address_amount_prefix_page(
        blockhash,
        provider,
        table.total_received_prefix(&alk),
        page,
        limit,
    )
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
