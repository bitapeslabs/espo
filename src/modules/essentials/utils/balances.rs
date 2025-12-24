// src/modules/essentials/utils/balances.rs

use super::super::storage::{
    BalanceEntry, HolderEntry, HolderId, addr_spk_key, alkane_balance_txs_by_token_key,
    alkane_balance_txs_key, alkane_balances_key, balances_key, decode_balances_vec,
    decode_holders_vec, encode_vec, holders_count_key, holders_key, mk_outpoint, outpoint_addr_key,
    outpoint_balances_key, outpoint_balances_prefix, spk_to_address_str, utxo_spk_key,
};
use crate::config::{get_metashrew, get_network};
use crate::modules::essentials::storage::get_holders_values_encoded;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Result, anyhow};
use bitcoin::{ScriptBuf, Transaction, Txid, hashes::Hash};
use borsh::BorshDeserialize;
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::{Protostone, ProtostoneEdict};
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

// Updated trace types
use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceReturnData, EspoSandshrewLikeTraceStatus, EspoTrace,
};

#[inline]
fn tx_has_op_return(tx: &Transaction) -> bool {
    tx.output.iter().any(|o| is_op_return(&o.script_pubkey))
}

fn parse_protostones(tx: &Transaction) -> Result<Vec<Protostone>> {
    let runestone = match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => r,
        _ => return Ok(vec![]),
    };
    let protos = Protostone::from_runestone(&runestone)
        .map_err(|e| anyhow!("failed to parse protostones: {e}"))?;
    Ok(protos)
}

#[derive(Default, Clone)]
struct Unallocated {
    map: HashMap<SchemaAlkaneId, u128>,
}
impl Unallocated {
    fn add(&mut self, id: SchemaAlkaneId, amt: u128) {
        *self.map.entry(id).or_default() =
            self.map.get(&id).copied().unwrap_or(0).saturating_add(amt);
    }
    #[allow(dead_code)]
    fn get(&self, id: &SchemaAlkaneId) -> u128 {
        self.map.get(id).copied().unwrap_or(0)
    }

    #[allow(dead_code)]
    fn take(&mut self, id: &SchemaAlkaneId, amt: u128) -> u128 {
        let cur = self.get(id);
        let take = cur.min(amt);
        if take == cur {
            self.map.remove(id);
        } else if let Some(e) = self.map.get_mut(id) {
            *e = cur - take;
        }
        take
    }
    fn drain_all(&mut self) -> BTreeMap<SchemaAlkaneId, u128> {
        let mut merged: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
        for (rid, amt) in self.map.drain() {
            if amt == 0 {
                continue;
            }
            *merged.entry(rid).or_default() =
                merged.get(&rid).copied().unwrap_or(0).saturating_add(amt);
        }
        merged
    }
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.map.values().all(|&v| v == 0)
    }
}

impl fmt::Display for Unallocated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render entries sorted by SchemaAlkaneId for stable output.
        // Format: {block:tx=amount, block:tx=amount, ...}
        let mut items: Vec<_> = self.map.iter().filter(|&(_, &amt)| amt != 0).collect();

        items.sort_by(|(a, _), (b, _)| a.cmp(b));

        write!(f, "{{")?;
        let mut first = true;
        for (id, amt) in items {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{}={}", id, amt)?;
        }
        write!(f, "}}")
    }
}

fn is_op_return(spk: &ScriptBuf) -> bool {
    let b = spk.as_bytes();
    !b.is_empty() && b[0] == bitcoin::opcodes::all::OP_RETURN.to_u8()
}

fn u128_to_u32(v: u128) -> Result<u32> {
    u32::try_from(v).map_err(|_| anyhow!("downcast failed: {v} does not fit into u32"))
}
fn u128_to_u64(v: u128) -> Result<u64> {
    u64::try_from(v).map_err(|_| anyhow!("downcast failed: {v} does not fit into u64"))
}
fn schema_id_from_parts(block_u128: u128, tx_u128: u128) -> Result<SchemaAlkaneId> {
    Ok(SchemaAlkaneId { block: u128_to_u32(block_u128)?, tx: u128_to_u64(tx_u128)? })
}

// tolerant hex parsers
fn parse_hex_u32(s: &str) -> Option<u32> {
    let x = s.strip_prefix("0x").unwrap_or(s);
    u32::from_str_radix(x, 16).ok()
}
fn parse_hex_u64(s: &str) -> Option<u64> {
    let x = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(x, 16).ok()
}
fn parse_hex_u128(s: &str) -> Option<u128> {
    let x = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(x, 16).ok()
}

fn i128_abs_u(v: i128) -> u128 {
    let abs = v.wrapping_abs();
    u128::try_from(abs)
        .unwrap_or_else(|_| panic!("[balances] overflow computing abs for i128 value {v}"))
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EspoTraceType {
    NOTRACE,
    REVERT,
    SUCCESS,
}

//// Return (netin, netout):
/// - netin  = first Invoke's context.incoming_alkanes (always present if a trace exists, per your note)
/// - netout = last Return's response.alkanes (no status filtering)
fn compute_nets(
    trace: &EspoSandshrewLikeTrace,
) -> (
    Option<BTreeMap<SchemaAlkaneId, u128>>, // netin: first Invoke.incoming_alkanes
    Option<BTreeMap<SchemaAlkaneId, u128>>, // netout: last Return.response.alkanes
    EspoTraceType,                          // NO_TRACE | REVERT | SUCCESS
) {
    // ---- netin: from the FIRST Invoke ----
    let mut netin: Option<BTreeMap<SchemaAlkaneId, u128>> = None;
    for ev in &trace.events {
        if let EspoSandshrewLikeTraceEvent::Invoke(inv) = ev {
            let mut m = BTreeMap::new();
            for t in &inv.context.incoming_alkanes {
                if let (Some(blk), Some(tx), Some(val)) =
                    (parse_hex_u32(&t.id.block), parse_hex_u64(&t.id.tx), parse_hex_u128(&t.value))
                {
                    let k = SchemaAlkaneId { block: blk, tx };
                    *m.entry(k).or_default() =
                        m.get(&k).copied().unwrap_or(0u128).saturating_add(val);
                }
            }
            netin = Some(m);
            break; // only the first Invoke matters
        }
    }

    // ---- last Return (for netout + status) ----
    let mut last_ret: Option<&EspoSandshrewLikeTraceReturnData> = None;
    for ev in &trace.events {
        if let EspoSandshrewLikeTraceEvent::Return(r) = ev {
            last_ret = Some(r);
        }
    }

    let (netout, status): (Option<BTreeMap<SchemaAlkaneId, u128>>, EspoTraceType) = match last_ret {
        None => (None, EspoTraceType::NOTRACE),
        Some(r) => {
            // Build netout from response.alkanes as-is
            let mut m = BTreeMap::new();
            for t in &r.response.alkanes {
                if let (Some(blk), Some(tx), Some(val)) =
                    (parse_hex_u32(&t.id.block), parse_hex_u64(&t.id.tx), parse_hex_u128(&t.value))
                {
                    let k = SchemaAlkaneId { block: blk, tx };
                    *m.entry(k).or_default() =
                        m.get(&k).copied().unwrap_or(0u128).saturating_add(val);
                }
            }
            let cls = match r.status {
                EspoSandshrewLikeTraceStatus::Failure => EspoTraceType::REVERT,
                EspoSandshrewLikeTraceStatus::Success => EspoTraceType::SUCCESS,
            };
            (Some(m), cls)
        }
    };

    (netin, netout, status)
}

#[derive(Default, Clone)]
struct TraceFrame {
    owner: SchemaAlkaneId,
    incoming: BTreeMap<SchemaAlkaneId, u128>,
    ignore_balances: bool,
}

fn parse_short_id(
    id: &crate::alkanes::trace::EspoSandshrewLikeTraceShortId,
) -> Option<SchemaAlkaneId> {
    let block = parse_hex_u32(&id.block)?;
    let tx = parse_hex_u64(&id.tx)?;
    Some(SchemaAlkaneId { block, tx })
}

fn transfers_to_sheet(
    transfers: &[crate::alkanes::trace::EspoSandshrewLikeTraceTransfer],
) -> BTreeMap<SchemaAlkaneId, u128> {
    let mut m = BTreeMap::new();
    for t in transfers {
        if let (Some(block), Some(tx), Some(val)) =
            (parse_hex_u32(&t.id.block), parse_hex_u64(&t.id.tx), parse_hex_u128(&t.value))
        {
            let k = SchemaAlkaneId { block, tx };
            *m.entry(k).or_default() = m.get(&k).copied().unwrap_or(0u128).saturating_add(val);
        }
    }
    m
}

fn accumulate_alkane_balance_deltas(
    trace: &EspoSandshrewLikeTrace,
    txid: &Txid,
) -> (bool, HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, i128>>) {
    let mut stack: Vec<TraceFrame> = Vec::new();
    let mut root_reverted = false;
    let mut local: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, i128>> = HashMap::new();

    let mut idx = 0;
    while idx < trace.events.len() {
        let ev = &trace.events[idx];
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                let typ = inv.typ.to_ascii_lowercase();
                let ignore_balances = typ == "staticcall" || typ == "delegatecall";

                let owner = if typ == "delegatecall" {
                    stack.last().map(|f| f.owner).or_else(|| parse_short_id(&inv.context.myself))
                } else {
                    parse_short_id(&inv.context.myself)
                };
                let Some(owner) = owner else { continue };

                let incoming = if ignore_balances {
                    BTreeMap::new()
                } else {
                    transfers_to_sheet(&inv.context.incoming_alkanes)
                };

                // Debit nearest balance-carrying ancestor (skip delegate/static frames)
                if !ignore_balances && !incoming.is_empty() {
                    if let Some(parent) = stack.iter_mut().rev().find(|frame| !frame.ignore_balances) {
                        for (rid, amt) in &incoming {
                            if *rid == parent.owner {
                                // self-token tracked via metashrew; do not debit here
                                continue;
                            }
                            let entry = parent.incoming.entry(*rid).or_default();
                            if *entry < *amt {
                                panic!(
                                    "[balances] parent underflow debiting child incoming (txid={}, parent={}, token={}, have={}, need={})",
                                    txid, parent.owner, rid, *entry, *amt
                                );
                            }
                            *entry = entry.saturating_sub(*amt);
                        }
                    }
                }

                stack.push(TraceFrame { owner, incoming, ignore_balances });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                // If we are at the root frame and there are additional consecutive
                // Return events, defer processing until the last one in that run.
                if stack.len() == 1 {
                    if let Some(next_ev) = trace.events.get(idx + 1) {
                        if matches!(next_ev, EspoSandshrewLikeTraceEvent::Return(_)) {
                            idx += 1;
                            continue;
                        }
                    }
                }

                let Some(frame) = stack.pop() else {
                    idx += 1;
                    continue;
                };

                // Failure: cancel frame, treat as if child never happened.
                if ret.status == EspoSandshrewLikeTraceStatus::Failure {
                    if stack.is_empty() {
                        root_reverted = true;
                    } else if !frame.incoming.is_empty() {
                        if let Some(parent) =
                            stack.iter_mut().rev().find(|fr| !fr.ignore_balances)
                        {
                            for (rid, amt) in frame.incoming {
                                *parent.incoming.entry(rid).or_default() =
                                    parent.incoming.get(&rid).copied().unwrap_or(0).saturating_add(amt);
                            }
                        } else {
                            panic!(
                                "[balances] no balance-carrying ancestor to restore child incoming on revert (txid={}, child_owner={})",
                                txid, frame.owner
                            );
                        }
                    }
                    continue;
                }

                if frame.ignore_balances {
                    // maintain stack alignment, but do not account value transfer
                    continue;
                }

                let outgoing = transfers_to_sheet(&ret.response.alkanes);

                // kept = incoming - outgoing (signed)
                let mut keys: BTreeMap<SchemaAlkaneId, ()> = BTreeMap::new();
                for k in frame.incoming.keys() {
                    keys.insert(*k, ());
                }
                for k in outgoing.keys() {
                    keys.insert(*k, ());
                }

                let mut tmp: BTreeMap<SchemaAlkaneId, i128> = BTreeMap::new();
                for rid in keys.keys() {
                    if *rid == frame.owner {
                        // self-mint/burn is derived from live adapter; skip indexing to avoid false negatives
                        continue;
                    }
                    let in_amt = frame.incoming.get(rid).copied().unwrap_or(0);
                    let out_amt = outgoing.get(rid).copied().unwrap_or(0);
                    let kept: i128 = (in_amt.saturating_sub(out_amt)) as i128;
                    // minted = out_amt.saturating_sub(in_amt) (informational only; not applied)
                    if kept > 0 {
                        *tmp.entry(*rid).or_default() += kept;
                    }
                }

                if !tmp.is_empty() {
                    let entry = local.entry(frame.owner).or_default();
                    for (rid, kept) in tmp {
                        *entry.entry(rid).or_default() += kept;
                    }
                }

                if let Some(parent) = stack.iter_mut().rev().find(|fr| !fr.ignore_balances) {
                    for (rid, amt) in outgoing {
                        if amt == 0 {
                            continue;
                        }
                        *parent.incoming.entry(rid).or_default() =
                            parent.incoming.get(&rid).copied().unwrap_or(0).saturating_add(amt);
                    }
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if root_reverted { (false, HashMap::new()) } else { (true, local) }
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

        // sheet starts as: net_out (from trace), plus explicitly routed incoming to this shadow.
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

fn apply_holders_delta(
    mut holders: Vec<HolderEntry>,
    holder: &HolderId,
    delta: i128,
) -> Vec<HolderEntry> {
    let idx = holders.iter().position(|h| h.holder == *holder);
    match delta.cmp(&0) {
        std::cmp::Ordering::Equal => return holders,
        std::cmp::Ordering::Greater => {
            let add = delta as u128;
            if let Some(i) = idx {
                holders[i].amount = holders[i].amount.saturating_add(add);
            } else {
                holders.push(HolderEntry { holder: holder.clone(), amount: add });
            }
        }
        std::cmp::Ordering::Less => {
            let sub = i128_abs_u(delta);
            if let Some(i) = idx {
                let cur = holders[i].amount;
                if sub > cur {
                    panic!(
                        "[balances] negative holder balance detected (holder={:?}, cur={}, sub={})",
                        holders[i].holder, cur, sub
                    );
                }
                let after = cur - sub;
                if after == 0 {
                    holders.swap_remove(i);
                } else {
                    holders[i].amount = after;
                }
            }
        }
    }
    holders.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => holder_order_key(&a.holder).cmp(&holder_order_key(&b.holder)),
        o => o,
    });
    holders
}

/* ===========================================================
Public API
=========================================================== */

#[allow(unused_assignments)]
pub fn bulk_update_balances_for_block(mdb: &Mdb, block: &EspoBlock) -> Result<()> {
    let network = get_network();

    eprintln!("[balances] >>> begin block #{} (txs={})", block.height, block.transactions.len());

    // --------- stats ----------
    let mut stat_outpoints_marked_spent: usize = 0;
    let mut stat_outpoints_written: usize = 0;
    let mut stat_minus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut stat_plus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

    let push_balance_txid =
        |map: &mut HashMap<SchemaAlkaneId, Vec<[u8; 32]>>, alk: SchemaAlkaneId, txid: [u8; 32]| {
            let entry = map.entry(alk).or_default();
            if !entry.iter().any(|b| b == &txid) {
                entry.push(txid);
            }
        };
    let push_balance_txid_pair = |map: &mut HashMap<(SchemaAlkaneId, SchemaAlkaneId), Vec<[u8; 32]>>,
                                  owner: SchemaAlkaneId,
                                  token: SchemaAlkaneId,
                                  txid: [u8; 32]| {
        let entry = map.entry((owner, token)).or_default();
        if !entry.iter().any(|b| b == &txid) {
            entry.push(txid);
        }
    };

    // holders_delta[alk][addr] = i128 delta
    let mut holders_delta: HashMap<SchemaAlkaneId, BTreeMap<HolderId, i128>> = HashMap::new();
    let mut alkane_balance_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, i128>> =
        HashMap::new();
    let mut alkane_balance_txids: HashMap<SchemaAlkaneId, Vec<[u8; 32]>> = HashMap::new();
    let mut alkane_balance_txids_by_token: HashMap<(SchemaAlkaneId, SchemaAlkaneId), Vec<[u8; 32]>> =
        HashMap::new();

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
    let mut block_created_outs: HashSet<String> = HashSet::new();
    for atx in &block.transactions {
        let tx = &atx.transaction;
        if !tx_has_op_return(tx) {
            continue; // no OP_RETURN → no Alkanes activity on its outputs
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

    // ---------- Pass B: fetch external inputs (batch read) ----------
    let mut balances_by_outpoint: HashMap<(Vec<u8>, u32), Vec<BalanceEntry>> = HashMap::new();
    let mut addr_by_outpoint: HashMap<(Vec<u8>, u32), String> = HashMap::new();
    let mut spk_by_outpoint: HashMap<(Vec<u8>, u32), ScriptBuf> = HashMap::new();

    if !external_inputs_vec.is_empty() {
        let mut k_balances: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());
        let mut k_addr: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());
        let mut k_spk: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());

        for op in &external_inputs_vec {
            k_balances.push(outpoint_balances_key(op)?);
            k_addr.push(outpoint_addr_key(op)?);
            k_spk.push(utxo_spk_key(op)?);
        }

        let v_balances = mdb.multi_get(&k_balances)?;
        let v_addr = mdb.multi_get(&k_addr)?;
        let v_spk = mdb.multi_get(&k_spk)?;

        for (i, op) in external_inputs_vec.iter().enumerate() {
            let key = (op.txid.clone(), op.vout);

            if let Some(bytes) = &v_balances[i] {
                if let Ok(bals) = decode_balances_vec(bytes) {
                    if !bals.is_empty() {
                        balances_by_outpoint.insert(key.clone(), bals);
                    }
                }
            }
            if let Some(addr_bytes) = &v_addr[i] {
                if let Ok(s) = std::str::from_utf8(addr_bytes) {
                    addr_by_outpoint.insert(key.clone(), s.to_string());
                }
            }
            if let Some(spk_bytes) = &v_spk[i] {
                spk_by_outpoint.insert(key, ScriptBuf::from(spk_bytes.clone()));
            }
        }
    }

    // ---------- Main per-tx loop ----------
    for atx in &block.transactions {
        let tx = &atx.transaction;
        let txid = tx.compute_txid();
        let txid_bytes = txid.to_byte_array();
        let mut holder_alkanes_changed: HashSet<SchemaAlkaneId> = HashSet::new();
        let mut local_alkane_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, i128>> =
            HashMap::new();

        let mut add_holder_delta =
            |alk: SchemaAlkaneId,
             holder: HolderId,
             delta: i128,
             holder_changed: &mut HashSet<SchemaAlkaneId>| {
                if delta == 0 {
                    return;
                }
                if let HolderId::Alkane(a) = holder {
                    holder_changed.insert(a);
                }
                holders_delta
                    .entry(alk)
                    .or_default()
                    .entry(holder)
                    .and_modify(|d| *d += delta)
                    .or_insert(delta);
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
            let in_key = (in_op.txid.clone(), in_op.vout);
            let in_str = in_op.as_outpoint_string();

            // 1) Ephemeral? (created earlier in this same block)
            if let Some(bals) = ephem_outpoint_balances.get(&in_str) {
                consumed_ephem_outpoints.insert(in_str.clone(), txid.as_byte_array().to_vec());

                if let Some(addr) = ephem_outpoint_addr.get(&in_str) {
                    for be in bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            -(be.amount as i128),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
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
                // resolve address: /outpoint_addr first, else /utxo_spk → address
                let mut resolved_addr = addr_by_outpoint.get(&in_key).cloned();
                if resolved_addr.is_none() {
                    if let Some(spk) = spk_by_outpoint.get(&in_key) {
                        resolved_addr = spk_to_address_str(spk, network);
                    }
                }

                if let Some(ref addr) = resolved_addr {
                    // holders-- and mark legacy addr-row delete
                    for be in &bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            -(be.amount as i128),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
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
                let (ok, deltas) = accumulate_alkane_balance_deltas(&t.sandshrew_trace, &txid);
                if !ok {
                    local_alkane_delta.clear();
                    holder_alkanes_changed.clear();
                    break;
                }
                for (owner, per_token) in deltas {
                    let entry = local_alkane_delta.entry(owner).or_default();
                    for (tok, delta) in per_token {
                        *entry.entry(tok).or_default() += delta;
                    }
                }
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
                    add_holder_delta(
                        alkane_id,
                        HolderId::Address(address_str.clone()),
                        delta_amount as i128,
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
        for (holder_alk, per_token) in &local_alkane_delta {
            let entry = alkane_balance_delta.entry(*holder_alk).or_default();
            for (token, delta) in per_token {
                if *delta == 0 {
                    continue;
                }
                add_holder_delta(
                    *token,
                    HolderId::Alkane(*holder_alk),
                    *delta,
                    &mut holder_alkanes_changed,
                );
                push_balance_txid_pair(
                    &mut alkane_balance_txids_by_token,
                    *holder_alk,
                    *token,
                    txid_bytes,
                );
                if *delta > 0 {
                    *stat_plus_by_alk.entry(*token).or_default() = stat_plus_by_alk
                        .get(token)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(*delta as u128);
                } else {
                    let amt = i128_abs_u(*delta);
                    *stat_minus_by_alk.entry(*token).or_default() =
                        stat_minus_by_alk.get(token).copied().unwrap_or(0).saturating_add(amt);
                }
                *entry.entry(*token).or_default() += *delta;
            }
        }

        for owner in &holder_alkanes_changed {
            push_balance_txid(&mut alkane_balance_txids, *owner, txid_bytes);
        }
    }

    // Accumulate alkane holder deltas (alkane -> token) and prepare rows for persistence.
    let mut alkane_balances_rows: HashMap<SchemaAlkaneId, Vec<BalanceEntry>> = HashMap::new();
    if !alkane_balance_delta.is_empty() {
        let mut owners: Vec<SchemaAlkaneId> = alkane_balance_delta.keys().copied().collect();
        owners.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(owners.len());
        for owner in &owners {
            keys.push(alkane_balances_key(owner));
        }
        let existing = mdb.multi_get(&keys)?;

        for (idx, owner) in owners.iter().enumerate() {
            let mut amounts: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(entries) = decode_balances_vec(bytes) {
                    for be in entries {
                        if be.amount == 0 {
                            continue;
                        }
                        amounts.insert(be.alkane, be.amount);
                    }
                }
            }

            if let Some(delta_map) = alkane_balance_delta.get(owner) {
                for (token, delta) in delta_map {
                    if *delta == 0 {
                        continue;
                    }
                    let cur = amounts.get(token).copied().unwrap_or(0);
                    let updated = if *delta >= 0 {
                        cur.saturating_add(*delta as u128)
                    } else {
                        let abs = i128_abs_u(*delta);
                        if abs > cur {
                            panic!(
                                "[balances] negative alkane balance detected (owner={}:{}, token={}:{}, cur={}, sub={})",
                                owner.block, owner.tx, token.block, token.tx, cur, abs
                            );
                        }
                        cur - abs
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

    // Build balance-change txid rows (merge existing + new)
    let mut alkane_balance_txs_rows: HashMap<SchemaAlkaneId, Vec<[u8; 32]>> = HashMap::new();
    let mut alkane_balance_txs_by_token_rows: HashMap<(SchemaAlkaneId, SchemaAlkaneId), Vec<[u8; 32]>> =
        HashMap::new();
    if !alkane_balance_txids.is_empty() {
        let mut tokens: Vec<SchemaAlkaneId> = alkane_balance_txids.keys().copied().collect();
        tokens.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for tok in &tokens {
            keys.push(alkane_balance_txs_key(tok));
        }
        let existing = mdb.multi_get(&keys)?;

        for (idx, tok) in tokens.iter().enumerate() {
            let mut merged: Vec<[u8; 32]> = Vec::new();
            let mut seen: HashSet<[u8; 32]> = HashSet::new();

            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(cur) = Vec::<[u8; 32]>::try_from_slice(bytes) {
                    for t in cur {
                        seen.insert(t);
                        merged.push(t);
                    }
                }
            }

            if let Some(new) = alkane_balance_txids.get(tok) {
                for t in new {
                    if seen.insert(*t) {
                        merged.push(*t);
                    }
                }
            }

            if !merged.is_empty() {
                alkane_balance_txs_rows.insert(*tok, merged);
            }
        }
    }
    if !alkane_balance_txids_by_token.is_empty() {
        let mut pairs: Vec<(SchemaAlkaneId, SchemaAlkaneId)> =
            alkane_balance_txids_by_token.keys().copied().collect();
        pairs.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(pairs.len());
        for (owner, token) in &pairs {
            keys.push(alkane_balance_txs_by_token_key(owner, token));
        }
        let existing = mdb.multi_get(&keys)?;

        for (idx, pair) in pairs.iter().enumerate() {
            let mut merged: Vec<[u8; 32]> = Vec::new();
            let mut seen: HashSet<[u8; 32]> = HashSet::new();

            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(cur) = Vec::<[u8; 32]>::try_from_slice(bytes) {
                    for t in cur {
                        seen.insert(t);
                        merged.push(t);
                    }
                }
            }

            if let Some(new) = alkane_balance_txids_by_token.get(pair) {
                for t in new {
                    if seen.insert(*t) {
                        merged.push(*t);
                    }
                }
            }

            if !merged.is_empty() {
                alkane_balance_txs_by_token_rows.insert(*pair, merged);
            }
        }
    }

    // logging metric
    stat_outpoints_marked_spent = spent_outpoints.len();

    // Build unified rows (new outputs + spent inputs)
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

    // --- Cleanup keys for outpoints that were spent (remove unspent variants) ---
    let mut del_keys_outpoint_balances: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_outpoint_addr: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_utxo_spk: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_addr_balances: Vec<Vec<u8>> = Vec::new();

    for row in &new_rows {
        if row.outpoint.tx_spent.is_some() {
            let unspent = mk_outpoint(row.outpoint.txid.clone(), row.outpoint.vout, None);

            del_keys_outpoint_balances.push(outpoint_balances_key(&unspent)?);
            del_keys_outpoint_addr.push(outpoint_addr_key(&unspent)?);
            del_keys_utxo_spk.push(utxo_spk_key(&unspent)?);
            del_keys_addr_balances.push(balances_key(&row.addr, &unspent)?);
        }
    }

    // ---- single write-batch ----
    let _resp = mdb.bulk_write(|wb: &mut MdbBatch<'_>| {
        // A) Address-scoped deletes
        for k in &del_keys_addr_balances {
            wb.delete(k);
        }

        // B) Reverse-index cleanup
        for k in &del_keys_outpoint_balances {
            wb.delete(k);
        }
        for k in &del_keys_outpoint_addr {
            wb.delete(k);
        }
        for k in &del_keys_utxo_spk {
            wb.delete(k);
        }

        // C) Persist new outputs (unspent + spent with tx_spent metadata)
        for row in &new_rows {
            let bkey = match balances_key(&row.addr, &row.outpoint) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let obkey = match outpoint_balances_key(&row.outpoint) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let oaddr_key = match outpoint_addr_key(&row.outpoint) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let uspk_key = match utxo_spk_key(&row.outpoint) {
                Ok(k) => k,
                Err(_) => continue,
            };

            wb.put(&bkey, &row.enc_balances);
            wb.put(&obkey, &row.enc_balances);
            wb.put(&oaddr_key, row.addr.as_bytes());
            if let Some(ref spk_bytes) = row.uspk_val {
                wb.put(&uspk_key, spk_bytes);
                wb.put(&addr_spk_key(&row.addr), spk_bytes);
            }
        }

        // C2) Persist alkane balance index (alkane -> Vec<BalanceEntry>)
        for (owner, entries) in alkane_balances_rows.iter() {
            let key = alkane_balances_key(owner);
            if entries.is_empty() {
                wb.delete(&key);
                continue;
            }
            if let Ok(buf) = encode_vec(entries) {
                wb.put(&key, &buf);
            }
        }

        // C3) Persist alkane balance change txids
        for (alk, txids) in alkane_balance_txs_rows.iter() {
            let key = alkane_balance_txs_key(alk);
            if txids.is_empty() {
                wb.delete(&key);
                continue;
            }
            if let Ok(buf) = encode_vec(txids) {
                wb.put(&key, &buf);
            }
        }
        // C3b) Persist alkane balance change txids by token
        for ((owner, token), txids) in alkane_balance_txs_by_token_rows.iter() {
            let key = alkane_balance_txs_by_token_key(owner, token);
            if txids.is_empty() {
                wb.delete(&key);
                continue;
            }
            if let Ok(buf) = encode_vec(txids) {
                wb.put(&key, &buf);
            }
        }

        // D) Holders deltas
        for (alkane, per_holder) in holders_delta.iter() {
            let holders_key = holders_key(alkane);
            let holders_count_key = holders_count_key(alkane);

            let current_holders = mdb.get(&holders_key).ok().flatten();
            let mut vec_holders: Vec<HolderEntry> = match current_holders {
                Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
                None => Vec::new(),
            };
            for (holder, delta) in per_holder {
                vec_holders = apply_holders_delta(vec_holders, holder, *delta);
            }
            if vec_holders.is_empty() {
                wb.delete(&holders_key);
            } else if let Ok((encoded_holders_vec, encoded_holders_count_vec)) =
                get_holders_values_encoded(vec_holders)
            {
                wb.put(&holders_key, &encoded_holders_vec);
                wb.put(&holders_count_key, &encoded_holders_count_vec);
            }
        }
    });

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
    match get_metashrew().get_latest_reserves_for_alkane(alk, alk) {
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

pub fn get_balance_for_address(mdb: &Mdb, address: &str) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let mut prefix = b"/balances/".to_vec();
    prefix.extend_from_slice(address.as_bytes());
    prefix.push(b'/');

    let keys = mdb.scan_prefix(&prefix).map_err(|e| anyhow!("scan_prefix failed: {e}"))?;
    let vals = mdb.multi_get(&keys).map_err(|e| anyhow!("multi_get failed: {e}"))?;

    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    for (k, v) in keys.iter().zip(vals) {
        if let Some(bytes) = v {
            // decode outpoint from key to determine spend status
            if k.len() <= prefix.len() {
                continue;
            }
            let Ok(op) = EspoOutpoint::try_from_slice(&k[prefix.len()..]) else { continue };
            if op.tx_spent.is_some() {
                continue; // skip spent outpoints when computing live balance
            }
            if let Ok(bals) = decode_balances_vec(&bytes) {
                for be in bals {
                    *agg.entry(be.alkane).or_default() =
                        agg.get(&be.alkane).copied().unwrap_or(0).saturating_add(be.amount);
                }
            }
        }
    }
    Ok(agg)
}

pub fn get_alkane_balances(
    mdb: &Mdb,
    owner: &SchemaAlkaneId,
) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let key = alkane_balances_key(owner);
    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();

    if let Some(bytes) = mdb.get(&key)? {
        if let Ok(bals) = decode_balances_vec(&bytes) {
            for be in bals {
                if be.amount == 0 {
                    continue;
                }
                *agg.entry(be.alkane).or_default() =
                    agg.get(&be.alkane).copied().unwrap_or(0).saturating_add(be.amount);
            }
        }
    }

    if let Some(self_balance) = lookup_self_balance(owner) {
        if self_balance == 0 {
            agg.remove(owner);
        } else {
            agg.insert(*owner, self_balance);
        }
    }

    Ok(agg)
}

#[derive(Default, Clone, Debug)]
pub struct OutpointLookup {
    pub balances: Vec<BalanceEntry>,
    pub spent_by: Option<Txid>,
}

pub fn get_outpoint_balances(mdb: &Mdb, txid: &Txid, vout: u32) -> Result<Vec<BalanceEntry>> {
    let pref = outpoint_balances_prefix(txid.as_byte_array().as_slice(), vout)?;
    let keys = mdb.scan_prefix(&pref)?;
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let vals = mdb.multi_get(&keys)?;
    for (_k, v) in keys.into_iter().zip(vals) {
        if let Some(bytes) = v {
            if let Ok(bals) = decode_balances_vec(&bytes) {
                return Ok(bals);
            }
        }
    }
    Ok(Vec::new())
}

pub fn get_outpoint_balances_with_spent(
    mdb: &Mdb,
    txid: &Txid,
    vout: u32,
) -> Result<OutpointLookup> {
    const OUTPOINT_BALANCES_PREFIX: &[u8] = b"/outpoint_balances/";

    let pref = outpoint_balances_prefix(txid.as_byte_array().as_slice(), vout)?;
    let keys = mdb.scan_prefix(&pref)?;
    if keys.is_empty() {
        return Ok(OutpointLookup::default());
    }

    let vals = mdb.multi_get(&keys)?;
    let mut fallback: Option<OutpointLookup> = None;

    for (k, v) in keys.into_iter().zip(vals) {
        let Some(bytes) = v else { continue };

        let spent_by = if k.len() > OUTPOINT_BALANCES_PREFIX.len() {
            EspoOutpoint::try_from_slice(&k[OUTPOINT_BALANCES_PREFIX.len()..])
                .ok()
                .and_then(|op| op.tx_spent.and_then(|t| Txid::from_slice(&t).ok()))
        } else {
            None
        };

        if let Ok(balances) = decode_balances_vec(&bytes) {
            let lookup = OutpointLookup { balances, spent_by };
            if lookup.spent_by.is_some() {
                return Ok(lookup);
            }
            if fallback.is_none() {
                fallback = Some(lookup);
            }
        }
    }

    Ok(fallback.unwrap_or_default())
}

pub fn get_holders_for_alkane(
    mdb: &Mdb,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize /*total*/, u128 /*supply*/, Vec<HolderEntry>)> {
    let key = holders_key(&alk);
    let cur = mdb.get(&key)?;
    let mut all = match cur {
        Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
        None => Vec::new(),
    };
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

pub fn get_scriptpubkey_for_address(mdb: &Mdb, addr: &str) -> Result<Option<ScriptBuf>> {
    let key = addr_spk_key(addr);
    let v = mdb.get(&key)?;
    Ok(v.map(ScriptBuf::from))
}
