// src/modules/essentials/utils/balances.rs

use super::super::storage::{
    BalanceEntry, HolderEntry, addr_spk_key, balances_key, decode_balances_vec, decode_holders_vec,
    encode_vec, holders_count_key, holders_key, outpoint_addr_key, outpoint_balances_key,
    spk_to_address_str, utxo_spk_key,
};
use crate::config::get_network;
use crate::modules::essentials::storage::get_holders_values_encoded;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Result, anyhow};
use bitcoin::{ScriptBuf, Transaction, Txid, hashes::Hash};
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

fn apply_holders_delta(mut holders: Vec<HolderEntry>, addr: &str, delta: i128) -> Vec<HolderEntry> {
    let idx = holders.iter().position(|h| h.address == addr);
    match delta.cmp(&0) {
        std::cmp::Ordering::Equal => return holders,
        std::cmp::Ordering::Greater => {
            let add = delta as u128;
            if let Some(i) = idx {
                holders[i].amount = holders[i].amount.saturating_add(add);
            } else {
                holders.push(HolderEntry { address: addr.to_string(), amount: add });
            }
        }
        std::cmp::Ordering::Less => {
            let sub = (-delta) as u128;
            if let Some(i) = idx {
                let after = holders[i].amount.saturating_sub(sub);
                if after == 0 {
                    holders.swap_remove(i);
                } else {
                    holders[i].amount = after;
                }
            }
        }
    }
    holders.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => a.address.cmp(&b.address),
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
    let mut stat_outpoints_deleted: usize = 0;
    let mut stat_outpoints_written: usize = 0;
    let mut stat_minus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut stat_plus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

    // holders_delta[alk][addr] = i128 delta
    let mut holders_delta: HashMap<SchemaAlkaneId, BTreeMap<String, i128>> = HashMap::new();

    // DB deletes for addr-scoped rows that actually exist
    let mut spent_map_db_only: HashMap<(String, EspoOutpoint), Vec<BalanceEntry>> = HashMap::new();

    // Reverse-index cleanup set
    let mut all_input_outpoints: HashSet<EspoOutpoint> = HashSet::new();

    // Ephemeral state for CPFP within the same block
    let mut ephem_outpoint_balances: HashMap<String, Vec<BalanceEntry>> = HashMap::new();
    let mut ephem_outpoint_addr: HashMap<String, String> = HashMap::new();
    let mut ephem_outpoint_spk: HashMap<String, ScriptBuf> = HashMap::new();
    let mut ephem_outpoint_struct: HashMap<String, EspoOutpoint> = HashMap::new();
    let mut consumed_ephem_outpoints: HashSet<String> = HashSet::new();

    // Holder delta helper
    let mut add_holder_delta = |alk: SchemaAlkaneId, addr: &str, delta: i128| {
        holders_delta
            .entry(alk)
            .or_default()
            .entry(addr.to_string())
            .and_modify(|d| *d += delta)
            .or_insert(delta);
    };

    // ---------- Pass A: collect block-created outpoints & external inputs ----------
    let mut block_created_outs: HashSet<String> = HashSet::new();
    for atx in &block.transactions {
        let tx = &atx.transaction;
        if !tx_has_op_return(tx) {
            continue; // no OP_RETURN → no Alkanes activity on its outputs
        }
        let txid = tx.compute_txid();
        for (vout, _o) in tx.output.iter().enumerate() {
            let op = EspoOutpoint { txid: txid.as_byte_array().to_vec(), vout: vout as u32 };
            block_created_outs.insert(op.as_outpoint_string());
        }
    }

    // Collect all non-ephemeral vins across the block (dedup)
    let mut external_inputs_vec: Vec<EspoOutpoint> = Vec::new();
    let mut external_inputs_set: HashSet<(Vec<u8>, u32)> = HashSet::new();

    for atx in &block.transactions {
        for input in &atx.transaction.input {
            let op = EspoOutpoint {
                txid: input.previous_output.txid.as_byte_array().to_vec(),
                vout: input.previous_output.vout,
            };
            let in_str = op.as_outpoint_string();
            if !block_created_outs.contains(&in_str) {
                let key = (op.txid.clone(), op.vout);
                if external_inputs_set.insert(key) {
                    external_inputs_vec.push(op);
                }
            }
        }
    }

    // ---------- Pass B: batch reads once for the whole block ----------
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

        for (i, op) in external_inputs_vec.into_iter().enumerate() {
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

        // Seed from VIN balances only
        let mut seed_unalloc = Unallocated::default();

        // Gather ephemerals for this tx & apply; for externals, use prefetched maps
        for input in &tx.input {
            let in_op = EspoOutpoint {
                txid: input.previous_output.txid.as_byte_array().to_vec(),
                vout: input.previous_output.vout,
            };
            let in_key = (in_op.txid.clone(), in_op.vout);
            let in_str = in_op.as_outpoint_string();

            all_input_outpoints.insert(in_op.clone());

            // 1) Ephemeral? (created earlier in this same block)
            if let Some(bals) = ephem_outpoint_balances.get(&in_str) {
                consumed_ephem_outpoints.insert(in_str.clone());

                if let Some(addr) = ephem_outpoint_addr.get(&in_str) {
                    for be in bals {
                        add_holder_delta(be.alkane, addr, -(be.amount as i128));
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

                if let Some(addr) = resolved_addr {
                    // holders-- and mark legacy addr-row delete
                    spent_map_db_only.insert((addr.clone(), in_op.clone()), bals.clone());
                    for be in &bals {
                        add_holder_delta(be.alkane, &addr, -(be.amount as i128));
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
                    }
                }

                for be in bals {
                    seed_unalloc.add(be.alkane, be.amount);
                }
            }
            // else: no balances row → nothing to do for this vin
        }

        // apply transfers with your semantics
        let allocations = if tx_has_op_return(tx) {
            let protostones = parse_protostones(tx)?;
            let traces_for_tx: Vec<EspoTrace> = atx.traces.clone().unwrap_or_default();
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

                let created_outpoint =
                    EspoOutpoint { txid: txid.as_byte_array().to_vec(), vout: vout_idx };
                let outpoint_str = created_outpoint.as_outpoint_string();

                // cache for same-block spends
                ephem_outpoint_balances.insert(outpoint_str.clone(), balances_for_outpoint.clone());
                ephem_outpoint_addr.insert(outpoint_str.clone(), address_str.clone());
                ephem_outpoint_spk.insert(outpoint_str.clone(), output.script_pubkey.clone());
                ephem_outpoint_struct.insert(outpoint_str.clone(), created_outpoint.clone());

                // holders++ stats
                for (alkane_id, delta_amount) in amounts_by_alkane {
                    add_holder_delta(alkane_id, &address_str, delta_amount as i128);
                    *stat_plus_by_alk.entry(alkane_id).or_default() = stat_plus_by_alk
                        .get(&alkane_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta_amount);
                }

                stat_outpoints_written += 1;
            }
        }
    }

    // logging metric
    stat_outpoints_deleted = spent_map_db_only.len();

    // --- Precompute cleanup keys ---
    let mut del_keys_outpoint_balances: Vec<Vec<u8>> =
        Vec::with_capacity(all_input_outpoints.len());
    let mut del_keys_outpoint_addr: Vec<Vec<u8>> = Vec::with_capacity(all_input_outpoints.len());
    let mut del_keys_utxo_spk: Vec<Vec<u8>> = Vec::with_capacity(all_input_outpoints.len());
    for op in &all_input_outpoints {
        del_keys_outpoint_balances.push(outpoint_balances_key(op)?);
        del_keys_outpoint_addr.push(outpoint_addr_key(op)?);
        del_keys_utxo_spk.push(utxo_spk_key(op)?);
    }

    // addr-row delete keys
    let mut del_keys_addr_balances: Vec<Vec<u8>> = Vec::with_capacity(spent_map_db_only.len());
    for ((addr, op), _bals) in &spent_map_db_only {
        del_keys_addr_balances.push(balances_key(addr, op)?);
    }

    // --- Precompute new-output rows to persist ---
    struct NewRow {
        bkey: Vec<u8>,             // /balances/{addr}/{borsh(EspoOutpoint)}
        obkey: Vec<u8>,            // /outpoint_balances/{outpoint}
        oaddr_key: Vec<u8>,        // /outpoint_addr/{outpoint}
        uspk_key: Vec<u8>,         // /utxo_spk/{outpoint}
        uspk_val: Option<Vec<u8>>, // spk bytes
        addr: String,
        enc_balances: Vec<u8>,
    }
    let mut new_rows: Vec<NewRow> = Vec::new();

    for (out_str, vec_out) in &ephem_outpoint_balances {
        if consumed_ephem_outpoints.contains(out_str) {
            continue; // created & spent within block ⇒ don't persist
        }
        let addr = match ephem_outpoint_addr.get(out_str) {
            Some(a) => a.clone(),
            None => continue,
        };
        let op = match ephem_outpoint_struct.get(out_str) {
            Some(o) => o.clone(),
            None => continue,
        };

        let bkey = balances_key(&addr, &op)?;
        let obkey = outpoint_balances_key(&op)?;
        let oaddr_key = outpoint_addr_key(&op)?;
        let uspk_key = utxo_spk_key(&op)?;
        let uspk_val = ephem_outpoint_spk.get(out_str).map(|spk| spk.as_bytes().to_vec());
        let enc_balances = encode_vec(vec_out)?;

        new_rows.push(NewRow { bkey, obkey, oaddr_key, uspk_key, uspk_val, addr, enc_balances });
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

        // C) Persist new outputs
        for row in &new_rows {
            wb.put(&row.bkey, &row.enc_balances);
            wb.put(&row.obkey, &row.enc_balances);
            wb.put(&row.oaddr_key, row.addr.as_bytes());
            if let Some(ref spk_bytes) = row.uspk_val {
                wb.put(&row.uspk_key, spk_bytes);
                wb.put(&addr_spk_key(&row.addr), spk_bytes);
            }
        }

        // D) Holders deltas
        for (alkane, per_address) in holders_delta.iter() {
            let holders_key = holders_key(alkane);
            let holders_count_key = holders_count_key(alkane);

            let current_holders = mdb.get(&holders_key).ok().flatten();
            let mut vec_holders: Vec<HolderEntry> = match current_holders {
                Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
                None => Vec::new(),
            };
            for (address, delta) in per_address {
                vec_holders = apply_holders_delta(vec_holders, address, *delta);
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

    // -------- block summary ----------
    let minus_total: u128 = stat_minus_by_alk.values().copied().sum();
    let plus_total: u128 = stat_plus_by_alk.values().copied().sum();

    eprintln!(
        "[balances] block #{}, txs={}, outpoints_written={}, outpoints_deleted={}, alkanes_added={}, alkanes_removed={}, unique_add={}, unique_remove={}",
        block.height,
        block.transactions.len(),
        stat_outpoints_written,
        stat_outpoints_deleted,
        plus_total,
        minus_total,
        stat_plus_by_alk.len(),
        stat_minus_by_alk.len()
    );
    eprintln!("[balances] <<< end   block #{}", block.height);

    Ok(())
}

/* -------------------------- Queries -------------------------- */

pub fn get_balance_for_address(mdb: &Mdb, address: &str) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let mut prefix = b"/balances/".to_vec();
    prefix.extend_from_slice(address.as_bytes());
    prefix.push(b'/');

    let keys = mdb.scan_prefix(&prefix).map_err(|e| anyhow!("scan_prefix failed: {e}"))?;
    let vals = mdb.multi_get(&keys).map_err(|e| anyhow!("multi_get failed: {e}"))?;

    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    for v in vals {
        if let Some(bytes) = v {
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

pub fn get_outpoint_balances(mdb: &Mdb, txid: &Txid, vout: u32) -> Result<Vec<BalanceEntry>> {
    let outpoint = EspoOutpoint { txid: txid.as_byte_array().to_vec(), vout };
    match mdb.get(&outpoint_balances_key(&outpoint)?)? {
        Some(bytes) => decode_balances_vec(&bytes),
        None => Ok(Vec::new()),
    }
}

pub fn get_holders_for_alkane(
    mdb: &Mdb,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize /*total*/, Vec<HolderEntry>)> {
    let key = holders_key(&alk);
    let cur = mdb.get(&key)?;
    let mut all = match cur {
        Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
        None => Vec::new(),
    };
    all.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => a.address.cmp(&b.address),
        o => o,
    });
    let total = all.len();
    let p = page.max(1);
    let l = limit.max(1);
    let off = l.saturating_mul(p - 1);
    let end = (off + l).min(total);
    let slice = if off >= total { vec![] } else { all[off..end].to_vec() };
    Ok((total, slice))
}

/* -------------------------- Optional: resolve address -> spk -------------------------- */

pub fn get_scriptpubkey_for_address(mdb: &Mdb, addr: &str) -> Result<Option<ScriptBuf>> {
    let key = addr_spk_key(addr);
    let v = mdb.get(&key)?;
    Ok(v.map(ScriptBuf::from))
}
