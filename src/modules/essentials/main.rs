use crate::alkanes::trace::{EspoBlock, EspoSandshrewLikeTraceEvent};
use crate::config::{debug_enabled, get_metashrew, get_network};
use crate::debug;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::consts::{
    ESSENTIALS_GENESIS_INSPECTIONS, essentials_genesis_block,
};
use crate::modules::essentials::rpc;
use crate::modules::essentials::storage::{
    BlockSummary, EssentialsProvider, GetRawValueParams, cache_block_summary,
    encode_creation_record,
};
use crate::modules::essentials::utils::creation_meta::{get_cap, get_value_per_mint};
use crate::modules::essentials::utils::inspections::{
    AlkaneCreationRecord, StoredInspectionResult, created_alkane_records_from_block,
    inspect_wasm_metadata,
};
use crate::modules::essentials::utils::names::{
    get_name as get_alkane_name, normalize_alkane_name,
};
use crate::runtime::mdb::Mdb;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use std::collections::HashMap;
use std::sync::Arc;

// ✅ bring in balances bulk updater
use crate::modules::essentials::utils::balances::bulk_update_balances_for_block;

fn parse_short_id(
    id: &crate::alkanes::trace::EspoSandshrewLikeTraceShortId,
) -> Option<SchemaAlkaneId> {
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

fn decode_u128_le_bytes(bytes: &[u8]) -> Option<u128> {
    if bytes.is_empty() {
        return None;
    }
    let mut buf = [0u8; 16];
    if bytes.len() >= 16 {
        buf.copy_from_slice(&bytes[..16]);
    } else {
        buf[..bytes.len()].copy_from_slice(bytes);
    }
    Some(u128::from_le_bytes(buf))
}

fn is_orbital_instance(inspection: &StoredInspectionResult) -> bool {
    inspection
        .metadata
        .as_ref()
        .map(|meta| meta.name.contains("OrbitalInstance"))
        .unwrap_or(false)
}

fn orbital_base_name(full_name: &str) -> Option<String> {
    let (base, _) = full_name.split_once('#')?;
    let trimmed = base.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn ensure_primary_name(names: &mut Vec<String>, name: &str) {
    if let Some(pos) = names.iter().position(|n| n == name) {
        if pos == 0 {
            return;
        }
        names.remove(pos);
    }
    names.insert(0, name.to_string());
}

pub struct Essentials {
    provider: Option<Arc<EssentialsProvider>>,
    inspection_cache: Arc<std::sync::RwLock<HashMap<SchemaAlkaneId, StoredInspectionResult>>>,
}

impl Essentials {
    pub fn new() -> Self {
        Self {
            provider: None,
            inspection_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    #[inline]
    fn provider(&self) -> &EssentialsProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        let resp = self.provider().get_index_height(
            crate::modules::essentials::storage::GetIndexHeightParams {
                blockhash: StateAt::Latest,
            },
        )?;
        Ok(resp.height)
    }

    fn persist_index_height(&self, height: u32, blockhash: StateAt) -> Result<()> {
        self.provider().set_index_height(
            crate::modules::essentials::storage::SetIndexHeightParams { blockhash, height },
        )
    }

    fn set_index_height(&self, new_height: u32, blockhash: StateAt) -> Result<()> {
        if let Some(prev) = self.load_index_height()? {
            if new_height < prev {
                eprintln!(
                    "[ESSENTIALS] index height rollback detected ({} -> {})",
                    prev, new_height
                );
            }
        }
        self.persist_index_height(new_height, blockhash)?;
        Ok(())
    }

    /* ---------------- key helpers now live in storage.rs ---------------- */
}

impl Default for Essentials {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Essentials {
    fn get_name(&self) -> &'static str {
        "essentials"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        self.provider = Some(Arc::new(EssentialsProvider::new(mdb.clone())));
        match self.load_index_height() {
            Ok(h) => eprintln!("[ESSENTIALS] loaded index height: {:?}", h),
            Err(e) => eprintln!("[ESSENTIALS] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        essentials_genesis_block(network)
    }

    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        self.provider.as_ref().map(|provider| Arc::new(provider.mdb().clone()))
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let provider = self.provider();
        let table = provider.table();
        let block_hash = block.block_header.block_hash();

        // -------- Phase A: coalesce per-block writes in memory --------
        // last-write-wins for values:
        //   kv_row_key(alk,skey) -> [ txid(32) | value(...) ]
        use std::collections::{HashMap, HashSet};

        let timer = debug::start_if(debug);
        let mut kv_rows: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        // dedup directory markers:
        //   dir_row_key(alk,skey) -> ()
        let mut dir_rows: HashSet<Vec<u8>> = HashSet::new();
        // creation records rows (by id and sequence index):
        let mut creation_rows_by_id: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut creation_rows_seq: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut holders_index_rows: HashSet<Vec<u8>> = HashSet::new();
        // in-block name/symbol updates detected from storage writes
        let mut meta_updates: HashMap<SchemaAlkaneId, (Vec<String>, Vec<String>)> = HashMap::new();
        let mut cap_updates: HashMap<SchemaAlkaneId, u128> = HashMap::new();
        let mut mint_updates: HashMap<SchemaAlkaneId, u128> = HashMap::new();
        let mut orbital_index_updates: HashMap<SchemaAlkaneId, u128> = HashMap::new();
        let mut orbital_collection_name_rows: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut orbital_collection_name_cache: HashMap<SchemaAlkaneId, Option<String>> =
            HashMap::new();
        let mut name_index_rows: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut symbol_index_rows: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let add_name_index =
            |rows: &mut HashMap<Vec<u8>, Vec<u8>>, alk: &SchemaAlkaneId, name: &str| {
                if let Some(norm) = normalize_alkane_name(name) {
                    rows.insert(table.alkane_name_index_key(&norm, alk), Vec::new());
                }
            };
        let add_symbol_index =
            |rows: &mut HashMap<Vec<u8>, Vec<u8>>, alk: &SchemaAlkaneId, symbol: &str| {
                if let Some(norm) = normalize_alkane_name(symbol) {
                    rows.insert(table.alkane_symbol_index_key(&norm, alk), Vec::new());
                }
            };
        // block summary row (trace count + header)
        let trace_count = block
            .transactions
            .iter()
            .map(|tx| tx.traces.as_ref().map(|t| t.len()).unwrap_or(0))
            .sum::<usize>() as u32;
        let mut header_bytes = Vec::new();
        block.block_header.consensus_encode(&mut header_bytes)?;
        let block_summary = BlockSummary { trace_count, header: header_bytes };
        let block_summary_bytes = borsh::to_vec(&block_summary)?;

        let mut total_pairs_dedup = 0usize;

        for tx in block.transactions.iter() {
            let Some(traces) = tx.traces.as_ref() else { continue };
            for trace in traces.iter() {
                let mut created_in_trace: HashSet<SchemaAlkaneId> = HashSet::new();
                for ev in trace.sandshrew_trace.events.iter() {
                    if let EspoSandshrewLikeTraceEvent::Create(create) = ev {
                        if let Some(id) = parse_short_id(create) {
                            created_in_trace.insert(id);
                        }
                    }
                }

                for (alk, kvs) in trace.storage_changes.iter() {
                    for (skey, (txid, value)) in kvs.iter() {
                        // Key for value row
                        let k_v = table.kv_row_key(alk, skey);

                        // Value layout: [ txid(32) | value(...) ]
                        let mut buf = Vec::with_capacity(32 + value.len());
                        buf.extend_from_slice(&txid.to_byte_array());
                        buf.extend_from_slice(value);

                        // last-write-wins for this key within the block
                        kv_rows.insert(k_v, buf);

                        // Dir entry is idempotent; one per (alk, skey) is enough
                        let k_dir = table.dir_row_key(alk, skey);
                        dir_rows.insert(k_dir);

                        // Track name/symbol updates for the alkane (append if new)
                        let push_if_new =
                            |map: &mut HashMap<SchemaAlkaneId, (Vec<String>, Vec<String>)>,
                             alk: SchemaAlkaneId,
                             name: Option<String>,
                             symbol: Option<String>| {
                                let entry = map.entry(alk).or_default();
                                if let Some(n) = name {
                                    if !entry.0.iter().any(|v| v == &n) {
                                        entry.0.push(n);
                                    }
                                }
                                if let Some(s) = symbol {
                                    if !entry.1.iter().any(|v| v == &s) {
                                        entry.1.push(s);
                                    }
                                }
                            };
                        if skey.as_slice() == b"/name" {
                            if let Ok(name) = String::from_utf8(value.clone()) {
                                push_if_new(&mut meta_updates, *alk, Some(name), None);
                            }
                        } else if skey.as_slice() == b"/symbol" {
                            if let Ok(symbol) = String::from_utf8(value.clone()) {
                                push_if_new(&mut meta_updates, *alk, None, Some(symbol));
                            }
                        } else if created_in_trace.contains(alk) {
                            if skey.as_slice() == b"/cap" {
                                if let Some(cap) = decode_u128_le_bytes(value) {
                                    cap_updates.insert(*alk, cap);
                                }
                            } else if skey.as_slice() == b"/value-per-mint"
                                || skey.as_slice() == b"/value_per_mint"
                            {
                                if let Some(mint_amount) = decode_u128_le_bytes(value) {
                                    mint_updates.insert(*alk, mint_amount);
                                }
                            } else if skey.as_slice() == b"/index" {
                                if let Some(idx) = decode_u128_le_bytes(value) {
                                    orbital_index_updates.insert(*alk, idx);
                                }
                            }
                        }

                        total_pairs_dedup += 1;
                    }
                }
            }
        }

        debug::log_elapsed(module, "collect_storage_changes", timer);
        let timer = debug::start_if(debug);
        let mut created_records = created_alkane_records_from_block(&block);
        // Special case: ensure genesis alkanes are inspected on the genesis block even if no trace emits a create.
        let genesis_height = essentials_genesis_block(get_network());
        if block.height == genesis_height {
            let genesis_targets: Vec<(SchemaAlkaneId, Option<(&str, &str)>)> =
                ESSENTIALS_GENESIS_INSPECTIONS
                    .iter()
                    .map(|(block, tx, meta)| (SchemaAlkaneId { block: *block, tx: *tx }, *meta))
                    .collect();
            // Use the coinbase txid when available; fall back to zeroed bytes.
            let txid_bytes = block
                .transactions
                .first()
                .map(|t| {
                    let mut b = t.transaction.compute_txid().to_byte_array();
                    b.reverse();
                    b
                })
                .unwrap_or([0u8; 32]);
            for (alkane, meta) in genesis_targets {
                if created_records.iter().any(|r| r.alkane == alkane) {
                    continue;
                }
                let (names, symbols) = match meta {
                    Some((name, symbol)) => (vec![name.to_string()], vec![symbol.to_string()]),
                    None => (Vec::new(), Vec::new()),
                };
                created_records.push(AlkaneCreationRecord {
                    alkane,
                    txid: txid_bytes,
                    creation_height: block.height,
                    creation_timestamp: block.block_header.time,
                    tx_index_in_block: 0,
                    inspection: None,
                    names,
                    symbols,
                    cap: 0,
                    mint_amount: 0,
                });
            }
        }

        // Attach name/symbol from this block's storage writes (append, preserving first seen).
        for rec in created_records.iter_mut() {
            if let Some((names, symbols)) = meta_updates.get(&rec.alkane) {
                for n in names {
                    if !rec.names.iter().any(|v| v == n) {
                        rec.names.push(n.clone());
                    }
                }
                for s in symbols {
                    if !rec.symbols.iter().any(|v| v == s) {
                        rec.symbols.push(s.clone());
                    }
                }
            }
        }

        for rec in created_records.iter_mut() {
            if let Some(cap) = cap_updates.get(&rec.alkane) {
                rec.cap = *cap;
            }
            if let Some(mint_amount) = mint_updates.get(&rec.alkane) {
                rec.mint_amount = *mint_amount;
            }
        }

        debug::log_elapsed(module, "build_creation_records", timer);
        let timer = debug::start_if(debug);
        let inspect_timer = debug::start_if(debug);
        let metashrew = get_metashrew();
        for rec in created_records.iter_mut() {
            match metashrew.get_alkane_wasm_bytes(&rec.alkane) {
                Ok(Some((wasm_bytes, factory_id))) => {
                    let cached = {
                        let cache = self.inspection_cache.read().unwrap();
                        cache.get(&factory_id).cloned()
                    };
                    if let Some(mut cached) = cached {
                        cached.alkane = rec.alkane;
                        cached.factory_alkane = Some(factory_id);
                        rec.inspection = Some(cached);
                        continue;
                    }
                    match inspect_wasm_metadata(&rec.alkane, &wasm_bytes, factory_id) {
                        Ok(record) => {
                            rec.inspection = Some(record.clone());
                            let mut cache = self.inspection_cache.write().unwrap();
                            cache.insert(factory_id, record);
                        }
                        Err(e) => {
                            eprintln!(
                                "[ESSENTIALS] inspection failed for {}:{}: {e}",
                                rec.alkane.block, rec.alkane.tx
                            );
                        }
                    }
                }
                Ok(None) => {
                    eprintln!(
                        "[ESSENTIALS] no wasm payload found for alkane {}:{}; skipping inspection",
                        rec.alkane.block, rec.alkane.tx
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[ESSENTIALS] failed to fetch wasm for alkane {}:{}: {e}",
                        rec.alkane.block, rec.alkane.tx
                    );
                }
            }
        }
        debug::log_elapsed(module, "inspect_and_enrich.inspect_wasm_metadata", inspect_timer);

        let names_timer = debug::start_if(debug);
        for rec in created_records.iter_mut() {
            let mut applied_orbital_name = false;
            if let Some(inspection) = rec.inspection.as_ref() {
                if is_orbital_instance(inspection) {
                    rec.symbols.clear();
                    let factory_id = inspection.factory_alkane.unwrap_or(rec.alkane);
                    let mut base_name =
                        if let Some(cached) = orbital_collection_name_cache.get(&factory_id) {
                            cached.clone()
                        } else {
                            let key = table.orbital_collection_name_key(&factory_id);
                            let name = provider
                                .get_raw_value(GetRawValueParams {
                                    blockhash: StateAt::Block(block_hash),
                                    key,
                                })
                                .ok()
                                .and_then(|resp| resp.value)
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                                .map(|s| s.trim_matches('\0').trim().to_string())
                                .filter(|s| !s.is_empty());
                            orbital_collection_name_cache.insert(factory_id, name.clone());
                            name
                        };
                    let mut name_from_simulate: Option<String> = None;

                    if base_name.is_none() {
                        if let Some(existing_name) = rec.names.first() {
                            base_name = orbital_base_name(existing_name);
                        }
                    }

                    if base_name.is_none() && rec.names.is_empty() {
                        if let Some(name) = get_alkane_name(
                            StateAt::Block(block_hash),
                            &block,
                            &rec.alkane,
                            rec.inspection.as_ref(),
                        ) {
                            name_from_simulate = Some(name.clone());
                            base_name = orbital_base_name(&name);
                        }
                    }

                    if let Some(base) = base_name.as_ref() {
                        let existing =
                            orbital_collection_name_cache.get(&factory_id).and_then(|v| v.as_ref());
                        if existing.is_none() {
                            let key = table.orbital_collection_name_key(&factory_id);
                            orbital_collection_name_rows.insert(key, base.as_bytes().to_vec());
                            orbital_collection_name_cache
                                .insert(factory_id, Some(base.to_string()));
                        }
                        if let Some(idx) = orbital_index_updates.get(&rec.alkane).copied() {
                            let constructed = format!("{base} #{}", idx.saturating_add(1));
                            ensure_primary_name(&mut rec.names, &constructed);
                            applied_orbital_name = true;
                        } else if let Some(name) = name_from_simulate.as_ref() {
                            ensure_primary_name(&mut rec.names, name);
                            applied_orbital_name = true;
                        }
                    } else if let Some(name) = name_from_simulate.as_ref() {
                        ensure_primary_name(&mut rec.names, name);
                        applied_orbital_name = true;
                    }
                }
            }

            if rec.names.is_empty() && !applied_orbital_name {
                if let Some(name) = get_alkane_name(
                    StateAt::Block(block_hash),
                    &block,
                    &rec.alkane,
                    rec.inspection.as_ref(),
                ) {
                    rec.names.push(name);
                }
            }
        }
        debug::log_elapsed(module, "inspect_and_enrich.names", names_timer);

        let meta_timer = debug::start_if(debug);
        for rec in created_records.iter_mut() {
            if rec.cap == 0 && !cap_updates.contains_key(&rec.alkane) {
                if let Some(cap) = get_cap(
                    StateAt::Block(block_hash),
                    block.height,
                    &rec.alkane,
                    rec.inspection.as_ref(),
                ) {
                    rec.cap = cap;
                }
            }
            if rec.mint_amount == 0 && !mint_updates.contains_key(&rec.alkane) {
                if let Some(mint_amount) = get_value_per_mint(
                    StateAt::Block(block_hash),
                    block.height,
                    &rec.alkane,
                    rec.inspection.as_ref(),
                ) {
                    rec.mint_amount = mint_amount;
                }
            }
        }
        debug::log_elapsed(module, "inspect_and_enrich.cap_mint", meta_timer);

        debug::log_elapsed(module, "inspect_and_enrich", timer);
        let timer = debug::start_if(debug);
        for rec in created_records.iter() {
            for name in rec.names.iter() {
                add_name_index(&mut name_index_rows, &rec.alkane, name);
            }
            for symbol in rec.symbols.iter() {
                add_symbol_index(&mut symbol_index_rows, &rec.alkane, symbol);
            }
        }

        debug::log_elapsed(module, "build_creation_indexes", timer);
        let mut creations_in_block: Vec<SchemaAlkaneId> = Vec::new();
        let mut seen_creations_in_block: HashSet<SchemaAlkaneId> = HashSet::new();
        for rec in created_records.iter() {
            if seen_creations_in_block.insert(rec.alkane) {
                creations_in_block.push(rec.alkane);
            }
        }
        // Dedup against existing records to avoid double-counting if re-run.
        let timer = debug::start_if(debug);
        let mut new_creations_added: u64 = 0;
        let mut next_creation_seq = provider
            .get_creation_count(crate::modules::essentials::storage::GetCreationCountParams {
                blockhash: StateAt::Block(block_hash),
            })?
            .count;
        if !created_records.is_empty() {
            let alkanes: Vec<SchemaAlkaneId> = created_records.iter().map(|r| r.alkane).collect();
            let existing = provider
                .get_creation_records_by_id(
                    crate::modules::essentials::storage::GetCreationRecordsByIdParams {
                        blockhash: StateAt::Block(block_hash),
                        alkanes,
                    },
                )?
                .records;
            let id_keys: Vec<Vec<u8>> = created_records
                .iter()
                .map(|r| table.alkane_creation_by_id_key(&r.alkane))
                .collect();

            for (idx, rec) in created_records.into_iter().enumerate() {
                let key_id = &id_keys[idx];
                let encoded = match encode_creation_record(&rec) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "[ESSENTIALS] failed to encode creation record for {}:{}: {e}",
                            rec.alkane.block, rec.alkane.tx
                        );
                        continue;
                    }
                };

                let already = existing.get(idx).and_then(|v| v.as_ref());
                if let Some(prev) = already {
                    // If we already have a record, upgrade inspection/name/symbol when present.
                    let mut updated = prev.clone();
                    let mut dirty = false;
                    if updated.inspection.is_none() && rec.inspection.is_some() {
                        updated.inspection = rec.inspection.clone();
                        dirty = true;
                    }
                    // Merge names/symbols (append if new)
                    let maybe_push = |vec: &mut Vec<String>, vals: &[String]| {
                        let mut changed = false;
                        for v in vals {
                            if !vec.iter().any(|x| x == v) {
                                vec.push(v.clone());
                                changed = true;
                            }
                        }
                        changed
                    };
                    if maybe_push(&mut updated.names, &rec.names) {
                        dirty = true;
                    }
                    if maybe_push(&mut updated.symbols, &rec.symbols) {
                        dirty = true;
                    }
                    if let Some((names, syms)) = meta_updates.get(&updated.alkane) {
                        if maybe_push(&mut updated.names, names) {
                            dirty = true;
                        }
                        if maybe_push(&mut updated.symbols, syms) {
                            dirty = true;
                        }
                    }
                    if dirty {
                        let encoded_updated = match encode_creation_record(&updated) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!(
                                    "[ESSENTIALS] failed to encode updated creation record for {}:{}: {e}",
                                    updated.alkane.block, updated.alkane.tx
                                );
                                continue;
                            }
                        };
                        creation_rows_by_id.insert(key_id.clone(), encoded_updated);
                    }
                    continue;
                }

                new_creations_added += 1;
                creation_rows_by_id.insert(key_id.clone(), encoded);
                let mut alkane_id_bytes = Vec::with_capacity(12);
                alkane_id_bytes.extend_from_slice(&rec.alkane.block.to_be_bytes());
                alkane_id_bytes.extend_from_slice(&rec.alkane.tx.to_be_bytes());
                creation_rows_seq
                    .insert(table.alkane_creation_seq_key(next_creation_seq), alkane_id_bytes);
                next_creation_seq = next_creation_seq.saturating_add(1);
                holders_index_rows.insert(table.alkane_holders_ordered_key(0, &rec.alkane));
            }
        }

        // Also update name/symbol for alkanes that had metadata writes in this block but were not newly created.
        for (alk, (names, symbols)) in meta_updates.iter() {
            let key_id = table.alkane_creation_by_id_key(alk);
            if creation_rows_by_id.contains_key(&key_id) {
                continue; // already updated via creation path above
            }
            let Some(mut rec) = provider
                .get_creation_record(crate::modules::essentials::storage::GetCreationRecordParams {
                    blockhash: StateAt::Block(block_hash),
                    alkane: *alk,
                })
                .ok()
                .and_then(|resp| resp.record)
            else {
                continue;
            };
            let mut name_dirty = false;
            let mut symbol_dirty = false;
            let maybe_push = |vec: &mut Vec<String>, vals: &[String]| {
                let mut changed = false;
                for v in vals {
                    if !vec.iter().any(|x| x == v) {
                        vec.push(v.clone());
                        changed = true;
                    }
                }
                changed
            };
            if maybe_push(&mut rec.names, names) {
                name_dirty = true;
            }
            if maybe_push(&mut rec.symbols, symbols) {
                symbol_dirty = true;
            }
            if !name_dirty && !symbol_dirty {
                continue;
            }
            if name_dirty {
                for name in rec.names.iter() {
                    add_name_index(&mut name_index_rows, &rec.alkane, name);
                }
            }
            if symbol_dirty {
                for symbol in rec.symbols.iter() {
                    add_symbol_index(&mut symbol_index_rows, &rec.alkane, symbol);
                }
            }
            let encoded = match encode_creation_record(&rec) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[ESSENTIALS] failed to encode updated creation record for {}:{}: {e}",
                        rec.alkane.block, rec.alkane.tx
                    );
                    continue;
                }
            };
            creation_rows_by_id.insert(key_id, encoded);
        }

        debug::log_elapsed(module, "dedup_and_update_creations", timer);
        // -------- Phase B: write in sorted key order (better LSM locality) --------
        let timer = debug::start_if(debug);
        let mut kv_keys: Vec<Vec<u8>> = kv_rows.keys().cloned().collect();
        kv_keys.sort_unstable();
        let mut dir_keys: Vec<Vec<u8>> = dir_rows.into_iter().collect();
        dir_keys.sort_unstable();
        let mut creation_keys_by_id: Vec<Vec<u8>> = creation_rows_by_id.keys().cloned().collect();
        creation_keys_by_id.sort_unstable();
        let mut creation_keys_seq: Vec<Vec<u8>> = creation_rows_seq.keys().cloned().collect();
        creation_keys_seq.sort_unstable();
        let mut name_index_keys: Vec<Vec<u8>> = name_index_rows.keys().cloned().collect();
        name_index_keys.sort_unstable();
        let mut symbol_index_keys: Vec<Vec<u8>> = symbol_index_rows.keys().cloned().collect();
        symbol_index_keys.sort_unstable();
        let mut orbital_collection_name_keys: Vec<Vec<u8>> =
            orbital_collection_name_rows.keys().cloned().collect();
        orbital_collection_name_keys.sort_unstable();
        let mut holders_index_keys: Vec<Vec<u8>> = holders_index_rows.into_iter().collect();
        holders_index_keys.sort_unstable();
        let mut creation_count_row: Option<[u8; 8]> = None;
        if new_creations_added > 0 {
            creation_count_row = Some(next_creation_seq.to_le_bytes());
        }

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for k in &kv_keys {
            if let Some(v) = kv_rows.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &dir_keys {
            puts.push((k.clone(), Vec::new()));
        }
        for k in &creation_keys_by_id {
            if let Some(v) = creation_rows_by_id.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &creation_keys_seq {
            if let Some(v) = creation_rows_seq.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &name_index_keys {
            if let Some(v) = name_index_rows.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &symbol_index_keys {
            if let Some(v) = symbol_index_rows.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &orbital_collection_name_keys {
            if let Some(v) = orbital_collection_name_rows.get(k) {
                puts.push((k.clone(), v.clone()));
            }
        }
        for k in &holders_index_keys {
            puts.push((k.clone(), Vec::new()));
        }
        puts.push((table.block_summary_key(block.height), block_summary_bytes));
        if let Some(count_bytes) = creation_count_row {
            puts.push((table.alkane_creation_count_key(), count_bytes.to_vec()));
        }
        if !creations_in_block.is_empty() {
            puts.push((
                table.alkane_creations_in_block_key(block.height),
                borsh::to_vec(&creations_in_block)?,
            ));
        }

        debug::log_elapsed(module, "prepare_batch", timer);
        let timer = debug::start_if(debug);
        if let Err(e) = provider.set_batch(crate::modules::essentials::storage::SetBatchParams {
            blockhash: StateAt::Latest,
            puts,
            deletes: Vec::new(),
        }) {
            eprintln!("[ESSENTIALS] bulk_write failed at block #{}: {e}", block.height);
            return Err(e.into());
        }
        cache_block_summary(block.height, block_summary);

        debug::log_elapsed(module, "write_batch", timer);
        // ✅ also update alkane balances/holders for this block
        let timer = debug::start_if(debug);
        if debug {
            eprintln!(
                "[debug] module={} section=update_balances.start height={} txs={}",
                module,
                block.height,
                block.transactions.len()
            );
        }
        if let Err(e) = bulk_update_balances_for_block(provider, &block) {
            eprintln!(
                "[ESSENTIALS] bulk_update_balances_for_block failed at block #{}: {e}",
                block.height
            );
            return Err(e);
        }
        debug::log_elapsed(module, "update_balances", timer);
        if debug {
            eprintln!(
                "[debug] module={} section=update_balances.done height={}",
                module, block.height
            );
        }

        let new_alkanes_saved = creation_rows_by_id.len();
        eprintln!(
            "[ESSENTIALS] block #{} indexed {} key/value updates (deduped); new alkanes {}",
            block.height, total_pairs_dedup, new_alkanes_saved
        );
        self.set_index_height(block.height, StateAt::Latest)?;
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        self.load_index_height().ok().flatten()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        rpc::register_rpc(
            reg.clone(),
            self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").clone(),
        );
    }
}
