use crate::alkanes::trace::EspoBlock;
use crate::config::{get_metashrew, get_network};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::consts::essentials_genesis_block;
use crate::modules::essentials::rpc;
use crate::modules::essentials::storage::{
    alkane_creation_by_id_key, alkane_creation_count_key, alkane_creation_ordered_key,
    encode_creation_record, load_creation_record, trace_count_key,
};
use crate::modules::essentials::utils::inspections::{
    created_alkane_records_from_block, inspect_wasm_metadata, AlkaneCreationRecord,
};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use bitcoin::hashes::Hash;
use std::sync::Arc;

// ✅ bring in balances bulk updater
use crate::modules::essentials::utils::balances::bulk_update_balances_for_block;

pub struct Essentials {
    mdb: Option<Arc<Mdb>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl Essentials {
    pub fn new() -> Self {
        Self { mdb: None, index_height: Arc::new(std::sync::RwLock::new(None)) }
    }

    #[inline]
    fn mdb(&self) -> &Mdb {
        self.mdb.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        if let Some(bytes) = self.mdb().get(Essentials::k_index_height())? {
            if bytes.len() != 4 {
                return Err(anyhow!("[ESSENTIALS] invalid /index_height length {}", bytes.len()));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&bytes);
            Ok(Some(u32::from_le_bytes(arr)))
        } else {
            Ok(None)
        }
    }

    fn persist_index_height(&self, height: u32) -> Result<()> {
        self.mdb()
            .put(Essentials::k_index_height(), &height.to_le_bytes())
            .map_err(|e| anyhow!("[ESSENTIALS] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32) -> Result<()> {
        if let Some(prev) = *self.index_height.read().unwrap() {
            if new_height < prev {
                eprintln!(
                    "[ESSENTIALS] index height rollback detected ({} -> {})",
                    prev, new_height
                );
            }
        }
        self.persist_index_height(new_height)?;
        *self.index_height.write().unwrap() = Some(new_height);
        Ok(())
    }

    /* ---------------- key helpers (RELATIVE KEYS) ---------------- */

    #[inline]
    pub(crate) fn k_index_height() -> &'static [u8] {
        b"/index_height"
    }

    /// Value row:
    ///   0x01 | block_be(4) | tx_be(8) | key_len_be(2) | key_bytes  ->  value_bytes
    #[inline]
    pub(crate) fn k_kv(alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
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

    /// Directory marker row (idempotent; duplicates ok):
    ///   0x03 | block_be(4) | tx_be(8) | key_len_be(2) | key_bytes  ->  []
    #[inline]
    pub(crate) fn k_dir_entry(alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + 4 + 8 + 2 + skey.len());
        v.push(0x03);
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
        self.mdb = Some(mdb.clone());
        match self.load_index_height() {
            Ok(h) => {
                *self.index_height.write().unwrap() = h;
                eprintln!("[ESSENTIALS] loaded index height: {:?}", h);
            }
            Err(e) => eprintln!("[ESSENTIALS] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        essentials_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let mdb = self.mdb();

        // -------- Phase A: coalesce per-block writes in memory --------
        // last-write-wins for values:
        //   k_kv(alk,skey) -> [ txid(32) | value(...) ]
        use std::collections::{HashMap, HashSet};

        let mut kv_rows: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        // dedup directory markers:
        //   k_dir_entry(alk,skey) -> ()
        let mut dir_rows: HashSet<Vec<u8>> = HashSet::new();
        // creation records rows (by id and by ordered key):
        let mut creation_rows_by_id: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut creation_rows_ordered: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        // in-block name/symbol updates detected from storage writes
        let mut meta_updates: HashMap<SchemaAlkaneId, (Vec<String>, Vec<String>)> =
            HashMap::new();
        // trace count row
        let trace_count = block
            .transactions
            .iter()
            .map(|tx| tx.traces.as_ref().map(|t| t.len()).unwrap_or(0))
            .sum::<usize>() as u32;

        let mut total_pairs_dedup = 0usize;

        for tx in block.transactions.iter() {
            // Access traces without cloning big structures if possible
            let storage_changes_iter = tx
                .traces
                .as_ref()
                .map(|traces| traces.iter())
                .into_iter()
                .flatten()
                .flat_map(|trace| trace.storage_changes.iter());

            for (alk, kvs) in storage_changes_iter {
                for (skey, (txid, value)) in kvs.iter() {
                    // Key for value row
                    let k_v = Essentials::k_kv(alk, skey);

                    // Value layout: [ txid(32) | value(...) ]
                    let mut buf = Vec::with_capacity(32 + value.len());
                    buf.extend_from_slice(&txid.to_byte_array());
                    buf.extend_from_slice(value);

                    // last-write-wins for this key within the block
                    kv_rows.insert(k_v, buf);

                    // Dir entry is idempotent; one per (alk, skey) is enough
                    let k_dir = Essentials::k_dir_entry(alk, skey);
                    dir_rows.insert(k_dir);

                    // Track name/symbol updates for the alkane (append if new)
                    let push_if_new = |map: &mut HashMap<SchemaAlkaneId, (Vec<String>, Vec<String>)>,
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
                    }

                    total_pairs_dedup += 1;
                }
            }
        }

        let mut created_records = created_alkane_records_from_block(&block);
        // Special case: ensure genesis alkane (2:0) is inspected on the genesis block even if no trace emits a create.
        let genesis_height = essentials_genesis_block(get_network());
        if block.height == genesis_height {
            let genesis = SchemaAlkaneId { block: 2, tx: 0 };
            let has_genesis = created_records.iter().any(|r| r.alkane == genesis);
            if !has_genesis {
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
                created_records.push(AlkaneCreationRecord {
                    alkane: genesis,
                    txid: txid_bytes,
                    creation_height: block.height,
                    creation_timestamp: block.block_header.time,
                    tx_index_in_block: 0,
                    inspection: None,
                    names: vec!["DIESEL".to_string()],
                    symbols: vec!["diesel".to_string()],
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

        let metashrew = get_metashrew();
        for rec in created_records.iter_mut() {
            match metashrew.get_alkane_wasm_bytes(&rec.alkane) {
                Ok(Some((wasm_bytes, factory_id))) => {
                    match inspect_wasm_metadata(&rec.alkane, &wasm_bytes, factory_id) {
                        Ok(record) => {
                            rec.inspection = Some(record);
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

        // Dedup against existing records to avoid double-counting if re-run.
        let mut new_creations_added: u64 = 0;
        if !created_records.is_empty() {
            let id_keys: Vec<Vec<u8>> =
                created_records.iter().map(|r| alkane_creation_by_id_key(&r.alkane)).collect();
            let existing = mdb.multi_get(&id_keys)?;

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
                if let Some(bytes) = already {
                    // If we already have a record, upgrade inspection/name/symbol when present.
                    match crate::modules::essentials::storage::decode_creation_record(bytes) {
                        Ok(prev) => {
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
                                creation_rows_by_id.insert(key_id.clone(), encoded_updated.clone());
                                let key_ord = alkane_creation_ordered_key(
                                    updated.creation_timestamp,
                                    updated.creation_height,
                                    updated.tx_index_in_block,
                                    &updated.alkane,
                                );
                                creation_rows_ordered.insert(key_ord, encoded_updated);
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "[ESSENTIALS] failed to decode existing creation record for {}:{}: {e}",
                                rec.alkane.block, rec.alkane.tx
                            );
                        }
                    }
                    continue;
                }

                new_creations_added += 1;
                creation_rows_by_id.insert(key_id.clone(), encoded.clone());
                let key_ord = alkane_creation_ordered_key(
                    rec.creation_timestamp,
                    rec.creation_height,
                    rec.tx_index_in_block,
                    &rec.alkane,
                );
                creation_rows_ordered.insert(key_ord, encoded);
            }
        }

        // Also update name/symbol for alkanes that had metadata writes in this block but were not newly created.
        for (alk, (names, symbols)) in meta_updates.iter() {
            let key_id = alkane_creation_by_id_key(alk);
            if creation_rows_by_id.contains_key(&key_id) {
                continue; // already updated via creation path above
            }
            let Some(mut rec) = load_creation_record(mdb, alk).ok().flatten() else {
                continue;
            };
            let mut dirty = false;
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
                dirty = true;
            }
            if maybe_push(&mut rec.symbols, symbols) {
                dirty = true;
            }
            if !dirty {
                continue;
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
            creation_rows_by_id.insert(key_id, encoded.clone());
            let key_ord = alkane_creation_ordered_key(
                rec.creation_timestamp,
                rec.creation_height,
                rec.tx_index_in_block,
                &rec.alkane,
            );
            creation_rows_ordered.insert(key_ord, encoded);
        }

        // If there’s nothing to write (typical empty block), skip the write-batch
        if !kv_rows.is_empty()
            || !dir_rows.is_empty()
            || !creation_rows_by_id.is_empty()
            || !creation_rows_ordered.is_empty()
        {
            // -------- Phase B: write in sorted key order (better LSM locality) --------
            let mut kv_keys: Vec<Vec<u8>> = kv_rows.keys().cloned().collect();
            kv_keys.sort_unstable();
            let mut dir_keys: Vec<Vec<u8>> = dir_rows.into_iter().collect();
            dir_keys.sort_unstable();
            let mut creation_keys_by_id: Vec<Vec<u8>> =
                creation_rows_by_id.keys().cloned().collect();
            creation_keys_by_id.sort_unstable();
            let mut creation_keys_ordered: Vec<Vec<u8>> =
                creation_rows_ordered.keys().cloned().collect();
            creation_keys_ordered.sort_unstable();
            let mut creation_count_row: Option<[u8; 8]> = None;
            if new_creations_added > 0 {
                let current = mdb
                    .get(alkane_creation_count_key())
                    .ok()
                    .flatten()
                    .and_then(|b| {
                        if b.len() == 8 {
                            let mut arr = [0u8; 8];
                            arr.copy_from_slice(&b);
                            Some(u64::from_le_bytes(arr))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let updated = current.saturating_add(new_creations_added);
                creation_count_row = Some(updated.to_le_bytes());
            }

            if let Err(e) = mdb.bulk_write(|wb: &mut MdbBatch<'_>| {
                // Values first
                for k in &kv_keys {
                    if let Some(v) = kv_rows.get(k) {
                        wb.put(k, v);
                    }
                }
                // Then directory markers
                for k in &dir_keys {
                    wb.put(k, &[]);
                }
                for k in &creation_keys_by_id {
                    if let Some(v) = creation_rows_by_id.get(k) {
                        wb.put(k, v);
                    }
                }
                for k in &creation_keys_ordered {
                    if let Some(v) = creation_rows_ordered.get(k) {
                        wb.put(k, v);
                    }
                }
                wb.put(&trace_count_key(block.height), &trace_count.to_le_bytes());
                if let Some(count_bytes) = creation_count_row {
                    wb.put(alkane_creation_count_key(), &count_bytes);
                }
            }) {
                eprintln!("[ESSENTIALS] bulk_write failed at block #{}: {e}", block.height);
                return Err(e.into());
            }
        }

        // ✅ also update alkane balances/holders for this block
        if let Err(e) = bulk_update_balances_for_block(mdb, &block) {
            eprintln!(
                "[ESSENTIALS] bulk_update_balances_for_block failed at block #{}: {e}",
                block.height
            );
            return Err(e);
        }

        let new_alkanes_saved = creation_rows_by_id.len();
        eprintln!(
            "[ESSENTIALS] block #{} indexed {} key/value updates (deduped); new alkanes {}",
            block.height, total_pairs_dedup, new_alkanes_saved
        );
        self.set_index_height(block.height)?;
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        rpc::register_rpc(reg.clone(), self.mdb().clone());
    }
}
