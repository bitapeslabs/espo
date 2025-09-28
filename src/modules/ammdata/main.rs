use super::schemas::{SchemaPoolSnapshot, SchemaReservesSnapshot};
use super::utils::candles::CandleCache;
use super::utils::trades::{TradeIndexAcc, TradeWriteAcc};
use crate::alkanes::trace::EspoBlock;

use crate::config::get_electrum_client;
use crate::modules::ammdata::consts::ammdata_genesis_block;
use crate::modules::ammdata::schemas::{SchemaMarketDefs, active_timeframes};
use crate::modules::ammdata::utils::candles::{price_base_per_quote, price_quote_per_base};
use crate::modules::ammdata::utils::reserves::{
    extract_new_pools_from_espo_transaction, extract_reserves_from_espo_transaction,
};
use crate::modules::ammdata::utils::trades::create_trade_v1;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use borsh::BorshDeserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::rpc::register_rpc;
// NEW: live reserves util
use crate::modules::ammdata::utils::live_reserves::fetch_latest_reserves_for_pools;

// Relative RocksDB key holding the entire reserves snapshot (BORSH)
#[inline]
fn reserves_snapshot_key() -> &'static [u8] {
    b"/reserves_snapshot_v1" // v1 as agreed
}

// Encode Snapshot -> BORSH (deterministic order via BTreeMap)
fn encode_reserves_snapshot(map: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>) -> Result<Vec<u8>> {
    let ordered: BTreeMap<SchemaAlkaneId, SchemaPoolSnapshot> =
        map.iter().map(|(k, v)| (*k, v.clone())).collect();
    let snap = SchemaReservesSnapshot { entries: ordered };
    Ok(borsh::to_vec(&snap)?)
}

// Decode BORSH -> Snapshot
fn decode_reserves_snapshot(bytes: &[u8]) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let snap = SchemaReservesSnapshot::try_from_slice(bytes)?;
    Ok(snap.entries.into_iter().collect())
}

/* ---------- module ---------- */

pub struct AmmData {
    mdb: Option<Arc<Mdb>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl AmmData {
    pub fn new() -> Self {
        Self { mdb: None, index_height: Arc::new(std::sync::RwLock::new(None)) }
    }

    #[inline]
    fn mdb(&self) -> &Mdb {
        self.mdb.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        if let Some(bytes) = self.mdb().get(AmmData::get_key_index_height())? {
            if bytes.len() != 4 {
                return Err(anyhow!("invalid /index_height length {}", bytes.len()));
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
            .put(AmmData::get_key_index_height(), &height.to_le_bytes())
            .map_err(|e| anyhow!("[AMMDATA] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32) -> Result<()> {
        if let Some(prev) = *self.index_height.read().unwrap() {
            if new_height < prev {
                return Ok(());
            }
        }
        self.persist_index_height(new_height)?;
        *self.index_height.write().unwrap() = Some(new_height);
        Ok(())
    }
}

impl Default for AmmData {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for AmmData {
    fn get_name(&self) -> &'static str {
        "ammdata"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        self.mdb = Some(mdb.clone());
        match self.load_index_height() {
            Ok(h) => {
                *self.index_height.write().unwrap() = h;
                eprintln!("[AMMDATA] loaded index height: {:?}", h);
            }
            Err(e) => eprintln!("[AMMDATA] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        ammdata_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        use bitcoin::consensus::deserialize;
        use bitcoin::{Transaction, Txid};
        use electrum_client::ElectrumApi;
        use std::collections::{HashMap, HashSet};

        let block_ts = block.block_header.time as u64;
        let height = block.height;
        println!("[AMMDATA] Indexing block #{height} for candles and trades...");

        // ---- Load existing snapshot (single read) ----
        let mut reserves_snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
            if let Some(bytes) = self.mdb().get(reserves_snapshot_key())? {
                match decode_reserves_snapshot(&bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("[AMMDATA] WARNING: failed to decode reserves snapshot: {e:?}");
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };

        // Build pools map the extractors expect
        let mut pools_map: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
        for (pool, snap) in reserves_snapshot.iter() {
            pools_map.insert(
                *pool,
                SchemaMarketDefs {
                    pool_alkane_id: *pool,
                    base_alkane_id: snap.base_id,
                    quote_alkane_id: snap.quote_id,
                },
            );
        }

        // Accumulators used in PASS 2
        let mut candle_cache = CandleCache::new();
        let mut trade_acc = TradeWriteAcc::new();
        let mut index_acc = TradeIndexAcc::new();
        let mut reserves_map_delta: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = HashMap::new();

        // -----------------------------------------------------------------------------------------
        // PASS 1: discover new pools and collect prevout txids ONLY for txs with reserve changes
        // -----------------------------------------------------------------------------------------
        let mut need_prevouts_for_txids: HashSet<Txid> = HashSet::new();

        for transaction in block.transactions.iter() {
            // skip tx without traces
            if transaction.traces.is_none() {
                continue;
            }

            // Discover newly created pools in this tx
            if let Ok(new_pools) = extract_new_pools_from_espo_transaction(transaction) {
                for (_pid, defs) in new_pools {
                    let pool = defs.pool_alkane_id;
                    let base = defs.base_alkane_id;
                    let quote = defs.quote_alkane_id;

                    // extend maps/snapshot so later reserve events can resolve ids
                    pools_map.insert(pool, defs);
                    reserves_snapshot.entry(pool).or_insert(SchemaPoolSnapshot {
                        base_reserve: 0,
                        quote_reserve: 0,
                        base_id: base,
                        quote_id: quote,
                    });

                    println!(
                        "[AMMDATA] New pool created @ block #{blk}, ts={ts}\n\
                     [AMMDATA]   Pool:  {pb}:{pt}\n\
                     [AMMDATA]   Base:  {bb}:{bt}\n\
                     [AMMDATA]   Quote: {qb}:{qt}",
                        blk = block.height,
                        ts = block_ts,
                        pb = pool.block,
                        pt = pool.tx,
                        bb = base.block,
                        bt = base.tx,
                        qb = quote.block,
                        qt = quote.tx
                    );
                }
            }

            // Mark this tx's prevout txids only if it actually has reserve changes
            match extract_reserves_from_espo_transaction(transaction, &pools_map) {
                Ok(reserve_data_vec) if !reserve_data_vec.is_empty() => {
                    let tx = &transaction.transaction;
                    for inp in &tx.input {
                        if inp.previous_output.is_null() {
                            continue;
                        } // coinbase
                        need_prevouts_for_txids.insert(inp.previous_output.txid);
                    }
                }
                _ => {}
            }
        }

        // -----------------------------------------------------------------------------------------
        // One Electrum batch for the subset of prevout txids we actually need
        // -----------------------------------------------------------------------------------------
        let mut prevouts_subset: HashMap<Txid, Transaction> = HashMap::new();
        if !need_prevouts_for_txids.is_empty() {
            eprintln!(
                "[AMMDATA] fetching prevouts subset via electrum: {} txids",
                need_prevouts_for_txids.len()
            );

            let electrum = get_electrum_client();
            let ids: Vec<Txid> = need_prevouts_for_txids.iter().copied().collect();

            // Try batch first (raw, faster), then granular fallbacks for empties or errors.
            match electrum.batch_transaction_get_raw(&ids) {
                Ok(raws) => {
                    for (i, raw) in raws.into_iter().enumerate() {
                        let txid = ids[i];
                        if raw.is_empty() {
                            // fallback to single
                            match electrum.transaction_get_raw(&txid) {
                                Ok(raw2) if !raw2.is_empty() => {
                                    if let Ok(tx) = deserialize::<Transaction>(&raw2) {
                                        prevouts_subset.insert(txid, tx);
                                    } else {
                                        eprintln!(
                                            "[AMMDATA] WARN: failed to deserialize prevout {txid} (single fallback)"
                                        );
                                    }
                                }
                                _ => {
                                    eprintln!("[AMMDATA] WARN: electrum missing prevout {txid}");
                                }
                            }
                        } else if let Ok(tx) = deserialize::<Transaction>(&raw) {
                            prevouts_subset.insert(txid, tx);
                        } else {
                            eprintln!(
                                "[AMMDATA] WARN: failed to deserialize prevout {txid} (batch)"
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[AMMDATA] WARN: electrum batch_transaction_get_raw failed: {e:?}; falling back per-tx"
                    );
                    for txid in ids {
                        match electrum.transaction_get_raw(&txid) {
                            Ok(raw) if !raw.is_empty() => {
                                if let Ok(tx) = deserialize::<Transaction>(&raw) {
                                    prevouts_subset.insert(txid, tx);
                                } else {
                                    eprintln!(
                                        "[AMMDATA] WARN: failed to deserialize prevout {txid} (per-tx fallback)"
                                    );
                                }
                            }
                            _ => eprintln!("[AMMDATA] WARN: electrum missing prevout {txid}"),
                        }
                    }
                }
            }
        } else {
            eprintln!("[AMMDATA] no reserve-change txs; skipping prevouts fetch");
        }

        // -----------------------------------------------------------------------------------------
        // PASS 2: candles + trades + reserves deltas (use prevouts_subset)
        // -----------------------------------------------------------------------------------------
        for transaction in block.transactions.iter() {
            if transaction.traces.is_none() {
                continue;
            }

            match extract_reserves_from_espo_transaction(transaction, &pools_map) {
                Ok(reserve_data_vec) if !reserve_data_vec.is_empty() => {
                    for rd in reserve_data_vec {
                        let pool_block_dec =
                            u32::from_str_radix(rd.pool.block.trim_start_matches("0x"), 16)
                                .unwrap_or_default();
                        let pool_tx_dec =
                            u64::from_str_radix(rd.pool.tx.trim_start_matches("0x"), 16)
                                .unwrap_or_default();

                        let pool_schema = SchemaAlkaneId { block: pool_block_dec, tx: pool_tx_dec };

                        // Update snapshot delta (event-derived)
                        if let Some(defs) = pools_map.get(&pool_schema) {
                            reserves_map_delta.insert(
                                pool_schema,
                                SchemaPoolSnapshot {
                                    base_reserve: rd.new_reserves.0,
                                    quote_reserve: rd.new_reserves.1,
                                    base_id: defs.base_alkane_id,
                                    quote_id: defs.quote_alkane_id,
                                },
                            );
                        }

                        // Candles
                        let (prev_base, prev_quote) = rd.prev_reserves;
                        let (new_base, new_quote) = rd.new_reserves;
                        let p_q_per_b = price_quote_per_base(new_base, new_quote);
                        let p_b_per_q = price_base_per_quote(new_base, new_quote);
                        let (vol_base_in, vol_quote_out) = rd.volume;

                        candle_cache.apply_trade_for_frames(
                            block_ts,
                            pool_schema,
                            &active_timeframes(),
                            p_b_per_q,
                            p_q_per_b,
                            vol_base_in,
                            vol_quote_out,
                        );

                        // Trades using the prevouts subset we fetched in Pass 1
                        if let Some(full_trade) =
                            create_trade_v1(block_ts, &prevouts_subset, transaction, &rd)
                        {
                            if let Ok(seq) =
                                trade_acc.push(pool_schema, block_ts, full_trade.clone())
                            {
                                index_acc.add(&pool_schema, block_ts, seq, &full_trade);
                            }
                        }

                        // Pretty logs
                        let d_base = (new_base as i128) - (prev_base as i128);
                        let d_quote = (new_quote as i128) - (prev_quote as i128);
                        let k_ratio_str = rd
                            .k_ratio_approx
                            .map(|r| format!("{:.6}", r))
                            .unwrap_or_else(|| "n/a".into());
                        let p_bq_str = if new_quote != 0 {
                            format!("{:.12}", (new_base as f64) / (new_quote as f64))
                        } else {
                            "n/a".into()
                        };
                        let p_qb_str = if new_base != 0 {
                            format!("{:.12}", (new_quote as f64) / (new_base as f64))
                        } else {
                            "n/a".into()
                        };

                        println!(
                            "[AMMDATA] Trade detected in pool {pool}:{tx} @ block #{blk}, ts={ts}\n\
                         [AMMDATA]   Δ: [base={d_base}, quote={d_quote}]  prev[{p0},{p1}] -> new[{n0},{n1}] | K≈{k}\n\
                         [AMMDATA]   Prices: base→quote={pbq} | quote→base={pqb}  Volume: base_in={vbin}, quote_out={vqout}",
                            pool = pool_block_dec,
                            tx = pool_tx_dec,
                            blk = block.height,
                            ts = block_ts,
                            d_base = d_base,
                            d_quote = d_quote,
                            p0 = prev_base,
                            p1 = prev_quote,
                            n0 = new_base,
                            n1 = new_quote,
                            k = k_ratio_str,
                            pbq = p_bq_str,
                            pqb = p_qb_str,
                            vbin = vol_base_in,
                            vqout = vol_quote_out
                        );
                    }
                }
                _ => {}
            }
        }

        // Tip override: pull live reserves and merge
        if block.is_latest {
            eprintln!("[AMMDATA] is_latest=true -> fetching live reserves from Metashrew…");
            match fetch_latest_reserves_for_pools(&pools_map) {
                Ok(live) => {
                    for (pool, snap) in live {
                        reserves_snapshot.insert(pool, snap.clone());
                        reserves_map_delta.insert(pool, snap);
                    }
                    eprintln!("[AMMDATA] live reserves applied for {} pools", pools_map.len());
                }
                Err(e) => {
                    eprintln!(
                        "[AMMDATA] live reserves fetch failed: {e:?} (keeping event-derived state)"
                    );
                }
            }
        }

        // ---------- one atomic DB write (candles + trades + indexes + reserves snapshot) ----------
        let candle_writes = candle_cache.into_writes(self.mdb())?;
        let trade_writes = trade_acc.into_writes();
        let idx_delta = index_acc.clone().per_pool_delta();
        let mut index_writes = index_acc.into_writes();

        // Update per-pool index counts
        for ((blk_id, tx_id), delta) in idx_delta {
            let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
            let count_k_rel = crate::modules::ammdata::utils::trades::idx_count_key(&pool);

            let current = if let Some(v) = self.mdb().get(&count_k_rel)? {
                crate::modules::ammdata::utils::trades::decode_u64_be(&v).unwrap_or(0)
            } else {
                0u64
            };
            let newv = current.saturating_add(delta);

            index_writes.push((
                count_k_rel,
                crate::modules::ammdata::utils::trades::encode_u64_be(newv).to_vec(),
            ));
        }

        // Merge deltas into snapshot & serialize
        for (pool, latest) in reserves_map_delta.into_iter() {
            reserves_snapshot.insert(pool, latest);
        }
        let reserves_blob = encode_reserves_snapshot(&reserves_snapshot)?;
        let reserves_key_rel = reserves_snapshot_key();

        let c_cnt = candle_writes.len();
        let t_cnt = trade_writes.len();
        let i_cnt = index_writes.len();

        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, trades={t_cnt}, indexes+counts={i_cnt}, reserves_snapshot=1",
            h = block.height,
            c_cnt = c_cnt,
            t_cnt = t_cnt,
            i_cnt = i_cnt,
        );

        if !candle_writes.is_empty()
            || !trade_writes.is_empty()
            || !index_writes.is_empty()
            || !reserves_blob.is_empty()
        {
            let db_prefix = self.mdb().prefix().to_vec(); // strip if candles were FULL
            let _ = self.mdb().bulk_write(|wb| {
                for (k_full_or_rel, v) in candle_writes.iter() {
                    let k_rel = if k_full_or_rel.starts_with(&db_prefix) {
                        &k_full_or_rel[db_prefix.len()..]
                    } else {
                        k_full_or_rel.as_slice()
                    };
                    wb.put(k_rel, v);
                }
                for (k_rel, v) in trade_writes.iter() {
                    wb.put(k_rel, v);
                }
                for (k_rel, v) in index_writes.iter() {
                    wb.put(k_rel, v);
                }
                wb.put(reserves_key_rel, &reserves_blob);
            });
        }

        println!(
            "[AMMDATA] Finished processing block #{} with {} traces",
            block.height,
            block.transactions.len()
        );
        self.set_index_height(block.height)?;
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let mdb = self.mdb().clone();
        register_rpc(reg, mdb);
    }
}
