use anyhow::{Context, Result, anyhow};
use bitcoin::Network;
use borsh::BorshDeserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::alkanes::trace::{EspoBlock, EspoSandshrewLikeTraceEvent};
use crate::modules::ammdata::consts::ammdata_genesis_block;
use crate::modules::ammdata::schemas::active_timeframes;
use crate::modules::ammdata::utils::candles::{price_base_per_quote, price_quote_per_base};
use crate::modules::ammdata::utils::reserves::{
    extract_new_pools_from_espo_transaction, extract_reserves_from_espo_transaction,
};
use crate::modules::ammdata::utils::trades::create_trade_v1;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;

use super::rpc::register_rpc;

/* ---------- helpers for /pools storage ---------- */

use crate::modules::ammdata::schemas::SchemaMarketDefs;

// Key format: /pools/{block:U32BE}{tx:U64BE}   (RELATIVE key)
fn pools_key(pool: &SchemaAlkaneId) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + 4 + 8);
    k.extend_from_slice(b"/pools/");
    k.extend_from_slice(&pool.block.to_be_bytes());
    k.extend_from_slice(&pool.tx.to_be_bytes());
    k
}

fn encode_defs(defs: &SchemaMarketDefs) -> Result<Vec<u8>> {
    borsh::to_vec(defs).context("borsh serialize SchemaMarketDefs")
}

fn decode_defs(bytes: &[u8]) -> Result<SchemaMarketDefs> {
    SchemaMarketDefs::try_from_slice(bytes).context("borsh deserialize SchemaMarketDefs")
}

fn parse_hex_u32(s: &str) -> Result<u32> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    Ok(u32::from_str_radix(s, 16)?)
}
fn parse_hex_u64(s: &str) -> Result<u64> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    Ok(u64::from_str_radix(s, 16)?)
}

fn schema_id_from_short_hex(block_hex: &str, tx_hex: &str) -> Result<SchemaAlkaneId> {
    Ok(SchemaAlkaneId { block: parse_hex_u32(block_hex)?, tx: parse_hex_u64(tx_hex)? })
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

    /// Scan a block to collect **all pools** we will touch:
    ///  - Pools created (Create events)
    ///  - Pools that serve getReserves (delegatecall 0x61)
    fn collect_pools_in_block(&self, block: &EspoBlock) -> HashSet<SchemaAlkaneId> {
        let mut set: HashSet<SchemaAlkaneId> = HashSet::new();

        for tx in &block.transactions {
            let trace = tx.sandshrew_trace.clone();

            // Created pools
            for ev in &trace.events {
                if let EspoSandshrewLikeTraceEvent::Create(c) = ev {
                    if let Ok(pool) =
                        schema_id_from_short_hex(&c.new_alkane.block, &c.new_alkane.tx)
                    {
                        set.insert(pool);
                    }
                }
            }

            // Pools responding to delegatecall 0x61 (used by reserve extractor anchors)
            for ev in &trace.events {
                if let EspoSandshrewLikeTraceEvent::Invoke(inv) = ev {
                    let is_get_reserves = inv.typ == "delegatecall"
                        && inv.context.inputs.len() == 1
                        && inv.context.inputs[0].as_str() == "0x61";
                    if is_get_reserves {
                        if let Ok(pool) = schema_id_from_short_hex(
                            &inv.context.myself.block,
                            &inv.context.myself.tx,
                        ) {
                            set.insert(pool);
                        }
                    }
                }
            }
        }

        set
    }

    /// Single read to load all /pools/* that we’ll need for this block.
    fn load_pools_map(
        &self,
        set: &HashSet<SchemaAlkaneId>,
    ) -> Result<HashMap<SchemaAlkaneId, SchemaMarketDefs>> {
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(set.len());
        let mut order: Vec<SchemaAlkaneId> = Vec::with_capacity(set.len());
        for id in set.iter() {
            keys.push(pools_key(id));
            order.push(*id);
        }

        // Prefer multi_get if exposed by your Mdb wrapper
        let values = self.mdb().multi_get(&keys)?; // Vec<Option<Vec<u8>>>

        let mut out = HashMap::with_capacity(order.len());
        for (i, opt) in values.into_iter().enumerate() {
            if let Some(buf) = opt {
                if let Ok(defs) = decode_defs(&buf) {
                    out.insert(order[i], defs);
                }
            }
        }
        Ok(out)
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
        let block_ts = block.block_header.time as u64; // unix seconds

        /* ---------- pre-scan pools & prefetch defs in one read ---------- */
        let pools_in_block = self.collect_pools_in_block(&block);
        let mut pools_map: HashMap<SchemaAlkaneId, SchemaMarketDefs> =
            self.load_pools_map(&pools_in_block)?;

        /* ---------- accumulators for this block ---------- */
        let mut candle_cache = crate::modules::ammdata::utils::candles::CandleCache::new();
        let mut trade_acc = crate::modules::ammdata::utils::trades::TradeWriteAcc::new();
        let mut index_acc = crate::modules::ammdata::utils::trades::TradeIndexAcc::new();

        // Buffer for new /pools/* upserts (RELATIVE keys)
        let mut new_pool_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for transaction in block.transactions.clone() {
            /* -------- NEW POOLS: detect, UPSERT, log -------- */
            if let Ok(new_pools) = extract_new_pools_from_espo_transaction(&transaction) {
                for (_pid, defs) in new_pools {
                    let pool = defs.pool_alkane_id;
                    let base = defs.base_alkane_id;
                    let quote = defs.quote_alkane_id;

                    // Upsert to /pools/{SchemaAlkaneId} (borsh)
                    let k_rel = pools_key(&pool);
                    let v = encode_defs(&defs)?;
                    new_pool_writes.push((k_rel, v));

                    // Update in-memory map so later txs in this same block can use it
                    pools_map.insert(pool, defs);

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

            /* -------- TRADES / RESERVE UPDATES -------- */
            match extract_reserves_from_espo_transaction(&transaction, &pools_map) {
                Ok(reserve_data_vec) => {
                    if reserve_data_vec.is_empty() {
                        continue;
                    }

                    for rd in reserve_data_vec {
                        // Pool id (hex -> decimal) just for logging + keys
                        let pool_block_dec =
                            u32::from_str_radix(rd.pool.block.trim_start_matches("0x"), 16)
                                .unwrap_or_default();
                        let pool_tx_dec =
                            u64::from_str_radix(rd.pool.tx.trim_start_matches("0x"), 16)
                                .unwrap_or_default();
                        let pool_schema = SchemaAlkaneId { block: pool_block_dec, tx: pool_tx_dec };
                        let pool_id_dec = format!("{}:{}", pool_block_dec, pool_tx_dec);

                        // Reserves (base, quote) oriented by extractor
                        let (prev_base, prev_quote) = rd.prev_reserves;
                        let (new_base, new_quote) = rd.new_reserves;

                        // Prices derived from NEW reserves
                        let p_q_per_b = price_quote_per_base(new_base, new_quote); // quote/base
                        let p_b_per_q = price_base_per_quote(new_base, new_quote); // base/quote

                        // Volumes from extractor (base_in, quote_out)
                        let (vol_base_in, vol_quote_out) = rd.volume;

                        // ----- accumulate CANDLES into the block-level cache -----
                        candle_cache.apply_trade_for_frames(
                            block_ts,
                            pool_schema,
                            &active_timeframes(),
                            p_b_per_q,
                            p_q_per_b,
                            vol_base_in,
                            vol_quote_out,
                        );

                        // ----- build & queue TRADE into the block-level trade accumulator -----
                        if let Some(full_trade) =
                            create_trade_v1(block_ts, &block.prevouts, &transaction, &rd)
                        {
                            // per (pool, ts) sequencing handled inside TradeWriteAcc
                            if let Ok(seq) =
                                trade_acc.push(pool_schema, block_ts, full_trade.clone())
                            {
                                // queue secondary index entries using same (pool, ts, seq)
                                index_acc.add(&pool_schema, block_ts, seq, &full_trade);
                            }
                        }

                        // ----- OPTIONAL LOGGING -----
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
                            "[AMMDATA] Trade detected in pool {pool} @ block #{blk}, ts={ts}\n\
                             [AMMDATA]   Tokens: base={base_id} | quote={quote_id}\n\
                             [AMMDATA]   Δ: [base={d_base}, quote={d_quote}]\n\
                             [AMMDATA]   Reserves: prev[base={p0}, quote={p1}] -> new[base={n0}, quote={n1}] | K≈{k}\n\
                             [AMMDATA]   Volume: base_in={vbin}, quote_out={vqout}\n\
                             [AMMDATA]   Prices (from NEW reserves): base→quote={pbq} | quote→base={pqb}",
                            pool = pool_id_dec,
                            blk = block.height,
                            ts = block_ts,
                            base_id =
                                format!("{}/{}", rd.token_ids.base.block, rd.token_ids.base.tx),
                            quote_id =
                                format!("{}/{}", rd.token_ids.quote.block, rd.token_ids.quote.tx),
                            d_base = d_base,
                            d_quote = d_quote,
                            p0 = prev_base,
                            p1 = prev_quote,
                            n0 = new_base,
                            n1 = new_quote,
                            k = k_ratio_str,
                            vbin = vol_base_in,
                            vqout = vol_quote_out,
                            pbq = p_bq_str,
                            pqb = p_qb_str
                        );
                    }
                }
                Err(_) => continue,
            }
        }

        /* ---------- one atomic DB write (candles + trades + indexes + new pools) ---------- */
        let candle_writes = candle_cache.into_writes(self.mdb())?; // likely FULL keys
        let trade_writes = trade_acc.into_writes(); // RELATIVE keys
        let idx_delta = index_acc.clone().per_pool_delta(); // counts per pool
        let mut index_writes = index_acc.into_writes(); // RELATIVE keys

        // Update per-pool index counts (RELATIVE)
        let mut count_updates = 0usize;
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
            count_updates += 1;
        }

        // Stats + sample key preview before writing
        let c_cnt = candle_writes.len();
        let t_cnt = trade_writes.len();
        let i_cnt = index_writes.len();
        let p_cnt = new_pool_writes.len();

        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, trades={t_cnt}, indexes+counts={i_cnt}, new_pools={p_cnt} (count-updates={cu})",
            h = block.height,
            c_cnt = c_cnt,
            t_cnt = t_cnt,
            i_cnt = i_cnt,
            p_cnt = p_cnt,
            cu = count_updates
        );

        // Persist atomically; ALWAYS pass RELATIVE keys to wb.put (MdbBatch will prefix once).
        if !candle_writes.is_empty()
            || !trade_writes.is_empty()
            || !index_writes.is_empty()
            || !new_pool_writes.is_empty()
        {
            let db_prefix = self.mdb().prefix().to_vec(); // used just to strip if candles came FULL
            let _ = self.mdb().bulk_write(|wb| {
                // candles: if FULL, strip db prefix to make them RELATIVE before wb.put
                for (k_full_or_rel, v) in candle_writes.iter() {
                    let k_rel = if k_full_or_rel.starts_with(&db_prefix) {
                        &k_full_or_rel[db_prefix.len()..]
                    } else {
                        k_full_or_rel.as_slice()
                    };
                    wb.put(k_rel, v);
                }
                // trades: RELATIVE
                for (k_rel, v) in trade_writes.iter() {
                    wb.put(k_rel, v);
                }
                // indexes + counts: RELATIVE
                for (k_rel, v) in index_writes.iter() {
                    wb.put(k_rel, v);
                }
                // new pools: RELATIVE
                for (k_rel, v) in new_pool_writes.iter() {
                    wb.put(k_rel, v);
                }
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
