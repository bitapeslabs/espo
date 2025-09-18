use crate::alkanes::trace::EspoBlock;
use crate::modules::ammdata::consts::ammdata_genesis_block;
use crate::modules::ammdata::schemas::{SchemaAlkaneId, active_timeframes};
use crate::modules::ammdata::utils::reserves::extract_reserves_from_espo_transaction;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::runtime::mdb::Mdb;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use std::sync::Arc;

use super::rpc::register_rpc;

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
        let block_ts = block.block_header.time as u64; // unix seconds

        for transaction in block.transactions.clone() {
            // cache for this tx
            let mut tx_cache = crate::modules::ammdata::utils::candles::CandleCache::new();

            match extract_reserves_from_espo_transaction(&transaction) {
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
                        let p_q_per_b =
                            crate::modules::ammdata::utils::candles::price_quote_per_base(
                                new_base, new_quote,
                            ); // quote/base
                        let p_b_per_q =
                            crate::modules::ammdata::utils::candles::price_base_per_quote(
                                new_base, new_quote,
                            ); // base/quote

                        // Volumes from extractor (base_in, quote_out)
                        let (vol_base_in, vol_quote_out) = rd.volume;

                        // Token ids (for log pretty)
                        let base_id =
                            format!("{}/{}", rd.token_ids.base.block, rd.token_ids.base.tx);
                        let quote_id =
                            format!("{}/{}", rd.token_ids.quote.block, rd.token_ids.quote.tx);

                        // K ratio (pretty)
                        let k_ratio_str = rd
                            .k_ratio_approx
                            .map(|r| format!("{:.6}", r))
                            .unwrap_or_else(|| "n/a".into());

                        // Pretty prices (floating log only)
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

                        // ----- accumulate into RAM for all active timeframes -----
                        tx_cache.apply_trade_for_frames(
                            block_ts,
                            pool_schema,
                            &active_timeframes(),
                            p_b_per_q,
                            p_q_per_b,
                            vol_base_in,
                            vol_quote_out,
                        );

                        // ----- log the trade -----
                        let d_base = (new_base as i128) - (prev_base as i128);
                        let d_quote = (new_quote as i128) - (prev_quote as i128);
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
                            base_id = base_id,
                            quote_id = quote_id,
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

                    // ----- flush this tx atomically (merge with DB) -----
                    let writes = tx_cache.into_writes(self.mdb())?;
                    if !writes.is_empty() {
                        let _ = self.mdb().bulk_write(|wb| {
                            for (k, v) in writes {
                                wb.put(&k, &v);
                            }
                        });
                    }
                }
                Err(_) => continue,
            }
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
