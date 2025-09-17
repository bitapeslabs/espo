use crate::alkanes::trace::EspoBlock;
use crate::modules::ammdata::consts::{KEY_INDEX_HEIGHT, ammdata_genesis_block, get_amm_contract};
use crate::modules::ammdata::utils::extract_reserves_from_espo_transaction;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::runtime::mdb::Mdb;
use anyhow::{Result, anyhow};
use bitcoin::{Network, transaction};
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

        for transaction in block.transactions {
            match extract_reserves_from_espo_transaction(&transaction) {
                Ok(reserve_data) => {
                    // Convert pool to decimal for matching/logging
                    let pool_block_str = reserve_data.pool.block.trim_start_matches("0x");
                    let pool_tx_hex = reserve_data.pool.tx.trim_start_matches("0x");
                    let pool_tx_dec = u32::from_str_radix(pool_tx_hex, 16).unwrap_or_default();

                    // Only log pool 2:68441
                    if pool_block_str == "2" && pool_tx_dec == 68441 {
                        let pool_id_dec = format!("{}:{}", pool_block_str, pool_tx_dec);

                        let prev0 = reserve_data.prev_reserves.0;
                        let prev1 = reserve_data.prev_reserves.1;
                        let new0 = reserve_data.new_reserves.0;
                        let new1 = reserve_data.new_reserves.1;

                        // Deltas (signed)
                        let d0 = (new0 as i128) - (prev0 as i128);
                        let d1 = (new1 as i128) - (prev1 as i128);

                        // Prices from NEW reserves (token0 is "base", token1 is "quote")
                        let price_base_to_quote: Option<f64> =
                            if new0 != 0 { Some((new1 as f64) / (new0 as f64)) } else { None };
                        let price_quote_to_base: Option<f64> =
                            if new1 != 0 { Some((new0 as f64) / (new1 as f64)) } else { None };

                        // Pretty strings
                        let token0_id = format!(
                            "{}/{}",
                            reserve_data.token_ids[0].block, reserve_data.token_ids[0].tx
                        );
                        let token1_id = format!(
                            "{}/{}",
                            reserve_data.token_ids[1].block, reserve_data.token_ids[1].tx
                        );

                        let k_ratio_str = reserve_data
                            .k_ratio_approx
                            .map(|r| format!("{:.6}", r))
                            .unwrap_or_else(|| "n/a".into());

                        let p_bq_str = price_base_to_quote
                            .map(|p| format!("{:.12}", p))
                            .unwrap_or_else(|| "n/a".into());
                        let p_qb_str = price_quote_to_base
                            .map(|p| format!("{:.12}", p))
                            .unwrap_or_else(|| "n/a".into());

                        println!(
                            "[AMMDATA] Trade detected in pool {pool} @ block #{blk}, ts={ts} \
                        → Mapping: {map}\n\
                        [AMMDATA]   Tokens: [{t0}, {t1}] \
                        | Δ: [token0={d0}, token1={d1}]\n\
                        [AMMDATA]   Reserves: prev[{p0}, {p1}] -> new[{n0}, {n1}] \
                        | K≈{k}\n\
                        [AMMDATA]   Prices (from NEW reserves): \
                        base→quote={pbq} | quote→base={pqb}",
                            pool = pool_id_dec,
                            blk = block.height,
                            ts = block_ts,
                            map = reserve_data.mapping,
                            t0 = token0_id,
                            t1 = token1_id,
                            d0 = d0,
                            d1 = d1,
                            p0 = prev0,
                            p1 = prev1,
                            n0 = new0,
                            n1 = new1,
                            k = k_ratio_str,
                            pbq = p_bq_str,
                            pqb = p_qb_str
                        );
                    }
                }
                Err(_) => continue,
            }
        }

        //eprintln!("[AMMDATA] finished indexing block #{} (ts={})", block.height, block_ts);
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
