use crate::config::get_electrum_client;
use crate::consts::alkanes_genesis_block;
use crate::modules::ammdata::consts::{AMM_CONTRACT, KEY_INDEX_HEIGHT, ammdata_genesis_block};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::{alkanes::trace::EspoBlock, consts::NETWORK};
use anyhow::{Result, anyhow};
use bitcoin::{Address, Network, Txid, hashes::Hash};
use borsh::{BorshDeserialize, BorshSerialize};
use electrum_client::ElectrumApi;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::rpc::register_rpc;
use super::schemas::{SchemaCandleV1, SchemaTradeV1};

fn collect_prevout_txids(block: &EspoBlock) -> BTreeSet<Txid> {
    let mut transaction_set = BTreeSet::new();
    for transaction in &block.transactions {
        for vin in &transaction.transaction.input {
            set.insert(vin.previous_output.txid);
        }
    }
    set
}

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
        if let Some(bytes) = self.mdb().get(KEY_INDEX_HEIGHT)? {
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

    fn persist_index_height(&self, h: u32) -> Result<()> {
        self.mdb()
            .put(KEY_INDEX_HEIGHT, &h.to_le_bytes())
            .map_err(|e| anyhow!("[AMMDATA] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_h: u32) -> Result<()> {
        if let Some(prev) = *self.index_height.read().unwrap() {
            if new_h < prev {
                return Ok(());
            }
        }
        self.persist_index_height(new_h)?;
        *self.index_height.write().unwrap() = Some(new_h);
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

        eprintln!("[AMMDATA] indexing block #{} (ts={})", block.height, block_ts);

        // 1) Batch prevouts for trader address heuristics
        let prev_txids = collect_prevout_txids(&block);
        let client = get_electrum_client();
        let raw_txs: Vec<Vec<u8>> = client
            .batch_transaction_get_raw(&uniq_txids)
            .context("electrum batch_transaction_get_raw failed")?;
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
