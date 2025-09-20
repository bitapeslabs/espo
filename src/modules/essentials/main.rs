use crate::alkanes::trace::EspoBlock;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::consts::essentials_genesis_block;
use crate::modules::essentials::rpc;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use bitcoin::hashes::Hash;
use std::sync::Arc;

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
                return Ok(());
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
        let mut total_pairs = 0usize;

        // Pure write-only path: for every (alk, key)->value change
        //   - put value row
        //   - put directory marker row
        let _ = mdb.bulk_write(|wb| {
            for tx in block.transactions.iter() {
                for (alk, kvs) in tx.storage_changes.iter() {
                    for (skey, (txid, value)) in kvs.iter() {
                        let k_kv = Essentials::k_kv(alk, skey);

                        // EXPECT: `txid` is 32 bytes (raw)
                        // Value layout: [ txid (32) | value (...) ]
                        let mut buf = Vec::with_capacity(32 + value.len());
                        buf.extend_from_slice(&txid.to_byte_array());
                        buf.extend_from_slice(value);

                        wb.put(&k_kv, &buf);

                        // dir marker: no read/merge; duplicates are fine
                        let k_dir = Essentials::k_dir_entry(alk, skey);
                        wb.put(&k_dir, &[]);

                        // dir marker: no read/merge; duplicates are fine
                        let k_dir = Essentials::k_dir_entry(alk, skey);
                        wb.put(&k_dir, &[]);

                        total_pairs += 1;
                    }
                }
            }
        });

        eprintln!("[ESSENTIALS] block #{} indexed {} key/value updates", block.height, total_pairs);
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
