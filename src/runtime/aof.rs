use anyhow::{Context, Result};
use bitcoin::BlockHash;
use hex::FromHex;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

/// Number of blocks we keep in the AOF window and the rollback depth for reorg protection.
pub const AOF_REORG_DEPTH: u32 = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AofChange {
    pub namespace: String,
    pub key_hex: String,
    pub before_hex: Option<String>,
    pub after_hex: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockLog {
    pub height: u32,
    pub block_hash: String,
    pub updates: Vec<AofChange>,
}

#[derive(Default)]
struct BlockState {
    current_height: Option<u32>,
    block_hash: Option<String>,
    updates: Vec<AofChange>,
    // map of key bytes -> index into updates (first occurrence wins for ordering)
    seen: HashMap<Vec<u8>, usize>,
}

#[derive(Clone)]
pub struct AofManager {
    db: Arc<DB>,
    log_db: Arc<DB>,
    depth: u32,
    state: Arc<Mutex<BlockState>>,
}

impl AofManager {
    pub fn new(db: Arc<DB>, path: impl AsRef<Path>, depth: u32) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            fs::create_dir_all(path)
                .with_context(|| format!("failed to create AOF directory {}", path.display()))?;
        } else if !path.is_dir() {
            anyhow::bail!("AOF path {} is not a directory", path.display());
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        let log_db = Arc::new(DB::open(&opts, path)?);

        let mgr =
            Self { db, log_db, depth, state: Arc::new(Mutex::new(BlockState::default())) };
        // Clean up any files beyond our retention window on startup.
        mgr.prune_old(None)?;
        Ok(mgr)
    }

    pub fn start_block(&self, height: u32, hash: &BlockHash) {
        let mut st = self.state.lock().expect("aof state poisoned");
        st.current_height = Some(height);
        st.block_hash = Some(hash.to_string());
        st.updates.clear();
        st.seen.clear();
    }

    pub fn finish_block(&self) -> Result<()> {
        let mut st = self.state.lock().expect("aof state poisoned");
        let height = match st.current_height {
            Some(h) => h,
            None => return Ok(()), // nothing to persist
        };
        let block_hash = st.block_hash.clone().unwrap_or_default();
        let updates = st.updates.clone();
        st.current_height = None;
        st.block_hash = None;
        st.updates.clear();
        st.seen.clear();
        drop(st);

        let entry = BlockLog { height, block_hash, updates };
        self.persist_block_log(&entry)?;
        self.prune_old(Some(height))?;
        Ok(())
    }

    pub fn record_put(
        &self,
        namespace: &str,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Vec<u8>,
    ) {
        self.record_change(namespace, key, before, Some(after));
    }

    pub fn record_delete(&self, namespace: &str, key: Vec<u8>, before: Option<Vec<u8>>) {
        self.record_change(namespace, key, before, None);
    }

    fn record_change(
        &self,
        namespace: &str,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    ) {
        let mut st = self.state.lock().expect("aof state poisoned");
        if st.current_height.is_none() {
            return; // ignore writes outside of a block context
        }

        let idx = if let Some(idx) = st.seen.get(&key) {
            *idx
        } else {
            let idx = st.updates.len();
            st.seen.insert(key.clone(), idx);
            st.updates.push(AofChange {
                namespace: namespace.to_string(),
                key_hex: hex::encode(&key),
                before_hex: before.as_ref().map(hex::encode),
                after_hex: after.as_ref().map(hex::encode),
            });
            return;
        };

        // Update existing record: preserve the first "before", update "after".
        if let Some(change) = st.updates.get_mut(idx) {
            if change.before_hex.is_none() {
                change.before_hex = before.as_ref().map(hex::encode);
            }
            change.after_hex = after.as_ref().map(hex::encode);
        }
    }

    fn block_key(height: u32) -> [u8; 5] {
        let mut buf = [0u8; 5];
        buf[0] = b'b';
        buf[1..].copy_from_slice(&height.to_be_bytes());
        buf
    }

    fn decode_height(key: &[u8]) -> Option<u32> {
        if key.len() != 5 || key[0] != b'b' {
            return None;
        }
        let mut h = [0u8; 4];
        h.copy_from_slice(&key[1..]);
        Some(u32::from_be_bytes(h))
    }

    fn persist_block_log(&self, log: &BlockLog) -> Result<()> {
        let data = serde_json::to_vec(log)?;
        self.log_db.put(Self::block_key(log.height), data)?;
        // Durability is important for reorg protection; flush best-effort.
        self.log_db.flush()?;
        Ok(())
    }

    fn list_block_heights(&self) -> Result<Vec<u32>> {
        let mut heights = Vec::new();
        for res in self.log_db.iterator(IteratorMode::Start) {
            let (k, _) = res?;
            if let Some(h) = Self::decode_height(&k) {
                heights.push(h);
            }
        }
        heights.sort_unstable();
        Ok(heights)
    }

    fn prune_old(&self, newest_height: Option<u32>) -> Result<()> {
        let mut heights = self.list_block_heights()?;
        if heights.is_empty() {
            return Ok(());
        }

        let anchor = newest_height.unwrap_or_else(|| *heights.iter().max().unwrap_or(&0));
        let keep_from = anchor.saturating_sub(self.depth.saturating_sub(1));

        let mut wb = WriteBatch::default();
        let mut delete_count = 0usize;
        for h in heights.drain(..) {
            if h < keep_from {
                wb.delete(Self::block_key(h));
                delete_count += 1;
            }
        }
        if delete_count > 0 {
            self.log_db.write(wb)?;
        }
        Ok(())
    }

    fn load_blocks_desc(&self, limit: Option<usize>) -> Result<Vec<BlockLog>> {
        let mut logs = Vec::new();
        for res in self.log_db.iterator(IteratorMode::End) {
            let (k, v) = res?;
            if Self::decode_height(&k).is_none() {
                continue;
            }
            let log: BlockLog = serde_json::from_slice(&v)?;
            logs.push(log);
            if let Some(max) = limit {
                if logs.len() >= max {
                    break;
                }
            }
        }
        Ok(logs)
    }

    pub fn recent_blocks(&self, limit: usize) -> Result<Vec<BlockLog>> {
        self.load_blocks_desc(Some(limit))
    }

    pub fn revert_last_blocks(&self, count: usize) -> Result<Option<u32>> {
        let logs = self.load_blocks_desc(Some(count))?;
        if logs.is_empty() {
            return Ok(None);
        }
        let last = logs.last().map(|l| l.height);
        self.apply_revert_logs(logs)?;
        Ok(last)
    }

    /// Revert every block currently tracked by the AOF (latest → earliest).
    /// Returns the earliest height reverted, if any.
    pub fn revert_all_blocks(&self) -> Result<Option<u32>> {
        let logs = self.load_blocks_desc(None)?;
        if logs.is_empty() {
            return Ok(None);
        }
        let earliest = logs.last().map(|l| l.height);
        self.apply_revert_logs(logs)?;
        Ok(earliest)
    }

    fn apply_revert_logs(&self, logs: Vec<BlockLog>) -> Result<()> {
        for log in logs {
            for change in log.updates.iter().rev() {
                let key = Vec::from_hex(&change.key_hex)
                    .with_context(|| format!("invalid hex key in AOF for block {}", log.height))?;

                match change.before_hex.as_ref().and_then(|s| Vec::from_hex(s).ok()) {
                    Some(prev) => {
                        self.db.put(&key, prev)?;
                    }
                    None => {
                        self.db.delete(&key)?;
                    }
                }
            }
        }
        Ok(())
    }
}
