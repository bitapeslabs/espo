use anyhow::{Context, Result};
use bitcoin::BlockHash;
use hex::FromHex;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
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
    path: PathBuf,
    depth: u32,
    state: Arc<Mutex<BlockState>>,
}

impl AofManager {
    pub fn new(db: Arc<DB>, path: impl AsRef<Path>, depth: u32) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create AOF directory {}", path.display()))?;
        } else if !path.is_dir() {
            anyhow::bail!("AOF path {} is not a directory", path.display());
        }

        let mgr = Self { db, path, depth, state: Arc::new(Mutex::new(BlockState::default())) };
        // Clean up any files beyond our retention window on startup.
        mgr.prune_old(None)?;
        Ok(mgr)
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
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

    fn persist_block_log(&self, log: &BlockLog) -> Result<()> {
        let data = serde_json::to_vec(log)?;
        let tmp = self.path.join(format!("{}.json.tmp", log.height));
        let final_path = self.block_path(log.height);

        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&data)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_path)
            .with_context(|| format!("failed to move AOF tmp {} -> {}", tmp.display(), final_path.display()))?;
        Ok(())
    }

    fn block_path(&self, height: u32) -> PathBuf {
        self.path.join(format!("{}.json", height))
    }

    fn list_block_files(&self) -> Result<Vec<u32>> {
        let mut heights = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if let Some(stripped) = name.strip_suffix(".json") {
                    if let Ok(h) = stripped.parse::<u32>() {
                        heights.push(h);
                    }
                }
            }
        }
        heights.sort_unstable();
        Ok(heights)
    }

    fn prune_old(&self, newest_height: Option<u32>) -> Result<()> {
        let mut heights = self.list_block_files()?;
        if heights.is_empty() {
            return Ok(());
        }

        let anchor = if let Some(h) = newest_height {
            h
        } else {
            *heights.iter().max().unwrap_or(&0)
        };
        let keep_from = anchor.saturating_sub(self.depth.saturating_sub(1));

        for h in heights.drain(..) {
            if h < keep_from {
                let _ = fs::remove_file(self.block_path(h));
            }
        }
        Ok(())
    }

    pub fn recent_blocks(&self, limit: usize) -> Result<Vec<BlockLog>> {
        let mut heights = self.list_block_files()?;
        heights.sort_unstable();
        heights.reverse();
        let mut out = Vec::new();
        for h in heights.into_iter().take(limit) {
            let data = fs::read(self.block_path(h))?;
            let log: BlockLog = serde_json::from_slice(&data)?;
            out.push(log);
        }
        Ok(out)
    }

    pub fn revert_last_blocks(&self, count: usize) -> Result<Option<u32>> {
        let logs = self.recent_blocks(count)?;
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
        let heights = self.list_block_files()?;
        if heights.is_empty() {
            return Ok(None);
        }
        let earliest = *heights.iter().min().unwrap_or(&0);
        let mut logs: Vec<BlockLog> = Vec::new();
        for h in heights.iter().rev() {
            let data = fs::read(self.block_path(*h))?;
            let log: BlockLog = serde_json::from_slice(&data)?;
            logs.push(log);
        }
        self.apply_revert_logs(logs)?;
        Ok(Some(earliest))
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
