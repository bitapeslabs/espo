use bitcoin::BlockHash;
use bitcoin::hashes::{Hash as _, sha256};
use borsh::{BorshDeserialize, BorshSerialize};
use rocksdb::{DB, Direction, Error as RocksError, IteratorMode, WriteBatch};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

// Internal keyspace for the persistent Merkle B+Tree.
const ROOT_PREFIX: &[u8] = b"__espo_bptree:";
const NODE_PREFIX: &[u8] = b"__espo_bptree:node:";
const META_ACTIVE_ROOT: &[u8] = b"__espo_bptree:meta:active_root";
const META_ACTIVE_BLOCK: &[u8] = b"__espo_bptree:meta:active_block";
const META_PINNED_ROOT: &[u8] = b"__espo_bptree:meta:pinned_root";
const META_PIN_UNTIL_HEIGHT: &[u8] = b"__espo_bptree:meta:pin_until_height";
const BLOCK_ROOT_PREFIX: &[u8] = b"__espo_bptree:block:";
const HEIGHT_BLOCK_PREFIX: &[u8] = b"__espo_bptree:height:";
const BLOCK_HEIGHT_PREFIX: &[u8] = b"__espo_bptree:block_height:";
const BLOCK_PARENT_PREFIX: &[u8] = b"__espo_bptree:parent:";

// Fixed fanout parameters (deterministic split at half).
const MAX_LEAF_ENTRIES: usize = 128;
const MAX_INTERNAL_KEYS: usize = 128;
// Legacy fixed-page size kept only for backward-compatible decoding.
const NODE_PAGE_BYTES: usize = 16 * 1024;
// Batch in-memory node cache controls (OOM protection under very large blocks).
const BATCH_PENDING_GC_INTERVAL: usize = 1024;
const BATCH_PENDING_SOFT_LIMIT: usize = 25_000;
const BATCH_PENDING_SOFT_GC_INTERVAL: usize = 2048;
const BATCH_PENDING_HARD_LIMIT: usize = 50_000;

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct LeafEntry {
    key: Vec<u8>,
    // Legacy Optional payload to decode historical tombstone-based nodes.
    value: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Eq, Default, BorshSerialize, BorshDeserialize)]
struct LeafNode {
    entries: Vec<LeafEntry>,
    next: Option<[u8; 32]>,
}

#[derive(Clone, PartialEq, Eq, Default, BorshSerialize, BorshDeserialize)]
struct InternalNode {
    // Separator keys: first key in each right child.
    keys: Vec<Vec<u8>>,
    // children.len() = keys.len() + 1
    children: Vec<[u8; 32]>,
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
enum BptreeNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

enum MutationOutcome {
    Node([u8; 32]),
    Split { left: [u8; 32], right: [u8; 32], separator: Vec<u8> },
}

struct MutationResult {
    outcome: MutationOutcome,
}

#[derive(Clone, Copy)]
struct BlockContext {
    height: u32,
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    working_root: [u8; 32],
}

struct TreeState {
    active_root: [u8; 32],
    active_block: Option<[u8; 32]>,
    pinned_root: Option<[u8; 32]>,
    pin_until_height: Option<u32>,
    current_block: Option<BlockContext>,
}

#[derive(Default)]
struct BatchWriteContext {
    pending_nodes: HashMap<[u8; 32], BptreeNode>,
    ops_since_gc: usize,
}

impl Default for TreeState {
    fn default() -> Self {
        Self {
            active_root: empty_root_id(),
            active_block: None,
            pinned_root: None,
            pin_until_height: None,
            current_block: None,
        }
    }
}

pub struct VersionedTreeDb {
    db: Arc<DB>,
    state: RwLock<TreeState>,
}

static TREE_DB: OnceLock<Arc<VersionedTreeDb>> = OnceLock::new();

pub fn init_global_tree_db(db: Arc<DB>) -> Result<(), RocksError> {
    let tree = Arc::new(VersionedTreeDb::new(db)?);
    let _ = TREE_DB.set(tree);
    Ok(())
}

pub fn get_global_tree_db() -> Option<Arc<VersionedTreeDb>> {
    TREE_DB.get().cloned()
}

fn hash_node(node: &BptreeNode) -> [u8; 32] {
    let encoded = borsh::to_vec(node).expect("b+tree node serialization must succeed");
    sha256::Hash::hash(&encoded).to_byte_array()
}

fn encode_node_page(node: &BptreeNode) -> Vec<u8> {
    // Store canonical variable-sized payload. Fixed page sizing is now a split heuristic only.
    borsh::to_vec(node).expect("b+tree node serialization must succeed")
}

fn decode_node_page(bytes: &[u8]) -> Option<BptreeNode> {
    if let Ok(node) = BptreeNode::try_from_slice(bytes) {
        return Some(node);
    }
    if bytes.len() == NODE_PAGE_BYTES {
        if bytes.len() < 4 {
            return None;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&bytes[..4]);
        let payload_len = u32::from_be_bytes(len_bytes) as usize;
        if payload_len + 4 > NODE_PAGE_BYTES {
            return None;
        }
        return BptreeNode::try_from_slice(&bytes[4..4 + payload_len]).ok();
    }
    None
}

fn empty_root_id() -> [u8; 32] {
    hash_node(&BptreeNode::Leaf(LeafNode::default()))
}

fn node_key(id: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(NODE_PREFIX.len() + id.len());
    out.extend_from_slice(NODE_PREFIX);
    out.extend_from_slice(id);
    out
}

fn block_root_key(hash: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BLOCK_ROOT_PREFIX.len() + hash.len());
    out.extend_from_slice(BLOCK_ROOT_PREFIX);
    out.extend_from_slice(hash);
    out
}

fn height_block_key(height: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEIGHT_BLOCK_PREFIX.len() + 4);
    out.extend_from_slice(HEIGHT_BLOCK_PREFIX);
    out.extend_from_slice(&height.to_be_bytes());
    out
}

fn block_height_key(block_hash: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BLOCK_HEIGHT_PREFIX.len() + 32);
    out.extend_from_slice(BLOCK_HEIGHT_PREFIX);
    out.extend_from_slice(block_hash);
    out
}

fn block_parent_key(block_hash: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BLOCK_PARENT_PREFIX.len() + 32);
    out.extend_from_slice(BLOCK_PARENT_PREFIX);
    out.extend_from_slice(block_hash);
    out
}

fn decode_height_block_key(key: &[u8]) -> Option<u32> {
    if key.len() != HEIGHT_BLOCK_PREFIX.len() + 4 || !key.starts_with(HEIGHT_BLOCK_PREFIX) {
        return None;
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&key[HEIGHT_BLOCK_PREFIX.len()..]);
    Some(u32::from_be_bytes(arr))
}

fn child_index_for_key(keys: &[Vec<u8>], key: &[u8]) -> usize {
    // Upper bound: first separator > key.
    let mut lo = 0usize;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if key < keys[mid].as_slice() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

fn prefix_end_exclusive(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] != 0xFF {
            end[i] = end[i].saturating_add(1);
            end.truncate(i + 1);
            return Some(end);
        }
    }
    None
}

impl VersionedTreeDb {
    pub fn new(db: Arc<DB>) -> Result<Self, RocksError> {
        let empty = BptreeNode::Leaf(LeafNode::default());
        let empty_id = hash_node(&empty);
        let empty_key = node_key(&empty_id);
        if db.get(&empty_key)?.is_none() {
            db.put(empty_key, encode_node_page(&empty))?;
        }

        let mut state = TreeState::default();

        if let Some(bytes) = db.get(META_ACTIVE_ROOT)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.active_root = arr;
            }
        }
        if let Some(bytes) = db.get(META_ACTIVE_BLOCK)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.active_block = Some(arr);
            }
        }
        if let Some(bytes) = db.get(META_PINNED_ROOT)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.pinned_root = Some(arr);
            }
        }
        if let Some(bytes) = db.get(META_PIN_UNTIL_HEIGHT)? {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                state.pin_until_height = Some(u32::from_be_bytes(arr));
            }
        }

        Ok(Self { db, state: RwLock::new(state) })
    }

    pub fn begin_block(
        &self,
        height: u32,
        block_hash: &BlockHash,
        parent_hash: &BlockHash,
    ) -> Result<(), RocksError> {
        let parent_arr = parent_hash.to_byte_array();
        let base_root = self.root_for_blockhash_bytes(&parent_arr)?.unwrap_or_else(empty_root_id);

        let mut st = self.state.write().expect("tree state poisoned");
        st.current_block = Some(BlockContext {
            height,
            block_hash: block_hash.to_byte_array(),
            parent_hash: parent_arr,
            working_root: base_root,
        });
        Ok(())
    }

    pub fn finish_block(&self) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        let Some(ctx) = st.current_block.take() else {
            return Ok(());
        };

        let mut wb = WriteBatch::default();
        wb.put(block_root_key(&ctx.block_hash), ctx.working_root);
        wb.put(height_block_key(ctx.height), ctx.block_hash);
        wb.put(block_height_key(&ctx.block_hash), ctx.height.to_be_bytes());
        wb.put(block_parent_key(&ctx.block_hash), ctx.parent_hash);

        let mut should_switch_active = true;
        if let Some(until_height) = st.pin_until_height {
            if ctx.height <= until_height {
                should_switch_active = false;
            } else {
                st.pin_until_height = None;
                st.pinned_root = None;
                wb.delete(META_PIN_UNTIL_HEIGHT);
                wb.delete(META_PINNED_ROOT);
            }
        }

        if should_switch_active {
            st.active_root = ctx.working_root;
            st.active_block = Some(ctx.block_hash);
            wb.put(META_ACTIVE_ROOT, ctx.working_root);
            wb.put(META_ACTIVE_BLOCK, ctx.block_hash);
        } else {
            wb.put(META_ACTIVE_ROOT, st.active_root);
            if let Some(active_block) = st.active_block {
                wb.put(META_ACTIVE_BLOCK, active_block);
            }
        }

        self.db.write(wb)
    }

    pub fn abort_block(&self) {
        let mut st = self.state.write().expect("tree state poisoned");
        st.current_block = None;
    }

    pub fn pin_active_root_until_height(&self, until_height: u32) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        st.pinned_root = Some(st.active_root);
        st.pin_until_height = Some(until_height);

        let mut wb = WriteBatch::default();
        wb.put(META_PINNED_ROOT, st.active_root);
        wb.put(META_PIN_UNTIL_HEIGHT, until_height.to_be_bytes());
        self.db.write(wb)
    }

    pub fn blockhash_for_height(&self, height: u32) -> Result<Option<BlockHash>, RocksError> {
        let Some(bytes) = self.db.get(height_block_key(height))? else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(BlockHash::from_byte_array(arr)))
    }

    pub fn active_blockhash(&self) -> Option<BlockHash> {
        let st = self.state.read().expect("tree state poisoned");
        st.active_block.map(BlockHash::from_byte_array)
    }

    pub fn height_for_blockhash(&self, block_hash: &BlockHash) -> Result<Option<u32>, RocksError> {
        let Some(bytes) = self.db.get(block_height_key(&block_hash.to_byte_array()))? else {
            return Ok(None);
        };
        if bytes.len() != 4 {
            return Ok(None);
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(Some(u32::from_be_bytes(arr)))
    }

    pub fn parent_for_blockhash(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, RocksError> {
        let Some(bytes) = self.db.get(block_parent_key(&block_hash.to_byte_array()))? else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(BlockHash::from_byte_array(arr)))
    }

    pub fn is_ancestor(
        &self,
        ancestor: &BlockHash,
        descendant: &BlockHash,
    ) -> Result<bool, RocksError> {
        let Some(ancestor_height) = self.height_for_blockhash(ancestor)? else {
            return Ok(false);
        };
        let Some(mut current_height) = self.height_for_blockhash(descendant)? else {
            return Ok(false);
        };
        if ancestor_height > current_height {
            return Ok(false);
        }
        let mut cursor = *descendant;
        while current_height > ancestor_height {
            let Some(parent) = self.parent_for_blockhash(&cursor)? else {
                return Ok(false);
            };
            cursor = parent;
            current_height = current_height.saturating_sub(1);
        }
        Ok(cursor == *ancestor)
    }

    pub fn indexed_height_bounds(&self) -> Result<Option<(u32, u32)>, RocksError> {
        let mut first: Option<u32> = None;
        for res in self.db.iterator(IteratorMode::From(HEIGHT_BLOCK_PREFIX, Direction::Forward)) {
            let (key, _value) = res?;
            let key_ref = key.as_ref();
            if !key_ref.starts_with(HEIGHT_BLOCK_PREFIX) {
                break;
            }
            if let Some(height) = decode_height_block_key(key_ref) {
                first = Some(height);
                break;
            }
        }

        let Some(first_height) = first else {
            return Ok(None);
        };

        let mut last = first_height;
        if let Some(end_prefix) = prefix_end_exclusive(HEIGHT_BLOCK_PREFIX) {
            for res in self.db.iterator(IteratorMode::From(&end_prefix, Direction::Reverse)) {
                let (key, _value) = res?;
                let key_ref = key.as_ref();
                if !key_ref.starts_with(HEIGHT_BLOCK_PREFIX) {
                    if key_ref < HEIGHT_BLOCK_PREFIX {
                        break;
                    }
                    continue;
                }
                if let Some(height) = decode_height_block_key(key_ref) {
                    last = height;
                    break;
                }
            }
        }

        Ok(Some((first_height, last)))
    }

    pub fn active_root(&self) -> [u8; 32] {
        let st = self.state.read().expect("tree state poisoned");
        st.pinned_root.unwrap_or(st.active_root)
    }

    pub fn root_for_blockhash(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<[u8; 32]>, RocksError> {
        self.root_for_blockhash_bytes(&block_hash.to_byte_array())
    }

    fn root_for_blockhash_bytes(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<Option<[u8; 32]>, RocksError> {
        {
            let st = self.state.read().expect("tree state poisoned");
            if let Some(ctx) = st.current_block {
                if &ctx.block_hash == block_hash {
                    return Ok(Some(ctx.working_root));
                }
            }
        }
        let Some(bytes) = self.db.get(block_root_key(block_hash))? else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(arr))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let root = self.active_root();
        self.get_at_root(root, key)
    }

    pub fn get_at_root(&self, root: [u8; 32], key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let mut current = root;
        loop {
            match self.load_node(&current)? {
                BptreeNode::Leaf(leaf) => {
                    match leaf.entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
                        Ok(idx) => return Ok(leaf.entries[idx].value.clone()),
                        Err(_) => return Ok(None),
                    }
                }
                BptreeNode::Internal(internal) => {
                    if internal.children.is_empty() {
                        return Ok(None);
                    }
                    let idx = child_index_for_key(&internal.keys, key);
                    let Some(next) = internal.children.get(idx).copied() else {
                        return Ok(None);
                    };
                    current = next;
                }
            }
        }
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, RocksError> {
        let root = self.active_root();
        keys.iter().map(|k| self.get_at_root(root, k)).collect()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.apply_mutation(key, Some(value.to_vec()))
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), RocksError> {
        self.apply_mutation(key, None)
    }

    pub fn apply_batch(&self, changes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<(), RocksError> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut owned = Vec::with_capacity(changes.len());
        for (k, v) in changes {
            owned.push((k.clone(), v.clone()));
        }
        self.apply_batch_owned(owned)
    }

    pub fn apply_batch_owned(
        &self,
        mut changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<(), RocksError> {
        if changes.is_empty() {
            return Ok(());
        }

        // Canonicalize for deterministic structure with last-write-wins semantics:
        // stable sort by key, then keep only the final value for duplicate keys.
        changes.sort_by(|a, b| a.0.cmp(&b.0));
        let mut write = 0usize;
        for read in 0..changes.len() {
            if write > 0 && changes[write - 1].0 == changes[read].0 {
                let replacement = changes[read].1.take();
                changes[write - 1].1 = replacement;
                continue;
            }
            if write != read {
                changes.swap(write, read);
            }
            write += 1;
        }
        changes.truncate(write);

        let progress = std::env::var_os("ESPO_TREE_BATCH_PROGRESS").is_some();
        let total_ops = changes.len();
        if progress {
            eprintln!("[tree_db] apply_batch_owned: canonical_ops={total_ops}");
        }

        let mut batch_ctx = BatchWriteContext::default();
        let mut st = self.state.write().expect("tree state poisoned");
        if let Some(ctx) = st.current_block.as_mut() {
            let mut root = ctx.working_root;
            for (idx, (k, v)) in changes.into_iter().enumerate() {
                root = self.apply_single(root, &k, v, Some(&mut batch_ctx))?;
                self.maybe_compact_batch_nodes(root, &mut batch_ctx)?;
                if progress && (idx + 1) % 100_000 == 0 {
                    eprintln!(
                        "[tree_db] apply_batch_owned: progress={}/{} pending_nodes={}",
                        idx + 1,
                        total_ops,
                        batch_ctx.pending_nodes.len()
                    );
                }
            }
            self.flush_pending_batch_nodes(root, &batch_ctx)?;
            if progress {
                eprintln!(
                    "[tree_db] apply_batch_owned: flushed_pending_nodes={}",
                    batch_ctx.pending_nodes.len()
                );
            }
            ctx.working_root = root;
            return Ok(());
        }

        let mut root = st.active_root;
        for (idx, (k, v)) in changes.into_iter().enumerate() {
            root = self.apply_single(root, &k, v, Some(&mut batch_ctx))?;
            self.maybe_compact_batch_nodes(root, &mut batch_ctx)?;
            if progress && (idx + 1) % 100_000 == 0 {
                eprintln!(
                    "[tree_db] apply_batch_owned: progress={}/{} pending_nodes={}",
                    idx + 1,
                    total_ops,
                    batch_ctx.pending_nodes.len()
                );
            }
        }
        self.flush_pending_batch_nodes(root, &batch_ctx)?;
        if progress {
            eprintln!(
                "[tree_db] apply_batch_owned: flushed_pending_nodes={}",
                batch_ctx.pending_nodes.len()
            );
        }
        st.active_root = root;

        let mut wb = WriteBatch::default();
        wb.put(META_ACTIVE_ROOT, root);
        if let Some(active_block) = st.active_block {
            wb.put(META_ACTIVE_BLOCK, active_block);
        }
        self.db.write(wb)
    }

    pub fn collect_prefixed_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, RocksError> {
        let root = self.active_root();
        self.collect_prefixed_keys_at_root(root, prefix)
    }

    pub fn collect_prefixed_keys_at_root(
        &self,
        root: [u8; 32],
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksError> {
        let entries = self.collect_prefixed_entries_at_root(root, prefix)?;
        Ok(entries.into_iter().map(|(k, _)| k).collect())
    }

    pub fn collect_prefixed_entries(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let root = self.active_root();
        self.collect_prefixed_entries_at_root(root, prefix)
    }

    pub fn collect_prefixed_entries_at_root(
        &self,
        root: [u8; 32],
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let end = prefix_end_exclusive(prefix);
        let mut out = self.range_entries_at_root(root, prefix, end.as_deref())?;
        out.retain(|(k, _)| k.starts_with(prefix));
        Ok(out)
    }

    pub fn range_entries(
        &self,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let root = self.active_root();
        self.range_entries_at_root(root, start_inclusive, end_exclusive)
    }

    pub fn range_entries_at_root(
        &self,
        root: [u8; 32],
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let mut out = Vec::new();
        let Some((mut cursor, mut path)) = self.find_leaf_with_path(root, start_inclusive)? else {
            return Ok(out);
        };
        let mut first_leaf = true;

        loop {
            let leaf = self.load_leaf(&cursor)?;
            let start_idx = if first_leaf {
                leaf.entries.partition_point(|entry| entry.key.as_slice() < start_inclusive)
            } else {
                0
            };

            for entry in leaf.entries.iter().skip(start_idx) {
                if let Some(end) = end_exclusive {
                    if entry.key.as_slice() >= end {
                        return Ok(out);
                    }
                }
                if let Some(v) = &entry.value {
                    out.push((entry.key.clone(), v.clone()));
                }
            }

            first_leaf = false;
            let Some(next) = self.next_leaf_from_path(&mut path)? else {
                break;
            };
            cursor = next;
        }

        Ok(out)
    }

    fn apply_mutation(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        if let Some(ctx) = st.current_block.as_mut() {
            ctx.working_root = self.apply_single(ctx.working_root, key, value, None)?;
            return Ok(());
        }

        st.active_root = self.apply_single(st.active_root, key, value, None)?;
        let mut wb = WriteBatch::default();
        wb.put(META_ACTIVE_ROOT, st.active_root);
        if let Some(active_block) = st.active_block {
            wb.put(META_ACTIVE_BLOCK, active_block);
        }
        self.db.write(wb)
    }

    fn apply_single(
        &self,
        root: [u8; 32],
        key: &[u8],
        value: Option<Vec<u8>>,
        mut batch_ctx: Option<&mut BatchWriteContext>,
    ) -> Result<[u8; 32], RocksError> {
        let res = self.mutate_node(root, key, value, batch_ctx.as_deref_mut())?;
        let next_root = match res.outcome {
            MutationOutcome::Node(id) => id,
            MutationOutcome::Split { left, right, separator } => {
                let new_root = BptreeNode::Internal(InternalNode {
                    keys: vec![separator],
                    children: vec![left, right],
                });
                self.store_node_with_ctx(&new_root, batch_ctx.as_deref_mut())?
            }
        };

        Ok(next_root)
    }

    fn maybe_compact_batch_nodes(
        &self,
        root: [u8; 32],
        batch_ctx: &mut BatchWriteContext,
    ) -> Result<(), RocksError> {
        if batch_ctx.pending_nodes.is_empty() {
            return Ok(());
        }
        batch_ctx.ops_since_gc = batch_ctx.ops_since_gc.saturating_add(1);
        let over_soft = batch_ctx.pending_nodes.len() >= BATCH_PENDING_SOFT_LIMIT;
        let should_gc = batch_ctx.ops_since_gc >= BATCH_PENDING_GC_INTERVAL
            || (over_soft && batch_ctx.ops_since_gc >= BATCH_PENDING_SOFT_GC_INTERVAL);
        if should_gc {
            self.retain_reachable_pending_nodes(root, batch_ctx);
            batch_ctx.ops_since_gc = 0;
        }

        if batch_ctx.pending_nodes.len() >= BATCH_PENDING_HARD_LIMIT {
            // Safety valve: spill current reachable frontier to disk and reset in-memory cache.
            self.retain_reachable_pending_nodes(root, batch_ctx);
            self.flush_pending_batch_nodes(root, batch_ctx)?;
            batch_ctx.pending_nodes.clear();
            batch_ctx.ops_since_gc = 0;
        }
        Ok(())
    }

    fn retain_reachable_pending_nodes(&self, root: [u8; 32], batch_ctx: &mut BatchWriteContext) {
        if batch_ctx.pending_nodes.is_empty() {
            return;
        }

        let mut reachable: HashSet<[u8; 32]> = HashSet::new();
        let mut stack = vec![root];

        while let Some(id) = stack.pop() {
            if !reachable.insert(id) {
                continue;
            }
            let Some(node) = batch_ctx.pending_nodes.get(&id) else {
                continue;
            };
            if let BptreeNode::Internal(internal) = node {
                for child in &internal.children {
                    stack.push(*child);
                }
            }
        }

        if reachable.len() == batch_ctx.pending_nodes.len() {
            return;
        }
        batch_ctx.pending_nodes.retain(|id, _| reachable.contains(id));
    }

    fn flush_pending_batch_nodes(
        &self,
        root: [u8; 32],
        batch_ctx: &BatchWriteContext,
    ) -> Result<(), RocksError> {
        if batch_ctx.pending_nodes.is_empty() {
            return Ok(());
        }

        let mut visited: HashSet<[u8; 32]> = HashSet::new();
        let mut stack = vec![root];
        let mut wb = WriteBatch::default();
        let mut writes = 0usize;

        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            let Some(node) = batch_ctx.pending_nodes.get(&id) else {
                continue;
            };
            // Nodes are content-addressed by hash; unconditional put is idempotent and avoids
            // expensive per-node DB reads in large batches.
            wb.put(node_key(&id), encode_node_page(node));
            writes = writes.saturating_add(1);
            if let BptreeNode::Internal(internal) = node {
                for child in &internal.children {
                    stack.push(*child);
                }
            }
        }

        if writes == 0 {
            return Ok(());
        }

        self.db.write(wb)
    }

    fn mutate_node(
        &self,
        node_id: [u8; 32],
        key: &[u8],
        value: Option<Vec<u8>>,
        mut batch_ctx: Option<&mut BatchWriteContext>,
    ) -> Result<MutationResult, RocksError> {
        match self.load_node_with_ctx(&node_id, batch_ctx.as_deref())? {
            BptreeNode::Leaf(mut leaf) => {
                let search = leaf.entries.binary_search_by(|entry| entry.key.as_slice().cmp(key));

                let mut changed = false;
                match search {
                    Ok(idx) => {
                        match value {
                            Some(next) => {
                                if leaf.entries[idx].value.as_ref() != Some(&next) {
                                    leaf.entries[idx].value = Some(next);
                                    changed = true;
                                }
                            }
                            None => {
                                // COW versioning already preserves history, so deletes remove keys
                                // from the new version instead of accumulating tombstones forever.
                                leaf.entries.remove(idx);
                                changed = true;
                            }
                        }
                    }
                    Err(idx) => {
                        if let Some(next) = value {
                            leaf.entries
                                .insert(idx, LeafEntry { key: key.to_vec(), value: Some(next) });
                            changed = true;
                        }
                    }
                }

                if leaf.entries.iter().any(|entry| entry.value.is_none()) {
                    // Opportunistically compact legacy tombstones when this leaf is touched.
                    leaf.entries.retain(|entry| entry.value.is_some());
                    changed = true;
                }

                if !changed {
                    return Ok(MutationResult { outcome: MutationOutcome::Node(node_id) });
                }

                if leaf.entries.len() <= MAX_LEAF_ENTRIES {
                    let new_id = self
                        .store_node_with_ctx(&BptreeNode::Leaf(leaf), batch_ctx.as_deref_mut())?;
                    return Ok(MutationResult { outcome: MutationOutcome::Node(new_id) });
                }

                // Deterministic split: exact half.
                let mid = leaf.entries.len() / 2;
                let right_entries = leaf.entries.split_off(mid);
                let left_entries = leaf.entries;

                let separator = right_entries[0].key.clone();
                let old_next = leaf.next;

                let right_node =
                    BptreeNode::Leaf(LeafNode { entries: right_entries, next: old_next });
                let right_id = self.store_node_with_ctx(&right_node, batch_ctx.as_deref_mut())?;

                let left_node =
                    BptreeNode::Leaf(LeafNode { entries: left_entries, next: Some(right_id) });
                let left_id = self.store_node_with_ctx(&left_node, batch_ctx.as_deref_mut())?;

                Ok(MutationResult {
                    outcome: MutationOutcome::Split { left: left_id, right: right_id, separator },
                })
            }
            BptreeNode::Internal(mut internal) => {
                if internal.children.is_empty() {
                    return Ok(MutationResult { outcome: MutationOutcome::Node(node_id) });
                }

                let idx = child_index_for_key(&internal.keys, key);
                let Some(child_id) = internal.children.get(idx).copied() else {
                    return Ok(MutationResult { outcome: MutationOutcome::Node(node_id) });
                };

                let child = self.mutate_node(child_id, key, value, batch_ctx.as_deref_mut())?;

                match child.outcome {
                    MutationOutcome::Node(new_child) => {
                        if new_child == child_id {
                            return Ok(MutationResult { outcome: MutationOutcome::Node(node_id) });
                        }
                        internal.children[idx] = new_child;
                    }
                    MutationOutcome::Split { left, right, separator } => {
                        internal.children[idx] = left;
                        internal.keys.insert(idx, separator);
                        internal.children.insert(idx + 1, right);
                    }
                }

                if internal.keys.len() <= MAX_INTERNAL_KEYS {
                    let new_id = self.store_node_with_ctx(
                        &BptreeNode::Internal(internal),
                        batch_ctx.as_deref_mut(),
                    )?;
                    return Ok(MutationResult { outcome: MutationOutcome::Node(new_id) });
                }

                // Deterministic split: exact half.
                let mid = internal.keys.len() / 2;
                let separator = internal.keys[mid].clone();

                let left_keys = internal.keys[..mid].to_vec();
                let right_keys = internal.keys[mid + 1..].to_vec();

                let left_children = internal.children[..mid + 1].to_vec();
                let right_children = internal.children[mid + 1..].to_vec();

                let left_id = self.store_node_with_ctx(
                    &BptreeNode::Internal(InternalNode {
                        keys: left_keys,
                        children: left_children,
                    }),
                    batch_ctx.as_deref_mut(),
                )?;
                let right_id = self.store_node_with_ctx(
                    &BptreeNode::Internal(InternalNode {
                        keys: right_keys,
                        children: right_children,
                    }),
                    batch_ctx.as_deref_mut(),
                )?;

                Ok(MutationResult {
                    outcome: MutationOutcome::Split { left: left_id, right: right_id, separator },
                })
            }
        }
    }

    fn find_leaf_with_path(
        &self,
        root: [u8; 32],
        key: &[u8],
    ) -> Result<Option<([u8; 32], Vec<([u8; 32], usize)>)>, RocksError> {
        let mut path = Vec::new();
        let mut current = root;
        loop {
            match self.load_node(&current)? {
                BptreeNode::Leaf(_) => return Ok(Some((current, path))),
                BptreeNode::Internal(internal) => {
                    if internal.children.is_empty() {
                        return Ok(None);
                    }
                    let idx = child_index_for_key(&internal.keys, key);
                    let Some(next) = internal.children.get(idx).copied() else {
                        return Ok(None);
                    };
                    path.push((current, idx));
                    current = next;
                }
            }
        }
    }

    fn next_leaf_from_path(
        &self,
        path: &mut Vec<([u8; 32], usize)>,
    ) -> Result<Option<[u8; 32]>, RocksError> {
        while let Some((internal_id, child_idx)) = path.pop() {
            let internal = self.load_internal(&internal_id)?;
            let next_idx = child_idx + 1;
            if next_idx >= internal.children.len() {
                continue;
            }

            let mut node_id = internal.children[next_idx];
            path.push((internal_id, next_idx));
            loop {
                match self.load_node(&node_id)? {
                    BptreeNode::Leaf(_) => return Ok(Some(node_id)),
                    BptreeNode::Internal(next_internal) => {
                        if next_internal.children.is_empty() {
                            return Ok(None);
                        }
                        path.push((node_id, 0));
                        node_id = next_internal.children[0];
                    }
                }
            }
        }
        Ok(None)
    }

    fn load_leaf(&self, id: &[u8; 32]) -> Result<LeafNode, RocksError> {
        match self.load_node(id)? {
            BptreeNode::Leaf(leaf) => Ok(leaf),
            BptreeNode::Internal(_) => Ok(LeafNode::default()),
        }
    }

    fn load_internal(&self, id: &[u8; 32]) -> Result<InternalNode, RocksError> {
        match self.load_node(id)? {
            BptreeNode::Internal(node) => Ok(node),
            BptreeNode::Leaf(_) => Ok(InternalNode::default()),
        }
    }

    fn load_node(&self, id: &[u8; 32]) -> Result<BptreeNode, RocksError> {
        let key = node_key(id);
        let Some(bytes) = self.db.get(key)? else {
            return Ok(BptreeNode::Leaf(LeafNode::default()));
        };
        if let Some(node) = decode_node_page(&bytes) {
            return Ok(node);
        }
        Ok(BptreeNode::Leaf(LeafNode::default()))
    }

    fn load_node_with_ctx(
        &self,
        id: &[u8; 32],
        batch_ctx: Option<&BatchWriteContext>,
    ) -> Result<BptreeNode, RocksError> {
        if let Some(ctx) = batch_ctx {
            if let Some(node) = ctx.pending_nodes.get(id) {
                return Ok(node.clone());
            }
        }
        self.load_node(id)
    }

    fn store_node_with_ctx(
        &self,
        node: &BptreeNode,
        batch_ctx: Option<&mut BatchWriteContext>,
    ) -> Result<[u8; 32], RocksError> {
        let id = hash_node(node);
        if let Some(ctx) = batch_ctx {
            ctx.pending_nodes.entry(id).or_insert_with(|| node.clone());
            return Ok(id);
        }
        let key = node_key(&id);
        if self.db.get(&key)?.is_none() {
            self.db.put(key, encode_node_page(node))?;
        }
        Ok(id)
    }
}

pub fn is_tree_internal_key(full_key: &[u8]) -> bool {
    full_key.starts_with(ROOT_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_tree() -> (TempDir, VersionedTreeDb) {
        let dir = TempDir::new().expect("tempdir");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).expect("rocksdb open"));
        let tree = VersionedTreeDb::new(db).expect("tree init");
        (dir, tree)
    }

    #[test]
    fn prefix_lookup_works() {
        let (_dir, tree) = new_tree();
        let key = b"essentials:/address/v2/some-very-long-address/outpoint/txid:vout";
        let value = b"v1".to_vec();
        tree.apply_batch(&[(key.to_vec(), Some(value.clone()))]).expect("apply");

        let prefix = b"essentials:/address/v2/some-very-long-address/outpoint/";
        let entries = tree.collect_prefixed_entries(prefix).expect("collect");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, key.to_vec());
        assert_eq!(entries[0].1, value);
    }

    #[test]
    fn split_and_delete_keep_sibling() {
        let (_dir, tree) = new_tree();
        let k1 = b"abcde".to_vec();
        let k2 = b"abc".to_vec();
        tree.apply_batch(&[(k1.clone(), Some(vec![1]))]).expect("put k1");
        tree.apply_batch(&[(k2.clone(), Some(vec![2]))]).expect("put k2");

        assert_eq!(tree.get(&k1).expect("get k1"), Some(vec![1]));
        assert_eq!(tree.get(&k2).expect("get k2"), Some(vec![2]));

        tree.apply_batch(&[(k2.clone(), None)]).expect("delete k2");
        assert_eq!(tree.get(&k2).expect("get k2 after delete"), None);
        assert_eq!(tree.get(&k1).expect("get k1 after delete"), Some(vec![1]));
    }

    #[test]
    fn collect_entries_order_is_lexicographic() {
        let (_dir, tree) = new_tree();
        let keys = vec![b"a/2".to_vec(), b"a/10".to_vec(), b"a/1".to_vec(), b"a/z".to_vec()];
        let mut changes = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            changes.push((k.clone(), Some(vec![i as u8])));
        }
        tree.apply_batch(&changes).expect("apply");

        let entries = tree.collect_prefixed_entries(b"a/").expect("collect");
        let observed: Vec<Vec<u8>> = entries.into_iter().map(|(k, _)| k).collect();
        let mut sorted = observed.clone();
        sorted.sort();
        assert_eq!(observed, sorted);
    }

    #[test]
    fn range_entries_remain_correct_after_many_splits() {
        let (_dir, tree) = new_tree();

        let mut initial = Vec::new();
        for i in 0..900u32 {
            let key = format!("holders/{i:04}").into_bytes();
            let value = vec![(i % 251) as u8];
            initial.push((key, Some(value)));
        }
        tree.apply_batch(&initial).expect("seed");

        let mut updates = Vec::new();
        for i in (0..900u32).step_by(4) {
            updates.push((format!("holders/{i:04}").into_bytes(), None));
        }
        for i in 900..1200u32 {
            updates.push((format!("holders/{i:04}").into_bytes(), Some(vec![(i % 251) as u8])));
        }
        tree.apply_batch(&updates).expect("updates");

        let entries = tree.collect_prefixed_entries(b"holders/").expect("collect");
        assert!(!entries.is_empty());

        let observed_keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
        let mut sorted = observed_keys.clone();
        sorted.sort();
        assert_eq!(observed_keys, sorted);

        // 1200 total keys minus 225 deletes (every 4th key in 0..900).
        assert_eq!(entries.len(), 975);
    }

    #[test]
    fn block_roots_preserve_historical_reads() {
        let (_dir, tree) = new_tree();
        let key = b"essentials:/k";

        let genesis = BlockHash::from_byte_array([0u8; 32]);
        let h1 = BlockHash::from_byte_array([1u8; 32]);
        let h2 = BlockHash::from_byte_array([2u8; 32]);

        tree.begin_block(1, &h1, &genesis).expect("begin block 1");
        tree.apply_batch(&[(key.to_vec(), Some(vec![1]))]).expect("apply block 1");
        tree.finish_block().expect("finish block 1");

        tree.begin_block(2, &h2, &h1).expect("begin block 2");
        tree.apply_batch(&[(key.to_vec(), Some(vec![2]))]).expect("apply block 2");
        tree.finish_block().expect("finish block 2");

        let r1 = tree.root_for_blockhash(&h1).expect("root h1").expect("root exists");
        let r2 = tree.root_for_blockhash(&h2).expect("root h2").expect("root exists");
        assert_eq!(tree.get_at_root(r1, key).expect("get h1"), Some(vec![1]));
        assert_eq!(tree.get_at_root(r2, key).expect("get h2"), Some(vec![2]));
    }

    #[test]
    fn root_for_in_progress_blockhash_is_visible() {
        let (_dir, tree) = new_tree();
        let key = b"essentials:/in-progress";
        let value = vec![9u8];

        let genesis = BlockHash::from_byte_array([0u8; 32]);
        let h1 = BlockHash::from_byte_array([7u8; 32]);

        tree.begin_block(1, &h1, &genesis).expect("begin block");
        tree.apply_batch(&[(key.to_vec(), Some(value.clone()))]).expect("apply");

        let in_progress_root = tree.root_for_blockhash(&h1).expect("root lookup").expect("root");
        assert_eq!(tree.get_at_root(in_progress_root, key).expect("get in-progress"), Some(value),);

        tree.finish_block().expect("finish block");
    }
}
