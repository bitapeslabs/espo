use bitcoin::BlockHash;
use rocksdb::{
    BlockBasedOptions, Cache, DB, Direction, Error as RocksError, IteratorMode, Options,
    ReadOptions, WriteBatch,
};
use std::{fmt, path::Path, sync::Arc};

use crate::runtime::remote_mdb::RemoteMdbClient;
use crate::runtime::tree_db::{get_global_tree_db, is_tree_internal_key};

/// ===== Cache / open-time tuning =====
/// How big you want the LRU block cache (data + index/filter when enabled).
pub const ROCKS_BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Warm the block cache for this namespace on open (iterate all keys once).
pub const WARM_CACHE_ON_OPEN: bool = true;

/// Bloom filter bits/key (helps point lookups).
pub const BLOOM_BITS_PER_KEY: f64 = 10.0;

/// Error type for Mdb operations. Local (RocksDB) reads surface `Rocks`;
/// remote-backed reads surface `Remote`; writes against a remote-backed Mdb
/// are always rejected with `RemoteReadOnly`.
#[derive(Debug)]
pub enum MdbError {
    Rocks(RocksError),
    Remote(String),
    RemoteReadOnly,
}

impl fmt::Display for MdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MdbError::Rocks(e) => write!(f, "{e}"),
            MdbError::Remote(msg) => write!(f, "remote mdb: {msg}"),
            MdbError::RemoteReadOnly => {
                write!(f, "remote mdb is read-only (writes require a local database)")
            }
        }
    }
}

impl std::error::Error for MdbError {}

impl From<RocksError> for MdbError {
    fn from(e: RocksError) -> Self {
        MdbError::Rocks(e)
    }
}

pub type MdbResult<T> = Result<T, MdbError>;

#[derive(Clone)]
enum MdbBackend {
    Local(Arc<DB>),
    Remote(Arc<RemoteMdbClient>),
}

#[derive(Clone)]
pub struct Mdb {
    backend: MdbBackend,
    prefix: Vec<u8>,
    versioned: bool,
}

impl Mdb {
    fn should_enable_versioned_namespace(prefix: &[u8]) -> bool {
        matches!(
            prefix,
            b"essentials:" | b"ammdata:" | b"tokendata:" | b"subfrost:" | b"pizzafun:" | b"oylapi:"
        )
    }

    fn from_parts(db: Arc<DB>, prefix: impl AsRef<[u8]>, versioned: bool) -> Self {
        let prefix_vec = prefix.as_ref().to_vec();
        Self { backend: MdbBackend::Local(db), prefix: prefix_vec, versioned }
    }

    pub fn from_db(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        // Back-compat constructor (no custom options)
        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        Self::from_parts(db, p, versioned)
    }

    /// Construct an Mdb whose reads are fulfilled by a remote espo instance's
    /// `internal.*` RPC methods instead of a local RocksDB. Versioned-tree
    /// semantics are resolved server-side. Writes are rejected.
    pub fn remote(client: Arc<RemoteMdbClient>, prefix: impl AsRef<[u8]>) -> Self {
        Self {
            backend: MdbBackend::Remote(client),
            prefix: prefix.as_ref().to_vec(),
            versioned: false,
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self.backend, MdbBackend::Remote(_))
    }

    /// Clone this handle onto the same underlying backend with a different namespace prefix.
    pub fn clone_with_prefix(&self, prefix: impl AsRef<[u8]>) -> Self {
        let p = prefix.as_ref().to_vec();
        match &self.backend {
            MdbBackend::Local(db) => {
                let versioned = Self::should_enable_versioned_namespace(&p);
                Self::from_parts(Arc::clone(db), p, versioned)
            }
            MdbBackend::Remote(client) => Self::remote(Arc::clone(client), p),
        }
    }

    pub fn open(path: impl AsRef<Path>, prefix: impl AsRef<[u8]>) -> Result<Self, RocksError> {
        // ---- Block cache + table options ----
        let cache = Cache::new_lru_cache(ROCKS_BLOCK_CACHE_BYTES);

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        // Put index + filter in the cache (hot metadata)
        table.set_cache_index_and_filter_blocks(true);
        // Pin L0 index/filter in cache (fastest for recent data)
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        // Bloom filter (not whole-key)
        table.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Keep readers open (avoid fd thrash)
        opts.set_max_open_files(-1);
        opts.set_block_based_table_factory(&table);

        let db = DB::open(&opts, path)?;

        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        let mdb = Self::from_parts(Arc::new(db), p, versioned);
        if WARM_CACHE_ON_OPEN {
            let _ = mdb.warm_up_namespace(); // best-effort
        }
        Ok(mdb)
    }

    pub fn open_read_only(
        path: impl AsRef<Path>,
        prefix: impl AsRef<[u8]>,
        error_if_log_file_exist: bool,
    ) -> Result<Self, RocksError> {
        let cache = Cache::new_lru_cache(ROCKS_BLOCK_CACHE_BYTES);

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_cache_index_and_filter_blocks(true);
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

        let mut opts = Options::default();
        opts.set_block_based_table_factory(&table);

        let db = DB::open_for_read_only(&opts, path, error_if_log_file_exist)?;
        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        let mdb = Self::from_parts(Arc::new(db), p, versioned);
        if WARM_CACHE_ON_OPEN {
            let _ = mdb.warm_up_namespace();
        }
        Ok(mdb)
    }

    /// Walk the namespace once to populate the block cache.
    /// Returns the number of KV pairs touched.
    pub fn warm_up_namespace(&self) -> MdbResult<usize> {
        let MdbBackend::Local(db) = &self.backend else {
            return Ok(0);
        };
        if self.versioned_manager().is_some() {
            return Ok(0);
        }
        let ns = self.prefix.clone();

        let mut ro = ReadOptions::default();
        ro.fill_cache(true); // populate block cache on read

        // Start at the namespace prefix and scan forward until it stops matching.
        let it = db.iterator_opt(IteratorMode::From(&ns, Direction::Forward), ro);

        let mut count = 0usize;
        for res in it {
            let (k, _v) = res?;
            if !k.starts_with(&ns) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    #[inline]
    pub fn prefixed(&self, k: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.prefix.len() + k.len());
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(k);
        out
    }

    fn remote_err(e: String) -> MdbError {
        MdbError::Remote(e)
    }

    pub fn get(&self, k: &[u8]) -> MdbResult<Option<Vec<u8>>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.get(&self.prefix, k, None).map_err(Self::remote_err)
            }
            MdbBackend::Local(db) => {
                let full = self.prefixed(k);
                if let Some(tree) = self.versioned_manager() {
                    if is_tree_internal_key(&full) {
                        return Ok(db.get(full)?);
                    }
                    return Ok(tree.get(&full)?);
                }
                Ok(db.get(full)?)
            }
        }
    }

    pub fn get_at_blockhash(&self, block_hash: &BlockHash, k: &[u8]) -> MdbResult<Option<Vec<u8>>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.get(&self.prefix, k, Some(block_hash)).map_err(Self::remote_err)
            }
            MdbBackend::Local(db) => {
                let full = self.prefixed(k);
                if let Some(tree) = self.versioned_manager() {
                    if let Some(root) = tree.root_for_blockhash(block_hash)? {
                        return Ok(tree.get_at_root(root, &full)?);
                    }
                    return Ok(None);
                }
                Ok(db.get(full)?)
            }
        }
    }

    pub fn scan_prefix_entries(&self, prefix: &[u8]) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.scan_prefix_entries(&self.prefix, prefix, None).map_err(Self::remote_err)
            }
            MdbBackend::Local(db) => {
                let ns_prefix = self.prefixed(prefix);
                if let Some(tree) = self.versioned_manager() {
                    let entries = tree.collect_prefixed_entries(&ns_prefix)?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }

                let mut out = Vec::new();
                for res in db.iterator(IteratorMode::From(&ns_prefix, Direction::Forward)) {
                    let (key, value) = res?;
                    if !key.starts_with(&ns_prefix) {
                        break;
                    }
                    if key.starts_with(&self.prefix) {
                        out.push((key[self.prefix.len()..].to_vec(), value.to_vec()));
                    }
                }
                Ok(out)
            }
        }
    }

    pub fn scan_prefix_entries_at_blockhash(
        &self,
        block_hash: &BlockHash,
        prefix: &[u8],
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_prefix_entries(&self.prefix, prefix, Some(block_hash))
                .map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let ns_prefix = self.prefixed(prefix);
                if let Some(tree) = self.versioned_manager() {
                    let Some(root) = tree.root_for_blockhash(block_hash)? else {
                        return Ok(Vec::new());
                    };
                    let entries = tree.collect_prefixed_entries_at_root(root, &ns_prefix)?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }
                self.scan_prefix_entries(prefix)
            }
        }
    }

    pub fn scan_range_entries(
        &self,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_range_entries(&self.prefix, start_inclusive, end_exclusive, None)
                .map_err(Self::remote_err),
            MdbBackend::Local(db) => {
                let ns_start = self.prefixed(start_inclusive);
                let ns_end = end_exclusive.map(|end| self.prefixed(end));
                if let Some(tree) = self.versioned_manager() {
                    let entries = tree.range_entries(&ns_start, ns_end.as_deref())?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }

                let mut out = Vec::new();
                for res in db.iterator(IteratorMode::From(&ns_start, Direction::Forward)) {
                    let (key, value) = res?;
                    if let Some(end) = &ns_end {
                        if key.as_ref() >= end.as_slice() {
                            break;
                        }
                    }
                    if key.starts_with(&self.prefix) {
                        out.push((key[self.prefix.len()..].to_vec(), value.to_vec()));
                    }
                }
                Ok(out)
            }
        }
    }

    pub fn scan_range_entries_at_blockhash(
        &self,
        block_hash: &BlockHash,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_range_entries(&self.prefix, start_inclusive, end_exclusive, Some(block_hash))
                .map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let ns_start = self.prefixed(start_inclusive);
                let ns_end = end_exclusive.map(|end| self.prefixed(end));
                if let Some(tree) = self.versioned_manager() {
                    let Some(root) = tree.root_for_blockhash(block_hash)? else {
                        return Ok(Vec::new());
                    };
                    let entries = tree.range_entries_at_root(root, &ns_start, ns_end.as_deref())?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }
                self.scan_range_entries(start_inclusive, end_exclusive)
            }
        }
    }

    pub fn scan_range_entries_page(
        &self,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_range_entries_page(
                    &self.prefix,
                    start_inclusive,
                    end_exclusive,
                    offset,
                    limit,
                    reverse,
                    None,
                )
                .map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let ns_start = self.prefixed(start_inclusive);
                let ns_end = end_exclusive.map(|end| self.prefixed(end));
                if let Some(tree) = self.versioned_manager() {
                    let entries = tree.range_entries_page_at_root(
                        tree.active_root(),
                        &ns_start,
                        ns_end.as_deref(),
                        offset,
                        limit,
                        reverse,
                    )?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }
                self.scan_range_entries_page_unversioned(
                    &ns_start,
                    ns_end.as_deref(),
                    offset,
                    limit,
                    reverse,
                )
            }
        }
    }

    pub fn scan_range_entries_page_at_blockhash(
        &self,
        block_hash: &BlockHash,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_range_entries_page(
                    &self.prefix,
                    start_inclusive,
                    end_exclusive,
                    offset,
                    limit,
                    reverse,
                    Some(block_hash),
                )
                .map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let ns_start = self.prefixed(start_inclusive);
                let ns_end = end_exclusive.map(|end| self.prefixed(end));
                if let Some(tree) = self.versioned_manager() {
                    let Some(root) = tree.root_for_blockhash(block_hash)? else {
                        return Ok(Vec::new());
                    };
                    let entries = tree.range_entries_page_at_root(
                        root,
                        &ns_start,
                        ns_end.as_deref(),
                        offset,
                        limit,
                        reverse,
                    )?;
                    let mut out = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        if key.starts_with(&self.prefix) {
                            out.push((key[self.prefix.len()..].to_vec(), value));
                        }
                    }
                    return Ok(out);
                }
                self.scan_range_entries_page_unversioned(
                    &ns_start,
                    ns_end.as_deref(),
                    offset,
                    limit,
                    reverse,
                )
            }
        }
    }

    pub fn scan_prefix_keys(&self, prefix: &[u8]) -> MdbResult<Vec<Vec<u8>>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.scan_prefix_keys(&self.prefix, prefix, None).map_err(Self::remote_err)
            }
            MdbBackend::Local(db) => {
                let ns_prefix = self.prefixed(prefix);
                if let Some(tree) = self.versioned_manager() {
                    let keys = tree.collect_prefixed_keys(&ns_prefix)?;
                    let mut out = Vec::with_capacity(keys.len());
                    for key in keys {
                        if key.starts_with(&self.prefix) {
                            out.push(key[self.prefix.len()..].to_vec());
                        }
                    }
                    return Ok(out);
                }

                let mut out = Vec::new();
                for res in db.iterator(IteratorMode::From(&ns_prefix, Direction::Forward)) {
                    let (key, _value) = res?;
                    if !key.starts_with(&ns_prefix) {
                        break;
                    }
                    if key.starts_with(&self.prefix) {
                        out.push(key[self.prefix.len()..].to_vec());
                    }
                }
                Ok(out)
            }
        }
    }

    pub fn scan_prefix_keys_at_blockhash(
        &self,
        block_hash: &BlockHash,
        prefix: &[u8],
    ) -> MdbResult<Vec<Vec<u8>>> {
        match &self.backend {
            MdbBackend::Remote(client) => client
                .scan_prefix_keys(&self.prefix, prefix, Some(block_hash))
                .map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let ns_prefix = self.prefixed(prefix);
                if let Some(tree) = self.versioned_manager() {
                    let Some(root) = tree.root_for_blockhash(block_hash)? else {
                        return Ok(Vec::new());
                    };
                    let keys = tree.collect_prefixed_keys_at_root(root, &ns_prefix)?;
                    let mut out = Vec::with_capacity(keys.len());
                    for key in keys {
                        if key.starts_with(&self.prefix) {
                            out.push(key[self.prefix.len()..].to_vec());
                        }
                    }
                    return Ok(out);
                }
                self.scan_prefix_keys(prefix)
            }
        }
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> MdbResult<Vec<Option<Vec<u8>>>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.multi_get(&self.prefix, keys, None).map_err(Self::remote_err)
            }
            MdbBackend::Local(db) => {
                if let Some(tree) = self.versioned_manager() {
                    let prefixed: Vec<Vec<u8>> = keys.iter().map(|k| self.prefixed(k)).collect();
                    return Ok(tree.multi_get(&prefixed)?);
                }
                // Apply DB prefix to each RELATIVE key
                let prefixed: Vec<Vec<u8>> = keys.iter().map(|k| self.prefixed(k)).collect();

                // rocksdb::DB::multi_get returns Vec<Result<Option<DBPinnableSlice>, Error>>
                let results = db.multi_get(prefixed);

                // Map to Result<Vec<Option<Vec<u8>>>, Error>, preserving order
                let mut out = Vec::with_capacity(results.len());
                for r in results {
                    match r {
                        Ok(Some(slice)) => out.push(Some(slice.to_vec())),
                        Ok(None) => out.push(None),
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(out)
            }
        }
    }

    pub fn multi_get_at_blockhash(
        &self,
        block_hash: &BlockHash,
        keys: &[Vec<u8>],
    ) -> MdbResult<Vec<Option<Vec<u8>>>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.multi_get(&self.prefix, keys, Some(block_hash)).map_err(Self::remote_err)
            }
            MdbBackend::Local(_) => {
                if let Some(tree) = self.versioned_manager() {
                    let Some(root) = tree.root_for_blockhash(block_hash)? else {
                        return Ok(vec![None; keys.len()]);
                    };
                    let prefixed: Vec<Vec<u8>> =
                        keys.iter().map(|key| self.prefixed(key)).collect();
                    return Ok(tree.multi_get_at_root(root, &prefixed)?);
                }
                self.multi_get(keys)
            }
        }
    }

    /// Resolve a block height to its canonical blockhash: via the local
    /// versioned tree when local, via the remote espo when remote.
    pub fn blockhash_for_height(&self, height: u32) -> MdbResult<Option<BlockHash>> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                client.blockhash_for_height(height).map_err(Self::remote_err)
            }
            MdbBackend::Local(_) => {
                let Some(tree) = get_global_tree_db() else {
                    return Err(MdbError::Remote("versioned_tree_unavailable".to_string()));
                };
                Ok(tree.blockhash_for_height(height)?)
            }
        }
    }

    /// Indexed height bounds (min, max): local versioned tree when local,
    /// remote espo when remote.
    pub fn indexed_height_bounds(&self) -> MdbResult<Option<(u32, u32)>> {
        match &self.backend {
            MdbBackend::Remote(client) => client.indexed_height_bounds().map_err(Self::remote_err),
            MdbBackend::Local(_) => {
                let Some(tree) = get_global_tree_db() else {
                    return Ok(None);
                };
                Ok(tree.indexed_height_bounds()?)
            }
        }
    }

    pub fn put(&self, k: &[u8], v: &[u8]) -> MdbResult<()> {
        let MdbBackend::Local(db) = &self.backend else {
            return Err(MdbError::RemoteReadOnly);
        };
        let prefixed = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if is_tree_internal_key(&prefixed) {
                return Ok(db.put(prefixed, v)?);
            }
            return Ok(tree.put(&prefixed, v)?);
        }
        Ok(db.put(&prefixed, v)?)
    }

    pub fn delete(&self, k: &[u8]) -> MdbResult<()> {
        let MdbBackend::Local(db) = &self.backend else {
            return Err(MdbError::RemoteReadOnly);
        };
        let prefixed = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if is_tree_internal_key(&prefixed) {
                return Ok(db.delete(prefixed)?);
            }
            return Ok(tree.delete(&prefixed)?);
        }
        Ok(db.delete(&prefixed)?)
    }

    pub fn bulk_write<F>(&self, build: F) -> MdbResult<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        let MdbBackend::Local(db) = &self.backend else {
            return Err(MdbError::RemoteReadOnly);
        };
        if let Some(tree) = self.versioned_manager() {
            let mut versioned_changes: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
            {
                let mut mb = MdbBatch {
                    mdb: self,
                    wb: None,
                    versioned_changes: Some(&mut versioned_changes),
                };
                build(&mut mb);
            }
            return Ok(tree.apply_batch_owned(versioned_changes)?);
        }

        let mut wb = WriteBatch::default();
        {
            let mut mb = MdbBatch { mdb: self, wb: Some(&mut wb), versioned_changes: None };
            build(&mut mb);
        }
        Ok(db.write(wb)?)
    }

    /// Iterate forward over raw DB starting from namespaced key `start` (inclusive).
    /// Yields FULL (namespaced) keys.
    pub fn iter_from(
        &self,
        start: &[u8],
    ) -> Box<dyn Iterator<Item = MdbResult<(Vec<u8>, Vec<u8>)>> + '_> {
        match &self.backend {
            MdbBackend::Remote(client) => {
                let entries = client
                    .scan_range_entries(&self.prefix, start, None, None)
                    .map_err(Self::remote_err);
                match entries {
                    Ok(entries) => {
                        let prefix = self.prefix.clone();
                        Box::new(entries.into_iter().map(move |(k, v)| {
                            let mut full = Vec::with_capacity(prefix.len() + k.len());
                            full.extend_from_slice(&prefix);
                            full.extend_from_slice(&k);
                            Ok((full, v))
                        }))
                    }
                    Err(e) => Box::new(std::iter::once(Err(e))),
                }
            }
            MdbBackend::Local(db) => {
                if let Some(tree) = self.versioned_manager() {
                    let start_full = self.prefixed(start);
                    let mut entries =
                        tree.collect_prefixed_entries(self.prefix()).unwrap_or_else(|_| Vec::new());
                    entries.retain(|(k, _)| k >= &start_full);
                    return Box::new(entries.into_iter().map(Ok));
                }
                let ns_start = self.prefixed(start);
                Box::new(
                    db.iterator(IteratorMode::From(&ns_start, Direction::Forward))
                        .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into)),
                )
            }
        }
    }

    /// Raw handle to the local RocksDB. Panics for remote-backed Mdbs — every
    /// caller of this is an indexer/maintenance path that must run with a
    /// local database.
    #[inline]
    pub fn inner_db(&self) -> &DB {
        match &self.backend {
            MdbBackend::Local(db) => db,
            MdbBackend::Remote(_) => {
                panic!("Mdb::inner_db() is unavailable on a remote-backed Mdb")
            }
        }
    }

    #[inline]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    #[inline]
    pub fn is_versioned(&self) -> bool {
        self.versioned_manager().is_some()
    }

    fn versioned_manager(&self) -> Option<Arc<crate::runtime::tree_db::VersionedTreeDb>> {
        if !self.versioned || self.is_remote() {
            return None;
        }
        get_global_tree_db()
    }

    fn scan_range_entries_page_unversioned(
        &self,
        ns_start: &[u8],
        ns_end: Option<&[u8]>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let MdbBackend::Local(db) = &self.backend else {
            return Err(MdbError::RemoteReadOnly);
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(limit);
        let mut skipped = 0usize;
        let mode = if reverse {
            match ns_end {
                Some(end) => IteratorMode::From(end, Direction::Reverse),
                None => IteratorMode::End,
            }
        } else {
            IteratorMode::From(ns_start, Direction::Forward)
        };

        for res in db.iterator(mode) {
            let (key, value) = res?;
            if reverse {
                if key.as_ref() < ns_start {
                    break;
                }
                if let Some(end) = ns_end {
                    if key.as_ref() >= end {
                        continue;
                    }
                }
            } else {
                if let Some(end) = ns_end {
                    if key.as_ref() >= end {
                        break;
                    }
                }
            }
            if !key.starts_with(&self.prefix) {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push((key[self.prefix.len()..].to_vec(), value.to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

pub struct MdbBatch<'a> {
    mdb: &'a Mdb,
    wb: Option<&'a mut WriteBatch>,
    versioned_changes: Option<&'a mut Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

impl<'a> MdbBatch<'a> {
    #[inline]
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        let key = self.mdb.prefixed(k);
        if let Some(buf) = self.versioned_changes.as_mut() {
            buf.push((key, Some(v.to_vec())));
            return;
        }
        if let Some(wb) = self.wb.as_mut() {
            wb.put(key, v);
        }
    }
    #[inline]
    pub fn delete(&mut self, k: &[u8]) {
        let key = self.mdb.prefixed(k);
        if let Some(buf) = self.versioned_changes.as_mut() {
            buf.push((key, None));
            return;
        }
        if let Some(wb) = self.wb.as_mut() {
            wb.delete(key);
        }
    }
}
