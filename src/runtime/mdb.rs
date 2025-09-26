use rocksdb::{
    BlockBasedOptions, Cache, DB, Direction, Error as RocksError, IteratorMode, Options,
    ReadOptions, WriteBatch,
};
use std::{path::Path, sync::Arc};

/// ===== Cache / open-time tuning =====
/// How big you want the LRU block cache (data + index/filter when enabled).
pub const ROCKS_BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Warm the block cache for this namespace on open (iterate all keys once).
pub const WARM_CACHE_ON_OPEN: bool = true;

/// Bloom filter bits/key (helps point lookups).
pub const BLOOM_BITS_PER_KEY: f64 = 10.0;

#[derive(Clone)]
pub struct Mdb {
    db: Arc<DB>,
    prefix: Vec<u8>,
    // Keep the cache alive as long as this handle is alive (important!)
}

impl Mdb {
    fn from_parts(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        Self { db, prefix: prefix.as_ref().to_vec() }
    }

    pub fn from_db(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        // Back-compat constructor (no custom options)
        Self::from_parts(db, prefix)
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

        let mdb = Self::from_parts(Arc::new(db), prefix);
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
        let mdb = Self::from_parts(Arc::new(db), prefix);
        if WARM_CACHE_ON_OPEN {
            let _ = mdb.warm_up_namespace();
        }
        Ok(mdb)
    }

    /// Walk the namespace once to populate the block cache.
    /// Returns the number of KV pairs touched.
    pub fn warm_up_namespace(&self) -> Result<usize, RocksError> {
        let ns = self.prefix.clone();

        let mut ro = ReadOptions::default();
        ro.fill_cache(true); // populate block cache on read

        // Start at the namespace prefix and scan forward until it stops matching.
        let it = self.db.iterator_opt(IteratorMode::From(&ns, Direction::Forward), ro);

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

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.db.get(self.prefixed(k))
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, RocksError> {
        // Apply DB prefix to each RELATIVE key
        let prefixed: Vec<Vec<u8>> = keys.iter().map(|k| self.prefixed(k)).collect();

        // rocksdb::DB::multi_get returns Vec<Result<Option<DBPinnableSlice>, Error>>
        let results = self.db.multi_get(prefixed);

        // Map to Result<Vec<Option<Vec<u8>>>, Error>, preserving order
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(Some(slice)) => out.push(Some(slice.to_vec())),
                Ok(None) => out.push(None),
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    pub fn put(&self, k: &[u8], v: &[u8]) -> Result<(), RocksError> {
        self.db.put(self.prefixed(k), v)
    }

    pub fn delete(&self, k: &[u8]) -> Result<(), RocksError> {
        self.db.delete(self.prefixed(k))
    }

    pub fn bulk_write<F>(&self, build: F) -> Result<(), RocksError>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        let mut wb = WriteBatch::default();
        {
            let mut mb = MdbBatch { mdb: self, wb: &mut wb };
            build(&mut mb);
        }
        self.db.write(wb)
    }

    /// Iterate forward over raw DB starting from namespaced key `start` (inclusive).
    pub fn iter_from(
        &self,
        start: &[u8],
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), RocksError>> + '_ {
        let ns_start = self.prefixed(start);
        self.db
            .iterator(IteratorMode::From(&ns_start, Direction::Forward))
            .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())))
    }

    /// Iterate backward over keys that share a **full** prefix `ns_prefix` (already composed by caller).
    /// Helper used by RPC: build "c10:BE(pool):" once and walk back.
    pub fn iter_prefix_rev(
        &self,
        ns_prefix: &[u8],
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), RocksError>> + '_ {
        // Own the prefix to avoid borrowing from the caller
        let prefix = ns_prefix.to_vec();

        // Seek to the end of the prefix range: prefix + 0xFF
        let mut upper = prefix.clone();
        upper.push(0xFF);

        self.db
            .iterator(IteratorMode::From(&upper, Direction::Reverse))
            .take_while(
                move |res| {
                    if let Ok((k, _)) = res { k.starts_with(&prefix) } else { false }
                },
            )
            .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())))
    }
    pub fn scan_prefix(&self, rel_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};
        let mut start = self.prefix().to_vec();
        start.extend_from_slice(rel_prefix);

        // compute upper bound
        let mut ub = start.clone();
        for i in (0..ub.len()).rev() {
            if ub[i] != 0xff {
                ub[i] += 1;
                ub.truncate(i + 1);
                break;
            }
            if i == 0 {
                ub.clear();
            } // no UB; iterate all, we will break by prefix
        }

        let mut ro = ReadOptions::default();
        if !ub.is_empty() {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let it = self.db.iterator_opt(IteratorMode::From(&start, Direction::Forward), ro);
        let mut keys = Vec::new();
        for kv in it {
            let (k_full, _v) = kv?;
            if !k_full.starts_with(&start) {
                break;
            }
            // Strip module prefix to return RELATIVE keys:
            let rel = &k_full[self.prefix().len()..];
            keys.push(rel.to_vec());
        }
        Ok(keys)
    }

    #[inline]
    pub fn inner_db(&self) -> &DB {
        &self.db
    }

    #[inline]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }
}

pub struct MdbBatch<'a> {
    mdb: &'a Mdb,
    wb: &'a mut WriteBatch,
}

impl<'a> MdbBatch<'a> {
    #[inline]
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        self.wb.put(self.mdb.prefixed(k), v);
    }
    #[inline]
    pub fn delete(&mut self, k: &[u8]) {
        self.wb.delete(self.mdb.prefixed(k));
    }
}
