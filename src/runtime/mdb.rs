use rocksdb::{DB, Direction, Error as RocksError, IteratorMode, Options, WriteBatch};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub struct Mdb {
    db: Arc<DB>,
    prefix: Vec<u8>,
}

impl Mdb {
    pub fn from_db(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        Self { db, prefix: prefix.as_ref().to_vec() }
    }

    pub fn open(path: impl AsRef<Path>, prefix: impl AsRef<[u8]>) -> Result<Self, RocksError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self::from_db(Arc::new(db), prefix))
    }

    pub fn open_read_only(
        path: impl AsRef<Path>,
        prefix: impl AsRef<[u8]>,
        error_if_log_file_exist: bool,
    ) -> Result<Self, RocksError> {
        let opts = Options::default();
        let db = DB::open_for_read_only(&opts, path, error_if_log_file_exist)?;
        Ok(Self::from_db(Arc::new(db), prefix))
    }

    #[inline]
    fn prefixed(&self, k: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.prefix.len() + k.len());
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(k);
        out
    }

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.db.get(self.prefixed(k))
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
