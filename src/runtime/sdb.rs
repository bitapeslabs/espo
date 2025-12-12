use anyhow::{Result, anyhow};
use rocksdb::{DB, Options};
use rocksdb::{DBIteratorWithThreadMode, IteratorMode, ReadOptions};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub struct SDB {
    sdb: Arc<DB>,
    stop: Arc<AtomicBool>,
    poller: Option<JoinHandle<()>>,
}

impl SDB {
    pub fn open<P: AsRef<Path>, S: AsRef<Path>>(
        primary_db_path: P,
        secondary_path: S,
        interval: Duration,
    ) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(false);

        let sdb = Arc::new(DB::open_as_secondary(
            &opts,
            primary_db_path.as_ref(),
            secondary_path.as_ref(),
        )?);

        let stop = Arc::new(AtomicBool::new(false));

        let sdb_clone = Arc::clone(&sdb);
        let stop_clone = Arc::clone(&stop);
        let poller = thread::spawn(move || {
            while !stop_clone.load(Ordering::Relaxed) {
                if let Err(e) = sdb_clone.try_catch_up_with_primary() {
                    eprintln!("[sdb] catch_up error: {e}");
                }
                thread::sleep(interval);
            }
        });

        Ok(Self { sdb, stop, poller: Some(poller) })
    }

    pub fn catch_up_now(&self) -> Result<()> {
        self.sdb.try_catch_up_with_primary()?;
        Ok(())
    }

    pub fn iterator_opt<'a>(
        &'a self,
        mode: IteratorMode<'a>,
        readopts: ReadOptions,
    ) -> DBIteratorWithThreadMode<'a, DB> {
        self.sdb.iterator_opt(mode, readopts)
    }

    /// Convenience iterator with default ReadOptions
    pub fn iterator<'a>(&'a self, mode: IteratorMode<'a>) -> DBIteratorWithThreadMode<'a, DB> {
        self.sdb.iterator(mode)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        Ok(self.sdb.get(key.as_ref())?)
    }

    /// Expose underlying RocksDB handle for direct reads (read-only context).
    pub fn as_db(&self) -> &DB {
        &self.sdb
    }

    pub fn multi_get<K>(&self, keys: impl IntoIterator<Item = K>) -> Result<Vec<Option<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        let owned: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let results = self.sdb.multi_get(refs);
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(Some(slice)) => out.push(Some(slice.to_vec())),
                Ok(None) => out.push(None),
                Err(e) => return Err(anyhow!(e)),
            }
        }
        Ok(out)
    }
}

impl Drop for SDB {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.poller.take() {
            let _ = handle.join();
        }
    }
}
