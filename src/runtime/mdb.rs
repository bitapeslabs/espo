use anyhow::{Result, anyhow};
use rocksdb::{DB, Options};
use std::path::Path;

pub struct MetashrewDB {
    mdb: DB,
}

impl MetashrewDB {
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        let mdb = DB::open_for_read_only(&opts, db_path, false)?;
        Ok(Self { mdb })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        Ok(self.mdb.get(key.as_ref())?)
    }

    pub fn multi_get<K>(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        let owned: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let results = self.mdb.multi_get(refs);
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
