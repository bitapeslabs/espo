use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::pointers::{KvPointer, ListPointer};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::BlockHash;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SeriesEntry {
    pub series_id: String,
    pub alkane_id: SchemaAlkaneId,
    pub creation_height: u32,
}

pub fn normalize_series_id(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

pub fn series_id_base_from_name(name_norm: &str) -> Option<String> {
    let trimmed = name_norm.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_ascii_lowercase();
    Some(lowered.split_whitespace().collect::<Vec<_>>().join("-"))
}

fn series_id_matches_name(series_id: &str, name_norm: &str) -> bool {
    if series_id == name_norm {
        return true;
    }
    if let Some(rest) = series_id.strip_prefix(name_norm) {
        if let Some(num) = rest.strip_prefix('-') {
            return !num.is_empty() && num.chars().all(|c| c.is_ascii_digit());
        }
    }
    false
}

fn dedupe_batch_ops(
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>) {
    let mut seen_puts: HashSet<Vec<u8>> = HashSet::new();
    let mut dedup_puts_rev: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(puts.len());
    for (key, value) in puts.into_iter().rev() {
        if seen_puts.insert(key.clone()) {
            dedup_puts_rev.push((key, value));
        }
    }
    dedup_puts_rev.reverse();

    let put_keys: HashSet<Vec<u8>> = dedup_puts_rev.iter().map(|(k, _)| k.clone()).collect();
    let mut seen_deletes: HashSet<Vec<u8>> = HashSet::new();
    let mut dedup_deletes: Vec<Vec<u8>> = Vec::with_capacity(deletes.len());
    for key in deletes {
        if put_keys.contains(&key) {
            continue;
        }
        if seen_deletes.insert(key.clone()) {
            dedup_deletes.push(key);
        }
    }

    (dedup_puts_rev, dedup_deletes)
}

#[allow(non_snake_case)]
#[derive(Clone)]
pub struct PizzafunTable<'a> {
    pub ROOT: KvPointer<'a>,
    pub INDEX_HEIGHT: KvPointer<'a>,
    pub SERIES_BY_ID: KvPointer<'a>,
    pub SERIES_BY_ALKANE: KvPointer<'a>,
    pub SERIES_ALL: ListPointer<'a>,
}

impl<'a> PizzafunTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = KvPointer::root(mdb);
        Self {
            INDEX_HEIGHT: root.keyword("/index_height"),
            SERIES_BY_ID: root.keyword("/series/by_id/"),
            SERIES_BY_ALKANE: root.keyword("/series/by_alkane/"),
            SERIES_ALL: root.list_keyword("/series/all/v2/"),
            ROOT: root,
        }
    }

    pub fn series_by_id_key(&self, series_id: &str) -> Vec<u8> {
        self.SERIES_BY_ID.select(series_id.as_bytes()).key().to_vec()
    }

    pub fn series_by_id_prefix(&self) -> Vec<u8> {
        self.SERIES_BY_ID.key().to_vec()
    }

    pub fn series_by_alkane_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.SERIES_BY_ALKANE.select(&suffix).key().to_vec()
    }

    pub fn series_by_alkane_prefix(&self) -> Vec<u8> {
        self.SERIES_BY_ALKANE.key().to_vec()
    }

    pub fn series_all_prefix(&self) -> Vec<u8> {
        self.SERIES_ALL.key().to_vec()
    }

    pub fn series_all_entry_prefix(&self) -> Vec<u8> {
        let mut key = self.series_all_prefix();
        key.extend_from_slice(b"entry/");
        key
    }

    pub fn series_all_entry_key(&self, series_id: &str) -> Vec<u8> {
        let mut key = self.series_all_entry_prefix();
        key.extend_from_slice(series_id.as_bytes());
        key
    }
}

pub struct GetIndexHeightParams {
    pub blockhash: StateAt,
}

pub struct GetIndexHeightResult {
    pub height: Option<u32>,
}

pub struct SetIndexHeightParams {
    pub blockhash: StateAt,

    pub height: u32,
}

pub struct GetSeriesByIdParams {
    pub blockhash: StateAt,
    pub series_id: String,
}

pub struct GetSeriesByIdsParams {
    pub blockhash: StateAt,
    pub series_ids: Vec<String>,
}

pub struct GetSeriesByAlkaneParams {
    pub blockhash: StateAt,
    pub alkane: SchemaAlkaneId,
}

pub struct GetSeriesByAlkanesParams {
    pub blockhash: StateAt,
    pub alkanes: Vec<SchemaAlkaneId>,
}

pub struct GetSeriesEntriesByNameParams {
    pub blockhash: StateAt,
    pub name_norm: String,
}

#[derive(Clone)]
pub struct PizzafunProvider {
    mdb: Arc<Mdb>,
    view_blockhash: Option<BlockHash>,
}

impl PizzafunProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        Self { mdb, view_blockhash: None }
    }

    pub fn with_view_blockhash(&self, blockhash: Option<BlockHash>) -> Self {
        Self { mdb: Arc::clone(&self.mdb), view_blockhash: blockhash }
    }

    pub fn with_height(&self, height: Option<u64>, height_present: bool) -> Result<Self> {
        if !height_present {
            return Ok(self.with_view_blockhash(None));
        }
        let Some(height) = height else {
            return Err(anyhow!("missing_or_invalid_height"));
        };
        let height_u32 = u32::try_from(height).map_err(|_| anyhow!("height_out_of_range"))?;
        let Some(blockhash) = self
            .mdb
            .blockhash_for_height(height_u32)
            .map_err(|e| anyhow!("tree lookup failed: {e}"))?
        else {
            return Err(anyhow!("height_not_indexed"));
        };
        Ok(self.with_view_blockhash(Some(blockhash)))
    }

    pub fn table(&self) -> PizzafunTable<'_> {
        PizzafunTable::new(self.mdb.as_ref())
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb.as_ref()
    }

    fn raw_get_at(&self, key: &[u8], blockhash: Option<BlockHash>) -> Result<Option<Vec<u8>>> {
        match blockhash {
            Some(blockhash) => self
                .mdb
                .get_at_blockhash(&blockhash, key)
                .map_err(|e| anyhow!("mdb.get_at_blockhash failed: {e}")),
            None => self.mdb.get(key).map_err(|e| anyhow!("mdb.get failed: {e}")),
        }
    }

    fn raw_multi_get_at(
        &self,
        keys: &[Vec<u8>],
        blockhash: Option<BlockHash>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        match blockhash {
            Some(blockhash) => {
                let mut out = Vec::with_capacity(keys.len());
                for key in keys {
                    out.push(self.raw_get_at(key, Some(blockhash))?);
                }
                Ok(out)
            }
            None => self.mdb.multi_get(keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}")),
        }
    }

    fn raw_scan_prefix_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut keys = match self.view_blockhash {
            Some(blockhash) => self
                .mdb
                .scan_prefix_keys_at_blockhash(&blockhash, prefix)
                .map_err(|e| anyhow!("mdb.scan_prefix_keys_at_blockhash failed: {e}"))?,
            None => self
                .mdb
                .scan_prefix_keys(prefix)
                .map_err(|e| anyhow!("mdb.scan_prefix_keys failed: {e}"))?,
        };
        keys.sort();
        Ok(keys)
    }

    fn read_series_ids_all(&self, blockhash: Option<BlockHash>) -> Result<Vec<String>> {
        let table = self.table();
        let entry_prefix = table.series_all_entry_prefix();
        let keys = self.with_view_blockhash(blockhash).raw_scan_prefix_keys(&entry_prefix)?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(suffix) = key.strip_prefix(entry_prefix.as_slice()) else {
                continue;
            };
            let Ok(series_id) = std::str::from_utf8(suffix) else {
                continue;
            };
            out.push(series_id.to_string());
        }
        Ok(out)
    }

    pub fn get_index_height(&self, _params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        crate::debug_timer_log!("pizzafun.get_index_height");
        let table = self.table();
        let Some(bytes) = self
            .raw_get_at(table.INDEX_HEIGHT.key(), _params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(GetIndexHeightResult { height: None });
        };
        if bytes.len() != 4 {
            return Err(anyhow!("[PIZZAFUN] invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(GetIndexHeightResult { height: Some(u32::from_le_bytes(arr)) })
    }

    pub fn set_index_height(&self, params: SetIndexHeightParams) -> Result<()> {
        crate::debug_timer_log!("pizzafun.set_index_height");
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        let table = self.table();
        table.INDEX_HEIGHT.put(&params.height.to_le_bytes())
    }

    pub fn get_series_by_id(&self, params: GetSeriesByIdParams) -> Result<Option<SeriesEntry>> {
        let table = self.table();
        let key = table.series_by_id_key(&params.series_id);
        let Some(bytes) = self.raw_get_at(&key, params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(None);
        };
        Ok(Some(SeriesEntry::try_from_slice(&bytes)?))
    }

    pub fn get_series_by_ids(
        &self,
        params: GetSeriesByIdsParams,
    ) -> Result<Vec<Option<SeriesEntry>>> {
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            params.series_ids.iter().map(|s| table.series_by_id_key(s)).collect();
        let raw = self.raw_multi_get_at(&keys, params.blockhash.resolve(self.view_blockhash))?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            match item {
                Some(bytes) => out.push(Some(SeriesEntry::try_from_slice(&bytes)?)),
                None => out.push(None),
            }
        }
        Ok(out)
    }

    pub fn get_series_by_alkane(
        &self,
        params: GetSeriesByAlkaneParams,
    ) -> Result<Option<SeriesEntry>> {
        let table = self.table();
        let key = table.series_by_alkane_key(&params.alkane);
        let Some(bytes) = self.raw_get_at(&key, params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(None);
        };
        Ok(Some(SeriesEntry::try_from_slice(&bytes)?))
    }

    pub fn get_series_by_alkanes(
        &self,
        params: GetSeriesByAlkanesParams,
    ) -> Result<Vec<Option<SeriesEntry>>> {
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            params.alkanes.iter().map(|a| table.series_by_alkane_key(a)).collect();
        let raw = self.raw_multi_get_at(&keys, params.blockhash.resolve(self.view_blockhash))?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            match item {
                Some(bytes) => out.push(Some(SeriesEntry::try_from_slice(&bytes)?)),
                None => out.push(None),
            }
        }
        Ok(out)
    }

    pub fn get_series_entries_by_name(
        &self,
        params: GetSeriesEntriesByNameParams,
    ) -> Result<Vec<SeriesEntry>> {
        let table = self.table();
        let mut lookup_names: Vec<String> = vec![params.name_norm.clone()];
        if let Some(series_base) = series_id_base_from_name(&params.name_norm) {
            if series_base != params.name_norm {
                lookup_names.push(series_base);
            }
        }

        let mut filtered_ids: Vec<String> = Vec::new();
        let mut seen_ids: HashSet<String> = HashSet::new();
        for name in &lookup_names {
            let prefix = table.series_by_id_key(name);
            let keys = self
                .with_view_blockhash(params.blockhash.resolve(self.view_blockhash))
                .raw_scan_prefix_keys(&prefix)?;
            for key in keys {
                let Some(suffix) = key.strip_prefix(table.series_by_id_prefix().as_slice()) else {
                    continue;
                };
                let Ok(series_id) = std::str::from_utf8(suffix) else {
                    continue;
                };
                if series_id_matches_name(series_id, name) && seen_ids.insert(series_id.to_string())
                {
                    filtered_ids.push(series_id.to_string());
                }
            }
        }

        if filtered_ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<Vec<u8>> = filtered_ids.iter().map(|id| table.series_by_id_key(id)).collect();
        let raw = self.raw_multi_get_at(&keys, params.blockhash.resolve(self.view_blockhash))?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            if let Some(bytes) = item {
                out.push(SeriesEntry::try_from_slice(&bytes)?);
            }
        }
        Ok(out)
    }

    pub fn update_series_for_name(
        &self,
        existing: &[SeriesEntry],
        updated: &[SeriesEntry],
    ) -> Result<()> {
        let table = self.table();
        let mut deletes: Vec<Vec<u8>> = Vec::with_capacity(existing.len() * 3);
        for entry in existing {
            deletes.push(table.series_by_id_key(&entry.series_id));
            deletes.push(table.series_by_alkane_key(&entry.alkane_id));
            deletes.push(table.series_all_entry_key(&entry.series_id));
        }

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(updated.len() * 3);
        for entry in updated {
            let encoded = borsh::to_vec(entry)?;
            puts.push((table.series_by_id_key(&entry.series_id), encoded.clone()));
            puts.push((table.series_by_alkane_key(&entry.alkane_id), encoded));
            puts.push((table.series_all_entry_key(&entry.series_id), Vec::new()));
        }
        let (puts, deletes) = dedupe_batch_ops(puts, deletes);

        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &deletes {
                    wb.delete(key);
                }
                for (key, value) in &puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }

    pub fn replace_series_entries(&self, entries: &[SeriesEntry], height: u32) -> Result<()> {
        let table = self.table();
        let existing_ids = self.read_series_ids_all(None)?;
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        if !existing_ids.is_empty() {
            let existing_rows = self.get_series_by_ids(GetSeriesByIdsParams {
                blockhash: StateAt::Latest,
                series_ids: existing_ids.clone(),
            })?;
            for (idx, maybe_entry) in existing_rows.into_iter().enumerate() {
                let Some(entry) = maybe_entry else { continue };
                if let Some(series_id) = existing_ids.get(idx) {
                    deletes.push(table.series_by_id_key(series_id));
                    deletes.push(table.series_all_entry_key(series_id));
                } else {
                    deletes.push(table.series_by_id_key(&entry.series_id));
                    deletes.push(table.series_all_entry_key(&entry.series_id));
                }
                deletes.push(table.series_by_alkane_key(&entry.alkane_id));
            }
        }

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len() * 3);
        for entry in entries {
            let encoded = borsh::to_vec(entry)?;
            puts.push((table.series_by_id_key(&entry.series_id), encoded.clone()));
            puts.push((table.series_by_alkane_key(&entry.alkane_id), encoded));
            puts.push((table.series_all_entry_key(&entry.series_id), Vec::new()));
        }
        let (puts, deletes) = dedupe_batch_ops(puts, deletes);

        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &deletes {
                    wb.delete(key);
                }
                for (key, value) in &puts {
                    wb.put(key, value);
                }
                wb.put(table.INDEX_HEIGHT.key(), &height.to_le_bytes());
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_series_id, series_id_base_from_name, series_id_matches_name};

    #[test]
    fn normalize_series_id_preserves_spacing_for_backcompat() {
        assert_eq!(normalize_series_id("  Love Bomb  ").as_deref(), Some("love bomb"));
        assert_eq!(normalize_series_id("   "), None);
    }

    #[test]
    fn series_id_base_from_name_matches_expected_slug() {
        assert_eq!(series_id_base_from_name("love bomb").as_deref(), Some("love-bomb"));
    }

    #[test]
    fn series_id_matching_supports_base_and_numbered_suffix() {
        assert!(series_id_matches_name("love-bomb", "love-bomb"));
        assert!(series_id_matches_name("love-bomb-2", "love-bomb"));
        assert!(!series_id_matches_name("love-bomb-two", "love-bomb"));
        assert!(!series_id_matches_name("love-bomb-2a", "love-bomb"));
    }
}
