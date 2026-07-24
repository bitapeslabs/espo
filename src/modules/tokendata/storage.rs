use super::schemas::{SchemaTokenActivityV1, TokenActivityKind, TokenActivitySource};
use crate::config::get_address_index_chunk_size;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::BlockHash;
use borsh::{BorshDeserialize, BorshSerialize};
use std::sync::Arc;

const INDEX_HEIGHT_KEY: &[u8] = b"/v3/index_height";
const TOKEN_ACTIVITY_TS_ROOT: &[u8] = b"/token_activity/v3/";
const TOKEN_ACTIVITY_AMOUNT_ROOT: &[u8] = b"/token_activity_amount/v3/";
const ADDRESS_ACTIVITY_TS_ROOT: &[u8] = b"/address_activity/v3/";
const ADDRESS_ACTIVITY_AMOUNT_ROOT: &[u8] = b"/address_activity_amount/v3/";
const ADDRESS_TOKEN_ACTIVITY_TS_ROOT: &[u8] = b"/address_token_activity/v3/";
const ADDRESS_TOKEN_ACTIVITY_AMOUNT_ROOT: &[u8] = b"/address_token_activity_amount/v3/";
const DIESEL_AVG_PRICE_USD_BY_HEIGHT_ROOT: &[u8] = b"/block_price/v2/diesel_avg_usd_by_height/";
const PTR_V1_PREFIX: &[u8] = b"/ptr/v1/";
const PTR_ENTITY_ACTIVITY_ROW: &[u8] = b"activity_row";
const PTR_ENTITY_ACTIVITY_INDEX_CHUNK: &[u8] = b"activity_index_chunk";
const ACTIVITY_INDEX_INLINE_CAP: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenActivityScope {
    All,
    Market,
    Mint,
}

impl TokenActivityScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Market => "market",
            Self::Mint => "mint",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenActivitySortField {
    Timestamp,
    Amount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDir {
    Desc,
    Asc,
}

#[derive(Clone)]
pub struct TokenDataTable<'a> {
    mdb: &'a Mdb,
}

impl<'a> TokenDataTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        Self { mdb }
    }

    pub fn index_height_key(&self) -> Vec<u8> {
        INDEX_HEIGHT_KEY.to_vec()
    }

    pub fn diesel_avg_price_usd_by_height_prefix(&self) -> Vec<u8> {
        DIESEL_AVG_PRICE_USD_BY_HEIGHT_ROOT.to_vec()
    }

    pub fn diesel_avg_price_usd_by_height_key(&self, height: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(DIESEL_AVG_PRICE_USD_BY_HEIGHT_ROOT.len() + 4);
        key.extend_from_slice(DIESEL_AVG_PRICE_USD_BY_HEIGHT_ROOT);
        key.extend_from_slice(&height.to_be_bytes());
        key
    }

    fn token_prefix(root: &[u8], scope: TokenActivityScope, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(root.len() + 4 + 1 + 12 + 1);
        key.extend_from_slice(root);
        key.extend_from_slice(scope.as_str().as_bytes());
        key.push(b'/');
        key.extend_from_slice(&token.block.to_be_bytes());
        key.extend_from_slice(&token.tx.to_be_bytes());
        key.push(b'/');
        key
    }

    fn address_prefix(root: &[u8], scope: TokenActivityScope, address_spk: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(root.len() + 4 + 1 + 2 + address_spk.len() + 1);
        key.extend_from_slice(root);
        key.extend_from_slice(scope.as_str().as_bytes());
        key.push(b'/');
        push_spk(&mut key, address_spk);
        key.push(b'/');
        key
    }

    fn address_token_prefix(
        root: &[u8],
        scope: TokenActivityScope,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(root.len() + 4 + 1 + 2 + address_spk.len() + 1 + 12 + 1);
        key.extend_from_slice(root);
        key.extend_from_slice(scope.as_str().as_bytes());
        key.push(b'/');
        push_spk(&mut key, address_spk);
        key.push(b'/');
        key.extend_from_slice(&token.block.to_be_bytes());
        key.extend_from_slice(&token.tx.to_be_bytes());
        key.push(b'/');
        key
    }

    pub fn token_activity_prefix(
        &self,
        scope: TokenActivityScope,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        Self::token_prefix(TOKEN_ACTIVITY_TS_ROOT, scope, token)
    }

    pub fn token_activity_amount_prefix(
        &self,
        scope: TokenActivityScope,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        Self::token_prefix(TOKEN_ACTIVITY_AMOUNT_ROOT, scope, token)
    }

    pub fn address_activity_prefix(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
    ) -> Vec<u8> {
        Self::address_prefix(ADDRESS_ACTIVITY_TS_ROOT, scope, address_spk)
    }

    pub fn address_activity_amount_prefix(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
    ) -> Vec<u8> {
        Self::address_prefix(ADDRESS_ACTIVITY_AMOUNT_ROOT, scope, address_spk)
    }

    pub fn address_token_activity_prefix(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        Self::address_token_prefix(ADDRESS_TOKEN_ACTIVITY_TS_ROOT, scope, address_spk, token)
    }

    pub fn address_token_activity_amount_prefix(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        Self::address_token_prefix(ADDRESS_TOKEN_ACTIVITY_AMOUNT_ROOT, scope, address_spk, token)
    }

    pub fn token_activity_key(
        &self,
        scope: TokenActivityScope,
        token: &SchemaAlkaneId,
        timestamp: u64,
        txid: &[u8; 32],
        ordinal: u32,
        kind: TokenActivityKind,
    ) -> Vec<u8> {
        let mut key = self.token_activity_prefix(scope, token);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(txid);
        key.extend_from_slice(&ordinal.to_be_bytes());
        key.push(activity_kind_code(kind));
        key
    }

    pub fn token_activity_amount_key(
        &self,
        scope: TokenActivityScope,
        token: &SchemaAlkaneId,
        amount: u128,
        row_id: u64,
    ) -> Vec<u8> {
        let mut key = self.token_activity_amount_prefix(scope, token);
        key.extend_from_slice(&amount.to_be_bytes());
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    pub fn address_activity_key(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        timestamp: u64,
        txid: &[u8; 32],
        ordinal: u32,
        kind: TokenActivityKind,
    ) -> Vec<u8> {
        let mut key = self.address_activity_prefix(scope, address_spk);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(txid);
        key.extend_from_slice(&ordinal.to_be_bytes());
        key.push(activity_kind_code(kind));
        key
    }

    pub fn address_activity_amount_key(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        amount: u128,
        row_id: u64,
    ) -> Vec<u8> {
        let mut key = self.address_activity_amount_prefix(scope, address_spk);
        key.extend_from_slice(&amount.to_be_bytes());
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    pub fn address_token_activity_key(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
        timestamp: u64,
        txid: &[u8; 32],
        ordinal: u32,
        kind: TokenActivityKind,
    ) -> Vec<u8> {
        let mut key = self.address_token_activity_prefix(scope, address_spk, token);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(txid);
        key.extend_from_slice(&ordinal.to_be_bytes());
        key.push(activity_kind_code(kind));
        key
    }

    pub fn address_token_activity_amount_key(
        &self,
        scope: TokenActivityScope,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
        amount: u128,
        row_id: u64,
    ) -> Vec<u8> {
        let mut key = self.address_token_activity_amount_prefix(scope, address_spk, token);
        key.extend_from_slice(&amount.to_be_bytes());
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb
    }

    pub fn activity_row_counter_key(&self) -> Vec<u8> {
        pointer_counter_key(PTR_ENTITY_ACTIVITY_ROW)
    }

    pub fn activity_row_blob_key(&self, id: u64) -> Vec<u8> {
        pointer_blob_key(PTR_ENTITY_ACTIVITY_ROW, id)
    }

    pub fn activity_index_meta_key(&self, prefix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 4);
        key.extend_from_slice(prefix);
        key.extend_from_slice(b"meta");
        key
    }

    pub fn activity_index_chunk_counter_key(&self) -> Vec<u8> {
        pointer_counter_key(PTR_ENTITY_ACTIVITY_INDEX_CHUNK)
    }

    pub fn activity_index_chunk_blob_key(&self, id: u64) -> Vec<u8> {
        pointer_blob_key(PTR_ENTITY_ACTIVITY_INDEX_CHUNK, id)
    }
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
enum InlineOrExternalU64V1 {
    Inline { items: Vec<u64> },
    External { chunk_ids: Vec<u64>, len: u64, chunk_size: u32 },
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct U64ChunkV1 {
    items: Vec<u64>,
}

#[derive(Clone)]
pub struct TokenDataProvider {
    mdb: Arc<Mdb>,
    blob_mdb: Arc<Mdb>,
    view_blockhash: Option<BlockHash>,
}

impl TokenDataProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        let blob_mdb = Arc::new(mdb.clone_with_prefix(b"tokendata_blob:"));
        Self { mdb, blob_mdb, view_blockhash: None }
    }

    pub fn with_view_blockhash(&self, blockhash: Option<BlockHash>) -> Self {
        Self {
            mdb: Arc::clone(&self.mdb),
            blob_mdb: Arc::clone(&self.blob_mdb),
            view_blockhash: blockhash,
        }
    }

    pub fn with_height(&self, height: Option<u64>, height_present: bool) -> Result<Self> {
        if !height_present {
            return Ok(self.with_view_blockhash(None));
        }
        let Some(height) = height else {
            return Err(anyhow!("missing_or_invalid_height"));
        };
        let height_u32 = u32::try_from(height).map_err(|_| anyhow!("height_out_of_range"))?;
        // Resolve via the Mdb so remote-backed handles (explorer_espo_rpc_host)
        // ask the remote espo instead of the local versioned tree.
        let Some(blockhash) = self
            .mdb
            .blockhash_for_height(height_u32)
            .map_err(|e| anyhow!("tree lookup failed: {e}"))?
        else {
            return Err(anyhow!("height_not_indexed"));
        };
        Ok(self.with_view_blockhash(Some(blockhash)))
    }

    pub fn table(&self) -> TokenDataTable<'_> {
        TokenDataTable::new(self.mdb.as_ref())
    }

    pub fn blob_mdb(&self) -> &Mdb {
        self.blob_mdb.as_ref()
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

    fn raw_blob_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.blob_mdb.get(key).map_err(|e| anyhow!("blob_mdb.get failed: {e}"))
    }

    fn raw_blob_multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.blob_mdb
            .multi_get(keys)
            .map_err(|e| anyhow!("blob_mdb.multi_get failed: {e}"))
    }

    fn raw_scan_prefix_entries(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut entries = match blockhash {
            Some(blockhash) => self
                .mdb
                .scan_prefix_entries_at_blockhash(&blockhash, prefix)
                .map_err(|e| anyhow!("mdb.scan_prefix_entries_at_blockhash failed: {e}"))?,
            None => self
                .mdb
                .scan_prefix_entries(prefix)
                .map_err(|e| anyhow!("mdb.scan_prefix_entries failed: {e}"))?,
        };
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }

    fn raw_scan_range_entries_page(
        &self,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        blockhash: Option<BlockHash>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match blockhash {
            Some(blockhash) => self
                .mdb
                .scan_range_entries_page_at_blockhash(
                    &blockhash,
                    start_inclusive,
                    end_exclusive,
                    offset,
                    limit,
                    reverse,
                )
                .map_err(|e| anyhow!("mdb.scan_range_entries_page_at_blockhash failed: {e}")),
            None => self
                .mdb
                .scan_range_entries_page(start_inclusive, end_exclusive, offset, limit, reverse)
                .map_err(|e| anyhow!("mdb.scan_range_entries_page failed: {e}")),
        }
    }

    pub fn get_index_height(&self, params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        let table = self.table();
        let Some(bytes) = self
            .raw_get_at(&table.index_height_key(), params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(GetIndexHeightResult { height: None });
        };
        if bytes.len() != 4 {
            return Err(anyhow!("invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(GetIndexHeightResult { height: Some(u32::from_le_bytes(arr)) })
    }

    pub fn set_index_height(&self, params: SetIndexHeightParams) -> Result<()> {
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        self.mdb
            .put(&self.table().index_height_key(), &params.height.to_le_bytes())
            .map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn get_diesel_avg_price_paid_usd_by_height(&self, height: u32) -> Result<Option<[u8; 32]>> {
        let Some(bytes) =
            self.raw_get_at(&self.table().diesel_avg_price_usd_by_height_key(height), None)?
        else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Err(anyhow!("invalid diesel avg usd price length {}", bytes.len()));
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(Some(out))
    }

    pub fn get_diesel_avg_price_paid_usd_points_through_height(
        &self,
        max_height: u32,
    ) -> Result<Vec<(u32, [u8; 32])>> {
        let table = self.table();
        let prefix = table.diesel_avg_price_usd_by_height_prefix();
        let entries = self.mdb.scan_prefix_entries(&prefix)?;
        let mut out = Vec::new();
        for (key, value) in entries {
            let Some(height) = key.strip_prefix(prefix.as_slice()).and_then(decode_height_tail)
            else {
                continue;
            };
            if height > max_height {
                continue;
            }
            if value.len() != 32 {
                continue;
            }
            let mut price = [0u8; 32];
            price.copy_from_slice(&value);
            out.push((height, price));
        }
        out.sort_by_key(|(height, _)| *height);
        Ok(out)
    }

    pub fn set_batch(&self, params: SetBatchParams) -> Result<()> {
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &params.deletes {
                    wb.delete(key);
                }
                for (key, value) in &params.puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }

    pub fn set_blob_batch(&self, params: SetBlobBatchParams) -> Result<()> {
        self.blob_mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for (key, value) in &params.puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("blob_mdb.bulk_write failed: {e}"))
    }

    pub fn append_activity_index_values_batch(
        &self,
        entries: Vec<(Vec<u8>, Vec<u64>)>,
        next_chunk_id: &mut u64,
        puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
        blob_puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let entries =
            entries.into_iter().filter(|(_, values)| !values.is_empty()).collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(());
        }

        let meta_keys = entries.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let raw_states = self
            .mdb
            .multi_get(&meta_keys)
            .map_err(|e| anyhow!("mdb.multi_get activity index states failed: {e}"))?;

        let states = raw_states
            .into_iter()
            .map(|raw| {
                raw.and_then(|bytes| decode_activity_index_state(&bytes))
                    .unwrap_or_else(|| InlineOrExternalU64V1::Inline { items: Vec::new() })
            })
            .collect::<Vec<_>>();

        let table = self.table();
        let mut last_chunk_keys = Vec::new();
        let mut last_chunk_slots = Vec::new();
        for (idx, state) in states.iter().enumerate() {
            let InlineOrExternalU64V1::External { chunk_ids, len, chunk_size } = state else {
                continue;
            };
            if chunk_ids.is_empty() || entries[idx].1.is_empty() {
                continue;
            }
            let chunk_size_usize = usize::try_from(*chunk_size).unwrap_or(0).max(1);
            let rem = usize::try_from(len % chunk_size_usize as u64).unwrap_or(0);
            if rem == 0 {
                continue;
            }
            let last_chunk_id = *chunk_ids.last().unwrap_or(&0);
            last_chunk_slots.push(idx);
            last_chunk_keys.push(table.activity_index_chunk_blob_key(last_chunk_id));
        }

        let last_chunk_values = if last_chunk_keys.is_empty() {
            Vec::new()
        } else {
            self.raw_blob_multi_get(&last_chunk_keys)?
        };
        let mut preloaded_last_chunks = vec![None; entries.len()];
        for (slot, raw) in last_chunk_slots.into_iter().zip(last_chunk_values.into_iter()) {
            preloaded_last_chunks[slot] = Some(raw);
        }

        for (((meta_key, values), state), preloaded_last_chunk) in entries
            .into_iter()
            .zip(states.into_iter())
            .zip(preloaded_last_chunks.into_iter())
        {
            self.append_activity_index_values_with_state(
                meta_key,
                &values,
                state,
                preloaded_last_chunk,
                next_chunk_id,
                puts,
                blob_puts,
            )?;
        }
        Ok(())
    }

    pub fn reset_all_data(&self) -> Result<()> {
        const DELETE_CHUNK_SIZE: usize = 10_000;

        let logical_keys = self
            .mdb
            .scan_prefix_keys(b"")
            .map_err(|e| anyhow!("mdb.scan_prefix_keys failed during reset: {e}"))?;
        for chunk in logical_keys.chunks(DELETE_CHUNK_SIZE) {
            let deletes = chunk.to_vec();
            self.mdb
                .bulk_write(|wb: &mut MdbBatch<'_>| {
                    for key in &deletes {
                        wb.delete(key);
                    }
                })
                .map_err(|e| anyhow!("mdb.bulk_write reset failed: {e}"))?;
        }

        let blob_keys = self
            .blob_mdb
            .scan_prefix_keys(b"")
            .map_err(|e| anyhow!("blob_mdb.scan_prefix_keys failed during reset: {e}"))?;
        for chunk in blob_keys.chunks(DELETE_CHUNK_SIZE) {
            let deletes = chunk.to_vec();
            self.blob_mdb
                .bulk_write(|wb: &mut MdbBatch<'_>| {
                    for key in &deletes {
                        wb.delete(key);
                    }
                })
                .map_err(|e| anyhow!("blob_mdb.bulk_write reset failed: {e}"))?;
        }

        Ok(())
    }

    pub fn get_activity_row_counter(&self) -> Result<u64> {
        let key = self.table().activity_row_counter_key();
        Ok(self
            .raw_blob_get(&key)?
            .and_then(|bytes| decode_u64_value(&bytes).ok())
            .unwrap_or(0))
    }

    pub fn get_activity_index_chunk_counter(&self) -> Result<u64> {
        let key = self.table().activity_index_chunk_counter_key();
        Ok(self
            .raw_blob_get(&key)?
            .and_then(|bytes| decode_u64_value(&bytes).ok())
            .unwrap_or(0))
    }

    pub fn append_activity_index_values(
        &self,
        meta_key: Vec<u8>,
        values: &[u64],
        next_chunk_id: &mut u64,
        puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
        blob_puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<u64> {
        let current = self
            .raw_get_at(&meta_key, None)?
            .and_then(|raw| decode_activity_index_state(&raw))
            .unwrap_or_else(|| InlineOrExternalU64V1::Inline { items: Vec::new() });

        self.append_activity_index_values_with_state(
            meta_key,
            values,
            current,
            None,
            next_chunk_id,
            puts,
            blob_puts,
        )
    }

    fn append_activity_index_values_with_state(
        &self,
        meta_key: Vec<u8>,
        values: &[u64],
        current: InlineOrExternalU64V1,
        preloaded_last_chunk: Option<Option<Vec<u8>>>,
        next_chunk_id: &mut u64,
        puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
        blob_puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<u64> {
        if values.is_empty() {
            return Ok(activity_index_total(&current));
        }

        let table = self.table();
        let next_state = match current {
            InlineOrExternalU64V1::Inline { mut items } => {
                if items.len().saturating_add(values.len()) <= ACTIVITY_INDEX_INLINE_CAP {
                    items.extend_from_slice(values);
                    InlineOrExternalU64V1::Inline { items }
                } else {
                    let chunk_size = activity_index_chunk_size();
                    let mut merged = Vec::with_capacity(items.len().saturating_add(values.len()));
                    merged.append(&mut items);
                    merged.extend_from_slice(values);

                    let mut chunk_ids = Vec::new();
                    for chunk in merged.chunks(chunk_size) {
                        let id = *next_chunk_id;
                        *next_chunk_id = next_chunk_id.saturating_add(1);
                        chunk_ids.push(id);
                        blob_puts.push((
                            table.activity_index_chunk_blob_key(id),
                            encode_u64_chunk(chunk.to_vec())?,
                        ));
                    }
                    InlineOrExternalU64V1::External {
                        chunk_ids,
                        len: merged.len() as u64,
                        chunk_size: chunk_size as u32,
                    }
                }
            }
            InlineOrExternalU64V1::External { mut chunk_ids, len, chunk_size } => {
                let chunk_size_usize = usize::try_from(chunk_size).unwrap_or(0).max(1);
                let chunk_size_u64 = chunk_size_usize as u64;
                let mut pending = values;

                if !chunk_ids.is_empty() && !pending.is_empty() {
                    let rem = usize::try_from(len % chunk_size_u64).unwrap_or(0);
                    if rem > 0 {
                        let last_chunk_id = *chunk_ids.last().unwrap_or(&0);
                        let last_key = table.activity_index_chunk_blob_key(last_chunk_id);
                        let last_raw = match preloaded_last_chunk {
                            Some(raw) => raw,
                            None => self.raw_blob_get(&last_key)?,
                        };
                        let mut last_items =
                            last_raw.map(|raw| decode_u64_chunk(&raw)).unwrap_or_default();
                        if last_items.len() > rem {
                            last_items.truncate(rem);
                        }
                        if last_items.len() < rem {
                            return Err(anyhow!(
                                "token activity index chunk {} is shorter than canonical length {}",
                                last_chunk_id,
                                rem
                            ));
                        }
                        if last_items.len() < chunk_size_usize {
                            let free = chunk_size_usize.saturating_sub(last_items.len());
                            let take = free.min(pending.len());
                            last_items.extend_from_slice(&pending[..take]);
                            blob_puts.push((last_key, encode_u64_chunk(last_items)?));
                            pending = &pending[take..];
                        }
                    }
                }

                while !pending.is_empty() {
                    let take = chunk_size_usize.min(pending.len());
                    let id = *next_chunk_id;
                    *next_chunk_id = next_chunk_id.saturating_add(1);
                    chunk_ids.push(id);
                    blob_puts.push((
                        table.activity_index_chunk_blob_key(id),
                        encode_u64_chunk(pending[..take].to_vec())?,
                    ));
                    pending = &pending[take..];
                }

                InlineOrExternalU64V1::External {
                    chunk_ids,
                    len: len.saturating_add(values.len() as u64),
                    chunk_size: chunk_size_usize as u32,
                }
            }
        };

        let new_len = activity_index_total(&next_state);
        puts.push((meta_key, encode_activity_index_state(&next_state)?));
        Ok(new_len)
    }

    fn get_activity_rows_by_ids(&self, ids: &[u64]) -> Result<Vec<SchemaTokenActivityV1>> {
        let blob_keys: Vec<Vec<u8>> =
            ids.iter().map(|id| self.table().activity_row_blob_key(*id)).collect();
        Ok(self
            .raw_blob_multi_get(&blob_keys)?
            .into_iter()
            .filter_map(|raw| {
                raw.and_then(|bytes| SchemaTokenActivityV1::try_from_slice(&bytes).ok())
            })
            .collect())
    }

    fn get_activity_index_row_ids_page(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
        offset: usize,
        limit: usize,
        dir: SortDir,
    ) -> Result<(Vec<u64>, usize)> {
        let meta_key = self.table().activity_index_meta_key(prefix);
        let Some(raw) = self.raw_get_at(&meta_key, blockhash)? else {
            return Ok((Vec::new(), 0));
        };
        let Some(state) = decode_activity_index_state(&raw) else {
            return Ok((Vec::new(), 0));
        };
        let total = usize::try_from(activity_index_total(&state)).unwrap_or(usize::MAX);
        if limit == 0 || offset >= total {
            return Ok((Vec::new(), total));
        }

        match state {
            InlineOrExternalU64V1::Inline { items } => {
                let selected = match dir {
                    SortDir::Asc => items.into_iter().skip(offset).take(limit).collect(),
                    SortDir::Desc => items.into_iter().rev().skip(offset).take(limit).collect(),
                };
                Ok((selected, total))
            }
            InlineOrExternalU64V1::External { chunk_ids, len, chunk_size } => {
                let total_u64 = len;
                let take = limit.min(total.saturating_sub(offset));
                let (start, end, reverse) = match dir {
                    SortDir::Asc => {
                        let start = offset as u64;
                        (start, start.saturating_add(take as u64), false)
                    }
                    SortDir::Desc => {
                        let end = total_u64.saturating_sub(offset as u64);
                        (end.saturating_sub(take as u64), end, true)
                    }
                };
                let mut ids = self.read_activity_index_row_id_range(
                    &chunk_ids,
                    usize::try_from(chunk_size).unwrap_or(0).max(1),
                    start,
                    end,
                )?;
                if reverse {
                    ids.reverse();
                }
                Ok((ids, total))
            }
        }
    }

    fn get_activity_index_all_row_ids(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
    ) -> Result<Vec<u64>> {
        let total = self
            .raw_get_at(&self.table().activity_index_meta_key(prefix), blockhash)?
            .and_then(|raw| decode_activity_index_state(&raw))
            .map(|state| activity_index_total(&state))
            .unwrap_or(0);
        let (ids, _) = self.get_activity_index_row_ids_page(
            prefix,
            blockhash,
            0,
            usize::try_from(total).unwrap_or(usize::MAX),
            SortDir::Asc,
        )?;
        Ok(ids)
    }

    fn get_activity_index_total(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
    ) -> Result<usize> {
        Ok(self
            .raw_get_at(&self.table().activity_index_meta_key(prefix), blockhash)?
            .and_then(|raw| decode_activity_index_state(&raw))
            .map(|state| usize::try_from(activity_index_total(&state)).unwrap_or(usize::MAX))
            .unwrap_or(0))
    }

    fn get_activity_index_row_ids_range(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
        start: usize,
        end: usize,
    ) -> Result<Vec<u64>> {
        if end <= start {
            return Ok(Vec::new());
        }
        let meta_key = self.table().activity_index_meta_key(prefix);
        let Some(raw) = self.raw_get_at(&meta_key, blockhash)? else {
            return Ok(Vec::new());
        };
        let Some(state) = decode_activity_index_state(&raw) else {
            return Ok(Vec::new());
        };
        let total = usize::try_from(activity_index_total(&state)).unwrap_or(usize::MAX);
        if start >= total {
            return Ok(Vec::new());
        }
        let start = start.min(total);
        let end = end.min(total);
        if end <= start {
            return Ok(Vec::new());
        }

        match state {
            InlineOrExternalU64V1::Inline { items } => {
                Ok(items.into_iter().skip(start).take(end.saturating_sub(start)).collect())
            }
            InlineOrExternalU64V1::External { chunk_ids, chunk_size, .. } => self
                .read_activity_index_row_id_range(
                    &chunk_ids,
                    usize::try_from(chunk_size).unwrap_or(0).max(1),
                    start as u64,
                    end as u64,
                ),
        }
    }

    fn get_activity_row_at_index(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
        index: usize,
    ) -> Result<Option<SchemaTokenActivityV1>> {
        let ids = self.get_activity_index_row_ids_range(
            prefix,
            blockhash,
            index,
            index.saturating_add(1),
        )?;
        if ids.is_empty() {
            return Ok(None);
        }
        Ok(self.get_activity_rows_by_ids(&ids)?.into_iter().next())
    }

    fn find_activity_index_timestamp_window(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<(usize, usize)> {
        let total = self.get_activity_index_total(prefix, blockhash)?;
        if total == 0 {
            return Ok((0, 0));
        }

        let mut lower = 0usize;
        if let Some(target) = start_time {
            let mut lo = 0usize;
            let mut hi = total;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let Some(row) = self.get_activity_row_at_index(prefix, blockhash, mid)? else {
                    return Ok((0, 0));
                };
                if row.timestamp < target {
                    lo = mid.saturating_add(1);
                } else {
                    hi = mid;
                }
            }
            lower = lo;
        }

        let mut upper = total;
        if let Some(target) = end_time {
            let mut lo = lower;
            let mut hi = total;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let Some(row) = self.get_activity_row_at_index(prefix, blockhash, mid)? else {
                    return Ok((lower.min(total), lower.min(total)));
                };
                if row.timestamp <= target {
                    lo = mid.saturating_add(1);
                } else {
                    hi = mid;
                }
            }
            upper = lo;
        }

        Ok((lower.min(total), upper.min(total)))
    }

    fn read_activity_index_row_id_range(
        &self,
        chunk_ids: &[u64],
        chunk_size: usize,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>> {
        if start >= end || chunk_size == 0 {
            return Ok(Vec::new());
        }
        let first_chunk = usize::try_from(start / chunk_size as u64).unwrap_or(usize::MAX);
        let mut last_chunk_excl =
            usize::try_from((end + chunk_size as u64 - 1) / chunk_size as u64)
                .unwrap_or(usize::MAX);
        if first_chunk >= chunk_ids.len() {
            return Ok(Vec::new());
        }
        last_chunk_excl = last_chunk_excl.min(chunk_ids.len());
        if last_chunk_excl <= first_chunk {
            return Ok(Vec::new());
        }

        let keys = chunk_ids[first_chunk..last_chunk_excl]
            .iter()
            .map(|id| self.table().activity_index_chunk_blob_key(*id))
            .collect::<Vec<_>>();
        let chunks = self.raw_blob_multi_get(&keys)?;
        let mut out = Vec::new();
        for (offset, raw_chunk) in chunks.into_iter().enumerate() {
            let Some(raw_chunk) = raw_chunk else { continue };
            let items = decode_u64_chunk(&raw_chunk);
            let global_chunk_idx = first_chunk.saturating_add(offset);
            let chunk_start = (global_chunk_idx as u64).saturating_mul(chunk_size as u64);
            let from = usize::try_from(start.saturating_sub(chunk_start))
                .unwrap_or(usize::MAX)
                .min(items.len());
            let to = usize::try_from(end.saturating_sub(chunk_start))
                .unwrap_or(usize::MAX)
                .min(items.len());
            if from < to {
                out.extend_from_slice(&items[from..to]);
            }
        }
        Ok(out)
    }

    pub fn get_token_activity_page(
        &self,
        params: GetTokenActivityPageParams,
    ) -> Result<GetTokenActivityPageResult> {
        let table = self.table();
        let timestamp_prefix = table.token_activity_prefix(params.scope, &params.token);
        let source_sort = if params.start_time.is_some() || params.end_time.is_some() {
            TokenActivitySortField::Timestamp
        } else {
            params.sort_by
        };
        let prefix = match source_sort {
            TokenActivitySortField::Timestamp => timestamp_prefix.clone(),
            TokenActivitySortField::Amount => {
                table.token_activity_amount_prefix(params.scope, &params.token)
            }
        };
        self.get_activity_page_from_prefix(
            prefix,
            timestamp_prefix,
            params.blockhash,
            params.kind,
            source_sort,
            params.sort_by,
            params.dir,
            params.offset,
            params.limit,
            params.start_time,
            params.end_time,
            params.quote_amount_filter,
        )
    }

    pub fn get_address_activity_page(
        &self,
        params: GetAddressActivityPageParams,
    ) -> Result<GetTokenActivityPageResult> {
        let table = self.table();
        let timestamp_prefix = match params.token {
            Some(token) => {
                table.address_token_activity_prefix(params.scope, &params.address_spk, &token)
            }
            None => table.address_activity_prefix(params.scope, &params.address_spk),
        };
        let source_sort = if params.start_time.is_some() || params.end_time.is_some() {
            TokenActivitySortField::Timestamp
        } else {
            params.sort_by
        };
        let prefix = match (params.token, source_sort) {
            (Some(_token), TokenActivitySortField::Timestamp) => timestamp_prefix.clone(),
            (Some(token), TokenActivitySortField::Amount) => table
                .address_token_activity_amount_prefix(params.scope, &params.address_spk, &token),
            (None, TokenActivitySortField::Timestamp) => timestamp_prefix.clone(),
            (None, TokenActivitySortField::Amount) => {
                table.address_activity_amount_prefix(params.scope, &params.address_spk)
            }
        };
        self.get_activity_page_from_prefix(
            prefix,
            timestamp_prefix,
            params.blockhash,
            params.kind,
            source_sort,
            params.sort_by,
            params.dir,
            params.offset,
            params.limit,
            params.start_time,
            params.end_time,
            None,
        )
    }

    fn get_activity_page_from_prefix(
        &self,
        prefix: Vec<u8>,
        timestamp_prefix: Vec<u8>,
        blockhash: StateAt,
        kind: Option<TokenActivityKind>,
        source_sort: TokenActivitySortField,
        requested_sort: TokenActivitySortField,
        dir: SortDir,
        offset: usize,
        limit: usize,
        start_time: Option<u64>,
        end_time: Option<u64>,
        quote_amount_filter: Option<TokenActivityQuoteAmountFilter>,
    ) -> Result<GetTokenActivityPageResult> {
        let blockhash = blockhash.resolve(self.view_blockhash);
        if quote_amount_filter.is_some()
            && kind.is_none()
            && start_time.is_none()
            && end_time.is_none()
            && source_sort == requested_sort
        {
            return self.get_filtered_activity_page_from_prefix(
                prefix,
                timestamp_prefix,
                blockhash,
                source_sort,
                dir,
                offset,
                limit,
                quote_amount_filter,
            );
        }
        if matches!(source_sort, TokenActivitySortField::Timestamp)
            && matches!(requested_sort, TokenActivitySortField::Timestamp)
            && kind.is_none()
            && start_time.is_none()
            && end_time.is_none()
        {
            let (selected, total) =
                self.get_activity_index_row_ids_page(&prefix, blockhash, offset, limit, dir)?;
            let entries = self.get_activity_rows_by_ids(&selected)?;
            return Ok(GetTokenActivityPageResult { entries, total });
        }
        if matches!(source_sort, TokenActivitySortField::Amount)
            && matches!(requested_sort, TokenActivitySortField::Amount)
            && kind.is_none()
            && start_time.is_none()
            && end_time.is_none()
        {
            let total = self.get_activity_index_total(&timestamp_prefix, blockhash)?;
            if limit == 0 || offset >= total {
                return Ok(GetTokenActivityPageResult { entries: Vec::new(), total });
            }
            let end_exclusive = prefix_end_exclusive(&prefix);
            let page = self.raw_scan_range_entries_page(
                &prefix,
                end_exclusive.as_deref(),
                blockhash,
                offset,
                limit.min(total.saturating_sub(offset)),
                matches!(dir, SortDir::Desc),
            )?;
            let ids = page
                .into_iter()
                .filter_map(|(_, value)| decode_u64_value(&value).ok())
                .collect::<Vec<_>>();
            let entries = self.get_activity_rows_by_ids(&ids)?;
            return Ok(GetTokenActivityPageResult { entries, total });
        }

        let mut timeframe_applied = false;
        let mut timeframe_total = None;
        let timeframe_window = if matches!(source_sort, TokenActivitySortField::Timestamp)
            && (start_time.is_some() || end_time.is_some())
        {
            let (window_start, window_end) = self
                .find_activity_index_timestamp_window(&prefix, blockhash, start_time, end_time)?;
            Some((window_start, window_end))
        } else {
            None
        };

        if let Some((window_start, window_end)) = timeframe_window {
            timeframe_applied = true;
            let window_total = window_end.saturating_sub(window_start);
            timeframe_total = Some(window_total);
            if matches!(requested_sort, TokenActivitySortField::Timestamp) && kind.is_none() {
                if limit == 0 || offset >= window_total {
                    return Ok(GetTokenActivityPageResult {
                        entries: Vec::new(),
                        total: window_total,
                    });
                }
                let take = limit.min(window_total.saturating_sub(offset));
                let (range_start, range_end, reverse) = match dir {
                    SortDir::Asc => {
                        let start = window_start.saturating_add(offset);
                        (start, start.saturating_add(take), false)
                    }
                    SortDir::Desc => {
                        let end = window_end.saturating_sub(offset);
                        (end.saturating_sub(take), end, true)
                    }
                };
                let mut ids = self.get_activity_index_row_ids_range(
                    &prefix,
                    blockhash,
                    range_start,
                    range_end,
                )?;
                if reverse {
                    ids.reverse();
                }
                let entries = self.get_activity_rows_by_ids(&ids)?;
                return Ok(GetTokenActivityPageResult { entries, total: window_total });
            }
        }

        let ids = match source_sort {
            TokenActivitySortField::Timestamp => {
                if let Some((window_start, window_end)) = timeframe_window {
                    self.get_activity_index_row_ids_range(
                        &prefix,
                        blockhash,
                        window_start,
                        window_end,
                    )?
                } else {
                    self.get_activity_index_all_row_ids(&prefix, blockhash)?
                }
            }
            TokenActivitySortField::Amount => self
                .raw_scan_prefix_entries(&prefix, blockhash)?
                .into_iter()
                .filter_map(|(_, value)| decode_u64_value(&value).ok())
                .collect::<Vec<_>>(),
        };

        let mut entries: Vec<(SchemaTokenActivityV1, (u128, u64, u32, [u8; 32]))> = self
            .get_activity_rows_by_ids(&ids)?
            .into_iter()
            .map(|row| {
                let meta = match source_sort {
                    TokenActivitySortField::Timestamp => (0, row.timestamp, 0, row.txid),
                    TokenActivitySortField::Amount => {
                        (amount_from_row(&row), row.timestamp, 0, row.txid)
                    }
                };
                (row, meta)
            })
            .collect();

        if !timeframe_applied
            && matches!(source_sort, TokenActivitySortField::Timestamp)
            && (start_time.is_some() || end_time.is_some())
        {
            entries.sort_by(|a, b| a.1.cmp(&b.1));
            let start_idx = start_time
                .map(|target| entries.partition_point(|(_, meta)| meta.1 < target))
                .unwrap_or(0);
            let end_idx = end_time
                .map(|target| entries.partition_point(|(_, meta)| meta.1 <= target))
                .unwrap_or(entries.len());
            entries = entries
                .into_iter()
                .skip(start_idx)
                .take(end_idx.saturating_sub(start_idx))
                .collect();
        }

        entries = entries
            .into_iter()
            .filter(|(entry, _)| kind.map(|k| entry.kind == k).unwrap_or(true))
            .filter(|(entry, _)| {
                timeframe_applied || start_time.map(|s| entry.timestamp >= s).unwrap_or(true)
            })
            .filter(|(entry, _)| {
                timeframe_applied || end_time.map(|e| entry.timestamp <= e).unwrap_or(true)
            })
            .filter(|(entry, _)| row_matches_quote_amount_filter(entry, quote_amount_filter))
            .collect();

        match requested_sort {
            TokenActivitySortField::Timestamp => {
                entries.sort_by(|a, b| timestamp_sort_tuple(&a.0).cmp(&timestamp_sort_tuple(&b.0)));
            }
            TokenActivitySortField::Amount => {
                entries.sort_by(|a, b| amount_sort_tuple(&a.0).cmp(&amount_sort_tuple(&b.0)));
            }
        }
        if matches!(dir, SortDir::Desc) {
            entries.reverse();
        }
        let total = if timeframe_applied && kind.is_none() {
            timeframe_total.unwrap_or(entries.len())
        } else {
            entries.len()
        };
        let page = entries.into_iter().skip(offset).take(limit).map(|(entry, _)| entry).collect();
        Ok(GetTokenActivityPageResult { entries: page, total })
    }

    fn get_filtered_activity_page_from_prefix(
        &self,
        prefix: Vec<u8>,
        timestamp_prefix: Vec<u8>,
        blockhash: Option<BlockHash>,
        source_sort: TokenActivitySortField,
        dir: SortDir,
        offset: usize,
        limit: usize,
        quote_amount_filter: Option<TokenActivityQuoteAmountFilter>,
    ) -> Result<GetTokenActivityPageResult> {
        if limit == 0 {
            return Ok(GetTokenActivityPageResult { entries: Vec::new(), total: 0 });
        }

        let mut seen = 0usize;
        let mut out = Vec::with_capacity(limit.saturating_add(1));
        let batch = limit.max(128).min(512);

        match source_sort {
            TokenActivitySortField::Timestamp => {
                let total_unfiltered =
                    self.get_activity_index_total(&timestamp_prefix, blockhash)?;
                let mut scan_offset = 0usize;
                while scan_offset < total_unfiltered && out.len() <= limit {
                    let take = batch.min(total_unfiltered.saturating_sub(scan_offset));
                    let (ids, _) = self.get_activity_index_row_ids_page(
                        &prefix,
                        blockhash,
                        scan_offset,
                        take,
                        dir,
                    )?;
                    if ids.is_empty() {
                        break;
                    }
                    scan_offset = scan_offset.saturating_add(ids.len());
                    for row in self.get_activity_rows_by_ids(&ids)? {
                        if !row_matches_quote_amount_filter(&row, quote_amount_filter) {
                            continue;
                        }
                        if seen < offset {
                            seen = seen.saturating_add(1);
                            continue;
                        }
                        out.push(row);
                        if out.len() > limit {
                            break;
                        }
                    }
                }
            }
            TokenActivitySortField::Amount => {
                let end_exclusive = prefix_end_exclusive(&prefix);
                let mut scan_offset = 0usize;
                loop {
                    let page = self.raw_scan_range_entries_page(
                        &prefix,
                        end_exclusive.as_deref(),
                        blockhash,
                        scan_offset,
                        batch,
                        matches!(dir, SortDir::Desc),
                    )?;
                    if page.is_empty() {
                        break;
                    }
                    scan_offset = scan_offset.saturating_add(page.len());
                    let ids = page
                        .into_iter()
                        .filter_map(|(_, value)| decode_u64_value(&value).ok())
                        .collect::<Vec<_>>();
                    for row in self.get_activity_rows_by_ids(&ids)? {
                        if !row_matches_quote_amount_filter(&row, quote_amount_filter) {
                            continue;
                        }
                        if seen < offset {
                            seen = seen.saturating_add(1);
                            continue;
                        }
                        out.push(row);
                        if out.len() > limit {
                            break;
                        }
                    }
                    if out.len() > limit {
                        break;
                    }
                }
            }
        }

        let has_more = out.len() > limit;
        if has_more {
            out.truncate(limit);
        }
        let total = offset.saturating_add(out.len()).saturating_add(if has_more { 1 } else { 0 });
        Ok(GetTokenActivityPageResult { entries: out, total })
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

pub struct SetBatchParams {
    pub blockhash: StateAt,
    pub deletes: Vec<Vec<u8>>,
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct SetBlobBatchParams {
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct GetTokenActivityPageParams {
    pub blockhash: StateAt,
    pub token: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
    pub kind: Option<TokenActivityKind>,
    pub scope: TokenActivityScope,
    pub sort_by: TokenActivitySortField,
    pub dir: SortDir,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub quote_amount_filter: Option<TokenActivityQuoteAmountFilter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenActivityQuoteAmountFilter {
    pub quote: SchemaAlkaneId,
    pub min_amount: u128,
}

fn decode_height_tail(bytes: &[u8]) -> Option<u32> {
    if bytes.len() != 4 {
        return None;
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Some(u32::from_be_bytes(arr))
}

pub struct GetAddressActivityPageParams {
    pub blockhash: StateAt,
    pub address_spk: Vec<u8>,
    pub token: Option<SchemaAlkaneId>,
    pub offset: usize,
    pub limit: usize,
    pub kind: Option<TokenActivityKind>,
    pub scope: TokenActivityScope,
    pub sort_by: TokenActivitySortField,
    pub dir: SortDir,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
}

pub struct GetTokenActivityPageResult {
    pub entries: Vec<SchemaTokenActivityV1>,
    pub total: usize,
}

fn activity_index_chunk_size() -> usize {
    get_address_index_chunk_size().max(1)
}

fn activity_index_total(state: &InlineOrExternalU64V1) -> u64 {
    match state {
        InlineOrExternalU64V1::Inline { items } => items.len() as u64,
        InlineOrExternalU64V1::External { len, .. } => *len,
    }
}

fn decode_activity_index_state(bytes: &[u8]) -> Option<InlineOrExternalU64V1> {
    InlineOrExternalU64V1::try_from_slice(bytes).ok()
}

fn encode_activity_index_state(state: &InlineOrExternalU64V1) -> Result<Vec<u8>> {
    borsh::to_vec(state).map_err(|e| anyhow!("encode token activity index state failed: {e}"))
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

fn decode_u64_chunk(bytes: &[u8]) -> Vec<u64> {
    U64ChunkV1::try_from_slice(bytes).map(|chunk| chunk.items).unwrap_or_default()
}

fn encode_u64_chunk(items: Vec<u64>) -> Result<Vec<u8>> {
    borsh::to_vec(&U64ChunkV1 { items })
        .map_err(|e| anyhow!("encode token activity index chunk failed: {e}"))
}

fn activity_kind_code(kind: TokenActivityKind) -> u8 {
    match kind {
        TokenActivityKind::Buy => 0,
        TokenActivityKind::Sell => 1,
        TokenActivityKind::LiquidityAdd => 2,
        TokenActivityKind::LiquidityRemove => 3,
        TokenActivityKind::PoolCreate => 4,
        TokenActivityKind::Mint => 5,
    }
}

pub fn amount_from_row(row: &SchemaTokenActivityV1) -> u128 {
    row.token_delta.unsigned_abs()
}

fn timestamp_sort_tuple(row: &SchemaTokenActivityV1) -> (u64, u32, [u8; 32]) {
    (row.timestamp, 0, row.txid)
}

fn amount_sort_tuple(row: &SchemaTokenActivityV1) -> (u128, u64, u32, [u8; 32]) {
    (amount_from_row(row), row.timestamp, 0, row.txid)
}

fn row_matches_quote_amount_filter(
    row: &SchemaTokenActivityV1,
    filter: Option<TokenActivityQuoteAmountFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    row.source == TokenActivitySource::Market
        && row.counter_token == Some(filter.quote)
        && row.counter_delta.unsigned_abs() >= filter.min_amount
}

pub fn scopes_for_source(source: TokenActivitySource) -> [TokenActivityScope; 2] {
    match source {
        TokenActivitySource::Market => [TokenActivityScope::All, TokenActivityScope::Market],
        TokenActivitySource::Mint => [TokenActivityScope::All, TokenActivityScope::Mint],
    }
}

fn pointer_counter_key(entity: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PTR_V1_PREFIX.len() + entity.len() + 9);
    key.extend_from_slice(PTR_V1_PREFIX);
    key.extend_from_slice(entity);
    key.extend_from_slice(b"/counter");
    key
}

fn pointer_blob_key(entity: &[u8], id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PTR_V1_PREFIX.len() + entity.len() + 14);
    key.extend_from_slice(PTR_V1_PREFIX);
    key.extend_from_slice(entity);
    key.extend_from_slice(b"/blob/");
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn encode_u64_value(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_u64_value(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(anyhow!("invalid u64 value length {}", bytes.len()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn push_spk(dst: &mut Vec<u8>, spk: &[u8]) {
    let len = spk.len().min(u16::MAX as usize) as u16;
    dst.extend_from_slice(&len.to_be_bytes());
    dst.extend_from_slice(&spk[..len as usize]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn new_provider_with_tempdb() -> TokenDataProvider {
        let dir = TempDir::new().expect("tempdir");
        let mdb = Arc::new(Mdb::open(dir.path(), b"tokendata_test:").expect("open mdb"));
        // Keep tempdir alive for process lifetime in tests.
        std::mem::forget(dir);
        TokenDataProvider::new(mdb)
    }

    #[test]
    fn activity_index_append_truncates_stale_tail_from_blob_chunk() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let prefix = b"/test/activity".to_vec();
        let meta_key = table.activity_index_meta_key(&prefix);
        let chunk_id = 11u64;
        let chunk_key = table.activity_index_chunk_blob_key(chunk_id);

        provider
            .set_blob_batch(SetBlobBatchParams {
                puts: vec![(
                    chunk_key.clone(),
                    encode_u64_chunk(vec![10, 11, 99, 100]).expect("encode chunk"),
                )],
            })
            .expect("write stale chunk");
        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![(
                    meta_key.clone(),
                    encode_activity_index_state(&InlineOrExternalU64V1::External {
                        chunk_ids: vec![chunk_id],
                        len: 2,
                        chunk_size: 4,
                    })
                    .expect("encode state"),
                )],
                deletes: Vec::new(),
            })
            .expect("write meta");

        let mut next_chunk_id = 12u64;
        let mut puts = Vec::new();
        let mut blob_puts = Vec::new();
        let new_len = provider
            .append_activity_index_values(
                meta_key,
                &[12, 13],
                &mut next_chunk_id,
                &mut puts,
                &mut blob_puts,
            )
            .expect("append");

        assert_eq!(new_len, 4);
        assert_eq!(next_chunk_id, 12);
        let (_, rewritten_chunk) = blob_puts
            .iter()
            .find(|(key, _)| key == &chunk_key)
            .expect("rewritten last chunk");
        assert_eq!(decode_u64_chunk(rewritten_chunk), vec![10, 11, 12, 13]);
    }
}
