use super::consts::KEY_INDEX_HEIGHT;
use super::schemas::SchemaWrapEventV1;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::pointers::{KvPointer, ListPointer};
use crate::runtime::state_at::StateAt;
use anyhow::{Result, anyhow};
use bitcoin::BlockHash;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashSet;
use std::sync::Arc;

#[allow(non_snake_case)]
#[derive(Clone)]
pub struct SubfrostTable<'a> {
    pub ROOT: KvPointer<'a>,
    pub INDEX_HEIGHT: KvPointer<'a>,
    pub WRAP_EVENTS_ALL: ListPointer<'a>,
    pub WRAP_EVENTS_BY_ADDRESS: ListPointer<'a>,
    pub UNWRAP_EVENTS_ALL: ListPointer<'a>,
    pub UNWRAP_EVENTS_BY_ADDRESS: ListPointer<'a>,
    pub UNWRAP_TOTAL_LATEST: KvPointer<'a>,
    pub UNWRAP_TOTAL_BY_HEIGHT: KvPointer<'a>,
    pub UNWRAP_TOTAL_LATEST_SUCCESS: KvPointer<'a>,
    pub UNWRAP_TOTAL_BY_HEIGHT_SUCCESS: KvPointer<'a>,
    pub UNWRAP_TOTAL_POINTS_ALL: ListPointer<'a>,
    pub UNWRAP_TOTAL_POINTS_SUCCESS: ListPointer<'a>,
}

impl<'a> SubfrostTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = KvPointer::root(mdb);
        SubfrostTable {
            ROOT: root.clone(),
            INDEX_HEIGHT: root.select(KEY_INDEX_HEIGHT),
            WRAP_EVENTS_ALL: root.list_keyword("/wrap_events_all/v2/"),
            WRAP_EVENTS_BY_ADDRESS: root.list_keyword("/wrap_events_by_address/v2/"),
            UNWRAP_EVENTS_ALL: root.list_keyword("/unwrap_events_all/v2/"),
            UNWRAP_EVENTS_BY_ADDRESS: root.list_keyword("/unwrap_events_by_address/v2/"),
            UNWRAP_TOTAL_LATEST: root.keyword("/unwrap_total_latest/v1"),
            UNWRAP_TOTAL_BY_HEIGHT: root.keyword("/unwrap_total_by_height/v1/"),
            UNWRAP_TOTAL_LATEST_SUCCESS: root.keyword("/unwrap_total_latest_success/v1"),
            UNWRAP_TOTAL_BY_HEIGHT_SUCCESS: root.keyword("/unwrap_total_by_height_success/v1/"),
            UNWRAP_TOTAL_POINTS_ALL: root.list_keyword("/unwrap_total_points/v2/all/"),
            UNWRAP_TOTAL_POINTS_SUCCESS: root.list_keyword("/unwrap_total_points/v2/success/"),
        }
    }

    pub fn wrap_events_by_address_prefix(&self, spk: &[u8]) -> Vec<u8> {
        let mut k = self.WRAP_EVENTS_BY_ADDRESS.key().to_vec();
        push_spk(&mut k, spk);
        k.push(b'/');
        k
    }

    pub fn unwrap_events_by_address_prefix(&self, spk: &[u8]) -> Vec<u8> {
        let mut k = self.UNWRAP_EVENTS_BY_ADDRESS.key().to_vec();
        push_spk(&mut k, spk);
        k.push(b'/');
        k
    }

    pub fn unwrap_total_latest_key(&self, successful: bool) -> Vec<u8> {
        if successful {
            self.UNWRAP_TOTAL_LATEST_SUCCESS.key().to_vec()
        } else {
            self.UNWRAP_TOTAL_LATEST.key().to_vec()
        }
    }

    pub fn unwrap_total_by_height_prefix(&self, successful: bool) -> Vec<u8> {
        if successful {
            self.UNWRAP_TOTAL_BY_HEIGHT_SUCCESS.key().to_vec()
        } else {
            self.UNWRAP_TOTAL_BY_HEIGHT.key().to_vec()
        }
    }

    pub fn unwrap_total_by_height_key(&self, height: u32, successful: bool) -> Vec<u8> {
        let mut k = self.unwrap_total_by_height_prefix(successful);
        k.extend_from_slice(&height.to_be_bytes());
        k
    }

    pub fn unwrap_total_points_prefix(&self, successful: bool) -> Vec<u8> {
        if successful {
            self.UNWRAP_TOTAL_POINTS_SUCCESS.key().to_vec()
        } else {
            self.UNWRAP_TOTAL_POINTS_ALL.key().to_vec()
        }
    }

    pub fn list_length_key(&self, list_prefix: &[u8]) -> Vec<u8> {
        let mut k = list_prefix.to_vec();
        k.extend_from_slice(b"length");
        k
    }

    pub fn list_item_key(&self, list_prefix: &[u8], idx: u64) -> Vec<u8> {
        let mut k = list_prefix.to_vec();
        k.extend_from_slice(b"item/");
        k.extend_from_slice(&idx.to_be_bytes());
        k
    }
}

fn push_spk(dst: &mut Vec<u8>, spk: &[u8]) {
    let len = spk.len().min(u16::MAX as usize) as u16;
    dst.extend_from_slice(&len.to_be_bytes());
    dst.extend_from_slice(&spk[..len as usize]);
}

fn decode_wrap_event(bytes: &[u8]) -> Result<SchemaWrapEventV1> {
    Ok(SchemaWrapEventV1::try_from_slice(bytes)?)
}

fn encode_wrap_event(event: &SchemaWrapEventV1) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(event)?)
}

#[derive(Clone, Copy, Debug, BorshSerialize, BorshDeserialize)]
pub struct UnwrapTotalPoint {
    pub height: u32,
    pub total: u128,
}

fn decode_u128_value(bytes: &[u8]) -> Option<u128> {
    if bytes.len() != 16 {
        return None;
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Some(u128::from_be_bytes(arr))
}

fn encode_u64_le(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn decode_u64_le(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Some(u64::from_le_bytes(arr))
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

#[derive(Clone)]
pub struct SubfrostProvider {
    mdb: Arc<Mdb>,
    view_blockhash: Option<BlockHash>,
}

impl SubfrostProvider {
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

    pub fn table(&self) -> SubfrostTable<'_> {
        SubfrostTable::new(self.mdb.as_ref())
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb.as_ref()
    }

    fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.view_blockhash {
            Some(blockhash) => self
                .mdb
                .get_at_blockhash(&blockhash, key)
                .map_err(|e| anyhow!("mdb.get_at_blockhash failed: {e}")),
            None => self.mdb.get(key).map_err(|e| anyhow!("mdb.get failed: {e}")),
        }
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

    fn raw_multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        match self.view_blockhash {
            Some(_blockhash) => {
                let mut out = Vec::with_capacity(keys.len());
                for key in keys {
                    out.push(self.raw_get(key)?);
                }
                Ok(out)
            }
            None => self.mdb.multi_get(keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}")),
        }
    }

    fn read_u64_len(&self, key: &[u8]) -> Result<u64> {
        Ok(self.raw_get(key)?.and_then(|v| decode_u64_le(&v)).unwrap_or(0))
    }

    fn read_event_list_all(&self, list_prefix: &[u8]) -> Result<Vec<SchemaWrapEventV1>> {
        let table = self.table();
        let len_key = table.list_length_key(list_prefix);
        let len = self.read_u64_len(&len_key)? as usize;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut keys = Vec::with_capacity(len);
        for idx in 0..len {
            keys.push(table.list_item_key(list_prefix, idx as u64));
        }

        let values = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(len);
        for raw in values {
            let Some(raw) = raw else { continue };
            out.push(decode_wrap_event(&raw)?);
        }
        Ok(out)
    }

    fn read_unwrap_total_points_all(&self, successful: bool) -> Result<Vec<UnwrapTotalPoint>> {
        let table = self.table();
        let list_prefix = table.unwrap_total_points_prefix(successful);
        let len_key = table.list_length_key(&list_prefix);
        let len = self.read_u64_len(&len_key)? as usize;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut keys = Vec::with_capacity(len);
        for idx in 0..len {
            keys.push(table.list_item_key(&list_prefix, idx as u64));
        }

        let values = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(len);
        for raw in values {
            let Some(raw) = raw else { continue };
            out.push(UnwrapTotalPoint::try_from_slice(&raw)?);
        }
        Ok(out)
    }

    pub fn get_raw_value(&self, params: GetRawValueParams) -> Result<GetRawValueResult> {
        let value = self.raw_get_at(&params.key, params.blockhash.resolve(self.view_blockhash))?;
        Ok(GetRawValueResult { value })
    }

    pub fn set_batch(&self, params: SetBatchParams) -> Result<()> {
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        let (puts, deletes) = dedupe_batch_ops(params.puts, params.deletes);
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

    pub fn build_event_list_appends(
        &self,
        params: BuildEventListAppendsParams,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if params.events.is_empty() {
            return Ok(Vec::new());
        }

        let table = self.table();
        let len_key = table.list_length_key(&params.list_prefix);
        let mut len = self.read_u64_len(&len_key)?;

        let mut puts = Vec::with_capacity(params.events.len() + 1);
        for ev in params.events {
            puts.push((table.list_item_key(&params.list_prefix, len), encode_wrap_event(&ev)?));
            len = len.saturating_add(1);
        }
        puts.push((len_key, encode_u64_le(len).to_vec()));
        Ok(puts)
    }

    pub fn build_unwrap_total_point_appends(
        &self,
        params: BuildUnwrapTotalPointAppendsParams,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if params.points.is_empty() {
            return Ok(Vec::new());
        }

        let table = self.table();
        let list_prefix = table.unwrap_total_points_prefix(params.successful);
        let len_key = table.list_length_key(&list_prefix);
        let mut len = self.read_u64_len(&len_key)?;

        let mut puts = Vec::with_capacity(params.points.len() + 1);
        for point in params.points {
            puts.push((table.list_item_key(&list_prefix, len), borsh::to_vec(&point)?));
            len = len.saturating_add(1);
        }
        puts.push((len_key, encode_u64_le(len).to_vec()));
        Ok(puts)
    }

    pub fn get_index_height(&self, params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        crate::debug_timer_log!("get_index_height");
        let table = self.table();
        let Some(bytes) = self
            .raw_get_at(table.INDEX_HEIGHT.key(), params.blockhash.resolve(self.view_blockhash))?
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
        crate::debug_timer_log!("set_index_height");
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        let table = self.table();
        table.INDEX_HEIGHT.put(&params.height.to_le_bytes())
    }

    pub fn get_wrap_events_by_address(
        &self,
        params: GetWrapEventsByAddressParams,
    ) -> Result<GetWrapEventsResult> {
        crate::debug_timer_log!("get_wrap_events_by_address");
        let view = match params.blockhash {
            StateAt::Block(blockhash) => self.with_view_blockhash(Some(blockhash)),
            StateAt::Latest => self.with_height(params.height, params.height_present)?,
        };
        let table = view.table();
        let prefix = table.wrap_events_by_address_prefix(&params.address_spk);
        read_events_from_list(&view, &prefix, params.offset, params.limit, params.successful)
    }

    pub fn get_unwrap_events_by_address(
        &self,
        params: GetUnwrapEventsByAddressParams,
    ) -> Result<GetWrapEventsResult> {
        crate::debug_timer_log!("get_unwrap_events_by_address");
        let view = match params.blockhash {
            StateAt::Block(blockhash) => self.with_view_blockhash(Some(blockhash)),
            StateAt::Latest => self.with_height(params.height, params.height_present)?,
        };
        let table = view.table();
        let prefix = table.unwrap_events_by_address_prefix(&params.address_spk);
        read_events_from_list(&view, &prefix, params.offset, params.limit, params.successful)
    }

    pub fn get_wrap_events_all(
        &self,
        params: GetWrapEventsAllParams,
    ) -> Result<GetWrapEventsResult> {
        crate::debug_timer_log!("get_wrap_events_all");
        let view = match params.blockhash {
            StateAt::Block(blockhash) => self.with_view_blockhash(Some(blockhash)),
            StateAt::Latest => self.with_height(params.height, params.height_present)?,
        };
        let table = view.table();
        let prefix = table.WRAP_EVENTS_ALL.key().to_vec();
        read_events_from_list(&view, &prefix, params.offset, params.limit, params.successful)
    }

    pub fn get_unwrap_events_all(
        &self,
        params: GetUnwrapEventsAllParams,
    ) -> Result<GetWrapEventsResult> {
        crate::debug_timer_log!("get_unwrap_events_all");
        let view = match params.blockhash {
            StateAt::Block(blockhash) => self.with_view_blockhash(Some(blockhash)),
            StateAt::Latest => self.with_height(params.height, params.height_present)?,
        };
        let table = view.table();
        let prefix = table.UNWRAP_EVENTS_ALL.key().to_vec();
        read_events_from_list(&view, &prefix, params.offset, params.limit, params.successful)
    }

    pub fn get_unwrap_total_latest(
        &self,
        params: GetUnwrapTotalLatestParams,
    ) -> Result<GetUnwrapTotalLatestResult> {
        crate::debug_timer_log!("get_unwrap_total_latest");
        let view = match params.blockhash {
            StateAt::Block(blockhash) => self.with_view_blockhash(Some(blockhash)),
            StateAt::Latest => self.with_height(params.height, params.height_present)?,
        };
        let table = view.table();
        let key = table.unwrap_total_latest_key(params.successful);
        let total = view.raw_get(&key)?.and_then(|v| decode_u128_value(&v)).unwrap_or(0);
        Ok(GetUnwrapTotalLatestResult { total })
    }

    pub fn get_unwrap_total_at_or_before(
        &self,
        params: GetUnwrapTotalAtOrBeforeParams,
    ) -> Result<GetUnwrapTotalAtOrBeforeResult> {
        crate::debug_timer_log!("get_unwrap_total_at_or_before");
        let view = self.with_view_blockhash(params.blockhash.resolve(self.view_blockhash));
        let points = view.read_unwrap_total_points_all(params.successful)?;
        if points.is_empty() {
            return Ok(GetUnwrapTotalAtOrBeforeResult { total: None });
        }

        let mut lo = 0usize;
        let mut hi = points.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if points[mid].height <= params.height {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo == 0 {
            return Ok(GetUnwrapTotalAtOrBeforeResult { total: None });
        }
        Ok(GetUnwrapTotalAtOrBeforeResult { total: Some(points[lo - 1].total) })
    }
}

fn read_events_from_list(
    provider: &SubfrostProvider,
    list_prefix: &[u8],
    offset: usize,
    limit: usize,
    successful: Option<bool>,
) -> Result<GetWrapEventsResult> {
    let all = provider.read_event_list_all(list_prefix)?;
    if all.is_empty() {
        return Ok(GetWrapEventsResult { entries: Vec::new(), total: 0 });
    }

    let mut total = 0usize;
    let mut out = Vec::new();
    let mut seen = 0usize;

    for entry in all.into_iter().rev() {
        if let Some(want) = successful {
            if want && !entry.success {
                continue;
            }
        }
        total = total.saturating_add(1);
        if seen < offset {
            seen = seen.saturating_add(1);
            continue;
        }
        if out.len() < limit {
            out.push(entry);
        }
    }

    Ok(GetWrapEventsResult { entries: out, total })
}

pub struct GetRawValueParams {
    pub blockhash: StateAt,

    pub key: Vec<u8>,
}

pub struct GetRawValueResult {
    pub value: Option<Vec<u8>>,
}

pub struct SetBatchParams {
    pub blockhash: StateAt,

    pub deletes: Vec<Vec<u8>>,
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct BuildEventListAppendsParams {
    pub list_prefix: Vec<u8>,
    pub events: Vec<SchemaWrapEventV1>,
}

pub struct BuildUnwrapTotalPointAppendsParams {
    pub successful: bool,
    pub points: Vec<UnwrapTotalPoint>,
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

pub struct GetWrapEventsByAddressParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
    pub successful: Option<bool>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct GetUnwrapEventsByAddressParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
    pub successful: Option<bool>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct GetWrapEventsAllParams {
    pub blockhash: StateAt,

    pub offset: usize,
    pub limit: usize,
    pub successful: Option<bool>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct GetUnwrapEventsAllParams {
    pub blockhash: StateAt,

    pub offset: usize,
    pub limit: usize,
    pub successful: Option<bool>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct GetUnwrapTotalLatestParams {
    pub blockhash: StateAt,

    pub successful: bool,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct GetUnwrapTotalLatestResult {
    pub total: u128,
}

pub struct GetUnwrapTotalAtOrBeforeParams {
    pub blockhash: StateAt,

    pub height: u32,
    pub successful: bool,
}

pub struct GetUnwrapTotalAtOrBeforeResult {
    pub total: Option<u128>,
}

pub struct GetWrapEventsResult {
    pub entries: Vec<SchemaWrapEventV1>,
    pub total: usize,
}

#[allow(dead_code)]
pub fn encode_wrap_event_value(event: &SchemaWrapEventV1) -> Result<Vec<u8>> {
    encode_wrap_event(event)
}

#[allow(dead_code)]
pub fn decode_wrap_event_value(bytes: &[u8]) -> Result<SchemaWrapEventV1> {
    decode_wrap_event(bytes)
}
