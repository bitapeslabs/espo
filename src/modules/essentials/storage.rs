use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoTrace, prettyify_protobuf_trace_json,
};
use crate::config::{
    get_address_index_chunk_size, get_bitcoind_rpc_client, get_electrum_like, get_espo_db,
    get_metashrew, get_network,
};
use crate::modules::essentials::utils::balances::{
    SignedU128, get_address_activity_for_address, get_alkane_balances,
    get_alkane_balances_at_or_before, get_alkane_volume_for_source, get_balance_for_address,
    get_holders_for_alkane, get_orbital_holders_for_factory, get_orbital_volume_for_factory,
    get_outpoint_address, get_outpoint_balances_with_spent_batch, get_total_received_for_alkane,
    get_transfer_volume_for_alkane,
};
use crate::modules::essentials::utils::inspections::{AlkaneCreationRecord, inspection_to_json};
use crate::modules::essentials::utils::names::{
    display_alkane_name_and_symbol, normalize_alkane_name,
};
use crate::modules::runes::main::runes_enabled_from_global_config;
use crate::modules::runes::storage::{RuneBalance, RunesProvider};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::pointers::{CursorScanPage, KvPointer, ListNonMutatePointer, ListPointer};
use crate::runtime::state_at::StateAt;
use crate::runtime::tree_db::get_global_tree_db;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use alkanes_support::proto::alkanes::AlkanesTrace;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{
    Address, AddressType, BlockHash, Network, ScriptBuf, Transaction, Txid, block::Header,
};
use bitcoincore_rpc::RpcApi;
use borsh::{BorshDeserialize, BorshSerialize};
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use serde_json::{Value, json, map::Map};

use crate::runtime::mempool::{
    MempoolBlockTx, MempoolEntry, get_mempool_index_transactions_ordered_by_block_and_fee,
    get_seen_txids_page, get_tx_from_mempool, pending_by_txid, pending_for_address,
};
use crate::utils::electrum_like::{AddressHistoryEntry, AddressUtxo, ElectrumLikeBackend};
pub use crate::utils::fee_rates::{BlockFeeRateSummary, compute_block_fee_rate_summary};
use anyhow::{Result, anyhow};
use hex;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

const ADDRESS_V2_PREFIX: &[u8] = b"/address/v2/";
const OUTPOINT_V2_PREFIX: &[u8] = b"/outpoint/v2/";
const ALKANE_V2_PREFIX: &[u8] = b"/alkane/v2/";
const TX_V2_PREFIX: &[u8] = b"/tx/v2/";
const OUTPOINT_V2_APPEND_PREFIX: &[u8] = b"/outpoint/v2a/";
const TX_V2_APPEND_PREFIX: &[u8] = b"/tx/v2a/";
const OUTPOINT_V2_POINT_PREFIX: &[u8] = b"/outpoint/v2p/";
const TX_V2_POINT_PREFIX: &[u8] = b"/tx/v2p/";
const PTR_V1_PREFIX: &[u8] = b"/ptr/v1/";
const PTR_ENTITY_OUTPOINT: &[u8] = b"outpoint";
const PTR_ENTITY_ALKANE_TX: &[u8] = b"alkane_tx";
const PTR_ENTITY_ADDR_OUTPOINT_IDX_CHUNK: &[u8] = b"addr_outpoint_idx_chunk";
const PTR_ENTITY_ADDR_ALKANE_TX_CHUNK: &[u8] = b"addr_alkane_tx_chunk";
const PTR_ENTITY_ALKANE_BALANCE_TXS_BY_TOKEN_CHUNK: &[u8] = b"alkane_balance_txs_by_token_chunk";
const PTR_ENTITY_ALKANE_BLOCK_TXS_CHUNK: &[u8] = b"alkane_block_txs_chunk";
const ADDRESS_INDEX_V2_PREFIX: &[u8] = b"/address_index/v2/";
const ADDRESS_INDEX_INLINE_CAP: usize = 8;
const BALANCE_CHANGES_V2_PREFIX: &[u8] = b"/balance_changes/v2/";
const ALKANE_LATEST_TRACES_V2_PREFIX: &[u8] = b"/alkane_latest_traces/v2/";
const TX_POINTER_FILTER_WORDS: usize = 1 << 23;
const TX_POINTER_FILTER_BITS: u64 = (TX_POINTER_FILTER_WORDS as u64) * 64;
const TX_POINTER_FILTER_MASK: u64 = TX_POINTER_FILTER_BITS - 1;
const TX_POINTER_FILTER_HASHES: u64 = 4;
const MAX_BLOCK_TIMES_BATCH: usize = 1_000;

struct TxPointerFilter {
    bits: Vec<u64>,
    entries: usize,
}

struct TxPointerFilterState {
    filter: Option<TxPointerFilter>,
    build_started: bool,
    pending: Vec<[u8; 32]>,
}

impl TxPointerFilterState {
    fn new() -> Self {
        Self { filter: None, build_started: false, pending: Vec::new() }
    }
}

static TX_POINTER_FILTER: OnceLock<RwLock<TxPointerFilterState>> = OnceLock::new();

fn tx_pointer_filter_lock() -> &'static RwLock<TxPointerFilterState> {
    TX_POINTER_FILTER.get_or_init(|| RwLock::new(TxPointerFilterState::new()))
}

fn tx_pointer_filter_hash(txid: &[u8; 32], seed: u64) -> u64 {
    let mut h = seed;
    for byte in txid {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^ (h >> 32)
}

impl TxPointerFilter {
    fn new() -> Self {
        Self { bits: vec![0; TX_POINTER_FILTER_WORDS], entries: 0 }
    }

    fn insert(&mut self, txid: &[u8; 32]) {
        let h1 = tx_pointer_filter_hash(txid, 0xcbf29ce484222325);
        let h2 = tx_pointer_filter_hash(txid, 0x9e3779b185ebca87) | 1;
        for i in 0..TX_POINTER_FILTER_HASHES {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) & TX_POINTER_FILTER_MASK;
            self.bits[(bit >> 6) as usize] |= 1u64 << (bit & 63);
        }
        self.entries = self.entries.saturating_add(1);
    }

    fn might_contain(&self, txid: &[u8; 32]) -> bool {
        let h1 = tx_pointer_filter_hash(txid, 0xcbf29ce484222325);
        let h2 = tx_pointer_filter_hash(txid, 0x9e3779b185ebca87) | 1;
        for i in 0..TX_POINTER_FILTER_HASHES {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) & TX_POINTER_FILTER_MASK;
            if (self.bits[(bit >> 6) as usize] & (1u64 << (bit & 63))) == 0 {
                return false;
            }
        }
        true
    }
}

fn build_tx_pointer_filter(provider: &EssentialsProvider) -> Result<TxPointerFilter> {
    let started = Instant::now();
    let table = provider.table();
    let family_prefix = table.tx_packed_outflow_pos_point_family_prefix();
    let namespaced_prefix = provider.blob_mdb().prefixed(&family_prefix);
    let mut readopts = ReadOptions::default();
    readopts.fill_cache(false);

    let mut filter = TxPointerFilter::new();
    for res in provider
        .blob_mdb()
        .inner_db()
        .iterator_opt(IteratorMode::From(&namespaced_prefix, Direction::Forward), readopts)
    {
        let (key, _) = res.map_err(|e| anyhow!("tx pointer filter scan failed: {e}"))?;
        let key_ref = key.as_ref();
        if !key_ref.starts_with(&namespaced_prefix) {
            break;
        }
        let relative = &key_ref[provider.blob_mdb().prefix().len()..];
        if relative.len() != family_prefix.len() + 32 {
            continue;
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&relative[family_prefix.len()..]);
        filter.insert(&txid);
    }

    let elapsed_ms = started.elapsed().as_millis();
    eprintln!(
        "[balances] tx pointer filter scanned entries={} bytes={} elapsed_ms={}",
        filter.entries,
        filter.bits.len().saturating_mul(std::mem::size_of::<u64>()),
        elapsed_ms
    );
    Ok(filter)
}

fn ensure_tx_pointer_filter(provider: &EssentialsProvider) -> Result<bool> {
    if std::env::var_os("ESPO_DISABLE_TX_POINTER_FILTER").is_some() {
        return Ok(false);
    }

    {
        let guard = tx_pointer_filter_lock()
            .read()
            .map_err(|_| anyhow!("tx pointer filter lock poisoned"))?;
        if guard.filter.is_some() {
            return Ok(true);
        }
    }

    let mut guard = tx_pointer_filter_lock()
        .write()
        .map_err(|_| anyhow!("tx pointer filter lock poisoned"))?;
    if guard.filter.is_some() {
        return Ok(true);
    }
    if guard.build_started {
        return Ok(false);
    }
    guard.build_started = true;
    let provider = provider.clone();
    std::thread::spawn(move || match build_tx_pointer_filter(&provider) {
        Ok(mut filter) => {
            let lock = tx_pointer_filter_lock();
            let Ok(mut guard) = lock.write() else {
                eprintln!("[balances] tx pointer filter build finished but lock was poisoned");
                return;
            };
            let pending_count = guard.pending.len();
            for txid in guard.pending.drain(..) {
                filter.insert(&txid);
            }
            eprintln!(
                "[balances] tx pointer filter ready entries={} pending_applied={}",
                filter.entries, pending_count
            );
            guard.filter = Some(filter);
        }
        Err(e) => {
            eprintln!("[balances] tx pointer filter build failed: {e:?}");
            if let Ok(mut guard) = tx_pointer_filter_lock().write() {
                guard.build_started = false;
            }
        }
    });
    Ok(false)
}

pub(crate) fn note_tx_pointer_filter_updates<I>(txids: I)
where
    I: IntoIterator<Item = [u8; 32]>,
{
    let Some(lock) = TX_POINTER_FILTER.get() else {
        return;
    };
    let Ok(mut guard) = lock.write() else {
        return;
    };
    if let Some(filter) = guard.filter.as_mut() {
        for txid in txids {
            filter.insert(&txid);
        }
    } else if guard.build_started {
        guard.pending.extend(txids);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressIndexListKind {
    OutpointIdx,
    AlkaneTxs,
    AlkaneBalanceTxsByToken,
    AlkaneBlockTxs,
}

impl AddressIndexListKind {
    fn as_key_segment(self) -> &'static [u8] {
        match self {
            Self::OutpointIdx => b"outpoint_idx",
            Self::AlkaneTxs => b"alkane_txs",
            Self::AlkaneBalanceTxsByToken => b"alkane_balance_txs_by_token",
            Self::AlkaneBlockTxs => b"alkane_block_txs",
        }
    }

    fn chunk_entity(self) -> &'static [u8] {
        match self {
            Self::OutpointIdx => PTR_ENTITY_ADDR_OUTPOINT_IDX_CHUNK,
            Self::AlkaneTxs => PTR_ENTITY_ADDR_ALKANE_TX_CHUNK,
            Self::AlkaneBalanceTxsByToken => PTR_ENTITY_ALKANE_BALANCE_TXS_BY_TOKEN_CHUNK,
            Self::AlkaneBlockTxs => PTR_ENTITY_ALKANE_BLOCK_TXS_CHUNK,
        }
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

fn encode_alkane_id_be(id: &SchemaAlkaneId) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..4].copy_from_slice(&id.block.to_be_bytes());
    out[4..].copy_from_slice(&id.tx.to_be_bytes());
    out
}

fn decode_alkane_id_be(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    if bytes.len() != 12 {
        return None;
    }
    let mut block = [0u8; 4];
    block.copy_from_slice(&bytes[..4]);
    let mut tx = [0u8; 8];
    tx.copy_from_slice(&bytes[4..12]);
    Some(SchemaAlkaneId { block: u32::from_be_bytes(block), tx: u64::from_be_bytes(tx) })
}

pub fn address_index_list_id_alkane_balance_txs_by_token(
    owner: &SchemaAlkaneId,
    token: &SchemaAlkaneId,
) -> String {
    format!("{}:{}|{}:{}", owner.block, owner.tx, token.block, token.tx)
}

pub fn address_index_list_id_alkane_block_txs(height: u64) -> String {
    height.to_string()
}

fn creation_seq_bounds(total: u64, offset: u64, limit: u64, desc: bool) -> (u64, u64, bool) {
    if limit == 0 || offset >= total {
        return (0, 0, false);
    }
    if !desc {
        let start = offset.min(total);
        let end = start.saturating_add(limit).min(total);
        return (start, end, false);
    }
    let end = total.saturating_sub(offset);
    let start = end.saturating_sub(limit);
    (start, end, true)
}

fn creation_debug_enabled() -> bool {
    std::env::var_os("ESPO_CREATION_PAGE_DEBUG").is_some()
}

fn outpoint_id_bytes(txid: &[u8], vout: u32) -> Option<[u8; 36]> {
    if txid.len() != 32 {
        return None;
    }
    let mut out = [0u8; 36];
    out[..32].copy_from_slice(txid);
    out[32..].copy_from_slice(&vout.to_be_bytes());
    Some(out)
}

fn parse_outpoint_id_bytes(bytes: &[u8]) -> Option<(Vec<u8>, u32)> {
    if bytes.len() != 36 {
        return None;
    }
    let mut vout = [0u8; 4];
    vout.copy_from_slice(&bytes[32..36]);
    Some((bytes[..32].to_vec(), u32::from_be_bytes(vout)))
}

fn parse_paged_cursor_u64(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if let Some(hex_str) = s.strip_prefix("0x") {
        return u64::from_str_radix(hex_str, 16).ok();
    }
    if s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(bytes) = hex::decode(s) {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                return Some(u64::from_be_bytes(arr));
            }
        }
    }
    s.parse::<u64>().ok()
}

fn encode_paged_cursor_u64(cursor: u64) -> String {
    hex::encode(cursor.to_be_bytes())
}

fn pointer_counter_key(entity: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PTR_V1_PREFIX.len() + entity.len() + 8);
    key.extend_from_slice(PTR_V1_PREFIX);
    key.extend_from_slice(entity);
    key.extend_from_slice(b"/counter");
    key
}

fn pointer_blob_key(entity: &[u8], id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PTR_V1_PREFIX.len() + entity.len() + 6 + 8);
    key.extend_from_slice(PTR_V1_PREFIX);
    key.extend_from_slice(entity);
    key.extend_from_slice(b"/blob/");
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn holder_id_bytes(holder: &HolderId) -> Vec<u8> {
    match holder {
        HolderId::Address(addr) => {
            let mut out = Vec::with_capacity(1 + addr.len());
            out.push(b'a');
            out.extend_from_slice(addr.as_bytes());
            out
        }
        HolderId::Alkane(id) => {
            let mut out = Vec::with_capacity(13);
            out.push(b'k');
            out.extend_from_slice(&encode_alkane_id_be(id));
            out
        }
    }
}

fn parse_holder_id_bytes(bytes: &[u8]) -> Option<HolderId> {
    if bytes.is_empty() {
        return None;
    }
    match bytes[0] {
        b'a' => std::str::from_utf8(&bytes[1..]).ok().map(|s| HolderId::Address(s.to_string())),
        b'k' => decode_alkane_id_be(&bytes[1..]).map(HolderId::Alkane),
        _ => None,
    }
}

fn dedupe_batch_ops(
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>) {
    let mut seen_puts: HashSet<Vec<u8>> = HashSet::with_capacity(puts.len());
    let mut dedup_puts_rev: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(puts.len());
    for (key, value) in puts.into_iter().rev() {
        if seen_puts.insert(key.clone()) {
            dedup_puts_rev.push((key, value));
        }
    }
    dedup_puts_rev.reverse();

    let mut seen_deletes: HashSet<Vec<u8>> = HashSet::with_capacity(deletes.len());
    let mut dedup_deletes: Vec<Vec<u8>> = Vec::with_capacity(deletes.len());
    for key in deletes {
        if seen_puts.contains(&key) {
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
pub struct EssentialsTable<'a> {
    pub ROOT: KvPointer<'a>,
    // Core kv directory rows (0x01 = values, 0x03 = directory entries).
    pub KV_ROWS: KvPointer<'a>,
    pub DIR_ROWS: ListPointer<'a>,
    pub INDEX_HEIGHT: KvPointer<'a>,
    // Balances + outpoint indexes (address/outpoint views).
    pub BALANCES: KvPointer<'a>,
    pub OUTPOINT_BALANCES: KvPointer<'a>,
    pub OUTPOINT_ADDR: KvPointer<'a>,
    pub UTXO_SPK: KvPointer<'a>,
    pub ADDR_SPK: KvPointer<'a>,
    // Alkane holders and balances.
    pub HOLDERS: KvPointer<'a>,
    pub HOLDERS_COUNT: KvPointer<'a>,
    pub HOLDERS_ORDERED: ListPointer<'a>,
    pub TRANSFER_VOLUME: KvPointer<'a>,
    pub TOTAL_RECEIVED: KvPointer<'a>,
    pub ADDRESS_ACTIVITY: KvPointer<'a>,
    pub ALKANE_BALANCES: KvPointer<'a>,
    pub ALKANE_BALANCES_BY_HEIGHT: KvPointer<'a>,
    // Alkane creation + metadata.
    pub ALKANE_INFO: KvPointer<'a>,
    pub ALKANE_NAME_INDEX: ListPointer<'a>,
    pub ALKANE_SYMBOL_INDEX: ListPointer<'a>,
    pub ORBITAL_NAME: KvPointer<'a>,
    pub ALKANE_CREATION_BY_ID: KvPointer<'a>,
    pub ALKANE_CREATION_SEQ: KvPointer<'a>,
    pub ALKANE_CREATION_COUNT: KvPointer<'a>,
    pub ALKANE_CREATIONS_IN_BLOCK: KvPointer<'a>,
    pub ALKANE_FACTORY_CHILDREN: ListPointer<'a>,
    pub CIRCULATING_SUPPLY: KvPointer<'a>,
    pub CIRCULATING_SUPPLY_LATEST: KvPointer<'a>,
    pub TOTAL_MINTED: KvPointer<'a>,
    pub TOTAL_MINTED_LATEST: KvPointer<'a>,
    // Transaction summaries + reverse indexes.
    pub ALKANE_TX_SUMMARY: KvPointer<'a>,
    pub ALKANE_BLOCK: ListPointer<'a>,
    pub ALKANE_ADDR: ListPointer<'a>,
    pub ALKANE_LATEST_TRACES: KvPointer<'a>,
    // Block summaries.
    pub BLOCK_SUMMARY: KvPointer<'a>,
    pub HEIGHT_TO_HASH: KvPointer<'a>,
}

impl<'a> EssentialsTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = KvPointer::root(mdb);
        EssentialsTable {
            ROOT: root.clone(),
            KV_ROWS: root.select(&[0x01]),
            DIR_ROWS: root.list_select(&[0x03]),
            INDEX_HEIGHT: root.keyword("/index_height"),
            BALANCES: root.keyword("/balances/"),
            OUTPOINT_BALANCES: root.keyword("/outpoint_balances/"),
            OUTPOINT_ADDR: root.keyword("/outpoint_addr/"),
            UTXO_SPK: root.keyword("/utxo_spk/"),
            ADDR_SPK: root.keyword("/addr_spk/"),
            HOLDERS: root.keyword("/alkane/v2/"),
            HOLDERS_COUNT: root.keyword("/alkane/v2/"),
            HOLDERS_ORDERED: root.list_keyword("/alkanes/holders/ordered/"),
            TRANSFER_VOLUME: root.keyword("/alkane/v2/"),
            TOTAL_RECEIVED: root.keyword("/alkane/v2/"),
            ADDRESS_ACTIVITY: root.keyword("/address/v2/"),
            ALKANE_BALANCES: root.keyword("/alkane/v2/"),
            ALKANE_BALANCES_BY_HEIGHT: root.keyword("/alkane/v2/"),
            ALKANE_INFO: root.keyword("/alkane_info/"),
            ALKANE_NAME_INDEX: root.list_keyword("/alkanes/name/"),
            ALKANE_SYMBOL_INDEX: root.list_keyword("/alkanes/symbol/"),
            ORBITAL_NAME: root.keyword("/orbitals/name/"),
            ALKANE_CREATION_BY_ID: root.keyword("/alkanes/creation/id/"),
            ALKANE_CREATION_SEQ: root.keyword("/alkanes/creation/seq/v1/"),
            ALKANE_CREATION_COUNT: root.keyword("/alkanes/creation/count"),
            ALKANE_CREATIONS_IN_BLOCK: root.keyword("/alkanes/creation/in_block/v2/"),
            ALKANE_FACTORY_CHILDREN: root.list_keyword("/alkanes/factory_children/v1/"),
            CIRCULATING_SUPPLY: root.keyword("/circulating_supply/v1/"),
            CIRCULATING_SUPPLY_LATEST: root.keyword("/circulating_supply/latest/"),
            TOTAL_MINTED: root.keyword("/total_minted/v1/"),
            TOTAL_MINTED_LATEST: root.keyword("/total_minted/latest/"),
            ALKANE_TX_SUMMARY: root.keyword("/alkane_tx_summary/"),
            ALKANE_BLOCK: root.list_keyword("/alkane_block/"),
            ALKANE_ADDR: root.list_keyword("/alkane_addr/"),
            ALKANE_LATEST_TRACES: root.keyword("/alkane_latest_traces"),
            BLOCK_SUMMARY: root.keyword("/block_summary/"),
            HEIGHT_TO_HASH: root.keyword("/height_to_hash/"),
        }
    }
}

impl<'a> EssentialsTable<'a> {
    pub fn kv_row_key(&self, alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(4 + 8 + 2 + skey.len());
        suffix.extend_from_slice(&alk.block.to_be_bytes());
        suffix.extend_from_slice(&alk.tx.to_be_bytes());
        let len = u16::try_from(skey.len()).unwrap_or(u16::MAX);
        suffix.extend_from_slice(&len.to_be_bytes());
        if len as usize != skey.len() {
            suffix.extend_from_slice(&skey[..(len as usize)]);
        } else {
            suffix.extend_from_slice(skey);
        }
        self.KV_ROWS.select(&suffix).key().to_vec()
    }

    pub fn dir_row_key(&self, alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(4 + 8 + 2 + skey.len());
        suffix.extend_from_slice(&alk.block.to_be_bytes());
        suffix.extend_from_slice(&alk.tx.to_be_bytes());
        let len = u16::try_from(skey.len()).unwrap_or(u16::MAX);
        suffix.extend_from_slice(&len.to_be_bytes());
        if len as usize != skey.len() {
            suffix.extend_from_slice(&skey[..(len as usize)]);
        } else {
            suffix.extend_from_slice(skey);
        }
        self.DIR_ROWS.select(&suffix).key().to_vec()
    }

    pub fn dir_list_prefix(&self, alk: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(4 + 8);
        suffix.extend_from_slice(&alk.block.to_be_bytes());
        suffix.extend_from_slice(&alk.tx.to_be_bytes());
        self.DIR_ROWS.select(&suffix).key().to_vec()
    }

    pub fn addr_spk_key(&self, addr: &str) -> Vec<u8> {
        self.ADDR_SPK.select(addr.as_bytes()).key().to_vec()
    }

    pub fn address_outpoint_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 10);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/outpoint/");
        key
    }

    pub fn address_outpoint_idx_list_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 14);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/outpoint_idx/");
        key
    }

    pub fn address_outpoint_idx_list_len_key(&self, address: &str) -> Vec<u8> {
        let mut key = self.address_outpoint_idx_list_prefix(address);
        key.extend_from_slice(b"length");
        key
    }

    pub fn address_outpoint_idx_list_idx_key(&self, address: &str, idx: u64) -> Vec<u8> {
        let mut key = self.address_outpoint_idx_list_prefix(address);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn address_index_meta_key(&self, address: &str, kind: AddressIndexListKind) -> Vec<u8> {
        let seg = kind.as_key_segment();
        let mut key = Vec::with_capacity(
            ADDRESS_INDEX_V2_PREFIX.len() + seg.len() + 1 + address.len() + b"/meta".len(),
        );
        key.extend_from_slice(ADDRESS_INDEX_V2_PREFIX);
        key.extend_from_slice(seg);
        key.push(b'/');
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/meta");
        key
    }

    pub fn address_outpoint_key(&self, address: &str, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(&outp.txid, outp.vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", outp.txid.len()))?;
        let mut key = self.address_outpoint_prefix(address);
        key.extend_from_slice(&outpoint_id);
        Ok(key)
    }

    pub fn parse_address_outpoint_key(&self, key: &[u8]) -> Option<(String, Vec<u8>, u32)> {
        if !key.starts_with(ADDRESS_V2_PREFIX) {
            return None;
        }
        let rest = &key[ADDRESS_V2_PREFIX.len()..];
        let marker = b"/outpoint/";
        let split = rest.windows(marker.len()).position(|w| w == marker)?;
        let address = std::str::from_utf8(&rest[..split]).ok()?.to_string();
        let outpoint_bytes = &rest[split + marker.len()..];
        let (txid, vout) = parse_outpoint_id_bytes(outpoint_bytes)?;
        Some((address, txid, vout))
    }

    pub fn address_balance_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 9);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/balance/");
        key
    }

    pub fn address_balance_key(&self, address: &str, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.address_balance_prefix(address);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key
    }

    pub fn address_balance_list_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 13);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/balance_idx/");
        key
    }

    pub fn address_balance_list_len_key(&self, address: &str) -> Vec<u8> {
        let mut key = self.address_balance_list_prefix(address);
        key.extend_from_slice(b"length");
        key
    }

    pub fn address_balance_list_idx_key(&self, address: &str, idx: u32) -> Vec<u8> {
        let mut key = self.address_balance_list_prefix(address);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn parse_address_balance_key(&self, key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
        if !key.starts_with(ADDRESS_V2_PREFIX) {
            return None;
        }
        let rest = &key[ADDRESS_V2_PREFIX.len()..];
        let marker = b"/balance/";
        let split = rest.windows(marker.len()).position(|w| w == marker)?;
        let address = std::str::from_utf8(&rest[..split]).ok()?.to_string();
        let alkane = decode_alkane_id_be(&rest[split + marker.len()..])?;
        Some((address, alkane))
    }

    pub fn outpoint_pos_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        self.outpoint_pos_key_from_parts(&outp.txid, outp.vout)
    }

    pub fn outpoint_pos_key_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(txid, vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", txid.len()))?;
        let mut key = Vec::with_capacity(OUTPOINT_V2_PREFIX.len() + 2 + outpoint_id.len());
        key.extend_from_slice(OUTPOINT_V2_PREFIX);
        key.extend_from_slice(b"p/");
        key.extend_from_slice(&outpoint_id);
        Ok(key)
    }

    pub fn outpoint_pos_append_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_APPEND_PREFIX.len() + 2);
        key.extend_from_slice(OUTPOINT_V2_APPEND_PREFIX);
        key.extend_from_slice(b"p/");
        key
    }

    pub fn outpoint_pos_append_prefix_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(txid, vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", txid.len()))?;
        let mut key =
            Vec::with_capacity(OUTPOINT_V2_APPEND_PREFIX.len() + 2 + outpoint_id.len() + 1);
        key.extend_from_slice(OUTPOINT_V2_APPEND_PREFIX);
        key.extend_from_slice(b"p/");
        key.extend_from_slice(&outpoint_id);
        key.push(b'/');
        Ok(key)
    }

    pub fn outpoint_pos_append_prefix(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        self.outpoint_pos_append_prefix_from_parts(&outp.txid, outp.vout)
    }

    pub fn outpoint_pos_append_key_from_parts(
        &self,
        txid: &[u8],
        vout: u32,
        height: u32,
        blockhash: &[u8; 32],
    ) -> Result<Vec<u8>> {
        let mut key = self.outpoint_pos_append_prefix_from_parts(txid, vout)?;
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(blockhash);
        Ok(key)
    }

    pub fn outpoint_pos_point_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_POINT_PREFIX.len() + 2);
        key.extend_from_slice(OUTPOINT_V2_POINT_PREFIX);
        key.extend_from_slice(b"p/");
        key
    }

    pub fn outpoint_pos_point_key_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(txid, vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", txid.len()))?;
        let mut key = Vec::with_capacity(OUTPOINT_V2_POINT_PREFIX.len() + 2 + outpoint_id.len());
        key.extend_from_slice(OUTPOINT_V2_POINT_PREFIX);
        key.extend_from_slice(b"p/");
        key.extend_from_slice(&outpoint_id);
        Ok(key)
    }

    pub fn outpoint_pointer_counter_key(&self) -> Vec<u8> {
        pointer_counter_key(PTR_ENTITY_OUTPOINT)
    }

    pub fn outpoint_pointer_blob_key(&self, id: u64) -> Vec<u8> {
        pointer_blob_key(PTR_ENTITY_OUTPOINT, id)
    }

    pub fn outpoint_prefix(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(&outp.txid, outp.vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", outp.txid.len()))?;
        let mut key = Vec::with_capacity(OUTPOINT_V2_PREFIX.len() + outpoint_id.len() + 1);
        key.extend_from_slice(OUTPOINT_V2_PREFIX);
        key.extend_from_slice(&outpoint_id);
        key.push(b'/');
        Ok(key)
    }

    pub fn outpoint_prefix_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(txid, vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", txid.len()))?;
        let mut key = Vec::with_capacity(OUTPOINT_V2_PREFIX.len() + outpoint_id.len() + 1);
        key.extend_from_slice(OUTPOINT_V2_PREFIX);
        key.extend_from_slice(&outpoint_id);
        key.push(b'/');
        Ok(key)
    }

    pub fn outpoint_spent_by_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        self.outpoint_spent_by_key_from_parts(&outp.txid, outp.vout)
    }

    pub fn outpoint_spent_by_key_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let outpoint_id = outpoint_id_bytes(txid, vout)
            .ok_or_else(|| anyhow!("invalid outpoint txid length {}", txid.len()))?;
        let mut key = Vec::with_capacity(OUTPOINT_V2_PREFIX.len() + 2 + outpoint_id.len());
        key.extend_from_slice(OUTPOINT_V2_PREFIX);
        key.extend_from_slice(b"s/");
        key.extend_from_slice(&outpoint_id);
        Ok(key)
    }

    pub fn outpoint_spent_by_id_key(&self, id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_PREFIX.len() + 4 + 8);
        key.extend_from_slice(OUTPOINT_V2_PREFIX);
        key.extend_from_slice(b"sid/");
        key.extend_from_slice(&id.to_be_bytes());
        key
    }

    pub fn outpoint_spent_by_id_append_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_APPEND_PREFIX.len() + 4);
        key.extend_from_slice(OUTPOINT_V2_APPEND_PREFIX);
        key.extend_from_slice(b"sid/");
        key
    }

    pub fn outpoint_spent_by_id_append_prefix(&self, id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_APPEND_PREFIX.len() + 4 + 8 + 1);
        key.extend_from_slice(OUTPOINT_V2_APPEND_PREFIX);
        key.extend_from_slice(b"sid/");
        key.extend_from_slice(&id.to_be_bytes());
        key.push(b'/');
        key
    }

    pub fn outpoint_spent_by_id_append_key(
        &self,
        id: u64,
        height: u32,
        blockhash: &[u8; 32],
    ) -> Vec<u8> {
        let mut key = self.outpoint_spent_by_id_append_prefix(id);
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(blockhash);
        key
    }

    pub fn outpoint_spent_by_id_point_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_POINT_PREFIX.len() + 4);
        key.extend_from_slice(OUTPOINT_V2_POINT_PREFIX);
        key.extend_from_slice(b"sid/");
        key
    }

    pub fn outpoint_spent_by_id_point_key(&self, id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(OUTPOINT_V2_POINT_PREFIX.len() + 4 + 8);
        key.extend_from_slice(OUTPOINT_V2_POINT_PREFIX);
        key.extend_from_slice(b"sid/");
        key.extend_from_slice(&id.to_be_bytes());
        key
    }

    pub fn outpoint_balance_prefix(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let mut key = self.outpoint_prefix(outp)?;
        key.extend_from_slice(b"balance/");
        Ok(key)
    }

    pub fn outpoint_balance_prefix_from_parts(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        let mut key = self.outpoint_prefix_from_parts(txid, vout)?;
        key.extend_from_slice(b"balance/");
        Ok(key)
    }

    pub fn outpoint_balance_key(
        &self,
        outp: &EspoOutpoint,
        alkane: &SchemaAlkaneId,
    ) -> Result<Vec<u8>> {
        let mut key = self.outpoint_balance_prefix(outp)?;
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        Ok(key)
    }

    pub fn outpoint_balance_list_prefix(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let mut key = self.outpoint_prefix(outp)?;
        key.extend_from_slice(b"balance_idx/");
        Ok(key)
    }

    pub fn outpoint_balance_list_prefix_from_parts(
        &self,
        txid: &[u8],
        vout: u32,
    ) -> Result<Vec<u8>> {
        let mut key = self.outpoint_prefix_from_parts(txid, vout)?;
        key.extend_from_slice(b"balance_idx/");
        Ok(key)
    }

    pub fn outpoint_balance_list_len_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let mut key = self.outpoint_balance_list_prefix(outp)?;
        key.extend_from_slice(b"length");
        Ok(key)
    }

    pub fn outpoint_balance_list_len_key_from_parts(
        &self,
        txid: &[u8],
        vout: u32,
    ) -> Result<Vec<u8>> {
        let mut key = self.outpoint_balance_list_prefix_from_parts(txid, vout)?;
        key.extend_from_slice(b"length");
        Ok(key)
    }

    pub fn outpoint_balance_list_idx_key(&self, outp: &EspoOutpoint, idx: u32) -> Result<Vec<u8>> {
        let mut key = self.outpoint_balance_list_prefix(outp)?;
        key.extend_from_slice(&idx.to_be_bytes());
        Ok(key)
    }

    pub fn outpoint_balance_list_idx_key_from_parts(
        &self,
        txid: &[u8],
        vout: u32,
        idx: u32,
    ) -> Result<Vec<u8>> {
        let mut key = self.outpoint_balance_list_prefix_from_parts(txid, vout)?;
        key.extend_from_slice(&idx.to_be_bytes());
        Ok(key)
    }

    pub fn parse_outpoint_balance_key(&self, key: &[u8]) -> Option<(Vec<u8>, u32, SchemaAlkaneId)> {
        if !key.starts_with(OUTPOINT_V2_PREFIX) {
            return None;
        }
        let rest = &key[OUTPOINT_V2_PREFIX.len()..];
        if rest.len() < 36 + "/balance/".len() + 12 {
            return None;
        }
        let (txid, vout) = parse_outpoint_id_bytes(&rest[..36])?;
        if !rest[36..].starts_with(b"/balance/") {
            return None;
        }
        let alkane = decode_alkane_id_be(&rest[(36 + "/balance/".len())..])?;
        Some((txid, vout, alkane))
    }

    pub fn holder_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 9);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/holder/");
        key
    }

    pub fn holder_key(&self, alkane: &SchemaAlkaneId, holder: &HolderId) -> Vec<u8> {
        let mut key = self.holder_prefix(alkane);
        key.extend_from_slice(&holder_id_bytes(holder));
        key
    }

    pub fn holder_list_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 12);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/holder_idx/");
        key
    }

    pub fn holder_list_len_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.holder_list_prefix(alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn holder_list_idx_key(&self, alkane: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.holder_list_prefix(alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn parse_holder_key(&self, key: &[u8]) -> Option<(SchemaAlkaneId, HolderId)> {
        if !key.starts_with(ALKANE_V2_PREFIX) {
            return None;
        }
        let rest = &key[ALKANE_V2_PREFIX.len()..];
        if rest.len() < 12 + "/holder/".len() {
            return None;
        }
        let alkane = decode_alkane_id_be(&rest[..12])?;
        if !rest[12..].starts_with(b"/holder/") {
            return None;
        }
        let holder = parse_holder_id_bytes(&rest[(12 + "/holder/".len())..])?;
        Some((alkane, holder))
    }

    pub fn orbital_holder_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 17);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_holder/");
        key
    }

    pub fn orbital_holder_key(&self, factory: &SchemaAlkaneId, holder: &HolderId) -> Vec<u8> {
        let mut key = self.orbital_holder_prefix(factory);
        key.extend_from_slice(&holder_id_bytes(holder));
        key
    }

    pub fn orbital_holder_list_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 21);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_holder_idx/");
        key
    }

    pub fn orbital_holder_list_len_key(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.orbital_holder_list_prefix(factory);
        key.extend_from_slice(b"length");
        key
    }

    pub fn orbital_holder_list_idx_key(&self, factory: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.orbital_holder_list_prefix(factory);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn orbital_holder_v2_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 20);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_holder_v2/");
        key
    }

    pub fn orbital_holder_v2_key(&self, factory: &SchemaAlkaneId, holder: &HolderId) -> Vec<u8> {
        let mut key = self.orbital_holder_v2_prefix(factory);
        key.extend_from_slice(&holder_id_bytes(holder));
        key
    }

    pub fn orbital_holder_v2_list_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 24);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_holder_v2_idx/");
        key
    }

    pub fn orbital_holder_v2_list_len_key(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.orbital_holder_v2_list_prefix(factory);
        key.extend_from_slice(b"length");
        key
    }

    pub fn orbital_holder_v2_list_idx_key(&self, factory: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.orbital_holder_v2_list_prefix(factory);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn transfer_volume_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 18);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/transfer_volume/");
        key
    }

    pub fn transfer_volume_entry_key(&self, alkane: &SchemaAlkaneId, address: &str) -> Vec<u8> {
        let mut key = self.transfer_volume_prefix(alkane);
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn transfer_volume_list_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 22);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/transfer_volume_idx/");
        key
    }

    pub fn transfer_volume_list_len_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.transfer_volume_list_prefix(alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn transfer_volume_list_idx_key(&self, alkane: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.transfer_volume_list_prefix(alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn total_received_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 17);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/total_received/");
        key
    }

    pub fn total_received_entry_key(&self, alkane: &SchemaAlkaneId, address: &str) -> Vec<u8> {
        let mut key = self.total_received_prefix(alkane);
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn total_received_list_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 21);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/total_received_idx/");
        key
    }

    pub fn total_received_list_len_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.total_received_list_prefix(alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn total_received_list_idx_key(&self, alkane: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.total_received_list_prefix(alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn address_activity_transfer_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 26);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/transfer_volume/");
        key
    }

    pub fn address_activity_transfer_key(&self, address: &str, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.address_activity_transfer_prefix(address);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key
    }

    pub fn address_activity_transfer_list_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 31);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/transfer_volume_idx/");
        key
    }

    pub fn address_activity_transfer_list_len_key(&self, address: &str) -> Vec<u8> {
        let mut key = self.address_activity_transfer_list_prefix(address);
        key.extend_from_slice(b"length");
        key
    }

    pub fn address_activity_transfer_list_idx_key(&self, address: &str, idx: u32) -> Vec<u8> {
        let mut key = self.address_activity_transfer_list_prefix(address);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn address_activity_total_received_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 25);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/total_received/");
        key
    }

    pub fn address_activity_total_received_key(
        &self,
        address: &str,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.address_activity_total_received_prefix(address);
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key
    }

    pub fn address_activity_total_received_list_prefix(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 30);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/total_received_idx/");
        key
    }

    pub fn address_activity_total_received_list_len_key(&self, address: &str) -> Vec<u8> {
        let mut key = self.address_activity_total_received_list_prefix(address);
        key.extend_from_slice(b"length");
        key
    }

    pub fn address_activity_total_received_list_idx_key(&self, address: &str, idx: u32) -> Vec<u8> {
        let mut key = self.address_activity_total_received_list_prefix(address);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn address_cumulative_send_alkanes_key(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 35);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/cumulative_send_alkanes");
        key
    }

    pub fn address_cumulative_receive_alkanes_key(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 38);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/cumulative_receive_alkanes");
        key
    }

    pub fn address_cumulative_send_orbitals_key(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 36);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/cumulative_send_orbitals");
        key
    }

    pub fn address_cumulative_receive_orbitals_key(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 39);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/cumulative_receive_orbitals");
        key
    }

    pub fn address_orbital_balances_v2_key(&self, address: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(ADDRESS_V2_PREFIX.len() + address.len() + 30);
        key.extend_from_slice(ADDRESS_V2_PREFIX);
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(b"/alkane/orbital_balances_v2");
        key
    }

    pub fn orbital_send_volume_entry_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        address: &str,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 25 + 12 + 1 + address.len());
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_send_volume/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn orbital_send_volume_list_prefix(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 29 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_send_volume_idx/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key
    }

    pub fn orbital_send_volume_list_len_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.orbital_send_volume_list_prefix(factory, alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn orbital_send_volume_list_idx_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        idx: u32,
    ) -> Vec<u8> {
        let mut key = self.orbital_send_volume_list_prefix(factory, alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn orbital_receive_volume_entry_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        address: &str,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 28 + 12 + 1 + address.len());
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_receive_volume/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn orbital_receive_volume_list_prefix(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 32 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(factory));
        key.extend_from_slice(b"/orbital_receive_volume_idx/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key
    }

    pub fn orbital_receive_volume_list_len_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.orbital_receive_volume_list_prefix(factory, alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn orbital_receive_volume_list_idx_key(
        &self,
        factory: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        idx: u32,
    ) -> Vec<u8> {
        let mut key = self.orbital_receive_volume_list_prefix(factory, alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn alkane_send_volume_entry_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        address: &str,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 24 + 12 + 1 + address.len());
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(source));
        key.extend_from_slice(b"/alkane_send_volume/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn alkane_send_volume_list_prefix(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 28 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(source));
        key.extend_from_slice(b"/alkane_send_volume_idx/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key
    }

    pub fn alkane_send_volume_list_len_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.alkane_send_volume_list_prefix(source, alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn alkane_send_volume_list_idx_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        idx: u32,
    ) -> Vec<u8> {
        let mut key = self.alkane_send_volume_list_prefix(source, alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn alkane_receive_volume_entry_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        address: &str,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 27 + 12 + 1 + address.len());
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(source));
        key.extend_from_slice(b"/alkane_receive_volume/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key.extend_from_slice(address.as_bytes());
        key
    }

    pub fn alkane_receive_volume_list_prefix(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 31 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(source));
        key.extend_from_slice(b"/alkane_receive_volume_idx/v2/");
        key.extend_from_slice(&encode_alkane_id_be(alkane));
        key.extend_from_slice(b"/");
        key
    }

    pub fn alkane_receive_volume_list_len_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.alkane_receive_volume_list_prefix(source, alkane);
        key.extend_from_slice(b"length");
        key
    }

    pub fn alkane_receive_volume_list_idx_key(
        &self,
        source: &SchemaAlkaneId,
        alkane: &SchemaAlkaneId,
        idx: u32,
    ) -> Vec<u8> {
        let mut key = self.alkane_receive_volume_list_prefix(source, alkane);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn alkane_balance_prefix(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 10);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance/");
        key
    }

    pub fn alkane_balance_key(&self, owner: &SchemaAlkaneId, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.alkane_balance_prefix(owner);
        key.extend_from_slice(&encode_alkane_id_be(token));
        key
    }

    pub fn alkane_balance_list_prefix(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 13);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance_idx/");
        key
    }

    pub fn alkane_balance_list_len_key(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = self.alkane_balance_list_prefix(owner);
        key.extend_from_slice(b"length");
        key
    }

    pub fn alkane_balance_list_idx_key(&self, owner: &SchemaAlkaneId, idx: u32) -> Vec<u8> {
        let mut key = self.alkane_balance_list_prefix(owner);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn parse_alkane_balance_key(&self, key: &[u8]) -> Option<(SchemaAlkaneId, SchemaAlkaneId)> {
        if !key.starts_with(ALKANE_V2_PREFIX) {
            return None;
        }
        let rest = &key[ALKANE_V2_PREFIX.len()..];
        if rest.len() < 12 + "/balance/".len() + 12 {
            return None;
        }
        let owner = decode_alkane_id_be(&rest[..12])?;
        if !rest[12..].starts_with(b"/balance/") {
            return None;
        }
        let token = decode_alkane_id_be(&rest[(12 + "/balance/".len())..])?;
        Some((owner, token))
    }

    pub fn alkane_balance_by_height_prefix(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 20);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance_by_height/");
        key
    }

    pub fn alkane_balance_by_height_token_prefix(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_by_height_prefix(owner);
        key.extend_from_slice(&encode_alkane_id_be(token));
        key.push(b'/');
        key
    }

    pub fn alkane_balance_by_height_key(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
        height: u32,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_by_height_token_prefix(owner, token);
        key.extend_from_slice(&height.to_be_bytes());
        key
    }

    pub fn alkane_balance_by_height_list_prefix(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 25 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance_by_height_idx/");
        key.extend_from_slice(&encode_alkane_id_be(token));
        key.push(b'/');
        key
    }

    pub fn alkane_balance_by_height_list_len_key(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_by_height_list_prefix(owner, token);
        key.extend_from_slice(b"length");
        key
    }

    pub fn alkane_balance_by_height_list_idx_key(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
        idx: u32,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_by_height_list_prefix(owner, token);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn tx_packed_outflow_pos_key(&self, txid: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_V2_PREFIX.len() + 2 + txid.len());
        key.extend_from_slice(TX_V2_PREFIX);
        key.extend_from_slice(b"l/");
        key.extend_from_slice(txid);
        key
    }

    pub fn tx_packed_outflow_pos_append_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_V2_APPEND_PREFIX.len() + 2);
        key.extend_from_slice(TX_V2_APPEND_PREFIX);
        key.extend_from_slice(b"l/");
        key
    }

    pub fn tx_packed_outflow_pos_append_prefix(&self, txid: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_V2_APPEND_PREFIX.len() + 2 + txid.len() + 1);
        key.extend_from_slice(TX_V2_APPEND_PREFIX);
        key.extend_from_slice(b"l/");
        key.extend_from_slice(txid);
        key.push(b'/');
        key
    }

    pub fn tx_packed_outflow_pos_append_key(
        &self,
        txid: &[u8; 32],
        height: u32,
        blockhash: &[u8; 32],
    ) -> Vec<u8> {
        let mut key = self.tx_packed_outflow_pos_append_prefix(txid);
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(blockhash);
        key
    }

    pub fn tx_packed_outflow_pos_point_family_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_V2_POINT_PREFIX.len() + 2);
        key.extend_from_slice(TX_V2_POINT_PREFIX);
        key.extend_from_slice(b"l/");
        key
    }

    pub fn tx_packed_outflow_pos_point_key(&self, txid: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_V2_POINT_PREFIX.len() + 2 + txid.len());
        key.extend_from_slice(TX_V2_POINT_PREFIX);
        key.extend_from_slice(b"l/");
        key.extend_from_slice(txid);
        key
    }

    pub fn tx_pointer_counter_key(&self) -> Vec<u8> {
        pointer_counter_key(PTR_ENTITY_ALKANE_TX)
    }

    pub fn tx_pointer_blob_key(&self, id: u64) -> Vec<u8> {
        pointer_blob_key(PTR_ENTITY_ALKANE_TX, id)
    }

    pub fn address_index_chunk_counter_key(&self, kind: AddressIndexListKind) -> Vec<u8> {
        pointer_counter_key(kind.chunk_entity())
    }

    pub fn address_index_chunk_blob_key(&self, kind: AddressIndexListKind, id: u64) -> Vec<u8> {
        pointer_blob_key(kind.chunk_entity(), id)
    }

    pub fn balance_changes_height_prefix(&self, height: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(BALANCE_CHANGES_V2_PREFIX.len() + 4 + 1);
        key.extend_from_slice(BALANCE_CHANGES_V2_PREFIX);
        key.extend_from_slice(&height.to_be_bytes());
        key.push(b'/');
        key
    }

    pub fn balance_changes_idx_key(&self, height: u32, idx: u32) -> Vec<u8> {
        let mut key = self.balance_changes_height_prefix(height);
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn balance_changes_length_key(&self, height: u32) -> Vec<u8> {
        let mut key = self.balance_changes_height_prefix(height);
        key.extend_from_slice(b"length");
        key
    }

    pub fn latest_traces_prefix(&self) -> Vec<u8> {
        ALKANE_LATEST_TRACES_V2_PREFIX.to_vec()
    }

    pub fn latest_traces_idx_key(&self, idx: u32) -> Vec<u8> {
        let mut key = self.latest_traces_prefix();
        key.extend_from_slice(&idx.to_be_bytes());
        key
    }

    pub fn latest_traces_length_key(&self) -> Vec<u8> {
        let mut key = self.latest_traces_prefix();
        key.extend_from_slice(b"length");
        key
    }

    pub fn balances_key(&self, address: &str, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let suffix = borsh::to_vec(outp)?;
        Ok(self
            .BALANCES
            .select(address.as_bytes())
            .keyword("/")
            .select(&suffix)
            .key()
            .to_vec())
    }

    pub fn holders_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.HOLDERS.select(&suffix).key().to_vec()
    }

    pub fn holders_count_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.HOLDERS_COUNT.select(&suffix).key().to_vec()
    }

    pub fn transfer_volume_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.TRANSFER_VOLUME.select(&suffix).key().to_vec()
    }

    pub fn total_received_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.TOTAL_RECEIVED.select(&suffix).key().to_vec()
    }

    pub fn address_activity_key(&self, address: &str) -> Vec<u8> {
        self.ADDRESS_ACTIVITY.select(address.as_bytes()).key().to_vec()
    }

    pub fn alkane_balance_txs_log_prefix(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 16);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance_txs/v2/");
        key
    }

    pub fn alkane_balance_txs_log_key(
        &self,
        owner: &SchemaAlkaneId,
        height: u32,
        tx_idx: u32,
        entry_id: u64,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_txs_log_prefix(owner);
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(&tx_idx.to_be_bytes());
        key.extend_from_slice(&entry_id.to_be_bytes());
        key
    }

    pub fn parse_alkane_balance_txs_log_key(
        &self,
        owner: &SchemaAlkaneId,
        key: &[u8],
    ) -> Option<(u32, u32, u64)> {
        let prefix = self.alkane_balance_txs_log_prefix(owner);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != 4 + 4 + 8 {
            return None;
        }
        let mut height = [0u8; 4];
        height.copy_from_slice(&rest[..4]);
        let mut tx_idx = [0u8; 4];
        tx_idx.copy_from_slice(&rest[4..8]);
        let mut entry_id = [0u8; 8];
        entry_id.copy_from_slice(&rest[8..16]);
        Some((u32::from_be_bytes(height), u32::from_be_bytes(tx_idx), u64::from_be_bytes(entry_id)))
    }

    pub fn alkane_balance_txs_by_token_log_prefix(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 12 + 25 + 12 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key.extend_from_slice(b"/balance_txs_by_token/v2/");
        key.extend_from_slice(&encode_alkane_id_be(token));
        key.push(b'/');
        key
    }

    pub fn alkane_balance_txs_by_token_log_key(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
        height: u32,
        tx_idx: u32,
        entry_id: u64,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_txs_by_token_log_prefix(owner, token);
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(&tx_idx.to_be_bytes());
        key.extend_from_slice(&entry_id.to_be_bytes());
        key
    }

    pub fn parse_alkane_balance_txs_by_token_log_key(
        &self,
        owner: &SchemaAlkaneId,
        token: &SchemaAlkaneId,
        key: &[u8],
    ) -> Option<(u32, u32, u64)> {
        let prefix = self.alkane_balance_txs_by_token_log_prefix(owner, token);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != 4 + 4 + 8 {
            return None;
        }
        let mut height = [0u8; 4];
        height.copy_from_slice(&rest[..4]);
        let mut tx_idx = [0u8; 4];
        tx_idx.copy_from_slice(&rest[4..8]);
        let mut entry_id = [0u8; 8];
        entry_id.copy_from_slice(&rest[8..16]);
        Some((u32::from_be_bytes(height), u32::from_be_bytes(tx_idx), u64::from_be_bytes(entry_id)))
    }

    pub fn alkane_balance_txs_by_height_log_prefix(&self, height: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(ALKANE_V2_PREFIX.len() + 31 + 4 + 1);
        key.extend_from_slice(ALKANE_V2_PREFIX);
        key.extend_from_slice(b"balance_txs_by_height/v2/");
        key.extend_from_slice(&height.to_be_bytes());
        key.push(b'/');
        key
    }

    pub fn alkane_balance_txs_by_height_log_key(
        &self,
        height: u32,
        tx_idx: u32,
        owner: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut key = self.alkane_balance_txs_by_height_log_prefix(height);
        key.extend_from_slice(&tx_idx.to_be_bytes());
        key.extend_from_slice(&encode_alkane_id_be(owner));
        key
    }

    pub fn parse_alkane_balance_txs_by_height_log_key(
        &self,
        height: u32,
        key: &[u8],
    ) -> Option<(u32, SchemaAlkaneId)> {
        let prefix = self.alkane_balance_txs_by_height_log_prefix(height);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != 4 + 12 {
            return None;
        }
        let mut tx_idx = [0u8; 4];
        tx_idx.copy_from_slice(&rest[..4]);
        let owner = decode_alkane_id_be(&rest[4..16])?;
        Some((u32::from_be_bytes(tx_idx), owner))
    }

    pub fn alkane_balances_key(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        self.alkane_balance_prefix(owner)
    }

    pub fn alkane_balances_by_height_key(&self, owner: &SchemaAlkaneId, height: u32) -> Vec<u8> {
        self.alkane_balance_by_height_key(owner, owner, height)
    }

    pub fn alkane_balances_by_height_prefix(&self, owner: &SchemaAlkaneId) -> Vec<u8> {
        self.alkane_balance_by_height_prefix(owner)
    }

    pub fn alkane_info_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.ALKANE_INFO.select(&suffix).key().to_vec()
    }

    pub fn alkane_name_index_key(&self, name: &str, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(name.len() + 1 + 12);
        suffix.extend_from_slice(name.as_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.ALKANE_NAME_INDEX.select(&suffix).key().to_vec()
    }

    pub fn alkane_name_index_prefix(&self, name_prefix: &str) -> Vec<u8> {
        self.ALKANE_NAME_INDEX.select(name_prefix.as_bytes()).key().to_vec()
    }

    pub fn alkane_symbol_index_key(&self, symbol: &str, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(symbol.len() + 1 + 12);
        suffix.extend_from_slice(symbol.as_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.ALKANE_SYMBOL_INDEX.select(&suffix).key().to_vec()
    }

    pub fn alkane_symbol_index_prefix(&self, symbol_prefix: &str) -> Vec<u8> {
        self.ALKANE_SYMBOL_INDEX.select(symbol_prefix.as_bytes()).key().to_vec()
    }

    pub fn orbital_name_key(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        self.ORBITAL_NAME.select(&suffix).key().to_vec()
    }

    pub fn alkane_holders_ordered_key(&self, count: u64, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(8 + 12);
        suffix.extend_from_slice(&count.to_be_bytes());
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.HOLDERS_ORDERED.select(&suffix).key().to_vec()
    }

    pub fn alkane_holders_ordered_prefix(&self) -> Vec<u8> {
        self.HOLDERS_ORDERED.key().to_vec()
    }

    pub fn parse_alkane_name_index_key(&self, key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
        let prefix = self.ALKANE_NAME_INDEX.key();
        if !key.starts_with(prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        let split = rest.iter().rposition(|b| *b == b'/')?;
        let name_bytes = &rest[..split];
        let id_bytes = &rest[split + 1..];
        if id_bytes.len() != 12 {
            return None;
        }
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        let name = String::from_utf8(name_bytes.to_vec()).ok()?;
        Some((
            name,
            SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) },
        ))
    }

    pub fn parse_alkane_symbol_index_key(&self, key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
        let prefix = self.ALKANE_SYMBOL_INDEX.key();
        if !key.starts_with(prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        let split = rest.iter().rposition(|b| *b == b'/')?;
        let symbol_bytes = &rest[..split];
        let id_bytes = &rest[split + 1..];
        if id_bytes.len() != 12 {
            return None;
        }
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        let symbol = String::from_utf8(symbol_bytes.to_vec()).ok()?;
        Some((
            symbol,
            SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) },
        ))
    }

    pub fn parse_alkane_holders_ordered_key(&self, key: &[u8]) -> Option<(u64, SchemaAlkaneId)> {
        let prefix = self.HOLDERS_ORDERED.key();
        if !key.starts_with(prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != 20 {
            return None;
        }
        let mut count_arr = [0u8; 8];
        count_arr.copy_from_slice(&rest[..8]);
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&rest[8..12]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&rest[12..20]);
        Some((
            u64::from_be_bytes(count_arr),
            SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) },
        ))
    }

    pub fn alkane_creation_by_id_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.ALKANE_CREATION_BY_ID.select(&suffix).key().to_vec()
    }

    pub fn alkane_creation_seq_key(&self, seq: u64) -> Vec<u8> {
        self.ALKANE_CREATION_SEQ.select(&seq.to_be_bytes()).key().to_vec()
    }

    pub fn circulating_supply_key(&self, alkane: &SchemaAlkaneId, height: u32) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 4);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        suffix.extend_from_slice(&height.to_be_bytes());
        self.CIRCULATING_SUPPLY.select(&suffix).key().to_vec()
    }

    pub fn circulating_supply_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.CIRCULATING_SUPPLY.select(&suffix).key().to_vec()
    }

    pub fn circulating_supply_latest_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.CIRCULATING_SUPPLY_LATEST.select(&suffix).key().to_vec()
    }

    pub fn total_minted_key(&self, alkane: &SchemaAlkaneId, height: u32) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 4);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        suffix.extend_from_slice(&height.to_be_bytes());
        self.TOTAL_MINTED.select(&suffix).key().to_vec()
    }

    pub fn total_minted_prefix(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.TOTAL_MINTED.select(&suffix).key().to_vec()
    }

    pub fn total_minted_latest_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.TOTAL_MINTED_LATEST.select(&suffix).key().to_vec()
    }

    pub fn alkane_creation_count_key(&self) -> Vec<u8> {
        self.ALKANE_CREATION_COUNT.key().to_vec()
    }

    pub fn alkane_creations_in_block_key(&self, height: u32) -> Vec<u8> {
        self.ALKANE_CREATIONS_IN_BLOCK.select(&height.to_be_bytes()).key().to_vec()
    }

    pub fn alkane_factory_child_key(
        &self,
        factory: &SchemaAlkaneId,
        child: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1 + 12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&child.block.to_be_bytes());
        suffix.extend_from_slice(&child.tx.to_be_bytes());
        self.ALKANE_FACTORY_CHILDREN.select(&suffix).key().to_vec()
    }

    pub fn alkane_factory_children_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        self.ALKANE_FACTORY_CHILDREN.select(&suffix).key().to_vec()
    }

    pub fn alkane_tx_summary_key(&self, txid: &[u8; 32]) -> Vec<u8> {
        self.tx_packed_outflow_pos_point_key(txid)
    }

    pub fn alkane_block_txid_key(&self, height: u64, idx: u64) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(8 + 1 + 8);
        suffix.extend_from_slice(&height.to_be_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&idx.to_be_bytes());
        self.ALKANE_BLOCK.select(&suffix).key().to_vec()
    }

    pub fn alkane_block_len_key(&self, height: u64) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(8 + 7);
        suffix.extend_from_slice(&height.to_be_bytes());
        suffix.extend_from_slice(b"/length");
        self.ALKANE_BLOCK.select(&suffix).key().to_vec()
    }

    pub fn alkane_address_txid_key(&self, addr: &str, idx: u64) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(addr.len() + 1 + 8);
        suffix.extend_from_slice(addr.as_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&idx.to_be_bytes());
        self.ALKANE_ADDR.select(&suffix).key().to_vec()
    }

    pub fn alkane_address_len_key(&self, addr: &str) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(addr.len() + 7);
        suffix.extend_from_slice(addr.as_bytes());
        suffix.extend_from_slice(b"/length");
        self.ALKANE_ADDR.select(&suffix).key().to_vec()
    }

    pub fn alkane_latest_traces_key(&self) -> Vec<u8> {
        self.ALKANE_LATEST_TRACES.key().to_vec()
    }

    pub fn outpoint_addr_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let suffix = borsh::to_vec(outp)?;
        Ok(self.OUTPOINT_ADDR.select(&suffix).key().to_vec())
    }

    pub fn utxo_spk_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        let suffix = borsh::to_vec(outp)?;
        Ok(self.UTXO_SPK.select(&suffix).key().to_vec())
    }

    pub fn outpoint_balances_key(&self, outp: &EspoOutpoint) -> Result<Vec<u8>> {
        self.outpoint_pos_point_key_from_parts(&outp.txid, outp.vout)
    }

    pub fn block_summary_key(&self, height: u32) -> Vec<u8> {
        self.BLOCK_SUMMARY.select(&height.to_be_bytes()).key().to_vec()
    }

    pub fn block_summary_by_hash_key(&self, blockhash: &BlockHash) -> Vec<u8> {
        self.BLOCK_SUMMARY.select(blockhash.to_string().as_bytes()).key().to_vec()
    }

    pub fn height_to_hash_length_key(&self, height: u32) -> Vec<u8> {
        self.HEIGHT_TO_HASH.select(format!("{height}/length").as_bytes()).key().to_vec()
    }

    pub fn height_to_hash_version_key(&self, height: u32, version: u32) -> Vec<u8> {
        self.HEIGHT_TO_HASH
            .select(format!("{height}/{version}").as_bytes())
            .key()
            .to_vec()
    }

    pub fn block_summary_prefix(&self) -> Vec<u8> {
        self.BLOCK_SUMMARY.key().to_vec()
    }

    pub fn outpoint_balances_prefix(&self, txid: &[u8], vout: u32) -> Result<Vec<u8>> {
        self.outpoint_pos_point_key_from_parts(txid, vout)
    }
}

#[derive(Clone)]
pub struct EssentialsProvider {
    mdb: Arc<Mdb>,
    blob_mdb: Arc<Mdb>,
    view_blockhash: Option<BlockHash>,
}

impl EssentialsProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        let blob_mdb = Arc::new(mdb.clone_with_prefix(b"essentials_blob:"));
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

    pub fn table(&self) -> EssentialsTable<'_> {
        EssentialsTable::new(self.mdb.as_ref())
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb.as_ref()
    }

    pub fn blob_mdb(&self) -> &Mdb {
        self.blob_mdb.as_ref()
    }

    pub fn view_blockhash(&self) -> Option<BlockHash> {
        self.view_blockhash
    }

    pub fn resolved_view_blockhash(&self) -> Option<BlockHash> {
        self.view_blockhash
            .or_else(|| get_global_tree_db().and_then(|tree| tree.active_blockhash()))
    }

    pub fn blockhash_is_ancestor(
        &self,
        ancestor: &BlockHash,
        descendant: &BlockHash,
    ) -> Result<bool> {
        let Some(tree) = get_global_tree_db() else {
            return Ok(false);
        };
        tree.is_ancestor(ancestor, descendant)
            .map_err(|e| anyhow!("tree.is_ancestor failed: {e}"))
    }

    pub fn blockhash_is_on_active_chain(&self, blockhash: &BlockHash) -> Result<bool> {
        let Some(tree) = get_global_tree_db() else {
            return Ok(false);
        };
        let Some(height) = tree
            .height_for_blockhash(blockhash)
            .map_err(|e| anyhow!("tree.height_for_blockhash failed: {e}"))?
        else {
            return Ok(false);
        };
        let Some(active_blockhash_at_height) = tree
            .blockhash_for_height(height)
            .map_err(|e| anyhow!("tree.blockhash_for_height failed: {e}"))?
        else {
            return Ok(false);
        };
        Ok(active_blockhash_at_height == *blockhash)
    }

    pub fn blockhash_for_height(&self, height: u32) -> Result<Option<BlockHash>> {
        let Some(tree) = get_global_tree_db() else {
            return Ok(None);
        };
        tree.blockhash_for_height(height)
            .map_err(|e| anyhow!("tree.blockhash_for_height failed: {e}"))
    }

    pub fn non_mutating_pointer(&self) -> ListNonMutatePointer<'_> {
        ListNonMutatePointer::root(self.mdb.as_ref(), self.blob_mdb.as_ref())
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

    fn raw_multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        match self.view_blockhash {
            Some(blockhash) => self.raw_multi_get_at(keys, Some(blockhash)),
            None => self.mdb.multi_get(keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}")),
        }
    }

    fn raw_multi_get_at(
        &self,
        keys: &[Vec<u8>],
        blockhash: Option<BlockHash>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        match blockhash {
            Some(blockhash) => self
                .mdb
                .multi_get_at_blockhash(&blockhash, keys)
                .map_err(|e| anyhow!("mdb.multi_get_at_blockhash failed: {e}")),
            None => self.mdb.multi_get(keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}")),
        }
    }

    fn raw_blob_multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.blob_mdb
            .multi_get(keys)
            .map_err(|e| anyhow!("blob_mdb.multi_get failed: {e}"))
    }

    fn raw_scan_prefix_entries_at(
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

    fn raw_scan_prefix_keys_at(
        &self,
        prefix: &[u8],
        blockhash: Option<BlockHash>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = match blockhash {
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

    pub fn get_raw_value(&self, params: GetRawValueParams) -> Result<GetRawValueResult> {
        let value = self.raw_get_at(&params.key, params.blockhash.resolve(self.view_blockhash))?;
        Ok(GetRawValueResult { value })
    }

    pub fn get_multi_values(&self, params: GetMultiValuesParams) -> Result<GetMultiValuesResult> {
        let values =
            self.raw_multi_get_at(&params.keys, params.blockhash.resolve(self.view_blockhash))?;
        Ok(GetMultiValuesResult { values })
    }

    pub fn get_blob_raw_value(&self, params: GetRawValueParams) -> Result<GetRawValueResult> {
        let value = self.raw_blob_get(&params.key)?;
        Ok(GetRawValueResult { value })
    }

    pub fn get_blob_multi_values(
        &self,
        params: GetMultiValuesParams,
    ) -> Result<GetMultiValuesResult> {
        let values = self.raw_blob_multi_get(&params.keys)?;
        Ok(GetMultiValuesResult { values })
    }

    pub fn get_list_keys_by_prefix(
        &self,
        params: GetListKeysByPrefixParams,
    ) -> Result<GetListKeysByPrefixResult> {
        let keys = self.raw_scan_prefix_keys_at(
            &params.prefix,
            params.blockhash.resolve(self.view_blockhash),
        )?;
        Ok(GetListKeysByPrefixResult { keys })
    }

    pub fn get_list_entries_desc(
        &self,
        params: GetListEntriesDescParams,
    ) -> Result<GetListEntriesDescResult> {
        let mut entries = self.raw_scan_prefix_entries_at(
            &params.prefix,
            params.blockhash.resolve(self.view_blockhash),
        )?;
        entries.reverse();
        Ok(GetListEntriesDescResult { entries })
    }

    pub fn get_list_entries_desc_cursor(
        &self,
        params: GetListEntriesDescCursorParams,
    ) -> Result<GetListEntriesDescCursorResult> {
        let list = ListPointer::root(self.mdb.as_ref()).select(&params.prefix);
        let cursor_page: CursorScanPage = list.scan_desc_cursor_page(
            params.blockhash.resolve(self.view_blockhash).as_ref(),
            params.cursor.as_deref(),
            params.limit.max(1),
        )?;
        Ok(GetListEntriesDescCursorResult {
            entries: cursor_page.entries,
            next_cursor: cursor_page.next_cursor,
            has_more: cursor_page.has_more,
        })
    }

    pub fn set_raw_value(&self, params: SetRawValueParams) -> Result<()> {
        self.set_batch(SetBatchParams {
            blockhash: params.blockhash,
            puts: vec![(params.key, params.value)],
            deletes: Vec::new(),
        })
    }

    pub fn set_batch(&self, params: SetBatchParams) -> Result<()> {
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }
        let batch_progress = std::env::var_os("ESPO_BATCH_PROGRESS").is_some();
        let started = std::time::Instant::now();
        let versioned = self.mdb.is_versioned();

        if batch_progress {
            eprintln!(
                "[storage.set_batch] begin versioned={} puts={} deletes={}",
                versioned,
                params.puts.len(),
                params.deletes.len()
            );
        }

        // Versioned B+Tree writes are canonically deduped in tree_db (last-write-wins),
        // so skipping this pre-pass avoids a large clone-heavy batch rewrite.
        let (all_puts, all_deletes) = if versioned {
            (params.puts, params.deletes)
        } else {
            dedupe_batch_ops(params.puts, params.deletes)
        };

        if batch_progress {
            eprintln!(
                "[storage.set_batch] prepared puts={} deletes={}",
                all_puts.len(),
                all_deletes.len()
            );
        }

        let res = self
            .mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &all_deletes {
                    wb.delete(key);
                }
                for (key, value) in &all_puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"));

        if batch_progress {
            eprintln!("[storage.set_batch] end elapsed_ms={}", started.elapsed().as_millis());
        }

        res
    }

    pub fn set_blob_values_if_missing(&self, params: SetBlobValuesIfMissingParams) -> Result<()> {
        if params.puts.is_empty() {
            return Ok(());
        }
        if params.blockhash.resolve(self.view_blockhash).is_some() {
            return Err(anyhow!("cannot_write_historical_view"));
        }

        let mut dedup: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(params.puts.len());
        for (k, v) in params.puts {
            dedup.entry(k).or_insert(v);
        }
        if dedup.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<Vec<u8>> = dedup.keys().cloned().collect();
        keys.sort();
        let existing = self
            .blob_mdb
            .multi_get(&keys)
            .map_err(|e| anyhow!("blob_mdb.multi_get failed: {e}"))?;

        let mut write_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (idx, key) in keys.into_iter().enumerate() {
            if existing.get(idx).and_then(|v| v.as_ref()).is_some() {
                continue;
            }
            let Some(value) = dedup.remove(&key) else {
                continue;
            };
            write_pairs.push((key, value));
        }

        if write_pairs.is_empty() {
            return Ok(());
        }

        self.blob_mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for (key, value) in &write_pairs {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("blob_mdb.bulk_write failed: {e}"))
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
            return Err(anyhow!("[ESSENTIALS] invalid /index_height length {}", bytes.len()));
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
        table
            .INDEX_HEIGHT
            .put(&params.height.to_le_bytes())
            .map_err(|e| anyhow!("[ESSENTIALS] rocksdb put(/index_height) failed: {e}"))
    }

    pub fn get_creation_record(
        &self,
        params: GetCreationRecordParams,
    ) -> Result<GetCreationRecordResult> {
        crate::debug_timer_log!("get_creation_record");
        let table = self.table();
        let key = table.alkane_creation_by_id_key(&params.alkane);
        let Some(bytes) = self.raw_get_at(&key, params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(GetCreationRecordResult { record: None });
        };
        let record = decode_creation_record(&bytes)?;
        Ok(GetCreationRecordResult { record: Some(record) })
    }

    pub fn get_creation_records_by_id(
        &self,
        params: GetCreationRecordsByIdParams,
    ) -> Result<GetCreationRecordsByIdResult> {
        crate::debug_timer_log!("get_creation_records_by_id");
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            params.alkanes.iter().map(|alk| table.alkane_creation_by_id_key(alk)).collect();
        let values = self.raw_multi_get_at(&keys, params.blockhash.resolve(self.view_blockhash))?;
        let mut records = Vec::with_capacity(values.len());
        for val in values {
            if let Some(bytes) = val {
                records.push(Some(decode_creation_record(&bytes)?));
            } else {
                records.push(None);
            }
        }
        Ok(GetCreationRecordsByIdResult { records })
    }

    pub fn get_creation_records_ordered(
        &self,
        params: GetCreationRecordsOrderedParams,
    ) -> Result<GetCreationRecordsOrderedResult> {
        crate::debug_timer_log!("get_creation_records_ordered");
        let started = Instant::now();
        let debug = creation_debug_enabled();
        let slow_threshold_ms: u128 = 250;
        let total = self
            .get_creation_count(GetCreationCountParams { blockhash: params.blockhash })?
            .count;
        if total == 0 {
            if debug {
                eprintln!(
                    "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered state={:?} total=0 elapsed_ms={}",
                    params.blockhash,
                    started.elapsed().as_millis()
                );
            }
            return Ok(GetCreationRecordsOrderedResult { records: Vec::new() });
        }
        let records = self
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: params.blockhash,
                offset: 0,
                limit: total,
                desc: true,
            })?
            .records;
        let elapsed_ms = started.elapsed().as_millis();
        if debug || elapsed_ms >= slow_threshold_ms {
            eprintln!(
                "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered state={:?} total={} requested_limit={} records_returned={} elapsed_ms={}",
                params.blockhash,
                total,
                total,
                records.len(),
                elapsed_ms
            );
        }
        Ok(GetCreationRecordsOrderedResult { records })
    }

    pub fn get_creation_records_ordered_page(
        &self,
        params: GetCreationRecordsOrderedPageParams,
    ) -> Result<GetCreationRecordsOrderedPageResult> {
        crate::debug_timer_log!("get_creation_records_ordered_page");
        let started = Instant::now();
        let debug = creation_debug_enabled();
        let slow_threshold_ms: u128 = 250;
        if params.limit == 0 {
            if debug {
                eprintln!(
                    "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered_page state={:?} offset={} limit=0 desc={} elapsed_ms={}",
                    params.blockhash,
                    params.offset,
                    params.desc,
                    started.elapsed().as_millis()
                );
            }
            return Ok(GetCreationRecordsOrderedPageResult { records: Vec::new() });
        }
        let at_blockhash = params.blockhash.resolve(self.view_blockhash);
        let table = self.table();
        let count_started = Instant::now();
        let total = self
            .get_creation_count(GetCreationCountParams { blockhash: params.blockhash })?
            .count;
        let count_elapsed_ms = count_started.elapsed().as_millis();
        let (start_seq, end_seq, reverse) =
            creation_seq_bounds(total, params.offset, params.limit, params.desc);
        if start_seq >= end_seq {
            let elapsed_ms = started.elapsed().as_millis();
            if debug || elapsed_ms >= slow_threshold_ms {
                eprintln!(
                    "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered_page state={:?} offset={} limit={} desc={} total={} window_start={} window_end={} reverse={} seq_keys=0 seq_hits=0 record_keys=0 record_hits=0 decoded=0 decode_fail=0 count_elapsed_ms={} seq_elapsed_ms=0 record_elapsed_ms=0 elapsed_ms={}",
                    params.blockhash,
                    params.offset,
                    params.limit,
                    params.desc,
                    total,
                    start_seq,
                    end_seq,
                    reverse,
                    count_elapsed_ms,
                    elapsed_ms
                );
            }
            return Ok(GetCreationRecordsOrderedPageResult { records: Vec::new() });
        }

        let mut seq_keys: Vec<Vec<u8>> =
            (start_seq..end_seq).map(|seq| table.alkane_creation_seq_key(seq)).collect();
        if reverse {
            seq_keys.reverse();
        }

        let seq_fetch_started = Instant::now();
        let seq_values = self.raw_multi_get_at(&seq_keys, at_blockhash)?;
        let seq_elapsed_ms = seq_fetch_started.elapsed().as_millis();
        let seq_hits = seq_values.iter().filter(|value| value.is_some()).count();
        let mut record_keys: Vec<Vec<u8>> = Vec::with_capacity(seq_values.len());
        for raw in seq_values.into_iter().flatten() {
            let Some(alkane) = decode_alkane_id_be(&raw) else {
                continue;
            };
            record_keys.push(table.alkane_creation_by_id_key(&alkane));
        }
        if record_keys.is_empty() {
            let elapsed_ms = started.elapsed().as_millis();
            if debug || elapsed_ms >= slow_threshold_ms {
                eprintln!(
                    "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered_page state={:?} offset={} limit={} desc={} total={} window_start={} window_end={} reverse={} seq_keys={} seq_hits={} record_keys=0 record_hits=0 decoded=0 decode_fail=0 count_elapsed_ms={} seq_elapsed_ms={} record_elapsed_ms=0 elapsed_ms={}",
                    params.blockhash,
                    params.offset,
                    params.limit,
                    params.desc,
                    total,
                    start_seq,
                    end_seq,
                    reverse,
                    seq_keys.len(),
                    seq_hits,
                    count_elapsed_ms,
                    seq_elapsed_ms,
                    elapsed_ms
                );
            }
            return Ok(GetCreationRecordsOrderedPageResult { records: Vec::new() });
        }

        let record_fetch_started = Instant::now();
        let record_values = self.raw_multi_get_at(&record_keys, at_blockhash)?;
        let record_elapsed_ms = record_fetch_started.elapsed().as_millis();
        let record_hits = record_values.iter().filter(|value| value.is_some()).count();
        let mut records = Vec::with_capacity(record_values.len());
        let mut decode_failures = 0usize;
        for value in record_values {
            let Some(v) = value else { continue };
            if let Ok(rec) = decode_creation_record(&v) {
                records.push(rec);
            } else {
                decode_failures = decode_failures.saturating_add(1);
            }
        }
        let elapsed_ms = started.elapsed().as_millis();
        if debug || elapsed_ms >= slow_threshold_ms {
            eprintln!(
                "[debug] module=espo::modules::essentials::storage fn=get_creation_records_ordered_page state={:?} offset={} limit={} desc={} total={} window_start={} window_end={} reverse={} seq_keys={} seq_hits={} record_keys={} record_hits={} decoded={} decode_fail={} count_elapsed_ms={} seq_elapsed_ms={} record_elapsed_ms={} elapsed_ms={}",
                params.blockhash,
                params.offset,
                params.limit,
                params.desc,
                total,
                start_seq,
                end_seq,
                reverse,
                seq_keys.len(),
                seq_hits,
                record_keys.len(),
                record_hits,
                records.len(),
                decode_failures,
                count_elapsed_ms,
                seq_elapsed_ms,
                record_elapsed_ms,
                elapsed_ms
            );
        }

        Ok(GetCreationRecordsOrderedPageResult { records })
    }

    pub fn get_alkane_ids_by_name_prefix(
        &self,
        params: GetAlkaneIdsByNamePrefixParams,
    ) -> Result<GetAlkaneIdsByNamePrefixResult> {
        crate::debug_timer_log!("get_alkane_ids_by_name_prefix");
        let table = self.table();
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: table.alkane_name_index_prefix(&params.prefix),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for key in keys {
            if let Some((_name, id)) = table.parse_alkane_name_index_key(&key) {
                if seen.insert(id) {
                    ids.push(id);
                }
            }
        }
        Ok(GetAlkaneIdsByNamePrefixResult { ids })
    }

    pub fn get_alkane_ids_by_name_prefix_page(
        &self,
        params: GetAlkaneIdsByNamePrefixPageParams,
    ) -> Result<GetAlkaneIdsByNamePrefixResult> {
        crate::debug_timer_log!("get_alkane_ids_by_name_prefix_page");
        let table = self.table();
        let prefix = table.alkane_name_index_prefix(&params.prefix);
        let mut keys = self
            .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|v| v.keys)
            .unwrap_or_default();
        keys.sort();
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        let mut unique_skipped: u64 = 0;

        for key in keys {
            if let Some((_name, id)) = table.parse_alkane_name_index_key(&key) {
                if seen.insert(id) {
                    if unique_skipped < params.offset {
                        unique_skipped += 1;
                        continue;
                    }
                    ids.push(id);
                    if ids.len() >= params.limit as usize {
                        break;
                    }
                }
            }
        }

        Ok(GetAlkaneIdsByNamePrefixResult { ids })
    }

    pub fn get_alkane_ids_by_symbol_prefix(
        &self,
        params: GetAlkaneIdsBySymbolPrefixParams,
    ) -> Result<GetAlkaneIdsBySymbolPrefixResult> {
        crate::debug_timer_log!("get_alkane_ids_by_symbol_prefix");
        let table = self.table();
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: table.alkane_symbol_index_prefix(&params.prefix),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for key in keys {
            if let Some((_sym, id)) = table.parse_alkane_symbol_index_key(&key) {
                if seen.insert(id) {
                    ids.push(id);
                }
            }
        }
        Ok(GetAlkaneIdsBySymbolPrefixResult { ids })
    }

    pub fn get_alkane_ids_by_symbol_prefix_page(
        &self,
        params: GetAlkaneIdsBySymbolPrefixPageParams,
    ) -> Result<GetAlkaneIdsBySymbolPrefixResult> {
        crate::debug_timer_log!("get_alkane_ids_by_symbol_prefix_page");
        let table = self.table();
        let prefix = table.alkane_symbol_index_prefix(&params.prefix);
        let mut keys = self
            .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|v| v.keys)
            .unwrap_or_default();
        keys.sort();
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        let mut unique_skipped: u64 = 0;

        for key in keys {
            if let Some((_symbol, id)) = table.parse_alkane_symbol_index_key(&key) {
                if seen.insert(id) {
                    if unique_skipped < params.offset {
                        unique_skipped += 1;
                        continue;
                    }
                    ids.push(id);
                    if ids.len() >= params.limit as usize {
                        break;
                    }
                }
            }
        }

        Ok(GetAlkaneIdsBySymbolPrefixResult { ids })
    }

    pub fn get_creation_count(
        &self,
        params: GetCreationCountParams,
    ) -> Result<GetCreationCountResult> {
        crate::debug_timer_log!("get_creation_count");
        let table = self.table();
        let count = self
            .get_raw_value(GetRawValueParams {
                blockhash: params.blockhash,
                key: table.alkane_creation_count_key(),
            })?
            .value
            .and_then(|b| {
                if b.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&b);
                    Some(u64::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        Ok(GetCreationCountResult { count })
    }

    pub fn get_creation_ids_in_block(
        &self,
        params: GetCreationIdsInBlockParams,
    ) -> Result<GetCreationIdsInBlockResult> {
        crate::debug_timer_log!("get_creation_ids_in_block");
        let table = self.table();
        let key = table.alkane_creations_in_block_key(params.height);
        let Some(bytes) = self.raw_get_at(&key, params.blockhash.resolve(self.view_blockhash))?
        else {
            return Ok(GetCreationIdsInBlockResult { alkanes: Vec::new() });
        };
        let alkanes = Vec::<SchemaAlkaneId>::try_from_slice(&bytes).map_err(|e| {
            anyhow!(
                "[ESSENTIALS] decode /alkanes/creation/in_block/v2 failed (height={}): {e}",
                params.height
            )
        })?;
        Ok(GetCreationIdsInBlockResult { alkanes })
    }

    pub fn get_factory_children(
        &self,
        params: GetFactoryChildrenParams,
    ) -> Result<GetFactoryChildrenResult> {
        crate::debug_timer_log!("get_factory_children");
        let table = self.table();
        let prefix = table.alkane_factory_children_prefix(&params.factory);
        let children = self.read_factory_children_from_index(&prefix, params.blockhash)?;

        Ok(GetFactoryChildrenResult { children })
    }

    fn read_factory_children_from_index(
        &self,
        prefix: &[u8],
        blockhash: StateAt,
    ) -> Result<Vec<SchemaAlkaneId>> {
        let keys = self
            .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash,
                prefix: prefix.to_vec(),
            })
            .map(|res| res.keys)
            .unwrap_or_default();
        let mut children = Vec::new();
        for key in keys {
            if key.len() < prefix.len() + 12 {
                continue;
            }
            let Some(child) = decode_alkane_id_be(&key[prefix.len()..]) else {
                continue;
            };
            children.push(child);
        }
        children.sort_by(|a, b| a.block.cmp(&b.block).then_with(|| a.tx.cmp(&b.tx)));
        children.dedup();
        Ok(children)
    }

    pub fn get_holders_count(
        &self,
        params: GetHoldersCountParams,
    ) -> Result<GetHoldersCountResult> {
        crate::debug_timer_log!("get_holders_count");
        let table = self.table();
        let count = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.holders_count_key(&params.alkane),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| HoldersCountEntry::try_from_slice(&raw).ok())
            .map(|entry| entry.count)
            .unwrap_or(0);
        Ok(GetHoldersCountResult { count })
    }

    pub fn get_holders_counts_by_id(
        &self,
        params: GetHoldersCountsByIdParams,
    ) -> Result<GetHoldersCountsByIdResult> {
        crate::debug_timer_log!("get_holders_counts_by_id");
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            params.alkanes.iter().map(|alk| table.holders_count_key(alk)).collect();
        let values = self.raw_multi_get(&keys)?;
        let mut counts = Vec::with_capacity(values.len());
        for val in values {
            let count = val
                .and_then(|raw| HoldersCountEntry::try_from_slice(&raw).ok())
                .map(|entry| entry.count)
                .unwrap_or(0);
            counts.push(count);
        }
        Ok(GetHoldersCountsByIdResult { counts })
    }

    pub fn get_holders_ordered_page(
        &self,
        params: GetHoldersOrderedPageParams,
    ) -> Result<GetHoldersOrderedPageResult> {
        crate::debug_timer_log!("get_holders_ordered_page");
        let table = self.table();
        let prefix = table.alkane_holders_ordered_prefix();
        let mut keys = if params.desc {
            self.get_list_entries_desc(GetListEntriesDescParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|resp| resp.entries.into_iter().map(|(k, _)| k).collect())
            .unwrap_or_default()
        } else {
            self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|resp| resp.keys)
            .unwrap_or_default()
        };
        if !params.desc {
            keys.sort();
        }
        let mut ids = Vec::new();
        let mut skipped: u64 = 0;
        for key in keys {
            let Some((_count, id)) = table.parse_alkane_holders_ordered_key(&key) else {
                continue;
            };
            if skipped < params.offset {
                skipped += 1;
                continue;
            }
            ids.push(id);
            if ids.len() >= params.limit as usize {
                break;
            }
        }

        Ok(GetHoldersOrderedPageResult { ids })
    }

    pub fn get_latest_circulating_supply(
        &self,
        params: GetLatestCirculatingSupplyParams,
    ) -> Result<GetLatestCirculatingSupplyResult> {
        crate::debug_timer_log!("get_latest_circulating_supply");
        let table = self.table();
        let supply = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.circulating_supply_latest_key(&params.alkane),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_u128_value(&raw).ok())
            .unwrap_or(0);
        Ok(GetLatestCirculatingSupplyResult { supply })
    }

    pub fn get_latest_total_minted(
        &self,
        params: GetLatestTotalMintedParams,
    ) -> Result<GetLatestTotalMintedResult> {
        crate::debug_timer_log!("get_latest_total_minted");
        let table = self.table();
        let total_minted = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.total_minted_latest_key(&params.alkane),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_u128_value(&raw).ok())
            .unwrap_or(0);
        Ok(GetLatestTotalMintedResult { total_minted })
    }

    pub fn get_circulating_supply(
        &self,
        params: GetCirculatingSupplyParams,
    ) -> Result<GetCirculatingSupplyResult> {
        crate::debug_timer_log!("get_circulating_supply");
        let table = self.table();
        // Historical callers pin this provider to the requested block hash. The latest-supply key
        // at that tree root is the supply at the requested height, including blocks where supply
        // did not change and no per-height event row was written.
        let supply = self
            .get_raw_value(GetRawValueParams {
                blockhash: params.blockhash,
                key: table.circulating_supply_latest_key(&params.alkane),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_u128_value(&raw).ok())
            .unwrap_or(0);
        Ok(GetCirculatingSupplyResult { supply })
    }

    pub fn get_alkane_storage_value(
        &self,
        params: GetAlkaneStorageValueParams,
    ) -> Result<GetAlkaneStorageValueResult> {
        crate::debug_timer_log!("get_alkane_storage_value");
        let table = self.table();
        let key = table.kv_row_key(&params.alkane, &params.key);
        let value = self
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
            .map(|raw| split_txid_value(&raw).1.to_vec());
        Ok(GetAlkaneStorageValueResult { value })
    }

    pub fn get_block_summary(
        &self,
        params: GetBlockSummaryParams,
    ) -> Result<GetBlockSummaryResult> {
        crate::debug_timer_log!("get_block_summary");
        let summary = match params.blockhash {
            StateAt::Block(blockhash) => self.get_block_summary_by_hash(&blockhash)?,
            StateAt::Latest => self.get_latest_block_summary_by_height(params.height)?,
        };
        Ok(GetBlockSummaryResult { summary })
    }

    pub fn get_block_summaries_by_heights(
        &self,
        heights: &[u32],
    ) -> Result<Vec<Option<BlockSummary>>> {
        crate::debug_timer_log!("get_block_summaries_by_heights");
        if heights.is_empty() {
            return Ok(Vec::new());
        }
        let table = self.table();

        if let Some(tree) = get_global_tree_db() {
            let mut out: Vec<Option<BlockSummary>> = vec![None; heights.len()];
            let mut canonical_hashes: Vec<Option<BlockHash>> = Vec::with_capacity(heights.len());
            let mut summary_keys: Vec<Vec<u8>> = Vec::new();
            let mut summary_key_positions: Vec<usize> = Vec::new();
            for (idx, height) in heights.iter().copied().enumerate() {
                let canonical_hash = tree
                    .blockhash_for_height(height)
                    .map_err(|e| anyhow!("tree.blockhash_for_height failed: {e}"))?;
                if let Some(blockhash) = canonical_hash {
                    summary_keys.push(table.block_summary_by_hash_key(&blockhash));
                    summary_key_positions.push(idx);
                }
                canonical_hashes.push(canonical_hash);
            }

            if !summary_keys.is_empty() {
                let summary_values = self.raw_blob_multi_get(&summary_keys)?;
                for (raw, idx) in summary_values.iter().zip(summary_key_positions.iter().copied()) {
                    let Some(summary) = raw.as_ref().and_then(|bytes| BlockSummary::decode(bytes))
                    else {
                        continue;
                    };
                    if summary.block_hash() == canonical_hashes[idx] {
                        out[idx] = Some(summary);
                    }
                }
            }

            if out.iter().any(|summary| summary.is_none()) {
                let legacy = self.get_legacy_block_summaries_by_heights(heights)?;
                for (idx, legacy_summary) in legacy.into_iter().enumerate() {
                    if out[idx].is_some() {
                        continue;
                    }
                    let Some(canonical_hash) = canonical_hashes[idx] else {
                        continue;
                    };
                    let Some(summary) = legacy_summary else {
                        continue;
                    };
                    let summary_hash = summary.block_hash();
                    if summary_hash.is_none() || summary_hash == Some(canonical_hash) {
                        out[idx] = Some(summary);
                    }
                }
            }

            return Ok(out);
        }

        let length_keys: Vec<Vec<u8>> =
            heights.iter().map(|height| table.height_to_hash_length_key(*height)).collect();
        let length_values = self.raw_blob_multi_get(&length_keys)?;

        let mut hash_keys: Vec<Vec<u8>> = Vec::new();
        let mut hash_key_positions: Vec<usize> = Vec::new();
        let mut out: Vec<Option<BlockSummary>> = vec![None; heights.len()];
        for (idx, raw) in length_values.iter().enumerate() {
            let Some(length) = raw.as_ref().and_then(|bytes| decode_u32_le(bytes)) else {
                continue;
            };
            if length == 0 {
                continue;
            }
            hash_keys.push(table.height_to_hash_version_key(heights[idx], length - 1));
            hash_key_positions.push(idx);
        }

        if hash_keys.is_empty() {
            return self.get_legacy_block_summaries_by_heights(heights);
        }

        let hash_values = self.raw_blob_multi_get(&hash_keys)?;
        let mut summary_keys: Vec<Vec<u8>> = Vec::new();
        let mut summary_key_positions: Vec<usize> = Vec::new();
        for (raw, idx) in hash_values.iter().zip(hash_key_positions.iter().copied()) {
            let Some(blockhash) = raw.as_ref().and_then(|bytes| decode_blockhash(bytes)) else {
                continue;
            };
            summary_keys.push(table.block_summary_by_hash_key(&blockhash));
            summary_key_positions.push(idx);
        }

        if !summary_keys.is_empty() {
            let summary_values = self.raw_blob_multi_get(&summary_keys)?;
            for (raw, idx) in summary_values.iter().zip(summary_key_positions.iter().copied()) {
                if let Some(summary) = raw.as_ref().and_then(|bytes| BlockSummary::decode(bytes)) {
                    out[idx] = Some(summary);
                }
            }
        }

        if out.iter().any(|summary| summary.is_none()) {
            let legacy = self.get_legacy_block_summaries_by_heights(heights)?;
            for (slot, legacy_summary) in out.iter_mut().zip(legacy.into_iter()) {
                if slot.is_none() {
                    *slot = legacy_summary;
                }
            }
        }

        Ok(out)
    }

    fn get_latest_block_summary_by_height(&self, height: u32) -> Result<Option<BlockSummary>> {
        let summaries = self.get_block_summaries_by_heights(&[height])?;
        Ok(summaries.into_iter().next().flatten())
    }

    fn get_block_summary_by_hash(&self, blockhash: &BlockHash) -> Result<Option<BlockSummary>> {
        let table = self.table();
        let key = table.block_summary_by_hash_key(blockhash);
        Ok(self.raw_blob_get(&key)?.and_then(|bytes| BlockSummary::decode(&bytes)))
    }

    fn get_legacy_block_summaries_by_heights(
        &self,
        heights: &[u32],
    ) -> Result<Vec<Option<BlockSummary>>> {
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            heights.iter().map(|height| table.block_summary_key(*height)).collect();
        let values = self.raw_multi_get(&keys)?;
        Ok(values
            .into_iter()
            .map(|raw| raw.and_then(|bytes| BlockSummary::decode(&bytes)))
            .collect())
    }

    pub fn put_block_summary_indexes(&self, summary: &BlockSummary) -> Result<()> {
        let Some(blockhash) = summary.block_hash() else {
            return Err(anyhow!("block summary missing blockhash"));
        };
        let table = self.table();
        let summary_key = table.block_summary_by_hash_key(&blockhash);
        let length_key = table.height_to_hash_length_key(summary.height);
        let length = self
            .raw_blob_get(&length_key)?
            .as_ref()
            .and_then(|raw| decode_u32_le(raw))
            .unwrap_or(0);
        let version_key = table.height_to_hash_version_key(summary.height, length);
        let summary_bytes = borsh::to_vec(summary)?;
        let blockhash_bytes = encode_blockhash(&blockhash);
        let next_length = length.checked_add(1).ok_or_else(|| {
            anyhow!("height_to_hash length overflow for height {}", summary.height)
        })?;

        self.blob_mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                wb.put(&summary_key, &summary_bytes);
                wb.put(&version_key, &blockhash_bytes);
                wb.put(&length_key, &next_length.to_le_bytes());
            })
            .map_err(|e| anyhow!("blob_mdb.bulk_write block summary indexes failed: {e}"))?;
        Ok(())
    }

    pub fn update_block_summary_by_hash(&self, summary: &BlockSummary) -> Result<()> {
        let Some(blockhash) = summary.block_hash() else {
            return Err(anyhow!("block summary missing blockhash"));
        };
        let summary_key = self.table().block_summary_by_hash_key(&blockhash);
        let summary_bytes = borsh::to_vec(summary)?;
        self.blob_mdb
            .put(&summary_key, &summary_bytes)
            .map_err(|e| anyhow!("blob_mdb.put block summary failed: {e}"))?;
        Ok(())
    }

    pub fn get_mempool_seen_page(
        &self,
        params: GetMempoolSeenPageParams,
    ) -> Result<GetMempoolSeenPageResult> {
        crate::debug_timer_log!("get_mempool_seen_page");
        let (txids, has_more) = get_seen_txids_page(params.page, params.limit);
        Ok(GetMempoolSeenPageResult { txids, has_more })
    }

    pub fn get_mempool_entry(
        &self,
        params: GetMempoolEntryParams,
    ) -> Result<GetMempoolEntryResult> {
        crate::debug_timer_log!("get_mempool_entry");
        Ok(GetMempoolEntryResult { entry: get_tx_from_mempool(&params.txid) })
    }

    pub fn get_mempool_pending_for_address(
        &self,
        params: GetMempoolPendingForAddressParams,
    ) -> Result<GetMempoolPendingForAddressResult> {
        crate::debug_timer_log!("get_mempool_pending_for_address");
        Ok(GetMempoolPendingForAddressResult { entries: pending_for_address(&params.address) })
    }

    pub fn rpc_get_mempool_traces(
        &self,
        params: RpcGetMempoolTracesParams,
    ) -> Result<RpcGetMempoolTracesResult> {
        let page = params.page.unwrap_or(1).max(1) as usize;
        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let address = params.address.as_deref().and_then(normalize_address);
        let min_fee_paid = params.fee_paid.filter(|value| value.is_finite() && *value >= 0.0);

        let filtered = get_mempool_index_transactions_ordered_by_block_and_fee()
            .into_iter()
            .filter(|entry| entry.traces.as_ref().is_some_and(|traces| !traces.is_empty()))
            .filter(|entry| {
                address
                    .as_ref()
                    .map_or(true, |addr| entry.addresses.iter().any(|candidate| candidate == addr))
            })
            .filter(|entry| min_fee_paid.map_or(true, |fee_paid| entry.fee_rate >= fee_paid))
            .collect::<Vec<_>>();
        let total_traces = filtered
            .iter()
            .map(|entry| entry.traces.as_ref().map_or(0, Vec::len))
            .sum::<usize>();
        let offset = limit.saturating_mul(page.saturating_sub(1));
        let items = filtered
            .iter()
            .skip(offset)
            .take(limit)
            .map(mem_block_tx_to_json)
            .collect::<Vec<_>>();
        let has_more = offset.saturating_add(items.len()) < filtered.len();

        Ok(RpcGetMempoolTracesResult {
            value: json!({
                "ok": true,
                "page": page,
                "limit": limit,
                "has_more": has_more,
                "total": total_traces,
                "tx_total": filtered.len(),
                "items": items,
            }),
        })
    }

    pub fn rpc_get_keys(&self, params: RpcGetKeysParams) -> Result<RpcGetKeysResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetKeysResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "alkane should be a string like \"2:68441\" or \"0x2:0x10b59\""
                }),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetKeysResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "alkane should be a string like \"2:68441\" or \"0x2:0x10b59\""
                }),
            });
        };

        let try_decode_utf8 = params.try_decode_utf8.unwrap_or(true);
        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let table = self.table();
        let all_keys: Vec<Vec<u8>> = if let Some(arr) = params.keys {
            let mut v = Vec::with_capacity(arr.len());
            for it in arr {
                if let Some(bytes) = parse_key_str_to_bytes(&it) {
                    v.push(bytes);
                }
            }
            dedup_sort_keys(v)
        } else {
            let scan_pref = table.dir_list_prefix(&alk);
            let rel_keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash: StateAt::Latest,
                prefix: scan_pref,
            }) {
                Ok(v) => v.keys,
                Err(_) => Vec::new(),
            };

            let mut extracted: Vec<Vec<u8>> = Vec::with_capacity(rel_keys.len());
            for rel in rel_keys {
                if rel.len() < 1 + 4 + 8 + 2 || rel[0] != 0x03 {
                    continue;
                }
                let key_len = u16::from_be_bytes([rel[13], rel[14]]) as usize;
                if rel.len() < 1 + 4 + 8 + 2 + key_len {
                    continue;
                }
                extracted.push(rel[15..15 + key_len].to_vec());
            }
            dedup_sort_keys(extracted)
        };

        let total = all_keys.len();
        let offset = limit.saturating_mul(page.saturating_sub(1));
        let end = (offset + limit).min(total);
        let window = if offset >= total { &[][..] } else { &all_keys[offset..end] };
        let has_more = end < total;

        let mut items: Map<String, Value> = Map::with_capacity(window.len());
        for k in window.iter() {
            let kv_key = table.kv_row_key(&alk, k);
            let (last_txid_val, value_hex, value_str_val, value_u128_val) = match self
                .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: kv_key })
            {
                Ok(resp) => {
                    if let Some(v) = resp.value {
                        let (ltxid_opt, raw) = split_txid_value(&v);
                        (
                            ltxid_opt.map(Value::String).unwrap_or(Value::Null),
                            fmt_bytes_hex(raw),
                            utf8_or_null(raw),
                            u128_le_or_null(raw),
                        )
                    } else {
                        (Value::Null, "0x".to_string(), Value::Null, Value::Null)
                    }
                }
                Err(_) => (Value::Null, "0x".to_string(), Value::Null, Value::Null),
            };

            let key_hex = fmt_bytes_hex(k);
            let key_str_val = utf8_or_null(k);

            let top_key = if try_decode_utf8 {
                if let Value::String(s) = &key_str_val { s.clone() } else { key_hex.clone() }
            } else {
                key_hex.clone()
            };

            items.insert(
                top_key,
                json!({
                    "key_hex":    key_hex,
                    "key_str":    key_str_val,
                    "value_hex":  value_hex,
                    "value_str":  value_str_val,
                    "value_u128": value_u128_val,
                    "last_txid":  last_txid_val
                }),
            );
        }

        Ok(RpcGetKeysResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alk.block, alk.tx),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": Value::Object(items)
            }),
        })
    }

    /// Search a factory's children for contracts whose storage keys satisfy a
    /// set of conditions (ANDed). Scoping to a factory keeps the scan bounded
    /// to point lookups over an existing index — there is deliberately no
    /// unscoped variant, since arbitrary key/value scans over every contract
    /// would be a DoS vector.
    pub fn rpc_search_factory_keys(
        &self,
        params: RpcSearchFactoryKeysParams,
    ) -> Result<RpcSearchFactoryKeysResult> {
        let err = |error: &str, hint: &str| -> Result<RpcSearchFactoryKeysResult> {
            Ok(RpcSearchFactoryKeysResult {
                value: json!({"ok": false, "error": error, "hint": hint}),
            })
        };

        let Some(factory) = params.factory.as_deref().and_then(parse_alkane_from_str) else {
            return err(
                "missing_or_invalid_factory",
                "provide factory as \"<block>:<tx>\" (hex ok)",
            );
        };

        // Conditions: either an explicit `conditions` array or the single
        // top-level key/op/value shorthand.
        let raw_conditions: Vec<Value> = match params.conditions {
            Some(arr) if !arr.is_empty() => arr,
            _ => match params.key.as_deref() {
                Some(key) => vec![json!({
                    "key": key,
                    "op": params.op.clone().unwrap_or_else(|| "eq".to_string()),
                    "value": params.value.clone(),
                })],
                None => Vec::new(),
            },
        };
        if raw_conditions.is_empty() {
            return err(
                "missing_conditions",
                "provide conditions: [{key, op, value}] or top-level key/op/value; ops: eq, ne, gt, lt, ge, le, exists",
            );
        }
        if raw_conditions.len() > MAX_KV_SEARCH_CONDITIONS {
            return err("too_many_conditions", "at most 8 conditions per query");
        }
        let mut conditions: Vec<KvSearchCondition> = Vec::with_capacity(raw_conditions.len());
        for raw in &raw_conditions {
            match KvSearchCondition::parse(raw) {
                Ok(cond) => conditions.push(cond),
                Err(hint) => return err("invalid_condition", &hint),
            }
        }

        let try_decode_utf8 = params.try_decode_utf8.unwrap_or(true);
        let limit = (params.limit.unwrap_or(100).max(1) as usize).min(MAX_KV_SEARCH_LIMIT);
        let page = params.page.unwrap_or(1).max(1) as usize;

        let table = self.table();
        let children = self
            .get_factory_children(GetFactoryChildrenParams { blockhash: StateAt::Latest, factory })?
            .children;
        if children.len() > MAX_KV_SEARCH_CHILDREN {
            return err("factory_too_large", "factory has too many children for a key/value scan");
        }

        // Evaluate conditions in order, narrowing the survivor set each pass.
        // Every condition requires the key to exist on the contract (an
        // absent key never matches, including `ne`).
        let mut survivors: Vec<SchemaAlkaneId> = children;
        let mut matched_values: HashMap<SchemaAlkaneId, Map<String, Value>> = HashMap::new();
        for cond in &conditions {
            if survivors.is_empty() {
                break;
            }
            let keys: Vec<Vec<u8>> =
                survivors.iter().map(|child| table.kv_row_key(child, &cond.key)).collect();
            let values = self
                .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
                .values;
            let mut next: Vec<SchemaAlkaneId> = Vec::new();
            for (child, raw) in survivors.iter().zip(values.into_iter()) {
                let Some(raw) = raw else { continue };
                let (last_txid, value_bytes) = split_txid_value(&raw);
                if !cond.matches(value_bytes) {
                    continue;
                }
                let key_hex = fmt_bytes_hex(&cond.key);
                let key_str_val = utf8_or_null(&cond.key);
                let top_key = if try_decode_utf8 {
                    if let Value::String(s) = &key_str_val { s.clone() } else { key_hex.clone() }
                } else {
                    key_hex.clone()
                };
                matched_values.entry(*child).or_default().insert(
                    top_key,
                    json!({
                        "key_hex":    key_hex,
                        "key_str":    key_str_val,
                        "value_hex":  fmt_bytes_hex(value_bytes),
                        "value_str":  utf8_or_null(value_bytes),
                        "value_u128": u128_le_or_null(value_bytes),
                        "last_txid":  last_txid.map(Value::String).unwrap_or(Value::Null),
                    }),
                );
                next.push(*child);
            }
            survivors = next;
        }

        let total = survivors.len();
        let offset = limit.saturating_mul(page.saturating_sub(1));
        let end = (offset + limit).min(total);
        let window = if offset >= total { &[][..] } else { &survivors[offset..end] };
        let has_more = end < total;

        let items: Vec<Value> = window
            .iter()
            .map(|child| {
                json!({
                    "alkane": format!("{}:{}", child.block, child.tx),
                    "keys": Value::Object(matched_values.remove(child).unwrap_or_default()),
                })
            })
            .collect();

        Ok(RpcSearchFactoryKeysResult {
            value: json!({
                "ok": true,
                "factory": format!("{}:{}", factory.block, factory.tx),
                "conditions": conditions.iter().map(KvSearchCondition::describe).collect::<Vec<_>>(),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items,
            }),
        })
    }

    pub fn rpc_get_all_alkanes(
        &self,
        params: RpcGetAllAlkanesParams,
    ) -> Result<RpcGetAllAlkanesResult> {
        let page = params.page.unwrap_or(1).max(1) as usize;
        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let offset = limit.saturating_mul(page.saturating_sub(1));

        let table = self.table();
        let total = self
            .get_creation_count(GetCreationCountParams { blockhash: StateAt::Latest })
            .map(|r| r.count)
            .unwrap_or(0);

        let mut items: Vec<Value> = Vec::new();
        let records = self
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: offset as u64,
                limit: limit as u64,
                desc: true,
            })
            .map(|resp| resp.records)
            .unwrap_or_default();
        for rec in records {
            let holder_count = self
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.holders_count_key(&rec.alkane),
                })
                .ok()
                .and_then(|resp| resp.value)
                .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
                .map(|hc| hc.count)
                .unwrap_or(0);
            let inspection_json = rec.inspection.as_ref().map(inspection_to_json);
            let (name, symbol) = display_alkane_name_and_symbol(&rec.names, &rec.symbols);
            items.push(json!({
                "alkane": format!("{}:{}", rec.alkane.block, rec.alkane.tx),
                "creation_txid": hex::encode(rec.txid),
                "creation_height": rec.creation_height,
                "creation_timestamp": rec.creation_timestamp,
                "tx_index_in_block": rec.tx_index_in_block,
                "name": name,
                "symbol": symbol,
                "names": rec.names,
                "symbols": rec.symbols,
                "holder_count": holder_count,
                "inspection": inspection_json,
            }));
        }

        Ok(RpcGetAllAlkanesResult {
            value: json!({
                "ok": true,
                "page": page,
                "limit": limit,
                "total": total,
                "items": items,
            }),
        })
    }

    pub fn rpc_search_alkane(
        &self,
        params: RpcSearchAlkaneParams,
    ) -> Result<RpcSearchAlkaneResult> {
        let Some(prefix) = params.prefix.as_deref().and_then(normalize_alkane_name) else {
            return Ok(RpcSearchAlkaneResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_prefix"
                }),
            });
        };
        let limit = params.limit.unwrap_or(20).clamp(1, 100) as usize;
        let candidate_limit = limit.saturating_mul(4).min(400) as u64;

        let name_ids = self
            .get_alkane_ids_by_name_prefix_page(GetAlkaneIdsByNamePrefixPageParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
                offset: 0,
                limit: candidate_limit,
            })?
            .ids;
        let symbol_ids = self
            .get_alkane_ids_by_symbol_prefix_page(GetAlkaneIdsBySymbolPrefixPageParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
                offset: 0,
                limit: candidate_limit,
            })?
            .ids;

        let mut seen = HashSet::new();
        let ids = name_ids
            .into_iter()
            .chain(symbol_ids)
            .filter(|id| seen.insert(*id))
            .collect::<Vec<_>>();
        let records = self
            .get_creation_records_by_id(GetCreationRecordsByIdParams {
                blockhash: StateAt::Latest,
                alkanes: ids.clone(),
            })?
            .records;
        let holder_counts = self
            .get_holders_counts_by_id(GetHoldersCountsByIdParams {
                blockhash: StateAt::Latest,
                alkanes: ids.clone(),
            })?
            .counts;

        let mut matches = ids
            .into_iter()
            .zip(records)
            .zip(holder_counts)
            .filter_map(|((alkane, record), holder_count)| {
                let record = record?;
                let (name, symbol) = display_alkane_name_and_symbol(&record.names, &record.symbols);
                let exact = name
                    .as_deref()
                    .and_then(normalize_alkane_name)
                    .is_some_and(|value| value == prefix)
                    || symbol
                        .as_deref()
                        .and_then(normalize_alkane_name)
                        .is_some_and(|value| value == prefix);
                Some((
                    !exact,
                    std::cmp::Reverse(holder_count),
                    name.clone().unwrap_or_default().to_ascii_lowercase(),
                    alkane,
                    name,
                    symbol,
                    record.creation_height,
                ))
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| left.3.block.cmp(&right.3.block))
                .then_with(|| left.3.tx.cmp(&right.3.tx))
        });
        matches.truncate(limit);

        let items = matches
            .into_iter()
            .map(|(_, holder_count, _, alkane, name, symbol, creation_height)| {
                json!({
                    "alkane": format!("{}:{}", alkane.block, alkane.tx),
                    "name": name,
                    "symbol": symbol,
                    "holder_count": holder_count.0,
                    "creation_height": creation_height,
                })
            })
            .collect::<Vec<_>>();

        Ok(RpcSearchAlkaneResult {
            value: json!({
                "ok": true,
                "prefix": prefix,
                "limit": limit,
                "items": items,
            }),
        })
    }

    pub fn rpc_get_alkane_info(
        &self,
        params: RpcGetAlkaneInfoParams,
    ) -> Result<RpcGetAlkaneInfoResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetAlkaneInfoResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetAlkaneInfoResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };

        let record = match self.get_creation_record(GetCreationRecordParams {
            blockhash: StateAt::Latest,
            alkane: alk,
        }) {
            Ok(resp) => match resp.record {
                Some(r) => r,
                None => {
                    return Ok(RpcGetAlkaneInfoResult {
                        value: json!({"ok": false, "error": "not_found"}),
                    });
                }
            },
            Err(_) => {
                return Ok(RpcGetAlkaneInfoResult {
                    value: json!({"ok": false, "error": "lookup_failed"}),
                });
            }
        };

        let table = self.table();
        let holder_count = get_holders_for_alkane(StateAt::Latest, self, alk, 1, 1)
            .map(|(total, _, _)| total as u64)
            .unwrap_or_else(|_| {
                self.get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.holders_count_key(&alk),
                })
                .ok()
                .and_then(|resp| resp.value)
                .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
                .map(|hc| hc.count)
                .unwrap_or(0)
            });
        let inspection_json = record.inspection.as_ref().map(inspection_to_json);
        let (name, symbol) = display_alkane_name_and_symbol(&record.names, &record.symbols);

        Ok(RpcGetAlkaneInfoResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", record.alkane.block, record.alkane.tx),
                "creation_txid": hex::encode(record.txid),
                "creation_height": record.creation_height,
                "creation_timestamp": record.creation_timestamp,
                "tx_index_in_block": record.tx_index_in_block,
                "name": name,
                "symbol": symbol,
                "names": record.names,
                "symbols": record.symbols,
                "holder_count": holder_count,
                "inspection": inspection_json,
            }),
        })
    }

    pub fn rpc_get_alkabi(&self, params: RpcGetAlkabiParams) -> Result<RpcGetAlkabiResult> {
        let Some(raw_alkane) = params.alkane.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAlkabiResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let Some(alkane) = crate::utils::parse_alkane_id(raw_alkane) else {
            return Ok(RpcGetAlkabiResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let Some(format) = crate::modules::essentials::utils::alkabi::AlkabiFormat::parse(
            params.format.as_deref(),
        ) else {
            return Ok(RpcGetAlkabiResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_format",
                    "hint": "provide format as \"json\" or \"ts\""
                }),
            });
        };

        let abi =
            match crate::modules::essentials::utils::alkabi::extract_contract_alkabi(self, &alkane)
            {
                Ok(abi) => abi,
                Err(error) => {
                    return Ok(RpcGetAlkabiResult {
                        value: json!({
                            "ok": false,
                            "error": "alkabi_generation_failed",
                            "detail": error.to_string()
                        }),
                    });
                }
            };
        let rendered = match format.render(&abi) {
            Ok(value) => value,
            Err(error) => {
                return Ok(RpcGetAlkabiResult {
                    value: json!({
                        "ok": false,
                        "error": "alkabi_generation_failed",
                        "detail": error.to_string()
                    }),
                });
            }
        };

        Ok(RpcGetAlkabiResult {
            value: json!({
                "ok": true,
                "alkane": alkane.to_string(),
                "format": format.as_str(),
                "abi": rendered
            }),
        })
    }

    /// RPC equivalent of the explorer's `/api/alkane/wasm/export` download:
    /// resolves the alkane's bytecode source (following proxy/factory
    /// indirection) and returns the wasm as base64, plus content metadata
    /// (`sha256` of the raw bytes, raw `length`, resolved `source`) so a
    /// caching layer can content-address, validate and dedup payloads.
    pub fn rpc_get_alkane_wasm(
        &self,
        params: RpcGetAlkaneWasmParams,
    ) -> Result<RpcGetAlkaneWasmResult> {
        let Some(raw_alkane) = params.alkane.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAlkaneWasmResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let Some(alkane) = crate::utils::parse_alkane_id(raw_alkane) else {
            return Ok(RpcGetAlkaneWasmResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_alkane",
                    "hint": "provide alkane as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let gzip = params.gzip.unwrap_or(false);

        let (wasm, source) =
            match crate::modules::essentials::utils::alkabi::load_contract_wasm_with_source(
                self, &alkane,
            ) {
                Ok(loaded) => loaded,
                Err(error) => {
                    return Ok(RpcGetAlkaneWasmResult {
                        value: json!({
                            "ok": false,
                            "error": "wasm_export_failed",
                            "detail": error.to_string()
                        }),
                    });
                }
            };

        let (encoding, payload_len, wasm_base64) = match encode_wasm_payload(&wasm, gzip) {
            Ok(encoded) => encoded,
            Err(error) => {
                return Ok(RpcGetAlkaneWasmResult {
                    value: json!({
                        "ok": false,
                        "error": "wasm_export_failed",
                        "detail": error.to_string()
                    }),
                });
            }
        };

        Ok(RpcGetAlkaneWasmResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alkane.block, alkane.tx),
                "source": format!("{}:{}", source.block, source.tx),
                "length": wasm.len(),
                "sha256": wasm_sha256_hex(&wasm),
                "encoding": encoding,
                "payload_length": payload_len,
                "wasm_base64": wasm_base64,
            }),
        })
    }

    pub fn rpc_get_factory_children(
        &self,
        params: RpcGetFactoryChildrenParams,
    ) -> Result<RpcGetFactoryChildrenResult> {
        let Some(factory_raw) = params.factory.as_deref() else {
            return Ok(RpcGetFactoryChildrenResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_factory",
                    "hint": "provide factory as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let Some(factory) = parse_alkane_from_str(factory_raw) else {
            return Ok(RpcGetFactoryChildrenResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_factory",
                    "hint": "provide factory as \"<block>:<tx>\" (hex ok)"
                }),
            });
        };
        let children = self
            .get_factory_children(GetFactoryChildrenParams { blockhash: StateAt::Latest, factory })?
            .children;
        Ok(RpcGetFactoryChildrenResult {
            value: json!({
                "ok": true,
                "factory": format!("{}:{}", factory.block, factory.tx),
                "children": children
                    .iter()
                    .map(|child| format!("{}:{}", child.block, child.tx))
                    .collect::<Vec<_>>(),
            }),
        })
    }

    pub fn rpc_get_block_summary(
        &self,
        params: RpcGetBlockSummaryParams,
    ) -> Result<RpcGetBlockSummaryResult> {
        let Some(height) = params.height else {
            return Ok(RpcGetBlockSummaryResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        };
        let Ok(height) = u32::try_from(height) else {
            return Ok(RpcGetBlockSummaryResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        };
        let summary = self
            .get_block_summary(GetBlockSummaryParams { blockhash: StateAt::Latest, height })
            .ok()
            .and_then(|resp| resp.summary);

        let (
            trace_count,
            interaction_count,
            tx_count,
            header_hex,
            blockhash,
            fee_avg,
            fee_median,
            fee_range,
            pool,
            block_time,
            found,
        ) = if let Some(summary) = summary {
            let blockhash = summary.block_hash().map(|h| h.to_string());
            let block_time = summary.block_time();
            let pool = summary.pool.map(|pool| {
                json!({
                    "id": pool.id,
                    "name": pool.name,
                    "slug": pool.slug,
                    "matched": pool.matched,
                    "link": pool.link,
                    "mempool_url": pool.mempool_url,
                    "icon_url": pool.icon_url,
                })
            });
            (
                summary.trace_count,
                summary.interaction_count,
                summary.tx_count,
                Some(hex::encode(summary.header)),
                blockhash,
                summary.fee_avg,
                summary.fee_median,
                summary.fee_range,
                pool,
                block_time,
                true,
            )
        } else {
            (0, 0, 0, None, None, 0.0, 0.0, Vec::new(), None, None, false)
        };

        Ok(RpcGetBlockSummaryResult {
            value: json!({
                "ok": true,
                "height": height,
                "found": found,
                "trace_count": trace_count,
                "interaction_count": interaction_count,
                "tx_count": tx_count,
                "blockhash": blockhash,
                "header_hex": header_hex,
                "fee_avg": fee_avg,
                "fee_median": fee_median,
                "fee_range": fee_range,
                "pool": pool,
                "block_time": block_time,
            }),
        })
    }

    pub fn rpc_get_block_time(
        &self,
        params: RpcGetBlockTimeParams,
    ) -> Result<RpcGetBlockTimeResult> {
        let Some(height) = params.height.and_then(|height| u32::try_from(height).ok()) else {
            return Ok(RpcGetBlockTimeResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        };
        let summary = self.get_block_summaries_by_heights(&[height])?.into_iter().next().flatten();
        let found = summary.is_some();
        let block_time = summary.as_ref().and_then(BlockSummary::block_time);

        Ok(RpcGetBlockTimeResult {
            value: json!({
                "ok": true,
                "height": height,
                "found": found,
                "block_time": block_time,
            }),
        })
    }

    pub fn rpc_get_block_times(
        &self,
        params: RpcGetBlockTimesParams,
    ) -> Result<RpcGetBlockTimesResult> {
        let Some(raw_heights) = params.heights else {
            return Ok(RpcGetBlockTimesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_heights"}),
            });
        };
        if raw_heights.len() > MAX_BLOCK_TIMES_BATCH {
            return Ok(RpcGetBlockTimesResult {
                value: json!({
                    "ok": false,
                    "error": "too_many_heights",
                    "max_heights": MAX_BLOCK_TIMES_BATCH,
                }),
            });
        }
        let heights = match raw_heights
            .iter()
            .copied()
            .map(u32::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(heights) => heights,
            Err(_) => {
                return Ok(RpcGetBlockTimesResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_heights"}),
                });
            }
        };
        let summaries = self.get_block_summaries_by_heights(&heights)?;
        let times = heights
            .into_iter()
            .zip(summaries)
            .map(|(height, summary)| {
                let found = summary.is_some();
                let block_time = summary.as_ref().and_then(BlockSummary::block_time);
                json!({
                    "height": height,
                    "found": found,
                    "block_time": block_time,
                })
            })
            .collect::<Vec<_>>();

        Ok(RpcGetBlockTimesResult {
            value: json!({
                "ok": true,
                "times": times,
            }),
        })
    }

    pub fn rpc_get_holders(&self, params: RpcGetHoldersParams) -> Result<RpcGetHoldersResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetHoldersResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetHoldersResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, _supply, slice) =
            match get_holders_for_alkane(StateAt::Latest, self, alk, page, limit) {
                Ok(tup) => tup,
                Err(_) => {
                    return Ok(RpcGetHoldersResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|h| match h.holder {
                HolderId::Address(addr) => json!({
                    "type": "address",
                    "address": addr,
                    "amount": h.amount.to_string()
                }),
                HolderId::Alkane(id) => json!({
                    "type": "alkane",
                    "alkane": format!("{}:{}", id.block, id.tx),
                    "amount": h.amount.to_string()
                }),
            })
            .collect();

        Ok(RpcGetHoldersResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alk.block, alk.tx),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    pub fn rpc_get_orbital_holders(
        &self,
        params: RpcGetOrbitalHoldersParams,
    ) -> Result<RpcGetOrbitalHoldersResult> {
        let Some(factory_raw) = params.factory.as_deref() else {
            return Ok(RpcGetOrbitalHoldersResult {
                value: json!({"ok": false, "error": "missing_or_invalid_factory"}),
            });
        };
        let Some(factory) = parse_alkane_from_str(factory_raw) else {
            return Ok(RpcGetOrbitalHoldersResult {
                value: json!({"ok": false, "error": "missing_or_invalid_factory"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, _supply, slice) =
            match get_orbital_holders_for_factory(StateAt::Latest, self, factory, page, limit) {
                Ok(tup) => tup,
                Err(_) => {
                    return Ok(RpcGetOrbitalHoldersResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|h| {
                let alkanes: Vec<Value> = h
                    .alkanes
                    .into_iter()
                    .map(|id| Value::String(format!("{}:{}", id.block, id.tx)))
                    .collect();
                match h.holder {
                    HolderId::Address(addr) => json!({
                        "type": "address",
                        "address": addr,
                        "amount": h.amount.to_string(),
                        "alkanes": alkanes
                    }),
                    HolderId::Alkane(id) => json!({
                        "type": "alkane",
                        "alkane": format!("{}:{}", id.block, id.tx),
                        "amount": h.amount.to_string(),
                        "alkanes": alkanes
                    }),
                }
            })
            .collect();

        Ok(RpcGetOrbitalHoldersResult {
            value: json!({
                "ok": true,
                "factory": format!("{}:{}", factory.block, factory.tx),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    fn rpc_get_orbital_volumes(
        &self,
        params: RpcGetOrbitalVolumesParams,
        receive: bool,
    ) -> Result<RpcGetOrbitalVolumesResult> {
        let Some(factory_raw) = params.factory.as_deref() else {
            return Ok(RpcGetOrbitalVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_factory"}),
            });
        };
        let Some(factory) = parse_alkane_from_str(factory_raw) else {
            return Ok(RpcGetOrbitalVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_factory"}),
            });
        };
        let Some(alkane_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetOrbitalVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alkane) = parse_alkane_from_str(alkane_raw) else {
            return Ok(RpcGetOrbitalVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, slice) = match get_orbital_volume_for_factory(
            StateAt::Latest,
            self,
            factory,
            alkane,
            receive,
            page,
            limit,
        ) {
            Ok(tup) => tup,
            Err(_) => {
                return Ok(RpcGetOrbitalVolumesResult {
                    value: json!({"ok": false, "error": "internal_error"}),
                });
            }
        };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|entry| {
                json!({
                    "address": entry.address,
                    "amount": entry.amount.to_string()
                })
            })
            .collect();

        Ok(RpcGetOrbitalVolumesResult {
            value: json!({
                "ok": true,
                "factory": format!("{}:{}", factory.block, factory.tx),
                "alkane": format!("{}:{}", alkane.block, alkane.tx),
                "kind": if receive { "receive" } else { "send" },
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    pub fn rpc_get_orbital_send_volumes(
        &self,
        params: RpcGetOrbitalVolumesParams,
    ) -> Result<RpcGetOrbitalVolumesResult> {
        self.rpc_get_orbital_volumes(params, false)
    }

    pub fn rpc_get_orbital_receive_volumes(
        &self,
        params: RpcGetOrbitalVolumesParams,
    ) -> Result<RpcGetOrbitalVolumesResult> {
        self.rpc_get_orbital_volumes(params, true)
    }

    fn rpc_get_alkane_volumes(
        &self,
        params: RpcGetAlkaneVolumesParams,
        receive: bool,
    ) -> Result<RpcGetAlkaneVolumesResult> {
        let Some(source_raw) = params.source_alkane.as_deref() else {
            return Ok(RpcGetAlkaneVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_source_alkane"}),
            });
        };
        let Some(source) = parse_alkane_from_str(source_raw) else {
            return Ok(RpcGetAlkaneVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_source_alkane"}),
            });
        };
        let Some(alkane_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetAlkaneVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alkane) = parse_alkane_from_str(alkane_raw) else {
            return Ok(RpcGetAlkaneVolumesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, slice) = match get_alkane_volume_for_source(
            StateAt::Latest,
            self,
            source,
            alkane,
            receive,
            page,
            limit,
        ) {
            Ok(tup) => tup,
            Err(_) => {
                return Ok(RpcGetAlkaneVolumesResult {
                    value: json!({"ok": false, "error": "internal_error"}),
                });
            }
        };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|entry| {
                json!({
                    "address": entry.address,
                    "amount": entry.amount.to_string()
                })
            })
            .collect();

        Ok(RpcGetAlkaneVolumesResult {
            value: json!({
                "ok": true,
                "source_alkane": format!("{}:{}", source.block, source.tx),
                "alkane": format!("{}:{}", alkane.block, alkane.tx),
                "kind": if receive { "receive" } else { "send" },
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    pub fn rpc_get_alkane_send_volumes(
        &self,
        params: RpcGetAlkaneVolumesParams,
    ) -> Result<RpcGetAlkaneVolumesResult> {
        self.rpc_get_alkane_volumes(params, false)
    }

    pub fn rpc_get_alkane_receive_volumes(
        &self,
        params: RpcGetAlkaneVolumesParams,
    ) -> Result<RpcGetAlkaneVolumesResult> {
        self.rpc_get_alkane_volumes(params, true)
    }

    pub fn rpc_get_transfer_volume(
        &self,
        params: RpcGetTransferVolumeParams,
    ) -> Result<RpcGetTransferVolumeResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetTransferVolumeResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetTransferVolumeResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, slice) =
            match get_transfer_volume_for_alkane(StateAt::Latest, self, alk, page, limit) {
                Ok(tup) => tup,
                Err(_) => {
                    return Ok(RpcGetTransferVolumeResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|entry| {
                json!({
                    "address": entry.address,
                    "amount": entry.amount.to_string()
                })
            })
            .collect();

        Ok(RpcGetTransferVolumeResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alk.block, alk.tx),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    pub fn rpc_get_total_received(
        &self,
        params: RpcGetTotalReceivedParams,
    ) -> Result<RpcGetTotalReceivedResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetTotalReceivedResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetTotalReceivedResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;

        let (total, slice) =
            match get_total_received_for_alkane(StateAt::Latest, self, alk, page, limit) {
                Ok(tup) => tup,
                Err(_) => {
                    return Ok(RpcGetTotalReceivedResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            };

        let has_more = page.saturating_mul(limit) < total;
        let items: Vec<Value> = slice
            .into_iter()
            .map(|entry| {
                json!({
                    "address": entry.address,
                    "amount": entry.amount.to_string()
                })
            })
            .collect();

        Ok(RpcGetTotalReceivedResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alk.block, alk.tx),
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "items": items
            }),
        })
    }

    pub fn rpc_get_circulating_supply(
        &self,
        params: RpcGetCirculatingSupplyParams,
    ) -> Result<RpcGetCirculatingSupplyResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetCirculatingSupplyResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alkane) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetCirculatingSupplyResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        if params.height_present && params.height.is_none() {
            return Ok(RpcGetCirculatingSupplyResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        }

        let (supply, height_value) = if params.height_present {
            let height_val = params.height.unwrap();
            let height_u32 = match u32::try_from(height_val) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(RpcGetCirculatingSupplyResult {
                        value: json!({"ok": false, "error": "height_out_of_range"}),
                    });
                }
            };
            let supply = self
                .get_circulating_supply(GetCirculatingSupplyParams {
                    blockhash: StateAt::Latest,
                    alkane,
                    height: height_u32,
                })?
                .supply;
            (supply, json!(height_val))
        } else {
            let supply = self
                .get_latest_circulating_supply(GetLatestCirculatingSupplyParams {
                    blockhash: StateAt::Latest,
                    alkane,
                })?
                .supply;
            (supply, Value::String("latest".to_string()))
        };

        let mut body = Map::new();
        body.insert("ok".to_string(), Value::Bool(true));
        body.insert("alkane".to_string(), Value::String(format!("{}:{}", alkane.block, alkane.tx)));
        body.insert("supply".to_string(), Value::String(supply.to_string()));
        body.insert("height".to_string(), height_value);

        Ok(RpcGetCirculatingSupplyResult { value: Value::Object(body) })
    }

    pub fn rpc_get_address_activity(
        &self,
        params: RpcGetAddressActivityParams,
    ) -> Result<RpcGetAddressActivityResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressActivityResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressActivityResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let activity = match get_address_activity_for_address(StateAt::Latest, self, &address) {
            Ok(entry) => entry,
            Err(_) => {
                return Ok(RpcGetAddressActivityResult {
                    value: json!({"ok": false, "error": "internal_error"}),
                });
            }
        };

        let mut transfer_volume: Map<String, Value> = Map::new();
        for (alk, amt) in activity.transfer_volume {
            transfer_volume
                .insert(format!("{}:{}", alk.block, alk.tx), Value::String(amt.to_string()));
        }
        let mut total_received: Map<String, Value> = Map::new();
        for (alk, amt) in activity.total_received {
            total_received
                .insert(format!("{}:{}", alk.block, alk.tx), Value::String(amt.to_string()));
        }

        Ok(RpcGetAddressActivityResult {
            value: json!({
                "ok": true,
                "address": address,
                "transfer_volume": Value::Object(transfer_volume),
                "total_received": Value::Object(total_received),
            }),
        })
    }

    fn rpc_get_address_cumulative_alkanes(
        &self,
        params: RpcGetAddressCumulativeAlkanesParams,
        receive: bool,
        orbital: bool,
    ) -> Result<RpcGetAddressCumulativeAlkanesResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressCumulativeAlkanesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressCumulativeAlkanesResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let key = if orbital && receive {
            self.table().address_cumulative_receive_orbitals_key(&address)
        } else if orbital {
            self.table().address_cumulative_send_orbitals_key(&address)
        } else if receive {
            self.table().address_cumulative_receive_alkanes_key(&address)
        } else {
            self.table().address_cumulative_send_alkanes_key(&address)
        };
        let entries = match self
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
        {
            Some(raw) => decode_address_contract_amount_entries(&raw).unwrap_or_default(),
            None => Vec::new(),
        };

        let items: Vec<Value> = entries
            .into_iter()
            .filter(|entry| entry.amount > 0)
            .map(|entry| {
                let mut item = Map::new();
                item.insert(
                    if orbital { "orbital" } else { "source_alkane" }.to_string(),
                    Value::String(format!("{}:{}", entry.contract.block, entry.contract.tx)),
                );
                item.insert(
                    "alkane".to_string(),
                    Value::String(format!("{}:{}", entry.alkane.block, entry.alkane.tx)),
                );
                item.insert("amount".to_string(), Value::String(entry.amount.to_string()));
                Value::Object(item)
            })
            .collect();

        Ok(RpcGetAddressCumulativeAlkanesResult {
            value: json!({
                "ok": true,
                "address": address,
                "kind": if receive { "receive" } else { "send" },
                "items": items
            }),
        })
    }

    pub fn rpc_address_cumulative_send_alkanes(
        &self,
        params: RpcGetAddressCumulativeAlkanesParams,
    ) -> Result<RpcGetAddressCumulativeAlkanesResult> {
        self.rpc_get_address_cumulative_alkanes(params, false, false)
    }

    pub fn rpc_address_cumulative_receive_alkanes(
        &self,
        params: RpcGetAddressCumulativeAlkanesParams,
    ) -> Result<RpcGetAddressCumulativeAlkanesResult> {
        self.rpc_get_address_cumulative_alkanes(params, true, false)
    }

    pub fn rpc_address_cumulative_send_orbitals(
        &self,
        params: RpcGetAddressCumulativeAlkanesParams,
    ) -> Result<RpcGetAddressCumulativeAlkanesResult> {
        self.rpc_get_address_cumulative_alkanes(params, false, true)
    }

    pub fn rpc_address_cumulative_receive_orbitals(
        &self,
        params: RpcGetAddressCumulativeAlkanesParams,
    ) -> Result<RpcGetAddressCumulativeAlkanesResult> {
        self.rpc_get_address_cumulative_alkanes(params, true, true)
    }

    pub fn rpc_get_orbital_balances(
        &self,
        params: RpcGetOrbitalBalancesParams,
    ) -> Result<RpcGetOrbitalBalancesResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetOrbitalBalancesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetOrbitalBalancesResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let key = self.table().address_orbital_balances_v2_key(&address);
        let entries = match self
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
        {
            Some(raw) => decode_address_orbital_balance_entries(&raw).unwrap_or_default(),
            None => Vec::new(),
        };

        let mut balances: Map<String, Value> = Map::new();
        for entry in entries {
            if entry.amount == 0 {
                continue;
            }
            let alkanes: Vec<Value> = entry
                .alkanes
                .into_iter()
                .map(|id| Value::String(format!("{}:{}", id.block, id.tx)))
                .collect();
            balances.insert(
                format!("{}:{}", entry.factory.block, entry.factory.tx),
                json!({
                    "amount": entry.amount.to_string(),
                    "alkanes": alkanes
                }),
            );
        }

        Ok(RpcGetOrbitalBalancesResult {
            value: json!({
                "ok": true,
                "address": address,
                "balances": Value::Object(balances)
            }),
        })
    }

    pub fn rpc_get_address_balances(
        &self,
        params: RpcGetAddressBalancesParams,
    ) -> Result<RpcGetAddressBalancesResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressBalancesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressBalancesResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let include_outpoints = params.include_outpoints.unwrap_or(false);

        let agg = match get_balance_for_address(StateAt::Latest, self, &address) {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcGetAddressBalancesResult {
                    value: json!({"ok": false, "error": "internal_error"}),
                });
            }
        };

        let mut balances: Map<String, Value> = Map::new();
        for (id, amt) in agg {
            balances.insert(format!("{}:{}", id.block, id.tx), Value::String(amt.to_string()));
        }

        let mut resp = json!({
            "ok": true,
            "address": address,
            "balances": Value::Object(balances),
        });

        if include_outpoints {
            let address = resp["address"].as_str().unwrap_or_default();
            let outpoint_len = get_address_index_list_len(
                self,
                StateAt::Latest,
                AddressIndexListKind::OutpointIdx,
                address,
            )
            .unwrap_or(0) as usize;

            let mut outpoints = Vec::new();
            if outpoint_len > 0 {
                let ids = get_address_index_list_range(
                    self,
                    StateAt::Latest,
                    AddressIndexListKind::OutpointIdx,
                    address,
                    0,
                    outpoint_len as u64,
                )
                .unwrap_or_default();
                for id in ids {
                    let Some(blob) = load_outpoint_pointer_blob_v3_by_id(self, id) else {
                        continue;
                    };
                    if resolve_outpoint_spent_by_id_v2(self, StateAt::Latest, id)
                        .ok()
                        .flatten()
                        .is_some()
                    {
                        continue;
                    }
                    let txid = Txid::from_byte_array(blob.txid);
                    let entries = blob.balances;
                    let entry_list: Vec<Value> = entries
                        .into_iter()
                        .map(|be| {
                            json!({
                                "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                                "amount": be.amount.to_string()
                            })
                        })
                        .collect();
                    outpoints.push(json!({
                        "outpoint": format!("{}:{}", txid, blob.vout),
                        "entries": entry_list
                    }));
                }
            }

            outpoints.sort_by(|a, b| {
                let sa = a.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
                let sb = b.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
                sa.cmp(sb)
            });
            outpoints.dedup_by(|a, b| {
                a.get("outpoint").and_then(|v| v.as_str())
                    == b.get("outpoint").and_then(|v| v.as_str())
            });

            resp.as_object_mut()
                .unwrap()
                .insert("outpoints".to_string(), Value::Array(outpoints));
        }

        Ok(RpcGetAddressBalancesResult { value: resp })
    }

    pub fn rpc_get_alkane_balances(
        &self,
        params: RpcGetAlkaneBalancesParams,
    ) -> Result<RpcGetAlkaneBalancesResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetAlkaneBalancesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetAlkaneBalancesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        if params.height_present && params.height.is_none() {
            return Ok(RpcGetAlkaneBalancesResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        }

        let mut resolved_height: Option<u32> = None;
        let agg = if params.height_present {
            let height_val = params.height.unwrap();
            let height_u32 = match u32::try_from(height_val) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(RpcGetAlkaneBalancesResult {
                        value: json!({"ok": false, "error": "height_out_of_range"}),
                    });
                }
            };
            match get_alkane_balances_at_or_before(StateAt::Latest, self, &alk, height_u32) {
                Ok((m, found_height)) => {
                    resolved_height = found_height;
                    m
                }
                Err(_) => {
                    return Ok(RpcGetAlkaneBalancesResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            }
        } else {
            match get_alkane_balances(StateAt::Latest, self, &alk) {
                Ok(m) => m,
                Err(_) => {
                    return Ok(RpcGetAlkaneBalancesResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            }
        };

        let mut balances: Map<String, Value> = Map::new();
        for (id, amt) in agg {
            balances.insert(format!("{}:{}", id.block, id.tx), Value::String(amt.to_string()));
        }

        let mut body = Map::new();
        body.insert("ok".to_string(), Value::Bool(true));
        body.insert("alkane".to_string(), Value::String(format!("{}:{}", alk.block, alk.tx)));
        body.insert("balances".to_string(), Value::Object(balances));
        if params.height_present {
            body.insert("requested_height".to_string(), json!(params.height.unwrap()));
            body.insert(
                "resolved_height".to_string(),
                resolved_height.map(|h| json!(h)).unwrap_or(Value::Null),
            );
        }

        Ok(RpcGetAlkaneBalancesResult { value: Value::Object(body) })
    }

    pub fn rpc_get_alkane_balance_metashrew(
        &self,
        params: RpcGetAlkaneBalanceMetashrewParams,
    ) -> Result<RpcGetAlkaneBalanceMetashrewResult> {
        let Some(owner_raw) = params.owner.as_deref() else {
            return Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "missing_or_invalid_owner"}),
            });
        };
        let Some(owner) = parse_alkane_from_str(owner_raw) else {
            return Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "missing_or_invalid_owner"}),
            });
        };

        let Some(target_raw) = params.target.as_deref() else {
            return Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "missing_or_invalid_target"}),
            });
        };
        let Some(target) = parse_alkane_from_str(target_raw) else {
            return Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "missing_or_invalid_target"}),
            });
        };

        if params.height_present && params.height.is_none() {
            return Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        }

        match get_metashrew().get_reserves_for_alkane(&owner, &target, params.height) {
            Ok(Some(bal)) => Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({
                    "ok": true,
                    "owner": format!("{}:{}", owner.block, owner.tx),
                    "alkane": format!("{}:{}", target.block, target.tx),
                    "balance": bal.to_string(),
                }),
            }),
            Ok(None) => Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({
                    "ok": true,
                    "owner": format!("{}:{}", owner.block, owner.tx),
                    "alkane": format!("{}:{}", target.block, target.tx),
                    "balance": "0",
                }),
            }),
            Err(_) => Ok(RpcGetAlkaneBalanceMetashrewResult {
                value: json!({"ok": false, "error": "metashrew_error"}),
            }),
        }
    }

    pub fn rpc_get_alkane_balance_txs(
        &self,
        params: RpcGetAlkaneBalanceTxsParams,
    ) -> Result<RpcGetAlkaneBalanceTxsResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetAlkaneBalanceTxsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alk) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetAlkaneBalanceTxsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;
        let table = self.table();
        let prefix = table.alkane_balance_txs_log_prefix(&alk);

        let cursor_bytes = match params.cursor.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => match hex::decode(s) {
                Ok(v) => {
                    if !v.starts_with(&prefix) {
                        return Ok(RpcGetAlkaneBalanceTxsResult {
                            value: json!({"ok": false, "error": "invalid_cursor"}),
                        });
                    }
                    Some(v)
                }
                Err(_) => {
                    return Ok(RpcGetAlkaneBalanceTxsResult {
                        value: json!({"ok": false, "error": "invalid_cursor"}),
                    });
                }
            },
            None => None,
        };

        let (keys_desc, total_opt, has_more, next_cursor_raw) = if cursor_bytes.is_some() {
            let page_data = self.get_list_entries_desc_cursor(GetListEntriesDescCursorParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
                cursor: cursor_bytes.clone(),
                limit,
            })?;
            let keys = page_data.entries.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
            (keys, None, page_data.has_more, page_data.next_cursor)
        } else {
            let mut all_keys = self
                .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                    blockhash: StateAt::Latest,
                    prefix: prefix.clone(),
                })
                .map(|v| v.keys)
                .unwrap_or_default();
            all_keys.sort();
            all_keys.reverse();
            let total = all_keys.len();
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(total);
            let slice = if off >= total { Vec::new() } else { all_keys[off..end].to_vec() };
            let has_more = end < total;
            let next_cursor =
                if has_more && !slice.is_empty() { slice.last().cloned() } else { None };
            (slice, Some(total), has_more, next_cursor)
        };

        let mut items: Vec<Value> = Vec::with_capacity(keys_desc.len());
        for key in keys_desc {
            let Some((height_from_key, _tx_idx, entry_id)) =
                table.parse_alkane_balance_txs_log_key(&alk, &key)
            else {
                continue;
            };
            let Some(blob) = load_tx_pointer_blob_v3_by_id(self, entry_id) else {
                continue;
            };
            let txid = Txid::from_byte_array(blob.txid);
            let height = blob.height.max(height_from_key);
            let owner_map = blob.outflows.get(&alk).cloned().unwrap_or_else(BTreeMap::new);

            let mut outflow: Map<String, Value> = Map::new();
            for (id, delta) in owner_map {
                outflow.insert(format!("{}:{}", id.block, id.tx), Value::String(delta.to_string()));
            }
            items.push(json!({
                "txid": txid.to_string(),
                "height": height,
                "outflow": Value::Object(outflow),
            }));
        }

        let next_cursor = next_cursor_raw.map(hex::encode);
        Ok(RpcGetAlkaneBalanceTxsResult {
            value: json!({
                "ok": true,
                "alkane": format!("{}:{}", alk.block, alk.tx),
                "page": if cursor_bytes.is_some() { Value::Null } else { json!(page) },
                "cursor": params.cursor,
                "next_cursor": next_cursor,
                "limit": limit,
                "total": total_opt.map(Value::from).unwrap_or(Value::Null),
                "has_more": has_more,
                "txids": items
            }),
        })
    }

    pub fn rpc_get_alkane_balance_txs_by_token(
        &self,
        params: RpcGetAlkaneBalanceTxsByTokenParams,
    ) -> Result<RpcGetAlkaneBalanceTxsByTokenResult> {
        let Some(owner_raw) = params.owner.as_deref() else {
            return Ok(RpcGetAlkaneBalanceTxsByTokenResult {
                value: json!({"ok": false, "error": "missing_or_invalid_owner"}),
            });
        };
        let Some(owner) = parse_alkane_from_str(owner_raw) else {
            return Ok(RpcGetAlkaneBalanceTxsByTokenResult {
                value: json!({"ok": false, "error": "missing_or_invalid_owner"}),
            });
        };
        let Some(token_raw) = params.token.as_deref() else {
            return Ok(RpcGetAlkaneBalanceTxsByTokenResult {
                value: json!({"ok": false, "error": "missing_or_invalid_token"}),
            });
        };
        let Some(token) = parse_alkane_from_str(token_raw) else {
            return Ok(RpcGetAlkaneBalanceTxsByTokenResult {
                value: json!({"ok": false, "error": "missing_or_invalid_token"}),
            });
        };

        let limit = params.limit.unwrap_or(100).max(1) as usize;
        let page = params.page.unwrap_or(1).max(1) as usize;
        let list_id = address_index_list_id_alkane_balance_txs_by_token(&owner, &token);
        let total = get_address_index_list_len(
            self,
            StateAt::Latest,
            AddressIndexListKind::AlkaneBalanceTxsByToken,
            &list_id,
        )
        .unwrap_or(0) as usize;
        let cursor_off =
            if let Some(raw) = params.cursor.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                let Some(v) = parse_paged_cursor_u64(raw) else {
                    return Ok(RpcGetAlkaneBalanceTxsByTokenResult {
                        value: json!({"ok": false, "error": "invalid_cursor"}),
                    });
                };
                Some(v as usize)
            } else {
                None
            };
        let off = cursor_off.unwrap_or_else(|| limit.saturating_mul(page.saturating_sub(1)));
        let end = (off + limit).min(total);
        let range_start = total.saturating_sub(end) as u64;
        let range_end = total.saturating_sub(off.min(total)) as u64;
        let ids = if range_end > range_start {
            get_address_index_list_range(
                self,
                StateAt::Latest,
                AddressIndexListKind::AlkaneBalanceTxsByToken,
                &list_id,
                range_start,
                range_end,
            )
            .unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut items: Vec<Value> = Vec::with_capacity(ids.len());
        for entry_id in ids.into_iter().rev() {
            let Some(blob) = load_tx_pointer_blob_v3_by_id(self, entry_id) else {
                continue;
            };
            let txid = Txid::from_byte_array(blob.txid);
            let mut outflow: Map<String, Value> = Map::new();
            let owner_map = blob.outflows.get(&owner).cloned().unwrap_or_else(BTreeMap::new);
            for (id, delta) in owner_map {
                outflow.insert(format!("{}:{}", id.block, id.tx), Value::String(delta.to_string()));
            }
            items.push(json!({
                "txid": txid.to_string(),
                "height": blob.height,
                "outflow": Value::Object(outflow),
            }));
        }

        let consumed = off.saturating_add(items.len());
        let has_more = consumed < total;
        let next_cursor =
            if has_more { Some(encode_paged_cursor_u64(consumed as u64)) } else { None };
        Ok(RpcGetAlkaneBalanceTxsByTokenResult {
            value: json!({
                "ok": true,
                "owner": format!("{}:{}", owner.block, owner.tx),
                "token": format!("{}:{}", token.block, token.tx),
                "page": if cursor_off.is_some() { Value::Null } else { json!(page) },
                "cursor": params.cursor,
                "next_cursor": next_cursor,
                "limit": limit,
                "total": if cursor_off.is_some() { Value::Null } else { json!(total) },
                "has_more": has_more,
                "txids": items
            }),
        })
    }

    pub fn rpc_get_outpoint_balances(
        &self,
        params: RpcGetOutpointBalancesParams,
    ) -> Result<RpcGetOutpointBalancesResult> {
        let Some(outpoint) = params.outpoint.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetOutpointBalancesResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_outpoint",
                    "hint": "expected \"<txid>:<vout>\""
                }),
            });
        };

        let (txid, vout_u32) = match parse_outpoint_str(outpoint) {
            Ok(tup) => tup,
            Err(err_val) => {
                return Ok(RpcGetOutpointBalancesResult { value: err_val });
            }
        };

        let lookup =
            match crate::modules::essentials::utils::balances::get_outpoint_balances_with_spent(
                StateAt::Latest,
                self,
                &txid,
                vout_u32,
            ) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(RpcGetOutpointBalancesResult {
                        value: json!({"ok": false, "error": "internal_error"}),
                    });
                }
            };
        let addr = get_outpoint_address(StateAt::Latest, self, &txid, vout_u32).ok().flatten();

        let entry_list: Vec<Value> = lookup
            .balances
            .into_iter()
            .map(|be| {
                json!({
                    "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                    "amount": be.amount.to_string()
                })
            })
            .collect();

        let mut item = json!({
            "outpoint": outpoint,
            "entries": entry_list
        });
        if let Some(a) = addr {
            item.as_object_mut().unwrap().insert("address".to_string(), Value::String(a));
        }

        Ok(RpcGetOutpointBalancesResult {
            value: json!({
                "ok": true,
                "outpoint": item["outpoint"],
                "items": [item]
            }),
        })
    }

    pub fn rpc_get_block_traces(
        &self,
        params: RpcGetBlockTracesParams,
    ) -> Result<RpcGetBlockTracesResult> {
        let Some(height) = params.height else {
            return Ok(RpcGetBlockTracesResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_height",
                    "hint": "expected {\"height\": <u64>}"
                }),
            });
        };

        let partials = match get_metashrew().traces_for_block_as_prost(height) {
            Ok(v) => v,
            Err(_) => {
                return Ok(RpcGetBlockTracesResult {
                    value: json!({"ok": false, "error": "metashrew_fetch_failed"}),
                });
            }
        };

        let mut traces: Vec<Value> = Vec::with_capacity(partials.len());
        for p in partials {
            if p.outpoint.len() < 36 {
                continue;
            }
            let (txid_le, vout_le) = p.outpoint.split_at(32);
            let mut txid_be = txid_le.to_vec();
            txid_be.reverse();
            let txid_hex = hex::encode(&txid_be);
            let vout = u32::from_le_bytes(vout_le[..4].try_into().expect("vout 4 bytes"));

            let events_str = match prettyify_protobuf_trace_json(&p.protobuf_trace) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let events: Value = serde_json::from_str(&events_str).unwrap_or(Value::Null);

            traces.push(json!({
                "outpoint": format!("{txid_hex}:{vout}"),
                "events": events
            }));
        }

        Ok(RpcGetBlockTracesResult {
            value: json!({
                "ok": true,
                "height": height,
                "traces": traces
            }),
        })
    }

    pub fn rpc_get_holders_count(
        &self,
        params: RpcGetHoldersCountParams,
    ) -> Result<RpcGetHoldersCountResult> {
        let Some(alk_raw) = params.alkane.as_deref() else {
            return Ok(RpcGetHoldersCountResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };
        let Some(alkane) = parse_alkane_from_str(alk_raw) else {
            return Ok(RpcGetHoldersCountResult {
                value: json!({"ok": false, "error": "missing_or_invalid_alkane"}),
            });
        };

        let table = self.table();
        let count: u64 = match HoldersCountEntry::try_from_slice(
            &self
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: table.holders_count_key(&alkane),
                })
                .ok()
                .and_then(|resp| resp.value)
                .unwrap_or_else(Vec::new),
        ) {
            Ok(count_value) => count_value.count,
            Err(_) => {
                return Ok(RpcGetHoldersCountResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_outpoint"}),
                });
            }
        };

        Ok(RpcGetHoldersCountResult {
            value: json!({
                "ok": true,
                "count": count,
            }),
        })
    }

    pub fn rpc_get_address_outpoints(
        &self,
        params: RpcGetAddressOutpointsParams,
    ) -> Result<RpcGetAddressOutpointsResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressOutpointsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressOutpointsResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let outpoint_len = get_address_index_list_len(
            self,
            StateAt::Latest,
            AddressIndexListKind::OutpointIdx,
            &address,
        )
        .unwrap_or(0) as usize;

        let mut outpoints: Vec<Value> = Vec::new();
        if outpoint_len > 0 {
            let ids = get_address_index_list_range(
                self,
                StateAt::Latest,
                AddressIndexListKind::OutpointIdx,
                &address,
                0,
                outpoint_len as u64,
            )
            .unwrap_or_default();
            for id in ids {
                let Some(blob) = load_outpoint_pointer_blob_v3_by_id(self, id) else {
                    continue;
                };
                if resolve_outpoint_spent_by_id_v2(self, StateAt::Latest, id)
                    .ok()
                    .flatten()
                    .is_some()
                {
                    continue;
                }
                let txid = Txid::from_byte_array(blob.txid);
                let entry_list: Vec<Value> = blob
                    .balances
                    .into_iter()
                    .map(|be| {
                        json!({
                            "alkane": format!("{}:{}", be.alkane.block, be.alkane.tx),
                            "amount": be.amount.to_string()
                        })
                    })
                    .collect();
                outpoints.push(json!({
                    "outpoint": format!("{}:{}", txid, blob.vout),
                    "entries": entry_list
                }));
            }
        }

        outpoints.sort_by(|a, b| {
            let sa = a.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
            let sb = b.get("outpoint").and_then(|v| v.as_str()).unwrap_or_default();
            sa.cmp(sb)
        });
        outpoints.dedup_by(|a, b| {
            a.get("outpoint").and_then(|v| v.as_str()) == b.get("outpoint").and_then(|v| v.as_str())
        });

        Ok(RpcGetAddressOutpointsResult {
            value: json!({
                "ok": true,
                "address": address,
                "outpoints": outpoints
            }),
        })
    }

    pub fn rpc_get_address_spendable_outpoints(
        &self,
        params: RpcGetAddressSpendableOutpointsParams,
    ) -> Result<RpcGetAddressSpendableOutpointsResult> {
        let omit_raw_tx = params.omit_raw_tx.unwrap_or(true);
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressSpendableOutpointsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address_norm) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressSpendableOutpointsResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };
        let network = get_network();
        let address =
            match Address::from_str(&address_norm).and_then(|a| a.require_network(network)) {
                Ok(a) => a,
                Err(_) => {
                    return Ok(RpcGetAddressSpendableOutpointsResult {
                        value: json!({"ok": false, "error": "invalid_address_format"}),
                    });
                }
            };

        let electrum = get_electrum_like();
        if electrum.backend() != ElectrumLikeBackend::EsploraHttp {
            return Ok(RpcGetAddressSpendableOutpointsResult {
                value: json!({
                    "ok": false,
                    "error": "unsupported_backend",
                    "detail": "essentials.get_address_spendable_outpoints requires electrs_esplora_url; electrum_rpc_url is not supported"
                }),
            });
        }

        let utxos = match electrum.address_utxos(&address) {
            Ok(v) => v,
            Err(e) => {
                return Ok(RpcGetAddressSpendableOutpointsResult {
                    value: json!({
                        "ok": false,
                        "error": "electrs_utxo_fetch_failed",
                        "detail": e.to_string()
                    }),
                });
            }
        };

        let tip_height = self
            .get_index_height(GetIndexHeightParams { blockhash: StateAt::Latest })
            .ok()
            .and_then(|r| r.height)
            .or_else(|| electrum.tip_height().ok())
            .unwrap_or(0);

        let mut utxo_by_outpoint: BTreeMap<(Txid, u32), AddressUtxo> = BTreeMap::new();
        for utxo in utxos {
            utxo_by_outpoint.insert((utxo.txid, utxo.vout), utxo);
        }
        let spendable_outpoints: Vec<(Txid, u32)> = utxo_by_outpoint.keys().copied().collect();
        let spendable_outpoint_set: HashSet<(Txid, u32)> =
            spendable_outpoints.iter().copied().collect();

        let mut indexed_balances_by_outpoint: BTreeMap<(Txid, u32), Vec<BalanceEntry>> =
            BTreeMap::new();
        let indexed_len = get_address_index_list_len(
            self,
            StateAt::Latest,
            AddressIndexListKind::OutpointIdx,
            &address_norm,
        )
        .unwrap_or(0);
        if indexed_len > 0 {
            let ids = get_address_index_list_range(
                self,
                StateAt::Latest,
                AddressIndexListKind::OutpointIdx,
                &address_norm,
                0,
                indexed_len,
            )
            .unwrap_or_default();
            for id in ids {
                if resolve_outpoint_spent_by_id_v2(self, StateAt::Latest, id)
                    .ok()
                    .flatten()
                    .is_some()
                {
                    continue;
                }
                let Some(blob) = load_outpoint_pointer_blob_v3_by_id(self, id) else {
                    continue;
                };
                indexed_balances_by_outpoint
                    .insert((Txid::from_byte_array(blob.txid), blob.vout), blob.balances);
            }
        }

        let direct_lookups = match get_outpoint_balances_with_spent_batch(
            StateAt::Latest,
            self,
            &spendable_outpoints,
        ) {
            Ok(v) => v,
            Err(e) => {
                return Ok(RpcGetAddressSpendableOutpointsResult {
                    value: json!({
                        "ok": false,
                        "error": "alkane_outpoint_lookup_failed",
                        "detail": e.to_string()
                    }),
                });
            }
        };

        let mut runes_by_outpoint: HashMap<(Txid, u32), Vec<Value>> = HashMap::new();
        if runes_enabled_from_global_config() && !spendable_outpoint_set.is_empty() {
            let runes_provider = spendable_outpoints_runes_provider();
            match runes_provider.get_address_outpoints(&address_norm) {
                Ok(rows) => {
                    for (txid, vout, row) in rows {
                        if spendable_outpoint_set.contains(&(txid, vout)) {
                            runes_by_outpoint
                                .insert((txid, vout), rune_balances_to_json(&row.balances));
                        }
                    }
                }
                Err(e) => {
                    return Ok(RpcGetAddressSpendableOutpointsResult {
                        value: json!({
                            "ok": false,
                            "error": "rune_outpoint_lookup_failed",
                            "detail": e.to_string()
                        }),
                    });
                }
            }
        }

        let unique_txids: Vec<Txid> = {
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for (txid, _) in &spendable_outpoints {
                if seen.insert(*txid) {
                    out.push(*txid);
                }
            }
            out
        };
        let raw_txs = match electrum.batch_transaction_get_raw(&unique_txids) {
            Ok(v) => v,
            Err(e) => {
                return Ok(RpcGetAddressSpendableOutpointsResult {
                    value: json!({
                        "ok": false,
                        "error": "raw_tx_fetch_failed",
                        "detail": e.to_string()
                    }),
                });
            }
        };
        let mut tx_by_txid: HashMap<Txid, (Transaction, String)> = HashMap::new();
        for (txid, raw) in unique_txids.iter().copied().zip(raw_txs.into_iter()) {
            if raw.is_empty() {
                return Ok(RpcGetAddressSpendableOutpointsResult {
                    value: json!({
                        "ok": false,
                        "error": "raw_tx_fetch_failed",
                        "txid": txid.to_string()
                    }),
                });
            }
            let tx: Transaction = match deserialize(&raw) {
                Ok(tx) => tx,
                Err(e) => {
                    return Ok(RpcGetAddressSpendableOutpointsResult {
                        value: json!({
                            "ok": false,
                            "error": "raw_tx_decode_failed",
                            "txid": txid.to_string(),
                            "detail": e.to_string()
                        }),
                    });
                }
            };
            let raw_hex = if omit_raw_tx { "0".to_string() } else { hex::encode(raw) };
            tx_by_txid.insert(txid, (tx, raw_hex));
        }

        let mut outpoints = Vec::with_capacity(spendable_outpoints.len());
        for (txid, vout) in spendable_outpoints {
            let Some(utxo) = utxo_by_outpoint.get(&(txid, vout)) else {
                continue;
            };
            let Some((tx, raw_hex)) = tx_by_txid.get(&txid) else {
                continue;
            };
            let Some(txout) = tx.output.get(vout as usize) else {
                return Ok(RpcGetAddressSpendableOutpointsResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_vout_in_raw_tx",
                        "outpoint": format!("{}:{}", txid, vout)
                    }),
                });
            };

            let mut balances: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
            if let Some(entries) = indexed_balances_by_outpoint.get(&(txid, vout)) {
                for entry in entries {
                    balances.insert(entry.alkane, entry.amount);
                }
            }
            if let Some(lookup) = direct_lookups.get(&(txid, vout)) {
                for entry in &lookup.balances {
                    balances.insert(entry.alkane, entry.amount);
                }
            }
            let alkanes: Vec<Value> = balances
                .into_iter()
                .filter(|(_, amount)| *amount > 0)
                .map(|(alkane, amount)| {
                    json!({
                        "alkane": format!("{}:{}", alkane.block, alkane.tx),
                        "amount": amount.to_string()
                    })
                })
                .collect();
            let runes = runes_by_outpoint.remove(&(txid, vout)).unwrap_or_default();

            let block_height = utxo.block_height;
            let confirmations = if utxo.confirmed {
                block_height
                    .map(|h| (tip_height as u64).saturating_sub(h).saturating_add(1))
                    .unwrap_or(0)
            } else {
                0
            };
            outpoints.push(json!({
                "outpoint": format!("{}:{}", txid, vout),
                "value": utxo.value,
                "script_pubkey_hex": hex::encode(txout.script_pubkey.as_bytes()),
                "block_height": block_height,
                "confirmations": confirmations,
                "coinbase": tx.is_coinbase(),
                "alkanes": alkanes,
                "runes": runes,
                "raw_tx_hex": raw_hex
            }));
        }

        Ok(RpcGetAddressSpendableOutpointsResult {
            value: json!({
                "ok": true,
                "address": address_norm,
                "height": tip_height,
                "length": outpoints.len(),
                "outpoints": outpoints
            }),
        })
    }

    pub fn rpc_get_alkane_tx_summary(
        &self,
        params: RpcGetAlkaneTxSummaryParams,
    ) -> Result<RpcGetAlkaneTxSummaryResult> {
        let Some(txid_hex) = params.txid.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(RpcGetAlkaneTxSummaryResult {
                value: json!({"ok": false, "error": "missing_or_invalid_txid"}),
            });
        };
        let txid = match Txid::from_str(txid_hex) {
            Ok(t) => t,
            Err(_) => {
                return Ok(RpcGetAlkaneTxSummaryResult {
                    value: json!({"ok": false, "error": "invalid_txid_format"}),
                });
            }
        };

        let Some(summary) = load_tx_summary_v2(self, &txid) else {
            return Ok(RpcGetAlkaneTxSummaryResult {
                value: json!({"ok": false, "error": "not_found"}),
            });
        };

        let traces = summary.traces.clone();
        let traces_json = serde_json::to_value(&traces).unwrap_or(Value::Null);
        let mut outflows_json: Vec<Value> = Vec::new();
        for entry in &summary.outflows {
            let mut outflow_map = Map::new();
            for (alk, delta) in &entry.outflow {
                outflow_map
                    .insert(format!("{}:{}", alk.block, alk.tx), Value::String(delta.to_string()));
            }
            outflows_json.push(json!({
                "txid": Txid::from_byte_array(entry.txid).to_string(),
                "height": entry.height,
                "outflow": outflow_map,
            }));
        }

        Ok(RpcGetAlkaneTxSummaryResult {
            value: json!({
                "ok": true,
                "txid": txid.to_string(),
                "height": summary.height,
                "traces": traces_json,
                "outflows": outflows_json,
            }),
        })
    }

    pub fn rpc_get_alkane_block_txs(
        &self,
        params: RpcGetAlkaneBlockTxsParams,
    ) -> Result<RpcGetAlkaneBlockTxsResult> {
        let Some(height) = params.height else {
            return Ok(RpcGetAlkaneBlockTxsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_height"}),
            });
        };
        let page = params.page.unwrap_or(1).max(1) as usize;
        let limit = params.limit.unwrap_or(50).max(1) as usize;
        let off = limit.saturating_mul(page.saturating_sub(1));
        let list_id = address_index_list_id_alkane_block_txs(height);
        let total = get_address_index_list_len(
            self,
            StateAt::Latest,
            AddressIndexListKind::AlkaneBlockTxs,
            &list_id,
        )
        .unwrap_or(0) as usize;

        if total == 0 {
            return Ok(RpcGetAlkaneBlockTxsResult {
                value: json!({
                    "ok": true,
                    "height": height,
                    "page": page,
                    "limit": limit,
                    "total": 0,
                    "txids": []
                }),
            });
        }

        let end = (off + limit).min(total);
        let mut txids: Vec<String> = Vec::new();
        let ids = if end > off {
            get_address_index_list_range(
                self,
                StateAt::Latest,
                AddressIndexListKind::AlkaneBlockTxs,
                &list_id,
                off as u64,
                end as u64,
            )
            .unwrap_or_default()
        } else {
            Vec::new()
        };
        for id in ids {
            let Some(blob) = load_tx_pointer_blob_v3_by_id(self, id) else {
                continue;
            };
            txids.push(Txid::from_byte_array(blob.txid).to_string());
        }

        Ok(RpcGetAlkaneBlockTxsResult {
            value: json!({
                "ok": true,
                "height": height,
                "page": page,
                "limit": limit,
                "total": total,
                "txids": txids
            }),
        })
    }

    pub fn rpc_get_alkane_address_txs(
        &self,
        params: RpcGetAlkaneAddressTxsParams,
    ) -> Result<RpcGetAlkaneAddressTxsResult> {
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAlkaneAddressTxsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAlkaneAddressTxsResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let page = params.page.unwrap_or(1).max(1) as usize;
        let limit = params.limit.unwrap_or(50).max(1) as usize;
        let off = limit.saturating_mul(page.saturating_sub(1));

        let total = get_address_index_list_len(
            self,
            StateAt::Latest,
            AddressIndexListKind::AlkaneTxs,
            &address,
        )
        .unwrap_or(0) as usize;

        if total == 0 {
            return Ok(RpcGetAlkaneAddressTxsResult {
                value: json!({
                    "ok": true,
                    "address": address,
                    "page": page,
                    "limit": limit,
                    "total": 0,
                    "txids": [],
                    "items": [],
                    "transactions": []
                }),
            });
        }

        let end = (off + limit).min(total);
        let range_start = total.saturating_sub(end) as u64;
        let range_end = total.saturating_sub(off) as u64;
        let ids = get_address_index_list_range(
            self,
            StateAt::Latest,
            AddressIndexListKind::AlkaneTxs,
            &address,
            range_start,
            range_end,
        )
        .unwrap_or_default();

        let mut tx_rows: Vec<(Txid, u32)> = Vec::new();
        for id in ids.into_iter().rev() {
            let Some(blob) = load_tx_pointer_blob_v3_by_id(self, id) else {
                continue;
            };
            tx_rows.push((Txid::from_byte_array(blob.txid), blob.height));
        }

        let txids: Vec<String> = tx_rows.iter().map(|(txid, _)| txid.to_string()).collect();
        let mut items: Vec<Value> = Vec::new();

        if !tx_rows.is_empty() {
            let chain_tip = get_bitcoind_rpc_client()
                .get_blockchain_info()
                .ok()
                .map(|info| info.blocks as u64);
            let heights: Vec<Option<u64>> = tx_rows.iter().map(|(_, h)| Some(*h as u64)).collect();
            let txid_rows: Vec<Txid> = tx_rows.iter().map(|(txid, _)| *txid).collect();
            let raw_txs =
                get_electrum_like().batch_transaction_get_raw(&txid_rows).unwrap_or_default();

            for (idx, txid) in txid_rows.iter().enumerate() {
                let height = heights.get(idx).copied().flatten();

                let confirmations = height.and_then(|h| {
                    chain_tip.and_then(|tip| if tip >= h { Some(tip - h + 1) } else { None })
                });

                let mut runestone = Value::Null;
                let mut protostones = Value::Array(Vec::new());
                let mut has_protostones = false;
                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                if !raw.is_empty() {
                    if let Ok(tx) = deserialize::<Transaction>(&raw) {
                        let (runestone_json, protostone_items) = runestone_data(&tx);
                        has_protostones = !protostone_items.is_empty();
                        protostones = Value::Array(protostone_items);
                        if let Some(value) = runestone_json {
                            runestone = value;
                        }
                    }
                }

                items.push(json!({
                    "txid": txid.to_string(),
                    "height": height,
                    "confirmations": confirmations,
                    "has_protostones": has_protostones,
                    "hasProtostones": has_protostones,
                    "protostones": protostones,
                    "runestone": runestone
                }));
            }
        }

        let transactions = items.clone();
        Ok(RpcGetAlkaneAddressTxsResult {
            value: json!({
                "ok": true,
                "address": address,
                "page": page,
                "limit": limit,
                "total": total,
                "txids": txids,
                "items": items,
                "transactions": transactions
            }),
        })
    }

    pub fn rpc_get_address_transactions(
        &self,
        params: RpcGetAddressTransactionsParams,
    ) -> Result<RpcGetAddressTransactionsResult> {
        const DEFAULT_PAGE_LIMIT: usize = 25;
        const MAX_PAGE_LIMIT: usize = 200;
        let Some(address_raw) = params.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
        else {
            return Ok(RpcGetAddressTransactionsResult {
                value: json!({"ok": false, "error": "missing_or_invalid_address"}),
            });
        };
        let Some(address) = normalize_address(address_raw) else {
            return Ok(RpcGetAddressTransactionsResult {
                value: json!({"ok": false, "error": "invalid_address_format"}),
            });
        };

        let page = params.page.unwrap_or(1).max(1);
        let limit = params
            .limit
            .unwrap_or(DEFAULT_PAGE_LIMIT as u64)
            .max(1)
            .min(MAX_PAGE_LIMIT as u64) as usize;
        let only_alkane_txs = params.only_alkane_txs.unwrap_or(true);
        let include_mempool = params.include_mempool.unwrap_or(false);
        let alkane_filter = if let Some(raw_filter) =
            params.filter.as_deref().map(str::trim).filter(|s| !s.is_empty())
        {
            if !only_alkane_txs {
                return Ok(RpcGetAddressTransactionsResult {
                    value: json!({
                        "ok": false,
                        "error": "filter_requires_only_alkane_txs",
                    }),
                });
            }
            let Some(alkane) = parse_alkane_from_str(raw_filter) else {
                return Ok(RpcGetAddressTransactionsResult {
                    value: json!({
                        "ok": false,
                        "error": "invalid_filter",
                        "detail": "filter must be an alkane id like \"2:0\"",
                    }),
                });
            };
            Some(alkane)
        } else {
            None
        };
        let network = get_network();
        let page_offset = page.saturating_sub(1).try_into().unwrap_or(usize::MAX);
        let off = limit.saturating_mul(page_offset);

        let electrum_like = get_electrum_like();
        let address_obj =
            match Address::from_str(&address).and_then(|addr| addr.require_network(network)) {
                Ok(addr) => addr,
                Err(_) => {
                    return Ok(RpcGetAddressTransactionsResult {
                        value: json!({"ok": false, "error": "invalid_address_format"}),
                    });
                }
            };

        if include_mempool && electrum_like.backend() != ElectrumLikeBackend::EsploraHttp {
            return Ok(RpcGetAddressTransactionsResult {
                value: json!({
                    "ok": false,
                    "error": "unsupported_backend",
                    "detail": "include_mempool requires electrs_esplora_url; electrum_rpc_url is not supported"
                }),
            });
        }

        let mempool_txids = if include_mempool {
            match electrum_like.address_mempool_txids(&address_obj) {
                Ok(txids) => txids,
                Err(error) => {
                    return Ok(RpcGetAddressTransactionsResult {
                        value: json!({
                            "ok": false,
                            "error": "electrs_mempool_fetch_failed",
                            "detail": error.to_string()
                        }),
                    });
                }
            }
        } else {
            Vec::new()
        };
        let raw_mempool_txs = if mempool_txids.is_empty() {
            Vec::new()
        } else {
            electrum_like.batch_transaction_get_raw(&mempool_txids).unwrap_or_default()
        };
        let mut pending_renders = Vec::new();
        for (index, txid) in mempool_txids.into_iter().enumerate() {
            let indexed_entry = pending_by_txid(&txid);
            let tx = raw_mempool_txs
                .get(index)
                .filter(|raw| !raw.is_empty())
                .and_then(|raw| deserialize::<Transaction>(raw).ok())
                .or_else(|| indexed_entry.as_ref().map(|entry| entry.tx.clone()));
            let Some(tx) = tx else {
                eprintln!(
                    "[rpc_get_address_transactions] failed to load electrs mempool tx {}",
                    txid
                );
                continue;
            };
            let traces = indexed_entry.and_then(|entry| entry.traces);
            if only_alkane_txs {
                if let Some(filter) = alkane_filter.as_ref() {
                    if !traces.as_ref().is_some_and(|traces| {
                        espo_traces_first_invoke_matches_filter(traces, filter)
                    }) {
                        continue;
                    }
                } else if traces.as_ref().is_none_or(|traces| traces.is_empty())
                    && runestone_data(&tx).1.is_empty()
                {
                    continue;
                }
            }
            pending_renders.push(AddressTxRender {
                txid,
                tx,
                traces,
                block_height: None,
                block_time: None,
                confirmations: Some(0),
                is_mempool: true,
            });
        }
        let pending_total = pending_renders.len();
        let pending_set = pending_renders.iter().map(|render| render.txid).collect::<HashSet<_>>();
        let mut tx_renders = if page == 1 { pending_renders } else { Vec::new() };
        let mut filtered_has_more = false;

        // Pending transactions are prepended to page one and never consume confirmed-page slots.
        let remaining_slots = limit;
        let chain_tip = get_bitcoind_rpc_client()
            .get_blockchain_info()
            .ok()
            .map(|info| info.blocks as u64);
        let confirmed_index_total = if only_alkane_txs {
            get_address_index_list_len(
                self,
                StateAt::Latest,
                AddressIndexListKind::AlkaneTxs,
                &address,
            )
            .unwrap_or(0) as usize
        } else {
            0
        };
        let mut confirmed_total = if alkane_filter.is_some() { 0 } else { confirmed_index_total };

        if only_alkane_txs {
            let confirmed_offset = off;
            if let Some(filter) = alkane_filter.as_ref() {
                let target_matches =
                    confirmed_offset.saturating_add(remaining_slots).saturating_add(1);
                if target_matches > 0 && !filtered_has_more {
                    let scan_chunk = get_address_index_chunk_size().max(256) as u64;
                    let mut range_end = confirmed_index_total as u64;
                    let mut matches: Vec<(Txid, AlkaneTxSummary)> = Vec::new();

                    while range_end > 0 && matches.len() < target_matches {
                        let range_start = range_end.saturating_sub(scan_chunk);
                        let ids = get_address_index_list_range(
                            self,
                            StateAt::Latest,
                            AddressIndexListKind::AlkaneTxs,
                            &address,
                            range_start,
                            range_end,
                        )
                        .unwrap_or_default();

                        for id in ids.into_iter().rev() {
                            let Some(blob) = load_tx_pointer_blob_v3_by_id(self, id) else {
                                continue;
                            };
                            if !sandshrew_traces_first_invoke_matches_filter(&blob.traces, filter) {
                                continue;
                            }
                            let txid = Txid::from_byte_array(blob.txid);
                            let summary = tx_summary_from_pointer_blob(blob);
                            matches.push((txid, summary));
                            if matches.len() >= target_matches {
                                break;
                            }
                        }

                        range_end = range_start;
                    }

                    let page_match_end = confirmed_offset.saturating_add(remaining_slots);
                    filtered_has_more = matches.len() > page_match_end;
                    if remaining_slots > 0 {
                        let page_matches = matches
                            .into_iter()
                            .skip(confirmed_offset)
                            .take(remaining_slots)
                            .collect::<Vec<_>>();
                        let txids: Vec<Txid> =
                            page_matches.iter().map(|(txid, _summary)| *txid).collect();
                        if !txids.is_empty() {
                            let raw_txs =
                                electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();

                            for (idx, (txid, summary)) in page_matches.into_iter().enumerate() {
                                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                                if raw.is_empty() {
                                    continue;
                                }
                                let tx: Transaction = match deserialize(&raw) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        eprintln!(
                                            "[rpc_get_address_transactions] failed to decode tx {}: {e}",
                                            txid
                                        );
                                        continue;
                                    }
                                };
                                let confirmations = {
                                    let h = summary.height as u64;
                                    if h == 0 {
                                        None
                                    } else {
                                        chain_tip.and_then(|tip| {
                                            if tip >= h { Some(tip - h + 1) } else { None }
                                        })
                                    }
                                };
                                let traces = traces_from_summary(&txid, &summary);
                                tx_renders.push(AddressTxRender {
                                    txid,
                                    tx,
                                    traces: (!traces.is_empty()).then_some(traces),
                                    block_height: Some(summary.height as u64),
                                    block_time: None,
                                    confirmations,
                                    is_mempool: false,
                                });
                            }
                        }
                    }
                }
            } else if remaining_slots > 0 {
                let confirmed_slice_start = confirmed_offset.min(confirmed_total);
                let confirmed_slice_end = (confirmed_offset + remaining_slots).min(confirmed_total);

                if confirmed_slice_end > confirmed_slice_start {
                    let mut tx_rows: Vec<(Txid, u32)> = Vec::new();
                    let range_start = confirmed_total.saturating_sub(confirmed_slice_end) as u64;
                    let range_end = confirmed_total.saturating_sub(confirmed_slice_start) as u64;
                    let ids = get_address_index_list_range(
                        self,
                        StateAt::Latest,
                        AddressIndexListKind::AlkaneTxs,
                        &address,
                        range_start,
                        range_end,
                    )
                    .unwrap_or_default();
                    for id in ids.into_iter().rev() {
                        let Some(blob) = load_tx_pointer_blob_v3_by_id(self, id) else {
                            continue;
                        };
                        tx_rows.push((Txid::from_byte_array(blob.txid), blob.height));
                    }

                    if !tx_rows.is_empty() {
                        let txids = tx_rows.iter().map(|(txid, _)| *txid).collect::<Vec<_>>();
                        let raw_txs =
                            electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();

                        for (idx, txid) in txids.iter().enumerate() {
                            let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                            if raw.is_empty() {
                                continue;
                            }
                            let tx: Transaction = match deserialize(&raw) {
                                Ok(value) => value,
                                Err(e) => {
                                    eprintln!(
                                        "[rpc_get_address_transactions] failed to decode tx {}: {e}",
                                        txid
                                    );
                                    continue;
                                }
                            };
                            let summary = load_tx_summary_v2(self, txid);
                            let block_height = tx_rows.get(idx).map(|(_, height)| *height as u64);
                            let confirmations = block_height.and_then(|height| {
                                chain_tip.and_then(|tip| {
                                    if tip >= height { Some(tip - height + 1) } else { None }
                                })
                            });
                            let traces = summary
                                .as_ref()
                                .map(|s| traces_from_summary(txid, s))
                                .filter(|v| !v.is_empty());
                            tx_renders.push(AddressTxRender {
                                txid: *txid,
                                tx,
                                traces,
                                block_height,
                                block_time: None,
                                confirmations,
                                is_mempool: false,
                            });
                        }
                    }
                }
            }
        } else {
            let confirmed_offset = off;
            let fetch_limit = remaining_slots.max(1);
            match electrum_like.address_history_page(&address_obj, confirmed_offset, fetch_limit) {
                Ok(hist_page) => {
                    let mut entries: Vec<AddressHistoryEntry> = hist_page
                        .entries
                        .into_iter()
                        .filter(|entry| !pending_set.contains(&entry.txid))
                        .collect();
                    confirmed_total = hist_page
                        .total
                        .unwrap_or(confirmed_offset + entries.len())
                        .max(entries.len());
                    if remaining_slots > 0 {
                        let to_take = remaining_slots.min(entries.len());
                        let entries_for_page = entries.drain(..to_take).collect::<Vec<_>>();
                        let txids: Vec<Txid> = entries_for_page.iter().map(|e| e.txid).collect();
                        if !txids.is_empty() {
                            let raw_txs =
                                electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
                            for (idx, txid) in txids.iter().enumerate() {
                                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                                if raw.is_empty() {
                                    continue;
                                }
                                let tx: Transaction = match deserialize(&raw) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        eprintln!(
                                            "[rpc_get_address_transactions] failed to decode tx {}: {e}",
                                            txid
                                        );
                                        continue;
                                    }
                                };
                                let summary = load_tx_summary_v2(self, txid);
                                let confirmations = entries_for_page[idx].height.and_then(|h| {
                                    chain_tip.and_then(|tip| {
                                        if tip >= h { Some(tip - h + 1) } else { None }
                                    })
                                });
                                let traces = summary
                                    .as_ref()
                                    .map(|s| traces_from_summary(txid, s))
                                    .filter(|v| !v.is_empty());
                                tx_renders.push(AddressTxRender {
                                    txid: *txid,
                                    tx,
                                    traces,
                                    block_height: entries_for_page[idx].height,
                                    block_time: None,
                                    confirmations,
                                    is_mempool: false,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[rpc_get_address_transactions] failed to fetch history for {}: {e}",
                        address
                    );
                }
            }
        }

        let mut seen_txids = HashSet::new();
        tx_renders.retain(|render| seen_txids.insert(render.txid));

        let mut block_heights = tx_renders
            .iter()
            .filter_map(|render| render.block_height)
            .filter_map(|height| u32::try_from(height).ok())
            .collect::<Vec<_>>();
        block_heights.sort_unstable();
        block_heights.dedup();
        if !block_heights.is_empty() {
            match self.get_block_summaries_by_heights(&block_heights) {
                Ok(summaries) => {
                    let block_times = block_heights
                        .into_iter()
                        .zip(summaries)
                        .filter_map(|(height, summary)| {
                            summary.and_then(|summary| {
                                summary.block_time().map(|block_time| (height, block_time))
                            })
                        })
                        .collect::<HashMap<_, _>>();
                    for render in &mut tx_renders {
                        render.block_time = render
                            .block_height
                            .and_then(|height| u32::try_from(height).ok())
                            .and_then(|height| block_times.get(&height).copied());
                    }
                }
                Err(error) => {
                    eprintln!(
                        "[rpc_get_address_transactions] failed to load block times: {error:#}"
                    );
                }
            }
        }
        let tx_total = pending_total.saturating_add(confirmed_total);
        let confirmed_render_count = tx_renders.iter().filter(|render| !render.is_mempool).count();
        let mut prev_txids: Vec<Txid> = Vec::new();
        for render in &tx_renders {
            for vin in &render.tx.input {
                if !vin.previous_output.is_null() {
                    prev_txids.push(vin.previous_output.txid);
                }
            }
        }
        prev_txids.sort();
        prev_txids.dedup();
        let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
        if !prev_txids.is_empty() {
            let raw_prev = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
            for (i, raw) in raw_prev.into_iter().enumerate() {
                if raw.is_empty() {
                    if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                        prev_map.insert(prev_txids[i], mempool_prev.tx);
                    }
                    continue;
                }
                if let Ok(prev_tx) = deserialize::<Transaction>(&raw) {
                    prev_map.insert(prev_txids[i], prev_tx);
                } else if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                    prev_map.insert(prev_txids[i], mempool_prev.tx);
                }
            }
        }

        let transactions: Vec<Value> = tx_renders
            .iter()
            .map(|render| enriched_transaction_json(render, &prev_map, network))
            .collect();

        Ok(RpcGetAddressTransactionsResult {
            value: json!({
                "ok": true,
                "address": address,
                "page": page,
                "limit": limit,
                "include_mempool": include_mempool,
                "total": if alkane_filter.is_some() { Value::Null } else { json!(tx_total) },
                "has_more": if alkane_filter.is_some() {
                    filtered_has_more
                } else {
                    off.saturating_add(confirmed_render_count) < confirmed_total
                },
                "transactions": transactions,
            }),
        })
    }

    pub fn rpc_get_alkane_latest_traces(
        &self,
        _params: RpcGetAlkaneLatestTracesParams,
    ) -> Result<RpcGetAlkaneLatestTracesResult> {
        let table = self.table();
        let len = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.latest_traces_length_key(),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|b| {
                if b.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&b);
                    Some(u32::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        let mut txids: Vec<String> = Vec::new();
        if len > 0 {
            let mut keys = Vec::with_capacity(len as usize);
            for idx in 0..len {
                keys.push(table.latest_traces_idx_key(idx));
            }
            let values = self
                .get_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })
                .ok()
                .map(|r| r.values);
            if let Some(values) = values {
                for v in values.into_iter().flatten() {
                    if v.len() != 32 {
                        continue;
                    }
                    if let Ok(txid) = Txid::from_slice(&v) {
                        txids.push(txid.to_string());
                    }
                }
            }
        }

        let txids: Vec<String> = txids.into_iter().take(20).collect();

        Ok(RpcGetAlkaneLatestTracesResult {
            value: json!({
                "ok": true,
                "txids": txids
            }),
        })
    }

    pub fn rpc_ping(&self, _params: RpcPingParams) -> Result<RpcPingResult> {
        Ok(RpcPingResult { value: Value::String("pong".to_string()) })
    }
}

pub struct GetRawValueParams {
    pub blockhash: StateAt,

    pub key: Vec<u8>,
}

pub struct GetRawValueResult {
    pub value: Option<Vec<u8>>,
}

pub struct GetMultiValuesParams {
    pub blockhash: StateAt,

    pub keys: Vec<Vec<u8>>,
}

pub struct GetMultiValuesResult {
    pub values: Vec<Option<Vec<u8>>>,
}

pub struct GetListKeysByPrefixParams {
    pub blockhash: StateAt,

    pub prefix: Vec<u8>,
}

pub struct GetListKeysByPrefixResult {
    pub keys: Vec<Vec<u8>>,
}

pub struct GetListEntriesDescParams {
    pub blockhash: StateAt,

    pub prefix: Vec<u8>,
}

pub struct GetListEntriesDescResult {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct GetListEntriesDescCursorParams {
    pub blockhash: StateAt,

    pub prefix: Vec<u8>,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

pub struct GetListEntriesDescCursorResult {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next_cursor: Option<Vec<u8>>,
    pub has_more: bool,
}

pub struct SetRawValueParams {
    pub blockhash: StateAt,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct SetBatchParams {
    pub blockhash: StateAt,

    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
}

pub struct SetBlobValuesIfMissingParams {
    pub blockhash: StateAt,

    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
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

pub struct GetCreationRecordParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
}

pub struct GetCreationRecordResult {
    pub record: Option<AlkaneCreationRecord>,
}

pub struct GetCreationRecordsByIdParams {
    pub blockhash: StateAt,

    pub alkanes: Vec<SchemaAlkaneId>,
}

pub struct GetCreationRecordsByIdResult {
    pub records: Vec<Option<AlkaneCreationRecord>>,
}

pub struct GetCreationRecordsOrderedParams {
    pub blockhash: StateAt,
}

pub struct GetCreationRecordsOrderedResult {
    pub records: Vec<AlkaneCreationRecord>,
}

pub struct GetCreationRecordsOrderedPageParams {
    pub blockhash: StateAt,

    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetCreationRecordsOrderedPageResult {
    pub records: Vec<AlkaneCreationRecord>,
}

pub struct GetAlkaneIdsByNamePrefixParams {
    pub blockhash: StateAt,

    pub prefix: String,
}

pub struct GetAlkaneIdsByNamePrefixResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetAlkaneIdsByNamePrefixPageParams {
    pub blockhash: StateAt,

    pub prefix: String,
    pub offset: u64,
    pub limit: u64,
}

pub struct GetAlkaneIdsBySymbolPrefixParams {
    pub blockhash: StateAt,

    pub prefix: String,
}

pub struct GetAlkaneIdsBySymbolPrefixResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetAlkaneIdsBySymbolPrefixPageParams {
    pub blockhash: StateAt,

    pub prefix: String,
    pub offset: u64,
    pub limit: u64,
}

pub struct GetCreationCountParams {
    pub blockhash: StateAt,
}

pub struct GetCreationCountResult {
    pub count: u64,
}

pub struct GetCreationIdsInBlockParams {
    pub blockhash: StateAt,

    pub height: u32,
}

pub struct GetCreationIdsInBlockResult {
    pub alkanes: Vec<SchemaAlkaneId>,
}

pub struct GetFactoryChildrenParams {
    pub blockhash: StateAt,

    pub factory: SchemaAlkaneId,
}

pub struct GetFactoryChildrenResult {
    pub children: Vec<SchemaAlkaneId>,
}

pub struct GetHoldersCountParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
}

pub struct GetHoldersCountResult {
    pub count: u64,
}

pub struct GetHoldersCountsByIdParams {
    pub blockhash: StateAt,

    pub alkanes: Vec<SchemaAlkaneId>,
}

pub struct GetHoldersCountsByIdResult {
    pub counts: Vec<u64>,
}

pub struct GetHoldersOrderedPageParams {
    pub blockhash: StateAt,

    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetHoldersOrderedPageResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetCirculatingSupplyParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
    pub height: u32,
}

pub struct GetCirculatingSupplyResult {
    pub supply: u128,
}

pub struct GetLatestCirculatingSupplyParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
}

pub struct GetLatestCirculatingSupplyResult {
    pub supply: u128,
}

pub struct GetLatestTotalMintedParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
}

pub struct GetLatestTotalMintedResult {
    pub total_minted: u128,
}

pub struct GetAlkaneStorageValueParams {
    pub blockhash: StateAt,

    pub alkane: SchemaAlkaneId,
    pub key: Vec<u8>,
}

pub struct GetAlkaneStorageValueResult {
    pub value: Option<Vec<u8>>,
}

pub struct GetBlockSummaryParams {
    pub blockhash: StateAt,

    pub height: u32,
}

pub struct GetBlockSummaryResult {
    pub summary: Option<BlockSummary>,
}

pub struct GetMempoolSeenPageParams {
    pub blockhash: StateAt,

    pub page: usize,
    pub limit: usize,
}

pub struct GetMempoolSeenPageResult {
    pub txids: Vec<Txid>,
    pub has_more: bool,
}

pub struct GetMempoolEntryParams {
    pub blockhash: StateAt,

    pub txid: Txid,
}

pub struct GetMempoolEntryResult {
    pub entry: Option<MempoolEntry>,
}

pub struct GetMempoolPendingForAddressParams {
    pub blockhash: StateAt,

    pub address: String,
}

pub struct GetMempoolPendingForAddressResult {
    pub entries: Vec<MempoolEntry>,
}

pub struct RpcGetMempoolTracesParams {
    pub page: Option<u64>,
    pub limit: Option<u64>,
    pub address: Option<String>,
    pub fee_paid: Option<f64>,
}

pub struct RpcGetMempoolTracesResult {
    pub value: Value,
}

pub struct RpcGetKeysParams {
    pub alkane: Option<String>,
    pub try_decode_utf8: Option<bool>,
    pub limit: Option<u64>,
    pub page: Option<u64>,
    pub keys: Option<Vec<String>>,
}

pub struct RpcGetKeysResult {
    pub value: Value,
}

pub struct RpcSearchFactoryKeysParams {
    pub factory: Option<String>,
    /// Explicit condition objects: [{key, op, value}, …] (ANDed).
    pub conditions: Option<Vec<Value>>,
    /// Single-condition shorthand used when `conditions` is absent.
    pub key: Option<String>,
    pub op: Option<String>,
    pub value: Option<Value>,
    pub try_decode_utf8: Option<bool>,
    pub limit: Option<u64>,
    pub page: Option<u64>,
}

pub struct RpcSearchFactoryKeysResult {
    pub value: Value,
}

pub struct RpcGetAllAlkanesParams {
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetAllAlkanesResult {
    pub value: Value,
}

pub struct RpcSearchAlkaneParams {
    pub prefix: Option<String>,
    pub limit: Option<u64>,
}

pub struct RpcSearchAlkaneResult {
    pub value: Value,
}

pub struct RpcGetAlkaneInfoParams {
    pub alkane: Option<String>,
}

pub struct RpcGetAlkaneInfoResult {
    pub value: Value,
}

pub struct RpcGetAlkabiParams {
    pub alkane: Option<String>,
    pub format: Option<String>,
}

pub struct RpcGetAlkabiResult {
    pub value: Value,
}

pub struct RpcGetAlkaneWasmParams {
    pub alkane: Option<String>,
    /// Gzip the payload before base64-encoding (wasm compresses ~3-4x).
    pub gzip: Option<bool>,
}

pub struct RpcGetAlkaneWasmResult {
    pub value: Value,
}

pub struct RpcGetFactoryChildrenParams {
    pub factory: Option<String>,
}

pub struct RpcGetFactoryChildrenResult {
    pub value: Value,
}

pub struct RpcGetBlockSummaryParams {
    pub height: Option<u64>,
}

pub struct RpcGetBlockSummaryResult {
    pub value: Value,
}

pub struct RpcGetBlockTimeParams {
    pub height: Option<u64>,
}

pub struct RpcGetBlockTimeResult {
    pub value: Value,
}

pub struct RpcGetBlockTimesParams {
    pub heights: Option<Vec<u64>>,
}

pub struct RpcGetBlockTimesResult {
    pub value: Value,
}

pub struct RpcGetHoldersParams {
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetHoldersResult {
    pub value: Value,
}

pub struct RpcGetOrbitalHoldersParams {
    pub factory: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetOrbitalHoldersResult {
    pub value: Value,
}

pub struct RpcGetOrbitalVolumesParams {
    pub factory: Option<String>,
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetOrbitalVolumesResult {
    pub value: Value,
}

pub struct RpcGetOrbitalBalancesParams {
    pub address: Option<String>,
}

pub struct RpcGetOrbitalBalancesResult {
    pub value: Value,
}

pub struct RpcGetAlkaneVolumesParams {
    pub source_alkane: Option<String>,
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetAlkaneVolumesResult {
    pub value: Value,
}

pub struct RpcGetTransferVolumeParams {
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetTransferVolumeResult {
    pub value: Value,
}

pub struct RpcGetTotalReceivedParams {
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetTotalReceivedResult {
    pub value: Value,
}

pub struct RpcGetCirculatingSupplyParams {
    pub alkane: Option<String>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct RpcGetCirculatingSupplyResult {
    pub value: Value,
}

pub struct RpcGetAddressActivityParams {
    pub address: Option<String>,
}

pub struct RpcGetAddressActivityResult {
    pub value: Value,
}

pub struct RpcGetAddressCumulativeAlkanesParams {
    pub address: Option<String>,
}

pub struct RpcGetAddressCumulativeAlkanesResult {
    pub value: Value,
}

pub struct RpcGetAddressBalancesParams {
    pub address: Option<String>,
    pub include_outpoints: Option<bool>,
}

pub struct RpcGetAddressBalancesResult {
    pub value: Value,
}

pub struct RpcGetAlkaneBalancesParams {
    pub alkane: Option<String>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct RpcGetAlkaneBalancesResult {
    pub value: Value,
}

pub struct RpcGetAlkaneBalanceMetashrewParams {
    pub owner: Option<String>,
    pub target: Option<String>,
    pub height: Option<u64>,
    pub height_present: bool,
}

pub struct RpcGetAlkaneBalanceMetashrewResult {
    pub value: Value,
}

pub struct RpcGetAlkaneBalanceTxsParams {
    pub alkane: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
    pub cursor: Option<String>,
}

pub struct RpcGetAlkaneBalanceTxsResult {
    pub value: Value,
}

pub struct RpcGetAlkaneBalanceTxsByTokenParams {
    pub owner: Option<String>,
    pub token: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
    pub cursor: Option<String>,
}

pub struct RpcGetAlkaneBalanceTxsByTokenResult {
    pub value: Value,
}

pub struct RpcGetOutpointBalancesParams {
    pub outpoint: Option<String>,
}

pub struct RpcGetOutpointBalancesResult {
    pub value: Value,
}

pub struct RpcGetBlockTracesParams {
    pub height: Option<u64>,
}

pub struct RpcGetBlockTracesResult {
    pub value: Value,
}

pub struct RpcGetHoldersCountParams {
    pub alkane: Option<String>,
}

pub struct RpcGetHoldersCountResult {
    pub value: Value,
}

pub struct RpcGetAddressOutpointsParams {
    pub address: Option<String>,
}

pub struct RpcGetAddressOutpointsResult {
    pub value: Value,
}

pub struct RpcGetAddressSpendableOutpointsParams {
    pub address: Option<String>,
    pub omit_raw_tx: Option<bool>,
}

pub struct RpcGetAddressSpendableOutpointsResult {
    pub value: Value,
}

pub struct RpcGetAlkaneTxSummaryParams {
    pub txid: Option<String>,
}

pub struct RpcGetAlkaneTxSummaryResult {
    pub value: Value,
}

pub struct RpcGetAlkaneBlockTxsParams {
    pub height: Option<u64>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetAlkaneBlockTxsResult {
    pub value: Value,
}

pub struct RpcGetAlkaneAddressTxsParams {
    pub address: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetAlkaneAddressTxsResult {
    pub value: Value,
}

pub struct RpcGetAddressTransactionsParams {
    pub address: Option<String>,
    pub page: Option<u64>,
    pub limit: Option<u64>,
    pub only_alkane_txs: Option<bool>,
    pub include_mempool: Option<bool>,
    pub filter: Option<String>,
}

pub struct RpcGetAddressTransactionsResult {
    pub value: Value,
}

pub struct RpcGetAlkaneLatestTracesParams;

pub struct RpcGetAlkaneLatestTracesResult {
    pub value: Value,
}

pub struct RpcPingParams;

pub struct RpcPingResult {
    pub value: Value,
}

/// Identifier for a holder: either a Bitcoin address or another Alkane.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, BorshSerialize, BorshDeserialize)]
pub enum HolderId {
    Address(String),
    Alkane(SchemaAlkaneId),
}

/// Entry in holders index (holder id + amount for one alkane)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HolderEntry {
    pub holder: HolderId,
    pub amount: u128,
}

/// Entry in orbital holders index: one holder and the child Alkanes it owns for a factory.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct OrbitalHolderEntry {
    pub holder: HolderId,
    pub amount: u128,
    pub alkanes: Vec<SchemaAlkaneId>,
}

/// Per-address orbital balance entry: one factory and the child Alkanes held by the address.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AddressOrbitalBalanceEntry {
    pub factory: SchemaAlkaneId,
    pub amount: u128,
    pub alkanes: Vec<SchemaAlkaneId>,
}

/// Entry in per-alkane address activity indexes.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AddressAmountEntry {
    pub address: String,
    pub amount: u128,
}

/// Per-address activity summary across alkanes.
#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct AddressActivityEntry {
    pub transfer_volume: BTreeMap<SchemaAlkaneId, u128>,
    pub total_received: BTreeMap<SchemaAlkaneId, u128>,
}

/// One cumulative contract-attributed amount for an address.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AddressContractAmountEntry {
    pub contract: SchemaAlkaneId,
    pub alkane: SchemaAlkaneId,
    pub amount: u128,
}

/// One alkane balance record inside a single outpoint (BORSH)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BalanceEntry {
    pub alkane: SchemaAlkaneId,
    pub amount: u128,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct OutpointRowV2 {
    pub address: String,
    pub spk: Vec<u8>,
    pub balances: Vec<BalanceEntry>,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct OutpointPointerBlobV3 {
    pub txid: [u8; 32],
    pub vout: u32,
    pub blockhash: [u8; 32],
    pub tx_idx: u32,
    pub address: String,
    pub spk: Vec<u8>,
    pub balances: Vec<BalanceEntry>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AlkaneBalanceTxEntry {
    pub txid: [u8; 32],
    pub height: u32,
    pub outflow: BTreeMap<SchemaAlkaneId, SignedU128>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AlkaneTxSummary {
    pub txid: [u8; 32],
    pub traces: Vec<EspoSandshrewLikeTrace>,
    pub outflows: Vec<AlkaneBalanceTxEntry>,
    pub height: u32,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct TxPackedOutflowRowV2 {
    pub height: u32,
    pub traces: Vec<EspoSandshrewLikeTrace>,
    pub outflows: BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct TxPointerBlobV3 {
    pub txid: [u8; 32],
    pub blockhash: [u8; 32],
    pub tx_idx: u32,
    pub height: u32,
    pub traces: Vec<EspoSandshrewLikeTrace>,
    pub outflows: BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HoldersCountEntry {
    pub count: u64,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BlockSummaryPool {
    pub id: Option<u16>,
    pub name: String,
    pub slug: String,
    pub matched: bool,
    pub link: Option<String>,
    pub mempool_url: Option<String>,
    pub icon_url: Option<String>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BlockSummary {
    pub height: u32,
    pub blockhash: [u8; 32],
    pub trace_count: u32,
    pub interaction_count: u32,
    pub tx_count: u32,
    pub header: Vec<u8>,
    pub fee_avg: f64,
    pub fee_median: f64,
    pub fee_range: Vec<f64>,
    pub pool: Option<BlockSummaryPool>,
}

#[derive(Clone, Debug, BorshDeserialize)]
struct LegacyBlockSummaryV3 {
    pub height: u32,
    pub blockhash: [u8; 32],
    pub trace_count: u32,
    pub interaction_count: u32,
    pub tx_count: u32,
    pub header: Vec<u8>,
    pub fee_avg: f64,
    pub fee_median: f64,
    pub fee_range: Vec<f64>,
}

#[derive(Clone, Debug, BorshDeserialize)]
struct LegacyBlockSummaryV2 {
    pub height: u32,
    pub blockhash: [u8; 32],
    pub trace_count: u32,
    pub tx_count: u32,
    pub header: Vec<u8>,
    pub fee_avg: f64,
    pub fee_median: f64,
    pub fee_range: Vec<f64>,
}

#[derive(Clone, Debug, BorshDeserialize)]
struct LegacyBlockSummaryV1 {
    pub trace_count: u32,
    pub tx_count: u32,
    pub header: Vec<u8>,
}

#[derive(Clone, Debug, BorshDeserialize)]
struct LegacyBlockSummaryV0 {
    pub trace_count: u32,
    pub header: Vec<u8>,
}

impl BlockSummary {
    pub fn decode(raw: &[u8]) -> Option<Self> {
        Self::try_from_slice(raw)
            .ok()
            .or_else(|| {
                LegacyBlockSummaryV3::try_from_slice(raw).ok().map(|legacy| Self {
                    height: legacy.height,
                    blockhash: legacy.blockhash,
                    trace_count: legacy.trace_count,
                    interaction_count: legacy.interaction_count,
                    tx_count: legacy.tx_count,
                    header: legacy.header,
                    fee_avg: legacy.fee_avg,
                    fee_median: legacy.fee_median,
                    fee_range: legacy.fee_range,
                    pool: None,
                })
            })
            .or_else(|| {
                LegacyBlockSummaryV2::try_from_slice(raw).ok().map(|legacy| Self {
                    height: legacy.height,
                    blockhash: legacy.blockhash,
                    trace_count: legacy.trace_count,
                    interaction_count: legacy.trace_count,
                    tx_count: legacy.tx_count,
                    header: legacy.header,
                    fee_avg: legacy.fee_avg,
                    fee_median: legacy.fee_median,
                    fee_range: legacy.fee_range,
                    pool: None,
                })
            })
            .or_else(|| {
                LegacyBlockSummaryV1::try_from_slice(raw).ok().map(|legacy| Self {
                    height: 0,
                    blockhash: [0; 32],
                    trace_count: legacy.trace_count,
                    interaction_count: legacy.trace_count,
                    tx_count: legacy.tx_count,
                    header: legacy.header,
                    fee_avg: 0.0,
                    fee_median: 0.0,
                    fee_range: Vec::new(),
                    pool: None,
                })
            })
            .or_else(|| {
                LegacyBlockSummaryV0::try_from_slice(raw).ok().map(|legacy| Self {
                    height: 0,
                    blockhash: [0; 32],
                    trace_count: legacy.trace_count,
                    interaction_count: legacy.trace_count,
                    tx_count: 0,
                    header: legacy.header,
                    fee_avg: 0.0,
                    fee_median: 0.0,
                    fee_range: Vec::new(),
                    pool: None,
                })
            })
    }

    pub fn block_hash(&self) -> Option<BlockHash> {
        if self.blockhash == [0; 32] {
            return None;
        }
        Some(BlockHash::from_byte_array(self.blockhash))
    }

    pub fn block_time(&self) -> Option<u64> {
        deserialize::<Header>(&self.header).ok().map(|header| header.time as u64)
    }
}

fn decode_blockhash(bytes: &[u8]) -> Option<BlockHash> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn encode_blockhash(blockhash: &BlockHash) -> Vec<u8> {
    blockhash.to_string().into_bytes()
}

const BLOCK_SUMMARY_CACHE_CAP: usize = 100;

struct BlockSummaryCache {
    order: VecDeque<u32>,
    map: HashMap<u32, BlockSummary>,
}

impl BlockSummaryCache {
    fn insert(&mut self, height: u32, summary: BlockSummary) {
        if self.map.contains_key(&height) {
            self.order.retain(|h| *h != height);
        }
        self.map.insert(height, summary);
        self.order.push_back(height);
        while self.order.len() > BLOCK_SUMMARY_CACHE_CAP {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
    }

    fn get(&self, height: u32) -> Option<BlockSummary> {
        self.map.get(&height).cloned()
    }
}

static BLOCK_SUMMARY_CACHE: OnceLock<Arc<RwLock<BlockSummaryCache>>> = OnceLock::new();

fn block_summary_cache() -> &'static Arc<RwLock<BlockSummaryCache>> {
    BLOCK_SUMMARY_CACHE.get_or_init(|| {
        Arc::new(RwLock::new(BlockSummaryCache { order: VecDeque::new(), map: HashMap::new() }))
    })
}

pub fn cache_block_summary(height: u32, summary: BlockSummary) {
    if let Ok(mut cache) = block_summary_cache().write() {
        cache.insert(height, summary);
    }
}

pub fn get_cached_block_summary(height: u32) -> Option<BlockSummary> {
    crate::debug_timer_log!("get_cached_block_summary");
    let summary = block_summary_cache().read().ok().and_then(|cache| cache.get(height))?;
    if let Some(tree) = get_global_tree_db() {
        let canonical_hash = tree.blockhash_for_height(height).ok().flatten()?;
        if summary.block_hash() != Some(canonical_hash) {
            return None;
        }
    }
    Some(summary)
}

pub fn preload_block_summary_cache(mdb: &Mdb) -> usize {
    let table = EssentialsTable::new(mdb);
    let index_height = mdb
        .get(table.INDEX_HEIGHT.key())
        .ok()
        .flatten()
        .and_then(|raw| decode_u32_le(&raw))
        .unwrap_or(0);
    if index_height == 0 {
        return 0;
    }

    let provider = EssentialsProvider::new(Arc::new(mdb.clone()));
    let mut loaded = 0usize;
    let mut misses_after_first_summary = 0usize;
    let mut height = index_height;
    loop {
        if loaded >= BLOCK_SUMMARY_CACHE_CAP {
            break;
        }
        if let Ok(Some(summary)) = provider.get_latest_block_summary_by_height(height) {
            cache_block_summary(height, summary);
            loaded += 1;
            misses_after_first_summary = 0;
        } else if loaded > 0 {
            misses_after_first_summary += 1;
            if misses_after_first_summary >= 256 {
                break;
            }
        }
        if height == 0 {
            break;
        }
        height = height.saturating_sub(1);
    }

    loaded
}

/// Creation metadata for an alkane.
#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct AlkaneInfo {
    pub creation_txid: [u8; 32],
    pub creation_height: u32,
    pub creation_timestamp: u32,
}

/// Helper to build an outpoint with optional spending txid for lookups.
pub fn mk_outpoint(txid: Vec<u8>, vout: u32, tx_spent: Option<Vec<u8>>) -> EspoOutpoint {
    EspoOutpoint { txid, vout, tx_spent }
}

pub fn spk_to_address_str(spk: &ScriptBuf, net: Network) -> Option<String> {
    Address::from_script(spk.as_script(), net).ok().map(|a| a.to_string())
}

pub fn encode_vec<T: BorshSerialize>(v: &Vec<T>) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_balances_vec(bytes: &[u8]) -> Result<Vec<BalanceEntry>> {
    Ok(Vec::<BalanceEntry>::try_from_slice(bytes)?)
}

pub fn encode_alkane_balance_tx_entry(entry: &AlkaneBalanceTxEntry) -> Result<Vec<u8>> {
    borsh::to_vec(entry).map_err(|e| anyhow!("encode alkane balance tx entry failed: {e}"))
}

pub fn decode_alkane_balance_tx_entry(bytes: &[u8]) -> Result<AlkaneBalanceTxEntry> {
    AlkaneBalanceTxEntry::try_from_slice(bytes)
        .map_err(|e| anyhow!("decode alkane balance tx entry failed: {e}"))
}

pub fn encode_tx_packed_outflow_row_v2(
    height: u32,
    traces: &[EspoSandshrewLikeTrace],
    outflows: &BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
) -> Result<Vec<u8>> {
    borsh::to_vec(&TxPackedOutflowRowV2 {
        height,
        traces: traces.to_vec(),
        outflows: outflows.clone(),
    })
    .map_err(|e| anyhow!("encode tx packed outflow v2 failed: {e}"))
}

pub fn decode_tx_packed_outflow_row_v2(bytes: &[u8]) -> Result<TxPackedOutflowRowV2> {
    TxPackedOutflowRowV2::try_from_slice(bytes)
        .map_err(|e| anyhow!("decode tx packed outflow v2 failed: {e}"))
}

pub fn encode_pointer_idx_u64(id: u64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

pub fn decode_pointer_idx_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(anyhow!("decode pointer idx failed: invalid length {}", bytes.len()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct VersionedU64EntryV1 {
    height: u32,
    blockhash: [u8; 32],
    value: u64,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct VersionedU64ListV1 {
    entries: Vec<VersionedU64EntryV1>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct VersionedBytes32EntryV1 {
    height: u32,
    blockhash: [u8; 32],
    value: [u8; 32],
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct VersionedBytes32ListV1 {
    entries: Vec<VersionedBytes32EntryV1>,
}

fn decode_versioned_u64_list(bytes: &[u8]) -> Vec<VersionedU64EntryV1> {
    VersionedU64ListV1::try_from_slice(bytes)
        .map(|row| row.entries)
        .unwrap_or_default()
}

fn encode_versioned_u64_list(entries: Vec<VersionedU64EntryV1>) -> Result<Vec<u8>> {
    borsh::to_vec(&VersionedU64ListV1 { entries })
        .map_err(|e| anyhow!("encode versioned_u64 list failed: {e}"))
}

fn decode_versioned_bytes32_list(bytes: &[u8]) -> Vec<VersionedBytes32EntryV1> {
    VersionedBytes32ListV1::try_from_slice(bytes)
        .map(|row| row.entries)
        .unwrap_or_default()
}

fn encode_versioned_bytes32_list(entries: Vec<VersionedBytes32EntryV1>) -> Result<Vec<u8>> {
    borsh::to_vec(&VersionedBytes32ListV1 { entries })
        .map_err(|e| anyhow!("encode versioned_bytes32 list failed: {e}"))
}

#[allow(dead_code)]
fn sort_versioned_u64_entries_desc(entries: &mut [VersionedU64EntryV1]) {
    entries.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| b.blockhash.as_slice().cmp(a.blockhash.as_slice()))
    });
}

fn sort_versioned_bytes32_entries_desc(entries: &mut [VersionedBytes32EntryV1]) {
    entries.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| b.blockhash.as_slice().cmp(a.blockhash.as_slice()))
    });
}

fn resolve_target_blockhash(
    provider: &EssentialsProvider,
    blockhash: StateAt,
) -> Option<BlockHash> {
    match blockhash {
        StateAt::Block(h) => Some(h),
        StateAt::Latest => provider.resolved_view_blockhash(),
    }
}

fn version_visible_for_target(
    provider: &EssentialsProvider,
    target: Option<BlockHash>,
    version_blockhash: BlockHash,
) -> bool {
    let Some(target_blockhash) = target else {
        return true;
    };
    if version_blockhash == target_blockhash {
        return true;
    }
    provider
        .blockhash_is_ancestor(&version_blockhash, &target_blockhash)
        .unwrap_or(false)
}

fn resolve_visible_u64_entry(
    provider: &EssentialsProvider,
    entries: &[VersionedU64EntryV1],
    target: Option<BlockHash>,
) -> Option<u64> {
    let fast_active_tip = provider
        .resolved_view_blockhash()
        .filter(|_| provider.view_blockhash().is_none());
    let mut active_visibility_cache: HashMap<BlockHash, bool> = HashMap::new();
    for entry in entries {
        let entry_blockhash = BlockHash::from_byte_array(entry.blockhash);
        if let (Some(target_blockhash), Some(active_tip)) = (target, fast_active_tip) {
            if target_blockhash == active_tip {
                let visible =
                    *active_visibility_cache.entry(entry_blockhash).or_insert_with(|| {
                        provider.blockhash_is_on_active_chain(&entry_blockhash).unwrap_or(false)
                    });
                if visible {
                    return Some(entry.value);
                }
                continue;
            }
        }
        if version_visible_for_target(provider, target, entry_blockhash) {
            return Some(entry.value);
        }
    }
    None
}

fn resolve_visible_bytes32_entry(
    provider: &EssentialsProvider,
    entries: &[VersionedBytes32EntryV1],
    target: Option<BlockHash>,
) -> Option<[u8; 32]> {
    let fast_active_tip = provider
        .resolved_view_blockhash()
        .filter(|_| provider.view_blockhash().is_none());
    let mut active_visibility_cache: HashMap<BlockHash, bool> = HashMap::new();
    for entry in entries {
        let entry_blockhash = BlockHash::from_byte_array(entry.blockhash);
        if let (Some(target_blockhash), Some(active_tip)) = (target, fast_active_tip) {
            if target_blockhash == active_tip {
                let visible =
                    *active_visibility_cache.entry(entry_blockhash).or_insert_with(|| {
                        provider.blockhash_is_on_active_chain(&entry_blockhash).unwrap_or(false)
                    });
                if visible {
                    return Some(entry.value);
                }
                continue;
            }
        }
        if version_visible_for_target(provider, target, entry_blockhash) {
            return Some(entry.value);
        }
    }
    None
}

#[allow(dead_code)]
pub(crate) fn build_outpoint_pos_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<([u8; 32], u32), u64>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<(([u8; 32], u32), u64)> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let keys: Vec<Vec<u8>> = pairs
        .iter()
        .map(|((txid, vout), _)| table.outpoint_pos_point_key_from_parts(txid, *vout))
        .collect::<Result<Vec<_>>>()?;
    let prev_vals = provider
        .get_blob_multi_values(GetMultiValuesParams {
            blockhash: StateAt::Latest,
            keys: keys.clone(),
        })?
        .values;
    let mut out = Vec::with_capacity(pairs.len());
    for (((_txid, _vout), value), prev_raw, key) in pairs
        .into_iter()
        .zip(prev_vals.into_iter())
        .zip(keys.into_iter())
        .map(|((a, b), c)| (a, b, c))
    {
        let mut entries =
            prev_raw.as_ref().map(|raw| decode_versioned_u64_list(raw)).unwrap_or_default();
        if let Some(existing) =
            entries.iter_mut().find(|e| e.height == height && e.blockhash == *blockhash)
        {
            existing.value = value;
        } else {
            entries.push(VersionedU64EntryV1 { height, blockhash: *blockhash, value });
        }
        sort_versioned_u64_entries_desc(&mut entries);
        out.push((key, encode_versioned_u64_list(entries)?));
    }
    Ok(out)
}

pub(crate) fn build_new_outpoint_pos_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<([u8; 32], u32), u64>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<(([u8; 32], u32), u64)> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = Vec::with_capacity(pairs.len());
    for ((txid, vout), value) in pairs {
        let key = table.outpoint_pos_point_key_from_parts(&txid, vout)?;
        out.push((
            key,
            encode_versioned_u64_list(vec![VersionedU64EntryV1 {
                height,
                blockhash: *blockhash,
                value,
            }])?,
        ));
    }
    Ok(out)
}

pub(crate) fn build_outpoint_spent_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<u64, [u8; 32]>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<(u64, [u8; 32])> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let keys: Vec<Vec<u8>> =
        pairs.iter().map(|(id, _)| table.outpoint_spent_by_id_point_key(*id)).collect();
    let prev_vals = provider
        .get_blob_multi_values(GetMultiValuesParams {
            blockhash: StateAt::Latest,
            keys: keys.clone(),
        })?
        .values;
    let mut out = Vec::with_capacity(pairs.len());
    for ((id_value, prev_raw), key) in
        pairs.into_iter().zip(prev_vals.into_iter()).zip(keys.into_iter())
    {
        let (_id, value) = id_value;
        let mut entries = prev_raw
            .as_ref()
            .map(|raw| decode_versioned_bytes32_list(raw))
            .unwrap_or_default();
        if let Some(existing) =
            entries.iter_mut().find(|e| e.height == height && e.blockhash == *blockhash)
        {
            existing.value = value;
        } else {
            entries.push(VersionedBytes32EntryV1 { height, blockhash: *blockhash, value });
        }
        sort_versioned_bytes32_entries_desc(&mut entries);
        out.push((key, encode_versioned_bytes32_list(entries)?));
    }
    Ok(out)
}

pub(crate) fn build_new_outpoint_spent_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<u64, [u8; 32]>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<(u64, [u8; 32])> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = Vec::with_capacity(pairs.len());
    for (id, value) in pairs {
        out.push((
            table.outpoint_spent_by_id_point_key(id),
            encode_versioned_bytes32_list(vec![VersionedBytes32EntryV1 {
                height,
                blockhash: *blockhash,
                value,
            }])?,
        ));
    }
    Ok(out)
}

#[allow(dead_code)]
pub(crate) fn build_tx_pos_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<[u8; 32], u64>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<([u8; 32], u64)> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    note_tx_pointer_filter_updates(pairs.iter().map(|(txid, _)| *txid));
    let keys: Vec<Vec<u8>> = pairs
        .iter()
        .map(|(txid, _)| table.tx_packed_outflow_pos_point_key(txid))
        .collect();
    let prev_vals = provider
        .get_blob_multi_values(GetMultiValuesParams {
            blockhash: StateAt::Latest,
            keys: keys.clone(),
        })?
        .values;
    let mut out = Vec::with_capacity(pairs.len());
    for ((tx_value, prev_raw), key) in
        pairs.into_iter().zip(prev_vals.into_iter()).zip(keys.into_iter())
    {
        let (_txid, value) = tx_value;
        let mut entries =
            prev_raw.as_ref().map(|raw| decode_versioned_u64_list(raw)).unwrap_or_default();
        if let Some(existing) =
            entries.iter_mut().find(|e| e.height == height && e.blockhash == *blockhash)
        {
            existing.value = value;
        } else {
            entries.push(VersionedU64EntryV1 { height, blockhash: *blockhash, value });
        }
        sort_versioned_u64_entries_desc(&mut entries);
        out.push((key, encode_versioned_u64_list(entries)?));
    }
    Ok(out)
}

pub(crate) fn build_new_tx_pos_versioned_puts(
    provider: &EssentialsProvider,
    height: u32,
    blockhash: &[u8; 32],
    updates: &HashMap<[u8; 32], u64>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let mut pairs: Vec<([u8; 32], u64)> = updates.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    note_tx_pointer_filter_updates(pairs.iter().map(|(txid, _)| *txid));
    let mut out = Vec::with_capacity(pairs.len());
    for (txid, value) in pairs {
        out.push((
            table.tx_packed_outflow_pos_point_key(&txid),
            encode_versioned_u64_list(vec![VersionedU64EntryV1 {
                height,
                blockhash: *blockhash,
                value,
            }])?,
        ));
    }
    Ok(out)
}

pub(crate) fn resolve_outpoint_id_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    txid: &[u8; 32],
    vout: u32,
) -> Result<Option<u64>> {
    let key = provider.table().outpoint_pos_point_key_from_parts(txid, vout)?;
    let Some(raw) = provider
        .get_blob_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
        .value
    else {
        return Ok(None);
    };
    let entries = decode_versioned_u64_list(&raw);
    let target = resolve_target_blockhash(provider, blockhash);
    Ok(resolve_visible_u64_entry(provider, &entries, target))
}

pub(crate) fn resolve_outpoint_spent_by_id_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    outpoint_id: u64,
) -> Result<Option<[u8; 32]>> {
    let key = provider.table().outpoint_spent_by_id_point_key(outpoint_id);
    let Some(raw) = provider
        .get_blob_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
        .value
    else {
        return Ok(None);
    };
    let entries = decode_versioned_bytes32_list(&raw);
    let target = resolve_target_blockhash(provider, blockhash);
    Ok(resolve_visible_bytes32_entry(provider, &entries, target))
}

pub(crate) fn resolve_tx_pointer_id_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    txid: &[u8; 32],
) -> Result<Option<u64>> {
    let key = provider.table().tx_packed_outflow_pos_point_key(txid);
    let Some(raw) = provider
        .get_blob_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
        .value
    else {
        return Ok(None);
    };
    let entries = decode_versioned_u64_list(&raw);
    let target = resolve_target_blockhash(provider, blockhash);
    Ok(resolve_visible_u64_entry(provider, &entries, target))
}

pub(crate) fn resolve_tx_pointer_ids_batch_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    txids: &[[u8; 32]],
) -> Result<Vec<Option<u64>>> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let filter_enabled = ensure_tx_pointer_filter(provider)?;
    let mut lookup_indices: Vec<usize> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    if filter_enabled {
        let guard = tx_pointer_filter_lock()
            .read()
            .map_err(|_| anyhow!("tx pointer filter lock poisoned"))?;
        if let Some(filter) = guard.filter.as_ref() {
            for (idx, txid) in txids.iter().enumerate() {
                if filter.might_contain(txid) {
                    lookup_indices.push(idx);
                    keys.push(table.tx_packed_outflow_pos_point_key(txid));
                }
            }
        }
    } else {
        lookup_indices = (0..txids.len()).collect();
        keys = txids.iter().map(|txid| table.tx_packed_outflow_pos_point_key(txid)).collect();
    }
    let mut out = vec![None; txids.len()];
    if keys.is_empty() {
        return Ok(out);
    }
    let raws = provider
        .get_blob_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
        .values;
    let target = resolve_target_blockhash(provider, blockhash);
    let active_tip = provider
        .resolved_view_blockhash()
        .filter(|_| provider.view_blockhash().is_none());
    let fast_active = matches!(target, Some(t) if Some(t) == active_tip);
    let mut active_blockhash_by_height: HashMap<u32, Option<BlockHash>> = HashMap::new();
    for (out_idx, raw) in lookup_indices.into_iter().zip(raws.into_iter()) {
        let Some(raw) = raw else {
            continue;
        };
        let entries = decode_versioned_u64_list(&raw);
        if fast_active {
            let mut chosen: Option<u64> = None;
            for entry in entries {
                let active_at_height =
                    if let Some(cached) = active_blockhash_by_height.get(&entry.height) {
                        *cached
                    } else {
                        let found = provider.blockhash_for_height(entry.height).unwrap_or(None);
                        active_blockhash_by_height.insert(entry.height, found);
                        found
                    };
                if active_at_height.map(|h| h.to_byte_array()) == Some(entry.blockhash) {
                    chosen = Some(entry.value);
                    break;
                }
            }
            out[out_idx] = chosen;
        } else {
            out[out_idx] = resolve_visible_u64_entry(provider, &entries, target);
        }
    }
    Ok(out)
}

pub(crate) fn resolve_outpoint_ids_batch_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    outpoints: &[(Txid, u32)],
) -> Result<Vec<Option<u64>>> {
    if outpoints.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let keys: Vec<Vec<u8>> = outpoints
        .iter()
        .map(|(txid, vout)| table.outpoint_pos_point_key_from_parts(txid.as_byte_array(), *vout))
        .collect::<Result<Vec<_>>>()?;
    let raws = provider
        .get_blob_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
        .values;
    let target = resolve_target_blockhash(provider, blockhash);
    let active_tip = provider
        .resolved_view_blockhash()
        .filter(|_| provider.view_blockhash().is_none());
    let fast_active = matches!(target, Some(t) if Some(t) == active_tip);
    let mut active_blockhash_by_height: HashMap<u32, Option<BlockHash>> = HashMap::new();
    let mut out = Vec::with_capacity(outpoints.len());
    for raw in raws {
        let Some(raw) = raw else {
            out.push(None);
            continue;
        };
        let entries = decode_versioned_u64_list(&raw);
        if fast_active {
            let mut chosen: Option<u64> = None;
            for entry in entries {
                let active_at_height =
                    if let Some(cached) = active_blockhash_by_height.get(&entry.height) {
                        *cached
                    } else {
                        let found = provider.blockhash_for_height(entry.height).unwrap_or(None);
                        active_blockhash_by_height.insert(entry.height, found);
                        found
                    };
                if active_at_height.map(|h| h.to_byte_array()) == Some(entry.blockhash) {
                    chosen = Some(entry.value);
                    break;
                }
            }
            out.push(chosen);
        } else {
            out.push(resolve_visible_u64_entry(provider, &entries, target));
        }
    }
    Ok(out)
}

pub(crate) fn resolve_outpoint_spent_by_ids_batch_v2(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    outpoint_ids: &[u64],
) -> Result<Vec<Option<[u8; 32]>>> {
    if outpoint_ids.is_empty() {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let keys: Vec<Vec<u8>> = outpoint_ids
        .iter()
        .map(|id| table.outpoint_spent_by_id_point_key(*id))
        .collect();
    let raws = provider
        .get_blob_multi_values(GetMultiValuesParams { blockhash: StateAt::Latest, keys })?
        .values;
    let target = resolve_target_blockhash(provider, blockhash);
    let active_tip = provider
        .resolved_view_blockhash()
        .filter(|_| provider.view_blockhash().is_none());
    let fast_active = matches!(target, Some(t) if Some(t) == active_tip);
    let mut active_blockhash_by_height: HashMap<u32, Option<BlockHash>> = HashMap::new();
    let mut out = Vec::with_capacity(outpoint_ids.len());
    for raw in raws {
        let Some(raw) = raw else {
            out.push(None);
            continue;
        };
        let entries = decode_versioned_bytes32_list(&raw);
        if fast_active {
            let mut chosen: Option<[u8; 32]> = None;
            for entry in entries {
                let active_at_height =
                    if let Some(cached) = active_blockhash_by_height.get(&entry.height) {
                        *cached
                    } else {
                        let found = provider.blockhash_for_height(entry.height).unwrap_or(None);
                        active_blockhash_by_height.insert(entry.height, found);
                        found
                    };
                if active_at_height.map(|h| h.to_byte_array()) == Some(entry.blockhash) {
                    chosen = Some(entry.value);
                    break;
                }
            }
            out.push(chosen);
        } else {
            out.push(resolve_visible_bytes32_entry(provider, &entries, target));
        }
    }
    Ok(out)
}

fn decode_address_index_state(bytes: &[u8]) -> Option<InlineOrExternalU64V1> {
    InlineOrExternalU64V1::try_from_slice(bytes).ok()
}

fn encode_address_index_state(state: &InlineOrExternalU64V1) -> Result<Vec<u8>> {
    borsh::to_vec(state).map_err(|e| anyhow!("encode address index state failed: {e}"))
}

fn decode_u64_chunk(bytes: &[u8]) -> Vec<u64> {
    U64ChunkV1::try_from_slice(bytes).map(|chunk| chunk.items).unwrap_or_default()
}

fn encode_u64_chunk(items: Vec<u64>) -> Result<Vec<u8>> {
    borsh::to_vec(&U64ChunkV1 { items })
        .map_err(|e| anyhow!("encode address index chunk failed: {e}"))
}

fn address_index_total(state: &InlineOrExternalU64V1) -> u64 {
    match state {
        InlineOrExternalU64V1::Inline { items } => items.len() as u64,
        InlineOrExternalU64V1::External { len, .. } => *len,
    }
}

pub fn get_address_index_list_len(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    kind: AddressIndexListKind,
    address: &str,
) -> Result<u64> {
    let key = provider.table().address_index_meta_key(address, kind);
    let raw = provider
        .get_raw_value(GetRawValueParams { blockhash, key })
        .map(|resp| resp.value)
        .ok()
        .flatten();
    let Some(raw) = raw else {
        return Ok(0);
    };
    let Some(state) = decode_address_index_state(&raw) else {
        return Ok(0);
    };
    Ok(address_index_total(&state))
}

pub fn get_address_index_list_range(
    provider: &EssentialsProvider,
    blockhash: StateAt,
    kind: AddressIndexListKind,
    address: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u64>> {
    if end <= start {
        return Ok(Vec::new());
    }
    let table = provider.table();
    let key = table.address_index_meta_key(address, kind);
    let raw = provider.get_raw_value(GetRawValueParams { blockhash, key })?.value;
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let Some(state) = decode_address_index_state(&raw) else {
        return Ok(Vec::new());
    };
    let total = address_index_total(&state);
    if total == 0 {
        return Ok(Vec::new());
    }
    let start = start.min(total);
    let end = end.min(total);
    if end <= start {
        return Ok(Vec::new());
    }

    match state {
        InlineOrExternalU64V1::Inline { items } => {
            let from = usize::try_from(start).unwrap_or(usize::MAX).min(items.len());
            let to = usize::try_from(end).unwrap_or(usize::MAX).min(items.len());
            if to <= from {
                return Ok(Vec::new());
            }
            Ok(items[from..to].to_vec())
        }
        InlineOrExternalU64V1::External { chunk_ids, chunk_size, .. } => {
            let chunk_size_u64 = u64::from(chunk_size.max(1));
            let first_chunk = usize::try_from(start / chunk_size_u64).unwrap_or(usize::MAX);
            let mut last_chunk_excl =
                usize::try_from((end + chunk_size_u64 - 1) / chunk_size_u64).unwrap_or(usize::MAX);
            if first_chunk >= chunk_ids.len() {
                return Ok(Vec::new());
            }
            last_chunk_excl = last_chunk_excl.min(chunk_ids.len());
            if last_chunk_excl <= first_chunk {
                return Ok(Vec::new());
            }

            let chunk_slice = &chunk_ids[first_chunk..last_chunk_excl];
            let chunk_keys: Vec<Vec<u8>> = chunk_slice
                .iter()
                .map(|id| table.address_index_chunk_blob_key(kind, *id))
                .collect();
            let chunk_vals = provider.get_blob_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: chunk_keys,
            })?;

            let mut out: Vec<u64> =
                Vec::with_capacity(usize::try_from(end.saturating_sub(start)).unwrap_or(0));
            for (offset, raw_chunk) in chunk_vals.values.into_iter().enumerate() {
                let Some(raw_chunk) = raw_chunk else { continue };
                let items = decode_u64_chunk(&raw_chunk);
                if items.is_empty() {
                    continue;
                }
                let global_chunk_idx = first_chunk.saturating_add(offset);
                let chunk_start = (global_chunk_idx as u64).saturating_mul(chunk_size_u64);
                let from = usize::try_from(start.saturating_sub(chunk_start))
                    .unwrap_or(usize::MAX)
                    .min(items.len());
                let max_to = usize::try_from(end.saturating_sub(chunk_start)).unwrap_or(usize::MAX);
                let to = max_to.min(items.len());
                if to > from {
                    out.extend_from_slice(&items[from..to]);
                }
            }
            Ok(out)
        }
    }
}

pub fn append_address_index_values(
    provider: &EssentialsProvider,
    kind: AddressIndexListKind,
    address: &str,
    values: &[u64],
    next_chunk_id: &mut u64,
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    blob_puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<u64> {
    let table = provider.table();
    let meta_key = table.address_index_meta_key(address, kind);
    let current = provider
        .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: meta_key.clone() })?
        .value
        .and_then(|raw| decode_address_index_state(&raw))
        .unwrap_or_else(|| InlineOrExternalU64V1::Inline { items: Vec::new() });

    if values.is_empty() {
        return Ok(address_index_total(&current));
    }

    let next_state = match current {
        InlineOrExternalU64V1::Inline { mut items } => {
            if items.len().saturating_add(values.len()) <= ADDRESS_INDEX_INLINE_CAP {
                items.extend_from_slice(values);
                InlineOrExternalU64V1::Inline { items }
            } else {
                let chunk_size = get_address_index_chunk_size().max(1);
                let mut merged = Vec::with_capacity(items.len().saturating_add(values.len()));
                merged.append(&mut items);
                merged.extend_from_slice(values);

                let mut chunk_ids: Vec<u64> = Vec::new();
                for chunk in merged.chunks(chunk_size) {
                    let id = *next_chunk_id;
                    *next_chunk_id = next_chunk_id.saturating_add(1);
                    chunk_ids.push(id);
                    blob_puts.push((
                        table.address_index_chunk_blob_key(kind, id),
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
                    let last_key = table.address_index_chunk_blob_key(kind, last_chunk_id);
                    let mut last_items = provider
                        .get_blob_raw_value(GetRawValueParams {
                            blockhash: StateAt::Latest,
                            key: last_key.clone(),
                        })?
                        .value
                        .map(|raw| decode_u64_chunk(&raw))
                        .unwrap_or_default();
                    if last_items.len() > rem {
                        last_items.truncate(rem);
                    }
                    if last_items.len() < rem {
                        return Err(anyhow!(
                            "address index chunk {} for {:?} is shorter than canonical length {}",
                            last_chunk_id,
                            kind,
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
                    table.address_index_chunk_blob_key(kind, id),
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

    let new_len = address_index_total(&next_state);
    puts.push((meta_key, encode_address_index_state(&next_state)?));
    Ok(new_len)
}

pub fn encode_tx_pointer_blob_v3(
    txid: &[u8; 32],
    blockhash: &[u8; 32],
    tx_idx: u32,
    height: u32,
    traces: &[EspoSandshrewLikeTrace],
    outflows: &BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
) -> Result<Vec<u8>> {
    borsh::to_vec(&TxPointerBlobV3 {
        txid: *txid,
        blockhash: *blockhash,
        tx_idx,
        height,
        traces: traces.to_vec(),
        outflows: outflows.clone(),
    })
    .map_err(|e| anyhow!("encode tx pointer blob v3 failed: {e}"))
}

pub fn decode_tx_pointer_blob_v3(bytes: &[u8]) -> Result<TxPointerBlobV3> {
    TxPointerBlobV3::try_from_slice(bytes)
        .map_err(|e| anyhow!("decode tx pointer blob v3 failed: {e}"))
}

pub fn encode_outpoint_pointer_blob_v3(
    txid: &[u8; 32],
    vout: u32,
    blockhash: &[u8; 32],
    tx_idx: u32,
    address: &str,
    spk: &[u8],
    balances: &Vec<BalanceEntry>,
) -> Result<Vec<u8>> {
    borsh::to_vec(&OutpointPointerBlobV3 {
        txid: *txid,
        vout,
        blockhash: *blockhash,
        tx_idx,
        address: address.to_string(),
        spk: spk.to_vec(),
        balances: balances.clone(),
    })
    .map_err(|e| anyhow!("encode outpoint pointer blob v3 failed: {e}"))
}

pub fn decode_outpoint_pointer_blob_v3(bytes: &[u8]) -> Result<OutpointPointerBlobV3> {
    OutpointPointerBlobV3::try_from_slice(bytes)
        .map_err(|e| anyhow!("decode outpoint pointer blob v3 failed: {e}"))
}

pub fn encode_outpoint_row_v2(
    address: &str,
    spk: &[u8],
    balances: &Vec<BalanceEntry>,
) -> Result<Vec<u8>> {
    borsh::to_vec(&OutpointRowV2 {
        address: address.to_string(),
        spk: spk.to_vec(),
        balances: balances.clone(),
    })
    .map_err(|e| anyhow!("encode outpoint row v2 failed: {e}"))
}

pub fn decode_outpoint_row_v2(bytes: &[u8]) -> Result<OutpointRowV2> {
    OutpointRowV2::try_from_slice(bytes).map_err(|e| anyhow!("decode outpoint row v2 failed: {e}"))
}

pub fn decode_holders_vec(bytes: &[u8]) -> Result<Vec<HolderEntry>> {
    if let Ok(parsed) = Vec::<HolderEntry>::try_from_slice(bytes) {
        return Ok(parsed);
    }

    #[derive(BorshDeserialize)]
    struct LegacyHolderEntry {
        address: String,
        amount: u128,
    }

    let legacy: Vec<LegacyHolderEntry> = Vec::<LegacyHolderEntry>::try_from_slice(bytes)?;
    Ok(legacy
        .into_iter()
        .map(|h| HolderEntry { holder: HolderId::Address(h.address), amount: h.amount })
        .collect())
}

fn normalize_alkane_id_vec(alkanes: &mut Vec<SchemaAlkaneId>) {
    alkanes.sort();
    alkanes.dedup();
}

pub fn decode_orbital_holder_entry(bytes: &[u8]) -> Result<OrbitalHolderEntry> {
    let mut entry = OrbitalHolderEntry::try_from_slice(bytes)?;
    normalize_alkane_id_vec(&mut entry.alkanes);
    entry.amount = entry.alkanes.len() as u128;
    Ok(entry)
}

pub fn encode_orbital_holder_entry(mut entry: OrbitalHolderEntry) -> Result<Vec<u8>> {
    normalize_alkane_id_vec(&mut entry.alkanes);
    entry.amount = entry.alkanes.len() as u128;
    Ok(borsh::to_vec(&entry)?)
}

pub fn decode_address_orbital_balance_entries(
    bytes: &[u8],
) -> Result<Vec<AddressOrbitalBalanceEntry>> {
    let mut entries = Vec::<AddressOrbitalBalanceEntry>::try_from_slice(bytes)?;
    for entry in &mut entries {
        normalize_alkane_id_vec(&mut entry.alkanes);
        entry.amount = entry.alkanes.len() as u128;
    }
    entries.sort_by(|a, b| a.factory.cmp(&b.factory));
    Ok(entries)
}

pub fn encode_address_orbital_balance_entries(
    mut entries: Vec<AddressOrbitalBalanceEntry>,
) -> Result<Vec<u8>> {
    for entry in &mut entries {
        normalize_alkane_id_vec(&mut entry.alkanes);
        entry.amount = entry.alkanes.len() as u128;
    }
    entries.retain(|entry| !entry.alkanes.is_empty());
    entries.sort_by(|a, b| a.factory.cmp(&b.factory));
    Ok(borsh::to_vec(&entries)?)
}

pub fn decode_address_amount_vec(bytes: &[u8]) -> Result<Vec<AddressAmountEntry>> {
    Ok(Vec::<AddressAmountEntry>::try_from_slice(bytes)?)
}

pub fn encode_address_amount_vec(entries: &Vec<AddressAmountEntry>) -> Result<Vec<u8>> {
    encode_vec(entries)
}

pub fn decode_address_activity_entry(bytes: &[u8]) -> Result<AddressActivityEntry> {
    Ok(AddressActivityEntry::try_from_slice(bytes)?)
}

pub fn encode_address_activity_entry(entry: &AddressActivityEntry) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(entry)?)
}

pub fn decode_address_contract_amount_entries(
    bytes: &[u8],
) -> Result<Vec<AddressContractAmountEntry>> {
    Ok(Vec::<AddressContractAmountEntry>::try_from_slice(bytes)?)
}

pub fn encode_address_contract_amount_entries(
    entries: &Vec<AddressContractAmountEntry>,
) -> Result<Vec<u8>> {
    encode_vec(entries)
}

pub fn encode_alkane_info(info: &AlkaneInfo) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(info)?)
}

pub fn decode_alkane_info(bytes: &[u8]) -> Result<AlkaneInfo> {
    Ok(AlkaneInfo::try_from_slice(bytes)?)
}

pub fn encode_u128_value(value: u128) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(&value)?)
}

pub fn decode_u128_value(bytes: &[u8]) -> Result<u128> {
    Ok(u128::try_from_slice(bytes)?)
}

pub fn encode_creation_record(record: &AlkaneCreationRecord) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(record)?)
}

pub fn decode_creation_record(bytes: &[u8]) -> Result<AlkaneCreationRecord> {
    // Try new schema first; fall back to legacy Option name/symbol layout.
    if let Ok(rec) = AlkaneCreationRecord::try_from_slice(bytes) {
        return Ok(rec);
    }

    #[derive(BorshDeserialize)]
    struct LegacyCreationRecordV2 {
        alkane: SchemaAlkaneId,
        txid: [u8; 32],
        creation_height: u32,
        creation_timestamp: u32,
        tx_index_in_block: u32,
        inspection: Option<crate::modules::essentials::utils::inspections::StoredInspectionResult>,
        names: Vec<String>,
        symbols: Vec<String>,
    }

    if let Ok(legacy) = LegacyCreationRecordV2::try_from_slice(bytes) {
        return Ok(AlkaneCreationRecord {
            alkane: legacy.alkane,
            txid: legacy.txid,
            creation_height: legacy.creation_height,
            creation_timestamp: legacy.creation_timestamp,
            tx_index_in_block: legacy.tx_index_in_block,
            inspection: legacy.inspection,
            names: legacy.names,
            symbols: legacy.symbols,
            cap: 0,
            mint_amount: 0,
        });
    }

    #[derive(BorshDeserialize)]
    struct LegacyCreationRecord {
        alkane: SchemaAlkaneId,
        txid: [u8; 32],
        creation_height: u32,
        creation_timestamp: u32,
        tx_index_in_block: u32,
        inspection: Option<crate::modules::essentials::utils::inspections::StoredInspectionResult>,
        name: Option<String>,
        symbol: Option<String>,
    }

    let legacy = LegacyCreationRecord::try_from_slice(bytes)?;
    let mut names = Vec::new();
    let mut symbols = Vec::new();
    if let Some(n) = legacy.name {
        names.push(n);
    }
    if let Some(s) = legacy.symbol {
        symbols.push(s);
    }
    Ok(AlkaneCreationRecord {
        alkane: legacy.alkane,
        txid: legacy.txid,
        creation_height: legacy.creation_height,
        creation_timestamp: legacy.creation_timestamp,
        tx_index_in_block: legacy.tx_index_in_block,
        inspection: legacy.inspection,
        names,
        symbols,
        cap: 0,
        mint_amount: 0,
    })
}

pub fn load_creation_record(
    mdb: &crate::runtime::mdb::Mdb,
    alkane: &SchemaAlkaneId,
) -> Result<Option<AlkaneCreationRecord>> {
    let table = EssentialsTable::new(mdb);
    let key = table.alkane_creation_by_id_key(alkane);
    if let Some(bytes) = mdb.get(&key)? {
        let record = decode_creation_record(&bytes)?;
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

pub fn get_holders_count_encoded(count: u64) -> Result<Vec<u8>> {
    crate::debug_timer_log!("get_holders_count_encoded");
    let count_value = HoldersCountEntry { count };

    Ok(borsh::to_vec(&count_value)?)
}

pub fn get_holders_values_encoded(holders: Vec<HolderEntry>) -> Result<(Vec<u8>, Vec<u8>)> {
    crate::debug_timer_log!("get_holders_values_encoded");
    Ok((encode_vec(&holders)?, get_holders_count_encoded(holders.len().try_into()?)?))
}

/// Build the key for alkane balances (public helper for strict mode validation)
pub fn build_alkane_balances_key(owner: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = ALKANE_V2_PREFIX.to_vec();
    key.extend_from_slice(&encode_alkane_id_be(owner));
    key.extend_from_slice(b"/balance/");
    key
}

fn decode_u32_le(bytes: &[u8]) -> Option<u32> {
    if bytes.len() != 4 {
        return None;
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Some(u32::from_le_bytes(arr))
}

const TX_TRACE_ROW_COMPACT_V1: u8 = 1;

#[derive(BorshSerialize, BorshDeserialize)]
struct CompactTxTraceRowV1 {
    vout: u32,
    events: Vec<EspoSandshrewLikeTraceEvent>,
}

fn parse_trace_outpoint_vout(outpoint: &str) -> Option<u32> {
    outpoint.rsplit_once(':').and_then(|(_, vout)| vout.parse::<u32>().ok())
}

pub fn encode_tx_trace_row(trace: &EspoSandshrewLikeTrace, compact: bool) -> Result<Vec<u8>> {
    if compact {
        if let Some(vout) = parse_trace_outpoint_vout(&trace.outpoint) {
            let compact_row = CompactTxTraceRowV1 { vout, events: trace.events.clone() };
            let mut bytes = Vec::with_capacity(1 + trace.events.len() * 16);
            bytes.push(TX_TRACE_ROW_COMPACT_V1);
            bytes.extend_from_slice(&borsh::to_vec(&compact_row)?);
            return Ok(bytes);
        }
    }
    Ok(borsh::to_vec(trace)?)
}

pub(crate) fn load_tx_packed_outflow_v2(
    provider: &EssentialsProvider,
    txid: &Txid,
) -> Option<TxPackedOutflowRowV2> {
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(txid.as_byte_array());
    let id = resolve_tx_pointer_id_v2(provider, StateAt::Latest, &txid_arr).ok().flatten()?;
    if let Some(blob) = load_tx_pointer_blob_v3_by_id(provider, id) {
        return Some(TxPackedOutflowRowV2 {
            height: blob.height,
            traces: blob.traces,
            outflows: blob.outflows,
        });
    }
    None
}

pub(crate) fn load_tx_pointer_blob_v3_by_id(
    provider: &EssentialsProvider,
    id: u64,
) -> Option<TxPointerBlobV3> {
    let row_key = provider.table().tx_pointer_blob_key(id);
    let row_raw = provider
        .get_blob_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: row_key })
        .ok()?
        .value?;
    decode_tx_pointer_blob_v3(&row_raw).ok()
}

pub(crate) fn load_outpoint_pointer_blob_v3_by_id(
    provider: &EssentialsProvider,
    id: u64,
) -> Option<OutpointPointerBlobV3> {
    let row_key = provider.table().outpoint_pointer_blob_key(id);
    let row_raw = provider
        .get_blob_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key: row_key })
        .ok()?
        .value?;
    decode_outpoint_pointer_blob_v3(&row_raw).ok()
}

pub(crate) fn load_tx_summary_v2(
    provider: &EssentialsProvider,
    txid: &Txid,
) -> Option<AlkaneTxSummary> {
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(txid.as_byte_array());
    let packed = load_tx_packed_outflow_v2(provider, txid)?;
    Some(tx_summary_from_parts(txid_arr, packed.height, packed.traces, packed.outflows))
}

fn tx_summary_from_pointer_blob(blob: TxPointerBlobV3) -> AlkaneTxSummary {
    tx_summary_from_parts(blob.txid, blob.height, blob.traces, blob.outflows)
}

fn tx_summary_from_parts(
    txid: [u8; 32],
    height: u32,
    traces: Vec<EspoSandshrewLikeTrace>,
    outflows_by_owner: BTreeMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
) -> AlkaneTxSummary {
    let mut outflows = Vec::with_capacity(outflows_by_owner.len());
    for (_owner, outflow_map) in outflows_by_owner {
        outflows.push(AlkaneBalanceTxEntry { txid, height, outflow: outflow_map });
    }
    AlkaneTxSummary { txid, traces, outflows, height }
}

fn mem_block_tx_to_json(entry: &MempoolBlockTx) -> Value {
    let traces_json = entry
        .traces
        .as_ref()
        .map(|traces| {
            traces
                .iter()
                .map(|trace| mempool_trace_to_json(&entry.txid, trace))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let protostones = entry.protostones.iter().map(protostone_to_value).collect::<Vec<_>>();
    let mempool_block = entry.position.as_ref().map(|position| position.block);
    let mempool_position_vsize = entry.position.as_ref().map(|position| position.vsize);

    json!({
        "txid": entry.txid.to_string(),
        "first_seen": entry.first_seen,
        "mempool_block": mempool_block,
        "mempool_position_vsize": mempool_position_vsize,
        "fee_sat": entry.fee_sat,
        "vsize": entry.vsize,
        "fee_paid": entry.fee_rate,
        "fee_rate": entry.fee_rate,
        "readiness": &entry.readiness,
        "defer_alkane_trace_status": entry.defer_alkane_trace_status,
        "has_protostones": !protostones.is_empty(),
        "hasProtostones": !protostones.is_empty(),
        "protostone": Value::Array(protostones.clone()),
        "protostones": Value::Array(protostones),
        "traces": traces_json,
    })
}

fn mempool_trace_to_json(txid: &Txid, trace: &EspoTrace) -> Value {
    let events_val = prettyify_protobuf_trace_json(&trace.protobuf_trace)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or(Value::Null);
    json!({
        "outpoint": format!("{}:{}", txid, trace.outpoint.vout),
        "events": events_val,
    })
}

fn spendable_outpoints_runes_provider() -> &'static RunesProvider {
    static PROVIDER: OnceLock<RunesProvider> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        let mdb = Arc::new(Mdb::from_db(get_espo_db(), b"runes:"));
        RunesProvider::new(mdb)
    })
}

fn rune_balances_to_json(balances: &[RuneBalance]) -> Vec<Value> {
    balances
        .iter()
        .filter(|balance| balance.amount > 0)
        .map(|balance| {
            json!({
                "id": balance.id.to_string(),
                "rune": balance.id.to_string(),
                "amount": balance.amount.to_string(),
            })
        })
        .collect()
}

fn normalize_address(s: &str) -> Option<String> {
    let network = get_network();
    Address::from_str(s)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.to_string())
}

struct AddressTxRender {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
    block_height: Option<u64>,
    block_time: Option<u64>,
    confirmations: Option<u64>,
    is_mempool: bool,
}

fn enriched_transaction_json(
    render: &AddressTxRender,
    prev_map: &HashMap<Txid, Transaction>,
    network: Network,
) -> Value {
    let tx = &render.tx;
    let mut input_sum: u64 = 0;
    let mut inputs: Vec<Value> = Vec::new();

    for vin in &tx.input {
        let mut obj = Map::new();
        obj.insert("txid".to_string(), json!(vin.previous_output.txid.to_string()));
        obj.insert("vout".to_string(), json!(vin.previous_output.vout));
        if vin.previous_output.is_null() {
            obj.insert("isCoinbase".to_string(), json!(true));
        } else if let Some(prev_tx) = prev_map.get(&vin.previous_output.txid) {
            if let Some(prev_out) = prev_tx.output.get(vin.previous_output.vout as usize) {
                input_sum = input_sum.saturating_add(prev_out.value.to_sat());
                obj.insert("amount".to_string(), json!(prev_out.value.to_sat()));
                if let Ok(addr) = Address::from_script(prev_out.script_pubkey.as_script(), network)
                {
                    obj.insert("address".to_string(), json!(addr.to_string()));
                }
            }
        }
        inputs.push(Value::Object(obj));
    }

    let mut output_sum: u64 = 0;
    let mut outputs: Vec<Value> = Vec::new();
    for out in &tx.output {
        let mut obj = Map::new();
        obj.insert("amount".to_string(), json!(out.value.to_sat()));
        obj.insert("scriptPubKey".to_string(), json!(hex::encode(out.script_pubkey.as_bytes())));
        if let Ok(addr) = Address::from_script(out.script_pubkey.as_script(), network) {
            obj.insert("address".to_string(), json!(addr.to_string()));
        }
        if let Some(script_type) = script_type_label(&out.script_pubkey, network) {
            obj.insert("scriptPubKeyType".to_string(), json!(script_type));
        }
        outputs.push(Value::Object(obj));
        output_sum = output_sum.saturating_add(out.value.to_sat());
    }

    let fee = if tx.is_coinbase() || input_sum < output_sum {
        None
    } else {
        Some(input_sum - output_sum)
    };
    let (runestone, protostones) = runestone_data(tx);
    let has_protostones = !protostones.is_empty();
    let alkanes_traces = render.traces.as_ref().and_then(|traces| {
        let vals = traces.iter().map(enriched_trace_to_value).collect::<Vec<_>>();
        if vals.is_empty() { None } else { Some(Value::Array(vals)) }
    });

    let mut out = Map::new();
    out.insert("txid".to_string(), json!(render.txid.to_string()));
    out.insert("blockHeight".to_string(), json!(render.block_height));
    out.insert("confirmations".to_string(), json!(render.confirmations));
    out.insert("blockTime".to_string(), json!(render.block_time));
    out.insert("confirmed".to_string(), json!(!render.is_mempool));
    out.insert("fee".to_string(), fee.map(|value| json!(value)).unwrap_or(Value::Null));
    out.insert("weight".to_string(), json!(tx.weight().to_wu()));
    out.insert("size".to_string(), json!(serialize(tx).len() as u64));
    out.insert("inputs".to_string(), Value::Array(inputs));
    out.insert("outputs".to_string(), Value::Array(outputs));
    out.insert("hasOpReturn".to_string(), json!(tx_has_op_return(tx)));
    out.insert("hasProtostones".to_string(), json!(has_protostones));
    out.insert("isRbf".to_string(), json!(tx.is_explicitly_rbf()));
    out.insert("isCoinbase".to_string(), json!(tx.is_coinbase()));
    if let Some(runestone) = runestone {
        out.insert("runestone".to_string(), runestone);
    }
    if let Some(alkane_traces) = alkanes_traces {
        out.insert("alkanesTraces".to_string(), alkane_traces);
    }

    Value::Object(out)
}

fn runestone_data(tx: &Transaction) -> (Option<Value>, Vec<Value>) {
    if let Some(Artifact::Runestone(runestone)) = Runestone::decipher(tx) {
        let protostones = Protostone::from_runestone(&runestone).unwrap_or_default();
        let protos_json = protostones.iter().map(protostone_to_value).collect::<Vec<_>>();
        if let Value::Object(mut map) = serde_json::to_value(&runestone).unwrap_or(Value::Null) {
            map.insert("protostones".to_string(), Value::Array(protos_json.clone()));
            return (Some(Value::Object(map)), protos_json);
        }
        let mut map = Map::new();
        map.insert("protostones".to_string(), Value::Array(protos_json.clone()));
        return (Some(Value::Object(map)), protos_json);
    }
    (None, Vec::new())
}

fn protostone_to_value(protostone: &Protostone) -> Value {
    let edicts = protostone
        .edicts
        .iter()
        .map(|edict| {
            json!({
                "id": {
                    "block": edict.id.block,
                    "tx": edict.id.tx,
                },
                "amount": edict.amount,
                "output": edict.output,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "burn": protostone.burn,
        "message": hex::encode(&protostone.message),
        "edicts": edicts,
        "refund": protostone.refund,
        "pointer": protostone.pointer,
        "from": protostone.from,
        "protocol_tag": protostone.protocol_tag,
    })
}

fn enriched_trace_to_value(trace: &EspoTrace) -> Value {
    let txid = Txid::from_slice(&trace.outpoint.txid)
        .map(|t| t.to_string())
        .unwrap_or_default();
    let protostone_index = trace
        .sandshrew_trace
        .events
        .iter()
        .filter_map(|event| match event {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => Some(inv.context.vout),
            _ => None,
        })
        .next()
        .unwrap_or(0);
    let trace_events = if trace.protobuf_trace.events.is_empty() {
        serde_json::to_value(&trace.sandshrew_trace.events).unwrap_or(Value::Null)
    } else {
        serde_json::to_value(&trace.protobuf_trace.events).unwrap_or(Value::Null)
    };
    json!({
        "vout": trace.outpoint.vout,
        "outpoint": format!("{txid}:{}", trace.outpoint.vout),
        "protostone_index": protostone_index,
        "trace": {
            "events": trace_events,
        },
    })
}

fn script_type_label(spk: &ScriptBuf, network: Network) -> Option<&'static str> {
    Address::from_script(spk.as_script(), network)
        .ok()
        .and_then(|a| match a.address_type() {
            Some(AddressType::P2pkh) => Some("P2PKH"),
            Some(AddressType::P2sh) => Some("P2SH"),
            Some(AddressType::P2wpkh) => Some("P2WPKH"),
            Some(AddressType::P2wsh) => Some("P2WSH"),
            Some(AddressType::P2tr) => Some("P2TR"),
            _ => None,
        })
}

fn tx_has_op_return(tx: &Transaction) -> bool {
    tx.output.iter().any(|out| {
        let bytes = out.script_pubkey.as_bytes();
        !bytes.is_empty() && bytes[0] == bitcoin::opcodes::all::OP_RETURN.to_u8()
    })
}

fn traces_from_summary(txid: &Txid, summary: &AlkaneTxSummary) -> Vec<EspoTrace> {
    summary
        .traces
        .iter()
        .filter_map(|trace| sandshrew_to_espo_trace(txid, trace))
        .collect()
}

fn sandshrew_traces_first_invoke_matches_filter(
    traces: &[EspoSandshrewLikeTrace],
    filter: &SchemaAlkaneId,
) -> bool {
    let Some(trace) = traces.first() else {
        return false;
    };
    trace_first_invoke_matches_filter(&trace.events, filter)
}

fn espo_traces_first_invoke_matches_filter(traces: &[EspoTrace], filter: &SchemaAlkaneId) -> bool {
    let Some(trace) = traces.first() else {
        return false;
    };
    trace_first_invoke_matches_filter(&trace.sandshrew_trace.events, filter)
}

fn trace_first_invoke_matches_filter(
    events: &[EspoSandshrewLikeTraceEvent],
    filter: &SchemaAlkaneId,
) -> bool {
    let Some(EspoSandshrewLikeTraceEvent::Invoke(invoke)) = events.first() else {
        return false;
    };
    parse_trace_id_u32(&invoke.context.myself.block) == Some(filter.block)
        && parse_trace_id_u64(&invoke.context.myself.tx) == Some(filter.tx)
}

fn parse_trace_id_u32(s: &str) -> Option<u32> {
    parse_trace_id_u128(s)?.try_into().ok()
}

fn parse_trace_id_u64(s: &str) -> Option<u64> {
    parse_trace_id_u128(s)?.try_into().ok()
}

fn parse_trace_id_u128(s: &str) -> Option<u128> {
    let s = s.trim();
    if let Some(x) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u128::from_str_radix(x, 16).ok()
    } else {
        s.parse::<u128>().ok()
    }
}

fn sandshrew_to_espo_trace(txid: &Txid, trace: &EspoSandshrewLikeTrace) -> Option<EspoTrace> {
    let (txid_hex, vout_s) = trace.outpoint.split_once(':')?;
    let vout = vout_s.parse::<u32>().ok()?;
    let trace_txid = Txid::from_str(txid_hex).unwrap_or(*txid);
    Some(EspoTrace {
        sandshrew_trace: trace.clone(),
        protobuf_trace: AlkanesTrace::default(),
        storage_changes: HashMap::new(),
        outpoint: EspoOutpoint { txid: trace_txid.to_byte_array().to_vec(), vout, tx_spent: None },
    })
}

fn parse_alkane_from_str(s: &str) -> Option<SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let parse_u32 = |t: &str| {
        if let Some(x) = t.strip_prefix("0x") {
            u32::from_str_radix(x, 16).ok()
        } else {
            t.parse::<u32>().ok()
        }
    };
    let parse_u64 = |t: &str| {
        if let Some(x) = t.strip_prefix("0x") {
            u64::from_str_radix(x, 16).ok()
        } else {
            t.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(parts[0])?, tx: parse_u64(parts[1])? })
}

/// Encode a wasm payload for JSON transport. Returns (encoding label,
/// payload byte length before base64, base64 string).
fn encode_wasm_payload(wasm: &[u8], gzip: bool) -> Result<(&'static str, usize, String)> {
    use base64::Engine as _;
    let (encoding, payload) = if gzip {
        ("base64+gzip", std::borrow::Cow::Owned(alkanes_support::gz::compress(wasm.to_vec())?))
    } else {
        ("base64", std::borrow::Cow::Borrowed(wasm))
    };
    let encoded = base64::engine::general_purpose::STANDARD.encode(payload.as_ref());
    Ok((encoding, payload.len(), encoded))
}

fn wasm_sha256_hex(wasm: &[u8]) -> String {
    use bitcoin::hashes::{Hash as _, sha256};
    hex::encode(sha256::Hash::hash(wasm).to_byte_array())
}

const MAX_KV_SEARCH_CONDITIONS: usize = 8;
const MAX_KV_SEARCH_CHILDREN: usize = 50_000;
const MAX_KV_SEARCH_LIMIT: usize = 1000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum KvSearchOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Exists,
}

impl KvSearchOp {
    fn parse(raw: &str) -> Option<Self> {
        Some(match raw.to_ascii_lowercase().as_str() {
            "eq" | "=" | "==" => Self::Eq,
            "ne" | "!=" | "<>" => Self::Ne,
            "gt" | ">" => Self::Gt,
            "lt" | "<" => Self::Lt,
            "ge" | ">=" => Self::Ge,
            "le" | "<=" => Self::Le,
            "exists" => Self::Exists,
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Gt => "gt",
            Self::Lt => "lt",
            Self::Ge => "ge",
            Self::Le => "le",
            Self::Exists => "exists",
        }
    }
}

struct KvSearchCondition {
    key: Vec<u8>,
    op: KvSearchOp,
    /// Comparison payload for eq/ne (raw bytes, hex `0x…` or literal string).
    value_bytes: Option<Vec<u8>>,
    /// Comparison payload for gt/lt/ge/le; stored values decode as LE u128.
    value_u128: Option<u128>,
}

impl KvSearchCondition {
    fn parse(raw: &Value) -> std::result::Result<Self, String> {
        let obj = raw
            .as_object()
            .ok_or_else(|| "each condition must be an object {key, op, value}".to_string())?;
        let key_raw = obj
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "condition.key must be a string (utf8 or 0x-hex)".to_string())?;
        let key = parse_key_str_to_bytes(key_raw)
            .ok_or_else(|| format!("condition.key could not be parsed: {key_raw}"))?;
        let op_raw = obj.get("op").and_then(|v| v.as_str()).unwrap_or("eq");
        let op = KvSearchOp::parse(op_raw)
            .ok_or_else(|| format!("unknown op '{op_raw}' (eq, ne, gt, lt, ge, le, exists)"))?;

        let value = obj.get("value");
        let mut value_bytes = None;
        let mut value_u128 = None;
        match op {
            KvSearchOp::Exists => {}
            KvSearchOp::Eq | KvSearchOp::Ne => {
                let raw_val = value.and_then(|v| v.as_str()).ok_or_else(|| {
                    "eq/ne conditions need value as a string (utf8 or 0x-hex bytes)".to_string()
                })?;
                value_bytes =
                    Some(parse_key_str_to_bytes(raw_val).ok_or_else(|| {
                        format!("condition.value could not be parsed: {raw_val}")
                    })?);
            }
            KvSearchOp::Gt | KvSearchOp::Lt | KvSearchOp::Ge | KvSearchOp::Le => {
                let parsed = match value {
                    Some(Value::Number(num)) => num.as_u64().map(|v| v as u128),
                    Some(Value::String(s)) => {
                        let trimmed = s.trim();
                        if let Some(hex) = trimmed.strip_prefix("0x") {
                            u128::from_str_radix(hex, 16).ok()
                        } else {
                            trimmed.parse::<u128>().ok()
                        }
                    }
                    _ => None,
                };
                value_u128 = Some(parsed.ok_or_else(|| {
                    "numeric conditions need value as a u128 (decimal or 0x-hex, string ok)"
                        .to_string()
                })?);
            }
        }
        Ok(Self { key, op, value_bytes, value_u128 })
    }

    /// Whether a present stored value satisfies the condition. Callers handle
    /// key absence (an absent key never matches any op).
    fn matches(&self, stored: &[u8]) -> bool {
        match self.op {
            KvSearchOp::Exists => true,
            KvSearchOp::Eq => Some(stored) == self.value_bytes.as_deref(),
            KvSearchOp::Ne => Some(stored) != self.value_bytes.as_deref(),
            KvSearchOp::Gt | KvSearchOp::Lt | KvSearchOp::Ge | KvSearchOp::Le => {
                if stored.len() > 16 {
                    return false;
                }
                let mut acc: u128 = 0;
                for (i, &byte) in stored.iter().enumerate() {
                    acc |= (byte as u128) << (i * 8);
                }
                let Some(target) = self.value_u128 else { return false };
                match self.op {
                    KvSearchOp::Gt => acc > target,
                    KvSearchOp::Lt => acc < target,
                    KvSearchOp::Ge => acc >= target,
                    KvSearchOp::Le => acc <= target,
                    _ => unreachable!(),
                }
            }
        }
    }

    fn describe(&self) -> Value {
        json!({
            "key": fmt_bytes_hex(&self.key),
            "key_str": utf8_or_null(&self.key),
            "op": self.op.as_str(),
            "value": self
                .value_bytes
                .as_ref()
                .map(|b| Value::String(fmt_bytes_hex(b)))
                .or_else(|| self.value_u128.map(|v| Value::String(v.to_string())))
                .unwrap_or(Value::Null),
        })
    }
}

fn parse_key_str_to_bytes(s: &str) -> Option<Vec<u8>> {
    if let Some(hex) = s.strip_prefix("0x") {
        if hex.len() % 2 == 0 && !hex.is_empty() {
            return hex::decode(hex).ok();
        }
    }
    Some(s.as_bytes().to_vec())
}

fn dedup_sort_keys(mut v: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    v.sort();
    v.dedup();
    v
}

fn parse_outpoint_str(s: &str) -> std::result::Result<(Txid, u32), Value> {
    let (txid_hex, vout_str) = match s.split_once(':') {
        Some(parts) => parts,
        None => {
            return Err(json!({
                "ok": false,
                "error": "invalid_outpoint_format",
                "hint": "expected \"<txid>:<vout>\""
            }));
        }
    };
    let txid = match Txid::from_str(txid_hex) {
        Ok(t) => t,
        Err(_) => {
            return Err(json!({"ok": false, "error": "invalid_txid"}));
        }
    };
    let vout_u32 = match vout_str.parse::<u32>() {
        Ok(n) => n,
        Err(_) => {
            return Err(json!({"ok": false, "error": "invalid_vout"}));
        }
    };
    Ok((txid, vout_u32))
}

/// Split the stored value row into `(last_txid_be_hex, raw_value_bytes)`.
/// First 32 bytes = txid in LE; we flip to BE for explorers.
/// Returns (Some("deadbeef…"), tail) or (None, whole) if no txid present.
fn split_txid_value(v: &[u8]) -> (Option<String>, &[u8]) {
    if v.len() >= 32 {
        let txid_le = &v[..32];
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();
        (Some(fmt_bytes_hex_noprefix(&txid_be)), &v[32..])
    } else {
        (None, v)
    }
}

fn fmt_bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

fn fmt_bytes_hex_noprefix(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

fn utf8_or_null(b: &[u8]) -> Value {
    match std::str::from_utf8(b) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => Value::Null,
    }
}

fn u128_le_or_null(b: &[u8]) -> Value {
    if b.len() > 16 {
        return Value::Null;
    }
    let mut acc: u128 = 0;
    for (i, &byte) in b.iter().enumerate() {
        acc |= (byte as u128) << (i * 8);
    }
    Value::String(acc.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AppConfig, JemallocProfileConfig, MempoolConfig, MiscConfig, get_network, init_config_from,
    };
    use crate::core::blockfetcher::BlockFetchMode;
    use crate::runtime::tree_db::{get_global_tree_db, init_global_tree_db};
    use bitcoin::secp256k1::{Secp256k1, XOnlyPublicKey};
    use bitcoin::{
        Address, BlockHash, Network, Transaction, absolute::LockTime,
        blockdata::constants::genesis_block, transaction::Version,
    };
    use rocksdb::{DB, Options};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_creation_record(seq: u64) -> AlkaneCreationRecord {
        AlkaneCreationRecord {
            alkane: SchemaAlkaneId { block: 1, tx: seq },
            txid: [seq as u8; 32],
            creation_height: 100 + seq as u32,
            creation_timestamp: 1_700_000_000 + seq as u32,
            tx_index_in_block: seq as u32,
            inspection: None,
            names: vec![format!("n{seq}")],
            symbols: vec![format!("s{seq}")],
            cap: seq as u128,
            mint_amount: seq as u128,
        }
    }

    fn make_block_summary(height: u32, blockhash: BlockHash, tx_count: u32) -> BlockSummary {
        BlockSummary {
            height,
            blockhash: blockhash.to_byte_array(),
            trace_count: 0,
            interaction_count: 0,
            tx_count,
            header: Vec::new(),
            fee_avg: 0.0,
            fee_median: 0.0,
            fee_range: Vec::new(),
            pool: None,
        }
    }

    #[test]
    fn block_summary_time_decodes_indexed_header() {
        let block = genesis_block(Network::Bitcoin);
        let mut summary = make_block_summary(0, block.block_hash(), 1);
        summary.header = serialize(&block.header);

        assert_eq!(summary.block_time(), Some(block.header.time as u64));

        summary.header.clear();
        assert_eq!(summary.block_time(), None);
    }

    #[test]
    fn enriched_transaction_json_uses_exact_block_height_and_time() {
        let render = AddressTxRender {
            txid: Txid::from_byte_array([1; 32]),
            tx: Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: Vec::new(),
                output: Vec::new(),
            },
            traces: None,
            block_height: Some(123),
            block_time: Some(1_700_000_000),
            confirmations: Some(4),
            is_mempool: false,
        };

        let value = enriched_transaction_json(&render, &HashMap::new(), Network::Bitcoin);

        assert_eq!(value.get("blockHeight"), Some(&json!(123)));
        assert_eq!(value.get("blockTime"), Some(&json!(1_700_000_000u64)));
        assert_eq!(value.get("confirmations"), Some(&json!(4)));
    }

    #[test]
    fn enriched_transaction_json_marks_mempool_transactions_unconfirmed() {
        let render = AddressTxRender {
            txid: Txid::from_byte_array([2; 32]),
            tx: Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: Vec::new(),
                output: Vec::new(),
            },
            traces: None,
            block_height: None,
            block_time: None,
            confirmations: Some(0),
            is_mempool: true,
        };

        let value = enriched_transaction_json(&render, &HashMap::new(), Network::Bitcoin);

        assert_eq!(value["confirmed"], json!(false));
        assert_eq!(value["blockHeight"], Value::Null);
        assert_eq!(value["blockTime"], Value::Null);
        assert_eq!(value["confirmations"], json!(0));
    }

    #[test]
    fn block_times_rpc_preserves_requested_heights() {
        let provider = new_provider_with_tempdb();

        let response = provider
            .rpc_get_block_times(RpcGetBlockTimesParams { heights: Some(vec![12, 4, 12]) })
            .expect("block times response")
            .value;
        let heights = response["times"]
            .as_array()
            .expect("times array")
            .iter()
            .filter_map(|entry| entry["height"].as_u64())
            .collect::<Vec<_>>();

        assert_eq!(response["ok"], json!(true));
        assert_eq!(heights, vec![12, 4, 12]);
    }

    #[test]
    fn block_times_rpc_enforces_batch_limit() {
        let provider = new_provider_with_tempdb();
        let response = provider
            .rpc_get_block_times(RpcGetBlockTimesParams {
                heights: Some(vec![0; MAX_BLOCK_TIMES_BATCH + 1]),
            })
            .expect("block times response")
            .value;

        assert_eq!(response["ok"], json!(false));
        assert_eq!(response["error"], json!("too_many_heights"));
        assert_eq!(response["max_heights"], json!(MAX_BLOCK_TIMES_BATCH));
    }

    #[test]
    fn search_alkane_matches_prefix_and_falls_back_symbol_to_uppercase_name() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let mut exact = make_creation_record(1);
        exact.names = vec!["Diesel".to_string()];
        exact.symbols.clear();
        let mut prefixed = make_creation_record(2);
        prefixed.names = vec!["Diesel Fuel".to_string()];
        prefixed.symbols = vec!["DSL-F".to_string()];
        write_creation_rows(&provider, &[(0, exact.clone()), (1, prefixed.clone())], 2);
        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![
                    (table.alkane_name_index_key("diesel", &exact.alkane), Vec::new()),
                    (table.alkane_name_index_key("diesel fuel", &prefixed.alkane), Vec::new()),
                ],
                deletes: Vec::new(),
            })
            .expect("write name index");

        let response = provider
            .rpc_search_alkane(RpcSearchAlkaneParams {
                prefix: Some("DIE".to_string()),
                limit: Some(10),
            })
            .expect("search response")
            .value;
        let items = response["items"].as_array().expect("items");

        assert_eq!(response["ok"], json!(true));
        assert_eq!(response["prefix"], json!("die"));
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["alkane"], json!("1:1"));
        assert_eq!(items[0]["name"], json!("Diesel"));
        assert_eq!(items[0]["symbol"], json!("DIESEL"));
    }

    fn write_creation_rows(
        provider: &EssentialsProvider,
        rows: &[(u64, AlkaneCreationRecord)],
        count: u64,
    ) {
        let table = provider.table();
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (seq, rec) in rows {
            let mut id_bytes = Vec::with_capacity(12);
            id_bytes.extend_from_slice(&rec.alkane.block.to_be_bytes());
            id_bytes.extend_from_slice(&rec.alkane.tx.to_be_bytes());
            puts.push((table.alkane_creation_seq_key(*seq), id_bytes));
            puts.push((
                table.alkane_creation_by_id_key(&rec.alkane),
                encode_creation_record(rec).expect("encode creation"),
            ));
        }
        puts.push((table.alkane_creation_count_key(), count.to_le_bytes().to_vec()));
        provider
            .set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes: Vec::new() })
            .expect("write creation rows");
    }

    fn new_provider_with_tempdb() -> EssentialsProvider {
        let dir = TempDir::new().expect("tempdir");
        let mdb = Arc::new(Mdb::open(dir.path(), b"essentials_test:").expect("open mdb"));
        // Keep tempdir alive for process lifetime in tests.
        std::mem::forget(dir);
        EssentialsProvider::new(mdb)
    }

    fn init_test_config_for_addresses() -> Network {
        let cfg_dir = TempDir::new().expect("temp config dir");
        let db_dir = TempDir::new().expect("temp espo db dir");
        let cfg = AppConfig {
            readonly_metashrew_db_dir: cfg_dir.path().to_string_lossy().to_string(),
            electrum_rpc_url: None,
            metashrew_rpc_url: "http://127.0.0.1:7044".to_string(),
            electrs_esplora_url: Some("http://127.0.0.1:4332".to_string()),
            bitcoind_rpc_url: "http://127.0.0.1:8332".to_string(),
            bitcoind_rpc_user: "test".to_string(),
            bitcoind_rpc_pass: "test".to_string(),
            b8_faucet_url: None,
            hosts: Default::default(),
            bitcoind_blocks_dir: "/tmp".to_string(),
            reset_mempool_on_startup: false,
            rollback: None,
            view_only: true,
            db_path: db_dir.path().to_string_lossy().to_string(),
            db_cache: false,
            alkabi_verify_trials: 128,
            sdb_poll_ms: 100,
            indexer_block_delay_ms: 0,
            port: 0,
            explorer_espo_rpc_host: None,
            enable_internal_rpc: false,
            explorer_host: None,
            explorer_base_path: "/".to_string(),
            explorer_pizza_tv_endpoint: "https://tv.pizza.fun".to_string(),
            explorer_amm_prefix: "https://www.oyl.io/swap".to_string(),
            sync_banner: None,
            network: Network::Regtest,
            metashrew_db_label: None,
            strict_mode: None,
            debug: false,
            debug_ignore_ms: 0,
            debug_backup: None,
            safe_tip_hook_script: None,
            block_source_mode: BlockFetchMode::RpcOnly,
            compact_tx_trace_rows: true,
            address_index_chunk_size: 512,
            trace_read_workers: 8,
            recover_missing_traces_by_txid: false,
            explorer_networks: None,
            google_analytics_tag: None,
            misc: MiscConfig::default(),
            jemalloc_profile: JemallocProfileConfig::default(),
            mempool: MempoolConfig::default(),
            modules: HashMap::new(),
        };
        let _ = init_config_from(cfg);
        std::mem::forget(cfg_dir);
        std::mem::forget(db_dir);
        get_network()
    }

    fn test_address_for_network(network: Network) -> String {
        let key = XOnlyPublicKey::from_slice(
            &hex::decode("79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
                .expect("decode xonly key"),
        )
        .expect("xonly key");
        let secp = Secp256k1::verification_only();
        Address::p2tr(&secp, key, None, network).to_string()
    }

    #[test]
    fn factory_children_reads_indexed_children_only() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let factory = SchemaAlkaneId { block: 4, tx: 780_993 };
        let child_a = SchemaAlkaneId { block: 2, tx: 80_663 };
        let child_b = SchemaAlkaneId { block: 2, tx: 80_664 };
        let other = SchemaAlkaneId { block: 2, tx: 80_665 };

        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![
                    (table.alkane_factory_child_key(&factory, &child_b), Vec::new()),
                    (table.alkane_factory_child_key(&factory, &child_a), Vec::new()),
                    (
                        table.alkane_factory_child_key(&SchemaAlkaneId { block: 4, tx: 1 }, &other),
                        Vec::new(),
                    ),
                ],
                deletes: Vec::new(),
            })
            .expect("write factory children");

        let result = provider
            .get_factory_children(GetFactoryChildrenParams { blockhash: StateAt::Latest, factory })
            .expect("factory children");

        assert_eq!(result.children, vec![child_a, child_b]);
    }

    #[test]
    fn rpc_get_orbital_holders_reads_indexed_factory_holders() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let factory = SchemaAlkaneId { block: 4, tx: 780_993 };
        let addr_a = HolderId::Address("addr_a".to_string());
        let addr_b = HolderId::Address("addr_b".to_string());
        let alkane_holder = HolderId::Alkane(SchemaAlkaneId { block: 9, tx: 9 });
        let stale = HolderId::Address("stale".to_string());
        let child_a = SchemaAlkaneId { block: 2, tx: 1 };
        let child_b = SchemaAlkaneId { block: 2, tx: 2 };
        let child_c = SchemaAlkaneId { block: 2, tx: 3 };
        let child_d = SchemaAlkaneId { block: 2, tx: 4 };
        let child_e = SchemaAlkaneId { block: 2, tx: 5 };
        let child_f = SchemaAlkaneId { block: 2, tx: 6 };

        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![
                    // Legacy count-only rows must be ignored by the v2 reader.
                    (table.orbital_holder_key(&factory, &addr_b), encode_u128_value(1).unwrap()),
                    (table.orbital_holder_list_idx_key(&factory, 0), holder_id_bytes(&addr_b)),
                    (table.orbital_holder_list_len_key(&factory), 1u32.to_le_bytes().to_vec()),
                    (
                        table.orbital_holder_v2_key(&factory, &addr_b),
                        encode_orbital_holder_entry(OrbitalHolderEntry {
                            holder: addr_b.clone(),
                            amount: 1,
                            alkanes: vec![child_a],
                        })
                        .unwrap(),
                    ),
                    (
                        table.orbital_holder_v2_key(&factory, &addr_a),
                        encode_orbital_holder_entry(OrbitalHolderEntry {
                            holder: addr_a.clone(),
                            amount: 3,
                            alkanes: vec![child_b, child_c, child_d],
                        })
                        .unwrap(),
                    ),
                    (
                        table.orbital_holder_v2_key(&factory, &alkane_holder),
                        encode_orbital_holder_entry(OrbitalHolderEntry {
                            holder: alkane_holder.clone(),
                            amount: 2,
                            alkanes: vec![child_e, child_f],
                        })
                        .unwrap(),
                    ),
                    (
                        table.orbital_holder_v2_key(&factory, &stale),
                        encode_orbital_holder_entry(OrbitalHolderEntry {
                            holder: stale.clone(),
                            amount: 0,
                            alkanes: Vec::new(),
                        })
                        .unwrap(),
                    ),
                    (table.orbital_holder_v2_list_idx_key(&factory, 0), holder_id_bytes(&addr_b)),
                    (table.orbital_holder_v2_list_idx_key(&factory, 1), holder_id_bytes(&stale)),
                    (
                        table.orbital_holder_v2_list_idx_key(&factory, 2),
                        holder_id_bytes(&alkane_holder),
                    ),
                    (table.orbital_holder_v2_list_idx_key(&factory, 3), holder_id_bytes(&addr_a)),
                    (table.orbital_holder_v2_list_len_key(&factory), 4u32.to_le_bytes().to_vec()),
                ],
                deletes: Vec::new(),
            })
            .expect("write orbital holder rows");

        let result = provider
            .rpc_get_orbital_holders(RpcGetOrbitalHoldersParams {
                factory: Some("4:780993".to_string()),
                page: Some(1),
                limit: Some(2),
            })
            .expect("rpc orbital holders")
            .value;

        assert_eq!(result["ok"], true);
        assert_eq!(result["factory"], "4:780993");
        assert_eq!(result["total"], 3);
        assert_eq!(result["has_more"], true);
        assert_eq!(result["items"][0]["type"], "address");
        assert_eq!(result["items"][0]["address"], "addr_a");
        assert_eq!(result["items"][0]["amount"], "3");
        assert_eq!(result["items"][0]["alkanes"], json!(["2:2", "2:3", "2:4"]));
        assert_eq!(result["items"][1]["type"], "alkane");
        assert_eq!(result["items"][1]["alkane"], "9:9");
        assert_eq!(result["items"][1]["amount"], "2");
        assert_eq!(result["items"][1]["alkanes"], json!(["2:5", "2:6"]));
    }

    #[test]
    fn rpc_get_orbital_balances_reads_address_factory_child_lists() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let address = test_address_for_network(init_test_config_for_addresses());
        let factory_a = SchemaAlkaneId { block: 4, tx: 520 };
        let factory_b = SchemaAlkaneId { block: 4, tx: 65522 };

        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![(
                    table.address_orbital_balances_v2_key(&address),
                    encode_address_orbital_balance_entries(vec![
                        AddressOrbitalBalanceEntry {
                            factory: factory_a,
                            amount: 2,
                            alkanes: vec![
                                SchemaAlkaneId { block: 2, tx: 77_631 },
                                SchemaAlkaneId { block: 2, tx: 77_635 },
                            ],
                        },
                        AddressOrbitalBalanceEntry {
                            factory: factory_b,
                            amount: 1,
                            alkanes: vec![SchemaAlkaneId { block: 2, tx: 80_663 }],
                        },
                    ])
                    .unwrap(),
                )],
                deletes: Vec::new(),
            })
            .expect("write address orbital balances");

        let result = provider
            .rpc_get_orbital_balances(RpcGetOrbitalBalancesParams {
                address: Some(address.to_string()),
            })
            .expect("rpc orbital balances")
            .value;

        assert_eq!(result["ok"], true);
        assert_eq!(result["address"], address);
        assert_eq!(result["balances"]["4:520"]["amount"], "2");
        assert_eq!(result["balances"]["4:520"]["alkanes"], json!(["2:77631", "2:77635"]));
        assert_eq!(result["balances"]["4:65522"]["amount"], "1");
        assert_eq!(result["balances"]["4:65522"]["alkanes"], json!(["2:80663"]));
    }

    #[test]
    fn rpc_get_orbital_volumes_reads_indexed_factory_address_volumes() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let factory = SchemaAlkaneId { block: 4, tx: 780_993 };
        let alkane = SchemaAlkaneId { block: 2, tx: 0 };
        let other_alkane = SchemaAlkaneId { block: 4, tx: 3 };

        fn legacy_orbital_volume_entry_key(
            factory: &SchemaAlkaneId,
            address: &str,
            receive: bool,
        ) -> Vec<u8> {
            let mut key = Vec::new();
            key.extend_from_slice(ALKANE_V2_PREFIX);
            key.extend_from_slice(&encode_alkane_id_be(factory));
            if receive {
                key.extend_from_slice(b"/orbital_receive_volume/");
            } else {
                key.extend_from_slice(b"/orbital_send_volume/");
            }
            key.extend_from_slice(address.as_bytes());
            key
        }

        fn legacy_orbital_volume_list_prefix(factory: &SchemaAlkaneId, receive: bool) -> Vec<u8> {
            let mut key = Vec::new();
            key.extend_from_slice(ALKANE_V2_PREFIX);
            key.extend_from_slice(&encode_alkane_id_be(factory));
            if receive {
                key.extend_from_slice(b"/orbital_receive_volume_idx/");
            } else {
                key.extend_from_slice(b"/orbital_send_volume_idx/");
            }
            key
        }

        fn legacy_orbital_volume_list_len_key(factory: &SchemaAlkaneId, receive: bool) -> Vec<u8> {
            let mut key = legacy_orbital_volume_list_prefix(factory, receive);
            key.extend_from_slice(b"length");
            key
        }

        fn legacy_orbital_volume_list_idx_key(
            factory: &SchemaAlkaneId,
            idx: u32,
            receive: bool,
        ) -> Vec<u8> {
            let mut key = legacy_orbital_volume_list_prefix(factory, receive);
            key.extend_from_slice(&idx.to_be_bytes());
            key
        }

        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![
                    (
                        legacy_orbital_volume_entry_key(&factory, "legacy", false),
                        encode_u128_value(99).unwrap(),
                    ),
                    (legacy_orbital_volume_list_idx_key(&factory, 0, false), b"legacy".to_vec()),
                    (
                        legacy_orbital_volume_list_len_key(&factory, false),
                        1u32.to_le_bytes().to_vec(),
                    ),
                    (
                        table.orbital_send_volume_entry_key(&factory, &alkane, "addr_b"),
                        encode_u128_value(1).unwrap(),
                    ),
                    (
                        table.orbital_send_volume_entry_key(&factory, &alkane, "addr_a"),
                        encode_u128_value(3).unwrap(),
                    ),
                    (
                        table.orbital_send_volume_entry_key(&factory, &alkane, "stale"),
                        encode_u128_value(0).unwrap(),
                    ),
                    (
                        table.orbital_send_volume_entry_key(&factory, &other_alkane, "addr_z"),
                        encode_u128_value(100).unwrap(),
                    ),
                    (
                        table.orbital_send_volume_list_idx_key(&factory, &alkane, 0),
                        b"addr_b".to_vec(),
                    ),
                    (
                        table.orbital_send_volume_list_idx_key(&factory, &alkane, 1),
                        b"stale".to_vec(),
                    ),
                    (
                        table.orbital_send_volume_list_idx_key(&factory, &alkane, 2),
                        b"addr_a".to_vec(),
                    ),
                    (
                        table.orbital_send_volume_list_len_key(&factory, &alkane),
                        3u32.to_le_bytes().to_vec(),
                    ),
                    (
                        table.orbital_send_volume_list_idx_key(&factory, &other_alkane, 0),
                        b"addr_z".to_vec(),
                    ),
                    (
                        table.orbital_send_volume_list_len_key(&factory, &other_alkane),
                        1u32.to_le_bytes().to_vec(),
                    ),
                    (
                        table.orbital_receive_volume_entry_key(&factory, &alkane, "addr_c"),
                        encode_u128_value(9).unwrap(),
                    ),
                    (
                        table.orbital_receive_volume_list_idx_key(&factory, &alkane, 0),
                        b"addr_c".to_vec(),
                    ),
                    (
                        table.orbital_receive_volume_list_len_key(&factory, &alkane),
                        1u32.to_le_bytes().to_vec(),
                    ),
                ],
                deletes: Vec::new(),
            })
            .expect("write orbital volume rows");

        let send = provider
            .rpc_get_orbital_send_volumes(RpcGetOrbitalVolumesParams {
                factory: Some("4:780993".to_string()),
                alkane: Some("2:0".to_string()),
                page: Some(1),
                limit: Some(1),
            })
            .expect("rpc orbital send volumes")
            .value;
        assert_eq!(send["ok"], true);
        assert_eq!(send["factory"], "4:780993");
        assert_eq!(send["alkane"], "2:0");
        assert_eq!(send["kind"], "send");
        assert_eq!(send["total"], 2);
        assert_eq!(send["has_more"], true);
        assert_eq!(send["items"][0]["address"], "addr_a");
        assert_eq!(send["items"][0]["amount"], "3");

        let receive = provider
            .rpc_get_orbital_receive_volumes(RpcGetOrbitalVolumesParams {
                factory: Some("4:780993".to_string()),
                alkane: Some("2:0".to_string()),
                page: Some(1),
                limit: Some(10),
            })
            .expect("rpc orbital receive volumes")
            .value;
        assert_eq!(receive["ok"], true);
        assert_eq!(receive["kind"], "receive");
        assert_eq!(receive["total"], 1);
        assert_eq!(receive["items"][0]["address"], "addr_c");
        assert_eq!(receive["items"][0]["amount"], "9");
    }

    #[test]
    fn rpc_get_alkane_volumes_reads_indexed_source_token_address_volumes() {
        let provider = new_provider_with_tempdb();
        let table = provider.table();
        let source = SchemaAlkaneId { block: 2, tx: 1 };
        let alkane = SchemaAlkaneId { block: 4, tx: 3 };
        let other_alkane = SchemaAlkaneId { block: 4, tx: 4 };

        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![
                    (
                        table.alkane_send_volume_entry_key(&source, &alkane, "addr_b"),
                        encode_u128_value(2).unwrap(),
                    ),
                    (
                        table.alkane_send_volume_entry_key(&source, &alkane, "addr_a"),
                        encode_u128_value(6).unwrap(),
                    ),
                    (
                        table.alkane_send_volume_entry_key(&source, &other_alkane, "addr_z"),
                        encode_u128_value(100).unwrap(),
                    ),
                    (
                        table.alkane_send_volume_list_idx_key(&source, &alkane, 0),
                        b"addr_b".to_vec(),
                    ),
                    (
                        table.alkane_send_volume_list_idx_key(&source, &alkane, 1),
                        b"addr_a".to_vec(),
                    ),
                    (
                        table.alkane_send_volume_list_len_key(&source, &alkane),
                        2u32.to_le_bytes().to_vec(),
                    ),
                    (
                        table.alkane_send_volume_list_idx_key(&source, &other_alkane, 0),
                        b"addr_z".to_vec(),
                    ),
                    (
                        table.alkane_send_volume_list_len_key(&source, &other_alkane),
                        1u32.to_le_bytes().to_vec(),
                    ),
                    (
                        table.alkane_receive_volume_entry_key(&source, &alkane, "addr_c"),
                        encode_u128_value(5).unwrap(),
                    ),
                    (
                        table.alkane_receive_volume_list_idx_key(&source, &alkane, 0),
                        b"addr_c".to_vec(),
                    ),
                    (
                        table.alkane_receive_volume_list_len_key(&source, &alkane),
                        1u32.to_le_bytes().to_vec(),
                    ),
                ],
                deletes: Vec::new(),
            })
            .expect("write alkane volume rows");

        let send = provider
            .rpc_get_alkane_send_volumes(RpcGetAlkaneVolumesParams {
                source_alkane: Some("2:1".to_string()),
                alkane: Some("4:3".to_string()),
                page: Some(1),
                limit: Some(10),
            })
            .expect("rpc alkane send volumes")
            .value;
        assert_eq!(send["ok"], true);
        assert_eq!(send["source_alkane"], "2:1");
        assert_eq!(send["alkane"], "4:3");
        assert_eq!(send["kind"], "send");
        assert_eq!(send["total"], 2);
        assert_eq!(send["items"][0]["address"], "addr_a");
        assert_eq!(send["items"][0]["amount"], "6");
        assert_eq!(send["items"][1]["address"], "addr_b");
        assert_eq!(send["items"][1]["amount"], "2");

        let receive = provider
            .rpc_get_alkane_receive_volumes(RpcGetAlkaneVolumesParams {
                source_alkane: Some("2:1".to_string()),
                alkane: Some("4:3".to_string()),
                page: Some(1),
                limit: Some(10),
            })
            .expect("rpc alkane receive volumes")
            .value;
        assert_eq!(receive["ok"], true);
        assert_eq!(receive["kind"], "receive");
        assert_eq!(receive["total"], 1);
        assert_eq!(receive["items"][0]["address"], "addr_c");
        assert_eq!(receive["items"][0]["amount"], "5");
    }

    #[test]
    fn address_index_append_truncates_stale_tail_from_blob_chunk() {
        let provider = new_provider_with_tempdb();
        let kind = AddressIndexListKind::AlkaneTxs;
        let address = "addr1";
        let table = provider.table();
        let meta_key = table.address_index_meta_key(address, kind);
        let chunk_id = 7u64;
        let chunk_key = table.address_index_chunk_blob_key(kind, chunk_id);

        provider
            .blob_mdb()
            .put(&chunk_key, &encode_u64_chunk(vec![1, 2, 99, 100]).expect("encode chunk"))
            .expect("write stale chunk");
        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![(
                    meta_key,
                    encode_address_index_state(&InlineOrExternalU64V1::External {
                        chunk_ids: vec![chunk_id],
                        len: 2,
                        chunk_size: 4,
                    })
                    .expect("encode state"),
                )],
                deletes: Vec::new(),
            })
            .expect("write meta");

        let mut next_chunk_id = 8u64;
        let mut puts = Vec::new();
        let mut blob_puts = Vec::new();
        let new_len = append_address_index_values(
            &provider,
            kind,
            address,
            &[3, 4],
            &mut next_chunk_id,
            &mut puts,
            &mut blob_puts,
        )
        .expect("append");

        assert_eq!(new_len, 4);
        assert_eq!(next_chunk_id, 8);
        let (_, rewritten_chunk) = blob_puts
            .iter()
            .find(|(key, _)| key == &chunk_key)
            .expect("rewritten last chunk");
        assert_eq!(decode_u64_chunk(rewritten_chunk), vec![1, 2, 3, 4]);
    }

    #[test]
    fn alkane_info_round_trip() {
        let info = AlkaneInfo {
            creation_txid: [7u8; 32],
            creation_height: 42,
            creation_timestamp: 1_700_000_000,
        };

        let encoded = encode_alkane_info(&info).expect("encode");
        let decoded = decode_alkane_info(&encoded).expect("decode");
        assert_eq!(info, decoded);
    }

    #[test]
    fn circulating_supply_read_does_not_require_an_exact_height_row() {
        let provider = new_provider_with_tempdb();
        let alkane = SchemaAlkaneId { block: 2, tx: 77623 };
        let supply = 5_227_124_025_507u128;
        provider
            .set_batch(SetBatchParams {
                blockhash: StateAt::Latest,
                puts: vec![(
                    provider.table().circulating_supply_latest_key(&alkane),
                    encode_u128_value(supply).expect("encode supply"),
                )],
                deletes: Vec::new(),
            })
            .expect("write latest supply");

        let result = provider
            .get_circulating_supply(GetCirculatingSupplyParams {
                blockhash: StateAt::Latest,
                alkane,
                height: 957_625,
            })
            .expect("read supply");

        assert_eq!(result.supply, supply);
    }

    #[test]
    fn creation_record_round_trip() {
        let rec = AlkaneCreationRecord {
            alkane: SchemaAlkaneId { block: 5, tx: 10 },
            txid: [9u8; 32],
            creation_height: 123,
            creation_timestamp: 99,
            tx_index_in_block: 3,
            inspection: None,
            names: vec!["demo".to_string(), "demo2".to_string()],
            symbols: vec!["DMO".to_string()],
            cap: 500,
            mint_amount: 25,
        };

        let encoded = encode_creation_record(&rec).expect("encode");
        let decoded = decode_creation_record(&encoded).expect("decode");
        assert_eq!(rec, decoded);
    }

    #[test]
    fn creation_seq_page_respects_offset_limit_and_desc() {
        let provider = new_provider_with_tempdb();
        let mut rows: Vec<(u64, AlkaneCreationRecord)> = Vec::new();
        for seq in 0..5u64 {
            rows.push((seq, make_creation_record(seq)));
        }
        write_creation_rows(&provider, &rows, 5);

        let asc_page = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 0,
                limit: 2,
                desc: false,
            })
            .expect("asc page");
        assert_eq!(asc_page.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![0, 1]);

        let asc_tail = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 4,
                limit: 3,
                desc: false,
            })
            .expect("asc tail");
        assert_eq!(asc_tail.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![4]);

        let desc_page = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 0,
                limit: 2,
                desc: true,
            })
            .expect("desc page");
        assert_eq!(desc_page.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![4, 3]);

        let desc_tail = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 4,
                limit: 2,
                desc: true,
            })
            .expect("desc tail");
        assert_eq!(desc_tail.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![0]);

        let desc_empty = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 5,
                limit: 1,
                desc: true,
            })
            .expect("desc empty");
        assert!(desc_empty.records.is_empty());
    }

    #[test]
    fn creation_seq_page_tolerates_count_ahead_of_seq_rows() {
        let provider = new_provider_with_tempdb();
        let rows = vec![(0, make_creation_record(10)), (1, make_creation_record(11))];
        write_creation_rows(&provider, &rows, 4);

        let page = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Latest,
                offset: 0,
                limit: 10,
                desc: true,
            })
            .expect("page");

        let ids: Vec<u64> = page.records.iter().map(|r| r.alkane.tx).collect();
        assert_eq!(ids, vec![11, 10]);
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn creation_seq_page_uses_requested_blockhash_state() {
        if get_global_tree_db().is_some() {
            return;
        }

        let dir = TempDir::new().expect("tempdir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).expect("open rocksdb"));
        init_global_tree_db(Arc::clone(&db)).expect("init tree db");

        let provider =
            EssentialsProvider::new(Arc::new(Mdb::from_db(Arc::clone(&db), b"essentials:")));
        let tree = get_global_tree_db().expect("global tree");

        let genesis = BlockHash::from_byte_array([0u8; 32]);
        let h1 = BlockHash::from_byte_array([1u8; 32]);
        let h2 = BlockHash::from_byte_array([2u8; 32]);

        tree.begin_block(1, &h1, &genesis).expect("begin h1");
        write_creation_rows(&provider, &[(0, make_creation_record(0))], 1);
        tree.finish_block().expect("finish h1");

        tree.begin_block(2, &h2, &h1).expect("begin h2");
        write_creation_rows(&provider, &[(1, make_creation_record(1))], 2);
        tree.finish_block().expect("finish h2");

        let h1_latest = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Block(h1),
                offset: 0,
                limit: 10,
                desc: true,
            })
            .expect("h1 page");
        assert_eq!(h1_latest.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![0]);

        let h1_offset = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Block(h1),
                offset: 1,
                limit: 1,
                desc: true,
            })
            .expect("h1 offset page");
        assert!(h1_offset.records.is_empty());

        let h2_latest = provider
            .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                blockhash: StateAt::Block(h2),
                offset: 0,
                limit: 10,
                desc: true,
            })
            .expect("h2 page");
        assert_eq!(h2_latest.records.iter().map(|r| r.alkane.tx).collect::<Vec<_>>(), vec![1, 0]);

        let orphan = BlockHash::from_byte_array([9u8; 32]);
        let canonical_summary = make_block_summary(2, h2, 22);
        let orphan_summary = make_block_summary(2, orphan, 99);
        provider
            .put_block_summary_indexes(&canonical_summary)
            .expect("write canonical summary");
        provider
            .put_block_summary_indexes(&orphan_summary)
            .expect("write orphan summary after canonical");

        let summary = provider
            .get_block_summaries_by_heights(&[2])
            .expect("summaries")
            .into_iter()
            .next()
            .flatten()
            .expect("summary");
        assert_eq!(summary.block_hash(), Some(h2));
        assert_eq!(summary.tx_count, 22);

        cache_block_summary(2, orphan_summary);
        assert!(get_cached_block_summary(2).is_none());
        cache_block_summary(2, canonical_summary);
        assert_eq!(get_cached_block_summary(2).and_then(|summary| summary.block_hash()), Some(h2));
    }

    fn seed_factory_kv_fixture(provider: &EssentialsProvider) -> SchemaAlkaneId {
        let table = provider.table();
        let factory = SchemaAlkaneId { block: 4, tx: 797 };
        let a = SchemaAlkaneId { block: 2, tx: 10 };
        let b = SchemaAlkaneId { block: 2, tx: 11 };
        let c = SchemaAlkaneId { block: 2, tx: 12 };

        let kv_row = |txid_byte: u8, value: &[u8]| -> Vec<u8> {
            let mut row = vec![txid_byte; 32];
            row.extend_from_slice(value);
            row
        };

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for child in [&a, &b, &c] {
            puts.push((table.alkane_factory_child_key(&factory, child), Vec::new()));
        }
        puts.push((table.kv_row_key(&a, b"/collateral_address"), kv_row(0x11, b"bc1qalice")));
        puts.push((table.kv_row_key(&b, b"/collateral_address"), kv_row(0x22, b"bc1qbob")));
        puts.push((
            table.kv_row_key(&a, b"/fire_amount"),
            kv_row(0x33, &1_000_000u128.to_le_bytes()),
        ));
        puts.push((
            table.kv_row_key(&b, b"/fire_amount"),
            kv_row(0x44, &2_600_000_000u128.to_le_bytes()),
        ));
        // Short (8-byte) LE value: numeric decoding must handle < 16 bytes.
        puts.push((table.kv_row_key(&c, b"/fire_amount"), kv_row(0x55, &500u64.to_le_bytes())));

        provider
            .set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes: Vec::new() })
            .expect("seed factory kv fixture");
        factory
    }

    fn search_factory_keys(
        provider: &EssentialsProvider,
        conditions: Value,
        limit: Option<u64>,
        page: Option<u64>,
    ) -> Value {
        provider
            .rpc_search_factory_keys(RpcSearchFactoryKeysParams {
                factory: Some("4:797".to_string()),
                conditions: conditions.as_array().map(|arr| arr.to_vec()),
                key: None,
                op: None,
                value: None,
                try_decode_utf8: None,
                limit,
                page,
            })
            .expect("search response")
            .value
    }

    fn matched_alkanes(response: &Value) -> Vec<String> {
        response["items"]
            .as_array()
            .expect("items array")
            .iter()
            .map(|item| item["alkane"].as_str().expect("alkane id").to_string())
            .collect()
    }

    #[test]
    fn search_factory_keys_matches_exact_value() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        let response = search_factory_keys(
            &provider,
            json!([{"key": "/collateral_address", "op": "eq", "value": "bc1qalice"}]),
            None,
            None,
        );

        assert_eq!(response["ok"], json!(true));
        assert_eq!(matched_alkanes(&response), vec!["2:10"]);
        let keys = &response["items"][0]["keys"]["/collateral_address"];
        assert_eq!(keys["value_str"], json!("bc1qalice"));
        assert_eq!(keys["last_txid"], json!("11".repeat(32)));
    }

    #[test]
    fn search_factory_keys_numeric_operands_decode_le_values() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        let gt = search_factory_keys(
            &provider,
            json!([{"key": "/fire_amount", "op": "gt", "value": "1000000"}]),
            None,
            None,
        );
        assert_eq!(matched_alkanes(&gt), vec!["2:11"]);

        let range = search_factory_keys(
            &provider,
            json!([
                {"key": "/fire_amount", "op": "ge", "value": 500},
                {"key": "/fire_amount", "op": "le", "value": "1000000"},
            ]),
            None,
            None,
        );
        assert_eq!(matched_alkanes(&range), vec!["2:10", "2:12"]);
    }

    #[test]
    fn search_factory_keys_ands_conditions_across_keys() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        let response = search_factory_keys(
            &provider,
            json!([
                {"key": "/fire_amount", "op": "ge", "value": 1000},
                {"key": "/collateral_address", "op": "eq", "value": "bc1qbob"},
            ]),
            None,
            None,
        );
        assert_eq!(matched_alkanes(&response), vec!["2:11"]);
    }

    #[test]
    fn search_factory_keys_absent_key_never_matches() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        // 2:12 has no /collateral_address row: `ne` must not treat the
        // missing key as "different value".
        let response = search_factory_keys(
            &provider,
            json!([{"key": "/collateral_address", "op": "ne", "value": "bc1qalice"}]),
            None,
            None,
        );
        assert_eq!(matched_alkanes(&response), vec!["2:11"]);
    }

    #[test]
    fn search_factory_keys_exists_and_pagination() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        let page2 = search_factory_keys(
            &provider,
            json!([{"key": "/collateral_address", "op": "exists"}]),
            Some(1),
            Some(2),
        );
        assert_eq!(page2["total"], json!(2));
        assert_eq!(page2["has_more"], json!(false));
        assert_eq!(matched_alkanes(&page2), vec!["2:11"]);
    }

    #[test]
    fn wasm_payload_encoding_round_trips() {
        use base64::Engine as _;
        let wasm: Vec<u8> = (0u16..2048).map(|i| (i % 251) as u8).collect();

        let (encoding, payload_len, b64) = encode_wasm_payload(&wasm, false).expect("raw encode");
        assert_eq!(encoding, "base64");
        assert_eq!(payload_len, wasm.len());
        assert_eq!(
            base64::engine::general_purpose::STANDARD.decode(&b64).expect("b64 decode"),
            wasm
        );

        let (encoding, payload_len, b64) = encode_wasm_payload(&wasm, true).expect("gzip encode");
        assert_eq!(encoding, "base64+gzip");
        let compressed =
            base64::engine::general_purpose::STANDARD.decode(&b64).expect("b64 decode");
        assert_eq!(payload_len, compressed.len());
        assert!(compressed.len() < wasm.len(), "payload should compress");
        assert_eq!(alkanes_support::gz::decompress(compressed).expect("gunzip"), wasm);

        // sha256 of the raw bytes is the stable content address regardless of
        // transport encoding.
        assert_eq!(
            wasm_sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn get_alkane_wasm_rejects_bad_alkane() {
        let provider = new_provider_with_tempdb();
        let response = provider
            .rpc_get_alkane_wasm(RpcGetAlkaneWasmParams { alkane: None, gzip: None })
            .expect("response")
            .value;
        assert_eq!(response["error"], json!("missing_or_invalid_alkane"));

        let response = provider
            .rpc_get_alkane_wasm(RpcGetAlkaneWasmParams {
                alkane: Some("nope".to_string()),
                gzip: None,
            })
            .expect("response")
            .value;
        assert_eq!(response["error"], json!("missing_or_invalid_alkane"));
    }

    #[test]
    fn search_factory_keys_rejects_bad_input() {
        let provider = new_provider_with_tempdb();
        seed_factory_kv_fixture(&provider);

        let missing_factory = provider
            .rpc_search_factory_keys(RpcSearchFactoryKeysParams {
                factory: None,
                conditions: None,
                key: Some("/x".to_string()),
                op: None,
                value: Some(json!("y")),
                try_decode_utf8: None,
                limit: None,
                page: None,
            })
            .expect("response")
            .value;
        assert_eq!(missing_factory["error"], json!("missing_or_invalid_factory"));

        let bad_op = search_factory_keys(
            &provider,
            json!([{"key": "/x", "op": "like", "value": "y"}]),
            None,
            None,
        );
        assert_eq!(bad_op["error"], json!("invalid_condition"));

        let no_conditions = provider
            .rpc_search_factory_keys(RpcSearchFactoryKeysParams {
                factory: Some("4:797".to_string()),
                conditions: None,
                key: None,
                op: None,
                value: None,
                try_decode_utf8: None,
                limit: None,
                page: None,
            })
            .expect("response")
            .value;
        assert_eq!(no_conditions["error"], json!("missing_conditions"));
    }
}
