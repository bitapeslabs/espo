use crate::alkanes::trace::{
    EspoHostFunctionValues, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceInvokeContext, EspoSandshrewLikeTraceInvokeData,
    EspoSandshrewLikeTraceShortId, EspoTrace, extract_alkane_storage, protobuf_trace_events,
};
use crate::bitcoind_flexible::FlexibleBitcoindClient as CoreClient;
use crate::config::{
    get_bitcoind_rpc_client, get_config, get_last_safe_tip, get_metashrew_rpc_url, get_network,
};
use crate::modules::essentials::storage::{BalanceEntry, EssentialsProvider};
use crate::modules::essentials::utils::balances::get_outpoint_balances_with_spent_batch;
use crate::modules::runes::main::runes_enabled_from_global_config;
use crate::modules::runes::storage::{
    OutpointRuneBalances, RuneBalance, RunesProvider, SchemaRuneId, TxRuneIo,
};
use crate::modules::runes::transfer::{
    OutputRuneSheets, RuneSheet, RunestoneTransfer, TransferRules,
};
use crate::runtime::shutdown::is_shutdown_requested;
use crate::runtime::state_at::StateAt;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Context, Result};
use bitcoin::ScriptBuf;
use bitcoin::consensus::Encodable;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network, OutPoint, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use futures::{StreamExt, stream};
use ordinals::{Artifact, Edict, RuneId, Runestone};
use prost::Message;
use protorune_support::proto::protorune;
use protorune_support::protostone::Protostone;
use protorune_support::utils::decode_varint_list;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Write};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// --- Tunables (edit as needed) ---
pub const MEMPOOL_POLL_SECS: u64 = 5;
pub const MEMPOOL_VIEW_BATCH_SIZE: usize = 10;
pub const MEMPOOL_LOG_STEP: usize = 100;
pub const MEMPOOL_MAX_TXS: usize = 200_000;
pub const MEMPOOL_MIN_FEE_RATE_SATS_VBYTE: f64 = 0.5;
/// --- End tunables ---

const UNCOMMON_GOODS_RUNE_ID: SchemaRuneId = SchemaRuneId { block: 1, tx: 0 };

#[derive(Clone, Debug)]
pub struct MempoolEntry {
    pub txid: Txid,
    pub tx: Transaction,
    pub traces: Option<Vec<EspoTrace>>,
    pub rune_io: Option<TxRuneIo>,
    pub has_alkane_action: bool,
    pub has_rune_action: bool,
    pub defer_alkane_trace_status: bool,
    pub first_seen: u64,
    pub position: Option<MempoolProjectedPosition>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MempoolTxReadiness {
    MetadataOnly,
    Hydrated,
    TracePending,
    TraceReady,
}

impl Default for MempoolTxReadiness {
    fn default() -> Self {
        Self::MetadataOnly
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolProjectedPosition {
    pub block: usize,
    pub vsize: u64,
}

#[derive(Clone, Debug)]
pub struct MempoolTransactionStruct {
    pub txid: Txid,
    pub tx: Option<Transaction>,
    pub protostones: Vec<Protostone>,
    pub fixed_trace: Option<Vec<EspoTrace>>,
    pub fixed_trace_context: Option<Vec<Txid>>,
    pub diesel_trace: Option<Vec<EspoTrace>>,
    pub rune_io: Option<TxRuneIo>,
    pub first_seen: u64,
    pub fee_sat: u64,
    pub weight: u64,
    pub vsize: u64,
    pub fee_rate: f64,
    pub inputs: Vec<Txid>,
    pub spent_outpoints: Vec<OutPoint>,
    pub addresses: Vec<String>,
    pub is_diesel_mint: bool,
    pub is_ug_mint: bool,
    pub template_index: Option<usize>,
    pub position: Option<MempoolProjectedPosition>,
    pub readiness: MempoolTxReadiness,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct MempoolBlockTemplate {
    pub index: usize,
    pub tx_count: usize,
    pub trace_count: usize,
    pub weight: u64,
    pub vsize: u64,
    pub total_fees: u64,
    pub median_fee_rate: Option<f64>,
    pub min_fee_rate: Option<f64>,
    pub max_fee_rate: Option<f64>,
    pub fee_range: Vec<f64>,
    pub transaction_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolBlockDelta {
    pub index: usize,
    pub sequence: u64,
    pub reset: bool,
    pub added_count: usize,
    pub removed_count: usize,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
    pub full: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct MempoolBlockTx {
    pub txid: Txid,
    pub tx: Transaction,
    pub protostones: Vec<Protostone>,
    pub traces: Option<Vec<EspoTrace>>,
    pub rune_io: Option<TxRuneIo>,
    pub addresses: Vec<String>,
    pub first_seen: u64,
    pub fee_sat: u64,
    pub vsize: u64,
    pub fee_rate: f64,
    pub position: Option<MempoolProjectedPosition>,
    pub readiness: MempoolTxReadiness,
    pub defer_alkane_trace_status: bool,
}

#[derive(Clone, Debug)]
pub struct MempoolBlockDetail {
    pub template: MempoolBlockTemplate,
    pub tx_total: usize,
    pub txs: Vec<MempoolBlockTx>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MempoolTxFilter {
    All,
    Action,
    Alkane,
    Rune,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MempoolSyncPhase {
    Starting,
    Syncing,
    Hydrating,
    InSync,
    Stale,
}

impl Default for MempoolSyncPhase {
    fn default() -> Self {
        Self::Starting
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolSyncStatus {
    pub phase: MempoolSyncPhase,
    pub in_sync: bool,
    pub hydrating: bool,
    pub stale: bool,
    pub hydration_pending: usize,
    pub last_raw_refresh_at: Option<u64>,
    pub last_successful_raw_refresh_at: Option<u64>,
    pub last_error: Option<String>,
    pub clear_protection_until: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolSnapshot {
    pub tx_count: usize,
    pub updated_at: u64,
    pub sequence: u64,
    pub status: MempoolSyncStatus,
    pub blocks: Vec<MempoolBlockTemplate>,
    pub deltas: Vec<MempoolBlockDelta>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolBlockSummary {
    pub index: usize,
    pub tx_count: usize,
    pub trace_count: usize,
    pub weight: u64,
    pub vsize: u64,
    pub total_fees: u64,
    pub median_fee_rate: Option<f64>,
    pub min_fee_rate: Option<f64>,
    pub max_fee_rate: Option<f64>,
    pub fee_range: Vec<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolCompactSnapshot {
    pub tx_count: usize,
    pub updated_at: u64,
    pub sequence: u64,
    pub status: MempoolSyncStatus,
    pub blocks: Vec<MempoolBlockSummary>,
    pub deltas: Vec<MempoolBlockDelta>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolTracePayloadStats {
    pub tx_count: usize,
    pub trace_count: usize,
    pub trace_payload_bytes: usize,
    pub sandshrew_trace_json_bytes: usize,
    pub protobuf_encoded_bytes: usize,
    pub storage_changes_payload_bytes: usize,
    pub outpoint_borsh_bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolPayloadByteStats {
    pub transactions_consensus_bytes: usize,
    pub fixed_traces_payload_bytes: usize,
    pub diesel_traces_payload_bytes: usize,
    pub all_traces_payload_bytes: usize,
    pub rune_io_borsh_bytes: usize,
    pub templates_json_bytes: usize,
    pub deltas_json_bytes: usize,
    pub status_json_bytes: usize,
    pub addresses_utf8_bytes: usize,
    pub input_txid_bytes: usize,
    pub spent_outpoint_bytes: usize,
    pub template_txid_string_bytes: usize,
    pub total_measured_payload_bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolPayloadCountStats {
    pub tx_count: usize,
    pub hydrated_tx_count: usize,
    pub protostone_tx_count: usize,
    pub protostone_count: usize,
    pub fixed_trace_tx_count: usize,
    pub diesel_trace_tx_count: usize,
    pub rune_io_tx_count: usize,
    pub address_count: usize,
    pub input_txid_count: usize,
    pub spent_outpoint_count: usize,
    pub template_count: usize,
    pub template_txid_count: usize,
    pub delta_count: usize,
    pub trace_queue_count: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MempoolMemoryStats {
    pub updated_at: u64,
    pub sequence: u64,
    pub counts: MempoolPayloadCountStats,
    pub bytes: MempoolPayloadByteStats,
    pub fixed_traces: MempoolTracePayloadStats,
    pub diesel_traces: MempoolTracePayloadStats,
}

#[derive(Default)]
struct InMemoryMempool {
    txs: HashMap<Txid, MempoolTransactionStruct>,
    templates: Vec<MempoolBlockTemplate>,
    deltas: Vec<MempoolBlockDelta>,
    status: MempoolSyncStatus,
    minimum_fee_rate: Option<f64>,
    sequence: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct VerboseMempoolEntry {
    #[serde(default)]
    vsize: Option<u64>,
    #[serde(default)]
    weight: Option<u64>,
    #[serde(default)]
    fees: Option<VerboseMempoolFees>,
    #[serde(default)]
    depends: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct VerboseMempoolFees {
    #[serde(default)]
    base: Option<f64>,
    #[serde(default)]
    modified: Option<f64>,
    #[serde(default)]
    ancestor: Option<f64>,
}

#[derive(Clone)]
struct MinerTx {
    fee: u64,
    weight: u64,
    adjusted_vsize: u64,
    fee_rate: f64,
    dependency_rate: f64,
    inputs: Vec<Txid>,
    spent_outpoints: Vec<OutPoint>,
    ancestors: HashSet<Txid>,
    children: HashSet<Txid>,
    ancestor_fee: u64,
    ancestor_vsize: u64,
    score: f64,
    used: bool,
    modified: bool,
}

static IN_MEMORY_MEMPOOL: OnceLock<Arc<RwLock<InMemoryMempool>>> = OnceLock::new();
static TRACE_QUEUE: OnceLock<Arc<Mutex<VecDeque<Txid>>>> = OnceLock::new();
static MEMPOOL_EVENTS: OnceLock<broadcast::Sender<String>> = OnceLock::new();
static RECALCULATE_TEMPLATES_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static HYDRATION_RUNNING: AtomicBool = AtomicBool::new(false);

fn mempool_state() -> &'static Arc<RwLock<InMemoryMempool>> {
    IN_MEMORY_MEMPOOL.get_or_init(|| Arc::new(RwLock::new(InMemoryMempool::default())))
}

fn trace_queue() -> &'static Arc<Mutex<VecDeque<Txid>>> {
    TRACE_QUEUE.get_or_init(|| Arc::new(Mutex::new(VecDeque::new())))
}

fn recalculate_templates_lock() -> &'static Mutex<()> {
    RECALCULATE_TEMPLATES_LOCK.get_or_init(|| Mutex::new(()))
}

pub fn subscribe_mempool_events() -> broadcast::Receiver<String> {
    mempool_event_sender().subscribe()
}

fn mempool_event_sender() -> &'static broadcast::Sender<String> {
    MEMPOOL_EVENTS.get_or_init(|| {
        let (sender, _) = broadcast::channel(128);
        sender
    })
}

fn publish_mempool_event(event: &Value) {
    if let Ok(encoded) = serde_json::to_string(event) {
        let _ = mempool_event_sender().send(encoded);
    }
}

fn publish_mempool_entry_event(entry: &MempoolTransactionStruct, event: &str) {
    let mempool_block = entry.position.as_ref().map(|position| position.block);
    publish_mempool_event(&json!({
        "type": "tx",
        "data": {
            "event": event,
            "status": "mempool",
            "txid": entry.txid.to_string(),
            "mempool_block": mempool_block,
            "addresses": entry.addresses,
        }
    }));

    for address in &entry.addresses {
        publish_mempool_event(&json!({
            "type": "address-tx",
            "data": {
                "event": event,
                "status": "mempool",
                "address": address,
                "txid": entry.txid.to_string(),
                "mempool_block": mempool_block,
            }
        }));
    }
}

pub fn publish_confirmed_tx_events(
    height: u32,
    timestamp: u32,
    txids: &[Txid],
    address_txs: &HashMap<String, Vec<Txid>>,
) {
    publish_mempool_event(&json!({
        "type": "tx",
        "data": {
            "event": "confirmed",
            "status": "confirmed",
            "height": height,
            "timestamp": timestamp,
            "txids": txids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }
    }));

    if !address_txs.is_empty() {
        let addresses: HashMap<&String, Vec<String>> = address_txs
            .iter()
            .map(|(address, txids)| {
                (address, txids.iter().map(ToString::to_string).collect::<Vec<_>>())
            })
            .collect();
        publish_mempool_event(&json!({
            "type": "address-tx",
            "data": {
                "event": "confirmed",
                "status": "confirmed",
                "height": height,
                "timestamp": timestamp,
                "addresses": addresses,
            }
        }));
    }
}

fn update_mempool_status<F>(f: F)
where
    F: FnOnce(&mut MempoolSyncStatus),
{
    let Ok(mut state) = mempool_state().write() else { return };
    f(&mut state.status);
    state.updated_at = now_ts();
}

fn mark_raw_refresh_start() {
    update_mempool_status(|status| {
        status.phase = MempoolSyncPhase::Syncing;
        status.in_sync = false;
        status.stale = false;
        status.last_raw_refresh_at = Some(now_ts());
        status.last_error = None;
    });
}

fn mark_raw_refresh_success() {
    update_mempool_status(|status| {
        status.phase =
            if status.hydrating { MempoolSyncPhase::Hydrating } else { MempoolSyncPhase::InSync };
        status.in_sync = !status.hydrating;
        status.stale = false;
        status.last_successful_raw_refresh_at = Some(now_ts());
        status.last_error = None;
    });
}

fn mark_raw_refresh_error(error: &anyhow::Error) {
    let message = format!("{error:?}");
    update_mempool_status(|status| {
        status.phase = MempoolSyncPhase::Stale;
        status.in_sync = false;
        status.stale = true;
        status.last_error = Some(message);
    });
}

fn set_hydration_status(hydrating: bool, pending: usize) {
    update_mempool_status(|status| {
        status.hydrating = hydrating;
        status.hydration_pending = pending;
        if hydrating {
            status.phase = MempoolSyncPhase::Hydrating;
            status.in_sync = false;
        } else if status.stale {
            status.phase = MempoolSyncPhase::Stale;
            status.in_sync = false;
        } else {
            status.phase = MempoolSyncPhase::InSync;
            status.in_sync = true;
        }
    });
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn protostones_for_tx(tx: &Transaction) -> Vec<Protostone> {
    match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => Protostone::from_runestone(&r).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn is_uncommon_goods_mint_tx(tx: &Transaction) -> bool {
    Runestone::decipher(tx)
        .as_ref()
        .and_then(|artifact| artifact.mint())
        .map(|id| SchemaRuneId::from(id) == UNCOMMON_GOODS_RUNE_ID)
        .unwrap_or(false)
}

fn rune_io_has_activity(io: &TxRuneIo) -> bool {
    !io.inputs.is_empty()
        || !io.outputs.is_empty()
        || !io.burned.is_empty()
        || !io.minted.is_empty()
        || io.etched.is_some()
}

fn mempool_runes_provider() -> &'static RunesProvider {
    static PROVIDER: OnceLock<RunesProvider> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        let mdb = Arc::new(crate::config::espo_mdb(b"runes:"));
        RunesProvider::new(mdb)
    })
}

fn btc_to_sat(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    (value * 100_000_000.0).round().max(0.0) as u64
}

fn tx_inputs(tx: &Transaction) -> Vec<Txid> {
    tx.input
        .iter()
        .filter_map(|vin| (!vin.previous_output.is_null()).then_some(vin.previous_output.txid))
        .collect()
}

fn tx_spent_outpoints(tx: &Transaction) -> Vec<OutPoint> {
    tx.input
        .iter()
        .filter_map(|vin| (!vin.previous_output.is_null()).then_some(vin.previous_output))
        .collect()
}

fn cellpack_from_protostone(
    protostone: &Protostone,
) -> Option<alkanes_support::cellpack::Cellpack> {
    if protostone.protocol_tag != 1 || protostone.message.is_empty() {
        return None;
    }
    let calldata: Vec<u8> = protostone.message.iter().flat_map(|v| v.to_be_bytes()).collect();
    let Ok(values) = decode_varint_list(&mut Cursor::new(calldata)) else {
        return None;
    };
    TryInto::<alkanes_support::cellpack::Cellpack>::try_into(values).ok()
}

fn is_diesel_mint_protostone(protostones: &[Protostone]) -> bool {
    protostones.iter().any(|protostone| {
        let Some(cellpack) = cellpack_from_protostone(protostone) else {
            return false;
        };
        cellpack.target.block == 2
            && cellpack.target.tx == 0
            && cellpack.inputs.first() == Some(&77)
    })
}

fn block_subsidy_sats(height: u64) -> u64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        return 0;
    }
    5_000_000_000u64 >> halvings
}

fn hex_u128(value: u128) -> String {
    format!("0x{value:x}")
}

fn mempool_essentials_provider() -> &'static EssentialsProvider {
    static PROVIDER: OnceLock<EssentialsProvider> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        let mdb = Arc::new(crate::config::espo_mdb(b"essentials:"));
        EssentialsProvider::new(mdb)
    })
}

fn alkane_transfer_json(id: &SchemaAlkaneId, value: u128) -> Value {
    json!({
        "id": {
            "block": hex_u128(id.block as u128),
            "tx": hex_u128(id.tx as u128),
        },
        "value": hex_u128(value),
    })
}

fn input_alkane_balances_for_tx(tx: &Transaction) -> Vec<BalanceEntry> {
    let mut outpoints: Vec<(Txid, u32)> = tx
        .input
        .iter()
        .filter_map(|vin| {
            if vin.previous_output.is_null() {
                None
            } else {
                Some((vin.previous_output.txid, vin.previous_output.vout))
            }
        })
        .collect();
    outpoints.sort();
    outpoints.dedup();
    if outpoints.is_empty() {
        return Vec::new();
    }

    let Ok(rows) = get_outpoint_balances_with_spent_batch(
        StateAt::Latest,
        mempool_essentials_provider(),
        &outpoints,
    ) else {
        return Vec::new();
    };

    let mut by_alkane: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    for lookup in rows.values() {
        for balance in &lookup.balances {
            if balance.amount == 0 {
                continue;
            }
            *by_alkane.entry(balance.alkane).or_default() = by_alkane
                .get(&balance.alkane)
                .copied()
                .unwrap_or(0)
                .saturating_add(balance.amount);
        }
    }

    by_alkane
        .into_iter()
        .map(|(alkane, amount)| BalanceEntry { alkane, amount })
        .collect()
}

fn add_rune_balances_to_sheet(
    entries: impl IntoIterator<Item = RuneBalance>,
    sheet: &mut RuneSheet<SchemaRuneId>,
) {
    for balance in entries {
        if balance.amount == 0 {
            continue;
        }
        let amount = sheet.entry(balance.id).or_default();
        *amount = amount.saturating_add(balance.amount);
    }
}

fn rune_balances_from_sheet(sheet: &RuneSheet<SchemaRuneId>) -> Vec<RuneBalance> {
    sheet
        .iter()
        .filter_map(|(id, amount)| {
            (*amount > 0).then_some(RuneBalance { id: *id, amount: *amount })
        })
        .collect()
}

fn projected_rune_output_row(
    tx: &Transaction,
    vout: u32,
    balances: Vec<RuneBalance>,
    network: Network,
) -> Option<OutpointRuneBalances> {
    let output = tx.output.get(vout as usize)?;
    Some(OutpointRuneBalances {
        address: Address::from_script(output.script_pubkey.as_script(), network)
            .ok()
            .map(|address| address.to_string()),
        script_pubkey: output.script_pubkey.to_bytes(),
        balances,
    })
}

fn mempool_rune_mint_amount(
    provider: &RunesProvider,
    id: SchemaRuneId,
    height: u64,
    tx_index: u32,
    minted_counts: &HashMap<SchemaRuneId, u128>,
) -> Option<u128> {
    if id == UNCOMMON_GOODS_RUNE_ID {
        return Some(1);
    }

    let entry = provider.get_rune(id).ok().flatten()?;
    let local_mints = minted_counts.get(&id).copied().unwrap_or_default();
    let cap = entry.terms.as_ref().and_then(|terms| terms.cap).unwrap_or_default();
    if entry.mints.saturating_add(local_mints) >= cap {
        return None;
    }
    entry.mintable(height, tx_index)
}

fn project_rune_io_for_block(
    txids: &[Txid],
    txs: &HashMap<Txid, MempoolTransactionStruct>,
    height: u64,
) -> HashMap<Txid, TxRuneIo> {
    let provider = mempool_runes_provider();
    let rules = TransferRules::default();
    let network = get_network();
    let mut projected_outpoints: HashMap<(Txid, u32), OutpointRuneBalances> = HashMap::new();
    let mut minted_counts: HashMap<SchemaRuneId, u128> = HashMap::new();
    let mut out = HashMap::new();

    for (tx_index, txid) in txids.iter().enumerate() {
        let Some(tx) = txs.get(txid).and_then(|entry| entry.tx.as_ref()) else {
            continue;
        };
        let artifact = Runestone::decipher(tx);
        let mut io = TxRuneIo::default();
        let mut unallocated: RuneSheet<SchemaRuneId> = BTreeMap::new();

        for (input_idx, input) in tx.input.iter().enumerate() {
            if input.previous_output.is_null() {
                continue;
            }
            let prev = input.previous_output;
            let row = projected_outpoints
                .remove(&(prev.txid, prev.vout))
                .or_else(|| provider.get_outpoint_balances(&prev.txid, prev.vout).ok().flatten());
            let Some(row) = row else {
                continue;
            };
            io.inputs.insert(input_idx as u32, row.balances.clone());
            add_rune_balances_to_sheet(row.balances, &mut unallocated);
        }

        if artifact.is_none() && unallocated.is_empty() {
            continue;
        }

        let tx_index_u32 = tx_index.saturating_add(1).min(u32::MAX as usize) as u32;
        let etched_id = match artifact.as_ref() {
            Some(Artifact::Runestone(runestone)) if runestone.etching.is_some() => {
                let id = SchemaRuneId { block: height, tx: tx_index_u32 };
                io.etched = Some(id);
                if let Some(premine) =
                    runestone.etching.as_ref().and_then(|etching| etching.premine)
                {
                    *unallocated.entry(id).or_default() =
                        unallocated.get(&id).copied().unwrap_or(0).saturating_add(premine);
                }
                Some(id)
            }
            Some(Artifact::Cenotaph(cenotaph)) if cenotaph.etching.is_some() => {
                let id = SchemaRuneId { block: height, tx: tx_index_u32 };
                io.etched = Some(id);
                Some(id)
            }
            _ => None,
        };

        if let Some(artifact) = artifact.as_ref() {
            if let Some(id) = artifact.mint() {
                let id = SchemaRuneId::from(id);
                if let Some(amount) =
                    mempool_rune_mint_amount(provider, id, height, tx_index_u32, &minted_counts)
                {
                    *unallocated.entry(id).or_default() =
                        unallocated.get(&id).copied().unwrap_or(0).saturating_add(amount);
                    *minted_counts.entry(id).or_default() =
                        minted_counts.get(&id).copied().unwrap_or(0).saturating_add(1);
                    io.minted.push(RuneBalance { id, amount });
                }
            }
        }

        let mut allocated: OutputRuneSheets<SchemaRuneId> = BTreeMap::new();
        if let Some(Artifact::Runestone(runestone)) = artifact.as_ref() {
            for Edict { id, amount, output } in runestone.edicts.iter().copied() {
                let resolved_id = if id == RuneId::default() {
                    let Some(etched_id) = etched_id else {
                        continue;
                    };
                    etched_id
                } else {
                    id.into()
                };
                rules.apply_edict(
                    tx,
                    &mut unallocated,
                    &mut allocated,
                    resolved_id,
                    amount,
                    output,
                );
            }
        }

        let burned = if matches!(artifact, Some(Artifact::Cenotaph(_))) {
            unallocated
        } else {
            let pointer = match artifact.as_ref() {
                Some(Artifact::Runestone(runestone)) => runestone.pointer,
                _ => None,
            };
            rules.route_leftovers(tx, unallocated, &mut allocated, pointer)
        };
        io.burned = rune_balances_from_sheet(&burned);

        for (vout, sheet) in allocated {
            if sheet.is_empty() {
                continue;
            }
            let balances = rune_balances_from_sheet(&sheet);
            if balances.is_empty() {
                continue;
            }
            if tx
                .output
                .get(vout as usize)
                .map(|output| output.script_pubkey.is_op_return())
                .unwrap_or(false)
            {
                io.burned.extend(balances);
                continue;
            }
            if let Some(row) = projected_rune_output_row(tx, vout, balances.clone(), network) {
                projected_outpoints.insert((*txid, vout), row);
                io.outputs.insert(vout, balances);
            }
        }

        if rune_io_has_activity(&io) {
            out.insert(*txid, io);
        }
    }

    out
}

fn diesel_trace_for_tx(
    txid: &Txid,
    tx: &Transaction,
    vout: u32,
    mint_amount: u128,
    input_balances: &[BalanceEntry],
) -> Option<Vec<EspoTrace>> {
    let mut raw: Value = serde_json::from_str(include_str!("diesel-mint-trace.json")).ok()?;
    let input_transfer_json: Vec<Value> = input_balances
        .iter()
        .filter(|entry| entry.amount > 0)
        .map(|entry| alkane_transfer_json(&entry.alkane, entry.amount))
        .collect();
    if let Some(incoming) = raw
        .get_mut(0)
        .and_then(|v| v.get_mut("data"))
        .and_then(|v| v.get_mut("context"))
        .and_then(|v| v.get_mut("incomingAlkanes"))
        .and_then(|v| v.as_array_mut())
    {
        if let Some(first) = incoming.get_mut(0) {
            first["value"] = Value::String(hex_u128(mint_amount));
        }
        incoming.extend(input_transfer_json.iter().cloned());
    }
    if let Some(v) = raw
        .get_mut(0)
        .and_then(|v| v.get_mut("data"))
        .and_then(|v| v.get_mut("context"))
        .and_then(|v| v.get_mut("vout"))
    {
        *v = json!(vout);
    }
    if let Some(alkanes) = raw
        .get_mut(3)
        .and_then(|v| v.get_mut("data"))
        .and_then(|v| v.get_mut("response"))
        .and_then(|v| v.get_mut("alkanes"))
        .and_then(|v| v.as_array_mut())
    {
        if let Some(first) = alkanes.get_mut(0) {
            first["value"] = Value::String(hex_u128(mint_amount));
        }
        alkanes.extend(input_transfer_json);
    }

    let events: Vec<EspoSandshrewLikeTraceEvent> = serde_json::from_value(raw).ok()?;
    let sandshrew_trace = EspoSandshrewLikeTrace { outpoint: format!("{}:{}", txid, vout), events };
    let outpoint = EspoOutpoint { txid: txid.to_byte_array().to_vec(), vout, tx_spent: None };
    let protobuf_trace = alkanes_support::proto::alkanes::AlkanesTrace::default();
    let storage_changes =
        extract_alkane_storage(&protobuf_trace, tx, &EspoHostFunctionValues::default())
            .unwrap_or_default();
    Some(vec![EspoTrace { sandshrew_trace, protobuf_trace, storage_changes, outpoint }])
}

fn trace_short_id_from_schema(id: &SchemaAlkaneId) -> EspoSandshrewLikeTraceShortId {
    EspoSandshrewLikeTraceShortId { block: hex_u128(id.block as u128), tx: hex_u128(id.tx as u128) }
}

fn fast_trace_for_protostone(
    txid: &Txid,
    tx: &Transaction,
    vout: u32,
    protostone: &Protostone,
) -> Option<EspoTrace> {
    let cellpack = cellpack_from_protostone(protostone)?;
    let contract_id = SchemaAlkaneId {
        block: cellpack.target.block.try_into().ok()?,
        tx: cellpack.target.tx.try_into().ok()?,
    };
    let invoke = EspoSandshrewLikeTraceEvent::Invoke(EspoSandshrewLikeTraceInvokeData {
        typ: "call".to_string(),
        context: EspoSandshrewLikeTraceInvokeContext {
            myself: trace_short_id_from_schema(&contract_id),
            caller: EspoSandshrewLikeTraceShortId {
                block: "0x0".to_string(),
                tx: "0x0".to_string(),
            },
            inputs: cellpack.inputs.iter().map(|value| hex_u128(*value)).collect(),
            incoming_alkanes: Vec::new(),
            vout,
        },
        fuel: 0,
    });
    let sandshrew_trace =
        EspoSandshrewLikeTrace { outpoint: format!("{}:{}", txid, vout), events: vec![invoke] };
    let protobuf_trace = alkanes_support::proto::alkanes::AlkanesTrace::default();
    let storage_changes =
        extract_alkane_storage(&protobuf_trace, tx, &EspoHostFunctionValues::default())
            .unwrap_or_default();
    let outpoint = EspoOutpoint { txid: txid.to_byte_array().to_vec(), vout, tx_spent: None };

    Some(EspoTrace { sandshrew_trace, protobuf_trace, storage_changes, outpoint })
}

fn fast_traces_for_tx(
    txid: &Txid,
    tx: &Transaction,
    protostones: &[Protostone],
) -> Option<Vec<EspoTrace>> {
    if protostones.is_empty() {
        return None;
    }

    let base = shadow_base(tx);
    let traces = protostones
        .iter()
        .enumerate()
        .filter_map(|(idx, protostone)| {
            fast_trace_for_protostone(txid, tx, base + idx as u32, protostone)
        })
        .collect();
    Some(traces)
}

fn combined_traces(entry: &MempoolTransactionStruct) -> Option<Vec<EspoTrace>> {
    entry.diesel_trace.clone().or_else(|| entry.fixed_trace.clone())
}

fn mempool_entry_from_state(entry: &MempoolTransactionStruct) -> Option<MempoolEntry> {
    let tx = entry.tx.clone()?;
    Some(MempoolEntry {
        txid: entry.txid,
        tx,
        traces: combined_traces(entry),
        rune_io: entry.rune_io.clone(),
        has_alkane_action: entry_has_alkane_action(entry),
        has_rune_action: entry_has_rune_action(entry),
        defer_alkane_trace_status: entry_defers_alkane_trace_status(entry),
        first_seen: entry.first_seen,
        position: entry.position.clone(),
    })
}

fn entry_has_alkane_action(entry: &MempoolTransactionStruct) -> bool {
    !entry.protostones.is_empty()
        || entry.fixed_trace.as_ref().map_or(false, |traces| !traces.is_empty())
        || entry.diesel_trace.as_ref().map_or(false, |traces| !traces.is_empty())
}

fn entry_defers_alkane_trace_status(entry: &MempoolTransactionStruct) -> bool {
    entry_has_alkane_action(entry) && !entry.is_diesel_mint
}

fn entry_has_rune_action(entry: &MempoolTransactionStruct) -> bool {
    entry.rune_io.as_ref().map(rune_io_has_activity).unwrap_or(false)
}

fn entry_spends_live_outpoint(
    entry: &MempoolTransactionStruct,
    include_alkanes: bool,
    include_runes: bool,
    live_alkane_outpoints: &HashSet<(Txid, u32)>,
    live_rune_outpoints: &HashSet<(Txid, u32)>,
) -> bool {
    entry.spent_outpoints.iter().any(|prev| {
        let key = (prev.txid, prev.vout);
        (include_alkanes && live_alkane_outpoints.contains(&key))
            || (include_runes && live_rune_outpoints.contains(&key))
    })
}

/// Whether `entry` spends an output of another mempool transaction that pays
/// `target_spk`.
///
/// Matching is on the script itself rather than on a rendered address string:
/// this runs for every transaction in the mempool on every address page, and
/// deriving an address per candidate output meant a bech32 encode and a
/// `String` allocation each time — tens of thousands of them per page against a
/// mempool this size. A script and an address are one-to-one for a given
/// network, so comparing the scripts answers the same question.
fn entry_spends_mempool_output_to_spk(
    entry: &MempoolTransactionStruct,
    target_spk: &ScriptBuf,
    state: &InMemoryMempool,
) -> bool {
    entry.spent_outpoints.iter().any(|prev| {
        let Some(parent) = state.txs.get(&prev.txid).and_then(|parent| parent.tx.as_ref()) else {
            return false;
        };
        parent
            .output
            .get(prev.vout as usize)
            .is_some_and(|output| output.script_pubkey == *target_spk)
    })
}

fn derive_readiness(entry: &MempoolTransactionStruct) -> MempoolTxReadiness {
    if entry.tx.is_none() {
        MempoolTxReadiness::MetadataOnly
    } else if entry.protostones.is_empty() {
        MempoolTxReadiness::Hydrated
    } else if entry.diesel_trace.is_some() || entry.fixed_trace.is_some() {
        MempoolTxReadiness::TraceReady
    } else {
        MempoolTxReadiness::TracePending
    }
}

fn snapshot_from_state(state: &InMemoryMempool) -> MempoolSnapshot {
    MempoolSnapshot {
        tx_count: state.txs.len(),
        updated_at: state.updated_at,
        sequence: state.sequence,
        status: state.status.clone(),
        blocks: state.templates.clone(),
        deltas: state.deltas.clone(),
    }
}

fn block_summary(template: &MempoolBlockTemplate) -> MempoolBlockSummary {
    MempoolBlockSummary {
        index: template.index,
        tx_count: template.tx_count,
        trace_count: template.trace_count,
        weight: template.weight,
        vsize: template.vsize,
        total_fees: template.total_fees,
        median_fee_rate: template.median_fee_rate,
        min_fee_rate: template.min_fee_rate,
        max_fee_rate: template.max_fee_rate,
        fee_range: template.fee_range.clone(),
    }
}

fn ensure_regtest_mempool_template(templates: &mut Vec<MempoolBlockTemplate>, network: Network) {
    if network != Network::Regtest || !templates.is_empty() {
        return;
    }
    templates.push(MempoolBlockTemplate {
        index: 0,
        tx_count: 0,
        trace_count: 0,
        weight: 0,
        vsize: 0,
        total_fees: 0,
        median_fee_rate: Some(0.0),
        min_fee_rate: Some(0.0),
        max_fee_rate: Some(0.0),
        fee_range: Vec::new(),
        transaction_ids: Vec::new(),
    });
}

fn compact_snapshot_from_state(
    state: &InMemoryMempool,
    include_deltas: bool,
) -> MempoolCompactSnapshot {
    MempoolCompactSnapshot {
        tx_count: state.txs.len(),
        updated_at: state.updated_at,
        sequence: state.sequence,
        status: state.status.clone(),
        blocks: state.templates.iter().map(block_summary).collect(),
        deltas: if include_deltas { state.deltas.clone() } else { Vec::new() },
    }
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn json_encoded_len<T: Serialize>(value: &T) -> usize {
    let mut writer = CountingWriter::default();
    if serde_json::to_writer(&mut writer, value).is_ok() { writer.bytes } else { 0 }
}

fn consensus_encoded_len(tx: &Transaction) -> usize {
    let mut encoded = Vec::new();
    tx.consensus_encode(&mut encoded).ok().map(|_| encoded.len()).unwrap_or(0)
}

fn borsh_encoded_len<T: borsh::BorshSerialize>(value: &T) -> usize {
    borsh::to_vec(value).ok().map(|encoded| encoded.len()).unwrap_or(0)
}

fn storage_changes_payload_len(storage: &crate::alkanes::trace::AlkaneStorageChanges) -> usize {
    storage
        .iter()
        .map(|(_alkane_id, changes)| {
            12usize.saturating_add(
                changes
                    .iter()
                    .map(|(key, (_txid, value))| {
                        key.len().saturating_add(32).saturating_add(value.len())
                    })
                    .sum::<usize>(),
            )
        })
        .sum()
}

fn add_trace_payload_stats(stats: &mut MempoolTracePayloadStats, traces: &[EspoTrace]) {
    stats.tx_count = stats.tx_count.saturating_add(1);
    stats.trace_count = stats.trace_count.saturating_add(traces.len());
    for trace in traces {
        let sandshrew_bytes = json_encoded_len(&trace.sandshrew_trace);
        let protobuf_bytes = trace.protobuf_trace.encoded_len();
        let storage_bytes = storage_changes_payload_len(&trace.storage_changes);
        let outpoint_bytes = borsh_encoded_len(&trace.outpoint);
        stats.trace_payload_bytes = stats
            .trace_payload_bytes
            .saturating_add(sandshrew_bytes)
            .saturating_add(protobuf_bytes)
            .saturating_add(storage_bytes)
            .saturating_add(outpoint_bytes);
        stats.sandshrew_trace_json_bytes =
            stats.sandshrew_trace_json_bytes.saturating_add(sandshrew_bytes);
        stats.protobuf_encoded_bytes = stats.protobuf_encoded_bytes.saturating_add(protobuf_bytes);
        stats.storage_changes_payload_bytes =
            stats.storage_changes_payload_bytes.saturating_add(storage_bytes);
        stats.outpoint_borsh_bytes = stats.outpoint_borsh_bytes.saturating_add(outpoint_bytes);
    }
}

pub fn current_mempool_memory_stats() -> Option<MempoolMemoryStats> {
    let state = mempool_state().read().ok()?;
    let trace_queue_count = trace_queue().lock().ok().map(|queue| queue.len()).unwrap_or_default();
    let mut stats = MempoolMemoryStats {
        updated_at: state.updated_at,
        sequence: state.sequence,
        ..Default::default()
    };
    stats.counts.tx_count = state.txs.len();
    stats.counts.template_count = state.templates.len();
    stats.counts.delta_count = state.deltas.len();
    stats.counts.trace_queue_count = trace_queue_count;

    stats.bytes.templates_json_bytes = json_encoded_len(&state.templates);
    stats.bytes.deltas_json_bytes = json_encoded_len(&state.deltas);
    stats.bytes.status_json_bytes = json_encoded_len(&state.status);
    stats.counts.template_txid_count =
        state.templates.iter().map(|template| template.transaction_ids.len()).sum();
    stats.bytes.template_txid_string_bytes = state
        .templates
        .iter()
        .flat_map(|template| template.transaction_ids.iter())
        .map(|txid| txid.len())
        .sum();

    for entry in state.txs.values() {
        if let Some(tx) = entry.tx.as_ref() {
            stats.counts.hydrated_tx_count = stats.counts.hydrated_tx_count.saturating_add(1);
            stats.bytes.transactions_consensus_bytes = stats
                .bytes
                .transactions_consensus_bytes
                .saturating_add(consensus_encoded_len(tx));
        }
        if !entry.protostones.is_empty() {
            stats.counts.protostone_tx_count = stats.counts.protostone_tx_count.saturating_add(1);
            stats.counts.protostone_count =
                stats.counts.protostone_count.saturating_add(entry.protostones.len());
        }
        if let Some(traces) = entry.fixed_trace.as_ref() {
            stats.counts.fixed_trace_tx_count = stats.counts.fixed_trace_tx_count.saturating_add(1);
            add_trace_payload_stats(&mut stats.fixed_traces, traces);
        }
        if let Some(traces) = entry.diesel_trace.as_ref() {
            stats.counts.diesel_trace_tx_count =
                stats.counts.diesel_trace_tx_count.saturating_add(1);
            add_trace_payload_stats(&mut stats.diesel_traces, traces);
        }
        if let Some(rune_io) = entry.rune_io.as_ref() {
            stats.counts.rune_io_tx_count = stats.counts.rune_io_tx_count.saturating_add(1);
            stats.bytes.rune_io_borsh_bytes =
                stats.bytes.rune_io_borsh_bytes.saturating_add(borsh_encoded_len(rune_io));
        }
        stats.counts.address_count =
            stats.counts.address_count.saturating_add(entry.addresses.len());
        stats.bytes.addresses_utf8_bytes = stats
            .bytes
            .addresses_utf8_bytes
            .saturating_add(entry.addresses.iter().map(|address| address.len()).sum::<usize>());
        stats.counts.input_txid_count =
            stats.counts.input_txid_count.saturating_add(entry.inputs.len());
        stats.bytes.input_txid_bytes = stats
            .bytes
            .input_txid_bytes
            .saturating_add(entry.inputs.len().saturating_mul(32));
        stats.counts.spent_outpoint_count =
            stats.counts.spent_outpoint_count.saturating_add(entry.spent_outpoints.len());
        stats.bytes.spent_outpoint_bytes = stats
            .bytes
            .spent_outpoint_bytes
            .saturating_add(entry.spent_outpoints.len().saturating_mul(36));
    }

    stats.bytes.fixed_traces_payload_bytes = stats.fixed_traces.trace_payload_bytes;
    stats.bytes.diesel_traces_payload_bytes = stats.diesel_traces.trace_payload_bytes;
    stats.bytes.all_traces_payload_bytes = stats
        .bytes
        .fixed_traces_payload_bytes
        .saturating_add(stats.bytes.diesel_traces_payload_bytes);
    stats.bytes.total_measured_payload_bytes = stats
        .bytes
        .transactions_consensus_bytes
        .saturating_add(stats.bytes.all_traces_payload_bytes)
        .saturating_add(stats.bytes.rune_io_borsh_bytes)
        .saturating_add(stats.bytes.templates_json_bytes)
        .saturating_add(stats.bytes.deltas_json_bytes)
        .saturating_add(stats.bytes.status_json_bytes)
        .saturating_add(stats.bytes.addresses_utf8_bytes)
        .saturating_add(stats.bytes.input_txid_bytes)
        .saturating_add(stats.bytes.spent_outpoint_bytes)
        .saturating_add(stats.bytes.template_txid_string_bytes);

    if let Ok(encoded) = serde_json::to_string(&stats) {
        eprintln!("[mempool][memory_stats] {encoded}");
    }

    Some(stats)
}

pub fn current_mempool_snapshot() -> MempoolSnapshot {
    let Ok(state) = mempool_state().read() else {
        return MempoolSnapshot::default();
    };
    snapshot_from_state(&state)
}

pub fn current_mempool_compact_snapshot() -> MempoolCompactSnapshot {
    let Ok(state) = mempool_state().read() else {
        return MempoolCompactSnapshot::default();
    };
    compact_snapshot_from_state(&state, false)
}

pub fn current_mempool_minimum_fee_rate() -> Option<f64> {
    mempool_state().read().ok()?.minimum_fee_rate
}

pub fn current_mempool_compact_snapshot_with_deltas() -> MempoolCompactSnapshot {
    let Ok(state) = mempool_state().read() else {
        return MempoolCompactSnapshot::default();
    };
    compact_snapshot_from_state(&state, true)
}

pub fn get_mempool_block_transaction_ids(index: usize) -> Vec<String> {
    let Ok(state) = mempool_state().read() else { return Vec::new() };
    state
        .templates
        .iter()
        .find(|template| template.index == index)
        .map(|template| template.transaction_ids.clone())
        .unwrap_or_default()
}

pub fn get_mempool_transactions(txids: &[Txid]) -> HashMap<Txid, Transaction> {
    let Ok(state) = mempool_state().read() else {
        return HashMap::new();
    };
    txids
        .iter()
        .filter_map(|txid| {
            state.txs.get(txid).and_then(|entry| entry.tx.clone().map(|tx| (*txid, tx)))
        })
        .collect()
}

pub fn get_mempool_outspends(txid: &Txid, output_count: usize) -> Vec<Option<Txid>> {
    let mut out = vec![None; output_count];
    let Ok(state) = mempool_state().read() else {
        return out;
    };
    for entry in state.txs.values() {
        for input in &entry.spent_outpoints {
            if input.txid == *txid {
                let idx = input.vout as usize;
                if idx >= out.len() {
                    out.resize(idx + 1, None);
                }
                out[idx] = Some(entry.txid);
            }
        }
    }
    out
}

pub fn get_mempool_block_detail(
    index: usize,
    page: usize,
    limit: usize,
    filter: MempoolTxFilter,
    hide_diesel_mints: bool,
) -> Option<MempoolBlockDetail> {
    let Ok(state) = mempool_state().read() else {
        return None;
    };
    let template = state.templates.iter().find(|template| template.index == index)?.clone();
    let mut ordered: Vec<Txid> = template
        .transaction_ids
        .iter()
        .filter_map(|txid_str| Txid::from_str(txid_str).ok())
        .collect();
    let package_rates = package_effective_rates_for_block(&ordered, &state.txs, &HashMap::new());
    ordered.retain(|txid| {
        let Some(entry) = state.txs.get(txid) else {
            return false;
        };
        if hide_diesel_mints && (entry.is_diesel_mint || entry.is_ug_mint) {
            return false;
        }
        match filter {
            MempoolTxFilter::All => true,
            MempoolTxFilter::Action => {
                entry_has_alkane_action(entry) || entry_has_rune_action(entry)
            }
            MempoolTxFilter::Alkane => entry_has_alkane_action(entry),
            MempoolTxFilter::Rune => entry_has_rune_action(entry),
        }
    });
    let tx_total = ordered.len();
    let off = limit.saturating_mul(page.saturating_sub(1));
    let end = off.saturating_add(limit).min(tx_total);
    let txs = if off < tx_total {
        ordered[off..end]
            .iter()
            .filter_map(|txid| {
                let entry = state.txs.get(txid)?;
                let tx = entry.tx.clone()?;
                Some(MempoolBlockTx {
                    txid: *txid,
                    tx,
                    protostones: entry.protostones.clone(),
                    traces: combined_traces(entry),
                    rune_io: entry.rune_io.clone(),
                    addresses: entry.addresses.clone(),
                    first_seen: entry.first_seen,
                    fee_sat: entry.fee_sat,
                    vsize: entry.vsize,
                    fee_rate: package_rates.get(txid).copied().unwrap_or(entry.fee_rate),
                    position: entry.position.clone(),
                    readiness: derive_readiness(entry),
                    defer_alkane_trace_status: entry_defers_alkane_trace_status(entry),
                })
            })
            .collect()
    } else {
        Vec::new()
    };
    Some(MempoolBlockDetail { template, tx_total, txs })
}

pub fn get_mempool_block_ordered_transactions(index: usize) -> Option<Vec<MempoolBlockTx>> {
    let Ok(state) = mempool_state().read() else {
        return None;
    };
    let template = state.templates.iter().find(|template| template.index == index)?;
    let ordered: Vec<Txid> = template
        .transaction_ids
        .iter()
        .filter_map(|txid_str| Txid::from_str(txid_str).ok())
        .collect();
    let package_rates = package_effective_rates_for_block(&ordered, &state.txs, &HashMap::new());
    Some(
        ordered
            .iter()
            .filter_map(|txid| {
                let entry = state.txs.get(txid)?;
                let tx = entry.tx.clone()?;
                Some(MempoolBlockTx {
                    txid: *txid,
                    tx,
                    protostones: entry.protostones.clone(),
                    traces: combined_traces(entry),
                    rune_io: entry.rune_io.clone(),
                    addresses: entry.addresses.clone(),
                    first_seen: entry.first_seen,
                    fee_sat: entry.fee_sat,
                    vsize: entry.vsize,
                    fee_rate: package_rates.get(txid).copied().unwrap_or(entry.fee_rate),
                    position: entry.position.clone(),
                    readiness: derive_readiness(entry),
                    defer_alkane_trace_status: entry_defers_alkane_trace_status(entry),
                })
            })
            .collect(),
    )
}

pub fn get_mempool_index_transactions_ordered_by_block_and_fee() -> Vec<MempoolBlockTx> {
    let Ok(state) = mempool_state().read() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for template in &state.templates {
        let ordered: Vec<Txid> = template
            .transaction_ids
            .iter()
            .filter_map(|txid_str| Txid::from_str(txid_str).ok())
            .collect();
        let package_rates =
            package_effective_rates_for_block(&ordered, &state.txs, &HashMap::new());
        let mut block_txs = ordered
            .iter()
            .filter_map(|txid| {
                let entry = state.txs.get(txid)?;
                let tx = entry.tx.clone()?;
                Some(MempoolBlockTx {
                    txid: *txid,
                    tx,
                    protostones: entry.protostones.clone(),
                    traces: combined_traces(entry),
                    rune_io: entry.rune_io.clone(),
                    addresses: entry.addresses.clone(),
                    first_seen: entry.first_seen,
                    fee_sat: entry.fee_sat,
                    vsize: entry.vsize,
                    fee_rate: package_rates.get(txid).copied().unwrap_or(entry.fee_rate),
                    position: entry.position.clone(),
                    readiness: derive_readiness(entry),
                    defer_alkane_trace_status: entry_defers_alkane_trace_status(entry),
                })
            })
            .collect::<Vec<_>>();
        block_txs.sort_by(|left, right| {
            right
                .fee_sat
                .cmp(&left.fee_sat)
                .then_with(|| {
                    right.fee_rate.partial_cmp(&left.fee_rate).unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.txid.cmp(&right.txid))
        });
        out.extend(block_txs);
    }
    out
}

/// Returns protostone transactions in canonical mempool order from block zero
/// through the last target. The prefix is required to carry projected vouts and
/// contract state across projected block boundaries.
pub fn get_mempool_block_transactions_for_targets(
    index: usize,
    targets: &HashSet<Txid>,
) -> Option<Vec<MempoolBlockTx>> {
    if targets.is_empty() {
        return Some(Vec::new());
    }

    let Ok(state) = mempool_state().read() else {
        return None;
    };
    let ordered = mempool_projection_prefix(&state.templates, index, targets)?;
    let mut package_rates = HashMap::new();
    for template in state.templates.iter().filter(|template| template.index <= index) {
        let block_ordered: Vec<Txid> = template
            .transaction_ids
            .iter()
            .filter_map(|txid_str| Txid::from_str(txid_str).ok())
            .collect();
        package_rates.extend(package_effective_rates_for_block(
            &block_ordered,
            &state.txs,
            &HashMap::new(),
        ));
    }

    Some(
        ordered
            .iter()
            .filter_map(|txid| {
                let entry = state.txs.get(txid)?;
                if entry.protostones.is_empty() && !targets.contains(txid) {
                    return None;
                }
                let tx = entry.tx.clone()?;
                Some(MempoolBlockTx {
                    txid: *txid,
                    tx,
                    protostones: entry.protostones.clone(),
                    traces: combined_traces(entry),
                    rune_io: entry.rune_io.clone(),
                    addresses: entry.addresses.clone(),
                    first_seen: entry.first_seen,
                    fee_sat: entry.fee_sat,
                    vsize: entry.vsize,
                    fee_rate: package_rates.get(txid).copied().unwrap_or(entry.fee_rate),
                    position: entry.position.clone(),
                    readiness: derive_readiness(entry),
                    defer_alkane_trace_status: entry_defers_alkane_trace_status(entry),
                })
            })
            .collect(),
    )
}

fn mempool_projection_prefix(
    templates: &[MempoolBlockTemplate],
    max_index: usize,
    targets: &HashSet<Txid>,
) -> Option<Vec<Txid>> {
    templates.iter().find(|template| template.index == max_index)?;
    let ordered: Vec<Txid> = templates
        .iter()
        .filter(|template| template.index <= max_index)
        .flat_map(|template| template.transaction_ids.iter())
        .filter_map(|txid| Txid::from_str(txid).ok())
        .collect();
    let last_target = ordered.iter().rposition(|txid| targets.contains(txid))?;
    Some(ordered.into_iter().take(last_target.saturating_add(1)).collect())
}

pub fn get_mempool_block_spenders(index: usize) -> Option<HashMap<(Txid, u32), Txid>> {
    let Ok(state) = mempool_state().read() else {
        return None;
    };
    let template = state.templates.iter().find(|template| template.index == index)?;
    let mut out = HashMap::new();
    for txid in template.transaction_ids.iter().filter_map(|txid| Txid::from_str(txid).ok()) {
        let Some(entry) = state.txs.get(&txid) else {
            continue;
        };
        for prev in &entry.spent_outpoints {
            out.insert((prev.txid, prev.vout), txid);
        }
    }
    Some(out)
}

pub fn publish_new_block_event(height: u32, timestamp: u32, txids: &[Txid]) {
    publish_mempool_event(&json!({
        "type": "block",
        "data": {
            "height": height,
            "timestamp": timestamp,
            "txids": txids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }
    }));
}

fn shadow_base(tx: &Transaction) -> u32 {
    tx.output.len() as u32 + 1
}

fn encode_outpoint_hex(txid: &Txid, vout: u32, height: u32) -> String {
    let mut outpoint = protorune::Outpoint::default();
    outpoint.txid = txid.to_byte_array().to_vec();
    outpoint.vout = vout;
    let mut bytes = Vec::with_capacity(4 + outpoint.encoded_len());
    bytes.extend_from_slice(&height.to_le_bytes());
    bytes.extend_from_slice(&outpoint.encode_to_vec());
    format!("0x{}", hex::encode(bytes))
}

fn decode_trace_hex(
    data_hex: &str,
    txid: &Txid,
    tx: &Transaction,
    vout: u32,
) -> Result<Option<EspoTrace>> {
    let trimmed = data_hex.strip_prefix("0x").unwrap_or(data_hex);
    let bytes = hex::decode(trimmed)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let protobuf_trace = alkanes_support::proto::alkanes::AlkanesTrace::decode(bytes.as_slice())
        .with_context(|| "failed to decode view trace protobuf")?;
    let events = protobuf_trace_events(&protobuf_trace)?;

    let sandshrew_trace = EspoSandshrewLikeTrace { outpoint: format!("{}:{}", txid, vout), events };
    let storage_changes =
        extract_alkane_storage(&protobuf_trace, tx, &EspoHostFunctionValues::default())?;
    let outpoint = EspoOutpoint { txid: txid.to_byte_array().to_vec(), vout, tx_spent: None };

    Ok(Some(EspoTrace { sandshrew_trace, protobuf_trace, storage_changes, outpoint }))
}

fn compact_view_error(error: &Value) -> String {
    let code = error.get("code").and_then(|v| v.as_i64());
    let message = error
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| error.as_str().unwrap_or("unknown metashrew_view error"))
        .lines()
        .next()
        .unwrap_or("unknown metashrew_view error");
    let mut summary: String = message.chars().take(300).collect();
    if message.chars().count() > 300 {
        summary.push_str("...");
    }
    match code {
        Some(code) => format!("code {code}: {summary}"),
        None => summary,
    }
}

async fn view_traces_for_tx(
    http: &Client,
    view_url: &str,
    txid: &Txid,
    tx: &Transaction,
    protostone_count: usize,
) -> Option<Vec<EspoTrace>> {
    if is_shutdown_requested() {
        return None;
    }
    if protostone_count == 0 {
        return None;
    }
    let base = shadow_base(tx);
    let input_height = get_last_safe_tip().unwrap_or_default();
    let mut jobs: Vec<(u32, String)> = Vec::with_capacity(protostone_count);
    for idx in 0..protostone_count {
        let vout = base + idx as u32;
        jobs.push((vout, encode_outpoint_hex(txid, vout, input_height)));
    }

    let mut traces: Vec<EspoTrace> = Vec::new();
    let mut had_error = false;
    for batch in jobs.chunks(MEMPOOL_VIEW_BATCH_SIZE) {
        if is_shutdown_requested() {
            return None;
        }
        let owned_batch: Vec<(u32, String)> = batch.to_vec();
        let futs = stream::iter(owned_batch.into_iter().map(|(vout, input_hex)| {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("{}:{}", txid, vout),
                "method": "metashrew_view",
                "params": [
                    "trace",
                    input_hex,
                    "latest",
                ]
            });
            let http = http.clone();
            let view_url = view_url.to_string();
            let txid = *txid;
            async move {
                if is_shutdown_requested() {
                    return Err(());
                }
                let resp_json: Value = match http.post(&view_url).json(&body).send().await {
                    Ok(r) => match r.error_for_status() {
                        Ok(ok) => match ok.json().await {
                            Ok(v) => v,
                            Err(e) => {
                                if !is_shutdown_requested() {
                                    eprintln!(
                                        "[mempool] view response decode failed for {}@{}: {:?}",
                                        txid, vout, e
                                    );
                                }
                                return Err(());
                            }
                        },
                        Err(e) => {
                            if !is_shutdown_requested() {
                                eprintln!(
                                    "[mempool] view HTTP error for {}@{}: {:?}",
                                    txid, vout, e
                                );
                            }
                            return Err(());
                        }
                    },
                    Err(e) => {
                        if !is_shutdown_requested() {
                            eprintln!("[mempool] view POST failed for {}@{}: {:?}", txid, vout, e);
                        }
                        return Err(());
                    }
                };

                if let Some(error) = resp_json.get("error") {
                    if !is_shutdown_requested() {
                        eprintln!(
                            "[mempool] metashrew_view trace failed for {}@{}: {}",
                            txid,
                            vout,
                            compact_view_error(error)
                        );
                    }
                    return Err(());
                }
                let result_hex = resp_json.get("result").and_then(|v| v.as_str()).or_else(|| {
                    resp_json.get("result").and_then(|v| v.get("trace")).and_then(|v| v.as_str())
                });
                let Some(result_hex) = result_hex else {
                    if !is_shutdown_requested() {
                        eprintln!(
                            "[mempool] metashrew_view trace missing result for {}@{}",
                            txid, vout
                        );
                    }
                    return Err(());
                };
                match decode_trace_hex(result_hex, &txid, tx, vout) {
                    Ok(trace) => Ok(trace),
                    Err(e) => {
                        if !is_shutdown_requested() {
                            eprintln!(
                                "[mempool] decode view trace {}@{} failed: {:?}",
                                txid, vout, e
                            );
                        }
                        Err(())
                    }
                }
            }
        }))
        .buffer_unordered(MEMPOOL_VIEW_BATCH_SIZE);

        futures::pin_mut!(futs);
        while let Some(res) = futs.next().await {
            match res {
                Ok(Some(t)) => {
                    traces.push(t);
                }
                Ok(None) => {}
                Err(()) => {
                    had_error = true;
                }
            }
        }
        if had_error {
            return None;
        }
    }

    if traces.is_empty() { None } else { Some(traces) }
}

pub fn get_seen_txids_page(page: usize, limit: usize) -> (Vec<Txid>, bool) {
    if limit == 0 {
        return (Vec::new(), false);
    }
    let offset = limit.saturating_mul(page.saturating_sub(1));
    let Ok(state) = mempool_state().read() else {
        return (Vec::new(), false);
    };
    let mut entries: Vec<(u64, Txid)> =
        state.txs.values().map(|entry| (entry.first_seen, entry.txid)).collect();
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    let has_more = entries.len() > offset.saturating_add(limit);
    let txids = entries.into_iter().skip(offset).take(limit).map(|(_, txid)| txid).collect();

    (txids, has_more)
}

pub fn decode_seen_key(raw: &[u8]) -> Option<(u64, Txid)> {
    // Kept for compatibility with callers that decode older mempool cursor keys.
    if !raw.starts_with(b"seen/") || raw.len() < 5 + 8 + 1 {
        return None;
    }
    let rest = &raw[5..];
    if rest.len() < 8 + 1 || rest[8] != b'/' {
        return None;
    }
    let mut ts_bytes = [0u8; 8];
    ts_bytes.copy_from_slice(&rest[..8]);
    let ts = u64::from_be_bytes(ts_bytes);
    let txid_str = std::str::from_utf8(&rest[9..]).ok()?;
    let txid = Txid::from_str(txid_str).ok()?;
    Some((ts, txid))
}

pub fn reset_mempool_store() -> Result<()> {
    if let Ok(mut state) = mempool_state().write() {
        let total = state.txs.len();
        state.txs.clear();
        state.templates.clear();
        state.deltas.clear();
        state.minimum_fee_rate = None;
        state.sequence = state.sequence.saturating_add(1);
        state.status = MempoolSyncStatus {
            phase: MempoolSyncPhase::Starting,
            in_sync: false,
            hydrating: false,
            stale: false,
            hydration_pending: 0,
            last_raw_refresh_at: None,
            last_successful_raw_refresh_at: None,
            last_error: None,
            clear_protection_until: None,
        };
        state.updated_at = now_ts();
        eprintln!("[mempool] reset in-memory store: deleted {} txs", total);
    }
    if let Ok(mut queue) = trace_queue().lock() {
        queue.clear();
    }
    Ok(())
}

fn enqueue_trace(txid: Txid) {
    let Ok(mut queue) = trace_queue().lock() else { return };
    if !queue.iter().any(|existing| *existing == txid) {
        queue.push_back(txid);
    }
}

fn prune_trace_queue(removed: &HashSet<Txid>) {
    let Ok(mut queue) = trace_queue().lock() else { return };
    queue.retain(|txid| !removed.contains(txid));
}

fn build_memory_entry(
    txid: Txid,
    tx: Transaction,
    verbose: Option<&VerboseMempoolEntry>,
    network: Network,
) -> MempoolTransactionStruct {
    let protostones = protostones_for_tx(&tx);
    let is_diesel_mint = is_diesel_mint_protostone(&protostones);
    let is_ug_mint = is_uncommon_goods_mint_tx(&tx);
    let readiness = if protostones.is_empty() {
        MempoolTxReadiness::Hydrated
    } else {
        MempoolTxReadiness::TracePending
    };
    let first_seen = now_ts();
    let weight = verbose.and_then(|v| v.weight).unwrap_or_else(|| tx.weight().to_wu() as u64);
    let vsize = verbose.and_then(|v| v.vsize).unwrap_or_else(|| tx.vsize() as u64).max(1);
    let fee_sat = verbose
        .and_then(|v| v.fees.as_ref())
        .and_then(|fees| fees.modified.or(fees.base).or(fees.ancestor))
        .map(btc_to_sat)
        .unwrap_or(0);
    let fee_rate = fee_sat as f64 / vsize as f64;
    let inputs = if let Some(v) = verbose {
        v.depends.iter().filter_map(|s| Txid::from_str(s).ok()).collect()
    } else {
        tx_inputs(&tx)
    };
    let spent_outpoints = tx_spent_outpoints(&tx);
    let addresses = tx
        .output
        .iter()
        .filter_map(|o| Address::from_script(o.script_pubkey.as_script(), network).ok())
        .map(|addr| addr.to_string())
        .collect();
    MempoolTransactionStruct {
        txid,
        tx: Some(tx),
        protostones,
        fixed_trace: None,
        fixed_trace_context: None,
        diesel_trace: None,
        rune_io: None,
        first_seen,
        fee_sat,
        weight,
        vsize,
        fee_rate,
        inputs,
        spent_outpoints,
        addresses,
        is_diesel_mint,
        is_ug_mint,
        template_index: None,
        position: None,
        readiness,
    }
}

fn build_memory_metadata_entry(
    txid: Txid,
    verbose: &VerboseMempoolEntry,
) -> MempoolTransactionStruct {
    let first_seen = now_ts();
    let vsize = verbose.vsize.unwrap_or(1).max(1);
    let weight = verbose.weight.unwrap_or_else(|| vsize.saturating_mul(4)).max(1);
    let fee_sat = verbose
        .fees
        .as_ref()
        .and_then(|fees| fees.modified.or(fees.base).or(fees.ancestor))
        .map(btc_to_sat)
        .unwrap_or(0);
    let fee_rate = fee_sat as f64 / vsize as f64;
    let inputs = verbose.depends.iter().filter_map(|s| Txid::from_str(s).ok()).collect();
    MempoolTransactionStruct {
        txid,
        tx: None,
        protostones: Vec::new(),
        fixed_trace: None,
        fixed_trace_context: None,
        diesel_trace: None,
        rune_io: None,
        first_seen,
        fee_sat,
        weight,
        vsize,
        fee_rate,
        inputs,
        spent_outpoints: Vec::new(),
        addresses: Vec::new(),
        is_diesel_mint: false,
        is_ug_mint: false,
        template_index: None,
        position: None,
        readiness: MempoolTxReadiness::MetadataOnly,
    }
}

fn upsert_memory_entry(entry: MempoolTransactionStruct) {
    let txid = entry.txid;
    let Ok(mut state) = mempool_state().write() else { return };
    let mut should_enqueue = !entry.protostones.is_empty() && !entry.is_diesel_mint;
    let mut removed_conflicts = HashSet::new();
    let mut tx_event = "seen";
    if !entry.spent_outpoints.is_empty() && entry.fee_sat > 0 {
        let spent: HashSet<OutPoint> = entry.spent_outpoints.iter().copied().collect();
        let conflicts: Vec<Txid> = state
            .txs
            .iter()
            .filter_map(|(existing_txid, existing)| {
                if *existing_txid == txid {
                    return None;
                }
                let spends_same_input =
                    existing.spent_outpoints.iter().any(|outpoint| spent.contains(outpoint));
                let is_rbf_replacement = spends_same_input
                    && entry.fee_sat > existing.fee_sat
                    && entry.fee_rate > existing.fee_rate;
                is_rbf_replacement.then_some(*existing_txid)
            })
            .collect();
        for conflict in conflicts {
            if state.txs.remove(&conflict).is_some() {
                removed_conflicts.insert(conflict);
            }
        }
    }
    match state.txs.get_mut(&txid) {
        Some(existing) => {
            tx_event = "updated";
            existing.first_seen = existing.first_seen.min(entry.first_seen);
            existing.weight = entry.weight;
            existing.vsize = entry.vsize.max(1);
            if entry.fee_sat > 0 || existing.fee_sat == 0 {
                existing.fee_sat = entry.fee_sat;
            }
            existing.fee_rate = existing.fee_sat as f64 / existing.vsize as f64;
            existing.inputs = entry.inputs;
            if !entry.spent_outpoints.is_empty() {
                existing.spent_outpoints = entry.spent_outpoints;
            }
            existing.template_index = entry.template_index;
            if entry.tx.is_some() {
                existing.tx = entry.tx;
                existing.protostones = entry.protostones;
                existing.addresses = entry.addresses;
                existing.is_diesel_mint = entry.is_diesel_mint;
                existing.is_ug_mint = entry.is_ug_mint;
                existing.rune_io = None;
                existing.readiness = derive_readiness(existing);
                should_enqueue = !existing.protostones.is_empty()
                    && !existing.is_diesel_mint
                    && existing.fixed_trace.is_none();
            } else {
                should_enqueue = false;
            }
        }
        None => {
            state.txs.insert(txid, entry);
        }
    }
    state.updated_at = now_ts();
    let event_entry = state.txs.get(&txid).cloned();
    drop(state);
    if let Some(entry) = event_entry.as_ref() {
        publish_mempool_entry_event(entry, tx_event);
    }
    if !removed_conflicts.is_empty() {
        eprintln!(
            "[mempool] rbf removed {} conflicting txs replaced by {}",
            removed_conflicts.len(),
            txid
        );
        prune_trace_queue(&removed_conflicts);
    }
    if should_enqueue {
        enqueue_trace(txid);
    }
}

fn remove_missing_memory_entries(canonical: &HashSet<Txid>) -> usize {
    let Ok(mut state) = mempool_state().write() else { return 0 };
    let before = state.txs.len();
    let removed: HashSet<Txid> =
        state.txs.keys().filter(|txid| !canonical.contains(*txid)).copied().collect();
    state.txs.retain(|txid, _| canonical.contains(txid));
    state.updated_at = now_ts();
    let after = state.txs.len();
    drop(state);
    if !removed.is_empty() {
        prune_trace_queue(&removed);
    }
    before.saturating_sub(after)
}

fn remove_memory_txid(txid: &Txid) -> bool {
    let Ok(mut state) = mempool_state().write() else { return false };
    let removed = state.txs.remove(txid).is_some();
    if removed {
        state.updated_at = now_ts();
    }
    drop(state);
    if removed {
        let mut removed_set = HashSet::new();
        removed_set.insert(*txid);
        prune_trace_queue(&removed_set);
    }
    removed
}

fn set_relatives(txid: Txid, pool: &mut HashMap<Txid, MinerTx>, visiting: &mut HashSet<Txid>) {
    if !visiting.insert(txid) {
        return;
    }
    let inputs = pool.get(&txid).map(|tx| tx.inputs.clone()).unwrap_or_default();
    let mut ancestors = HashSet::new();
    for parent in inputs {
        if !pool.contains_key(&parent) {
            continue;
        }
        set_relatives(parent, pool, visiting);
        ancestors.insert(parent);
        if let Some(parent_tx) = pool.get(&parent) {
            for ancestor in &parent_tx.ancestors {
                ancestors.insert(*ancestor);
            }
        }
        if let Some(parent_tx) = pool.get_mut(&parent) {
            parent_tx.children.insert(txid);
        }
    }
    let mut ancestor_fee = pool.get(&txid).map(|tx| tx.fee).unwrap_or_default();
    let mut ancestor_vsize = pool.get(&txid).map(|tx| tx.adjusted_vsize).unwrap_or(1);
    for ancestor in &ancestors {
        if let Some(parent) = pool.get(ancestor) {
            ancestor_fee = ancestor_fee.saturating_add(parent.fee);
            ancestor_vsize = ancestor_vsize.saturating_add(parent.adjusted_vsize);
        }
    }
    if let Some(tx) = pool.get_mut(&txid) {
        tx.ancestors = ancestors;
        tx.ancestor_fee = ancestor_fee;
        tx.ancestor_vsize = ancestor_vsize.max(1);
        tx.score = tx.ancestor_fee as f64 / tx.ancestor_vsize as f64;
    }
    visiting.remove(&txid);
}

fn update_descendants(
    root: Txid,
    pool: &mut HashMap<Txid, MinerTx>,
    modified: &mut Vec<Txid>,
    cluster_rate: f64,
) {
    let mut stack: Vec<Txid> = pool
        .get(&root)
        .map(|tx| tx.children.iter().copied().collect())
        .unwrap_or_default();
    let mut seen = HashSet::new();
    while let Some(child) = stack.pop() {
        if !seen.insert(child) {
            continue;
        }
        let children: Vec<Txid> = pool
            .get(&child)
            .map(|tx| tx.children.iter().copied().collect())
            .unwrap_or_default();
        let mut changed = false;
        if let (Some(root_tx), Some(child_tx)) = (pool.get(&root).cloned(), pool.get_mut(&child)) {
            if child_tx.ancestors.remove(&root) {
                child_tx.ancestor_fee = child_tx.ancestor_fee.saturating_sub(root_tx.fee);
                child_tx.ancestor_vsize =
                    child_tx.ancestor_vsize.saturating_sub(root_tx.adjusted_vsize).max(1);
                child_tx.score = child_tx.ancestor_fee as f64 / child_tx.ancestor_vsize as f64;
                child_tx.dependency_rate = child_tx.dependency_rate.min(cluster_rate);
                changed = true;
            }
        }
        if changed {
            if let Some(child_tx) = pool.get_mut(&child) {
                if !child_tx.modified {
                    child_tx.modified = true;
                    modified.push(child);
                }
            }
        }
        stack.extend(children);
    }
}

fn calculate_block_templates(
    txs: &HashMap<Txid, MempoolTransactionStruct>,
    max_blocks: usize,
    weight_limit: u64,
) -> (Vec<Vec<Txid>>, HashMap<Txid, f64>) {
    let mut pool: HashMap<Txid, MinerTx> = txs
        .iter()
        .map(|(txid, tx)| {
            let adjusted_vsize = tx.vsize.max(1);
            (
                *txid,
                MinerTx {
                    fee: tx.fee_sat,
                    weight: tx.weight.max(adjusted_vsize * 4),
                    adjusted_vsize,
                    fee_rate: tx.fee_rate,
                    dependency_rate: tx.fee_rate,
                    inputs: tx
                        .inputs
                        .iter()
                        .copied()
                        .filter(|parent| txs.contains_key(parent))
                        .collect(),
                    spent_outpoints: tx.spent_outpoints.clone(),
                    ancestors: HashSet::new(),
                    children: HashSet::new(),
                    ancestor_fee: tx.fee_sat,
                    ancestor_vsize: adjusted_vsize,
                    score: tx.fee_rate,
                    used: false,
                    modified: false,
                },
            )
        })
        .collect();

    let keys: Vec<Txid> = pool.keys().copied().collect();
    for txid in keys {
        set_relatives(txid, &mut pool, &mut HashSet::new());
    }

    let mut mempool_array: Vec<Txid> = pool.keys().copied().collect();
    mempool_array.sort_by(|a, b| {
        let aa = pool.get(a).map(|tx| tx.score).unwrap_or_default();
        let bb = pool.get(b).map(|tx| tx.score).unwrap_or_default();
        bb.partial_cmp(&aa).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
    });

    let mut blocks = Vec::new();
    let mut block = Vec::new();
    let mut block_weight = 4_000u64;
    let mut modified: Vec<Txid> = Vec::new();
    let mut overflow: Vec<Txid> = Vec::new();
    let mut failures = 0usize;
    let mut top = 0usize;
    let mut spent_outpoints: HashSet<OutPoint> = HashSet::new();
    let mut effective_rates: HashMap<Txid, f64> = HashMap::new();

    while top < mempool_array.len() || !modified.is_empty() {
        while top < mempool_array.len()
            && pool.get(&mempool_array[top]).map(|tx| tx.used || tx.modified).unwrap_or(true)
        {
            top += 1;
        }
        modified.sort_by(|a, b| {
            let aa = pool.get(a).map(|tx| tx.score).unwrap_or_default();
            let bb = pool.get(b).map(|tx| tx.score).unwrap_or_default();
            bb.partial_cmp(&aa).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
        });

        let next_pool = mempool_array.get(top).copied();
        let next_modified = modified.first().copied();
        let next = match (next_pool, next_modified) {
            (Some(pool_tx), Some(mod_tx)) => {
                let pool_score = pool.get(&pool_tx).map(|tx| tx.score).unwrap_or_default();
                let mod_score = pool.get(&mod_tx).map(|tx| tx.score).unwrap_or_default();
                if pool_score > mod_score {
                    top += 1;
                    Some(pool_tx)
                } else {
                    modified.remove(0);
                    Some(mod_tx)
                }
            }
            (Some(pool_tx), None) => {
                top += 1;
                Some(pool_tx)
            }
            (None, Some(mod_tx)) => {
                modified.remove(0);
                Some(mod_tx)
            }
            (None, None) => None,
        };

        if let Some(next_txid) = next {
            if pool.get(&next_txid).map(|tx| tx.used).unwrap_or(true) {
                continue;
            }
            let next_tx = pool.get(&next_txid).cloned();
            let Some(next_tx) = next_tx else { continue };
            let package_fits = blocks.len() >= max_blocks.saturating_sub(1)
                || block_weight.saturating_add(next_tx.ancestor_vsize.saturating_mul(4))
                    < weight_limit;
            if package_fits {
                let mut package: Vec<Txid> = next_tx.ancestors.iter().copied().collect();
                package.sort_by_key(|txid| {
                    pool.get(txid).map(|tx| tx.ancestors.len()).unwrap_or_default()
                });
                package.push(next_txid);
                let package_conflicts = package.iter().any(|txid| {
                    pool.get(txid)
                        .map(|tx| {
                            tx.spent_outpoints
                                .iter()
                                .any(|outpoint| spent_outpoints.contains(outpoint))
                        })
                        .unwrap_or(false)
                });
                if package_conflicts {
                    if let Some(tx) = pool.get_mut(&next_txid) {
                        tx.used = true;
                    }
                    failures += 1;
                    continue;
                }
                let effective_rate = next_tx
                    .dependency_rate
                    .min(next_tx.ancestor_fee as f64 / next_tx.ancestor_vsize.max(1) as f64);
                let mut used = Vec::new();
                for txid in package {
                    if let Some(tx) = pool.get_mut(&txid) {
                        if tx.used {
                            continue;
                        }
                        tx.used = true;
                        tx.fee_rate = effective_rate;
                        for outpoint in &tx.spent_outpoints {
                            spent_outpoints.insert(*outpoint);
                        }
                        effective_rates.insert(txid, effective_rate);
                        block_weight = block_weight.saturating_add(tx.weight);
                        block.push(txid);
                        used.push(txid);
                    }
                }
                for txid in used {
                    update_descendants(txid, &mut pool, &mut modified, effective_rate);
                }
                failures = 0;
            } else {
                overflow.push(next_txid);
                failures += 1;
            }
        }

        let exceeded_tries = failures > 1000 && block_weight > weight_limit.saturating_sub(4_000);
        let queue_empty = top >= mempool_array.len() && modified.is_empty();
        if (exceeded_tries || queue_empty) && blocks.len() < max_blocks.saturating_sub(1) {
            if block.is_empty() {
                break;
            }
            blocks.push(block);
            block = Vec::new();
            block_weight = 4_000;
            for txid in overflow.drain(..).rev() {
                if pool.get(&txid).map(|tx| tx.modified).unwrap_or(false) {
                    modified.push(txid);
                } else {
                    top = top.saturating_sub(1);
                    if top < mempool_array.len() {
                        mempool_array[top] = txid;
                    }
                }
            }
        }
    }

    if !block.is_empty() {
        blocks.push(block);
    }
    (blocks, effective_rates)
}

fn fee_stats_for_block(
    block_txs: &[Txid],
    state: &HashMap<Txid, MempoolTransactionStruct>,
    effective_rates: &HashMap<Txid, f64>,
) -> (Option<f64>, Option<f64>, Option<f64>, Vec<f64>) {
    let mut rates: Vec<f64> = block_txs
        .iter()
        .filter_map(|txid| {
            effective_rates
                .get(txid)
                .copied()
                .or_else(|| state.get(txid).map(|tx| tx.fee_rate))
        })
        .collect();
    rates.retain(|v| v.is_finite());
    if rates.is_empty() {
        return (None, None, None, Vec::new());
    }
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min = rates.first().copied();
    let max = rates.last().copied();
    let median = rates.get(rates.len() / 2).copied();
    let last = rates.len().saturating_sub(1);
    let fee_range = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0]
        .iter()
        .filter_map(|percentile| rates.get((last as f64 * percentile).round() as usize).copied())
        .collect();
    (median, min, max, fee_range)
}

fn package_effective_rates_for_block(
    block_txs: &[Txid],
    state: &HashMap<Txid, MempoolTransactionStruct>,
    selected_rates: &HashMap<Txid, f64>,
) -> HashMap<Txid, f64> {
    let block_set: HashSet<Txid> = block_txs.iter().copied().collect();
    let mut graph: HashMap<Txid, Vec<Txid>> = HashMap::new();
    for txid in block_txs {
        graph.entry(*txid).or_default();
        let Some(tx) = state.get(txid) else { continue };
        for parent in &tx.inputs {
            if !block_set.contains(parent) {
                continue;
            }
            graph.entry(*txid).or_default().push(*parent);
            graph.entry(*parent).or_default().push(*txid);
        }
    }

    let mut out = HashMap::new();
    let mut seen = HashSet::new();
    for txid in block_txs {
        if !seen.insert(*txid) {
            continue;
        }
        let mut stack = vec![*txid];
        let mut component = Vec::new();
        while let Some(cur) = stack.pop() {
            component.push(cur);
            if let Some(neighbors) = graph.get(&cur) {
                for next in neighbors {
                    if seen.insert(*next) {
                        stack.push(*next);
                    }
                }
            }
        }

        let total_fee: u64 =
            component.iter().filter_map(|id| state.get(id).map(|tx| tx.fee_sat)).sum();
        let total_vsize: u64 =
            component.iter().filter_map(|id| state.get(id).map(|tx| tx.vsize.max(1))).sum();
        let package_rate = if component.len() > 1 && total_vsize > 0 {
            Some(total_fee as f64 / total_vsize as f64)
        } else {
            None
        };
        for id in component {
            let rate = package_rate
                .or_else(|| selected_rates.get(&id).copied())
                .or_else(|| state.get(&id).map(|tx| tx.fee_rate))
                .unwrap_or_default();
            out.insert(id, rate);
        }
    }

    out
}

fn calculate_template_deltas(
    previous: &[MempoolBlockTemplate],
    current: &[MempoolBlockTemplate],
    sequence: u64,
) -> Vec<MempoolBlockDelta> {
    const INLINE_DELTA_LIMIT: usize = 512;
    let mut deltas = Vec::new();
    for index in 0..previous.len().max(current.len()) {
        let prev_ids: HashSet<String> = previous
            .get(index)
            .map(|template| template.transaction_ids.iter().cloned().collect())
            .unwrap_or_default();
        let curr_ids: HashSet<String> = current
            .get(index)
            .map(|template| template.transaction_ids.iter().cloned().collect())
            .unwrap_or_default();
        let mut added: Vec<String> = curr_ids.difference(&prev_ids).cloned().collect();
        let mut removed: Vec<String> = prev_ids.difference(&curr_ids).cloned().collect();
        added.sort();
        removed.sort();
        let summary_changed = previous.get(index) != current.get(index);
        if !added.is_empty() || !removed.is_empty() || summary_changed {
            let added_count = added.len();
            let removed_count = removed.len();
            let reset = added_count.saturating_add(removed_count) > INLINE_DELTA_LIMIT;
            if reset {
                added.clear();
                removed.clear();
            }
            deltas.push(MempoolBlockDelta {
                index,
                sequence,
                reset,
                added_count,
                removed_count,
                added,
                removed,
                changed: Vec::new(),
                full: None,
            });
        }
    }
    deltas
}

fn recalculate_memory_templates() {
    let Ok(_recalculate_guard) = recalculate_templates_lock().try_lock() else {
        return;
    };
    let cfg = get_config().mempool.clone();
    let next_height = crate::config::get_espo_next_height() as u64;
    let template_input = {
        let Ok(state) = mempool_state().read() else { return };
        state
            .txs
            .iter()
            .map(|(txid, tx)| {
                (
                    *txid,
                    MempoolTransactionStruct {
                        txid: *txid,
                        tx: None,
                        protostones: Vec::new(),
                        fixed_trace: None,
                        fixed_trace_context: None,
                        diesel_trace: None,
                        rune_io: None,
                        first_seen: tx.first_seen,
                        fee_sat: tx.fee_sat,
                        weight: tx.weight,
                        vsize: tx.vsize,
                        fee_rate: tx.fee_rate,
                        inputs: tx.inputs.clone(),
                        spent_outpoints: tx.spent_outpoints.clone(),
                        addresses: Vec::new(),
                        is_diesel_mint: tx.is_diesel_mint,
                        is_ug_mint: tx.is_ug_mint,
                        template_index: None,
                        position: None,
                        readiness: tx.readiness.clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>()
    };
    let (mut template_txids, effective_rates) =
        calculate_block_templates(&template_input, cfg.template_blocks, cfg.block_weight_units);

    let selected_txids: HashSet<Txid> =
        template_txids.iter().flat_map(|txids| txids.iter().copied()).collect();
    let mut template_state: HashMap<Txid, MempoolTransactionStruct> = {
        let Ok(state) = mempool_state().read() else { return };
        selected_txids
            .iter()
            .filter_map(|txid| state.txs.get(txid).map(|tx| (*txid, tx.clone())))
            .collect()
    };

    for txids in &mut template_txids {
        txids.retain(|txid| template_state.contains_key(txid));
    }
    template_txids.retain(|txids| !txids.is_empty());

    let mut stale_trace_txids: HashSet<Txid> = HashSet::new();
    for txids in &template_txids {
        for txid in txids {
            let Some(tx) = template_state.get(txid) else {
                continue;
            };
            if tx.is_diesel_mint || tx.protostones.is_empty() {
                continue;
            }
            if tx.fixed_trace.is_none() {
                stale_trace_txids.insert(*txid);
            }
        }
    }

    for tx in template_state.values_mut() {
        tx.template_index = None;
        tx.diesel_trace = None;
        tx.rune_io = None;
        tx.position = None;
        tx.readiness = derive_readiness(tx);
    }

    struct TemplateTxUpdate {
        template_index: usize,
        position: MempoolProjectedPosition,
        diesel_trace: Option<Vec<EspoTrace>>,
        rune_io: Option<TxRuneIo>,
    }

    let mut templates = Vec::with_capacity(template_txids.len());
    let mut tx_updates: HashMap<Txid, TemplateTxUpdate> = HashMap::new();
    for (index, txids) in template_txids.iter().enumerate() {
        let package_rates =
            package_effective_rates_for_block(txids, &template_state, &effective_rates);
        let diesel_mints: Vec<Txid> = txids
            .iter()
            .filter(|txid| template_state.get(*txid).map(|tx| tx.is_diesel_mint).unwrap_or(false))
            .copied()
            .collect();
        let per_mint = if diesel_mints.is_empty() {
            0
        } else {
            block_subsidy_sats(next_height + index as u64) as u128 / diesel_mints.len() as u128
        };
        for txid in &diesel_mints {
            if let Some(tx) = template_state.get_mut(txid) {
                if let Some(transaction) = tx.tx.as_ref() {
                    let vout = shadow_base(transaction);
                    let input_balances = input_alkane_balances_for_tx(transaction);
                    tx.diesel_trace =
                        diesel_trace_for_tx(txid, transaction, vout, per_mint, &input_balances);
                }
            }
        }
        if runes_enabled_from_global_config() {
            let rune_ios =
                project_rune_io_for_block(txids, &template_state, next_height + index as u64);
            for (txid, io) in rune_ios {
                if let Some(tx) = template_state.get_mut(&txid) {
                    tx.rune_io = Some(io);
                }
            }
        }

        let mut weight = 0u64;
        let mut vsize = 0u64;
        let mut fees = 0u64;
        let mut trace_count = 0usize;
        for txid in txids {
            if let Some(tx) = template_state.get_mut(txid) {
                let position = MempoolProjectedPosition {
                    block: index,
                    vsize: vsize.saturating_add(tx.vsize / 2),
                };
                tx.template_index = Some(index);
                tx.position = Some(position.clone());
                weight = weight.saturating_add(tx.weight);
                vsize = vsize.saturating_add(tx.vsize);
                fees = fees.saturating_add(tx.fee_sat);
                if entry_has_alkane_action(tx) || entry_has_rune_action(tx) {
                    trace_count = trace_count.saturating_add(1);
                }
                tx.readiness = derive_readiness(tx);
                tx_updates.insert(
                    *txid,
                    TemplateTxUpdate {
                        template_index: index,
                        position,
                        diesel_trace: tx.diesel_trace.clone(),
                        rune_io: tx.rune_io.clone(),
                    },
                );
            }
        }
        let (median_fee_rate, min_fee_rate, max_fee_rate, fee_range) =
            fee_stats_for_block(txids, &template_state, &package_rates);
        templates.push(MempoolBlockTemplate {
            index,
            tx_count: txids.len(),
            trace_count,
            weight,
            vsize,
            total_fees: fees,
            median_fee_rate,
            min_fee_rate,
            max_fee_rate,
            fee_range,
            transaction_ids: txids.iter().map(ToString::to_string).collect(),
        });
    }
    ensure_regtest_mempool_template(&mut templates, get_network());
    let Ok(mut state) = mempool_state().write() else { return };
    let previous_templates = state.templates.clone();
    let templates_changed = previous_templates != templates;
    let mut reset_txids: HashSet<Txid> = tx_updates.keys().copied().collect();
    for template in &previous_templates {
        for txid in &template.transaction_ids {
            if let Ok(txid) = Txid::from_str(txid) {
                reset_txids.insert(txid);
            }
        }
    }

    for txid in reset_txids {
        if let Some(tx) = state.txs.get_mut(&txid) {
            tx.template_index = None;
            tx.diesel_trace = None;
            tx.rune_io = None;
            tx.position = None;
            tx.readiness = derive_readiness(tx);
        }
    }
    let mut trace_requeue: Vec<Txid> = Vec::new();
    for txid in stale_trace_txids.iter().copied() {
        if state.txs.contains_key(&txid) {
            trace_requeue.push(txid);
        }
    }

    let mut projected_updated_txids: Vec<String> = Vec::new();
    for (txid, update) in tx_updates {
        if let Some(tx) = state.txs.get_mut(&txid) {
            let old_template_index = tx.template_index;
            let old_position = tx.position.as_ref().map(|pos| (pos.block, pos.vsize));
            let old_diesel_trace_len = tx.diesel_trace.as_ref().map(|traces| traces.len());
            let old_rune_io = tx.rune_io.clone();
            tx.template_index = Some(update.template_index);
            tx.diesel_trace = update.diesel_trace;
            tx.rune_io = update.rune_io;
            tx.position = Some(update.position);
            tx.readiness = derive_readiness(tx);
            let new_position = tx.position.as_ref().map(|pos| (pos.block, pos.vsize));
            let new_diesel_trace_len = tx.diesel_trace.as_ref().map(|traces| traces.len());
            if old_template_index != tx.template_index
                || old_position != new_position
                || old_diesel_trace_len != new_diesel_trace_len
                || old_rune_io != tx.rune_io
            {
                projected_updated_txids.push(txid.to_string());
            }
        }
    }

    let mut updated_txids = Vec::new();
    if templates_changed {
        state.sequence = state.sequence.saturating_add(1);
        state.deltas = calculate_template_deltas(&previous_templates, &templates, state.sequence);
        let mut seen = HashSet::new();
        for delta in &state.deltas {
            for txid in delta.added.iter().chain(delta.removed.iter()).chain(delta.changed.iter()) {
                if seen.insert(txid.clone()) {
                    updated_txids.push(txid.clone());
                }
            }
        }
    } else {
        state.deltas.clear();
    }
    state.templates = templates;
    state.updated_at = now_ts();
    let snapshot = compact_snapshot_from_state(&state, true);
    drop(state);
    if !updated_txids.is_empty() {
        publish_mempool_event(&json!({
            "type": "tx",
            "data": {
                "event": "mempool_updated",
                "status": "mempool",
                "txids": updated_txids,
            }
        }));
    }
    if !projected_updated_txids.is_empty() {
        publish_mempool_event(&json!({
            "type": "tx",
            "data": {
                "event": "mempool_projection_updated",
                "status": "mempool",
                "txids": projected_updated_txids,
            }
        }));
    }
    publish_mempool_event(&json!({ "type": "mempool-blocks", "data": snapshot }));
    for txid in trace_requeue {
        enqueue_trace(txid);
    }
}

async fn refresh_memory_mempool(rpc: &CoreClient, network: Network) -> Result<()> {
    mark_raw_refresh_start();
    if let Ok(info) = rpc.get_mempool_info() {
        let minimum_fee_rate = info.mempool_min_fee.to_sat() as f64 / 1_000.0;
        if minimum_fee_rate.is_finite() && minimum_fee_rate >= 0.0 {
            if let Ok(mut state) = mempool_state().write() {
                state.minimum_fee_rate = Some(minimum_fee_rate);
            }
        }
    }
    let verbose: HashMap<String, VerboseMempoolEntry> = match rpc
        .call("getrawmempool", &[json!(true)])
        .context("bitcoind getrawmempool verbose failed")
    {
        Ok(verbose) => verbose,
        Err(e) => {
            mark_raw_refresh_error(&e);
            return Err(e);
        }
    };
    let cfg = get_config().mempool.clone();
    let mut entries: Vec<(&String, &VerboseMempoolEntry)> = verbose.iter().collect();
    if cfg.max_txs > 0 && entries.len() > cfg.max_txs {
        entries.sort_by(|(_, a), (_, b)| {
            let a_fee = a.fees.as_ref().and_then(|f| f.base).map(btc_to_sat).unwrap_or(0) as f64;
            let b_fee = b.fees.as_ref().and_then(|f| f.base).map(btc_to_sat).unwrap_or(0) as f64;
            let a_rate = a
                .vsize
                .filter(|vsize| *vsize > 0)
                .map(|vsize| a_fee / vsize as f64)
                .unwrap_or(0.0);
            let b_rate = b
                .vsize
                .filter(|vsize| *vsize > 0)
                .map(|vsize| b_fee / vsize as f64)
                .unwrap_or(0.0);
            b_rate.partial_cmp(&a_rate).unwrap_or(std::cmp::Ordering::Equal)
        });
        entries.truncate(cfg.max_txs);
    }
    let mut canonical = HashSet::with_capacity(entries.len());
    for (txid_str, entry) in entries {
        let Ok(txid) = Txid::from_str(txid_str) else { continue };
        canonical.insert(txid);
        upsert_memory_entry(build_memory_metadata_entry(txid, entry));
    }
    let current_count =
        mempool_state().read().ok().map(|state| state.txs.len()).unwrap_or_default();
    let now = now_ts();
    let clear_until = mempool_state()
        .read()
        .ok()
        .and_then(|state| state.status.clear_protection_until);
    let clear_active = clear_until.map(|until| until > now).unwrap_or(false);
    let sharp_drop = current_count > 20_000
        && canonical.len().saturating_mul(100) <= current_count.saturating_mul(80);
    let skip_removal = sharp_drop || (clear_active && canonical.len() < current_count);
    let mut protected_refresh = false;
    if skip_removal {
        protected_refresh = true;
        let until = clear_until
            .filter(|until| *until > now)
            .unwrap_or_else(|| now.saturating_add(cfg.clear_protection_secs.max(1)));
        update_mempool_status(|status| {
            status.phase = MempoolSyncPhase::Stale;
            status.in_sync = false;
            status.stale = true;
            status.clear_protection_until = Some(until);
            status.last_error = Some(format!(
                "clear protection active: canonical getrawmempool returned {}/{} txs",
                canonical.len(),
                current_count
            ));
        });
        eprintln!(
            "[mempool] clear protection active: refusing to remove txs after canonical getrawmempool returned {}/{} txs",
            canonical.len(),
            current_count
        );
    } else {
        update_mempool_status(|status| {
            if status.clear_protection_until.map(|until| until <= now).unwrap_or(false) {
                status.clear_protection_until = None;
            }
        });
        let removed = remove_missing_memory_entries(&canonical);
        if removed > 0 {
            eprintln!("[mempool] removed {} txs absent from canonical getrawmempool", removed);
        }
    }
    recalculate_memory_templates();
    start_mempool_hydration(network);
    if !protected_refresh {
        mark_raw_refresh_success();
    }
    Ok(())
}

fn start_mempool_hydration(network: Network) {
    if HYDRATION_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }
    let txids = {
        let Ok(state) = mempool_state().read() else {
            HYDRATION_RUNNING.store(false, Ordering::SeqCst);
            return;
        };
        let mut ordered = Vec::new();
        let mut seen = HashSet::new();
        for template in &state.templates {
            for txid_str in &template.transaction_ids {
                let Ok(txid) = Txid::from_str(txid_str) else { continue };
                if seen.insert(txid)
                    && state.txs.get(&txid).map(|entry| entry.tx.is_none()).unwrap_or(false)
                {
                    ordered.push(txid);
                }
            }
        }
        let mut rest: Vec<Txid> = state
            .txs
            .iter()
            .filter_map(|(txid, entry)| (entry.tx.is_none() && seen.insert(*txid)).then_some(*txid))
            .collect();
        rest.sort_by(|a, b| {
            let aa = state.txs.get(a).map(|tx| tx.fee_rate).unwrap_or_default();
            let bb = state.txs.get(b).map(|tx| tx.fee_rate).unwrap_or_default();
            bb.partial_cmp(&aa).unwrap_or(std::cmp::Ordering::Equal)
        });
        ordered.extend(rest);
        ordered
    };
    let workers = get_config().mempool.hydration_workers.max(1);

    std::thread::spawn(move || {
        let total = txids.len();
        if total == 0 {
            HYDRATION_RUNNING.store(false, Ordering::SeqCst);
            set_hydration_status(false, 0);
            return;
        }
        let worker_count = workers.min(total);
        set_hydration_status(true, total);
        eprintln!("[mempool] hydrating {total} raw transactions with {worker_count} workers");

        let txids = Arc::new(txids);
        let next = Arc::new(AtomicUsize::new(0));
        let hydrated = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let txids = Arc::clone(&txids);
            let next = Arc::clone(&next);
            let hydrated = Arc::clone(&hydrated);
            handles.push(std::thread::spawn(move || {
                let rpc = get_bitcoind_rpc_client();
                loop {
                    let idx = next.fetch_add(1, Ordering::SeqCst);
                    let Some(txid) = txids.get(idx).copied() else { break };
                    let already_loaded = mempool_state()
                        .read()
                        .ok()
                        .and_then(|state| state.txs.get(&txid).map(|entry| entry.tx.is_some()))
                        .unwrap_or(false);
                    if already_loaded {
                        continue;
                    }
                    let raw_hex = match rpc.get_raw_transaction_hex(&txid, None) {
                        Ok(raw) => raw,
                        Err(e) => {
                            eprintln!("[mempool] hydrate getrawtransaction {} failed: {}", txid, e);
                            continue;
                        }
                    };
                    let raw_tx = match hex::decode(raw_hex.trim()) {
                        Ok(raw) => raw,
                        Err(e) => {
                            eprintln!("[mempool] hydrate decode raw tx {} failed: {}", txid, e);
                            continue;
                        }
                    };
                    let tx = match deserialize::<Transaction>(&raw_tx) {
                        Ok(tx) => tx,
                        Err(e) => {
                            eprintln!(
                                "[mempool] hydrate deserialize raw tx {} failed: {}",
                                txid, e
                            );
                            continue;
                        }
                    };
                    let mem_entry = build_memory_entry(txid, tx, None, network);
                    upsert_memory_entry(mem_entry);
                    let done = hydrated.fetch_add(1, Ordering::SeqCst) + 1;
                    if done % 1000 == 0 {
                        eprintln!("[mempool] hydrated {done}/{total} raw transactions");
                        recalculate_memory_templates();
                    }
                }
            }));
        }

        for handle in handles {
            if handle.join().is_err() {
                eprintln!("[mempool] hydration worker panicked");
            }
        }

        let hydrated = hydrated.load(Ordering::SeqCst);
        if hydrated > 0 {
            eprintln!("[mempool] hydrated {hydrated}/{total} raw transactions complete");
            recalculate_memory_templates();
        }
        HYDRATION_RUNNING.store(false, Ordering::SeqCst);
        set_hydration_status(false, total.saturating_sub(hydrated));
    });
}

async fn trace_worker(http: Client, view_url: String, populate_with_views: bool) {
    loop {
        if is_shutdown_requested() {
            break;
        }
        let next = trace_queue().lock().ok().and_then(|mut queue| queue.pop_front());
        let Some(txid) = next else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let entry = mempool_state().read().ok().and_then(|state| state.txs.get(&txid).cloned());
        let Some(entry) = entry else { continue };
        if entry.is_diesel_mint || entry.protostones.is_empty() {
            continue;
        }
        let Some(transaction) = entry.tx.as_ref() else {
            continue;
        };
        if entry.fixed_trace.is_some() {
            continue;
        }
        if is_shutdown_requested() {
            break;
        }
        let traces = if populate_with_views {
            view_traces_for_tx(&http, &view_url, &txid, transaction, entry.protostones.len()).await
        } else {
            fast_traces_for_tx(&txid, transaction, &entry.protostones)
        };
        if let Some(traces) = traces {
            let mut event_entry = None;
            if let Ok(mut state) = mempool_state().write() {
                if let Some(current) = state.txs.get_mut(&txid) {
                    current.fixed_trace = Some(traces);
                    current.fixed_trace_context = Some(vec![txid]);
                    current.readiness = derive_readiness(current);
                    event_entry = Some(current.clone());
                    state.updated_at = now_ts();
                }
            }
            recalculate_memory_templates();
            if let Some(entry) = event_entry.as_ref() {
                publish_mempool_entry_event(entry, "updated");
            }
        }
    }
}

fn ingest_zmq_rawtx(url: String, network: Network) {
    let rpc = get_bitcoind_rpc_client();
    std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let socket = match ctx.socket(zmq::SUB) {
            Ok(socket) => socket,
            Err(e) => {
                eprintln!("[mempool][zmq] socket init failed: {e}");
                return;
            }
        };
        if let Err(e) = socket.connect(&url) {
            eprintln!("[mempool][zmq] connect {url} failed: {e}");
            return;
        }
        if let Err(e) = socket.set_subscribe(b"rawtx") {
            eprintln!("[mempool][zmq] subscribe rawtx failed: {e}");
            return;
        }
        eprintln!("[mempool][zmq] subscribed to rawtx at {url}");
        loop {
            let topic = match socket.recv_bytes(0) {
                Ok(topic) => topic,
                Err(e) => {
                    eprintln!("[mempool][zmq] recv topic failed: {e}");
                    continue;
                }
            };
            let body = match socket.recv_bytes(0) {
                Ok(body) => body,
                Err(e) => {
                    eprintln!("[mempool][zmq] recv body failed: {e}");
                    continue;
                }
            };
            let _seq = socket.recv_bytes(0);
            if topic.as_slice() != b"rawtx" {
                continue;
            }
            let Ok(tx) = deserialize::<Transaction>(&body) else { continue };
            let txid = tx.compute_txid();
            let verbose: Option<VerboseMempoolEntry> =
                rpc.call("getmempoolentry", &[json!(txid.to_string())]).ok();
            let entry = build_memory_entry(txid, tx, verbose.as_ref(), network);
            upsert_memory_entry(entry);
            recalculate_memory_templates();
        }
    });
}

fn ingest_zmq_sequence(url: String) {
    std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let socket = match ctx.socket(zmq::SUB) {
            Ok(socket) => socket,
            Err(e) => {
                eprintln!("[mempool][zmq] sequence socket init failed: {e}");
                return;
            }
        };
        if let Err(e) = socket.connect(&url) {
            eprintln!("[mempool][zmq] sequence connect {url} failed: {e}");
            return;
        }
        if let Err(e) = socket.set_subscribe(b"sequence") {
            eprintln!("[mempool][zmq] subscribe sequence failed: {e}");
            return;
        }
        eprintln!("[mempool][zmq] subscribed to sequence at {url}");
        loop {
            let topic = match socket.recv_bytes(0) {
                Ok(topic) => topic,
                Err(e) => {
                    eprintln!("[mempool][zmq] sequence recv topic failed: {e}");
                    continue;
                }
            };
            let body = match socket.recv_bytes(0) {
                Ok(body) => body,
                Err(e) => {
                    eprintln!("[mempool][zmq] sequence recv body failed: {e}");
                    continue;
                }
            };
            let _seq = socket.recv_bytes(0);
            if topic.as_slice() != b"sequence" || body.len() < 33 {
                continue;
            }
            let label = body[32] as char;
            if label != 'R' {
                continue;
            }
            let mut removed_any = false;
            if let Ok(txid) = Txid::from_slice(&body[..32]) {
                if remove_memory_txid(&txid) {
                    eprintln!("[mempool][zmq] sequence removed {txid}");
                    removed_any = true;
                }
            }
            let mut reversed = body[..32].to_vec();
            reversed.reverse();
            if let Ok(txid) = Txid::from_slice(&reversed) {
                if remove_memory_txid(&txid) {
                    eprintln!("[mempool][zmq] sequence removed {txid}");
                    removed_any = true;
                }
            }
            if removed_any {
                recalculate_memory_templates();
            }
        }
    });
}

pub async fn run_mempool_service(network: Network) -> Result<()> {
    let rpc = get_bitcoind_rpc_client();
    let view_url = get_metashrew_rpc_url().to_string();
    let http = Client::new();
    let cfg = get_config().mempool.clone();

    if !cfg.enabled {
        eprintln!("[mempool] disabled by config");
        return Ok(());
    }

    eprintln!(
        "[mempool] service starting (raw_poll={}s, template_poll={}s, populate_with_views={}, view_url={})",
        cfg.raw_poll_secs, cfg.template_poll_secs, cfg.populate_with_views, view_url
    );

    eprintln!("[mempool] startup getrawmempool refresh");
    if let Err(e) = refresh_memory_mempool(&rpc, network).await {
        eprintln!("[mempool] startup refresh failed: {e:?}");
    }

    if let Some(zmq_url) = cfg.zmq_rawtx_url.as_ref().and_then(|s| normalize_zmq_url(s)) {
        ingest_zmq_rawtx(zmq_url, network);
    }
    if let Some(zmq_url) = cfg.zmq_sequence_url.as_ref().and_then(|s| normalize_zmq_url(s)) {
        ingest_zmq_sequence(zmq_url);
    }

    for _ in 0..cfg.trace_workers.max(1) {
        tokio::spawn(trace_worker(http.clone(), view_url.clone(), cfg.populate_with_views));
    }

    let template_poll = Duration::from_secs(cfg.template_poll_secs.max(1));
    let raw_poll = Duration::from_secs(cfg.raw_poll_secs.max(1));
    let mut last_raw_refresh = SystemTime::now();

    loop {
        if is_shutdown_requested() {
            return Ok(());
        }
        let should_refresh = last_raw_refresh.elapsed().unwrap_or_default() >= raw_poll;
        if should_refresh {
            eprintln!("[mempool] canonical getrawmempool refresh");
            if let Err(e) = refresh_memory_mempool(&rpc, network).await {
                eprintln!("[mempool] canonical refresh failed: {e:?}");
            }
            last_raw_refresh = SystemTime::now();
        } else {
            recalculate_memory_templates();
        }

        tokio::time::sleep(template_poll).await;
    }
}

fn normalize_zmq_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

pub fn get_tx_from_mempool(txid: &Txid) -> Option<MempoolEntry> {
    let state = mempool_state().read().ok()?;
    let entry = state.txs.get(txid)?;
    mempool_entry_from_state(entry)
}

pub fn pending_by_txid(txid: &Txid) -> Option<MempoolEntry> {
    get_tx_from_mempool(txid)
}

pub fn pending_action_entries() -> Vec<MempoolEntry> {
    let Ok(state) = mempool_state().read() else {
        return Vec::new();
    };
    let mut out: Vec<MempoolEntry> = state
        .txs
        .values()
        .filter(|entry| entry_has_alkane_action(entry) || entry_has_rune_action(entry))
        .filter_map(mempool_entry_from_state)
        .collect();
    out.sort_by(|a, b| b.first_seen.cmp(&a.first_seen).then_with(|| b.txid.cmp(&a.txid)));
    out
}

pub fn pending_action_entries_for_address(
    addr: &str,
    network: Network,
    include_alkanes: bool,
    include_runes: bool,
    live_alkane_outpoints: &HashSet<(Txid, u32)>,
    live_rune_outpoints: &HashSet<(Txid, u32)>,
) -> Vec<MempoolEntry> {
    if !include_alkanes && !include_runes {
        return Vec::new();
    }

    let Ok(state) = mempool_state().read() else {
        return Vec::new();
    };
    let target_spk = Address::from_str(addr)
        .ok()
        .and_then(|address| address.require_network(network).ok())
        .map(|address| address.script_pubkey());
    let mut out: Vec<MempoolEntry> = state
        .txs
        .values()
        .filter(|entry| {
            (include_alkanes && entry_has_alkane_action(entry))
                || (include_runes && entry_has_rune_action(entry))
        })
        .filter(|entry| {
            entry.addresses.iter().any(|address| address == addr)
                || entry_spends_live_outpoint(
                    entry,
                    include_alkanes,
                    include_runes,
                    live_alkane_outpoints,
                    live_rune_outpoints,
                )
                || target_spk
                    .as_ref()
                    .is_some_and(|spk| entry_spends_mempool_output_to_spk(entry, spk, &state))
        })
        .filter_map(mempool_entry_from_state)
        .collect();
    out.sort_by(|a, b| b.first_seen.cmp(&a.first_seen).then_with(|| b.txid.cmp(&a.txid)));
    out
}

/// Transactions currently held in the in-memory mempool. Callers use this to
/// skip pending work entirely when no mempool service is running (client mode
/// keeps an empty store and answers from the data instance).
pub fn mempool_tx_count() -> usize {
    mempool_state().read().map(|state| state.txs.len()).unwrap_or(0)
}

/// One page of an address's pending transactions, newest first, with the total
/// number of matches.
///
/// Matching a mempool transaction to an address is cheap in two of the three
/// cases: the entry carries its own output addresses, and a chained spend of
/// another mempool transaction's output can be resolved from the store. The
/// third — a transaction spending a *confirmed* output of the address — is
/// only visible through that output's owner, and used to be resolved by
/// enumerating every outpoint the address had ever held (112k blob reads for a
/// busy address, which is what made those pages take minutes). That case now
/// arrives as `candidate_txids`: the caller passes the mempool txids the
/// address index already attributes to this address, so the cost is bounded by
/// the address's mempool activity instead of its entire history.
///
/// Only entries inside the requested window are materialised — a match outside
/// it costs one txid.
pub fn pending_page_for_address(
    addr: &str,
    network: Network,
    include_alkanes: bool,
    include_runes: bool,
    actions_only: bool,
    candidate_txids: &HashSet<Txid>,
    offset: usize,
    limit: usize,
) -> (Vec<MempoolEntry>, usize) {
    let Ok(state) = mempool_state().read() else {
        return (Vec::new(), 0);
    };
    pending_page_from_state(
        &state,
        addr,
        network,
        include_alkanes,
        include_runes,
        actions_only,
        candidate_txids,
        offset,
        limit,
    )
}

#[allow(clippy::too_many_arguments)]
fn pending_page_from_state(
    state: &InMemoryMempool,
    addr: &str,
    network: Network,
    include_alkanes: bool,
    include_runes: bool,
    actions_only: bool,
    candidate_txids: &HashSet<Txid>,
    offset: usize,
    limit: usize,
) -> (Vec<MempoolEntry>, usize) {
    if actions_only && !include_alkanes && !include_runes {
        return (Vec::new(), 0);
    }

    // Derived once for the whole scan rather than per candidate output.
    let target_spk = Address::from_str(addr)
        .ok()
        .and_then(|address| address.require_network(network).ok())
        .map(|address| address.script_pubkey());

    let mut matches: Vec<(u64, Txid)> = Vec::new();
    for (txid, entry) in state.txs.iter() {
        // Entries without a transaction body cannot be rendered, so they are
        // left out of the total as well — it counts what this page can show.
        if entry.tx.is_none() {
            continue;
        }
        if actions_only
            && !((include_alkanes && entry_has_alkane_action(entry))
                || (include_runes && entry_has_rune_action(entry)))
        {
            continue;
        }
        let matched = entry.addresses.iter().any(|address| address == addr)
            || candidate_txids.contains(txid)
            || target_spk
                .as_ref()
                .is_some_and(|spk| entry_spends_mempool_output_to_spk(entry, spk, state));
        if matched {
            matches.push((entry.first_seen, *txid));
        }
    }

    let total = matches.len();
    matches.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    let entries = matches
        .into_iter()
        .skip(offset)
        .take(limit)
        .filter_map(|(_, txid)| state.txs.get(&txid).and_then(mempool_entry_from_state))
        .collect();
    (entries, total)
}

pub fn pending_for_address(addr: &str) -> Vec<MempoolEntry> {
    let Ok(state) = mempool_state().read() else { return Vec::new() };
    let mut out: Vec<MempoolEntry> = state
        .txs
        .values()
        .filter(|entry| entry.addresses.iter().any(|a| a == addr))
        .filter_map(mempool_entry_from_state)
        .collect();
    out.sort_by(|a, b| b.first_seen.cmp(&a.first_seen).then_with(|| b.txid.cmp(&a.txid)));
    out
}

pub fn purge_confirmed_txids(txids: &[Txid]) -> Result<usize> {
    let Ok(mut state) = mempool_state().write() else { return Ok(0) };
    let mut removed = 0usize;
    for txid in txids {
        if state.txs.remove(txid).is_some() {
            removed += 1;
        }
    }
    if removed > 0 {
        state.updated_at = now_ts();
    }
    drop(state);
    if removed > 0 {
        recalculate_memory_templates();
    }
    Ok(removed)
}

pub fn purge_confirmed_from_chain() -> Result<usize> {
    let rpc = get_bitcoind_rpc_client();
    let txids: Vec<Txid> = mempool_state()
        .read()
        .ok()
        .map(|state| state.txs.keys().copied().collect())
        .unwrap_or_default();
    let mut confirmed = Vec::new();
    for txid in txids {
        if let Ok(info) = rpc.get_raw_transaction_info(&txid, None) {
            if info.blockhash.is_some() {
                confirmed.push(txid);
            }
        }
    }

    let removed = purge_confirmed_txids(&confirmed)?;
    if removed > 0 {
        eprintln!("[mempool] purged {} confirmed txs from store", removed);
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkanes_support::cellpack::Cellpack;
    use alkanes_support::id::AlkaneId;
    use bitcoin::{
        Amount, ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute::LockTime, transaction::Version,
    };

    fn sample_tx() -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut { value: Amount::ZERO, script_pubkey: ScriptBuf::new() }],
        }
    }

    fn cellpack_protostone(target: AlkaneId, inputs: Vec<u128>) -> Protostone {
        let cellpack = Cellpack { target, inputs };
        Protostone {
            burn: None,
            message: cellpack.encipher(),
            edicts: Vec::new(),
            refund: None,
            pointer: None,
            from: None,
            protocol_tag: 1,
        }
    }

    fn mempool_transaction(seed: u8, vsize: u64, fee_sat: u64) -> (Txid, MempoolTransactionStruct) {
        let txid = Txid::from_byte_array([seed; 32]);
        (
            txid,
            MempoolTransactionStruct {
                txid,
                tx: None,
                protostones: Vec::new(),
                fixed_trace: None,
                fixed_trace_context: None,
                diesel_trace: None,
                rune_io: None,
                first_seen: 0,
                fee_sat,
                weight: vsize.saturating_mul(4),
                vsize,
                fee_rate: fee_sat as f64 / vsize.max(1) as f64,
                inputs: Vec::new(),
                spent_outpoints: Vec::new(),
                addresses: Vec::new(),
                is_diesel_mint: false,
                is_ug_mint: false,
                template_index: None,
                position: None,
                readiness: MempoolTxReadiness::MetadataOnly,
            },
        )
    }

    #[test]
    fn calculate_block_templates_returns_no_blocks_for_empty_mempool() {
        let txs = HashMap::new();

        let (blocks, rates) = calculate_block_templates(&txs, 8, 1_000_000);

        assert!(blocks.is_empty());
        assert!(rates.is_empty());
    }

    #[test]
    fn regtest_keeps_one_empty_mempool_template() {
        let mut templates = Vec::new();

        ensure_regtest_mempool_template(&mut templates, Network::Regtest);

        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].index, 0);
        assert_eq!(templates[0].tx_count, 0);
        assert!(templates[0].transaction_ids.is_empty());
    }

    #[test]
    fn mainnet_does_not_add_empty_mempool_template() {
        let mut templates = Vec::new();

        ensure_regtest_mempool_template(&mut templates, Network::Bitcoin);

        assert!(templates.is_empty());
    }

    #[test]
    fn calculate_block_templates_returns_only_populated_blocks_needed() {
        let txs: HashMap<_, _> = [1, 2, 3]
            .into_iter()
            .map(|seed| mempool_transaction(seed, 100_000, 100_000))
            .collect();

        let (blocks, _) = calculate_block_templates(&txs, 8, 1_000_000);

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.iter().map(Vec::len).sum::<usize>(), 3);
        assert!(blocks.iter().all(|block| !block.is_empty()));
    }

    #[test]
    fn mempool_projection_prefix_keeps_prior_blocks_and_stops_at_last_target() {
        let txids: Vec<Txid> = (1..=5).map(|seed| Txid::from_byte_array([seed; 32])).collect();
        let templates = vec![
            MempoolBlockTemplate {
                index: 0,
                transaction_ids: txids[..2].iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
            MempoolBlockTemplate {
                index: 1,
                transaction_ids: txids[2..].iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
        ];
        let targets = HashSet::from([txids[3]]);

        let prefix = mempool_projection_prefix(&templates, 1, &targets).expect("prefix");

        assert_eq!(prefix, txids[..4]);
    }

    #[test]
    fn fast_traces_for_tx_builds_summary_from_protostone_cellpack() {
        let tx = sample_tx();
        let txid = tx.compute_txid();
        let protostone = cellpack_protostone(AlkaneId { block: 4, tx: 797 }, vec![42, 99, 1000]);

        let traces = fast_traces_for_tx(&txid, &tx, &[protostone]).expect("fast traces");

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].sandshrew_trace.outpoint, format!("{}:2", txid));
        assert!(traces[0].protobuf_trace.events.is_empty());
        assert_eq!(traces[0].outpoint.vout, 2);
        assert_eq!(traces[0].outpoint.txid, txid.to_byte_array().to_vec());

        let EspoSandshrewLikeTraceEvent::Invoke(invoke) = &traces[0].sandshrew_trace.events[0]
        else {
            panic!("expected invoke event");
        };
        assert_eq!(invoke.typ, "call");
        assert_eq!(invoke.context.myself.block, "0x4");
        assert_eq!(invoke.context.myself.tx, "0x31d");
        assert_eq!(invoke.context.caller.block, "0x0");
        assert_eq!(invoke.context.caller.tx, "0x0");
        assert_eq!(invoke.context.inputs, vec!["0x2a", "0x63", "0x3e8"]);
        assert_eq!(invoke.context.vout, 2);
    }

    #[test]
    fn fast_traces_for_tx_marks_non_cellpack_protostones_processed() {
        let tx = sample_tx();
        let txid = tx.compute_txid();
        let protostone = Protostone {
            burn: None,
            message: Vec::new(),
            edicts: Vec::new(),
            refund: None,
            pointer: None,
            from: None,
            protocol_tag: 1,
        };

        let traces = fast_traces_for_tx(&txid, &tx, &[protostone]).expect("fast traces");

        assert!(traces.is_empty());
    }

    /// The address page asks for one window of pending transactions. Matches
    /// come from three places — the entry's own output addresses, txids the
    /// address index attributes to the address, and chained spends of another
    /// mempool transaction's output — and only the window is materialised.
    #[test]
    fn pending_page_matches_all_three_ways_and_pages() {
        use bitcoin::WPubkeyHash;
        use bitcoin::hashes::Hash as _;

        let spk = ScriptBuf::new_p2wpkh(&WPubkeyHash::from_byte_array([7u8; 20]));
        let addr = Address::from_script(spk.as_script(), Network::Bitcoin)
            .expect("address from script")
            .to_string();

        let paying_tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut { value: Amount::ZERO, script_pubkey: spk.clone() }],
        };

        let mut state = InMemoryMempool::default();

        // Pays the address directly.
        let (paying_txid, mut paying) = mempool_transaction(1, 100, 1000);
        paying.tx = Some(paying_tx.clone());
        paying.addresses = vec![addr.clone()];
        paying.first_seen = 30;
        state.txs.insert(paying_txid, paying);

        // Spends a confirmed output of the address: nothing in the entry says
        // so, it is only known because the address index named it.
        let (spending_txid, mut spending) = mempool_transaction(2, 100, 1000);
        spending.tx = Some(sample_tx());
        spending.first_seen = 20;
        state.txs.insert(spending_txid, spending);

        // Spends the mempool output that pays the address.
        let (chained_txid, mut chained) = mempool_transaction(3, 100, 1000);
        let mut chained_tx = sample_tx();
        chained_tx.input[0].previous_output = OutPoint { txid: paying_txid, vout: 0 };
        chained.spent_outpoints = vec![OutPoint { txid: paying_txid, vout: 0 }];
        chained.tx = Some(chained_tx);
        chained.first_seen = 40;
        state.txs.insert(chained_txid, chained);

        // Unrelated to the address.
        let (other_txid, mut other) = mempool_transaction(4, 100, 1000);
        other.tx = Some(sample_tx());
        other.first_seen = 50;
        state.txs.insert(other_txid, other);

        // Matches the address but has no transaction body, so it cannot be
        // rendered and must not inflate the total.
        let (bodyless_txid, mut bodyless) = mempool_transaction(5, 100, 1000);
        bodyless.addresses = vec![addr.clone()];
        bodyless.first_seen = 60;
        state.txs.insert(bodyless_txid, bodyless);

        let candidates: HashSet<Txid> = [spending_txid].into_iter().collect();

        let (page, total) = pending_page_from_state(
            &state,
            &addr,
            Network::Bitcoin,
            true,
            true,
            false,
            &candidates,
            0,
            10,
        );
        assert_eq!(total, 3, "paying + spending + chained, not the unrelated or bodyless one");
        let ids: Vec<Txid> = page.iter().map(|e| e.txid).collect();
        assert_eq!(ids, vec![chained_txid, paying_txid, spending_txid], "newest first");

        // Windows: only the requested slice comes back, total is unchanged.
        let (first, total_first) = pending_page_from_state(
            &state,
            &addr,
            Network::Bitcoin,
            true,
            true,
            false,
            &candidates,
            0,
            2,
        );
        assert_eq!(total_first, 3);
        assert_eq!(
            first.iter().map(|e| e.txid).collect::<Vec<_>>(),
            vec![chained_txid, paying_txid]
        );

        let (second, total_second) = pending_page_from_state(
            &state,
            &addr,
            Network::Bitcoin,
            true,
            true,
            false,
            &candidates,
            2,
            2,
        );
        assert_eq!(total_second, 3);
        assert_eq!(second.iter().map(|e| e.txid).collect::<Vec<_>>(), vec![spending_txid]);

        let (past_end, _) = pending_page_from_state(
            &state,
            &addr,
            Network::Bitcoin,
            true,
            true,
            false,
            &candidates,
            10,
            2,
        );
        assert!(past_end.is_empty());

        // None of these carry an alkane or rune action, so the action-filtered
        // tabs show nothing.
        let (actions, actions_total) = pending_page_from_state(
            &state,
            &addr,
            Network::Bitcoin,
            true,
            true,
            true,
            &candidates,
            0,
            10,
        );
        assert!(actions.is_empty());
        assert_eq!(actions_total, 0);
    }
}
