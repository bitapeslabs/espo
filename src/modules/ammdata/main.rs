use super::schemas::{ActivityKind, SchemaCandleV1, SchemaMarketDefs, Timeframe};
use super::storage::{
    AmmDataProvider, GetListEntriesDescParams, GetPoolDefsParams, GetRawValueParams,
    GetTokenPoolsParams, SetBatchParams,
};
use super::utils::activity::decode_activity_v1;
use crate::alkanes::trace::EspoBlock;
use crate::alkanes::trace::EspoSandshrewLikeTraceInvokeData;
use crate::config::{debug_enabled, get_espo_module_mdb, get_network};
use crate::debug;
use crate::modules::ammdata::config::{AmmDataConfig, DerivedMergeStrategy, DerivedQuoteConfig};
use crate::modules::ammdata::consts::{
    AMOUNT_SCALE, CanonicalQuoteUnit, PRICE_SCALE, ammdata_genesis_block, canonical_quotes,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, EssentialsProvider,
    GetListEntriesDescParams as EssentialsGetListEntriesDescParams, GetMultiValuesParams,
    decode_pointer_idx_u64, load_tx_pointer_blob_v3_by_id,
};
use crate::modules::essentials::utils::balances::SignedU128;
use crate::modules::essentials::utils::inspections::{
    StoredInspectionMetadata, StoredInspectionResult,
};
use crate::runtime::mdb::Mdb;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use bitcoin::{ScriptBuf, Transaction};
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::rpc::register_rpc;

/* ---------- module ---------- */

const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
const AMM_FACTORY_OPCODES: [u128; 14] = [0, 1, 2, 3, 4, 7, 10, 11, 12, 13, 14, 21, 29, 50];
const KV_KEY_BEACON: &[u8] = b"/beacon";

fn is_amm_factory_metadata(meta: &StoredInspectionMetadata) -> bool {
    let has_opcodes = AMM_FACTORY_OPCODES
        .iter()
        .all(|opcode| meta.methods.iter().any(|m| m.opcode == *opcode));
    if has_opcodes {
        return true;
    }
    meta.name.eq_ignore_ascii_case("ammfactory")
}

pub(crate) fn inspection_is_amm_factory(inspection: &StoredInspectionResult) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    is_amm_factory_metadata(meta)
}

fn decode_kv_implementation(raw: &[u8]) -> Option<SchemaAlkaneId> {
    if raw.len() < 32 {
        return None;
    }
    let block_bytes: [u8; 16] = raw[0..16].try_into().ok()?;
    let tx_bytes: [u8; 16] = raw[16..32].try_into().ok()?;
    let block = u128::from_le_bytes(block_bytes);
    let tx = u128::from_le_bytes(tx_bytes);
    if block > u32::MAX as u128 || tx > u64::MAX as u128 {
        return None;
    }
    Some(SchemaAlkaneId { block: block as u32, tx: tx as u64 })
}

pub(crate) fn lookup_proxy_target(
    blockhash: StateAt,
    essentials: &EssentialsProvider,
    alkane: SchemaAlkaneId,
) -> Option<SchemaAlkaneId> {
    let table = essentials.table();
    let keys = vec![
        table.kv_row_key(&alkane, KV_KEY_IMPLEMENTATION),
        table.kv_row_key(&alkane, KV_KEY_BEACON),
    ];
    let values = essentials
        .get_multi_values(GetMultiValuesParams { blockhash, keys })
        .ok()?
        .values;
    for value in values.into_iter().flatten() {
        // Storage rows are `[last_txid(32 bytes)] + raw_value`.
        let raw = if value.len() >= 32 { &value[32..] } else { value.as_slice() };
        if let Some(target) = decode_kv_implementation(raw) {
            return Some(target);
        }
    }
    None
}

pub(crate) fn parse_hex_u32(s: &str) -> Option<u32> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(trimmed, 16)
        .ok()
        .and_then(|v| if v > u32::MAX as u128 { None } else { Some(v as u32) })
}

pub(crate) fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(trimmed, 16)
        .ok()
        .and_then(|v| if v > u64::MAX as u128 { None } else { Some(v as u64) })
}

fn parse_hex_u128(s: &str) -> Option<u128> {
    u128::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

pub(crate) fn merge_candles(
    base: SchemaCandleV1,
    other: SchemaCandleV1,
    strategy: &DerivedMergeStrategy,
) -> SchemaCandleV1 {
    let merge = |a: u128, b: u128| -> u128 {
        match strategy {
            DerivedMergeStrategy::Optimistic => a.max(b),
            DerivedMergeStrategy::Pessimistic => a.min(b),
            DerivedMergeStrategy::Neutral => (a.saturating_add(b)) / 2,
            DerivedMergeStrategy::NeutralVwap => {
                let va = base.volume;
                let vb = other.volume;
                let denom = va.saturating_add(vb);
                if denom == 0 {
                    (a.saturating_add(b)) / 2
                } else {
                    let num = a.saturating_mul(va).saturating_add(b.saturating_mul(vb));
                    num / denom
                }
            }
        }
    };
    let volume = if base.volume == 0 { other.volume } else { base.volume };
    SchemaCandleV1 {
        open: merge(base.open, other.open),
        high: merge(base.high, other.high),
        low: merge(base.low, other.low),
        close: merge(base.close, other.close),
        volume,
    }
}

pub(crate) fn invert_price_value(p: u128) -> Option<u128> {
    if p == 0 { None } else { Some(PRICE_SCALE.saturating_mul(PRICE_SCALE) / p) }
}

pub(crate) fn parse_factory_create_call(
    inv: &EspoSandshrewLikeTraceInvokeData,
    factories: &HashSet<SchemaAlkaneId>,
) -> Option<SchemaAlkaneId> {
    let inputs = &inv.context.inputs;
    let opcode0 = inputs.get(0).and_then(|s| parse_hex_u128(s));
    let opcode2 = inputs.get(2).and_then(|s| parse_hex_u128(s));
    if opcode0 != Some(1) && opcode2 != Some(1) {
        return None;
    }

    // Legacy layout: [factory_block, factory_tx, opcode, ...]
    if opcode2 == Some(1) {
        if let (Some(block), Some(tx)) = (parse_hex_u32(&inputs[0]), parse_hex_u64(&inputs[1])) {
            let id = SchemaAlkaneId { block, tx };
            if factories.contains(&id) {
                return Some(id);
            }
        }
    }

    // Current layout: [opcode, ...], invoked contract is the factory.
    if let (Some(block), Some(tx)) =
        (parse_hex_u32(&inv.context.myself.block), parse_hex_u64(&inv.context.myself.tx))
    {
        let id = SchemaAlkaneId { block, tx };
        if factories.contains(&id) {
            return Some(id);
        }
    }

    None
}

pub(crate) fn pool_creator_spk_from_protostone(tx: &Transaction) -> Option<ScriptBuf> {
    let runestone = match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => r,
        _ => return None,
    };
    let protos = Protostone::from_runestone(&runestone).ok()?;
    for ps in protos {
        if ps.protocol_tag != 1 {
            continue;
        }
        if let Some(ptr) = ps.pointer {
            let idx = ptr as usize;
            if let Some(out) = tx.output.get(idx) {
                return Some(out.script_pubkey.clone());
            }
        }
    }
    None
}

pub(crate) fn signed_from_delta(delta: Option<&SignedU128>) -> i128 {
    let Some(d) = delta else { return 0 };
    let (neg, amt) = d.as_parts();
    if neg { -(amt as i128) } else { amt as i128 }
}

pub(crate) fn apply_delta_u128(current: u128, delta: i128) -> u128 {
    if delta >= 0 {
        current.saturating_add(delta as u128)
    } else {
        current.saturating_sub((-delta) as u128)
    }
}

#[inline]
pub(crate) fn scale_price_u128(value: u128) -> f64 {
    (value as f64) / (PRICE_SCALE as f64)
}

#[inline]
pub(crate) fn percent_change_basis_points(prev: u128, now: u128) -> i64 {
    if prev == 0 {
        return 0;
    }
    let (neg, delta) = if now >= prev { (false, now - prev) } else { (true, prev - now) };
    let scaled = delta.saturating_mul(1_000_000).saturating_div(prev);
    let mag = if scaled > i64::MAX as u128 { i64::MAX } else { scaled as i64 };
    if neg { -mag } else { mag }
}

#[inline]
pub(crate) fn percent_change_str(prev: u128, now: u128) -> String {
    crate::modules::ammdata::storage::format_change_basis_points(percent_change_basis_points(
        prev, now,
    ))
}

#[inline]
pub(crate) fn apr_basis_points_30d(pool_volume_30d_usd: u128, pool_tvl_usd: u128) -> i64 {
    if pool_tvl_usd == 0 {
        return 0;
    }
    let scaled = pool_volume_30d_usd.saturating_mul(36_000).saturating_div(pool_tvl_usd);
    if scaled > i64::MAX as u128 { i64::MAX } else { scaled as i64 }
}

#[inline]
pub(crate) fn apr_30d_str(pool_volume_30d_usd: u128, pool_tvl_usd: u128) -> String {
    crate::modules::ammdata::storage::format_change_basis_points(apr_basis_points_30d(
        pool_volume_30d_usd,
        pool_tvl_usd,
    ))
}

#[inline]
pub(crate) fn parse_change_f64(raw: &str) -> f64 {
    crate::modules::ammdata::storage::change_basis_points_to_f64(
        crate::modules::ammdata::storage::parse_change_basis_points(raw),
    )
}

#[inline]
pub(crate) fn pool_name_display(raw: &str) -> String {
    raw.trim().to_string()
}

pub(crate) fn strip_lp_suffix(raw: &str) -> String {
    let trimmed = raw.trim_end();
    let upper = trimmed.to_ascii_uppercase();
    if upper.ends_with(" LP") && trimmed.len() >= 3 {
        return trimmed[..trimmed.len() - 3].trim_end().to_string();
    }
    if upper.ends_with("LP") && trimmed.len() >= 2 {
        return trimmed[..trimmed.len() - 2].trim_end().to_string();
    }
    trimmed.to_string()
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PoolTradeWindows {
    pub(crate) token0_1d: u128,
    pub(crate) token1_1d: u128,
    pub(crate) token0_7d: u128,
    pub(crate) token1_7d: u128,
    pub(crate) token0_30d: u128,
    pub(crate) token1_30d: u128,
    pub(crate) token0_all: u128,
    pub(crate) token1_all: u128,
    pub(crate) has_all_time: bool,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct TokenTradeWindows {
    pub(crate) amount_1d: u128,
    pub(crate) amount_7d: u128,
    pub(crate) amount_30d: u128,
    pub(crate) amount_all: u128,
    pub(crate) block_amount: u128,
    pub(crate) has_all_time: bool,
}

pub(crate) fn abs_i128(value: i128) -> u128 {
    if value < 0 { (-value) as u128 } else { value as u128 }
}

fn decode_ts_seq_from_index(prefix_len: usize, key: &[u8], val: &[u8]) -> Option<(u64, u32)> {
    if val.len() == 12 {
        let mut ts = [0u8; 8];
        let mut seq = [0u8; 4];
        ts.copy_from_slice(&val[0..8]);
        seq.copy_from_slice(&val[8..12]);
        return Some((u64::from_be_bytes(ts), u32::from_be_bytes(seq)));
    }
    if key.len() >= prefix_len + 12 {
        let mut ts = [0u8; 8];
        let mut seq = [0u8; 4];
        ts.copy_from_slice(&key[prefix_len..prefix_len + 8]);
        seq.copy_from_slice(&key[prefix_len + 8..prefix_len + 12]);
        return Some((u64::from_be_bytes(ts), u32::from_be_bytes(seq)));
    }
    None
}

fn activity_key_for(pool: &SchemaAlkaneId, ts: u64, seq: u32) -> Vec<u8> {
    let mut k = format!("activity:v1:{}:{}:", pool.block, pool.tx).into_bytes();
    k.extend_from_slice(ts.to_string().as_bytes());
    k.push(b':');
    k.extend_from_slice(seq.to_string().as_bytes());
    k
}

fn trade_index_prefix(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("activity:idx:v1:{}:{}:trades:ts:", pool.block, pool.tx).into_bytes()
}

pub(crate) fn pool_trade_windows(
    provider: &AmmDataProvider,
    pool: &SchemaAlkaneId,
    now_ts: u64,
    in_block: &HashMap<SchemaAlkaneId, (u128, u128)>,
    cache: &mut HashMap<SchemaAlkaneId, PoolTradeWindows>,
    full_history: bool,
) -> Result<PoolTradeWindows> {
    if let Some(cached) = cache.get(pool) {
        if !full_history || cached.has_all_time {
            return Ok(*cached);
        }
    }

    let start_1d = now_ts.saturating_sub(24 * 60 * 60);
    let start_7d = now_ts.saturating_sub(7 * 24 * 60 * 60);
    let start_30d = now_ts.saturating_sub(30 * 24 * 60 * 60);

    let prefix = trade_index_prefix(pool);
    let prefix_len = prefix.len();
    let mut out = PoolTradeWindows::default();

    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        })?
        .entries
    {
        let Some((ts, seq)) = decode_ts_seq_from_index(prefix_len, &k, &v) else {
            continue;
        };
        if !full_history && ts < start_30d {
            break;
        }

        let key = activity_key_for(pool, ts, seq);
        let Some(raw) = provider
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
        else {
            continue;
        };
        let activity = match decode_activity_v1(&raw) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if !matches!(activity.kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
            continue;
        }

        let base_abs = abs_i128(activity.base_delta);
        let quote_abs = abs_i128(activity.quote_delta);

        if ts >= start_30d {
            out.token0_30d = out.token0_30d.saturating_add(base_abs);
            out.token1_30d = out.token1_30d.saturating_add(quote_abs);
        }
        if ts >= start_7d {
            out.token0_7d = out.token0_7d.saturating_add(base_abs);
            out.token1_7d = out.token1_7d.saturating_add(quote_abs);
        }
        if ts >= start_1d {
            out.token0_1d = out.token0_1d.saturating_add(base_abs);
            out.token1_1d = out.token1_1d.saturating_add(quote_abs);
        }
        if full_history {
            out.token0_all = out.token0_all.saturating_add(base_abs);
            out.token1_all = out.token1_all.saturating_add(quote_abs);
        }
    }

    if let Some((base_abs, quote_abs)) = in_block.get(pool) {
        if now_ts >= start_30d {
            out.token0_30d = out.token0_30d.saturating_add(*base_abs);
            out.token1_30d = out.token1_30d.saturating_add(*quote_abs);
        }
        if now_ts >= start_7d {
            out.token0_7d = out.token0_7d.saturating_add(*base_abs);
            out.token1_7d = out.token1_7d.saturating_add(*quote_abs);
        }
        if now_ts >= start_1d {
            out.token0_1d = out.token0_1d.saturating_add(*base_abs);
            out.token1_1d = out.token1_1d.saturating_add(*quote_abs);
        }
        if full_history {
            out.token0_all = out.token0_all.saturating_add(*base_abs);
            out.token1_all = out.token1_all.saturating_add(*quote_abs);
        }
    }

    out.has_all_time = full_history;
    cache.insert(*pool, out);
    Ok(out)
}

pub(crate) fn token_trade_windows(
    provider: &AmmDataProvider,
    pools_map: &HashMap<SchemaAlkaneId, SchemaMarketDefs>,
    token: &SchemaAlkaneId,
    now_ts: u64,
    in_block: &HashMap<SchemaAlkaneId, (u128, u128)>,
    pool_cache: &mut HashMap<SchemaAlkaneId, PoolTradeWindows>,
    full_history: bool,
) -> Result<TokenTradeWindows> {
    let pools = provider
        .get_token_pools(GetTokenPoolsParams { blockhash: StateAt::Latest, token: *token })
        .map(|res| res.pools)
        .unwrap_or_default();

    let mut out = TokenTradeWindows::default();
    for pool in pools {
        let defs = pools_map.get(&pool).cloned().or_else(|| {
            provider
                .get_pool_defs(GetPoolDefsParams { blockhash: StateAt::Latest, pool })
                .ok()
                .and_then(|res| res.defs)
        });
        let Some(defs) = defs else { continue };

        let token_is_base = defs.base_alkane_id == *token;
        let token_is_quote = defs.quote_alkane_id == *token;
        if !token_is_base && !token_is_quote {
            continue;
        }

        let pool_windows =
            pool_trade_windows(provider, &pool, now_ts, in_block, pool_cache, full_history)?;
        if token_is_base {
            out.amount_1d = out.amount_1d.saturating_add(pool_windows.token0_1d);
            out.amount_7d = out.amount_7d.saturating_add(pool_windows.token0_7d);
            out.amount_30d = out.amount_30d.saturating_add(pool_windows.token0_30d);
            if full_history {
                out.amount_all = out.amount_all.saturating_add(pool_windows.token0_all);
            }
        } else {
            out.amount_1d = out.amount_1d.saturating_add(pool_windows.token1_1d);
            out.amount_7d = out.amount_7d.saturating_add(pool_windows.token1_7d);
            out.amount_30d = out.amount_30d.saturating_add(pool_windows.token1_30d);
            if full_history {
                out.amount_all = out.amount_all.saturating_add(pool_windows.token1_all);
            }
        }

        if let Some((base_abs, quote_abs)) = in_block.get(&pool) {
            let block_amt = if token_is_base { *base_abs } else { *quote_abs };
            out.block_amount = out.block_amount.saturating_add(block_amt);
        }
    }

    out.has_all_time = full_history;
    Ok(out)
}

pub(crate) fn alkane_id_json(alkane: &SchemaAlkaneId) -> serde_json::Value {
    json!({ "block": alkane.block.to_string(), "tx": alkane.tx.to_string() })
}

pub(crate) fn canonical_quote_amount_tvl_usd(
    amount: u128,
    unit: CanonicalQuoteUnit,
    btc_price_usd: Option<u128>,
) -> Option<u128> {
    match unit {
        CanonicalQuoteUnit::Usd => Some(amount.saturating_mul(PRICE_SCALE) / AMOUNT_SCALE),
        CanonicalQuoteUnit::Btc => {
            let btc_price = btc_price_usd?;
            Some(amount.saturating_mul(btc_price) / AMOUNT_SCALE)
        }
    }
}

pub(crate) fn canonical_quote_amount_tvl_sats(
    amount: u128,
    unit: CanonicalQuoteUnit,
    btc_price_usd: Option<u128>,
) -> Option<u128> {
    match unit {
        CanonicalQuoteUnit::Btc => Some(amount),
        CanonicalQuoteUnit::Usd => {
            let btc_price = btc_price_usd?;
            if btc_price == 0 {
                return Some(0);
            }
            Some(amount.saturating_mul(PRICE_SCALE).saturating_div(btc_price))
        }
    }
}

pub(crate) fn load_balance_txs_by_height(
    essentials: &EssentialsProvider,
    height: u32,
) -> Result<BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>> {
    let table = essentials.table();
    let prefix = table.alkane_balance_txs_by_height_log_prefix(height);
    let entries = essentials
        .get_list_entries_desc(EssentialsGetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        })?
        .entries;

    let mut parsed_with_idx: BTreeMap<SchemaAlkaneId, Vec<(u32, AlkaneBalanceTxEntry)>> =
        BTreeMap::new();
    for (key, value) in entries {
        let Some((tx_idx, owner)) = table.parse_alkane_balance_txs_by_height_log_key(height, &key)
        else {
            continue;
        };
        let Ok(entry_id) = decode_pointer_idx_u64(&value) else {
            continue;
        };
        let Some(blob) = load_tx_pointer_blob_v3_by_id(essentials, entry_id) else {
            continue;
        };
        let entry = AlkaneBalanceTxEntry {
            txid: blob.txid,
            height: blob.height,
            outflow: blob.outflows.get(&owner).cloned().unwrap_or_default(),
        };
        parsed_with_idx.entry(owner).or_default().push((tx_idx, entry));
    }

    let mut parsed: BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>> = BTreeMap::new();
    for (owner, mut entries) in parsed_with_idx {
        entries.sort_by_key(|(tx_idx, _)| *tx_idx);
        parsed.insert(owner, entries.into_iter().map(|(_, entry)| entry).collect());
    }
    Ok(parsed)
}

pub struct AmmData {
    provider: Option<Arc<AmmDataProvider>>,
}

impl AmmData {
    pub fn new() -> Self {
        Self { provider: None }
    }

    #[inline]
    fn provider(&self) -> &AmmDataProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        let resp = self.provider().get_index_height(super::storage::GetIndexHeightParams {
            blockhash: StateAt::Latest,
        })?;
        Ok(resp.height)
    }

    fn persist_index_height(&self, height: u32, blockhash: StateAt) -> Result<()> {
        self.provider()
            .set_index_height(super::storage::SetIndexHeightParams { blockhash, height })
            .map_err(|e| anyhow!("[AMMDATA] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32, blockhash: StateAt) -> Result<()> {
        if let Some(prev) = self.load_index_height()? {
            if new_height < prev {
                eprintln!("[AMMDATA] index height rollback detected ({} -> {})", prev, new_height);
            }
        }
        self.persist_index_height(new_height, blockhash)?;
        Ok(())
    }
}

impl Default for AmmData {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for AmmData {
    fn get_name(&self) -> &'static str {
        "ammdata"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let essentials_provider =
            Arc::new(EssentialsProvider::new(get_espo_module_mdb("essentials")));
        self.provider = Some(Arc::new(AmmDataProvider::new(mdb.clone(), essentials_provider)));
        match self.load_index_height() {
            Ok(h) => eprintln!("[AMMDATA] loaded index height: {:?}", h),
            Err(e) => eprintln!("[AMMDATA] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        ammdata_genesis_block(network)
    }

    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        self.provider.as_ref().map(|provider| Arc::new(provider.mdb().clone()))
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let block_ts = block.block_header.time as u64;
        let block_hash = block.block_header.block_hash();
        let blockhash_state = StateAt::Block(block_hash);
        let height = block.height;
        println!("[AMMDATA] Indexing block #{height} for candles and activity...");

        let write_provider = self.provider();
        let read_provider = write_provider.with_view_blockhash(Some(block_hash));
        let provider = &read_provider;
        let essentials = provider.essentials();
        let search_cfg = AmmDataConfig::load_from_global_config().ok();
        let search_index_enabled =
            search_cfg.as_ref().map(|c| c.search_index_enabled).unwrap_or(false);
        let use_historical_backfill =
            search_cfg.as_ref().map(|c| c.use_historical_backfill).unwrap_or(true);
        let mut search_prefix_min =
            search_cfg.as_ref().map(|c| c.search_prefix_min_len as usize).unwrap_or(2);
        let mut search_prefix_max =
            search_cfg.as_ref().map(|c| c.search_prefix_max_len as usize).unwrap_or(6);
        let derived_quotes: Vec<DerivedQuoteConfig> = search_cfg
            .as_ref()
            .and_then(|c| c.derived_liquidity.as_ref())
            .map(|c| c.derived_quotes.clone())
            .unwrap_or_default();
        if search_prefix_min == 0 {
            search_prefix_min = 2;
        }
        if search_prefix_max < search_prefix_min {
            search_prefix_max = search_prefix_min;
        }

        let timer = debug::start_if(debug);
        let reserves_snapshot =
            crate::modules::ammdata::utils::index_snapshot::load_reserves_snapshot(&provider)?;
        let pools_map = crate::modules::ammdata::utils::index_snapshot::pools_map_from_snapshot(
            &reserves_snapshot,
        );
        let mut state = crate::modules::ammdata::utils::index_state::IndexState::new(
            reserves_snapshot,
            pools_map,
        );
        debug::log_elapsed(module, "load_snapshot", timer);

        let network = get_network();
        let canonical_quotes_list = canonical_quotes(network);
        let mut canonical_quote_units: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for cq in canonical_quotes_list.iter() {
            canonical_quote_units.insert(cq.id, cq.unit);
        }

        let timer = debug::start_if(debug);
        let (amm_factories, amm_factory_writes) =
            crate::modules::ammdata::utils::index_factories::prepare_factories(
                &block, provider, essentials,
            )?;
        state.amm_factory_writes = amm_factory_writes;
        debug::log_elapsed(module, "load_factories", timer);

        let timer = debug::start_if(debug);
        let boot_cnt =
            crate::modules::ammdata::utils::index_pools::bootstrap_pools_from_creation_records(
                blockhash_state.clone(),
                height,
                provider,
                essentials,
                essentials,
                &canonical_quote_units,
                &amm_factories,
                &mut state,
            )?;
        if boot_cnt > 0 {
            eprintln!("[AMMDATA] bootstrapped {boot_cnt} pools from creation records");
        }
        debug::log_elapsed(module, "bootstrap_pools_from_creation_records", timer);

        let frames = vec![Timeframe::M10];

        let timer = debug::start_if(debug);
        let discovery = crate::modules::ammdata::utils::index_pools::discover_new_pools(
            blockhash_state.clone(),
            &block,
            block_ts,
            height,
            provider,
            essentials,
            &canonical_quote_units,
            &amm_factories,
            &mut state,
        )?;
        debug::log_elapsed(module, "discover_new_pools", timer);

        let timer = debug::start_if(debug);
        crate::modules::ammdata::utils::index_activity::process_balance_deltas(
            block_ts,
            height,
            provider,
            essentials,
            &canonical_quote_units,
            &frames,
            &discovery.tx_meta,
            &mut state,
        );
        debug::log_elapsed(module, "process_traces_activity", timer);

        let timer = debug::start_if(debug);
        crate::modules::ammdata::utils::index_tokens::derive_token_data(
            block_ts,
            height,
            provider,
            essentials,
            &canonical_quote_units,
            &derived_quotes,
            use_historical_backfill,
            search_index_enabled,
            search_prefix_min,
            search_prefix_max,
            &mut state,
        )?;
        debug::log_elapsed(module, "derive_token_metrics", timer);

        let timer = debug::start_if(debug);
        crate::modules::ammdata::utils::index_pool_metrics::derive_pool_metrics(
            blockhash_state.clone(),
            block_ts,
            height,
            provider,
            essentials,
            network,
            &canonical_quote_units,
            &mut state,
        )?;
        debug::log_elapsed(module, "pool_metrics_tvl", timer);

        let timer = debug::start_if(debug);
        let finalize =
            crate::modules::ammdata::utils::index_finalize::prepare_batch(provider, &mut state)?;
        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, token_usd_candles={tc_cnt}, token_mcusd_candles={tmc_cnt}, token_derived_usd_candles={tdc_cnt}, token_derived_mcusd_candles={tdmc_cnt}, chart_changes={cc_cnt}, token_metrics={tm_cnt}, token_metrics_index={tmi_cnt}, token_search_index={tsi_cnt}, token_derived_metrics={tdm_cnt}, token_derived_metrics_index={tdmi_cnt}, token_derived_search_index={tdsi_cnt}, btc_usd_price={btc_cnt}, btc_usd_line={btcl_cnt}, canonical_pools={cp_cnt}, pool_name_index={pn_cnt}, amm_factories={af_cnt}, factory_pools={fp_cnt}, pool_factory={pf_cnt}, pool_creation_info={pc_cnt}, pool_creations={pcg_cnt}, token_pools={tp_cnt}, pool_defs={pd_cnt}, pool_metrics={pm_cnt}, pool_metrics_index={pmi_cnt}, pool_lp_supply={pls_cnt}, pool_details_snapshot={pds_cnt}, tvl_versioned={tvl_cnt}, token_swaps={ts_cnt}, address_pool_swaps={aps_cnt}, address_token_swaps={ats_cnt}, address_pool_creations={apc_cnt}, address_pool_mints={apm_cnt}, address_pool_burns={apb_cnt}, address_amm_history={aah_cnt}, amm_history_all={ah_cnt}, activity={a_cnt}, indexes+counts={i_cnt}, reserves_snapshot=1",
            h = block.height,
            c_cnt = finalize.stats.candle_writes,
            tc_cnt = finalize.stats.token_usd_candles,
            tmc_cnt = finalize.stats.token_mcusd_candles,
            tdc_cnt = finalize.stats.token_derived_usd_candles,
            tdmc_cnt = finalize.stats.token_derived_mcusd_candles,
            cc_cnt = finalize.stats.chart_changes,
            tm_cnt = finalize.stats.token_metrics,
            tmi_cnt = finalize.stats.token_metrics_index,
            tsi_cnt = finalize.stats.token_search_index,
            tdm_cnt = finalize.stats.derived_metrics,
            tdmi_cnt = finalize.stats.derived_metrics_index,
            tdsi_cnt = finalize.stats.derived_search_index,
            btc_cnt = finalize.stats.btc_usd_price,
            btcl_cnt = finalize.stats.btc_usd_line,
            cp_cnt = finalize.stats.canonical_pools,
            pn_cnt = finalize.stats.pool_name_index,
            af_cnt = finalize.stats.amm_factories,
            fp_cnt = finalize.stats.factory_pools,
            pf_cnt = finalize.stats.pool_factory,
            pc_cnt = finalize.stats.pool_creation_info,
            pcg_cnt = finalize.stats.pool_creations,
            pd_cnt = finalize.stats.pool_defs,
            pm_cnt = finalize.stats.pool_metrics,
            pmi_cnt = finalize.stats.pool_metrics_index,
            pls_cnt = finalize.stats.pool_lp_supply,
            tvl_cnt = finalize.stats.tvl_versioned,
            ts_cnt = finalize.stats.token_swaps,
            apc_cnt = finalize.stats.address_pool_creations,
            apm_cnt = finalize.stats.address_pool_mints,
            apb_cnt = finalize.stats.address_pool_burns,
            aah_cnt = finalize.stats.address_amm_history,
            ah_cnt = finalize.stats.amm_history_all,
            tp_cnt = finalize.stats.token_pools,
            pds_cnt = finalize.stats.pool_details_snapshot,
            aps_cnt = finalize.stats.address_pool_swaps,
            ats_cnt = finalize.stats.address_token_swaps,
            a_cnt = finalize.stats.activity,
            i_cnt = finalize.stats.index_writes,
        );

        if finalize.should_write {
            write_provider
                .set_batch(SetBatchParams {
                    blockhash: StateAt::Latest,
                    puts: finalize.puts,
                    deletes: finalize.deletes,
                })
                .map_err(|e| anyhow!("[AMMDATA] set_batch failed at height {}: {e}", height))?;
        }

        debug::log_elapsed(module, "write_batch", timer);
        println!(
            "[AMMDATA] Finished processing block #{} with {} traces",
            block.height,
            block.transactions.len()
        );
        self.set_index_height(block.height, StateAt::Latest)?;
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        self.load_index_height().ok().flatten()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let provider = self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").clone();
        register_rpc(reg, provider);
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(AmmDataConfig::spec())
    }

    fn set_config(&mut self, config: &serde_json::Value) -> Result<()> {
        AmmDataConfig::from_value(config).map(|_| ())
    }
}
