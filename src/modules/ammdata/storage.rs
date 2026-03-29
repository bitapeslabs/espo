use super::schemas::{
    ActivityKind, SchemaActivityV1, SchemaCandleV1, SchemaCanonicalPoolEntry, SchemaMarketDefs,
    SchemaPoolCreationInfoV1, SchemaPoolDetailsSnapshot, SchemaPoolMetricsV1, SchemaPoolMetricsV2,
    SchemaPoolSnapshot, SchemaReservesSnapshot, SchemaTokenMetricsV1, Timeframe,
};
use crate::config::get_network;
use crate::modules::ammdata::consts::{
    CanonicalQuoteUnit, KEY_INDEX_HEIGHT, PRICE_SCALE, SATS_PER_BTC, canonical_quotes,
};
use crate::modules::ammdata::schemas::SchemaFullCandleV1;
use crate::modules::ammdata::utils::activity::{
    ActivityFilter, ActivityPage, ActivitySideFilter, ActivitySortKey, SortDir, decode_activity_v1,
    read_activity_for_pool, read_activity_for_pool_sorted,
};
use crate::modules::ammdata::utils::candles::{CandleSlice, PriceSide, read_candles_v1};
use crate::modules::ammdata::utils::live_reserves::fetch_all_pools;
use crate::modules::ammdata::utils::pathfinder::{
    DEFAULT_FEE_BPS, plan_best_mev_swap, plan_exact_in_default_fee, plan_exact_out_default_fee,
    plan_implicit_default_fee, plan_swap_exact_tokens_for_tokens,
    plan_swap_exact_tokens_for_tokens_implicit, plan_swap_tokens_for_exact_tokens,
};
use crate::modules::essentials::storage::EssentialsProvider;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::pointers::{CursorScanPage, KvPointer, ListPointer};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::BlockHash;
use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::{Value, json, map::Map};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
pub struct AmmDataTable<'a> {
    pub ROOT: KvPointer<'a>,
    // Core index height for the ammdata module.
    pub INDEX_HEIGHT: KvPointer<'a>,
    // Reserve snapshots and pool summaries.
    pub RESERVES_SNAPSHOT: ListPointer<'a>,
    pub POOLS: KvPointer<'a>,
    // Candle series (fc1:<blk>:<tx>:<tf>:...).
    pub CANDLES: ListPointer<'a>,
    pub TOKEN_USD_CANDLES: ListPointer<'a>,
    pub TOKEN_DERIVED_USD_CANDLES: ListPointer<'a>,
    pub TOKEN_MCAP_USD_CANDLES: ListPointer<'a>,
    pub BTC_USD_PRICE: KvPointer<'a>,
    pub BTC_USD_LINE: ListPointer<'a>,
    pub TOKEN_DERIVED_MCAP_USD_CANDLES: ListPointer<'a>,
    pub CHART_CHANGE_EVENTS: KvPointer<'a>,
    pub CHART_CHANGE_LATEST: KvPointer<'a>,
    // Activity logs + secondary indexes for sort/paging.
    pub ACTIVITY: ListPointer<'a>,
    pub ACTIVITY_INDEX: ListPointer<'a>,
    // Token-level indices.
    pub CANONICAL_POOL: ListPointer<'a>,
    pub TOKEN_METRICS: KvPointer<'a>,
    pub TOKEN_DERIVED_METRICS: KvPointer<'a>,
    pub TOKEN_METRICS_INDEX: ListPointer<'a>,
    pub TOKEN_DERIVED_METRICS_INDEX: ListPointer<'a>,
    pub TOKEN_METRICS_INDEX_COUNT: KvPointer<'a>,
    pub TOKEN_DERIVED_METRICS_INDEX_COUNT: KvPointer<'a>,
    pub POOL_METRICS_INDEX: ListPointer<'a>,
    pub POOL_METRICS_INDEX_COUNT: KvPointer<'a>,
    pub TOKEN_SEARCH_INDEX: ListPointer<'a>,
    pub TOKEN_DERIVED_SEARCH_INDEX: ListPointer<'a>,
    pub POOL_NAME_INDEX: ListPointer<'a>,
    // Factory + pool indices.
    pub AMM_FACTORIES: ListPointer<'a>,
    pub FACTORY_BOOTSTRAP_CREATION_COUNT: KvPointer<'a>,
    pub POOL_BOOTSTRAP_CREATION_COUNT: KvPointer<'a>,
    pub FACTORY_POOLS: ListPointer<'a>,
    pub POOL_FACTORY: KvPointer<'a>,
    pub POOL_METRICS: KvPointer<'a>,
    pub POOL_METRICS_V2: KvPointer<'a>,
    pub POOL_CREATION_INFO: KvPointer<'a>,
    pub POOL_LP_SUPPLY: KvPointer<'a>,
    pub POOL_DETAILS_SNAPSHOT: KvPointer<'a>,
    pub TVL_VERSIONED: KvPointer<'a>,
    pub TOKEN_SWAPS: ListPointer<'a>,
    pub POOL_CREATIONS: ListPointer<'a>,
    pub ADDRESS_POOL_SWAPS: ListPointer<'a>,
    pub ADDRESS_TOKEN_SWAPS: ListPointer<'a>,
    pub ADDRESS_POOL_CREATIONS: ListPointer<'a>,
    pub ADDRESS_POOL_MINTS: ListPointer<'a>,
    pub ADDRESS_POOL_BURNS: ListPointer<'a>,
    pub ADDRESS_AMM_HISTORY: ListPointer<'a>,
    pub AMM_HISTORY_ALL: ListPointer<'a>,
    pub TOKEN_POOLS: ListPointer<'a>,
}

impl<'a> AmmDataTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = KvPointer::root(mdb);
        AmmDataTable {
            ROOT: root.clone(),
            INDEX_HEIGHT: root.select(KEY_INDEX_HEIGHT),
            RESERVES_SNAPSHOT: root.list_keyword("/reserves_snapshot/v2/pool/"),
            POOLS: root.keyword("/pools/"),
            CANDLES: root.list_keyword("fc1:"),
            TOKEN_USD_CANDLES: root.list_keyword("tuc1:"),
            TOKEN_DERIVED_USD_CANDLES: root.list_keyword("tud1:"),
            TOKEN_MCAP_USD_CANDLES: root.list_keyword("tmc1:"),
            BTC_USD_PRICE: root.keyword("/btc_usd_price/v1/"),
            BTC_USD_LINE: root.list_keyword("btu1:"),
            TOKEN_DERIVED_MCAP_USD_CANDLES: root.list_keyword("tdmc1:"),
            CHART_CHANGE_EVENTS: root.keyword("/chart_change_events/v1/"),
            CHART_CHANGE_LATEST: root.keyword("/chart_change_latest/v1/"),
            ACTIVITY: root.list_keyword("activity:v1:"),
            ACTIVITY_INDEX: root.list_keyword("activity:idx:"),
            CANONICAL_POOL: root.list_keyword("/canonical_pool/v2/"),
            TOKEN_METRICS: root.keyword("/token_metrics/v1/"),
            TOKEN_DERIVED_METRICS: root.keyword("/token_metrics/derived/v1/"),
            TOKEN_METRICS_INDEX: root.list_keyword("/token_metrics/index/"),
            TOKEN_DERIVED_METRICS_INDEX: root.list_keyword("/token_metrics/derived/index/"),
            TOKEN_METRICS_INDEX_COUNT: root.keyword("/token_metrics/index_count"),
            TOKEN_DERIVED_METRICS_INDEX_COUNT: root.keyword("/token_metrics/derived/index_count"),
            POOL_METRICS_INDEX: root.list_keyword("/pool_metrics/index/"),
            POOL_METRICS_INDEX_COUNT: root.keyword("/pool_metrics/index_count"),
            TOKEN_SEARCH_INDEX: root.list_keyword("/token_search_index/v1/"),
            TOKEN_DERIVED_SEARCH_INDEX: root.list_keyword("/token_search_index/derived/v1/"),
            POOL_NAME_INDEX: root.list_keyword("/pool_name_index/"),
            AMM_FACTORIES: root.list_keyword("/amm_factories/v1/"),
            FACTORY_BOOTSTRAP_CREATION_COUNT: root
                .keyword("/bootstrap/factories/creation_count/v1"),
            POOL_BOOTSTRAP_CREATION_COUNT: root.keyword("/bootstrap/pools/creation_count/v1"),
            FACTORY_POOLS: root.list_keyword("/factory_pools/v1/"),
            POOL_FACTORY: root.keyword("/pool_factory/v1/"),
            POOL_METRICS: root.keyword("/pool_metrics/v1/"),
            POOL_METRICS_V2: root.keyword("/pool_metrics/v2/"),
            POOL_CREATION_INFO: root.keyword("/pool_creation_info/v1/"),
            POOL_LP_SUPPLY: root.keyword("/pool_lp_supply/latest/"),
            POOL_DETAILS_SNAPSHOT: root.keyword("/pool_details/v2/"),
            TVL_VERSIONED: root.keyword("/tvlVersioned/"),
            TOKEN_SWAPS: root.list_keyword("/token_swaps/v1/"),
            POOL_CREATIONS: root.list_keyword("/pool_creations/v1/"),
            ADDRESS_POOL_SWAPS: root.list_keyword("/address_pool_swaps/v1/"),
            ADDRESS_TOKEN_SWAPS: root.list_keyword("/address_token_swaps/v1/"),
            ADDRESS_POOL_CREATIONS: root.list_keyword("/address_pool_creations/v1/"),
            ADDRESS_POOL_MINTS: root.list_keyword("/address_pool_mints/v1/"),
            ADDRESS_POOL_BURNS: root.list_keyword("/address_pool_burns/v1/"),
            ADDRESS_AMM_HISTORY: root.list_keyword("/address_amm_history/v1/"),
            AMM_HISTORY_ALL: root.list_keyword("/amm_history_all/v1/"),
            TOKEN_POOLS: root.list_keyword("/token_pools/v1/"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenMetricsIndexField {
    PriceUsd,
    MarketcapUsd,
    Volume1d,
    Volume7d,
    Volume30d,
    VolumeAllTime,
    Change1d,
    Change7d,
    Change30d,
    ChangeAllTime,
}

impl TokenMetricsIndexField {
    pub fn as_str(&self) -> &'static str {
        match self {
            TokenMetricsIndexField::PriceUsd => "price",
            TokenMetricsIndexField::MarketcapUsd => "marketcap",
            TokenMetricsIndexField::Volume1d => "volume_1d",
            TokenMetricsIndexField::Volume7d => "volume_7d",
            TokenMetricsIndexField::Volume30d => "volume_30d",
            TokenMetricsIndexField::VolumeAllTime => "volume_all_time",
            TokenMetricsIndexField::Change1d => "change_1d",
            TokenMetricsIndexField::Change7d => "change_7d",
            TokenMetricsIndexField::Change30d => "change_30d",
            TokenMetricsIndexField::ChangeAllTime => "change_all_time",
        }
    }

    pub fn value_len(&self) -> usize {
        match self {
            TokenMetricsIndexField::Change1d
            | TokenMetricsIndexField::Change7d
            | TokenMetricsIndexField::Change30d
            | TokenMetricsIndexField::ChangeAllTime => 8,
            _ => 16,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PoolMetricsIndexField {
    TvlUsd,
    Volume1dUsd,
    Volume7dUsd,
    Volume30dUsd,
    VolumeAllTimeUsd,
    Apr,
    TvlChange24h,
}

impl PoolMetricsIndexField {
    pub fn as_str(&self) -> &'static str {
        match self {
            PoolMetricsIndexField::TvlUsd => "tvl_usd",
            PoolMetricsIndexField::Volume1dUsd => "volume_1d_usd",
            PoolMetricsIndexField::Volume7dUsd => "volume_7d_usd",
            PoolMetricsIndexField::Volume30dUsd => "volume_30d_usd",
            PoolMetricsIndexField::VolumeAllTimeUsd => "volume_all_time_usd",
            PoolMetricsIndexField::Apr => "apr",
            PoolMetricsIndexField::TvlChange24h => "tvl_change_24h",
        }
    }

    pub fn value_len(&self) -> usize {
        match self {
            PoolMetricsIndexField::Apr | PoolMetricsIndexField::TvlChange24h => 8,
            _ => 16,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SearchIndexField {
    Marketcap,
    Holders,
    Volume7d,
    Change7d,
    VolumeAllTime,
}

impl SearchIndexField {
    pub fn as_str(&self) -> &'static str {
        match self {
            SearchIndexField::Marketcap => "marketcap",
            SearchIndexField::Holders => "holders",
            SearchIndexField::Volume7d => "volume_7d",
            SearchIndexField::Change7d => "change_7d",
            SearchIndexField::VolumeAllTime => "volume_all_time",
        }
    }

    pub fn score_len(&self) -> usize {
        match self {
            SearchIndexField::Change7d => 8,
            SearchIndexField::Holders => 8,
            SearchIndexField::Marketcap
            | SearchIndexField::Volume7d
            | SearchIndexField::VolumeAllTime => 16,
        }
    }
}

pub fn parse_change_basis_points(change: &str) -> i64 {
    let raw = change.trim();
    if raw.is_empty() {
        return 0;
    }

    let (neg, body) = if let Some(rest) = raw.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        (false, rest)
    } else {
        (false, raw)
    };
    if body.is_empty() {
        return 0;
    }

    let (whole_s, frac_s) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };

    if !whole_s.chars().all(|c| c.is_ascii_digit()) || !frac_s.chars().all(|c| c.is_ascii_digit()) {
        return 0;
    }

    let whole =
        if whole_s.is_empty() { 0u128 } else { whole_s.parse::<u128>().unwrap_or(u128::MAX) };

    let mut frac4 = 0u128;
    let mut count = 0usize;
    let mut round_up = false;
    for (idx, ch) in frac_s.chars().enumerate() {
        if idx < 4 {
            frac4 = frac4.saturating_mul(10).saturating_add((ch as u8 - b'0') as u128);
            count += 1;
        } else {
            round_up = ch >= '5';
            break;
        }
    }
    while count < 4 {
        frac4 = frac4.saturating_mul(10);
        count += 1;
    }

    let mut scaled = whole.saturating_mul(10_000).saturating_add(frac4);
    if round_up {
        scaled = scaled.saturating_add(1);
    }

    if neg {
        let min_mag = (i64::MAX as u128).saturating_add(1);
        if scaled >= min_mag { i64::MIN } else { -(scaled as i64) }
    } else if scaled > i64::MAX as u128 {
        i64::MAX
    } else {
        scaled as i64
    }
}

pub fn format_change_basis_points(value: i64) -> String {
    let neg = value < 0;
    let mag = if neg {
        value
            .checked_abs()
            .map(|v| v as u64)
            .unwrap_or((i64::MAX as u64).saturating_add(1))
    } else {
        value as u64
    };
    let whole = mag / 10_000;
    let frac = mag % 10_000;
    if neg { format!("-{whole}.{frac:04}") } else { format!("{whole}.{frac:04}") }
}

pub fn change_basis_points_to_f64(value: i64) -> f64 {
    (value as f64) / 10_000.0
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaChartChangeValueV1 {
    pub usd_bp: i64,
    pub mcusd_bp: i64,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaChartChangeSetV1 {
    pub timeframe_changes: BTreeMap<String, SchemaChartChangeValueV1>,
}

pub fn encode_i64_be_ordered(value: i64) -> [u8; 8] {
    let biased = (value as u64) ^ (1u64 << 63);
    biased.to_be_bytes()
}

impl<'a> AmmDataTable<'a> {
    pub fn candle_ns_prefix(&self, pool: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
        let blk_hex = format!("{:x}", pool.block);
        let tx_hex = format!("{:x}", pool.tx);
        let suffix = format!("{}:{}:{}:", blk_hex, tx_hex, tf.code());
        self.CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn activity_ns_prefix(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let suffix = format!("{}:{}:", pool.block, pool.tx);
        self.ACTIVITY.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn activity_key(&self, pool: &SchemaAlkaneId, ts: u64, seq: u32) -> Vec<u8> {
        let mut k = self.activity_ns_prefix(pool);
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k
    }

    pub fn candle_key_seq(
        &self,
        pool: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
        seq: u32,
    ) -> Vec<u8> {
        let mut k = self.candle_ns_prefix(pool, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k
    }

    pub fn candle_key(&self, pool: &SchemaAlkaneId, tf: Timeframe, bucket_ts: u64) -> Vec<u8> {
        let mut k = self.candle_ns_prefix(pool, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn token_usd_candle_ns_prefix(&self, token: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
        let blk_hex = format!("{:x}", token.block);
        let tx_hex = format!("{:x}", token.tx);
        let suffix = format!("{}:{}:{}:", blk_hex, tx_hex, tf.code());
        self.TOKEN_USD_CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn token_usd_candle_key(
        &self,
        token: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
    ) -> Vec<u8> {
        let mut k = self.token_usd_candle_ns_prefix(token, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn token_derived_usd_candle_ns_prefix(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
        tf: Timeframe,
    ) -> Vec<u8> {
        let blk_hex = format!("{:x}", token.block);
        let tx_hex = format!("{:x}", token.tx);
        let q_blk_hex = format!("{:x}", quote.block);
        let q_tx_hex = format!("{:x}", quote.tx);
        let suffix = format!("{}:{}:{}:{}:{}:", blk_hex, tx_hex, q_blk_hex, q_tx_hex, tf.code());
        self.TOKEN_DERIVED_USD_CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn token_derived_usd_candle_key(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
    ) -> Vec<u8> {
        let mut k = self.token_derived_usd_candle_ns_prefix(token, quote, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn token_mcusd_candle_ns_prefix(&self, token: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
        let blk_hex = format!("{:x}", token.block);
        let tx_hex = format!("{:x}", token.tx);
        let suffix = format!("{}:{}:{}:", blk_hex, tx_hex, tf.code());
        self.TOKEN_MCAP_USD_CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn token_mcusd_candle_key(
        &self,
        token: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
    ) -> Vec<u8> {
        let mut k = self.token_mcusd_candle_ns_prefix(token, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn token_derived_mcusd_candle_ns_prefix(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
        tf: Timeframe,
    ) -> Vec<u8> {
        let blk_hex = format!("{:x}", token.block);
        let tx_hex = format!("{:x}", token.tx);
        let q_blk_hex = format!("{:x}", quote.block);
        let q_tx_hex = format!("{:x}", quote.tx);
        let suffix = format!("{}:{}:{}:{}:{}:", blk_hex, tx_hex, q_blk_hex, q_tx_hex, tf.code());
        self.TOKEN_DERIVED_MCAP_USD_CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn token_derived_mcusd_candle_key(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
    ) -> Vec<u8> {
        let mut k = self.token_derived_mcusd_candle_ns_prefix(token, quote, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn btc_usd_price_prefix(&self) -> Vec<u8> {
        self.BTC_USD_PRICE.key().to_vec()
    }

    pub fn btc_usd_price_key(&self, height: u64) -> Vec<u8> {
        let mut k = self.btc_usd_price_prefix();
        k.extend_from_slice(&height.to_be_bytes());
        k
    }

    pub fn parse_btc_usd_price_key(&self, key: &[u8]) -> Option<u64> {
        let prefix = self.btc_usd_price_prefix();
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != 8 {
            return None;
        }
        let mut height = [0u8; 8];
        height.copy_from_slice(rest);
        Some(u64::from_be_bytes(height))
    }

    pub fn btc_usd_line_ns_prefix(&self, tf: Timeframe) -> Vec<u8> {
        let suffix = format!("{}:", tf.code());
        self.BTC_USD_LINE.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn btc_usd_line_key(&self, tf: Timeframe, bucket_ts: u64) -> Vec<u8> {
        let mut k = self.btc_usd_line_ns_prefix(tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn chart_change_events_prefix(&self) -> Vec<u8> {
        self.CHART_CHANGE_EVENTS.key().to_vec()
    }

    pub fn chart_change_event_key(&self, height: u64, chart: &str) -> Vec<u8> {
        let mut k = self.chart_change_events_prefix();
        k.extend_from_slice(&height.to_be_bytes());
        k.push(b'/');
        k.extend_from_slice(chart.as_bytes());
        k
    }

    pub fn chart_change_event_height_prefix(&self, height: u64) -> Vec<u8> {
        let mut k = self.chart_change_events_prefix();
        k.extend_from_slice(&height.to_be_bytes());
        k.push(b'/');
        k
    }

    pub fn parse_chart_change_event_key_for_height(
        &self,
        height: u64,
        key: &[u8],
    ) -> Option<String> {
        let prefix = self.chart_change_event_height_prefix(height);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        std::str::from_utf8(rest).ok().map(|s| s.to_string())
    }

    pub fn chart_change_latest_key(&self, chart: &str) -> Vec<u8> {
        self.CHART_CHANGE_LATEST.select(chart.as_bytes()).key().to_vec()
    }

    pub fn token_metrics_index_prefix(&self, field: TokenMetricsIndexField) -> Vec<u8> {
        let mut k = self.TOKEN_METRICS_INDEX.select(field.as_str().as_bytes()).key().to_vec();
        k.push(b'/');
        k
    }

    pub fn token_metrics_index_key_u128(
        &self,
        field: TokenMetricsIndexField,
        value: u128,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_metrics_index_prefix(field);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_metrics_index_key_i64(
        &self,
        field: TokenMetricsIndexField,
        value: i64,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_metrics_index_prefix(field);
        k.extend_from_slice(&encode_i64_be_ordered(value));
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn parse_token_metrics_index_key(
        &self,
        field: TokenMetricsIndexField,
        key: &[u8],
    ) -> Option<SchemaAlkaneId> {
        let prefix = self.token_metrics_index_prefix(field);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != field.value_len() + 12 {
            return None;
        }
        let id_bytes = &rest[field.value_len()..];
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        Some(SchemaAlkaneId {
            block: u32::from_be_bytes(block_arr),
            tx: u64::from_be_bytes(tx_arr),
        })
    }

    pub fn token_metrics_index_count_key(&self) -> Vec<u8> {
        self.TOKEN_METRICS_INDEX_COUNT.key().to_vec()
    }

    pub fn pool_metrics_index_prefix(&self, field: PoolMetricsIndexField) -> Vec<u8> {
        let mut k = self.POOL_METRICS_INDEX.select(field.as_str().as_bytes()).key().to_vec();
        k.push(b'/');
        k
    }

    pub fn pool_metrics_index_key_u128(
        &self,
        field: PoolMetricsIndexField,
        value: u128,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.pool_metrics_index_prefix(field);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn pool_metrics_index_key_i64(
        &self,
        field: PoolMetricsIndexField,
        value: i64,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.pool_metrics_index_prefix(field);
        k.extend_from_slice(&encode_i64_be_ordered(value));
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn parse_pool_metrics_index_key(
        &self,
        field: PoolMetricsIndexField,
        key: &[u8],
    ) -> Option<SchemaAlkaneId> {
        let prefix = self.pool_metrics_index_prefix(field);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != field.value_len() + 12 {
            return None;
        }
        let id_bytes = &rest[field.value_len()..];
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        Some(SchemaAlkaneId {
            block: u32::from_be_bytes(block_arr),
            tx: u64::from_be_bytes(tx_arr),
        })
    }

    pub fn pool_metrics_index_count_key(&self) -> Vec<u8> {
        self.POOL_METRICS_INDEX_COUNT.key().to_vec()
    }

    pub fn token_search_index_prefix(&self, field: SearchIndexField, prefix: &str) -> Vec<u8> {
        let mut k = self.TOKEN_SEARCH_INDEX.select(field.as_str().as_bytes()).key().to_vec();
        k.push(b'/');
        k.extend_from_slice(prefix.as_bytes());
        k.push(b'/');
        k
    }

    pub fn token_search_index_key_u128(
        &self,
        field: SearchIndexField,
        prefix: &str,
        value: u128,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_search_index_prefix(field, prefix);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_search_index_key_u64(
        &self,
        field: SearchIndexField,
        prefix: &str,
        value: u64,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_search_index_prefix(field, prefix);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_search_index_key_i64(
        &self,
        field: SearchIndexField,
        prefix: &str,
        value: i64,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_search_index_prefix(field, prefix);
        k.extend_from_slice(&encode_i64_be_ordered(value));
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn parse_token_search_index_key(
        &self,
        field: SearchIndexField,
        key: &[u8],
    ) -> Option<SchemaAlkaneId> {
        if key.len() < field.score_len() + 12 {
            return None;
        }
        let id_bytes = &key[key.len() - 12..];
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        Some(SchemaAlkaneId {
            block: u32::from_be_bytes(block_arr),
            tx: u64::from_be_bytes(tx_arr),
        })
    }

    pub fn canonical_pool_token_prefix(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.CANONICAL_POOL.key().to_vec();
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k.push(b'/');
        k
    }

    pub fn canonical_pool_quote_key(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.canonical_pool_token_prefix(token);
        k.extend_from_slice(&quote.block.to_be_bytes());
        k.extend_from_slice(&quote.tx.to_be_bytes());
        k
    }

    pub fn token_metrics_key(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        self.TOKEN_METRICS.select(&suffix).key().to_vec()
    }

    pub fn token_derived_metrics_key(
        &self,
        token: &SchemaAlkaneId,
        quote: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(24);
        suffix.extend_from_slice(&quote.block.to_be_bytes());
        suffix.extend_from_slice(&quote.tx.to_be_bytes());
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        self.TOKEN_DERIVED_METRICS.select(&suffix).key().to_vec()
    }

    pub fn token_derived_metrics_index_prefix(
        &self,
        quote: &SchemaAlkaneId,
        field: TokenMetricsIndexField,
    ) -> Vec<u8> {
        let mut k = self.TOKEN_DERIVED_METRICS_INDEX.key().to_vec();
        k.extend_from_slice(&quote.block.to_be_bytes());
        k.extend_from_slice(&quote.tx.to_be_bytes());
        k.push(b'/');
        k.extend_from_slice(field.as_str().as_bytes());
        k.push(b'/');
        k
    }

    pub fn token_derived_metrics_index_key_u128(
        &self,
        quote: &SchemaAlkaneId,
        field: TokenMetricsIndexField,
        value: u128,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_derived_metrics_index_prefix(quote, field);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_derived_metrics_index_key_i64(
        &self,
        quote: &SchemaAlkaneId,
        field: TokenMetricsIndexField,
        value: i64,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_derived_metrics_index_prefix(quote, field);
        k.extend_from_slice(&encode_i64_be_ordered(value));
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn parse_token_derived_metrics_index_key(
        &self,
        quote: &SchemaAlkaneId,
        field: TokenMetricsIndexField,
        key: &[u8],
    ) -> Option<SchemaAlkaneId> {
        let prefix = self.token_derived_metrics_index_prefix(quote, field);
        if !key.starts_with(&prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() != field.value_len() + 12 {
            return None;
        }
        let id_bytes = &rest[field.value_len()..];
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        Some(SchemaAlkaneId {
            block: u32::from_be_bytes(block_arr),
            tx: u64::from_be_bytes(tx_arr),
        })
    }

    pub fn token_derived_metrics_index_count_key(&self, quote: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&quote.block.to_be_bytes());
        suffix.extend_from_slice(&quote.tx.to_be_bytes());
        self.TOKEN_DERIVED_METRICS_INDEX_COUNT.select(&suffix).key().to_vec()
    }

    pub fn token_derived_search_index_prefix(
        &self,
        quote: &SchemaAlkaneId,
        field: SearchIndexField,
        prefix: &str,
    ) -> Vec<u8> {
        let mut k = self.TOKEN_DERIVED_SEARCH_INDEX.key().to_vec();
        k.extend_from_slice(&quote.block.to_be_bytes());
        k.extend_from_slice(&quote.tx.to_be_bytes());
        k.push(b'/');
        k.extend_from_slice(field.as_str().as_bytes());
        k.push(b'/');
        k.extend_from_slice(prefix.as_bytes());
        k.push(b'/');
        k
    }

    pub fn token_derived_search_index_key_u128(
        &self,
        quote: &SchemaAlkaneId,
        field: SearchIndexField,
        prefix: &str,
        value: u128,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_derived_search_index_prefix(quote, field, prefix);
        k.extend_from_slice(&value.to_be_bytes());
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_derived_search_index_key_i64(
        &self,
        quote: &SchemaAlkaneId,
        field: SearchIndexField,
        prefix: &str,
        value: i64,
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_derived_search_index_prefix(quote, field, prefix);
        k.extend_from_slice(&encode_i64_be_ordered(value));
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn parse_token_derived_search_index_key(
        &self,
        _quote: &SchemaAlkaneId,
        field: SearchIndexField,
        key: &[u8],
    ) -> Option<SchemaAlkaneId> {
        if key.len() < field.score_len() + 12 {
            return None;
        }
        let id_bytes = &key[key.len() - 12..];
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        Some(SchemaAlkaneId {
            block: u32::from_be_bytes(block_arr),
            tx: u64::from_be_bytes(tx_arr),
        })
    }

    pub fn pool_name_index_key(&self, name: &str, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(name.len() + 1 + 12);
        suffix.extend_from_slice(name.as_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_NAME_INDEX.select(&suffix).key().to_vec()
    }

    pub fn pool_name_index_prefix(&self, name_prefix: &str) -> Vec<u8> {
        self.POOL_NAME_INDEX.select(name_prefix.as_bytes()).key().to_vec()
    }

    pub fn parse_pool_name_index_key(&self, key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
        let prefix = self.POOL_NAME_INDEX.key();
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

    pub fn amm_factory_key(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        self.AMM_FACTORIES.select(&suffix).key().to_vec()
    }

    pub fn factory_bootstrap_creation_count_key(&self) -> Vec<u8> {
        self.FACTORY_BOOTSTRAP_CREATION_COUNT.key().to_vec()
    }

    pub fn pool_bootstrap_creation_count_key(&self) -> Vec<u8> {
        self.POOL_BOOTSTRAP_CREATION_COUNT.key().to_vec()
    }

    pub fn factory_pools_key(&self, factory: &SchemaAlkaneId, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1 + 12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.FACTORY_POOLS.select(&suffix).key().to_vec()
    }

    pub fn factory_pools_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        self.FACTORY_POOLS.select(&suffix).key().to_vec()
    }

    pub fn pool_factory_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_FACTORY.select(&suffix).key().to_vec()
    }

    pub fn pool_metrics_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_METRICS.select(&suffix).key().to_vec()
    }

    pub fn pool_metrics_v2_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_METRICS_V2.select(&suffix).key().to_vec()
    }

    pub fn pool_creation_info_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_CREATION_INFO.select(&suffix).key().to_vec()
    }

    pub fn pool_lp_supply_latest_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_LP_SUPPLY.select(&suffix).key().to_vec()
    }

    pub fn pool_details_snapshot_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(13);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        suffix.push(b'/');
        self.POOL_DETAILS_SNAPSHOT.select(&suffix).key().to_vec()
    }

    pub fn pool_details_snapshot_field_key(&self, pool: &SchemaAlkaneId, field: &str) -> Vec<u8> {
        let mut k = self.pool_details_snapshot_key(pool);
        k.extend_from_slice(field.as_bytes());
        k
    }

    pub fn pool_details_snapshot_fields_prefix(&self) -> Vec<u8> {
        self.POOL_DETAILS_SNAPSHOT.key().to_vec()
    }

    pub fn tvl_versioned_prefix(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        suffix.push(b'/');
        self.TVL_VERSIONED.select(&suffix).key().to_vec()
    }

    pub fn tvl_versioned_key(&self, pool: &SchemaAlkaneId, height: u32) -> Vec<u8> {
        let mut k = self.tvl_versioned_prefix(pool);
        k.extend_from_slice(&height.to_be_bytes());
        k
    }

    pub fn token_swaps_prefix(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        suffix.push(b'/');
        self.TOKEN_SWAPS.select(&suffix).key().to_vec()
    }

    pub fn token_swaps_key(
        &self,
        token: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_swaps_prefix(token);
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k.push(b'/');
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn pool_creations_prefix(&self) -> Vec<u8> {
        self.POOL_CREATIONS.key().to_vec()
    }

    pub fn pool_creations_key(&self, ts: u64, seq: u32, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.pool_creations_prefix();
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k.push(b'/');
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_swaps_prefix(&self, address_spk: &[u8], pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_SWAPS.key().to_vec();
        push_spk(&mut k, address_spk);
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_swaps_key(
        &self,
        address_spk: &[u8],
        pool: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
    ) -> Vec<u8> {
        let mut k = self.address_pool_swaps_prefix(address_spk, pool);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    pub fn address_token_swaps_prefix(
        &self,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.ADDRESS_TOKEN_SWAPS.key().to_vec();
        push_spk(&mut k, address_spk);
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn address_token_swaps_key(
        &self,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_token_swaps_prefix(address_spk, token);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_creations_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_CREATIONS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_creations_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_creations_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_mints_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_MINTS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_mints_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_mints_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_burns_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_BURNS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_burns_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_burns_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_amm_history_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_AMM_HISTORY.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_amm_history_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        kind: ActivityKind,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_amm_history_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.push(activity_kind_code(kind));
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn amm_history_all_prefix(&self) -> Vec<u8> {
        self.AMM_HISTORY_ALL.key().to_vec()
    }

    pub fn amm_history_all_key(
        &self,
        ts: u64,
        seq: u32,
        kind: ActivityKind,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.amm_history_all_prefix();
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.push(activity_kind_code(kind));
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn token_pools_prefix(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.TOKEN_POOLS.key().to_vec();
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn token_pools_key(&self, token: &SchemaAlkaneId, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.token_pools_prefix(token);
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn pools_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOLS.select(&suffix).key().to_vec()
    }

    pub fn reserves_snapshot_pool_prefix(&self) -> Vec<u8> {
        self.RESERVES_SNAPSHOT.key().to_vec()
    }

    pub fn reserves_snapshot_pool_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.reserves_snapshot_pool_prefix();
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }
}

fn parse_ts_seq_from_key(bytes: &[u8]) -> Option<(u64, u32)> {
    let mut parts = bytes.splitn(2, |b| *b == b':');
    let ts_bytes = parts.next()?;
    let seq_bytes = parts.next()?;
    let ts = std::str::from_utf8(ts_bytes).ok()?.parse::<u64>().ok()?;
    let seq = std::str::from_utf8(seq_bytes).ok()?.parse::<u32>().ok()?;
    Some((ts, seq))
}

fn parse_ts_seq_from_tail_be(bytes: &[u8]) -> Option<(u64, u32)> {
    if bytes.len() < 12 {
        return None;
    }
    let ts_bytes = &bytes[bytes.len() - 12..bytes.len() - 4];
    let seq_bytes = &bytes[bytes.len() - 4..];
    let mut ts_arr = [0u8; 8];
    let mut seq_arr = [0u8; 4];
    ts_arr.copy_from_slice(ts_bytes);
    seq_arr.copy_from_slice(seq_bytes);
    Some((u64::from_be_bytes(ts_arr), u32::from_be_bytes(seq_arr)))
}

fn push_spk(dst: &mut Vec<u8>, spk: &[u8]) {
    let len = spk.len().min(u16::MAX as usize) as u16;
    dst.extend_from_slice(&len.to_be_bytes());
    dst.extend_from_slice(&spk[..len as usize]);
}

fn activity_kind_code(kind: ActivityKind) -> u8 {
    match kind {
        ActivityKind::TradeBuy => 0,
        ActivityKind::TradeSell => 1,
        ActivityKind::LiquidityAdd => 2,
        ActivityKind::LiquidityRemove => 3,
        ActivityKind::PoolCreate => 4,
    }
}

fn activity_kind_from_code(code: u8) -> Option<ActivityKind> {
    match code {
        0 => Some(ActivityKind::TradeBuy),
        1 => Some(ActivityKind::TradeSell),
        2 => Some(ActivityKind::LiquidityAdd),
        3 => Some(ActivityKind::LiquidityRemove),
        4 => Some(ActivityKind::PoolCreate),
        _ => None,
    }
}

fn read_address_pool_events(
    provider: &AmmDataProvider,
    prefix: Vec<u8>,
    offset: usize,
    limit: usize,
) -> Result<GetAddressPoolEventsPageResult> {
    let entries = match provider.get_list_entries_desc(GetListEntriesDescParams {
        blockhash: StateAt::Latest,
        prefix: prefix.clone(),
    }) {
        Ok(v) => v.entries,
        Err(_) => Vec::new(),
    };

    let mut parsed: Vec<AddressPoolEventEntry> = Vec::new();
    for (k, _v) in entries {
        if !k.starts_with(&prefix) {
            continue;
        }
        if k.len() < prefix.len() + 24 {
            continue;
        }
        let pool_bytes = &k[k.len() - 12..];
        let ts_seq_bytes = &k[k.len() - 24..k.len() - 12];
        let (ts, seq) = match parse_ts_seq_from_tail_be(ts_seq_bytes) {
            Some(v) => v,
            None => continue,
        };
        let pool = match decode_alkane_id_be(pool_bytes) {
            Some(p) => p,
            None => continue,
        };
        parsed.push(AddressPoolEventEntry { ts, seq, pool });
    }

    let total = parsed.len();
    let offset = offset.min(total);
    let end = (offset + limit).min(total);
    let page = if offset >= total { &[] } else { &parsed[offset..end] };

    Ok(GetAddressPoolEventsPageResult { entries: page.to_vec(), total })
}

fn read_amm_history(
    provider: &AmmDataProvider,
    prefix: Vec<u8>,
    offset: usize,
    limit: usize,
    kind_filter: Option<ActivityKind>,
) -> Result<GetAmmHistoryPageResult> {
    let entries = match provider.get_list_entries_desc(GetListEntriesDescParams {
        blockhash: StateAt::Latest,
        prefix: prefix.clone(),
    }) {
        Ok(v) => v.entries,
        Err(_) => Vec::new(),
    };

    let mut total = 0usize;
    let mut out = Vec::new();
    let mut seen = 0usize;
    for (k, _v) in entries {
        if !k.starts_with(&prefix) {
            continue;
        }
        let rest = &k[prefix.len()..];
        if rest.len() < 25 {
            continue;
        }
        let mut ts_arr = [0u8; 8];
        let mut seq_arr = [0u8; 4];
        ts_arr.copy_from_slice(&rest[0..8]);
        seq_arr.copy_from_slice(&rest[8..12]);
        let kind = match activity_kind_from_code(rest[12]) {
            Some(v) => v,
            None => continue,
        };
        if let Some(want) = kind_filter {
            if want != kind {
                continue;
            }
        }
        let pool = match decode_alkane_id_be(&rest[13..25]) {
            Some(p) => p,
            None => continue,
        };
        total += 1;
        if seen < offset {
            seen += 1;
            continue;
        }
        if out.len() < limit {
            out.push(AmmHistoryEntry {
                ts: u64::from_be_bytes(ts_arr),
                seq: u32::from_be_bytes(seq_arr),
                pool,
                kind,
            });
        }
    }

    Ok(GetAmmHistoryPageResult { entries: out, total })
}

#[derive(Clone)]
pub struct AmmDataProvider {
    mdb: Arc<Mdb>,
    essentials: Arc<EssentialsProvider>,
    view_blockhash: Option<BlockHash>,
}

impl AmmDataProvider {
    pub fn new(mdb: Arc<Mdb>, essentials: Arc<EssentialsProvider>) -> Self {
        Self { mdb, essentials, view_blockhash: None }
    }

    pub fn with_view_blockhash(&self, blockhash: Option<BlockHash>) -> Self {
        Self {
            mdb: Arc::clone(&self.mdb),
            essentials: Arc::new(self.essentials.with_view_blockhash(blockhash)),
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
        let Some(blockhash) = self
            .mdb
            .blockhash_for_height(height_u32)
            .map_err(|e| anyhow!("tree lookup failed: {e}"))?
        else {
            return Err(anyhow!("height_not_indexed"));
        };
        Ok(self.with_view_blockhash(Some(blockhash)))
    }

    pub fn table(&self) -> AmmDataTable<'_> {
        AmmDataTable::new(self.mdb.as_ref())
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb.as_ref()
    }

    /// Load the most recent BTC/USD price recorded by the ammdata indexer.
    /// This is intended for read-paths (RPC / API), so it never calls external price feeds.
    pub fn get_latest_btc_usd_price_entry(
        &self,
        params: GetLatestBtcUsdPriceParams,
    ) -> Result<Option<(u64, u128)>> {
        let table = self.table();
        let rel_prefix = table.btc_usd_price_prefix();
        let entries = self
            .get_list_entries_desc(GetListEntriesDescParams {
                blockhash: params.blockhash,
                prefix: rel_prefix,
            })
            .map(|resp| resp.entries)
            .unwrap_or_default();
        for (k, v) in entries {
            let Some(height) = table.parse_btc_usd_price_key(&k) else {
                continue;
            };
            if let Ok(price) = decode_u128_value(&v) {
                if price > 0 {
                    return Ok(Some((height, price)));
                }
            }
        }
        Ok(None)
    }

    /// Load the most recent BTC/USD price recorded by the ammdata indexer.
    /// This is intended for read-paths (RPC / API), so it never calls external price feeds.
    pub fn get_latest_btc_usd_price(
        &self,
        params: GetLatestBtcUsdPriceParams,
    ) -> Result<Option<u128>> {
        Ok(self.get_latest_btc_usd_price_entry(params)?.map(|(_, price)| price))
    }

    pub fn essentials(&self) -> &EssentialsProvider {
        self.essentials.as_ref()
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
        let page = self.get_list_entries_desc_cursor(GetListEntriesDescCursorParams {
            blockhash: params.blockhash,
            prefix: params.prefix,
            cursor: None,
            limit: usize::MAX,
        })?;
        Ok(GetListEntriesDescResult { entries: page.entries })
    }

    pub fn get_list_entries_desc_cursor(
        &self,
        params: GetListEntriesDescCursorParams,
    ) -> Result<GetListEntriesDescCursorResult> {
        let root = ListPointer::root(self.mdb.as_ref());
        let list = root.select(&params.prefix);
        let cursor_page: CursorScanPage = list.scan_desc_cursor_page(
            params.blockhash.resolve(self.view_blockhash).as_ref(),
            params.cursor.as_deref(),
            params.limit,
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
        let (all_puts, all_deletes) = dedupe_batch_ops(params.puts, params.deletes);

        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &all_deletes {
                    wb.delete(key);
                }
                for (key, value) in &all_puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
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

    pub fn get_reserves_snapshot(
        &self,
        _params: GetReservesSnapshotParams,
    ) -> Result<GetReservesSnapshotResult> {
        crate::debug_timer_log!("get_reserves_snapshot");
        let out = fetch_all_pools(self)?;
        if out.is_empty() {
            return Ok(GetReservesSnapshotResult { snapshot: None });
        }
        Ok(GetReservesSnapshotResult { snapshot: Some(out) })
    }

    pub fn set_reserves_snapshot(&self, params: SetReservesSnapshotParams) -> Result<()> {
        crate::debug_timer_log!("set_reserves_snapshot");
        let table = self.table();
        let mut puts = Vec::new();
        for (pool, snap) in &params.snapshot {
            if let Ok(enc) = encode_pool_snapshot(snap) {
                puts.push((table.reserves_snapshot_pool_key(pool), enc));
            }
        }
        self.set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes: Vec::new() })
    }

    pub fn get_canonical_pools(
        &self,
        params: GetCanonicalPoolsParams,
    ) -> Result<GetCanonicalPoolsResult> {
        crate::debug_timer_log!("get_canonical_pools");
        let table = self.table();
        let prefix = table.canonical_pool_token_prefix(&params.token);
        let keys = self
            .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|v| v.keys)
            .unwrap_or_default();
        if keys.is_empty() {
            return Ok(GetCanonicalPoolsResult { pools: Vec::new() });
        }
        let values = self
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: keys.clone(),
            })
            .map(|v| v.values)
            .unwrap_or_default();

        let mut pools: Vec<SchemaCanonicalPoolEntry> = Vec::new();
        for (key, value) in keys.into_iter().zip(values.into_iter()) {
            if !key.starts_with(&prefix) {
                continue;
            }
            let Some(quote_id) = decode_alkane_id_be(&key[prefix.len()..]) else {
                continue;
            };
            let Some(raw) = value else {
                continue;
            };
            let Some(pool_id) = decode_alkane_id_be(&raw) else {
                continue;
            };
            pools.push(SchemaCanonicalPoolEntry { pool_id, quote_id });
        }
        pools.sort_by_key(|e| (e.quote_id.block, e.quote_id.tx, e.pool_id.block, e.pool_id.tx));
        Ok(GetCanonicalPoolsResult { pools })
    }

    pub fn get_latest_token_usd_close(
        &self,
        params: GetLatestTokenUsdCloseParams,
    ) -> Result<GetLatestTokenUsdCloseResult> {
        crate::debug_timer_log!("get_latest_token_usd_close");
        let table = self.table();
        let prefix = table.token_usd_candle_ns_prefix(&params.token, params.timeframe);
        let entries = self
            .get_list_entries_desc(GetListEntriesDescParams { blockhash: StateAt::Latest, prefix })
            .ok()
            .map(|resp| resp.entries)
            .unwrap_or_default();
        let close = entries
            .into_iter()
            .next()
            .and_then(|(_k, v)| decode_candle_v1(&v).ok())
            .map(|c| c.close);
        Ok(GetLatestTokenUsdCloseResult { close })
    }

    pub fn get_token_metrics(
        &self,
        params: GetTokenMetricsParams,
    ) -> Result<GetTokenMetricsResult> {
        crate::debug_timer_log!("get_token_metrics");
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.token_metrics_key(&params.token),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_token_metrics(&raw).ok())
            .unwrap_or_default();
        Ok(GetTokenMetricsResult { metrics })
    }

    pub fn get_token_derived_metrics(
        &self,
        params: GetTokenDerivedMetricsParams,
    ) -> Result<GetTokenDerivedMetricsResult> {
        crate::debug_timer_log!("get_token_derived_metrics");
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.token_derived_metrics_key(&params.token, &params.quote),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_token_metrics(&raw).ok());
        Ok(GetTokenDerivedMetricsResult { metrics })
    }

    pub fn get_token_metrics_by_id(
        &self,
        params: GetTokenMetricsByIdParams,
    ) -> Result<GetTokenMetricsByIdResult> {
        crate::debug_timer_log!("get_token_metrics_by_id");
        let table = self.table();
        let keys: Vec<Vec<u8>> =
            params.tokens.iter().map(|token| table.token_metrics_key(token)).collect();
        let values = self.raw_multi_get(&keys)?;
        let mut metrics = Vec::with_capacity(values.len());
        for val in values {
            if let Some(bytes) = val {
                metrics.push(decode_token_metrics(&bytes).ok());
            } else {
                metrics.push(None);
            }
        }
        Ok(GetTokenMetricsByIdResult { metrics })
    }

    pub fn get_token_derived_metrics_by_id(
        &self,
        params: GetTokenDerivedMetricsByIdParams,
    ) -> Result<GetTokenDerivedMetricsByIdResult> {
        crate::debug_timer_log!("get_token_derived_metrics_by_id");
        let table = self.table();
        let keys: Vec<Vec<u8>> = params
            .tokens
            .iter()
            .map(|token| table.token_derived_metrics_key(token, &params.quote))
            .collect();
        let values = self.raw_multi_get(&keys)?;
        let mut metrics = Vec::with_capacity(values.len());
        for val in values {
            if let Some(bytes) = val {
                metrics.push(decode_token_metrics(&bytes).ok());
            } else {
                metrics.push(None);
            }
        }
        Ok(GetTokenDerivedMetricsByIdResult { metrics })
    }

    pub fn get_token_metrics_index_page(
        &self,
        params: GetTokenMetricsIndexPageParams,
    ) -> Result<GetTokenMetricsIndexPageResult> {
        crate::debug_timer_log!("get_token_metrics_index_page");
        let table = self.table();
        let prefix = table.token_metrics_index_prefix(params.field);
        let keys = if params.desc {
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
        let mut out: Vec<SchemaAlkaneId> = Vec::new();
        let mut skipped: u64 = 0;
        for key in keys {
            let Some(id) = table.parse_token_metrics_index_key(params.field, &key) else {
                continue;
            };
            if skipped < params.offset {
                skipped += 1;
                continue;
            }
            out.push(id);
            if out.len() >= params.limit as usize {
                break;
            }
        }

        Ok(GetTokenMetricsIndexPageResult { ids: out })
    }

    pub fn get_token_derived_metrics_index_page(
        &self,
        params: GetTokenDerivedMetricsIndexPageParams,
    ) -> Result<GetTokenDerivedMetricsIndexPageResult> {
        crate::debug_timer_log!("get_token_derived_metrics_index_page");
        let table = self.table();
        let prefix = table.token_derived_metrics_index_prefix(&params.quote, params.field);
        let keys = if params.desc {
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
        let mut out: Vec<SchemaAlkaneId> = Vec::new();
        let mut skipped: u64 = 0;
        for key in keys {
            let Some(id) =
                table.parse_token_derived_metrics_index_key(&params.quote, params.field, &key)
            else {
                continue;
            };
            if skipped < params.offset {
                skipped += 1;
                continue;
            }
            out.push(id);
            if out.len() >= params.limit as usize {
                break;
            }
        }

        Ok(GetTokenDerivedMetricsIndexPageResult { ids: out })
    }

    pub fn get_token_metrics_index_count(
        &self,
        _params: GetTokenMetricsIndexCountParams,
    ) -> Result<GetTokenMetricsIndexCountResult> {
        crate::debug_timer_log!("get_token_metrics_index_count");
        let table = self.table();
        let count = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.token_metrics_index_count_key(),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| {
                if raw.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&raw);
                    Some(u64::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        Ok(GetTokenMetricsIndexCountResult { count })
    }

    pub fn get_token_derived_metrics_index_count(
        &self,
        params: GetTokenDerivedMetricsIndexCountParams,
    ) -> Result<GetTokenDerivedMetricsIndexCountResult> {
        crate::debug_timer_log!("get_token_derived_metrics_index_count");
        let table = self.table();
        let count = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.token_derived_metrics_index_count_key(&params.quote),
            })?
            .value
            .and_then(|raw| {
                if raw.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&raw);
                    Some(u64::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        Ok(GetTokenDerivedMetricsIndexCountResult { count })
    }

    pub fn get_token_search_index_page(
        &self,
        params: GetTokenSearchIndexPageParams,
    ) -> Result<GetTokenSearchIndexPageResult> {
        crate::debug_timer_log!("get_token_search_index_page");
        let table = self.table();
        let prefix = table.token_search_index_prefix(params.field, &params.prefix);
        let keys = if params.desc {
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
        let mut out: Vec<SchemaAlkaneId> = Vec::new();
        let mut skipped: u64 = 0;
        for key in keys {
            let Some(id) = table.parse_token_search_index_key(params.field, &key) else {
                continue;
            };
            if skipped < params.offset {
                skipped += 1;
                continue;
            }
            out.push(id);
            if out.len() >= params.limit as usize {
                break;
            }
        }

        Ok(GetTokenSearchIndexPageResult { ids: out })
    }

    pub fn get_token_derived_search_index_page(
        &self,
        params: GetTokenDerivedSearchIndexPageParams,
    ) -> Result<GetTokenDerivedSearchIndexPageResult> {
        crate::debug_timer_log!("get_token_derived_search_index_page");
        let table = self.table();
        let prefix =
            table.token_derived_search_index_prefix(&params.quote, params.field, &params.prefix);
        let keys = if params.desc {
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
        let mut out: Vec<SchemaAlkaneId> = Vec::new();
        let mut skipped: u64 = 0;
        for key in keys {
            let Some(id) =
                table.parse_token_derived_search_index_key(&params.quote, params.field, &key)
            else {
                continue;
            };
            if skipped < params.offset {
                skipped += 1;
                continue;
            }
            out.push(id);
            if out.len() >= params.limit as usize {
                break;
            }
        }

        Ok(GetTokenDerivedSearchIndexPageResult { ids: out })
    }

    pub fn get_pool_ids_by_name_prefix(
        &self,
        params: GetPoolIdsByNamePrefixParams,
    ) -> Result<GetPoolIdsByNamePrefixResult> {
        crate::debug_timer_log!("get_pool_ids_by_name_prefix");
        let table = self.table();
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: table.pool_name_index_prefix(&params.prefix),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for key in keys {
            if let Some((_name, id)) = table.parse_pool_name_index_key(&key) {
                if seen.insert(id) {
                    ids.push(id);
                }
            }
        }
        Ok(GetPoolIdsByNamePrefixResult { ids })
    }

    pub fn get_amm_factories(
        &self,
        _params: GetAmmFactoriesParams,
    ) -> Result<GetAmmFactoriesResult> {
        crate::debug_timer_log!("get_amm_factories");
        let table = self.table();
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: table.AMM_FACTORIES.key().to_vec(),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        for key in keys {
            if let Some(id) = parse_alkane_id_from_prefixed_key(table.AMM_FACTORIES.key(), &key) {
                ids.push(id);
            }
        }
        Ok(GetAmmFactoriesResult { factories: ids })
    }

    pub fn get_factory_pools(
        &self,
        params: GetFactoryPoolsParams,
    ) -> Result<GetFactoryPoolsResult> {
        crate::debug_timer_log!("get_factory_pools");
        let table = self.table();
        let prefix = table.factory_pools_prefix(&params.factory);
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut pools = Vec::new();
        for key in keys {
            if let Some(id) = parse_alkane_id_from_prefixed_key(prefix.as_slice(), &key) {
                pools.push(id);
            }
        }
        Ok(GetFactoryPoolsResult { pools })
    }

    pub fn get_pool_defs(&self, params: GetPoolDefsParams) -> Result<GetPoolDefsResult> {
        crate::debug_timer_log!("get_pool_defs");
        let table = self.table();
        let defs = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pools_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| SchemaMarketDefs::try_from_slice(&raw).ok());
        Ok(GetPoolDefsResult { defs })
    }

    pub fn get_pool_factory(&self, params: GetPoolFactoryParams) -> Result<GetPoolFactoryResult> {
        crate::debug_timer_log!("get_pool_factory");
        let table = self.table();
        let factory = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pool_factory_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_alkane_id_be(&raw));
        Ok(GetPoolFactoryResult { factory })
    }

    pub fn get_pool_metrics(&self, params: GetPoolMetricsParams) -> Result<GetPoolMetricsResult> {
        crate::debug_timer_log!("get_pool_metrics");
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pool_metrics_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_pool_metrics(&raw).ok())
            .unwrap_or_default();
        Ok(GetPoolMetricsResult { metrics })
    }

    pub fn get_pool_metrics_v2(
        &self,
        params: GetPoolMetricsV2Params,
    ) -> Result<GetPoolMetricsV2Result> {
        crate::debug_timer_log!("get_pool_metrics_v2");
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pool_metrics_v2_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_pool_metrics_v2(&raw).ok());
        Ok(GetPoolMetricsV2Result { metrics })
    }

    pub fn get_pool_creation_info(
        &self,
        params: GetPoolCreationInfoParams,
    ) -> Result<GetPoolCreationInfoResult> {
        crate::debug_timer_log!("get_pool_creation_info");
        let table = self.table();
        let info = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pool_creation_info_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_pool_creation_info(&raw).ok());
        Ok(GetPoolCreationInfoResult { info })
    }

    pub fn get_pool_lp_supply_latest(
        &self,
        params: GetPoolLpSupplyLatestParams,
    ) -> Result<GetPoolLpSupplyLatestResult> {
        crate::debug_timer_log!("get_pool_lp_supply_latest");
        let table = self.table();
        let supply = self
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: table.pool_lp_supply_latest_key(&params.pool),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_u128_value(&raw).ok())
            .unwrap_or(0);
        Ok(GetPoolLpSupplyLatestResult { supply })
    }

    pub fn get_pool_details_snapshot(
        &self,
        params: GetPoolDetailsSnapshotParams,
    ) -> Result<GetPoolDetailsSnapshotResult> {
        crate::debug_timer_log!("get_pool_details_snapshot");
        let table = self.table();
        let keys = vec![
            table.pool_details_snapshot_field_key(&params.pool, "token0_tvl_usd"),
            table.pool_details_snapshot_field_key(&params.pool, "token1_tvl_usd"),
            table.pool_details_snapshot_field_key(&params.pool, "token0_tvl_sats"),
            table.pool_details_snapshot_field_key(&params.pool, "token1_tvl_sats"),
            table.pool_details_snapshot_field_key(&params.pool, "pool_tvl_usd"),
            table.pool_details_snapshot_field_key(&params.pool, "pool_volume_1d_usd"),
            table.pool_details_snapshot_field_key(&params.pool, "pool_volume_30d_usd"),
            table.pool_details_snapshot_field_key(&params.pool, "pool_apr"),
            table.pool_details_snapshot_field_key(&params.pool, "tvl_change_24h"),
            table.pool_details_snapshot_field_key(&params.pool, "lp_supply"),
        ];
        let values = self
            .get_multi_values(GetMultiValuesParams {
                blockhash: StateAt::Latest,
                keys: keys.clone(),
            })
            .ok()
            .map(|resp| resp.values)
            .unwrap_or_default();
        let snapshot = if values.len() == keys.len() && values.iter().all(|v| v.is_some()) {
            let get_u128 = |idx: usize| -> u128 {
                values
                    .get(idx)
                    .and_then(|v| v.as_ref())
                    .and_then(|raw| decode_u128_value(raw).ok())
                    .unwrap_or(0)
            };
            let get_f64 = |idx: usize| -> f64 {
                values
                    .get(idx)
                    .and_then(|v| v.as_ref())
                    .and_then(|raw| decode_f64_value(raw).ok())
                    .unwrap_or(0.0)
            };
            Some(SchemaPoolDetailsSnapshot {
                value_json: Vec::new(),
                token0_tvl_usd: get_u128(0),
                token1_tvl_usd: get_u128(1),
                token0_tvl_sats: get_u128(2),
                token1_tvl_sats: get_u128(3),
                pool_tvl_usd: get_u128(4),
                pool_volume_1d_usd: get_u128(5),
                pool_volume_30d_usd: get_u128(6),
                pool_apr: get_f64(7),
                tvl_change_24h: get_f64(8),
                lp_supply: get_u128(9),
            })
        } else {
            None
        };
        Ok(GetPoolDetailsSnapshotResult { snapshot })
    }

    pub fn get_pool_activity_entries(
        &self,
        params: GetPoolActivityEntriesParams,
    ) -> Result<GetPoolActivityEntriesResult> {
        crate::debug_timer_log!("get_pool_activity_entries");
        let table = self.table();
        let prefix = table.activity_ns_prefix(&params.pool);
        let entries = match self
            .get_list_entries_desc(GetListEntriesDescParams { blockhash: StateAt::Latest, prefix })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut total = 0usize;
        let mut out: Vec<SchemaActivityV1> = Vec::new();
        let mut seen = 0usize;
        for (_k, v) in entries {
            let entry = match decode_activity_v1(&v) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Some(ref kinds) = params.kinds {
                if !kinds.contains(&entry.kind) {
                    continue;
                }
            }
            if let Some(want) = params.successful {
                if want && !entry.success {
                    continue;
                }
            }
            total += 1;
            if seen < params.offset {
                seen += 1;
                continue;
            }
            if out.len() < params.limit {
                out.push(entry);
            }
            if out.len() >= params.limit
                && params.offset + out.len() >= total
                && !params.include_total
            {
                break;
            }
        }

        Ok(GetPoolActivityEntriesResult { entries: out, total })
    }

    pub fn get_activity_entry(
        &self,
        params: GetActivityEntryParams,
    ) -> Result<GetActivityEntryResult> {
        crate::debug_timer_log!("get_activity_entry");
        let table = self.table();
        let key = table.activity_key(&params.pool, params.ts, params.seq);
        let entry = self
            .get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?
            .value
            .and_then(|raw| decode_activity_v1(&raw).ok());
        Ok(GetActivityEntryResult { entry })
    }

    pub fn get_token_swaps_page(
        &self,
        params: GetTokenSwapsPageParams,
    ) -> Result<GetTokenSwapsPageResult> {
        crate::debug_timer_log!("get_token_swaps_page");
        let table = self.table();
        let prefix = table.token_swaps_prefix(&params.token);
        let entries = match self.get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<TokenSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let mut parts = rest.splitn(2, |b| *b == b'/');
            let ts_seq = parts.next().unwrap_or(&[]);
            let pool_bytes = parts.next().unwrap_or(&[]);
            let (ts, seq) = match parse_ts_seq_from_key(ts_seq) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(TokenSwapEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetTokenSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_pool_creations_page(
        &self,
        params: GetPoolCreationsPageParams,
    ) -> Result<GetPoolCreationsPageResult> {
        crate::debug_timer_log!("get_pool_creations_page");
        let table = self.table();
        let prefix = table.pool_creations_prefix();
        let entries = match self.get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<PoolCreationEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let mut parts = rest.splitn(2, |b| *b == b'/');
            let ts_seq = parts.next().unwrap_or(&[]);
            let pool_bytes = parts.next().unwrap_or(&[]);
            let (ts, seq) = match parse_ts_seq_from_key(ts_seq) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(PoolCreationEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetPoolCreationsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_pool_swaps_page(
        &self,
        params: GetAddressPoolSwapsPageParams,
    ) -> Result<GetAddressPoolSwapsPageResult> {
        crate::debug_timer_log!("get_address_pool_swaps_page");
        let table = self.table();
        let prefix = table.address_pool_swaps_prefix(&params.address_spk, &params.pool);
        let entries = match self.get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<AddressPoolSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let (ts, seq) = match parse_ts_seq_from_tail_be(rest) {
                Some(v) => v,
                None => continue,
            };
            parsed.push(AddressPoolSwapEntry { ts, seq });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetAddressPoolSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_token_swaps_page(
        &self,
        params: GetAddressTokenSwapsPageParams,
    ) -> Result<GetAddressTokenSwapsPageResult> {
        crate::debug_timer_log!("get_address_token_swaps_page");
        let table = self.table();
        let prefix = table.address_token_swaps_prefix(&params.address_spk, &params.token);
        let entries = match self.get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<AddressTokenSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            if k.len() < prefix.len() + 24 {
                continue;
            }
            let pool_bytes = &k[k.len() - 12..];
            let ts_seq_bytes = &k[k.len() - 24..k.len() - 12];
            let (ts, seq) = match parse_ts_seq_from_tail_be(ts_seq_bytes) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(AddressTokenSwapEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetAddressTokenSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_pool_creations_page(
        &self,
        params: GetAddressPoolCreationsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        crate::debug_timer_log!("get_address_pool_creations_page");
        let table = self.table();
        let prefix = table.address_pool_creations_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_address_pool_mints_page(
        &self,
        params: GetAddressPoolMintsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        crate::debug_timer_log!("get_address_pool_mints_page");
        let table = self.table();
        let prefix = table.address_pool_mints_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_address_pool_burns_page(
        &self,
        params: GetAddressPoolBurnsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        crate::debug_timer_log!("get_address_pool_burns_page");
        let table = self.table();
        let prefix = table.address_pool_burns_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_address_amm_history_page(
        &self,
        params: GetAddressAmmHistoryPageParams,
    ) -> Result<GetAmmHistoryPageResult> {
        crate::debug_timer_log!("get_address_amm_history_page");
        let table = self.table();
        let prefix = table.address_amm_history_prefix(&params.address_spk);
        read_amm_history(self, prefix, params.offset, params.limit, params.kind)
    }

    pub fn get_amm_history_all_page(
        &self,
        params: GetAmmHistoryAllPageParams,
    ) -> Result<GetAmmHistoryPageResult> {
        crate::debug_timer_log!("get_amm_history_all_page");
        let table = self.table();
        let prefix = table.amm_history_all_prefix();
        read_amm_history(self, prefix, params.offset, params.limit, params.kind)
    }

    pub fn get_token_pools(&self, params: GetTokenPoolsParams) -> Result<GetTokenPoolsResult> {
        crate::debug_timer_log!("get_token_pools");
        let table = self.table();
        let prefix = table.token_pools_prefix(&params.token);
        let keys = match self.get_list_keys_by_prefix(GetListKeysByPrefixParams {
            blockhash: StateAt::Latest,
            prefix: prefix.clone(),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut pools = Vec::new();
        for key in keys {
            if !key.starts_with(&prefix) {
                continue;
            }
            let rest = &key[prefix.len()..];
            if let Some(pool) = decode_alkane_id_be(rest) {
                pools.push(pool);
            }
        }
        Ok(GetTokenPoolsResult { pools })
    }

    pub fn get_tvl_versioned_at_or_before(
        &self,
        params: GetTvlVersionedAtOrBeforeParams,
    ) -> Result<GetTvlVersionedAtOrBeforeResult> {
        crate::debug_timer_log!("get_tvl_versioned_at_or_before");
        let table = self.table();
        let prefix = table.tvl_versioned_prefix(&params.pool);
        let entries = match self
            .get_list_entries_desc(GetListEntriesDescParams { blockhash: StateAt::Latest, prefix })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };
        let mut value = None;
        for (k, v) in entries {
            let height = parse_height_from_tvl_key(&k)?;
            if height <= params.height {
                value = decode_u128_value(&v).ok();
                break;
            }
        }
        Ok(GetTvlVersionedAtOrBeforeResult { value })
    }

    pub fn get_canonical_pool_prices(
        &self,
        params: GetCanonicalPoolPricesParams,
    ) -> Result<GetCanonicalPoolPricesResult> {
        crate::debug_timer_log!("get_canonical_pool_prices");
        let mut frbtc_price = 0u128;
        let mut busd_price = 0u128;
        let mut unit_map: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for cq in canonical_quotes(get_network()) {
            unit_map.insert(cq.id, cq.unit);
        }
        let pools = self
            .get_canonical_pools(GetCanonicalPoolsParams {
                blockhash: StateAt::Latest,
                token: params.token,
            })?
            .pools;
        for entry in pools {
            let unit = match unit_map.get(&entry.quote_id) {
                Some(u) => *u,
                None => continue,
            };
            let defs = match self.get_pool_defs(GetPoolDefsParams {
                blockhash: StateAt::Latest,
                pool: entry.pool_id,
            }) {
                Ok(res) => res.defs,
                Err(_) => None,
            };
            let Some(defs) = defs else { continue };
            let side = if defs.base_alkane_id == params.token
                && defs.quote_alkane_id == entry.quote_id
            {
                PriceSide::Base
            } else if defs.quote_alkane_id == params.token && defs.base_alkane_id == entry.quote_id
            {
                PriceSide::Quote
            } else {
                continue;
            };
            let res =
                read_candles_v1(self, entry.pool_id, Timeframe::M10, 1, params.now_ts, side).ok();
            let close = res
                .and_then(|slice| slice.candles_newest_first.first().copied())
                .map(|c| c.close)
                .unwrap_or(0);
            match unit {
                CanonicalQuoteUnit::Btc => frbtc_price = close,
                CanonicalQuoteUnit::Usd => busd_price = close,
            }
        }
        Ok(GetCanonicalPoolPricesResult { frbtc_price, busd_price })
    }

    pub fn rpc_get_candles(&self, params: RpcGetCandlesParams) -> Result<RpcGetCandlesResult> {
        let tf = params.timeframe.as_deref().and_then(parse_timeframe).unwrap_or(Timeframe::H1);

        let legacy_size = params.size.map(|n| n as usize);
        let limit = params.limit.map(|n| n as usize).or(legacy_size).unwrap_or(120);
        let page = params.page.map(|n| n as usize).unwrap_or(1);

        let side = params.side.as_deref().and_then(parse_price_side).unwrap_or(PriceSide::Base);

        let now = params.now.unwrap_or_else(now_ts);

        let pool_raw = match params.pool.as_deref() {
            Some(p) => p,
            None => {
                return Ok(RpcGetCandlesResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_or_invalid_pool",
                        "hint": "pool should be a string like \"2:68441\" or \"2:0-usd\""
                    }),
                });
            }
        };

        let parse_token_or_derived =
            |raw: &str| -> Result<(SchemaAlkaneId, Option<SchemaAlkaneId>)> {
                if let Some((token_part, quote_part)) = raw.split_once("-derived_") {
                    match (parse_id_from_str(token_part), parse_id_from_str(quote_part)) {
                        (Some(p), Some(q)) => Ok((p, Some(q))),
                        _ => Err(anyhow!("invalid_pool")),
                    }
                } else {
                    match parse_id_from_str(raw) {
                        Some(p) => Ok((p, None)),
                        None => Err(anyhow!("invalid_pool")),
                    }
                }
            };

        let mut derived_quote: Option<SchemaAlkaneId> = None;
        let mut is_usd = false;
        let mut is_mcusd = false;
        let mut is_sats = false;
        let mut is_mcsats = false;

        let pool = if let Some(stripped) = pool_raw.strip_suffix("-mcsats") {
            match parse_token_or_derived(stripped) {
                Ok((p, q)) => {
                    derived_quote = q;
                    is_mcsats = true;
                    is_mcusd = true;
                    p
                }
                Err(_) => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\", \"2:0-usd\", \"2:0-sats\", \"2:0-mcusd\", \"2:0-mcsats\", or \"2:0-derived_2:1-usd\""
                        }),
                    });
                }
            }
        } else if let Some(stripped) = pool_raw.strip_suffix("-sats") {
            match parse_token_or_derived(stripped) {
                Ok((p, q)) => {
                    derived_quote = q;
                    is_sats = true;
                    is_usd = true;
                    p
                }
                Err(_) => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\", \"2:0-usd\", \"2:0-sats\", \"2:0-mcusd\", \"2:0-mcsats\", or \"2:0-derived_2:1-usd\""
                        }),
                    });
                }
            }
        } else if let Some(stripped) = pool_raw.strip_suffix("-mcusd") {
            match parse_token_or_derived(stripped) {
                Ok((p, q)) => {
                    derived_quote = q;
                    is_mcusd = true;
                    p
                }
                Err(_) => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\", \"2:0-usd\", \"2:0-sats\", \"2:0-mcusd\", \"2:0-mcsats\", or \"2:0-derived_2:1-usd\""
                        }),
                    });
                }
            }
        } else if let Some(stripped) = pool_raw.strip_suffix("-usd") {
            match parse_token_or_derived(stripped) {
                Ok((p, q)) => {
                    derived_quote = q;
                    is_usd = true;
                    p
                }
                Err(_) => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\", \"2:0-usd\", \"2:0-sats\", \"2:0-mcusd\", \"2:0-mcsats\", or \"2:0-derived_2:1-usd\""
                        }),
                    });
                }
            }
        } else {
            match parse_id_from_str(pool_raw) {
                Some(p) => p,
                None => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\", \"2:0-usd\", \"2:0-sats\", \"2:0-mcusd\", \"2:0-mcsats\", or \"2:0-derived_2:1-usd\""
                        }),
                    });
                }
            }
        };

        let slice = if is_mcusd {
            if let Some(quote) = derived_quote {
                read_token_derived_mcusd_candles_v1(self, pool, quote, tf, now)
            } else {
                read_token_mcusd_candles_v1(self, pool, tf, now)
            }
        } else if is_usd {
            if let Some(quote) = derived_quote {
                read_token_derived_usd_candles_v1(self, pool, quote, tf, now)
            } else {
                read_token_usd_candles_v1(self, pool, tf, now)
            }
        } else {
            read_candles_v1(self, pool, tf, /*unused*/ limit, now, side)
        };

        match slice {
            Ok(slice) => {
                let total = slice.candles_newest_first.len();
                let dur = tf.duration_secs();
                let newest_ts = slice.newest_ts;

                // Low-risk chart fix: for token-derived USD/MCUSD series, force the newest
                // (active/unresolved) candle `close` to match the current spot close from the 10m
                // series. This avoids cross-timeframe close drift without changing indexing.
                let spot_close_10m: Option<u128> = if tf != Timeframe::M10 && (is_usd || is_mcusd) {
                    let spot_tf = Timeframe::M10;
                    let spot_slice = if is_mcusd {
                        if let Some(quote) = derived_quote {
                            read_token_derived_mcusd_candles_v1(self, pool, quote, spot_tf, now)
                        } else {
                            read_token_mcusd_candles_v1(self, pool, spot_tf, now)
                        }
                    } else {
                        if let Some(quote) = derived_quote {
                            read_token_derived_usd_candles_v1(self, pool, quote, spot_tf, now)
                        } else {
                            read_token_usd_candles_v1(self, pool, spot_tf, now)
                        }
                    };
                    spot_slice
                        .ok()
                        .and_then(|slice| slice.candles_newest_first.first().copied())
                        .map(|c| c.close)
                } else {
                    None
                };

                let offset = limit.saturating_mul(page.saturating_sub(1));
                let end = (offset + limit).min(total);
                let page_slice = if offset >= total {
                    &[][..]
                } else {
                    &slice.candles_newest_first[offset..end]
                };

                let btc_slice = if is_sats || is_mcsats {
                    read_btc_usd_line_v1(self, tf, now).ok()
                } else {
                    None
                };

                let btc_price_for_ts = |slice: &CandleSlice, ts: u64, dur: u64| -> u128 {
                    if slice.candles_newest_first.is_empty() {
                        return 0;
                    }
                    let newest_ts = slice.newest_ts;
                    if ts > newest_ts {
                        return slice.candles_newest_first[0].close;
                    }
                    let offset = ((newest_ts.saturating_sub(ts)) / dur) as usize;
                    if offset < slice.candles_newest_first.len() {
                        slice.candles_newest_first[offset].close
                    } else {
                        slice.candles_newest_first.last().map(|c| c.close).unwrap_or(0)
                    }
                };

                let usd_to_sats_scaled = |usd_scaled: u128, btc_usd_scaled: u128| -> u128 {
                    if btc_usd_scaled == 0 {
                        return 0;
                    }
                    let usd_sats = usd_scaled.saturating_mul(SATS_PER_BTC);
                    let whole = usd_sats / btc_usd_scaled;
                    let rem = usd_sats % btc_usd_scaled;
                    whole
                        .saturating_mul(PRICE_SCALE)
                        .saturating_add(rem.saturating_mul(PRICE_SCALE) / btc_usd_scaled)
                };

                let arr: Vec<Value> = page_slice
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let global_idx = offset + i;
                        let ts = newest_ts.saturating_sub((global_idx as u64) * dur);
                        let mut candle = *c;
                        if global_idx == 0 {
                            if let Some(spot_close) = spot_close_10m {
                                candle.close = spot_close;
                                candle.high = candle.high.max(candle.open).max(candle.close);
                                candle.low = candle.low.min(candle.open).min(candle.close);
                            }
                        }
                        if is_sats || is_mcsats {
                            let btc_price = btc_slice
                                .as_ref()
                                .map(|slice| btc_price_for_ts(slice, ts, dur))
                                .unwrap_or(0);
                            candle.high = usd_to_sats_scaled(candle.high, btc_price);
                            candle.low = usd_to_sats_scaled(candle.low, btc_price);
                            candle.close = usd_to_sats_scaled(candle.close, btc_price);
                            candle.volume = usd_to_sats_scaled(candle.volume, btc_price);
                            candle.open = if global_idx + 1 < total {
                                let prev_ts =
                                    newest_ts.saturating_sub(((global_idx + 1) as u64) * dur);
                                let prev_close_usd =
                                    slice.candles_newest_first[global_idx + 1].close;
                                let prev_btc_price = btc_slice
                                    .as_ref()
                                    .map(|slice| btc_price_for_ts(slice, prev_ts, dur))
                                    .unwrap_or(0);
                                usd_to_sats_scaled(prev_close_usd, prev_btc_price)
                            } else {
                                usd_to_sats_scaled(candle.open, btc_price)
                            };
                            if candle.open > candle.high {
                                candle.high = candle.open;
                            }
                            if candle.open < candle.low {
                                candle.low = candle.open;
                            }
                        }
                        json!({
                            "ts":     ts,
                            "open":   candle.open.to_string(),
                            "high":   candle.high.to_string(),
                            "low":    candle.low.to_string(),
                            "close":  candle.close.to_string(),
                            "volume": candle.volume.to_string(),
                        })
                    })
                    .collect();

                let pool_label = if is_usd || is_mcusd {
                    if let Some(q) = derived_quote {
                        let suffix = if is_mcsats {
                            "mcsats"
                        } else if is_sats {
                            "sats"
                        } else if is_mcusd {
                            "mcusd"
                        } else {
                            "usd"
                        };
                        format!("{}-derived_{}-{}", id_str(&pool), id_str(&q), suffix)
                    } else if is_mcsats {
                        format!("{}-mcsats", id_str(&pool))
                    } else if is_sats {
                        format!("{}-sats", id_str(&pool))
                    } else if is_mcusd {
                        format!("{}-mcusd", id_str(&pool))
                    } else {
                        format!("{}-usd", id_str(&pool))
                    }
                } else {
                    id_str(&pool)
                };

                Ok(RpcGetCandlesResult {
                    value: json!({
                        "ok": true,
                        "pool": pool_label,
                        "timeframe": tf.code(),
                        "side": if is_usd || is_mcusd {
                            "base"
                        } else {
                            match side {
                                PriceSide::Base => "base",
                                PriceSide::Quote => "quote",
                            }
                        },
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": end < total,
                        "candles": arr
                    }),
                })
            }
            Err(e) => Ok(RpcGetCandlesResult {
                value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
            }),
        }
    }

    pub fn rpc_get_chart_change_block(
        &self,
        params: RpcGetChartChangeBlockParams,
    ) -> Result<RpcGetChartChangeBlockResult> {
        let Some(chart) = params.chart.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(RpcGetChartChangeBlockResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_chart"
                }),
            });
        };
        let height = if let Some(h) = params.height {
            h
        } else {
            self.get_index_height(GetIndexHeightParams { blockhash: StateAt::Latest })?
                .height
                .map(|v| v as u64)
                .unwrap_or(0)
        };
        if height == 0 {
            return Ok(RpcGetChartChangeBlockResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_height"
                }),
            });
        }
        let table = self.table();
        let key = table.chart_change_event_key(height, chart);
        let raw = self.get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?;
        let Some(bytes) = raw.value else {
            return Ok(RpcGetChartChangeBlockResult {
                value: json!({
                    "ok": true,
                    "available": false,
                    "chart": chart,
                    "height": height,
                    "changes": {}
                }),
            });
        };
        let decoded = match decode_chart_change_set_v1(&bytes) {
            Ok(v) => v,
            Err(_) => {
                return Ok(RpcGetChartChangeBlockResult {
                    value: json!({
                        "ok": false,
                        "error": "decode_failed"
                    }),
                });
            }
        };

        let mut changes = Map::new();
        for (tf, v) in decoded.timeframe_changes {
            changes.insert(
                tf,
                json!({
                    "usd": format_change_basis_points(v.usd_bp),
                    "usd_bp": v.usd_bp,
                    "mcusd": format_change_basis_points(v.mcusd_bp),
                    "mcusd_bp": v.mcusd_bp
                }),
            );
        }

        Ok(RpcGetChartChangeBlockResult {
            value: json!({
                "ok": true,
                "available": true,
                "chart": chart,
                "height": height,
                "changes": changes
            }),
        })
    }

    pub fn rpc_get_chart_changes_block(
        &self,
        params: RpcGetChartChangesBlockParams,
    ) -> Result<RpcGetChartChangesBlockResult> {
        let height = if let Some(h) = params.height {
            h
        } else {
            self.get_index_height(GetIndexHeightParams { blockhash: StateAt::Latest })?
                .height
                .map(|v| v as u64)
                .unwrap_or(0)
        };
        if height == 0 {
            return Ok(RpcGetChartChangesBlockResult {
                value: json!({
                    "ok": false,
                    "error": "missing_or_invalid_height"
                }),
            });
        }
        let table = self.table();
        let prefix = table.chart_change_event_height_prefix(height);
        let entries = self
            .get_list_entries_desc(GetListEntriesDescParams {
                blockhash: StateAt::Latest,
                prefix: prefix.clone(),
            })
            .map(|resp| resp.entries)
            .unwrap_or_default();

        let mut charts = Map::new();
        for (key, value) in entries {
            let Some(chart) = table.parse_chart_change_event_key_for_height(height, &key) else {
                continue;
            };
            let Ok(decoded) = decode_chart_change_set_v1(&value) else {
                continue;
            };
            let mut changes = Map::new();
            for (tf, v) in decoded.timeframe_changes {
                changes.insert(
                    tf,
                    json!({
                        "usd": format_change_basis_points(v.usd_bp),
                        "usd_bp": v.usd_bp,
                        "mcusd": format_change_basis_points(v.mcusd_bp),
                        "mcusd_bp": v.mcusd_bp
                    }),
                );
            }
            charts.insert(chart, Value::Object(changes));
        }

        Ok(RpcGetChartChangesBlockResult {
            value: json!({
                "ok": true,
                "available": !charts.is_empty(),
                "height": height,
                "charts": charts
            }),
        })
    }

    pub fn rpc_get_activity(&self, params: RpcGetActivityParams) -> Result<RpcGetActivityResult> {
        let limit = params.limit.map(|n| n as usize).unwrap_or(50);
        let page = params.page.map(|n| n as usize).unwrap_or(1);

        let side = params.side.as_deref().and_then(parse_price_side).unwrap_or(PriceSide::Base);

        let filter_side = parse_side_filter_str(params.filter_side.as_deref());
        let activity_type = parse_activity_type_str(params.activity_type.as_deref());

        let sort_token: Option<String> = params.sort.clone();
        let dir = parse_sort_dir_str(params.dir.as_deref());
        let (sort_key, sort_code) = map_sort(side, sort_token.as_deref());

        let pool = match params.pool.as_deref().and_then(parse_id_from_str) {
            Some(p) => p,
            None => {
                return Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_or_invalid_pool",
                        "hint": "pool should be a string like \"2:68441\""
                    }),
                });
            }
        };

        if sort_token.is_some()
            || !matches!(filter_side, ActivitySideFilter::All)
            || !matches!(activity_type, ActivityFilter::All)
        {
            match read_activity_for_pool_sorted(
                self,
                pool,
                page,
                limit,
                side,
                sort_key,
                dir,
                filter_side,
                activity_type,
            ) {
                Ok(ActivityPage { activity, total }) => Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": true,
                        "pool": id_str(&pool),
                        "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                        "filter_side": match filter_side {
                            ActivitySideFilter::All => "all",
                            ActivitySideFilter::Buy => "buy",
                            ActivitySideFilter::Sell => "sell"
                        },
                        "activity_type": match activity_type {
                            ActivityFilter::All => "all",
                            ActivityFilter::Trades => "trades",
                            ActivityFilter::Events => "events",
                        },
                        "sort": sort_code,
                        "dir": match dir { SortDir::Asc => "asc", SortDir::Desc => "desc" },
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": page.saturating_mul(limit) < total,
                        "activity": activity
                    }),
                }),
                Err(e) => Ok(RpcGetActivityResult {
                    value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
                }),
            }
        } else {
            match read_activity_for_pool(self, pool, page, limit, side, activity_type) {
                Ok(ActivityPage { activity, total }) => Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": true,
                        "pool": id_str(&pool),
                        "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                        "filter_side": "all",
                        "activity_type": "all",
                        "sort": "ts",
                        "dir": "desc",
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": page.saturating_mul(limit) < total,
                        "activity": activity
                    }),
                }),
                Err(e) => Ok(RpcGetActivityResult {
                    value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
                }),
            }
        }
    }

    pub fn rpc_get_pools(&self, params: RpcGetPoolsParams) -> Result<RpcGetPoolsResult> {
        let live_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self) {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcGetPoolsResult {
                    value: json!({
                        "ok": false,
                        "error": "live_fetch_failed",
                        "hint": "could not load live reserves"
                    }),
                });
            }
        };

        let mut rows: Vec<(SchemaAlkaneId, SchemaPoolSnapshot)> = live_map.into_iter().collect();
        rows.sort_by(|(a, _), (b, _)| a.block.cmp(&b.block).then(a.tx.cmp(&b.tx)));
        let total = rows.len();

        let limit = params.limit.map(|n| n as usize).unwrap_or(total.max(1)).clamp(1, 20_000);
        let page = params.page.map(|n| n as usize).unwrap_or(1).max(1);

        let offset = limit.saturating_mul(page.saturating_sub(1));
        let end = (offset + limit).min(total);
        let window = if offset >= total { &[][..] } else { &rows[offset..end] };
        let has_more = end < total;

        let mut pools_obj: Map<String, Value> = Map::with_capacity(window.len());
        for (pool, snap) in window.iter() {
            pools_obj.insert(
                format!("{}:{}", pool.block, pool.tx),
                json!({
                    "base":          format!("{}:{}", snap.base_id.block,  snap.base_id.tx),
                    "quote":         format!("{}:{}", snap.quote_id.block, snap.quote_id.tx),
                    "base_reserve":  snap.base_reserve.to_string(),
                    "quote_reserve": snap.quote_reserve.to_string(),
                    "source": "live",
                }),
            );
        }

        Ok(RpcGetPoolsResult {
            value: json!({
                "ok": true,
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "pools": Value::Object(pools_obj)
            }),
        })
    }

    pub fn rpc_get_amm_factories(
        &self,
        params: RpcGetAmmFactoriesParams,
    ) -> Result<RpcGetAmmFactoriesResult> {
        let mut factories =
            match self.get_amm_factories(GetAmmFactoriesParams { blockhash: StateAt::Latest }) {
                Ok(res) => res.factories,
                Err(e) => {
                    return Ok(RpcGetAmmFactoriesResult {
                        value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
                    });
                }
            };

        factories.sort();
        let total = factories.len();

        let limit = params.limit.map(|n| n as usize).unwrap_or(total.max(1)).clamp(1, 20_000);
        let page = params.page.map(|n| n as usize).unwrap_or(1).max(1);
        let offset = limit.saturating_mul(page.saturating_sub(1));
        let end = (offset + limit).min(total);
        let window = if offset >= total { &[][..] } else { &factories[offset..end] };
        let has_more = end < total;

        let list: Vec<Value> = window.iter().map(|id| Value::String(id_str(id))).collect();

        Ok(RpcGetAmmFactoriesResult {
            value: json!({
                "ok": true,
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "factories": list,
            }),
        })
    }

    pub fn rpc_find_best_swap_path(
        &self,
        params: RpcFindBestSwapPathParams,
    ) -> Result<RpcFindBestSwapPathResult> {
        let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self)
        {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves unavailable"
                    }),
                });
            }
        };

        if snapshot_map.is_empty() {
            return Ok(RpcFindBestSwapPathResult {
                value: json!({
                    "ok": false,
                    "error": "no_liquidity",
                    "hint": "live reserves map is empty"
                }),
            });
        }

        let mode = params.mode.as_deref().unwrap_or("exact_in").to_ascii_lowercase();

        let token_in = match params.token_in.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token_in"}),
                });
            }
        };
        let token_out = match params.token_out.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token_out"}),
                });
            }
        };

        let fee_bps = params.fee_bps.map(|n| n as u32).unwrap_or(DEFAULT_FEE_BPS);
        let max_hops = params.max_hops.map(|n| n as usize).unwrap_or(3).max(1).min(6);

        let plan = match mode.as_str() {
            "exact_in" => {
                let amount_in = match parse_u128_arg(params.amount_in.as_ref()) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_in"}),
                        });
                    }
                };
                let min_out = parse_u128_arg(params.amount_out_min.as_ref()).unwrap_or(0u128);

                if params.fee_bps.is_some() {
                    plan_swap_exact_tokens_for_tokens(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_in,
                        min_out,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_exact_in_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_in,
                        min_out,
                        max_hops,
                    )
                }
            }
            "exact_out" => {
                let amount_out = match parse_u128_arg(params.amount_out.as_ref()) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_out"}),
                        });
                    }
                };
                let in_max = parse_u128_arg(params.amount_in_max.as_ref()).unwrap_or(u128::MAX);

                if params.fee_bps.is_some() {
                    plan_swap_tokens_for_exact_tokens(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_out,
                        in_max,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_exact_out_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_out,
                        in_max,
                        max_hops,
                    )
                }
            }
            "implicit" => {
                let available_in = match parse_u128_arg(
                    params.amount_in.as_ref().or(params.available_in.as_ref()),
                ) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_in"}),
                        });
                    }
                };
                let min_out = parse_u128_arg(params.amount_out_min.as_ref()).unwrap_or(0u128);

                if params.fee_bps.is_some() {
                    plan_swap_exact_tokens_for_tokens_implicit(
                        &snapshot_map,
                        token_in,
                        token_out,
                        available_in,
                        min_out,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_implicit_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        available_in,
                        min_out,
                        max_hops,
                    )
                }
            }
            _ => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": false,
                        "error": "invalid_mode",
                        "hint": "use exact_in | exact_out | implicit"
                    }),
                });
            }
        };

        match plan {
            Some(pq) => {
                let hops: Vec<Value> = pq
                    .hops
                    .iter()
                    .map(|h| {
                        json!({
                            "pool":       id_str(&h.pool),
                            "token_in":   id_str(&h.token_in),
                            "token_out":  id_str(&h.token_out),
                            "amount_in":  h.amount_in.to_string(),
                            "amount_out": h.amount_out.to_string(),
                        })
                    })
                    .collect();

                Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": true,
                        "mode": mode,
                        "token_in":  id_str(&token_in),
                        "token_out": id_str(&token_out),
                        "fee_bps": fee_bps,
                        "max_hops": max_hops,
                        "amount_in":  pq.amount_in.to_string(),
                        "amount_out": pq.amount_out.to_string(),
                        "hops": hops
                    }),
                })
            }
            None => Ok(RpcFindBestSwapPathResult {
                value: json!({"ok": false, "error": "no_path_found"}),
            }),
        }
    }

    pub fn rpc_get_best_mev_swap(
        &self,
        params: RpcGetBestMevSwapParams,
    ) -> Result<RpcGetBestMevSwapResult> {
        let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self)
        {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcGetBestMevSwapResult {
                    value: json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves unavailable"
                    }),
                });
            }
        };

        if snapshot_map.is_empty() {
            return Ok(RpcGetBestMevSwapResult {
                value: json!({
                    "ok": false,
                    "error": "no_liquidity",
                    "hint": "live reserves map is empty"
                }),
            });
        }

        let token = match params.token.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcGetBestMevSwapResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token"}),
                });
            }
        };
        let fee_bps = params.fee_bps.map(|n| n as u32).unwrap_or(DEFAULT_FEE_BPS);
        let max_hops = params.max_hops.map(|n| n as usize).unwrap_or(3).clamp(2, 6);

        match plan_best_mev_swap(&snapshot_map, token, fee_bps, max_hops) {
            Some(pq) => {
                let hops: Vec<Value> = pq
                    .hops
                    .iter()
                    .map(|h| {
                        json!({
                            "pool":       id_str(&h.pool),
                            "token_in":   id_str(&h.token_in),
                            "token_out":  id_str(&h.token_out),
                            "amount_in":  h.amount_in.to_string(),
                            "amount_out": h.amount_out.to_string(),
                        })
                    })
                    .collect();

                Ok(RpcGetBestMevSwapResult {
                    value: json!({
                        "ok": true,
                        "token":   id_str(&token),
                        "fee_bps": fee_bps,
                        "max_hops": max_hops,
                        "amount_in":  pq.amount_in.to_string(),
                        "amount_out": pq.amount_out.to_string(),
                        "profit": (pq.amount_out as i128 - pq.amount_in as i128).to_string(),
                        "hops": hops
                    }),
                })
            }
            None => Ok(RpcGetBestMevSwapResult {
                value: json!({"ok": false, "error": "no_profitable_cycle"}),
            }),
        }
    }

    pub fn rpc_get_btc_usd_price(
        &self,
        params: RpcGetBtcUsdPriceParams,
    ) -> Result<RpcGetBtcUsdPriceResult> {
        let table = self.table();
        let out = if let Some(height) = params.height {
            let key = table.btc_usd_price_key(height);
            let value =
                self.get_raw_value(GetRawValueParams { blockhash: StateAt::Latest, key })?;
            match value.value.and_then(|raw| decode_u128_value(&raw).ok()).filter(|v| *v > 0) {
                Some(price) => json!({
                    "ok": true,
                    "height": height,
                    "price": price.to_string(),
                    "source": "ammdata_index"
                }),
                None => match self.get_latest_btc_usd_price_entry(GetLatestBtcUsdPriceParams {
                    blockhash: StateAt::Latest,
                })? {
                    Some((fallback_height, price)) => json!({
                        "ok": true,
                        "height": fallback_height,
                        "requested_height": height,
                        "price": price.to_string(),
                        "source": "ammdata_index_last_active"
                    }),
                    None => json!({
                        "ok": false,
                        "error": "price_unavailable",
                        "height": height
                    }),
                },
            }
        } else {
            match self.get_latest_btc_usd_price_entry(GetLatestBtcUsdPriceParams {
                blockhash: StateAt::Latest,
            })? {
                Some((height, price)) => json!({
                    "ok": true,
                    "height": height,
                    "price": price.to_string(),
                    "source": "ammdata_index"
                }),
                None => json!({
                    "ok": false,
                    "error": "price_unavailable"
                }),
            }
        };
        Ok(RpcGetBtcUsdPriceResult { value: out })
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

pub struct GetReservesSnapshotParams {
    pub blockhash: StateAt,
}

pub struct GetReservesSnapshotResult {
    pub snapshot: Option<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>>,
}

pub struct GetCanonicalPoolsParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
}

pub struct GetCanonicalPoolsResult {
    pub pools: Vec<SchemaCanonicalPoolEntry>,
}

pub struct GetLatestBtcUsdPriceParams {
    pub blockhash: StateAt,
}

pub struct GetLatestTokenUsdCloseParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
    pub timeframe: Timeframe,
}

pub struct GetLatestTokenUsdCloseResult {
    pub close: Option<u128>,
}

pub struct GetTokenMetricsParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
}

pub struct GetTokenMetricsResult {
    pub metrics: SchemaTokenMetricsV1,
}

pub struct GetTokenDerivedMetricsParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
    pub quote: SchemaAlkaneId,
}

pub struct GetTokenDerivedMetricsResult {
    pub metrics: Option<SchemaTokenMetricsV1>,
}

pub struct GetTokenMetricsByIdParams {
    pub blockhash: StateAt,

    pub tokens: Vec<SchemaAlkaneId>,
}

pub struct GetTokenMetricsByIdResult {
    pub metrics: Vec<Option<SchemaTokenMetricsV1>>,
}

pub struct GetTokenDerivedMetricsByIdParams {
    pub blockhash: StateAt,

    pub tokens: Vec<SchemaAlkaneId>,
    pub quote: SchemaAlkaneId,
}

pub struct GetTokenDerivedMetricsByIdResult {
    pub metrics: Vec<Option<SchemaTokenMetricsV1>>,
}

pub struct GetTokenMetricsIndexPageParams {
    pub blockhash: StateAt,

    pub field: TokenMetricsIndexField,
    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetTokenMetricsIndexPageResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetTokenDerivedMetricsIndexPageParams {
    pub blockhash: StateAt,

    pub quote: SchemaAlkaneId,
    pub field: TokenMetricsIndexField,
    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetTokenDerivedMetricsIndexPageResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetTokenMetricsIndexCountParams {
    pub blockhash: StateAt,
}

pub struct GetTokenMetricsIndexCountResult {
    pub count: u64,
}

pub struct GetTokenDerivedMetricsIndexCountParams {
    pub blockhash: StateAt,

    pub quote: SchemaAlkaneId,
}

pub struct GetTokenDerivedMetricsIndexCountResult {
    pub count: u64,
}

pub struct GetTokenSearchIndexPageParams {
    pub blockhash: StateAt,

    pub field: SearchIndexField,
    pub prefix: String,
    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetTokenSearchIndexPageResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetTokenDerivedSearchIndexPageParams {
    pub blockhash: StateAt,

    pub quote: SchemaAlkaneId,
    pub field: SearchIndexField,
    pub prefix: String,
    pub offset: u64,
    pub limit: u64,
    pub desc: bool,
}

pub struct GetTokenDerivedSearchIndexPageResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetPoolIdsByNamePrefixParams {
    pub blockhash: StateAt,

    pub prefix: String,
}

pub struct GetPoolIdsByNamePrefixResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetAmmFactoriesParams {
    pub blockhash: StateAt,
}

pub struct GetAmmFactoriesResult {
    pub factories: Vec<SchemaAlkaneId>,
}

pub struct GetFactoryPoolsParams {
    pub blockhash: StateAt,

    pub factory: SchemaAlkaneId,
}

pub struct GetFactoryPoolsResult {
    pub pools: Vec<SchemaAlkaneId>,
}

pub struct GetPoolDefsParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolDefsResult {
    pub defs: Option<SchemaMarketDefs>,
}

pub struct GetPoolFactoryParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolFactoryResult {
    pub factory: Option<SchemaAlkaneId>,
}

pub struct GetPoolMetricsParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolMetricsResult {
    pub metrics: SchemaPoolMetricsV1,
}

pub struct GetPoolMetricsV2Params {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolMetricsV2Result {
    pub metrics: Option<SchemaPoolMetricsV2>,
}

pub struct GetPoolCreationInfoParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolCreationInfoResult {
    pub info: Option<SchemaPoolCreationInfoV1>,
}

pub struct GetPoolLpSupplyLatestParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolLpSupplyLatestResult {
    pub supply: u128,
}

pub struct GetPoolDetailsSnapshotParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
}

pub struct GetPoolDetailsSnapshotResult {
    pub snapshot: Option<SchemaPoolDetailsSnapshot>,
}

pub struct GetPoolActivityEntriesParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
    pub kinds: Option<Vec<ActivityKind>>,
    pub successful: Option<bool>,
    pub include_total: bool,
}

pub struct GetPoolActivityEntriesResult {
    pub entries: Vec<SchemaActivityV1>,
    pub total: usize,
}

pub struct GetActivityEntryParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
    pub ts: u64,
    pub seq: u32,
}

pub struct GetActivityEntryResult {
    pub entry: Option<SchemaActivityV1>,
}

#[derive(Clone, Debug)]
pub struct TokenSwapEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetTokenSwapsPageParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetTokenSwapsPageResult {
    pub entries: Vec<TokenSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct PoolCreationEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolCreationsPageParams {
    pub blockhash: StateAt,

    pub offset: usize,
    pub limit: usize,
}

pub struct GetPoolCreationsPageResult {
    pub entries: Vec<PoolCreationEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressPoolSwapEntry {
    pub ts: u64,
    pub seq: u32,
}

pub struct GetAddressPoolSwapsPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub pool: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolSwapsPageResult {
    pub entries: Vec<AddressPoolSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressTokenSwapEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetAddressTokenSwapsPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub token: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressTokenSwapsPageResult {
    pub entries: Vec<AddressTokenSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressPoolEventEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetAddressPoolCreationsPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolMintsPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolBurnsPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolEventsPageResult {
    pub entries: Vec<AddressPoolEventEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AmmHistoryEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
    pub kind: ActivityKind,
}

pub struct GetAddressAmmHistoryPageParams {
    pub blockhash: StateAt,

    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
    pub kind: Option<ActivityKind>,
}

pub struct GetAmmHistoryAllPageParams {
    pub blockhash: StateAt,

    pub offset: usize,
    pub limit: usize,
    pub kind: Option<ActivityKind>,
}

pub struct GetAmmHistoryPageResult {
    pub entries: Vec<AmmHistoryEntry>,
    pub total: usize,
}

pub struct GetTokenPoolsParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
}

pub struct GetTokenPoolsResult {
    pub pools: Vec<SchemaAlkaneId>,
}

pub struct GetTvlVersionedAtOrBeforeParams {
    pub blockhash: StateAt,

    pub pool: SchemaAlkaneId,
    pub height: u32,
}

pub struct GetTvlVersionedAtOrBeforeResult {
    pub value: Option<u128>,
}

pub struct GetCanonicalPoolPricesParams {
    pub blockhash: StateAt,

    pub token: SchemaAlkaneId,
    pub now_ts: u64,
}

pub struct GetCanonicalPoolPricesResult {
    pub frbtc_price: u128,
    pub busd_price: u128,
}

pub struct SetReservesSnapshotParams {
    pub blockhash: StateAt,

    pub snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
}

pub struct RpcGetCandlesParams {
    pub pool: Option<String>,
    pub timeframe: Option<String>,
    pub limit: Option<u64>,
    pub size: Option<u64>,
    pub page: Option<u64>,
    pub side: Option<String>,
    pub now: Option<u64>,
}

pub struct RpcGetCandlesResult {
    pub value: Value,
}

pub struct RpcGetChartChangeBlockParams {
    pub chart: Option<String>,
    pub height: Option<u64>,
}

pub struct RpcGetChartChangeBlockResult {
    pub value: Value,
}

pub struct RpcGetChartChangesBlockParams {
    pub height: Option<u64>,
}

pub struct RpcGetChartChangesBlockResult {
    pub value: Value,
}

pub struct RpcGetActivityParams {
    pub pool: Option<String>,
    pub limit: Option<u64>,
    pub page: Option<u64>,
    pub side: Option<String>,
    pub filter_side: Option<String>,
    pub activity_type: Option<String>,
    pub sort: Option<String>,
    pub dir: Option<String>,
}

pub struct RpcGetActivityResult {
    pub value: Value,
}

pub struct RpcGetPoolsParams {
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetPoolsResult {
    pub value: Value,
}

pub struct RpcGetAmmFactoriesParams {
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetAmmFactoriesResult {
    pub value: Value,
}

pub struct RpcFindBestSwapPathParams {
    pub mode: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub fee_bps: Option<u64>,
    pub max_hops: Option<u64>,
    pub amount_in: Option<Value>,
    pub amount_out_min: Option<Value>,
    pub amount_out: Option<Value>,
    pub amount_in_max: Option<Value>,
    pub available_in: Option<Value>,
}

pub struct RpcFindBestSwapPathResult {
    pub value: Value,
}

pub struct RpcGetBestMevSwapParams {
    pub token: Option<String>,
    pub fee_bps: Option<u64>,
    pub max_hops: Option<u64>,
}

pub struct RpcGetBestMevSwapResult {
    pub value: Value,
}

pub struct RpcGetBtcUsdPriceParams {
    pub height: Option<u64>,
}

pub struct RpcGetBtcUsdPriceResult {
    pub value: Value,
}

pub struct RpcPingParams;

pub struct RpcPingResult {
    pub value: Value,
}

/// Hex without "0x", lowercase
#[inline]
pub fn to_hex_no0x<T: Into<u128>>(x: T) -> String {
    format!("{:x}", x.into())
}

/// Deserialize helper
pub fn decode_full_candle_v1(bytes: &[u8]) -> anyhow::Result<SchemaFullCandleV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaFullCandleV1::try_from_slice(bytes)?)
}

/// Encode helper
pub fn encode_full_candle_v1(v: &SchemaFullCandleV1) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    v.serialize(&mut out)?;
    Ok(out)
}

pub fn decode_candle_v1(bytes: &[u8]) -> anyhow::Result<SchemaCandleV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaCandleV1::try_from_slice(bytes)?)
}

pub fn encode_candle_v1(v: &SchemaCandleV1) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(40);
    v.serialize(&mut out)?;
    Ok(out)
}

pub fn decode_canonical_pools(bytes: &[u8]) -> anyhow::Result<Vec<SchemaCanonicalPoolEntry>> {
    use borsh::BorshDeserialize;
    Ok(Vec::<SchemaCanonicalPoolEntry>::try_from_slice(bytes)?)
}

pub fn encode_canonical_pools(entries: &[SchemaCanonicalPoolEntry]) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(entries)?)
}

pub fn decode_chart_change_set_v1(bytes: &[u8]) -> anyhow::Result<SchemaChartChangeSetV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaChartChangeSetV1::try_from_slice(bytes)?)
}

pub fn encode_chart_change_set_v1(v: &SchemaChartChangeSetV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_token_metrics(bytes: &[u8]) -> anyhow::Result<SchemaTokenMetricsV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaTokenMetricsV1::try_from_slice(bytes)?)
}

pub fn encode_token_metrics(v: &SchemaTokenMetricsV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_pool_metrics(bytes: &[u8]) -> anyhow::Result<SchemaPoolMetricsV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolMetricsV1::try_from_slice(bytes)?)
}

pub fn encode_pool_metrics(v: &SchemaPoolMetricsV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_pool_metrics_v2(bytes: &[u8]) -> anyhow::Result<SchemaPoolMetricsV2> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolMetricsV2::try_from_slice(bytes)?)
}

pub fn encode_pool_metrics_v2(v: &SchemaPoolMetricsV2) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_pool_creation_info(bytes: &[u8]) -> anyhow::Result<SchemaPoolCreationInfoV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolCreationInfoV1::try_from_slice(bytes)?)
}

pub fn encode_pool_creation_info(v: &SchemaPoolCreationInfoV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_u128_value(bytes: &[u8]) -> anyhow::Result<u128> {
    if bytes.len() != 16 {
        return Err(anyhow!("invalid u128 length {}", bytes.len()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes[..16]);
    Ok(u128::from_le_bytes(arr))
}

pub fn encode_u128_value(value: u128) -> anyhow::Result<Vec<u8>> {
    Ok(value.to_le_bytes().to_vec())
}

pub fn decode_f64_value(bytes: &[u8]) -> anyhow::Result<f64> {
    if bytes.len() != 8 {
        return Err(anyhow!("invalid f64 length {}", bytes.len()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    Ok(f64::from_le_bytes(arr))
}

pub fn encode_f64_value(value: f64) -> anyhow::Result<Vec<u8>> {
    Ok(value.to_le_bytes().to_vec())
}

pub fn decode_pool_snapshot(bytes: &[u8]) -> anyhow::Result<SchemaPoolSnapshot> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolSnapshot::try_from_slice(bytes)?)
}

pub fn encode_pool_snapshot(v: &SchemaPoolSnapshot) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

// Encode Snapshot -> BORSH (deterministic order via BTreeMap)
pub fn encode_reserves_snapshot(
    map: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
) -> Result<Vec<u8>> {
    let ordered: BTreeMap<SchemaAlkaneId, SchemaPoolSnapshot> =
        map.iter().map(|(k, v)| (*k, v.clone())).collect();
    let snap = SchemaReservesSnapshot { entries: ordered };
    Ok(borsh::to_vec(&snap)?)
}

// Decode BORSH -> Snapshot
pub fn decode_reserves_snapshot(
    bytes: &[u8],
) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let snap = SchemaReservesSnapshot::try_from_slice(bytes)?;
    Ok(snap.entries.into_iter().collect())
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn parse_id_from_str(s: &str) -> Option<SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let parse_u32 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u32::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u32>().ok()
        }
    };
    let parse_u64 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u64::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(parts[0])?, tx: parse_u64(parts[1])? })
}

fn decode_alkane_id_be(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    if bytes.len() != 12 {
        return None;
    }
    let mut block_arr = [0u8; 4];
    block_arr.copy_from_slice(&bytes[..4]);
    let mut tx_arr = [0u8; 8];
    tx_arr.copy_from_slice(&bytes[4..12]);
    Some(SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) })
}

pub fn encode_alkane_id_be(id: &SchemaAlkaneId) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..4].copy_from_slice(&id.block.to_be_bytes());
    out[4..].copy_from_slice(&id.tx.to_be_bytes());
    out
}

fn parse_alkane_id_from_prefixed_key(prefix: &[u8], key: &[u8]) -> Option<SchemaAlkaneId> {
    if !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    decode_alkane_id_be(rest)
}

fn parse_height_from_tvl_key(key: &[u8]) -> Result<u32> {
    if key.len() < 4 {
        return Err(anyhow!("tvlVersioned key too short"));
    }
    let height_bytes = &key[key.len() - 4..];
    let mut arr = [0u8; 4];
    arr.copy_from_slice(height_bytes);
    Ok(u32::from_be_bytes(arr))
}

fn read_token_usd_candles_v1(
    provider: &AmmDataProvider,
    token: SchemaAlkaneId,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.token_usd_candle_ns_prefix(&token, tf);
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: logical,
        })?
        .entries
    {
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if !per_bucket.contains_key(&ts) {
                        if let Ok(c) = decode_candle_v1(&v) {
                            per_bucket.insert(ts, c);
                        }
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(candle) = per_bucket.get(&bts) {
            let mut c = *candle;
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn read_token_derived_usd_candles_v1(
    provider: &AmmDataProvider,
    token: SchemaAlkaneId,
    quote: SchemaAlkaneId,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.token_derived_usd_candle_ns_prefix(&token, &quote, tf);
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: logical,
        })?
        .entries
    {
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if !per_bucket.contains_key(&ts) {
                        if let Ok(c) = decode_candle_v1(&v) {
                            per_bucket.insert(ts, c);
                        }
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(candle) = per_bucket.get(&bts) {
            let mut c = *candle;
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn read_token_derived_mcusd_candles_v1(
    provider: &AmmDataProvider,
    token: SchemaAlkaneId,
    quote: SchemaAlkaneId,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.token_derived_mcusd_candle_ns_prefix(&token, &quote, tf);
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: logical,
        })?
        .entries
    {
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if !per_bucket.contains_key(&ts) {
                        if let Ok(c) = decode_candle_v1(&v) {
                            per_bucket.insert(ts, c);
                        }
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(candle) = per_bucket.get(&bts) {
            let mut c = *candle;
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn read_token_mcusd_candles_v1(
    provider: &AmmDataProvider,
    token: SchemaAlkaneId,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.token_mcusd_candle_ns_prefix(&token, tf);
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: logical,
        })?
        .entries
    {
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if !per_bucket.contains_key(&ts) {
                        if let Ok(c) = decode_candle_v1(&v) {
                            per_bucket.insert(ts, c);
                        }
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(candle) = per_bucket.get(&bts) {
            let mut c = *candle;
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn read_btc_usd_line_v1(
    provider: &AmmDataProvider,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.btc_usd_line_ns_prefix(tf);
    let mut per_bucket: BTreeMap<u64, u128> = BTreeMap::new();
    for (k, v) in provider
        .get_list_entries_desc(GetListEntriesDescParams {
            blockhash: StateAt::Latest,
            prefix: logical,
        })?
        .entries
    {
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if !per_bucket.contains_key(&ts) {
                        if let Ok(p) = decode_u128_value(&v) {
                            per_bucket.insert(ts, p);
                        }
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_price: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(price) = per_bucket.get(&bts) {
            let p = *price;
            last_price = p;
            have_prev = true;
            forward.insert(bts, SchemaCandleV1 { open: p, high: p, low: p, close: p, volume: 0 });
        } else if have_prev {
            let p = last_price;
            forward.insert(bts, SchemaCandleV1 { open: p, high: p, low: p, close: p, volume: 0 });
        } else {
            forward.insert(bts, SchemaCandleV1 { open: 0, high: 0, low: 0, close: 0, volume: 0 });
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let p = last_price;
            forward.insert(t, SchemaCandleV1 { open: p, high: p, low: p, close: p, volume: 0 });
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn parse_timeframe(s: &str) -> Option<Timeframe> {
    match s {
        "10m" | "m10" => Some(Timeframe::M10),
        "1h" | "h1" => Some(Timeframe::H1),
        "4h" | "h4" => Some(Timeframe::H4),
        "1d" | "d1" => Some(Timeframe::D1),
        "1w" | "w1" => Some(Timeframe::W1),
        "1M" | "m1" => Some(Timeframe::M1),
        _ => None,
    }
}

fn parse_price_side(s: &str) -> Option<PriceSide> {
    match s.to_ascii_lowercase().as_str() {
        "base" | "b" => Some(PriceSide::Base),
        "quote" | "q" => Some(PriceSide::Quote),
        _ => None,
    }
}

fn parse_side_filter_str(s: Option<&str>) -> ActivitySideFilter {
    if let Some(s) = s {
        return match s.to_ascii_lowercase().as_str() {
            "buy" | "b" => ActivitySideFilter::Buy,
            "sell" | "s" => ActivitySideFilter::Sell,
            "all" | "a" | "" => ActivitySideFilter::All,
            _ => ActivitySideFilter::All,
        };
    }
    ActivitySideFilter::All
}

fn parse_activity_type_str(s: Option<&str>) -> ActivityFilter {
    if let Some(s) = s {
        return match s.to_ascii_lowercase().as_str() {
            "trades" | "trade" => ActivityFilter::Trades,
            "events" | "event" => ActivityFilter::Events,
            "all" | "" => ActivityFilter::All,
            _ => ActivityFilter::All,
        };
    }
    ActivityFilter::All
}

fn parse_sort_dir_str(s: Option<&str>) -> SortDir {
    if let Some(s) = s {
        match s.to_ascii_lowercase().as_str() {
            "asc" | "ascending" => return SortDir::Asc,
            "desc" | "descending" => return SortDir::Desc,
            _ => {}
        }
    }
    SortDir::Desc
}

fn norm_token(s: &str) -> Option<&'static str> {
    match s.to_ascii_lowercase().as_str() {
        "ts" | "time" | "timestamp" => Some("ts"),
        "amt" | "amount" => Some("amount"),
        "side" | "s" => Some("side"),
        "absb" | "amount_base" | "base_amount" => Some("absb"),
        "absq" | "amount_quote" | "quote_amount" => Some("absq"),
        _ => None,
    }
}

fn map_sort(side: PriceSide, token: Option<&str>) -> (ActivitySortKey, &'static str) {
    if let Some(tok) = token.and_then(norm_token) {
        return match tok {
            "ts" => (ActivitySortKey::Timestamp, "ts"),
            "amount" => match side {
                PriceSide::Base => (ActivitySortKey::AmountBaseAbs, "absb"),
                PriceSide::Quote => (ActivitySortKey::AmountQuoteAbs, "absq"),
            },
            "side" => match side {
                PriceSide::Base => (ActivitySortKey::SideBaseTs, "sb_ts"),
                PriceSide::Quote => (ActivitySortKey::SideQuoteTs, "sq_ts"),
            },
            "absb" => (ActivitySortKey::AmountBaseAbs, "absb"),
            "absq" => (ActivitySortKey::AmountQuoteAbs, "absq"),
            _ => (ActivitySortKey::Timestamp, "ts"),
        };
    }
    (ActivitySortKey::Timestamp, "ts")
}

fn parse_u128_arg(v: Option<&Value>) -> Option<u128> {
    match v {
        Some(Value::String(s)) => s.parse::<u128>().ok(),
        Some(Value::Number(n)) => n.as_u64().map(|x| x as u128),
        _ => None,
    }
}

fn id_str(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}
