use crate::alkanes::metashrew::MetashrewAdapter;
use crate::runtime::{dbpaths::get_sdb_path_for_metashrew, sdb::SDB, tree_db::init_global_tree_db};
use crate::utils::electrum_like::{ElectrumLike, ElectrumRpcClient, EsploraElectrumLike};
use crate::{ESPO_HEIGHT, SAFE_TIP};
use anyhow::{Context, Result};
use clap::Parser;
use electrum_client::Client;
use rocksdb::{BlockBasedOptions, Cache, DB, Options};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{
    fs,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

// Bitcoin Core / bitcoin::Network
use crate::bitcoind_flexible::FlexibleBitcoindClient as CoreClient;
use bitcoincore_rpc::bitcoin::Network;

// Block fetcher (blk files + RPC fallback)
use crate::core::blockfetcher::{BlkOrRpcBlockSource, BlockFetchMode};

static CONFIG: OnceLock<AppConfig> = OnceLock::new();
static ELECTRUM_CLIENT: OnceLock<Arc<Client>> = OnceLock::new();
static ELECTRUM_LIKE: OnceLock<Arc<dyn ElectrumLike>> = OnceLock::new();
static BITCOIND_CLIENT: OnceLock<CoreClient> = OnceLock::new();
static METASHREW_SDB: OnceLock<std::sync::Arc<SDB>> = OnceLock::new();
static ESPO_DB: OnceLock<std::sync::Arc<DB>> = OnceLock::new();
static CACHE_DB: OnceLock<std::sync::Arc<DB>> = OnceLock::new();
static BLOCK_SOURCE: OnceLock<BlkOrRpcBlockSource> = OnceLock::new();

// NEW: Global bitcoin::Network
static NETWORK: OnceLock<Network> = OnceLock::new();

const ESPO_ROCKS_BLOCK_CACHE_BYTES: usize = 512 * 1024 * 1024;
const ESPO_ROCKS_MAX_OPEN_FILES: i32 = 1024;
const CACHE_ROCKS_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const CACHE_ROCKS_MAX_OPEN_FILES: i32 = 128;

fn configure_espo_rocksdb_options(opts: &mut Options) {
    let cache = Cache::new_lru_cache(ESPO_ROCKS_BLOCK_CACHE_BYTES);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&cache);
    table.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&table);
    opts.set_max_open_files(ESPO_ROCKS_MAX_OPEN_FILES);
}

fn configure_cache_rocksdb_options(opts: &mut Options) {
    let cache = Cache::new_lru_cache(CACHE_ROCKS_BLOCK_CACHE_BYTES);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&cache);
    table.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&table);
    opts.set_max_open_files(CACHE_ROCKS_MAX_OPEN_FILES);
}

fn parse_network(s: &str) -> Result<Network> {
    let normalized = s.trim().to_ascii_lowercase();
    let mapped = match normalized.as_str() {
        "mainnet" => "bitcoin",
        "testnet3" => "testnet",
        other => other,
    };
    Network::from_str(mapped).map_err(|_| {
        anyhow::anyhow!(
            "invalid value for network: expected mainnet | regtest | signet | testnet | testnet3 | testnet4"
        )
    })
}

fn parse_block_fetch_mode(s: &str) -> std::result::Result<BlockFetchMode, String> {
    match s.to_ascii_lowercase().as_str() {
        "auto" => Ok(BlockFetchMode::Auto),
        "rpc" | "rpc-only" | "rpc_only" => Ok(BlockFetchMode::RpcOnly),
        "blk" | "blk-only" | "blk_only" | "files" => Ok(BlockFetchMode::BlkOnly),
        _ => Err("invalid value for block_source_mode: use auto | rpc-only | blk-only".into()),
    }
}

fn normalize_explorer_base_path(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Ok("/".to_string());
    }
    let no_trailing = trimmed.trim_end_matches('/');
    let normalized = if no_trailing.starts_with('/') {
        no_trailing.to_string()
    } else {
        format!("/{no_trailing}")
    };
    Ok(normalized)
}

fn default_bitcoind_blocks_dir() -> String {
    "~/.bitcoin/blocks".to_string()
}

fn default_db_path() -> String {
    "./db".to_string()
}

fn default_alkabi_verify_trials() -> u32 {
    128
}

fn default_sdb_poll_ms() -> u16 {
    5000
}

fn default_port() -> u16 {
    8080
}

fn default_explorer_espo_rpc_cache_ms() -> u64 {
    2000
}

fn default_explorer_base_path() -> String {
    "/".to_string()
}

fn default_explorer_pizza_tv_endpoint() -> String {
    "https://tv.pizza.fun".to_string()
}

fn default_explorer_amm_prefix() -> String {
    "https://www.oyl.io/swap".to_string()
}

fn default_network() -> String {
    "mainnet".to_string()
}

fn default_block_source_mode() -> String {
    "rpc".to_string()
}

fn default_compact_tx_trace_rows() -> bool {
    true
}

fn default_address_index_chunk_size() -> u32 {
    512
}

fn default_trace_read_workers() -> u16 {
    8
}

fn default_mempool_enabled() -> bool {
    true
}

fn default_mempool_raw_poll_secs() -> u64 {
    60
}

fn default_mempool_template_poll_secs() -> u64 {
    10
}

fn default_mempool_trace_workers() -> usize {
    1
}

fn default_mempool_hydration_workers() -> usize {
    16
}

fn default_mempool_clear_protection_secs() -> u64 {
    180
}

fn default_mempool_max_txs() -> usize {
    100_000
}

fn default_mempool_template_blocks() -> usize {
    8
}

fn default_mempool_block_weight_units() -> u64 {
    4_000_000
}

fn default_regtest_block_interval_secs() -> u64 {
    600
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn default_sync_banner_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExplorerNetworks {
    #[serde(default)]
    pub mainnet: Option<String>,
    #[serde(default)]
    pub signet: Option<String>,
    #[serde(default, rename = "testnet3")]
    pub testnet3: Option<String>,
    #[serde(default, rename = "testnet4")]
    pub testnet4: Option<String>,
    #[serde(default)]
    pub regtest: Option<String>,
}

impl ExplorerNetworks {
    fn normalized(&self) -> Option<Self> {
        let normalized = Self {
            mainnet: normalize_optional_string(self.mainnet.clone()),
            signet: normalize_optional_string(self.signet.clone()),
            testnet3: normalize_optional_string(self.testnet3.clone()),
            testnet4: normalize_optional_string(self.testnet4.clone()),
            regtest: normalize_optional_string(self.regtest.clone()),
        };
        if normalized.is_empty() { None } else { Some(normalized) }
    }

    pub fn is_empty(&self) -> bool {
        self.mainnet.is_none()
            && self.signet.is_none()
            && self.testnet3.is_none()
            && self.testnet4.is_none()
            && self.regtest.is_none()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HostsConfig {
    #[serde(default)]
    pub explorer_host: Option<String>,
    #[serde(default)]
    pub rpc_host: Option<String>,
    #[serde(default)]
    pub oyl_api_host: Option<String>,
}

impl HostsConfig {
    fn normalized(self) -> Self {
        Self {
            explorer_host: normalize_optional_string(self.explorer_host),
            rpc_host: normalize_optional_string(self.rpc_host),
            oyl_api_host: normalize_optional_string(self.oyl_api_host),
        }
    }
}

#[cfg(test)]
mod hosts_config_tests {
    use super::{ConfigFile, HostsConfig};

    #[test]
    fn hosts_object_and_fields_are_optional() {
        let file: ConfigFile = serde_json::from_value(serde_json::json!({
            "readonly_metashrew_db_dir": "/tmp/metashrew",
            "metashrew_rpc_url": "http://127.0.0.1:7145",
            "bitcoind_rpc_url": "http://127.0.0.1:8332"
        }))
        .unwrap();
        let hosts: HostsConfig = serde_json::from_value(serde_json::json!({})).unwrap();

        assert!(file.hosts.explorer_host.is_none());
        assert!(file.hosts.rpc_host.is_none());
        assert!(file.hosts.oyl_api_host.is_none());
        assert!(!file.db_cache);
        assert_eq!(file.alkabi_verify_trials, 128);
        assert!(hosts.explorer_host.is_none());
        assert!(hosts.rpc_host.is_none());
        assert!(hosts.oyl_api_host.is_none());
    }

    #[test]
    fn persistent_db_cache_is_optional_and_can_be_enabled() {
        let file: ConfigFile = serde_json::from_value(serde_json::json!({
            "readonly_metashrew_db_dir": "/tmp/metashrew",
            "metashrew_rpc_url": "http://127.0.0.1:7145",
            "bitcoind_rpc_url": "http://127.0.0.1:8332",
            "db_cache": true,
            "alkabi_verify_trials": 256
        }))
        .unwrap();

        assert!(file.db_cache);
        assert_eq!(file.alkabi_verify_trials, 256);
    }

    #[test]
    fn hosts_are_trimmed_and_blank_values_are_ignored() {
        let hosts = serde_json::from_value::<HostsConfig>(serde_json::json!({
            "explorer_host": " https://explorer.example.com ",
            "rpc_host": "   ",
            "oyl_api_host": "https://oyl.example.com"
        }))
        .unwrap()
        .normalized();

        assert_eq!(hosts.explorer_host.as_deref(), Some("https://explorer.example.com"));
        assert!(hosts.rpc_host.is_none());
        assert_eq!(hosts.oyl_api_host.as_deref(), Some("https://oyl.example.com"));
    }
}

#[derive(Debug, Clone)]
pub struct DebugBackupConfig {
    pub blocks: Vec<u32>,
    pub dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrictModeConfig {
    pub check_utxos: bool,
    pub check_alkane_balances: bool,
    pub check_trace_mismatches: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MiscConfig {
    #[serde(default)]
    pub show_terminal_ad: bool,
}

fn default_jemalloc_profile_dump_dir() -> String {
    "./jemalloc-profiles".to_string()
}

fn default_jemalloc_profile_interval_secs() -> u64 {
    1800
}

fn default_jemalloc_profile_dump_on_shutdown() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct JemallocProfileConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_jemalloc_profile_dump_dir")]
    pub dump_dir: String,
    #[serde(default = "default_jemalloc_profile_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_jemalloc_profile_dump_on_shutdown")]
    pub dump_on_shutdown: bool,
}

impl Default for JemallocProfileConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dump_dir: default_jemalloc_profile_dump_dir(),
            interval_secs: default_jemalloc_profile_interval_secs(),
            dump_on_shutdown: default_jemalloc_profile_dump_on_shutdown(),
        }
    }
}

impl JemallocProfileConfig {
    fn normalized(mut self) -> Result<Self> {
        self.dump_dir = self.dump_dir.trim().to_string();
        if self.enabled && self.dump_dir.is_empty() {
            anyhow::bail!("jemalloc_profile.dump_dir must be non-empty when enabled");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncBannerConfig {
    #[serde(default = "default_sync_banner_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub message_zh: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub link_text: Option<String>,
    #[serde(default)]
    pub link_text_zh: Option<String>,
}

impl SyncBannerConfig {
    fn normalized(self) -> Option<Self> {
        if !self.enabled {
            return None;
        }

        let message = self.message.trim().to_string();
        if message.is_empty() {
            return None;
        }

        Some(Self {
            enabled: self.enabled,
            message,
            message_zh: normalize_optional_string(self.message_zh),
            url: normalize_optional_string(self.url),
            link_text: normalize_optional_string(self.link_text),
            link_text_zh: normalize_optional_string(self.link_text_zh),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MempoolConfig {
    #[serde(default = "default_mempool_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub populate_with_views: bool,
    #[serde(default = "default_mempool_raw_poll_secs")]
    pub raw_poll_secs: u64,
    #[serde(default = "default_mempool_template_poll_secs")]
    pub template_poll_secs: u64,
    #[serde(default = "default_mempool_trace_workers")]
    pub trace_workers: usize,
    #[serde(default = "default_mempool_hydration_workers")]
    pub hydration_workers: usize,
    #[serde(default = "default_mempool_clear_protection_secs")]
    pub clear_protection_secs: u64,
    #[serde(default = "default_mempool_max_txs")]
    pub max_txs: usize,
    #[serde(default = "default_mempool_template_blocks")]
    pub template_blocks: usize,
    #[serde(default = "default_mempool_block_weight_units")]
    pub block_weight_units: u64,
    #[serde(default = "default_regtest_block_interval_secs")]
    pub regtest_block_interval_secs: u64,
    #[serde(default)]
    pub zmq_rawtx_url: Option<String>,
    #[serde(default)]
    pub zmq_sequence_url: Option<String>,
    #[serde(default)]
    pub websocket_enabled: bool,
    #[serde(default)]
    pub websocket_path: Option<String>,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            enabled: default_mempool_enabled(),
            populate_with_views: false,
            raw_poll_secs: default_mempool_raw_poll_secs(),
            template_poll_secs: default_mempool_template_poll_secs(),
            trace_workers: default_mempool_trace_workers(),
            hydration_workers: default_mempool_hydration_workers(),
            clear_protection_secs: default_mempool_clear_protection_secs(),
            max_txs: default_mempool_max_txs(),
            template_blocks: default_mempool_template_blocks(),
            block_weight_units: default_mempool_block_weight_units(),
            regtest_block_interval_secs: default_regtest_block_interval_secs(),
            zmq_rawtx_url: None,
            zmq_sequence_url: None,
            websocket_enabled: false,
            websocket_path: Some("/api/events/ws".to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for DebugBackupConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawDebugBackupConfig {
            pub dir: String,
            #[serde(default)]
            pub block: Option<u32>,
            #[serde(default)]
            pub blocks: Option<Vec<u32>>,
        }

        let raw = RawDebugBackupConfig::deserialize(deserializer)?;
        let dir = raw.dir.trim().to_string();
        if dir.is_empty() {
            return Err(serde::de::Error::custom(
                "debug_backup.dir must be a non-empty string when provided",
            ));
        }

        let mut blocks = raw.blocks.unwrap_or_default();
        if let Some(block) = raw.block {
            blocks.push(block);
        }
        blocks.sort_unstable();
        blocks.dedup();
        if blocks.is_empty() {
            return Err(serde::de::Error::custom(
                "debug_backup requires 'blocks' (array) or 'block' (number)",
            ));
        }

        Ok(DebugBackupConfig { blocks, dir })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigFile {
    pub readonly_metashrew_db_dir: String,
    #[serde(default)]
    pub electrum_rpc_url: Option<String>,
    pub metashrew_rpc_url: String,
    #[serde(default)]
    pub electrs_esplora_url: Option<String>,
    pub bitcoind_rpc_url: String,
    #[serde(default)]
    pub bitcoind_rpc_user: String,
    #[serde(default)]
    pub bitcoind_rpc_pass: String,
    #[serde(default)]
    pub b8_faucet_url: Option<String>,
    #[serde(default)]
    pub hosts: HostsConfig,
    #[serde(default = "default_bitcoind_blocks_dir")]
    pub bitcoind_blocks_dir: String,
    #[serde(default)]
    pub reset_mempool_on_startup: bool,
    #[serde(default)]
    pub rollback: Option<u32>,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default)]
    pub db_cache: bool,
    #[serde(default = "default_alkabi_verify_trials")]
    pub alkabi_verify_trials: u32,
    #[serde(default = "default_sdb_poll_ms")]
    pub sdb_poll_ms: u16,
    #[serde(default)]
    pub indexer_block_delay_ms: u64,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub explorer_host: Option<SocketAddr>,
    #[serde(default)]
    pub explorer_espo_rpc_host: Option<String>,
    #[serde(default)]
    pub explorer_espo_rpc_key: Option<String>,
    #[serde(default = "default_explorer_espo_rpc_cache_ms")]
    pub explorer_espo_rpc_cache_ms: u64,
    #[serde(default)]
    pub enable_internal_rpc: bool,
    #[serde(default)]
    pub internal_rpc_key: Option<String>,
    #[serde(default = "default_explorer_base_path")]
    pub explorer_base_path: String,
    #[serde(default = "default_explorer_pizza_tv_endpoint")]
    pub explorer_pizza_tv_endpoint: String,
    #[serde(default = "default_explorer_amm_prefix")]
    pub explorer_amm_prefix: String,
    #[serde(default)]
    pub sync_banner: Option<SyncBannerConfig>,
    #[serde(default = "default_network")]
    pub network: String,
    #[serde(default)]
    pub metashrew_db_label: Option<String>,
    #[serde(default)]
    pub strict_mode: Option<StrictModeConfig>,
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub debug_ignore_ms: u64,
    #[serde(default)]
    pub debug_backup: Option<DebugBackupConfig>,
    #[serde(default)]
    pub safe_tip_hook_script: Option<String>,
    #[serde(default = "default_block_source_mode")]
    pub block_source_mode: String,
    #[serde(default = "default_compact_tx_trace_rows")]
    pub compact_tx_trace_rows: bool,
    #[serde(default = "default_address_index_chunk_size")]
    pub address_index_chunk_size: u32,
    #[serde(default = "default_trace_read_workers")]
    pub trace_read_workers: u16,
    #[serde(default)]
    pub recover_missing_traces_by_txid: bool,
    #[serde(default)]
    pub explorer_networks: Option<ExplorerNetworks>,
    #[serde(default)]
    pub google_analytics_tag: Option<String>,
    #[serde(default)]
    pub misc: MiscConfig,
    #[serde(default)]
    pub jemalloc_profile: JemallocProfileConfig,
    #[serde(default)]
    pub mempool: MempoolConfig,
    #[serde(default)]
    pub modules: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub readonly_metashrew_db_dir: String,
    pub electrum_rpc_url: Option<String>,
    pub metashrew_rpc_url: String,
    pub electrs_esplora_url: Option<String>,
    pub bitcoind_rpc_url: String,
    pub bitcoind_rpc_user: String,
    pub bitcoind_rpc_pass: String,
    pub b8_faucet_url: Option<String>,
    pub hosts: HostsConfig,
    pub bitcoind_blocks_dir: String,
    pub reset_mempool_on_startup: bool,
    pub rollback: Option<u32>,
    pub view_only: bool,
    pub db_path: String,
    pub db_cache: bool,
    pub alkabi_verify_trials: u32,
    pub sdb_poll_ms: u16,
    pub indexer_block_delay_ms: u64,
    pub port: u16,
    pub explorer_host: Option<SocketAddr>,
    pub explorer_espo_rpc_host: Option<String>,
    pub explorer_espo_rpc_key: Option<String>,
    pub explorer_espo_rpc_cache_ms: u64,
    pub enable_internal_rpc: bool,
    pub internal_rpc_key: Option<String>,
    pub explorer_base_path: String,
    pub explorer_pizza_tv_endpoint: String,
    pub explorer_amm_prefix: String,
    pub sync_banner: Option<SyncBannerConfig>,
    pub network: Network,
    pub metashrew_db_label: Option<String>,
    pub strict_mode: Option<StrictModeConfig>,
    pub debug: bool,
    pub debug_ignore_ms: u64,
    pub debug_backup: Option<DebugBackupConfig>,
    pub safe_tip_hook_script: Option<String>,
    pub block_source_mode: BlockFetchMode,
    pub compact_tx_trace_rows: bool,
    pub address_index_chunk_size: u32,
    pub trace_read_workers: u16,
    pub recover_missing_traces_by_txid: bool,
    pub explorer_networks: Option<ExplorerNetworks>,
    pub google_analytics_tag: Option<String>,
    pub misc: MiscConfig,
    pub jemalloc_profile: JemallocProfileConfig,
    pub mempool: MempoolConfig,
    pub modules: HashMap<String, serde_json::Value>,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    /// Path to JSON config file.
    #[arg(long, default_value = "./config.json")]
    pub config_path: String,

    /// Serve existing data without running the indexer or mempool service.
    #[arg(long, default_value_t = false)]
    pub view_only: bool,

    /// On startup only, rewind indexed state so indexing resumes at this height.
    #[arg(long)]
    pub rollback: Option<u32>,
}

fn load_config_file(path: &str) -> Result<ConfigFile> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read config file: {path}"))?;
    serde_json::from_str(&raw).context("failed to parse config JSON")
}

impl AppConfig {
    fn from_file(file: ConfigFile, view_only: bool) -> Result<Self> {
        let network = parse_network(&file.network)?;
        let block_source_mode =
            parse_block_fetch_mode(&file.block_source_mode).map_err(|e| anyhow::anyhow!(e))?;
        let explorer_base_path = normalize_explorer_base_path(&file.explorer_base_path)?;
        let explorer_pizza_tv_endpoint =
            normalize_optional_string(Some(file.explorer_pizza_tv_endpoint))
                .unwrap_or_else(default_explorer_pizza_tv_endpoint);
        let explorer_amm_prefix = normalize_optional_string(Some(file.explorer_amm_prefix))
            .unwrap_or_else(default_explorer_amm_prefix);
        let explorer_networks = file.explorer_networks.and_then(|n| n.normalized());
        let google_analytics_tag = normalize_optional_string(file.google_analytics_tag);
        let sync_banner = file.sync_banner.and_then(|b| b.normalized());
        let debug_backup = file.debug_backup;
        let jemalloc_profile = file.jemalloc_profile.normalized()?;

        Ok(Self {
            readonly_metashrew_db_dir: file.readonly_metashrew_db_dir,
            electrum_rpc_url: normalize_optional_string(file.electrum_rpc_url),
            metashrew_rpc_url: file.metashrew_rpc_url,
            electrs_esplora_url: normalize_optional_string(file.electrs_esplora_url),
            bitcoind_rpc_url: file.bitcoind_rpc_url,
            bitcoind_rpc_user: file.bitcoind_rpc_user,
            bitcoind_rpc_pass: file.bitcoind_rpc_pass,
            b8_faucet_url: normalize_optional_string(file.b8_faucet_url),
            hosts: file.hosts.normalized(),
            bitcoind_blocks_dir: file.bitcoind_blocks_dir,
            reset_mempool_on_startup: file.reset_mempool_on_startup,
            rollback: file.rollback,
            view_only,
            db_path: file.db_path,
            db_cache: file.db_cache,
            alkabi_verify_trials: file.alkabi_verify_trials,
            sdb_poll_ms: file.sdb_poll_ms,
            indexer_block_delay_ms: file.indexer_block_delay_ms,
            port: file.port,
            explorer_host: file.explorer_host,
            explorer_espo_rpc_host: normalize_optional_string(file.explorer_espo_rpc_host),
            explorer_espo_rpc_key: normalize_optional_string(file.explorer_espo_rpc_key),
            explorer_espo_rpc_cache_ms: file.explorer_espo_rpc_cache_ms,
            enable_internal_rpc: file.enable_internal_rpc,
            internal_rpc_key: normalize_optional_string(file.internal_rpc_key),
            explorer_base_path,
            explorer_pizza_tv_endpoint,
            explorer_amm_prefix,
            sync_banner,
            network,
            metashrew_db_label: normalize_optional_string(file.metashrew_db_label),
            strict_mode: file.strict_mode,
            debug: file.debug,
            debug_ignore_ms: file.debug_ignore_ms,
            debug_backup,
            safe_tip_hook_script: normalize_optional_string(file.safe_tip_hook_script),
            block_source_mode,
            compact_tx_trace_rows: file.compact_tx_trace_rows,
            address_index_chunk_size: file.address_index_chunk_size,
            trace_read_workers: file.trace_read_workers,
            recover_missing_traces_by_txid: file.recover_missing_traces_by_txid,
            explorer_networks,
            google_analytics_tag,
            misc: file.misc,
            jemalloc_profile,
            mempool: file.mempool,
            modules: file.modules,
        })
    }
}

pub fn init_config_from(cfg: AppConfig) -> Result<()> {
    init_config_from_inner(cfg, false)
}

pub fn init_config_from_read_only(cfg: AppConfig) -> Result<()> {
    init_config_from_inner(cfg, true)
}

fn init_config_from_inner(cfg: AppConfig, espo_read_only: bool) -> Result<()> {
    let mut cfg = cfg;

    // --- validations ---
    let db = Path::new(&cfg.readonly_metashrew_db_dir);
    let metashrew_dir_ok = db.exists() && db.is_dir();
    if !metashrew_dir_ok {
        // A remote-explorer deployment (explorer_espo_rpc_host) may not have a
        // local metashrew database; SSR data comes from the remote espo, so a
        // missing metashrew dir is survivable there and fatal otherwise.
        if cfg.explorer_espo_rpc_host.is_some() {
            eprintln!(
                "[config] metashrew db dir missing or invalid ({}); continuing because explorer_espo_rpc_host is set — metashrew-backed features are disabled",
                cfg.readonly_metashrew_db_dir
            );
        } else if !db.exists() {
            anyhow::bail!("Database path does not exist: {}", cfg.readonly_metashrew_db_dir);
        } else {
            anyhow::bail!("Database path is not a directory: {}", cfg.readonly_metashrew_db_dir);
        }
    }

    if cfg.metashrew_rpc_url.trim().is_empty() {
        anyhow::bail!("metashrew_rpc_url must be provided");
    }

    if cfg.enable_internal_rpc && cfg.internal_rpc_key.is_none() {
        anyhow::bail!(
            "enable_internal_rpc requires internal_rpc_key: the internal.* storage methods must never run unauthenticated"
        );
    }
    if cfg.explorer_espo_rpc_host.is_some() && cfg.explorer_espo_rpc_key.is_none() {
        eprintln!(
            "[config] WARN: explorer_espo_rpc_host is set without explorer_espo_rpc_key; internal.* calls to the remote espo will be rejected unless it runs without a key"
        );
    }

    let db_root = Path::new(&cfg.db_path);
    if !db_root.exists() {
        fs::create_dir_all(db_root)
            .map_err(|e| anyhow::anyhow!("Failed to create db_path {}: {e}", cfg.db_path))?;
    } else if !db_root.is_dir() {
        anyhow::bail!("db_path is not a directory: {}", cfg.db_path);
    }

    let tmp = db_root.join("tmp");
    if !tmp.exists() {
        fs::create_dir_all(&tmp)
            .map_err(|e| anyhow::anyhow!("Failed to create tmp dbs dir {}: {e}", tmp.display()))?;
    } else if !tmp.is_dir() {
        anyhow::bail!("Temporary dbs dir is not a directory: {}", tmp.display());
    }

    if cfg.db_cache {
        let cache = db_root.join("cache");
        if !cache.exists() {
            fs::create_dir_all(&cache).map_err(|e| {
                anyhow::anyhow!("Failed to create cache db dir {}: {e}", cache.display())
            })?;
        } else if !cache.is_dir() {
            anyhow::bail!("Cache db path is not a directory: {}", cache.display());
        }
    }

    let espo_dir = db_root.join("espo");
    if !espo_dir.exists() {
        fs::create_dir_all(&espo_dir).map_err(|e| {
            anyhow::anyhow!("Failed to create espo db dir {}: {e}", espo_dir.display())
        })?;
    } else if !espo_dir.is_dir() {
        anyhow::bail!("espo db dir is not a directory: {}", espo_dir.display());
    }

    if cfg.block_source_mode != BlockFetchMode::RpcOnly {
        let blocks_dir = Path::new(&cfg.bitcoind_blocks_dir);
        if !blocks_dir.exists() {
            anyhow::bail!("bitcoind blocks dir does not exist: {}", cfg.bitcoind_blocks_dir);
        }
        if !blocks_dir.is_dir() {
            anyhow::bail!("bitcoind blocks dir is not a directory: {}", cfg.bitcoind_blocks_dir);
        }
    }

    if cfg.sdb_poll_ms == 0 {
        anyhow::bail!("sdb_poll_ms must be greater than 0");
    }
    if cfg.address_index_chunk_size == 0 {
        anyhow::bail!("address_index_chunk_size must be greater than 0");
    }
    if cfg.trace_read_workers == 0 {
        anyhow::bail!("trace_read_workers must be greater than 0");
    }
    if cfg.alkabi_verify_trials == 0 {
        anyhow::bail!("alkabi_verify_trials must be greater than 0");
    }
    if cfg.mempool.regtest_block_interval_secs == 0 {
        anyhow::bail!("mempool.regtest_block_interval_secs must be greater than 0");
    }

    cfg.explorer_base_path = normalize_explorer_base_path(&cfg.explorer_base_path)?;

    let electrum_url = cfg.electrum_rpc_url.clone().filter(|s| !s.is_empty());
    let esplora_url = cfg.electrs_esplora_url.clone().filter(|s| !s.is_empty());
    if electrum_url.is_none() && esplora_url.is_none() {
        anyhow::bail!("provide either electrum_rpc_url or electrs_esplora_url");
    }
    if electrum_url.is_some() && esplora_url.is_some() {
        eprintln!(
            "[config] both electrum rpc and electrs esplora URLs provided; electrum rpc will be used"
        );
    }

    // --- store config ---
    CONFIG
        .set(cfg.clone())
        .map_err(|_| anyhow::anyhow!("config already initialized"))?;

    // NEW: store global Network
    NETWORK
        .set(cfg.network)
        .map_err(|_| anyhow::anyhow!("network already initialized"))?;

    // --- init Electrum-like client once ---
    // SKIP if ESPO_SKIP_EXTERNAL_SERVICES env var is set (for testing)
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() {
        let electrum_like: Arc<dyn ElectrumLike> = if let Some(url) = electrum_url {
            let electrum_url = format!("tcp://{}", url);
            let client: Arc<Client> = Arc::new(Client::new(&electrum_url)?);
            ELECTRUM_CLIENT
                .set(client.clone())
                .map_err(|_| anyhow::anyhow!("electrum client already initialized"))?;
            Arc::new(ElectrumRpcClient::new(client))
        } else {
            let base =
                esplora_url.expect("validation ensures esplora url exists when electrum is None");
            Arc::new(EsploraElectrumLike::new(base)?)
        };
        ELECTRUM_LIKE
            .set(electrum_like)
            .map_err(|_| anyhow::anyhow!("electrum-like client already initialized"))?;
    }

    // --- init Bitcoin Core RPC client once ---
    // SKIP if ESPO_SKIP_EXTERNAL_SERVICES env var is set (for testing)
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() {
        let auth = if !cfg.bitcoind_rpc_user.is_empty() && !cfg.bitcoind_rpc_pass.is_empty() {
            Some((cfg.bitcoind_rpc_user.clone(), cfg.bitcoind_rpc_pass.clone()))
        } else {
            None
        };
        let core = CoreClient::new(&cfg.bitcoind_rpc_url, auth)?;
        BITCOIND_CLIENT
            .set(core)
            .map_err(|_| anyhow::anyhow!("bitcoind rpc client already initialized"))?;
    }

    // --- init Secondary RocksDB (SDB) once ---
    // SKIP if ESPO_SKIP_EXTERNAL_SERVICES env var is set (for testing)
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() && metashrew_dir_ok {
        let secondary_path = get_sdb_path_for_metashrew()?;
        match SDB::open(
            cfg.readonly_metashrew_db_dir.clone(),
            secondary_path,
            Duration::from_millis(cfg.sdb_poll_ms as u64),
        ) {
            Ok(sdb) => {
                METASHREW_SDB
                    .set(std::sync::Arc::new(sdb))
                    .map_err(|_| anyhow::anyhow!("metashrew SDB already initialized"))?;
            }
            // A remote-explorer deployment (explorer_espo_rpc_host) may not have
            // a local metashrew database; SSR data comes from the remote espo,
            // so a missing metashrew db is survivable there and fatal otherwise.
            Err(e) if cfg.explorer_espo_rpc_host.is_some() => {
                eprintln!(
                    "[config] metashrew db unavailable ({e:#}); continuing because explorer_espo_rpc_host is set — metashrew-backed features are disabled"
                );
            }
            Err(e) => return Err(e),
        }
    }

    // --- init ESPO RocksDB once ---
    let mut espo_opts = Options::default();
    configure_espo_rocksdb_options(&mut espo_opts);
    if !espo_read_only {
        espo_opts.create_if_missing(true);
    }
    let espo_path = Path::new(&cfg.db_path).join("espo");
    let espo_db = if espo_read_only {
        std::sync::Arc::new(DB::open_for_read_only(&espo_opts, espo_path, false)?)
    } else {
        std::sync::Arc::new(DB::open(&espo_opts, espo_path)?)
    };
    ESPO_DB
        .set(espo_db.clone())
        .map_err(|_| anyhow::anyhow!("ESPO DB already initialized"))?;

    if cfg.db_cache {
        let mut cache_opts = Options::default();
        cache_opts.create_if_missing(true);
        configure_cache_rocksdb_options(&mut cache_opts);
        let cache_path = Path::new(&cfg.db_path).join("cache");
        let cache_db = std::sync::Arc::new(DB::open(&cache_opts, &cache_path)?);
        CACHE_DB
            .set(cache_db)
            .map_err(|_| anyhow::anyhow!("Cache DB already initialized"))?;
        eprintln!("[cache] persistent cache DB enabled at {}", cache_path.display());
    }

    init_global_tree_db(espo_db.clone())?;

    // SKIP if ESPO_SKIP_EXTERNAL_SERVICES env var is set (for testing)
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() {
        init_block_source()?;
    }

    Ok(())
}

pub fn init_config() -> Result<()> {
    let cli = CliArgs::parse();
    let mut cfg = load_config_from_path(&cli.config_path, cli.view_only)?;
    if cli.rollback.is_some() {
        cfg.rollback = cli.rollback;
    }
    init_config_from(cfg)
}

pub fn load_config_from_path(path: &str, view_only: bool) -> Result<AppConfig> {
    let file = load_config_file(path)?;
    AppConfig::from_file(file, view_only)
}

// UPDATED: no param; uses global NETWORK
pub fn init_block_source() -> Result<()> {
    if BLOCK_SOURCE.get().is_some() {
        return Ok(());
    }
    let args = get_config();
    let network = get_network();
    let src = BlkOrRpcBlockSource::new_with_config(
        &args.bitcoind_blocks_dir,
        network,
        args.block_source_mode,
    )?;
    BLOCK_SOURCE
        .set(src)
        .map_err(|_| anyhow::anyhow!("block source already initialized"))?;
    Ok(())
}

pub fn get_config() -> &'static AppConfig {
    CONFIG.get().expect("init_config() must be called once at startup")
}

pub fn get_module_config(name: &str) -> Option<&'static serde_json::Value> {
    get_config().modules.get(name)
}

pub fn get_electrum_client() -> Option<Arc<Client>> {
    ELECTRUM_CLIENT.get().cloned()
}

pub fn get_electrum_like() -> Arc<dyn ElectrumLike> {
    ELECTRUM_LIKE
        .get()
        .expect("init_config() must be called once at startup")
        .clone()
}

pub fn get_bitcoind_rpc_client() -> &'static CoreClient {
    BITCOIND_CLIENT.get().expect("init_config() must be called once at startup")
}

/// Cloneable handle to the live secondary RocksDB
pub fn get_metashrew_sdb() -> std::sync::Arc<SDB> {
    std::sync::Arc::clone(
        METASHREW_SDB.get().expect("init_config() must be called once at startup"),
    )
}

/// Like `get_metashrew_sdb`, but survives deployments without a local
/// metashrew database (remote-explorer mode). Callers that can degrade
/// should prefer this.
pub fn try_get_metashrew_sdb() -> Option<std::sync::Arc<SDB>> {
    METASHREW_SDB.get().map(std::sync::Arc::clone)
}

/// Getter for the ESPO module DB path (directory for RocksDB)
pub fn get_espo_db_path() -> String {
    Path::new(&get_config().db_path).join("espo").to_string_lossy().into_owned()
}

/// Cloneable handle to the global ESPO RocksDB
pub fn get_espo_db() -> std::sync::Arc<DB> {
    std::sync::Arc::clone(ESPO_DB.get().expect("init_config() must be called once at startup"))
}

/// Optional writable cache database for derived, reproducible results.
pub fn get_cache_db() -> Option<std::sync::Arc<DB>> {
    CACHE_DB.get().map(std::sync::Arc::clone)
}

/// When set, the explorer fulfils its SSR data needs by calling this remote
/// espo's JSON-RPC endpoint instead of reading the local database.
pub fn get_explorer_espo_rpc_host() -> Option<&'static str> {
    get_config().explorer_espo_rpc_host.as_deref()
}

/// Whether this instance serves the `internal.*` storage-primitive RPC
/// methods that a remote explorer needs (see `explorer_espo_rpc_host`).
pub fn internal_rpc_enabled() -> bool {
    get_config().enable_internal_rpc
}

/// The shared key `internal.*` requests must carry. Always present when
/// `enable_internal_rpc` is true (enforced at startup).
pub fn internal_rpc_key() -> Option<&'static str> {
    get_config().internal_rpc_key.as_deref()
}

static EXPLORER_REMOTE_MDB_CLIENT: OnceLock<
    Option<std::sync::Arc<crate::runtime::remote_mdb::RemoteMdbClient>>,
> = OnceLock::new();

/// Shared remote-Mdb client for the explorer, present only when
/// `explorer_espo_rpc_host` is configured.
pub fn get_explorer_remote_mdb_client()
-> Option<std::sync::Arc<crate::runtime::remote_mdb::RemoteMdbClient>> {
    EXPLORER_REMOTE_MDB_CLIENT
        .get_or_init(|| {
            get_explorer_espo_rpc_host().map(|host| {
                eprintln!("[explorer] SSR data source: remote espo rpc at {host}");
                let cfg = get_config();
                std::sync::Arc::new(crate::runtime::remote_mdb::RemoteMdbClient::new_with(
                    host,
                    cfg.explorer_espo_rpc_key.clone(),
                    Duration::from_millis(cfg.explorer_espo_rpc_cache_ms),
                ))
            })
        })
        .clone()
}

/// The Mdb the explorer should use for a module namespace: remote-backed when
/// `explorer_espo_rpc_host` is configured, the local espo DB otherwise.
pub fn explorer_mdb(prefix: &[u8]) -> crate::runtime::mdb::Mdb {
    match get_explorer_remote_mdb_client() {
        Some(client) => crate::runtime::mdb::Mdb::remote(client, prefix),
        None => crate::runtime::mdb::Mdb::from_db(get_espo_db(), prefix),
    }
}

/// Indexed height bounds for explorer views: the remote espo's bounds when
/// `explorer_espo_rpc_host` is configured, the local versioned tree otherwise.
pub fn explorer_indexed_height_bounds() -> Option<(u32, u32)> {
    match get_explorer_remote_mdb_client() {
        Some(client) => client.indexed_height_bounds().ok().flatten(),
        None => crate::runtime::tree_db::get_global_tree_db()
            .and_then(|db| db.indexed_height_bounds().ok().flatten()),
    }
}

/// Global accessor for the block source (blk files + RPC fallback)
pub fn get_block_source() -> &'static BlkOrRpcBlockSource {
    BLOCK_SOURCE
        .get()
        .expect("init_block_source() must be called after init_config()")
}

/// NEW: Global accessor for bitcoin::Network
pub fn get_network() -> Network {
    *NETWORK.get().expect("init_config() must set NETWORK")
}

pub fn compact_tx_trace_rows_enabled() -> bool {
    get_config().compact_tx_trace_rows
}

pub fn get_address_index_chunk_size() -> usize {
    get_config().address_index_chunk_size.max(1) as usize
}

pub fn get_trace_read_workers() -> usize {
    std::env::var("ESPO_TRACE_READ_WORKERS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|workers| *workers > 0)
        .unwrap_or_else(|| get_config().trace_read_workers.max(1) as usize)
}

pub fn recover_missing_traces_by_txid() -> bool {
    get_config().recover_missing_traces_by_txid
}

pub fn is_strict_mode() -> bool {
    get_config()
        .strict_mode
        .as_ref()
        .map(|cfg| cfg.check_utxos || cfg.check_alkane_balances || cfg.check_trace_mismatches)
        .unwrap_or(false)
}

pub fn strict_check_utxos() -> bool {
    get_config().strict_mode.as_ref().map(|cfg| cfg.check_utxos).unwrap_or(false)
}

pub fn strict_check_alkane_balances() -> bool {
    get_config()
        .strict_mode
        .as_ref()
        .map(|cfg| cfg.check_alkane_balances)
        .unwrap_or(false)
}

pub fn strict_check_trace_mismatches() -> bool {
    get_config()
        .strict_mode
        .as_ref()
        .map(|cfg| cfg.check_trace_mismatches)
        .unwrap_or(false)
}

pub fn debug_enabled() -> bool {
    CONFIG.get().map(|cfg| cfg.debug).unwrap_or(false)
}

pub fn debug_ignore_ms() -> u64 {
    CONFIG.get().map(|cfg| cfg.debug_ignore_ms).unwrap_or(0)
}

pub fn get_metashrew() -> MetashrewAdapter {
    let label = get_config().metashrew_db_label.clone();

    MetashrewAdapter::new(label)
}

pub fn get_metashrew_rpc_url() -> &'static str {
    &get_config().metashrew_rpc_url
}

pub fn get_explorer_base_path() -> &'static str {
    &get_config().explorer_base_path
}

pub fn get_explorer_pizza_tv_endpoint() -> &'static str {
    &get_config().explorer_pizza_tv_endpoint
}

pub fn get_explorer_amm_prefix() -> &'static str {
    &get_config().explorer_amm_prefix
}

pub fn get_sync_banner() -> Option<&'static SyncBannerConfig> {
    get_config().sync_banner.as_ref()
}

pub fn get_explorer_networks() -> Option<&'static ExplorerNetworks> {
    get_config().explorer_networks.as_ref()
}

pub fn get_google_analytics_tag() -> Option<&'static str> {
    get_config().google_analytics_tag.as_deref()
}

pub fn show_terminal_ad() -> bool {
    get_config().misc.show_terminal_ad
}

pub fn get_espo_next_height() -> u32 {
    ESPO_HEIGHT
        .get()
        .expect("indexer must be initialized before calling get_espo_next_height")
        .load(Ordering::Relaxed)
}

pub fn get_espo_indexed_height() -> Option<u32> {
    ESPO_HEIGHT.get().map(|cell| cell.load(Ordering::Relaxed).saturating_sub(1))
}

pub fn update_safe_tip(height: u32) {
    let cell = SAFE_TIP.get_or_init(|| Arc::new(AtomicU32::new(height)));
    cell.store(height, Ordering::Relaxed);
}

pub fn get_last_safe_tip() -> Option<u32> {
    SAFE_TIP.get().map(|cell| cell.load(Ordering::Relaxed))
}
