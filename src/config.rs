use crate::alkanes::metashrew::MetashrewAdapter;
use crate::runtime::{
    dbpaths::get_sdb_path_for_metashrew, mdb::Mdb, sdb::SDB, tree_db::VersionedTreeDb,
};
use crate::utils::electrum_like::{ElectrumLike, ElectrumRpcClient, EsploraElectrumLike};
use crate::{ESPO_HEIGHT, SAFE_TIP};
use anyhow::{Context, Result};
use clap::Parser;
use electrum_client::Client;
use rocksdb::{DB, Options};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{
    fs,
    path::Path,
    sync::{Arc, OnceLock, RwLock},
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
static ESPO_MODULE_MDBS: OnceLock<RwLock<HashMap<String, Arc<Mdb>>>> = OnceLock::new();
static BLOCK_SOURCE: OnceLock<BlkOrRpcBlockSource> = OnceLock::new();

// NEW: Global bitcoin::Network
static NETWORK: OnceLock<Network> = OnceLock::new();

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

fn default_sdb_poll_ms() -> u16 {
    5000
}

fn default_port() -> u16 {
    8080
}

fn default_explorer_base_path() -> String {
    "/".to_string()
}

fn default_explorer_pizza_tv_endpoint() -> String {
    "https://tv.pizza.fun".to_string()
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

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
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
    #[serde(default = "default_bitcoind_blocks_dir")]
    pub bitcoind_blocks_dir: String,
    #[serde(default)]
    pub reset_mempool_on_startup: bool,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_sdb_poll_ms")]
    pub sdb_poll_ms: u16,
    #[serde(default)]
    pub indexer_block_delay_ms: u64,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub explorer_host: Option<SocketAddr>,
    #[serde(default = "default_explorer_base_path")]
    pub explorer_base_path: String,
    #[serde(default = "default_explorer_pizza_tv_endpoint")]
    pub explorer_pizza_tv_endpoint: String,
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
    #[serde(default)]
    pub explorer_networks: Option<ExplorerNetworks>,
    #[serde(default)]
    pub google_analytics_tag: Option<String>,
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
    pub bitcoind_blocks_dir: String,
    pub reset_mempool_on_startup: bool,
    pub view_only: bool,
    pub db_path: String,
    pub sdb_poll_ms: u16,
    pub indexer_block_delay_ms: u64,
    pub port: u16,
    pub explorer_host: Option<SocketAddr>,
    pub explorer_base_path: String,
    pub explorer_pizza_tv_endpoint: String,
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
    pub explorer_networks: Option<ExplorerNetworks>,
    pub google_analytics_tag: Option<String>,
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
        let explorer_networks = file.explorer_networks.and_then(|n| n.normalized());
        let google_analytics_tag = normalize_optional_string(file.google_analytics_tag);
        let debug_backup = file.debug_backup;

        Ok(Self {
            readonly_metashrew_db_dir: file.readonly_metashrew_db_dir,
            electrum_rpc_url: normalize_optional_string(file.electrum_rpc_url),
            metashrew_rpc_url: file.metashrew_rpc_url,
            electrs_esplora_url: normalize_optional_string(file.electrs_esplora_url),
            bitcoind_rpc_url: file.bitcoind_rpc_url,
            bitcoind_rpc_user: file.bitcoind_rpc_user,
            bitcoind_rpc_pass: file.bitcoind_rpc_pass,
            bitcoind_blocks_dir: file.bitcoind_blocks_dir,
            reset_mempool_on_startup: file.reset_mempool_on_startup,
            view_only,
            db_path: file.db_path,
            sdb_poll_ms: file.sdb_poll_ms,
            indexer_block_delay_ms: file.indexer_block_delay_ms,
            port: file.port,
            explorer_host: file.explorer_host,
            explorer_base_path,
            explorer_pizza_tv_endpoint,
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
            explorer_networks,
            google_analytics_tag,
            modules: file.modules,
        })
    }
}

pub fn init_config_from(cfg: AppConfig) -> Result<()> {
    let mut cfg = cfg;

    // --- validations ---
    let db = Path::new(&cfg.readonly_metashrew_db_dir);
    if !db.exists() {
        anyhow::bail!("Database path does not exist: {}", cfg.readonly_metashrew_db_dir);
    }
    if !db.is_dir() {
        anyhow::bail!("Database path is not a directory: {}", cfg.readonly_metashrew_db_dir);
    }

    if cfg.metashrew_rpc_url.trim().is_empty() {
        anyhow::bail!("metashrew_rpc_url must be provided");
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

    let espo_root = db_root.join("espo");
    if !espo_root.exists() {
        fs::create_dir_all(&espo_root).map_err(|e| {
            anyhow::anyhow!("Failed to create espo db root {}: {e}", espo_root.display())
        })?;
    } else if !espo_root.is_dir() {
        anyhow::bail!("espo db root is not a directory: {}", espo_root.display());
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
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() {
        let secondary_path = get_sdb_path_for_metashrew()?;
        let sdb = SDB::open(
            cfg.readonly_metashrew_db_dir.clone(),
            secondary_path,
            Duration::from_millis(cfg.sdb_poll_ms as u64),
        )?;
        METASHREW_SDB
            .set(std::sync::Arc::new(sdb))
            .map_err(|_| anyhow::anyhow!("metashrew SDB already initialized"))?;
    }

    // --- init ESPO RocksDB once ---
    let mut espo_opts = Options::default();
    espo_opts.create_if_missing(true);
    let espo_path = Path::new(&cfg.db_path).join("espo").join("_shared");
    let espo_db = std::sync::Arc::new(DB::open(&espo_opts, espo_path)?);
    ESPO_DB
        .set(espo_db.clone())
        .map_err(|_| anyhow::anyhow!("ESPO DB already initialized"))?;
    let _ = ESPO_MODULE_MDBS.set(RwLock::new(HashMap::new()));

    // SKIP if ESPO_SKIP_EXTERNAL_SERVICES env var is set (for testing)
    if std::env::var("ESPO_SKIP_EXTERNAL_SERVICES").is_err() {
        init_block_source()?;
    }

    Ok(())
}

pub fn init_config() -> Result<()> {
    let cli = CliArgs::parse();
    let cfg = load_config_from_path(&cli.config_path, cli.view_only)?;
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

/// Getter for the ESPO module DB path (directory for RocksDB)
pub fn get_espo_db_path() -> String {
    Path::new(&get_config().db_path)
        .join("espo")
        .join("_shared")
        .to_string_lossy()
        .into_owned()
}

pub fn get_espo_root_path() -> String {
    Path::new(&get_config().db_path).join("espo").to_string_lossy().into_owned()
}

pub fn get_espo_module_db_path(name: &str) -> String {
    Path::new(&get_espo_root_path()).join(name).to_string_lossy().into_owned()
}

/// Cloneable handle to the global ESPO RocksDB
pub fn get_espo_db() -> std::sync::Arc<DB> {
    std::sync::Arc::clone(ESPO_DB.get().expect("init_config() must be called once at startup"))
}

pub fn get_espo_module_mdb(name: &str) -> Arc<Mdb> {
    let map = ESPO_MODULE_MDBS.get_or_init(|| RwLock::new(HashMap::new()));
    if let Some(existing) = map.read().expect("module mdb map poisoned").get(name).cloned() {
        return existing;
    }

    let mut write = map.write().expect("module mdb map poisoned");
    if let Some(existing) = write.get(name).cloned() {
        return existing;
    }

    let path = Path::new(&get_espo_root_path()).join(name);
    fs::create_dir_all(&path)
        .unwrap_or_else(|e| panic!("failed to create module db dir {}: {e}", path.display()));

    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(
        DB::open(&opts, &path)
            .unwrap_or_else(|e| panic!("failed to open module db {}: {e}", path.display())),
    );
    let tree = Arc::new(
        VersionedTreeDb::new(Arc::clone(&db))
            .unwrap_or_else(|e| panic!("failed to init module tree {}: {e}", path.display())),
    );
    let mdb = Arc::new(Mdb::from_db_with_tree(db, b"", tree));
    write.insert(name.to_string(), Arc::clone(&mdb));
    mdb
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

pub fn get_explorer_networks() -> Option<&'static ExplorerNetworks> {
    get_config().explorer_networks.as_ref()
}

pub fn get_google_analytics_tag() -> Option<&'static str> {
    get_config().google_analytics_tag.as_deref()
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
