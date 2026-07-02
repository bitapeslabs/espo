// Module declarations - these reference lib.rs modules indirectly
#[cfg(not(target_arch = "wasm32"))]
pub mod alkanes;
#[cfg(not(target_arch = "wasm32"))]
pub mod bitcoind_flexible;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
pub mod consts;
#[cfg(not(target_arch = "wasm32"))]
pub mod core;
#[cfg(not(target_arch = "wasm32"))]
pub mod debug;
#[cfg(not(target_arch = "wasm32"))]
pub mod explorer;
#[cfg(not(target_arch = "wasm32"))]
pub mod modules;
#[cfg(not(target_arch = "wasm32"))]
pub mod runtime;
#[cfg(not(target_arch = "wasm32"))]
pub mod schemas;
#[cfg(not(target_arch = "wasm32"))]
pub mod utils;

#[cfg(all(not(target_arch = "wasm32"), feature = "jemalloc-prof"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use crate::config::{DebugBackupConfig, get_block_source, init_block_source};
//modules
use crate::config::get_metashrew_sdb;
use crate::config::get_network;
use crate::modules::ammdata::main::AmmData;
use crate::modules::essentials::main::Essentials;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetBlockSummaryParams, cache_block_summary, preload_block_summary_cache,
};
use crate::modules::memgraph::main::Memgraph;
use crate::modules::oylapi::main::OylApi;
use crate::modules::pizzafun::main::Pizzafun;
use crate::modules::runes::main::{Runes, runes_enabled_from_global_config};
use crate::modules::runes::storage::RunesProvider;
use crate::modules::subfrost::main::Subfrost;
use crate::modules::tokendata::main::TokenData;
use crate::utils::{EtaTracker, fmt_duration};
use anyhow::{Context, Result};

use crate::core::blockfetcher::BlockSource;
use crate::explorer::run_explorer;
use crate::{
    alkanes::{
        trace::{EspoAlkanesTransaction, EspoBlock, get_espo_block},
        utils::get_safe_tip,
    },
    config::{
        get_bitcoind_rpc_client, get_config, get_espo_db, get_module_config, init_config,
        update_safe_tip,
    },
    consts::alkanes_genesis_block,
    modules::defs::ModuleRegistry,
    runtime::mdb::Mdb,
    runtime::mempool::{
        publish_confirmed_tx_events, publish_new_block_event, purge_confirmed_from_chain,
        purge_confirmed_txids, reset_mempool_store, run_mempool_service,
    },
    runtime::rpc::run_rpc,
    runtime::shutdown::request_shutdown,
    runtime::state_at::StateAt,
    runtime::tree_db::get_global_tree_db,
};
use bitcoin::hashes::Hash;
use bitcoin::{Address, Txid};
use bitcoincore_rpc::RpcApi;
pub use espo::{ESPO_HEIGHT, SAFE_TIP};
use rocksdb::checkpoint::Checkpoint;
use tokio::runtime::Builder as TokioBuilder;

const NO_REWIND: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetashrewCanonicalityWaitKind {
    TipBehind,
    MissingHash,
    HashMismatch,
}

impl MetashrewCanonicalityWaitKind {
    fn as_str(self) -> &'static str {
        match self {
            MetashrewCanonicalityWaitKind::TipBehind => "metashrew_tip_behind",
            MetashrewCanonicalityWaitKind::MissingHash => "metashrew_missing_height_hash",
            MetashrewCanonicalityWaitKind::HashMismatch => "metashrew_hash_mismatch",
        }
    }
}

struct AtomicFlagGuard {
    flag: Arc<AtomicBool>,
}

impl AtomicFlagGuard {
    fn new(flag: &Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self { flag: flag.clone() }
    }
}

impl Drop for AtomicFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct CanonicalityWaitTracker {
    last_height: Option<u32>,
    last_kind: Option<MetashrewCanonicalityWaitKind>,
    attempts: u32,
}

impl CanonicalityWaitTracker {
    fn bump(&mut self, height: u32, kind: MetashrewCanonicalityWaitKind) -> u32 {
        if self.last_height == Some(height) && self.last_kind == Some(kind) {
            self.attempts = self.attempts.saturating_add(1);
        } else {
            self.last_height = Some(height);
            self.last_kind = Some(kind);
            self.attempts = 1;
        }
        self.attempts
    }

    fn reset(&mut self) {
        self.last_height = None;
        self.last_kind = None;
        self.attempts = 0;
    }
}

fn classify_metashrew_canonicality_wait(
    err: &anyhow::Error,
) -> Option<MetashrewCanonicalityWaitKind> {
    for cause in err.chain() {
        let message = cause.to_string();
        if message.contains("metashrew tip ") && message.contains(" is behind required height ") {
            return Some(MetashrewCanonicalityWaitKind::TipBehind);
        }
        if message.contains("metashrew missing stored block hash at height ") {
            return Some(MetashrewCanonicalityWaitKind::MissingHash);
        }
        if message.contains("metashrew hash mismatch at height ") {
            return Some(MetashrewCanonicalityWaitKind::HashMismatch);
        }
    }
    None
}

fn canonicality_retry_delay(attempt: u32) -> Duration {
    if attempt >= 8 {
        Duration::from_secs(15)
    } else if attempt >= 4 {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(5)
    }
}

fn should_log_canonicality_wait(attempt: u32) -> bool {
    attempt <= 3 || attempt.is_power_of_two()
}

fn log_canonicality_wait(
    stage: &str,
    height: u32,
    kind: MetashrewCanonicalityWaitKind,
    attempt: u32,
    retry_delay: Duration,
    err: &anyhow::Error,
) {
    eprintln!(
        "[reorg_wait] stage={} height={} reason={} attempt={} retry_in={} detail={}",
        stage,
        height,
        kind.as_str(),
        attempt,
        fmt_duration(retry_delay),
        err
    );
}

fn set_rewind_target(rewind_target: &AtomicU32, divergence_height: u32) -> bool {
    let mut current = rewind_target.load(Ordering::Relaxed);
    while divergence_height < current {
        match rewind_target.compare_exchange(
            current,
            divergence_height,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
    false
}

fn rewind_tree_to_before(next_height: u32) -> Result<()> {
    let Some(tree) = get_global_tree_db() else {
        return Ok(());
    };

    let target_height = match next_height.checked_sub(1) {
        Some(parent_height) => match tree.indexed_height_bounds()? {
            Some((first_height, _)) if parent_height >= first_height => Some(parent_height),
            _ => None,
        },
        None => None,
    };

    tree.rewind_to_height(target_height)
        .with_context(|| format!("failed to rewind versioned tree before height {next_height}"))?;
    Ok(())
}

fn handle_reorg_switch(mods: &ModuleRegistry, next_height: u32) -> Result<()> {
    for m in mods.modules() {
        m.preflight_reorg(next_height).with_context(|| {
            format!("module {} cannot roll back to height {next_height}", m.get_name())
        })?;
    }
    rewind_tree_to_before(next_height)?;
    for m in mods.modules() {
        m.handle_reorg(next_height).with_context(|| {
            format!("module {} failed to handle reorg to height {next_height}", m.get_name())
        })?;
    }
    for m in mods.modules() {
        let Some(height) = m.get_index_height() else {
            continue;
        };
        if height >= next_height {
            anyhow::bail!(
                "module {} still reports index height {} after reorg to next_height {}",
                m.get_name(),
                height,
                next_height
            );
        }
    }
    Ok(())
}

fn rollback_failed_block(mods: &ModuleRegistry, next_height: u32) -> Result<()> {
    if let Some(tree) = get_global_tree_db() {
        tree.abort_block();
    }
    handle_reorg_switch(mods, next_height)
}

fn run_debug_backup(db_path: &str, backup: &DebugBackupConfig, block: u32) -> std::io::Result<()> {
    let db_root = Path::new(db_path);
    let backup_root = Path::new(&backup.dir);
    if backup_root.starts_with(db_root) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "debug_backup.dir may not be inside db_path",
        ));
    }
    std::fs::create_dir_all(backup_root)?;
    let dest_dir = backup_root.join(format!("bkp-{block}"));
    if dest_dir.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("backup destination already exists: {}", dest_dir.display()),
        ));
    }
    eprintln!("[debug_backup] starting copy: '{}' -> '{}'", db_root.display(), dest_dir.display());
    copy_debug_backup_tree(db_root, &dest_dir)?;
    eprintln!("[debug_backup] finished copy to '{}'", dest_dir.display());
    Ok(())
}

fn copy_debug_backup_tree(src_root: &Path, dest_root: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dest_root)?;
    for entry in std::fs::read_dir(src_root)? {
        let entry = entry?;
        let src_path = entry.path();
        let dest_path = dest_root.join(entry.file_name());
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            if entry.file_name() == "espo" {
                checkpoint_espo_db(&dest_path)?;
            } else if entry.file_name() == "tmp" {
                // Secondary RocksDB state is derived from the primary metashrew DB.
                // Recreate the directory structure instead of copying a live secondary.
                std::fs::create_dir_all(&dest_path)?;
            } else {
                copy_dir_recursive(&src_path, &dest_path)?;
            }
        } else if file_type.is_file() {
            std::fs::copy(&src_path, &dest_path)?;
        }
    }
    Ok(())
}

fn copy_dir_recursive(src_dir: &Path, dest_dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dest_dir)?;
    for entry in std::fs::read_dir(src_dir)? {
        let entry = entry?;
        let src_path = entry.path();
        let dest_path = dest_dir.join(entry.file_name());
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dest_path)?;
        } else if file_type.is_file() {
            std::fs::copy(&src_path, &dest_path)?;
        }
    }
    Ok(())
}

fn checkpoint_espo_db(dest_dir: &Path) -> std::io::Result<()> {
    let espo_db = get_espo_db();
    espo_db
        .flush_wal(true)
        .map_err(|e| std::io::Error::other(format!("failed to flush espo WAL: {e}")))?;
    espo_db
        .flush()
        .map_err(|e| std::io::Error::other(format!("failed to flush espo memtables: {e}")))?;

    let checkpoint = Checkpoint::new(espo_db.as_ref())
        .map_err(|e| std::io::Error::other(format!("failed to create espo checkpoint: {e}")))?;
    checkpoint
        .create_checkpoint(dest_dir)
        .map_err(|e| std::io::Error::other(format!("failed to write espo checkpoint: {e}")))?;
    Ok(())
}

fn detect_first_divergence_height(
    indexed_tip: u32,
    active_tip: u32,
    genesis_height: u32,
) -> Option<u32> {
    let Some(tree) = get_global_tree_db() else { return None };
    let check_tip = indexed_tip.min(active_tip);
    if check_tip < genesis_height {
        return None;
    }
    let rpc = get_bitcoind_rpc_client();

    let mut h = check_tip;
    loop {
        let chain_hash = match rpc.get_block_hash(h as u64) {
            Ok(hash) => hash,
            Err(e) => {
                eprintln!("[reorg] failed to fetch chain hash at {}: {e:?}", h);
                return None;
            }
        };
        let indexed_hash = match tree.blockhash_for_height(h) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[reorg] failed to read indexed hash at {}: {e:?}", h);
                return None;
            }
        };

        if matches!(indexed_hash, Some(stored) if stored == chain_hash) {
            if h == check_tip {
                return None;
            }
            return Some(h.saturating_add(1));
        }

        if h == genesis_height {
            return Some(genesis_height);
        }
        h = h.saturating_sub(1);
    }
}

fn get_core_tip_height() -> Result<u32> {
    let tip = get_bitcoind_rpc_client().get_block_count().context("bitcoind getblockcount")?;
    u32::try_from(tip).context("bitcoind height does not fit in u32")
}

async fn run_reorg_poller(
    rewind_target: Arc<AtomicU32>,
    shutdown_requested: Arc<AtomicBool>,
    genesis_height: u32,
) {
    const REORG_POLL_INTERVAL: Duration = Duration::from_secs(10);

    loop {
        if shutdown_requested.load(Ordering::Relaxed) {
            break;
        }

        match get_safe_tip() {
            Ok(h) => update_safe_tip(h),
            Err(e) => eprintln!("[reorg] failed to fetch safe tip: {e:?}"),
        }
        let core_tip = match get_core_tip_height() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[reorg] failed to fetch core tip: {e:?}");
                tokio::time::sleep(REORG_POLL_INTERVAL).await;
                continue;
            }
        };

        let indexed_tip = ESPO_HEIGHT
            .get()
            .map(|h| h.load(Ordering::Relaxed).saturating_sub(1))
            .unwrap_or(genesis_height.saturating_sub(1));

        if let Some(divergence_height) =
            detect_first_divergence_height(indexed_tip, core_tip, genesis_height)
        {
            if set_rewind_target(&rewind_target, divergence_height) {
                eprintln!(
                    "[reorg] detected divergence at height {} (indexed_tip={}, core_tip={})",
                    divergence_height, indexed_tip, core_tip
                );
            }
        }

        tokio::time::sleep(REORG_POLL_INTERVAL).await;
    }
}

fn run_safe_tip_hook(script: &str, next_height: u32, tip: u32) {
    let script = script.trim();
    if script.is_empty() {
        return;
    }
    let script = script.to_string();
    std::thread::spawn(move || {
        eprintln!("[safe_tip_hook] running (next_height={}, tip={}): {}", next_height, tip, script);
        match Command::new("sh").arg("-c").arg(&script).status() {
            Ok(status) => eprintln!("[safe_tip_hook] finished: {}", status),
            Err(e) => eprintln!("[safe_tip_hook] failed: {e:?}"),
        }
    });
}

fn get_indexer_block(height: u32, tip: u32, network: bitcoin::Network) -> Result<EspoBlock> {
    let alkane_genesis = alkanes_genesis_block(network);
    if height >= alkane_genesis {
        return get_espo_block(height.into(), tip.into());
    }

    eprintln!(
        "[indexer] loading raw pre-alkane block #{} without traces (alkane_genesis={})",
        height, alkane_genesis
    );
    let block_result = get_block_source()
        .get_block_result_by_height(height, tip)
        .with_context(|| format!("BlockSource: get raw pre-alkane block {height}"))?;
    let tx_count = block_result.block.txdata.len();
    Ok(EspoBlock {
        is_latest: height == tip,
        height,
        block_header: block_result.block.header,
        host_function_values: (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        fee_summary: block_result.fee_summary,
        tx_count,
        transactions: block_result
            .block
            .txdata
            .into_iter()
            .map(|transaction| EspoAlkanesTransaction { traces: None, transaction })
            .collect(),
    })
}

fn update_indexed_block_interaction_summary(
    essentials_provider: &EssentialsProvider,
    runes_provider: Option<&RunesProvider>,
    block: &EspoBlock,
) -> Result<()> {
    let Some(mut summary) = essentials_provider
        .get_block_summary(GetBlockSummaryParams {
            blockhash: StateAt::Latest,
            height: block.height,
        })?
        .summary
    else {
        return Ok(());
    };

    let mut interaction_txids: std::collections::HashSet<Txid> = std::collections::HashSet::new();
    for tx in block.transactions.iter() {
        if tx.traces.as_ref().map(|traces| !traces.is_empty()).unwrap_or(false) {
            interaction_txids.insert(tx.transaction.compute_txid());
        }
    }
    if let Some(provider) = runes_provider {
        let total = provider.get_block_tx_count(block.height as u64).unwrap_or(0);
        if total > 0 {
            for pointer in
                provider.get_block_tx_range(block.height as u64, 0, total).unwrap_or_default()
            {
                interaction_txids.insert(Txid::from_byte_array(pointer.txid));
            }
        }
    }
    let interaction_count = interaction_txids.len().min(u32::MAX as usize) as u32;

    if summary.interaction_count != interaction_count {
        summary.interaction_count = interaction_count;
        essentials_provider.update_block_summary_by_hash(&summary)?;
        cache_block_summary(block.height, summary);
    }

    Ok(())
}

fn block_output_address_txs(
    block: &EspoBlock,
    network: bitcoin::Network,
) -> std::collections::HashMap<String, Vec<Txid>> {
    let mut out: std::collections::HashMap<String, Vec<Txid>> = std::collections::HashMap::new();
    let block_txs: std::collections::HashMap<Txid, &bitcoin::Transaction> = block
        .transactions
        .iter()
        .map(|tx| (tx.transaction.compute_txid(), &tx.transaction))
        .collect();
    for tx in &block.transactions {
        let txid = tx.transaction.compute_txid();
        let mut seen_in_tx = std::collections::HashSet::new();
        for input in &tx.transaction.input {
            if input.previous_output.is_null() {
                continue;
            }
            let Some(prev_tx) = block_txs.get(&input.previous_output.txid) else {
                continue;
            };
            let Some(prevout) = prev_tx.output.get(input.previous_output.vout as usize) else {
                continue;
            };
            let Ok(address) = Address::from_script(prevout.script_pubkey.as_script(), network)
            else {
                continue;
            };
            let address = address.to_string();
            if seen_in_tx.insert(address.clone()) {
                out.entry(address).or_default().push(txid);
            }
        }
        for output in &tx.transaction.output {
            let Ok(address) = Address::from_script(output.script_pubkey.as_script(), network)
            else {
                continue;
            };
            let address = address.to_string();
            if seen_in_tx.insert(address.clone()) {
                out.entry(address).or_default().push(txid);
            }
        }
    }
    out
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm =
        signal(SignalKind::terminate()).context("failed to register SIGTERM handler")?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().context("failed to wait for shutdown signal")?;
    Ok(())
}

async fn run_indexer_loop(
    mods: ModuleRegistry,
    start_height: u32,
    mut next_height: u32,
    network: bitcoin::Network,
    metashrew_sdb: std::sync::Arc<crate::runtime::sdb::SDB>,
    cfg: crate::config::AppConfig,
    shutdown_requested: Arc<AtomicBool>,
    db_write_active: Arc<AtomicBool>,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    let genesis_height = alkanes_genesis_block(network);
    let stop_after_block = std::env::var("ESPO_STOP_AFTER_BLOCK")
        .ok()
        .and_then(|value| value.parse::<u32>().ok());
    let rewind_target = Arc::new(AtomicU32::new(NO_REWIND));
    let essentials_provider =
        EssentialsProvider::new(Arc::new(Mdb::from_db(get_espo_db(), b"essentials:")));
    let runes_summary_provider = runes_enabled_from_global_config()
        .then(|| RunesProvider::new(Arc::new(Mdb::from_db(get_espo_db(), b"runes:"))));
    let mut last_tip: Option<u32> = None;
    let mut mempool_started = false;
    let mut logged_start = false;
    let mut safe_tip_hook_ran = false;
    let mut safe_tip_waits = CanonicalityWaitTracker::default();
    let mut block_waits = CanonicalityWaitTracker::default();
    if cfg.reset_mempool_on_startup {
        if let Err(e) = reset_mempool_store() {
            eprintln!("[mempool] failed to reset store on startup: {e:?}");
        }
    }
    if let Err(e) = purge_confirmed_from_chain() {
        eprintln!("[mempool] failed to purge confirmed txs on startup: {e:?}");
    }

    // ETA tracker
    let mut eta = EtaTracker::new(3.0); // EMA smoothing factor (tweak if you want faster/slower adaptation)
    let mut debug_backup_remaining: std::collections::HashSet<u32> = cfg
        .debug_backup
        .as_ref()
        .map(|backup| backup.blocks.iter().copied().collect())
        .unwrap_or_default();

    {
        let shutdown_for_poller = shutdown_requested.clone();
        let rewind_target_for_poller = rewind_target.clone();
        tokio::spawn(async move {
            eprintln!("[reorg] poller started (10s cadence)");
            run_reorg_poller(rewind_target_for_poller, shutdown_for_poller, genesis_height).await;
        });
    }

    loop {
        if shutdown_requested.load(Ordering::Relaxed) {
            break;
        }

        let requested_rewind = rewind_target.swap(NO_REWIND, Ordering::SeqCst);
        if requested_rewind != NO_REWIND && requested_rewind < next_height {
            if let Err(e) = handle_reorg_switch(&mods, requested_rewind) {
                eprintln!("[reorg] failed to switch indexer to height {}: {e:?}", requested_rewind);
                shutdown_requested.store(true, Ordering::Relaxed);
                break;
            }
            next_height = requested_rewind;
            if let Some(h) = ESPO_HEIGHT.get() {
                h.store(next_height, Ordering::Relaxed);
            }
            if let Err(e) = reset_mempool_store() {
                eprintln!("[mempool] failed to reset store after reorg switch: {e:?}");
            }
            eprintln!("[reorg] switching indexer to height {}", next_height);
        }

        if let Err(e) = metashrew_sdb.catch_up_now() {
            eprintln!("[indexer] metashrew catch_up before tip fetch: {e:?}");
        }

        let tip = match get_safe_tip() {
            Ok(h) => h,
            Err(e) => {
                if let Some(kind) = classify_metashrew_canonicality_wait(&e) {
                    let attempt = safe_tip_waits.bump(next_height, kind);
                    let retry_delay = canonicality_retry_delay(attempt);
                    if should_log_canonicality_wait(attempt) {
                        log_canonicality_wait(
                            "safe_tip",
                            next_height,
                            kind,
                            attempt,
                            retry_delay,
                            &e,
                        );
                    }
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
                safe_tip_waits.reset();
                eprintln!("[indexer] failed to fetch safe tip: {e:?}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };
        safe_tip_waits.reset();
        update_safe_tip(tip);
        let core_tip = match get_core_tip_height() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[indexer] failed to fetch core tip for reorg check: {e:?}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };

        let indexed_tip = ESPO_HEIGHT
            .get()
            .map(|h| h.load(Ordering::Relaxed).saturating_sub(1))
            .unwrap_or(genesis_height.saturating_sub(1));
        if let Some(divergence_height) =
            detect_first_divergence_height(indexed_tip, core_tip, genesis_height)
        {
            if divergence_height < next_height {
                if set_rewind_target(&rewind_target, divergence_height) {
                    eprintln!(
                        "[reorg] detected divergence at height {} before indexing (indexed_tip={}, safe_tip={}, core_tip={})",
                        divergence_height, indexed_tip, tip, core_tip
                    );
                }
                continue;
            }
        }

        let target_tip = stop_after_block.unwrap_or(tip);
        if stop_after_block.is_some_and(|end| next_height > end) {
            eprintln!(
                "[indexer] reached configured stop block {}; shutting down indexer",
                next_height.saturating_sub(1)
            );
            shutdown_requested.store(true, Ordering::Relaxed);
            break;
        }
        if let Some(prev_tip) = last_tip {
            if tip > prev_tip {
                if let Err(e) = metashrew_sdb.catch_up_now() {
                    eprintln!(
                        "[indexer] metashrew catch_up after new tip {} (prev {}) detected: {e:?}",
                        tip, prev_tip
                    );
                }
            }
        }
        last_tip = Some(tip);

        if next_height == start_height && !logged_start {
            let remaining = target_tip.saturating_sub(next_height) + 1;
            let eta_str = fmt_duration(eta.eta(remaining));
            eprintln!(
                "[indexer] starting at {}, safe tip {}, {} blocks behind, ETA ~ {}",
                next_height, target_tip, remaining, eta_str
            );
            logged_start = true;
        }

        if shutdown_requested.load(Ordering::Relaxed) {
            break;
        }

        if next_height <= target_tip {
            // Compute a fresh ETA before starting the block
            let remaining = target_tip.saturating_sub(next_height) + 1;
            let eta_str = fmt_duration(eta.eta(remaining));

            eprintln!(
                "[indexer] indexing block #{} ({} left → ETA ~ {})",
                next_height, remaining, eta_str
            );

            eta.start_block();

            if let Err(e) = metashrew_sdb.catch_up_now() {
                eprintln!(
                    "[indexer] metashrew catch_up before indexing block {}: {e:?}",
                    next_height
                );
            }

            match get_indexer_block(next_height, target_tip, network)
                .with_context(|| format!("failed to load espo block {next_height}"))
            {
                Ok(espo_block) => {
                    block_waits.reset();
                    let block_txids: Vec<Txid> = espo_block
                        .transactions
                        .iter()
                        .map(|t| t.transaction.compute_txid())
                        .collect();
                    let block_address_txs = block_output_address_txs(&espo_block, network);

                    let block_hash = espo_block.block_header.block_hash();
                    let db_write_guard = AtomicFlagGuard::new(&db_write_active);

                    let mut block_failed: Option<anyhow::Error> = None;
                    match get_bitcoind_rpc_client().get_block_hash(next_height as u64) {
                        Ok(canonical_hash) if canonical_hash != block_hash => {
                            block_failed = Some(anyhow::anyhow!(
                                "block source returned non-canonical block at height {}: source={} core={}",
                                next_height,
                                block_hash,
                                canonical_hash
                            ));
                        }
                        Ok(_) => {}
                        Err(e) => {
                            block_failed = Some(anyhow::anyhow!(
                                "failed to verify canonical block hash at height {}: {}",
                                next_height,
                                e
                            ));
                        }
                    }
                    if let Some(tree) = get_global_tree_db() {
                        if block_failed.is_none() {
                            if let Err(e) = tree.begin_block(
                                next_height,
                                &block_hash,
                                &espo_block.block_header.prev_blockhash,
                            ) {
                                block_failed = Some(anyhow::anyhow!(
                                    "tree failed to begin block {} ({}): {}",
                                    next_height,
                                    block_hash,
                                    e
                                ));
                            }
                        }
                    }

                    let mut deferred_runes_module = None;
                    if block_failed.is_none() {
                        for m in mods.modules() {
                            if m.get_name() == "runes" {
                                deferred_runes_module = Some(m.clone());
                                continue;
                            }
                            if next_height >= m.get_genesis_block(network) {
                                if let Err(e) = m.index_block(espo_block.clone()) {
                                    block_failed = Some(e.context(format!(
                                        "module {} failed at height {}",
                                        m.get_name(),
                                        next_height
                                    )));
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(e) = block_failed {
                        if let Err(rollback_err) = rollback_failed_block(&mods, next_height) {
                            eprintln!(
                                "[indexer] failed to roll back block {} after error: {rollback_err:?}; original error: {e:?}",
                                next_height
                            );
                            shutdown_requested.store(true, Ordering::Relaxed);
                            break;
                        }
                        drop(db_write_guard);
                        eprintln!(
                            "[indexer] block {} failed before commit; rolled back and will retry: {e:?}",
                            next_height
                        );
                        tokio::time::sleep(POLL_INTERVAL).await;
                        continue;
                    }

                    if let Err(e) = crate::debug::flush_timer_totals() {
                        eprintln!(
                            "[debug] failed to flush timer totals at height {}: {}",
                            next_height, e
                        );
                    }

                    if let Some(tree) = get_global_tree_db() {
                        if let Err(e) = tree.finish_block() {
                            if let Err(rollback_err) = rollback_failed_block(&mods, next_height) {
                                eprintln!(
                                    "[indexer] failed to roll back block {} after tree finish error: {rollback_err:?}; original error: {e:?}",
                                    next_height
                                );
                                shutdown_requested.store(true, Ordering::Relaxed);
                                break;
                            }
                            drop(db_write_guard);
                            eprintln!(
                                "[tree] failed to finish block {}; rolled back and will retry: {e:?}",
                                next_height
                            );
                            tokio::time::sleep(POLL_INTERVAL).await;
                            continue;
                        }
                    }

                    if let Some(m) = deferred_runes_module {
                        if next_height >= m.get_genesis_block(network) {
                            if let Err(e) = m.index_block(espo_block.clone()) {
                                eprintln!(
                                    "[module:{}] height {} failed after tree commit: {e:?}",
                                    m.get_name(),
                                    next_height
                                );
                                if let Err(rollback_err) = rollback_failed_block(&mods, next_height)
                                {
                                    eprintln!(
                                        "[indexer] failed to roll back block {} after runes error: {rollback_err:?}; original error: {e:?}",
                                        next_height
                                    );
                                    shutdown_requested.store(true, Ordering::Relaxed);
                                    break;
                                }
                                drop(db_write_guard);
                                eprintln!(
                                    "[indexer] block {} rolled back after runes error; retrying",
                                    next_height
                                );
                                tokio::time::sleep(POLL_INTERVAL).await;
                                continue;
                            }
                        }
                    }

                    if let Err(e) = update_indexed_block_interaction_summary(
                        &essentials_provider,
                        runes_summary_provider.as_ref(),
                        &espo_block,
                    ) {
                        if let Err(rollback_err) = rollback_failed_block(&mods, next_height) {
                            eprintln!(
                                "[indexer] failed to roll back block {} after summary error: {rollback_err:?}; original error: {e:?}",
                                next_height
                            );
                            shutdown_requested.store(true, Ordering::Relaxed);
                            break;
                        }
                        drop(db_write_guard);
                        eprintln!(
                            "[summary] failed to update interaction count at height {}; rolled back and will retry: {e:?}",
                            next_height
                        );
                        tokio::time::sleep(POLL_INTERVAL).await;
                        continue;
                    }

                    match purge_confirmed_txids(&block_txids) {
                        Ok(removed) => {
                            if removed > 0 {
                                eprintln!(
                                    "[mempool] removed {} confirmed txs at height {}",
                                    removed, next_height
                                );
                            }
                        }
                        Err(e) => eprintln!(
                            "[mempool] failed to purge confirmed txs at height {}: {e:?}",
                            next_height
                        ),
                    }
                    publish_new_block_event(next_height, &block_txids);
                    publish_confirmed_tx_events(next_height, &block_txids, &block_address_txs);
                    drop(db_write_guard);

                    if let Some(backup) = cfg.debug_backup.as_ref() {
                        if debug_backup_remaining.remove(&next_height) {
                            eprintln!(
                                "[debug_backup] reached block {}, copying db dir '{}' to '{}/bkp-{}'",
                                next_height, cfg.db_path, backup.dir, next_height
                            );
                            match run_debug_backup(&cfg.db_path, backup, next_height) {
                                Ok(_) => eprintln!("[debug_backup] backup complete"),
                                Err(e) => eprintln!("[debug_backup] backup failed: {e}"),
                            }
                        }
                    }

                    eta.finish_block();
                    next_height = next_height.saturating_add(1);
                    if let Some(h) = ESPO_HEIGHT.get() {
                        h.store(next_height, std::sync::atomic::Ordering::Relaxed);
                    }
                    if cfg.indexer_block_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(cfg.indexer_block_delay_ms)).await;
                    }
                }
                Err(e) => {
                    if let Some(kind) = classify_metashrew_canonicality_wait(&e) {
                        let attempt = block_waits.bump(next_height, kind);
                        let retry_delay = canonicality_retry_delay(attempt);
                        if should_log_canonicality_wait(attempt) {
                            log_canonicality_wait(
                                "block_load",
                                next_height,
                                kind,
                                attempt,
                                retry_delay,
                                &e,
                            );
                        }
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                    block_waits.reset();
                    eprintln!("[indexer] error at height {}: {e:?}", next_height);
                    // Don’t update EMA on failure; just wait and retry
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        } else {
            if !safe_tip_hook_ran {
                if let Some(script) = cfg.safe_tip_hook_script.as_deref() {
                    safe_tip_hook_ran = true;
                    run_safe_tip_hook(script, next_height, target_tip);
                }
            }
            // Caught up; chill then poll again
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        if shutdown_requested.load(Ordering::Relaxed) {
            break;
        }

        if stop_after_block.is_none() && !mempool_started && next_height >= tip.saturating_sub(1) {
            mempool_started = true;
            let network_for_task = network;
            std::thread::spawn(move || {
                let rt = TokioBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build mempool runtime");
                if let Err(e) = rt.block_on(run_mempool_service(network_for_task)) {
                    eprintln!("[mempool] service error: {e:?}");
                }
            });
            eprintln!(
                "[mempool] service started near safe tip (next_height={}, tip={})",
                next_height, tip
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<()> {
    tokio::task::block_in_place(init_config)?;
    let cfg = get_config().clone();
    let mut jemalloc_profiler = runtime::jemalloc_prof::start(&cfg.jemalloc_profile);
    let network = get_network();
    let view_only = cfg.view_only;
    tokio::task::block_in_place(init_block_source)?;

    if view_only {
        eprintln!(
            "[mode] view-only enabled: indexer and mempool are disabled; serving existing data only"
        );
    }
    let metashrew_sdb = get_metashrew_sdb();

    // Build module registry with the global ESPO DB
    let mut mods = ModuleRegistry::with_db(get_espo_db());
    // Essentials must run before any optional modules.
    mods.register_module(Essentials::new());
    mods.register_module(Pizzafun::new());
    if get_module_config("ammdata").is_some() {
        mods.register_module(AmmData::new());
    } else {
        eprintln!("[modules] ammdata disabled (missing config)");
    }
    if Memgraph::enabled_from_global_config() {
        mods.register_module(Memgraph::new());
    } else {
        eprintln!("[modules] memgraph disabled (requires modules.memgraph.enable=true)");
    }
    if runes_enabled_from_global_config() {
        mods.register_module(Runes::new());
    } else {
        eprintln!("[modules] runes disabled (requires modules.runes.enable=true)");
    }
    mods.register_module(TokenData::new());
    if get_module_config("subfrost").is_some() {
        mods.register_module(Subfrost::new());
    } else {
        eprintln!("[modules] subfrost disabled (missing config)");
    }
    if get_module_config("oylapi").is_some() {
        mods.register_module(OylApi::new());
    } else {
        eprintln!("[modules] oylapi disabled (missing config)");
    }
    // mods.register_module(TracesData::new());

    let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
    let loaded = preload_block_summary_cache(&essentials_mdb);
    if loaded > 0 {
        eprintln!("[cache] preloaded {} block summaries", loaded);
    }

    let mut service_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Start RPC server
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let rpc_router = mods.router.clone();
    service_handles.push(tokio::spawn(async move {
        if let Err(e) = run_rpc(rpc_router, addr).await {
            eprintln!("[rpc] server error: {e:?}");
        }
    }));
    eprintln!("[rpc] listening on {}", addr);

    // Optional SSR explorer server
    if let Some(explorer_addr) = cfg.explorer_host {
        service_handles.push(tokio::spawn(async move {
            if let Err(e) = run_explorer(explorer_addr).await {
                eprintln!("[explorer] server error: {e:?}");
            }
        }));
        eprintln!("[explorer] listening on {}", explorer_addr);
    }

    // Decide initial start height (resume at last+1 per module)
    let mut start_height = mods
        .modules()
        .iter()
        .filter(|m| m.contributes_to_start_height())
        .map(|m| {
            let g = m.get_genesis_block(network);
            match m.get_index_height() {
                Some(h) => h.saturating_add(1).max(g),
                None => g,
            }
        })
        .min()
        .unwrap_or_else(|| alkanes_genesis_block(network));
    if let Some(forced_start) = std::env::var("ESPO_START_BLOCK")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    {
        eprintln!("[indexer] forcing start block from ESPO_START_BLOCK={forced_start}");
        start_height = forced_start;
    }

    let height_cell = Arc::new(AtomicU32::new(start_height));

    ESPO_HEIGHT
        .set(height_cell.clone())
        .map_err(|_| anyhow::anyhow!("espo height client already initialized"))?;
    let next_height: u32 = start_height;

    if view_only {
        let indexed_height = start_height.saturating_sub(1);
        update_safe_tip(indexed_height);
        eprintln!(
            "[mode] view-only: explorer/RPC running; indexed height {}, next height {}",
            indexed_height, start_height
        );
        wait_for_shutdown_signal().await?;
        eprintln!("[PROCESS] exit signal received; stopping servers");
        request_shutdown();
        for handle in service_handles.drain(..) {
            handle.abort();
        }
        jemalloc_profiler.shutdown_dump();
        return Ok(());
    }

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_for_indexer = shutdown_requested.clone();
    let db_write_active = Arc::new(AtomicBool::new(false));
    let db_write_active_for_indexer = db_write_active.clone();
    let indexer_handle = std::thread::spawn(move || {
        let rt = TokioBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build indexer runtime");
        rt.block_on(run_indexer_loop(
            mods,
            start_height,
            next_height,
            network,
            metashrew_sdb,
            cfg,
            shutdown_for_indexer,
            db_write_active_for_indexer,
        ));
    });

    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);
    loop {
        if indexer_handle.is_finished() {
            if let Err(err) = indexer_handle.join() {
                eprintln!("[indexer] thread panicked: {err:?}");
                std::process::abort();
            }
            jemalloc_profiler.shutdown_dump();
            return Ok(());
        }

        tokio::select! {
            result = &mut shutdown_signal => {
                result?;
                eprintln!("[PROCESS] exit signal received; stopping servers");
                request_shutdown();
                shutdown_requested.store(true, Ordering::Relaxed);
                for handle in service_handles.drain(..) {
                    handle.abort();
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }

    let shutdown_started = Instant::now();
    let mut logged_db_wait = false;
    loop {
        if indexer_handle.is_finished() {
            if let Err(err) = indexer_handle.join() {
                eprintln!("[indexer] thread panicked: {err:?}");
                std::process::abort();
            }
            jemalloc_profiler.shutdown_dump();
            return Ok(());
        }

        let db_active = db_write_active.load(Ordering::Relaxed);
        if !db_active && shutdown_started.elapsed() >= Duration::from_secs(5) {
            eprintln!(
                "[PROCESS] forcing exit after shutdown grace; indexer is not in a db write section"
            );
            jemalloc_profiler.shutdown_dump();
            std::process::exit(130);
        }
        if db_active && !logged_db_wait {
            eprintln!("[PROCESS] waiting for active block db write to finish before exit");
            logged_db_wait = true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// Dummy main for WASM builds (should never be called)
#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("ESPO binary cannot be compiled for WASM");
}
