pub mod alkanes;
pub mod config;
pub mod consts;
pub mod core;
pub mod explorer;
pub mod modules;
pub mod runtime;
pub mod schemas;
pub mod utils;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use crate::config::init_block_source;
//modules
use crate::config::get_metashrew_sdb;
use crate::config::get_network;
use crate::modules::ammdata::main::AmmData;
use crate::modules::essentials::main::Essentials;
use crate::utils::{EtaTracker, fmt_duration};
use anyhow::{Context, Result};

use crate::explorer::run_explorer;
use bitcoincore_rpc::RpcApi;
use crate::{
    alkanes::{trace::get_espo_block, utils::get_safe_tip},
    config::{
        get_aof_manager, get_bitcoind_rpc_client, get_config, get_espo_db, init_config,
        update_safe_tip,
    },
    consts::alkanes_genesis_block,
    modules::defs::ModuleRegistry,
    runtime::aof::AOF_REORG_DEPTH,
    runtime::rpc::run_rpc,
    runtime::mempool::{purge_confirmed_from_chain, purge_confirmed_txids, reset_mempool_store, run_mempool_service},
};
pub use espo::{ESPO_HEIGHT, SAFE_TIP};
use bitcoin::Txid;
use tokio::runtime::Builder as TokioBuilder;

fn should_watch_for_reorg(next_height: u32, safe_tip: u32) -> bool {
    safe_tip.saturating_sub(next_height) <= AOF_REORG_DEPTH
}

fn check_and_handle_reorg(next_height: u32, safe_tip: u32) -> Option<u32> {
    let Some(aof) = get_aof_manager() else { return None };
    if !should_watch_for_reorg(next_height, safe_tip) {
        return None;
    }

    let logs = match aof.recent_blocks(AOF_REORG_DEPTH as usize) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[aof] failed to load recent blocks: {e:?}");
            return None;
        }
    };
    if logs.is_empty() {
        return None;
    }

    let rpc = get_bitcoind_rpc_client();
    let mut mismatch = false;
    for log in &logs {
        match rpc.get_block_hash(log.height as u64) {
            Ok(h) => {
                if h.to_string() != log.block_hash {
                    mismatch = true;
                    break;
                }
            }
            Err(e) => {
                eprintln!("[aof] failed to fetch block hash for {}: {e:?}", log.height);
                return None;
            }
        }
    }

    if !mismatch {
        return None;
    }

    let revert_count = logs.len().min(AOF_REORG_DEPTH as usize);
    eprintln!(
        "[aof] detected reorg near height {} (safe tip {}); reverting {} blocks",
        next_height, safe_tip, revert_count
    );

    if let Err(e) = aof.revert_last_blocks(revert_count) {
        eprintln!("[aof] rollback failed: {e:?}");
        return None;
    }

    if let Err(e) = reset_mempool_store() {
        eprintln!("[mempool] failed to reset store after reorg: {e:?}");
    }

    Some(next_height.saturating_sub(revert_count as u32))
}

#[tokio::main]
async fn main() -> Result<()> {
    init_config()?;
    let network = get_network();
    init_block_source()?;
    let cfg = get_config();
    let metashrew_sdb = get_metashrew_sdb();

    if cfg.simulate_reorg {
        match get_aof_manager() {
            Some(aof) => match aof.revert_all_blocks() {
                Ok(Some(h)) => {
                    eprintln!("[aof] simulate-reorg: reverted through height {}, will reindex", h);
                    if let Err(e) = reset_mempool_store() {
                        eprintln!("[mempool] failed to reset store after simulated reorg: {e:?}");
                    }
                }
                Ok(None) => eprintln!("[aof] simulate-reorg set but no AOF logs to revert"),
                Err(e) => eprintln!("[aof] simulate-reorg failed: {e:?}"),
            },
            None => eprintln!("[aof] simulate-reorg set but AOF is disabled; nothing to revert"),
        }
    }

    // Build module registry with the global ESPO DB
    let mut mods = ModuleRegistry::with_db_and_aof(get_espo_db(), get_aof_manager());
    mods.register_module(AmmData::new());
    mods.register_module(Essentials::new());
    // mods.register_module(TracesData::new());

    // Start RPC server
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let rpc_router = mods.router.clone();
    tokio::spawn(async move {
        if let Err(e) = run_rpc(rpc_router, addr).await {
            eprintln!("[rpc] server error: {e:?}");
        }
    });
    eprintln!("[rpc] listening on {}", addr);

    // Optional SSR explorer server
    if let Some(explorer_addr) = cfg.explorer_host {
        tokio::spawn(async move {
            if let Err(e) = run_explorer(explorer_addr).await {
                eprintln!("[explorer] server error: {e:?}");
            }
        });
        eprintln!("[explorer] listening on {}", explorer_addr);
    }

    let global_genesis = alkanes_genesis_block(network);

    // Decide initial start height (resume at last+1 per module)
    let start_height = mods
        .modules()
        .iter()
        .map(|m| {
            let g = m.get_genesis_block(network);
            match m.get_index_height() {
                Some(h) => h.saturating_add(1).max(g),
                None => g,
            }
        })
        .min()
        .unwrap_or(global_genesis)
        .max(global_genesis);

    let height_cell = Arc::new(AtomicU32::new(start_height));

    ESPO_HEIGHT
        .set(height_cell.clone())
        .map_err(|_| anyhow::anyhow!("espo height client already initialized"))?;
    let mut next_height: u32 = start_height;

    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    let mut last_tip: Option<u32> = None;
    let mut mempool_started = false;
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

    loop {
        if let Err(e) = metashrew_sdb.catch_up_now() {
            eprintln!("[indexer] metashrew catch_up before tip fetch: {e:?}");
        }

        let tip = match get_safe_tip() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[indexer] failed to fetch safe tip: {e:?}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };
        update_safe_tip(tip);
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

        if next_height == start_height {
            let remaining = tip.saturating_sub(next_height) + 1;
            let eta_str = fmt_duration(eta.eta(remaining));
            eprintln!(
                "[indexer] starting at {}, safe tip {}, {} blocks behind, ETA ~ {}",
                next_height, tip, remaining, eta_str
            );
        }

        if next_height <= tip {
            // Compute a fresh ETA before starting the block
            let remaining = tip.saturating_sub(next_height) + 1;
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

            match get_espo_block(next_height.into(), tip.into())
                .with_context(|| format!("failed to load espo block {next_height}"))
            {
                Ok(espo_block) => {
                    // (Optional) include hash or tx count here as you like
                    let block_txids: Vec<Txid> = espo_block
                        .transactions
                        .iter()
                        .map(|t| t.transaction.compute_txid())
                        .collect();

                    // Only capture AOF changes when we're near the safe tip; skip during deep catch-up.
                    let aof_for_block =
                        get_aof_manager().filter(|_| should_watch_for_reorg(next_height, tip));
                    if let Some(aof) = &aof_for_block {
                        let block_hash = espo_block.block_header.block_hash();
                        aof.start_block(next_height, &block_hash);
                    }

                    for m in mods.modules() {
                        if next_height >= m.get_genesis_block(network) {
                            if let Err(e) = m.index_block(espo_block.clone()) {
                                eprintln!(
                                    "[module:{}] height {}: {e:?}",
                                    m.get_name(),
                                    next_height
                                );
                            }
                        }
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

                    if let Some(aof) = &aof_for_block {
                        if let Err(e) = aof.finish_block() {
                            eprintln!(
                                "[aof] failed to persist block {} changes: {e:?}",
                                next_height
                            );
                        }
                    }

                    eta.finish_block();
                    next_height = next_height.saturating_add(1);
                    if let Some(h) = ESPO_HEIGHT.get() {
                        h.store(next_height, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    eprintln!("[indexer] error at height {}: {e:?}", next_height);
                    // Don’t update EMA on failure; just wait and retry
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        } else {
            if let Some(new_next) = check_and_handle_reorg(next_height, tip) {
                if new_next < next_height {
                    next_height = new_next;
                    if let Some(h) = ESPO_HEIGHT.get() {
                        h.store(next_height, std::sync::atomic::Ordering::Relaxed);
                    }
                    eprintln!("[aof] rollback complete, restarting from height {}", next_height);
                }
            }
            // Caught up; chill then poll again
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        if !mempool_started && next_height >= tip.saturating_sub(1) {
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
