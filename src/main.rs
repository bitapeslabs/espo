pub mod alkanes;
pub mod config;
pub mod consts;
pub mod core;
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

use crate::{
    alkanes::{trace::get_espo_block, utils::get_safe_tip},
    config::{get_config, get_espo_db, init_config},
    consts::alkanes_genesis_block,
    modules::defs::ModuleRegistry,
    runtime::rpc::run_rpc,
};
use espo::ESPO_HEIGHT;

#[tokio::main]
async fn main() -> Result<()> {
    init_config()?;
    let network = get_network();
    init_block_source()?;
    let cfg = get_config();
    let metashrew_sdb = get_metashrew_sdb();

    // Build module registry with the global ESPO DB
    let mut mods = ModuleRegistry::with_db(get_espo_db());
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
            // Caught up; chill then poll again
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
