pub mod alkanes;
pub mod config;
pub mod consts;
pub mod modules;
pub mod runtime;
pub mod schemas;
pub mod types;
pub mod utils;

use std::net::SocketAddr;
use std::time::Duration;

//modules
use crate::modules::ammdata::main::AmmData;
use crate::modules::essentials::main::Essentials;
use crate::utils::{EtaTracker, fmt_duration};

use anyhow::{Context, Result};

use crate::{
    alkanes::{trace::get_espo_block, utils::get_safe_tip},
    config::{get_config, get_espo_db, init_config},
    consts::{NETWORK, alkanes_genesis_block},
    modules::defs::ModuleRegistry,
    runtime::rpc::run_rpc,
};
#[tokio::main]
async fn main() -> Result<()> {
    init_config()?;
    let cfg = get_config();

    // Build module registry with the global ESPO DB
    let mut mods = ModuleRegistry::with_db(get_espo_db());
    mods.register_module(AmmData::new());
    //mods.register_module(Essentials::new());
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

    let global_genesis = alkanes_genesis_block(NETWORK);

    // Decide initial start height (resume at last+1 per module)
    let start_height = mods
        .modules()
        .iter()
        .map(|m| {
            let g = m.get_genesis_block(NETWORK);
            match m.get_index_height() {
                Some(h) => h.saturating_add(1).max(g),
                None => g,
            }
        })
        .min()
        .unwrap_or(global_genesis)
        .max(global_genesis);

    let mut next_height: u32 = start_height;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);

    // ETA tracker
    let mut eta = EtaTracker::new(3.0); // EMA smoothing factor (tweak if you want faster/slower adaptation)

    loop {
        let tip = match get_safe_tip() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[indexer] failed to fetch safe tip: {e:?}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };

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

            match get_espo_block(next_height.into())
                .with_context(|| format!("failed to load espo block {next_height}"))
            {
                Ok(espo_block) => {
                    // (Optional) include hash or tx count here as you like

                    for m in mods.modules() {
                        if next_height >= m.get_genesis_block(NETWORK) {
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
