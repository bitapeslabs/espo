pub mod alkanes;
pub mod config;
pub mod consts;
pub mod modules;
pub mod runtime;
pub mod types;
use std::net::SocketAddr;

use anyhow::{Context, Result};

use crate::{
    alkanes::{trace::get_espo_block, utils::get_safe_tip},
    config::{get_config, init_config},
    consts::{NETWORK, alkanes_genesis_block},
    modules::defs::ModuleRegistry,
    runtime::rpc::run_rpc,
};

#[tokio::main]
async fn main() -> Result<()> {
    // 1) Init config & shared clients.
    init_config()?;
    let cfg = get_config();
    let tip = get_safe_tip()?;

    eprintln!("[debug] safeheight on {}", tip);

    // 2) Build module registry and register your modules here.
    let mut mods = ModuleRegistry::new();
    // mods.register_module(AmmData);
    // mods.register_module(TracesData);

    // 3) Start RPC server (single endpoint) in the background.
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let rpc_router = mods.router.clone();
    tokio::spawn(async move {
        if let Err(e) = run_rpc(rpc_router, addr).await {
            eprintln!("[rpc] server error: {e:?}");
        }
    });
    eprintln!("[rpc] listening on {}", addr);

    // 4) Decide indexing range.
    let global_genesis = alkanes_genesis_block(NETWORK);

    // If there are modules, start at the earliest module’s genesis; otherwise use global.
    let start_height = mods
        .modules()
        .iter()
        .map(|m| m.get_genesis_block(NETWORK))
        .min()
        .unwrap_or(global_genesis)
        .min(global_genesis);

    // Current tip from Electrum.
    let tip = get_safe_tip()?;
    eprintln!("[indexer] range: {}..={}", start_height, tip);

    // 5) Index loop: fetch each block once, feed only interested modules.
    for height in start_height..=tip {
        let espo_block = get_espo_block(height.into())
            .with_context(|| format!("failed to load espo block {height}"))?;

        eprintln!(
            "[indexer] indexing block #{} with {} traces",
            height,
            espo_block.transactions.len()
        );

        for m in mods.modules() {
            if height >= m.get_genesis_block(NETWORK) {
                if let Err(e) = m.index_block(espo_block.clone()) {
                    eprintln!("[module:{}] height {}: {e:?}", m.get_name(), height);
                }
            }
        }
    }

    Ok(())
}
