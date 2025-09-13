pub mod consts;
pub mod runtime;
pub mod types;
pub mod utils;

use anyhow::{Context, Result};
use electrum_client::ElectrumApi;
use utils::cli::init_config;

use crate::utils::{cli::get_electrum_client, trace};
pub fn debug_probe<C: ElectrumApi>(client: &C) -> Result<()> {
    // tip height (widely supported)
    let sub = client
        .block_headers_subscribe_raw()
        .context("blockchain.headers.subscribe failed")?;
    eprintln!("[electrum] tip height = {}", sub.height);
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_config()?;
    debug_probe(get_electrum_client())?;

    let init_block: u64 = 880_000;
    for block in init_block..910_000u64 {
        println!("{}: {}", "\nIndexing block: ", block);
        let espo_block = trace::get_espo_block(block)?;
        println!("∟-----> Espo block has: {} traces", espo_block.transactions.len())
    }
    //runtime::server::run().await
    Ok(())
}
