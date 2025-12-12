use crate::config::{get_electrum_client, get_metashrew};
use anyhow::{Context, Result, anyhow};
use electrum_client::ElectrumApi; // bring trait into scope for *_subscribe_* methods

fn get_electrum_tip() -> Result<u32> {
    let client = get_electrum_client();

    // Your electrum-client version exposes the *_raw variant:
    let sub = client
        .block_headers_subscribe_raw()
        .context("electrum: blockchain.headers.subscribe failed")?;

    let tip: u32 = sub
        .height
        .try_into()
        .map_err(|_| anyhow!("electrum: tip height doesn't fit into u32"))?;

    Ok(tip)
}

pub fn get_safe_tip() -> Result<u32> {
    let alkanes_tip = get_metashrew().get_alkanes_tip_height()?;
    let electrum_tip = get_electrum_tip().context("Failed to get electrum tip height")?;

    Ok(std::cmp::min(alkanes_tip, electrum_tip))
}
