use crate::{
    config::{get_electrum_client, get_metashrew_sdb},
    consts::METASHREW_TIP_HEIGHT_PREFIX,
};
use anyhow::{Context, Result, anyhow};
use electrum_client::ElectrumApi; // bring trait into scope for *_subscribe_* methods

pub fn get_alkanes_tip_height() -> Result<u32> {
    let metashrew_sdb = get_metashrew_sdb();

    let Some(bytes) = metashrew_sdb.get(METASHREW_TIP_HEIGHT_PREFIX)? else {
        return Err(anyhow!("ESPO ERROR: Failed to get metashrew height"));
    };

    // Convert Vec<u8> -> [u8; 4]
    let arr: [u8; 4] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("ESPO ERROR: Expected 4 bytes, got {}", bytes.len()))?;

    // Use the endianness you stored with
    Ok(u32::from_le_bytes(arr))
    // Or: Ok(u32::from_be_bytes(arr))
}

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
    let alkanes_tip =
        get_alkanes_tip_height().context("Failed to get alkanes/metashrew tip height")?;
    let electrum_tip = get_electrum_tip().context("Failed to get electrum tip height")?;

    Ok(std::cmp::min(alkanes_tip, electrum_tip))
}
