use crate::config::{get_electrum_like, get_metashrew};
use anyhow::Result;

fn get_electrum_tip() -> Result<u32> {
    let client = get_electrum_like();
    client.tip_height()
}

pub fn get_safe_tip() -> Result<u32> {
    let alkanes_tip = get_metashrew().get_alkanes_tip_height()?;
    let electrum_tip = match get_electrum_tip() {
        Ok(tip) => Some(tip),
        Err(e) => {
            eprintln!("[tip] electrum/esplora tip fetch failed: {e:?}; using metashrew tip only");
            None
        }
    };

    Ok(electrum_tip.map(|t| std::cmp::min(alkanes_tip, t)).unwrap_or(alkanes_tip))
}
