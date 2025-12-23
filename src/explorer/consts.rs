pub const DEFAULT_PAGE_LIMIT: usize = 25;
pub const MAX_PAGE_LIMIT: usize = 200;

use bitcoin::Network;

use crate::config::get_network;

pub const ALKANE_ICON_BASE: &str = "https://ordiscan.com/alkane";
pub const ALKANE_ICON_FALLBACK_BASE: &str = "https://cdn.ordiscan.com/alkanes";

// --- Mainnet overrides ---
const MAINNET_ALKANE_NAME_OVERRIDES: &[(&str, &str, &str)] =
    &[("2:0", "DIESEL", "DIESEL"), ("32:0", "frBTC", "FRBTC"), ("2:68479", "TORTILLA", "TORTILLA")];
const MAINNET_ICON_OVERRIDES: &[(&str, &str)] = &[
    ("2:68479", "https://cdn.idclub.io/alkanes/2-62083.webp"),
    ("32:0", "https://i.ibb.co/CpNspq3D/btc-empty.png"),
];
const MAINNET_CONTRACT_NAME_OVERRIDES: &[(&str, &str)] = &[("4:65522", "Oyl AMM")];

// --- Regtest overrides (extend as needed) ---
const REGTEST_ALKANE_NAME_OVERRIDES: &[(&str, &str, &str)] = &[];
const REGTEST_ICON_OVERRIDES: &[(&str, &str)] = &[];
const REGTEST_CONTRACT_NAME_OVERRIDES: &[(&str, &str)] = &[];

pub fn alkane_name_overrides() -> &'static [(&'static str, &'static str, &'static str)] {
    match get_network() {
        Network::Bitcoin => MAINNET_ALKANE_NAME_OVERRIDES,
        Network::Regtest => REGTEST_ALKANE_NAME_OVERRIDES,
        _ => MAINNET_ALKANE_NAME_OVERRIDES,
    }
}

pub fn alkane_icon_overrides() -> &'static [(&'static str, &'static str)] {
    match get_network() {
        Network::Bitcoin => MAINNET_ICON_OVERRIDES,
        Network::Regtest => REGTEST_ICON_OVERRIDES,
        _ => MAINNET_ICON_OVERRIDES,
    }
}

/// Optional overrides specifically for contract display names.
pub fn alkane_contract_name_overrides() -> &'static [(&'static str, &'static str)] {
    match get_network() {
        Network::Bitcoin => MAINNET_CONTRACT_NAME_OVERRIDES,
        Network::Regtest => REGTEST_CONTRACT_NAME_OVERRIDES,
        _ => MAINNET_CONTRACT_NAME_OVERRIDES,
    }
}
