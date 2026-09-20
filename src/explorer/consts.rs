pub const DEFAULT_PAGE_LIMIT: usize = 25;
pub const MAX_PAGE_LIMIT: usize = 200;

use bitcoin::Network;

use crate::config::get_network;

pub const ALKANE_TOKEN_ICON_BASE: &str = "https://cdn.ordiscan.com/alkanes";
pub const ALKANE_CONTRACT_ICON_BASE: &str = "https://cdn.ordiscan.com/alkanes";
const FRBTC_ICON_URL: &str = "https://i.ibb.co/6cR2hC05/frbtc-improved-1.png";

// --- Mainnet overrides ---
const MAINNET_ALKANE_NAME_OVERRIDES: &[(&str, &str, &str)] =
    &[("2:0", "DIESEL", "DIESEL"), ("32:0", "frBTC", "FRBTC"), ("2:68479", "TORTILLA", "TORTILLA")];
const MAINNET_ICON_OVERRIDES: &[(&str, &str)] = &[
    ("2:68479", "https://cdn.idclub.io/alkanes/2-62083.webp"),
    ("2:77269", "https://i.ibb.co/RTZw3zyh/tortilla-Lp-2.png"),
    ("2:77623", "https://i.ibb.co/nN1LKyZb/fire.png"),
    ("32:0", FRBTC_ICON_URL),
    ("4:8888", "https://i.ibb.co/DD229cX0/slice-logo.png"),
];
const MAINNET_CONTRACT_NAME_OVERRIDES: &[(&str, &str)] =
    &[("4:65522", "Oyl AMM"), ("4:8888", "SLICE")];
const MAINNET_FACTORY_ICON_BLACKLIST: &[&str] =
    &["4:3804", "4:103", "4:102", "4:3803", "4:3805", "4:3806", "4:3807", "4:3800", "4:3802"];

// --- Regtest overrides (extend as needed) ---
const REGTEST_ALKANE_NAME_OVERRIDES: &[(&str, &str, &str)] = &[];
const REGTEST_ICON_OVERRIDES: &[(&str, &str)] = &[("32:0", FRBTC_ICON_URL)];
const REGTEST_CONTRACT_NAME_OVERRIDES: &[(&str, &str)] = &[("4:65522", "Oyl AMM")];
const REGTEST_FACTORY_ICON_BLACKLIST: &[&str] = &[];

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

pub fn alkane_factory_icon_blacklist() -> &'static [&'static str] {
    match get_network() {
        Network::Bitcoin => MAINNET_FACTORY_ICON_BLACKLIST,
        Network::Regtest => REGTEST_FACTORY_ICON_BLACKLIST,
        _ => MAINNET_FACTORY_ICON_BLACKLIST,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FRBTC_ICON_URL, MAINNET_CONTRACT_NAME_OVERRIDES, MAINNET_ICON_OVERRIDES,
        REGTEST_CONTRACT_NAME_OVERRIDES, REGTEST_ICON_OVERRIDES,
    };

    #[test]
    fn frbtc_icon_is_overridden_on_mainnet_and_regtest() {
        for overrides in [MAINNET_ICON_OVERRIDES, REGTEST_ICON_OVERRIDES] {
            assert!(overrides.contains(&("32:0", FRBTC_ICON_URL)));
        }
    }

    #[test]
    fn oyl_amm_name_is_overridden_on_mainnet_and_regtest() {
        for overrides in [MAINNET_CONTRACT_NAME_OVERRIDES, REGTEST_CONTRACT_NAME_OVERRIDES] {
            assert!(overrides.contains(&("4:65522", "Oyl AMM")));
        }
    }
}
