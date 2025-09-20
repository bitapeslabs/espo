use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;

pub fn ammdata_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 908_367,
        _ => 0,
    }
}

pub fn get_amm_contract(network: Network) -> Result<SchemaAlkaneId> {
    match network {
        Network::Bitcoin => Ok(SchemaAlkaneId { block: 4u32, tx: 65522u64 }),
        _ => Err(anyhow!("AMMDATA ERROR: Amm contract not defined for this network")),
    }
}

pub const KEY_INDEX_HEIGHT: &[u8] = b"/index_height";
pub const GET_RESERVES_OPCODE: u8 = 0x61;
pub const DEPLOY_AMM_OPCODE: u8 = 0x01;
pub const PRICE_SCALE: u128 = 100_000_000; // 1e18
