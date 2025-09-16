use bitcoin::Network;

use crate::modules::ammdata::schemas::SchemaAlkaneId;

pub fn ammdata_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 904_648,
        _ => 0,
    }
}

pub const AMM_CONTRACT: SchemaAlkaneId = SchemaAlkaneId { block: 4u32, tx: 65522u64 };
pub const KEY_INDEX_HEIGHT: &[u8] = b"/index_height";
