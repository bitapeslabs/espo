use bitcoin::Network;

pub const METASHREW_TIP_HEIGHT_PREFIX: &[u8] = b"/__INTERNAL/tip-height";

pub const TRACES_BY_BLOCK_PREFIX: &[u8] = b"/trace/";

pub const NETWORK: Network = Network::Bitcoin;

pub fn alkanes_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 880_000,
        _ => 0,
    }
}
