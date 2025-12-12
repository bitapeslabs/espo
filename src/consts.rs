use bitcoin::Network;

pub fn alkanes_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 880_000,
        _ => 0,
    }
}
