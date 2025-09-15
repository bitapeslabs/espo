use bitcoin::Network;

pub fn ammdata_genesis_block(network: Network) -> u64 {
    match network {
        Network::Bitcoin => 904_648,
        _ => 0,
    }
}
