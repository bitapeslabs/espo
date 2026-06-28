use bitcoin::Network;

/// Genesis block for the explorerextensions module.
///
/// Mirrors `essentials_genesis_block`: alkanes activate at 880,000 on
/// mainnet, and from height 0 on every other network. The reverse
/// trace indexes only become meaningful once alkanes traces exist, so
/// there's nothing to index before this height.
pub fn explorerextensions_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 880_000,
        _ => 0,
    }
}
