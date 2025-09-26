use bitcoin::Network;

//tests to run before running a full reindex if u changed balances.rs

//906,300 -> 906,400 bc1pvjhh9wzdzyw8vcny7lqgsuw705nt88mtytadv22cqfxesz92uz7su3aea0 2:80 balance === 67184000000000 (tests merge between trace and outpoint balances)
//910,000 -> No runaway addresses that gain diesel over >100k (if there is a bug, an address ending with 'gaaa' will  be the top holder, this means u fucked the merge between traces and balance outpoints)
//908,494 -> 908,550 -> 2:68479 has > 600 holders (edicts allocating properly)
//911,610 -> 914,180 -> outpoint 638873ac37b73e6dbd58e8b8c379bbb023624a317966a1172f3add0c0f077021:0 has a balance of 160.69 diesel (tests if reverts affect balance state)

pub fn essentials_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 880_000,
        _ => 0,
    }
}
