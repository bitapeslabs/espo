use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;

pub fn ammdata_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 904_647,
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
pub const PRICE_SCALE_DECIMALS: u32 = 16;
pub const PRICE_SCALE: u128 = 10_000_000_000_000_000; // 1e16
pub const AMOUNT_SCALE: u128 = 100_000_000; // on-chain token amount precision (1e8)
pub const SATS_PER_BTC: u128 = AMOUNT_SCALE;
pub const K_TOLERANCE_BPS: u128 = 10; // 0.1%
pub const FRBTC_ALKANE_ID: SchemaAlkaneId = SchemaAlkaneId { block: 32, tx: 0 };
pub const BUSD_ALKANE_ID: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 56801 };
pub const MAINNET_FIRE_ALKANE_ID: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 77623 };
pub const MAINNET_FIRE_USD_CHART_START_TS: u64 = 1_780_875_600;
// BUSD stops contributing as a canonical USD quote at this height and is treated as a normal token.
pub const BUSD_CANONICAL_QUOTE_FORK_HEIGHT: u32 = 946_500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalQuoteUnit {
    Btc,
    Usd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalQuote {
    pub id: SchemaAlkaneId,
    pub unit: CanonicalQuoteUnit,
}

pub fn busd_is_canonical_quote_at_height(height: u32) -> bool {
    height < BUSD_CANONICAL_QUOTE_FORK_HEIGHT
}

pub fn canonical_quotes(network: Network) -> Vec<CanonicalQuote> {
    canonical_quotes_at_height(network, 0)
}

pub fn canonical_quotes_at_height(network: Network, height: u32) -> Vec<CanonicalQuote> {
    let mut mainnet = vec![CanonicalQuote { id: FRBTC_ALKANE_ID, unit: CanonicalQuoteUnit::Btc }];
    if busd_is_canonical_quote_at_height(height) {
        mainnet.push(CanonicalQuote { id: BUSD_ALKANE_ID, unit: CanonicalQuoteUnit::Usd });
    }
    match network {
        Network::Bitcoin => mainnet,
        _ => mainnet,
    }
}

/// A (token, derived quote) pair that detaches from its derived pool at a height.
///
/// Before the height the token's `-derived_<quote>-usd` chart merges the token's
/// canonical (frBTC) pricing with its price through the quote's pool. From the
/// height on, the pool is no longer registered as the pair's derived pool, so the
/// quote's price moves stop being written into new candles and the chart follows
/// the token's direct USD pricing only - as if `-usd` had been requested. The
/// chart's history before the height is untouched.
#[derive(Debug, Clone, Copy)]
pub struct DerivedQuoteFork {
    pub token: SchemaAlkaneId,
    pub quote: SchemaAlkaneId,
    pub height: u32,
}

/// TORTILLA stops taking DIESEL price movements into its derived chart here.
pub const MAINNET_TORTILLA_DIESEL_DETACH_HEIGHT: u32 = 969_393;

const MAINNET_DERIVED_QUOTE_FORKS: &[DerivedQuoteFork] = &[DerivedQuoteFork {
    token: SchemaAlkaneId { block: 2, tx: 68479 },
    quote: SchemaAlkaneId { block: 2, tx: 0 },
    height: MAINNET_TORTILLA_DIESEL_DETACH_HEIGHT,
}];

/// True when `token`'s derived chart against `quote` must ignore the quote's pool
/// at `height`.
pub fn derived_quote_detached_at_height(
    network: Network,
    token: &SchemaAlkaneId,
    quote: &SchemaAlkaneId,
    height: u32,
) -> bool {
    let table: &[DerivedQuoteFork] = match network {
        Network::Bitcoin => MAINNET_DERIVED_QUOTE_FORKS,
        _ => &[],
    };
    table
        .iter()
        .any(|f| f.token == *token && f.quote == *quote && height >= f.height)
}

/// Holders whose balance of `alkane` is treated as non-circulating by the
/// `cmcap` chart: vesting contracts, treasuries, LP pools the team seeded, burn
/// sinks. A network fact, so it lives here rather than in config.
#[derive(Debug, Clone, Copy)]
pub struct NonCirculatingHolders {
    pub alkane: SchemaAlkaneId,
    pub addresses: &'static [&'static str],
    pub alkanes: &'static [SchemaAlkaneId],
}

const MAINNET_NON_CIRCULATING: &[NonCirculatingHolders] = &[
    // DIESEL
    NonCirculatingHolders {
        alkane: SchemaAlkaneId { block: 2, tx: 0 },
        addresses: &["bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8"],
        alkanes: &[],
    },
    // TORTILLA: the deployer contract and the burn address. The TORT/frBTC LP pool
    // is deliberately not here - pooled liquidity is circulating.
    NonCirculatingHolders {
        alkane: SchemaAlkaneId { block: 2, tx: 68479 },
        addresses: &["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        alkanes: &[SchemaAlkaneId { block: 2, tx: 68478 }],
    },
    // SLICE: four lockers and the treasury address.
    NonCirculatingHolders {
        alkane: SchemaAlkaneId { block: 4, tx: 8888 },
        addresses: &["bc1pz860hkyek9584tx5l84pyax5puu7crerspaksz0jen2svww3ce2smzyjer"],
        alkanes: &[
            SchemaAlkaneId { block: 4, tx: 55 },
            SchemaAlkaneId { block: 4, tx: 53 },
            SchemaAlkaneId { block: 4, tx: 54 },
            SchemaAlkaneId { block: 4, tx: 56 },
        ],
    },
];

pub fn non_circulating_holders(
    network: Network,
    token: &SchemaAlkaneId,
) -> Option<&'static NonCirculatingHolders> {
    let table: &[NonCirculatingHolders] = match network {
        Network::Bitcoin => MAINNET_NON_CIRCULATING,
        _ => &[],
    };
    table.iter().find(|h| h.alkane == *token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busd_is_canonical_before_fork_height() {
        let quotes =
            canonical_quotes_at_height(Network::Bitcoin, BUSD_CANONICAL_QUOTE_FORK_HEIGHT - 1);
        assert!(quotes.iter().any(|q| q.id == BUSD_ALKANE_ID));
        assert!(quotes.iter().any(|q| q.id == FRBTC_ALKANE_ID));
    }

    #[test]
    fn tortilla_detaches_from_diesel_at_the_fork_height() {
        let tort = SchemaAlkaneId { block: 2, tx: 68479 };
        let diesel = SchemaAlkaneId { block: 2, tx: 0 };
        let h = MAINNET_TORTILLA_DIESEL_DETACH_HEIGHT;
        assert!(!derived_quote_detached_at_height(Network::Bitcoin, &tort, &diesel, h - 1));
        assert!(derived_quote_detached_at_height(Network::Bitcoin, &tort, &diesel, h));
        assert!(derived_quote_detached_at_height(Network::Bitcoin, &tort, &diesel, h + 100_000));
        // Mainnet only, and only that pair.
        assert!(!derived_quote_detached_at_height(Network::Regtest, &tort, &diesel, h));
        let other = SchemaAlkaneId { block: 2, tx: 1 };
        assert!(!derived_quote_detached_at_height(Network::Bitcoin, &other, &diesel, h));
        assert!(!derived_quote_detached_at_height(Network::Bitcoin, &tort, &other, h));
    }

    #[test]
    fn non_circulating_table_entries_all_list_holders() {
        for entry in MAINNET_NON_CIRCULATING {
            assert!(
                !entry.addresses.is_empty() || !entry.alkanes.is_empty(),
                "{}:{} lists no holders",
                entry.alkane.block,
                entry.alkane.tx
            );
            // A holder that is the token itself would make the ratio meaningless.
            assert!(!entry.alkanes.contains(&entry.alkane));
        }
    }

    #[test]
    fn non_circulating_lookup_is_mainnet_only() {
        let slice = SchemaAlkaneId { block: 4, tx: 8888 };
        let hit = non_circulating_holders(Network::Bitcoin, &slice).expect("mainnet entry");
        assert_eq!(hit.alkanes.len(), 4);
        assert_eq!(hit.addresses.len(), 1);
        assert!(non_circulating_holders(Network::Regtest, &slice).is_none());
        assert!(
            non_circulating_holders(Network::Bitcoin, &SchemaAlkaneId { block: 2, tx: 1 })
                .is_none()
        );
    }

    #[test]
    fn busd_is_not_canonical_at_fork_height() {
        let quotes = canonical_quotes_at_height(Network::Bitcoin, BUSD_CANONICAL_QUOTE_FORK_HEIGHT);
        assert!(!quotes.iter().any(|q| q.id == BUSD_ALKANE_ID));
        assert!(quotes.iter().any(|q| q.id == FRBTC_ALKANE_ID));
    }
}
