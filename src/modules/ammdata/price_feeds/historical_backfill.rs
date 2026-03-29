use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

const BTC_USD_HISTORICAL_REL_PATH: &str = "resources/btc_usd_historical.json";
static BTC_USD_HISTORICAL_BACKFILL: OnceLock<Result<BTreeMap<u64, u128>, String>> = OnceLock::new();

#[derive(Deserialize)]
struct BtcUsdHistoricalPoint {
    height: u64,
    price_scaled: String,
}

#[derive(Deserialize)]
struct BtcUsdHistoricalFile {
    points: Vec<BtcUsdHistoricalPoint>,
}

pub fn get_historical_btc_usd_price(height: u64) -> Result<Option<u128>> {
    let result = BTC_USD_HISTORICAL_BACKFILL
        .get_or_init(|| load_historical_backfill().map_err(|e| e.to_string()));
    match result {
        Ok(prices) => Ok(prices.range(..=height).next_back().map(|(_h, p)| *p)),
        Err(err) => Err(anyhow!("historical btc/usd backfill load failed: {err}")),
    }
}

fn load_historical_backfill() -> Result<BTreeMap<u64, u128>> {
    let path = historical_backfill_path();
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: BtcUsdHistoricalFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    let mut prices = BTreeMap::new();
    for point in parsed.points {
        let price = point.price_scaled.parse::<u128>().with_context(|| {
            format!(
                "invalid price_scaled '{}' at bitcoin height {}",
                point.price_scaled, point.height
            )
        })?;
        prices.insert(point.height, price);
    }
    Ok(prices)
}

fn historical_backfill_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(BTC_USD_HISTORICAL_REL_PATH)
}
