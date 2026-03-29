use super::defs::PriceFeed;
use crate::config::get_bitcoind_rpc_client;
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::consts::PRICE_SCALE_DECIMALS;
use anyhow::{Result, anyhow};
use bitcoincore_rpc::RpcApi;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use alloy_primitives::{Address, U256, U512};

const WBTC_USDC_POOL: Address =
    alloy_primitives::address!("99ac8cA7087fA4A2A1FB6357269965A2014ABc35");
const WBTC_TOKEN: Address = alloy_primitives::address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599");
const USDC_TOKEN: Address = alloy_primitives::address!("A0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

const SLOT0_SELECTOR: &str = "0x3850c7bd";
const TOKEN0_SELECTOR: &str = "0x0dfe1681";
const TOKEN1_SELECTOR: &str = "0xd21220a7";
const USDC_DECIMALS: u32 = 6;
const WBTC_DECIMALS: u32 = 8;

#[derive(Clone)]
pub struct UniswapPriceFeed {
    rpc_url: String,
    client: Client,
    eth_call_throttle: Duration,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct EthBlock {
    timestamp: String,
}

impl UniswapPriceFeed {
    pub fn new(rpc_url: impl Into<String>, eth_call_throttle_ms: u64, headers: HashMap<String, String>) -> Self {
        let mut default_headers = HeaderMap::new();
        for (k, v) in &headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                default_headers.insert(name, value);
            } else {
                eprintln!("[ammdata] warning: skipping invalid header: {k}: {v}");
            }
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .default_headers(default_headers)
            .build()
            .expect("build reqwest client");
        Self {
            rpc_url: rpc_url.into(),
            client,
            eth_call_throttle: Duration::from_millis(eth_call_throttle_ms),
        }
    }

    pub fn from_global_config() -> Result<Self> {
        let cfg = AmmDataConfig::load_from_global_config()?;
        Ok(Self::new(cfg.eth_rpc, cfg.eth_call_throttle_ms, cfg.eth_rpc_headers))
    }

    fn rpc_call_with_url<T: DeserializeOwned>(
        &self,
        url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let req = RpcRequest { jsonrpc: "2.0", id: 1, method, params };
        let resp = self.client.post(url).json(&req).send()?.error_for_status()?;
        let resp: RpcResponse<T> = resp.json()?;
        if let Some(err) = resp.error {
            anyhow::bail!("eth rpc error {}: {}", err.code, err.message);
        }
        let result = resp.result.ok_or_else(|| anyhow!("missing result for {method}"))?;
        if !self.eth_call_throttle.is_zero() {
            thread::sleep(self.eth_call_throttle);
        }
        Ok(result)
    }

    fn format_address(addr: &Address) -> String {
        addr.to_string()
    }

    fn parse_hex_u64(raw: &str) -> Result<u64> {
        let trimmed = raw.trim_start_matches("0x");
        u64::from_str_radix(trimmed, 16).map_err(|e| anyhow!("invalid hex u64 '{raw}': {e}"))
    }

    fn eth_call(&self, to: &Address, data: &str, block: Option<u64>) -> Result<String> {
        let call = json!({
            "to": Self::format_address(to),
            "data": data,
        });
        let block_param = match block {
            Some(height) => json!(format!("0x{:x}", height)),
            None => json!("latest"),
        };
        self.rpc_call_with_url(&self.rpc_url, "eth_call", json!([call, block_param]))
    }

    fn get_latest_block_number(&self) -> Result<u64> {
        let hex: String = self.rpc_call_with_url(&self.rpc_url, "eth_blockNumber", json!([]))?;
        Self::parse_hex_u64(&hex)
    }

    fn get_block_timestamp(&self, height: u64) -> Result<u64> {
        let block: EthBlock = self.rpc_call_with_url(
            &self.rpc_url,
            "eth_getBlockByNumber",
            json!([format!("0x{:x}", height), false]),
        )?;
        Self::parse_hex_u64(&block.timestamp)
    }

    fn get_pool_tokens(&self) -> Result<(Address, Address)> {
        let raw0 = self.eth_call(&WBTC_USDC_POOL, TOKEN0_SELECTOR, None)?;
        let raw1 = self.eth_call(&WBTC_USDC_POOL, TOKEN1_SELECTOR, None)?;
        let token0 = Self::decode_address_word(&Self::decode_call_bytes(&raw0)?)?;
        let token1 = Self::decode_address_word(&Self::decode_call_bytes(&raw1)?)?;
        Ok((token0, token1))
    }

    fn decode_call_bytes(raw: &str) -> Result<Vec<u8>> {
        let trimmed = raw.trim_start_matches("0x");
        if trimmed.is_empty() {
            anyhow::bail!("empty eth_call result");
        }
        hex::decode(trimmed).map_err(|e| anyhow!("invalid hex in eth_call: {e}"))
    }

    fn decode_address_word(bytes: &[u8]) -> Result<Address> {
        if bytes.len() < 32 {
            anyhow::bail!("eth_call result too short for address: {} bytes", bytes.len());
        }
        let addr_hex = format!("0x{}", hex::encode(&bytes[12..32]));
        addr_hex
            .parse::<Address>()
            .map_err(|e| anyhow!("invalid address {addr_hex}: {e}"))
    }

    fn get_sqrt_price_x96_at_block(&self, height: u64) -> Result<U256> {
        let raw = self.eth_call(&WBTC_USDC_POOL, SLOT0_SELECTOR, Some(height))?;
        let bytes = Self::decode_call_bytes(&raw)?;
        if bytes.len() < 32 {
            anyhow::bail!("eth_call slot0 response too short: {} bytes", bytes.len());
        }
        Ok(U256::from_be_slice(&bytes[0..32]))
    }

    fn find_closest_block_by_timestamp(&self, target_ts: u64) -> Result<u64> {
        let latest = self.get_latest_block_number()?;
        let mut low = 0u64;
        let mut high = latest;

        while low <= high {
            let mid = (low + high) / 2;
            let ts = self.get_block_timestamp(mid)?;
            if ts == target_ts {
                return Ok(mid);
            }
            if ts < target_ts {
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }

        let mut candidates = Vec::new();
        if high <= latest {
            candidates.push(high);
        }
        if low <= latest {
            candidates.push(low);
        }
        if candidates.is_empty() {
            anyhow::bail!("failed to find eth block candidates for timestamp {target_ts}");
        }

        let mut best = candidates[0];
        let mut best_diff = self.get_block_timestamp(best)?.abs_diff(target_ts);
        for candidate in candidates.into_iter().skip(1) {
            let diff = self.get_block_timestamp(candidate)?.abs_diff(target_ts);
            if diff < best_diff {
                best = candidate;
                best_diff = diff;
            }
        }
        Ok(best)
    }

    fn bitcoin_block_timestamp(height: u64) -> Result<u64> {
        let rpc = get_bitcoind_rpc_client();
        let hash = rpc.get_block_hash(height)?;
        let header = rpc.get_block_header_info(&hash)?;
        Ok(header.time as u64)
    }

    fn price_from_sqrt_price(
        &self,
        sqrt_price_x96: U256,
        token0: Address,
        token1: Address,
    ) -> Result<u128> {
        let scale = 10u128
            .checked_pow((PRICE_SCALE_DECIMALS + WBTC_DECIMALS - USDC_DECIMALS) as u32)
            .ok_or_else(|| anyhow!("price scale overflow"))?;
        let scale = U512::from(scale);
        let sqrt = U512::from(sqrt_price_x96);
        let numerator =
            sqrt.checked_mul(sqrt).ok_or_else(|| anyhow!("sqrtPriceX96 squared overflow"))?;
        if numerator.is_zero() {
            anyhow::bail!("sqrtPriceX96 is zero");
        }
        let q192: U512 = U512::from(1u64) << 192;

        let price = if token0 == WBTC_TOKEN && token1 == USDC_TOKEN {
            let scaled = numerator
                .checked_mul(scale)
                .ok_or_else(|| anyhow!("price numerator overflow"))?;
            scaled / q192
        } else if token0 == USDC_TOKEN && token1 == WBTC_TOKEN {
            let scaled =
                q192.checked_mul(scale).ok_or_else(|| anyhow!("price numerator overflow"))?;
            scaled / numerator
        } else {
            anyhow::bail!("unexpected pool tokens: {token0:?} / {token1:?}");
        };

        u128::try_from(price).map_err(|_| anyhow!("price exceeds u128 range"))
    }

    fn price_at_bitcoin_height(&self, height: u64) -> Result<u128> {
        let btc_ts = Self::bitcoin_block_timestamp(height)?;
        eprintln!("[uniswap] btc height {} -> timestamp {}", height, btc_ts);
        let eth_block = self.find_closest_block_by_timestamp(btc_ts)?;
        eprintln!("[uniswap] closest eth block {} for btc ts {}", eth_block, btc_ts);
        let sqrt_price_x96 = self.get_sqrt_price_x96_at_block(eth_block)?;
        eprintln!("[uniswap] slot0 sqrtPriceX96 at eth block {}: {}", eth_block, sqrt_price_x96);
        let (token0, token1) = self.get_pool_tokens()?;
        self.price_from_sqrt_price(sqrt_price_x96, token0, token1)
    }
}

impl PriceFeed for UniswapPriceFeed {
    fn get_bitcoin_price_usd_at_block_height(&self, height: u64) -> Result<u128> {
        self.price_at_bitcoin_height(height)
    }
}
