use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum DerivedMergeStrategy {
    Neutral,
    NeutralVwap,
    Optimistic,
    Pessimistic,
}

#[derive(Clone, Debug)]
pub struct DerivedQuoteConfig {
    pub alkane: SchemaAlkaneId,
    pub strategy: DerivedMergeStrategy,
}

#[derive(Clone, Debug)]
pub struct DerivedLiquidityConfig {
    pub derived_quotes: Vec<DerivedQuoteConfig>,
}

#[derive(Clone, Debug)]
pub struct AmmDataConfig {
    pub eth_rpc: String,
    pub eth_rpc_headers: HashMap<String, String>,
    pub eth_call_throttle_ms: u64,
    pub use_historical_backfill: bool,
    pub search_index_enabled: bool,
    pub search_prefix_min_len: u8,
    pub search_prefix_max_len: u8,
    pub search_fallback_scan_cap: u64,
    pub search_limit_cap: u64,
    pub derived_liquidity: Option<DerivedLiquidityConfig>,
}

impl AmmDataConfig {
    pub fn spec() -> &'static str {
        "{ \"eth_rpc\": \"<url>\", \"eth_call_throttle\": <ms>, \"use_historical_backfill\": <bool=true>, \"search_index_enabled\": <bool>, \"search_prefix_min\": <2>, \"search_prefix_max\": <6>, \"search_fallback_scan_cap\": <num>, \"search_limit_cap\": <num>, \"derived_liquidity\": [ { \"alkane\": \"2:0\", \"strategy\": \"neutral|neutral-vwap|optimistic|pessimistic\" } ] }"
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let obj = value.as_object().ok_or_else(|| {
            anyhow!("ammdata config must be an object; expected: {}", Self::spec())
        })?;
        let eth_rpc_val = obj
            .get("eth_rpc")
            .ok_or_else(|| anyhow!("ammdata.eth_rpc missing; expected: {}", Self::spec()))?;
        let eth_call_throttle_val = obj.get("eth_call_throttle").ok_or_else(|| {
            anyhow!("ammdata.eth_call_throttle missing; expected: {}", Self::spec())
        })?;
        let eth_rpc = eth_rpc_val
            .as_str()
            .ok_or_else(|| anyhow!("ammdata.eth_rpc must be a string; expected: {}", Self::spec()))?
            .trim()
            .to_string();
        if eth_rpc.is_empty() {
            anyhow::bail!("ammdata.eth_rpc must be set; expected: {}", Self::spec());
        }
        let eth_call_throttle_ms = eth_call_throttle_val.as_u64().ok_or_else(|| {
            anyhow!(
                "ammdata.eth_call_throttle must be a non-negative integer; expected: {}",
                Self::spec()
            )
        })?;
        let use_historical_backfill =
            obj.get("use_historical_backfill").and_then(|v| v.as_bool()).unwrap_or(true);

        let eth_rpc_headers = match obj.get("eth_rpc_headers") {
            Some(Value::Object(map)) => {
                let mut headers = HashMap::new();
                for (k, v) in map {
                    let val = v.as_str().ok_or_else(|| {
                        anyhow!("ammdata.eth_rpc_headers values must be strings; key '{}' is not", k)
                    })?;
                    headers.insert(k.clone(), val.to_string());
                }
                headers
            }
            Some(Value::Null) | None => HashMap::new(),
            _ => anyhow::bail!("ammdata.eth_rpc_headers must be an object mapping header names to string values"),
        };

        let search_index_enabled =
            obj.get("search_index_enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        let search_prefix_min_len = obj
            .get("search_prefix_min")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(2);
        let search_prefix_max_len = obj
            .get("search_prefix_max")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(6);
        let search_fallback_scan_cap =
            obj.get("search_fallback_scan_cap").and_then(|v| v.as_u64()).unwrap_or(5000);
        let search_limit_cap = obj.get("search_limit_cap").and_then(|v| v.as_u64()).unwrap_or(20);

        let derived_liquidity = match obj.get("derived_liquidity") {
            None => None,
            Some(Value::Null) => None,
            Some(val) => {
                let derived_quotes_arr = if let Some(arr) = val.as_array() {
                    arr
                } else if let Some(dl_obj) = val.as_object() {
                    let derived_quotes_val = dl_obj.get("derived_quotes").ok_or_else(|| {
                        anyhow!(
                            "ammdata.derived_liquidity.derived_quotes missing; expected: {}",
                            Self::spec()
                        )
                    })?;
                    derived_quotes_val.as_array().ok_or_else(|| {
                        anyhow!(
                            "ammdata.derived_liquidity.derived_quotes must be an array; expected: {}",
                            Self::spec()
                        )
                    })?
                } else {
                    return Err(anyhow!(
                        "ammdata.derived_liquidity must be an array; expected: {}",
                        Self::spec()
                    ));
                };

                let mut derived_quotes = Vec::new();
                for entry in derived_quotes_arr {
                    let entry_obj = entry.as_object().ok_or_else(|| {
                        anyhow!(
                            "ammdata.derived_liquidity entries must be objects; expected: {}",
                            Self::spec()
                        )
                    })?;
                    let alkane_str =
                        entry_obj.get("alkane").and_then(|v| v.as_str()).ok_or_else(|| {
                            anyhow!(
                                "ammdata.derived_liquidity[].alkane must be a string; expected: {}",
                                Self::spec()
                            )
                        })?;
                    let alkane = parse_alkane_id_str(alkane_str).ok_or_else(|| {
                        anyhow!(
                            "ammdata.derived_liquidity[].alkane must be like \"2:0\"; got {}",
                            alkane_str
                        )
                    })?;
                    let strategy_str =
                        entry_obj.get("strategy").and_then(|v| v.as_str()).ok_or_else(|| {
                            anyhow!(
                                "ammdata.derived_liquidity[].strategy missing; expected: {}",
                                Self::spec()
                            )
                        })?;
                    let strategy = match strategy_str.trim().to_ascii_lowercase().as_str() {
                        "neutral" => DerivedMergeStrategy::Neutral,
                        "neutral-vwap" | "neutral_vwap" => DerivedMergeStrategy::NeutralVwap,
                        "optimistic" => DerivedMergeStrategy::Optimistic,
                        "pessimistic" => DerivedMergeStrategy::Pessimistic,
                        _ => {
                            return Err(anyhow!(
                                "ammdata.derived_liquidity[].strategy must be neutral|neutral-vwap|optimistic|pessimistic; got {}",
                                strategy_str
                            ));
                        }
                    };
                    derived_quotes.push(DerivedQuoteConfig { alkane, strategy });
                }
                Some(DerivedLiquidityConfig { derived_quotes })
            }
        };

        let parsed = reqwest::Url::parse(&eth_rpc)
            .map_err(|e| anyhow!("ammdata.eth_rpc must be an absolute URL (http/https): {e}"))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            anyhow::bail!(
                "ammdata.eth_rpc must be an http/https URL; got scheme '{}'",
                parsed.scheme()
            );
        }

        Ok(Self {
            eth_rpc,
            eth_rpc_headers,
            eth_call_throttle_ms,
            use_historical_backfill,
            search_index_enabled,
            search_prefix_min_len,
            search_prefix_max_len,
            search_fallback_scan_cap,
            search_limit_cap,
            derived_liquidity,
        })
    }

    pub fn load_from_global_config() -> Result<Self> {
        let value = crate::config::get_module_config("ammdata")
            .ok_or_else(|| {
                anyhow!(
                    "No config defined for ammdata module, but ammdata module was loaded and defines a config. Expected: {}",
                    Self::spec()
                )
            })?;
        Self::from_value(value)
    }
}

fn parse_alkane_id_str(raw: &str) -> Option<SchemaAlkaneId> {
    let mut parts = raw.split(':');
    let block = parts.next()?.parse::<u32>().ok()?;
    let tx = parts.next()?.parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}
