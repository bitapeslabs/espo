use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use serde_json::Value;

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
    pub espo_pricer_host: String,
    pub use_historical_backfill: bool,
    pub pre_ammdata_btc_usd_price: u128,
    pub search_index_enabled: bool,
    pub search_prefix_min_len: u8,
    pub search_prefix_max_len: u8,
    pub search_fallback_scan_cap: u64,
    pub search_limit_cap: u64,
    pub derived_liquidity: Option<DerivedLiquidityConfig>,
}

impl AmmDataConfig {
    pub fn spec() -> &'static str {
        "{ \"espo_pricer_host\": \"http://127.0.0.1:6901\", \"use_historical_backfill\": <bool=true>, \"pre_ammdata_btc_usd_price\": <86500 or \"86500.12\">, \"search_index_enabled\": <bool>, \"search_prefix_min\": <2>, \"search_prefix_max\": <6>, \"search_fallback_scan_cap\": <num>, \"search_limit_cap\": <num>, \"derived_liquidity\": [ { \"alkane\": \"2:0\", \"strategy\": \"neutral|neutral-vwap|optimistic|pessimistic\" } ] }"
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let obj = value.as_object().ok_or_else(|| {
            anyhow!("ammdata config must be an object; expected: {}", Self::spec())
        })?;
        let espo_pricer_host_val = obj.get("espo_pricer_host").ok_or_else(|| {
            anyhow!("ammdata.espo_pricer_host missing; expected: {}", Self::spec())
        })?;
        let espo_pricer_host_raw = espo_pricer_host_val.as_str().ok_or_else(|| {
            anyhow!("ammdata.espo_pricer_host must be a string; expected: {}", Self::spec())
        })?;
        let espo_pricer_host = normalize_espo_pricer_host(espo_pricer_host_raw)?;
        let use_historical_backfill =
            obj.get("use_historical_backfill").and_then(|v| v.as_bool()).unwrap_or(true);
        let pre_ammdata_btc_usd_price = obj
            .get("pre_ammdata_btc_usd_price")
            .map(parse_scaled_price_value)
            .transpose()?
            .unwrap_or(0);

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

        Ok(Self {
            espo_pricer_host,
            use_historical_backfill,
            pre_ammdata_btc_usd_price,
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

fn normalize_espo_pricer_host(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("ammdata.espo_pricer_host must be set; expected: {}", AmmDataConfig::spec());
    }
    let with_scheme =
        if trimmed.contains("://") { trimmed.to_string() } else { format!("http://{trimmed}") };
    let parsed = reqwest::Url::parse(&with_scheme).map_err(|e| {
        anyhow!("ammdata.espo_pricer_host must be an absolute URL or host:port: {e}")
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        anyhow::bail!(
            "ammdata.espo_pricer_host must be an http/https URL; got scheme '{}'",
            parsed.scheme()
        );
    }
    Ok(with_scheme)
}

fn parse_alkane_id_str(raw: &str) -> Option<SchemaAlkaneId> {
    let mut parts = raw.split(':');
    let block = parts.next()?.parse::<u32>().ok()?;
    let tx = parts.next()?.parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}

fn parse_scaled_price_value(value: &Value) -> Result<u128> {
    if let Some(raw) = value.as_u64() {
        return Ok((raw as u128).saturating_mul(PRICE_SCALE));
    }
    let raw = value
        .as_str()
        .ok_or_else(|| anyhow!("ammdata.pre_ammdata_btc_usd_price must be a number or string"))?
        .trim();
    if raw.is_empty() {
        return Err(anyhow!("ammdata.pre_ammdata_btc_usd_price must not be empty"));
    }
    let (whole_raw, frac_raw) = raw.split_once('.').unwrap_or((raw, ""));
    let whole = whole_raw
        .parse::<u128>()
        .map_err(|_| anyhow!("invalid ammdata.pre_ammdata_btc_usd_price"))?;
    let frac_digits = frac_raw.chars().filter(|c| c.is_ascii_digit()).take(16).collect::<String>();
    let mut frac_scaled = frac_digits;
    while frac_scaled.len() < 16 {
        frac_scaled.push('0');
    }
    let frac = if frac_scaled.is_empty() {
        0
    } else {
        frac_scaled
            .parse::<u128>()
            .map_err(|_| anyhow!("invalid ammdata.pre_ammdata_btc_usd_price"))?
    };
    Ok(whole.saturating_mul(PRICE_SCALE).saturating_add(frac))
}
