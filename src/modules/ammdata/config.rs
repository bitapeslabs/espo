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
    /// Height this alkane starts acting as derived liquidity. `None` means from
    /// the ammdata genesis. An entry with a height is a *fork*: the first time a
    /// token gets a candle against it, the token's history against the default
    /// quote (the first entry) is copied across so the series continues.
    pub height: Option<u32>,
}

impl DerivedQuoteConfig {
    pub fn active_at(&self, height: u32) -> bool {
        self.height.is_none_or(|h| height >= h)
    }
}

#[derive(Clone, Debug)]
pub struct DerivedLiquidityConfig {
    pub derived_quotes: Vec<DerivedQuoteConfig>,
}

/// From `height` on, trading on the source alkane's pool against `alkane` is
/// ignored by the source's derived chart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropForkConfig {
    pub alkane: SchemaAlkaneId,
    pub height: u32,
}

/// Per-alkane chart routing, so a consumer can ask for `<alkane>-full` and not
/// track which derived quote the alkane is on.
#[derive(Clone, Debug)]
pub struct DeriveSourceConfig {
    pub alkane: SchemaAlkaneId,
    /// Derived quote `-full` proxies to. Falls back to the default (first
    /// `derived_liquidity` entry) when unset or not yet active.
    pub target: Option<SchemaAlkaneId>,
    pub drop_forks: Vec<DropForkConfig>,
    /// Candles before this height are not returned on any of the alkane's charts.
    pub start_offset: Option<u32>,
}

/// What the indexer needs about derived liquidity at one block height.
#[derive(Clone, Debug, Default)]
pub struct DerivedIndexPlan {
    /// Derived quotes in force at this height, in config order.
    pub active_quotes: Vec<DerivedQuoteConfig>,
    /// The default quote: forked quotes copy their history from it.
    pub fork_source: Option<SchemaAlkaneId>,
    /// Active quotes that came into force at a height (never the default).
    pub forked_quotes: Vec<SchemaAlkaneId>,
    /// `(source alkane, dropped quote, height)`.
    pub drop_forks: Vec<(SchemaAlkaneId, SchemaAlkaneId, u32)>,
}

impl DerivedIndexPlan {
    pub fn is_forked_quote(&self, quote: &SchemaAlkaneId) -> bool {
        self.forked_quotes.contains(quote)
    }

    /// True when trading on `token`'s pool against `other` is ignored at `height`.
    /// `other` may be a derived quote or a canonical one; what that means for the
    /// chart differs and is decided by the caller.
    pub fn pair_dropped(
        &self,
        token: &SchemaAlkaneId,
        other: &SchemaAlkaneId,
        height: u32,
    ) -> bool {
        self.drop_forks.iter().any(|(t, o, h)| t == token && o == other && height >= *h)
    }

    /// True when every canonical quote in force is dropped for `token`: its
    /// canonical pricing (the direct USD candle) must then stay out of its derived
    /// charts, which are left following the derived quote's pool alone.
    ///
    /// The direct USD candle is one merged series across all canonical pools, so
    /// it can only be left out whole. A token that drops some canonical quotes but
    /// not all keeps the leg.
    pub fn canonical_leg_dropped(
        &self,
        token: &SchemaAlkaneId,
        canonical_quotes: &[SchemaAlkaneId],
        height: u32,
    ) -> bool {
        !canonical_quotes.is_empty()
            && canonical_quotes.iter().all(|cq| self.pair_dropped(token, cq, height))
    }

    /// Tokens whose canonical leg is dropped at `height`.
    pub fn canonical_dropped_tokens(
        &self,
        canonical_quotes: &[SchemaAlkaneId],
        height: u32,
    ) -> Vec<SchemaAlkaneId> {
        let mut out: Vec<SchemaAlkaneId> = Vec::new();
        for (token, _, _) in self.drop_forks.iter() {
            if !out.contains(token) && self.canonical_leg_dropped(token, canonical_quotes, height) {
                out.push(*token);
            }
        }
        out
    }
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
    pub derive_sources: Vec<DeriveSourceConfig>,
}

impl AmmDataConfig {
    pub fn derived_quotes(&self) -> &[DerivedQuoteConfig] {
        self.derived_liquidity
            .as_ref()
            .map(|c| c.derived_quotes.as_slice())
            .unwrap_or_default()
    }

    /// The default derived quote: the first `derived_liquidity` entry.
    pub fn default_derived_quote(&self) -> Option<SchemaAlkaneId> {
        self.derived_quotes().first().map(|q| q.alkane)
    }

    pub fn active_derived_quotes(&self, height: u32) -> Vec<DerivedQuoteConfig> {
        self.derived_quotes().iter().filter(|q| q.active_at(height)).cloned().collect()
    }

    pub fn derive_source(&self, token: &SchemaAlkaneId) -> Option<&DeriveSourceConfig> {
        self.derive_sources.iter().find(|s| s.alkane == *token)
    }

    pub fn start_offset(&self, token: &SchemaAlkaneId) -> Option<u32> {
        self.derive_source(token).and_then(|s| s.start_offset)
    }

    /// The derived quote `<token>-full` proxies to at `height`: the token's
    /// configured target if it is active, else the default quote if that is
    /// active, else `None` (serve the direct chart).
    pub fn full_target(&self, token: &SchemaAlkaneId, height: u32) -> Option<SchemaAlkaneId> {
        let is_active = |q: &SchemaAlkaneId| {
            self.derived_quotes().iter().any(|d| d.alkane == *q && d.active_at(height))
        };
        if let Some(target) = self.derive_source(token).and_then(|s| s.target) {
            if is_active(&target) {
                return Some(target);
            }
        }
        self.default_derived_quote().filter(|q| is_active(q))
    }

    pub fn derived_index_plan(&self, height: u32) -> DerivedIndexPlan {
        let fork_source = self.default_derived_quote();
        let active_quotes = self.active_derived_quotes(height);
        let forked_quotes = active_quotes
            .iter()
            .filter(|q| q.height.is_some() && Some(q.alkane) != fork_source)
            .map(|q| q.alkane)
            .collect();
        let drop_forks = self
            .derive_sources
            .iter()
            .flat_map(|s| s.drop_forks.iter().map(move |d| (s.alkane, d.alkane, d.height)))
            .collect();
        DerivedIndexPlan { active_quotes, fork_source, forked_quotes, drop_forks }
    }

    pub fn spec() -> &'static str {
        "{ \"espo_pricer_host\": \"http://127.0.0.1:6901\", \"use_historical_backfill\": <bool=true>, \"pre_ammdata_btc_usd_price\": <86500 or \"86500.12\">, \"search_index_enabled\": <bool>, \"search_prefix_min\": <2>, \"search_prefix_max\": <6>, \"search_fallback_scan_cap\": <num>, \"search_limit_cap\": <num>, \"derived_liquidity\": [ { \"alkane\": \"2:0\", \"strategy\": \"neutral|neutral-vwap|optimistic|pessimistic\", \"height\": <optional u32> } ], \"derive_sources\": [ { \"alkane\": \"2:68479\", \"target\": \"2:0\", \"drop_forks\": [ { \"alkane\": \"2:0\", \"height\": <u32> } ], \"start_offset\": <optional u32> } ] }"
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
                    let height = match entry_obj.get("height") {
                        None | Some(Value::Null) => None,
                        Some(v) => Some(parse_height(v).ok_or_else(|| {
                            anyhow!(
                                "ammdata.derived_liquidity[].height must be a block height; got {}",
                                v
                            )
                        })?),
                    };
                    derived_quotes.push(DerivedQuoteConfig { alkane, strategy, height });
                }
                Some(DerivedLiquidityConfig { derived_quotes })
            }
        };

        let mut derive_sources: Vec<DeriveSourceConfig> = Vec::new();
        // `derived_sources` is accepted as an alias; the two spellings are easy to mix up.
        if let Some(raw) = obj.get("derive_sources").or_else(|| obj.get("derived_sources")) {
            let known_quotes: Vec<SchemaAlkaneId> = derived_liquidity
                .as_ref()
                .map(|c| c.derived_quotes.iter().map(|q| q.alkane).collect())
                .unwrap_or_default();
            let entries = raw.as_array().ok_or_else(|| {
                anyhow!("ammdata.derive_sources must be an array; expected: {}", Self::spec())
            })?;
            for entry in entries {
                let entry_obj = entry.as_object().ok_or_else(|| {
                    anyhow!(
                        "ammdata.derive_sources entries must be objects; expected: {}",
                        Self::spec()
                    )
                })?;
                let alkane_str =
                    entry_obj.get("alkane").and_then(|v| v.as_str()).ok_or_else(|| {
                        anyhow!(
                            "ammdata.derive_sources[].alkane must be a string; expected: {}",
                            Self::spec()
                        )
                    })?;
                let alkane = parse_alkane_id_str(alkane_str).ok_or_else(|| {
                    anyhow!(
                        "ammdata.derive_sources[].alkane must be like \"2:0\"; got {}",
                        alkane_str
                    )
                })?;
                if derive_sources.iter().any(|s| s.alkane == alkane) {
                    return Err(anyhow!(
                        "ammdata.derive_sources lists {} more than once",
                        alkane_str
                    ));
                }
                let target = match entry_obj.get("target") {
                    None | Some(Value::Null) => None,
                    Some(v) => {
                        let target_str = v.as_str().ok_or_else(|| {
                            anyhow!("ammdata.derive_sources[].target must be a string like \"2:0\"")
                        })?;
                        let target = parse_alkane_id_str(target_str).ok_or_else(|| {
                            anyhow!(
                                "ammdata.derive_sources[].target must be like \"2:0\"; got {}",
                                target_str
                            )
                        })?;
                        // A target that is not a derived quote at all can never become
                        // active, so it is a mistake rather than a not-yet.
                        if !known_quotes.contains(&target) {
                            return Err(anyhow!(
                                "ammdata.derive_sources[{}].target {} is not in derived_liquidity",
                                alkane_str,
                                target_str
                            ));
                        }
                        Some(target)
                    }
                };
                let mut drop_forks: Vec<DropForkConfig> = Vec::new();
                if let Some(v) = entry_obj.get("drop_forks") {
                    let arr = v.as_array().ok_or_else(|| {
                        anyhow!("ammdata.derive_sources[].drop_forks must be an array")
                    })?;
                    for d in arr {
                        let d_obj = d.as_object().ok_or_else(|| {
                            anyhow!("ammdata.derive_sources[].drop_forks entries must be objects")
                        })?;
                        let d_str =
                            d_obj.get("alkane").and_then(|v| v.as_str()).ok_or_else(|| {
                                anyhow!(
                                    "ammdata.derive_sources[].drop_forks[].alkane must be a string"
                                )
                            })?;
                        let d_alkane = parse_alkane_id_str(d_str).ok_or_else(|| {
                            anyhow!("ammdata.derive_sources[].drop_forks[].alkane must be like \"2:0\"; got {}", d_str)
                        })?;
                        let d_height = d_obj.get("height").and_then(parse_height).ok_or_else(|| {
                            anyhow!("ammdata.derive_sources[].drop_forks[].height must be a block height")
                        })?;
                        drop_forks.push(DropForkConfig { alkane: d_alkane, height: d_height });
                    }
                }
                let start_offset = match entry_obj.get("start_offset") {
                    None | Some(Value::Null) => None,
                    Some(v) => Some(parse_height(v).ok_or_else(|| {
                        anyhow!(
                            "ammdata.derive_sources[].start_offset must be a block height; got {}",
                            v
                        )
                    })?),
                };
                derive_sources.push(DeriveSourceConfig {
                    alkane,
                    target,
                    drop_forks,
                    start_offset,
                });
            }
        }

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
            derive_sources,
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

fn parse_height(value: &Value) -> Option<u32> {
    value.as_u64().and_then(|h| u32::try_from(h).ok())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const DIESEL: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 0 };
    const TORT: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 68479 };
    const OTHER: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 77 };

    fn cfg(extra: Value) -> Result<AmmDataConfig> {
        let mut m = serde_json::Map::new();
        m.insert("espo_pricer_host".into(), json!("http://127.0.0.1:6901"));
        for (k, v) in extra.as_object().unwrap() {
            m.insert(k.clone(), v.clone());
        }
        AmmDataConfig::from_value(&Value::Object(m))
    }

    fn forked() -> AmmDataConfig {
        cfg(json!({
            "derived_liquidity": [
                { "alkane": "2:0", "strategy": "neutral-vwap" },
                { "alkane": "2:68479", "strategy": "neutral-vwap", "height": 969925 }
            ],
            "derive_sources": [
                { "alkane": "2:77", "target": "2:68479" },
                { "alkane": "2:68479", "target": "2:0",
                  "drop_forks": [{ "alkane": "2:0", "height": 970000 }],
                  "start_offset": 960000 }
            ]
        }))
        .expect("parses")
    }

    #[test]
    fn a_quote_with_a_height_is_inactive_until_it() {
        let c = forked();
        assert_eq!(c.default_derived_quote(), Some(DIESEL));
        assert_eq!(c.active_derived_quotes(969_924).len(), 1);
        assert_eq!(c.active_derived_quotes(969_925).len(), 2);

        let before = c.derived_index_plan(969_924);
        assert!(before.forked_quotes.is_empty());
        let after = c.derived_index_plan(969_925);
        assert_eq!(after.fork_source, Some(DIESEL));
        assert_eq!(after.forked_quotes, vec![TORT]);
        // The default quote is never a fork, even if it is what everything forks from.
        assert!(!after.is_forked_quote(&DIESEL));
    }

    #[test]
    fn full_proxies_to_the_target_only_once_it_is_active() {
        let c = forked();
        // 2:77 targets TORT, which is not in force yet: falls back to the default.
        assert_eq!(c.full_target(&OTHER, 969_924), Some(DIESEL));
        assert_eq!(c.full_target(&OTHER, 969_925), Some(TORT));
        // No derive_sources entry: the default.
        assert_eq!(c.full_target(&SchemaAlkaneId { block: 2, tx: 5 }, 969_925), Some(DIESEL));
    }

    #[test]
    fn full_has_no_target_without_derived_liquidity() {
        let c = cfg(json!({})).expect("parses");
        assert_eq!(c.full_target(&OTHER, 1_000_000), None);
        assert!(c.derived_index_plan(1_000_000).active_quotes.is_empty());
    }

    #[test]
    fn drop_forks_apply_to_the_source_pair_from_their_height() {
        let plan = forked().derived_index_plan(970_000);
        assert!(!plan.pair_dropped(&TORT, &DIESEL, 969_999));
        assert!(plan.pair_dropped(&TORT, &DIESEL, 970_000));
        // Only that source, only that quote.
        assert!(!plan.pair_dropped(&OTHER, &DIESEL, 970_000));
        assert!(!plan.pair_dropped(&TORT, &OTHER, 970_000));
    }

    #[test]
    fn dropping_the_canonical_quote_drops_the_canonical_leg() {
        // The shape of the mainnet example: SLICE moves onto TORT and stops taking
        // its frBTC pool, both at the same block.
        let frbtc = SchemaAlkaneId { block: 32, tx: 0 };
        let busd = SchemaAlkaneId { block: 2, tx: 56801 };
        let slice = SchemaAlkaneId { block: 4, tx: 8888 };
        let c = cfg(json!({
            "derived_liquidity": [
                { "alkane": "2:0", "strategy": "neutral-vwap" },
                { "alkane": "2:68479", "strategy": "neutral-vwap", "height": 969931 }
            ],
            "derive_sources": [
                { "alkane": "4:8888", "target": "2:68479", "start_offset": 968034,
                  "drop_forks": [{ "alkane": "32:0", "height": 969931 }] }
            ]
        }))
        .expect("parses");
        assert_eq!(c.start_offset(&slice), Some(968_034));
        assert_eq!(c.full_target(&slice, 969_930), Some(DIESEL));
        assert_eq!(c.full_target(&slice, 969_931), Some(TORT));

        let before = c.derived_index_plan(969_930);
        assert!(!before.canonical_leg_dropped(&slice, &[frbtc], 969_930));
        assert!(before.canonical_dropped_tokens(&[frbtc], 969_930).is_empty());

        let at = c.derived_index_plan(969_931);
        assert!(at.canonical_leg_dropped(&slice, &[frbtc], 969_931));
        assert_eq!(at.canonical_dropped_tokens(&[frbtc], 969_931), vec![slice]);
        // Another token is untouched.
        assert!(!at.canonical_leg_dropped(&TORT, &[frbtc], 969_931));
        // With a second canonical quote still in force the leg stays: the direct
        // USD candle is one merged series and cannot be half-excluded.
        assert!(!at.canonical_leg_dropped(&slice, &[frbtc, busd], 969_931));
        // No canonical quotes at all is not "all dropped".
        assert!(!at.canonical_leg_dropped(&slice, &[], 969_931));
    }

    #[test]
    fn the_committed_example_config_parses_and_means_what_the_docs_say() {
        // example_configs/config.json is documentation people copy from; this keeps
        // it loadable and pins the scenario docs/1-forkable-derived-liquidity.md
        // walks through.
        let raw = include_str!("../../../example_configs/config.json");
        let root: Value = serde_json::from_str(raw).expect("example is valid json");
        let c = AmmDataConfig::from_value(&root["modules"]["ammdata"]).expect("ammdata parses");

        let frbtc = SchemaAlkaneId { block: 32, tx: 0 };
        let slice = SchemaAlkaneId { block: 4, tx: 8888 };

        assert_eq!(c.default_derived_quote(), Some(DIESEL));
        assert_eq!(c.derived_quotes().len(), 2);
        assert_eq!(c.derived_quotes()[1].alkane, TORT);
        assert_eq!(c.derived_quotes()[1].height, Some(969_931));

        assert_eq!(c.start_offset(&slice), Some(968_034));
        assert_eq!(c.full_target(&slice, 969_930), Some(DIESEL));
        assert_eq!(c.full_target(&slice, 969_931), Some(TORT));

        let plan = c.derived_index_plan(969_931);
        assert_eq!(plan.forked_quotes, vec![TORT]);
        assert!(plan.canonical_leg_dropped(&slice, &[frbtc], 969_931));
        assert!(!c.derived_index_plan(969_930).canonical_leg_dropped(&slice, &[frbtc], 969_930));

        // The example must never carry real credentials.
        assert!(root["bitcoind_rpc_pass"].as_str().unwrap().starts_with('<'));
        assert!(root["modules"]["memgraph"]["password"].as_str().unwrap().starts_with('<'));
    }

    #[test]
    fn start_offset_is_per_alkane() {
        let c = forked();
        assert_eq!(c.start_offset(&TORT), Some(960_000));
        assert_eq!(c.start_offset(&OTHER), None);
    }

    #[test]
    fn a_target_that_is_not_a_derived_quote_is_rejected() {
        let err = cfg(json!({
            "derived_liquidity": [{ "alkane": "2:0", "strategy": "neutral" }],
            "derive_sources": [{ "alkane": "2:77", "target": "2:999" }]
        }))
        .expect_err("rejects");
        assert!(err.to_string().contains("is not in derived_liquidity"), "{err}");
    }

    #[test]
    fn derive_sources_accepts_the_derived_sources_spelling_and_optional_fields() {
        let c = cfg(json!({
            "derived_liquidity": [{ "alkane": "2:0", "strategy": "neutral" }],
            "derived_sources": [{ "alkane": "2:77", "start_offset": 5 }]
        }))
        .expect("parses");
        let src = c.derive_source(&OTHER).expect("entry");
        assert_eq!(src.target, None);
        assert!(src.drop_forks.is_empty());
        assert_eq!(src.start_offset, Some(5));
    }

    #[test]
    fn existing_configs_without_heights_still_parse() {
        let c =
            cfg(json!({ "derived_liquidity": [{ "alkane": "2:0", "strategy": "neutral-vwap" }] }))
                .expect("parses");
        assert_eq!(c.derived_quotes()[0].height, None);
        assert!(c.derived_quotes()[0].active_at(0));
        assert!(c.derive_sources.is_empty());
    }
}
