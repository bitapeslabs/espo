use crate::config::{get_cache_db, get_metashrew, get_network};
use crate::modules::essentials::storage::EssentialsProvider;
use crate::modules::essentials::utils::inspections::resolve_contract_wasm_source;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Context, Result};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

/// The alkabi revision is part of the cache key: an export produced by one
/// revision must never be served after the dependency moves, since what the
/// ABI contains is exactly what changes between revisions.
const ALKABI_CACHE_KEY_PREFIX: &[u8] = b"alkabi:86d04e040416910acfb08012334261b0c27e1575:abi-v1:";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedAlkabi {
    pub contract: String,
    json: String,
    typescript: String,
}

type FlightResult = std::result::Result<Arc<RenderedAlkabi>, String>;

struct AlkabiFlight {
    result: Mutex<Option<FlightResult>>,
    ready: Condvar,
}

static ALKABI_FLIGHTS: OnceLock<Mutex<HashMap<Vec<u8>, Arc<AlkabiFlight>>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlkabiFormat {
    Json,
    TypeScript,
}

impl AlkabiFormat {
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        match raw?.trim().to_ascii_lowercase().as_str() {
            "json" => Some(Self::Json),
            "ts" => Some(Self::TypeScript),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::TypeScript => "ts",
        }
    }

    pub fn render(self, abi: &RenderedAlkabi) -> Result<Value> {
        match self {
            Self::Json => serde_json::from_str(&abi.json).context("decode Alkabi JSON"),
            Self::TypeScript => Ok(Value::String(abi.typescript.clone())),
        }
    }

    pub fn download_body(self, abi: &RenderedAlkabi) -> &str {
        match self {
            Self::Json => &abi.json,
            Self::TypeScript => &abi.typescript,
        }
    }
}

pub fn extract_contract_alkabi(
    provider: &EssentialsProvider,
    alkane: &SchemaAlkaneId,
) -> Result<Arc<RenderedAlkabi>> {
    let source = contract_wasm_source(provider, alkane);
    let Some(cache) = get_cache_db() else {
        let wasm = load_contract_wasm_from_source(&source)?;
        return render_alkabi(&wasm).map(Arc::new);
    };
    load_or_generate(cache, alkabi_cache_key(&source), || {
        let wasm = load_contract_wasm_from_source(&source)?;
        render_alkabi(&wasm)
    })
}

pub fn load_contract_wasm(
    provider: &EssentialsProvider,
    alkane: &SchemaAlkaneId,
) -> Result<Vec<u8>> {
    load_contract_wasm_from_source(&contract_wasm_source(provider, alkane))
}

/// Load bytecode exactly the way the alkanes runtime's `get_alkane_binary`
/// does: read `/alkanes/{id}`, recurse when the payload is a 32-byte factory
/// pointer, otherwise gunzip. No proxy hop — `/implementation` and `/beacon`
/// are never consulted, matching the runtime.
///
/// `prefer_first_version` selects the first stored version of the payload
/// instead of the current one; the runtime always loads the current one, so
/// simulate-style callers want `false`.
pub fn load_contract_wasm_no_resolution(
    alkane: &SchemaAlkaneId,
    prefer_first_version: bool,
) -> Result<(Vec<u8>, SchemaAlkaneId)> {
    let metashrew = get_metashrew();
    let loaded = if prefer_first_version {
        metashrew.get_alkane_wasm_bytes_prefer_first_version(alkane)?
    } else {
        metashrew.get_alkane_wasm_bytes(alkane)?
    };
    loaded.context("contract wasm not found")
}

/// Like `load_contract_wasm`, but also reports which alkane actually provided
/// the bytecode (after proxy/factory resolution) so callers can dedup and
/// content-address the payload.
pub fn load_contract_wasm_with_source(
    provider: &EssentialsProvider,
    alkane: &SchemaAlkaneId,
) -> Result<(Vec<u8>, SchemaAlkaneId)> {
    let source = contract_wasm_source(provider, alkane);
    get_metashrew()
        .get_alkane_wasm_bytes_prefer_first_version(&source)?
        .context("contract wasm not found")
}

fn contract_wasm_source(provider: &EssentialsProvider, alkane: &SchemaAlkaneId) -> SchemaAlkaneId {
    resolve_contract_wasm_source(alkane, provider).unwrap_or(*alkane)
}

fn load_contract_wasm_from_source(source: &SchemaAlkaneId) -> Result<Vec<u8>> {
    get_metashrew()
        .get_alkane_wasm_bytes_prefer_first_version(source)?
        .map(|(wasm, _)| wasm)
        .context("contract wasm not found")
}

fn render_alkabi(wasm: &[u8]) -> Result<RenderedAlkabi> {
    let abi = alkabi::extract::extract_abi(wasm).context("extract Alkabi ABI")?;
    Ok(RenderedAlkabi {
        contract: abi.contract.clone(),
        json: abi.to_json_pretty(),
        typescript: abi.to_ts(),
    })
}

fn alkabi_cache_key(source: &SchemaAlkaneId) -> Vec<u8> {
    let network = get_network().to_string();
    alkabi_cache_key_for(&network, source)
}

fn alkabi_cache_key_for(network: &str, source: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = Vec::with_capacity(ALKABI_CACHE_KEY_PREFIX.len() + network.len() + 1 + 16);
    key.extend_from_slice(ALKABI_CACHE_KEY_PREFIX);
    key.extend_from_slice(network.as_bytes());
    key.push(0);
    key.extend_from_slice(&source.block.to_be_bytes());
    key.extend_from_slice(&source.tx.to_be_bytes());
    key
}

fn read_cached(cache: &DB, key: &[u8]) -> Option<Arc<RenderedAlkabi>> {
    let bytes = match cache.get(key) {
        Ok(value) => value?,
        Err(error) => {
            eprintln!("[alkabi-cache] read failed: {error}");
            return None;
        }
    };
    match serde_json::from_slice::<RenderedAlkabi>(&bytes) {
        Ok(rendered) => Some(Arc::new(rendered)),
        Err(error) => {
            eprintln!("[alkabi-cache] invalid entry discarded: {error}");
            let _ = cache.delete(key);
            None
        }
    }
}

fn write_cached(cache: &DB, key: &[u8], rendered: &RenderedAlkabi) {
    let result = serde_json::to_vec(rendered)
        .context("encode Alkabi cache entry")
        .and_then(|bytes| cache.put(key, bytes).context("write Alkabi cache entry"));
    if let Err(error) = result {
        eprintln!("[alkabi-cache] write failed: {error:#}");
    }
}

fn load_or_generate<F>(cache: Arc<DB>, key: Vec<u8>, generate: F) -> Result<Arc<RenderedAlkabi>>
where
    F: FnOnce() -> Result<RenderedAlkabi>,
{
    if let Some(rendered) = read_cached(&cache, &key) {
        return Ok(rendered);
    }

    let flights = ALKABI_FLIGHTS.get_or_init(|| Mutex::new(HashMap::new()));
    let (flight, leader) = {
        let mut active = flights.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(flight) = active.get(&key) {
            (Arc::clone(flight), false)
        } else {
            let flight = Arc::new(AlkabiFlight { result: Mutex::new(None), ready: Condvar::new() });
            active.insert(key.clone(), Arc::clone(&flight));
            (flight, true)
        }
    };

    if !leader {
        let mut state = flight.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.is_none() {
            state = flight.ready.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let generated = state.as_ref().expect("flight result set before notification").clone();
        drop(state);
        if let Some(rendered) = read_cached(&cache, &key) {
            return Ok(rendered);
        }
        return generated.map_err(anyhow::Error::msg);
    }

    let generated: FlightResult = match catch_unwind(AssertUnwindSafe(generate)) {
        Ok(result) => result.map(Arc::new).map_err(|error| format!("{error:#}")),
        Err(_) => Err("Alkabi generation panicked".to_string()),
    };
    if let Ok(rendered) = &generated {
        write_cached(&cache, &key, rendered);
    }
    {
        let mut state = flight.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = Some(generated.clone());
        flight.ready.notify_all();
    }
    flights.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).remove(&key);
    generated.map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::{
        AlkabiFormat, RenderedAlkabi, alkabi_cache_key_for, load_or_generate, render_alkabi,
    };
    use crate::schemas::SchemaAlkaneId;
    use rocksdb::DB;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn bundled_factory_wasm_renders_json_and_typescript() {
        let wasm = include_bytes!("../../../../test_data/factory.wasm");
        let abi = render_alkabi(wasm).expect("extract Alkabi ABI");
        let json = AlkabiFormat::Json.render(&abi).expect("render Alkabi JSON");
        let typescript = AlkabiFormat::TypeScript
            .render(&abi)
            .expect("render Alkabi TypeScript")
            .as_str()
            .expect("TypeScript string")
            .to_string();

        assert_eq!(json["contract"], abi.contract);
        assert!(json["methods"].as_array().is_some_and(|methods| !methods.is_empty()));
        assert!(typescript.contains(&format!("export const {}Abi", abi.contract)));
    }

    #[test]
    fn concurrent_requests_share_one_job_and_persist_the_result() {
        let dir = tempfile::tempdir_in(".").expect("cache tempdir");
        let cache = Arc::new(DB::open_default(dir.path()).expect("open cache DB"));
        let key = alkabi_cache_key_for("regtest", &SchemaAlkaneId { block: 2, tx: 12345 });
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let key = key.clone();
                let calls = Arc::clone(&calls);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    load_or_generate(cache, key, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(100));
                        Ok(RenderedAlkabi {
                            contract: "CachedContract".to_string(),
                            json: "{\"alkabi\":1,\"contract\":\"CachedContract\",\"types\":{},\"methods\":[]}".to_string(),
                            typescript: "export const CachedContractAbi = {};".to_string(),
                        })
                    })
                    .expect("load or generate")
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            assert_eq!(handle.join().expect("request thread").contract, "CachedContract");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.get(&key).expect("read cache").is_some());

        let cached = load_or_generate(Arc::clone(&cache), key, || {
            panic!("persistent cache hit must not regenerate")
        })
        .expect("persistent cache hit");
        assert_eq!(cached.contract, "CachedContract");
    }

    #[test]
    fn cache_keys_are_scoped_by_network_and_resolved_source() {
        let source = SchemaAlkaneId { block: 2, tx: 12345 };

        assert_ne!(
            alkabi_cache_key_for("mainnet", &source),
            alkabi_cache_key_for("regtest", &source)
        );
        assert_ne!(
            alkabi_cache_key_for("regtest", &source),
            alkabi_cache_key_for("regtest", &SchemaAlkaneId { block: 2, tx: 12346 })
        );
    }

    #[test]
    fn output_format_accepts_only_json_and_ts() {
        assert_eq!(AlkabiFormat::parse(Some("JSON")), Some(AlkabiFormat::Json));
        assert_eq!(AlkabiFormat::parse(Some(" ts ")), Some(AlkabiFormat::TypeScript));
        assert_eq!(AlkabiFormat::parse(Some("typescript")), None);
        assert_eq!(AlkabiFormat::parse(None), None);
    }
}
