//! Blocking JSON-RPC client used by `Mdb` when `explorer_espo_rpc_host` is
//! configured: instead of reading the local RocksDB, every Mdb read primitive
//! is fulfilled by the `internal.*` RPC methods of a remote espo instance
//! (which must run with `enable_internal_rpc: true`).
//!
//! The client is deliberately synchronous (ureq) so it can be called from the
//! same call sites that previously performed synchronous RocksDB reads —
//! including inside async handlers, where it merely blocks the thread the
//! same way a local RocksDB read would (just for longer).

use bitcoin::BlockHash;
use bitcoin::hashes::Hash;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Bounded, short-TTL memo for point lookups: SSR pages re-read the same hot
/// rows (icons, inspections, counters) on every page view, so a couple of
/// seconds of reuse collapses most repeat round-trips without meaningfully
/// staleing an explorer. Cleared wholesale when it grows past the cap.
const GET_CACHE_MAX_ENTRIES: usize = 100_000;

pub struct RemoteMdbClient {
    rpc_url: String,
    agent: ureq::Agent,
    /// Shared secret attached to `internal.*` calls; the remote rejects those
    /// methods without it.
    key: Option<String>,
    cache_ttl: Duration,
    get_cache: Mutex<HashMap<(Vec<u8>, Vec<u8>), (Instant, Option<Vec<u8>>)>>,
    calls_total: AtomicU64,
}

pub type RemoteResult<T> = Result<T, String>;

fn hex_bytes(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

fn parse_hex(value: &Value, what: &str) -> RemoteResult<Vec<u8>> {
    let raw = value.as_str().ok_or_else(|| format!("{what}: expected hex string"))?;
    hex::decode(raw.strip_prefix("0x").unwrap_or(raw)).map_err(|e| format!("{what}: {e}"))
}

fn parse_opt_hex(value: &Value, what: &str) -> RemoteResult<Option<Vec<u8>>> {
    if value.is_null() {
        return Ok(None);
    }
    parse_hex(value, what).map(Some)
}

impl RemoteMdbClient {
    pub fn new(host: &str) -> Self {
        Self::new_with(host, None, Duration::ZERO)
    }

    pub fn new_with(host: &str, key: Option<String>, cache_ttl: Duration) -> Self {
        let trimmed = host.trim_end_matches('/');
        let rpc_url =
            if trimmed.ends_with("/rpc") { trimmed.to_string() } else { format!("{trimmed}/rpc") };
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build();
        Self {
            rpc_url,
            agent,
            key,
            cache_ttl,
            get_cache: Mutex::new(HashMap::new()),
            calls_total: AtomicU64::new(0),
        }
    }

    /// Total JSON-RPC round-trips issued by this client (cache hits excluded).
    pub fn total_calls(&self) -> u64 {
        self.calls_total.load(Ordering::Relaxed)
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub fn call(&self, method: &str, params: Value) -> RemoteResult<Value> {
        let mut params = params;
        if let Some(key) = &self.key {
            if method.starts_with("internal.") {
                params["auth"] = json!(key);
            }
        }
        let total = self.calls_total.fetch_add(1, Ordering::Relaxed) + 1;
        if std::env::var_os("ESPO_REMOTE_MDB_LOG_CALLS").is_some() {
            eprintln!("[remote_mdb] call={method} total={total}");
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response = self
            .agent
            .post(&self.rpc_url)
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| format!("remote espo rpc {method} failed: {e}"))?;
        let parsed: Value = response
            .into_json()
            .map_err(|e| format!("remote espo rpc {method}: invalid json: {e}"))?;
        if let Some(err) = parsed.get("error") {
            if !err.is_null() {
                return Err(format!("remote espo rpc {method}: {err}"));
            }
        }
        let result = parsed.get("result").cloned().unwrap_or(Value::Null);
        if result.get("ok").and_then(Value::as_bool) == Some(false) {
            let detail = result.get("error").and_then(Value::as_str).unwrap_or("unknown_error");
            return Err(format!("remote espo rpc {method}: {detail}"));
        }
        Ok(result)
    }

    fn with_blockhash(mut params: Value, blockhash: Option<&BlockHash>) -> Value {
        if let Some(bh) = blockhash {
            params["blockhash"] = json!(hex::encode(bh.to_byte_array()));
        }
        params
    }

    pub fn get(
        &self,
        prefix: &[u8],
        key: &[u8],
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Option<Vec<u8>>> {
        // Only latest-state point reads are memoized; blockhash-pinned views
        // are rare on SSR paths and stay uncached for correctness.
        let cache_key = (blockhash.is_none() && !self.cache_ttl.is_zero())
            .then(|| (prefix.to_vec(), key.to_vec()));
        if let Some(ck) = &cache_key {
            if let Some((stored_at, value)) = self.get_cache.lock().unwrap().get(ck) {
                if stored_at.elapsed() <= self.cache_ttl {
                    return Ok(value.clone());
                }
            }
        }
        let params = Self::with_blockhash(
            json!({ "prefix": String::from_utf8_lossy(prefix), "key": hex_bytes(key) }),
            blockhash,
        );
        let result = self.call("internal.mdb_get", params)?;
        let value = parse_opt_hex(result.get("value").unwrap_or(&Value::Null), "mdb_get value")?;
        if let Some(ck) = cache_key {
            let mut cache = self.get_cache.lock().unwrap();
            if cache.len() >= GET_CACHE_MAX_ENTRIES {
                cache.clear();
            }
            cache.insert(ck, (Instant::now(), value.clone()));
        }
        Ok(value)
    }

    pub fn multi_get(
        &self,
        prefix: &[u8],
        keys: &[Vec<u8>],
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Vec<Option<Vec<u8>>>> {
        let params = Self::with_blockhash(
            json!({
                "prefix": String::from_utf8_lossy(prefix),
                "keys": keys.iter().map(|k| hex_bytes(k)).collect::<Vec<_>>(),
            }),
            blockhash,
        );
        let result = self.call("internal.mdb_multi_get", params)?;
        let values = result
            .get("values")
            .and_then(Value::as_array)
            .ok_or_else(|| "mdb_multi_get: missing values array".to_string())?;
        if values.len() != keys.len() {
            return Err(format!(
                "mdb_multi_get: expected {} values, got {}",
                keys.len(),
                values.len()
            ));
        }
        values.iter().map(|v| parse_opt_hex(v, "mdb_multi_get value")).collect()
    }

    fn parse_entries(result: &Value, what: &str) -> RemoteResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let entries = result
            .get("entries")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("{what}: missing entries array"))?;
        entries
            .iter()
            .map(|entry| {
                let pair =
                    entry.as_array().ok_or_else(|| format!("{what}: entry is not a pair"))?;
                if pair.len() != 2 {
                    return Err(format!("{what}: entry is not a pair"));
                }
                Ok((parse_hex(&pair[0], what)?, parse_hex(&pair[1], what)?))
            })
            .collect()
    }

    pub fn scan_prefix_entries(
        &self,
        prefix: &[u8],
        scan_prefix: &[u8],
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let params = Self::with_blockhash(
            json!({
                "prefix": String::from_utf8_lossy(prefix),
                "scan_prefix": hex_bytes(scan_prefix),
            }),
            blockhash,
        );
        let result = self.call("internal.mdb_scan_prefix_entries", params)?;
        Self::parse_entries(&result, "mdb_scan_prefix_entries")
    }

    pub fn scan_prefix_keys(
        &self,
        prefix: &[u8],
        scan_prefix: &[u8],
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Vec<Vec<u8>>> {
        let params = Self::with_blockhash(
            json!({
                "prefix": String::from_utf8_lossy(prefix),
                "scan_prefix": hex_bytes(scan_prefix),
            }),
            blockhash,
        );
        let result = self.call("internal.mdb_scan_prefix_keys", params)?;
        let keys = result
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| "mdb_scan_prefix_keys: missing keys array".to_string())?;
        keys.iter().map(|k| parse_hex(k, "mdb_scan_prefix_keys key")).collect()
    }

    pub fn scan_range_entries(
        &self,
        prefix: &[u8],
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut params = json!({
            "prefix": String::from_utf8_lossy(prefix),
            "start": hex_bytes(start_inclusive),
        });
        if let Some(end) = end_exclusive {
            params["end"] = json!(hex_bytes(end));
        }
        let params = Self::with_blockhash(params, blockhash);
        let result = self.call("internal.mdb_scan_range_entries", params)?;
        Self::parse_entries(&result, "mdb_scan_range_entries")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scan_range_entries_page(
        &self,
        prefix: &[u8],
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        offset: usize,
        limit: usize,
        reverse: bool,
        blockhash: Option<&BlockHash>,
    ) -> RemoteResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut params = json!({
            "prefix": String::from_utf8_lossy(prefix),
            "start": hex_bytes(start_inclusive),
            "offset": offset,
            "limit": limit,
            "reverse": reverse,
        });
        if let Some(end) = end_exclusive {
            params["end"] = json!(hex_bytes(end));
        }
        let params = Self::with_blockhash(params, blockhash);
        let result = self.call("internal.mdb_scan_range_entries_page", params)?;
        Self::parse_entries(&result, "mdb_scan_range_entries_page")
    }

    pub fn blockhash_for_height(&self, height: u32) -> RemoteResult<Option<BlockHash>> {
        let result =
            self.call("internal.tree_blockhash_for_height", json!({ "height": height }))?;
        let raw = result.get("blockhash").cloned().unwrap_or(Value::Null);
        let Some(bytes) = parse_opt_hex(&raw, "tree_blockhash_for_height blockhash")? else {
            return Ok(None);
        };
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "tree_blockhash_for_height: blockhash not 32 bytes".to_string())?;
        Ok(Some(BlockHash::from_byte_array(arr)))
    }

    pub fn indexed_height_bounds(&self) -> RemoteResult<Option<(u32, u32)>> {
        let result = self.call("internal.tree_indexed_height_bounds", json!({}))?;
        let min = result.get("min").and_then(Value::as_u64);
        let max = result.get("max").and_then(Value::as_u64);
        match (min, max) {
            (Some(min), Some(max)) => Ok(Some((min as u32, max as u32))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mdb::{Mdb, MdbError};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    /// Minimal one-shot HTTP JSON-RPC responder: for each expected request,
    /// asserts the method and replies with the canned result.
    fn spawn_mock_server(responses: Vec<(&'static str, Value)>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock addr");
        std::thread::spawn(move || {
            for (expected_method, result) in responses {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let body = loop {
                    let n = stream.read(&mut tmp).expect("read");
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]).to_string();
                        let content_length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                if name.eq_ignore_ascii_case("content-length") {
                                    value.trim().parse::<usize>().ok()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);
                        let body_start = pos + 4;
                        while buf.len() < body_start + content_length {
                            let n = stream.read(&mut tmp).expect("read body");
                            buf.extend_from_slice(&tmp[..n]);
                        }
                        break buf[body_start..body_start + content_length].to_vec();
                    }
                };
                let request: Value = serde_json::from_slice(&body).expect("request json");
                assert_eq!(
                    request["method"].as_str(),
                    Some(expected_method),
                    "unexpected rpc method"
                );
                let reply =
                    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
                        .unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.len(),
                    reply
                );
                stream.write_all(response.as_bytes()).expect("write response");
            }
        });
        format!("http://{addr}")
    }

    #[test]
    fn remote_mdb_get_and_multi_get_round_trip() {
        let host = spawn_mock_server(vec![
            ("internal.mdb_get", json!({ "ok": true, "value": "deadbeef" })),
            ("internal.mdb_get", json!({ "ok": true, "value": Value::Null })),
            ("internal.mdb_multi_get", json!({ "ok": true, "values": ["0011", Value::Null] })),
        ]);
        let mdb = Mdb::remote(Arc::new(RemoteMdbClient::new(&host)), b"essentials:");

        assert_eq!(mdb.get(b"some-key").expect("get"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(mdb.get(b"absent").expect("get"), None);
        assert_eq!(
            mdb.multi_get(&[b"a".to_vec(), b"b".to_vec()]).expect("multi_get"),
            vec![Some(vec![0x00, 0x11]), None]
        );
    }

    #[test]
    fn remote_mdb_scans_round_trip() {
        let host = spawn_mock_server(vec![
            (
                "internal.mdb_scan_prefix_entries",
                json!({ "ok": true, "entries": [["6161", "01"], ["6162", "02"]] }),
            ),
            ("internal.mdb_scan_prefix_keys", json!({ "ok": true, "keys": ["6161"] })),
            (
                "internal.mdb_scan_range_entries_page",
                json!({ "ok": true, "entries": [["6162", "02"]] }),
            ),
        ]);
        let mdb = Mdb::remote(Arc::new(RemoteMdbClient::new(&host)), b"essentials:");

        assert_eq!(
            mdb.scan_prefix_entries(b"a").expect("scan entries"),
            vec![(b"aa".to_vec(), vec![0x01]), (b"ab".to_vec(), vec![0x02])]
        );
        assert_eq!(mdb.scan_prefix_keys(b"a").expect("scan keys"), vec![b"aa".to_vec()]);
        assert_eq!(
            mdb.scan_range_entries_page(b"a", Some(b"b"), 0, 10, true).expect("page"),
            vec![(b"ab".to_vec(), vec![0x02])]
        );
    }

    #[test]
    fn remote_mdb_tree_helpers_round_trip() {
        let host = spawn_mock_server(vec![
            (
                "internal.tree_blockhash_for_height",
                json!({ "ok": true, "height": 5, "blockhash": "11".repeat(32) }),
            ),
            (
                "internal.tree_indexed_height_bounds",
                json!({ "ok": true, "min": 880000, "max": 946000 }),
            ),
        ]);
        let mdb = Mdb::remote(Arc::new(RemoteMdbClient::new(&host)), b"essentials:");

        let bh = mdb.blockhash_for_height(5).expect("blockhash").expect("some blockhash");
        assert_eq!(bh.to_byte_array(), [0x11u8; 32]);
        assert_eq!(mdb.indexed_height_bounds().expect("bounds"), Some((880000, 946000)));
    }

    #[test]
    fn internal_calls_carry_auth_key_and_get_cache_collapses_repeats() {
        // Mock serves exactly ONE request and asserts the auth key is present;
        // the second identical get must be served from the client-side cache
        // (a second network hit would make accept() block and the test fail).
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock addr");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            let body = loop {
                let n = stream.read(&mut tmp).expect("read");
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            if name.eq_ignore_ascii_case("content-length") {
                                value.trim().parse::<usize>().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    let body_start = pos + 4;
                    while buf.len() < body_start + content_length {
                        let n = stream.read(&mut tmp).expect("read body");
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    break buf[body_start..body_start + content_length].to_vec();
                }
            };
            let request: Value = serde_json::from_slice(&body).expect("request json");
            assert_eq!(request["params"]["auth"].as_str(), Some("sekrit"));
            // the storage key param must NOT be clobbered by the auth key
            assert_eq!(request["params"]["key"].as_str(), Some(hex::encode(b"row").as_str()));
            let reply = serde_json::to_string(
                &json!({ "jsonrpc": "2.0", "id": 1, "result": { "ok": true, "value": "beef" } }),
            )
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                reply.len(),
                reply
            );
            stream.write_all(response.as_bytes()).expect("write response");
        });

        let client = RemoteMdbClient::new_with(
            &format!("http://{addr}"),
            Some("sekrit".to_string()),
            Duration::from_secs(60),
        );
        let mdb = Mdb::remote(Arc::new(client), b"essentials:");

        assert_eq!(mdb.get(b"row").expect("first get"), Some(vec![0xbe, 0xef]));
        // Served from cache; no second connection is accepted by the mock.
        assert_eq!(mdb.get(b"row").expect("cached get"), Some(vec![0xbe, 0xef]));
    }

    #[test]
    fn remote_mdb_rejects_writes_and_surfaces_rpc_errors() {
        let host = spawn_mock_server(vec![(
            "internal.mdb_get",
            json!({ "ok": false, "error": "invalid_prefix" }),
        )]);
        let mdb = Mdb::remote(Arc::new(RemoteMdbClient::new(&host)), b"essentials:");

        assert!(matches!(mdb.put(b"k", b"v"), Err(MdbError::RemoteReadOnly)));
        assert!(matches!(mdb.delete(b"k"), Err(MdbError::RemoteReadOnly)));
        assert!(matches!(mdb.bulk_write(|_| {}), Err(MdbError::RemoteReadOnly)));

        let err = mdb.get(b"k").expect_err("rpc error should propagate");
        assert!(err.to_string().contains("invalid_prefix"), "got: {err}");
    }
}
