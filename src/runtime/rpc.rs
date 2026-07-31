use crate::{
    config::{
        get_bitcoind_rpc_client, get_config, get_electrum_like, get_espo_next_height, get_network,
    },
    modules::defs::RpcRegistry,
    runtime::mempool::{
        MempoolBlockSummary, current_mempool_compact_snapshot, current_mempool_minimum_fee_rate,
    },
};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::post,
};
use bitcoin::{Address, Transaction, Txid, consensus::deserialize};
use bitcoincore_rpc::RpcApi;
use futures::FutureExt;
use serde::Serialize;
use serde_json::{Value, json};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tarpc::context;
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct RpcState {
    pub registry: RpcRegistry,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Value,
}

const JSONRPC_VERSION: &str = "2.0";
const MAX_SAFE_INTEGER_F64: f64 = 9_007_199_254_740_991.0;
const MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;
const MAX_RAW_TRANSACTION_HEX_LEN: usize = 8_000_000;
const PRECISE_FEE_INCREMENT: f64 = 0.001;

/// Bitcoin Core caps a package at 25 transactions.
const MAX_PACKAGE_TRANSACTIONS: usize = 25;

/// Who is calling, for the handful of methods that must pass the caller's
/// identity onward. The faucet rate-limits per IP, so a request arriving over
/// RPC has to carry the same forwarded headers and peer address the explorer's
/// own faucet endpoints send.
pub struct CallerContext {
    pub headers: axum::http::HeaderMap,
    pub peer: Option<SocketAddr>,
}

/// Methods espo answers itself, before the module registry is consulted.
///
/// The `btc.*` ones are proxies onto the Bitcoin backends — electrs/Esplora and
/// Bitcoin Core — rather than anything espo indexes, which is why they carry
/// their own namespace instead of sitting unprefixed beside `get_espo_height`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuiltinMethod {
    GetEspoHeight,
    GetMethodLineChart,
    BtcGetTransaction,
    BtcGetAddress,
    BtcBroadcastTransaction,
    BtcSubmitPackage,
    BtcFeeEstimates,
    BtcFaucetRequest,
    BtcFaucetStatus,
}

/// Resolves a method name to a built-in, or `None` to fall through to the
/// module registry.
///
/// The Bitcoin proxies answer under `btc.*` only. Their former unprefixed
/// spellings — `get_transaction`, `get_address`, `broadcast_transaction`,
/// `submit_package`, `fee_estimates` — are gone rather than aliased, so a
/// client still using one gets `-32601` instead of silently depending on a
/// name that no longer appears in the documentation.
fn builtin_method(method: &str) -> Option<BuiltinMethod> {
    match method {
        "get_espo_height" => Some(BuiltinMethod::GetEspoHeight),
        "get_method_line_chart" => Some(BuiltinMethod::GetMethodLineChart),
        "btc.get_transaction" => Some(BuiltinMethod::BtcGetTransaction),
        "btc.get_address" => Some(BuiltinMethod::BtcGetAddress),
        "btc.broadcast_transaction" => Some(BuiltinMethod::BtcBroadcastTransaction),
        "btc.submit_package" => Some(BuiltinMethod::BtcSubmitPackage),
        "btc.fee_estimates" => Some(BuiltinMethod::BtcFeeEstimates),
        // Regtest-only, and only when a faucet is configured: anywhere else the
        // name is not a method at all and resolves to -32601.
        "btc.faucet_request" if crate::explorer::faucet::faucet_enabled() => {
            Some(BuiltinMethod::BtcFaucetRequest)
        }
        "btc.faucet_status" if crate::explorer::faucet::faucet_enabled() => {
            Some(BuiltinMethod::BtcFaucetStatus)
        }
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FeeEstimates {
    fastest_fee: f64,
    half_hour_fee: f64,
    hour_fee: f64,
    economy_fee: f64,
    minimum_fee: f64,
}

fn err_response(id: Value, code: i64, message: &str, data: Option<Value>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: None,
        error: Some(JsonRpcError { code, message: message.to_string(), data }),
        id,
    }
}

fn get_espo_tip_height_response(id: Value) -> JsonRpcResponse {
    let height: u32 = get_espo_next_height().saturating_sub(1);

    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: Some(json!({
            "height": height
        })),
        error: None,
        id,
    }
}

fn is_builtin_root_method(method: &str) -> bool {
    builtin_method(method).is_some()
}

async fn builtin_response(
    builtin: BuiltinMethod,
    state: &RpcState,
    caller: &CallerContext,
    id: Value,
    params: Value,
) -> JsonRpcResponse {
    match builtin {
        BuiltinMethod::GetEspoHeight => get_espo_tip_height_response(id),
        BuiltinMethod::GetMethodLineChart => {
            get_method_line_chart_response(state, id, params).await
        }
        BuiltinMethod::BtcGetTransaction => get_transaction_response(id, params).await,
        BuiltinMethod::BtcGetAddress => get_address_response(id, params).await,
        BuiltinMethod::BtcBroadcastTransaction => broadcast_transaction_response(id, params).await,
        BuiltinMethod::BtcSubmitPackage => submit_package_response(id, params).await,
        BuiltinMethod::BtcFeeEstimates => fee_estimates_response(id),
        BuiltinMethod::BtcFaucetRequest => faucet_request_response(caller, id, params).await,
        BuiltinMethod::BtcFaucetStatus => faucet_status_response(caller, id).await,
    }
}

fn round_to_increment(value: f64, increment: f64) -> f64 {
    (value / increment).round() * increment
}

fn round_up_to_increment(value: f64, increment: f64) -> f64 {
    (value / increment).ceil() * increment
}

fn round_to_three_decimals(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn optimized_median_fee(
    block: &MempoolBlockSummary,
    next_block: Option<&MempoolBlockSummary>,
    previous_fee: Option<f64>,
    minimum_fee: f64,
    max_block_vsize: f64,
) -> f64 {
    let median = block.median_fee_rate.filter(|fee| fee.is_finite() && *fee >= 0.0);
    let Some(median) = median else { return minimum_fee };
    let use_fee = previous_fee.map(|fee| (median + fee) / 2.0).unwrap_or(median);
    let half_full = max_block_vsize * 0.5;
    let nearly_full = max_block_vsize * 0.95;

    if block.vsize as f64 <= half_full || median < minimum_fee {
        return minimum_fee;
    }
    if block.vsize as f64 <= nearly_full && next_block.is_none() {
        let multiplier = (block.vsize as f64 - half_full) / (max_block_vsize - half_full);
        return round_to_increment(use_fee * multiplier, PRECISE_FEE_INCREMENT).max(minimum_fee);
    }
    round_up_to_increment(use_fee, PRECISE_FEE_INCREMENT).max(minimum_fee)
}

fn calculate_fee_estimates(
    blocks: &[MempoolBlockSummary],
    minimum_fee: f64,
    max_block_vsize: f64,
) -> FeeEstimates {
    let minimum_fee = if minimum_fee.is_finite() && minimum_fee >= 0.0 {
        minimum_fee.max(PRECISE_FEE_INCREMENT)
    } else {
        PRECISE_FEE_INCREMENT
    };

    let first = blocks.first().map(|block| {
        optimized_median_fee(block, blocks.get(1), None, minimum_fee, max_block_vsize)
    });
    let second = blocks.get(1).map(|block| {
        optimized_median_fee(block, blocks.get(2), first, minimum_fee, max_block_vsize)
    });
    let third = blocks.get(2).map(|block| {
        optimized_median_fee(block, blocks.get(3), second, minimum_fee, max_block_vsize)
    });

    let mut fastest_fee = first.unwrap_or(minimum_fee).max(minimum_fee);
    let mut half_hour_fee = second.unwrap_or(minimum_fee).max(minimum_fee);
    let mut hour_fee = third.unwrap_or(minimum_fee).max(minimum_fee);
    let economy_fee = third.unwrap_or(minimum_fee).min(2.0 * minimum_fee).max(minimum_fee);

    fastest_fee = fastest_fee.max(half_hour_fee).max(hour_fee).max(economy_fee);
    half_hour_fee = half_hour_fee.max(hour_fee).max(economy_fee);
    hour_fee = hour_fee.max(economy_fee);

    FeeEstimates {
        fastest_fee: round_to_three_decimals((fastest_fee + 0.5).max(1.0)),
        half_hour_fee: round_to_three_decimals((half_hour_fee + 0.25).max(0.5)),
        hour_fee: round_to_three_decimals(hour_fee),
        economy_fee: round_to_three_decimals(economy_fee),
        minimum_fee: round_to_three_decimals(minimum_fee),
    }
}

fn fee_estimates_response(id: Value) -> JsonRpcResponse {
    let snapshot = current_mempool_compact_snapshot();
    let minimum_fee = current_mempool_minimum_fee_rate().unwrap_or(PRECISE_FEE_INCREMENT);
    let max_block_vsize = (get_config().mempool.block_weight_units as f64 / 4.0).max(1.0);
    let estimates = calculate_fee_estimates(&snapshot.blocks, minimum_fee, max_block_vsize);

    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: Some(serde_json::to_value(estimates).unwrap_or_else(|_| json!({}))),
        error: None,
        id,
    }
}

fn parse_raw_transaction_params(params: Value) -> Result<Vec<u8>, String> {
    let raw_tx = match params {
        Value::Object(params) => params
            .get("raw_tx")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "raw_tx is required and must be a string".to_string())?,
        Value::Array(params) if params.len() == 1 => params[0]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "params[0] must be a raw transaction hex string".to_string())?,
        Value::Array(_) => {
            return Err("params must contain exactly one raw transaction hex string".to_string());
        }
        _ => return Err("params must be an object or a one-item array".to_string()),
    };
    let raw_tx = raw_tx.trim();
    if raw_tx.is_empty() {
        return Err("raw transaction must not be empty".to_string());
    }
    if raw_tx.len() > MAX_RAW_TRANSACTION_HEX_LEN {
        return Err("raw transaction exceeds the maximum supported size".to_string());
    }
    let bytes =
        hex::decode(raw_tx).map_err(|e| format!("raw transaction is not valid hex: {e}"))?;
    deserialize::<Transaction>(&bytes)
        .map_err(|e| format!("raw transaction could not be decoded: {e}"))?;
    Ok(bytes)
}

async fn broadcast_transaction_response(id: Value, params: Value) -> JsonRpcResponse {
    let raw_tx = match parse_raw_transaction_params(params) {
        Ok(raw_tx) => raw_tx,
        Err(detail) => return invalid_params(id, &detail),
    };
    let electrum = get_electrum_like();
    let result = tokio::task::spawn_blocking(move || {
        match electrum.transaction_broadcast_raw(&raw_tx) {
            Ok(txid) => Ok(txid.to_string()),
            Err(electrum_error) => {
                eprintln!(
                    "[rpc] configured transaction backend rejected broadcast; trying Bitcoin Core: {electrum_error:#}"
                );
                get_bitcoind_rpc_client()
                    .send_raw_transaction(raw_tx.as_slice())
                    .map(|txid| txid.to_string())
                    .map_err(|core_error| {
                        format!(
                            "configured backend failed: {electrum_error:#}; Bitcoin Core fallback failed: {core_error}"
                        )
                    })
            }
        }
    })
    .await;

    match result {
        Ok(Ok(txid)) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            result: Some(json!({ "txid": txid })),
            error: None,
            id,
        },
        Ok(Err(detail)) => err_response(
            id,
            -32000,
            "Transaction broadcast failed",
            Some(json!({ "detail": detail })),
        ),
        Err(error) => internal_error(id, &format!("transaction broadcast task failed: {error}")),
    }
}

fn parse_txid_params(params: Value) -> Result<Txid, String> {
    let txid = match params {
        Value::Object(params) => params
            .get("txid")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "txid is required and must be a string".to_string())?,
        Value::Array(params) if params.len() == 1 => params[0]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "params[0] must be a txid string".to_string())?,
        Value::Array(_) => {
            return Err("params must contain exactly one txid string".to_string());
        }
        _ => return Err("params must be an object or a one-item array".to_string()),
    };
    Txid::from_str(txid.trim()).map_err(|e| format!("invalid txid: {e}"))
}

async fn get_transaction_response(id: Value, params: Value) -> JsonRpcResponse {
    let txid = match parse_txid_params(params) {
        Ok(txid) => txid,
        Err(detail) => return invalid_params(id, &detail),
    };
    let electrum = get_electrum_like();
    let result = tokio::task::spawn_blocking(move || electrum.transaction_details(&txid)).await;

    match result {
        Ok(Ok(Some((tx, hex)))) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            result: Some(json!({ "ok": true, "found": true, "tx": tx, "hex": hex })),
            error: None,
            id,
        },
        // A transaction the index has never seen is an answer, not a failure.
        Ok(Ok(None)) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            result: Some(json!({ "ok": true, "found": false })),
            error: None,
            id,
        },
        Ok(Err(error)) => err_response(
            id,
            -32000,
            "Transaction lookup failed",
            Some(json!({ "detail": format!("{error:#}") })),
        ),
        Err(error) => internal_error(id, &format!("transaction lookup task failed: {error}")),
    }
}

/// Raw transaction hexes for a package submission, in the order given: Core
/// expects the parents before the child they fund.
fn parse_package_params(params: Value) -> Result<Vec<String>, String> {
    let entries = match params {
        Value::Object(params) => params
            .get("txs")
            .ok_or_else(|| {
                "txs is required and must be an array of raw transaction hex".to_string()
            })?
            .clone(),
        Value::Array(params) if params.len() == 1 && params[0].is_array() => params[0].clone(),
        Value::Array(params) => Value::Array(params),
        _ => return Err("params must be an object or an array".to_string()),
    };
    let Value::Array(entries) = entries else {
        return Err("txs must be an array of raw transaction hex strings".to_string());
    };
    if entries.is_empty() {
        return Err("txs must contain at least one raw transaction".to_string());
    }
    if entries.len() > MAX_PACKAGE_TRANSACTIONS {
        return Err(format!(
            "a package may contain at most {MAX_PACKAGE_TRANSACTIONS} transactions"
        ));
    }

    let mut out = Vec::with_capacity(entries.len());
    for (idx, entry) in entries.iter().enumerate() {
        let Some(raw) = entry.as_str() else {
            return Err(format!("txs[{idx}] must be a raw transaction hex string"));
        };
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(format!("txs[{idx}] must not be empty"));
        }
        if raw.len() > MAX_RAW_TRANSACTION_HEX_LEN {
            return Err(format!("txs[{idx}] exceeds the maximum supported size"));
        }
        let bytes = hex::decode(raw).map_err(|e| format!("txs[{idx}] is not valid hex: {e}"))?;
        deserialize::<Transaction>(&bytes)
            .map_err(|e| format!("txs[{idx}] could not be decoded: {e}"))?;
        out.push(raw.to_string());
    }
    Ok(out)
}

/// Submits a package straight to Bitcoin Core's `submitpackage`, for callers
/// who need related transactions accepted together — a child paying for its
/// parent, say — which broadcasting one at a time cannot express.
///
/// Core's reply is passed through untouched under `result`: it reports per
/// transaction, and a package can partly succeed, so collapsing it into a
/// single status would lose the part the caller needs.
async fn submit_package_response(id: Value, params: Value) -> JsonRpcResponse {
    let txs = match parse_package_params(params) {
        Ok(txs) => txs,
        Err(detail) => return invalid_params(id, &detail),
    };

    let result = tokio::task::spawn_blocking(move || {
        get_bitcoind_rpc_client().call::<Value>("submitpackage", &[json!(txs)])
    })
    .await;

    match result {
        Ok(Ok(value)) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            result: Some(json!({ "ok": true, "result": value })),
            error: None,
            id,
        },
        Ok(Err(error)) => err_response(
            id,
            -32000,
            "Package submission failed",
            Some(json!({ "detail": format!("{error}") })),
        ),
        Err(error) => internal_error(id, &format!("package submission task failed: {error}")),
    }
}

/// `{ address, amount?, asset? }`, mirroring the body the explorer's
/// `POST /api/faucet/send` accepts.
fn parse_faucet_request_params(
    params: Value,
) -> Result<(String, Option<f64>, Option<String>), String> {
    let Value::Object(params) = params else {
        return Err("params must be an object".to_string());
    };
    let address = parse_required_non_empty_string_param(&params, "address")?.to_string();

    let amount = match params.get("amount") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let Some(amount) = value.as_f64() else {
                return Err("amount must be a number".to_string());
            };
            Some(amount)
        }
    };
    let asset = match params.get("asset") {
        None | Some(Value::Null) => None,
        Some(Value::String(asset)) => Some(asset.clone()),
        Some(_) => return Err("asset must be a string".to_string()),
    };

    Ok((address, amount, asset))
}

/// Requests regtest coins from the configured faucet, the same proxy the
/// explorer's faucet page uses. Only resolvable on regtest with a faucet
/// configured; elsewhere the method does not exist.
async fn faucet_request_response(
    caller: &CallerContext,
    id: Value,
    params: Value,
) -> JsonRpcResponse {
    let (address, amount, asset) = match parse_faucet_request_params(params) {
        Ok(parsed) => parsed,
        Err(detail) => return invalid_params(id, &detail),
    };

    match crate::explorer::faucet::faucet_request_rpc(
        &address,
        amount,
        asset.as_deref(),
        &caller.headers,
        caller.peer,
    )
    .await
    {
        Ok(body) => faucet_body_response(id, body),
        Err("invalid_address") => invalid_params(id, "address must be a regtest address"),
        Err(detail) => faucet_error_response(id, detail),
    }
}

/// Requests the faucet's per-asset availability and limits — the same proxy
/// behind the explorer's `GET /api/faucet/status`. Same regtest-and-configured
/// gate as `btc.faucet_request`.
async fn faucet_status_response(caller: &CallerContext, id: Value) -> JsonRpcResponse {
    match crate::explorer::faucet::faucet_status_rpc(&caller.headers, caller.peer).await {
        Ok(body) => faucet_body_response(id, body),
        Err(detail) => faucet_error_response(id, detail),
    }
}

/// The faucet answers in JSON-RPC itself, so its envelope is unwrapped rather
/// than nested inside this one: its result becomes the result and its error
/// becomes an error, code and message intact. A body shaped like neither is
/// passed through as it came.
fn faucet_body_response(id: Value, mut body: Value) -> JsonRpcResponse {
    match body.get("error") {
        Some(error) if !error.is_null() => {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Faucet request failed")
                .to_string();
            let data = error.get("data").cloned();
            err_response(id, code, &message, data)
        }
        _ => {
            let result = body.get_mut("result").map(Value::take).unwrap_or(body);
            JsonRpcResponse { jsonrpc: JSONRPC_VERSION, result: Some(result), error: None, id }
        }
    }
}

fn faucet_error_response(id: Value, detail: &str) -> JsonRpcResponse {
    match detail {
        "invalid_asset" => invalid_params(id, "asset must be rbtc or diesel"),
        "invalid_amount" => invalid_params(id, "amount must be a non-negative number"),
        "not_configured" => err_response(id, -32004, "Faucet is not available", None),
        _ => err_response(
            id,
            -32002,
            "Unable to reach the faucet service",
            Some(json!({ "detail": detail })),
        ),
    }
}

fn parse_address_params(params: Value) -> Result<String, String> {
    let address = match params {
        Value::Object(params) => params
            .get("address")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "params.address must be a string".to_string())?,
        Value::Array(params) if params.len() == 1 => params[0]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "params[0] must be an address string".to_string())?,
        Value::Array(_) => {
            return Err("params must contain exactly one address string".to_string());
        }
        _ => return Err("params must be an object or a one-item array".to_string()),
    };
    let address = address.trim();
    if address.is_empty() {
        return Err("address must not be empty".to_string());
    }
    Ok(address.to_string())
}

async fn get_address_response(id: Value, params: Value) -> JsonRpcResponse {
    let address_raw = match parse_address_params(params) {
        Ok(address) => address,
        Err(detail) => return invalid_params(id, &detail),
    };
    let address = match Address::from_str(&address_raw)
        .and_then(|address| address.require_network(get_network()))
    {
        Ok(address) => address,
        Err(error) => return invalid_params(id, &format!("invalid address: {error}")),
    };
    let electrum = get_electrum_like();
    let result = tokio::task::spawn_blocking(move || electrum.address_summary(&address)).await;

    match result {
        Ok(Ok(summary)) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            result: Some(json!(summary)),
            error: None,
            id,
        },
        Ok(Err(error)) => err_response(
            id,
            -32000,
            "Address lookup failed",
            Some(json!({ "detail": format!("{error:#}") })),
        ),
        Err(error) => internal_error(id, &format!("address lookup task failed: {error}")),
    }
}

fn parse_optional_u32_param(
    params: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<u32>, String> {
    let Some(value) = params.get(key) else {
        return Ok(None);
    };
    let Some(num) = value.as_u64() else {
        return Err(format!("{key} must be an unsigned integer"));
    };
    let parsed = u32::try_from(num).map_err(|_| format!("{key} is out of range"))?;
    Ok(Some(parsed))
}

fn parse_required_non_empty_string_param<'a>(
    params: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a str, String> {
    let Some(value) = params.get(key) else {
        return Err(format!("{key} is required"));
    };
    let Some(as_str) = value.as_str() else {
        return Err(format!("{key} must be a string"));
    };
    let trimmed = as_str.trim();
    if trimmed.is_empty() {
        return Err(format!("{key} must not be empty"));
    }
    Ok(trimmed)
}

struct ParsedChartValue {
    number_value: Option<serde_json::Number>,
    string_value: String,
    requires_string: bool,
}

impl ParsedChartValue {
    fn zero() -> Self {
        Self {
            number_value: Some(serde_json::Number::from(0)),
            string_value: "0".to_string(),
            requires_string: false,
        }
    }

    fn into_json(self, force_string: bool) -> Value {
        if force_string || self.requires_string {
            Value::String(self.string_value)
        } else {
            self.number_value.map(Value::Number).unwrap_or(Value::String(self.string_value))
        }
    }
}

fn parse_chart_numeric_value(value: &Value) -> Option<ParsedChartValue> {
    match value {
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                return Some(ParsedChartValue {
                    number_value: Some(n.clone()),
                    string_value: u.to_string(),
                    requires_string: u > MAX_SAFE_INTEGER_U64,
                });
            }
            if let Some(i) = n.as_i64() {
                return Some(ParsedChartValue {
                    number_value: Some(n.clone()),
                    string_value: i.to_string(),
                    requires_string: i.unsigned_abs() > MAX_SAFE_INTEGER_U64,
                });
            }
            let parsed = n.as_f64()?;
            if !parsed.is_finite() {
                return None;
            }
            Some(ParsedChartValue {
                number_value: Some(n.clone()),
                string_value: n.to_string(),
                requires_string: parsed.abs() > MAX_SAFE_INTEGER_F64,
            })
        }
        Value::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return None;
            }
            let parsed = trimmed.parse::<f64>().ok()?;
            if parsed.is_nan() {
                return None;
            }
            if parsed.is_infinite() || parsed.abs() > MAX_SAFE_INTEGER_F64 {
                return Some(ParsedChartValue {
                    number_value: None,
                    string_value: trimmed.to_string(),
                    requires_string: true,
                });
            }
            let number = serde_json::Number::from_f64(parsed)?;
            Some(ParsedChartValue {
                string_value: number.to_string(),
                number_value: Some(number),
                requires_string: false,
            })
        }
        _ => None,
    }
}

fn extract_value_at_path<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = root;
    for segment in path {
        match current {
            Value::Object(map) => {
                current = map.get(*segment)?;
            }
            Value::Array(items) => {
                let idx = segment.parse::<usize>().ok()?;
                current = items.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn sample_heights(range_min: u32, range_max: u32, range_interval: u32) -> Vec<u32> {
    let mut heights = Vec::new();
    let mut current = range_min;

    loop {
        heights.push(current);
        if current >= range_max {
            break;
        }
        let Some(next) = current.checked_add(range_interval) else {
            if heights.last().copied() != Some(range_max) {
                heights.push(range_max);
            }
            break;
        };
        if next > range_max {
            if heights.last().copied() != Some(range_max) {
                heights.push(range_max);
            }
            break;
        }
        current = next;
    }

    heights
}

fn indexed_height_bounds() -> Result<(u32, u32), String> {
    // Remote-aware: client mode asks the data espo for its bounds; local
    // instances read the versioned tree as before.
    crate::config::explorer_indexed_height_bounds()
        .ok_or_else(|| "no indexed heights available".to_string())
}

async fn get_method_line_chart_response(
    state: &RpcState,
    id: Value,
    params: Value,
) -> JsonRpcResponse {
    let params_obj = match params {
        Value::Object(obj) => obj,
        _ => return invalid_params(id, "params must be an object"),
    };

    let target_method = match parse_required_non_empty_string_param(&params_obj, "method") {
        Ok(value) => value.to_string(),
        Err(detail) => return invalid_params(id, &detail),
    };
    if is_builtin_root_method(&target_method) {
        return invalid_params(id, "params.method cannot target a root built-in method");
    }

    let key = match parse_required_non_empty_string_param(&params_obj, "key") {
        Ok(value) => value.to_string(),
        Err(detail) => return invalid_params(id, &detail),
    };
    let path_parts: Vec<&str> = key.split('.').collect();
    if path_parts.iter().any(|p| p.is_empty()) {
        return invalid_params(id, "key contains an empty path segment");
    }

    let base_body = match params_obj.get("body") {
        Some(Value::Object(obj)) => obj.clone(),
        Some(_) => return invalid_params(id, "body must be an object"),
        None => return invalid_params(id, "body is required"),
    };

    let range_min_param = match parse_optional_u32_param(&params_obj, "range_min") {
        Ok(v) => v,
        Err(detail) => return invalid_params(id, &detail),
    };
    let range_max_param = match parse_optional_u32_param(&params_obj, "range_max") {
        Ok(v) => v,
        Err(detail) => return invalid_params(id, &detail),
    };
    let range_interval = match parse_optional_u32_param(&params_obj, "range_interval") {
        Ok(Some(v)) => v,
        Ok(None) => 50,
        Err(detail) => return invalid_params(id, &detail),
    };
    if range_interval == 0 {
        return invalid_params(id, "range_interval must be greater than 0");
    }

    let (default_min, default_max) = match indexed_height_bounds() {
        Ok(bounds) => bounds,
        Err(detail) => return internal_error(id, &detail),
    };
    let range_min = range_min_param.unwrap_or(default_min);
    let range_max = range_max_param.unwrap_or(default_max);

    if range_min > range_max {
        return invalid_params(id, "range_min must be <= range_max");
    }
    if range_min < default_min || range_max > default_max {
        let detail = format!("range must be inside indexed bounds [{default_min}, {default_max}]");
        return invalid_params(id, &detail);
    }

    let methods = state.registry.list().await;
    if !methods.iter().any(|m| m == &target_method) {
        let detail = format!("target method not found: {target_method}");
        return invalid_params(id, &detail);
    }

    let sampled = sample_heights(range_min, range_max, range_interval);
    let mut raw_points: Vec<(u32, ParsedChartValue)> = Vec::with_capacity(sampled.len());
    let mut force_string_values = false;
    for height in sampled {
        let mut payload = base_body.clone();
        payload.insert("height".to_string(), json!(height));

        let cx = context::current();
        let result = match std::panic::AssertUnwindSafe(state.registry.call(
            cx,
            target_method.as_str(),
            Value::Object(payload),
        ))
        .catch_unwind()
        .await
        {
            Ok(v) => v,
            Err(_) => return internal_error(id, "target handler panicked"),
        };

        let parsed_value = match extract_value_at_path(&result, &path_parts) {
            None | Some(Value::Null) => ParsedChartValue::zero(),
            Some(value) => match parse_chart_numeric_value(value) {
                Some(v) => v,
                None => {
                    let detail = format!("value at key is not numeric at height {height}");
                    return invalid_params(id, &detail);
                }
            },
        };

        if parsed_value.requires_string {
            force_string_values = true;
        }
        raw_points.push((height, parsed_value));
    }

    let points: Vec<Value> = raw_points
        .into_iter()
        .map(|(height, parsed)| {
            json!({
                "height": height,
                "value": parsed.into_json(force_string_values)
            })
        })
        .collect();

    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: Some(json!({
            "method": target_method,
            "key": key,
            "range_min": range_min,
            "range_max": range_max,
            "range_interval": range_interval,
            "points": points,
        })),
        error: None,
        id,
    }
}

fn parse_error() -> JsonRpcResponse {
    err_response(Value::Null, -32700, "Parse error", None)
}

fn invalid_request() -> JsonRpcResponse {
    err_response(Value::Null, -32600, "Invalid Request", None)
}

fn method_not_found(id: Value) -> JsonRpcResponse {
    err_response(id, -32601, "Method not found", None)
}

fn invalid_params(id: Value, detail: &str) -> JsonRpcResponse {
    err_response(id, -32602, "Invalid params", Some(json!({ "detail": detail })))
}

fn internal_error(id: Value, detail: &str) -> JsonRpcResponse {
    err_response(id, -32603, "Internal error", Some(json!({ "detail": detail })))
}

fn is_valid_id(v: &Value) -> bool {
    matches!(v, Value::String(_) | Value::Number(_) | Value::Null)
}

fn extract_method_and_params(
    obj: &serde_json::Map<String, Value>,
) -> Result<(&str, Value), &'static str> {
    // jsonrpc MUST be "2.0"
    match obj.get("jsonrpc") {
        Some(Value::String(s)) if s == JSONRPC_VERSION => {}
        _ => return Err("jsonrpc version missing or not 2.0"),
    }

    // method MUST be a string and MUST NOT start with "rpc."
    let method = match obj.get("method") {
        Some(Value::String(m)) if !m.starts_with("rpc.") => m.as_str(),
        Some(Value::String(_)) => return Err("method name reserved (rpc.*)"),
        _ => return Err("method must be a string"),
    };

    // params MAY be omitted; if present MUST be array or object
    let params = match obj.get("params") {
        None => Value::Null,
        Some(Value::Array(_)) | Some(Value::Object(_)) => obj.get("params").cloned().unwrap(),
        _ => return Err("params must be an array or an object"),
    };

    Ok((method, params))
}

fn extract_id(obj: &serde_json::Map<String, Value>) -> Option<Value> {
    match obj.get("id") {
        Some(v) if is_valid_id(v) => Some(v.clone()),
        Some(_) => Some(Value::Null), // present but invalid → spec wants Null on error
        None => None,                 // notification
    }
}

async fn handle_single_request(
    state: &RpcState,
    caller: &CallerContext,
    req_obj: &serde_json::Map<String, Value>,
) -> Option<JsonRpcResponse> {
    let id_opt = extract_id(req_obj);
    // Notifications (no id): no response at all
    let id_for_errors = id_opt.clone().unwrap_or(Value::Null);

    let (method, params) = match extract_method_and_params(req_obj) {
        Ok(x) => x,
        Err("method name reserved (rpc.*)") => return Some(method_not_found(id_for_errors)),
        Err("method must be a string") | Err("jsonrpc version missing or not 2.0") => {
            return Some(invalid_request());
        }
        Err(detail) => {
            // params wrong shape, etc.
            return Some(invalid_params(id_for_errors, detail));
        }
    };

    // --- Built-in root methods support (notifications still receive no reply) ---
    if id_opt.is_none() {
        // Valid notification → process but do not respond
        let method_exists = {
            if is_builtin_root_method(method) {
                true
            } else {
                let methods = state.registry.list().await;
                methods.iter().any(|m| m == method)
            }
        };
        if !method_exists {
            // MUST NOT reply to a notification (even if unknown)
            return None;
        }
        // Process side-effecting built-ins even though notifications have no response.
        match builtin_method(method) {
            Some(BuiltinMethod::BtcBroadcastTransaction) => {
                let _ = broadcast_transaction_response(Value::Null, params).await;
            }
            Some(BuiltinMethod::BtcSubmitPackage) => {
                let _ = submit_package_response(Value::Null, params).await;
            }
            Some(_) => {}
            None => {
                let cx = context::current();
                let _ = state.registry.call(cx, method, params.clone()).await;
            }
        }
        return None;
    }

    // Normal call (must produce a response)
    let id = id_opt.unwrap(); // safe

    // If a built-in is requested, handle immediately.
    if let Some(builtin) = builtin_method(method) {
        return Some(builtin_response(builtin, state, caller, id, params).await);
    }

    // Check method existence to produce -32601 at the protocol layer
    let method_exists = {
        let methods = state.registry.list().await;
        methods.iter().any(|m| m == method)
    };
    if !method_exists {
        return Some(method_not_found(id));
    }

    // Invoke registered method WITH THE ORIGINAL PARAMS
    let cx = context::current();
    let result = match std::panic::AssertUnwindSafe(state.registry.call(cx, method, params))
        .catch_unwind()
        .await
    {
        Ok(v) => v,
        Err(_) => return Some(internal_error(id, "handler panicked")),
    };

    Some(JsonRpcResponse { jsonrpc: JSONRPC_VERSION, result: Some(result), error: None, id })
}

// ---- Axum wiring ------------------------------------------------------------

pub async fn run_rpc(registry: RpcRegistry, addr: SocketAddr) -> anyhow::Result<()> {
    let state = Arc::new(RpcState { registry });
    let app = Router::new().route("/rpc", post(handle_rpc)).with_state(state);

    eprintln!("[rpc] listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

#[inline]
fn json_ok(body: Vec<u8>) -> Response {
    (StatusCode::OK, [(CONTENT_TYPE, "application/json")], body).into_response()
}

async fn handle_rpc(
    State(state): State<Arc<RpcState>>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let caller = CallerContext { headers, peer: Some(peer) };
    // 1) Try to parse raw JSON (to distinguish -32700 from other errors)
    let parsed: serde_json::Result<Value> = serde_json::from_slice(&body);

    let value = match parsed {
        Ok(v) => v,
        Err(_) => {
            let resp = parse_error();
            let body = serde_json::to_vec(&resp).unwrap_or_else(|_| b"{}".to_vec());
            return json_ok(body);
        }
    };

    // 2) Handle batch or single
    match value {
        Value::Array(items) => {
            // Empty array is invalid request
            if items.is_empty() {
                let resp = invalid_request();
                let body = serde_json::to_vec(&resp).unwrap();
                return json_ok(body);
            }

            // Process each element; invalid entries produce individual -32600
            let mut responses: Vec<JsonRpcResponse> = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Object(obj) => {
                        if let Some(resp) = handle_single_request(&state, &caller, &obj).await {
                            responses.push(resp);
                        }
                    }
                    _ => {
                        // Each non-object entry yields its own -32600 with id = null
                        responses.push(invalid_request());
                    }
                }
            }

            if responses.is_empty() {
                // All were notifications → MUST return nothing at all
                return StatusCode::NO_CONTENT.into_response();
            }

            let body = serde_json::to_vec(&responses).unwrap();
            json_ok(body)
        }
        Value::Object(obj) => match handle_single_request(&state, &caller, &obj).await {
            Some(resp) => {
                let body = serde_json::to_vec(&resp).unwrap();
                json_ok(body)
            }
            None => {
                // Valid notification → no content, no body
                StatusCode::NO_CONTENT.into_response()
            }
        },
        _ => {
            // Non-object, non-array top-level → invalid request
            let resp = invalid_request();
            let body = serde_json::to_vec(&resp).unwrap();
            json_ok(body)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Transaction, absolute::LockTime, consensus::serialize, transaction::Version};

    fn fee_block(index: usize, vsize: u64, median_fee_rate: f64) -> MempoolBlockSummary {
        MempoolBlockSummary {
            index,
            tx_count: 1,
            trace_count: 0,
            weight: vsize.saturating_mul(4),
            vsize,
            total_fees: 0,
            median_fee_rate: Some(median_fee_rate),
            min_fee_rate: Some(median_fee_rate),
            max_fee_rate: Some(median_fee_rate),
            fee_range: vec![median_fee_rate],
        }
    }

    #[test]
    fn precise_fee_estimates_use_projected_block_medians() {
        let blocks = vec![
            fee_block(0, 1_000_000, 10.0),
            fee_block(1, 1_000_000, 6.0),
            fee_block(2, 1_000_000, 2.0),
        ];

        let estimates = calculate_fee_estimates(&blocks, 1.0, 1_000_000.0);

        assert_eq!(
            estimates,
            FeeEstimates {
                fastest_fee: 10.5,
                half_hour_fee: 8.25,
                hour_fee: 5.0,
                economy_fee: 2.0,
                minimum_fee: 1.0,
            }
        );
    }

    #[test]
    fn precise_fee_estimates_have_stable_empty_view_floors() {
        let estimates = calculate_fee_estimates(&[], 0.1, 1_000_000.0);

        assert_eq!(
            estimates,
            FeeEstimates {
                fastest_fee: 1.0,
                half_hour_fee: 0.5,
                hour_fee: 0.1,
                economy_fee: 0.1,
                minimum_fee: 0.1,
            }
        );
    }

    #[test]
    fn raw_transaction_params_accept_object_and_positional_forms() {
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: Vec::new(),
            output: Vec::new(),
        };
        let raw = serialize(&tx);
        let raw_hex = hex::encode(&raw);

        assert_eq!(parse_raw_transaction_params(json!({ "raw_tx": raw_hex })).unwrap(), raw);
        assert_eq!(parse_raw_transaction_params(json!([hex::encode(&raw)])).unwrap(), raw);
    }

    #[test]
    fn raw_transaction_params_reject_invalid_payloads() {
        assert!(parse_raw_transaction_params(json!({ "raw_tx": "not-hex" })).is_err());
        assert!(parse_raw_transaction_params(json!([])).is_err());
        assert!(parse_raw_transaction_params(Value::Null).is_err());
    }

    #[test]
    fn address_params_accept_object_and_positional_forms() {
        let address = "1wiz18xYmhRX6xStj2b9t1rwWX4GKUgpv";
        assert_eq!(parse_address_params(json!({ "address": address })).unwrap(), address);
        assert_eq!(parse_address_params(json!([address])).unwrap(), address);
    }

    #[test]
    fn address_params_reject_invalid_payloads() {
        assert!(parse_address_params(json!({ "address": "" })).is_err());
        assert!(parse_address_params(json!([])).is_err());
        assert!(parse_address_params(Value::Null).is_err());
    }
}
