use crate::{
    config::{get_espo_module_mdb, get_espo_next_height},
    modules::defs::RpcRegistry,
};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::FutureExt;
use serde::Serialize;
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc};
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

// Built-in root method name
const ROOT_METHOD_GET_ESPO_HEIGHT: &str = "get_espo_height";
const ROOT_METHOD_GET_METHOD_LINE_CHART: &str = "get_method_line_chart";

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
    method == ROOT_METHOD_GET_ESPO_HEIGHT || method == ROOT_METHOD_GET_METHOD_LINE_CHART
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
    get_espo_module_mdb("essentials")
        .indexed_height_bounds()
        .map_err(|e| format!("failed to read indexed height bounds: {e}"))?
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
        // Fire-and-forget invoke for registered methods; built-ins do nothing.
        if !is_builtin_root_method(method) {
            let cx = context::current();
            let _ = state.registry.call(cx, method, params.clone()).await;
        }
        return None;
    }

    // Normal call (must produce a response)
    let id = id_opt.unwrap(); // safe

    // If the built-in root method is requested, handle immediately.
    if method == ROOT_METHOD_GET_ESPO_HEIGHT {
        return Some(get_espo_tip_height_response(id));
    }
    if method == ROOT_METHOD_GET_METHOD_LINE_CHART {
        return Some(get_method_line_chart_response(state, id, params).await);
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
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

#[inline]
fn json_ok(body: Vec<u8>) -> Response {
    (StatusCode::OK, [(CONTENT_TYPE, "application/json")], body).into_response()
}

async fn handle_rpc(State(state): State<Arc<RpcState>>, body: Bytes) -> Response {
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
                        if let Some(resp) = handle_single_request(&state, &obj).await {
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
        Value::Object(obj) => match handle_single_request(&state, &obj).await {
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
