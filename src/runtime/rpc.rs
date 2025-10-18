use crate::{config::get_espo_next_height, modules::defs::RpcRegistry};
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

// Built-in root method name
const ROOT_METHOD_GET_ESPO_HEIGHT: &str = "get_espo_height";

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

    // --- Built-in "get_espo_height" support (notifications still receive no reply) ---
    if id_opt.is_none() {
        // Valid notification → process but do not respond
        let method_exists = {
            // Consider built-in root method as existing
            if method == ROOT_METHOD_GET_ESPO_HEIGHT {
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
        // Fire-and-forget invoke for registered methods; built-in does nothing
        if method != ROOT_METHOD_GET_ESPO_HEIGHT {
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
