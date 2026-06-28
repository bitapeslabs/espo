//! JSON-RPC handlers for the `explorerextensions` module.
//!
//! Methods (namespaced as `explorerextensions.*`):
//!   * `txs_by_alkane { alkane, page?, limit? }`
//!   * `internal_txs_by_alkane { alkane, page?, limit? }`
//!
//! `alkane` is the `"block:tx"` decimal id. `page` is 1-indexed
//! (default 1); `limit` defaults to 50 and is capped at 500. Results are
//! newest-first.

use crate::modules::defs::RpcNsRegistrar;
use crate::modules::explorerextensions::storage::{
    ExplorerExtProvider, normalize_limit, parse_alkane_id,
};
use serde_json::{Value, json};
use std::sync::Arc;

pub fn register_rpc(reg: RpcNsRegistrar, provider: Arc<ExplorerExtProvider>) {
    eprintln!("[RPC::EXPLOREREXT] registering RPC handlers…");

    {
        let reg_top = reg.clone();
        let prov = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_top
                .register("txs_by_alkane", move |_cx, payload| {
                    let prov = Arc::clone(&prov);
                    async move { handle_txs(prov.as_ref(), &payload, false) }
                })
                .await;
        });
    }

    {
        let reg_int = reg.clone();
        let prov = Arc::clone(&provider);
        tokio::spawn(async move {
            reg_int
                .register("internal_txs_by_alkane", move |_cx, payload| {
                    let prov = Arc::clone(&prov);
                    async move { handle_txs(prov.as_ref(), &payload, true) }
                })
                .await;
        });
    }
}

fn handle_txs(provider: &ExplorerExtProvider, payload: &Value, internal: bool) -> Value {
    let Some(alkane_str) = payload.get("alkane").and_then(|v| v.as_str()) else {
        return json!({ "ok": false, "error": "missing_alkane" });
    };
    let Some(alk) = parse_alkane_id(alkane_str) else {
        return json!({ "ok": false, "error": "invalid_alkane", "detail": alkane_str });
    };
    let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(1).max(1);
    let limit = normalize_limit(payload.get("limit").and_then(|v| v.as_u64()));

    let result = if internal {
        provider.internal_txs_by_alkane(&alk, page, limit)
    } else {
        provider.txs_by_alkane(&alk, page, limit)
    };

    match result {
        Ok((total, txs)) => json!({
            "ok": true,
            "alkane": format!("{}:{}", alk.block, alk.tx),
            "page": page,
            "limit": limit,
            "total": total,
            "txs": txs,
        }),
        Err(e) => json!({ "ok": false, "error": "internal_error", "detail": e.to_string() }),
    }
}
