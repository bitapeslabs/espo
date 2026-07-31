//! Documentation for the `btc.*` JSON-RPC namespace.
//!
//! These methods are proxies onto the Bitcoin backends espo is configured
//! against — electrs/Esplora for reads, Bitcoin Core for writes — rather than
//! anything espo indexes itself. They live in their own file for the same
//! reason they live in their own namespace: nothing here is answered from
//! espo's own indices, so it reads and changes separately from the rest.

use serde_json::json;

use super::docs::{MethodDoc, ModuleDoc, rpc_doc};

pub(super) fn btc_module_doc() -> ModuleDoc {
    ModuleDoc {
        slug: "btc-rpc",
        title: "Bitcoin JSON-RPC (btc.*)",
        intro: "Proxies onto the configured Bitcoin backends: electrs/Esplora for transaction and address reads, Bitcoin Core for broadcast and package submission. Nothing here is served from Espo's own indices, and the read methods return their backend's response shape unchanged.",
        methods: btc_methods(),
    }
}

fn btc_methods() -> Vec<MethodDoc> {
    let mut methods = vec![
        rpc_doc(
            "btc.get_transaction",
            "Returns one transaction in the configured electrs/Esplora JSON shape, passed through unchanged, together with its raw hex. Covers mempool as well as confirmed transactions. A transaction the index has never seen returns ok with found false rather than an error. Requires electrs_esplora_url; native Electrum RPC does not expose this shape.",
            json!({ "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90" }),
            json!({
                "ok": true,
                "found": true,
                "tx": {
                    "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                    "version": 2,
                    "locktime": 0,
                    "vin": [{ "txid": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8", "vout": 3, "prevout": { "scriptpubkey_address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "value": 546 }, "sequence": 4294967293u64 }],
                    "vout": [{ "scriptpubkey_address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "value": 546 }],
                    "size": 312,
                    "weight": 792,
                    "fee": 1410,
                    "status": { "confirmed": true, "block_height": 946000, "block_hash": "00000000000000000000f0b2e1a4f2ae1b0b0f4a6f0b2e1a4f2ae1b0b0f4a6f0", "block_time": 1741000000u64 }
                },
                "hex": "0200000000010..."
            }),
        ),
        rpc_doc(
            "btc.get_address",
            "Returns the configured electrs/Esplora address summary without changing its field names or response shape. This method requires electrs_esplora_url; native Electrum RPC does not expose the exact aggregate statistics.",
            json!({ "address": "1wiz18xYmhRX6xStj2b9t1rwWX4GKUgpv" }),
            json!({
                "address": "1wiz18xYmhRX6xStj2b9t1rwWX4GKUgpv",
                "chain_stats": {
                    "funded_txo_count": 11,
                    "funded_txo_sum": 15007688098u64,
                    "spent_txo_count": 5,
                    "spent_txo_sum": 15007599040u64,
                    "tx_count": 13
                },
                "mempool_stats": {
                    "funded_txo_count": 0,
                    "funded_txo_sum": 0,
                    "spent_txo_count": 0,
                    "spent_txo_sum": 0,
                    "tx_count": 0
                }
            }),
        ),
        rpc_doc(
            "btc.broadcast_transaction",
            "Broadcasts a raw Bitcoin transaction through the configured electrs or Esplora backend, with Bitcoin Core as a fallback.",
            json!({ "raw_tx": "0200000001..." }),
            json!({ "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90" }),
        ),
        rpc_doc(
            "btc.submit_package",
            "Submits related raw transactions to Bitcoin Core together through submitpackage, for cases one-at-a-time broadcasting cannot express — a child paying for its parent, most often. Order them parents first, at most 25. Core's per-transaction reply is passed through untouched under `result`, since a package can partly succeed. There is no Esplora fallback: package submission exists only on Core.",
            json!({ "txs": ["0200000001...parent...", "0200000001...child..."] }),
            json!({
                "ok": true,
                "result": {
                    "package_msg": "success",
                    "tx-results": {
                        "e3f1...": { "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90", "vsize": 141, "fees": { "base": 0.00000141 } }
                    },
                    "replaced-transactions": []
                }
            }),
        ),
        rpc_doc(
            "btc.fee_estimates",
            "Returns precise sat/vB fee recommendations derived from Espo's projected mempool blocks. The fields match mempool.space's precise fee response shape.",
            json!({}),
            json!({
                "fastestFee": 1.017,
                "halfHourFee": 0.722,
                "hourFee": 0.448,
                "economyFee": 0.2,
                "minimumFee": 0.1
            }),
        ),
    ];

    // The faucet method only exists on regtest with a faucet configured, so it
    // is only documented where it can actually be called.
    if crate::explorer::faucet::faucet_enabled() {
        methods.push(rpc_doc(
            "btc.faucet_status",
            "Returns the faucet's per-asset availability, limits and remaining balance, as it reports them — the same proxy behind the explorer's faucet page. Takes no parameters. This method exists only on regtest with a faucet configured; elsewhere it is not a method at all.",
            json!({}),
            json!({
                "rbtc": {
                    "enabled": true,
                    "min_amount": 0.0001,
                    "max_amount": 0.01,
                    "total_available": 12.5,
                    "max_per_ip_per_day": 0.05
                },
                "diesel": {
                    "enabled": true,
                    "min_amount": 1,
                    "max_amount": 100,
                    "total_available": 50000,
                    "max_per_ip_per_day": 500
                }
            }),
        ));
        methods.push(rpc_doc(
            "btc.faucet_request",
            "Requests regtest coins from the configured faucet — the same proxy behind the explorer's faucet page, including the caller IP it rate-limits on. `amount` and `asset` are optional; `asset` is rbtc or diesel. The faucet's own reply is returned unchanged. This method exists only on regtest with a faucet configured; elsewhere it is not a method at all.",
            json!({ "address": "bcrt1q9d4ywgfnd8h43da5tpcxcn6ajv590cg6d3tg6axemvljvt2k76zqwq2fs3", "amount": 0.001, "asset": "rbtc" }),
            json!({ "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90" }),
        ));
    }

    methods
}
