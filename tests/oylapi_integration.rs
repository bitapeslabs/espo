#![cfg(not(target_arch = "wasm32"))]

/// Integration tests for the oylapi module
///
/// These tests verify that the oylapi module correctly:
/// - Serves alkane-related endpoints
/// - Serves pool-related endpoints
/// - Serves swap/mint/burn history endpoints
/// - Serves wrap/unwrap endpoints
/// - Serves AMM transaction history endpoints
/// - Serves token pair endpoints
mod common;

use anyhow::Result;
use axum::Router;
use axum::http::StatusCode;
use espo::modules::ammdata::storage::AmmDataProvider;
use espo::modules::essentials::storage::EssentialsProvider;
use espo::modules::oylapi::config::OylApiConfig;
use espo::modules::oylapi::server::router;
use espo::modules::oylapi::storage::OylApiState;
use espo::modules::subfrost::storage::SubfrostProvider;
use espo::runtime::mdb::Mdb;
use rocksdb::{DB, Options};
use serde_json::{Value, json};
use std::sync::{Arc, Once};
use tower::ServiceExt;

static INIT: Once = Once::new();

/// Initialize global config once for all tests
fn init_global_config() {
    INIT.call_once(|| {
        // Set environment variable to skip external services
        // SAFETY: This is called once at the start of all tests before any threads are spawned
        unsafe {
            std::env::set_var("ESPO_SKIP_EXTERNAL_SERVICES", "1");
        }

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path();

        let db_path = base_path.join("espo_db");
        let metashrew_db = base_path.join("metashrew_db");
        let blocks_dir = base_path.join("blocks");

        std::fs::create_dir_all(&db_path).expect("Failed to create db_path");
        std::fs::create_dir_all(&metashrew_db).expect("Failed to create metashrew_db");
        std::fs::create_dir_all(&blocks_dir).expect("Failed to create blocks_dir");

        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, &db_path).expect("Failed to open espo DB");
        DB::open(&opts, &metashrew_db).expect("Failed to open metashrew DB");

        let config = espo::config::AppConfig {
            debug_backup: None,
            debug_ignore_ms: 0,
            safe_tip_hook_script: None,
            readonly_metashrew_db_dir: metashrew_db.to_str().unwrap().to_string(),
            electrum_rpc_url: Some(String::from("127.0.0.1:50001")),
            metashrew_rpc_url: String::from("http://127.0.0.1:9999"),
            electrs_esplora_url: Some(String::from("http://127.0.0.1:3000")),
            bitcoind_rpc_url: String::from("http://127.0.0.1:8332"),
            bitcoind_rpc_user: String::from("test"),
            bitcoind_rpc_pass: String::from("test"),
            bitcoind_blocks_dir: blocks_dir.to_str().unwrap().to_string(),
            reset_mempool_on_startup: false,
            view_only: true,
            db_path: db_path.to_str().unwrap().to_string(),
            sdb_poll_ms: 100,
            indexer_block_delay_ms: 0,
            port: 9090,
            explorer_host: None,
            explorer_base_path: String::from("/"),
            explorer_pizza_tv_endpoint: String::from("https://tv.pizza.fun"),
            network: bitcoin::Network::Regtest,
            metashrew_db_label: None,
            strict_mode: None,
            debug: false,
            block_source_mode: espo::core::blockfetcher::BlockFetchMode::Auto,
            compact_tx_trace_rows: true,
            address_index_chunk_size: 512,
            explorer_networks: None,
            google_analytics_tag: None,
            modules: std::collections::HashMap::new(),
        };

        std::mem::forget(temp_dir);
        espo::config::init_config_from(config).expect("Failed to initialize global config");
    });
}

/// Create test OylApiState with mock providers
fn create_test_state() -> Result<OylApiState> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_db");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, &db_path)?);

    let essentials_mdb = Mdb::from_db(db.clone(), b"essentials:");
    let essentials = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));

    let amm_mdb = Mdb::from_db(db.clone(), b"ammdata:");
    let ammdata = Arc::new(AmmDataProvider::new(Arc::new(amm_mdb), essentials.clone()));

    let subfrost_mdb = Mdb::from_db(db, b"subfrost:");
    let subfrost = Arc::new(SubfrostProvider::new(Arc::new(subfrost_mdb)));

    let config = OylApiConfig {
        host: "127.0.0.1".to_string(),
        port: 3001,
        alkane_icon_cdn: "https://cdn.example.com/icons".to_string(),
        ord_endpoint: None,
    };

    std::mem::forget(temp_dir);

    Ok(OylApiState { config, essentials, ammdata, subfrost, http_client: reqwest::Client::new() })
}

/// Helper to make POST request to router
async fn post_request(app: &Router, path: &str, body: Value) -> Result<(StatusCode, Value)> {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::POST)
                .uri(path)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes)?;

    Ok((status, body_json))
}

// ============================================================================
// Router Creation Tests
// ============================================================================

#[tokio::test]
async fn test_oylapi_router_creation() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let _app = router(state);

    // Verify router was created successfully
    Ok(())
}

// ============================================================================
// Token/Alkane Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_alkanes_by_address_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest"
    });

    let (status, response) = post_request(&app, "/get-alkanes-by-address", request_body).await?;

    // Should return 200 OK even with empty database
    assert_eq!(status, StatusCode::OK);
    assert!(response.is_object() || response.is_array());

    Ok(())
}

#[tokio::test]
async fn test_get_alkanes_utxo_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest"
    });

    let (status, response) = post_request(&app, "/get-alkanes-utxo", request_body).await?;

    assert_eq!(status, StatusCode::OK);
    assert!(response.is_object() || response.is_array());

    Ok(())
}

#[tokio::test]
async fn test_get_amm_utxos_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "spendStrategy": null
    });

    let (status, response) = post_request(&app, "/get-amm-utxos", request_body).await?;

    assert_eq!(status, StatusCode::OK);
    assert!(response.is_object() || response.is_array());

    Ok(())
}

#[tokio::test]
async fn test_get_alkanes_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "limit": 10,
        "offset": 0,
        "sortBy": "name",
        "order": "asc"
    });

    let (status, response) = post_request(&app, "/get-alkanes", request_body).await?;

    assert_eq!(status, StatusCode::OK);
    assert!(response.is_object() || response.is_array());

    Ok(())
}

#[tokio::test]
async fn test_global_alkanes_search_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "searchQuery": "test"
    });

    let (status, response) = post_request(&app, "/global-alkanes-search", request_body).await?;

    assert_eq!(status, StatusCode::OK);
    assert!(response.is_object() || response.is_array());

    Ok(())
}

#[tokio::test]
async fn test_get_alkane_details_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "alkaneId": {
            "block": "840000",
            "tx": "0"
        }
    });

    let (status, _response) = post_request(&app, "/get-alkane-details", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Pool Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_pools_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        },
        "limit": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-pools", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_pool_details_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        },
        "poolId": {
            "block": "840001",
            "tx": "0"
        }
    });

    let (status, _response) = post_request(&app, "/get-pool-details", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_all_pools_details_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        },
        "limit": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-all-pools-details", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_address_positions_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "factoryId": {
            "block": "840000",
            "tx": "0"
        }
    });

    let (status, _response) = post_request(&app, "/address-positions", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Swap History Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_pool_swap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "poolId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0,
        "successful": true,
        "includeTotal": true
    });

    let (status, _response) = post_request(&app, "/get-pool-swap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_token_swap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "tokenId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0,
        "successful": true,
        "includeTotal": true
    });

    let (status, _response) = post_request(&app, "/get-token-swap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_swap_history_for_pool_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "poolId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-swap-history-for-pool", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_swap_history_for_token_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "tokenId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-swap-history-for-token", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Pool Activity Endpoint Tests (Mint/Burn/Create)
// ============================================================================

#[tokio::test]
async fn test_get_pool_mint_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "poolId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-pool-mint-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_pool_burn_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "poolId": {
            "block": "840000",
            "tx": "0"
        },
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-pool-burn-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_pool_creation_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "poolId": null,
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-pool-creation-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_pool_creation_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "poolId": null,
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-pool-creation-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_pool_mint_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-pool-mint-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_pool_burn_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-pool-burn-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Wrap/Unwrap Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_address_wrap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-address-wrap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_address_unwrap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-address-unwrap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_all_wrap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-all-wrap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_all_unwrap_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-all-unwrap-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_total_unwrap_amount_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "blockHeight": 840000,
        "successful": true
    });

    let (status, _response) = post_request(&app, "/get-total-unwrap-amount", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// AMM Transaction History Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_all_address_amm_tx_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "address": "bcrt1qtest",
        "poolId": null,
        "transactionType": null,
        "count": 10,
        "offset": 0
    });

    let (status, _response) =
        post_request(&app, "/get-all-address-amm-tx-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_all_amm_tx_history_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "poolId": null,
        "transactionType": null,
        "count": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-all-amm-tx-history", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Token Pair Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_get_all_token_pairs_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        }
    });

    let (status, _response) = post_request(&app, "/get-all-token-pairs", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_token_pairs_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        },
        "alkaneId": {
            "block": "840001",
            "tx": "0"
        },
        "sortBy": "liquidity",
        "limit": 10,
        "offset": 0
    });

    let (status, _response) = post_request(&app, "/get-token-pairs", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_get_alkane_swap_pair_details_endpoint() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    let request_body = json!({
        "factoryId": {
            "block": "840000",
            "tx": "0"
        },
        "tokenAId": {
            "block": "840001",
            "tx": "0"
        },
        "tokenBId": {
            "block": "840002",
            "tx": "0"
        }
    });

    let (status, _response) =
        post_request(&app, "/get-alkane-swap-pair-details", request_body).await?;

    assert_eq!(status, StatusCode::OK);

    Ok(())
}

// ============================================================================
// Integration Test - Multiple Endpoints
// ============================================================================

#[tokio::test]
async fn test_all_endpoints_respond() -> Result<()> {
    init_global_config();

    let state = create_test_state()?;
    let app = router(state);

    // Test a few representative endpoints in sequence
    let endpoints = vec![
        ("/get-alkanes-by-address", json!({"address": "bcrt1qtest"})),
        ("/get-alkanes", json!({"limit": 10})),
        ("/get-pools", json!({"factoryId": {"block": "840000", "tx": "0"}, "limit": 10})),
        ("/get-all-wrap-history", json!({"count": 10})),
    ];

    for (path, body) in endpoints {
        let (status, _) = post_request(&app, path, body).await?;
        assert_eq!(status, StatusCode::OK, "Endpoint {} failed", path);
    }

    Ok(())
}
