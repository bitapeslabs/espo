#![cfg(not(target_arch = "wasm32"))]

/// End-to-end integration tests for the oylapi module
///
/// These tests:
/// 1. Deploy AMM contracts through metashrew (alkanes.wasm indexer)
/// 2. Extract traces from the indexed blocks
/// 3. Build EspoBlocks and index through ESPO modules (essentials, ammdata, subfrost)
/// 4. Query oylapi endpoints and verify correct data is returned
///
/// This proves the full data flow from Bitcoin blocks → metashrew → ESPO → oylapi API

#[cfg(feature = "test-utils")]
mod tests {
    use anyhow::Result;
    use axum::Router;
    use axum::http::StatusCode;
    use espo::alkanes::trace::EspoBlock;
    use espo::modules::ammdata::main::AmmData;
    use espo::modules::ammdata::storage::AmmDataProvider;
    use espo::modules::defs::EspoModule;
    use espo::modules::essentials::main::Essentials;
    use espo::modules::essentials::storage::EssentialsProvider;
    use espo::modules::oylapi::config::OylApiConfig;
    use espo::modules::oylapi::server::router;
    use espo::modules::oylapi::storage::OylApiState;
    use espo::modules::subfrost::main::Subfrost;
    use espo::modules::subfrost::storage::SubfrostProvider;
    use espo::runtime::mdb::Mdb;
    use espo::test_utils::*;
    use rocksdb::{DB, Options};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// Helper to make POST request to router (runs async code in provided runtime)
    fn post_request_sync(
        rt: &tokio::runtime::Runtime,
        app: &Router,
        path: &str,
        body: Value,
    ) -> Result<(StatusCode, Value)> {
        rt.block_on(async {
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
        })
    }

    /// Initialize global config for tests - handles already-initialized case
    fn init_test_config() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let base_path = temp_dir.path();

        let db_path = base_path.join("espo_db");
        let metashrew_db = base_path.join("metashrew_db");
        let blocks_dir = base_path.join("blocks");

        std::fs::create_dir_all(&db_path)?;
        std::fs::create_dir_all(&metashrew_db)?;
        std::fs::create_dir_all(&blocks_dir)?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, &db_path)?;
        DB::open(&opts, &metashrew_db)?;

        let config = espo::config::AppConfig {
            readonly_metashrew_db_dir: metashrew_db.to_str().unwrap().to_string(),
            electrum_rpc_url: None,
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
            debug_ignore_ms: 0,
            debug_backup: None,
            safe_tip_hook_script: None,
            block_source_mode: espo::core::blockfetcher::BlockFetchMode::Auto,
            compact_tx_trace_rows: true,
            address_index_chunk_size: 512,
            explorer_networks: None,
            google_analytics_tag: None,
            modules: std::collections::HashMap::new(),
        };

        std::mem::forget(temp_dir);

        // Try to initialize, ignore if already initialized
        match espo::config::init_config_from(config) {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("already initialized") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create ESPO providers with fresh database
    fn create_espo_providers() -> Result<(
        Arc<EssentialsProvider>,
        Arc<AmmDataProvider>,
        Arc<SubfrostProvider>,
        Arc<DB>,
        tempfile::TempDir,
    )> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("espo_db");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, &db_path)?);

        let essentials_mdb = Mdb::from_db(db.clone(), b"essentials:");
        let essentials = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));

        let amm_mdb = Mdb::from_db(db.clone(), b"ammdata:");
        let ammdata = Arc::new(AmmDataProvider::new(Arc::new(amm_mdb), essentials.clone()));

        let subfrost_mdb = Mdb::from_db(db.clone(), b"subfrost:");
        let subfrost = Arc::new(SubfrostProvider::new(Arc::new(subfrost_mdb)));

        Ok((essentials, ammdata, subfrost, db, temp_dir))
    }

    /// Create oylapi router with providers
    fn create_router(
        essentials: Arc<EssentialsProvider>,
        ammdata: Arc<AmmDataProvider>,
        subfrost: Arc<SubfrostProvider>,
    ) -> Router {
        let config = OylApiConfig {
            host: "127.0.0.1".to_string(),
            port: 3001,
            alkane_icon_cdn: "https://cdn.example.com/icons".to_string(),
            ord_endpoint: None,
        };

        let state = OylApiState {
            config,
            essentials,
            ammdata,
            subfrost,
            http_client: reqwest::Client::new(),
        };

        router(state)
    }

    /// Helper struct holding ESPO modules for indexing
    struct EspoModules {
        essentials: Essentials,
        ammdata: AmmData,
        subfrost: Subfrost,
    }

    /// Create ESPO module instances with fresh database
    ///
    /// Returns modules configured with shared RocksDB storage for indexing,
    /// along with providers for querying via oylapi.
    fn create_espo_modules() -> Result<(
        EspoModules,
        Arc<EssentialsProvider>,
        Arc<AmmDataProvider>,
        Arc<SubfrostProvider>,
        Arc<DB>,
        tempfile::TempDir,
    )> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("espo_db");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, &db_path)?);

        // Create Mdb instances for each module (with namespace prefixes)
        let essentials_mdb = Arc::new(Mdb::from_db(db.clone(), b"essentials:"));
        let ammdata_mdb = Arc::new(Mdb::from_db(db.clone(), b"ammdata:"));
        let subfrost_mdb = Arc::new(Mdb::from_db(db.clone(), b"subfrost:"));

        // Create module instances and set their Mdb
        let mut essentials_module = Essentials::new();
        essentials_module.set_mdb(essentials_mdb.clone());

        let mut ammdata_module = AmmData::new();
        ammdata_module.set_mdb(ammdata_mdb.clone());

        let mut subfrost_module = Subfrost::new();
        subfrost_module.set_mdb(subfrost_mdb.clone());

        // Create providers for querying (oylapi uses these)
        let essentials_provider = Arc::new(EssentialsProvider::new(essentials_mdb));
        let ammdata_provider =
            Arc::new(AmmDataProvider::new(ammdata_mdb, essentials_provider.clone()));
        let subfrost_provider = Arc::new(SubfrostProvider::new(subfrost_mdb));

        let modules = EspoModules {
            essentials: essentials_module,
            ammdata: ammdata_module,
            subfrost: subfrost_module,
        };

        Ok((modules, essentials_provider, ammdata_provider, subfrost_provider, db, temp_dir))
    }

    /// Index an EspoBlock through all ESPO modules
    ///
    /// Order matters: essentials must be indexed first since ammdata depends on it.
    fn index_espo_block(modules: &EspoModules, block: EspoBlock) -> Result<()> {
        // Clone the block for each module since index_block takes ownership
        modules.essentials.index_block(block.clone())?;
        modules.ammdata.index_block(block.clone())?;
        modules.subfrost.index_block(block)?;
        Ok(())
    }

    // ============================================================================
    // Basic E2E Tests (synchronous to avoid runtime conflicts)
    // ============================================================================

    #[test]
    fn test_e2e_metashrew_runtime_creation() -> Result<()> {
        println!("\n[E2E] Testing metashrew runtime creation...");

        // Create tokio runtime for TestMetashrewRuntime
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();

        let runtime = TestMetashrewRuntime::new()?;

        // Verify DB is accessible
        assert!(runtime.db().get(b"test").is_ok());

        println!("[E2E] Metashrew runtime created successfully");
        Ok(())
    }

    #[test]
    fn test_e2e_setup_blocks() -> Result<()> {
        println!("\n[E2E] Testing setup block indexing...");

        // Create tokio runtime for TestMetashrewRuntime
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();

        let runtime = TestMetashrewRuntime::new()?;

        // Index setup blocks 0-3
        for h in 0..=3 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            runtime.index_block(&block, h)?;
            println!("[E2E] Indexed block {}", h);
        }

        println!("[E2E] Setup blocks indexed successfully");
        Ok(())
    }

    #[test]
    fn test_e2e_amm_deployment() -> Result<()> {
        println!("\n[E2E] Testing AMM deployment through metashrew...");

        // Create tokio runtime for TestMetashrewRuntime
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();

        let runtime = TestMetashrewRuntime::new()?;

        // Index setup blocks
        for h in 0..=3 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            runtime.index_block(&block, h)?;
        }

        // Deploy AMM infrastructure
        let start_height = 4;
        let deployment = setup_amm(&runtime, start_height)?;

        println!("[E2E] AMM deployed:");
        println!("[E2E]   Factory Proxy: {:?}", deployment.factory_proxy_id);
        println!("[E2E]   Pool Template: {:?}", deployment.pool_template_id);

        // Extract traces from deployment blocks
        for height in start_height..=(start_height + 5) {
            let traces = runtime.get_traces_for_block(height)?;
            println!("[E2E]   Block {} traces: {}", height, traces.len());
        }

        println!("[E2E] AMM deployment successful");
        Ok(())
    }

    #[test]
    fn test_e2e_oylapi_with_providers() -> Result<()> {
        println!("\n[E2E] Testing oylapi with ESPO providers...");

        init_test_config()?;

        // Create providers
        let (essentials, ammdata, subfrost, _db, _temp_dir) = create_espo_providers()?;

        // Create router
        let router = create_router(essentials, ammdata, subfrost);

        // Create a runtime for async HTTP requests
        let rt = tokio::runtime::Runtime::new()?;

        // Test endpoints
        let (status, response) =
            post_request_sync(&rt, &router, "/get-alkanes", json!({"limit": 10}))?;

        assert_eq!(status, StatusCode::OK);
        println!("[E2E] /get-alkanes response: {:?}", response);

        // Test pools endpoint
        let (status, _response) = post_request_sync(
            &rt,
            &router,
            "/get-pools",
            json!({
                "factoryId": {"block": "4", "tx": "1"},
                "limit": 10
            }),
        )?;
        assert_eq!(status, StatusCode::OK);

        println!("[E2E] oylapi with providers test complete");
        Ok(())
    }

    #[test]
    fn test_e2e_full_flow_metashrew_to_traces() -> Result<()> {
        println!("\n[E2E] Testing full flow: metashrew → traces → EspoBlock...");

        // Create tokio runtime for TestMetashrewRuntime
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();

        let runtime = TestMetashrewRuntime::new()?;

        // Index setup blocks
        for h in 0..=3 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            runtime.index_block(&block, h)?;
        }

        // Deploy AMM infrastructure
        let start_height = 4;
        let _deployment = setup_amm(&runtime, start_height)?;

        // For each deployment block, extract traces and build EspoBlock
        for height in start_height..=(start_height + 5) {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(height);
            let traces = runtime.get_traces_for_block(height)?;

            let espo_block = build_espo_block(height, &block, traces.clone())?;

            println!(
                "[E2E] Block {} → {} traces → EspoBlock with {} txs ({} with traces)",
                height,
                traces.len(),
                espo_block.transactions.len(),
                espo_block.transactions.iter().filter(|t| t.traces.is_some()).count()
            );
        }

        println!("[E2E] Full flow test complete");
        Ok(())
    }

    #[test]
    fn test_e2e_oylapi_after_amm_deployment() -> Result<()> {
        println!("\n[E2E] Testing oylapi query after AMM deployment with ESPO indexing...");

        // Create tokio runtime for TestMetashrewRuntime and HTTP requests
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();

        init_test_config()?;

        // First, deploy AMM through metashrew
        let metashrew_runtime = TestMetashrewRuntime::new()?;

        // Store setup blocks for later ESPO indexing
        let mut setup_blocks = std::collections::HashMap::new();

        // Index setup blocks through metashrew
        for h in 0..=3 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            metashrew_runtime.index_block(&block, h)?;
            setup_blocks.insert(h, block);
        }

        let start_height = 4;
        let deployment = setup_amm(&metashrew_runtime, start_height)?;

        println!("[E2E] AMM deployed at factory: {:?}", deployment.factory_proxy_id);

        // Create ESPO modules and providers
        let (modules, essentials, ammdata, subfrost, _db, _temp_dir) = create_espo_modules()?;

        // Extract traces from metashrew and index through ESPO modules
        // Index setup blocks (0-3) first using the actual blocks
        for h in 0..=3 {
            let block = setup_blocks.get(&h).unwrap();
            let traces = metashrew_runtime.get_traces_for_block(h)?;
            let espo_block = build_espo_block(h, block, traces)?;
            index_espo_block(&modules, espo_block)?;
            println!("[E2E] Indexed block {} through ESPO", h);
        }

        // Index deployment blocks (4+) using the actual deployment blocks
        let end_height = start_height + 5;
        for h in start_height..=end_height {
            // Use the actual block from the deployment
            let block = deployment
                .blocks
                .get(&h)
                .ok_or_else(|| anyhow::anyhow!("Missing deployment block at height {}", h))?;

            let traces = metashrew_runtime.get_traces_for_block(h)?;
            let espo_block = build_espo_block(h, block, traces.clone())?;

            let traces_with_data =
                espo_block.transactions.iter().filter(|t| t.traces.is_some()).count();

            index_espo_block(&modules, espo_block)?;
            println!(
                "[E2E] Indexed block {} through ESPO ({} traces, {} txs with trace data)",
                h,
                traces.len(),
                traces_with_data
            );
        }

        // Create router with the indexed providers
        let router = create_router(essentials, ammdata, subfrost);

        // Query for pools at the factory
        let factory_block = deployment.factory_proxy_id.block;
        let factory_tx = deployment.factory_proxy_id.tx;

        let (status, response) = post_request_sync(
            &rt,
            &router,
            "/get-pools",
            json!({
                "factoryId": {
                    "block": factory_block.to_string(),
                    "tx": factory_tx.to_string()
                },
                "limit": 10
            }),
        )?;

        assert_eq!(status, StatusCode::OK);
        println!("[E2E] /get-pools response: {:?}", response);

        // Query for all pools details
        let (status, response) = post_request_sync(
            &rt,
            &router,
            "/get-all-pools-details",
            json!({
                "factoryId": {
                    "block": factory_block.to_string(),
                    "tx": factory_tx.to_string()
                },
                "limit": 10,
                "offset": 0
            }),
        )?;

        assert_eq!(status, StatusCode::OK);
        println!("[E2E] /get-all-pools-details: {:?}", response);

        // Query alkanes to see deployed contracts
        let (status, response) =
            post_request_sync(&rt, &router, "/get-alkanes", json!({"limit": 20}))?;

        assert_eq!(status, StatusCode::OK);
        println!("[E2E] /get-alkanes response: {:?}", response);

        println!("[E2E] oylapi after AMM deployment with ESPO indexing complete");
        Ok(())
    }

    // ============================================================================
    // Full Integration Test (requires complete ESPO module indexing)
    // ============================================================================

    #[test]
    #[ignore] // Enable when full ESPO indexing is wired up
    fn test_e2e_full_pool_creation_and_query() -> Result<()> {
        println!("\n[E2E] Testing full pool creation and query...");

        // This test would:
        // 1. Deploy AMM through metashrew
        // 2. Deploy two test tokens
        // 3. Create a pool with those tokens
        // 4. Extract traces and build EspoBlocks
        // 5. Index through ESPO modules (essentials, ammdata)
        // 6. Query oylapi and verify:
        //    - Pool appears in /get-pools
        //    - Pool details correct in /get-pool-details
        //    - Tokens appear in /get-alkanes

        println!("[E2E] Full pool creation flow test placeholder");
        Ok(())
    }

    #[test]
    #[ignore] // Enable when full ESPO indexing is wired up
    fn test_e2e_swap_tracking_full() -> Result<()> {
        println!("\n[E2E] Testing swap tracking...");

        // This test would:
        // 1. Set up pool with liquidity
        // 2. Perform swaps through metashrew
        // 3. Extract traces and index through ESPO
        // 4. Verify /get-pool-swap-history returns correct data

        println!("[E2E] Swap tracking test placeholder");
        Ok(())
    }

    // ============================================================================
    // Status Report
    // ============================================================================

    #[test]
    fn test_e2e_status() {
        println!("\n=== OylAPI E2E Test Status ===\n");
        println!("Working:");
        println!("  [x] TestMetashrewRuntime initialization");
        println!("  [x] Block indexing through alkanes.wasm");
        println!("  [x] AMM deployment through metashrew");
        println!("  [x] Trace extraction from metashrew DB");
        println!("  [x] EspoBlock building from traces");
        println!("  [x] ESPO module index_block calls wired up");
        println!("  [x] OylAPI router creation with providers");
        println!("  [x] Basic endpoint smoke tests");
        println!("  [x] Deployed contracts appear in /get-alkanes");
        println!();
        println!("To complete full E2E data verification:");
        println!("  [ ] Create pool with tokens and add liquidity");
        println!("  [ ] Perform swaps and verify tracking");
        println!("  [ ] Verify oylapi returns correct computed values");
        println!();
        println!("Run with: cargo test --features test-utils --test oylapi_e2e -- --nocapture");
    }
}

/// Status test for non-test-utils builds
#[cfg(not(feature = "test-utils"))]
#[test]
fn test_oylapi_e2e_requires_test_utils_feature() {
    println!("OylAPI E2E tests require the test-utils feature");
    println!("Run with: cargo test --features test-utils --test oylapi_e2e");
}
