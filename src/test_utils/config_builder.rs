use crate::config::{AppConfig, StrictModeConfig};
use crate::core::blockfetcher::BlockFetchMode;
use bitcoin::Network;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

/// Builder for creating test AppConfig instances with temporary directories
pub struct TestConfigBuilder {
    config: AppConfig,
    temp_dirs: Vec<TempDir>,
}

impl TestConfigBuilder {
    /// Create a new test config builder with sensible defaults
    pub fn new() -> Self {
        // Create temporary directories
        let metashrew_temp = TempDir::new().expect("create temp metashrew db");
        let espo_temp = TempDir::new().expect("create temp espo db");

        let metashrew_path = metashrew_temp.path().to_string_lossy().to_string();
        let espo_path = espo_temp.path().to_string_lossy().to_string();

        let config = AppConfig {
            readonly_metashrew_db_dir: metashrew_path,
            electrum_rpc_url: None,
            metashrew_rpc_url: "http://127.0.0.1:7044".to_string(), // Placeholder
            electrs_esplora_url: Some("http://127.0.0.1:4332".to_string()), // Placeholder
            bitcoind_rpc_url: "http://127.0.0.1:8332".to_string(),  // Placeholder
            bitcoind_rpc_user: "test".to_string(),
            bitcoind_rpc_pass: "test".to_string(),
            bitcoind_blocks_dir: "/tmp".to_string(), // Placeholder
            reset_mempool_on_startup: false,
            view_only: true, // Default to view-only for tests
            db_path: espo_path,
            sdb_poll_ms: 100, // Fast polling for tests
            indexer_block_delay_ms: 0,
            port: 0, // Let OS assign port
            explorer_host: None,
            explorer_base_path: "/".to_string(),
            explorer_pizza_tv_endpoint: "https://tv.pizza.fun".to_string(),
            network: Network::Regtest, // Default to regtest
            metashrew_db_label: None,
            strict_mode: None,
            debug: false,
            debug_ignore_ms: 0,
            debug_backup: None,
            safe_tip_hook_script: None,
            block_source_mode: BlockFetchMode::RpcOnly,
            compact_tx_trace_rows: true,
            address_index_chunk_size: 512,
            explorer_networks: None,
            google_analytics_tag: None,
            modules: HashMap::new(),
        };

        Self { config, temp_dirs: vec![metashrew_temp, espo_temp] }
    }

    /// Set the network (mainnet, testnet, regtest, etc.)
    pub fn with_network(mut self, network: Network) -> Self {
        self.config.network = network;
        self
    }

    /// Enable strict mode
    pub fn with_strict_mode(mut self, enabled: bool) -> Self {
        self.config.strict_mode = if enabled {
            Some(StrictModeConfig {
                check_utxos: true,
                check_alkane_balances: true,
                check_trace_mismatches: true,
            })
        } else {
            None
        };
        self
    }

    /// Set view-only mode
    pub fn with_view_only(mut self, view_only: bool) -> Self {
        self.config.view_only = view_only;
        self
    }

    /// Set a custom metashrew DB directory
    pub fn with_metashrew_db(mut self, path: &str) -> Self {
        self.config.readonly_metashrew_db_dir = path.to_string();
        self
    }

    /// Set a custom ESPO DB directory
    pub fn with_espo_db(mut self, path: &str) -> Self {
        self.config.db_path = path.to_string();
        self
    }

    /// Set the block source mode
    pub fn with_block_source_mode(mut self, mode: BlockFetchMode) -> Self {
        self.config.block_source_mode = mode;
        self
    }

    /// Add a temporary directory that will be managed by the builder
    pub fn add_temp_dir(&mut self, temp_dir: TempDir) {
        self.temp_dirs.push(temp_dir);
    }

    /// Create a new temporary directory and return its path
    pub fn create_temp_dir(&mut self) -> PathBuf {
        let temp_dir = TempDir::new().expect("create temp dir");
        let path = temp_dir.path().to_path_buf();
        self.temp_dirs.push(temp_dir);
        path
    }

    /// Build and return the config along with temp directory handles
    /// The caller must keep the TempDir handles alive to prevent cleanup
    pub fn build(self) -> (AppConfig, Vec<TempDir>) {
        (self.config, self.temp_dirs)
    }

    /// Build and return just the config (temp dirs will be dropped immediately)
    /// Warning: Only use this if you don't need the directories to persist
    pub fn build_transient(self) -> AppConfig {
        self.config
    }
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder_defaults() {
        let (config, _temp_dirs) = TestConfigBuilder::new().build();

        assert_eq!(config.network, Network::Regtest);
        assert_eq!(config.view_only, true);
        assert!(config.strict_mode.is_none());
    }

    #[test]
    fn test_config_builder_customization() {
        let (config, _temp_dirs) = TestConfigBuilder::new()
            .with_network(Network::Testnet)
            .with_strict_mode(true)
            .build();

        assert_eq!(config.network, Network::Testnet);
        assert!(config.strict_mode.is_some());
    }

    #[test]
    fn test_temp_dirs_exist() {
        let (config, temp_dirs) = TestConfigBuilder::new().build();

        // Verify temp directories exist
        assert!(std::path::Path::new(&config.readonly_metashrew_db_dir).exists());
        assert!(std::path::Path::new(&config.db_path).exists());

        // Verify we have temp dir handles
        assert_eq!(temp_dirs.len(), 2);
    }
}
