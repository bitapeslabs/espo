use crate::config::{debug_enabled, get_config, get_espo_module_mdb};
use crate::debug;
use crate::modules::ammdata::storage::AmmDataProvider;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::EssentialsProvider;
use crate::modules::oylapi::config::OylApiConfig;
use crate::modules::oylapi::server::run as run_oylapi;
use crate::modules::oylapi::storage::OylApiState;
use crate::modules::subfrost::storage::SubfrostProvider;
use crate::runtime::mdb::Mdb;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct OylApi {
    config: Option<OylApiConfig>,
    mdb: Option<Arc<Mdb>>,
    essentials: Option<Arc<EssentialsProvider>>,
    ammdata: Option<Arc<AmmDataProvider>>,
    subfrost: Option<Arc<SubfrostProvider>>,
}

impl OylApi {
    pub fn new() -> Self {
        Self { config: None, mdb: None, essentials: None, ammdata: None, subfrost: None }
    }
}

impl Default for OylApi {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for OylApi {
    fn get_name(&self) -> &'static str {
        "oylapi"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let essentials = Arc::new(EssentialsProvider::new(get_espo_module_mdb("essentials")));
        let ammdata =
            Arc::new(AmmDataProvider::new(get_espo_module_mdb("ammdata"), essentials.clone()));
        let subfrost = Arc::new(SubfrostProvider::new(get_espo_module_mdb("subfrost")));
        self.mdb = Some(mdb);
        self.essentials = Some(essentials);
        self.ammdata = Some(ammdata);
        self.subfrost = Some(subfrost);
    }

    fn get_genesis_block(&self, _network: Network) -> u32 {
        u32::MAX
    }

    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        self.mdb.clone()
    }

    fn index_block(&self, block: crate::alkanes::trace::EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();

        let timer = debug::start_if(debug);
        let has_config = self.config.is_some();
        debug::log_elapsed(module, "check_config", timer);

        let timer = debug::start_if(debug);
        let has_essentials = self.essentials.is_some();
        debug::log_elapsed(module, "check_essentials", timer);

        let timer = debug::start_if(debug);
        let has_ammdata = self.ammdata.is_some();
        debug::log_elapsed(module, "check_ammdata", timer);

        let timer = debug::start_if(debug);
        let has_subfrost = self.subfrost.is_some();
        debug::log_elapsed(module, "check_subfrost", timer);

        let timer = debug::start_if(debug);
        let _state = (has_config, has_essentials, has_ammdata, has_subfrost);
        debug::log_elapsed(module, "finalize", timer);
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        None
    }

    fn register_rpc(&self, _reg: &RpcNsRegistrar) {
        let Some(cfg) = self.config.clone() else {
            return;
        };
        let essentials = self
            .essentials
            .as_ref()
            .expect("oylapi module missing essentials provider")
            .clone();
        let ammdata =
            self.ammdata.as_ref().expect("oylapi module missing ammdata provider").clone();
        let subfrost =
            self.subfrost.as_ref().expect("oylapi module missing subfrost provider").clone();

        let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port)
            .parse()
            .unwrap_or_else(|e| panic!("invalid oylapi host/port: {e}"));

        let state = OylApiState {
            config: cfg,
            essentials,
            ammdata,
            subfrost,
            http_client: reqwest::Client::new(),
        };

        tokio::spawn(async move {
            if let Err(e) = run_oylapi(addr, state).await {
                eprintln!("[oylapi] server error: {e:?}");
            }
        });
        eprintln!("[oylapi] listening on {}", addr);
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(OylApiConfig::spec())
    }

    fn set_config(&mut self, config: &serde_json::Value) -> Result<()> {
        if get_config().electrs_esplora_url.is_none() {
            return Err(anyhow!("oylapi requires electrs_esplora_url (script-hash UTXO support)"));
        }
        let parsed = OylApiConfig::from_value(config)?;
        self.config = Some(parsed);
        Ok(())
    }
}
