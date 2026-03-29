use anyhow::Result;
use bitcoin::Network;
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::HashMap, path::Path, sync::Arc};
use tarpc::context;
use tokio::sync::RwLock;

use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_module_mdb, get_module_config};
use crate::runtime::mdb::Mdb;

/// Object-safe handler: (Context, JSON) -> JSON (async)
type HandlerFn = dyn Fn(context::Context, Value) -> BoxFuture<'static, Value> + Send + Sync;

#[derive(Clone, Default)]
pub struct RpcRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<HandlerFn>>>>,
}

impl RpcRegistry {
    pub async fn register<F, Fut>(&self, name: impl Into<String>, f: F)
    where
        F: Fn(context::Context, Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Value> + Send + 'static,
    {
        let name = name.into();
        let arc: Arc<HandlerFn> = Arc::new(move |cx, val| Box::pin(f(cx, val)));
        self.inner.write().await.insert(name, arc);
    }

    pub async fn list(&self) -> Vec<String> {
        self.inner.read().await.keys().cloned().collect()
    }

    pub async fn call(&self, cx: context::Context, method: &str, payload: Value) -> Value {
        match self.inner.read().await.get(method) {
            Some(h) => h(cx, payload).await,
            None => serde_json::json!({ "error": format!("unknown method: {method}") }),
        }
    }
}

/// A namespaced registrar that forces a fixed prefix on all registered RPC methods.
/// Modules only receive this, so they cannot choose global names.
/// For module "ammdata", every method becomes "ammdata.<suffix>".
#[derive(Clone)]
pub struct RpcNsRegistrar {
    inner: RpcRegistry,
    prefix: String, // e.g., "ammdata."
}

pub struct RpcCtx {
    pub registrar: RpcNsRegistrar,
    pub mdb: Mdb,
}

impl RpcNsRegistrar {
    pub fn new(inner: RpcRegistry, module_name: &str) -> Self {
        // Always end with a dot
        let mut prefix = String::with_capacity(module_name.len() + 1);
        prefix.push_str(module_name);
        if !prefix.ends_with('.') {
            prefix.push('.');
        }
        Self { inner, prefix }
    }

    /// Register a method using only the suffix; full name will be "<prefix><suffix>".
    pub async fn register<F, Fut>(&self, suffix: impl Into<String>, f: F)
    where
        F: Fn(context::Context, Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Value> + Send + 'static,
    {
        // Normalize suffix (strip any accidental leading dot)
        let mut s = suffix.into();
        if let Some(stripped) = s.strip_prefix('.') {
            s = stripped.to_string();
        }
        let full = format!("{}{}", self.prefix, s);
        self.inner.register(full, f).await;
    }

    /// Optional: list only methods under this namespace (helper).
    pub async fn list(&self) -> Vec<String> {
        self.inner
            .list()
            .await
            .into_iter()
            .filter(|m| m.starts_with(&self.prefix))
            .collect()
    }
}

/// Object-safe module interface (storable as dyn)
pub trait EspoModule: Send + Sync {
    fn get_name(&self) -> &'static str;

    /// Injected by the registry before boxing into Arc<dyn EspoModule>
    fn set_mdb(&mut self, mdb: Arc<Mdb>);

    fn get_genesis_block(&self, network: Network) -> u32;

    fn index_block(&self, block: EspoBlock) -> Result<()>;
    fn get_index_height(&self) -> Option<u32>;
    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        None
    }

    /// Modules can only register RPCs via a namespaced registrar.
    /// For a module named "ammdata", all methods will be "ammdata.<suffix>".
    fn register_rpc(&self, reg: &RpcNsRegistrar);

    /// Return Some to declare this module expects a config section.
    /// The string should describe the expected shape (used for error messages).
    fn config_spec(&self) -> Option<&'static str> {
        None
    }

    /// Load the module config section from config.json.
    fn set_config(&mut self, _config: &serde_json::Value) -> Result<()> {
        Ok(())
    }
}

/// Registry that holds modules and the RPC router.
pub struct ModuleRegistry {
    modules: Vec<Arc<dyn EspoModule>>,
    pub router: RpcRegistry,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self { modules: Vec::new(), router: RpcRegistry::default() }
    }

    /// Back-compat constructor.
    pub fn with_db<T>(_module_db: T) -> Self {
        Self::new()
    }

    /// Back-compat constructor.
    pub fn with_db_path(path: impl AsRef<Path>) -> Result<Self> {
        let _ = path.as_ref();
        Ok(Self::new())
    }

    /// Back-compat constructor.
    pub fn with_db_path_read_only(
        path: impl AsRef<Path>,
        error_if_log_file_exist: bool,
    ) -> Result<Self> {
        let _ = path.as_ref();
        let _ = error_if_log_file_exist;
        Ok(Self::new())
    }

    /// Register a module:
    /// - build a namespaced `Mdb` from the shared DB using `get_name()` as the prefix,
    /// - inject it via `set_mdb`,
    /// - provide a namespaced RPC registrar so the module can only register under "<name>.*".
    pub fn register_module<M>(&mut self, mut module: M)
    where
        M: EspoModule + 'static,
    {
        let name = module.get_name();
        if self.modules.is_empty() {
            if name != "essentials" {
                panic!("ModuleRegistry requires essentials to be registered first (got {name})");
            }
        } else if name == "essentials" {
            panic!("ModuleRegistry requires essentials to be registered before any other modules");
        } else if self.modules.first().map(|m| m.get_name() != "essentials").unwrap_or(true) {
            panic!("ModuleRegistry requires essentials to be registered first");
        }

        if let Some(spec) = module.config_spec() {
            let cfg = get_module_config(name).unwrap_or_else(|| {
                panic!(
                    "No config defined for {name} module, but {name} module was loaded and defines a config. Expected: {spec}"
                )
            });
            if let Err(e) = module.set_config(cfg) {
                panic!("module '{name}' config invalid; expected: {spec}; error: {e:?}");
            }
        }

        let mdb = get_espo_module_mdb(name);
        module.set_mdb(mdb);

        // --- RPC prefix like "ammdata." ---
        let ns = RpcNsRegistrar::new(self.router.clone(), name);

        let m = Arc::new(module);
        m.register_rpc(&ns);

        self.modules.push(m);
    }

    pub fn modules(&self) -> &[Arc<dyn EspoModule>] {
        &self.modules
    }
}
