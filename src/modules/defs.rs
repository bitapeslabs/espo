use anyhow::Result;
use bitcoin::Network;
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::HashMap, path::Path, sync::Arc};
use tarpc::context;
use tokio::sync::RwLock;

use crate::alkanes::trace::EspoBlock;
use crate::runtime::mdb::Mdb;
use rocksdb::{DB, Options};

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

    /// Modules can only register RPCs via a namespaced registrar.
    /// For a module named "ammdata", all methods will be "ammdata.<suffix>".
    fn register_rpc(&self, reg: &RpcNsRegistrar);
}

/// Registry that holds modules, the RPC router, and one shared RocksDB
pub struct ModuleRegistry {
    modules: Vec<Arc<dyn EspoModule>>,
    pub router: RpcRegistry,
    module_db: Arc<DB>,
}

impl ModuleRegistry {
    /// Construct from an existing Arc<DB> (one global DB shared by all modules).
    pub fn with_db(module_db: Arc<DB>) -> Self {
        Self { modules: Vec::new(), router: RpcRegistry::default(), module_db }
    }

    /// Convenience: open a global read-write DB at a path, create if missing.
    pub fn with_db_path(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, path)?);
        Ok(Self::with_db(db))
    }

    /// Convenience: open a global read-only DB at a path.
    pub fn with_db_path_read_only(
        path: impl AsRef<Path>,
        error_if_log_file_exist: bool,
    ) -> Result<Self> {
        let opts = Options::default();
        let db = Arc::new(DB::open_for_read_only(&opts, path, error_if_log_file_exist)?);
        Ok(Self::with_db(db))
    }

    /// Register a module:
    /// - build a namespaced `Mdb` from the shared DB using `get_name()` as the prefix,
    /// - inject it via `set_mdb`,
    /// - provide a namespaced RPC registrar so the module can only register under "<name>.*".
    pub fn register_module<M>(&mut self, mut module: M)
    where
        M: EspoModule + 'static,
    {
        // --- Mdb prefix like "ammdata:" ---
        let mut prefix_kv = Vec::with_capacity(module.get_name().len() + 1);
        prefix_kv.extend_from_slice(module.get_name().as_bytes());
        prefix_kv.push(b':');

        let mdb = Arc::new(Mdb::from_db(self.module_db.clone(), prefix_kv));
        module.set_mdb(mdb);

        // --- RPC prefix like "ammdata." ---
        let ns = RpcNsRegistrar::new(self.router.clone(), module.get_name());

        let m = Arc::new(module);
        m.register_rpc(&ns);

        self.modules.push(m);
    }

    pub fn modules(&self) -> &[Arc<dyn EspoModule>] {
        &self.modules
    }
}
