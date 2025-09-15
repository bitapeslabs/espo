use anyhow::Result;
use bitcoin::Network;
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tarpc::context;
use tokio::sync::RwLock;

use crate::alkanes::trace::EspoBlock;

/// Object-safe handler: (Context, JSON) -> JSON (async)
type HandlerFn = dyn Fn(context::Context, Value) -> BoxFuture<'static, Value> + Send + Sync;

/// Shared registry of RPC handlers (namespaced strings -> handler)
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

/// Object-safe module interface (storable as dyn)
pub trait EspoModule: Send + Sync {
    // For RocksDB prefixing and namespacing RPC methods
    fn get_name(&self) -> &'static str;

    // A module can “start” at some later block (e.g., when the contract went live)
    fn get_genesis_block(&self, network: Network) -> u32;

    // Indexing API
    fn index_block(&self, block: EspoBlock) -> Result<()>;
    fn get_index_height(&self) -> Result<u64>;

    // Register this module’s RPCs (e.g., "<name>_getcandles", etc.)
    fn register_rpc(&self, reg: &RpcRegistry);
}

/// Simple registry that holds modules and the RPC router
pub struct ModuleRegistry {
    modules: Vec<Arc<dyn EspoModule>>,
    pub router: RpcRegistry,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self { modules: Vec::new(), router: RpcRegistry::default() }
    }

    pub fn register_module<M>(&mut self, module: M)
    where
        M: EspoModule + 'static,
    {
        let m = Arc::new(module);
        // Allow module to register its RPC handlers into the single router:
        m.register_rpc(&self.router);
        self.modules.push(m);
    }

    /// Optional: expose modules if you want to drive indexing externally
    pub fn modules(&self) -> &[Arc<dyn EspoModule>] {
        &self.modules
    }
}
