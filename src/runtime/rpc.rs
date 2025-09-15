use serde_json::Value;
use std::net::SocketAddr;
use tarpc::{
    context,
    server::{BaseChannel, Channel},
};
use tokio_serde::formats::Json;

use futures::StreamExt;

use crate::modules::defs::RpcRegistry;

#[tarpc::service]
pub trait Rpc {
    async fn call(method: String, payload: Value) -> Value;
    async fn list_methods() -> Vec<String>;
}

#[derive(Clone)]
pub struct RpcServer {
    pub reg: RpcRegistry,
}

impl Rpc for RpcServer {
    async fn call(self, cx: context::Context, method: String, payload: Value) -> Value {
        self.reg.call(cx, &method, payload).await
    }

    async fn list_methods(self, _cx: context::Context) -> Vec<String> {
        self.reg.list().await
    }
}

pub async fn run_rpc(reg: RpcRegistry, addr: SocketAddr) -> anyhow::Result<()> {
    // JSON codec comes from tokio_serde::formats
    let listener = tarpc::serde_transport::tcp::listen(addr, Json::default).await?;

    let svc = RpcServer { reg };

    listener
        .filter_map(|r| async move { r.ok() })
        .map(BaseChannel::with_defaults)
        .map(|chan| {
            chan.execute(svc.clone().serve()).for_each(|resp| async move {
                tokio::spawn(resp);
            })
        })
        .buffer_unordered(usize::MAX)
        .for_each(|_| async {})
        .await;

    Ok(())
}
