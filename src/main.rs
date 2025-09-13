pub mod consts;
pub mod runtime;
pub mod types;
pub mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    runtime::server::run().await
}
