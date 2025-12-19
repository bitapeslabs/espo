mod api;
pub mod components;
pub mod consts;
mod pages;

use std::net::SocketAddr;

use api::carousel_blocks;
use axum::Router;
use axum::routing::get;
use pages::address::address_page;
use pages::alkane::alkane_page;
use pages::block::block_page;
use pages::home::home_page;
use pages::search::search;
use pages::state::ExplorerState;
use pages::tx::tx_page;
use tokio::net::TcpListener;

use components::layout::style;

pub fn explorer_router(state: ExplorerState) -> Router {
    Router::new()
        .route("/", get(home_page))
        .route("/search", get(search))
        .route("/block/{height}", get(block_page))
        .route("/tx/{txid}", get(tx_page))
        .route("/address/{address}", get(address_page))
        .route("/alkane/{alkane}", get(alkane_page))
        .route("/api/blocks/carousel", get(carousel_blocks))
        .route("/static/style.css", get(style))
        .with_state(state)
}

pub async fn run_explorer(addr: SocketAddr) -> anyhow::Result<()> {
    let state = ExplorerState::new();
    let app = explorer_router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
