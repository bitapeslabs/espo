use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use rocksdb::{DB, Options};
use serde::Deserialize;

use crate::utils::{cli::get_config, trace};

#[derive(Clone)]
struct AppState {
    db: Arc<DB>,
}

#[derive(Deserialize)]
struct BlockPath {
    block: u64,
}

pub async fn run() -> Result<()> {
    let cfg = get_config()?;
    let addr: SocketAddr = ([0, 0, 0, 0], cfg.port).into();

    let opts = Options::default();
    let db = Arc::new(DB::open_for_read_only(&opts, &cfg.metashrew_db_path, true)?);

    let state = AppState { db };

    // API mounted under the configurable endpoint, e.g. "/traces"
    let traces_api = Router::new()
        .route("/{block}", get(get_traces)) // <-- use {block}
        .with_state(state.clone());

    let app = Router::new()
        .route("/healthz", get(health))
        .nest("/traces", traces_api);

    println!(
        "Listening on http://{}  (GET {}/{{block}})",
        addr, "/traces"
    );
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn get_traces(
    State(state): State<AppState>,
    Path(BlockPath { block }): Path<BlockPath>,
) -> impl IntoResponse {
    let db = state.db.clone();
    let body_res: Result<String> =
        tokio::task::spawn_blocking(move || trace::traces_for_block(&db, block))
            .await
            .map_err(|e| anyhow::anyhow!("join error: {e}"))
            .and_then(|r| r);

    match body_res {
        Ok(json) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            json,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error: {e:#}")).into_response(),
    }
}

async fn health() -> &'static str {
    "ok"
}
