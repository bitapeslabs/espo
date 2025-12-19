use axum::extract::Query;
use axum::response::Json;
use serde::Deserialize;
use serde::Serialize;

use crate::alkanes::trace::traces_for_block_as_prost;
use crate::config::{get_bitcoind_rpc_client, get_espo_next_height};
use bitcoincore_rpc::RpcApi;

#[derive(Deserialize)]
pub struct CarouselQuery {
    pub center: Option<u64>,
    pub radius: Option<u64>,
}

#[derive(Serialize)]
pub struct CarouselBlock {
    pub height: u64,
    pub traces: usize,
    pub time: Option<u32>,
}

#[derive(Serialize)]
pub struct CarouselResponse {
    pub espo_tip: u64,
    pub blocks: Vec<CarouselBlock>,
}

pub async fn carousel_blocks(Query(q): Query<CarouselQuery>) -> Json<CarouselResponse> {
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let center = q.center.unwrap_or(espo_tip).min(espo_tip);
    let radius = q.radius.unwrap_or(8).min(50); // guardrail

    let start = center.saturating_sub(radius);
    let end = (center + radius).min(espo_tip);

    let rpc = get_bitcoind_rpc_client();
    let mut blocks: Vec<CarouselBlock> = Vec::with_capacity((end - start + 1) as usize);

    for h in start..=end {
        let block_hash = match rpc.get_block_hash(h) {
            Ok(bh) => bh,
            Err(_) => continue,
        };

        let header_info = rpc.get_block_header_info(&block_hash).ok();
        let time = header_info.as_ref().map(|hi| hi.time as u32);

        let traces = traces_for_block_as_prost(h).map(|v| v.len()).unwrap_or(0);

        blocks.push(CarouselBlock { height: h, traces, time });
    }

    Json(CarouselResponse { espo_tip, blocks })
}
