use axum::extract::Query;
use axum::response::{IntoResponse, Redirect, Response};
use bitcoincore_rpc::RpcApi;
use serde::Deserialize;
use std::str::FromStr;

use crate::config::get_bitcoind_rpc_client;

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
}

pub async fn search(Query(q): Query<SearchQuery>) -> Response {
    let Some(mut query) = q.q else {
        return Redirect::to("/").into_response();
    };
    query = query.trim().to_string();
    if query.is_empty() {
        return Redirect::to("/").into_response();
    }

    if let Ok(h) = query.parse::<u64>() {
        return Redirect::to(&format!("/block/{h}")).into_response();
    }

    if query.len() == 64 && query.chars().all(|c| c.is_ascii_hexdigit()) {
        match bitcoincore_rpc::bitcoin::BlockHash::from_str(&query) {
            Ok(hash) => match get_bitcoind_rpc_client().get_block_header_info(&hash) {
                Ok(info) => {
                    return Redirect::to(&format!("/block/{}", info.height)).into_response();
                }
                Err(_) => {}
            },
            Err(_) => {}
        }
    }

    Redirect::to("/").into_response()
}
