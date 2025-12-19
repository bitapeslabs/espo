use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, State};
use axum::response::Html;
use bitcoin::consensus::encode::deserialize;
use bitcoin::{Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;

use crate::config::{get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::layout::layout;
use crate::explorer::components::tx_view::render_tx;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent,
};

pub async fn tx_page(
    State(state): State<ExplorerState>,
    Path(txid_str): Path<String>,
) -> Html<String> {
    let txid = match Txid::from_str(&txid_str) {
        Ok(t) => t,
        Err(_) => return layout("Transaction", html! { p class="error" { "Invalid txid." } }),
    };

    let electrum_like = get_electrum_like();
    let raw = match electrum_like.transaction_get_raw(&txid) {
        Ok(b) => b,
        Err(e) => {
            return layout(
                "Transaction",
                html! { p class="error" { (format!("Failed to fetch raw tx: {e:?}")) } },
            );
        }
    };
    let tx: Transaction = match deserialize(&raw) {
        Ok(t) => t,
        Err(e) => {
            return layout(
                "Transaction",
                html! { p class="error" { (format!("Failed to decode tx: {e:?}")) } },
            );
        }
    };

    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let rpc = get_bitcoind_rpc_client();
    let tx_height: Option<u64> = rpc
        .get_raw_transaction_info(&txid, None)
        .ok()
        .and_then(|info| info.blockhash)
        .and_then(|bh| rpc.get_block_header_info(&bh).ok())
        .map(|hdr| hdr.height as u64);

    // Prevouts (best-effort): batch fetch unique txids.
    let mut prev_txids: Vec<Txid> = tx
        .input
        .iter()
        .filter_map(|vin| (!vin.previous_output.is_null()).then_some(vin.previous_output.txid))
        .collect();
    prev_txids.sort();
    prev_txids.dedup();

    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    if !prev_txids.is_empty() {
        let raws = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
        for (i, raw_prev) in raws.into_iter().enumerate() {
            if raw_prev.is_empty() {
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                prev_map.insert(prev_txids[i], prev_tx);
            }
        }
    }

    let outpoint_fn = |txid: &Txid, vout: u32| -> OutpointLookup {
        get_outpoint_balances_with_spent(&state.essentials_mdb, txid, vout).unwrap_or_default()
    };
    let outspends_fn = |txid: &Txid| -> Vec<Option<Txid>> {
        electrum_like.transaction_get_outspends(txid).unwrap_or_default()
    };

    layout(
        &format!("Tx {txid}"),
        html! {

            (block_carousel(tx_height, espo_tip))
            div class="row" {
                h1 class="h1" { "Transaction" }
                span class="mono" { (txid.to_string()) }
            }
            (render_tx(&txid, &tx, None, state.network, &prev_map, &outpoint_fn, &outspends_fn))
        },
    )
}
