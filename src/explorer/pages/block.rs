use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::response::Html;
use bitcoin::consensus::encode::deserialize;
use bitcoin::{Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;
use serde::Deserialize;

use crate::alkanes::trace::{EspoTrace, GetEspoBlockOpts, get_espo_block_with_opts};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_network,
};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::layout::layout;
use crate::explorer::components::tx_view::render_tx;
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent,
};

#[derive(Deserialize)]
pub struct BlockPageQuery {
    pub tab: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

pub async fn block_page(
    State(state): State<ExplorerState>,
    Path(height): Path<u64>,
    Query(q): Query<BlockPageQuery>,
) -> Html<String> {
    let rpc = get_bitcoind_rpc_client();
    let electrum_like = get_electrum_like();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let nav_tip = espo_tip.min(tip);
    let espo_indexed = height <= espo_tip;

    let block_hash = match rpc.get_block_hash(height) {
        Ok(h) => h,
        Err(e) => {
            return layout(
                "Block",
                html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
            );
        }
    };
    let hdr = rpc.get_block_header_info(&block_hash).ok();

    let _tab = q.tab.unwrap_or_else(|| "txs".to_string());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);

    let espo_block = if espo_indexed {
        let opts = GetEspoBlockOpts { page, limit };
        match get_espo_block_with_opts(height, nav_tip, Some(opts)) {
            Ok(b) => Some(b),
            Err(e) => {
                return layout(
                    "Block",
                    html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
                );
            }
        }
    } else {
        None
    };

    let outpoint_fn = |txid: &Txid, vout: u32| -> OutpointLookup {
        get_outpoint_balances_with_spent(&state.essentials_mdb, txid, vout).unwrap_or_default()
    };
    let outspends_fn = |txid: &Txid| -> Vec<Option<Txid>> {
        electrum_like.transaction_get_outspends(txid).unwrap_or_default()
    };

    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    let mut tx_total = 0usize;
    let mut tx_items = Vec::new();
    let mut tx_has_prev = false;
    let mut tx_has_next = false;

    if let Some(espo_block) = espo_block {
        // Build prevout map once for this page of txs
        let mut prev_txids: Vec<Txid> = Vec::new();
        for atx in &espo_block.transactions {
            for vin in &atx.transaction.input {
                if !vin.previous_output.is_null() {
                    prev_txids.push(vin.previous_output.txid);
                }
            }
        }
        prev_txids.sort();
        prev_txids.dedup();

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

        tx_total = espo_block.tx_count;
        tx_items = espo_block.transactions;
        let off = limit.saturating_mul(page.saturating_sub(1));
        let end = (off + limit).min(tx_total);
        tx_has_prev = page > 1;
        tx_has_next = end < tx_total;
    }

    layout(
        &format!("Block {height}"),
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(Some(height), espo_tip))

            }

            @if !espo_indexed {
                p class="error" { (format!("ESPO hasn't indexed this block yet (latest indexed height: {}).", espo_tip)) }
            }

            div class="block-hero-inner" {
                div class="row" {
                    div class="pillrow" {
                        h1 class="h1" { "Block " (height) }

                    }
                }
            }

            div class="card" {
                dl class="kv" {
                    dt { "Hash" } dd class="mono" { (block_hash.to_string()) }
                    @if let Some(h) = &hdr {
                        dt { "Confirmations" } dd { (h.confirmations) }
                        dt { "Time" } dd class="mono" { (h.time) }
                        dt { "Txs" } dd { (h.n_tx) }
                    }
                }
            }

            div class="card" {
                div class="row" {
                    h2 class="h2" { "Transactions" }

                }

                @if !espo_indexed {
                    p class="muted" { "Transactions will appear once ESPO indexes this block." }
                } @else if tx_total == 0 {
                    p class="muted" { "No transactions found." }
                } @else {
                    div class="list" {
                        @for atx in tx_items {
                            @let txid = atx.transaction.compute_txid();
                            @let traces: Option<&[EspoTrace]> = atx.traces.as_ref().map(|v| v.as_slice());
                            (render_tx(&txid, &atx.transaction, traces, get_network(), &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb))
                        }
                    }
                }

                @if espo_indexed {
                    div class="pillrow" {
                        @if tx_has_prev {
                            a class="pill" href=(format!("/block/{height}?tab=txs&page={}&limit={limit}", page - 1)) { "Prev" }
                        } @else {
                            span class="pill disabled" { "Prev" }
                        }
                        @if tx_has_next {
                            a class="pill" href=(format!("/block/{height}?tab=txs&page={}&limit={limit}", page + 1)) { "Next" }
                        } @else {
                            span class="pill disabled" { "Next" }
                        }
                        span class="muted" { "Page " (page) " • " (tx_total) " txs" }
                    }
                }
            }
        },
    )
}
