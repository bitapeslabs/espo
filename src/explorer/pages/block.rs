use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::response::Html;
use bitcoin::consensus::encode::deserialize;
use bitcoin::{BlockHash, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;
use serde::Deserialize;

use crate::alkanes::trace::{
    EspoTrace, GetEspoBlockOpts, get_espo_block_with_opts, traces_for_block_as_prost,
};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_network,
};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::header::{
    HeaderPillTone, HeaderProps, HeaderSummaryItem, header, header_scripts,
};
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_arrow_up_right, icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::tx_view::render_tx;
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent,
};

fn format_with_commas(n: u64) -> String {
    let mut s = n.to_string();
    let mut i = s.len() as isize - 3;
    while i > 0 {
        s.insert(i as usize, ',');
        i -= 3;
    }
    s
}

fn format_sats_short(n: u64) -> String {
    format!("{} sats", format_with_commas(n))
}

fn mempool_block_url(network: bitcoin::Network, block_hash: &BlockHash) -> Option<String> {
    let base = match network {
        bitcoin::Network::Bitcoin => "https://mempool.space",
        bitcoin::Network::Testnet => "https://mempool.space/testnet",
        bitcoin::Network::Signet => "https://mempool.space/signet",
        bitcoin::Network::Regtest => return None,
        _ => "https://mempool.space",
    };
    Some(format!("{base}/block/{block_hash}"))
}

#[derive(Deserialize)]
pub struct BlockPageQuery {
    pub tab: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub traces: Option<String>,
}

pub async fn block_page(
    State(state): State<ExplorerState>,
    Path(height): Path<u64>,
    Query(q): Query<BlockPageQuery>,
) -> Html<String> {
    let rpc = get_bitcoind_rpc_client();
    let electrum_like = get_electrum_like();
    let network = get_network();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let nav_tip = espo_tip.min(tip);
    let espo_indexed = height <= espo_tip;
    let traces_only = q
        .traces
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(true);

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
    let block_hash_hex = block_hash.to_string();
    let block_stats = rpc.get_block_stats(height).ok();

    let _tab = q.tab.unwrap_or_else(|| "txs".to_string());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);

    let espo_block = if espo_indexed {
        let opts = if traces_only { None } else { Some(GetEspoBlockOpts { page, limit }) };
        match get_espo_block_with_opts(height, nav_tip, opts) {
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

    let mempool_url = mempool_block_url(network, &block_hash);

    let mut tx_total = 0usize;
    let mut tx_items = Vec::new();
    let mut tx_has_prev = false;
    let mut tx_has_next = false;
    let mut display_start = 0usize;
    let mut display_end = 0usize;
    let mut last_page = 1usize;
    let traces_param = if traces_only { "1" } else { "0" };
    if let Some(espo_block) = espo_block.clone() {
        if traces_only {
            let mut txs = espo_block.transactions;
            txs.retain(|t| t.traces.as_ref().map_or(false, |v| !v.is_empty()));
            tx_total = txs.len();
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1;
            tx_has_next = end < tx_total;
            tx_items = txs.into_iter().skip(off).take(limit).collect();
            if tx_total > 0 && off < tx_total {
                display_start = off + 1;
                display_end = (off + tx_items.len()).min(tx_total);
                last_page = (tx_total + limit - 1) / limit;
            }
        } else {
            tx_total = espo_block.tx_count;
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1;
            tx_has_next = end < tx_total;
            tx_items = espo_block.transactions;
            if tx_total > 0 {
                display_start = off + 1;
                display_end = (off + tx_items.len()).min(tx_total);
                last_page = (tx_total + limit - 1) / limit;
            }
        }
    }

    let outpoint_fn = |txid: &Txid, vout: u32| -> OutpointLookup {
        get_outpoint_balances_with_spent(&state.essentials_mdb, txid, vout).unwrap_or_default()
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mut dedup: Vec<Txid> = tx_items
            .iter()
            .map(|t| t.transaction.compute_txid())
            .collect();
        dedup.sort();
        dedup.dedup();
        let fetched = electrum_like.batch_transaction_get_outspends(&dedup).unwrap_or_default();
        dedup.into_iter().zip(fetched.into_iter()).collect()
    };
    let outspends_fn = move |txid: &Txid| -> Vec<Option<Txid>> {
        outspends_map.get(txid).cloned().unwrap_or_default()
    };

    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    if !tx_items.is_empty() {
        let mut prev_txids: Vec<Txid> = Vec::new();
        for atx in &tx_items {
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
    }

    let block_time: Option<u64> = hdr
        .as_ref()
        .map(|h| h.time as u64)
        .or_else(|| block_stats.as_ref().map(|s| s.time));
    let confirmations = hdr
        .as_ref()
        .and_then(|h| (h.confirmations >= 0).then_some(h.confirmations as u64))
        .or_else(|| (tip >= height).then_some(tip - height + 1));
    let traces_count: Option<usize> = if espo_indexed {
        match traces_for_block_as_prost(height) {
            Ok(v) => Some(v.len()),
            Err(e) => {
                eprintln!("[block_page] failed to fetch traces for block {height}: {e}");
                None
            }
        }
    } else {
        None
    };
    let tx_count: Option<u64> = hdr
        .as_ref()
        .map(|h| h.n_tx as u64)
        .or_else(|| block_stats.as_ref().map(|s| s.txs as u64))
        .or_else(|| espo_block.as_ref().map(|b| b.tx_count as u64));
    let avg_fee_sat: Option<u64> = block_stats.as_ref().map(|s| s.avg_fee.to_sat());

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Timestamp".to_string(),
        value: match block_time {
            Some(ts) => html! {
                div class="summary-inline" data-ts-group="" {
                    span class="summary-value" data-header-ts=(ts) { (ts) }
                    span class="summary-sub" data-header-ts-rel { "" }
                }
            },
            None => html! { span class="summary-value muted" { "Pending" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Tx count".to_string(),
        value: match tx_count {
            Some(c) => html! { span class="summary-value" { (format_with_commas(c)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Traces".to_string(),
        value: match traces_count {
            Some(t) => html! { span class="summary-value" { (format_with_commas(t as u64)) } },
            None => html! { span class="summary-value muted" { (if espo_indexed { "—" } else { "Not indexed" }) } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Avg fee".to_string(),
        value: match avg_fee_sat {
            Some(fee) => html! { span class="summary-value" { (format_sats_short(fee)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });

    let pill = confirmations
        .map(|c| (format!("{} confirmations", format_with_commas(c)), HeaderPillTone::Success))
        .or_else(|| Some(("Unconfirmed".to_string(), HeaderPillTone::Warning)));
    let header_markup = header(HeaderProps {
        title: format!("Block {}", format_with_commas(height)),
        id: Some(block_hash_hex.clone()),
        show_copy: true,
        pill,
        summary_items,
        cta: None,
    });

    layout(
        &format!("Block {height}"),
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(Some(height), espo_tip))
            }

            (header_markup)
            @if let Some(url) = mempool_url {
                div class="tx-mempool-row" {
                    a class="tx-mempool-link" href=(url) target="_blank" rel="noopener noreferrer" {
                        "view on mempool.space"
                        (icon_arrow_up_right())
                    }
                }
            }

            @if !espo_indexed {
                p class="error" { (format!("ESPO hasn't indexed this block yet (latest indexed height: {}).", espo_tip)) }
            }

            div class="card" {
                div class="row" {
                    h2 class="h2" { "Transactions" }
                    @if espo_indexed {
                        form class="trace-toggle" method="get" action=(format!("/block/{height}")) {
                            input type="hidden" name="tab" value="txs";
                            input type="hidden" name="limit" value=(limit);
                            input type="hidden" name="page" value="1";
                            input type="hidden" name="traces" value=(traces_param);
                            label class="switch" {
                                span class="switch-label" { "Only Alkanes txs" }
                                input
                                    class="switch-input"
                                    type="checkbox"
                                    checked[traces_only]
                                    onchange="this.form.traces.value = this.checked ? '1' : '0'; this.form.submit();";
                                span class="switch-slider" {}
                            }
                        }
                    }
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
                            (render_tx(&txid, &atx.transaction, traces, network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, true))
                        }
                    }
                }

                @if espo_indexed {
                    div class="pager" {
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(format!("/block/{height}?tab=txs&page=1&limit={limit}&traces={traces_param}")) aria-label="First page" {
                                (icon_skip_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                        }
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}", page - 1)) aria-label="Previous page" {
                                (icon_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                        }
                        span class="pager-meta muted" { "Showing "
                            (if tx_total > 0 { display_start } else { 0 })
                            @if tx_total > 0 {
                                "-"
                                (display_end)
                            }
                            " / "
                            (tx_total)
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}", page + 1)) aria-label="Next page" {
                                (icon_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}", last_page)) aria-label="Last page" {
                                (icon_skip_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                        }
                    }
                }
            }

            (header_scripts())
        },
    )
}
