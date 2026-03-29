use std::collections::HashMap;
use std::str::FromStr;

use alkanes_cli_common::alkanes_pb::AlkanesTrace;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;
use serde::Deserialize;

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId, EspoTrace,
    GetEspoBlockOpts, get_espo_block_with_opts, traces_for_block_as_prost,
};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_network,
};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::header::{
    HeaderPillTone, HeaderProps, HeaderSummaryItem, header, header_scripts,
};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::{
    icon_arrow_up_right, icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::tx_view::{TxPill, TxPillTone, render_tx};
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::{
    AddressIndexListKind, AlkaneTxSummary, address_index_list_id_alkane_block_txs,
    get_address_index_list_len, get_address_index_list_range, load_tx_pointer_blob_v3_by_id,
    load_tx_summary_v2,
};
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent_batch,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::EspoOutpoint;

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
    pub hide_diesel_mints: Option<String>,
}

struct BlockTxItem {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
}

fn traces_from_summary(txid: &Txid, summary: &AlkaneTxSummary) -> Vec<EspoTrace> {
    summary
        .traces
        .iter()
        .filter_map(|trace| sandshrew_to_espo_trace(txid, trace))
        .collect()
}

fn sandshrew_to_espo_trace(txid: &Txid, trace: &EspoSandshrewLikeTrace) -> Option<EspoTrace> {
    let (txid_hex, vout_s) = trace.outpoint.split_once(':')?;
    let vout = vout_s.parse::<u32>().ok()?;
    let trace_txid = Txid::from_str(txid_hex).unwrap_or(*txid);
    Some(EspoTrace {
        sandshrew_trace: trace.clone(),
        protobuf_trace: AlkanesTrace::default(),
        storage_changes: HashMap::new(),
        outpoint: EspoOutpoint { txid: trace_txid.to_byte_array().to_vec(), vout, tx_spent: None },
    })
}

fn parse_u128_from_str(s: &str) -> Option<u128> {
    if let Some(hex) = s.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u128>().ok()
    }
}

fn parse_u32_or_hex(s: &str) -> Option<u32> {
    if let Some(hex) = s.strip_prefix("0x") {
        u32::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u32>().ok()
    }
}

fn parse_u64_or_hex(s: &str) -> Option<u64> {
    if let Some(hex) = s.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u64>().ok()
    }
}

fn parse_short_id_to_parts(id: &EspoSandshrewLikeTraceShortId) -> Option<(u32, u64)> {
    Some((parse_u32_or_hex(&id.block)?, parse_u64_or_hex(&id.tx)?))
}

fn is_diesel_mint_trace(trace: &EspoTrace) -> bool {
    let mut invoke_count = 0usize;
    let mut contract_id: Option<(u32, u64)> = None;
    let mut inputs: Option<&[String]> = None;

    for ev in &trace.sandshrew_trace.events {
        if let EspoSandshrewLikeTraceEvent::Invoke(data) = ev {
            invoke_count += 1;
            if invoke_count > 1 {
                return false;
            }
            contract_id = parse_short_id_to_parts(&data.context.myself);
            inputs = Some(&data.context.inputs);
        }
    }

    if invoke_count != 1 {
        return false;
    }
    let Some((block, tx)) = contract_id else {
        return false;
    };
    if block != 2 || tx != 0 {
        return false;
    }
    let Some(inputs) = inputs else {
        return false;
    };
    if inputs.is_empty() {
        return false;
    }
    let mut iter = inputs.iter();
    let opcode = iter.next().and_then(|s| parse_u128_from_str(s)).unwrap_or_default();
    if opcode != 77 {
        return false;
    }
    iter.all(|s| parse_u128_from_str(s).map_or(false, |v| v == 0))
}

fn is_diesel_mint_tx(traces: Option<&[EspoTrace]>) -> bool {
    let Some(traces) = traces else {
        return false;
    };
    if traces.len() != 1 {
        return false;
    }
    is_diesel_mint_trace(&traces[0])
}

pub async fn block_page(
    State(state): State<ExplorerState>,
    Path(height): Path<u64>,
    Query(q): Query<BlockPageQuery>,
) -> Response {
    let rpc = get_bitcoind_rpc_client();
    let electrum_like = get_electrum_like();
    let network = get_network();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let nav_tip = espo_tip.min(tip);
    let espo_indexed = height <= espo_tip;
    let essentials_provider = state.essentials_provider();
    let traces_only = q
        .traces
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(true);
    let hide_diesel_mints = q
        .hide_diesel_mints
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(false);
    let canonical_path = format!("/block/{height}");

    let block_hash = match rpc.get_block_hash(height) {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                layout_with_meta(
                    "Block",
                    &canonical_path,
                    None,
                    html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
                ),
            )
                .into_response();
        }
    };
    let hdr = rpc.get_block_header_info(&block_hash).ok();
    let block_hash_hex = block_hash.to_string();
    let block_stats = rpc.get_block_stats(height).ok();

    let _tab = q.tab.unwrap_or_else(|| "txs".to_string());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);

    let espo_block = if espo_indexed && !traces_only {
        match get_espo_block_with_opts(height, nav_tip, Some(GetEspoBlockOpts { page, limit })) {
            Ok(b) => Some(b),
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    layout_with_meta(
                        "Block",
                        &canonical_path,
                        None,
                        html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
                    ),
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    let mempool_url = mempool_block_url(network, &block_hash);

    let mut tx_total = 0usize;
    let mut tx_items: Vec<BlockTxItem> = Vec::new();
    let mut tx_has_prev = false;
    let mut tx_has_next = false;
    let mut display_start = 0usize;
    let mut display_end = 0usize;
    let mut last_page = 1usize;
    let traces_param = if traces_only { "1" } else { "0" };
    let hide_diesel_param = if hide_diesel_mints { "1" } else { "0" };

    if espo_indexed {
        if traces_only {
            let list_id = address_index_list_id_alkane_block_txs(height);
            let total = get_address_index_list_len(
                &essentials_provider,
                StateAt::Latest,
                AddressIndexListKind::AlkaneBlockTxs,
                &list_id,
            )
            .unwrap_or(0) as usize;
            if hide_diesel_mints {
                let mut all_txids: Vec<Txid> = Vec::new();
                if total > 0 {
                    let ids = get_address_index_list_range(
                        &essentials_provider,
                        StateAt::Latest,
                        AddressIndexListKind::AlkaneBlockTxs,
                        &list_id,
                        0,
                        total as u64,
                    )
                    .unwrap_or_default();
                    for id in ids {
                        let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id)
                        else {
                            continue;
                        };
                        all_txids.push(Txid::from_byte_array(blob.txid));
                    }
                }

                let mut summary_map: HashMap<Txid, Option<AlkaneTxSummary>> = HashMap::new();
                let mut filtered_txids: Vec<Txid> = Vec::new();
                for txid in &all_txids {
                    let summary = load_tx_summary_v2(&essentials_provider, txid);
                    let traces = summary
                        .as_ref()
                        .map(|s| traces_from_summary(txid, s))
                        .filter(|t| !t.is_empty());
                    if !is_diesel_mint_tx(traces.as_deref()) {
                        filtered_txids.push(*txid);
                    }
                    summary_map.insert(*txid, summary);
                }

                tx_total = filtered_txids.len();
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(tx_total);
                tx_has_prev = page > 1 && off < tx_total;
                tx_has_next = end < tx_total;
                if tx_total > 0 && off < tx_total {
                    display_start = off + 1;
                    display_end = (off + end.saturating_sub(off)).min(tx_total);
                    last_page = (tx_total + limit - 1) / limit;
                }

                if end > off {
                    let page_txids: Vec<Txid> = filtered_txids[off..end].to_vec();
                    let raw_txs =
                        electrum_like.batch_transaction_get_raw(&page_txids).unwrap_or_default();
                    for (idx, txid) in page_txids.iter().enumerate() {
                        let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                        if raw.is_empty() {
                            continue;
                        }
                        let Ok(tx) = deserialize::<Transaction>(&raw) else {
                            continue;
                        };
                        let summary = summary_map.get(txid).cloned().unwrap_or(None);
                        let traces = summary
                            .as_ref()
                            .map(|s| traces_from_summary(txid, s))
                            .filter(|t| !t.is_empty());
                        tx_items.push(BlockTxItem { txid: *txid, tx, traces });
                    }
                    if tx_total > 0 && off < tx_total {
                        display_end = (off + tx_items.len()).min(tx_total);
                    }
                }
            } else {
                tx_total = total;
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(tx_total);
                tx_has_prev = page > 1;
                tx_has_next = end < tx_total;
                if tx_total > 0 && off < tx_total {
                    display_start = off + 1;
                    display_end = (off + end.saturating_sub(off)).min(tx_total);
                    last_page = (tx_total + limit - 1) / limit;
                }

                if end > off {
                    let mut txids: Vec<Txid> = Vec::new();
                    let ids = get_address_index_list_range(
                        &essentials_provider,
                        StateAt::Latest,
                        AddressIndexListKind::AlkaneBlockTxs,
                        &list_id,
                        off as u64,
                        end as u64,
                    )
                    .unwrap_or_default();
                    for id in ids {
                        let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id)
                        else {
                            continue;
                        };
                        txids.push(Txid::from_byte_array(blob.txid));
                    }

                    let raw_txs =
                        electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
                    for (idx, txid) in txids.iter().enumerate() {
                        let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                        if raw.is_empty() {
                            continue;
                        }
                        let Ok(tx) = deserialize::<Transaction>(&raw) else {
                            continue;
                        };
                        let summary = load_tx_summary_v2(&essentials_provider, txid);
                        let traces = summary
                            .as_ref()
                            .map(|s| traces_from_summary(txid, s))
                            .filter(|t| !t.is_empty());
                        tx_items.push(BlockTxItem { txid: *txid, tx, traces });
                    }
                    if tx_total > 0 && off < tx_total {
                        display_end = (off + tx_items.len()).min(tx_total);
                    }
                }
            }
        } else if let Some(espo_block) = espo_block.clone() {
            tx_total = espo_block.tx_count;
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1;
            tx_has_next = end < tx_total;
            tx_items = espo_block
                .transactions
                .into_iter()
                .map(|atx| BlockTxItem {
                    txid: atx.transaction.compute_txid(),
                    tx: atx.transaction,
                    traces: atx.traces,
                })
                .collect();
            if hide_diesel_mints {
                tx_items.retain(|item| !is_diesel_mint_tx(item.traces.as_deref()));
            }
            if tx_total > 0 {
                display_start = off + 1;
                display_end = (off + tx_items.len()).min(tx_total);
                last_page = (tx_total + limit - 1) / limit;
            }
        }
    }

    let mut all_outpoints: Vec<(Txid, u32)> = Vec::new();
    for item in &tx_items {
        for (vout, _) in item.tx.output.iter().enumerate() {
            all_outpoints.push((item.txid, vout as u32));
        }
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                all_outpoints.push((vin.previous_output.txid, vin.previous_output.vout));
            }
        }
    }
    all_outpoints.sort();
    all_outpoints.dedup();
    let outpoint_map = get_outpoint_balances_with_spent_batch(
        StateAt::Latest,
        &state.essentials_provider(),
        &all_outpoints,
    )
    .unwrap_or_default();
    let outpoint_fn = move |txid: &Txid, vout: u32| -> OutpointLookup {
        outpoint_map.get(&(*txid, vout)).cloned().unwrap_or_default()
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mut dedup: Vec<Txid> = tx_items.iter().map(|t| t.txid).collect();
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
        for item in &tx_items {
            for vin in &item.tx.input {
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
        hero_class: None,
    });

    layout_with_meta(
        &format!("Block {height}"),
        &canonical_path,
        None,
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
                        form class="trace-toggle" method="get" action=(explorer_path(&format!("/block/{height}"))) {
                            input type="hidden" name="tab" value="txs";
                            input type="hidden" name="limit" value=(limit);
                            input type="hidden" name="page" value="1";
                            input type="hidden" name="traces" value=(traces_param);
                            input type="hidden" name="hide_diesel_mints" value=(hide_diesel_param);
                            label class="switch" {
                                span class="switch-label" { "Only Alkanes txs" }
                                input
                                    class="switch-input"
                                    type="checkbox"
                                    checked[traces_only]
                                    onchange="this.form.traces.value = this.checked ? '1' : '0'; this.form.submit();";
                                span class="switch-slider" {}
                            }
                            label class="switch" {
                                span class="switch-label" { "Hide Diesel mints" }
                                input
                                    class="switch-input"
                                    type="checkbox"
                                    checked[hide_diesel_mints]
                                    onchange="this.form.hide_diesel_mints.value = this.checked ? '1' : '0'; this.form.submit();";
                                span class="switch-slider" {}
                            }
                        }
                    }
                }

                @if !espo_indexed {
                    p class="muted" { "Transactions will appear once ESPO indexes this block." }
                } @else if tx_total == 0 {
                    p class="muted" { "No transactions found." }
                } @else if tx_items.is_empty() {
                    p class="muted" { "No transactions match the current filters." }
                } @else {
                    @let block_confirmations = tip.saturating_sub(height).saturating_add(1);
                    @let base_pill = TxPill {
                        label: format!("{} confirmations", format_with_commas(block_confirmations)),
                        tone: TxPillTone::Success,
                    };
                    div class="list" {
                        @for item in tx_items {
                            @let traces: Option<&[EspoTrace]> = item.traces.as_ref().map(|v| v.as_slice());
                            (render_tx(&item.txid, &item.tx, traces, network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, Some(base_pill.clone()), true))
                        }
                    }
                }

                @if espo_indexed {
                    div class="pager" {
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&traces={traces_param}&hide_diesel_mints={hide_diesel_param}"))) aria-label="First page" {
                                (icon_skip_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                        }
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}&hide_diesel_mints={hide_diesel_param}", page - 1))) aria-label="Previous page" {
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
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}&hide_diesel_mints={hide_diesel_param}", page + 1))) aria-label="Next page" {
                                (icon_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&traces={traces_param}&hide_diesel_mints={hide_diesel_param}", last_page))) aria-label="Last page" {
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
    .into_response()
}
