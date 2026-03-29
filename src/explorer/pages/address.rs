use alkanes_cli_common::alkanes_pb::AlkanesTrace;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bitcoin::address::AddressType;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::alkanes::trace::{EspoSandshrewLikeTrace, EspoTrace};
use crate::config::{get_bitcoind_rpc_client, get_electrum_like};
use crate::explorer::components::alk_balances::render_alkane_balance_cards;
use crate::explorer::components::header::{HeaderProps, HeaderSummaryItem, header, header_scripts};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::{
    icon_arrow_up_right, icon_dropdown_caret, icon_left, icon_right, icon_skip_left,
    icon_skip_right,
};
use crate::explorer::components::tx_view::{
    AlkaneMetaCache, TxPill, TxPillTone, alkane_meta, icon_bg_style, render_tx,
};
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::common::fmt_sats;
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::{current_language, explorer_path};
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::storage::{
    AddressIndexListKind, AlkaneTxSummary, get_address_index_list_len,
    get_address_index_list_range, load_tx_pointer_blob_v3_by_id, load_tx_summary_v2,
};
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_balance_for_address, get_outpoint_rows_batch,
};
use crate::runtime::mempool::{MempoolEntry, pending_by_txid, pending_for_address};
use crate::runtime::state_at::StateAt;
use crate::schemas::EspoOutpoint;
use crate::utils::electrum_like::{AddressHistoryEntry, ElectrumLikeBackend};

#[derive(Deserialize)]
pub struct AddressPageQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub traces: Option<String>,
    pub cursor: Option<String>,
    pub stack: Option<String>,
}

struct AddressTxRender {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
    confirmations: Option<u64>,
    is_mempool: bool,
}

#[derive(Clone)]
struct AddressChartToken {
    alkane_id: String,
    label: String,
    asset_name: String,
    symbol: String,
    icon_url: String,
    fallback_letter: String,
}

fn format_with_commas(n: u64) -> String {
    let mut s = n.to_string();
    let mut i = s.len() as isize - 3;
    while i > 0 {
        s.insert(i as usize, ',');
        i -= 3;
    }
    s
}

fn token_fallback_letter(label: &str, fallback: &str) -> String {
    label
        .chars()
        .find(|c| c.is_ascii_alphanumeric())
        .or_else(|| fallback.chars().find(|c| c.is_ascii_alphanumeric()))
        .map(|c| c.to_ascii_uppercase().to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn address_chart_token_icon(icon_url: &str, fallback_letter: &str) -> Markup {
    html! {
        span class="alk-icon-wrap address-balance-dropdown-alk-icon" aria-hidden="true" {
            span class="alk-icon-img" style=(icon_bg_style(icon_url)) {}
            span class="alk-icon-letter" { (fallback_letter) }
        }
    }
}

fn log_address_page_perf(address: &str, step: &str, started: Instant, detail: &str) {
    eprintln!(
        "[address_page][perf] address={} step={} elapsed_ms={} {}",
        address,
        step,
        started.elapsed().as_millis(),
        detail
    );
}

fn address_type_label(address: &Address) -> Option<&'static str> {
    match address.address_type()? {
        AddressType::P2pkh => Some("P2PKH"),
        AddressType::P2sh => Some("P2SH"),
        AddressType::P2wpkh => Some("P2WPKH"),
        AddressType::P2wsh => Some("P2WSH"),
        AddressType::P2tr => Some("P2TR"),
        _ => None,
    }
}

fn mempool_address_url(network: Network, address: &str) -> Option<String> {
    let base = match network {
        Network::Bitcoin => "https://mempool.space",
        Network::Testnet => "https://mempool.space/testnet",
        Network::Signet => "https://mempool.space/signet",
        Network::Regtest => return None,
        _ => "https://mempool.space",
    };
    Some(format!("{base}/address/{address}"))
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

pub async fn address_page(
    State(state): State<ExplorerState>,
    Path(address_raw): Path<String>,
    Query(q): Query<AddressPageQuery>,
) -> Response {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(10);
    let canonical_path_fallback = "/address".to_string();

    let address = match Address::from_str(address_raw.trim())
        .ok()
        .and_then(|a| a.require_network(state.network).ok())
    {
        Some(a) => a,
        None => {
            return (
                StatusCode::NOT_FOUND,
                layout_with_meta(
                    "Address",
                    &canonical_path_fallback,
                    None,
                    html! { p class="error" { "Invalid address for this network." } },
                ),
            )
                .into_response();
        }
    };

    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
    let traces_only = q
        .traces
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(true);
    let traces_param = if traces_only { "1" } else { "0" };
    let all_range_label = if current_language().is_chinese() { "全部" } else { "All" };
    let cursor_txid = q.cursor.as_ref().and_then(|v| Txid::from_str(v).ok());
    let cursor_stack: Vec<String> = q
        .stack
        .as_deref()
        .map(|v| {
            v.split(',')
                .filter_map(|s| Txid::from_str(s).ok().map(|t| t.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let electrum_like = get_electrum_like();
    let address_str = address.to_string();
    let canonical_path = format!("/address/{address_str}");
    eprintln!(
        "[address_page][perf] address={} request_start page={} limit={} traces_only={} cursor_present={} stack_depth={}",
        address_str,
        page,
        limit,
        traces_only,
        cursor_txid.is_some(),
        cursor_stack.len()
    );
    let essentials_provider = state.essentials_provider();
    let address_stats_t0 = Instant::now();
    let address_stats = electrum_like
        .address_stats(&address)
        .map_err(|e| {
            eprintln!("[address_page] failed to fetch address stats for {address_str}: {e}");
        })
        .ok();
    log_address_page_perf(
        &address_str,
        "electrum_like.address_stats",
        address_stats_t0,
        &format!(
            "ok={} backend={:?}",
            address_stats.is_some(),
            address_stats.as_ref().map(|s| s.backend)
        ),
    );

    let balances_t0 = Instant::now();
    let balances =
        get_balance_for_address(StateAt::Latest, &state.essentials_provider(), &address_str)
            .unwrap_or_default();
    log_address_page_perf(
        &address_str,
        "essentials.get_balance_for_address",
        balances_t0,
        &format!("alkanes={}", balances.len()),
    );
    let mut balance_entries: Vec<BalanceEntry> = balances
        .into_iter()
        .map(|(alk, amt)| BalanceEntry { alkane: alk, amount: amt })
        .collect();
    balance_entries.sort_by(|a, b| {
        a.alkane.block.cmp(&b.alkane.block).then_with(|| a.alkane.tx.cmp(&b.alkane.tx))
    });
    let chart_tokens_t0 = Instant::now();
    let mut chart_meta_cache: AlkaneMetaCache = HashMap::new();
    let chart_tokens: Vec<AddressChartToken> = balance_entries
        .iter()
        .map(|entry| {
            let alkane_id = format!("{}:{}", entry.alkane.block, entry.alkane.tx);
            let meta = alkane_meta(&entry.alkane, &mut chart_meta_cache, &state.essentials_mdb);
            let label = if meta.name.known && meta.name.value != alkane_id {
                format!("{} ({})", meta.name.value, alkane_id)
            } else {
                alkane_id.clone()
            };
            AddressChartToken {
                alkane_id: alkane_id.clone(),
                label,
                asset_name: meta.name.value.clone(),
                symbol: meta.symbol.clone(),
                icon_url: meta.icon_url.clone(),
                fallback_letter: token_fallback_letter(&meta.name.value, &alkane_id),
            }
        })
        .collect();
    let default_chart_alkane = chart_tokens.first().map(|token| token.alkane_id.clone());
    log_address_page_perf(
        &address_str,
        "build_chart_tokens",
        chart_tokens_t0,
        &format!("tokens={}", chart_tokens.len()),
    );

    let chain_tip_t0 = Instant::now();
    let chain_tip = get_bitcoind_rpc_client().get_blockchain_info().ok().map(|i| i.blocks as u64);
    log_address_page_perf(
        &address_str,
        "bitcoind.get_blockchain_info",
        chain_tip_t0,
        &format!("tip={:?}", chain_tip),
    );

    let off = limit.saturating_mul(page.saturating_sub(1));
    let pending_t0 = Instant::now();
    let mut pending_entries: Vec<MempoolEntry> = pending_for_address(&address_str);
    pending_entries.sort_by(|a, b| b.txid.cmp(&a.txid));
    let pending_filtered: Vec<MempoolEntry> = pending_entries
        .into_iter()
        .filter(|e| !traces_only || e.traces.as_ref().map_or(false, |t| !t.is_empty()))
        .collect();
    let pending_total = pending_filtered.len();
    log_address_page_perf(
        &address_str,
        "mempool.pending_for_address",
        pending_t0,
        &format!("pending_filtered={}", pending_total),
    );
    let pending_set: HashSet<Txid> = pending_filtered.iter().map(|e| e.txid).collect();

    let mut tx_renders: Vec<AddressTxRender> = Vec::new();
    let mut history_error: Option<String> = None;
    let tx_has_next: bool;
    let tx_has_prev = page > 1;
    let tx_total: usize;

    let pending_slice_start = off.min(pending_total);
    let pending_slice_end = (off + limit).min(pending_total);
    for entry in pending_filtered
        .iter()
        .skip(pending_slice_start)
        .take(pending_slice_end.saturating_sub(pending_slice_start))
    {
        tx_renders.push(AddressTxRender {
            txid: entry.txid,
            tx: entry.tx.clone(),
            traces: entry.traces.clone(),
            confirmations: None,
            is_mempool: true,
        });
    }

    let remaining_slots = limit.saturating_sub(tx_renders.len());
    let confirmed_offset = off.saturating_sub(pending_total);

    let mut next_cursor: Option<Txid> = None;
    if traces_only {
        let confirmed_total_t0 = Instant::now();
        let confirmed_total = get_address_index_list_len(
            &essentials_provider,
            StateAt::Latest,
            AddressIndexListKind::AlkaneTxs,
            &address_str,
        )
        .unwrap_or(0) as usize;
        log_address_page_perf(
            &address_str,
            "essentials.get_address_index_list_len.alkane_txs",
            confirmed_total_t0,
            &format!("confirmed_total={}", confirmed_total),
        );
        let confirmed_slice_start = confirmed_offset.min(confirmed_total);
        let confirmed_slice_end = (confirmed_offset + remaining_slots).min(confirmed_total);

        if confirmed_slice_end > confirmed_slice_start {
            let range_start = confirmed_total.saturating_sub(confirmed_slice_end) as u64;
            let range_end = confirmed_total.saturating_sub(confirmed_slice_start) as u64;
            let txid_multi_get_t0 = Instant::now();
            let ids = get_address_index_list_range(
                &essentials_provider,
                StateAt::Latest,
                AddressIndexListKind::AlkaneTxs,
                &address_str,
                range_start,
                range_end,
            )
            .unwrap_or_default();
            log_address_page_perf(
                &address_str,
                "essentials.get_address_index_list_range.alkane_txs",
                txid_multi_get_t0,
                &format!("start={} end={} ids={}", range_start, range_end, ids.len()),
            );
            let mut txids: Vec<Txid> = Vec::new();
            for id in ids.into_iter().rev() {
                let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id) else {
                    continue;
                };
                txids.push(Txid::from_byte_array(blob.txid));
            }

            let raw_txs_t0 = Instant::now();
            let raw_txs = electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
            log_address_page_perf(
                &address_str,
                "electrum_like.batch_transaction_get_raw.traces_only",
                raw_txs_t0,
                &format!("txids={} raws={}", txids.len(), raw_txs.len()),
            );

            let decode_and_summary_t0 = Instant::now();
            let mut decode_failures = 0usize;
            let mut summary_hits = 0usize;
            let mut traces_hits = 0usize;

            for (idx, txid) in txids.iter().enumerate() {
                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                if raw.is_empty() {
                    continue;
                }
                let tx: Transaction = match deserialize(&raw) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("[address_page] failed to decode tx {}: {e}", txid);
                        decode_failures = decode_failures.saturating_add(1);
                        continue;
                    }
                };
                let summary = load_tx_summary_v2(&essentials_provider, txid);
                if summary.is_some() {
                    summary_hits = summary_hits.saturating_add(1);
                }
                let confirmations = summary.as_ref().and_then(|s| {
                    let h = s.height as u64;
                    chain_tip.and_then(|tip| if tip >= h { Some(tip - h + 1) } else { None })
                });
                let traces = summary
                    .as_ref()
                    .map(|s| traces_from_summary(txid, s))
                    .filter(|t| !t.is_empty());
                if traces.is_some() {
                    traces_hits = traces_hits.saturating_add(1);
                }

                tx_renders.push(AddressTxRender {
                    txid: *txid,
                    tx,
                    traces,
                    confirmations,
                    is_mempool: false,
                });
            }
            log_address_page_perf(
                &address_str,
                "decode_and_summary.traces_only",
                decode_and_summary_t0,
                &format!(
                    "txids={} rendered={} summary_hits={} traces_hits={} decode_failures={}",
                    txids.len(),
                    tx_renders.len(),
                    summary_hits,
                    traces_hits,
                    decode_failures
                ),
            );
        }

        tx_total = pending_total + confirmed_total;
        tx_has_next = (off + tx_renders.len()) < tx_total;
        log_address_page_perf(
            &address_str,
            "traces_only.pagination",
            start_time,
            &format!(
                "tx_total={} tx_renders={} has_next={}",
                tx_total,
                tx_renders.len(),
                tx_has_next
            ),
        );
    } else {
        match address_stats.as_ref().map(|s| s.backend) {
            Some(ElectrumLikeBackend::ElectrumRpc) => {
                history_error = Some(
                    "Esplora backend is required to show all address transactions.".to_string(),
                );
                tx_total = pending_total + tx_renders.len();
                tx_has_next = false;
            }
            _ => {
                let fetch_limit = remaining_slots.max(1);
                let history_page_t0 = Instant::now();
                match electrum_like.address_history_page_cursor(
                    &address,
                    cursor_txid.as_ref(),
                    fetch_limit,
                ) {
                    Ok(hist_page) => {
                        log_address_page_perf(
                            &address_str,
                            "electrum_like.address_history_page_cursor",
                            history_page_t0,
                            &format!(
                                "entries={} total={:?} has_more={} fetch_limit={}",
                                hist_page.entries.len(),
                                hist_page.total,
                                hist_page.has_more,
                                fetch_limit
                            ),
                        );
                        let entries: Vec<AddressHistoryEntry> = hist_page
                            .entries
                            .into_iter()
                            .filter(|e| !pending_set.contains(&e.txid))
                            .collect();
                        let confirmed_total =
                            hist_page.total.unwrap_or(entries.len()).max(entries.len());
                        let txids: Vec<Txid> =
                            entries.iter().take(remaining_slots).map(|h| h.txid).collect();
                        let raw_txs_t0 = Instant::now();
                        let raw_txs =
                            electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
                        log_address_page_perf(
                            &address_str,
                            "electrum_like.batch_transaction_get_raw.full_history",
                            raw_txs_t0,
                            &format!("txids={} raws={}", txids.len(), raw_txs.len()),
                        );

                        let decode_and_summary_t0 = Instant::now();
                        let mut decode_failures = 0usize;
                        let mut summary_hits = 0usize;
                        let mut traces_hits = 0usize;

                        for (idx, entry) in entries.iter().take(remaining_slots).enumerate() {
                            let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                            if raw.is_empty() {
                                continue;
                            }
                            let tx: Transaction = match deserialize(&raw) {
                                Ok(t) => t,
                                Err(e) => {
                                    eprintln!(
                                        "[address_page] failed to decode tx {}: {e}",
                                        entry.txid
                                    );
                                    decode_failures = decode_failures.saturating_add(1);
                                    continue;
                                }
                            };
                            let summary = load_tx_summary_v2(&essentials_provider, &entry.txid);
                            if summary.is_some() {
                                summary_hits = summary_hits.saturating_add(1);
                            }
                            let traces = summary
                                .as_ref()
                                .map(|s| traces_from_summary(&entry.txid, s))
                                .filter(|t| !t.is_empty());
                            if traces.is_some() {
                                traces_hits = traces_hits.saturating_add(1);
                            }
                            let confirmations = entry.height.and_then(|h| {
                                chain_tip
                                    .and_then(|tip| if tip >= h { Some(tip - h + 1) } else { None })
                            });
                            tx_renders.push(AddressTxRender {
                                txid: entry.txid,
                                tx,
                                traces,
                                confirmations,
                                is_mempool: false,
                            });
                        }
                        log_address_page_perf(
                            &address_str,
                            "decode_and_summary.full_history",
                            decode_and_summary_t0,
                            &format!(
                                "entries={} rendered={} summary_hits={} traces_hits={} decode_failures={}",
                                entries.len(),
                                tx_renders.len(),
                                summary_hits,
                                traces_hits,
                                decode_failures
                            ),
                        );

                        next_cursor = entries.last().map(|e| e.txid);
                        tx_total = pending_total + confirmed_total;
                        tx_has_next = (off + tx_renders.len()) < tx_total || hist_page.has_more;
                    }
                    Err(e) => {
                        log_address_page_perf(
                            &address_str,
                            "electrum_like.address_history_page_cursor",
                            history_page_t0,
                            &format!("error={}", e),
                        );
                        eprintln!(
                            "[address_page] failed to fetch address history for {address_str}: {e}"
                        );
                        tx_total = pending_total + tx_renders.len();
                        tx_has_next = false;
                    }
                }
            }
        }
    }

    if start_time.elapsed() > timeout {
        log_address_page_perf(
            &address_str,
            "timeout_guard",
            start_time,
            &format!("timed_out_at_ms={}", start_time.elapsed().as_millis()),
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            layout_with_meta(
                "Address",
                &canonical_path,
                None,
                html! { p class="error" { "Address has too many transactions to render quickly." } },
            ),
        )
            .into_response();
    }

    let display_start = if tx_total > 0 && off < tx_total { off + 1 } else { 0 };
    let display_end = (off + tx_renders.len()).min(tx_total);
    let last_page = if tx_total > 0 { (tx_total + limit - 1) / limit } else { 1 };
    let use_cursor = !traces_only && history_error.is_none();
    let prev_cursor = cursor_stack.last().cloned();
    let prev_stack = if cursor_stack.len() > 1 {
        cursor_stack[..cursor_stack.len() - 1].join(",")
    } else {
        String::new()
    };
    let current_cursor = cursor_txid.map(|t| t.to_string());
    let mut next_stack_vec = cursor_stack.clone();
    if let Some(cur) = current_cursor {
        next_stack_vec.push(cur);
    }
    let next_stack = if next_stack_vec.is_empty() { None } else { Some(next_stack_vec.join(",")) };
    let next_cursor_str = next_cursor.map(|t| t.to_string());
    let base_path = explorer_path(&format!("/address/{}", address_str));
    let first_href = format!("{base_path}?page=1&limit={limit}&traces={traces_param}");
    let prev_href = if use_cursor {
        let mut q = format!("page={}&limit={limit}&traces={traces_param}", page - 1);
        if let Some(prev) = prev_cursor.as_ref() {
            q.push_str(&format!("&cursor={}", prev));
        }
        if !prev_stack.is_empty() {
            q.push_str(&format!("&stack={}", prev_stack));
        }
        format!("{base_path}?{q}")
    } else {
        format!("{base_path}?page={}&limit={limit}&traces={traces_param}", page - 1)
    };
    let next_href = if use_cursor {
        let mut q = format!("page={}&limit={limit}&traces={traces_param}", page + 1);
        if let Some(next) = next_cursor_str.as_ref() {
            q.push_str(&format!("&cursor={}", next));
        }
        if let Some(stack) = next_stack.as_ref() {
            q.push_str(&format!("&stack={}", stack));
        }
        format!("{base_path}?{q}")
    } else {
        format!("{base_path}?page={}&limit={limit}&traces={traces_param}", page + 1)
    };
    let last_href = format!("{base_path}?page={last_page}&limit={limit}&traces={traces_param}");

    let mut prev_txids: Vec<Txid> = Vec::new();
    for item in &tx_renders {
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                prev_txids.push(vin.previous_output.txid);
            }
        }
    }

    prev_txids.sort();
    prev_txids.dedup();
    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    if !prev_txids.is_empty() {
        let prev_raw_t0 = Instant::now();
        let raw_prev = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
        log_address_page_perf(
            &address_str,
            "electrum_like.batch_transaction_get_raw.prev_txids",
            prev_raw_t0,
            &format!("prev_txids={} raws={}", prev_txids.len(), raw_prev.len()),
        );
        let prev_decode_t0 = Instant::now();
        let mut prev_decode_failures = 0usize;
        let mut prev_from_mempool = 0usize;
        for (i, raw) in raw_prev.into_iter().enumerate() {
            if raw.is_empty() {
                if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                    prev_map.insert(prev_txids[i], mempool_prev.tx);
                    prev_from_mempool = prev_from_mempool.saturating_add(1);
                }
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw) {
                prev_map.insert(prev_txids[i], prev_tx);
            } else if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                prev_map.insert(prev_txids[i], mempool_prev.tx);
                prev_from_mempool = prev_from_mempool.saturating_add(1);
            } else {
                prev_decode_failures = prev_decode_failures.saturating_add(1);
            }
        }
        log_address_page_perf(
            &address_str,
            "decode_prev_txs",
            prev_decode_t0,
            &format!(
                "resolved={} decode_failures={} mempool_fallbacks={}",
                prev_map.len(),
                prev_decode_failures,
                prev_from_mempool
            ),
        );
    }

    let mut all_outpoints: Vec<(Txid, u32)> = Vec::new();
    let mut input_outpoint_candidates = 0usize;
    let mut input_outpoints_selected = 0usize;
    for item in &tx_renders {
        for (vout, _) in item.tx.output.iter().enumerate() {
            all_outpoints.push((item.txid, vout as u32));
        }
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                input_outpoint_candidates = input_outpoint_candidates.saturating_add(1);
                let Some(prev_tx) = prev_map.get(&vin.previous_output.txid) else {
                    continue;
                };
                let Some(prev_out) = prev_tx.output.get(vin.previous_output.vout as usize) else {
                    continue;
                };
                let belongs_to_address =
                    Address::from_script(prev_out.script_pubkey.as_script(), state.network)
                        .ok()
                        .map(|a| a.to_string() == address_str)
                        .unwrap_or(false);
                if belongs_to_address {
                    all_outpoints.push((vin.previous_output.txid, vin.previous_output.vout));
                    input_outpoints_selected = input_outpoints_selected.saturating_add(1);
                }
            }
        }
    }
    all_outpoints.sort();
    all_outpoints.dedup();
    let outpoints_batch_t0 = Instant::now();
    let outpoint_map =
        get_outpoint_rows_batch(StateAt::Latest, &state.essentials_provider(), &all_outpoints)
            .unwrap_or_default();
    log_address_page_perf(
        &address_str,
        "essentials.get_outpoint_rows_batch",
        outpoints_batch_t0,
        &format!(
            "requested_outpoints={} returned={} input_candidates={} input_selected={}",
            all_outpoints.len(),
            outpoint_map.len(),
            input_outpoint_candidates,
            input_outpoints_selected
        ),
    );
    let outpoint_fn = move |txid: &Txid, vout: u32| -> OutpointLookup {
        outpoint_map.get(&(*txid, vout)).cloned().unwrap_or_default()
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mut dedup = tx_renders.iter().map(|t| t.txid).collect::<Vec<_>>();
        dedup.sort();
        dedup.dedup();
        let outspends_t0 = Instant::now();
        let fetched = electrum_like.batch_transaction_get_outspends(&dedup).unwrap_or_default();
        log_address_page_perf(
            &address_str,
            "electrum_like.batch_transaction_get_outspends",
            outspends_t0,
            &format!("txids={} rows={}", dedup.len(), fetched.len()),
        );
        dedup.into_iter().zip(fetched.into_iter()).collect()
    };
    let outspends_rows = outspends_map.len();
    let outspends_fn = move |txid: &Txid| -> Vec<Option<Txid>> {
        outspends_map.get(txid).cloned().unwrap_or_default()
    };

    let balances_markup_t0 = Instant::now();
    let balances_markup = if balance_entries.is_empty() {
        html! { p class="muted" { "No alkanes tracked for this address." } }
    } else {
        render_alkane_balance_cards(&balance_entries, &state.essentials_mdb)
    };
    log_address_page_perf(
        &address_str,
        "render_alkane_balance_cards",
        balances_markup_t0,
        &format!("entries={}", balance_entries.len()),
    );

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Confirmed balance".to_string(),
        value: match address_stats.as_ref().and_then(|s| s.confirmed_balance) {
            Some(sats) => html! { span class="summary-value" { (fmt_sats(sats)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Total Received".to_string(),
        value: match (
            address_stats.as_ref().and_then(|s| s.total_received),
            address_stats.as_ref().map(|s| s.backend),
        ) {
            (Some(total), _) => html! { span class="summary-value" { (fmt_sats(total)) } },
            (None, Some(ElectrumLikeBackend::ElectrumRpc)) => {
                html! { span class="summary-value muted" { "Unsupported" } }
            }
            _ => html! { span class="summary-value muted" { "Unavailable" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Confirmed UTXOs".to_string(),
        value: match address_stats.as_ref().and_then(|s| s.confirmed_utxos) {
            Some(count) => {
                html! { span class="summary-value" { (format_with_commas(count as u64)) } }
            }
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Address Type".to_string(),
        value: match address_type_label(&address) {
            Some(t) => html! { span class="summary-value" { span class="pill small" { (t) } } },
            None => html! { span class="summary-value muted" { "Unknown" } },
        },
    });

    let header_t0 = Instant::now();
    let header_markup = header(HeaderProps {
        title: "Address".to_string(),
        id: Some(address_str.clone()),
        show_copy: true,
        pill: None,
        summary_items,
        cta: None,
        hero_class: Some("tx-hero-address".to_string()),
    });
    log_address_page_perf(
        &address_str,
        "render_header",
        header_t0,
        &format!("summary_items={}", 4),
    );

    let mempool_url = mempool_address_url(state.network, &address_str);
    log_address_page_perf(
        &address_str,
        "address_page.pre_layout",
        start_time,
        &format!(
            "tx_total={} tx_renders={} prev_map={} outpoint_rows={} outspends_rows={} mempool_url={}",
            tx_total,
            tx_renders.len(),
            prev_map.len(),
            all_outpoints.len(),
            outspends_rows,
            mempool_url.is_some()
        ),
    );
    let layout_t0 = Instant::now();
    let page = layout_with_meta(
        &format!("Address {address_str}"),
        &canonical_path,
        None,
        html! {
            (header_markup)
            @if let Some(url) = mempool_url {
                div class="tx-mempool-row" {
                    a class="tx-mempool-link" href=(url) target="_blank" rel="noopener noreferrer" {
                        "view on mempool.space"
                        (icon_arrow_up_right())
                    }
                }
            }

            @if !balance_entries.is_empty() {
                h2 class="h2" { "Alkane Balances" }
                (balances_markup)
                @if let Some(default_alkane) = default_chart_alkane.as_ref() {
                    div
                        class="card address-balance-chart-card"
                        data-address-chart=""
                        data-address=(address_str.clone())
                        data-default-alkane=(default_alkane)
                        data-default-range="all"
                    {
                        div class="address-balance-chart-head" {
                            h2 class="h2" { "Balance History" }
                            div class="address-balance-chart-controls" {
                                div class="dropdown address-balance-dropdown" data-dropdown="" data-open="" data-address-chart-token="" {
                                    button
                                        class="dropdown-trigger"
                                        type="button"
                                        aria-label="Alkane"
                                        aria-haspopup="true"
                                        aria-expanded="false"
                                        data-dropdown-toggle=""
                                    {
                                        span class="dropdown-icon dropdown-trigger-icon" data-address-chart-token-trigger-icon="" {
                                            (address_chart_token_icon(
                                                &chart_tokens[0].icon_url,
                                                &chart_tokens[0].fallback_letter
                                            ))
                                        }
                                        span class="dropdown-label" data-address-chart-token-trigger-label="" {
                                            (chart_tokens[0].label.clone())
                                        }
                                        span class="dropdown-caret" { (icon_dropdown_caret()) }
                                    }
                                    div class="dropdown-panel address-balance-dropdown-panel" role="menu" aria-hidden="true" {
                                        @for token in chart_tokens.iter() {
                                            @let item_class = if token.alkane_id == chart_tokens[0].alkane_id {
                                                "dropdown-item selected"
                                            } else {
                                                "dropdown-item"
                                            };
                                            a
                                                class=(item_class)
                                                href="#"
                                                role="menuitem"
                                                data-address-chart-token-option=""
                                                data-alkane-id=(token.alkane_id.clone())
                                                data-name=(token.asset_name.clone())
                                                data-symbol=(token.symbol.clone())
                                                data-label=(token.label.clone())
                                            {
                                                span class="dropdown-icon" {
                                                    (address_chart_token_icon(
                                                        &token.icon_url,
                                                        &token.fallback_letter
                                                    ))
                                                }
                                                span class="dropdown-label" { (token.label.clone()) }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        div class="address-balance-chart-plot" data-address-chart-root {
                            div class="address-balance-chart-loading" data-address-chart-loading="" data-spinning="1" {
                                span class="spinner address-balance-chart-spinner" data-address-chart-loading-spinner="" aria-hidden="true" {}
                                span data-address-chart-loading-text="" { "Loading chart..." }
                            }
                        }
                        div class="address-balance-chart-tabs" {
                            button type="button" class="address-balance-chart-tab" data-range="1d" { "1D" }
                            button type="button" class="address-balance-chart-tab" data-range="1w" { "1W" }
                            button type="button" class="address-balance-chart-tab" data-range="1m" { "1M" }
                            button type="button" class="address-balance-chart-tab active" data-range="all" { (all_range_label) }
                        }
                    }
                }
            }

            div class="card" {
                div class="row" {
                    h2 class="h2" { "Transactions" }
                    form class="trace-toggle" method="get" action=(explorer_path(&format!("/address/{}", address_str))) {
                        input type="hidden" name="page" value="1";
                        input type="hidden" name="limit" value=(limit);
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

                @if let Some(err) = history_error.as_ref() {
                    p class="error" { (err) }
                }
                @if tx_total == 0 {
                    p class="muted" { "No transactions found." }
                } @else {
                    div class="list" {
                        @for item in tx_renders {
                            @let traces_ref: Option<&[EspoTrace]> = item.traces.as_ref().map(|v| v.as_slice());
                            @let pill = if item.is_mempool {
                                Some(TxPill { label: "Unconfirmed".to_string(), tone: TxPillTone::Danger })
                            } else if let Some(c) = item.confirmations {
                                Some(TxPill {
                                    label: format!("{} confirmations", format_with_commas(c)),
                                    tone: TxPillTone::Success,
                                })
                            } else {
                                None
                            };
                            (render_tx(&item.txid, &item.tx, traces_ref, state.network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, pill, true))
                        }
                    }

                    div class="pager" {
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(first_href) aria-label="First page" {
                                (icon_skip_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                        }
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(prev_href) aria-label="Previous page" {
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
                            a class="pill iconbtn" href=(next_href) aria-label="Next page" {
                                (icon_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                        }
                        @if tx_has_next && !use_cursor {
                            a class="pill iconbtn" href=(last_href) aria-label="Last page" {
                                (icon_skip_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                        }
                    }
                }
            }

            (header_scripts())
            (address_chart_scripts())
        },
    );
    log_address_page_perf(
        &address_str,
        "render_layout",
        layout_t0,
        &format!("total_ms={}", start_time.elapsed().as_millis()),
    );
    page.into_response()
}

fn address_chart_scripts() -> Markup {
    let script = r#"
<script>
(() => {
  const apiPath = '../api/address/chart';
  const card = document.querySelector('[data-address-chart]');
  if (!card) return;

  const address = card.dataset.address || '';
  if (!address) return;

  let activeAlkane = card.dataset.defaultAlkane || '';
  if (!activeAlkane) return;

  const root = card.querySelector('[data-address-chart-root]');
  const loadingEl = card.querySelector('[data-address-chart-loading]');
  const loadingTextEl = card.querySelector('[data-address-chart-loading-text]');
  const loadingSpinnerEl = card.querySelector('[data-address-chart-loading-spinner]');
  const dropdownEl = card.querySelector('[data-address-chart-token]');
  const optionNodes = Array.from(card.querySelectorAll('[data-address-chart-token-option]'));
  const triggerLabelEl = card.querySelector('[data-address-chart-token-trigger-label]');
  const triggerIconEl = card.querySelector('[data-address-chart-token-trigger-icon]');
  const tabs = Array.from(card.querySelectorAll('[data-range]'));
  const defaultRange = (card.dataset.defaultRange || 'all').toLowerCase();

  let activeRange = defaultRange;
  let activeName = '';
  let activeIconHtml = '';
  let chart = null;
  let canvas = null;
  let tooltipEl = null;
  let loading = false;
  let pillSmallTheme = (() => {
    const probe = document.createElement('span');
    probe.className = 'pill small';
    probe.style.position = 'absolute';
    probe.style.visibility = 'hidden';
    probe.style.pointerEvents = 'none';
    document.body.appendChild(probe);
    const styles = getComputedStyle(probe);
    const theme = {
      text: styles.color || '#aac8ff',
      bg: styles.backgroundColor || 'rgba(158, 161, 228, 0.15)'
    };
    probe.remove();
    return theme;
  })();

  const optionById = (alkaneId) => {
    if (!alkaneId) return null;
    return (
      optionNodes.find(
        (node) => ((node.dataset && node.dataset.alkaneId) || '').trim() === alkaneId
      ) || null
    );
  };

  const currentOption = () => optionById(activeAlkane) || optionNodes[0] || null;

  const syncSelectedMeta = () => {
    const option = currentOption();
    if (!option) {
      return;
    }
    const nextAlkane = ((option.dataset && option.dataset.alkaneId) || '').trim();
    if (nextAlkane) activeAlkane = nextAlkane;
    activeName = option.dataset ? (option.dataset.name || '').trim() : '';
    if (!activeName) {
      activeName = option.dataset ? (option.dataset.label || '').trim() : '';
    }
    if (!activeName) {
      activeName = activeAlkane;
    }
    const icon = option.querySelector('.dropdown-icon');
    activeIconHtml = icon ? icon.innerHTML : '';
    if (triggerLabelEl) {
      const label = (option.dataset && option.dataset.label) || option.textContent || activeAlkane;
      triggerLabelEl.textContent = (label || activeAlkane).trim();
    }
    if (triggerIconEl) {
      triggerIconEl.innerHTML = activeIconHtml;
    }
    optionNodes.forEach((node) => node.classList.toggle('selected', node === option));
  };

  const formatAmount = (value, maxDigits = 8) => {
    if (!Number.isFinite(value)) return '0';
    return new Intl.NumberFormat('en-US', {
      maximumFractionDigits: maxDigits
    }).format(value);
  };

  const formatBlock = (height) => {
    if (!Number.isFinite(height)) return 'Block';
    return `Block ${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(height)}`;
  };

  const formatTooltipValue = (value) => {
    const amount = formatAmount(value, 8);
    const tokenName = activeName || activeAlkane;
    return tokenName ? `${amount} ${tokenName}` : amount;
  };

  const setActiveTab = (range) => {
    tabs.forEach((tab) => {
      tab.classList.toggle('active', tab.dataset.range === range);
    });
  };

  const ensureScript = (src) => new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      if (existing.dataset.loaded === '1') {
        resolve();
      } else {
        existing.addEventListener('load', () => resolve(), { once: true });
        existing.addEventListener('error', () => reject(new Error('load_failed')), { once: true });
      }
      return;
    }
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.dataset.chartLib = '1';
    script.addEventListener(
      'load',
      () => {
        script.dataset.loaded = '1';
        resolve();
      },
      { once: true }
    );
    script.addEventListener('error', () => reject(new Error('load_failed')), { once: true });
    document.head.appendChild(script);
  });

  const loadChartJs = async () => {
    if (window.Chart) return;
    await ensureScript('https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js');
  };

  const ensureTooltip = () => {
    if (!root) return null;
    if (tooltipEl && tooltipEl.isConnected) return tooltipEl;
    tooltipEl = document.createElement('div');
    tooltipEl.className = 'address-balance-chart-tooltip';
    tooltipEl.innerHTML = `
      <div class="address-balance-chart-tooltip-title" data-address-chart-tooltip-title=""></div>
      <div class="address-balance-chart-tooltip-row">
        <span class="address-balance-chart-tooltip-icon" data-address-chart-tooltip-icon="" aria-hidden="true"></span>
        <span class="address-balance-chart-tooltip-value" data-address-chart-tooltip-value=""></span>
      </div>
    `;
    root.appendChild(tooltipEl);
    return tooltipEl;
  };

  const hideTooltip = () => {
    if (!tooltipEl) return;
    tooltipEl.dataset.visible = '0';
    tooltipEl.style.opacity = '0';
  };

  const renderTooltip = (context) => {
    const tooltip = context && context.tooltip ? context.tooltip : null;
    const el = ensureTooltip();
    if (!tooltip || !el) return;

    if (tooltip.opacity === 0 || !tooltip.dataPoints || tooltip.dataPoints.length === 0) {
      hideTooltip();
      return;
    }

    const dataPoint = tooltip.dataPoints[0];
    const rawHeight = Number(dataPoint ? dataPoint.label : NaN);
    const rawValue =
      dataPoint && dataPoint.parsed && typeof dataPoint.parsed.y === 'number'
        ? dataPoint.parsed.y
        : dataPoint
          ? dataPoint.parsed
          : NaN;

    const titleEl = el.querySelector('[data-address-chart-tooltip-title]');
    if (titleEl) {
      titleEl.textContent = formatBlock(rawHeight);
    }

    const valueEl = el.querySelector('[data-address-chart-tooltip-value]');
    if (valueEl) {
      valueEl.textContent = formatTooltipValue(Number(rawValue));
    }

    const iconEl = el.querySelector('[data-address-chart-tooltip-icon]');
    if (iconEl) {
      iconEl.innerHTML = activeIconHtml;
    }

    const padding = 8;
    const width = el.offsetWidth;
    const height = el.offsetHeight;
    const maxLeft = Math.max(padding, root.clientWidth - width - padding);
    const maxTop = Math.max(padding, root.clientHeight - height - padding);
    const left = Math.min(Math.max(tooltip.caretX + 12, padding), maxLeft);
    const top = Math.min(Math.max(tooltip.caretY + 12, padding), maxTop);

    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.dataset.visible = '1';
    el.style.opacity = '1';
  };

  const ensureCanvas = () => {
    if (!root) return null;
    if (!canvas) {
      canvas = document.createElement('canvas');
      canvas.setAttribute('aria-label', 'Address balance history');
      canvas.setAttribute('role', 'img');
      if (loadingEl && loadingEl.parentNode === root) {
        root.insertBefore(canvas, loadingEl);
      } else {
        root.appendChild(canvas);
      }
    }
    return canvas.getContext('2d');
  };

  const clearChart = () => {
    if (chart) {
      chart.destroy();
      chart = null;
    }
    hideTooltip();
    if (canvas) {
      canvas.remove();
      canvas = null;
    }
  };

  const setLoadingState = (message, spinning) => {
    if (!loadingEl) return;
    hideTooltip();
    if (loadingTextEl) {
      loadingTextEl.textContent = message;
    } else {
      loadingEl.textContent = message;
    }
    loadingEl.dataset.spinning = spinning ? '1' : '0';
    if (loadingSpinnerEl) {
      loadingSpinnerEl.style.display = spinning ? '' : 'none';
    }
    loadingEl.style.display = '';
  };

  const hideLoading = () => {
    if (loadingEl) loadingEl.style.display = 'none';
  };

  const renderChart = (points) => {
    if (!window.Chart) return;
    const ctx = ensureCanvas();
    if (!ctx) return;

    const lineColor = pillSmallTheme.text;
    const areaColor = pillSmallTheme.bg;
    const labels = points.map((p) => p.height);
    const values = points.map((p) => p.value);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const span = Math.max(maxValue - minValue, Math.abs(maxValue) || 1);
    const pad = span * 0.12;
    const yMin = minValue - pad;
    const yMax = maxValue + pad;

    if (chart) {
      chart.data.labels = labels;
      chart.data.datasets[0].data = values;
      chart.data.datasets[0].borderColor = lineColor;
      chart.data.datasets[0].backgroundColor = areaColor;
      chart.data.datasets[0].fill = 'start';
      chart.options.scales.y.min = yMin;
      chart.options.scales.y.max = yMax;
      chart.update('none');
      return;
    }

    chart = new window.Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            data: values,
            borderColor: lineColor,
            borderWidth: 3,
            pointRadius: 0,
            tension: 0.35,
            cubicInterpolationMode: 'monotone',
            fill: 'start',
            backgroundColor: areaColor
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: false,
            external: renderTooltip
          }
        },
        interaction: {
          mode: 'index',
          intersect: false
        },
        hover: {
          mode: 'index',
          intersect: false
        },
        scales: {
          x: { display: false },
          y: {
            display: false,
            min: yMin,
            max: yMax
          }
        }
      }
    });
  };

  const fetchRange = async (range) => {
    const params = new URLSearchParams({
      address,
      alkane: activeAlkane,
      range
    });
    const res = await fetch(`${apiPath}?${params.toString()}`, {
      headers: { Accept: 'application/json' }
    });
    const data = await res.json();
    if (!data || !data.ok) return null;
    return data;
  };

  const updateCard = (data, range, canRender) => {
    const points = Array.isArray(data && data.points) ? data.points.slice() : [];
    syncSelectedMeta();
    if (points.length === 0) {
      clearChart();
      card.removeAttribute('data-tone');
      setLoadingState('No chart data for this selection', false);
      return;
    }

    points.sort((a, b) => a.height - b.height);
    const first = Number(points[0].value);
    const last = Number(points[points.length - 1].value);
    const change = points.length > 1 && first !== 0 ? ((last - first) / Math.abs(first)) * 100 : 0;
    const isUp = change >= 0;
    card.dataset.tone = isUp ? 'up' : 'down';
    hideLoading();

    if (canRender) {
      renderChart(points);
    } else {
      clearChart();
      setLoadingState('Chart unavailable', false);
    }
  };

  const load = async (range) => {
    if (loading) return;
    loading = true;
    setLoadingState('Loading chart...', true);
    try {
      const data = await fetchRange(range);
      if (!data) {
        clearChart();
        setLoadingState('Chart unavailable', false);
        return;
      }

      let canRender = true;
      try {
        await loadChartJs();
      } catch (_) {
        canRender = false;
      }
      updateCard(data, range, canRender);
    } catch (_) {
      clearChart();
      setLoadingState('Chart unavailable', false);
    } finally {
      loading = false;
    }
  };

  optionNodes.forEach((option) => {
    option.addEventListener('click', (event) => {
      event.preventDefault();
      const selected = ((option.dataset && option.dataset.alkaneId) || '').trim();
      if (!selected || selected === activeAlkane) return;
      activeAlkane = selected;
      syncSelectedMeta();
      if (dropdownEl) {
        dropdownEl.dataset.open = '';
        const toggle = dropdownEl.querySelector('[data-dropdown-toggle]');
        const panel = dropdownEl.querySelector('.dropdown-panel');
        if (toggle) toggle.setAttribute('aria-expanded', 'false');
        if (panel) panel.setAttribute('aria-hidden', 'true');
      }
      load(activeRange);
    });
  });

  tabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      const range = (tab.dataset.range || '').toLowerCase();
      if (!range || range === activeRange) return;
      activeRange = range;
      setActiveTab(range);
      load(range);
    });
  });

  setActiveTab(activeRange);
  syncSelectedMeta();
  load(activeRange);
})();
</script>
"#;

    PreEscaped(script.to_string())
}
