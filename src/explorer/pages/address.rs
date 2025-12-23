use axum::extract::{Path, Query, State};
use axum::response::Html;
use bitcoin::address::AddressType;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use std::str::FromStr;

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoTrace, PartialEspoTrace,
    extract_alkane_storage, prettyify_protobuf_trace_json,
};
use crate::config::{get_bitcoind_rpc_client, get_electrum_like, get_metashrew};
use crate::explorer::components::alk_balances::render_alkane_balance_cards;
use crate::explorer::components::header::{HeaderProps, HeaderSummaryItem, header, header_scripts};
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_arrow_up_right, icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::tx_view::{TxPill, TxPillTone, render_tx};
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::common::fmt_sats;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_balance_for_address, get_outpoint_balances_with_spent,
};
use crate::modules::essentials::storage::BalanceEntry;
use crate::runtime::mempool::{MempoolEntry, pending_by_txid, pending_for_address};
use crate::utils::electrum_like::{AddressHistoryEntry, ElectrumLikeBackend};

#[derive(Deserialize)]
pub struct AddressPageQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub traces: Option<String>,
}

struct AddressTxRender {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
    confirmations: Option<u64>,
    is_mempool: bool,
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

fn build_traces_from_partials(
    txid: &Txid,
    tx: &Transaction,
    partials: &[PartialEspoTrace],
) -> Vec<EspoTrace> {
    let mut out: Vec<EspoTrace> = Vec::new();
    let tx_hex = txid.to_string();

    for partial in partials {
        let (txid_le, vout_le) = partial.outpoint.split_at(32);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();

        let trace_txid = match Txid::from_slice(&txid_be) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("[address_page] failed to parse trace txid: {e}");
                continue;
            }
        };
        if trace_txid != *txid {
            continue;
        }

        let vout = match vout_le.try_into().map(u32::from_le_bytes) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[address_page] failed to parse trace vout: {e}");
                continue;
            }
        };

        let events_json_str = match prettyify_protobuf_trace_json(&partial.protobuf_trace) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[address_page] pretty trace json failed for {tx_hex}: {e}");
                continue;
            }
        };
        let events: Vec<EspoSandshrewLikeTraceEvent> = match serde_json::from_str(&events_json_str)
        {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!("[address_page] decode trace events failed for {tx_hex}: {e}");
                continue;
            }
        };

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = match extract_alkane_storage(&partial.protobuf_trace, tx) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[address_page] extract storage failed for {tx_hex}: {e}");
                continue;
            }
        };

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace.clone(),
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    out
}

pub async fn address_page(
    State(state): State<ExplorerState>,
    Path(address_raw): Path<String>,
    Query(q): Query<AddressPageQuery>,
) -> Html<String> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(2);

    let address = match Address::from_str(address_raw.trim())
        .ok()
        .and_then(|a| a.require_network(state.network).ok())
    {
        Some(a) => a,
        None => {
            return layout(
                "Address",
                html! { p class="error" { "Invalid address for this network." } },
            );
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

    let electrum_like = get_electrum_like();
    let address_str = address.to_string();
    let address_stats = electrum_like
        .address_stats(&address)
        .map_err(|e| {
            eprintln!("[address_page] failed to fetch address stats for {address_str}: {e}");
        })
        .ok();

    let balances = get_balance_for_address(&state.essentials_mdb, &address_str).unwrap_or_default();
    let mut balance_entries: Vec<BalanceEntry> = balances
        .into_iter()
        .map(|(alk, amt)| BalanceEntry { alkane: alk, amount: amt })
        .collect();
    balance_entries.sort_by(|a, b| {
        a.alkane.block
            .cmp(&b.alkane.block)
            .then_with(|| a.alkane.tx.cmp(&b.alkane.tx))
    });

    let chain_tip = get_bitcoind_rpc_client().get_blockchain_info().ok().map(|i| i.blocks as u64);

    let off = limit.saturating_mul(page.saturating_sub(1));
    let mut traces_partials: HashMap<Txid, Vec<PartialEspoTrace>> = HashMap::new();

    let mut pending_entries: Vec<MempoolEntry> = pending_for_address(&address_str);
    pending_entries.sort_by(|a, b| b.txid.cmp(&a.txid));
    let pending_filtered: Vec<MempoolEntry> = pending_entries
        .into_iter()
        .filter(|e| !traces_only || e.traces.as_ref().map_or(false, |t| !t.is_empty()))
        .collect();
    let pending_total = pending_filtered.len();
    let pending_set: HashSet<Txid> = pending_filtered.iter().map(|e| e.txid).collect();

    let mut tx_renders: Vec<AddressTxRender> = Vec::new();
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
            traces: if traces_only { entry.traces.clone() } else { None },
            confirmations: None,
            is_mempool: true,
        });
    }

    let remaining_slots = limit.saturating_sub(tx_renders.len());
    let confirmed_offset = off.saturating_sub(pending_total);

    if traces_only {
        let mut collected: Vec<AddressHistoryEntry> = Vec::new();
        let mut skipped_traces: usize = 0;
        let mut scan_offset: usize = 0;
        let mut has_more = true;
        let target_for_count = remaining_slots.max(1);
        let page_fetch_limit = limit.saturating_mul(2).max(limit);
        let mut confirmed_seen: usize = 0;

        while has_more && collected.len() < target_for_count {
            let page_res =
                electrum_like.address_history_page(&address, scan_offset, page_fetch_limit);
            let Ok(hist_page) = page_res else {
                eprintln!("[address_page] history fetch failed for {address_str}");
                has_more = false;
                break;
            };
            if start_time.elapsed() > timeout {
                return layout(
                    "Address",
                    html! { p class="error" { "Address has too many transactions to render quickly. Please try a smaller range or disable traces." } },
                );
            }
            if hist_page.entries.is_empty() {
                has_more = false;
                break;
            }

            for entry in &hist_page.entries {
                if pending_set.contains(&entry.txid) || entry.height.is_none() {
                    continue;
                }
                let partials = match get_metashrew().traces_for_tx(&entry.txid) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("[address_page] failed traces_for_tx for {}: {e}", entry.txid);
                        Vec::new()
                    }
                };
                if partials.is_empty() {
                    continue;
                }
                confirmed_seen += 1;
                if skipped_traces < confirmed_offset {
                    skipped_traces += 1;
                    continue;
                }
                if collected.len() < remaining_slots {
                    traces_partials.insert(entry.txid, partials);
                    collected.push(entry.clone());
                }
                if collected.len() >= target_for_count {
                    break;
                }
            }

            scan_offset += hist_page.entries.len();
            has_more = hist_page.has_more;
        }

        let renderable: Vec<AddressHistoryEntry> = collected.into_iter().take(remaining_slots).collect();
        let txids: Vec<Txid> = renderable.iter().map(|h| h.txid).collect();
        let raw_txs = electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();

        for (idx, entry) in renderable.iter().enumerate() {
            let raw = raw_txs.get(idx).cloned().unwrap_or_default();
            if raw.is_empty() {
                continue;
            }
            let tx: Transaction = match deserialize(&raw) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("[address_page] failed to decode tx {}: {e}", entry.txid);
                    continue;
                }
            };
            let confirmations = entry.height.and_then(|h| {
                chain_tip.and_then(|tip| if tip >= h { Some(tip - h + 1) } else { None })
            });
            let partials = traces_partials
                .remove(&entry.txid)
                .unwrap_or_else(|| get_metashrew().traces_for_tx(&entry.txid).unwrap_or_default());
            let traces = if partials.is_empty() {
                None
            } else {
                Some(build_traces_from_partials(&entry.txid, &tx, &partials))
            };

            tx_renders.push(AddressTxRender {
                txid: entry.txid,
                tx,
                traces,
                confirmations,
                is_mempool: false,
            });
        }

        let confirmed_total_est =
            confirmed_offset + confirmed_seen + if has_more { 1 } else { 0 };
        tx_total = pending_total + confirmed_total_est;
        tx_has_next = (off + tx_renders.len()) < tx_total;
    } else {
        let fetch_limit = remaining_slots.max(1);
        match electrum_like.address_history_page(&address, confirmed_offset, fetch_limit) {
            Ok(hist_page) => {
                let entries: Vec<AddressHistoryEntry> = hist_page
                    .entries
                    .into_iter()
                    .filter(|e| !pending_set.contains(&e.txid))
                    .collect();
                let total_raw = hist_page.total.unwrap_or(confirmed_offset + entries.len());
                let confirmed_total =
                    total_raw.saturating_sub(pending_total).max(entries.len());
                let txids: Vec<Txid> =
                    entries.iter().take(remaining_slots).map(|h| h.txid).collect();
                let raw_txs = electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();

                for (idx, entry) in entries.iter().take(remaining_slots).enumerate() {
                    let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                    if raw.is_empty() {
                        continue;
                    }
                    let tx: Transaction = match deserialize(&raw) {
                        Ok(t) => t,
                        Err(e) => {
                            eprintln!("[address_page] failed to decode tx {}: {e}", entry.txid);
                            continue;
                        }
                    };
                    let confirmations = entry.height.and_then(|h| {
                        chain_tip.and_then(|tip| if tip >= h { Some(tip - h + 1) } else { None })
                    });
                    tx_renders.push(AddressTxRender {
                        txid: entry.txid,
                        tx,
                        traces: None,
                        confirmations,
                        is_mempool: false,
                    });
                }

                tx_total = pending_total + confirmed_total;
                tx_has_next = (off + tx_renders.len()) < tx_total || hist_page.has_more;
            }
            Err(e) => {
                eprintln!("[address_page] failed to fetch address history for {address_str}: {e}");
                tx_total = pending_total + tx_renders.len();
                tx_has_next = false;
            }
        }
    }

    if start_time.elapsed() > timeout {
        return layout(
            "Address",
            html! { p class="error" { "Address has too many transactions to render quickly." } },
        );
    }

    let display_start = if tx_total > 0 && off < tx_total { off + 1 } else { 0 };
    let display_end = (off + tx_renders.len()).min(tx_total);
    let last_page = if tx_total > 0 { (tx_total + limit - 1) / limit } else { 1 };

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
        let raw_prev = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
        for (i, raw) in raw_prev.into_iter().enumerate() {
            if raw.is_empty() {
                if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                    prev_map.insert(prev_txids[i], mempool_prev.tx);
                }
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw) {
                prev_map.insert(prev_txids[i], prev_tx);
            } else if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                prev_map.insert(prev_txids[i], mempool_prev.tx);
            }
        }
    }

    let outpoint_fn = |txid: &Txid, vout: u32| -> OutpointLookup {
        get_outpoint_balances_with_spent(&state.essentials_mdb, txid, vout).unwrap_or_default()
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mut dedup = tx_renders.iter().map(|t| t.txid).collect::<Vec<_>>();
        dedup.sort();
        dedup.dedup();
        let fetched = electrum_like.batch_transaction_get_outspends(&dedup).unwrap_or_default();
        dedup.into_iter().zip(fetched.into_iter()).collect()
    };
    let outspends_fn = move |txid: &Txid| -> Vec<Option<Txid>> {
        outspends_map.get(txid).cloned().unwrap_or_default()
    };

    let balances_markup = if balance_entries.is_empty() {
        html! { p class="muted" { "No alkanes tracked for this address." } }
    } else {
        render_alkane_balance_cards(&balance_entries, &state.essentials_mdb)
    };

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

    let header_markup = header(HeaderProps {
        title: "Address".to_string(),
        id: Some(address_str.clone()),
        show_copy: true,
        pill: None,
        summary_items,
        cta: None,
    });

    let mempool_url = mempool_address_url(state.network, &address_str);

    layout(
        &format!("Address {address_str}"),
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

            h2 class="h2" { "Alkane Balances" }
            (balances_markup)

            div class="card" {
                div class="row" {
                    h2 class="h2" { "Transactions" }
                    form class="trace-toggle" method="get" action=(format!("/address/{}", address_str)) {
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
                            a class="pill iconbtn" href=(format!("/address/{}?page=1&limit={limit}&traces={traces_param}", address_str)) aria-label="First page" {
                                (icon_skip_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                        }
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(format!("/address/{}?page={}&limit={limit}&traces={traces_param}", address_str, page - 1)) aria-label="Previous page" {
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
                            a class="pill iconbtn" href=(format!("/address/{}?page={}&limit={limit}&traces={traces_param}", address_str, page + 1)) aria-label="Next page" {
                                (icon_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(format!("/address/{}?page={}&limit={limit}&traces={traces_param}", address_str, last_page)) aria-label="Last page" {
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
