use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, State};
use axum::response::Html;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Network, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoTrace, extract_alkane_storage,
    prettyify_protobuf_trace_json, traces_for_block_as_prost,
};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_metashrew,
};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::header::{
    HeaderCta, HeaderPillTone, HeaderProps, HeaderSummaryItem, header, header_scripts,
};
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::icon_arrow_up_right;
use crate::explorer::components::tx_view::render_tx;
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

fn format_fee_rate(rate: f64) -> String {
    let rounded = if (rate - rate.round()).abs() < 0.05 {
        format!("{:.0}", rate.round())
    } else if rate >= 10.0 {
        format!("{rate:.1}")
    } else {
        format!("{rate:.2}")
    };
    format!("{rounded} sats/vB")
}

fn mempool_tx_url(network: Network, txid: &Txid) -> Option<String> {
    let base = match network {
        Network::Bitcoin => "https://mempool.space",
        Network::Testnet => "https://mempool.space/testnet",
        Network::Signet => "https://mempool.space/signet",
        Network::Regtest => return None,
        _ => "https://mempool.space",
    };
    Some(format!("{base}/tx/{txid}"))
}

fn fee_and_rate(
    tx: &Transaction,
    prev_map: &HashMap<Txid, Transaction>,
) -> (Option<u64>, Option<f64>) {
    let mut input_total = Some(0u64);
    for vin in &tx.input {
        if vin.previous_output.is_null() {
            input_total = None;
            break;
        }
        let Some(prev_tx) = prev_map.get(&vin.previous_output.txid) else {
            input_total = None;
            break;
        };
        let Some(prev_out) = prev_tx.output.get(vin.previous_output.vout as usize) else {
            input_total = None;
            break;
        };
        input_total = input_total.and_then(|acc| acc.checked_add(prev_out.value.to_sat()));
    }

    let Some(inputs) = input_total else {
        return (None, None);
    };
    let outputs: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
    let Some(fee) = inputs.checked_sub(outputs) else {
        return (None, None);
    };
    let vbytes = tx.vsize() as u64;
    let fee_rate = if vbytes > 0 { Some(fee as f64 / vbytes as f64) } else { None };
    (Some(fee), fee_rate)
}

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
    let chain_tip = rpc.get_blockchain_info().ok().map(|i| i.blocks as u64);
    let tx_info = rpc.get_raw_transaction_info(&txid, None).ok();
    let tx_block_info = tx_info
        .as_ref()
        .and_then(|info| info.blockhash.as_ref())
        .and_then(|bh| rpc.get_block_header_info(bh).ok());
    let tx_height_rpc: Option<u64> = tx_block_info.as_ref().map(|hdr| hdr.height as u64);
    let tx_height: Option<u64> = tx_height_rpc.or_else(|| {
        electrum_like
            .transaction_get_height(&txid)
            .map_err(|e| eprintln!("[tx_page] electrum height fetch failed for {txid}: {e}"))
            .ok()
            .flatten()
    });
    let confirmations = tx_block_info
        .as_ref()
        .and_then(|hdr| (hdr.confirmations >= 0).then_some(hdr.confirmations as u64))
        .or_else(|| tx_info.as_ref().and_then(|info| info.confirmations.map(|c| c as u64)))
        .or_else(|| match (chain_tip, tx_height) {
            (Some(tip), Some(h)) if tip >= h => Some(tip - h + 1),
            _ => None,
        });
    let tx_timestamp: Option<u64> = tx_block_info
        .as_ref()
        .map(|hdr| hdr.time as u64)
        .or_else(|| tx_info.as_ref().and_then(|info| info.blocktime.map(|t| t as u64)))
        .or_else(|| tx_info.as_ref().and_then(|info| info.time.map(|t| t as u64)));
    let txid_hex = txid.to_string();

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
    let (fee_sat, fee_rate) = fee_and_rate(&tx, &prev_map);
    let mempool_url = mempool_tx_url(state.network, &txid);

    let traces_for_tx: Option<Vec<EspoTrace>> = tx_height
        .and_then(|h| match fetch_traces_for_tx(h, &txid, &tx) {
            Ok(v) if !v.is_empty() => Some(v),
            Ok(_) => None,
            Err(e) => {
                eprintln!("[tx_page] failed to fetch traces for {txid}: {e}");
                None
            }
        })
        .or_else(|| match fetch_traces_for_tx_noheight(&txid, &tx) {
            Ok(v) if !v.is_empty() => Some(v),
            Ok(_) => None,
            Err(e) => {
                eprintln!("[tx_page] failed to fetch traces (noheight) for {txid}: {e}");
                None
            }
        });
    let traces_ref: Option<&[EspoTrace]> = traces_for_tx.as_ref().map(|v| v.as_slice());

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Timestamp".to_string(),
        value: match tx_timestamp {
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
        label: "Block".to_string(),
        value: match tx_height {
            Some(h) => html! { a class="summary-value link" href=(format!("/block/{h}")) { (format_with_commas(h)) } },
            None => html! { span class="summary-value muted" { "Unconfirmed" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Fee".to_string(),
        value: match fee_sat {
            Some(fee) => html! { span class="summary-value" { (format_sats_short(fee)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Fee rate".to_string(),
        value: match fee_rate {
            Some(rate) => html! { span class="summary-value" { (format_fee_rate(rate)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });

    let pill = confirmations
        .map(|c| (format!("{} confirmations", format_with_commas(c)), HeaderPillTone::Success))
        .or_else(|| Some(("Unconfirmed".to_string(), HeaderPillTone::Warning)));
    let cta: Option<HeaderCta> = None;
    let header_markup = header(HeaderProps {
        title: "Transaction".to_string(),
        id: Some(txid_hex.clone()),
        show_copy: true,
        pill,
        summary_items,
        cta,
    });

    layout(
        &format!("Tx {txid}"),
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(tx_height, espo_tip))
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
            h2 class="h2" { "Inputs & Outputs" }
            (render_tx(&txid, &tx, traces_ref, state.network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, false))
            (header_scripts())
        },
    )
}

fn fetch_traces_for_tx(
    height: u64,
    txid: &Txid,
    tx: &Transaction,
) -> anyhow::Result<Vec<EspoTrace>> {
    let partials = traces_for_block_as_prost(height)?;
    let mut out: Vec<EspoTrace> = Vec::new();
    let tx_hex = txid.to_string();

    for partial in partials {
        let (txid_le, vout_le) = partial.outpoint.split_at(32);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();

        let trace_txid = Txid::from_slice(&txid_be)?;
        if trace_txid != *txid {
            continue;
        }

        let vout = u32::from_le_bytes(vout_le.try_into()?);
        let events_json_str = prettyify_protobuf_trace_json(&partial.protobuf_trace)?;
        let events: Vec<EspoSandshrewLikeTraceEvent> = serde_json::from_str(&events_json_str)?;

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = extract_alkane_storage(&partial.protobuf_trace, tx)?;

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace,
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    Ok(out)
}

fn fetch_traces_for_tx_noheight(txid: &Txid, tx: &Transaction) -> anyhow::Result<Vec<EspoTrace>> {
    let partials = get_metashrew().traces_for_tx(txid)?;
    let mut out: Vec<EspoTrace> = Vec::new();
    let tx_hex = txid.to_string();

    for partial in partials {
        let (txid_le, vout_le) = partial.outpoint.split_at(32);
        let mut txid_be = txid_le.to_vec();
        txid_be.reverse();

        let trace_txid = Txid::from_slice(&txid_be)?;
        if trace_txid != *txid {
            continue;
        }

        let vout = u32::from_le_bytes(vout_le.try_into()?);
        let events_json_str = prettyify_protobuf_trace_json(&partial.protobuf_trace)?;
        let events: Vec<EspoSandshrewLikeTraceEvent> = serde_json::from_str(&events_json_str)?;

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = extract_alkane_storage(&partial.protobuf_trace, tx)?;

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace,
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    Ok(out)
}
