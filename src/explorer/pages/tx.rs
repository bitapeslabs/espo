use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, State};
use axum::response::Html;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoTrace, extract_alkane_storage,
    prettyify_protobuf_trace_json, traces_for_block_as_prost,
};
use crate::config::{get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_metashrew};
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
    let tx_height_rpc: Option<u64> = rpc
        .get_raw_transaction_info(&txid, None)
        .ok()
        .and_then(|info| info.blockhash)
        .and_then(|bh| rpc.get_block_header_info(&bh).ok())
        .map(|hdr| hdr.height as u64);
    let tx_height: Option<u64> = tx_height_rpc.or_else(|| {
        electrum_like
            .transaction_get_height(&txid)
            .map_err(|e| eprintln!("[tx_page] electrum height fetch failed for {txid}: {e}"))
            .ok()
            .flatten()
    });

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

    let traces_for_tx: Option<Vec<EspoTrace>> = tx_height.and_then(|h| match fetch_traces_for_tx(h, &txid, &tx) {
        Ok(v) if !v.is_empty() => Some(v),
        Ok(_) => None,
        Err(e) => {
            eprintln!("[tx_page] failed to fetch traces for {txid}: {e}");
            None
        }
    }).or_else(|| {
        match fetch_traces_for_tx_noheight(&txid, &tx) {
            Ok(v) if !v.is_empty() => Some(v),
            Ok(_) => None,
            Err(e) => {
                eprintln!("[tx_page] failed to fetch traces (noheight) for {txid}: {e}");
                None
            }
        }
    });
    let traces_ref: Option<&[EspoTrace]> = traces_for_tx.as_ref().map(|v| v.as_slice());

    layout(
        &format!("Tx {txid}"),
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(tx_height, espo_tip))
            }
            div class="block-hero-inner" {
                div class="row" {
                    h1 class="h1" { "Transaction" }
                    span class="mono tx-page-id" { (txid.to_string()) }
                }
            }
            (render_tx(&txid, &tx, traces_ref, state.network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, false))
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
        let events: Vec<EspoSandshrewLikeTraceEvent> =
            serde_json::from_str(&events_json_str)?;

        let sandshrew_trace = EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = extract_alkane_storage(&partial.protobuf_trace, tx)?;

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace,
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint {
                txid: txid_be,
                vout,
                tx_spent: None,
            },
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
        let events: Vec<EspoSandshrewLikeTraceEvent> =
            serde_json::from_str(&events_json_str)?;

        let sandshrew_trace = EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
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
