use crate::runtime::state_at::StateAt;
use alkanes_cli_common::alkanes_pb::AlkanesTrace;
use axum::extract::State;
use axum::response::Html;
use bitcoin::Txid;
use bitcoin::hashes::Hash;
use bitcoincore_rpc::RpcApi;
use borsh::BorshDeserialize;
use maud::{Markup, html};
use std::collections::HashMap;
use std::str::FromStr;

use crate::alkanes::trace::{EspoSandshrewLikeTrace, EspoTrace};
use crate::config::{get_bitcoind_rpc_client, get_espo_next_height};
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::icon_right;
use crate::explorer::components::table::{AlkaneTableRow, alkanes_table};
use crate::explorer::components::tx_view::{alkane_icon_url, render_trace_summaries};
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::{
    AlkaneTxSummary, EssentialsProvider, EssentialsTable, GetHoldersOrderedPageParams,
    HoldersCountEntry, load_creation_record, load_tx_summary_v2,
};
use crate::schemas::EspoOutpoint;
use std::sync::Arc;

struct AlkaneTxRow {
    txid: Txid,
    trace: EspoTrace,
}

fn load_top_alkanes_by_holders(
    mdb: &crate::runtime::mdb::Mdb,
    limit: usize,
) -> Vec<AlkaneTableRow> {
    let mut rows: Vec<AlkaneTableRow> = Vec::new();
    if limit == 0 {
        return rows;
    }

    let table = EssentialsTable::new(mdb);
    let provider = EssentialsProvider::new(Arc::new(mdb.clone()));
    let ids = provider
        .get_holders_ordered_page(GetHoldersOrderedPageParams {
            blockhash: StateAt::Latest,
            offset: 0,
            limit: limit as u64,
            desc: true,
        })
        .map(|res| res.ids)
        .unwrap_or_default();
    for alk in ids {
        if rows.len() >= limit {
            break;
        }
        let Some(rec) = load_creation_record(mdb, &alk).ok().flatten() else { continue };
        let holders = mdb
            .get(&table.holders_count_key(&alk))
            .ok()
            .flatten()
            .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
            .map(|hc| hc.count)
            .unwrap_or(0);

        let id = format!("{}:{}", rec.alkane.block, rec.alkane.tx);
        let name = rec
            .names
            .first()
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Unnamed".to_string());
        let icon_url = alkane_icon_url(&rec.alkane, mdb);
        let fallback = if name == "Unnamed" {
            '?'
        } else {
            name.chars()
                .find(|c| !c.is_whitespace())
                .map(|c| c.to_ascii_uppercase())
                .unwrap_or('?')
        };
        let creation_txid = hex::encode(rec.txid);

        rows.push(AlkaneTableRow {
            id,
            name,
            holders,
            icon_url,
            fallback,
            creation_height: rec.creation_height,
            creation_txid,
        });
    }

    rows
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

fn load_latest_alkane_txs(mdb: &crate::runtime::mdb::Mdb, limit: usize) -> Vec<AlkaneTxRow> {
    let mut out: Vec<AlkaneTxRow> = Vec::new();
    if limit == 0 {
        return out;
    }

    let table = EssentialsTable::new(mdb);
    let mut txid_vals: Vec<Option<Vec<u8>>> = Vec::new();

    // Newer layout: /alkane_latest_traces/v2/{length,idx}
    let len = mdb
        .get(&table.latest_traces_length_key())
        .ok()
        .flatten()
        .and_then(|b| {
            if b.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&b);
                Some(u32::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);
    if len > 0 {
        let mut keys = Vec::with_capacity(len as usize);
        for idx in 0..len {
            keys.push(table.latest_traces_idx_key(idx));
        }
        txid_vals = mdb.multi_get(&keys).unwrap_or_default();
    }

    if txid_vals.is_empty() {
        return out;
    }

    let provider = EssentialsProvider::new(Arc::new(mdb.clone()));

    for v in txid_vals {
        if out.len() >= limit {
            break;
        }
        let Some(txid_bytes) = v else { continue };
        let Ok(txid) = Txid::from_slice(&txid_bytes) else { continue };
        let summary = load_tx_summary_v2(&provider, &txid);
        let Some(summary) = summary else { continue };
        let mut traces = traces_from_summary(&txid, &summary);
        if traces.is_empty() {
            continue;
        }
        let trace = traces.remove(0);
        out.push(AlkaneTxRow { txid, trace });
    }

    out
}

pub async fn home_page(State(state): State<ExplorerState>) -> Html<String> {
    let rpc = get_bitcoind_rpc_client();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let latest_height = espo_tip.min(tip);
    let top_alkanes = load_top_alkanes_by_holders(&state.essentials_mdb, 10);
    let latest_alkane_txs = load_latest_alkane_txs(&state.essentials_mdb, 4);
    let latest_block_link = explorer_path(&format!("/block/{espo_tip}?traces=1"));
    let alkanes_link = explorer_path("/alkanes");
    let recent_block_heights: Vec<u64> =
        (latest_height.saturating_sub(9)..=latest_height).rev().collect();

    let top_alkanes_table: Markup = if top_alkanes.is_empty() {
        html! { p class="muted" { "No alkanes found." } }
    } else {
        alkanes_table(&top_alkanes, false, false, true)
    };

    let latest_txs_table: Markup = if latest_alkane_txs.is_empty() {
        html! { p class="muted" { "No alkane transactions found." } }
    } else {
        html! {
            table class="table holders_table home-table" {
                tbody {
                    @for row in latest_alkane_txs {
                        tr {
                            td class="tx-trace-cell" {
                                div class="tx-trace-header" {
                                    a class="link mono tx-trace-id" href=(explorer_path(&format!("/tx/{}", row.txid))) { (row.txid) }
                                }
                                (render_trace_summaries(std::slice::from_ref(&row.trace), &state.essentials_mdb))
                            }
                        }
                    }
                }
            }
        }
    };

    layout_with_meta(
        "Blocks",
        "/",
        None,
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(Some(latest_height), espo_tip))
            }
            div class="home-recent-blocks muted" {
                "Latest blocks: "
                @for (idx, height) in recent_block_heights.iter().enumerate() {
                    @if idx > 0 {
                        " · "
                    }
                    a class="link mono" href=(explorer_path(&format!("/block/{height}"))) { (height) }
                }
            }

            div class="home-table-intro" {
                h2 class="home-table-title" {
                    "Explore "
                    span class="home-table-accent" { "programmable" }
                    " Bitcoin"
                }
            }
            div class="grid2 home-table-grid" {
                div class="home-table-block" {
                    div class="home-table-header" {
                        h2 class="h2" { "Top Alkanes" }
                        a class="home-table-link" href=(alkanes_link) {
                            "View more Alkanes"
                            (icon_right())
                        }
                    }
                    div class="home-table-card" {
                        (top_alkanes_table)
                    }
                }
                div class="home-table-block" {
                    div class="home-table-header" {
                            h2 class="h2" { "Latest Traces" }
                        a class="home-table-link" href=(latest_block_link) {
                            "View more Alkane txs"
                            (icon_right())
                        }
                    }
                    div class="home-table-card" {
                        (latest_txs_table)
                    }
                }
            }
        },
    )
}
