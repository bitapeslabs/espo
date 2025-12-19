use axum::extract::{Path, State};
use axum::response::Html;
use bitcoin::hashes::Hash;
use bitcoin::{Address, Txid};
use bitcoincore_rpc::RpcApi;
use borsh::BorshDeserialize;
use maud::{Markup, html};
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::explorer::components::layout::layout;
use crate::explorer::components::table::table;
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::utils::balances::{get_balance_for_address, get_outpoint_balances};
use crate::schemas::EspoOutpoint;

pub async fn address_page(
    State(state): State<ExplorerState>,
    Path(address_raw): Path<String>,
) -> Html<String> {
    let address = match Address::from_str(address_raw.trim())
        .ok()
        .and_then(|a| a.require_network(state.network).ok())
    {
        Some(a) => a.to_string(),
        None => {
            return layout(
                "Address",
                html! { p class="error" { "Invalid address for this network." } },
            );
        }
    };

    let balances = get_balance_for_address(&state.essentials_mdb, &address).unwrap_or_default();

    // Outpoints currently tracked for this address (Alkane-bearing UTXOs).
    let mut pref = b"/balances/".to_vec();
    pref.extend_from_slice(address.as_bytes());
    pref.push(b'/');

    let mut outpoints: Vec<(EspoOutpoint, Vec<BalanceEntry>)> = Vec::new();
    if let Ok(keys) = state.essentials_mdb.scan_prefix(&pref) {
        for k in keys {
            if k.len() <= pref.len() {
                continue;
            }
            let Ok(op) = EspoOutpoint::try_from_slice(&k[pref.len()..]) else { continue };
            if op.tx_spent.is_some() {
                continue;
            }
            let txid = match Txid::from_slice(&op.txid) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let entries =
                get_outpoint_balances(&state.essentials_mdb, &txid, op.vout).unwrap_or_default();
            outpoints.push((op, entries));
        }
    }
    outpoints.sort_by(|(a, _), (b, _)| a.as_outpoint_string().cmp(&b.as_outpoint_string()));

    // BTC value for these UTXOs (best-effort via Core's UTXO set).
    let rpc = crate::config::get_bitcoind_rpc_client();
    let mut btc_sats: u64 = 0;
    for (op, _) in &outpoints {
        let Ok(txid) = Txid::from_slice(&op.txid) else { continue };
        if let Ok(Some(utxo)) = rpc.get_tx_out(&txid, op.vout, None) {
            btc_sats = btc_sats.saturating_add(utxo.value.to_sat());
        }
    }

    let mut balances_sorted: BTreeMap<String, u128> = BTreeMap::new();
    for (alk, amt) in balances {
        balances_sorted.insert(format!("{}:{}", alk.block, alk.tx), amt);
    }

    let mut txids: Vec<String> = outpoints
        .iter()
        .filter_map(|(op, _)| Txid::from_slice(&op.txid).ok().map(|t| t.to_string()))
        .collect();
    txids.sort();
    txids.dedup();

    let balances_rows: Vec<Vec<Markup>> = balances_sorted
        .iter()
        .map(|(alk, amt)| {
            vec![
                html! { a class="link mono" href=(format!("/alkane/{alk}")) { (alk) } },
                html! { span class="right mono" { (fmt_alkane_amount(*amt)) } },
            ]
        })
        .collect();

    let holder_table = if balances_rows.is_empty() {
        html! { p class="muted" { "No alkanes tracked for this address." } }
    } else {
        table(&["Alkane", "Amount"], balances_rows)
    };

    layout(
        &format!("Address {address}"),
        html! {
            h1 class="h1" { "Address" }
            p class="mono" { (address.clone()) }

            div class="grid2" {
                div class="card" {
                    h2 class="h2" { "Bitcoin Balance" }
                    p class="muted" { "Sum of BTC in currently tracked Alkane UTXOs." }
                    p class="mono" { (crate::explorer::pages::common::fmt_sats(btc_sats)) }
                }

                div class="card" {
                    h2 class="h2" { "Alkane Balances" }
                    (holder_table)
                }
            }

            div class="card" {
                h2 class="h2" { "Transactions" }
                @if txids.is_empty() {
                    p class="muted" { "No tracked transactions." }
                } @else {
                    div class="list" {
                        @for t in txids {
                            a class="rowlink mono" href=(format!("/tx/{t}")) { (t) }
                        }
                    }
                }
            }

            div class="card" {
                h2 class="h2" { "Outpoints" }
                @if outpoints.is_empty() {
                    p class="muted" { "No tracked outpoints." }
                } @else {
                    @for (op, entries) in outpoints {
                        @let txid = Txid::from_slice(&op.txid).ok().map(|t| t.to_string()).unwrap_or_else(|| "unknown".to_string());
                        div class="item" {
                            div class="itemhead" {
                                a class="link mono" href=(format!("/tx/{txid}")) { (format!("{}:{}", txid, op.vout)) }
                            }
                            @if entries.is_empty() {
                                div class="sub muted" { "No alkane balances." }
                            } @else {
                                div class="tags" {
                                    @for be in entries {
                                        @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                                        a class="tag mono" href=(format!("/alkane/{alk}")) {
                                            (alk) " • " (fmt_alkane_amount(be.amount))
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}
