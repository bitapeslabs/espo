use std::collections::HashMap;

use bitcoin::{Address, Network, Transaction, Txid};
use maud::{Markup, html};

use crate::alkanes::trace::{EspoTrace, prettyify_protobuf_trace_json};
use crate::explorer::consts::{ALKANE_ICON_BASE, ALKANE_NAME_OVERRIDES};
use crate::explorer::pages::common::{fmt_alkane_amount, fmt_amount};
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::utils::balances::OutpointLookup;
use crate::schemas::SchemaAlkaneId;

const ADDR_PREFIX_LEN: usize = 31;
const ADDR_SUFFIX_LEN: usize = 8;

fn addr_suffix(addr: &str) -> String {
    let suffix_len = addr.len().saturating_sub(ADDR_PREFIX_LEN).min(ADDR_SUFFIX_LEN);
    if suffix_len == 0 {
        return String::new();
    }
    addr[addr.len().saturating_sub(suffix_len)..].to_string()
}

fn arrow_svg() -> Markup {
    html! {
        svg class="io-arrow-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" aria-hidden="true" {
            path d="M224.49,136.49l-72,72a12,12,0,0,1-17-17L187,140H40a12,12,0,0,1,0-24H187L135.51,64.48a12,12,0,0,1,17-17l72,72A12,12,0,0,1,224.49,136.49Z" fill="currentColor" {}
        }
    }
}

/// Render a transaction with VINs, VOUTs, and optional trace details.
pub fn render_tx(
    txid: &Txid,
    tx: &Transaction,
    traces: Option<&[EspoTrace]>,
    network: Network,
    prev_map: &HashMap<Txid, Transaction>,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
    outspends_fn: &dyn Fn(&Txid) -> Vec<Option<Txid>>,
) -> Markup {
    let vins_markup = render_vins(tx, network, prev_map, outpoint_fn);
    let outspends = outspends_fn(txid);
    let vouts_markup = render_vouts(txid, tx, network, outpoint_fn, &outspends);
    let traces_markup = render_traces(traces);

    html! {
        div class="card tx-card" {
            span class="mono tx-title" { a class="link" href=(format!("/tx/{}", txid)) { (txid) } }
            div class="tx-io-grid" {
                div class="io-col" { (vins_markup) }
                div class="io-col" { (vouts_markup) }
            }
            (traces_markup)
        }
    }
}

fn render_vins(
    tx: &Transaction,
    network: Network,
    prev_map: &HashMap<Txid, Transaction>,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
) -> Markup {
    html! {
        @if tx.input.is_empty() {
            p class="muted" { "No inputs" }
        } @else {
            div class="io-list" {
                @for vin in tx.input.iter() {
                    @if vin.previous_output.is_null() {
                        div class="io-row" {
                            span class="io-arrow in" title="Coinbase input" { (arrow_svg()) }
                            div class="io-main" {
                                div class="io-addr-row" {
                                    div class="io-addr mono" { "Coinbase" }
                                    div class="io-amount muted" { "—" }
                                }
                            }
                        }
                    } @else {
                        @let prev_txid = vin.previous_output.txid;
                        @let prev_vout = vin.previous_output.vout;
                        @let prevout = prev_map.get(&prev_txid).and_then(|ptx| ptx.output.get(prev_vout as usize));
                        @let prevout_view = outpoint_fn(&prev_txid, prev_vout);
                        div class="io-row" {
                            a class="io-arrow io-arrow-link in" href=(format!("/tx/{}", prev_txid)) title="View previous transaction" { (arrow_svg()) }
                            div class="io-main" {
                                @match prevout {
                                    Some(po) => {
                                        @let addr_opt = Address::from_script(po.script_pubkey.as_script(), network).ok();
                                        div class="io-addr-row" {
                                            div class="io-addr" {
                                                @match addr_opt {
                                                    Some(a) => {
                                                        @let addr = a.to_string();
                                                        @let addr_suffix = addr_suffix(&addr);
                                                        a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                            span class="addr-prefix-wrap" {
                                                                span class="addr-prefix" { (addr) }
                                                            }
                                                            @if !addr_suffix.is_empty() {
                                                                span class="addr-suffix" { (addr_suffix) }
                                                            }
                                                        }
                                                    }
                                                    None => span class="mono muted" { "unknown" },
                                                }
                                            }
                                            div class="io-amount muted" { (fmt_amount(po.value)) }
                                        }
                                    }
                                    None => {
                                        div class="io-addr-row" {
                                            div class="io-addr muted" { "prevout unavailable" }
                                            div class="io-amount muted" { "—" }
                                        }
                                    }
                                }
                                (balances_list(&prevout_view.balances))
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_vouts(
    txid: &Txid,
    tx: &Transaction,
    network: Network,
    outpoint_fn: &dyn Fn(&Txid, u32) -> OutpointLookup,
    outspends: &[Option<Txid>],
) -> Markup {
    html! {
        @if tx.output.is_empty() {
            p class="muted" { "No outputs" }
        } @else {
            div class="io-list" {
                @for (vout, o) in tx.output.iter().enumerate() {
                    @let OutpointLookup { balances, spent_by: db_spent } = outpoint_fn(txid, vout as u32);
                    @let spent_by = outspends.get(vout).cloned().flatten().or(db_spent);
                    div class="io-row" {
                        div class="io-main" {
                            @let addr_opt = Address::from_script(o.script_pubkey.as_script(), network).ok();
                            div class="io-addr-row" {
                                div class="io-addr" {
                                    @match addr_opt {
                                        Some(a) => {
                                            @let addr = a.to_string();
                                            @let addr_suffix = addr_suffix(&addr);
                                            a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                                                span class="addr-prefix-wrap" {
                                                    span class="addr-prefix" { (addr) }
                                                }
                                                @if !addr_suffix.is_empty() {
                                                    span class="addr-suffix" { (addr_suffix) }
                                                }
                                            }
                                        }
                                        None => span class="mono muted" { "non-standard" },
                                    }
                                }
                                div class="io-amount mono muted" { (fmt_amount(o.value)) }
                            }
                            (balances_list(&balances))
                        }
                        @match spent_by {
                            Some(spender) => a class="io-arrow io-arrow-link out spent" href=(format!("/tx/{}", spender)) title="Spent by transaction" { (arrow_svg()) },
                            None => span class="io-arrow out" title="Unspent output" { (arrow_svg()) },
                        }
                    }
                }
            }
        }
    }
}

fn balances_list(entries: &[BalanceEntry]) -> Markup {
    if entries.is_empty() {
        return html! {};
    }
    html! {
        div class="io-alkanes" {
            @for be in entries {
                @let (name, sym, icon) = alkane_meta(&be.alkane);
                @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                div class="alk-chip" {
                    img class="icon-img" src=(icon) alt=(sym.clone()) {}
                    div class="alk-text" {
                        div class="alk-row" {
                            a class="link mono" href=(format!("/alkane/{alk}")) { (sym) }
                            span class="muted mono" { " • " (name) }
                        }
                        div class="alk-amt mono" { (fmt_alkane_amount(be.amount)) }
                    }
                }
            }
        }
    }
}

fn alkane_meta(id: &SchemaAlkaneId) -> (String, String, String) {
    let key = format!("{}:{}", id.block, id.tx);
    for (id_s, name, sym) in ALKANE_NAME_OVERRIDES {
        if *id_s == key {
            let icon = format!("{}/{}_{}.png", ALKANE_ICON_BASE, id.block, id.tx);
            return (name.to_string(), sym.to_string(), icon);
        }
    }
    let icon = format!("{}/{}_{}.png", ALKANE_ICON_BASE, id.block, id.tx);
    (key.clone(), key, icon)
}

fn render_traces(traces: Option<&[EspoTrace]>) -> Markup {
    let Some(traces) = traces else {
        return html! {};
    };

    if traces.is_empty() {
        return html! {};
    }

    html! {
        div class="tx-traces" {
            @for t in traces {
                @let events = prettyify_protobuf_trace_json(&t.protobuf_trace).unwrap_or_else(|_| "[]".to_string());
                details class="details" {
                    summary class="summary link" { "view alkane trace" }
                    pre class="code" { (events) }
                }
            }
        }
    }
}
