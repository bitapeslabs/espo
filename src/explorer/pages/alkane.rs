use crate::runtime::state_at::StateAt;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use hex;
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;

use crate::explorer::components::alk_balances::render_alkane_balance_cards;
use crate::explorer::components::header::header_scripts;
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::{
    icon_caret_right, icon_dropdown_caret, icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::holders_table;
use crate::explorer::components::tx_view::{
    AlkaneMetaCache, alkane_icon_url_unfiltered, alkane_meta, icon_bg_style,
};
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::{current_language, explorer_path};
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::schemas::Timeframe;
use crate::modules::ammdata::storage::{AmmDataProvider, AmmDataTable, GetListKeysByPrefixParams};
use crate::modules::essentials::storage::{
    BalanceEntry, EssentialsProvider, GetRawValueParams, HolderId, load_creation_record,
};
use crate::modules::essentials::utils::balances::{
    get_alkane_balances, get_holders_for_alkane, get_total_received_for_alkane,
    get_transfer_volume_for_alkane,
};
use crate::modules::essentials::utils::inspections::{StoredInspectionMethod, load_inspection};
use crate::modules::pizzafun::storage::{GetSeriesByAlkaneParams, PizzafunProvider};
use crate::schemas::SchemaAlkaneId;
use std::collections::HashSet;
use std::sync::Arc;

const ADDR_SUFFIX_LEN: usize = 8;
const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
const KV_KEY_BEACON: &[u8] = b"/beacon";
const UPGRADEABLE_METHODS: [(&str, u128); 2] = [("initialize", 32767), ("forward", 36863)];

#[derive(Deserialize)]
pub struct PageQuery {
    pub tab: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AlkaneTab {
    Holders,
    Inspect,
    TransferVolume,
    TotalReceived,
}

impl AlkaneTab {
    fn from_query(raw: Option<&str>) -> Self {
        match raw {
            Some("inspect") => AlkaneTab::Inspect,
            Some("transfer_volume") => AlkaneTab::TransferVolume,
            Some("total_received") => AlkaneTab::TotalReceived,
            _ => AlkaneTab::Holders,
        }
    }
}

#[derive(Clone)]
struct AlkaneBalanceChartToken {
    alkane_id: String,
    label: String,
    asset_name: String,
    symbol: String,
    icon_url: String,
    fallback_letter: String,
}

fn token_fallback_letter(label: &str, fallback: &str) -> String {
    label
        .chars()
        .find(|c| c.is_ascii_alphanumeric())
        .or_else(|| fallback.chars().find(|c| c.is_ascii_alphanumeric()))
        .map(|c| c.to_ascii_uppercase().to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn alkane_chart_token_icon(icon_url: &str, fallback_letter: &str) -> Markup {
    html! {
        span class="alk-icon-wrap address-balance-dropdown-alk-icon" aria-hidden="true" {
            span class="alk-icon-img" style=(icon_bg_style(icon_url)) {}
            span class="alk-icon-letter" { (fallback_letter) }
        }
    }
}

pub async fn alkane_page(
    State(state): State<ExplorerState>,
    Path(alkane_raw): Path<String>,
    Query(q): Query<PageQuery>,
) -> Response {
    let canonical_path_fallback = "/alkane".to_string();
    let Some(alk) = parse_alkane_id(&alkane_raw) else {
        return (
            StatusCode::NOT_FOUND,
            layout_with_meta(
                "Alkane",
                &canonical_path_fallback,
                None,
                html! { p class="error" { "Invalid alkane id; expected \"<block>:<tx>\"." } },
            ),
        )
            .into_response();
    };

    let tab = AlkaneTab::from_query(q.tab.as_deref());
    let all_range_label = if current_language().is_chinese() { "全部" } else { "All" };
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let alk_str = format!("{}:{}", alk.block, alk.tx);
    let mut kv_cache: AlkaneMetaCache = Default::default();
    let meta = alkane_meta(&alk, &mut kv_cache, &state.essentials_mdb);
    let display_name = meta.name.value.clone();
    let fallback_letter = meta.name.fallback_letter();
    let hero_icon_url = alkane_icon_url_unfiltered(&alk, &state.essentials_mdb);
    let page_title = if meta.name.known && display_name != alk_str {
        format!("Alkane {display_name} ({alk_str})")
    } else {
        format!("Alkane {alk_str}")
    };

    let creation_record = load_creation_record(&state.essentials_mdb, &alk).ok().flatten();
    let creation_ts = creation_record.as_ref().map(|r| r.creation_timestamp as u64);
    let creation_height = creation_record.as_ref().map(|r| r.creation_height);
    let creation_txid = creation_record.as_ref().map(|r| hex::encode(r.txid));

    let balances_map = get_alkane_balances(StateAt::Latest, &state.essentials_provider(), &alk)
        .unwrap_or_default();
    let mut balance_entries: Vec<BalanceEntry> = balances_map
        .into_iter()
        .map(|(alk, amt)| BalanceEntry { alkane: alk, amount: amt })
        .collect();
    balance_entries.sort_by(|a, b| {
        a.alkane.block.cmp(&b.alkane.block).then_with(|| a.alkane.tx.cmp(&b.alkane.tx))
    });
    let balance_chart_tokens: Vec<AlkaneBalanceChartToken> = balance_entries
        .iter()
        .map(|entry| {
            let alkane_id = format!("{}:{}", entry.alkane.block, entry.alkane.tx);
            let token_meta = alkane_meta(&entry.alkane, &mut kv_cache, &state.essentials_mdb);
            let label = if token_meta.name.known && token_meta.name.value != alkane_id {
                format!("{} ({})", token_meta.name.value, alkane_id)
            } else {
                alkane_id.clone()
            };
            AlkaneBalanceChartToken {
                alkane_id: alkane_id.clone(),
                label,
                asset_name: token_meta.name.value.clone(),
                symbol: token_meta.symbol.clone(),
                icon_url: token_meta.icon_url.clone(),
                fallback_letter: token_fallback_letter(&token_meta.name.value, &alkane_id),
            }
        })
        .collect();
    let default_balance_chart_alkane = balance_chart_tokens.get(0).map(|t| t.alkane_id.clone());

    let (total, circulating_supply, holders) =
        get_holders_for_alkane(StateAt::Latest, &state.essentials_provider(), alk, page, limit)
            .unwrap_or((0, 0, Vec::new()));
    let off = limit.saturating_mul(page.saturating_sub(1));
    let holders_len = holders.len();
    let has_prev = page > 1;
    let has_next = off + holders_len < total;
    let display_start = if total > 0 && off < total { off + 1 } else { 0 };
    let display_end = (off + holders_len).min(total);
    let last_page = if total > 0 { (total + limit - 1) / limit } else { 1 };
    let icon_url = meta.icon_url.clone();
    let coin_label = meta.name.value.clone();
    let holders_count = total;
    let supply_f64 = circulating_supply as f64;

    let tv_iframe_src: Option<String> = {
        let series_id = {
            let pizzafun_mdb = crate::config::get_espo_module_mdb("pizzafun");
            let pizzafun = PizzafunProvider::new(pizzafun_mdb);
            pizzafun
                .get_series_by_alkane(GetSeriesByAlkaneParams {
                    blockhash: StateAt::Latest,
                    alkane: alk,
                })
                .ok()
                .flatten()
                .map(|e| e.series_id)
        };

        let has_market_chart = {
            // Special-case: 2:0 is the derived liquidity quote token itself, so it won't have
            // token-derived series. Show its chart if the plain 2:0-usd candle series exists.
            let is_derived_quote_token = alk.block == 2 && alk.tx == 0;

            let derived_quotes: Vec<SchemaAlkaneId> = AmmDataConfig::load_from_global_config()
                .ok()
                .and_then(|c| c.derived_liquidity)
                .map(|dl| dl.derived_quotes.into_iter().map(|q| q.alkane).collect())
                .unwrap_or_default();

            let amm_mdb = crate::config::get_espo_module_mdb("ammdata");
            let table = AmmDataTable::new(&amm_mdb);
            let amm_provider =
                AmmDataProvider::new(Arc::clone(&amm_mdb), Arc::new(state.essentials_provider()));

            let has_prefix = |rel_prefix: Vec<u8>| -> bool {
                amm_provider
                    .get_list_keys_by_prefix(GetListKeysByPrefixParams {
                        blockhash: StateAt::Latest,
                        prefix: rel_prefix,
                    })
                    .map(|res| !res.keys.is_empty())
                    .unwrap_or(false)
            };

            if is_derived_quote_token {
                has_prefix(table.token_usd_candle_ns_prefix(&alk, Timeframe::D1))
            } else if derived_quotes.is_empty() {
                false
            } else {
                derived_quotes.iter().any(|quote| {
                    has_prefix(table.token_derived_mcusd_candle_ns_prefix(
                        &alk,
                        quote,
                        Timeframe::D1,
                    ))
                })
            }
        };

        match (series_id, has_market_chart) {
            (Some(series_id), true) => Some(pizza_tv_iframe_src(&series_id)),
            _ => None,
        }
    };
    let chart_hidden = if tv_iframe_src.is_some() { "0" } else { "1" };

    let inspection = creation_record.as_ref().and_then(|r| r.inspection.as_ref());
    let mut inspect_source = inspection.cloned();
    let mut proxy_target_label: Option<String> = None;
    let inspect_alkane_id = alk_str.clone();
    if let Some(proxy_target) = resolve_proxy_target_recursive(&alk, &state.essentials_provider()) {
        let label = format!("{}:{}", proxy_target.block, proxy_target.tx);
        proxy_target_label = Some(label.clone());
        inspect_source =
            load_inspection(&state.essentials_provider(), &proxy_target).ok().flatten();
    }
    let (view_methods, write_methods) = split_methods(inspect_source.as_ref());
    let inspect_name = display_name.clone();
    let inspect_id_label = if let Some(label) = proxy_target_label.as_ref() {
        format!("{alk_str} (proxied to {label})")
    } else {
        alk_str.clone()
    };

    let rows: Vec<Vec<Markup>> = holders
        .into_iter()
        .enumerate()
        .map(|(idx, h)| {
            let rank = off + idx + 1;
            let pct = if supply_f64 > 0.0 {
                (h.amount as f64) * 100.0 / supply_f64
            } else {
                0.0
            };
            let pct_label = format!("{pct:.2}%");
            let holder_cell = match h.holder {
                HolderId::Address(addr) => {
                    let (addr_prefix, addr_suffix) = addr_prefix_suffix(&addr);
                    html! {
                        a class="link mono addr-inline" href=(explorer_path(&format!("/address/{}", addr))) {
                            span class="addr-rank" { (format!("{rank}.")) }
                            span class="addr-prefix" { (addr_prefix) }
                            span class="addr-suffix" { (addr_suffix) }
                        }
                    }
                }
                HolderId::Alkane(id) => {
                    let id_str = format!("{}:{}", id.block, id.tx);
                    let h_meta = alkane_meta(&id, &mut kv_cache, &state.essentials_mdb);
                    let h_fallback_letter = h_meta.name.fallback_letter();
                    html! {
                        a class="link mono addr-inline" href=(explorer_path(&format!("/alkane/{id_str}"))) {
                            span class="addr-rank" { (format!("{rank}.")) }
                            div class="alk-icon-wrap" aria-hidden="true" {
                                span class="alk-icon-img" style=(icon_bg_style(&h_meta.icon_url)) {}
                                span class="alk-icon-letter" { (h_fallback_letter) }
                            }
                            span class="addr-prefix" { (h_meta.name.value.clone()) }
                            span class="addr-suffix mono" { (format!(" ({id_str})")) }
                        }
                    }
                }
            };
            vec![
                holder_cell,
                html! {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            span class="alk-icon-img" style=(icon_bg_style(&icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_alkane_amount(h.amount)) }
                        a class="alk-sym link mono" href=(explorer_path(&format!("/alkane/{alk_str}"))) { (coin_label.clone()) }
                    }
                },
                html! {
                    span class="alk-holding-pct mono" { (pct_label) }
                },
            ]
        })
        .collect();

    let table_markup = if rows.is_empty() {
        html! { div class="alkane-panel" { p class="muted" { "No holders." } } }
    } else {
        html! {
            div class="alkane-panel alkane-holders-card" {
                (holders_table(&["Holder", "Balance", "Holding %"], rows))
            }
        }
    };

    let (activity_total, activity_entries, activity_label) = match tab {
        AlkaneTab::TransferVolume => {
            let (total, entries) = get_transfer_volume_for_alkane(
                StateAt::Latest,
                &state.essentials_provider(),
                alk,
                page,
                limit,
            )
            .unwrap_or((0, Vec::new()));
            (total, entries, "Transfer volume")
        }
        AlkaneTab::TotalReceived => {
            let (total, entries) = get_total_received_for_alkane(
                StateAt::Latest,
                &state.essentials_provider(),
                alk,
                page,
                limit,
            )
            .unwrap_or((0, Vec::new()));
            (total, entries, "Total received")
        }
        _ => (0, Vec::new(), "Transfer volume"),
    };
    let activity_off = limit.saturating_mul(page.saturating_sub(1));
    let activity_len = activity_entries.len();
    let activity_has_prev = page > 1;
    let activity_has_next = activity_off + activity_len < activity_total;
    let activity_display_start =
        if activity_total > 0 && activity_off < activity_total { activity_off + 1 } else { 0 };
    let activity_display_end = (activity_off + activity_len).min(activity_total);
    let activity_last_page =
        if activity_total > 0 { (activity_total + limit - 1) / limit } else { 1 };

    let activity_rows: Vec<Vec<Markup>> = activity_entries
        .into_iter()
        .enumerate()
        .map(|(idx, entry)| {
            let rank = activity_off + idx + 1;
            let (addr_prefix, addr_suffix) = addr_prefix_suffix(&entry.address);
            vec![
                html! {
                    a class="link mono addr-inline" href=(explorer_path(&format!("/address/{}", entry.address))) {
                        span class="addr-rank" { (format!("{rank}.")) }
                        span class="addr-prefix" { (addr_prefix) }
                        span class="addr-suffix" { (addr_suffix) }
                    }
                },
                html! {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            span class="alk-icon-img" style=(icon_bg_style(&icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_activity_amount(entry.amount)) }
                        a class="alk-sym link mono" href=(explorer_path(&format!("/alkane/{alk_str}"))) { (coin_label.clone()) }
                    }
                },
            ]
        })
        .collect();

    let activity_table_markup = if activity_rows.is_empty() {
        html! { div class="alkane-panel" { p class="muted" { "No activity yet." } } }
    } else {
        html! {
            div class="alkane-panel alkane-holders-card alkane-activity-card" {
                (holders_table(&["Address", activity_label], activity_rows))
            }
        }
    };

    let balances_markup = if balance_entries.is_empty() {
        html! { p class="muted" { "No alkanes tracked for this alkane." } }
    } else {
        render_alkane_balance_cards(&balance_entries, &state.essentials_mdb)
    };

    layout_with_meta(
        &page_title,
        &format!("/alkane/{alk_str}"),
        None,
        html! {
            div class="alkane-page" {
                div class="alkane-hero-card" {
                    div class="alk-icon-wrap alk-icon-lg" aria-hidden="true" {
                        span class="alk-icon-img" style=(icon_bg_style(&hero_icon_url)) {}
                        span class="alk-icon-letter" { (fallback_letter) }
                    }
                    div class="alkane-hero-text" {
                        span class="alkane-tag" { "TOKEN" }
                        h1 class="alkane-hero-title" { (display_name.clone()) }
                        span class="alkane-hero-id mono" { (alk_str.clone()) }
                    }
                }

                section class="alkane-section" data-alkane-overview="" {
                    div class="alkane-overview-grid" data-chart-hidden=(chart_hidden) {
                        @if let Some(src) = tv_iframe_src.as_ref() {
                            div class="alkane-market-pane" {
                                h2 class="section-title alkane-market-title" { "Market" }
                                div class="alkane-market-card alkane-market-tv" {
                                    iframe class="alkane-market-iframe" src=(src) title="Market chart" {
                                        "Market chart"
                                    }
                                }
                            }
                        }
                        div class="alkane-overview-pane" {
                            h2 class="section-title" { "Overview" }
                            div class="alkane-overview-card" {
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Symbol" }
                                    div class="alkane-stat-line" {
                                        span class="alkane-stat-value" { (meta.symbol.clone()) }
                                    }
                                }
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Circulating supply" }
                                    div class="alkane-stat-line" {
                                        span class="alkane-stat-value" { (fmt_alkane_amount(circulating_supply)) }
                                        span class="alkane-stat-sub" { "(with 8 decimals)" }
                                    }
                                }
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Holders" }
                                    div class="alkane-stat-line" {
                                        span class="alkane-stat-value" { (holders_count) }
                                    }
                                }
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Deploy date" }
                                    @if let Some(ts) = creation_ts {
                                        div class="alkane-stat-line" data-ts-group="" {
                                            span class="alkane-stat-value" data-header-ts=(ts) { (ts) }
                                            span class="alkane-stat-sub" data-header-ts-rel { "" }
                                        }
                                    } @else {
                                        div class="alkane-stat-line" {
                                            span class="alkane-stat-value muted" { "Unknown" }
                                        }
                                    }
                                }
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Deploy transaction" }
                                    @if let Some(txid) = creation_txid.as_ref() {
                                        div class="alkane-stat-line" {
                                            a class="alkane-stat-value link mono" href=(explorer_path(&format!("/tx/{txid}"))) { (short_hex(txid)) }
                                        }
                                    } @else {
                                        div class="alkane-stat-line" {
                                            span class="alkane-stat-value muted" { "Unknown" }
                                        }
                                    }
                                }
                                div class="alkane-stat" {
                                    span class="alkane-stat-label" { "Deploy block" }
                                    @if let Some(h) = creation_height {
                                        div class="alkane-stat-line" {
                                            a class="alkane-stat-value link" href=(explorer_path(&format!("/block/{h}"))) { (h) }
                                        }
                                    } @else {
                                        div class="alkane-stat-line" {
                                            span class="alkane-stat-value muted" { "Unknown" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                section class="alkane-section" {
                    h2 class="section-title" { "Alkane Balances" }
                    (balances_markup)
                    @if let Some(default_alkane) = default_balance_chart_alkane.as_ref() {
                        div
                            class="card address-balance-chart-card"
                            data-alkane-balance-chart=""
                            data-alkane=(alk_str.clone())
                            data-default-alkane=(default_alkane)
                            data-default-range="all"
                        {
                            div class="address-balance-chart-head" {
                                h2 class="h2" { "Balance History" }
                                div class="address-balance-chart-controls" {
                                    div class="dropdown address-balance-dropdown" data-dropdown="" data-open="" data-alkane-balance-chart-token="" {
                                        button
                                            class="dropdown-trigger"
                                            type="button"
                                            aria-label="Alkane"
                                            aria-haspopup="true"
                                            aria-expanded="false"
                                            data-dropdown-toggle=""
                                        {
                                            span class="dropdown-icon dropdown-trigger-icon" data-alkane-balance-chart-token-trigger-icon="" {
                                                (alkane_chart_token_icon(
                                                    &balance_chart_tokens[0].icon_url,
                                                    &balance_chart_tokens[0].fallback_letter
                                                ))
                                            }
                                            span class="dropdown-label" data-alkane-balance-chart-token-trigger-label="" {
                                                (balance_chart_tokens[0].label.clone())
                                            }
                                            span class="dropdown-caret" { (icon_dropdown_caret()) }
                                        }
                                        div class="dropdown-panel address-balance-dropdown-panel" role="menu" aria-hidden="true" {
                                            @for token in balance_chart_tokens.iter() {
                                                @let item_class = if token.alkane_id == balance_chart_tokens[0].alkane_id {
                                                    "dropdown-item selected"
                                                } else {
                                                    "dropdown-item"
                                                };
                                                a
                                                    class=(item_class)
                                                    href="#"
                                                    role="menuitem"
                                                    data-alkane-balance-chart-token-option=""
                                                    data-alkane-id=(token.alkane_id.clone())
                                                    data-name=(token.asset_name.clone())
                                                    data-symbol=(token.symbol.clone())
                                                    data-label=(token.label.clone())
                                                {
                                                    span class="dropdown-icon" {
                                                        (alkane_chart_token_icon(
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
                            div class="address-balance-chart-plot" data-alkane-balance-chart-root {
                                div class="address-balance-chart-loading" data-alkane-balance-chart-loading="" data-spinning="1" {
                                    span class="spinner address-balance-chart-spinner" data-alkane-balance-chart-loading-spinner="" aria-hidden="true" {}
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

                section class="alkane-section" {
                    div class="alkane-tabs" {
                        div class="alkane-tab-list" {
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Holders { " active" } else { "" }))
                                href=(explorer_path(&format!("/alkane/{alk_str}?page={page}&limit={limit}"))) { "Holders" }
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::TransferVolume { " active" } else { "" }))
                                href=(explorer_path(&format!("/alkane/{alk_str}?tab=transfer_volume&page={page}&limit={limit}"))) { "Transfer Volume" }
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::TotalReceived { " active" } else { "" }))
                                href=(explorer_path(&format!("/alkane/{alk_str}?tab=total_received&page={page}&limit={limit}"))) { "Total Received" }
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Inspect { " active" } else { "" }))
                                href=(explorer_path(&format!("/alkane/{alk_str}?tab=inspect&page={page}&limit={limit}"))) { "Inspect contract" }
                        }
                        div class="alkane-tab-panel" {
                            @if tab == AlkaneTab::Holders {
                                (table_markup)
                                div class="pager" {
                                    @if has_prev {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?page=1&limit={limit}"))) aria-label="First page" {
                                            (icon_skip_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                                    }
                                    @if has_prev {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?page={}&limit={limit}", page - 1))) aria-label="Previous page" {
                                            (icon_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                                    }
                                    span class="pager-meta muted" { "Showing "
                                        (if total > 0 { display_start } else { 0 })
                                        @if total > 0 {
                                            "-"
                                            (display_end)
                                        }
                                        " / "
                                        (total)
                                    }
                                    @if has_next {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?page={}&limit={limit}", page + 1))) aria-label="Next page" {
                                            (icon_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                                    }
                                    @if has_next {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?page={}&limit={limit}", last_page))) aria-label="Last page" {
                                            (icon_skip_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                                    }
                                }
                            } @else if tab == AlkaneTab::TransferVolume || tab == AlkaneTab::TotalReceived {
                                (activity_table_markup)
                                div class="pager" {
                                    @if activity_has_prev {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?tab={}&page=1&limit={limit}", match tab { AlkaneTab::TransferVolume => "transfer_volume", AlkaneTab::TotalReceived => "total_received", _ => "transfer_volume" }))) aria-label="First page" {
                                            (icon_skip_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                                    }
                                    @if activity_has_prev {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?tab={}&page={}&limit={limit}", match tab { AlkaneTab::TransferVolume => "transfer_volume", AlkaneTab::TotalReceived => "total_received", _ => "transfer_volume" }, page - 1))) aria-label="Previous page" {
                                            (icon_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                                    }
                                    span class="pager-meta muted" { "Showing "
                                        (if activity_total > 0 { activity_display_start } else { 0 })
                                        @if activity_total > 0 {
                                            "-"
                                            (activity_display_end)
                                        }
                                        " / "
                                        (activity_total)
                                    }
                                    @if activity_has_next {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?tab={}&page={}&limit={limit}", match tab { AlkaneTab::TransferVolume => "transfer_volume", AlkaneTab::TotalReceived => "total_received", _ => "transfer_volume" }, page + 1))) aria-label="Next page" {
                                            (icon_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                                    }
                                    @if activity_has_next {
                                        a class="pill iconbtn" href=(explorer_path(&format!("/alkane/{alk_str}?tab={}&page={}&limit={limit}", match tab { AlkaneTab::TransferVolume => "transfer_volume", AlkaneTab::TotalReceived => "total_received", _ => "transfer_volume" }, activity_last_page))) aria-label="Last page" {
                                            (icon_skip_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                                    }
                                }
                            } @else {
                                div class="alkane-inspect-card" data-alkane-inspect="" data-alkane-id=(inspect_alkane_id.clone()) {
                                    div class="alkane-inspect-header" {
                                        span class="alkane-inspect-name" { (inspect_name.clone()) }
                                        span class="alkane-inspect-id mono" { (inspect_id_label.clone()) }
                                    }
                                    div class="alkane-inspect-block-control" {
                                        span class="alkane-inspect-block-label" { "View as block:" }
                                        div class="hero-search-input alkane-inspect-block-input-wrap" {
                                            input class="hero-search-field alkane-inspect-block-input mono" type="text" value="latest" placeholder="latest" data-sim-block-input="" aria-label="View as block" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false";
                                        }
                                    }
                                    @if view_methods.is_empty() && write_methods.is_empty() {
                                        p class="muted" { "No contract methods found." }
                                    } @else {
                                        div class="alkane-method-group" {
                                            h3 class="alkane-method-title" { "Read methods:" }
                                            @if view_methods.is_empty() {
                                                p class="muted" { "No read methods." }
                                            } @else {
                                                @for method in &view_methods {
                                                    details class="opret-toggle alkane-method-toggle" data-alkane-method=(method.name.clone()) data-alkane-opcode=(method.opcode) data-alkane-returns=(method.returns.clone()) data-alkane-view="1" {
                                                        summary class="opret-toggle-summary" {
                                                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                                            span class="opret-toggle-label" { (method.name.clone()) }
                                                            span class="trace-opcode" { (format!("opcode {}", method.opcode)) }
                                                        }
                                                        div class="opret-toggle-body" {
                                                            div class="alkane-method-result" data-sim-result="" data-status="idle" {
                                                                span class="alkane-method-label" { "Result:" }
                                                                div class="alkane-method-value" data-sim-value="" { "—" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        div class="alkane-method-group" {
                                            h3 class="alkane-method-title" { "Write methods:" }
                                            @if write_methods.is_empty() {
                                                p class="muted" { "No write methods." }
                                            } @else {
                                                @for method in &write_methods {
                                                    details class="opret-toggle alkane-method-toggle" data-alkane-method=(method.name.clone()) data-alkane-opcode=(method.opcode) data-alkane-returns=(method.returns.clone()) data-alkane-view="0" {
                                                        summary class="opret-toggle-summary" {
                                                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                                            span class="opret-toggle-label" { (method.name.clone()) }
                                                            span class="trace-opcode" { (format!("opcode {}", method.opcode)) }
                                                        }
                                                        div class="opret-toggle-body" {
                                                            div class="alkane-method-result" data-sim-result="" data-status="idle" {
                                                                span class="alkane-method-label" { "Result:" }
                                                                div class="alkane-method-value muted" data-sim-value="" data-default-text="Providing inputs to simulate methods is not currently supported on espo" {
                                                                    "Providing inputs to write methods is not currently supported on Espo"
                                                                }
                                                            }
                                                            button class="alkane-method-btn" type="button" { "Simulate anyways" }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            (header_scripts())
            @if default_balance_chart_alkane.is_some() {
                (alkane_balance_chart_scripts())
            }
            @if tab == AlkaneTab::Inspect {
                (inspect_scripts())
            }
        },
    )
    .into_response()
}

fn short_hex(s: &str) -> String {
    const KEEP: usize = 6;
    if s.len() <= KEEP * 2 {
        return s.to_string();
    }
    format!("{}...{}", &s[..KEEP], &s[s.len() - KEEP..])
}

fn parse_alkane_id(s: &str) -> Option<crate::schemas::SchemaAlkaneId> {
    let (a, b) = s.split_once(':')?;
    let block = parse_u32_any(a)?;
    let tx = parse_u64_any(b)?;
    Some(crate::schemas::SchemaAlkaneId { block, tx })
}

fn parse_u32_any(s: &str) -> Option<u32> {
    let t = s.trim();
    if let Some(h) = t.strip_prefix("0x") {
        u32::from_str_radix(h, 16).ok()
    } else {
        t.parse().ok()
    }
}

fn parse_u64_any(s: &str) -> Option<u64> {
    let t = s.trim();
    if let Some(h) = t.strip_prefix("0x") {
        u64::from_str_radix(h, 16).ok()
    } else {
        t.parse().ok()
    }
}

fn addr_prefix_suffix(addr: &str) -> (String, String) {
    let suffix_len = addr.len().min(ADDR_SUFFIX_LEN);
    let split_at = addr.len().saturating_sub(suffix_len);
    let prefix = addr[..split_at].to_string();
    let suffix = addr[split_at..].to_string();
    (prefix, suffix)
}

fn url_escape_component(raw: &str) -> String {
    // Minimal percent-encoding for URL query components.
    let mut out = String::with_capacity(raw.len());
    for &b in raw.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn pizza_tv_iframe_src(series_id: &str) -> String {
    let symbol = url_escape_component(series_id);
    let base = crate::config::get_explorer_pizza_tv_endpoint().trim_end_matches('/');
    format!(
        "{base}/?symbol={symbol}&timeframe=1d&type=mcap&pool=all&quote=usd&metaprotocol=alkanes&theme=espo"
    )
}

fn fmt_activity_amount(raw: u128) -> String {
    const MILLION: u128 = 1_000_000;
    const BILLION: u128 = 1_000_000_000;
    const TRILLION: u128 = 1_000_000_000_000;
    const QUADRILLION: u128 = 1_000_000_000_000_000;

    let units = raw / crate::explorer::pages::common::ALKANE_SCALE;
    if units < MILLION {
        return fmt_alkane_amount(raw);
    }

    let (unit, suffix) = if units >= QUADRILLION {
        (QUADRILLION, "Q")
    } else if units >= TRILLION {
        (TRILLION, "T")
    } else if units >= BILLION {
        (BILLION, "B")
    } else {
        (MILLION, "M")
    };

    let whole = units / unit;
    let rem = units % unit;
    let dec = (rem * 10) / unit;
    if dec == 0 { format!("{whole}{suffix}") } else { format!("{whole}.{dec}{suffix}") }
}

fn split_methods(
    inspection: Option<&crate::modules::essentials::utils::inspections::StoredInspectionResult>,
) -> (Vec<StoredInspectionMethod>, Vec<StoredInspectionMethod>) {
    let mut view = Vec::new();
    let mut write = Vec::new();
    if let Some(meta) = inspection.and_then(|i| i.metadata.as_ref()) {
        for method in &meta.methods {
            if method.name.starts_with("get_") {
                view.push(method.clone());
            } else {
                write.push(method.clone());
            }
        }
    }
    (view, write)
}

fn is_upgradeable_proxy(
    inspection: &crate::modules::essentials::utils::inspections::StoredInspectionResult,
) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    UPGRADEABLE_METHODS.iter().all(|(name, opcode)| {
        meta.methods
            .iter()
            .any(|m| m.name.eq_ignore_ascii_case(name) && m.opcode == *opcode)
    })
}

fn kv_row_key(alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4 + 8 + 2 + skey.len());
    v.push(0x01);
    v.extend_from_slice(&alk.block.to_be_bytes());
    v.extend_from_slice(&alk.tx.to_be_bytes());
    let len = u16::try_from(skey.len()).unwrap_or(u16::MAX);
    v.extend_from_slice(&len.to_be_bytes());
    if len as usize != skey.len() {
        v.extend_from_slice(&skey[..(len as usize)]);
    } else {
        v.extend_from_slice(skey);
    }
    v
}

fn decode_kv_implementation(raw: &[u8]) -> Option<SchemaAlkaneId> {
    if raw.len() < 32 {
        return None;
    }
    let block_bytes: [u8; 16] = raw[0..16].try_into().ok()?;
    let tx_bytes: [u8; 16] = raw[16..32].try_into().ok()?;
    let block = u128::from_le_bytes(block_bytes);
    let tx = u128::from_le_bytes(tx_bytes);
    if block > u32::MAX as u128 || tx > u64::MAX as u128 {
        return None;
    }
    Some(SchemaAlkaneId { block: block as u32, tx: tx as u64 })
}

fn proxy_target_from_db(
    alk: &SchemaAlkaneId,
    provider: &EssentialsProvider,
) -> Option<SchemaAlkaneId> {
    let lookup = |key| {
        provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: kv_row_key(alk, key),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| {
                if raw.len() >= 32 {
                    decode_kv_implementation(&raw[32..])
                } else {
                    decode_kv_implementation(&raw)
                }
            })
    };
    lookup(KV_KEY_IMPLEMENTATION).or_else(|| lookup(KV_KEY_BEACON))
}

fn resolve_proxy_target_recursive(
    start: &SchemaAlkaneId,
    provider: &EssentialsProvider,
) -> Option<SchemaAlkaneId> {
    let mut current = *start;
    let mut seen: HashSet<SchemaAlkaneId> = HashSet::new();
    for _ in 0..8 {
        let inspection = load_inspection(provider, &current).ok().flatten()?;
        if !is_upgradeable_proxy(&inspection) {
            return (current != *start).then_some(current);
        }
        let next = proxy_target_from_db(&current, provider)?;
        if !seen.insert(next) {
            return None;
        }
        current = next;
    }
    None
}

fn alkane_balance_chart_scripts() -> Markup {
    let script = r#"
<script>
(() => {
  const apiPath = '../api/alkane/balance-chart';
  const card = document.querySelector('[data-alkane-balance-chart]');
  if (!card) return;

  const alkane = card.dataset.alkane || '';
  if (!alkane) return;

  let activeAlkane = card.dataset.defaultAlkane || '';
  if (!activeAlkane) return;

  const root = card.querySelector('[data-alkane-balance-chart-root]');
  const loadingEl = card.querySelector('[data-alkane-balance-chart-loading]');
  const loadingTextEl = card.querySelector(
    '[data-address-chart-loading-text], [data-alkane-balance-chart-loading-text]'
  );
  const loadingSpinnerEl = card.querySelector('[data-alkane-balance-chart-loading-spinner]');
  const dropdownEl = card.querySelector('[data-alkane-balance-chart-token]');
  const optionNodes = Array.from(card.querySelectorAll('[data-alkane-balance-chart-token-option]'));
  const triggerLabelEl = card.querySelector('[data-alkane-balance-chart-token-trigger-label]');
  const triggerIconEl = card.querySelector('[data-alkane-balance-chart-token-trigger-icon]');
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
      canvas.setAttribute('aria-label', 'Alkane balance history');
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
      alkane,
      balance_alkane: activeAlkane,
      range
    });
    const res = await fetch(`${apiPath}?${params.toString()}`, {
      headers: { Accept: 'application/json' }
    });
    const data = await res.json();
    if (!data || !data.ok) return null;
    return data;
  };

  const updateCard = (data, canRender) => {
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
      updateCard(data, canRender);
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

fn inspect_scripts() -> Markup {
    let base_path_js = format!("{:?}", explorer_path("/"));
    let script = r#"
<script>
(() => {
  const basePath = __BASE_PATH__;
  const basePrefix = basePath === '/' ? '' : basePath;
  const root = document.querySelector('[data-alkane-inspect]');
  if (!root) return;
  const alkaneId = root.dataset.alkaneId || '';
  if (!alkaneId) return;
  const writeDefault = 'Providing inputs to simulate methods is not currently supported on espo';
  const blockInput = root.querySelector('[data-sim-block-input]');
  const currentBlockTag = () => {
    const value = blockInput && typeof blockInput.value === 'string' ? blockInput.value.trim() : '';
    return value || 'latest';
  };
  const clearValueNode = (node) => {
    if (!node) return;
    node.removeAttribute('data-cards');
    node.replaceChildren();
  };
  const setValueText = (node, text) => {
    if (!node) return;
    node.removeAttribute('data-cards');
    node.textContent = text;
  };
  const buildAlkaneIcon = (item) => {
    const wrap = document.createElement('span');
    wrap.className = 'alk-icon-wrap search-alk-icon';
    const img = document.createElement('span');
    img.className = 'alk-icon-img';
    if (item.icon_url) {
      img.style.backgroundImage = `url("${item.icon_url}")`;
    }
    const letter = document.createElement('span');
    letter.className = 'alk-icon-letter';
    const fallback = item.fallback_letter || (item.label || '').trim().charAt(0) || '?';
    letter.textContent = fallback.toUpperCase();
    wrap.appendChild(img);
    wrap.appendChild(letter);
    return wrap;
  };
  const buildAddressIcon = () => {
    const icon = document.createElement('span');
    icon.className = 'search-result-icon';
    icon.textContent = '@';
    return icon;
  };
  const buildCardIcon = (kind, item) => {
    if (kind === 'address') {
      return buildAddressIcon();
    }
    return buildAlkaneIcon(item);
  };
  const setValueCards = (node, items, overflow, kind) => {
    if (!node) return;
    node.dataset.cards = '1';
    node.replaceChildren();
    const wrap = document.createElement('div');
    wrap.className = 'search-results-items';
    items.forEach((item) => {
      const hasHref = Boolean(item.href);
      const entry = document.createElement(hasHref ? 'a' : 'div');
      entry.className = 'search-result';
      if (hasHref) {
        entry.setAttribute('href', item.href);
      } else {
        entry.dataset.disabled = '1';
      }
      const icon = buildCardIcon(kind, item);
      const label = document.createElement('span');
      label.className = 'search-result-label';
      label.textContent = item.label || item.value || '';
      entry.appendChild(icon);
      entry.appendChild(label);
      wrap.appendChild(entry);
    });
    node.appendChild(wrap);
    if (overflow && overflow > 0) {
      const note = document.createElement('div');
      note.className = 'alkane-overflow-note';
      note.textContent = `... plus ${overflow} other pools (too many to be displayed)`;
      node.appendChild(note);
    }
  };

  const toggles = root.querySelectorAll('[data-alkane-method]');
  const resetWrite = (details) => {
    const button = details.querySelector('.alkane-method-btn');
    if (button) {
      button.style.display = '';
    }
    const resultWrap = details.querySelector('[data-sim-result]');
    const valueNode = details.querySelector('[data-sim-value]');
    if (resultWrap && valueNode) {
      resultWrap.dataset.status = 'idle';
      setValueText(valueNode, valueNode.dataset.defaultText || writeDefault);
    }
  };

  const runSim = async (details) => {
    if (!details || details.dataset.loading === '1') return;
    const opcode = details.dataset.alkaneOpcode || details.dataset.opcode;
    const returnsType = details.dataset.alkaneReturns || '';
    if (!opcode) return;
    const resultWrap = details.querySelector('[data-sim-result]');
    const valueNode = details.querySelector('[data-sim-value]');
    if (!resultWrap || !valueNode) return;

    details.dataset.loading = '1';
    resultWrap.dataset.status = 'loading';
    setValueText(valueNode, 'Loading...');

    try {
      const payload = { alkane: alkaneId, opcode: Number(opcode), block: currentBlockTag() };
      if (returnsType) {
        payload.returns = returnsType;
      }
      const res = await fetch(`${basePrefix}/api/alkane/simulate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!data || !data.ok) {
        const msg = data && data.error ? data.error : 'Simulation failed';
        resultWrap.dataset.status = 'failure';
        setValueText(valueNode, msg);
      } else {
        const status = data.status || 'success';
        resultWrap.dataset.status = status;
        if (Array.isArray(data.alkanes) && data.alkanes.length) {
          setValueCards(valueNode, data.alkanes, data.alkanes_overflow || 0, 'alkane');
        } else if (Array.isArray(data.addresses) && data.addresses.length) {
          setValueCards(valueNode, data.addresses, 0, 'address');
        } else {
          setValueText(valueNode, data.data || 'No data');
        }
      }
    } catch (_) {
      resultWrap.dataset.status = 'failure';
      setValueText(valueNode, 'Simulation failed');
    } finally {
      details.dataset.loading = '0';
    }
  };

  toggles.forEach((details) => {
    details.addEventListener('toggle', async () => {
      if (!details.open) {
        if (details.dataset.alkaneView !== '1') {
          resetWrite(details);
        }
        return;
      }
      if (details.dataset.alkaneView !== '1') return;
      await runSim(details);
    });
  });

  const buttons = root.querySelectorAll('.alkane-method-btn');
  buttons.forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      const details = button.closest('details');
      if (!details) return;
      details.open = true;
      button.style.display = 'none';
      await runSim(details);
    });
  });
})();
</script>
"#;
    PreEscaped(script.replace("__BASE_PATH__", &base_path_js))
}

#[allow(dead_code)]
fn chart_scripts() -> Markup {
    let base_path_js = format!("{:?}", explorer_path("/"));
    let script = r#"
<script>
(() => {
  const basePath = __BASE_PATH__;
  const basePrefix = basePath === '/' ? '' : basePath;
  const card = document.querySelector('[data-alkane-chart]');
  if (!card) return;
  const alkaneId = card.dataset.alkaneId || '';
  if (!alkaneId) return;

  const root = card.querySelector('[data-alkane-chart-root]');
  const overview = card.closest('[data-alkane-overview]');
  const grid = overview ? overview.querySelector('[data-alkane-chart-grid]') : null;
  const head = overview ? overview.querySelector('[data-alkane-chart-head]') : null;
  const priceEl = card.querySelector('[data-alkane-price]');
  const changeEl = card.querySelector('[data-alkane-change]');
  const rangeEl = card.querySelector('[data-alkane-range]');
  const loadingEl = card.querySelector('[data-alkane-loading]');
  const tabs = Array.from(card.querySelectorAll('[data-range]'));
  const defaultRange = (card.dataset.defaultRange || '3m').toLowerCase();
  let activeRange = defaultRange;
  let source = null;
  let quote = null;
  let chart = null;
  let canvas = null;
  let loading = false;

  const rangeLabel = (range) => {
    switch (range) {
      case '4h':
        return 'Past 4 hours';
      case '1d':
        return 'Past 24 hours';
      case '1w':
        return 'Past 7 days';
      case '1m':
        return 'Past 30 days';
      case '3m':
      default:
        return 'Past 3 months';
    }
  };

  const formatUsd = (value) => {
    if (!Number.isFinite(value)) return '$0.00';
    const digits = value >= 1 ? 2 : 6;
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: digits
    }).format(value);
  };

  const formatPct = (value) => {
    if (!Number.isFinite(value)) return '0.00%';
    return `${value.toFixed(2)}%`;
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

  const resolveColor = (cssVar, fallback) => {
    const value = getComputedStyle(document.documentElement).getPropertyValue(cssVar).trim();
    return value || fallback;
  };

  const ensureCanvas = () => {
    if (!root) return null;
    if (!canvas) {
      canvas = document.createElement('canvas');
      canvas.setAttribute('aria-label', 'Market chart');
      canvas.setAttribute('role', 'img');
      root.replaceChildren(canvas);
    }
    return canvas.getContext('2d');
  };

  const renderChart = (points, isUp) => {
    if (!root || !window.Chart) return;
    const ctx = ensureCanvas();
    if (!ctx) return;
    const lineColor = isUp
      ? resolveColor('--chart-green', '#33e183')
      : resolveColor('--chart-red', '#ff5555');
    const tooltipBg = resolveColor('--panel3', '#1f2228');
    const tooltipBorder = resolveColor('--panel2', '#353742');
    const tooltipText = resolveColor('--text', '#ffffff');
    const labels = points.map((p) => p.ts);
    const values = points.map((p) => p.close);

    if (chart) {
      chart.data.labels = labels;
      chart.data.datasets[0].data = values;
      chart.data.datasets[0].borderColor = lineColor;
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
            fill: false
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
            enabled: true,
            displayColors: false,
            backgroundColor: tooltipBg,
            borderColor: tooltipBorder,
            borderWidth: 0,
            titleColor: tooltipText,
            bodyColor: tooltipText,
            callbacks: {
              title: (items) => {
                const ts = Number(items && items[0] && items[0].label);
                if (!Number.isFinite(ts)) return '';
                return new Intl.DateTimeFormat('en-US', {
                  dateStyle: 'medium',
                  timeStyle: 'short'
                }).format(new Date(ts * 1000));
              },
              label: (item) => {
                const value =
                  item && item.parsed && typeof item.parsed.y === 'number'
                    ? item.parsed.y
                    : item.parsed;
                return formatUsd(Number(value));
              }
            }
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
          y: { display: false }
        },
        layout: {
          padding: { top: 48, right: 12, bottom: 16, left: 12 }
        }
      }
    });
  };

  const fetchRange = async (range) => {
    const params = new URLSearchParams({ alkane: alkaneId, range });
    if (source) params.set('source', source);
    if (quote) params.set('quote', quote);
    const res = await fetch(`${basePrefix}/api/alkane/chart?${params.toString()}`);
    const data = await res.json();
    if (!data || !data.ok) return null;
    source = data.source || source;
    quote = data.quote || quote;
    return data;
  };

  const setChartHidden = (hidden) => {
    if (hidden) {
      card.style.display = 'none';
    } else {
      card.style.removeProperty('display');
    }
    if (head) {
      if (hidden) {
        head.dataset.chartHidden = '1';
      } else {
        delete head.dataset.chartHidden;
      }
    }
    if (grid) {
      if (hidden) {
        grid.dataset.chartHidden = '1';
      } else {
        delete grid.dataset.chartHidden;
      }
    }
  };

  const updateCard = (data, range, canRender) => {
    if (!data || !Array.isArray(data.candles) || data.candles.length === 0) {
      setChartHidden(true);
      if (chart) {
        chart.destroy();
        chart = null;
      }
      if (canvas) {
        canvas.remove();
        canvas = null;
      }
      return;
    }
    const points = data.candles.slice().sort((a, b) => a.ts - b.ts);
    const first = points[0].close;
    const last = points[points.length - 1].close;
    const change = points.length > 1 && first ? ((last - first) / first) * 100 : 0;
    const isUp = change >= 0;
    card.dataset.tone = isUp ? 'up' : 'down';
    if (priceEl) priceEl.textContent = formatUsd(last);
    if (changeEl) changeEl.textContent = formatPct(change);
    if (rangeEl) rangeEl.textContent = rangeLabel(range);
    if (loadingEl) loadingEl.style.display = 'none';
    setChartHidden(false);
    if (canRender) {
      renderChart(points, isUp);
    } else if (loadingEl) {
      loadingEl.textContent = 'Chart unavailable';
      loadingEl.style.display = '';
    }
  };

  const load = async (range) => {
    if (loading) return;
    loading = true;
    if (loadingEl) loadingEl.style.display = '';
    try {
      const data = await fetchRange(range);
      if (!data) {
        setChartHidden(true);
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
      if (loadingEl) {
        loadingEl.textContent = 'Chart unavailable';
        loadingEl.style.display = '';
      }
    } finally {
      loading = false;
    }
  };

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
  load(activeRange);
})();
</script>
"#;
    PreEscaped(script.replace("__BASE_PATH__", &base_path_js))
}
