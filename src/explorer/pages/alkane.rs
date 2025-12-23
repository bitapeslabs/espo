use axum::extract::{Path, Query, State};
use axum::response::Html;
use maud::{Markup, html};
use serde::Deserialize;
use hex;

use crate::explorer::components::header::header_scripts;
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::holders_table;
use crate::explorer::components::tx_view::{
    AlkaneMetaCache, alkane_icon_fallback_url, alkane_meta, icon_onerror,
};
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::storage::load_creation_record;
use crate::modules::essentials::utils::balances::get_holders_for_alkane;

const ADDR_SUFFIX_LEN: usize = 8;

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
}

impl AlkaneTab {
    fn from_query(raw: Option<&str>) -> Self {
        match raw {
            Some("inspect") => AlkaneTab::Inspect,
            _ => AlkaneTab::Holders,
        }
    }
}

pub async fn alkane_page(
    State(state): State<ExplorerState>,
    Path(alkane_raw): Path<String>,
    Query(q): Query<PageQuery>,
) -> Html<String> {
    let Some(alk) = parse_alkane_id(&alkane_raw) else {
        return layout(
            "Alkane",
            html! { p class="error" { "Invalid alkane id; expected \"<block>:<tx>\"." } },
        );
    };

    let tab = AlkaneTab::from_query(q.tab.as_deref());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let alk_str = format!("{}:{}", alk.block, alk.tx);
    let mut kv_cache: AlkaneMetaCache = Default::default();
    let meta = alkane_meta(&alk, &mut kv_cache, &state.essentials_mdb);
    let display_name = meta.name.value.clone();
    let fallback_letter = meta.name.fallback_letter();
    let fallback_icon_url = alkane_icon_fallback_url(&alk);
    let page_title = if meta.name.known && display_name != alk_str {
        format!("Alkane {display_name} ({alk_str})")
    } else {
        format!("Alkane {alk_str}")
    };

    let creation_record = load_creation_record(&state.essentials_mdb, &alk).ok().flatten();
    let creation_ts = creation_record.as_ref().map(|r| r.creation_timestamp as u64);
    let creation_height = creation_record.as_ref().map(|r| r.creation_height);
    let creation_txid = creation_record.as_ref().map(|r| hex::encode(r.txid));

    let (total, circulating_supply, holders) =
        get_holders_for_alkane(&state.essentials_mdb, alk, page, limit)
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

    let rows: Vec<Vec<Markup>> = holders
        .into_iter()
        .enumerate()
        .map(|(idx, h)| {
            let rank = off + idx + 1;
            let (addr_prefix, addr_suffix) = addr_prefix_suffix(&h.address);
            vec![
                html! {
                    a class="link mono addr-inline" href=(format!("/address/{}", h.address)) {
                        span class="addr-rank" { (format!("{rank}.")) }
                        span class="addr-prefix" { (addr_prefix) }
                        span class="addr-suffix" { (addr_suffix) }
                    }
                },
                html! {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            img class="alk-icon-img" src=(icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror=(icon_onerror(&fallback_icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_alkane_amount(h.amount)) }
                        a class="alk-sym link mono" href=(format!("/alkane/{alk_str}")) { (coin_label.clone()) }
                    }
                },
            ]
        })
        .collect();

    let table_markup = if rows.is_empty() {
        html! { div class="alkane-panel" { p class="muted" { "No holders." } } }
    } else {
        html! {
            div class="alkane-panel alkane-holders-card" {
                (holders_table(&["Address", "Balance"], rows))
            }
        }
    };

    layout(
        &page_title,
        html! {
            div class="alkane-page" {
                div class="alkane-hero-card" {
                    div class="alk-icon-wrap alk-icon-lg" aria-hidden="true" {
                        img class="alk-icon-img" src=(meta.icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror=(icon_onerror(&fallback_icon_url)) {}
                        span class="alk-icon-letter" { (fallback_letter) }
                    }
                    div class="alkane-hero-text" {
                        span class="alkane-tag" { "TOKEN" }
                        h1 class="alkane-hero-title" { (display_name.clone()) }
                        span class="alkane-hero-id mono" { (alk_str.clone()) }
                    }
                }

                section class="alkane-section" {
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
                                    a class="alkane-stat-value link mono" href=(format!("/tx/{txid}")) { (short_hex(txid)) }
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
                                    a class="alkane-stat-value link" href=(format!("/block/{h}")) { (h) }
                                }
                            } @else {
                                div class="alkane-stat-line" {
                                    span class="alkane-stat-value muted" { "Unknown" }
                                }
                            }
                        }
                    }
                }

                section class="alkane-section" {
                    div class="alkane-tabs" {
                        div class="alkane-tab-list" {
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Holders { " active" } else { "" }))
                                href=(format!("/alkane/{alk_str}?page={page}&limit={limit}")) { "Holders" }
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Inspect { " active" } else { "" }))
                                href=(format!("/alkane/{alk_str}?tab=inspect&page={page}&limit={limit}")) { "Inspect contract" }
                        }
                        div class="alkane-tab-panel" {
                            @if tab == AlkaneTab::Holders {
                                (table_markup)
                                div class="pager" {
                                    @if has_prev {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page=1&limit={limit}")) aria-label="First page" {
                                            (icon_skip_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                                    }
                                    @if has_prev {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", page - 1)) aria-label="Previous page" {
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
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", page + 1)) aria-label="Next page" {
                                            (icon_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                                    }
                                    @if has_next {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", last_page)) aria-label="Last page" {
                                            (icon_skip_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                                    }
                                }
                            } @else {
                                div class="alkane-panel alkane-panel-empty" { }
                            }
                        }
                    }
                }
            }
            (header_scripts())
        },
    )
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
