use axum::extract::{Path, Query, State};
use axum::response::Html;
use maud::{Markup, html};
use serde::Deserialize;

use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::table;
use crate::explorer::components::tx_view::{AlkaneKvCache, alkane_meta};
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::utils::balances::get_holders_for_alkane;

#[derive(Deserialize)]
pub struct PageQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
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

    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let alk_str = format!("{}:{}", alk.block, alk.tx);
    let mut kv_cache: AlkaneKvCache = Default::default();
    let meta = alkane_meta(&alk, &mut kv_cache, &state.essentials_mdb);
    let display_name = meta.name.value.clone();
    let fallback_letter = meta.name.fallback_letter();
    let page_title = if meta.name.known && display_name != alk_str {
        format!("Alkane {display_name} ({alk_str})")
    } else {
        format!("Alkane {alk_str}")
    };

    let (total, holders) =
        get_holders_for_alkane(&state.essentials_mdb, alk, page, limit).unwrap_or((0, Vec::new()));
    let off = limit.saturating_mul(page.saturating_sub(1));
    let holders_len = holders.len();
    let has_prev = page > 1;
    let has_next = off + holders_len < total;
    let display_start = if total > 0 && off < total { off + 1 } else { 0 };
    let display_end = (off + holders_len).min(total);
    let last_page = if total > 0 { (total + limit - 1) / limit } else { 1 };
    let icon_url = meta.icon_url.clone();
    let coin_label = meta.name.value.clone();

    let rows: Vec<Vec<Markup>> = holders
        .into_iter()
        .map(|h| {
            vec![
                html! { a class="link mono" href=(format!("/address/{}", h.address)) { (h.address) } },
                html! {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            img class="alk-icon-img" src=(icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror="this.remove()" {}
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
        html! { p class="muted" { "No holders." } }
    } else {
        table(&["Address", "Balance"], rows)
    };

    layout(
        &page_title,
        html! {
            div class="row" {
                div class="trace-contract-row alkane-hero" {
                    div class="alk-icon-wrap" aria-hidden="true" {
                        img class="alk-icon-img" src=(meta.icon_url.clone()) alt=(meta.symbol.clone()) loading="lazy" onerror="this.remove()" {}
                        span class="alk-icon-letter" { (fallback_letter) }
                    }
                    div class="trace-contract-meta" {
                        h1 class="h1" { (display_name.clone()) }
                        span class="muted mono" { (alk_str.clone()) }
                    }
                }
            }

            div class="card" {
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
            }
        },
    )
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
