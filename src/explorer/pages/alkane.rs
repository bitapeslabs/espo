use axum::extract::{Path, Query, State};
use axum::response::Html;
use maud::{Markup, html};
use serde::Deserialize;

use crate::explorer::components::layout::layout;
use crate::explorer::components::table::table;
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
    let (total, holders) =
        get_holders_for_alkane(&state.essentials_mdb, alk, page, limit).unwrap_or((0, Vec::new()));
    let alk_str = format!("{}:{}", alk.block, alk.tx);

    let has_prev = page > 1;
    let has_next = page.saturating_mul(limit) < total;

    let rows: Vec<Vec<Markup>> = holders
        .into_iter()
        .map(|h| {
            vec![
                html! { a class="link mono" href=(format!("/address/{}", h.address)) { (h.address) } },
                html! { span class="right mono" { (fmt_alkane_amount(h.amount)) } },
            ]
        })
        .collect();

    let table_markup = if rows.is_empty() {
        html! { p class="muted" { "No holders." } }
    } else {
        table(&["Address", "Balance"], rows)
    };

    layout(
        &format!("Alkane {alk_str}"),
        html! {
            div class="row" {
                h1 class="h1" { "Alkane " span class="mono" { (alk_str.clone()) } }
                div class="pillrow" {
                    @if has_prev {
                        a class="pill" href=(format!("/alkane/{alk_str}?page={}&limit={}", page - 1, limit)) { "Prev" }
                    }
                    @if has_next {
                        a class="pill" href=(format!("/alkane/{alk_str}?page={}&limit={}", page + 1, limit)) { "Next" }
                    }
                    span class="muted" { "Page " (page) " • " (total) " holders" }
                }
            }

            div class="card" { (table_markup) }
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
