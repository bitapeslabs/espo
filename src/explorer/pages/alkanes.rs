use crate::runtime::state_at::StateAt;
use axum::extract::{Query, State};
use axum::response::Html;
use borsh::BorshDeserialize;
use maud::{Markup, html};
use serde::Deserialize;

use crate::explorer::components::dropdown::{DropdownItem, DropdownProps, dropdown};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::{
    icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::{AlkaneTableRow, alkanes_table};
use crate::explorer::components::tx_view::alkane_icon_url;
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::{
    EssentialsTable, GetCreationRecordsOrderedPageParams, GetHoldersOrderedPageParams,
    HoldersCountEntry, load_creation_record,
};
use crate::modules::essentials::utils::inspections::AlkaneCreationRecord;

#[derive(Deserialize)]
pub struct PageQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub order: Option<String>,
    pub dir: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortField {
    Age,
    Holders,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortDir {
    Desc,
    Asc,
}

impl SortField {
    fn from_query(order: Option<&str>) -> Self {
        match order {
            Some("holders") => Self::Holders,
            Some("holders_desc") | Some("holders_asc") => Self::Holders,
            _ => Self::Age,
        }
    }

    fn as_query(self) -> &'static str {
        match self {
            Self::Age => "age",
            Self::Holders => "holders",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Age => "Age",
            Self::Holders => "Holder Count",
        }
    }
}

impl SortDir {
    fn from_query(order: Option<&str>, dir: Option<&str>) -> Self {
        match order {
            Some("age_asc") | Some("holders_asc") => Self::Asc,
            Some("age_desc") | Some("holders_desc") => Self::Desc,
            _ => match dir {
                Some("asc") => Self::Asc,
                _ => Self::Desc,
            },
        }
    }

    fn as_query(self) -> &'static str {
        match self {
            Self::Desc => "desc",
            Self::Asc => "asc",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Desc => "Descending",
            Self::Asc => "Ascending",
        }
    }
}

fn alkanes_url(page: usize, limit: usize, field: SortField, dir: SortDir) -> String {
    explorer_path(&format!(
        "/alkanes?page={page}&limit={limit}&order={}&dir={}",
        field.as_query(),
        dir.as_query()
    ))
}

pub async fn alkanes_page(
    State(state): State<ExplorerState>,
    Query(q): Query<PageQuery>,
) -> Html<String> {
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 50);
    let offset = limit.saturating_mul(page.saturating_sub(1));
    let provider = state.essentials_provider();
    let field = SortField::from_query(q.order.as_deref());
    let dir = SortDir::from_query(q.order.as_deref(), q.dir.as_deref());
    let table = EssentialsTable::new(&state.essentials_mdb);

    let total: u64 = state
        .essentials_mdb
        .get(&table.alkane_creation_count_key())
        .ok()
        .flatten()
        .and_then(|b| {
            if b.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&b);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);

    let holders_for = |rec: &AlkaneCreationRecord| {
        state
            .essentials_mdb
            .get(&table.holders_count_key(&rec.alkane))
            .ok()
            .flatten()
            .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
            .map(|hc| hc.count)
            .unwrap_or(0)
    };

    let build_row = |rec: &AlkaneCreationRecord, holders: u64| {
        let id = format!("{}:{}", rec.alkane.block, rec.alkane.tx);
        let name = rec
            .names
            .first()
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Unnamed".to_string());
        let icon_url = alkane_icon_url(&rec.alkane, &state.essentials_mdb);
        let fallback = if name == "Unnamed" {
            '?'
        } else {
            name.chars()
                .find(|c| !c.is_whitespace())
                .map(|c| c.to_ascii_uppercase())
                .unwrap_or('?')
        };
        let creation_txid = hex::encode(rec.txid);

        AlkaneTableRow {
            id,
            name,
            holders,
            icon_url,
            fallback,
            creation_height: rec.creation_height,
            creation_txid,
        }
    };

    let mut rows: Vec<AlkaneTableRow> = Vec::new();
    match (field, dir) {
        (SortField::Age, SortDir::Desc) => {
            let records = provider
                .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                    blockhash: StateAt::Latest,
                    offset: offset as u64,
                    limit: limit as u64,
                    desc: true,
                })
                .map(|res| res.records)
                .unwrap_or_default();
            for rec in records {
                let holders = holders_for(&rec);
                rows.push(build_row(&rec, holders));
            }
        }
        (SortField::Age, SortDir::Asc) => {
            let records = provider
                .get_creation_records_ordered_page(GetCreationRecordsOrderedPageParams {
                    blockhash: StateAt::Latest,
                    offset: offset as u64,
                    limit: limit as u64,
                    desc: false,
                })
                .map(|res| res.records)
                .unwrap_or_default();
            for rec in records {
                let holders = holders_for(&rec);
                rows.push(build_row(&rec, holders));
            }
        }
        (SortField::Holders, SortDir::Desc) => {
            let ids = provider
                .get_holders_ordered_page(GetHoldersOrderedPageParams {
                    blockhash: StateAt::Latest,
                    offset: offset as u64,
                    limit: limit as u64,
                    desc: true,
                })
                .map(|res| res.ids)
                .unwrap_or_default();
            for alk in ids {
                let Some(rec) = load_creation_record(&state.essentials_mdb, &alk).ok().flatten()
                else {
                    continue;
                };
                let holders = holders_for(&rec);
                rows.push(build_row(&rec, holders));
            }
        }
        (SortField::Holders, SortDir::Asc) => {
            let ids = provider
                .get_holders_ordered_page(GetHoldersOrderedPageParams {
                    blockhash: StateAt::Latest,
                    offset: offset as u64,
                    limit: limit as u64,
                    desc: false,
                })
                .map(|res| res.ids)
                .unwrap_or_default();
            for alk in ids {
                let Some(rec) = load_creation_record(&state.essentials_mdb, &alk).ok().flatten()
                else {
                    continue;
                };
                let holders = holders_for(&rec);
                rows.push(build_row(&rec, holders));
            }
        }
    }

    let display_start = if total > 0 && offset < total as usize { (offset + 1) as u64 } else { 0 };
    let display_end = (offset as u64 + rows.len() as u64).min(total);
    let has_prev = page > 1;
    let has_next = (offset as u64 + rows.len() as u64) < total;
    let last_page =
        if total > 0 { ((total + limit as u64 - 1) / limit as u64).max(1) as usize } else { 1 };
    let show_creation_block = has_prev || has_next;

    let field_options = [SortField::Age, SortField::Holders];
    let field_dropdown = dropdown(DropdownProps {
        label: Some(field.label().to_string()),
        selected_icon: None,
        items: field_options
            .iter()
            .map(|opt| DropdownItem {
                label: opt.label().to_string(),
                href: alkanes_url(1, limit, *opt, dir),
                icon: None,
                selected: *opt == field,
            })
            .collect(),
        aria_label: Some("Order alkanes".to_string()),
    });
    let dir_options = [SortDir::Asc, SortDir::Desc];
    let dir_dropdown = dropdown(DropdownProps {
        label: Some(dir.label().to_string()),
        selected_icon: None,
        items: dir_options
            .iter()
            .map(|opt| DropdownItem {
                label: opt.label().to_string(),
                href: alkanes_url(1, limit, field, *opt),
                icon: None,
                selected: *opt == dir,
            })
            .collect(),
        aria_label: Some("Order direction".to_string()),
    });

    let table: Markup = if rows.is_empty() {
        html! { p class="muted" { "No alkanes found." } }
    } else {
        html! { div class="alkanes-card" { (alkanes_table(&rows, true, show_creation_block, true)) } }
    };

    layout_with_meta(
        "Alkanes",
        "/alkanes",
        None,
        html! {
            div class="row" {
                h1 class="h1" { "All Alkanes" }
                div class="order-control" {
                    span class="muted" { "Order by:" }
                    (field_dropdown)
                    (dir_dropdown)
                }
            }
            (table)
            div class="pager" {
                @if has_prev {
                    a class="pill iconbtn" href=(alkanes_url(1, limit, field, dir)) aria-label="First page" { (icon_skip_left()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                }
                @if has_prev {
                    a class="pill iconbtn" href=(alkanes_url(page - 1, limit, field, dir)) aria-label="Previous page" { (icon_left()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                }
                span class="pager-meta muted" { "Showing "
                    (display_start)
                    @if total > 0 {
                        "-"
                        (display_end)
                    }
                    " / "
                    (total)
                }
                @if has_next {
                    a class="pill iconbtn" href=(alkanes_url(page + 1, limit, field, dir)) aria-label="Next page" { (icon_right()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                }
                @if has_next {
                    a class="pill iconbtn" href=(alkanes_url(last_page, limit, field, dir)) aria-label="Last page" { (icon_skip_right()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                }
            }
        },
    )
}
