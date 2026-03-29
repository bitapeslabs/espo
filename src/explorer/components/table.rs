use maud::{Markup, html};

use crate::explorer::components::svg_assets::icon_user;
use crate::explorer::components::tx_view::icon_bg_style;
use crate::explorer::paths::explorer_path;

#[derive(Clone, Debug)]
pub struct AlkaneTableRow {
    pub id: String,
    pub name: String,
    pub holders: u64,
    pub icon_url: String,
    pub fallback: char,
    pub creation_height: u32,
    pub creation_txid: String,
}

/// Table renderer with fixed width last column used for holders lists.
pub fn holders_table(headers: &[&str], rows: Vec<Vec<Markup>>) -> Markup {
    // assumes every row has the same number of cells as headers
    let n = headers.len();

    html! {
        table class="holders_table table" {
            colgroup {
                @for i in 0..n {
                    @if i == n - 1 {
                        col style="width: 300px;";
                    } @else {
                        col;
                    }
                }
            }

            thead {
                tr {
                    @for h in headers {
                        th { (h) }
                    }
                }
            }

            tbody {
                @for row in rows {
                    tr {
                        @for cell in row {
                            td { (cell) }
                        }
                    }
                }
            }
        }
    }
}

/// Simple table renderer without column sizing.
pub fn table(headers: &[&str], rows: Vec<Vec<Markup>>) -> Markup {
    html! {
        table class="table" {
            thead {
                tr {
                    @for h in headers {
                        th { (h) }
                    }
                }
            }
            tbody {
                @for row in rows {
                    tr {
                        @for cell in row {
                            td { (cell) }
                        }
                    }
                }
            }
        }
    }
}

pub fn alkanes_table(
    rows: &[AlkaneTableRow],
    show_header: bool,
    show_creation_block: bool,
    show_holder_icon: bool,
) -> Markup {
    let table_class = if show_creation_block {
        "table holders_table alkanes-table has-block"
    } else {
        "table holders_table alkanes-table"
    };
    html! {
        table class=(table_class) {
            colgroup {
                @if show_creation_block {
                    col style="width: 44%;";
                    col style="width: 12%;";
                    col style="width: 30%;";
                    col style="width: 14%;";
                } @else {
                    col style="width: 56%;";
                    col style="width: 28%;";
                    col style="width: 16%;";
                }
            }
            @if show_header {
                thead {
                    tr {
                        th { "Alkane" }
                        @if show_creation_block {
                            th { "Creation block" }
                        }
                        th { "Creation tx" }
                        th class="right" { "Holders" }
                    }
                }
            }
            tbody {
                @for row in rows {
                    tr {
                        td class="alkane-main-cell" {
                            div class="alk-line" {
                                div class="alk-icon-wrap" aria-hidden="true" {
                                    span class="alk-icon-img" style=(icon_bg_style(&row.icon_url)) {}
                                    span class="alk-icon-letter" { (row.fallback) }
                                }
                                div class="alkane-meta" {
                                    a class="alk-sym link mono alkane-name-link" href=(explorer_path(&format!("/alkane/{}", row.id))) { (row.name.clone()) }
                                    div class="muted mono alkane-id" { (row.id.clone()) }
                                }
                            }
                        }
                        @if show_creation_block {
                            td class="alkane-block-cell" {
                                a class="link mono" href=(explorer_path(&format!("/block/{}", row.creation_height))) { (row.creation_height) }
                            }
                        }
                        td class="mono alkane-tx-cell" {
                            a class="link ellipsis alkane-txid" href=(explorer_path(&format!("/tx/{}", row.creation_txid))) { (&row.creation_txid) }
                        }
                        td class="alkane-holders-cell" {
                            span class="mono holders-count" {
                                @if show_holder_icon {
                                    (icon_user())
                                }
                                (row.holders)
                            }
                        }
                    }
                }
            }
        }
    }
}
