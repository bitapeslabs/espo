use maud::{Markup, html};

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
