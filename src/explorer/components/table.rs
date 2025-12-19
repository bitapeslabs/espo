use maud::{Markup, html};

/// Simple reusable table renderer.
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
