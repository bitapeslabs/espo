use maud::{Markup, html};

use crate::explorer::components::svg_assets::{icon_github, icon_heart, logo_espo};

pub fn footer() -> Markup {
    html! {
        footer class="site-footer" {
            div class="app" {
                div class="footer-row" {
                    div class="footer-left" {
                        div class="footer-link" {
                            "Created with "
                            span class="footer-heart" { (icon_heart()) }
                            " by "
                            a class="footer-link-accent" href="https://x.com/mork1e" target="_blank" rel="noopener noreferrer" { "mork1e" }
                        }
                        div class="footer-link" {
                            "Design inspired by "
                            a class="footer-link-accent" href="https://ordiscan.com" target="_blank" rel="noopener noreferrer" { "Ordiscan" }
                        }
                    }
                    div class="footer-center" {
                        div class="footer-brand"{
                            (logo_espo())
                            span class="footer-brand-text" { "Espo" }
                        }
                    }
                    div class="footer-right" {
                        a class="footer-icon" href="https://github.com/bitapeslabs/espo" target="_blank" rel="noopener noreferrer" aria-label="GitHub repository" {
                            (icon_github())
                        }
                    }
                }
            }
        }
    }
}
