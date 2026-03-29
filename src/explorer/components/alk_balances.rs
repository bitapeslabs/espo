use std::collections::HashMap;

use maud::{Markup, html};

use crate::explorer::components::tx_view::{AlkaneMetaCache, alkane_meta, icon_bg_style};
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::BalanceEntry;
use crate::runtime::mdb::Mdb;

pub fn render_alkane_balance_cards(entries: &[BalanceEntry], essentials_mdb: &Mdb) -> Markup {
    if entries.is_empty() {
        return html! {};
    }

    let mut cache: AlkaneMetaCache = HashMap::new();

    html! {
        div class="io-alkanes io-alkanes-grid" {
            @for be in entries {
                @let meta = alkane_meta(&be.alkane, &mut cache, essentials_mdb);
                @let alk = format!("{}:{}", be.alkane.block, be.alkane.tx);
                @let fallback_letter = meta.name.fallback_letter();
                div class="alk-card" {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            span class="alk-icon-img" style=(icon_bg_style(&meta.icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_alkane_amount(be.amount)) }
                        a class="alk-sym link mono" href=(explorer_path(&format!("/alkane/{alk}"))) { (meta.name.value.clone()) }
                    }
                }
            }
        }
    }
}
