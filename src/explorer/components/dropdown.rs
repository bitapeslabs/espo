use maud::{Markup, html};

use crate::explorer::components::svg_assets::{icon_dropdown_caret, icon_dropdown_check};

pub struct DropdownItem {
    pub label: String,
    pub href: String,
    pub icon: Option<Markup>,
    pub selected: bool,
}

pub struct DropdownProps {
    pub label: Option<String>,
    pub selected_icon: Option<Markup>,
    pub items: Vec<DropdownItem>,
    pub aria_label: Option<String>,
}

pub fn dropdown(props: DropdownProps) -> Markup {
    let trigger_label = props.label.clone().unwrap_or_default();
    let aria_label = props
        .aria_label
        .clone()
        .or_else(|| (!trigger_label.is_empty()).then_some(trigger_label.clone()))
        .unwrap_or_else(|| "Dropdown".to_string());

    html! {
        div class="dropdown" data-dropdown="" data-open="" {
            button class="dropdown-trigger" type="button" aria-label=(aria_label) aria-haspopup="true" aria-expanded="false" data-dropdown-toggle="" {
                @if let Some(icon) = props.selected_icon {
                    span class="dropdown-icon dropdown-trigger-icon" { (icon) }
                }
                @if !trigger_label.is_empty() {
                    span class="dropdown-label" { (trigger_label) }
                }
                span class="dropdown-caret" { (icon_dropdown_caret()) }
            }
            div class="dropdown-panel" role="menu" aria-hidden="true" {
                @for item in props.items {
                    @let item_class = if item.selected { "dropdown-item selected" } else { "dropdown-item" };
                    a class=(item_class) href=(item.href) role="menuitem" {
                        @if let Some(icon) = item.icon {
                            span class="dropdown-icon" { (icon) }
                        } @else if item.selected {
                            span class="dropdown-icon" { (icon_dropdown_check()) }
                        }
                        span class="dropdown-label" { (item.label) }
                    }
                }
            }
        }
    }
}
