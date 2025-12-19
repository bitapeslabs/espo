use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use maud::{DOCTYPE, Markup, html};

use crate::explorer::components::svg_assets::logo_espo;

const STYLE_CSS: &str = include_str!("../assets/style.css");

pub async fn style() -> impl IntoResponse {
    (StatusCode::OK, [(CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

pub fn layout(title: &str, content: Markup) -> Html<String> {
    let markup = html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                link rel="stylesheet" href="/static/style.css";
            }
            body {
                header class="topbar" {
                    div class="app" {
                        nav class="nav" {
                            a class="brand" href="/" {
                                (logo_espo())
                                span class="brand-text" { "EspoExplorer" }
                            }
                            a class="navlink" href="/" { "Blocks" }
                        }
                    }
                }
                main class="app" {
                    (content)
                }
            }
        }
    };
    Html(markup.into_string())
}
