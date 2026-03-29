mod api;
pub mod components;
pub mod consts;
pub mod i18n;
mod pages;
pub mod paths;

use std::net::SocketAddr;

use api::{
    address_chart, alkane_balance_chart, alkane_chart, carousel_blocks, search_guess,
    simulate_contract,
};
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use i18n::ExplorerLanguage;
use pages::address::address_page;
use pages::alkane::alkane_page;
use pages::alkanes::alkanes_page;
use pages::block::block_page;
use pages::home::home_page;
use pages::search::search;
use pages::state::ExplorerState;
use pages::tx::tx_page;
use tokio::net::TcpListener;

use crate::config::{
    get_espo_next_height, get_explorer_base_path, get_explorer_networks, get_network,
};
use components::layout::{favicon, style};
use paths::with_language;

pub fn explorer_router(state: ExplorerState) -> Router {
    let pages = Router::new()
        .route("/", get(home_page))
        .route("/search", get(search))
        .route("/block/{height}", get(block_page))
        .route("/tx/{txid}", get(tx_page))
        .route("/address/{address}", get(address_page))
        .route("/alkane/{alkane}", get(alkane_page))
        .route("/alkanes", get(alkanes_page));

    let api = Router::new()
        .route("/api/blocks/carousel", get(carousel_blocks))
        .route("/api/search/guess", get(search_guess))
        .route("/api/alkane/simulate", post(simulate_contract))
        .route("/api/alkane/chart", get(alkane_chart))
        .route("/api/alkane/balance-chart", get(alkane_balance_chart))
        .route("/api/address/chart", get(address_chart));

    let assets = Router::new()
        .route("/static/style.css", get(style))
        .route("/favicon.svg", get(favicon));
    let seo = Router::new()
        .route("/robots.txt", get(robots_txt))
        .route("/sitemap.xml", get(sitemap_xml));

    let chinese = Router::new()
        .merge(pages.clone())
        .merge(api.clone())
        .merge(assets.clone())
        .layer(middleware::from_fn(chinese_language_middleware));

    Router::new()
        .merge(pages)
        .merge(api)
        .merge(assets)
        .merge(seo)
        .nest("/zh", chinese)
        .with_state(state)
}

async fn chinese_language_middleware(req: Request, next: Next) -> Response {
    with_language(ExplorerLanguage::Chinese, next.run(req)).await
}

pub async fn run_explorer(addr: SocketAddr) -> anyhow::Result<()> {
    let state = ExplorerState::new();
    let base_path = get_explorer_base_path();
    let app = if base_path == "/" {
        explorer_router(state)
    } else {
        Router::new().nest(base_path, explorer_router(state))
    };
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn robots_txt() -> impl IntoResponse {
    let sitemap = current_public_base_url()
        .map(|base| format!("{base}/sitemap.xml"))
        .unwrap_or_else(|| "/sitemap.xml".to_string());
    let body = format!(
        "User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /static/\nSitemap: {sitemap}\n"
    );
    (StatusCode::OK, [(CONTENT_TYPE, "text/plain; charset=utf-8")], body)
}

async fn sitemap_xml() -> impl IntoResponse {
    let base = match current_public_base_url() {
        Some(v) => v,
        None => {
            return (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/xml; charset=utf-8")],
                r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>"#
                    .to_string(),
            );
        }
    };

    let tip = get_espo_next_height().saturating_sub(1) as u64;
    let latest_start = tip.saturating_sub(49);
    let mut paths: Vec<String> =
        vec!["/".to_string(), "/zh".to_string(), "/alkanes".to_string(), "/zh/alkanes".to_string()];
    for height in (latest_start..=tip).rev() {
        paths.push(format!("/block/{height}"));
        paths.push(format!("/zh/block/{height}"));
    }

    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"#,
    );
    for path in paths {
        let loc = absolute_url(&base, &path);
        xml.push_str("<url><loc>");
        xml.push_str(&xml_escape(&loc));
        xml.push_str("</loc></url>");
    }
    xml.push_str("</urlset>");

    (StatusCode::OK, [(CONTENT_TYPE, "application/xml; charset=utf-8")], xml)
}

fn absolute_url(base: &str, path: &str) -> String {
    if path == "/" {
        return base.to_string();
    }
    format!("{base}{path}")
}

fn xml_escape(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn current_public_base_url() -> Option<String> {
    let networks = get_explorer_networks()?;
    let raw = match get_network() {
        bitcoin::Network::Bitcoin => networks.mainnet.as_deref(),
        bitcoin::Network::Signet => networks.signet.as_deref(),
        bitcoin::Network::Regtest => networks.regtest.as_deref(),
        _ => {
            let tag = get_network().to_string();
            if tag == "testnet4" {
                networks.testnet4.as_deref()
            } else {
                networks.testnet3.as_deref()
            }
        }
    }?;
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}
