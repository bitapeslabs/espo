mod api;
pub mod components;
pub mod consts;
pub mod faucet;
pub mod i18n;
pub mod mining_pools;
mod pages;
pub mod paths;
pub mod phishing;
pub mod relay;

use std::net::SocketAddr;

use crate::modules::essentials::storage::{EssentialsProvider, GetHoldersOrderedPageParams};
use crate::modules::runes::storage::RunesProvider;
use crate::runtime::state_at::StateAt;
use api::{
    address_chart, alkane_abi_export, alkane_balance_chart, alkane_chart, alkane_holders_export,
    alkane_wasm_export, carousel_blocks, explorer_events_ws, mempool_blocks, minting_price_chart,
    rune_holders_export, search_guess, simulate_contract,
};
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use i18n::ExplorerLanguage;
use mining_pools::{block_mining_pool_api, mining_pool_icon};
use pages::address::address_page;
use pages::alkane::alkane_page;
use pages::alkanes::alkanes_page;
use pages::block::{block_page, mempool_block_page};
use pages::docs::docs_page;
use pages::faucet::faucet_page;
use pages::home::home_page;
use pages::rune::{rune_icon_asset, rune_page};
use pages::runes::runes_page;
use pages::search::search;
use pages::state::ExplorerState;
use pages::tx::tx_page;
use tokio::net::TcpListener;

use crate::config::{get_config, get_explorer_base_path, get_explorer_networks, get_network};
use crate::modules::runes::main::runes_enabled_from_global_config;
use components::layout::{favicon, style, waves_light};
use faucet::{faucet_enabled, faucet_send, faucet_status};
use paths::with_language;

pub fn explorer_router(state: ExplorerState) -> Router {
    let runes_enabled = runes_enabled_from_global_config();
    let mut pages = Router::new()
        .route("/", get(home_page))
        .route("/search", get(search))
        .route("/block/{height}", get(block_page))
        .route("/mempool-block/{index}", get(mempool_block_page))
        .route("/tx/{txid}", get(tx_page))
        .route("/address/{address}", get(address_page))
        .route("/alkane/{alkane}", get(alkane_page))
        .route("/alkanes", get(alkanes_page))
        .route("/docs", get(docs_page));
    if runes_enabled {
        pages = pages.route("/rune/{rune}", get(rune_page)).route("/runes", get(runes_page));
    }
    if faucet_enabled() {
        pages = pages.route("/faucet", get(faucet_page));
    }

    let mut api = Router::new()
        .route("/api/blocks/carousel", get(carousel_blocks))
        .route("/api/block/pool", get(block_mining_pool_api))
        .route("/api/mempool/blocks", get(mempool_blocks))
        .route("/api/search/guess", get(search_guess))
        .route("/api/alkane/simulate", post(simulate_contract))
        .route("/api/alkane/abi/export", get(alkane_abi_export))
        .route("/api/alkane/wasm/export", get(alkane_wasm_export))
        .route("/api/alkane/holders/export", get(alkane_holders_export))
        .route("/api/alkane/chart", get(alkane_chart))
        .route("/api/alkane/balance-chart", get(alkane_balance_chart))
        .route("/api/minting-price-chart", get(minting_price_chart))
        .route("/api/address/chart", get(address_chart));
    if runes_enabled {
        api = api.route("/api/rune/holders/export", get(rune_holders_export));
    }
    if faucet_enabled() {
        api = api
            .route("/api/faucet/status", get(faucet_status))
            .route("/api/faucet/send", post(faucet_send));
    }
    let mempool_cfg = &get_config().mempool;
    // Client mode relays the data instance's events socket, so the route is
    // needed even though the local mempool service is disabled.
    if mempool_cfg.websocket_enabled || crate::config::get_explorer_espo_events_host().is_some() {
        let ws_path = mempool_cfg.websocket_path.as_deref().unwrap_or("/api/events/ws");
        api = api.route(ws_path, get(explorer_events_ws));
    }

    let mut assets = Router::new()
        .route("/static/style.css", get(style))
        .route("/static/waves-light.svg", get(waves_light))
        .route("/static/mining-pools/{slug}", get(mining_pool_icon))
        .route("/favicon.svg", get(favicon));
    if runes_enabled {
        assets = assets.route("/static/rune-icons/{rune}", get(rune_icon_asset));
    }
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
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

async fn robots_txt() -> impl IntoResponse {
    let sitemap = current_public_base_url()
        .map(|base| format!("{base}/sitemap.xml"))
        .unwrap_or_else(|| "/sitemap.xml".to_string());
    let body = robots_body(get_explorer_base_path(), &sitemap);
    (StatusCode::OK, [(CONTENT_TYPE, "text/plain; charset=utf-8")], body)
}

/// Crawlable surface: the English and Chinese homepages, and Alkane token
/// pages in both locales. Everything else — blocks, transactions, addresses,
/// runes, docs, search, APIs and assets — is disallowed.
///
/// `Allow` beats `Disallow` on longest match, and `$` anchors the end of the
/// path, so the bare `/$` rule exposes the homepage without exposing every
/// path beneath it.
fn robots_body(base_path: &str, sitemap: &str) -> String {
    let base = base_path.trim_end_matches('/');
    let path = |suffix: &str| format!("{base}{suffix}");
    let mut body = String::from("User-agent: *\n");
    for allow in [
        path("/$"),
        path("/zh$"),
        path("/zh/$"),
        path("/docs$"),
        path("/zh/docs$"),
        path("/alkane/"),
        path("/zh/alkane/"),
    ] {
        body.push_str("Allow: ");
        body.push_str(&allow);
        body.push('\n');
    }
    body.push_str("Disallow: ");
    body.push_str(&path("/"));
    body.push('\n');
    body.push_str("Sitemap: ");
    body.push_str(sitemap);
    body.push('\n');
    body
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

    // Crawlable surface only (see robots_body): homepages, docs, and the top
    // alkanes/runes in both locales. Block pages are deliberately absent —
    // they churn every block and are disallowed to crawlers.
    const SITEMAP_TOP_LIMIT: usize = 20;
    let mut paths: Vec<String> =
        vec!["/".to_string(), "/zh".to_string(), "/docs".to_string(), "/zh/docs".to_string()];

    let essentials =
        EssentialsProvider::new(std::sync::Arc::new(crate::config::espo_mdb(b"essentials:")));
    if let Ok(top_alkanes) = essentials.get_holders_ordered_page(GetHoldersOrderedPageParams {
        blockhash: StateAt::Latest,
        offset: 0,
        limit: SITEMAP_TOP_LIMIT as u64,
        desc: true,
    }) {
        for alkane in top_alkanes.ids {
            let id = format!("{}:{}", alkane.block, alkane.tx);
            paths.push(format!("/alkane/{id}"));
            paths.push(format!("/zh/alkane/{id}"));
        }
    }

    if runes_enabled_from_global_config() {
        let runes = RunesProvider::new(std::sync::Arc::new(crate::config::espo_mdb(b"runes:")));
        if let Ok(top_runes) = runes.get_top_runes(1, SITEMAP_TOP_LIMIT) {
            for (entry, _holders) in top_runes {
                paths.push(format!("/rune/{}", entry.id));
                paths.push(format!("/zh/rune/{}", entry.id));
            }
        }
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

#[cfg(test)]
mod robots_tests {
    use super::robots_body;

    #[test]
    fn allows_only_homepages_and_alkane_pages() {
        let body = robots_body("/", "https://espo.sh/sitemap.xml");
        let allows: Vec<&str> =
            body.lines().filter_map(|line| line.strip_prefix("Allow: ")).collect();
        assert_eq!(
            allows,
            vec!["/$", "/zh$", "/zh/$", "/docs$", "/zh/docs$", "/alkane/", "/zh/alkane/"]
        );
        assert!(body.lines().any(|line| line == "Disallow: /"));
        assert!(body.starts_with("User-agent: *\n"));
        assert!(body.ends_with("Sitemap: https://espo.sh/sitemap.xml\n"));

        // The anchored homepage rule must not read as a prefix rule that
        // would expose everything under it.
        assert!(!allows.contains(&"/"));
    }

    #[test]
    fn respects_a_non_root_base_path() {
        let body = robots_body("/explorer", "https://espo.sh/sitemap.xml");
        assert!(body.contains("Allow: /explorer/$\n"));
        assert!(body.contains("Allow: /explorer/zh$\n"));
        assert!(body.contains("Allow: /explorer/alkane/\n"));
        assert!(body.contains("Disallow: /explorer/\n"));
    }
}
