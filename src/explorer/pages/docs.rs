use axum::response::Html;
use maud::{Markup, PreEscaped, html};
use serde_json::json;

use crate::config::get_config;
use crate::explorer::components::header::{HeaderProps, header};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::tx_view::json_viewer;

const HTTP_BASE: &str = "https://api.alkanode.com";

fn docs_host(configured: Option<&str>) -> &str {
    configured.unwrap_or(HTTP_BASE).trim_end_matches('/')
}

fn docs_endpoint(host: Option<&str>, path: &str) -> String {
    format!("{}{}", docs_host(host), path)
}

fn rpc_docs_endpoint() -> String {
    rpc_endpoint(get_config().hosts.rpc_host.as_deref())
}

fn rpc_endpoint(configured: Option<&str>) -> String {
    let host = docs_host(configured);
    if host.ends_with("/rpc") { host.to_string() } else { format!("{host}/rpc") }
}

pub(super) struct ModuleDoc {
    pub(super) slug: &'static str,
    pub(super) title: &'static str,
    pub(super) intro: &'static str,
    pub(super) methods: Vec<MethodDoc>,
}

pub(super) struct MethodDoc {
    anchor: String,
    title: String,
    badge: String,
    transport: &'static str,
    description: &'static str,
    query_prefix: Option<String>,
    query_json: Option<serde_json::Value>,
    query_fallback: String,
    response_json: serde_json::Value,
    response_fallback: String,
}

pub async fn docs_page() -> Html<String> {
    let modules = docs_modules();
    let method_count = modules.iter().map(|m| m.methods.len()).sum::<usize>();

    layout_with_meta(
        "API Docs",
        "/docs",
        Some("Reference for every Espo JSON-RPC and HTTP API method, with examples."),
        html! {
            (header(HeaderProps {
                title: "API Documentation".to_string(),
                id: Some(format!("{method_count} methods")),
                show_copy: false,
                pill: None,
                summary_items: vec![],
                cta: None,
                hero_class: Some("docs-hero".to_string()),
            }))

            div class="docs-shell" {
                aside class="docs-nav" aria-label="API modules" {
                    @for module in &modules {
                        p { (module.title) }
                        @for method in &module.methods {
                            a href=(format!("#{}", method.anchor)) { (method.title) }
                        }
                    }
                }
                div class="docs-content" {
                    @for module in &modules {
                        section class="docs-module" id=(module.slug) {
                            div class="docs-module-head" {
                                h2 { (module.title) }
                                p class="muted" { (module.intro) }
                            }
                            @for method in &module.methods {
                                (render_method(method))
                            }
                        }
                    }
                }
            }
            (docs_script())
        },
    )
}

fn render_method(method: &MethodDoc) -> Markup {
    let notes = method_notes(method);

    html! {
        article class="docs-method" id=(method.anchor.as_str()) {
            a class="docs-method-title" id=(format!("{}-tab-header", method.anchor)) href=(format!("#{}", method.anchor)) data-docs-section-header="" {
                span { (method.title.as_str()) }
                span class="docs-method-meta" { (method.badge.as_str()) }
            }
            div class="docs-method-body" {
                div class="docs-description" {
                    p { (method.description) }
                    p { (method_detail(method)) }
                    @if !notes.is_empty() {
                        div class="docs-subtitle docs-know-title" { "What to know" }
                        ul class="docs-know-list" {
                            @for note in &notes {
                                li { (note) }
                            }
                        }
                    }
                }
                div class="docs-example-head" {
                    div class="docs-subtitle" { "Example query" }
                }
                (render_json_block(
                    method.query_prefix.as_deref(),
                    method.query_json.as_ref(),
                    method.query_fallback.as_str(),
                ))
                div class="docs-subtitle" { "Example response" }
                (render_json_block(
                    None,
                    Some(&method.response_json),
                    method.response_fallback.as_str(),
                ))
            }
        }
    }
}

fn render_json_block(
    prefix: Option<&str>,
    value: Option<&serde_json::Value>,
    fallback: &str,
) -> Markup {
    html! {
        div class="docs-json-block" {
            @if let Some(prefix) = prefix {
                div class="docs-endpoint-line" { (prefix) }
            }
            @if value.is_some() {
                (json_viewer(value, fallback))
            } @else {
                pre class="docs-endpoint-line docs-endpoint-only" { (fallback) }
            }
        }
    }
}

fn docs_script() -> Markup {
    PreEscaped(
        r#"
<script>
(() => {
  const mobileQuery = window.matchMedia('(max-width: 992px)');

  const closeSection = (section, body) => {
    section.style.height = 'auto';
    body.style.top = '-10000px';
    body.style.opacity = '0';
    body.classList.remove('open');
  };

  const openSection = (section, header, body) => {
    const headerHeight = header.scrollHeight;
    const bodyHeight = body.scrollHeight;
    section.style.height = `${bodyHeight + headerHeight + 28}px`;
    body.style.top = `${headerHeight + 28}px`;
    body.style.opacity = '1';
    body.classList.add('open');
  };

  const toggleSection = (event, header) => {
    if (!mobileQuery.matches) return;
    event.preventDefault();
    const section = header.closest('.docs-method');
    const body = section && section.querySelector('.docs-method-body');
    if (!section || !body) return;
    window.scrollTo({ top: section.offsetTop - 100 });
    history.pushState({}, '', `${location.pathname}${location.search}#${section.id}`);
    if (body.classList.contains('open')) {
      closeSection(section, body);
    } else {
      openSection(section, header, body);
    }
  };

  document.querySelectorAll('[data-docs-section-header]').forEach((header) => {
    header.addEventListener('click', (event) => toggleSection(event, header));
  });

  const openHashSection = () => {
    if (!mobileQuery.matches || !location.hash) return;
    const section = document.querySelector(location.hash);
    if (!section || !section.classList.contains('docs-method')) return;
    const header = section.querySelector('[data-docs-section-header]');
    const body = section.querySelector('.docs-method-body');
    if (!header || !body || body.classList.contains('open')) return;
    openSection(section, header, body);
    const offset = 100;
    setTimeout(() => {
      window.scrollTo({ top: section.offsetTop - offset });
    }, 1);
  };

  openHashSection();
  window.addEventListener('hashchange', openHashSection);
  mobileQuery.addEventListener('change', () => {
    if (!mobileQuery.matches) {
      document.querySelectorAll('.docs-method').forEach((section) => {
        const body = section.querySelector('.docs-method-body');
        if (!body) return;
        section.style.height = '';
        body.style.top = '';
        body.style.opacity = '';
        body.classList.remove('open');
      });
    } else {
      openHashSection();
    }
  });
})();
</script>
"#
        .to_string(),
    )
}

fn method_detail(method: &MethodDoc) -> &'static str {
    let name = method.title.as_str();

    if name == "get_espo_height" {
        return "Clients commonly call this before pagination or historical reads so they can tell whether the explorer is caught up to the chain tip they expect.";
    }
    if name == "get_method_line_chart" {
        return "The chart method calls another numeric RPC repeatedly over a height range, so choose a narrow interval when you need quick responses.";
    }
    if name.contains("mempool_traces") {
        return "Use this for unconfirmed Alkane activity previews; results can disappear or change when transactions are replaced, evicted, or mined.";
    }
    if name.contains("memory_stats") || name.contains("debug_timer") {
        return "This is an operational endpoint intended for monitoring and debugging a running Espo node rather than user-facing portfolio state.";
    }
    if name.ends_with(".ping") {
        return "A successful response only proves the module handler is reachable; it does not guarantee every optional backing service is enabled.";
    }
    if name.contains("get_keys") {
        return "Storage keys are low-level contract state. Values may be binary, encoded integers, or UTF-8 text depending on the contract convention.";
    }
    if name.contains("all_alkanes") || name == "/get-alkanes" || name == "/global-alkanes-search" {
        return "These list and search methods are designed for discovery screens. They return indexed metadata, not live contract execution results.";
    }
    if name.contains("alkane_info") || name == "/get-alkane-details" {
        return "Use this when you already know the Alkane id and need the explorer's normalized metadata, display fields, and indexed creation context.";
    }
    if name.contains("block_summary") {
        return "Block summaries are aggregated during indexing and are useful for block pages, progress checks, and quickly finding Alkane-heavy blocks.";
    }
    if name.contains("holders_count") {
        return "This returns the aggregate count only; use the holder list endpoints when you need balances, address rows, or pagination.";
    }
    if name.contains("holders")
        || name.contains("address_balances")
        || name.contains("alkane_balances")
    {
        return "Balances are derived from indexed UTXO state, so historical reads reflect the indexer's view at that height rather than a wallet's local cache.";
    }
    if name.contains("transfer_volume") || name.contains("total_received") {
        return "These ranking endpoints are useful for leaderboards and analytics, but they should not be treated as spendable balance calculations.";
    }
    if name.contains("circulating_supply") {
        return "Supply is reported as the raw indexed token amount. Apply token decimals or display scaling in the client when presenting it to users.";
    }
    if name.contains("address_activity")
        || name.contains("get_token_activity")
        || name.contains("get_activity")
    {
        return "Activity rows are normalized event views intended for timelines. For exact wallet spendability, combine them with balance or outpoint endpoints.";
    }
    if name.contains("spendable_outpoints")
        || name.contains("address_utxos")
        || name.contains("account_utxos")
    {
        return "These endpoints are for wallet construction flows and may include enough transaction context to build or simulate spends.";
    }
    if name.contains("outpoint_balances")
        || name.contains("alkanes-utxo")
        || name.contains("amm-utxos")
    {
        return "Outpoint-level responses are the most precise way to understand which UTXOs carry token state before constructing a transaction.";
    }
    if name.contains("block_traces")
        || name.contains("tx_summary")
        || name.contains("latest_traces")
    {
        return "Trace data explains contract execution, emitted events, and state changes. It is heavier than summary data and should be paged or scoped where possible.";
    }
    if name.contains("candles")
        || name.contains("chart")
        || name.contains("price")
        || name.contains("volume")
    {
        return "Market and chart data is derived from indexed AMM activity. Missing buckets or sparse periods can occur when no qualifying events were indexed.";
    }
    if name.contains("pool")
        || name.contains("amm")
        || name.contains("swap")
        || name.contains("liquidity")
    {
        return "AMM endpoints use pool and factory ids from indexed contracts. Route calculations are informational and clients should still validate transactions before broadcast.";
    }
    if name.contains("runes.") || name.contains("rune") {
        return "Rune endpoints are available only when the runes module is enabled and follow the Rune id and spaced-name conventions used by the indexer.";
    }
    if name.contains("pizza") || name.contains("series") {
        return "Pizza.fun mapping methods bridge off-explorer series identifiers with Alkane ids; null batch entries mean no confirmed mapping was found.";
    }
    if name.contains("wrap") || name.contains("unwrap") || name.contains("subfrost") {
        return "Subfrost history endpoints are event indexes for frBTC flows. Use success and fulfillment filters to separate requested actions from completed ones.";
    }
    if name == "/api/alkane/simulate" {
        return "Simulation executes against indexed state and is meant for previewing contract behavior before building or broadcasting a transaction.";
    }
    if name.contains("/api/blocks") || name.contains("/api/mempool") || name.contains("/api/block")
    {
        return "Explorer endpoints are optimized for UI payloads and may change shape with the explorer; use module RPC methods for stable integration contracts.";
    }
    if name.contains("/export") {
        return "Export routes are built for downloads and can return file content instead of a typical JSON document depending on the requested format.";
    }
    if method.transport == "WEBSOCKET" {
        return "The websocket sends event messages as the explorer observes chain and mempool changes, so consumers should handle reconnects and duplicate state updates.";
    }
    if method.transport == "HTTP" {
        return "HTTP methods are compatibility routes used by the explorer and Oyl-style clients; they wrap indexed data into route-specific response shapes.";
    }

    "This RPC reads Espo's indexed state and returns a normalized JSON shape intended for explorers, wallets, analytics jobs, or integration tests."
}

fn method_notes(method: &MethodDoc) -> Vec<String> {
    let mut notes = Vec::new();
    let query_text = method.query_fallback.as_str();
    let combined = format!(
        "{} {} {}",
        method.title,
        method.query_prefix.as_deref().unwrap_or_default(),
        query_text
    );

    if contains_any(
        &combined,
        &["\"height\"", "height=", "blockHeight", "from_height", "to_height"],
    ) {
        push_note(
            &mut notes,
            "Height filters resolve against indexed chain state. Omitting an optional height generally means the latest indexed height.",
        );
    }
    if contains_any(&combined, &["\"page\"", "\"limit\""]) {
        push_note(
            &mut notes,
            "`page` pagination is 1-based. `limit` may be clamped by the handler to protect the backing index.",
        );
    }
    if contains_any(&combined, &["\"count\"", "\"offset\"", "includeTotal", "include_total"]) {
        push_note(
            &mut notes,
            "`count` and `offset` use offset-based pagination. Request totals only when the endpoint supports `includeTotal` or `include_total`.",
        );
    }
    if contains_any(&combined, &["\"address\"", "address=", "owner"]) {
        push_note(
            &mut notes,
            "Bitcoin addresses are validated for the configured network, so mainnet, testnet, signet, and regtest addresses are not interchangeable.",
        );
    }
    if contains_any(
        &combined,
        &[
            "alkane",
            "alkane_id",
            "alkaneId",
            "token",
            "tokenId",
            "poolId",
            "factoryId",
            "tokenAId",
            "tokenBId",
        ],
    ) {
        push_note(
            &mut notes,
            "Alkane ids use the `<block>:<tx>` identity. Oyl-compatible HTTP bodies split the same id into `{ \"block\": \"...\", \"tx\": \"...\" }`.",
        );
    }
    if combined.contains("outpoint") {
        push_note(
            &mut notes,
            "Outpoints use `<txid>:<vout>`, where `vout` is the zero-based output index.",
        );
    }
    if contains_any(&combined, &["\"from\"", "\"to\"", "start_time", "end_time"]) {
        push_note(&mut notes, "Time filters are Unix timestamps in seconds.");
    }
    if combined.contains("successful") {
        push_note(
            &mut notes,
            "`successful` filters contract events by trace success state. Omit it to include both successes and failures.",
        );
    }
    if combined.contains("fulfilled") {
        push_note(
            &mut notes,
            "`fulfilled` separates requested unwraps from requests that have completed their fulfillment flow.",
        );
    }
    if combined.contains("spendStrategy") {
        push_note(
            &mut notes,
            "`spendStrategy` is passed through to wallet-style UTXO selection. Use `null` or omit it for the route default.",
        );
    }
    if combined.contains("searchQuery") {
        push_note(
            &mut notes,
            "Search text is matched against indexed token or pool metadata and may be capped by module search limits.",
        );
    }
    if contains_any(&combined, &["sort_by", "\"sort\"", "\"order\"", "\"dir\""]) {
        push_note(
            &mut notes,
            "Sort fields are route-specific. Unsupported values fall back to the handler default or return a validation error, depending on the route.",
        );
    }
    if contains_any(&combined, &["canonical_quote", "min_quote_amount", "min_amount"]) {
        push_note(
            &mut notes,
            "`canonical_quote`/`quote` filters token activity to rows whose counter token is that Alkane id. `min_quote_amount`/`min_amount` is a raw 1e8-scaled token amount, such as `100000` for 0.001 frBTC.",
        );
    }
    if method.title == "essentials.get_address_transactions" {
        push_note(
            &mut notes,
            "`only_alkane_txs` defaults to true. Set it to false only when you want the full Bitcoin address history instead of the Alkane transaction index.",
        );
        push_note(
            &mut notes,
            "`filter` accepts an Alkane id such as `2:0` and is valid only when `only_alkane_txs` is true or omitted.",
        );
        push_note(
            &mut notes,
            "Filtered results match Alkane transactions whose first trace event is an `invoke` where `context.myself` equals the requested Alkane id.",
        );
        push_note(
            &mut notes,
            "The filter is applied at request time by scanning the existing address Alkane transaction list until the requested page is filled. It does not require a new index.",
        );
        push_note(
            &mut notes,
            "When `filter` is provided, `total` is `null` because the filtered total is not known without scanning the complete address history. Use `has_more` for pagination.",
        );
        push_note(
            &mut notes,
            "`include_mempool` defaults to false. When true, all matching transactions currently returned by electrs `/address/:address/txs/mempool` are prepended to page 1 and do not consume confirmed pagination slots.",
        );
        push_note(
            &mut notes,
            "Mempool inclusion requires `electrs_esplora_url`. Requests with `include_mempool: true` return `unsupported_backend` when Espo is configured to use Electrum RPC instead.",
        );
        push_note(
            &mut notes,
            "Pending transactions have `confirmed: false`, `blockHeight: null`, `blockTime: null`, and `confirmations: 0`. Espo queries electrs on every included request, so dropped or RBF-replaced transactions disappear on the next poll.",
        );
    }
    if method.title == "essentials.get_alkabi" {
        push_note(
            &mut notes,
            "With `format: \"json\"`, `abi` is a JSON object. With `format: \"ts\"`, `abi` is a string containing a complete generated TypeScript module.",
        );
        push_note(
            &mut notes,
            "Proxy contracts resolve their indexed implementation recursively, and factory clones use their indexed factory WASM before Alkabi extraction.",
        );
        push_note(
            &mut notes,
            "Espo uses the top-level `alkabi_verify_trials` config value, which defaults to 128 and must be greater than zero. Reducible view methods include a verified `plan`; unsupported views omit it so consumers can fall back to simulation.",
        );
        push_note(
            &mut notes,
            "When top-level config `db_cache` is true, analyzed exports are persisted by network, resolved immutable WASM source, and verification trial count in `${db_path}/cache`. Concurrent misses for the same source and trial count share one job and wait for its cached result.",
        );
    }
    if method.title == "essentials.get_alkane_wasm" {
        push_note(
            &mut notes,
            "Cache by `sha256` (or `source`) rather than by the requested Alkane id: plain contracts are immutable, but a proxy's resolved `source` changes if its implementation is repointed.",
        );
        push_note(
            &mut notes,
            "The payload decodes with standard base64; when `encoding` is `base64+gzip`, gunzip the decoded bytes to recover the raw wasm. `sha256` and `length` always describe the raw wasm.",
        );
    }
    if method.title == "essentials.search_factory_keys" {
        push_note(
            &mut notes,
            "An absent key never matches any operator — including `ne` — so contracts that never set the key are excluded rather than treated as \"different value\".",
        );
        push_note(
            &mut notes,
            "Numeric operators decode the stored value as a little-endian u128 and only match values of 16 bytes or fewer; `eq`/`ne` compare the exact raw bytes. Results are ordered by child id (block, then tx), so pagination is deterministic.",
        );
        push_note(
            &mut notes,
            "There is deliberately no unscoped variant: the factory requirement keeps the scan bounded to batched point lookups over the factory-children index.",
        );
    }
    if combined.contains("include_outpoints") {
        push_note(
            &mut notes,
            "Including outpoints returns larger payloads and is intended for wallet or account-detail views.",
        );
    }

    notes
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn push_note(notes: &mut Vec<String>, note: &str) {
    if !notes.iter().any(|existing| existing == note) {
        notes.push(note.to_string());
    }
}

fn docs_modules() -> Vec<ModuleDoc> {
    vec![
        ModuleDoc {
            slug: "root-rpc",
            title: "Root JSON-RPC",
            intro: "Espo's own built-in methods, available without a module prefix. Proxies onto the Bitcoin backends live under btc.* instead.",
            methods: vec![
                rpc_doc(
                    "get_espo_height",
                    "Returns the latest Espo indexed height. Use this as the health and freshness check for clients.",
                    json!({}),
                    json!({ "height": 946000 }),
                ),
                rpc_doc(
                    "get_method_line_chart",
                    "Samples a numeric value from another RPC method across indexed heights and returns chart-ready points.",
                    json!({
                        "method": "essentials.get_holders_count",
                        "key": "count",
                        "body": { "alkane": "2:0" },
                        "range_min": 945900,
                        "range_max": 946000,
                        "range_interval": 25
                    }),
                    json!({
                        "method": "essentials.get_holders_count",
                        "key": "count",
                        "range_min": 945900,
                        "range_max": 946000,
                        "range_interval": 25,
                        "points": [
                            { "height": 945900, "value": 6409 },
                            { "height": 945925, "value": 6407 }
                        ]
                    }),
                ),
            ],
        },
        super::docs_btc::btc_module_doc(),
        ModuleDoc {
            slug: "essentials-rpc",
            title: "Essentials JSON-RPC",
            intro: "Core Alkane, address, outpoint, trace, holder, and mempool reads.",
            methods: vec![
                rpc_doc(
                    "essentials.get_mempool_tx",
                    "Returns one pending transaction's projected traces by txid, along with its projected mempool block and trace-status flags. Answers found false when the transaction is not in the mempool. The traces carry the same outpoint/events shape as get_block_traces, so one reconstruction covers pending and confirmed transactions alike; a remote explorer uses this to render an unconfirmed transaction's estimated trace and call summary, which it cannot project itself.",
                    json!({ "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90" }),
                    json!({
                        "ok": true,
                        "found": true,
                        "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                        "first_seen": 1785100016u64,
                        "mempool_block": 0,
                        "mempool_position_vsize": 141,
                        "defer_alkane_trace_status": false,
                        "has_alkane_action": true,
                        "has_rune_action": false,
                        "traces": [{
                            "outpoint": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90:3",
                            "events": [{ "event": "invoke", "data": { "type": "call", "context": { "myself": { "block": "0x2", "tx": "0x0" }, "inputs": ["0x4d", "0x0"] } } }]
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_mempool_traces",
                    "Returns paged Alkane traces from the in-memory projected mempool index, optionally filtered by address and minimum sats/vbyte paid via fee_paid. Results are ordered by projected mempool block with the next block first, then by fee paid within that block.",
                    json!({ "page": 1, "limit": 10, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "fee_paid": 2.16 }),
                    json!({
                        "ok": true,
                        "page": 1,
                        "limit": 10,
                        "has_more": false,
                        "total": 1,
                        "tx_total": 1,
                        "items": [{
                            "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                            "mempool_block": 0,
                            "fee_sat": 1540,
                            "fee_paid": 10.0,
                            "fee_rate": 10.0,
                            "vsize": 154,
                            "protostone": [{
                                "protocol_tag": 1,
                                "message": "02000000000000000000000000000000",
                                "edicts": [],
                                "pointer": null,
                                "refund": null,
                                "from": null,
                                "burn": null
                            }],
                            "traces": [{
                                "outpoint": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90:0",
                                "events": [{ "event": "invoke", "data": { "context": { "myself": { "block": "0x2", "tx": "0x0" } } } }]
                            }]
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_mempool_memory_stats",
                    "Returns in-memory mempool service counters when the mempool service is active.",
                    json!({}),
                    json!({ "ok": true, "stats": { "txs": 1200, "projected_blocks": 8 } }),
                ),
                rpc_doc(
                    "essentials.get_keys",
                    "Reads contract storage keys for an Alkane. Provide explicit keys or page through the key directory.",
                    json!({ "alkane": "2:0", "keys": ["name"], "try_decode_utf8": true }),
                    json!({ "ok": true, "alkane": "2:0", "total": 1, "items": { "name": { "key_hex": "0x6e616d65", "key_str": "name", "value_hex": "0x", "value_str": null } } }),
                ),
                rpc_doc(
                    "essentials.get_all_alkanes",
                    "Lists Alkane creation records with basic metadata. When only a name or symbol is indexed, the missing scalar field falls back to the available value; name-to-symbol fallbacks are uppercased. The raw names and symbols arrays remain unchanged.",
                    json!({ "page": 1, "limit": 1 }),
                    json!({ "ok": true, "page": 1, "limit": 1, "total": 77735, "items": [{ "alkane": "2:77579", "name": "Beep Boop Orbited #1376", "symbol": "BEEP BOOP ORBITED #1376", "holder_count": 1 }] }),
                ),
                rpc_doc(
                    "essentials.search_alkane",
                    "Searches indexed Alkane names and symbols by a case-insensitive prefix. Exact matches rank first, followed by holder count. limit defaults to 20 and is capped at 100. A missing symbol falls back to the uppercased name, and a missing name falls back to the symbol.",
                    json!({ "prefix": "die", "limit": 10 }),
                    json!({
                        "ok": true,
                        "prefix": "die",
                        "limit": 10,
                        "items": [{
                            "alkane": "2:0",
                            "name": "DIESEL",
                            "symbol": "diesel",
                            "holder_count": 6409,
                            "creation_height": 880000
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_alkane_info",
                    "Returns the creation metadata, names, icon, and indexed details for one Alkane. A missing scalar symbol falls back to the uppercased name, and a missing scalar name falls back to the symbol.",
                    json!({ "alkane": "2:0" }),
                    json!({ "ok": true, "alkane": "2:0", "name": "DIESEL", "symbol": "diesel", "holder_count": 6409, "creation_height": 880000 }),
                ),
                rpc_doc(
                    "essentials.get_alkabi",
                    "Extracts a contract's Alkabi ABI from its indexed WASM and synthesizes verified static plans for reducible view methods. Set format to json to return the ABI as a JSON object, or ts to return a generated TypeScript module string. Proxy implementations and factory clones resolve through the same indexed metadata used by the explorer's contract inspector. An optional height selects the indexed proxy and factory metadata at that height.",
                    json!({ "alkane": "2:0", "format": "json" }),
                    json!({
                        "ok": true,
                        "alkane": "2:0",
                        "format": "json",
                        "abi": {
                            "alkabi": 1,
                            "contract": "GenesisAlkane",
                            "types": {},
                            "methods": [{
                                "name": "mint",
                                "opcode": 77,
                                "kind": "execute"
                            }]
                        }
                    }),
                ),
                rpc_doc(
                    "essentials.get_alkane_wasm",
                    "Returns a contract's WASM bytecode, resolving proxy implementations and factory clones through the same indexed metadata as the explorer's /api/alkane/wasm/export download. The payload is base64-encoded; set gzip to true to compress it first (encoding becomes base64+gzip, typically ~3x smaller). sha256 and length always describe the raw wasm regardless of transport encoding, and source reports which Alkane actually provided the bytecode after resolution — factory clones and proxies sharing one binary resolve to the same source, so caches can dedup and content-address payloads. Set resolve to false (or no_resolution to true) to load exactly what the alkanes runtime's get_alkane_binary would: read /alkanes/{id}, recurse when the payload is a 32-byte factory pointer, otherwise gunzip — no /implementation or /beacon hop, so a proxy returns its own bytecode rather than its implementation's. That mode also loads the CURRENT stored payload, which for upgraded built-ins (DIESEL 2:0, frBTC 32:0, frSIGIL 32:1) is the binary the runtime executes today; pass first_version to get the original payload instead (the default resolved path always reports the first version). The response envelope is identical in both modes.",
                    json!({ "alkane": "2:0", "gzip": true, "resolve": false }),
                    json!({
                        "ok": true,
                        "alkane": "2:0",
                        "source": "2:0",
                        "length": 174225,
                        "sha256": "58cca703f5462c733975c068c6d1e245cc32941741f4b6e61a468257b4b5ffb6",
                        "encoding": "base64+gzip",
                        "payload_length": 63504,
                        "wasm_base64": "H4sIAAAAAAAC/..."
                    }),
                ),
                rpc_doc(
                    "essentials.get_factory_children",
                    "Returns child Alkane IDs indexed for a factory Alkane. The index is populated from creation records as new blocks are indexed; historical children appear after a reindex.",
                    json!({ "factory": "4:780993" }),
                    json!({ "ok": true, "factory": "4:780993", "children": ["2:80663"] }),
                ),
                rpc_doc(
                    "essentials.search_factory_keys",
                    "Searches a factory's children for contracts whose storage keys satisfy conditions. Conditions are ANDed and each requires the key to exist: eq and ne compare raw bytes (value as utf8 string or 0x-hex), gt, lt, ge and le decode the stored value as a little-endian u128 (value as decimal or 0x-hex, number or string), and exists matches any present value. A single condition can be passed as top-level key, op and value instead of the conditions array. Matched values are echoed per key in the same shape as get_keys items. Scoped to a factory by design: the scan resolves the factory-children index and does batched point lookups, capped at 8 conditions, 50,000 children and limit 1000.",
                    json!({
                        "factory": "4:780993",
                        "conditions": [
                            { "key": "/collateral_address", "op": "eq", "value": "bc1q..." },
                            { "key": "/fire_amount", "op": "gt", "value": "13000000" }
                        ],
                        "page": 1,
                        "limit": 100
                    }),
                    json!({
                        "ok": true,
                        "factory": "4:780993",
                        "conditions": [
                            { "key": "0x2f636f6c6c61746572616c5f61646472657373", "key_str": "/collateral_address", "op": "eq", "value": "0x626331712e2e2e" },
                            { "key": "0x2f666972655f616d6f756e74", "key_str": "/fire_amount", "op": "gt", "value": "13000000" }
                        ],
                        "page": 1,
                        "limit": 100,
                        "total": 1,
                        "has_more": false,
                        "items": [{
                            "alkane": "2:80663",
                            "keys": {
                                "/collateral_address": { "key_hex": "0x2f636f6c6c61746572616c5f61646472657373", "key_str": "/collateral_address", "value_hex": "0x626331712e2e2e", "value_str": "bc1q...", "value_u128": null, "last_txid": "84ec..." },
                                "/fire_amount": { "key_hex": "0x2f666972655f616d6f756e74", "key_str": "/fire_amount", "value_hex": "0x406f400100000000", "value_str": null, "value_u128": "21000000", "last_txid": "84ec..." }
                            }
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_block_summary",
                    "Returns the indexed summary, canonical block hash, serialized header, and exact Unix block time for a block height.",
                    json!({ "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "found": true, "blockhash": "0000000000000000000000000000000000000000000000000000000000000000", "header_hex": "00000020...", "block_time": 1779308930, "tx_count": 2864, "trace_count": 38, "interaction_count": 60, "pool": { "name": "AntPool", "slug": "antpool" } }),
                ),
                rpc_doc(
                    "essentials.get_block_time",
                    "Returns the exact Unix timestamp from the canonical indexed block header at one height.",
                    json!({ "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "found": true, "block_time": 1779308930 }),
                ),
                rpc_doc(
                    "essentials.get_block_times",
                    "Returns exact Unix timestamps for up to 1,000 canonical indexed block heights in request order.",
                    json!({ "heights": [945999, 946000] }),
                    json!({
                        "ok": true,
                        "times": [
                            { "height": 945999, "found": true, "block_time": 1779308321 },
                            { "height": 946000, "found": true, "block_time": 1779308930 }
                        ]
                    }),
                ),
                rpc_doc(
                    "essentials.get_holders",
                    "Returns holders and balances for an Alkane.",
                    json!({ "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "alkane": "2:0", "page": 1, "limit": 1, "total": 6409, "items": [{ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "30950001348973" }] }),
                ),
                rpc_doc(
                    "essentials.get_orbital_holders",
                    "Returns holders for an orbital factory, counting each child Alkane held as one unit and listing the child Alkane IDs held by each holder.",
                    json!({ "factory": "4:780993", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "factory": "4:780993", "page": 1, "limit": 1, "total": 249, "items": [{ "type": "address", "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "3", "alkanes": ["2:80663", "2:80664", "2:80665"] }] }),
                ),
                rpc_doc(
                    "essentials.get_orbital_balances",
                    "Returns orbital child Alkane balances held by an address, keyed by factory Alkane.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "balances": { "4:780993": { "amount": "3", "alkanes": ["2:80663", "2:80664", "2:80665"] } } }),
                ),
                rpc_doc(
                    "essentials.get_transfer_volume",
                    "Ranks addresses by cumulative transfer volume for an Alkane.",
                    json!({ "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "alkane": "2:0", "page": 1, "limit": 1, "total": 23936, "items": [{ "address": "bc1qnlaz6rt6734pfd23ehx68nyczs5pfjdp6ct0aa", "amount": "3864636702544018" }] }),
                ),
                rpc_doc(
                    "essentials.get_total_received",
                    "Ranks addresses by total received amount for an Alkane.",
                    json!({ "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "alkane": "2:0", "page": 1, "limit": 1, "total": 23936, "items": [{ "address": "bc1qnlaz6rt6734pfd23ehx68nyczs5pfjdp6ct0aa", "amount": "3863765984261495" }] }),
                ),
                rpc_doc(
                    "essentials.get_circulating_supply",
                    "Returns the circulating supply for an Alkane at latest or at a requested height.",
                    json!({ "alkane": "2:0" }),
                    json!({ "ok": true, "alkane": "2:0", "height": "latest", "supply": "62907254954708" }),
                ),
                rpc_doc(
                    "essentials.get_address_activity",
                    "Returns Alkane activity detected for a Bitcoin address.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "total_received": { "2:0": "357260014838703" }, "transfer_volume": { "2:0": "357810014838703" } }),
                ),
                rpc_doc(
                    "essentials.address_cumulative_send_alkanes",
                    "Returns cumulative address sends attributed to source Alkanes and tokens.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "kind": "send", "items": [{ "source_alkane": "2:0", "alkane": "2:0", "amount": "715000000" }] }),
                ),
                rpc_doc(
                    "essentials.address_cumulative_receive_alkanes",
                    "Returns cumulative address receives attributed to source Alkanes and tokens.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "kind": "receive", "items": [{ "source_alkane": "2:0", "alkane": "2:0", "amount": "312500000" }] }),
                ),
                rpc_doc(
                    "essentials.address_cumulative_send_orbitals",
                    "Returns cumulative address sends attributed to factory orbitals and tokens.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "kind": "send", "items": [{ "orbital": "2:0", "alkane": "4:3", "amount": "1" }] }),
                ),
                rpc_doc(
                    "essentials.address_cumulative_receive_orbitals",
                    "Returns cumulative address receives attributed to factory orbitals and tokens.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "kind": "receive", "items": [{ "orbital": "2:0", "alkane": "4:3", "amount": "1" }] }),
                ),
                rpc_doc(
                    "essentials.get_orbital_send_volumes",
                    "Ranks addresses by cumulative send volume attributed to an orbital for one Alkane token.",
                    json!({ "factory": "4:780993", "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "factory": "4:780993", "alkane": "2:0", "kind": "send", "page": 1, "limit": 1, "total": 249, "items": [{ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "12" }] }),
                ),
                rpc_doc(
                    "essentials.get_orbital_receive_volumes",
                    "Ranks addresses by cumulative receive volume attributed to an orbital for one Alkane token.",
                    json!({ "factory": "4:780993", "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "factory": "4:780993", "alkane": "2:0", "kind": "receive", "page": 1, "limit": 1, "total": 249, "items": [{ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "12" }] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_send_volumes",
                    "Ranks addresses by cumulative send volume attributed to one source Alkane for one Alkane token.",
                    json!({ "source_alkane": "2:1", "alkane": "4:3", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "source_alkane": "2:1", "alkane": "4:3", "kind": "send", "page": 1, "limit": 1, "total": 249, "items": [{ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "12" }] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_receive_volumes",
                    "Ranks addresses by cumulative receive volume attributed to one source Alkane for one Alkane token.",
                    json!({ "source_alkane": "2:1", "alkane": "4:3", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "source_alkane": "2:1", "alkane": "4:3", "kind": "receive", "page": 1, "limit": 1, "total": 249, "items": [{ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "amount": "12" }] }),
                ),
                rpc_doc(
                    "essentials.get_address_balances",
                    "Returns all Alkane balances held by an address. Set include_outpoints to include UTXO-level entries.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "include_outpoints": true }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "balances": { "2:0": "30950001348973", "2:68441": "31612184088" }, "outpoints": [{ "outpoint": "ee46dd269ba0f826b0dc9f78de1875fab3ab80983e51e741ffdfa6f3b7d4aef7:0" }] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_balances",
                    "Returns holders and balances for a specific Alkane, optionally at a historical height.",
                    json!({ "alkane": "2:0" }),
                    json!({ "ok": true, "alkane": "2:0", "balances": {} }),
                ),
                rpc_doc(
                    "essentials.get_alkane_balance_metashrew",
                    "Reads a single owner/token balance using the metashrew-compatible balance path.",
                    json!({ "owner": "2:53014", "alkane": "2:0" }),
                    json!({ "ok": true, "owner": "2:53014", "alkane": "2:0", "balance": "37604481010" }),
                ),
                rpc_doc(
                    "essentials.get_alkane_balance_txs",
                    "Lists balance-changing transactions for an Alkane with cursor pagination.",
                    json!({ "alkane": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "alkane": "2:0", "page": 1, "limit": 1, "total": 61235, "txids": [{ "height": 946000, "txid": "d39685c77b1af6b1734c07990f116cc71f301c33a7f8448c5db90720765d8902", "outflow": { "2:0": "-8128760" } }] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_balance_txs_by_token",
                    "Lists balance-changing transactions for one owner and token.",
                    json!({ "owner": "2:53014", "token": "2:0", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "owner": "2:53014", "token": "2:0", "page": 1, "limit": 1, "total": 2918, "txids": [{ "height": 945956, "txid": "9f825c8e8b83828b499d4754d5ad5cdfe9d6086fa67c1bb1489b56d354241e15", "outflow": { "2:0": "1000000", "2:16": "-14061191674" } }] }),
                ),
                rpc_doc(
                    "essentials.get_outpoint_balances",
                    "Returns Alkane balances assigned to a specific outpoint.",
                    json!({ "outpoint": "ee46dd269ba0f826b0dc9f78de1875fab3ab80983e51e741ffdfa6f3b7d4aef7:0" }),
                    json!({ "ok": true, "outpoint": "ee46dd269ba0f826b0dc9f78de1875fab3ab80983e51e741ffdfa6f3b7d4aef7:0", "items": [{ "alkane": "2:0", "amount": "30950001348973" }] }),
                ),
                rpc_doc(
                    "essentials.get_runtime_balances_metashrew",
                    "Returns the protocol's runtime balance sheet — Alkanes held by the runtime itself rather than by any outpoint. Takes no parameters; there is one sheet for the whole protocol, and it is always read at the metashrew tip (a `height` is validated like every other method here but does not select a historical sheet). Entries use the same shape as the ones inside get_outpoint_balances, minus the outpoint, and are ordered by Alkane id.",
                    json!({}),
                    json!({ "ok": true, "entries": [{ "alkane": "2:0", "amount": "9445870526523" }] }),
                ),
                rpc_doc(
                    "essentials.get_block_traces",
                    "Returns Alkane traces indexed for a block height.",
                    json!({ "height": 946000 }),
                    json!({
                        "ok": true,
                        "height": 946000,
                        "traces": [{
                            "outpoint": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8:3",
                            "events": [
                                {
                                    "event": "invoke",
                                    "data": {
                                        "type": "call",
                                        "fuel": 25382538,
                                        "context": {
                                            "myself": { "block": "0x2", "tx": "0x0" },
                                            "caller": { "block": "0x0", "tx": "0x0" },
                                            "vout": 3,
                                            "inputs": ["0x4d", "0x0"]
                                        }
                                    }
                                },
                                {
                                    "event": "return",
                                    "data": {
                                        "status": "success",
                                        "response": {
                                            "alkanes": [{ "id": { "block": "0x2", "tx": "0x0" }, "value": "0x2330ba25" }],
                                            "data": "0x",
                                            "storage": [{ "key": "/fees", "value": "0x9f478513110000000000000000000000" }]
                                        }
                                    }
                                }
                            ]
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_holders_count",
                    "Returns the holder count for one Alkane.",
                    json!({ "alkane": "2:0" }),
                    json!({ "ok": true, "count": 6409 }),
                ),
                rpc_doc(
                    "essentials.get_address_outpoints",
                    "Lists Alkane-bearing outpoints for an address.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "outpoints": [{ "outpoint": "ee46dd269ba0f826b0dc9f78de1875fab3ab80983e51e741ffdfa6f3b7d4aef7:0", "entries": [{ "alkane": "2:0", "amount": "30950001348973" }] }] }),
                ),
                rpc_doc(
                    "essentials.get_address_spendable_outpoints",
                    "Returns address UTXOs that are spendable for Alkane-aware wallet flows.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "omit_raw_tx": true }),
                    json!({
                        "ok": true,
                        "height": 951279,
                        "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8",
                        "length": 9,
                        "outpoints": [{
                            "outpoint": "b8cb5e85a7d024fb26c85e29678a377e257dd04c0a19905de6b1d7e3cd772c14:0",
                            "value": 546,
                            "confirmations": 11735,
                            "alkanes": { "2:77183": "1" },
                            "runes": {},
                            "script_pubkey_hex": "5120b819f74e24970413521ae6dcf8ec58ed4b65db6c36cbed4c8c7d95e56d4cfd4f"
                        }]
                    }),
                ),
                rpc_doc(
                    "essentials.get_alkane_tx_summary",
                    "Returns the indexed Alkane summary for a transaction.",
                    json!({ "txid": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8" }),
                    json!({ "ok": true, "height": 946000, "txid": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8", "traces": [{ "outpoint": "a44d1f42e1eb15b779f75089cd496f61b73ef68d411d09701ebd9ea51ade7cf8:3" }] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_block_txs",
                    "Returns Alkane transactions for a block height with paging.",
                    json!({ "height": 946000, "page": 1, "limit": 1 }),
                    json!({ "ok": true, "height": 946000, "page": 1, "limit": 1, "total": 39, "txids": ["5f989ae22dc2d2d178a16d88dcca3e7b92ecea6231a13b56e93b82fe3df56e4d"] }),
                ),
                rpc_doc(
                    "essentials.get_alkane_address_txs",
                    "Returns Alkane transactions involving an address.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "page": 1, "limit": 1 }),
                    json!({ "ok": true, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "page": 1, "limit": 1, "total": 19, "txids": ["e212e704173d61d19a280de3af2f6a5166ecf95e9a2a98f74ceeeb3de323ea1c"] }),
                ),
                rpc_doc(
                    "essentials.get_address_transactions",
                    "Returns Bitcoin transactions for an address with exact indexed block heights, Unix block times, and confirmations, and can be narrowed to Alkane transactions. Pending transactions can be included from electrs REST on page 1 only. When `filter` is provided with `only_alkane_txs`, it scans Alkane transactions until the requested confirmed page is filled and returns `total: null`.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "page": 1, "limit": 1, "only_alkane_txs": true, "filter": "2:0", "include_mempool": true }),
                    json!({
                        "ok": true,
                        "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8",
                        "page": 1,
                        "limit": 1,
                        "include_mempool": true,
                        "total": null,
                        "has_more": true,
                        "transactions": [
                            {
                                "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                                "blockHeight": null,
                                "blockTime": null,
                                "confirmations": 0,
                                "confirmed": false
                            },
                            {
                                "txid": "e212e704173d61d19a280de3af2f6a5166ecf95e9a2a98f74ceeeb3de323ea1c",
                                "blockHeight": 939827,
                                "blockTime": 1779308930,
                                "confirmations": 6174,
                                "confirmed": true
                            }
                        ]
                    }),
                ),
                rpc_doc(
                    "essentials.get_alkane_latest_traces",
                    "Returns the recent Alkane trace feed used by the explorer.",
                    json!({}),
                    json!({
                        "ok": true,
                        "txids": [
                            "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                            "4778e266cd2db0be03ebe96bb2713a2fe27d1587e4f10085f64b542f2bfd3617"
                        ]
                    }),
                ),
                rpc_doc(
                    "essentials.get_debug_timer_totals",
                    "Returns optional debug timer totals and can reset them when reset is true.",
                    json!({ "limit": 20, "reset": false }),
                    json!({
                        "ok": true,
                        "reset": false,
                        "reset_deleted": null,
                        "timers": [{
                            "title": "module=essentials section=update_balances",
                            "kind": "section",
                            "module": "essentials",
                            "label": "update_balances",
                            "count": 42,
                            "total_ms": 84512,
                            "avg_ms": 2012.19,
                            "max_ms": 4110,
                            "min_ms": 881,
                            "last_ms": 1915
                        }],
                        "returned": 1,
                        "total_entries": 1,
                        "total_ms": 84512,
                        "total_calls": 42
                    }),
                ),
                rpc_doc(
                    "essentials.ping",
                    "Checks that the essentials module can answer RPC calls.",
                    json!({}),
                    json!({ "ok": true, "pong": true }),
                ),
            ],
        },
        ModuleDoc {
            slug: "ammdata-rpc",
            title: "AMM Data JSON-RPC",
            intro: "Pool, candle, activity, price, swap routing, and AMM analytics methods.",
            methods: vec![
                rpc_doc(
                    "ammdata.get_candles",
                    "Returns OHLCV candles for a pool or token pair over a supported timeframe.",
                    json!({ "pool": "2:53014", "timeframe": "1h", "limit": 10, "page": 1, "side": "base" }),
                    json!({ "ok": true, "candles": [{ "ts": 1710000000, "open": "1", "high": "2", "low": "1", "close": "2", "volume": "100" }] }),
                ),
                rpc_doc(
                    "ammdata.get_btc_usd_candles",
                    "Returns Espo's native indexed BTC/USD history without deriving it through an Alkane market. Supported timeframes are 10m, 1h, 4h, 1d, 1w, and 1M. Candles are newest first and prices are fixed-point integers scaled by 10^16; divide open, high, low, and close by price_scale to obtain USD. Missing buckets are forward-filled from the last indexed price, and volume is always zero because this is a price index rather than an AMM market.",
                    json!({ "timeframe": "1h", "limit": 2, "page": 1 }),
                    json!({
                        "ok": true,
                        "pair": "btc-usd",
                        "timeframe": "1h",
                        "page": 1,
                        "limit": 2,
                        "total": 1000,
                        "has_more": true,
                        "price_scale": "10000000000000000",
                        "price_decimals": 16,
                        "candles": [{
                            "ts": 1779307200,
                            "open": "650000000000000000000",
                            "high": "650000000000000000000",
                            "low": "650000000000000000000",
                            "close": "650000000000000000000",
                            "volume": "0"
                        }]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_alkanes_quote",
                    "Returns current and 24-hour USD quotes for BTC and requested Alkanes. frBTC (32:0) is pegged directly to Espo's indexed BTC/USD history, so its prices and changes match BTC exactly. Other Alkane quotes prefer the configured merged <token>-derived_<quote>-usd chart, fall back to the direct <token>-usd chart, and return zero prices when neither chart exists. Current prices use the latest 10-minute close and comparison prices use hourly candle index 24. change_24h is the percentage change and change_24h_usd is the absolute price change.",
                    json!({ "assets": ["btc", "2:0", "2:68479"] }),
                    json!({
                        "ok": true,
                        "timeframe": "1h",
                        "comparison_hours": 24,
                        "assets": {
                            "btc": {
                                "name": "Bitcoin",
                                "symbol": "BTC",
                                "price_now_usd": "65000",
                                "price_24h_ago_usd": "64000",
                                "change_24h": "1.5625",
                                "change_24h_usd": "1000",
                                "price_now_ts": 1779307200,
                                "price_24h_ago_ts": 1779220800
                            },
                            "2:0": {
                                "name": "DIESEL",
                                "symbol": "diesel",
                                "price_now_usd": "0.75",
                                "price_24h_ago_usd": "0.7",
                                "change_24h": "7.1428",
                                "change_24h_usd": "0.05",
                                "price_now_ts": 1779307200,
                                "price_24h_ago_ts": 1779220800
                            },
                            "2:68479": {
                                "name": "TORTILLA",
                                "symbol": "TORTILLA",
                                "price_now_usd": "0",
                                "price_24h_ago_usd": "0",
                                "change_24h": "0.0000",
                                "change_24h_usd": "0",
                                "price_now_ts": null,
                                "price_24h_ago_ts": null
                            }
                        }
                    }),
                ),
                rpc_doc(
                    "ammdata.get_alkane_quote",
                    "Returns one BTC or Alkane quote using the same BTC-pegged frBTC, merged-derived, direct-USD, then zero fallback order as ammdata.get_alkanes_quote. The asset field also accepts an Alkane ID under the alkane alias.",
                    json!({ "asset": "2:0" }),
                    json!({
                        "ok": true,
                        "asset": "2:0",
                        "timeframe": "1h",
                        "comparison_hours": 24,
                        "name": "DIESEL",
                        "symbol": "diesel",
                        "price_now_usd": "0.75",
                        "price_24h_ago_usd": "0.7",
                        "change_24h": "7.1428",
                        "change_24h_usd": "0.05",
                        "price_now_ts": 1779307200,
                        "price_24h_ago_ts": 1779220800
                    }),
                ),
                rpc_doc(
                    "ammdata.get_portfolio_stats",
                    "Values an address's confirmed BTC and Alkane balances at latest or an optional indexed height. Historical BTC balances are reconstructed from address transaction history, while BTC prices come from Espo's indexed BTC/USD candles. frBTC (32:0) uses that same BTC price history. The same selected-height balances are valued at current and 24-hour-old prices, so changes represent price movement without treating purchases, sales, or transfers as gains. change_24h is the percentage change and change_24h_usd is the corresponding portfolio USD value change. Alkane names and symbols fall back to each other when one is missing, with name-to-symbol fallbacks uppercased. complete is false when any balance lacks a required price.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "height": 946000 }),
                    json!({
                        "ok": true,
                        "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8",
                        "height": 946000,
                        "complete": true,
                        "unpriced_assets": [],
                        "total_value_usd": "65150",
                        "total_value_24h_ago_usd": "64140",
                        "change_24h": "1.5746",
                        "change_24h_usd": "1010",
                        "assets": {
                            "btc": {
                                "name": "Bitcoin",
                                "symbol": "BTC",
                                "balance": "100000000",
                                "price_now_usd": "65000",
                                "price_24h_ago_usd": "64000",
                                "change_24h": "1.5625",
                                "change_24h_usd": "1000",
                                "value_now_usd": "65000",
                                "value_24h_ago_usd": "64000",
                                "value_change_24h_usd": "1000"
                            },
                            "2:0": {
                                "name": "DIESEL",
                                "symbol": "diesel",
                                "balance": "20000000000",
                                "price_now_usd": "0.75",
                                "price_24h_ago_usd": "0.7",
                                "change_24h": "7.1428",
                                "change_24h_usd": "0.05",
                                "value_now_usd": "150",
                                "value_24h_ago_usd": "140",
                                "value_change_24h_usd": "10"
                            }
                        }
                    }),
                ),
                rpc_doc(
                    "ammdata.get_token_volume",
                    "Returns raw token-side AMM volume buckets for a token. A swap contributes the amount of that token traded whether the token is the pool base asset or the quote asset.",
                    json!({ "token": "2:0", "timeframe": "1h", "limit": 10, "page": 1 }),
                    json!({
                        "ok": true,
                        "token": "2:0",
                        "timeframe": "1h",
                        "page": 1,
                        "limit": 10,
                        "total": 128,
                        "has_more": true,
                        "newest_ts": 1779886800,
                        "points": [
                            { "ts": 1779886800, "volume": "11486258" },
                            { "ts": 1779883200, "volume": "875000000" }
                        ]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_chart_change_block",
                    "Returns one chart change point for a named chart at a height.",
                    json!({ "chart": "btc_usd", "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "value": "65000" }),
                ),
                rpc_doc(
                    "ammdata.get_chart_changes_block",
                    "Returns all chart change points available at a height.",
                    json!({ "height": 951270 }),
                    json!({
                        "ok": true,
                        "available": true,
                        "height": 951270,
                        "charts": {
                            "2:0-usd": {
                                "10m": { "open": "760876.3864547", "high": "760876.3864547", "low": "760876.3864547", "close": "760876.3864547", "volume": "1287761936316" },
                                "1h": { "open": "760876.3864547", "high": "760876.3864547", "low": "760876.3864547", "close": "760876.3864547", "volume": "1287761936316" }
                            }
                        }
                    }),
                ),
                rpc_doc(
                    "ammdata.get_activity",
                    "Returns AMM activity for a pool with side, type, sort, and paging filters.",
                    json!({ "pool": "2:53014", "page": 1, "limit": 10, "type": "trade", "sort": "timestamp", "dir": "desc" }),
                    json!({
                        "ok": true,
                        "page": 1,
                        "limit": 10,
                        "has_more": true,
                        "activity_type": "trades",
                        "dir": "desc",
                        "filter_side": "all",
                        "activity": [{
                            "kind": "swap",
                            "txid": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3",
                            "timestamp": 1779888869,
                            "side": "base",
                            "direction": "sell",
                            "amount": "11486258",
                            "base_delta": "-11486258",
                            "quote_delta": "1061552883247"
                        }]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_token_activity",
                    "Returns AMM and token-market activity for a token.",
                    json!({ "token": "2:0", "page": 1, "limit": 10, "kind": "trade", "sort_by": "timestamp" }),
                    json!({
                        "ok": true,
                        "page": 1,
                        "limit": 10,
                        "has_more": true,
                        "activity_type": "all",
                        "kind": null,
                        "activity": [{
                            "kind": "swap",
                            "index_kind": "swap",
                            "pool": "DIESEL / CH4",
                            "pool_id": "2:53014",
                            "txid": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3",
                            "timestamp": 1779888869,
                            "direction": "sell",
                            "amount": "11486258",
                            "base": "2:0",
                            "quote": "2:16",
                            "base_delta": "-11486258",
                            "quote_delta": "1061552883247"
                        }]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_pools",
                    "Lists indexed AMM pools.",
                    json!({ "page": 1, "limit": 20 }),
                    json!({
                        "ok": true,
                        "page": 1,
                        "limit": 20,
                        "has_more": true,
                        "total": 149,
                        "pools": {
                            "2:53014": {
                                "base": "2:0",
                                "quote": "2:16",
                                "base_reserve": "21486066722",
                                "quote_reserve": "1624687948631013",
                                "source": "live"
                            }
                        }
                    }),
                ),
                rpc_doc(
                    "ammdata.get_amm_factories",
                    "Lists AMM factory contracts known to the indexer.",
                    json!({ "page": 1, "limit": 20 }),
                    json!({ "ok": true, "page": 1, "limit": 20, "has_more": true, "total": 172, "factories": ["2:50187", "2:50193"] }),
                ),
                rpc_doc(
                    "ammdata.find_best_swap_path",
                    "Computes the best known swap route between two Alkanes for exact-in or exact-out flows.",
                    json!({ "mode": "exact_in", "token_in": "2:0", "token_out": "2:16", "amount_in": "100000000", "fee_bps": 30, "max_hops": 3 }),
                    json!({
                        "ok": true,
                        "mode": "exact_in",
                        "token_in": "2:0",
                        "token_out": "2:16",
                        "amount_in": "1000000",
                        "amount_out": "89019369508",
                        "fee_bps": 30,
                        "max_hops": 3,
                        "hops": [{
                            "pool": "2:53014",
                            "token_in": "2:0",
                            "token_out": "2:16",
                            "amount_in": "1000000",
                            "amount_out": "75445130234"
                        }]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_best_mev_swap",
                    "Returns the best detected MEV-style swap opportunity for a token under the routing constraints.",
                    json!({ "token": "2:0", "fee_bps": 30, "max_hops": 3 }),
                    json!({ "ok": true, "swap": null }),
                ),
                rpc_doc(
                    "ammdata.get_btc_usd_price",
                    "Returns the BTC/USD price at latest or at a requested height. price is a fixed-point integer scaled by 10^16.",
                    json!({ "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "price": "650000000000000000000", "source": "ammdata_index" }),
                ),
                rpc_doc(
                    "ammdata.get_total_volume_amm",
                    "Returns total AMM volume over a height range or preset range.",
                    json!({ "unit": "usd", "from_height": 945900, "to_height": 946000, "page": 1, "limit": 10 }),
                    json!({
                        "ok": true,
                        "page": 1,
                        "limit": 10,
                        "range_min": 951000,
                        "range_max": 951279,
                        "scale": "10000000000000000",
                        "latest": { "height": 951279, "value": "64040138676553303341511" },
                        "points": [{ "height": 951000, "value": "64024572352483080490211" }]
                    }),
                ),
                rpc_doc(
                    "ammdata.get_token_total_volume",
                    "Returns the cumulative raw AMM amount traded for one token, keyed by block height. The latest value is the all-time token-side AMM volume recorded by the index.",
                    json!({ "token": "2:0", "from_height": 951000, "to_height": 951279, "page": 1, "limit": 10 }),
                    json!({
                        "ok": true,
                        "token": "2:0",
                        "page": 1,
                        "limit": 10,
                        "range_min": 951000,
                        "range_max": 951279,
                        "latest": { "height": 951279, "value": "2381492765411" },
                        "points": [
                            { "height": 951000, "value": "2367000000000" },
                            { "height": 951001, "value": "2368123456789" }
                        ]
                    }),
                ),
                rpc_doc(
                    "ammdata.ping",
                    "Checks that the AMM data module can answer RPC calls.",
                    json!({}),
                    json!({ "ok": true, "pong": true }),
                ),
            ],
        },
        ModuleDoc {
            slug: "runes-rpc",
            title: "Runes JSON-RPC",
            intro: "Runes metadata, balances, outpoints, transaction IO, activity, and transaction index methods. These are available when the runes module is enabled.",
            methods: vec![
                rpc_doc(
                    "runes.get_rune",
                    "Looks up a Rune by id, spaced name, or accepted query alias.",
                    json!({ "rune": "1:0" }),
                    json!({ "rune": { "id": "1:0", "spaced_name": "UNCOMMON GOODS" } }),
                ),
                rpc_doc(
                    "runes.get_top_runes",
                    "Lists top Runes by holder count.",
                    json!({ "page": 1, "limit": 10 }),
                    json!({
                        "runes": [{
                            "id": "1:0",
                            "number": 0,
                            "rune": "UNCOMMONGOODS",
                            "spaced_rune": "UNCOMMON GOODS",
                            "symbol": "G",
                            "divisibility": 0,
                            "supply": "105594793",
                            "mints": "105594793",
                            "holders": 249173,
                            "turbo": true
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_holders",
                    "Returns holders for one Rune.",
                    json!({ "id": "1:0", "page": 1, "limit": 1 }),
                    json!({ "holders": [{ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "amount": "2079397" }] }),
                ),
                rpc_doc(
                    "runes.get_address_balances",
                    "Returns Rune balances held by an address, optionally including outpoint rows.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "include_outpoints": true }),
                    json!({
                        "ok": true,
                        "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                        "balances": { "1:0": "2079397", "840000:2": "91000000" },
                        "items": [{ "id": "1:0", "rune": "UNCOMMON GOODS", "amount": "2079397" }],
                        "outpoints": [{
                            "outpoint": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2:1",
                            "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                            "entries": [{ "id": "1:0", "rune": "UNCOMMON GOODS", "amount": "2079397" }]
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_address_outpoints",
                    "Lists Rune-bearing outpoints for an address.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta" }),
                    json!({
                        "ok": true,
                        "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                        "outpoints": [{
                            "outpoint": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2:1",
                            "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                            "entries": [{ "id": "1:0", "rune": "UNCOMMON GOODS", "amount": "2079397" }]
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_outpoint_balances",
                    "Returns Rune balances assigned to a specific outpoint.",
                    json!({ "outpoint": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2:1" }),
                    json!({
                        "ok": true,
                        "outpoint": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2:1",
                        "items": [{
                            "outpoint": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2:1",
                            "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                            "entries": [{ "id": "1:0", "rune": "UNCOMMON GOODS", "amount": "2079397" }]
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_tx_io",
                    "Returns Rune inputs, outputs, burns, mints, and etched Rune data for a transaction.",
                    json!({ "txid": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2" }),
                    json!({
                        "ok": true,
                        "txid": "d11e7d18e03850c08bebaf0a9926288a4963d91b6ebcc40f8615a16ed81d40c2",
                        "io": {
                            "inputs": {},
                            "outputs": { "1": { "1:0": "1" } },
                            "minted": [{ "id": "1:0", "amount": "1" }],
                            "etched": null
                        }
                    }),
                ),
                rpc_doc(
                    "runes.get_mint_activity",
                    "Returns mint activity for one Rune.",
                    json!({ "id": "1:0", "page": 1, "limit": 10 }),
                    json!({
                        "activity": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "amount": "1",
                            "destination": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta",
                            "fee_paid_sats": 192
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_activity",
                    "Returns paged Rune activity with kind, scope, sort, address, and time filters.",
                    json!({ "id": "1:0", "page": 1, "limit": 10, "kind": "mint" }),
                    json!({
                        "ok": true,
                        "total": 105594794,
                        "entries": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "kind": "mint",
                            "id": "1:0",
                            "amount": "1",
                            "destination": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta"
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_rune_activity",
                    "Alias-style Rune activity endpoint with the same filters as runes.get_activity.",
                    json!({ "rune": "1:0", "page": 1, "limit": 10 }),
                    json!({
                        "ok": true,
                        "total": 105594794,
                        "activity": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "kind": "mint",
                            "amount": "1"
                        }],
                        "entries": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "kind": "mint",
                            "amount": "1"
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_address_activity",
                    "Returns Rune activity for an address and optionally for a single Rune.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "id": "all", "page": 1, "limit": 10 }),
                    json!({
                        "ok": true,
                        "total": 1,
                        "activity": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "kind": "mint",
                            "id": "1:0",
                            "amount": "1"
                        }],
                        "entries": [{
                            "height": 840113,
                            "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2",
                            "kind": "mint",
                            "id": "1:0",
                            "amount": "1"
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_block_tx_count",
                    "Returns the number of Rune transactions indexed for a block.",
                    json!({ "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "count": 12 }),
                ),
                rpc_doc(
                    "runes.get_block_txs",
                    "Returns a range of Rune transaction pointers for a block.",
                    json!({ "height": 946000, "page": 1, "limit": 10 }),
                    json!({
                        "ok": true,
                        "height": 946000,
                        "txs": [{
                            "height": 946000,
                            "tx_index": 235,
                            "txid": "2d70c5d4d4d7a77551cbbd3225542ea167984a28e7bb13bc143546705ac76da6",
                            "io": { "outputs": { "1": { "1:0": "1" } } }
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_address_tx_count",
                    "Returns the number of Rune transactions indexed for an address.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta" }),
                    json!({ "ok": true, "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "count": 48 }),
                ),
                rpc_doc(
                    "runes.get_address_txs",
                    "Returns a range of Rune transaction pointers for an address.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "page": 1, "limit": 10 }),
                    json!({ "ok": true, "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "txs": [{ "height": 840113, "tx_index": 465, "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2" }] }),
                ),
                rpc_doc(
                    "runes.get_action_block_tx_count",
                    "Returns the number of Rune action transactions indexed for a block.",
                    json!({ "height": 946000 }),
                    json!({ "ok": true, "height": 946000, "count": 3 }),
                ),
                rpc_doc(
                    "runes.get_action_block_txs",
                    "Returns Rune action transaction pointers for a block.",
                    json!({ "height": 946000, "start": 0, "end": 10 }),
                    json!({
                        "ok": true,
                        "height": 946000,
                        "txs": [{
                            "height": 946000,
                            "tx_index": 235,
                            "txid": "2d70c5d4d4d7a77551cbbd3225542ea167984a28e7bb13bc143546705ac76da6",
                            "has_rune": true,
                            "has_alkane": false
                        }]
                    }),
                ),
                rpc_doc(
                    "runes.get_action_address_tx_count",
                    "Returns the number of Rune action transactions indexed for an address.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta" }),
                    json!({ "ok": true, "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "count": 65 }),
                ),
                rpc_doc(
                    "runes.get_action_address_txs",
                    "Returns Rune action transaction pointers for an address.",
                    json!({ "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "start": 0, "end": 10 }),
                    json!({ "ok": true, "address": "bc1pc8sn4zzfessnglpvy8mj27z0jkgp3j6ra2l7w3rpjcatf84mhdeqtlveta", "txs": [{ "height": 840113, "tx_index": 465, "txid": "c0ace2bc013a2ea764143aabd41333590cb2ce3db8885c6467e50868143c5cc2", "has_rune": true }] }),
                ),
            ],
        },
        ModuleDoc {
            slug: "tokendata-rpc",
            title: "Token Data JSON-RPC",
            intro: "Token activity views combining mint and AMM sources with time and sort filters.",
            methods: vec![
                rpc_doc(
                    "tokendata.get_token_activity",
                    "Returns token activity for one Alkane across market and mint sources. Market reads can be filtered to a canonical quote pool with a minimum quote-side amount.",
                    json!({
                        "token": "2:68479",
                        "page": 1,
                        "limit": 10,
                        "from": 1700000000,
                        "to": 1800000000,
                        "filter": "market",
                        "sort_by": "timestamp",
                        "dir": "desc",
                        "canonical_quote": "32:0",
                        "min_quote_amount": "100000"
                    }),
                    json!({
                        "ok": true,
                        "total": 11,
                        "entries": [{
                            "kind": "buy",
                            "source": "market",
                            "height": 951279,
                            "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                            "token": "2:68479",
                            "pool": "2:90001",
                            "counter_token": "32:0",
                            "token_delta": "250000000000",
                            "counter_delta": "-100000",
                            "chain_txids": ["f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90"],
                            "success": true
                        }]
                    }),
                ),
                rpc_doc(
                    "tokendata.get_address_activity",
                    "Returns token activity for one address, optionally filtered to a token.",
                    json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "token": "all", "page": 1, "limit": 10, "from": 1700000000, "to": 1800000000 }),
                    json!({
                        "ok": true,
                        "total": 1,
                        "entries": [{
                            "kind": "mint",
                            "height": 951279,
                            "txid": "f390179d0a4586016c834a972abde346f1f0f095e3876513a5c96b8a93194f90",
                            "token": "2:0",
                            "amount": "1000000",
                            "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8"
                        }]
                    }),
                ),
            ],
        },
        ModuleDoc {
            slug: "pizzafun-rpc",
            title: "Pizza.fun JSON-RPC",
            intro: "Pizza.fun series id to Alkane id lookup methods.",
            methods: vec![
                rpc_doc(
                    "pizzafun.get_series_id_from_alkane_id",
                    "Resolves a Pizza.fun series id from a single Alkane id.",
                    json!({ "alkane_id": "2:0" }),
                    json!({ "ok": true, "series_id": "diesel", "alkane_id": "2:0", "confirmations": 100 }),
                ),
                rpc_doc(
                    "pizzafun.get_series_ids_from_alkane_ids",
                    "Batch resolves Pizza.fun series ids from Alkane ids.",
                    json!({ "alkane_ids": ["2:0", "2:77578"] }),
                    json!({ "ok": true, "items": [{ "series_id": "diesel", "alkane_id": "2:0" }, { "series_id": "wai", "alkane_id": "2:77578" }] }),
                ),
                rpc_doc(
                    "pizzafun.get_alkane_id_from_series_id",
                    "Resolves an Alkane id from a single Pizza.fun series id.",
                    json!({ "series_id": "diesel" }),
                    json!({ "ok": true, "series_id": "diesel", "alkane_id": "2:0", "confirmations": 100 }),
                ),
                rpc_doc(
                    "pizzafun.get_alkane_ids_from_series_ids",
                    "Batch resolves Alkane ids from Pizza.fun series ids.",
                    json!({ "series_ids": ["diesel", "wai"] }),
                    json!({ "ok": true, "items": [{ "series_id": "diesel", "alkane_id": "2:0" }, { "series_id": "wai", "alkane_id": "2:77578" }] }),
                ),
            ],
        },
        ModuleDoc {
            slug: "subfrost-rpc",
            title: "Subfrost JSON-RPC",
            intro: "Current frBTC signer and wrap and unwrap event and request history methods.",
            methods: vec![
                rpc_doc(
                    "subfrost.get_signer",
                    "Returns the current Subfrost signer address from the indexed /signer scriptPubKey on frBTC contract 32:0.",
                    json!({}),
                    json!({ "ok": true, "alkane": "32:0", "storage_key": "/signer", "script_pubkey": "0x51201d4830313fb48f68b43b07391fe1232f8488621b2cbc5fb4b26d8935e4bf1cb4", "address": "bcrt1pr4yrqvflkj8k3dpmquu3lcfr97zgscsm9j79ld9jdkynte9lrj6qlcsdcx" }),
                ),
                rpc_doc(
                    "subfrost.get_wrap_events_by_address",
                    "Returns frBTC wrap events for an address.",
                    json!({ "address": "bc1p9qftzwgufdv5h7zk674jvppfjfa9eys56zzflfvvrg4cekpwfy9s3yerpx", "count": 10, "offset": 0, "successful": true }),
                    json!({ "items": [{ "txid": "7edddf6ee5f6e39e503a8c9c7f80ff27285e2da37b61d6728b40f5903f316f36", "amount": "100000", "success": true, "timestamp": 1779308930, "address_spk": "51202aabf8f66f109d8a79e0c922d769bb5b31a85ecaf35920aa8b3d16c1f032c155" }], "total": 3565 }),
                ),
                rpc_doc(
                    "subfrost.get_unwrap_events_by_address",
                    "Returns frBTC unwrap events for an address.",
                    json!({ "address": "bc1q2l9yryzdq82pteuhrjt93cuvgazr5ph8z5zgqw", "count": 10, "offset": 0 }),
                    json!({ "items": [{ "txid": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "amount": "100000", "success": true, "timestamp": 1779285102, "address_spk": "001457ca41904d01d415e7971c9658e38c47443a06e7" }], "total": 2339 }),
                ),
                rpc_doc(
                    "subfrost.get_wrap_events_all",
                    "Returns global frBTC wrap events.",
                    json!({ "count": 10, "offset": 0, "successful": true }),
                    json!({ "items": [{ "txid": "7edddf6ee5f6e39e503a8c9c7f80ff27285e2da37b61d6728b40f5903f316f36", "amount": "100000", "success": true, "timestamp": 1779308930, "address_spk": "51202aabf8f66f109d8a79e0c922d769bb5b31a85ecaf35920aa8b3d16c1f032c155" }], "total": 3565 }),
                ),
                rpc_doc(
                    "subfrost.get_unwrap_events_all",
                    "Returns global frBTC unwrap events.",
                    json!({ "count": 10, "offset": 0 }),
                    json!({ "items": [{ "txid": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "amount": "100000", "success": true, "timestamp": 1779285102, "address_spk": "001457ca41904d01d415e7971c9658e38c47443a06e7" }], "total": 2339 }),
                ),
                rpc_doc(
                    "subfrost.get_unwrap_requests_by_address",
                    "Returns unwrap requests for an address, optionally filtered by fulfillment state.",
                    json!({ "address": "bc1q2l9yryzdq82pteuhrjt93cuvgazr5ph8z5zgqw", "count": 10, "offset": 0, "fulfilled": false }),
                    json!({ "items": [{ "request_txid": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "amount": "100000", "fulfilled": false, "timestamp": 1779285102, "address_spk": "001457ca41904d01d415e7971c9658e38c47443a06e7" }], "total": 1 }),
                ),
                rpc_doc(
                    "subfrost.get_unwrap_requests_all",
                    "Returns global unwrap requests, optionally filtered by fulfillment state.",
                    json!({ "count": 10, "offset": 0, "fulfilled": false }),
                    json!({ "items": [{ "request_txid": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "amount": "100000", "fulfilled": false, "timestamp": 1779285102, "address_spk": "001457ca41904d01d415e7971c9658e38c47443a06e7" }], "total": 1 }),
                ),
            ],
        },
        ModuleDoc {
            slug: "oyl-http",
            title: "Oyl-Compatible HTTP API",
            intro: "POST endpoints served by the oylapi module for wallet and AMM clients.",
            methods: oyl_http_docs(),
        },
        ModuleDoc {
            slug: "explorer-http",
            title: "Explorer HTTP API",
            intro: "HTTP and websocket endpoints used by the explorer interface.",
            methods: explorer_http_docs(),
        },
        ModuleDoc {
            slug: "internal-rpc",
            title: "Internal getter RPC",
            intro: "Getter-level RPC methods that let a remote espo explorer (configured with explorer_espo_rpc_host, which is used verbatim as the endpoint URL and so must include the /rpc path) render entirely from RPC calls against this instance: every storage getter the explorer uses is mirrored as internal.<module>_<getter>, so one getter invocation is exactly one round-trip carrying the getter's full native result. Requests carry the getter's native params struct as \"p\" (serde JSON; blockhash pins versioned state) and responses return the native result as \"r\" — simple results as plain JSON, heavy nested results (creation records, block summaries, tx summaries, pointer blobs, holder pages, rune entries) as hex-encoded Borsh in *_borsh fields, the same encoding they are stored with. Registered only when enable_internal_rpc is true, which requires internal_rpc_key: every request must carry the key as \"auth\" or it is rejected with unauthorized (the remote explorer sends it automatically from explorer_espo_rpc_key). These methods exist for trusted explorer replicas — keep them off untrusted public endpoints. The full method list mirrors the getter names in each module's internal_rpc.rs (essentials, runes, ammdata, tokendata, pizzafun, subfrost) plus internal.tree_blockhash_for_height and internal.tree_indexed_height_bounds.",
            methods: vec![
                rpc_doc(
                    "internal.essentials_get_holders_count",
                    "Representative example of a simple getter: native params in, native result out as plain JSON.",
                    json!({ "auth": "<internal_rpc_key>", "p": { "blockhash": "Latest", "alkane": { "block": 2, "tx": 0 } } }),
                    json!({ "ok": true, "r": { "count": 6409 } }),
                ),
                rpc_doc(
                    "internal.essentials_get_creation_records_ordered_page",
                    "Representative example of a heavy getter: the result rows are hex-encoded Borsh in the stored encoding.",
                    json!({ "auth": "<internal_rpc_key>", "p": { "blockhash": "Latest", "offset": 0, "limit": 2, "desc": true } }),
                    json!({ "ok": true, "r": { "records": ["01a3f2…", "01b871…"] } }),
                ),
                rpc_doc(
                    "internal.tree_blockhash_for_height",
                    "Resolves an indexed height to its canonical blockhash (internal byte order, hex); remote explorers use it for height-pinned views.",
                    json!({ "auth": "<internal_rpc_key>", "p": { "height": 946000 } }),
                    json!({ "ok": true, "r": { "blockhash": "0000000000000000000000000000000000000000000000000000000000000000" } }),
                ),
                rpc_doc(
                    "internal.tree_indexed_height_bounds",
                    "Returns the (min, max) indexed heights of the versioned tree, or null when nothing is indexed.",
                    json!({ "auth": "<internal_rpc_key>", "p": {} }),
                    json!({ "ok": true, "r": { "bounds": [880000, 946000] } }),
                ),
            ],
        },
    ]
}

pub(super) fn rpc_doc(
    method: &'static str,
    description: &'static str,
    params: serde_json::Value,
    result: serde_json::Value,
) -> MethodDoc {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let response = json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": 1,
    });
    MethodDoc {
        anchor: anchor_for(method),
        title: method.to_string(),
        badge: "JSON-RPC".to_string(),
        transport: "JSON-RPC",
        description,
        query_prefix: Some(format!("POST {}", rpc_docs_endpoint())),
        query_fallback: pretty(&request),
        query_json: Some(request),
        response_fallback: pretty(&response),
        response_json: response,
    }
}

fn http_doc(
    http_method: &'static str,
    path: &'static str,
    description: &'static str,
    body: serde_json::Value,
    response: serde_json::Value,
) -> MethodDoc {
    http_doc_for_host(
        get_config().hosts.oyl_api_host.as_deref(),
        http_method,
        path,
        description,
        body,
        response,
    )
}

fn explorer_http_doc(
    http_method: &'static str,
    path: &'static str,
    description: &'static str,
    body: serde_json::Value,
    response: serde_json::Value,
) -> MethodDoc {
    http_doc_for_host(
        get_config().hosts.explorer_host.as_deref(),
        http_method,
        path,
        description,
        body,
        response,
    )
}

fn http_doc_for_host(
    host: Option<&str>,
    http_method: &'static str,
    path: &'static str,
    description: &'static str,
    body: serde_json::Value,
    response: serde_json::Value,
) -> MethodDoc {
    let endpoint = docs_endpoint(host, path);
    let query =
        if http_method == "GET" { format!("{http_method} {endpoint}") } else { pretty(&body) };
    MethodDoc {
        anchor: anchor_for(path),
        title: path.to_string(),
        badge: http_method.to_string(),
        transport: "HTTP",
        description,
        query_prefix: if http_method == "GET" {
            None
        } else {
            Some(format!("{http_method} {endpoint}"))
        },
        query_json: if http_method == "GET" { None } else { Some(body) },
        query_fallback: query,
        response_fallback: pretty(&response),
        response_json: response,
    }
}

fn ws_doc(
    path: &'static str,
    description: &'static str,
    payload: serde_json::Value,
    response: serde_json::Value,
) -> MethodDoc {
    let endpoint = docs_endpoint(get_config().hosts.explorer_host.as_deref(), path);
    MethodDoc {
        anchor: anchor_for(path),
        title: path.to_string(),
        badge: "WEBSOCKET".to_string(),
        transport: "WEBSOCKET",
        description,
        query_prefix: Some(format!("WEBSOCKET {endpoint}")),
        query_fallback: pretty(&payload),
        query_json: Some(payload),
        response_fallback: pretty(&response),
        response_json: response,
    }
}

fn oyl_http_docs() -> Vec<MethodDoc> {
    vec![
        http_doc(
            "POST",
            "/get-alkanes-by-address",
            "Returns Alkane balances and metadata for a Bitcoin address.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
            json!({ "statusCode": 200, "data": [{ "alkaneId": { "block": "2", "tx": "0" }, "name": "DIESEL", "symbol": "DIESEL", "balance": "26064398814169", "priceInSatoshi": "10927011166337", "floorPrice": 82.07730465298008 }] }),
        ),
        http_doc(
            "POST",
            "/get-bitcoin-price",
            "Returns the cached BTC/USD price used by Oyl-compatible responses.",
            json!({}),
            json!({ "price": "65000" }),
        ),
        http_doc(
            "POST",
            "/get-alkanes-utxo",
            "Returns Alkane UTXOs for an address.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8" }),
            json!({ "statusCode": 200, "data": [{ "txId": "b8cb5e85a7d024fb26c85e29678a377e257dd04c0a19905de6b1d7e3cd772c14", "outputIndex": 0, "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "satoshis": 546, "confirmations": 11735, "indexed": true, "alkanes": { "2:77183": "1" }, "runes": {} }] }),
        ),
        http_doc(
            "POST",
            "/get-address-utxos",
            "Returns portfolio UTXOs for an address with optional spend strategy filtering.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "spendStrategy": null }),
            json!({ "statusCode": 200, "data": { "totalBalance": 244280, "spendableTotalBalance": 239912, "pendingTotalBalance": 0, "alkaneUtxos": [{ "txId": "b8cb5e85a7d024fb26c85e29678a377e257dd04c0a19905de6b1d7e3cd772c14", "outputIndex": 0, "satoshis": 546, "alkanes": { "2:77183": "1" } }], "spendableUtxos": [{ "txId": "b7d8b76bec7b9a703cbd39b311e7edb6ed0951c1c856fcbe6581396247d9afcf", "outputIndex": 1, "satoshis": 239912 }] } }),
        ),
        http_doc(
            "POST",
            "/get-account-utxos",
            "Alias of get-address-utxos for clients that use account terminology.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "spendStrategy": null }),
            json!({ "statusCode": 200, "data": { "totalBalance": 244280, "spendableTotalBalance": 239912, "alkaneUtxos": [{ "txId": "b8cb5e85a7d024fb26c85e29678a377e257dd04c0a19905de6b1d7e3cd772c14", "outputIndex": 0, "satoshis": 546, "alkanes": { "2:77183": "1" } }] } }),
        ),
        http_doc(
            "POST",
            "/get-amm-utxos",
            "Returns UTXOs suitable for AMM transactions for an address.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "spendStrategy": null }),
            json!({ "statusCode": 200, "data": { "utxos": [{ "txId": "b8cb5e85a7d024fb26c85e29678a377e257dd04c0a19905de6b1d7e3cd772c14", "outputIndex": 0, "satoshis": 546, "confirmations": 11735, "alkanes": { "2:77183": "1" } }] } }),
        ),
        http_doc(
            "POST",
            "/get-alkanes",
            "Lists Alkanes with pagination, sorting, and optional search query.",
            json!({ "limit": 20, "offset": 0, "sort_by": "holders", "order": "desc", "searchQuery": "DIESEL" }),
            json!({ "statusCode": 200, "data": { "count": 1, "limit": 20, "offset": 0, "total": 1, "tokens": [{ "alkaneId": { "block": "2", "tx": "0" }, "name": "DIESEL", "symbol": "DIESEL", "holders": 6409, "fdvUsd": 47790983.31 }] } }),
        ),
        http_doc(
            "POST",
            "/global-alkanes-search",
            "Searches Alkanes globally by name, symbol, or id.",
            json!({ "searchQuery": "DIESEL" }),
            json!({ "statusCode": 200, "data": { "tokens": [{ "alkaneId": { "block": "2", "tx": "0" }, "name": "DIESEL", "symbol": "DIESEL" }], "pools": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4" }] } }),
        ),
        http_doc(
            "POST",
            "/get-alkane-details",
            "Returns detailed metadata and market fields for one Alkane.",
            json!({ "alkaneId": { "block": "2", "tx": "0" } }),
            json!({ "alkane": { "id": "2:0" } }),
        ),
        http_doc(
            "POST",
            "/get-pools",
            "Lists pools for a factory.",
            json!({ "factoryId": { "block": "4", "tx": "65522" }, "limit": 20, "offset": 0 }),
            json!({ "statusCode": 200, "limit": 20, "offset": 0, "total": 149, "data": [{ "block": "2", "tx": "53014" }, { "block": "2", "tx": "53044" }] }),
        ),
        http_doc(
            "POST",
            "/get-pool-details",
            "Returns details for one factory pool.",
            json!({ "factoryId": { "block": "4", "tx": "65522" }, "poolId": { "block": "2", "tx": "53014" } }),
            json!({ "pool": { "id": "2:53014" } }),
        ),
        http_doc(
            "POST",
            "/get-pool-swap-history",
            "Returns swap history for a pool.",
            json!({ "poolId": { "block": "2", "tx": "53014" }, "count": 20, "offset": 0, "successful": true, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": { "pool": { "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4" }, "swaps": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "soldTokenBlockId": "2", "soldTokenTxId": "0", "boughtTokenBlockId": "2", "boughtTokenTxId": "16", "soldAmount": "11486258", "boughtAmount": "1061552883247", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 2868 }, "count": 1, "offset": 0, "total": 2868 } }),
        ),
        http_doc(
            "POST",
            "/get-token-swap-history",
            "Returns swap history for a token across pools.",
            json!({ "tokenId": { "block": "2", "tx": "0" }, "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "poolBlockId": "2", "poolTxId": "53014", "soldTokenBlockId": "2", "soldTokenTxId": "0", "boughtTokenBlockId": "2", "boughtTokenTxId": "16", "soldAmount": "11486258", "boughtAmount": "1061552883247", "sellerAddress": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 19041 } }),
        ),
        http_doc(
            "POST",
            "/get-pool-mint-history",
            "Returns liquidity mint history for a pool.",
            json!({ "poolId": { "block": "2", "tx": "53014" }, "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "89c7a58ba8dc9e7efda8cd8d50cf397313999f69b43149c4214ff6a15507c7c2", "poolBlockId": "2", "poolTxId": "53014", "token0BlockId": "2", "token0TxId": "0", "token1BlockId": "2", "token1TxId": "16", "token0Amount": "244212912", "token1Amount": "20600", "lpTokenAmount": "70915448", "minterAddress": "bc1pf8c420ues7wvh0fgmh56675xsm4as33ms006tee876h3v08y5yqqfruu2w", "timestamp": "2025-02-18T21:08:26Z" }], "count": 1, "offset": 0, "total": 296 } }),
        ),
        http_doc(
            "POST",
            "/get-pool-burn-history",
            "Returns liquidity burn history for a pool.",
            json!({ "poolId": { "block": "2", "tx": "53014" }, "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "fd6f91549cc1ea3d5803406a9ab8679562e3d780f4bb099320b490af0680262f", "poolBlockId": "2", "poolTxId": "53014", "token0BlockId": "2", "token0TxId": "0", "token1BlockId": "2", "token1TxId": "16", "token0Amount": "8128760", "token1Amount": "609138011383", "lpTokenAmount": "2366448", "burnerAddress": "bc1qnlaz6rt6734pfd23ehx68nyczs5pfjdp6ct0aa", "timestamp": "2026-05-13T14:24:53Z" }], "count": 1, "offset": 0, "total": 159 } }),
        ),
        http_doc(
            "POST",
            "/get-pool-creation-history",
            "Returns pool creation events.",
            json!({ "poolId": null, "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "89c7a58ba8dc9e7efda8cd8d50cf397313999f69b43149c4214ff6a15507c7c2", "poolBlockId": "2", "poolTxId": "53014", "token0BlockId": "2", "token0TxId": "0", "token1BlockId": "2", "token1TxId": "16", "token0Amount": "244212912", "token1Amount": "20600", "creatorAddress": "bc1pf8c420ues7wvh0fgmh56675xsm4as33ms006tee876h3v08y5yqqfruu2w", "timestamp": "2025-02-18T21:08:26Z" }], "count": 1, "offset": 0, "total": 149 } }),
        ),
        http_doc(
            "POST",
            "/get-address-swap-history-for-pool",
            "Returns swap history for an address in one pool.",
            json!({ "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "poolId": { "block": "2", "tx": "53014" }, "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "poolBlockId": "2", "poolTxId": "53014", "soldTokenBlockId": "2", "soldTokenTxId": "0", "boughtTokenBlockId": "2", "boughtTokenTxId": "16", "soldAmount": "11486258", "boughtAmount": "1061552883247", "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/get-address-swap-history-for-token",
            "Returns swap history for an address and one token.",
            json!({ "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "tokenId": { "block": "2", "tx": "0" }, "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "poolBlockId": "2", "poolTxId": "53014", "soldTokenBlockId": "2", "soldTokenTxId": "0", "boughtTokenBlockId": "2", "boughtTokenTxId": "16", "soldAmount": "11486258", "boughtAmount": "1061552883247", "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/get-address-wrap-history",
            "Returns frBTC wrap events for an address.",
            json!({ "address": "bc1p9qftzwgufdv5h7zk674jvppfjfa9eys56zzflfvvrg4cekpwfy9s3yerpx", "count": 20, "offset": 0, "successful": true, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "7edddf6ee5f6e39e503a8c9c7f80ff27285e2da37b61d6728b40f5903f316f36", "address": "bc1p9qftzwgufdv5h7zk674jvppfjfa9eys56zzflfvvrg4cekpwfy9s3yerpx", "amount": "100000", "timestamp": "2026-05-20T16:28:50Z" }], "count": 1, "offset": 0, "total": 3565 } }),
        ),
        http_doc(
            "POST",
            "/get-address-unwrap-history",
            "Returns frBTC unwrap events for an address.",
            json!({ "address": "bc1q2l9yryzdq82pteuhrjt93cuvgazr5ph8z5zgqw", "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "address": "bc1q2l9yryzdq82pteuhrjt93cuvgazr5ph8z5zgqw", "amount": "100000", "timestamp": "2026-05-20T09:51:42Z" }], "count": 1, "offset": 0, "total": 2339 } }),
        ),
        http_doc(
            "POST",
            "/get-all-wrap-history",
            "Returns global frBTC wrap events.",
            json!({ "count": 20, "offset": 0, "successful": true, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "7edddf6ee5f6e39e503a8c9c7f80ff27285e2da37b61d6728b40f5903f316f36", "address": "bc1p9qftzwgufdv5h7zk674jvppfjfa9eys56zzflfvvrg4cekpwfy9s3yerpx", "amount": "100000", "timestamp": "2026-05-20T16:28:50Z" }], "count": 1, "offset": 0, "total": 3565 } }),
        ),
        http_doc(
            "POST",
            "/get-all-unwrap-history",
            "Returns global frBTC unwrap events.",
            json!({ "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "b1267c0dcf9de9fb8a8b5bb4d34da75ae7dca36abf3361906c7f83c6e661f155", "address": "bc1q2l9yryzdq82pteuhrjt93cuvgazr5ph8z5zgqw", "amount": "100000", "timestamp": "2026-05-20T09:51:42Z" }], "count": 1, "offset": 0, "total": 2339 } }),
        ),
        http_doc(
            "POST",
            "/get-total-unwrap-amount",
            "Returns the total unwrapped amount at latest or at a block height.",
            json!({ "blockHeight": 946000, "successful": true }),
            json!({ "amount": "100000000" }),
        ),
        http_doc(
            "POST",
            "/get-address-pool-creation-history",
            "Returns pool creation history associated with an address.",
            json!({ "address": "bc1pqd8pq7pchx2wheh5gx0rgg6rkt7en3pvlrhphaq4gntvnt3mm7dqxwzm2e", "poolId": { "block": "2", "tx": "53014" }, "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "89c7a58ba8dc9e7efda8cd8d50cf397313999f69b43149c4214ff6a15507c7c2", "poolBlockId": "2", "poolTxId": "53014", "token0BlockId": "2", "token0TxId": "0", "token1BlockId": "2", "token1TxId": "16", "creatorAddress": "bc1pqd8pq7pchx2wheh5gx0rgg6rkt7en3pvlrhphaq4gntvnt3mm7dqxwzm2e", "timestamp": "2025-02-18T21:08:26Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/get-address-pool-mint-history",
            "Returns liquidity mint history associated with an address.",
            json!({ "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "89c7a58ba8dc9e7efda8cd8d50cf397313999f69b43149c4214ff6a15507c7c2", "poolBlockId": "2", "poolTxId": "53014", "token0Amount": "244212912", "token1Amount": "20600", "lpTokenAmount": "70915448", "minterAddress": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "timestamp": "2025-02-18T21:08:26Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/get-address-pool-burn-history",
            "Returns liquidity burn history associated with an address.",
            json!({ "address": "bc1pmwsf07u2s53gfq38jlafq4w0dgw3x5kquh2ayx465nnsghphjrjqs5k0l7", "count": 20, "offset": 0 }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "fd6f91549cc1ea3d5803406a9ab8679562e3d780f4bb099320b490af0680262f", "poolBlockId": "2", "poolTxId": "53014", "token0Amount": "8128760", "token1Amount": "609138011383", "lpTokenAmount": "2366448", "burnerAddress": "bc1pmwsf07u2s53gfq38jlafq4w0dgw3x5kquh2ayx465nnsghphjrjqs5k0l7", "timestamp": "2026-05-13T14:24:53Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/address-positions",
            "Returns an address position summary for pools under a factory.",
            json!({ "address": "bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8", "factoryId": { "block": "4", "tx": "65522" } }),
            json!({ "statusCode": 200, "data": { "positions": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "lpBalance": "2366448", "token0Amount": "8128760", "token1Amount": "609138011383", "valueInUsd": 96.22 }] } }),
        ),
        http_doc(
            "POST",
            "/get-all-pools-details",
            "Returns detailed pool rows for a factory with search, sort, paging, and optional address filtering.",
            json!({ "factoryId": { "block": "4", "tx": "65522" }, "limit": 20, "offset": 0, "sort_by": "volume", "order": "desc", "searchQuery": "", "address": null }),
            json!({ "statusCode": 200, "data": { "count": 1, "limit": 20, "offset": 0, "total": 149, "totalPoolVolume24h": 676270.3489941867, "largestPool": { "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "poolApr": 14.7428, "creationBlockHeight": 916291 }, "pools": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "reserve0": "21486066722", "reserve1": "1624687948631013", "poolTvlInUsd": 35819.907581967265, "poolVolume1dInUsd": 15526.198504698415 }] } }),
        ),
        http_doc(
            "POST",
            "/get-all-address-amm-tx-history",
            "Returns all AMM transaction history for an address.",
            json!({ "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "transactionType": "swap", "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "type": "swap", "poolBlockId": "2", "poolTxId": "53014", "soldAmount": "11486258", "boughtAmount": "1061552883247", "address": "bc1p4g0w7n3yjuzpx5s6umw03mzca49ktkmvxm976nyv0k272m2vl48slrrw5l", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 1 } }),
        ),
        http_doc(
            "POST",
            "/get-all-amm-tx-history",
            "Returns global AMM transaction history.",
            json!({ "transactionType": "swap", "count": 20, "offset": 0, "includeTotal": true }),
            json!({ "statusCode": 200, "data": { "items": [{ "transactionId": "02146673f5ba09a67042626a21727466a3ac4c68bdce49e12fd9f734133489d3", "type": "swap", "poolBlockId": "2", "poolTxId": "53014", "soldTokenBlockId": "2", "soldTokenTxId": "0", "boughtTokenBlockId": "2", "boughtTokenTxId": "16", "soldAmount": "11486258", "boughtAmount": "1061552883247", "timestamp": "2026-05-26T21:34:29Z" }], "count": 1, "offset": 0, "total": 25372 } }),
        ),
        http_doc(
            "POST",
            "/get-all-token-pairs",
            "Returns all token pairs known for a factory.",
            json!({ "factoryId": { "block": "4", "tx": "65522" } }),
            json!({ "statusCode": 200, "data": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "reserve0": "21486066722", "reserve1": "1624687948631013", "poolTvlInUsd": 35819.907581967265, "poolVolume1dInUsd": 15526.198504698415, "token0": { "alkaneId": { "block": "2", "tx": "0" }, "name": "DIESEL", "symbol": "DIESEL", "token0Amount": "21486066722" }, "token1": { "alkaneId": { "block": "2", "tx": "16" }, "name": "CH4", "symbol": "CH4", "token1Amount": "1624687948631013" } }] }),
        ),
        http_doc(
            "POST",
            "/get-token-pairs",
            "Returns pairs for one token under a factory.",
            json!({ "factoryId": { "block": "4", "tx": "65522" }, "alkaneId": { "block": "2", "tx": "0" }, "sort_by": "volume", "limit": 20, "offset": 0, "searchQuery": "" }),
            json!({ "statusCode": 200, "data": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "reserve0": "21486066722", "reserve1": "1624687948631013", "poolTvlInUsd": 35819.907581967265, "poolVolume1dInUsd": 15526.198504698415, "token0": { "alkaneId": { "block": "2", "tx": "0" }, "name": "DIESEL", "symbol": "DIESEL" }, "token1": { "alkaneId": { "block": "2", "tx": "16" }, "name": "CH4", "symbol": "CH4" } }] }),
        ),
        http_doc(
            "POST",
            "/get-alkane-swap-pair-details",
            "Returns details for a token pair under a factory.",
            json!({ "factoryId": { "block": "4", "tx": "65522" }, "tokenAId": { "block": "2", "tx": "0" }, "tokenBId": { "block": "2", "tx": "16" } }),
            json!({ "statusCode": 200, "data": [{ "path": [{ "block": "2", "tx": "0" }, { "block": "2", "tx": "16" }], "pools": [{ "poolId": { "block": "2", "tx": "53014" }, "poolName": "DIESEL / CH4", "reserve0": "21486066722", "reserve1": "1624687948631013" }] }] }),
        ),
    ]
}

fn explorer_http_docs() -> Vec<MethodDoc> {
    let http_doc = explorer_http_doc;
    vec![
        http_doc(
            "GET",
            "/api/blocks/carousel?height=946000",
            "Returns compact block data for the homepage carousel.",
            json!({}),
            json!({
                "espo_tip": 951279,
                "blocks": [{
                    "height": 951271,
                    "shell": false,
                    "traces": 2070,
                    "median_fee_rate": 1.9523132154711105,
                    "min_fee_rate": 1.0068649885583525,
                    "max_fee_rate": 301.58653846153845,
                    "fee_range": [1.0068649885583525, 1.1, 1.9523132154711105, 4.2, 301.58653846153845],
                    "tx_count": 4501
                }]
            }),
        ),
        http_doc(
            "GET",
            "/api/block/pool?height=946000",
            "Returns mining pool attribution for a block.",
            json!({}),
            json!({ "height": 946000, "pool": "unknown" }),
        ),
        http_doc(
            "GET",
            "/api/mempool/blocks",
            "Returns projected mempool block summaries for the explorer.",
            json!({}),
            json!({
                "tx_count": 97385,
                "updated_at": 1779900427,
                "sequence": 1449,
                "status": { "phase": "in_sync", "in_sync": true, "hydrating": false, "stale": false },
                "blocks": [{
                    "index": 0,
                    "tx_count": 3879,
                    "trace_count": 2119,
                    "weight": 3987528,
                    "vsize": 998957,
                    "total_fees": 2224788,
                    "median_fee_rate": 1.3,
                    "min_fee_rate": 1.001531393568147
                }],
                "deltas": [{ "index": 0, "tx_count": 3879, "trace_count": 2119 }]
            }),
        ),
        http_doc(
            "GET",
            "/api/faucet/status",
            "Returns B8's separate rBTC and DIESEL faucet settings, spendable balances, minimum and maximum request amounts, rolling 24-hour usage, and configured caps when the regtest faucet is enabled. The top-level fields mirror rBTC for compatibility.",
            json!({}),
            json!({
                "id": 1,
                "jsonrpc": "2.0",
                "result": {
                    "amount": 1.0,
                    "enabled": true,
                    "max_amount": 1.0,
                    "rbtc": {
                        "amount": 1.0,
                        "claims_last_24h": 2,
                        "enabled": true,
                        "min_amount": 0.1,
                        "max_amount": 1.0,
                        "total_available": 149.5,
                        "max_per_address_per_day": 10.0,
                        "max_per_day": 500.0,
                        "max_per_ip_per_day": 10.0,
                        "sent_last_24h": 2.0
                    },
                    "diesel": {
                        "amount": 10.0,
                        "claims_last_24h": 3,
                        "enabled": true,
                        "min_amount": 1.0,
                        "max_amount": 10.0,
                        "total_available": 3200.0,
                        "max_per_address_per_day": 100.0,
                        "max_per_day": 5000.0,
                        "max_per_ip_per_day": 100.0,
                        "sent_last_24h": 8.0
                    }
                }
            }),
        ),
        http_doc(
            "POST",
            "/api/faucet/send",
            "Requests rBTC or DIESEL within that asset's configured B8 faucet minimum and maximum for one regtest address. asset defaults to rbtc when omitted. Omitting amount retains B8's maximum-payout behavior. Available only on regtest when b8_faucet_url is configured.",
            json!({ "address": "bcrt1q...", "amount": 3.0, "asset": "diesel" }),
            json!({
                "id": 1,
                "jsonrpc": "2.0",
                "result": {
                    "amount": 3.0,
                    "asset": "diesel",
                    "txid": "7be14a09c9..."
                }
            }),
        ),
        http_doc(
            "GET",
            "/api/search/guess?q=2%3A0",
            "Returns search suggestions and target type guesses.",
            json!({}),
            json!({
                "query": "2:0",
                "groups": [{
                    "kind": "alkanes",
                    "title": "Alkanes",
                    "items": [{ "label": "DIESEL", "href": "/alkane/2:0", "subtitle": "2:0" }]
                }]
            }),
        ),
        http_doc(
            "POST",
            "/api/alkane/simulate",
            "Simulates an Alkane contract call from the explorer.",
            json!({ "alkane": "2:0", "opcode": 99, "inputs": [] }),
            json!({ "ok": true, "result": null }),
        ),
        http_doc(
            "GET",
            "/api/alkane/holders/export?alkane=2%3A0",
            "Exports holders for an Alkane as a downloadable response.",
            json!({}),
            json!({ "content_type": "text/csv" }),
        ),
        http_doc(
            "GET",
            "/api/alkane/wasm/export?alkane=2%3A0",
            "Downloads the resolved contract WASM. Proxy implementations and factory clones use the same indexed source resolution as Alkabi exports.",
            json!({}),
            json!({ "content_type": "application/wasm", "filename": "alkane-2-0.wasm" }),
        ),
        http_doc(
            "GET",
            "/api/alkane/chart?alkane=2%3A16&source=derived&quote=2%3A0",
            "Returns chart series data for an Alkane metric.",
            json!({}),
            json!({ "ok": true, "available": true, "range": "3m", "source": "derived", "quote": "2:0", "candles": [{ "ts": 1779890400, "close": 0.00001234 }], "error": null }),
        ),
        http_doc(
            "GET",
            "/api/alkane/balance-chart?alkane=2%3A53014&balance_alkane=2%3A0",
            "Returns balance chart data for an address and Alkane.",
            json!({}),
            json!({ "ok": true, "available": true, "range": "1d", "points": [{ "height": 951000, "value": 214.86066722 }], "error": null }),
        ),
        http_doc(
            "GET",
            "/api/minting-price-chart?alkane=2%3A0",
            "Returns minting price chart data for an Alkane.",
            json!({}),
            json!({ "ok": true, "available": true, "range": "all", "points": [{ "height": 880500, "value": 0.495472 }, { "height": 881000, "value": 0.0990944 }], "error": null }),
        ),
        http_doc(
            "GET",
            "/api/address/chart?address=bc1phqvgwn7wn5e4s8g0999rtgafd07jpuuy59rkdrk4s5thw9jafkasg8umr8&alkane=2%3A0",
            "Returns address-level chart data for explorer pages.",
            json!({}),
            json!({ "ok": true, "available": true, "range": "all", "points": [{ "height": 946000, "value": 309500.01348973 }], "error": null }),
        ),
        http_doc(
            "GET",
            "/api/rune/holders/export?rune=1%3A0",
            "Exports holders for a Rune when the runes module is enabled.",
            json!({}),
            json!({ "content_type": "text/csv" }),
        ),
        ws_doc(
            "/api/events/ws",
            "Streams explorer events when websocket support is enabled. Clients receive only the categories and identifiers requested with `action: want`: `block`, `mempool-blocks`, a specific `txid`, or a specific `address`. Transaction and address batches are narrowed server-side to the tracked values. A transaction request returns its current status immediately and then streams only that transaction's mempool and confirmation changes on the same connection.",
            json!({ "action": "want", "data": ["tx"], "txid": "4d3f...9a10" }),
            json!({ "type": "tx-status", "data": { "txid": "4d3f...9a10", "status": "confirmed", "height": 958411, "timestamp": 1784382000, "confirmations": 2 } }),
        ),
    ]
}

fn pretty(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::{HTTP_BASE, docs_endpoint, rpc_endpoint};

    #[test]
    fn documentation_endpoints_use_configured_hosts_without_double_slashes() {
        assert_eq!(
            docs_endpoint(Some("https://explorer.example.com/"), "/api/blocks"),
            "https://explorer.example.com/api/blocks"
        );
        assert_eq!(
            docs_endpoint(Some("https://oyl.example.com"), "/get-bitcoin-price"),
            "https://oyl.example.com/get-bitcoin-price"
        );
    }

    #[test]
    fn rpc_endpoint_accepts_a_host_or_complete_rpc_endpoint() {
        assert_eq!(rpc_endpoint(Some("https://rpc.example.com/")), "https://rpc.example.com/rpc");
        assert_eq!(
            rpc_endpoint(Some("https://rpc.example.com/rpc")),
            "https://rpc.example.com/rpc"
        );
        assert_eq!(rpc_endpoint(None), format!("{HTTP_BASE}/rpc"));
    }
}

fn anchor_for(raw: &str) -> String {
    raw.trim_matches('/')
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}
