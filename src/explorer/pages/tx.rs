use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Network, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::{Markup, PreEscaped, html};

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoTrace, extract_alkane_storage, protobuf_trace_events,
    traces_for_block_as_prost,
};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_metashrew,
};
use crate::explorer::api::cached_bitcoin_chain_tip_height;
use crate::explorer::components::block_carousel::{block_carousel, block_carousel_with_mempool};
use crate::explorer::components::header::{
    HeaderCta, HeaderPillTone, HeaderProps, HeaderSummaryItem, header, header_scripts,
};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::icon_arrow_up_right;
use crate::explorer::components::tx_view::{TxPill, TxPillTone, render_tx};
use crate::explorer::pages::block::mempool_block_projected_balances;
use crate::explorer::pages::common::format_fee_rate;
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::BalanceEntry;
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent, get_outpoint_balances_with_spent_batch,
};
use crate::runtime::mempool::{
    get_mempool_block_spenders, get_mempool_block_transactions_for_targets, get_mempool_outspends,
    pending_by_txid,
};
use crate::runtime::state_at::StateAt;

fn format_with_commas(n: u64) -> String {
    let mut s = n.to_string();
    let mut i = s.len() as isize - 3;
    while i > 0 {
        s.insert(i as usize, ',');
        i -= 3;
    }
    s
}

fn format_sats_short(n: u64) -> String {
    format!("{} sats", format_with_commas(n))
}

fn mempool_tx_url(network: Network, txid: &Txid) -> Option<String> {
    let base = match network {
        Network::Bitcoin => "https://mempool.space",
        Network::Testnet => "https://mempool.space/testnet",
        Network::Signet => "https://mempool.space/signet",
        Network::Regtest => return None,
        _ => "https://mempool.space",
    };
    Some(format!("{base}/tx/{txid}"))
}

fn tx_event_listener_script(txid: &Txid) -> Markup {
    tx_event_listener_script_with_block_prefix(txid, &explorer_path("/block/"))
}

fn tx_event_listener_script_with_block_prefix(txid: &Txid, block_prefix: &str) -> Markup {
    let txid_js = serde_json::to_string(&txid.to_string()).unwrap_or_else(|_| "\"\"".into());
    let block_prefix_js =
        serde_json::to_string(block_prefix).unwrap_or_else(|_| "\"/block/\"".into());

    PreEscaped(format!(
        r#"
<script data-tx-event-listener="">
(() => {{
  const txid = {txid_js};
  const blockPrefix = {block_prefix_js};
  const events = window.__espoBlockCarouselEvents;
  let liveRefreshInFlight = false;
  let liveRefreshQueued = false;
  let liveRefreshRetryTimer = null;
  let liveRefreshTargetHeight = null;
  let liveRefreshAttempt = 0;
  let listenerDisposed = false;
  let confirmedHeight = null;
  let latestTip = null;
  let indexedContentLoaded = document.querySelector('[data-tx-live-content]')?.dataset.txState === 'confirmed';

  const initHeaderInteractions = () => {{
    document.querySelectorAll('[data-copy-btn]').forEach((btn) => {{
      if (btn.dataset.copyBound === '1') return;
      btn.dataset.copyBound = '1';
      const label = btn.querySelector('[data-copy-label]');
      const value = btn.dataset.copyValue || '';
      if (!value) return;
      const markCopied = () => {{
        btn.dataset.copied = '1';
        if (label) label.textContent = 'Copied';
        setTimeout(() => {{
          btn.dataset.copied = '';
          if (label) label.textContent = 'Copy';
        }}, 1000);
      }};
      btn.addEventListener('click', async () => {{
        try {{
          if (navigator.clipboard && navigator.clipboard.writeText) {{
            await navigator.clipboard.writeText(value);
            markCopied();
            return;
          }}
        }} catch (_) {{}}
        const ta = document.createElement('textarea');
        ta.value = value;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        try {{
          document.execCommand('copy');
          markCopied();
        }} catch (_) {{
          btn.dataset.error = '1';
        }}
        ta.remove();
      }});
    }});

    const formatRel = (ts) => {{
      const diff = Math.max(0, Date.now() / 1000 - ts);
      const mins = Math.floor(diff / 60);
      const hrs = Math.floor(mins / 60);
      const days = Math.floor(hrs / 24);
      if (days > 365) return `${{Math.floor(days / 365)}}y ago`;
      if (days > 30) return `${{Math.floor(days / 30)}}mo ago`;
      if (days > 0) return `${{days}}d ago`;
      if (hrs > 0) return `${{hrs}}h ago`;
      if (mins > 0) return `${{mins}}m ago`;
      return 'just now';
    }};
    document.querySelectorAll('[data-ts-group]').forEach((group) => {{
      const tsNode = group.querySelector('[data-header-ts]');
      if (!tsNode) return;
      const raw = Number(tsNode.dataset.headerTs);
      if (!Number.isFinite(raw)) return;
      const date = new Date(raw * 1000);
      const formatter = new Intl.DateTimeFormat(undefined, {{ dateStyle: 'medium', timeStyle: 'short' }});
      const formattedDate = formatter.format(date);
      tsNode.textContent = formattedDate;
      const relNode = group.querySelector('[data-header-ts-rel]');
      if (relNode) {{
        relNode.textContent = relNode.hasAttribute('data-rel-only')
          ? formatRel(raw)
          : `(${{formatRel(raw)}})`;
        relNode.title = formattedDate;
      }}
    }});
  }};

  const arrayIncludesTxid = (value) => Array.isArray(value) && value.includes(txid);

  const isWaiting = () => Boolean(document.querySelector('[data-tx-waiting="1"]'));

  const clearLiveRefreshRetry = () => {{
    if (liveRefreshRetryTimer) window.clearTimeout(liveRefreshRetryTimer);
    liveRefreshRetryTimer = null;
    liveRefreshTargetHeight = null;
    liveRefreshAttempt = 0;
  }};

  const scheduleLiveRefreshRetry = () => {{
    if (
      listenerDisposed ||
      indexedContentLoaded ||
      liveRefreshRetryTimer ||
      !Number.isFinite(liveRefreshTargetHeight)
    ) return;
    const delay = Math.min(5_000, 250 * (2 ** Math.min(liveRefreshAttempt, 5)));
    liveRefreshAttempt += 1;
    liveRefreshRetryTimer = window.setTimeout(() => {{
      liveRefreshRetryTimer = null;
      refreshLiveContent(liveRefreshTargetHeight);
    }}, delay);
  }};

  const refreshLiveContent = async (expectedConfirmedHeight = null) => {{
    const hasExpectedHeight = expectedConfirmedHeight !== null
      && expectedConfirmedHeight !== undefined
      && expectedConfirmedHeight !== '';
    const expectedHeight = hasExpectedHeight ? Number(expectedConfirmedHeight) : Number.NaN;
    if (Number.isFinite(expectedHeight) && expectedHeight >= 0) {{
      if (liveRefreshTargetHeight !== expectedHeight) liveRefreshAttempt = 0;
      liveRefreshTargetHeight = expectedHeight;
    }}
    if (listenerDisposed) return;
    if (liveRefreshInFlight) {{
      liveRefreshQueued = true;
      return;
    }}
    liveRefreshInFlight = true;
    let needsRetry = false;
    try {{
      const response = await fetch(window.location.href, {{
        cache: 'no-store',
        headers: {{ Accept: 'text/html' }}
      }});
      if (!response.ok) {{
        needsRetry = Number.isFinite(liveRefreshTargetHeight);
        return;
      }}
      const text = await response.text();
      const doc = new DOMParser().parseFromString(text, 'text/html');
      const next = doc.querySelector('[data-tx-live-content]');
      const current = document.querySelector('[data-tx-live-content]');
      if (!next || !current) {{
        needsRetry = Number.isFinite(liveRefreshTargetHeight);
        return;
      }}
      if (Number.isFinite(liveRefreshTargetHeight) && next.dataset.txState !== 'confirmed') {{
        needsRetry = true;
        return;
      }}
      next.querySelectorAll('script').forEach((script) => script.remove());
      current.replaceWith(next);
      indexedContentLoaded = next.dataset.txState === 'confirmed';
      if (indexedContentLoaded) clearLiveRefreshRetry();
      initHeaderInteractions();
    }} catch (_) {{
      needsRetry = Number.isFinite(liveRefreshTargetHeight);
    }} finally {{
      liveRefreshInFlight = false;
      if (liveRefreshQueued) {{
        liveRefreshQueued = false;
        if (!indexedContentLoaded) refreshLiveContent(liveRefreshTargetHeight);
      }} else if (needsRetry) {{
        scheduleLiveRefreshRetry();
      }}
    }}
  }};

  const summaryItem = (label) => Array.from(
    document.querySelectorAll('[data-tx-live-content] .summary-item')
  ).find((item) => {{
    const labelNode = item.querySelector('.summary-label');
    return labelNode && labelNode.textContent.trim() === label;
  }});

  const replaceSummaryValue = (label, value) => {{
    const item = summaryItem(label);
    if (!item) return;
    const labelNode = item.querySelector('.summary-label');
    Array.from(item.children).forEach((child) => {{
      if (child !== labelNode) child.remove();
    }});
    item.append(value);
  }};

  const updateConfirmationCount = (tipHeight, explicitCount = null) => {{
    if (!Number.isFinite(confirmedHeight)) return;
    const count = Number.isFinite(explicitCount)
      ? Math.max(1, explicitCount)
      : Math.max(1, Number(tipHeight) - confirmedHeight + 1);
    const pill = document.querySelector('[data-tx-live-content] .tx-conf-pill');
    if (!pill) return;
    pill.classList.remove('pending', 'neutral');
    pill.textContent = `${{new Intl.NumberFormat().format(count)}} ${{count === 1 ? 'confirmation' : 'confirmations'}}`;
  }};

  const markConfirmed = (heightValue, timestampValue, confirmationsValue = null) => {{
    const height = Number(heightValue);
    if (!Number.isFinite(height)) return;
    if (liveRefreshTargetHeight !== height) liveRefreshAttempt = 0;
    liveRefreshTargetHeight = height;
    confirmedHeight = height;
    if (events && typeof events.selectConfirmedBlock === 'function') {{
      events.selectConfirmedBlock(height);
    }}
    if (isWaiting()) {{
      refreshLiveContent(height);
      return;
    }}

    const live = document.querySelector('[data-tx-live-content]');
    const needsIndexedRefresh = !indexedContentLoaded;
    if (live) live.dataset.txState = 'confirmed';

    const timestamp = Number(timestampValue);
    if (Number.isFinite(timestamp) && timestamp > 0) {{
      const timestampGroup = document.createElement('div');
      timestampGroup.className = 'summary-inline';
      timestampGroup.dataset.tsGroup = '';
      const timestampNode = document.createElement('span');
      timestampNode.className = 'summary-value';
      timestampNode.dataset.headerTs = String(timestamp);
      timestampNode.textContent = String(timestamp);
      const relativeNode = document.createElement('span');
      relativeNode.className = 'summary-sub';
      relativeNode.setAttribute('data-header-ts-rel', '');
      timestampGroup.append(timestampNode, relativeNode);
      replaceSummaryValue('Timestamp', timestampGroup);
    }}

    const blockLink = document.createElement('a');
    blockLink.className = 'summary-value link';
    blockLink.href = `${{blockPrefix}}${{height}}`;
    blockLink.textContent = new Intl.NumberFormat().format(height);
    replaceSummaryValue('Block', blockLink);

    document.querySelectorAll('[data-tx-live-content] .tx-pill-status').forEach((pill) => {{
      const row = pill.closest('.tx-pill-row');
      pill.remove();
      if (row && !row.children.length) row.remove();
    }});
    updateConfirmationCount(latestTip ?? height, Number(confirmationsValue));
    initHeaderInteractions();
    if (needsIndexedRefresh) refreshLiveContent(height);
  }};

  const handleEvent = (payload) => {{
    if (!payload || typeof payload !== 'object') return;
    const data = payload.data || {{}};
    if (payload.type === 'hello') {{
      const tip = Number(data.espo_tip);
      if (Number.isFinite(tip)) latestTip = tip;
      return;
    }}
    if (payload.type === 'block') {{
      const height = Number(data.height);
      if (Number.isFinite(height)) latestTip = height;
      if (arrayIncludesTxid(data.txids)) {{
        markConfirmed(height, data.timestamp, 1);
      }} else if (confirmedHeight !== null) {{
        updateConfirmationCount(height);
      }}
      return;
    }}
    if (payload.type === 'tx-status' && data.txid === txid) {{
      if (data.status === 'confirmed') {{
        markConfirmed(data.height, data.timestamp, Number(data.confirmations));
      }} else if (data.status === 'mempool') {{
        if (events && typeof events.selectMempoolBlock === 'function') {{
          events.selectMempoolBlock(data.mempool_block);
        }}
        if (isWaiting()) refreshLiveContent();
      }}
      return;
    }}
    if (payload.type !== 'tx') return;
    const matches = data.txid === txid || arrayIncludesTxid(data.txids);
    if (!matches) return;
    if (data.status === 'confirmed' || data.event === 'confirmed') {{
      markConfirmed(data.height, data.timestamp, 1);
    }} else {{
      if (events && typeof events.selectMempoolBlock === 'function') {{
        events.selectMempoolBlock(data.mempool_block);
      }}
      if (isWaiting()) refreshLiveContent();
    }}
  }};

  document.querySelectorAll('[data-copy-btn]').forEach((btn) => {{
    btn.dataset.copyBound = '1';
  }});
  if (!events || typeof events.subscribe !== 'function') return;
  const unsubscribe = events.subscribe(handleEvent);
  if (typeof events.trackTransaction === 'function') events.trackTransaction(txid);
  window.addEventListener('pagehide', () => {{
    listenerDisposed = true;
    if (liveRefreshRetryTimer) window.clearTimeout(liveRefreshRetryTimer);
    unsubscribe();
  }}, {{ once: true }});
}})();
</script>
"#,
        txid_js = txid_js,
        block_prefix_js = block_prefix_js,
    ))
}

fn render_waiting_tx_page(state: &ExplorerState, txid: &Txid, canonical_path: &str) -> Response {
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let txid_hex = txid.to_string();
    let header_markup = header(HeaderProps {
        title: "Transaction".to_string(),
        id: Some(txid_hex.clone()),
        show_copy: true,
        pill: Some(("Watching".to_string(), HeaderPillTone::Neutral)),
        summary_items: vec![
            HeaderSummaryItem {
                label: "Status".to_string(),
                value: html! { span class="summary-value muted" { "Not found yet" } },
            },
            HeaderSummaryItem {
                label: "Network".to_string(),
                value: html! { span class="summary-value muted" { (format!("{:?}", state.network)) } },
            },
        ],
        cta: None,
        hero_class: None,
    });

    layout_with_meta(
        &format!("Tx {txid}"),
        canonical_path,
        None,
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(None, espo_tip))
            }
            div data-tx-live-content="" data-tx-state="waiting" {
                (header_markup)
                div class="card tx-wait-card" data-tx-waiting="1" {
                    div class="tx-wait-copy" {
                        h2 class="h2" { "Transaction not found" }
                        p class="muted" { "Waiting for this transaction to reach the mempool." }
                    }
                    div class="tx-wait-spinner" aria-hidden="true" {}
                }
                (header_scripts())
            }
            (tx_event_listener_script(txid))
        },
    )
    .into_response()
}

fn match_trace_outpoint(outpoint: &[u8], txid: &Txid) -> Option<(Vec<u8>, u32)> {
    if outpoint.len() < 36 {
        return None;
    }
    let (tx_bytes, vout_le) = outpoint.split_at(32);
    let vout = u32::from_le_bytes(vout_le[..4].try_into().ok()?);

    if let Ok(trace_txid) = Txid::from_slice(tx_bytes) {
        if trace_txid == *txid {
            return Some((tx_bytes.to_vec(), vout));
        }
    }

    let mut txid_be = tx_bytes.to_vec();
    txid_be.reverse();
    if let Ok(trace_txid) = Txid::from_slice(&txid_be) {
        if trace_txid == *txid {
            return Some((txid_be, vout));
        }
    }

    None
}

fn fee_and_rate(
    tx: &Transaction,
    prev_map: &HashMap<Txid, Transaction>,
) -> (Option<u64>, Option<f64>) {
    let mut input_total = Some(0u64);
    for vin in &tx.input {
        if vin.previous_output.is_null() {
            input_total = None;
            break;
        }
        let Some(prev_tx) = prev_map.get(&vin.previous_output.txid) else {
            input_total = None;
            break;
        };
        let Some(prev_out) = prev_tx.output.get(vin.previous_output.vout as usize) else {
            input_total = None;
            break;
        };
        input_total = input_total.and_then(|acc| acc.checked_add(prev_out.value.to_sat()));
    }

    let Some(inputs) = input_total else {
        return (None, None);
    };
    let outputs: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
    let Some(fee) = inputs.checked_sub(outputs) else {
        return (None, None);
    };
    let vbytes = tx.vsize() as u64;
    let fee_rate = if vbytes > 0 { Some(fee as f64 / vbytes as f64) } else { None };
    (Some(fee), fee_rate)
}

pub async fn tx_page(State(state): State<ExplorerState>, Path(txid_str): Path<String>) -> Response {
    let canonical_path = format!("/tx/{txid_str}");
    let txid = match Txid::from_str(&txid_str) {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                layout_with_meta(
                    "Transaction",
                    &canonical_path,
                    None,
                    html! { p class="error" { "Invalid txid." } },
                ),
            )
                .into_response();
        }
    };

    let electrum_like = get_electrum_like();
    let mempool_entry = pending_by_txid(&txid);

    let tx: Transaction = if let Some(entry) = mempool_entry.as_ref() {
        entry.tx.clone()
    } else {
        match electrum_like.transaction_get_raw(&txid) {
            Ok(bytes) => match deserialize(&bytes) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("[tx_page] decode tx via electrum failed for {txid}: {e:?}");
                    return (
                        StatusCode::NOT_FOUND,
                        layout_with_meta(
                            "Transaction",
                            &canonical_path,
                            None,
                            html! { p class="error" { (format!("Failed to decode tx: {e:?}")) } },
                        ),
                    )
                        .into_response();
                }
            },
            Err(e) => {
                let message = format!("{e:?}");
                if !message.contains("404 Not Found") {
                    eprintln!("[tx_page] electrum raw fetch failed for {txid}: {e:?}");
                }
                return render_waiting_tx_page(&state, &txid, &canonical_path);
            }
        }
    };

    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let rpc = get_bitcoind_rpc_client();
    let chain_tip = cached_bitcoin_chain_tip_height();
    let tx_info = rpc.get_raw_transaction_info(&txid, None).ok();
    let tx_block_info = tx_info
        .as_ref()
        .and_then(|info| info.blockhash.as_ref())
        .and_then(|bh| rpc.get_block_header_info(bh).ok());
    let tx_height_rpc: Option<u64> = tx_block_info.as_ref().map(|hdr| hdr.height as u64);
    let tx_height: Option<u64> = tx_height_rpc.or_else(|| {
        electrum_like
            .transaction_get_height(&txid)
            .map_err(|e| eprintln!("[tx_page] electrum height fetch failed for {txid}: {e}"))
            .ok()
            .flatten()
    });
    let confirmations = tx_block_info
        .as_ref()
        .and_then(|hdr| (hdr.confirmations >= 0).then_some(hdr.confirmations as u64))
        .or_else(|| tx_info.as_ref().and_then(|info| info.confirmations.map(|c| c as u64)))
        .or_else(|| match (chain_tip, tx_height) {
            (Some(tip), Some(h)) if tip >= h => Some(tip - h + 1),
            _ => None,
        });
    let tx_timestamp: Option<u64> = tx_block_info
        .as_ref()
        .map(|hdr| hdr.time as u64)
        .or_else(|| tx_info.as_ref().and_then(|info| info.blocktime.map(|t| t as u64)))
        .or_else(|| tx_info.as_ref().and_then(|info| info.time.map(|t| t as u64)));
    let txid_hex = txid.to_string();

    // Prevouts (best-effort): batch fetch unique txids.
    let mut prev_txids: Vec<Txid> = tx
        .input
        .iter()
        .filter_map(|vin| (!vin.previous_output.is_null()).then_some(vin.previous_output.txid))
        .collect();
    prev_txids.sort();
    prev_txids.dedup();

    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    if !prev_txids.is_empty() {
        let raws = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
        for (i, raw_prev) in raws.into_iter().enumerate() {
            if raw_prev.is_empty() {
                if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                    prev_map.insert(prev_txids[i], mempool_prev.tx);
                }
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                prev_map.insert(prev_txids[i], prev_tx);
            } else if let Some(mempool_prev) = pending_by_txid(&prev_txids[i]) {
                prev_map.insert(prev_txids[i], mempool_prev.tx);
            }
        }
    }

    let (fee_sat, fee_rate) = fee_and_rate(&tx, &prev_map);
    let mempool_url = mempool_tx_url(state.network, &txid);

    let mempool_entry = pending_by_txid(&txid);
    let selected_mempool_index = if tx_height.is_none() {
        mempool_entry
            .as_ref()
            .and_then(|entry| entry.position.as_ref().map(|pos| pos.block))
    } else {
        None
    };
    let mut mempool_projected_balances_by_tx: HashMap<Txid, HashMap<u32, Vec<BalanceEntry>>> =
        HashMap::new();
    let mut mempool_projected_rune_io =
        mempool_entry.as_ref().and_then(|entry| entry.rune_io.clone());
    let mut render_fee_rate = fee_rate;
    let mut defer_alkane_trace_status = tx_height
        .is_none()
        .then(|| {
            mempool_entry
                .as_ref()
                .map(|entry| entry.defer_alkane_trace_status)
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let mempool_block_spenders = if let Some(template_index) = selected_mempool_index {
        let targets: HashSet<Txid> = [txid].into_iter().collect();
        if let Some(projection_txs) =
            get_mempool_block_transactions_for_targets(template_index, &targets)
        {
            let mut projection_outpoints: Vec<(Txid, u32)> = Vec::new();
            for item in &projection_txs {
                for vin in &item.tx.input {
                    if !vin.previous_output.is_null() {
                        projection_outpoints
                            .push((vin.previous_output.txid, vin.previous_output.vout));
                    }
                }
            }
            projection_outpoints.sort();
            projection_outpoints.dedup();
            let projection_outpoint_map = get_outpoint_balances_with_spent_batch(
                StateAt::Latest,
                &state.essentials_provider(),
                &projection_outpoints,
            )
            .unwrap_or_default();
            mempool_projected_balances_by_tx =
                mempool_block_projected_balances(&projection_txs, &projection_outpoint_map);
            if let Some(item) = projection_txs.iter().find(|item| item.txid == txid) {
                mempool_projected_rune_io = item.rune_io.clone();
                render_fee_rate = Some(item.fee_rate);
                defer_alkane_trace_status = item.defer_alkane_trace_status;
            }
        }
        get_mempool_block_spenders(template_index).unwrap_or_default()
    } else {
        HashMap::new()
    };
    let projected_balances = mempool_projected_balances_by_tx.get(&txid);
    let outpoint_fn = |lookup_txid: &Txid, vout: u32| -> OutpointLookup {
        let mut lookup = get_outpoint_balances_with_spent(
            StateAt::Latest,
            &state.essentials_provider(),
            lookup_txid,
            vout,
        )
        .unwrap_or_default();
        if lookup.balances.is_empty() {
            if let Some(projected) = mempool_projected_balances_by_tx
                .get(lookup_txid)
                .and_then(|tx_outputs| tx_outputs.get(&vout))
            {
                lookup.balances = projected.clone();
            }
        }
        lookup
    };
    let outspends_fn = |lookup_txid: &Txid| -> Vec<Option<Txid>> {
        let mut outspends =
            electrum_like.transaction_get_outspends(lookup_txid).unwrap_or_default();
        let mempool_outspends = if selected_mempool_index.is_some() {
            let output_count = prev_map
                .get(lookup_txid)
                .map(|prev_tx| prev_tx.output.len())
                .or_else(|| (lookup_txid == &txid).then_some(tx.output.len()))
                .unwrap_or(outspends.len());
            let mut block_outspends = vec![None; output_count];
            for ((spent_txid, spent_vout), spender) in &mempool_block_spenders {
                if spent_txid == lookup_txid {
                    let idx = *spent_vout as usize;
                    if idx >= block_outspends.len() {
                        block_outspends.resize(idx + 1, None);
                    }
                    block_outspends[idx] = Some(*spender);
                }
            }
            block_outspends
        } else {
            get_mempool_outspends(lookup_txid, outspends.len())
        };
        if outspends.len() < mempool_outspends.len() {
            outspends.resize(mempool_outspends.len(), None);
        }
        for (idx, spender) in mempool_outspends.into_iter().enumerate() {
            if spender.is_some() {
                outspends[idx] = spender;
            }
        }
        outspends
    };
    let traces_for_tx: Option<Vec<EspoTrace>> = if let Some(h) = tx_height {
        match fetch_traces_for_tx(h, &txid, &tx) {
            Ok(v) if !v.is_empty() => Some(v),
            Ok(_) => mempool_entry.as_ref().and_then(|m| m.traces.clone()),
            Err(e) => {
                if !is_metashrew_missing_stored_block_hash(&e) {
                    eprintln!("[tx_page] failed to fetch traces for {txid}: {e}");
                }
                mempool_entry.as_ref().and_then(|m| m.traces.clone())
            }
        }
        .or_else(|| match fetch_traces_for_tx_noheight(&txid, &tx) {
            Ok(v) if !v.is_empty() => Some(v),
            Ok(_) => None,
            Err(e) => {
                eprintln!("[tx_page] failed to fetch traces (noheight) for {txid}: {e}");
                None
            }
        })
    } else {
        mempool_entry.as_ref().and_then(|m| m.traces.clone()).or_else(|| {
            match fetch_traces_for_tx_noheight(&txid, &tx) {
                Ok(v) if !v.is_empty() => Some(v),
                Ok(_) => None,
                Err(e) => {
                    eprintln!("[tx_page] failed to fetch traces (noheight) for {txid}: {e}");
                    None
                }
            }
        })
    };
    let traces_ref: Option<&[EspoTrace]> = traces_for_tx.as_ref().map(|v| v.as_slice());
    let tx_pill = if tx_height.is_none() {
        Some(TxPill { label: "Unconfirmed".to_string(), tone: TxPillTone::Danger })
    } else {
        None
    };
    let projected_rune_io =
        if tx_height.is_none() { mempool_projected_rune_io.as_ref() } else { None };

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Timestamp".to_string(),
        value: match tx_timestamp {
            Some(ts) => html! {
                div class="summary-inline" data-ts-group="" {
                    span class="summary-value" data-header-ts=(ts) { (ts) }
                    span class="summary-sub" data-header-ts-rel { "" }
                }
            },
            None => html! { span class="summary-value muted" { "Pending" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Block".to_string(),
        value: match tx_height {
            Some(h) => html! { a class="summary-value link" href=(explorer_path(&format!("/block/{h}"))) { (format_with_commas(h)) } },
            None => html! { span class="summary-value muted" { "Unconfirmed" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Fee".to_string(),
        value: match fee_sat {
            Some(fee) => html! { span class="summary-value" { (format_sats_short(fee)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Fee rate".to_string(),
        value: match render_fee_rate {
            Some(rate) => html! { span class="summary-value" { (format_fee_rate(rate)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });

    let pill = confirmations
        .map(|c| (format!("{} confirmations", format_with_commas(c)), HeaderPillTone::Success))
        .or_else(|| Some(("Unconfirmed".to_string(), HeaderPillTone::Warning)));
    let cta: Option<HeaderCta> = None;
    let header_markup = header(HeaderProps {
        title: "Transaction".to_string(),
        id: Some(txid_hex.clone()),
        show_copy: true,
        pill,
        summary_items,
        cta,
        hero_class: None,
    });
    layout_with_meta(
        &format!("Tx {txid}"),
        &format!("/tx/{txid}"),
        None,
        html! {
            div class="block-hero full-bleed" {
                @if let Some(index) = selected_mempool_index {
                    (block_carousel_with_mempool(Some(index), espo_tip))
                } @else {
                    (block_carousel(tx_height, espo_tip))
                }
            }
            div data-tx-live-content="" data-tx-state=(if tx_height.is_some() { "confirmed" } else { "mempool" }) {
                (header_markup)
                @if let Some(url) = mempool_url {
                    div class="tx-mempool-row" {
                        a class="tx-mempool-link" href=(url) target="_blank" rel="noopener noreferrer" {
                            "view on mempool.space"
                            (icon_arrow_up_right())
                        }
                    }
                }
                h2 class="h2" { "Inputs & Outputs" }
                (render_tx(&txid, &tx, traces_ref, state.network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, tx_pill, render_fee_rate, projected_balances, projected_rune_io, false, defer_alkane_trace_status))
                (header_scripts())
            }
            (tx_event_listener_script(&txid))
        },
    )
    .into_response()
}

/// Remote-mode trace fetch: pull the block's stored traces from the remote
/// espo via `essentials.get_block_traces` and keep the ones for this txid.
/// The sandshrew-like events are what the tx view renders; protobuf/storage
/// enrichment is metashrew-local and intentionally omitted here.
fn fetch_traces_for_tx_remote(
    client: &crate::runtime::remote_mdb::RemoteMdbClient,
    height: u64,
    txid: &Txid,
) -> anyhow::Result<Vec<EspoTrace>> {
    let result = client
        .call("essentials.get_block_traces", serde_json::json!({ "height": height }))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let traces = result.get("traces").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let tx_hex = txid.to_string();
    let mut out: Vec<EspoTrace> = Vec::new();
    for trace in traces {
        let Some(outpoint) = trace.get("outpoint").and_then(|v| v.as_str()) else { continue };
        let Some((trace_txid, vout_str)) = outpoint.split_once(':') else { continue };
        if !trace_txid.eq_ignore_ascii_case(&tx_hex) {
            continue;
        }
        let Ok(vout) = vout_str.parse::<u32>() else { continue };
        let Some(events_value) = trace.get("events") else { continue };
        let Ok(events) = serde_json::from_value::<
            Vec<crate::alkanes::trace::EspoSandshrewLikeTraceEvent>,
        >(events_value.clone()) else {
            continue;
        };
        out.push(EspoTrace {
            sandshrew_trace: EspoSandshrewLikeTrace {
                outpoint: format!("{tx_hex}:{vout}"),
                events,
            },
            protobuf_trace: Default::default(),
            storage_changes: Default::default(),
            outpoint: crate::schemas::EspoOutpoint {
                txid: txid.to_byte_array().to_vec(),
                vout,
                tx_spent: None,
            },
        });
    }
    Ok(out)
}

fn fetch_traces_for_tx(
    height: u64,
    txid: &Txid,
    tx: &Transaction,
) -> anyhow::Result<Vec<EspoTrace>> {
    if let Some(client) = crate::config::get_explorer_remote_mdb_client() {
        return fetch_traces_for_tx_remote(&client, height, txid);
    }
    let partials = traces_for_block_as_prost(height)?;
    let mut out: Vec<EspoTrace> = Vec::new();
    let tx_hex = txid.to_string();

    for partial in partials {
        let Some((txid_be, vout)) = match_trace_outpoint(&partial.outpoint, txid) else {
            continue;
        };
        let events = protobuf_trace_events(&partial.protobuf_trace)?;

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = extract_alkane_storage(
            &partial.protobuf_trace,
            tx,
            &crate::alkanes::trace::EspoHostFunctionValues::default(),
        )?;

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace,
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    Ok(out)
}

fn is_metashrew_missing_stored_block_hash(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.to_string().contains("metashrew missing stored block hash at height "))
}

fn fetch_traces_for_tx_noheight(txid: &Txid, tx: &Transaction) -> anyhow::Result<Vec<EspoTrace>> {
    // Remote mode has no local metashrew to scan for unindexed/mempool txids;
    // confirmed txs are covered by fetch_traces_for_tx_remote via the remote's
    // stored block traces.
    if crate::config::get_explorer_remote_mdb_client().is_some() {
        return Ok(Vec::new());
    }
    let partials = get_metashrew().traces_for_tx(txid)?;
    let mut out: Vec<EspoTrace> = Vec::new();
    let tx_hex = txid.to_string();

    for partial in partials {
        let Some((txid_be, vout)) = match_trace_outpoint(&partial.outpoint, txid) else {
            continue;
        };
        let events = protobuf_trace_events(&partial.protobuf_trace)?;

        let sandshrew_trace =
            EspoSandshrewLikeTrace { outpoint: format!("{tx_hex}:{vout}"), events };
        let storage_changes = extract_alkane_storage(
            &partial.protobuf_trace,
            tx,
            &crate::alkanes::trace::EspoHostFunctionValues::default(),
        )?;

        out.push(EspoTrace {
            sandshrew_trace,
            protobuf_trace: partial.protobuf_trace,
            storage_changes,
            outpoint: crate::schemas::EspoOutpoint { txid: txid_be, vout, tx_spent: None },
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bitcoin::Txid;

    use super::tx_event_listener_script_with_block_prefix;

    #[test]
    fn pending_transaction_listener_uses_shared_events_without_polling_or_page_replacement() {
        let txid =
            Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap();
        let script = tx_event_listener_script_with_block_prefix(&txid, "/block/").into_string();

        assert!(script.contains("window.__espoBlockCarouselEvents"));
        assert!(script.contains("events.subscribe(handleEvent)"));
        assert!(script.contains("events.trackTransaction(txid)"));
        assert!(script.contains("events.selectConfirmedBlock(height)"));
        assert!(script.contains("const needsIndexedRefresh = !indexedContentLoaded"));
        assert!(script.contains("if (needsIndexedRefresh) refreshLiveContent(height)"));
        assert!(script.contains("expectedConfirmedHeight !== null"));
        assert!(script.contains("next.dataset.txState !== 'confirmed'"));
        assert!(script.contains("scheduleLiveRefreshRetry()"));
        assert!(script.contains("current.replaceWith(next)"));
        assert!(script.contains("liveRefreshQueued"));
        assert!(!script.contains("new WebSocket"));
        assert!(!script.contains("setInterval"));
        assert!(!script.contains("main.app"));
    }
}
