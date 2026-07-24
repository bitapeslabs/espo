use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use alkanes_support::proto::alkanes::AlkanesTrace;
use alloy_primitives::U256;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use maud::html;
use serde::Deserialize;

use crate::alkanes::trace::{
    EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId, EspoTrace,
    GetEspoBlockOpts, get_espo_block_with_opts,
};
use crate::config::{
    get_bitcoind_rpc_client, get_electrum_like, get_espo_next_height, get_network,
};
use crate::explorer::api::cached_bitcoin_chain_tip_height;
use crate::explorer::components::block_carousel::{block_carousel, block_carousel_with_mempool};
use crate::explorer::components::dropdown::{DropdownItem, DropdownProps, dropdown};
use crate::explorer::components::header::{
    HeaderPillTone, HeaderProps, HeaderSummaryItem, header, header_scripts,
};
use crate::explorer::components::layout::layout_with_meta;
use crate::explorer::components::svg_assets::{
    icon_arrow_up_right, icon_pager_first, icon_pager_last, icon_pager_left, icon_pager_right,
};
use crate::explorer::components::tx_view::{TxPill, TxPillTone, render_tx};
use crate::explorer::consts::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use crate::explorer::pages::common::format_fee_rate;
use crate::explorer::pages::state::ExplorerState;
use crate::explorer::paths::explorer_path;
use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::essentials::storage::{
    AddressIndexListKind, AlkaneTxSummary, BalanceEntry, address_index_list_id_alkane_block_txs,
    get_address_index_list_len, get_address_index_list_range, load_tx_pointer_blob_v3_by_id,
    load_tx_summary_v2,
};
use crate::modules::essentials::utils::balances::{
    OutpointLookup, get_outpoint_balances_with_spent_batch,
    project_tx_output_balances_from_traces_with_projector,
};
use crate::modules::runes::main::runes_enabled_from_global_config;
use crate::modules::runes::storage::{RunesProvider, SchemaRuneId};
use crate::modules::tokendata::storage::TokenDataProvider;
use crate::runtime::mempool::{
    MempoolBlockTx, MempoolTxFilter, get_mempool_block_detail, get_mempool_block_spenders,
    get_mempool_block_transactions_for_targets, get_mempool_transactions,
};
use crate::runtime::mempool_projection::MempoolProjectionRegistry;
use crate::runtime::state_at::StateAt;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};

fn format_with_commas(n: u64) -> String {
    let mut s = n.to_string();
    let mut i = s.len() as isize - 3;
    while i > 0 {
        s.insert(i as usize, ',');
        i -= 3;
    }
    s
}

fn comma_decimal_digits(digits: &str) -> String {
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn format_scaled_usd_price(bytes: [u8; 32]) -> String {
    let scale = U256::from(PRICE_SCALE);
    let cents = (U256::from_be_bytes(bytes).saturating_mul(U256::from(100u64))
        + (scale / U256::from(2u64)))
        / scale;
    let mut cents_str = cents.to_string();
    if cents_str.len() < 3 {
        cents_str = format!("{}{}", "0".repeat(3 - cents_str.len()), cents_str);
    }
    let split = cents_str.len().saturating_sub(2);
    let whole = comma_decimal_digits(&cents_str[..split]);
    let frac = &cents_str[split..];
    format!("${whole}.{frac}")
}

fn price_summary_value(value: Option<[u8; 32]>) -> maud::Markup {
    match value {
        Some(bytes) => html! { span class="summary-value" { (format_scaled_usd_price(bytes)) } },
        None => html! { span class="summary-value muted strong" { "Not Seen" } },
    }
}

fn mempool_block_url(network: bitcoin::Network, block_hash: &BlockHash) -> Option<String> {
    let base = match network {
        bitcoin::Network::Bitcoin => "https://mempool.space",
        bitcoin::Network::Testnet => "https://mempool.space/testnet",
        bitcoin::Network::Signet => "https://mempool.space/signet",
        bitcoin::Network::Regtest => return None,
        _ => "https://mempool.space",
    };
    Some(format!("{base}/block/{block_hash}"))
}

#[derive(Deserialize)]
pub struct BlockPageQuery {
    pub tab: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub traces: Option<String>,
    pub txs: Option<String>,
    pub hide_diesel_mints: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxFilter {
    All,
    Action,
    Alkane,
    Rune,
}

impl TxFilter {
    fn from_query(txs: Option<&str>, traces: Option<&str>) -> Self {
        match txs {
            Some("all") => Self::All,
            Some("action") | Some("actions") | Some("all_actions") => Self::Action,
            Some("rune") | Some("runes") => Self::Rune,
            Some("alkane") | Some("alkanes") => Self::Alkane,
            _ => {
                let traces_only =
                    traces.map(|v| matches!(v, "1" | "true" | "on" | "yes")).unwrap_or(true);
                if traces_only { Self::Alkane } else { Self::All }
            }
        }
    }

    fn query_value(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Action => "actions",
            Self::Alkane => "alkane",
            Self::Rune => "rune",
        }
    }

    fn mempool_filter(self) -> MempoolTxFilter {
        match self {
            Self::All => MempoolTxFilter::All,
            Self::Action => MempoolTxFilter::Action,
            Self::Alkane => MempoolTxFilter::Alkane,
            Self::Rune => MempoolTxFilter::Rune,
        }
    }
}

struct BlockTxItem {
    txid: Txid,
    tx: Transaction,
    traces: Option<Vec<EspoTrace>>,
}

fn traces_from_summary(txid: &Txid, summary: &AlkaneTxSummary) -> Vec<EspoTrace> {
    summary
        .traces
        .iter()
        .filter_map(|trace| sandshrew_to_espo_trace(txid, trace))
        .collect()
}

fn is_uncommon_goods_mint_tx(txid: &Txid, runes_provider: &RunesProvider) -> bool {
    let uncommon_goods = SchemaRuneId { block: 1, tx: 0 };
    runes_provider
        .get_tx_io(txid)
        .ok()
        .flatten()
        .map(|io| io.minted.iter().any(|minted| minted.id == uncommon_goods))
        .unwrap_or(false)
}

fn sandshrew_to_espo_trace(txid: &Txid, trace: &EspoSandshrewLikeTrace) -> Option<EspoTrace> {
    let (txid_hex, vout_s) = trace.outpoint.split_once(':')?;
    let vout = vout_s.parse::<u32>().ok()?;
    let trace_txid = Txid::from_str(txid_hex).unwrap_or(*txid);
    Some(EspoTrace {
        sandshrew_trace: trace.clone(),
        protobuf_trace: AlkanesTrace::default(),
        storage_changes: HashMap::new(),
        outpoint: EspoOutpoint { txid: trace_txid.to_byte_array().to_vec(), vout, tx_spent: None },
    })
}

fn parse_u128_from_str(s: &str) -> Option<u128> {
    if let Some(hex) = s.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u128>().ok()
    }
}

fn parse_u32_or_hex(s: &str) -> Option<u32> {
    if let Some(hex) = s.strip_prefix("0x") {
        u32::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u32>().ok()
    }
}

fn parse_u64_or_hex(s: &str) -> Option<u64> {
    if let Some(hex) = s.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u64>().ok()
    }
}

fn parse_short_id_to_parts(id: &EspoSandshrewLikeTraceShortId) -> Option<(u32, u64)> {
    Some((parse_u32_or_hex(&id.block)?, parse_u64_or_hex(&id.tx)?))
}

fn is_diesel_mint_trace(trace: &EspoTrace) -> bool {
    let mut invoke_count = 0usize;
    let mut contract_id: Option<(u32, u64)> = None;
    let mut inputs: Option<&[String]> = None;

    for ev in &trace.sandshrew_trace.events {
        if let EspoSandshrewLikeTraceEvent::Invoke(data) = ev {
            invoke_count += 1;
            if invoke_count > 1 {
                return false;
            }
            contract_id = parse_short_id_to_parts(&data.context.myself);
            inputs = Some(&data.context.inputs);
        }
    }

    if invoke_count != 1 {
        return false;
    }
    let Some((block, tx)) = contract_id else {
        return false;
    };
    if block != 2 || tx != 0 {
        return false;
    }
    let Some(inputs) = inputs else {
        return false;
    };
    if inputs.is_empty() {
        return false;
    }
    let mut iter = inputs.iter();
    let opcode = iter.next().and_then(|s| parse_u128_from_str(s)).unwrap_or_default();
    if opcode != 77 {
        return false;
    }
    iter.all(|s| parse_u128_from_str(s).map_or(false, |v| v == 0))
}

fn is_diesel_mint_tx(traces: Option<&[EspoTrace]>) -> bool {
    let Some(traces) = traces else {
        return false;
    };
    if traces.len() != 1 {
        return false;
    }
    is_diesel_mint_trace(&traces[0])
}

fn aggregate_balances(
    entries: &[BalanceEntry],
    out: &mut BTreeMap<crate::schemas::SchemaAlkaneId, u128>,
) {
    for entry in entries {
        if entry.amount == 0 {
            continue;
        }
        *out.entry(entry.alkane).or_default() =
            out.get(&entry.alkane).copied().unwrap_or(0).saturating_add(entry.amount);
    }
}

pub(crate) fn mempool_block_projected_balances(
    ordered_txs: &[MempoolBlockTx],
    db_outpoints: &HashMap<(Txid, u32), OutpointLookup>,
) -> HashMap<Txid, HashMap<u32, Vec<BalanceEntry>>> {
    let mut projected_by_outpoint: HashMap<(Txid, u32), Vec<BalanceEntry>> = HashMap::new();
    let mut projected_by_tx: HashMap<Txid, HashMap<u32, Vec<BalanceEntry>>> = HashMap::new();
    let mut contract_projector = MempoolProjectionRegistry::from_latest_indices();

    for item in ordered_txs {
        contract_projector.begin_transaction();
        let mut input_totals: BTreeMap<crate::schemas::SchemaAlkaneId, u128> = BTreeMap::new();
        for vin in &item.tx.input {
            if vin.previous_output.is_null() {
                continue;
            }
            let key = (vin.previous_output.txid, vin.previous_output.vout);
            if let Some(entries) = projected_by_outpoint.get(&key) {
                aggregate_balances(entries, &mut input_totals);
            } else if let Some(lookup) = db_outpoints.get(&key) {
                aggregate_balances(&lookup.balances, &mut input_totals);
            }
        }

        let input_balances: Vec<BalanceEntry> = input_totals
            .into_iter()
            .map(|(alkane, amount)| BalanceEntry { alkane, amount })
            .collect();
        let traces = item.traces.as_deref().unwrap_or(&[]);
        let projected = project_tx_output_balances_from_traces_with_projector(
            &item.tx,
            traces,
            input_balances,
            Some(&mut contract_projector),
        );
        if projected.is_empty() && !contract_projector.did_apply() {
            continue;
        }
        let projected = sanitize_diesel_ug_projection(item, projected);

        for (vout, entries) in &projected {
            projected_by_outpoint.insert((item.txid, *vout), entries.clone());
        }
        projected_by_tx.insert(item.txid, projected);
    }

    projected_by_tx
}

fn sanitize_diesel_ug_projection(
    item: &MempoolBlockTx,
    mut projected: HashMap<u32, Vec<BalanceEntry>>,
) -> HashMap<u32, Vec<BalanceEntry>> {
    const DIESEL_ID: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 0 };
    let has_ug_mint = item
        .rune_io
        .as_ref()
        .map(|io| io.minted.iter().any(|minted| minted.id.block == 1 && minted.id.tx == 0))
        .unwrap_or(false);
    if !has_ug_mint {
        return projected;
    }

    for entries in projected.values_mut() {
        let diesel_rows = entries.iter().filter(|entry| entry.alkane == DIESEL_ID).count();
        if diesel_rows <= 1 {
            continue;
        }
        let mut removed_one_unit = false;
        entries.retain(|entry| {
            if !removed_one_unit && entry.alkane == DIESEL_ID && entry.amount == 1 {
                removed_one_unit = true;
                false
            } else {
                true
            }
        });
    }
    projected
}

pub async fn block_page(
    State(state): State<ExplorerState>,
    Path(height): Path<u64>,
    Query(q): Query<BlockPageQuery>,
) -> Response {
    let rpc = get_bitcoind_rpc_client();
    let electrum_like = get_electrum_like();
    let network = get_network();
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let tip = cached_bitcoin_chain_tip_height().unwrap_or(espo_tip);
    let nav_tip = espo_tip.min(tip);
    let espo_indexed = height <= espo_tip;
    let essentials_provider = state.essentials_provider();
    let runes_enabled = runes_enabled_from_global_config();
    let requested_filter = if runes_enabled && q.txs.is_none() && q.traces.is_none() {
        TxFilter::Action
    } else {
        TxFilter::from_query(q.txs.as_deref(), q.traces.as_deref())
    };
    let tx_filter =
        if !runes_enabled && matches!(requested_filter, TxFilter::Rune | TxFilter::Action) {
            TxFilter::Alkane
        } else {
            requested_filter
        };
    let hide_diesel_mints = q
        .hide_diesel_mints
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(false);
    let runes_provider = RunesProvider::new(Arc::new(state.runes_mdb.clone()));
    let tokendata_provider =
        TokenDataProvider::new(Arc::new(crate::config::explorer_mdb(b"tokendata:")));
    let canonical_path = format!("/block/{height}");

    let block_hash = match rpc.get_block_hash(height) {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                layout_with_meta(
                    "Block",
                    &canonical_path,
                    None,
                    html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
                ),
            )
                .into_response();
        }
    };
    let block_hash_hex = block_hash.to_string();
    let diesel_avg_price_paid_usd = tokendata_provider
        .get_diesel_avg_price_paid_usd_by_height(height as u32)
        .ok()
        .flatten();
    let uncommon_goods_avg_price_paid_usd = runes_provider
        .get_uncommon_goods_avg_price_paid_usd_by_height(height as u32)
        .ok()
        .flatten();
    let block_summary = essentials_provider
        .get_block_summary(crate::modules::essentials::storage::GetBlockSummaryParams {
            blockhash: StateAt::Latest,
            height: height as u32,
        })
        .ok()
        .and_then(|resp| resp.summary);

    let _tab = q.tab.unwrap_or_else(|| "txs".to_string());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);

    let espo_block = if espo_indexed && tx_filter == TxFilter::All {
        match get_espo_block_with_opts(height, nav_tip, Some(GetEspoBlockOpts { page, limit })) {
            Ok(b) => Some(b),
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    layout_with_meta(
                        "Block",
                        &canonical_path,
                        None,
                        html! { p class="error" { (format!("Failed to fetch block: {e:?}")) } },
                    ),
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    let mempool_url = mempool_block_url(network, &block_hash);

    let mut tx_total = 0usize;
    let mut tx_items: Vec<BlockTxItem> = Vec::new();
    let mut tx_has_prev = false;
    let mut tx_has_next = false;
    let mut display_start = 0usize;
    let mut display_end = 0usize;
    let mut last_page = 1usize;
    let txs_param = tx_filter.query_value();
    let hide_diesel_param = if hide_diesel_mints { "1" } else { "0" };

    if espo_indexed {
        if tx_filter == TxFilter::Action {
            let total = runes_provider.get_action_block_tx_count(height).unwrap_or(0) as usize;
            let mut pointers = if hide_diesel_mints {
                runes_provider
                    .get_action_block_tx_range(height, 0, total as u64)
                    .unwrap_or_default()
            } else {
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(total);
                runes_provider
                    .get_action_block_tx_range(height, off as u64, end as u64)
                    .unwrap_or_default()
            };
            if hide_diesel_mints {
                pointers.retain(|pointer| {
                    let txid = Txid::from_byte_array(pointer.txid);
                    let summary = load_tx_summary_v2(&essentials_provider, &txid);
                    let traces = summary
                        .as_ref()
                        .map(|s| traces_from_summary(&txid, s))
                        .filter(|t| !t.is_empty());
                    !is_diesel_mint_tx(traces.as_deref())
                        && !is_uncommon_goods_mint_tx(&txid, &runes_provider)
                });
            }

            tx_total = if hide_diesel_mints { pointers.len() } else { total };
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1 && off < tx_total;
            tx_has_next = end < tx_total;
            if tx_total > 0 && off < tx_total {
                display_start = off + 1;
                display_end = end;
                last_page = (tx_total + limit - 1) / limit;
            }

            let page_pointers = if hide_diesel_mints {
                pointers.into_iter().skip(off).take(end.saturating_sub(off)).collect::<Vec<_>>()
            } else {
                pointers
            };
            let txids: Vec<Txid> = page_pointers
                .iter()
                .map(|pointer| Txid::from_byte_array(pointer.txid))
                .collect();
            let raw_txs = electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
            for (idx, txid) in txids.iter().enumerate() {
                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                if raw.is_empty() {
                    continue;
                }
                let Ok(tx) = deserialize::<Transaction>(&raw) else {
                    continue;
                };
                let summary = load_tx_summary_v2(&essentials_provider, txid);
                let traces = summary
                    .as_ref()
                    .map(|s| traces_from_summary(txid, s))
                    .filter(|t| !t.is_empty());
                tx_items.push(BlockTxItem { txid: *txid, tx, traces });
            }
            if tx_total > 0 && off < tx_total {
                display_end = (off + tx_items.len()).min(tx_total);
            }
        } else if tx_filter == TxFilter::Alkane {
            let list_id = address_index_list_id_alkane_block_txs(height);
            let total = get_address_index_list_len(
                &essentials_provider,
                StateAt::Latest,
                AddressIndexListKind::AlkaneBlockTxs,
                &list_id,
            )
            .unwrap_or(0) as usize;
            if hide_diesel_mints {
                let mut all_txids: Vec<Txid> = Vec::new();
                if total > 0 {
                    let ids = get_address_index_list_range(
                        &essentials_provider,
                        StateAt::Latest,
                        AddressIndexListKind::AlkaneBlockTxs,
                        &list_id,
                        0,
                        total as u64,
                    )
                    .unwrap_or_default();
                    for id in ids {
                        let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id)
                        else {
                            continue;
                        };
                        all_txids.push(Txid::from_byte_array(blob.txid));
                    }
                }

                let mut summary_map: HashMap<Txid, Option<AlkaneTxSummary>> = HashMap::new();
                let mut filtered_txids: Vec<Txid> = Vec::new();
                for txid in &all_txids {
                    let summary = load_tx_summary_v2(&essentials_provider, txid);
                    let traces = summary
                        .as_ref()
                        .map(|s| traces_from_summary(txid, s))
                        .filter(|t| !t.is_empty());
                    if !is_diesel_mint_tx(traces.as_deref())
                        && (!runes_enabled || !is_uncommon_goods_mint_tx(txid, &runes_provider))
                    {
                        filtered_txids.push(*txid);
                    }
                    summary_map.insert(*txid, summary);
                }

                tx_total = filtered_txids.len();
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(tx_total);
                tx_has_prev = page > 1 && off < tx_total;
                tx_has_next = end < tx_total;
                if tx_total > 0 && off < tx_total {
                    display_start = off + 1;
                    display_end = (off + end.saturating_sub(off)).min(tx_total);
                    last_page = (tx_total + limit - 1) / limit;
                }

                if end > off {
                    let page_txids: Vec<Txid> = filtered_txids[off..end].to_vec();
                    let raw_txs =
                        electrum_like.batch_transaction_get_raw(&page_txids).unwrap_or_default();
                    for (idx, txid) in page_txids.iter().enumerate() {
                        let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                        if raw.is_empty() {
                            continue;
                        }
                        let Ok(tx) = deserialize::<Transaction>(&raw) else {
                            continue;
                        };
                        let summary = summary_map.get(txid).cloned().unwrap_or(None);
                        let traces = summary
                            .as_ref()
                            .map(|s| traces_from_summary(txid, s))
                            .filter(|t| !t.is_empty());
                        tx_items.push(BlockTxItem { txid: *txid, tx, traces });
                    }
                    if tx_total > 0 && off < tx_total {
                        display_end = (off + tx_items.len()).min(tx_total);
                    }
                }
            } else {
                tx_total = total;
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(tx_total);
                tx_has_prev = page > 1;
                tx_has_next = end < tx_total;
                if tx_total > 0 && off < tx_total {
                    display_start = off + 1;
                    display_end = (off + end.saturating_sub(off)).min(tx_total);
                    last_page = (tx_total + limit - 1) / limit;
                }

                if end > off {
                    let mut txids: Vec<Txid> = Vec::new();
                    let ids = get_address_index_list_range(
                        &essentials_provider,
                        StateAt::Latest,
                        AddressIndexListKind::AlkaneBlockTxs,
                        &list_id,
                        off as u64,
                        end as u64,
                    )
                    .unwrap_or_default();
                    for id in ids {
                        let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id)
                        else {
                            continue;
                        };
                        txids.push(Txid::from_byte_array(blob.txid));
                    }

                    let raw_txs =
                        electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
                    for (idx, txid) in txids.iter().enumerate() {
                        let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                        if raw.is_empty() {
                            continue;
                        }
                        let Ok(tx) = deserialize::<Transaction>(&raw) else {
                            continue;
                        };
                        let summary = load_tx_summary_v2(&essentials_provider, txid);
                        let traces = summary
                            .as_ref()
                            .map(|s| traces_from_summary(txid, s))
                            .filter(|t| !t.is_empty());
                        tx_items.push(BlockTxItem { txid: *txid, tx, traces });
                    }
                    if tx_total > 0 && off < tx_total {
                        display_end = (off + tx_items.len()).min(tx_total);
                    }
                }
            }
        } else if tx_filter == TxFilter::Rune {
            let total = runes_provider.get_block_tx_count(height).unwrap_or(0) as usize;
            let mut pointers = if hide_diesel_mints {
                runes_provider.get_block_tx_range(height, 0, total as u64).unwrap_or_default()
            } else {
                let off = limit.saturating_mul(page.saturating_sub(1));
                let end = (off + limit).min(total);
                runes_provider
                    .get_block_tx_range(height, off as u64, end as u64)
                    .unwrap_or_default()
            };
            if hide_diesel_mints {
                let uncommon_goods = SchemaRuneId { block: 1, tx: 0 };
                pointers.retain(|pointer| {
                    !pointer.io.minted.iter().any(|minted| minted.id == uncommon_goods)
                });
            }

            tx_total = if hide_diesel_mints { pointers.len() } else { total };
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1 && off < tx_total;
            tx_has_next = end < tx_total;
            if tx_total > 0 && off < tx_total {
                display_start = off + 1;
                display_end = end;
                last_page = (tx_total + limit - 1) / limit;
            }

            let page_pointers = if hide_diesel_mints {
                pointers.into_iter().skip(off).take(end.saturating_sub(off)).collect::<Vec<_>>()
            } else {
                pointers
            };
            let txids: Vec<Txid> = page_pointers
                .iter()
                .map(|pointer| Txid::from_byte_array(pointer.txid))
                .collect();
            let raw_txs = electrum_like.batch_transaction_get_raw(&txids).unwrap_or_default();
            for (idx, txid) in txids.iter().enumerate() {
                let raw = raw_txs.get(idx).cloned().unwrap_or_default();
                if raw.is_empty() {
                    continue;
                }
                let Ok(tx) = deserialize::<Transaction>(&raw) else {
                    continue;
                };
                let summary = load_tx_summary_v2(&essentials_provider, txid);
                let traces = summary
                    .as_ref()
                    .map(|s| traces_from_summary(txid, s))
                    .filter(|t| !t.is_empty());
                tx_items.push(BlockTxItem { txid: *txid, tx, traces });
            }
            if tx_total > 0 && off < tx_total {
                display_end = (off + tx_items.len()).min(tx_total);
            }
        } else if let Some(espo_block) = espo_block.clone() {
            tx_total = espo_block.tx_count;
            let off = limit.saturating_mul(page.saturating_sub(1));
            let end = (off + limit).min(tx_total);
            tx_has_prev = page > 1;
            tx_has_next = end < tx_total;
            tx_items = espo_block
                .transactions
                .into_iter()
                .map(|atx| BlockTxItem {
                    txid: atx.transaction.compute_txid(),
                    tx: atx.transaction,
                    traces: atx.traces,
                })
                .collect();
            if hide_diesel_mints {
                tx_items.retain(|item| {
                    !is_diesel_mint_tx(item.traces.as_deref())
                        && (!runes_enabled
                            || !is_uncommon_goods_mint_tx(&item.txid, &runes_provider))
                });
            }
            if tx_total > 0 {
                display_start = off + 1;
                display_end = (off + tx_items.len()).min(tx_total);
                last_page = (tx_total + limit - 1) / limit;
            }
        }
    }

    let mut all_outpoints: Vec<(Txid, u32)> = Vec::new();
    for item in &tx_items {
        for (vout, _) in item.tx.output.iter().enumerate() {
            all_outpoints.push((item.txid, vout as u32));
        }
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                all_outpoints.push((vin.previous_output.txid, vin.previous_output.vout));
            }
        }
    }
    all_outpoints.sort();
    all_outpoints.dedup();
    let outpoint_map = get_outpoint_balances_with_spent_batch(
        StateAt::Latest,
        &state.essentials_provider(),
        &all_outpoints,
    )
    .unwrap_or_default();
    let outpoint_fn = move |txid: &Txid, vout: u32| -> OutpointLookup {
        outpoint_map.get(&(*txid, vout)).cloned().unwrap_or_default()
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mut dedup: Vec<Txid> = tx_items.iter().map(|t| t.txid).collect();
        dedup.sort();
        dedup.dedup();
        let fetched = electrum_like.batch_transaction_get_outspends(&dedup).unwrap_or_default();
        dedup.into_iter().zip(fetched.into_iter()).collect()
    };
    let outspends_fn = move |txid: &Txid| -> Vec<Option<Txid>> {
        outspends_map.get(txid).cloned().unwrap_or_default()
    };

    let mut prev_map: HashMap<Txid, Transaction> = HashMap::new();
    if !tx_items.is_empty() {
        let mut prev_txids: Vec<Txid> = Vec::new();
        for item in &tx_items {
            for vin in &item.tx.input {
                if !vin.previous_output.is_null() {
                    prev_txids.push(vin.previous_output.txid);
                }
            }
        }
        prev_txids.sort();
        prev_txids.dedup();

        if !prev_txids.is_empty() {
            let raws = electrum_like.batch_transaction_get_raw(&prev_txids).unwrap_or_default();
            for (i, raw_prev) in raws.into_iter().enumerate() {
                if raw_prev.is_empty() {
                    continue;
                }
                if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                    prev_map.insert(prev_txids[i], prev_tx);
                }
            }
        }
    }

    let block_time: Option<u64> = block_summary
        .as_ref()
        .and_then(|summary| deserialize::<bitcoin::blockdata::block::Header>(&summary.header).ok())
        .map(|header| header.time as u64);
    let confirmations = (tip >= height).then_some(tip - height + 1);
    let traces_count: Option<usize> =
        block_summary.as_ref().map(|summary| summary.trace_count as usize);
    let interaction_count: Option<usize> = if runes_enabled && espo_indexed {
        let mut txids: HashSet<Txid> = HashSet::new();
        let list_id = address_index_list_id_alkane_block_txs(height);
        let alkane_total = get_address_index_list_len(
            &essentials_provider,
            StateAt::Latest,
            AddressIndexListKind::AlkaneBlockTxs,
            &list_id,
        )
        .unwrap_or(0);
        if alkane_total > 0 {
            let ids = get_address_index_list_range(
                &essentials_provider,
                StateAt::Latest,
                AddressIndexListKind::AlkaneBlockTxs,
                &list_id,
                0,
                alkane_total,
            )
            .unwrap_or_default();
            for id in ids {
                if let Some(blob) = load_tx_pointer_blob_v3_by_id(&essentials_provider, id) {
                    txids.insert(Txid::from_byte_array(blob.txid));
                }
            }
        }
        let rune_total = runes_provider.get_block_tx_count(height).unwrap_or(0);
        if rune_total > 0 {
            for pointer in
                runes_provider.get_block_tx_range(height, 0, rune_total).unwrap_or_default()
            {
                txids.insert(Txid::from_byte_array(pointer.txid));
            }
        }
        Some(if txids.is_empty() { traces_count.unwrap_or(0) } else { txids.len() })
    } else {
        traces_count
    };
    let tx_count: Option<u64> = block_summary
        .as_ref()
        .and_then(|summary| (summary.tx_count > 0).then_some(summary.tx_count as u64))
        .or_else(|| espo_block.as_ref().map(|b| b.tx_count as u64));
    let median_fee_rate: Option<f64> = block_summary.as_ref().map(|summary| summary.fee_median);

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Timestamp".to_string(),
        value: match block_time {
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
        label: "Tx count".to_string(),
        value: match tx_count {
            Some(c) => html! { span class="summary-value" { (format_with_commas(c)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: if runes_enabled { "Interactions" } else { "Traces" }.to_string(),
        value: match interaction_count {
            Some(t) => html! { span class="summary-value" { (format_with_commas(t as u64)) } },
            None => html! { span class="summary-value muted" { (if espo_indexed { "—" } else { "Not indexed" }) } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Median feerate".to_string(),
        value: match median_fee_rate {
            Some(fee_rate) => html! { span class="summary-value" { (format_fee_rate(fee_rate)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Avg Diesel".to_string(),
        value: price_summary_value(diesel_avg_price_paid_usd),
    });
    summary_items.push(HeaderSummaryItem {
        label: "Avg UG".to_string(),
        value: price_summary_value(uncommon_goods_avg_price_paid_usd),
    });

    let pill = confirmations
        .map(|c| (format!("{} confirmations", format_with_commas(c)), HeaderPillTone::Success))
        .or_else(|| Some(("Unconfirmed".to_string(), HeaderPillTone::Warning)));
    let header_markup = header(HeaderProps {
        title: format!("Block {}", format_with_commas(height)),
        id: Some(block_hash_hex.clone()),
        show_copy: true,
        pill,
        summary_items,
        cta: None,
        hero_class: None,
    });
    let tx_filter_label = match tx_filter {
        TxFilter::All => "All Txs",
        TxFilter::Action => "All Actions",
        TxFilter::Alkane => "Only Alkanes",
        TxFilter::Rune => "Only Runes",
    };
    let mut tx_filter_dropdown_items = vec![
        DropdownItem {
            label: "All Txs".to_string(),
            href: explorer_path(&format!(
                "/block/{height}?tab=txs&page=1&limit={limit}&txs=all&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::All,
        },
        DropdownItem {
            label: "Only Alkanes".to_string(),
            href: explorer_path(&format!(
                "/block/{height}?tab=txs&page=1&limit={limit}&txs=alkane&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::Alkane,
        },
    ];
    if runes_enabled {
        tx_filter_dropdown_items.insert(
            1,
            DropdownItem {
                label: "All Actions".to_string(),
                href: explorer_path(&format!(
                    "/block/{height}?tab=txs&page=1&limit={limit}&txs=actions&hide_diesel_mints={hide_diesel_param}"
                )),
                icon: None,
                selected: tx_filter == TxFilter::Action,
            },
        );
        tx_filter_dropdown_items.push(DropdownItem {
            label: "Only Runes".to_string(),
            href: explorer_path(&format!(
                "/block/{height}?tab=txs&page=1&limit={limit}&txs=rune&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::Rune,
        });
    }
    let tx_filter_dropdown = dropdown(DropdownProps {
        label: Some(tx_filter_label.to_string()),
        selected_icon: None,
        items: tx_filter_dropdown_items,
        aria_label: Some("Transaction filter".to_string()),
    });

    layout_with_meta(
        &format!("Block {height}"),
        &canonical_path,
        None,
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(Some(height), espo_tip))
            }

            (header_markup)
            @if let Some(url) = mempool_url {
                div class="tx-mempool-row" {
                    a class="tx-mempool-link" href=(url) target="_blank" rel="noopener noreferrer" {
                        "view on mempool.space"
                        (icon_arrow_up_right())
                    }
                }
            }

            @if !espo_indexed {
                p class="error" { (format!("ESPO hasn't indexed this block yet (latest indexed height: {}).", espo_tip)) }
            }

            div class="card" {
                div class="row tx-filter-row" {
                    h2 class="h2" { "Transactions" }
                    @if espo_indexed {
                        div class="trace-toggle" {
                            div class="tx-filter-segments segmented-control" role="group" aria-label="Transaction filter" {
                                a class=(if tx_filter == TxFilter::All { "segment active" } else { "segment" })
                                    href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&txs=all&hide_diesel_mints={hide_diesel_param}"))) {
                                    "All Txs"
                                }
                                @if runes_enabled {
                                    a class=(if tx_filter == TxFilter::Action { "segment active" } else { "segment" })
                                        href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&txs=actions&hide_diesel_mints={hide_diesel_param}"))) {
                                        "All Actions"
                                    }
                                }
                                a class=(if tx_filter == TxFilter::Alkane { "segment active" } else { "segment" })
                                    href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&txs=alkane&hide_diesel_mints={hide_diesel_param}"))) {
                                    "Only Alkanes"
                                }
                                @if runes_enabled {
                                    a class=(if tx_filter == TxFilter::Rune { "segment active" } else { "segment" })
                                        href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&txs=rune&hide_diesel_mints={hide_diesel_param}"))) {
                                        "Only Runes"
                                    }
                                }
                            }
                            div class="tx-filter-dropdown" {
                                (tx_filter_dropdown)
                            }
                            form method="get" action=(explorer_path(&format!("/block/{height}"))) {
                                input type="hidden" name="tab" value="txs";
                                input type="hidden" name="limit" value=(limit);
                                input type="hidden" name="page" value="1";
                                input type="hidden" name="txs" value=(txs_param);
                                input type="hidden" name="hide_diesel_mints" value=(hide_diesel_param);
                                label class="switch" {
                                    span class="switch-label" {
                                        @if runes_enabled {
                                            "Hide Diesel+UG mints"
                                        } @else {
                                            "Hide Diesel mints"
                                        }
                                    }
                                    input
                                        class="switch-input"
                                        type="checkbox"
                                        checked[hide_diesel_mints]
                                        onchange="this.form.hide_diesel_mints.value = this.checked ? '1' : '0'; this.form.submit();";
                                    span class="switch-slider" {}
                                }
                            }
                        }
                    }
                }

                @if !espo_indexed {
                    p class="muted" { "Transactions will appear once ESPO indexes this block." }
                } @else if tx_total == 0 {
                    p class="muted" { "No transactions found." }
                } @else if tx_items.is_empty() {
                    p class="muted" { "No transactions match the current filters." }
                } @else {
                    @let block_confirmations = tip.saturating_sub(height).saturating_add(1);
                    @let base_pill = TxPill {
                        label: format!("{} confirmations", format_with_commas(block_confirmations)),
                        tone: TxPillTone::Success,
                    };
                    div class="list" {
                        @for item in tx_items {
                            @let traces: Option<&[EspoTrace]> = item.traces.as_ref().map(|v| v.as_slice());
                            (render_tx(&item.txid, &item.tx, traces, network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, Some(base_pill.clone()), None, None, None, true, false))
                        }
                    }
                }

                @if espo_indexed {
                    div class="pager" {
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page=1&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}"))) aria-label="First page" {
                                (icon_pager_first())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_first()) }
                        }
                        @if tx_has_prev {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}", page - 1))) aria-label="Previous page" {
                                (icon_pager_left())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_left()) }
                        }
                        span class="pager-meta muted" { "Showing "
                            (format_with_commas(if tx_total > 0 { display_start as u64 } else { 0 }))
                            @if tx_total > 0 {
                                "-"
                                (format_with_commas(display_end as u64))
                            }
                            " / "
                            (format_with_commas(tx_total as u64))
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}", page + 1))) aria-label="Next page" {
                                (icon_pager_right())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_right()) }
                        }
                        @if tx_has_next {
                            a class="pill iconbtn" href=(explorer_path(&format!("/block/{height}?tab=txs&page={}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}", last_page))) aria-label="Last page" {
                                (icon_pager_last())
                            }
                        } @else {
                            span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_last()) }
                        }
                    }
                }
            }

            (header_scripts())
        },
    )
    .into_response()
}

pub async fn mempool_block_page(
    State(state): State<ExplorerState>,
    Path(display_index): Path<usize>,
    Query(q): Query<BlockPageQuery>,
) -> Response {
    let network = get_network();
    let electrum_like = get_electrum_like();
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
    let runes_enabled = runes_enabled_from_global_config();
    let requested_filter = if runes_enabled && q.txs.is_none() && q.traces.is_none() {
        TxFilter::Action
    } else {
        TxFilter::from_query(q.txs.as_deref(), q.traces.as_deref())
    };
    let tx_filter =
        if !runes_enabled && matches!(requested_filter, TxFilter::Rune | TxFilter::Action) {
            TxFilter::Alkane
        } else {
            requested_filter
        };
    let hide_diesel_mints = q
        .hide_diesel_mints
        .as_deref()
        .map(|v| matches!(v, "1" | "true" | "on" | "yes"))
        .unwrap_or(false);
    let txs_param = tx_filter.query_value();
    let hide_diesel_param = if hide_diesel_mints { "1" } else { "0" };
    let display_index = display_index.max(1);
    let template_index = display_index - 1;
    let canonical_path = format!("/mempool-block/{display_index}");

    let Some(detail) = get_mempool_block_detail(
        template_index,
        page,
        limit,
        tx_filter.mempool_filter(),
        hide_diesel_mints,
    ) else {
        return (
            StatusCode::NOT_FOUND,
            layout_with_meta(
                "Mempool Block",
                &canonical_path,
                None,
                html! { p class="error" { "Projected mempool block is not available yet. The mempool service may still be loading." } },
            ),
        )
            .into_response();
    };

    let tx_total = detail.tx_total;
    let off = limit.saturating_mul(page.saturating_sub(1));
    let display_start = if tx_total > 0 && off < tx_total { off + 1 } else { 0 };
    let display_end =
        if tx_total > 0 && off < tx_total { (off + detail.txs.len()).min(tx_total) } else { 0 };
    let last_page = if tx_total > 0 { (tx_total + limit - 1) / limit } else { 1 };
    let tx_has_prev = page > 1 && off < tx_total;
    let tx_has_next = display_end < tx_total;
    let page_txids: HashSet<Txid> = detail.txs.iter().map(|item| item.txid).collect();
    let projection_txs_for_balances =
        get_mempool_block_transactions_for_targets(template_index, &page_txids)
            .unwrap_or_else(|| detail.txs.clone());

    let mut all_outpoints: Vec<(Txid, u32)> = Vec::new();
    for item in &detail.txs {
        for (vout, _) in item.tx.output.iter().enumerate() {
            all_outpoints.push((item.txid, vout as u32));
        }
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                all_outpoints.push((vin.previous_output.txid, vin.previous_output.vout));
            }
        }
    }
    for item in &projection_txs_for_balances {
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                all_outpoints.push((vin.previous_output.txid, vin.previous_output.vout));
            }
        }
    }
    all_outpoints.sort();
    all_outpoints.dedup();
    let outpoint_map = get_outpoint_balances_with_spent_batch(
        StateAt::Latest,
        &state.essentials_provider(),
        &all_outpoints,
    )
    .unwrap_or_default();
    let projected_balances_by_tx =
        mempool_block_projected_balances(&projection_txs_for_balances, &outpoint_map);
    let projected_balances_by_outpoint: HashMap<(Txid, u32), Vec<BalanceEntry>> =
        projected_balances_by_tx
            .iter()
            .flat_map(|(txid, outputs)| {
                outputs.iter().map(|(vout, balances)| ((*txid, *vout), balances.clone()))
            })
            .collect();
    let outpoint_fn = move |txid: &Txid, vout: u32| -> OutpointLookup {
        let mut lookup = outpoint_map.get(&(*txid, vout)).cloned().unwrap_or_default();
        if lookup.balances.is_empty() {
            if let Some(projected) = projected_balances_by_outpoint.get(&(*txid, vout)) {
                lookup.balances = projected.clone();
            }
        }
        lookup
    };
    let outspends_map: std::collections::HashMap<Txid, Vec<Option<Txid>>> = {
        let mempool_spenders = get_mempool_block_spenders(template_index).unwrap_or_default();
        let mut dedup: Vec<Txid> = detail.txs.iter().map(|t| t.txid).collect();
        dedup.sort();
        dedup.dedup();
        let fetched = electrum_like.batch_transaction_get_outspends(&dedup).unwrap_or_default();
        let mut map: HashMap<Txid, Vec<Option<Txid>>> =
            dedup.iter().copied().zip(fetched.into_iter()).collect();
        for item in &detail.txs {
            let entry = map.entry(item.txid).or_default();
            if entry.len() < item.tx.output.len() {
                entry.resize(item.tx.output.len(), None);
            }
            for vout in 0..item.tx.output.len() {
                if let Some(spender) = mempool_spenders.get(&(item.txid, vout as u32)).copied() {
                    entry[vout] = Some(spender);
                }
            }
        }
        map
    };
    let outspends_fn = move |txid: &Txid| -> Vec<Option<Txid>> {
        outspends_map.get(txid).cloned().unwrap_or_default()
    };

    let mut prev_txids: Vec<Txid> = Vec::new();
    for item in &detail.txs {
        for vin in &item.tx.input {
            if !vin.previous_output.is_null() {
                prev_txids.push(vin.previous_output.txid);
            }
        }
    }
    prev_txids.sort();
    prev_txids.dedup();

    let mut prev_map = get_mempool_transactions(&prev_txids);
    let missing_prev: Vec<Txid> =
        prev_txids.into_iter().filter(|txid| !prev_map.contains_key(txid)).collect();
    if !missing_prev.is_empty() {
        let raws = electrum_like.batch_transaction_get_raw(&missing_prev).unwrap_or_default();
        for (i, raw_prev) in raws.into_iter().enumerate() {
            if raw_prev.is_empty() {
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                prev_map.insert(missing_prev[i], prev_tx);
            }
        }
    }

    let mut summary_items: Vec<HeaderSummaryItem> = Vec::new();
    summary_items.push(HeaderSummaryItem {
        label: "Timestamp".to_string(),
        value: html! { span class="summary-value muted" { "Pending" } },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Tx count".to_string(),
        value: html! { span class="summary-value" { (format_with_commas(detail.template.tx_count as u64)) } },
    });
    summary_items.push(HeaderSummaryItem {
        label: if runes_enabled { "Interactions" } else { "Traces" }.to_string(),
        value: html! { span class="summary-value" { (format_with_commas(detail.template.trace_count as u64)) } },
    });
    summary_items.push(HeaderSummaryItem {
        label: "Median feerate".to_string(),
        value: match detail.template.median_fee_rate {
            Some(fee_rate) => html! { span class="summary-value" { (format_fee_rate(fee_rate)) } },
            None => html! { span class="summary-value muted" { "—" } },
        },
    });

    let header_markup = header(HeaderProps {
        title: format!("Mempool Block #{}", display_index),
        id: None,
        show_copy: false,
        pill: None,
        summary_items,
        cta: None,
        hero_class: None,
    });
    let tx_filter_label = match tx_filter {
        TxFilter::All => "All Txs",
        TxFilter::Action => "All Actions",
        TxFilter::Alkane => "Only Alkanes",
        TxFilter::Rune => "Only Runes",
    };
    let mut tx_filter_dropdown_items = vec![
        DropdownItem {
            label: "All Txs".to_string(),
            href: explorer_path(&format!(
                "/mempool-block/{display_index}?page=1&limit={limit}&txs=all&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::All,
        },
        DropdownItem {
            label: "Only Alkanes".to_string(),
            href: explorer_path(&format!(
                "/mempool-block/{display_index}?page=1&limit={limit}&txs=alkane&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::Alkane,
        },
    ];
    if runes_enabled {
        tx_filter_dropdown_items.insert(
            1,
            DropdownItem {
                label: "All Actions".to_string(),
                href: explorer_path(&format!(
                    "/mempool-block/{display_index}?page=1&limit={limit}&txs=actions&hide_diesel_mints={hide_diesel_param}"
                )),
                icon: None,
                selected: tx_filter == TxFilter::Action,
            },
        );
        tx_filter_dropdown_items.push(DropdownItem {
            label: "Only Runes".to_string(),
            href: explorer_path(&format!(
                "/mempool-block/{display_index}?page=1&limit={limit}&txs=rune&hide_diesel_mints={hide_diesel_param}"
            )),
            icon: None,
            selected: tx_filter == TxFilter::Rune,
        });
    }
    let tx_filter_dropdown = dropdown(DropdownProps {
        label: Some(tx_filter_label.to_string()),
        selected_icon: None,
        items: tx_filter_dropdown_items,
        aria_label: Some("Transaction filter".to_string()),
    });

    layout_with_meta(
        &format!("Mempool Block #{display_index}"),
        &canonical_path,
        None,
        html! {
            div class="block-hero full-bleed" {
                (block_carousel_with_mempool(Some(template_index), espo_tip))
            }

            (header_markup)

            div class="card" {
                div class="row tx-filter-row" {
                    h2 class="h2" { "Transactions" }
                    div class="trace-toggle" {
                        div class="tx-filter-segments segmented-control" role="group" aria-label="Transaction filter" {
                            a class=(if tx_filter == TxFilter::All { "segment active" } else { "segment" })
                                href=(explorer_path(&format!("/mempool-block/{display_index}?page=1&limit={limit}&txs=all&hide_diesel_mints={hide_diesel_param}"))) {
                                "All Txs"
                            }
                            @if runes_enabled {
                                a class=(if tx_filter == TxFilter::Action { "segment active" } else { "segment" })
                                    href=(explorer_path(&format!("/mempool-block/{display_index}?page=1&limit={limit}&txs=actions&hide_diesel_mints={hide_diesel_param}"))) {
                                    "All Actions"
                                }
                            }
                            a class=(if tx_filter == TxFilter::Alkane { "segment active" } else { "segment" })
                                href=(explorer_path(&format!("/mempool-block/{display_index}?page=1&limit={limit}&txs=alkane&hide_diesel_mints={hide_diesel_param}"))) {
                                "Only Alkanes"
                            }
                            @if runes_enabled {
                                a class=(if tx_filter == TxFilter::Rune { "segment active" } else { "segment" })
                                    href=(explorer_path(&format!("/mempool-block/{display_index}?page=1&limit={limit}&txs=rune&hide_diesel_mints={hide_diesel_param}"))) {
                                    "Only Runes"
                                }
                            }
                        }
                        div class="tx-filter-dropdown" {
                            (tx_filter_dropdown)
                        }
                        form method="get" action=(explorer_path(&format!("/mempool-block/{display_index}"))) {
                            input type="hidden" name="limit" value=(limit);
                            input type="hidden" name="page" value="1";
                            input type="hidden" name="txs" value=(txs_param);
                            input type="hidden" name="hide_diesel_mints" value=(hide_diesel_param);
                            label class="switch" {
                                span class="switch-label" {
                                    @if runes_enabled {
                                        "Hide Diesel+UG mints"
                                    } @else {
                                        "Hide Diesel mints"
                                    }
                                }
                                input
                                    class="switch-input"
                                    type="checkbox"
                                    checked[hide_diesel_mints]
                                    onchange="this.form.hide_diesel_mints.value = this.checked ? '1' : '0'; this.form.submit();";
                                span class="switch-slider" {}
                            }
                        }
                    }
                }

                @if tx_total == 0 {
                    @if hide_diesel_mints {
                        p class="muted" { "No non-mint transactions match the current filters." }
                    } @else if tx_filter == TxFilter::Alkane {
                        p class="muted" { "No Alkanes transactions are projected for this mempool block yet." }
                    } @else if tx_filter == TxFilter::Rune {
                        p class="muted" { "No Runes transactions are projected for this mempool block yet." }
                    } @else if tx_filter == TxFilter::Action {
                        p class="muted" { "No action transactions are projected for this mempool block yet." }
                    } @else {
                        p class="muted" { "No transactions projected for this mempool block yet." }
                    }
                } @else if detail.txs.is_empty() {
                    p class="muted" { "No transactions on this page." }
                } @else {
                    div class="list" {
                        @for item in detail.txs {
                            @let traces: Option<&[EspoTrace]> = item.traces.as_ref().map(|v| v.as_slice());
                            @let status_pill = TxPill {
                                label: "Unconfirmed".to_string(),
                                tone: TxPillTone::Danger,
                            };
                            @let projected_balances = projected_balances_by_tx.get(&item.txid);
                            @let projected_rune_io = item.rune_io.as_ref();
                            (render_tx(&item.txid, &item.tx, traces, network, &prev_map, &outpoint_fn, &outspends_fn, &state.essentials_mdb, Some(status_pill), Some(item.fee_rate), projected_balances, projected_rune_io, true, item.defer_alkane_trace_status))
                        }
                    }
                }

                div class="pager" {
                    @if tx_has_prev {
                        a class="pill iconbtn" href=(explorer_path(&format!("/mempool-block/{display_index}?page=1&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}"))) aria-label="First page" {
                            (icon_pager_first())
                        }
                    } @else {
                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_first()) }
                    }
                    @if tx_has_prev {
                        a class="pill iconbtn" href=(explorer_path(&format!("/mempool-block/{display_index}?page={}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}", page - 1))) aria-label="Previous page" {
                            (icon_pager_left())
                        }
                    } @else {
                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_left()) }
                    }
                    span class="pager-meta muted" { "Showing "
                        (format_with_commas(display_start as u64))
                        @if tx_total > 0 {
                            "-"
                            (format_with_commas(display_end as u64))
                        }
                        " / "
                        (format_with_commas(tx_total as u64))
                    }
                    @if tx_has_next {
                        a class="pill iconbtn" href=(explorer_path(&format!("/mempool-block/{display_index}?page={}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}", page + 1))) aria-label="Next page" {
                            (icon_pager_right())
                        }
                    } @else {
                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_right()) }
                    }
                    @if tx_has_next {
                        a class="pill iconbtn" href=(explorer_path(&format!("/mempool-block/{display_index}?page={last_page}&limit={limit}&txs={txs_param}&hide_diesel_mints={hide_diesel_param}"))) aria-label="Last page" {
                            (icon_pager_last())
                        }
                    } @else {
                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_pager_last()) }
                    }
                }
            }

            (header_scripts())
        },
    )
    .into_response()
}
