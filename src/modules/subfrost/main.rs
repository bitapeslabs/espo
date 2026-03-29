use super::consts::get_frbtc_alkane;
use super::rpc::register_rpc;
use super::schemas::SchemaWrapEventV1;
use super::storage::{
    BuildEventListAppendsParams, BuildUnwrapTotalPointAppendsParams, GetIndexHeightParams,
    SetBatchParams, SetIndexHeightParams, SubfrostProvider, UnwrapTotalPoint,
};
use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceInvokeData,
    EspoSandshrewLikeTraceStatus, EspoSandshrewLikeTraceTransfer,
};
use crate::config::{debug_enabled, get_electrum_like, get_network};
use crate::debug;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::utils::balances::clean_espo_sandshrew_like_trace;
use crate::runtime::mdb::Mdb;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash as _;
use bitcoin::{Network, Transaction, Txid};
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Subfrost {
    provider: Option<Arc<SubfrostProvider>>,
}

impl Subfrost {
    pub fn new() -> Self {
        Self { provider: None }
    }

    #[inline]
    fn provider(&self) -> &SubfrostProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        let resp = self
            .provider()
            .get_index_height(GetIndexHeightParams { blockhash: StateAt::Latest })?;
        Ok(resp.height)
    }

    fn persist_index_height(&self, height: u32, blockhash: StateAt) -> Result<()> {
        self.provider()
            .set_index_height(SetIndexHeightParams { blockhash, height })
            .map_err(|e| anyhow!("[SUBFROST] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32, blockhash: StateAt) -> Result<()> {
        if let Some(prev) = self.load_index_height()? {
            if new_height < prev {
                eprintln!("[SUBFROST] index height rollback detected ({} -> {})", prev, new_height);
            }
        }
        self.persist_index_height(new_height, blockhash)?;
        Ok(())
    }
}

impl Default for Subfrost {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Subfrost {
    fn get_name(&self) -> &'static str {
        "subfrost"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        self.provider = Some(Arc::new(SubfrostProvider::new(mdb)));
        match self.load_index_height() {
            Ok(h) => eprintln!("[SUBFROST] loaded index height: {:?}", h),
            Err(e) => eprintln!("[SUBFROST] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, _network: Network) -> u32 {
        0
    }

    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        self.provider.as_ref().map(|provider| Arc::new(provider.mdb().clone()))
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let provider = self.provider();
        let table = provider.table();
        let height = block.height;
        let block_hash = block.block_header.block_hash();

        let timer = debug::start_if(debug);
        let block_ts = block.block_header.time as u64;
        let frbtc = get_frbtc_alkane(get_network());

        let mut block_tx_map: HashMap<Txid, &Transaction> = HashMap::new();
        for atx in &block.transactions {
            block_tx_map.insert(atx.transaction.compute_txid(), &atx.transaction);
        }
        let mut prev_tx_cache: HashMap<Txid, Transaction> = HashMap::new();

        let mut wrap_count: usize = 0;
        let mut unwrap_count: usize = 0;
        let mut unwrap_delta_all: u128 = 0;
        let mut unwrap_delta_success: u128 = 0;
        let mut wrap_events_all: Vec<SchemaWrapEventV1> = Vec::new();
        let mut unwrap_events_all: Vec<SchemaWrapEventV1> = Vec::new();
        let mut wrap_events_by_address: HashMap<Vec<u8>, Vec<SchemaWrapEventV1>> = HashMap::new();
        let mut unwrap_events_by_address: HashMap<Vec<u8>, Vec<SchemaWrapEventV1>> = HashMap::new();
        debug::log_elapsed(module, "init_context", timer);

        let timer = debug::start_if(debug);
        for tx in &block.transactions {
            let txid = tx.transaction.compute_txid();
            let Some(traces) = &tx.traces else { continue };
            let mut address_spk_bytes: Option<Vec<u8>> = None;
            for trace in traces {
                let Some(cleaned) = clean_espo_sandshrew_like_trace(
                    &trace.sandshrew_trace,
                    &block.host_function_values,
                ) else {
                    continue;
                };
                let mut stack: Vec<Option<PendingWrap>> = Vec::new();
                for ev in &cleaned.events {
                    match ev {
                        EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                            let Some((kind, amount)) = parse_wrap_invoke(inv, frbtc) else {
                                stack.push(None);
                                continue;
                            };
                            let address_spk_bytes = address_spk_bytes.get_or_insert_with(|| {
                                let address_spk = tx_owner_spk(
                                    &tx.transaction,
                                    &block_tx_map,
                                    &mut prev_tx_cache,
                                );
                                address_spk.map(|s| s.as_bytes().to_vec()).unwrap_or_default()
                            });
                            stack.push(Some(PendingWrap {
                                kind,
                                amount,
                                address_spk: address_spk_bytes.clone(),
                            }));
                        }
                        EspoSandshrewLikeTraceEvent::Return(ret) => {
                            let Some(pending) = stack.pop().flatten() else { continue };
                            let success = ret.status == EspoSandshrewLikeTraceStatus::Success;
                            let amount = match pending.kind {
                                WrapKind::Wrap => {
                                    extract_amount_for_alkane(&ret.response.alkanes, frbtc)
                                }
                                WrapKind::Unwrap => pending.amount,
                            };
                            let Some(amount) = amount else { continue };
                            let event = SchemaWrapEventV1 {
                                timestamp: block_ts,
                                txid: txid.to_byte_array(),
                                amount,
                                address_spk: pending.address_spk,
                                success,
                            };
                            if matches!(pending.kind, WrapKind::Unwrap) {
                                unwrap_delta_all = unwrap_delta_all.saturating_add(amount);
                                if success {
                                    unwrap_delta_success =
                                        unwrap_delta_success.saturating_add(amount);
                                }
                            }
                            match pending.kind {
                                WrapKind::Wrap => {
                                    wrap_events_by_address
                                        .entry(event.address_spk.clone())
                                        .or_default()
                                        .push(event.clone());
                                    wrap_events_all.push(event);
                                    wrap_count = wrap_count.saturating_add(1);
                                }
                                WrapKind::Unwrap => {
                                    unwrap_events_by_address
                                        .entry(event.address_spk.clone())
                                        .or_default()
                                        .push(event.clone());
                                    unwrap_events_all.push(event);
                                    unwrap_count = unwrap_count.saturating_add(1);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        debug::log_elapsed(module, "process_traces", timer);

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if !wrap_events_all.is_empty() {
            puts.extend(provider.build_event_list_appends(BuildEventListAppendsParams {
                list_prefix: table.WRAP_EVENTS_ALL.key().to_vec(),
                events: wrap_events_all,
            })?);
        }
        if !unwrap_events_all.is_empty() {
            puts.extend(provider.build_event_list_appends(BuildEventListAppendsParams {
                list_prefix: table.UNWRAP_EVENTS_ALL.key().to_vec(),
                events: unwrap_events_all,
            })?);
        }
        for (spk, events) in wrap_events_by_address {
            puts.extend(provider.build_event_list_appends(BuildEventListAppendsParams {
                list_prefix: table.wrap_events_by_address_prefix(&spk),
                events,
            })?);
        }
        for (spk, events) in unwrap_events_by_address {
            puts.extend(provider.build_event_list_appends(BuildEventListAppendsParams {
                list_prefix: table.unwrap_events_by_address_prefix(&spk),
                events,
            })?);
        }

        if !puts.is_empty() || unwrap_delta_all > 0 || unwrap_delta_success > 0 {
            let timer = debug::start_if(debug);
            if unwrap_delta_all > 0 || unwrap_delta_success > 0 {
                let prev_all = provider
                    .get_unwrap_total_latest(super::storage::GetUnwrapTotalLatestParams {
                        blockhash: StateAt::Block(block_hash),
                        successful: false,
                        height: None,
                        height_present: false,
                    })
                    .map(|res| res.total)
                    .unwrap_or(0);
                let prev_success = provider
                    .get_unwrap_total_latest(super::storage::GetUnwrapTotalLatestParams {
                        blockhash: StateAt::Block(block_hash),
                        successful: true,
                        height: None,
                        height_present: false,
                    })
                    .map(|res| res.total)
                    .unwrap_or(0);
                let total_all = prev_all.saturating_add(unwrap_delta_all);
                let total_success = prev_success.saturating_add(unwrap_delta_success);
                puts.push((table.unwrap_total_latest_key(false), encode_u128_value(total_all)));
                puts.push((table.unwrap_total_latest_key(true), encode_u128_value(total_success)));
                puts.push((
                    table.unwrap_total_by_height_key(block.height, false),
                    encode_u128_value(total_all),
                ));
                puts.push((
                    table.unwrap_total_by_height_key(block.height, true),
                    encode_u128_value(total_success),
                ));
                puts.extend(provider.build_unwrap_total_point_appends(
                    BuildUnwrapTotalPointAppendsParams {
                        successful: false,
                        points: vec![UnwrapTotalPoint { height: block.height, total: total_all }],
                    },
                )?);
                puts.extend(provider.build_unwrap_total_point_appends(
                    BuildUnwrapTotalPointAppendsParams {
                        successful: true,
                        points: vec![UnwrapTotalPoint {
                            height: block.height,
                            total: total_success,
                        }],
                    },
                )?);
            }
            debug::log_elapsed(module, "update_totals", timer);
            let timer = debug::start_if(debug);
            provider
                .set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes: Vec::new() })
                .map_err(|e| anyhow!("[SUBFROST] set_batch failed at height {}: {e}", height))?;
            debug::log_elapsed(module, "write_batch", timer);
        }

        println!(
            "[SUBFROST] finished block #{} (wraps={}, unwraps={})",
            block.height, wrap_count, unwrap_count
        );
        let timer = debug::start_if(debug);
        self.set_index_height(block.height, StateAt::Latest)?;
        debug::log_elapsed(module, "finalize", timer);
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        self.load_index_height().ok().flatten()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        if let Some(provider) = self.provider.as_ref() {
            register_rpc(reg, provider.clone());
        }
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some("{ }")
    }
}

#[derive(Clone)]
enum WrapKind {
    Wrap,
    Unwrap,
}

#[derive(Clone)]
struct PendingWrap {
    kind: WrapKind,
    amount: Option<u128>,
    address_spk: Vec<u8>,
}

fn parse_wrap_invoke(
    invoke: &EspoSandshrewLikeTraceInvokeData,
    frbtc: SchemaAlkaneId,
) -> Option<(WrapKind, Option<u128>)> {
    let myself = parse_trace_id(&invoke.context.myself)?;
    if myself != frbtc {
        return None;
    }
    let opcode0 = invoke.context.inputs.get(0).and_then(|s| parse_hex_u64(s));
    let opcode2 = invoke.context.inputs.get(2).and_then(|s| parse_hex_u64(s));
    let opcode = match opcode0 {
        Some(0x4d) | Some(0x4e) => opcode0,
        _ => opcode2,
    }?;
    let kind = match opcode {
        0x4d => WrapKind::Wrap,
        0x4e => WrapKind::Unwrap,
        _ => return None,
    };
    let amount = match kind {
        WrapKind::Wrap => None,
        WrapKind::Unwrap => extract_amount_for_alkane(&invoke.context.incoming_alkanes, frbtc),
    };
    Some((kind, amount))
}

fn extract_amount_for_alkane(
    transfers: &[EspoSandshrewLikeTraceTransfer],
    target: SchemaAlkaneId,
) -> Option<u128> {
    let mut found = false;
    let mut total: u128 = 0;
    for t in transfers {
        let Some(id) = parse_trace_id(&t.id) else { continue };
        if id != target {
            continue;
        }
        let Some(value) = parse_hex_u128(&t.value) else { continue };
        found = true;
        total = total.saturating_add(value);
    }
    if found { Some(total) } else { None }
}

fn parse_trace_id(
    id: &crate::alkanes::trace::EspoSandshrewLikeTraceShortId,
) -> Option<SchemaAlkaneId> {
    let block = parse_hex_u32(&id.block)?;
    let tx = parse_hex_u64(&id.tx)?;
    Some(SchemaAlkaneId { block, tx })
}

fn parse_hex_u32(s: &str) -> Option<u32> {
    s.strip_prefix("0x")
        .and_then(|h| u32::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u32>().ok())
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    s.strip_prefix("0x")
        .and_then(|h| u64::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u64>().ok())
}

fn parse_hex_u128(s: &str) -> Option<u128> {
    s.strip_prefix("0x")
        .and_then(|h| u128::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u128>().ok())
}

fn tx_owner_spk(
    tx: &Transaction,
    block_tx_map: &HashMap<Txid, &Transaction>,
    prev_tx_cache: &mut HashMap<Txid, Transaction>,
) -> Option<bitcoin::ScriptBuf> {
    let spk = spk_from_protostone(tx);
    if spk.is_some() {
        return spk;
    }

    let mut lowest_spk: Option<bitcoin::ScriptBuf> = None;
    let mut lowest_value: Option<u64> = None;
    for vin in &tx.input {
        if vin.previous_output.is_null() {
            continue;
        }
        let prev_txid = vin.previous_output.txid;
        let prev_tx = if let Some(tx) = block_tx_map.get(&prev_txid) {
            Some((*tx).clone())
        } else if let Some(tx) = prev_tx_cache.get(&prev_txid) {
            Some(tx.clone())
        } else {
            let raw = get_electrum_like()
                .batch_transaction_get_raw(&[prev_txid])
                .unwrap_or_default()
                .into_iter()
                .next()
                .unwrap_or_default();
            if raw.is_empty() {
                None
            } else {
                deserialize::<Transaction>(&raw).ok().map(|tx| {
                    prev_tx_cache.insert(prev_txid, tx.clone());
                    tx
                })
            }
        };
        let Some(prev_tx) = prev_tx else { continue };
        let idx = vin.previous_output.vout as usize;
        let Some(prev_out) = prev_tx.output.get(idx) else { continue };
        let value = prev_out.value.to_sat();
        if lowest_value.map_or(true, |v| value < v) {
            lowest_value = Some(value);
            lowest_spk = Some(prev_out.script_pubkey.clone());
        }
    }
    lowest_spk
}

fn spk_from_protostone(tx: &Transaction) -> Option<bitcoin::ScriptBuf> {
    let Some(Artifact::Runestone(ref runestone)) = Runestone::decipher(tx) else {
        return None;
    };
    let protos = Protostone::from_runestone(runestone).ok()?;
    for ps in protos {
        if ps.protocol_tag != 1 {
            continue;
        }
        if let Some(ptr) = ps.pointer {
            let idx = ptr as usize;
            if let Some(out) = tx.output.get(idx) {
                return Some(out.script_pubkey.clone());
            }
        }
    }
    None
}

fn encode_u128_value(value: u128) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}
