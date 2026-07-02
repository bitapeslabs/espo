use super::config::{MemgraphConfig, memgraph_enabled_from_config};
use super::consts::MEMGRAPH_FAIL_ON_ERROR;
use super::storage::{
    AddressActionRow, AmmEventRow, GraphRows, MemgraphProvider, OutpointRow, ParticipationRow,
    TransferLegRow, TxRow,
};
use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_db, get_module_config, get_network};
use crate::modules::ammdata::main::{
    load_balance_txs_by_height, pool_creator_spk_from_protostone, signed_from_delta,
};
use crate::modules::ammdata::schemas::SchemaMarketDefs;
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetPoolCreationInfoParams, GetPoolDefsParams,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::consts::essentials_genesis_block;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationIdsInBlockParams, OutpointPointerBlobV3,
    load_outpoint_pointer_blob_v3_by_id, resolve_outpoint_id_v2,
};
use crate::runtime::mdb::Mdb;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Context, Result, anyhow};
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network, ScriptBuf, Transaction, Txid};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
struct FlowAmount {
    address: String,
    token: SchemaAlkaneId,
    amount: u128,
    outpoint_id: String,
}

pub struct Memgraph {
    config: MemgraphConfig,
    provider: Option<Arc<MemgraphProvider>>,
    essentials_provider: Option<Arc<EssentialsProvider>>,
    amm_provider: Option<Arc<AmmDataProvider>>,
    index_height: Arc<RwLock<Option<u32>>>,
}

impl Memgraph {
    pub fn new() -> Self {
        Self {
            config: MemgraphConfig::default(),
            provider: None,
            essentials_provider: None,
            amm_provider: None,
            index_height: Arc::new(RwLock::new(None)),
        }
    }

    pub fn enabled_from_global_config() -> bool {
        memgraph_enabled_from_config(get_module_config("memgraph"))
    }

    fn provider(&self) -> &MemgraphProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn essentials(&self) -> &EssentialsProvider {
        self.essentials_provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn amm(&self) -> &AmmDataProvider {
        self.amm_provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        self.provider().get_index_height()
    }

    fn set_index_height(&self, height: u32) -> Result<()> {
        self.provider().set_index_height(height)?;
        *self.index_height.write().unwrap() = Some(height);
        Ok(())
    }
}

impl Default for Memgraph {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Memgraph {
    fn get_name(&self) -> &'static str {
        "memgraph"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let db = get_espo_db();
        let essentials = Arc::new(EssentialsProvider::new(Arc::new(Mdb::from_db(
            Arc::clone(&db),
            b"essentials:",
        ))));
        let amm = Arc::new(AmmDataProvider::new(
            Arc::new(Mdb::from_db(db, b"ammdata:")),
            Arc::clone(&essentials),
        ));
        let provider = Arc::new(MemgraphProvider::new(mdb));
        if let Err(e) = provider.connect_memgraph(&self.config) {
            eprintln!("[MEMGRAPH] failed to initialize Bolt client: {e:?}");
        }
        self.provider = Some(provider);
        self.essentials_provider = Some(essentials);
        self.amm_provider = Some(amm);

        match self.load_index_height() {
            Ok(height) => {
                *self.index_height.write().unwrap() = height;
                eprintln!("[MEMGRAPH] loaded index height: {:?}", height);
            }
            Err(e) => eprintln!("[MEMGRAPH] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        essentials_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let started = std::time::Instant::now();
        let height = block.height;
        if let Some(prev) = *self.index_height.read().unwrap() {
            if height <= prev {
                eprintln!("[MEMGRAPH] skipping already indexed block #{height} (last={prev})");
                return Ok(());
            }
        }

        if !self.provider().is_memgraph_connected() {
            let err = anyhow!("Memgraph Bolt client is not initialized");
            if MEMGRAPH_FAIL_ON_ERROR {
                return Err(err);
            }
            eprintln!("[MEMGRAPH] {err:?}; marking height as indexed");
            self.set_index_height(height)?;
            return Ok(());
        }

        let result = (|| -> Result<()> {
            self.provider().ensure_constraints()?;
            let rows = build_graph_rows(&block, self.essentials(), self.amm(), get_network())?;
            self.provider().write_graph_rows(rows)?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.set_index_height(height)?;
                eprintln!(
                    "[indexer] module=memgraph height={} index_block done in {:?}",
                    height,
                    started.elapsed()
                );
                Ok(())
            }
            Err(e) if MEMGRAPH_FAIL_ON_ERROR => {
                Err(e.context(format!("[MEMGRAPH] failed at height {height}")))
            }
            Err(e) => {
                eprintln!(
                    "[MEMGRAPH] failed at height {} but marking indexed; error={e:?}",
                    height
                );
                self.set_index_height(height)?;
                Ok(())
            }
        }
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn contributes_to_start_height(&self) -> bool {
        self.get_index_height().is_some()
    }

    fn handle_reorg(&self, next_height: u32) -> Result<()> {
        if self.provider().is_memgraph_connected() {
            let result = self.provider().delete_from_height(next_height);
            if let Err(e) = result {
                if MEMGRAPH_FAIL_ON_ERROR {
                    return Err(e.context("[MEMGRAPH] failed to delete reorged graph rows"));
                }
                eprintln!("[MEMGRAPH] failed to delete reorged graph rows: {e:?}");
            }
        }

        let new_height = next_height.saturating_sub(1);
        self.provider().set_index_height(new_height)?;
        *self.index_height.write().unwrap() = Some(new_height);
        eprintln!("[MEMGRAPH] reorg rollback complete; index height: {:?}", Some(new_height));
        Ok(())
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let _ = reg;
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(MemgraphConfig::SPEC)
    }

    fn set_config(&mut self, config: &Value) -> Result<()> {
        self.config = MemgraphConfig::from_json(config)?;
        Ok(())
    }
}

fn build_graph_rows(
    block: &EspoBlock,
    essentials: &EssentialsProvider,
    amm: &AmmDataProvider,
    network: Network,
) -> Result<GraphRows> {
    let mut rows = GraphRows::default();
    let height = block.height;
    let timestamp = block.block_header.time as u64;
    let block_hash = block.block_header.block_hash().to_string();
    let mut tx_index_by_txid: HashMap<Txid, u32> = HashMap::new();
    let mut tx_by_txid: HashMap<Txid, &Transaction> = HashMap::new();
    let mut tx_addresses: HashMap<Txid, HashSet<String>> = HashMap::new();

    for (idx, atx) in block.transactions.iter().enumerate() {
        let txid = atx.transaction.compute_txid();
        tx_index_by_txid.insert(txid, idx as u32);
        tx_by_txid.insert(txid, &atx.transaction);
        rows.txs.push(TxRow {
            txid: txid.to_string(),
            height,
            tx_index: idx as u32,
            timestamp,
            block_hash: block_hash.clone(),
        });
    }

    for atx in &block.transactions {
        index_raw_transfers_for_tx(
            &mut rows,
            essentials,
            &mut tx_addresses,
            &atx.transaction,
            height,
            timestamp,
            &block_hash,
            *tx_index_by_txid.get(&atx.transaction.compute_txid()).unwrap_or(&0),
        )?;
    }

    index_amm_actions(
        &mut rows,
        block,
        amm,
        essentials,
        network,
        &tx_by_txid,
        &tx_index_by_txid,
        &tx_addresses,
    )?;
    index_pool_creations(&mut rows, block, amm, essentials, network, &tx_index_by_txid)?;

    Ok(rows)
}

fn index_raw_transfers_for_tx(
    rows: &mut GraphRows,
    essentials: &EssentialsProvider,
    tx_addresses: &mut HashMap<Txid, HashSet<String>>,
    tx: &Transaction,
    height: u32,
    timestamp: u64,
    block_hash: &str,
    tx_index: u32,
) -> Result<()> {
    let txid = tx.compute_txid();
    let txid_s = txid.to_string();
    let mut inputs_by_token: BTreeMap<SchemaAlkaneId, VecDeque<FlowAmount>> = BTreeMap::new();
    let mut outputs_by_token: BTreeMap<SchemaAlkaneId, Vec<FlowAmount>> = BTreeMap::new();

    for input in &tx.input {
        if input.previous_output.is_null() {
            continue;
        }
        let prev_txid = input.previous_output.txid;
        let vout = input.previous_output.vout;
        let Some(blob) = load_outpoint_blob(essentials, &prev_txid, vout)? else {
            continue;
        };
        let outpoint_id = format!("{prev_txid}:{vout}");
        add_outpoint_row(rows, &blob, outpoint_id.clone(), None, height, Some(txid.to_string()));
        if !blob.address.is_empty() {
            tx_addresses.entry(txid).or_default().insert(blob.address.clone());
            rows.participations.insert(ParticipationRow {
                address: blob.address.clone(),
                txid: txid_s.clone(),
                height,
                timestamp,
            });
        }
        for balance in blob.balances {
            if balance.amount == 0 || blob.address.is_empty() {
                continue;
            }
            rows.add_alkane(balance.alkane);
            inputs_by_token.entry(balance.alkane).or_default().push_back(FlowAmount {
                address: blob.address.clone(),
                token: balance.alkane,
                amount: balance.amount,
                outpoint_id: outpoint_id.clone(),
            });
        }
    }

    for (vout, _output) in tx.output.iter().enumerate() {
        let vout = vout as u32;
        let Some(blob) = load_outpoint_blob(essentials, &txid, vout)? else {
            continue;
        };
        let outpoint_id = format!("{txid}:{vout}");
        add_outpoint_row(rows, &blob, outpoint_id.clone(), Some(height), height, None);
        if !blob.address.is_empty() {
            tx_addresses.entry(txid).or_default().insert(blob.address.clone());
            rows.participations.insert(ParticipationRow {
                address: blob.address.clone(),
                txid: txid_s.clone(),
                height,
                timestamp,
            });
        }
        for balance in blob.balances {
            if balance.amount == 0 || blob.address.is_empty() {
                continue;
            }
            rows.add_alkane(balance.alkane);
            outputs_by_token.entry(balance.alkane).or_default().push(FlowAmount {
                address: blob.address.clone(),
                token: balance.alkane,
                amount: balance.amount,
                outpoint_id: outpoint_id.clone(),
            });
        }
    }

    let mut tokens: HashSet<SchemaAlkaneId> = inputs_by_token.keys().copied().collect();
    tokens.extend(outputs_by_token.keys().copied());
    let mut seq = 0u32;

    for token in tokens {
        let Some(outputs) = outputs_by_token.get(&token) else {
            if let Some(inputs) = inputs_by_token.get(&token) {
                for source in inputs {
                    if source.amount == 0 {
                        continue;
                    }
                    push_one_sided_transfer_action(
                        rows,
                        "BURN",
                        "sent",
                        "raw",
                        &source.address,
                        None,
                        source.token,
                        source.amount,
                        Some(source.outpoint_id.clone()),
                        txid,
                        height,
                        timestamp,
                        tx_index,
                        block_hash,
                        seq,
                    );
                    seq = seq.saturating_add(1);
                }
            }
            continue;
        };
        let sources = inputs_by_token.entry(token).or_default();
        for dest in outputs {
            let mut remaining = dest.amount;
            while remaining > 0 {
                let Some(source) = sources.front_mut() else {
                    push_one_sided_transfer_action(
                        rows,
                        "MINT",
                        "received",
                        "raw",
                        &dest.address,
                        None,
                        dest.token,
                        remaining,
                        Some(dest.outpoint_id.clone()),
                        txid,
                        height,
                        timestamp,
                        tx_index,
                        block_hash,
                        seq,
                    );
                    seq = seq.saturating_add(1);
                    break;
                };
                let amount = source.amount.min(remaining);
                if amount == 0 {
                    sources.pop_front();
                    continue;
                }
                let source_address = source.address.clone();
                let source_outpoint = source.outpoint_id.clone();
                push_paired_transfer_actions(
                    rows,
                    &source_address,
                    &dest.address,
                    token,
                    amount,
                    Some(source_outpoint),
                    Some(dest.outpoint_id.clone()),
                    txid,
                    height,
                    timestamp,
                    tx_index,
                    block_hash,
                    seq,
                );
                source.amount = source.amount.saturating_sub(amount);
                remaining = remaining.saturating_sub(amount);
                if source.amount == 0 {
                    sources.pop_front();
                }
                seq = seq.saturating_add(1);
            }
        }

        while let Some(source) = sources.pop_front() {
            if source.amount == 0 {
                continue;
            }
            push_one_sided_transfer_action(
                rows,
                "BURN",
                "sent",
                "raw",
                &source.address,
                None,
                source.token,
                source.amount,
                Some(source.outpoint_id),
                txid,
                height,
                timestamp,
                tx_index,
                block_hash,
                seq,
            );
            seq = seq.saturating_add(1);
        }
    }

    Ok(())
}

fn index_amm_actions(
    rows: &mut GraphRows,
    block: &EspoBlock,
    amm: &AmmDataProvider,
    essentials: &EssentialsProvider,
    network: Network,
    tx_by_txid: &HashMap<Txid, &Transaction>,
    tx_index_by_txid: &HashMap<Txid, u32>,
    tx_addresses: &HashMap<Txid, HashSet<String>>,
) -> Result<()> {
    let height = block.height;
    let timestamp = block.block_header.time as u64;
    let block_hash = block.block_header.block_hash().to_string();
    let balance_txs = load_balance_txs_by_height(essentials, height)
        .with_context(|| format!("load balance txs for Memgraph height {height}"))?;
    let mut seq_by_tx_pool: HashMap<(Txid, SchemaAlkaneId), u32> = HashMap::new();

    for (pool, entries) in balance_txs {
        let Some(defs) =
            amm.get_pool_defs(GetPoolDefsParams { blockhash: StateAt::Latest, pool })?.defs
        else {
            continue;
        };
        rows.add_pool(defs);

        for entry in entries {
            let base_delta = signed_from_delta(entry.outflow.get(&defs.base_alkane_id));
            let quote_delta = signed_from_delta(entry.outflow.get(&defs.quote_alkane_id));
            if base_delta == 0 && quote_delta == 0 {
                continue;
            }
            let Some(kind) = classify_amm_kind(base_delta, quote_delta) else {
                continue;
            };
            let txid = Txid::from_byte_array(entry.txid);
            let tx_index = *tx_index_by_txid.get(&txid).unwrap_or(&0);
            let tx = tx_by_txid.get(&txid).copied();
            let trader_address = tx
                .and_then(|tx| pool_creator_spk_from_protostone(tx))
                .and_then(|spk| script_to_address(&spk, network))
                .or_else(|| {
                    tx_addresses.get(&txid).and_then(|addresses| addresses.iter().min().cloned())
                });
            let seq = seq_by_tx_pool.entry((txid, pool)).or_insert(0);
            let event_seq = *seq;
            *seq = seq.saturating_add(1);
            push_amm_event_and_action(
                rows,
                kind,
                trader_address.as_deref(),
                txid,
                tx_index,
                height,
                timestamp,
                &block_hash,
                event_seq,
                pool,
                defs,
                base_delta,
                quote_delta,
            );
        }
    }

    Ok(())
}

fn index_pool_creations(
    rows: &mut GraphRows,
    block: &EspoBlock,
    amm: &AmmDataProvider,
    essentials: &EssentialsProvider,
    network: Network,
    tx_index_by_txid: &HashMap<Txid, u32>,
) -> Result<()> {
    let height = block.height;
    let timestamp = block.block_header.time as u64;
    let block_hash = block.block_header.block_hash().to_string();
    let created = essentials
        .get_creation_ids_in_block(GetCreationIdsInBlockParams {
            blockhash: StateAt::Latest,
            height,
        })?
        .alkanes;
    if created.is_empty() {
        return Ok(());
    }

    let mut txid_by_created: HashMap<SchemaAlkaneId, Txid> = HashMap::new();
    for atx in &block.transactions {
        if let Some(traces) = atx.traces.as_ref() {
            for trace in traces {
                for event in &trace.sandshrew_trace.events {
                    if let crate::alkanes::trace::EspoSandshrewLikeTraceEvent::Create(id) = event {
                        if let Some(created_id) = parse_trace_short_id(id) {
                            txid_by_created
                                .entry(created_id)
                                .or_insert_with(|| atx.transaction.compute_txid());
                        }
                    }
                }
            }
        }
    }

    for pool in created {
        let Some(defs) =
            amm.get_pool_defs(GetPoolDefsParams { blockhash: StateAt::Latest, pool })?.defs
        else {
            continue;
        };
        rows.add_pool(defs);
        let info = amm
            .get_pool_creation_info(GetPoolCreationInfoParams { blockhash: StateAt::Latest, pool })?
            .info;
        let creator_address = info.as_ref().and_then(|info| {
            if info.creator_spk.is_empty() {
                None
            } else {
                let spk = ScriptBuf::from(info.creator_spk.clone());
                script_to_address(&spk, network)
            }
        });
        let Some(creator_address) = creator_address else {
            continue;
        };
        let txid = txid_by_created.get(&pool).copied().unwrap_or_else(|| {
            block
                .transactions
                .first()
                .map(|tx| tx.transaction.compute_txid())
                .unwrap_or_else(|| Txid::from_byte_array([0u8; 32]))
        });
        let tx_index = *tx_index_by_txid.get(&txid).unwrap_or(&0);
        let base_amount = info.as_ref().map(|i| i.initial_token0_amount).unwrap_or(0);
        let quote_amount = info.as_ref().map(|i| i.initial_token1_amount).unwrap_or(0);
        let event_id = format!("{}:pool_create:{}", txid, alkane_id_string(&pool));
        rows.amm_events.entry(event_id.clone()).or_insert(AmmEventRow {
            id: event_id.clone(),
            kind: "POOL_CREATE".to_string(),
            txid: txid.to_string(),
            height,
            tx_index,
            timestamp,
            pool_id: alkane_id_string(&pool),
            base_id: alkane_id_string(&defs.base_alkane_id),
            quote_id: alkane_id_string(&defs.quote_alkane_id),
            base_delta_str: base_amount.to_string(),
            quote_delta_str: quote_amount.to_string(),
            trader_address: Some(creator_address.clone()),
            side_token_id: None,
            paid_token_id: None,
            received_token_id: None,
            paid_amount_str: None,
            received_amount_str: None,
        });

        let action_id = format!("{event_id}:{}", creator_address);
        rows.add_action(AddressActionRow {
            id: action_id.clone(),
            address: creator_address.clone(),
            kind: "POOL_CREATE".to_string(),
            source: "amm".to_string(),
            txid: txid.to_string(),
            height,
            tx_index,
            timestamp,
            block_hash: block_hash.clone(),
            pool_id: Some(alkane_id_string(&pool)),
            side_token_id: None,
            paid_token_id: None,
            received_token_id: Some(alkane_id_string(&pool)),
            paid_amount_str: None,
            received_amount_str: info.as_ref().map(|i| i.initial_lp_supply.to_string()),
            base_amount_str: Some(base_amount.to_string()),
            quote_amount_str: Some(quote_amount.to_string()),
            success: true,
        });
        rows.participations.insert(ParticipationRow {
            address: creator_address.clone(),
            txid: txid.to_string(),
            height,
            timestamp,
        });
        if base_amount > 0 {
            rows.add_leg(TransferLegRow {
                id: format!("{action_id}:sent:{}", alkane_id_string(&defs.base_alkane_id)),
                action_id: action_id.clone(),
                direction: "sent".to_string(),
                source: "amm".to_string(),
                txid: txid.to_string(),
                height,
                timestamp,
                token_id: alkane_id_string(&defs.base_alkane_id),
                token_block: defs.base_alkane_id.block,
                token_tx: defs.base_alkane_id.tx,
                amount_str: base_amount.to_string(),
                counterparty_address: None,
                counterparty_pool_id: Some(alkane_id_string(&pool)),
                outpoint_id: None,
            });
        }
        if quote_amount > 0 {
            rows.add_leg(TransferLegRow {
                id: format!("{action_id}:sent:{}", alkane_id_string(&defs.quote_alkane_id)),
                action_id,
                direction: "sent".to_string(),
                source: "amm".to_string(),
                txid: txid.to_string(),
                height,
                timestamp,
                token_id: alkane_id_string(&defs.quote_alkane_id),
                token_block: defs.quote_alkane_id.block,
                token_tx: defs.quote_alkane_id.tx,
                amount_str: quote_amount.to_string(),
                counterparty_address: None,
                counterparty_pool_id: Some(alkane_id_string(&pool)),
                outpoint_id: None,
            });
        }
    }

    Ok(())
}

fn add_outpoint_row(
    rows: &mut GraphRows,
    blob: &OutpointPointerBlobV3,
    outpoint_id: String,
    created_height: Option<u32>,
    seen_height: u32,
    spent_by_txid: Option<String>,
) {
    if blob.address.is_empty() {
        return;
    }
    rows.outpoints.entry(outpoint_id.clone()).or_insert_with(|| OutpointRow {
        id: outpoint_id,
        txid: Txid::from_byte_array(blob.txid).to_string(),
        vout: blob.vout,
        address: blob.address.clone(),
        created_height,
        seen_height,
        spent_by_txid,
    });
}

fn load_outpoint_blob(
    essentials: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<Option<OutpointPointerBlobV3>> {
    let txid_arr = txid.to_byte_array();
    let Some(id) = resolve_outpoint_id_v2(essentials, StateAt::Latest, &txid_arr, vout)? else {
        return Ok(None);
    };
    Ok(load_outpoint_pointer_blob_v3_by_id(essentials, id))
}

fn push_paired_transfer_actions(
    rows: &mut GraphRows,
    from: &str,
    to: &str,
    token: SchemaAlkaneId,
    amount: u128,
    source_outpoint_id: Option<String>,
    dest_outpoint_id: Option<String>,
    txid: Txid,
    height: u32,
    timestamp: u64,
    tx_index: u32,
    block_hash: &str,
    seq: u32,
) {
    if amount == 0 {
        return;
    }
    if from == to {
        push_one_sided_transfer_action(
            rows,
            "TRANSFER_SELF",
            "received",
            "raw",
            to,
            Some(from.to_string()),
            token,
            amount,
            dest_outpoint_id,
            txid,
            height,
            timestamp,
            tx_index,
            block_hash,
            seq,
        );
        return;
    }

    push_one_sided_transfer_action(
        rows,
        "TRANSFER_OUT",
        "sent",
        "raw",
        from,
        Some(to.to_string()),
        token,
        amount,
        source_outpoint_id,
        txid,
        height,
        timestamp,
        tx_index,
        block_hash,
        seq,
    );
    push_one_sided_transfer_action(
        rows,
        "TRANSFER_IN",
        "received",
        "raw",
        to,
        Some(from.to_string()),
        token,
        amount,
        dest_outpoint_id,
        txid,
        height,
        timestamp,
        tx_index,
        block_hash,
        seq,
    );
}

#[allow(clippy::too_many_arguments)]
fn push_one_sided_transfer_action(
    rows: &mut GraphRows,
    kind: &str,
    direction: &str,
    source: &str,
    address: &str,
    counterparty_address: Option<String>,
    token: SchemaAlkaneId,
    amount: u128,
    outpoint_id: Option<String>,
    txid: Txid,
    height: u32,
    timestamp: u64,
    tx_index: u32,
    block_hash: &str,
    seq: u32,
) {
    if address.is_empty() || amount == 0 {
        return;
    }
    rows.add_alkane(token);
    let token_id = alkane_id_string(&token);
    let action_id = format!("{txid}:{source}:{kind}:{address}:{token_id}:{seq}");
    rows.add_action(AddressActionRow {
        id: action_id.clone(),
        address: address.to_string(),
        kind: kind.to_string(),
        source: source.to_string(),
        txid: txid.to_string(),
        height,
        tx_index,
        timestamp,
        block_hash: block_hash.to_string(),
        pool_id: None,
        side_token_id: Some(token_id.clone()),
        paid_token_id: if direction == "sent" { Some(token_id.clone()) } else { None },
        received_token_id: if direction == "received" { Some(token_id.clone()) } else { None },
        paid_amount_str: if direction == "sent" { Some(amount.to_string()) } else { None },
        received_amount_str: if direction == "received" { Some(amount.to_string()) } else { None },
        base_amount_str: None,
        quote_amount_str: None,
        success: true,
    });
    rows.add_leg(TransferLegRow {
        id: format!("{action_id}:{direction}:{token_id}"),
        action_id,
        direction: direction.to_string(),
        source: source.to_string(),
        txid: txid.to_string(),
        height,
        timestamp,
        token_id,
        token_block: token.block,
        token_tx: token.tx,
        amount_str: amount.to_string(),
        counterparty_address,
        counterparty_pool_id: None,
        outpoint_id,
    });
}

#[allow(clippy::too_many_arguments)]
fn push_amm_event_and_action(
    rows: &mut GraphRows,
    kind: &str,
    trader_address: Option<&str>,
    txid: Txid,
    tx_index: u32,
    height: u32,
    timestamp: u64,
    block_hash: &str,
    seq: u32,
    pool: SchemaAlkaneId,
    defs: SchemaMarketDefs,
    base_delta: i128,
    quote_delta: i128,
) {
    let pool_id = alkane_id_string(&pool);
    let base_id = alkane_id_string(&defs.base_alkane_id);
    let quote_id = alkane_id_string(&defs.quote_alkane_id);
    let base_abs = abs_i128(base_delta);
    let quote_abs = abs_i128(quote_delta);
    let event_id = format!("{txid}:amm:{pool_id}:{seq}");

    let (side_token_id, paid_token_id, received_token_id, paid_amount, received_amount) = match kind
    {
        "SWAP_SELL" => (
            Some(base_id.clone()),
            Some(base_id.clone()),
            Some(quote_id.clone()),
            Some(base_abs),
            Some(quote_abs),
        ),
        "SWAP_BUY" => (
            Some(base_id.clone()),
            Some(quote_id.clone()),
            Some(base_id.clone()),
            Some(quote_abs),
            Some(base_abs),
        ),
        "LP_ADD" => (None, None, None, Some(base_abs.saturating_add(quote_abs)), None),
        "LP_REMOVE" => (None, None, None, None, Some(base_abs.saturating_add(quote_abs))),
        _ => (None, None, None, None, None),
    };

    rows.amm_events.entry(event_id.clone()).or_insert(AmmEventRow {
        id: event_id.clone(),
        kind: kind.to_string(),
        txid: txid.to_string(),
        height,
        tx_index,
        timestamp,
        pool_id: pool_id.clone(),
        base_id: base_id.clone(),
        quote_id: quote_id.clone(),
        base_delta_str: base_delta.to_string(),
        quote_delta_str: quote_delta.to_string(),
        trader_address: trader_address.map(|s| s.to_string()),
        side_token_id: side_token_id.clone(),
        paid_token_id: paid_token_id.clone(),
        received_token_id: received_token_id.clone(),
        paid_amount_str: paid_amount.map(|v| v.to_string()),
        received_amount_str: received_amount.map(|v| v.to_string()),
    });

    let Some(address) = trader_address.filter(|v| !v.is_empty()) else {
        return;
    };
    let action_id = format!("{event_id}:{address}");
    rows.add_action(AddressActionRow {
        id: action_id.clone(),
        address: address.to_string(),
        kind: kind.to_string(),
        source: "amm".to_string(),
        txid: txid.to_string(),
        height,
        tx_index,
        timestamp,
        block_hash: block_hash.to_string(),
        pool_id: Some(pool_id.clone()),
        side_token_id,
        paid_token_id,
        received_token_id,
        paid_amount_str: paid_amount.map(|v| v.to_string()),
        received_amount_str: received_amount.map(|v| v.to_string()),
        base_amount_str: Some(base_abs.to_string()),
        quote_amount_str: Some(quote_abs.to_string()),
        success: true,
    });
    rows.participations.insert(ParticipationRow {
        address: address.to_string(),
        txid: txid.to_string(),
        height,
        timestamp,
    });

    match kind {
        "SWAP_SELL" => {
            add_amm_leg(
                rows,
                &action_id,
                "sent",
                txid,
                height,
                timestamp,
                defs.base_alkane_id,
                base_abs,
                &pool_id,
            );
            add_amm_leg(
                rows,
                &action_id,
                "received",
                txid,
                height,
                timestamp,
                defs.quote_alkane_id,
                quote_abs,
                &pool_id,
            );
        }
        "SWAP_BUY" => {
            add_amm_leg(
                rows,
                &action_id,
                "sent",
                txid,
                height,
                timestamp,
                defs.quote_alkane_id,
                quote_abs,
                &pool_id,
            );
            add_amm_leg(
                rows,
                &action_id,
                "received",
                txid,
                height,
                timestamp,
                defs.base_alkane_id,
                base_abs,
                &pool_id,
            );
        }
        "LP_ADD" => {
            add_amm_leg(
                rows,
                &action_id,
                "sent",
                txid,
                height,
                timestamp,
                defs.base_alkane_id,
                base_abs,
                &pool_id,
            );
            add_amm_leg(
                rows,
                &action_id,
                "sent",
                txid,
                height,
                timestamp,
                defs.quote_alkane_id,
                quote_abs,
                &pool_id,
            );
        }
        "LP_REMOVE" => {
            add_amm_leg(
                rows,
                &action_id,
                "received",
                txid,
                height,
                timestamp,
                defs.base_alkane_id,
                base_abs,
                &pool_id,
            );
            add_amm_leg(
                rows,
                &action_id,
                "received",
                txid,
                height,
                timestamp,
                defs.quote_alkane_id,
                quote_abs,
                &pool_id,
            );
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn add_amm_leg(
    rows: &mut GraphRows,
    action_id: &str,
    direction: &str,
    txid: Txid,
    height: u32,
    timestamp: u64,
    token: SchemaAlkaneId,
    amount: u128,
    pool_id: &str,
) {
    if amount == 0 {
        return;
    }
    rows.add_alkane(token);
    let token_id = alkane_id_string(&token);
    rows.add_leg(TransferLegRow {
        id: format!("{action_id}:{direction}:{token_id}"),
        action_id: action_id.to_string(),
        direction: direction.to_string(),
        source: "amm".to_string(),
        txid: txid.to_string(),
        height,
        timestamp,
        token_id,
        token_block: token.block,
        token_tx: token.tx,
        amount_str: amount.to_string(),
        counterparty_address: None,
        counterparty_pool_id: Some(pool_id.to_string()),
        outpoint_id: None,
    });
}

fn classify_amm_kind(base_delta: i128, quote_delta: i128) -> Option<&'static str> {
    match (base_delta.signum(), quote_delta.signum()) {
        (1, -1) => Some("SWAP_SELL"),
        (-1, 1) => Some("SWAP_BUY"),
        (1, 1) => Some("LP_ADD"),
        (-1, -1) => Some("LP_REMOVE"),
        _ => None,
    }
}

fn abs_i128(value: i128) -> u128 {
    if value < 0 { (-value) as u128 } else { value as u128 }
}

fn alkane_id_string(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

fn script_to_address(spk: &ScriptBuf, network: Network) -> Option<String> {
    Address::from_script(spk.as_script(), network).ok().map(|addr| addr.to_string())
}

fn parse_trace_short_id(
    id: &crate::alkanes::trace::EspoSandshrewLikeTraceShortId,
) -> Option<SchemaAlkaneId> {
    fn parse_u32(s: &str) -> Option<u32> {
        if let Some(hex) = s.strip_prefix("0x") {
            u32::from_str_radix(hex, 16).ok()
        } else {
            s.parse::<u32>().ok()
        }
    }
    fn parse_u64(s: &str) -> Option<u64> {
        if let Some(hex) = s.strip_prefix("0x") {
            u64::from_str_radix(hex, 16).ok()
        } else {
            s.parse::<u64>().ok()
        }
    }
    Some(SchemaAlkaneId { block: parse_u32(&id.block)?, tx: parse_u64(&id.tx)? })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_amm_events_from_pool_deltas() {
        assert_eq!(classify_amm_kind(10, -9), Some("SWAP_SELL"));
        assert_eq!(classify_amm_kind(-10, 9), Some("SWAP_BUY"));
        assert_eq!(classify_amm_kind(10, 9), Some("LP_ADD"));
        assert_eq!(classify_amm_kind(-10, -9), Some("LP_REMOVE"));
        assert_eq!(classify_amm_kind(10, 0), None);
    }

    #[test]
    fn formats_alkane_ids_for_graph_keys() {
        assert_eq!(alkane_id_string(&SchemaAlkaneId { block: 2, tx: 0 }), "2:0");
    }

    #[test]
    #[ignore]
    fn dry_runs_rows_against_existing_espo_db() -> Result<()> {
        use crate::alkanes::trace::{EspoAlkanesTransaction, EspoBlock};
        use crate::runtime::tree_db::init_global_tree_db;
        use bitcoincore_rpc::{Auth, Client as CoreClient, RpcApi};
        use rocksdb::{BlockBasedOptions, Cache, DB, Options};
        use std::fs;
        use std::path::{Path, PathBuf};

        let config_path = std::env::var("ESPO_MEMGRAPH_DRY_RUN_CONFIG")
            .unwrap_or_else(|_| "/root/espo/config.json".to_string());
        let height = std::env::var("ESPO_MEMGRAPH_DRY_RUN_BLOCK")
            .unwrap_or_else(|_| "935453".to_string())
            .parse::<u32>()
            .context("ESPO_MEMGRAPH_DRY_RUN_BLOCK must be a u32")?;
        let config_text = fs::read_to_string(&config_path)
            .with_context(|| format!("read dry-run config {config_path}"))?;
        let config: Value = serde_json::from_str(&config_text)
            .with_context(|| format!("parse dry-run config {config_path}"))?;
        let config_dir = Path::new(&config_path).parent().unwrap_or_else(|| Path::new("."));

        let db_root = config
            .get("db_path")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./db"));
        let db_root = if db_root.is_absolute() { db_root } else { config_dir.join(db_root) };
        let espo_db_path = db_root.join("espo");

        let mut opts = Options::default();
        let cache = Cache::new_lru_cache(512 * 1024 * 1024);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&table);
        opts.set_max_open_files(1024);
        let db = Arc::new(DB::open_for_read_only(&opts, &espo_db_path, false)?);
        init_global_tree_db(Arc::clone(&db))?;

        let essentials = Arc::new(EssentialsProvider::new(Arc::new(Mdb::from_db(
            Arc::clone(&db),
            b"essentials:",
        ))));
        let amm = AmmDataProvider::new(
            Arc::new(Mdb::from_db(Arc::clone(&db), b"ammdata:")),
            Arc::clone(&essentials),
        );

        let rpc_url = config
            .get("bitcoind_rpc_url")
            .and_then(Value::as_str)
            .unwrap_or("http://127.0.0.1:8332");
        let rpc_user = config.get("bitcoind_rpc_user").and_then(Value::as_str).unwrap_or("");
        let rpc_pass = config.get("bitcoind_rpc_pass").and_then(Value::as_str).unwrap_or("");
        let auth = if rpc_user.is_empty() && rpc_pass.is_empty() {
            Auth::None
        } else {
            Auth::UserPass(rpc_user.to_string(), rpc_pass.to_string())
        };
        let core = CoreClient::new(rpc_url, auth)?;
        let block_hash = core.get_block_hash(height as u64)?;
        let raw_block = core.get_block(&block_hash)?;
        let tx_count = raw_block.txdata.len();
        let block = EspoBlock {
            is_latest: false,
            height,
            block_header: raw_block.header,
            host_function_values: (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            fee_summary: None,
            tx_count,
            transactions: raw_block
                .txdata
                .into_iter()
                .map(|transaction| EspoAlkanesTransaction { traces: None, transaction })
                .collect(),
        };

        let rows = build_graph_rows(&block, &essentials, &amm, Network::Bitcoin)?;
        let tx_rows = rows.txs.len();
        let participation_rows = rows.participations.len();
        let alkane_rows = rows.alkanes.len();
        let pool_rows = rows.pools.len();
        let outpoint_rows = rows.outpoints.len();
        let action_rows = rows.actions.len();
        let leg_rows = rows.legs.len();
        let amm_event_rows = rows.amm_events.len();
        eprintln!(
            "[MEMGRAPH dry-run] height={} txs={} addresses={} alkanes={} pools={} outpoints={} actions={} legs={} amm_events={}",
            height,
            tx_rows,
            participation_rows,
            alkane_rows,
            pool_rows,
            outpoint_rows,
            action_rows,
            leg_rows,
            amm_event_rows
        );

        if std::env::var("ESPO_MEMGRAPH_DRY_RUN_WRITE").ok().as_deref() == Some("1") {
            let memgraph_config_path = std::env::var("ESPO_MEMGRAPH_DRY_RUN_MEMGRAPH_CONFIG")
                .unwrap_or_else(|_| config_path.clone());
            let memgraph_config_text = fs::read_to_string(&memgraph_config_path)
                .with_context(|| format!("read Memgraph dry-run config {memgraph_config_path}"))?;
            let memgraph_config_json: Value = serde_json::from_str(&memgraph_config_text)
                .with_context(|| format!("parse Memgraph dry-run config {memgraph_config_path}"))?;
            let memgraph_config_value = memgraph_config_json
                .get("modules")
                .and_then(|modules| modules.get("memgraph"))
                .or_else(|| memgraph_config_json.get("memgraph"))
                .ok_or_else(|| anyhow!("dry-run Memgraph config is missing modules.memgraph"))?;
            let memgraph_config = MemgraphConfig::from_json(memgraph_config_value)?;

            let graph_dir = tempfile::tempdir()?;
            let mut graph_opts = Options::default();
            graph_opts.create_if_missing(true);
            let graph_db = Arc::new(DB::open(&graph_opts, graph_dir.path())?);
            let provider =
                MemgraphProvider::new(Arc::new(Mdb::from_db(graph_db, b"memgraph-test:")));
            provider.connect_memgraph(&memgraph_config)?;
            provider.ensure_constraints()?;
            provider.write_graph_rows(rows)?;
            eprintln!("[MEMGRAPH dry-run] wrote graph rows to {}", memgraph_config.uri);
        }

        assert_eq!(tx_rows, tx_count);
        Ok(())
    }
}
