use super::api::MemgraphHost;
use super::config::MemgraphConfig;
use super::consts::{MEMGRAPH_BATCH_SIZE, MEMGRAPH_INITIALIZE_CONSTRAINTS};
use crate::modules::ammdata::schemas::SchemaMarketDefs;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

const INDEX_HEIGHT_KEY: &[u8] = b"/index_height";

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TxRow {
    pub(crate) txid: String,
    pub(crate) height: u32,
    pub(crate) tx_index: u32,
    pub(crate) timestamp: u64,
    pub(crate) block_hash: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub(crate) struct ParticipationRow {
    pub(crate) address: String,
    pub(crate) txid: String,
    pub(crate) height: u32,
    pub(crate) timestamp: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AlkaneRow {
    pub(crate) id: String,
    pub(crate) block: u32,
    pub(crate) tx: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PoolRow {
    pub(crate) id: String,
    pub(crate) block: u32,
    pub(crate) tx: u64,
    pub(crate) base_id: String,
    pub(crate) base_block: u32,
    pub(crate) base_tx: u64,
    pub(crate) quote_id: String,
    pub(crate) quote_block: u32,
    pub(crate) quote_tx: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OutpointRow {
    pub(crate) id: String,
    pub(crate) txid: String,
    pub(crate) vout: u32,
    pub(crate) address: String,
    pub(crate) created_height: Option<u32>,
    pub(crate) seen_height: u32,
    pub(crate) spent_by_txid: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AddressActionRow {
    pub(crate) id: String,
    pub(crate) address: String,
    pub(crate) kind: String,
    pub(crate) source: String,
    pub(crate) txid: String,
    pub(crate) height: u32,
    pub(crate) tx_index: u32,
    pub(crate) timestamp: u64,
    pub(crate) block_hash: String,
    pub(crate) pool_id: Option<String>,
    pub(crate) side_token_id: Option<String>,
    pub(crate) paid_token_id: Option<String>,
    pub(crate) received_token_id: Option<String>,
    pub(crate) paid_amount_str: Option<String>,
    pub(crate) received_amount_str: Option<String>,
    pub(crate) base_amount_str: Option<String>,
    pub(crate) quote_amount_str: Option<String>,
    pub(crate) success: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TransferLegRow {
    pub(crate) id: String,
    pub(crate) action_id: String,
    pub(crate) direction: String,
    pub(crate) source: String,
    pub(crate) txid: String,
    pub(crate) height: u32,
    pub(crate) timestamp: u64,
    pub(crate) token_id: String,
    pub(crate) token_block: u32,
    pub(crate) token_tx: u64,
    pub(crate) amount_str: String,
    pub(crate) counterparty_address: Option<String>,
    pub(crate) counterparty_pool_id: Option<String>,
    pub(crate) outpoint_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AmmEventRow {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) txid: String,
    pub(crate) height: u32,
    pub(crate) tx_index: u32,
    pub(crate) timestamp: u64,
    pub(crate) pool_id: String,
    pub(crate) base_id: String,
    pub(crate) quote_id: String,
    pub(crate) base_delta_str: String,
    pub(crate) quote_delta_str: String,
    pub(crate) trader_address: Option<String>,
    pub(crate) side_token_id: Option<String>,
    pub(crate) paid_token_id: Option<String>,
    pub(crate) received_token_id: Option<String>,
    pub(crate) paid_amount_str: Option<String>,
    pub(crate) received_amount_str: Option<String>,
}

#[derive(Default)]
pub(crate) struct GraphRows {
    pub(crate) txs: Vec<TxRow>,
    pub(crate) participations: HashSet<ParticipationRow>,
    pub(crate) alkanes: HashMap<String, AlkaneRow>,
    pub(crate) pools: HashMap<String, PoolRow>,
    pub(crate) outpoints: HashMap<String, OutpointRow>,
    pub(crate) actions: HashMap<String, AddressActionRow>,
    pub(crate) legs: HashMap<String, TransferLegRow>,
    pub(crate) amm_events: HashMap<String, AmmEventRow>,
}

impl GraphRows {
    pub(crate) fn add_alkane(&mut self, id: SchemaAlkaneId) {
        let key = alkane_id_string(&id);
        self.alkanes.entry(key.clone()).or_insert(AlkaneRow {
            id: key,
            block: id.block,
            tx: id.tx,
        });
    }

    pub(crate) fn add_pool(&mut self, defs: SchemaMarketDefs) {
        self.add_alkane(defs.pool_alkane_id);
        self.add_alkane(defs.base_alkane_id);
        self.add_alkane(defs.quote_alkane_id);
        let pool_key = alkane_id_string(&defs.pool_alkane_id);
        self.pools.entry(pool_key.clone()).or_insert(PoolRow {
            id: pool_key,
            block: defs.pool_alkane_id.block,
            tx: defs.pool_alkane_id.tx,
            base_id: alkane_id_string(&defs.base_alkane_id),
            base_block: defs.base_alkane_id.block,
            base_tx: defs.base_alkane_id.tx,
            quote_id: alkane_id_string(&defs.quote_alkane_id),
            quote_block: defs.quote_alkane_id.block,
            quote_tx: defs.quote_alkane_id.tx,
        });
    }

    pub(crate) fn add_action(&mut self, row: AddressActionRow) {
        self.actions.entry(row.id.clone()).or_insert(row);
    }

    pub(crate) fn add_leg(&mut self, row: TransferLegRow) {
        self.legs.entry(row.id.clone()).or_insert(row);
    }
}

fn alkane_id_string(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

pub struct MemgraphProvider {
    mdb: Arc<Mdb>,
    memgraph: RwLock<Option<MemgraphHost>>,
    constraints_initialized: RwLock<bool>,
}

impl MemgraphProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        Self { mdb, memgraph: RwLock::new(None), constraints_initialized: RwLock::new(false) }
    }

    pub(crate) fn connect_memgraph(&self, config: &MemgraphConfig) -> Result<()> {
        let client = MemgraphHost::new(config)?;
        *self.memgraph.write().unwrap() = Some(client);
        *self.constraints_initialized.write().unwrap() = false;
        Ok(())
    }

    pub fn is_memgraph_connected(&self) -> bool {
        self.memgraph.read().unwrap().is_some()
    }

    fn memgraph_host(&self) -> Result<MemgraphHost> {
        self.memgraph
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("Memgraph Bolt client is not initialized"))
    }

    pub fn get_index_height(&self) -> Result<Option<u32>> {
        let Some(bytes) = self
            .mdb
            .get(INDEX_HEIGHT_KEY)
            .map_err(|e| anyhow!("memgraph mdb.get failed: {e}"))?
        else {
            return Ok(None);
        };
        if bytes.len() != 4 {
            return Err(anyhow!("[MEMGRAPH] invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(Some(u32::from_le_bytes(arr)))
    }

    pub fn set_index_height(&self, height: u32) -> Result<()> {
        self.mdb
            .put(INDEX_HEIGHT_KEY, &height.to_le_bytes())
            .map_err(|e| anyhow!("[MEMGRAPH] rocksdb put(/index_height) failed: {e}"))
    }

    pub(crate) fn ensure_constraints(&self) -> Result<()> {
        if !MEMGRAPH_INITIALIZE_CONSTRAINTS {
            return Ok(());
        }
        if *self.constraints_initialized.read().unwrap() {
            return Ok(());
        }

        let statements = vec![
            "CREATE INDEX ON :Block(height)",
            "CREATE INDEX ON :Tx(txid)",
            "CREATE INDEX ON :Address(address)",
            "CREATE INDEX ON :Alkane(id)",
            "CREATE INDEX ON :Outpoint(id)",
            "CREATE INDEX ON :AddressAction(id)",
            "CREATE INDEX ON :TransferLeg(id)",
            "CREATE INDEX ON :AmmEvent(id)",
        ];

        let client = self.memgraph_host()?;
        for statement in statements {
            if let Err(e) = client.execute_many_without_params(vec![statement]) {
                if !is_existing_schema_error(&e) {
                    return Err(e);
                }
            }
        }
        *self.constraints_initialized.write().unwrap() = true;
        Ok(())
    }

    pub(crate) fn write_graph_rows(&self, rows: GraphRows) -> Result<()> {
        self.write_batches(WRITE_TXS, rows.txs)?;
        self.write_batches(WRITE_PARTICIPATIONS, rows.participations.into_iter().collect())?;
        self.write_batches(WRITE_ALKANES, rows.alkanes.into_values().collect())?;
        self.write_batches(WRITE_POOLS, rows.pools.into_values().collect())?;
        self.write_batches(WRITE_OUTPOINTS, rows.outpoints.into_values().collect())?;
        self.write_batches(WRITE_ACTIONS, rows.actions.into_values().collect())?;

        let mut sent = Vec::new();
        let mut received = Vec::new();
        for leg in rows.legs.into_values() {
            if leg.direction == "sent" {
                sent.push(leg);
            } else {
                received.push(leg);
            }
        }
        self.write_batches(WRITE_SENT_LEGS, sent.clone())?;
        self.write_batches(WRITE_RECEIVED_LEGS, received.clone())?;
        self.write_batches(
            WRITE_LEG_ADDRESS_COUNTERPARTIES,
            sent.iter().chain(received.iter()).collect::<Vec<&TransferLegRow>>(),
        )?;
        self.write_batches(
            WRITE_LEG_POOL_COUNTERPARTIES,
            sent.iter().chain(received.iter()).collect::<Vec<&TransferLegRow>>(),
        )?;
        self.write_batches(WRITE_AMM_EVENTS, rows.amm_events.into_values().collect())?;
        Ok(())
    }

    fn write_batches<T: Serialize>(&self, statement: &'static str, rows: Vec<T>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let client = self.memgraph_host()?;
        for chunk in rows.chunks(MEMGRAPH_BATCH_SIZE) {
            client
                .execute(statement, json!({ "rows": chunk }))
                .with_context(|| format!("Memgraph write failed for {} rows", chunk.len()))?;
        }
        Ok(())
    }

    pub(crate) fn delete_from_height(&self, next_height: u32) -> Result<()> {
        self.memgraph_host()?
            .execute(DELETE_FROM_HEIGHT, json!({ "height": next_height }))
    }
}

fn is_existing_schema_error(error: &anyhow::Error) -> bool {
    let msg = format!("{error:?}").to_ascii_lowercase();
    msg.contains("already exists") || msg.contains("index exists") || msg.contains("exists already")
}

const WRITE_TXS: &str = r#"
UNWIND $rows AS row
MERGE (b:Block {height: row.height})
SET b.hash = row.block_hash,
    b.timestamp = row.timestamp
MERGE (tx:Tx {txid: row.txid})
SET tx.height = row.height,
    tx.tx_index = row.tx_index,
    tx.timestamp = row.timestamp,
    tx.block_hash = row.block_hash
MERGE (tx)-[:IN_BLOCK]->(b)
"#;

const WRITE_PARTICIPATIONS: &str = r#"
UNWIND $rows AS row
MERGE (addr:Address {address: row.address})
ON CREATE SET addr.first_seen_height = row.height
MERGE (tx:Tx {txid: row.txid})
MERGE (addr)-[:PARTICIPATED_IN]->(tx)
"#;

const WRITE_ALKANES: &str = r#"
UNWIND $rows AS row
MERGE (alk:Alkane {id: row.id})
SET alk.block = row.block,
    alk.tx = row.tx
"#;

const WRITE_POOLS: &str = r#"
UNWIND $rows AS row
MERGE (pool:Alkane {id: row.id})
SET pool:Pool,
    pool.block = row.block,
    pool.tx = row.tx,
    pool.base_id = row.base_id,
    pool.quote_id = row.quote_id
MERGE (base:Alkane {id: row.base_id})
SET base.block = row.base_block,
    base.tx = row.base_tx
MERGE (quote:Alkane {id: row.quote_id})
SET quote.block = row.quote_block,
    quote.tx = row.quote_tx
MERGE (pool)-[:BASE_ASSET]->(base)
MERGE (pool)-[:QUOTE_ASSET]->(quote)
"#;

const WRITE_OUTPOINTS: &str = r#"
UNWIND $rows AS row
MERGE (op:Outpoint {id: row.id})
SET op.txid = row.txid,
    op.vout = row.vout,
    op.address = row.address,
    op.height = row.seen_height,
    op.created_height = row.created_height,
    op.seen_height = row.seen_height,
    op.spent_by_txid = row.spent_by_txid
MERGE (addr:Address {address: row.address})
ON CREATE SET addr.first_seen_height = row.seen_height
MERGE (op)-[:LOCKED_TO]->(addr)
"#;

const WRITE_ACTIONS: &str = r#"
UNWIND $rows AS row
MERGE (addr:Address {address: row.address})
ON CREATE SET addr.first_seen_height = row.height
MERGE (tx:Tx {txid: row.txid})
MERGE (act:AddressAction {id: row.id})
SET act.kind = row.kind,
    act.source = row.source,
    act.txid = row.txid,
    act.height = row.height,
    act.tx_index = row.tx_index,
    act.timestamp = row.timestamp,
    act.block_hash = row.block_hash,
    act.pool_id = row.pool_id,
    act.side_token_id = row.side_token_id,
    act.paid_token_id = row.paid_token_id,
    act.received_token_id = row.received_token_id,
    act.paid_amount_str = row.paid_amount_str,
    act.received_amount_str = row.received_amount_str,
    act.base_amount_str = row.base_amount_str,
    act.quote_amount_str = row.quote_amount_str,
    act.success = row.success
MERGE (addr)-[:DID]->(act)
MERGE (act)-[:IN_TX]->(tx)
MERGE (addr)-[:PARTICIPATED_IN]->(tx)
"#;

const WRITE_SENT_LEGS: &str = r#"
UNWIND $rows AS row
MATCH (act:AddressAction {id: row.action_id})
MERGE (leg:TransferLeg {id: row.id})
SET leg.direction = row.direction,
    leg.source = row.source,
    leg.txid = row.txid,
    leg.height = row.height,
    leg.timestamp = row.timestamp,
    leg.token_id = row.token_id,
    leg.amount_str = row.amount_str,
    leg.outpoint_id = row.outpoint_id
MERGE (token:Alkane {id: row.token_id})
SET token.block = row.token_block,
    token.tx = row.token_tx
MERGE (act)-[:SENT]->(leg)
MERGE (leg)-[:ASSET]->(token)
"#;

const WRITE_RECEIVED_LEGS: &str = r#"
UNWIND $rows AS row
MATCH (act:AddressAction {id: row.action_id})
MERGE (leg:TransferLeg {id: row.id})
SET leg.direction = row.direction,
    leg.source = row.source,
    leg.txid = row.txid,
    leg.height = row.height,
    leg.timestamp = row.timestamp,
    leg.token_id = row.token_id,
    leg.amount_str = row.amount_str,
    leg.outpoint_id = row.outpoint_id
MERGE (token:Alkane {id: row.token_id})
SET token.block = row.token_block,
    token.tx = row.token_tx
MERGE (act)-[:RECEIVED]->(leg)
MERGE (leg)-[:ASSET]->(token)
"#;

const WRITE_LEG_ADDRESS_COUNTERPARTIES: &str = r#"
UNWIND $rows AS row
WITH row WHERE row.counterparty_address IS NOT NULL
MATCH (leg:TransferLeg {id: row.id})
MERGE (cp:Address {address: row.counterparty_address})
ON CREATE SET cp.first_seen_height = row.height
MERGE (leg)-[:COUNTERPARTY]->(cp)
"#;

const WRITE_LEG_POOL_COUNTERPARTIES: &str = r#"
UNWIND $rows AS row
WITH row WHERE row.counterparty_pool_id IS NOT NULL
MATCH (leg:TransferLeg {id: row.id})
MERGE (pool:Alkane {id: row.counterparty_pool_id})
SET pool:Pool
MERGE (leg)-[:COUNTERPARTY]->(pool)
"#;

const WRITE_AMM_EVENTS: &str = r#"
UNWIND $rows AS row
MERGE (event:AmmEvent {id: row.id})
SET event.kind = row.kind,
    event.txid = row.txid,
    event.height = row.height,
    event.tx_index = row.tx_index,
    event.timestamp = row.timestamp,
    event.pool_id = row.pool_id,
    event.base_id = row.base_id,
    event.quote_id = row.quote_id,
    event.base_delta_str = row.base_delta_str,
    event.quote_delta_str = row.quote_delta_str,
    event.trader_address = row.trader_address,
    event.side_token_id = row.side_token_id,
    event.paid_token_id = row.paid_token_id,
    event.received_token_id = row.received_token_id,
    event.paid_amount_str = row.paid_amount_str,
    event.received_amount_str = row.received_amount_str
MERGE (tx:Tx {txid: row.txid})
MERGE (pool:Alkane {id: row.pool_id})
SET pool:Pool
MERGE (base:Alkane {id: row.base_id})
MERGE (quote:Alkane {id: row.quote_id})
MERGE (event)-[:IN_TX]->(tx)
MERGE (event)-[:POOL]->(pool)
MERGE (event)-[:BASE]->(base)
MERGE (event)-[:QUOTE]->(quote)
WITH row, event
WHERE row.trader_address IS NOT NULL
MERGE (addr:Address {address: row.trader_address})
ON CREATE SET addr.first_seen_height = row.height
MERGE (addr)-[:TRIGGERED]->(event)
WITH row, event
MATCH (act:AddressAction {id: row.id + ':' + row.trader_address})
MERGE (act)-[:AMM_EVENT]->(event)
"#;

const DELETE_FROM_HEIGHT: &str = r#"
MATCH (n)
WHERE (n:Block OR n:Tx OR n:Outpoint OR n:AddressAction OR n:TransferLeg OR n:AmmEvent)
  AND n.height >= $height
DETACH DELETE n
"#;
