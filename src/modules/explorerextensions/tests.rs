//! Hermetic unit tests for the explorerextensions indexes.
//!
//! These build an `EspoBlock` with hand-crafted alkanes traces (no
//! metashrew / network), run the module's `index_block`, then read the
//! reverse indexes back through a provider over the same temp RocksDB.

use super::main::ExplorerExtensions;
use super::storage::ExplorerExtProvider;
use crate::alkanes::trace::{
    EspoAlkanesTransaction, EspoBlock, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceInvokeContext, EspoSandshrewLikeTraceInvokeData,
    EspoSandshrewLikeTraceReturnData, EspoSandshrewLikeTraceReturnResponse,
    EspoSandshrewLikeTraceShortId, EspoSandshrewLikeTraceStatus, EspoTrace,
};
use crate::modules::defs::EspoModule;
use crate::runtime::mdb::Mdb;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use alkanes_cli_common::alkanes_pb::AlkanesTrace;
use bitcoin::absolute::LockTime;
use bitcoin::block::Header;
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version;
use bitcoin::Transaction;
use rocksdb::{DB, Options};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

// Real 80-byte mainnet header (block 909402) — index_block never reads it,
// but EspoBlock requires a valid Header value.
const HEADER_HEX: &str = "0060d52d0116257ed16bdf8429aea185252dee053b8c5e1b6137000000000000000000005c745239a3cdd49796f824a9e02059e482aac6916881351fabd1d957377cf6cde4699868b32c02170d1142fa";

fn header() -> Header {
    deserialize(&hex::decode(HEADER_HEX).unwrap()).unwrap()
}

fn make_tx(lock: u32) -> Transaction {
    // Distinct lock_time → distinct txid; inputs/outputs are irrelevant
    // to the trace-derived indexes.
    Transaction {
        version: Version(2),
        lock_time: LockTime::from_consensus(lock),
        input: vec![],
        output: vec![],
    }
}

fn short(block: &str, tx: &str) -> EspoSandshrewLikeTraceShortId {
    EspoSandshrewLikeTraceShortId { block: block.to_string(), tx: tx.to_string() }
}

fn invoke(
    typ: &str,
    myself: EspoSandshrewLikeTraceShortId,
    caller: EspoSandshrewLikeTraceShortId,
    inputs: Vec<&str>,
) -> EspoSandshrewLikeTraceEvent {
    EspoSandshrewLikeTraceEvent::Invoke(EspoSandshrewLikeTraceInvokeData {
        typ: typ.to_string(),
        context: EspoSandshrewLikeTraceInvokeContext {
            myself,
            caller,
            inputs: inputs.into_iter().map(|s| s.to_string()).collect(),
            incoming_alkanes: vec![],
            vout: 0,
        },
        fuel: 0,
    })
}

fn ret(success: bool) -> EspoSandshrewLikeTraceEvent {
    EspoSandshrewLikeTraceEvent::Return(EspoSandshrewLikeTraceReturnData {
        status: if success {
            EspoSandshrewLikeTraceStatus::Success
        } else {
            EspoSandshrewLikeTraceStatus::Failure
        },
        response: EspoSandshrewLikeTraceReturnResponse {
            alkanes: vec![],
            data: "0x".to_string(),
            storage: vec![],
        },
    })
}

fn trace_for(tx: &Transaction, events: Vec<EspoSandshrewLikeTraceEvent>) -> EspoTrace {
    let txid = tx.compute_txid();
    EspoTrace {
        sandshrew_trace: EspoSandshrewLikeTrace { outpoint: format!("{txid}:0"), events },
        protobuf_trace: AlkanesTrace::default(),
        storage_changes: HashMap::new(),
        outpoint: EspoOutpoint { txid: txid.as_byte_array().to_vec(), vout: 0, tx_spent: None },
    }
}

fn open_module() -> (ExplorerExtensions, Arc<Mdb>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, tmp.path()).unwrap());
    let mdb = Arc::new(Mdb::from_db(db, b"explorerextensions:"));
    let mut module = ExplorerExtensions::new();
    module.set_mdb(mdb.clone());
    (module, mdb, tmp)
}

#[test]
fn top_level_and_internal_indexes_are_separated() {
    let (module, mdb, _tmp) = open_module();

    // tx A: 2:0 (top-level call) → delegatecalls 4:70002 (internal).
    let tx_a = make_tx(1);
    let trace_a = trace_for(
        &tx_a,
        vec![
            invoke("call", short("2", "0"), short("0", "0"), vec!["0x1"]),
            invoke("delegatecall", short("4", "70002"), short("2", "0"), vec![]),
            ret(true),
            ret(true),
        ],
    );
    // tx B: 4:70002 is itself the top-level target, and it reverts.
    let tx_b = make_tx(2);
    let trace_b =
        trace_for(&tx_b, vec![invoke("call", short("4", "70002"), short("0", "0"), vec![]), ret(false)]);

    let txid_a = tx_a.compute_txid().to_string();
    let txid_b = tx_b.compute_txid().to_string();

    let block = EspoBlock {
        is_latest: true,
        height: 880_001,
        block_header: header(),
        host_function_values: (vec![], vec![], vec![], vec![]),
        tx_count: 2,
        transactions: vec![
            EspoAlkanesTransaction { traces: Some(vec![trace_a]), transaction: tx_a },
            EspoAlkanesTransaction { traces: Some(vec![trace_b]), transaction: tx_b },
        ],
    };

    module.index_block(block).unwrap();
    assert_eq!(module.get_index_height(), Some(880_001));

    let provider = ExplorerExtProvider::new(mdb);
    let diesel = SchemaAlkaneId { block: 2, tx: 0 };
    let lp = SchemaAlkaneId { block: 4, tx: 70002 };

    // 2:0 is a top-level target in tx A only.
    let (total, txs) = provider.txs_by_alkane(&diesel, 1, 50).unwrap();
    assert_eq!(total, 1);
    assert_eq!(txs[0]["txid"], txid_a);
    assert_eq!(txs[0]["status"], "success");
    assert_eq!(txs[0]["opcode"], "1");
    // …and never reached internally.
    assert_eq!(provider.internal_txs_by_alkane(&diesel, 1, 50).unwrap().0, 0);

    // 4:70002 is a top-level target in tx B (reverting) …
    let (total_b, txs_b) = provider.txs_by_alkane(&lp, 1, 50).unwrap();
    assert_eq!(total_b, 1);
    assert_eq!(txs_b[0]["txid"], txid_b);
    assert_eq!(txs_b[0]["status"], "failure");

    // … and is reached internally (delegatecall from 2:0) in tx A.
    let (total_i, txs_i) = provider.internal_txs_by_alkane(&lp, 1, 50).unwrap();
    assert_eq!(total_i, 1);
    assert_eq!(txs_i[0]["txid"], txid_a);
    let touches = txs_i[0]["touches"].as_array().unwrap();
    assert_eq!(touches.len(), 1);
    assert_eq!(touches[0]["call_type"], "delegatecall");
    assert_eq!(touches[0]["caller"], "2:0");

    // An unrelated alkane has no rows in either index.
    let other = SchemaAlkaneId { block: 3, tx: 3 };
    assert_eq!(provider.txs_by_alkane(&other, 1, 50).unwrap().0, 0);
    assert_eq!(provider.internal_txs_by_alkane(&other, 1, 50).unwrap().0, 0);
}

#[test]
fn reindexing_same_block_is_idempotent() {
    let (module, mdb, _tmp) = open_module();
    let tx = make_tx(7);
    let trace = trace_for(&tx, vec![invoke("call", short("2", "0"), short("0", "0"), vec![]), ret(true)]);
    let mk_block = |tx: Transaction, trace: EspoTrace| EspoBlock {
        is_latest: true,
        height: 880_010,
        block_header: header(),
        host_function_values: (vec![], vec![], vec![], vec![]),
        tx_count: 1,
        transactions: vec![EspoAlkanesTransaction { traces: Some(vec![trace]), transaction: tx }],
    };

    module.index_block(mk_block(tx.clone(), trace.clone())).unwrap();
    // Re-indexing the same height is a no-op (height gate) — no duplicate rows.
    module.index_block(mk_block(tx, trace)).unwrap();

    let provider = ExplorerExtProvider::new(mdb);
    let (total, _) = provider.txs_by_alkane(&SchemaAlkaneId { block: 2, tx: 0 }, 1, 50).unwrap();
    assert_eq!(total, 1);
}
