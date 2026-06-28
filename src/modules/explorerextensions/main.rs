//! `explorerextensions` module.
//!
//! Maintains two trace-derived reverse indexes per alkane id so the
//! explorer / wallets can render an etherscan-style account view:
//!   * `explorerextensions.txs_by_alkane` — txs whose top-level cellpack
//!     target is the alkane (the EOA-level "to").
//!   * `explorerextensions.internal_txs_by_alkane` — txs that reach the
//!     alkane through an internal call (call/delegatecall/staticcall) at
//!     any depth > 0.
//!
//! Both are derived from the alkanes execution trace already attached to
//! every `EspoBlock` transaction, so no extra metashrew calls are needed
//! during indexing.

use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId,
    EspoSandshrewLikeTraceStatus,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::explorerextensions::consts::explorerextensions_genesis_block;
use crate::modules::explorerextensions::rpc;
use crate::modules::explorerextensions::storage::{
    ExplorerExtProvider, InternalTouch, TopLevelRow, call_type_code,
};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use bitcoin::hashes::Hash;
use std::collections::HashMap;
use std::sync::Arc;

/// Parse a sandshrew-style short id, whose `block`/`tx` fields are either
/// hex (`0x…`) or decimal. Oversized ids clamp to the schema max, matching
/// `AlkaneId -> SchemaAlkaneId` in schemas.rs.
fn parse_short_id(id: &EspoSandshrewLikeTraceShortId) -> Option<SchemaAlkaneId> {
    let block = parse_u128_str(&id.block)?;
    let tx = parse_u128_str(&id.tx)?;
    Some(SchemaAlkaneId {
        block: u32::try_from(block).unwrap_or(u32::MAX),
        tx: u64::try_from(tx).unwrap_or(u64::MAX),
    })
}

fn parse_u128_str(s: &str) -> Option<u128> {
    let s = s.trim();
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        if hex.is_empty() {
            return Some(0);
        }
        return u128::from_str_radix(hex, 16).ok();
    }
    s.parse::<u128>().ok()
}

pub struct ExplorerExtensions {
    provider: Option<Arc<ExplorerExtProvider>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl ExplorerExtensions {
    pub fn new() -> Self {
        Self { provider: None, index_height: Arc::new(std::sync::RwLock::new(None)) }
    }

    #[inline]
    fn provider(&self) -> &ExplorerExtProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }
}

impl Default for ExplorerExtensions {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for ExplorerExtensions {
    fn get_name(&self) -> &'static str {
        "explorerextensions"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let provider = Arc::new(ExplorerExtProvider::new(mdb));
        match provider.get_index_height() {
            Ok(h) => {
                *self.index_height.write().unwrap() = h;
                eprintln!("[EXPLOREREXT] loaded index height: {:?}", h);
            }
            Err(e) => eprintln!("[EXPLOREREXT] failed to load /index_height: {e:?}"),
        }
        self.provider = Some(provider);
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        explorerextensions_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let height = block.height;
        if let Some(prev) = *self.index_height.read().unwrap() {
            if height <= prev {
                return Ok(());
            }
        }

        // Coalesce per (alkane, txid) within the block. A tx may target an
        // alkane top-level once (last write wins) and touch others
        // internally any number of times (accumulated).
        let mut toplevel: HashMap<(SchemaAlkaneId, [u8; 32]), TopLevelRow> = HashMap::new();
        let mut internal: HashMap<(SchemaAlkaneId, [u8; 32]), Vec<InternalTouch>> = HashMap::new();

        for tx in block.transactions.iter() {
            let Some(traces) = tx.traces.as_ref() else { continue };
            let txid_bytes = tx.transaction.compute_txid().to_byte_array();

            for trace in traces.iter() {
                // Walk the flat event stream with a depth stack: each
                // Invoke pushes the entered alkane, each Return pops. The
                // first (depth-0) Invoke of a trace is the cellpack target;
                // anything entered at depth > 0 is an internal call.
                let mut stack: Vec<SchemaAlkaneId> = Vec::with_capacity(8);
                for ev in trace.sandshrew_trace.events.iter() {
                    match ev {
                        EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                            let Some(alk) = parse_short_id(&inv.context.myself) else {
                                // Still need to keep the stack balanced.
                                stack.push(SchemaAlkaneId { block: u32::MAX, tx: u64::MAX });
                                continue;
                            };
                            let depth = stack.len();
                            if depth == 0 {
                                let opcode =
                                    inv.context.inputs.first().and_then(|s| parse_u128_str(s));
                                toplevel.insert(
                                    (alk, txid_bytes),
                                    TopLevelRow { vout: inv.context.vout, status: 0, opcode },
                                );
                            } else {
                                let caller = parse_short_id(&inv.context.caller)
                                    .unwrap_or(SchemaAlkaneId { block: 0, tx: 0 });
                                internal.entry((alk, txid_bytes)).or_default().push(
                                    InternalTouch {
                                        call_type: call_type_code(&inv.typ),
                                        caller_block: caller.block,
                                        caller_tx: caller.tx,
                                        vout: inv.context.vout,
                                    },
                                );
                            }
                            stack.push(alk);
                        }
                        EspoSandshrewLikeTraceEvent::Return(ret) => {
                            // A return that closes the outermost frame
                            // records the top-level exit status.
                            if stack.len() == 1 {
                                let alk0 = stack[0];
                                if let Some(row) = toplevel.get_mut(&(alk0, txid_bytes)) {
                                    row.status =
                                        if ret.status == EspoSandshrewLikeTraceStatus::Failure {
                                            1
                                        } else {
                                            0
                                        };
                                }
                            }
                            stack.pop();
                        }
                        EspoSandshrewLikeTraceEvent::Create(_) => {}
                    }
                }
            }
        }

        let toplevel_vec: Vec<(SchemaAlkaneId, [u8; 32], TopLevelRow)> =
            toplevel.into_iter().map(|((alk, txid), row)| (alk, txid, row)).collect();
        let internal_vec: Vec<(SchemaAlkaneId, [u8; 32], Vec<InternalTouch>)> =
            internal.into_iter().map(|((alk, txid), touches)| (alk, txid, touches)).collect();

        let toplevel_n = toplevel_vec.len();
        let internal_n = internal_vec.len();
        self.provider().write_block(height, &toplevel_vec, &internal_vec)?;
        *self.index_height.write().unwrap() = Some(height);

        if toplevel_n > 0 || internal_n > 0 {
            eprintln!(
                "[EXPLOREREXT] block #{height} indexed {toplevel_n} top-level + {internal_n} internal alkane-tx rows"
            );
        }
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        rpc::register_rpc(
            reg.clone(),
            self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").clone(),
        );
    }
}
