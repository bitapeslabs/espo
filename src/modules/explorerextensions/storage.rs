//! Storage for the `explorerextensions` module.
//!
//! Two RocksDB reverse indexes, both keyed by alkane id, that power an
//! etherscan-style account view for an alkane:
//!
//!   * **top-level** (`T`): transactions whose outermost cellpack target
//!     (depth 0 in the alkanes trace, i.e. the "to" of the EOA-level call)
//!     is the alkane. This answers `txs_by_alkane`.
//!   * **internal** (`I`): transactions that reach the alkane via an
//!     internal call (`call` / `delegatecall` / `staticcall`) at any depth
//!     greater than 0 — the alkanes analog of etherscan "internal txns".
//!     This answers `internal_txs_by_alkane`.
//!
//! Key layout (relative to the module's `explorerextensions:` namespace):
//!   `<tag:1> <block:4 BE> <tx:8 BE> <height:4 BE> <txid:32>`
//! so a prefix scan over `<tag><block><tx>` yields every tx for the alkane
//! in ascending (height, txid) order. Values are borsh-encoded metadata.

use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Txid;
use bitcoin::hashes::Hash;
use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::{Value, json};
use std::sync::Arc;

const TAG_TOPLEVEL: u8 = b'T';
const TAG_INTERNAL: u8 = b'I';
const KEY_INDEX_HEIGHT: &[u8] = b"/index_height";

const DEFAULT_LIMIT: u64 = 50;
const MAX_LIMIT: u64 = 500;

/// Per-(alkane, tx) record for a transaction whose top-level cellpack
/// target is the alkane. `status` is the outermost frame's exit status
/// (0 = success, 1 = failure); `opcode` is the first cellpack input when
/// present (the method opcode), kept as u128 for fidelity.
#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct TopLevelRow {
    pub vout: u32,
    pub status: u8,
    pub opcode: Option<u128>,
}

/// One internal call frame that entered the alkane within a transaction.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct InternalTouch {
    pub call_type: u8, // 0 call, 1 delegatecall, 2 staticcall, 255 unknown
    pub caller_block: u32,
    pub caller_tx: u64,
    pub vout: u32,
}

pub fn call_type_code(typ: &str) -> u8 {
    match typ {
        "call" => 0,
        "delegatecall" => 1,
        "staticcall" => 2,
        _ => 255,
    }
}

fn call_type_label(code: u8) -> &'static str {
    match code {
        0 => "call",
        1 => "delegatecall",
        2 => "staticcall",
        _ => "unknown",
    }
}

fn status_label(code: u8) -> &'static str {
    match code {
        0 => "success",
        _ => "failure",
    }
}

#[inline]
fn alkane_key_bytes(alk: &SchemaAlkaneId) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[..4].copy_from_slice(&alk.block.to_be_bytes());
    b[4..].copy_from_slice(&alk.tx.to_be_bytes());
    b
}

#[inline]
fn row_prefix(tag: u8, alk: &SchemaAlkaneId) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 12);
    k.push(tag);
    k.extend_from_slice(&alkane_key_bytes(alk));
    k
}

#[inline]
fn row_key(tag: u8, alk: &SchemaAlkaneId, height: u32, txid: &[u8; 32]) -> Vec<u8> {
    let mut k = row_prefix(tag, alk);
    k.extend_from_slice(&height.to_be_bytes());
    k.extend_from_slice(txid);
    k
}

/// Parse the trailing `<height:4 BE><txid:32>` from a relative key whose
/// layout is `<tag:1><alk:12><height:4><txid:32>` (49 bytes).
fn parse_height_txid(key: &[u8]) -> Option<(u32, [u8; 32])> {
    if key.len() != 1 + 12 + 4 + 32 {
        return None;
    }
    let height = u32::from_be_bytes(key[13..17].try_into().ok()?);
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&key[17..49]);
    Some((height, txid))
}

fn txid_to_string(bytes: &[u8; 32]) -> String {
    Txid::from_byte_array(*bytes).to_string()
}

#[derive(Clone)]
pub struct ExplorerExtProvider {
    mdb: Arc<Mdb>,
}

impl ExplorerExtProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        Self { mdb }
    }

    pub fn get_index_height(&self) -> Result<Option<u32>> {
        match self.mdb.get(KEY_INDEX_HEIGHT)? {
            Some(v) if v.len() >= 4 => {
                let arr: [u8; 4] = v[..4].try_into().expect("checked len");
                Ok(Some(u32::from_le_bytes(arr)))
            }
            _ => Ok(None),
        }
    }

    /// Persist this block's reverse-index rows + the new index height in a
    /// single batch. `toplevel` / `internal` are keyed by (alkane, txid).
    pub fn write_block(
        &self,
        height: u32,
        toplevel: &[(SchemaAlkaneId, [u8; 32], TopLevelRow)],
        internal: &[(SchemaAlkaneId, [u8; 32], Vec<InternalTouch>)],
    ) -> Result<()> {
        self.mdb.bulk_write(|b| {
            for (alk, txid, row) in toplevel {
                let v = borsh::to_vec(row).unwrap_or_default();
                b.put(&row_key(TAG_TOPLEVEL, alk, height, txid), &v);
            }
            for (alk, txid, touches) in internal {
                let v = borsh::to_vec(touches).unwrap_or_default();
                b.put(&row_key(TAG_INTERNAL, alk, height, txid), &v);
            }
            b.put(KEY_INDEX_HEIGHT, &height.to_le_bytes());
        })?;
        Ok(())
    }

    /// Transactions whose top-level cellpack target is `alk`, newest first.
    /// Returns `(total, page_items_as_json)`.
    pub fn txs_by_alkane(
        &self,
        alk: &SchemaAlkaneId,
        page: u64,
        limit: u64,
    ) -> Result<(usize, Vec<Value>)> {
        let entries = self.mdb.scan_prefix_entries(&row_prefix(TAG_TOPLEVEL, alk))?;
        let mut rows: Vec<(u32, [u8; 32], TopLevelRow)> = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            let Some((height, txid)) = parse_height_txid(&k) else { continue };
            let row = TopLevelRow::try_from_slice(&v).unwrap_or_default();
            rows.push((height, txid, row));
        }
        // Newest first; tie-break on txid bytes for determinism.
        rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let total = rows.len();
        let (start, end) = page_bounds(page, limit, total);
        let items = rows[start..end]
            .iter()
            .map(|(height, txid, row)| {
                json!({
                    "txid": txid_to_string(txid),
                    "height": height,
                    "vout": row.vout,
                    "status": status_label(row.status),
                    "opcode": row.opcode.map(|o| o.to_string()),
                })
            })
            .collect();
        Ok((total, items))
    }

    /// Transactions that reach `alk` via an internal call at any depth,
    /// newest first. Each item carries the internal call frames (touches).
    pub fn internal_txs_by_alkane(
        &self,
        alk: &SchemaAlkaneId,
        page: u64,
        limit: u64,
    ) -> Result<(usize, Vec<Value>)> {
        let entries = self.mdb.scan_prefix_entries(&row_prefix(TAG_INTERNAL, alk))?;
        let mut rows: Vec<(u32, [u8; 32], Vec<InternalTouch>)> = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            let Some((height, txid)) = parse_height_txid(&k) else { continue };
            let touches = Vec::<InternalTouch>::try_from_slice(&v).unwrap_or_default();
            rows.push((height, txid, touches));
        }
        rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let total = rows.len();
        let (start, end) = page_bounds(page, limit, total);
        let items = rows[start..end]
            .iter()
            .map(|(height, txid, touches)| {
                let touches_json: Vec<Value> = touches
                    .iter()
                    .map(|t| {
                        json!({
                            "call_type": call_type_label(t.call_type),
                            "caller": format!("{}:{}", t.caller_block, t.caller_tx),
                            "vout": t.vout,
                        })
                    })
                    .collect();
                json!({
                    "txid": txid_to_string(txid),
                    "height": height,
                    "touches": touches_json,
                })
            })
            .collect();
        Ok((total, items))
    }
}

/// Clamp page/limit and compute the slice bounds for `total` items.
pub fn page_bounds(page: u64, limit: u64, total: usize) -> (usize, usize) {
    let limit = limit.clamp(1, MAX_LIMIT) as usize;
    let page = page.max(1) as usize;
    let start = limit.saturating_mul(page - 1).min(total);
    let end = (start + limit).min(total);
    (start, end)
}

/// Normalize an optional `limit` RPC param.
pub fn normalize_limit(limit: Option<u64>) -> u64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Parse a `"block:tx"` decimal alkane id from an RPC param.
pub fn parse_alkane_id(s: &str) -> Option<SchemaAlkaneId> {
    let s = s.trim();
    let (b, t) = s.split_once(':')?;
    let block = b.trim().parse::<u32>().ok()?;
    let tx = t.trim().parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}
