use crate::schemas::SchemaAlkaneId;
use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use bitcoin::{
    Address, Network, Script, Transaction, TxIn, TxOut, Txid, blockdata::script::Instruction,
    hashes::Hash,
};
use borsh::{BorshDeserialize, to_vec};

use crate::modules::ammdata::schemas::SchemaTradeV1;
use crate::modules::ammdata::utils::candles::PriceSide;
use crate::runtime::mdb::Mdb;

/* ----------------------- storage key helpers -----------------------
IMPORTANT: All keys here are RELATIVE to the DB-level namespace.
The DB layer (mdb) will prepend "ammdata:" exactly once.           */

fn trades_ns_prefix(pool: &SchemaAlkaneId) -> Vec<u8> {
    // namespace:  "trades:v1:<block>:<tx>:"
    format!("trades:v1:{}:{}:", pool.block, pool.tx).into_bytes()
}

/// Key = namespace + "<ts>:<seq>"
fn trade_key(pool: &SchemaAlkaneId, ts: u64, seq: u32) -> Vec<u8> {
    let mut k = trades_ns_prefix(pool);
    k.extend_from_slice(ts.to_string().as_bytes());
    k.push(b':');
    k.extend_from_slice(seq.to_string().as_bytes());
    k
}

// simple encode/decode (borsh) for SchemaTradeV1
#[inline]
fn encode_trade_v1(trade: &SchemaTradeV1) -> Result<Vec<u8>> {
    Ok(to_vec(trade)?)
}

#[inline]
fn decode_trade_v1(v: &[u8]) -> Result<SchemaTradeV1> {
    Ok(SchemaTradeV1::try_from_slice(v)?)
}

/* ---------------- address / x-only key extraction ------------------ */

fn derive_addr_and_xonly(
    network: Network,
    prev_tx: &Transaction,
    vout: u32,
    spending_input: &TxIn,
) -> Option<(String, [u8; 32])> {
    let txo: &TxOut = prev_tx.output.get(vout as usize)?;
    let spk: &Script = &txo.script_pubkey;

    let addr = Address::from_script(spk, network).map(|a| a.to_string());

    // P2TR (OP_1 <32>)
    if spk.is_p2tr() {
        let mut it = spk.instructions();
        let _ = it.next(); // OP_1
        if let Some(Ok(Instruction::PushBytes(pk))) = it.next() {
            let bytes = pk.as_bytes();
            if bytes.len() == 32 {
                let mut x = [0u8; 32];
                x.copy_from_slice(bytes);
                return Some((addr.clone().unwrap_or_default(), x));
            }
        }
    }

    // P2WPKH (33B pubkey in witness) → x-only = last 32 bytes
    if spk.is_p2wpkh() {
        if let Some(pk) = spending_input.witness.last() {
            if pk.len() == 33 {
                let mut x = [0u8; 32];
                x.copy_from_slice(&pk[1..]);
                return Some((addr.clone().unwrap_or_default(), x));
            }
        }
    }

    // Fallback
    Some((addr.unwrap_or_default(), [0u8; 32]))
}

fn primary_spender_info(
    network: Network,
    prevouts: &HashMap<Txid, Transaction>,
    spending_tx: &Transaction,
) -> Option<(String, [u8; 32])> {
    for txin in spending_tx.input.iter() {
        let prev = prevouts.get(&txin.previous_output.txid)?;
        if let Some((addr, xonly)) =
            derive_addr_and_xonly(network, prev, txin.previous_output.vout, txin)
        {
            return Some((addr, xonly));
        }
    }
    None
}

/* ---------------- build a SchemaTradeV1 from extraction ------------- */

use crate::alkanes::trace::EspoAlkanesTransaction;
use crate::consts;
use crate::modules::ammdata::utils::reserves::ReserveExtraction;

/// Create a SchemaTradeV1 from a ReserveExtraction and its hosting tx.
pub fn create_trade_v1(
    timestamp: u64,
    prevouts: &HashMap<Txid, Transaction>,
    tx: &EspoAlkanesTransaction,
    extraction: &ReserveExtraction,
) -> Option<SchemaTradeV1> {
    // identify a primary swapper address + x-only pubkey
    let network: Network = consts::NETWORK;
    let (address, xpubkey) = primary_spender_info(network, prevouts, &tx.transaction)
        .unwrap_or((String::new(), [0u8; 32]));

    // inflows to the POOL = new - prev
    let (pb, pq) = extraction.prev_reserves;
    let (nb, nq) = extraction.new_reserves;
    let base_inflow: i128 = (nb as i128) - (pb as i128);
    let quote_inflow: i128 = (nq as i128) - (pq as i128);

    // txid bytes (compute_txid() is the non-deprecated name)
    let mut txid_bytes = [0u8; 32];
    txid_bytes.copy_from_slice(tx.transaction.compute_txid().as_raw_hash().as_byte_array());

    Some(SchemaTradeV1 {
        timestamp,
        txid: txid_bytes, // stored LE; we’ll flip to BE in the reader
        address,
        xpubkey,
        base_inflow,
        quote_inflow,
    })
}

/* ---------------- writer: collect all writes for a block ----------- */

#[inline]
fn be_u32(x: u32) -> [u8; 4] {
    x.to_be_bytes()
}
#[inline]
fn be_u64(x: u64) -> [u8; 8] {
    x.to_be_bytes()
}
#[inline]
fn be_u128(x: u128) -> [u8; 16] {
    x.to_be_bytes()
}
#[inline]
fn abs_i128(x: i128) -> u128 {
    if x < 0 { (-x) as u128 } else { x as u128 }
}

/// Side byte codes used in side-aware indexes.
/// 0=buy (v<0), 1=neutral (v==0), 2=sell (v>0)
#[inline]
pub fn side_code(v: i128) -> u8 {
    if v < 0 {
        0
    } else if v > 0 {
        2
    } else {
        1
    }
}

fn idx_ns_prefix(pool: &SchemaAlkaneId) -> String {
    format!("trades:idx:v1:{}:{}:", pool.block, pool.tx)
}

fn idx_prefix_ts(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}ts:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_absb(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}absb:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_absq(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}absq:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_sb_absb(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}sb_absb:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_sq_absq(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}sq_absq:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_sb_ts(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}sb_ts:", idx_ns_prefix(pool)).into_bytes()
}
fn idx_prefix_sq_ts(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}sq_ts:", idx_ns_prefix(pool)).into_bytes()
}

/// Public so index updater can maintain O(1) totals per pool (optional).
pub fn idx_count_key(pool: &SchemaAlkaneId) -> Vec<u8> {
    format!("{}__count", idx_ns_prefix(pool)).into_bytes()
}

/// Public so writer/reader can encode/decode counts when used.
pub fn encode_u64_be(x: u64) -> [u8; 8] {
    x.to_be_bytes()
}
pub fn decode_u64_be(v: &[u8]) -> Option<u64> {
    if v.len() == 8 {
        let mut b = [0u8; 8];
        b.copy_from_slice(v);
        Some(u64::from_be_bytes(b))
    } else {
        None
    }
}

/// Accumulator while walking a block (ensures unique keys per ts via seq).
/// Also emits secondary index keys (with values = [ts_be|seq_be]) for robust reads.
pub struct TradeWriteAcc {
    seqs: BTreeMap<(SchemaAlkaneId, u64), u32>,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TradeWriteAcc {
    pub fn new() -> Self {
        Self { seqs: BTreeMap::new(), writes: Vec::new() }
    }

    /// Push one trade value for a given pool+timestamp. Handles `seq`.
    /// Writes secondary indexes with value = ts_be(8) || seq_be(4)
    pub fn push(&mut self, pool: SchemaAlkaneId, ts: u64, t: SchemaTradeV1) -> Result<u32> {
        // primary
        let entry = self.seqs.entry((pool, ts)).or_insert(0);
        let seq = *entry;
        *entry = entry.wrapping_add(1);

        let k = trade_key(&pool, ts, seq);
        let v = encode_trade_v1(&t)?;
        self.writes.push((k, v));

        // ---- secondary indexes ----
        // value payload to make reads robust and backward/future-proof
        let mut val = Vec::with_capacity(12);
        val.extend_from_slice(&be_u64(ts));
        val.extend_from_slice(&be_u32(seq));

        let absb = abs_i128(t.base_inflow);
        let absq = abs_i128(t.quote_inflow);
        let sb = side_code(t.base_inflow); // 0 buy, 1 neutral, 2 sell
        let sq = side_code(t.quote_inflow);

        // ts:       ... ts(8) seq(4)
        {
            let mut k = idx_prefix_ts(&pool);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // absb:     ... absb(16) ts(8) seq(4)
        {
            let mut k = idx_prefix_absb(&pool);
            k.extend_from_slice(&be_u128(absb));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // absq:     ... absq(16) ts(8) seq(4)
        {
            let mut k = idx_prefix_absq(&pool);
            k.extend_from_slice(&be_u128(absq));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sb_absb:  ... side(1) absb(16) ts(8) seq(4)
        {
            let mut k = idx_prefix_sb_absb(&pool);
            k.push(sb);
            k.extend_from_slice(&be_u128(absb));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sq_absq:  ... side(1) absq(16) ts(8) seq(4)
        {
            let mut k = idx_prefix_sq_absq(&pool);
            k.push(sq);
            k.extend_from_slice(&be_u128(absq));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sb_ts:    ... side(1) ts(8) seq(4)
        {
            let mut k = idx_prefix_sb_ts(&pool);
            k.push(sb);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sq_ts:    ... side(1) ts(8) seq(4)
        {
            let mut k = idx_prefix_sq_ts(&pool);
            k.push(sq);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val));
        }

        Ok(seq)
    }

    pub fn into_writes(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.writes
    }
}

/* ---- optional external accumulator with `add` (Clone) -------------- */

/// Accumulator for secondary index writes and per-pool count deltas.
#[derive(Clone)]
pub struct TradeIndexAcc {
    writes: Vec<(Vec<u8>, Vec<u8>)>,
    /// number of new trades per pool in this block (all indexes share same count)
    per_pool_delta: HashMap<(u32, u64), u64>,
}

impl TradeIndexAcc {
    pub fn new() -> Self {
        Self { writes: Vec::new(), per_pool_delta: HashMap::new() }
    }

    /// Mirror the same index layout/value as TradeWriteAcc::push (ts_be||seq_be).
    pub fn add(&mut self, pool: &SchemaAlkaneId, ts: u64, seq: u32, t: &SchemaTradeV1) {
        let mut val = Vec::with_capacity(12);
        val.extend_from_slice(&be_u64(ts));
        val.extend_from_slice(&be_u32(seq));

        let absb = abs_i128(t.base_inflow);
        let absq = abs_i128(t.quote_inflow);
        let sb = side_code(t.base_inflow);
        let sq = side_code(t.quote_inflow);

        // ts
        {
            let mut k = idx_prefix_ts(pool);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // absb
        {
            let mut k = idx_prefix_absb(pool);
            k.extend_from_slice(&be_u128(absb));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // absq
        {
            let mut k = idx_prefix_absq(pool);
            k.extend_from_slice(&be_u128(absq));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sb_absb
        {
            let mut k = idx_prefix_sb_absb(pool);
            k.push(sb);
            k.extend_from_slice(&be_u128(absb));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sq_absq
        {
            let mut k = idx_prefix_sq_absq(pool);
            k.push(sq);
            k.extend_from_slice(&be_u128(absq));
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sb_ts
        {
            let mut k = idx_prefix_sb_ts(pool);
            k.push(sb);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val.clone()));
        }
        // sq_ts
        {
            let mut k = idx_prefix_sq_ts(pool);
            k.push(sq);
            k.extend_from_slice(&be_u64(ts));
            k.extend_from_slice(&be_u32(seq));
            self.writes.push((k, val));
        }

        *self.per_pool_delta.entry((pool.block, pool.tx)).or_insert(0) += 1;
    }

    pub fn into_writes(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.writes
    }
    pub fn per_pool_delta(self) -> HashMap<(u32, u64), u64> {
        self.per_pool_delta
    }
}

/* ---------------- reader: paginated & side-injected ---------------- */
use crate::modules::ammdata::consts::PRICE_SCALE;
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct TradeRow {
    pub timestamp: u64,
    /// txid hex, big-endian (display order)
    pub txid: String,
    pub address: String,
    /// x-only pubkey hex
    pub xpubkey: String,

    /// raw inflows as strings (exact i128 as stored)
    pub base_inflow: String,
    pub quote_inflow: String,

    /// derived from chosen side + inflow sign
    pub side: TradeUiSide,

    /// absolute volume on the chosen side (scaled by 1e8)
    pub amount: f64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TradeUiSide {
    Buy,
    Sell,
    Neutral,
}

impl TradeRow {
    #[inline]
    fn to_be(mut h: [u8; 32]) -> [u8; 32] {
        h.reverse();
        h
    }
    #[inline]
    fn scale_u128(x: u128) -> f64 {
        (x as f64) / (PRICE_SCALE as f64)
    }

    pub fn from_storage(t: &SchemaTradeV1, chosen: PriceSide) -> Self {
        let (amt_i128, side) = match chosen {
            PriceSide::Base => {
                let v = t.base_inflow;
                let s = if v < 0 {
                    TradeUiSide::Buy
                } else if v > 0 {
                    TradeUiSide::Sell
                } else {
                    TradeUiSide::Neutral
                };
                (v, s)
            }
            PriceSide::Quote => {
                let v = t.quote_inflow;
                let s = if v < 0 {
                    TradeUiSide::Buy
                } else if v > 0 {
                    TradeUiSide::Sell
                } else {
                    TradeUiSide::Neutral
                };
                (v, s)
            }
        };
        let amt_abs_u128 = if amt_i128 < 0 { (-amt_i128) as u128 } else { amt_i128 as u128 };

        TradeRow {
            timestamp: t.timestamp,
            txid: hex::encode(Self::to_be(t.txid)),
            address: t.address.clone(),
            xpubkey: hex::encode(t.xpubkey),
            base_inflow: t.base_inflow.to_string(),
            quote_inflow: t.quote_inflow.to_string(),
            side,
            amount: Self::scale_u128(amt_abs_u128),
        }
    }
}

pub struct TradePage {
    pub trades: Vec<TradeRow>,
    pub total: usize,
}

/* -------- legacy reader: newest→oldest by timestamp (primary) ------ */

#[inline]
fn parse_ts_from_key_tail(k: &[u8]) -> Option<u64> {
    // key ends with "...:<ts>:<seq>"
    let mut parts = k.rsplit(|&b| b == b':');
    let _seq_b = parts.next();
    if let Some(ts_b) = parts.next() {
        if let Ok(ts_s) = std::str::from_utf8(ts_b) {
            if let Ok(ts) = ts_s.parse::<u64>() {
                return Some(ts);
            }
        }
    }
    None
}

/// Read trades newest→oldest with pagination and return Buy/Sell **for the chosen side**.
pub fn read_trades_for_pool(
    mdb: &Mdb,
    pool: SchemaAlkaneId,
    page: usize,
    limit: usize,
    side: PriceSide,
) -> Result<TradePage> {
    // Build FULL DB prefix once for reverse iteration (iter_prefix_rev expects full prefix)
    let mut prefix = Vec::with_capacity(mdb.prefix().len() + 64);
    prefix.extend_from_slice(mdb.prefix());
    prefix.extend_from_slice(&trades_ns_prefix(&pool));

    // Collect newest → oldest
    let mut all: Vec<(u64, SchemaTradeV1)> = Vec::new();
    for res in mdb.iter_prefix_rev(&prefix) {
        let (k, v) = res?;
        let ts = parse_ts_from_key_tail(&k).unwrap_or_default();
        let t = decode_trade_v1(&v)?;
        all.push((ts, t));
    }

    let total = all.len();
    if limit == 0 {
        return Ok(TradePage { trades: vec![], total });
    }

    // paging: page is 1-based; newest-first already
    let start = page.saturating_sub(1).saturating_mul(limit);
    let end = (start + limit).min(total);
    let slice: &[(u64, SchemaTradeV1)] = if start >= end { &[] } else { &all[start..end] };

    let trades: Vec<TradeRow> =
        slice.iter().map(|(_ts, t)| TradeRow::from_storage(t, side)).collect();

    Ok(TradePage { trades, total })
}

/* --------- new: sorted reads over secondary index prefixes ---------- */

#[derive(Clone, Copy, Debug)]
pub enum TradeSortKey {
    Timestamp,       // "ts"
    AmountBaseAbs,   // "absb"
    AmountQuoteAbs,  // "absq"
    SideBaseTs,      // "sb_ts"
    SideQuoteTs,     // "sq_ts"
    SideBaseAmount,  // "sb_absb"
    SideQuoteAmount, // "sq_absq"
}

#[derive(Clone, Copy, Debug)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TradeSideFilter {
    All,
    Buy,
    Sell,
}

fn idx_prefix_for(pool: &SchemaAlkaneId, k: TradeSortKey) -> Vec<u8> {
    match k {
        TradeSortKey::Timestamp => idx_prefix_ts(pool),
        TradeSortKey::AmountBaseAbs => idx_prefix_absb(pool),
        TradeSortKey::AmountQuoteAbs => idx_prefix_absq(pool),
        TradeSortKey::SideBaseAmount => idx_prefix_sb_absb(pool),
        TradeSortKey::SideQuoteAmount => idx_prefix_sq_absq(pool),
        TradeSortKey::SideBaseTs => idx_prefix_sb_ts(pool),
        TradeSortKey::SideQuoteTs => idx_prefix_sq_ts(pool),
    }
}

/// Returns (effective_sort_key, fixed_side_byte_or_none) when a side filter is applied.
fn adjust_for_side_filter(
    requested_sort: TradeSortKey,
    chosen_side: PriceSide,
    filter: TradeSideFilter,
) -> (TradeSortKey, Option<u8>) {
    let fixed = match filter {
        TradeSideFilter::All => None,
        TradeSideFilter::Buy => Some(0u8),
        TradeSideFilter::Sell => Some(2u8),
    };

    if fixed.is_none() {
        return (requested_sort, None);
    }

    // ensure we use a side-aware index and fix the side-byte in prefix
    let eff = match requested_sort {
        TradeSortKey::Timestamp => match chosen_side {
            PriceSide::Base => TradeSortKey::SideBaseTs,
            PriceSide::Quote => TradeSortKey::SideQuoteTs,
        },
        TradeSortKey::AmountBaseAbs => TradeSortKey::SideBaseAmount,
        TradeSortKey::AmountQuoteAbs => TradeSortKey::SideQuoteAmount,
        TradeSortKey::SideBaseAmount
        | TradeSortKey::SideQuoteAmount
        | TradeSortKey::SideBaseTs
        | TradeSortKey::SideQuoteTs => requested_sort,
    };

    (eff, fixed)
}

/// Try to get the O(1) count; fallback to reverse scanning when a fixed-side subset is used.
fn total_for_pool_index(
    mdb: &Mdb,
    pool: &SchemaAlkaneId,
    prefix: &[u8],
    allow_count_key: bool,
) -> Result<usize> {
    if allow_count_key {
        // RELATIVE count key for mdb.get()
        let count_k = idx_count_key(pool);
        if let Some(v) = mdb.get(&count_k)? {
            if let Some(n) = decode_u64_be(&v) {
                return Ok(n as usize);
            }
        }
    }
    // Fallback: reverse scan of FULL prefix
    let mut c = 0usize;
    for _ in mdb.iter_prefix_rev(prefix) {
        c += 1;
    }
    Ok(c)
}

/// Decode (ts, seq) from index value if present, else from key tail (last 12 bytes).
fn decode_ts_seq_from_entry(key: &[u8], val: &[u8]) -> Option<(u64, u32)> {
    if val.len() == 12 {
        let mut bts = [0u8; 8];
        let mut bsq = [0u8; 4];
        bts.copy_from_slice(&val[0..8]);
        bsq.copy_from_slice(&val[8..12]);
        return Some((u64::from_be_bytes(bts), u32::from_be_bytes(bsq)));
    }
    if key.len() >= 12 {
        let mut bts = [0u8; 8];
        let mut bsq = [0u8; 4];
        bts.copy_from_slice(&key[key.len() - 12..key.len() - 4]);
        bsq.copy_from_slice(&key[key.len() - 4..]);
        return Some((u64::from_be_bytes(bts), u32::from_be_bytes(bsq)));
    }
    None
}

/// Read (page, limit) according to the chosen secondary index (sort), direction, and optional side filter.
pub fn read_trades_for_pool_sorted(
    mdb: &Mdb,
    pool: SchemaAlkaneId,
    page: usize,
    limit: usize,
    chosen_side: PriceSide,
    sort: TradeSortKey,
    dir: SortDir,
    filter: TradeSideFilter,
) -> Result<TradePage> {
    // adjust sort & get side byte (if any) when filtering
    let (eff_sort, fixed_side) = adjust_for_side_filter(sort, chosen_side, filter);

    // base index prefix (FULL) for reverse iteration
    let mut iprefix = Vec::with_capacity(mdb.prefix().len() + 64);
    iprefix.extend_from_slice(mdb.prefix());
    iprefix.extend_from_slice(&idx_prefix_for(&pool, eff_sort));

    // if we have a fixed side, narrow the prefix one more byte
    if let Some(sb) = fixed_side {
        iprefix.push(sb);
    }

    // count
    let allow_count_key = fixed_side.is_none()
        && matches!(
            eff_sort,
            TradeSortKey::Timestamp | TradeSortKey::AmountBaseAbs | TradeSortKey::AmountQuoteAbs
        );
    let total = total_for_pool_index(mdb, &pool, &iprefix, allow_count_key)?;
    if limit == 0 {
        return Ok(TradePage { trades: vec![], total });
    }

    let skip = limit.saturating_mul(page.saturating_sub(1));

    // scan index keys and decode (ts, seq) from value (preferred) or key tail
    let read_pairs_from_prefix = |prefix: &[u8]| -> Result<Vec<(u64, u32)>> {
        let mut out: Vec<(u64, u32)> = Vec::with_capacity(limit);
        match dir {
            SortDir::Desc => {
                let mut it = mdb.iter_prefix_rev(prefix);
                for (i, res) in it.by_ref().enumerate() {
                    if i < skip {
                        continue;
                    }
                    if out.len() >= limit {
                        break;
                    }
                    let (k, v) = res?;
                    if let Some(pair) = decode_ts_seq_from_entry(&k, &v) {
                        out.push(pair);
                    }
                }
            }
            SortDir::Asc => {
                let total_here = total_for_pool_index(mdb, &pool, prefix, false)?;
                let start = skip;
                let end = (skip + limit).min(total_here);
                if start < end {
                    let drop_rev = total_here.saturating_sub(end);
                    let take_rev = end - start;
                    let mut it = mdb.iter_prefix_rev(prefix);
                    for _ in it.by_ref().take(drop_rev) {}
                    for res in it.by_ref().take(take_rev) {
                        let (k, v) = res?;
                        if let Some(pair) = decode_ts_seq_from_entry(&k, &v) {
                            out.push(pair);
                        }
                    }
                    out.reverse();
                }
            }
        }
        Ok(out)
    };

    // use the requested (possibly side-aware) index
    let pairs = read_pairs_from_prefix(&iprefix)?;

    // materialize primaries → TradeRow
    let mut rows = Vec::with_capacity(pairs.len());
    for (ts, seq) in pairs {
        // RELATIVE primary key: "trades:v1:<pool>:<ts>:<seq>"
        let mut pk = trades_ns_prefix(&pool);
        pk.extend_from_slice(ts.to_string().as_bytes());
        pk.push(b':');
        pk.extend_from_slice(seq.to_string().as_bytes());

        if let Some(v) = mdb.get(&pk)? {
            let t = decode_trade_v1(&v)?;
            // sanity: if filtering, enforce it (robust even if very old index existed)
            if let Some(sb) = fixed_side {
                let actual = match chosen_side {
                    PriceSide::Base => side_code(t.base_inflow),
                    PriceSide::Quote => side_code(t.quote_inflow),
                };
                if actual != sb {
                    continue;
                }
            }
            rows.push(TradeRow::from_storage(&t, chosen_side));
        }
    }

    Ok(TradePage { trades: rows, total })
}
