use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::ammdata::schemas::{SchemaCandleV1, SchemaFullCandleV1, Timeframe};
use crate::schemas::SchemaAlkaneId;

use crate::modules::ammdata::storage::{
    candle_key, candle_ns_prefix, decode_full_candle_v1, encode_full_candle_v1,
};
use crate::runtime::mdb::Mdb;
use anyhow::Result;
use std::collections::BTreeMap;

/* ---------- price helpers ---------- */

#[inline]
fn div_scaled(n: u128, d: u128, scale: u128) -> u128 {
    if d == 0 { 0 } else { n.saturating_mul(scale) / d }
}

/// price quoted in QUOTE per 1 BASE (quote/base)
#[inline]
pub fn price_quote_per_base(base_res: u128, quote_res: u128) -> u128 {
    div_scaled(quote_res, base_res, PRICE_SCALE)
}

/// price quoted in BASE per 1 QUOTE (base/quote)
#[inline]
pub fn price_base_per_quote(base_res: u128, quote_res: u128) -> u128 {
    div_scaled(base_res, quote_res, PRICE_SCALE)
}

/* ---------- time bucketing ---------- */

#[inline]
fn bucket_start(ts: u64, frame: Timeframe) -> u64 {
    let d = frame.duration_secs();
    ts / d * d
}

/* ---------- in-memory aggregation cache ---------- */

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CandleKey {
    pool: SchemaAlkaneId,
    tf: Timeframe,
    bucket_ts: u64,
}

#[derive(Debug, Clone, Copy)]
struct DualCandle {
    // base_candle: price = quote/base, vol = base_in
    base: SchemaCandleV1,
    // quote_candle: price = base/quote, vol = quote_out
    quote: SchemaCandleV1,
}

impl DualCandle {
    fn new(p_base: u128, p_quote: u128) -> Self {
        let cb =
            SchemaCandleV1 { open: p_base, high: p_base, low: p_base, close: p_base, volume: 0 };
        let cq = SchemaCandleV1 {
            open: p_quote,
            high: p_quote,
            low: p_quote,
            close: p_quote,
            volume: 0,
        };
        Self { base: cb, quote: cq }
    }

    fn update(&mut self, p_base: u128, p_quote: u128, base_in: u128, quote_out: u128) {
        // base side
        self.base.high = self.base.high.max(p_base);
        self.base.low = self.base.low.min(p_base);
        self.base.close = p_base;
        self.base.volume = self.base.volume.saturating_add(base_in);

        // quote side
        self.quote.high = self.quote.high.max(p_quote);
        self.quote.low = self.quote.low.min(p_quote);
        self.quote.close = p_quote;
        self.quote.volume = self.quote.volume.saturating_add(quote_out);
    }
}

pub struct CandleCache {
    map: BTreeMap<CandleKey, DualCandle>,
}

impl CandleCache {
    pub fn new() -> Self {
        Self { map: BTreeMap::new() }
    }

    /// Apply one trade to all specified frames.
    /// - `p_b_per_q`   = price quoted in BASE per 1 QUOTE (base/quote)
    /// - `p_q_per_b`   = price quoted in QUOTE per 1 BASE (quote/base)
    /// - `base_in`     = amount of BASE sent into the pool
    /// - `quote_out`   = amount of QUOTE sent out of the pool
    pub fn apply_trade_for_frames(
        &mut self,
        ts: u64,
        pool: SchemaAlkaneId,
        frames: &[Timeframe],
        p_b_per_q: u128,
        p_q_per_b: u128,
        base_in: u128,
        quote_out: u128,
    ) {
        for &tf in frames {
            let key = CandleKey { pool, tf, bucket_ts: bucket_start(ts, tf) };
            self.map
                .entry(key)
                .and_modify(|dc| dc.update(p_q_per_b, p_b_per_q, base_in, quote_out))
                .or_insert_with(|| {
                    let mut dc = DualCandle::new(p_q_per_b, p_b_per_q);
                    dc.update(p_q_per_b, p_b_per_q, base_in, quote_out);
                    dc
                });
        }
    }

    /// Create RocksDB writes by **merging** cache candles with any existing candle in DB.
    /// - open: keep the earliest (existing if present)
    /// - high/low: max/min over existing and cache
    /// - close: use cache.close (later)
    /// - volume: sum
    pub fn into_writes(self, mdb: &Mdb) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut writes = Vec::with_capacity(self.map.len());

        for (ck, dc_new) in self.map.into_iter() {
            // Key for this pool/tf/bucket
            let k = candle_key(&ck.pool, ck.tf, ck.bucket_ts);

            // Merge with existing (if any)
            let merged = if let Some(raw) = mdb.get(&k)? {
                let existing = decode_full_candle_v1(&raw)?;

                let mut base = existing.base_candle;
                base.high = base.high.max(dc_new.base.high);
                base.low = base.low.min(dc_new.base.low);
                base.close = dc_new.base.close;
                base.volume = base.volume.saturating_add(dc_new.base.volume);

                let mut quote = existing.quote_candle;
                quote.high = quote.high.max(dc_new.quote.high);
                quote.low = quote.low.min(dc_new.quote.low);
                quote.close = dc_new.quote.close;
                quote.volume = quote.volume.saturating_add(dc_new.quote.volume);

                SchemaFullCandleV1 { base_candle: base, quote_candle: quote }
            } else {
                SchemaFullCandleV1 { base_candle: dc_new.base, quote_candle: dc_new.quote }
            };

            let v = encode_full_candle_v1(&merged)?;
            writes.push((k, v));
        }

        Ok(writes)
    }
}

/* ---------- public reader for RPC ---------- */

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceSide {
    /// price = quote/base (quote units per 1 base) — use base_candle
    Base,
    /// price = base/quote (base units per 1 quote) — use quote_candle
    Quote,
}

///// What we return to RPC so it can timestamp correctly.
pub struct CandleSlice {
    pub candles_newest_first: Vec<SchemaCandleV1>,
    pub newest_ts: u64, // bucket start of the newest candle that actually exists
}
pub fn read_candles_v1(
    mdb: &Mdb,
    pool: SchemaAlkaneId,
    tf: Timeframe,
    _limit_unused: usize,
    now_ts: u64,
    side: PriceSide,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();

    let logical = candle_ns_prefix(&pool, tf);
    let mut full_prefix = Vec::with_capacity(mdb.prefix().len() + logical.len());
    full_prefix.extend_from_slice(mdb.prefix());
    full_prefix.extend_from_slice(&logical);

    let mut per_bucket: BTreeMap<u64, SchemaFullCandleV1> = BTreeMap::new();
    for res in mdb.iter_prefix_rev(&full_prefix) {
        let (k, v) = res?;
        if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
            if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    let fc = decode_full_candle_v1(&v)?;
                    if !per_bucket.contains_key(&ts) {
                        per_bucket.insert(ts, fc);
                    }
                }
            }
        }
    }

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(fc) = per_bucket.get(&bts) {
            let mut c = match side {
                PriceSide::Base => fc.base_candle,
                PriceSide::Quote => fc.quote_candle,
            };
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> = forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}
