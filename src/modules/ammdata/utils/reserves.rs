use super::super::consts::K_TOLERANCE;
use crate::alkanes::trace::EspoTrace;
use crate::schemas::SchemaAlkaneId;
use crate::{
    alkanes::trace::{
        EspoAlkanesTransaction, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId,
    },
    modules::ammdata::schemas::SchemaMarketDefs,
};
use anyhow::{Context, Result, anyhow};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ReserveTokens {
    pub base: EspoSandshrewLikeTraceShortId,
    pub quote: EspoSandshrewLikeTraceShortId,
}

#[derive(Debug, Clone)]
pub struct ReserveExtraction {
    pub pool: EspoSandshrewLikeTraceShortId,
    pub token_ids: ReserveTokens,    // { base, quote }
    pub prev_reserves: (u128, u128), // (base, quote)
    pub new_reserves: (u128, u128),  // (base, quote)
    pub volume: (u128, u128),        // (base_in, quote_out)
    pub k_ratio_approx: Option<f64>, // best-effort diagnostic
}

pub fn extract_new_pools_from_espo_transaction(
    transaction: &EspoAlkanesTransaction,
) -> Result<Vec<(SchemaAlkaneId, SchemaMarketDefs)>> {
    /* ---------- helpers ---------- */
    let traces: &Vec<EspoTrace> = match &transaction.traces {
        Some(t) => t,
        None => return Ok(vec![]), // 👈 bail early with empty Vec
    };

    fn strip_0x(s: &str) -> &str {
        s.strip_prefix("0x").unwrap_or(s)
    }

    fn parse_hex_u32(s: &str) -> Result<u32> {
        let v = u128::from_str_radix(strip_0x(s), 16).context("hex->u128 failed (u32)")?;
        if v > u32::MAX as u128 {
            return Err(anyhow!("u32 overflow when parsing hex"));
        }
        Ok(v as u32)
    }

    fn parse_hex_u64(s: &str) -> Result<u64> {
        let v = u128::from_str_radix(strip_0x(s), 16).context("hex->u128 failed (u64)")?;
        if v > u64::MAX as u128 {
            return Err(anyhow!("u64 overflow when parsing hex"));
        }
        Ok(v as u64)
    }

    fn schema_id_from_short(id: &EspoSandshrewLikeTraceShortId) -> Result<SchemaAlkaneId> {
        Ok(SchemaAlkaneId { block: parse_hex_u32(&id.block)?, tx: parse_hex_u64(&id.tx)? })
    }

    fn le_u128_pair_from_32b_hex(h: &str) -> Result<(u128, u128)> {
        let hex = strip_0x(h);
        if hex.len() != 64 {
            return Err(anyhow!("expected 32-byte hex payload"));
        }
        let bytes = hex::decode(hex).context("hex decode failed")?;
        let read_le_u128 = |b: &[u8]| -> u128 {
            let mut v: u128 = 0;
            // interpret as little-endian
            for (i, byte) in b.iter().enumerate() {
                v |= (*byte as u128) << (8 * i as u32);
            }
            v
        };
        Ok((read_le_u128(&bytes[0..16]), read_le_u128(&bytes[16..32])))
    }

    fn schema_id_from_storage_val_32b(h: &str) -> Result<SchemaAlkaneId> {
        let (block_le, tx_le) =
            le_u128_pair_from_32b_hex(h).context("decode (block, tx) LE u128 pair")?;
        if block_le > u32::MAX as u128 || tx_le > u64::MAX as u128 {
            return Err(anyhow!("id parts overflow target sizes"));
        }
        Ok(SchemaAlkaneId { block: block_le as u32, tx: tx_le as u64 })
    }

    let mut created_ids: HashSet<(String, String)> = HashSet::new();
    for trace in traces.clone() {
        for ev in &trace.sandshrew_trace.events {
            if let EspoSandshrewLikeTraceEvent::Create(c) = ev {
                // keep as hex-strings for quick comparison with call-stack ids
                created_ids.insert((c.block.clone(), c.tx.clone()));
            }
        }
    }

    if created_ids.is_empty() {
        // No contracts created -> no new pools
        return Ok(Vec::new());
    }

    /* ---------- walk the call stack; detect pool init writes ---------- */
    let mut stack: VecDeque<EspoSandshrewLikeTraceShortId> = VecDeque::new();
    let mut results: Vec<(SchemaAlkaneId, SchemaMarketDefs)> = Vec::new();
    let mut seen_pools: HashSet<(u32, u64)> = HashSet::new(); // dedupe if multiple returns

    for trace in traces {
        for ev in &trace.sandshrew_trace.events {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                    stack.push_back(inv.context.myself.clone());
                }
                EspoSandshrewLikeTraceEvent::Return(ret) => {
                    let leaving = match stack.pop_back() {
                        Some(x) => x,
                        None => continue,
                    };

                    // Only consider returns from contracts created in this same transaction.
                    if !created_ids.contains(&(leaving.block.clone(), leaving.tx.clone())) {
                        continue;
                    }

                    // Look for both /alkane/0 and /alkane/1 in storage.
                    if ret.response.storage.is_empty() {
                        continue;
                    }

                    let mut alk0: Option<&str> = None;
                    let mut alk1: Option<&str> = None;
                    for kv in &ret.response.storage {
                        match kv.key.as_str() {
                            "/alkane/0" => alk0 = Some(kv.value.as_str()),
                            "/alkane/1" => alk1 = Some(kv.value.as_str()),
                            _ => {}
                        }
                    }

                    let (Some(v0), Some(v1)) = (alk0, alk1) else { continue };

                    // Decode base/quote from the 32-byte LE encoded (block, tx) pairs.
                    let base_id =
                        schema_id_from_storage_val_32b(v0).context("decode /alkane/0 failed")?;
                    let quote_id =
                        schema_id_from_storage_val_32b(v1).context("decode /alkane/1 failed")?;

                    let pool_id = schema_id_from_short(&leaving)
                        .context("decode created pool id (leaving)")?;

                    // Deduplicate in case multiple returns write the same keys.
                    if !seen_pools.insert((pool_id.block, pool_id.tx)) {
                        continue;
                    }

                    results.push((
                        pool_id,
                        SchemaMarketDefs {
                            base_alkane_id: base_id,
                            quote_alkane_id: quote_id,
                            pool_alkane_id: pool_id,
                        },
                    ));
                }
                EspoSandshrewLikeTraceEvent::Create(_c) => {
                    // already captured above; nothing to do during streaming walk
                }
            }
        }
    }

    Ok(results)
}
pub fn extract_reserves_from_espo_transaction<'a>(
    transaction: &EspoAlkanesTransaction,
    pools: &HashMap<SchemaAlkaneId, SchemaMarketDefs>,
) -> Result<Vec<ReserveExtraction>> {
    let traces: &Vec<EspoTrace> = match &transaction.traces {
        Some(t) => t,
        None => return Ok(vec![]), // 👈 bail early with empty Vec
    };
    #[inline]
    fn strip_0x(s: &str) -> &str {
        s.strip_prefix("0x").unwrap_or(s)
    }

    /// Parse a 32-byte hex string as two little-endian u128s: (base, quote).
    #[inline]
    fn le_u128_pair_from_32b_hex(h: &str) -> Result<(u128, u128)> {
        let hex = strip_0x(h);
        if hex.len() != 64 {
            return Err(anyhow!("expected 32-byte hex payload for reserves"));
        }
        let bytes = hex::decode(hex).context("hex decode failed")?;
        let read_le_u128 = |b: &[u8]| -> u128 {
            let mut v: u128 = 0;
            for (i, byte) in b.iter().enumerate() {
                v |= (*byte as u128) << (8 * i as u32);
            }
            v
        };
        Ok((read_le_u128(&bytes[0..16]), read_le_u128(&bytes[16..32])))
    }

    #[inline]
    fn parse_hex_u32(s: &str) -> Result<u32> {
        Ok(u32::from_str_radix(strip_0x(s), 16)?)
    }
    #[inline]
    fn parse_hex_u64(s: &str) -> Result<u64> {
        Ok(u64::from_str_radix(strip_0x(s), 16)?)
    }

    #[inline]
    fn short_to_schema(id: &EspoSandshrewLikeTraceShortId) -> Result<SchemaAlkaneId> {
        Ok(SchemaAlkaneId { block: parse_hex_u32(&id.block)?, tx: parse_hex_u64(&id.tx)? })
    }
    #[inline]
    fn schema_to_short(id: &SchemaAlkaneId) -> EspoSandshrewLikeTraceShortId {
        EspoSandshrewLikeTraceShortId {
            block: format!("0x{:x}", id.block),
            tx: format!("0x{:x}", id.tx),
        }
    }

    /// Next Return index at or after `start`.
    #[inline]
    fn next_return_idx(evs: &[EspoSandshrewLikeTraceEvent], start: usize) -> Option<usize> {
        (start..evs.len()).find(|&i| matches!(&evs[i], EspoSandshrewLikeTraceEvent::Return(_)))
    }

    /// The specific reserve anchor we want:
    /// - delegatecall
    /// - calldata == 0x61
    /// - caller == factory (0x4/0xfff2)
    #[inline]
    fn is_anchor(inv: &crate::alkanes::trace::EspoSandshrewLikeTraceInvokeData) -> bool {
        inv.typ == "delegatecall"
            && inv.context.inputs.len() == 1
            && inv.context.inputs[0].as_str() == "0x61"
            && inv.context.caller.block.eq_ignore_ascii_case("0x4")
            && inv.context.caller.tx.eq_ignore_ascii_case("0xfff2")
    }

    #[inline]
    fn ids_equal(a: &EspoSandshrewLikeTraceShortId, b: &EspoSandshrewLikeTraceShortId) -> bool {
        a.block.eq_ignore_ascii_case(&b.block) && a.tx.eq_ignore_ascii_case(&b.tx)
    }

    #[inline]
    fn parse_hex_u128_be(s: &str) -> Result<u128> {
        Ok(u128::from_str_radix(strip_0x(s), 16)?)
    }

    let evs: Vec<EspoSandshrewLikeTraceEvent> = traces
        .into_iter()
        .flat_map(|trace| trace.sandshrew_trace.events.clone())
        .collect();

    let mut results: Vec<ReserveExtraction> = Vec::new();
    let mut i = 0usize;

    while i < evs.len() {
        // 1) find the anchor
        let inv = match &evs[i] {
            EspoSandshrewLikeTraceEvent::Invoke(x) if is_anchor(x) => x,
            _ => {
                i += 1;
                continue;
            }
        };

        // pool identity + schema id
        let pool_short = inv.context.myself.clone();
        let pool_schema = short_to_schema(&pool_short).context("pool id parse failed")?;

        // authoritative base/quote ids from preloaded map
        let defs = match pools.get(&pool_schema) {
            Some(d) => *d,
            None => {
                i += 1;
                continue;
            }
        };
        let base_sid = schema_to_short(&defs.base_alkane_id);
        let quote_sid = schema_to_short(&defs.quote_alkane_id);

        // 2) the next two Return events must be identical (prev reserves)
        let r1 =
            next_return_idx(&evs, i + 1).ok_or_else(|| anyhow!("missing first reserves return"))?;
        let r2 = next_return_idx(&evs, r1 + 1)
            .ok_or_else(|| anyhow!("missing second reserves return"))?;

        let (d1, d2) = match (&evs[r1], &evs[r2]) {
            (EspoSandshrewLikeTraceEvent::Return(a), EspoSandshrewLikeTraceEvent::Return(b)) => {
                (a.response.data.as_str(), b.response.data.as_str())
            }
            _ => {
                i = r2 + 1;
                continue;
            }
        };
        if d1 != d2 || strip_0x(d1).len() != 64 {
            i = r2 + 1;
            continue;
        }

        // PRE-SWAP reserves (base, quote)
        let (prev_base, prev_quote) =
            le_u128_pair_from_32b_hex(d1).context("parse prev reserves")?;

        // 3) scan right after twins: first pool Invoke(call) gives the amount entering the POOL.
        //    Only accept if *exactly one* of {base, quote} appears in incomingAlkanes (swap).
        //    If both or none match → add-liquidity or unknown → skip.
        let mut j = r2 + 1;
        let mut pool_depth: i32 = 0;
        let mut base_in: u128 = 0;
        let mut quote_in: u128 = 0;
        let mut saw_candidate_call = false;
        let mut valid_swap = false;

        while j < evs.len() {
            match &evs[j] {
                EspoSandshrewLikeTraceEvent::Invoke(v) => {
                    if ids_equal(&v.context.myself, &pool_short) {
                        pool_depth += 1;

                        if v.typ == "call" && !saw_candidate_call {
                            // Count how many of the incoming match base/quote, and capture their amounts.
                            let mut matches = 0usize;
                            let mut tmp_base_in: u128 = 0;
                            let mut tmp_quote_in: u128 = 0;

                            for inc in &v.context.incoming_alkanes {
                                if ids_equal(&inc.id, &base_sid) {
                                    matches += 1;
                                    tmp_base_in =
                                        parse_hex_u128_be(&inc.value).context("parse base_in")?;
                                } else if ids_equal(&inc.id, &quote_sid) {
                                    matches += 1;
                                    tmp_quote_in =
                                        parse_hex_u128_be(&inc.value).context("parse quote_in")?;
                                }
                            }

                            saw_candidate_call = true;

                            // Accept only if *exactly one* matches (pure swap).
                            if matches == 1 {
                                base_in = tmp_base_in;
                                quote_in = tmp_quote_in;
                                valid_swap = true;
                            } else {
                                // matches == 0 (no swap) or matches >= 2 (add-liquidity)
                                // Mark invalid and break once we exit the pool frame.
                                valid_swap = false;
                            }
                        }
                    }
                }
                EspoSandshrewLikeTraceEvent::Return(_) => {
                    if pool_depth > 0 {
                        pool_depth -= 1;
                        if pool_depth == 0 {
                            break; // finished this pool frame slice
                        }
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
            j += 1;
        }

        if !valid_swap {
            // no single-sided input observed; skip this anchor
            i = r2 + 1;
            continue;
        }

        // 4) constant-product solve using *opposite* side as OUT:
        //    - base_in = a:  (b + a) * (q - y) = b*q  => y = q - floor( (b*q)/(b+a) )
        //    - quote_in = a: (b - x) * (q + a) = b*q  => x = b - floor( (b*q)/(q+a) )
        let k_prev = (prev_base as u128)
            .checked_mul(prev_quote as u128)
            .ok_or_else(|| anyhow!("k overflow"))?;

        let (new_base, new_quote, base_in_res, quote_out_res) = if base_in > 0 {
            // base_in → quote_out
            let nb = prev_base.checked_add(base_in).ok_or_else(|| anyhow!("base add overflow"))?;
            let nq = if nb == 0 { 0 } else { k_prev / nb }; // floor
            if nq > prev_quote {
                i = r2 + 1;
                continue;
            } // sanity
            let y = prev_quote - nq; // quote_out
            (nb, nq, base_in, y)
        } else {
            // quote_in → base_out
            let nq =
                prev_quote.checked_add(quote_in).ok_or_else(|| anyhow!("quote add overflow"))?;
            let nb = if nq == 0 { 0 } else { k_prev / nq }; // floor
            if nb > prev_base {
                i = r2 + 1;
                continue;
            } // sanity
            let _x = prev_base - nb; // base_out (not part of volume per (base_in, quote_out) schema)
            // volume stays (0,0) in this branch
            (nb, nq, 0u128, 0u128)
        };

        // sanity: K ratio (should be ~1 within tolerance)
        let k_ratio = if prev_base != 0 && prev_quote != 0 {
            ((new_base as f64) * (new_quote as f64)) / ((prev_base as f64) * (prev_quote as f64))
        } else {
            f64::INFINITY
        };
        if (k_ratio - 1.0).abs() > K_TOLERANCE {
            i = r2 + 1;
            continue;
        }

        // 5) emit result in strict (base, quote) order; volume=(base_in, quote_out)
        results.push(ReserveExtraction {
            pool: pool_short.clone(),
            token_ids: ReserveTokens { base: base_sid.clone(), quote: quote_sid.clone() },
            prev_reserves: (prev_base, prev_quote),
            new_reserves: (new_base, new_quote),
            volume: (base_in_res, quote_out_res),
            k_ratio_approx: if k_ratio.is_finite() { Some(k_ratio) } else { None },
        });

        // continue after this anchor’s twin returns
        i = r2 + 1;
    }

    if results.is_empty() {
        return Err(anyhow!("no_valid_swaps_after_k_filter"));
    }
    Ok(results)
}
