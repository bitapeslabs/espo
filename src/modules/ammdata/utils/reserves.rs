use crate::alkanes::trace::{
    EspoAlkanesTransaction, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceShortId,
};
use anyhow::{Context, Result, anyhow};
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

const K_TOLERANCE: f64 = 0.001; // 0.1%

pub fn extract_reserves_from_espo_transaction(
    transaction: &EspoAlkanesTransaction,
) -> Result<Vec<ReserveExtraction>> {
    /* ---------- helpers ---------- */

    let trace: EspoSandshrewLikeTrace = transaction.sandshrew_trace.clone();

    fn strip_0x(s: &str) -> &str {
        s.strip_prefix("0x").unwrap_or(s)
    }

    fn id_key(id: &EspoSandshrewLikeTraceShortId) -> String {
        format!("{}/{}", id.block.to_lowercase(), id.tx.to_lowercase())
    }

    fn hex_u128_be(s: &str) -> Result<u128> {
        u128::from_str_radix(strip_0x(s), 16).context("hex->u128 (BE) failed")
    }

    fn u128_to_i128(x: u128) -> Result<i128> {
        if x > i128::MAX as u128 { Err(anyhow!("value won't fit in i128")) } else { Ok(x as i128) }
    }

    fn le_u128_pair_from_32b_hex(h: &str) -> Result<(u128, u128)> {
        let hex = strip_0x(h);
        if hex.len() != 64 {
            return Err(anyhow!("expected 32-byte hex payload"));
        }
        let bytes = hex::decode(hex).context("hex decode failed")?;
        let read_le_u128 = |b: &[u8]| -> u128 {
            let mut v: u128 = 0;
            for &byte in b.iter().rev() {
                v = (v << 8) | (byte as u128);
            }
            v
        };
        Ok((read_le_u128(&bytes[0..16]), read_le_u128(&bytes[16..32])))
    }

    fn find_matching_return_index(
        events: &[EspoSandshrewLikeTraceEvent],
        start: usize,
    ) -> Option<usize> {
        let mut depth = 1i32;
        for i in start + 1..events.len() {
            match &events[i] {
                EspoSandshrewLikeTraceEvent::Invoke(_) => depth += 1,
                EspoSandshrewLikeTraceEvent::Return(_) => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        None
    }

    #[derive(Debug, Clone)]
    struct Anchor {
        pool: EspoSandshrewLikeTraceShortId,
        prev_reserves_token_order: (u128, u128), // (token0, token1)
        seg_start: usize,                        // inclusive
        seg_end: usize,                          // exclusive
    }

    let mut tails: Vec<(EspoSandshrewLikeTraceShortId, (u128, u128), usize)> = Vec::new();

    for i in 0..trace.events.len() {
        let EspoSandshrewLikeTraceEvent::Invoke(inv) = &trace.events[i] else { continue };
        let is_get_reserves = inv.typ == "delegatecall"
            && inv.context.inputs.len() == 1
            && inv.context.inputs[0].as_str() == "0x61";
        if !is_get_reserves {
            continue;
        }

        let r1 = find_matching_return_index(&trace.events, i)
            .ok_or_else(|| anyhow!("reserve extraction failure: none value @ r1"))?;
        let r2 = find_matching_return_index(&trace.events, r1)
            .ok_or_else(|| anyhow!("reserve extraction failure: none value @ r2"))?;
        let (d1, d2) = match (&trace.events[r1], &trace.events[r2]) {
            (
                EspoSandshrewLikeTraceEvent::Return(ret1),
                EspoSandshrewLikeTraceEvent::Return(ret2),
            ) => (ret1.response.data.as_str(), ret2.response.data.as_str()),
            _ => continue,
        };
        if d1 == d2 && strip_0x(d1).len() == 64 {
            let (a, b) =
                le_u128_pair_from_32b_hex(d1).context("failed to parse reserves (LE u128 pair)")?;
            tails.push((inv.context.myself.clone(), (a, b), r2 + 1));
        }
    }

    if tails.is_empty() {
        return Err(anyhow!("no_prev_reserves"));
    }

    let mut anchors: Vec<Anchor> = Vec::new();
    for (idx, (pool, prev_reserves, seg_start)) in tails.iter().enumerate() {
        let seg_end = if idx + 1 < tails.len() { tails[idx + 1].2 } else { trace.events.len() };
        anchors.push(Anchor {
            pool: pool.clone(),
            prev_reserves_token_order: *prev_reserves,
            seg_start: *seg_start,
            seg_end,
        });
    }

    #[derive(Default, Clone, Debug)]
    struct Flow {
        total_in: u128,
        total_out: u128,
        net_to_pool: i128, // signed: in - out
    }

    fn push_incoming(
        flows: &mut HashMap<String, Flow>,
        token_key: String,
        amount_u: u128,
    ) -> Result<()> {
        let amount_i = u128_to_i128(amount_u)?;
        let e = flows.entry(token_key).or_default();
        e.total_in = e.total_in.saturating_add(amount_u);
        e.net_to_pool = e.net_to_pool.saturating_add(amount_i);
        Ok(())
    }

    fn push_outgoing(
        flows: &mut HashMap<String, Flow>,
        token_key: String,
        amount_u: u128,
    ) -> Result<()> {
        let amount_i = u128_to_i128(amount_u)?;
        let e = flows.entry(token_key).or_default();
        e.total_out = e.total_out.saturating_add(amount_u);
        e.net_to_pool = e.net_to_pool.saturating_sub(amount_i);
        Ok(())
    }

    fn try_mapping(
        prev0: u128,
        prev1: u128,
        d0_i: i128,
        d1_i: i128,
    ) -> Option<((u128, u128), f64)> {
        let n0 = if d0_i >= 0 {
            prev0.checked_add(d0_i as u128)?
        } else {
            prev0.checked_sub((-d0_i) as u128)?
        };
        let n1 = if d1_i >= 0 {
            prev1.checked_add(d1_i as u128)?
        } else {
            prev1.checked_sub((-d1_i) as u128)?
        };
        if n0 == 0 || n1 == 0 {
            return None;
        }
        let k_prev = (prev0 as f64) * (prev1 as f64);
        let k_new = (n0 as f64) * (n1 as f64);
        let ratio =
            if k_prev.is_finite() && k_prev != 0.0 { k_new / k_prev } else { f64::INFINITY };
        Some(((n0, n1), ratio))
    }

    let mut results: Vec<ReserveExtraction> = Vec::new();

    for anchor in anchors {
        let pool_key = id_key(&anchor.pool);

        let mut flows: HashMap<String, Flow> = HashMap::new();
        let mut call_stack: Vec<String> = Vec::new();
        let mut pool_depth: i32 = 0;
        let mut capture_next_return_as_outflow = false;
        let mut seen_in_dedup: HashSet<String> = HashSet::new();

        for ev in &trace.events[anchor.seg_start..anchor.seg_end] {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                    let self_key = id_key(&inv.context.myself);
                    call_stack.push(self_key.clone());

                    if self_key == pool_key {
                        pool_depth += 1;
                        if pool_depth == 1 {
                            seen_in_dedup.clear();
                        }
                        if inv.typ == "call" {
                            for inc in &inv.context.incoming_alkanes {
                                let token_key = id_key(&inc.id);
                                let val_hex = inc.value.clone();
                                let amount = hex_u128_be(&val_hex)?;
                                let sig = format!("{}:{}", token_key, val_hex);
                                if seen_in_dedup.insert(sig) {
                                    push_incoming(&mut flows, token_key, amount)?;
                                }
                            }
                        }
                    }
                }
                EspoSandshrewLikeTraceEvent::Return(ret) => {
                    let leaving_key = call_stack.pop().unwrap_or_default();
                    if leaving_key == pool_key {
                        if ret.response.storage.is_empty() {
                            for out in &ret.response.alkanes {
                                let token_key = id_key(&out.id);
                                let amount = hex_u128_be(&out.value)?;
                                push_outgoing(&mut flows, token_key, amount)?;
                            }
                        } else {
                            capture_next_return_as_outflow = true;
                        }
                        pool_depth = (pool_depth - 1).max(0);
                    } else if capture_next_return_as_outflow {
                        for out in &ret.response.alkanes {
                            let token_key = id_key(&out.id);
                            let amount = hex_u128_be(&out.value)?;
                            push_outgoing(&mut flows, token_key, amount)?;
                        }
                        capture_next_return_as_outflow = false;
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
        }

        if flows.len() < 2 {
            continue;
        }

        // Rank by |net| + in + out
        let mut ranked: Vec<(String, Flow, u128)> = flows
            .into_iter()
            .map(|(k, f)| {
                let abs_net = if f.net_to_pool >= 0 {
                    f.net_to_pool as u128
                } else {
                    (-f.net_to_pool) as u128
                };
                let score = abs_net.saturating_add(f.total_in).saturating_add(f.total_out);
                (k, f, score)
            })
            .collect();
        ranked.sort_by(|a, b| b.2.cmp(&a.2));

        let (t1_id, t1_flow, _) = ranked[0].clone();
        let (t2_id, t2_flow, _) = ranked[1].clone();

        // Clean swap: one net in (>0), the other net out (<0)
        let is_clean_swap = (t1_flow.net_to_pool > 0 && t2_flow.net_to_pool < 0)
            || (t1_flow.net_to_pool < 0 && t2_flow.net_to_pool > 0);
        if !is_clean_swap {
            continue;
        }

        // Define base/quote by direction:
        // base = token sent *into* the pool (net_to_pool > 0)
        // quote = token sent *out* of the pool (net_to_pool < 0)
        let (base_id_str, base_flow, quote_id_str, quote_flow) = if t1_flow.net_to_pool > 0 {
            (t1_id.clone(), t1_flow.clone(), t2_id.clone(), t2_flow.clone())
        } else {
            (t2_id.clone(), t2_flow.clone(), t1_id.clone(), t1_flow.clone())
        };

        let (prev_token0, prev_token1) = anchor.prev_reserves_token_order;

        // We still need to align flows (which are per-token) with token0/token1 order to compute new reserves,
        // then re-orient the final reserves to (base, quote).
        let d_first = if base_id_str == t1_id { t1_flow.net_to_pool } else { t2_flow.net_to_pool };
        let d_second = if base_id_str == t1_id { t2_flow.net_to_pool } else { t1_flow.net_to_pool };

        let cand1 = try_mapping(prev_token0, prev_token1, d_first, d_second); // first → token0, second → token1
        let cand2 = try_mapping(prev_token0, prev_token1, d_second, d_first); // second → token0, first → token1

        // Choose mapping with K closest to constant
        enum ChosenMap {
            FirstToToken0,
            SecondToToken0,
        }
        let (chosen_map, new_token_order, k_ratio) = match (cand1, cand2) {
            (Some((n1, r1)), Some((n2, r2))) => {
                if (r1 - 1.0).abs() <= (r2 - 1.0).abs() {
                    (ChosenMap::FirstToToken0, n1, Some(r1))
                } else {
                    (ChosenMap::SecondToToken0, n2, Some(r2))
                }
            }
            (Some((n, r)), None) => (ChosenMap::FirstToToken0, n, Some(r)),
            (None, Some((n, r))) => (ChosenMap::SecondToToken0, n, Some(r)),
            _ => continue,
        };

        // Filter by K tolerance
        let pass_k = k_ratio.map(|r| (r - 1.0).abs() <= K_TOLERANCE).unwrap_or(false);
        if !pass_k {
            continue;
        }

        // Convert prev/new reserves from (token0, token1) into (base, quote)
        // Figure out which logical token ("base" or "quote") mapped to token0.
        let base_is_token0 = match chosen_map {
            ChosenMap::FirstToToken0 => base_id_str == t1_id,
            ChosenMap::SecondToToken0 => base_id_str == t2_id,
        };

        let prev_base_quote =
            if base_is_token0 { (prev_token0, prev_token1) } else { (prev_token1, prev_token0) };
        let new_base_quote = if base_is_token0 {
            (new_token_order.0, new_token_order.1)
        } else {
            (new_token_order.1, new_token_order.0)
        };

        // Volumes: base_in (>0), quote_out (>0)
        let base_in =
            (if base_flow.net_to_pool > 0 { base_flow.net_to_pool as i128 } else { 0 }) as u128;
        let quote_out =
            (if quote_flow.net_to_pool < 0 { (-quote_flow.net_to_pool) as i128 } else { 0 })
                as u128;

        let parse_id = |s: &str| -> EspoSandshrewLikeTraceShortId {
            let (block, tx) = s.split_once('/').unwrap_or(("", ""));
            EspoSandshrewLikeTraceShortId { block: block.to_string(), tx: tx.to_string() }
        };

        results.push(ReserveExtraction {
            pool: anchor.pool.clone(),
            token_ids: ReserveTokens {
                base: parse_id(&base_id_str),
                quote: parse_id(&quote_id_str),
            },
            prev_reserves: prev_base_quote,
            new_reserves: new_base_quote,
            volume: (base_in, quote_out),
            k_ratio_approx: k_ratio.filter(|r| r.is_finite()),
        });
    }

    if results.is_empty() {
        return Err(anyhow!("no_valid_swaps_after_k_filter"));
    }
    Ok(results)
}
