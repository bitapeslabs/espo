use crate::alkanes::trace::{
    EspoAlkanesTransaction, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceShortId,
};
use anyhow::{Context, Result, anyhow};
use std::collections::{HashMap, HashSet};

/* ===== Result payload ===== */
#[derive(Debug, Clone)]
pub struct ReserveExtraction {
    pub pool: EspoSandshrewLikeTraceShortId,
    pub prev_reserves: (u128, u128), // (token0, token1)
    pub new_reserves: (u128, u128),  // (token0, token1), chosen mapping
    pub token_ids: [EspoSandshrewLikeTraceShortId; 2], // order: first, second
    pub mapping: &'static str,       // "first→token0, second→token1" or the opposite
    pub k_ratio_approx: Option<f64>, // best-effort diagnostic
}

pub fn extract_reserves_from_espo_transaction(
    transaction: &EspoAlkanesTransaction,
) -> Result<ReserveExtraction> {
    /* ---------- small helpers (explicit + readable) ---------- */

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

    // read two little-endian u128 values from a 32-byte hex string
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

    // find the RETURN that closes the INVOKE at `start`, respecting nesting
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

    /* ---------- 1) Locate previous reserves + pool id ---------- */
    let mut prev_reserves: Option<(u128, u128)> = None;
    let mut pool_id: Option<EspoSandshrewLikeTraceShortId> = None;

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
            prev_reserves = Some((a, b));
            pool_id = Some(inv.context.myself.clone());
            break;
        }
    }

    let (prev_token0, prev_token1) = prev_reserves.ok_or_else(|| anyhow!("no_prev_reserves"))?;
    let pool = pool_id.ok_or_else(|| anyhow!("pool id not found"))?;
    let pool_key = id_key(&pool);

    /* ---------- 2) Aggregate pool-scoped token flows (no double-counting) ---------- */
    #[derive(Default, Clone, Debug)]
    struct Flow {
        total_in: u128,
        total_out: u128,
        net_to_pool: i128, // signed: in - out
    }

    // small helpers to avoid borrow conflicts (no capturing closures)
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

    let mut flows: HashMap<String, Flow> = HashMap::new();
    let mut call_stack: Vec<String> = Vec::new();
    let mut pool_depth: i32 = 0;
    let mut capture_next_return_as_outflow = false;
    let mut seen_in_dedup: HashSet<String> = HashSet::new(); // (token,value) per top-level pool call

    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                let self_key = id_key(&inv.context.myself);
                call_stack.push(self_key.clone());

                if self_key == pool_key {
                    pool_depth += 1;
                    if pool_depth == 1 {
                        seen_in_dedup.clear(); // new top-level pool frame
                    }
                    // Count incoming ONLY on the pool's direct "call" (skip delegatecall duplication)
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
                    // If storage is empty, the pool's own return carries the sends.
                    if ret.response.storage.is_empty() {
                        for out in &ret.response.alkanes {
                            let token_key = id_key(&out.id);
                            let amount = hex_u128_be(&out.value)?;
                            push_outgoing(&mut flows, token_key, amount)?;
                        }
                    } else {
                        // Otherwise expect router fan-out: capture the very next RETURN.
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
        return Err(anyhow!("only_one_token_moved"));
    }

    /* ---------- 3) Choose the two most active tokens (|net| + in + out) ---------- */
    let mut ranked: Vec<(String, Flow, u128)> = flows
        .into_iter()
        .map(|(k, f)| {
            let abs_net =
                if f.net_to_pool >= 0 { f.net_to_pool as u128 } else { (-f.net_to_pool) as u128 };
            let score = abs_net.saturating_add(f.total_in).saturating_add(f.total_out);
            (k, f, score)
        })
        .collect();

    ranked.sort_by(|a, b| b.2.cmp(&a.2)); // descending by score
    let (first_token_id, first_flow, _) = ranked[0].clone();
    let (second_token_id, second_flow, _) = ranked[1].clone();

    // Clean swap: one net in (>0), the other net out (<0)
    let is_clean_swap = (first_flow.net_to_pool > 0 && second_flow.net_to_pool < 0)
        || (first_flow.net_to_pool < 0 && second_flow.net_to_pool > 0);
    if !is_clean_swap {
        return Err(anyhow!("not_clean_swap"));
    }

    /* ---------- 4) Compute new reserves under both mappings; choose K≈constant ---------- */
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

    let d_first = first_flow.net_to_pool;
    let d_second = second_flow.net_to_pool;

    let candidate1 = try_mapping(prev_token0, prev_token1, d_first, d_second); // first→token0, second→token1
    let candidate2 = try_mapping(prev_token0, prev_token1, d_second, d_first); // second→token0, first→token1

    let (chosen_label, chosen_new, chosen_ratio) = match (candidate1, candidate2) {
        (Some((n1, r1)), Some((n2, r2))) => {
            if (r1 - 1.0).abs() <= (r2 - 1.0).abs() {
                ("first→token0, second→token1", n1, Some(r1))
            } else {
                ("second→token0, first→token1", n2, Some(r2))
            }
        }
        (Some((n, r)), None) => ("first→token0, second→token1", n, Some(r)),
        (None, Some((n, r))) => ("second→token0, first→token1", n, Some(r)),
        _ => return Err(anyhow!("non_positive_reserves_after_mapping")),
    };

    /* ---------- 5) Build result ---------- */
    let parse_id = |s: &str| -> EspoSandshrewLikeTraceShortId {
        let (block, tx) = s.split_once('/').unwrap_or(("", ""));
        EspoSandshrewLikeTraceShortId { block: block.to_string(), tx: tx.to_string() }
    };

    Ok(ReserveExtraction {
        pool,
        prev_reserves: (prev_token0, prev_token1),
        new_reserves: chosen_new,
        token_ids: [parse_id(&first_token_id), parse_id(&second_token_id)],
        mapping: chosen_label,
        k_ratio_approx: chosen_ratio.filter(|r| r.is_finite()),
    })
}
