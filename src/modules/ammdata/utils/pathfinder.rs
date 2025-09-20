// src/modules/ammdata/pathfinder.rs

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::modules::ammdata::schemas::SchemaPoolSnapshot;
use crate::schemas::SchemaAlkaneId;

/// Default per-hop fee in basis points (0.5% = 50 bps).
pub const DEFAULT_FEE_BPS: u32 = 50;

pub fn plan_swap_exact_tokens_for_tokens(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    amount_in: u128,
    amount_out_min: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    let g = Graph::from_snapshot(snapshot);
    let q = best_first_exact_in(&g, token_in, token_out, amount_in, fee_bps, max_hops)?;
    if q.amount_out >= amount_out_min { Some(q) } else { None }
}

pub fn plan_swap_tokens_for_exact_tokens(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    amount_out: u128,
    amount_in_max: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    let g = Graph::from_snapshot(snapshot);
    let q = best_first_exact_out(&g, token_in, token_out, amount_out, fee_bps, max_hops)?;
    if q.amount_in <= amount_in_max { Some(q) } else { None }
}

pub fn plan_swap_exact_tokens_for_tokens_implicit(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    available_in: u128,
    amount_out_min: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    plan_swap_exact_tokens_for_tokens(
        snapshot,
        token_in,
        token_out,
        available_in,
        amount_out_min,
        fee_bps,
        max_hops,
    )
}

/* ---------- Convenience wrappers using DEFAULT_FEE_BPS ---------- */

pub fn plan_exact_in_default_fee(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    amount_in: u128,
    amount_out_min: u128,
    max_hops: usize,
) -> Option<PathQuote> {
    plan_swap_exact_tokens_for_tokens(
        snapshot,
        token_in,
        token_out,
        amount_in,
        amount_out_min,
        DEFAULT_FEE_BPS,
        max_hops,
    )
}

pub fn plan_exact_out_default_fee(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    amount_out: u128,
    amount_in_max: u128,
    max_hops: usize,
) -> Option<PathQuote> {
    plan_swap_tokens_for_exact_tokens(
        snapshot,
        token_in,
        token_out,
        amount_out,
        amount_in_max,
        DEFAULT_FEE_BPS,
        max_hops,
    )
}

pub fn plan_implicit_default_fee(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    available_in: u128,
    amount_out_min: u128,
    max_hops: usize,
) -> Option<PathQuote> {
    plan_swap_exact_tokens_for_tokens_implicit(
        snapshot,
        token_in,
        token_out,
        available_in,
        amount_out_min,
        DEFAULT_FEE_BPS,
        max_hops,
    )
}

/* --------------------------------------------------------------------------------
   Quotes & Path shapes
-------------------------------------------------------------------------------- */

#[derive(Clone, Debug)]
pub struct Hop {
    pub pool: SchemaAlkaneId,
    pub token_in: SchemaAlkaneId,
    pub token_out: SchemaAlkaneId,
    pub amount_in: u128,
    pub amount_out: u128,
}

#[derive(Clone, Debug)]
pub struct PathQuote {
    pub hops: Vec<Hop>,
    pub amount_in: u128,
    pub amount_out: u128,
}

/* --------------------------------------------------------------------------------
   Best-first planners (Dijkstra-like on realized amounts)
-------------------------------------------------------------------------------- */

fn best_first_exact_in(
    g: &Graph,
    src: SchemaAlkaneId,
    dst: SchemaAlkaneId,
    amount_in: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    if src == dst {
        return Some(PathQuote { hops: vec![], amount_in, amount_out: amount_in });
    }

    // (max-heap) entries keyed by current amount achieved at a token with a concrete path
    #[derive(Clone)]
    struct Node {
        amount: u128, // achieved at 'at'
        at: SchemaAlkaneId,
        hops: Vec<Edge>, // concrete edges from src -> at
    }
    impl PartialEq for Node {
        fn eq(&self, o: &Self) -> bool {
            self.amount == o.amount
        }
    }
    impl Eq for Node {}
    impl PartialOrd for Node {
        fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Node {
        fn cmp(&self, o: &Self) -> Ordering {
            self.amount.cmp(&o.amount)
        } // max-heap by amount
    }

    let mut heap = BinaryHeap::new();
    heap.push(Node { amount: amount_in, at: src, hops: vec![] });

    // best amount seen at (token, hop_count) → prune dominated states
    let mut best_seen: HashMap<(SchemaAlkaneId, usize), u128> = HashMap::new();

    while let Some(Node { amount, at, hops }) = heap.pop() {
        let depth = hops.len();
        if depth > max_hops {
            continue;
        }

        // If we reached destination, reconstruct quote
        if at == dst && depth > 0 {
            return Some(pathquote_from_edges_exact_in(g, &hops, amount_in, fee_bps)?);
        }

        // Dominance check
        if let Some(&best_amt) = best_seen.get(&(at, depth)) {
            if amount <= best_amt {
                continue;
            }
        }
        best_seen.insert((at, depth), amount);

        // Expand neighbors
        if let Some(nexts) = g.neighbors.get(&at) {
            for (to, ek) in nexts {
                if hops.iter().any(|e| e.token_out == *to) {
                    continue;
                } // avoid revisiting tokens
                let edge = Edge { pool: ek.pool, token_in: ek.token_in, token_out: ek.token_out };
                // simulate *this single hop* to get the *greedy* next-amount
                if let Some((rin, rout)) = g.reserves_for(&edge) {
                    if rin == 0 || rout == 0 {
                        continue;
                    }
                    if let Some(next_amt) = xyk_out_exact_in(rin, rout, amount, fee_bps) {
                        if next_amt == 0 {
                            continue;
                        }
                        let mut nhops = hops.clone();
                        nhops.push(edge);
                        heap.push(Node { amount: next_amt, at: *to, hops: nhops });
                    }
                }
            }
        }
    }
    None
}

fn best_first_exact_out(
    g: &Graph,
    src: SchemaAlkaneId,
    dst: SchemaAlkaneId,
    amount_out: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    if src == dst {
        return Some(PathQuote { hops: vec![], amount_in: amount_out, amount_out });
    }

    // (min-heap via negation) entries keyed by current input required at a token
    #[derive(Clone)]
    struct Node {
        need_in: u128, // minimal input needed at 'at' to deliver required down the path
        at: SchemaAlkaneId,
        hops_rev: Vec<Edge>, // edges in reverse (dst -> ... -> at)
    }
    impl PartialEq for Node {
        fn eq(&self, o: &Self) -> bool {
            self.need_in == o.need_in
        }
    }
    impl Eq for Node {}
    impl PartialOrd for Node {
        fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Node {
        fn cmp(&self, o: &Self) -> Ordering {
            o.need_in.cmp(&self.need_in)
        } // min-heap
    }

    let mut heap = BinaryHeap::new();
    heap.push(Node { need_in: amount_out, at: dst, hops_rev: vec![] });

    let mut best_seen: HashMap<(SchemaAlkaneId, usize), u128> = HashMap::new();

    while let Some(Node { need_in, at, hops_rev }) = heap.pop() {
        let depth = hops_rev.len();
        if depth > max_hops {
            continue;
        }

        if at == src && depth > 0 {
            // reverse to forward and compute full quote
            let mut fwd = hops_rev.clone();
            fwd.reverse();
            return Some(pathquote_from_edges_exact_out(g, &fwd, amount_out, fee_bps)?);
        }

        if let Some(&best_need) = best_seen.get(&(at, depth)) {
            if need_in >= best_need {
                continue;
            }
        }
        best_seen.insert((at, depth), need_in);

        // Expand "incoming" neighbors (reverse traversal): edges that end at `at`
        if let Some(incomings) = g.in_neighbors.get(&at) {
            for (from, ek) in incomings {
                if hops_rev.iter().any(|e| e.token_in == *from) {
                    continue;
                } // avoid token repeats
                let edge = Edge { pool: ek.pool, token_in: ek.token_in, token_out: ek.token_out };
                if let Some((rin, rout)) = g.reserves_for(&edge) {
                    if rin == 0 || rout == 0 {
                        continue;
                    }
                    if let Some(need_up) = xyk_in_for_exact_out(rin, rout, need_in, fee_bps) {
                        let mut nhops = hops_rev.clone();
                        nhops.push(edge);
                        heap.push(Node { need_in: need_up, at: *from, hops_rev: nhops });
                    }
                }
            }
        }
    }
    None
}

/* --------------------------------------------------------------------------------
   Constant-product AMM math (Uniswap V2 style) with flat fee_bps per hop
-------------------------------------------------------------------------------- */

#[inline]
fn apply_fee(amount_in: u128, fee_bps: u32) -> u128 {
    let numer = amount_in.saturating_mul((10_000u128).saturating_sub(fee_bps as u128));
    numer / 10_000u128
}

fn xyk_out_exact_in(r_in: u128, r_out: u128, amount_in: u128, fee_bps: u32) -> Option<u128> {
    if r_in == 0 || r_out == 0 {
        return None;
    }
    let x = apply_fee(amount_in, fee_bps);
    if x == 0 {
        return Some(0);
    }
    let denom = r_in.saturating_add(x);
    if denom == 0 {
        return None;
    }
    Some(x.saturating_mul(r_out) / denom)
}

fn xyk_in_for_exact_out(r_in: u128, r_out: u128, amount_out: u128, fee_bps: u32) -> Option<u128> {
    if r_in == 0 || r_out == 0 || amount_out == 0 || amount_out >= r_out {
        return None;
    }
    let rem_out = r_out.saturating_sub(amount_out);
    if rem_out == 0 {
        return None;
    }
    let num = amount_out.saturating_mul(r_in);
    let x_prime = div_ceil(num, rem_out);
    let denom_bps = (10_000u128).saturating_sub(fee_bps as u128);
    if denom_bps == 0 {
        return None;
    }
    Some(div_ceil(x_prime.saturating_mul(10_000u128), denom_bps))
}

#[inline]
fn div_ceil(a: u128, b: u128) -> u128 {
    if b == 0 {
        return u128::MAX;
    }
    if a == 0 {
        return 0;
    }
    1u128.saturating_add((a - 1) / b)
}

/* --------------------------------------------------------------------------------
   Graph model (built from the single-key snapshot)
-------------------------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct EdgeKey {
    pool: SchemaAlkaneId,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
}

#[derive(Clone, Debug)]
struct Edge {
    pool: SchemaAlkaneId,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
}

#[derive(Clone, Debug)]
struct Graph {
    neighbors: HashMap<SchemaAlkaneId, Vec<(SchemaAlkaneId, EdgeKey)>>, // out-edges
    in_neighbors: HashMap<SchemaAlkaneId, Vec<(SchemaAlkaneId, EdgeKey)>>, // in-edges
    pools: HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
}

impl Graph {
    fn from_snapshot(snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>) -> Self {
        let mut neighbors: HashMap<SchemaAlkaneId, Vec<(SchemaAlkaneId, EdgeKey)>> = HashMap::new();
        let mut in_neighbors: HashMap<SchemaAlkaneId, Vec<(SchemaAlkaneId, EdgeKey)>> =
            HashMap::new();

        for (pool_id, snap) in snapshot.iter() {
            let a = snap.base_id;
            let b = snap.quote_id;

            let e_ab = EdgeKey { pool: *pool_id, token_in: a, token_out: b };
            let e_ba = EdgeKey { pool: *pool_id, token_in: b, token_out: a };

            neighbors.entry(a).or_default().push((b, e_ab));
            neighbors.entry(b).or_default().push((a, e_ba));

            in_neighbors.entry(b).or_default().push((a, e_ab));
            in_neighbors.entry(a).or_default().push((b, e_ba));
        }

        Self { neighbors, in_neighbors, pools: snapshot.clone() }
    }

    fn reserves_for(&self, e: &Edge) -> Option<(u128, u128)> {
        let snap = self.pools.get(&e.pool)?;
        if e.token_in == snap.base_id && e.token_out == snap.quote_id {
            Some((snap.base_reserve, snap.quote_reserve))
        } else if e.token_in == snap.quote_id && e.token_out == snap.base_id {
            Some((snap.quote_reserve, snap.base_reserve))
        } else {
            None
        }
    }
}

/* --------------------------------------------------------------------------------
   Quote assembly
-------------------------------------------------------------------------------- */

fn pathquote_from_edges_exact_in(
    g: &Graph,
    edges: &[Edge],
    mut amount_in: u128,
    fee_bps: u32,
) -> Option<PathQuote> {
    let mut hops_out: Vec<Hop> = Vec::with_capacity(edges.len());
    for e in edges {
        let (rin, rout) = g.reserves_for(e)?;
        let out = xyk_out_exact_in(rin, rout, amount_in, fee_bps)?;
        hops_out.push(Hop {
            pool: e.pool,
            token_in: e.token_in,
            token_out: e.token_out,
            amount_in,
            amount_out: out,
        });
        amount_in = out;
    }
    Some(PathQuote {
        amount_in: hops_out.first().map(|h| h.amount_in).unwrap_or(0),
        amount_out: hops_out.last().map(|h| h.amount_out).unwrap_or(0),
        hops: hops_out,
    })
}

fn pathquote_from_edges_exact_out(
    g: &Graph,
    edges: &[Edge], // forward order
    mut required_out: u128,
    fee_bps: u32,
) -> Option<PathQuote> {
    let mut hops_rev: Vec<Hop> = Vec::with_capacity(edges.len());
    for e in edges.iter().rev() {
        let (rin, rout) = g.reserves_for(e)?;
        let need_in = xyk_in_for_exact_out(rin, rout, required_out, fee_bps)?;
        hops_rev.push(Hop {
            pool: e.pool,
            token_in: e.token_in,
            token_out: e.token_out,
            amount_in: need_in,
            amount_out: required_out,
        });
        required_out = need_in;
    }
    hops_rev.reverse();
    Some(PathQuote {
        amount_in: hops_rev.first().map(|h| h.amount_in).unwrap_or(0),
        amount_out: hops_rev.last().map(|h| h.amount_out).unwrap_or(0),
        hops: hops_rev,
    })
}

/* --------------------------------------------------------------------------------
   Pretty helpers
-------------------------------------------------------------------------------- */

#[allow(dead_code)]
pub fn fmt_alkane(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

#[allow(dead_code)]
pub fn fmt_path(p: &PathQuote) -> String {
    let mut s = String::new();
    for (i, h) in p.hops.iter().enumerate() {
        if i > 0 {
            s.push_str(" -> ");
        }
        s.push_str(&format!(
            "[{}] {}:{} {} -> {} ({} -> {})",
            i,
            h.pool.block,
            h.pool.tx,
            fmt_alkane(&h.token_in),
            fmt_alkane(&h.token_out),
            h.amount_in,
            h.amount_out
        ));
    }
    s
}
