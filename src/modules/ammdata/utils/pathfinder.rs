// src/modules/ammdata/pathfinder.rs

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::modules::ammdata::schemas::SchemaPoolSnapshot;
use crate::schemas::SchemaAlkaneId;

/// Default per-hop fee in basis points (0.5% = 50 bps).
pub const DEFAULT_FEE_BPS: u32 = 50;

/* --------------------------------------------------------------------------------
   Public API (three planners)
   NOTE: These functions accept the single-key snapshot map:
         HashMap<SchemaAlkaneId /*pool*/, SchemaPoolSnapshot>
   This works for both event-derived and "is_latest" live-corrected snapshots,
   since they share the same schema and keys.
-------------------------------------------------------------------------------- */

pub fn plan_swap_exact_tokens_for_tokens(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token_in: SchemaAlkaneId,
    token_out: SchemaAlkaneId,
    amount_in: u128,
    amount_out_min: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    if amount_in == 0 || token_in == token_out {
        return None;
    }
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
    if amount_out == 0 || token_in == token_out {
        return None;
    }
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
   NEW: MEV self-cycle optimizer
-------------------------------------------------------------------------------- */

/// Find the **best self-cycle** starting and ending at `token`,
/// choosing BOTH the path and the input amount that maximizes profit:
///     profit(x) = amount_out(x) - x
///
/// Returns `Some(PathQuote)` only if max profit > 0.
pub fn plan_best_mev_swap(
    snapshot: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
    token: SchemaAlkaneId,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    let g = Graph::from_snapshot(snapshot);

    // Enumerate simple cycles (no token repeats) up to max_hops, length >= 2.
    let cycles = enumerate_cycles(&g, token, max_hops);

    let mut best: Option<PathQuote> = None;
    let mut best_profit: i128 = 0;

    for path in cycles {
        // Determine a safe cap for input search: 1/3 of the tightest inbound reserve across hops.
        let cap = cap_for_path(&g, &path, fee_bps);
        if cap == 0 {
            continue;
        }

        if let Some(q) = optimize_exact_in_on_path(&g, &path, fee_bps, cap) {
            let profit = q.amount_out as i128 - q.amount_in as i128;
            if profit > best_profit {
                best_profit = profit;
                best = Some(q);
            }
        }
    }

    if best_profit > 0 { best } else { None }
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
    #[derive(Clone)]
    struct Node {
        amount: u128,       // achieved at `at`
        at: SchemaAlkaneId, // current token
        hops: Vec<Edge>,    // edges src -> at
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
        }
    } // max-heap

    let mut heap = BinaryHeap::new();
    heap.push(Node { amount: amount_in, at: src, hops: vec![] });

    let mut best_seen: HashMap<(SchemaAlkaneId, usize), u128> = HashMap::new();
    let mut best_quote: Option<PathQuote> = None;
    let mut best_out_at_dst: u128 = 0;

    while let Some(Node { amount, at, hops }) = heap.pop() {
        let depth = hops.len();
        if depth > max_hops {
            continue;
        }

        if at == dst && depth > 0 {
            if let Some(q) = pathquote_from_edges_exact_in(g, &hops, amount_in, fee_bps) {
                if q.amount_out > best_out_at_dst {
                    best_out_at_dst = q.amount_out;
                    best_quote = Some(q);
                }
            }
        }

        if let Some(&best_amt) = best_seen.get(&(at, depth)) {
            if amount <= best_amt {
                continue;
            }
        }
        best_seen.insert((at, depth), amount);

        if let Some(nexts) = g.neighbors.get(&at) {
            for (to, ek) in nexts {
                // Avoid revisiting intermediate tokens, allow closing at dst.
                if hops.iter().any(|e| e.token_out == *to) && *to != dst {
                    continue;
                }

                // Avoid immediate ping-pong on the SAME pool
                let is_immediate_backtrack_same_pool = hops.last().map_or(false, |prev| {
                    prev.pool == ek.pool && prev.token_in == *to && prev.token_out == at
                });
                if is_immediate_backtrack_same_pool {
                    continue;
                }

                let edge = Edge { pool: ek.pool, token_in: ek.token_in, token_out: ek.token_out };
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
    best_quote
}

fn best_first_exact_out(
    g: &Graph,
    src: SchemaAlkaneId,
    dst: SchemaAlkaneId,
    amount_out: u128,
    fee_bps: u32,
    max_hops: usize,
) -> Option<PathQuote> {
    #[derive(Clone)]
    struct Node {
        need_in: u128,       // minimal input needed at `at`
        at: SchemaAlkaneId,  // reverse search token
        hops_rev: Vec<Edge>, // edges in reverse (dst -> … -> at)
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
        }
    } // min-heap

    let mut heap = BinaryHeap::new();
    heap.push(Node { need_in: amount_out, at: dst, hops_rev: vec![] });

    let mut best_seen: HashMap<(SchemaAlkaneId, usize), u128> = HashMap::new();
    let mut best_quote: Option<PathQuote> = None;
    let mut best_in_at_src: Option<u128> = None;

    while let Some(Node { need_in, at, hops_rev }) = heap.pop() {
        let depth = hops_rev.len();
        if depth > max_hops {
            continue;
        }

        if at == src && depth > 0 {
            let mut fwd = hops_rev.clone();
            fwd.reverse();
            if let Some(q) = pathquote_from_edges_exact_out(g, &fwd, amount_out, fee_bps) {
                if best_in_at_src.map_or(true, |cur| q.amount_in < cur) {
                    best_in_at_src = Some(q.amount_in);
                    best_quote = Some(q);
                }
            }
        }

        if let Some(&best_need) = best_seen.get(&(at, depth)) {
            if need_in >= best_need {
                continue;
            }
        }
        best_seen.insert((at, depth), need_in);

        if let Some(incomings) = g.in_neighbors.get(&at) {
            for (from, ek) in incomings {
                if hops_rev.iter().any(|e| e.token_in == *from) && *from != src {
                    continue;
                }

                let is_immediate_backtrack_same_pool = hops_rev.last().map_or(false, |prev| {
                    prev.pool == ek.pool && prev.token_out == *from && prev.token_in == at
                });
                if is_immediate_backtrack_same_pool {
                    continue;
                }

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
    best_quote
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
    // x' = ceil(out * r_in / (r_out - out))
    let num = amount_out.saturating_mul(r_in);
    let x_prime = div_ceil(num, rem_out);
    // x = ceil( x' * 10000 / (10000 - fee_bps) )
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
            // Skip zero-liquidity pools entirely to avoid dead edges
            if snap.base_reserve == 0 || snap.quote_reserve == 0 {
                continue;
            }

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

    /// Fetch reserves in the exact direction of the edge (token_in -> token_out).
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
    if edges.is_empty() {
        return None;
    }
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
    if edges.is_empty() {
        return None;
    }
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
   Cycle enumeration & input optimization (for MEV)
-------------------------------------------------------------------------------- */

/// Enumerate simple cycles starting and ending at `start`, with 2..=max_hops edges.
fn enumerate_cycles(g: &Graph, start: SchemaAlkaneId, max_hops: usize) -> Vec<Vec<Edge>> {
    let mut out: Vec<Vec<Edge>> = Vec::new();
    let mut visited = HashSet::<SchemaAlkaneId>::new();
    let mut acc: Vec<Edge> = Vec::new();

    visited.insert(start);
    dfs_cycles(g, start, start, max_hops, &mut visited, &mut acc, &mut out);
    out
}

fn dfs_cycles(
    g: &Graph,
    cur: SchemaAlkaneId,
    start: SchemaAlkaneId,
    remaining: usize,
    visited: &mut HashSet<SchemaAlkaneId>,
    acc: &mut Vec<Edge>,
    out: &mut Vec<Vec<Edge>>,
) {
    if remaining == 0 {
        return;
    }

    if let Some(nexts) = g.neighbors.get(&cur) {
        for (to, ek) in nexts {
            // Allow returning to start (closing the cycle) only if length >= 1 already.
            if *to == start {
                if !acc.is_empty() {
                    // Require at least 2 edges total for a meaningful cycle.
                    if acc.len() + 1 >= 2 {
                        let mut path = acc.clone();
                        path.push(Edge {
                            pool: ek.pool,
                            token_in: ek.token_in,
                            token_out: ek.token_out,
                        });

                        // Disallow immediate backtrack on the same pool (…A->B via P then B->A via P)
                        if !is_immediate_backtrack_same_pool(&path) {
                            out.push(path);
                        }
                    }
                }
                continue;
            }

            // Avoid revisiting intermediate tokens to keep it simple.
            if visited.contains(to) {
                continue;
            }

            let edge = Edge { pool: ek.pool, token_in: ek.token_in, token_out: ek.token_out };

            // Avoid immediate backtrack on SAME pool while building.
            if acc.last().map_or(false, |prev| {
                prev.pool == edge.pool && prev.token_in == *to && prev.token_out == cur
            }) {
                continue;
            }

            acc.push(edge);
            visited.insert(*to);
            dfs_cycles(g, *to, start, remaining - 1, visited, acc, out);
            visited.remove(to);
            acc.pop();
        }
    }
}

fn is_immediate_backtrack_same_pool(path: &[Edge]) -> bool {
    if path.len() < 2 {
        return false;
    }
    let a = &path[path.len() - 2];
    let b = &path[path.len() - 1];
    a.pool == b.pool && a.token_in == b.token_out && a.token_out == b.token_in
}

/// Compute a conservative cap for input search: min over hops of (res_in / 3).
fn cap_for_path(g: &Graph, edges: &[Edge], _fee_bps: u32) -> u128 {
    let mut cap = u128::MAX;
    for e in edges {
        if let Some((rin, _rout)) = g.reserves_for(e) {
            if rin == 0 {
                return 0;
            }
            let hop_cap = rin / 3;
            if hop_cap < cap {
                cap = hop_cap;
            }
        } else {
            return 0;
        }
    }
    if cap == 0 { 0 } else { cap }
}

/// For a fixed path, search input x ∈ [1, cap] to maximize profit f(x)-x.
/// Uses integer ternary search (40 iters) with a small-range fallback linear scan.
fn optimize_exact_in_on_path(
    g: &Graph,
    edges: &[Edge],
    fee_bps: u32,
    cap: u128,
) -> Option<PathQuote> {
    if cap == 0 {
        return None;
    }
    // Tiny ranges: brute force for exactness.
    if cap <= 64 {
        let mut best: Option<PathQuote> = None;
        let mut best_profit: i128 = i128::MIN;
        for x in 1..=cap {
            if let Some(q) = pathquote_from_edges_exact_in(g, edges, x, fee_bps) {
                let p = q.amount_out as i128 - q.amount_in as i128;
                if p > best_profit {
                    best_profit = p;
                    best = Some(q);
                }
            }
        }
        return best;
    }

    // Ternary search on integer domain (assumes quasi-concave profit curve for CPMM composition).
    let mut lo: u128 = 1;
    let mut hi: u128 = cap;
    let mut best_local: Option<PathQuote> = None;
    let mut best_profit: i128 = i128::MIN;

    for _ in 0..40 {
        let m1 = lo + (hi - lo) / 3;
        let m2 = hi - (hi - lo) / 3;

        let q1 = pathquote_from_edges_exact_in(g, edges, m1, fee_bps)?;
        let q2 = pathquote_from_edges_exact_in(g, edges, m2, fee_bps)?;

        let p1 = q1.amount_out as i128 - q1.amount_in as i128;
        let p2 = q2.amount_out as i128 - q2.amount_in as i128;

        // track best seen
        if p1 > best_profit {
            best_profit = p1;
            best_local = Some(q1.clone());
        }
        if p2 > best_profit {
            best_profit = p2;
            best_local = Some(q2.clone());
        }

        if p1 < p2 {
            lo = m1 + 1;
        } else {
            hi = m2.saturating_sub(1);
        }
        if lo >= hi {
            break;
        }
    }

    // Final local scan around [max(lo,1)..min(hi,cap)] to be safe
    let start = lo.saturating_sub(16).max(1);
    let end = (hi + 16).min(cap);
    for x in start..=end {
        if let Some(q) = pathquote_from_edges_exact_in(g, edges, x, fee_bps) {
            let p = q.amount_out as i128 - q.amount_in as i128;
            if p > best_profit {
                best_profit = p;
                best_local = Some(q);
            }
        }
    }

    best_local
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
