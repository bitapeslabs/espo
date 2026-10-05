//! The full route of a swap, reassembled from the per-pool legs it left behind.
//!
//! Activity is recorded per pool: a swap routed through three pools is three
//! independent rows that share only a txid, and a token's activity feed holds just
//! the rows whose pool contains that token. So a multi-hop buy shows up in the
//! bought token's feed as its last leg alone, priced in the intermediate asset,
//! and an arbitrage cycle through a token shows up there as an unrelated buy and
//! sell. This module puts the legs of one transaction back together.
//!
//! Everything here is pure: it takes legs and returns a route. Finding the legs is
//! the caller's job (see `AmmDataProvider::trade_legs_at_ts`).

use crate::schemas::SchemaAlkaneId;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

/// One pool's part in a transaction. Deltas are the pool's: positive is what the
/// pool took in, negative is what it paid out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteLeg {
    pub pool: SchemaAlkaneId,
    pub base: SchemaAlkaneId,
    pub quote: SchemaAlkaneId,
    pub base_delta: i128,
    pub quote_delta: i128,
    pub seq: u32,
}

impl RouteLeg {
    /// `(token paid in, amount, token paid out, amount)` from the trader's side, or
    /// `None` if this is not a swap (both sides moved the same way).
    pub fn flow(&self) -> Option<(SchemaAlkaneId, u128, SchemaAlkaneId, u128)> {
        match (self.base_delta.signum(), self.quote_delta.signum()) {
            (1, -1) => Some((
                self.base,
                self.base_delta.unsigned_abs(),
                self.quote,
                self.quote_delta.unsigned_abs(),
            )),
            (-1, 1) => Some((
                self.quote,
                self.quote_delta.unsigned_abs(),
                self.base,
                self.base_delta.unsigned_abs(),
            )),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// Legs in the order the swap flowed through them, as far as that can be told.
    pub legs: Vec<RouteLeg>,
    /// What the trader ended up with per token across the whole transaction:
    /// negative is paid, positive is received. Tokens that net to zero are left out.
    pub net: BTreeMap<SchemaAlkaneId, i128>,
    /// Tokens that were only passed through: received by one leg and spent by the
    /// next, netting to exactly zero.
    pub via: Vec<SchemaAlkaneId>,
}

/// Order a transaction's legs into a route and total what the trader paid and got.
///
/// Legs carry no execution order of their own, so it is recovered by following the
/// tokens: a leg that pays out X is followed by a leg that takes X in. The start is
/// a leg whose input no other leg produced.
///
/// A cycle has no such leg, and balance changes alone cannot say where it really
/// began - every rotation of it leaves the same deltas. So the start is a
/// convention: the token the trader net paid; failing that the token they came out
/// ahead in, so an arbitrage reads "spent X, got back more X"; failing that the
/// lowest pool id. Legs that do not chain (a split route) follow in pool order.
pub fn build_route(mut legs: Vec<RouteLeg>) -> Route {
    legs.retain(|leg| leg.flow().is_some());
    legs.sort_by_key(|leg| (leg.pool.block, leg.pool.tx, leg.seq));

    let mut totals: BTreeMap<SchemaAlkaneId, i128> = BTreeMap::new();
    for leg in legs.iter() {
        // The trader's side is the mirror of the pool's.
        *totals.entry(leg.base).or_insert(0) -= leg.base_delta;
        *totals.entry(leg.quote).or_insert(0) -= leg.quote_delta;
    }
    let via: Vec<SchemaAlkaneId> =
        totals.iter().filter(|(_, v)| **v == 0).map(|(k, _)| *k).collect();
    let net: BTreeMap<SchemaAlkaneId, i128> =
        totals.iter().filter(|(_, v)| **v != 0).map(|(k, v)| (*k, *v)).collect();

    let flows: Vec<(SchemaAlkaneId, SchemaAlkaneId)> = legs
        .iter()
        .map(|leg| leg.flow().map(|(i, _, o, _)| (i, o)).expect("retained"))
        .collect();
    let mut used = vec![false; legs.len()];
    let mut order: Vec<usize> = Vec::with_capacity(legs.len());

    let pick_start = |used: &[bool]| -> Option<usize> {
        let free: Vec<usize> = (0..flows.len()).filter(|i| !used[*i]).collect();
        // A leg whose input nothing else (still unplaced) produces.
        free.iter()
            .copied()
            .find(|i| !free.iter().any(|j| j != i && flows[*j].1 == flows[*i].0))
            // In a cycle: the leg spending what the trader net paid...
            .or_else(|| {
                free.iter().copied().find(|i| net.get(&flows[*i].0).is_some_and(|v| *v < 0))
            })
            // ...or, for a cycle that only made a profit, the leg spending the
            // token it came out ahead in, so the route reads "spent X, got back
            // more X" and ends on the token reported as the net result.
            .or_else(|| {
                free.iter().copied().find(|i| net.get(&flows[*i].0).is_some_and(|v| *v > 0))
            })
            .or_else(|| free.first().copied())
    };

    while let Some(start) = pick_start(&used) {
        used[start] = true;
        order.push(start);
        let mut holding = flows[start].1;
        while let Some(next) = (0..flows.len()).find(|i| !used[*i] && flows[*i].0 == holding) {
            used[next] = true;
            order.push(next);
            holding = flows[next].1;
        }
    }

    let ordered = order.into_iter().map(|i| legs[i].clone()).collect();
    Route { legs: ordered, net, via }
}

fn id_str(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

/// JSON for a route, as attached to an activity row.
///
/// `token_in`/`amount_in` are set when the trader net paid exactly one token, and
/// `token_out`/`amount_out` when they net received exactly one - independently, so
/// an arbitrage cycle that only yields a profit reports an out and no in.
/// `pass_through` says the token whose feed this row is in was only a hop.
pub fn route_json(route: &Route, queried: &SchemaAlkaneId) -> Value {
    let legs: Vec<Value> = route
        .legs
        .iter()
        .filter_map(|leg| {
            let (token_in, amount_in, token_out, amount_out) = leg.flow()?;
            Some(json!({
                "pool": id_str(&leg.pool),
                "base": id_str(&leg.base),
                "quote": id_str(&leg.quote),
                "token_in": id_str(&token_in),
                "amount_in": amount_in.to_string(),
                "token_out": id_str(&token_out),
                "amount_out": amount_out.to_string(),
            }))
        })
        .collect();

    let mut net = Map::new();
    for (token, amount) in route.net.iter() {
        net.insert(id_str(token), json!(amount.to_string()));
    }

    let paid: Vec<(&SchemaAlkaneId, &i128)> = route.net.iter().filter(|(_, v)| **v < 0).collect();
    let got: Vec<(&SchemaAlkaneId, &i128)> = route.net.iter().filter(|(_, v)| **v > 0).collect();
    let (token_in, amount_in) = match paid.as_slice() {
        [(token, amount)] => (json!(id_str(token)), json!(amount.unsigned_abs().to_string())),
        _ => (Value::Null, Value::Null),
    };
    let (token_out, amount_out) = match got.as_slice() {
        [(token, amount)] => (json!(id_str(token)), json!(amount.unsigned_abs().to_string())),
        _ => (Value::Null, Value::Null),
    };

    json!({
        "hops": legs.len(),
        "legs": legs,
        "via": route.via.iter().map(id_str).collect::<Vec<_>>(),
        "net": Value::Object(net),
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
        "pass_through": route.via.contains(queried),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRBTC: SchemaAlkaneId = SchemaAlkaneId { block: 32, tx: 0 };
    const DIESEL: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 0 };
    const TORT: SchemaAlkaneId = SchemaAlkaneId { block: 2, tx: 68479 };

    fn leg(
        pool_tx: u64,
        base: SchemaAlkaneId,
        quote: SchemaAlkaneId,
        base_delta: i128,
        quote_delta: i128,
    ) -> RouteLeg {
        RouteLeg {
            pool: SchemaAlkaneId { block: 2, tx: pool_tx },
            base,
            quote,
            base_delta,
            quote_delta,
            seq: 0,
        }
    }

    #[test]
    fn a_single_hop_is_a_route_of_one() {
        // Pool TORT/frBTC takes 1000 sats, pays 50 TORT.
        let route = build_route(vec![leg(1, TORT, FRBTC, -50, 1_000)]);
        assert_eq!(route.legs.len(), 1);
        assert!(route.via.is_empty());

        let v = route_json(&route, &TORT);
        assert_eq!(v["hops"], json!(1));
        assert_eq!(v["token_in"], json!("32:0"));
        assert_eq!(v["amount_in"], json!("1000"));
        assert_eq!(v["token_out"], json!("2:68479"));
        assert_eq!(v["amount_out"], json!("50"));
        assert_eq!(v["pass_through"], json!(false));
    }

    #[test]
    fn a_multi_hop_buy_reports_what_was_really_paid() {
        // frBTC -> DIESEL -> TORT, given out of order and with the second pool
        // carrying TORT on its quote side.
        let route = build_route(vec![
            leg(9, DIESEL, TORT, 400, -7_000), // pool DIESEL/TORT: takes DIESEL, pays TORT
            leg(3, DIESEL, FRBTC, -400, 1_000), // pool DIESEL/frBTC: takes frBTC, pays DIESEL
        ]);

        // Ordered by following the tokens, not by pool id.
        assert_eq!(route.legs[0].pool.tx, 3);
        assert_eq!(route.legs[1].pool.tx, 9);
        assert_eq!(route.via, vec![DIESEL]);

        // TORT's feed only holds the last leg, which says the buyer paid DIESEL.
        // The route says what they actually paid.
        let v = route_json(&route, &TORT);
        assert_eq!(v["hops"], json!(2));
        assert_eq!(v["via"], json!(["2:0"]));
        assert_eq!(v["token_in"], json!("32:0"));
        assert_eq!(v["amount_in"], json!("1000"));
        assert_eq!(v["token_out"], json!("2:68479"));
        assert_eq!(v["amount_out"], json!("7000"));
        assert_eq!(v["pass_through"], json!(false));
        assert_eq!(v["legs"][0]["token_in"], json!("32:0"));
        assert_eq!(v["legs"][1]["token_out"], json!("2:68479"));

        // The same route seen from the intermediate token's feed.
        assert_eq!(route_json(&route, &DIESEL)["pass_through"], json!(true));
    }

    #[test]
    fn an_arbitrage_cycle_is_a_pass_through_with_only_a_profit() {
        // The shape of mainnet tx bb0a04df..: frBTC -> TORT -> DIESEL -> frBTC,
        // ending with a little more DIESEL than it spent.
        let route = build_route(vec![
            leg(77269, TORT, FRBTC, -168_075_379_605, 21_470),
            leg(70020, DIESEL, TORT, -49_772_500, 168_075_379_605),
            leg(77087, DIESEL, FRBTC, 48_334_811, -21_470),
        ]);
        assert_eq!(route.legs.len(), 3);
        // frBTC and TORT both net to zero; DIESEL is the profit.
        assert_eq!(route.via, vec![TORT, FRBTC]);
        assert_eq!(route.net.get(&DIESEL), Some(&1_437_689));

        let v = route_json(&route, &TORT);
        assert_eq!(v["pass_through"], json!(true));
        assert_eq!(v["token_in"], Value::Null);
        assert_eq!(v["token_out"], json!("2:0"));
        assert_eq!(v["amount_out"], json!("1437689"));
        // Each leg hands its output to the next.
        let legs = v["legs"].as_array().unwrap();
        for pair in legs.windows(2) {
            assert_eq!(pair[0]["token_out"], pair[1]["token_in"]);
        }
        // A cycle has no real start, so it is a convention: begin and end on the
        // token the trader came out ahead in.
        assert_eq!(legs[0]["token_in"], json!("2:0"));
        assert_eq!(legs[2]["token_out"], json!("2:0"));
        assert_eq!(legs[0]["pool"], json!("2:77087"));
    }

    #[test]
    fn a_split_route_totals_both_paths() {
        // frBTC -> TORT through two different pools in one transaction.
        let route =
            build_route(vec![leg(1, TORT, FRBTC, -50, 1_000), leg(2, TORT, FRBTC, -30, 600)]);
        assert!(route.via.is_empty());
        let v = route_json(&route, &TORT);
        assert_eq!(v["hops"], json!(2));
        assert_eq!(v["amount_in"], json!("1600"));
        assert_eq!(v["amount_out"], json!("80"));
    }

    #[test]
    fn liquidity_legs_are_not_part_of_a_route() {
        // Both sides moving the same way is an add or a remove, not a swap.
        let route = build_route(vec![leg(1, TORT, FRBTC, 50, 1_000), leg(2, TORT, FRBTC, -5, 100)]);
        assert_eq!(route.legs.len(), 1);
        assert_eq!(route.legs[0].pool.tx, 2);
    }
}
