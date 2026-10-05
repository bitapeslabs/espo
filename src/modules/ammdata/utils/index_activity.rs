use crate::modules::ammdata::consts::CanonicalQuoteUnit;
use crate::modules::ammdata::schemas::{ActivityDirection, ActivityKind, Timeframe};
use crate::modules::ammdata::storage::AmmDataProvider;
use crate::modules::ammdata::utils::candles::{
    bucket_start_for, price_base_per_quote, price_quote_per_base,
};
use crate::modules::ammdata::utils::index_state::IndexState;
use crate::modules::essentials::storage::EssentialsProvider;
use crate::schemas::SchemaAlkaneId;
use bitcoin::Txid;
use bitcoin::hashes::Hash;
use std::collections::HashMap;

pub fn process_balance_deltas(
    block_ts: u64,
    height: u32,
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
    canonical_quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    frames: &[Timeframe],
    token_volume_frames: &[Timeframe],
    tx_meta: &HashMap<Txid, (Vec<u8>, bool)>,
    state: &mut IndexState,
) {
    let table = provider.table();
    let balance_txs = match crate::modules::ammdata::load_balance_txs_by_height(essentials, height)
    {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[AMMDATA] failed to load balance txs for height {height}: {e:?}");
            std::collections::BTreeMap::new()
        }
    };

    // Apply balance deltas per pool and emit activity + candles.
    for (owner, entries) in balance_txs {
        let Some(defs) = state.pools_map.get(&owner) else { continue };
        let Some(snapshot) = state.reserves_snapshot.get_mut(&owner) else { continue };

        for entry in entries {
            let base_delta =
                crate::modules::ammdata::signed_from_delta(entry.outflow.get(&defs.base_alkane_id));
            let quote_delta = crate::modules::ammdata::signed_from_delta(
                entry.outflow.get(&defs.quote_alkane_id),
            );
            if base_delta == 0 && quote_delta == 0 {
                continue;
            }

            let prev_base = snapshot.base_reserve;
            let prev_quote = snapshot.quote_reserve;
            let new_base = crate::modules::ammdata::apply_delta_u128(prev_base, base_delta);
            let new_quote = crate::modules::ammdata::apply_delta_u128(prev_quote, quote_delta);
            snapshot.base_reserve = new_base;
            snapshot.quote_reserve = new_quote;

            let (kind, direction) = match (base_delta.signum(), quote_delta.signum()) {
                (1, -1) => (ActivityKind::TradeSell, Some(ActivityDirection::BaseIn)),
                (-1, 1) => (ActivityKind::TradeBuy, Some(ActivityDirection::QuoteIn)),
                (1, 1) => (ActivityKind::LiquidityAdd, None),
                (-1, -1) => (ActivityKind::LiquidityRemove, None),
                _ => continue,
            };
            state.pools_touched.insert(owner);

            let txid = Txid::from_byte_array(entry.txid);
            let (address_spk, success) =
                tx_meta.get(&txid).cloned().unwrap_or_else(|| (Vec::new(), true));
            let address_spk = address_spk.clone();

            let activity = crate::modules::ammdata::schemas::SchemaActivityV1 {
                timestamp: block_ts,
                txid: entry.txid,
                kind,
                direction,
                base_delta,
                quote_delta,
                address_spk: address_spk.clone(),
                success,
            };

            if let Ok(seq) = state.activity_acc.push(owner, block_ts, activity.clone()) {
                state.index_acc.add(&owner, block_ts, seq, &activity);
                let base_amount = activity.base_delta.unsigned_abs();
                let quote_amount = activity.quote_delta.unsigned_abs();
                // This path only processes concrete pool defs from pools_map; synthetic
                // chart ids such as "-usd" and "-derived_*" never enter this index.
                state.token_activity_writes.push((
                    table.token_activity_key(&defs.base_alkane_id, block_ts, seq, kind, &owner),
                    Vec::new(),
                ));
                state.token_activity_amount_writes.push((
                    table.token_activity_amount_key(
                        &defs.base_alkane_id,
                        base_amount,
                        block_ts,
                        seq,
                        kind,
                        &owner,
                    ),
                    Vec::new(),
                ));
                if defs.quote_alkane_id != defs.base_alkane_id {
                    state.token_activity_writes.push((
                        table.token_activity_key(
                            &defs.quote_alkane_id,
                            block_ts,
                            seq,
                            kind,
                            &owner,
                        ),
                        Vec::new(),
                    ));
                    state.token_activity_amount_writes.push((
                        table.token_activity_amount_key(
                            &defs.quote_alkane_id,
                            quote_amount,
                            block_ts,
                            seq,
                            kind,
                            &owner,
                        ),
                        Vec::new(),
                    ));
                }
                if matches!(kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
                    state.token_swaps_writes.push((
                        table.token_swaps_key(&defs.base_alkane_id, block_ts, seq, &owner),
                        Vec::new(),
                    ));
                    state.token_swaps_writes.push((
                        table.token_swaps_key(&defs.quote_alkane_id, block_ts, seq, &owner),
                        Vec::new(),
                    ));
                    if !address_spk.is_empty() {
                        state.address_pool_swaps_writes.push((
                            table.address_pool_swaps_key(&address_spk, &owner, block_ts, seq),
                            Vec::new(),
                        ));
                        state.address_token_swaps_writes.push((
                            table.address_token_swaps_key(
                                &address_spk,
                                &defs.base_alkane_id,
                                block_ts,
                                seq,
                                &owner,
                            ),
                            Vec::new(),
                        ));
                        state.address_token_swaps_writes.push((
                            table.address_token_swaps_key(
                                &address_spk,
                                &defs.quote_alkane_id,
                                block_ts,
                                seq,
                                &owner,
                            ),
                            Vec::new(),
                        ));
                    }
                }
                if !address_spk.is_empty() {
                    match kind {
                        ActivityKind::LiquidityAdd => {
                            state.address_pool_mints_writes.push((
                                table.address_pool_mints_key(&address_spk, block_ts, seq, &owner),
                                Vec::new(),
                            ));
                        }
                        ActivityKind::LiquidityRemove => {
                            state.address_pool_burns_writes.push((
                                table.address_pool_burns_key(&address_spk, block_ts, seq, &owner),
                                Vec::new(),
                            ));
                        }
                        _ => {}
                    }
                }
                state
                    .amm_history_all_writes
                    .push((table.amm_history_all_key(block_ts, seq, kind, &owner), Vec::new()));
                if !address_spk.is_empty() {
                    state.address_amm_history_writes.push((
                        table.address_amm_history_key(&address_spk, block_ts, seq, kind, &owner),
                        Vec::new(),
                    ));
                }
            }

            if matches!(kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
                state.has_trades = true;
                let base_abs = crate::modules::ammdata::abs_i128(base_delta);
                let quote_abs = crate::modules::ammdata::abs_i128(quote_delta);
                let entry = state.in_block_trade_volumes.entry(owner).or_insert((0, 0));
                entry.0 = entry.0.saturating_add(base_abs);
                entry.1 = entry.1.saturating_add(quote_abs);
                let p_q_per_b = price_quote_per_base(new_base, new_quote);
                let p_b_per_q = price_base_per_quote(new_base, new_quote);
                let base_volume = base_abs;
                let quote_volume = quote_abs;

                state.candle_cache.apply_trade_for_frames(
                    block_ts,
                    owner,
                    frames,
                    p_b_per_q,
                    p_q_per_b,
                    base_volume,
                    quote_volume,
                );
                state.token_volume_cache.apply_trade_for_frames(
                    block_ts,
                    defs.base_alkane_id,
                    token_volume_frames,
                    base_volume,
                );
                if defs.quote_alkane_id != defs.base_alkane_id {
                    state.token_volume_cache.apply_trade_for_frames(
                        block_ts,
                        defs.quote_alkane_id,
                        token_volume_frames,
                        quote_volume,
                    );
                }

                // The pool's own candles, volume and activity above are recorded
                // either way; only a pool from a trusted factory re-prices a token.
                let prices_tokens = !state.pricing_excluded_pools.contains(&owner);
                if prices_tokens && canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                    let entry =
                        state.canonical_trade_buckets.entry(defs.base_alkane_id).or_default();
                    for tf in frames {
                        entry.insert((*tf, bucket_start_for(block_ts, *tf)));
                    }
                }
                if prices_tokens && canonical_quote_units.contains_key(&defs.base_alkane_id) {
                    let entry =
                        state.canonical_trade_buckets.entry(defs.quote_alkane_id).or_default();
                    for tf in frames {
                        entry.insert((*tf, bucket_start_for(block_ts, *tf)));
                    }
                }
            }
        }
    }
}
