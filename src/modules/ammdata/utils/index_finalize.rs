use crate::modules::ammdata::storage::{AmmDataProvider, GetRawValueParams, encode_pool_snapshot};
use crate::modules::ammdata::utils::activity::{
    decode_u64_be, encode_u64_be, idx_count_key, idx_count_key_group,
};
use crate::modules::ammdata::utils::index_state::IndexState;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;

pub struct FinalizeStats {
    pub candle_writes: usize,
    pub token_usd_candles: usize,
    pub token_mcusd_candles: usize,
    pub token_derived_usd_candles: usize,
    pub token_derived_mcusd_candles: usize,
    pub chart_changes: usize,
    pub token_metrics: usize,
    pub token_metrics_index: usize,
    pub token_search_index: usize,
    pub derived_metrics: usize,
    pub derived_metrics_index: usize,
    pub derived_search_index: usize,
    pub btc_usd_price: usize,
    pub btc_usd_line: usize,
    pub total_volume_amm: usize,
    pub token_volume_candles: usize,
    pub token_volume_total: usize,
    pub canonical_pools: usize,
    pub pool_name_index: usize,
    pub amm_factories: usize,
    pub factory_pools: usize,
    pub pool_factory: usize,
    pub pool_creation_info: usize,
    pub pool_creations: usize,
    pub token_pools: usize,
    pub pool_defs: usize,
    pub pool_metrics: usize,
    pub pool_metrics_index: usize,
    pub pool_lp_supply: usize,
    pub pool_details_snapshot: usize,
    pub tvl_versioned: usize,
    pub tvl_line: usize,
    pub tvl_total: usize,
    pub pool_tvl_anchor: usize,
    pub token_activity: usize,
    pub token_activity_amount: usize,
    pub token_swaps: usize,
    pub address_pool_swaps: usize,
    pub address_token_swaps: usize,
    pub address_pool_creations: usize,
    pub address_pool_mints: usize,
    pub address_pool_burns: usize,
    pub address_amm_history: usize,
    pub amm_history_all: usize,
    pub activity: usize,
    pub index_writes: usize,
}

pub struct FinalizeResult {
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
    pub stats: FinalizeStats,
    pub should_write: bool,
}

pub fn prepare_batch(provider: &AmmDataProvider, state: &mut IndexState) -> Result<FinalizeResult> {
    let table = provider.table();

    let activity_writes = std::mem::take(&mut state.activity_acc).into_writes();
    let idx_delta = state.index_acc.clone().per_pool_delta();
    let idx_group_delta = state.index_acc.clone().per_pool_group_delta();
    let mut index_writes = std::mem::take(&mut state.index_acc).into_writes();

    for ((blk_id, tx_id), delta) in idx_delta {
        let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
        let count_k_rel = idx_count_key(&pool);

        let current = if let Some(v) = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: count_k_rel.clone(),
            })?
            .value
        {
            decode_u64_be(&v).unwrap_or(0)
        } else {
            0u64
        };
        let newv = current.saturating_add(delta);

        index_writes.push((count_k_rel, encode_u64_be(newv).to_vec()));
    }
    for ((blk_id, tx_id, group), delta) in idx_group_delta {
        let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
        let count_k_rel = idx_count_key_group(&pool, group);

        let current = if let Some(v) = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: count_k_rel.clone(),
            })?
            .value
        {
            decode_u64_be(&v).unwrap_or(0)
        } else {
            0u64
        };
        let newv = current.saturating_add(delta);

        index_writes.push((count_k_rel, encode_u64_be(newv).to_vec()));
    }

    if state.token_metrics_index_new > 0 {
        let count_key = table.token_metrics_index_count_key();
        let current = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: count_key.clone(),
            })?
            .value
            .and_then(|raw| {
                if raw.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&raw);
                    Some(u64::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let updated = current.saturating_add(state.token_metrics_index_new);
        state
            .token_metrics_index_writes
            .push((count_key, updated.to_le_bytes().to_vec()));
    }

    if state.pool_metrics_index_new > 0 {
        let count_key = table.pool_metrics_index_count_key();
        let current = provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: count_key.clone(),
            })?
            .value
            .and_then(|raw| {
                if raw.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&raw);
                    Some(u64::from_le_bytes(arr))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let updated = current.saturating_add(state.pool_metrics_index_new);
        state
            .pool_metrics_index_writes
            .push((count_key, updated.to_le_bytes().to_vec()));
    }

    if !state.derived_metrics_index_new.is_empty() {
        for (quote, add) in state.derived_metrics_index_new.iter() {
            if *add == 0 {
                continue;
            }
            let count_key = table.token_derived_metrics_index_count_key(quote);
            let current = provider
                .get_raw_value(GetRawValueParams {
                    blockhash: StateAt::Latest,
                    key: count_key.clone(),
                })?
                .value
                .and_then(|raw| {
                    if raw.len() == 8 {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&raw);
                        Some(u64::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let updated = current.saturating_add(*add);
            state
                .derived_metrics_index_writes
                .push((count_key, updated.to_le_bytes().to_vec()));
        }
    }

    let reserves_cnt = state.reserves_snapshot.len();

    let c_cnt = state.candle_writes.len();
    let tc_cnt = state.token_usd_candle_writes.len();
    let tmc_cnt = state.token_mcusd_candle_writes.len();
    let tdc_cnt = state.token_derived_usd_candle_writes.len();
    let tdmc_cnt = state.token_derived_mcusd_candle_writes.len();
    let cc_cnt = state.chart_change_writes.len();
    let tm_cnt = state.token_metrics_writes.len();
    let tmi_cnt = state.token_metrics_index_writes.len();
    let tsi_cnt = state.token_search_index_writes.len();
    let tdm_cnt = state.derived_metrics_writes.len();
    let tdmi_cnt = state.derived_metrics_index_writes.len();
    let tdsi_cnt = state.derived_search_index_writes.len();
    let btc_cnt = state.btc_usd_price_writes.len();
    let btcl_cnt = state.btc_usd_line_writes.len();
    let tva_cnt = state.total_volume_amm_writes.len();
    let tvc_cnt = state.token_volume_candle_writes.len();
    let tvt_cnt = state.token_volume_total_writes.len();
    let cp_cnt = state.canonical_pool_writes.len();
    let pn_cnt = state.pool_name_index_writes.len();
    let af_cnt = state.amm_factory_writes.len();
    let fp_cnt = state.factory_pools_writes.len();
    let pf_cnt = state.pool_factory_writes.len();
    let pc_cnt = state.pool_creation_info_writes.len();
    let pcg_cnt = state.pool_creations_writes.len();
    let aps_cnt = state.address_pool_swaps_writes.len();
    let ats_cnt = state.address_token_swaps_writes.len();
    let apc_cnt = state.address_pool_creations_writes.len();
    let apm_cnt = state.address_pool_mints_writes.len();
    let apb_cnt = state.address_pool_burns_writes.len();
    let aah_cnt = state.address_amm_history_writes.len();
    let ah_cnt = state.amm_history_all_writes.len();
    let tp_cnt = state.token_pools_writes.len();
    let pd_cnt = state.pool_defs_writes.len();
    let pm_cnt = state.pool_metrics_writes.len();
    let pmi_cnt = state.pool_metrics_index_writes.len();
    let pls_cnt = state.pool_lp_supply_writes.len();
    let pds_cnt = state.pool_details_snapshot_writes.len();
    let tvl_cnt = state.tvl_versioned_writes.len();
    let tvll_cnt = state.tvl_line_writes.len();
    let tvlt_cnt = state.tvl_total_writes.len();
    let pta_cnt = state.pool_tvl_anchor_writes.len();
    let ta_cnt = state.token_activity_writes.len();
    let taa_cnt = state.token_activity_amount_writes.len();
    let ts_cnt = state.token_swaps_writes.len();
    let a_cnt = activity_writes.len();
    let i_cnt = index_writes.len();

    let should_write = !state.candle_writes.is_empty()
        || !state.token_usd_candle_writes.is_empty()
        || !state.token_mcusd_candle_writes.is_empty()
        || !state.token_derived_usd_candle_writes.is_empty()
        || !state.token_derived_mcusd_candle_writes.is_empty()
        || !state.chart_change_writes.is_empty()
        || !state.token_metrics_writes.is_empty()
        || !state.token_metrics_index_writes.is_empty()
        || !state.token_metrics_index_deletes.is_empty()
        || !state.token_search_index_writes.is_empty()
        || !state.token_search_index_deletes.is_empty()
        || !state.derived_metrics_writes.is_empty()
        || !state.derived_metrics_index_writes.is_empty()
        || !state.derived_metrics_index_deletes.is_empty()
        || !state.derived_search_index_writes.is_empty()
        || !state.derived_search_index_deletes.is_empty()
        || !state.pool_metrics_index_writes.is_empty()
        || !state.pool_metrics_index_deletes.is_empty()
        || !state.btc_usd_price_writes.is_empty()
        || !state.btc_usd_line_writes.is_empty()
        || !state.total_volume_amm_writes.is_empty()
        || !state.token_volume_candle_writes.is_empty()
        || !state.token_volume_total_writes.is_empty()
        || !state.canonical_pool_writes.is_empty()
        || !state.pool_name_index_writes.is_empty()
        || !state.amm_factory_writes.is_empty()
        || !state.factory_pools_writes.is_empty()
        || !state.pool_factory_writes.is_empty()
        || !state.pool_creation_info_writes.is_empty()
        || !state.pool_creations_writes.is_empty()
        || !state.pool_defs_writes.is_empty()
        || !state.token_pools_writes.is_empty()
        || !state.pool_metrics_writes.is_empty()
        || !state.pool_lp_supply_writes.is_empty()
        || !state.pool_details_snapshot_writes.is_empty()
        || !state.tvl_versioned_writes.is_empty()
        || !state.tvl_line_writes.is_empty()
        || !state.tvl_total_writes.is_empty()
        || !state.pool_tvl_anchor_writes.is_empty()
        || !state.token_activity_writes.is_empty()
        || !state.token_activity_amount_writes.is_empty()
        || !state.token_swaps_writes.is_empty()
        || !state.address_pool_swaps_writes.is_empty()
        || !state.address_token_swaps_writes.is_empty()
        || !state.address_pool_creations_writes.is_empty()
        || !state.address_pool_mints_writes.is_empty()
        || !state.address_pool_burns_writes.is_empty()
        || !state.address_amm_history_writes.is_empty()
        || !state.amm_history_all_writes.is_empty()
        || !activity_writes.is_empty()
        || !index_writes.is_empty()
        || reserves_cnt > 0;

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    puts.extend(std::mem::take(&mut state.candle_writes));
    puts.extend(std::mem::take(&mut state.token_usd_candle_writes));
    puts.extend(std::mem::take(&mut state.token_mcusd_candle_writes));
    puts.extend(std::mem::take(&mut state.token_derived_usd_candle_writes));
    puts.extend(std::mem::take(&mut state.token_derived_mcusd_candle_writes));
    puts.extend(std::mem::take(&mut state.chart_change_writes));
    puts.extend(std::mem::take(&mut state.token_metrics_writes));
    puts.extend(std::mem::take(&mut state.token_metrics_index_writes));
    puts.extend(std::mem::take(&mut state.token_search_index_writes));
    puts.extend(std::mem::take(&mut state.derived_metrics_writes));
    puts.extend(std::mem::take(&mut state.derived_metrics_index_writes));
    puts.extend(std::mem::take(&mut state.derived_search_index_writes));
    puts.extend(std::mem::take(&mut state.btc_usd_price_writes));
    puts.extend(std::mem::take(&mut state.btc_usd_line_writes));
    puts.extend(std::mem::take(&mut state.total_volume_amm_writes));
    puts.extend(std::mem::take(&mut state.token_volume_candle_writes));
    puts.extend(std::mem::take(&mut state.token_volume_total_writes));
    puts.extend(std::mem::take(&mut state.canonical_pool_writes));
    puts.extend(std::mem::take(&mut state.pool_name_index_writes));
    puts.extend(std::mem::take(&mut state.amm_factory_writes));
    puts.extend(std::mem::take(&mut state.factory_pools_writes));
    puts.extend(std::mem::take(&mut state.pool_factory_writes));
    puts.extend(std::mem::take(&mut state.pool_creation_info_writes));
    puts.extend(std::mem::take(&mut state.pool_creations_writes));
    puts.extend(std::mem::take(&mut state.pool_defs_writes));
    puts.extend(std::mem::take(&mut state.token_pools_writes));
    puts.extend(std::mem::take(&mut state.pool_metrics_writes));
    puts.extend(std::mem::take(&mut state.pool_metrics_index_writes));
    puts.extend(std::mem::take(&mut state.pool_lp_supply_writes));
    puts.extend(std::mem::take(&mut state.pool_details_snapshot_writes));
    puts.extend(std::mem::take(&mut state.tvl_versioned_writes));
    puts.extend(std::mem::take(&mut state.tvl_line_writes));
    puts.extend(std::mem::take(&mut state.tvl_total_writes));
    puts.extend(std::mem::take(&mut state.pool_tvl_anchor_writes));
    puts.extend(std::mem::take(&mut state.token_activity_writes));
    puts.extend(std::mem::take(&mut state.token_activity_amount_writes));
    puts.extend(std::mem::take(&mut state.token_swaps_writes));
    puts.extend(std::mem::take(&mut state.address_pool_swaps_writes));
    puts.extend(std::mem::take(&mut state.address_token_swaps_writes));
    puts.extend(std::mem::take(&mut state.address_pool_creations_writes));
    puts.extend(std::mem::take(&mut state.address_pool_mints_writes));
    puts.extend(std::mem::take(&mut state.address_pool_burns_writes));
    puts.extend(std::mem::take(&mut state.address_amm_history_writes));
    puts.extend(std::mem::take(&mut state.amm_history_all_writes));
    puts.extend(activity_writes);
    puts.extend(index_writes);
    for (pool, snapshot) in &state.reserves_snapshot {
        if let Ok(enc) = encode_pool_snapshot(snapshot) {
            puts.push((table.reserves_snapshot_pool_key(pool), enc));
        }
    }

    let mut deletes: Vec<Vec<u8>> = Vec::new();
    deletes.extend(std::mem::take(&mut state.token_metrics_index_deletes));
    deletes.extend(std::mem::take(&mut state.token_search_index_deletes));
    deletes.extend(std::mem::take(&mut state.derived_metrics_index_deletes));
    deletes.extend(std::mem::take(&mut state.derived_search_index_deletes));
    deletes.extend(std::mem::take(&mut state.pool_metrics_index_deletes));

    let stats = FinalizeStats {
        candle_writes: c_cnt,
        token_usd_candles: tc_cnt,
        token_mcusd_candles: tmc_cnt,
        token_derived_usd_candles: tdc_cnt,
        token_derived_mcusd_candles: tdmc_cnt,
        chart_changes: cc_cnt,
        token_metrics: tm_cnt,
        token_metrics_index: tmi_cnt,
        token_search_index: tsi_cnt,
        derived_metrics: tdm_cnt,
        derived_metrics_index: tdmi_cnt,
        derived_search_index: tdsi_cnt,
        btc_usd_price: btc_cnt,
        btc_usd_line: btcl_cnt,
        total_volume_amm: tva_cnt,
        token_volume_candles: tvc_cnt,
        token_volume_total: tvt_cnt,
        canonical_pools: cp_cnt,
        pool_name_index: pn_cnt,
        amm_factories: af_cnt,
        factory_pools: fp_cnt,
        pool_factory: pf_cnt,
        pool_creation_info: pc_cnt,
        pool_creations: pcg_cnt,
        token_pools: tp_cnt,
        pool_defs: pd_cnt,
        pool_metrics: pm_cnt,
        pool_metrics_index: pmi_cnt,
        pool_lp_supply: pls_cnt,
        pool_details_snapshot: pds_cnt,
        tvl_versioned: tvl_cnt,
        tvl_line: tvll_cnt,
        tvl_total: tvlt_cnt,
        pool_tvl_anchor: pta_cnt,
        token_activity: ta_cnt,
        token_activity_amount: taa_cnt,
        token_swaps: ts_cnt,
        address_pool_swaps: aps_cnt,
        address_token_swaps: ats_cnt,
        address_pool_creations: apc_cnt,
        address_pool_mints: apm_cnt,
        address_pool_burns: apb_cnt,
        address_amm_history: aah_cnt,
        amm_history_all: ah_cnt,
        activity: a_cnt,
        index_writes: i_cnt,
    };

    Ok(FinalizeResult { puts, deletes, stats, should_write })
}
