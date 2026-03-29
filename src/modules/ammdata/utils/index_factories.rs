use crate::alkanes::trace::EspoBlock;
use crate::modules::ammdata::storage::{AmmDataProvider, GetAmmFactoriesParams};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationIdsInBlockParams, GetCreationRecordParams,
};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use std::collections::{HashMap, HashSet};

fn lookup_proxy_target_cached(
    blockhash: StateAt,
    essentials: &EssentialsProvider,
    cache: &mut HashMap<SchemaAlkaneId, Option<SchemaAlkaneId>>,
    alkane: SchemaAlkaneId,
) -> Option<SchemaAlkaneId> {
    if let Some(value) = cache.get(&alkane) {
        return *value;
    }
    let value = crate::modules::ammdata::lookup_proxy_target(blockhash, essentials, alkane);
    cache.insert(alkane, value);
    value
}

pub fn prepare_factories(
    block: &EspoBlock,
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
) -> Result<(HashSet<SchemaAlkaneId>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let table = provider.table();
    let blockhash = StateAt::Block(block.block_header.block_hash());
    let mut amm_factories: HashSet<SchemaAlkaneId> = provider
        .get_amm_factories(GetAmmFactoriesParams { blockhash: blockhash.clone() })
        .map(|res| res.factories.into_iter().collect())
        .unwrap_or_default();
    let mut amm_factory_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut proxy_target_cache: HashMap<SchemaAlkaneId, Option<SchemaAlkaneId>> = HashMap::new();
    let created_alkanes = essentials
        .get_creation_ids_in_block(GetCreationIdsInBlockParams {
            blockhash: blockhash.clone(),
            height: block.height,
        })
        .map(|res| res.alkanes)
        .unwrap_or_default();

    for alk in created_alkanes {
        if amm_factories.contains(&alk) {
            continue;
        }
        let mut is_factory = false;
        if let Ok(resp) = essentials.get_creation_record(GetCreationRecordParams {
            blockhash: blockhash.clone(),
            alkane: alk,
        }) {
            if let Some(rec) = resp.record {
                if let Some(inspection) = rec.inspection.as_ref() {
                    if crate::modules::ammdata::inspection_is_amm_factory(inspection) {
                        is_factory = true;
                    }
                    if let Some(factory_id) = inspection.factory_alkane {
                        if !amm_factories.contains(&factory_id) {
                            amm_factories.insert(factory_id);
                            amm_factory_writes
                                .push((table.amm_factory_key(&factory_id), Vec::new()));
                        }
                    }
                }
            }
        }
        if !is_factory {
            if let Some(proxy_target) = lookup_proxy_target_cached(
                blockhash.clone(),
                essentials,
                &mut proxy_target_cache,
                alk,
            ) {
                if let Ok(resp) = essentials.get_creation_record(GetCreationRecordParams {
                    blockhash: blockhash.clone(),
                    alkane: proxy_target,
                }) {
                    if let Some(rec) = resp.record {
                        if let Some(inspection) = rec.inspection.as_ref() {
                            if crate::modules::ammdata::inspection_is_amm_factory(inspection) {
                                is_factory = true;
                            }
                        }
                    }
                }
            }
        }
        if is_factory {
            amm_factories.insert(alk);
            amm_factory_writes.push((table.amm_factory_key(&alk), Vec::new()));
        }
    }

    Ok((amm_factories, amm_factory_writes))
}
