use crate::alkanes::trace::EspoBlock;
use crate::config::{debug_enabled, get_espo_module_mdb};
use crate::debug;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationIdsInBlockParams, GetCreationRecordsByIdParams,
    GetIndexHeightParams as EssentialsGetIndexHeightParams,
};
use crate::modules::essentials::utils::names::normalize_alkane_name;
use crate::runtime::mdb::Mdb;
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::consts::PRIORITY_SERIES_ALKANES;
use super::rpc;
use super::storage::{
    GetIndexHeightParams as PizzafunGetIndexHeightParams, GetSeriesEntriesByNameParams,
    PizzafunProvider, SeriesEntry, series_id_base_from_name,
};

fn parse_alkane_id_str(s: &str) -> Option<SchemaAlkaneId> {
    let (block_raw, tx_raw) = s.split_once(':')?;
    let parse_u32 = |v: &str| {
        if let Some(hex) = v.strip_prefix("0x") {
            u32::from_str_radix(hex, 16).ok()
        } else {
            v.parse::<u32>().ok()
        }
    };
    let parse_u64 = |v: &str| {
        if let Some(hex) = v.strip_prefix("0x") {
            u64::from_str_radix(hex, 16).ok()
        } else {
            v.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(block_raw)?, tx: parse_u64(tx_raw)? })
}

pub struct Pizzafun {
    essentials_provider: Option<Arc<EssentialsProvider>>,
    provider: Option<Arc<PizzafunProvider>>,
}

impl Pizzafun {
    pub fn new() -> Self {
        Self { essentials_provider: None, provider: None }
    }

    #[inline]
    fn essentials_provider(&self) -> &EssentialsProvider {
        self.essentials_provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    #[inline]
    fn provider(&self) -> &PizzafunProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    fn load_essentials_index_height(&self) -> Option<u32> {
        let resp = self
            .essentials_provider()
            .get_index_height(EssentialsGetIndexHeightParams { blockhash: StateAt::Latest })
            .ok()?;
        resp.height
    }

    fn load_index_height(&self) -> Option<u32> {
        if let Ok(resp) = self
            .provider()
            .get_index_height(PizzafunGetIndexHeightParams { blockhash: StateAt::Latest })
        {
            if resp.height.is_some() {
                return resp.height;
            }
        }
        self.load_essentials_index_height()
    }

    fn priority_index_map() -> HashMap<SchemaAlkaneId, usize> {
        let mut priority_index: HashMap<SchemaAlkaneId, usize> = HashMap::new();
        for (idx, raw) in PRIORITY_SERIES_ALKANES.iter().enumerate() {
            if let Some(id) = parse_alkane_id_str(raw) {
                priority_index.entry(id).or_insert(idx);
            }
        }
        priority_index
    }

    fn sort_series_entries(
        entries: &mut Vec<SeriesEntry>,
        priority_index: &HashMap<SchemaAlkaneId, usize>,
    ) {
        entries.sort_by(|a, b| {
            let a_pri = priority_index.get(&a.alkane_id);
            let b_pri = priority_index.get(&b.alkane_id);
            match (a_pri, b_pri) {
                (Some(ai), Some(bi)) => ai
                    .cmp(bi)
                    .then_with(|| a.creation_height.cmp(&b.creation_height))
                    .then_with(|| a.alkane_id.cmp(&b.alkane_id)),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => a
                    .creation_height
                    .cmp(&b.creation_height)
                    .then_with(|| a.alkane_id.cmp(&b.alkane_id)),
            }
        });
    }
}

impl Default for Pizzafun {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Pizzafun {
    fn get_name(&self) -> &'static str {
        "pizzafun"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let essentials_provider =
            Arc::new(EssentialsProvider::new(get_espo_module_mdb("essentials")));
        self.essentials_provider = Some(essentials_provider);
        self.provider = Some(Arc::new(PizzafunProvider::new(mdb)));
        eprintln!("[PIZZAFUN] loaded index height: {:?}", self.load_index_height());
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        crate::modules::essentials::consts::essentials_genesis_block(network)
    }

    fn get_mdb(&self) -> Option<Arc<Mdb>> {
        self.provider.as_ref().map(|provider| Arc::new(provider.mdb().clone()))
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let block_hash = block.block_header.block_hash();

        let timer = debug::start_if(debug);
        let mut new_alkanes = self
            .essentials_provider()
            .get_creation_ids_in_block(GetCreationIdsInBlockParams {
                blockhash: StateAt::Block(block_hash),
                height: block.height,
            })?
            .alkanes;
        let mut seen: HashSet<SchemaAlkaneId> = HashSet::new();
        new_alkanes.retain(|a| seen.insert(*a));
        debug::log_elapsed(module, "collect_created_alkanes", timer);

        let timer = debug::start_if(debug);
        if !new_alkanes.is_empty() {
            let records = self
                .essentials_provider()
                .get_creation_records_by_id(GetCreationRecordsByIdParams {
                    blockhash: StateAt::Block(block_hash),
                    alkanes: new_alkanes,
                })?
                .records;

            let mut by_name: HashMap<String, Vec<SeriesEntry>> = HashMap::new();
            for rec in records.into_iter().flatten() {
                let Some(raw_name) = rec.names.first() else { continue };
                let Some(name_norm) = normalize_alkane_name(raw_name) else { continue };
                by_name.entry(name_norm).or_default().push(SeriesEntry {
                    series_id: String::new(),
                    alkane_id: rec.alkane,
                    creation_height: rec.creation_height,
                });
            }

            if !by_name.is_empty() {
                let priority_index = Self::priority_index_map();
                for (name, mut new_entries) in by_name {
                    let Some(series_base) = series_id_base_from_name(&name) else { continue };
                    let existing = self.provider().get_series_entries_by_name(
                        GetSeriesEntriesByNameParams {
                            blockhash: StateAt::Block(block_hash),
                            name_norm: name.clone(),
                        },
                    )?;
                    if !existing.is_empty() {
                        let mut existing_ids: HashSet<SchemaAlkaneId> =
                            existing.iter().map(|e| e.alkane_id).collect();
                        new_entries.retain(|e| existing_ids.insert(e.alkane_id));
                    }
                    if new_entries.is_empty() {
                        continue;
                    }

                    let mut combined = existing.clone();
                    combined.extend(new_entries);
                    Self::sort_series_entries(&mut combined, &priority_index);

                    let mut updated: Vec<SeriesEntry> = Vec::with_capacity(combined.len());
                    for (idx, entry) in combined.into_iter().enumerate() {
                        let series_id = if idx == 0 {
                            series_base.clone()
                        } else {
                            format!("{}-{}", series_base, idx + 1)
                        };
                        updated.push(SeriesEntry {
                            series_id,
                            alkane_id: entry.alkane_id,
                            creation_height: entry.creation_height,
                        });
                    }

                    self.provider().update_series_for_name(&existing, &updated)?;
                }
            }
        }
        debug::log_elapsed(module, "update_series_index", timer);

        let timer = debug::start_if(debug);
        self.provider().set_index_height(super::storage::SetIndexHeightParams {
            blockhash: StateAt::Latest,
            height: block.height,
        })?;
        debug::log_elapsed(module, "store_height", timer);

        let timer = debug::start_if(debug);
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        debug::log_elapsed(module, "finalize", timer);
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        self.load_index_height()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let provider = self.provider.as_ref().expect("ModuleRegistry must call set_mdb()");
        rpc::register_rpc(reg.clone(), Arc::clone(provider));
    }
}
