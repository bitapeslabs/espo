use anyhow::{Result, anyhow};
use std::collections::HashMap;

use crate::config::get_metashrew;
use crate::modules::ammdata::schemas::{SchemaMarketDefs, SchemaPoolSnapshot};
use crate::modules::ammdata::storage::{decode_reserves_snapshot, reserves_snapshot_key};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;

/// Fetch real-time reserves for all pools in `pools` by querying Metashrew balances:
/// - base_reserve = balance of {what = base_id} held by {who = pool_alkane_id}
/// - quote_reserve = balance of {what = quote_id} held by {who = pool_alkane_id}
///
/// Returns a snapshot map identical to your in-memory schema.
pub fn fetch_latest_reserves_for_pools(
    pools: &HashMap<SchemaAlkaneId, SchemaMarketDefs>,
) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let metashrew = get_metashrew();
    let mut out: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = HashMap::with_capacity(pools.len());

    for (pool_id, defs) in pools {
        let base_bal = metashrew
            .get_latest_reserves_for_alkane(pool_id, &defs.base_alkane_id)?
            .unwrap_or(0);
        let quote_bal = metashrew
            .get_latest_reserves_for_alkane(pool_id, &defs.quote_alkane_id)?
            .unwrap_or(0);

        eprintln!(
            "[AMMDATA-LIVE] pool {}/{} live reserves: base={}, quote={}",
            pool_id.block, pool_id.tx, base_bal, quote_bal
        );

        out.insert(
            *pool_id,
            SchemaPoolSnapshot {
                base_reserve: base_bal,
                quote_reserve: quote_bal,
                base_id: defs.base_alkane_id,
                quote_id: defs.quote_alkane_id,
            },
        );
    }

    Ok(out)
}

pub fn fetch_all_pools(mdb: &Mdb) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let pools_snapshot_bytes = mdb
        .get(reserves_snapshot_key())?
        .ok_or(anyhow!("AMMDATA ERROR: Failed to fetch all pools"))?;

    decode_reserves_snapshot(&pools_snapshot_bytes)
}
