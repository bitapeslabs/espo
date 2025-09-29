use anyhow::{Result, anyhow};
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::collections::HashMap;

use crate::config::get_metashrew_sdb;
use crate::modules::ammdata::schemas::{SchemaMarketDefs, SchemaPoolSnapshot};
use crate::modules::ammdata::storage::{decode_reserves_snapshot, reserves_snapshot_key};
use crate::runtime::mdb::Mdb;
use crate::runtime::sdb::SDB;
use crate::schemas::SchemaAlkaneId;

const HEX_ALKANES: &[u8] = b"/alkanes/";
const HEX_BALANCES: &[u8] = b"/balances/";

#[inline]
fn u128_to_le16(x: u128) -> [u8; 16] {
    x.to_le_bytes()
}

/// Encode AlkaneId as RAW32-LE: 16B block (u128 LE) || 16B tx (u128 LE)
#[inline]
fn enc_alkaneid_raw32(id: &SchemaAlkaneId) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&u128_to_le16(id.block as u128));
    out[16..].copy_from_slice(&u128_to_le16(id.tx as u128));
    out
}

/// Build `/alkanes/{WHAT}/balances/{WHO}` *prefix* (no slot yet)
#[inline]
fn balance_prefix(what: &SchemaAlkaneId, who: &SchemaAlkaneId) -> Vec<u8> {
    let what32 = enc_alkaneid_raw32(what);
    let who32 = enc_alkaneid_raw32(who);
    let mut v = Vec::with_capacity(9 + 32 + 10 + 32);
    v.extend_from_slice(HEX_ALKANES);
    v.extend_from_slice(&what32);
    v.extend_from_slice(HEX_BALANCES);
    v.extend_from_slice(&who32);
    v
}

/// Compute an exclusive upper bound for a prefix (prefix++ in lexicographic byte order)
#[inline]
fn next_prefix(mut p: Vec<u8>) -> Option<Vec<u8>> {
    for i in (0..p.len()).rev() {
        if p[i] != 0xff {
            p[i] += 1;
            p.truncate(i + 1);
            return Some(p);
        }
    }
    None
}

/// Value layout in Metashrew balances:
/// - 20 bytes: [0..16) = balance u128 LE, [16..20) = updated u32 LE
/// - 16 bytes: balance u128 LE (no updated)
/// - 8/4 bytes: occasionally compacted values (we'll decode as u64/u32)
#[derive(Debug, Clone, Copy)]
struct DecodedVal {
    balance: Option<u128>,
    updated: Option<u32>,
}
#[inline]
fn decode_balance_value(bytes: &[u8]) -> DecodedVal {
    match bytes.len() {
        20 => {
            let mut bal = [0u8; 16];
            bal.copy_from_slice(&bytes[..16]);
            let mut ts = [0u8; 4];
            ts.copy_from_slice(&bytes[16..20]);
            DecodedVal {
                balance: Some(u128::from_le_bytes(bal)),
                updated: Some(u32::from_le_bytes(ts)),
            }
        }
        16 => {
            let mut bal = [0u8; 16];
            bal.copy_from_slice(bytes);
            DecodedVal { balance: Some(u128::from_le_bytes(bal)), updated: None }
        }
        8 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            DecodedVal { balance: Some(u64::from_le_bytes(a) as u128), updated: None }
        }
        4 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            DecodedVal { balance: Some(u32::from_le_bytes(a) as u128), updated: None }
        }
        _ => DecodedVal { balance: None, updated: None },
    }
}

/// Scan `/alkanes/{what}/balances/{who||*}` and return the latest balance by max(updated).
/// Ties break by larger slot, then by RocksDB iter order.
fn latest_balance_for_pair(
    db: &SDB,
    what: &SchemaAlkaneId,
    who: &SchemaAlkaneId,
) -> Result<Option<(u128, Option<u32>)>> {
    let prefix = balance_prefix(what, who);

    let mut ro = ReadOptions::default();
    if let Some(ub) = next_prefix(prefix.clone()) {
        ro.set_iterate_upper_bound(ub);
    }
    ro.set_total_order_seek(true);

    let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);

    let mut best_updated: Option<u32> = None;
    let mut best_slot: u32 = 0;
    let mut best_balance: Option<u128> = None;

    while let Some(Ok((k, v))) = it.next() {
        if !k.starts_with(&prefix) {
            break;
        }
        // key suffix = slot u32 LE (bytes 32..36 of WHO segment), but we don't need to parse WHO again
        if k.len() < prefix.len() + 4 {
            continue;
        }
        let mut slot_bytes = [0u8; 4];
        slot_bytes.copy_from_slice(&k[prefix.len()..prefix.len() + 4]);
        let slot = u32::from_le_bytes(slot_bytes);

        let dec = decode_balance_value(&v);
        if let Some(bal) = dec.balance {
            // prefer higher updated; if none available, prefer higher slot
            let better = match (best_updated, dec.updated) {
                (None, Some(_)) => true,
                (Some(prev), Some(cur)) if cur > prev => true,
                (Some(prev), Some(cur)) if cur == prev && slot > best_slot => true,
                (None, None) if slot > best_slot => true,
                (None, None) => best_balance.is_none(), // first one wins
                _ => false,
            };
            if better {
                best_updated = dec.updated;
                best_slot = slot;
                best_balance = Some(bal);
            }
        }
    }

    Ok(best_balance.map(|b| (b, best_updated)))
}

/// Fetch real-time reserves for all pools in `pools` by querying Metashrew balances:
/// - base_reserve = balance of {what = base_id} held by {who = pool_alkane_id}
/// - quote_reserve = balance of {what = quote_id} held by {who = pool_alkane_id}
///
/// Returns a snapshot map identical to your in-memory schema.
pub fn fetch_latest_reserves_for_pools(
    pools: &HashMap<SchemaAlkaneId, SchemaMarketDefs>,
) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let db = get_metashrew_sdb(); // SDB is a shared RocksDB wrapper

    let mut out: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = HashMap::with_capacity(pools.len());

    for (pool_id, defs) in pools {
        let (base_bal, base_upd) =
            latest_balance_for_pair(&db, &defs.base_alkane_id, pool_id)?.unwrap_or((0u128, None));

        let (quote_bal, quote_upd) =
            latest_balance_for_pair(&db, &defs.quote_alkane_id, pool_id)?.unwrap_or((0u128, None));

        // Debug log: show which entries we considered "latest"
        if let (Some(bu), Some(qu)) = (base_upd, quote_upd) {
            eprintln!(
                "[AMMDATA-LIVE] pool {}/{} live reserves: base={} (upd={}), quote={} (upd={})",
                pool_id.block, pool_id.tx, base_bal, bu, quote_bal, qu
            );
        } else {
            eprintln!(
                "[AMMDATA-LIVE] pool {}/{} live reserves: base={}, quote={}",
                pool_id.block, pool_id.tx, base_bal, quote_bal
            );
        }

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
