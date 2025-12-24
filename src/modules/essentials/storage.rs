use crate::modules::essentials::utils::inspections::AlkaneCreationRecord;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use bitcoin::{Address, Network, ScriptBuf};
use borsh::{BorshDeserialize, BorshSerialize};

use anyhow::Result;

/// Identifier for a holder: either a Bitcoin address or another Alkane.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, BorshSerialize, BorshDeserialize)]
pub enum HolderId {
    Address(String),
    Alkane(SchemaAlkaneId),
}

/// Entry in holders index (holder id + amount for one alkane)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HolderEntry {
    pub holder: HolderId,
    pub amount: u128,
}

/// One alkane balance record inside a single outpoint (BORSH)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BalanceEntry {
    pub alkane: SchemaAlkaneId,
    pub amount: u128,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HoldersCountEntry {
    pub count: u64,
}

/// Creation metadata for an alkane.
#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct AlkaneInfo {
    pub creation_txid: [u8; 32],
    pub creation_height: u32,
    pub creation_timestamp: u32,
}

pub fn addr_spk_key(addr: &str) -> Vec<u8> {
    let mut k = b"/addr_spk/".to_vec();
    k.extend_from_slice(addr.as_bytes());
    k
}

// /balances/{address}/{borsh(EspoOutpoint)}
pub fn balances_key(address: &str, outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/balances/".to_vec();
    k.extend_from_slice(address.as_bytes());
    k.push(b'/');
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /holders/{alkane block:u32be}{tx:u64be}
pub fn holders_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut k = b"/holders/".to_vec();
    k.extend_from_slice(&alkane.block.to_be_bytes());
    k.extend_from_slice(&alkane.tx.to_be_bytes());
    k
}
// /holders/count/{alkane block:u32be}{tx:u64be}
pub fn holders_count_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/holders/count/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkane_balance_txs/{alkane block:u32be}{tx:u64be}
pub fn alkane_balance_txs_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balance_txs/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkane_balance_txs_by_token/{owner block:u32be}{owner tx:u64be}/{token block:u32be}{token tx:u64be}
pub fn alkane_balance_txs_by_token_key(owner: &SchemaAlkaneId, token: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balance_txs_by_token/".to_vec();
    key.extend_from_slice(&owner.block.to_be_bytes());
    key.extend_from_slice(&owner.tx.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&token.block.to_be_bytes());
    key.extend_from_slice(&token.tx.to_be_bytes());
    key
}

// /alkane_balances/{owner block:u32be}{tx:u64be}
pub fn alkane_balances_key(owner: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balances/".to_vec();
    key.extend_from_slice(&owner.block.to_be_bytes());
    key.extend_from_slice(&owner.tx.to_be_bytes());
    key
}
// /alkane_info/{alkane block:u32be}{tx:u64be}
pub fn alkane_info_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_info/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}
// /alkanes/creation/id/{alkane block:u32be}{tx:u64be}
pub fn alkane_creation_by_id_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkanes/creation/id/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkanes/creation/ordered/{ts_be(4)}{height_be(4)}{tx_index_be(4)}{alk_block_be(4)}{alk_tx_be(8)}
pub fn alkane_creation_ordered_key(
    timestamp: u32,
    height: u32,
    tx_index: u32,
    alkane: &SchemaAlkaneId,
) -> Vec<u8> {
    let mut key = b"/alkanes/creation/ordered/".to_vec();
    key.extend_from_slice(&timestamp.to_be_bytes());
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(&tx_index.to_be_bytes());
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

pub fn alkane_creation_ordered_prefix() -> &'static [u8] {
    b"/alkanes/creation/ordered/"
}

pub fn alkane_creation_count_key() -> &'static [u8] {
    b"/alkanes/creation/count"
}
// /outpoint_addr/{borsh(EspoOutpoint)} -> address (utf8)
pub fn outpoint_addr_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_addr/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /utxo_spk/{borsh(EspoOutpoint)} -> ScriptPubKey (raw bytes)
pub fn utxo_spk_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/utxo_spk/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /outpoint_balances/{borsh(EspoOutpoint)} -> Vec<BalanceEntry>
pub fn outpoint_balances_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_balances/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /trace_count/{height_be}
pub fn trace_count_key(height: u32) -> Vec<u8> {
    let mut k = b"/trace_count/".to_vec();
    k.extend_from_slice(&height.to_be_bytes());
    k
}

#[derive(BorshSerialize)]
struct OutpointPrefix {
    txid: Vec<u8>,
    vout: u32,
}

/// Prefix for matching any serialization of an outpoint (with or without tx_spent)
pub fn outpoint_balances_prefix(txid: &[u8], vout: u32) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_balances/".to_vec();
    k.extend_from_slice(&borsh::to_vec(&OutpointPrefix { txid: txid.to_vec(), vout })?);
    Ok(k)
}

/// Helper to build an outpoint with optional spending txid for lookups.
pub fn mk_outpoint(txid: Vec<u8>, vout: u32, tx_spent: Option<Vec<u8>>) -> EspoOutpoint {
    EspoOutpoint { txid, vout, tx_spent }
}

pub fn spk_to_address_str(spk: &ScriptBuf, net: Network) -> Option<String> {
    Address::from_script(spk.as_script(), net).ok().map(|a| a.to_string())
}

pub fn encode_vec<T: BorshSerialize>(v: &Vec<T>) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_balances_vec(bytes: &[u8]) -> Result<Vec<BalanceEntry>> {
    Ok(Vec::<BalanceEntry>::try_from_slice(bytes)?)
}

pub fn decode_holders_vec(bytes: &[u8]) -> Result<Vec<HolderEntry>> {
    if let Ok(parsed) = Vec::<HolderEntry>::try_from_slice(bytes) {
        return Ok(parsed);
    }

    #[derive(BorshDeserialize)]
    struct LegacyHolderEntry {
        address: String,
        amount: u128,
    }

    let legacy: Vec<LegacyHolderEntry> = Vec::<LegacyHolderEntry>::try_from_slice(bytes)?;
    Ok(legacy
        .into_iter()
        .map(|h| HolderEntry { holder: HolderId::Address(h.address), amount: h.amount })
        .collect())
}

pub fn encode_alkane_info(info: &AlkaneInfo) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(info)?)
}

pub fn decode_alkane_info(bytes: &[u8]) -> Result<AlkaneInfo> {
    Ok(AlkaneInfo::try_from_slice(bytes)?)
}

pub fn encode_creation_record(record: &AlkaneCreationRecord) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(record)?)
}

pub fn decode_creation_record(bytes: &[u8]) -> Result<AlkaneCreationRecord> {
    // Try new schema first; fall back to legacy Option name/symbol layout.
    if let Ok(rec) = AlkaneCreationRecord::try_from_slice(bytes) {
        return Ok(rec);
    }

    #[derive(BorshDeserialize)]
    struct LegacyCreationRecord {
        alkane: SchemaAlkaneId,
        txid: [u8; 32],
        creation_height: u32,
        creation_timestamp: u32,
        tx_index_in_block: u32,
        inspection: Option<crate::modules::essentials::utils::inspections::StoredInspectionResult>,
        name: Option<String>,
        symbol: Option<String>,
    }

    let legacy = LegacyCreationRecord::try_from_slice(bytes)?;
    let mut names = Vec::new();
    let mut symbols = Vec::new();
    if let Some(n) = legacy.name {
        names.push(n);
    }
    if let Some(s) = legacy.symbol {
        symbols.push(s);
    }
    Ok(AlkaneCreationRecord {
        alkane: legacy.alkane,
        txid: legacy.txid,
        creation_height: legacy.creation_height,
        creation_timestamp: legacy.creation_timestamp,
        tx_index_in_block: legacy.tx_index_in_block,
        inspection: legacy.inspection,
        names,
        symbols,
    })
}

pub fn load_creation_record(mdb: &crate::runtime::mdb::Mdb, alkane: &SchemaAlkaneId) -> Result<Option<AlkaneCreationRecord>> {
    let key = alkane_creation_by_id_key(alkane);
    if let Some(bytes) = mdb.get(&key)? {
        let record = decode_creation_record(&bytes)?;
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

pub fn get_holders_count_encoded(count: u64) -> Result<Vec<u8>> {
    let count_value = HoldersCountEntry { count };

    Ok(borsh::to_vec(&count_value)?)
}

pub fn get_holders_values_encoded(holders: Vec<HolderEntry>) -> Result<(Vec<u8>, Vec<u8>)> {
    Ok((encode_vec(&holders)?, get_holders_count_encoded(holders.len().try_into()?)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alkane_info_round_trip() {
        let info = AlkaneInfo {
            creation_txid: [7u8; 32],
            creation_height: 42,
            creation_timestamp: 1_700_000_000,
        };

        let encoded = encode_alkane_info(&info).expect("encode");
        let decoded = decode_alkane_info(&encoded).expect("decode");
        assert_eq!(info, decoded);
    }

    #[test]
    fn creation_record_round_trip() {
        let rec = AlkaneCreationRecord {
            alkane: SchemaAlkaneId { block: 5, tx: 10 },
            txid: [9u8; 32],
            creation_height: 123,
            creation_timestamp: 99,
            tx_index_in_block: 3,
            inspection: None,
            names: vec!["demo".to_string(), "demo2".to_string()],
            symbols: vec!["DMO".to_string()],
        };

        let encoded = encode_creation_record(&rec).expect("encode");
        let decoded = decode_creation_record(&encoded).expect("decode");
        assert_eq!(rec, decoded);
    }
}
