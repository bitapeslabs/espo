use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use bitcoin::{Address, Network, ScriptBuf};
use borsh::{BorshDeserialize, BorshSerialize};

use anyhow::Result;

/// Entry in holders index (address string + amount for one alkane)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HolderEntry {
    pub address: String,
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
    Ok(Vec::<HolderEntry>::try_from_slice(bytes)?)
}

pub fn get_holders_count_encoded(count: u64) -> Result<Vec<u8>> {
    let count_value = HoldersCountEntry { count };

    Ok(borsh::to_vec(&count_value)?)
}

pub fn get_holders_values_encoded(holders: Vec<HolderEntry>) -> Result<(Vec<u8>, Vec<u8>)> {
    Ok((encode_vec(&holders)?, get_holders_count_encoded(holders.len().try_into()?)?))
}
