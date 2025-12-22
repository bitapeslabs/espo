use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId,
};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_cli_common::alkanes::inspector::types::{AlkaneMetadata, AlkaneMethod};
use alkanes_cli_common::alkanes::inspector::{AlkaneInspector, InspectionConfig, InspectionResult};
use alkanes_cli_common::alkanes::types::AlkaneId as CliAlkaneId;
use anyhow::{Context, Result, anyhow};
use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::future::Future;
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StoredInspectionMethod {
    pub name: String,
    pub opcode: u128,
    pub params: Vec<String>,
    pub returns: String,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StoredInspectionMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub methods: Vec<StoredInspectionMethod>,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StoredInspectionResult {
    pub alkane: SchemaAlkaneId,
    pub bytecode_length: u64,
    pub metadata: Option<StoredInspectionMetadata>,
    pub metadata_error: Option<String>,
    pub factory_alkane: Option<SchemaAlkaneId>,
}

impl StoredInspectionResult {
    pub fn from_inspection_result(
        alkane: &SchemaAlkaneId,
        result: &InspectionResult,
        factory_alkane: SchemaAlkaneId,
    ) -> Result<Self> {
        let bytecode_length = u64::try_from(result.bytecode_length)
            .map_err(|_| anyhow!("bytecode length does not fit into u64"))?;
        Ok(Self {
            alkane: *alkane,
            bytecode_length,
            metadata: result.metadata.as_ref().map(StoredInspectionMetadata::from),
            metadata_error: result.metadata_error.clone(),
            factory_alkane: Some(factory_alkane),
        })
    }
}

impl From<&AlkaneMetadata> for StoredInspectionMetadata {
    fn from(value: &AlkaneMetadata) -> Self {
        Self {
            name: value.name.clone(),
            version: value.version.clone(),
            description: value.description.clone(),
            methods: value.methods.iter().map(StoredInspectionMethod::from).collect(),
        }
    }
}

impl From<&AlkaneMethod> for StoredInspectionMethod {
    fn from(value: &AlkaneMethod) -> Self {
        Self {
            name: value.name.clone(),
            opcode: value.opcode,
            params: value.params.clone(),
            returns: value.returns.clone(),
        }
    }
}

fn block_on_result<F, T>(fut: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match Handle::try_current() {
        Ok(handle) => block_in_place(|| handle.block_on(fut)),
        Err(_) => {
            let rt = Runtime::new().context("failed to build ad-hoc Tokio runtime")?;
            rt.block_on(fut)
        }
    }
}

pub fn inspect_wasm_metadata(
    alkane: &SchemaAlkaneId,
    wasm_bytes: &[u8],
    factory_alkane: SchemaAlkaneId,
) -> Result<StoredInspectionResult> {
    let inspector = AlkaneInspector::new();
    let cfg = InspectionConfig {
        disasm: false,
        fuzz: false,
        fuzz_ranges: None,
        meta: true,
        codehash: false,
        raw: false,
    };

    let cli_id = CliAlkaneId { block: alkane.block as u64, tx: alkane.tx };
    let wasm_vec = wasm_bytes.to_vec();
    let res = block_on_result(inspector.inspect_alkane_with_bytes(&wasm_vec, &cli_id, &cfg))?;
    StoredInspectionResult::from_inspection_result(alkane, &res, factory_alkane)
}

pub fn inspection_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/inspections/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

pub fn encode_inspection(record: &StoredInspectionResult) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(record)?)
}

pub fn decode_inspection(bytes: &[u8]) -> Result<StoredInspectionResult> {
    Ok(StoredInspectionResult::try_from_slice(bytes)?)
}

pub fn load_inspection(
    mdb: &Mdb,
    alkane: &SchemaAlkaneId,
) -> Result<Option<StoredInspectionResult>> {
    let key = inspection_key(alkane);
    if let Some(bytes) = mdb.get(&key)? {
        let record = decode_inspection(&bytes)?;
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

fn parse_short_id(id: &EspoSandshrewLikeTraceShortId) -> Option<SchemaAlkaneId> {
    fn parse_u32_or_hex(s: &str) -> Option<u32> {
        if let Some(hex) = s.strip_prefix("0x") {
            return u32::from_str_radix(hex, 16).ok();
        }
        s.parse::<u32>().ok()
    }
    fn parse_u64_or_hex(s: &str) -> Option<u64> {
        if let Some(hex) = s.strip_prefix("0x") {
            return u64::from_str_radix(hex, 16).ok();
        }
        s.parse::<u64>().ok()
    }

    let block = parse_u32_or_hex(&id.block)?;
    let tx = parse_u64_or_hex(&id.tx)?;
    Some(SchemaAlkaneId { block, tx })
}

pub fn created_alkanes_from_block(block: &EspoBlock) -> Vec<SchemaAlkaneId> {
    let mut seen: HashSet<SchemaAlkaneId> = HashSet::new();

    for tx in block.transactions.iter() {
        let Some(traces) = tx.traces.as_ref() else { continue };
        for trace in traces {
            for ev in trace.sandshrew_trace.events.iter() {
                if let EspoSandshrewLikeTraceEvent::Create(create) = ev {
                    if let Some(id) = parse_short_id(&create.new_alkane) {
                        seen.insert(id);
                    }
                }
            }
        }
    }

    seen.into_iter().collect()
}

fn method_to_json(m: &StoredInspectionMethod) -> Value {
    json!({
        "name": m.name,
        "opcode": m.opcode.to_string(),
        "params": m.params,
        "returns": m.returns,
    })
}

fn metadata_to_json(meta: &StoredInspectionMetadata) -> Value {
    json!({
        "name": meta.name,
        "version": meta.version,
        "description": meta.description,
        "methods": meta.methods.iter().map(method_to_json).collect::<Vec<_>>(),
    })
}

pub fn inspection_to_json(record: &StoredInspectionResult) -> Value {
    let factory_str = record.factory_alkane.map(|f| format!("{}:{}", f.block, f.tx));
    json!({
        "alkane": format!("{}:{}", record.alkane.block, record.alkane.tx),
        "bytecode_length": record.bytecode_length,
        "metadata": record.metadata.as_ref().map(metadata_to_json),
        "metadata_error": record.metadata_error,
        "factory_alkane": factory_str,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspection_round_trip() {
        let record = StoredInspectionResult {
            alkane: SchemaAlkaneId { block: 1, tx: 2 },
            bytecode_length: 42,
            metadata: Some(StoredInspectionMetadata {
                name: "demo".to_string(),
                version: "1.0.0".to_string(),
                description: Some("hello".to_string()),
                methods: vec![StoredInspectionMethod {
                    name: "run".to_string(),
                    opcode: 7,
                    params: vec!["u64".to_string()],
                    returns: "bool".to_string(),
                }],
            }),
            metadata_error: None,
            factory_alkane: Some(SchemaAlkaneId { block: 9, tx: 9 }),
        };

        let bytes = encode_inspection(&record).expect("encode");
        let decoded = decode_inspection(&bytes).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn parse_short_ids() {
        let short =
            EspoSandshrewLikeTraceShortId { block: "0x2".to_string(), tx: "16".to_string() };
        let parsed = parse_short_id(&short).expect("parsed");
        assert_eq!(parsed.block, 2);
        assert_eq!(parsed.tx, 16);
    }
}
