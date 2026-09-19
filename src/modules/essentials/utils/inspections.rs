use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceShortId,
    EspoSandshrewLikeTraceStatus, EspoTrace,
};
use crate::modules::essentials::storage::{EssentialsProvider, GetRawValueParams};
use crate::runtime::state_at::StateAt;
use crate::schemas::SchemaAlkaneId;
use alkanes_cli_common::alkanes::inspector::types::{AlkaneMetadata, AlkaneMethod};
use alkanes_cli_common::alkanes::inspector::{AlkaneInspector, InspectionConfig, InspectionResult};
use alkanes_cli_common::alkanes::types::AlkaneId as CliAlkaneId;
use anyhow::{Context, Result, anyhow};
use bitcoin::hashes::Hash;
use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::future::Future;
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;

pub const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
pub const KV_KEY_BEACON: &[u8] = b"/beacon";
/// alkanes-std-upgradeable's own ABI — kept as the fixture of what the
/// canonical proxy looks like; detection itself is `is_upgradeable_proxy`.
#[cfg(test)]
const UPGRADEABLE_METHODS: [(&str, u128); 2] = [("initialize", 32767), ("forward", 36863)];
/// The proxy constructor's opcode: alkanes-runtime's
/// `observe_proxy_initialization` convention (0x7fff), far above any
/// implementation's own range so that every other opcode falls through to
/// the delegatecall.
const PROXY_INITIALIZE_OPCODE: u128 = 32767;
/// alkanes-std-upgradeable's pass-through method.
const PROXY_FORWARD_OPCODE: u128 = 36863;

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
    provider: &EssentialsProvider,
    alkane: &SchemaAlkaneId,
) -> Result<Option<StoredInspectionResult>> {
    // The inspection is now stored alongside the creation record; keep a small
    // helper here for call sites that expect it.
    let rec = provider
        .get_creation_record(crate::modules::essentials::storage::GetCreationRecordParams {
            blockhash: StateAt::Latest,
            alkane: *alkane,
        })?
        .record;
    Ok(rec.and_then(|r| r.inspection))
}

/// Is this contract an upgradeable (delegatecall) proxy?
///
/// Recognized by SHAPE, not by one template's exact ABI: a method at the
/// proxy-constructor opcode 32767, whatever it is called (`initialize`,
/// `proxy_initialize`, …), plus the evidence that the code behind it can be
/// swapped or is only passed through — an `upgrade` method (any opcode), or
/// alkanes-std-upgradeable's `forward` at 36863. That covers
/// alkanes-std-upgradeable, the beacon proxies and third-party proxies
/// written against the same runtime convention (pizza.fun's cheese-proxy:
/// `proxy_initialize`@32767 + `upgrade`@32766, no `forward`).
///
/// This only says "look for a pointer": the target still has to be readable
/// from `/implementation` or `/beacon` (`get_proxy_implementation`), so a
/// contract that merely resembles a proxy resolves to nothing and is shown
/// as itself.
pub fn is_upgradeable_proxy(inspection: &StoredInspectionResult) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    let has_constructor = meta.methods.iter().any(|m| m.opcode == PROXY_INITIALIZE_OPCODE);
    let swappable = meta.methods.iter().any(|m| {
        m.name.eq_ignore_ascii_case("upgrade")
            || (m.opcode == PROXY_FORWARD_OPCODE && m.name.eq_ignore_ascii_case("forward"))
    });
    has_constructor && swappable
}

fn kv_row_key(alkane: &SchemaAlkaneId, storage_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4 + 8 + 2 + storage_key.len());
    key.push(0x01);
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    let len = u16::try_from(storage_key.len()).unwrap_or(u16::MAX);
    key.extend_from_slice(&len.to_be_bytes());
    if len as usize != storage_key.len() {
        key.extend_from_slice(&storage_key[..len as usize]);
    } else {
        key.extend_from_slice(storage_key);
    }
    key
}

/// The alkane id held in a proxy's `/implementation` (or `/beacon`) slot.
///
/// Two encodings are in the wild:
/// - 32 bytes — alkanes-support's `AlkaneId` as bytes (u128 block LE, u128 tx
///   LE), what alkanes-std-upgradeable writes;
/// - 12 bytes — a borsh `{ block: u32, tx: u64 }`, what contracts built on
///   borsh schemas write (pizza.fun's cheese-proxy).
///
/// Anything else is not a pointer. A zero id is not a pointer either.
pub fn decode_kv_implementation(raw: &[u8]) -> Option<SchemaAlkaneId> {
    let id = if raw.len() == 12 {
        SchemaAlkaneId {
            block: u32::from_le_bytes(raw[0..4].try_into().ok()?),
            tx: u64::from_le_bytes(raw[4..12].try_into().ok()?),
        }
    } else if raw.len() >= 32 {
        let block = u128::from_le_bytes(raw[0..16].try_into().ok()?);
        let tx = u128::from_le_bytes(raw[16..32].try_into().ok()?);
        SchemaAlkaneId { block: u32::try_from(block).ok()?, tx: u64::try_from(tx).ok()? }
    } else {
        return None;
    };
    (id.block != 0 || id.tx != 0).then_some(id)
}

/// The implementation (or beacon) pointer stored by an upgradeable proxy.
///
/// This is a semantic getter rather than a raw KV read so client-mode
/// explorers can resolve it against the remote espo — without it, proxy
/// contracts (e.g. the AMM) lose their implementation ABI and every call
/// renders as a generic "contract call".
pub fn get_proxy_implementation(
    provider: &EssentialsProvider,
    alkane: &SchemaAlkaneId,
) -> Option<SchemaAlkaneId> {
    if let Some(remote) = crate::config::explorer_remote() {
        return crate::modules::essentials::internal_rpc::remote_get_proxy_implementation(
            &remote, alkane,
        )
        .map_err(|e| eprintln!("[remote] get_proxy_implementation failed: {e}"))
        .ok()
        .flatten();
    }
    proxy_target_from_db(alkane, provider)
}

fn proxy_target_from_db(
    alkane: &SchemaAlkaneId,
    provider: &EssentialsProvider,
) -> Option<SchemaAlkaneId> {
    let lookup = |storage_key| {
        provider
            .get_raw_value(GetRawValueParams {
                blockhash: StateAt::Latest,
                key: kv_row_key(alkane, storage_key),
            })
            .ok()
            .and_then(|response| response.value)
            .and_then(|raw| {
                // stored rows carry a 32-byte prefix (the writing txid) before the value
                if raw.len() >= 32 + 12 {
                    decode_kv_implementation(&raw[32..])
                } else {
                    decode_kv_implementation(&raw)
                }
            })
    };
    lookup(KV_KEY_IMPLEMENTATION).or_else(|| lookup(KV_KEY_BEACON))
}

pub fn resolve_proxy_target_recursive(
    start: &SchemaAlkaneId,
    provider: &EssentialsProvider,
) -> Option<SchemaAlkaneId> {
    let mut current = *start;
    let mut seen = HashSet::new();
    for _ in 0..8 {
        let inspection = load_inspection(provider, &current).ok().flatten()?;
        if !is_upgradeable_proxy(&inspection) {
            return (current != *start).then_some(current);
        }
        let next = get_proxy_implementation(provider, &current)?;
        if !seen.insert(next) {
            return None;
        }
        current = next;
    }
    None
}

pub fn resolve_contract_wasm_source(
    start: &SchemaAlkaneId,
    provider: &EssentialsProvider,
) -> Option<SchemaAlkaneId> {
    let resolved = resolve_proxy_target_recursive(start, provider).unwrap_or(*start);
    load_inspection(provider, &resolved)
        .ok()
        .flatten()
        .and_then(|inspection| inspection.factory_alkane)
        .or(Some(resolved))
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

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct AlkaneCreationRecord {
    pub alkane: SchemaAlkaneId,
    pub txid: [u8; 32],
    pub creation_height: u32,
    pub creation_timestamp: u32,
    pub tx_index_in_block: u32,
    pub inspection: Option<StoredInspectionResult>,
    pub names: Vec<String>,
    pub symbols: Vec<String>,
    pub cap: u128,
    pub mint_amount: u128,
}

pub fn trace_succeeded(trace: &EspoTrace) -> bool {
    trace
        .sandshrew_trace
        .events
        .iter()
        .rev()
        .find_map(|event| {
            if let EspoSandshrewLikeTraceEvent::Return(data) = event {
                Some(!matches!(data.status, EspoSandshrewLikeTraceStatus::Failure))
            } else {
                None
            }
        })
        .unwrap_or(true)
}

pub fn created_alkane_records_from_block(block: &EspoBlock) -> Vec<AlkaneCreationRecord> {
    let mut seen: HashSet<SchemaAlkaneId> = HashSet::new();
    let mut out: Vec<AlkaneCreationRecord> = Vec::new();

    for (tx_index, tx) in block.transactions.iter().enumerate() {
        let Some(traces) = tx.traces.as_ref() else { continue };
        let mut txid = tx.transaction.compute_txid().to_byte_array();
        txid.reverse(); // store txid in standard BE display order
        for trace in traces {
            if !trace_succeeded(trace) {
                continue;
            }
            for ev in trace.sandshrew_trace.events.iter() {
                if let EspoSandshrewLikeTraceEvent::Create(create) = ev {
                    if let Some(id) = parse_short_id(&create) {
                        if seen.insert(id) {
                            out.push(AlkaneCreationRecord {
                                alkane: id,
                                txid,
                                creation_height: block.height,
                                creation_timestamp: block.block_header.time,
                                tx_index_in_block: tx_index as u32,
                                inspection: None,
                                names: Vec::new(),
                                symbols: Vec::new(),
                                cap: 0,
                                mint_amount: 0,
                            });
                        }
                    }
                }
            }
        }
    }

    out
}

pub fn created_alkanes_from_block(block: &EspoBlock) -> Vec<SchemaAlkaneId> {
    created_alkane_records_from_block(block)
        .into_iter()
        .map(|rec| rec.alkane)
        .collect()
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
    use crate::alkanes::trace::{
        EspoAlkanesTransaction, EspoBlock, EspoSandshrewLikeTrace,
        EspoSandshrewLikeTraceReturnData, EspoSandshrewLikeTraceReturnResponse,
        EspoSandshrewLikeTraceStatus, EspoTrace,
    };
    use crate::modules::essentials::storage::{SetBatchParams, encode_creation_record};
    use crate::runtime::mdb::Mdb;
    use crate::schemas::EspoOutpoint;
    use alkanes_support::proto::alkanes::AlkanesTrace;
    use bitcoin::block::Header;
    use bitcoin::blockdata::constants::genesis_block;
    use bitcoin::hashes::Hash;
    use bitcoin::{
        Amount, Network, ScriptBuf, Transaction, TxOut, locktime::absolute, transaction,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_tx(value: u64) -> Transaction {
        Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![],
            output: vec![TxOut { value: Amount::from_sat(value), script_pubkey: ScriptBuf::new() }],
        }
    }

    fn short_id(block: &str, tx: &str) -> EspoSandshrewLikeTraceShortId {
        EspoSandshrewLikeTraceShortId { block: block.to_string(), tx: tx.to_string() }
    }

    fn return_event(status: EspoSandshrewLikeTraceStatus) -> EspoSandshrewLikeTraceEvent {
        EspoSandshrewLikeTraceEvent::Return(EspoSandshrewLikeTraceReturnData {
            status,
            response: EspoSandshrewLikeTraceReturnResponse {
                alkanes: vec![],
                data: "0x".to_string(),
                storage: vec![],
            },
        })
    }

    fn create_trace(outpoint: String, status: EspoSandshrewLikeTraceStatus) -> EspoTrace {
        EspoTrace {
            sandshrew_trace: EspoSandshrewLikeTrace {
                outpoint,
                events: vec![
                    EspoSandshrewLikeTraceEvent::Create(short_id("0x2", "0x13e67")),
                    return_event(status),
                ],
            },
            protobuf_trace: AlkanesTrace::default(),
            storage_changes: HashMap::new(),
            outpoint: EspoOutpoint::default(),
        }
    }

    fn test_block(transactions: Vec<EspoAlkanesTransaction>) -> EspoBlock {
        let header: Header = genesis_block(Network::Bitcoin).header;
        EspoBlock {
            is_latest: true,
            height: 953133,
            block_header: header,
            host_function_values: (vec![], vec![], vec![], vec![]),
            fee_summary: None,
            tx_count: transactions.len(),
            transactions,
        }
    }

    fn inspection_record(
        alkane: SchemaAlkaneId,
        methods: Vec<StoredInspectionMethod>,
        factory_alkane: Option<SchemaAlkaneId>,
    ) -> AlkaneCreationRecord {
        AlkaneCreationRecord {
            alkane,
            txid: [0; 32],
            creation_height: 1,
            creation_timestamp: 1,
            tx_index_in_block: 0,
            inspection: Some(StoredInspectionResult {
                alkane,
                bytecode_length: 1,
                metadata: Some(StoredInspectionMetadata {
                    name: "TestContract".to_string(),
                    version: "1.0.0".to_string(),
                    description: None,
                    methods,
                }),
                metadata_error: None,
                factory_alkane,
            }),
            names: Vec::new(),
            symbols: Vec::new(),
            cap: 0,
            mint_amount: 0,
        }
    }

    fn write_inspection_records(
        provider: &EssentialsProvider,
        records: &[AlkaneCreationRecord],
        extra_puts: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let table = provider.table();
        let mut puts = records
            .iter()
            .map(|record| {
                (
                    table.alkane_creation_by_id_key(&record.alkane),
                    encode_creation_record(record).expect("encode creation record"),
                )
            })
            .collect::<Vec<_>>();
        puts.extend(extra_puts);
        provider
            .set_batch(SetBatchParams { blockhash: StateAt::Latest, puts, deletes: Vec::new() })
            .expect("write inspection records");
    }

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
    fn resolves_factory_clone_wasm_source_from_indexed_inspection() {
        let dir = tempfile::tempdir_in(".").expect("tempdir");
        let provider = EssentialsProvider::new(Arc::new(
            Mdb::open(dir.path(), b"inspection_factory_test:").expect("open mdb"),
        ));
        let clone = SchemaAlkaneId { block: 2, tx: 10 };
        let factory = SchemaAlkaneId { block: 4, tx: 20 };
        write_inspection_records(
            &provider,
            &[inspection_record(clone, Vec::new(), Some(factory))],
            Vec::new(),
        );

        assert_eq!(resolve_contract_wasm_source(&clone, &provider), Some(factory));
    }

    #[test]
    fn resolves_proxy_to_factory_clone_wasm_source() {
        let dir = tempfile::tempdir_in(".").expect("tempdir");
        let provider = EssentialsProvider::new(Arc::new(
            Mdb::open(dir.path(), b"inspection_proxy_test:").expect("open mdb"),
        ));
        let proxy = SchemaAlkaneId { block: 2, tx: 10 };
        let implementation = SchemaAlkaneId { block: 4, tx: 20 };
        let factory = SchemaAlkaneId { block: 8, tx: 30 };
        let proxy_methods = UPGRADEABLE_METHODS
            .iter()
            .map(|(name, opcode)| StoredInspectionMethod {
                name: (*name).to_string(),
                opcode: *opcode,
                params: Vec::new(),
                returns: String::new(),
            })
            .collect();
        let mut implementation_value = vec![0; 32];
        implementation_value.extend_from_slice(&(implementation.block as u128).to_le_bytes());
        implementation_value.extend_from_slice(&(implementation.tx as u128).to_le_bytes());
        write_inspection_records(
            &provider,
            &[
                inspection_record(proxy, proxy_methods, None),
                inspection_record(implementation, Vec::new(), Some(factory)),
            ],
            vec![(kv_row_key(&proxy, KV_KEY_IMPLEMENTATION), implementation_value)],
        );

        assert_eq!(resolve_contract_wasm_source(&proxy, &provider), Some(factory));
    }

    #[test]
    fn resolves_a_borsh_pointer_proxy_without_the_std_forward_method() {
        // pizza.fun's cheese-proxy: `proxy_initialize`@32767 + `upgrade`@32766 (no
        // `forward`), and `/implementation` holds a 12-byte borsh { u32, u64 }.
        let dir = tempfile::tempdir_in(".").expect("tempdir");
        let provider = EssentialsProvider::new(Arc::new(
            Mdb::open(dir.path(), b"inspection_borsh_proxy_test:").expect("open mdb"),
        ));
        let proxy = SchemaAlkaneId { block: 4, tx: 396 };
        let implementation = SchemaAlkaneId { block: 4, tx: 393 };
        let method = |name: &str, opcode: u128| StoredInspectionMethod {
            name: name.to_string(),
            opcode,
            params: Vec::new(),
            returns: String::new(),
        };
        let proxy_methods = vec![
            method("proxy_initialize", 32767),
            method("upgrade", 32766),
            method("get_implementation", 32765),
            method("get_data_id", 32764),
        ];
        let mut implementation_value = vec![0; 32];
        implementation_value.extend_from_slice(&implementation.block.to_le_bytes());
        implementation_value.extend_from_slice(&implementation.tx.to_le_bytes());
        write_inspection_records(
            &provider,
            &[
                inspection_record(proxy, proxy_methods, None),
                inspection_record(implementation, vec![method("stake", 1)], None),
            ],
            vec![(kv_row_key(&proxy, KV_KEY_IMPLEMENTATION), implementation_value)],
        );

        assert_eq!(resolve_proxy_target_recursive(&proxy, &provider), Some(implementation));
    }

    #[test]
    fn proxy_shape_needs_the_constructor_and_a_way_to_swap_the_code() {
        let with = |methods: Vec<(&str, u128)>| {
            let mut record = inspection_record(
                SchemaAlkaneId { block: 2, tx: 1 },
                methods
                    .into_iter()
                    .map(|(name, opcode)| StoredInspectionMethod {
                        name: name.to_string(),
                        opcode,
                        params: Vec::new(),
                        returns: String::new(),
                    })
                    .collect(),
                None,
            );
            record.inspection.take().expect("inspection")
        };
        assert!(is_upgradeable_proxy(&with(vec![("initialize", 32767), ("forward", 36863)])));
        assert!(is_upgradeable_proxy(&with(vec![("proxy_initialize", 32767), ("upgrade", 32766)])));
        // an ordinary contract with an `upgrade` of its own is not a proxy…
        assert!(!is_upgradeable_proxy(&with(vec![("initialize", 0), ("upgrade", 7)])));
        // …and neither is one that only happens to use the high opcode
        assert!(!is_upgradeable_proxy(&with(vec![("initialize", 32767), ("stake", 1)])));
    }

    #[test]
    fn decodes_both_pointer_encodings() {
        let id = SchemaAlkaneId { block: 4, tx: 393 };
        let mut wide = Vec::new();
        wide.extend_from_slice(&(id.block as u128).to_le_bytes());
        wide.extend_from_slice(&(id.tx as u128).to_le_bytes());
        assert_eq!(decode_kv_implementation(&wide), Some(id));
        let mut borsh = Vec::new();
        borsh.extend_from_slice(&id.block.to_le_bytes());
        borsh.extend_from_slice(&id.tx.to_le_bytes());
        assert_eq!(decode_kv_implementation(&borsh), Some(id));
        assert_eq!(decode_kv_implementation(&[0u8; 12]), None, "a zero id is no pointer");
        assert_eq!(decode_kv_implementation(&[1u8; 20]), None, "neither encoding");
    }

    #[test]
    fn parse_short_ids() {
        let short =
            EspoSandshrewLikeTraceShortId { block: "0x2".to_string(), tx: "16".to_string() };
        let parsed = parse_short_id(&short).expect("parsed");
        assert_eq!(parsed.block, 2);
        assert_eq!(parsed.tx, 16);
    }

    #[test]
    fn duplicate_create_skips_failed_trace_and_keeps_successful_later_create() {
        let tx1 = test_tx(1);
        let tx2 = test_tx(2);
        let mut txid2 = tx2.compute_txid().to_byte_array();
        txid2.reverse();
        let block = test_block(vec![
            EspoAlkanesTransaction {
                traces: Some(vec![create_trace(
                    "first:0".to_string(),
                    EspoSandshrewLikeTraceStatus::Failure,
                )]),
                transaction: tx1,
            },
            EspoAlkanesTransaction {
                traces: Some(vec![create_trace(
                    "second:0".to_string(),
                    EspoSandshrewLikeTraceStatus::Success,
                )]),
                transaction: tx2,
            },
        ]);

        let records = created_alkane_records_from_block(&block);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].alkane, SchemaAlkaneId { block: 2, tx: 81511 });
        assert_eq!(records[0].txid, txid2);
        assert_eq!(records[0].tx_index_in_block, 1);
    }
}
