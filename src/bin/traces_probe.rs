use alkanes_support::proto::alkanes::AlkanesTrace;
use anyhow::Result;
use clap::Parser;
use espo::alkanes::metashrew::decode_trace_blob;
use espo::alkanes::trace::PartialEspoTrace;
use espo::runtime::sdb::SDB;
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::fs;
use std::path::Path;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "traces_probe",
    about = "Inspect traces for blocks directly from the secondary DB"
)]
struct Args {
    #[arg(long, short = 'r')]
    readonly_metashrew_db_dir: String,

    /// Where to put the secondary view (will be created if missing).
    #[arg(long, default_value = "./db/tmp/metashrew-probe")]
    secondary_path: String,

    /// Poll interval for secondary catch-up (ms).
    #[arg(long, short = 's', default_value_t = 5000)]
    sdb_poll_ms: u64,

    /// Block heights to probe.
    #[arg(required = true)]
    blocks: Vec<u64>,
}

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

fn traces_for_block(db: &SDB, block: u64) -> Result<Vec<PartialEspoTrace>> {
    let mut prefix = b"/trace/".to_vec();
    prefix.extend_from_slice(&block.to_le_bytes());
    prefix.push(b'/');

    let mut ro = ReadOptions::default();
    if let Some(ub) = next_prefix(prefix.clone()) {
        ro.set_iterate_upper_bound(ub);
    }
    ro.set_total_order_seek(true);

    let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut outpoints: Vec<Vec<u8>> = Vec::new();
    let mut pointers = 0usize;

    while let Some(Ok((k, v))) = it.next() {
        if !k.starts_with(&prefix) {
            break;
        }

        let val_str = match std::str::from_utf8(&v) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let (_block_str, hex_part) = match val_str.split_once(':') {
            Some(parts) => parts,
            None => continue,
        };
        let hex_bytes = match hex::decode(hex_part) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if hex_bytes.len() < 36 {
            continue;
        }

        let (tx_be, vout) = hex_bytes.split_at(32);
        let trace_idx = k.split(|b| *b == b'/').last().unwrap_or(b"0");

        let mut key = Vec::with_capacity(7 + tx_be.len() + vout.len() + 1 + trace_idx.len());
        key.extend_from_slice(b"/trace/");
        key.extend_from_slice(tx_be);
        key.extend_from_slice(vout);
        key.push(b'/');
        key.extend_from_slice(trace_idx);
        keys.push(key);

        let mut outpoint_le = tx_be.to_vec();
        outpoint_le.reverse();
        outpoint_le.extend_from_slice(vout);
        outpoints.push(outpoint_le);
        pointers += 1;
    }

    let values: Vec<Option<Vec<u8>>> = db.multi_get(keys.iter())?;
    let sample_blob = values.iter().find_map(|v| v.as_ref()).cloned();
    let mut blobs_found = values.iter().filter(|v| v.is_some()).count();

    let mut traces: Vec<PartialEspoTrace> = values
        .into_iter()
        .enumerate()
        .filter_map(|(idx, maybe_bytes)| {
            maybe_bytes.as_deref().and_then(decode_trace_blob).map(|protobuf_trace| {
                PartialEspoTrace { protobuf_trace, outpoint: outpoints[idx].clone() }
            })
        })
        .collect();

    if traces.is_empty() && !keys.is_empty() {
        for (idx, key) in keys.iter().enumerate() {
            if let Some(bytes) = db.get(key)? {
                blobs_found += 1;
                if let Some(protobuf_trace) = decode_trace_blob(&bytes) {
                    traces.push(PartialEspoTrace {
                        protobuf_trace,
                        outpoint: outpoints[idx].clone(),
                    });
                }
            }
        }
    }

    if traces.is_empty() {
        if let Some(sample) = sample_blob {
            let preview: String = sample.iter().take(12).map(|b| format!("{:02x}", b)).collect();
            let utf8_preview = String::from_utf8_lossy(&sample[..sample.len().min(80)]);
            eprintln!("  decode probe: len={} head={} (block {})", sample.len(), preview, block);
            eprintln!("  decode probe (utf8 prefix): {utf8_preview}");

            if decode_trace_blob(&sample).is_some() {
                eprintln!("  decode probe: decode_trace_blob -> ok");
            } else {
                match AlkanesTrace::decode(sample.as_slice()) {
                    Ok(_) => eprintln!("  decode probe: raw decode ok"),
                    Err(e) => eprintln!("  decode probe: raw decode error: {e}"),
                }
                if sample.len() >= 4 {
                    match AlkanesTrace::decode(&sample[..sample.len() - 4]) {
                        Ok(_) => eprintln!("  decode probe: strip last 4 bytes -> ok"),
                        Err(e) => eprintln!("  decode probe: strip last 4 bytes -> {e}"),
                    }
                }
            }
        }
    }

    println!(
        "block {block}: pointers={} blob_keys={} blobs={} traces={}",
        pointers,
        keys.len(),
        blobs_found,
        traces.len()
    );

    Ok(traces)
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(parent) = Path::new(&args.secondary_path).parent() {
        fs::create_dir_all(parent)?;
    }

    let sdb = SDB::open(
        &args.readonly_metashrew_db_dir,
        &args.secondary_path,
        Duration::from_millis(args.sdb_poll_ms),
    )?;
    let _ = sdb.catch_up_now();

    for b in &args.blocks {
        match traces_for_block(&sdb, *b) {
            Ok(traces) => {
                for (i, t) in traces.iter().enumerate().take(5) {
                    println!(
                        "  [{b}] trace[{i}] outpoint={} len={}",
                        hex::encode(&t.outpoint),
                        t.protobuf_trace.encoded_len()
                    );
                }
            }
            Err(e) => eprintln!("block {b}: error {e:?}"),
        }
    }

    Ok(())
}
