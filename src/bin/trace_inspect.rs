use anyhow::{Context, Result};
use rocksdb::{DB, Direction, IteratorMode, Options, ReadOptions};

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

fn pointer_to_blob_key(prefix: &[u8], key: &[u8], val: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    // key: /trace/<block_le>/<idx>
    let suffix = &key[prefix.len()..];
    let parts: Vec<&[u8]> = suffix.split(|b| *b == b'/').collect();
    if parts.len() != 2 {
        return None;
    }
    let idx = parts[1];

    let val_str = std::str::from_utf8(val).ok()?;
    let (_block_str, hex_part) = val_str.split_once(':')?;
    let hex_bytes = hex::decode(hex_part).ok()?;
    if hex_bytes.len() < 36 {
        return None;
    }

    let (tx_be, vout) = hex_bytes.split_at(32);

    let mut blob_key = Vec::with_capacity(7 + tx_be.len() + vout.len() + 1 + idx.len());
    blob_key.extend_from_slice(b"/trace/");
    blob_key.extend_from_slice(tx_be);
    blob_key.extend_from_slice(vout);
    blob_key.push(b'/');
    blob_key.extend_from_slice(idx);

    let mut outpoint_le = tx_be.to_vec();
    outpoint_le.reverse();
    outpoint_le.extend_from_slice(vout);

    Some((blob_key, outpoint_le))
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let block: u64 = args
        .next()
        .context("usage: trace_inspect <block> [db_path]")?
        .parse()
        .context("block must be integer")?;
    let db_path = args.next().unwrap_or_else(|| "/data/.metashrew/v9/.metashrew-v9".to_string());

    let mut opts = Options::default();
    opts.create_if_missing(false);
    let db = DB::open_for_read_only(&opts, &db_path, false)
        .with_context(|| format!("open RocksDB at {db_path}"))?;

    let mut prefix = b"/trace/".to_vec();
    prefix.extend_from_slice(&block.to_le_bytes());
    prefix.push(b'/');

    let mut ro = ReadOptions::default();
    if let Some(ub) = next_prefix(prefix.clone()) {
        ro.set_iterate_upper_bound(ub);
    }
    ro.set_total_order_seek(true);

    let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);

    let mut first_pointers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut blob_keys: Vec<Vec<u8>> = Vec::new();
    let mut outpoints: Vec<Vec<u8>> = Vec::new();
    let mut pointer_count = 0usize;

    while let Some(Ok((k, v))) = it.next() {
        if !k.starts_with(&prefix) {
            break;
        }

        pointer_count += 1;
        if first_pointers.len() < 5 {
            first_pointers.push((k.to_vec(), v.to_vec()));
        }

        if let Some((blob_key, outpoint_le)) = pointer_to_blob_key(&prefix, &k, &v) {
            blob_keys.push(blob_key);
            outpoints.push(outpoint_le);
        }
    }

    println!("pointer keys for block {block}: {}", pointer_count);
    for (i, (k, v)) in first_pointers.iter().enumerate() {
        println!("ptr[{i}] key={} val_utf8={}", hex::encode(k), String::from_utf8_lossy(v));
    }

    let mut found = 0usize;
    let mut sample_len: Option<usize> = None;
    for res in db.multi_get(blob_keys.iter()) {
        match res {
            Ok(Some(val)) => {
                found += 1;
                if sample_len.is_none() {
                    sample_len = Some(val.len());
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("multi_get error: {e}"),
        }
    }

    println!("blob keys derived: {} (found {})", blob_keys.len(), found);
    if let Some(len) = sample_len {
        println!("example blob length: {len} bytes");
    }

    Ok(())
}
