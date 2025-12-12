use anyhow::{Context, Result};
use bitcoin::block::Block;
use bitcoin::consensus::encode::deserialize;
use bitcoin::{Txid, Wtxid};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Debug helper: read a block from a specific blk*.dat offset/len and dump txids/wtxids.
fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let file = args.next().expect("usage: dump_block_slice <blkfile> <offset> <len>");
    let offset: u64 = args
        .next()
        .expect("missing offset")
        .parse()
        .expect("offset must be integer");
    let len: usize = args
        .next()
        .expect("missing len")
        .parse()
        .expect("len must be integer");

    let path = PathBuf::from(file);
    let mut f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    f.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek to {} in {}", offset, path.display()))?;

    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)
        .with_context(|| format!("read {} bytes from {}", len, path.display()))?;

    let block: Block = deserialize(&buf).context("deserialize block")?;
    println!(
        "block slice: txs={} (hash={})",
        block.txdata.len(),
        block.block_hash()
    );

    for (i, tx) in block.txdata.iter().enumerate() {
        let txid: Txid = tx.compute_txid();
        let wtxid: Wtxid = tx.compute_wtxid();
        println!("{:5} txid={} wtxid={}", i, txid, wtxid);
    }

    Ok(())
}
