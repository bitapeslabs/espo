// blockfetcher.rs
use anyhow::{Context, Result};
use bitcoincore_rpc::bitcoin::hashes::Hash; // for to_byte_array()
use bitcoincore_rpc::bitcoin::{Block, BlockHash, Network, consensus};
use bitcoincore_rpc::{Client as CoreClient, RpcApi};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use crate::config::{get_bitcoind_rpc_client, get_espo_db};
use crate::consts::alkanes_genesis_block;
use crate::runtime::mdb::Mdb;

/// === Tuning ==================================================================
/// Max expected payload size from blk header (sanity).
const MAX_BLOCK_PAYLOAD: u32 = 8_000_000;
/// If height is within this distance from tip, fetch via RPC (avoid file tail races).
const NEAR_TIP_RPC_THRESHOLD: u32 = 6_000;
/// ============================================================================

/// Public trait: source of blocks for a given height.
pub trait BlockSource {
    /// Returns the full block for `height`. `tip` is used to optionally route near-tip to RPC.
    fn get_block_by_height(&self, height: u32, tip: u32) -> Result<Block>;
}

/// Borsh-encoded value stored in Mdb for each block hash.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BlockFileLocationDescriptor {
    /// blk file number (e.g. 5159 for blk05159.dat)
    pub file_no: u32,
    /// byte offset of the *payload* (right after the 8B [magic|len] record header)
    pub offset: u64,
    /// payload length in bytes from the record header
    pub len: u32,
    /// cached tx count (handy; not required to load the block)
    pub txs: u32,
}

/// In-memory decoded cache for exactly ONE blk file.
#[derive(Default)]
struct DecodedFileCache {
    file_no: Option<u32>,
    // All decoded blocks from that file (ACTIVE-CHAIN ONLY — verified via RPC):
    blocks: HashMap<BlockHash, Block>,
}

/// Main implementation: uses Mdb-backed index of blk files, falling back to Core RPC.
pub struct BlkOrRpcBlockSource {
    mdb: Mdb,
    blocks_dir: PathBuf,
    network: Network,
    rpc: &'static CoreClient, // borrow the global RPC client from config

    /// Stop indexing once this hash is known in the index (genesis of Alkanes range)
    genesis_stop_hash: Option<BlockHash>,

    /// Decoded-block cache for the most recent blk file we touched.
    decoded_cache: Mutex<DecodedFileCache>,

    /// Preloaded mapping of height -> block hash for everything we already have indexed.
    height_to_hash: Mutex<HashMap<u32, BlockHash>>,
}

impl BlkOrRpcBlockSource {
    /// Namespace prefix inside ESPO DB for this index. (Literal; includes the trailing slash.)
    pub const MDB_PREFIX: &'static str = "block_core_index/";

    pub fn new(
        blocks_dir: impl AsRef<Path>,
        network: Network,
        rpc: &'static CoreClient,
    ) -> Result<Self> {
        let db = get_espo_db(); // Arc<DB> from config
        let mdb = Mdb::from_db(db, Self::MDB_PREFIX);

        // Precompute the “stop at genesis” hash for this network (if > 0)
        let genesis_height = alkanes_genesis_block(network);
        let genesis_stop_hash = if genesis_height > 0 {
            match rpc.get_block_hash(genesis_height as u64) {
                Ok(h) => Some(h),
                Err(e) => {
                    eprintln!(
                        "[BLOCKFETCHER] warn: failed to fetch genesis stop hash at height {}: {:?}",
                        genesis_height, e
                    );
                    None
                }
            }
        } else {
            None
        };

        // Preload height->hash for everything currently indexed under our namespace.
        let height_map = Self::build_height_map(&mdb, rpc)?;
        eprintln!(
            "[BLOCKFETCHER] preloaded {} height→hash entries from index (~{} KB)",
            height_map.len(),
            approx_height_map_kb(height_map.len())
        );

        Ok(Self {
            mdb,
            blocks_dir: blocks_dir.as_ref().to_path_buf(),
            network,
            rpc,
            genesis_stop_hash,
            decoded_cache: Mutex::new(DecodedFileCache::default()),
            height_to_hash: Mutex::new(height_map),
        })
    }

    /// Convenience constructor that uses the Core RPC client from config directly.
    pub fn new_with_config(blocks_dir: impl AsRef<Path>, network: Network) -> Result<Self> {
        let rpc: &'static CoreClient = get_bitcoind_rpc_client();
        Self::new(blocks_dir, network, rpc)
    }

    /// Utility: rough size estimate for logging (bytes per entry ~ (u32 height + 32B hash) = 36B).
    #[inline]
    fn log_height_map_stats(wherefrom: &str, entries: usize) {
        eprintln!(
            "[BLOCKFETCHER] {} height→hash: {} entries (~{} KB)",
            wherefrom,
            entries,
            approx_height_map_kb(entries)
        );
    }

    /// Scan our namespace and build a map {height -> hash} for every hash we’ve indexed.
    /// We filter out file-markers (5B keys). For each 32B key, query header info once to get height.
    fn build_height_map(mdb: &Mdb, rpc: &CoreClient) -> Result<HashMap<u32, BlockHash>> {
        let mut out: HashMap<u32, BlockHash> = HashMap::new();
        eprintln!("[BLOCKFETCHER] Loading height map from DB (first run may take a bit)...");
        let keys = mdb.scan_prefix(&[]).context("scan_prefix for block_core_index/")?;

        for rel in keys {
            if rel.len() != 32 {
                continue; // skip 'F' markers or anything unexpected
            }
            let hash = match BlockHash::from_slice(&rel) {
                Ok(h) => h,
                Err(_) => continue,
            };
            match rpc.get_block_header_info(&hash) {
                Ok(hdr) => {
                    // Only map ACTIVE chain blocks (confirmations > 0). Skip stale/orphans here.
                    if hdr.confirmations > 0 {
                        out.insert(hdr.height as u32, hash);
                    }
                }
                Err(e) => {
                    // Best-effort: if pruned or unknown, just skip
                    eprintln!("[BLOCKFETCHER] build_height_map: header({hash}) err: {:?}", e);
                }
            }
        }

        Ok(out)
    }

    /// Rebuild and replace the in-memory height→hash map from RocksDB.
    fn refresh_height_map_from_db(&self) -> Result<()> {
        let new_map = Self::build_height_map(&self.mdb, self.rpc)?;
        Self::log_height_map_stats("refreshed", new_map.len());
        *self.height_to_hash.lock().unwrap() = new_map;
        Ok(())
    }

    #[inline]
    fn network_magic(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 0xD9B4BEF9,
            Network::Testnet => 0x0709_110B,
            Network::Signet => 0x0A03_CF40,
            Network::Regtest => 0xDAB5_BFFA,
            _ => 0xD9B4BEF9,
        }
    }

    /// Metadata key (under the same Mdb prefix) marking a blk file as "already indexed".
    #[inline]
    fn meta_key_file_indexed(file_no: u32) -> [u8; 5] {
        let mut k = [0u8; 5];
        k[0] = b'F';
        k[1..5].copy_from_slice(&file_no.to_le_bytes());
        k
    }

    /// Extract file number from "blk05159.dat" => 5159.
    fn parse_file_no(p: &Path) -> Result<u32> {
        let name = p
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("bad blk filename: {}", p.display()))?;
        let stem = name.trim_start_matches("blk").trim_end_matches(".dat");
        Ok(stem.parse::<u32>()?)
    }

    /// List blk files newest → oldest by filename.
    fn list_blk_files_desc(&self) -> Result<Vec<PathBuf>> {
        let mut v: Vec<PathBuf> = fs::read_dir(&self.blocks_dir)
            .with_context(|| format!("read_dir {}", self.blocks_dir.display()))?
            .filter_map(|e| {
                let p = e.ok()?.path();
                let name = p.file_name()?.to_string_lossy().to_string();
                if p.extension().map(|e| e == "dat").unwrap_or(false) && name.starts_with("blk") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        v.sort_by(|a, b| b.file_name().cmp(&a.file_name())); // newest first
        Ok(v)
    }

    /// Fetch a location from the index (None if not present).
    #[inline]
    fn index_get(&self, hash: &BlockHash) -> Result<Option<BlockFileLocationDescriptor>> {
        let key = hash.to_byte_array(); // local [u8;32] buffer keeps lifetime simple
        if let Some(val) = self.mdb.get(&key)? {
            let loc =
                BlockFileLocationDescriptor::try_from_slice(&val).context("borsh decode loc")?;
            Ok(Some(loc))
        } else {
            Ok(None)
        }
    }

    /// Check whether a file_no has already been indexed.
    #[inline]
    fn is_file_indexed(&self, file_no: u32) -> Result<bool> {
        let key = Self::meta_key_file_indexed(file_no);
        Ok(self.mdb.get(&key)?.is_some())
    }

    /// Verify a decoded block against Core:
    /// - It must be **in the active chain** (confirmations > 0).
    /// - If decode was from disk and we suspect mismatch, we could fetch RPC body (kept as hook).
    fn verify_block_active_via_rpc(&self, h: &BlockHash, blk: &Block) -> Result<Option<Block>> {
        match self.rpc.get_block_header_info(h) {
            Ok(info) => {
                if info.confirmations <= 0 {
                    // Not in active chain → do not cache this body.
                    eprintln!(
                        "[BLOCKFETCHER] skip cache: {} is not in active chain (confs={})",
                        h, info.confirmations
                    );
                    return Ok(None);
                }
                // Optional: if you want extra paranoia, you could refetch and compare merkle.
                // Here we trust the file decode for active blocks; keep RPC fallback hook:
                Ok(Some(blk.clone()))
            }
            Err(e) => {
                eprintln!(
                    "[BLOCKFETCHER] header({}) not known ({}). Trying RPC get_block as fallback…",
                    h, e
                );
                // If Core is pruned and lacks the header, or some transient issue — best effort.
                match self.rpc.get_block(h) {
                    Ok(b) => Ok(Some(b)),
                    Err(e2) => {
                        eprintln!("[BLOCKFETCHER] RPC get_block({}) failed: {:?}", h, e2);
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Fully decode **all blocks** in the given blk file into the single-file cache.
    /// Only ACTIVE-CHAIN blocks (confirmations > 0) are inserted into the cache.
    fn ensure_decoded_file_cached(&self, file_no: u32) -> Result<()> {
        let mut cache = self.decoded_cache.lock().unwrap();
        if cache.file_no == Some(file_no) {
            return Ok(());
        }

        let path = self.blocks_dir.join(format!("blk{:05}.dat", file_no));
        eprintln!(
            "[BLOCKFETCHER] warming decoded cache for file {}",
            path.file_name().unwrap().to_string_lossy()
        );

        // Reset cache for new file
        cache.blocks.clear();
        cache.file_no = Some(file_no);

        let expected_magic = self.network_magic();
        let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut r = BufReader::with_capacity(16 << 20, f);

        loop {
            let mut header = [0u8; 8];
            if let Err(e) = r.read_exact(&mut header) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    eprintln!("[BLOCKFETCHER] cache warm read header {}: {:?}", path.display(), e);
                    break;
                }
            }
            if header.iter().all(|&b| b == 0) {
                break; // zero padding at tail
            }

            let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if magic != expected_magic || len == 0 || len > MAX_BLOCK_PAYLOAD {
                eprintln!(
                    "[BLOCKFETCHER] cache warm: bad record (magic={:#X}, len={}) in {}",
                    magic,
                    len,
                    path.display()
                );
                break;
            }

            let mut payload = vec![0u8; len as usize];
            if let Err(e) = r.read_exact(&mut payload) {
                eprintln!("[BLOCKFETCHER] cache warm payload {}: {:?}", path.display(), e);
                break;
            }

            let blk_from_file: Block = match consensus::encode::deserialize(&payload) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("[BLOCKFETCHER] cache warm decode {}: {:?}", path.display(), e);
                    break;
                }
            };
            let h = blk_from_file.block_hash();

            // === NEW: verify against RPC and only cache ACTIVE-CHAIN blocks ===
            match self.verify_block_active_via_rpc(&h, &blk_from_file)? {
                Some(verified) => {
                    cache.blocks.insert(h, verified);
                }
                None => {
                    // Skip inserting; either not in active chain or RPC failed.
                }
            }
        }

        Ok(())
    }

    /// Index a single blk file: read each record and store (hash → Borsh(loc)) in ONE batch.
    /// Returns (#blocks_indexed, last_block_height_opt) where last_block_height_opt is the height
    /// of the **last** block in this file (via RPC), used for the progress estimate.
    fn index_file(&self, path: &Path, file_no: u32) -> Result<(usize, Option<u32>)> {
        let t0 = Instant::now();
        eprintln!(
            "[BLOCKFETCHER] indexing file {} (no={})",
            path.file_name().unwrap().to_string_lossy(),
            file_no
        );

        let expected_magic = self.network_magic();

        // Open read-only; if missing (pruned), just skip.
        let f = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[BLOCKFETCHER] skip missing {}: {:?}", path.display(), e);
                return Ok((0, None));
            }
        };
        let mut r = BufReader::with_capacity(16 << 20, f);

        let mut file_pos: u64 = 0u64;
        let mut blocks = 0usize;
        let mut last_hash: Option<BlockHash> = None;

        self.mdb.bulk_write(|wb| {
            loop {
                let mut header = [0u8; 8];
                if let Err(e) = r.read_exact(&mut header) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        eprintln!("[BLOCKFETCHER] read header error {}: {:?}", path.display(), e);
                        break;
                    }
                }
                if header.iter().all(|&b| b == 0) {
                    break; // zero padding at tail
                }

                let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
                let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
                if magic != expected_magic {
                    eprintln!(
                        "[BLOCKFETCHER] bad magic in {} at pos {} (exp={:#X} got={:#X})",
                        path.display(),
                        file_pos,
                        expected_magic,
                        magic
                    );
                    break;
                }
                if len == 0 || len > MAX_BLOCK_PAYLOAD {
                    eprintln!(
                        "[BLOCKFETCHER] suspicious len={} in {} at pos {}; abort file",
                        len,
                        path.display(),
                        file_pos
                    );
                    break;
                }

                // Read payload and decode (full decode acceptable)
                let mut payload = vec![0u8; len as usize];
                if let Err(e) = r.read_exact(&mut payload) {
                    eprintln!("[BLOCKFETCHER] payload read error {}: {:?}", path.display(), e);
                    break;
                }

                let blk: Block = match consensus::encode::deserialize(&payload) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("[BLOCKFETCHER] decode error {}: {:?}", path.display(), e);
                        break;
                    }
                };
                let hash = blk.block_hash();
                last_hash = Some(hash);
                let txs = blk.txdata.len() as u32;

                // We still index *every* block hash → location (including stale),
                // because get_block_by_height always resolves a canonical hash via RPC first.
                let loc = BlockFileLocationDescriptor { file_no, offset: file_pos + 8, len, txs };
                let key = hash.to_byte_array();
                let val = borsh::to_vec(&loc).expect("borsh encode loc");
                wb.put(&key, &val);

                blocks += 1;
                file_pos += 8 + len as u64;
            }

            // mark file as indexed in same batch
            let mark_key = Self::meta_key_file_indexed(file_no);
            wb.put(&mark_key, &[1u8]);
        })?;

        // Get the height of the last block in this file for progress estimation
        let last_height = match last_hash {
            Some(h) => match self.rpc.get_block_header_info(&h) {
                Ok(info) => Some(info.height as u32),
                Err(e) => {
                    eprintln!("[BLOCKFETCHER] warn: get_block_header_info({h}) failed: {:?}", e);
                    None
                }
            },
            None => None,
        };

        eprintln!(
            "[BLOCKFETCHER] indexed {}: {} blocks in {:.2?}",
            path.file_name().unwrap().to_string_lossy(),
            blocks,
            t0.elapsed()
        );
        Ok((blocks, last_height))
    }

    /// Ensure the index contains `hash`; if not, lazily index blk files (newest → older),
    /// skipping those marked as already indexed. We also **stop** once the Alkanes genesis
    /// stop-hash is present in the index, to avoid indexing pre-genesis files.
    /// Progress is: remaining ≈ last_height_in_file - alkanes_genesis_block(network).
    fn ensure_index_contains(&self, hash: &BlockHash, _target_height: u32) -> Result<bool> {
        if self.index_get(hash)?.is_some() {
            return Ok(true);
        }

        if let Some(stop_hash) = self.genesis_stop_hash {
            if self.index_get(&stop_hash)?.is_some() {
                eprintln!(
                    "[BLOCKFETCHER] stop-hash already indexed; target not found → stop scanning"
                );
                return Ok(false);
            }
        }

        let files = self.list_blk_files_desc()?;
        eprintln!(
            "[BLOCKFETCHER] ensure_index_contains: scanning {} files (newest→older) to find {}",
            files.len(),
            hash
        );

        let genesis_h = alkanes_genesis_block(self.network);
        let mut indexed_any = false;

        for (i, p) in files.iter().enumerate() {
            // Stop early if target already present.
            if self.index_get(hash)?.is_some() {
                eprintln!("[BLOCKFETCHER] found {} after scanning {} files", hash, i);
                break;
            }

            // Stop once stop-hash present in index (no pre-genesis scanning).
            if let Some(stop_hash) = self.genesis_stop_hash {
                if self.index_get(&stop_hash)?.is_some() {
                    eprintln!(
                        "[BLOCKFETCHER] stop-hash {} present after {} files; ending scan",
                        stop_hash, i
                    );
                    break;
                }
            }

            let file_no = match Self::parse_file_no(p) {
                Ok(n) => n,
                Err(_) => continue,
            };

            if self.is_file_indexed(file_no)? {
                eprintln!(
                    "[BLOCKFETCHER] skip already-indexed {} [{}/{}]",
                    p.file_name().unwrap().to_string_lossy(),
                    i + 1,
                    files.len(),
                );
                continue;
            }

            eprintln!(
                "[BLOCKFETCHER] → indexing {} [{}/{}]",
                p.file_name().unwrap().to_string_lossy(),
                i + 1,
                files.len()
            );

            match self.index_file(p, file_no) {
                Ok((delta, last_h_opt)) => {
                    indexed_any = true;
                    let remaining = last_h_opt.map(|h| h.saturating_sub(genesis_h)).unwrap_or(0);
                    eprintln!(
                        "   → file done: ~{} blocks indexed; approx ~{} to genesis (based on last block in file)",
                        delta, remaining
                    );
                }
                Err(e) => {
                    eprintln!("[BLOCKFETCHER] index_file failed {}: {:?}", p.display(), e);
                    // continue; RPC fallback still possible
                }
            }
        }

        // If we indexed anything this pass, rebuild (and log) the height→hash map now.
        if indexed_any {
            if let Err(e) = self.refresh_height_map_from_db() {
                eprintln!("[BLOCKFETCHER] warn: failed to refresh height map after scan: {:?}", e);
            }
        }

        Ok(self.index_get(hash)?.is_some())
    }

    /// Read a block directly from a known file location (with **single-file decoded cache**).
    /// Blocks added to the cache are verified against Core (active chain) in ensure_decoded_file_cached.
    fn read_block_from_loc(
        &self,
        hash: &BlockHash,
        loc: &BlockFileLocationDescriptor,
    ) -> Result<Block> {
        // Warm/flip the decoded cache to this file if needed (does active-chain verification).
        self.ensure_decoded_file_cached(loc.file_no)?;

        // Now serve from the in-memory map (O(1)) if present
        if let Some(b) = self.decoded_cache.lock().unwrap().blocks.get(hash).cloned() {
            return Ok(b);
        }

        // Fallback (rare): read just this one from disk, then verify before returning/caching.
        let path = self.blocks_dir.join(format!("blk{:05}.dat", loc.file_no));
        let mut f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        f.seek(SeekFrom::Start(loc.offset))
            .with_context(|| format!("seek {}", path.display()))?;
        let mut payload = vec![0u8; loc.len as usize];
        f.read_exact(&mut payload)
            .with_context(|| format!("read {} bytes {}", payload.len(), path.display()))?;
        let blk_from_file: Block =
            consensus::encode::deserialize(&payload).context("consensus decode block payload")?;
        let h = blk_from_file.block_hash();

        if &h != hash {
            eprintln!(
                "[BLOCKFETCHER] WARNING: payload hash {} != expected {}; trying RPC fallback…",
                h, hash
            );
        }

        // Verify via RPC (active chain). If accepted, also insert into cache for future calls.
        if let Some(verified) = self.verify_block_active_via_rpc(hash, &blk_from_file)? {
            self.decoded_cache.lock().unwrap().blocks.insert(*hash, verified.clone());
            return Ok(verified);
        }

        // As a last resort (e.g., pruned header lookup oddity), try direct RPC get_block
        eprintln!(
            "[BLOCKFETCHER] read_block_from_loc: disk body not verified; using RPC get_block({})",
            hash
        );
        let blk = self
            .rpc
            .get_block(hash)
            .with_context(|| format!("bitcoind: getblock({hash})"))?;
        // Don’t cache here unless you want to (it’s okay to cache — it’s active by virtue of height lookup)
        self.decoded_cache.lock().unwrap().blocks.insert(*hash, blk.clone());
        Ok(blk)
    }
}

impl BlockSource for BlkOrRpcBlockSource {
    fn get_block_by_height(&self, height: u32, tip: u32) -> Result<Block> {
        let t0 = Instant::now();
        eprintln!(
            "[BLOCKFETCHER] request height={} (tip={}, Δ={})",
            height,
            tip,
            tip.saturating_sub(height)
        );

        // 0) First: consult the preloaded height→hash map (already filtered to active chain)
        if let Some(h) = self.height_to_hash.lock().unwrap().get(&height).cloned() {
            if let Some(loc) = self.index_get(&h)? {
                eprintln!(
                    "[BLOCKFETCHER] height={} (preloaded map) using BLK (file={}, off={}, len={})",
                    height, loc.file_no, loc.offset, loc.len
                );
                let blk = self.read_block_from_loc(&h, &loc)?;
                eprintln!("[BLOCKFETCHER] height={} BLK ok in {:.2?}", height, t0.elapsed());
                return Ok(blk);
            }
            // If the map has the hash but location is missing (shouldn't happen), fall through.
        }

        // 1) height → hash via RPC (canonical)
        let hash: BlockHash = self
            .rpc
            .get_block_hash(height as u64)
            .with_context(|| format!("bitcoind: getblockhash({height})"))?;
        // Opportunistically cache this mapping for subsequent calls in the same run.
        self.height_to_hash.lock().unwrap().insert(height, hash);

        // Near-tip guard: direct RPC (avoid tail races on a file being appended)
        if tip.saturating_sub(height) <= NEAR_TIP_RPC_THRESHOLD {
            eprintln!("[BLOCKFETCHER] height={} using RPC (near tip)", height);
            let blk = self
                .rpc
                .get_block(&hash)
                .with_context(|| format!("bitcoind: getblock({hash})"))?;
            eprintln!("[BLOCKFETCHER] height={} RPC ok in {:.2?}", height, t0.elapsed());
            return Ok(blk);
        }

        // 2) Try local index → blk file
        if let Some(loc) = self.index_get(&hash)? {
            eprintln!(
                "[BLOCKFETCHER] height={} hash={} using BLK (file={}, off={}, len={})",
                height, hash, loc.file_no, loc.offset, loc.len
            );
            let blk = self.read_block_from_loc(&hash, &loc)?;
            eprintln!("[BLOCKFETCHER] height={} BLK ok in {:.2?}", height, t0.elapsed());
            return Ok(blk);
        }

        // 3) Lazily index files until found (but stop once stop-hash is present)
        eprintln!("[BLOCKFETCHER] height={} hash={} not in index → lazy index", height, hash);
        if self.ensure_index_contains(&hash, height)? {
            if let Some(loc) = self.index_get(&hash)? {
                eprintln!(
                    "[BLOCKFETCHER] height={} found after indexing → BLK (file={}, off={}, len={})",
                    height, loc.file_no, loc.offset, loc.len
                );
                let blk = self.read_block_from_loc(&hash, &loc)?;
                eprintln!("[BLOCKFETCHER] height={} BLK ok in {:.2?}", height, t0.elapsed());
                return Ok(blk);
            }
        }

        // 4) Fallback to RPC (e.g., pruned file or not in local blk files)
        eprintln!("[BLOCKFETCHER] height={} fallback to RPC (not in local blk files)", height);
        let blk = self
            .rpc
            .get_block(&hash)
            .with_context(|| format!("bitcoind: getblock({hash})"))?;
        eprintln!("[BLOCKFETCHER] height={} RPC ok in {:.2?}", height, t0.elapsed());
        Ok(blk)
    }
}

/// Helper for logging approximate memory use of the height map.
#[inline]
fn approx_height_map_kb(entries: usize) -> usize {
    // ~36 bytes per entry (u32 + 32B hash) — HashMap overhead not included.
    ((entries * 36) + 1023) / 1024
}
