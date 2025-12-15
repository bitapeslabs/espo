pub mod electrum_like;

use std::time::{Duration, Instant};

/// Tracks a simple running *average* of seconds per block.
pub struct EtaTracker {
    block_start: Option<Instant>,
    total_secs: f64,
    blocks_measured: u64,
    /// Fallback until at least one block finishes.
    fallback_secs_per_block: f64,
}

impl EtaTracker {
    /// `fallback_secs_per_block` is used before any blocks are measured.
    pub fn new(fallback_secs_per_block: f64) -> Self {
        Self { block_start: None, total_secs: 0.0, blocks_measured: 0, fallback_secs_per_block }
    }

    /// Call right before you start indexing a block.
    pub fn start_block(&mut self) {
        self.block_start = Some(Instant::now());
    }

    /// Call right after you finish indexing a block (only on success).
    pub fn finish_block(&mut self) {
        if let Some(start) = self.block_start.take() {
            let secs = start.elapsed().as_secs_f64();
            self.total_secs += secs;
            self.blocks_measured += 1;
        }
    }

    /// Current average seconds per block (or fallback if none measured yet).
    pub fn secs_per_block(&self) -> f64 {
        if self.blocks_measured == 0 {
            self.fallback_secs_per_block
        } else {
            self.total_secs / self.blocks_measured as f64
        }
    }

    /// Estimate remaining time for `remaining_blocks`.
    pub fn eta(&self, remaining_blocks: u32) -> Duration {
        let total_secs = self.secs_per_block() * remaining_blocks as f64;
        Duration::from_secs_f64(total_secs.max(0.0))
    }
}

/// Pretty format a `Duration` like `3h 07m`, `12m 05s`, or `42s`.
pub fn fmt_duration(d: Duration) -> String {
    let total = d.as_secs();
    let (h, rem) = (total / 3600, total % 3600);
    let (m, s) = (rem / 60, rem % 60);
    if h > 0 {
        format!("{h}h {m:02}m")
    } else if m > 0 {
        format!("{m}m {s:02}s")
    } else {
        format!("{s}s")
    }
}
