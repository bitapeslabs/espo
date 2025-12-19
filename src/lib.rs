pub mod alkanes;
pub mod config;
pub mod consts;
pub mod core;
pub mod explorer;
pub mod modules;
pub mod runtime;
pub mod schemas;
pub mod utils;

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;

// Shared ESPO height cell (used by config helpers and the indexer).
pub static ESPO_HEIGHT: OnceLock<Arc<AtomicU32>> = OnceLock::new();
