use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use crate::runtime::mdb::Mdb;

const TIMER_DB_PREFIX: &[u8] = b"/timer_totals/v1/";
const TIMER_DB_SEP: u8 = 0x1f;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum TimerKind {
    Function,
    Section,
}

impl TimerKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Function => "fn",
            Self::Section => "section",
        }
    }

    fn as_byte(self) -> u8 {
        match self {
            Self::Function => b'f',
            Self::Section => b's',
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'f' => Some(Self::Function),
            b's' => Some(Self::Section),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TimerKey {
    kind: TimerKind,
    module: String,
    label: String,
}

#[derive(Clone, Debug, Default)]
struct TimerTotals {
    count: u64,
    total_ms: u64,
    max_ms: u64,
    min_ms: u64,
    last_ms: u64,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct TimerTotalsDiskV1 {
    count: u64,
    total_ms: u64,
    max_ms: u64,
    min_ms: u64,
    last_ms: u64,
}

impl From<&TimerTotals> for TimerTotalsDiskV1 {
    fn from(value: &TimerTotals) -> Self {
        Self {
            count: value.count,
            total_ms: value.total_ms,
            max_ms: value.max_ms,
            min_ms: value.min_ms,
            last_ms: value.last_ms,
        }
    }
}

impl From<TimerTotalsDiskV1> for TimerTotals {
    fn from(value: TimerTotalsDiskV1) -> Self {
        Self {
            count: value.count,
            total_ms: value.total_ms,
            max_ms: value.max_ms,
            min_ms: value.min_ms,
            last_ms: value.last_ms,
        }
    }
}

#[derive(Debug)]
struct TimerState {
    loaded: bool,
    totals: HashMap<TimerKey, TimerTotals>,
    dirty: HashSet<TimerKey>,
}

impl Default for TimerState {
    fn default() -> Self {
        Self { loaded: false, totals: HashMap::new(), dirty: HashSet::new() }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TimerTotalsEntry {
    pub title: String,
    pub kind: String,
    pub module: String,
    pub label: String,
    pub count: u64,
    pub total_ms: u64,
    pub avg_ms: f64,
    pub max_ms: u64,
    pub min_ms: u64,
    pub last_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TimerTotalsSnapshot {
    pub entries: Vec<TimerTotalsEntry>,
    pub total_ms: u64,
    pub total_calls: u64,
    pub total_entries: usize,
}

static DEBUG_MDB: OnceLock<Mdb> = OnceLock::new();
static TIMER_STATE: OnceLock<Mutex<TimerState>> = OnceLock::new();

fn debug_mdb() -> &'static Mdb {
    DEBUG_MDB.get_or_init(|| Mdb::from_db(crate::config::get_espo_db(), b"debug_metrics:"))
}

fn timer_state() -> &'static Mutex<TimerState> {
    TIMER_STATE.get_or_init(|| Mutex::new(TimerState::default()))
}

fn timer_db_key(key: &TimerKey) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(TIMER_DB_PREFIX.len() + 2 + key.module.len() + 1 + key.label.len());
    out.extend_from_slice(TIMER_DB_PREFIX);
    out.push(key.kind.as_byte());
    out.push(TIMER_DB_SEP);
    out.extend_from_slice(key.module.as_bytes());
    out.push(TIMER_DB_SEP);
    out.extend_from_slice(key.label.as_bytes());
    out
}

fn parse_timer_db_key(key: &[u8]) -> Option<TimerKey> {
    if !key.starts_with(TIMER_DB_PREFIX) {
        return None;
    }
    let rest = &key[TIMER_DB_PREFIX.len()..];
    if rest.len() < 3 || rest[1] != TIMER_DB_SEP {
        return None;
    }
    let kind = TimerKind::from_byte(rest[0])?;
    let parts = &rest[2..];
    let split = parts.iter().position(|b| *b == TIMER_DB_SEP)?;
    let module = std::str::from_utf8(&parts[..split]).ok()?.to_string();
    let label = std::str::from_utf8(&parts[(split + 1)..]).ok()?.to_string();
    Some(TimerKey { kind, module, label })
}

fn flush_dirty_locked(state: &mut TimerState) -> Result<(), String> {
    if state.dirty.is_empty() {
        return Ok(());
    }

    let mut keys: Vec<TimerKey> = state.dirty.iter().cloned().collect();
    keys.sort_by(|a, b| {
        a.module
            .cmp(&b.module)
            .then_with(|| a.label.cmp(&b.label))
            .then_with(|| a.kind.as_byte().cmp(&b.kind.as_byte()))
    });

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(keys.len());
    for key in &keys {
        let Some(totals) = state.totals.get(key) else {
            continue;
        };
        let value = TimerTotalsDiskV1::from(totals);
        let Ok(encoded) = borsh::to_vec(&value) else {
            continue;
        };
        puts.push((timer_db_key(key), encoded));
    }

    if puts.is_empty() {
        state.dirty.clear();
        return Ok(());
    }

    let write = debug_mdb().bulk_write(|wb| {
        for (k, v) in &puts {
            wb.put(k, v);
        }
    });

    match write {
        Ok(_) => {
            state.dirty.clear();
            Ok(())
        }
        Err(e) => Err(format!("debug timer flush write failed: {e}")),
    }
}

fn ensure_loaded_locked(state: &mut TimerState) {
    if state.loaded {
        return;
    }
    state.loaded = true;

    let Ok(entries) = debug_mdb().scan_prefix_entries(TIMER_DB_PREFIX) else {
        return;
    };

    for (raw_key, raw_value) in entries {
        let Some(key) = parse_timer_db_key(&raw_key) else {
            continue;
        };
        let Ok(decoded) = TimerTotalsDiskV1::try_from_slice(&raw_value) else {
            continue;
        };
        state.totals.insert(key, decoded.into());
    }
}

fn record_timer(kind: TimerKind, module: &str, label: &str, elapsed_ms: u64) {
    let mut guard = timer_state().lock().unwrap_or_else(|e| e.into_inner());
    ensure_loaded_locked(&mut guard);

    let key = TimerKey { kind, module: module.to_string(), label: label.to_string() };
    let entry = guard.totals.entry(key.clone()).or_insert_with(|| TimerTotals {
        count: 0,
        total_ms: 0,
        max_ms: 0,
        min_ms: elapsed_ms,
        last_ms: 0,
    });
    entry.count = entry.count.saturating_add(1);
    entry.total_ms = entry.total_ms.saturating_add(elapsed_ms);
    entry.max_ms = entry.max_ms.max(elapsed_ms);
    if entry.count == 1 {
        entry.min_ms = elapsed_ms;
    } else {
        entry.min_ms = entry.min_ms.min(elapsed_ms);
    }
    entry.last_ms = elapsed_ms;
    guard.dirty.insert(key);
}

pub fn flush_timer_totals() -> Result<(), String> {
    let mut guard = timer_state().lock().unwrap_or_else(|e| e.into_inner());
    ensure_loaded_locked(&mut guard);
    flush_dirty_locked(&mut guard)
}

pub fn reset_timer_totals() -> Result<usize, String> {
    let mut guard = timer_state().lock().unwrap_or_else(|e| e.into_inner());
    ensure_loaded_locked(&mut guard);

    let keys = debug_mdb()
        .scan_prefix_keys(TIMER_DB_PREFIX)
        .map_err(|e| format!("debug timer reset scan failed: {e}"))?;

    if !keys.is_empty() {
        debug_mdb()
            .bulk_write(|wb| {
                for key in &keys {
                    wb.delete(key);
                }
            })
            .map_err(|e| format!("debug timer reset delete failed: {e}"))?;
    }

    guard.totals.clear();
    guard.dirty.clear();
    guard.loaded = true;
    Ok(keys.len())
}

pub fn get_timer_totals(limit: Option<usize>) -> TimerTotalsSnapshot {
    let mut guard = timer_state().lock().unwrap_or_else(|e| e.into_inner());
    ensure_loaded_locked(&mut guard);
    if let Err(e) = flush_dirty_locked(&mut guard) {
        eprintln!("[debug] {e}");
    }

    let mut entries: Vec<TimerTotalsEntry> = guard
        .totals
        .iter()
        .map(|(key, totals)| {
            let avg_ms =
                if totals.count == 0 { 0.0 } else { totals.total_ms as f64 / totals.count as f64 };
            TimerTotalsEntry {
                title: format!("module={} {}={}", key.module, key.kind.as_str(), key.label),
                kind: key.kind.as_str().to_string(),
                module: key.module.clone(),
                label: key.label.clone(),
                count: totals.count,
                total_ms: totals.total_ms,
                avg_ms,
                max_ms: totals.max_ms,
                min_ms: totals.min_ms,
                last_ms: totals.last_ms,
            }
        })
        .collect();

    let total_ms: u64 = entries.iter().map(|e| e.total_ms).sum();
    let total_calls: u64 = entries.iter().map(|e| e.count).sum();
    let total_entries = entries.len();

    entries.sort_by(|a, b| {
        b.total_ms
            .cmp(&a.total_ms)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.title.cmp(&b.title))
    });
    if let Some(limit) = limit {
        entries.truncate(limit);
    }

    TimerTotalsSnapshot { entries, total_ms, total_calls, total_entries }
}

pub struct DebugTimer {
    module: &'static str,
    name: &'static str,
    start: Instant,
}

impl DebugTimer {
    pub fn new(module: &'static str, name: &'static str) -> Self {
        Self { module, name, start: Instant::now() }
    }
}

impl Drop for DebugTimer {
    fn drop(&mut self) {
        let elapsed_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        record_timer(TimerKind::Function, self.module, self.name, elapsed_ms);
        let ignore_below = crate::config::debug_ignore_ms() as u128;
        if ignore_below != 0 && (elapsed_ms as u128) < ignore_below {
            return;
        }
        eprintln!("[debug] module={} fn={} elapsed_ms={}", self.module, self.name, elapsed_ms);
    }
}

#[macro_export]
macro_rules! debug_timer_log {
    ($name:expr) => {
        let _debug_timer = if crate::config::debug_enabled() {
            Some(crate::debug::DebugTimer::new(std::module_path!(), $name))
        } else {
            None
        };
    };
}

pub fn start_if(enabled: bool) -> Option<Instant> {
    if enabled { Some(Instant::now()) } else { None }
}

pub fn log_elapsed(module: &str, section: &str, start: Option<Instant>) {
    if let Some(start) = start {
        let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        record_timer(TimerKind::Section, module, section, elapsed_ms);
        let ignore_below = crate::config::debug_ignore_ms() as u128;
        if ignore_below != 0 && (elapsed_ms as u128) < ignore_below {
            return;
        }
        eprintln!("[debug] module={} section={} elapsed_ms={}", module, section, elapsed_ms);
    }
}
