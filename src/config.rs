use anyhow::Result;
use clap::Parser;
use electrum_client::Client;
use rocksdb::{DB, Options};
use std::{fs, path::Path, sync::OnceLock, time::Duration};

use crate::runtime::{dbpaths::get_sdb_path_for_metashrew, sdb::SDB};

static CONFIG: OnceLock<CliArgs> = OnceLock::new();
static ELECTRUM_CLIENT: OnceLock<Client> = OnceLock::new();
static METASHREW_SDB: OnceLock<std::sync::Arc<SDB>> = OnceLock::new();
static ESPO_DB: OnceLock<std::sync::Arc<DB>> = OnceLock::new();

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    #[arg(short, long)]
    pub readonly_metashrew_db_dir: String,

    #[arg(short, long)]
    pub electrum_rpc_url: String,

    #[arg(short, long, default_value = "./db/tmp")]
    pub tmp_dbs_dir: String,

    /// Path for ESPO module DB (RocksDB dir). Will be created if missing.
    #[arg(long, default_value = "./db/espo")]
    pub espo_db_path: String,

    #[arg(short, long, default_value_t = 5000)]
    pub sdb_poll_ms: u16,

    #[arg(short = 'p', long, default_value_t = 8080)]
    pub port: u16,
}

pub fn init_config() -> Result<()> {
    let args = CliArgs::parse();

    // --- validations ---
    let db = Path::new(&args.readonly_metashrew_db_dir);
    if !db.exists() {
        anyhow::bail!("Database path does not exist: {}", args.readonly_metashrew_db_dir);
    }
    if !db.is_dir() {
        anyhow::bail!("Database path is not a directory: {}", args.readonly_metashrew_db_dir);
    }

    // Ensure tmp_dbs_dir exists (create if missing)
    let tmp = Path::new(&args.tmp_dbs_dir);
    if !tmp.exists() {
        fs::create_dir_all(tmp).map_err(|e| {
            anyhow::anyhow!("Failed to create tmp_dbs_dir {}: {e}", args.tmp_dbs_dir)
        })?;
    } else if !tmp.is_dir() {
        anyhow::bail!("Temporary dbs dir is not a directory: {}", args.tmp_dbs_dir);
    }

    // Ensure espo_db_path exists (create if missing)
    let espo_dir = Path::new(&args.espo_db_path);
    if !espo_dir.exists() {
        fs::create_dir_all(espo_dir).map_err(|e| {
            anyhow::anyhow!("Failed to create espo_db_path {}: {e}", args.espo_db_path)
        })?;
    } else if !espo_dir.is_dir() {
        anyhow::bail!("espo_db_path is not a directory: {}", args.espo_db_path);
    }

    if args.sdb_poll_ms == 0 {
        anyhow::bail!("sdb_poll_ms must be greater than 0");
    }

    // --- store config ---
    CONFIG
        .set(args.clone())
        .map_err(|_| anyhow::anyhow!("config already initialized"))?;

    // --- init Electrum client once ---
    let electrum_url = format!("tcp://{}", args.electrum_rpc_url);
    let client = Client::new(&electrum_url)?;
    ELECTRUM_CLIENT
        .set(client)
        .map_err(|_| anyhow::anyhow!("electrum client already initialized"))?;

    // --- init Secondary RocksDB (SDB) once ---
    let secondary_path = get_sdb_path_for_metashrew()?;
    let sdb = SDB::open(
        args.readonly_metashrew_db_dir.clone(),
        secondary_path,
        Duration::from_millis(args.sdb_poll_ms as u64),
    )?;
    METASHREW_SDB
        .set(std::sync::Arc::new(sdb))
        .map_err(|_| anyhow::anyhow!("metashrew SDB already initialized"))?;

    // --- init ESPO RocksDB once ---
    let mut espo_opts = Options::default();
    espo_opts.create_if_missing(true);
    let espo_db = DB::open(&espo_opts, &args.espo_db_path)?;
    ESPO_DB
        .set(std::sync::Arc::new(espo_db))
        .map_err(|_| anyhow::anyhow!("ESPO DB already initialized"))?;

    Ok(())
}

pub fn get_config() -> &'static CliArgs {
    CONFIG.get().expect("init_config() must be called once at startup")
}

pub fn get_electrum_client() -> &'static Client {
    ELECTRUM_CLIENT.get().expect("init_config() must be called once at startup")
}

/// Cloneable handle to the live secondary RocksDB
pub fn get_metashrew_sdb() -> std::sync::Arc<SDB> {
    std::sync::Arc::clone(
        METASHREW_SDB.get().expect("init_config() must be called once at startup"),
    )
}

/// Getter for the ESPO module DB path (directory for RocksDB)
pub fn get_espo_db_path() -> &'static str {
    &get_config().espo_db_path
}

/// Cloneable handle to the global ESPO RocksDB
pub fn get_espo_db() -> std::sync::Arc<DB> {
    std::sync::Arc::clone(ESPO_DB.get().expect("init_config() must be called once at startup"))
}
