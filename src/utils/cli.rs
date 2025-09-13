use anyhow::Result;
use clap::Parser;
use electrum_client::Client;
use std::{fs, path::Path, sync::OnceLock, time::Duration};

use crate::runtime::{paths::get_sdb_path_for_metashrew, sdb::SDB};

static CONFIG: OnceLock<CliArgs> = OnceLock::new();
static ELECTRUM_CLIENT: OnceLock<Client> = OnceLock::new();
static METASHREW_SDB: OnceLock<std::sync::Arc<SDB>> = OnceLock::new();

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    #[arg(short, long)]
    pub readonly_metashrew_db_dir: String,

    #[arg(short, long)]
    pub electrum_rpc_url: String,

    #[arg(short, long, default_value = "./db/tmp")]
    pub tmp_dbs_dir: String,

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
    let secondary_path = get_sdb_path_for_metashrew()?; // likely uses tmp_dbs_dir under the hood
    let sdb = SDB::open(
        args.readonly_metashrew_db_dir.clone(),
        secondary_path,
        Duration::from_millis(args.sdb_poll_ms as u64),
    )?;
    METASHREW_SDB
        .set(std::sync::Arc::new(sdb))
        .map_err(|_| anyhow::anyhow!("metashrew SDB already initialized"))?;

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
