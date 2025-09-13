use anyhow::Result;
use clap::Parser;
use std::path::Path;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    /// Path to Metashrew RocksDB
    #[arg(short, long)]
    pub metashrew_db_path: String,

    /// Port to bind (default 8080)
    #[arg(short = 'p', long, default_value_t = 8080)]
    pub port: u16,

    /// Endpoint prefix (must start with '/'), e.g. "/traces"
    #[arg(short = 'e', long, default_value = "/traces")]
    pub endpoint: String,
}

pub fn get_config() -> Result<CliArgs> {
    let mut args = CliArgs::parse();

    let metashrew_db_path = Path::new(&args.metashrew_db_path);
    if !metashrew_db_path.exists() {
        anyhow::bail!("Database path does not exist: {}", args.metashrew_db_path);
    }
    if !metashrew_db_path.is_dir() {
        anyhow::bail!("Database path is not a dir: {}", args.metashrew_db_path);
    }

    // Normalize endpoint: ensure it starts with '/' and has no trailing slash (except root).
    if !args.endpoint.starts_with('/') {
        args.endpoint = format!("/{}", args.endpoint);
    }
    if args.endpoint.len() > 1 && args.endpoint.ends_with('/') {
        args.endpoint.pop();
    }

    Ok(args)
}
