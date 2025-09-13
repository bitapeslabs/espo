use anyhow::Result;
use clap::Parser;
use std::path::Path;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    #[arg(short, long)]
    pub metashrew_db_path: String,

    #[arg(short = 'p', long, default_value_t = 8080)]
    pub port: u16,
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

    Ok(args)
}
