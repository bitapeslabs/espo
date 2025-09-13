use crate::utils::cli::get_config;
use anyhow::Result;

pub fn get_sdb_path_for_metashrew() -> Result<String> {
    let cfg = get_config();
    Ok(format!("{}{}", cfg.tmp_dbs_dir.clone(), "/metashrew"))
}

pub fn get_sdb_path_for_electrs() -> Result<String> {
    let cfg = get_config();
    Ok(format!("{}{}", cfg.tmp_dbs_dir.clone(), "/electrs"))
}
