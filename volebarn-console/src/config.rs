//! Console application configuration

use crate::{Result, ConsoleError};
use std::path::PathBuf;

pub struct Config {
    pub server_url: String,
    pub local_folder: PathBuf,
}

impl Config {
    pub fn load(
        config_path: Option<&str>,
        server_url: String,
        local_folder: Option<String>,
    ) -> Result<Self> {
        let local_folder = local_folder
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
            
        Ok(Config {
            server_url,
            local_folder,
        })
    }
}