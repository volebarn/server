//! Main console application

use crate::{Config, Result, ConsoleError};

pub struct App {
    config: Config,
}

impl App {
    pub async fn new(config: Config) -> Result<Self> {
        Ok(App { config })
    }
    
    pub async fn run(self) -> Result<()> {
        Err(ConsoleError::Config("Not implemented".to_string()))
    }
}