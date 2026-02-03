// Project:   dfe-archiver
// File:      src/config/mod.rs
// Purpose:   Configuration loading and hot-reload support
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

mod loader;
mod shared;
mod types;

pub use loader::load_config;
pub use shared::SharedConfig;
pub use types::*;

use crate::Result;

impl Config {
    /// Load configuration from file with cascade: CLI → ENV → .env → file → defaults
    ///
    /// # Errors
    /// Returns error if config file cannot be read or parsed
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        load_config(config_path)
    }
}
