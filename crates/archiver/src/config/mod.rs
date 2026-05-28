// Project:   dfe-archiver
// File:      crates/archiver/src/config/mod.rs
// Purpose:   Configuration loading and hot-reload support
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

mod loader;

pub use dfe_archiver_core::config::*;
pub use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
pub use hyperi_rustlib::config::shared::SharedConfig;
pub use loader::{load_config, validate_config};
