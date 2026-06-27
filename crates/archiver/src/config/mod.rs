// Project:   dfe-archiver
// File:      crates/archiver/src/config/mod.rs
// Purpose:   Configuration loading and hot-reload support
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

mod loader;

pub use dfe_archiver_core::config::*;
pub use loader::{load_config, validate_config};
pub use scalo::config::reloader::{ConfigReloader, ReloaderConfig};
pub use scalo::config::shared::SharedConfig;
