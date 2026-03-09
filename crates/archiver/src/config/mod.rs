// Project:   dfe-archiver
// File:      crates/archiver/src/config/mod.rs
// Purpose:   Configuration loading and hot-reload support
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

mod loader;
mod shared;

pub use dfe_archiver_core::config::*;
pub use loader::load_config;
pub use shared::SharedConfig;
