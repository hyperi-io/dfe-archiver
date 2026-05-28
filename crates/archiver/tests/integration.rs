// Project:   dfe-archiver
// File:      crates/archiver/tests/integration.rs
// Purpose:   Single-binary integration test harness (3x compile-time win)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Integration tests consolidated into a single binary for compile-time efficiency.
//!
//! Each `tests/*.rs` file compiles as a separate binary (separate link cycle).
//! One entry point with submodules = 1 link cycle = ~3x faster test compilation.
//!
//! Run with: `cargo nextest run --test integration`

#![allow(clippy::expect_used)]

#[path = "common/mod.rs"]
mod common;

#[path = "integration/archive.rs"]
mod archive;
#[path = "integration/config.rs"]
mod config;
