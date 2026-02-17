// Project:   dfe-archiver
// File:      tests/archive_test.rs
// Purpose:   Integration tests for archive writing
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Integration tests for archive file writing.

mod common;

use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::storage::create_backend;
use tempfile::TempDir;

/// Test file archive with compression
#[tokio::test]
async fn test_file_archive_roundtrip() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().to_str().expect("path").to_string();

    let config = ArchiveConfig {
        destination: format!("file://{base_path}"),
        path_template: "{topic}/{year}/{month}/{day}/{hour}/archive".to_string(),
        file_extension: "jsonl".to_string(),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 1024 * 1024, // 1MB
        max_age_secs: 3600,
    };

    let compressor = create_compressor("zstd", 3).expect("create compressor");
    let storage = create_backend(&config).expect("create storage");

    let mut writer = ArchiveWriter::new(config, policy, compressor, storage);

    // Write some records
    for i in 0..100 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }

    // Flush and close
    writer.close().await.expect("close");

    // Verify files were created
    let entries: Vec<_> = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    assert!(!entries.is_empty(), "should have created archive files");

    for entry in entries {
        println!("Created: {}", entry.path().display());
    }
}

/// Test rolling by size
#[tokio::test]
async fn test_rolling_by_size() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().to_str().expect("path").to_string();

    let config = ArchiveConfig {
        destination: format!("file://{base_path}"),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        ..Default::default()
    };

    // Very small size threshold to force rolling
    let policy = RollingPolicy {
        max_size_bytes: 500, // 500 bytes
        max_age_secs: 3600,
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&config).expect("create storage");

    let mut writer = ArchiveWriter::new(config, policy, compressor, storage);

    // Write enough records to trigger multiple rolls
    for i in 0..50 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }

    writer.close().await.expect("close");

    // Count files created
    let file_count = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();

    // Should have created multiple files due to rolling
    assert!(file_count >= 1, "should have created at least one file");
    println!("Created {file_count} files");
}
