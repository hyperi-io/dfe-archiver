// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/archive.rs
// Purpose:   Integration tests for archive writing and rolling
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::common;
use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::io::create_backend;
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
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .collect();

    assert!(!entries.is_empty(), "should have created archive files");

    for entry in entries {
        println!("Created: {}", entry.path().display());
    }
}

/// Test rolling by size — verifies that the writer creates multiple files
/// when the compressed file size exceeds the rolling threshold.
///
/// Key detail: `should_roll()` checks `compressed_bytes` which only gets
/// updated during `flush()`. We must call `flush()` between write batches
/// so the rolling trigger can fire on the next `write()` call.
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

    // Very small size threshold to force rolling (500 bytes)
    let policy = RollingPolicy {
        max_size_bytes: 500,
        max_age_secs: 3600,
    };

    // No compression so sizes are predictable
    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&config).expect("create storage");

    let mut writer = ArchiveWriter::new(config, policy, compressor, storage);

    // Write records in batches with flush between each to update compressed_bytes.
    // Each test message is ~150 bytes, so 5 records = ~750 bytes > 500 byte threshold.
    for batch in 0..10 {
        for i in 0..5 {
            let id = batch * 5 + i;
            let record = common::test_json_message(id, "test-org", "test-event");
            writer.write_record(&record).await.expect("write record");
        }
        // Flush to update compressed_bytes so should_roll() can trigger
        writer.flush().await.expect("flush");
    }

    writer.close().await.expect("close");

    // Count files created
    let file_count = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .count();

    // With 50 records at ~150 bytes each (~7500 bytes total) and 500 byte roll size,
    // we should get multiple files (at least 5-10)
    assert!(
        file_count >= 3,
        "expected at least 3 files from rolling, got {file_count}"
    );
    println!("Rolling by size created {file_count} files (500 byte threshold, no compression)");
}

/// Test rolling by time — verifies that the writer creates a new file
/// when the max age is exceeded.
#[tokio::test]
async fn test_rolling_by_time() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().to_str().expect("path").to_string();

    let config = ArchiveConfig {
        destination: format!("file://{base_path}"),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        ..Default::default()
    };

    // Large size (won't trigger), very short age (1 second)
    let policy = RollingPolicy {
        max_size_bytes: 100 * 1024 * 1024, // 100MB - won't trigger
        max_age_secs: 1,                   // 1 second
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&config).expect("create storage");

    let mut writer = ArchiveWriter::new(config, policy, compressor, storage);

    // Write first batch
    for i in 0..5 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }
    writer.flush().await.expect("flush");

    // Wait for the age threshold to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Write second batch — should trigger time-based roll
    for i in 5..10 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }
    writer.flush().await.expect("flush");

    writer.close().await.expect("close");

    // Count files
    let file_count = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .count();

    assert!(
        file_count >= 2,
        "expected at least 2 files from time-based rolling, got {file_count}"
    );
    println!("Rolling by time created {file_count} files (1 second threshold)");
}
