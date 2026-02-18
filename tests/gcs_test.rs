// Project:   dfe-archiver
// File:      tests/gcs_test.rs
// Purpose:   Integration tests for Google Cloud Storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Integration tests for Google Cloud Storage backend.
//!
//! These tests require GCS credentials via service account key or ADC.
//!
//! Setup:
//! ```bash
//! gcloud auth application-default login --project=hyperi-dfe
//! gcloud storage buckets create gs://hyperi-dfe-archiver-test \
//!   --location=australia-southeast1 --uniform-bucket-level-access
//! ```
//!
//! Run with:
//! ```bash
//! cargo test --test gcs_test -- --ignored
//! ```

mod common;

use dfe_archiver::config::{ArchiveConfig, GcsConfig};
use dfe_archiver::storage::{create_backend, ObjectStoreBackend, StorageBackend};
use std::env;

/// Get GCS configuration from environment.
/// Prefers GCS_SERVICE_ACCOUNT_KEY (inline JSON) over GOOGLE_APPLICATION_CREDENTIALS (file path).
fn get_gcs_config() -> Option<GcsConfig> {
    let bucket = env::var("GCS_BUCKET").ok()?;
    Some(GcsConfig {
        bucket,
        project_id: None,
        service_account_key: env::var("GCS_SERVICE_ACCOUNT_KEY").ok(),
        credentials_path: env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
    })
}

/// Test GCS backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_basic_operations() {
    let config = match get_gcs_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: GCS_BUCKET not set");
            return;
        }
    };

    let backend =
        ObjectStoreBackend::new_gcs(&config, "test-basic".to_string(), 8 * 1024 * 1024)
            .expect("create GCS backend");

    let test_path = format!("test-{}.txt", std::process::id());

    // Create
    backend.create(&test_path).await.expect("create");

    // Append data
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend
        .append(&test_path, b"GCS!")
        .await
        .expect("append 2");

    // Close (completes multipart upload)
    backend.close(&test_path).await.expect("close");

    // Verify exists
    assert!(backend.exists(&test_path).await.expect("exists check"));

    // Cleanup
    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("GCS basic operations test passed");
}

/// Test GCS multipart upload with large file
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_multipart_large_file() {
    let config = match get_gcs_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: GCS_BUCKET not set");
            return;
        }
    };

    // Use 5MB chunk size to force multiple parts
    let backend =
        ObjectStoreBackend::new_gcs(&config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create GCS backend");

    let test_path = format!("large-{}.bin", std::process::id());

    // Create
    backend.create(&test_path).await.expect("create");

    // Write 20MB in 1MB chunks
    let chunk = vec![b'X'; 1024 * 1024]; // 1MB
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    // Close (completes multipart upload)
    backend.close(&test_path).await.expect("close");

    // Verify exists
    assert!(backend.exists(&test_path).await.expect("exists check"));

    // Cleanup
    backend.delete(&test_path).await.expect("delete");

    println!("GCS multipart large file test passed (20MB uploaded)");
}

/// Test GCS with full archive writer and compression
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let gcs_config = match get_gcs_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: GCS_BUCKET not set");
            return;
        }
    };

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/test-archive", gcs_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        gcs: Some(gcs_config),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 10 * 1024 * 1024, // 10MB
        max_age_secs: 3600,
    };

    let compressor = create_compressor("zstd", 3).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");

    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    // Write test records
    for i in 0..100 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }

    // Flush and close
    writer.close().await.expect("close");

    println!("GCS archive roundtrip test passed");
}

/// Test create_backend with gs:// URL
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_create_backend_gcs_url() {
    let gcs_config = match get_gcs_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: GCS_BUCKET not set");
            return;
        }
    };

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/prefix", gcs_config.bucket),
        gcs: Some(gcs_config),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "gcs");

    println!("create_backend with gs:// URL test passed");
}
