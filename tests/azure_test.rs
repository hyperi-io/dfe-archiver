// Project:   dfe-archiver
// File:      tests/azure_test.rs
// Purpose:   Integration tests for Azure Blob storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Integration tests for Azure Blob storage backend.
//!
//! These tests require Azure credentials via env vars or .env file.
//!
//! Setup:
//! ```bash
//! az login
//! az storage account create --name dfearchivertest --resource-group hyperstack \
//!   --location australiaeast --sku Standard_LRS
//! az storage container create --name archive-test --account-name dfearchivertest --auth-mode login
//! ```
//!
//! Run with:
//! ```bash
//! cargo test --test azure_test -- --ignored
//! ```

mod common;

use dfe_archiver::config::{ArchiveConfig, AzureConfig};
use dfe_archiver::storage::{create_backend, ObjectStoreBackend, StorageBackend};
use std::env;

/// Get Azure configuration from environment
fn get_azure_config() -> Option<AzureConfig> {
    let account_name = env::var("AZURE_STORAGE_ACCOUNT").ok()?;
    Some(AzureConfig {
        account_name,
        account_key: env::var("AZURE_STORAGE_KEY").ok(),
        sas_token: None,
        container: env::var("AZURE_CONTAINER")
            .unwrap_or_else(|_| "archive-test".to_string()),
        use_emulator: false,
        endpoint: None,
    })
}

/// Test Azure backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_basic_operations() {
    let config = match get_azure_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
            return;
        }
    };

    let backend =
        ObjectStoreBackend::new_azure(&config, "test-basic".to_string(), 8 * 1024 * 1024)
            .expect("create Azure backend");

    let test_path = format!("test-{}.txt", std::process::id());

    // Create
    backend.create(&test_path).await.expect("create");

    // Append data
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend
        .append(&test_path, b"Azure!")
        .await
        .expect("append 2");

    // Close (completes multipart upload)
    backend.close(&test_path).await.expect("close");

    // Verify exists
    assert!(backend.exists(&test_path).await.expect("exists check"));

    // Cleanup
    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("Azure basic operations test passed");
}

/// Test Azure multipart upload with large file
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_multipart_large_file() {
    let config = match get_azure_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
            return;
        }
    };

    // Use 5MB chunk size to force multiple parts
    let backend =
        ObjectStoreBackend::new_azure(&config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create Azure backend");

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

    println!("Azure multipart large file test passed (20MB uploaded)");
}

/// Test Azure with full archive writer and compression
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let azure_config = match get_azure_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
            return;
        }
    };

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/test-archive", azure_config.container),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        azure: Some(azure_config),
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

    println!("Azure archive roundtrip test passed");
}

/// Test create_backend with az:// URL
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_create_backend_azure_url() {
    let azure_config = match get_azure_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
            return;
        }
    };

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/prefix", azure_config.container),
        azure: Some(azure_config),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "azure");

    println!("create_backend with az:// URL test passed");
}
