// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/azure.rs
// Purpose:   E2E tests for Azure Blob storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Requires Azure credentials. Run with: cargo nextest run --test e2e -- --ignored
#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::{ArchiveConfig, AzureConfig};
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;
use std::env;

/// Get Azure configuration from environment
fn get_azure_config() -> Option<AzureConfig> {
    let account_name = env::var("AZURE_STORAGE_ACCOUNT").ok()?;
    Some(AzureConfig {
        account_name,
        account_key: env::var("AZURE_STORAGE_KEY").ok(),
        sas_token: None,
        container: env::var("AZURE_CONTAINER").unwrap_or_else(|_| "archive-test".to_string()),
        use_emulator: false,
        endpoint: None,
    })
}

/// Test Azure backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_basic_operations() {
    let config = if let Some(c) = get_azure_config() {
        c
    } else {
        eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
        return;
    };

    let backend = ObjectStoreBackend::new_azure(&config, "test-basic".to_string(), 8 * 1024 * 1024)
        .expect("create Azure backend");

    let test_path = format!("test-{}.txt", std::process::id());

    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend
        .append(&test_path, b"Azure!")
        .await
        .expect("append 2");
    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("Azure basic operations test passed");
}

/// Test Azure multipart upload with large file
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_multipart_large_file() {
    let config = if let Some(c) = get_azure_config() {
        c
    } else {
        eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
        return;
    };

    let backend =
        ObjectStoreBackend::new_azure(&config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create Azure backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    let chunk = vec![b'X'; 1024 * 1024];
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");

    println!("Azure multipart large file test passed (20MB uploaded)");
}

/// Test Azure with full archive writer and compression
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let azure_config = if let Some(c) = get_azure_config() {
        c
    } else {
        eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/test-archive", azure_config.container),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        azure: Some(azure_config),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 10 * 1024 * 1024,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("zstd", 3).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");

    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    for i in 0..100 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }

    writer.close().await.expect("close");

    println!("Azure archive roundtrip test passed");
}

/// Test Azure rolling by size
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_azure_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let azure_config = if let Some(c) = get_azure_config() {
        c
    } else {
        eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/{test_prefix}", azure_config.container),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        azure: Some(azure_config.clone()),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 500,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");

    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    for batch in 0..10 {
        for i in 0..5 {
            let id = batch * 5 + i;
            let record = common::test_json_message(id, "test-org", "test-event");
            writer.write_record(&record).await.expect("write record");
        }
        writer.flush().await.expect("flush");
    }

    writer.close().await.expect("close");

    let verify_backend =
        ObjectStoreBackend::new_azure(&azure_config, test_prefix.clone(), 8 * 1024 * 1024)
            .expect("create verify backend");

    let objects = verify_backend
        .list_prefix("data/")
        .await
        .expect("list objects");

    assert!(
        objects.len() >= 3,
        "expected at least 3 rolled files, got {}",
        objects.len()
    );

    println!(
        "Azure rolling by size created {} files (500 byte threshold)",
        objects.len()
    );

    for obj in &objects {
        let rel_path = obj.strip_prefix(&format!("{test_prefix}/")).unwrap_or(obj);
        verify_backend
            .delete(rel_path)
            .await
            .expect("delete object");
    }
}

/// Test `create_backend` with az:// URL
#[tokio::test]
#[ignore = "requires Azure credentials - run with --ignored"]
async fn test_create_backend_azure_url() {
    let azure_config = if let Some(c) = get_azure_config() {
        c
    } else {
        eprintln!("Skipping: AZURE_STORAGE_ACCOUNT not set");
        return;
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
