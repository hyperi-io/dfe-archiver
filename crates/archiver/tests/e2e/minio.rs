// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/minio.rs
// Purpose:   E2E tests for MinIO storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Requires running MinIO. Run with: cargo nextest run --test e2e -- --ignored

#[allow(unused_imports)]
use crate::common;

use dfe_archiver::config::{ArchiveConfig, MinioConfig};
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;
use std::env;

/// Check if `MinIO` is available (async version)
async fn minio_available() -> bool {
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .ok();

    if let Some(client) = client {
        let health_url = format!("{endpoint}/minio/health/live");
        if let Ok(resp) = client.get(&health_url).send().await {
            return resp.status().is_success();
        }
    }

    false
}

/// Get `MinIO` configuration from environment or defaults
fn get_minio_config() -> MinioConfig {
    MinioConfig {
        endpoint: env::var("MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9000".to_string()),
        access_key: env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
        secret_key: env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
        bucket: env::var("MINIO_BUCKET").unwrap_or_else(|_| "archive-test".to_string()),
        use_ssl: false,
    }
}

/// Test `MinIO` backend basic operations
#[tokio::test]
#[ignore = "requires running MinIO - run with --ignored"]
async fn test_minio_basic_operations() {
    if !minio_available().await {
        eprintln!("Skipping: MinIO not available at localhost:9000");
        eprintln!("Start with: docker compose -f docker-compose.dev.yaml up -d minio");
        return;
    }

    let config = get_minio_config();
    let backend =
        ObjectStoreBackend::new_minio(&config, "test-prefix".to_string(), 8 * 1024 * 1024)
            .expect("create MinIO backend");

    let test_path = format!("test-{}.txt", std::process::id());

    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend
        .append(&test_path, b"MinIO!")
        .await
        .expect("append 2");
    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("MinIO basic operations test passed");
}

/// Test `MinIO` backend with archive writer
#[tokio::test]
#[ignore = "requires running MinIO - run with --ignored"]
async fn test_minio_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    if !minio_available().await {
        eprintln!("Skipping: MinIO not available");
        return;
    }

    let minio_config = get_minio_config();

    let archive_config = ArchiveConfig {
        destination: format!("minio://{}/test-archives", minio_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        minio: Some(minio_config.clone()),
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

    println!("MinIO archive roundtrip test passed");
}

/// Test `MinIO` backend large file (rolling test)
#[tokio::test]
#[ignore = "requires running MinIO - run with --ignored"]
async fn test_minio_large_file_upload() {
    if !minio_available().await {
        eprintln!("Skipping: MinIO not available");
        return;
    }

    let config = get_minio_config();
    let backend = ObjectStoreBackend::new_minio(&config, "large-test".to_string(), 8 * 1024 * 1024)
        .expect("create MinIO backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    let chunk = vec![b'X'; 1024 * 1024];
    for i in 0..5 {
        backend
            .append(&test_path, &chunk)
            .await
            .unwrap_or_else(|_| panic!("append chunk {i}"));
    }

    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");

    println!("MinIO large file test passed (5MB uploaded)");
}

/// Test `MinIO` rolling by size
#[tokio::test]
#[ignore = "requires running MinIO - run with --ignored"]
async fn test_minio_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    if !minio_available().await {
        eprintln!("Skipping: MinIO not available");
        return;
    }

    let minio_config = get_minio_config();
    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("minio://{}/{test_prefix}", minio_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        minio: Some(minio_config.clone()),
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
        ObjectStoreBackend::new_minio(&minio_config, test_prefix.clone(), 8 * 1024 * 1024)
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
        "MinIO rolling by size created {} files (500 byte threshold)",
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

/// Test `create_backend` with minio:// URL
#[tokio::test]
#[ignore = "requires running MinIO - run with --ignored"]
async fn test_create_backend_minio_url() {
    if !minio_available().await {
        eprintln!("Skipping: MinIO not available");
        return;
    }

    let minio_config = get_minio_config();

    let archive_config = ArchiveConfig {
        destination: format!("minio://{}/prefix", minio_config.bucket),
        minio: Some(minio_config),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "minio");

    println!("create_backend with minio:// URL test passed");
}
