// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/s3.rs
// Purpose:   E2E tests for AWS S3 storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Requires AWS credentials. Run with: cargo nextest run --test e2e -- --ignored

#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::{ArchiveConfig, S3Config};
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;
use std::env;

/// Get S3 configuration from environment
fn get_s3_config() -> Option<S3Config> {
    let bucket = env::var("S3_BUCKET").ok()?;
    Some(S3Config {
        bucket,
        region: env::var("S3_REGION")
            .ok()
            .or(Some("ap-southeast-2".to_string())),
        access_key_id: env::var("S3_ACCESS_KEY_ID").ok(),
        secret_access_key: env::var("S3_SECRET_ACCESS_KEY").ok(),
        endpoint: env::var("S3_ENDPOINT").ok(),
    })
}

/// Test S3 backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "requires AWS credentials - run with --ignored"]
async fn test_s3_basic_operations() {
    let config = if let Some(c) = get_s3_config() {
        c
    } else {
        eprintln!("Skipping: S3_BUCKET not set");
        return;
    };

    let backend = ObjectStoreBackend::new_s3(&config, "test-basic".to_string(), 8 * 1024 * 1024)
        .expect("create S3 backend");

    let test_path = format!("test-{}.txt", std::process::id());

    // Create
    backend.create(&test_path).await.expect("create");

    // Append data
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend.append(&test_path, b"S3!").await.expect("append 2");

    // Close (completes multipart upload)
    backend.close(&test_path).await.expect("close");

    // Verify exists
    assert!(backend.exists(&test_path).await.expect("exists check"));

    // Cleanup
    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("S3 basic operations test passed");
}

/// Test S3 multipart upload with large file
#[tokio::test]
#[ignore = "requires AWS credentials - run with --ignored"]
async fn test_s3_multipart_large_file() {
    let config = if let Some(c) = get_s3_config() {
        c
    } else {
        eprintln!("Skipping: S3_BUCKET not set");
        return;
    };

    // Use 5MB chunk size (S3 minimum) to force multiple parts
    let backend =
        ObjectStoreBackend::new_s3(&config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create S3 backend");

    let test_path = format!("large-{}.bin", std::process::id());

    // Create
    backend.create(&test_path).await.expect("create");

    // Write 20MB in 1MB chunks — this forces 4 multipart parts at 5MB chunk size
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

    println!("S3 multipart large file test passed (20MB uploaded)");
}

/// Test S3 with full archive writer and compression
#[tokio::test]
#[ignore = "requires AWS credentials - run with --ignored"]
async fn test_s3_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let s3_config = if let Some(c) = get_s3_config() {
        c
    } else {
        eprintln!("Skipping: S3_BUCKET not set");
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/test-archive", s3_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        s3: Some(s3_config),
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

    println!("S3 archive roundtrip test passed");
}

/// Test S3 rolling by size — verifies `ArchiveWriter` creates multiple objects
/// when the compressed file size exceeds the rolling threshold.
#[tokio::test]
#[ignore = "requires AWS credentials - run with --ignored"]
async fn test_s3_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let s3_config = if let Some(c) = get_s3_config() {
        c
    } else {
        eprintln!("Skipping: S3_BUCKET not set");
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/{test_prefix}", s3_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        s3: Some(s3_config.clone()),
        ..Default::default()
    };

    // Small roll size to force multiple files
    let policy = RollingPolicy {
        max_size_bytes: 500,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");

    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    // Write records in batches with flush to trigger rolling
    for batch in 0..10 {
        for i in 0..5 {
            let id = batch * 5 + i;
            let record = common::test_json_message(id, "test-org", "test-event");
            writer.write_record(&record).await.expect("write record");
        }
        writer.flush().await.expect("flush");
    }

    writer.close().await.expect("close");

    // Verify multiple objects were created
    let verify_backend =
        ObjectStoreBackend::new_s3(&s3_config, test_prefix.clone(), 8 * 1024 * 1024)
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
        "S3 rolling by size created {} files (500 byte threshold)",
        objects.len()
    );

    // Cleanup
    for obj in &objects {
        let rel_path = obj.strip_prefix(&format!("{test_prefix}/")).unwrap_or(obj);
        verify_backend
            .delete(rel_path)
            .await
            .expect("delete object");
    }
}

/// Test `create_backend` with s3:// URL
#[tokio::test]
#[ignore = "requires AWS credentials - run with --ignored"]
async fn test_create_backend_s3_url() {
    let s3_config = if let Some(c) = get_s3_config() {
        c
    } else {
        eprintln!("Skipping: S3_BUCKET not set");
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/prefix", s3_config.bucket),
        s3: Some(s3_config),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "s3");

    println!("create_backend with s3:// URL test passed");
}
