// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/gcs.rs
// Purpose:   E2E tests for Google Cloud Storage backend
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Requires GCS credentials. Run with: cargo nextest run --test e2e -- --ignored

#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::{ArchiveConfig, GcsConfig};
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;
use std::env;

/// Get GCS configuration from environment.
/// Prefers `GCS_SERVICE_ACCOUNT_KEY` (inline JSON) over `GOOGLE_APPLICATION_CREDENTIALS` (file path).
///
/// Loads `.env` from the project root first so tests use host-configured credentials.
fn get_gcs_config() -> Option<GcsConfig> {
    common::load_dotenv();
    let bucket = env::var("GCS_BUCKET").ok()?;
    Some(GcsConfig {
        bucket,
        project_id: None,
        service_account_key: env::var("GCS_SERVICE_ACCOUNT_KEY")
            .ok()
            .map(dfe_archiver::config::sensitive::SensitiveString::from),
        credentials_path: env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
    })
}

/// Test GCS backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_basic_operations() {
    let config = if let Some(c) = get_gcs_config() {
        c
    } else {
        eprintln!("Skipping: GCS_BUCKET not set");
        return;
    };

    let backend = ObjectStoreBackend::new_gcs(&config, "test-basic".to_string(), 8 * 1024 * 1024)
        .expect("create GCS backend");

    let test_path = format!("test-{}.txt", std::process::id());

    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend.append(&test_path, b"GCS!").await.expect("append 2");
    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));

    println!("GCS basic operations test passed");
}

/// Test GCS multipart upload with large file
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_multipart_large_file() {
    let config = if let Some(c) = get_gcs_config() {
        c
    } else {
        eprintln!("Skipping: GCS_BUCKET not set");
        return;
    };

    let backend =
        ObjectStoreBackend::new_gcs(&config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create GCS backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    let chunk = vec![b'X'; 1024 * 1024];
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");

    println!("GCS multipart large file test passed (20MB uploaded)");
}

/// Test GCS with full archive writer and compression
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let gcs_config = if let Some(c) = get_gcs_config() {
        c
    } else {
        eprintln!("Skipping: GCS_BUCKET not set");
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/test-archive", gcs_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        gcs: Some(gcs_config),
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

    println!("GCS archive roundtrip test passed");
}

/// Test GCS rolling by size
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_gcs_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let gcs_config = if let Some(c) = get_gcs_config() {
        c
    } else {
        eprintln!("Skipping: GCS_BUCKET not set");
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/{test_prefix}", gcs_config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        gcs: Some(gcs_config.clone()),
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
        ObjectStoreBackend::new_gcs(&gcs_config, test_prefix.clone(), 8 * 1024 * 1024)
            .expect("create verify backend");

    let objects = verify_backend
        .list_prefix("data/", None)
        .await
        .expect("list objects");

    assert!(
        objects.len() >= 3,
        "expected at least 3 rolled files, got {}",
        objects.len()
    );

    println!(
        "GCS rolling by size created {} files (500 byte threshold)",
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

/// Test `create_backend` with gs:// URL
#[tokio::test]
#[ignore = "requires GCS credentials - run with --ignored"]
async fn test_create_backend_gcs_url() {
    let gcs_config = if let Some(c) = get_gcs_config() {
        c
    } else {
        eprintln!("Skipping: GCS_BUCKET not set");
        return;
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
