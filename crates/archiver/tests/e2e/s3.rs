// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/s3.rs
// Purpose:   E2E tests for AWS S3 storage backend
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

// Runs against LocalStack, so the S3 write path is exercised with no AWS
// account and no manual setup -- just Docker. `common::acquire_s3` prefers a
// live bucket when S3_BUCKET comes with credentials.
//
// LocalStack rather than the dev-stack MinIO: the rest of the fleet already
// standardises on it, and a per-test container cleans itself up where a shared
// compose service cannot. `minio.rs` still covers `new_minio`, which is its own
// code path.
//
// These are deliberately NOT #[ignore]d. Ignoring them is what kept the S3
// backend at zero automated coverage.

#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;

/// Test S3 backend basic operations (create, append, close, exists, delete)
#[tokio::test]
async fn test_s3_basic_operations() {
    let (_staging_dir, staging) = common::staging();
    let Some(s3) = common::acquire_s3("test_s3_basic_operations", "archive-test").await else {
        return; // reason printed by acquire_s3, and a hard failure in CI
    };

    let backend = ObjectStoreBackend::new_s3(
        &s3.config,
        "test-basic".to_string(),
        8 * 1024 * 1024,
        &staging,
    )
    .expect("create S3 backend");

    let test_path = format!("test-{}.txt", std::process::id());

    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"Hello, ")
        .await
        .expect("append 1");
    backend.append(&test_path, b"S3!").await.expect("append 2");
    common::finish(backend.close(&test_path).await.expect("close")).await;

    assert!(backend.exists(&test_path).await.expect("exists check"));

    backend.delete(&test_path).await.expect("delete");
    assert!(!backend.exists(&test_path).await.expect("not exists"));
}

/// Test S3 multipart upload with large file.
///
/// 20MB in 1MB writes against a 5MB chunk size -- four parts, so the upload has
/// to be initiated, the parts uploaded and the list completed. A single put
/// would not exercise any of that.
#[tokio::test]
async fn test_s3_multipart_large_file() {
    let (_staging_dir, staging) = common::staging();
    let Some(s3) = common::acquire_s3("test_s3_multipart_large_file", "archive-test").await else {
        return;
    };

    let backend = ObjectStoreBackend::new_s3(
        &s3.config,
        "test-multipart".to_string(),
        5 * 1024 * 1024,
        &staging,
    )
    .expect("create S3 backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    let chunk = vec![b'X'; 1024 * 1024];
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    common::finish(backend.close(&test_path).await.expect("close")).await;

    assert!(backend.exists(&test_path).await.expect("exists check"));

    // `exists` is true of a zero-byte object, so ask the server how much
    // actually arrived -- a completed upload with no parts would pass above.
    if s3.manages_container() {
        let len = common::localstack_object_len(&s3.config, "test-multipart", &test_path)
            .await
            .expect("HEAD the uploaded object");
        assert_eq!(
            len,
            20 * 1024 * 1024,
            "all 20MB should have been committed by the multipart complete"
        );
    }

    backend.delete(&test_path).await.expect("delete");
}

/// Test S3 with full archive writer and compression
#[tokio::test]
async fn test_s3_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;
    let (_staging_dir, staging) = common::staging();

    let Some(s3) = common::acquire_s3("test_s3_archive_roundtrip", "archive-test").await else {
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/test-archive", s3.config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        s3: Some(s3.config.clone()),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 10 * 1024 * 1024,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("zstd", 3).expect("create compressor");
    let storage = create_backend(&archive_config, &staging).expect("create storage");

    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    for i in 0..100 {
        let record = common::test_json_message(i, "test-org", "test-event");
        writer.write_record(&record).await.expect("write record");
    }

    writer.close().await.expect("close");
    common::upload_closed(&mut writer).await;

    // A clean close proves nothing if nothing reads the object back.
    let verify = ObjectStoreBackend::new_s3(
        &s3.config,
        "test-archive".to_string(),
        8 * 1024 * 1024,
        &staging,
    )
    .expect("create verify backend");
    let objects = verify
        .list_prefix("data/", None)
        .await
        .expect("list objects");
    assert!(
        !objects.is_empty(),
        "the writer closed but nothing is in the bucket"
    );
}

/// Test S3 rolling by size -- verifies `ArchiveWriter` creates multiple objects
/// when the compressed file size exceeds the rolling threshold.
#[tokio::test]
async fn test_s3_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;
    let (_staging_dir, staging) = common::staging();

    let Some(s3) = common::acquire_s3("test_s3_rolling_by_size", "archive-test").await else {
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/{test_prefix}", s3.config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        s3: Some(s3.config.clone()),
        ..Default::default()
    };

    // Small roll size to force multiple files
    let policy = RollingPolicy {
        max_size_bytes: 500,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&archive_config, &staging).expect("create storage");

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
    common::upload_closed(&mut writer).await;

    let verify_backend =
        ObjectStoreBackend::new_s3(&s3.config, test_prefix.clone(), 8 * 1024 * 1024, &staging)
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
async fn test_create_backend_s3_url() {
    let (_staging_dir, staging) = common::staging();
    let Some(s3) = common::acquire_s3("test_create_backend_s3_url", "archive-test").await else {
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("s3://{}/prefix", s3.config.bucket),
        s3: Some(s3.config.clone()),
        ..Default::default()
    };

    let backend = create_backend(&archive_config, &staging).expect("create backend");
    assert_eq!(backend.name(), "s3");

    // `name()` alone would pass on a backend that cannot reach anything, so put
    // a byte through the thing create_backend handed back.
    let test_path = format!("url-{}.txt", std::process::id());
    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"routed via s3:// URL")
        .await
        .expect("append");
    common::finish(backend.close(&test_path).await.expect("close")).await;
    assert!(backend.exists(&test_path).await.expect("exists check"));
    backend.delete(&test_path).await.expect("delete");
}
