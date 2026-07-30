// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/azure.rs
// Purpose:   E2E tests for Azure Blob storage backend
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

// Runs against Azurite, Microsoft's own Azure Blob emulator, so the Azure write
// path is exercised with no Azure subscription and no manual setup -- just
// Docker. `common::acquire_azure` prefers a live account when
// AZURE_STORAGE_ACCOUNT is set, so pointing these at real Azure is still a
// matter of exporting credentials.
//
// These are deliberately NOT #[ignore]d. Ignoring them is what kept the Azure
// backend at zero automated coverage for as long as it has had any.

#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;

/// Test Azure backend basic operations (create, append, close, exists, delete)
#[tokio::test]
async fn test_azure_basic_operations() {
    let Some(azure) = common::acquire_azure("test_azure_basic_operations", "archive-test").await
    else {
        return; // reason printed by acquire_azure, and a hard failure in CI
    };

    let backend =
        ObjectStoreBackend::new_azure(&azure.config, "test-basic".to_string(), 8 * 1024 * 1024)
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
}

/// Test Azure multipart upload with large file.
///
/// Azure's multipart is Put Block plus Put Block List, a different API from
/// S3's, and Azurite implements both -- so a chunked upload that only assembles
/// correctly at close is genuinely covered here.
#[tokio::test]
async fn test_azure_multipart_large_file() {
    let Some(azure) =
        common::acquire_azure("test_azure_multipart_large_file", "archive-test").await
    else {
        return;
    };

    let backend =
        ObjectStoreBackend::new_azure(&azure.config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create Azure backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    // 20MB in 1MB writes, against a 5MB chunk size -- four blocks, so the block
    // list has to be assembled and committed rather than a single put.
    let chunk = vec![b'X'; 1024 * 1024];
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    // `exists` is true of a zero-byte blob, so ask Azurite how much actually
    // arrived -- an empty committed block list would pass the check above.
    if azure.manages_container() {
        let len = common::azurite_blob_len(&azure.config, "test-multipart", &test_path)
            .await
            .expect("HEAD the uploaded blob");
        assert_eq!(
            len,
            20 * 1024 * 1024,
            "all 20MB should have been committed by the block list"
        );
    }

    backend.delete(&test_path).await.expect("delete");
}

/// Test Azure with full archive writer and compression
#[tokio::test]
async fn test_azure_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let Some(azure) = common::acquire_azure("test_azure_archive_roundtrip", "archive-test").await
    else {
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/test-archive", azure.config.container),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        azure: Some(azure.config.clone()),
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

    // The writer reports success on a failed upload if nothing reads the object
    // back, so confirm an archive actually landed in the container.
    let verify =
        ObjectStoreBackend::new_azure(&azure.config, "test-archive".to_string(), 8 * 1024 * 1024)
            .expect("create verify backend");
    let objects = verify
        .list_prefix("data/", None)
        .await
        .expect("list objects");
    assert!(
        !objects.is_empty(),
        "the writer closed but nothing is in the container"
    );
}

/// Test Azure rolling by size
#[tokio::test]
async fn test_azure_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let Some(azure) = common::acquire_azure("test_azure_rolling_by_size", "archive-test").await
    else {
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/{test_prefix}", azure.config.container),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        azure: Some(azure.config.clone()),
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
        ObjectStoreBackend::new_azure(&azure.config, test_prefix.clone(), 8 * 1024 * 1024)
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

/// Test `create_backend` with az:// URL
#[tokio::test]
async fn test_create_backend_azure_url() {
    let Some(azure) = common::acquire_azure("test_create_backend_azure_url", "archive-test").await
    else {
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("az://{}/prefix", azure.config.container),
        azure: Some(azure.config.clone()),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "azure");

    // `name()` alone would pass on a backend that cannot reach anything, so put
    // a byte through the thing create_backend handed back.
    let test_path = format!("url-{}.txt", std::process::id());
    backend.create(&test_path).await.expect("create");
    backend
        .append(&test_path, b"routed via az:// URL")
        .await
        .expect("append");
    backend.close(&test_path).await.expect("close");
    assert!(backend.exists(&test_path).await.expect("exists check"));
    backend.delete(&test_path).await.expect("delete");
}
