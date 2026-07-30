// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/gcs.rs
// Purpose:   E2E tests for Google Cloud Storage backend
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

// GCS is the backend that CANNOT have its write path emulated, and this is the
// evidence rather than an #[ignore] that reads as routine.
//
// `ObjectStoreBackend::create` calls `object_store`'s `put_multipart`, which on
// GCS is implemented ONLY as the XML multipart API -- `POST /<bucket>/<object>
// ?uploads=` then `PUT ?uploadId=` (object_store 0.14 gcp/client.rs, no
// resumable-upload alternative and no switch). fsouza/fake-gcs-server 1.55.1,
// the maintained GCS emulator, does not implement that API: probed directly, it
// returns 404 with `{"error":{"code":404,"reason":"notFound"}}` for an initiate
// against a bucket it has just created.
//
// So the write tests below stay #[ignore]d and say why. What IS emulable is
// tested for real and NOT ignored: `new_gcs`'s credential and base-URL wiring,
// and the exists / list_prefix / delete paths, against objects the fixture seeds
// over the emulator's JSON API. That is the majority of the GCS surface and it
// is the part a misconfiguration breaks first.
//
// What would close the gap, in rough order of cost:
//   - fake-gcs-server growing XML multipart support (upstream issue territory);
//   - object_store gaining a resumable-upload path for GCS put_multipart;
//   - Google's own gcr.io/cloud-devrel-public-resources/storage-testbench, if it
//     turns out to implement the XML API -- not verified here;
//   - a real GCS bucket in CI, which is the thing this whole plan exists to
//     avoid needing.

#[allow(unused_imports)]
use crate::common;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::io::{ObjectStoreBackend, create_backend};
use dfe_archiver::storage::StorageBackend;

/// The GCS read, list and delete paths, against a real emulator.
///
/// Objects are seeded by the fixture rather than written by the archiver,
/// because the archiver's write path is the one the emulator cannot serve. What
/// this does cover is everything between `GcsConfig` and the wire: the inline
/// service-account key, the emulator base URL taken from it, prefix handling,
/// and the three operations that are not multipart.
#[tokio::test]
async fn test_gcs_read_list_and_delete() {
    let Some(gcs) = common::acquire_gcs("test_gcs_read_list_and_delete", "archive-test").await
    else {
        return; // reason printed by acquire_gcs, and a hard failure in CI
    };
    if !gcs.manages_container() {
        eprintln!("skipping: a live GCS bucket is configured, so seeding it is not ours to do");
        return;
    }

    let prefix = format!("test-read-{}", std::process::id());
    let backend = ObjectStoreBackend::new_gcs(&gcs.config, prefix.clone(), 8 * 1024 * 1024)
        .expect("create GCS backend");

    // Seed three objects under the prefix and one outside it, so list_prefix has
    // something to exclude as well as something to find.
    for name in ["data/one.jsonl", "data/two.jsonl", "data/three.jsonl"] {
        common::fake_gcs_put_object(&gcs.config, &format!("{prefix}/{name}"), b"{\"a\":1}\n")
            .await
            .expect("seed object");
    }
    common::fake_gcs_put_object(&gcs.config, "somewhere-else/four.jsonl", b"{\"a\":1}\n")
        .await
        .expect("seed object outside the prefix");

    assert!(
        backend
            .exists("data/one.jsonl")
            .await
            .expect("exists check"),
        "the backend cannot see an object that is in the bucket"
    );
    assert!(
        !backend
            .exists("data/missing.jsonl")
            .await
            .expect("exists check"),
        "the backend claims a missing object exists"
    );

    let objects = backend
        .list_prefix("data/", None)
        .await
        .expect("list objects");
    assert_eq!(
        objects.len(),
        3,
        "expected the 3 objects under {prefix}/data/, got {objects:?}"
    );
    assert!(
        objects.iter().all(|o| o.starts_with(&prefix)),
        "list_prefix leaked objects from outside the prefix: {objects:?}"
    );

    let capped = backend
        .list_prefix("data/", Some(2))
        .await
        .expect("list capped");
    assert_eq!(capped.len(), 2, "limit=2 should cap the listing");

    backend.delete("data/one.jsonl").await.expect("delete");
    assert!(
        !backend
            .exists("data/one.jsonl")
            .await
            .expect("exists check"),
        "delete reported success but the object is still there"
    );

    for name in ["data/two.jsonl", "data/three.jsonl"] {
        backend.delete(name).await.expect("delete");
    }
}

/// `create_backend` routes gs:// to a GCS backend that can reach the emulator.
///
/// The write path is not exercised (see the file header), so this seeds an
/// object and reads it back through the backend `create_backend` handed over --
/// `name()` alone would pass on a backend wired to nothing.
#[tokio::test]
async fn test_create_backend_gcs_url() {
    let Some(gcs) = common::acquire_gcs("test_create_backend_gcs_url", "archive-test").await else {
        return;
    };

    let prefix = format!("test-url-{}", std::process::id());
    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/{prefix}", gcs.config.bucket),
        gcs: Some(gcs.config.clone()),
        ..Default::default()
    };

    let backend = create_backend(&archive_config).expect("create backend");
    assert_eq!(backend.name(), "gcs");

    if !gcs.manages_container() {
        return;
    }
    common::fake_gcs_put_object(&gcs.config, &format!("{prefix}/probe.txt"), b"routed")
        .await
        .expect("seed object");
    assert!(
        backend.exists("probe.txt").await.expect("exists check"),
        "the gs:// backend cannot read an object at its own prefix"
    );
    backend.delete("probe.txt").await.expect("delete");
}

/// Test GCS backend basic operations (create, append, close, exists, delete)
#[tokio::test]
#[ignore = "write path needs GCS credentials: object_store uses the XML multipart API and fake-gcs-server does not implement it"]
async fn test_gcs_basic_operations() {
    let Some(gcs) = common::acquire_gcs("test_gcs_basic_operations", "archive-test").await else {
        return;
    };

    let backend =
        ObjectStoreBackend::new_gcs(&gcs.config, "test-basic".to_string(), 8 * 1024 * 1024)
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
}

/// Test GCS multipart upload with large file
#[tokio::test]
#[ignore = "write path needs GCS credentials: object_store uses the XML multipart API and fake-gcs-server does not implement it"]
async fn test_gcs_multipart_large_file() {
    let Some(gcs) = common::acquire_gcs("test_gcs_multipart_large_file", "archive-test").await
    else {
        return;
    };

    let backend =
        ObjectStoreBackend::new_gcs(&gcs.config, "test-multipart".to_string(), 5 * 1024 * 1024)
            .expect("create GCS backend");

    let test_path = format!("large-{}.bin", std::process::id());

    backend.create(&test_path).await.expect("create");

    let chunk = vec![b'X'; 1024 * 1024];
    for _ in 0..20 {
        backend.append(&test_path, &chunk).await.expect("append");
    }

    backend.close(&test_path).await.expect("close");

    assert!(backend.exists(&test_path).await.expect("exists check"));

    // `exists` is true of a zero-byte object, so ask the server how much
    // actually arrived.
    if gcs.manages_container() {
        let len = common::fake_gcs_object_len(&gcs.config, "test-multipart", &test_path)
            .await
            .expect("read the uploaded object's metadata");
        assert_eq!(len, 20 * 1024 * 1024, "all 20MB should have been committed");
    }

    backend.delete(&test_path).await.expect("delete");
}

/// Test GCS with full archive writer and compression
#[tokio::test]
#[ignore = "write path needs GCS credentials: object_store uses the XML multipart API and fake-gcs-server does not implement it"]
async fn test_gcs_archive_roundtrip() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let Some(gcs) = common::acquire_gcs("test_gcs_archive_roundtrip", "archive-test").await else {
        return;
    };

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/test-archive", gcs.config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        gcs: Some(gcs.config.clone()),
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

    // A clean close proves nothing if nothing reads the object back.
    let verify =
        ObjectStoreBackend::new_gcs(&gcs.config, "test-archive".to_string(), 8 * 1024 * 1024)
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

/// Test GCS rolling by size
#[tokio::test]
#[ignore = "write path needs GCS credentials: object_store uses the XML multipart API and fake-gcs-server does not implement it"]
async fn test_gcs_rolling_by_size() {
    use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver::compression::create_compressor;

    let Some(gcs) = common::acquire_gcs("test_gcs_rolling_by_size", "archive-test").await else {
        return;
    };

    let test_prefix = format!("test-rolling-{}", std::process::id());

    let archive_config = ArchiveConfig {
        destination: format!("gs://{}/{test_prefix}", gcs.config.bucket),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        gcs: Some(gcs.config.clone()),
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
        ObjectStoreBackend::new_gcs(&gcs.config, test_prefix.clone(), 8 * 1024 * 1024)
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
