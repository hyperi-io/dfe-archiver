// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/org_path.rs
// Purpose:   The archive path keys on the stamped _org_id, never the payload org_id
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

//! A sender controls the payload `org_id`, so the receiver and fetcher stamp
//! `_org_id` and the archive path follows that. A record with no usable
//! `_org_id` belongs to the deployment default org, `default`.

use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::{ArchiveConfig, Config, TRANSPORT_GRPC};
use dfe_archiver::io::{Staging, create_backend};
use dfe_archiver::routing::Router;
use dfe_archiver::types::KafkaMessage;
use std::path::{Path, PathBuf};

/// Every file under `dir`, recursively.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// Routes `payload` with the default routing of `config`, writes it the way the
/// archiver writes a destination's file, and returns the path of the file,
/// relative to the archive base.
async fn archive(config: &Config, topic: &str, payload: &str) -> String {
    let router = Router::new(config.effective_routing());
    let message = KafkaMessage::for_test(payload.as_bytes().to_vec(), topic, 0, 0);
    let destination = router.route(&message).expect("route").destination;

    let tmp = tempfile::TempDir::new().expect("temp dir");
    let base = tmp.path().join("archive");
    std::fs::create_dir_all(&base).expect("base");
    let staging = Staging::open(tmp.path().join("uploads")).expect("staging");
    let archive = ArchiveConfig {
        destination: format!("file://{}", base.display()),
        path_template: format!("{destination}/{{year}}"),
        ..ArchiveConfig::default()
    };
    let mut writer = ArchiveWriter::new(
        archive.clone(),
        RollingPolicy::default(),
        create_compressor("none", 0).expect("compressor"),
        create_backend(&archive, &staging).expect("backend"),
    );
    writer
        .write_record(payload.as_bytes())
        .await
        .expect("write");
    writer.close().await.expect("close");

    let files = files_under(&base);
    assert_eq!(files.len(), 1, "one file expected: {files:?}");
    files[0]
        .strip_prefix(&base)
        .expect("under base")
        .to_string_lossy()
        .replace('\\', "/")
}

fn grpc() -> Config {
    Config {
        transport: TRANSPORT_GRPC.to_string(),
        ..Config::default()
    }
}

#[tokio::test]
async fn the_stamped_org_names_the_directory_not_the_payload_org() {
    let path = archive(
        &Config::default(),
        "events",
        r#"{"_org_id":"acme","org_id":"evil","n":1}"#,
    )
    .await;
    assert!(path.starts_with("events/acme/"), "archived at {path}");
}

#[tokio::test]
async fn a_record_with_no_stamped_org_goes_under_default_whatever_org_id_says() {
    let path = archive(&Config::default(), "events", r#"{"org_id":"evil","n":1}"#).await;
    assert!(path.starts_with("events/default/"), "archived at {path}");
}

#[tokio::test]
async fn a_blank_or_null_stamped_org_goes_under_default() {
    for payload in [
        r#"{"_org_id":"","n":1}"#,
        r#"{"_org_id":"   ","n":1}"#,
        r#"{"_org_id":null,"n":1}"#,
    ] {
        let path = archive(&Config::default(), "events", payload).await;
        assert!(
            path.starts_with("events/default/"),
            "{payload} archived at {path}"
        );
    }
}

#[tokio::test]
async fn direct_transport_folders_are_the_source_then_the_stamped_org() {
    let stamped = archive(
        &grpc(),
        "",
        r#"{"_source":"cisco_ios.log","_org_id":"acme","org_id":"evil"}"#,
    )
    .await;
    assert!(
        stamped.starts_with("cisco_ios.log/acme/"),
        "archived at {stamped}"
    );

    let unstamped = archive(
        &grpc(),
        "",
        r#"{"_source":"cisco_ios.log","org_id":"evil"}"#,
    )
    .await;
    assert!(
        unstamped.starts_with("cisco_ios.log/default/"),
        "archived at {unstamped}"
    );
}
