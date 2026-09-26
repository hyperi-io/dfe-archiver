// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/traversal.rs
// Purpose:   Record field values never name a path outside the archive
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Expression routing names a directory after each routing field's value, and
//! those values arrive from dfe-receiver, which takes untrusted input. So a
//! value is attacker-controlled, and the path it names must stay one segment
//! under the archive destination whatever it holds.

use dfe_archiver::RoutingConfig;
use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::ArchiveConfig;
use dfe_archiver::io::{Staging, create_backend};
use dfe_archiver::routing::Router;
use dfe_archiver::types::KafkaMessage;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Field values that name something other than one directory when joined into
/// a path as they arrive.
const HOSTILE: [&str; 11] = [
    "..",
    ".",
    "",
    "../../escape",
    "/etc/passwd",
    "a/b",
    "a\\..\\b",
    "..\\..\\windows",
    "nul\u{0}byte",
    "line\nbreak",
    "=3D",
];

fn router() -> Router {
    Router::new(RoutingConfig {
        mode: "expression".to_string(),
        expression_fields: vec!["org_id".to_string()],
        default_segment: "unknown".to_string(),
    })
}

/// A record whose `org_id` is `value`, encoded as JSON so a NUL or a newline
/// is a real character of the value.
fn record(value: &str) -> KafkaMessage {
    let payload = serde_json::json!({ "org_id": value, "n": 1 }).to_string();
    KafkaMessage::for_test(payload.into_bytes(), "events", 0, 0)
}

fn destination(value: &str) -> String {
    router()
        .route(&record(value))
        .expect("route")
        .destination
        .to_string()
}

#[test]
fn every_hostile_value_is_one_plain_segment_under_its_topic() {
    for value in HOSTILE {
        let destination = destination(value);
        let segments: Vec<&str> = destination.split('/').collect();
        assert_eq!(
            segments.len(),
            2,
            "{value:?} routed to {destination:?}, not one segment under the topic"
        );
        assert_eq!(segments[0], "events");
        let segment = segments[1];
        assert!(
            !matches!(segment, "" | "." | ".."),
            "{value:?} routed to the segment {segment:?}"
        );
        assert!(
            segment
                .bytes()
                .all(|b| b.is_ascii_graphic() && !matches!(b, b'/' | b'\\')),
            "{value:?} routed to {segment:?}, which carries a separator, a control byte or a space"
        );
    }
}

#[test]
fn distinct_values_never_share_a_directory() {
    let values = HOSTILE.iter().copied().chain(["a=2Fb", "=", "=2E", "acme"]);
    let mut seen = BTreeSet::new();
    let mut count = 0;
    for value in values {
        count += 1;
        seen.insert(destination(value));
    }
    assert_eq!(seen.len(), count, "two values share a directory: {seen:?}");
}

#[test]
fn a_plain_value_keeps_its_name() {
    for value in ["acme", "cisco_ios.log", "tenant-01", "8.0.0", "A1_b-2.c"] {
        assert_eq!(destination(value), format!("events/{value}"));
    }
}

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

/// Written the way the archiver writes a destination's file, a record carrying
/// each hostile value lands under the archive base, one file per value.
#[tokio::test]
async fn a_hostile_value_writes_under_the_archive_base() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    // Two levels below the temporary directory, so a value that escapes the
    // base is caught and still cleaned up with the directory.
    let base = tmp.path().join("deep").join("er").join("archive");
    std::fs::create_dir_all(&base).expect("base");
    let staging = Staging::open(tmp.path().join("uploads")).expect("staging");

    let mut failed = Vec::new();
    for value in HOSTILE {
        let destination = destination(value);
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
        let written = match writer.write_record(br#"{"n":1}"#).await {
            Ok(()) => writer.close().await.map(|_| ()),
            Err(e) => Err(e),
        };
        if let Err(e) = written {
            failed.push(format!("{value:?}: {e}"));
        }
    }

    let canonical_base = base.canonicalize().expect("canonical base");
    let written: Vec<PathBuf> = files_under(tmp.path())
        .into_iter()
        .filter(|path| !path.starts_with(tmp.path().join("uploads")))
        .collect();
    let escaped: Vec<PathBuf> = written
        .iter()
        .map(|file| file.canonicalize().expect("canonical file"))
        .filter(|file| !file.starts_with(&canonical_base))
        .collect();
    assert!(
        escaped.is_empty(),
        "written outside the archive base {}: {escaped:?}",
        canonical_base.display()
    );
    assert!(
        failed.is_empty(),
        "values that could not be written: {failed:?}"
    );
    assert_eq!(
        written.len(),
        HOSTILE.len(),
        "one file per value, each in its own directory: {written:?}"
    );
}
