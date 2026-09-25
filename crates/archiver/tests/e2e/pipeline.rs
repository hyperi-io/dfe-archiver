// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/pipeline.rs
// Purpose:   E2E: produce to Kafka, archive to file, verify output
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::common::{self, kafka_test_config, test_json_message, test_topic_name};
use crate::skip_if_no_kafka;
use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::{ArchiveConfig, KafkaConfig};
use dfe_archiver::io::{TransportAdapter, create_backend};
use std::time::Duration;
use tempfile::TempDir;

/// Build a `KafkaConfig` for a specific topic
fn build_kafka_config(topic: &str) -> KafkaConfig {
    let kf = kafka_test_config();
    KafkaConfig {
        brokers: kf
            .brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect(),
        group_id: format!("dfe-archiver-e2e-{}", std::process::id()),
        topics: vec![topic.to_string()],
        sasl_mechanism: kf.sasl_mechanism,
        sasl_username: kf.sasl_user,
        sasl_password: kf
            .sasl_password
            .map(dfe_archiver::config::sensitive::SensitiveString::from),
        security_protocol: kf.security_protocol,
        batch_size: 100,
        ..Default::default()
    }
}

/// Build an rdkafka `ClientConfig` with auth for the current test mode
fn rdkafka_client_config(kf: &common::KafkaTestConfig) -> rdkafka::config::ClientConfig {
    let mut config = rdkafka::config::ClientConfig::new();
    config.set("bootstrap.servers", &kf.brokers);
    config.set("security.protocol", &kf.security_protocol);

    if let Some(ref mechanism) = kf.sasl_mechanism {
        config.set("sasl.mechanism", mechanism);
        if let Some(ref user) = kf.sasl_user {
            config.set("sasl.username", user);
        }
        if let Some(ref password) = kf.sasl_password {
            config.set("sasl.password", password);
        }
    }

    config
}

/// Get the e2e test topic name.
///
/// Docker mode: creates a unique topic per test run (auto-create enabled).
/// Remote mode: uses static `dfe-archiver-e2e` topic (must be pre-created with ACLs).
fn e2e_topic(suffix: &str) -> String {
    match common::TestMode::detect() {
        common::TestMode::Docker => test_topic_name(&format!("e2e-{suffix}")),
        common::TestMode::Remote => format!("dfe-archiver-e2e-{suffix}"),
    }
}

/// Create a topic via the admin client (idempotent — ignores "already exists").
///
/// Works in both modes: docker (auto-create) and remote (requires Create ACL on prefix).
/// The `dfe-archiver` Kafka user has `Create` permission on `dfe-archiver-*` topics.
async fn ensure_topic(kf: &common::KafkaTestConfig, topic: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let replication = match common::TestMode::detect() {
        common::TestMode::Docker => 1,
        common::TestMode::Remote => 3,
    };

    let admin: AdminClient<DefaultClientContext> = rdkafka_client_config(kf)
        .create()
        .expect("create admin client");

    let new_topic = NewTopic::new(topic, 3, TopicReplication::Fixed(replication));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let results = admin
        .create_topics(&[new_topic], &opts)
        .await
        .expect("create topic");
    for result in results {
        match result {
            Ok(_) | Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("failed to create topic {name}: {code}"),
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Produce messages to a Kafka topic using rdkafka directly
async fn produce_messages(topic: &str, count: u64, kf: &common::KafkaTestConfig) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let mut config = rdkafka_client_config(kf);
    config.set("message.timeout.ms", "30000");

    let producer: FutureProducer = config.create().expect("create producer");

    for i in 0..count {
        let payload = test_json_message(i, "e2e-org", "e2e-event");
        let key = format!("key-{i}");
        let record = FutureRecord::to(topic).payload(&payload).key(key.as_str());

        producer
            .send(record, Duration::from_secs(30))
            .await
            .expect("send message");
    }
}

/// E2E: produce messages to Kafka, consume via transport, write to file archive
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_e2e_kafka_to_file_archive() {
    skip_if_no_kafka!();

    let topic = e2e_topic("archive");
    let kf = kafka_test_config();
    let message_count = 50u64;

    ensure_topic(&kf, &topic).await;
    produce_messages(&topic, message_count, &kf).await;

    // Set up file archive destination
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().to_str().expect("path").to_string();

    let archive_config = ArchiveConfig {
        destination: format!("file://{base_path}"),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 1024 * 1024,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("none", 0).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");
    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    // Consume from Kafka
    let config = build_kafka_config(&topic);
    let transport = TransportAdapter::new(&config, None)
        .await
        .expect("create transport");

    let mut total_received = 0u64;
    let mut attempts = 0;

    while total_received < message_count && attempts < 30 {
        let messages = transport.recv(100).await.expect("recv").messages;

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            attempts += 1;
            continue;
        }

        for msg in &messages {
            writer
                .write_record(&msg.payload)
                .await
                .expect("write record");
        }
        total_received += messages.len() as u64;

        // Release the offsets, which commits them
        let tokens: Vec<_> = messages.iter().map(|m| m.token().clone()).collect();
        transport
            .release(&tokens, scalo::transport::DeliveryStatus::Delivered)
            .await
            .expect("release");

        attempts = 0; // reset on successful recv
    }

    writer.close().await.expect("close writer");
    transport.close().await.expect("close transport");

    // Verify: check that archive files were created and contain data
    let files: Vec<_> = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .collect();

    assert!(
        !files.is_empty(),
        "expected archive files to be created, got none"
    );

    let total_bytes: u64 = files
        .iter()
        .map(|f| f.metadata().expect("metadata").len())
        .sum();

    assert!(total_bytes > 0, "expected archive files to contain data");

    assert_eq!(
        total_received, message_count,
        "expected to consume all {message_count} messages, got {total_received}"
    );

    println!(
        "E2E passed: produced {message_count} messages to '{topic}', \
         consumed {total_received}, archived to {} files ({total_bytes} bytes)",
        files.len()
    );
}

/// E2E: produce, consume, archive with zstd compression, verify compressed output
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_e2e_kafka_to_compressed_archive() {
    skip_if_no_kafka!();

    let topic = e2e_topic("zstd");
    let kf = kafka_test_config();
    let message_count = 100u64;

    ensure_topic(&kf, &topic).await;
    produce_messages(&topic, message_count, &kf).await;

    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().to_str().expect("path").to_string();

    let archive_config = ArchiveConfig {
        destination: format!("file://{base_path}"),
        path_template: "data/{timestamp}".to_string(),
        file_extension: "jsonl".to_string(),
        ..Default::default()
    };

    let policy = RollingPolicy {
        max_size_bytes: 1024 * 1024,
        max_age_secs: 3600,
    };

    let compressor = create_compressor("zstd", 3).expect("create compressor");
    let storage = create_backend(&archive_config).expect("create storage");
    let mut writer = ArchiveWriter::new(archive_config, policy, compressor, storage);

    let config = build_kafka_config(&topic);
    let transport = TransportAdapter::new(&config, None)
        .await
        .expect("create transport");

    let mut total_received = 0u64;
    let mut attempts = 0;

    while total_received < message_count && attempts < 30 {
        let messages = transport.recv(200).await.expect("recv").messages;

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            attempts += 1;
            continue;
        }

        for msg in &messages {
            writer
                .write_record(&msg.payload)
                .await
                .expect("write record");
        }
        total_received += messages.len() as u64;

        let tokens: Vec<_> = messages.iter().map(|m| m.token().clone()).collect();
        transport
            .release(&tokens, scalo::transport::DeliveryStatus::Delivered)
            .await
            .expect("release");
        attempts = 0;
    }

    writer.close().await.expect("close writer");
    transport.close().await.expect("close transport");

    let files: Vec<_> = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .collect();

    assert!(!files.is_empty(), "expected archive files");

    // With zstd compression, output should be smaller than raw JSON
    let total_bytes: u64 = files
        .iter()
        .map(|f| f.metadata().expect("metadata").len())
        .sum();

    let raw_size_estimate = message_count * 150; // ~150 bytes per JSON message
    assert!(
        total_bytes < raw_size_estimate,
        "compressed output ({total_bytes}) should be smaller than raw estimate ({raw_size_estimate})"
    );

    println!(
        "E2E zstd passed: {message_count} messages, {total_received} consumed, \
         {} files, {total_bytes} bytes (raw estimate: {raw_size_estimate})",
        files.len()
    );
}
