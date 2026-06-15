// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/kafka.rs
// Purpose:   E2E tests against real Kafka (remote or docker-local)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::common::kafka_test_config;
use crate::skip_if_no_kafka;
use dfe_archiver::config::KafkaConfig;
use dfe_archiver::io::TransportAdapter;

/// Build a `KafkaConfig` from the test config helper
fn build_kafka_config(topics: Vec<String>, group_suffix: &str) -> KafkaConfig {
    let kf = kafka_test_config();
    KafkaConfig {
        brokers: kf
            .brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect(),
        group_id: format!("dfe-archiver-test-{group_suffix}"),
        topics,
        sasl_mechanism: kf.sasl_mechanism,
        sasl_username: kf.sasl_user,
        sasl_password: kf
            .sasl_password
            .map(dfe_archiver::config::sensitive::SensitiveString::from),
        security_protocol: kf.security_protocol,
        ..Default::default()
    }
}

/// Test basic Kafka connection
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_kafka_connection() {
    skip_if_no_kafka!();

    let config = build_kafka_config(vec!["test-topic".to_string()], "conn");

    let transport = TransportAdapter::new(&config, None)
        .await
        .expect("create transport");

    assert!(transport.is_healthy());

    transport.close().await.expect("close");
}

/// Test message consumption (requires messages in topic)
#[tokio::test]
#[ignore = "requires Kafka with data - run with --ignored"]
async fn test_kafka_consume() {
    skip_if_no_kafka!();

    let config = build_kafka_config(
        vec!["test-topic".to_string()],
        &format!("{}", std::process::id()),
    );

    let transport = TransportAdapter::new(&config, None)
        .await
        .expect("create transport");

    // Try to receive a batch (may be empty if topic is empty)
    let batch = transport.recv(100).await.expect("recv");
    println!("Received {} messages", batch.messages.len());

    // Commit if we got any
    if !batch.messages.is_empty() {
        let offsets: Vec<_> = batch
            .messages
            .iter()
            .map(std::convert::Into::into)
            .collect();
        transport.commit(offsets).await.expect("commit");
    }

    transport.close().await.expect("close");
}
