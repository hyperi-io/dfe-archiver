// Project:   dfe-archiver
// File:      crates/archiver/tests/kafka_test.rs
// Purpose:   Integration tests against real Kafka
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Integration tests for Kafka transport.
//!
//! These tests require a running Kafka instance. Configure via .env:
//! - KAFKA_BROKERS=k8s.tyrell.com.au:30092
//! - KAFKA_SASL_MECHANISM=SCRAM-SHA-512
//! - KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
//! - KAFKA_SASL_USER=loader
//! - KAFKA_SASL_PASSWORD=TyrellPOC2024
//!
//! Run with: cargo test --test kafka_test -- --ignored

mod common;

use common::{get_kafka_brokers, get_kafka_sasl, kafka_available};
use dfe_archiver::config::KafkaConfig;
use dfe_archiver::io::TransportAdapter;

/// Test basic Kafka connection
#[tokio::test]
#[ignore = "requires external Kafka - run with --ignored"]
async fn test_kafka_connection() {
    if !kafka_available() {
        eprintln!("Skipping: Kafka not available");
        return;
    }

    let brokers = get_kafka_brokers();
    let sasl = get_kafka_sasl();

    let mut config = KafkaConfig {
        brokers: brokers.split(',').map(|s| s.trim().to_string()).collect(),
        group_id: "dfe-archiver-test".to_string(),
        topics: vec!["test-topic".to_string()],
        ..Default::default()
    };

    if let Some(sasl) = sasl {
        config.sasl_mechanism = Some(sasl.mechanism);
        config.sasl_username = Some(sasl.username);
        config.sasl_password = Some(sasl.password);
        config.security_protocol = sasl.protocol;
    }

    let transport = TransportAdapter::new(&config)
        .await
        .expect("create transport");

    assert!(transport.is_healthy());

    transport.close().await.expect("close");
}

/// Test message consumption (requires messages in topic)
#[tokio::test]
#[ignore = "requires external Kafka with data - run with --ignored"]
async fn test_kafka_consume() {
    if !kafka_available() {
        eprintln!("Skipping: Kafka not available");
        return;
    }

    let brokers = get_kafka_brokers();
    let sasl = get_kafka_sasl();

    let mut config = KafkaConfig {
        brokers: brokers.split(',').map(|s| s.trim().to_string()).collect(),
        group_id: format!("dfe-archiver-test-{}", std::process::id()),
        topics: vec!["test-topic".to_string()],
        batch_size: 100,
        ..Default::default()
    };

    if let Some(sasl) = sasl {
        config.sasl_mechanism = Some(sasl.mechanism);
        config.sasl_username = Some(sasl.username);
        config.sasl_password = Some(sasl.password);
        config.security_protocol = sasl.protocol;
    }

    let transport = TransportAdapter::new(&config)
        .await
        .expect("create transport");

    // Try to receive messages (may be empty if topic is empty)
    let messages = transport.recv(100).await.expect("recv");
    println!("Received {} messages", messages.len());

    // Commit if we got any
    if !messages.is_empty() {
        let offsets: Vec<_> = messages.iter().map(|m| m.into()).collect();
        transport.commit(&offsets).await.expect("commit");
    }

    transport.close().await.expect("close");
}
