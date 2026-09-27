// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/kafka.rs
// Purpose:   E2E tests against real Kafka (remote or docker-local)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::common::{self, kafka_test_config};
use crate::skip_if_no_kafka;
use dfe_archiver::config::KafkaConfig;
use dfe_archiver::io::TransportAdapter;
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use scalo::metrics::{MetricsConfig, MetricsManager};
use scalo::transport::DeliveryStatus;
use scalo::{AckHeldSource, SelfRegulationConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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

    // Release what we got, which commits it
    if !batch.messages.is_empty() {
        let tokens: Vec<_> = batch.messages.iter().map(|m| m.token().clone()).collect();
        transport
            .release(&tokens, scalo::transport::DeliveryStatus::Delivered)
            .await
            .expect("release");
    }

    transport.close().await.expect("close");
}

fn records(ids: std::ops::Range<u64>) -> Vec<Vec<u8>> {
    ids.map(|id| format!(r#"{{"id":{id}}}"#).into_bytes())
        .collect()
}

/// Receive one batch, release it delivered, and return how many records it
/// held. Every receive polls the consumer, which serves its statistics.
async fn receive(transport: &TransportAdapter) -> usize {
    let batch = transport.recv(100).await.expect("recv");
    let tokens: Vec<_> = batch.messages.iter().map(|m| m.token().clone()).collect();
    transport
        .release(&tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    batch.messages.len()
}

/// Receive until `condition` holds for the transport and the records read so
/// far, failing after 45 s. Returns the records read.
async fn receive_until(
    transport: &TransportAdapter,
    what: &str,
    mut condition: impl FnMut(&TransportAdapter, usize) -> bool,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut read = 0;
    loop {
        read += receive(transport).await;
        if condition(transport, read) {
            return read;
        }
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
    }
}

/// The gauge `archiver_{name}` whose labels include every one of `labels`, as
/// the offline manager renders it.
fn gauge(manager: &MetricsManager, name: &str, labels: &[&str]) -> Option<f64> {
    let metric = format!("archiver_{name}");
    manager.render().lines().find_map(|line| {
        let (series, value) = line.rsplit_once(' ')?;
        let (series_name, series_labels) = series.split_once('{').unwrap_or((series, ""));
        if series_name == metric && labels.iter().all(|label| series_labels.contains(label)) {
            value.parse().ok()
        } else {
            None
        }
    })
}

/// The consuming transport measures its own backlog: records written while
/// intake is held are its position lag, the value the `kafka_lag` scaling term
/// reads, and its statistics publish the `rdkafka_*` gauges. No other consumer
/// is involved in either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_consuming_transport_reports_its_own_backlog() {
    let Some(kafka) = common::acquire_kafka("own_backlog").await else {
        return;
    };
    kafka.create_topic("events", &[]).await;
    kafka.produce("events", &records(0..10)).await;
    let manager = MetricsManager::with_config(MetricsConfig::offline("archiver"));

    // A hard source at its ceiling holds intake, as memory pressure does.
    let brake = Arc::new(AtomicU64::new(0));
    let governor = SelfRegulationConfig::default()
        .build(Arc::new(MemoryGuard::with_usage_source(
            MemoryGuardConfig::default(),
            UsageSource::Reservations,
        )))
        .expect("self-regulation is on by default");
    governor
        .pressure()
        .attach_source(Arc::new(AckHeldSource::new(Arc::clone(&brake), 1)));

    let config = KafkaConfig {
        brokers: vec![kafka.bootstrap.clone()],
        group_id: "own-backlog".to_string(),
        topics: vec!["events".to_string()],
        ..KafkaConfig::default()
    };
    let transport = TransportAdapter::new(&config, Some(&governor))
        .await
        .expect("transport");

    receive_until(&transport, "the first 10 records are read", |_, read| {
        read >= 10
    })
    .await;

    // The next receive pauses the assignment before anything more is written.
    brake.store(1, Ordering::Release);
    assert_eq!(receive(&transport).await, 0);
    kafka.produce("events", &records(10..40)).await;
    let read = receive_until(&transport, "the 30 unread records are the lag", |t, _| {
        t.position_lag() == 30
            && gauge(
                &manager,
                "rdkafka_topic_partition_consumer_lag",
                &[r#"topic="events""#, r#"partition="0""#],
            ) == Some(30.0)
    })
    .await;
    assert_eq!(read, 0, "a record was read while intake was held");
    assert!(
        gauge(&manager, "rdkafka_broker_rtt_avg_seconds", &[]).is_some(),
        "no broker RTT gauge:\n{}",
        manager.render()
    );
    assert!(
        gauge(&manager, "rdkafka_consumer_rebalance_count", &[]).is_some_and(|n| n >= 1.0),
        "no rebalance count:\n{}",
        manager.render()
    );

    brake.store(0, Ordering::Release);
    receive_until(
        &transport,
        "the backlog is read and the lag clears",
        |t, read| read >= 30 && t.position_lag() == 0,
    )
    .await;

    transport.close().await.expect("close");
}
