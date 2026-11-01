// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/kafka_tls.rs
// Purpose:   The Kafka consumer against a real TLS broker with a private CA
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

//! A deployment that trusts a private Kafka CA through `SSL_CERT_FILE`, with
//! no `kafka.ssl_ca_location`, reads from a TLS broker that CA signed.

use dfe_archiver::config::KafkaConfig;
use dfe_archiver::io::TransportAdapter;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::deployment::test_support::shared_service::Suite;
use scalo::transport::DeliveryStatus;
use scalo::transport::kafka::test_support::{TlsBroker, require_docker};
use std::time::{Duration, Instant};

const SUITE: Suite = Suite {
    repo: "dfe-archiver",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

const TOPIC: &str = "tls-events";

/// Produce `count` records to [`TOPIC`] over the broker's TLS listener.
async fn produce(broker: &TlsBroker, count: u64) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker.tls)
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", broker.pki.file("ca.crt"))
        .create()
        .expect("producer");
    for id in 0..count {
        let payload = format!(r#"{{"id":{id}}}"#);
        producer
            .send(
                FutureRecord::<(), _>::to(TOPIC).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("produce over TLS: {e}"));
    }
}

/// The records a consumer in `group` reads from [`TOPIC`] within `limit`,
/// stopping at `want`, built with `SSL_CERT_FILE` naming `ca_file`.
async fn consume(
    broker: &TlsBroker,
    group: &str,
    ca_file: &str,
    want: usize,
    limit: Duration,
) -> usize {
    let config = KafkaConfig {
        brokers: vec![broker.tls.clone()],
        group_id: group.to_string(),
        topics: vec![TOPIC.to_string()],
        security_protocol: "SSL".to_string(),
        ssl_ca_location: None,
        ..KafkaConfig::default()
    };
    // The client takes its CA from the variable when it is built, so it is set around construction only.
    let transport = tokio::task::block_in_place(|| {
        temp_env::with_var("SSL_CERT_FILE", Some(ca_file), || {
            tokio::runtime::Handle::current().block_on(TransportAdapter::new(&config, None))
        })
    })
    .expect("transport");

    let deadline = Instant::now() + limit;
    let mut read = 0;
    while read < want && Instant::now() < deadline {
        let Ok(batch) = transport.recv(100).await else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let tokens: Vec<_> = batch.messages.iter().map(|m| m.token().clone()).collect();
        read += tokens.len();
        transport
            .release(&tokens, DeliveryStatus::Delivered)
            .await
            .expect("release");
    }
    transport.close().await.expect("close");
    read
}

/// The CA named by `SSL_CERT_FILE` verifies the broker, and a CA that did
/// not sign it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_trusts_the_broker_ca_named_by_ssl_cert_file() {
    if !require_docker() {
        return;
    }
    let broker = TlsBroker::start(&SUITE, "kafka_tls_ssl_cert_file")
        .await
        .unwrap_or_else(|e| panic!("TLS broker: {e}"));
    produce(&broker, 20).await;

    let rogue = broker.pki.file("rogue-ca.crt");
    let read = consume(&broker, "tls-untrusted", &rogue, 1, Duration::from_secs(10)).await;
    assert_eq!(
        read, 0,
        "a CA that did not sign the broker's certificate verified it"
    );

    let ca = broker.pki.file("ca.crt");
    let read = consume(&broker, "tls-trusted", &ca, 20, Duration::from_secs(60)).await;
    assert_eq!(read, 20, "the consumer did not read every record over TLS");
}
