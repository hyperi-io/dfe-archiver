// Project:   dfe-archiver
// File:      tests/common/mod.rs
// Purpose:   Shared test utilities and infrastructure
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

use std::env;
use std::net::TcpStream;
use std::time::Duration;

pub mod containers;

/// Check if external Kafka is available (from .env)
pub fn kafka_available() -> bool {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_default();
    if brokers.is_empty() {
        return false;
    }

    // Try to connect to first broker
    let first_broker = brokers.split(',').next().unwrap_or("");
    if first_broker.is_empty() {
        return false;
    }

    // TCP connect check with timeout
    TcpStream::connect_timeout(
        &first_broker.parse().unwrap_or_else(|_| "127.0.0.1:9092".parse().expect("valid addr")),
        Duration::from_secs(2),
    )
    .is_ok()
}

/// Get Kafka brokers from environment
pub fn get_kafka_brokers() -> String {
    env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Get Kafka SASL configuration from environment
pub fn get_kafka_sasl() -> Option<KafkaSasl> {
    let mechanism = env::var("KAFKA_SASL_MECHANISM").ok()?;
    let username = env::var("KAFKA_SASL_USER").ok()?;
    let password = env::var("KAFKA_SASL_PASSWORD").ok()?;
    let protocol = env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());

    Some(KafkaSasl {
        mechanism,
        username,
        password,
        protocol,
    })
}

/// Kafka SASL configuration
#[derive(Debug, Clone)]
pub struct KafkaSasl {
    pub mechanism: String,
    pub username: String,
    pub password: String,
    pub protocol: String,
}

/// Skip test if Kafka not available
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        if !$crate::common::kafka_available() {
            eprintln!("Skipping test: Kafka not available (set KAFKA_BROKERS in .env)");
            return;
        }
    };
}

/// Generate unique topic name for tests
pub fn test_topic_name(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_millis();
    format!("{prefix}-test-{ts}")
}

/// Generate test JSON message
pub fn test_json_message(id: u64, org_id: &str, event_type: &str) -> Vec<u8> {
    serde_json::json!({
        "id": id,
        "org_id": org_id,
        "event_type": event_type,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "data": {
            "field1": "value1",
            "field2": 123,
            "nested": {
                "key": "value"
            }
        }
    })
    .to_string()
    .into_bytes()
}
