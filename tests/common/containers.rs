// Project:   dfe-archiver
// File:      tests/common/containers.rs
// Purpose:   Testcontainers setup for Docker-based integration tests
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Docker container setup for integration tests.
//!
//! Enable with `--features testcontainers`.
//! These are used when external Kafka (from .env) is not available.

#[cfg(feature = "testcontainers")]
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};

#[cfg(feature = "testcontainers")]
use testcontainers_modules::kafka::Kafka;

/// Test infrastructure with optional containers
pub struct TestInfrastructure {
    #[cfg(feature = "testcontainers")]
    pub kafka: Option<ContainerAsync<Kafka>>,

    /// Kafka brokers address (from container or external)
    pub kafka_brokers: String,
}

impl TestInfrastructure {
    /// Create test infrastructure using external Kafka from .env
    pub async fn from_env() -> Self {
        let kafka_brokers = std::env::var("KAFKA_BROKERS")
            .unwrap_or_else(|_| "localhost:9092".to_string());

        Self {
            #[cfg(feature = "testcontainers")]
            kafka: None,
            kafka_brokers,
        }
    }

    /// Create test infrastructure with Docker containers
    #[cfg(feature = "testcontainers")]
    pub async fn with_containers() -> Self {
        use tracing::info;

        info!("Starting Kafka container...");

        let kafka_container = Kafka::default()
            .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .start()
            .await
            .expect("Failed to start Kafka container");

        let kafka_port = kafka_container
            .get_host_port_ipv4(9093)
            .await
            .expect("Failed to get Kafka port");

        let kafka_brokers = format!("127.0.0.1:{kafka_port}");

        info!(brokers = %kafka_brokers, "Kafka container started");

        Self {
            kafka: Some(kafka_container),
            kafka_brokers,
        }
    }

    /// Get Kafka brokers address
    #[must_use]
    pub fn kafka_brokers(&self) -> &str {
        &self.kafka_brokers
    }
}

#[cfg(feature = "testcontainers")]
impl Drop for TestInfrastructure {
    fn drop(&mut self) {
        // Containers are automatically stopped when dropped
    }
}
