// Project:   dfe-archiver
// File:      crates/archiver/src/main.rs
// Purpose:   CLI entry point for the archiver service
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Allocator selection (compile-time feature)
// jemalloc takes priority if both features are enabled (e.g., --all-features)
#[cfg(all(feature = "jemalloc", not(feature = "mimalloc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", feature = "mimalloc"))]
#[global_allocator]
static GLOBAL_JEMALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;
use dfe_archiver::{Archiver, Result, config::load_config, metrics::start_metrics_server};
use std::sync::Arc;
use tracing::info;

/// DFE Archiver - High-volume Kafka-to-storage archiver
#[derive(Parser, Debug)]
#[command(name = "dfe-archiver")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, env = "ARCHIVER_CONFIG")]
    config: Option<String>,

    /// Kafka broker addresses (comma-separated)
    #[arg(long, env = "KAFKA_BROKERS")]
    kafka_brokers: Option<String>,

    /// Kafka consumer group ID
    #[arg(long, env = "KAFKA_GROUP_ID")]
    kafka_group_id: Option<String>,

    /// Kafka topics to consume (comma-separated)
    #[arg(long, env = "KAFKA_TOPICS")]
    kafka_topics: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Metrics server bind address
    #[arg(long, env = "METRICS_ADDRESS", default_value = "0.0.0.0:9090")]
    metrics_address: String,

    /// Output destination (file://, s3://, gs://, az://, minio://)
    #[arg(long, env = "ARCHIVER_DESTINATION")]
    destination: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialise logging via hyperi-rustlib (auto-detects JSON/Text format)
    hyperi_rustlib::logger::setup_default()
        .map_err(|e| dfe_archiver::Error::Config(format!("logger init failed: {e}")))?;

    info!(version = dfe_archiver::VERSION, "Starting dfe-archiver");

    // Load configuration (CLI → ENV → .env → file → defaults)
    let config = load_config(args.config.as_deref())?;

    info!(
        kafka_brokers = %config.kafka.brokers.join(","),
        kafka_topics = %config.kafka.topics.join(","),
        destination = %config.archive.destination,
        "Configuration loaded"
    );

    // Start metrics server
    let _metrics_manager = start_metrics_server(&config.metrics).await?;

    // Create and start archiver
    let archiver = Arc::new(Archiver::new(config).await?);
    let archiver_run = Arc::clone(&archiver);

    // Verify Kafka connection
    archiver.check_connection().await?;

    info!("dfe-archiver ready");

    // Spawn main loop
    let run_handle = tokio::spawn(async move {
        if let Err(e) = archiver_run.run().await {
            tracing::error!(error = %e, "Archiver run failed");
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| dfe_archiver::Error::Runtime(format!("signal handler failed: {e}")))?;

    info!("Shutdown signal received");

    // Signal shutdown and wait for drain
    archiver.shutdown();

    // Give the run loop time to exit
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Drain remaining buffers
    archiver.drain().await?;

    // Wait for run handle to complete
    let _ = run_handle.await;

    info!("Shutdown complete");
    Ok(())
}
