// Project:   dfe-archiver
// File:      crates/archiver/src/main.rs
// Purpose:   CLI entry point using hyperi-rustlib DfeApp pattern
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

use clap::{Parser, Subcommand};
use dfe_archiver::contract::deployment_contract;
use dfe_archiver::{Archiver, config::load_config, metrics::start_metrics_server};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo, run_app};
use hyperi_rustlib::deployment::{generate_chart, generate_dockerfile};
use std::sync::Arc;
use tracing::info;

/// DFE Archiver - High-volume Kafka-to-storage archiver
#[derive(Parser, Debug)]
#[command(name = "dfe-archiver", version)]
struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    #[command(flatten)]
    Standard(StandardCommand),

    /// Generate Dockerfile from deployment contract
    EmitDockerfile {
        /// Output path (- for stdout)
        #[arg(default_value = "-")]
        output: String,
    },

    /// Generate Helm chart from deployment contract
    EmitHelm {
        /// Output directory
        #[arg(default_value = "chart")]
        output: String,
    },

    /// Print deployment contract as JSON
    EmitContract,
}

impl DfeApp for App {
    type Config = dfe_archiver_core::config::Config;

    fn name(&self) -> &'static str {
        "dfe-archiver"
    }

    fn env_prefix(&self) -> &'static str {
        "ARCHIVER"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-archiver", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        match &self.command {
            Some(Command::Standard(cmd)) => Some(cmd),
            _ => None,
        }
    }

    fn load_config(&self, path: Option<&str>) -> Result<Self::Config, CliError> {
        load_config(path).map_err(|e| CliError::Config(e.to_string()))
    }

    async fn run_service(&self, config: Self::Config) -> Result<(), CliError> {
        info!(
            kafka_brokers = %config.kafka.brokers.join(","),
            kafka_topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            "Configuration loaded"
        );

        // Start metrics server
        let _metrics_manager = start_metrics_server(&config.metrics)
            .await
            .map_err(|e| CliError::Service(format!("metrics server failed: {e}")))?;

        // Create and start archiver
        let archiver = Arc::new(
            Archiver::new(config)
                .await
                .map_err(|e| CliError::Service(e.to_string()))?,
        );
        let archiver_run = Arc::clone(&archiver);

        // Verify Kafka connection
        archiver
            .check_connection()
            .await
            .map_err(|e| CliError::Service(e.to_string()))?;

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
            .map_err(|e| CliError::Service(format!("signal handler failed: {e}")))?;

        info!("Shutdown signal received");

        // Signal shutdown and wait for drain
        archiver.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        archiver
            .drain()
            .await
            .map_err(|e| CliError::Service(e.to_string()))?;

        let _ = run_handle.await;
        info!("Shutdown complete");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let app = App::parse();

    // Handle deployment contract commands before run_app
    // (these don't need logger/config init)
    match &app.command {
        Some(Command::EmitDockerfile { output }) => {
            let contract = deployment_contract();
            let content = generate_dockerfile(&contract);
            if output == "-" {
                print!("{content}");
            } else {
                std::fs::write(output, &content).unwrap_or_else(|e| {
                    eprintln!("error: failed to write {output}: {e}");
                    std::process::exit(1);
                });
                eprintln!("Dockerfile written to {output}");
            }
            return;
        }
        Some(Command::EmitHelm { output }) => {
            let contract = deployment_contract();
            generate_chart(&contract, output).unwrap_or_else(|e| {
                eprintln!("error: failed to generate chart: {e}");
                std::process::exit(1);
            });
            eprintln!("Helm chart generated at {output}/");
            return;
        }
        Some(Command::EmitContract) => {
            let contract = deployment_contract();
            println!("{}", contract.to_json());
            return;
        }
        _ => {}
    }

    // Standard DfeApp lifecycle (run, version, config-check)
    if let Err(e) = run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
