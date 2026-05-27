// Project:   dfe-archiver
// File:      crates/archiver/src/main.rs
// Purpose:   CLI entry point using hyperi-rustlib DfeApp pattern
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

// Allocator selection: jemalloc (only allocator at all hyperi-ci channels per
// 2026-04-17 DFE policy). hyperi-ci enables `--features jemalloc` per channel;
// local `cargo build` uses the system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};
use dfe_archiver::config::{
    ConfigReloader, ReloaderConfig, SharedConfig, load_config, validate_config,
};
use dfe_archiver::contract::deployment_contract;
use dfe_archiver::{Archiver, metrics::init_metrics};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo, run_app};
use hyperi_rustlib::deployment::{generate_chart, generate_dockerfile};
use hyperi_rustlib::logger::security;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

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

    #[allow(clippy::too_many_lines)]
    async fn run_service(
        &self,
        config: Self::Config,
        mut runtime: hyperi_rustlib::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        info!(
            kafka_brokers = %config.kafka.brokers.join(","),
            kafka_group_id = %config.kafka.group_id,
            kafka_topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            storage_backend = %config.archive.backend_name(),
            compression_codec = %config.compression.codec,
            compression_level = config.compression.level,
            "Configuration loaded"
        );
        debug!(
            kafka_batch_size = config.kafka.batch_size,
            kafka_session_timeout_ms = config.kafka.session_timeout_ms,
            kafka_max_poll_interval_ms = config.kafka.max_poll_interval_ms,
            buffer_flush_bytes = config.buffer.flush_bytes,
            buffer_flush_age_secs = config.buffer.flush_age_secs,
            buffer_writer_parallelism = config.buffer.writer_parallelism,
            archive_roll_size_bytes = config.archive.roll_size_bytes,
            archive_roll_interval_secs = config.archive.roll_interval_secs,
            archive_path_template = %config.archive.path_template,
            routing_mode = %config.routing.mode,
            metrics_enabled = config.metrics.enabled,
            metrics_address = %config.metrics.address,
            dlq_enabled = config.dlq.enabled,
            "Detailed configuration"
        );

        // Register archiver-specific metrics on the runtime's existing manager
        let commit = option_env!("GIT_COMMIT").unwrap_or("unknown");
        let metrics = init_metrics(&mut runtime.metrics, commit);

        // Wrap config in SharedConfig for hot-reload
        let shared_config = SharedConfig::new(config);

        // Start config reloader (SIGHUP + file polling)
        let config_path = self.common.config.clone();
        let reloader = ConfigReloader::new(
            ReloaderConfig {
                config_path: config_path.as_ref().map(std::path::PathBuf::from),
                poll_interval: Duration::from_secs(5),
                enable_sighup: true,
                ..ReloaderConfig::default()
            },
            shared_config.clone(),
            move || {
                load_config(config_path.as_deref())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
            |cfg| {
                validate_config(cfg)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        );
        let _reloader_handle = reloader.start();

        // Spawn config change security event watcher
        let security_config = shared_config.clone();
        tokio::spawn(async move {
            let mut rx = security_config.subscribe();
            while rx.changed().await.is_ok() {
                let version = *rx.borrow();
                security::config_changed(
                    "config_reload",
                    "system",
                    &format!("pipeline config reloaded (version {version})"),
                );
            }
        });

        // Create and start archiver (pass pre-initialised metrics)
        let archiver = Arc::new(
            Archiver::new(shared_config, metrics)
                .await
                .map_err(|e| CliError::Service(e.to_string()))?,
        );
        let archiver_run = Arc::clone(&archiver);

        // Verify Kafka connection
        archiver
            .check_connection()
            .map_err(|e| CliError::Service(e.to_string()))?;

        // Register health checks
        let archiver_health = Arc::clone(&archiver);
        hyperi_rustlib::health::HealthRegistry::register("kafka", move || {
            if archiver_health.is_transport_healthy() {
                hyperi_rustlib::health::HealthStatus::Healthy
            } else {
                hyperi_rustlib::health::HealthStatus::Unhealthy
            }
        });

        // Use runtime's shutdown token (signal handler already installed with pre-stop delay)
        let shutdown_token = runtime.shutdown.clone();

        // Mark pipeline ready
        archiver.metrics().set_pipeline_ready(true);
        info!("dfe-archiver ready");

        // Spawn main loop
        let run_handle = tokio::spawn(async move {
            if let Err(e) = archiver_run.run().await {
                tracing::error!(error = %e, "Archiver run failed");
            }
        });

        // Wait for shutdown signal
        shutdown_token.cancelled().await;
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

    fn deployment_contract(&self) -> Option<hyperi_rustlib::deployment::DeploymentContract> {
        Some(deployment_contract())
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
            generate_chart(&contract, output, None).unwrap_or_else(|e| {
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
