// Project:   dfe-archiver
// File:      crates/archiver/src/main.rs
// Purpose:   CLI entry point using the scalo ServiceApp pattern
// Language:  Rust
//
// License:      BUSL-1.1
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
use dfe_archiver::metrics::{ArchiverMetrics, init_metrics};
use dfe_archiver::{Archiver, restart_required_changes};
use scalo::cli::{CliError, CommonArgs, ServiceApp, StandardCommand, VersionInfo, run_app};
use scalo::deployment::generate_chart;
use scalo::logger::security;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

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

impl ServiceApp for App {
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

    /// The archiver has work when it has somewhere to write and something to
    /// read. `Config::idle_reason` is the whole predicate, so the charts, the
    /// tests and this hook cannot disagree about what "no work" means.
    fn work_state(&self, config: &Self::Config) -> scalo::lifecycle::WorkState {
        config
            .idle_reason()
            .map_or(scalo::lifecycle::WorkState::Active, |reason| {
                scalo::lifecycle::WorkState::idle(reason)
            })
    }

    #[allow(clippy::too_many_lines)]
    async fn run_service(
        &self,
        config: Self::Config,
        mut runtime: scalo::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        info!(
            transport = %config.transport,
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
            metrics_address = %self.common.effective_metrics_addr(),
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

        // Spawn config change security event watcher. A section bound at
        // construction is reported as needing a restart, never as a reload.
        let security_config = shared_config.clone();
        tokio::spawn(async move {
            let mut rx = security_config.subscribe();
            let mut applied = security_config.get();
            while rx.changed().await.is_ok() {
                let version = *rx.borrow();
                let current = security_config.get();
                let pending = restart_required_changes(&applied, &current);
                if pending.is_empty() {
                    security::config_changed(
                        "config_reload",
                        "system",
                        &format!("pipeline config reloaded (version {version})"),
                    );
                } else {
                    let sections = pending.join(",");
                    warn!(
                        version,
                        sections = %sections,
                        "Config changed in sections bound at startup -- the running archiver keeps its startup values until it restarts"
                    );
                    security::config_changed(
                        "config_reload",
                        "system",
                        &format!(
                            "config change needs a restart to take effect (version {version}): {sections}"
                        ),
                    );
                }
                applied = current;
            }
        });

        // Create and start archiver. Share the runtime's memory guard (the one
        // feeding the self-regulation governor) and pass the governor so the
        // Kafka receiver gets the inbound pause-partitions brake. When
        // self_regulation is disabled, `runtime.governor` is None and the
        // receiver is built without a gate (byte-identical to pre-governor).
        // Share the runtime's unified ScalingPressure engine, built from the
        // components `scaling_components()` registers below and reaching KEDA
        // as the `dfe_scaling_pressure` gauge. The archiver's loops drive its
        // values (kafka_lag, buffer_depth, memory, circuit) directly. When the
        // `scaling` feature is off the runtime hands back None; fall back to a
        // standalone engine so the gauge path is unchanged.
        let archiver = Arc::new(
            Archiver::new(
                shared_config,
                metrics,
                Arc::clone(&runtime.memory_guard),
                runtime.governor.as_ref(),
                runtime.scaling.clone(),
            )
            .await
            .map_err(|e| CliError::Service(e.to_string()))?,
        );
        let archiver_run = Arc::clone(&archiver);

        // Verify the inbound transport and probe the archive sink.
        archiver
            .check_connection()
            .await
            .map_err(|e| CliError::Service(e.to_string()))?;

        // Register health checks. Named for the role, not for one transport:
        // the same component reports the bus consumer or the Push listener.
        let archiver_health = Arc::clone(&archiver);
        scalo::health::HealthRegistry::register("transport", move || {
            if archiver_health.is_transport_healthy() {
                scalo::health::HealthStatus::Healthy
            } else {
                scalo::health::HealthStatus::Unhealthy
            }
        });

        // The archive sink, reported separately so an unreachable bucket shows
        // on the health endpoint instead of only in the scaling gauge.
        let archiver_sink = Arc::clone(&archiver);
        scalo::health::HealthRegistry::register("archive_sink", move || {
            archiver_sink.sink_health()
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

    fn scaling_components(&self, _config: &Self::Config) -> Vec<scalo::scaling::ScalingComponent> {
        // Register the archiver's weighted KEDA components on the runtime's
        // unified ScalingPressure. The
        // pipeline drives these values (kafka_lag from assigned-partition lag,
        // buffer_depth from hot-buffer count, memory from the cgroup guard) plus
        // the object-store circuit gate, so KEDA scales on the single composite.
        dfe_archiver::scaling_components()
    }

    fn register_metrics(&self, manager: &scalo::metrics::MetricsManager) {
        // `metrics-manifest` and `generate-artefacts` read the registry without
        // starting the service, so the catalogue is empty until the archiver's
        // own metrics are built against their manager.
        let _ = ArchiverMetrics::register(manager, option_env!("GIT_COMMIT").unwrap_or("unknown"));
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(deployment_contract())
    }

    fn version_check_defaults(&self) -> scalo::version_check::VersionCheckConfig {
        // The runtime overlays the version_check cascade keys on this, so a
        // deployment's explicit enabled: false always wins.
        dfe_archiver::version_check_defaults()
    }
}

/// Live heap bytes from jemalloc, for scalo's memory guard.
///
/// jemalloc caches its statistics, so the epoch advance is what refreshes them.
#[cfg(feature = "jemalloc")]
fn heap_allocated_bytes() -> usize {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0)
}

#[tokio::main]
async fn main() {
    // Without a registered heap source the memory guard sees only the bytes the
    // pipeline accounted, so `memory_used_bytes` and the inbound brake it feeds
    // both read near zero.
    #[cfg(feature = "jemalloc")]
    let _ = scalo::memory::set_heap_source(heap_allocated_bytes);

    dotenvy::dotenv().ok();

    let app = App::parse();

    // Handle deployment contract commands before run_app
    // (these don't need logger/config init)
    match &app.command {
        Some(Command::EmitDockerfile { output }) => {
            let content = dfe_archiver::contract::emit_dockerfile();
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

    // Standard ServiceApp lifecycle (run, version, config-check)
    if let Err(e) = run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `metrics-manifest` builds a manager, calls `register_metrics` and prints
    /// the registry, so an app that leaves the no-op default in place emits an
    /// empty catalogue.
    #[test]
    fn register_metrics_fills_the_manifest() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("dfe-archiver"),
        );
        let app = App::parse_from(["dfe-archiver"]);

        app.register_metrics(&manager);

        let names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "dfe-archiver_files_created_total"),
            "metrics-manifest catalogue is missing the archiver's own metrics: {names:?}"
        );
    }

    /// The registered heap source has to move with the process heap, not with
    /// the bytes the pipeline accounted.
    #[cfg(feature = "jemalloc")]
    #[test]
    fn the_heap_source_tracks_a_live_allocation() {
        let before = heap_allocated_bytes();
        let ballast: Vec<u8> = vec![7; 64 * 1024 * 1024];
        let after = heap_allocated_bytes();
        assert!(
            after >= before + 32 * 1024 * 1024,
            "heap source read {before} then {after} across a 64 MiB allocation"
        );
        drop(ballast);
    }
}
