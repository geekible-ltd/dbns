//! Logging and metrics initialisation (spec §24).
//!
//! Logging is provided by `tracing`; metrics are exposed on a
//! Prometheus-compatible scrape endpoint via `metrics-exporter-prometheus`.

use metrics_exporter_prometheus::PrometheusBuilder;
use ndb_common::{Error, Result};
use ndb_config::{Config, LoggingConfig};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

/// Initialises the global `tracing` subscriber from configuration.
///
/// `logging.level` accepts either a plain level (`info`, `debug`, ...) or a
/// full filter directive (`ndb_server=trace,tokio=warn`).
/// `logging.structured` selects JSON output for log aggregation.
pub fn init_logging(config: &LoggingConfig) {
    let filter = EnvFilter::try_new(&config.level).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if config.structured {
        builder.json().init();
    } else {
        builder.init();
    }
}

/// Installs the Prometheus recorder and serves the scrape endpoint on
/// `node.listen_address:monitoring.metrics_port`.
///
/// A no-op when `monitoring.metrics_enabled` is false.
pub fn init_metrics(config: &Config) -> Result<()> {
    if !config.monitoring.metrics_enabled {
        return Ok(());
    }
    let addr = SocketAddr::new(config.node.listen_address, config.monitoring.metrics_port);
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| Error::Internal(format!("failed to start metrics exporter: {e}")))?;

    metrics::describe_counter!(
        "ndb_server_starts_total",
        "Number of times the server process has started"
    );
    metrics::counter!("ndb_server_starts_total").increment(1);
    Ok(())
}
