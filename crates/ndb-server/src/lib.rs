//! `ndb-server` — application composition and server lifecycle (spec §19.1).

pub mod telemetry;

use ndb_common::Result;
use ndb_config::Config;
use std::path::Path;
use tracing::info;

/// Loads configuration, initialises observability and runs the server until a
/// shutdown signal is received.
///
/// Phase 0: no storage, search or network services are started yet — this
/// establishes the lifecycle scaffolding those subsystems will attach to.
pub async fn run(config_path: &Path) -> Result<()> {
    let config = Config::load(config_path)?;

    telemetry::init_logging(&config.logging);
    telemetry::init_metrics(&config)?;

    info!(
        node_id = %config.node.id,
        cluster = %config.cluster.name,
        version = env!("CARGO_PKG_VERSION"),
        "ndb-server starting"
    );
    info!(
        storage_path = %config.storage.path.display(),
        page_size = config.storage.page_size,
        buffer_pool = %config.storage.buffer_pool_size,
        replication_factor = config.cluster.replication_factor,
        isolation = ?config.transactions.isolation,
        "configuration loaded"
    );

    tokio::signal::ctrl_c()
        .await
        .map_err(ndb_common::Error::Io)?;
    info!("shutdown signal received");
    Ok(())
}
