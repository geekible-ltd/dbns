//! `ndb-server` — application composition and server lifecycle (spec §19.1).
//!
//! Boots the storage engine over `StorageFile`, recovers the WAL, then
//! serves the REST (`ndb-rest`, spec §15) and gRPC (`ndb-grpc`, spec §16)
//! APIs until shutdown. A periodic checkpoint task runs at the configured
//! interval.

pub mod telemetry;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use ndb_common::Result;
use ndb_common::error::Error;
use ndb_config::Config;
use ndb_storage::StorageFile;
use tracing::{info, warn};

/// Loads configuration, initialises observability, opens or creates the
/// engine and serves the REST + gRPC APIs until a shutdown signal.
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

    let engine = boot_engine(&config)?;

    // ---- REST (§15) -------------------------------------------------------
    let auth = auth_from_env();
    if auth.reader.is_some() || auth.writer.is_some() || auth.admin.is_some() {
        info!("bearer-token authentication configured");
    }
    let mut state = ndb_rest::AppState::new(Arc::clone(&engine));
    state.auth = Arc::new(auth);
    // The stress runner dials the gRPC listener on loopback.
    *state.grpc_target.lock().unwrap() =
        Some(format!("http://127.0.0.1:{}", config.node.grpc_port));
    *state.config.lock().unwrap() = Some(serde_yaml::to_value(serde_json::json!({
        "node": {"id": config.node.id, "listen": config.node.listen_address.to_string(),
                 "rest_port": config.node.rest_port, "grpc_port": config.node.grpc_port},
        "cluster": {"name": config.cluster.name, "replication_factor": config.cluster.replication_factor},
        "storage": {"path": config.storage.path.display().to_string(),
                    "page_size": config.storage.page_size,
                    "buffer_pool_bytes": config.storage.buffer_pool_size.bytes(),
                    "checkpoint_interval_seconds": config.storage.checkpoint_interval_seconds},
    })).unwrap_or_default());
    let app_state = Arc::new(state);
    let rest_app = ndb_rest::router(app_state.clone());

    // ---- gRPC (§16) -------------------------------------------------------
    let grpc_state = Arc::new(ndb_grpc::ServerState::new(engine));
    let svc = ndb_grpc::services(grpc_state);

    let rest_addr = SocketAddr::new(config.node.listen_address, config.node.rest_port);
    let grpc_addr = SocketAddr::new(config.node.listen_address, config.node.grpc_port);

    let rest_listener = tokio::net::TcpListener::bind(rest_addr)
        .await
        .map_err(Error::Io)?;
    info!(%rest_addr, "REST API listening (/api/v1)");
    info!(%grpc_addr, "gRPC API listening");

    // ---- checkpoint task ----------------------------------------------------
    let ck_engine = Arc::clone(&app_state.engine);
    let interval = config.storage.checkpoint_interval_seconds.max(5);
    let ck_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval));
        loop {
            tick.tick().await;
            if let Err(e) = ck_engine.checkpoint() {
                warn!(error = %e, "periodic checkpoint failed");
            }
        }
    });

    let grpc_server = tonic::transport::Server::builder()
        .add_service(
            ndb_grpc::proto::database_service_server::DatabaseServiceServer::new(svc.database),
        )
        .add_service(
            ndb_grpc::proto::collection_service_server::CollectionServiceServer::new(
                svc.collection,
            ),
        )
        .add_service(
            ndb_grpc::proto::document_service_server::DocumentServiceServer::new(svc.document),
        )
        .add_service(ndb_grpc::proto::search_service_server::SearchServiceServer::new(svc.search))
        .add_service(ndb_grpc::proto::index_service_server::IndexServiceServer::new(svc.index))
        .add_service(
            ndb_grpc::proto::administration_service_server::AdministrationServiceServer::new(
                svc.admin,
            ),
        )
        .add_service(
            ndb_grpc::proto::cluster_service_server::ClusterServiceServer::new(svc.cluster),
        )
        .add_service(
            ndb_grpc::proto::transaction_service_server::TransactionServiceServer::new(
                svc.transaction,
            ),
        )
        .serve(grpc_addr);

    tokio::select! {
        r = axum::serve(rest_listener, rest_app) => {
            if let Err(e) = r { warn!(error = %e, "REST server exited"); }
        }
        r = grpc_server => {
            if let Err(e) = r { warn!(error = %e, "gRPC server exited"); }
        }
        s = tokio::signal::ctrl_c() => {
            match s {
                Ok(()) => info!("shutdown signal received"),
                Err(e) => warn!(error = %e, "shutdown signal error"),
            }
        }
    }

    ck_handle.abort();
    if let Err(e) = app_state.engine.checkpoint() {
        warn!(error = %e, "final checkpoint failed");
    }
    info!("ndb-server stopped");
    Ok(())
}

/// Bearer tokens come from the environment — config-file secrets are a
/// Phase-10 concern (§17). `NDB_AUTH_{READER,WRITER,ADMIN}_TOKEN`.
fn auth_from_env() -> ndb_rest::auth::AuthConfig {
    ndb_rest::auth::AuthConfig {
        reader: std::env::var("NDB_AUTH_READER_TOKEN").ok(),
        writer: std::env::var("NDB_AUTH_WRITER_TOKEN").ok(),
        admin: std::env::var("NDB_AUTH_ADMIN_TOKEN").ok(),
    }
}

/// Opens (or formats) the data file + WAL and constructs the engine.
fn boot_engine(config: &Config) -> Result<Arc<ndb_index::Engine<StorageFile>>> {
    std::fs::create_dir_all(&config.storage.path).map_err(Error::Io)?;
    let data_path = config.storage.path.join("data.dbns");
    let wal_dir = config.storage.path.join("wal");
    std::fs::create_dir_all(&wal_dir).map_err(Error::Io)?;

    let page_size = config.storage.page_size;
    let pool_bytes = config
        .storage
        .buffer_pool_size
        .bytes()
        .max(page_size as u64 * 8);
    let txn_cfg = ndb_transaction::TxnConfig {
        snapshot_timeout: std::time::Duration::from_secs(
            config.transactions.timeout_seconds.max(30),
        ),
        wal_segment_size: 0,
    };

    let file = StorageFile::open_or_create(&data_path)?;
    let fresh = file.file_len()? == 0;

    if fresh {
        info!(path = %data_path.display(), "formatting new data file");
        let pm = ndb_page::manager::PageManager::create(file, page_size, 8)?;
        let pool = Arc::new(ndb_buffer::BufferPool::new(pm, pool_bytes));
        let (wal, _) = ndb_wal::Wal::open(&wal_dir, 64 * 1024 * 1024)?;
        let engine = ndb_index::Engine::create(pool, Arc::new(wal), txn_cfg)?;
        info!("engine initialised (fresh)");
        Ok(Arc::new(engine))
    } else {
        info!(path = %data_path.display(), "opening existing data file");
        let mut pm = ndb_page::manager::PageManager::open(file)?;
        let report = ndb_transaction::recover(&wal_dir, 64 * 1024 * 1024, &mut pm)?;
        info!(
            redone = report.pages_redone,
            undone = report.pages_undone,
            segments_retired = report.segments_retired,
            "WAL recovery complete"
        );
        let pool = Arc::new(ndb_buffer::BufferPool::new(pm, pool_bytes));
        let (wal, _) = ndb_wal::Wal::open(&wal_dir, 64 * 1024 * 1024)?;
        let engine = ndb_index::Engine::open(pool, Arc::new(wal), txn_cfg)?;
        info!("engine initialised (recovered)");
        Ok(Arc::new(engine))
    }
}
