//! `ndb-config` — versioned YAML configuration framework (spec §25).
//!
//! Configuration is validated in full before the server starts. Invalid
//! configuration produces clear, aggregated errors — defaults are never
//! applied silently to fields the user explicitly set.
//!
//! Unknown fields are rejected (`deny_unknown_fields`) so that misspelled
//! keys cannot silently fall back to defaults.

mod size;

pub use size::ByteSize;

use serde::Deserialize;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

/// Currently supported configuration file version.
pub const SUPPORTED_CONFIG_VERSION: u16 = 1;

/// Root server configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration file format version.
    #[serde(default = "default_config_version")]
    pub version: u16,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub transactions: TransactionConfig,
    #[serde(default)]
    pub indexing: IndexingConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub vector: VectorConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
}

const fn default_config_version() -> u16 {
    SUPPORTED_CONFIG_VERSION
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Unique node identifier, e.g. `node-001`.
    #[serde(default)]
    pub id: String,
    #[serde(default = "default_listen_addr")]
    pub listen_address: IpAddr,
    #[serde(default = "default_rest_port")]
    pub rest_port: u16,
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(default = "default_cluster_name")]
    pub name: String,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,
    #[serde(default)]
    pub consensus: ConsensusAlgorithm,
    /// Virtual shards per large collection (spec §4.3).
    #[serde(default = "default_virtual_shards")]
    pub virtual_shards: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Directory containing all persistent data.
    #[serde(default)]
    pub path: PathBuf,
    /// B+Tree page size in bytes (spec §5.2 — benchmark 8/16/32 KB).
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: ByteSize,
    #[serde(default = "default_true")]
    pub wal_enabled: bool,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionConfig {
    #[serde(default)]
    pub isolation: IsolationLevel,
    #[serde(default = "default_txn_timeout")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingConfig {
    /// Maximum allowed search-visibility lag (spec §7.3).
    #[serde(default = "default_visibility_seconds")]
    pub max_visibility_seconds: u64,
    #[serde(default = "default_index_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_index_workers")]
    pub max_workers: u32,
    #[serde(default = "default_true")]
    pub durable_queue: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchConfig {
    #[serde(default = "default_search_limit")]
    pub default_limit: u32,
    #[serde(default = "default_search_max_limit")]
    pub max_limit: u32,
    #[serde(default)]
    pub bm25: Bm25Config,
    #[serde(default)]
    pub fuzzy: FuzzyConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bm25Config {
    #[serde(default = "default_bm25_k1")]
    pub k1: f64,
    #[serde(default = "default_bm25_b")]
    pub b: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuzzyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_fuzzy_max_distance")]
    pub max_distance: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    #[serde(default)]
    pub default_algorithm: VectorAlgorithm,
    #[serde(default)]
    pub default_metric: DistanceMetric,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    #[serde(default = "default_true")]
    pub authentication_enabled: bool,
    #[serde(default = "default_true")]
    pub tls_enabled: bool,
    #[serde(default = "default_true")]
    pub encryption_at_rest: bool,
    #[serde(default = "default_true")]
    pub audit_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// `tracing` level filter, e.g. `info`, `debug`, `ndb_server=trace`.
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Emit structured (JSON) logs rather than human-readable text.
    #[serde(default = "default_true")]
    pub structured: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitoringConfig {
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,
    /// Port on which the Prometheus scrape endpoint is served (spec §24).
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsensusAlgorithm {
    #[default]
    Raft,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IsolationLevel {
    /// Snapshot isolation — the initial target (spec §5.7).
    #[default]
    Snapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorAlgorithm {
    #[default]
    Hnsw,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[default]
    Cosine,
    Dot,
    Euclidean,
}

const fn default_listen_addr() -> IpAddr {
    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
}
const fn default_rest_port() -> u16 {
    8080
}
const fn default_grpc_port() -> u16 {
    9090
}
fn default_cluster_name() -> String {
    "ndb-cluster".to_string()
}
const fn default_replication_factor() -> u16 {
    3
}
const fn default_virtual_shards() -> u32 {
    256
}
const fn default_page_size() -> u32 {
    16 * 1024
}
const fn default_buffer_pool_size() -> ByteSize {
    ByteSize(3_000_000_000)
}
const fn default_checkpoint_interval() -> u64 {
    60
}
const fn default_txn_timeout() -> u64 {
    30
}
const fn default_visibility_seconds() -> u64 {
    5
}
const fn default_index_batch_size() -> u32 {
    1000
}
const fn default_index_workers() -> u32 {
    8
}
const fn default_search_limit() -> u32 {
    20
}
const fn default_search_max_limit() -> u32 {
    1000
}
const fn default_bm25_k1() -> f64 {
    1.2
}
const fn default_bm25_b() -> f64 {
    0.75
}
const fn default_fuzzy_max_distance() -> u32 {
    2
}
fn default_log_level() -> String {
    "info".to_string()
}
const fn default_metrics_port() -> u16 {
    9091
}
const fn default_true() -> bool {
    true
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            listen_address: default_listen_addr(),
            rest_port: default_rest_port(),
            grpc_port: default_grpc_port(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: default_cluster_name(),
            replication_factor: default_replication_factor(),
            consensus: ConsensusAlgorithm::default(),
            virtual_shards: default_virtual_shards(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            page_size: default_page_size(),
            buffer_pool_size: default_buffer_pool_size(),
            wal_enabled: true,
            checkpoint_interval_seconds: default_checkpoint_interval(),
        }
    }
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation: IsolationLevel::default(),
            timeout_seconds: default_txn_timeout(),
        }
    }
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            max_visibility_seconds: default_visibility_seconds(),
            batch_size: default_index_batch_size(),
            max_workers: default_index_workers(),
            durable_queue: true,
        }
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            default_limit: default_search_limit(),
            max_limit: default_search_max_limit(),
            bm25: Bm25Config::default(),
            fuzzy: FuzzyConfig::default(),
        }
    }
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self {
            k1: default_bm25_k1(),
            b: default_bm25_b(),
        }
    }
}

impl Default for FuzzyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_distance: default_fuzzy_max_distance(),
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authentication_enabled: true,
            tls_enabled: true,
            encryption_at_rest: true,
            audit_enabled: true,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            structured: true,
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            metrics_port: default_metrics_port(),
        }
    }
}

impl Config {
    /// Loads and validates configuration from a YAML file.
    ///
    /// Fails with a clear error if the file cannot be read, the YAML is
    /// malformed, fields are unknown, or validation fails.
    pub fn load(path: &Path) -> ndb_common::Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            ndb_common::Error::Config(format!("cannot read {}: {e}", path.display()))
        })?;
        Self::from_yaml(&text)
    }

    /// Parses and validates configuration from a YAML string.
    pub fn from_yaml(text: &str) -> ndb_common::Result<Self> {
        let config: Config = serde_yaml::from_str(text)
            .map_err(|e| ndb_common::Error::Config(format!("invalid configuration syntax: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates the whole configuration, aggregating every problem into a
    /// single error rather than failing on the first one found.
    pub fn validate(&self) -> ndb_common::Result<()> {
        let mut problems = Vec::new();

        if self.version != SUPPORTED_CONFIG_VERSION {
            problems.push(format!(
                "unsupported config version {} (supported: {SUPPORTED_CONFIG_VERSION})",
                self.version
            ));
        }
        if self.node.id.trim().is_empty() {
            problems.push("node.id must not be empty".into());
        }
        if self.node.rest_port == 0 {
            problems.push("node.rest_port must be non-zero".into());
        }
        if self.node.grpc_port == 0 {
            problems.push("node.grpc_port must be non-zero".into());
        }
        if self.node.rest_port == self.node.grpc_port {
            problems.push("node.rest_port and node.grpc_port must differ".into());
        }
        if self.cluster.replication_factor < 3 || self.cluster.replication_factor.is_multiple_of(2)
        {
            problems.push("cluster.replication_factor must be an odd number >= 3".into());
        }
        if self.cluster.virtual_shards == 0 {
            problems.push("cluster.virtual_shards must be non-zero".into());
        }
        if self.storage.path.as_os_str().is_empty() {
            problems.push("storage.path must not be empty".into());
        }
        if !self.storage.page_size.is_power_of_two()
            || !(4096..=65536).contains(&self.storage.page_size)
        {
            problems.push("storage.page_size must be a power of two between 4KB and 64KB".into());
        }
        if self.storage.buffer_pool_size.bytes() == 0 {
            problems.push("storage.buffer_pool_size must be non-zero".into());
        }
        if self.storage.checkpoint_interval_seconds == 0 {
            problems.push("storage.checkpoint_interval_seconds must be non-zero".into());
        }
        if self.transactions.timeout_seconds == 0 {
            problems.push("transactions.timeout_seconds must be non-zero".into());
        }
        if self.indexing.max_visibility_seconds == 0 {
            problems.push("indexing.max_visibility_seconds must be non-zero".into());
        }
        if self.indexing.batch_size == 0 {
            problems.push("indexing.batch_size must be non-zero".into());
        }
        if self.indexing.max_workers == 0 {
            problems.push("indexing.max_workers must be non-zero".into());
        }
        if self.search.default_limit == 0 || self.search.default_limit > self.search.max_limit {
            problems.push("search.default_limit must be non-zero and <= search.max_limit".into());
        }
        if self.search.bm25.k1 <= 0.0 {
            problems.push("search.bm25.k1 must be positive".into());
        }
        if !(0.0..=1.0).contains(&self.search.bm25.b) {
            problems.push("search.bm25.b must be within (0, 1]".into());
        }
        if self.search.fuzzy.max_distance > 3 {
            problems.push(
                "search.fuzzy.max_distance above 3 would cause unbounded dictionary expansion"
                    .into(),
            );
        }
        if self
            .logging
            .level
            .parse::<tracing_subscriber::filter::LevelFilter>()
            .is_err()
            && tracing_subscriber::EnvFilter::try_new(&self.logging.level).is_err()
        {
            problems.push(format!(
                "logging.level '{}' is not a valid level or filter",
                self.logging.level
            ));
        }
        if self.monitoring.metrics_enabled && self.monitoring.metrics_port == 0 {
            problems.push("monitoring.metrics_port must be non-zero".into());
        }

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ndb_common::Error::Config(format!(
                "{} configuration problem(s):\n  - {}",
                problems.len(),
                problems.join("\n  - ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
version: 1
node:
  id: node-001
  listen_address: 0.0.0.0
  rest_port: 8080
  grpc_port: 9090
cluster:
  name: ndb-cluster
  replication_factor: 3
  consensus: raft
  virtual_shards: 256
storage:
  path: ./data
  page_size: 16384
  buffer_pool_size: 3GB
  wal_enabled: true
  checkpoint_interval_seconds: 60
transactions:
  isolation: snapshot
  timeout_seconds: 30
indexing:
  max_visibility_seconds: 5
  batch_size: 1000
  max_workers: 8
  durable_queue: true
search:
  default_limit: 20
  max_limit: 1000
  bm25:
    k1: 1.2
    b: 0.75
  fuzzy:
    enabled: true
    max_distance: 2
vector:
  default_algorithm: hnsw
  default_metric: cosine
security:
  authentication_enabled: true
  tls_enabled: true
  encryption_at_rest: true
  audit_enabled: true
logging:
  level: info
  structured: true
monitoring:
  metrics_enabled: true
"#;

    #[test]
    fn parses_spec_example() {
        let cfg = Config::from_yaml(VALID).unwrap();
        assert_eq!(cfg.node.id, "node-001");
        assert_eq!(cfg.storage.page_size, 16384);
        assert_eq!(cfg.storage.buffer_pool_size.bytes(), 3_000_000_000);
        assert_eq!(cfg.cluster.replication_factor, 3);
        assert_eq!(cfg.search.bm25.k1, 1.2);
        assert_eq!(cfg.transactions.isolation, IsolationLevel::Snapshot);
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let cfg = Config::from_yaml("node:\n  id: n1\nstorage:\n  path: ./data\n").unwrap();
        assert_eq!(cfg.cluster.replication_factor, 3);
        assert_eq!(cfg.search.default_limit, 20);
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = Config::from_yaml("node:\n  id: n1\n  port_typo: 1\nstorage:\n  path: ./d\n")
            .unwrap_err();
        assert!(err.to_string().contains("port_typo") || err.to_string().contains("unknown"));
    }

    #[test]
    fn aggregates_validation_errors() {
        let err = Config::from_yaml("node:\n  id: ''\nstorage:\n  path: ''\n  page_size: 12345\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("node.id"));
        assert!(err.contains("storage.path"));
        assert!(err.contains("page_size"));
        assert!(err.contains("3 configuration problem"));
    }

    #[test]
    fn rejects_even_replication_factor() {
        let yaml = VALID.replace("replication_factor: 3", "replication_factor: 4");
        let err = Config::from_yaml(&yaml).unwrap_err().to_string();
        assert!(err.contains("replication_factor"));
    }
}
