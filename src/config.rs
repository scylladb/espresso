//! Espresso workload profile — configuration types and loader.
//!
//! Load a YAML profile with [`Profile::load`].

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde::de::{Deserializer, Error as DeError};
use serde_yaml::Value as YamlValue;

// ── Top-level ─────────────────────────────────────────────────────────────────

/// A complete espresso workload profile.
#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub version: u32,

    /// Top-level variables.  Values may be overridden from the CLI.
    #[serde(default)]
    pub vars: HashMap<String, YamlValue>,

    pub target: Target,
    #[serde(alias = "table", deserialize_with = "deserialize_tables_section")]
    pub tables: Vec<TableDef>,
    pub schema: SchemaConfig,
    pub phases: Vec<Phase>,

    pub report: Option<ReportConfig>,
    pub compatibility: Option<CompatibilityConfig>,
}

/// The built-in default key/value profile embedded at compile time.
pub const DEFAULT_PROFILE_YAML: &str = include_str!("default_profile.yml");

impl Profile {
    /// Load and parse a profile from a YAML file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read profile {path:?}"))?;
        let profile: Self = serde_yaml::from_str(&text)
            .with_context(|| format!("failed to parse profile {path:?}"))?;
        Ok(profile)
    }

    /// Return the built-in default key/value profile, optionally overriding
    /// the target node list.
    pub fn default_profile(nodes: &[String]) -> Result<Self> {
        let mut profile: Self = serde_yaml::from_str(DEFAULT_PROFILE_YAML)
            .context("failed to parse embedded default profile")?;
        if !nodes.is_empty() {
            profile.target.nodes = nodes.to_vec();
        }
        Ok(profile)
    }
}

// ── Target ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct Target {
    pub kind: TargetKind,
    /// Seed node endpoints.
    /// Alternator: URL like `http://node1:8000`.
    /// CQL: host:port like `node1:9042`.
    pub nodes: Vec<String>,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_node_execution_mode")]
    pub node_execution_mode: NodeExecutionMode,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub cql: CqlConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Alternator,
    Cql,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionMode {
    RoundRobin,
    NodeAffinity,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    /// When `true`, Espresso connects to Scylla via CQL using `access_key` /
    /// `secret_key` as CQL credentials and fetches the `salted_hash` from
    /// `system.roles` for the role matching `access_key`.  That hash is then
    /// used as the actual Alternator `secret_key`.
    ///
    /// Required for ScyllaDB Cloud clusters where Alternator authentication
    /// uses the internal role password hash rather than the plaintext CQL
    /// password.
    #[serde(default)]
    pub fetch_salted_hash: bool,
    /// CQL contact point(s) used only for `fetch_salted_hash` lookups.
    /// Defaults to the first Alternator node with port 9042 if not set.
    #[serde(default)]
    pub cql_contact_points: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CqlConfig {
    #[serde(default = "default_keyspace")]
    pub keyspace: String,
    #[serde(default = "default_connections_per_shard")]
    pub connections_per_shard: usize,
    #[serde(default)]
    pub local_dc: Option<String>,
    #[serde(default = "default_cql_replication_factor")]
    pub replication_factor: u32,
    #[serde(default)]
    pub tablets_enabled: Option<bool>,
    #[serde(default)]
    pub tablets_initial: Option<u32>,
}

impl Default for CqlConfig {
    fn default() -> Self {
        Self {
            keyspace: default_keyspace(),
            connections_per_shard: default_connections_per_shard(),
            local_dc: None,
            replication_factor: default_cql_replication_factor(),
            tablets_enabled: None,
            tablets_initial: None,
        }
    }
}

fn default_cql_replication_factor() -> u32 {
    1
}

// ── Tables ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub billing_mode: BillingMode,
    pub key_schema: KeySchema,
    pub attributes: Vec<AttributeDef>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BillingMode {
    PayPerRequest,
    Provisioned,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeySchema {
    pub partition_key: KeyDef,
    #[serde(default)]
    pub sort_key: Option<KeyDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyDef {
    pub name: String,
    #[serde(rename = "type")]
    pub attr_type: AttributeType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AttributeDef {
    pub name: String,
    #[serde(rename = "type")]
    pub attr_type: AttributeType,
    /// For binary (`B`) attributes, the number of random bytes to generate.
    #[serde(default)]
    pub size_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributeType {
    #[serde(alias = "N", alias = "n", alias = "number")]
    BigInt,
    #[serde(alias = "S", alias = "s", alias = "string", alias = "varchar")]
    Text,
    #[serde(alias = "B", alias = "b", alias = "binary")]
    Blob,
    Int,
    SmallInt,
    TinyInt,
    Float,
    Double,
    Boolean,
}

impl AttributeType {
    pub fn is_text_like(self) -> bool {
        matches!(self, Self::Text)
    }

    pub fn is_blob_like(self) -> bool {
        matches!(self, Self::Blob)
    }

    pub fn is_numeric_like(self) -> bool {
        matches!(
            self,
            Self::BigInt | Self::Int | Self::SmallInt | Self::TinyInt | Self::Float | Self::Double
        )
    }

    pub fn to_cql_type(self) -> &'static str {
        match self {
            Self::BigInt => "bigint",
            Self::Text => "text",
            Self::Blob => "blob",
            Self::Int => "int",
            Self::SmallInt => "smallint",
            Self::TinyInt => "tinyint",
            Self::Float => "float",
            Self::Double => "double",
            Self::Boolean => "boolean",
        }
    }
}

// ── Schema ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaConfig {
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default)]
    pub delete_if_exists: bool,
    pub apply_to_tables: ApplyToTables,
}

/// Which tables the schema action applies to.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ApplyToTables {
    /// The string literal `"all"`.
    All(AllTablesTag),
    /// An explicit list of table names.
    Named(Vec<String>),
}

/// Helper that only deserializes from the exact string `"all"`.
#[derive(Debug, Clone, Deserialize)]
pub enum AllTablesTag {
    #[serde(rename = "all")]
    All,
}

// ── Phases ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct Phase {
    pub name: String,
    pub mode: PhaseMode,

    /// Wall-clock limit for `mode: duration` (e.g. `"60s"`).
    pub duration: Option<String>,

    /// Total operations for `mode: operation_count`.
    /// May be a var reference such as `${vars.populate_operation_count}`.
    pub operations: Option<NumOrVarRef>,

    /// Number of worker tasks.
    #[serde(default = "default_one_u32")]
    pub threads: u32,

    /// Per-thread in-flight request concurrency.
    #[serde(default = "default_one_u32")]
    pub concurrency: u32,

    /// Warm-up period before metrics are collected (e.g. `"10s"`, `"0s"`).
    pub warmup: String,

    #[serde(default = "default_true")]
    pub include_in_metrics: bool,

    pub table_operations: Vec<TableOperation>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseMode {
    Duration,
    OperationCount,
}

/// A value that is either a literal `u64` or a variable reference string
/// (`${vars.foo}`).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum NumOrVarRef {
    Num(u64),
    VarRef(String),
}

// ── Table operations inside a phase ──────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TableOperation {
    pub table: String,

    /// Per-table `vars` overrides for this phase entry.
    #[serde(default)]
    pub vars_overrides: HashMap<String, YamlValue>,

    /// Relative weights for each DynamoDB operation.
    /// Values are normalised to probabilities at runtime.
    pub mix: HashMap<String, f64>,

    /// Keyed by operation name (e.g. `"put_item"`).
    pub operation_templates: HashMap<String, OperationTemplate>,
}

/// Template for a single DynamoDB operation.
///
/// Which fields are populated depends on the operation:
/// - `put_item`: `item`
/// - `batch_write_item`: `items`
/// - `get_item` / `delete_item` / `update_item`: `key`
/// - `batch_get_item`: `keys`
/// - `query`: `partition_key` (+ optionally `sort_key`)
#[derive(Debug, Clone, Deserialize, Default)]
pub struct OperationTemplate {
    pub item: Option<Generator>,
    pub items: Option<Vec<Generator>>,
    pub key: Option<Generator>,
    pub keys: Option<Vec<Generator>>,
    pub partition_key: Option<Generator>,
    pub sort_key: Option<Generator>,
    pub update: Option<Generator>,
    /// Native CQL statement for named CQL operations.
    ///
    /// This allows custom operation names under `mix` / `operation_templates`
    /// for CQL workloads (for example `read_by_pk`, `write_by_pk`).
    /// Use `?` positional parameters and provide parameter sources with
    /// `cql_params`.
    ///
    /// Optional placeholder: `{table}` expands to `<keyspace>.<table>`.
    pub cql_query: Option<String>,
    /// Ordered parameter sources for `cql_query` positional `?` binds.
    /// Values reference generated attribute names (for example `key`, `val`).
    #[serde(default)]
    pub cql_params: Vec<String>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub retry_unprocessed_keys: bool,
    #[serde(default)]
    pub retry_unprocessed_items: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff: RetryBackoff,
    #[serde(default = "default_retry_base_delay_ms")]
    pub base_delay_ms: u64,
    #[serde(default = "default_retry_max_delay_ms")]
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_max_attempts(),
            backoff: RetryBackoff::default(),
            base_delay_ms: default_retry_base_delay_ms(),
            max_delay_ms: default_retry_max_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RetryBackoff {
    Fixed,
    Exponential,
    #[default]
    ExponentialJitter,
}

// ── Generator / strategy ──────────────────────────────────────────────────────

/// Configuration for generating a key or value field.
#[derive(Debug, Clone, Deserialize)]
pub struct Generator {
    pub strategy: GeneratorStrategy,

    /// Which attribute in the item holds the key value.
    pub key_field: Option<String>,
    /// Which attribute in the item holds the value (for put-style ops).
    pub value_field: Option<String>,

    // ---- strategy-specific options ----
    /// `zipf` θ skew parameter (0 < θ < 1, default 0.99).
    pub theta: Option<f64>,

    /// `fixed` — literal value to emit.
    pub value: Option<YamlValue>,

    /// `sequential` — start value (defaults to `offset` var or 0).
    pub start: Option<i64>,
    /// `sequential` — step between consecutive values (default 1).
    pub step: Option<i64>,

    /// Optional inclusive numeric range, e.g. `range: [1, 100]`.
    ///
    /// For `uniform`/`zipf`: constrains generated values to this range.
    /// For `sequential`: emits values in this range and wraps to start.
    pub range: Option<(i64, i64)>,

    /// Optional string prefix for generated keys.
    /// Applied only when the destination key type is `S`.
    pub prefix: Option<String>,

    /// Optional string suffix for generated keys.
    /// Applied only when the destination key type is `S`.
    pub suffix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorStrategy {
    #[serde(alias = "auto")]
    GeneratedFromVars,
    Sequential,
    Uniform,
    Zipf,
    Fixed,
    CachedWorkingSet,
}

// ── Report ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct ReportConfig {
    pub live_progress: LiveProgressConfig,
    pub outputs: ReportOutputs,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LiveProgressConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How often to emit a live progress line (e.g. `"1s"`).
    pub interval: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReportOutputs {
    #[serde(default = "default_true")]
    pub text_summary: bool,
    pub json_file: Option<String>,
    pub hdr_histogram_file: Option<String>,
}

// ── Compatibility ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct CompatibilityConfig {
    pub endpoint_type: String,
    #[serde(default)]
    pub enforce_supported_operations: bool,
    pub write_isolation_mode: String,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn default_true() -> bool {
    true
}

fn default_one_u32() -> u32 {
    1
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_node_execution_mode() -> NodeExecutionMode {
    NodeExecutionMode::RoundRobin
}

fn default_keyspace() -> String {
    "espresso".to_string()
}

fn default_connections_per_shard() -> usize {
    1
}

fn default_retry_max_attempts() -> u32 {
    10
}

fn default_retry_base_delay_ms() -> u64 {
    100
}

fn default_retry_max_delay_ms() -> u64 {
    5000
}

fn deserialize_tables_section<'de, D>(deserializer: D) -> std::result::Result<Vec<TableDef>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrManyTables {
        One(TableDef),
        Many(Vec<TableDef>),
    }

    match OneOrManyTables::deserialize(deserializer)? {
        OneOrManyTables::One(t) => Ok(vec![t]),
        OneOrManyTables::Many(ts) => {
            if ts.is_empty() {
                Err(D::Error::custom("profile must define at least one table"))
            } else {
                Ok(ts)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_key_value_profile() {
        let p = Profile::load("espresso_profiles/key_value.yml")
            .expect("key_value.yml should parse without errors");

        assert_eq!(p.version, 1);
        assert_eq!(p.tables.len(), 2);
        assert_eq!(p.phases.len(), 2);

        // vars
        assert!(p.vars.contains_key("partition_count"));

        // target
        assert_eq!(p.target.nodes.len(), 3);
        assert_eq!(p.target.region, "us-east-1");

        // tables
        assert_eq!(p.tables[0].name, "key_value_a");
        assert_eq!(p.tables[1].name, "key_value_b");
        assert_eq!(
            p.tables[1]
                .attributes
                .iter()
                .find(|a| a.name == "val")
                .unwrap()
                .size_bytes,
            Some(2048)
        );

        // phases
        let populate = &p.phases[0];
        assert_eq!(populate.name, "populate");
        assert_eq!(populate.mode, PhaseMode::OperationCount);
        assert!(matches!(populate.operations, Some(NumOrVarRef::VarRef(_))));

        let run = &p.phases[1];
        assert_eq!(run.name, "run_read_heavy");
        assert_eq!(run.mode, PhaseMode::Duration);
    }

    #[test]
    fn parse_retry_template_fields() {
        let yaml = r#"
{
    "version": 1,
    "target": {
        "kind": "alternator",
        "nodes": ["http://127.0.0.1:8000"]
    },
    "tables": [
        {
            "name": "t",
            "billing_mode": "pay_per_request",
            "key_schema": {
                "partition_key": {
                    "name": "key",
                    "type": "N"
                }
            },
            "attributes": [
                {
                    "name": "key",
                    "type": "N"
                }
            ]
        }
    ],
    "schema": {
        "create_if_missing": false,
        "delete_if_exists": false,
        "apply_to_tables": "all"
    },
    "phases": [
        {
            "name": "p",
            "mode": "duration",
            "duration": "1s",
            "concurrency": 1,
            "warmup": "0s",
            "include_in_metrics": true,
            "table_operations": [
                {
                    "table": "t",
                    "mix": {
                        "batch_get_item": 1.0
                    },
                    "operation_templates": {
                        "batch_get_item": {
                            "retry_unprocessed_keys": true,
                            "retry": {
                                "max_attempts": 8,
                                "backoff": "exponential_jitter",
                                "base_delay_ms": 50,
                                "max_delay_ms": 1500
                            }
                        }
                    }
                }
            ]
        }
    ]
}
"#;

        let p: Profile = serde_yaml::from_str(yaml).expect("profile with retry should parse");
        let tmpl = p.phases[0].table_operations[0]
            .operation_templates
            .get("batch_get_item")
            .expect("batch_get_item template missing");

        assert!(tmpl.retry_unprocessed_keys);
        let retry = tmpl.retry.as_ref().expect("retry config missing");
        assert_eq!(retry.max_attempts, 8);
        assert_eq!(retry.backoff, RetryBackoff::ExponentialJitter);
        assert_eq!(retry.base_delay_ms, 50);
        assert_eq!(retry.max_delay_ms, 1500);
    }

}
