//! Espresso library — public API used by the CLI binary and integration tests.

pub mod config;
pub mod engine;
pub mod generator;
pub mod metrics;
pub mod node_router;
pub mod resolver;
pub mod schema;

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use scylla::client::PoolSize;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing::DefaultPolicy;
use tracing::info;

use config::{Profile, TargetKind};
use node_router::NodeRouter;
use resolver::RoundRobinResolver;

#[derive(Clone)]
pub enum EspressoClient {
    Alternator(Client),
    Cql(Arc<Session>),
}

/// Build runtime client from profile target kind.
pub async fn build_client(profile: &Profile) -> Result<EspressoClient> {
    let t = &profile.target;
    match t.kind {
        TargetKind::Alternator => {
            let c = build_alternator_client_from_nodes(
                &t.nodes,
                &t.region,
                &t.auth.access_key,
                &t.auth.secret_key,
            )
            .await?;
            Ok(EspressoClient::Alternator(c))
        }
        TargetKind::Cql => {
            let s = build_cql_session_from_nodes(
                &t.nodes,
                t.cql.connections_per_shard,
                if t.auth.access_key.is_empty() {
                    None
                } else {
                    Some((t.auth.access_key.as_str(), t.auth.secret_key.as_str()))
                },
                t.cql.local_dc.as_deref(),
            )
            .await?;
            Ok(EspressoClient::Cql(Arc::new(s)))
        }
    }
}

/// Backward-compatible helper for existing tests/callers.
pub async fn build_client_from_nodes(
    nodes: &[String],
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Client> {
    build_alternator_client_from_nodes(nodes, region, access_key, secret_key).await
}

/// Build an Alternator DynamoDB client directly from node URLs and credentials.
pub async fn build_alternator_client_from_nodes(
    nodes: &[String],
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Client> {
    if nodes.is_empty() {
        anyhow::bail!("node list must not be empty");
    }

    let creds = Credentials::new(access_key, secret_key, None, None, "espresso");

    // Phase 1: seed client for DescribeEndpoints discovery.
    let seed = &nodes[0];
    let seed_config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .credentials_provider(creds.clone())
        .endpoint_url(seed)
        .build();
    let seed_client = Client::from_conf(seed_config);

    // Phase 2: discover full node list.
    let router = match NodeRouter::discover(&seed_client, seed).await {
        Ok(r) => {
            let discovered = r.nodes().to_vec();
            let configured = nodes.to_vec();

            if discovered.len() < configured.len() {
                tracing::warn!(
                    "Discovery returned {} node(s), but {} node(s) were configured; using configured nodes to ensure full coverage",
                    discovered.len(),
                    configured.len()
                );
                NodeRouter::from_seeds(dedupe_nodes(configured))
            } else {
                let merged = dedupe_nodes(
                    configured
                        .into_iter()
                        .chain(discovered.into_iter())
                        .collect(),
                );
                info!(
                    "Discovered {} Alternator node(s); using {} node(s)",
                    r.node_count(),
                    merged.len()
                );
                NodeRouter::from_seeds(merged)
            }
        }
        Err(e) => {
            tracing::warn!("Node discovery failed ({e}), using seed list");
            NodeRouter::from_seeds(nodes.to_vec())
        }
    };

    // Phase 3: production client with round-robin routing.
    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .credentials_provider(creds)
        .endpoint_resolver(RoundRobinResolver::new(router))
        .build();

    Ok(Client::from_conf(config))
}

/// Build a Scylla/Cassandra CQL session from node addresses.
pub async fn build_cql_session_from_nodes(
    nodes: &[String],
    connections_per_shard: usize,
    auth: Option<(&str, &str)>,
    local_dc: Option<&str>,
) -> Result<Session> {
    if nodes.is_empty() {
        anyhow::bail!("node list must not be empty");
    }

    let known_nodes: Vec<String> = nodes.iter().map(|n| normalize_cql_node(n)).collect();
    let count = NonZeroUsize::new(connections_per_shard.max(1)).context("invalid CQL pool size")?;

    let mut lb_builder = DefaultPolicy::builder().token_aware(true);
    if let Some(dc) = local_dc {
        lb_builder = lb_builder
            .prefer_datacenter(dc.to_string())
            .permit_dc_failover(true);
    }

    let profile_handle = ExecutionProfile::builder()
        .load_balancing_policy(lb_builder.build())
        .build()
        .into_handle();

    let mut builder = SessionBuilder::new()
        .known_nodes(&known_nodes)
        .pool_size(PoolSize::PerShard(count))
        .default_execution_profile_handle(profile_handle);

    if let Some((user, pass)) = auth {
        builder = builder.user(user, pass);
    }

    let session = builder
        .build()
        .await
        .context("failed to build CQL session")?;
    info!(
        "Connected CQL session to {} node(s) with {} connection(s) per shard",
        known_nodes.len(),
        count
    );
    Ok(session)
}

/// Parse `["KEY=VALUE", ...]` CLI override pairs into a YAML value map.
pub fn parse_var_overrides(pairs: &[String]) -> Result<HashMap<String, serde_yaml::Value>> {
    let mut map = HashMap::new();
    for pair in pairs {
        let (key, val) = pair
            .split_once('=')
            .with_context(|| format!("--var '{pair}' must be KEY=VALUE"))?;
        let yaml_val = if let Ok(n) = val.parse::<i64>() {
            serde_yaml::Value::Number(n.into())
        } else if let Ok(f) = val.parse::<f64>() {
            serde_yaml::Value::Number(serde_yaml::Number::from(f))
        } else {
            serde_yaml::Value::String(val.to_string())
        };
        map.insert(key.to_string(), yaml_val);
    }
    Ok(map)
}

fn dedupe_nodes(nodes: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(nodes.len());
    for n in nodes {
        if seen.insert(n.clone()) {
            out.push(n);
        }
    }
    out
}

fn normalize_cql_node(node: &str) -> String {
    let trimmed = node.trim();
    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .or_else(|| trimmed.strip_prefix("cql://"))
        .unwrap_or(trimmed);

    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:9042")
    }
}
