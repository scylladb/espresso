use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use aws_sdk_dynamodb::Client;
use tracing::info;

/// Holds the discovered list of Alternator node base URLs and provides
/// round-robin selection across them.
///
/// Arc-cloned into the custom HTTP connector so every Tokio task shares
/// the same node list without copying.
#[derive(Debug, Clone)]
pub struct NodeRouter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Base URLs of all known Alternator nodes, e.g. "http://10.0.0.1:8000"
    nodes: Vec<String>,
    /// Monotonically incrementing counter used for round-robin selection.
    counter: AtomicUsize,
}

impl NodeRouter {
    /// Build a NodeRouter from a static list of seed URLs provided by the
    /// user config.  Used before `discover` is called, and as a fallback if
    /// DescribeEndpoints is not available.
    pub fn from_seeds(seeds: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                nodes: seeds,
                counter: AtomicUsize::new(0),
            }),
        }
    }

    /// Call Alternator's DescribeEndpoints on the seed node to retrieve the
    /// full cluster node list, then return a new NodeRouter backed by that
    /// list.
    ///
    /// Alternator returns the full node set in the Endpoints field.
    /// Each endpoint address is "host:port"; we prepend the scheme from the
    /// seed URL.
    pub async fn discover(seed_client: &Client, seed_url: &str) -> Result<Self> {
        let scheme = if seed_url.starts_with("https") {
            "https"
        } else {
            "http"
        };

        let resp = seed_client
            .describe_endpoints()
            .send()
            .await
            .context("DescribeEndpoints failed on seed node")?;

        let endpoints = resp.endpoints();

        if endpoints.is_empty() {
            return Err(anyhow!(
                "DescribeEndpoints returned no endpoints from seed {seed_url}"
            ));
        }

        let nodes: Vec<String> = endpoints
            .iter()
            .map(|e| format!("{scheme}://{}", e.address()))
            .collect();

        info!("Discovered {} Alternator nodes: {:?}", nodes.len(), nodes);

        Ok(Self::from_seeds(nodes))
    }

    /// Pick the next node in round-robin order.  Wraps on overflow safely via
    /// modular arithmetic; the occasional modulo skip on counter wrap is
    /// acceptable for a stress tool.
    pub fn next_node(&self) -> &str {
        let idx = self.inner.counter.fetch_add(1, Ordering::Relaxed) % self.inner.nodes.len();
        &self.inner.nodes[idx]
    }

    pub fn node_count(&self) -> usize {
        self.inner.nodes.len()
    }

    pub fn nodes(&self) -> &[String] {
        &self.inner.nodes
    }
}
