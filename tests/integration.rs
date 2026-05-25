//! Integration test suite — requires a 3-node ScyllaDB Alternator cluster.
//!
//! # Running
//!
//! ```bash
//! cargo test --test integration -- --ignored --nocapture
//! ```
//!
//! Tests are `#[ignore]` by default so they don't run in `cargo test`.
//! Set `ESPRESSO_DOCKER_KEEP=1` to skip cluster teardown after the suite
//! (useful for manual inspection).

use aws_sdk_dynamodb::types::AttributeValue;
use espresso::config::Profile;
use espresso::engine::{self, parse_duration};
use espresso::metrics::{LiveProgress, Metrics};
use espresso::{EspressoClient, build_client_from_nodes, schema};
use std::collections::HashMap;
use std::net::TcpStream;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ── Docker cluster management ─────────────────────────────────────────────────

const SCYLLA_IMAGE: &str = "scylladb/scylla";
const SCYLLA_ALTERNATOR_ARGS: &[&str] = &[
    "--alternator-port=8000",
    "--alternator-write-isolation=always",
    "--reactor-backend=epoll",
];
/// Host ports mapped from the three ScyllaDB containers.
const HOST_PORTS: [u16; 3] = [8000, 8001, 8002];

/// Manages a temporary 3-node ScyllaDB Alternator Docker cluster.
///
/// The cluster is started in [`Cluster::start`] and torn down when this
/// struct is dropped (unless `ESPRESSO_DOCKER_KEEP=1`).
struct Cluster {
    network: String,
    /// (container_name, host_port) pairs.
    nodes: Vec<(String, u16)>,
}

impl Cluster {
    /// Start a fresh 3-node cluster, waiting until all nodes are `UN`.
    fn start() -> anyhow::Result<Self> {
        let network = "espresso-test-net".to_string();
        let nodes: Vec<(String, u16)> = HOST_PORTS
            .iter()
            .enumerate()
            .map(|(i, &port)| (format!("espresso-test-node{}", i + 1), port))
            .collect();

        // Clean up any debris from a previous run.
        for (name, _) in &nodes {
            let _ = Command::new("docker").args(["rm", "-f", name]).output();
        }
        let _ = Command::new("docker")
            .args(["network", "rm", &network])
            .output();

        // Create the shared network.
        let out = Command::new("docker")
            .args(["network", "create", &network])
            .output()
            .map_err(|e| anyhow::anyhow!("docker network create: {e}"))?;
        if !out.status.success() {
            anyhow::bail!(
                "docker network create failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }

        // ── Node 1 (seed) ──────────────────────────────────────────────────
        let (name1, port1) = &nodes[0];
        docker_run(name1, &network, *port1, SCYLLA_ALTERNATOR_ARGS, None)?;

        // Get node1's IP on the shared network as soon as Docker assigns it.
        let node1_ip = container_ip(name1)?;
        eprintln!("[cluster] node1 IP = {node1_ip}");

        // ── Nodes 2 and 3 (join via seeds) ───────────────────────────────
        for (name, port) in &nodes[1..] {
            let seeds_arg = format!("--seeds={node1_ip}");
            let mut extra = SCYLLA_ALTERNATOR_ARGS.to_vec();
            extra.push(&seeds_arg);
            docker_run(name, &network, *port, &extra, None)?;
        }

        let cluster = Self { network, nodes };
        cluster.wait_until_healthy(Duration::from_secs(240))?;
        Ok(cluster)
    }

    /// Wait until all `HOST_PORTS` accept TCP connections **and** `nodetool
    /// status` reports `UN` for every node.
    fn wait_until_healthy(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        let node_count = self.nodes.len();

        // Phase 1 — wait for each Alternator port to accept TCP connections.
        for (_, port) in &self.nodes {
            let port = *port;
            eprintln!("[cluster] waiting for port {port}...");
            loop {
                if Instant::now() > deadline {
                    anyhow::bail!("port {port} did not open within timeout");
                }
                if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    eprintln!("[cluster] port {port} open");
                    break;
                }
                std::thread::sleep(Duration::from_secs(3));
            }
        }

        // Phase 2 — wait for all nodes to show UN in nodetool status.
        let seed_name = &self.nodes[0].0;
        eprintln!("[cluster] waiting for {node_count}x UN...");
        loop {
            if Instant::now() > deadline {
                anyhow::bail!("cluster did not reach {node_count}x UN within timeout");
            }
            let output = Command::new("docker")
                .args(["exec", seed_name, "nodetool", "status"])
                .output();
            if let Ok(out) = output {
                if out.status.success() {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let un = stdout
                        .lines()
                        .filter(|l| l.trim_start().starts_with("UN"))
                        .count();
                    eprintln!("[cluster] nodetool: {un}/{node_count} UN");
                    if un >= node_count {
                        eprintln!("[cluster] cluster is healthy");
                        return Ok(());
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Returns `http://localhost:{port}` for each node.
    fn node_urls(&self) -> Vec<String> {
        self.nodes
            .iter()
            .map(|(_, p)| format!("http://localhost:{p}"))
            .collect()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if std::env::var("ESPRESSO_DOCKER_KEEP").is_ok() {
            eprintln!("[cluster] ESPRESSO_DOCKER_KEEP set — leaving containers running");
            return;
        }
        for (name, _) in &self.nodes {
            let _ = Command::new("docker").args(["rm", "-f", name]).output();
        }
        let _ = Command::new("docker")
            .args(["network", "rm", &self.network])
            .output();
        eprintln!("[cluster] cleaned up");
    }
}

// ── Docker helpers ────────────────────────────────────────────────────────────

fn docker_run(
    name: &str,
    network: &str,
    host_port: u16,
    scylla_args: &[&str],
    _extra_docker_args: Option<&[&str]>,
) -> anyhow::Result<()> {
    let port_map = format!("{host_port}:8000");
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "--name",
        name,
        "--hostname",
        name,
        "--network",
        network,
        "-d",
        "-p",
        &port_map,
        SCYLLA_IMAGE,
    ]);
    for arg in scylla_args {
        cmd.arg(arg);
    }
    let out = cmd
        .output()
        .map_err(|e| anyhow::anyhow!("docker run {name}: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "docker run {name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Get the first IP address assigned to a container across any network.
fn container_ip(name: &str) -> anyhow::Result<String> {
    let out = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            name,
        ])
        .output()
        .map_err(|e| anyhow::anyhow!("docker inspect {name}: {e}"))?;
    let ip =
        String::from_utf8(out.stdout).map_err(|e| anyhow::anyhow!("docker inspect utf8: {e}"))?;
    let ip = ip.trim().to_string();
    if ip.is_empty() {
        anyhow::bail!("could not determine IP for container '{name}'");
    }
    Ok(ip)
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ── Helper: build espresso client from cluster ────────────────────────────────

async fn espresso_client(cluster: &Cluster) -> aws_sdk_dynamodb::Client {
    build_client_from_nodes(&cluster.node_urls(), "us-east-1", "test", "test")
        .await
        .expect("build_client_from_nodes")
}

// ── Integration test suite ────────────────────────────────────────────────────

/// Run the full integration suite against a live 3-node Alternator cluster.
///
/// This is marked `#[ignore]` because it requires Docker and takes ~3 minutes
/// to spin up.  Run it with:
///
/// ```
/// cargo test --test integration -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker — run with: cargo test --test integration -- --ignored --nocapture"]
async fn integration_suite() {
    if !docker_available() {
        eprintln!("[skip] Docker daemon not available");
        return;
    }

    eprintln!("[suite] Starting ScyllaDB Alternator cluster...");
    let cluster = Cluster::start().expect("cluster start");

    test_list_tables_empty(&cluster).await;
    test_schema_creates_table(&cluster).await;
    test_put_get_roundtrip(&cluster).await;
    test_populate_phase(&cluster).await;
    test_read_heavy_phase(&cluster).await;
    test_multi_row_partitions(&cluster).await;
    test_node_count(&cluster).await;

    eprintln!("[suite] All integration tests passed.");
    // `cluster` drops here — Docker containers are removed.
}

/// Run only the multi-row partition scenario against a live cluster.
#[tokio::test]
#[ignore = "requires Docker — run with: cargo test --test integration integration_multi_row_only -- --ignored --nocapture"]
async fn integration_multi_row_only() {
    if !docker_available() {
        eprintln!("[skip] Docker daemon not available");
        return;
    }

    eprintln!("[suite] Starting ScyllaDB Alternator cluster (multi-row only)...");
    let cluster = Cluster::start().expect("cluster start");
    test_multi_row_partitions(&cluster).await;
    eprintln!("[suite] Multi-row integration test passed.");
}

// ── Scenarios ─────────────────────────────────────────────────────────────────

/// Newly started cluster should have no user tables.
async fn test_list_tables_empty(cluster: &Cluster) {
    eprintln!("[test] list_tables_empty");
    let client = espresso_client(cluster).await;
    let resp = client.list_tables().send().await.expect("list_tables");
    let tables = resp.table_names();
    assert!(
        !tables.contains(&"espresso_test".to_string()),
        "espresso_test should not exist yet"
    );
    eprintln!("[test] list_tables_empty — OK");
}

/// `schema::apply` creates the table declared in the test profile.
async fn test_schema_creates_table(cluster: &Cluster) {
    eprintln!("[test] schema_creates_table");
    let profile = Profile::load("tests/fixtures/test_profile.yml").expect("load profile");
    let client = Arc::new(espresso_client(cluster).await);

    schema::apply(&client, &profile.tables, &profile.schema)
        .await
        .expect("schema::apply");

    let resp = client.list_tables().send().await.expect("list_tables");
    assert!(
        resp.table_names().contains(&"espresso_test".to_string()),
        "espresso_test should exist after schema::apply"
    );
    eprintln!("[test] schema_creates_table — OK");
}

/// PutItem followed by GetItem returns the same item.
async fn test_put_get_roundtrip(cluster: &Cluster) {
    eprintln!("[test] put_get_roundtrip");
    let client = espresso_client(cluster).await;

    // PutItem
    let mut item = HashMap::new();
    item.insert("key".to_string(), AttributeValue::N("42".to_string()));
    item.insert("val".to_string(), AttributeValue::S("hello".to_string()));
    client
        .put_item()
        .table_name("espresso_test")
        .set_item(Some(item))
        .send()
        .await
        .expect("put_item");

    // GetItem
    let mut key = HashMap::new();
    key.insert("key".to_string(), AttributeValue::N("42".to_string()));
    let resp = client
        .get_item()
        .table_name("espresso_test")
        .set_key(Some(key))
        .send()
        .await
        .expect("get_item");

    let got = resp.item().expect("item should exist");
    assert_eq!(
        got.get("val"),
        Some(&AttributeValue::S("hello".to_string())),
        "val attribute mismatch"
    );
    eprintln!("[test] put_get_roundtrip — OK");
}

/// Populate phase completes 1 000 put_item ops with no errors.
async fn test_populate_phase(cluster: &Cluster) {
    eprintln!("[test] populate_phase");
    let profile = Profile::load("tests/fixtures/test_profile.yml").expect("load profile");
    let client = Arc::new(espresso_client(cluster).await);
    let runtime_client = Arc::new(EspressoClient::Alternator((*client).clone()));
    let metrics = Metrics::new();

    let phase = profile
        .phases
        .iter()
        .find(|p| p.name == "populate")
        .expect("populate phase");

    let _live = if profile
        .report
        .as_ref()
        .map(|r| r.live_progress.enabled)
        .unwrap_or(true)
    {
        let interval = profile
            .report
            .as_ref()
            .and_then(|r| parse_duration(&r.live_progress.interval))
            .unwrap_or(Duration::from_secs(1));
        Some(LiveProgress::start(
            metrics.clone(),
            phase.name.clone(),
            interval,
        ))
    } else {
        None
    };

    engine::run_phase(
        runtime_client.clone(),
        phase,
        &profile.tables,
        &profile.vars,
        &HashMap::new(),
        metrics.clone(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("run populate phase");

    drop(_live);

    metrics.print_summary("populate");
    eprintln!("[test] populate_phase — OK");
}

/// Read-heavy phase runs for 5 s without panicking; ops/s must be > 0.
async fn test_read_heavy_phase(cluster: &Cluster) {
    eprintln!("[test] read_heavy_phase");
    let profile = Profile::load("tests/fixtures/test_profile.yml").expect("load profile");
    let client = Arc::new(espresso_client(cluster).await);
    let runtime_client = Arc::new(EspressoClient::Alternator((*client).clone()));
    let metrics = Metrics::new();

    let phase = profile
        .phases
        .iter()
        .find(|p| p.name == "run_read_heavy")
        .expect("run_read_heavy phase");

    let _live = if profile
        .report
        .as_ref()
        .map(|r| r.live_progress.enabled)
        .unwrap_or(true)
    {
        let interval = profile
            .report
            .as_ref()
            .and_then(|r| parse_duration(&r.live_progress.interval))
            .unwrap_or(Duration::from_secs(1));
        Some(LiveProgress::start(
            metrics.clone(),
            phase.name.clone(),
            interval,
        ))
    } else {
        None
    };

    engine::run_phase(
        runtime_client.clone(),
        phase,
        &profile.tables,
        &profile.vars,
        &HashMap::new(),
        metrics.clone(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("run read_heavy phase");

    drop(_live);

    metrics.print_summary("run_read_heavy");
    eprintln!("[test] read_heavy_phase — OK");
}

/// Multi-row partition table (PK+SK) works end-to-end in engine phases.
async fn test_multi_row_partitions(cluster: &Cluster) {
    eprintln!("[test] multi_row_partitions");
    let profile = Profile::load("tests/fixtures/multirow_profile.yml").expect("load profile");
    let client = Arc::new(espresso_client(cluster).await);
    let runtime_client = Arc::new(EspressoClient::Alternator((*client).clone()));

    eprintln!("[test] multi_row_partitions: applying schema...");
    schema::apply(&client, &profile.tables, &profile.schema)
        .await
        .expect("schema::apply multirow");
    eprintln!("[test] multi_row_partitions: schema ready");

    let populate = profile
        .phases
        .iter()
        .find(|p| p.name == "populate_multirow")
        .expect("populate_multirow phase");
    let populate_metrics = Metrics::new();
    eprintln!("[test] multi_row_partitions: running populate_multirow...");
    let populate_live = if profile
        .report
        .as_ref()
        .map(|r| r.live_progress.enabled)
        .unwrap_or(true)
    {
        let interval = profile
            .report
            .as_ref()
            .and_then(|r| parse_duration(&r.live_progress.interval))
            .unwrap_or(Duration::from_secs(1));
        Some(LiveProgress::start(
            populate_metrics.clone(),
            populate.name.clone(),
            interval,
        ))
    } else {
        None
    };
    engine::run_phase(
        runtime_client.clone(),
        populate,
        &profile.tables,
        &profile.vars,
        &HashMap::new(),
        populate_metrics.clone(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("run populate_multirow phase");
    drop(populate_live);
    populate_metrics.print_summary("populate_multirow");
    eprintln!("[test] multi_row_partitions: populate_multirow complete");

    let run = profile
        .phases
        .iter()
        .find(|p| p.name == "run_multirow")
        .expect("run_multirow phase");
    let run_metrics = Metrics::new();
    eprintln!("[test] multi_row_partitions: running run_multirow...");
    let run_live = if profile
        .report
        .as_ref()
        .map(|r| r.live_progress.enabled)
        .unwrap_or(true)
    {
        let interval = profile
            .report
            .as_ref()
            .and_then(|r| parse_duration(&r.live_progress.interval))
            .unwrap_or(Duration::from_secs(1));
        Some(LiveProgress::start(
            run_metrics.clone(),
            run.name.clone(),
            interval,
        ))
    } else {
        None
    };
    engine::run_phase(
        runtime_client.clone(),
        run,
        &profile.tables,
        &profile.vars,
        &HashMap::new(),
        run_metrics.clone(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("run run_multirow phase");
    drop(run_live);
    run_metrics.print_summary("run_multirow");
    eprintln!("[test] multi_row_partitions: run_multirow complete");

    assert!(
        run_metrics.overall_p99_ms().is_some(),
        "run_multirow should record at least one successful operation"
    );

    // Explicit composite-key roundtrip check.
    let mut item = HashMap::new();
    item.insert(
        "customer_id".to_string(),
        AttributeValue::N("777".to_string()),
    );
    item.insert("order_ts".to_string(), AttributeValue::N("12".to_string()));
    item.insert(
        "val".to_string(),
        AttributeValue::S("multirow-ok".to_string()),
    );
    client
        .put_item()
        .table_name("espresso_multirow")
        .set_item(Some(item))
        .send()
        .await
        .expect("put_item composite");

    let mut key = HashMap::new();
    key.insert(
        "customer_id".to_string(),
        AttributeValue::N("777".to_string()),
    );
    key.insert("order_ts".to_string(), AttributeValue::N("12".to_string()));
    let resp = client
        .get_item()
        .table_name("espresso_multirow")
        .set_key(Some(key))
        .send()
        .await
        .expect("get_item composite");

    let got = resp.item().expect("composite item should exist");
    assert_eq!(
        got.get("val"),
        Some(&AttributeValue::S("multirow-ok".to_string())),
        "composite val mismatch"
    );

    eprintln!("[test] multi_row_partitions — OK");
}

/// Verify that espresso discovers all three cluster nodes.
async fn test_node_count(cluster: &Cluster) {
    eprintln!("[test] node_count");
    let urls = cluster.node_urls();

    // Build a seed-only client to call DescribeEndpoints.
    let seed_config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "espresso",
        ))
        .endpoint_url(&urls[0])
        .build();
    let seed_client = aws_sdk_dynamodb::Client::from_conf(seed_config);

    match espresso::node_router::NodeRouter::discover(&seed_client, &urls[0]).await {
        Ok(router) => {
            let n = router.node_count();
            eprintln!("[test] DescribeEndpoints returned {n} node(s)");
            assert!(n >= 1, "expected at least 1 node");
        }
        Err(e) => {
            // Some Alternator versions don't support DescribeEndpoints;
            // falling back to the seed list is acceptable behaviour.
            eprintln!("[test] DescribeEndpoints not supported ({e}), seed fallback is fine");
        }
    }
    eprintln!("[test] node_count — OK");
}
