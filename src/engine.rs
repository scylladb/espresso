//! Workload execution engine.
//!
//! [`run_phase`] drives a single profile phase: it spawns a pool of Tokio
//! worker tasks, each running a tight loop of randomly-selected DynamoDB
//! operations, and collects per-operation latency samples into a shared
//! [`Metrics`] instance.

use std::collections::HashMap;
use std::f64::consts::TAU;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest};
use scylla::client::session::Session;
use scylla::response::PagingState;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlValue;
use tokio::task::JoinSet;
use tracing::debug;

use crate::EspressoClient;
use crate::config::{
    AttributeType, GeneratorStrategy, NumOrVarRef, OperationTemplate, Phase, PhaseMode,
    RetryBackoff, RetryConfig, TableDef, TableOperation,
};
use crate::generator::{
    CachedWorkingSet, KeyGen, ResolvedVars, ValueGen, build_key_gen, build_value_gen, resolve_vars,
};
use crate::metrics::Metrics;

// ── Phase entry-point ─────────────────────────────────────────────────────────

/// Run a single phase, writing results into the provided [`Metrics`] instance.
///
/// The caller creates `Metrics::new()`, optionally starts a `LiveProgress`
/// reporter on a clone, then passes it here.  Because `Metrics` is
/// `Clone + Arc`-backed, all clones observe the same live data.
pub async fn run_phase(
    client: Arc<EspressoClient>,
    phase: &Phase,
    tables: &[TableDef],
    profile_vars: &HashMap<String, serde_yaml::Value>,
    cli_vars: &HashMap<String, serde_yaml::Value>,
    metrics: Metrics,
    throttle_ops_per_sec: Option<u32>,
    target_p99_ms: Option<f64>,
    throttle_sine_amplitude: Option<f64>,
    throttle_sine_period: Option<Duration>,
    cql_keyspace: Option<String>,
) -> Result<()> {
    // Resolve stop condition.
    let stop = Arc::new(StopCondition::from_phase(phase, profile_vars)?);

    // Build one dispatcher per table_operation entry.
    let dispatchers: Vec<Arc<Dispatcher>> = phase
        .table_operations
        .iter()
        .map(|top| {
            let table_def = tables
                .iter()
                .find(|t| t.name == top.table)
                .with_context(|| {
                    format!(
                        "phase '{}': table '{}' not found in profile tables",
                        phase.name, top.table
                    )
                })?;

            // Merge vars: profile → table_operation overrides → cli
            let base_vars = resolve_vars(profile_vars, &top.vars_overrides);
            let resolved = resolve_vars(
                &serde_yaml::to_value(&base_vars)
                    .ok()
                    .and_then(|v| {
                        v.as_mapping().map(|m| {
                            m.iter()
                                .map(|(k, v)| (k.as_str().unwrap_or("").to_string(), v.clone()))
                                .collect::<HashMap<_, _>>()
                        })
                    })
                    .unwrap_or_default(),
                cli_vars,
            );
            // Prefer the richer merged set
            let vars = {
                let mut v = base_vars;
                v.extend(resolved);
                v
            };

            Dispatcher::new(top, table_def, &vars, cql_keyspace.clone())
                .map(Arc::new)
                .with_context(|| format!("failed to build dispatcher for table '{}'", top.table))
        })
        .collect::<Result<_>>()?;

    // Total weight for table selection (round-robin proportional to equal
    // weight per table_operation entry — simple uniform choice for now).
    let n_dispatchers = dispatchers.len();

    // Warmup — run but don't record metrics.
    let warmup_dur = parse_duration(&phase.warmup).unwrap_or(Duration::ZERO);

    // Spawn worker tasks.
    let effective_concurrency = phase
        .threads
        .saturating_mul(phase.concurrency.max(1));
    let concurrency = effective_concurrency as usize;
    let mut join_set = JoinSet::new();
    let adaptive_default_rate = effective_concurrency.max(1) * 100;
    let initial_rate =
        throttle_ops_per_sec.or_else(|| target_p99_ms.map(|_| adaptive_default_rate));
    let throttle = initial_rate
        .map(|rate| {
            GlobalThrottle::new(
                rate,
                throttle_sine_amplitude.unwrap_or(0.0),
                throttle_sine_period.unwrap_or(Duration::from_secs(60)),
            )
        })
        .map(Arc::new);

    // If adaptive p99 targeting is enabled, continuously adjust throttle.
    let controller = if let (Some(target), Some(t)) = (target_p99_ms, throttle.clone()) {
        let stop = stop.clone();
        let metrics = metrics.clone();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if stop.should_stop() {
                    break;
                }

                let Some(p99_ms) = metrics.overall_p99_ms() else {
                    continue;
                };

                let current = t.rate();
                let new_rate = if p99_ms > target * 1.05 {
                    ((current as f64 * 0.90).floor() as u32).max(1)
                } else if p99_ms < target * 0.90 {
                    ((current as f64 * 1.10).ceil() as u32).min(1_000_000)
                } else {
                    current
                };

                if new_rate != current {
                    t.set_rate(new_rate);
                    tracing::debug!(
                        "adaptive-throttle: p99={:.2}ms target={:.2}ms rate {} -> {} ops/s",
                        p99_ms,
                        target,
                        current,
                        new_rate
                    );
                }
            }
        }))
    } else {
        None
    };

    for worker_id in 0..concurrency {
        let client = client.clone();
        let dispatchers = dispatchers.clone();
        let metrics = metrics.clone();
        let stop = stop.clone();
        let throttle = throttle.clone();
        let warmup_end = Instant::now() + warmup_dur;
        let include = phase.include_in_metrics;

        join_set.spawn(async move {
            let mut op_idx: usize = worker_id; // stagger starting op per worker
            loop {
                if stop.should_stop() {
                    break;
                }

                let dispatcher = &dispatchers[op_idx % n_dispatchers];
                let op_name = dispatcher.sample_op();
                op_idx = op_idx.wrapping_add(1);

                let scheduled_at = match &throttle {
                    Some(t) => t.acquire().await,
                    None => Instant::now(),
                };

                let result = dispatcher.execute(&client, &op_name).await;
                let elapsed = Instant::now().saturating_duration_since(scheduled_at);

                let recording = include && Instant::now() >= warmup_end;

                match result {
                    Ok(()) => {
                        if recording {
                            metrics.record_ok(&op_name, elapsed);
                        }
                        if stop.is_op_count() {
                            stop.increment();
                        }
                    }
                    Err(e) => {
                        if recording {
                            metrics.record_err(&op_name);
                        }
                        debug!("op '{}' error: {e:#}", op_name);
                        if stop.is_op_count() {
                            stop.increment();
                        }
                    }
                }
            }
        });
    }

    join_set.join_all().await;
    if let Some(h) = controller {
        h.abort();
    }
    Ok(())
}

// ── Global throttle ──────────────────────────────────────────────────────────

/// Phase-wide operation rate limiter shared across all workers.
///
/// It schedules operation starts on a single global timeline, so aggregate
/// throughput converges to `rate` ops/sec regardless of concurrency.
struct GlobalThrottle {
    rate: std::sync::atomic::AtomicU32,
    start: Instant,
    sine_amplitude: f64,
    sine_period: Duration,
    next_deadline: tokio::sync::Mutex<tokio::time::Instant>,
}

impl GlobalThrottle {
    fn new(rate: u32, sine_amplitude: f64, sine_period: Duration) -> Self {
        let start = Instant::now();
        Self {
            rate: std::sync::atomic::AtomicU32::new(rate.max(1)),
            start,
            sine_amplitude,
            sine_period,
            next_deadline: tokio::sync::Mutex::new(tokio::time::Instant::from_std(start)),
        }
    }

    fn rate(&self) -> u32 {
        self.rate.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_rate(&self, rate: u32) {
        self.rate
            .store(rate.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    fn current_rate(&self, now: Instant) -> f64 {
        let base_rate = self.rate() as f64;
        if self.sine_amplitude <= 0.0 {
            return base_rate;
        }

        let elapsed = now.saturating_duration_since(self.start).as_secs_f64();
        let period_secs = self.sine_period.as_secs_f64().max(0.001);
        let phase = TAU * (elapsed / period_secs);
        let adjusted = base_rate * (1.0 + self.sine_amplitude * phase.sin());

        adjusted.max(1.0)
    }

    async fn acquire(&self) -> Instant {
        let mut next_deadline = self.next_deadline.lock().await;
        let now = tokio::time::Instant::now();
        if *next_deadline < now {
            *next_deadline = now;
        }

        let scheduled = *next_deadline;
        let rate = self.current_rate(Instant::now());
        let interval = Duration::from_secs_f64(1.0 / rate);
        *next_deadline += interval;
        drop(next_deadline);

        tokio::time::sleep_until(scheduled).await;
        scheduled.into_std()
    }
}

// ── Stop condition ────────────────────────────────────────────────────────────

enum StopCondition {
    Duration(Instant),
    OpCount {
        target: u64,
        counter: std::sync::atomic::AtomicU64,
    },
}

impl StopCondition {
    fn from_phase(phase: &Phase, vars: &HashMap<String, serde_yaml::Value>) -> Result<Self> {
        match phase.mode {
            PhaseMode::Duration => {
                let dur_str = phase.duration.as_deref().unwrap_or("60s");
                let dur = parse_duration(dur_str)
                    .with_context(|| format!("invalid duration '{dur_str}'"))?;
                Ok(Self::Duration(Instant::now() + dur))
            }
            PhaseMode::OperationCount => {
                let target = match &phase.operations {
                    None => bail!(
                        "operation_count phase '{}' missing 'operations'",
                        phase.name
                    ),
                    Some(NumOrVarRef::Num(n)) => *n,
                    Some(NumOrVarRef::VarRef(s)) => {
                        // Parse ${vars.foo} syntax.
                        let var_name = s.trim_start_matches("${vars.").trim_end_matches('}');
                        vars.get(var_name)
                            .and_then(|v| v.as_u64())
                            .with_context(|| {
                                format!("var '{var_name}' not found or not a number")
                            })?
                    }
                };
                Ok(Self::OpCount {
                    target,
                    counter: std::sync::atomic::AtomicU64::new(0),
                })
            }
        }
    }

    fn should_stop(&self) -> bool {
        match self {
            Self::Duration(deadline) => Instant::now() >= *deadline,
            Self::OpCount { target, counter } => {
                counter.load(std::sync::atomic::Ordering::Relaxed) >= *target
            }
        }
    }

    fn is_op_count(&self) -> bool {
        matches!(self, Self::OpCount { .. })
    }

    fn increment(&self) {
        if let Self::OpCount { counter, .. } = self {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

// ── Operation dispatcher ──────────────────────────────────────────────────────

/// Holds the compiled generators for one `table_operations` entry and can
/// dispatch operations against a live DynamoDB client.
struct Dispatcher {
    table: String,
    cql_keyspace: Option<String>,
    attr_types: HashMap<String, AttributeType>,
    pk_name: String,
    sk_name: Option<String>,
    /// Operation name → cumulative weight (for weighted selection).
    op_weights: Vec<(String, f64)>,
    /// Per-operation generators.
    op_gens: HashMap<String, OpGen>,
    cql_prepared: tokio::sync::Mutex<HashMap<String, Arc<PreparedStatement>>>,
}

/// Compiled generators for a single operation.
struct OpGen {
    partition_key_gen: Option<KeyGen>,
    partition_key_cws: Option<CachedWorkingSet>,
    partition_key_type: AttributeType,
    partition_key_prefix: Option<String>,
    partition_key_suffix: Option<String>,
    sort_key_gen: Option<KeyGen>,
    sort_key_type: Option<AttributeType>,
    sort_key_prefix: Option<String>,
    sort_key_suffix: Option<String>,
    sort_key_offset: i64,
    rows_per_partition_fixed: Option<u64>,
    rows_per_partition_range: Option<(u64, u64)>,
    value_gen: Option<ValueGen>,
    value_field: Option<String>,
    batch_size: usize,
    cql_query: Option<String>,
    cql_params: Vec<String>,
    retry: RetryPolicy,
    retry_unprocessed_keys: bool,
    retry_unprocessed_items: bool,
}

fn canonical_cql_op_name(op: &str) -> &str {
    match op {
        "insert" => "put_item",
        "select" => "get_item",
        "delete" => "delete_item",
        "update" => "update_item",
        "batch_select" => "batch_get_item",
        "batch_insert" => "batch_write_item",
        _ => op,
    }
}

fn canonical_op_name_for_generator(op: &str) -> &str {
    canonical_cql_op_name(op)
}

#[derive(Debug, Clone)]
struct RetryPolicy {
    max_attempts: u32,
    backoff: RetryBackoff,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

impl RetryPolicy {
    fn from_config(cfg: Option<&RetryConfig>) -> Self {
        let mut policy = match cfg {
            Some(c) => Self {
                max_attempts: c.max_attempts.max(1),
                backoff: c.backoff.clone(),
                base_delay_ms: c.base_delay_ms.max(1),
                max_delay_ms: c.max_delay_ms.max(1),
            },
            None => Self {
                max_attempts: 1,
                backoff: RetryBackoff::ExponentialJitter,
                base_delay_ms: 100,
                max_delay_ms: 5000,
            },
        };

        if policy.max_delay_ms < policy.base_delay_ms {
            policy.max_delay_ms = policy.base_delay_ms;
        }
        policy
    }

    fn next_delay(&self, retry_idx: u32) -> Duration {
        let shift = retry_idx.min(20);
        let exp = self.base_delay_ms.saturating_mul(1u64 << shift);
        let bounded = exp.min(self.max_delay_ms);
        let delay_ms = match self.backoff {
            RetryBackoff::Fixed => self.base_delay_ms.min(self.max_delay_ms),
            RetryBackoff::Exponential => bounded,
            RetryBackoff::ExponentialJitter => {
                use rand::Rng;
                rand::thread_rng().gen_range(0..=bounded)
            }
        };
        Duration::from_millis(delay_ms.max(1))
    }
}

impl Dispatcher {
    fn new(
        top: &TableOperation,
        table_def: &TableDef,
        vars: &ResolvedVars,
        cql_keyspace: Option<String>,
    ) -> Result<Self> {
        // Normalise mix weights into a cumulative distribution.
        let total: f64 = top.mix.values().sum();
        if total == 0.0 {
            bail!("table '{}' mix has zero total weight", top.table);
        }
        let mut cumulative = 0.0f64;
        let op_weights: Vec<(String, f64)> = top
            .mix
            .iter()
            .map(|(name, w)| {
                cumulative += w / total;
                (name.clone(), cumulative)
            })
            .collect();

        // Build generators for each operation in the mix.
        let mut op_gens = HashMap::new();
        for (op_name, _) in &op_weights {
            let tmpl = top
                .operation_templates
                .get(op_name.as_str())
                .cloned()
                .unwrap_or_default();

            let og = build_op_gen(&tmpl, table_def, op_name, vars)?;
            op_gens.insert(op_name.clone(), og);
        }

        Ok(Self {
            table: top.table.clone(),
            cql_keyspace,
            attr_types: table_def
                .attributes
                .iter()
                .map(|a| (a.name.clone(), a.attr_type))
                .collect(),
            pk_name: table_def.key_schema.partition_key.name.clone(),
            sk_name: table_def
                .key_schema
                .sort_key
                .as_ref()
                .map(|k| k.name.clone()),
            op_weights,
            op_gens,
            cql_prepared: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Pick an operation name according to the mix weights.
    fn sample_op(&self) -> String {
        // Fast path for single-op tables.
        if self.op_weights.len() == 1 {
            return self.op_weights[0].0.clone();
        }
        // Use thread_rng for selection — cheap, non-shared.
        use rand::Rng;
        let r: f64 = rand::thread_rng().gen_range(0.0..1.0);
        for (name, threshold) in &self.op_weights {
            if r < *threshold {
                return name.clone();
            }
        }
        self.op_weights.last().unwrap().0.clone()
    }

    async fn execute(&self, client: &EspressoClient, op: &str) -> Result<()> {
        let og = self.op_gens.get(op).context("missing op generator")?;
        self.execute_with_retry(op, &og.retry, || async {
            match client {
                EspressoClient::Alternator(c) => match op {
                    "put_item" => self.put_item(c, og).await,
                    "get_item" => self.get_item(c, og).await,
                    "delete_item" => self.delete_item(c, og).await,
                    "update_item" => self.update_item(c, og).await,
                    "batch_get_item" => self.batch_get_item(c, og).await,
                    "batch_write_item" => self.batch_write_item(c, og).await,
                    other => bail!("unsupported operation '{other}'"),
                },
                EspressoClient::Cql(session) => match op {
                    _ if og.cql_query.is_some() => self.cql_named_query(session, op, og).await,
                    _ => match canonical_cql_op_name(op) {
                        "put_item" => self.cql_put_item(session, og).await,
                        "get_item" => self.cql_get_item(session, og).await,
                        "delete_item" => self.cql_delete_item(session, og).await,
                        "update_item" => self.cql_update_item(session, og).await,
                        "batch_get_item" => self.cql_batch_get_item(session, og).await,
                        "batch_write_item" => self.cql_batch_write_item(session, og).await,
                        other => bail!("unsupported CQL operation '{other}'"),
                    },
                },
            }
        })
        .await
    }

    async fn execute_with_retry<F, Fut>(
        &self,
        op: &str,
        policy: &RetryPolicy,
        mut run: F,
    ) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut attempt = 0u32;
        loop {
            match run().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    attempt += 1;
                    if attempt >= policy.max_attempts {
                        return Err(err.context(format!(
                            "operation '{op}' failed after {attempt} attempt(s)"
                        )));
                    }

                    let delay = policy.next_delay(attempt - 1);
                    debug!(
                        "op '{}' failed on attempt {}/{}; retrying in {:?}",
                        op, attempt, policy.max_attempts, delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    fn next_key_map(&self, og: &OpGen) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::new();
        let pk = og.next_partition_key();
        key.insert(self.pk_name.clone(), pk.clone());
        if let Some(sk_name) = &self.sk_name {
            key.insert(sk_name.clone(), og.next_sort_key_for_partition(&pk));
        }
        key
    }

    fn next_item_map(&self, og: &OpGen) -> HashMap<String, AttributeValue> {
        let mut item = self.next_key_map(og);
        if let Some(vg) = &og.value_gen {
            if let Some(value_field) = &og.value_field {
                item.insert(value_field.clone(), vg.next());
            }
        }
        item
    }

    async fn put_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let item = self.next_item_map(og);
        client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .send()
            .await
            .context("put_item failed")?;
        Ok(())
    }

    async fn get_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        client
            .get_item()
            .table_name(&self.table)
            .set_key(Some(key))
            .send()
            .await
            .context("get_item failed")?;
        Ok(())
    }

    async fn delete_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        client
            .delete_item()
            .table_name(&self.table)
            .set_key(Some(key))
            .send()
            .await
            .context("delete_item failed")?;
        Ok(())
    }

    async fn update_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        let new_val = og
            .value_gen
            .as_ref()
            .map(|v| v.next())
            .unwrap_or(AttributeValue::N("0".to_string()));
        let value_field = og.value_field.as_deref().unwrap_or("val");
        client
            .update_item()
            .table_name(&self.table)
            .set_key(Some(key))
            .update_expression("SET #v = :v")
            .expression_attribute_names("#v", value_field)
            .expression_attribute_values(":v", new_val)
            .send()
            .await
            .context("update_item failed")?;
        Ok(())
    }

    async fn batch_get_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let keys: Vec<HashMap<String, AttributeValue>> =
            (0..og.batch_size).map(|_| self.next_key_map(og)).collect();

        let kaa = KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .build()
            .context("failed to build KeysAndAttributes")?;

        let out = client
            .batch_get_item()
            .request_items(self.table.clone(), kaa)
            .send()
            .await
            .context("batch_get_item failed")?;

        if og.retry_unprocessed_keys {
            let has_unprocessed = out
                .unprocessed_keys()
                .map(|m| !m.is_empty())
                .unwrap_or(false);
            if has_unprocessed {
                bail!("batch_get_item returned unprocessed keys")
            }
        }
        Ok(())
    }

    async fn batch_write_item(&self, client: &Client, og: &OpGen) -> Result<()> {
        let mut writes = Vec::with_capacity(og.batch_size);
        for _ in 0..og.batch_size {
            let item = self.next_item_map(og);
            let put = PutRequest::builder()
                .set_item(Some(item))
                .build()
                .context("failed to build PutRequest")?;
            let wr = WriteRequest::builder().put_request(put).build();
            writes.push(wr);
        }

        let out = client
            .batch_write_item()
            .request_items(self.table.clone(), writes)
            .send()
            .await
            .context("batch_write_item failed")?;

        if og.retry_unprocessed_items {
            let has_unprocessed = out
                .unprocessed_items()
                .map(|m| !m.is_empty())
                .unwrap_or(false);
            if has_unprocessed {
                bail!("batch_write_item returned unprocessed items")
            }
        }
        Ok(())
    }

    async fn cql_prepare(
        &self,
        session: &Session,
        cache_key: String,
        cql: String,
    ) -> Result<Arc<PreparedStatement>> {
        if let Some(stmt) = self.cql_prepared.lock().await.get(&cache_key).cloned() {
            return Ok(stmt);
        }

        let prepared = Arc::new(
            session
                .prepare(Statement::new(cql))
                .await
                .context("failed to prepare CQL statement")?,
        );

        let mut cache = self.cql_prepared.lock().await;
        let entry = cache.entry(cache_key).or_insert_with(|| prepared.clone());
        Ok(entry.clone())
    }

    fn cql_table_name(&self) -> String {
        match &self.cql_keyspace {
            Some(ks) => format!("{ks}.{}", self.table),
            None => self.table.clone(),
        }
    }

    fn cql_sort_clause(&self) -> &'static str {
        if self.sk_name.is_some() {
            " AND sk = ?"
        } else {
            ""
        }
    }

    fn cql_value_col(&self, og: &OpGen) -> String {
        og.value_field.clone().unwrap_or_else(|| "val".to_string())
    }

    fn attr_type_for(&self, name: &str) -> Result<AttributeType> {
        self.attr_types
            .get(name)
            .copied()
            .with_context(|| format!("attribute '{name}' not found in table '{}'", self.table))
    }

    async fn cql_put_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        let item = self.next_item_map(og);
        let pk = item.get(&self.pk_name).context("missing partition key")?;
        let val_col = self.cql_value_col(og);
        let sk_val = self.sk_name.as_ref().and_then(|name| item.get(name));
        let val = item.get(&val_col);

        match (sk_val, val) {
            (Some(sk), Some(v)) => {
                let q = format!(
                    "INSERT INTO {} ({}, {}, {}) VALUES (?, ?, ?)",
                    self.cql_table_name(),
                    self.pk_name,
                    self.sk_name.as_ref().unwrap(),
                    val_col
                );
                let stmt = self
                    .cql_prepare(session, format!("put:{}:3", self.table), q)
                    .await?;
                let (pk, sk, v) = (
                    to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                    to_cql_value(
                        sk,
                        self.attr_type_for(self.sk_name.as_deref().unwrap_or(""))?,
                    )?,
                    to_cql_value(v, self.attr_type_for(&val_col)?)?,
                );
                session
                    .execute_single_page(&stmt, vec![pk, sk, v], PagingState::start())
                    .await
                    .context("CQL put_item failed")?;
            }
            (Some(sk), None) => {
                let q = format!(
                    "INSERT INTO {} ({}, {}) VALUES (?, ?)",
                    self.cql_table_name(),
                    self.pk_name,
                    self.sk_name.as_ref().unwrap()
                );
                let stmt = self
                    .cql_prepare(session, format!("put:{}:2pk", self.table), q)
                    .await?;
                let (pk, sk) = (
                    to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                    to_cql_value(
                        sk,
                        self.attr_type_for(self.sk_name.as_deref().unwrap_or(""))?,
                    )?,
                );
                session
                    .execute_single_page(&stmt, vec![pk, sk], PagingState::start())
                    .await
                    .context("CQL put_item failed")?;
            }
            (None, Some(v)) => {
                let q = format!(
                    "INSERT INTO {} ({}, {}) VALUES (?, ?)",
                    self.cql_table_name(),
                    self.pk_name,
                    val_col
                );
                let stmt = self
                    .cql_prepare(session, format!("put:{}:2", self.table), q)
                    .await?;
                let (pk, v) = (
                    to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                    to_cql_value(v, self.attr_type_for(&val_col)?)?,
                );
                session
                    .execute_single_page(&stmt, vec![pk, v], PagingState::start())
                    .await
                    .context("CQL put_item failed")?;
            }
            (None, None) => {
                let q = format!(
                    "INSERT INTO {} ({}) VALUES (?)",
                    self.cql_table_name(),
                    self.pk_name
                );
                let stmt = self
                    .cql_prepare(session, format!("put:{}:1", self.table), q)
                    .await?;
                let pk = to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?;
                session
                    .execute_single_page(&stmt, vec![pk], PagingState::start())
                    .await
                    .context("CQL put_item failed")?;
            }
        }

        Ok(())
    }

    async fn cql_get_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        let pk = key.get(&self.pk_name).context("missing partition key")?;
        let sk = self.sk_name.as_ref().and_then(|name| key.get(name));

        let q = format!(
            "SELECT * FROM {} WHERE {} = ?{}",
            self.cql_table_name(),
            self.pk_name,
            self.cql_sort_clause()
        );
        let stmt = self
            .cql_prepare(session, format!("get:{}", self.table), q)
            .await?;

        match sk {
            Some(sk) => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![
                            to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                            to_cql_value(
                                sk,
                                self.attr_type_for(self.sk_name.as_deref().unwrap_or(""))?,
                            )?,
                        ],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL get_item failed")?;
            }
            None => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL get_item failed")?;
            }
        }

        Ok(())
    }

    async fn cql_delete_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        let pk = key.get(&self.pk_name).context("missing partition key")?;
        let sk = self.sk_name.as_ref().and_then(|name| key.get(name));

        let q = format!(
            "DELETE FROM {} WHERE {} = ?{}",
            self.cql_table_name(),
            self.pk_name,
            self.cql_sort_clause()
        );
        let stmt = self
            .cql_prepare(session, format!("delete:{}", self.table), q)
            .await?;

        match sk {
            Some(sk) => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![
                            to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                            to_cql_value(
                                sk,
                                self.attr_type_for(self.sk_name.as_deref().unwrap_or(""))?,
                            )?,
                        ],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL delete_item failed")?;
            }
            None => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL delete_item failed")?;
            }
        }

        Ok(())
    }

    async fn cql_update_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        let key = self.next_key_map(og);
        let pk = key.get(&self.pk_name).context("missing partition key")?;
        let sk = self.sk_name.as_ref().and_then(|name| key.get(name));
        let val_col = self.cql_value_col(og);
        let new_val = og
            .value_gen
            .as_ref()
            .map(|v| v.next())
            .unwrap_or(AttributeValue::N("0".to_string()));

        let q = format!(
            "UPDATE {} SET {} = ? WHERE {} = ?{}",
            self.cql_table_name(),
            val_col,
            self.pk_name,
            self.cql_sort_clause()
        );
        let stmt = self
            .cql_prepare(session, format!("update:{}", self.table), q)
            .await?;

        match sk {
            Some(sk) => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![
                            to_cql_value(&new_val, self.attr_type_for(&val_col)?)?,
                            to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                            to_cql_value(
                                sk,
                                self.attr_type_for(self.sk_name.as_deref().unwrap_or(""))?,
                            )?,
                        ],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL update_item failed")?;
            }
            None => {
                session
                    .execute_single_page(
                        &stmt,
                        vec![
                            to_cql_value(&new_val, self.attr_type_for(&val_col)?)?,
                            to_cql_value(pk, self.attr_type_for(&self.pk_name)?)?,
                        ],
                        PagingState::start(),
                    )
                    .await
                    .context("CQL update_item failed")?;
            }
        }

        Ok(())
    }

    async fn cql_batch_get_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        // For CQL there is no true BatchGet equivalent; execute N point-reads.
        for _ in 0..og.batch_size {
            self.cql_get_item(session, og).await?;
        }
        Ok(())
    }

    async fn cql_batch_write_item(&self, session: &Session, og: &OpGen) -> Result<()> {
        // Use repeated prepared inserts for throughput; avoids logged batch bottlenecks.
        for _ in 0..og.batch_size {
            self.cql_put_item(session, og).await?;
        }
        Ok(())
    }

    async fn cql_named_query(&self, session: &Session, op: &str, og: &OpGen) -> Result<()> {
        let query_tmpl = og
            .cql_query
            .as_ref()
            .context("missing cql_query for named CQL operation")?;

        let query = query_tmpl.replace("{table}", &self.cql_table_name());
        let stmt = self
            .cql_prepare(session, format!("named:{}:{op}", self.table), query)
            .await?;

        let values = self.next_item_map(og);
        let params: Vec<CqlValue> = og
            .cql_params
            .iter()
            .map(|name| {
                values
                    .get(name)
                    .with_context(|| {
                        format!(
                            "named CQL op '{op}' references param '{name}' that is not generated"
                        )
                    })
                    .and_then(|v| to_cql_value(v, self.attr_type_for(name)?))
            })
            .collect::<Result<_>>()?;

        session
            .execute_single_page(&stmt, params, PagingState::start())
            .await
            .with_context(|| format!("named CQL operation '{op}' failed"))?;
        Ok(())
    }
}

// ── OpGen helpers ─────────────────────────────────────────────────────────────

impl OpGen {
    fn next_partition_key(&self) -> AttributeValue {
        let raw = if let Some(cws) = &self.partition_key_cws {
            cws.next()
        } else {
            self.partition_key_gen
                .as_ref()
                .map(|g| g.next())
                .unwrap_or(AttributeValue::N("0".to_string()))
        };
        coerce_key_attr(
            raw,
            self.partition_key_type,
            self.partition_key_prefix.as_deref(),
            self.partition_key_suffix.as_deref(),
        )
    }

    fn next_sort_key_for_partition(&self, partition_key: &AttributeValue) -> AttributeValue {
        let mut raw_i64 = self
            .sort_key_gen
            .as_ref()
            .map(|g| g.next_i64())
            .unwrap_or(self.sort_key_offset);

        if let Some(rows) = self.rows_for_partition(partition_key) {
            let span = rows.max(1) as i64;
            raw_i64 = self.sort_key_offset + (raw_i64 - self.sort_key_offset).rem_euclid(span);
        }

        coerce_key_attr(
            AttributeValue::N(raw_i64.to_string()),
            self.sort_key_type.unwrap_or(AttributeType::BigInt),
            self.sort_key_prefix.as_deref(),
            self.sort_key_suffix.as_deref(),
        )
    }

    fn rows_for_partition(&self, partition_key: &AttributeValue) -> Option<u64> {
        if let Some(fixed) = self.rows_per_partition_fixed {
            return Some(fixed.max(1));
        }

        if let Some((min, max)) = self.rows_per_partition_range {
            let min = min.max(1);
            let max = max.max(min);
            let width = max - min + 1;
            let h = partition_bucket(partition_key);
            return Some(min + (h % width));
        }

        None
    }
}

fn partition_bucket(v: &AttributeValue) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    match v {
        AttributeValue::N(n) => n.hash(&mut hasher),
        AttributeValue::S(s) => s.hash(&mut hasher),
        AttributeValue::B(b) => b.as_ref().hash(&mut hasher),
        _ => "unknown-partition".hash(&mut hasher),
    }
    hasher.finish()
}

fn coerce_key_attr(
    v: AttributeValue,
    attr_type: AttributeType,
    prefix: Option<&str>,
    suffix: Option<&str>,
) -> AttributeValue {
    let as_string = match v {
        AttributeValue::N(n) => n,
        AttributeValue::S(s) => s,
        AttributeValue::B(b) => String::from_utf8_lossy(b.as_ref()).to_string(),
        other => return other,
    };

    if attr_type.is_text_like() {
        let p = prefix.unwrap_or("");
        let s = suffix.unwrap_or("");
        AttributeValue::S(format!("{p}{as_string}{s}"))
    } else if attr_type.is_blob_like() {
        AttributeValue::B(Blob::new(as_string.into_bytes()))
    } else {
        AttributeValue::N(as_string)
    }
}

fn to_cql_value(v: &AttributeValue, attr_type: AttributeType) -> Result<CqlValue> {
    match attr_type {
        AttributeType::Text => match v {
            AttributeValue::S(s) => Ok(CqlValue::Text(s.clone())),
            AttributeValue::N(n) => Ok(CqlValue::Text(n.clone())),
            AttributeValue::B(b) => Ok(CqlValue::Text(
                String::from_utf8_lossy(b.as_ref()).to_string(),
            )),
            other => bail!("unsupported text value for CQL binding: {other:?}"),
        },
        AttributeType::Blob => match v {
            AttributeValue::B(b) => Ok(CqlValue::Blob(b.as_ref().to_vec())),
            AttributeValue::S(s) => Ok(CqlValue::Blob(s.as_bytes().to_vec())),
            AttributeValue::N(n) => Ok(CqlValue::Blob(n.as_bytes().to_vec())),
            other => bail!("unsupported blob value for CQL binding: {other:?}"),
        },
        AttributeType::BigInt => {
            let n = as_number_str(v)?;
            Ok(CqlValue::BigInt(n.parse::<i64>().with_context(|| {
                format!("failed to parse '{n}' as bigint")
            })?))
        }
        AttributeType::Int => {
            let n = as_number_str(v)?;
            Ok(CqlValue::Int(n.parse::<i32>().with_context(|| {
                format!("failed to parse '{n}' as int")
            })?))
        }
        AttributeType::SmallInt => {
            let n = as_number_str(v)?;
            Ok(CqlValue::SmallInt(n.parse::<i16>().with_context(|| {
                format!("failed to parse '{n}' as smallint")
            })?))
        }
        AttributeType::TinyInt => {
            let n = as_number_str(v)?;
            Ok(CqlValue::TinyInt(n.parse::<i8>().with_context(|| {
                format!("failed to parse '{n}' as tinyint")
            })?))
        }
        AttributeType::Float => {
            let n = as_number_str(v)?;
            Ok(CqlValue::Float(n.parse::<f32>().with_context(|| {
                format!("failed to parse '{n}' as float")
            })?))
        }
        AttributeType::Double => {
            let n = as_number_str(v)?;
            Ok(CqlValue::Double(n.parse::<f64>().with_context(|| {
                format!("failed to parse '{n}' as double")
            })?))
        }
        AttributeType::Boolean => match v {
            AttributeValue::N(n) => Ok(CqlValue::Boolean(n != "0")),
            AttributeValue::S(s) => {
                let b = s
                    .parse::<bool>()
                    .with_context(|| format!("failed to parse '{s}' as boolean"))?;
                Ok(CqlValue::Boolean(b))
            }
            other => bail!("unsupported boolean value for CQL binding: {other:?}"),
        },
    }
}

fn as_number_str(v: &AttributeValue) -> Result<&str> {
    match v {
        AttributeValue::N(n) => Ok(n.as_str()),
        AttributeValue::S(s) => Ok(s.as_str()),
        other => bail!("unsupported numeric value for CQL binding: {other:?}"),
    }
}

fn default_value_field(table_def: &TableDef) -> Option<String> {
    let pk_name = &table_def.key_schema.partition_key.name;
    let sk_name = table_def
        .key_schema
        .sort_key
        .as_ref()
        .map(|k| k.name.as_str());
    table_def
        .attributes
        .iter()
        .find(|a| {
            a.name != *pk_name && sk_name.map(|sk| a.name != sk).unwrap_or(true) && a.name == "val"
        })
        .or_else(|| {
            table_def
                .attributes
                .iter()
                .find(|a| a.name != *pk_name && sk_name.map(|sk| a.name != sk).unwrap_or(true))
        })
        .map(|a| a.name.clone())
}

fn build_sort_key_gen(
    tmpl: &OperationTemplate,
    op_name: &str,
    vars: &ResolvedVars,
    has_sort_key: bool,
) -> (Option<KeyGen>, Option<String>, Option<String>) {
    if !has_sort_key {
        return (None, None, None);
    }
    let op_name = canonical_op_name_for_generator(op_name);
    let base_cfg = match op_name {
        "put_item" | "update_item" => tmpl.item.as_ref(),
        "get_item" | "delete_item" => tmpl.key.as_ref(),
        _ => tmpl.key.as_ref().or(tmpl.item.as_ref()),
    };
    let sort_cfg = tmpl.sort_key.as_ref().or(base_cfg);
    let key_gen = sort_cfg.map(|cfg| build_key_gen(cfg, vars)).or_else(|| {
        let offset = vars.get("offset").copied().unwrap_or(0);
        Some(KeyGen::sequential(offset, 1))
    });
    let prefix = sort_cfg.and_then(|cfg| cfg.prefix.clone());
    let suffix = sort_cfg.and_then(|cfg| cfg.suffix.clone());
    (key_gen, prefix, suffix)
}

fn build_partition_key_gen(
    gen_cfg: Option<&crate::config::Generator>,
    vars: &ResolvedVars,
) -> (Option<KeyGen>, Option<CachedWorkingSet>) {
    if let Some(cfg) = gen_cfg {
        if cfg.strategy == GeneratorStrategy::CachedWorkingSet {
            let hit_ratio = vars.get("cache_hit_ratio").copied().unwrap_or(80) as f64 / 100.0;
            let row_count = vars.get("row_count").copied().unwrap_or(1_000_000);
            let offset = vars.get("offset").copied().unwrap_or(0);
            let hot_count = vars.get("hot_row_count").copied().unwrap_or(0);
            let capacity = if hot_count > 0 {
                hot_count as usize
            } else {
                (row_count as f64 * hit_ratio).ceil() as usize + 1
            };
            let fallback = KeyGen::uniform(offset, offset + row_count);
            return (
                None,
                Some(CachedWorkingSet::new(capacity, hit_ratio, fallback)),
            );
        }
        return (Some(build_key_gen(cfg, vars)), None);
    }

    // No template — fall back to sequential from offset.
    let offset = vars.get("offset").copied().unwrap_or(0);
    (Some(KeyGen::sequential(offset, 1)), None)
}

fn build_op_gen(
    tmpl: &OperationTemplate,
    table_def: &TableDef,
    op_name: &str,
    vars: &ResolvedVars,
) -> Result<OpGen> {
    let op_name = canonical_op_name_for_generator(op_name);
    // Determine the relevant generator config (key or item depending on op).
    let gen_cfg = match op_name {
        "put_item" | "update_item" | "batch_write_item" => tmpl
            .item
            .as_ref()
            .or_else(|| tmpl.items.as_ref().and_then(|v| v.first())),
        "get_item" | "delete_item" | "batch_get_item" => tmpl
            .key
            .as_ref()
            .or_else(|| tmpl.keys.as_ref().and_then(|v| v.first())),
        _ => tmpl.key.as_ref().or(tmpl.item.as_ref()),
    };

    let (partition_key_gen, partition_key_cws) = build_partition_key_gen(gen_cfg, vars);
    let partition_key_prefix = gen_cfg.and_then(|cfg| cfg.prefix.clone());
    let partition_key_suffix = gen_cfg.and_then(|cfg| cfg.suffix.clone());
    let (sort_key_gen, sort_key_prefix, sort_key_suffix) =
        build_sort_key_gen(tmpl, op_name, vars, table_def.key_schema.sort_key.is_some());

    // Value generator: only needed for put/update.
    let mut value_field = None;
    let value_gen = if matches!(op_name, "put_item" | "update_item" | "batch_write_item") {
        if let Some(cfg) = tmpl.item.as_ref() {
            let selected_field = cfg
                .value_field
                .clone()
                .or_else(|| default_value_field(table_def));
            if let Some(val_field) = selected_field {
                value_field = Some(val_field.clone());
                if let Some(attr) = table_def.attributes.iter().find(|a| a.name == val_field) {
                    Some(build_value_gen(cfg, attr.attr_type, attr.size_bytes))
                } else {
                    Some(build_value_gen(
                        cfg,
                        crate::config::AttributeType::Blob,
                        Some(64),
                    ))
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let batch_size = match op_name {
        "batch_get_item" => tmpl.keys.as_ref().map(|v| v.len()).unwrap_or(25),
        "batch_write_item" => tmpl.items.as_ref().map(|v| v.len()).unwrap_or(25),
        _ => 1,
    }
    .max(1)
    .min(100);

    let rows_per_partition_fixed = vars
        .get("rows_per_partition")
        .or_else(|| vars.get("sort_key_rows_per_partition"))
        .and_then(|v| u64::try_from(*v).ok())
        .filter(|v| *v > 0);

    let rows_per_partition_min = vars
        .get("rows_per_partition_min")
        .or_else(|| vars.get("sort_key_rows_per_partition_min"))
        .and_then(|v| u64::try_from(*v).ok())
        .filter(|v| *v > 0);

    let rows_per_partition_max = vars
        .get("rows_per_partition_max")
        .or_else(|| vars.get("sort_key_rows_per_partition_max"))
        .and_then(|v| u64::try_from(*v).ok())
        .filter(|v| *v > 0);

    let rows_per_partition_range = match (rows_per_partition_min, rows_per_partition_max) {
        (Some(min), Some(max)) => {
            if min <= max {
                Some((min, max))
            } else {
                Some((max, min))
            }
        }
        _ => None,
    };

    Ok(OpGen {
        partition_key_gen,
        partition_key_cws,
        partition_key_type: table_def.key_schema.partition_key.attr_type,
        partition_key_prefix,
        partition_key_suffix,
        sort_key_gen,
        sort_key_type: table_def.key_schema.sort_key.as_ref().map(|k| k.attr_type),
        sort_key_prefix,
        sort_key_suffix,
        sort_key_offset: vars
            .get("sort_key_offset")
            .copied()
            .unwrap_or_else(|| vars.get("offset").copied().unwrap_or(0)),
        rows_per_partition_fixed,
        rows_per_partition_range,
        value_gen,
        value_field,
        batch_size,
        cql_query: tmpl.cql_query.clone(),
        cql_params: tmpl.cql_params.clone(),
        retry: RetryPolicy::from_config(tmpl.retry.as_ref()),
        retry_unprocessed_keys: tmpl.retry_unprocessed_keys,
        retry_unprocessed_items: tmpl.retry_unprocessed_items,
    })
}

// ── Duration parser ───────────────────────────────────────────────────────────

/// Parse simple duration strings: `"0s"`, `"10s"`, `"2m"`, `"1h"`.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('s') {
        n.trim().parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(n) = s.strip_suffix('m') {
        n.trim()
            .parse::<u64>()
            .ok()
            .map(|n| Duration::from_secs(n * 60))
    } else if let Some(n) = s.strip_suffix('h') {
        n.trim()
            .parse::<u64>()
            .ok()
            .map(|n| Duration::from_secs(n * 3600))
    } else {
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_durations() {
        assert_eq!(parse_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
    }
}
