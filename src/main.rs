use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

use espresso::config::Profile;
use espresso::engine::{self, parse_duration};
use espresso::metrics::{LiveProgress, Metrics};
use espresso::{EspressoClient, build_client, parse_var_overrides, schema};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "espresso",
    version,
    about = "High-throughput ScyllaDB Alternator stress tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a workload profile.
    ///
    /// When --profile is omitted, espresso uses a built-in key/value profile
    /// targeting localhost:8000.  Use --nodes to point at a real cluster.
    Run {
        /// Path to a YAML workload profile.
        /// When omitted the built-in default key/value profile is used.
        #[arg(short, long)]
        profile: Option<String>,

        /// Target node URLs (overrides the profile's node list).
        /// May be repeated or comma-separated:
        ///   --nodes http://n1:8000 --nodes http://n2:8000
        ///   --nodes http://n1:8000,http://n2:8000
        #[arg(long, value_delimiter = ',')]
        nodes: Vec<String>,

        /// Number of rows to write during populate/write phases.
        /// Sets both `row_count` and `populate_operation_count` in vars.
        #[arg(long)]
        rows: Option<u64>,

        /// Override the duration of all duration-mode phases (e.g. `30s`, `5m`, `2h`).
        #[arg(long, value_name = "DURATION")]
        duration: Option<String>,

        /// Override a profile variable: --var row_count=500000
        #[arg(long, value_name = "KEY=VALUE")]
        var: Vec<String>,

        /// Override concurrency for all phases.
        #[arg(long)]
        concurrency: Option<u32>,

        /// Global target throughput in operations/second for each running phase.
        ///
        /// This is a phase-wide cap shared across all worker tasks.
        ///
        /// Set to `0` to disable throttling (run as fast as possible).
        #[arg(long)]
        throttle: Option<u32>,

        /// Optional sinusoidal variation amplitude for --throttle.
        ///
        /// Expressed as a fraction in [0.0, 1.0], where 0.2 means +/-20%
        /// variation around the base throttle.
        #[arg(long)]
        throttle_sine_amplitude: Option<f64>,

        /// Period for sinusoidal variation of --throttle (e.g. `60s`, `5m`).
        #[arg(long, value_name = "DURATION")]
        throttle_sine_period: Option<String>,

        /// Adaptive mode target for p99 latency (milliseconds).
        ///
        /// When set, espresso automatically adjusts throttle at runtime to
        /// converge toward this latency target.
        #[arg(long)]
        target_p99_ms: Option<f64>,

        /// Run only a specific phase by name.
        #[arg(long)]
        phase: Option<String>,
    },

    /// Apply schema (create/delete tables) without running any workload.
    Schema {
        /// Path to a YAML workload profile.
        /// When omitted the built-in default key/value profile is used.
        #[arg(short, long)]
        profile: Option<String>,

        /// Target node URLs (overrides the profile's node list).
        #[arg(long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "espresso=info,warn".to_string()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            profile: profile_path,
            nodes,
            rows,
            duration: duration_override,
            var,
            concurrency,
            throttle,
            throttle_sine_amplitude,
            throttle_sine_period,
            target_p99_ms,
            phase: phase_filter,
        } => {
            if cfg!(debug_assertions) {
                warn!(
                    "Running a debug build; latency/throughput numbers are not representative. Use `cargo run --release -- ...` for benchmarking."
                );
            }

            let mut profile = match profile_path {
                Some(ref path) => {
                    Profile::load(path).with_context(|| format!("loading profile '{path}'"))?
                }
                None => {
                    info!("No --profile supplied; using built-in default key/value profile");
                    Profile::default_profile(&nodes)?
                }
            };
            // --nodes overrides whatever nodes are in the loaded profile.
            if !nodes.is_empty() {
                profile.target.nodes = nodes.clone();
            }

            // --rows sets row_count and populate_operation_count together.
            if let Some(n) = rows {
                profile.vars.insert(
                    "row_count".to_string(),
                    serde_yaml::Value::Number(serde_yaml::Number::from(n)),
                );
                profile.vars.insert(
                    "populate_operation_count".to_string(),
                    serde_yaml::Value::Number(serde_yaml::Number::from(n)),
                );
                info!("--rows {n}: set row_count and populate_operation_count to {n}");
            }

            // --duration overrides phase.duration for all duration-mode phases.
            if let Some(ref dur_str) = duration_override {
                if parse_duration(dur_str).is_none() {
                    bail!(
                        "--duration '{}' is not a valid duration (examples: 30s, 5m, 2h)",
                        dur_str
                    );
                }
                for phase in &mut profile.phases {
                    if phase.mode == espresso::config::PhaseMode::Duration {
                        phase.duration = Some(dur_str.clone());
                    }
                }
                info!("--duration {dur_str}: applied to all duration-mode phases");
            }

            let cli_vars = parse_var_overrides(&var)?;
            let client = Arc::new(build_client(&profile).await?);
            let throttle = throttle.filter(|v| *v > 0);
            if let Some(amplitude) = throttle_sine_amplitude {
                if !(0.0..=1.0).contains(&amplitude) {
                    bail!("--throttle-sine-amplitude must be in [0.0, 1.0]")
                }
                if throttle.is_none() {
                    bail!("--throttle-sine-amplitude requires --throttle")
                }
            }

            let throttle_sine_period = if let Some(ref period) = throttle_sine_period {
                let parsed = parse_duration(period).with_context(|| {
                    format!(
                        "--throttle-sine-period '{}' is not a valid duration (examples: 30s, 5m, 2h)",
                        period
                    )
                })?;
                if parsed.is_zero() {
                    bail!("--throttle-sine-period must be > 0s")
                }
                if throttle.is_none() {
                    bail!("--throttle-sine-period requires --throttle")
                }
                Some(parsed)
            } else {
                None
            };

            if let Some(target) = target_p99_ms {
                if target <= 0.0 {
                    bail!("--target-p99-ms must be > 0")
                }
            }

            match client.as_ref() {
                EspressoClient::Alternator(c) => {
                    schema::apply(c, &profile.tables, &profile.schema).await?
                }
                EspressoClient::Cql(s) => {
                    schema::apply_cql(
                        s,
                        &profile.target.cql.keyspace,
                        &profile.tables,
                        &profile.schema,
                    )
                    .await?
                }
            }

            for phase in &profile.phases {
                if let Some(ref filter) = phase_filter {
                    if &phase.name != filter {
                        continue;
                    }
                }

                let mut phase = phase.clone();
                if let Some(c) = concurrency {
                    phase.concurrency = c;
                }

                info!(
                    "Starting phase '{}' (mode={:?}, concurrency={}, throttle={:?} ops/s, target_p99_ms={:?})",
                    phase.name, phase.mode, phase.concurrency, throttle, target_p99_ms
                );

                // Create one Metrics instance; share it between LiveProgress
                // (observer) and run_phase (writer) via cheap Arc-clone.
                let metrics = Metrics::new();

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
                    client.clone(),
                    &phase,
                    &profile.tables,
                    &profile.vars,
                    &cli_vars,
                    metrics.clone(),
                    throttle,
                    target_p99_ms,
                    throttle_sine_amplitude,
                    throttle_sine_period,
                    Some(profile.target.cql.keyspace.clone()),
                )
                .await?;

                drop(_live); // flush final \r live-progress line
                metrics.print_summary(&phase.name);
            }
        }

        Commands::Schema {
            profile: profile_path,
            nodes,
        } => {
            let mut profile = match profile_path {
                Some(ref path) => {
                    Profile::load(path).with_context(|| format!("loading profile '{path}'"))?
                }
                None => {
                    info!("No --profile supplied; using built-in default key/value profile");
                    Profile::default_profile(&nodes)?
                }
            };
            if !nodes.is_empty() {
                profile.target.nodes = nodes;
            }
            let client = build_client(&profile).await?;
            match &client {
                EspressoClient::Alternator(c) => {
                    schema::apply(c, &profile.tables, &profile.schema).await?
                }
                EspressoClient::Cql(s) => {
                    schema::apply_cql(
                        s,
                        &profile.target.cql.keyspace,
                        &profile.tables,
                        &profile.schema,
                    )
                    .await?
                }
            }
            info!("Schema applied successfully");
        }
    }

    Ok(())
}
