//! Metrics collection and reporting.
//!
//! [`Metrics`] is a shared, clone-able struct that worker tasks write into
//! during a phase.  After the phase completes, [`Metrics::print_summary`]
//! emits a text table of latency percentiles and throughput.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

const REPORT_SEPARATOR_WIDTH: usize = 126;

// ── Metrics ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Metrics(Arc<MetricsInner>);

#[derive(Debug)]
struct MetricsInner {
    start: Instant,
    ops: Mutex<HashMap<String, OpStats>>,
}

#[derive(Debug, Default)]
struct OpStats {
    /// Latency histogram in microseconds.
    hist: Option<Histogram<u64>>,
    errors: u64,
}

#[derive(Debug, Clone)]
struct OpRow {
    name: String,
    count: u64,
    errors: u64,
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    p999_ms: Option<f64>,
    max_ms: Option<f64>,
    ops_per_sec: f64,
}

#[derive(Debug, Clone)]
struct TotalRow {
    count: u64,
    throughput: f64,
    errors: u64,
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    p999_ms: Option<f64>,
    max_ms: Option<f64>,
}

impl OpStats {
    fn hist_mut(&mut self) -> &mut Histogram<u64> {
        self.hist.get_or_insert_with(|| {
            Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid HDR histogram bounds")
        })
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self(Arc::new(MetricsInner {
            start: Instant::now(),
            ops: Mutex::new(HashMap::new()),
        }))
    }

    /// Record a successful operation with its measured latency.
    pub fn record_ok(&self, op: &str, latency: Duration) {
        let us = latency.as_micros().min(u64::MAX as u128) as u64;
        let mut map = self.0.ops.lock().unwrap();
        let stats = map.entry(op.to_string()).or_default();
        // Saturate at max recordable value rather than panicking.
        let _ = stats.hist_mut().record(us.max(1));
    }

    /// Record a failed operation (counted separately from latency).
    pub fn record_err(&self, op: &str) {
        let mut map = self.0.ops.lock().unwrap();
        map.entry(op.to_string()).or_default().errors += 1;
    }

    /// Wall-clock duration since this `Metrics` was created.
    pub fn elapsed(&self) -> Duration {
        self.0.start.elapsed()
    }

    /// Aggregate p99 latency across all recorded operations in milliseconds.
    pub fn overall_p99_ms(&self) -> Option<f64> {
        let map = self.0.ops.lock().unwrap();
        let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).ok()?;
        let mut has_samples = false;

        for stats in map.values() {
            if let Some(h) = &stats.hist {
                if h.len() > 0 {
                    let _ = merged.add(h);
                    has_samples = true;
                }
            }
        }

        if has_samples {
            Some(us_to_ms(merged.value_at_quantile(0.99)))
        } else {
            None
        }
    }

    /// Print a human-readable summary to stdout.
    pub fn print_summary(&self, phase_name: &str) {
        self.print_snapshot("Phase", phase_name, self.elapsed(), false);
    }

    /// Print a latte-style live table snapshot.
    pub fn print_live_snapshot(&self, phase_name: &str) {
        self.print_snapshot("Live", phase_name, self.elapsed(), true);
    }

    fn print_snapshot(
        &self,
        prefix: &str,
        phase_name: &str,
        elapsed: Duration,
        include_blank_line_before: bool,
    ) {
        let secs = elapsed.as_secs_f64();
        let rows = self.snapshot_rows(secs);

        if include_blank_line_before {
            eprintln!();
        }
        eprintln!("── {prefix}: {phase_name} ─────────────────────────────────────────");
        eprintln!("  Elapsed: {:.1}s", secs);

        if rows.is_empty() {
            eprintln!("  (no operations recorded)");
            return;
        }

        eprintln!(
            "\n  {:<20} {:>14} {:>14} {:>10} {:>11} {:>11} {:>11} {:>11} {:>11}",
            "operation",
            "count",
            "throughput",
            "errors",
            "p50 ms",
            "p95 ms",
            "p99 ms",
            "p999 ms",
            "max ms"
        );
        eprintln!("  {}", "─".repeat(REPORT_SEPARATOR_WIDTH));

        let total_ops: u64 = rows.iter().map(|r| r.count).sum();
        let total_errors: u64 = rows.iter().map(|r| r.errors).sum();

        for row in &rows {
            eprintln!(
                "  {:<20} {:>14} {:>14} {:>10} {:>11} {:>11} {:>11} {:>11} {:>11}",
                row.name,
                row.count,
                format!("{:.0}", row.ops_per_sec),
                row.errors,
                fmt_opt(row.p50_ms),
                fmt_opt(row.p95_ms),
                fmt_opt(row.p99_ms),
                fmt_opt(row.p999_ms),
                fmt_opt(row.max_ms),
            );
        }

        if rows.len() > 1 {
            eprintln!("  {}", "─".repeat(REPORT_SEPARATOR_WIDTH));
            let total_ops_per_sec = if secs > 0.0 {
                total_ops as f64 / secs
            } else {
                0.0
            };
            eprintln!(
                "  {:<20} {:>14} {:>14} {:>10} {:>59}",
                "TOTAL",
                total_ops,
                format!("{total_ops_per_sec:.0}"),
                total_errors,
                ""
            );
        }
    }

    fn snapshot_rows(&self, secs: f64) -> Vec<OpRow> {
        let map = self.0.ops.lock().unwrap();
        let mut names: Vec<&String> = map.keys().collect();
        names.sort();

        names
            .into_iter()
            .map(|name| {
                let stats = &map[name];
                if let Some(h) = &stats.hist {
                    let count = h.len();
                    OpRow {
                        name: name.clone(),
                        count,
                        errors: stats.errors,
                        p50_ms: Some(us_to_ms(h.value_at_quantile(0.50))),
                        p95_ms: Some(us_to_ms(h.value_at_quantile(0.95))),
                        p99_ms: Some(us_to_ms(h.value_at_quantile(0.99))),
                        p999_ms: Some(us_to_ms(h.value_at_quantile(0.999))),
                        max_ms: Some(us_to_ms(h.max())),
                        ops_per_sec: if secs > 0.0 { count as f64 / secs } else { 0.0 },
                    }
                } else {
                    OpRow {
                        name: name.clone(),
                        count: 0,
                        errors: stats.errors,
                        p50_ms: None,
                        p95_ms: None,
                        p99_ms: None,
                        p999_ms: None,
                        max_ms: None,
                        ops_per_sec: 0.0,
                    }
                }
            })
            .collect()
    }

    fn snapshot_total_row(&self, secs: f64) -> TotalRow {
        let map = self.0.ops.lock().unwrap();

        let mut total_count = 0u64;
        let mut total_errors = 0u64;
        let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).ok();

        for stats in map.values() {
            total_errors += stats.errors;
            if let Some(h) = &stats.hist {
                total_count += h.len();
                if let Some(m) = &mut merged {
                    let _ = m.add(h);
                }
            }
        }

        let throughput = if secs > 0.0 {
            total_count as f64 / secs
        } else {
            0.0
        };

        if let Some(m) = merged {
            if m.len() > 0 {
                return TotalRow {
                    count: total_count,
                    throughput,
                    errors: total_errors,
                    p50_ms: Some(us_to_ms(m.value_at_quantile(0.50))),
                    p95_ms: Some(us_to_ms(m.value_at_quantile(0.95))),
                    p99_ms: Some(us_to_ms(m.value_at_quantile(0.99))),
                    p999_ms: Some(us_to_ms(m.value_at_quantile(0.999))),
                    max_ms: Some(us_to_ms(m.max())),
                };
            }
        }

        TotalRow {
            count: total_count,
            throughput,
            errors: total_errors,
            p50_ms: None,
            p95_ms: None,
            p99_ms: None,
            p999_ms: None,
            max_ms: None,
        }
    }
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

// ── Live progress printer ─────────────────────────────────────────────────────

/// Spawns a background task that prints a single live progress line every
/// `interval`.  The task stops when the returned handle is dropped.
pub struct LiveProgress {
    _handle: tokio::task::JoinHandle<()>,
}

impl LiveProgress {
    pub fn start(metrics: Metrics, phase_name: String, interval: Duration) -> Self {
        let handle = tokio::spawn(async move {
            let mut printed_header = false;
            loop {
                tokio::time::sleep(interval).await;

                let elapsed = metrics.elapsed();
                let rows = metrics.snapshot_rows(elapsed.as_secs_f64());
                if rows.is_empty() {
                    continue;
                }
                let total = metrics.snapshot_total_row(elapsed.as_secs_f64());

                if !printed_header {
                    eprintln!();
                    eprintln!("── Live: {phase_name} ─────────────────────────────────────────");
                    eprintln!("  Elapsed: {:.1}s", elapsed.as_secs_f64());
                    eprintln!(
                        "\n  {:<20} {:>14} {:>14} {:>10} {:>11} {:>11} {:>11} {:>11} {:>11}",
                        "operation",
                        "count",
                        "throughput",
                        "errors",
                        "p50 ms",
                        "p95 ms",
                        "p99 ms",
                        "p999 ms",
                        "max ms"
                    );
                    eprintln!("  {}", "─".repeat(REPORT_SEPARATOR_WIDTH));
                    printed_header = true;
                }

                for row in &rows {
                    eprintln!(
                        "  {:<20} {:>14} {:>14} {:>10} {:>11} {:>11} {:>11} {:>11} {:>11}",
                        row.name,
                        row.count,
                        format!("{:.0}", row.ops_per_sec),
                        row.errors,
                        fmt_opt(row.p50_ms),
                        fmt_opt(row.p95_ms),
                        fmt_opt(row.p99_ms),
                        fmt_opt(row.p999_ms),
                        fmt_opt(row.max_ms),
                    );
                }

                if rows.len() > 1 {
                    eprintln!(
                        "  {:<20} {:>14} {:>14} {:>10} {:>11} {:>11} {:>11} {:>11} {:>11}",
                        "TOTAL",
                        total.count,
                        format!("{:.0}", total.throughput),
                        total.errors,
                        fmt_opt(total.p50_ms),
                        fmt_opt(total.p95_ms),
                        fmt_opt(total.p99_ms),
                        fmt_opt(total.p999_ms),
                        fmt_opt(total.max_ms),
                    );
                }
            }
        });
        Self { _handle: handle }
    }
}

impl Drop for LiveProgress {
    fn drop(&mut self) {
        // The JoinHandle is aborted when dropped.
        self._handle.abort();
    }
}

fn fmt_opt(v: Option<f64>) -> String {
    match v {
        Some(n) => format!("{n:.2}"),
        None => "-".to_string(),
    }
}
