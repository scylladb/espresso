//! Data generators — produce `AttributeValue`s for DynamoDB operations.
//!
//! Each generator is cheaply clonable (wraps an `Arc`) so it can be shared
//! across worker tasks without contention on a central state.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

// ── Key generator ─────────────────────────────────────────────────────────────

/// Produces integer keys (DynamoDB `N` type).
///
/// Clone is cheap — all clones share the same underlying state.
#[derive(Clone, Debug)]
pub struct KeyGen(Arc<KeyGenInner>);

#[derive(Debug)]
enum KeyGenInner {
    Sequential {
        counter: AtomicI64,
        step: i64,
    },
    SequentialRange {
        state: Mutex<i64>,
        min: i64,
        max: i64,
    },
    Uniform {
        rng: Mutex<SmallRng>,
        min: i64,
        max: i64,
    },
    Zipf {
        rng: Mutex<SmallRng>,
        n: i64,
        theta: f64,
        offset: i64,
    },
    Fixed(i64),
}

impl KeyGen {
    /// Monotonically increasing integers: `start, start+step, start+2*step, …`
    pub fn sequential(start: i64, step: i64) -> Self {
        Self(Arc::new(KeyGenInner::Sequential {
            counter: AtomicI64::new(start),
            step,
        }))
    }

    /// Monotonic sequence constrained to inclusive range `[min, max]`.
    /// Values wrap to `min` after reaching `max`.
    pub fn sequential_range(min: i64, max: i64) -> Self {
        Self(Arc::new(KeyGenInner::SequentialRange {
            state: Mutex::new(min),
            min,
            max,
        }))
    }

    /// Uniform random integer in `[min, max)`.
    pub fn uniform(min: i64, max: i64) -> Self {
        Self(Arc::new(KeyGenInner::Uniform {
            rng: Mutex::new(SmallRng::from_entropy()),
            min,
            max,
        }))
    }

    /// Power-law (Zipf-like) sample in `[0, n)` with skew parameter `theta`.
    ///
    /// `theta` ∈ (0, 1): higher value → heavier skew toward low-numbered keys.
    /// Uses the inverse-CDF approximation: k = floor(n · u^(1/(1−θ))).
    pub fn zipf(n: i64, theta: f64, offset: i64) -> Self {
        Self(Arc::new(KeyGenInner::Zipf {
            rng: Mutex::new(SmallRng::from_entropy()),
            n,
            theta,
            offset,
        }))
    }

    /// Always returns the same value.
    pub fn fixed(value: i64) -> Self {
        Self(Arc::new(KeyGenInner::Fixed(value)))
    }

    /// Generate the next key as a raw `i64`.
    pub fn next_i64(&self) -> i64 {
        match self.0.as_ref() {
            KeyGenInner::Sequential { counter, step } => {
                counter.fetch_add(*step, Ordering::Relaxed)
            }
            KeyGenInner::SequentialRange { state, min, max } => {
                let mut cur = state.lock().unwrap();
                let out = *cur;
                *cur = if *cur >= *max { *min } else { *cur + 1 };
                out
            }
            KeyGenInner::Uniform { rng, min, max } => rng.lock().unwrap().gen_range(*min..*max),
            KeyGenInner::Zipf {
                rng,
                n,
                theta,
                offset,
            } => zipf_sample(*n, *theta, &mut *rng.lock().unwrap()) + offset,
            KeyGenInner::Fixed(v) => *v,
        }
    }

    /// Generate the next key as a DynamoDB `AttributeValue::N`.
    pub fn next(&self) -> AttributeValue {
        AttributeValue::N(self.next_i64().to_string())
    }
}

// ── Value generator ───────────────────────────────────────────────────────────

/// Produces arbitrary values for DynamoDB attributes.
///
/// Clone is cheap — all clones share the same underlying state.
#[derive(Clone, Debug)]
pub struct ValueGen {
    inner: Arc<ValueGenInner>,
    prefix: Option<String>,
    suffix: Option<String>,
}

#[derive(Debug)]
enum ValueGenInner {
    /// Fixed-length random bytes (`B` type).
    RandomBytes { rng: Mutex<SmallRng>, size: usize },
    /// Fixed literal string (`S` type).
    FixedString(String),
    /// Fixed literal number (`N` type).
    FixedNumber(String),
}

impl ValueGen {
    /// `size_bytes` random bytes, regenerated on every call.
    pub fn random_bytes(size: usize) -> Self {
        Self {
            inner: Arc::new(ValueGenInner::RandomBytes {
                rng: Mutex::new(SmallRng::from_entropy()),
                size,
            }),
            prefix: None,
            suffix: None,
        }
    }

    /// Always emit the same string.
    pub fn fixed_string(s: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ValueGenInner::FixedString(s.into())),
            prefix: None,
            suffix: None,
        }
    }

    /// Always emit the same number.
    pub fn fixed_number(n: i64) -> Self {
        Self {
            inner: Arc::new(ValueGenInner::FixedNumber(n.to_string())),
            prefix: None,
            suffix: None,
        }
    }

    /// Decorate string values with optional prefix/suffix.
    pub fn with_affixes(mut self, prefix: Option<String>, suffix: Option<String>) -> Self {
        self.prefix = prefix;
        self.suffix = suffix;
        self
    }

    /// Generate the next value as a DynamoDB `AttributeValue`.
    pub fn next(&self) -> AttributeValue {
        let out = match self.inner.as_ref() {
            ValueGenInner::RandomBytes { rng, size } => {
                let mut buf = vec![0u8; *size];
                rng.lock().unwrap().fill(buf.as_mut_slice());
                AttributeValue::B(Blob::new(buf))
            }
            ValueGenInner::FixedString(s) => AttributeValue::S(s.clone()),
            ValueGenInner::FixedNumber(n) => AttributeValue::N(n.clone()),
        };

        match out {
            AttributeValue::S(s) => {
                let p = self.prefix.as_deref().unwrap_or("");
                let suf = self.suffix.as_deref().unwrap_or("");
                AttributeValue::S(format!("{p}{s}{suf}"))
            }
            other => other,
        }
    }
}

// ── Cached working set ────────────────────────────────────────────────────────

/// Maintains a bounded hot set of keys and serves reads from it.
///
/// On a cache hit (probability ≈ `hit_ratio`) a key from the working set is
/// returned.  On a miss the fallback generator is used and the result is
/// inserted into the working set (evicting a random entry).
#[derive(Clone, Debug)]
pub struct CachedWorkingSet(Arc<CwsInner>);

#[derive(Debug)]
struct CwsInner {
    capacity: usize,
    hit_ratio: f64,
    hot_keys: Mutex<Vec<i64>>,
    rng: Mutex<SmallRng>,
    fallback: KeyGen,
}

impl CachedWorkingSet {
    /// `capacity`: max entries in the hot set.
    /// `hit_ratio`: probability 0.0–1.0 of serving from the hot set.
    /// `fallback`: used when the working set is cold or on a miss.
    pub fn new(capacity: usize, hit_ratio: f64, fallback: KeyGen) -> Self {
        Self(Arc::new(CwsInner {
            capacity,
            hit_ratio,
            hot_keys: Mutex::new(Vec::with_capacity(capacity)),
            rng: Mutex::new(SmallRng::from_entropy()),
            fallback,
        }))
    }

    pub fn next(&self) -> AttributeValue {
        let i = &*self.0;
        let p: f64 = i.rng.lock().unwrap().gen_range(0.0f64..1.0f64);
        let hot_len = i.hot_keys.lock().unwrap().len();

        if hot_len > 0 && p < i.hit_ratio {
            // Cache hit — pick a random entry from the working set.
            let idx = i.rng.lock().unwrap().gen_range(0..hot_len);
            let key = i.hot_keys.lock().unwrap()[idx];
            AttributeValue::N(key.to_string())
        } else {
            // Cache miss — generate a new key and insert into the working set.
            let key = i.fallback.next_i64();
            let mut hot = i.hot_keys.lock().unwrap();
            if hot.len() >= i.capacity {
                let idx = i.rng.lock().unwrap().gen_range(0..hot.len());
                hot[idx] = key;
            } else {
                hot.push(key);
            }
            AttributeValue::N(key.to_string())
        }
    }
}

// ── Factory: build generators from config ────────────────────────────────────

use crate::config::{AttributeType, Generator as GenConfig, GeneratorStrategy};
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;

/// Resolved, numeric variables (after `${vars.x}` substitution).
pub type ResolvedVars = HashMap<String, i64>;

/// Build a [`KeyGen`] from a generator config entry and resolved vars.
pub fn build_key_gen(cfg: &GenConfig, vars: &ResolvedVars) -> KeyGen {
    let partition_count = vars.get("partition_count").copied().unwrap_or(1_000_000);
    let offset = vars.get("offset").copied().unwrap_or(0);

    let range_bounds = cfg.range.map(|(a, b)| if a <= b { (a, b) } else { (b, a) });

    match cfg.strategy {
        GeneratorStrategy::Sequential => {
            if let Some((min, max)) = range_bounds {
                KeyGen::sequential_range(min, max)
            } else {
                let start = cfg.start.unwrap_or(offset);
                let step = cfg.step.unwrap_or(1);
                KeyGen::sequential(start, step)
            }
        }
        GeneratorStrategy::Uniform | GeneratorStrategy::GeneratedFromVars => {
            if let Some((min, max)) = range_bounds {
                KeyGen::uniform(min, max + 1)
            } else {
                KeyGen::uniform(offset, offset + partition_count)
            }
        }
        GeneratorStrategy::Zipf => {
            let theta = cfg.theta.unwrap_or(0.99);
            if let Some((min, max)) = range_bounds {
                let n = (max - min + 1).max(1);
                KeyGen::zipf(n, theta, min)
            } else {
                KeyGen::zipf(partition_count, theta, offset)
            }
        }
        GeneratorStrategy::Fixed => {
            let v = cfg.value.as_ref().and_then(yaml_as_i64).unwrap_or(0);
            KeyGen::fixed(v)
        }
        GeneratorStrategy::CachedWorkingSet => {
            // Callers that need exact hit-ratio semantics should use
            // `CachedWorkingSet::new` directly. Here we return a uniform
            // fallback so the factory always yields a `KeyGen`.
            KeyGen::uniform(offset, offset + partition_count)
        }
    }
}

/// Build a [`ValueGen`] from a generator config entry and the attribute's
/// `size_bytes` hint (from the table definition).
pub fn build_value_gen(
    cfg: &GenConfig,
    attr_type: AttributeType,
    size_bytes: Option<usize>,
) -> ValueGen {
    let vg = match cfg.strategy {
        GeneratorStrategy::Fixed => {
            if let Some(v) = &cfg.value {
                if attr_type.is_text_like() {
                    ValueGen::fixed_string(v.as_str().unwrap_or("").to_owned())
                } else {
                    ValueGen::fixed_number(yaml_as_i64(v).unwrap_or(0))
                }
            } else {
                ValueGen::fixed_string(String::new())
            }
        }
        _ => {
            if attr_type.is_blob_like() {
                ValueGen::random_bytes(size_bytes.unwrap_or(64))
            } else if attr_type.is_text_like() {
                ValueGen::fixed_string("x".repeat(size_bytes.unwrap_or(16)))
            } else {
                ValueGen::fixed_number(0)
            }
        }
    };

    // Value field decorations apply only to string attributes.
    if attr_type.is_text_like() {
        vg.with_affixes(cfg.prefix.clone(), cfg.suffix.clone())
    } else {
        vg
    }
}

// ── Resolve vars ──────────────────────────────────────────────────────────────

/// Convert top-level `vars` from the profile (arbitrary YAML) to `i64` values,
/// silently skipping non-numeric entries.
pub fn resolve_vars(
    base: &HashMap<String, YamlValue>,
    overrides: &HashMap<String, YamlValue>,
) -> ResolvedVars {
    let mut out = HashMap::new();
    for (k, v) in base.iter().chain(overrides.iter()) {
        if let Some(n) = yaml_as_i64(v) {
            out.insert(k.clone(), n);
        }
    }
    out
}

// ── Zipf approximation ────────────────────────────────────────────────────────

/// Sample from a Zipf-like power-law distribution over [0, n) with skew θ.
///
/// Uses the continuous inverse-CDF approximation:
///   k = floor(n · u^(1/(1−θ)))
/// where u ~ Uniform(0, 1).  Accurate for large n; gives heavy skew toward
/// small keys as θ → 1.
fn zipf_sample(n: i64, theta: f64, rng: &mut SmallRng) -> i64 {
    let u: f64 = rng.gen_range(0.0f64..1.0f64);
    let exponent = 1.0 / (1.0 - theta);
    let k = (n as f64 * u.powf(exponent)).floor() as i64;
    k.clamp(0, n - 1)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn yaml_as_i64(v: &YamlValue) -> Option<i64> {
    match v {
        YamlValue::Number(n) => n.as_i64(),
        YamlValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_increments() {
        let g = KeyGen::sequential(0, 1);
        assert_eq!(g.next_i64(), 0);
        assert_eq!(g.next_i64(), 1);
        assert_eq!(g.next_i64(), 2);
    }

    #[test]
    fn sequential_clone_shares_state() {
        let g = KeyGen::sequential(0, 1);
        let g2 = g.clone();
        assert_eq!(g.next_i64(), 0);
        assert_eq!(g2.next_i64(), 1); // shared counter
    }

    #[test]
    fn uniform_in_range() {
        let g = KeyGen::uniform(100, 200);
        for _ in 0..100 {
            let v = g.next_i64();
            assert!((100..200).contains(&v));
        }
    }

    #[test]
    fn zipf_in_range() {
        let g = KeyGen::zipf(1000, 0.99, 0);
        for _ in 0..200 {
            let v = g.next_i64();
            assert!((0..1000).contains(&v), "out of range: {v}");
        }
    }

    #[test]
    fn random_bytes_correct_length() {
        let g = ValueGen::random_bytes(128);
        if let AttributeValue::B(b) = g.next() {
            assert_eq!(b.as_ref().len(), 128);
        } else {
            panic!("expected B variant");
        }
    }

    #[test]
    fn cws_stays_in_range() {
        let fallback = KeyGen::uniform(0, 1000);
        let cws = CachedWorkingSet::new(50, 0.8, fallback);
        for _ in 0..200 {
            if let AttributeValue::N(s) = cws.next() {
                let v: i64 = s.parse().unwrap();
                assert!((0..1000).contains(&v));
            } else {
                panic!("expected N variant");
            }
        }
    }
}
