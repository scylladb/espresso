# CQL Backend Guide

This guide explains how to run espresso against native Scylla CQL and how to tune it for high-throughput workloads.

## What CQL Backend Uses

Espresso CQL backend uses the Scylla Rust driver with:
- Per-shard connection pools (`connections_per_shard`)
- Token-aware routing
- Optional local datacenter preference (`local_dc`)
- Configurable keyspace replication factor and tablet settings
- Prepared statement caching per dispatcher
- Shared runtime engine (same phases/throttle/metrics model as Alternator)

## Minimal CQL Profile

```yaml
version: 1

target:
  kind: cql
  nodes:
    - 127.0.0.1:9042
  cql:
    keyspace: espresso
    connections_per_shard: 1

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

tables:
  - name: key_value_cql
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: key
        type: bigint
    attributes:
      - name: key
        type: bigint
      - name: val
        type: blob
        size_bytes: 512

phases:
  - name: run_read_heavy
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: key_value_cql
        mix:
          get_item: 0.9
          put_item: 0.1
        operation_templates:
          get_item:
            key:
              strategy: cached_working_set
              key_field: key
          put_item:
            item:
              strategy: generated_from_vars
              key_field: key
              value_field: val
```

Reference sample profile:
- [espresso_profiles/cql_key_value.yaml](../espresso_profiles/cql_key_value.yaml)

Type notes:
- For CQL profiles, prefer native Scylla/CQL types in schema definitions (`bigint`, `text`, `blob`, `int`, `smallint`, `tinyint`, `float`, `double`, `boolean`).
- Legacy shorthand (`N`, `S`, `B`) is still accepted for compatibility.

## Run Commands

```bash
# Run all phases
cargo run --release -- run --profile espresso_profiles/cql_key_value.yaml

# Override nodes
cargo run --release -- run --profile espresso_profiles/cql_key_value.yaml \
  --nodes 10.0.0.11:9042,10.0.0.12:9042,10.0.0.13:9042

# Run one phase only
cargo run --release -- run --profile espresso_profiles/cql_key_value.yaml --phase run_read_heavy

# Apply schema only
cargo run --release -- schema --profile espresso_profiles/cql_key_value.yaml
```

## Throughput and Latency Controls

These apply equally to CQL and Alternator:
- `--throttle`: phase-wide cap in ops/sec
- `--throttle-sine-amplitude`: sinusoidal variation around base rate
- `--throttle-sine-period`: period of sine variation
- `--target-p99-ms`: adaptive throttle aiming at p99 latency target

For CQL specifically:
- `target.cql.connections_per_shard` controls driver connections opened per shard.
- `target.cql.replication_factor` sets the replication factor for the keyspace espresso creates (default `1`).
- `target.cql.tablets_enabled` / `target.cql.tablets_initial` control tablet settings for the keyspace (omitted from DDL if not set).
- Total in-flight requests ≈ `phase.concurrency` (same knob as Alternator).

Example:

```bash
cargo run --release -- run --profile espresso_profiles/cql_key_value.yaml \
  --throttle 15000 --throttle-sine-amplitude 0.25 --throttle-sine-period 60s
```

## Retry Configuration (Latte-Style)

Retry is configured per operation template.

Supported fields:
- `retry.max_attempts` (default `10`)
- `retry.backoff`: `fixed`, `exponential`, `exponential_jitter` (default)
- `retry.base_delay_ms` (default `100`)
- `retry.max_delay_ms` (default `5000`)

Batch-specific flags:
- `retry_unprocessed_keys` for `batch_get_item`
- `retry_unprocessed_items` for `batch_write_item`

Example:

```yaml
operation_templates:
  batch_get_item:
    retry_unprocessed_keys: true
    retry:
      max_attempts: 8
      backoff: exponential_jitter
      base_delay_ms: 100
      max_delay_ms: 5000
```

## Cassandra-Stress-Style Named Queries

For CQL profiles, you can define operation names directly in `mix` and bind each name to a raw CQL statement.

Example:

```yaml
phases:
  - name: run_named_queries
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: key_value_cql
        mix:
          read_by_pk: 0.85
          write_by_pk: 0.15
        operation_templates:
          read_by_pk:
            cql_query: "SELECT key, val FROM {table} WHERE key = ?"
            cql_params: [key]
            key:
              strategy: cached_working_set
              key_field: key
          write_by_pk:
            cql_query: "INSERT INTO {table} (key, val) VALUES (?, ?)"
            cql_params: [key, val]
            item:
              strategy: generated_from_vars
              key_field: key
              value_field: val
```

Notes:
- `cql_query` supports positional `?` bind markers.
- `cql_params` order must match the `?` order.
- Parameter names refer to generated attributes (for example `key`, `val`).
- `{table}` is optional and expands to `<keyspace>.<table>`.

## Notes and Limits

- CQL backend currently supports DynamoDB-equivalent operations:
  - `put_item`, `get_item`, `delete_item`, `update_item`, `batch_get_item`, `batch_write_item`
- CQL backend also accepts SQL-style aliases for readability:
  - `insert` -> `put_item`
  - `select` -> `get_item`
  - `update` -> `update_item`
  - `delete` -> `delete_item`
  - `batch_insert` -> `batch_write_item`
  - `batch_select` -> `batch_get_item`
- `query` and `scan` operation templates are documented for Alternator but are not currently implemented in CQL dispatch path.
- Use `cargo run --release` for benchmark-quality numbers.

## ScyllaDB Cloud Alternator Authentication

ScyllaDB Cloud clusters authenticate Alternator requests using the **salted password hash** stored in `system.roles`, not the plaintext CQL password. To use espresso against a ScyllaDB Cloud Alternator endpoint, set `auth.fetch_salted_hash: true`.

When enabled, espresso:
1. Opens a CQL connection to the cluster using `access_key`/`secret_key` as CQL credentials.
2. Reads `salted_hash` from `system.roles` for the matching role.
3. Uses that hash as the Alternator `X-Amz-Security-Token` / secret key for all HTTP requests.

```yaml
target:
  kind: alternator
  nodes:
    - https://node1.scylladb.cloud:8043
  auth:
    access_key: my-user
    secret_key: my-plaintext-password
    fetch_salted_hash: true
    # Optional: explicit CQL contact points (defaults to Alternator nodes with port 9042)
    cql_contact_points:
      - node1.scylladb.cloud:9142
```

If `cql_contact_points` is omitted, espresso derives CQL addresses by stripping the HTTP scheme from each Alternator node URL and substituting port `9042`.

## Related Docs

- [Profile Configuration Reference](config-reference.md)
- [Profile Cookbook](cookbook.md)
- [Operation Docs Index](README.md)
