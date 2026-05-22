# Espresso Profile Cookbook

A set of ready-to-use profiles for common Alternator stress scenarios.

For the full list of available config keys and strategies, see the [Configuration Reference](config-reference.md).

## Patterns

- [1. Write-Heavy (Sequential Populate + Sustained Writes)](#1-write-heavy-sequential-populate--sustained-writes)
- [2. Read-Heavy with Hot-Key Skew (Zipf)](#2-read-heavy-with-hot-key-skew-zipf)
- [3. Mixed Read/Write with Cached Working Set](#3-mixed-readwrite-with-cached-working-set)
- [4. Batch-Heavy (BatchWriteItem + BatchGetItem)](#4-batch-heavy-batchwriteitem--batchgetitem)
- [5. Query and Scan with Range Access](#5-query-and-scan-with-range-access)

---

## 1. Write-Heavy (Sequential Populate + Sustained Writes)

Use this pattern when:
- Benchmarking ingest throughput
- Testing write isolation modes
- Generating tombstone pressure with sequential key cycling

The `sequential` generator ensures every key in range is touched predictably, making
it easy to reason about coverage and reproduce results.

```yaml
version: 1

vars:
  row_count: 5000000
  populate_operation_count: 5000000

target:
  kind: alternator
  nodes:
    - http://node1:8000
    - http://node2:8000
  region: us-east-1
  auth:
    access_key: test
    secret_key: test
  tls:
    enabled: false

tables:
  - name: write_heavy
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: key
        type: N
    attributes:
      - name: key
        type: N
      - name: val
        type: B
        size_bytes: 1024

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: populate
    mode: operation_count
    operations: ${vars.populate_operation_count}
    concurrency: 256
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: write_heavy
        mix:
          put_item: 1.0
        operation_templates:
          put_item:
            item:
              key:
                field: key
                strategy: sequential
                range: [1, ${vars.row_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "seed"

  - name: run_write_heavy
    mode: duration
    duration: 120s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: write_heavy
        mix:
          put_item: 1.0
        operation_templates:
          put_item:
            item:
              key:
                field: key
                strategy: sequential
                range: [1, ${vars.row_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "run"

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/write_heavy.json
    hdr_histogram_file: ./results/write_heavy.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: non_lwt
```

---

## 2. Read-Heavy with Hot-Key Skew (Zipf)

Use this pattern when:
- Measuring latency under realistic skewed read traffic
- Testing cache effectiveness under Zipf-distributed access
- Evaluating hot-partition handling in Alternator

The `zipf` generator with `s: 1.2` produces strong access skew where a small
number of keys absorb the majority of reads.

```yaml
version: 1

vars:
  row_count: 10000000
  populate_operation_count: 1000000

target:
  kind: alternator
  nodes:
    - http://node1:8000
    - http://node2:8000
  region: us-east-1
  auth:
    access_key: test
    secret_key: test
  tls:
    enabled: false

tables:
  - name: read_skewed
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: key
        type: N
    attributes:
      - name: key
        type: N
      - name: val
        type: B
        size_bytes: 512

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: populate
    mode: operation_count
    operations: ${vars.populate_operation_count}
    concurrency: 128
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: read_skewed
        mix:
          put_item: 1.0
        operation_templates:
          put_item:
            item:
              key:
                field: key
                strategy: sequential
                range: [1, ${vars.row_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "seed"

  - name: run_read_skewed
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: read_skewed
        mix:
          get_item: 0.95
          put_item: 0.05
        operation_templates:
          get_item:
            key:
              field: key
              strategy: zipf
              range: [1, ${vars.row_count}]
              s: 1.2
          put_item:
            item:
              key:
                field: key
                strategy: uniform
                range: [1, ${vars.row_count}]
              value:
                field: val
                strategy: fixed
                fixed_pattern: "update"

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/read_skewed.json
    hdr_histogram_file: ./results/read_skewed.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```

---

## 3. Mixed Read/Write with Cached Working Set

Use this pattern when:
- Simulating a real application with a hot working set in cache
- Measuring cache-hit vs cache-miss latency difference
- Testing 80/20 or 90/10 read/write mixes

Setting `hot_row_count > 0` enables a two-tier key pool:
reads are served from the hot set at `cache_hit_ratio` percent of the time.
Setting `hot_row_count: 0` disables the hot set (pure uniform reads).

```yaml
version: 1

vars:
  row_count: 5000000
  hot_row_count: 50000
  cache_hit_ratio: 80
  populate_operation_count: 500000

target:
  kind: alternator
  nodes:
    - http://node1:8000
    - http://node2:8000
  region: us-east-1
  auth:
    access_key: test
    secret_key: test
  tls:
    enabled: false

tables:
  - name: mixed_cached
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: key
        type: N
    attributes:
      - name: key
        type: N
      - name: val
        type: B
        size_bytes: 1024

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: populate
    mode: operation_count
    operations: ${vars.populate_operation_count}
    concurrency: 128
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: mixed_cached
        mix:
          put_item: 1.0
        operation_templates:
          put_item:
            item:
              key:
                field: key
                strategy: sequential
                range: [1, ${vars.row_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "seed"

  - name: run_mixed
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: mixed_cached
        mix:
          get_item: 0.8
          put_item: 0.2
        operation_templates:
          get_item:
            key:
              field: key
              strategy: cached_working_set
          put_item:
            item:
              key:
                field: key
                strategy: uniform
                range: [1, ${vars.row_count}]
              value:
                field: val
                strategy: fixed
                fixed_pattern: "write"

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/mixed_cached.json
    hdr_histogram_file: ./results/mixed_cached.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```

---

## 4. Batch-Heavy (BatchWriteItem + BatchGetItem)

Use this pattern when:
- Benchmarking batch ingest and batch read throughput
- Testing unprocessed item/key retry behavior
- Comparing per-request vs batched operation efficiency

Alternator supports up to 100 items per `BatchWriteItem` request (vs 25 for standard DynamoDB).

```yaml
version: 1

vars:
  row_count: 10000000
  populate_operation_count: 2000000

target:
  kind: alternator
  nodes:
    - http://node1:8000
    - http://node2:8000
  region: us-east-1
  auth:
    access_key: test
    secret_key: test
  tls:
    enabled: false

tables:
  - name: batch_table
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: key
        type: N
    attributes:
      - name: key
        type: N
      - name: val
        type: B
        size_bytes: 256

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: populate
    mode: operation_count
    operations: ${vars.populate_operation_count}
    concurrency: 64
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: batch_table
        mix:
          batch_write_item: 1.0
        operation_templates:
          batch_write_item:
            request_items_per_call: 100
            item_template:
              key:
                field: key
                strategy: sequential
                range: [1, ${vars.row_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "seed"
            retry_unprocessed_items: true
            retry:
              max_attempts: 8
              backoff: exponential_jitter

  - name: run_batch
    mode: duration
    duration: 90s
    concurrency: 128
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: batch_table
        mix:
          batch_write_item: 0.4
          batch_get_item: 0.6
        operation_templates:
          batch_write_item:
            request_items_per_call: 100
            item_template:
              key:
                field: key
                strategy: uniform
                range: [1, ${vars.row_count}]
              value:
                field: val
                strategy: fixed
                fixed_pattern: "run"
            retry_unprocessed_items: true
            retry:
              max_attempts: 8
              backoff: exponential_jitter
          batch_get_item:
            keys_per_call: 100
            key:
              field: key
              strategy: zipf
              range: [1, ${vars.row_count}]
              s: 1.1
            retry_unprocessed_keys: true
            retry:
              max_attempts: 8
              backoff: exponential_jitter

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/batch_heavy.json
    hdr_histogram_file: ./results/batch_heavy.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```

---

## 5. Query and Scan with Range Access

Use this pattern when:
- Testing range query throughput across partitions
- Generating full-table scan load for compaction or GC pressure
- Benchmarking sort key range conditions

The `sequential` partition key generator combined with a fixed sort key window
ensures consistent and reproducible range access behavior.

```yaml
version: 1

vars:
  partition_count: 50000
  sort_window: 200
  row_count: 10000000
  populate_operation_count: 1000000

target:
  kind: alternator
  nodes:
    - http://node1:8000
    - http://node2:8000
  region: us-east-1
  auth:
    access_key: test
    secret_key: test
  tls:
    enabled: false

tables:
  - name: range_table
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: pk
        type: N
      sort_key:
        name: sk
        type: N
    attributes:
      - name: pk
        type: N
      - name: sk
        type: N
      - name: val
        type: B
        size_bytes: 512

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: populate
    mode: operation_count
    operations: ${vars.populate_operation_count}
    concurrency: 128
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: range_table
        mix:
          put_item: 1.0
        operation_templates:
          put_item:
            item:
              key:
                field: pk
                strategy: sequential
                range: [1, ${vars.partition_count}]
                overflow: cycle
              value:
                field: val
                strategy: fixed
                fixed_pattern: "seed"

  - name: run_query_scan
    mode: duration
    duration: 60s
    concurrency: 128
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: range_table
        mix:
          query: 0.8
          scan: 0.2
        operation_templates:
          query:
            key_condition:
              partition_key:
                field: pk
                strategy: sequential
                range: [1, ${vars.partition_count}]
                overflow: cycle
              sort_key:
                field: sk
                operator: between
                lower: 1
                upper: ${vars.sort_window}
            limit: 100
            consistent_read: false
            scan_index_forward: true
            projection:
              - pk
              - sk
              - val
          scan:
            limit: 200
            consistent_read: false
            segment:
              total_segments: 16
              strategy: worker_affinity

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/query_scan.json
    hdr_histogram_file: ./results/query_scan.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
