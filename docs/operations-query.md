# Query

Use Query to stress partition-scoped reads with optional range and projection behavior.

## How to use

- Query requires a partition key and is most meaningful with a sort key.
- Control read size with `limit`.
- Toggle `scan_index_forward` for ascending or descending order.

## Full configuration example

```yaml
version: 1

vars:
  partition_count: 10000
  sort_window: 100

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
  - name: query_table
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
  - name: run_query
    mode: duration
    duration: 60s
    concurrency: 128
    warmup: 5s
    include_in_metrics: true
    table_operations:
      - table: query_table
        mix:
          query: 1.0
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

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/query.json
    hdr_histogram_file: ./results/query.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
