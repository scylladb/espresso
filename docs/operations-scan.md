# Scan

Use Scan to generate full-table read load and evaluate large-range read throughput.

## How to use

- Use `limit` to control page size.
- Use segmented scan for parallel table traversal.
- Avoid using Scan as a substitute for Query when key conditions exist.

## Full configuration example

```yaml
version: 1

vars:
  row_count: 3000000

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
  - name: scan_table
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
    operations: 500000
    concurrency: 128
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: scan_table
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
                fixed_pattern: "scan-seed"

  - name: run_scan
    mode: duration
    duration: 60s
    concurrency: 64
    warmup: 5s
    include_in_metrics: true
    table_operations:
      - table: scan_table
        mix:
          scan: 1.0
        operation_templates:
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
    json_file: ./results/scan.json
    hdr_histogram_file: ./results/scan.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
