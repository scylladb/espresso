# DeleteItem

Use DeleteItem to test targeted delete throughput, tombstone pressure, and mixed read/write cleanup flows.

## How to use

- Use key strategy to control which rows are deleted.
- Combine with GetItem in mix ratios to model churn.
- Optionally add delete conditions for conditional delete benchmarks.

## Full configuration example

```yaml
version: 1

vars:
  row_count: 1000000

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
  - name: delete_table
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
  - name: run_delete
    mode: duration
    duration: 60s
    concurrency: 128
    warmup: 5s
    include_in_metrics: true
    table_operations:
      - table: delete_table
        mix:
          delete_item: 1.0
        operation_templates:
          delete_item:
            key:
              field: key
              strategy: sequential
              range: [1, ${vars.row_count}]
              overflow: cycle
            condition:
              type: optional
              expression: attribute_exists(key)

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/delete_item.json
    hdr_histogram_file: ./results/delete_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
