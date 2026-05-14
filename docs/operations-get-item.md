# GetItem

Use GetItem for point-read latency and cache sensitivity testing.

## How to use

- Choose key strategy by table operation.
- Use `uniform` for baseline random reads.
- Use `zipf` to model skewed hot-key access.

## Full configuration example

```yaml
version: 1

vars:
  row_count: 2000000

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
  - name: get_item_table
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

  - name: get_item_table_hot
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
    operations: 200000
    concurrency: 128
    warmup: 0s
    include_in_metrics: false
    table_operations:
      - table: get_item_table
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
      - table: get_item_table_hot
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

  - name: run_get_only
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: get_item_table
        mix:
          get_item: 1.0
        operation_templates:
          get_item:
            key:
              field: key
              strategy: uniform
              range: [1, ${vars.row_count}]

      - table: get_item_table_hot
        mix:
          get_item: 1.0
        operation_templates:
          get_item:
            key:
              field: key
              strategy: zipf
              range: [1, ${vars.row_count}]
              s: 1.2

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/get_item.json
    hdr_histogram_file: ./results/get_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
