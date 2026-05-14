# PutItem

Use PutItem to generate write-heavy traffic with predictable item shape and payload size.

## How to use

- Define one or more tables under `tables`.
- Set attribute payload size with `size_bytes` on non-key attributes.
- In `phases[].table_operations[].operation_templates.put_item.item`, use explicit generators.
- This example uses `sequential` for keys and `fixed` for value payload content.

## Full configuration example

```yaml
version: 1

vars:
  row_count: 1000000
  populate_operation_count: 200000

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
  - name: put_item_table
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
      - table: put_item_table
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
                fixed_pattern: "espresso"

  - name: run_put_only
    mode: duration
    duration: 60s
    concurrency: 256
    warmup: 5s
    include_in_metrics: true
    table_operations:
      - table: put_item_table
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
                fixed_pattern: "espresso"

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/put_item.json
    hdr_histogram_file: ./results/put_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
