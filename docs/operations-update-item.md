# UpdateItem

Use UpdateItem to stress partial-row mutations and conditional write behavior.

## How to use

- Use key strategy to choose updated rows.
- Use `set`, `add`, and `remove` update clauses.
- Add optional conditions to evaluate conditional write overhead.

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
  - name: update_table
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
      - name: version
        type: N

schema:
  create_if_missing: true
  delete_if_exists: false
  apply_to_tables: all

phases:
  - name: run_update
    mode: duration
    duration: 60s
    concurrency: 128
    warmup: 5s
    include_in_metrics: true
    table_operations:
      - table: update_table
        mix:
          update_item: 1.0
        operation_templates:
          update_item:
            key:
              field: key
              strategy: zipf
              range: [1, ${vars.row_count}]
              s: 1.3
            update:
              set:
                val:
                  field: val
                  strategy: fixed
                  fixed_pattern: "updated"
              add:
                version: 1
            condition:
              type: optional
              expression: attribute_exists(key)

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/update_item.json
    hdr_histogram_file: ./results/update_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
