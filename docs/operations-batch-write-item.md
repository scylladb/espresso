# BatchWriteItem

Use BatchWriteItem to stress high-ingest paths and unprocessed-item retry behavior.

## How to use

- Set `request_items_per_call` based on endpoint limits.
- For Alternator, use up to 100 items per request.
- Enable retries for unprocessed items.
- This example uses `sequential` keys and `fixed` value content in each batch item.

## Full configuration example

```yaml
version: 1

vars:
  row_count: 5000000

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
  - name: batch_write_table
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
  - name: run_batch_write
    mode: duration
    duration: 90s
    concurrency: 128
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: batch_write_table
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
                fixed_pattern: "batch-write"
            retry_unprocessed_items: true
            retry:
              max_attempts: 8
              backoff: exponential_jitter

report:
  live_progress:
    enabled: true
    interval: 1s
  outputs:
    text_summary: true
    json_file: ./results/batch_write_item.json
    hdr_histogram_file: ./results/batch_write_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
