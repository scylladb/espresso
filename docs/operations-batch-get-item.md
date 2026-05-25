# BatchGetItem

Use BatchGetItem to model batched point-read traffic and unprocessed key retries.

## How to use

- Use `key` for the generator template and `keys` when you want to control batch size by list length.
- For Alternator, up to 100 keys per request is supported.
- Enable retry for unprocessed keys to simulate production client behavior.
- Use `retry:` for generic request retries. `retry_unprocessed_keys` only applies to `batch_get_item`.

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
  - name: batch_get_table
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
  - name: run_batch_get
    mode: duration
    duration: 90s
    concurrency: 128
    warmup: 10s
    include_in_metrics: true
    table_operations:
      - table: batch_get_table
        mix:
          batch_get_item: 1.0
        operation_templates:
          batch_get_item:
            key:
              strategy: zipf
              range: [1, ${vars.row_count}]
              theta: 1.1
            keys:
              - strategy: zipf
                key_field: key
                range: [1, ${vars.row_count}]
                theta: 1.1
              - strategy: zipf
                key_field: key
                range: [1, ${vars.row_count}]
                theta: 1.1
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
    json_file: ./results/batch_get_item.json
    hdr_histogram_file: ./results/batch_get_item.hdr

compatibility:
  endpoint_type: alternator
  enforce_supported_operations: true
  write_isolation_mode: unknown
```
