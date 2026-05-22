# Espresso Profile Configuration Reference

This is the authoritative schema reference for espresso workload profiles. It covers every top-level key, allowed types and values, and validation rules.

For full copyable examples by use-case, see the [Cookbook](cookbook.md).

## File Format

| Property | Value |
|---|---|
| Supported formats | YAML, TOML |
| Recommended extension | .yaml |
| Profiles per file | 1 |

## Top-Level Keys

| Key | Type | Required | Description |
|---|---|---|---|
| version | integer | yes | Profile schema version. Currently must be `1`. |
| vars | map | no | Named scalar variables. Referenced as `${vars.name}` elsewhere in the file. |
| target | object | yes | Endpoint connection and auth settings. |
| tables | list | yes | Table schema definitions. All tables used in phases must be declared here. |
| schema | object | yes | Table lifecycle management before phases run. |
| phases | list | yes | Ordered workload stages. Executed sequentially. |
| report | object | no | Metrics output settings. Recommended. |
| compatibility | object | no | Endpoint constraint settings. Recommended. |

---

### vars

Map of named scalars. Any key name is valid. Values can be integers, floats, or strings.

Referenced anywhere in the file with `${vars.<name>}`. Example: `${vars.row_count}`.

Conventionally used vars in examples:

| Var | Typical use |
|---|---|
| row_count | Total keys in the logical keyspace |
| hot_row_count | Size of hot working set for cached_working_set strategy |
| cache_hit_ratio | Percentage of reads from hot set (0–100) |
| populate_operation_count | How many operations to run in populate phase |
| partition_count | Number of distinct partition keys for Query workloads |
| sort_window | Range width for sort key conditions in Query |

---

### target

| Key | Type | Required | Allowed values / notes |
|---|---|---|---|
| kind | string | yes | `alternator` or `cql` |
| nodes | list of strings | yes | Alternator: URL list (for example `http://node1:8000`). CQL: host:port list (for example `node1:9042`). |
| region | string | no | Alternator region string. Default: `us-east-1`. |
| auth.access_key | string | no | Optional for both backends. For CQL, used as username when non-empty. |
| auth.secret_key | string | no | Optional for both backends. For CQL, used as password when access_key is non-empty. |
| tls.enabled | boolean | no | Default: `false` |
| cql.keyspace | string | no | CQL keyspace name. Default: `espresso`. |
| cql.connections_per_shard | integer | no | Native CQL connections per shard. Default: `1`. |
| cql.local_dc | string | no | Optional local datacenter preference for token-aware routing. |

Backend notes:
- Use `kind: alternator` for DynamoDB-compatible HTTP workloads.
- Use `kind: cql` for native Scylla CQL workloads.
- The same phase/mix/generator definitions work for both backends.

---

### tables

List of table definitions. Each entry:

| Key | Type | Required | Notes |
|---|---|---|---|
| name | string | yes | Unique within the profile |
| billing_mode | string | yes | `pay_per_request` recommended |
| key_schema.partition_key.name | string | yes | Attribute name |
| key_schema.partition_key.type | string | yes | `N`, `S`, or `B` |
| key_schema.sort_key.name | string | no | Attribute name |
| key_schema.sort_key.type | string | no | `N`, `S`, or `B` |
| attributes | list | yes | All projected attributes |

Each attribute in `attributes`:

| Key | Type | Required | Notes |
|---|---|---|---|
| name | string | yes | Attribute name |
| type | string | yes | Alternator: `N`, `S`, `B`. CQL native: `bigint`, `text`, `blob`, `int`, `smallint`, `tinyint`, `float`, `double`, `boolean` (legacy `N`/`S`/`B` still accepted). |
| size_bytes | integer | no | Controls generated payload size. Meaningful for `S` and `B`. |

---

### schema

| Key | Type | Required | Notes |
|---|---|---|---|
| create_if_missing | boolean | yes | Creates the table before phases run if it does not exist |
| delete_if_exists | boolean | yes | Drops and recreates the table. Use with caution. |
| apply_to_tables | string or list | yes | `all` applies to every table. Explicit list (planned). |

---

### phases

List of phase objects, executed in order. Each phase:

| Key | Type | Required | Notes |
|---|---|---|---|
| name | string | yes | Unique label, appears in reports |
| mode | string | yes | `duration` or `operation_count` |
| duration | string | if mode=duration | Duration string, e.g. `60s`, `5m` |
| operations | integer | if mode=operation_count | Total operation count. Accepts `${vars.*}`. |
| concurrency | integer | yes | Number of concurrent workers |
| warmup | string | no | Duration excluded from metrics. e.g. `10s` |
| include_in_metrics | boolean | yes | Set `false` for populate phases |
| table_operations | list | yes | Per-table operation configuration |

---

### report

| Key | Type | Notes |
|---|---|---|
| live_progress.enabled | boolean | Print progress to stdout during run |
| live_progress.interval | string | Update interval, e.g. `1s` |
| outputs.text_summary | boolean | Print final summary to stdout |
| outputs.json_file | path | Write final report as JSON |
| outputs.hdr_histogram_file | path | Write HDR-compatible histogram file |

Note: `live_progress.interval` is only applied when `live_progress.enabled` is `true`.

---

### Runtime CLI Overrides

Some execution controls are CLI-only and are not stored in the profile.

| CLI option | Type | Description |
|---|---|---|
| `--concurrency` | integer | Overrides `phase.concurrency` for all selected phases |
| `--phase` | string | Runs only the named phase |
| `--var KEY=VALUE` | repeatable | Overrides values in top-level `vars` |
| `--throttle` | integer | Global phase throughput target (ops/sec). `0` disables throttling (unlimited). |
| `--target-p99-ms` | float | Enables adaptive throttle control to target a specific p99 latency (milliseconds). |

`--target-p99-ms` behavior:
- The controller adjusts throttle every second based on observed p99.
- If `--throttle` is provided, it is used as the initial starting rate.
- If `--throttle` is omitted, espresso starts from an internal default rate and adapts.

---

### compatibility

| Key | Type | Allowed values | Notes |
|---|---|---|---|
| endpoint_type | string | `alternator`, `dynamodb_compatible` (planned) | Controls validation rules applied |
| enforce_supported_operations | boolean | — | Rejects profiles using Alternator-unsupported ops |
| write_isolation_mode | string | `unknown`, `lwt`, `non_lwt` | Reported in output. Does not configure the cluster. |

## table_operations

Each phase contains a list of `table_operations`. Each entry targets one table.

| Key | Type | Required | Notes |
|---|---|---|---|
| table | string | yes | Must match a name in `tables` |
| vars_overrides | map | no | Overrides specific `vars` for this table only within this phase |
| mix | map | yes | Operation name → weight. Weights must sum to 1.0. |
| operation_templates | map | yes | One entry per operation name present in `mix` |

### mix — Supported Operation Keys

| Key | DynamoDB Operation |
|---|---|
| put_item | PutItem |
| get_item | GetItem |
| batch_write_item | BatchWriteItem |
| batch_get_item | BatchGetItem |
| query | Query |
| scan | Scan |
| update_item | UpdateItem |
| delete_item | DeleteItem |

## Composite Partition Setup (PK + SK)

To model multi-row partitions, define a sort key in `tables[].key_schema`.

Example:

```yaml
tables:
  - name: orders
    billing_mode: pay_per_request
    key_schema:
      partition_key:
        name: customer_id
        type: N
      sort_key:
        name: order_ts
        type: N
    attributes:
      - name: customer_id
        type: N
      - name: order_ts
        type: N
      - name: val
        type: B
        size_bytes: 256
```

Runtime behavior:
- Espresso reads key names/types directly from `key_schema`.
- For PK+SK tables, CRUD and batch requests include both keys automatically.
- Supported for `put_item`, `get_item`, `delete_item`, `update_item`, `batch_get_item`, and `batch_write_item`.

Sort key generation:
- If `operation_templates.<op>.sort_key` is provided, Espresso uses that generator.
- If omitted, Espresso falls back to the same generator as the partition key.
- If neither is specified, Espresso uses a sequential fallback from `offset`.

Rows-per-partition controls (PK+SK tables):

| Var | Type | Behavior |
|---|---|---|
| `rows_per_partition` | integer > 0 | Fixed sort-key rows per partition |
| `rows_per_partition_min` + `rows_per_partition_max` | integers > 0 | Randomized rows per partition within inclusive range |

Notes:
- If `rows_per_partition` is set, it takes precedence.
- If min/max are set, Espresso computes a stable per-partition value in `[min, max]`.
- This allows uneven partition sizes (for example some partitions with 5 rows, others with 90).
- These values define the active logical sort-key window per partition; exact on-disk uniqueness depends on generator strategy and write mix.

Simple bounded key syntax:
- Generators support `range: [min, max]` (inclusive).
- `uniform` and `zipf` stay within this range.
- `sequential` walks this range and wraps to `min` after `max`.

String formatting (`S` attributes only):
- Generators support optional `prefix` and `suffix` fields.
- These are applied when the destination attribute type is `S`.
- Works for partition keys, sort keys, and non-key string value fields (`value_field`).
- For `N` and `B` attributes, `prefix`/`suffix` are ignored.

Example (`customer_id` becomes `sometext-1`, `sometext-2`, ...):

```yaml
operation_templates:
  get_item:
    key:
      strategy: sequential
      key_field: customer_id
      range: [1, 1000]
      prefix: "sometext-"
```

Example with suffix (`1-v1`, `2-v1`, ...):

```yaml
operation_templates:
  get_item:
    key:
      strategy: uniform
      key_field: customer_id
      range: [1, 1000]
      suffix: "-v1"
```

Example for sort key formatting (`ts-1-v1`, `ts-2-v1`, ...):

```yaml
operation_templates:
  get_item:
    key:
      strategy: uniform
      key_field: customer_id
      range: [1, 1000]
    sort_key:
      strategy: sequential
      key_field: order_ts
      range: [1, 1000]
      prefix: "ts-"
      suffix: "-v1"
```

Example for non-key string value field (`payload-xxxxxxxx`):

```yaml
operation_templates:
  put_item:
    item:
      strategy: uniform
      key_field: customer_id
      value_field: payload
      prefix: "payload-"
```

Recommended patterns:
- Deterministic bounded keys: `strategy: sequential` with `range: [min, max]`.
- Random bounded keys: `strategy: uniform` with `range: [min, max]`.
- Skewed bounded keys: `strategy: zipf` with `range: [min, max]`.

Example:

```yaml
vars:
  rows_per_partition_min: 1
  rows_per_partition_max: 100

operation_templates:
  get_item:
    key:
      strategy: uniform
      key_field: customer_id
      range: [1, 100000]
    sort_key:
      strategy: uniform
      key_field: order_ts
      range: [1, 100]
```

## Batch Operation Setup

### batch_get_item

Path: `phases[].table_operations[].operation_templates.batch_get_item`

```yaml
operation_templates:
  batch_get_item:
    keys:
      - strategy: uniform
        key_field: customer_id
```

Notes:
- `keys` is optional.
- Batch size uses `keys` list length when present.
- If omitted, batch size defaults to 25.
- Effective batch size is clamped to `1..100`.
- `retry_unprocessed_keys: true` treats unprocessed keys as retriable failures.
- `retry` config (optional) applies Latte-style retries for operation errors:
  - `max_attempts` (default `10`)
  - `backoff`: `fixed`, `exponential`, `exponential_jitter` (default)
  - `base_delay_ms` (default `100`)
  - `max_delay_ms` (default `5000`)

### batch_write_item

Path: `phases[].table_operations[].operation_templates.batch_write_item`

```yaml
operation_templates:
  batch_write_item:
    items:
      - strategy: generated_from_vars
        key_field: customer_id
        value_field: val
```

Notes:
- `items` is optional.
- Batch size uses `items` list length when present.
- If omitted, batch size defaults to 25.
- Effective batch size is clamped to `1..100`.
- `retry_unprocessed_items: true` treats unprocessed items as retriable failures.
- `retry` config (optional) uses the same fields as `batch_get_item`.

Example mix with batch operations:

```yaml
table_operations:
  - table: orders
    mix:
      batch_get_item: 0.6
      batch_write_item: 0.4
    operation_templates:
      batch_get_item:
        keys:
          - strategy: cached_working_set
            key_field: customer_id
      batch_write_item:
        items:
          - strategy: generated_from_vars
            key_field: customer_id
            value_field: val
```

## Generator Strategy Reference

The `strategy` field on a key or value generator controls how values are produced per operation.

### sequential

| Key | Type | Required | Notes |
|---|---|---|---|
| strategy | string | yes | `sequential` |
| range | [integer, integer] | yes | Inclusive `[start, end]` range |
| overflow | string | no | `cycle` (default) — wraps back to start when end is reached |

Behavior: emits `start, start+1, ..., end, start, start+1, ...` continuously.

Use when: seeding tables deterministically, testing with intentional duplicates, verifying upsert behavior.

Example:
```yaml
key:
  field: key
  strategy: sequential
  range: [1, 1000]
  overflow: cycle
```

---

### uniform

| Key | Type | Required | Notes |
|---|---|---|---|
| strategy | string | yes | `uniform` |
| range | [integer, integer] | yes | Inclusive `[start, end]` range |

Behavior: picks a value in range with equal probability on every operation.

Use when: baseline throughput testing, cache-miss dominated workloads.

Example:
```yaml
key:
  field: key
  strategy: uniform
  range: [1, 10000000]
```

---

### zipf

| Key | Type | Required | Notes |
|---|---|---|---|
| strategy | string | yes | `zipf` |
| range | [integer, integer] | yes | Inclusive `[start, end]` range |
| s | float | yes | Skew exponent. Common values: `1.05` (mild), `1.2` (moderate), `1.5` (heavy) |

Behavior: lower-numbered ranks are chosen exponentially more often. Rank 1 is hottest.

Use when: modeling realistic access skew, hot-key pressure testing.

Example:
```yaml
key:
  field: key
  strategy: zipf
  range: [1, 10000000]
  s: 1.2
```

---

### fixed

| Key | Type | Required | Notes |
|---|---|---|---|
| strategy | string | yes | `fixed` |
| fixed_pattern | string | one of | Pattern repeated/truncated to fill `size_bytes` |
| fixed_value | scalar | one of (planned) | Exact literal value used as-is |

Behavior: every generated value for this field is identical.

Use when: isolating key performance from value generation cost, seeding with known content.

Example:
```yaml
value:
  field: val
  strategy: fixed
  fixed_pattern: "espresso"
```

---

### cached_working_set

| Key | Type | Required | Notes |
|---|---|---|---|
| strategy | string | yes | `cached_working_set` |
| vars source | implicit | n/a | Reads `row_count`, `hot_row_count`, `cache_hit_ratio` from active vars automatically |

Expected vars:

| Var | Meaning |
|---|---|
| row_count | Full key range size |
| hot_row_count | Size of hot subset. Set to `0` to disable hot set. |
| cache_hit_ratio | Integer 0–100. Probability of selecting from hot set. |

Behavior: with probability `cache_hit_ratio / 100` selects from `[1, hot_row_count]`; otherwise selects uniformly from `[1, row_count]`.

Use when: simulating realistic caching behavior, measuring cache efficiency impact.

Example:
```yaml
key:
  field: key
  strategy: cached_working_set
```

## Operation Template Reference

### put_item

Path: `phases[].table_operations[].operation_templates.put_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| item.key.field | string | yes | Attribute name for the partition key |
| item.key.strategy | string | yes | Generator strategy (see Generator Strategy Reference) |
| item.key.range | [int, int] | yes for sequential/uniform/zipf | Key generation range |
| item.key.overflow | string | no | `cycle` (default) for sequential |
| item.key.s | float | yes for zipf | Skew exponent |
| item.value.field | string | yes | Attribute name for the value field |
| item.value.strategy | string | yes | Generator strategy |
| item.value.fixed_pattern | string | yes for fixed | Pattern to fill value bytes |

---

### get_item

Path: `phases[].table_operations[].operation_templates.get_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| key.field | string | yes | Attribute name for the partition key |
| key.strategy | string | yes | Generator strategy |
| key.range | [int, int] | yes for sequential/uniform/zipf | Key selection range |
| key.s | float | yes for zipf | Skew exponent |
| key vars source | implicit | n/a | For `cached_working_set`, hot set params are read from active vars automatically |

---

### batch_write_item

Path: `phases[].table_operations[].operation_templates.batch_write_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| request_items_per_call | integer | yes | Items per batch. Alternator max: 100 |
| item_template | object | yes | Same structure as `put_item.item` |
| retry_unprocessed_items | boolean | no | Retry unprocessed items from response |
| retry.max_attempts | integer | no | Maximum retry attempts |
| retry.backoff | string | no | `exponential_jitter` or `fixed` (planned) |

---

### batch_get_item

Path: `phases[].table_operations[].operation_templates.batch_get_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| keys_per_call | integer | yes | Keys per batch. Alternator max: 100 |
| key | object | yes | Same structure as `get_item.key` |
| retry_unprocessed_keys | boolean | no | Retry unprocessed keys from response |
| retry.max_attempts | integer | no | Maximum retry attempts |
| retry.backoff | string | no | `exponential_jitter` or `fixed` (planned) |

---

### query

Path: `phases[].table_operations[].operation_templates.query`

| Key | Type | Required | Notes |
|---|---|---|---|
| key_condition.partition_key.field | string | yes | Partition key attribute name |
| key_condition.partition_key.strategy | string | yes | Generator strategy for partition key |
| key_condition.partition_key.range | [int, int] | yes | Partition key selection range |
| key_condition.partition_key.overflow | string | no | `cycle` for sequential |
| key_condition.sort_key.field | string | no | Sort key attribute name |
| key_condition.sort_key.operator | string | no | `eq`, `lt`, `lte`, `gt`, `gte`, `between`, `begins_with` |
| key_condition.sort_key.lower | integer | yes for between | Lower bound |
| key_condition.sort_key.upper | integer | yes for between | Upper bound |
| limit | integer | no | Max items per page |
| consistent_read | boolean | no | Default: `false` |
| scan_index_forward | boolean | no | `true` = ascending, `false` = descending. Default: `true` |
| projection | list of strings | no | Attribute names to return |

---

### scan

Path: `phases[].table_operations[].operation_templates.scan`

| Key | Type | Required | Notes |
|---|---|---|---|
| limit | integer | no | Max items per page |
| consistent_read | boolean | no | Default: `false` |
| segment.total_segments | integer | no | Enables parallel scan |
| segment.strategy | string | no | `worker_affinity` — assigns segment per worker |

---

### update_item

Path: `phases[].table_operations[].operation_templates.update_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| key | object | yes | Same structure as `get_item.key` |
| update.set | map | no | Attribute name → value generator for SET updates |
| update.add | map | no | Attribute name → numeric increment for ADD updates |
| update.remove | list | no (planned) | Attribute names to remove |
| condition.type | string | no | `optional` — skip if not met. `required` — fail if not met |
| condition.expression | string | no | DynamoDB condition expression string |

---

### delete_item

Path: `phases[].table_operations[].operation_templates.delete_item`

| Key | Type | Required | Notes |
|---|---|---|---|
| key | object | yes | Same structure as `get_item.key` |
| condition.type | string | no | `optional` or `required` |
| condition.expression | string | no | DynamoDB condition expression string |

---

### Named CQL Operations

Path: `phases[].table_operations[].operation_templates.<operation_name>` when `target.kind: cql`

This mode allows Cassandra-stress-style named queries, where `mix` keys are user-defined query names.

| Key | Type | Required | Notes |
|---|---|---|---|
| cql_query | string | yes | Raw CQL with positional `?` parameters. `{table}` placeholder expands to `<keyspace>.<table>`. |
| cql_params | list of strings | no | Ordered parameter source names for each `?` in `cql_query`. |
| key / item / sort_key | object | no | Standard generators used to produce values referenced by `cql_params`. |
| retry.* | object | no | Same retry behavior as other operations. |

Example:

```yaml
table_operations:
  - table: key_value_cql
    mix:
      read_by_pk: 0.8
      write_by_pk: 0.2
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

## Validation Rules

| Rule | Detail |
|---|---|
| `version` is supported | Currently must be `1` |
| `mix` weights sum to 1.0 | Epsilon of 0.001 allowed |
| Every `mix` key has a matching `operation_templates` entry | Validation error otherwise |
| `table_operations[].table` exists in `tables` | Validation error otherwise |
| `mode: duration` requires `duration`, forbids `operations` | Validation error otherwise |
| `mode: operation_count` requires `operations`, forbids `duration` | Validation error otherwise |
| `batch_write_item.request_items_per_call` ≤ 100 for Alternator | Hard limit |
| `batch_get_item.keys_per_call` ≤ 100 for Alternator | Hard limit |
| `range[0]` ≤ `range[1]` for all strategies | Validation error otherwise |
| `zipf.s` > 0 | Validation error otherwise |
| `size_bytes` > 0 when provided | Validation error otherwise |
