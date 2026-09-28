# Anomaly Detection Component

Detect anomalies and outliers in customer behavior, transactions, and business metrics using statistical methods.

## Purpose

Identify unusual patterns using proven statistical techniques:
- **Z-Score**: Detects values far from mean (standard deviations)
- **IQR (Interquartile Range)**: Robust outlier detection using quartiles
- **Moving Average**: Time-series deviation detection
- **Threshold**: Simple threshold-based alerts

## Ingestion

Two ways to get the rows in, set exactly one:

- **`upstream_asset_key`**: the usual Dagster way -- point at any asset producing a DataFrame with `metric_column`.
- **`source: {kind: warehouse_query, resource_key: ..., sql: ...}`**: pull rows directly via SQL, no upstream asset required. Works out of the box with `duckdb_resource` and any resource exposing `.get_engine()`/`.get_connection()`, or a bare SQLAlchemy connection string via `database_url_env_var` when no Dagster resource is registered.

## `execution_mode: sql` -- the same statistics, computed server-side

All four detection methods (z_score, iqr, moving_average, threshold) are genuinely portable statistics -- `AVG`/`STDDEV`/`PERCENTILE_CONT` window functions -- not vendor-specific ML, so `execution_mode: sql` runs them as ONE query via the warehouse's own engine on any of 6 dialects (`snowflake`, `bigquery`, `databricks`, `postgres`, `redshift`, `duckdb`), instead of pulling the whole table into memory first:

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: transaction_anomalies_sql
  source:
    kind: warehouse_query
    resource_key: snowflake_resource
    sql: "SELECT * FROM raw.transactions"
  execution_mode: sql
  sql_dialect: snowflake
  output_table: analytics.transaction_anomalies
  detection_method: z_score
  metric_column: amount
  threshold: 3.0
```

Requires `source` (there must be a SQL FROM-clause to compute over -- `upstream_asset_key` doesn't have one), `sql_dialect`, `output_table`, and `metric_column` (no auto-detection is possible without a DataFrame to inspect). `moving_average` additionally requires `timestamp_field`.

Two dialect-specific quirks worth knowing: BigQuery's `PERCENTILE_CONT` is analytic-only (`PERCENTILE_CONT(x, p) OVER (...)`, positional args) rather than the ANSI aggregate form other dialects use; DuckDB has no `PERCENTILE_CONT` at all and uses `QUANTILE_CONT(x, p) OVER (...)` instead (confirmed live) -- both are handled automatically based on `sql_dialect`.

## Use Cases

- **Fraud Detection**: Unusual transaction amounts or patterns
- **Quality Monitoring**: Detect data quality issues
- **System Monitoring**: Performance anomalies and errors
- **Business Metrics**: Revenue, conversion rate anomalies
- **Customer Behavior**: Unusual usage patterns
- **Security**: Detect suspicious activity

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `metric_column` | number | ✓ | (specified in config) | Metric to analyze |
| `id` | string | | record_id, transaction_id | Record identifier |
| `timestamp` | datetime | | date, created_at | Timestamp (for MA method) |
| `group_by` | string | | customer_id, category | Grouping field (optional) |

**Compatible Upstream Components:**
- Any component with numeric metrics to monitor
- `ecommerce_standardizer` (transaction amounts)
- `event_data_standardizer` (event metrics)
- System monitoring data

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `[all input columns]` | various | Original data preserved |
| `is_anomaly` | boolean | Anomaly flag |
| `anomaly_score` | number | Severity score |
| `anomaly_reason` | string | Why flagged as anomaly |

## Configuration

### Z-Score Method (Recommended)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: transaction_anomalies
  detection_method: z_score
  metric_column: amount
  threshold: 3.0  # Standard deviations
  description: Transaction anomaly detection
```

### IQR Method (Robust to Outliers)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: metric_anomalies
  detection_method: iqr
  metric_column: revenue
  threshold: 1.5  # IQR multiplier
```

### Moving Average (Time Series)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: daily_metric_anomalies
  detection_method: moving_average
  metric_column: daily_active_users
  moving_average_window: 7
  threshold: 2.0
  timestamp_field: date
```

## Detection Methods Explained

### Z-Score Method
Measures how many standard deviations a value is from the mean.

**When to use:**
- Normally distributed data
- General purpose anomaly detection
- Known expected range

**Threshold guidance:**
- 2.0 = ~95% of data (5% flagged)
- 3.0 = ~99.7% of data (0.3% flagged, recommended)
- 4.0 = ~99.99% of data (very strict)

### IQR Method
Uses interquartile range, resistant to extreme outliers.

**When to use:**
- Skewed distributions
- Data with existing outliers
- Robust detection needed

**Threshold guidance:**
- 1.5 = Standard (typical outlier definition)
- 2.0 = Moderate (fewer false positives)
- 3.0 = Conservative

### Moving Average
Compares current value to recent trend.

**When to use:**
- Time-series data
- Trending metrics
- Detect sudden changes

**Threshold guidance:**
- 2.0 = Moderate sensitivity
- 3.0 = Standard
- 4.0 = Low sensitivity (major changes only)

### Threshold Method
Simple comparison to fixed value.

**When to use:**
- Known acceptable limits
- Business rules (e.g., max transaction $10,000)
- Simple alerts

## Best Practices

**Choose the Right Method:**
- **Fraud/Security**: Z-score or IQR
- **Time Series**: Moving average
- **Business Rules**: Threshold
- **Unknown distribution**: IQR (most robust)

**Set Appropriate Thresholds:**
- Start conservative (3.0 for Z-score, 1.5 for IQR)
- Monitor false positive rate
- Adjust based on domain knowledge

**Handle False Positives:**
- Review flagged anomalies manually
- Refine thresholds over time
- Consider business context

**Group-Level Detection:**
- Use `group_by` for per-customer/per-category analysis
- Detects anomalies relative to normal behavior per group

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"monitoring"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `output_table` | `str` | — | Required when execution_mode='sql'. Destination table the scored rows are written to, in the same database. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream asset key providing a DataFrame to analyze for anomalies. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | Pull rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Also required (with `execution_mode='sql'`) to… _(full docs in schema.json + component README)_ |
| `execution_mode` | `str` | `"python"` | 'python' (default): pulls rows into a DataFrame and computes z_score/iqr/moving_average/threshold locally -- works with any source, but data leaves the database and the corpus must fit in memory. 'sql': the same statisti… _(full docs in schema.json + component README)_ |
| `sql_dialect` | `str` | — | `f"Required when execution_mode='sql'. One of: {_SQL_DIALECTS}."` |
| `detection_method` | `str` | `"z_score"` | Method: z_score, iqr, moving_average, threshold |
| `metric_column` | `Union[str, int]` | — | Column containing metric to analyze for anomalies |
| `threshold` | `float` | `3.0` | Detection threshold (Z-score stdevs, IQR multiplier, or absolute threshold) |
| `moving_average_window` | `int` | `7` | Window size for moving average method (days/records) |
| `group_by` | `str` | — | Group by field (e.g., customer_id) for per-group anomaly detection |
| `timestamp_field` | `str` | — | Timestamp field for time-series anomaly detection (optional) |
| `id_field` | `str` | — | ID field for tracking individual records (auto-detected) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

[//]: # (FIELDS:END)

## Example Use Cases

### Fraud Detection
```yaml
# Flag unusually high transaction amounts per customer
detection_method: z_score
metric_column: transaction_amount
group_by: customer_id
threshold: 3.5
```

### Revenue Monitoring
```yaml
# Alert on daily revenue anomalies
detection_method: moving_average
metric_column: daily_revenue
moving_average_window: 7
threshold: 2.0
timestamp_field: date
```

### Data Quality
```yaml
# Detect invalid data (e.g., negative amounts)
detection_method: threshold
metric_column: amount
threshold: 0  # Flag negative values
```

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).

## Validation

`validation.level: code` for the `source`/`execution_mode` additions.

**Live-verified (nothing mocked)**: `source: {kind: warehouse_query}` against a real DuckDB database; `execution_mode: sql` for all four detection methods (z_score, iqr, moving_average, threshold), generated and executed for real against DuckDB (one of the 6 supported dialects), with results asserted to **numerically match** the existing python-mode computation, not just structurally resemble it. Along the way, confirmed live that DuckDB has no `PERCENTILE_CONT` function at all (`Catalog Error: ... Did you mean "pi"?`) and requires `QUANTILE_CONT` instead — fixed before this shipped.

**Structural only, not executed (no live warehouse credentials in this environment)**: `snowflake`, `bigquery`, `databricks`, `postgres`, `redshift` dialects — the `PERCENTILE_CONT`/`STDDEV`/window-function syntax is standard/documented for each, but not run against a real account of any of the five.
