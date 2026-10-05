# Field Naming Conventions

This document defines the canonical field names used across components in the registry. The goal: a Dagster engineer should be able to write `defs.yaml` without having to look up "is it `upstream_asset_key` or `source_asset`? `n_bins` or `n_tiles`? `random_state` or `random_seed`?"

If you're authoring a new component, use these names. If you're updating an existing component and the rename is mechanical (no logic change), prefer the canonical name and add the old name as a Pydantic `validation_alias` so existing YAML keeps working.

---

## Asset wiring

| Canonical | Type | Use when |
|---|---|---|
| `asset_name` | `str` | The asset key this component produces |
| `upstream_asset_key` | `str` | The component depends on **one** upstream Dagster asset |
| `upstream_asset_keys` | `List[str]` | The component fans **multiple homogeneous** upstream assets in (unions) |
| `<role>_asset_key` | `str` | The component takes **multiple distinct upstream roles** — e.g. `regions_asset_key`, `lookup_asset_key`, `left_asset_key` / `right_asset_key`. Suffix is always `_asset_key`, prefix names the role. |
| `deps` | `Optional[List[str]]` | Lineage-only dependency (no data passed) |
| `group_name` | `Optional[str]` | Dagster asset group |

**Avoid:** `source_asset`, `input_asset`, `<role>_data_asset`. Always `upstream_asset_key` for single-upstream, `<role>_asset_key` for named-role multi-upstream.

**Examples of named-role multi-upstream:**

```yaml
# spatial_join (points + regions)
upstream_asset_key: customer_locations
regions_asset_key: store_polygons

# dataframe_join (left + right)
left_asset_key: orders
right_asset_key: customers

# customer_360 (multi-source standardizer)
customer_asset_key: customer_records
transaction_asset_key: transactions
support_asset_key: support_tickets
```

A handful of older analytics standardizers (`customer_360`, `customer_segmentation`, `revenue_attribution`, etc.) still use `<role>_data_asset` instead of `<role>_asset_key`. Those should be renamed in a future sweep with backward-compat aliases — same shape as the `source_asset` → `upstream_asset_key` migration.

## Dataframe-or-SQL dual input

Any component whose primary input is "a DataFrame of rows" (not a file,
not a blob, not a nested document) can optionally also support pulling
those rows directly via SQL, with no intermediate Dagster asset required.
This is NOT the `source_asset`/`input_asset` naming mistake flagged
above — `source` here is a structured config dict, not an asset
reference, and it's a genuine alternative input mode, not a rename of
`upstream_asset_key`.

| Canonical | Type | Use when |
|---|---|---|
| `upstream_asset_key` | `Optional[str]` | Depend on one upstream Dagster asset (unchanged meaning — just made `Optional` so `source` can be the alternative) |
| `source` | `Optional[Dict[str, Any]]` | `{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}`. Mutually exclusive with `upstream_asset_key` — set exactly one. |

**`build_defs()` must validate the mutual exclusivity** (copy verbatim, swap the class name):

```python
if bool(self.upstream_asset_key) == bool(self.source):
    raise ValueError("MyComponent: set exactly one of `upstream_asset_key` or `source`.")
```

**The ingestion helper is copy-paste canonical, not reinvented per component.** Grep any of the 39 current adopters (e.g. `transforms/hash/component.py`, `assets/analytics/automl_asset/component.py`) for `_ingest_warehouse_query` and copy it verbatim — don't write a new one. It dispatches, in order:

1. `.get_engine()` — SQLAlchemy engine (ClickHouse, SAP HANA, generic SQL resources)
2. `.get_connection()` — DBAPI2 connection (Postgres, Snowflake, DuckDB, MySQL, Oracle, MSSQL, Databricks SQL, Trino/Starburst, TimescaleDB, DB2, Doris, StarRocks)
3. `.get_client()` — vendor client, dispatched on the CLIENT's own shape (there's no universal `get_client` calling convention): BigQuery's `.query(sql).to_dataframe()`, Redshift's `.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)`
4. A bare SQLAlchemy engine built from `database_url_env_var`, when no Dagster resource is registered at all

**Wiring the asset decorator** — `ins=`/`required_resource_keys` become conditional instead of assumed:

```python
ins=({"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))} if self.upstream_asset_key else None),
required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
```

```python
def _asset(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
    df = kwargs.get("df")
    if df is None:
        df = _ingest_warehouse_query(self.source, context)
    ...
```

**Known gaps, not silently unsupported:** Cassandra (`cassandra_resource`, CQL-based, only exposes `.get_session()`) and CosmosDB (`cosmosdb_resource`, `.get_client()` but a document-query API, not BigQuery/Redshift-shaped) aren't dispatchable yet — calling `source` against either raises the helper's own `"must expose .get_engine()/..."` error, not a silent wrong-data bug. Add a dispatch branch if someone actually needs one.

**Out of scope by design, not by oversight:** components whose primary input is a file, blob, image, PDF, or nested document (not flat tabular rows) shouldn't get this field — there's no "SQL query" equivalent for "give me a PDF." Table-format catalogs without their own query engine (Iceberg) and RPC/REST-only resources (SAP RFC, NetSuite) are the same story on the resource side.

**Two-input components** (joins, lookups, SCD comparisons) only get dual-input on the PRIMARY slot — the secondary join/lookup/target input stays an always-required asset dependency. Don't build combinatorial dual-mode for every input; the secondary side is reference data, not the thing being queried.

**Fan-out components** (one asset per output, all siblings consuming the same upstream — e.g. `router`) run the SQL query once PER SIBLING when `source` is used, not once overall — Dagster doesn't share execution state across sibling consumers either way (an `ins=`-based fan-out re-reads the same materialized file per consumer too). Document this explicitly in the component's README; don't leave it as a silent surprise.

## Selecting columns

| Canonical | Type | Use when |
|---|---|---|
| `column` | `str` | The component operates on **exactly one** column (e.g. `tile_binning`, `array_exploder`) |
| `columns` | `Optional[List[str]]` | The component operates on **a set** of columns. `None` typically means "auto-detect / all numeric / all" |
| `feature_columns` | `List[str]` | ML features (separate from the prediction target) |
| `target_column` | `str` | ML target |
| `value_column` / `value_columns` | `str` / `List[str]` | The numeric column(s) being aggregated/transformed |
| `date_column` / `timestamp_field` | `str` | Datetime-typed column. Prefer `_column` suffix for new components. |
| `customer_id_field`, `order_id_field`, etc. | `Optional[str]` | When the component has heuristic auto-detection but lets the user override |
| `group_by` | `List[str]` | Group-by keys |

**Avoid:** `group_by_field` (singular, conflicts with `group_by` plural). Use `group_by: [single_col]` instead.

## Strategy vs method

Both are accepted. Prefer **`strategy`** for new components when the field selects an algorithm or approach (e.g. imputation strategy, scaling strategy, outlier clipping strategy — matches sklearn convention).

Use **`method`** when the field selects a *named operation* with no implicit configuration (e.g. join method, aggregation method).

Existing components using either name are valid; don't churn working code just to rename.

## Bin counts

| Canonical | Notes |
|---|---|
| `n_bins` | For tile_binning, lift_chart, histograms, etc. |
| `n_clusters` | For k-means, spatial clustering |

**Avoid:** `n_tiles`, `num_bins`, `bin_count`.

## ML model components

Models that produce DataFrames (predictions, importances, coefficients) should expose:

| Field | Type | Allowed values |
|---|---|---|
| `target_column` | `str` | required |
| `feature_columns` | `List[str]` | required |
| `task_type` | `str` | `classification` \| `regression` |
| `output_mode` | `str` | `predictions` \| `feature_importance` \| `coefficients` |
| `test_size` | `float` | default 0.2 |
| `random_state` | `int` | default 42 |

`output_mode` is what lets a single model component produce parallel branches (predictions + importance, etc.).

**Avoid:** `random_seed` (use `random_state` — sklearn convention).

## Filtering

| Canonical | Use when |
|---|---|
| `condition: str` | Single pandas-query expression (e.g. `"age > 18 and country == 'US'"`) |
| `conditions: List[Dict]` | Builder-UI-friendly multi-criteria (e.g. `[{column: age, operator: gt, value: 18}, ...]`) |

The two are different complexity levels. A component should pick one based on its target audience (UI builder vs. CLI/YAML author).

## Date/datetime columns

For columns that hold datetime values:

- New components: prefer the `_column` suffix (`date_column`, `timestamp_column`).
- `_field` suffix (`activity_date_field`, `first_date_field`) is allowed for **semantic differentiation** when one component takes multiple datetime columns with distinct meanings.

`first_date_field` should be auto-derivable from `min(activity_date_field)` when not provided — see `cohort_analysis` for the pattern.

## Output format / mode

These are different concepts:

| Field | Domain |
|---|---|
| `output_mode` | Which **shape** the asset emits — `predictions`, `feature_importance`, `coefficients`, etc. |
| `output_format` | Which **render** of the same data — `text`, `json`, `markdown`, `html`. Common in AI/LLM components. |

Both are valid; use whichever fits the component's intent.

## Preview / sample metadata

Every DataFrame-producing component should expose the same two fields so builder UIs (Astronomer Blueprint, Lakeflow Designer, the registry's web UI) can render an asset preview without warehouse access:

| Field | Type | Default |
|---|---|---|
| `include_preview_metadata` | `bool` | `False` |
| `preview_rows` | `int` (1-500) | `25` |

The metadata key is **`preview`** (singular). The render format is `MetadataValue.md(<df>.to_markdown(index=False))`. For `len(df) > preview_rows * 10`, use `df.sample(preview_rows)` instead of `df.head(preview_rows)` so sorted/filtered outputs are represented by their distribution rather than by the top-of-sort.

Wrap the emission in `try/except` so a markdown rendering bug doesn't kill the asset:

```python
if include_preview and len(df) > 0:
    try:
        _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
        metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
    except Exception as e:
        context.log.warning(f"preview emission failed: {e}")
```

## Catalog / governance fields

All components should accept the standard Dagster catalog fields:

| Field | Type |
|---|---|
| `description` | `Optional[str]` |
| `owners` | `Optional[List[str]]` (`team:foo` or `user@host`) |
| `asset_tags` | `Optional[Dict[str, str]]` |
| `kinds` | `Optional[List[str]]` (auto-inferred if unset) |
| `freshness_max_lag_minutes` | `Optional[int]` |
| `freshness_cron` | `Optional[str]` |
| `column_lineage` | `Optional[Dict[str, List[str]]]` (output-col → list-of-input-cols) |

## Partitioning fields

Two shapes — pick whichever fits your component's complexity. Components
that accept partitions accept both: the flat-fields path is enough for
the common single-axis cases; `partition_dimensions` overrides them when
you need full multi-axis flexibility.

### Flat-fields shape (single-axis or legacy multi)

| Field | Type | Notes |
|---|---|---|
| `partition_type` | `Optional[str]` | `daily` \| `weekly` \| `monthly` \| `hourly` \| `static` \| `dynamic` \| `multi` |
| `partition_start` | `Optional[str]` | ISO date string for time-based types |
| `partition_values` | `Optional[str]` | Comma-separated values for `static` and legacy `multi` |
| `dynamic_partition_name` | `Optional[str]` | Required when `partition_type=dynamic`. Becomes the `name=` on `DynamicPartitionsDefinition`. e.g. `tenants` |
| `partition_date_column` | `Optional[str]` | Column to filter rows by current date partition (in transform/sink components) |
| `partition_static_column` | `Optional[str]` | Column to filter rows by current static partition value |
| `partition_static_dim` | `Optional[str]` | Dimension name for the static axis in legacy `multi` shape |

`partition_type=multi` is the legacy `(date Daily, static_dim Static)`
shape. For richer multi-axis combinations — `(tenant Dynamic, date
Daily)`, `(static, static)`, etc. — use `partition_dimensions` below.

### Multi-axis shape (`partition_dimensions`)

When set, this list of dim specs overrides the flat fields:

```yaml
partition_dimensions:
  - name: tenant
    type: dynamic
    dynamic_partition_name: tenants
  - name: date
    type: daily
    start: "2024-01-01"
```

Each spec accepts:

| Key | Required when | Notes |
|---|---|---|
| `name` | always | Dimension name in the resulting `MultiPartitionsDefinition` |
| `type` | always | `daily` \| `weekly` \| `monthly` \| `hourly` \| `static` \| `dynamic` |
| `start` | time-based types | ISO date string |
| `values` | `static` | List or comma-separated string |
| `dynamic_partition_name` | `dynamic` | Defaults to `name` if omitted |

A single-element `partition_dimensions` produces a single-axis def
(equivalent to the flat-fields shape but with custom dim name). Two or
more produces a `MultiPartitionsDefinition`.

### Implementation

Components include a self-contained `_build_partitions_def(...)` helper
at module top that maps both shapes to a Dagster `partitions_def`. This
is the canonical implementation across the registry — copy it as-is when
authoring a new partition-aware component.

## Retry policy

| Field | Type | Notes |
|---|---|---|
| `retry_policy_max_retries` | `Optional[int]` | Opt-in; if set, defines a `RetryPolicy` |
| `retry_policy_delay_seconds` | `Optional[int]` | Default 1 |
| `retry_policy_backoff` | `str` | `linear` \| `exponential` (default) |

## Output return shape (NOT a field, but a convention)

A DataFrame-producing asset should be **annotated** `-> pd.DataFrame` and **return a bare `df`**, not `Output(value=df, metadata=...)`. The latter raises `DagsterInvariantViolationError` against the bare annotation.

To attach metadata, use `context.add_output_metadata({...})` before the `return df`.

```python
@asset(name=asset_name)
def my_asset(context) -> pd.DataFrame:
    df = ...
    context.add_output_metadata({"row_count": MetadataValue.int(len(df))})
    return df
```

For non-DataFrame producers, return `MaterializeResult(metadata={...})`.

## Renaming fields

Just rename. The registry isn't widely deployed yet, so YAML in the wild is rare. If a renamed field needs backward compatibility later (e.g. after public launch), add `validation_alias=AliasChoices(new, old)` then — but keep the surface clean for now.

---

## Where this list lives

This file is the **source of truth**. It belongs at the repo root so the registry web UI, future component-author docs, and any tooling (linters, scaffolders) can sync against it.
