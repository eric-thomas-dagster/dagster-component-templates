"""SQL Transform Asset Component.

Warehouse-pushdown SQL transform: takes a SELECT statement plus a destination
table name, runs `CREATE TABLE <dest> AS <SELECT>` directly on the warehouse,
no data round-tripped through Python. Intended for teams who live in
Snowflake / BigQuery / Redshift and want a lightweight pushdown asset
without spinning up a full dbt project.

Connection is via a SQLAlchemy URL env var, so any warehouse with a
SQLAlchemy dialect works:
- Snowflake: `snowflake://user:pwd@account/db/schema?warehouse=...`
- BigQuery:  `bigquery://project-id/dataset` (requires sqlalchemy-bigquery)
- Redshift:  `redshift+psycopg2://user:pwd@host:5439/dbname`
- Postgres:  `postgresql://user:pwd@host:5432/dbname`

For multi-step warehouse transforms, prefer `dagster-dbt`. This component
is for the simple "one CTAS asset" case.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class SqlTransformComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a SQL SELECT on the warehouse and materialize the result as a table.

    Example:
        ```yaml
        type: dagster_component_templates.SqlTransformComponent
        attributes:
          asset_name: orders_dedup
          connection_url_env_var: SNOWFLAKE_URL
          destination_table: ANALYTICS.PUBLIC.ORDERS_DEDUP
          sql: |
            SELECT *
            FROM {{ upstream }}
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY order_id ORDER BY updated_at DESC
            ) = 1
          template_vars:
            upstream: ANALYTICS.PUBLIC.ORDERS_RAW
          if_exists: replace
          upstream_asset_keys:
            - orders_raw
          group_name: warehouse_transforms
        ```

    The SQL field is a SELECT body. The component wraps it as either:
    - replace: DROP TABLE IF EXISTS <dest>; CREATE TABLE <dest> AS <sql>
    - append:  INSERT INTO <dest> <sql>

    `template_vars` does simple `{{ name }}` substitution before execution.
    No Jinja, no dbt — just str.replace. Keeps the dependency surface
    minimal and the substitution behavior obvious.
    """

    asset_name: str = Field(description="Dagster asset name")
    connection_url_env_var: str = Field(
        default="DATABASE_URL",
        description=(
            "Env var holding a SQLAlchemy URL. Examples: SNOWFLAKE_URL, "
            "BIGQUERY_URL, REDSHIFT_URL, POSTGRES_URL, or DATABASE_URL (default) "
            "pointed at a local file for a zero-setup demo, e.g. 'duckdb:///demo.db'."
        ),
    )
    destination_table: Optional[str] = Field(
        default=None,
        description=(
            "Fully-qualified destination table. Format depends on the "
            "warehouse: 'DB.SCHEMA.TABLE' for Snowflake, 'project.dataset.table' "
            "(or backticked) for BigQuery, 'schema.table' for Redshift. "
            "Required when return_dataframe=false."
        ),
    )
    sql: str = Field(
        description=(
            "The SELECT body. Will be wrapped in `CREATE TABLE <dest> AS <sql>` "
            "or `INSERT INTO <dest> <sql>` based on if_exists. This string is "
            "resolved by dg's own component-YAML Jinja templating BEFORE this "
            "component ever sees it, same as every other field. A `{{ partition_key }}` "
            "(or `partition_keys`/`partition_key_range_start`/etc.) reference -- which "
            "this component's OWN, separate Jinja render fills in at materialization "
            "time -- must be wrapped in `{% raw %}...{% endraw %}` in the YAML so dg's "
            "pass leaves it untouched: e.g. `WHERE dt = '{% raw %}{{ partition_key }}{% endraw %}'`. "
            "Without it, dg tries to resolve `partition_key` itself at load time (before "
            "a run/partition exists) and fails."
        ),
    )
    template_vars: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Variables to interpolate into the SQL template. Rendered via Jinja2 "
            "when installed (so `{% if ... %}` / `{% for ... %}` work), otherwise "
            "a simple `{{ name }}` str.replace fallback."
        ),
    )
    return_dataframe: bool = Field(
        default=False,
        description=(
            "When true, execute the SQL as a SELECT and return its result as "
            "a DataFrame asset (no CTAS / INSERT). Useful for inline reads "
            "downstream of pandas transforms. When false (default), the SQL "
            "is wrapped in CREATE TABLE / INSERT INTO `destination_table`."
        ),
    )
    if_exists: Literal["replace", "append"] = Field(
        default="replace",
        description=(
            "'replace' issues DROP TABLE IF EXISTS + CREATE TABLE AS. "
            "'append' issues INSERT INTO. Ignored when return_dataframe=true."
        ),
    )
    upstream_asset_keys: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Upstream Dagster asset keys this transform depends on. Typically "
            "external_snowflake_table / external_bigquery_table assets that "
            "represent the source warehouse tables."
        ),
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description=(
            "Asset kinds for the catalog. Auto-inferred from the connection URL "
            "scheme if unset (e.g. 'snowflake', 'bigquery', 'redshift')."
        ),
    )
    group_name: Optional[str] = Field(
        default="warehouse_transforms",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the catalog.",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Key-value tags for the catalog.",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names or email addresses.",
    )
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )


    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} "
            "dicts (same shape as the ExternalXAsset components). Overrides partition_type/"
            "partition_values/partition_static_dim when set -- use this instead of "
            "partition_type='multi' for full control over axis names (partition_type='multi' always "
            "names its two axes 'date' and 'static_dim'/partition_static_dim)."
        ),
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run: the SQL template is "
            "rendered ONCE with partition_key_range_start/partition_key_range_end "
            "and partition_time_window_start/partition_time_window_end spanning the "
            "whole range (write the SQL against the window, not {{ partition_key }}, "
            "which is deliberately left undefined for a multi-partition run so a "
            "template that depends on it fails loudly instead of silently using one "
            "partition). 'multi_run' launches one run per partition (optionally "
            "capped via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. With return_dataframe=true, 'single_run' "
            "requires a partition-range-aware IO manager -- the default filesystem "
            "IO manager cannot persist one output spanning multiple partitions and "
            "will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="multi", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Run a SELECT on the warehouse and materialize the result as a table (CTAS pushdown)."

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        import os

        # Capture for closure
        asset_name = self.asset_name
        url_env_var = self.connection_url_env_var
        destination_table = self.destination_table
        sql_template = self.sql
        template_vars = self.template_vars or {}
        return_dataframe = self.return_dataframe
        if_exists = self.if_exists
        upstream_keys = [
            dg.AssetKey.from_user_string(k) for k in (self.upstream_asset_keys or [])
        ]

        # Infer kinds from the env var name as a hint (best-effort).
        kinds = set(self.kinds or [])
        if not kinds:
            ev = url_env_var.lower()
            for hint in ("snowflake", "bigquery", "redshift", "postgres", "mysql", "trino", "duckdb"):
                if hint in ev:
                    kinds.add(hint)
                    break
            kinds.add("sql")

        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        # Retry policy
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            None, self.partition_dimensions,
            static_dim_name=self.partition_static_dim,
        )
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(freshness_policy=_freshness_policy_obj, 
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            key=dg.AssetKey.from_user_string(asset_name),
            group_name=self.group_name or "warehouse_transforms",
            description=self.description or f"SQL pushdown → {destination_table}",
            tags=tags,
            deps=upstream_keys,
            owners=self.owners or [],
            retry_policy=_retry_policy,
        )
        def _sql_transform_asset(context: dg.AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            try:
                import sqlalchemy
            except ImportError as e:
                raise ImportError(
                    "sqlalchemy is required for sql_transform. Install with `pip install sqlalchemy` "
                    "(plus a warehouse-specific driver: snowflake-sqlalchemy, "
                    "sqlalchemy-bigquery, sqlalchemy-redshift, etc.)."
                ) from e

            url = os.environ.get(url_env_var)
            if not url:
                raise ValueError(
                    f"Env var {url_env_var} is not set. Expected a SQLAlchemy "
                    "connection URL."
                )

            # Auto-inject Dagster context vars into the template environment.
            # Mirrors the Snowflake TemplatedSQLComponent convention so the
            # same partition_key / asset_key bindings work across backends.
            auto_vars: Dict[str, Any] = {
                "asset_key": "/".join(context.asset_key.path),
                "run_id": context.run_id,
            }
            if context.has_partition_key:
                pk = context.partition_key
                auto_vars["partition_key"] = pk
                auto_vars["partition_keys"] = [pk]
            elif context.has_partition_key_range:
                # single_run backfill: one run spans many partitions. Expose the
                # whole list but NOT a single `partition_key` (ambiguous).
                auto_vars["partition_keys"] = list(context.partition_keys)
            if context.has_partition_key or context.has_partition_key_range:
                # Time-window partitions also expose start/end (for a range:
                # start of the first partition .. end of the last)
                try:
                    pk_range = context.partition_key_range
                    auto_vars["partition_key_range_start"] = pk_range.start
                    auto_vars["partition_key_range_end"] = pk_range.end
                except Exception:
                    pass
                try:
                    tw = context.partition_time_window
                    auto_vars["partition_time_window_start"] = tw.start.isoformat()
                    auto_vars["partition_time_window_end"] = tw.end.isoformat()
                except Exception:
                    pass
            # User-supplied vars win on conflict
            merged_vars = {**auto_vars, **template_vars}

            # Render the template. Prefer Jinja2 (supports `{% if %}` / `{% for %}`
            # / filters). Fall back to plain str.replace if Jinja2 isn't installed
            # — backward compatible with the existing simple `{{ name }}` shape.
            try:
                import jinja2
                rendered_sql = jinja2.Template(
                    sql_template, undefined=jinja2.StrictUndefined
                ).render(**merged_vars)
            except ImportError:
                rendered_sql = sql_template
                for k, v in merged_vars.items():
                    rendered_sql = rendered_sql.replace("{{ " + k + " }}", str(v))
                    rendered_sql = rendered_sql.replace("{{" + k + "}}", str(v))

            engine = sqlalchemy.create_engine(url)
            try:
                # Mode 1: return DataFrame (no CTAS / INSERT). Useful for
                # inline reads downstream of pandas transforms.
                if return_dataframe:
                    import pandas as pd
                    context.log.info(
                        f"Executing SELECT (return_dataframe=true):\n{rendered_sql}\n"
                    )
                    with engine.begin() as conn:
                        df = pd.read_sql(sqlalchemy.text(rendered_sql), conn)
                    context.log.info(
                        f"SQL transform complete — returned DataFrame with "
                        f"{len(df)} rows × {len(df.columns)} columns"
                    )
                    context.add_output_metadata({
                        "dagster/row_count": dg.MetadataValue.int(len(df)),
                        "sql": dg.MetadataValue.md(f"```sql\n{rendered_sql}\n```"),
                        "mode": dg.MetadataValue.text("return_dataframe"),
                    })
                    return df

                # Mode 2: CTAS / INSERT INTO <destination_table>
                if not destination_table:
                    raise ValueError(
                        "destination_table is required unless return_dataframe=true."
                    )
                if if_exists == "replace":
                    drop_dml = f"DROP TABLE IF EXISTS {destination_table}"
                    ctas_dml = f"CREATE TABLE {destination_table} AS\n{rendered_sql}"
                    statements = [drop_dml, ctas_dml]
                    op = "CREATE TABLE AS (replace)"
                else:
                    statements = [f"INSERT INTO {destination_table}\n{rendered_sql}"]
                    op = "INSERT INTO (append)"

                with engine.begin() as conn:
                    for stmt in statements:
                        context.log.info(f"Executing:\n{stmt}\n")
                        conn.execute(sqlalchemy.text(stmt))
                    try:
                        result = conn.execute(
                            sqlalchemy.text(f"SELECT COUNT(*) FROM {destination_table}")
                        )
                        rows_written = int(result.scalar() or 0)
                    except Exception:
                        rows_written = -1
            finally:
                engine.dispose()

            context.log.info(
                f"SQL transform complete — {op} on {destination_table} "
                f"({rows_written if rows_written >= 0 else '?'} rows)"
            )

            metadata: Dict[str, Any] = {
                "destination_table": dg.MetadataValue.text(destination_table),
                "operation": dg.MetadataValue.text(op),
                "sql": dg.MetadataValue.md(f"```sql\n{rendered_sql}\n```"),
            }
            if rows_written >= 0:
                metadata["dagster/row_count"] = dg.MetadataValue.int(rows_written)
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_sql_transform_asset])


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    static_dim_name=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            (static_dim_name or "static_dim"): StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
