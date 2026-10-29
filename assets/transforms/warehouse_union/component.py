"""WarehouseUnionComponent — stack N tables vertically via CTAS UNION ALL.

Equivalent to `dataframe_union` but executed AS SQL. The warehouse engine
unions and writes the result to a new table. No data through Python.

Supports both UNION ALL (default, keeps duplicates) and UNION DISTINCT.
"""
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


_SUPPORTED_DIALECTS = {"duckdb", "postgres", "postgresql", "snowflake", "bigquery",
                        "redshift", "databricks", "mssql", "mysql"}


def _quote(ident: str, dialect: str) -> str:
    if ident.lstrip().startswith("("):  # an inline sliced-source subquery
        return ident
    parts = ident.split(".")
    if dialect == "mssql":
        return ".".join(f"[{p}]" for p in parts)
    if dialect == "mysql":
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _ctas_union(output_table: str, upstream_tables: List[str], distinct: bool,
                 select_cols: Optional[List[str]], mode: str, dialect: str) -> Optional[str]:
    if not upstream_tables or len(upstream_tables) < 2:
        raise ValueError("warehouse_union: provide at least 2 upstream_tables.")
    op = "UNION" if distinct else "UNION ALL"
    cols = ", ".join(select_cols) if select_cols else "*"
    selects = [f"SELECT {cols} FROM {_quote(t, dialect)}" for t in upstream_tables]
    select_sql = f"\n  {op}\n".join(selects)

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS\n  {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n  {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")



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


class WarehouseUnionComponent(Component, Model, Resolvable):
    """Stack N tables vertically in the warehouse via UNION ALL (or UNION DISTINCT).

    All `upstream_tables` must have a compatible schema (same column count + types).
    Use `select_cols:` to project a common subset when schemas only partially overlap.

    Example:
        type: dagster_community_components.WarehouseUnionComponent
        attributes:
          asset_name: orders_all_regions
          database_url: snowflake://...
          dialect: snowflake
          upstream_tables:
            - raw.orders_us
            - raw.orders_eu
            - raw.orders_apac
          output_table: analytics.orders_all_regions
          distinct: false        # default — UNION ALL (keep duplicates)
          select_cols:
            - order_id
            - customer_id
            - amount
            - region
          mode: replace
          deps: [orders_us_in_warehouse, orders_eu_in_warehouse, orders_apac_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_tables: List[str] = Field(description="List of table names to union (≥ 2).")
    output_table: str = Field(description="Destination table name")
    distinct: bool = Field(default=False, description="True → UNION (drops duplicates). False (default) → UNION ALL.")
    select_cols: Optional[List[str]] = Field(
        default=None,
        description="Optional explicit column projection. Use when input schemas only partially overlap.",
    )
    mode: str = Field(default="replace", description="'replace' or 'create_if_not_exists'")
    group_name: Optional[str] = Field(default=None)
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic', or None for unpartitioned. A partitioned run reads only its slice of the source and replaces only that slice of the output (delete the slice, insert the new rows).",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the row-filter "
            "logic below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time partitions.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated keys for static partitions, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column holding each row's date. A time-partitioned run reads only the source rows in its window and replaces only those rows of the output, so the output must keep this column.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column holding each row's partition key (static/dynamic partitions).",
    )
    partition_column_type: str = Field(
        default="timestamp",
        description="'timestamp' (default) or 'date': the type of partition_date_column, which decides how the window is compared (UTC).",
    )
    partition_output_column: Optional[str] = Field(
        default=None,
        description="Column of the output table that identifies a row's partition (default: the date/static column). Set it when the transform renames or derives that column.",
    )
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=200)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @classmethod
    def get_description(cls) -> str:
        return "Stack N tables vertically in the warehouse via UNION [ALL]. No Python materialization."

    def _resolve_url(self) -> str:
        import os
        if self.database_url:
            return self.database_url
        if self.database_url_env_var:
            v = os.environ.get(self.database_url_env_var)
            if not v:
                raise EnvironmentError(f"Env var {self.database_url_env_var!r} is not set")
            return v
        raise ValueError("Set either 'database_url' or 'database_url_env_var'")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        dialect = self.dialect.lower()
        if dialect not in _SUPPORTED_DIALECTS:
            raise ValueError(f"dialect={self.dialect!r} not supported. Use one of {sorted(_SUPPORTED_DIALECTS)}.")
        asset_name = self.asset_name
        upstream_tables = list(self.upstream_tables)
        output_table = self.output_table
        distinct = self.distinct
        select_cols = list(self.select_cols) if self.select_cols else None
        mode = self.mode.lower()
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        kinds = list(self.kinds or []) or [dialect, "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""
        resolve_url = self._resolve_url

        _validate_partition_config(self.partition_type, self.partition_date_column, self.partition_static_column)
        partitions_def = _build_partitions_def(self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, None,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_column_type = self.partition_column_type
        partition_output_column = self.partition_output_column

        @asset(partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _warehouse_union_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            _part = _PartitionScope(context, dialect, output_table, upstream_tables, partition_date_column, partition_column_type,
                                    partition_static_column, partition_output_column, _quote)
            _ins, _out = _part.names()
            sql = _ctas_union(_out, _ins, distinct, select_cols, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(_out, dialect)}")
                    sql = _ctas_union(_out, _ins, distinct, select_cols,
                                       "create_if_not_exists", dialect)
                context.log.info(f"CTAS UNION ({'distinct' if distinct else 'all'}): {len(upstream_tables)} tables")
                conn.exec_driver_sql(sql)
                _part.swap(conn)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "warehouse/upstream_count": MetadataValue.int(len(upstream_tables)),
                }
                if include_preview and row_count > 0:
                    try:
                        prev_rows = conn.exec_driver_sql(
                            f"SELECT * FROM {_quote(output_table, dialect)} LIMIT {preview_rows}"
                        ).fetchall()
                        if prev_rows:
                            cols = list(prev_rows[0]._mapping.keys())
                            metadata["preview"] = MetadataValue.md(
                                "| " + " | ".join(cols) + " |\n"
                                "| " + " | ".join(["---"] * len(cols)) + " |\n" +
                                "\n".join("| " + " | ".join(str(v) for v in r) + " |" for r in prev_rows)
                            )
                    except Exception as e:
                        context.log.warning(f"preview emission failed: {e}")
            return dg.MaterializeResult(metadata=metadata)

        return Definitions(assets=[_warehouse_union_asset])


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder


# BEGIN warehouse partition helpers (inlined per component; keep copies identical, see tests/test_partition_warehouse.py)
def _validate_partition_config(partition_type, date_column, static_column):
    """A partitioned warehouse transform must know which column carries the partition, or it would append full copies."""
    if not partition_type:
        return
    if partition_type == "multi":
        raise ValueError("partition_type='multi' is not supported by warehouse transforms; use a time or static partition.")
    if partition_type in ("daily", "weekly", "monthly", "hourly"):
        if not date_column:
            raise ValueError(f"partition_type={partition_type!r} needs partition_date_column (the column holding each row's date).")
    elif not static_column:
        raise ValueError(f"partition_type={partition_type!r} needs partition_static_column (the column holding each row's partition key).")


class _PartitionScope:
    """Partitioned CTAS: read only the run's slice of the source, build the result in a scratch table, then replace only
    that slice of the output table (delete the slice, insert the new rows) inside the caller's transaction.

    An unpartitioned run is untouched: `names()` returns the configured tables and `swap()` does nothing."""

    def __init__(self, context, dialect, output_table, input_tables, date_column, column_type, static_column, output_column, quote):
        self.dialect, self.output_table, self.input_tables, self.quote = dialect, output_table, list(input_tables), quote
        self.active = bool(context.has_partition_key or context.has_partition_key_range)
        self.date_column, self.static_column = date_column, static_column
        self.column_type = column_type or "timestamp"
        self.output_column = output_column or date_column or static_column
        self.run_id = getattr(context, "run_id", "run")[:8]
        self.keys = [str(k) for k in context.partition_keys] if self.active else []
        self.window = None
        if self.active and date_column:
            try:
                self.window = context.partition_time_window
            except Exception:
                self.window = None
        parts = output_table.split(".")
        self.scratch = ".".join(parts[:-1] + [f"_dagster_part_{self.run_id}_{parts[-1]}"])

    def _slice(self, column):
        from datetime import timezone

        conds = []
        if self.window is not None:
            kind = "DATE" if self.column_type == "date" else {"mysql": "DATETIME", "mssql": "DATETIME2"}.get(self.dialect, "TIMESTAMP")
            fmt = "%Y-%m-%d" if self.column_type == "date" else "%Y-%m-%d %H:%M:%S"
            lo, hi = (t.astimezone(timezone.utc).strftime(fmt) for t in (self.window.start, self.window.end))
            conds.append(f"{column} >= CAST('{lo}' AS {kind}) AND {column} < CAST('{hi}' AS {kind})")
        elif self.static_column:
            values = ", ".join("'" + k.replace("'", "''") + "'" for k in self.keys)
            conds.append(f"{self.static_column} IN ({values})")
        else:
            raise ValueError("partitioned run, but no partition column applies (a time partition needs partition_date_column)")
        return " AND ".join(conds)

    def names(self, alias=True):
        """(source refs, output table) to build the CTAS from: the sliced sources and a scratch output when partitioned."""
        if not self.active:
            return self.input_tables, self.output_table
        pred = self._slice(self.date_column if self.window is not None else self.static_column)
        tail = " AS _dagster_src" if alias else ""
        refs = [f"(SELECT * FROM {self.quote(t, self.dialect)} WHERE {pred}){tail}" for t in self.input_tables]
        return refs, self.scratch

    def swap(self, conn):
        """Replace the output's slice with the scratch rows, then drop the scratch table."""
        if not self.active:
            return
        out, scratch = self.quote(self.output_table, self.dialect), self.quote(self.scratch, self.dialect)
        # a SQLAlchemy connection or a session wrapper exposing execute()/rows_and_columns()
        wrapped = hasattr(conn, "rows_and_columns")
        run = conn.execute if wrapped else conn.exec_driver_sql
        try:
            if wrapped:
                cols = list(conn.rows_and_columns(f"SELECT * FROM {scratch} WHERE 1=0")[1])
            else:
                cols = list(conn.exec_driver_sql(f"SELECT * FROM {scratch} WHERE 1=0").keys())
            column = self.output_column
            if not any(c.lower() == str(column).strip('"`[]').lower() for c in cols):
                raise ValueError(
                    f"the transform's output has no column {column!r}, so the partition's rows cannot be replaced. "
                    "Keep that column in the output (for a summarize, add it to group_by) or set partition_output_column.")
            run(f"CREATE TABLE IF NOT EXISTS {out} AS SELECT * FROM {scratch} WHERE 1=0")
            run(f"DELETE FROM {out} WHERE {self._slice(column)}")
            names = ", ".join(self.quote(c, self.dialect) for c in cols)
            run(f"INSERT INTO {out} ({names}) SELECT {names} FROM {scratch}")
        finally:
            run(f"DROP TABLE IF EXISTS {scratch}")
# END warehouse partition helpers
