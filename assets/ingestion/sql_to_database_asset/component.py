"""SQL to Database Asset Component.

Reads rows from a source database table/query and writes them to a destination
database table via SQLAlchemy. Designed to be triggered by sql_monitor or a schedule.

Supports any SQLAlchemy-compatible source and destination (Postgres, MySQL, MSSQL,
SQLite, Snowflake, BigQuery, Redshift, DuckDB, etc.).
"""
from typing import Any, Dict, List, Literal, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, BackfillPolicy, Config
from pydantic import Field



def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


# BEGIN CDC delete-flag coercion (inlined per component; keep copies identical — see tests/test_cdc_delete_semantics.py)
def _is_delete_flag(value) -> bool:
    """True when a delete_column's raw value marks a row for CDC delete. Accepts a real
    bool, a numeric truthy value, or a case-insensitive string marker -- CDC sources vary
    widely in how they encode "this row was deleted" (bool column, 0/1, 'D'/'DELETE', ...)."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("true", "1", "yes", "y", "d", "delete", "deleted")


def _coerce_delete_column(df, delete_column):
    """Returns a copy of df with delete_column coerced to 0/1 ints -- a boolean literal
    that's portable across every SQL dialect this component supports, unlike `true`/`TRUE`
    (rejected by some engines in this comparison position)."""
    df = df.copy()
    df[delete_column] = df[delete_column].apply(_is_delete_flag).astype(int)
    return df
# END CDC delete-flag coercion


# BEGIN upsert-merge-sql (inlined per component; keep copies identical — see tests/test_upsert_merge_sql_drift.py)
def build_merge_sql(schema, table, staging_table, columns, key_columns, quote='"'):
    """Standard-SQL MERGE INTO statement moving staged rows into `table`, updating matched
    rows by `key_columns` and inserting the rest -- the upsert mechanism behind
    `if_exists='upsert'`. Supported natively (confirmed per-platform, not assumed) by
    Snowflake, Databricks SQL/Delta, and Amazon Redshift (AWS added real MERGE support --
    see docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)."""
    def q(name):
        return f"{quote}{name}{quote}"

    full_table = f"{q(schema)}.{q(table)}" if schema else q(table)
    full_staging = f"{q(schema)}.{q(staging_table)}" if schema else q(staging_table)
    on_clause = " AND ".join(f"t.{q(c)} = s.{q(c)}" for c in key_columns)
    non_key_cols = [c for c in columns if c not in key_columns]
    insert_cols = ", ".join(q(c) for c in columns)
    insert_vals = ", ".join(f"s.{q(c)}" for c in columns)
    update_clause = ""
    if non_key_cols:
        set_clause = ", ".join(f"{q(c)} = s.{q(c)}" for c in non_key_cols)
        update_clause = f"WHEN MATCHED THEN UPDATE SET {set_clause} "
    return (
        f"MERGE INTO {full_table} AS t USING {full_staging} AS s ON {on_clause} "
        f"{update_clause}"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
# END upsert-merge-sql


_UPSERT_DIALECT_QUOTE = {"mysql": "`", "mariadb": "`"}


def build_dialect_upsert_sql(dialect, schema, table, staging_table, columns, key_columns):
    """Dialect-appropriate upsert SQL moving staged rows into `table`, keyed by
    `key_columns`. Postgres/SQLite/DuckDB use INSERT...ON CONFLICT; MySQL/MariaDB use
    INSERT...ON DUPLICATE KEY UPDATE (both require a UNIQUE/PRIMARY KEY constraint on
    key_columns at the database level -- this function only builds the SQL, it doesn't
    create one); mssql/oracle/snowflake use a standard MERGE INTO (build_merge_sql).
    Raises for a dialect with no known portable upsert syntax here -- including 'redshift'
    via a plain postgresql:// URL, which this dispatch can't see is actually Redshift (which
    does NOT support ON CONFLICT) -- use dataframe_to_redshift's own upsert for that."""
    quote = _UPSERT_DIALECT_QUOTE.get(dialect, '"')

    def q(name):
        return f"{quote}{name}{quote}"

    full_table = f"{q(schema)}.{q(table)}" if schema else q(table)
    full_staging = f"{q(schema)}.{q(staging_table)}" if schema else q(staging_table)
    non_key_cols = [c for c in columns if c not in key_columns]
    insert_cols = ", ".join(q(c) for c in columns)
    select_cols = ", ".join(q(c) for c in columns)

    if dialect in ("postgresql", "sqlite", "duckdb"):
        conflict_cols = ", ".join(q(c) for c in key_columns)
        if non_key_cols:
            set_clause = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in non_key_cols)
            action = f"DO UPDATE SET {set_clause}"
        else:
            action = "DO NOTHING"
        return (
            f"INSERT INTO {full_table} ({insert_cols}) SELECT {select_cols} FROM {full_staging} "
            # WHERE 1=1 is load-bearing, not decorative: SQLite's parser reads a bare
            # "... FROM x ON CONFLICT" as a continuation of a FROM-join clause and fails
            # with "near 'DO': syntax error" -- confirmed empirically against sqlite3
            # 3.50.4. A trailing WHERE before ON CONFLICT disambiguates it on every
            # dialect in this bucket (Postgres/DuckDB accept it as a no-op filter).
            f"WHERE 1=1 ON CONFLICT ({conflict_cols}) {action}"
        )
    if dialect in ("mysql", "mariadb"):
        if non_key_cols:
            set_clause = ", ".join(f"{q(c)} = VALUES({q(c)})" for c in non_key_cols)
            return (
                f"INSERT INTO {full_table} ({insert_cols}) SELECT {select_cols} FROM {full_staging} "
                f"ON DUPLICATE KEY UPDATE {set_clause}"
            )
        return f"INSERT IGNORE INTO {full_table} ({insert_cols}) SELECT {select_cols} FROM {full_staging}"
    if dialect in ("mssql", "oracle", "snowflake"):
        return build_merge_sql(schema, table, staging_table, columns, key_columns, quote=quote)
    raise ValueError(
        f"sql_to_database_asset: if_exists='upsert' has no known syntax for SQLAlchemy "
        f"dialect {dialect!r}. Supported: postgresql, sqlite, duckdb, mysql, mariadb, "
        f"mssql, oracle, snowflake."
    )


def write_or_upsert(df, dest_table, dst_engine, schema, if_exists, upsert_key_columns, chunksize, run_id, delete_column=None):
    """Write `df` to `dest_table`, or -- when if_exists='upsert' -- write it to a staging
    table and upsert from there by `upsert_key_columns`. Shared by every write call site in
    this component (a normal tick and a combined-range backfill both write once).

    delete_column (CDC apply): when set, rows flagged for delete are removed from the
    TARGET (by key, via a portable DELETE...WHERE EXISTS correlated subquery against
    staging -- plain ANSI SQL, not a dialect extension) and dropped from staging before the
    normal upsert runs, so they're never re-inserted -- including a delete for a key the
    target never had (it's simply absent from staging afterward, so the upsert below has
    nothing to insert for it)."""
    if if_exists != "upsert":
        df.to_sql(dest_table, con=dst_engine, schema=schema, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
        return
    if not upsert_key_columns:
        raise ValueError("sql_to_database_asset: if_exists='upsert' requires upsert_key_columns.")
    import sqlalchemy

    if delete_column:
        if delete_column not in df.columns:
            raise ValueError(f"delete_column={delete_column!r} is not a column in the DataFrame.")
        df = _coerce_delete_column(df, delete_column)

    dialect = dst_engine.dialect.name
    quote = _UPSERT_DIALECT_QUOTE.get(dialect, '"')
    full_target = f"{quote}{schema}{quote}.{quote}{dest_table}{quote}" if schema else f"{quote}{dest_table}{quote}"
    target_exists = sqlalchemy.inspect(dst_engine).has_table(dest_table, schema=schema)

    stage_table = f"{dest_table}_stage_{run_id.replace('-', '')[:12]}"
    full_stage = f"{quote}{schema}{quote}.{quote}{stage_table}{quote}" if schema else f"{quote}{stage_table}{quote}"

    # Auto-create path (target doesn't exist yet): a delete-flagged row has nothing to
    # delete and must never be inserted -- strip it before it ever reaches the table
    # that becomes the target via rename below.
    stage_df = df[df[delete_column] == 0] if (delete_column and not target_exists) else df
    stage_df.to_sql(stage_table, con=dst_engine, schema=schema, if_exists="replace", index=False, method="multi", chunksize=chunksize)

    with dst_engine.begin() as conn:
        if not target_exists:
            conn.execute(sqlalchemy.text(f"ALTER TABLE {full_stage} RENAME TO {quote}{dest_table}{quote}"))
            if dialect in ("postgresql", "sqlite", "duckdb", "mysql", "mariadb"):
                # ON CONFLICT / ON DUPLICATE KEY UPDATE both require a UNIQUE index on the
                # key columns to even parse -- pandas' own auto-created table never has
                # one, so a freshly-created target needs one added before any later
                # upsert run can match rows by key. A unique INDEX (not ALTER TABLE ADD
                # CONSTRAINT) is the one mechanism all 5 of these dialects share --
                # SQLite in particular has no ALTER TABLE ADD CONSTRAINT at all.
                # No "IF NOT EXISTS": this only ever runs once, right after the table
                # itself was first created, so the index can't already exist -- and
                # skipping it sidesteps MySQL<8.0's lack of CREATE INDEX IF NOT EXISTS.
                key_cols_sql = ", ".join(f"{quote}{c}{quote}" for c in upsert_key_columns)
                conn.execute(sqlalchemy.text(
                    f"CREATE UNIQUE INDEX {quote}{dest_table}_upsert_key{quote} "
                    f"ON {full_target} ({key_cols_sql})"
                ))
        else:
            if delete_column:
                on_clause = " AND ".join(
                    f"{full_target}.{quote}{c}{quote} = {full_stage}.{quote}{c}{quote}"
                    for c in upsert_key_columns
                )
                conn.execute(sqlalchemy.text(
                    f"DELETE FROM {full_target} WHERE EXISTS (SELECT 1 FROM {full_stage} "
                    f"WHERE {on_clause} AND {full_stage}.{quote}{delete_column}{quote} = 1)"
                ))
                conn.execute(sqlalchemy.text(
                    f"DELETE FROM {full_stage} WHERE {quote}{delete_column}{quote} = 1"
                ))
            merge_sql = build_dialect_upsert_sql(dialect, schema, dest_table, stage_table, list(stage_df.columns), upsert_key_columns)
            conn.execute(sqlalchemy.text(merge_sql))
            conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {full_stage}"))


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



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


class SQLToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read from a source database and write to a destination database table.

    Supports full-table copy, incremental watermark-based loads, and custom SQL.
    Triggered by sql_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_community_components.SQLToDatabaseAssetComponent
        attributes:
          asset_name: crm_contacts_sync
          source_url_env_var: SOURCE_DB_URL
          destination_url_env_var: DESTINATION_DB_URL
          source_table: contacts
          destination_table: raw_contacts
          watermark_column: updated_at
          watermark_env_var: CONTACTS_WATERMARK
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    source_url_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy database URL. Set this OR source_resource_key.")
    destination_url_env_var: Optional[str] = Field(default=None, description="Env var with destination SQLAlchemy database URL. Set this OR destination_resource_key.")
    source_resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource to use for the source connection instead of source_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    destination_resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource to use for the destination connection instead of destination_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    source_table: Optional[str] = Field(default=None, description="Source table name (use source_table OR source_query)")
    source_schema: Optional[str] = Field(default=None, description="Source schema name")
    source_query: Optional[str] = Field(default=None, description="Custom SQL query (overrides source_table)")
    destination_table: str = Field(description="Destination table name")
    destination_schema: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, append, or upsert (requires upsert_key_columns)")
    upsert_key_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Key columns identifying a row for if_exists='upsert' (ADF's SQL sink "
            "writeBehavior='upsert' + upsertSettings.keys). Writes to a staging table, "
            "then upserts by these keys using syntax chosen by the destination's "
            "SQLAlchemy dialect: INSERT...ON CONFLICT (postgresql/sqlite/duckdb), "
            "INSERT...ON DUPLICATE KEY UPDATE (mysql/mariadb), or MERGE INTO "
            "(mssql/oracle/snowflake). A Redshift destination connected via a plain "
            "postgresql:// URL is treated as Postgres (ON CONFLICT), which Redshift does "
            "NOT support -- use dataframe_to_redshift's own upsert for a Redshift "
            "destination instead. The target table is auto-created on first write."
        ),
    )
    delete_column: Optional[str] = Field(
        default=None,
        description=(
            "Column whose truthy value (True, 1, 'true', 'yes', 'y', 'd', 'delete', "
            "'deleted' -- case-insensitive) marks a row for DELETE by upsert_key_columns, "
            "instead of upsert -- true CDC apply (insert + update + delete), not just "
            "insert+update. Only used with if_exists='upsert'. A delete-flagged row is "
            "removed from the destination if matched, and never inserted if unmatched "
            "(e.g. a delete arriving for a key the destination never had, or for a table "
            "that doesn't exist yet -- auto-create then skips it entirely). The column is "
            "written into the destination table like any other column (a CDC audit "
            "trail), not stripped."
        ),
    )
    watermark_column: Optional[Union[str, int]] = Field(default=None, description="Incremental watermark column (e.g. updated_at, id)")
    watermark_env_var: Optional[str] = Field(default=None, description="Env var storing the last watermark value")
    chunksize: int = Field(default=10000, description="Rows to read/write per chunk")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, monthly, hourly, static, multi, or dynamic")

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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
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

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the written rows in the materialization metadata.",
    )

    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run. For time-based "
            "partition types (daily/weekly/monthly/hourly) with a "
            "watermark_column set, this is ONE combined bounded-range query "
            "spanning the whole window (efficient -- not one query per day); "
            "destination_table may not contain '{partition_key}' in this "
            "combination (ambiguous: one combined query can't be split across "
            "multiple destination tables). For static/dynamic/multi partition "
            "types (no natural time window), each partition key is read and "
            "written independently -- destination_table MAY vary per key via "
            "'{partition_key}' templating. 'multi_run' launches one run per "
            "partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is 'none'. Unlike dlt-based ingestion "
            "connectors, this asset writes directly to SQL and returns no "
            "IO-managed value, so 'single_run' works with any IO manager."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "Log what would be read/written (source query, destination table, watermark/partition "
            "window) and skip the real database connections entirely -- same convention as the "
            "SSIS/Synapse/SAP job-trigger components' own dry_run flag."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SQL To Database Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        partitions_def = _build_partitions_def(
            _self.partition_type if _self.partition_type != "none" else None,
            _self.partition_start or _self.partition_start_date,
            _self.partition_values,
            _self.dynamic_partition_name,
            _self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        source_desc = _self.source_query[:50] + "..." if _self.source_query else (
            f"{_self.source_schema + '.' if _self.source_schema else ''}{_self.source_table}"
        )

        class SQLRunConfig(Config):
            watermark_value: Optional[str] = None  # override watermark at runtime

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        _backfill_policy_obj = None
        if partitions_def is not None and _self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and _self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=_self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(freshness_policy=_freshness_policy_obj, retry_policy=_retry_policy,
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"SQL:{source_desc} → {_self.destination_table}",
            group_name=_self.group_name,
            kinds={"sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            required_resource_keys={
                k for k in (_self.source_resource_key, _self.destination_resource_key) if k
            },
        )
        def sql_to_database_asset(context: AssetExecutionContext, config: SQLRunConfig):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            if _self.dry_run:
                context.log.info(
                    f"[DRY RUN] would read {source_desc!r} and write to "
                    f"{_self.destination_table!r} (if_exists={_self.if_exists}, "
                    f"watermark_column={_self.watermark_column})"
                )
                return dg.MaterializeResult(metadata={"source": source_desc, "table": _self.destination_table, "status": "dry_run — not executed"})
            import os
            import pandas as pd
            from sqlalchemy import create_engine, text

            def _resolve_db_url(resource_key, url_env_var, side):
                """Resolve a SQLAlchemy-compatible database URL: from a registered
                resource (WHERE to connect) if resource_key is set and the resource
                exposes 'connection_string', otherwise from url_env_var. The write/
                read mechanics (HOW) are unchanged either way."""
                if resource_key:
                    resource = getattr(context.resources, resource_key, None)
                    if resource is None:
                        raise ValueError(f"Resource {resource_key!r} is not registered in this project's Definitions.")
                    url = getattr(resource, "connection_string", None)
                    if not url:
                        raise ValueError(
                            f"Resource {resource_key!r} ({type(resource).__name__}) does not expose a "
                            f"'connection_string' property, so it can't be used to determine the {side} "
                            f"database URL automatically. Use {side}_url_env_var instead."
                        )
                    return url
                if url_env_var:
                    if url_env_var not in os.environ:
                        raise KeyError(f"Env var '{url_env_var}' is not set")
                    return _require_env(url_env_var)
                raise ValueError(f"Set '{side}_resource_key' or '{side}_url_env_var'.")

            src_url = _resolve_db_url(_self.source_resource_key, _self.source_url_env_var, "source")
            dst_url = _resolve_db_url(_self.destination_resource_key, _self.destination_url_env_var, "destination")

            src_engine = create_engine(src_url)
            dst_engine = create_engine(dst_url)

            # Build source query
            if _self.source_query:
                base_query = _self.source_query
            else:
                src_table = f"{_self.source_schema + '.' if _self.source_schema else ''}{_self.source_table}"
                base_query = f"SELECT * FROM {src_table}"

            def _process_one(pk: Optional[str]):
                """Read + write one partition key's batch (or the whole table
                when pk is None). Time-based partitions get a real bounded
                [start, end) window instead of an open-ended watermark --
                otherwise every partition after the first would re-pull an
                ever-growing superset instead of just its own slice. Matches
                the original (pre-range) single-tick behavior exactly."""
                query = base_query
                watermark = config.watermark_value
                if not watermark and _self.watermark_env_var:
                    watermark = os.environ.get(_self.watermark_env_var)

                range_start = None
                range_end = None
                if pk is not None:
                    watermark = pk
                    try:
                        _window = context.partition_time_window
                        range_start = _window.start.isoformat()
                        range_end = _window.end.isoformat()
                    except Exception:
                        pass  # static/dynamic/multi partition -- no natural time window

                use_bounded_range = range_start is not None and _self.watermark_column and not _self.source_query

                if use_bounded_range:
                    query += f" WHERE {_self.watermark_column} >= :range_start AND {_self.watermark_column} < :range_end"
                elif watermark and _self.watermark_column and not _self.source_query:
                    query += f" WHERE {_self.watermark_column} > :watermark"

                context.log.info(f"Reading from source: {source_desc}")
                if use_bounded_range:
                    context.log.info(f"Partition window: {_self.watermark_column} in [{range_start}, {range_end})")
                elif watermark and _self.watermark_column:
                    context.log.info(f"Watermark: {_self.watermark_column} > {watermark}")

                with src_engine.connect() as src_conn:
                    if use_bounded_range:
                        df = pd.read_sql(text(query), src_conn, params={"range_start": range_start, "range_end": range_end})
                    elif watermark and _self.watermark_column and not _self.source_query:
                        df = pd.read_sql(text(query), src_conn, params={"watermark": watermark})
                    else:
                        df = pd.read_sql(text(query), src_conn)

                context.log.info(f"Read {len(df)} rows, {len(df.columns)} columns from source")

                if _self.column_mapping:
                    df = df.rename(columns=_self.column_mapping)

                if pk is not None:
                    df["_partition_key"] = pk

                dest_table = _self.destination_table
                if pk is not None and "{partition_key}" in dest_table:
                    dest_table = dest_table.replace("{partition_key}", pk)

                write_or_upsert(
                    df, dest_table, dst_engine, _self.destination_schema, _self.if_exists,
                    _self.upsert_key_columns, _self.chunksize, context.run.run_id,
                    delete_column=_self.delete_column,
                )

                qualified = f"{_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}"
                context.log.info(f"Wrote {len(df)} rows to {qualified}")
                watermark_meta = (
                    {"partition_window_start": range_start, "partition_window_end": range_end}
                    if use_bounded_range
                    else ({"watermark": watermark} if watermark else {})
                )
                return df, qualified, watermark_meta

            # Partition key(s) this call must process: a normal tick gives
            # exactly one (or none); a single_run/multi_run backfill gives
            # the whole range.
            partition_keys: List[Optional[str]] = [None]
            is_range = False
            if context.has_partition_key or context.has_partition_key_range:
                partition_keys = list(context.partition_keys)
                is_range = context.has_partition_key_range

            if not is_range:
                # Normal tick (0 or 1 partition key) -- identical to the
                # original (pre-backfill_policy) behavior.
                df, qualified, watermark_meta = _process_one(partition_keys[0])
                extra_metadata = {"table": qualified, **watermark_meta}
                preview_df = df
            else:
                # A single_run/multi_run backfill range. If the WHOLE range
                # has one natural bounded time window (time-based partition
                # types with watermark_column set), one combined query is
                # both correct and far more efficient than looping per key.
                try:
                    _window = context.partition_time_window
                    _range_start = _window.start.isoformat()
                    _range_end = _window.end.isoformat()
                except Exception:
                    _range_start = _range_end = None

                use_combined_range = (
                    _range_start is not None and _self.watermark_column and not _self.source_query
                )
                if use_combined_range:
                    if "{partition_key}" in _self.destination_table:
                        raise ValueError(
                            "destination_table contains '{partition_key}' but this run "
                            "covers a partition RANGE (single_run/multi_run backfill) over "
                            "a bounded time window -- a single combined query can't be "
                            "split across multiple destination tables in one run. Remove "
                            "the {partition_key} templating, or use a static/dynamic "
                            "partition_type if per-partition destination tables are needed."
                        )
                    query = base_query + f" WHERE {_self.watermark_column} >= :range_start AND {_self.watermark_column} < :range_end"
                    context.log.info(f"Reading from source: {source_desc}")
                    context.log.info(
                        f"Partition window: {_self.watermark_column} in [{_range_start}, {_range_end})"
                    )
                    with src_engine.connect() as src_conn:
                        df = pd.read_sql(
                            text(query), src_conn,
                            params={"range_start": _range_start, "range_end": _range_end},
                        )
                    context.log.info(f"Read {len(df)} rows, {len(df.columns)} columns from source")
                    if _self.column_mapping:
                        df = df.rename(columns=_self.column_mapping)
                    dest_table = _self.destination_table
                    write_or_upsert(
                        df, dest_table, dst_engine, _self.destination_schema, _self.if_exists,
                        _self.upsert_key_columns, _self.chunksize, context.run.run_id,
                        delete_column=_self.delete_column,
                    )
                    qualified = f"{_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}"
                    context.log.info(f"Wrote {len(df)} rows to {qualified}")
                    extra_metadata = {
                        "table": qualified,
                        "partition_window_start": _range_start,
                        "partition_window_end": _range_end,
                    }
                    preview_df = df
                else:
                    # Static/dynamic/multi partitioning with no time window --
                    # each key is read + written independently (destination
                    # table may legitimately differ per key, e.g. one table
                    # per customer, same as _to_database_asset components).
                    dfs = []
                    tables_written: List[str] = []
                    for pk in partition_keys:
                        df_pk, qualified_pk, _ = _process_one(pk)
                        dfs.append(df_pk)
                        if qualified_pk not in tables_written:
                            tables_written.append(qualified_pk)
                    df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
                    extra_metadata = {"table": ", ".join(tables_written)}
                    preview_df = df

            _preview_metadata = {}
            if _self.include_preview_metadata:
                _prev_df = preview_df.sample(min(_self.preview_rows, len(preview_df))) if len(preview_df) > _self.preview_rows * 10 else preview_df.head(_self.preview_rows)
                _cols = list(_prev_df.columns)
                _preview_metadata["preview"] = dg.MetadataValue.md(
                    "| " + " | ".join(_cols) + " |\n"
                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +
                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))
                )

            return dg.MaterializeResult(metadata={**_preview_metadata,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "source": source_desc,
                **extra_metadata,
            })

        return dg.Definitions(assets=[sql_to_database_asset])


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
