"""SqlServerChangeTrackingAssetComponent.

Reads changed rows from a SQL Server table with Change Tracking enabled
(`ALTER TABLE ... ENABLE CHANGE_TRACKING`), using SQL Server's own
`CHANGETABLE(CHANGES ...)` function -- not a generic polling diff, the real
native CDC-lite mechanism SQL Server has shipped since 2008.

How the cursor (last synced version) is tracked: this asset reads its OWN
previous materialization's metadata (`instance.get_asset_records([self_key])`
-> `.asset_entry.last_materialization_record` -> output metadata `ct_version`)
rather than external state storage -- the same pattern this registry already
uses for `openlineage_export_job`'s column-lineage lookups. No prior
materialization means a first run: do a full table load instead of
`CHANGETABLE`, with the current `CHANGE_TRACKING_CURRENT_VERSION()` as the
new baseline.

A real correctness nuance handled explicitly: SQL Server only retains change
history back to `CHANGE_TRACKING_MIN_VALID_VERSION(table)` (bounded by the
table's `CHANGE_RETENTION` setting). If the stored cursor is older than that
-- the server cleaned up history since the last run -- `CHANGETABLE` would
either error or silently miss changes. This checks that explicitly and falls
back to a full reload with a warning log, rather than returning wrong data.

Real, verified T-SQL (CHANGE_TRACKING_CURRENT_VERSION, CHANGE_TRACKING_
MIN_VALID_VERSION, CHANGETABLE(CHANGES ...), SYS_CHANGE_OPERATION/
SYS_CHANGE_VERSION) -- a stable, long-documented SQL Server feature, not
from a web API that could drift. `columns` lists every base-table column to
track besides the primary key, since `CHANGETABLE`'s own columns (change
metadata + the primary key only -- it does not carry other column values)
must be joined back to the base table, which needs explicit column names
rather than `SELECT *`.
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import dagster as dg
from pydantic import Field


def _quote_ident(name: str) -> str:
    return f"[{name}]"


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _build_current_version_sql() -> str:
    return "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version"


def _build_min_valid_version_sql(schema: str, table: str) -> str:
    return f"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{schema}.{table}')) AS min_valid_version"


def _build_full_load_sql(schema: str, table: str, pk_columns: List[str], columns: List[str]) -> str:
    all_cols = list(pk_columns) + [c for c in columns if c not in pk_columns]
    cols_sql = ", ".join(_quote_ident(c) for c in all_cols)
    return f"SELECT {cols_sql} FROM {_qualified_table(schema, table)}"


def _build_incremental_sql(schema: str, table: str, pk_columns: List[str], columns: List[str]) -> str:
    qtable = _qualified_table(schema, table)
    pk_select = ", ".join(f"CT.{_quote_ident(c)}" for c in pk_columns)
    other_cols = [c for c in columns if c not in pk_columns]
    other_select = ", ".join(f"T.{_quote_ident(c)}" for c in other_cols)
    select_cols = pk_select + (f", {other_select}" if other_select else "")
    join_on = " AND ".join(f"CT.{_quote_ident(c)} = T.{_quote_ident(c)}" for c in pk_columns)
    return (
        f"SELECT CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, {select_cols} "
        f"FROM CHANGETABLE(CHANGES {qtable}, :since_version) AS CT "
        f"LEFT JOIN {qtable} AS T ON {join_on} "
        "ORDER BY CT.SYS_CHANGE_VERSION"
    )


def _fetchall_as_df(result) -> pd.DataFrame:
    rows = result.fetchall()
    cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


def _read_changes(conn, schema: str, table: str, pk_columns: List[str], columns: List[str],
                   last_version: Optional[int], log) -> "tuple[pd.DataFrame, int]":
    """conn: a SQLAlchemy Connection (or any object whose .execute(sa.text(...), params) returns a
    result with .fetchall()/.keys()). Returns (changes_df, new_version_to_persist)."""
    import sqlalchemy as sa

    current_version = int(conn.execute(sa.text(_build_current_version_sql())).fetchone()[0])

    if last_version is None:
        log.info(f"sql_server_change_tracking_asset: no prior ct_version found for {schema}.{table} -- full initial load.")
        df = _fetchall_as_df(conn.execute(sa.text(_build_full_load_sql(schema, table, pk_columns, columns))))
        df["sys_change_operation"] = "I"
        return df, current_version

    min_valid_row = conn.execute(sa.text(_build_min_valid_version_sql(schema, table))).fetchone()
    min_valid_version = int(min_valid_row[0]) if min_valid_row and min_valid_row[0] is not None else None

    if min_valid_version is not None and last_version < min_valid_version:
        log.warning(
            f"sql_server_change_tracking_asset: stored ct_version={last_version} is older than "
            f"CHANGE_TRACKING_MIN_VALID_VERSION={min_valid_version} for {schema}.{table} -- change "
            "history has been cleaned up since the last run. Falling back to a full reload instead of "
            "risking missed changes."
        )
        df = _fetchall_as_df(conn.execute(sa.text(_build_full_load_sql(schema, table, pk_columns, columns))))
        df["sys_change_operation"] = "I"
        return df, current_version

    df = _fetchall_as_df(conn.execute(sa.text(_build_incremental_sql(schema, table, pk_columns, columns)), {"since_version": last_version}))
    return df, current_version


def _make_engine(url: str):
    import sqlalchemy as sa
    return sa.create_engine(url, poolclass=sa.pool.NullPool)


def _get_last_ct_version(instance, asset_key) -> Optional[int]:
    """Read this asset's own previous materialization's ct_version metadata -- the cursor for the
    next incremental read. None means "no prior run / first materialization"."""
    try:
        records = instance.get_asset_records([asset_key])
    except Exception:
        return None
    if not records:
        return None
    last_mat = records[0].asset_entry.last_materialization_record
    if last_mat is None:
        return None
    try:
        metadata = last_mat.event_log_entry.dagster_event.event_specific_data.materialization.metadata
    except Exception:
        return None
    value = metadata.get("ct_version") if metadata else None
    return int(value.value) if value is not None else None


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


class SqlServerChangeTrackingAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read changed rows from a SQL Server Change-Tracking-enabled table.

    Requires `ALTER TABLE <schema>.<table> ENABLE CHANGE_TRACKING` (and
    `ALTER DATABASE ... SET CHANGE_TRACKING = ON`) already set up on the
    SQL Server side -- this component only reads, it never enables tracking
    itself.
    """

    asset_name: str = Field(description="Dagster asset key.")
    connection_url_env_var: str = Field(description="Env var holding a SQLAlchemy connection URL, e.g. 'mssql+pyodbc://user:pass@dsn'.")
    schema_name: str = Field(description="SQL Server schema the table lives in, e.g. 'dbo'.")
    table_name: str = Field(description="Table name (without schema).")
    primary_key_columns: List[str] = Field(description="The table's primary key column(s), in order. Required -- CHANGETABLE only carries these plus change metadata; other columns are joined back from the base table.")
    columns: List[str] = Field(description="Every column to read, including primary_key_columns. Required -- CHANGETABLE can't be queried with SELECT *.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['mssql', 'cdc'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.primary_key_columns:
            raise ValueError("primary_key_columns must be non-empty.")
        missing_pk = [c for c in self.primary_key_columns if c not in self.columns]
        if missing_pk:
            raise ValueError(f"primary_key_columns {missing_pk} must also be listed in columns.")

        _self = self
        _inferred_kinds = list(self.kinds or ["mssql", "cdc"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"SQL Server Change Tracking: {self.schema_name}.{self.table_name}",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _sql_server_change_tracking_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            self = _self
            import os

            url = os.environ.get(self.connection_url_env_var)
            if not url:
                raise RuntimeError(f"Env var {self.connection_url_env_var!r} is not set. Expected a SQLAlchemy connection URL.")

            last_version = _get_last_ct_version(context.instance, context.asset_key)
            engine = _make_engine(url)
            try:
                with engine.connect() as conn:
                    df, new_version = _read_changes(
                        conn, self.schema_name, self.table_name, self.primary_key_columns, self.columns,
                        last_version, context.log,
                    )
            finally:
                engine.dispose()

            op_counts = df["sys_change_operation"].value_counts().to_dict() if "sys_change_operation" in df.columns and len(df) else {}
            context.log.info(
                f"sql_server_change_tracking_asset: {len(df)} changed row(s) for "
                f"{self.schema_name}.{self.table_name} (ct_version {last_version} -> {new_version}), ops={op_counts}"
            )

            context.add_output_metadata({
                "ct_version": dg.MetadataValue.int(new_version),
                "previous_ct_version": dg.MetadataValue.int(last_version) if last_version is not None else dg.MetadataValue.text("none (first run)"),
                "row_count": dg.MetadataValue.int(len(df)),
                "operation_counts": dg.MetadataValue.json({str(k): int(v) for k, v in op_counts.items()}),
            })
            return df

        return dg.Definitions(assets=[_sql_server_change_tracking_asset])
