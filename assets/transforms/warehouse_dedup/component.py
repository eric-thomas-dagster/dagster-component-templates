"""WarehouseDedupComponent — deduplicate via CTAS in the warehouse.

Equivalent to `unique_dedup` but executed AS SQL. When `subset:` is set
we use `ROW_NUMBER() OVER (PARTITION BY subset ORDER BY tiebreak) = 1`
so the tie-breaker is explicit (which row to keep when subset values
collide). When `subset:` is unset we use `SELECT DISTINCT *`.
"""
from typing import Any, Dict, List, Optional

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
    parts = ident.split(".")
    if dialect == "mssql":
        return ".".join(f"[{p}]" for p in parts)
    if dialect == "mysql":
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _ctas_dedup(output_table: str, upstream_table: str, subset: Optional[List[str]],
                order_by: Optional[List[str]], descending: bool, mode: str,
                dialect: str) -> Optional[str]:
    upstream_q = _quote(upstream_table, dialect)
    if subset:
        partition_clause = ", ".join(_quote(c, dialect) for c in subset)
        if order_by:
            order_clause = ", ".join(
                f"{_quote(c, dialect)} {'DESC' if descending else 'ASC'}" for c in order_by
            )
        else:
            order_clause = partition_clause + (" DESC" if descending else " ASC")
        inner = (
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} "
            f"ORDER BY {order_clause}) AS \"_dedup_rn\" FROM {upstream_q}"
        )
        select_sql = f"SELECT * FROM ({inner}) AS _t WHERE \"_dedup_rn\" = 1"
    else:
        select_sql = f"SELECT DISTINCT * FROM {upstream_q}"

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseDedupComponent(Component, Model, Resolvable):
    """Deduplicate rows in the warehouse via CTAS — no Python materialization.

    When `subset:` is set, ROW_NUMBER() picks one row per subset-key (the one
    whose `order_by` ranks first — set descending=true to pick the most-recent).
    When `subset:` is unset, SELECT DISTINCT removes fully-duplicate rows.

    Example:
        type: dagster_component_templates.WarehouseDedupComponent
        attributes:
          asset_name: customers_unique
          database_url: snowflake://...
          dialect: snowflake
          upstream_table: raw.customers
          output_table: analytics.customers_unique
          subset: [customer_id]
          order_by: [updated_at]
          descending: true
          mode: replace
          deps: [raw_customers_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy URL. Set this OR database_url_env_var.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with the URL. Set this OR database_url.")
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_table: str = Field(description="Source table name")
    output_table: str = Field(description="Destination table name")
    subset: Optional[List[str]] = Field(default=None, description="Columns to dedup by. If unset, uses SELECT DISTINCT *.")
    order_by: Optional[List[str]] = Field(default=None, description="Tiebreaker columns. If unset, falls back to subset cols.")
    descending: bool = Field(default=False, description="If true, ORDER BY DESC (keep latest). Default false (keep first).")
    mode: str = Field(default="replace", description="'replace' or 'create_if_not_exists'")
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=200)

    @classmethod
    def get_description(cls) -> str:
        return "Deduplicate rows in the warehouse via CTAS (ROW_NUMBER or DISTINCT). No Python materialization."

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
        upstream_table = self.upstream_table
        output_table = self.output_table
        subset = list(self.subset) if self.subset else None
        order_by = list(self.order_by) if self.order_by else None
        descending = self.descending
        mode = self.mode.lower()
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        kinds = list(self.kinds or []) or [dialect, "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""
        resolve_url = self._resolve_url

        @asset(
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _warehouse_dedup_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_dedup(output_table, upstream_table, subset, order_by, descending, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_dedup(output_table, upstream_table, subset, order_by, descending,
                                       "create_if_not_exists", dialect)
                context.log.info(f"CTAS: {sql}")
                conn.exec_driver_sql(sql)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
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

        return Definitions(assets=[_warehouse_dedup_asset])
