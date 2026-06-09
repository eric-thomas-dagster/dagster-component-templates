"""WarehouseUnionComponent — stack N tables vertically via CTAS UNION ALL.

Equivalent to `dataframe_union` but executed AS SQL. The warehouse engine
unions and writes the result to a new table. No data through Python.

Supports both UNION ALL (default, keeps duplicates) and UNION DISTINCT.
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


class WarehouseUnionComponent(Component, Model, Resolvable):
    """Stack N tables vertically in the warehouse via UNION ALL (or UNION DISTINCT).

    All `upstream_tables` must have a compatible schema (same column count + types).
    Use `select_cols:` to project a common subset when schemas only partially overlap.

    Example:
        type: dagster_component_templates.WarehouseUnionComponent
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
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=200)

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

        @asset(
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
            sql = _ctas_union(output_table, upstream_tables, distinct, select_cols, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_union(output_table, upstream_tables, distinct, select_cols,
                                       "create_if_not_exists", dialect)
                context.log.info(f"CTAS UNION ({'distinct' if distinct else 'all'}): {len(upstream_tables)} tables")
                conn.exec_driver_sql(sql)
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
