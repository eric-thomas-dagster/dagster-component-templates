"""WarehouseTopNPerGroupComponent — Top-N-per-group via window function in the warehouse.

Equivalent to `top_n_per_group` but executed AS SQL via a CTAS with
ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) <= N. No data through
Python — the warehouse engine ranks and prunes.

Example output SQL:
    CREATE OR REPLACE TABLE analytics.top_3_per_region AS
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS _rn
      FROM raw.orders
    ) WHERE _rn <= 3
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


def _ctas_top_n(output_table: str, upstream_table: str, group_by: List[str],
                sort_by: str, n: int, ascending: bool, rank_column: Optional[str],
                mode: str, dialect: str) -> Optional[str]:
    partition_clause = ", ".join(_quote(c, dialect) for c in group_by)
    sort_clause = f"{_quote(sort_by, dialect)} {'ASC' if ascending else 'DESC'}"
    rn_alias = _quote(rank_column, dialect) if rank_column else '"_rn"'
    upstream_quoted = _quote(upstream_table, dialect)

    inner = (
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} "
        f"ORDER BY {sort_clause}) AS {rn_alias} FROM {upstream_quoted}"
    )
    select_sql = f"SELECT * FROM ({inner}) AS _t WHERE {rn_alias} <= {int(n)}"

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseTopNPerGroupComponent(Component, Model, Resolvable):
    """Top-N-per-group in the warehouse via ROW_NUMBER() — no Python materialization.

    Example:
        type: dagster_component_templates.WarehouseTopNPerGroupComponent
        attributes:
          asset_name: top_3_products_per_category
          database_url: snowflake://user:pass@account/db/schema
          dialect: snowflake
          upstream_table: raw.products
          output_table: analytics.top_3_products_per_category
          group_by: [category]
          sort_by: revenue
          n: 3
          ascending: false      # default — top N (descending)
          rank_column: rank     # optional output rank column
          mode: replace
          deps: [raw_products_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy URL. Set this OR database_url_env_var.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with the URL. Set this OR database_url.")
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_table: str = Field(description="Source table name")
    output_table: str = Field(description="Destination table name")
    group_by: List[str] = Field(description="Partition columns (e.g. ['category'])")
    sort_by: str = Field(description="Column to sort by within each group (the 'top' criterion)")
    n: int = Field(default=3, description="Number of rows to keep per group", ge=1)
    ascending: bool = Field(default=False, description="If true, keep bottom N. Default false (top N).")
    rank_column: Optional[str] = Field(default=None, description="Output column name for the 1..N rank. If unset, the helper column is hidden.")
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
        return "Top-N-per-group in the warehouse (ROW_NUMBER pushdown). No Python materialization."

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
        group_by = list(self.group_by)
        sort_by = self.sort_by
        n = self.n
        ascending = self.ascending
        rank_column = self.rank_column
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
        def _warehouse_top_n_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_top_n(output_table, upstream_table, group_by, sort_by, n,
                              ascending, rank_column, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_top_n(output_table, upstream_table, group_by, sort_by, n,
                                      ascending, rank_column, "create_if_not_exists", dialect)
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

        return Definitions(assets=[_warehouse_top_n_asset])
