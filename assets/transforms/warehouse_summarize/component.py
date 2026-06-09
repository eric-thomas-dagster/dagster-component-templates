"""WarehouseSummarizeComponent — warehouse-native (pushdown) summarize.

Equivalent to `summarize` but executed AS SQL in the warehouse via a
CTAS — no data ever materializes through Python. The asset's "output" is
a table that lives in the warehouse; downstream assets reference it by
table name (or via a sibling reader component) rather than by DataFrame.

Composes with other `warehouse_*` components by chaining
`upstream_table` → `output_table` across CTAS hops.
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


# pandas-agg-func → SQL aggregate-function. Limited intentionally to ones that
# have identical semantics across the major SQL dialects we target. Add more
# (median, std, var) once a dialect-aware translator lands.
_SQL_AGG = {
    "sum": "SUM",
    "mean": "AVG",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count": "COUNT",
    "nunique": "COUNT(DISTINCT {col})",  # special form
    "n_unique": "COUNT(DISTINCT {col})",
}

_SUPPORTED_DIALECTS = {"duckdb", "postgres", "postgresql", "snowflake", "bigquery", "redshift", "databricks", "mssql", "mysql"}


def _quote(ident: str, dialect: str) -> str:
    """Quote a SQL identifier per dialect. Allows multi-part names (schema.table)."""
    parts = ident.split(".")
    if dialect in ("mssql",):
        return ".".join(f"[{p}]" for p in parts)
    if dialect in ("mysql",):
        return ".".join(f"`{p}`" for p in parts)
    # Standard double-quoting (works for duckdb/postgres/snowflake/bigquery/redshift/databricks)
    return ".".join(f'"{p}"' for p in parts)


def _agg_expr(func: str, col: str, dialect: str) -> str:
    """Build a SQL aggregate expression for the given func+col."""
    f = func.lower()
    if f not in _SQL_AGG:
        raise ValueError(
            f"warehouse_summarize: agg func {func!r} is not supported. "
            f"Use one of {sorted(set(_SQL_AGG))}."
        )
    template = _SQL_AGG[f]
    quoted_col = _quote(col, dialect)
    if "{col}" in template:
        return template.format(col=quoted_col)
    return f"{template}({quoted_col})"


def _ctas_sql(output_table: str, upstream_table: str, group_by: List[str],
              aggregations: Dict[str, Any], mode: str, dialect: str) -> Optional[str]:
    """Build the CREATE-TABLE-AS-SELECT (or REPLACE) statement."""
    select_parts = [_quote(c, dialect) for c in group_by]
    for out_col, spec in aggregations.items():
        if isinstance(spec, dict) and "col" in spec and "agg" in spec:
            src_col, func = spec["col"], spec["agg"]
        else:
            # simple form: out_col == source col
            src_col, func = out_col, spec
        select_parts.append(f"{_agg_expr(func, src_col, dialect)} AS {_quote(out_col, dialect)}")

    group_list = ", ".join(_quote(c, dialect) for c in group_by)
    select_sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {_quote(upstream_table, dialect)} "
        f"GROUP BY {group_list}"
    )

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        # Standard CREATE OR REPLACE — supported by duckdb/snowflake/bigquery/databricks.
        # For postgres/redshift/mssql/mysql which lack it, fall through to DROP + CREATE.
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None  # Caller handles DROP + CREATE
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseSummarizeComponent(Component, Model, Resolvable):
    """Group + aggregate inside the warehouse via a CTAS statement.

    No data is read into Python. Useful when the upstream table is large and
    you want the warehouse engine (Snowflake / BigQuery / DuckDB / etc.) to
    do the work. Output is a table in the same warehouse; downstream
    `warehouse_*` components compose against it by table name.

    Example:
        type: dagster_component_templates.WarehouseSummarizeComponent
        attributes:
          asset_name: revenue_by_region
          database_url: snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH
          dialect: snowflake
          upstream_table: raw.orders
          output_table: analytics.revenue_by_region
          group_by: [region, product_category]
          aggregations:
            revenue:        {col: total, agg: sum}
            order_count:    {col: order_id, agg: count}
            avg_order:      {col: total, agg: mean}
          mode: replace
          deps: [raw_orders_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name (the table this asset produces).")
    database_url: Optional[str] = Field(
        default=None,
        description="SQLAlchemy URL for the warehouse. Set this OR database_url_env_var.",
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the warehouse SQLAlchemy URL. Set this OR database_url.",
    )
    dialect: str = Field(
        description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.",
    )
    upstream_table: str = Field(description="Source table name (e.g. 'raw.orders').")
    output_table: str = Field(description="Destination table name (e.g. 'analytics.revenue_by_region').")
    group_by: List[str] = Field(description="Columns to group by.")
    aggregations: Dict = Field(
        description=(
            "Same shape as `summarize`: {out_col: agg_func} OR "
            "{out_col: {col: <src_col>, agg: <agg_func>}}. "
            "Supported aggs: sum, mean/avg, min, max, count, nunique."
        ),
    )
    mode: str = Field(
        default="replace",
        description=(
            "'replace' (CREATE OR REPLACE on dialects that support it; "
            "DROP + CREATE on those that don't) or 'create_if_not_exists'."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys (lineage only — no data passed at runtime).",
    )
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(
        default=False,
        description="Emit a small SELECT … LIMIT N preview as asset metadata after CTAS.",
    )
    preview_rows: int = Field(default=25, ge=1, le=200)

    @classmethod
    def get_description(cls) -> str:
        return "Group + aggregate inside the warehouse (CTAS pushdown). No Python materialization."

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
        aggregations = dict(self.aggregations)
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
        def _warehouse_summarize_asset(context: AssetExecutionContext):
            import sqlalchemy
            url = resolve_url()
            engine = sqlalchemy.create_engine(url)
            sql = _ctas_sql(output_table, upstream_table, group_by, aggregations, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    # mode=replace on a dialect without CREATE OR REPLACE → DROP + CREATE.
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_sql(output_table, upstream_table, group_by, aggregations,
                                    "create_if_not_exists", dialect)
                context.log.info(f"CTAS: {sql}")
                conn.exec_driver_sql(sql)

                # Row count via a small SELECT — keeps the principle: no data
                # into Python except small metadata-shaped queries.
                row_count_res = conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).fetchone()
                row_count = int(row_count_res[0]) if row_count_res else 0

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
                            header = "| " + " | ".join(cols) + " |"
                            sep = "| " + " | ".join(["---"] * len(cols)) + " |"
                            body = "\n".join(
                                "| " + " | ".join(str(v) for v in r) + " |" for r in prev_rows
                            )
                            metadata["preview"] = MetadataValue.md(f"{header}\n{sep}\n{body}")
                    except Exception as e:
                        context.log.warning(f"preview emission failed: {e}")

            return dg.MaterializeResult(metadata=metadata)

        return Definitions(assets=[_warehouse_summarize_asset])
