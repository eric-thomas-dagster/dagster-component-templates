"""WarehouseMultiRowFormulaComponent — row-relative formulas via window functions.

Alteryx "Multi-Row Formula" equivalent. Adds columns whose values depend
on OTHER ROWS in the same window — running totals, LAG/LEAD, ranks,
moving averages, percentiles. Compiles to SQL window functions.

Sibling: `warehouse_formula` (general inline expressions — can include
window functions verbatim if you write the OVER clause yourself). This
component gives you a declarative DSL for the common window-function
patterns so you don't write OVER (...) on every line.
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


def _build_over(partition_by: Optional[List[str]], order_by: Optional[List[str]],
                  order_descending: Optional[List[bool]], dialect: str) -> str:
    parts = []
    if partition_by:
        parts.append("PARTITION BY " + ", ".join(_quote(c, dialect) for c in partition_by))
    if order_by:
        order_descending = order_descending or [False] * len(order_by)
        cols = []
        for c, d in zip(order_by, order_descending):
            cols.append(f"{_quote(c, dialect)} {'DESC' if d else 'ASC'}")
        parts.append("ORDER BY " + ", ".join(cols))
    return "OVER (" + " ".join(parts) + ")"


def _build_window_expr(out_col: str, spec: Dict[str, Any], over_clause: str,
                        dialect: str) -> str:
    """Build a SQL expression string for one window-function output column."""
    kind = spec.get("kind", "expression")
    if kind == "running_total" or kind == "running_sum":
        col = spec["col"]
        return f"(SUM({_quote(col, dialect)}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "running_avg" or kind == "running_mean":
        col = spec["col"]
        return f"(AVG({_quote(col, dialect)}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "lag":
        col = spec["col"]
        n = int(spec.get("offset", 1))
        default = spec.get("default", "NULL")
        return f"(LAG({_quote(col, dialect)}, {n}, {default}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "lead":
        col = spec["col"]
        n = int(spec.get("offset", 1))
        default = spec.get("default", "NULL")
        return f"(LEAD({_quote(col, dialect)}, {n}, {default}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "row_number":
        return f"(ROW_NUMBER() {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "rank":
        return f"(RANK() {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "dense_rank":
        return f"(DENSE_RANK() {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "percent_rank":
        return f"(PERCENT_RANK() {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "first_value":
        col = spec["col"]
        return f"(FIRST_VALUE({_quote(col, dialect)}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "last_value":
        col = spec["col"]
        return f"(LAST_VALUE({_quote(col, dialect)}) {over_clause}) AS {_quote(out_col, dialect)}"
    if kind == "expression":
        # Raw SQL with optional {col} placeholder
        expr = spec["expression"]
        if "{col}" in expr and "col" in spec:
            expr = expr.replace("{col}", _quote(spec["col"], dialect))
        return f"({expr} {over_clause}) AS {_quote(out_col, dialect)}"
    raise ValueError(
        f"warehouse_multi_row_formula: kind={kind!r} not supported. "
        f"Use one of: running_total, running_avg, lag, lead, row_number, rank, "
        f"dense_rank, percent_rank, first_value, last_value, expression"
    )


def _ctas_multi_row(output_table: str, upstream_table: str,
                     expressions: Dict[str, Dict[str, Any]],
                     partition_by: Optional[List[str]], order_by: Optional[List[str]],
                     order_descending: Optional[List[bool]],
                     mode: str, dialect: str) -> Optional[str]:
    if not expressions:
        raise ValueError("warehouse_multi_row_formula: expressions cannot be empty.")
    over_clause = _build_over(partition_by, order_by, order_descending, dialect)
    new_cols = [_build_window_expr(out_col, spec, over_clause, dialect)
                for out_col, spec in expressions.items()]
    select_clause = "*, " + ", ".join(new_cols)
    select_sql = f"SELECT {select_clause} FROM {_quote(upstream_table, dialect)}"
    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseMultiRowFormulaComponent(Component, Model, Resolvable):
    """Row-relative formulas (running totals, LAG/LEAD, ranks) via SQL window functions.

    Each entry in `expressions:` declares an output column with a `kind:` (built-in
    window pattern like running_total / lag / rank / ...) or a raw `expression:`
    string. All entries share the same OVER(...) clause defined by partition_by
    and order_by at the top level.

    Example:
        type: dagster_component_templates.WarehouseMultiRowFormulaComponent
        attributes:
          asset_name: orders_with_running
          database_url: snowflake://...
          dialect: snowflake
          upstream_table: raw.orders
          output_table: analytics.orders_with_running
          partition_by: [customer_id]
          order_by: [order_date]
          order_descending: [false]
          expressions:
            running_total:   {kind: running_total, col: amount}
            running_avg:     {kind: running_avg,   col: amount}
            prev_amount:     {kind: lag, col: amount, offset: 1, default: 0}
            order_rank:      {kind: row_number}
            pct_within_customer:
              kind: expression
              expression: "100.0 * amount / SUM(amount)"
              col: amount
          mode: replace
          deps: [raw_orders_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_table: str = Field(description="Source table name")
    output_table: str = Field(description="Destination table name")
    expressions: Dict[str, Dict[str, Any]] = Field(
        description=(
            "Map of output_column → {kind: <window_pattern>, col: <source>, ...}. "
            "Supported kinds: running_total, running_avg, lag, lead, row_number, "
            "rank, dense_rank, percent_rank, first_value, last_value, expression."
        ),
    )
    partition_by: Optional[List[str]] = Field(
        default=None,
        description="PARTITION BY columns. Omit for unpartitioned (whole table) window.",
    )
    order_by: Optional[List[str]] = Field(
        default=None,
        description="ORDER BY columns. Required for ordered window functions (lag/lead/rank/running_*).",
    )
    order_descending: Optional[List[bool]] = Field(
        default=None,
        description="Per-order_by descending flags. Defaults to all ascending.",
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
        return "Row-relative formulas (running totals / LAG/LEAD / ranks) via SQL window functions. Alteryx Multi-Row Formula equivalent."

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
        expressions = dict(self.expressions)
        partition_by = list(self.partition_by) if self.partition_by else None
        order_by = list(self.order_by) if self.order_by else None
        order_descending = list(self.order_descending) if self.order_descending else None
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
        def _warehouse_multi_row_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_multi_row(output_table, upstream_table, expressions,
                                    partition_by, order_by, order_descending, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_multi_row(output_table, upstream_table, expressions,
                                            partition_by, order_by, order_descending,
                                            "create_if_not_exists", dialect)
                context.log.info(f"CTAS MULTI-ROW ({len(expressions)} window cols): {sql}")
                conn.exec_driver_sql(sql)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "warehouse/window_cols": MetadataValue.text(", ".join(expressions.keys())),
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

        return Definitions(assets=[_warehouse_multi_row_asset])
