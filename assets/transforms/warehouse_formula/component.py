"""WarehouseFormulaComponent — add/replace columns via CTAS.

The Alteryx "Formula In-DB" equivalent: add computed columns to a table
without moving the data through Python. Each entry in `expressions:` is
a SQL fragment (any expression the warehouse understands — arithmetic,
CASE, EXTRACT, date funcs, window funcs, ...) that becomes a column in
the output.

Sibling components:
- pandas:      `formula` / `multi_field_formula` / `multi_row_formula`
- polars:      `polars_pipeline` op `with_columns`
- pyspark:     `pyspark_pipeline` op `with_columns`
- snowpark:    `snowpark_pipeline` op `with_columns`
- warehouse:   this component (one CTAS per asset)
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


def _ctas_with_columns(output_table: str, upstream_table: str,
                        expressions: Dict[str, str], keep_existing: bool,
                        keep_columns: Optional[List[str]], mode: str,
                        dialect: str) -> Optional[str]:
    if not expressions:
        raise ValueError("warehouse_formula: expressions cannot be empty.")
    new_col_clauses = [
        f"({expr}) AS {_quote(out_col, dialect)}"
        for out_col, expr in expressions.items()
    ]
    if keep_existing:
        # SELECT *, expr1 AS out_col1, expr2 AS out_col2 FROM upstream
        select_clause = "*, " + ", ".join(new_col_clauses)
    elif keep_columns:
        # Keep only named original columns + computed columns
        keep_clauses = [_quote(c, dialect) for c in keep_columns]
        select_clause = ", ".join(keep_clauses + new_col_clauses)
    else:
        # ONLY the computed columns
        select_clause = ", ".join(new_col_clauses)

    select_sql = f"SELECT {select_clause} FROM {_quote(upstream_table, dialect)}"
    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseFormulaComponent(Component, Model, Resolvable):
    """Add or replace columns via inline SQL expressions — CTAS pushdown.

    Equivalent to Alteryx's `Formula In-DB` or polars's `with_columns`,
    executed in the warehouse engine. Each entry in `expressions:` becomes
    a SELECT-expression in the generated CTAS.

    Example:
        type: dagster_component_templates.WarehouseFormulaComponent
        attributes:
          asset_name: orders_enriched
          database_url: snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH
          dialect: snowflake
          upstream_table: raw.orders
          output_table: analytics.orders_enriched
          expressions:
            net_amount:     "amount - tax"
            is_high_value:  "CASE WHEN amount > 1000 THEN TRUE ELSE FALSE END"
            order_year:     "EXTRACT(YEAR FROM created_at)"
            running_total:  "SUM(amount) OVER (PARTITION BY customer_id ORDER BY created_at)"
          keep_existing: true
          mode: replace
          deps: [raw_orders_in_warehouse]

    The expression body is opaque SQL — anything the dialect supports
    (arithmetic, CASE, EXTRACT, date math, JSON path, window functions,
    even subqueries) goes through verbatim. Use whichever expression style
    matches your warehouse.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy URL. Set this OR database_url_env_var.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with the URL. Set this OR database_url.")
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_table: str = Field(description="Source table name (e.g. 'raw.orders')")
    output_table: str = Field(description="Destination table name (e.g. 'analytics.orders_enriched')")
    expressions: Dict[str, str] = Field(
        description=(
            "Mapping of output_column_name → SQL expression. The expression "
            "is inlined verbatim into the SELECT clause and aliased as the "
            "output column. Use any SQL the dialect supports."
        ),
    )
    keep_existing: bool = Field(
        default=True,
        description=(
            "If true (default): SELECT *, <expressions> — adds the new columns alongside "
            "all originals. If false + keep_columns set: project only those originals "
            "plus the new columns. If false + keep_columns unset: ONLY the new columns."
        ),
    )
    keep_columns: Optional[List[str]] = Field(
        default=None,
        description="Explicit projection of original columns to retain when keep_existing=false.",
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
        return "Add/replace columns via inline SQL expressions (CTAS pushdown). Alteryx Formula In-DB equivalent."

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
        keep_existing = self.keep_existing
        keep_columns = list(self.keep_columns) if self.keep_columns else None
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
        def _warehouse_formula_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_with_columns(output_table, upstream_table, expressions,
                                      keep_existing, keep_columns, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_with_columns(output_table, upstream_table, expressions,
                                              keep_existing, keep_columns,
                                              "create_if_not_exists", dialect)
                context.log.info(f"CTAS WITH_COLUMNS ({len(expressions)} new): {sql}")
                conn.exec_driver_sql(sql)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "warehouse/added_columns": MetadataValue.text(", ".join(expressions.keys())),
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

        return Definitions(assets=[_warehouse_formula_asset])
