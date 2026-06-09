"""WarehouseJoinComponent — join two tables via CTAS in the warehouse.

Standard `LEFT/RIGHT/INNER/FULL/CROSS JOIN ... ON ...` rendered into a
CTAS. No data through Python. The warehouse engine joins and writes
the result to a new table.

For N-way joins, chain multiple `warehouse_join` assets in sequence;
each output_table becomes the next left_table.
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
_VALID_HOWS = {"inner", "left", "right", "outer", "full", "cross"}


def _quote(ident: str, dialect: str) -> str:
    parts = ident.split(".")
    if dialect == "mssql":
        return ".".join(f"[{p}]" for p in parts)
    if dialect == "mysql":
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _ctas_join(output_table: str, left_table: str, right_table: str,
                how: str, on: Optional[List[str]], left_on: Optional[List[str]],
                right_on: Optional[List[str]], select_cols: Optional[List[str]],
                mode: str, dialect: str) -> Optional[str]:
    if how not in _VALID_HOWS:
        raise ValueError(f"how={how!r} must be one of {sorted(_VALID_HOWS)}")
    how_sql = {"outer": "FULL OUTER", "full": "FULL OUTER"}.get(how, how.upper())

    l = _quote(left_table, dialect)
    r = _quote(right_table, dialect)
    if how == "cross":
        on_clause = ""
    elif on:
        on_clause = "ON " + " AND ".join(
            f"_l.{_quote(c, dialect)} = _r.{_quote(c, dialect)}" for c in on
        )
    elif left_on and right_on:
        if len(left_on) != len(right_on):
            raise ValueError("left_on and right_on must have the same length")
        on_clause = "ON " + " AND ".join(
            f"_l.{_quote(lc, dialect)} = _r.{_quote(rc, dialect)}"
            for lc, rc in zip(left_on, right_on)
        )
    else:
        raise ValueError("Provide either 'on' OR both 'left_on' and 'right_on' (unless how='cross')")

    select_clause = ", ".join(select_cols) if select_cols else "_l.*, _r.*"
    inner_sql = (
        f"SELECT {select_clause} FROM {l} AS _l "
        f"{how_sql} JOIN {r} AS _r {on_clause}".strip()
    )

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {inner_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {inner_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseJoinComponent(Component, Model, Resolvable):
    """Join two tables in the warehouse via CTAS — no Python materialization.

    Example:
        type: dagster_component_templates.WarehouseJoinComponent
        attributes:
          asset_name: orders_with_customers
          database_url: snowflake://...
          dialect: snowflake
          left_table: raw.orders
          right_table: raw.customers
          output_table: analytics.orders_with_customers
          how: left
          on_columns: [customer_id]
          select_cols:
            - _l.order_id
            - _l.amount
            - _l.status
            - _r.customer_name
            - _r.region
          mode: replace
          deps: [raw_orders_in_warehouse, raw_customers_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    left_table: str = Field(description="Left-side table name (aliased as _l in select_cols)")
    right_table: str = Field(description="Right-side table name (aliased as _r in select_cols)")
    output_table: str = Field(description="Destination table name")
    how: str = Field(default="inner", description=f"Join type: one of {sorted(_VALID_HOWS)}")
    on_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Join columns (same name on both sides). NB: this is named "
            "`on_columns` rather than `on` because YAML 1.1 parses `on:` as "
            "a boolean (True), breaking schema validation."
        ),
    )
    left_on: Optional[List[str]] = Field(default=None, description="Left join columns (when names differ)")
    right_on: Optional[List[str]] = Field(default=None, description="Right join columns (when names differ)")
    select_cols: Optional[List[str]] = Field(
        default=None,
        description=(
            "Explicit column projection. Use _l.col / _r.col to disambiguate. "
            "If unset, defaults to '_l.*, _r.*' (warehouse will error on duplicate column names)."
        ),
    )
    mode: str = Field(default="replace")
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
        return "Join two tables in the warehouse via CTAS. No Python materialization."

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
        left_table = self.left_table
        right_table = self.right_table
        output_table = self.output_table
        how = self.how.lower()
        on_columns = list(self.on_columns) if self.on_columns else None
        left_on = list(self.left_on) if self.left_on else None
        right_on = list(self.right_on) if self.right_on else None
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
        def _warehouse_join_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_join(output_table, left_table, right_table, how, on_columns, left_on, right_on,
                             select_cols, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_join(output_table, left_table, right_table, how, on_columns, left_on, right_on,
                                     select_cols, "create_if_not_exists", dialect)
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

        return Definitions(assets=[_warehouse_join_asset])
