"""WarehouseMultiFieldFormulaComponent — apply ONE formula template to N columns.

the Multi-Field Formula transform. When you want to apply the SAME
transformation to a SET of columns (e.g., `UPPER(TRIM(...))` to every
string column, or `ROUND(..., 2)` to every numeric column), this saves
having to write the same expression N times.

The `expression:` template uses `{col}` as the placeholder for each
column name from `columns:`. Generated SELECT clauses substitute the
column name into the template.

Sibling: `warehouse_formula` (general inline expressions, one per output column).
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


def _ctas_multi_field(output_table: str, upstream_table: str, expression: str,
                       columns: List[str], output_mode: str, suffix: str,
                       prefix: str, mode: str, dialect: str) -> Optional[str]:
    if not columns:
        raise ValueError("warehouse_multi_field_formula: columns cannot be empty.")
    if "{col}" not in expression:
        raise ValueError("warehouse_multi_field_formula: expression must include the {col} placeholder.")

    upstream_q = _quote(upstream_table, dialect)

    # Build the projected SELECT clauses.
    if output_mode == "replace":
        # Replace each column in-place with the transformed expression
        # using its original name. SELECT <transformed> AS col + every
        # other column passes through.
        new_col_select = []
        replaced = set(columns)
        for c in columns:
            expr = expression.replace("{col}", _quote(c, dialect))
            new_col_select.append(f"({expr}) AS {_quote(c, dialect)}")
        # passthrough: SELECT * EXCEPT (columns) is dialect-specific.
        # Easier: list the new ones, then * EXCEPT for duckdb/bigquery;
        # else inline the original column list. We'll use * EXCEPT where supported.
        if dialect in ("duckdb", "bigquery", "snowflake", "databricks"):
            # Bareword column list; dialect-specific exclusion keyword:
            #   DuckDB / Snowflake: SELECT * EXCLUDE (a, b)
            #   BigQuery / Databricks: SELECT * EXCEPT (a, b)
            except_list = ", ".join(columns)
            kw = "EXCLUDE" if dialect in ("duckdb", "snowflake") else "EXCEPT"
            select_clause = f"* {kw} ({except_list}), " + ", ".join(new_col_select)
        else:
            # Postgres/Redshift/MSSQL/MySQL: must enumerate. The caller can
            # pass keep_columns to project; otherwise this raises.
            raise ValueError(
                f"output_mode='replace' on dialect={dialect!r} needs SELECT * EXCEPT() "
                f"support (DuckDB/BigQuery/Snowflake/Databricks only). "
                f"Use output_mode='add_suffix' on this dialect."
            )
    elif output_mode == "add_suffix":
        # New columns named <col><suffix>, original passes through.
        new_col_select = []
        for c in columns:
            expr = expression.replace("{col}", _quote(c, dialect))
            new_col_select.append(f"({expr}) AS {_quote(c + suffix, dialect)}")
        select_clause = "*, " + ", ".join(new_col_select)
    elif output_mode == "add_prefix":
        new_col_select = []
        for c in columns:
            expr = expression.replace("{col}", _quote(c, dialect))
            new_col_select.append(f"({expr}) AS {_quote(prefix + c, dialect)}")
        select_clause = "*, " + ", ".join(new_col_select)
    else:
        raise ValueError(f"output_mode must be 'replace', 'add_suffix', or 'add_prefix', got {output_mode!r}")

    select_sql = f"SELECT {select_clause} FROM {upstream_q}"
    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS {select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS {select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehouseMultiFieldFormulaComponent(Component, Model, Resolvable):
    """Apply ONE formula template to N columns via CTAS — the Multi-Field transform.

    The `expression:` template uses `{col}` as the placeholder.

    Example:
        type: dagster_component_templates.WarehouseMultiFieldFormulaComponent
        attributes:
          asset_name: customers_normalized
          database_url: snowflake://...
          dialect: snowflake
          upstream_table: raw.customers
          output_table: analytics.customers_normalized
          expression: "UPPER(TRIM({col}))"
          columns: [name, email, address]
          output_mode: replace             # replace | add_suffix | add_prefix
          suffix: _norm                    # used when output_mode = add_suffix
          mode: replace
          deps: [raw_customers_in_warehouse]
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    upstream_table: str = Field(description="Source table name")
    output_table: str = Field(description="Destination table name")
    expression: str = Field(
        description=(
            "Formula template applied to each column in `columns`. "
            "Use {col} as the placeholder for the column reference. "
            "Example: 'UPPER(TRIM({col}))' applied to [name, email] yields "
            "UPPER(TRIM(name)) and UPPER(TRIM(email))."
        ),
    )
    columns: List[str] = Field(description="Columns to apply the expression to.")
    output_mode: str = Field(
        default="add_suffix",
        description=(
            "'replace' — transformed values overwrite the original columns (uses SELECT * EXCEPT(); duckdb/bigquery/snowflake/databricks only). "
            "'add_suffix' — original passes through plus new <col><suffix> columns. "
            "'add_prefix' — same with prefix instead of suffix."
        ),
    )
    suffix: str = Field(default="_calc", description="Used when output_mode='add_suffix'.")
    prefix: str = Field(default="calc_", description="Used when output_mode='add_prefix'.")
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
        return "Apply one formula template to N columns via CTAS. the Multi-Field Formula transform."

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
        expression = self.expression
        columns = list(self.columns)
        output_mode = self.output_mode.lower()
        suffix = self.suffix
        prefix = self.prefix
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
        def _warehouse_multi_field_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_multi_field(output_table, upstream_table, expression, columns,
                                     output_mode, suffix, prefix, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_multi_field(output_table, upstream_table, expression, columns,
                                              output_mode, suffix, prefix,
                                              "create_if_not_exists", dialect)
                context.log.info(f"CTAS MULTI-FIELD ({len(columns)} cols, {output_mode}): {sql}")
                conn.exec_driver_sql(sql)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "warehouse/applied_to": MetadataValue.text(", ".join(columns)),
                    "warehouse/expression": MetadataValue.text(expression),
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

        return Definitions(assets=[_warehouse_multi_field_asset])
