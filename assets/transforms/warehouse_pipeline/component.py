"""WarehousePipelineComponent — single-asset multi-step CTE chain.

Compiles a YAML-defined sequence of operations into ONE SQL statement
using WITH-clauses (CTE chain), then runs it as a single CTAS. The
warehouse engine plans the whole chain together — predicate pushdown
across steps, projection pruning, join reordering — and writes the
final result to the output table.

Contrast with the per-step warehouse_* family (filter / summarize /
top_n / dedup / join / union), where each step is its own asset and
the intermediates are real tables. Use this when:
- You want ONE asset (coarser lineage) instead of N
- You want the engine to plan the whole chain as one query
- Intermediate results don't need to be inspectable

Analogous to polars_pipeline (LazyFrame chain in one asset), but the
optimizer is the warehouse's instead of polars's.

Example:
    type: dagster_component_templates.WarehousePipelineComponent
    attributes:
      asset_name: top_5_categories_by_revenue
      database_url: snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH
      dialect: snowflake
      source:
        upstream_table: raw.orders
      operations:
        - op: filter
          predicate: "status = 'paid'"
        - op: group_by
          group_by: [category]
          aggregations:
            revenue:     {col: amount,   agg: sum}
            order_count: {col: order_id, agg: count}
        - op: top_n
          sort_by: revenue
          n: 5
          ascending: false
      output_table: analytics.top_5_categories_by_revenue
      mode: replace
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

_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "limit", "top_n", "top_n_per_group",
              "dedup", "distinct", "union", "join"}

_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count",
                    "nunique", "n_unique", "median", "stddev", "variance"}


def _quote(ident: str, dialect: str) -> str:
    parts = ident.split(".")
    if dialect == "mssql":
        return ".".join(f"[{p}]" for p in parts)
    if dialect == "mysql":
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _agg_expr(func: str, col: str, dialect: str) -> str:
    f = func.lower()
    if f == "sum":      return f"SUM({_quote(col, dialect)})"
    if f in ("mean", "avg"): return f"AVG({_quote(col, dialect)})"
    if f == "min":      return f"MIN({_quote(col, dialect)})"
    if f == "max":      return f"MAX({_quote(col, dialect)})"
    if f == "count":    return f"COUNT({_quote(col, dialect)})"
    if f in ("nunique", "n_unique"): return f"COUNT(DISTINCT {_quote(col, dialect)})"
    if f == "median":   return f"MEDIAN({_quote(col, dialect)})"
    if f == "stddev":   return f"STDDEV({_quote(col, dialect)})"
    if f == "variance": return f"VARIANCE({_quote(col, dialect)})"
    raise ValueError(f"agg func {func!r} not supported. Use one of {sorted(_SUPPORTED_AGGS)}")


def _build_op_sql(prev_ref: str, op: Dict[str, Any], dialect: str) -> str:
    """Build the SELECT body for ONE op, given the name of the previous CTE."""
    kind = op["op"].lower()
    prev = _quote(prev_ref, dialect)

    if kind == "filter":
        predicate = op["predicate"]
        return f"SELECT * FROM {prev} WHERE {predicate}"
    if kind == "select":
        cols = ", ".join(_quote(c, dialect) for c in op["columns"])
        return f"SELECT {cols} FROM {prev}"
    if kind == "drop":
        # Use SELECT * EXCEPT (...) on dialects that support it; otherwise raise.
        if dialect in ("duckdb", "bigquery", "snowflake", "databricks"):
            cols = ", ".join(_quote(c, dialect) for c in op["columns"])
            return f"SELECT * EXCEPT ({cols}) FROM {prev}"
        raise ValueError(f"warehouse_pipeline op='drop' needs SELECT * EXCEPT(); not supported on {dialect}. Use 'select' to enumerate the kept cols.")
    if kind == "rename":
        # Need to enumerate all columns. Without schema introspection we can't
        # do this generically — require the user to use a select op for clarity.
        raise ValueError("warehouse_pipeline op='rename' requires explicit projection — use 'select' with aliases instead, e.g. `columns: ['order_id AS id', 'total']`.")
    if kind == "with_columns":
        expressions = op["expressions"]
        new_cols = ", ".join(f"({expr}) AS {_quote(out_col, dialect)}" for out_col, expr in expressions.items())
        return f"SELECT *, {new_cols} FROM {prev}"
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        select_parts = [_quote(c, dialect) for c in group_by]
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                src_col, func = spec["col"], spec["agg"]
            else:
                src_col, func = out_col, spec
            select_parts.append(f"{_agg_expr(func, src_col, dialect)} AS {_quote(out_col, dialect)}")
        group_list = ", ".join(_quote(c, dialect) for c in group_by)
        return f"SELECT {', '.join(select_parts)} FROM {prev} GROUP BY {group_list}"
    if kind == "sort":
        by = op["by"] if isinstance(op["by"], list) else [op["by"]]
        descending = op.get("descending", False)
        descending = descending if isinstance(descending, list) else [descending] * len(by)
        order_clause = ", ".join(
            f"{_quote(c, dialect)} {'DESC' if d else 'ASC'}" for c, d in zip(by, descending)
        )
        return f"SELECT * FROM {prev} ORDER BY {order_clause}"
    if kind == "limit":
        return f"SELECT * FROM {prev} LIMIT {int(op['n'])}"
    if kind == "top_n":
        sort_by = op["sort_by"]
        ascending = op.get("ascending", False)
        n = int(op["n"])
        return f"SELECT * FROM {prev} ORDER BY {_quote(sort_by, dialect)} {'ASC' if ascending else 'DESC'} LIMIT {n}"
    if kind == "top_n_per_group":
        group_by = op["group_by"]
        sort_by = op["sort_by"]
        ascending = op.get("ascending", False)
        n = int(op["n"])
        partition_clause = ", ".join(_quote(c, dialect) for c in group_by)
        return (
            f"SELECT * EXCEPT (\"_rn\") FROM "
            f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY "
            f"{_quote(sort_by, dialect)} {'ASC' if ascending else 'DESC'}) AS \"_rn\" "
            f"FROM {prev}) WHERE \"_rn\" <= {n}"
        ) if dialect in ("duckdb", "bigquery", "snowflake", "databricks") else (
            # Without SELECT * EXCEPT, we leave the _rn column in.
            f"SELECT * FROM "
            f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY "
            f"{_quote(sort_by, dialect)} {'ASC' if ascending else 'DESC'}) AS \"_rn\" "
            f"FROM {prev}) AS _t WHERE \"_rn\" <= {n}"
        )
    if kind == "dedup":
        subset = op.get("subset")
        if subset:
            partition_clause = ", ".join(_quote(c, dialect) for c in subset)
            order_by = op.get("order_by") or subset
            descending = op.get("descending", False)
            order_clause = ", ".join(
                f"{_quote(c, dialect)} {'DESC' if descending else 'ASC'}" for c in order_by
            )
            return (
                f"SELECT * FROM "
                f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} "
                f"ORDER BY {order_clause}) AS \"_dedup_rn\" FROM {prev}) AS _t "
                f"WHERE \"_dedup_rn\" = 1"
            )
        return f"SELECT DISTINCT * FROM {prev}"
    if kind == "distinct":
        return f"SELECT DISTINCT * FROM {prev}"
    if kind == "union":
        # Other CTE name as second source. Combined with already-running CTE chain.
        other = op["other"]
        op_kw = "UNION" if op.get("distinct", False) else "UNION ALL"
        select_cols = op.get("select_cols")
        cols = ", ".join(select_cols) if select_cols else "*"
        return f"SELECT {cols} FROM {prev} {op_kw} SELECT {cols} FROM {_quote(other, dialect)}"
    if kind == "join":
        right = op["right"]
        how = op.get("how", "inner").upper()
        if how == "OUTER":
            how = "FULL OUTER"
        on_columns = op.get("on_columns") or op.get("on")
        left_on = op.get("left_on")
        right_on = op.get("right_on")
        right_quoted = _quote(right, dialect)
        if on_columns:
            on_clause = "ON " + " AND ".join(
                f"_l.{_quote(c, dialect)} = _r.{_quote(c, dialect)}" for c in on_columns
            )
        elif left_on and right_on:
            on_clause = "ON " + " AND ".join(
                f"_l.{_quote(lo, dialect)} = _r.{_quote(ro, dialect)}"
                for lo, ro in zip(left_on, right_on)
            )
        elif how == "CROSS":
            on_clause = ""
        else:
            raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
        select_cols = op.get("select_cols")
        select_clause = ", ".join(select_cols) if select_cols else "_l.*, _r.*"
        return f"SELECT {select_clause} FROM {prev} AS _l {how} JOIN {right_quoted} AS _r {on_clause}".strip()
    raise ValueError(f"warehouse_pipeline: op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")


def _ctas_pipeline(output_table: str, source_table: str, operations: List[Dict[str, Any]],
                    mode: str, dialect: str) -> Optional[str]:
    if not operations:
        # Just copy the source table
        select_sql = f"SELECT * FROM {_quote(source_table, dialect)}"
    else:
        # Build CTE chain: WITH step_1 AS (op1 against source), step_2 AS (op2 against step_1), ...
        ctes = []
        prev_ref = source_table
        for i, op in enumerate(operations):
            step_name = f"_step_{i + 1}"
            cte_body = _build_op_sql(prev_ref, op, dialect)
            ctes.append(f"{_quote(step_name, dialect)} AS (\n    {cte_body}\n  )")
            prev_ref = step_name
        with_clause = "WITH " + ",\n  ".join(ctes)
        select_sql = f"{with_clause}\nSELECT * FROM {_quote(prev_ref, dialect)}"

    out_quoted = _quote(output_table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS\n{select_sql}"
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n{select_sql}"
    raise ValueError(f"mode must be 'replace' or 'create_if_not_exists', got {mode!r}")


class WarehousePipelineComponent(Component, Model, Resolvable):
    """Multi-step warehouse-native pipeline compiled to ONE CTAS via CTE chain.

    The whole chain is planned by the warehouse engine as a single query.
    Output is one materialized table at the end; intermediate steps are
    CTEs (not tables) — invisible to inspection but available to the
    optimizer for fusion / pushdown.

    Supported ops (within a single chain):
      filter / with_columns / select / drop / group_by / sort / limit /
      top_n / top_n_per_group / dedup / distinct / union / join

    Each subsequent op runs against the previous step's CTE.

    For per-step lineage (each transform as its own Dagster asset), use
    the per-step warehouse_* family (warehouse_filter / warehouse_summarize /
    warehouse_top_n_per_group / warehouse_dedup / warehouse_join / warehouse_union
    / warehouse_formula / warehouse_multi_field_formula / warehouse_multi_row_formula).
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")
    source: Dict[str, Any] = Field(
        description="Source spec: {upstream_table: 'schema.table'} (more shapes — e.g. inline SQL — to come).",
    )
    operations: List[Dict[str, Any]] = Field(
        description="Ordered list of ops. Whole chain compiles to one CTAS with CTE-WITH clauses.",
    )
    output_table: str = Field(description="Destination table name")
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
        return "Multi-step warehouse-native pipeline compiled to one CTAS via CTE chain. YAML-defined stored-procedure shape."

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
        source_table = self.source.get("upstream_table")
        if not source_table:
            raise ValueError("source.upstream_table is required (more source shapes coming).")
        asset_name = self.asset_name
        operations = list(self.operations)
        output_table = self.output_table
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
        def _warehouse_pipeline_asset(context: AssetExecutionContext):
            import sqlalchemy
            engine = sqlalchemy.create_engine(resolve_url())
            sql = _ctas_pipeline(output_table, source_table, operations, mode, dialect)
            with engine.begin() as conn:
                if sql is None:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_quote(output_table, dialect)}")
                    sql = _ctas_pipeline(output_table, source_table, operations,
                                          "create_if_not_exists", dialect)
                context.log.info(f"warehouse_pipeline: {len(operations)} ops, compiling to one CTE-CTAS")
                context.log.info(f"SQL: {sql}")
                conn.exec_driver_sql(sql)
                row_count = int(conn.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {_quote(output_table, dialect)}"
                ).scalar() or 0)
                metadata = {
                    "dagster/row_count": MetadataValue.int(row_count),
                    "warehouse/output_table": MetadataValue.text(output_table),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "warehouse/operation_count": MetadataValue.int(len(operations)),
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

        return Definitions(assets=[_warehouse_pipeline_asset])
