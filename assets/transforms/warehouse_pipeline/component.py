"""WarehousePipelineComponent — single-asset multi-step CTE pipeline.

Compiles a YAML-defined DAG of steps into ONE SQL plan per sink using
WITH-clauses (CTE chain). The warehouse engine plans the whole graph
together — predicate pushdown across steps, projection pruning, join
reordering — and writes one or more output tables.

Two YAML shapes are supported, both compile to the same CTE-CTAS engine:

  (a) Flat shape — one source, one ops chain, one sink:

      source:
        upstream_table: raw.orders
      operations:
        - {op: filter, predicate: "status = 'paid'"}
        - {op: group_by, group_by: [category], aggregations: {revenue: {col: amount, agg: sum}}}
      output_table: analytics.top_categories
      mode: replace

  (b) Multi-step pipeline (multiple sources / inter-step refs / multi-sink):

      steps:
        - id: paid_orders
          source: {kind: table, table: raw.orders}
          operations:
            - {op: filter, predicate: "status = 'paid'"}

        - id: gold_customers
          source: {kind: table, table: raw.customers}
          operations:
            - {op: filter, predicate: "tier = 'gold'"}

        - id: enriched
          source: {kind: ref, ref: paid_orders}
          operations:
            - {op: join, right: {ref: gold_customers}, on_columns: [customer_id]}
            - {op: sql, sql: "SELECT *, amount * 0.15 AS commission FROM <<self>>"}
            - {op: group_by, group_by: [region],
               aggregations: {revenue: {col: amount, agg: sum}}}

      sinks:
        - {from: enriched, table: analytics.regional_top_paid, mode: overwrite}

Both forms produce a single Dagster asset. Multi-sink emits one CTAS per
sink; each sink's CTAS includes the full WITH clause so the optimizer
sees the whole graph.
"""
from typing import Any, Dict, List, Literal, Optional, Tuple

import dagster as dg
from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    DailyPartitionsDefinition,
    Definitions,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MetadataValue,
    Model,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    Resolvable,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Build a PartitionsDefinition matching the canonical dataframe_to_snowflake
    factory. Supports daily/weekly/monthly/hourly/static/dynamic/multi."""
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _substitute_partition_key(obj, partition_key: Optional[str]):
    """Walk a nested dict/list/str structure and replace `<<partition_key>>`
    chevron placeholders with the runtime partition key. Used so the user
    can reference the current partition in op:filter predicates / op:sql /
    sink table names without needing their own templating."""
    if partition_key is None:
        return obj
    if isinstance(obj, str):
        return obj.replace("<<partition_key>>", str(partition_key))
    if isinstance(obj, list):
        return [_substitute_partition_key(x, partition_key) for x in obj]
    if isinstance(obj, dict):
        return {k: _substitute_partition_key(v, partition_key) for k, v in obj.items()}
    return obj


_SUPPORTED_DIALECTS = {"duckdb", "postgres", "postgresql", "snowflake", "bigquery",
                        "redshift", "databricks", "mssql", "mysql"}

_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "limit", "top_n", "top_n_per_group",
              "dedup", "distinct", "union", "join", "sql",
              "pivot", "lookup", "window_calculation", "unpivot", "rank"}

# SQL-native window-function ranking (op='rank'): RANK/DENSE_RANK/ROW_NUMBER are standard
# across every dialect this component targets. The pandas-style tie-breaking vocabulary
# ('average'/'min'/'max'/'first' from pandas_pipeline/polars_pipeline's own `rank` op) has no
# direct SQL window-function equivalent -- forcing it would mean compiling a non-portable
# secondary aggregation per dialect, so this op intentionally uses SQL's own vocabulary
# instead of pandas'. 'ntile'/'cume_dist' are shared with window_calculation -- 'rank' is the
# more complete superset (adds rank/dense_rank/row_number), kept as a separate op rather than
# folded into window_calculation to avoid changing that op's already-tested behavior.
_RANK_METHOD_SQL = {
    "rank": "RANK()", "dense_rank": "DENSE_RANK()", "row_number": "ROW_NUMBER()",
}

_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count",
                    "nunique", "n_unique", "median", "stddev", "variance"}

# Dialects with native `ORDER BY col NULLS FIRST|LAST` support (used by op=sort's
# nulls_last). mssql/mysql have no such syntax and need a portable CASE-based
# workaround (see _build_op_sql's 'sort' branch).
_NATIVE_NULLS_ORDER_DIALECTS = {"duckdb", "postgres", "postgresql", "snowflake",
                                 "bigquery", "redshift", "databricks"}

# Dialects supporting a `SELECT * <kw> (col, ...)` column-exclusion wildcard, and the
# exact keyword each one needs -- NOT the same keyword everywhere: BigQuery and
# Databricks use `EXCEPT` (confirmed in both products' own SQL reference docs), while
# DuckDB and Snowflake use `EXCLUDE` (`EXCEPT` is reserved for the set operator on both --
# confirmed empirically against a real DuckDB connection; Snowflake's own docs show only
# `SELECT * EXCLUDE col_name`). Used by op=drop and the row-ranking cleanup in
# op=top_n_per_group / op=lookup's match_sort_by.
_EXCLUDE_COLUMNS_KEYWORD = {
    "duckdb": "EXCLUDE",
    "snowflake": "EXCLUDE",
    "bigquery": "EXCEPT",
    "databricks": "EXCEPT",
}


def _quote(ident: str, dialect: str) -> str:
    parts = ident.split(".")
    if dialect == "mssql":
        return ".".join(f"[{p}]" for p in parts)
    if dialect == "mysql":
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _quote_alias(name: str, dialect: str) -> str:
    """Quote a single identifier segment, never splitting on '.' -- used for column
    aliases *built from data values* (op=pivot's pivot_values, op=lookup/window_calculation's
    output_column), which may legitimately contain a literal dot or other character that
    `_quote`'s dotted-identifier splitting would mishandle."""
    if dialect == "mssql":
        return f"[{name}]"
    if dialect == "mysql":
        return f"`{name}`"
    return f'"{name}"'


def _literal(value: Any, dialect: str) -> str:
    """Render a Python value as a safe SQL literal (e.g. op=pivot's pivot_values inside a
    CASE WHEN). Strings are single-quoted with embedded quotes doubled -- the same
    IN-list literal-escaping convention already used by the sibling warehouse_* components
    (warehouse_join/warehouse_dedup/warehouse_filter/etc: `"'" + k.replace("'", "''") + "'"`)."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _agg_func_sql(func: str, expr: str) -> str:
    """Wrap an arbitrary SQL expression (a quoted column OR a CASE WHEN, for op=pivot's
    conditional aggregation) in the aggregate function named by `func`."""
    f = func.lower()
    if f == "sum":      return f"SUM({expr})"
    if f in ("mean", "avg"): return f"AVG({expr})"
    if f == "min":      return f"MIN({expr})"
    if f == "max":      return f"MAX({expr})"
    if f == "count":    return f"COUNT({expr})"
    if f in ("nunique", "n_unique"): return f"COUNT(DISTINCT {expr})"
    if f == "median":   return f"MEDIAN({expr})"
    if f == "stddev":   return f"STDDEV({expr})"
    if f == "variance": return f"VARIANCE({expr})"
    raise ValueError(f"agg func {func!r} not supported. Use one of {sorted(_SUPPORTED_AGGS)}")


def _agg_expr(func: str, col: str, dialect: str) -> str:
    return _agg_func_sql(func, _quote(col, dialect))


def _resolve_right_ref(right: Any, step_refs: Dict[str, str], dialect: str) -> str:
    """Resolve a `right:`/`other:` operand (join/lookup/union) to a quoted SQL source --
    either a table name string, or a {ref: <step_id>} pointing at an earlier step's CTE."""
    if isinstance(right, dict) and "ref" in right:
        right_ref = step_refs.get(right["ref"])
        if right_ref is None:
            raise ValueError(f"right.ref={right['ref']!r} doesn't match any earlier step id")
        return _quote(right_ref, dialect)
    if isinstance(right, str):
        return _quote(right, dialect)
    raise ValueError("'right' must be a table string or {ref: <step_id>}")


def _build_on_clause(op: Dict[str, Any], dialect: str,
                      left_alias: str = "_l", right_alias: str = "_r") -> Optional[str]:
    """Build the `<left> = <right>` condition list (no leading 'ON') shared by join's
    ON clause and semi/anti's correlated EXISTS predicate, from on_columns/on, or
    left_on + right_on. Returns None if neither was provided."""
    on_columns = op.get("on_columns") or op.get("on")
    left_on = op.get("left_on")
    right_on = op.get("right_on")
    if on_columns:
        return " AND ".join(
            f"{left_alias}.{_quote(c, dialect)} = {right_alias}.{_quote(c, dialect)}" for c in on_columns
        )
    if left_on and right_on:
        return " AND ".join(
            f"{left_alias}.{_quote(lo, dialect)} = {right_alias}.{_quote(ro, dialect)}"
            for lo, ro in zip(left_on, right_on)
        )
    return None


def _resolve_sql_template(sql: str, prev_ref: str, step_refs: Dict[str, str], dialect: str,
                          this_table: Optional[str] = None) -> str:
    """Replace SQL placeholders with quoted CTE / table names.

    Two syntaxes supported, both resolving to the same CTE name:

    1. Native (`<<name>>` / `<<self>>`) — angle-bracket chevrons; chosen
       so they never collide with Dagster's component-YAML Jinja renderer.

    2. **dbt-friendly** — `$ref('step_id')` / `$self` / `$this`. dbt uses
       `{{ ref(...) }}` / `{{ this }}` in Jinja, but Dagster's YAML layer
       eats those before this component sees them, so we use the `$` sigil
       (never Jinja-processed) to give dbt users a familiar look.

       - `$ref('step_id')` → the referenced step's output CTE.
       - `$self` → the previous CTE in this step (same as `<<self>>`).
       - `$this` → the CURRENT SINK's target table. Handy for
         incremental predicates: `WHERE updated_at > (SELECT MAX(updated_at) FROM $this)`.
    """
    import re as _re
    out = sql
    # Native <<...>> syntax.
    out = out.replace("<<self>>", _quote(prev_ref, dialect))
    out = out.replace("<< self >>", _quote(prev_ref, dialect))
    for sid, ref in step_refs.items():
        out = out.replace(f"<<{sid}>>", _quote(ref, dialect))
        out = out.replace(f"<< {sid} >>", _quote(ref, dialect))

    # dbt-style $ref('step_id') / $self / $this.
    out = out.replace("$self", _quote(prev_ref, dialect))
    if this_table is not None:
        out = out.replace("$this", _quote(this_table, dialect))

    def _ref_sub(m):
        sid = m.group(1)
        if sid not in step_refs:
            raise ValueError(
                f"$ref({sid!r}) — no step with that id. Available: {sorted(step_refs.keys())}"
            )
        return _quote(step_refs[sid], dialect)

    out = _re.sub(r"\$ref\(\s*['\"]([^'\"]+)['\"]\s*\)", _ref_sub, out)
    return out


def _build_op_sql(prev_ref: str, op: Dict[str, Any], dialect: str,
                  step_refs: Dict[str, str]) -> str:
    """Build the SELECT body for ONE op, given the name of the previous CTE."""
    kind = op["op"].lower()
    prev = _quote(prev_ref, dialect)

    if kind == "sql":
        # Escape hatch. The user provides a raw SQL fragment using <<self>>
        # (this step's previous CTE) and/or <<step_id>> (other step refs).
        # Chevron syntax is used (not `{{ }}`) because Dagster pre-renders YAML
        # through Jinja and would consume `{{ ... }}` before this code runs.
        # The SQL must be a single SELECT — it becomes the body of a CTE.
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        return _resolve_sql_template(sql, prev_ref, step_refs, dialect).strip()

    if kind == "filter":
        predicate = op["predicate"]
        return f"SELECT * FROM {prev} WHERE {predicate}"
    if kind == "select":
        cols = ", ".join(_quote(c, dialect) for c in op["columns"])
        return f"SELECT {cols} FROM {prev}"
    if kind == "drop":
        if dialect in _EXCLUDE_COLUMNS_KEYWORD:
            cols = ", ".join(_quote(c, dialect) for c in op["columns"])
            kw = _EXCLUDE_COLUMNS_KEYWORD[dialect]
            return f"SELECT * {kw} ({cols}) FROM {prev}"
        raise ValueError(f"warehouse_pipeline op='drop' needs SELECT * EXCEPT()/EXCLUDE(); not supported on {dialect}. Use 'select' to enumerate the kept cols.")
    if kind == "rename":
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
        # ADF's own Sort transform defaults to nulls FIRST -- nulls_last=False matches that.
        nulls_last = op.get("nulls_last", False)
        nulls_last = nulls_last if isinstance(nulls_last, list) else [nulls_last] * len(by)
        if dialect in _NATIVE_NULLS_ORDER_DIALECTS:
            order_clause = ", ".join(
                f"{_quote(c, dialect)} {'DESC' if d else 'ASC'} {'NULLS LAST' if nl else 'NULLS FIRST'}"
                for c, d, nl in zip(by, descending, nulls_last)
            )
        else:
            # mssql / mysql have no NULLS FIRST/LAST syntax -- portable workaround: a
            # leading CASE-based tiebreaker groups NULLs to the requested end before the
            # real per-column direction is applied as a secondary sort key.
            parts = []
            for c, d, nl in zip(by, descending, nulls_last):
                qc = _quote(c, dialect)
                null_rank = "1" if nl else "0"
                not_null_rank = "0" if nl else "1"
                parts.append(f"(CASE WHEN {qc} IS NULL THEN {null_rank} ELSE {not_null_rank} END) ASC")
                parts.append(f"{qc} {'DESC' if d else 'ASC'}")
            order_clause = ", ".join(parts)
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
            f"SELECT * {_EXCLUDE_COLUMNS_KEYWORD[dialect]} (\"_rn\") FROM "
            f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY "
            f"{_quote(sort_by, dialect)} {'ASC' if ascending else 'DESC'}) AS \"_rn\" "
            f"FROM {prev}) WHERE \"_rn\" <= {n}"
        ) if dialect in _EXCLUDE_COLUMNS_KEYWORD else (
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
        # `other` may be a table name  OR a {ref: <step_id>}.
        other = op["other"]
        if isinstance(other, dict) and "ref" in other:
            other_ref = step_refs.get(other["ref"])
            if other_ref is None:
                raise ValueError(f"union.other.ref={other['ref']!r} doesn't match any earlier step id")
            other_quoted = _quote(other_ref, dialect)
        elif isinstance(other, str):
            other_quoted = _quote(other, dialect)
        else:
            raise ValueError("union.other must be a table string or {ref: <step_id>}")
        op_kw = "UNION" if op.get("distinct", False) else "UNION ALL"
        select_cols = op.get("select_cols")
        cols = ", ".join(select_cols) if select_cols else "*"
        return f"SELECT {cols} FROM {prev} {op_kw} SELECT {cols} FROM {other_quoted}"
    if kind == "join":
        # `right` may be a table name  OR a {ref: <step_id>}.
        right = op["right"]
        right_quoted = _resolve_right_ref(right, step_refs, dialect)
        how = op.get("how", "inner").upper()
        if how == "OUTER":
            how = "FULL OUTER"
        if how in ("SEMI", "ANTI"):
            # Portable semi/anti join: NOT a `SEMI JOIN`/`ANTI JOIN` keyword (not standard
            # across this component's target dialects) -- a correlated
            # WHERE [NOT] EXISTS (...) subquery, which every supported dialect understands.
            # Output is left-side columns only (semi/anti never pull right-side columns).
            cond = _build_on_clause(op, dialect)
            if cond is None:
                raise ValueError("join op: how='semi'/'anti' requires 'on_columns' OR 'left_on' + 'right_on'")
            select_cols = op.get("select_cols")
            select_clause = ", ".join(select_cols) if select_cols else "_l.*"
            exists_kw = "EXISTS" if how == "SEMI" else "NOT EXISTS"
            return (
                f"SELECT {select_clause} FROM {prev} AS _l "
                f"WHERE {exists_kw} (SELECT 1 FROM {right_quoted} AS _r WHERE {cond})"
            )
        on_clause_inner = _build_on_clause(op, dialect)
        if on_clause_inner is not None:
            on_clause = "ON " + on_clause_inner
        elif how == "CROSS":
            on_clause = ""
        else:
            raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
        select_cols = op.get("select_cols")
        select_clause = ", ".join(select_cols) if select_cols else "_l.*, _r.*"
        return f"SELECT {select_clause} FROM {prev} AS _l {how} JOIN {right_quoted} AS _r {on_clause}".strip()
    if kind == "pivot":
        index_cols = op.get("index")
        if not index_cols:
            raise ValueError("op='pivot' requires 'index': [col, ...] -- the column(s) kept as row identity")
        if isinstance(index_cols, str):
            index_cols = [index_cols]
        pivot_column = op.get("pivot_column")
        if not pivot_column:
            raise ValueError("op='pivot' requires 'pivot_column': <col> -- whose distinct values become new output columns")
        value_columns = op.get("value_columns")
        if not value_columns:
            raise ValueError("op='pivot' requires 'value_columns': [col, ...] -- the column(s) aggregated into each pivoted cell")
        if isinstance(value_columns, str):
            value_columns = [value_columns]
        agg_func = op.get("agg_func", "sum")
        pivot_values = op.get("pivot_values")
        if not pivot_values:
            raise ValueError(
                "op='pivot' requires 'pivot_values': [v1, v2, ...] -- the exact distinct values "
                "of pivot_column to materialize as output columns. Unlike pandas_pipeline/"
                "polars_pipeline's pivot (which discovers distinct values at RUNTIME, in-process), "
                "this component compiles portable conditional-aggregation SQL "
                "(SUM(CASE WHEN pivot_column = 'v' THEN value_column END) AS v, ...) at QUERY-BUILD "
                "TIME, before execution -- there's no universal unbounded PIVOT keyword across "
                "every dialect this component targets, so the value list must be known upfront. "
                "If you don't know the values ahead of time, run a `SELECT DISTINCT <pivot_column>` "
                "query first and feed the result in here, or use the pandas_pipeline/polars_pipeline "
                "sibling instead, which can discover pivot values at runtime."
            )
        fill_value = op.get("fill_value")
        multi_value = len(value_columns) > 1
        select_parts = [_quote(c, dialect) for c in index_cols]
        for value_col in value_columns:
            for pv in pivot_values:
                case_expr = (
                    f"CASE WHEN {_quote(pivot_column, dialect)} = {_literal(pv, dialect)} "
                    f"THEN {_quote(value_col, dialect)} END"
                )
                agg_sql = _agg_func_sql(agg_func, case_expr)
                if fill_value is not None:
                    agg_sql = f"COALESCE({agg_sql}, {_literal(fill_value, dialect)})"
                alias = f"{value_col}_{pv}" if multi_value else str(pv)
                select_parts.append(f"{agg_sql} AS {_quote_alias(alias, dialect)}")
        group_list = ", ".join(_quote(c, dialect) for c in index_cols)
        return f"SELECT {', '.join(select_parts)} FROM {prev} GROUP BY {group_list}"
    if kind == "lookup":
        right = op["right"]
        right_quoted = _resolve_right_ref(right, step_refs, dialect)
        on_clause_inner = _build_on_clause(op, dialect)
        if on_clause_inner is None:
            raise ValueError("lookup op: provide 'on_columns' OR 'left_on' + 'right_on'")
        output_mode = (op.get("output_mode") or "columns").lower()

        if output_mode == "is_match":
            # Existence doesn't depend on match_sort_by's one-row-per-key narrowing --
            # EXISTS is true/false the same either way -- so this ignores it and probes
            # the raw right side directly.
            output_column = op.get("output_column", "is_match")
            return (
                f"SELECT _l.*, CASE WHEN EXISTS "
                f"(SELECT 1 FROM {right_quoted} AS _r WHERE {on_clause_inner}) "
                f"THEN TRUE ELSE FALSE END AS {_quote_alias(output_column, dialect)} "
                f"FROM {prev} AS _l"
            )
        if output_mode != "columns":
            raise ValueError(f"lookup op: output_mode must be 'columns' or 'is_match', got {output_mode!r}")

        right_source = right_quoted
        match_sort_by = op.get("match_sort_by")
        if match_sort_by:
            if isinstance(match_sort_by, str):
                match_sort_by = [match_sort_by]
            match_sort_ascending = op.get("match_sort_ascending", False)
            match_sort_ascending = (
                match_sort_ascending if isinstance(match_sort_ascending, list)
                else [match_sort_ascending] * len(match_sort_by)
            )
            right_key_cols = op.get("right_on") or op.get("on_columns") or op.get("on")
            if not right_key_cols:
                raise ValueError("lookup op: match_sort_by requires 'on_columns' or 'right_on' to define the partition key")
            partition_clause = ", ".join(_quote(c, dialect) for c in right_key_cols)
            order_clause = ", ".join(
                f"{_quote(c, dialect)} {'ASC' if a else 'DESC'}"
                for c, a in zip(match_sort_by, match_sort_ascending)
            )
            # Same ROW_NUMBER()-over-PARTITION pattern as op=top_n_per_group above --
            # deterministically picks exactly one right-side row per key.
            ranked = (
                f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY "
                f"{order_clause}) AS \"_lookup_rn\" FROM {right_quoted}) AS _ranked"
            )
            if dialect in _EXCLUDE_COLUMNS_KEYWORD:
                kw = _EXCLUDE_COLUMNS_KEYWORD[dialect]
                right_source = f"(SELECT * {kw} (\"_lookup_rn\") FROM {ranked} WHERE \"_lookup_rn\" = 1)"
            else:
                # Same limitation as op=top_n_per_group's own fallback: the ranking column
                # leaks into the result on dialects with no column-exclusion wildcard.
                right_source = f"(SELECT * FROM {ranked} WHERE \"_lookup_rn\" = 1)"

        select_cols = op.get("select_cols")
        select_clause = ", ".join(select_cols) if select_cols else "_l.*, _r.*"
        return (
            f"SELECT {select_clause} FROM {prev} AS _l "
            f"LEFT JOIN {right_source} AS _r ON {on_clause_inner}"
        )
    if kind == "window_calculation":
        function = (op.get("function") or "").lower()
        if function not in ("ntile", "cume_dist"):
            raise ValueError(f"window_calculation op: 'function' must be 'ntile' or 'cume_dist', got {op.get('function')!r}")
        order_by = op.get("order_by")
        if not order_by:
            raise ValueError("window_calculation op requires 'order_by': [col, ...]")
        if isinstance(order_by, str):
            order_by = [order_by]
        ascending = op.get("ascending", True)
        ascending = ascending if isinstance(ascending, list) else [ascending] * len(order_by)
        order_clause = ", ".join(
            f"{_quote(c, dialect)} {'ASC' if a else 'DESC'}" for c, a in zip(order_by, ascending)
        )
        group_by = op.get("group_by")
        partition_clause = ""
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            partition_clause = "PARTITION BY " + ", ".join(_quote(c, dialect) for c in group_by) + " "
        if function == "ntile":
            n = op.get("n")
            if not n:
                raise ValueError("window_calculation op: function='ntile' requires 'n': <int>")
            func_sql = f"NTILE({int(n)})"
        else:
            func_sql = "CUME_DIST()"
        output_column = op.get("output_column") or function
        return (
            f"SELECT *, {func_sql} OVER ({partition_clause}ORDER BY {order_clause}) "
            f"AS {_quote_alias(output_column, dialect)} FROM {prev}"
        )
    if kind == "unpivot":
        # ADF's Unpivot transform, compiled as a portable UNION ALL (one branch per value
        # column) rather than a native UNPIVOT keyword -- UNPIVOT syntax/support is
        # inconsistent across this component's target dialects (e.g. postgres/redshift/mysql
        # have no native UNPIVOT at all), while UNION ALL of per-column SELECTs works
        # identically on every one of them.
        id_columns = op.get("id_columns")
        if not id_columns:
            raise ValueError("unpivot op requires 'id_columns': [col, ...]")
        if isinstance(id_columns, str):
            id_columns = [id_columns]
        value_columns = op.get("value_columns")
        if not value_columns:
            raise ValueError("unpivot op requires 'value_columns': [col, ...]")
        if isinstance(value_columns, str):
            value_columns = [value_columns]
        var_column = op.get("var_column") or op.get("variable_name") or "variable"
        value_column = op.get("value_column") or op.get("value_name") or "value"
        id_select = ", ".join(_quote(c, dialect) for c in id_columns)
        drop_nulls = op.get("drop_null_values", False)
        branches = []
        for vc in value_columns:
            where = f" WHERE {_quote(vc, dialect)} IS NOT NULL" if drop_nulls else ""
            branches.append(
                f"SELECT {id_select}, {_literal(vc, dialect)} AS {_quote_alias(var_column, dialect)}, "
                f"{_quote(vc, dialect)} AS {_quote_alias(value_column, dialect)} FROM {prev}{where}"
            )
        return " UNION ALL ".join(branches)
    if kind == "rank":
        order_by = op.get("order_by")
        if not order_by:
            raise ValueError("rank op requires 'order_by': [col, ...]")
        if isinstance(order_by, str):
            order_by = [order_by]
        ascending = op.get("ascending", True)
        ascending = ascending if isinstance(ascending, list) else [ascending] * len(order_by)
        method = str(op.get("method") or "rank").lower()
        _valid = set(_RANK_METHOD_SQL) | {"ntile", "cume_dist"}
        if method not in _valid:
            raise ValueError(
                f"rank op: method must be one of {sorted(_valid)}, got {op.get('method')!r}. "
                "Pandas-style tie-breaking ('average'/'min'/'max'/'first', 'dense') isn't a "
                "portable SQL window function -- use pandas_pipeline/polars_pipeline's own "
                "`rank` op for that instead."
            )
        order_clause = ", ".join(
            f"{_quote(c, dialect)} {'ASC' if a else 'DESC'}" for c, a in zip(order_by, ascending)
        )
        group_by = op.get("group_by")
        partition_clause = ""
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            partition_clause = "PARTITION BY " + ", ".join(_quote(c, dialect) for c in group_by) + " "
        output_column = op.get("output_column") or method
        if method == "ntile":
            n = op.get("ntile_buckets") or op.get("n")
            if not n:
                raise ValueError("rank op: method='ntile' requires 'ntile_buckets': <int>")
            func_sql = f"NTILE({int(n)})"
        elif method == "cume_dist":
            func_sql = "CUME_DIST()"
        else:
            func_sql = _RANK_METHOD_SQL[method]
        return (
            f"SELECT *, {func_sql} OVER ({partition_clause}ORDER BY {order_clause}) "
            f"AS {_quote_alias(output_column, dialect)} FROM {prev}"
        )
    raise ValueError(f"warehouse_pipeline: op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")


def _resolve_step_source(source_spec: Dict[str, Any], step_refs: Dict[str, str]
                          ) -> Tuple[str, Optional[Tuple[str, str]]]:
    """Resolve a step's source to (initial_ref, optional_seed_cte).

    Returns:
      initial_ref — the CTE/table name the first op should select from.
      optional_seed_cte — for kind=sql sources, a (cte_name, body) pair that
        wraps the inline SQL into a CTE; None for table/ref sources.
    """
    # Flat shape: source: {upstream_table: ...}
    if "upstream_table" in source_spec and "kind" not in source_spec:
        return source_spec["upstream_table"], None

    kind = source_spec.get("kind", "table")
    if kind == "table":
        table = source_spec.get("table") or source_spec.get("upstream_table")
        if not table:
            raise ValueError("source kind=table requires a 'table' field")
        return table, None
    if kind == "ref":
        ref = source_spec.get("ref")
        if ref not in step_refs:
            raise ValueError(f"source kind=ref: ref={ref!r} doesn't match any earlier step id")
        return step_refs[ref], None
    if kind == "sql":
        sql = source_spec.get("sql")
        if not sql:
            raise ValueError("source kind=sql requires a 'sql' field (a SELECT statement)")
        # Caller assigns the CTE name; we return a sentinel that the caller
        # converts into a seed CTE before applying ops.
        return "__INLINE_SQL__", (sql.strip(),)  # type: ignore[return-value]
    raise ValueError(f"source.kind={kind!r} not supported. Use 'table', 'ref', or 'sql'.")


def _compile_step(step_id: str, source_spec: Dict[str, Any],
                   operations: List[Dict[str, Any]], step_refs: Dict[str, str],
                   dialect: str) -> Tuple[List[Tuple[str, str]], str]:
    """Compile one step into a list of (cte_name, cte_body) pairs.

    Returns (cte_list, last_cte_name). The last_cte_name is what later
    steps will reference via {kind: ref, ref: <step_id>}.
    """
    resolved = _resolve_step_source(source_spec, step_refs)
    initial_ref, seed = resolved

    ctes: List[Tuple[str, str]] = []
    if seed is not None:
        seed_name = f"{step_id}__src"
        ctes.append((seed_name, seed[0]))
        initial_ref = seed_name

    if not operations:
        # Always produce at least one named CTE for this step so later steps
        # can ref it cleanly. Passthrough.
        out_name = f"{step_id}__output"
        ctes.append((out_name, f"SELECT * FROM {_quote(initial_ref, dialect)}"))
        return ctes, out_name

    prev_ref = initial_ref
    for i, op in enumerate(operations):
        kind = op.get("op", "").lower()
        if kind not in _VALID_OPS:
            raise ValueError(f"step {step_id!r} op #{i + 1}: op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")
        cte_name = f"{step_id}__step_{i + 1}"
        body = _build_op_sql(prev_ref, op, dialect, step_refs)
        ctes.append((cte_name, body))
        prev_ref = cte_name
    return ctes, prev_ref


def _compile_pipeline(steps: List[Dict[str, Any]], dialect: str
                      ) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    """Compile all steps. Returns (all_ctes, step_refs)."""
    step_refs: Dict[str, str] = {}
    all_ctes: List[Tuple[str, str]] = []
    seen_ids = set()
    for step in steps:
        sid = step.get("id")
        if not sid:
            raise ValueError("each step requires an 'id' field")
        if sid in seen_ids:
            raise ValueError(f"duplicate step id {sid!r}")
        seen_ids.add(sid)
        source = step.get("source")
        if not source:
            raise ValueError(f"step {sid!r}: 'source' is required")
        ops = step.get("operations") or []
        ctes, last_ref = _compile_step(sid, source, ops, step_refs, dialect)
        all_ctes.extend(ctes)
        step_refs[sid] = last_ref
    return all_ctes, step_refs


def _sanitize_sink_asset_name(table: str) -> str:
    """Turn a fully-qualified table like `mart.gold_orders` into a valid
    Dagster asset name — `mart_gold_orders`. Consumers can override with
    `sink.asset_name` if the auto-derivation isn't right."""
    import re as _re
    s = _re.sub(r"[^0-9a-zA-Z_]+", "_", table).strip("_")
    return s or "warehouse_output"


_MERGE_INTO_DIALECTS = {"duckdb", "snowflake", "bigquery", "databricks", "redshift", "mssql", "oracle"}


def _compile_select(sink: Dict[str, Any], step_refs: Dict[str, str],
                     all_ctes: List[Tuple[str, str]], dialect: str) -> str:
    """The compiled `SELECT` (with any CTEs) a sink's `from` step resolves to -- split out
    from `_emit_sink_sql` so the caller can also run it as a probe query (`mode='merge'`
    needs the SELECT's real output columns, which this pure/no-DB-access function can't
    know on its own)."""
    from_step = sink.get("from")
    if not from_step:
        raise ValueError("each sink requires a 'from' field (matching a step id)")
    if from_step not in step_refs:
        raise ValueError(f"sink.from={from_step!r} doesn't match any step id")
    src_ref = step_refs[from_step]
    if all_ctes:
        with_clause = "WITH " + ",\n  ".join(
            f"{_quote(name, dialect)} AS (\n    {body}\n  )" for name, body in all_ctes
        )
        return f"{with_clause}\nSELECT * FROM {_quote(src_ref, dialect)}"
    return f"SELECT * FROM {_quote(src_ref, dialect)}"


def _emit_sink_sql(sink: Dict[str, Any], step_refs: Dict[str, str],
                    all_ctes: List[Tuple[str, str]], dialect: str,
                    columns: Optional[List[str]] = None) -> Optional[str]:
    """Emit the SQL for one sink. Returns None when the caller must issue
    a DROP + CREATE fallback (mode=replace on dialects without OR REPLACE).

    Supported modes (dbt-analog):
      - `replace` / `overwrite` — CREATE OR REPLACE TABLE (default).
      - `create_if_not_exists` — CREATE TABLE IF NOT EXISTS.
      - `append` — plain INSERT INTO target SELECT ... (creating the table
        first if it doesn't exist yet). No truncation, no incremental
        watermark filter, no dedup — every materialization adds the full
        SELECT's rows on top of whatever is already there. The
        unconditional-insert counterpart to `incremental` (which filters by
        a watermark column) and `truncate_insert` (which wipes first).
      - `view` — CREATE OR REPLACE VIEW. Cheap; the query re-runs on read.
      - `incremental` — INSERT INTO target SELECT ... WHERE
        <incremental_key> > (SELECT COALESCE(MAX(<incremental_key>), <bootstrap>)
        FROM target). On first materialization when the target doesn't
        exist, falls through to a full CTAS. Requires `incremental_key`
        column in the SELECT output.
      - `truncate_insert` — keep the existing table (schema, grants, indexes)
        and replace its rows: TRUNCATE (DELETE on sqlite) then INSERT.
      - `merge` — a true UPDATE-by-key upsert: INSERT...ON CONFLICT
        (postgresql/sqlite), INSERT...ON DUPLICATE KEY UPDATE (mysql), or a
        real MERGE INTO everywhere else (duckdb/snowflake/bigquery/databricks/
        redshift/mssql/oracle -- confirmed per-platform, not assumed).
        Requires `merge_keys: [<column>, ...]`, and `columns` (the caller
        must probe the select's real output columns first -- see
        `_compile_select`, since a portable `UPDATE SET *` wildcard doesn't
        exist: confirmed Databricks-only, Snowflake explicitly rejects it).
    """
    table = sink.get("table")
    if not table:
        raise ValueError("each sink requires a 'table' field")
    mode = (sink.get("mode") or "replace").lower()
    if mode == "overwrite":
        mode = "replace"

    select_sql = _compile_select(sink, step_refs, all_ctes, dialect)
    out_quoted = _quote(table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS\n{select_sql}"
        # postgres / redshift / mssql / mysql: caller will issue DROP + CREATE
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n{select_sql}"
    if mode == "append":
        # Unconditional INSERT — bootstrap the table (schema only, no rows)
        # the first time it doesn't exist yet, same as truncate_insert's own
        # bootstrap, then insert every time with no truncate/filter/dedup.
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n"
            f"WITH _schema AS (\n{select_sql}\n)\n"
            f"SELECT * FROM _schema WHERE 1=0"
        )
        return f"{create_sql};\nINSERT INTO {out_quoted}\n{select_sql}"
    if mode == "view":
        return f"CREATE OR REPLACE VIEW {out_quoted} AS\n{select_sql}"
    if mode == "incremental":
        key = sink.get("incremental_key")
        if not key:
            raise ValueError(
                f"sink {table!r} mode=incremental requires `incremental_key: <column>`"
            )
        # Two-statement plan: create if missing, then INSERT ... WHERE > watermark.
        # Emitted as one string joined by `;` — the caller uses exec_driver_sql
        # which handles multi-statement scripts on supported dialects.
        bootstrap = sink.get("incremental_bootstrap", "NULL")
        merge_key = sink.get("incremental_merge_key")  # optional dedup on merge

        # Build the watermark subquery — use a scalar subquery on the target.
        # When target has no rows, MAX returns NULL → the > filter is falsy for
        # everything → 0 new rows, which is correct on empty tables.
        watermark = (
            f"(SELECT COALESCE(MAX({_quote(key, dialect)}), {bootstrap}) FROM {out_quoted})"
        )
        # Wrap select_sql in a subquery so we can filter on the incremental key.
        insert_sql = (
            f"INSERT INTO {out_quoted}\n"
            f"WITH _incoming AS (\n{select_sql}\n)\n"
            f"SELECT * FROM _incoming WHERE {_quote(key, dialect)} > {watermark}"
        )
        if merge_key:
            # Anti-join dedup on the merge key — keeps incremental idempotent
            # if you re-run the same window twice.
            insert_sql = (
                f"INSERT INTO {out_quoted}\n"
                f"WITH _incoming AS (\n{select_sql}\n)\n"
                f"SELECT i.* FROM _incoming i "
                f"WHERE i.{_quote(key, dialect)} > {watermark} "
                f"AND NOT EXISTS ("
                f"SELECT 1 FROM {out_quoted} t "
                f"WHERE t.{_quote(merge_key, dialect)} = i.{_quote(merge_key, dialect)})"
            )
        # Bootstrap: create the target on first run. Uses CREATE TABLE IF NOT
        # EXISTS with a WHERE FALSE to get the schema without any rows, then
        # the INSERT populates it.
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n"
            f"WITH _schema AS (\n{select_sql}\n)\n"
            f"SELECT * FROM _schema WHERE 1=0"
        )
        return f"{create_sql};\n{insert_sql}"
    if mode == "truncate_insert":
        # Keeps the existing table (schema, grants, indexes) rather than dropping and
        # recreating it, unlike `replace` -- a Mapping Data Flow SQL sink that truncates
        # then inserts, not a full CTAS.
        truncate_stmt = "DELETE FROM" if dialect == "sqlite" else f"TRUNCATE TABLE"
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n"
            f"WITH _schema AS (\n{select_sql}\n)\n"
            f"SELECT * FROM _schema WHERE 1=0"
        )
        return f"{create_sql};\n{truncate_stmt} {out_quoted};\nINSERT INTO {out_quoted}\n{select_sql}"
    if mode == "merge":
        merge_keys = sink.get("merge_keys")
        if isinstance(merge_keys, str):
            merge_keys = [merge_keys]
        if not merge_keys:
            raise ValueError(f"sink {table!r} mode=merge requires `merge_keys: [<column>, ...]`")
        if not columns:
            raise ValueError(f"sink {table!r} mode=merge needs its output columns resolved first (internal: pass columns=)")
        non_key_cols = [c for c in columns if c not in merge_keys]
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n"
            f"WITH _schema AS (\n{select_sql}\n)\n"
            f"SELECT * FROM _schema WHERE 1=0"
        )
        statements = [create_sql]
        insert_cols = ", ".join(_quote(c, dialect) for c in columns)

        if dialect in ("postgresql", "sqlite", "mysql"):
            # ON CONFLICT / ON DUPLICATE KEY UPDATE both require a UNIQUE index on the
            # merge keys to even parse -- CREATE TABLE AS never declares one.
            key_cols_sql = ", ".join(_quote(c, dialect) for c in merge_keys)
            idx_name = _sanitize_sink_asset_name(table) + "_merge_key"
            if dialect == "mysql":
                # No CREATE INDEX IF NOT EXISTS in MySQL -- this only needs to run
                # once; a real repeat run errors clearly ("duplicate key name")
                # rather than silently doing the wrong thing.
                statements.append(f"CREATE UNIQUE INDEX {_quote(idx_name, dialect)} ON {out_quoted} ({key_cols_sql})")
            else:
                statements.append(f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote(idx_name, dialect)} ON {out_quoted} ({key_cols_sql})")

        if dialect in ("postgresql", "sqlite"):
            conflict_cols = ", ".join(_quote(c, dialect) for c in merge_keys)
            if non_key_cols:
                set_clause = ", ".join(f"{_quote(c, dialect)} = EXCLUDED.{_quote(c, dialect)}" for c in non_key_cols)
                action = f"DO UPDATE SET {set_clause}"
            else:
                action = "DO NOTHING"
            # WHERE 1=1 before ON CONFLICT is load-bearing on SQLite -- see
            # sql_to_database_asset's build_dialect_upsert_sql, same empirically
            # confirmed parser quirk ("near 'DO': syntax error" without it).
            statements.append(
                f"INSERT INTO {out_quoted} ({insert_cols})\n{select_sql} WHERE 1=1\n"
                f"ON CONFLICT ({conflict_cols}) {action}"
            )
        elif dialect == "mysql":
            if non_key_cols:
                set_clause = ", ".join(f"{_quote(c, dialect)} = VALUES({_quote(c, dialect)})" for c in non_key_cols)
                statements.append(f"INSERT INTO {out_quoted} ({insert_cols})\n{select_sql}\nON DUPLICATE KEY UPDATE {set_clause}")
            else:
                statements.append(f"INSERT IGNORE INTO {out_quoted} ({insert_cols})\n{select_sql}")
        elif dialect in _MERGE_INTO_DIALECTS:
            on_clause = " AND ".join(f"t.{_quote(c, dialect)} = s.{_quote(c, dialect)}" for c in merge_keys)
            insert_vals = ", ".join(f"s.{_quote(c, dialect)}" for c in columns)
            update_clause = ""
            if non_key_cols:
                set_clause = ", ".join(f"{_quote(c, dialect)} = s.{_quote(c, dialect)}" for c in non_key_cols)
                update_clause = f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            statements.append(
                f"MERGE INTO {out_quoted} AS t USING (\n{select_sql}\n) AS s ON {on_clause} "
                f"{update_clause}"
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
        else:
            raise ValueError(f"sink {table!r} mode=merge has no known syntax for dialect {dialect!r}.")
        return ";\n".join(statements)
    raise ValueError(
        f"sink.mode must be 'replace' | 'overwrite' | 'create_if_not_exists' | "
        f"'append' | 'view' | 'incremental' | 'truncate_insert' | 'merge', got {mode!r}"
    )



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


class WarehousePipelineComponent(Component, Model, Resolvable):
    """Multi-step warehouse-native pipeline compiled to ONE plan per sink.

    Two equivalent shapes are accepted:

    * **Flat shape (one source, one ops chain, one sink)** — top-level `source` + `operations` +
      `output_table` + `mode`. Compiles to one anonymous step + one anonymous
      sink. Use this for the common case of one input → one output.

    * **Multi-step `steps:` form** — list of named steps (each with its own
      `source` and `operations`) plus a `sinks:` list. Use this when you
      need multiple sources, inter-step joins/unions via `{ref: <id>}`, an
      `op: sql` escape hatch for ad-hoc SQL the DSL doesn't model, or
      multiple sink tables from one asset.

    Supported ops (in any step): filter / with_columns / select / drop /
    group_by / sort / limit / top_n / top_n_per_group / dedup / distinct /
    union / join / sql / pivot / lookup / window_calculation.

    The `op: sql` body may reference `<<self>>` (previous CTE in this
    step) or `<<step_id>>` (any earlier step's output).
    """

    asset_name: str = Field(description="Output Dagster asset name")
    database_url: Optional[str] = Field(default=None)
    database_url_env_var: Optional[str] = Field(default=None)
    dialect: str = Field(description=f"SQL dialect: one of {sorted(_SUPPORTED_DIALECTS)}.")

    # Flat-shape single-source shape ------------------------------------------
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Single-source sugar: {upstream_table: 'schema.table'} or {kind: table|sql, ...}",
    )
    operations: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Flat shape: ordered list of ops applied to 'source'. Compiles to one anonymous step.",
    )
    output_table: Optional[str] = Field(default=None, description="Flat shape: destination table for the single-source shape")
    mode: Optional[str] = Field(default=None, description="Sink mode (flat shape): 'replace', 'create_if_not_exists', 'append', 'view', or 'truncate_insert'. 'incremental'/'merge' need per-sink params only available via the multi-sink 'sinks' field.")

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named steps. Each: {id, source: {kind: table|ref|sql, ...}, operations: [...]}. "
            "Required when using the multi-step form."
        ),
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Output sinks. Each: {from: <step_id>, table: 'schema.table', "
            "mode: replace|create_if_not_exists|append|view|incremental|truncate_insert|merge}. "
            "'append' is a plain unconditional INSERT (no truncate, no watermark filter, "
            "no dedup) -- for incremental/dedup'd inserts use 'incremental'/'merge' instead. "
            "'incremental' needs incremental_key (+ optional incremental_merge_key for "
            "insert-only dedup). 'truncate_insert' keeps the existing table (schema, "
            "grants, indexes) and replaces its rows. 'merge' needs merge_keys: "
            "[<column>, ...] -- a true UPDATE-by-key upsert (INSERT...ON CONFLICT / "
            "ON DUPLICATE KEY UPDATE / a real MERGE INTO, chosen by dialect). "
            "Required when using the multi-step form. Multiple sinks emit multiple CTAS statements."
        ),
    )

    # Asset metadata -------------------------------------------------------
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    # ── Partitions ──
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic', 'multi', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run -- the partition_key "
            "token threaded through this pipeline's step templating becomes a "
            "comma-joined string spanning the whole range (not a single key) when "
            "the run covers a range. 'multi_run' launches one run per partition "
            "(optionally capped via backfill_policy_max_partitions_per_run). Leave "
            "unset for Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem/default IO manager cannot persist a "
            "single_run output spanning multiple partitions and will raise a clear "
            "error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'us,eu,apac'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )

    # ── Retry policy ──
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on materialization failure. Defines a RetryPolicy. Useful for transient warehouse failures, query timeouts, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    automation_condition: Optional[Any] = Field(
        default=None,
        description=(
            "AutomationCondition for this asset. In YAML, write as a Jinja "
            "template against the dg namespace, e.g. "
            "'{{ dg.AutomationCondition.eager() }}' — Dagster's component "
            "loader resolves it to the actual AutomationCondition object."
        ),
    )
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=200)
    return_dataframe: bool = Field(
        default=False,
        description=(
            "When true, after running the CTAS the asset SELECTs from the "
            "primary sink table and returns the result as a pandas DataFrame "
            "so downstream pandas-consuming assets can read it via Dagster's "
            "IO manager. the "
            "boundary where data leaves the warehouse and lands in memory. "
            "Leave false (default) for pure SQL chains that end at a final "
            "warehouse table; flip on when this is the bridge step between "
            "warehouse-side SQL and downstream pandas work."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step warehouse-native pipeline compiled to one CTE-CTAS plan per sink. YAML-defined stored-procedure shape."

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

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Return (steps, sinks) regardless of which YAML shape was used."""
        flat_present = bool(self.source or self.operations or self.output_table or self.mode)
        multi_present = bool(self.steps or self.sinks)
        if multi_present and flat_present:
            raise ValueError(
                "warehouse_pipeline: choose ONE shape — either top-level "
                "source/operations/output_table OR steps/sinks, not both."
            )
        if multi_present:
            if not self.steps:
                raise ValueError("warehouse_pipeline: 'sinks' provided without 'steps'.")
            if not self.sinks:
                raise ValueError("warehouse_pipeline: 'steps' provided without 'sinks'.")
            return list(self.steps), list(self.sinks)
        if not (self.source and self.operations is not None and self.output_table):
            raise ValueError(
                "warehouse_pipeline: provide either 'steps' + 'sinks' OR top-level "
                "'source' + 'operations' + 'output_table'."
            )
        flat_step = {
            "id": "_default",
            "source": dict(self.source),
            "operations": list(self.operations),
        }
        flat_sink = {
            "from": "_default",
            "table": self.output_table,
            "mode": (self.mode or "replace").lower(),
        }
        return [flat_step], [flat_sink]

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Warehouse Pipeline', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        dialect = self.dialect.lower()
        if dialect not in _SUPPORTED_DIALECTS:
            raise ValueError(f"dialect={self.dialect!r} not supported. Use one of {sorted(_SUPPORTED_DIALECTS)}.")
        steps, sinks = self._normalize()
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        return_dataframe = self.return_dataframe
        kinds = list(self.kinds or []) or [dialect, "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""
        resolve_url = self._resolve_url

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        # Decide the shape: single-sink → @asset (backward compat with the
        # flat shape). Multi-sink → @multi_asset(can_subset=True), one Dagster
        # asset per sink — the dbt-model equivalent.
        use_multi_asset = len(sinks) > 1

        # Derive per-sink Dagster asset names. Explicit `sink.asset_name`
        # wins; otherwise sanitize `sink.table` (replace `.` with `_`, etc.)
        for _sink in sinks:
            if "asset_name" not in _sink:
                _sink["asset_name"] = _sanitize_sink_asset_name(_sink["table"])

        if use_multi_asset:
            return self._build_multi_asset_defs(
                sinks=sinks, steps=steps, dialect=dialect,
                partitions_def=partitions_def, retry_policy=_retry_policy,
                backfill_policy=backfill_policy,
                all_tags=all_tags, kinds=kinds, resolve_url=resolve_url,
                include_preview=include_preview, preview_rows=preview_rows,
            )

        # Single-sink path — @asset, backward-compatible.
        asset_kwargs: Dict[str, Any] = dict(
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        if partitions_def is not None:
            asset_kwargs["partitions_def"] = partitions_def
        if backfill_policy is not None:
            asset_kwargs["backfill_policy"] = backfill_policy
        if self.automation_condition is not None:
            asset_kwargs["automation_condition"] = self.automation_condition
        if _retry_policy is not None:
            asset_kwargs["retry_policy"] = _retry_policy

        @asset(**asset_kwargs)
        def _warehouse_pipeline_asset(context: AssetExecutionContext):
            import sqlalchemy
            import time as _time
            # Substitute <<partition_key>> placeholders in steps/sinks so the
            # user can reference the current partition in op:filter predicates,
            # op:sql bodies, and sink table names without their own templating.
            if context.has_partition_key:
                partition_key = context.partition_key
            elif context.has_partition_key_range:
                # single_run backfill: represent the whole range as one joined token.
                partition_key = ",".join(str(k) for k in context.partition_keys)
            else:
                partition_key = None
            local_steps: List[Dict[str, Any]] = (
                _substitute_partition_key(steps, partition_key) if partition_key else steps  # type: ignore[assignment]
            )
            local_sinks: List[Dict[str, Any]] = (
                _substitute_partition_key(sinks, partition_key) if partition_key else sinks  # type: ignore[assignment]
            )
            if partition_key:
                context.log.info(f"warehouse_pipeline: partition_key={partition_key!r}")
            engine = sqlalchemy.create_engine(resolve_url())
            _compile_t0 = _time.time()
            all_ctes, step_refs = _compile_pipeline(local_steps, dialect)  # type: ignore[arg-type]
            compile_seconds = round(_time.time() - _compile_t0, 4)
            context.log.info(
                f"warehouse_pipeline: compiled {len(steps)} step(s), "
                f"{len(all_ctes)} CTE(s), into {len(sinks)} sink(s) in {compile_seconds}s"
            )

            sink_metadata: Dict[str, Any] = {}
            primary_row_count = 0
            sql_log: List[str] = []
            total_execution_seconds = 0.0
            with engine.begin() as conn:
                for sink in local_sinks:  # type: ignore[union-attr]
                    merge_columns = None
                    if (sink.get("mode") or "").lower() == "merge":
                        # No portable "UPDATE SET *" wildcard exists (Databricks-only;
                        # Snowflake explicitly rejects it) -- probe the select's real
                        # output columns before building the MERGE/ON CONFLICT SQL.
                        probe_select = _compile_select(sink, step_refs, all_ctes, dialect)
                        merge_columns = list(conn.exec_driver_sql(f"SELECT * FROM (\n{probe_select}\n) AS _probe WHERE 1=0").keys())
                    sql = _emit_sink_sql(sink, step_refs, all_ctes, dialect, columns=merge_columns)
                    if sql is None:
                        # Dialect without CREATE OR REPLACE — DROP + CREATE.
                        out_quoted = _quote(sink["table"], dialect)
                        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {out_quoted}")
                        sink_for_create = dict(sink, mode="create_if_not_exists")
                        sql = _emit_sink_sql(sink_for_create, step_refs, all_ctes, dialect)
                    context.log.info(f"sink {sink['table']}: executing")
                    assert sql is not None, "emit_sink_sql returned None post-fallback"
                    sql_log.append(f"-- → {sink['table']}\n{sql}")
                    _exec_t0 = _time.time()
                    # Split multi-statement SQL (used by mode=incremental)
                    # into individual statements — many drivers only accept
                    # one statement per exec_driver_sql call.
                    for _stmt in [s.strip() for s in sql.split(";\n") if s.strip()]:
                        conn.exec_driver_sql(_stmt)  # type: ignore[arg-type]
                    exec_seconds = round(_time.time() - _exec_t0, 4)
                    total_execution_seconds += exec_seconds
                    row_count = int(conn.exec_driver_sql(
                        f"SELECT COUNT(*) FROM {_quote(sink['table'], dialect)}"
                    ).scalar() or 0)
                    sink_metadata[f"warehouse/{sink['table']}/row_count"] = MetadataValue.int(row_count)
                    sink_metadata[f"warehouse/{sink['table']}/execution_seconds"] = MetadataValue.float(exec_seconds)
                    sink_metadata[f"warehouse/{sink['table']}/rows_per_second"] = MetadataValue.float(
                        round(row_count / max(exec_seconds, 1e-6), 1)
                    )
                    context.log.info(
                        f"sink {sink['table']}: {row_count} rows in {exec_seconds}s "
                        f"({round(row_count / max(exec_seconds, 1e-6), 1)} rows/s)"
                    )
                    if not primary_row_count:
                        primary_row_count = row_count

                # Column schema for the primary sink — pulled via INFORMATION_SCHEMA
                # equivalent through SQLAlchemy's inspector so it works across all
                # supported dialects.
                col_schema_meta = None
                try:
                    from dagster import TableSchema, TableColumn
                    inspector = sqlalchemy.inspect(engine)
                    primary_sink_table = sinks[0]["table"]
                    # Split "schema.table" if present.
                    schema_name = None
                    table_only = primary_sink_table
                    if "." in primary_sink_table:
                        schema_name, table_only = primary_sink_table.split(".", 1)
                    cols = inspector.get_columns(table_only, schema=schema_name)
                    if cols:
                        col_schema_meta = MetadataValue.table_schema(TableSchema(columns=[
                            TableColumn(name=c["name"], type=str(c.get("type", "unknown")))
                            for c in cols
                        ]))
                except Exception as _e:  # noqa: BLE001
                    context.log.warning(f"warehouse_pipeline: column schema probe failed: {_e}")

                metadata: Dict[str, Any] = {
                    "dagster/row_count": MetadataValue.int(primary_row_count),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/step_count": MetadataValue.int(len(steps)),
                    "warehouse/sink_count": MetadataValue.int(len(sinks)),
                    "warehouse/compile_seconds": MetadataValue.float(compile_seconds),
                    "warehouse/execution_seconds": MetadataValue.float(round(total_execution_seconds, 4)),
                    "warehouse/rows_per_second": MetadataValue.float(
                        round(primary_row_count / max(total_execution_seconds, 1e-6), 1)
                    ),
                    "warehouse/cte_count": MetadataValue.int(len(all_ctes)),
                    "warehouse/sql": MetadataValue.md("```sql\n" + "\n\n".join(sql_log) + "\n```"),
                }
                if col_schema_meta is not None:
                    metadata["dagster/column_schema"] = col_schema_meta
                metadata.update(sink_metadata)
                if include_preview and primary_row_count > 0:
                    primary = sinks[0]["table"]
                    try:
                        prev_rows = conn.exec_driver_sql(
                            f"SELECT * FROM {_quote(primary, dialect)} LIMIT {preview_rows}"
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

                if return_dataframe:
                    # Bridge: pull the primary sink back into pandas so
                    # downstream non-warehouse assets can consume it via the
                    # IO manager. Same connection, no extra round-trip.
                    import pandas as pd
                    primary_table = sinks[0]["table"]
                    df = pd.read_sql(
                        f"SELECT * FROM {_quote(primary_table, dialect)}", conn
                    )
                    metadata["dagster/row_count"] = MetadataValue.int(len(df))
                    context.add_output_metadata(metadata)
                    return df
            return dg.MaterializeResult(metadata=metadata)

        return Definitions(assets=[_warehouse_pipeline_asset])

    def _build_multi_asset_defs(
        self,
        *,
        sinks: List[Dict[str, Any]],
        steps: List[Dict[str, Any]],
        dialect: str,
        partitions_def,
        retry_policy,
        backfill_policy=None,
        all_tags: Dict[str, str],
        kinds: List[str],
        resolve_url,
        include_preview: bool,
        preview_rows: int,
    ) -> Definitions:
        """Multi-sink path — one Dagster asset per sink. `can_subset=True`
        so users can retry or backfill a single sink without rerunning
        the whole pipeline. dbt-analog: each sink is a "model"."""
        from dagster import AssetOut, multi_asset, MaterializeResult, AssetKey as _AssetKey
        outs: Dict[str, AssetOut] = {}
        # Cross-sink deps: if sink A's `from` step also feeds sink B, then
        # from Dagster's POV those sinks are peers (not upstream/downstream
        # of each other). Lineage between sinks would require a separate
        # design. Today all sinks share the same CTE preamble → they're
        # siblings under the same @multi_asset.
        for sink in sinks:
            out_kwargs: Dict[str, Any] = {
                "kinds": set(kinds),
                "group_name": self.group_name,
            }
            if sink.get("description"):
                out_kwargs["description"] = sink["description"]
            elif self.description:
                out_kwargs["description"] = self.description
            outs[sink["asset_name"]] = AssetOut(**out_kwargs)

        # multi_asset() accepts a subset of @asset kwargs — `owners` + `tags`
        # live on each AssetOut rather than on the decorator. `is_required=False`
        # is critical: with `can_subset=True`, Dagster still expects every
        # declared output to be yielded PER RUN unless the output is marked
        # optional. is_required=False lets the compute yield only the
        # selected sinks without triggering DagsterStepOutputNotFoundError.
        outs = {name: AssetOut(**{
            "is_required": False,
            **({"kinds": set(kinds)} if kinds else {}),
            **({"group_name": self.group_name} if self.group_name else {}),
            **({"owners": self.owners} if self.owners else {}),
            **({"tags": all_tags} if all_tags else {}),
            **({"description": _sink.get("description") or self.description}
               if (_sink.get("description") or self.description) else {}),
        }) for (name, _), _sink in zip(outs.items(), sinks)}

        ma_kwargs: Dict[str, Any] = dict(
            outs=outs,
            can_subset=True,
            name=self.asset_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        if partitions_def is not None:
            ma_kwargs["partitions_def"] = partitions_def
        if retry_policy is not None:
            ma_kwargs["retry_policy"] = retry_policy
        if backfill_policy is not None:
            ma_kwargs["backfill_policy"] = backfill_policy

        # Snapshot the closed-over refs for the compute fn.
        _sinks = list(sinks)
        _steps = list(steps)
        _dialect = dialect
        _include_preview = include_preview
        _preview_rows = preview_rows

        @multi_asset(**ma_kwargs)
        def _warehouse_pipeline_multi_asset(context: AssetExecutionContext):
            import sqlalchemy
            import time as _time
            if context.has_partition_key:
                partition_key = context.partition_key
            elif context.has_partition_key_range:
                # single_run backfill: represent the whole range as one joined token.
                partition_key = ",".join(str(k) for k in context.partition_keys)
            else:
                partition_key = None
            local_steps: List[Dict[str, Any]] = (
                _substitute_partition_key(_steps, partition_key) if partition_key else _steps  # type: ignore[assignment]
            )
            local_sinks: List[Dict[str, Any]] = (
                _substitute_partition_key(_sinks, partition_key) if partition_key else _sinks  # type: ignore[assignment]
            )
            if partition_key:
                context.log.info(f"warehouse_pipeline: partition_key={partition_key!r}")

            # Determine which sinks are selected for this run. can_subset=True
            # means a run may materialize a subset. If nothing is queried,
            # default to all (defensive — Dagster usually provides this).
            try:
                selected_names = set(context.op_execution_context.selected_output_names)  # type: ignore[union-attr]
            except Exception:  # noqa: BLE001
                selected_names = {s["asset_name"] for s in local_sinks}
            selected_sinks = [s for s in local_sinks if s["asset_name"] in selected_names]
            if not selected_sinks:
                # All were somehow deselected — nothing to do.
                context.log.info("warehouse_pipeline: no sinks selected; skipping.")
                return

            context.log.info(
                f"warehouse_pipeline: {len(selected_sinks)}/{len(local_sinks)} sink(s) selected: "
                f"{[s['asset_name'] for s in selected_sinks]}"
            )

            engine = sqlalchemy.create_engine(resolve_url())
            _compile_t0 = _time.time()
            # Compile the FULL CTE chain — selected sinks may reference any
            # earlier step's output. Compilation is cheap; execution is what
            # subsetting saves.
            all_ctes, step_refs = _compile_pipeline(local_steps, _dialect)  # type: ignore[arg-type]
            compile_seconds = round(_time.time() - _compile_t0, 4)

            with engine.begin() as conn:
                # Auto-create any schema referenced in a sink `table:` (dbt-parity).
                _schemas_needed = {
                    t.split(".", 1)[0] for t in (s["table"] for s in selected_sinks) if "." in t
                }
                for _sch in sorted(_schemas_needed):
                    try:
                        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_quote(_sch, _dialect)}")
                    except Exception as _e:  # noqa: BLE001
                        context.log.warning(f"schema {_sch!r} auto-create failed: {_e}")
                for sink in selected_sinks:
                    merge_columns = None
                    if (sink.get("mode") or "").lower() == "merge":
                        probe_select = _compile_select(sink, step_refs, all_ctes, _dialect)
                        merge_columns = list(conn.exec_driver_sql(f"SELECT * FROM (\n{probe_select}\n) AS _probe WHERE 1=0").keys())
                    sql = _emit_sink_sql(sink, step_refs, all_ctes, _dialect, columns=merge_columns)
                    if sql is None:
                        out_quoted = _quote(sink["table"], _dialect)
                        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {out_quoted}")
                        sink_for_create = dict(sink, mode="create_if_not_exists")
                        sql = _emit_sink_sql(sink_for_create, step_refs, all_ctes, _dialect)
                    assert sql is not None, "emit_sink_sql returned None post-fallback"
                    context.log.info(f"sink {sink['table']}: executing")
                    _exec_t0 = _time.time()
                    for _stmt in [s.strip() for s in sql.split(";\n") if s.strip()]:
                        conn.exec_driver_sql(_stmt)  # type: ignore[arg-type]
                    exec_seconds = round(_time.time() - _exec_t0, 4)
                    # Views don't have a stable row count; COUNT(*) still works.
                    row_count = int(conn.exec_driver_sql(
                        f"SELECT COUNT(*) FROM {_quote(sink['table'], _dialect)}"
                    ).scalar() or 0)

                    # Column schema for THIS sink (per-asset metadata).
                    col_schema_meta = None
                    try:
                        from dagster import TableSchema, TableColumn
                        inspector = sqlalchemy.inspect(engine)
                        schema_name = None
                        table_only = sink["table"]
                        if "." in table_only:
                            schema_name, table_only = table_only.split(".", 1)
                        cols = inspector.get_columns(table_only, schema=schema_name)
                        if cols:
                            col_schema_meta = MetadataValue.table_schema(TableSchema(columns=[
                                TableColumn(name=c["name"], type=str(c.get("type", "unknown")))
                                for c in cols
                            ]))
                    except Exception:  # noqa: BLE001
                        pass

                    md: Dict[str, Any] = {
                        "dagster/row_count": MetadataValue.int(row_count),
                        "warehouse/dialect": MetadataValue.text(_dialect),
                        "warehouse/mode": MetadataValue.text(sink.get("mode") or "replace"),
                        "warehouse/table": MetadataValue.text(sink["table"]),
                        "warehouse/compile_seconds": MetadataValue.float(compile_seconds),
                        "warehouse/execution_seconds": MetadataValue.float(exec_seconds),
                        "warehouse/rows_per_second": MetadataValue.float(
                            round(row_count / max(exec_seconds, 1e-6), 1)
                        ),
                        "warehouse/cte_count": MetadataValue.int(len(all_ctes)),
                        "warehouse/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    }
                    if col_schema_meta is not None:
                        md["dagster/column_schema"] = col_schema_meta
                    if _include_preview and row_count > 0:
                        try:
                            prev_rows = conn.exec_driver_sql(
                                f"SELECT * FROM {_quote(sink['table'], _dialect)} LIMIT {_preview_rows}"
                            ).fetchall()
                            if prev_rows:
                                cols_p = list(prev_rows[0]._mapping.keys())
                                md["preview"] = MetadataValue.md(
                                    "| " + " | ".join(cols_p) + " |\n"
                                    "| " + " | ".join(["---"] * len(cols_p)) + " |\n" +
                                    "\n".join("| " + " | ".join(str(v) for v in r) + " |" for r in prev_rows)
                                )
                        except Exception as e:  # noqa: BLE001
                            context.log.warning(f"preview emission failed: {e}")

                    context.log.info(
                        f"sink {sink['table']}: {row_count} rows in {exec_seconds}s "
                        f"({round(row_count / max(exec_seconds, 1e-6), 1)} rows/s)"
                    )
                    yield MaterializeResult(
                        asset_key=_AssetKey.from_user_string(sink["asset_name"]),
                        metadata=md,
                    )

        return Definitions(assets=[_warehouse_pipeline_multi_asset])
