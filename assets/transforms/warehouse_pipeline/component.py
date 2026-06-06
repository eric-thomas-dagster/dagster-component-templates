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
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from dagster import (
    AssetExecutionContext,
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
):
    """Build a PartitionsDefinition matching the canonical dataframe_to_snowflake
    factory. Supports daily/weekly/monthly/hourly/static/dynamic/multi."""
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
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
              "dedup", "distinct", "union", "join", "sql"}

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


def _resolve_sql_template(sql: str, prev_ref: str, step_refs: Dict[str, str], dialect: str) -> str:
    """Replace <<self>> and <<step_id>> placeholders with quoted CTE names.

    Placeholder syntax is `<<name>>` (angle-bracket chevrons) — chosen so it
    doesn't collide with Jinja `{{ ... }}`, which Dagster's component YAML
    loader pre-renders before this component sees the value.
    """
    out = sql
    out = out.replace("<<self>>", _quote(prev_ref, dialect))
    out = out.replace("<< self >>", _quote(prev_ref, dialect))
    for sid, ref in step_refs.items():
        out = out.replace(f"<<{sid}>>", _quote(ref, dialect))
        out = out.replace(f"<< {sid} >>", _quote(ref, dialect))
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
        if dialect in ("duckdb", "bigquery", "snowflake", "databricks"):
            cols = ", ".join(_quote(c, dialect) for c in op["columns"])
            return f"SELECT * EXCEPT ({cols}) FROM {prev}"
        raise ValueError(f"warehouse_pipeline op='drop' needs SELECT * EXCEPT(); not supported on {dialect}. Use 'select' to enumerate the kept cols.")
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
        if isinstance(right, dict) and "ref" in right:
            right_ref = step_refs.get(right["ref"])
            if right_ref is None:
                raise ValueError(f"join.right.ref={right['ref']!r} doesn't match any earlier step id")
            right_quoted = _quote(right_ref, dialect)
        elif isinstance(right, str):
            right_quoted = _quote(right, dialect)
        else:
            raise ValueError("join.right must be a table string or {ref: <step_id>}")
        how = op.get("how", "inner").upper()
        if how == "OUTER":
            how = "FULL OUTER"
        on_columns = op.get("on_columns") or op.get("on")
        left_on = op.get("left_on")
        right_on = op.get("right_on")
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


def _emit_sink_sql(sink: Dict[str, Any], step_refs: Dict[str, str],
                    all_ctes: List[Tuple[str, str]], dialect: str) -> Optional[str]:
    """One CTAS per sink. Returns None if mode=replace on a dialect without OR REPLACE."""
    from_step = sink.get("from")
    if not from_step:
        raise ValueError("each sink requires a 'from' field (matching a step id)")
    if from_step not in step_refs:
        raise ValueError(f"sink.from={from_step!r} doesn't match any step id")
    table = sink.get("table")
    if not table:
        raise ValueError("each sink requires a 'table' field")
    mode = (sink.get("mode") or "replace").lower()
    if mode == "overwrite":
        mode = "replace"

    src_ref = step_refs[from_step]
    if all_ctes:
        with_clause = "WITH " + ",\n  ".join(
            f"{_quote(name, dialect)} AS (\n    {body}\n  )" for name, body in all_ctes
        )
        select_sql = f"{with_clause}\nSELECT * FROM {_quote(src_ref, dialect)}"
    else:
        select_sql = f"SELECT * FROM {_quote(src_ref, dialect)}"

    out_quoted = _quote(table, dialect)
    if mode == "replace":
        if dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            return f"CREATE OR REPLACE TABLE {out_quoted} AS\n{select_sql}"
        # postgres / redshift / mssql / mysql: caller will issue DROP + CREATE
        return None
    if mode == "create_if_not_exists":
        return f"CREATE TABLE IF NOT EXISTS {out_quoted} AS\n{select_sql}"
    raise ValueError(f"sink.mode must be 'replace'/'overwrite' or 'create_if_not_exists', got {mode!r}")


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
    union / join / sql.

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
    mode: Optional[str] = Field(default=None, description="Sink mode (flat shape): 'replace' or 'create_if_not_exists'")

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
            "Output sinks. Each: {from: <step_id>, table: 'schema.table', mode: replace|create_if_not_exists}. "
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
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
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
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
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
            "IO manager. Equivalent to Alteryx's In-DB Stream Out tool — the "
            "boundary where data leaves the warehouse and lands in memory. "
            "Leave false (default) for pure SQL chains that end at a final "
            "warehouse table; flip on when this is the bridge step between "
            "warehouse-side SQL and downstream pandas work."
        ),
    )

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
        )

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
        if self.automation_condition is not None:
            asset_kwargs["automation_condition"] = self.automation_condition

        # Retry policy (opt-in via retry_policy_max_retries).
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            asset_kwargs["retry_policy"] = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(**asset_kwargs)
        def _warehouse_pipeline_asset(context: AssetExecutionContext):
            import sqlalchemy
            # Substitute <<partition_key>> placeholders in steps/sinks so the
            # user can reference the current partition in op:filter predicates,
            # op:sql bodies, and sink table names without their own templating.
            partition_key = context.partition_key if context.has_partition_key else None
            local_steps: List[Dict[str, Any]] = (
                _substitute_partition_key(steps, partition_key) if partition_key else steps  # type: ignore[assignment]
            )
            local_sinks: List[Dict[str, Any]] = (
                _substitute_partition_key(sinks, partition_key) if partition_key else sinks  # type: ignore[assignment]
            )
            if partition_key:
                context.log.info(f"warehouse_pipeline: partition_key={partition_key!r}")
            engine = sqlalchemy.create_engine(resolve_url())
            all_ctes, step_refs = _compile_pipeline(local_steps, dialect)  # type: ignore[arg-type]
            context.log.info(
                f"warehouse_pipeline: compiled {len(steps)} step(s), "
                f"{len(all_ctes)} CTE(s), into {len(sinks)} sink(s)"
            )

            sink_metadata: Dict[str, Any] = {}
            primary_row_count = 0
            sql_log: List[str] = []
            with engine.begin() as conn:
                for sink in local_sinks:  # type: ignore[union-attr]
                    sql = _emit_sink_sql(sink, step_refs, all_ctes, dialect)
                    if sql is None:
                        # Dialect without CREATE OR REPLACE — DROP + CREATE.
                        out_quoted = _quote(sink["table"], dialect)
                        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {out_quoted}")
                        sink_for_create = dict(sink, mode="create_if_not_exists")
                        sql = _emit_sink_sql(sink_for_create, step_refs, all_ctes, dialect)
                    context.log.info(f"sink {sink['table']}: executing")
                    sql_log.append(f"-- → {sink['table']}\n{sql}")
                    conn.exec_driver_sql(sql)  # type: ignore[arg-type]
                    row_count = int(conn.exec_driver_sql(
                        f"SELECT COUNT(*) FROM {_quote(sink['table'], dialect)}"
                    ).scalar() or 0)
                    sink_metadata[f"warehouse/{sink['table']}/row_count"] = MetadataValue.int(row_count)
                    if not primary_row_count:
                        primary_row_count = row_count

                metadata: Dict[str, Any] = {
                    "dagster/row_count": MetadataValue.int(primary_row_count),
                    "warehouse/dialect": MetadataValue.text(dialect),
                    "warehouse/step_count": MetadataValue.int(len(steps)),
                    "warehouse/sink_count": MetadataValue.int(len(sinks)),
                    "warehouse/sql": MetadataValue.md("```sql\n" + "\n\n".join(sql_log) + "\n```"),
                }
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
                    # IO manager. Equivalent to Alteryx's In-DB Stream Out
                    # tool — same connection, no extra round-trip.
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
