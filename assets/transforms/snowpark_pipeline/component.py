"""SnowparkPipelineComponent — single-asset multi-step Snowpark pipeline.

Snowpark's DataFrame API is fully lazy: every op against a DataFrame builds
a logical query plan, and the plan is compiled to Snowflake SQL at a
terminal action. This component builds the WHOLE pipeline as ONE plan —
across every step, every op, every sink — so the Snowflake optimizer can
fuse predicates, prune projections, reorder joins, and execute everything
server-side. No data ever flows through Python.

Two YAML shapes — both run inside a single Dagster asset / single
Snowpark Session:

  (a) Flat shape (one source, one ops chain, one sink) — top-level `source` + `operations` + `sink`:

      source: {kind: table, table: RAW.ORDERS}
      operations: [...]
      sink:   {kind: table, table: ANALYTICS.OUT, mode: overwrite}

  (b) Multi-step `steps:` form with `sinks:` (plural):

      steps:
        - id: paid_orders
          source: {kind: table, table: RAW.ORDERS}
          operations:
            - {op: filter, predicate: "STATUS = 'paid'"}
        - id: gold_customers
          source: {kind: table, table: RAW.CUSTOMERS}
          operations:
            - {op: filter, predicate: "TIER = 'gold'"}
        - id: enriched
          source: {kind: ref, ref: paid_orders}
          operations:
            - {op: join, right: {ref: gold_customers}, on_columns: [CUSTOMER_ID], how: inner}
            - {op: sql, sql: "SELECT *, AMOUNT * 0.15 AS COMMISSION FROM self"}
            - {op: group_by, group_by: [REGION],
               aggregations: {REVENUE: {col: AMOUNT, agg: sum}}}
      sinks:
        - {from: enriched, kind: table, table: ANALYTICS.ENRICHED, mode: overwrite}

Source kinds: table, sql, ref.
Sink kinds:   table, none (collect to pandas).
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


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name: raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _substitute_partition_key(obj, partition_key):
    """Walk a nested dict/list/str and replace `<<partition_key>>` with the
    runtime partition key. Lets users reference the partition in op:filter
    predicates / op:sql / sink table names."""
    if partition_key is None: return obj
    if isinstance(obj, str): return obj.replace("<<partition_key>>", str(partition_key))
    if isinstance(obj, list): return [_substitute_partition_key(x, partition_key) for x in obj]
    if isinstance(obj, dict): return {k: _substitute_partition_key(v, partition_key) for k, v in obj.items()}
    return obj


_VALID_OPS = {"filter", "select", "drop", "rename", "with_columns",
              "group_by", "sort", "limit", "distinct", "drop_nulls",
              "join", "union", "sql"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count",
                    "count_distinct", "stddev", "variance"}


def _apply_op(session, df, op: Dict[str, Any], step_outputs: Dict[str, Any]):
    from snowflake.snowpark import functions as F
    kind = op["op"].lower()
    if kind == "filter":
        return df.filter(F.sql_expr(op["predicate"]))
    if kind == "select":
        return df.select(*op["columns"])
    if kind == "drop":
        return df.drop(*op["columns"])
    if kind == "rename":
        out = df
        for old, new in op["mapping"].items():
            out = out.rename({old: new})
        return out
    if kind == "with_columns":
        out = df
        for name, expr_str in op["expressions"].items():
            out = out.with_column(name, F.sql_expr(expr_str))
        return out
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        agg_exprs = []
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                src_col, func = spec["col"], spec["agg"]
            else:
                src_col, func = out_col, spec
            f = func.lower()
            if f not in _SUPPORTED_AGGS:
                raise ValueError(
                    f"snowpark_pipeline: agg func {func!r} not supported. "
                    f"Use one of {sorted(_SUPPORTED_AGGS)}"
                )
            _fn_map = {"mean": "avg"}
            agg_fn = getattr(F, _fn_map.get(f, f))
            agg_exprs.append(agg_fn(F.col(src_col)).alias(out_col))
        return df.group_by(*group_by).agg(*agg_exprs)
    if kind == "sort":
        by = op["by"] if isinstance(op["by"], list) else [op["by"]]
        descending = op.get("descending", False)
        descending = descending if isinstance(descending, list) else [descending] * len(by)
        cols = [(F.col(c).desc() if d else F.col(c).asc()) for c, d in zip(by, descending)]
        return df.sort(*cols)
    if kind == "limit":
        return df.limit(op["n"])
    if kind == "distinct":
        return df.distinct()
    if kind == "drop_nulls":
        subset = op.get("subset")
        return df.na.drop(subset=subset) if subset else df.na.drop()
    if kind == "join":
        right_spec = op["right"]
        if isinstance(right_spec, dict) and "ref" in right_spec:
            right_id = right_spec["ref"]
            if right_id not in step_outputs:
                raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
            right_df = step_outputs[right_id]
        elif isinstance(right_spec, dict) and "table" in right_spec:
            right_df = session.table(right_spec["table"])
        elif isinstance(right_spec, str):
            # older `right_table: T` form is read in build_defs; keep this for safety
            right_df = session.table(right_spec)
        else:
            raise ValueError("join.right must be {ref: <step_id>} or {table: <name>}")
        how = op.get("how", "inner").lower()
        on_cols = op.get("on_columns") or op.get("on")
        if on_cols:
            return df.join(right_df, on=on_cols, how=how)
        left_on, right_on = op.get("left_on"), op.get("right_on")
        if left_on and right_on:
            return df.join(
                right_df,
                on=[F.col(lo) == right_df[ro] for lo, ro in zip(left_on, right_on)],
                how=how,
            )
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "union":
        other = op["other"]
        if not isinstance(other, dict) or "ref" not in other:
            raise ValueError("snowpark_pipeline union.other must be {ref: <step_id>}")
        other_id = other["ref"]
        if other_id not in step_outputs:
            raise ValueError(f"union.other.ref={other_id!r} doesn't match any earlier step id")
        other_df = step_outputs[other_id]
        if op.get("distinct", False):
            return df.union(other_df)  # Snowpark's .union dedups; .union_all keeps dupes
        return df.union_all(other_df)
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        # Snowpark: register the current chain as 'self' + every prior step
        # as its step id, then run session.sql(...).
        df.create_or_replace_temp_view("self")
        for sid, other_df in step_outputs.items():
            other_df.create_or_replace_temp_view(sid)
        return session.sql(sql)
    raise ValueError(f"snowpark_pipeline: unsupported op {kind!r}. Valid: {sorted(_VALID_OPS)}")


def _read_source(session, source: Dict[str, Any]):
    kind = (source.get("kind") or "table").lower()
    if kind == "table":
        return session.table(source["table"])
    if kind == "sql":
        return session.sql(source["query"])
    raise ValueError(f"snowpark_pipeline source.kind={kind!r} not supported. Use 'table', 'sql', or 'ref'.")


def _write_sink(df, sink: Dict[str, Any]):
    kind = (sink.get("kind") or "table").lower()
    if kind == "table":
        mode = sink.get("mode", "overwrite")
        df.write.save_as_table(sink["table"], mode=mode)
        return None
    if kind == "none":
        return df.to_pandas()
    raise ValueError(f"snowpark_pipeline sink.kind={kind!r} not supported. Use 'table' or 'none'.")


class SnowparkPipelineComponent(Component, Model, Resolvable):
    """Multi-step Snowpark pipeline — all compute pushed to Snowflake.

    Two shapes:
      * Flat shape: top-level `source` + `operations` + `sink`.
      * Multi-step: `steps:` (each with `source`/`operations`) + `sinks:`.

    Supported ops: filter, select, drop, rename, with_columns, group_by,
    sort, limit, distinct, drop_nulls, join, union, sql.

    `op: sql` registers the current chain as `self` and every prior step
    output by its id (as temp views), then runs `session.sql(...)`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    connection: Dict[str, Any] = Field(
        description=(
            "Snowflake connection params. Any field may end with `_env_var` to source it "
            "from an environment variable (account_env_var, password_env_var, etc.)."
        ),
    )

    # Flat-shape shape ---------------------------------------------------------
    source: Optional[Dict[str, Any]] = Field(default=None, description="Flat shape: {kind: table|sql, ...}")
    operations: Optional[List[Dict[str, Any]]] = Field(default=None)
    sink: Optional[Dict[str, Any]] = Field(default=None, description="Flat shape: {kind: table|none, ...}")

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Named steps. Each: {id, source: {kind: table|sql|ref, ...}, operations: [...]}.",
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Sinks. Each: {from: <step_id>, kind: table|none, table: 'DB.SCH.T', mode}.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
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
        description="Max retries on materialization failure. Defines a RetryPolicy. Useful for transient Snowpark session failures or warehouse rate-limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step Snowpark pipeline — all compute pushed to Snowflake (one query plan per asset)."

    def _resolve_connection(self) -> Dict[str, Any]:
        import os
        params = dict(self.connection)
        for kv in list(params.keys()):
            if not kv.endswith("_env_var"):
                continue
            base = kv[: -len("_env_var")]
            if base in params:
                params.pop(kv)
                continue
            env_var = params.pop(kv)
            val = os.environ.get(env_var)
            if not val:
                raise EnvironmentError(f"Env var {env_var!r} (for {base}) is not set")
            params[base] = val
        return params

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        flat_present = bool(self.source or self.operations or self.sink)
        multi_present = bool(self.steps or self.sinks)
        if multi_present and flat_present:
            raise ValueError(
                "snowpark_pipeline: choose ONE shape — either top-level "
                "source/operations/sink OR steps/sinks, not both."
            )
        if multi_present:
            if not self.steps:
                raise ValueError("snowpark_pipeline: 'sinks' provided without 'steps'.")
            if not self.sinks:
                raise ValueError("snowpark_pipeline: 'steps' provided without 'sinks'.")
            return list(self.steps), list(self.sinks)
        if not (self.source and self.operations is not None and self.sink):
            raise ValueError(
                "snowpark_pipeline: provide either 'steps' + 'sinks' OR "
                "top-level 'source' + 'operations' + 'sink'."
            )
        flat_step = {
            "id": "_default",
            "source": dict(self.source),
            "operations": list(self.operations),
        }
        flat_sink = dict(self.sink, **{"from": "_default"})
        return [flat_step], [flat_sink]

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps, sinks = self._normalize()
        asset_name = self.asset_name
        resolve_connection = self._resolve_connection

        # Validate ops up front
        for s in steps:
            for i, op in enumerate(s.get("operations") or []):
                if not isinstance(op, dict) or "op" not in op:
                    raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with 'op' key")
                if op["op"].lower() not in _VALID_OPS:
                    raise ValueError(
                        f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. Valid: {sorted(_VALID_OPS)}"
                    )

        kinds = list(self.kinds or []) or ["snowflake", "snowpark"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        )

        # Retry policy (opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            retry_policy=_retry_policy,
            partitions_def=partitions_def,
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _snowpark_pipeline_asset(context: AssetExecutionContext) -> Any:
            from snowflake.snowpark import Session

            # Substitute <<partition_key>> in step + sink strings so users can
            # reference the current partition in op:filter predicates / op:sql /
            # sink table names.
            partition_key = context.partition_key if context.has_partition_key else None
            local_steps: List[Dict[str, Any]] = (
                _substitute_partition_key(steps, partition_key) if partition_key else steps  # type: ignore[assignment]
            )
            local_sinks: List[Dict[str, Any]] = (
                _substitute_partition_key(sinks, partition_key) if partition_key else sinks  # type: ignore[assignment]
            )
            if partition_key:
                context.log.info(f"snowpark_pipeline: partition_key={partition_key!r}")

            session = Session.builder.configs(resolve_connection()).create()
            try:
                step_outputs: Dict[str, Any] = {}

                for s_idx, step in enumerate(local_steps):
                    sid = step["id"]
                    src = step.get("source") or {}
                    src_kind = (src.get("kind") or "table").lower()
                    if src_kind == "ref":
                        ref = src.get("ref")
                        if ref not in step_outputs:
                            raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
                        df = step_outputs[ref]
                        context.log.info(f"step {sid}: ref → {ref}")
                    else:
                        df = _read_source(session, src)
                        context.log.info(f"step {sid}: read source {src_kind}")

                    for op in step.get("operations") or []:
                        df = _apply_op(session, df, op, step_outputs)
                    step_outputs[sid] = df
                    context.log.info(f"step {sid}: {len(step.get('operations') or [])} op(s) staged ({s_idx + 1}/{len(steps)})")

                # Write sinks — Snowpark compiles each to ONE Snowflake SQL stmt.
                sink_metadata: Dict[str, Any] = {}
                collected_pandas = None
                for sink in local_sinks:
                    from_id = sink.get("from") or ""
                    if from_id not in step_outputs:
                        raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
                    df = step_outputs[from_id]
                    kind = (sink.get("kind") or "table").lower()
                    result = _write_sink(df, sink)
                    if kind == "none" and result is not None:
                        collected_pandas = result
                        sink_metadata[f"snowpark/sink/{from_id}/row_count"] = MetadataValue.int(len(result))
                    else:
                        sink_metadata[f"snowpark/sink/{from_id}/kind"] = MetadataValue.text(kind)
                        if sink.get("table"):
                            sink_metadata[f"snowpark/sink/{from_id}/table"] = MetadataValue.text(str(sink["table"]))
                            try:
                                rc = session.table(sink["table"]).count()
                                sink_metadata[f"snowpark/sink/{from_id}/row_count"] = MetadataValue.int(int(rc))
                            except Exception:
                                pass

                metadata: Dict[str, Any] = {
                    "snowpark/step_count": MetadataValue.int(len(steps)),
                    "snowpark/sink_count": MetadataValue.int(len(sinks)),
                }
                metadata.update(sink_metadata)
                if collected_pandas is not None:
                    metadata["dagster/row_count"] = MetadataValue.int(len(collected_pandas))
                    context.add_output_metadata(metadata)
                    return collected_pandas
                return dg.MaterializeResult(metadata=metadata)
            finally:
                session.close()

        return Definitions(assets=[_snowpark_pipeline_asset])
