"""SnowparkPipelineComponent — single-asset Snowpark DataFrame chain.

Snowpark's DataFrame API is fully lazy: every `.filter() / .group_by() /
.join() / .agg()` builds a query plan, and the plan is compiled to a
single Snowflake SQL statement at the terminal action (write / collect /
count). All compute runs in Snowflake — no data through Python.

This is the more expressive sibling of `warehouse_summarize`. Where
`warehouse_summarize` exposes a simple GROUP BY DSL via CTAS, this
exposes the full Snowpark DataFrame API for complex chains
(filter → join → group_by → window → sort → write).

Composes by writing to a Snowflake table at the end of the chain;
downstream `warehouse_*` or `snowpark_pipeline` components consume by
table name.

Example:
    type: dagster_component_templates.SnowparkPipelineComponent
    attributes:
      asset_name: top_customers_by_region
      connection:
        account: my_account.us-east-1
        user: ETL_USER
        password_env_var: SNOWFLAKE_PASSWORD
        role: TRANSFORMER
        warehouse: COMPUTE_WH
        database: ANALYTICS
        schema: STAGING
      source:
        kind: table
        table: RAW.ORDERS
      operations:
        - op: filter
          predicate: "STATUS = 'paid' AND AMOUNT > 100"
        - op: group_by
          group_by: [REGION, CUSTOMER_ID]
          aggregations:
            TOTAL:       {col: AMOUNT,   agg: sum}
            ORDER_COUNT: {col: ORDER_ID, agg: count}
        - op: sort
          by: [REGION, TOTAL]
          descending: [false, true]
        - op: limit
          n: 100
      sink:
        kind: table
        table: ANALYTICS.TOP_CUSTOMERS_BY_REGION
        mode: overwrite
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


_VALID_OPS = {"filter", "select", "drop", "rename", "with_columns",
              "group_by", "sort", "limit", "distinct", "drop_nulls", "join"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count",
                    "count_distinct", "stddev", "variance"}


def _apply_op(df, op: Dict[str, Any], spark=None):
    """Apply one op to a Snowpark DataFrame. `spark` is the Snowpark session (kept for symmetry)."""
    from snowflake.snowpark import functions as F
    kind = op["op"]
    if kind == "filter":
        # Snowpark's filter takes an expression string OR a Column.
        # Using sql_expr() lets users write SQL predicates directly.
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
            # Snowpark function naming
            _fn_map = {"mean": "avg", "count_distinct": "count_distinct"}
            fn_name = _fn_map.get(f, f)
            agg_fn = getattr(F, fn_name)
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
        if subset:
            return df.na.drop(subset=subset)
        return df.na.drop()
    if kind == "join":
        # Requires a second Snowpark DataFrame loaded from another source.
        # For simplicity, this expects op["right_table"] as a Snowflake table name.
        right = spark.table(op["right_table"])
        how = op.get("how", "inner")
        if "on" in op:
            return df.join(right, on=op["on"], how=how)
        if "left_on" in op and "right_on" in op:
            return df.join(right,
                           on=[F.col(lo) == right[ro] for lo, ro in zip(op["left_on"], op["right_on"])],
                           how=how)
        raise ValueError("join: provide either 'on' or both 'left_on' and 'right_on'")
    raise ValueError(f"snowpark_pipeline: unsupported op {kind!r}. Valid: {sorted(_VALID_OPS)}")


def _read_source(spark, source: Dict[str, Any]):
    kind = source["kind"].lower()
    if kind == "table":
        return spark.table(source["table"])
    if kind == "sql":
        return spark.sql(source["query"])
    raise ValueError(f"Unknown source kind {kind!r} (snowpark: 'table' or 'sql')")


def _write_sink(df, sink: Dict[str, Any]):
    kind = sink["kind"].lower()
    if kind == "table":
        mode = sink.get("mode", "overwrite")
        # Snowpark .write.save_as_table(...) handles append/overwrite
        df.write.save_as_table(sink["table"], mode=mode)
        return None
    if kind == "none":
        return df.to_pandas()
    raise ValueError(f"Unknown sink kind {kind!r} (snowpark: 'table' or 'none')")


class SnowparkPipelineComponent(Component, Model, Resolvable):
    """Single-asset Snowpark DataFrame chain — compute runs entirely in Snowflake.

    Supported source kinds: table, sql.
    Supported sink kinds:   table, none (collect to pandas for the asset return).
    Supported ops: filter, select, drop, rename, with_columns, group_by,
                   sort, limit, distinct, drop_nulls, join.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    connection: Dict[str, Any] = Field(
        description=(
            "Snowflake connection params: {account, user, password OR password_env_var, "
            "role, warehouse, database, schema}. Authenticator/private_key/etc. also accepted."
        ),
    )
    source: Dict[str, Any] = Field(description="Source spec: {kind: table|sql, table|query: ...}")
    operations: List[Dict[str, Any]] = Field(
        description="Ordered list of Snowpark ops. Whole chain compiles to one SQL statement at the sink.",
    )
    sink: Dict[str, Any] = Field(description="Sink spec: {kind: table|none, table: ..., mode: overwrite|append}")
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step Snowpark DataFrame chain — full compute pushdown to Snowflake."

    def _resolve_connection(self) -> Dict[str, Any]:
        import os
        params = dict(self.connection)
        # Resolve password_env_var → password
        if "password_env_var" in params and "password" not in params:
            env_var = params.pop("password_env_var")
            pw = os.environ.get(env_var)
            if not pw:
                raise EnvironmentError(f"Env var {env_var!r} (password) is not set")
            params["password"] = pw
        # Similarly for private_key, authenticator, etc. if env-var-named.
        for kv in ("private_key_env_var", "private_key_passphrase_env_var", "authenticator_env_var"):
            if kv in params:
                base = kv.replace("_env_var", "")
                env_var = params.pop(kv)
                val = os.environ.get(env_var)
                if not val:
                    raise EnvironmentError(f"Env var {env_var!r} ({base}) is not set")
                params[base] = val
        return params

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        source = dict(self.source)
        operations = list(self.operations)
        sink = dict(self.sink)
        asset_name = self.asset_name
        resolve_connection = self._resolve_connection

        kinds = list(self.kinds or []) or ["snowflake", "snowpark"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        @asset(
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

            session = Session.builder.configs(resolve_connection()).create()
            try:
                df = _read_source(session, source)
                context.log.info(f"snowpark_pipeline: read source {source['kind']}")

                for i, op in enumerate(operations):
                    df = _apply_op(df, op, spark=session)
                    context.log.info(f"  step {i + 1}/{len(operations)}: {op['op']}")

                # Snowpark's lazy plan compiles to ONE Snowflake SQL statement here.
                result = _write_sink(df, sink)
                context.log.info(f"snowpark_pipeline: wrote sink {sink['kind']}")

                metadata = {
                    "snowpark/source_kind": MetadataValue.text(source["kind"]),
                    "snowpark/sink_kind": MetadataValue.text(sink["kind"]),
                    "snowpark/operation_count": MetadataValue.int(len(operations)),
                }
                if result is None:
                    # Materialized to a Snowflake table — return MaterializeResult.
                    try:
                        row_count = session.table(sink["table"]).count()
                        metadata["dagster/row_count"] = MetadataValue.int(int(row_count))
                    except Exception:
                        pass
                    return dg.MaterializeResult(metadata=metadata)
                # sink.kind = 'none' → pandas DataFrame returned to Dagster IO.
                metadata["dagster/row_count"] = MetadataValue.int(len(result))
                context.add_output_metadata(metadata)
                return result
            finally:
                session.close()

        return Definitions(assets=[_snowpark_pipeline_asset])
