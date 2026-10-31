"""PySparkFeedOrchestratorComponent — folder-scan sibling of pyspark_pipeline.

Points at a folder of feed config files (`*.json` or `*.yaml/.yml`) — each
one describes ONE pipeline in the same shape `pyspark_pipeline` accepts,
plus the control-flow step types (`condition` / `checkpoint` / `for_each`).

For each feed file:
  - Loads the config.
  - Extracts asset keys the feed materializes (from `sinks[].from` + the
    step it references) → becomes the emitted asset keys.
  - Extracts asset keys the feed depends on (from `steps[].source.kind:
    upstream` + `sources[].kind: table` when tables get produced by ANOTHER
    feed in the same folder). Automatically resolves cross-feed edges when
    two feeds share a table name.
  - Instantiates a PySparkPipelineComponent with the parsed shape.
  - Emits its Definitions (multi_asset + optional ScheduleDefinition).

Feed shape (superset of pyspark_pipeline; JSON or YAML):

    {
      "name": "customer_daily_features",
      "description": "...",
      "schedule": "0 6 * * *",           # optional cron
      "owners": ["team@corp.com"],
      "group_name": "spark_feeds",
      "variables": {"target_date": "2026-08-30"},
      "steps": [
        {"id": "customers", "source": {"kind": "parquet", "path": "..."},
         "operations": [...]},
        {"id": "guard", "type": "condition", "when": "customers.row_count > 0"},
        ...
      ],
      "sinks": [
        {"from": "features", "kind": "parquet", "path": "features/", "mode": "overwrite"}
      ]
    }

Cross-feed lineage: when a feed's step reads `{"kind": "table", "table":
"warehouse.customers"}`, the orchestrator checks if any other feed in the
folder writes a sink to `warehouse.customers`. If yes → the assets are
wired; if no → the source appears as an external asset (dashed border).

Same feed configs unchanged, unified observability, actual lineage. Ports
the customer prototype pattern to a first-class DCC component.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import dagster as dg
from dagster import (
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from pydantic import Field

# Reuse pyspark_pipeline component's execute path — but since the no-shared-
# code rule requires each component be self-contained, we spawn a
# PySparkPipelineComponent PER feed and let IT own the actual execution.
# The orchestrator's job is discovery + cross-file lineage resolution.


def _load_feed(path: Path) -> Dict[str, Any]:
    """Load a feed file. Supports .json / .yaml / .yml."""
    suffix = path.suffix.lower()
    text = path.read_text()
    if suffix == ".json":
        return json.loads(text)
    if suffix in (".yaml", ".yml"):
        import yaml
        return yaml.safe_load(text)
    raise ValueError(f"pyspark_feed_orchestrator: unsupported feed extension {suffix!r} on {path}")


def _write_targets(feed: Dict[str, Any]) -> List[str]:
    """All `sinks[].table` / `sinks[].path` (converted to asset-key strings)."""
    out: List[str] = []
    for sink in feed.get("sinks") or []:
        # Prefer explicit `asset_key` field, then `table`, then last resort:
        # a path-derived key (base file/dir name without extension).
        key = sink.get("asset_key") or sink.get("table")
        if not key and sink.get("path"):
            key = Path(sink["path"]).with_suffix("").name
        if key and key not in out:
            out.append(key)
    return out


def _read_sources(feed: Dict[str, Any]) -> List[str]:
    """All potential upstream asset keys read across steps + nested for_each."""
    out: List[str] = []

    def _walk(steps: List[Dict[str, Any]]) -> None:
        for s in steps:
            stype = (s.get("type") or "").lower()
            if stype == "for_each":
                _walk(s.get("steps") or [])
                continue
            if stype in {"condition", "checkpoint"}:
                continue
            src = s.get("source") or {}
            kind = (src.get("kind") or "").lower()
            if kind == "upstream":
                k = src.get("upstream_asset_key")
                if k and k not in out:
                    out.append(k)
            elif kind == "table":
                k = src.get("table")
                if k and k not in out:
                    out.append(k)
            elif kind == "ref":
                # Intra-feed reference — not a cross-feed dep.
                pass

    _walk(feed.get("steps") or [])
    return out



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


# Self-contained: the PySpark pipeline engine is inlined from assets/transforms/pyspark_pipeline (a
# component.py must not import a sibling component -- the CLI copies only this file). Private name so the
# registry doesn't see a duplicate public component. Keep in sync with that component.
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
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
              "group_by", "sort", "limit", "distinct", "drop_nulls",
              "join", "union", "sql"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count", "countDistinct",
                    "first", "last", "stddev", "variance"}

# Control-flow step types — see the module docstring for shapes.
_CONTROL_TYPES = {"condition", "checkpoint", "for_each"}


def _safe_eval(expr: str, context: Dict[str, Any]) -> Any:
    """Evaluate a control-flow expression against a scoped dict.

    Used by `type: condition when: <expr>` and `type: for_each over: <expr>`.
    Restricted eval: no __ names, no attribute walks into private members,
    no imports. Function surface is limited to len / min / max / int / float /
    bool / str / round / sum / any / all / abs — matches
    parametric_data_generator's formula sandbox.
    """
    import ast
    _SAFE_FUNCS = {
        "len": len, "min": min, "max": max, "int": int, "float": float,
        "bool": bool, "str": str, "round": round, "sum": sum,
        "any": any, "all": all, "abs": abs,
    }

    tree = ast.parse(expr, mode="eval")

    class _Walk(ast.NodeVisitor):
        def visit_Name(self, node):
            if node.id.startswith("__"):
                raise ValueError(f"safe_eval: name {node.id!r} not allowed")
            self.generic_visit(node)

        def visit_Attribute(self, node):
            if node.attr.startswith("_"):
                raise ValueError(f"safe_eval: attribute {node.attr!r} not allowed")
            self.generic_visit(node)

        def visit_Call(self, node):
            if isinstance(node.func, ast.Name) and node.func.id not in _SAFE_FUNCS:
                raise ValueError(f"safe_eval: function {node.func.id!r} not allowed")
            self.generic_visit(node)

    _Walk().visit(tree)
    return eval(compile(tree, filename="<safe_eval>", mode="eval"),
                {"__builtins__": {}}, {**_SAFE_FUNCS, **context})


def _step_output_meta(spark_df) -> Dict[str, Any]:
    """Return a small dict of side-info about a step's DataFrame for use in
    `condition when:` expressions — e.g. `{{ orders.row_count }} > 0`. Kept
    lightweight — `row_count` triggers a Spark action, so the value is
    cached on the wrapper.
    """
    class _Info:
        def __init__(self, df):
            self._df = df
            self._row_count: Optional[int] = None
            self._columns: Optional[List[str]] = None

        @property
        def row_count(self):
            if self._row_count is None:
                self._row_count = self._df.count()
            return self._row_count

        @property
        def columns(self):
            if self._columns is None:
                self._columns = list(self._df.columns)
            return self._columns

    return _Info(spark_df)


def _load_checkpoint(path):
    """Load prior-run checkpoint JSON; return {} if the file doesn't exist."""
    import json
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text()) or {}
    except Exception:  # noqa: BLE001
        return {}


def _save_checkpoint(path, state: Dict[str, Any]) -> None:
    """Overwrite the checkpoint JSON with the given state."""
    import json
    from pathlib import Path
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2, default=str))


def _apply_op(spark, df, op: Dict[str, Any], step_outputs: Dict[str, Any]):
    """Apply one op to a Spark DataFrame; returns the new DataFrame.

    step_outputs is the map of prior-step ids → their DataFrame, used by
    join/union/sql ops that reference other steps.
    """
    from pyspark.sql import functions as F
    kind = op["op"].lower()
    if kind == "filter":
        return df.filter(op["predicate"])
    if kind == "select":
        return df.select(*op["columns"])
    if kind == "drop":
        return df.drop(*op["columns"])
    if kind == "rename":
        out = df
        for old, new in op["mapping"].items():
            out = out.withColumnRenamed(old, new)
        return out
    if kind == "with_columns":
        out = df
        for name, expr_str in op["expressions"].items():
            out = out.withColumn(name, F.expr(expr_str))
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
            if func not in _SUPPORTED_AGGS:
                raise ValueError(
                    f"pyspark_pipeline: agg func {func!r} not supported. "
                    f"Use one of {sorted(_SUPPORTED_AGGS)}"
                )
            agg_fn = getattr(F, func) if hasattr(F, func) else getattr(F, "count_distinct")
            agg_exprs.append(agg_fn(F.col(src_col)).alias(out_col))
        return df.groupBy(*group_by).agg(*agg_exprs)
    if kind == "sort":
        by = op["by"] if isinstance(op["by"], list) else [op["by"]]
        descending = op.get("descending", False)
        descending = descending if isinstance(descending, list) else [descending] * len(by)
        cols = [(F.col(c).desc() if d else F.col(c).asc()) for c, d in zip(by, descending)]
        return df.orderBy(*cols)
    if kind == "limit":
        return df.limit(op["n"])
    if kind == "distinct":
        return df.distinct()
    if kind == "drop_nulls":
        subset = op.get("subset")
        return df.dropna(subset=subset)
    if kind == "join":
        right_spec = op["right"]
        if not isinstance(right_spec, dict) or "ref" not in right_spec:
            raise ValueError("pyspark_pipeline join.right must be {ref: <step_id>}")
        right_id = right_spec["ref"]
        if right_id not in step_outputs:
            raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
        right_df = step_outputs[right_id]
        how = op.get("how", "inner").lower()
        on_cols = op.get("on_columns") or op.get("on")
        if on_cols:
            return df.join(right_df, on=on_cols, how=how)
        left_on, right_on = op.get("left_on"), op.get("right_on")
        if left_on and right_on:
            cond = None
            for lo, ro in zip(left_on, right_on):
                term = df[lo] == right_df[ro]
                cond = term if cond is None else (cond & term)
            return df.join(right_df, on=cond, how=how)
        if how == "cross":
            return df.crossJoin(right_df)
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "union":
        other = op["other"]
        if not isinstance(other, dict) or "ref" not in other:
            raise ValueError("pyspark_pipeline union.other must be {ref: <step_id>}")
        other_id = other["ref"]
        if other_id not in step_outputs:
            raise ValueError(f"union.other.ref={other_id!r} doesn't match any earlier step id")
        other_df = step_outputs[other_id]
        if op.get("distinct", False):
            return df.union(other_df).distinct()
        return df.union(other_df)
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        # Register the current chain as 'self' + every prior step output by id.
        df.createOrReplaceTempView("self")
        for sid, other_df in step_outputs.items():
            other_df.createOrReplaceTempView(sid)
        return spark.sql(sql)
    raise ValueError(f"pyspark_pipeline: unsupported op {kind!r}. Valid: {sorted(_VALID_OPS)}")


def _read_source(spark, source: Dict[str, Any]):
    kind = source["kind"].lower()
    if kind == "parquet":
        return spark.read.parquet(source["path"])
    if kind == "csv":
        return spark.read.csv(source["path"], header=source.get("header", True),
                              inferSchema=source.get("inferSchema", True))
    if kind == "json":
        return spark.read.json(source["path"])
    if kind == "orc":
        return spark.read.orc(source["path"])
    if kind == "delta":
        return spark.read.format("delta").load(source["path"])
    if kind == "table":
        return spark.read.table(source["table"])
    if kind == "jdbc":
        opts = source.get("options", {})
        return (spark.read.format("jdbc")
                .option("url", source["url"])
                .option("dbtable", source.get("dbtable") or f"({source['query']}) AS _t")
                .options(**opts).load())
    raise ValueError(f"Unknown source kind {kind!r}")


def _write_sink(df, sink: Dict[str, Any]):
    kind = sink["kind"].lower()
    mode = sink.get("mode", "overwrite")
    if kind == "parquet":
        df.write.mode(mode).parquet(sink["path"]); return None
    if kind == "csv":
        df.write.mode(mode).csv(sink["path"], header=sink.get("header", True)); return None
    if kind == "json":
        df.write.mode(mode).json(sink["path"]); return None
    if kind == "delta":
        df.write.format("delta").mode(mode).save(sink["path"]); return None
    if kind == "table":
        df.write.mode(mode).saveAsTable(sink["table"]); return None
    if kind == "jdbc":
        opts = sink.get("options", {})
        (df.write.format("jdbc")
            .option("url", sink["url"]).option("dbtable", sink["dbtable"])
            .options(**opts).mode(mode).save())
        return None
    if kind == "none":
        return df.toPandas()
    raise ValueError(f"Unknown sink kind {kind!r}")



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization






class _PySparkPipelineImpl(Component, Model, Resolvable):
    """Multi-step PySpark DataFrame pipeline in a single Dagster asset.

    Two shapes:
      * Flat shape: top-level `source` + `operations` + `sink`.
      * Multi-step: `steps:` (each with `source`/`operations`) + `sinks:` (list).

    Supported ops: filter, select, drop, rename, with_columns, group_by,
    sort, limit, distinct, drop_nulls, join, union, sql.

    `op: sql` registers the current chain as `self` and every prior step
    output by its id, then runs `spark.sql(...)`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    spark_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="SparkConf options. Stringified before passing to .config(...).",
    )
    spark_app_name: str = Field(default="dagster-pyspark-pipeline")

    # Flat-shape shape ---------------------------------------------------------
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Top-level single-source shape: {kind: parquet|csv|json|orc|delta|table|jdbc|upstream, ...}",
    )
    operations: Optional[List[Dict[str, Any]]] = Field(default=None)
    sink: Optional[Dict[str, Any]] = Field(default=None, description="Top-level single-sink shape.")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Only when (flat shape) source.kind='upstream'.",
    )

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Named steps. Each: {id, source: {kind: parquet|table|jdbc|upstream|ref|..., ...}, operations: [...]}",
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Sinks. Each: {from: <step_id>, kind: parquet|csv|json|delta|table|jdbc, path/table/url, mode}.",
    )

    # Control-flow surface (Bucket-B-adjacent additions) ------------------
    #
    # These enable pipelines that mirror the customer's feed-shaped pattern:
    # `type: condition | checkpoint | for_each` steps in the `steps:` list.
    # See the module docstring for the full step-type reference.
    variables: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Feed-level constants available at run time via {{ vars.<key> }} "
            "in condition when: / for_each over: / SQL where relevant. Merged "
            "with prior-run `checkpoint.*` values (see checkpoint_dir)."
        ),
    )
    checkpoint_dir: Optional[str] = Field(
        default=None,
        description=(
            "Directory (relative to project root, or absolute) where "
            "`type: checkpoint` steps persist named values across runs. "
            "Default `checkpoints/` under the project root. On each run, "
            "`checkpoints/<asset_name>.json` is loaded and its keys become "
            "available as {{ checkpoint.<key> }}."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type of this feed's asset ('daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic'), from the feed's `partitions:` block.",
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
    partition_start: Optional[str] = Field(default=None, description="Start date for time partitions (ISO).")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Keys for static partitions.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for dynamic partitions.")
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step PySpark DataFrame pipeline in a single Dagster asset (Catalyst-optimized across all steps)."

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """Return (steps, sinks, upstream_keys)."""
        flat_present = bool(self.source or self.operations or self.sink or self.upstream_asset_key)
        multi_present = bool(self.steps or self.sinks)
        if multi_present and flat_present:
            raise ValueError(
                "pyspark_pipeline: choose ONE shape — either top-level "
                "source/operations/sink OR steps/sinks, not both."
            )
        if multi_present:
            if not self.steps:
                raise ValueError("pyspark_pipeline: 'sinks' provided without 'steps'.")
            steps = list(self.steps)
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []

            def _walk_upstreams(step_list: List[Dict[str, Any]]) -> None:
                for s in step_list:
                    stype = (s.get("type") or "").lower()
                    if stype in _CONTROL_TYPES:
                        # for_each nests further steps — recurse
                        if stype == "for_each":
                            _walk_upstreams(s.get("steps") or [])
                        continue
                    src = s.get("source") or {}
                    if (src.get("kind") or "").lower() == "upstream":
                        k = src.get("upstream_asset_key")
                        if not k:
                            raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                        if k not in upstream_keys:
                            upstream_keys.append(k)

            _walk_upstreams(steps)
            return steps, sinks, upstream_keys
        # Flat shape
        if not (self.source and self.operations is not None and self.sink):
            raise ValueError(
                "pyspark_pipeline: provide either 'steps' + 'sinks' OR "
                "top-level 'source' + 'operations' + 'sink'."
            )
        flat_step = {
            "id": "_default",
            "source": dict(self.source),
            "operations": list(self.operations),
        }
        # Annotate the upstream_asset_key on the source if flat upstream was given.
        if (self.source.get("kind") or "").lower() == "upstream":
            if not self.upstream_asset_key:
                raise ValueError("source.kind='upstream' requires upstream_asset_key.")
            flat_step["source"]["upstream_asset_key"] = self.upstream_asset_key
        flat_sink = dict(self.sink, **{"from": "_default"})
        upstream_keys = [self.upstream_asset_key] if self.upstream_asset_key else []
        return [flat_step], [flat_sink], upstream_keys

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps, sinks, upstream_keys = self._normalize()
        spark_config = dict(self.spark_config or {})
        spark_app_name = self.spark_app_name
        asset_name = self.asset_name
        partitions_def = _build_partitions_def(self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, None)

        # Validate ops up front (walks into for_each nested steps)
        def _validate_ops(step_list: List[Dict[str, Any]]) -> None:
            for s in step_list:
                stype = (s.get("type") or "").lower()
                if stype in _CONTROL_TYPES:
                    if stype == "for_each":
                        _validate_ops(s.get("steps") or [])
                    continue
                for i, op in enumerate(s.get("operations") or []):
                    if not isinstance(op, dict) or "op" not in op:
                        raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with 'op' key")
                    if op["op"].lower() not in _VALID_OPS:
                        raise ValueError(
                            f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. Valid: {sorted(_VALID_OPS)}"
                        )

        _validate_ops(steps)

        kinds = list(self.kinds or []) or ["pyspark", "spark"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        ins = {
            f"upstream_{j}": AssetIn(key=AssetKey.from_user_string(k))
            for j, k in enumerate(upstream_keys)
        }
        upstream_arg_names = {k: f"upstream_{j}" for j, k in enumerate(upstream_keys)}

        description = self.description or self.get_description()
        owners = self.owners or []
        group_name = self.group_name
        deps = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        kinds_set = set(kinds)

        # Snapshot control-flow config into the closure so _execute stays pure.
        variables = dict(self.variables or {})
        checkpoint_dir = self.checkpoint_dir

        @asset(
            key=dg.AssetKey.from_user_string(asset_name), description=description, owners=owners,
            tags=all_tags, group_name=group_name, deps=deps, kinds=kinds_set,
            ins=ins, partitions_def=partitions_def,
        )
        def _pyspark_pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            _tokens = _partition_vars(context)  # {partition_*} in this feed's sources, SQL, sink paths and variables
            return _execute(context, spark_config, spark_app_name, _substitute_partition(steps, _tokens), _substitute_partition(sinks, _tokens),
                             upstream_arg_names, upstreams,
                             variables=_substitute_partition(variables, _tokens), checkpoint_dir=checkpoint_dir,
                             asset_name=asset_name)

        return Definitions(assets=[_pyspark_pipeline_asset])


def _execute(context, spark_config, spark_app_name, steps, sinks,
              upstream_arg_names, upstreams,
              variables: Optional[Dict[str, Any]] = None,
              checkpoint_dir: Optional[str] = None,
              asset_name: str = ""):
    from pyspark.sql import SparkSession
    from pathlib import Path

    builder = SparkSession.builder.appName(spark_app_name)
    for k, v in (spark_config or {}).items():
        builder = builder.config(k, str(v))
    spark = builder.getOrCreate()

    step_outputs: Dict[str, Any] = {}
    # `meta` is a parallel map keyed by step id → _Info wrapper for
    # condition/for_each expressions that reference `<step>.row_count` etc.
    step_meta: Dict[str, Any] = {}

    # ── Load prior-run checkpoint (if configured) ─────────────────────
    ckpt_file: Optional[Path] = None
    prior_checkpoint: Dict[str, Any] = {}
    if checkpoint_dir:
        ckpt_file = Path(checkpoint_dir) / f"{asset_name}.json"
        prior_checkpoint = _load_checkpoint(ckpt_file)
        if prior_checkpoint:
            context.log.info(
                f"checkpoint: loaded {len(prior_checkpoint)} keys from {ckpt_file}"
            )

    vars_ctx = dict(variables or {})
    # In-run checkpoint state — mutated by `type: checkpoint` steps.
    live_checkpoint: Dict[str, Any] = dict(prior_checkpoint)

    def _eval_scope(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build the eval scope for condition when: / for_each over: exprs.

        Keys: `vars.<x>`, `checkpoint.<x>`, `<step_id>.row_count`, etc.
        Since ast.parse doesn't do dotted-access sub-eval, we mimic with
        a tiny attribute-holder dict-wrapper.
        """
        class _Dot(dict):
            __getattr__ = dict.get  # type: ignore[assignment]
        scope: Dict[str, Any] = {}
        scope["vars"] = _Dot(vars_ctx)
        scope["checkpoint"] = _Dot(live_checkpoint)
        for sid, meta in step_meta.items():
            scope[sid] = meta
        if extra:
            scope.update(extra)
        return scope

    # ── Step execution — recursive so `for_each` can nest ─────────────
    def _run_regular_step(step: Dict[str, Any]) -> None:
        sid = step["id"]
        src = step.get("source") or {}
        src_kind = (src.get("kind") or "").lower()

        if src_kind == "upstream":
            uk = src["upstream_asset_key"]
            obj = upstreams[upstream_arg_names[uk]]
            try:
                import polars as pl
                if isinstance(obj, pl.DataFrame):
                    obj = obj.to_pandas()
            except Exception:
                pass
            df = spark.createDataFrame(obj)
            context.log.info(f"step {sid}: read upstream pandas/polars DataFrame ({len(obj)} rows)")
        elif src_kind == "ref":
            ref = src.get("ref")
            if ref not in step_outputs:
                raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
            df = step_outputs[ref]
            context.log.info(f"step {sid}: ref → {ref}")
        else:
            df = _read_source(spark, src)
            context.log.info(f"step {sid}: read source {src.get('kind')}")

        for op in step.get("operations") or []:
            df = _apply_op(spark, df, op, step_outputs)
        step_outputs[sid] = df
        step_meta[sid] = _step_output_meta(df)
        context.log.info(f"step {sid}: {len(step.get('operations') or [])} op(s) staged")

    def _run_condition_step(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        sid = step.get("id") or "<condition>"
        when_expr = step.get("when")
        if not when_expr:
            raise ValueError(f"step {sid!r} type=condition requires 'when'")
        on_false = (step.get("on_false") or "skip_rest").lower()
        try:
            value = _safe_eval(when_expr, _eval_scope())
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"step {sid!r}: condition eval failed on {when_expr!r}: {e}") from e
        if bool(value):
            context.log.info(f"condition {sid}: {when_expr!r} → True (continuing)")
            return
        context.log.info(f"condition {sid}: {when_expr!r} → False (on_false={on_false})")
        if on_false == "fail":
            raise dg.Failure(f"condition {sid}: {when_expr!r} evaluated to False")
        if on_false == "skip_rest":
            skip_state["skip"] = True
            return
        raise ValueError(f"condition {sid!r}: on_false={on_false!r} not supported (fail | skip_rest)")

    def _run_checkpoint_step(step: Dict[str, Any]) -> None:
        sid = step.get("id") or "<checkpoint>"
        keys = step.get("keys") or []
        if not isinstance(keys, list):
            raise ValueError(f"step {sid!r} type=checkpoint requires 'keys' (list of var / step-attr names)")
        # Persist referenced values. Each key can be a plain name (from vars_ctx)
        # or a dotted expression like `orders.row_count`.
        scope = _eval_scope()
        new_state: Dict[str, Any] = dict(live_checkpoint)
        for k in keys:
            try:
                new_state[k] = _safe_eval(k, scope)
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"checkpoint {sid}: could not resolve {k!r}: {e}")
        live_checkpoint.clear()
        live_checkpoint.update(new_state)
        if ckpt_file is not None:
            _save_checkpoint(ckpt_file, new_state)
            context.log.info(f"checkpoint {sid}: saved {len(new_state)} keys to {ckpt_file}")
        else:
            context.log.warning(
                f"checkpoint {sid}: no checkpoint_dir configured — held in-run only, not persisted"
            )

    def _run_for_each_step(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        sid = step.get("id") or "<for_each>"
        over_expr = step.get("over")
        as_name = step.get("as") or "item"
        inner = step.get("steps") or []
        if not over_expr:
            raise ValueError(f"step {sid!r} type=for_each requires 'over'")
        try:
            iterable = _safe_eval(over_expr, _eval_scope())
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"step {sid!r}: for_each over eval failed on {over_expr!r}: {e}") from e
        if not hasattr(iterable, "__iter__"):
            raise ValueError(f"step {sid!r}: for_each over={over_expr!r} did not evaluate to an iterable (got {type(iterable).__name__})")
        elements = list(iterable)
        context.log.info(f"for_each {sid}: {len(elements)} iteration(s) over {over_expr!r}")
        for i, element in enumerate(elements):
            # `loop.<as>` binding — but our eval scope doesn't do dotted-index
            # into user-defined objects, so we emulate with a per-iteration
            # extra namespace shipped as `loop` (dict-attr wrapper).
            class _Dot(dict):
                __getattr__ = dict.get  # type: ignore[assignment]
            loop_ns = _Dot({as_name: element, "index": i})
            # Re-evaluate expressions inside inner steps by string substitution
            # for `{{ loop.<as> }}` tokens — pragmatic + preserves the current
            # DataFrame-source model without introducing a full templater.
            def _sub(v):
                if isinstance(v, str) and "{{ loop." in v:
                    out = v.replace(f"{{{{ loop.{as_name} }}}}", str(element))
                    out = out.replace("{{ loop.index }}", str(i))
                    return out
                return v

            def _sub_tree(node):
                if isinstance(node, dict):
                    return {k: _sub_tree(v) for k, v in node.items()}
                if isinstance(node, list):
                    return [_sub_tree(x) for x in node]
                return _sub(node)

            # Push a per-iteration `loop` into eval scope for condition/etc.
            def _push():
                step_meta["loop"] = loop_ns
            def _pop():
                step_meta.pop("loop", None)

            _push()
            try:
                for inner_step in inner:
                    if skip_state.get("skip"):
                        return
                    substituted = _sub_tree(inner_step)
                    _dispatch(substituted, skip_state)
            finally:
                _pop()

    def _dispatch(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        stype = (step.get("type") or "").lower()
        if stype == "condition":
            _run_condition_step(step, skip_state)
        elif stype == "checkpoint":
            _run_checkpoint_step(step)
        elif stype == "for_each":
            _run_for_each_step(step, skip_state)
        else:
            _run_regular_step(step)

    skip_state: Dict[str, bool] = {"skip": False}
    for s_idx, step in enumerate(steps):
        if skip_state["skip"]:
            context.log.info(f"skip_rest: bypassing remaining steps (halted at index {s_idx})")
            break
        _dispatch(step, skip_state)

    # Write all sinks. Spark fires the Catalyst plan at each terminal action.
    sink_metadata: Dict[str, Any] = {}
    collected_pandas = None
    for sink in sinks:
        from_id = sink.get("from") or ""
        if from_id not in step_outputs:
            raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
        df = step_outputs[from_id]
        kind = (sink.get("kind") or "").lower()
        result = _write_sink(df, sink)
        if kind == "none" and result is not None:
            # Caller wants the data back in Python.
            collected_pandas = result
            sink_metadata[f"pyspark/sink/{from_id}/row_count"] = MetadataValue.int(len(result))
        else:
            sink_metadata[f"pyspark/sink/{from_id}/kind"] = MetadataValue.text(kind)
            for k in ("path", "table", "dbtable"):
                if sink.get(k):
                    sink_metadata[f"pyspark/sink/{from_id}/{k}"] = MetadataValue.text(str(sink[k]))

    metadata: Dict[str, Any] = {
        "pyspark/step_count": MetadataValue.int(len(steps)),
        "pyspark/sink_count": MetadataValue.int(len(sinks)),
    }
    metadata.update(sink_metadata)
    if collected_pandas is not None:
        metadata["dagster/row_count"] = MetadataValue.int(len(collected_pandas))
        context.add_output_metadata(metadata)
        return collected_pandas
    return dg.MaterializeResult(metadata=metadata)


class PySparkFeedOrchestratorComponent(dg.Component, Model, Resolvable):
    """Scan a folder of feed files and emit one Dagster multi_asset per feed.

    Example:

    ```yaml
    type: dagster_community_components.PySparkFeedOrchestratorComponent
    attributes:
      feeds_dir: feeds
      spark_config:
        spark.master: "local[*]"
      checkpoint_dir: checkpoints
      group_name: spark_feeds
    ```
    """

    feeds_dir: str = Field(
        description=(
            "Path (relative to component YAML dir, or absolute) to the folder "
            "containing feed config files. Every `.json` / `.yaml` / `.yml` "
            "file at the top level becomes one Dagster multi_asset via "
            "PySparkPipelineComponent."
        ),
    )
    file_glob: str = Field(
        default="*.json,*.yaml,*.yml",
        description="Comma-separated glob patterns for feed files (default matches JSON + YAML).",
    )
    spark_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Base SparkConf applied to every feed. Feed-level `spark_config` overlays.",
    )
    spark_app_name_prefix: str = Field(
        default="dagster-pyspark-feed",
        description="Spark app name is `<prefix>-<feed_name>`.",
    )
    checkpoint_dir: Optional[str] = Field(
        default=None,
        description=(
            "Checkpoint directory passed to every feed's PySparkPipelineComponent. "
            "Each feed writes `<checkpoint_dir>/<feed_name>.json`."
        ),
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Fallback Dagster group name (feed-level `group_name` overrides).",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Fallback asset kinds. Default: ['pyspark', 'spark'].",
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Fallback catalog tags (feed-level tags overlay).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds',), csv=(), tags=('tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Resolve feeds_dir against the component's YAML location.
        base = _resolve_base(context)
        feeds_dir = Path(self.feeds_dir)
        if not feeds_dir.is_absolute():
            feeds_dir = (base / feeds_dir).resolve()

        if not feeds_dir.exists():
            # Bail cleanly — likely dg check before the feeds folder exists yet.
            context.log.warning(  # type: ignore[attr-defined]
                f"PySparkFeedOrchestratorComponent: feeds_dir={feeds_dir!s} does not exist yet; "
                "no assets emitted."
            ) if hasattr(context, "log") else None
            return Definitions()

        # Enumerate feeds.
        patterns = [p.strip() for p in self.file_glob.split(",") if p.strip()]
        feed_files: List[Path] = []
        seen: set = set()
        for pat in patterns:
            for f in sorted(feeds_dir.glob(pat)):
                if f.is_file() and str(f) not in seen:
                    seen.add(str(f))
                    feed_files.append(f)

        if not feed_files:
            return Definitions()

        # Load + inventory produced/consumed keys across every feed.
        parsed: List[Tuple[Path, Dict[str, Any]]] = []
        produced_keys: set = set()
        for f in feed_files:
            try:
                feed = _load_feed(f)
            except Exception as e:  # noqa: BLE001
                raise ValueError(f"pyspark_feed_orchestrator: failed to parse {f.name}: {e}") from e
            if not isinstance(feed, dict):
                raise ValueError(f"pyspark_feed_orchestrator: {f.name} did not parse to a dict")
            feed.setdefault("name", f.stem)
            parsed.append((f, feed))
            produced_keys.update(_write_targets(feed))

        # Build one PySparkPipelineComponent per feed. External sources become
        # explicit AssetSpecs so the graph shows them as first-class nodes with
        # dashed borders (not implicit "auto-external" stubs).
        all_defs_children: List[Definitions] = []
        external_specs: List[dg.AssetSpec] = []
        external_seen: set = set()

        for path, feed in parsed:
            feed_name = feed.get("name") or path.stem
            read_srcs = _read_sources(feed)
            # Externals — sources referenced but NOT produced by any feed in this folder.
            for src in read_srcs:
                if src not in produced_keys and src not in external_seen:
                    external_seen.add(src)
                    external_specs.append(
                        dg.AssetSpec(
                            key=dg.AssetKey.from_user_string(src),
                            group_name="external",
                            kinds={"external"},
                            description=f"External input to spark feed {feed_name!r}.",
                        )
                    )

            # Merge component-level defaults into feed-level values.
            merged_spark_cfg = dict(self.spark_config or {})
            merged_spark_cfg.update(feed.get("spark_config") or {})

            # Route ALL feed sinks — first sink's target defines the asset name.
            targets = _write_targets(feed)
            if not targets:
                # No sinks — treat the feed as a no-op observation asset keyed on feed name.
                asset_name = feed_name
            else:
                asset_name = targets[0]

            feed_partitions = _feed_partitions(feed_name, feed)
            child = _PySparkPipelineImpl(
                asset_name=asset_name,
                **feed_partitions,
                spark_config=merged_spark_cfg,
                spark_app_name=f"{self.spark_app_name_prefix}-{feed_name}",
                steps=feed.get("steps") or [],
                sinks=feed.get("sinks") or [],
                variables=feed.get("variables"),
                checkpoint_dir=feed.get("checkpoint_dir") or self.checkpoint_dir,
                group_name=feed.get("group_name") or self.group_name,
                description=feed.get("description"),
                owners=feed.get("owners"),
                asset_tags={**(self.tags or {}), **(feed.get("tags") or {})} or None,
                kinds=feed.get("kinds") or self.kinds,
                deps=[k for k in read_srcs if k not in {t for t in targets}] or None,
            )
            child_defs = child.build_defs(context)
            all_defs_children.append(child_defs)

            # Attach a schedule if the feed declares one.
            cron = feed.get("schedule")
            if cron:
                sched_name = f"{feed_name}_schedule"
                target = dg.AssetSelection.assets(dg.AssetKey.from_user_string(asset_name))
                if feed_partitions["partition_type"]:
                    sched = _partitioned_schedule(sched_name, cron, target, feed.get("timezone"), feed_name, feed_partitions)
                else:
                    sched = dg.ScheduleDefinition(
                        name=sched_name,
                        cron_schedule=cron,
                        target=target,
                        execution_timezone=feed.get("timezone"),
                    )
                all_defs_children.append(Definitions(schedules=[sched]))

        # Combine every feed's Definitions + external specs into one.
        if external_specs:
            all_defs_children.append(Definitions(assets=external_specs))

        return Definitions.merge(*all_defs_children)


def _resolve_base(context: ComponentLoadContext) -> Path:
    """Best-effort resolution of the component YAML directory."""
    for attr in ("path", "component_path", "yaml_path", "file_path"):
        val = getattr(context, attr, None)
        if val is None:
            continue
        if callable(val):
            try:
                val = val()
            except Exception:  # noqa: BLE001
                continue
            if val is None:
                continue
        p = Path(val)
        return p if p.is_dir() else p.parent
    return Path.cwd()

# BEGIN feed partition helpers (inlined; the builder and token blocks below are copies shared with the other partition-aware components)
def _feed_partitions(feed_name, feed):
    """The `partitions:` block of a feed file -> _PySparkPipelineImpl fields. Partitioning is opt-in per feed, because it
    changes what a run writes (use {partition_date} etc. in the feed's paths and SQL)."""
    block = feed.get("partitions")
    if not block:
        return {
            "partition_type": None, "partition_start": None, "partition_values": None,
            "dynamic_partition_name": None, "partition_cron_schedule": None, "partition_cron_fmt": None,
        }
    unknown = set(block) - {"type", "start", "values", "dynamic_partition_name", "cron_schedule", "cron_fmt"}
    if unknown:
        raise ValueError(f"feed {feed_name!r}: unknown partitions keys {sorted(unknown)}; use type, start, values, dynamic_partition_name, cron_schedule, cron_fmt")
    if not block.get("type"):
        raise ValueError(f"feed {feed_name!r}: partitions needs a `type` (daily, weekly, monthly, hourly, static, dynamic, cron)")
    if "{partition_" not in json.dumps({k: feed.get(k) for k in ("steps", "sinks", "variables")}, default=str):
        raise ValueError(
            f"feed {feed_name!r} is partitioned but never uses a {{partition_*}} token in its steps, sinks or variables, so every "
            "partition would read and write the same data. Use e.g. path: s3://lake/in/dt={partition_date}."
        )
    for sink in feed.get("sinks") or []:
        if "{partition_" in str(sink.get("path", "")) and not (sink.get("asset_key") or sink.get("table")):
            raise ValueError(
                f"feed {feed_name!r}: a sink path with a {{partition_*}} token cannot name the asset, so give that sink an explicit "
                "`asset_key:` (for example asset_key: events_daily)."
            )
    return {"partition_type": block["type"], "partition_start": block.get("start"), "partition_values": block.get("values"),
            "dynamic_partition_name": block.get("dynamic_partition_name"),
            "partition_cron_schedule": block.get("cron_schedule"), "partition_cron_fmt": block.get("cron_fmt")}


def _partitioned_schedule(name, cron, target, timezone, feed_name, parts):
    """A cron schedule for a partitioned feed: each tick runs the latest completed partition (time partitions only)."""
    if parts["partition_type"] not in ("daily", "weekly", "monthly", "hourly", "cron"):
        raise ValueError(f"feed {feed_name!r}: a `schedule` needs a time partition (daily/weekly/monthly/hourly/cron), not {parts['partition_type']!r}")
    partitions_def = _build_partitions_def(parts["partition_type"], parts["partition_start"], parts["partition_values"], parts["dynamic_partition_name"], None,
    cron_schedule=parts["partition_cron_schedule"],
    cron_fmt=parts["partition_cron_fmt"] or "%Y-%m-%d",
)

    @dg.schedule(name=name, cron_schedule=cron, target=target, execution_timezone=timezone)
    def _schedule(context):
        keys = partitions_def.get_partition_keys(current_time=context.scheduled_execution_time)
        if not keys:
            return dg.SkipReason(f"No completed partition of {feed_name!r} yet.")
        return dg.RunRequest(partition_key=keys[-1])

    return _schedule


from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
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
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
# END feed partition helpers
