"""DynamicFanoutAssetComponent — the asset-lineage sibling of
DynamicFanoutJobComponent.

Same runtime shape as the job version — `discover → .map() → .collect()`
via Dagster's DynamicOut. But instead of producing a `@dg.job`, this
component produces a `@dg.graph_asset` so the fan-out lives in your asset
catalog with proper lineage:

  <upstream_asset_key>  (optional — loaded and passed to discover)
        │
        ▼  @dg.graph_asset  <asset_name>
              _discover  (@op, DynamicOut)  ← emits N items
                │
                ├── _process[key1]  ← via .map()  } run in parallel per the
                ├── _process[key2]  ← via .map()  } run executor's slot count
                └── _process[keyN]  ← via .map()  }
                │
                _collect(list_of_results)  ← optional aggregation
        │
        ▼  downstream assets consume the asset's materialized value

Same 3 user-provided callables as the job version:

    discover(upstream=<upstream_value>, **discover_kwargs) → List[item]
    process(item, **process_kwargs)                        → Any
    collect(results: List[Any])                            → Any   (optional)

Set `upstream_asset_key` when discovery reads from another Dagster asset —
the graph_asset takes that asset as an @ins param and passes it into
discover via `upstream=<value>`. Leave `upstream_asset_key` unset when
discover fetches items from its own source (a URL list, a queue, a
filesystem scan).

When to reach for this vs DynamicFanoutJobComponent:
  - You want the fan-out to appear in the asset graph (with upstream/
    downstream lineage) → this component.
  - You want a scheduled/on-demand job with no asset semantics → the job
    version.
"""

import importlib
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _resolve(callable_path: str):
    module_path, fn_name = callable_path.split(":")
    mod = importlib.import_module(module_path)
    return getattr(mod, fn_name)


class DynamicFanoutAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generic DynamicOut fan-out AS AN ASSET: discover → map → collect."""

    asset_name: str = Field(description="Name of the emitted @graph_asset.")
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['fanout', 'transform'].",
    )

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key to load and pass into the discover "
            "callable as `upstream=<value>`. When None, discover fetches its "
            "items from its own source (URL list, filesystem, queue, etc.)."
        ),
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of upstream Dagster asset keys as lineage-only "
            "dependencies — declares graph edges without loading data through "
            "the IO manager. Use for assets whose materialization doesn't "
            "write a value to the IO manager (e.g. dbt models writing to "
            "an external warehouse). For value-loading upstreams use "
            "`upstream_asset_key` instead."
        ),
    )

    discover_callable_path: str = Field(
        description=(
            "'module:function' that returns an iterable of items. Signature: "
            "discover(upstream=<loaded>, **discover_kwargs) → List[item]. "
            "The upstream kwarg is only passed when upstream_asset_key is set."
        ),
    )
    discover_kwargs: Optional[Dict[str, Any]] = Field(
        default=None, description="Static kwargs passed to discover."
    )

    process_callable_path: str = Field(
        description="'module:function' that takes one item and returns a result. Signature: process(item, **process_kwargs)."
    )
    process_kwargs: Optional[Dict[str, Any]] = Field(
        default=None, description="Static kwargs merged into each process call."
    )

    collect_callable_path: Optional[str] = Field(
        default=None,
        description=(
            "Optional 'module:function' that takes the list of process results "
            "and returns the asset's final value. If omitted, the asset "
            "materializes as the raw list of process results."
        ),
    )

    mapping_key_field: Optional[str] = Field(
        default=None,
        description=(
            "If items are dicts, use this field as the DynamicOutput "
            "mapping_key so per-item retries are stable across runs. None → "
            "the loop index is used (stable within a run, not across)."
        ),
    )

    max_concurrent_tag_value: Optional[str] = Field(
        default=None,
        description=(
            "If set, applies `dagster/concurrency_key=<value>` to the process "
            "op so an instance-level concurrency limit governs fan-out "
            "parallelism."
        ),
    )

    retry_max_retries: Optional[int] = Field(default=None, description="Per-item retry max.")
    retry_delay_seconds: Optional[int] = Field(default=None)
    retry_backoff: str = Field(default="exponential")

    fail_on_empty: bool = Field(
        default=False, description="Fail the graph if discover returns no items."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        retry = None
        if self.retry_max_retries is not None:
            retry = dg.RetryPolicy(
                max_retries=self.retry_max_retries,
                delay=self.retry_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_backoff == "exponential" else dg.Backoff.LINEAR,
            )
        op_tags = {"dagster/concurrency_key": self.max_concurrent_tag_value} if self.max_concurrent_tag_value else None

        # ─── Ops ───────────────────────────────────────────────────────────
        # discover takes optional Nothing-typed ordering ins:
        #   - `upstream` (if upstream_asset_key set) — value is loaded in-op
        #     via load_asset_value; the input itself is Nothing for ordering.
        #   - `_dep_N` for each lineage-only dep (deps field) — pure ordering.
        _upstream_key = self.upstream_asset_key
        _dep_names: List[str] = [f"_dep_{i}" for i, _ in enumerate(self.deps or [])]

        _discover_ins: Dict[str, Any] = {}
        if _upstream_key:
            _discover_ins["upstream"] = dg.In(dg.Nothing)
        for n in _dep_names:
            _discover_ins[n] = dg.In(dg.Nothing)

        if _upstream_key:
            @dg.op(out=dg.DynamicOut(), ins=_discover_ins)
            def _discover(context, **_ignored):
                fn = _resolve(_self.discover_callable_path)
                upstream_val = context.op_execution_context.load_asset_value(
                    dg.AssetKey.from_user_string(_upstream_key)
                )
                items = fn(upstream=upstream_val, **(_self.discover_kwargs or {}))
                items = list(items)
                context.log.info(f"discovered {len(items)} item(s) via {_self.discover_callable_path}")
                if not items and _self.fail_on_empty:
                    raise Exception("discover returned no items and fail_on_empty=True")
                for i, item in enumerate(items):
                    if _self.mapping_key_field and isinstance(item, dict) and _self.mapping_key_field in item:
                        key = str(item[_self.mapping_key_field])
                    else:
                        key = str(i)
                    yield dg.DynamicOutput(item, mapping_key=key)
        else:
            @dg.op(out=dg.DynamicOut(), ins=_discover_ins)
            def _discover(context, **_ignored):
                fn = _resolve(_self.discover_callable_path)
                items = fn(**(_self.discover_kwargs or {}))
                items = list(items)
                context.log.info(f"discovered {len(items)} item(s) via {_self.discover_callable_path}")
                if not items and _self.fail_on_empty:
                    raise Exception("discover returned no items and fail_on_empty=True")
                for i, item in enumerate(items):
                    if _self.mapping_key_field and isinstance(item, dict) and _self.mapping_key_field in item:
                        key = str(item[_self.mapping_key_field])
                    else:
                        key = str(i)
                    yield dg.DynamicOutput(item, mapping_key=key)

        @dg.op(retry_policy=retry, tags=op_tags, name="process")
        def _process(context, item):
            fn = _resolve(_self.process_callable_path)
            extra = _self.process_kwargs or {}
            result = fn(item, **extra)
            context.log.info(f"processed → {str(result)[:200]}")
            return result

        @dg.op(name="collect")
        def _collect(context, results: list):
            if _self.collect_callable_path:
                fn = _resolve(_self.collect_callable_path)
                final = fn(results)
                context.log.info(f"collected {len(results)} result(s)")
                return final
            return list(results)

        # ─── Kinds / tags ─────────────────────────────────────────────────
        _kinds = set(self.kinds or []) | {"fanout", "transform"}
        _tags = dict(self.tags or {})
        for k in _kinds:
            _tags[f"dagster/kind/{k}"] = ""

        # ─── The graph_asset ───────────────────────────────────────────────
        _asset_kwargs: Dict[str, Any] = dict(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Dynamic fan-out asset: {self.asset_name}",
            owners=list(self.owners or []),
            tags=_tags,
        )
        _ins: Dict[str, Any] = {}
        if _upstream_key:
            _ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(_upstream_key))
        # Lineage-only deps: declared as Nothing-typed AssetIns so Dagster wires
        # the graph edge but the IO manager is never asked to load a value.
        # Used when the upstream asset writes to an external store (dbt models
        # in DuckDB, external warehouses, etc.) instead of the IO manager.
        _dep_names: List[str] = []
        for i, d in enumerate(self.deps or []):
            name = f"_dep_{i}"
            _ins[name] = dg.AssetIn(key=dg.AssetKey.from_user_string(d), dagster_type=dg.Nothing)
            _dep_names.append(name)
        if _ins:
            _asset_kwargs["ins"] = _ins

        # Build graph function whose signature matches @graph_asset's declared
        # ins (upstream first if set, then one param per Nothing-typed dep).
        # Each parameter is routed into _discover as its Nothing-typed input.
        params = (["upstream"] if _upstream_key else []) + _dep_names
        if params:
            arglist = ", ".join(params)
            discover_kwargs_pass = ", ".join(f"{n}={n}" for n in params)
            src = (
                f"def _fanout_graph_asset({arglist}):\n"
                f"    items = _discover({discover_kwargs_pass})\n"
                f"    processed = items.map(_process)\n"
                f"    return _collect(processed.collect())\n"
            )
            ns: Dict[str, Any] = {"_discover": _discover, "_process": _process, "_collect": _collect}
            exec(src, ns)
            _fanout_graph_asset = dg.graph_asset(**_asset_kwargs)(ns["_fanout_graph_asset"])
        else:
            @dg.graph_asset(**_asset_kwargs)
            def _fanout_graph_asset():
                items = _discover()
                processed = items.map(_process)
                return _collect(processed.collect())

        return dg.Definitions(assets=[_fanout_graph_asset])
