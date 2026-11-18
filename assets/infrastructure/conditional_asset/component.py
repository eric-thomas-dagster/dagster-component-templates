"""ConditionalAssetComponent.

Branch / short-circuit as a Dagster asset -- the Airflow `ShortCircuitOperator` /
`BranchPythonOperator` equivalent. Built on a real, verified, native Dagster mechanism: a
`@dg.multi_asset` output declared `AssetOut(is_required=False)` that the compute function
chooses not to `yield` causes every downstream step depending on it to skip (cascading through
arbitrarily long chains), while the run still succeeds -- confirmed with real
`dg.materialize()` runs before building this, not assumed.
"""
import inspect
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


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


def _resolve_callable(callable_path: str):
    import importlib
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


def _bind_call(fn, args, kwargs, upstream_kwargs):
    """Inject only the names `fn`'s own signature declares (or everything, for **kwargs) --
    the same "inject only what's declared" rule as python_callable_asset."""
    sig_params = inspect.signature(fn).parameters
    has_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values())
    call_kwargs = dict(kwargs or {})
    for name, val in upstream_kwargs.items():
        if has_var_kw or name in sig_params:
            call_kwargs.setdefault(name, val)
    return fn(*(args or []), **call_kwargs)


class ConditionalAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Branch / short-circuit as a Dagster asset -- the Airflow `ShortCircuitOperator` /
    `BranchPythonOperator` equivalent.

    `mode: "short_circuit"`: the callable returns a truthy/falsy value. Truthy -> the single
    `asset_key` materializes and everything downstream runs normally. Falsy -> it doesn't
    materialize this run, and every downstream asset depending on it is skipped (not failed) --
    cascading through the whole downstream chain.

    `mode: "branch"`: the callable returns the name of exactly one of the statically-declared
    `branches` (mirroring Airflow's BranchPythonOperator -- the *set* of possible branches is
    always known at authoring time; only which one runs is dynamic). Only that branch's asset
    materializes; every other branch -- and everything depending on it -- skips.
    """

    mode: Literal["short_circuit", "branch"] = Field(description="'short_circuit': one asset, materializes or not. 'branch': pick exactly one of several statically-declared branches.")
    callable_path: str = Field(description="'module.path:function_name'. short_circuit: returns a truthy/falsy value. branch: returns a branch name (str), or (branch_name, payload) for an explicit payload.")
    args: Optional[List[Any]] = Field(default=None, description="Static positional arguments passed to the callable.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword arguments passed to the callable.")
    inputs: Optional[Dict[str, str]] = Field(
        default=None,
        description="{name: upstream_asset_key}. Each entry's value is loaded via the IO manager and bound to a parameter of that name if the callable's signature declares one (TaskFlow-style) -- same shape as python_callable_asset's `inputs`.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value).")

    asset_key: Optional[str] = Field(default=None, description="mode='short_circuit' only (required then): the single gate asset key, slash-delimited.")
    branches: Optional[Dict[str, str]] = Field(
        default=None,
        description="mode='branch' only (required then): {branch_name: asset_key}. asset_key is slash-delimited. The callable must return one of these branch_name values.",
    )

    group_name: str = Field(default="infrastructure", description="Dagster asset group name (applied to every branch uniformly in branch mode).")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset(s) in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_mode_fields(self):
        if self.mode == "short_circuit":
            if not self.asset_key:
                raise ValueError("ConditionalAssetComponent: mode='short_circuit' requires `asset_key`.")
            if self.branches:
                raise ValueError("ConditionalAssetComponent: `branches` is only used with mode='branch'.")
        else:
            if not self.branches:
                raise ValueError("ConditionalAssetComponent: mode='branch' requires `branches` ({branch_name: asset_key}).")
            if self.asset_key:
                raise ValueError("ConditionalAssetComponent: `asset_key` is only used with mode='short_circuit'.")
        for name in (self.inputs or {}):
            if not name.isidentifier():
                raise ValueError(f"ConditionalAssetComponent: inputs key {name!r} must be a valid Python identifier.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Conditional Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        ins = {name: dg.AssetIn(key=dg.AssetKey.from_user_string(key)) for name, key in (component.inputs or {}).items()}
        deps_keys = [dg.AssetKey(d.split("/")) for d in (component.deps or [])]
        tags = component.asset_tags or None
        kinds = set(component.kinds) if component.kinds else None

        if component.mode == "short_circuit":
            outs = {
                "gate": dg.AssetOut(
                    key=dg.AssetKey(component.asset_key.split("/")),
                    description=component.description,
                    is_required=False,
                    owners=component.owners or None,
                    tags=tags,
                    kinds=kinds,
                ),
            }

            @dg.multi_asset(outs=outs, ins=ins or None, deps=deps_keys, group_name=component.group_name)
            def _conditional(context: dg.AssetExecutionContext, **upstream_kwargs):
                fn = _resolve_callable(component.callable_path)
                result = _bind_call(fn, component.args, component.kwargs, upstream_kwargs)
                if result:
                    yield dg.Output(result, output_name="gate")
                else:
                    context.log.info("short_circuit: callable returned falsy -- gate asset not materialized, downstream will skip.")

            return dg.Definitions(assets=[_conditional])

        branches = component.branches
        outs = {
            name: dg.AssetOut(
                key=dg.AssetKey(key.split("/")),
                description=component.description,
                is_required=False,
                owners=component.owners or None,
                tags=tags,
                kinds=kinds,
            )
            for name, key in branches.items()
        }

        @dg.multi_asset(outs=outs, ins=ins or None, deps=deps_keys, group_name=component.group_name)
        def _conditional(context: dg.AssetExecutionContext, **upstream_kwargs):
            fn = _resolve_callable(component.callable_path)
            result = _bind_call(fn, component.args, component.kwargs, upstream_kwargs)
            if isinstance(result, tuple) and len(result) == 2:
                chosen, payload = result
            else:
                chosen, payload = result, result
            if chosen not in branches:
                raise dg.Failure(
                    description=f"callable {component.callable_path!r} returned branch {chosen!r}, "
                    f"not one of the declared branches {sorted(branches)}."
                )
            context.log.info(f"branch: chose {chosen!r}; {sorted(set(branches) - {chosen})} will skip.")
            yield dg.Output(payload, output_name=chosen)

        return dg.Definitions(assets=[_conditional])
