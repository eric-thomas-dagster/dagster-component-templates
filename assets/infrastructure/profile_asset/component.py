"""ProfileAssetComponent + `@profile` — auto-emit a data profile per materialization.

Every materialization computes a lightweight profile of the produced
DataFrame and emits it as `AssetObservation` events with typed
metadata. Over time, the event log becomes a series of profile
snapshots — drift detection becomes free: query the event log,
compute deltas.

## What gets profiled (per materialization)

Global:
- `row_count` (int)
- `column_count` (int)

Per column:
- `dtype` (str)
- `null_count` / `null_ratio` (int / float)
- `distinct_count` (int)
- For numeric: `min` / `max` / `mean` / `std` (float)
- For categorical (< N distinct): `top_value_ratio` (float)

Optional user extensions via `custom_probes: List[Dict]` — each probe
runs a user function that returns extra metadata.

## Why this belongs in Dagster

Every profile stat becomes an `AssetObservation` with typed
`MetadataValue`. Consumers of the profile (drift alerts, DQ dashboards,
agents) can query them via `context.instance.get_event_records` and get
a full history without any external metrics store. The Dagster UI's
observation panel shows the trend automatically.

## Two shapes

- **`ProfileAssetComponent`** (YAML) — new asset with profiling wrapped in.
- **`@profile` decorator** — wraps an existing @dg.asset.

## Composes with

- `@data_contract` — the profile stats can feed contract SLA checks
  (row-count drop, null-ratio drift).
- `@lifecycle` — profile the STAGING data before publish; audit checks
  can reference profile metadata.
- `@cached` — profile only fires on cache miss (compute actually ran).
"""

import functools
import importlib
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Profile computation
# --------------------------------------------------------------------------


def _numeric_dtype(dtype_str: str) -> bool:
    d = dtype_str.lower()
    return d.startswith("int") or d.startswith("uint") or d.startswith("float") or d.startswith("bool")


def _compute_profile(
    df,
    categorical_max_distinct: int = 50,
    top_n_columns: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a nested dict: {global: {...}, columns: {col: {...}}}."""
    import pandas as pd
    n_rows = int(len(df))
    columns = list(df.columns)
    if top_n_columns:
        columns = columns[:top_n_columns]
    col_profiles: Dict[str, Dict[str, Any]] = {}
    for c in columns:
        col = df[c]
        dtype_s = str(col.dtype)
        n_null = int(col.isna().sum())
        p: Dict[str, Any] = {
            "dtype": dtype_s,
            "null_count": n_null,
            "null_ratio": round((n_null / n_rows) if n_rows else 0.0, 6),
            "distinct_count": int(col.nunique(dropna=False)),
        }
        if _numeric_dtype(dtype_s):
            try:
                p["min"] = float(col.min())
                p["max"] = float(col.max())
                p["mean"] = float(col.mean())
                p["std"] = float(col.std())
            except Exception:  # noqa: BLE001
                pass
        # Categorical: top value ratio if few distinct.
        if p["distinct_count"] > 0 and p["distinct_count"] <= categorical_max_distinct:
            try:
                vc = col.value_counts(dropna=False)
                top = int(vc.iloc[0])
                p["top_value_ratio"] = round((top / n_rows) if n_rows else 0.0, 6)
            except Exception:  # noqa: BLE001
                pass
        col_profiles[c] = p
    return {
        "global": {
            "row_count": n_rows,
            "column_count": int(df.shape[1]),
            "profiled_columns": len(col_profiles),
        },
        "columns": col_profiles,
    }


def _run_custom_probes(df, probes: List[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    """Run user-provided `custom_probes` — each `{name, python: 'mod:fn'}`.

    User function receives the DataFrame, returns a dict of metadata.
    """
    results: Dict[str, Any] = {}
    for probe in probes:
        name = probe.get("name") or probe.get("python") or "unnamed_probe"
        ref = probe.get("python")
        if not ref or ":" not in ref:
            results[name] = {"error": "missing or malformed python ref"}
            continue
        try:
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                results[name] = {"error": f"{ref!r} not callable"}
                continue
            v = fn(df)
            results[name] = v if isinstance(v, dict) else {"value": v}
        except Exception as exc:  # noqa: BLE001
            results[name] = {"error": f"{type(exc).__name__}: {exc}"}
    return results


def _emit_profile_observations(
    context: Any, profile: Dict[str, Any],
) -> Dict[str, Any]:
    """Emit AssetObservation with typed metadata; return the flat metadata dict
    suitable for the primary AssetMaterialization."""
    md: Dict[str, Any] = {
        "profile_row_count": dg.MetadataValue.int(int(profile["global"]["row_count"])),
        "profile_column_count": dg.MetadataValue.int(int(profile["global"]["column_count"])),
        "profile_columns_summary": dg.MetadataValue.json(profile["columns"]),
    }
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["profile_asset"])
        # Emit ONE observation with the full profile JSON — searchable.
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "profile_row_count": str(profile["global"]["row_count"]),
                    "profile_column_count": str(profile["global"]["column_count"]),
                },
                metadata={
                    "profile": dg.MetadataValue.json(profile),
                },
            ))
    except Exception:  # noqa: BLE001
        pass
    return md


# --------------------------------------------------------------------------
# @profile decorator
# --------------------------------------------------------------------------


def profile(
    *,
    categorical_max_distinct: int = 50,
    top_n_columns: Optional[int] = None,
    custom_probes: Optional[List[Dict[str, Any]]] = None,
) -> Callable:
    """Auto-profile the DataFrame returned by the decorated compute.

    Applied BEFORE `@dg.asset`. Emits one `AssetObservation` with the
    full profile + a set of typed `MetadataValue`s on the materialization.

    ```python
    from dagster_community_components import profile

    @dg.asset
    @profile(
        categorical_max_distinct=100,
        custom_probes=[
            {"name": "avg_order_value", "python": "my_project.probes:avg_order_value"},
        ],
    )
    def orders(context):
        return build_orders()
    ```

    Profile fields per column: dtype, null_count, null_ratio,
    distinct_count. For numerics: min, max, mean, std. For categoricals
    (< `categorical_max_distinct` distinct): top_value_ratio.

    `custom_probes` — user extensions. Each `python: 'mod:fn'` receives
    the DataFrame and returns a dict (mixed into the observation metadata).
    """
    _probes = list(custom_probes or [])

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            import pandas as pd
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError(
                    "@profile requires a Dagster context — decorator must wrap a Dagster asset/op compute."
                )
            df = fn(*args, **kwargs)
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"@profile: compute must return a pandas DataFrame; got {type(df).__name__}."
                )
            prof = _compute_profile(df, categorical_max_distinct, top_n_columns)
            if _probes:
                prof["custom"] = _run_custom_probes(df, _probes, context)
            md = _emit_profile_observations(context, prof)
            context.log.info(
                f"[profile] rows={prof['global']['row_count']} "
                f"cols={prof['global']['column_count']} "
                f"probes={len(_probes)}"
            )
            yield dg.Output(df, metadata=md)
        return _wrapped
    return _decorator


# --------------------------------------------------------------------------
# ProfileAssetComponent — YAML-defined new asset
# --------------------------------------------------------------------------


class ProfileAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the profiler. Defines a new asset that computes and
    emits a data profile on every materialization.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(
        description="`{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame."
    )
    categorical_max_distinct: int = Field(
        default=50,
        description="Columns with <= this many distinct values get `top_value_ratio` computed.",
    )
    top_n_columns: Optional[int] = Field(
        default=None,
        description="Profile only first N columns (for very wide DataFrames). Omit to profile all.",
    )
    custom_probes: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Extensions: [{name, python: 'mod:fn'}]. fn(df) returns dict.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'profile', 'observability'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Profile Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        cat_max = self.categorical_max_distinct
        top_n = self.top_n_columns
        probes = list(self.custom_probes or [])

        kinds_set = set(self.kinds or []) | {"python", "profile", "observability"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Auto-profiled asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _profiled_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"ProfileAssetComponent v1 supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                df = fn()
            elif n_positional == 1:
                df = fn(context)
            else:
                df = fn(context, kwargs.get("upstream"))

            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"compute must return a DataFrame; got {type(df).__name__}")

            prof = _compute_profile(df, cat_max, top_n)
            if probes:
                prof["custom"] = _run_custom_probes(df, probes, context)
            md = _emit_profile_observations(context, prof)
            context.log.info(
                f"[profile] rows={prof['global']['row_count']} "
                f"cols={prof['global']['column_count']} probes={len(probes)}"
            )
            return dg.MaterializeResult(metadata=md)

        return dg.Definitions(assets=[_profiled_asset])
