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
    histogram_bins: Optional[int] = None,
    quantiles: Optional[List[float]] = None,
    correlation_matrix: bool = False,
) -> Dict[str, Any]:
    """Return a nested dict: {global: {...}, columns: {col: {...}}, [extras: {...}]}.

    Extras (all opt-in):
    - `histogram_bins`: per-numeric-column histogram — stored on each column
      profile as `histogram: {bin_edges: [...], counts: [...]}`.
    - `quantiles`: per-numeric-column quantile fractions — stored on each
      column profile as `quantiles: {"p25": ..., "p50": ..., ...}`.
    - `correlation_matrix`: Pearson correlation across numeric columns —
      stored on the top-level result as `correlation_matrix`.
    """
    import pandas as pd
    n_rows = int(len(df))
    columns = list(df.columns)
    if top_n_columns:
        columns = columns[:top_n_columns]
    q_list = list(quantiles) if quantiles else []
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
            # Histogram — bin_edges + counts.
            if histogram_bins and histogram_bins > 0:
                try:
                    import numpy as np
                    clean = col.dropna()
                    if len(clean) > 0:
                        counts, edges = np.histogram(clean.to_numpy(), bins=int(histogram_bins))
                        p["histogram"] = {
                            "bin_edges": [float(x) for x in edges.tolist()],
                            "counts": [int(x) for x in counts.tolist()],
                        }
                except Exception:  # noqa: BLE001
                    pass
            # Quantiles — {p25: ..., p50: ..., ...}.
            if q_list:
                try:
                    clean = col.dropna()
                    if len(clean) > 0:
                        qvals = clean.quantile(q_list)
                        p["quantiles"] = {
                            f"p{int(round(q * 100))}": float(qvals.loc[q])
                            for q in q_list
                        }
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
    result: Dict[str, Any] = {
        "global": {
            "row_count": n_rows,
            "column_count": int(df.shape[1]),
            "profiled_columns": len(col_profiles),
        },
        "columns": col_profiles,
    }
    if correlation_matrix:
        try:
            numeric_df = df.select_dtypes(include="number")
            # Drop all-null columns so we don't emit NaN pairs.
            keep = [c for c in numeric_df.columns if numeric_df[c].notna().any()]
            if len(keep) >= 2:
                corr = numeric_df[keep].corr(method="pearson")
                cm: Dict[str, Dict[str, float]] = {}
                for a in corr.columns:
                    row: Dict[str, float] = {}
                    for b in corr.columns:
                        v = corr.at[a, b]
                        # Skip NaN cells (all-null overlap, zero-variance, etc.).
                        try:
                            fv = float(v)
                        except Exception:  # noqa: BLE001
                            continue
                        if fv != fv:  # NaN check
                            continue
                        row[str(b)] = round(fv, 6)
                    cm[str(a)] = row
                result["correlation_matrix"] = cm
        except Exception:  # noqa: BLE001
            pass
    return result


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


_SPARK_BARS = "▁▂▃▄▅▆▇█"


def _render_sparkline(counts: List[int]) -> str:
    """One-line Unicode sparkline of a histogram — 8 gradient chars."""
    if not counts:
        return ""
    mx = max(counts)
    if mx == 0:
        return _SPARK_BARS[0] * len(counts)
    n_levels = len(_SPARK_BARS) - 1
    return "".join(_SPARK_BARS[int(round((c / mx) * n_levels))] for c in counts)


def _render_histogram_png(col_name: str, hist: Dict[str, Any]) -> Optional[str]:
    """Render a histogram as a PNG data-URI for embedding in Markdown.
    Returns None if matplotlib isn't installed. Opt-in only — PNG is bigger."""
    edges = hist.get("bin_edges") or []
    counts = hist.get("counts") or []
    if not counts or not edges or len(edges) < 2:
        return None
    try:
        import base64 as _b64
        import io as _io
        import matplotlib
        matplotlib.use("Agg")   # no display backend needed
        import matplotlib.pyplot as _plt
    except ImportError:
        return None

    fig, ax = _plt.subplots(figsize=(6, 2.2), dpi=100)
    widths = [edges[i + 1] - edges[i] for i in range(len(counts))]
    lefts = edges[:-1]
    ax.bar(lefts, counts, width=widths, align="edge", edgecolor="black", linewidth=0.3)
    ax.set_title(f"{col_name} — n={sum(counts)}", fontsize=10)
    ax.set_ylabel("count", fontsize=8)
    ax.tick_params(axis="both", labelsize=8)
    fig.tight_layout()
    buf = _io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    _plt.close(fig)
    b64 = _b64.b64encode(buf.getvalue()).decode("ascii")
    return f"![{col_name} histogram](data:image/png;base64,{b64})"


def _render_histogram_md(
    col_name: str,
    hist: Dict[str, Any],
    render_mode: str = "ascii",
) -> str:
    """Render a numeric column's histogram as inline Markdown.

    render_mode:
    - "ascii" (default, zero deps) — Unicode-bar table + sparkline
    - "png" (opt-in, requires matplotlib) — embedded PNG data-URI
    - "both" — sparkline preview + PNG (best of both)
    """
    edges = hist.get("bin_edges") or []
    counts = hist.get("counts") or []
    if not counts or not edges or len(edges) < 2:
        return f"### `{col_name}` histogram\n_(empty)_"

    spark = _render_sparkline(counts)
    parts = [f"### `{col_name}` histogram", "", f"`{spark}` (n={sum(counts)})"]

    if render_mode in ("png", "both"):
        png = _render_histogram_png(col_name, hist)
        if png:
            parts.extend(["", png])
        elif render_mode == "png":
            # Fall back to ascii table if matplotlib unavailable
            render_mode = "ascii"
            parts.append("")
            parts.append("_matplotlib not installed — falling back to ASCII_")

    if render_mode in ("ascii", "both") and render_mode != "png":
        max_c = max(counts)
        parts.extend(["", "| bin | count | |", "|---|---:|:---|"])
        for i, c in enumerate(counts):
            lo, hi = edges[i], edges[i + 1]
            bar_len = 0 if max_c == 0 else int(round((c / max_c) * 24))
            bar = "█" * bar_len if bar_len > 0 else "▏"
            parts.append(f"| `{lo:.4g}`..`{hi:.4g}` | {c} | {bar} |")

    return "\n".join(parts)


def _render_quantiles_md(quantiles_by_col: Dict[str, Dict[str, float]]) -> str:
    """Render numeric quantiles across columns as one Markdown table."""
    if not quantiles_by_col:
        return ""
    # Union of quantile keys across cols, preserving common order.
    all_qs: List[str] = []
    seen = set()
    for q_map in quantiles_by_col.values():
        for k in q_map.keys():
            if k not in seen:
                seen.add(k)
                all_qs.append(k)
    if not all_qs:
        return ""
    header = "| column | " + " | ".join(all_qs) + " |"
    sep = "|---|" + "|".join(["---:"] * len(all_qs)) + "|"
    rows = []
    for col, qmap in quantiles_by_col.items():
        cells = [f"{qmap.get(q, ''):.4g}" if isinstance(qmap.get(q), (int, float)) else "" for q in all_qs]
        rows.append(f"| `{col}` | " + " | ".join(cells) + " |")
    return "### Quantiles\n\n" + "\n".join([header, sep] + rows)


def _render_correlation_md(corr: Dict[str, Dict[str, float]]) -> str:
    """Render Pearson correlation as a Markdown matrix."""
    if not corr:
        return ""
    cols = list(corr.keys())
    header = "| | " + " | ".join(f"`{c}`" for c in cols) + " |"
    sep = "|---|" + "|".join(["---:"] * len(cols)) + "|"
    rows = []
    for a in cols:
        cells = []
        for b in cols:
            v = corr.get(a, {}).get(b)
            cells.append(f"{v:.3f}" if isinstance(v, (int, float)) else "")
        rows.append(f"| `{a}` | " + " | ".join(cells) + " |")
    return "### Correlation matrix (Pearson)\n\n" + "\n".join([header, sep] + rows)


def _profile_markdown(profile: Dict[str, Any], histogram_render: str = "ascii") -> str:
    """Roll up the whole profile into one Markdown string for the UI.
    Renders as native Markdown in Dagster's Metadata panel via MetadataValue.md.

    histogram_render: 'ascii' | 'png' | 'both' — see _render_histogram_md.
    """
    parts: List[str] = []
    g = profile.get("global", {})
    parts.append(f"## Profile — {g.get('row_count', '?')} rows × {g.get('column_count', '?')} cols")

    cols = profile.get("columns", {}) or {}

    # Per-column histograms (numeric only)
    histograms = {name: p["histogram"] for name, p in cols.items() if isinstance(p, dict) and p.get("histogram")}
    if histograms:
        parts.append("")
        parts.append("## Numeric distributions")
        for col_name, hist in histograms.items():
            parts.append("")
            parts.append(_render_histogram_md(col_name, hist, render_mode=histogram_render))

    # Quantiles table
    quantiles_by_col = {name: p["quantiles"] for name, p in cols.items() if isinstance(p, dict) and p.get("quantiles")}
    if quantiles_by_col:
        parts.append("")
        parts.append(_render_quantiles_md(quantiles_by_col))

    # Correlation matrix (only present when correlation_matrix=True was passed)
    corr = profile.get("correlation_matrix")
    if corr:
        parts.append("")
        parts.append(_render_correlation_md(corr))

    return "\n".join(parts)


def _emit_profile_observations(
    context: Any, profile: Dict[str, Any], histogram_render: str = "ascii",
) -> Dict[str, Any]:
    """Emit AssetObservation with typed metadata; return the flat metadata dict
    suitable for the primary AssetMaterialization.

    The profile is rendered TWO ways:
    - `profile_report` as `MetadataValue.md(...)` — Unicode-bar histograms +
      quantile table + correlation matrix render inline in the Dagster UI.
      When `histogram_render='png'` or `'both'` is set + matplotlib is
      available, embedded PNG data-URIs render instead of / alongside the
      ASCII bar table.
    - `profile` as `MetadataValue.json(...)` — full structured payload for
      programmatic queries (dashboards, drift detection, etc.).
    """
    rendered_md = _profile_markdown(profile, histogram_render=histogram_render)
    md: Dict[str, Any] = {
        "profile_row_count": dg.MetadataValue.int(int(profile["global"]["row_count"])),
        "profile_column_count": dg.MetadataValue.int(int(profile["global"]["column_count"])),
        "profile_columns_summary": dg.MetadataValue.json(profile["columns"]),
        "profile_report": dg.MetadataValue.md(rendered_md),
    }
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["profile_asset"])
        # Emit ONE observation with the full profile JSON + rendered markdown.
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "profile_row_count": str(profile["global"]["row_count"]),
                    "profile_column_count": str(profile["global"]["column_count"]),
                },
                metadata={
                    "profile": dg.MetadataValue.json(profile),
                    "profile_report": dg.MetadataValue.md(rendered_md),
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
    histogram_bins: Optional[int] = None,
    quantiles: Optional[List[float]] = None,
    correlation_matrix: bool = False,
    histogram_render: str = "ascii",
) -> Callable:
    """Auto-profile the DataFrame returned by the decorated compute.

    Applied BEFORE `@dg.asset`. Emits one `AssetObservation` with the
    full profile + a set of typed `MetadataValue`s on the materialization.

    ```python
    from dagster_community_components import profile

    @dg.asset
    @profile(
        categorical_max_distinct=100,
        histogram_bins=10,
        quantiles=[0.25, 0.5, 0.75, 0.95, 0.99],
        correlation_matrix=True,
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

    Optional per-numeric-column extras: `histogram` (bin_edges + counts) when
    `histogram_bins` is set; `quantiles` ({"p25": ..., "p50": ..., ...}) when
    `quantiles` is non-empty.

    Optional top-level extra: `correlation_matrix` (Pearson) across numeric
    columns when `correlation_matrix=True`. Skipped for column pairs where
    either side is all-null.

    `custom_probes` — user extensions. Each `python: 'mod:fn'` receives
    the DataFrame and returns a dict (mixed into the observation metadata).
    """
    _probes = list(custom_probes or [])
    _q = list(quantiles) if quantiles is not None else [0.25, 0.5, 0.75, 0.95, 0.99]

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
            prof = _compute_profile(
                df,
                categorical_max_distinct,
                top_n_columns,
                histogram_bins=histogram_bins,
                quantiles=_q,
                correlation_matrix=correlation_matrix,
            )
            if _probes:
                prof["custom"] = _run_custom_probes(df, _probes, context)
            md = _emit_profile_observations(context, prof, histogram_render=histogram_render)
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
    """YAML shape of the profiler. Two authoring modes:

    1. **Define a new profiled asset** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds
       a single asset that computes and emits a data profile on every
       materialization.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets are materialized as they would normally, and
       each compute's DataFrame return value is auto-profiled. Preserves
       inner asset partitions, deps, resources, kinds, tags, group,
       description. Direct YAML analog of `@profile @dg.asset` in Python.
       Requires the inner component's asset to return a `pandas.DataFrame`.

    `wraps:` and `compute:` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with auto-profiling instead of defining new compute. "
            "Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. "
            "Inner asset must return pandas.DataFrame. Mutually exclusive with `compute`."
        ),
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
    histogram_bins: Optional[int] = Field(
        default=None,
        description="If set, emit per-numeric-column histogram with this many bins.",
    )
    quantiles: List[float] = Field(
        default_factory=lambda: [0.25, 0.5, 0.75, 0.95, 0.99],
        description="Quantile fractions to compute per numeric column. Empty list disables.",
    )
    correlation_matrix: bool = Field(
        default=False,
        description="If True, compute Pearson correlation between numeric columns and emit as metadata. Expensive on wide tables — off by default.",
    )
    histogram_render: str = Field(
        default="ascii",
        description=(
            "How histograms render in the Metadata panel. "
            "'ascii' (default, zero deps) = Unicode-bar table + sparkline; "
            "'png' (requires matplotlib) = embedded PNG data-URI; "
            "'both' = sparkline preview + PNG. Falls back to 'ascii' if matplotlib is unavailable."
        ),
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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("ProfileAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)
        if self.compute is None:
            raise ValueError("ProfileAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("ProfileAssetComponent: `asset_name` required when using `compute:`.")

        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        cat_max = self.categorical_max_distinct
        top_n = self.top_n_columns
        probes = list(self.custom_probes or [])
        hist_bins = self.histogram_bins
        q_list = list(self.quantiles or [])
        corr_on = bool(self.correlation_matrix)
        histogram_render = str(self.histogram_render or "ascii")

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

            prof = _compute_profile(
                df,
                cat_max,
                top_n,
                histogram_bins=hist_bins,
                quantiles=q_list,
                correlation_matrix=corr_on,
            )
            if probes:
                prof["custom"] = _run_custom_probes(df, probes, context)
            md = _emit_profile_observations(context, prof, histogram_render=histogram_render)
            context.log.info(
                f"[profile] rows={prof['global']['row_count']} "
                f"cols={prof['global']['column_count']} probes={len(probes)}"
            )
            return dg.MaterializeResult(metadata=md)

        return dg.Definitions(assets=[_profiled_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)
        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                wrapped_assets.append(asset_def)
                continue
            wrapped_assets.append(self._wrap_single_asset(asset_def))
        return dg.Definitions(
            assets=wrapped_assets,
            resources=inner_defs.resources,
            sensors=inner_defs.sensors,
            schedules=inner_defs.schedules,
            asset_checks=inner_defs.asset_checks,
            jobs=inner_defs.jobs,
            loggers=inner_defs.loggers,
        )

    def _wrap_single_asset(self, asset_def: "dg.AssetsDefinition") -> "dg.AssetsDefinition":
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)
        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        cat_max = self.categorical_max_distinct
        top_n = self.top_n_columns
        probes = list(self.custom_probes or [])
        hist_bins = self.histogram_bins
        q_list = list(self.quantiles or [])
        corr_on = bool(self.correlation_matrix)
        histogram_render = str(self.histogram_render or "ascii")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"profile", "observability"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Profile-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [profile: cat_max={cat_max}, top_n={top_n}, probes={len(probes)}]"
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []

        @dg.asset(
            key=key,
            partitions_def=asset_def.partitions_def,
            deps=inner_deps,
            group_name=(spec.group_name if spec else None),
            kinds=merged_kinds,
            tags=merged_tags,
            owners=merged_owners,
            description=merged_description,
            metadata=(dict(spec.metadata) if (spec and spec.metadata) else {}),
            code_version=(spec.code_version if spec else None),
        )
        def _profile_wrapped(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            result = inner_compute(context, **kwargs)

            # Extract the DataFrame from the inner's return value.
            df_to_profile = None
            if isinstance(result, pd.DataFrame):
                df_to_profile = result
            elif isinstance(result, dg.Output):
                if isinstance(result.value, pd.DataFrame):
                    df_to_profile = result.value
            elif isinstance(result, dg.MaterializeResult):
                context.log.warning(
                    "[profile wrap] inner returned MaterializeResult (no value); "
                    "cannot profile — passing through unchanged"
                )
                return result

            if df_to_profile is None:
                context.log.warning(
                    f"[profile wrap] inner returned {type(result).__name__} (not DataFrame); "
                    "skipping profile — passing through unchanged"
                )
                return result

            prof = _compute_profile(
                df_to_profile,
                cat_max,
                top_n,
                histogram_bins=hist_bins,
                quantiles=q_list,
                correlation_matrix=corr_on,
            )
            if probes:
                prof["custom"] = _run_custom_probes(df_to_profile, probes, context)
            md = _emit_profile_observations(context, prof, histogram_render=histogram_render)
            context.log.info(
                f"[profile wrap] rows={prof['global']['row_count']} "
                f"cols={prof['global']['column_count']} probes={len(probes)}"
            )
            return dg.Output(df_to_profile, metadata=md)

        return _profile_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: '...', attributes: {...}}` → instantiated component."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("ProfileAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"ProfileAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"ProfileAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"ProfileAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
