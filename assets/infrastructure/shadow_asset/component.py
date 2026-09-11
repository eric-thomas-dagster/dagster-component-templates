"""ShadowAssetComponent + `@shadow` — dual-run old + new implementations, diff outputs.

Runs the primary compute (returned as the canonical asset value), then
runs a shadow implementation in parallel with the same inputs. Compares
outputs; emits an `AssetObservation` per run with `shadow_match=true|false`.
Production always uses the primary result. The shadow only reports.

## Why this belongs in Dagster

- **AssetObservation with typed diff metadata** — every run leaves an
  event log entry: `shadow_match`, `shadow_diff_rows`, `shadow_extra_cols`,
  `shadow_missing_cols`, `shadow_error`. Sensors can escalate after N
  consecutive mismatches.
- **Composes with `@dry_run`** — you can shadow a candidate migration
  end-to-end without ever risking a bad materialization.
- **Fits the migration playbook** — ship shadow → verify convergence →
  flip primary → drop shadow. All the state is in the event log.

## Two shapes

- **`ShadowAssetComponent`** (YAML)
- **`@shadow` decorator** (Python)

## Diff strategy

Ordered fallback:
1. Both `None` → match.
2. pandas.DataFrame → compare `shape`, column sets, row-order-agnostic
   set equality on the first 500 rows.
3. list / tuple → element equality after optional sort.
4. dict → key-value equality.
5. Everything else → `primary == shadow`.

Diff details are emitted as `AssetObservation` metadata so downstream
sensors can classify.

## Behavior

- **Primary** value is always returned — production unchanged.
- **Shadow** runs after primary (sequentially in v1 — parallel with
  a worker pool is on the roadmap).
- Shadow **exceptions are trapped**: emitted as
  `shadow_error=<class>` observation, primary result still returned.
- `enforce_match=True` → mismatch raises `dg.Failure` (opt-in for
  release gates; default off = observe only).

## Composes with

- `@dry_run` — shadow a candidate migration end-to-end without dirtying prod.
- `@lifecycle` — shadow at the audit stage; primary publishes, shadow observes.
- `@profile` — profile both primary and shadow, then diff the profiles.
- `@smart_retry` — retry shadow independently of primary.
"""

import functools
import importlib
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field


_SHADOW_TAG = "shadow_match"


def _load_callable(ref: str) -> Callable:
    if not ref or ":" not in ref:
        raise ValueError(f"shadow compute must be 'module.path:function_name'; got {ref!r}")
    mod_path, fn_name = ref.rsplit(":", 1)
    fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
    if not callable(fn):
        raise ValueError(f"shadow compute {ref!r} not callable")
    return fn


def _apply_fuzzy_prep(df, fuzzy_match: Optional[Dict[str, Any]]):
    """Drop ignore_columns + optionally sort rows for order-invariant compare.

    Returns the mutated DataFrame (or the original if no prep needed).
    """
    if not fuzzy_match:
        return df
    ignore_cols = fuzzy_match.get("ignore_columns") or []
    if ignore_cols:
        drop = [c for c in ignore_cols if c in df.columns]
        if drop:
            df = df.drop(columns=drop)
    if fuzzy_match.get("ignore_row_order"):
        sort_cols = list(df.columns)
        if sort_cols:
            try:
                df = df.sort_values(sort_cols, kind="stable").reset_index(drop=True)
            except Exception:  # noqa: BLE001
                # unsortable (e.g. mixed types) — fall back to as-is order
                pass
    return df


def _dataframes_equal_with_tolerance(
    df_p, df_s, fuzzy_match: Optional[Dict[str, Any]],
) -> bool:
    """Element-wise DataFrame equality with float tolerance from fuzzy_match."""
    import pandas as pd
    tol = float((fuzzy_match or {}).get("float_tolerance", 0.0) or 0.0)
    if tol <= 0:
        return df_p.equals(df_s)
    if df_p.shape != df_s.shape or list(df_p.columns) != list(df_s.columns):
        return False
    for col in df_p.columns:
        c_p = df_p[col]
        c_s = df_s[col]
        if pd.api.types.is_float_dtype(c_p) and pd.api.types.is_float_dtype(c_s):
            close = ((c_p - c_s).abs() <= tol)
            # NaN handling: both NaN counts as equal.
            both_nan = c_p.isna() & c_s.isna()
            if not bool((close | both_nan).all()):
                return False
        else:
            if not c_p.equals(c_s):
                return False
    return True


def _row_diff_mask(df_p, df_s, fuzzy_match: Optional[Dict[str, Any]]):
    """Return a boolean Series marking rows where primary != shadow, honoring float tolerance."""
    import pandas as pd
    tol = float((fuzzy_match or {}).get("float_tolerance", 0.0) or 0.0)
    if tol <= 0:
        return (df_p != df_s).any(axis=1)
    # Build per-cell equal mask with tolerance-aware float comparison.
    eq_frames = []
    for col in df_p.columns:
        c_p = df_p[col]
        c_s = df_s[col]
        if pd.api.types.is_float_dtype(c_p) and pd.api.types.is_float_dtype(c_s):
            close = ((c_p - c_s).abs() <= tol) | (c_p.isna() & c_s.isna())
            eq_frames.append(~close)
        else:
            eq_frames.append(c_p != c_s)
    concat = pd.concat(eq_frames, axis=1)
    return concat.any(axis=1)


def _diff(
    primary: Any, shadow: Any, fuzzy_match: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return diff summary — always a dict; `match` key is the verdict.

    `fuzzy_match` (optional dict): `{float_tolerance: 1e-6,
    ignore_columns: [...], ignore_row_order: bool}`. Applies only when
    comparing pandas DataFrames.
    """
    out: Dict[str, Any] = {"match": False}
    try:
        if primary is None and shadow is None:
            out["match"] = True
            out["mode"] = "none"
            return out

        try:
            import pandas as pd
            if isinstance(primary, pd.DataFrame) and isinstance(shadow, pd.DataFrame):
                out["mode"] = "dataframe"
                p_prep = _apply_fuzzy_prep(primary, fuzzy_match)
                s_prep = _apply_fuzzy_prep(shadow, fuzzy_match)
                out["primary_shape"] = list(p_prep.shape)
                out["shadow_shape"] = list(s_prep.shape)
                cols_p = set(p_prep.columns)
                cols_s = set(s_prep.columns)
                out["shadow_extra_cols"] = sorted(cols_s - cols_p)
                out["shadow_missing_cols"] = sorted(cols_p - cols_s)
                if p_prep.shape != s_prep.shape or cols_p != cols_s:
                    out["match"] = False
                    return out
                common = sorted(cols_p)
                sample_p = p_prep[common].head(500).reset_index(drop=True)
                sample_s = s_prep[common].head(500).reset_index(drop=True)
                out["match"] = _dataframes_equal_with_tolerance(
                    sample_p, sample_s, fuzzy_match,
                )
                if not out["match"]:
                    diff_mask = _row_diff_mask(sample_p, sample_s, fuzzy_match)
                    out["shadow_diff_rows"] = int(diff_mask.sum())
                if fuzzy_match:
                    out["fuzzy_match_applied"] = True
                    if fuzzy_match.get("float_tolerance"):
                        out["float_tolerance"] = float(fuzzy_match["float_tolerance"])
                    if fuzzy_match.get("ignore_columns"):
                        out["ignore_columns"] = list(fuzzy_match["ignore_columns"])
                    if fuzzy_match.get("ignore_row_order"):
                        out["ignore_row_order"] = True
                return out
        except ImportError:
            pass

        if isinstance(primary, (list, tuple)) and isinstance(shadow, (list, tuple)):
            out["mode"] = "sequence"
            out["primary_len"] = len(primary)
            out["shadow_len"] = len(shadow)
            out["match"] = list(primary) == list(shadow)
            return out

        if isinstance(primary, dict) and isinstance(shadow, dict):
            out["mode"] = "dict"
            out["primary_keys"] = sorted(str(k) for k in primary.keys())
            out["shadow_keys"] = sorted(str(k) for k in shadow.keys())
            out["match"] = primary == shadow
            return out

        out["mode"] = "eq"
        out["match"] = bool(primary == shadow)
        return out
    except Exception as e:  # noqa: BLE001
        out["mode"] = "error"
        out["match"] = False
        out["diff_error"] = repr(e)
        return out


def _export_diff_rows(
    context: Any,
    primary: Any,
    shadow: Any,
    diff_export_uri: str,
    fuzzy_match: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Write mismatched rows to `<diff_export_uri>/<asset>/<ts>__<run_id>.parquet`.

    Returns the written path on success, else None. Only DataFrame diffs
    export rows; other types are a no-op. Never raises — best-effort.
    """
    try:
        import pandas as pd
        if not isinstance(primary, pd.DataFrame) or not isinstance(shadow, pd.DataFrame):
            return None
        p_prep = _apply_fuzzy_prep(primary, fuzzy_match)
        s_prep = _apply_fuzzy_prep(shadow, fuzzy_match)
        if p_prep.shape != s_prep.shape or list(p_prep.columns) != list(s_prep.columns):
            # Shape/schema mismatch — export both slices, tag with source.
            tagged_p = p_prep.assign(_shadow_source="primary").head(500)
            tagged_s = s_prep.assign(_shadow_source="shadow").head(500)
            diff_df = pd.concat([tagged_p, tagged_s], ignore_index=True)
        else:
            diff_mask = _row_diff_mask(
                p_prep.reset_index(drop=True), s_prep.reset_index(drop=True),
                fuzzy_match,
            )
            if not bool(diff_mask.any()):
                return None
            diff_p = p_prep.reset_index(drop=True)[diff_mask].assign(_shadow_source="primary")
            diff_s = s_prep.reset_index(drop=True)[diff_mask].assign(_shadow_source="shadow")
            diff_df = pd.concat([diff_p, diff_s], ignore_index=True)

        asset_key = getattr(context, "asset_key", None)
        asset_slug = asset_key.to_user_string() if asset_key else "shadow_asset"
        asset_slug = asset_slug.replace("/", "__")
        try:
            run_id = getattr(context.run, "run_id", "unknown")
        except Exception:  # noqa: BLE001
            run_id = "unknown"
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rel = f"{asset_slug}/{ts}__{run_id}.parquet"
        base = diff_export_uri.rstrip("/")
        full_path = f"{base}/{rel}"

        # Local filesystem path — mkdir + parquet write.
        if "://" not in full_path:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            diff_df.to_parquet(full_path, index=False)
            return full_path

        # Remote URI — try fsspec; if unavailable, log and give up.
        try:
            import fsspec
            fs, path = fsspec.core.url_to_fs(full_path)
            parent = path.rsplit("/", 1)[0] if "/" in path else ""
            if parent:
                try:
                    fs.makedirs(parent, exist_ok=True)
                except Exception:  # noqa: BLE001
                    pass
            with fs.open(path, "wb") as f:
                diff_df.to_parquet(f, index=False)
            return full_path
        except ImportError:
            try:
                context.log.warning(
                    f"@shadow: diff_export_uri={diff_export_uri!r} requires fsspec "
                    "for remote URIs; skipping export (`pip install fsspec`)."
                )
            except Exception:  # noqa: BLE001
                pass
            return None
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"@shadow: diff export failed: {type(e).__name__}: {e}")
        except Exception:  # noqa: BLE001
            pass
        return None


def _emit_shadow_observation(
    context: Any, diff: Dict[str, Any], shadow_elapsed_s: float,
    shadow_error: Optional[BaseException] = None,
    diff_export_path: Optional[str] = None,
) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["shadow_asset"])
        tags = {_SHADOW_TAG: str(bool(diff.get("match"))).lower()}
        if shadow_error is not None:
            tags["shadow_error"] = type(shadow_error).__name__

        metadata: Dict[str, Any] = {
            "shadow_match": dg.MetadataValue.bool(bool(diff.get("match"))),
            "shadow_elapsed_seconds": dg.MetadataValue.float(float(round(shadow_elapsed_s, 3))),
            "shadow_diff_mode": dg.MetadataValue.text(str(diff.get("mode", ""))),
        }
        for numeric_key in ("shadow_diff_rows", "primary_len", "shadow_len"):
            if numeric_key in diff and isinstance(diff[numeric_key], int):
                metadata[numeric_key] = dg.MetadataValue.int(diff[numeric_key])
        for list_key in ("shadow_extra_cols", "shadow_missing_cols"):
            if list_key in diff and isinstance(diff[list_key], list):
                metadata[list_key] = dg.MetadataValue.json(diff[list_key])
        if diff.get("fuzzy_match_applied"):
            metadata["fuzzy_match_applied"] = dg.MetadataValue.bool(True)
            if "float_tolerance" in diff:
                metadata["float_tolerance"] = dg.MetadataValue.float(
                    float(diff["float_tolerance"])
                )
        if diff_export_path:
            metadata["shadow_diff_export_path"] = dg.MetadataValue.path(diff_export_path)
        if shadow_error is not None:
            metadata["shadow_error"] = dg.MetadataValue.text(f"{type(shadow_error).__name__}: {shadow_error}")

        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata=metadata,
            ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"@shadow: could not emit observation: {e}")
        except Exception:  # noqa: BLE001
            pass


def _run_shadow(
    context: Any,
    shadow_fn: Callable,
    args: Tuple,
    kwargs: Dict[str, Any],
    primary_result: Any,
    enforce_match: bool,
    fuzzy_match: Optional[Dict[str, Any]] = None,
    diff_export_uri: Optional[str] = None,
) -> None:
    """Run the shadow implementation, diff against primary, emit observation.

    Exceptions inside `shadow_fn` are trapped (recorded as observation with
    `shadow_error` tag) — the primary result is what production sees.

    Args:
        fuzzy_match: Optional dict `{float_tolerance, ignore_columns,
            ignore_row_order}`. Applies to DataFrame diffs.
        diff_export_uri: Optional fsspec URI base. On mismatch, mismatched
            rows are written to
            `<uri>/<asset>/<UTC_ts>__<run_id>.parquet` and the path is
            emitted as observation metadata `shadow_diff_export_path`.
    """
    t0 = time.time()
    shadow_error = None
    shadow_result = None
    try:
        shadow_result = shadow_fn(*args, **kwargs)
    except BaseException as e:  # noqa: BLE001
        shadow_error = e
    elapsed = time.time() - t0

    if shadow_error is not None:
        diff = {"match": False, "mode": "error", "diff_error": repr(shadow_error)}
    else:
        diff = _diff(primary_result, shadow_result, fuzzy_match=fuzzy_match)

    diff_export_path: Optional[str] = None
    if (
        diff_export_uri
        and not diff.get("match")
        and shadow_error is None
        and shadow_result is not None
    ):
        diff_export_path = _export_diff_rows(
            context, primary_result, shadow_result, diff_export_uri, fuzzy_match,
        )

    _emit_shadow_observation(context, diff, elapsed, shadow_error, diff_export_path)

    if not diff.get("match"):
        try:
            context.log.warning(
                f"@shadow: MISMATCH (mode={diff.get('mode')}, "
                f"diff_rows={diff.get('shadow_diff_rows')}, "
                f"extra_cols={diff.get('shadow_extra_cols')}, "
                f"missing_cols={diff.get('shadow_missing_cols')})"
            )
            if diff_export_path:
                context.log.warning(f"@shadow: diff rows exported to {diff_export_path}")
        except Exception:  # noqa: BLE001
            pass
        if enforce_match:
            raise dg.Failure(
                description=f"@shadow enforce_match=True: primary/shadow disagreed (mode={diff.get('mode')})",
                metadata={
                    "shadow_diff_mode": dg.MetadataValue.text(str(diff.get("mode", ""))),
                    "shadow_match": dg.MetadataValue.bool(False),
                },
            )


def shadow(
    shadow_fn: Callable,
    *,
    enforce_match: bool = False,
    fuzzy_match: Optional[Dict[str, Any]] = None,
    diff_export_uri: Optional[str] = None,
) -> Callable:
    """Dual-run the wrapped compute + a shadow implementation, diff outputs.

    ```python
    @dg.asset
    @shadow(new_report_impl)              # observe-only
    def report(context, upstream):
        return old_report_impl(context, upstream)
    ```

    The primary result (return value of the wrapped fn) is what
    production sees. Shadow runs after primary; its result is diffed and
    the outcome is emitted as an `AssetObservation` tagged
    `shadow_match=true|false`. Shadow exceptions are trapped — they
    never fail the run unless `enforce_match=True`.

    Args:
        shadow_fn: Callable with the same signature as the wrapped
            compute. Any exception raised by shadow_fn is trapped.
        enforce_match: If True, mismatch raises `dg.Failure`. Off by
            default so shadow is safe to run in prod.
        fuzzy_match: Optional dict — DataFrame-diff config.
            `{float_tolerance: 1e-6, ignore_columns: [...],
            ignore_row_order: bool}`. Float cells within tolerance count
            as equal; ignored columns are dropped before compare;
            row-order-invariant sorts both by all columns first.
        diff_export_uri: Optional fsspec URI base (local path or
            `s3://`, `gs://`, `abfs://`). On mismatch, mismatched rows
            are written to
            `<uri>/<asset>/<UTC_ts>__<run_id>.parquet` and the path is
            emitted as observation metadata `shadow_diff_export_path`.
    """
    if not callable(shadow_fn):
        raise TypeError(f"@shadow requires a callable; got {type(shadow_fn).__name__}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@shadow requires a Dagster context.")

            primary_result = fn(*args, **kwargs)
            _run_shadow(
                context, shadow_fn, args, kwargs, primary_result, enforce_match,
                fuzzy_match=fuzzy_match, diff_export_uri=diff_export_uri,
            )
            return primary_result

        return _wrapped
    return _decorator


class ShadowAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@shadow`. Two authoring modes:

    1. **Define a new shadow-instrumented asset**: `asset_name` +
       `compute: {...}` (primary) + `shadow_compute: {...}` (shadow).

    2. **Wrap two DCC components**: `wraps: {type, attributes}` (primary
       component whose assets register) + `shadow_wraps: {type, attributes}`
       (shadow component whose compute runs alongside; assets NOT registered).
       Perfect for vendor swaps: `wraps: NewVendor { ... }, shadow_wraps: OldVendor { ... }`.

    `wraps:` and `compute:`/`asset_name` are mutually exclusive. `shadow_wraps:`
    is required in wraps mode; `shadow_compute:` is required in compute mode.
    """

    asset_name: Optional[str] = Field(default=None, description="Required when NOT using `wraps:`.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Primary compute: `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    shadow_compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Shadow compute: `{kind: python, python: 'mod:fn'}`. Same signature as primary. Required in `compute:` mode.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap a DCC component's assets with shadow instrumentation. "
            "Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. "
            "The outer shadow adds side-by-side execution of the `shadow_wraps:` component; "
            "primary result is what materializes."
        ),
    )
    shadow_wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Shadow component to run alongside `wraps:` — its result is diffed against the primary "
            "but NOT materialized. Required when using `wraps:`."
        ),
    )

    enforce_match: bool = Field(
        default=False,
        description="When True, mismatch between primary and shadow raises dg.Failure. Default off = observe only.",
    )
    fuzzy_match: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Fuzzy comparison config for DataFrame diffs. "
            "`{float_tolerance: 1e-6, ignore_columns: [col_a, col_b], ignore_row_order: true}`. "
            "Float cells within tolerance count as equal; ignored columns are dropped before "
            "compare; row-order-invariant sorts both by all columns first. Ignored on non-DataFrame outputs."
        ),
    )
    diff_export_uri: Optional[str] = Field(
        default=None,
        description=(
            "When mismatch is detected, write mismatched rows to this fsspec URI (local path "
            "or `s3://`, `gs://`, `abfs://`). Path shape: `<uri>/<asset>/<UTC_ts>__<run_id>.parquet`. "
            "Emitted as observation metadata `shadow_diff_export_path`. DataFrame outputs only."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'shadow'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Shadow Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-two-components vs builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("ShadowAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            if self.shadow_wraps is None:
                raise ValueError("ShadowAssetComponent: `wraps:` mode requires `shadow_wraps:` (the alt implementation to run alongside).")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("ShadowAssetComponent: supply either `compute` + `shadow_compute` OR `wraps` + `shadow_wraps`.")
        if self.shadow_compute is None:
            raise ValueError("ShadowAssetComponent: `compute:` mode requires `shadow_compute:`.")
        if not self.asset_name:
            raise ValueError("ShadowAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        primary = dict(self.compute)
        shadow_cfg = dict(self.shadow_compute)
        enforce = bool(self.enforce_match)
        fuzzy_cfg = dict(self.fuzzy_match) if self.fuzzy_match else None
        diff_export = self.diff_export_uri

        kinds_set = set(self.kinds or []) | {"python", "shadow"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Shadow-instrumented asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind_p = (primary.get("kind") or "python").lower()
            kind_s = (shadow_cfg.get("kind") or "python").lower()
            if kind_p != "python" or kind_s != "python":
                raise ValueError("ShadowAssetComponent supports compute.kind=python only")

            primary_fn = _load_callable(primary.get("python", ""))
            shadow_fn_ = _load_callable(shadow_cfg.get("python", ""))

            import inspect
            sig = inspect.signature(primary_fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                call_args, call_kwargs = (), {}
            elif n_positional == 1:
                call_args, call_kwargs = (context,), {}
            else:
                call_args, call_kwargs = (context, kwargs.get("upstream")), {}

            primary_result = primary_fn(*call_args, **call_kwargs)
            _run_shadow(
                context, shadow_fn_, call_args, call_kwargs, primary_result, enforce,
                fuzzy_match=fuzzy_cfg, diff_export_uri=diff_export,
            )

            return primary_result

        return dg.Definitions(assets=[_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability — outer shadow wraps two components side-by-side
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        primary_inner = _resolve_inner_component(self.wraps or {}, "wraps")
        shadow_inner = _resolve_inner_component(self.shadow_wraps or {}, "shadow_wraps")
        primary_defs = primary_inner.build_defs(context)
        shadow_defs = shadow_inner.build_defs(context)

        # Build a map of shadow asset key -> shadow's raw compute callable
        # so we can call it side-by-side per primary key.
        shadow_computes: Dict[Any, Any] = {}
        for a in list(shadow_defs.assets or []):
            for k in a.keys:
                inner_op = a.op
                shadow_computes[k] = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        # For each primary asset (single-key only), rebuild with shadow side-effect.
        wrapped_assets = []
        for asset_def in list(primary_defs.assets or []):
            if len(asset_def.keys) != 1:
                wrapped_assets.append(asset_def)
                continue
            primary_key = next(iter(asset_def.keys))
            shadow_compute = shadow_computes.get(primary_key)
            if shadow_compute is None:
                # No matching shadow asset — fall back to any single shadow asset
                if len(shadow_computes) == 1:
                    shadow_compute = next(iter(shadow_computes.values()))
                else:
                    context.log.warning(
                        f"ShadowAssetComponent.wraps: no shadow compute matching key "
                        f"{primary_key.to_user_string()!r} (found {len(shadow_computes)} shadow assets); "
                        f"skipping shadow for this asset"
                    )
                    wrapped_assets.append(asset_def)
                    continue
            wrapped_assets.append(self._wrap_single_asset(asset_def, shadow_compute))

        return dg.Definitions(
            assets=wrapped_assets,
            resources=primary_defs.resources,
            sensors=primary_defs.sensors,
            schedules=primary_defs.schedules,
            asset_checks=primary_defs.asset_checks,
            jobs=primary_defs.jobs,
            loggers=primary_defs.loggers,
        )

    def _wrap_single_asset(self, asset_def: "dg.AssetsDefinition", shadow_compute) -> "dg.AssetsDefinition":
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)
        inner_op = asset_def.op
        primary_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        enforce = bool(self.enforce_match)
        fuzzy_cfg = dict(self.fuzzy_match) if self.fuzzy_match else None
        diff_export = self.diff_export_uri

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"shadow"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Shadow-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [shadow: enforce_match={enforce}]"
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
        def _shadow_wrapped(context: dg.AssetExecutionContext, **kwargs):
            primary_result = primary_compute(context, **kwargs)
            _run_shadow(
                context, shadow_compute, (context,), dict(kwargs), primary_result, enforce,
                fuzzy_match=fuzzy_cfg, diff_export_uri=diff_export,
            )
            return primary_result

        return _shadow_wrapped


def _resolve_inner_component(wraps: Dict[str, Any], field_name: str):
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError(f"ShadowAssetComponent.{field_name} requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"ShadowAssetComponent.{field_name}: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"ShadowAssetComponent.{field_name}: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"ShadowAssetComponent.{field_name}: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
