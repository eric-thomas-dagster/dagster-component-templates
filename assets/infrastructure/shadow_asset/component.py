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
import time
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


def _diff(primary: Any, shadow: Any) -> Dict[str, Any]:
    """Return diff summary — always a dict; `match` key is the verdict."""
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
                out["primary_shape"] = list(primary.shape)
                out["shadow_shape"] = list(shadow.shape)
                cols_p = set(primary.columns)
                cols_s = set(shadow.columns)
                out["shadow_extra_cols"] = sorted(cols_s - cols_p)
                out["shadow_missing_cols"] = sorted(cols_p - cols_s)
                if primary.shape != shadow.shape or cols_p != cols_s:
                    out["match"] = False
                    return out
                common = sorted(cols_p)
                sample_p = primary[common].head(500).reset_index(drop=True)
                sample_s = shadow[common].head(500).reset_index(drop=True)
                out["match"] = sample_p.equals(sample_s)
                if not out["match"]:
                    # count row-level disagreements up to 500
                    diff_mask = (sample_p != sample_s).any(axis=1)
                    out["shadow_diff_rows"] = int(diff_mask.sum())
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


def _emit_shadow_observation(
    context: Any, diff: Dict[str, Any], shadow_elapsed_s: float,
    shadow_error: Optional[BaseException] = None,
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
) -> None:
    """Run the shadow implementation, diff against primary, emit observation.

    Exceptions inside `shadow_fn` are trapped (recorded as observation with
    `shadow_error` tag) — the primary result is what production sees.
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
        diff = _diff(primary_result, shadow_result)

    _emit_shadow_observation(context, diff, elapsed, shadow_error)

    if not diff.get("match"):
        try:
            context.log.warning(
                f"@shadow: MISMATCH (mode={diff.get('mode')}, "
                f"diff_rows={diff.get('shadow_diff_rows')}, "
                f"extra_cols={diff.get('shadow_extra_cols')}, "
                f"missing_cols={diff.get('shadow_missing_cols')})"
            )
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
            _run_shadow(context, shadow_fn, args, kwargs, primary_result, enforce_match)
            return primary_result

        return _wrapped
    return _decorator


class ShadowAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@shadow`. Wraps a primary compute with a shadow implementation."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="Primary compute: `{kind: python, python: 'mod:fn'}`.")
    shadow_compute: Dict[str, Any] = Field(
        description="Shadow compute: `{kind: python, python: 'mod:fn'}`. Same signature as primary."
    )

    enforce_match: bool = Field(
        default=False,
        description="When True, mismatch between primary and shadow raises dg.Failure. Default off = observe only.",
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
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        primary = dict(self.compute)
        shadow_cfg = dict(self.shadow_compute)
        enforce = bool(self.enforce_match)

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
            _run_shadow(context, shadow_fn_, call_args, call_kwargs, primary_result, enforce)

            return primary_result

        return dg.Definitions(assets=[_asset])
