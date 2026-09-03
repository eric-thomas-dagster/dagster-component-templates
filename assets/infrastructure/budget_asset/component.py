"""BudgetAssetComponent + `@budget` — per-asset $ cost tracking with cumulative windows.

Estimate the cost of each materialization + track a rolling window of
prior costs via the Dagster event log. On breach:
`warn` (observation only), `fail` (dg.Failure before compute — saves
the run), or `skip` (return without materializing).

## Why this belongs in Dagster

- **Cost history lives in the event log** — no side database.
- **AssetObservation with typed `cost_estimate_usd`** — sums, averages,
  and dashboards fall out of ordinary observation queries.
- **Pre-flight guard** — with `strict=fail`, we check cumulative BEFORE
  running compute so an over-budget run doesn't even start.
- **`cost_fn` callback** — for LLM/API costs where wall-clock doesn't
  correlate with $ (e.g., $ per output token), you supply a
  `mod:fn` that returns USD from `(context, elapsed_seconds, result)`.

## Two shapes

- **`BudgetAssetComponent`** (YAML)
- **`@budget` decorator** (Python)

## Cost estimation

Priority:

1. `cost_fn` — callable returning USD given `(context, elapsed_seconds, result)`.
   Called AFTER compute so `result` (return value) is available.
2. `cost_per_second` — wall-clock rate. Used when `cost_fn` is None.

## Cumulative window

`window_days` (default 30): sum `budget_cost_estimate_usd` metadata
from `ASSET_OBSERVATION` events tagged `budget_cost_asset=<asset_key>`
within the window. This is your rolling cost view.

## Enforcement modes

- `warn` (default) — always run; emit `budget_breach=true` observation
  when cumulative > budget.
- `fail` — pre-flight: if cumulative >= budget, raise `dg.Failure`
  BEFORE the compute runs. Also post-flight: refuse to emit if new
  materialization would breach (post-check).
- `skip` — pre-flight: if cumulative >= budget, return without
  materializing + emit `budget_skipped=true` observation.

## Composes with

- `@sla`, `@timeout` — SLA measures duration, `@timeout` kills long
  runs; `@budget` measures $. Overlap: an SLA breach usually implies a
  cost breach.
- `@throttle` — orthogonal: rate limit + budget together model
  "no more than N runs per hour AND no more than $M per month."
- `@dry_run` — dry runs still cost compute time; the observation lands.
"""

import functools
import importlib
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


_COST_TAG = "budget_cost_asset"


def _cumulative_cost_usd(context: Any, asset_key: dg.AssetKey, window_days: float) -> float:
    """Sum budget_cost_estimate_usd metadata from observations in the last window_days."""
    try:
        instance = getattr(context, "instance", None)
        if instance is None:
            return 0.0
        from dagster import EventRecordsFilter, DagsterEventType
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=asset_key,
            ),
            limit=1000,
            ascending=False,
        )
        cutoff = time.time() - window_days * 86400.0
        total = 0.0
        for r in records:
            ts = getattr(r, "timestamp", None)
            if ts is None or float(ts) < cutoff:
                continue
            obs = getattr(r, "asset_observation", None)
            if obs is None:
                continue
            md = getattr(obs, "metadata", None) or {}
            v = md.get("budget_cost_estimate_usd")
            if v is None:
                continue
            val = getattr(v, "value", v)
            try:
                total += float(val)
            except (TypeError, ValueError):
                pass
        return total
    except Exception:  # noqa: BLE001
        return 0.0


def _emit_observation(context: Any, tags: Dict[str, str], metadata: Dict[str, Any]) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["budget_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata=metadata,
            ))
    except Exception:  # noqa: BLE001
        pass


def _asset_key_str(context: Any) -> str:
    try:
        ak = getattr(context, "asset_key", None)
        if ak is not None:
            return ak.to_user_string()
    except Exception:  # noqa: BLE001
        pass
    return "budget_asset"


def _resolve_cost_fn(cost_fn: Any) -> Optional[Callable]:
    if cost_fn is None:
        return None
    if callable(cost_fn):
        return cost_fn
    if isinstance(cost_fn, str) and ":" in cost_fn:
        mod_path, fn_name = cost_fn.rsplit(":", 1)
        fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
        if callable(fn):
            return fn
    raise TypeError(f"@budget cost_fn must be callable or 'mod:fn' string; got {cost_fn!r}")


def _preflight(
    context: Any,
    asset_key: dg.AssetKey,
    budget_usd: Optional[float],
    on_breach: str,
    window_days: float,
) -> Optional[dg.MaterializeResult]:
    """Return a MaterializeResult if we should skip, or raise Failure. Otherwise None."""
    if budget_usd is None or budget_usd <= 0:
        return None
    cumulative = _cumulative_cost_usd(context, asset_key, window_days)
    key = _asset_key_str(context)
    if cumulative >= budget_usd:
        if on_breach == "fail":
            raise dg.Failure(
                description=f"@budget: window budget breached BEFORE run — "
                            f"cumulative ${cumulative:.4f} >= budget ${budget_usd:.4f} "
                            f"({window_days}-day window)",
                metadata={
                    "budget_cumulative_usd": dg.MetadataValue.float(float(round(cumulative, 6))),
                    "budget_usd": dg.MetadataValue.float(float(budget_usd)),
                    "budget_window_days": dg.MetadataValue.float(float(window_days)),
                    "budget_asset_key": dg.MetadataValue.text(key),
                },
            )
        if on_breach == "skip":
            _emit_observation(
                context,
                tags={_COST_TAG: key, "budget_skipped": "true"},
                metadata={
                    "budget_cumulative_usd": dg.MetadataValue.float(float(round(cumulative, 6))),
                    "budget_usd": dg.MetadataValue.float(float(budget_usd)),
                    "budget_window_days": dg.MetadataValue.float(float(window_days)),
                    "budget_skipped": dg.MetadataValue.bool(True),
                },
            )
            try:
                context.log.warning(
                    f"@budget: SKIP run — cumulative ${cumulative:.4f} >= budget ${budget_usd:.4f}"
                )
            except Exception:  # noqa: BLE001
                pass
            return dg.MaterializeResult(
                metadata={
                    "budget_skipped": dg.MetadataValue.bool(True),
                    "budget_cumulative_usd": dg.MetadataValue.float(float(round(cumulative, 6))),
                    "budget_usd": dg.MetadataValue.float(float(budget_usd)),
                }
            )
    return None


def _emit_cost_observation(
    context: Any,
    elapsed_s: float,
    cost_usd: float,
    budget_usd: Optional[float],
    window_days: float,
    breached: bool,
) -> float:
    """Emit AssetObservation with cost metadata. Return new cumulative (including this run)."""
    key = _asset_key_str(context)
    asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["budget_asset"])
    prior_cumulative = _cumulative_cost_usd(context, asset_key, window_days)
    new_cumulative = prior_cumulative + float(cost_usd)
    metadata: Dict[str, Any] = {
        "budget_cost_estimate_usd": dg.MetadataValue.float(float(round(cost_usd, 6))),
        "budget_elapsed_seconds": dg.MetadataValue.float(float(round(elapsed_s, 3))),
        "budget_cumulative_usd": dg.MetadataValue.float(float(round(new_cumulative, 6))),
        "budget_window_days": dg.MetadataValue.float(float(window_days)),
        "budget_asset_key": dg.MetadataValue.text(key),
    }
    if budget_usd is not None:
        metadata["budget_usd"] = dg.MetadataValue.float(float(budget_usd))
        metadata["budget_breached"] = dg.MetadataValue.bool(bool(breached))
    tags = {_COST_TAG: key}
    if breached:
        tags["budget_breach"] = "true"
    _emit_observation(context, tags, metadata)
    return new_cumulative


def budget(
    *,
    cost_per_second: Optional[float] = None,
    cost_fn: Optional[Callable] = None,
    budget_usd: Optional[float] = None,
    window_days: float = 30.0,
    on_breach: str = "warn",
) -> Callable:
    """Track $ cost per materialization + rolling window budget.

    ```python
    @dg.asset
    @budget(cost_per_second=0.02, budget_usd=100.0, window_days=30, on_breach="warn")
    def costly_pipeline(context):
        return build()

    def openai_cost(context, elapsed_s, result):
        return result.get("usage_tokens", 0) * 0.000002

    @dg.asset
    @budget(cost_fn=openai_cost, budget_usd=50.0, on_breach="fail")
    def llm_summarizer(context):
        return call_openai()
    ```

    Args:
        cost_per_second: Wall-clock rate (USD/sec). Used when cost_fn is None.
        cost_fn: Optional callable `(context, elapsed_s, result) -> usd`.
        budget_usd: Rolling window cap. None → observation-only, no breach.
        window_days: Rolling window size (days). Default 30.
        on_breach: `warn` (default), `fail`, or `skip`.
    """
    if on_breach not in ("warn", "fail", "skip"):
        raise ValueError(f"on_breach must be 'warn', 'fail', or 'skip'; got {on_breach!r}")
    if cost_per_second is None and cost_fn is None:
        raise ValueError("@budget requires cost_per_second OR cost_fn")

    resolved_cost_fn = _resolve_cost_fn(cost_fn) if cost_fn is not None else None

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@budget requires a Dagster context.")

            asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["budget_asset"])

            preflight = _preflight(context, asset_key, budget_usd, on_breach, window_days)
            if preflight is not None:
                return preflight

            t0 = time.time()
            result = fn(*args, **kwargs)
            elapsed = time.time() - t0

            if resolved_cost_fn is not None:
                try:
                    cost = float(resolved_cost_fn(context, elapsed, result))
                except Exception as e:  # noqa: BLE001
                    try:
                        context.log.warning(f"@budget: cost_fn raised — falling back to per-second rate: {e}")
                    except Exception:  # noqa: BLE001
                        pass
                    cost = elapsed * (cost_per_second or 0.0)
            else:
                cost = elapsed * (cost_per_second or 0.0)

            breached = budget_usd is not None and (cost >= budget_usd or
                _cumulative_cost_usd(context, asset_key, window_days) + cost >= budget_usd)
            _emit_cost_observation(context, elapsed, cost, budget_usd, window_days, breached)

            if breached and on_breach == "fail":
                raise dg.Failure(
                    description=f"@budget: this run pushes cumulative over budget "
                                f"(cost=${cost:.4f}, budget=${budget_usd:.4f})",
                    metadata={
                        "budget_cost_estimate_usd": dg.MetadataValue.float(float(round(cost, 6))),
                        "budget_usd": dg.MetadataValue.float(float(budget_usd)),
                        "budget_window_days": dg.MetadataValue.float(float(window_days)),
                    },
                )

            return result

        return _wrapped
    return _decorator


class BudgetAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@budget`. Wraps a compute with per-run + rolling-window cost tracking."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    cost_per_second: Optional[float] = Field(
        default=None,
        description="Wall-clock USD/sec rate. Used when cost_fn is null.",
    )
    cost_fn: Optional[str] = Field(
        default=None,
        description="Optional 'mod:fn' callable returning USD given (context, elapsed_s, result).",
    )
    budget_usd: Optional[float] = Field(
        default=None,
        description="Rolling window cap. Null → observation-only, no breach.",
    )
    window_days: float = Field(
        default=30.0,
        description="Rolling window (days) for cumulative cost sum.",
    )
    on_breach: str = Field(
        default="warn",
        description="'warn' (default): always run, emit budget_breach observation; "
                    "'fail': dg.Failure pre-flight if cumulative >= budget or post-flight if this run breaches; "
                    "'skip': return MaterializeResult(budget_skipped=true) pre-flight.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'budget', 'cost'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Budget Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        cost_per_s = self.cost_per_second
        cost_fn_str = self.cost_fn
        budget = self.budget_usd
        window_d = float(self.window_days)
        breach = self.on_breach

        if breach not in ("warn", "fail", "skip"):
            raise ValueError(f"on_breach must be 'warn', 'fail', or 'skip'; got {breach!r}")
        if cost_per_s is None and not cost_fn_str:
            raise ValueError("BudgetAssetComponent requires cost_per_second OR cost_fn")

        resolved_cost_fn = _resolve_cost_fn(cost_fn_str) if cost_fn_str else None

        kinds_set = set(self.kinds or []) | {"python", "budget", "cost"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Cost-tracked asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            asset_key = context.asset_key
            preflight = _preflight(context, asset_key, budget, breach, window_d)
            if preflight is not None:
                return preflight

            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"BudgetAssetComponent supports compute.kind=python only; got {kind!r}")
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

            t0 = time.time()
            if n_positional == 0:
                result = fn()
            elif n_positional == 1:
                result = fn(context)
            else:
                result = fn(context, kwargs.get("upstream"))
            elapsed = time.time() - t0

            if resolved_cost_fn is not None:
                try:
                    cost = float(resolved_cost_fn(context, elapsed, result))
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"@budget: cost_fn raised — falling back to per-second rate: {e}")
                    cost = elapsed * (cost_per_s or 0.0)
            else:
                cost = elapsed * (cost_per_s or 0.0)

            breached = budget is not None and (cost >= budget or
                _cumulative_cost_usd(context, asset_key, window_d) + cost >= budget)
            _emit_cost_observation(context, elapsed, cost, budget, window_d, breached)

            if breached and breach == "fail":
                raise dg.Failure(
                    description=f"@budget: this run pushes cumulative over budget "
                                f"(cost=${cost:.4f}, budget=${budget:.4f})",
                    metadata={
                        "budget_cost_estimate_usd": dg.MetadataValue.float(float(round(cost, 6))),
                        "budget_usd": dg.MetadataValue.float(float(budget)),
                    },
                )

            return dg.MaterializeResult(
                metadata={
                    "budget_cost_estimate_usd": dg.MetadataValue.float(float(round(cost, 6))),
                    "budget_elapsed_seconds": dg.MetadataValue.float(float(round(elapsed, 3))),
                    "budget_breached": dg.MetadataValue.bool(bool(breached)),
                }
            )

        return dg.Definitions(assets=[_asset])
