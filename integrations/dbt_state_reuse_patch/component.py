"""dbt State-Reuse + Microbatch Resilience Patch.

**Bridge component** — monkey-patches `dagster-dbt` at code-location load to
treat `no-op` (dbt Cloud state-reuse) and `partial success` (incremental
microbatch) statuses as materialization-yielding, matching the semantics of
`success`.

Upstream fix: dagster-io/dagster#34010. Remove this component from your
`defs.yaml` once you upgrade to the dagster-dbt release that includes the fix.

## Why it exists

dbt Cloud state-reuse (and incremental microbatch) can complete a model with
`status: "no-op"` or `status: "partial success"`. `dagster-dbt` today does a
strict `status == NodeStatus.Success` check when deciding whether to yield an
`AssetMaterialization` event. Non-matching statuses fall through — the OSS
`@dbt_assets` op then raises on missing required outputs, and the whole
Dagster run is marked failed even though dbt itself succeeded.

This component wraps two entry points:

  1. `DbtCliEventMessage._get_node_status` — the OSS CLI path. Returns
     `"success"` when the underlying status is `no-op` or `partial success`,
     so every downstream `==` comparison passes.
  2. `DbtCloudJobRunResults.to_default_asset_events` — the dbt Cloud v2 path.
     Rewrites `status: "no-op" | "partial success"` to `"success"` in the
     in-memory run_results dict before the original method sees it.

Both patches are idempotent. When the installed `dagster-dbt` has the upstream
fix (detected by presence of `_SUCCESS_EQUIVALENT_NODE_STATUSES` in either
module), this component's `build_defs` is a no-op — safe to leave in defs.yaml.

## Usage

```yaml
type: dagster_community_components.DbtStateReusePatchComponent
attributes: {}
```

That's it. On code-location load, patches apply globally to dagster-dbt for
the duration of the process.
"""
import copy
import logging
from dataclasses import dataclass
from typing import Any

import dagster as dg


logger = logging.getLogger(__name__)


_SUCCESS_EQUIVALENT_RAW_STATUSES = frozenset({"no-op", "partial success"})


def _apply_dbt_resilience_patches() -> dict[str, str]:
    """Idempotent monkey-patch. Returns a dict describing what was patched
    (or why it wasn't) for logging."""
    result: dict[str, str] = {}

    # ─── OSS CLI path ────────────────────────────────────────────────────
    try:
        from dagster_dbt.core import dbt_cli_event
    except ImportError:
        result["oss"] = "skipped: dagster_dbt not installed"
    else:
        if hasattr(dbt_cli_event, "_SUCCESS_EQUIVALENT_NODE_STATUSES"):
            result["oss"] = "skipped: upstream fix present"
        elif getattr(dbt_cli_event.DbtCliEventMessage, "_dcc_resilience_patched", False):
            result["oss"] = "skipped: already patched by this component"
        else:
            _original_get_node_status = dbt_cli_event.DbtCliEventMessage._get_node_status

            def _patched_get_node_status(self) -> str:
                status = _original_get_node_status(self)
                if status in _SUCCESS_EQUIVALENT_RAW_STATUSES:
                    return "success"
                return status

            dbt_cli_event.DbtCliEventMessage._get_node_status = _patched_get_node_status  # type: ignore[method-assign]
            dbt_cli_event.DbtCliEventMessage._dcc_resilience_patched = True  # type: ignore[attr-defined]
            result["oss"] = "patched: DbtCliEventMessage._get_node_status"

    # ─── Cloud v2 path ───────────────────────────────────────────────────
    try:
        from dagster_dbt.cloud_v2 import run_handler
    except ImportError:
        result["cloud_v2"] = "skipped: dagster_dbt.cloud_v2 not installed"
    else:
        if hasattr(run_handler, "_SUCCESS_EQUIVALENT_NODE_STATUSES"):
            result["cloud_v2"] = "skipped: upstream fix present"
        elif getattr(run_handler.DbtCloudJobRunResults, "_dcc_resilience_patched", False):
            result["cloud_v2"] = "skipped: already patched by this component"
        else:
            _original_to_events = run_handler.DbtCloudJobRunResults.to_default_asset_events

            def _patched_to_events(self, *args: Any, **kwargs: Any):
                # Rewrite success-equivalent statuses to plain "success" in
                # a fresh copy of run_results so the original method's
                # strict `== NodeStatus.Success` check passes.
                original_run_results = self.run_results
                rewritten = copy.deepcopy(dict(original_run_results))
                mutated = 0
                for r in rewritten.get("results", []):
                    if r.get("status") in _SUCCESS_EQUIVALENT_RAW_STATUSES:
                        r["status"] = "success"
                        mutated += 1

                if mutated:
                    object.__setattr__(self, "run_results", rewritten)
                    try:
                        yield from _original_to_events(self, *args, **kwargs)
                    finally:
                        object.__setattr__(self, "run_results", original_run_results)
                else:
                    yield from _original_to_events(self, *args, **kwargs)

            run_handler.DbtCloudJobRunResults.to_default_asset_events = _patched_to_events  # type: ignore[method-assign]
            run_handler.DbtCloudJobRunResults._dcc_resilience_patched = True  # type: ignore[attr-defined]
            result["cloud_v2"] = "patched: DbtCloudJobRunResults.to_default_asset_events"

    return result


@dataclass
class DbtStateReusePatchComponent(dg.Component, dg.Resolvable, dg.Model):
    """Applies dbt state-reuse + microbatch resilience patches to dagster-dbt.

    Bridge component until dagster-io/dagster#34010 merges + releases.
    Idempotent — safe to leave in `defs.yaml` after the upstream fix ships.

    Example:

        ```yaml
        type: dagster_community_components.DbtStateReusePatchComponent
        attributes: {}
        ```

    No configuration needed. Patches apply globally at code-location load.
    """

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        result = _apply_dbt_resilience_patches()
        # Log to the dagster logger so it surfaces in the code-location boot log.
        for path, status in result.items():
            logger.info(f"dbt_state_reuse_patch [{path}]: {status}")
        return dg.Definitions()
