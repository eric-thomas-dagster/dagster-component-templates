"""Enriched dbt Cloud Workspace Component.

Cloud sibling of ``EnrichedDbtProjectComponent`` — extends the official
``dagster-dbt`` ``DbtCloudComponent`` with:

- **Mid-run per-model monitor** (``monitor_runs=True``) — parses dbt Cloud
  debug logs during execution to yield per-model Output events as models
  complete, instead of waiting for the entire job to finish. Ports
  ``DbtCloudRunMonitor`` from the ``dbt-cloud-mesh-demo`` repo (which itself
  mirrors the ``et/dbt-cloud-monitor-runs`` PR).
- **Job selection DSL** (``job_selection_include`` / ``job_selection_exclude``)
  — filter which dbt Cloud jobs get mirrored using dbt-style selectors
  (``type:deploy``, ``*_prod``, ``id:12345``, …). Ports
  ``et/dbt-cloud-mirror-jobs-selection`` PR branch.
- **Same enrichment fields** as ``EnrichedDbtProjectComponent`` for
  metadata surfacing (docs URL, exposures, contracts, freshness, …).
- **Real dbt test results as per-model `AssetCheckEvaluation`s**, for
  ``mirror_jobs``'s trigger op specifically — a dbt test node is mapped to
  its parent model via the manifest's ``attached_node``/``depends_on``,
  not surfaced as its own materialization. A failed test is reported as a
  failed check (alertable via Dagster+'s native "Asset" alert policy) but
  does not itself fail the op -- the overall dbt Cloud run's own status
  (checked earlier) already reflects whatever dbt Cloud's job settings
  consider run-blocking. (The separate enhanced polling sensor, for the
  primary `select`/`exclude` asset path, does not yet do this -- see
  Roadmap.)

Companion component: ``EnrichedDbtProjectComponent`` for dbt Core projects.
Both live in the same category (``dbt``) with the same enrichment
vocabulary so switching between Core and Cloud is a config-level change.

``DbtCloudRunMonitor`` and the job-selection DSL are vendored directly in
this file (DCC rule: no cross-component shared modules, and no sibling
files within a component's own folder either — everything lives in
``component.py``).

## Fields

Cloud-specific (in addition to the base ``DbtCloudComponent`` fields):

- ``monitor_runs``  (default False) — enable mid-run monitor
- ``fail_fast``     (default False) — cancel on first failure (only with monitor_runs)
- ``poll_interval`` (default 5.0)   — seconds between debug-log polls
- ``job_selection_include`` — selection string, jobs matching are mirrored (default: all)
- ``job_selection_exclude`` — selection string, jobs matching are dropped after include

Enrichment (same vocabulary as ``EnrichedDbtProjectComponent``):

- ``dbt_docs_url``, ``include_exposures``, ``include_metrics``,
  ``include_semantic_models``, ``include_contracts``, ``include_meta``,
  ``include_source_freshness``, ``include_doc_blocks``
- ``emit_exposures_as_assets``, ``derive_freshness_policies``,
  ``emit_contract_checks``, ``external_packages``, ``asset_overrides``

Automation condition precedence (per-model ``meta.dagster.automation_condition``
always wins; this previously only existed on the Core sibling — the base
``DbtCloudComponent`` translator only reads the older, narrower
``meta.dagster.auto_materialize_policy: {type: eager|lazy}`` shape):

1. per-model ``meta.dagster.automation_condition`` (dbt YAML, always wins)
2. ``auto_trigger_on_freshness_failure`` → ``freshness_failed()``
3. ``derive_lag_tolerance_automation`` → lag-tolerance-derived condition
4. ``default_automation_condition`` — component-level fallback, same shape as
   #1, applied only when nothing above set one. Lets a team change its
   default automation policy via YAML instead of patching this component.

``external_packages`` stub keys are computed via this component's own
configured translator (``self.get_asset_spec``), not a bare model-name
guess — see README for why this matters for dbt-mesh lineage.
``asset_overrides`` is keyed by either the serialized AssetKey string or the
dbt ``unique_id``.

## Roadmap

**Phase 3+ (queued):**

- Semantic layer AssetSpecs (``emit_semantic_layer_as_assets``)
- Sensor emits AssetCheckEvaluations for dbt test results
  (``et/dbt-cloud-sensor-check-evaluations`` PR)
- Mesh-aware sensor filtering (``et/dbt-cloud-sensor-mesh-aware`` PR)
- ``code_version_strategy: hash | sqlglot | disabled``
- Skip-reason metadata surfaced on materialization events
- ``dbt state explain`` output as per-model metadata
- ``lag_tolerance`` on freshness derivation
"""
import fnmatch
import json
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Mapping, Optional, Union

import dagster as dg
from pydantic import Field

_UNIQUE_ID_KEY = "dagster_dbt/unique_id"
_CONTRACT_ENFORCED_KEY = "dagster_dbt/contract_enforced"
_COLUMN_CONSTRAINTS_KEY = "dagster_dbt/column_constraints"
_MODEL_CONSTRAINTS_KEY = "dagster_dbt/model_constraints"
_EXPOSURE_TYPE_KEY = "dagster_dbt/exposure_type"
_EXPOSURE_URL_KEY = "dagster_dbt/exposure_url"
_EXPOSURE_MATURITY_KEY = "dagster_dbt/exposure_maturity"
_EXTERNAL_PACKAGE_KEY = "dagster_dbt/external_package"
_SEMANTIC_MEASURES_KEY = "dagster_dbt/measures"
_SEMANTIC_DIMENSIONS_KEY = "dagster_dbt/dimensions"
_SEMANTIC_ENTITIES_KEY = "dagster_dbt/entities"
_METRIC_TYPE_KEY = "dagster_dbt/metric_type"
_METRIC_LABEL_KEY = "dagster_dbt/metric_label"

_DBT_EXPOSURE_TYPE_TO_KIND: Mapping[str, str] = {
    "dashboard": "dashboard",
    "notebook": "notebook",
    "analysis": "analysis",
    "ml": "ml",
    "application": "application",
}

_DBT_FRESHNESS_PERIOD_TO_TIMEDELTA_KWARG: Mapping[str, str] = {
    "minute": "minutes",
    "hour": "hours",
    "day": "days",
}


# ─── Vendored freshness derivation ─────────────────────────────────────
# Ported from `et/dbt-source-freshness-policies` + `et/dbt-model-freshness-
# automation-condition`. Kept per-component (DCC rule: no shared modules
# across component packages).


def _dbt_freshness_spec_to_timedelta(
    freshness_spec: Optional[Mapping[str, Any]],
) -> Optional[timedelta]:
    if not freshness_spec:
        return None
    count = freshness_spec.get("count")
    period = freshness_spec.get("period")
    if count is None or period is None:
        return None
    kwarg = _DBT_FRESHNESS_PERIOD_TO_TIMEDELTA_KWARG.get(period)
    if kwarg is None:
        return None
    return timedelta(**{kwarg: count})


def _freshness_policy_from_meta_dagster(
    meta_freshness_policy: Any,
) -> Optional[dg.FreshnessPolicy]:
    if not isinstance(meta_freshness_policy, Mapping):
        return None
    policy_type = meta_freshness_policy.get("type")
    if policy_type == "time_window":
        fail_window_seconds = meta_freshness_policy.get("fail_window_seconds")
        if not isinstance(fail_window_seconds, (int, float)):
            return None
        warn_window_seconds = meta_freshness_policy.get("warn_window_seconds")
        warn_window = (
            timedelta(seconds=warn_window_seconds)
            if isinstance(warn_window_seconds, (int, float))
            else None
        )
        return dg.FreshnessPolicy.time_window(
            fail_window=timedelta(seconds=fail_window_seconds),
            warn_window=warn_window,
        )
    if policy_type == "cron":
        deadline_cron = meta_freshness_policy.get("deadline_cron")
        lower_bound_delta_seconds = meta_freshness_policy.get("lower_bound_delta_seconds")
        if not isinstance(deadline_cron, str) or not isinstance(
            lower_bound_delta_seconds, (int, float)
        ):
            return None
        timezone = meta_freshness_policy.get("timezone", "UTC")
        if not isinstance(timezone, str):
            return None
        return dg.FreshnessPolicy.cron(
            deadline_cron=deadline_cron,
            lower_bound_delta=timedelta(seconds=lower_bound_delta_seconds),
            timezone=timezone,
        )
    return None


def _parse_duration_string(s: Any) -> Optional[timedelta]:
    """Parse dbt-style duration ``"4h"`` / ``"45m"`` / ``"7d"`` into a timedelta.
    Used for ``config.state.lag_tolerance`` (real dbt State feature, ~2.0+)."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return timedelta(seconds=float(s))
    if not isinstance(s, str):
        return None
    s = s.strip().lower()
    if not s:
        return None
    unit_map = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    unit = s[-1]
    if unit not in unit_map:
        try:
            return timedelta(seconds=float(s))
        except ValueError:
            return None
    try:
        value = float(s[:-1])
    except ValueError:
        return None
    return timedelta(**{unit_map[unit]: value})


def _load_state_manifest(state_manifest_path: str) -> Optional[dict]:
    from pathlib import Path as _P
    p = _P(state_manifest_path)
    if p.is_dir():
        p = p / "manifest.json"
    try:
        return json.loads(p.read_text())
    except (FileNotFoundError, PermissionError, json.JSONDecodeError):
        return None


def _state_explain_for_node(
    dbt_resource_props: Mapping[str, Any],
    state_manifest: Optional[dict],
) -> Optional[dict]:
    """Compare per-model checksum vs state manifest. Returns
    {state: new|unchanged|modified, explanation: str}. See Core enriched
    component for full docs."""
    if not state_manifest:
        return None
    if dbt_resource_props.get("resource_type") != "model":
        return None
    unique_id = dbt_resource_props.get("unique_id")
    if not unique_id:
        return None
    state_nodes = state_manifest.get("nodes") or {}
    state_node = state_nodes.get(unique_id)
    current_checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
    if state_node is None:
        return {
            "state": "new",
            "explanation": f"Model '{unique_id}' does not exist in the state manifest — will rebuild.",
        }
    state_checksum = (state_node.get("checksum") or {}).get("checksum")
    if current_checksum == state_checksum:
        return {
            "state": "unchanged",
            "explanation": (
                f"Model '{unique_id}' checksum matches state — dbt will reuse the "
                "state's build (no-op) unless a deeper state selector detects a change."
            ),
        }
    return {
        "state": "modified",
        "explanation": (
            f"Model '{unique_id}' checksum differs from state — will rebuild. "
            f"(state: {(state_checksum or '')[:8]}, current: {(current_checksum or '')[:8]})"
        ),
    }


def _lag_tolerance_of(dbt_resource_props: Mapping[str, Any]) -> Optional[timedelta]:
    lag = ((dbt_resource_props.get("config") or {}).get("state") or {}).get("lag_tolerance")
    return _parse_duration_string(lag)


_CRON_DIVISORS_MIN: list = [
    (1, "* * * * *"), (2, "*/2 * * * *"), (5, "*/5 * * * *"),
    (10, "*/10 * * * *"), (15, "*/15 * * * *"), (20, "*/20 * * * *"),
    (30, "*/30 * * * *"),
]
_CRON_DIVISORS_HOUR: list = [
    (1, "0 * * * *"), (2, "0 */2 * * *"), (3, "0 */3 * * *"),
    (4, "0 */4 * * *"), (6, "0 */6 * * *"), (8, "0 */8 * * *"),
    (12, "0 */12 * * *"),
]


def _lag_tolerance_to_cron(lag: timedelta) -> Optional[str]:
    """Snap lag_tolerance to the largest cron divisor <= the delay."""
    total_seconds = lag.total_seconds()
    if total_seconds < 60:
        return "* * * * *"
    if total_seconds < 3600:
        minutes = int(total_seconds // 60)
        best = _CRON_DIVISORS_MIN[0]
        for div, cron in _CRON_DIVISORS_MIN:
            if div <= minutes:
                best = (div, cron)
        return best[1]
    if total_seconds < 86400:
        hours = int(total_seconds // 3600)
        best = _CRON_DIVISORS_HOUR[0]
        for div, cron in _CRON_DIVISORS_HOUR:
            if div <= hours:
                best = (div, cron)
        return best[1]
    days = int(total_seconds // 86400)
    if days == 1:
        return "0 0 * * *"
    if days < 7:
        return f"0 0 */{days} * *"
    return "0 0 * * 0"


def _dbt_dialect_from_adapter(adapter_type: Optional[str]) -> Optional[str]:
    if not adapter_type:
        return None
    _MAP = {
        "snowflake": "snowflake", "postgres": "postgres", "redshift": "redshift",
        "bigquery": "bigquery", "duckdb": "duckdb", "databricks": "databricks",
        "spark": "spark", "sparksql": "spark", "mysql": "mysql",
        "trino": "trino", "presto": "presto", "clickhouse": "clickhouse",
        "athena": "athena", "sqlite": "sqlite", "oracle": "oracle",
    }
    return _MAP.get(adapter_type.lower())


def _derive_code_version(
    dbt_resource_props: Mapping[str, Any],
    strategy: str,
    adapter_type: Optional[str] = None,
) -> Optional[str]:
    """Derive Dagster code_version for a dbt model. See Core enriched
    component for full docs. ``disabled`` / ``hash`` / ``sqlglot`` — sqlglot
    canonicalizes SQL before hashing so whitespace/comment changes don't bump."""
    if strategy == "disabled":
        return None
    if dbt_resource_props.get("resource_type") != "model":
        return None
    if strategy == "hash":
        checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
        return str(checksum) if checksum else None
    if strategy == "sqlglot":
        compiled = (
            dbt_resource_props.get("compiled_code")
            or dbt_resource_props.get("compiled_sql")
            or dbt_resource_props.get("raw_code")
            or dbt_resource_props.get("raw_sql")
        )
        if not compiled:
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None
        try:
            import hashlib
            import sqlglot
            dialect = _dbt_dialect_from_adapter(adapter_type)
            tree = sqlglot.parse_one(compiled, read=dialect) if dialect else sqlglot.parse_one(compiled)
            canonical = tree.sql(pretty=False, comments=False, dialect=dialect)
            return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        except ImportError:
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None
        except Exception:
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None
    return None


def _lag_tolerance_automation_condition(lag: timedelta) -> Optional[Any]:
    """Compose AutomationCondition that fires ~lag_tolerance after upstream
    is newly-updated (same pattern as Core enriched component)."""
    cron = _lag_tolerance_to_cron(lag)
    if cron is None:
        return None
    try:
        return (
            dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated().since(
                    dg.AutomationCondition.cron_tick_passed(cron)
                )
                & ~dg.AutomationCondition.executed_with_root_target()
            ).newly_true()
            & ~dg.AutomationCondition.in_progress()
            & dg.AutomationCondition.in_latest_time_window()
        )
    except Exception:
        try:
            return dg.AutomationCondition.eager() & ~dg.AutomationCondition.in_progress()
        except Exception:
            return None


def _automation_condition_from_meta(meta: Mapping[str, Any]) -> Optional[Any]:
    """Convert ``meta.dagster.automation_condition`` dict into an AutomationCondition.

    Same shape/behavior as the Core enriched component's helper of the same
    name (ported here — this component previously had NO per-model
    automation_condition override at all, unlike its Core sibling; the base
    DbtCloudComponent's own translator only reads the older, narrower
    ``meta.dagster.auto_materialize_policy: {type: eager|lazy}`` shape).

    Supported shapes:
      - ``{preset: eager | on_missing | any_downstream_conditions}``
      - ``{preset: on_deploy_if_code_changed}``  (synthetic composite)
      - ``{cron: "0 9 * * *"}``
    """
    if not meta or not isinstance(meta, Mapping):
        return None
    preset = meta.get("preset")
    if preset:
        if preset == "on_deploy_if_code_changed":
            return (
                dg.AutomationCondition.code_version_changed().since_last_handled()
                & ~dg.AutomationCondition.in_progress()
            )
        method = getattr(dg.AutomationCondition, preset, None)
        if method is None or not callable(method):
            return None
        try:
            result = method()
        except Exception:
            return None
        return result if isinstance(result, dg.AutomationCondition) else None
    cron = meta.get("cron")
    if cron:
        try:
            return dg.AutomationCondition.on_cron(cron)
        except Exception:
            return None
    return None


def _derive_freshness_policy(
    dbt_resource_props: Mapping[str, Any],
) -> Optional[dg.FreshnessPolicy]:
    meta_dagster = (dbt_resource_props.get("meta") or {}).get("dagster") or {}
    meta_policy = _freshness_policy_from_meta_dagster(meta_dagster.get("freshness_policy"))
    if meta_policy is not None:
        return meta_policy
    resource_type = dbt_resource_props.get("resource_type")
    lag_tolerance = _lag_tolerance_of(dbt_resource_props) if resource_type == "model" else None
    if resource_type == "source":
        freshness_config = dbt_resource_props.get("freshness") or {}
        error_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("error_after"))
        warn_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("warn_after"))
        if error_after is None:
            return None
        if warn_after is not None and warn_after >= error_after:
            warn_after = None
        return dg.FreshnessPolicy.time_window(fail_window=error_after, warn_window=warn_after)
    if resource_type == "model":
        freshness_config = (dbt_resource_props.get("config") or {}).get("freshness") or {}
        build_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("build_after"))
        # Take max(build_after, lag_tolerance) — either alone gives fail_window;
        # if both set, effective SLA is whichever dbt waits longer for.
        effective = build_after or lag_tolerance
        if effective is None:
            return None
        if build_after is not None and lag_tolerance is not None and lag_tolerance > build_after:
            effective = lag_tolerance
        return dg.FreshnessPolicy.time_window(fail_window=effective)
    return None


def _derive_contract_metadata(
    dbt_resource_props: Mapping[str, Any],
) -> Dict[str, Any]:
    if dbt_resource_props.get("resource_type") != "model":
        return {}
    contract = (dbt_resource_props.get("config") or {}).get("contract") or {}
    if not contract.get("enforced"):
        return {}
    column_constraints: Dict[str, List[str]] = {}
    for column_name, column_info in (dbt_resource_props.get("columns") or {}).items():
        constraint_types = [
            constraint.get("type")
            for constraint in (column_info.get("constraints") or [])
            if constraint.get("type")
        ]
        if constraint_types:
            column_constraints[column_name] = constraint_types
    model_constraints = list(dbt_resource_props.get("constraints") or [])
    return {
        _CONTRACT_ENFORCED_KEY: True,
        _COLUMN_CONSTRAINTS_KEY: dg.MetadataValue.json(column_constraints),
        _MODEL_CONSTRAINTS_KEY: dg.MetadataValue.json(model_constraints),
    }


def _get_str_meta(metadata: dict, key: str) -> Optional[str]:
    val = metadata.get(key)
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if hasattr(val, "value"):
        return str(val.value)
    if hasattr(val, "text"):
        return val.text
    return str(val)


def _build_child_map(manifest: Mapping[str, Any]) -> Dict[str, List[str]]:
    """Manifests produced by dbt v2 / Fusion don't include a top-level
    `child_map` key at all (confirmed against dagster_dbt's own internal
    `_build_child_map`, which hit the identical gap and works around it the
    same way) -- build the parent -> children edges ourselves from each
    resource's own `depends_on.nodes` list whenever the manifest doesn't
    supply one. Without this, include_exposures/include_metrics/
    include_semantic_models silently return nothing under a Fusion manifest
    -- `manifest.get("child_map", {})` never raises, it just degrades."""
    existing = manifest.get("child_map")
    if existing:
        return existing
    child_map: Dict[str, List[str]] = {}
    for resources in (
        manifest.get("nodes") or {},
        manifest.get("sources") or {},
        manifest.get("exposures") or {},
        manifest.get("metrics") or {},
        manifest.get("semantic_models") or {},
    ):
        for unique_id, node in resources.items():
            for upstream_unique_id in (node.get("depends_on") or {}).get("nodes", []) or []:
                child_map.setdefault(upstream_unique_id, []).append(unique_id)
    return child_map


@dataclass
class AssetOverride(dg.Resolvable):
    depends_on: Optional[List[str]] = None


def _resolve_override_deps(
    asset_overrides: Optional[Dict[str, "AssetOverride"]],
    lookup_key: str,
    unique_id: Optional[str] = None,
) -> List[dg.AssetKey]:
    """Look up an override by the asset's serialized AssetKey first (existing
    behavior), falling back to its dbt `unique_id` if provided and present.
    The unique_id form (e.g. `model.shared_core.customer_summary`) is easier
    to get right than a hand-computed, translation-scheme-dependent AssetKey
    string, especially across the two dbt-mesh projects' code locations."""
    if not asset_overrides:
        return []
    ov = asset_overrides.get(lookup_key)
    if ov is None and unique_id:
        ov = asset_overrides.get(unique_id)
    if not ov or not ov.depends_on:
        return []
    return [dg.AssetKey(d.split("/")) if "/" in d else dg.AssetKey(d) for d in ov.depends_on]


# ─── Vendored mirror-jobs helpers ─────────────────────────────────────
# Ported from `et/dbt-cloud-mirror-jobs` PR branch. Delete when it merges +
# releases; then import `_build_dbt_cloud_job_asset_specs` +
# `_build_mirrored_dbt_cloud_job` from dagster_dbt.cloud_v2.component.

_DAGSTER_ADHOC_PREFIX = "DAGSTER_ADHOC_JOB__"
_DBT_CLOUD_JOB_ID_METADATA_KEY = "dbt_cloud/job_id"
_DBT_CLOUD_JOB_NAME_METADATA_KEY = "dbt_cloud/job_name"


def _sanitize_job_name(name: str) -> str:
    """dbt Cloud job names → Dagster op-safe identifiers (alnum + underscore)."""
    import re
    out = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    if not out or not (out[0].isalpha() or out[0] == "_"):
        out = f"job_{out}" if out else "job"
    return out


def _list_dbt_cloud_jobs_via_client(workspace: Any) -> list[dict]:
    """Best-effort list of dbt Cloud jobs.

    Uses ``workspace.client.list_jobs()`` if available, else falls back to a
    raw REST call via ``requests``. Returns each job as a plain dict with the
    fields the mirror-jobs builders care about (id, name, job_type).
    """
    client = getattr(workspace, "client", None) or workspace
    # Preferred: public method on the workspace/client
    for attr in ("list_jobs", "get_jobs", "jobs"):
        fn = getattr(client, attr, None)
        if fn is None:
            continue
        try:
            result = fn() if callable(fn) else fn
            # Normalize to list-of-dicts
            if isinstance(result, list):
                return [j if isinstance(j, dict) else j.__dict__ for j in result]
            if hasattr(result, "jobs"):
                return [j if isinstance(j, dict) else j.__dict__ for j in result.jobs]
        except Exception:
            continue
    # Fallback: direct REST call
    import requests as req
    account_id = (
        getattr(client, "account_id", None)
        or getattr(workspace, "account_id", None)
    )
    api_url = getattr(client, "api_v2_url", None) or getattr(client, "api_url", None)
    token = getattr(client, "token", None) or getattr(client, "api_token", None)
    if not (account_id and api_url and token):
        return []
    try:
        resp = req.get(
            f"{api_url}/accounts/{account_id}/jobs/",
            headers={"Authorization": f"Token {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("data") or []
    except Exception:
        return []


def _iter_mirrorable_cloud_jobs(jobs: list[dict]):
    """Yield (id, name, job_type) tuples for user-defined Cloud jobs, filtering
    out Dagster's internal DAGSTER_ADHOC_JOB__* pool."""
    for j in jobs:
        name = (j.get("name") or "")
        if name.startswith(_DAGSTER_ADHOC_PREFIX):
            continue
        yield j.get("id"), name, j.get("job_type")


# ─── Vendored job selection DSL ────────────────────────────────────────
# Ported from `et/dbt-cloud-mirror-jobs-selection` PR branch. Delete when
# the PR merges + releases; then import
# `dagster_dbt.cloud_v2.job_selection.matches_selection` / `apply_selection`
# directly. Kept per-component (DCC rule: no shared modules across component
# packages).
#
# A selection string is a space-separated list of selectors; a job matches
# if ANY selector matches (union/OR semantics):
#
# - `type:<value>` — matches `job.job_type` exactly (`type:ci`, `type:deploy`, …)
# - `name:<glob>`   — fnmatch glob against `job.name` (case-sensitive)
# - `id:<int>`      — exact `job.id` match
# - `<glob>`        — bare token = shorthand for `name:<glob>`
# - `*` or empty    — matches every job
#
# Include defaults to "everything", exclude defaults to "nothing." Exclude
# runs after include.


def _match_single_selector(cloud_job: Any, selector: str) -> bool:
    """Match a single selector token against one Cloud job."""
    selector = selector.strip()
    if not selector or selector == "*":
        return True

    if ":" in selector:
        kind, _, value = selector.partition(":")
        kind = kind.strip()
        value = value.strip()
        if kind == "type":
            return (getattr(cloud_job, "job_type", None) or "") == value
        if kind == "name":
            return fnmatch.fnmatchcase(getattr(cloud_job, "name", None) or "", value)
        if kind == "id":
            try:
                return getattr(cloud_job, "id", None) == int(value)
            except ValueError:
                return False
        # Unknown selector kind: no match (safer than silently matching all).
        return False

    # Bare token = name glob shorthand.
    return fnmatch.fnmatchcase(getattr(cloud_job, "name", None) or "", selector)


def matches_selection(cloud_job: Any, selection: Optional[str]) -> bool:
    """Match ``cloud_job`` against a whole selection string.

    Returns True if ``cloud_job`` matches ANY selector in the space-separated
    ``selection`` string. ``None`` or empty string means "match everything."
    """
    if not selection or not selection.strip():
        return True
    tokens = selection.split()
    return any(_match_single_selector(cloud_job, tok) for tok in tokens)


def apply_selection(
    cloud_jobs: Iterable[Any],
    include: Optional[str],
    exclude: Optional[str],
) -> list:
    """Filter ``cloud_jobs`` by ``include`` then ``exclude`` selection strings.

    - ``include=None`` or empty: include every job.
    - ``exclude=None`` or empty: exclude nothing.
    - Exclude wins: a job matching both include and exclude is dropped.
    """
    result: list = []
    for cloud_job in cloud_jobs:
        if not matches_selection(cloud_job, include):
            continue
        if exclude and matches_selection(cloud_job, exclude):
            continue
        result.append(cloud_job)
    return result


try:
    from dagster_dbt import DbtCloudComponent as _DbtCloudComponent
    from dagster_dbt.cloud_v2.client import DbtCloudJobRunStatusType
    from dagster_dbt.cloud_v2.run_handler import DbtCloudJobRunResults
    from dagster_dbt.cloud_v2.types import DbtCloudRun
    from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

    # ─── Vendored mid-run per-model monitor ────────────────────────────
    # Ported from `eric-thomas-dagster/dbt-cloud-mesh-demo` (which itself
    # mirrors the upstream `et/dbt-cloud-monitor-runs` PR to
    # dagster-io/dagster). Delete when the PR merges + releases; then import
    # `dagster_dbt.cloud_v2.run_monitor.DbtCloudRunMonitor` directly. Kept
    # per-component (DCC rule: no shared modules across component packages).
    #
    # Streams per-model Dagster events (Output, AssetCheckResult,
    # AssetMaterialization) as individual models complete during the dbt
    # Cloud run — like dbt Core's `.stream()` but for dbt Cloud. Enables
    # mid-run alerting via Dagster+ instead of waiting for the entire job
    # to finish.
    #
    # Architecture:
    # 1. Poll dbt Cloud every `poll_interval` seconds
    # 2. Fetch debug logs from the steps API for per-model results
    # 3. Map log results (`SCHEMA.model_name`) to Dagster asset keys via manifest
    # 4. Yield `Output` events immediately — Dagster processes them (triggers
    #    alerts, updates UI) then resumes the generator
    # 5. On completion, yield remaining events from `run_results.json` for
    #    anything not already yielded (tests, missed models)

    _RUN_MONITOR_TERMINAL_STATUSES = {
        DbtCloudJobRunStatusType.SUCCESS,
        DbtCloudJobRunStatusType.ERROR,
        DbtCloudJobRunStatusType.CANCELLED,
    }
    _RUN_MONITOR_FAILURE_STATUSES = {"error", "fail"}

    # Strip ANSI color codes before parsing debug logs
    _RUN_MONITOR_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")

    # Model results in dbt logs: "N of M OK created ... SCHEMA.model_name"
    _RUN_MONITOR_MODEL_RESULT_PATTERN = re.compile(
        r"(\d+)\s+of\s+(\d+)\s+"
        r"(OK|ERROR)\s+"
        r"(?:created|creating)\s+"
        r"(?:sql\s+)?(?:table|view|incremental)\s+"
        r"(?:model|snapshot|seed)\s+"
        r"(\S+\.\S+)",
    )

    # Test results: "N of M PASS test_name"
    _RUN_MONITOR_TEST_RESULT_PATTERN = re.compile(
        r"(\d+)\s+of\s+(\d+)\s+"
        r"(PASS|FAIL|WARN|SKIP)\s+"
        r"(?:\d+\s+)?"
        r"(\S+)",
    )

    # Explicit error patterns
    _RUN_MONITOR_RUNTIME_ERROR_PATTERN = re.compile(r"Runtime Error in model (\S+)", re.IGNORECASE)
    _RUN_MONITOR_COMPILATION_ERROR_PATTERN = re.compile(
        r"Compilation Error in (?:model|test|snapshot|seed) (\S+)", re.IGNORECASE
    )
    _RUN_MONITOR_DATABASE_ERROR_PATTERN = re.compile(r"Database Error in model (\S+)", re.IGNORECASE)

    @dataclass
    class DbtCloudRunMonitor:
        """Streams per-model Dagster events from a dbt Cloud run mid-execution.

        Args:
            client: The dbt Cloud workspace client.
            run_id: The dbt Cloud run ID to monitor.
            poll_interval: Seconds between polls. Default 5.
            fail_fast: Cancel the dbt Cloud run on first model failure.
        """

        client: Any  # DbtCloudWorkspaceClient
        run_id: int
        poll_interval: float = 5.0
        fail_fast: bool = False
        _seen_log_nodes: set = field(default_factory=set)
        _yielded_output_names: set = field(default_factory=set)
        _failures: list = field(default_factory=list)

        def stream(
            self,
            context: dg.AssetExecutionContext,
            manifest: Mapping[str, Any],
            dagster_dbt_translator: Any = None,  # DagsterDbtTranslator | None
            timeout: Optional[float] = None,
        ) -> Iterator[Union[dg.AssetCheckEvaluation, dg.AssetCheckResult, dg.AssetMaterialization, dg.Output]]:
            """Stream Dagster events as dbt Cloud models complete.

            Yields Output events mid-run from debug log parsing, then yields
            remaining events (tests, missed models) from run_results.json on
            completion. Raises Failure if any models errored.
            """
            translator = dagster_dbt_translator or DagsterDbtTranslator()
            start_time = time.time()
            poll_count = 0
            log_parsing_available = False

            # Build lookup: "SCHEMA.model_name" (lowered) → unique_id
            log_name_to_unique_id = self._build_log_name_lookup(manifest)

            # Track whether the dbt Cloud run reached a terminal state; if not
            # and the generator is closed (Dagster run cancelled), the finally
            # block cancels the dbt Cloud run so it doesn't keep consuming compute.
            run_completed = False
            try:
                while True:
                    poll_count += 1
                    if timeout and (time.time() - start_time) > timeout:
                        raise TimeoutError(f"dbt Cloud run {self.run_id} timed out after {timeout}s")

                    # 1. Get run status
                    run_details = self._get_run_details_safe(context)
                    run = DbtCloudRun.from_run_details(run_details)
                    status_name = run.status.name if run.status else "UNKNOWN"
                    context.log.info(f"Run {self.run_id} status: {status_name}")

                    if poll_count == 1:
                        run_url = run.url or run_details.get("href", "")
                        if run_url:
                            context.log.info(f"dbt Cloud run URL: {run_url}")

                    # 2. Try to activate debug log parsing
                    if not log_parsing_available:
                        run_steps = run_details.get("run_steps", [])
                        if run_steps:
                            latest_step = max(run_steps, key=lambda s: s.get("index", 0))
                            test_logs = self._fetch_step_debug_logs(latest_step["id"])
                            if test_logs:
                                log_parsing_available = True
                                context.log.info("Per-model log streaming active.")

                    # 3. Parse debug logs and YIELD events for completed models
                    if log_parsing_available:
                        yield from self._stream_from_debug_logs(
                            run_details, context, manifest, translator,
                            log_name_to_unique_id,
                        )

                    # 4. Fail fast?
                    if self.fail_fast and self._failures:
                        failed = ", ".join(self._failures)
                        context.log.error(
                            f"Fail-fast triggered. Cancelling dbt Cloud run {self.run_id}. "
                            f"Failed: {failed}"
                        )
                        self.cancel_run(context)
                        run_completed = True
                        yield from self._stream_remaining_from_artifacts(
                            context, manifest, translator
                        )
                        raise dg.Failure(
                            f"dbt Cloud run '{self.run_id}' cancelled — failures: {failed}",
                            metadata={"run_id": dg.MetadataValue.int(self.run_id)},
                        )

                    # 5. Terminal state?
                    if run.status in _RUN_MONITOR_TERMINAL_STATUSES:
                        run_completed = True
                        yield from self._stream_remaining_from_artifacts(
                            context, manifest, translator
                        )

                        if run.status in {DbtCloudJobRunStatusType.ERROR, DbtCloudJobRunStatusType.CANCELLED}:
                            failed = ", ".join(self._failures) if self._failures else "see dbt Cloud logs"
                            raise dg.Failure(
                                f"dbt Cloud run '{self.run_id}' finished with {status_name}. "
                                f"Failures: {failed}",
                                metadata={"run_id": dg.MetadataValue.int(self.run_id)},
                            )
                        return

                    time.sleep(self.poll_interval)
            finally:
                if not run_completed:
                    context.log.warning(
                        f"Dagster run terminated. Cancelling dbt Cloud run {self.run_id}."
                    )
                    self.cancel_run(context)

        def _stream_from_debug_logs(
            self,
            run_details: dict,
            context: dg.AssetExecutionContext,
            manifest: Mapping[str, Any],
            translator: Any,
            log_name_to_unique_id: dict,
        ) -> Iterator[dg.Output]:
            """Parse debug logs and yield Output events for completed models."""
            try:
                log_text = self._get_logs_from_steps(run_details)
                if not log_text:
                    return
                log_text = _RUN_MONITOR_ANSI_ESCAPE.sub("", log_text)
            except Exception:
                return

            # Parse model results
            for match in _RUN_MONITOR_MODEL_RESULT_PATTERN.finditer(log_text):
                _seq, _total, status_str, log_name = match.groups()
                if log_name in self._seen_log_nodes:
                    continue
                self._seen_log_nodes.add(log_name)

                is_error = status_str.upper() == "ERROR"
                if is_error:
                    context.log.error(f"MODEL FAILED (logs): {log_name}")
                    self._failures.append(log_name)
                    continue

                unique_id = log_name_to_unique_id.get(log_name.lower())
                if not unique_id:
                    context.log.info(f"Model OK (logs): {log_name} (no manifest mapping, will yield from artifacts)")
                    continue

                node = manifest.get("nodes", {}).get(unique_id)
                if not node:
                    continue

                asset_key = translator.get_asset_key(node)
                output_name = asset_key.to_python_identifier()

                if output_name in self._yielded_output_names:
                    continue
                self._yielded_output_names.add(output_name)

                context.log.info(f"Model OK (streaming): {log_name} → {asset_key}")
                yield dg.Output(
                    value=None,
                    output_name=output_name,
                    metadata={
                        "unique_id": unique_id,
                        "streamed_from": "debug_logs",
                    },
                )

            # Parse test results (log only; tests yielded from artifacts with full metadata)
            for match in _RUN_MONITOR_TEST_RESULT_PATTERN.finditer(log_text):
                _seq, _total, status_str, test_name = match.groups()
                if "." in test_name:
                    continue
                if test_name in self._seen_log_nodes:
                    continue
                self._seen_log_nodes.add(test_name)

                if status_str.upper() == "FAIL":
                    context.log.error(f"TEST FAILED (logs): {test_name}")
                    self._failures.append(test_name)
                elif status_str.upper() == "WARN":
                    context.log.warning(f"Test warning (logs): {test_name}")
                else:
                    context.log.info(f"Test {status_str.lower()} (logs): {test_name}")

            # Catch explicit error patterns
            for pattern in (
                _RUN_MONITOR_RUNTIME_ERROR_PATTERN,
                _RUN_MONITOR_COMPILATION_ERROR_PATTERN,
                _RUN_MONITOR_DATABASE_ERROR_PATTERN,
            ):
                for match in pattern.finditer(log_text):
                    node_name = match.group(1)
                    if node_name in self._seen_log_nodes:
                        continue
                    self._seen_log_nodes.add(node_name)
                    context.log.error(f"ERROR (logs): {match.group(0).strip()}")
                    self._failures.append(node_name)

        def _stream_remaining_from_artifacts(
            self,
            context: dg.AssetExecutionContext,
            manifest: Mapping[str, Any],
            translator: Any,
        ) -> Iterator[Union[dg.AssetCheckEvaluation, dg.AssetCheckResult, dg.AssetMaterialization, dg.Output]]:
            """Yield events from run_results.json for anything not already streamed."""
            try:
                run_results_json = self.client.get_run_results_json(self.run_id)
            except Exception:
                context.log.warning("run_results.json not available — no remaining events to yield")
                return

            run_results = DbtCloudJobRunResults.from_run_results_json(run_results_json)

            for event in run_results.to_default_asset_events(
                client=self.client,
                manifest=manifest,
                dagster_dbt_translator=translator,
                context=context,
            ):
                if isinstance(event, dg.Output) and event.output_name in self._yielded_output_names:
                    continue
                if isinstance(event, dg.Output):
                    self._yielded_output_names.add(event.output_name)
                    unique_id = (event.metadata or {}).get("unique_id", "")
                    context.log.info(f"OK (artifacts): {unique_id}")
                yield event

            # Track failures from run_results for error reporting
            for result in run_results_json.get("results", []):
                status = result.get("status", "")
                if status in _RUN_MONITOR_FAILURE_STATUSES:
                    unique_id = result.get("unique_id", "")
                    if unique_id not in self._failures:
                        self._failures.append(unique_id)
                        context.log.error(
                            f"FAILED (artifacts): {unique_id} — {result.get('message', '')}"
                        )

        def _build_log_name_lookup(self, manifest: Mapping[str, Any]) -> dict:
            """Map ``schema.name`` (lowered) → unique_id for model/snapshot/seed nodes."""
            lookup: dict = {}
            for unique_id, node in manifest.get("nodes", {}).items():
                if node.get("resource_type") not in ("model", "snapshot", "seed"):
                    continue
                schema = node.get("schema", "")
                name = node.get("alias") or node.get("name", "")
                if schema and name:
                    key = f"{schema}.{name}".lower()
                    lookup[key] = unique_id
            return lookup

        def _dbt_cloud_get(self, endpoint: str, params: Optional[dict] = None) -> dict:
            """GET request to dbt Cloud API using requests directly."""
            import requests as req
            url = f"{self.client.api_v2_url}/{endpoint}"
            resp = req.get(
                url,
                headers={
                    "Authorization": f"Token {self.client.token}",
                    "Content-Type": "application/json",
                },
                params=params,
                timeout=self.client.request_timeout,
            )
            resp.raise_for_status()
            return resp.json()

        def _get_run_details_safe(self, context: dg.AssetExecutionContext) -> dict:
            """Fetch run details with run_steps included."""
            try:
                return self._dbt_cloud_get(
                    f"runs/{self.run_id}",
                    params={"include_related": '["run_steps"]'},
                )["data"]
            except Exception:
                try:
                    return self.client.get_run_details(self.run_id)
                except Exception as e:
                    context.log.warning(f"Failed to fetch run details: {e}")
                    raise

        def _fetch_step_debug_logs(self, step_id: int) -> str:
            """Fetch debug logs for a specific run step."""
            try:
                data = self._dbt_cloud_get(
                    f"steps/{step_id}",
                    params={"include_related": '["debug_logs"]'},
                )
                step_data = data.get("data", {})
                return step_data.get("debug_logs", "") or step_data.get("logs", "") or ""
            except Exception:
                return ""

        def _get_logs_from_steps(self, run_details: dict) -> str:
            """Fetch and concatenate debug logs from all run steps."""
            run_steps = run_details.get("run_steps", [])
            if not run_steps:
                return ""
            parts: list = []
            for step in sorted(run_steps, key=lambda s: s.get("index", 0)):
                step_id = step.get("id")
                if step_id:
                    logs = self._fetch_step_debug_logs(step_id)
                    if logs:
                        parts.append(logs)
            return "\n".join(parts)

        def cancel_run(self, context: dg.AssetExecutionContext) -> None:
            """Cancel the dbt Cloud run."""
            import requests as req
            try:
                url = f"{self.client.api_v2_url}/runs/{self.run_id}/cancel/"
                resp = req.post(
                    url,
                    headers={
                        "Authorization": f"Token {self.client.token}",
                        "Content-Type": "application/json",
                    },
                    timeout=self.client.request_timeout,
                )
                resp.raise_for_status()
                context.log.warning(f"Cancelled dbt Cloud run {self.run_id}")
            except Exception as e:
                context.log.error(f"Failed to cancel run {self.run_id}: {e}")

    class EnrichedDbtCloudWorkspaceComponent(_DbtCloudComponent):
        """Enriched drop-in for ``DbtCloudComponent``. See module docstring."""

        # ── Cloud-specific: mid-run monitor + selection DSL ────────────
        monitor_runs: bool = False
        """Enable mid-run per-model monitoring. When True, Dagster parses dbt
        Cloud debug logs during execution to detect and log individual model
        successes and failures as they happen — instead of waiting for the
        entire job to finish."""

        fail_fast: bool = False
        """Only applies when monitor_runs is True. When True, the dbt Cloud
        run is cancelled on the first model failure and the Dagster run fails
        immediately. When False, failures are logged in real time but the run
        continues so all failures are captured in a single run."""

        poll_interval: float = 5.0
        """Seconds between debug log polls when monitor_runs is enabled.
        Lower values catch failures faster but make more API calls."""

        mirror_jobs: Literal["off", "asset", "job", "both"] = "off"
        """How to surface each dbt Cloud job in Dagster (ports
        ``et/dbt-cloud-mirror-jobs`` PR):

        - ``off`` — Do not mirror (backward-compatible default)
        - ``asset`` — Emit an observable AssetSpec per Cloud job (kind
          ``dbt_cloud_job``). Downstream AutomationConditions can react
          when the job runs. Materializations flow through the polling
          sensor.
        - ``job`` — Emit a Dagster @job per Cloud job that triggers +
          waits for the Cloud run. Users can schedule, launch from the
          UI, or wire ``@run_status_sensor`` downstream.
        - ``both`` — Emit both an AssetSpec AND a launchable @job.
        """

        job_trigger_defaults: Optional[Dict[str, Any]] = None
        """Default trigger overrides sent by every mirrored @job (applies
        with ``mirror_jobs`` = ``job`` or ``both``). The real dbt Cloud
        Cloud v2 client's ``trigger_job_run(job_id, steps_override=None)``
        only accepts ``steps_override`` -- confirmed against the installed
        client directly -- so ``{"steps_override": [...]}`` is the only key
        this actually affects. A per-run ``steps_override`` set via the
        Dagster UI launchpad or `dg launch --config` overrides this default
        for that run only, with no YAML edit."""

        job_selection_include: Optional[str] = None
        """Selection string; jobs matching any selector are mirrored. Default
        None = mirror everything. Selectors: `type:deploy`, `*_prod`,
        `id:12345`, or bare glob = name-glob shorthand."""

        job_selection_exclude: Optional[str] = None
        """Selection string; jobs matching are dropped AFTER include. Default
        None = drop nothing."""

        # ── Enrichment (same vocabulary as EnrichedDbtProjectComponent) ─
        dbt_docs_url: Optional[str] = None
        include_exposures: bool = False
        include_metrics: bool = False
        include_semantic_models: bool = False
        include_contracts: bool = False
        include_meta: bool = False
        include_source_freshness: bool = False
        include_doc_blocks: bool = False
        manifest_path: Optional[str] = None
        asset_overrides: Optional[Dict[str, AssetOverride]] = None

        emit_exposures_as_assets: bool = False
        emit_source_assets: bool = False
        """Emit each dbt source as an observable external AssetSpec (kinds
        `dbt`, `source`). Sources become first-class Dagster assets."""
        derive_freshness_policies: bool = False
        emit_contract_checks: bool = False
        external_packages: Optional[List[str]] = None
        emit_semantic_layer_as_assets: bool = False
        """Emit dbt semantic_models + metrics as observable AssetSpecs (kinds
        `semantic_model` / `metric`). Ports `et/dbt-semantic-layer-assets`."""
        enable_materialization_kinds: bool = False
        """Add each model's dbt `materialized` value as a Dagster kind (table /
        view / incremental / etc.). Ports part of
        `et/dbt-polish-kinds-explorer-desc`."""
        auto_trigger_on_freshness_failure: bool = False
        """With `derive_freshness_policies`: also attach
        `AutomationCondition.freshness_failed()` so Dagster triggers the
        rebuild when the derived FreshnessPolicy fails."""

        derive_lag_tolerance_automation: bool = False
        """For models with `config.state.lag_tolerance`: attach an
        AutomationCondition that fires ~lag_tolerance after upstream is
        newly-updated. Uses .newly_updated().since(cron_tick_passed(cron))
        pattern; cron is snapped from lag_tolerance (30m → */30 * * * *,
        4h → 0 */4 * * *, etc). Skipped if a user-supplied automation is
        set. auto_trigger_on_freshness_failure wins when both apply."""

        default_automation_condition: Optional[Dict[str, Any]] = None
        """Fallback AutomationCondition applied when nothing else set one —
        after per-model `meta.dagster.automation_condition`, after
        `auto_trigger_on_freshness_failure`, after
        `derive_lag_tolerance_automation`. Same shape as
        `meta.dagster.automation_condition` (`{preset: eager}`,
        `{preset: on_deploy_if_code_changed}`, or `{cron: "0 9 * * *"}`),
        parsed by the same helper. Lets a team declare its own default
        automation policy once, via YAML, instead of annotating every model's
        `meta.dagster.*` or patching this component."""

        code_version_strategy: Literal["disabled", "hash", "sqlglot"] = "disabled"
        """How to derive Dagster ``code_version`` per model:
        - ``disabled`` (default) — no code_version.
        - ``hash`` — use dbt's manifest checksum (bumps on any file edit).
        - ``sqlglot`` — parse compiled_code with sqlglot, canonicalize
          (strip comments + normalize whitespace), SHA256 the result.
          Whitespace/comment edits do NOT bump; only semantic SQL changes
          do. Optional dep, falls back to ``hash`` if sqlglot is missing."""

        emit_test_check_evaluations: bool = False
        """Replace the base polling sensor with one that emits
        AssetCheckEvaluation events for every dbt model result — passed for
        success, failed for error/fail. This makes failed models show a
        degraded check tile in the Dagster UI for EXTERNALLY-triggered dbt
        Cloud runs (Cloud schedule, Cloud UI, external cron). The default
        base sensor only emits AssetMaterialization; test/check outcomes are
        dropped."""

        emit_skip_reason_observations: bool = False
        """Requires enhanced polling sensor (auto-enabled when this is set).
        For every skipped model in a Cloud run's ``run_results.json``, emit
        an AssetObservation with the ``skip_reason`` (from dbt's ``message``
        field) as metadata. Shows up in the Dagster UI as an informational
        observation on the asset — engineers see WHY the model was skipped
        (parent failed, state-reuse, microbatch batch skipped, selector
        excluded, etc.) without opening dbt Cloud."""

        filter_external_packages_from_sensor: bool = False
        """Requires enhanced polling sensor. Skip sensor events for models
        whose ``package_name`` is in ``external_packages`` — the upstream
        Dagster code location owns those assets. Prevents duplicate
        materializations when both projects' sensors see the same run."""

        state_manifest_path: Optional[str] = None
        """Path to a dbt state manifest.json (or a directory containing one).
        Enables the checksum-comparison enrichments below."""

        include_state_explain: bool = False
        """Requires ``state_manifest_path``. Attach ``dbt_state/state`` +
        ``dbt_state/explanation`` metadata to each model — the checksum
        comparison result (new / unchanged / modified) with a human-readable
        reason. Equivalent to running ``dbt state explain`` at build time."""

        derive_state_tags: bool = False
        """Requires ``state_manifest_path``. Add a ``dbt/state`` tag with the
        modified / unchanged / new value so ``tag:dbt/state=modified``
        selections work."""

        # ─────────────────────────────────────────────────────────────
        # Internal helpers
        # ─────────────────────────────────────────────────────────────

        def _resolve_manifest(self) -> Optional[dict]:
            """Best-effort load of the dbt manifest.

            Priority:
              1. ``manifest_path`` override (an explicit file path)
              2. ``self.workspace.get_manifest()`` if the base exposes one
              3. Any ``manifest_json`` / ``_manifest`` attribute on the workspace

            Returns None if none of the above yield a manifest — enrichments
            that need it silently no-op.
            """
            if self.manifest_path:
                try:
                    return json.loads(Path(self.manifest_path).read_text())
                except (FileNotFoundError, PermissionError, json.JSONDecodeError):
                    pass
            for attr in ("get_manifest", "manifest_json", "_manifest"):
                obj = getattr(self.workspace, attr, None)
                if obj is None:
                    continue
                try:
                    return obj() if callable(obj) else obj
                except Exception:
                    continue
            return None

        def _enrich_spec(self, spec: dg.AssetSpec, manifest: dict) -> dg.AssetSpec:
            """Attach metadata + real policies to a single AssetSpec. Same shape
            as the Core component's ``_enrich_spec``."""
            unique_id = _get_str_meta(dict(spec.metadata), _UNIQUE_ID_KEY)
            if not unique_id:
                return spec
            all_nodes: dict = {
                **manifest.get("nodes", {}),
                **manifest.get("sources", {}),
                **manifest.get("snapshots", {}),
            }
            node = all_nodes.get(unique_id)
            if not node:
                return spec

            extra: dict[str, dg.MetadataValue] = {}
            resource_type: str = node.get("resource_type", "model")
            # Cached at the instance level -- _enrich_spec runs once per
            # asset, and this is O(all nodes) to build when the manifest
            # doesn't supply its own child_map (dbt v2 / Fusion).
            if not hasattr(self, "_cached_child_map"):
                self._cached_child_map = _build_child_map(manifest)  # type: ignore[attr-defined]
            child_ids: list[str] = self._cached_child_map.get(unique_id, [])  # type: ignore[attr-defined]

            if self.dbt_docs_url:
                url = f"{self.dbt_docs_url}/#!/{resource_type}/{unique_id}"
                extra["dbt_docs/url"] = dg.MetadataValue.url(url)

            if self.include_exposures:
                exposure_ids = [c for c in child_ids if c.startswith("exposure.")]
                if exposure_ids:
                    exposures = []
                    for eid in exposure_ids:
                        exp = manifest.get("exposures", {}).get(eid, {})
                        entry: dict = {
                            "name": exp.get("name"),
                            "type": exp.get("type"),
                            "description": exp.get("description"),
                            "maturity": exp.get("maturity"),
                        }
                        owner = exp.get("owner", {})
                        if owner:
                            entry["owner"] = owner.get("email") or owner.get("name")
                        if exp.get("url"):
                            entry["url"] = exp["url"]
                        if exp.get("label"):
                            entry["label"] = exp["label"]
                        exposures.append(entry)
                    extra["dbt_docs/exposures"] = dg.MetadataValue.json(exposures)

            if self.include_metrics:
                metric_ids = [c for c in child_ids if c.startswith("metric.")]
                if metric_ids:
                    metrics = []
                    for mid in metric_ids:
                        m = manifest.get("metrics", {}).get(mid, {})
                        metrics.append({
                            "name": m.get("name"),
                            "label": m.get("label"),
                            "type": m.get("type"),
                            "description": m.get("description"),
                            "time_granularity": m.get("time_granularity"),
                        })
                    extra["dbt_docs/metrics"] = dg.MetadataValue.json(metrics)

            if self.include_semantic_models:
                sm_ids = [c for c in child_ids if c.startswith("semantic_model.")]
                if sm_ids:
                    sms = []
                    for smid in sm_ids:
                        sm = manifest.get("semantic_models", {}).get(smid, {})
                        sms.append({
                            "name": sm.get("name"),
                            "label": sm.get("label"),
                            "description": sm.get("description"),
                            "primary_entity": sm.get("primary_entity"),
                            "measures": [x.get("name") for x in sm.get("measures", [])],
                            "dimensions": [x.get("name") for x in sm.get("dimensions", [])],
                            "entities": [x.get("name") for x in sm.get("entities", [])],
                        })
                    extra["dbt_docs/semantic_models"] = dg.MetadataValue.json(sms)

            if self.include_contracts:
                extra.update(_derive_contract_metadata(node))

            if self.include_meta:
                meta = node.get("meta", {})
                non_dagster = {k: v for k, v in meta.items() if k != "dagster"}
                if non_dagster:
                    extra["dbt_docs/meta"] = dg.MetadataValue.json(non_dagster)

            if self.include_source_freshness and resource_type == "source":
                freshness = node.get("freshness")
                if freshness and any(freshness.get(k) for k in ["warn_after", "error_after"]):
                    extra["dbt_docs/freshness"] = dg.MetadataValue.json(freshness)
                loaded_at = node.get("loaded_at_field")
                if loaded_at:
                    extra["dbt_docs/loaded_at_field"] = dg.MetadataValue.text(loaded_at)
                loader = node.get("loader")
                if loader:
                    extra["dbt_docs/loader"] = dg.MetadataValue.text(loader)

            access = node.get("config", {}).get("access")
            if access and access != "protected":
                extra["dbt_docs/access"] = dg.MetadataValue.text(access)

            language = node.get("language")
            if language and language != "sql":
                extra["dbt_docs/language"] = dg.MetadataValue.text(language)

            patch_path = node.get("patch_path")
            if patch_path:
                display = patch_path.split("://")[-1] if "://" in patch_path else patch_path
                extra["dbt_docs/patch_path"] = dg.MetadataValue.text(display)

            if self.include_doc_blocks:
                doc_block_names = node.get("doc_blocks", [])
                if doc_block_names:
                    docs_lookup = manifest.get("docs", {})
                    resolved_blocks: dict[str, str] = {}
                    for block_name in doc_block_names:
                        for _uid, doc_node in docs_lookup.items():
                            if doc_node.get("name") == block_name:
                                resolved_blocks[block_name] = doc_node.get("block_contents", "")
                                break
                    if resolved_blocks:
                        extra["dbt_docs/doc_blocks"] = dg.MetadataValue.json(resolved_blocks)

            # meta.dagster.automation_condition — per-model override, always
            # wins. The base DbtCloudComponent's translator doesn't read this
            # key at all (only the older, narrower auto_materialize_policy
            # shape), so without this the only escape hatch from
            # auto_trigger_on_freshness_failure / derive_lag_tolerance_automation
            # was patching this component directly.
            dagster_meta = node.get("meta", {}).get("dagster", {}) or {}
            per_model_automation = _automation_condition_from_meta(
                dagster_meta.get("automation_condition") or {}
            )

            enriched = spec
            if extra:
                enriched = enriched.merge_attributes(metadata=extra)
            if per_model_automation is not None:
                enriched = enriched.replace_attributes(automation_condition=per_model_automation)

            if self.enable_materialization_kinds:
                mat = (node.get("config") or {}).get("materialized")
                if mat and isinstance(mat, str):
                    try:
                        existing_kinds = set(enriched.kinds or ())
                        existing_kinds.add(mat)
                        enriched = enriched.merge_attributes(kinds=existing_kinds)
                    except Exception:
                        pass

            if self.derive_freshness_policies:
                policy = _derive_freshness_policy(node)
                if policy is not None:
                    enriched = enriched.replace_attributes(freshness_policy=policy)
                    if self.auto_trigger_on_freshness_failure and enriched.automation_condition is None:
                        try:
                            enriched = enriched.replace_attributes(
                                automation_condition=dg.AutomationCondition.freshness_failed()
                            )
                        except Exception:
                            pass

            # State comparison — attach per-model state explain metadata + tag
            if (self.include_state_explain or self.derive_state_tags) and self.state_manifest_path:
                if not hasattr(self, "_cached_state_manifest"):
                    self._cached_state_manifest = _load_state_manifest(self.state_manifest_path)  # type: ignore[attr-defined]
                state_result = _state_explain_for_node(
                    {**node, "unique_id": unique_id}, self._cached_state_manifest  # type: ignore[attr-defined]
                )
                if state_result is not None:
                    if self.include_state_explain:
                        try:
                            enriched = enriched.merge_attributes(
                                metadata={
                                    "dbt_state/state": dg.MetadataValue.text(state_result["state"]),
                                    "dbt_state/explanation": dg.MetadataValue.text(state_result["explanation"]),
                                }
                            )
                        except Exception:
                            pass
                    if self.derive_state_tags:
                        try:
                            enriched = enriched.merge_attributes(
                                tags={"dbt/state": state_result["state"]}
                            )
                        except Exception:
                            pass

            # code_version derivation (hash | sqlglot)
            if self.code_version_strategy != "disabled":
                adapter_type = (manifest.get("metadata") or {}).get("adapter_type")
                code_version = _derive_code_version(
                    node, self.code_version_strategy, adapter_type=adapter_type
                )
                if code_version:
                    try:
                        enriched = enriched.replace_attributes(code_version=code_version)
                    except Exception:
                        pass

            if self.derive_lag_tolerance_automation and enriched.automation_condition is None:
                lag = _lag_tolerance_of(node)
                if lag is not None:
                    cond = _lag_tolerance_automation_condition(lag)
                    if cond is not None:
                        try:
                            enriched = enriched.replace_attributes(automation_condition=cond)
                        except Exception:
                            pass

            # default_automation_condition → last-resort fallback. Applied
            # only when NOTHING above has set one (per-model meta, freshness,
            # lag_tolerance all win). Same shape/parser as
            # meta.dagster.automation_condition, just declared once at the
            # component level instead of per-model.
            if self.default_automation_condition and enriched.automation_condition is None:
                cond = _automation_condition_from_meta(self.default_automation_condition)
                if cond is not None:
                    try:
                        enriched = enriched.replace_attributes(automation_condition=cond)
                    except Exception:
                        pass

            return enriched

        def _build_exposure_specs(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetSpec]:
            """Emit AssetSpec per exposure with deps on referenced upstream models."""
            specs: List[dg.AssetSpec] = []
            for exposure_unique_id, exposure_props in (manifest.get("exposures") or {}).items():
                exposure_type = str(exposure_props.get("type") or "").lower()
                kind = _DBT_EXPOSURE_TYPE_TO_KIND.get(exposure_type)
                kinds = {kind} if kind else None

                deps: List[dg.AssetDep] = []
                seen: set[dg.AssetKey] = set()
                for upstream_id in (exposure_props.get("depends_on") or {}).get("nodes", []) or []:
                    upstream_key = base_specs_by_unique_id.get(upstream_id)
                    if upstream_key is None or upstream_key in seen:
                        continue
                    seen.add(upstream_key)
                    deps.append(dg.AssetDep(asset=upstream_key))

                owner_config = exposure_props.get("owner") or {}
                owner_email = owner_config.get("email")
                owners = [owner_email] if isinstance(owner_email, str) and owner_email else None

                tags = {
                    tag: ""
                    for tag in exposure_props.get("tags") or []
                    if isinstance(tag, str)
                }

                metadata: Dict[str, Any] = {
                    _UNIQUE_ID_KEY: exposure_unique_id,
                    _EXPOSURE_TYPE_KEY: exposure_type or "",
                }
                url = exposure_props.get("url")
                if isinstance(url, str) and url:
                    metadata[_EXPOSURE_URL_KEY] = url
                maturity = exposure_props.get("maturity")
                if isinstance(maturity, str) and maturity:
                    metadata[_EXPOSURE_MATURITY_KEY] = maturity

                name = exposure_props.get("name") or exposure_unique_id.split(".")[-1]
                specs.append(
                    dg.AssetSpec(
                        key=dg.AssetKey(name),
                        deps=deps,
                        description=exposure_props.get("description"),
                        metadata=metadata,
                        owners=owners,
                        tags=tags,
                        kinds=kinds,
                    )
                )
            return specs

        def _build_semantic_layer_specs(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetSpec]:
            """Emit AssetSpec per dbt semantic_model + metric."""
            specs: List[dg.AssetSpec] = []
            for sm_uid, sm in (manifest.get("semantic_models") or {}).items():
                deps: List[dg.AssetDep] = []
                seen: set[dg.AssetKey] = set()
                for up_uid in (sm.get("depends_on") or {}).get("nodes", []) or []:
                    k = base_specs_by_unique_id.get(up_uid)
                    if k is None or k in seen:
                        continue
                    seen.add(k)
                    deps.append(dg.AssetDep(asset=k))
                name = sm.get("name") or sm_uid.split(".")[-1]
                specs.append(
                    dg.AssetSpec(
                        key=dg.AssetKey(name),
                        deps=deps,
                        description=sm.get("description"),
                        kinds={"semantic_model"},
                        metadata={
                            _UNIQUE_ID_KEY: sm_uid,
                            _SEMANTIC_MEASURES_KEY: dg.MetadataValue.json(
                                [m.get("name") for m in sm.get("measures", [])]
                            ),
                            _SEMANTIC_DIMENSIONS_KEY: dg.MetadataValue.json(
                                [d.get("name") for d in sm.get("dimensions", [])]
                            ),
                            _SEMANTIC_ENTITIES_KEY: dg.MetadataValue.json(
                                [e.get("name") for e in sm.get("entities", [])]
                            ),
                        },
                    )
                )
            sm_key_by_uid: Dict[str, dg.AssetKey] = {}
            for sm_uid, sm in (manifest.get("semantic_models") or {}).items():
                name = sm.get("name") or sm_uid.split(".")[-1]
                sm_key_by_uid[sm_uid] = dg.AssetKey(name)
            for m_uid, m in (manifest.get("metrics") or {}).items():
                deps = []
                seen = set()
                for up_uid in (m.get("depends_on") or {}).get("nodes", []) or []:
                    k = sm_key_by_uid.get(up_uid) or base_specs_by_unique_id.get(up_uid)
                    if k is None or k in seen:
                        continue
                    seen.add(k)
                    deps.append(dg.AssetDep(asset=k))
                name = m.get("name") or m_uid.split(".")[-1]
                specs.append(
                    dg.AssetSpec(
                        key=dg.AssetKey(name),
                        deps=deps,
                        description=m.get("description"),
                        kinds={"metric"},
                        metadata={
                            _UNIQUE_ID_KEY: m_uid,
                            _METRIC_TYPE_KEY: str(m.get("type") or ""),
                            _METRIC_LABEL_KEY: str(m.get("label") or ""),
                        },
                    )
                )
            return specs

        def _build_source_specs(self, manifest: dict) -> List[dg.AssetSpec]:
            """Emit observable AssetSpec per dbt source."""
            specs: List[dg.AssetSpec] = []
            seen: set[dg.AssetKey] = set()
            for src_uid, src in (manifest.get("sources") or {}).items():
                schema = src.get("schema") or src.get("source_name") or ""
                name = src.get("identifier") or src.get("name") or ""
                if not name:
                    continue
                key_parts = [schema, name] if schema else [name]
                key = dg.AssetKey([str(p) for p in key_parts if p])
                if key in seen:
                    continue
                seen.add(key)
                policy = _derive_freshness_policy({**src, "unique_id": src_uid}) if self.derive_freshness_policies else None
                specs.append(
                    dg.AssetSpec(
                        key=key,
                        description=src.get("description"),
                        kinds={"dbt", "source"},
                        metadata={_UNIQUE_ID_KEY: src_uid},
                        freshness_policy=policy,
                    )
                )
            return specs

        def _build_external_package_specs(self, manifest: dict) -> List[dg.AssetSpec]:
            """Emit stub AssetSpec per model whose ``package_name`` is in
            ``external_packages`` (dbt mesh — upstream project owns the asset).

            Prefers ``self.get_asset_spec`` — this project's own configured
            DagsterDbtTranslator (``translation`` / ``translation_settings``) —
            for the key, so the stub matches real dagster-dbt conventions
            (schema nesting, ``meta.dagster.asset_key`` overrides) instead of
            a bare model-name guess. When the mesh's upstream project uses an
            equivalent translation scheme (the common setup), this key now
            matches what that project's own code location actually publishes,
            with no manual ``asset_overrides`` needed. Falls back to the old
            manual ``meta.dagster.asset_key`` / bare-alias derivation only if
            the translator call raises."""
            if not self.external_packages:
                return []
            package_set = set(self.external_packages)
            specs: List[dg.AssetSpec] = []
            for unique_id, props in (manifest.get("nodes") or {}).items():
                if props.get("resource_type") != "model":
                    continue
                if props.get("package_name") not in package_set:
                    continue
                try:
                    key = self.get_asset_spec(manifest, unique_id, None).key
                except Exception:
                    meta_asset_key = ((props.get("meta") or {}).get("dagster") or {}).get("asset_key")
                    if meta_asset_key:
                        if isinstance(meta_asset_key, str):
                            key = dg.AssetKey(meta_asset_key.split("/"))
                        elif isinstance(meta_asset_key, list):
                            key = dg.AssetKey([str(x) for x in meta_asset_key])
                        else:
                            continue
                    else:
                        alias = props.get("alias") or props.get("name")
                        if not alias:
                            continue
                        key = dg.AssetKey(alias)
                specs.append(
                    dg.AssetSpec(
                        key=key,
                        description=props.get("description"),
                        metadata={
                            _UNIQUE_ID_KEY: unique_id,
                            _EXTERNAL_PACKAGE_KEY: props.get("package_name") or "",
                        },
                        kinds={"dbt", "external"},
                    )
                )
            return specs

        def _build_contract_asset_checks(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetCheckSpec]:
            checks: List[dg.AssetCheckSpec] = []
            for unique_id, node in (manifest.get("nodes") or {}).items():
                if node.get("resource_type") != "model":
                    continue
                contract = (node.get("config") or {}).get("contract") or {}
                if not contract.get("enforced"):
                    continue
                asset_key = base_specs_by_unique_id.get(unique_id)
                if asset_key is None:
                    continue
                for column_name, column_info in (node.get("columns") or {}).items():
                    for constraint in column_info.get("constraints") or []:
                        constraint_type = constraint.get("type")
                        if not constraint_type:
                            continue
                        checks.append(
                            dg.AssetCheckSpec(
                                name=f"contract_{column_name}_{constraint_type}",
                                asset=asset_key,
                                description=(
                                    f"dbt contract: column `{column_name}` has "
                                    f"`{constraint_type}` constraint."
                                ),
                            )
                        )
            return checks

        def _wants_enhanced_sensor(self) -> bool:
            return (
                self.emit_test_check_evaluations
                or self.emit_skip_reason_observations
                or self.filter_external_packages_from_sensor
            )

        def _build_enhanced_polling_sensor(
            self, manifest: dict
        ) -> Optional[dg.SensorDefinition]:
            """Build a polling sensor that:
             - emits AssetMaterialization for successful models (base behavior)
             - emits AssetCheckEvaluation for every result (pass/fail) if
               emit_test_check_evaluations
             - emits AssetObservation with skip_reason for skipped models if
               emit_skip_reason_observations
             - filters out external_packages if
               filter_external_packages_from_sensor

            Replaces the base component's OOTB polling sensor when any of
            the above flags are set. Ports the pattern from
            eric-thomas-dagster/dbt-cloud-mesh-demo's mesh_aware_sensor.
            """
            from datetime import timedelta as _td

            workspace = self.workspace

            # Build the {AssetKey → node metadata} lookup used per-event
            # for external-package filtering.
            external_asset_keys: set[dg.AssetKey] = set()
            if self.filter_external_packages_from_sensor and self.external_packages:
                package_set = set(self.external_packages)
                for _uid, props in (manifest.get("nodes") or {}).items():
                    if props.get("resource_type") != "model":
                        continue
                    if props.get("package_name") not in package_set:
                        continue
                    alias = props.get("alias") or props.get("name")
                    if alias:
                        external_asset_keys.add(dg.AssetKey(alias))

            emit_checks = self.emit_test_check_evaluations
            emit_skips = self.emit_skip_reason_observations
            captured_manifest = manifest

            @dg.sensor(
                name=f"enriched_dbt_cloud_polling_sensor_{id(workspace)}",
                description=(
                    "Enhanced dbt Cloud polling sensor — emits check evaluations "
                    "on test results and skip-reason observations in addition to "
                    "materializations."
                ),
                minimum_interval_seconds=30,
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
            def _enhanced_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
                from dagster._time import datetime_from_timestamp, get_current_datetime

                cursor_ts = float(context.cursor) if context.cursor else None
                now = get_current_datetime()
                lower = cursor_ts if cursor_ts is not None else (now - _td(seconds=60)).timestamp()
                upper = now.timestamp()

                client = workspace.get_client() if hasattr(workspace, "get_client") else getattr(workspace, "client", None)
                if client is None:
                    context.log.warning("workspace has no client — skipping tick")
                    return dg.SensorResult()

                workspace_data = None
                try:
                    workspace_data = workspace.get_or_fetch_workspace_data()
                except Exception:
                    pass

                # Load the recent-runs list. API shape varies slightly across
                # dagster-dbt versions.
                runs: list = []
                try:
                    project_id = getattr(workspace, "project_id", None) or getattr(client, "project_id", None)
                    environment_id = getattr(workspace, "environment_id", None) or getattr(client, "environment_id", None)
                    result = client.get_runs_batch(
                        project_id=project_id,
                        environment_id=environment_id,
                        finished_at_lower_bound=datetime_from_timestamp(lower),
                        finished_at_upper_bound=datetime_from_timestamp(upper),
                        offset=0,
                    )
                    runs = result[0] if isinstance(result, tuple) else (result or [])
                except Exception as e:
                    context.log.warning(f"failed to fetch runs batch: {e}")
                    return dg.SensorResult()

                adhoc_job_ids: set = set(getattr(workspace_data, "adhoc_job_ids", set()) or set())

                all_events: list = []
                for run_details in runs:
                    try:
                        from dagster_dbt.cloud_v2.types import DbtCloudRun
                        run = DbtCloudRun.from_run_details(run_details)
                    except Exception:
                        continue
                    if run.job_definition_id in adhoc_job_ids:
                        continue

                    try:
                        run_results_json = client.get_run_results_json(run_id=run.id)
                    except Exception:
                        continue

                    invocation_id = (run_results_json.get("metadata") or {}).get("invocation_id", "")
                    run_url = getattr(run, "url", None)

                    for result in run_results_json.get("results", []):
                        unique_id = result.get("unique_id", "")
                        node = captured_manifest.get("nodes", {}).get(unique_id)
                        if not node:
                            continue
                        if node.get("resource_type") not in ("model", "seed", "snapshot"):
                            continue
                        alias = node.get("alias") or node.get("name")
                        if not alias:
                            continue
                        asset_key = dg.AssetKey(alias)

                        if asset_key in external_asset_keys:
                            continue

                        status = str(result.get("status") or "").lower()
                        exec_time = result.get("execution_time", 0.0)
                        message = result.get("message") or ""

                        base_meta: dict = {
                            "unique_id": unique_id,
                            "invocation_id": invocation_id,
                            "execution_duration": exec_time,
                        }
                        if run_url:
                            base_meta["run_url"] = dg.MetadataValue.url(run_url)

                        # Success → materialization (base behavior)
                        if status in ("success", "pass", "noop", "partial_success"):
                            all_events.append(
                                dg.AssetMaterialization(asset_key=asset_key, metadata=base_meta)
                            )

                        # Skipped → observation with skip_reason
                        if status == "skipped" and emit_skips:
                            all_events.append(
                                dg.AssetObservation(
                                    asset_key=asset_key,
                                    metadata={**base_meta, "skip_reason": message or "(no reason from dbt)"},
                                )
                            )

                        # Any result → check evaluation
                        if emit_checks and status in ("success", "pass", "fail", "error", "warn"):
                            all_events.append(
                                dg.AssetCheckEvaluation(
                                    asset_key=asset_key,
                                    check_name="dbt_cloud_run_status",
                                    passed=(status in ("success", "pass")),
                                    metadata={**base_meta, "status": status, "message": message},
                                    severity=(
                                        dg.AssetCheckSeverity.WARN
                                        if status == "warn"
                                        else dg.AssetCheckSeverity.ERROR
                                    ),
                                )
                            )

                context.update_cursor(str(upper))
                context.log.info(
                    f"enriched dbt Cloud sensor emitting {len(all_events)} events from {len(runs)} runs"
                )
                return dg.SensorResult(asset_events=all_events)

            return _enhanced_sensor

        def _build_mirror_jobs_addendum(self) -> dg.Definitions:
            """Return a Definitions with AssetSpec + @job for every user-defined
            Cloud job, per ``mirror_jobs`` mode. Filtered by
            ``job_selection_include/exclude`` if set."""
            if self.mirror_jobs == "off":
                return dg.Definitions()

            raw_jobs = _list_dbt_cloud_jobs_via_client(self.workspace)
            if not raw_jobs:
                return dg.Definitions()

            # Filter via selection DSL (needs .name / .id / .job_type attrs)
            class _JobShim:
                def __init__(self, j):
                    self.id = j.get("id")
                    self.name = j.get("name")
                    self.job_type = j.get("job_type")
            shims = [_JobShim(j) for j in raw_jobs
                     if not (j.get("name") or "").startswith(_DAGSTER_ADHOC_PREFIX)]
            if self.job_selection_include or self.job_selection_exclude:
                shims = apply_selection(
                    shims, self.job_selection_include, self.job_selection_exclude
                )

            emit_asset = self.mirror_jobs in ("asset", "both")
            emit_job = self.mirror_jobs in ("job", "both")

            job_specs: List[dg.AssetSpec] = []
            mirrored_jobs: List[Any] = []
            workspace = self.workspace
            trigger_defaults = self.job_trigger_defaults or {}

            class DbtCloudJobTriggerRunConfig(dg.Config):
                """Per-run override for a mirrored dbt Cloud job trigger.

                `steps_override` is the one field the real dbt Cloud Cloud v2
                client's `trigger_job_run(job_id, steps_override=None)`
                actually accepts — confirmed against the installed
                dagster-dbt client directly; earlier revisions of this config
                also exposed `cause`/`schema_override`/`git_branch`/`git_sha`,
                which that method has no parameters for at all, so setting
                any of them would have broken the trigger call outright.

                Defaults to None, meaning "use this job's own
                job_trigger_defaults (or dbt Cloud's own job-level command,
                if that's unset too)". Set it via the Dagster UI launchpad or
                `dg launch --config` to override for this one run only — no
                YAML edit, no redeploy.
                """
                steps_override: Optional[List[str]] = None

            for shim in shims:
                cloud_job_id = shim.id
                cloud_job_name = shim.name or f"job_{cloud_job_id}"
                key = dg.AssetKey(_sanitize_job_name(cloud_job_name))

                if emit_asset:
                    job_specs.append(
                        dg.AssetSpec(
                            key=key,
                            kinds={"dbt_cloud_job"},
                            description=f"dbt Cloud job {cloud_job_id}: {cloud_job_name!r}",
                            metadata={
                                _DBT_CLOUD_JOB_ID_METADATA_KEY: cloud_job_id,
                                _DBT_CLOUD_JOB_NAME_METADATA_KEY: cloud_job_name,
                            },
                        )
                    )

                if emit_job:
                    op_name = _sanitize_job_name(cloud_job_name)
                    default_steps_override = trigger_defaults.get("steps_override")
                    translator = self.translator

                    @dg.op(name=f"trigger_dbt_cloud_{op_name}")
                    def _trigger_op(
                        context: dg.OpExecutionContext,
                        config: DbtCloudJobTriggerRunConfig,
                        _workspace=workspace,
                        _cloud_job_id=cloud_job_id,
                        _cloud_job_name=cloud_job_name,
                        _default_steps_override=default_steps_override,
                        _translator=translator,
                    ):
                        client = getattr(_workspace, "client", None) or _workspace
                        steps_override = (
                            config.steps_override
                            if config.steps_override is not None
                            else _default_steps_override
                        )

                        run = client.trigger_job_run(
                            job_id=_cloud_job_id, steps_override=steps_override
                        )
                        run_id = getattr(run, "id", None) or (run or {}).get("id")
                        context.log.info(f"Triggered dbt Cloud job {_cloud_job_id} → run {run_id}")

                        run_details = client.poll_run(run_id)
                        status = (run_details or {}).get("status")
                        if status != DbtCloudJobRunStatusType.SUCCESS.value:
                            raise dg.Failure(
                                f"dbt Cloud job {_cloud_job_id} run {run_id} finished with "
                                f"status {status!r}, not success"
                            )

                        # Real per-model results (run_results.json) + the real
                        # manifest for THIS run, not a hand-maintained asset-key
                        # list -- so materializations always reflect what the
                        # job actually built, and keys match however the
                        # project's own translator would key the same model
                        # elsewhere, instead of a parallel guessed scheme.
                        run_results = client.get_run_results_json(run_id)
                        manifest = client.get_run_manifest_json(run_id)
                        nodes = {**(manifest.get("nodes") or {}), **(manifest.get("sources") or {})}

                        any_failed = False
                        for result in run_results.get("results") or []:
                            unique_id = result.get("unique_id")
                            result_status = result.get("status")
                            node = nodes.get(unique_id)
                            if node is None:
                                continue
                            # run_results.json covers every node type the
                            # invocation touched -- tests, seeds, snapshots,
                            # not just models. A dbt test result belongs on
                            # the MODEL asset it tests, as a real
                            # AssetCheckEvaluation, not a materialization of
                            # its own -- handled in the branch below. The
                            # overall run's status was already checked above
                            # (dg.Failure raised if the whole dbt Cloud run
                            # didn't report success), so a test result
                            # reaching this point is one dbt Cloud's own job
                            # settings already decided wasn't run-blocking --
                            # surfaced here as a real, alertable check
                            # (Dagster+'s "Asset" alert policy fires on check
                            # failures), not escalated into failing this op
                            # too.
                            if node.get("resource_type") == "test":
                                # Prefer `attached_node` (newer dbt manifests:
                                # the single node this generic test is
                                # attached to) over the first model/seed/
                                # snapshot in depends_on.nodes (a relationship
                                # test can depend on two models; attached_node
                                # is dbt's own canonical "this test belongs to
                                # X" answer when there is one).
                                parent_unique_id = node.get("attached_node")
                                if not parent_unique_id:
                                    for dep_id in (node.get("depends_on") or {}).get("nodes") or []:
                                        dep_node = nodes.get(dep_id)
                                        if dep_node and dep_node.get("resource_type") in (
                                            "model", "seed", "snapshot",
                                        ):
                                            parent_unique_id = dep_id
                                            break
                                parent_node = nodes.get(parent_unique_id) if parent_unique_id else None
                                if parent_node is None:
                                    context.log.warning(
                                        f"dbt test {unique_id!r} has no resolvable parent "
                                        f"model/seed/snapshot -- skipping check emission"
                                    )
                                    continue
                                try:
                                    parent_asset_key = _translator.get_asset_key(parent_node)
                                except Exception as e:
                                    context.log.warning(
                                        f"could not derive an asset key for test {unique_id!r}'s "
                                        f"parent {parent_unique_id!r}: {e}"
                                    )
                                    continue
                                test_passed = result_status == "pass"
                                context.log_event(
                                    dg.AssetCheckEvaluation(
                                        asset_key=parent_asset_key,
                                        check_name=node.get("name") or unique_id,
                                        passed=test_passed,
                                        severity=(
                                            dg.AssetCheckSeverity.WARN
                                            if result_status == "warn"
                                            else dg.AssetCheckSeverity.ERROR
                                        ),
                                        description=(
                                            result.get("message")
                                            or f"dbt test {node.get('name') or unique_id} "
                                               f"finished with status {result_status!r}"
                                        ),
                                        metadata={
                                            "dbt_cloud/job_id": _cloud_job_id,
                                            "dbt_cloud/run_id": run_id,
                                            "dbt/status": result_status,
                                            "dbt/execution_time": result.get("execution_time"),
                                        },
                                    )
                                )
                                continue
                            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                                continue
                            try:
                                asset_key = _translator.get_asset_key(node)
                            except Exception as e:
                                context.log.warning(
                                    f"could not derive an asset key for {unique_id!r}: {e}"
                                )
                                continue
                            if result_status in ("success", "pass"):
                                context.log_event(
                                    dg.AssetMaterialization(
                                        asset_key=asset_key,
                                        description=(
                                            f"Materialized via dbt Cloud job "
                                            f"{_cloud_job_id} (run {run_id})"
                                        ),
                                        metadata={
                                            "dbt_cloud/job_id": _cloud_job_id,
                                            "dbt_cloud/run_id": run_id,
                                            "dbt/status": result_status,
                                            "dbt/execution_time": result.get("execution_time"),
                                        },
                                    )
                                )
                            else:
                                any_failed = True
                                context.log.error(
                                    f"dbt node {unique_id} ({asset_key.to_user_string()}) "
                                    f"finished with status {result_status!r}: {result.get('message')}"
                                )
                        if any_failed:
                            raise dg.Failure(
                                f"dbt Cloud job {_cloud_job_id} run {run_id} succeeded overall "
                                f"but one or more models finished with a non-success status "
                                f"(see per-model errors above)"
                            )

                    # NOTE: a job-composition function's parameters are all
                    # treated as graph inputs by Dagster's composition DSL
                    # (do_composition), discarding any Python default value
                    # -- `def _mirrored_job(_op=_trigger_op): _op()` raises
                    # "InputMappingNode object is not callable" the moment
                    # this job is ever actually built, for every mirrored
                    # job, unconditionally. Plain closure capture (no
                    # parameter) is correct here because @dg.job composition
                    # runs synchronously within this same loop iteration --
                    # no late-binding risk despite _trigger_op being
                    # reassigned each iteration.
                    @dg.job(name=op_name)
                    def _mirrored_job():
                        _trigger_op()

                    mirrored_jobs.append(_mirrored_job)

            return dg.Definitions(
                assets=job_specs if job_specs else None,
                jobs=mirrored_jobs if mirrored_jobs else None,
            )

        def _wrap_with_monitor(self, defs: dg.Definitions) -> dg.Definitions:
            """Replace each AssetsDefinition with a monitored version that
            streams per-model Output events during execution."""
            workspace = self.workspace
            poll_interval = self.poll_interval
            fail_fast = self.fail_fast

            monitored_assets: list[Any] = []
            for asset in defs.assets or []:
                if not isinstance(asset, dg.AssetsDefinition):
                    monitored_assets.append(asset)
                    continue

                @dg.multi_asset(
                    specs=list(asset.specs),
                    check_specs=list(asset.check_specs),
                    can_subset=True,
                    name=asset.op.name,
                )
                def _monitored_dbt_cloud_assets(
                    context: dg.AssetExecutionContext,
                    _workspace=workspace,
                    _poll_interval=poll_interval,
                    _fail_fast=fail_fast,
                ):
                    invocation = _workspace.cli(["build"], context=context)
                    run_id = invocation.run_handler.run_id
                    context.log.info(f"Triggered dbt Cloud run {run_id}")
                    monitor = DbtCloudRunMonitor(
                        client=invocation.client,
                        run_id=run_id,
                        poll_interval=_poll_interval,
                        fail_fast=_fail_fast,
                    )
                    yield from monitor.stream(
                        context=context,
                        manifest=invocation.manifest,
                        dagster_dbt_translator=invocation.dagster_dbt_translator,
                    )

                monitored_assets.append(_monitored_dbt_cloud_assets)

            return dg.Definitions(
                assets=monitored_assets,
                resources=defs.resources,
                schedules=defs.schedules,
                sensors=defs.sensors,
                asset_checks=list(defs.asset_checks) if defs.asset_checks else None,
                jobs=list(defs.jobs) if defs.jobs else None,
            )

        def _filter_mirrored_jobs(self, defs: dg.Definitions) -> dg.Definitions:
            """Filter Cloud jobs mirrored as Dagster jobs via the include/exclude
            selection DSL. No-op when both are unset."""
            if not self.job_selection_include and not self.job_selection_exclude:
                return defs
            if not defs.jobs:
                return defs
            # Each mirrored job carries the DbtCloudJob attributes on its
            # metadata; the filter operates on the underlying job objects
            # exposed as metadata (fall back to Dagster job name matching if
            # the metadata shape isn't there).
            kept: list[Any] = []
            for job in defs.jobs:
                cloud_job = getattr(job, "_dbt_cloud_job", None) or getattr(job, "cloud_job", None)
                if cloud_job is None:
                    # Fallback: treat the Dagster job's name as the DbtCloudJob name.
                    class _NameOnly:
                        pass
                    cloud_job = _NameOnly()
                    cloud_job.name = job.name  # type: ignore[attr-defined]
                    cloud_job.id = None  # type: ignore[attr-defined]
                    cloud_job.job_type = None  # type: ignore[attr-defined]
                if apply_selection(
                    [cloud_job], self.job_selection_include, self.job_selection_exclude
                ):
                    kept.append(job)
            return dg.Definitions(
                assets=list(defs.assets) if defs.assets else None,
                resources=defs.resources,
                schedules=defs.schedules,
                sensors=defs.sensors,
                asset_checks=list(defs.asset_checks) if defs.asset_checks else None,
                jobs=kept,
            )

        # ─────────────────────────────────────────────────────────────
        # Override build_defs
        # ─────────────────────────────────────────────────────────────

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            base_defs = super().build_defs(context)

            manifest = self._resolve_manifest()
            defs = base_defs
            if manifest is not None:
                # Build {unique_id → AssetKey} from base specs for exposure /
                # contract-check builders to resolve deps against real keys.
                base_specs_by_unique_id: Dict[str, dg.AssetKey] = {}
                for spec in base_defs.resolve_all_asset_specs():
                    uid = _get_str_meta(dict(spec.metadata), _UNIQUE_ID_KEY)
                    if uid:
                        base_specs_by_unique_id[uid] = spec.key

                def enrich(spec: dg.AssetSpec) -> dg.AssetSpec:
                    try:
                        enriched = self._enrich_spec(spec, manifest)
                    except Exception:
                        enriched = spec
                    if self.asset_overrides:
                        lookup_key = spec.key.to_user_string()
                        uid = _get_str_meta(dict(spec.metadata), _UNIQUE_ID_KEY)
                        override_deps = _resolve_override_deps(self.asset_overrides, lookup_key, uid)
                        if override_deps:
                            try:
                                existing = list(enriched.deps or [])
                                enriched = enriched.merge_attributes(
                                    deps=existing + list(override_deps)
                                )
                            except Exception:
                                pass
                    return enriched

                defs = base_defs.map_resolved_asset_specs(func=enrich)

                extra_specs: List[dg.AssetSpec] = []
                if self.emit_exposures_as_assets:
                    try:
                        extra_specs.extend(
                            self._build_exposure_specs(manifest, base_specs_by_unique_id)
                        )
                    except Exception:
                        pass
                if self.emit_source_assets:
                    try:
                        extra_specs.extend(self._build_source_specs(manifest))
                    except Exception:
                        pass
                if self.external_packages:
                    try:
                        extra_specs.extend(self._build_external_package_specs(manifest))
                    except Exception:
                        pass
                if self.emit_semantic_layer_as_assets:
                    try:
                        extra_specs.extend(
                            self._build_semantic_layer_specs(manifest, base_specs_by_unique_id)
                        )
                    except Exception:
                        pass

                extra_checks: List[dg.AssetCheckSpec] = []
                if self.emit_contract_checks:
                    try:
                        extra_checks.extend(
                            self._build_contract_asset_checks(manifest, base_specs_by_unique_id)
                        )
                    except Exception:
                        pass

                if extra_specs or extra_checks:
                    addendum = dg.Definitions(
                        assets=list(extra_specs) if extra_specs else None,
                        asset_checks=list(extra_checks) if extra_checks else None,
                    )
                    defs = dg.Definitions.merge(defs, addendum)

            if self.monitor_runs:
                try:
                    defs = self._wrap_with_monitor(defs)
                except Exception as e:
                    if hasattr(context, "log"):
                        context.log.warning(  # type: ignore[attr-defined]
                            f"monitor_runs wrap failed, using unmonitored defs: {e}"
                        )

            # Enhanced polling sensor — replaces base OOTB sensor with one
            # that emits check evaluations + skip-reason observations + can
            # filter external_packages. Only when the user asked for one of
            # the enhancements; otherwise leave the base sensor alone.
            if manifest is not None and self._wants_enhanced_sensor():
                try:
                    enhanced_sensor = self._build_enhanced_polling_sensor(manifest)
                    if enhanced_sensor is not None:
                        # Drop the base polling sensor (any sensor whose name
                        # contains 'polling') and add ours in its place. Other
                        # user-supplied sensors pass through unchanged.
                        kept_sensors = [
                            s for s in (defs.sensors or [])
                            if "polling" not in (getattr(s, "name", "") or "").lower()
                        ]
                        kept_sensors.append(enhanced_sensor)
                        defs = dg.Definitions(
                            assets=list(defs.assets) if defs.assets else None,
                            resources=defs.resources,
                            schedules=defs.schedules,
                            sensors=kept_sensors,
                            asset_checks=list(defs.asset_checks) if defs.asset_checks else None,
                            jobs=list(defs.jobs) if defs.jobs else None,
                        )
                except Exception as e:
                    if hasattr(context, "log"):
                        context.log.warning(  # type: ignore[attr-defined]
                            f"enhanced polling sensor build failed, keeping base sensor: {e}"
                        )

            # Mirror Cloud jobs as AssetSpecs / Dagster @jobs / both.
            # Filtered by job_selection_include/exclude inside the builder.
            if self.mirror_jobs != "off":
                try:
                    mirror_addendum = self._build_mirror_jobs_addendum()
                    defs = dg.Definitions.merge(defs, mirror_addendum)
                except Exception as e:
                    if hasattr(context, "log"):
                        context.log.warning(  # type: ignore[attr-defined]
                            f"mirror_jobs failed, skipping: {e}"
                        )

            defs = self._filter_mirrored_jobs(defs)
            return defs

except ImportError:
    # dagster-dbt not installed — stub keeps the class name resolvable so
    # YAML validates; build_defs raises with an install hint.
    class EnrichedDbtCloudWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Stub: requires ``dagster-dbt`` (with the ``cloud_v2`` module) to be installed.

        Install with: ``pip install 'dagster-dbt[cloud]'``
        """

        workspace: Optional[Any] = Field(default=None)
        translation: Optional[Any] = Field(default=None)
        select: Optional[str] = Field(default=None)
        exclude: Optional[str] = Field(default=None)

        # Cloud-specific
        monitor_runs: bool = Field(default=False)
        fail_fast: bool = Field(default=False)
        poll_interval: float = Field(default=5.0)
        mirror_jobs: Literal["off", "asset", "job", "both"] = Field(default="off")
        job_trigger_defaults: Optional[Dict[str, Any]] = Field(default=None)
        job_selection_include: Optional[str] = Field(default=None)
        job_selection_exclude: Optional[str] = Field(default=None)

        # Enrichment
        dbt_docs_url: Optional[str] = Field(default=None)
        include_exposures: bool = Field(default=False)
        include_metrics: bool = Field(default=False)
        include_semantic_models: bool = Field(default=False)
        include_contracts: bool = Field(default=False)
        include_meta: bool = Field(default=False)
        include_source_freshness: bool = Field(default=False)
        include_doc_blocks: bool = Field(default=False)
        manifest_path: Optional[str] = Field(default=None)
        asset_overrides: Optional[Dict[str, AssetOverride]] = Field(default=None)

        emit_exposures_as_assets: bool = Field(default=False)
        emit_source_assets: bool = Field(default=False)
        derive_freshness_policies: bool = Field(default=False)
        emit_contract_checks: bool = Field(default=False)
        external_packages: Optional[List[str]] = Field(default=None)
        emit_semantic_layer_as_assets: bool = Field(default=False)
        enable_materialization_kinds: bool = Field(default=False)
        auto_trigger_on_freshness_failure: bool = Field(default=False)
        derive_lag_tolerance_automation: bool = Field(default=False)
        default_automation_condition: Optional[Dict[str, Any]] = Field(default=None)
        code_version_strategy: Literal["disabled", "hash", "sqlglot"] = Field(default="disabled")
        state_manifest_path: Optional[str] = Field(default=None)
        include_state_explain: bool = Field(default=False)
        derive_state_tags: bool = Field(default=False)
        emit_test_check_evaluations: bool = Field(default=False)
        emit_skip_reason_observations: bool = Field(default=False)
        filter_external_packages_from_sensor: bool = Field(default=False)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            raise ImportError(
                "EnrichedDbtCloudWorkspaceComponent requires dagster-dbt with the "
                "cloud_v2 module. Install with: pip install 'dagster-dbt[cloud]'"
            )
