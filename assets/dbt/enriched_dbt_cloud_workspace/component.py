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

Companion component: ``EnrichedDbtProjectComponent`` for dbt Core projects.
Both live in the same category (``dbt``) with the same enrichment
vocabulary so switching between Core and Cloud is a config-level change.

Per-component helper files (kept in this folder — DCC rule: no cross-
component shared modules):

- ``_run_monitor.py`` — vendored ``DbtCloudRunMonitor``
- ``_job_selection.py`` — vendored selection DSL

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
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Mapping, Optional

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


def _lag_tolerance_of(dbt_resource_props: Mapping[str, Any]) -> Optional[timedelta]:
    lag = ((dbt_resource_props.get("config") or {}).get("state") or {}).get("lag_tolerance")
    return _parse_duration_string(lag)


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


@dataclass
class AssetOverride(dg.Resolvable):
    depends_on: Optional[List[str]] = None


def _resolve_override_deps(
    asset_overrides: Optional[Dict[str, "AssetOverride"]],
    lookup_key: str,
) -> List[dg.AssetKey]:
    if not asset_overrides:
        return []
    ov = asset_overrides.get(lookup_key)
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


try:
    from dagster_dbt import DbtCloudComponent as _DbtCloudComponent

    # Local vendored modules (kept per-folder — DCC rule)
    from ._job_selection import apply_selection
    from ._run_monitor import DbtCloudRunMonitor

    @dataclass
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
        """Trigger overrides sent by every mirrored @job (applies with
        ``mirror_jobs`` = ``job`` or ``both``). Any unset field is not sent
        to dbt Cloud — the Cloud job's configured value is used. Common
        fields: ``cause`` (str), ``steps_override`` (list[str]), ``git_sha``
        (str), ``git_branch`` (str), ``schema_override`` (str),
        ``threads_override`` (int)."""

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
        rebuild when the derived FreshnessPolicy fails. lag_tolerance
        alone does NOT trigger — dbt handles the settling gate."""

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
            child_map: dict = manifest.get("child_map", {})
            child_ids: list[str] = child_map.get(unique_id, [])

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

            enriched = spec
            if extra:
                enriched = enriched.merge_attributes(metadata=extra)

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

        def _build_external_package_specs(self, manifest: dict) -> List[dg.AssetSpec]:
            if not self.external_packages:
                return []
            package_set = set(self.external_packages)
            specs: List[dg.AssetSpec] = []
            for unique_id, props in (manifest.get("nodes") or {}).items():
                if props.get("resource_type") != "model":
                    continue
                if props.get("package_name") not in package_set:
                    continue
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

                    @dg.op(name=f"trigger_dbt_cloud_{op_name}")
                    def _trigger_op(
                        context: dg.OpExecutionContext,
                        _workspace=workspace,
                        _cloud_job_id=cloud_job_id,
                        _cloud_job_name=cloud_job_name,
                        _trigger_defaults=trigger_defaults,
                    ):
                        client = getattr(_workspace, "client", None) or _workspace
                        # Trigger + poll — API varies slightly by dagster-dbt
                        # version; try the most common shapes.
                        for trigger_attr in ("trigger_job_run", "trigger_job", "run_job"):
                            fn = getattr(client, trigger_attr, None)
                            if not callable(fn):
                                continue
                            try:
                                run = fn(job_id=_cloud_job_id, **_trigger_defaults)
                            except TypeError:
                                try:
                                    run = fn(_cloud_job_id, **_trigger_defaults)
                                except Exception as e:
                                    context.log.warning(f"{trigger_attr} failed: {e}")
                                    continue
                            run_id = getattr(run, "id", None) or (run or {}).get("id")
                            context.log.info(
                                f"Triggered dbt Cloud job {_cloud_job_id} → run {run_id}"
                            )
                            if run_id:
                                for poll_attr in ("poll_run", "wait_for_run", "poll_job_run"):
                                    poll_fn = getattr(client, poll_attr, None)
                                    if callable(poll_fn):
                                        poll_fn(run_id)
                                        break
                            return
                        raise dg.Failure(
                            f"dbt Cloud workspace client has no known trigger method — "
                            f"tried trigger_job_run / trigger_job / run_job"
                        )

                    @dg.job(name=op_name)
                    def _mirrored_job(_op=_trigger_op):
                        _op()

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
                        override_deps = _resolve_override_deps(self.asset_overrides, lookup_key)
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
        derive_freshness_policies: bool = Field(default=False)
        emit_contract_checks: bool = Field(default=False)
        external_packages: Optional[List[str]] = Field(default=None)
        emit_semantic_layer_as_assets: bool = Field(default=False)
        enable_materialization_kinds: bool = Field(default=False)
        auto_trigger_on_freshness_failure: bool = Field(default=False)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            raise ImportError(
                "EnrichedDbtCloudWorkspaceComponent requires dagster-dbt with the "
                "cloud_v2 module. Install with: pip install 'dagster-dbt[cloud]'"
            )
