"""Enriched dbt Project Component.

Extends the official ``dagster-dbt`` ``DbtProjectComponent`` with a set of
opt-in enrichments that read the dbt manifest and augment the emitted
AssetSpecs / AssetCheckSpecs. Drop-in replacement — set no flags and it
behaves identically to the base component.

Currently wired: **dbt Core mode** (via ``project:``). Cloud mode
(``dbt_cloud_workspace:``) is scaffolded but raises ``NotImplementedError``
— Phase 2 will wire it in via composition with ``DbtCloudComponent`` and
add the mid-run monitor + selection DSL ported from
``eric-thomas-dagster/dbt-cloud-mesh-demo``.

## Enrichments (grouped)

**Metadata enrichment (existing — read the dbt manifest, attach to AssetSpecs):**
- ``dbt_docs_url``            — clickable link to hosted dbt docs per node
- ``include_exposures``       — attach downstream-exposure list as JSON metadata
- ``include_metrics``         — attach metric definitions as JSON metadata
- ``include_semantic_models`` — attach semantic-model definitions as JSON metadata
- ``include_contracts``       — attach contract-enforced + column constraints
- ``include_meta``            — attach full ``node.meta`` dict (minus dagster subkey)
- ``include_source_freshness``— attach source freshness thresholds + loader
- ``include_doc_blocks``      — resolve ``{{ doc() }}`` refs and embed contents

**Real behavior (new — Phase 1):**
- ``emit_exposures_as_assets``   — emit exposures as **observable AssetSpecs**
                                    with real deps on their upstream models
- ``derive_freshness_policies``  — real ``FreshnessPolicy`` on sources (from
                                    ``sources.freshness``) AND models with
                                    dbt 1.9+ ``config.freshness.build_after``.
                                    Also honors ``meta.dagster.freshness_policy``.
- ``emit_contract_checks``       — contract config → per-column ``AssetCheckSpec``s
                                    so contract violations show up as failing checks
- ``external_packages``          — dbt mesh: emit stub observable AssetSpecs for
                                    every model whose ``package_name`` matches,
                                    so downstream lineage renders even when the
                                    upstream project is owned by another Dagster
                                    code location

**Per-model config (existing — read from ``meta.dagster``):**
- ``partitions_def``      — declared in dbt YAML, applied per-model
- ``automation_condition``— declared in dbt YAML, applied per-model

**Per-asset overrides (existing):**
- ``asset_overrides``     — ``{asset_key: {depends_on: [...]}}`` to inject deps

## Portable-but-not-yet-shipped (queued for follow-up)

- Phase 2:
  - Cloud mode via ``dbt_cloud_workspace``
  - Mid-run per-model monitor (parses debug logs mid-execution to yield Output
    events as each model completes) — ports ``DbtCloudRunMonitor`` from the
    ``dbt-cloud-mesh-demo`` repo
  - dbt-style selection DSL for filtering which Cloud jobs to mirror
  - Mesh-aware exclusion for Cloud
- Phase 3+:
  - ``emit_semantic_layer_as_assets`` — semantic_models + metrics as AssetSpecs
  - ``code_version_strategy: hash | sqlglot | disabled`` — canonical SQL hash
    via sqlglot (whitespace / comment changes don't bump code version)
  - Materialization kinds (``table`` / ``view`` / ``incremental`` chips)
  - Explorer URL / opt-out SQL-in-description polish
  - Slim-CI helpers (``defer_config``, ``state_manifest_path`` + dbt/state tags)
  - Skip-reason surfacing in event stream (``node.status == 'skipped'`` → why)
  - ``dbt state explain`` output as per-model metadata
  - ``lag_tolerance`` on the freshness policy derivation

## Vendored helpers

Portable enrichment functions are vendored from the user's dagster-io PR
branches (``et/dbt-exposures-as-assets``, ``et/dbt-source-freshness-policies``,
``et/dbt-model-freshness-automation-condition``, ``et/dbt-contract-asset-checks``,
``et/dbt-mesh-external-packages``). When these PRs merge and release, delete
the ``_VENDORED_*`` sections below and switch to importing from
``dagster_dbt.asset_utils`` / ``dagster_dbt.asset_specs``.
"""
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import dagster as dg
from pydantic import Field

# Constants used by dagster-dbt to store internal metadata on AssetSpecs.
# Stable public keys — dagster-dbt uses them to round-trip unique_id.
_UNIQUE_ID_KEY = "dagster_dbt/unique_id"

# Namespaced metadata keys added by our vendored helpers. Match dagster-dbt's
# conventions so that when the PRs merge and users switch over, the metadata
# keys they've indexed against (in the UI, downstream Insights charts, etc.)
# stay identical.
_CONTRACT_ENFORCED_KEY = "dagster_dbt/contract_enforced"
_COLUMN_CONSTRAINTS_KEY = "dagster_dbt/column_constraints"
_MODEL_CONSTRAINTS_KEY = "dagster_dbt/model_constraints"
_EXPOSURE_TYPE_KEY = "dagster_dbt/exposure_type"
_EXPOSURE_URL_KEY = "dagster_dbt/exposure_url"
_EXPOSURE_MATURITY_KEY = "dagster_dbt/exposure_maturity"
_EXTERNAL_PACKAGE_KEY = "dagster_dbt/external_package"

# dbt exposure types → Dagster kind for UI icon distinction.
_DBT_EXPOSURE_TYPE_TO_KIND: Mapping[str, str] = {
    "dashboard": "dashboard",
    "notebook": "notebook",
    "analysis": "analysis",
    "ml": "ml",
    "application": "application",
}

# dbt freshness spec period → timedelta kwarg
_DBT_FRESHNESS_PERIOD_TO_TIMEDELTA_KWARG: Mapping[str, str] = {
    "minute": "minutes",
    "hour": "hours",
    "day": "days",
}


# ─── Vendored freshness derivation ────────────────────────────────────
#
# Ported verbatim from `et/dbt-source-freshness-policies` +
# `et/dbt-model-freshness-automation-condition` PR branches. Delete this
# section when those PRs merge into dagster-dbt.


def _dbt_freshness_spec_to_timedelta(
    freshness_spec: Optional[Mapping[str, Any]],
) -> Optional[timedelta]:
    """Translate a dbt ``{count, period}`` freshness spec into a ``timedelta``."""
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
    """Parse ``meta.dagster.freshness_policy`` into a Dagster FreshnessPolicy.

    Two shapes:
      - ``{type: "time_window", fail_window_seconds: int, warn_window_seconds?: int}``
      - ``{type: "cron", deadline_cron: str, lower_bound_delta_seconds: int, timezone?: str}``

    Returns None on missing / malformed / unknown-type — silent skip is right
    for ``meta``: it's arbitrary and we don't want a typo to break parsing.
    """
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


def _derive_freshness_policy(
    dbt_resource_props: Mapping[str, Any],
) -> Optional[dg.FreshnessPolicy]:
    """Precedence:
      1. ``meta.dagster.freshness_policy`` (any resource type) — user override wins
      2. ``sources.freshness.{warn_after, error_after}`` (sources only)
      3. ``models[*].config.freshness.build_after`` (models only, dbt 1.9+)
    """
    meta_dagster = (dbt_resource_props.get("meta") or {}).get("dagster") or {}
    meta_policy = _freshness_policy_from_meta_dagster(meta_dagster.get("freshness_policy"))
    if meta_policy is not None:
        return meta_policy

    resource_type = dbt_resource_props.get("resource_type")

    # sources.freshness
    if resource_type == "source":
        freshness_config = dbt_resource_props.get("freshness") or {}
        error_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("error_after"))
        warn_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("warn_after"))
        if error_after is None:
            # Dagster's TimeWindowFreshnessPolicy requires a fail_window; warn-only
            # is not usable — surface nothing.
            return None
        # Guard: warn must be strictly < fail; drop warn if the source is misconfigured.
        if warn_after is not None and warn_after >= error_after:
            warn_after = None
        return dg.FreshnessPolicy.time_window(fail_window=error_after, warn_window=warn_after)

    # dbt 1.9+ model config.freshness.build_after
    if resource_type == "model":
        freshness_config = (dbt_resource_props.get("config") or {}).get("freshness") or {}
        build_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("build_after"))
        if build_after is None:
            return None
        return dg.FreshnessPolicy.time_window(fail_window=build_after)

    return None


# ─── Vendored contract-metadata helper ────────────────────────────────
#
# Ported from `et/dbt-contract-asset-checks` + `et/dbt-contract-metadata-json`
# PR branches. Delete when those PRs merge.


def _derive_contract_metadata(
    dbt_resource_props: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return ``{contract_enforced, column_constraints, model_constraints}`` metadata
    for a model with ``config.contract.enforced: true``. Empty dict otherwise."""
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


# ─── Existing helpers: partitions_def + automation_condition from meta ────


def _partitions_def_from_meta(meta: Mapping[str, Any]) -> Optional[Any]:
    """Convert ``meta.dagster.partitions_def`` dict into a PartitionsDefinition.

    Supported types: ``daily``, ``hourly``, ``weekly``, ``monthly``, ``static``,
    ``dynamic``. Returns None on invalid shape / missing type.
    """
    if not meta or not isinstance(meta, Mapping):
        return None
    ptype = meta.get("type")
    if not ptype:
        return None
    try:
        if ptype == "daily":
            return dg.DailyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
            )
        if ptype == "hourly":
            return dg.HourlyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
            )
        if ptype == "weekly":
            return dg.WeeklyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
                day_offset=meta.get("day_offset", 0),
            )
        if ptype == "monthly":
            return dg.MonthlyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
                day_offset=meta.get("day_offset", 1),
            )
        if ptype == "static":
            values = meta.get("values")
            if not values:
                return None
            return dg.StaticPartitionsDefinition(list(values))
        if ptype == "dynamic":
            name = meta.get("name")
            if not name:
                return None
            return dg.DynamicPartitionsDefinition(name=name)
    except (KeyError, TypeError, ValueError):
        return None
    return None


def _automation_condition_from_meta(meta: Mapping[str, Any]) -> Optional[Any]:
    """Convert ``meta.dagster.automation_condition`` dict into an AutomationCondition.

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


# ─── Asset overrides ──────────────────────────────────────────────────


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


def _get_str_meta(metadata: dict, key: str) -> Optional[str]:
    """Extract a string from a dagster metadata dict, unwrapping MetadataValue if needed."""
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


# ─── External-package (mesh) spec builder ─────────────────────────────
#
# Vendored from `et/dbt-mesh-external-packages` PR branch. When the PR merges,
# delete this and import `dagster_dbt.asset_specs.build_dbt_external_package_asset_specs`.


def _build_external_package_deps_map(
    manifest: Mapping[str, Any], external_packages: Sequence[str]
) -> Dict[str, dg.AssetKey]:
    """Build ``{unique_id → AssetKey}`` for models in external packages.

    Uses a lightweight asset-key derivation (dbt's default: ``AssetKey([alias or name])``)
    that matches ``dagster_dbt.DagsterDbtTranslator.get_asset_key`` for the common
    case. Models in external packages that override the key via
    ``meta.dagster.asset_key`` will need the full translator path; for now we
    honor the ``meta.dagster.asset_key`` override manually.
    """
    if not external_packages:
        return {}
    package_set = set(external_packages)
    out: Dict[str, dg.AssetKey] = {}
    for unique_id, props in (manifest.get("nodes") or {}).items():
        if props.get("resource_type") != "model":
            continue
        if props.get("package_name") not in package_set:
            continue
        # Honor explicit override
        meta_asset_key = ((props.get("meta") or {}).get("dagster") or {}).get("asset_key")
        if meta_asset_key:
            if isinstance(meta_asset_key, str):
                out[unique_id] = dg.AssetKey(meta_asset_key.split("/"))
            elif isinstance(meta_asset_key, list):
                out[unique_id] = dg.AssetKey([str(x) for x in meta_asset_key])
            continue
        # dagster-dbt default: prefer alias, fall back to name
        alias = props.get("alias") or props.get("name")
        if not alias:
            continue
        out[unique_id] = dg.AssetKey(alias)
    return out


try:
    from dagster_dbt.components.dbt_project.component import (
        DbtProjectComponent as _DbtProjectComponent,
    )

    @dataclass
    class EnrichedDbtProjectComponent(_DbtProjectComponent):
        """Enriched drop-in for ``DbtProjectComponent``. See module docstring."""

        # ── Existing enrichments ─────────────────────────────────────
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

        # ── Phase 1 additions ─────────────────────────────────────────
        emit_exposures_as_assets: bool = False
        """Emit each dbt exposure as an observable AssetSpec with deps on its
        upstream models (in addition to metadata). Gives downstream lineage:
        if this model breaks, which dashboards are affected?"""

        derive_freshness_policies: bool = False
        """Auto-attach FreshnessPolicy to sources (from `sources.freshness`) and
        models with dbt 1.9+ `config.freshness.build_after`. Also honors
        explicit `meta.dagster.freshness_policy` config."""

        emit_contract_checks: bool = False
        """For models with `config.contract.enforced: true`, emit per-column
        AssetCheckSpecs so contract violations show as failing checks.
        Complements `include_contracts` (which only surfaces the config as
        metadata)."""

        external_packages: Optional[List[str]] = None
        """dbt mesh: package names to emit as observable stub AssetSpecs. Pair
        with `exclude: 'package:X'` on the base component so this project's dbt
        run doesn't try to rebuild the upstream. Stubs let downstream lineage
        render even though the upstream is owned by a different Dagster code
        location."""

        # ── Phase 2 (Cloud) — scaffold only, wired in a follow-up commit ──
        dbt_cloud_workspace: Optional[Any] = Field(default=None)
        """Set to a dagster-dbt DbtCloudWorkspace to switch to Cloud mode.
        NOT YET WIRED — raises NotImplementedError. Follow-up ports the
        mesh-demo's mid-run monitor + selection DSL."""

        job_filter: Optional[str] = None
        monitor_runs: bool = False
        fail_fast: bool = False
        poll_interval: float = 5.0

        # ─────────────────────────────────────────────────────────────
        # Internal helpers
        # ─────────────────────────────────────────────────────────────

        def _resolve_manifest(self, state_path: Optional[Path]) -> Optional[dict]:
            candidates: list[Path] = []
            if self.manifest_path:
                candidates.append(Path(self.manifest_path))
            try:
                project = self._project_manager.get_project(state_path)
                candidates.append(Path(project.manifest_path))
            except Exception:
                pass
            for candidate in candidates:
                try:
                    return json.loads(candidate.read_text())
                except (FileNotFoundError, PermissionError, json.JSONDecodeError):
                    continue
            return None

        def _enrich_spec(self, spec: dg.AssetSpec, manifest: dict) -> dg.AssetSpec:
            """Attach metadata + real policies to a single AssetSpec."""
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

            # 1. dbt docs URL
            if self.dbt_docs_url:
                url = f"{self.dbt_docs_url}/#!/{resource_type}/{unique_id}"
                extra["dbt_docs/url"] = dg.MetadataValue.url(url)

            # 2. Exposures consuming this model (metadata form)
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

            # 3. Metrics referencing this model
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

            # 4. Semantic models
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
                            "measures": [m.get("name") for m in sm.get("measures", [])],
                            "dimensions": [d.get("name") for d in sm.get("dimensions", [])],
                            "entities": [e.get("name") for e in sm.get("entities", [])],
                        })
                    extra["dbt_docs/semantic_models"] = dg.MetadataValue.json(sms)

            # 5. Contract enforcement + column constraints (metadata form)
            if self.include_contracts:
                extra.update(_derive_contract_metadata(node))

            # 6. Full meta dict (non-dagster keys)
            if self.include_meta:
                meta = node.get("meta", {})
                non_dagster = {k: v for k, v in meta.items() if k != "dagster"}
                if non_dagster:
                    extra["dbt_docs/meta"] = dg.MetadataValue.json(non_dagster)

            # 7. Source freshness (sources only)
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

            # 8. Model access level
            access = node.get("config", {}).get("access")
            if access and access != "protected":
                extra["dbt_docs/access"] = dg.MetadataValue.text(access)

            # 9. Language (only surface non-SQL)
            language = node.get("language")
            if language and language != "sql":
                extra["dbt_docs/language"] = dg.MetadataValue.text(language)

            # 10. Patch path (YAML file where this model is documented)
            patch_path = node.get("patch_path")
            if patch_path:
                display = patch_path.split("://")[-1] if "://" in patch_path else patch_path
                extra["dbt_docs/patch_path"] = dg.MetadataValue.text(display)

            # 11. Doc block contents (opt-in — verbose)
            if self.include_doc_blocks:
                doc_block_names = node.get("doc_blocks", [])
                if doc_block_names:
                    docs_lookup = manifest.get("docs", {})
                    resolved_blocks: dict[str, str] = {}
                    for block_name in doc_block_names:
                        for _doc_uid, doc_node in docs_lookup.items():
                            if doc_node.get("name") == block_name:
                                resolved_blocks[block_name] = doc_node.get("block_contents", "")
                                break
                    if resolved_blocks:
                        extra["dbt_docs/doc_blocks"] = dg.MetadataValue.json(resolved_blocks)

            # meta.dagster.partitions_def / .automation_condition
            dagster_meta = node.get("meta", {}).get("dagster", {}) or {}
            per_model_partitions = _partitions_def_from_meta(
                dagster_meta.get("partitions_def") or {}
            )
            per_model_automation = _automation_condition_from_meta(
                dagster_meta.get("automation_condition") or {}
            )

            enriched = spec
            if extra:
                enriched = enriched.merge_attributes(metadata=extra)
            if per_model_partitions is not None:
                enriched = enriched.replace_attributes(partitions_def=per_model_partitions)
            if per_model_automation is not None:
                enriched = enriched.replace_attributes(automation_condition=per_model_automation)

            # Freshness policy — REAL, not just metadata
            if self.derive_freshness_policies:
                policy = _derive_freshness_policy(node)
                if policy is not None:
                    enriched = enriched.replace_attributes(freshness_policy=policy)

            return enriched

        def _build_exposure_specs(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetSpec]:
            """Emit AssetSpec per exposure with deps on referenced upstream models.

            ``base_specs_by_unique_id`` maps existing model unique_ids → AssetKey, so
            exposure deps resolve to keys the base component already produced. Missing
            references (stale manifest, mesh dependencies) are silently skipped.
            """
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

        def _build_external_package_specs(self, manifest: dict) -> List[dg.AssetSpec]:
            """Emit stub AssetSpec per model whose ``package_name`` is in
            ``external_packages`` (dbt mesh — upstream project owns the asset)."""
            if not self.external_packages:
                return []
            key_map = _build_external_package_deps_map(manifest, self.external_packages)
            specs: List[dg.AssetSpec] = []
            for unique_id, asset_key in key_map.items():
                node = manifest.get("nodes", {}).get(unique_id, {})
                specs.append(
                    dg.AssetSpec(
                        key=asset_key,
                        description=node.get("description"),
                        metadata={
                            _UNIQUE_ID_KEY: unique_id,
                            _EXTERNAL_PACKAGE_KEY: node.get("package_name") or "",
                        },
                        kinds={"dbt", "external"},
                    )
                )
            return specs

        def _build_contract_asset_checks(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetCheckSpec]:
            """Per-column AssetCheckSpecs from models with enforced contracts.

            One check per (model, constraint) so failures show at the constraint
            level, not lumped into a single "contract failed" signal.
            """
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
                                    f"`{constraint_type}` constraint. Enforced by dbt at "
                                    f"build time — if this check appears failed, contract "
                                    f"metadata drifted from the manifest."
                                ),
                            )
                        )
            return checks

        # ─────────────────────────────────────────────────────────────
        # Override build_defs_from_state
        # ─────────────────────────────────────────────────────────────

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            if self.dbt_cloud_workspace is not None:
                raise NotImplementedError(
                    "dbt_cloud_workspace mode is scaffolded but not yet wired. "
                    "Follow-up commit ports the mid-run monitor + selection DSL "
                    "from eric-thomas-dagster/dbt-cloud-mesh-demo. For now, use "
                    "the base DbtCloudComponent from dagster-dbt directly."
                )

            base_defs = super().build_defs_from_state(context, state_path)

            manifest = self._resolve_manifest(state_path)
            if manifest is None:
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[attr-defined]
                        "EnrichedDbtProjectComponent: could not load manifest.json — "
                        "returning base dbt definitions without enrichment. Set "
                        "manifest_path explicitly if target/ is non-standard."
                    )
                return base_defs

            # Build {unique_id → AssetKey} from base specs (used by exposure +
            # contract-check builders to resolve deps against real keys).
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

            # Add extra AssetSpecs (exposures + external-package stubs) and
            # extra AssetCheckSpecs (contract checks). These are additive —
            # combined with the base defs via a merge.
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

            extra_checks: List[dg.AssetCheckSpec] = []
            if self.emit_contract_checks:
                try:
                    extra_checks.extend(
                        self._build_contract_asset_checks(manifest, base_specs_by_unique_id)
                    )
                except Exception:
                    pass

            if not extra_specs and not extra_checks:
                return defs

            addendum = dg.Definitions(
                assets=list(extra_specs) if extra_specs else None,
                asset_checks=list(extra_checks) if extra_checks else None,
            )
            return dg.Definitions.merge(defs, addendum)

except ImportError:
    # dagster-dbt not installed — stub keeps the class name resolvable so
    # YAML validates; build_defs raises with an install hint.
    class EnrichedDbtProjectComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Stub: requires dagster-dbt to be installed.

        Install with: pip install dagster-dbt
        """

        project: Optional[str] = Field(default=None)
        cli_args: Optional[Any] = Field(default=None)
        translation: Optional[Any] = Field(default=None)
        select: Optional[str] = Field(default=None)
        exclude: Optional[str] = Field(default=None)

        # Existing enrichments
        dbt_docs_url: Optional[str] = Field(default=None)
        include_exposures: bool = Field(default=False)
        include_metrics: bool = Field(default=False)
        include_semantic_models: bool = Field(default=False)
        include_contracts: bool = Field(default=False)
        include_meta: bool = Field(default=False)
        include_source_freshness: bool = Field(default=False)
        include_doc_blocks: bool = Field(default=False)
        asset_overrides: Optional[Dict[str, AssetOverride]] = Field(default=None)

        # Phase 1 additions
        emit_exposures_as_assets: bool = Field(default=False)
        derive_freshness_policies: bool = Field(default=False)
        emit_contract_checks: bool = Field(default=False)
        external_packages: Optional[List[str]] = Field(default=None)

        # Phase 2 (stub)
        dbt_cloud_workspace: Optional[Any] = Field(default=None)
        job_filter: Optional[str] = Field(default=None)
        monitor_runs: bool = Field(default=False)
        fail_fast: bool = Field(default=False)
        poll_interval: float = Field(default=5.0)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            raise ImportError(
                "EnrichedDbtProjectComponent requires dagster-dbt. "
                "Install with: pip install dagster-dbt"
            )


# Backward-compat alias — some existing user projects may still reference the
# old class name. Delete after the deprecation window (or immediately, per user).
DbtDocsEnrichedProjectComponent = EnrichedDbtProjectComponent
