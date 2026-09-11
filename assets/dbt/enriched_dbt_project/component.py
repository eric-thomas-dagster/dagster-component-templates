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
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence

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
_SEMANTIC_MEASURES_KEY = "dagster_dbt/measures"
_SEMANTIC_DIMENSIONS_KEY = "dagster_dbt/dimensions"
_SEMANTIC_ENTITIES_KEY = "dagster_dbt/entities"
_METRIC_TYPE_KEY = "dagster_dbt/metric_type"
_METRIC_LABEL_KEY = "dagster_dbt/metric_label"

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


def _parse_duration_string(s: Any) -> Optional[timedelta]:
    """Parse a dbt-style duration string like ``"4h"`` / ``"45m"`` / ``"7d"`` /
    ``"30s"`` / ``"1w"`` into a ``timedelta``. Also accepts numeric seconds.

    Used for ``config.state.lag_tolerance`` (dbt State, ~2.0+). Returns None
    on unparseable input — silent skip is right since dbt allows Jinja-templated
    values that we can't evaluate at manifest-parse time.
    """
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
        # Try bare number = seconds
        try:
            return timedelta(seconds=float(s))
        except ValueError:
            return None
    try:
        value = float(s[:-1])
    except ValueError:
        return None
    return timedelta(**{unit_map[unit]: value})


# ─── State comparison (dbt state explain equivalent) ─────────────────
#
# dbt's `dbt state explain` command (dbt 2.0+) explains WHY a model was
# rebuilt / reused / cloned. The core comparison it does is
# per-model-checksum against a state manifest. We reproduce that
# comparison at build time and attach the result as per-model metadata,
# so users can see the reason in the Dagster UI without running the
# CLI.


def _load_state_manifest(state_manifest_path: str) -> Optional[dict]:
    """Load a state manifest.json (or the manifest.json inside a state dir)."""
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
) -> Optional[dict[str, str]]:
    """Return ``{state, explanation}`` for a model comparing its
    ``checksum.checksum`` to the state manifest's checksum for the same
    unique_id. State is one of ``modified`` / ``unchanged`` / ``new``.
    Returns None for non-model resources or when state_manifest is None."""
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
                "state's build (no-op) unless a config/macro/contract change is detected "
                "by a deeper state selector."
            ),
        }
    return {
        "state": "modified",
        "explanation": (
            f"Model '{unique_id}' checksum differs from state — will rebuild. "
            f"(state checksum: {(state_checksum or '')[:8]}, current: {(current_checksum or '')[:8]})"
        ),
    }


def _lag_tolerance_of(dbt_resource_props: Mapping[str, Any]) -> Optional[timedelta]:
    """Extract dbt State ``config.state.lag_tolerance`` as a ``timedelta`` (real
    dbt feature — see https://docs.getdbt.com/reference/resource-configs/lag-tolerance).
    Returns None when unset."""
    lag = ((dbt_resource_props.get("config") or {}).get("state") or {}).get("lag_tolerance")
    return _parse_duration_string(lag)


# Cron granularities we snap arbitrary lag_tolerance durations to. Cron
# doesn't express "every 45 minutes" cleanly; the nearest divisor is
# 30 or 60. We pick the largest supported divisor <= the requested delay
# so the automation waits AT LEAST as long as lag_tolerance asked for.
_CRON_DIVISORS_MIN: list[tuple[int, str]] = [
    (1, "* * * * *"),
    (2, "*/2 * * * *"),
    (5, "*/5 * * * *"),
    (10, "*/10 * * * *"),
    (15, "*/15 * * * *"),
    (20, "*/20 * * * *"),
    (30, "*/30 * * * *"),
]
_CRON_DIVISORS_HOUR: list[tuple[int, str]] = [
    (1, "0 * * * *"),
    (2, "0 */2 * * *"),
    (3, "0 */3 * * *"),
    (4, "0 */4 * * *"),
    (6, "0 */6 * * *"),
    (8, "0 */8 * * *"),
    (12, "0 */12 * * *"),
]


def _lag_tolerance_to_cron(lag: timedelta) -> Optional[str]:
    """Snap a lag_tolerance duration to the largest cron divisor <= the delay.

    Rounding down (rather than up) means the automation may fire slightly
    sooner than lag_tolerance asked for — which is fine because dbt itself
    still enforces the lag_tolerance skip on build. The automation just
    tells Dagster when to ATTEMPT a rebuild; dbt is the source of truth for
    whether it actually runs.
    """
    total_seconds = lag.total_seconds()
    if total_seconds < 60:
        return "* * * * *"  # 1-minute minimum
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
    # 1+ days
    days = int(total_seconds // 86400)
    if days == 1:
        return "0 0 * * *"  # daily at midnight UTC
    if days < 7:
        return f"0 0 */{days} * *"
    return "0 0 * * 0"  # weekly on Sunday at midnight


# ─── Vendored code-version derivation ─────────────────────────────────
# Ported from `et/dbt-code-version-automation` PR + new sqlglot mode for
# whitespace/comment-insensitive versioning. When the PR merges, delete
# the `hash` branch and import from dagster_dbt.


def _dbt_dialect_from_adapter(adapter_type: Optional[str]) -> Optional[str]:
    """Map a dbt adapter type (`snowflake`, `postgres`, `bigquery`, `redshift`,
    `duckdb`, `databricks`, …) to a sqlglot dialect name. sqlglot's dialect
    names are close but not always identical — this table covers the common
    warehouses. Returns None for unknown adapters; sqlglot falls back to
    generic parsing when dialect is unset."""
    if not adapter_type:
        return None
    _MAP = {
        "snowflake": "snowflake",
        "postgres": "postgres",
        "redshift": "redshift",
        "bigquery": "bigquery",
        "duckdb": "duckdb",
        "databricks": "databricks",
        "spark": "spark",
        "sparksql": "spark",
        "mysql": "mysql",
        "trino": "trino",
        "presto": "presto",
        "clickhouse": "clickhouse",
        "athena": "athena",
        "sqlite": "sqlite",
        "oracle": "oracle",
    }
    return _MAP.get(adapter_type.lower())


def _derive_code_version(
    dbt_resource_props: Mapping[str, Any],
    strategy: str,
    adapter_type: Optional[str] = None,
) -> Optional[str]:
    """Derive a Dagster code_version for a dbt node.

    - ``"disabled"`` — returns None (default, backward compatible).
    - ``"hash"``     — use dbt's own manifest ``checksum.checksum`` (SHA1 of
                       the source model file). Fast, no extra deps. Bumps
                       code version on any file edit including whitespace/
                       comment-only changes.
    - ``"sqlglot"``  — parse the compiled_code with sqlglot, canonicalize
                       (strip comments + normalize whitespace + reformat),
                       then SHA256 the canonical form. Whitespace + comment
                       changes DO NOT bump the code version — only semantic
                       SQL changes do. Requires ``sqlglot`` (optional).
                       Falls back to ``"hash"`` if the parse fails.

    Only applies to models (checksums / compiled_code are model-only in dbt).
    """
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
            # No compiled SQL — fall back to raw checksum
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None
        try:
            import hashlib
            import sqlglot  # optional dep
            dialect = _dbt_dialect_from_adapter(adapter_type)
            tree = sqlglot.parse_one(compiled, read=dialect) if dialect else sqlglot.parse_one(compiled)
            # comments=False strips block/line comments; pretty=False + sql()
            # gives a stable canonical form insensitive to whitespace.
            canonical = tree.sql(pretty=False, comments=False, dialect=dialect)
            return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        except ImportError:
            # sqlglot not installed — degrade to hash strategy
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None
        except Exception:
            # Parse failure (unusual SQL, dialect mismatch, …) — degrade to hash
            checksum = (dbt_resource_props.get("checksum") or {}).get("checksum")
            return str(checksum) if checksum else None

    return None


def _lag_tolerance_automation_condition(lag: timedelta) -> Optional[Any]:
    """Compose an AutomationCondition that fires ~lag_tolerance after any
    upstream dep is newly-updated. Uses the standard Dagster pattern:

        any_deps_match(
            newly_updated().since(cron_tick_passed(cron))
            & ~executed_with_root_target()
        ).newly_true()
        & ~in_progress()
        & in_latest_time_window()

    Where ``cron`` maps from lag_tolerance via ``_lag_tolerance_to_cron``.
    Users get "wait ~lag_tolerance after upstream changed, then rebuild"
    behavior with Dagster driving the trigger and dbt enforcing the
    exact gate on its own build step.
    """
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
        # Some dagster versions may not expose all these primitives; degrade
        # to a simpler shape so we don't break the whole enrichment.
        try:
            return dg.AutomationCondition.eager() & ~dg.AutomationCondition.in_progress()
        except Exception:
            return None


def _derive_freshness_policy(
    dbt_resource_props: Mapping[str, Any],
) -> Optional[dg.FreshnessPolicy]:
    """Precedence:
      1. ``meta.dagster.freshness_policy`` (any resource type) — user override wins
      2. ``sources.freshness.{warn_after, error_after}`` (sources only)
      3. ``models[*].config.freshness.build_after`` (models only, dbt 1.9+)
      4. Widened by ``config.state.lag_tolerance`` (models, dbt State ~2.0+) —
         if the derived fail_window is shorter than lag_tolerance, use the
         larger of the two (dbt won't rebuild inside lag_tolerance regardless).
    """
    meta_dagster = (dbt_resource_props.get("meta") or {}).get("dagster") or {}
    meta_policy = _freshness_policy_from_meta_dagster(meta_dagster.get("freshness_policy"))
    if meta_policy is not None:
        return meta_policy

    resource_type = dbt_resource_props.get("resource_type")
    lag_tolerance = _lag_tolerance_of(dbt_resource_props) if resource_type == "model" else None

    # sources.freshness
    if resource_type == "source":
        freshness_config = dbt_resource_props.get("freshness") or {}
        error_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("error_after"))
        warn_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("warn_after"))
        if error_after is None:
            return None
        if warn_after is not None and warn_after >= error_after:
            warn_after = None
        return dg.FreshnessPolicy.time_window(fail_window=error_after, warn_window=warn_after)

    # dbt 1.9+ model config.freshness.build_after — widened by lag_tolerance
    if resource_type == "model":
        freshness_config = (dbt_resource_props.get("config") or {}).get("freshness") or {}
        build_after = _dbt_freshness_spec_to_timedelta(freshness_config.get("build_after"))
        # If only lag_tolerance is set (no build_after), use lag_tolerance as
        # the fail_window — it's still a valid SLA lower bound.
        effective = build_after or lag_tolerance
        if effective is None:
            return None
        if build_after is not None and lag_tolerance is not None and lag_tolerance > build_after:
            effective = lag_tolerance
        return dg.FreshnessPolicy.time_window(fail_window=effective)

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


@dataclass
class DbtDeferConfig(dg.Resolvable):
    """Configuration for dbt's ``--defer``/``--state``/``--favor-state`` slim-CI options.

    Ported from ``et/dbt-defer-state`` PR. Slim CI pattern: dbt runs models that
    changed vs a state manifest, deferring ``ref()`` resolution to the state's
    tables for unchanged upstream models. Skips rebuilding data that hasn't
    changed.

    Args:
        state_path: Path to the state directory (containing ``manifest.json``) or
            the ``manifest.json`` file itself. Passed to dbt via ``--state <path>``.
        defer: If True (default), pass ``--defer`` so missing / unbuilt models
            resolve to the state's tables. Slim CI's core primitive.
        favor_state: If True, pass ``--favor-state`` so dbt prefers the state's
            version even when the current run has updated the table. Useful for
            cross-environment reads. Defaults to False.
    """

    state_path: str
    defer: bool = True
    favor_state: bool = False

    def to_cli_args(self) -> List[str]:
        args: List[str] = ["--state", self.state_path]
        if self.defer:
            args.append("--defer")
        if self.favor_state:
            args.append("--favor-state")
        return args


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

        defer_config: Optional[DbtDeferConfig] = None
        """dbt slim-CI config — append ``--state <path> [--defer] [--favor-state]``
        to the dbt invocation. When set, the component runs models that changed
        vs the supplied state manifest, deferring ref() resolution to the state's
        tables for unchanged upstream models. Combines naturally with
        ``state_manifest_path`` (which drives the metadata enrichments) —
        typically both point at the same state artifact."""

        # ── Phase 1 additions ─────────────────────────────────────────
        emit_exposures_as_assets: bool = False
        """Emit each dbt exposure as an observable AssetSpec with deps on its
        upstream models (in addition to metadata). Gives downstream lineage:
        if this model breaks, which dashboards are affected?"""

        emit_source_assets: bool = False
        """Emit each dbt source as an observable external AssetSpec (kinds
        `dbt`, `source`). By default sources are only rendered as upstream
        deps of models — this makes them first-class Dagster assets so the
        UI shows them as nodes, freshness policies apply, and downstream
        assets can be selected via `+<source_key>`. When another integration
        (Fivetran, Sling, manual observable_source_asset) declares an asset
        with the same key, Dagster merges the two — dbt's freshness policy
        + kinds layer on top of the upstream materializer's declaration."""

        derive_freshness_policies: bool = False
        """Auto-attach FreshnessPolicy to sources (from `sources.freshness`) and
        models with dbt 1.9+ `config.freshness.build_after`. Also honors
        explicit `meta.dagster.freshness_policy` config."""

        emit_contract_checks: bool = False
        """For models with `config.contract.enforced: true`, emit per-column
        AssetCheckSpecs so contract violations show as failing checks.
        Complements `include_contracts` (which only surfaces the config as
        metadata)."""

        emit_semantic_layer_as_assets: bool = False
        """Emit dbt semantic_models + metrics as observable AssetSpecs (kinds
        `semantic_model` / `metric` for UI icon distinction). semantic_models
        depend on their upstream models; metrics depend on the semantic models
        they aggregate. Ports `et/dbt-semantic-layer-assets` PR branch."""

        enable_materialization_kinds: bool = False
        """Add each model's dbt `materialized` value (`table` / `view` /
        `incremental` / `materialized_view` / `ephemeral` / `seed` /
        `snapshot`) as a Dagster kind so the UI renders distinct icons.
        Additive — existing kinds untouched. Ports part of
        `et/dbt-polish-kinds-explorer-desc` PR branch."""

        auto_trigger_on_freshness_failure: bool = False
        """When combined with `derive_freshness_policies`, also attach
        `AutomationCondition.freshness_failed()` to every asset that gets a
        derived FreshnessPolicy. Dagster will then trigger the rebuild when
        the policy's fail_window elapses.

        Applies to models with `config.freshness.build_after` (dbt 1.9+) and
        sources with `sources.freshness.error_after`. Skipped for assets that
        already carry an `automation_condition` from `meta.dagster.*` (user
        override wins)."""

        state_manifest_path: Optional[str] = None
        """Path to a dbt state manifest.json (or a directory containing one).
        When set alongside ``include_state_explain`` or ``derive_state_tags``,
        each model gets a per-checksum comparison result — matches dbt's own
        ``dbt state explain`` behavior for the checksum path (see
        https://docs.getdbt.com/docs/deploy/dbt-state-about)."""

        include_state_explain: bool = False
        """Requires ``state_manifest_path``. Attach ``dbt_state/state`` (one of
        ``modified`` / ``unchanged`` / ``new``) and ``dbt_state/explanation``
        (human-readable reason) as metadata on each model asset. Users can
        see WHY a model is scheduled to rebuild / reuse in the Dagster UI
        without running ``dbt state explain`` in a terminal."""

        derive_state_tags: bool = False
        """Requires ``state_manifest_path``. Add a ``dbt/state`` tag with the
        same modified / unchanged / new value so asset selections like
        ``tag:dbt/state=modified`` work. Independent of
        ``include_state_explain`` (which adds metadata, not tags)."""

        code_version_strategy: Literal["disabled", "hash", "sqlglot"] = "disabled"
        """How to derive Dagster ``code_version`` per model asset:

        - ``disabled`` (default) — no code_version derived; base behavior.
        - ``hash``     — use dbt's manifest ``checksum.checksum`` directly.
          Fast, zero extra deps. Bumps on ANY file edit (whitespace, comment).
        - ``sqlglot``  — parse the compiled_code with sqlglot, canonicalize
          (strip comments + normalize whitespace), then SHA256 the canonical
          form. Whitespace / comment-only edits do NOT bump code_version;
          only semantic SQL changes do. Optional dep (falls back to ``hash``
          if sqlglot isn't installed or parsing fails). Pairs well with
          ``AutomationCondition.code_version_changed()``."""

        derive_lag_tolerance_automation: bool = False
        """For models with `config.state.lag_tolerance` set: attach an
        AutomationCondition that fires ~lag_tolerance after any upstream is
        newly-updated. Pattern:

            any_deps_match(
              newly_updated().since(cron_tick_passed("*/30 * * * *"))
              & ~executed_with_root_target()
            ).newly_true()
            & ~in_progress()
            & in_latest_time_window()

        where the cron granularity is snapped from lag_tolerance (30m →
        `*/30 * * * *`, 4h → `0 */4 * * *`, etc). Dagster drives the trigger;
        dbt still enforces the exact lag_tolerance gate on build.

        User-supplied `automation_condition` from `meta.dagster.*` wins.
        `auto_trigger_on_freshness_failure` wins over this when both would
        apply — freshness-failed is the more direct SLO tie-in."""

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

            # Materialization kind (table / view / incremental / seed / snapshot / ...)
            if self.enable_materialization_kinds:
                mat = (node.get("config") or {}).get("materialized")
                if mat and isinstance(mat, str):
                    try:
                        existing_kinds = set(enriched.kinds or ())
                        existing_kinds.add(mat)
                        enriched = enriched.merge_attributes(kinds=existing_kinds)
                    except Exception:
                        pass

            # Freshness policy — REAL, not just metadata
            if self.derive_freshness_policies:
                policy = _derive_freshness_policy(node)
                if policy is not None:
                    enriched = enriched.replace_attributes(freshness_policy=policy)
                    # Optionally auto-trigger rebuild on freshness failure.
                    # User-supplied automation_condition (from meta.dagster.*)
                    # wins — only attach the derived one when nothing's set.
                    if self.auto_trigger_on_freshness_failure and (
                        per_model_automation is None and enriched.automation_condition is None
                    ):
                        try:
                            enriched = enriched.replace_attributes(
                                automation_condition=dg.AutomationCondition.freshness_failed()
                            )
                        except Exception:
                            pass

            # State comparison — attach per-model state explain metadata + tag
            # if state_manifest_path is set. Loaded lazily and cached at the
            # instance level so N calls to _enrich_spec don't re-read the file.
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

            # code_version derivation (hash | sqlglot). Applied after
            # freshness so the derived version rides alongside the policy.
            if self.code_version_strategy != "disabled":
                # Adapter type is dbt-project-wide; pull from manifest metadata
                # if available (usually stored at manifest["metadata"]["adapter_type"]).
                adapter_type = (manifest.get("metadata") or {}).get("adapter_type")
                code_version = _derive_code_version(
                    node, self.code_version_strategy, adapter_type=adapter_type
                )
                if code_version:
                    try:
                        enriched = enriched.replace_attributes(code_version=code_version)
                    except Exception:
                        pass

            # lag_tolerance → AutomationCondition (settling-time debounce).
            # Applied AFTER auto_trigger_on_freshness_failure so the latter
            # wins when both would apply.
            if (
                self.derive_lag_tolerance_automation
                and per_model_automation is None
                and enriched.automation_condition is None
            ):
                lag = _lag_tolerance_of(node)
                if lag is not None:
                    cond = _lag_tolerance_automation_condition(lag)
                    if cond is not None:
                        try:
                            enriched = enriched.replace_attributes(
                                automation_condition=cond
                            )
                        except Exception:
                            pass

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

        def _build_semantic_layer_specs(
            self, manifest: dict, base_specs_by_unique_id: Dict[str, dg.AssetKey]
        ) -> List[dg.AssetSpec]:
            """Emit AssetSpec per dbt semantic_model + metric, kinds
            ``semantic_model`` / ``metric``. semantic_models depend on their
            upstream model; metrics depend on the semantic models they aggregate.
            Ports ``et/dbt-semantic-layer-assets`` PR."""
            specs: List[dg.AssetSpec] = []

            # semantic_models — one AssetSpec each, dep on upstream model
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

            # Build lookup for {semantic_model unique_id → AssetKey} so metrics
            # can dep on them by key.
            sm_key_by_uid: Dict[str, dg.AssetKey] = {}
            for sm_uid, sm in (manifest.get("semantic_models") or {}).items():
                name = sm.get("name") or sm_uid.split(".")[-1]
                sm_key_by_uid[sm_uid] = dg.AssetKey(name)

            # metrics — deps on semantic_models they reference
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
            """Emit observable AssetSpec per dbt source. Same key derivation
            as the base translator (schema + table_name → AssetKey). Sources
            with freshness config get a FreshnessPolicy if
            derive_freshness_policies is also set."""
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
                        metadata={
                            _UNIQUE_ID_KEY: src_uid,
                        },
                        freshness_policy=policy,
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

        def _apply_defer_config_to_cli_args(self) -> None:
            """Append defer_config.to_cli_args() to self.cli_args in place.
            Idempotent — doesn't double-append if called twice."""
            if self.defer_config is None:
                return
            defer_flags = self.defer_config.to_cli_args()
            existing = list(self.cli_args or [])
            if "--state" in existing:
                return  # already applied or user set --state manually
            self.cli_args = existing + defer_flags  # type: ignore[assignment]

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

            # Slim CI — append --state / --defer / --favor-state to cli_args
            # before the base runs.
            self._apply_defer_config_to_cli_args()

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
        defer_config: Optional[DbtDeferConfig] = Field(default=None)

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
