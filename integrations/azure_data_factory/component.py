"""Azure Data Factory Workspace Component.

Full Fivetran-shape workspace component for Azure Data Factory:
- `workspace:` block (canonical `Annotated[AzureDataFactoryResource, Resolver(...)]`
  shape — same as dagster-fivetran / dagster-databricks / SnowflakeWorkspace)
- `translation:` callable — per-asset customization hook
  (renames / tag additions / group overrides)
- `@public get_asset_spec(props)` — override in subclasses
- `AzureDataFactoryObjectProps` + `AzureDataFactoryComponentTranslator`
- Per-kind import toggles: `import_pipelines`, `import_triggers`,
  `import_linked_services`, `import_datasets`, `import_data_flows`,
  `import_integration_runtimes` (last 4 marked UNTESTED — Fivetran-shape
  additions land as external assets; validate against your ADF factory
  before relying on them in prod)
- StateBackedComponent — discovery cached to disk

Aligns with SnowflakeWorkspaceComponent / QlikReplicateWorkspaceComponent
/ FivetranAccountComponent.
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import AssetSpec, ComponentLoadContext, Definitions, Model, Resolvable, Resolver
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    TranslationFn,
    TranslationFnResolver,
)
from dagster_shared.record import record
from pydantic import Field


# ── Translator props ─────────────────────────────────────────────────────────


@record
class AzureDataFactoryObjectProps:
    """Data passed to translation callables for each imported ADF object.

    Mirrors `SnowflakeObjectProps` / `FivetranConnectorTableProps` /
    `QlikReplicateObjectProps` — a single record describing the object so
    `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'pipeline', 'trigger', 'linked_service',
            'dataset', 'data_flow', 'integration_runtime'.
        object_name: The ADF object name.
        factory_name: The parent ADF factory.
        resource_group: The Azure resource group.
        subscription_id: The Azure subscription.
        extra: Kind-specific metadata (activities_count for pipelines, etc.).
    """
    object_kind: str
    object_name: str
    factory_name: Optional[str] = None
    resource_group: Optional[str] = None
    subscription_id: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


# ── Resource ──────────────────────────────────────────────────────────────────

class AzureDataFactoryResource(dg.ConfigurableResource):
    """Shared Azure Data Factory connection config.

    Supports both DefaultAzureCredential and explicit Service Principal auth.

    Example:
        ```python
        resources = {
            "adf": AzureDataFactoryResource(
                subscription_id="12345678-...",
                resource_group_name="my-rg",
                factory_name="my-adf",
                tenant_id_env_var="AZURE_TENANT_ID",
                client_id_env_var="AZURE_CLIENT_ID",
                client_secret_env_var="AZURE_CLIENT_SECRET",
            )
        }
        ```
    """

    subscription_id: str = Field(description="Azure subscription ID")
    resource_group_name: str = Field(description="Azure resource group name")
    factory_name: str = Field(description="Azure Data Factory name")
    tenant_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Azure AD tenant ID (optional — uses DefaultAzureCredential if absent)",
    )
    client_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Azure AD client/application ID (optional)",
    )
    client_secret_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Azure AD client secret (optional)",
    )

    def get_client(self):
        """Return an authenticated DataFactoryManagementClient."""
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient

        if self.tenant_id_env_var and self.client_id_env_var and self.client_secret_env_var:
            credential = ClientSecretCredential(
                tenant_id=dg.EnvVar(self.tenant_id_env_var).get_value(),
                client_id=dg.EnvVar(self.client_id_env_var).get_value(),
                client_secret=dg.EnvVar(self.client_secret_env_var).get_value(),
            )
        else:
            credential = DefaultAzureCredential()

        return DataFactoryManagementClient(credential, self.subscription_id)


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _get_adf_client(
    subscription_id: str,
    tenant_id: Optional[str],
    client_id: Optional[str],
    client_secret: Optional[str],
):
    """Build an ADF management client from explicit credential values."""
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient

    if tenant_id and client_id and client_secret:
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        credential = DefaultAzureCredential()

    return DataFactoryManagementClient(credential, subscription_id)


def _matches_filters(
    name: str,
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    filter_by_tags: Optional[str],
    tags: Optional[Dict[str, str]] = None,
) -> bool:
    """Return True if *name* passes all configured filters."""
    if filter_by_name_pattern and not re.search(filter_by_name_pattern, name):
        return False
    if exclude_name_pattern and re.search(exclude_name_pattern, name):
        return False
    if filter_by_tags and tags:
        required_keys = [k.strip() for k in filter_by_tags.split(",")]
        if not all(k in tags for k in required_keys):
            return False
    return True


def _fetch_pipelines(
    client,
    resource_group_name: str,
    factory_name: str,
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    filter_by_tags: Optional[str],
) -> List[Dict[str, Any]]:
    """List all matching pipelines and return serialisable dicts."""
    result = []
    for pipeline in client.pipelines.list_by_factory(resource_group_name, factory_name):
        name = pipeline.name or ""
        if not _matches_filters(
            name,
            filter_by_name_pattern,
            exclude_name_pattern,
            filter_by_tags,
        ):
            continue
        # Count activities safely
        activities = getattr(pipeline, "activities", None) or []
        result.append(
            {
                "name": name,
                "description": getattr(pipeline, "description", None) or "",
                "parameters": list((getattr(pipeline, "parameters", None) or {}).keys()),
                "activities_count": len(activities),
            }
        )
    return result


def _fetch_triggers(
    client,
    resource_group_name: str,
    factory_name: str,
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    filter_by_tags: Optional[str],
) -> List[str]:
    """List all matching trigger names."""
    result = []
    for trigger in client.triggers.list_by_factory(resource_group_name, factory_name):
        name = trigger.name or ""
        if _matches_filters(name, filter_by_name_pattern, exclude_name_pattern, filter_by_tags):
            result.append(name)
    return result


# ── UNTESTED: 4 new object-kind fetch helpers ──────────────────────────────
# Follow the standard Azure SDK naming convention (`client.<resource>.list_by_factory`).
# If your ADF SDK version deviates, adjust the accessor name. These emit
# EXTERNAL ASSETS only (no runtime action). Validate against your factory
# before relying on them in prod.


def _fetch_by_kind(client_attr, resource_group_name: str, factory_name: str,
                   filter_by_name_pattern: Optional[str],
                   exclude_name_pattern: Optional[str],
                   filter_by_tags: Optional[str]) -> List[Dict[str, Any]]:
    """Generic best-effort list_by_factory over any ADF client attribute
    (linked_services / datasets / data_flows / integration_runtimes)."""
    result: List[Dict[str, Any]] = []
    if client_attr is None:
        return result
    try:
        iterator = client_attr.list_by_factory(resource_group_name, factory_name)
    except Exception:  # noqa: BLE001 — SDK naming may differ across versions
        return result
    for obj in iterator:
        name = getattr(obj, "name", "") or ""
        if not _matches_filters(
            name, filter_by_name_pattern, exclude_name_pattern, filter_by_tags,
        ):
            continue
        result.append({
            "name": name,
            "description": getattr(obj, "description", None) or "",
            # Best-effort — kind-specific detail fields land in `extra`.
            "type_name": getattr(getattr(obj, "properties", None), "type", None) or "",
        })
    return result


def _fetch_linked_services(client, resource_group_name, factory_name,
                           filter_by_name_pattern, exclude_name_pattern, filter_by_tags):
    """UNTESTED: list ADF linked services (data source / sink connections)."""
    return _fetch_by_kind(
        getattr(client, "linked_services", None),
        resource_group_name, factory_name,
        filter_by_name_pattern, exclude_name_pattern, filter_by_tags,
    )


def _fetch_datasets(client, resource_group_name, factory_name,
                    filter_by_name_pattern, exclude_name_pattern, filter_by_tags):
    """UNTESTED: list ADF datasets (schemas over linked services)."""
    return _fetch_by_kind(
        getattr(client, "datasets", None),
        resource_group_name, factory_name,
        filter_by_name_pattern, exclude_name_pattern, filter_by_tags,
    )


def _fetch_data_flows(client, resource_group_name, factory_name,
                      filter_by_name_pattern, exclude_name_pattern, filter_by_tags):
    """UNTESTED: list ADF Mapping Data Flows (visual transformations)."""
    return _fetch_by_kind(
        getattr(client, "data_flows", None),
        resource_group_name, factory_name,
        filter_by_name_pattern, exclude_name_pattern, filter_by_tags,
    )


def _fetch_integration_runtimes(client, resource_group_name, factory_name,
                                filter_by_name_pattern, exclude_name_pattern, filter_by_tags):
    """UNTESTED: list ADF Integration Runtimes (SSIS / Azure IR / Self-hosted IR)."""
    return _fetch_by_kind(
        getattr(client, "integration_runtimes", None),
        resource_group_name, factory_name,
        filter_by_name_pattern, exclude_name_pattern, filter_by_tags,
    )


# ── assets_by_pipeline_name helpers ───────────────────────────────────────────

def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_pipeline_overrides(
    default_spec: dg.AssetSpec,
    pipeline_name: str,
    overrides: Optional[dict],
) -> list:
    """Apply assets_by_pipeline_name overrides. Returns list (1 usually, >1 if one pipeline -> multiple assets)."""
    if not overrides or pipeline_name not in overrides:
        return [default_spec]
    ov = overrides[pipeline_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Core definition builder (no network calls) ────────────────────────────────

def _build_adf_defs(
    pipelines: List[Dict[str, Any]],
    trigger_names: List[str],
    subscription_id: str,
    resource_group_name: str,
    factory_name: str,
    tenant_id: Optional[str],
    client_id: Optional[str],
    client_secret: Optional[str],
    group_name: str,
    import_pipelines: bool,
    import_triggers: bool,
    generate_sensor: bool,
    poll_interval_seconds: int,
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    assets_by_pipeline_name: Optional[dict] = None,
    # Comprehensive options — all default to backward-compatible no-ops
    pipeline_parameters: Optional[Dict[str, Any]] = None,
    partition_type: Optional[str] = None,
    partition_start: Optional[str] = None,
    partition_values: Optional[List[str]] = None,
    partition_parameter_name: Optional[str] = None,
    max_wait_seconds: int = 3600,
    run_poll_interval_seconds: int = 30,
    wait_for_completion: bool = True,
    capture_activity_metadata: bool = True,
    owners: Optional[List[str]] = None,
    asset_tags: Optional[Dict[str, str]] = None,
    extra_kinds: Optional[List[str]] = None,
    freshness_max_lag_minutes: Optional[int] = None,
    freshness_cron: Optional[str] = None,
    upstream_asset_keys: Optional[List[str]] = None,
    retry_policy_max_retries: Optional[int] = None,
    retry_policy_delay_seconds: Optional[int] = None,
    retry_policy_backoff: str = "exponential",
) -> dg.Definitions:
    """Build Dagster Definitions from pre-fetched ADF metadata (no network calls)."""
    assets: list = []
    sensors: list = []

    # Build the PartitionsDefinition once (shared across all pipeline assets)
    _partitions_def = None
    if partition_type == "daily":
        _partitions_def = dg.DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    elif partition_type == "weekly":
        _partitions_def = dg.WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    elif partition_type == "monthly":
        _partitions_def = dg.MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    elif partition_type == "hourly":
        _partitions_def = dg.HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01-00:00")
    elif partition_type == "static":
        _partitions_def = dg.StaticPartitionsDefinition(partition_values or [])

    # Build the FreshnessPolicy once (legacy API; safe across versions)
    _freshness = None
    if freshness_max_lag_minutes:
        try:
            _freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=freshness_max_lag_minutes,
                cron_schedule=freshness_cron,
            )
        except Exception:
            _freshness = None  # newer Dagster removed this; ignore silently

    # Build the RetryPolicy once
    _retry_policy = None
    if retry_policy_max_retries is not None:
        from dagster import Backoff, RetryPolicy
        _retry_policy = RetryPolicy(
            max_retries=retry_policy_max_retries,
            delay=retry_policy_delay_seconds or 1,
            backoff=Backoff[retry_policy_backoff.upper()],
        )

    # Default kinds for ADF pipelines + any user additions
    _kinds = {"azure", "adf", *(extra_kinds or [])}

    # Helper for the ADF Monitor portal deeplink — always useful in metadata
    def _monitor_url(run_id: str) -> str:
        factory_uri = (
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}"
            f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        )
        return f"https://adf.azure.com/en/monitoring/pipelineruns/{run_id}?factory={factory_uri}"

    # ── Pipeline assets ────────────────────────────────────────────────────────
    if import_pipelines:
        for pipeline_meta in pipelines:
            pipeline_name = pipeline_meta["name"]

            # Build the default AssetSpec for this pipeline
            spec_kwargs = dict(
                key=dg.AssetKey([f"adf_pipeline_{pipeline_name}"]),
                description=pipeline_meta.get("description") or f"ADF pipeline: {pipeline_name}",
                group_name=group_name,
                metadata={
                    "pipeline_name": dg.MetadataValue.text(pipeline_name),
                    "factory_name": dg.MetadataValue.text(factory_name),
                    "resource_group": dg.MetadataValue.text(resource_group_name),
                    "activities_count": dg.MetadataValue.int(
                        pipeline_meta.get("activities_count", 0)
                    ),
                    "parameters": dg.MetadataValue.text(
                        ", ".join(pipeline_meta.get("parameters", [])) or "(none)"
                    ),
                },
                kinds=_kinds,
            )
            if owners:
                spec_kwargs["owners"] = owners
            if asset_tags:
                spec_kwargs["tags"] = asset_tags
            if _partitions_def is not None:
                spec_kwargs["partitions_def"] = _partitions_def
            if _freshness is not None:
                spec_kwargs["freshness_policy"] = _freshness
            if upstream_asset_keys:
                spec_kwargs["deps"] = [dg.AssetKey.from_user_string(k) for k in upstream_asset_keys]
            default_spec = dg.AssetSpec(**spec_kwargs)

            # Apply any user overrides (may expand to multiple specs)
            expanded_specs = _apply_pipeline_overrides(
                default_spec, pipeline_name, assets_by_pipeline_name
            )

            # Build a spec_key_path tuple -> pipeline_name mapping for the execution body
            spec_key_to_pipeline: Dict[tuple, str] = {
                tuple(spec.key.path): pipeline_name for spec in expanded_specs
            }

            # Capture loop-variable values via a factory closure. Putting them as
            # default arguments confuses Dagster's multi_asset, which treats every
            # parameter as an asset input.
            def _make_pipeline_asset(
                _pipeline_name=pipeline_name,
                _subscription_id=subscription_id,
                _resource_group_name=resource_group_name,
                _factory_name=factory_name,
                _tenant_id=tenant_id,
                _client_id=client_id,
                _client_secret=client_secret,
                _spec_key_to_pipeline=spec_key_to_pipeline,
                _pipeline_parameters=pipeline_parameters,
                _partition_parameter_name=partition_parameter_name,
                _max_wait_seconds=max_wait_seconds,
                _poll_interval=run_poll_interval_seconds,
                _wait_for_completion=wait_for_completion,
                _capture_activity_metadata=capture_activity_metadata,
            ):
                @dg.multi_asset(
                    specs=expanded_specs,
                    name=f"adf_pipeline_{_pipeline_name}",
                    retry_policy=_retry_policy,
                )
                def pipeline_multi_asset(context: dg.AssetExecutionContext):
                    adf_client = _get_adf_client(
                        _subscription_id, _tenant_id, _client_id, _client_secret
                    )

                    # Determine which ADF pipelines need to run for the selected asset keys
                    selected_keys = set(
                        tuple(k.path) for k in context.selected_asset_keys
                    )
                    pipelines_to_run: Dict[str, list] = {}
                    for key_path, p_name in _spec_key_to_pipeline.items():
                        if key_path in selected_keys:
                            pipelines_to_run.setdefault(p_name, []).append(key_path)

                    # Build the ADF pipeline parameters dict — user-provided +
                    # auto-injected partition_key (when partitioned)
                    adf_params: Dict[str, Any] = dict(_pipeline_parameters or {})
                    if context.has_partition_key:
                        pkey = context.partition_key
                        pname = _partition_parameter_name or "partition_key"
                        adf_params.setdefault(pname, pkey)
                        context.log.info(
                            f"partitioned run: passing {pname}={pkey} to ADF"
                        )

                    for p_name, key_paths in pipelines_to_run.items():
                        create_kwargs: Dict[str, Any] = {}
                        if adf_params:
                            create_kwargs["parameters"] = adf_params
                        run_response = adf_client.pipelines.create_run(
                            _resource_group_name,
                            _factory_name,
                            p_name,
                            **create_kwargs,
                        )
                        run_id = run_response.run_id
                        monitor_url = _monitor_url(run_id)
                        context.log.info(f"ADF pipeline run started. Run ID: {run_id}")
                        context.log.info(f"Monitor: {monitor_url}")

                        if not _wait_for_completion:
                            # Fire-and-forget — yield immediately
                            for key_path in key_paths:
                                yield dg.MaterializeResult(
                                    asset_key=dg.AssetKey(list(key_path)),
                                    metadata={
                                        "run_id": dg.MetadataValue.text(run_id),
                                        "status": dg.MetadataValue.text("Submitted"),
                                        "pipeline_name": dg.MetadataValue.text(p_name),
                                        "monitor_url": dg.MetadataValue.url(monitor_url),
                                        "parameters": dg.MetadataValue.json(adf_params),
                                    },
                                )
                            continue

                        elapsed = 0
                        pipeline_run = None
                        while elapsed < _max_wait_seconds:
                            pipeline_run = adf_client.pipeline_runs.get(
                                _resource_group_name, _factory_name, run_id,
                            )
                            status = pipeline_run.status
                            context.log.info(f"  poll: {p_name} status={status} elapsed={elapsed}s")

                            if status in ("Succeeded", "Failed", "Cancelled"):
                                duration_seconds = 0.0
                                if pipeline_run.run_end and pipeline_run.run_start:
                                    duration_seconds = (
                                        pipeline_run.run_end - pipeline_run.run_start
                                    ).total_seconds()

                                run_metadata: Dict[str, Any] = {
                                    "run_id": dg.MetadataValue.text(run_id),
                                    "status": dg.MetadataValue.text(status),
                                    "pipeline_name": dg.MetadataValue.text(p_name),
                                    "start_time": dg.MetadataValue.text(str(pipeline_run.run_start)),
                                    "end_time": dg.MetadataValue.text(str(pipeline_run.run_end)),
                                    "duration_seconds": dg.MetadataValue.float(duration_seconds),
                                    "monitor_url": dg.MetadataValue.url(monitor_url),
                                    "parameters": dg.MetadataValue.json(adf_params),
                                }

                                # Per-activity metadata: each activity's status, duration, and any error
                                if _capture_activity_metadata and pipeline_run.run_start and pipeline_run.run_end:
                                    try:
                                        from azure.mgmt.datafactory.models import RunFilterParameters
                                        activity_runs = adf_client.activity_runs.query_by_pipeline_run(
                                            _resource_group_name, _factory_name, run_id,
                                            RunFilterParameters(
                                                last_updated_after=pipeline_run.run_start,
                                                last_updated_before=pipeline_run.run_end,
                                            ),
                                        )
                                        activities_summary = []
                                        for ar in (activity_runs.value or []):
                                            ar_dur = 0.0
                                            if ar.activity_run_end and ar.activity_run_start:
                                                ar_dur = (ar.activity_run_end - ar.activity_run_start).total_seconds()
                                            activities_summary.append({
                                                "name": ar.activity_name,
                                                "type": ar.activity_type,
                                                "status": ar.status,
                                                "duration_seconds": ar_dur,
                                                "error": (ar.error or {}).get("message") if isinstance(ar.error, dict) else (str(ar.error) if ar.error else None),
                                                "output_keys": list((ar.output or {}).keys()) if isinstance(ar.output, dict) else None,
                                            })
                                            # Stream a per-activity log line so users see them in dg
                                            context.log.info(
                                                f"  activity: {ar.activity_name} ({ar.activity_type}) "
                                                f"status={ar.status} duration={ar_dur:.1f}s"
                                            )
                                        run_metadata["activities"] = dg.MetadataValue.json(activities_summary)
                                        run_metadata["activity_count"] = dg.MetadataValue.int(len(activities_summary))
                                        failed_activities = [a["name"] for a in activities_summary if a["status"] == "Failed"]
                                        if failed_activities:
                                            run_metadata["failed_activities"] = dg.MetadataValue.json(failed_activities)
                                    except Exception as _exc:
                                        context.log.warning(f"  could not fetch activity metadata: {_exc}")

                                if status == "Failed":
                                    error_msg = getattr(pipeline_run, "message", None) or "Pipeline failed"
                                    run_metadata["error"] = dg.MetadataValue.text(error_msg)
                                    # Yield the materialization with status before raising — so the
                                    # failure metadata is recorded in the catalog, not lost.
                                    for key_path in key_paths:
                                        yield dg.MaterializeResult(
                                            asset_key=dg.AssetKey(list(key_path)),
                                            metadata={**run_metadata, "outcome": dg.MetadataValue.text("failed")},
                                        )
                                    raise Exception(
                                        f"ADF pipeline '{p_name}' failed: {error_msg} (run_id={run_id})"
                                    )

                                for key_path in key_paths:
                                    yield dg.MaterializeResult(
                                        asset_key=dg.AssetKey(list(key_path)),
                                        metadata=run_metadata,
                                    )
                                break

                            time.sleep(_poll_interval)
                            elapsed += _poll_interval

                        else:
                            context.log.warning(
                                f"ADF pipeline run timed out after {_max_wait_seconds}s"
                            )
                            for key_path in key_paths:
                                yield dg.MaterializeResult(
                                    asset_key=dg.AssetKey(list(key_path)),
                                    metadata={
                                        "run_id": dg.MetadataValue.text(run_id),
                                        "status": dg.MetadataValue.text("Timeout"),
                                        "pipeline_name": dg.MetadataValue.text(p_name),
                                        "monitor_url": dg.MetadataValue.url(monitor_url),
                                        "max_wait_seconds": dg.MetadataValue.int(_max_wait_seconds),
                                    },
                                )

                return pipeline_multi_asset

            assets.append(_make_pipeline_asset())

    # ── Trigger assets ─────────────────────────────────────────────────────────
    if import_triggers:
        for trigger_name in trigger_names:

            @dg.asset(retry_policy=_retry_policy,
                name=f"adf_trigger_{trigger_name}",
                group_name=group_name,
                description=f"ADF trigger: {trigger_name}",
                metadata={
                    "trigger_name": dg.MetadataValue.text(trigger_name),
                    "factory_name": dg.MetadataValue.text(factory_name),
                    "resource_group": dg.MetadataValue.text(resource_group_name),
                },
                kinds={"azure", "adf"},
            )
            def trigger_asset(
                context: dg.AssetExecutionContext,
                _trigger_name: str = trigger_name,
                _subscription_id: str = subscription_id,
                _resource_group_name: str = resource_group_name,
                _factory_name: str = factory_name,
                _tenant_id: Optional[str] = tenant_id,
                _client_id: Optional[str] = client_id,
                _client_secret: Optional[str] = client_secret,
            ):
                """Start an Azure Data Factory trigger (no-op if already running)."""
                adf_client = _get_adf_client(
                    _subscription_id, _tenant_id, _client_id, _client_secret
                )

                trigger = adf_client.triggers.get(
                    _resource_group_name,
                    _factory_name,
                    _trigger_name,
                )
                runtime_state = getattr(trigger, "runtime_state", "Unknown")
                context.log.info(f"Trigger runtime state: {runtime_state}")

                if runtime_state != "Started":
                    adf_client.triggers.begin_start(
                        _resource_group_name,
                        _factory_name,
                        _trigger_name,
                    ).result()
                    context.log.info(f"Trigger {_trigger_name} started")
                else:
                    context.log.info(f"Trigger {_trigger_name} already running")

                return dg.MaterializeResult(
                    metadata={
                        "trigger_name": dg.MetadataValue.text(_trigger_name),
                        "runtime_state": dg.MetadataValue.text("Started"),
                        "trigger_type": dg.MetadataValue.text(
                            getattr(trigger, "type", "Unknown") or "Unknown"
                        ),
                    }
                )

            assets.append(trigger_asset)

    # ── Observation sensor ─────────────────────────────────────────────────────
    if generate_sensor and (import_pipelines or import_triggers):

        @dg.sensor(
            name=f"{group_name}_observation_sensor",
            minimum_interval_seconds=poll_interval_seconds,
        )
        def adf_observation_sensor(context: dg.SensorEvaluationContext):
            """Observe Azure Data Factory pipeline runs and trigger runs."""
            from azure.mgmt.datafactory.models import RunFilterParameters

            adf_client = _get_adf_client(subscription_id, tenant_id, client_id, client_secret)

            cursor = context.cursor
            last_check = (
                datetime.fromisoformat(cursor)
                if cursor
                else datetime.utcnow() - timedelta(hours=1)
            )
            now = datetime.utcnow()

            filter_params = RunFilterParameters(
                last_updated_after=last_check,
                last_updated_before=now,
            )

            pipeline_runs = adf_client.pipeline_runs.query_by_factory(
                resource_group_name, factory_name, filter_params
            )

            for run in pipeline_runs.value:
                if run.status not in ("Succeeded", "Failed", "Cancelled"):
                    continue
                run_pipeline_name = run.pipeline_name or ""
                if not _matches_filters(
                    run_pipeline_name,
                    filter_by_name_pattern,
                    exclude_name_pattern,
                    None,
                ):
                    continue

                duration = 0.0
                if run.run_end and run.run_start:
                    duration = (run.run_end - run.run_start).total_seconds()

                meta: Dict[str, Any] = {
                    "run_id": dg.MetadataValue.text(run.run_id or ""),
                    "status": dg.MetadataValue.text(run.status),
                    "pipeline_name": dg.MetadataValue.text(run_pipeline_name),
                    "start_time": dg.MetadataValue.text(str(run.run_start)),
                    "end_time": dg.MetadataValue.text(str(run.run_end)),
                    "duration_seconds": dg.MetadataValue.float(duration),
                }
                if run.status == "Failed" and getattr(run, "message", None):
                    meta["error"] = dg.MetadataValue.text(run.message)

                yield dg.AssetMaterialization(
                    asset_key=f"adf_pipeline_{run_pipeline_name}",
                    metadata=meta,
                )

            # Log trigger run activity
            trigger_runs = adf_client.trigger_runs.query_by_factory(
                resource_group_name, factory_name, filter_params
            )
            for run in trigger_runs.value:
                if run.status in ("Succeeded", "Failed"):
                    context.log.info(
                        f"Trigger run: {run.trigger_name} — Status: {run.status} — "
                        f"Time: {run.trigger_run_timestamp}"
                    )

            context.update_cursor(now.isoformat())

        sensors.append(adf_observation_sensor)

    return dg.Definitions(assets=assets, sensors=sensors)


# ── UNTESTED: emit external assets for 4 new object kinds ────────────────────
# Fivetran-shape additions. Discovery calls the ADF Management API; each object
# becomes a Dagster external asset (no materialization action — read-only view
# in the catalog). Validate against your factory before relying on them.


def _emit_external_assets(
    rows: List[Dict[str, Any]],
    kind: str,
    factory_name: str,
    resource_group_name: str,
    subscription_id: str,
    group_name: str,
    key_prefix: List[str],
    extra_kinds: Optional[List[str]] = None,
    apply_translation=None,  # optional callable: (base_spec, props) -> spec
) -> List[AssetSpec]:
    """Turn a list of {name, description, type_name} rows into AssetSpecs."""
    kinds = {"azure", "adf", kind, *(extra_kinds or [])}
    specs: List[AssetSpec] = []
    for row in rows:
        name = row["name"]
        base = AssetSpec(
            key=dg.AssetKey([*key_prefix, f"adf_{kind}_{name}"]),
            description=row.get("description") or f"ADF {kind}: {name}",
            group_name=group_name,
            kinds=kinds,
            metadata={
                "adf/kind":            dg.MetadataValue.text(kind),
                "adf/name":            dg.MetadataValue.text(name),
                "adf/type":            dg.MetadataValue.text(row.get("type_name") or "unknown"),
                "adf/factory":         dg.MetadataValue.text(factory_name),
                "adf/resource_group":  dg.MetadataValue.text(resource_group_name),
                "adf/subscription_id": dg.MetadataValue.text(subscription_id),
                "adf/validation":      dg.MetadataValue.text("UNTESTED — validate against your factory"),
            },
        )
        if apply_translation is not None:
            props = AzureDataFactoryObjectProps(
                object_kind=kind,
                object_name=name,
                factory_name=factory_name,
                resource_group=resource_group_name,
                subscription_id=subscription_id,
                extra=row,
            )
            base = apply_translation(base, props)
        specs.append(base)
    return specs


# ── Component (StateBackedComponent, flagship shape) ────────────────────────


@public
class AzureDataFactoryComponent(StateBackedComponent, Model, Resolvable):
    """Azure Data Factory workspace component — one Dagster asset per ADF object.

    Full Fivetran-shape workspace: canonical `workspace:` block
    (`AzureDataFactoryResource`), `translation:` callable, `@public
    get_asset_spec` hook, StateBackedComponent discovery caching.
    Aligns with `SnowflakeWorkspaceComponent` / `FivetranAccountComponent`
    / `QlikReplicateWorkspaceComponent`.

    Example (canonical `workspace:` block — matches dagster-fivetran):

        ```yaml
        type: dagster_community_components.AzureDataFactoryComponent
        attributes:
          workspace:
            subscription_id: "{{ env.AZURE_SUBSCRIPTION_ID }}"
            resource_group_name: my-resource-group
            factory_name: my-adf
            tenant_id_env_var: AZURE_TENANT_ID
            client_id_env_var: AZURE_CLIENT_ID
            client_secret_env_var: AZURE_CLIENT_SECRET
          import_pipelines: true
          import_triggers: false
          # Fivetran-shape untested additions — external assets only:
          import_linked_services: true
          import_datasets: true
          import_data_flows: true
          import_integration_runtimes: true
          polling_sensor: true
          poll_interval_seconds: 60
        ```

    Populate the discovery cache:
        dagster dev                        # automatic in dev
        dg utils refresh-defs-state        # CI/CD / image build
    """

    # ── Connection: workspace: block IS an AzureDataFactoryResource ──
    # Canonical shape — mirrors dagster-fivetran / dagster-databricks /
    # dagster-powerbi / SnowflakeWorkspaceComponent.
    workspace: Annotated[
        AzureDataFactoryResource,
        Resolver(
            lambda context, model: AzureDataFactoryResource(
                **resolve_fields(model, AzureDataFactoryResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Azure Data Factory connection as an AzureDataFactoryResource. "
            "Fields: subscription_id + resource_group_name + factory_name + "
            "optional {tenant_id_env_var, client_id_env_var, client_secret_env_var} "
            "(Service Principal) OR omit for DefaultAzureCredential. Secrets "
            "typically arrive via `{{ env.XXX }}` templating in defs.yaml."
        ),
    )

    # ── Translation hook ─────────────────────────────────────────────────
    translation: Annotated[
        Optional[TranslationFn[AzureDataFactoryObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate ADF object properties into Dagster "
            "asset specs. Called for each imported pipeline / trigger / "
            "linked_service / dataset / data_flow / integration_runtime. "
            "Signature: `def fn(props: AzureDataFactoryObjectProps) -> AssetSpec`. "
            "If unset, the base translator's default AssetSpec is used."
        ),
    )

    # ── Import toggles ──────────────────────────────────────────────────
    import_pipelines: bool = Field(default=True, description="Import ADF pipelines as materializable assets (default true).")
    import_triggers: bool = Field(default=False, description="Import ADF triggers as observable external assets.")

    # ── UNTESTED: 4 new object kinds ────────────────────────────────────
    # Emit as external assets only (no runtime action). Follows the standard
    # Azure SDK naming (`client.<resource>.list_by_factory`). Validate against
    # your ADF factory before relying on them in prod — no live customer
    # validation as of the v0.10.78 ship.
    import_linked_services: bool = Field(
        default=False,
        description=(
            "**UNTESTED.** Import ADF linked services (source/sink connection "
            "configurations) as external Dagster assets. Read-only surface — "
            "no runtime action. Validate against your factory before use."
        ),
    )
    import_datasets: bool = Field(
        default=False,
        description=(
            "**UNTESTED.** Import ADF datasets (schemas over linked services) "
            "as external Dagster assets. Read-only surface. Validate before use."
        ),
    )
    import_data_flows: bool = Field(
        default=False,
        description=(
            "**UNTESTED.** Import ADF Mapping Data Flows (visual "
            "transformations) as external Dagster assets. Read-only surface. "
            "Validate before use."
        ),
    )
    import_integration_runtimes: bool = Field(
        default=False,
        description=(
            "**UNTESTED.** Import ADF Integration Runtimes (SSIS / Azure IR / "
            "Self-hosted IR) as external Dagster assets. Read-only surface. "
            "Validate before use."
        ),
    )

    # ── Filtering ───────────────────────────────────────────────────────
    filter_by_name_pattern: Optional[str] = Field(default=None, description="Regex to filter entities by name.")
    exclude_name_pattern: Optional[str] = Field(default=None, description="Regex to exclude entities by name.")
    filter_by_tags: Optional[str] = Field(default=None, description="Comma-separated tag keys entities must carry.")

    # ── Observation sensor ──────────────────────────────────────────────
    polling_sensor: bool = Field(
        default=True,
        description=(
            "Emit a polling sensor that observes ADF pipeline-run status and "
            "emits AssetObservation events. Matches the `polling_sensor` "
            "convention on FivetranAccountComponent / SnowflakeWorkspaceComponent "
            "/ QlikReplicateWorkspaceComponent."
        ),
    )
    poll_interval_seconds: int = Field(default=60, description="Sensor polling interval (seconds).")

    # ── Presentation ────────────────────────────────────────────────────
    group_name: str = Field(default="azure_data_factory", description="Asset group.")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    extra_kinds: Optional[List[str]] = Field(default=None, description="Extra `dagster/kind/*` tags applied to every asset.")
    asset_key_prefix: List[str] = Field(
        default_factory=list,
        description="Optional key prefix. Every asset key gets `[<prefix>..., adf_<kind>_<name>]`.",
    )

    # ── Per-pipeline asset overrides (legacy hook, still supported) ─────
    assets_by_pipeline_name: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Override or expand AssetSpecs for specific ADF pipelines. Keys "
            "are ADF pipeline names; values are either a single spec-override "
            "dict OR a list of them (one pipeline → multiple Dagster assets). "
            "Supported override keys: key, description, group_name, metadata, "
            "tags, kinds, deps. For finer-grained per-asset customization, "
            "prefer the `translation:` callable — this legacy hook stays for "
            "backward compatibility with existing YAML."
        ),
    )

    # ── Pipeline-execution config ───────────────────────────────────────
    pipeline_parameters: Optional[Dict[str, Any]] = Field(default=None)
    partition_parameter_name: Optional[str] = Field(default=None)
    max_wait_seconds: int = Field(default=3600)
    run_poll_interval_seconds: int = Field(default=30)
    wait_for_completion: bool = Field(default=True)
    capture_activity_metadata: bool = Field(default=True)

    # ── Partitions ──────────────────────────────────────────────────────
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[List[str]] = Field(default=None)

    # ── Freshness / retry ───────────────────────────────────────────────
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    upstream_asset_keys: Optional[List[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    # ── State backing ───────────────────────────────────────────────────
    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Override per-deploy for Dagster Cloud."
        ),
    )

    @public
    def get_asset_spec(self, props: AzureDataFactoryObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given ADF object.

        Override in a subclass to customize how ADF objects are converted
        to Dagster asset specs. Default delegates to the configured
        translator, which respects the `translation:` field.
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "AzureDataFactoryComponentTranslator":
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = AzureDataFactoryComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        default_key = (
            f"{self.__class__.__name__}"
            f"[{self.workspace.subscription_id}/{self.workspace.resource_group_name}"
            f"/{self.workspace.factory_name}]"
        )
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(self, base_spec: AssetSpec, props: AzureDataFactoryObjectProps) -> AssetSpec:
        """Fold the `translation:` callable into a base AssetSpec. When no
        callable is set, returns `base_spec` unchanged."""
        if self.translation is None:
            return base_spec
        result = self.get_asset_spec(props)
        return result

    # ── Discovery (state-backed) ────────────────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        """Call ADF Management API and cache all requested object kinds to disk."""
        client = self.workspace.get_client()
        rg = self.workspace.resource_group_name
        fac = self.workspace.factory_name

        state: Dict[str, Any] = {"pipelines": [], "triggers": [],
                                 "linked_services": [], "datasets": [],
                                 "data_flows": [], "integration_runtimes": []}

        if self.import_pipelines:
            state["pipelines"] = _fetch_pipelines(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )
        if self.import_triggers:
            state["triggers"] = _fetch_triggers(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )
        # UNTESTED kinds:
        if self.import_linked_services:
            state["linked_services"] = _fetch_linked_services(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )
        if self.import_datasets:
            state["datasets"] = _fetch_datasets(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )
        if self.import_data_flows:
            state["data_flows"] = _fetch_data_flows(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )
        if self.import_integration_runtimes:
            state["integration_runtimes"] = _fetch_integration_runtimes(
                client, rg, fac,
                self.filter_by_name_pattern, self.exclude_name_pattern, self.filter_by_tags,
            )

        state_path.write_text(json.dumps(state, indent=2))

    def build_defs_from_state(
        self, context: ComponentLoadContext, state_path: Optional[Path]
    ) -> Definitions:
        """Build assets from cached ADF metadata — no network calls."""
        if state_path is None or not state_path.exists():
            if hasattr(context, "log"):
                context.log.warning(  # type: ignore[union-attr]
                    "AzureDataFactoryComponent: no cached state. Run "
                    "`dg utils refresh-defs-state` or `dagster dev` to populate."
                )
            return Definitions()

        state = json.loads(state_path.read_text())
        pipelines = state.get("pipelines", [])
        trigger_names = [
            t["name"] if isinstance(t, dict) else t
            for t in state.get("triggers", [])
        ]
        linked_services = state.get("linked_services", [])
        datasets = state.get("datasets", [])
        data_flows = state.get("data_flows", [])
        integration_runtimes = state.get("integration_runtimes", [])

        # Resolve auth for the runtime (pipeline-run trigger + polling sensor).
        _ten = os.environ.get(self.workspace.tenant_id_env_var) if self.workspace.tenant_id_env_var else None
        _cid = os.environ.get(self.workspace.client_id_env_var) if self.workspace.client_id_env_var else None
        _sec = os.environ.get(self.workspace.client_secret_env_var) if self.workspace.client_secret_env_var else None

        base_defs = _build_adf_defs(
            pipelines=pipelines,
            trigger_names=trigger_names,
            subscription_id=self.workspace.subscription_id,
            resource_group_name=self.workspace.resource_group_name,
            factory_name=self.workspace.factory_name,
            tenant_id=_ten, client_id=_cid, client_secret=_sec,
            group_name=self.group_name,
            import_pipelines=self.import_pipelines,
            import_triggers=self.import_triggers,
            generate_sensor=self.polling_sensor,       # renamed field, same signature
            poll_interval_seconds=self.poll_interval_seconds,
            filter_by_name_pattern=self.filter_by_name_pattern,
            exclude_name_pattern=self.exclude_name_pattern,
            assets_by_pipeline_name=self.assets_by_pipeline_name,
            pipeline_parameters=self.pipeline_parameters,
            partition_type=self.partition_type,
            partition_start=self.partition_start,
            partition_values=self.partition_values,
            partition_parameter_name=self.partition_parameter_name,
            max_wait_seconds=self.max_wait_seconds,
            run_poll_interval_seconds=self.run_poll_interval_seconds,
            wait_for_completion=self.wait_for_completion,
            capture_activity_metadata=self.capture_activity_metadata,
            owners=self.owners,
            asset_tags=self.asset_tags,
            extra_kinds=self.extra_kinds,
            freshness_max_lag_minutes=self.freshness_max_lag_minutes,
            freshness_cron=self.freshness_cron,
            upstream_asset_keys=self.upstream_asset_keys,
            retry_policy_max_retries=self.retry_policy_max_retries,
            retry_policy_delay_seconds=self.retry_policy_delay_seconds,
            retry_policy_backoff=self.retry_policy_backoff,
        )

        # Extend with UNTESTED external-asset kinds.
        extras: List[Any] = list(base_defs.assets or [])
        for rows, kind, flag in [
            (linked_services,       "linked_service",       self.import_linked_services),
            (datasets,              "dataset",              self.import_datasets),
            (data_flows,            "data_flow",            self.import_data_flows),
            (integration_runtimes,  "integration_runtime",  self.import_integration_runtimes),
        ]:
            if not flag or not rows:
                continue
            extras.extend(_emit_external_assets(
                rows=rows, kind=kind,
                factory_name=self.workspace.factory_name,
                resource_group_name=self.workspace.resource_group_name,
                subscription_id=self.workspace.subscription_id,
                group_name=self.group_name,
                key_prefix=self.asset_key_prefix,
                extra_kinds=self.extra_kinds,
                apply_translation=self._apply_translation,
            ))

        return Definitions(
            assets=extras,
            sensors=list(base_defs.sensors or []),
        )


# ── Translator ──────────────────────────────────────────────────────────────
class AzureDataFactoryComponentTranslator:
    """Base translator turning `AzureDataFactoryObjectProps` into an
    AssetSpec. Bridges the user's `translation:` callable with the default
    per-object spec. Same convention as `SnowflakeComponentTranslator` /
    `QlikReplicateComponentTranslator` / `FivetranComponentTranslator`."""

    def __init__(self, component: "AzureDataFactoryComponent"):
        self._component = component

    @property
    def component(self) -> "AzureDataFactoryComponent":
        return self._component

    def get_asset_spec(self, props: AzureDataFactoryObjectProps) -> AssetSpec:
        # Default base spec — the exact same shape _emit_external_assets and
        # the pipeline builder produce, so translation callables see a stable
        # input regardless of which kind fired.
        base = AssetSpec(
            key=dg.AssetKey([f"adf_{props.object_kind}_{props.object_name}"]),
            description=f"ADF {props.object_kind}: {props.object_name}",
            group_name=self._component.group_name,
            kinds={"azure", "adf", props.object_kind, *(self._component.extra_kinds or [])},
            metadata={
                "adf/kind":            props.object_kind,
                "adf/name":            props.object_name,
                "adf/factory":         props.factory_name or "",
                "adf/resource_group":  props.resource_group or "",
                "adf/subscription_id": props.subscription_id or "",
            },
        )
        if self._component.translation is None:
            return base
        # Callable path — user gets full control of the final spec.
        return self._component.translation(props)  # type: ignore[misc]

