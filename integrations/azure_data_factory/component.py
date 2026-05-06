"""Azure Data Factory Component.

Extends StateBackedComponent so the ADF Management API is called once at prepare time
(write_state_to_path) and the pipeline list is cached on disk. build_defs_from_state
builds asset specs from the cached list with zero network calls, keeping code-server
reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


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

            @dg.multi_asset(
                specs=expanded_specs,
                name=f"adf_pipeline_{pipeline_name}",
                retry_policy=_retry_policy,
            )
            def pipeline_multi_asset(
                context: dg.AssetExecutionContext,
                _pipeline_name: str = pipeline_name,
                _subscription_id: str = subscription_id,
                _resource_group_name: str = resource_group_name,
                _factory_name: str = factory_name,
                _tenant_id: Optional[str] = tenant_id,
                _client_id: Optional[str] = client_id,
                _client_secret: Optional[str] = client_secret,
                _spec_key_to_pipeline: Dict[tuple, str] = spec_key_to_pipeline,
                _expanded_specs: list = expanded_specs,
                _pipeline_parameters: Optional[Dict[str, Any]] = pipeline_parameters,
                _partition_parameter_name: Optional[str] = partition_parameter_name,
                _max_wait_seconds: int = max_wait_seconds,
                _poll_interval: int = run_poll_interval_seconds,
                _wait_for_completion: bool = wait_for_completion,
                _capture_activity_metadata: bool = capture_activity_metadata,
            ):
                """Trigger an Azure Data Factory pipeline run and wait for completion."""
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

            assets.append(pipeline_multi_asset)

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


# ── Component (StateBackedComponent path) ──────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class AzureDataFactoryComponent(StateBackedComponent, dg.Resolvable):
        """Azure Data Factory component — one Dagster asset per ADF pipeline (and trigger).

        Uses StateBackedComponent to cache the pipeline list from the ADF Management API,
        so code-server reloads are fast. Populate the cache with:
          dagster dev                        (automatic in dev)
          dg utils refresh-defs-state        (CI/CD / image build)

        On first load (before the cache is populated) returns empty Definitions.

        Example:
            ```yaml
            type: dagster_component_templates.AzureDataFactoryComponent
            attributes:
              subscription_id: "12345678-1234-1234-1234-123456789012"
              resource_group_name: my-resource-group
              factory_name: my-data-factory
              tenant_id: "{{ env('AZURE_TENANT_ID') }}"
              client_id: "{{ env('AZURE_CLIENT_ID') }}"
              client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
              import_pipelines: true
              import_triggers: false
            ```
        """

        # ── Required fields ────────────────────────────────────────────────────
        subscription_id: str
        resource_group_name: str
        factory_name: str

        # ── Auth (optional — falls back to DefaultAzureCredential) ────────────
        # Two ways to provide service-principal creds: literal values or
        # *_env_var fields the component reads at load time. Either works.
        tenant_id: Optional[str] = None
        client_id: Optional[str] = None
        client_secret: Optional[str] = None
        tenant_id_env_var: Optional[str] = None
        client_id_env_var: Optional[str] = None
        client_secret_env_var: Optional[str] = None

        # ── Import toggles ─────────────────────────────────────────────────────
        import_pipelines: bool = True
        import_triggers: bool = False

        # ── Filtering ──────────────────────────────────────────────────────────
        filter_by_name_pattern: Optional[str] = None
        exclude_name_pattern: Optional[str] = None
        filter_by_tags: Optional[str] = None

        # ── Sensor ────────────────────────────────────────────────────────────
        generate_sensor: bool = True
        poll_interval_seconds: int = 60

        # ── Presentation ──────────────────────────────────────────────────────
        group_name: str = "azure_data_factory"
        description: Optional[str] = None

        # ── Per-pipeline asset overrides ───────────────────────────────────────
        # The `assets_by_pipeline_name` dict supports per-pipeline keys:
        #   {<pipeline_name>: {key, description, group_name, metadata, tags, kinds, deps}}
        # `deps` is a list of asset keys this specific ADF pipeline depends on —
        # use it to wire individual ADF pipelines into the broader Dagster lineage.
        assets_by_pipeline_name: Optional[dict] = None

        # ── Pipeline-execution config ─────────────────────────────────────────
        pipeline_parameters: Optional[dict] = None
        partition_parameter_name: Optional[str] = None
        max_wait_seconds: int = 3600
        run_poll_interval_seconds: int = 30
        wait_for_completion: bool = True
        capture_activity_metadata: bool = True

        # ── Partitions ────────────────────────────────────────────────────────
        partition_type: Optional[str] = None
        partition_start: Optional[str] = None
        partition_values: Optional[list] = None

        # ── Standard catalog fields ───────────────────────────────────────────
        owners: Optional[list] = None
        asset_tags: Optional[dict] = None
        extra_kinds: Optional[list] = None
        freshness_max_lag_minutes: Optional[int] = None
        freshness_cron: Optional[str] = None
        upstream_asset_keys: Optional[list] = None

        # ── Retry policy ──────────────────────────────────────────────────────
        retry_policy_max_retries: Optional[int] = None
        retry_policy_delay_seconds: Optional[int] = None
        retry_policy_backoff: str = "exponential"

        # ── State backing ─────────────────────────────────────────────────────
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"AzureDataFactoryComponent[{self.factory_name}]",
            )

        # ── State write (runs once at prepare time) ────────────────────────────

        def write_state_to_path(self, state_path: Path) -> None:
            """Call ADF Management API and cache pipeline (and trigger) metadata to disk."""
            import os as _os
            _ten = self.tenant_id or (_os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None)
            _cid = self.client_id or (_os.environ.get(self.client_id_env_var) if self.client_id_env_var else None)
            _sec = self.client_secret or (_os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None)
            client = _get_adf_client(
                self.subscription_id,
                _ten,
                _cid,
                _sec,
            )

            state: Dict[str, Any] = {}

            if self.import_pipelines:
                state["pipelines"] = _fetch_pipelines(
                    client,
                    self.resource_group_name,
                    self.factory_name,
                    self.filter_by_name_pattern,
                    self.exclude_name_pattern,
                    self.filter_by_tags,
                )
            else:
                state["pipelines"] = []

            if self.import_triggers:
                state["triggers"] = _fetch_triggers(
                    client,
                    self.resource_group_name,
                    self.factory_name,
                    self.filter_by_name_pattern,
                    self.exclude_name_pattern,
                    self.filter_by_tags,
                )
            else:
                state["triggers"] = []

            state_path.write_text(json.dumps(state, indent=2))

        # ── Defs build from cache (zero network calls) ─────────────────────────

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset specs from cached ADF metadata — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[union-attr]
                        "AzureDataFactoryComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            state = json.loads(state_path.read_text())
            pipelines: List[Dict[str, Any]] = state.get("pipelines", [])
            trigger_names: List[str] = state.get("triggers", [])

            return _build_adf_defs(
                pipelines=pipelines,
                trigger_names=trigger_names,
                subscription_id=self.subscription_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                tenant_id=self.tenant_id or (os.environ.get(self.tenant_id_env_var) if getattr(self, 'tenant_id_env_var', None) else None),
                client_id=self.client_id or (os.environ.get(self.client_id_env_var) if getattr(self, 'client_id_env_var', None) else None),
                client_secret=self.client_secret or (os.environ.get(self.client_secret_env_var) if getattr(self, 'client_secret_env_var', None) else None),
                group_name=self.group_name,
                import_pipelines=self.import_pipelines,
                import_triggers=self.import_triggers,
                generate_sensor=self.generate_sensor,
                poll_interval_seconds=self.poll_interval_seconds,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                assets_by_pipeline_name=self.assets_by_pipeline_name,
                pipeline_parameters=getattr(self, "pipeline_parameters", None),
                partition_type=getattr(self, "partition_type", None),
                partition_start=getattr(self, "partition_start", None),
                partition_values=getattr(self, "partition_values", None),
                partition_parameter_name=getattr(self, "partition_parameter_name", None),
                max_wait_seconds=getattr(self, "max_wait_seconds", 3600),
                run_poll_interval_seconds=getattr(self, "run_poll_interval_seconds", 30),
                wait_for_completion=getattr(self, "wait_for_completion", True),
                capture_activity_metadata=getattr(self, "capture_activity_metadata", True),
                owners=getattr(self, "owners", None),
                asset_tags=getattr(self, "asset_tags", None),
                extra_kinds=getattr(self, "extra_kinds", None),
                freshness_max_lag_minutes=getattr(self, "freshness_max_lag_minutes", None),
                freshness_cron=getattr(self, "freshness_cron", None),
                upstream_asset_keys=getattr(self, "upstream_asset_keys", None),
                retry_policy_max_retries=getattr(self, "retry_policy_max_retries", None),
                retry_policy_delay_seconds=getattr(self, "retry_policy_delay_seconds", None),
                retry_policy_backoff=getattr(self, "retry_policy_backoff", "exponential"),
            )

else:
    # ── Fallback: StateBackedComponent not available in this dagster version ──
    # Falls back to calling the ADF API on every build_defs (original behaviour).
    class AzureDataFactoryComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Azure Data Factory component (fallback: no state caching).

        Upgrade to dagster>=1.8 to enable StateBackedComponent caching.

        Example:
            ```yaml
            type: dagster_component_templates.AzureDataFactoryComponent
            attributes:
              subscription_id: "12345678-1234-1234-1234-123456789012"
              resource_group_name: my-resource-group
              factory_name: my-data-factory
              tenant_id: "{{ env('AZURE_TENANT_ID') }}"
              client_id: "{{ env('AZURE_CLIENT_ID') }}"
              client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
              import_pipelines: true
            ```
        """

        subscription_id: str = Field(description="Azure subscription ID")
        resource_group_name: str = Field(description="Azure resource group name")
        factory_name: str = Field(description="Azure Data Factory name")

        tenant_id: Optional[str] = Field(
            default=None,
            description="Azure AD tenant ID (optional — uses DefaultAzureCredential if absent)",
        )
        client_id: Optional[str] = Field(
            default=None,
            description="Azure AD client/application ID (optional)",
        )
        client_secret: Optional[str] = Field(
            default=None,
            description="Azure AD client secret (optional)",
        )

        import_pipelines: bool = Field(default=True, description="Import pipelines as assets")
        import_triggers: bool = Field(default=False, description="Import triggers as assets")

        filter_by_name_pattern: Optional[str] = Field(
            default=None, description="Regex to filter entities by name"
        )
        exclude_name_pattern: Optional[str] = Field(
            default=None, description="Regex to exclude entities by name"
        )
        filter_by_tags: Optional[str] = Field(
            default=None, description="Comma-separated tag keys to filter entities"
        )

        generate_sensor: bool = Field(default=True, description="Generate observation sensor")
        poll_interval_seconds: int = Field(default=60, description="Sensor poll interval (s)")

        group_name: str = Field(default="azure_data_factory", description="Asset group name")
        description: Optional[str] = Field(default=None, description="Component description")

        assets_by_pipeline_name: Optional[dict] = Field(
            default=None,
            description=(
                "Override or expand AssetSpecs for specific ADF pipelines. "
                "Keys are ADF pipeline names; values are either a single spec-override dict "
                "or a list of spec-override dicts (one pipeline -> multiple Dagster assets). "
                "Supported keys per override: key, description, group_name, metadata, tags, kinds, deps."
            ),
        )

        retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure (transient network/rate-limit issues)")
        retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1)")
        retry_policy_backoff: str = Field(default="exponential", description="Backoff: 'linear' or 'exponential'")

        # Pipeline-execution config
        pipeline_parameters: Optional[dict] = Field(
            default=None,
            description="Parameters dict passed to every ADF pipeline run (key→value).",
        )
        partition_parameter_name: Optional[str] = Field(
            default=None,
            description=(
                "When the asset is partitioned, auto-pass the partition_key as this "
                "ADF pipeline parameter (default: 'partition_key'). Example: a daily-"
                "partitioned asset with partition_parameter_name='ODATE' passes "
                "ODATE='2026-05-06' to the ADF pipeline at run time."
            ),
        )
        max_wait_seconds: int = Field(
            default=3600,
            description="How long to wait for the pipeline to complete before timing out.",
        )
        run_poll_interval_seconds: int = Field(
            default=30,
            description="Seconds between status polls while a run is in progress.",
        )
        wait_for_completion: bool = Field(
            default=True,
            description="If False, fire-and-forget — yield Submitted immediately and don't poll.",
        )
        capture_activity_metadata: bool = Field(
            default=True,
            description="On completion, fetch each ADF activity's status/duration/error/output and surface as metadata.",
        )

        # Partition fields
        partition_type: Optional[str] = Field(
            default=None,
            description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | None (unpartitioned).",
        )
        partition_start: Optional[str] = Field(
            default=None,
            description="Start date for time-based partitions, ISO format (e.g. '2024-01-01').",
        )
        partition_values: Optional[list] = Field(
            default=None,
            description="List of partition values for static partitions (e.g. ['us', 'eu', 'apac']).",
        )

        # Standard catalog fields
        owners: Optional[list] = Field(default=None, description="Asset owners (team or email).")
        asset_tags: Optional[dict] = Field(default=None, description="Catalog tags.")
        extra_kinds: Optional[list] = Field(default=None, description="Additional asset kinds beyond the default {azure, adf}.")
        freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Freshness SLO in minutes (legacy FreshnessPolicy).")
        freshness_cron: Optional[str] = Field(default=None, description="Cron schedule for the freshness policy.")
        upstream_asset_keys: Optional[list] = Field(
            default=None,
            description=(
                "Asset keys that ALL imported ADF pipeline assets should depend on. "
                "Lets non-ADF Dagster assets gate ADF pipeline runs (e.g. only run "
                "ADF pipelines after dbt has refreshed the upstream tables). "
                "For per-pipeline overrides, use assets_by_pipeline_name's `deps` key."
            ),
        )

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            """Build Definitions by calling the ADF API at load time."""
            import os as _os
            _ten = self.tenant_id or (_os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None)
            _cid = self.client_id or (_os.environ.get(self.client_id_env_var) if self.client_id_env_var else None)
            _sec = self.client_secret or (_os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None)
            client = _get_adf_client(
                self.subscription_id,
                _ten,
                _cid,
                _sec,
            )

            pipelines: List[Dict[str, Any]] = []
            trigger_names: List[str] = []

            if self.import_pipelines:
                pipelines = _fetch_pipelines(
                    client,
                    self.resource_group_name,
                    self.factory_name,
                    self.filter_by_name_pattern,
                    self.exclude_name_pattern,
                    self.filter_by_tags,
                )

            if self.import_triggers:
                trigger_names = _fetch_triggers(
                    client,
                    self.resource_group_name,
                    self.factory_name,
                    self.filter_by_name_pattern,
                    self.exclude_name_pattern,
                    self.filter_by_tags,
                )

            return _build_adf_defs(
                pipelines=pipelines,
                trigger_names=trigger_names,
                subscription_id=self.subscription_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                tenant_id=self.tenant_id or (os.environ.get(self.tenant_id_env_var) if getattr(self, 'tenant_id_env_var', None) else None),
                client_id=self.client_id or (os.environ.get(self.client_id_env_var) if getattr(self, 'client_id_env_var', None) else None),
                client_secret=self.client_secret or (os.environ.get(self.client_secret_env_var) if getattr(self, 'client_secret_env_var', None) else None),
                group_name=self.group_name,
                import_pipelines=self.import_pipelines,
                import_triggers=self.import_triggers,
                generate_sensor=self.generate_sensor,
                poll_interval_seconds=self.poll_interval_seconds,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                assets_by_pipeline_name=self.assets_by_pipeline_name,
                pipeline_parameters=getattr(self, "pipeline_parameters", None),
                partition_type=getattr(self, "partition_type", None),
                partition_start=getattr(self, "partition_start", None),
                partition_values=getattr(self, "partition_values", None),
                partition_parameter_name=getattr(self, "partition_parameter_name", None),
                max_wait_seconds=getattr(self, "max_wait_seconds", 3600),
                run_poll_interval_seconds=getattr(self, "run_poll_interval_seconds", 30),
                wait_for_completion=getattr(self, "wait_for_completion", True),
                capture_activity_metadata=getattr(self, "capture_activity_metadata", True),
                owners=getattr(self, "owners", None),
                asset_tags=getattr(self, "asset_tags", None),
                extra_kinds=getattr(self, "extra_kinds", None),
                freshness_max_lag_minutes=getattr(self, "freshness_max_lag_minutes", None),
                freshness_cron=getattr(self, "freshness_cron", None),
                upstream_asset_keys=getattr(self, "upstream_asset_keys", None),
                retry_policy_max_retries=getattr(self, "retry_policy_max_retries", None),
                retry_policy_delay_seconds=getattr(self, "retry_policy_delay_seconds", None),
                retry_policy_backoff=getattr(self, "retry_policy_backoff", "exponential"),
            )
