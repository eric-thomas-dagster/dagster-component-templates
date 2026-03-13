"""Azure Data Factory Component.

Extends StateBackedComponent so the ADF Management API is called once at prepare time
(write_state_to_path) and the pipeline list is cached on disk. build_defs_from_state
builds asset specs from the cached list with zero network calls, keeping code-server
reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg

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

    subscription_id: str = dg.Field(description="Azure subscription ID")
    resource_group_name: str = dg.Field(description="Azure resource group name")
    factory_name: str = dg.Field(description="Azure Data Factory name")
    tenant_id_env_var: Optional[str] = dg.Field(
        default=None,
        description="Env var holding the Azure AD tenant ID (optional — uses DefaultAzureCredential if absent)",
    )
    client_id_env_var: Optional[str] = dg.Field(
        default=None,
        description="Env var holding the Azure AD client/application ID (optional)",
    )
    client_secret_env_var: Optional[str] = dg.Field(
        default=None,
        description="Env var holding the Azure AD client secret (optional)",
    )

    def get_client(self):
        """Return an authenticated DataFactoryManagementClient."""
        import os
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient

        if self.tenant_id_env_var and self.client_id_env_var and self.client_secret_env_var:
            credential = ClientSecretCredential(
                tenant_id=os.environ[self.tenant_id_env_var],
                client_id=os.environ[self.client_id_env_var],
                client_secret=os.environ[self.client_secret_env_var],
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
) -> dg.Definitions:
    """Build Dagster Definitions from pre-fetched ADF metadata (no network calls)."""
    assets: list = []
    sensors: list = []

    # ── Pipeline assets ────────────────────────────────────────────────────────
    if import_pipelines:
        for pipeline_meta in pipelines:
            pipeline_name = pipeline_meta["name"]

            # Build the default AssetSpec for this pipeline
            default_spec = dg.AssetSpec(
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
                kinds={"azure", "adf"},
            )

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

                for p_name, key_paths in pipelines_to_run.items():
                    run_response = adf_client.pipelines.create_run(
                        _resource_group_name,
                        _factory_name,
                        p_name,
                    )
                    run_id = run_response.run_id
                    context.log.info(f"Pipeline run started. Run ID: {run_id}")

                    max_wait_seconds = 3600  # 60 minutes
                    poll_interval = 30
                    elapsed = 0

                    while elapsed < max_wait_seconds:
                        pipeline_run = adf_client.pipeline_runs.get(
                            _resource_group_name,
                            _factory_name,
                            run_id,
                        )
                        status = pipeline_run.status
                        context.log.info(f"Pipeline run status: {status}")

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
                            }

                            if status == "Failed":
                                error_msg = getattr(pipeline_run, "message", None) or "Pipeline failed"
                                run_metadata["error"] = dg.MetadataValue.text(error_msg)
                                raise Exception(
                                    f"ADF pipeline '{p_name}' failed: {error_msg}"
                                )

                            # Yield MaterializeResult for all asset keys produced by this pipeline
                            for key_path in key_paths:
                                yield dg.MaterializeResult(
                                    asset_key=dg.AssetKey(list(key_path)),
                                    metadata=run_metadata,
                                )
                            break

                        time.sleep(poll_interval)
                        elapsed += poll_interval

                    else:
                        context.log.warning(
                            f"Pipeline run timed out after {max_wait_seconds // 60} minutes"
                        )
                        for key_path in key_paths:
                            yield dg.MaterializeResult(
                                asset_key=dg.AssetKey(list(key_path)),
                                metadata={
                                    "run_id": dg.MetadataValue.text(run_id),
                                    "status": dg.MetadataValue.text("Timeout"),
                                    "pipeline_name": dg.MetadataValue.text(p_name),
                                },
                            )

            assets.append(pipeline_multi_asset)

    # ── Trigger assets ─────────────────────────────────────────────────────────
    if import_triggers:
        for trigger_name in trigger_names:

            @dg.asset(
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
        tenant_id: Optional[str] = None
        client_id: Optional[str] = None
        client_secret: Optional[str] = None

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
        assets_by_pipeline_name: Optional[dict] = None

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
            client = _get_adf_client(
                self.subscription_id,
                self.tenant_id,
                self.client_id,
                self.client_secret,
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
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                group_name=self.group_name,
                import_pipelines=self.import_pipelines,
                import_triggers=self.import_triggers,
                generate_sensor=self.generate_sensor,
                poll_interval_seconds=self.poll_interval_seconds,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                assets_by_pipeline_name=self.assets_by_pipeline_name,
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

        subscription_id: str = dg.Field(description="Azure subscription ID")
        resource_group_name: str = dg.Field(description="Azure resource group name")
        factory_name: str = dg.Field(description="Azure Data Factory name")

        tenant_id: Optional[str] = dg.Field(
            default=None,
            description="Azure AD tenant ID (optional — uses DefaultAzureCredential if absent)",
        )
        client_id: Optional[str] = dg.Field(
            default=None,
            description="Azure AD client/application ID (optional)",
        )
        client_secret: Optional[str] = dg.Field(
            default=None,
            description="Azure AD client secret (optional)",
        )

        import_pipelines: bool = dg.Field(default=True, description="Import pipelines as assets")
        import_triggers: bool = dg.Field(default=False, description="Import triggers as assets")

        filter_by_name_pattern: Optional[str] = dg.Field(
            default=None, description="Regex to filter entities by name"
        )
        exclude_name_pattern: Optional[str] = dg.Field(
            default=None, description="Regex to exclude entities by name"
        )
        filter_by_tags: Optional[str] = dg.Field(
            default=None, description="Comma-separated tag keys to filter entities"
        )

        generate_sensor: bool = dg.Field(default=True, description="Generate observation sensor")
        poll_interval_seconds: int = dg.Field(default=60, description="Sensor poll interval (s)")

        group_name: str = dg.Field(default="azure_data_factory", description="Asset group name")
        description: Optional[str] = dg.Field(default=None, description="Component description")

        assets_by_pipeline_name: Optional[dict] = dg.Field(
            default=None,
            description=(
                "Override or expand AssetSpecs for specific ADF pipelines. "
                "Keys are ADF pipeline names; values are either a single spec-override dict "
                "or a list of spec-override dicts (one pipeline -> multiple Dagster assets). "
                "Supported keys per override: key, description, group_name, metadata, tags, kinds, deps."
            ),
        )

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            """Build Definitions by calling the ADF API at load time."""
            client = _get_adf_client(
                self.subscription_id,
                self.tenant_id,
                self.client_id,
                self.client_secret,
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
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                group_name=self.group_name,
                import_pipelines=self.import_pipelines,
                import_triggers=self.import_triggers,
                generate_sensor=self.generate_sensor,
                poll_interval_seconds=self.poll_interval_seconds,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                assets_by_pipeline_name=self.assets_by_pipeline_name,
            )
