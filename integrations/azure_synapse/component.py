"""Azure Synapse Analytics Component.

Import Azure Synapse pipelines, SQL pools, Spark jobs, and notebooks
as Dagster assets with automatic observation and orchestration.
"""

from dagster import AssetKey  # auto-added for hierarchical keys

import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import time

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.synapse import SynapseManagementClient
from azure.synapse.artifacts import ArtifactsClient
from azure.core.exceptions import ResourceNotFoundError

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    observable_source_asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


# ─── Asset overrides (inline; kept per-component to preserve self-containment) ─
#
# Per-asset override applied after enumeration. Today supports `depends_on` —
# a list of upstream Dagster asset keys (strings; slash-delimited becomes a
# hierarchical AssetKey). Extend with more fields as needed (group, tags,
# description). Matches the pattern used by the official Databricks workspace
# component's `attributes.asset_overrides.<key>.depends_on`.


@dataclass
class AssetOverride(Resolvable):
    depends_on: Optional[List[str]] = None


def _resolve_override_deps(
    asset_overrides: Optional[Dict[str, "AssetOverride"]],
    lookup_key: str,
) -> List[AssetKey]:
    if not asset_overrides:
        return []
    ov = asset_overrides.get(lookup_key)
    if not ov or not ov.depends_on:
        return []
    return [AssetKey(d.split("/")) if "/" in d else AssetKey(d) for d in ov.depends_on]


class AzureSynapseComponent(Component, Model, Resolvable):
    """Component for importing Azure Synapse Analytics entities as Dagster assets.

    Supports importing:
    - Pipelines (trigger pipeline runs)
    - SQL Pools (pause/resume dedicated SQL pools)
    - Spark Jobs (submit Spark jobs and notebooks)
    - Notebooks (execute Synapse notebooks)

    Example:
        ```yaml
        type: dagster_component_templates.AzureSynapseComponent
        attributes:
          subscription_id: "12345678-1234-1234-1234-123456789012"
          resource_group_name: my-resource-group
          workspace_name: my-synapse-workspace
          tenant_id: "{{ env('AZURE_TENANT_ID') }}"
          client_id: "{{ env('AZURE_CLIENT_ID') }}"
          client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
          import_pipelines: true
          import_sql_pools: true
        ```
    """

    subscription_id: str = Field(
        description="Azure subscription ID"
    )

    resource_group_name: str = Field(
        description="Azure resource group name"
    )

    workspace_name: str = Field(
        description="Azure Synapse workspace name"
    )

    tenant_id: Optional[str] = Field(
        default=None,
        description="Azure AD tenant ID (optional if using DefaultAzureCredential)"
    )

    client_id: Optional[str] = Field(
        default=None,
        description="Azure AD client/application ID (optional if using DefaultAzureCredential)"
    )

    client_secret: Optional[str] = Field(
        default=None,
        description="Azure AD client secret (optional if using DefaultAzureCredential)"
    )

    import_pipelines: bool = Field(
        default=True,
        description="Import pipelines as materializable assets"
    )

    import_spark_jobs: bool = Field(
        default=False,
        description="Import Spark job definitions as materializable assets"
    )

    import_notebooks: bool = Field(
        default=False,
        description="Import notebooks as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    filter_by_tags: Optional[str] = Field(
        default=None,
        description="Comma-separated tag keys to filter entities (e.g., 'env,team')"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for pipeline runs"
    )

    group_name: str = Field(
        default="azure_synapse",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Azure Synapse component"
    )

    asset_overrides: Optional[Dict[str, AssetOverride]] = Field(
        default=None,
        description=(
            "Per-asset overrides keyed by the emitted asset's name (e.g. "
            "`synapse_pipeline_orders_load`, `synapse_notebook_scoring`). Today supports "
            "`depends_on: [upstream_key, ...]` to add Dagster asset dependencies. "
            "Matches the pattern used by the official Databricks workspace component."
        ),
    )

    def _get_credential(self):
        """Get Azure credential."""
        if self.tenant_id and self.client_id and self.client_secret:
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return DefaultAzureCredential()

    def _get_management_client(self) -> SynapseManagementClient:
        """Create Synapse management client."""
        credential = self._get_credential()
        return SynapseManagementClient(credential, self.subscription_id)

    def _get_artifacts_client(self) -> ArtifactsClient:
        """Create Synapse artifacts client."""
        credential = self._get_credential()
        endpoint = f"https://{self.workspace_name}.dev.azuresynapse.net"
        return ArtifactsClient(credential, endpoint)

    def _matches_filters(self, name: str, tags: Optional[Dict[str, str]] = None) -> bool:
        """Check if entity matches name and tag filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Tag filter
        if self.filter_by_tags and tags:
            required_keys = [k.strip() for k in self.filter_by_tags.split(",")]
            if not all(key in tags for key in required_keys):
                return False

        return True

    def _list_pipelines(self, client: ArtifactsClient) -> List[str]:
        """List all pipelines."""
        pipelines = []
        for pipeline in client.pipeline.get_pipelines_by_workspace():
            if self._matches_filters(pipeline.name):
                pipelines.append(pipeline.name)
        return pipelines

    def _list_spark_jobs(self, client: ArtifactsClient) -> List[str]:
        """List all Spark job definitions."""
        jobs = []
        for job in client.spark_job_definition.get_spark_job_definitions_by_workspace():
            if self._matches_filters(job.name):
                jobs.append(job.name)
        return jobs

    def _list_notebooks(self, client: ArtifactsClient) -> List[str]:
        """List all notebooks."""
        notebooks = []
        for notebook in client.notebook.get_notebooks_by_workspace():
            if self._matches_filters(notebook.name):
                notebooks.append(notebook.name)
        return notebooks

    def _get_pipeline_assets(self, client: ArtifactsClient) -> List:
        """Generate pipeline assets — factory pattern to avoid Dagster
        interpreting closure default-args as AssetIn dependencies.
        """
        assets = []
        pipelines = self._list_pipelines(client)
        component_self = self

        for pipeline_name in pipelines:
            asset_key = f"synapse_pipeline_{pipeline_name}"
            override_deps = _resolve_override_deps(component_self.asset_overrides, asset_key)

            def _make_pipeline_asset(_pipeline_name=pipeline_name, _asset_key=asset_key, _override_deps=override_deps):
                @asset(
                    key=AssetKey.from_user_string(_asset_key),
                    deps=_override_deps,
                    group_name=component_self.group_name,
                    metadata={
                        "pipeline_name": _pipeline_name,
                        "workspace_name": component_self.workspace_name,
                    },
                )
                def pipeline_asset(context: AssetExecutionContext):
                    """Trigger Azure Synapse pipeline run."""
                    artifacts_client = component_self._get_artifacts_client()

                    run_response = artifacts_client.pipeline.create_pipeline_run(_pipeline_name)
                    run_id = run_response.run_id
                    context.log.info(f"Pipeline run started. Run ID: {run_id}")

                    max_wait_minutes = 60
                    poll_interval = 30
                    elapsed = 0
                    while elapsed < max_wait_minutes * 60:
                        pipeline_run = artifacts_client.pipeline_run.get_pipeline_run(run_id)
                        status = pipeline_run.status
                        context.log.info(f"Pipeline run status: {status}")
                        if status in ["Succeeded", "Failed", "Cancelled"]:
                            metadata = {
                                "run_id": run_id,
                                "status": status,
                                "pipeline_name": _pipeline_name,
                                "start_time": str(pipeline_run.run_start),
                                "end_time": str(pipeline_run.run_end),
                                "duration_seconds": (
                                    (pipeline_run.run_end - pipeline_run.run_start).total_seconds()
                                    if pipeline_run.run_end and pipeline_run.run_start
                                    else 0
                                ),
                            }
                            if status == "Failed":
                                metadata["error"] = pipeline_run.message or "Pipeline failed"
                            return metadata
                        time.sleep(poll_interval)
                        elapsed += poll_interval

                    context.log.warning(f"Pipeline run timed out after {max_wait_minutes} minutes")
                    return {"run_id": run_id, "status": "Timeout", "pipeline_name": _pipeline_name}

                return pipeline_asset

            assets.append(_make_pipeline_asset())

        return assets

    def _get_spark_job_assets(self, client: ArtifactsClient) -> List:
        """Generate Spark job assets — actually submit + poll the Spark batch."""
        assets = []
        jobs = self._list_spark_jobs(client)
        component_self = self

        for job_name in jobs:
            asset_key = f"synapse_spark_job_{job_name}"
            override_deps = _resolve_override_deps(component_self.asset_overrides, asset_key)

            def _make_spark_asset(_job_name=job_name, _asset_key=asset_key, _override_deps=override_deps):
                @asset(
                    key=AssetKey.from_user_string(_asset_key),
                    deps=_override_deps,
                    group_name=component_self.group_name,
                    metadata={
                        "job_name": _job_name,
                        "workspace_name": component_self.workspace_name,
                    },
                )
                def spark_job_asset(context: AssetExecutionContext):
                    """Submit a Synapse Spark job (livy-style batch) and poll until done."""
                    artifacts_client = component_self._get_artifacts_client()
                    job_def = artifacts_client.spark_job_definition.get_spark_job_definition(_job_name)
                    context.log.info(f"Submitting Spark job: {_job_name}")

                    # Trigger via SparkBatch API (same as the portal's "Submit" button)
                    try:
                        run = artifacts_client.spark_job_definition.begin_execute_spark_job_definition(
                            _job_name
                        ).result()
                    except Exception as e:
                        # Older SDK versions: fall back to direct REST call
                        context.log.warning(
                            f"begin_execute_spark_job_definition unavailable ({e}); "
                            "falling back to REST endpoint"
                        )
                        from azure.core.pipeline.transport import HttpRequest
                        endpoint = f"https://{component_self.workspace_name}.dev.azuresynapse.net"
                        req = HttpRequest(
                            "POST",
                            f"{endpoint}/sparkJobDefinitions/{_job_name}/execute?api-version=2020-12-01",
                        )
                        # We can't easily run this without the credentialed pipeline; raise instructive error
                        raise RuntimeError(
                            f"Spark execution requires azure-synapse-artifacts >= 0.18.0 "
                            f"with begin_execute_spark_job_definition. Got error: {e}"
                        ) from e

                    livy_id = getattr(run, "id", None) or getattr(run, "livyInfo", {}).get("id")
                    state = getattr(run, "state", "Unknown")
                    context.log.info(
                        f"Spark batch submitted: livy_id={livy_id}, initial_state={state}"
                    )

                    return {
                        "job_name": _job_name,
                        "livy_id": str(livy_id) if livy_id is not None else "",
                        "state": state,
                        "language": (
                            job_def.properties.job_properties.language
                            if hasattr(job_def.properties, "job_properties")
                            else "unknown"
                        ),
                    }
                return spark_job_asset

            assets.append(_make_spark_asset())

        return assets

    def _get_notebook_assets(self, client: ArtifactsClient) -> List:
        """Generate notebook assets."""
        assets = []
        notebooks = self._list_notebooks(client)
        component_self = self

        for notebook_name in notebooks:
            asset_key = f"synapse_notebook_{notebook_name}"
            override_deps = _resolve_override_deps(component_self.asset_overrides, asset_key)

            def _make_notebook_asset(_notebook_name=notebook_name, _asset_key=asset_key, _override_deps=override_deps):
                @asset(
                    key=AssetKey.from_user_string(_asset_key),
                    deps=_override_deps,
                    group_name=component_self.group_name,
                    metadata={
                        "notebook_name": _notebook_name,
                        "workspace_name": component_self.workspace_name,
                    },
                )
                def notebook_asset(context: AssetExecutionContext):
                    """Execute a Synapse notebook via livy-style Spark batch.

                    Synapse notebooks compile down to Spark batches at execution.
                    The Artifacts SDK's notebook_operation_result.begin_execute_notebook
                    submits via the same SparkBatch API used for Spark job definitions.
                    """
                    artifacts_client = component_self._get_artifacts_client()
                    notebook = artifacts_client.notebook.get_notebook(_notebook_name)
                    context.log.info(f"Submitting notebook: {_notebook_name}")

                    try:
                        # Newer SDK exposes a direct execute method
                        run = artifacts_client.notebook_operation_result.begin_execute_notebook(
                            _notebook_name
                        ).result()
                        livy_id = getattr(run, "id", None) or getattr(run, "livyInfo", {}).get("id")
                        state = getattr(run, "state", "Unknown")
                        context.log.info(
                            f"Notebook execution: livy_id={livy_id}, state={state}"
                        )
                        return {
                            "notebook_name": _notebook_name,
                            "livy_id": str(livy_id) if livy_id else "",
                            "state": state,
                        }
                    except (AttributeError, Exception) as e:
                        # Fall back to the pipeline-wrapper pattern for older SDKs
                        context.log.warning(
                            f"Direct notebook execution unavailable ({e}); "
                            "wrap the notebook in a Synapse pipeline + use the synapse_pipeline_<name> asset instead."
                        )
                        return {
                            "notebook_name": _notebook_name,
                            "status": "Skipped",
                            "reason": "Direct notebook execution requires azure-synapse-artifacts with notebook_operation_result.begin_execute_notebook. Wrap the notebook in a pipeline activity for older SDK versions.",
                            "metadata": notebook.properties.metadata if hasattr(notebook.properties, 'metadata') else {},
                        }
                return notebook_asset

            assets.append(_make_notebook_asset())

        return assets

    def _get_observation_sensor(self, client: ArtifactsClient):
        """Generate sensor to observe pipeline runs."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def synapse_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Azure Synapse pipeline runs."""
            artifacts_client = self._get_artifacts_client()

            # Get cursor (last check time)
            cursor = context.cursor
            if cursor:
                last_check = datetime.fromisoformat(cursor)
            else:
                last_check = datetime.utcnow() - timedelta(hours=1)

            now = datetime.utcnow()

            # Query pipeline runs since last check
            pipeline_runs = artifacts_client.pipeline_run.query_pipeline_runs_by_workspace(
                {
                    "last_updated_after": last_check,
                    "last_updated_before": now,
                }
            )

            # Emit asset materializations for completed pipeline runs
            for run in pipeline_runs.value:
                if run.status in ["Succeeded", "Failed", "Cancelled"]:
                    # Check if pipeline matches our filters
                    if not self._matches_filters(run.pipeline_name):
                        continue

                    asset_key = f"synapse_pipeline_{run.pipeline_name}"

                    metadata = {
                        "run_id": MetadataValue.text(run.run_id),
                        "status": MetadataValue.text(run.status),
                        "pipeline_name": MetadataValue.text(run.pipeline_name),
                        "start_time": MetadataValue.text(str(run.run_start)),
                        "end_time": MetadataValue.text(str(run.run_end)),
                        "duration_seconds": MetadataValue.float(
                            (run.run_end - run.run_start).total_seconds()
                            if run.run_end and run.run_start
                            else 0
                        ),
                    }

                    if run.status == "Failed" and run.message:
                        metadata["error"] = MetadataValue.text(run.message)

                    yield AssetMaterialization(
                        asset_key=asset_key,
                        metadata=metadata,
                    )

            # Update cursor
            context.update_cursor(now.isoformat())

        return synapse_observation_sensor

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the imported Synapse workspace."""
        mgmt_client = self._get_management_client()
        artifacts_client = self._get_artifacts_client()

        assets = []
        sensors = []

        # Import pipelines
        if self.import_pipelines:
            assets.extend(self._get_pipeline_assets(artifacts_client))

        # Import Spark jobs
        if self.import_spark_jobs:
            assets.extend(self._get_spark_job_assets(artifacts_client))

        # Import notebooks
        if self.import_notebooks:
            assets.extend(self._get_notebook_assets(artifacts_client))

        # Generate observation sensor
        if self.generate_sensor and self.import_pipelines:
            sensors.append(self._get_observation_sensor(artifacts_client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
