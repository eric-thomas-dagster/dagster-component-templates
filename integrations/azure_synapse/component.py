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

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    observable_source_asset,
    sensor,
    SensorEvaluationContext,
    SensorResult,
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


# ── Explicit item declarations (skip discovery entirely; no live service needed) ─────────────────────
@dataclass
class SynapseNotebookDeclaration(Resolvable):
    """One explicitly declared Synapse notebook -- set `notebooks:` on the component to use this list
    instead of discovering notebooks from the live workspace. No Azure client is constructed for
    notebooks at all when every enabled category (notebooks/spark_jobs) is declared this way."""

    name: str
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class SynapseSparkJobDeclaration(Resolvable):
    """One explicitly declared Synapse Spark job definition -- set `spark_jobs:` on the component to use
    this list instead of discovering jobs from the live workspace.

    No args/conf/driver-or-executor-sizing override fields: confirmed (against the real
    azure-synapse-artifacts SDK/REST surface, not guessed) that
    `begin_execute_spark_job_definition` takes ONLY the job definition's name -- there is no
    per-execution parameter override at all. A Spark job definition's args/conf/sizing are fixed
    when the job definition itself is authored in Synapse Studio; Dagster can trigger it or not,
    nothing in between. Building fields here that silently did nothing would be worse than not
    having them."""

    name: str


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
        type: dagster_community_components.AzureSynapseComponent
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

    notebooks: Optional[List[SynapseNotebookDeclaration]] = Field(
        default=None,
        description=(
            "Explicit list of notebooks ({name, parameters?}), in place of discovering them from the "
            "live Synapse workspace. When set, no Azure client is constructed for notebook discovery at "
            "all -- useful for a converter-generated static definition where the deploy pipeline already "
            "knows exactly which notebooks exist (ignored unless import_notebooks is also true)."
        ),
    )
    spark_jobs: Optional[List[SynapseSparkJobDeclaration]] = Field(
        default=None,
        description=(
            "Explicit list of Spark job definitions ({name, args?, conf?, driver/executor sizing?}), in "
            "place of discovering them from the live Synapse workspace (ignored unless import_spark_jobs "
            "is also true)."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "Log what would be triggered (pipeline run / Spark batch / notebook run, with its resolved "
            "parameters) and skip the real Azure call entirely -- same convention as the SQL, email, and "
            "SAP job-trigger components' own dry_run flag."
        ),
    )
    default_spark_pool_name: Optional[str] = Field(
        default=None,
        description=(
            "Fallback Spark pool name used to poll a submitted Spark batch's terminal state, only if "
            "begin_execute_spark_job_definition's own response doesn't carry spark_pool_name (it normally "
            "does). Required if that fallback is ever actually needed and import_spark_jobs is true."
        ),
    )

    def _get_credential(self):
        """Get Azure credential. Lazy-imported (like every other Azure-backed component in this
        registry) so component.py itself loads fine with no azure-identity installed, and so this
        is only ever invoked where it's actually needed -- never at component-load/build_defs time
        unless genuine live discovery is required (see build_defs)."""
        from azure.identity import ClientSecretCredential, DefaultAzureCredential

        if self.tenant_id and self.client_id and self.client_secret:
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return DefaultAzureCredential()

    def _get_management_client(self):
        """Create Synapse management client."""
        from azure.mgmt.synapse import SynapseManagementClient

        credential = self._get_credential()
        return SynapseManagementClient(credential, self.subscription_id)

    def _get_artifacts_client(self):
        """Create Synapse artifacts client."""
        from azure.synapse.artifacts import ArtifactsClient

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

    def _list_pipelines(self, client: Any) -> List[str]:
        """List all pipelines."""
        pipelines = []
        for pipeline in client.pipeline.get_pipelines_by_workspace():
            if self._matches_filters(pipeline.name):
                pipelines.append(pipeline.name)
        return pipelines

    def _list_spark_jobs(self, client: Any) -> List[str]:
        """List all Spark job definitions."""
        jobs = []
        for job in client.spark_job_definition.get_spark_job_definitions_by_workspace():
            if self._matches_filters(job.name):
                jobs.append(job.name)
        return jobs

    def _list_notebooks(self, client: Any) -> List[str]:
        """List all notebooks."""
        notebooks = []
        for notebook in client.notebook.get_notebooks_by_workspace():
            if self._matches_filters(notebook.name):
                notebooks.append(notebook.name)
        return notebooks

    def _get_pipeline_assets(self, client: Any) -> List:
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
                    if component_self.dry_run:
                        context.log.info(f"[DRY RUN] would trigger pipeline run {_pipeline_name!r}")
                        return {"pipeline_name": _pipeline_name, "status": "dry_run — not triggered"}
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

    def _resolve_spark_job_items(self, client_factory) -> List[Dict[str, Any]]:
        """{name} per job -- from self.spark_jobs if declared (no Azure client constructed), else
        discovered live via client_factory() (built/cached lazily, only once actually needed)."""
        if self.spark_jobs is not None:
            return [{"name": j.name} for j in self.spark_jobs]
        names = self._list_spark_jobs(client_factory())
        return [{"name": n} for n in names]

    def _get_spark_job_assets(self, items: List[Dict[str, Any]]) -> List:
        """Generate Spark job assets — actually submit + poll the Spark batch."""
        assets = []
        component_self = self

        for item in items:
            job_name = item["name"]
            asset_key = f"synapse_spark_job_{job_name}"
            override_deps = _resolve_override_deps(component_self.asset_overrides, asset_key)

            def _make_spark_asset(_job_name=job_name, _asset_key=asset_key, _override_deps=override_deps, _item=item):
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
                    """Submit a Synapse Spark job (livy-style batch) and poll the batch itself to a
                    TERMINAL state. `begin_execute_spark_job_definition(...).result()` only confirms the
                    submission request was accepted -- it does NOT wait for the Spark batch to finish
                    (confirmed against the real SDK/REST surface) -- so the real completion state has to
                    come from a SEPARATE poll via azure-synapse-spark's SparkClient.spark_batch.get_spark_batch_job.
                    Any real submission/poll failure PROPAGATES (no broad except-and-report-fake-success)."""
                    if component_self.dry_run:
                        context.log.info(f"[DRY RUN] would submit Spark job {_job_name!r}")
                        return {"job_name": _job_name, "status": "dry_run — not submitted"}
                    artifacts_client = component_self._get_artifacts_client()
                    context.log.info(f"Submitting Spark job: {_job_name}")

                    run = artifacts_client.spark_job_definition.begin_execute_spark_job_definition(_job_name).result()
                    batch_id = getattr(run, "id", None)
                    initial_state = getattr(run, "state", None)
                    if batch_id is None:
                        raise RuntimeError(
                            f"Spark job {_job_name!r}: begin_execute_spark_job_definition().result() returned no "
                            f"batch id (got {run!r}) -- cannot poll for completion."
                        )
                    context.log.info(f"Spark batch submitted: batch_id={batch_id}, initial_state={initial_state}")

                    try:
                        from azure.synapse.spark import SparkClient
                    except ImportError as e:
                        raise RuntimeError(
                            "Spark job final-state polling requires the azure-synapse-spark package "
                            "(pip install azure-synapse-spark) -- azure-synapse-artifacts alone has no way "
                            "to check a submitted batch's terminal state."
                        ) from e
                    endpoint = f"https://{component_self.workspace_name}.dev.azuresynapse.net"
                    spark_pool_name = getattr(run, "spark_pool_name", None) or component_self.default_spark_pool_name
                    if not spark_pool_name:
                        raise RuntimeError(
                            f"Spark job {_job_name!r}: could not determine the Spark pool name needed to poll "
                            f"batch {batch_id} for completion (begin_execute_spark_job_definition's own result "
                            "didn't carry one)."
                        )
                    spark_client = SparkClient(component_self._get_credential(), endpoint, spark_pool_name)

                    _TERMINAL_STATES = {"success", "error", "dead", "killed"}
                    max_wait_minutes = 60
                    poll_interval = 15
                    elapsed = 0
                    while True:
                        batch = spark_client.spark_batch.get_spark_batch_job(batch_id)
                        state = getattr(batch, "state", None)
                        context.log.info(f"Spark batch {batch_id} status: {state}")
                        if state in _TERMINAL_STATES:
                            if state != "success":
                                raise RuntimeError(
                                    f"Spark job {_job_name!r} (batch {batch_id}) finished with state={state!r}."
                                )
                            return {"job_name": _job_name, "batch_id": str(batch_id), "state": state}
                        if elapsed >= max_wait_minutes * 60:
                            raise TimeoutError(
                                f"Spark job {_job_name!r} (batch {batch_id}) exceeded timeout of "
                                f"{max_wait_minutes} minutes (last state={state!r})."
                            )
                        time.sleep(poll_interval)
                        elapsed += poll_interval
                return spark_job_asset

            assets.append(_make_spark_asset())

        return assets

    def _resolve_notebook_items(self, client_factory) -> List[Dict[str, Any]]:
        """{name, parameters} per notebook -- from self.notebooks if declared (no Azure client
        constructed), else discovered live via client_factory()."""
        if self.notebooks is not None:
            return [{"name": n.name, "parameters": n.parameters or {}} for n in self.notebooks]
        names = self._list_notebooks(client_factory())
        return [{"name": n, "parameters": {}} for n in names]

    def _get_notebook_assets(self, items: List[Dict[str, Any]]) -> List:
        """Generate notebook assets."""
        assets = []
        component_self = self

        for item in items:
            notebook_name = item["name"]
            asset_key = f"synapse_notebook_{notebook_name}"
            override_deps = _resolve_override_deps(component_self.asset_overrides, asset_key)

            def _make_notebook_asset(_notebook_name=notebook_name, _asset_key=asset_key, _override_deps=override_deps, _item=item):
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
                    """Execute a Synapse notebook via run_notebook.begin_create_run + a SEPARATE poll of
                    run_notebook.get_status to a terminal run_status (confirmed against the real SDK: the
                    create-run poller's own .result() only confirms the create-run request was accepted,
                    NOT that the notebook finished -- the previous implementation called a method
                    (notebook_operation_result.begin_execute_notebook) that doesn't exist on the real SDK
                    at all, caught the resulting AttributeError (and, via a bare `except ... Exception`,
                    every other possible failure too) and reported a fake 'Skipped' success. Any real
                    failure here PROPAGATES -- no broad except-and-report-fake-success."""
                    if component_self.dry_run:
                        context.log.info(f"[DRY RUN] would execute notebook {_notebook_name!r} -- parameters={_item['parameters']}")
                        return {"notebook_name": _notebook_name, "status": "dry_run — not executed"}
                    import uuid
                    from azure.synapse.artifacts.models import RunNotebookParameter, RunNotebookRequest

                    artifacts_client = component_self._get_artifacts_client()
                    notebook = artifacts_client.notebook.get_notebook(_notebook_name)
                    context.log.info(f"Submitting notebook: {_notebook_name}")

                    def _infer_type(v: Any) -> str:
                        if isinstance(v, bool):
                            return "bool"
                        if isinstance(v, int):
                            return "int"
                        if isinstance(v, float):
                            return "float"
                        return "string"

                    run_params = {
                        k: RunNotebookParameter(type=_infer_type(v), value=v)
                        for k, v in (_item["parameters"] or {}).items()
                    } or None

                    run_id = str(uuid.uuid4())
                    artifacts_client.run_notebook.begin_create_run(
                        run_id, RunNotebookRequest(notebook=notebook, parameters=run_params)
                    ).result()  # only confirms the create-run request was accepted, NOT completion
                    context.log.info(f"Notebook run created: run_id={run_id}")

                    _TERMINAL_STATES = {"success", "error", "dead", "killed"}
                    max_wait_minutes = 60
                    poll_interval = 15
                    elapsed = 0
                    while True:
                        status = artifacts_client.run_notebook.get_status(run_id)
                        result = getattr(status, "result", None)
                        run_status = getattr(result, "run_status", None) if result is not None else None
                        context.log.info(f"Notebook run {run_id} status: {run_status}")
                        if run_status in _TERMINAL_STATES:
                            if run_status != "success":
                                raise RuntimeError(f"Notebook {_notebook_name!r} (run {run_id}) finished with run_status={run_status!r}.")
                            return {"notebook_name": _notebook_name, "run_id": run_id, "run_status": run_status}
                        if elapsed >= max_wait_minutes * 60:
                            raise TimeoutError(
                                f"Notebook {_notebook_name!r} (run {run_id}) exceeded timeout of "
                                f"{max_wait_minutes} minutes (last run_status={run_status!r})."
                            )
                        time.sleep(poll_interval)
                        elapsed += poll_interval
                return notebook_asset

            assets.append(_make_notebook_asset())

        return assets

    def _get_observation_sensor(self):
        """Generate sensor to observe pipeline runs. Builds its own Azure client fresh at each tick
        (inside the sensor function body below) -- never at build_defs/component-load time."""

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

            # Emit asset materializations for completed pipeline runs.
            # NOTE: Sensor evaluations can only return SensorResult /
            # RunRequest / SkipReason. Direct `yield AssetMaterialization`
            # is silently dropped — collect and return via SensorResult.
            asset_events: List[AssetMaterialization] = []
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

                    asset_events.append(AssetMaterialization(
                        asset_key=asset_key,
                        metadata=metadata,
                    ))

            return SensorResult(
                asset_events=asset_events,
                cursor=now.isoformat(),
            )

        return synapse_observation_sensor

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Azure Synapse', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the imported Synapse workspace.

        No Azure client (nor credential) is constructed here unless something actually needs live
        discovery: a placeholder/unresolved tenant_id used to fail CODE-LOCATION LOAD (azure-identity
        validates it eagerly) even when every enabled category was declared statically via
        `notebooks:`/`spark_jobs:` -- because build_defs used to build both clients unconditionally,
        up front, regardless of whether anything was actually going to use them (the management client
        was never even used afterward). Each category now only triggers client construction when it
        truly needs live discovery, and that construction is cached/shared across categories via
        `_client_factory` rather than repeated.
        """
        assets = []
        sensors = []
        _cached_client: Dict[str, Any] = {}

        def _client_factory():
            if "client" not in _cached_client:
                _cached_client["client"] = self._get_artifacts_client()
            return _cached_client["client"]

        if self.import_pipelines:
            assets.extend(self._get_pipeline_assets(_client_factory()))

        if self.import_spark_jobs:
            assets.extend(self._get_spark_job_assets(self._resolve_spark_job_items(_client_factory)))

        if self.import_notebooks:
            assets.extend(self._get_notebook_assets(self._resolve_notebook_items(_client_factory)))

        if self.generate_sensor and self.import_pipelines:
            sensors.append(self._get_observation_sensor())

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
