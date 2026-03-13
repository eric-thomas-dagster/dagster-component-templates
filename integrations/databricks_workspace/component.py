"""Databricks Workspace Component.

Import Databricks jobs, notebooks, Delta Live Tables pipelines, and ML model endpoints
as Dagster assets with automatic lineage discovery and observation.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.pipelines import PipelineStateInfo
from databricks.sdk.service.serving import ServingEndpoint

import dagster as dg
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
    EventLogEntry,
    Resolvable,
    Model,
    MetadataValue,
    MaterializeResult,
)
from pydantic import Field


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


def _apply_job_overrides(
    default_spec: dg.AssetSpec,
    job_name: str,
    overrides: Optional[dict],
) -> list:
    """Apply assets_by_job_name overrides. Returns list (1 usually, >1 if one job -> multiple assets)."""
    if not overrides or job_name not in overrides:
        return [default_spec]
    ov = overrides[job_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


class DatabricksWorkspaceComponent(Component, Model, Resolvable):
    """Component for importing Databricks workspace entities as Dagster assets.

    Supports importing:
    - Jobs (with automatic detection of root vs downstream tasks)
    - Standalone notebooks
    - Delta Live Tables (DLT) pipelines
    - ML model serving endpoints

    Root entities (no upstream dependencies) are created as regular assets that Dagster
    can materialize. Downstream entities are created as observable source assets that
    Databricks orchestrates internally.

    Example:
        ```yaml
        type: dagster_component_templates.DatabricksWorkspaceComponent
        attributes:
          workspace_url: https://dbc-abc123.cloud.databricks.com
          access_token: "{{ env('DATABRICKS_TOKEN') }}"
          import_jobs: true
          import_notebooks: false
          filter_by_tags: production,analytics
        ```
    """

    workspace_url: str = Field(
        description="Databricks workspace URL (e.g., https://dbc-abc123.cloud.databricks.com)"
    )

    access_token: str = Field(
        description="Databricks personal access token"
    )

    import_jobs: bool = Field(
        default=True,
        description="Import Databricks jobs as assets"
    )

    import_notebooks: bool = Field(
        default=False,
        description="Import standalone notebooks as assets"
    )

    import_dlt_pipelines: bool = Field(
        default=False,
        description="Import Delta Live Tables (DLT) pipelines as materializable assets that can be triggered from Dagster"
    )

    import_model_endpoints: bool = Field(
        default=False,
        description="Import ML model serving endpoints as observable assets"
    )

    filter_by_tags: Optional[str] = Field(
        default=None,
        description="Comma-separated list of tags to filter entities"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    notebook_base_path: Optional[str] = Field(
        default=None,
        description="Base path for importing notebooks (e.g., /Users/user@company.com/production)"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="How often (in seconds) the sensor should check for completed runs"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Create a sensor to observe runs from Databricks"
    )

    group_name: Optional[str] = Field(
        default="databricks",
        description="Group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Databricks workspace component"
    )

    assets_by_job_name: Optional[dict] = Field(
        default=None,
        description=(
            "Override AssetSpec for specific Databricks jobs, keyed by job name. "
            "Each value may be a single override dict or a list of dicts (to produce "
            "multiple Dagster assets from one job). Supported keys per override: "
            "key, description, group_name, metadata, tags, kinds, deps."
        )
    )

    def _create_client(self) -> WorkspaceClient:
        """Create and return a Databricks workspace client."""
        return WorkspaceClient(
            host=self.workspace_url,
            token=self.access_token
        )

    def _should_include_entity(self, name: str, tags: Dict[str, str] = None) -> bool:
        """Check if an entity should be included based on filters."""
        # Check name exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Check name inclusion pattern
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Check tags filter
        if self.filter_by_tags and tags:
            filter_tags = [t.strip() for t in self.filter_by_tags.split(',')]
            entity_tags = list(tags.keys()) if isinstance(tags, dict) else []
            if not any(tag in entity_tags for tag in filter_tags):
                return False

        return True

    def _get_job_upstream_dependencies(self, job: Job) -> List[str]:
        """Extract upstream job dependencies from job configuration.

        Returns list of upstream job IDs that this job depends on.
        """
        dependencies = []

        if not job.settings or not job.settings.tasks:
            return dependencies

        # Check task dependencies within the job
        for task in job.settings.tasks:
            # Check for depends_on field
            if hasattr(task, 'depends_on') and task.depends_on:
                for dep in task.depends_on:
                    if hasattr(dep, 'task_key'):
                        # This is an internal task dependency, not a job dependency
                        pass

            # Check if task runs another job (job_task_settings)
            if hasattr(task, 'job_task_settings') and task.job_task_settings:
                job_id = task.job_task_settings.job_id
                if job_id:
                    dependencies.append(str(job_id))

        return dependencies

    def _is_root_job(self, job: Job, all_jobs: List[Job]) -> bool:
        """Determine if a job is a root job (no upstream Databricks dependencies)."""
        upstream_deps = self._get_job_upstream_dependencies(job)

        # If no upstream job dependencies, it's a root job
        return len(upstream_deps) == 0

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from Databricks workspace entities."""
        client = self._create_client()

        assets_list = []
        sensors_list = []

        # Track job and pipeline metadata for sensor
        job_metadata = {}
        dlt_pipeline_metadata = {}

        # Import Jobs
        if self.import_jobs:
            try:
                jobs = list(client.jobs.list())

                # First pass: collect all jobs and determine which are root
                jobs_to_import = []
                for job in jobs:
                    if not self._should_include_entity(
                        job.settings.name if job.settings else f"job_{job.job_id}",
                        job.settings.tags if job.settings else None
                    ):
                        continue
                    jobs_to_import.append(job)

                # Maps spec key tuple -> job dict (for multi-asset execution lookup)
                spec_key_to_job: Dict[tuple, dict] = {}

                # Second pass: create assets
                for job in jobs_to_import:
                    job_id = job.job_id
                    job_name = job.settings.name if job.settings else f"job_{job_id}"
                    # Sanitize name for asset key
                    asset_key = re.sub(r'[^a-zA-Z0-9_]', '_', job_name.lower())

                    is_root = self._is_root_job(job, jobs_to_import)

                    # Build default AssetSpec
                    default_spec = dg.AssetSpec(
                        key=dg.AssetKey([asset_key]),
                        description=f"Databricks job: {job_name}",
                        group_name=self.group_name,
                        metadata={
                            "databricks_job_id": job_id,
                            "databricks_job_name": job_name,
                            "databricks_workspace": self.workspace_url,
                            "entity_type": "job",
                        },
                    )

                    # Apply assets_by_job_name overrides (keyed by original job name)
                    expanded_specs = _apply_job_overrides(
                        default_spec, job_name, self.assets_by_job_name
                    )

                    # Register spec -> job mapping so the execution body can find job_id
                    job_dict = {
                        'job_id': job_id,
                        'job_name': job_name,
                        'is_root': is_root,
                    }
                    for spec in expanded_specs:
                        spec_key_to_job[tuple(spec.key.path)] = job_dict

                    # Store metadata for sensor (use first spec key for backwards compat)
                    sensor_key = tuple(expanded_specs[0].key.path)
                    job_metadata["/".join(sensor_key)] = {
                        'job_id': job_id,
                        'job_name': job_name,
                        'is_root': is_root,
                    }

                    if is_root:
                        # Root job - create regular asset that can be materialized
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Databricks job: {job_name}",
                            metadata={
                                "databricks_job_id": job_id,
                                "databricks_job_name": job_name,
                                "databricks_workspace": self.workspace_url,
                                "entity_type": "job",
                            }
                        )
                        def _job_asset(context: AssetExecutionContext):
                            """Materialize by triggering Databricks job run."""
                            client = self._create_client()

                            # Trigger job run
                            run = client.jobs.run_now(job_id=job_id)
                            context.log.info(f"Triggered Databricks job {job_name} (ID: {job_id}), run_id: {run.run_id}")

                            # Wait for completion (with timeout)
                            run_result = client.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)

                            run_metadata = {
                                "run_id": run.run_id,
                                "run_state": str(run_result.state.life_cycle_state),
                                "run_url": run_result.run_page_url,
                            }

                            context.log.info(f"Job completed with state: {run_result.state.life_cycle_state}")

                            # Yield MaterializeResult for all specs mapped to this job
                            selected_keys = [
                                dg.AssetKey(list(k))
                                for k, jd in spec_key_to_job.items()
                                if jd['job_id'] == job_id
                            ]
                            for ak in selected_keys:
                                yield MaterializeResult(asset_key=ak, metadata=run_metadata)

                        assets_list.append(_job_asset)

                    else:
                        # Downstream job - create observable source asset
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Databricks job: {job_name} (orchestrated by Databricks)",
                            metadata={
                                "databricks_job_id": job_id,
                                "databricks_job_name": job_name,
                                "databricks_workspace": self.workspace_url,
                                "entity_type": "job",
                            }
                        )
                        def _observable_job_asset(context: AssetExecutionContext):
                            """Observable asset - Databricks manages orchestration."""
                            client = self._create_client()

                            # Get recent runs to report observations
                            runs = client.jobs.list_runs(job_id=job_id, limit=1)
                            for run in runs:
                                context.log.info(f"Latest run: {run.run_id}, state: {run.state.life_cycle_state}")

                        assets_list.append(_observable_job_asset)

            except Exception as e:
                context.log.error(f"Error importing Databricks jobs: {e}")

        # Import DLT Pipelines
        if self.import_dlt_pipelines:
            try:
                pipelines = list(client.pipelines.list_pipelines())

                for pipeline in pipelines:
                    if not self._should_include_entity(pipeline.name or f"pipeline_{pipeline.pipeline_id}"):
                        continue

                    pipeline_id = pipeline.pipeline_id
                    pipeline_name = pipeline.name or f"pipeline_{pipeline_id}"
                    asset_key = f"dlt_{re.sub(r'[^a-zA-Z0-9_]', '_', pipeline_name.lower())}"

                    # Store metadata for sensor
                    dlt_pipeline_metadata[asset_key] = {
                        'pipeline_id': pipeline_id,
                        'pipeline_name': pipeline_name,
                    }

                    # DLT pipelines are materializable - they can be triggered via API
                    @asset(
                        name=asset_key,
                        group_name=self.group_name,
                        description=f"Delta Live Tables pipeline: {pipeline_name}",
                        metadata={
                            "databricks_pipeline_id": pipeline_id,
                            "databricks_pipeline_name": pipeline_name,
                            "databricks_workspace": self.workspace_url,
                            "entity_type": "dlt_pipeline",
                        }
                    )
                    def _dlt_pipeline_asset(context: AssetExecutionContext):
                        """Materialize by triggering DLT pipeline update."""
                        client = self._create_client()

                        # Trigger pipeline update
                        update = client.pipelines.start_update(pipeline_id=pipeline_id)
                        context.log.info(f"Triggered DLT pipeline {pipeline_name} (ID: {pipeline_id}), update_id: {update.update_id}")

                        # Wait for pipeline update to complete
                        try:
                            final_update = client.pipelines.wait_get_pipeline_idle(pipeline_id=pipeline_id)
                            context.log.info(f"DLT pipeline completed. State: {final_update.state}")

                            metadata = {
                                "update_id": update.update_id,
                                "pipeline_state": str(final_update.state),
                                "pipeline_id": pipeline_id,
                            }

                            # Add latest update info if available
                            if final_update.latest_updates and len(final_update.latest_updates) > 0:
                                latest = final_update.latest_updates[0]
                                metadata["update_state"] = str(latest.state) if latest.state else None

                        except Exception as e:
                            context.log.warning(f"Error waiting for pipeline completion: {e}. Pipeline may still be running.")
                            # Get current state as fallback
                            pipeline_info = client.pipelines.get(pipeline_id=pipeline_id)
                            metadata = {
                                "update_id": update.update_id,
                                "pipeline_state": str(pipeline_info.state),
                                "pipeline_id": pipeline_id,
                                "note": "Pipeline triggered but status check failed",
                            }

                        return metadata

                    assets_list.append(_dlt_pipeline_asset)

            except Exception as e:
                context.log.error(f"Error importing DLT pipelines: {e}")

        # Import Model Endpoints
        if self.import_model_endpoints:
            try:
                endpoints = list(client.serving_endpoints.list())

                for endpoint in endpoints:
                    if not self._should_include_entity(endpoint.name):
                        continue

                    endpoint_name = endpoint.name
                    asset_key = f"model_{re.sub(r'[^a-zA-Z0-9_]', '_', endpoint_name.lower())}"

                    # Model endpoints are observable (deployments, not training)
                    @observable_source_asset(
                        name=asset_key,
                        group_name=self.group_name,
                        description=f"Model serving endpoint: {endpoint_name}",
                        metadata={
                            "databricks_endpoint_name": endpoint_name,
                            "databricks_workspace": self.workspace_url,
                            "entity_type": "model_endpoint",
                        }
                    )
                    def _model_endpoint_asset(context: AssetExecutionContext):
                        """Observable model serving endpoint."""
                        client = self._create_client()

                        # Get endpoint state
                        endpoint_info = client.serving_endpoints.get(name=endpoint_name)
                        context.log.info(f"Model endpoint state: {endpoint_info.state}")

                    assets_list.append(_model_endpoint_asset)

            except Exception as e:
                context.log.error(f"Error importing model endpoints: {e}")

        # Create observation sensor if requested
        if self.generate_sensor and (job_metadata or dlt_pipeline_metadata):
            @sensor(
                name=f"{self.group_name}_observation_sensor",
                minimum_interval_seconds=self.poll_interval_seconds
            )
            def databricks_observation_sensor(context: SensorEvaluationContext):
                """Sensor to observe Databricks job runs and DLT pipeline updates, emitting AssetMaterialization events."""
                client = self._create_client()

                # Check for completed job runs
                for asset_key, metadata in job_metadata.items():
                    job_id = metadata['job_id']

                    try:
                        # Get recent runs
                        runs = client.jobs.list_runs(job_id=job_id, limit=5)

                        for run in runs:
                            # Only emit for successful completions
                            if run.state.life_cycle_state == "TERMINATED" and run.state.result_state == "SUCCESS":
                                # Check if we've already seen this run
                                # (In production, you'd track this in sensor state)
                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "run_id": run.run_id,
                                        "run_url": run.run_page_url,
                                        "start_time": str(run.start_time) if run.start_time else None,
                                        "end_time": str(run.end_time) if run.end_time else None,
                                        "source": "databricks_observation_sensor",
                                        "entity_type": "job",
                                    }
                                )
                    except Exception as e:
                        context.log.error(f"Error checking runs for job {job_id}: {e}")

                # Check for completed DLT pipeline updates
                for asset_key, metadata in dlt_pipeline_metadata.items():
                    pipeline_id = metadata['pipeline_id']

                    try:
                        # Get pipeline info with recent updates
                        pipeline_info = client.pipelines.get(pipeline_id=pipeline_id)

                        # Check recent updates
                        if pipeline_info.latest_updates:
                            for update in pipeline_info.latest_updates[:5]:  # Check last 5 updates
                                # Only emit for successful completions
                                if update.state and str(update.state).upper() == "COMPLETED":
                                    # Check if we've already seen this update
                                    # (In production, you'd track this in sensor state)
                                    update_metadata = {
                                        "update_id": update.update_id,
                                        "pipeline_id": pipeline_id,
                                        "source": "databricks_observation_sensor",
                                        "entity_type": "dlt_pipeline",
                                    }

                                    # Add timing info if available
                                    if hasattr(update, 'creation_time') and update.creation_time:
                                        update_metadata["start_time"] = str(update.creation_time)

                                    yield AssetMaterialization(
                                        asset_key=asset_key,
                                        metadata=update_metadata
                                    )
                    except Exception as e:
                        context.log.error(f"Error checking updates for DLT pipeline {pipeline_id}: {e}")

            sensors_list.append(databricks_observation_sensor)

        return Definitions(
            assets=assets_list,
            sensors=sensors_list if sensors_list else None,
        )
