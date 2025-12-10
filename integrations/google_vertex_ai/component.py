"""Google Vertex AI Component.

Import Google Vertex AI training jobs, batch prediction jobs, custom jobs, and pipelines
as Dagster assets for orchestrating machine learning workflows.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from google.cloud import aiplatform
from google.api_core import exceptions
from google.oauth2 import service_account

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class GoogleVertexAIComponent(Component, Model, Resolvable):
    """Component for importing Google Vertex AI entities as Dagster assets.

    Supports importing:
    - Training Jobs (AutoML and custom training)
    - Batch Prediction Jobs (batch inference)
    - Custom Jobs (custom training code)
    - Pipelines (ML workflow orchestration)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleVertexAIComponent
        attributes:
          project_id: my-gcp-project
          location: us-central1
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_training_jobs: true
          import_batch_prediction_jobs: true
          import_pipelines: true
        ```
    """

    project_id: str = Field(
        description="GCP project ID"
    )

    location: str = Field(
        default="us-central1",
        description="GCP location/region for Vertex AI resources"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional if using default credentials)"
    )

    import_training_jobs: bool = Field(
        default=True,
        description="Import training job definitions as materializable assets"
    )

    import_batch_prediction_jobs: bool = Field(
        default=True,
        description="Import batch prediction job definitions as materializable assets"
    )

    import_custom_jobs: bool = Field(
        default=True,
        description="Import custom job definitions as materializable assets"
    )

    import_pipelines: bool = Field(
        default=True,
        description="Import pipeline definitions as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    filter_by_labels: Optional[str] = Field(
        default=None,
        description="Comma-separated label keys to filter entities (e.g., 'env,team')"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for completed jobs"
    )

    group_name: str = Field(
        default="google_vertex_ai",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Vertex AI component"
    )

    def _init_vertex_ai(self):
        """Initialize Vertex AI with credentials."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            aiplatform.init(
                project=self.project_id,
                location=self.location,
                credentials=credentials
            )
        else:
            aiplatform.init(
                project=self.project_id,
                location=self.location
            )

    def _matches_filters(self, name: str, labels: Optional[Dict[str, str]] = None) -> bool:
        """Check if entity matches name and label filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Label filter
        if self.filter_by_labels and labels:
            required_keys = [k.strip() for k in self.filter_by_labels.split(",")]
            if not all(key in labels for key in required_keys):
                return False

        return True

    def _list_training_jobs(self) -> List[Dict[str, Any]]:
        """List recent training jobs."""
        self._init_vertex_ai()
        jobs = []

        try:
            # Get recent successful training jobs as templates
            training_jobs = aiplatform.CustomTrainingJob.list(
                filter='state="JOB_STATE_SUCCEEDED"',
                order_by='create_time desc'
            )

            for job in training_jobs[:20]:  # Limit to 20 most recent
                display_name = job.display_name
                if self._matches_filters(display_name, job.labels):
                    jobs.append({
                        "display_name": display_name,
                        "resource_name": job.resource_name,
                        "state": job.state.name if job.state else "UNKNOWN",
                    })

        except exceptions.GoogleAPICallError as e:
            # May not have permissions or no jobs exist
            pass

        return jobs

    def _list_batch_prediction_jobs(self) -> List[Dict[str, Any]]:
        """List recent batch prediction jobs."""
        self._init_vertex_ai()
        jobs = []

        try:
            batch_jobs = aiplatform.BatchPredictionJob.list(
                filter='state="JOB_STATE_SUCCEEDED"',
                order_by='create_time desc'
            )

            for job in batch_jobs[:20]:
                display_name = job.display_name
                if self._matches_filters(display_name, job.labels):
                    jobs.append({
                        "display_name": display_name,
                        "resource_name": job.resource_name,
                        "state": job.state.name if job.state else "UNKNOWN",
                        "model": job.model_name if hasattr(job, 'model_name') else None,
                    })

        except exceptions.GoogleAPICallError as e:
            pass

        return jobs

    def _list_pipelines(self) -> List[Dict[str, Any]]:
        """List Vertex AI pipelines."""
        self._init_vertex_ai()
        pipelines = []

        try:
            # List pipeline jobs (executions)
            from google.cloud import aiplatform_v1

            client = aiplatform_v1.PipelineServiceClient()
            parent = f"projects/{self.project_id}/locations/{self.location}"

            request = aiplatform_v1.ListPipelineJobsRequest(
                parent=parent,
                filter='state="PIPELINE_STATE_SUCCEEDED"',
            )

            for pipeline_job in client.list_pipeline_jobs(request=request):
                display_name = pipeline_job.display_name
                pipeline_name = pipeline_job.name.split("/")[-1]

                if self._matches_filters(display_name):
                    pipelines.append({
                        "display_name": display_name,
                        "name": pipeline_name,
                        "resource_name": pipeline_job.name,
                        "pipeline_spec_uri": pipeline_job.template_uri if hasattr(pipeline_job, 'template_uri') else None,
                    })

                if len(pipelines) >= 20:  # Limit results
                    break

        except exceptions.GoogleAPICallError as e:
            pass

        return pipelines

    def _get_training_job_assets(self) -> List:
        """Generate training job assets."""
        assets = []
        jobs = self._list_training_jobs()

        for job_info in jobs:
            display_name = job_info["display_name"]
            # Create safe asset key
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', display_name)
            asset_key = f"training_job_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "display_name": display_name,
                    "job_type": "training",
                    "project": self.project_id,
                    "location": self.location,
                },
            )
            def training_asset(context: AssetExecutionContext, job_info=job_info):
                """Observe Vertex AI training job."""
                # Note: Actual job creation would require full job specification
                # This is a template showing how to observe job status

                metadata = {
                    "display_name": job_info["display_name"],
                    "state": job_info["state"],
                    "resource_name": job_info["resource_name"],
                    "note": "Template job - implement full training job creation logic with container spec, machine type, etc."
                }

                context.log.info(f"Training job template: {job_info['display_name']}")

                return metadata

            assets.append(training_asset)

        return assets

    def _get_batch_prediction_assets(self) -> List:
        """Generate batch prediction job assets."""
        assets = []
        jobs = self._list_batch_prediction_jobs()

        for job_info in jobs:
            display_name = job_info["display_name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', display_name)
            asset_key = f"batch_prediction_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "display_name": display_name,
                    "job_type": "batch_prediction",
                    "model": job_info.get("model"),
                    "project": self.project_id,
                },
            )
            def batch_prediction_asset(context: AssetExecutionContext, job_info=job_info):
                """Observe Vertex AI batch prediction job."""

                metadata = {
                    "display_name": job_info["display_name"],
                    "state": job_info["state"],
                    "model": job_info.get("model", "N/A"),
                    "resource_name": job_info["resource_name"],
                    "note": "Template job - implement full batch prediction creation logic"
                }

                context.log.info(f"Batch prediction job template: {job_info['display_name']}")

                return metadata

            assets.append(batch_prediction_asset)

        return assets

    def _get_pipeline_assets(self) -> List:
        """Generate pipeline assets."""
        assets = []
        pipelines = self._list_pipelines()

        for pipeline_info in pipelines:
            display_name = pipeline_info["display_name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', display_name)
            asset_key = f"pipeline_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "display_name": display_name,
                    "pipeline_name": pipeline_info["name"],
                    "project": self.project_id,
                },
            )
            def pipeline_asset(context: AssetExecutionContext, pipeline_info=pipeline_info):
                """Start Vertex AI pipeline execution."""
                # Note: Pipeline execution requires pipeline spec and parameters
                # This is a template

                metadata = {
                    "display_name": pipeline_info["display_name"],
                    "pipeline_name": pipeline_info["name"],
                    "pipeline_spec_uri": pipeline_info.get("pipeline_spec_uri", "N/A"),
                    "note": "Template pipeline - implement full pipeline execution with spec and parameters"
                }

                context.log.info(f"Pipeline template: {pipeline_info['display_name']}")

                return metadata

            assets.append(pipeline_asset)

        return assets

    def _get_observation_sensor(self):
        """Generate sensor to observe Vertex AI jobs."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def vertex_ai_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Google Vertex AI jobs and pipelines."""
            self._init_vertex_ai()

            # Observe completed training jobs
            if self.import_training_jobs:
                try:
                    recent_jobs = aiplatform.CustomTrainingJob.list(
                        filter='state="JOB_STATE_SUCCEEDED"',
                        order_by='create_time desc'
                    )

                    for job in recent_jobs[:10]:  # Limit to 10 most recent
                        display_name = job.display_name

                        if self._matches_filters(display_name, job.labels):
                            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', display_name)
                            asset_key = f"training_job_{safe_name}"

                            yield AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "display_name": MetadataValue.text(display_name),
                                    "state": MetadataValue.text(job.state.name if job.state else "UNKNOWN"),
                                },
                            )

                except exceptions.GoogleAPICallError as e:
                    context.log.warning(f"Failed to list training jobs: {e}")

        return vertex_ai_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        assets = []
        sensors = []

        # Import training jobs
        if self.import_training_jobs:
            assets.extend(self._get_training_job_assets())

        # Import batch prediction jobs
        if self.import_batch_prediction_jobs:
            assets.extend(self._get_batch_prediction_assets())

        # Import custom jobs (similar to training jobs)
        # if self.import_custom_jobs:
        #     assets.extend(self._get_custom_job_assets())

        # Import pipelines
        if self.import_pipelines:
            assets.extend(self._get_pipeline_assets())

        # Generate observation sensor
        if self.generate_sensor:
            sensors.append(self._get_observation_sensor())

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
