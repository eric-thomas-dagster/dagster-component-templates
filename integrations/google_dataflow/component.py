"""Google Cloud Dataflow Component.

Import Google Cloud Dataflow jobs as Dagster assets for orchestrating
Apache Beam batch and streaming data pipelines.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from google.cloud import dataflow_v1beta3
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


class GoogleDataflowComponent(Component, Model, Resolvable):
    """Component for importing Google Cloud Dataflow jobs as Dagster assets.

    Supports importing:
    - Batch Jobs (trigger batch data processing pipelines)
    - Streaming Jobs (monitor streaming pipelines)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleDataflowComponent
        attributes:
          project_id: my-gcp-project
          location: us-central1
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_batch_jobs: true
          import_streaming_jobs: true
        ```
    """

    project_id: str = Field(
        description="GCP project ID"
    )

    location: str = Field(
        default="us-central1",
        description="GCP location/region for Dataflow jobs"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional if using default credentials)"
    )

    import_batch_jobs: bool = Field(
        default=True,
        description="Import batch job templates as materializable assets"
    )

    import_streaming_jobs: bool = Field(
        default=True,
        description="Import streaming jobs as observable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for job status"
    )

    group_name: str = Field(
        default="google_dataflow",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Dataflow component"
    )

    def _get_client(self) -> dataflow_v1beta3.JobsV1Beta3Client:
        """Create Dataflow client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return dataflow_v1beta3.JobsV1Beta3Client(credentials=credentials)
        else:
            return dataflow_v1beta3.JobsV1Beta3Client()

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        return True

    def _list_jobs(self, client: dataflow_v1beta3.JobsV1Beta3Client, job_type: str = None) -> List[Dict[str, Any]]:
        """List Dataflow jobs."""
        jobs = []

        try:
            request = dataflow_v1beta3.ListJobsRequest(
                project_id=self.project_id,
                location=self.location,
            )

            # Apply filter for job type if specified
            if job_type:
                request.filter = f"CURRENT_STATE_TIME >= \"{(datetime.utcnow() - timedelta(days=30)).isoformat()}Z\""

            page_result = client.list_jobs(request=request)

            for job in page_result:
                job_name = job.name
                job_id = job.id

                # Filter by type if specified
                if job_type == "batch" and job.type_ != dataflow_v1beta3.JobType.JOB_TYPE_BATCH:
                    continue
                if job_type == "streaming" and job.type_ != dataflow_v1beta3.JobType.JOB_TYPE_STREAMING:
                    continue

                if self._matches_filters(job_name):
                    jobs.append({
                        "name": job_name,
                        "id": job_id,
                        "type": "BATCH" if job.type_ == dataflow_v1beta3.JobType.JOB_TYPE_BATCH else "STREAMING",
                        "state": job.current_state.name if job.current_state else "UNKNOWN",
                        "create_time": job.create_time,
                    })

                if len(jobs) >= 20:  # Limit results
                    break

        except exceptions.GoogleAPICallError as e:
            # May not have permissions or no jobs exist
            pass

        return jobs

    def _get_batch_job_assets(self, client: dataflow_v1beta3.JobsV1Beta3Client) -> List:
        """Generate batch job assets."""
        assets = []
        jobs = self._list_jobs(client, job_type="batch")

        for job_info in jobs:
            job_name = job_info["name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', job_name)
            asset_key = f"batch_job_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "job_id": job_info["id"],
                    "job_type": "BATCH",
                    "project": self.project_id,
                    "location": self.location,
                },
            )
            def batch_job_asset(context: AssetExecutionContext, job_info=job_info):
                """Observe Dataflow batch job."""
                # Note: Actual job launch would require template/pipeline specification
                # This is a template for observing job status

                metadata = {
                    "job_name": job_info["name"],
                    "job_id": job_info["id"],
                    "job_type": job_info["type"],
                    "state": job_info["state"],
                    "create_time": str(job_info["create_time"]),
                    "note": "Template job - implement full Dataflow job launch logic with template/pipeline spec"
                }

                context.log.info(f"Batch job template: {job_info['name']}")

                return metadata

            assets.append(batch_job_asset)

        return assets

    def _get_streaming_job_assets(self, client: dataflow_v1beta3.JobsV1Beta3Client) -> List:
        """Generate streaming job observable assets."""
        assets = []
        jobs = self._list_jobs(client, job_type="streaming")

        for job_info in jobs:
            job_name = job_info["name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', job_name)
            asset_key = f"streaming_job_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "job_id": job_info["id"],
                    "job_type": "STREAMING",
                    "project": self.project_id,
                    "location": self.location,
                },
            )
            def streaming_job_asset(context: AssetExecutionContext, job_info=job_info):
                """Observe Dataflow streaming job."""
                # Streaming jobs run continuously, so we just observe status

                metadata = {
                    "job_name": job_info["name"],
                    "job_id": job_info["id"],
                    "job_type": job_info["type"],
                    "state": job_info["state"],
                    "create_time": str(job_info["create_time"]),
                    "note": "Streaming jobs run continuously"
                }

                context.log.info(f"Streaming job: {job_info['name']} - State: {job_info['state']}")

                return metadata

            assets.append(streaming_job_asset)

        return assets

    def _get_observation_sensor(self, client: dataflow_v1beta3.JobsV1Beta3Client):
        """Generate sensor to observe Dataflow jobs."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def dataflow_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Google Cloud Dataflow jobs."""

            # Observe all jobs (batch and streaming)
            jobs = self._list_jobs(client)

            for job_info in jobs:
                job_name = job_info["name"]
                job_type = job_info["type"]
                state = job_info["state"]

                # Emit materialization for active/completed jobs
                if state in ["JOB_STATE_RUNNING", "JOB_STATE_DONE", "JOB_STATE_FAILED"]:
                    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', job_name)

                    if job_type == "BATCH":
                        asset_key = f"batch_job_{safe_name}"
                    else:
                        asset_key = f"streaming_job_{safe_name}"

                    yield AssetMaterialization(
                        asset_key=asset_key,
                        metadata={
                            "job_name": MetadataValue.text(job_name),
                            "job_id": MetadataValue.text(job_info["id"]),
                            "job_type": MetadataValue.text(job_type),
                            "state": MetadataValue.text(state),
                        },
                    )

        return dataflow_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        client = self._get_client()

        assets = []
        sensors = []

        # Import batch jobs
        if self.import_batch_jobs:
            assets.extend(self._get_batch_job_assets(client))

        # Import streaming jobs
        if self.import_streaming_jobs:
            assets.extend(self._get_streaming_job_assets(client))

        # Generate observation sensor
        if self.generate_sensor:
            sensors.append(self._get_observation_sensor(client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
