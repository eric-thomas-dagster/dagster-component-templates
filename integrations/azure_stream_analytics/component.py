"""Azure Stream Analytics Component.

Import Azure Stream Analytics jobs as Dagster assets with automatic observation and orchestration.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import time

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.streamanalytics import StreamAnalyticsManagementClient

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


class AzureStreamAnalyticsComponent(Component, Model, Resolvable):
    """Component for importing Azure Stream Analytics entities as Dagster assets.

    Supports importing:
    - Streaming Jobs (start/stop real-time analytics jobs)
    - Inputs (observe input sources)
    - Outputs (observe output destinations)

    Example:
        ```yaml
        type: dagster_component_templates.AzureStreamAnalyticsComponent
        attributes:
          subscription_id: "12345678-1234-1234-1234-123456789012"
          resource_group_name: my-resource-group
          tenant_id: "{{ env('AZURE_TENANT_ID') }}"
          client_id: "{{ env('AZURE_CLIENT_ID') }}"
          client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
          import_streaming_jobs: true
        ```
    """

    subscription_id: str = Field(
        description="Azure subscription ID"
    )

    resource_group_name: str = Field(
        description="Azure resource group name"
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

    import_streaming_jobs: bool = Field(
        default=True,
        description="Import streaming jobs as materializable assets"
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
        description="Generate observation sensor for streaming job status"
    )

    group_name: str = Field(
        default="azure_stream_analytics",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Azure Stream Analytics component"
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

    def _get_client(self) -> StreamAnalyticsManagementClient:
        """Create Stream Analytics management client."""
        credential = self._get_credential()
        return StreamAnalyticsManagementClient(credential, self.subscription_id)

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

    def _list_streaming_jobs(self, client: StreamAnalyticsManagementClient) -> List[Dict]:
        """List all streaming jobs."""
        jobs = []
        for job in client.streaming_jobs.list_by_resource_group(self.resource_group_name):
            if self._matches_filters(job.name, job.tags):
                jobs.append({
                    "name": job.name,
                    "job_state": job.job_state,
                    "sku": job.sku.name if job.sku else "standard",
                })
        return jobs

    def _get_streaming_job_assets(self, client: StreamAnalyticsManagementClient) -> List:
        """Generate streaming job assets."""
        assets = []
        jobs = self._list_streaming_jobs(client)

        for job_info in jobs:
            job_name = job_info["name"]
            asset_key = f"asa_job_{job_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "resource_group": self.resource_group_name,
                },
            )
            def streaming_job_asset(
                context: AssetExecutionContext,
                job_name=job_name,
            ):
                """Start Azure Stream Analytics job."""
                asa_client = self._get_client()

                # Get current job status
                job = asa_client.streaming_jobs.get(
                    self.resource_group_name,
                    job_name,
                )

                current_state = job.job_state
                context.log.info(f"Current job state: {current_state}")

                # Start job if not running
                if current_state in ["Created", "Stopped", "Failed"]:
                    context.log.info(f"Starting streaming job: {job_name}")

                    # Start the job
                    asa_client.streaming_jobs.begin_start(
                        self.resource_group_name,
                        job_name,
                    ).result()

                    context.log.info(f"Job {job_name} started")

                    # Wait for job to reach running state
                    max_wait = 300  # 5 minutes
                    elapsed = 0
                    poll_interval = 15

                    while elapsed < max_wait:
                        time.sleep(poll_interval)
                        elapsed += poll_interval

                        job = asa_client.streaming_jobs.get(
                            self.resource_group_name,
                            job_name,
                        )

                        state = job.job_state
                        context.log.info(f"Job state: {state}")

                        if state == "Running":
                            break
                        elif state in ["Failed", "Degraded"]:
                            context.log.warning(f"Job reached state: {state}")
                            break

                # Get job metrics
                job = asa_client.streaming_jobs.get(
                    self.resource_group_name,
                    job_name,
                )

                metadata = {
                    "job_name": job_name,
                    "job_state": job.job_state,
                    "sku": job.sku.name if job.sku else "standard",
                    "compatibility_level": job.compatibility_level,
                    "provisioning_state": job.provisioning_state,
                }

                if job.last_output_event_time:
                    metadata["last_output_event_time"] = str(job.last_output_event_time)

                return metadata

            assets.append(streaming_job_asset)

        return assets

    def _get_observation_sensor(self, client: StreamAnalyticsManagementClient):
        """Generate sensor to observe streaming job status."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def asa_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Azure Stream Analytics job status."""
            asa_client = self._get_client()

            # Get all streaming jobs
            jobs = self._list_streaming_jobs(asa_client)

            for job_info in jobs:
                job_name = job_info["name"]

                # Get job details
                job = asa_client.streaming_jobs.get(
                    self.resource_group_name,
                    job_name,
                )

                # Emit materialization for running jobs
                if job.job_state in ["Running", "Degraded"]:
                    asset_key = f"asa_job_{job_name}"

                    metadata = {
                        "job_name": MetadataValue.text(job_name),
                        "job_state": MetadataValue.text(job.job_state),
                        "sku": MetadataValue.text(job.sku.name if job.sku else "standard"),
                        "provisioning_state": MetadataValue.text(job.provisioning_state),
                    }

                    if job.last_output_event_time:
                        metadata["last_output_event_time"] = MetadataValue.text(
                            str(job.last_output_event_time)
                        )

                    yield AssetMaterialization(
                        asset_key=asset_key,
                        metadata=metadata,
                    )

        return asa_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        client = self._get_client()

        assets = []
        sensors = []

        # Import streaming jobs
        if self.import_streaming_jobs:
            assets.extend(self._get_streaming_job_assets(client))

        # Generate observation sensor
        if self.generate_sensor and self.import_streaming_jobs:
            sensors.append(self._get_observation_sensor(client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
