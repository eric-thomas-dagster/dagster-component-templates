"""Google Cloud Run Jobs Component.

Import Google Cloud Run Jobs as Dagster assets for executing containerized batch workloads.
"""

import re
from typing import Optional, List, Dict, Any

from google.cloud import run_v2
from google.api_core import exceptions
from google.oauth2 import service_account

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class GoogleCloudRunJobsComponent(Component, Model, Resolvable):
    """Component for importing Google Cloud Run Jobs as Dagster assets.

    Supports importing:
    - Jobs (execute containerized batch workloads)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleCloudRunJobsComponent
        attributes:
          project_id: my-gcp-project
          location: us-central1
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_jobs: true
        ```
    """

    project_id: str = Field(description="GCP project ID")

    location: str = Field(
        default="us-central1",
        description="GCP location/region for Cloud Run Jobs"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional)"
    )

    import_jobs: bool = Field(
        default=True,
        description="Import Cloud Run Jobs as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    group_name: str = Field(
        default="google_cloud_run_jobs",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Cloud Run Jobs component"
    )

    def _get_client(self) -> run_v2.JobsClient:
        """Create Cloud Run Jobs client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return run_v2.JobsClient(credentials=credentials)
        return run_v2.JobsClient()

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False
        return True

    def _list_jobs(self, client: run_v2.JobsClient) -> List[Dict[str, Any]]:
        """List all Cloud Run Jobs."""
        jobs = []
        parent = f"projects/{self.project_id}/locations/{self.location}"

        try:
            for job in client.list_jobs(parent=parent):
                job_name = job.name.split("/")[-1]
                if self._matches_filters(job_name):
                    jobs.append({
                        "name": job_name,
                        "full_name": job.name,
                    })
        except exceptions.GoogleAPICallError:
            pass

        return jobs

    def _get_job_assets(self, client: run_v2.JobsClient) -> List:
        """Generate Cloud Run Job assets."""
        assets = []
        jobs = self._list_jobs(client)

        for job_info in jobs:
            job_name = job_info["name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', job_name)
            asset_key = f"cloud_run_job_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "project": self.project_id,
                    "location": self.location,
                },
            )
            def job_asset(context: AssetExecutionContext, job_info=job_info):
                """Execute Cloud Run Job."""
                client = self._get_client()

                try:
                    # Run the job
                    request = run_v2.RunJobRequest(name=job_info["full_name"])
                    operation = client.run_job(request=request)

                    context.log.info(f"Started Cloud Run Job: {job_info['name']}")

                    # Note: Actual implementation would wait for operation completion
                    metadata = {
                        "job_name": job_info["name"],
                        "operation": operation.operation.name if hasattr(operation, 'operation') else "N/A",
                        "note": "Job execution started - implement operation polling for completion status"
                    }

                    return metadata

                except exceptions.GoogleAPICallError as e:
                    context.log.error(f"Failed to run job: {e}")
                    raise

            assets.append(job_asset)

        return assets

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        assets = []

        if self.import_jobs:
            client = self._get_client()
            assets.extend(self._get_job_assets(client))

        return Definitions(assets=assets)
