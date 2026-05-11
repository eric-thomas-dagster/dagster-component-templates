"""DataformRepositoryResourceComponent — YAML wrapper around dagster_dataform.DataformRepositoryResource.

`dagster-dataform` (community package) ships the full integration:
  - DataformRepositoryResource (compile, invoke workflows, load assets + checks)
  - dataform_workflow_invocation_sensor
  - create_dataform_orchestration_schedule

This component exposes the resource via YAML so component-style projects
don't need to write Python to instantiate it. After that, all standard
dagster-dataform APIs work — schedules, sensors, asset loading.

Usage:
  After installing this component, reference the resource from any other
  Python module:

    from dagster_dataform import create_dataform_orchestration_schedule

    @dg.definitions
    def defs(context: dg.ComponentLoadContext):
        dataform = context.resources["dataform_repo"]
        sched = create_dataform_orchestration_schedule(
            dataform_repository_resource=dataform,
            cron_schedule="0 6 * * *",
        )
        return dg.Definitions(assets=dataform.assets,
                              asset_checks=dataform.asset_checks,
                              schedules=[sched])
"""

import json
import os
from typing import Any, Dict, Optional

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from pydantic import Field


class DataformRepositoryResourceComponent(Component, Model, Resolvable):
    """Expose `dagster_dataform.DataformRepositoryResource` via YAML config."""

    resource_key: str = Field(
        default="dataform_repo",
        description="Key under which the resource is exposed in Definitions.resources.",
    )

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    project_id: Optional[str] = Field(default=None, description="GCP project. Default: project from credentials.")
    location: str = Field(description="Dataform location (e.g. 'us-central1', 'europe-west1').")
    repository_id: str = Field(description="Dataform repository id within the project/location.")
    environment: str = Field(
        description="Dataform release environment (a Dataform concept — typically 'production', 'staging', etc.).",
    )

    sensor_minimum_interval_seconds: int = Field(default=120)
    asset_fresh_policy_lag_minutes: float = Field(default=1440)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        project_id = self.project_id or creds_dict.get("project_id")
        resource_key = self.resource_key

        try:
            from dagster_dataform import DataformRepositoryResource
            from google.cloud import dataform_v1
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "pip install dagster-dataform google-cloud-dataform google-auth — "
                "this component wraps dagster_dataform.DataformRepositoryResource."
            )

        sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = dataform_v1.DataformClient(credentials=sa_creds)

        resource = DataformRepositoryResource(
            project_id=project_id,
            location=self.location,
            repository_id=self.repository_id,
            environment=self.environment,
            sensor_minimum_interval_seconds=self.sensor_minimum_interval_seconds,
            client=client,
            asset_fresh_policy_lag_minutes=self.asset_fresh_policy_lag_minutes,
        )

        return Definitions(
            resources={resource_key: resource},
            assets=resource.assets,
            asset_checks=resource.asset_checks,
        )
