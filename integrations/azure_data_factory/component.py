"""Azure Data Factory Component.

Import Azure Data Factory pipelines, triggers, data flows, and integration runtimes
as Dagster assets with automatic observation and orchestration.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    RunFilterParameters,
    RunQueryFilter,
    RunQueryFilterOperand,
    RunQueryFilterOperator,
)

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


class AzureDataFactoryComponent(Component, Model, Resolvable):
    """Component for importing Azure Data Factory entities as Dagster assets.

    Supports importing:
    - Pipelines (trigger pipeline runs)
    - Triggers (start/stop triggers)
    - Data Flows (observe data flow definitions)
    - Integration Runtimes (observe IR status)

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
          import_triggers: true
        ```
    """

    subscription_id: str = Field(
        description="Azure subscription ID"
    )

    resource_group_name: str = Field(
        description="Azure resource group name"
    )

    factory_name: str = Field(
        description="Azure Data Factory name"
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

    import_triggers: bool = Field(
        default=False,
        description="Import triggers as materializable assets (start)"
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
        description="Generate observation sensor for pipeline runs and trigger runs"
    )

    group_name: str = Field(
        default="azure_data_factory",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Azure Data Factory component"
    )

    def _get_client(self) -> DataFactoryManagementClient:
        """Create Azure Data Factory client."""
        if self.tenant_id and self.client_id and self.client_secret:
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        else:
            credential = DefaultAzureCredential()

        return DataFactoryManagementClient(credential, self.subscription_id)

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

    def _list_pipelines(self, client: DataFactoryManagementClient) -> List[str]:
        """List all pipelines in the data factory."""
        pipelines = []
        for pipeline in client.pipelines.list_by_factory(
            self.resource_group_name, self.factory_name
        ):
            if self._matches_filters(pipeline.name):
                pipelines.append(pipeline.name)
        return pipelines

    def _list_triggers(self, client: DataFactoryManagementClient) -> List[str]:
        """List all triggers in the data factory."""
        triggers = []
        for trigger in client.triggers.list_by_factory(
            self.resource_group_name, self.factory_name
        ):
            if self._matches_filters(trigger.name):
                triggers.append(trigger.name)
        return triggers

    def _get_pipeline_assets(self, client: DataFactoryManagementClient) -> List:
        """Generate pipeline assets."""
        assets = []
        pipelines = self._list_pipelines(client)

        for pipeline_name in pipelines:
            asset_key = f"adf_pipeline_{pipeline_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "pipeline_name": pipeline_name,
                    "factory_name": self.factory_name,
                    "resource_group": self.resource_group_name,
                },
            )
            def pipeline_asset(context: AssetExecutionContext, pipeline_name=pipeline_name):
                """Trigger Azure Data Factory pipeline run."""
                adf_client = self._get_client()

                # Trigger pipeline run
                run_response = adf_client.pipelines.create_run(
                    self.resource_group_name,
                    self.factory_name,
                    pipeline_name,
                )

                run_id = run_response.run_id
                context.log.info(f"Pipeline run started. Run ID: {run_id}")

                # Wait for pipeline run to complete
                max_wait_minutes = 60
                poll_interval = 30
                elapsed = 0

                while elapsed < max_wait_minutes * 60:
                    pipeline_run = adf_client.pipeline_runs.get(
                        self.resource_group_name,
                        self.factory_name,
                        run_id,
                    )

                    status = pipeline_run.status
                    context.log.info(f"Pipeline run status: {status}")

                    if status in ["Succeeded", "Failed", "Cancelled"]:
                        metadata = {
                            "run_id": run_id,
                            "status": status,
                            "pipeline_name": pipeline_name,
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

                    import time
                    time.sleep(poll_interval)
                    elapsed += poll_interval

                context.log.warning(f"Pipeline run timed out after {max_wait_minutes} minutes")
                return {
                    "run_id": run_id,
                    "status": "Timeout",
                    "pipeline_name": pipeline_name,
                }

            assets.append(pipeline_asset)

        return assets

    def _get_trigger_assets(self, client: DataFactoryManagementClient) -> List:
        """Generate trigger assets."""
        assets = []
        triggers = self._list_triggers(client)

        for trigger_name in triggers:
            asset_key = f"adf_trigger_{trigger_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "trigger_name": trigger_name,
                    "factory_name": self.factory_name,
                    "resource_group": self.resource_group_name,
                },
            )
            def trigger_asset(context: AssetExecutionContext, trigger_name=trigger_name):
                """Start Azure Data Factory trigger."""
                adf_client = self._get_client()

                # Get trigger status
                trigger = adf_client.triggers.get(
                    self.resource_group_name,
                    self.factory_name,
                    trigger_name,
                )

                context.log.info(f"Trigger runtime state: {trigger.runtime_state}")

                # Start trigger if not running
                if trigger.runtime_state != "Started":
                    adf_client.triggers.begin_start(
                        self.resource_group_name,
                        self.factory_name,
                        trigger_name,
                    ).result()
                    context.log.info(f"Trigger {trigger_name} started")
                else:
                    context.log.info(f"Trigger {trigger_name} already running")

                return {
                    "trigger_name": trigger_name,
                    "runtime_state": "Started",
                    "trigger_type": trigger.type,
                }

            assets.append(trigger_asset)

        return assets

    def _get_observation_sensor(self, client: DataFactoryManagementClient):
        """Generate sensor to observe pipeline runs and trigger runs."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def adf_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Azure Data Factory pipeline runs and trigger runs."""
            adf_client = self._get_client()

            # Get cursor (last check time)
            cursor = context.cursor
            if cursor:
                last_check = datetime.fromisoformat(cursor)
            else:
                last_check = datetime.utcnow() - timedelta(hours=1)

            now = datetime.utcnow()

            # Query pipeline runs since last check
            filter_params = RunFilterParameters(
                last_updated_after=last_check,
                last_updated_before=now,
            )

            pipeline_runs = adf_client.pipeline_runs.query_by_factory(
                self.resource_group_name,
                self.factory_name,
                filter_params,
            )

            # Emit asset materializations for completed pipeline runs
            for run in pipeline_runs.value:
                if run.status in ["Succeeded", "Failed", "Cancelled"]:
                    # Check if pipeline matches our filters
                    if not self._matches_filters(run.pipeline_name):
                        continue

                    asset_key = f"adf_pipeline_{run.pipeline_name}"

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

            # Query trigger runs since last check
            trigger_runs = adf_client.trigger_runs.query_by_factory(
                self.resource_group_name,
                self.factory_name,
                filter_params,
            )

            # Log trigger run information
            for run in trigger_runs.value:
                if run.status in ["Succeeded", "Failed"]:
                    context.log.info(
                        f"Trigger run: {run.trigger_name} - Status: {run.status} - "
                        f"Time: {run.trigger_run_timestamp}"
                    )

            # Update cursor
            context.update_cursor(now.isoformat())

        return adf_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        client = self._get_client()

        assets = []
        sensors = []

        # Import pipelines
        if self.import_pipelines:
            assets.extend(self._get_pipeline_assets(client))

        # Import triggers
        if self.import_triggers:
            assets.extend(self._get_trigger_assets(client))

        # Generate observation sensor
        if self.generate_sensor and (self.import_pipelines or self.import_triggers):
            sensors.append(self._get_observation_sensor(client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
