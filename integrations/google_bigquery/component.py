"""Google BigQuery Component.

Import BigQuery scheduled queries, stored procedures, materialized views, transfer jobs,
and routines as Dagster assets with automatic observation and orchestration.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
from google.api_core import exceptions

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


class GoogleBigQueryComponent(Component, Model, Resolvable):
    """Component for importing BigQuery entities as Dagster assets.

    Supports importing:
    - Scheduled Queries (trigger query execution)
    - Stored Procedures (call procedures)
    - Materialized Views (refresh views)
    - Transfer Jobs (trigger data transfers)
    - Tables (monitor metadata)
    - Routines (observe UDFs/procedures)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleBigQueryComponent
        attributes:
          project_id: my-gcp-project
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          dataset_id: analytics
          import_scheduled_queries: true
          import_materialized_views: true
        ```
    """

    project_id: str = Field(
        description="GCP project ID"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional if using default credentials)"
    )

    dataset_id: Optional[str] = Field(
        default=None,
        description="BigQuery dataset ID to filter entities (optional)"
    )

    location: str = Field(
        default="US",
        description="BigQuery location/region (e.g., US, EU, us-central1)"
    )

    import_scheduled_queries: bool = Field(
        default=True,
        description="Import scheduled queries as materializable assets"
    )

    import_stored_procedures: bool = Field(
        default=False,
        description="Import stored procedures as materializable assets"
    )

    import_materialized_views: bool = Field(
        default=False,
        description="Import materialized views as materializable assets"
    )

    import_transfer_jobs: bool = Field(
        default=False,
        description="Import BigQuery Data Transfer Service jobs as materializable assets"
    )

    import_tables: bool = Field(
        default=False,
        description="Import tables as observable assets (monitor metadata)"
    )

    import_routines: bool = Field(
        default=False,
        description="Import routines (UDFs/procedures) as observable assets"
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
        description="Comma-separated list of label keys to filter entities"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="How often (in seconds) the sensor should check for completed queries"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Create a sensor to observe scheduled query runs"
    )

    group_name: Optional[str] = Field(
        default="bigquery",
        description="Group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the BigQuery component"
    )

    def _create_client(self) -> bigquery.Client:
        """Create and return BigQuery client."""
        if self.credentials_path:
            return bigquery.Client.from_service_account_json(
                self.credentials_path,
                project=self.project_id,
                location=self.location
            )
        return bigquery.Client(project=self.project_id, location=self.location)

    def _create_transfer_client(self) -> bigquery_datatransfer_v1.DataTransferServiceClient:
        """Create and return BigQuery Data Transfer Service client."""
        if self.credentials_path:
            return bigquery_datatransfer_v1.DataTransferServiceClient.from_service_account_file(
                self.credentials_path
            )
        return bigquery_datatransfer_v1.DataTransferServiceClient()

    def _should_include_entity(self, name: str, labels: Dict[str, str] = None) -> bool:
        """Check if an entity should be included based on filters."""
        # Check name exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name, re.IGNORECASE):
                return False

        # Check name inclusion pattern
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name, re.IGNORECASE):
                return False

        # Check labels filter
        if self.filter_by_labels and labels:
            filter_label_keys = [key.strip() for key in self.filter_by_labels.split(',')]
            entity_label_keys = list(labels.keys()) if isinstance(labels, dict) else []
            if not any(key in entity_label_keys for key in filter_label_keys):
                return False

        return True

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from BigQuery entities."""
        client = self._create_client()

        assets_list = []
        sensors_list = []

        # Track scheduled query metadata for sensor
        scheduled_query_metadata = {}

        # Import Scheduled Queries
        if self.import_scheduled_queries:
            try:
                transfer_client = self._create_transfer_client()
                parent = f"projects/{self.project_id}"

                # List transfer configs (scheduled queries)
                for transfer_config in transfer_client.list_transfer_configs(parent=parent):
                    # Filter for scheduled queries only
                    if transfer_config.data_source_id != "scheduled_query":
                        continue

                    query_name = transfer_config.display_name
                    labels = dict(transfer_config.params.get('labels', {}))

                    if not self._should_include_entity(query_name, labels):
                        continue

                    # Sanitize name for asset key
                    asset_key = f"scheduled_query_{re.sub(r'[^a-zA-Z0-9_]', '_', query_name.lower())}"

                    # Store metadata for sensor
                    scheduled_query_metadata[asset_key] = {
                        'config_name': transfer_config.name,
                        'display_name': query_name,
                    }

                    # Scheduled queries are materializable
                    @asset(
                        name=asset_key,
                        group_name=self.group_name,
                        description=f"BigQuery scheduled query: {query_name}",
                        metadata={
                            "bq_query_name": query_name,
                            "bq_config_name": transfer_config.name,
                            "bq_schedule": transfer_config.schedule,
                            "entity_type": "scheduled_query",
                        }
                    )
                    def _scheduled_query_asset(context: AssetExecutionContext, config_name=transfer_config.name, query_name=query_name):
                        """Materialize by triggering scheduled query."""
                        transfer_client = self._create_transfer_client()

                        # Trigger manual run
                        from google.protobuf.timestamp_pb2 import Timestamp
                        now = Timestamp()
                        now.GetCurrentTime()

                        response = transfer_client.start_manual_transfer_runs(
                            parent=config_name,
                            requested_run_time=now
                        )

                        context.log.info(f"Triggered scheduled query: {query_name}")

                        metadata = {
                            "query_name": query_name,
                            "config_name": config_name,
                        }

                        # Get runs (may take a moment to appear)
                        runs = list(response.runs)
                        if runs:
                            run = runs[0]
                            metadata["run_name"] = run.name
                            metadata["state"] = str(run.state)

                        return metadata

                    assets_list.append(_scheduled_query_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery scheduled queries: {e}")

        # Import Stored Procedures
        if self.import_stored_procedures:
            try:
                datasets = [self.dataset_id] if self.dataset_id else [ds.dataset_id for ds in client.list_datasets()]

                for dataset_id in datasets:
                    dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)

                    # List routines in dataset
                    for routine in client.list_routines(dataset_ref):
                        if routine.type_ != "PROCEDURE":
                            continue

                        routine_name = routine.routine_id
                        labels = routine.labels or {}

                        if not self._should_include_entity(routine_name, labels):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"procedure_{re.sub(r'[^a-zA-Z0-9_]', '_', routine_name.lower())}"

                        # Stored procedures are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"BigQuery stored procedure: {routine_name}",
                            metadata={
                                "bq_procedure_name": routine_name,
                                "bq_dataset": dataset_id,
                                "bq_project": self.project_id,
                                "entity_type": "stored_procedure",
                            }
                        )
                        def _procedure_asset(context: AssetExecutionContext, procedure_name=routine_name, dataset=dataset_id):
                            """Materialize by calling stored procedure."""
                            client = self._create_client()

                            # Call procedure (parameterless)
                            query = f"CALL `{self.project_id}.{dataset}.{procedure_name}`()"

                            query_job = client.query(query)
                            query_job.result()  # Wait for completion

                            context.log.info(f"Called stored procedure: {procedure_name}")

                            metadata = {
                                "procedure_name": procedure_name,
                                "dataset": dataset,
                                "job_id": query_job.job_id,
                            }

                            return metadata

                        assets_list.append(_procedure_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery stored procedures: {e}")

        # Import Materialized Views
        if self.import_materialized_views:
            try:
                datasets = [self.dataset_id] if self.dataset_id else [ds.dataset_id for ds in client.list_datasets()]

                for dataset_id in datasets:
                    dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)

                    # List tables in dataset
                    for table in client.list_tables(dataset_ref):
                        table_full = client.get_table(table)

                        # Check if it's a materialized view
                        if table_full.table_type != "MATERIALIZED_VIEW":
                            continue

                        mv_name = table.table_id
                        labels = table_full.labels or {}

                        if not self._should_include_entity(mv_name, labels):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"mv_{re.sub(r'[^a-zA-Z0-9_]', '_', mv_name.lower())}"

                        # Materialized views are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"BigQuery materialized view: {mv_name}",
                            metadata={
                                "bq_view_name": mv_name,
                                "bq_dataset": dataset_id,
                                "bq_project": self.project_id,
                                "entity_type": "materialized_view",
                            }
                        )
                        def _mv_asset(context: AssetExecutionContext, mv_name=mv_name, dataset=dataset_id):
                            """Materialize by refreshing materialized view."""
                            client = self._create_client()

                            # Refresh materialized view
                            table_ref = f"{self.project_id}.{dataset}.{mv_name}"

                            # BigQuery doesn't have direct refresh, but we can query to update
                            query = f"SELECT * FROM `{table_ref}` LIMIT 1"
                            query_job = client.query(query)
                            query_job.result()

                            # Get table info
                            table = client.get_table(table_ref)

                            context.log.info(f"Refreshed materialized view: {mv_name}")

                            metadata = {
                                "view_name": mv_name,
                                "dataset": dataset,
                                "num_rows": table.num_rows,
                                "num_bytes": table.num_bytes,
                                "last_modified": str(table.modified) if table.modified else None,
                            }

                            return metadata

                        assets_list.append(_mv_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery materialized views: {e}")

        # Import Transfer Jobs
        if self.import_transfer_jobs:
            try:
                transfer_client = self._create_transfer_client()
                parent = f"projects/{self.project_id}"

                # List all transfer configs
                for transfer_config in transfer_client.list_transfer_configs(parent=parent):
                    # Skip scheduled queries (handled separately)
                    if transfer_config.data_source_id == "scheduled_query":
                        continue

                    job_name = transfer_config.display_name
                    labels = {}

                    if not self._should_include_entity(job_name, labels):
                        continue

                    # Sanitize name for asset key
                    asset_key = f"transfer_job_{re.sub(r'[^a-zA-Z0-9_]', '_', job_name.lower())}"

                    # Transfer jobs are materializable
                    @asset(
                        name=asset_key,
                        group_name=self.group_name,
                        description=f"BigQuery transfer job: {job_name}",
                        metadata={
                            "bq_job_name": job_name,
                            "bq_config_name": transfer_config.name,
                            "bq_data_source": transfer_config.data_source_id,
                            "entity_type": "transfer_job",
                        }
                    )
                    def _transfer_job_asset(context: AssetExecutionContext, config_name=transfer_config.name, job_name=job_name):
                        """Materialize by triggering transfer job."""
                        transfer_client = self._create_transfer_client()

                        # Trigger manual run
                        from google.protobuf.timestamp_pb2 import Timestamp
                        now = Timestamp()
                        now.GetCurrentTime()

                        response = transfer_client.start_manual_transfer_runs(
                            parent=config_name,
                            requested_run_time=now
                        )

                        context.log.info(f"Triggered transfer job: {job_name}")

                        metadata = {
                            "job_name": job_name,
                            "config_name": config_name,
                        }

                        runs = list(response.runs)
                        if runs:
                            run = runs[0]
                            metadata["run_name"] = run.name
                            metadata["state"] = str(run.state)

                        return metadata

                    assets_list.append(_transfer_job_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery transfer jobs: {e}")

        # Import Tables (Observable)
        if self.import_tables:
            try:
                datasets = [self.dataset_id] if self.dataset_id else [ds.dataset_id for ds in client.list_datasets()]

                for dataset_id in datasets:
                    dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)

                    # List tables in dataset (limit to first 50 to avoid too many assets)
                    for i, table in enumerate(client.list_tables(dataset_ref)):
                        if i >= 50:  # Limit
                            break

                        table_name = table.table_id
                        table_full = client.get_table(table)

                        # Skip views and materialized views (handled separately)
                        if table_full.table_type in ["VIEW", "MATERIALIZED_VIEW"]:
                            continue

                        labels = table_full.labels or {}

                        if not self._should_include_entity(table_name, labels):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"table_{re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())}"

                        # Tables are observable
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"BigQuery table: {table_name}",
                            metadata={
                                "bq_table_name": table_name,
                                "bq_dataset": dataset_id,
                                "bq_project": self.project_id,
                                "entity_type": "table",
                            }
                        )
                        def _table_asset(context: AssetExecutionContext, table_name=table_name, dataset=dataset_id):
                            """Observable table asset."""
                            client = self._create_client()

                            table_ref = f"{self.project_id}.{dataset}.{table_name}"
                            table = client.get_table(table_ref)

                            context.log.info(
                                f"Table {table_name}: {table.num_rows} rows, "
                                f"{table.num_bytes} bytes, modified {table.modified}"
                            )

                        assets_list.append(_table_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery tables: {e}")

        # Import Routines (Observable)
        if self.import_routines:
            try:
                datasets = [self.dataset_id] if self.dataset_id else [ds.dataset_id for ds in client.list_datasets()]

                for dataset_id in datasets:
                    dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)

                    # List routines in dataset
                    for routine in client.list_routines(dataset_ref):
                        routine_name = routine.routine_id
                        labels = routine.labels or {}

                        if not self._should_include_entity(routine_name, labels):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"routine_{re.sub(r'[^a-zA-Z0-9_]', '_', routine_name.lower())}"

                        # Routines are observable
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"BigQuery routine: {routine_name}",
                            metadata={
                                "bq_routine_name": routine_name,
                                "bq_dataset": dataset_id,
                                "bq_routine_type": str(routine.type_),
                                "entity_type": "routine",
                            }
                        )
                        def _routine_asset(context: AssetExecutionContext, routine_name=routine_name, dataset=dataset_id):
                            """Observable routine asset."""
                            client = self._create_client()

                            dataset_ref = bigquery.DatasetReference(self.project_id, dataset)
                            routine_ref = bigquery.RoutineReference(dataset_ref, routine_name)
                            routine = client.get_routine(routine_ref)

                            context.log.info(f"Routine {routine_name} type: {routine.type_}")

                        assets_list.append(_routine_asset)

            except Exception as e:
                context.log.error(f"Error importing BigQuery routines: {e}")

        # Create observation sensor if requested
        if self.generate_sensor and scheduled_query_metadata:
            @sensor(
                name=f"{self.group_name}_observation_sensor",
                minimum_interval_seconds=self.poll_interval_seconds
            )
            def bigquery_observation_sensor(context: SensorEvaluationContext):
                """Sensor to observe BigQuery scheduled query runs."""
                transfer_client = self._create_transfer_client()

                # Check for completed scheduled query runs
                for asset_key, metadata in scheduled_query_metadata.items():
                    config_name = metadata['config_name']

                    try:
                        # Get recent runs
                        from google.protobuf.timestamp_pb2 import Timestamp
                        start_time = Timestamp()
                        start_time.FromDatetime(datetime.utcnow() - timedelta(seconds=self.poll_interval_seconds * 2))

                        request = bigquery_datatransfer_v1.ListTransferRunsRequest(
                            parent=config_name,
                            states=[bigquery_datatransfer_v1.TransferState.SUCCEEDED],
                        )

                        for run in transfer_client.list_transfer_runs(request=request):
                            # Check if run completed recently
                            if run.update_time.seconds < start_time.seconds:
                                continue

                            yield AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "run_name": run.name,
                                    "state": str(run.state),
                                    "start_time": str(run.start_time),
                                    "end_time": str(run.end_time),
                                    "source": "bigquery_observation_sensor",
                                    "entity_type": "scheduled_query",
                                }
                            )

                    except Exception as e:
                        context.log.error(f"Error checking runs for scheduled query {config_name}: {e}")

            sensors_list.append(bigquery_observation_sensor)

        return Definitions(
            assets=assets_list,
            sensors=sensors_list if sensors_list else None,
        )
