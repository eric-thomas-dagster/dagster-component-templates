"""AWS Glue Component.

Import AWS Glue jobs, crawlers, workflows, DataBrew recipes, and data quality rulesets
as Dagster assets with automatic lineage tracking from Data Catalog tables.

Glue jobs are materializable assets that can be triggered from Dagster, with automatic
dependencies on the Data Catalog tables they read. This creates a lineage graph showing
data flow from tables through jobs.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

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


class AWSGlueComponent(Component, Model, Resolvable):
    """Component for importing AWS Glue entities as Dagster assets with lineage tracking.

    Supports importing:
    - Glue Jobs (Spark/Python ETL jobs) - Materializable with automatic lineage to Data Catalog tables
    - Glue Data Catalog Tables - Observable source assets
    - Glue Crawlers (metadata discovery)
    - Glue Workflows (job orchestration)
    - Glue DataBrew Jobs (visual data preparation)
    - Glue Data Quality Rulesets (data validation)

    Glue jobs are materializable assets that can be triggered from Dagster. They include
    automatic dependencies on Data Catalog tables they read, creating a lineage graph
    that shows data flow from tables through jobs.

    Example:
        ```yaml
        type: dagster_component_templates.AWSGlueComponent
        attributes:
          aws_region: us-east-1
          aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
          aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
          import_jobs: true
          import_catalog_tables: true
          catalog_database_filter: production,analytics
        ```
    """

    aws_region: str = Field(
        description="AWS region (e.g., us-east-1)"
    )

    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID (optional if using IAM role)"
    )

    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key (optional if using IAM role)"
    )

    aws_session_token: Optional[str] = Field(
        default=None,
        description="AWS session token for temporary credentials (optional)"
    )

    import_jobs: bool = Field(
        default=True,
        description="Import Glue jobs as materializable assets"
    )

    import_crawlers: bool = Field(
        default=False,
        description="Import Glue crawlers as materializable assets"
    )

    import_workflows: bool = Field(
        default=False,
        description="Import Glue workflows as materializable assets"
    )

    import_databrew_jobs: bool = Field(
        default=False,
        description="Import Glue DataBrew recipe jobs as materializable assets"
    )

    import_data_quality_rulesets: bool = Field(
        default=False,
        description="Import Glue Data Quality rulesets as observable assets"
    )

    import_catalog_tables: bool = Field(
        default=True,
        description="Import Glue Data Catalog tables as observable source assets for lineage"
    )

    catalog_database_filter: Optional[str] = Field(
        default=None,
        description="Filter Data Catalog tables to specific databases (comma-separated)"
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
        description="Comma-separated list of tag keys to filter entities (entities with ANY of these tags)"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="How often (in seconds) the sensor should check for completed runs"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Create a sensor to observe Glue job runs, crawler runs, and workflow runs"
    )

    group_name: Optional[str] = Field(
        default="aws_glue",
        description="Group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the AWS Glue component"
    )

    def _create_glue_client(self):
        """Create and return AWS Glue client."""
        session_kwargs = {'region_name': self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs['aws_access_key_id'] = self.aws_access_key_id
            session_kwargs['aws_secret_access_key'] = self.aws_secret_access_key

        if self.aws_session_token:
            session_kwargs['aws_session_token'] = self.aws_session_token

        session = boto3.Session(**session_kwargs)
        return session.client('glue')

    def _create_databrew_client(self):
        """Create and return AWS Glue DataBrew client."""
        session_kwargs = {'region_name': self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs['aws_access_key_id'] = self.aws_access_key_id
            session_kwargs['aws_secret_access_key'] = self.aws_secret_access_key

        if self.aws_session_token:
            session_kwargs['aws_session_token'] = self.aws_session_token

        session = boto3.Session(**session_kwargs)
        return session.client('databrew')

    def _should_include_entity(self, name: str, tags: Dict[str, str] = None) -> bool:
        """Check if an entity should be included based on filters."""
        # Check name exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name, re.IGNORECASE):
                return False

        # Check name inclusion pattern
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name, re.IGNORECASE):
                return False

        # Check tags filter
        if self.filter_by_tags and tags:
            filter_tag_keys = [t.strip() for t in self.filter_by_tags.split(',')]
            entity_tag_keys = list(tags.keys()) if isinstance(tags, dict) else []
            if not any(tag_key in entity_tag_keys for tag_key in filter_tag_keys):
                return False

        return True

    def _extract_table_references_from_job(self, job: Dict[str, Any]) -> List[str]:
        """Extract Data Catalog table references from Glue job metadata.

        Returns list of table asset keys in format: glue_table_{database}_{table}
        """
        table_refs = []

        # Check DefaultArguments for common table reference patterns
        default_args = job.get('DefaultArguments', {})

        # Common argument patterns that contain table references
        db_arg = default_args.get('--database_name') or default_args.get('--database')
        table_args = []

        # Look for table name arguments
        for key, value in default_args.items():
            if 'table' in key.lower() and '--' in key:
                table_args.append(value)

        # If we found a database, construct table refs
        if db_arg:
            for table_name in table_args:
                asset_key = f"glue_table_{re.sub(r'[^a-zA-Z0-9_]', '_', db_arg.lower())}_{re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())}"
                table_refs.append(asset_key)

        # Also check Connections which may reference catalog connections
        connections = job.get('Connections', {}).get('Connections', [])
        # Note: Connections are typically network connections, not table references,
        # but we keep this here for potential future enhancement

        return table_refs

    def _list_catalog_tables(self, glue_client) -> List[Dict[str, Any]]:
        """List all Data Catalog tables across databases."""
        tables = []

        try:
            # Get list of databases
            databases_to_scan = []

            if self.catalog_database_filter:
                databases_to_scan = [db.strip() for db in self.catalog_database_filter.split(',')]
            else:
                # Get all databases
                db_paginator = glue_client.get_paginator('get_databases')
                for page in db_paginator.paginate():
                    for db in page.get('DatabaseList', []):
                        databases_to_scan.append(db['Name'])

            # Get tables from each database
            for database_name in databases_to_scan:
                try:
                    table_paginator = glue_client.get_paginator('get_tables')
                    for page in table_paginator.paginate(DatabaseName=database_name):
                        for table in page.get('TableList', []):
                            table_name = table['Name']

                            # Apply entity filters
                            if not self._should_include_entity(f"{database_name}.{table_name}"):
                                continue

                            tables.append({
                                'database': database_name,
                                'table': table_name,
                                'location': table.get('StorageDescriptor', {}).get('Location'),
                                'update_time': table.get('UpdateTime'),
                            })
                except Exception as e:
                    # Log but continue with other databases
                    pass

        except Exception as e:
            pass

        return tables

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from AWS Glue entities."""
        glue_client = self._create_glue_client()

        assets_list = []
        sensors_list = []

        # Track job, crawler, and workflow metadata for sensor
        job_metadata = {}
        crawler_metadata = {}
        workflow_metadata = {}

        # Track Data Catalog tables for lineage
        catalog_table_keys = set()

        # Import Data Catalog Tables (for lineage)
        if self.import_catalog_tables:
            try:
                tables = self._list_catalog_tables(glue_client)

                for table_info in tables:
                    database = table_info['database']
                    table_name = table_info['table']

                    # Create asset key
                    asset_key = f"glue_table_{re.sub(r'[^a-zA-Z0-9_]', '_', database.lower())}_{re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())}"
                    catalog_table_keys.add(asset_key)

                    # Data Catalog tables are observable source assets
                    @observable_source_asset(
                        name=asset_key,
                        group_name=self.group_name,
                        description=f"Glue Data Catalog table: {database}.{table_name}",
                        metadata={
                            "glue_database": database,
                            "glue_table": table_name,
                            "glue_location": table_info.get('location'),
                            "entity_type": "catalog_table",
                        }
                    )
                    def _catalog_table_asset(context: AssetExecutionContext, database=database, table_name=table_name):
                        """Observable Data Catalog table."""
                        client = self._create_glue_client()

                        try:
                            # Get table metadata
                            table_response = client.get_table(DatabaseName=database, Name=table_name)
                            table_data = table_response['Table']

                            metadata = {
                                "database": database,
                                "table": table_name,
                                "location": table_data.get('StorageDescriptor', {}).get('Location'),
                                "update_time": str(table_data.get('UpdateTime')) if table_data.get('UpdateTime') else None,
                                "row_count": table_data.get('Parameters', {}).get('numRows', 'N/A'),
                            }

                            context.log.info(f"Observed Glue table {database}.{table_name}")
                            return metadata

                        except Exception as e:
                            context.log.warning(f"Could not get table info for {database}.{table_name}: {e}")
                            return {"database": database, "table": table_name}

                    assets_list.append(_catalog_table_asset)

            except Exception as e:
                context.log.error(f"Error importing Glue Data Catalog tables: {e}")

        # Import Glue Jobs
        if self.import_jobs:
            try:
                paginator = glue_client.get_paginator('get_jobs')

                for page in paginator.paginate():
                    jobs = page.get('Jobs', [])

                    for job in jobs:
                        job_name = job['Name']
                        tags = job.get('Tags', {})

                        if not self._should_include_entity(job_name, tags):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"glue_job_{re.sub(r'[^a-zA-Z0-9_]', '_', job_name.lower())}"

                        # Extract table dependencies for lineage
                        table_deps = self._extract_table_references_from_job(job)
                        # Filter to only tables that exist in catalog
                        table_deps = [dep for dep in table_deps if dep in catalog_table_keys]

                        # Store metadata for sensor
                        job_metadata[asset_key] = {
                            'job_name': job_name,
                            'command': job.get('Command', {}).get('Name'),
                        }

                        # Glue jobs are materializable and can have dependencies on Data Catalog tables
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"AWS Glue job: {job_name}",
                            metadata={
                                "glue_job_name": job_name,
                                "glue_command": job.get('Command', {}).get('Name'),
                                "glue_role": job.get('Role'),
                                "glue_worker_type": job.get('WorkerType'),
                                "entity_type": "glue_job",
                                "upstream_tables": len(table_deps),
                            },
                            deps=table_deps if table_deps else None,
                        )
                        def _glue_job_asset(context: AssetExecutionContext, job_name=job_name, table_deps=table_deps):
                            """Materialize by running Glue job."""
                            client = self._create_glue_client()

                            # Start job run
                            response = client.start_job_run(JobName=job_name)
                            run_id = response['JobRunId']

                            context.log.info(f"Started Glue job {job_name}, run ID: {run_id}")
                            if table_deps:
                                context.log.info(f"Upstream tables: {', '.join(table_deps)}")

                            # Wait for completion
                            waiter = client.get_waiter('job_run_complete')
                            waiter.wait(
                                JobName=job_name,
                                RunId=run_id,
                                WaiterConfig={'Delay': 30, 'MaxAttempts': 120}
                            )

                            # Get run details
                            run_info = client.get_job_run(JobName=job_name, RunId=run_id)
                            job_run = run_info['JobRun']

                            metadata = {
                                "run_id": run_id,
                                "job_run_state": job_run.get('JobRunState'),
                                "execution_time": job_run.get('ExecutionTime'),
                                "started_on": str(job_run.get('StartedOn')) if job_run.get('StartedOn') else None,
                                "completed_on": str(job_run.get('CompletedOn')) if job_run.get('CompletedOn') else None,
                                "upstream_tables": ', '.join(table_deps) if table_deps else 'None detected',
                            }

                            context.log.info(f"Glue job {job_name} completed with state: {job_run.get('JobRunState')}")

                            return metadata

                        assets_list.append(_glue_job_asset)

            except Exception as e:
                context.log.error(f"Error importing AWS Glue jobs: {e}")

        # Import Glue Crawlers
        if self.import_crawlers:
            try:
                paginator = glue_client.get_paginator('get_crawlers')

                for page in paginator.paginate():
                    crawlers = page.get('Crawlers', [])

                    for crawler in crawlers:
                        crawler_name = crawler['Name']
                        tags = crawler.get('Tags', {})

                        if not self._should_include_entity(crawler_name, tags):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"glue_crawler_{re.sub(r'[^a-zA-Z0-9_]', '_', crawler_name.lower())}"

                        # Store metadata for sensor
                        crawler_metadata[asset_key] = {
                            'crawler_name': crawler_name,
                        }

                        # Crawlers are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"AWS Glue crawler: {crawler_name}",
                            metadata={
                                "glue_crawler_name": crawler_name,
                                "glue_role": crawler.get('Role'),
                                "glue_database": crawler.get('DatabaseName'),
                                "entity_type": "glue_crawler",
                            }
                        )
                        def _glue_crawler_asset(context: AssetExecutionContext, crawler_name=crawler_name):
                            """Materialize by running Glue crawler."""
                            client = self._create_glue_client()

                            # Start crawler
                            client.start_crawler(Name=crawler_name)
                            context.log.info(f"Started Glue crawler: {crawler_name}")

                            # Get crawler state
                            crawler_info = client.get_crawler(Name=crawler_name)
                            crawler_data = crawler_info['Crawler']

                            metadata = {
                                "crawler_name": crawler_name,
                                "state": crawler_data.get('State'),
                                "last_crawl": str(crawler_data.get('LastCrawl', {}).get('StartTime')) if crawler_data.get('LastCrawl') else None,
                            }

                            context.log.info(f"Glue crawler {crawler_name} started, state: {crawler_data.get('State')}")

                            return metadata

                        assets_list.append(_glue_crawler_asset)

            except Exception as e:
                context.log.error(f"Error importing AWS Glue crawlers: {e}")

        # Import Glue Workflows
        if self.import_workflows:
            try:
                paginator = glue_client.get_paginator('list_workflows')

                for page in paginator.paginate():
                    workflow_names = page.get('Workflows', [])

                    if workflow_names:
                        # Get workflow details
                        workflows_response = glue_client.batch_get_workflows(Names=workflow_names)
                        workflows = workflows_response.get('Workflows', [])

                        for workflow in workflows:
                            workflow_name = workflow['Name']
                            tags = {}  # Workflows don't have direct tags in response

                            if not self._should_include_entity(workflow_name, tags):
                                continue

                            # Sanitize name for asset key
                            asset_key = f"glue_workflow_{re.sub(r'[^a-zA-Z0-9_]', '_', workflow_name.lower())}"

                            # Store metadata for sensor
                            workflow_metadata[asset_key] = {
                                'workflow_name': workflow_name,
                            }

                            # Workflows are materializable
                            @asset(
                                name=asset_key,
                                group_name=self.group_name,
                                description=f"AWS Glue workflow: {workflow_name}",
                                metadata={
                                    "glue_workflow_name": workflow_name,
                                    "glue_created_on": str(workflow.get('CreatedOn')) if workflow.get('CreatedOn') else None,
                                    "entity_type": "glue_workflow",
                                }
                            )
                            def _glue_workflow_asset(context: AssetExecutionContext, workflow_name=workflow_name):
                                """Materialize by running Glue workflow."""
                                client = self._create_glue_client()

                                # Start workflow run
                                response = client.start_workflow_run(Name=workflow_name)
                                run_id = response['RunId']

                                context.log.info(f"Started Glue workflow {workflow_name}, run ID: {run_id}")

                                # Get workflow run status
                                run_info = client.get_workflow_run(Name=workflow_name, RunId=run_id)
                                workflow_run = run_info['Run']

                                metadata = {
                                    "run_id": run_id,
                                    "status": workflow_run.get('Status'),
                                    "started_on": str(workflow_run.get('StartedOn')) if workflow_run.get('StartedOn') else None,
                                }

                                context.log.info(f"Glue workflow {workflow_name} started, run ID: {run_id}")

                                return metadata

                            assets_list.append(_glue_workflow_asset)

            except Exception as e:
                context.log.error(f"Error importing AWS Glue workflows: {e}")

        # Import Glue DataBrew Jobs
        if self.import_databrew_jobs:
            try:
                databrew_client = self._create_databrew_client()
                paginator = databrew_client.get_paginator('list_jobs')

                for page in paginator.paginate():
                    jobs = page.get('Jobs', [])

                    for job in jobs:
                        job_name = job['Name']
                        tags = job.get('Tags', {})

                        if not self._should_include_entity(job_name, tags):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"databrew_job_{re.sub(r'[^a-zA-Z0-9_]', '_', job_name.lower())}"

                        # DataBrew jobs are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"AWS Glue DataBrew job: {job_name}",
                            metadata={
                                "databrew_job_name": job_name,
                                "databrew_type": job.get('Type'),
                                "databrew_recipe": job.get('RecipeName'),
                                "entity_type": "databrew_job",
                            }
                        )
                        def _databrew_job_asset(context: AssetExecutionContext, job_name=job_name):
                            """Materialize by running DataBrew job."""
                            client = self._create_databrew_client()

                            # Start job run
                            response = client.start_job_run(Name=job_name)
                            run_id = response['RunId']

                            context.log.info(f"Started DataBrew job {job_name}, run ID: {run_id}")

                            metadata = {
                                "run_id": run_id,
                                "job_name": job_name,
                            }

                            return metadata

                        assets_list.append(_databrew_job_asset)

            except Exception as e:
                context.log.error(f"Error importing AWS Glue DataBrew jobs: {e}")

        # Import Glue Data Quality Rulesets
        if self.import_data_quality_rulesets:
            try:
                paginator = glue_client.get_paginator('list_data_quality_rulesets')

                for page in paginator.paginate():
                    rulesets = page.get('Rulesets', [])

                    for ruleset in rulesets:
                        ruleset_name = ruleset.get('Name')

                        if not ruleset_name or not self._should_include_entity(ruleset_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"data_quality_{re.sub(r'[^a-zA-Z0-9_]', '_', ruleset_name.lower())}"

                        # Data quality rulesets are observable
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"AWS Glue Data Quality ruleset: {ruleset_name}",
                            metadata={
                                "ruleset_name": ruleset_name,
                                "target_table": ruleset.get('TargetTable'),
                                "entity_type": "data_quality_ruleset",
                            }
                        )
                        def _data_quality_asset(context: AssetExecutionContext, ruleset_name=ruleset_name):
                            """Observable data quality ruleset."""
                            client = self._create_glue_client()

                            # Get ruleset details
                            try:
                                ruleset_info = client.get_data_quality_ruleset(Name=ruleset_name)
                                context.log.info(f"Data quality ruleset {ruleset_name} found")
                            except ClientError as e:
                                context.log.warning(f"Could not get data quality ruleset {ruleset_name}: {e}")

                        assets_list.append(_data_quality_asset)

            except Exception as e:
                context.log.error(f"Error importing AWS Glue Data Quality rulesets: {e}")

        # Create observation sensor if requested
        if self.generate_sensor and (job_metadata or crawler_metadata or workflow_metadata):
            @sensor(
                name=f"{self.group_name}_observation_sensor",
                minimum_interval_seconds=self.poll_interval_seconds
            )
            def glue_observation_sensor(context: SensorEvaluationContext):
                """Sensor to observe Glue job runs, crawler runs, and workflow runs."""
                client = self._create_glue_client()

                # Check for completed job runs
                for asset_key, metadata in job_metadata.items():
                    job_name = metadata['job_name']

                    try:
                        # Get recent job runs
                        response = client.get_job_runs(JobName=job_name, MaxResults=5)
                        job_runs = response.get('JobRuns', [])

                        for run in job_runs:
                            # Only emit for successful completions
                            if run.get('JobRunState') == 'SUCCEEDED':
                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "run_id": run.get('Id'),
                                        "job_run_state": run.get('JobRunState'),
                                        "started_on": str(run.get('StartedOn')) if run.get('StartedOn') else None,
                                        "completed_on": str(run.get('CompletedOn')) if run.get('CompletedOn') else None,
                                        "execution_time": run.get('ExecutionTime'),
                                        "source": "glue_observation_sensor",
                                        "entity_type": "glue_job",
                                    }
                                )
                    except Exception as e:
                        context.log.error(f"Error checking runs for Glue job {job_name}: {e}")

                # Check for completed crawler runs
                for asset_key, metadata in crawler_metadata.items():
                    crawler_name = metadata['crawler_name']

                    try:
                        # Get crawler metrics
                        response = client.get_crawler_metrics(CrawlerNameList=[crawler_name])
                        metrics = response.get('CrawlerMetricsList', [])

                        if metrics:
                            metric = metrics[0]
                            if metric.get('LastRuntimeSeconds', 0) > 0:
                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "crawler_name": crawler_name,
                                        "tables_created": metric.get('TablesCreated', 0),
                                        "tables_updated": metric.get('TablesUpdated', 0),
                                        "tables_deleted": metric.get('TablesDeleted', 0),
                                        "last_runtime_seconds": metric.get('LastRuntimeSeconds'),
                                        "source": "glue_observation_sensor",
                                        "entity_type": "glue_crawler",
                                    }
                                )
                    except Exception as e:
                        context.log.error(f"Error checking metrics for Glue crawler {crawler_name}: {e}")

                # Check for completed workflow runs
                for asset_key, metadata in workflow_metadata.items():
                    workflow_name = metadata['workflow_name']

                    try:
                        # Get recent workflow runs
                        response = client.get_workflow_runs(Name=workflow_name, MaxResults=5)
                        runs = response.get('Runs', [])

                        for run in runs:
                            # Only emit for successful completions
                            if run.get('Status') == 'COMPLETED':
                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "run_id": run.get('WorkflowRunId'),
                                        "status": run.get('Status'),
                                        "started_on": str(run.get('StartedOn')) if run.get('StartedOn') else None,
                                        "completed_on": str(run.get('CompletedOn')) if run.get('CompletedOn') else None,
                                        "source": "glue_observation_sensor",
                                        "entity_type": "glue_workflow",
                                    }
                                )
                    except Exception as e:
                        context.log.error(f"Error checking runs for Glue workflow {workflow_name}: {e}")

            sensors_list.append(glue_observation_sensor)

        return Definitions(
            assets=assets_list,
            sensors=sensors_list if sensors_list else None,
        )
