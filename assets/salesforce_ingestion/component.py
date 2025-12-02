"""Salesforce Ingestion Component.

Ingest Salesforce CRM data using dlt (data load tool).
Extracts accounts, contacts, opportunities, leads, and more.
"""

from typing import Optional, List
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field
import dlt


class SalesforceIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Salesforce CRM data using dlt.

    Salesforce is the leading enterprise CRM platform. This component extracts
    data from Salesforce's API and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    Available data objects (20+):

    Replace Mode (full refresh):
    - User, UserRole: User accounts and permissions
    - Lead: Potential customers
    - Contact: Individual contact records
    - Campaign: Marketing campaigns
    - Product2, Pricebook2, PricebookEntry: Product catalog

    Merge Mode (incremental loading):
    - Account: Company/organization records
    - Opportunity, OpportunityLineItem, OpportunityContactRole: Sales pipeline
    - CampaignMember: Campaign participation
    - Task, Event: Activities and calendar events

    The component uses dlt's verified Salesforce source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.SalesforceIngestionComponent
        attributes:
          asset_name: salesforce_crm_data
          username: "{{ env('SALESFORCE_USERNAME') }}"
          password: "{{ env('SALESFORCE_PASSWORD') }}"
          security_token: "{{ env('SALESFORCE_SECURITY_TOKEN') }}"
          sf_objects:
            - Account
            - Opportunity
            - Contact
            - Lead
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    username: str = Field(
        description="Salesforce username"
    )

    password: str = Field(
        description="Salesforce password"
    )

    security_token: str = Field(
        description="Salesforce security token (from Settings > Personal Setup > Reset My Security Token)"
    )

    sf_objects: List[str] = Field(
        default=["Account", "Opportunity", "Contact", "Lead"],
        description="Salesforce objects to extract (Account, Opportunity, Contact, Lead, Campaign, Task, Event, etc.)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="salesforce",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    
    destination: Optional[str] = Field(
        default=None,
        description="Optional dlt destination (e.g., 'snowflake', 'bigquery', 'postgres', 'redshift'). If not set, uses in-memory DuckDB and returns DataFrame."
    )

    destination_config: Optional[str] = Field(
        default=None,
        description="Optional destination configuration as connection string or JSON. Required if destination is set."
    )

    persist_and_return: bool = Field(
        default=False,
        description="If True with destination set: persist to database AND return DataFrame. If False: only persist to database."
    )

    def _get_effective_destination(self) -> Optional[str]:
        """Get destination based on environment routing if enabled."""
        import os

        if not self.use_environment_routing:
            return self.destination

        # Check Dagster Cloud environment variables
        is_branch = os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "").lower() == "true"
        deployment_name = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "")

        # Determine which destination to use
        if is_branch and self.destination_branch:
            return self.destination_branch
        elif deployment_name and not is_branch and self.destination_prod:
            # In Dagster Cloud but not a branch deployment = production
            return self.destination_prod
        elif not deployment_name and self.destination_local:
            # Not in Dagster Cloud = local development
            return self.destination_local
        else:
            # Fallback to main destination field
            return self.destination

(self) -> dict:
        """Build dlt destination config from structured fields."""
        if not self.destination:
            return {}

        if self.destination == "snowflake":
            return {
                "credentials": {
                    "database": self.snowflake_database,
                    "username": self.snowflake_username,
                    "password": self.snowflake_password,
                    "host": self.snowflake_account,
                    "warehouse": self.snowflake_warehouse,
                    "role": self.snowflake_role if self.snowflake_role else None,
                }
            }
        elif self.destination == "bigquery":
            config = {
                "project_id": self.bigquery_project_id,
                "dataset": self.bigquery_dataset,
            }
            if self.bigquery_credentials_path:
                config["credentials"] = self.bigquery_credentials_path
            if self.bigquery_location:
                config["location"] = self.bigquery_location
            return config
        elif self.destination == "postgres":
            return {
                "credentials": {
                    "database": self.postgres_database,
                    "username": self.postgres_username,
                    "password": self.postgres_password,
                    "host": self.postgres_host,
                    "port": self.postgres_port,
                }
            }
        elif self.destination == "redshift":
            return {
                "credentials": {
                    "database": self.redshift_database,
                    "username": self.redshift_username,
                    "password": self.redshift_password,
                    "host": self.redshift_host,
                    "port": self.redshift_port,
                }
            }
        elif self.destination == "duckdb":
            return {
                "credentials": self.duckdb_database_path if self.duckdb_database_path else ":memory:"
            }
        elif self.destination == "motherduck":
            return {
                "credentials": {
                    "database": self.motherduck_database,
                    "token": self.motherduck_token,
                }
            }
        elif self.destination == "databricks":
            config = {
                "credentials": {
                    "server_hostname": self.databricks_server_hostname,
                    "http_path": self.databricks_http_path,
                    "access_token": self.databricks_access_token,
                }
            }
            if self.databricks_catalog:
                config["credentials"]["catalog"] = self.databricks_catalog
            if self.databricks_schema:
                config["credentials"]["schema"] = self.databricks_schema
            return config
        elif self.destination == "clickhouse":
            return {
                "credentials": {
                    "database": self.clickhouse_database,
                    "username": self.clickhouse_username,
                    "password": self.clickhouse_password,
                    "host": self.clickhouse_host,
                    "port": self.clickhouse_port,
                }
            }
        elif self.destination == "mssql":
            return {
                "credentials": {
                    "database": self.mssql_database,
                    "username": self.mssql_username,
                    "password": self.mssql_password,
                    "host": self.mssql_host,
                    "port": self.mssql_port,
                }
            }
        elif self.destination == "athena":
            return {
                "credentials": {
                    "query_result_bucket": self.athena_query_result_bucket,
                    "database": self.athena_database,
                    "aws_access_key_id": self.athena_aws_access_key_id,
                    "aws_secret_access_key": self.athena_aws_secret_access_key,
                    "region_name": self.athena_region,
                }
            }
        elif self.destination == "mysql":
            return {
                "credentials": {
                    "database": self.mysql_database,
                    "username": self.mysql_username,
                    "password": self.mysql_password,
                    "host": self.mysql_host,
                    "port": self.mysql_port,
                }
            }
        elif self.destination == "filesystem":
            config = {
                "bucket_url": self.filesystem_bucket_path if self.filesystem_bucket_path else "/tmp/dlt_data",
            }
            if self.filesystem_format:
                config["format"] = self.filesystem_format
            return config
        elif self.destination == "synapse":
            return {
                "credentials": {
                    "database": self.synapse_database,
                    "username": self.synapse_username,
                    "password": self.synapse_password,
                    "host": self.synapse_host,
                }
            }


def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        username = self.username
        password = self.password
        security_token = self.security_token
        sf_objects = self.sf_objects
        description = self.description or f"Salesforce data ({', '.join(sf_objects)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def salesforce_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Salesforce data using dlt."""
            from dlt.sources.salesforce import salesforce_source

            context.log.info(f"Starting Salesforce ingestion for objects: {sf_objects}")
            # Determine destination (with environment routing if enabled)

            effective_destination = self._get_effective_destination() if hasattr(self, '_get_effective_destination') else destination

            use_destination = effective_destination if effective_destination else "duckdb"

            destination_config = self._build_destination_config() if effective_destination else {}

            context.log.info(f"Using destination: {use_destination}")


            # Create pipeline (in-memory DuckDB or specified destination)

            pipeline_kwargs = {

                "pipeline_name": f"{asset_name}_pipeline",

                "destination": use_destination,

                "dataset_name": asset_name if destination else f"{asset_name}_temp"

            }


            # Add credentials if destination is configured

            if destination_config:

                if "credentials" in destination_config:

                    pipeline_kwargs["credentials"] = destination_config["credentials"]

                # For BigQuery, project_id goes at root level

                if use_destination == "bigquery" and "project_id" in destination_config:

                    pipeline_kwargs["project_id"] = destination_config["project_id"]

                    if "location" in destination_config:

                        pipeline_kwargs["location"] = destination_config["location"]


            pipeline = dlt.pipeline(**pipeline_kwargs)

            # Create Salesforce source with credentials
            source = salesforce_source(
                user_name=username,
                password=password,
                security_token=security_token,
            )

            # Filter to requested objects
            if sf_objects:
                # Select specific resources
                selected_resources = []
                for object_name in sf_objects:
                    # Salesforce objects are typically in replace or merge mode
                    # Try to find the resource by name
                    if hasattr(source, object_name):
                        selected_resources.append(getattr(source, object_name))
                    elif hasattr(source, object_name.lower()):
                        selected_resources.append(getattr(source, object_name.lower()))
                    else:
                        context.log.warning(f"Object {object_name} not found in Salesforce source")

                if not selected_resources:
                    raise ValueError(f"No valid objects found. Check Salesforce object names.")

                # Run pipeline with selected resources
                load_info = pipeline.run(selected_resources)
            else:
                # Run with all resources
                load_info = pipeline.run(source)

            context.log.info(f"Salesforce data loaded: {load_info}")

            # Handle based on destination mode
            if effective_destination and not persist_and_return:
                # Persist only mode: data is in destination, return metadata only
                context.log.info(f"Data persisted to {effective_destination}. Not returning DataFrame (persist_and_return=False)")

                # Get row counts from load_info if available
                try:
                    total_rows = sum(
                        package.get('row_counts', {}).get(resource_name, 0)
                        for package in load_info.load_packages
                        for resource_name in resources_list if 'resources_list' in locals()
                    )
                except:
                    total_rows = 0

                metadata = {
                    "destination": effective_destination,
                    "dataset_name": asset_name,
                    "row_count": total_rows,
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return
                context.add_output_metadata(metadata)

                # Return empty DataFrame with metadata
                return pd.DataFrame({"status": ["persisted"], "destination": [effective_destination], "row_count": [total_rows]})

            # DataFrame return mode: extract data from destination
            dataset_name = asset_name if effective_destination else f"{asset_name}_temp"


            # Extract data from DuckDB to DataFrame
            all_data = []
            for object_name in sf_objects:
                try:
                    # Try both capitalized and lowercase table names
                    for table_name in [object_name, object_name.lower()]:
                        try:
                            query = f"SELECT * FROM {dataset_name}.{table_name}"
                            with pipeline.sql_client() as client:
                                with client.execute_query(query) as cursor:
                                    columns = [desc[0] for desc in cursor.description]
                                    rows = cursor.fetchall()
                                    if rows:
                                        df = pd.DataFrame(rows, columns=columns)
                                        df['_resource_type'] = object_name
                                        all_data.append(df)
                                        context.log.info(f"Extracted {len(df)} rows from {object_name}")
                                        break
                        except:
                            continue
                except Exception as e:
                    context.log.warning(f"Could not extract {object_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Salesforce")
                return pd.DataFrame()

            # Combine all objects into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Salesforce ingestion complete: {len(combined_df)} total rows from {len(all_data)} objects"
            )

            # Add metadata
            metadata = {
                "row_count": len(combined_df),
                "objects_extracted": len(all_data),
                "object_types": list(combined_df['_resource_type'].unique()) if '_resource_type' in combined_df.columns else [],
            }

            # Return with metadata
            if include_sample and len(combined_df) > 0:
                return Output(
                    value=combined_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(combined_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(combined_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return combined_df

        return Definitions(assets=[salesforce_ingestion_asset])
