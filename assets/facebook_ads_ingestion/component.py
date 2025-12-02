"""Facebook Ads Ingestion Component using dlt.

Ingest Facebook Ads data (campaigns, ad sets, ads, creatives, leads, and insights)
using dlt's verified Facebook Ads source. Returns DataFrames for flexible transformation.
Mimics capabilities of Supermetrics and Funnel.io.
"""

from typing import Optional, Literal, List
import pandas as pd
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class FacebookAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Facebook Ads data using dlt - returns DataFrames.

    This component uses dlt's verified Facebook Ads source to extract data and
    returns it as a pandas DataFrame for downstream transformation and analysis.

    Resources extracted:
    - campaigns: Campaign configurations and budgets
    - ad_sets: Ad set configurations within campaigns
    - ads: Individual ad units
    - creatives: Visual and textual ad content
    - ad_leads: Lead generation form submissions
    - insights: Performance metrics (impressions, clicks, spend, etc.)

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.FacebookAdsIngestionComponent
        attributes:
          asset_name: facebook_ads_data
          account_id: "act_123456789"
          access_token: "${FACEBOOK_ACCESS_TOKEN}"
          resources: "campaigns,ads,insights"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the Facebook Ads data"
    )

    account_id: str = Field(
        description="Facebook Ads Account ID (format: act_123456789). Find in Ads Manager URL."
    )

    access_token: str = Field(
        description="Facebook Access Token with ads_read and lead_retrieval permissions. Use ${FACEBOOK_ACCESS_TOKEN} for env vars."
    )

    app_id: Optional[str] = Field(
        default=None,
        description="Facebook App ID (optional, for long-lived tokens)"
    )

    app_secret: Optional[str] = Field(
        default=None,
        description="Facebook App Secret (optional, for long-lived tokens). Use ${FACEBOOK_APP_SECRET} for env vars."
    )

    resources: str = Field(
        default="insights",
        description="Comma-separated list of resources to extract: campaigns, ad_sets, ads, creatives, ad_leads, insights"
    )

    initial_load_past_days: int = Field(
        default=30,
        description="Number of days of historical data to load"
    )

    ad_states: str = Field(
        default="ACTIVE,PAUSED",
        description="Comma-separated ad states to extract: ACTIVE, PAUSED, DELETED, ARCHIVED, DISAPPROVED, etc."
    )

    insights_fields: Optional[str] = Field(
        default=None,
        description="Comma-separated list of insights fields to extract (leave empty for defaults)"
    )

    insights_breakdown: Optional[str] = Field(
        default=None,
        description="Insights breakdown dimension: age, gender, country, region, platform, device_platform"
    )

    time_increment_days: int = Field(
        default=1,
        description="Time increment for insights reports in days (1 = daily, 7 = weekly)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="facebook_ads",
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
        account_id = self.account_id
        access_token = self.access_token
        app_id = self.app_id
        app_secret = self.app_secret
        resources_str = self.resources
        initial_load_past_days = self.initial_load_past_days
        ad_states_str = self.ad_states
        insights_fields_str = self.insights_fields
        insights_breakdown = self.insights_breakdown
        time_increment_days = self.time_increment_days
        description = self.description or "Facebook Ads data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def facebook_ads_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Facebook Ads data using dlt and returns as DataFrame."""

            context.log.info(f"Starting Facebook Ads ingestion for account: {account_id}")

            # Parse resources to load
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

            # Parse ad states
            ad_states_list = [s.strip() for s in ad_states_str.split(',')]

            # Parse insights fields if provided
            insights_fields_list = None
            if insights_fields_str:
                insights_fields_list = [f.strip() for f in insights_fields_str.split(',')]

            # Import dlt Facebook Ads source
            try:
                from dlt.sources.facebook_ads import facebook_ads_source, facebook_insights_source
                import dlt
            except ImportError as e:
                context.log.error(f"Failed to import dlt Facebook Ads source: {e}")
                context.log.info("Install with: pip install 'dlt[facebook_ads]'")
                raise
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

            context.log.info("Created dlt pipeline for data extraction")

            # Collect all data
            all_data = []
            resource_metadata = {}

            # Load standard resources (campaigns, ad_sets, ads, creatives, ad_leads)
            standard_resources = [r for r in resources_list if r != "insights"]

            if standard_resources:
                context.log.info(f"Extracting standard resources: {standard_resources}")

                # Create Facebook Ads source
                source = facebook_ads_source(
                    account_id=account_id,
                    access_token=access_token,
                    app_id=app_id,
                    app_secret=app_secret,
                    initial_load_past_days=initial_load_past_days,
                )

                # Configure ad states if ads resource is included
                if "ads" in standard_resources:
                    source.ads.bind(states=tuple(ad_states_list))
                    context.log.info(f"Ad states filter: {ad_states_list}")

                # Select only requested resources
                load_data = source.with_resources(*standard_resources)

                # Run pipeline
                load_info = pipeline.run(load_data)
                context.log.info(f"Loaded {len(standard_resources)} standard resources")

                # Extract data from DuckDB to DataFrame
                for resource_name in standard_resources:
                    try:
                        # Query the loaded data
                        query = f"SELECT * FROM {dataset_name}.{resource_name}"
                        with pipeline.sql_client() as client:
                            df = client.execute_df(query)

                        if len(df) > 0:
                            df['_resource_type'] = resource_name
                            all_data.append(df)
                            resource_metadata[resource_name] = len(df)
                            context.log.info(f"  {resource_name}: {len(df)} rows")
                    except Exception as e:
                        context.log.warning(f"Could not load {resource_name}: {e}")

            # Load insights separately if requested
            if "insights" in resources_list:
                context.log.info("Extracting Facebook Insights...")

                # Create insights source with custom config
                insights_config = {
                    "account_id": account_id,
                    "access_token": access_token,
                    "initial_load_past_days": initial_load_past_days,
                    "time_increment_days": time_increment_days,
                }

                if insights_fields_list:
                    insights_config["fields"] = insights_fields_list

                if insights_breakdown:
                    insights_config["breakdowns"] = [insights_breakdown]
                    context.log.info(f"Insights breakdown: {insights_breakdown}")

                insights_source = facebook_insights_source(**insights_config)

                # Run insights pipeline
                load_info = pipeline.run(insights_source)
                context.log.info("Loaded Facebook Insights")

                # Extract insights data
                try:
                    query = f"SELECT * FROM {dataset_name}.insights"
                    with pipeline.sql_client() as client:
                        df = client.execute_df(query)

                    if len(df) > 0:
                        df['_resource_type'] = 'insights'
                        all_data.append(df)
                        resource_metadata['insights'] = len(df)
                        context.log.info(f"  insights: {len(df)} rows")
                except Exception as e:
                    context.log.warning(f"Could not load insights: {e}")

            # Combine all data into single DataFrame
            if not all_data:
                context.log.warning("No data extracted")
                return pd.DataFrame()

            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Extraction complete: {len(combined_df)} total rows, "
                f"{len(combined_df.columns)} columns"
            )

            # Add output metadata
            total_rows = len(combined_df)
            metadata = {
                "account_id": account_id,
                "resources_loaded": list(resource_metadata.keys()),
                "total_rows": total_rows,
                "columns": list(combined_df.columns),
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return

            # Add per-resource row counts
            for resource, rows in resource_metadata.items():
                metadata[f"rows_{resource}"] = rows

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(combined_df) > 0:
                return Output(
                    value=combined_df,
                    metadata={
                        "row_count": len(combined_df),
                        "column_count": len(combined_df.columns),
                        "resources": list(resource_metadata.keys()),
                        "sample": MetadataValue.md(combined_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(combined_df.head(10))
                    }
                )
            else:
                return combined_df

        return Definitions(assets=[facebook_ads_ingestion_asset])
