"""Stripe Ingestion Component using dlt.

Ingest Stripe data (customers, subscriptions, charges, invoices, products, and events)
using dlt's verified Stripe source. Returns DataFrames for flexible transformation.
"""

from typing import Optional
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


class StripeIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Stripe payment and customer data using dlt - returns DataFrames.

    This component uses dlt's verified Stripe source to extract payment, subscription,
    and customer data and returns it as a pandas DataFrame for downstream analysis.

    Resources extracted:
    - customers: Customer profiles and information
    - charges: Payment charges and transactions
    - subscriptions: Recurring subscription data
    - invoices: Invoice records
    - products: Products and services offered
    - prices: Pricing information
    - payment_intents: Payment attempt records
    - balance_transactions: Account balance movements
    - events: Stripe webhook events

    The DataFrame can then be:
    - Combined with marketing data for revenue attribution
    - Analyzed for subscription metrics (MRR, churn, LTV)
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.StripeIngestionComponent
        attributes:
          asset_name: stripe_data
          api_key: "${STRIPE_API_KEY}"
          resources: "customers,subscriptions,charges,invoices"
          start_date: "2024-01-01"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the Stripe data"
    )

    api_key: str = Field(
        description="Stripe API Secret Key. Use ${STRIPE_API_KEY} for env vars. Find in Stripe Dashboard > Developers > API Keys."
    )

    resources: str = Field(
        default="customers,subscriptions,charges",
        description="Comma-separated list: customers, subscriptions, charges, invoices, products, prices, payment_intents, balance_transactions, events"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for data extraction (YYYY-MM-DD). Defaults to all historical data."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for data extraction (YYYY-MM-DD). Defaults to today."
    )

    incremental: bool = Field(
        default=False,
        description="Use incremental loading (append mode) instead of full refresh"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="stripe",
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        incremental = self.incremental
        description = self.description or "Stripe payment and customer data via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        destination_config = self.destination_config
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def stripe_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Stripe data using dlt and returns as DataFrame."""

            context.log.info("Starting Stripe data ingestion")

            # Parse resources to load
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

            # Import dlt Stripe source
            try:
                from dlt.sources.stripe_analytics import stripe_source, incremental_stripe_source
                import dlt
            except ImportError as e:
                context.log.error(f"Failed to import dlt Stripe source: {e}")
                context.log.info("Install with: pip install 'dlt[stripe]'")
                raise
            # Determine destination
            use_destination = destination if destination else "duckdb"
            if destination and not destination_config:
                raise ValueError(f"destination_config is required when destination is set to '{destination}'")

            context.log.info(f"Using destination: {use_destination}")

            # Create pipeline (in-memory DuckDB or specified destination)
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=use_destination,
                dataset_name=asset_name if destination else f"{asset_name}_temp"
            )

            context.log.info("Created dlt pipeline for data extraction")

            # Prepare source configuration
            source_config = {
                "stripe_secret_key": api_key,
                "endpoints": tuple(resources_list),
            }

            if start_date:
                source_config["start_date"] = start_date
                context.log.info(f"Start date: {start_date}")

            if end_date:
                source_config["end_date"] = end_date
                context.log.info(f"End date: {end_date}")

            # Create Stripe source (incremental or full)
            try:
                if incremental and start_date:
                    context.log.info("Using incremental loading mode")
                    source = incremental_stripe_source(
                        initial_start_date=start_date,
                        end_date=end_date,
                        endpoints=tuple(resources_list)
                    )
                else:
                    context.log.info("Using full refresh mode")
                    source = stripe_source(**source_config)
            except Exception as e:
                context.log.error(f"Failed to create Stripe source: {e}")
                raise

            # Run pipeline
            context.log.info("Extracting Stripe data...")
            load_info = pipeline.run(source)
            context.log.info(f"Loaded {len(resources_list)} resources")

            # Collect all data
            all_data = []
            resource_metadata = {}

            # Extract data from DuckDB to DataFrame
            for resource_name in resources_list:
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
                "resources_loaded": list(resource_metadata.keys()),
                "total_rows": total_rows,
                "columns": list(combined_df.columns),
                "incremental_mode": incremental,
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return

            if start_date:
                metadata["start_date"] = start_date
            if end_date:
                metadata["end_date"] = end_date

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

        return Definitions(assets=[stripe_ingestion_asset])
