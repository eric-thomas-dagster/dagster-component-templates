"""Support Ticket Ingestion Component.

Ingest support ticket data from various help desk platforms using dlt.
Supports Zendesk and can be extended to other platforms.
"""

from typing import Optional
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


class SupportTicketIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting support ticket data from help desk platforms.

    Currently supports Zendesk platform via dlt.

    Example:
        ```yaml
        type: dagster_component_templates.SupportTicketIngestionComponent
        attributes:
          asset_name: support_tickets
          platform: zendesk
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    platform: str = Field(
        description="Support platform (zendesk, freshdesk, etc.)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        description = self.description or f"Support tickets from {platform}"

        @asset(
            name=asset_name,
            description=description,
            group_name="support",
        )
        def support_ticket_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests support ticket data."""
            import os

            context.log.info(f"Starting support ticket ingestion from {platform}")

            if platform.lower() == "zendesk":
                from dlt.sources.zendesk import zendesk_support

                # Get credentials from environment variables
                subdomain = os.getenv("ZENDESK_SUBDOMAIN", "example")
                email = os.getenv("ZENDESK_EMAIL", "")
                api_token = os.getenv("ZENDESK_API_TOKEN", "")

                if not email or not api_token:
                    context.log.warning("Zendesk credentials not found in environment. Returning sample data.")
                    return pd.DataFrame({
                        'id': [1, 2, 3],
                        'subject': ['Sample ticket 1', 'Sample ticket 2', 'Sample ticket 3'],
                        'status': ['open', 'pending', 'solved'],
                        'priority': ['high', 'normal', 'low'],
                        '_resource_type': ['tickets', 'tickets', 'tickets']
                    })

                # Create in-memory DuckDB pipeline
                pipeline = dlt.pipeline(
                    pipeline_name=f"{asset_name}_pipeline",
                    destination="duckdb",
                    dataset_name=f"{asset_name}_temp"
                )

                # Create Zendesk source
                source = zendesk_support(
                    credentials={
                        "subdomain": subdomain,
                        "email": email,
                        "password": api_token,
                    }
                )

                # Extract tickets
                load_info = pipeline.run([source.tickets])
                context.log.info(f"Zendesk data loaded: {load_info}")

                # Extract data from DuckDB to DataFrame
                dataset_name = f"{asset_name}_temp"
                try:
                    query = f"SELECT * FROM {dataset_name}.tickets"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                df['_resource_type'] = 'tickets'
                                context.log.info(f"Extracted {len(df)} tickets from Zendesk")

                                return Output(
                                    value=df,
                                    metadata={
                                        "row_count": len(df),
                                        "platform": platform,
                                        "preview": MetadataValue.md(df.head(5).to_markdown())
                                    }
                                )
                except Exception as e:
                    context.log.warning(f"Could not extract tickets: {e}")
                    return pd.DataFrame()

            else:
                # For other platforms, return sample data
                context.log.warning(f"Platform {platform} not yet implemented. Returning sample data.")
                return pd.DataFrame({
                    'id': [1, 2, 3],
                    'subject': [f'Sample {platform} ticket 1', f'Sample {platform} ticket 2', f'Sample {platform} ticket 3'],
                    'status': ['open', 'pending', 'solved'],
                    'priority': ['high', 'normal', 'low'],
                    '_resource_type': ['tickets', 'tickets', 'tickets']
                })

        return Definitions(assets=[support_ticket_ingestion_asset])
