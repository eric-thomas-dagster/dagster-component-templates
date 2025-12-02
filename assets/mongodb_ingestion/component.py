"""MongoDB Ingestion Component.

Ingest MongoDB database data using dlt (data load tool).
Extracts collections from MongoDB databases.
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


class MongoDBIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting MongoDB database data using dlt.

    MongoDB is a popular NoSQL document database. This component extracts data
    from MongoDB collections and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    The component uses dlt's verified MongoDB source to handle connection management
    and efficient data extraction.

    Example:
        ```yaml
        type: dagster_component_templates.MongoDBIngestionComponent
        attributes:
          asset_name: mongodb_data
          connection_url: "{{ env('MONGODB_CONNECTION_URL') }}"
          database: "my_database"
          collection_names:
            - users
            - orders
            - products
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    connection_url: str = Field(
        description="MongoDB connection URL (e.g., mongodb://localhost:27017)"
    )

    database: str = Field(
        description="Name of the MongoDB database to extract from"
    )

    collection_names: List[str] = Field(
        description="List of collection names to extract"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="mongodb",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        connection_url = self.connection_url
        database = self.database
        collection_names = self.collection_names
        description = self.description or f"MongoDB data from {database} ({', '.join(collection_names)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def mongodb_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests MongoDB data using dlt."""
            from dlt.sources.mongodb import mongodb

            context.log.info(f"Starting MongoDB ingestion for collections: {collection_names}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create MongoDB source
            source = mongodb(
                connection_url=connection_url,
                database=database,
                collection_names=collection_names,
            )

            # Run pipeline
            load_info = pipeline.run(source)
            context.log.info(f"MongoDB data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []
            for collection_name in collection_names:
                try:
                    query = f"SELECT * FROM {asset_name}_temp.{collection_name}"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                df['_resource_type'] = collection_name
                                all_data.append(df)
                                context.log.info(f"Extracted {len(df)} rows from {collection_name}")
                except Exception as e:
                    context.log.warning(f"Could not extract {collection_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from MongoDB")
                return pd.DataFrame()

            # Combine all collections into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"MongoDB ingestion complete: {len(combined_df)} total rows from {len(all_data)} collections"
            )

            # Add metadata
            metadata = {
                "row_count": len(combined_df),
                "resources_extracted": len(all_data),
                "resource_types": list(combined_df['_resource_type'].unique()) if '_resource_type' in combined_df.columns else [],
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

        return Definitions(assets=[mongodb_ingestion_asset])
