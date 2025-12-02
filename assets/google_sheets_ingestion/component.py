"""Google Sheets Ingestion Component.

Ingest Google Sheets data using dlt (data load tool).
Extracts data from multiple sheets within a spreadsheet.
"""

from typing import Optional, List, Dict, Any
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


class GoogleSheetsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Google Sheets data using dlt.

    Google Sheets is a cloud-based spreadsheet application. This component
    extracts data from Google Sheets and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    The component uses dlt's verified Google Sheets source to handle authentication
    and data extraction.

    Example:
        ```yaml
        type: dagster_component_templates.GoogleSheetsIngestionComponent
        attributes:
          asset_name: google_sheets_data
          credentials: "{{ env('GOOGLE_SHEETS_CREDENTIALS_JSON') }}"
          spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
          sheet_names:
            - Sheet1
            - Sales Data
            - Products
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    credentials: Dict[str, Any] = Field(
        description="Google service account credentials JSON (as dict or JSON string)"
    )

    spreadsheet_id: str = Field(
        description="Google Sheets spreadsheet ID (from the URL)"
    )

    sheet_names: List[str] = Field(
        description="List of sheet names to extract from the spreadsheet"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="google_sheets",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        credentials = self.credentials
        spreadsheet_id = self.spreadsheet_id
        sheet_names = self.sheet_names
        description = self.description or f"Google Sheets data ({', '.join(sheet_names)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def google_sheets_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Google Sheets data using dlt."""
            from dlt.sources.google_sheets import google_spreadsheet

            context.log.info(f"Starting Google Sheets ingestion for sheets: {sheet_names}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Google Sheets source
            source = google_spreadsheet(
                credentials=credentials,
                spreadsheet_id=spreadsheet_id,
                range_names=sheet_names,
            )

            # Run pipeline
            load_info = pipeline.run(source)
            context.log.info(f"Google Sheets data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []
            for sheet_name in sheet_names:
                try:
                    # Clean sheet name for table lookup (replace spaces, special chars)
                    table_name = sheet_name.replace(" ", "_").replace("-", "_").lower()
                    query = f"SELECT * FROM {asset_name}_temp.{table_name}"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                df['_resource_type'] = sheet_name
                                all_data.append(df)
                                context.log.info(f"Extracted {len(df)} rows from {sheet_name}")
                except Exception as e:
                    context.log.warning(f"Could not extract {sheet_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Google Sheets")
                return pd.DataFrame()

            # Combine all sheets into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Google Sheets ingestion complete: {len(combined_df)} total rows from {len(all_data)} sheets"
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

        return Definitions(assets=[google_sheets_ingestion_asset])
