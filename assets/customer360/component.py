"""Customer 360 Component.

Create unified customer profile from multiple data sources.
This is a simplified transformation component that accepts any upstream data
and creates a basic customer 360 view.
"""

from typing import Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
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


class Customer360Component(Component, Model, Resolvable):
    """Component for creating unified customer profiles (Customer 360 view).

    This component unifies customer data from multiple upstream sources
    into a single customer profile with aggregated metrics.

    Example:
        ```yaml
        type: dagster_component_templates.Customer360Component
        attributes:
          asset_name: customer_360
        ```
    """

    asset_name: str = Field(
        description="Name of the unified customer profile asset"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or "Unified customer 360 profiles"

        @asset(
            name=asset_name,
            description=description,
            group_name="customer_analytics",
        )
        def customer_360_asset(context: AssetExecutionContext, **upstream_assets) -> pd.DataFrame:
            """Asset that creates unified customer profiles from upstream data."""

            context.log.info(f"Building Customer 360 unified profiles from {len(upstream_assets)} upstream assets")

            if not upstream_assets:
                context.log.warning("No upstream data sources found. Returning sample data.")
                return pd.DataFrame({
                    'customer_id': [1, 2, 3],
                    'email': ['customer1@example.com', 'customer2@example.com', 'customer3@example.com'],
                    'total_interactions': [10, 25, 5],
                    'first_seen': pd.date_range('2024-01-01', periods=3, freq='D'),
                    'is_active': [True, True, False]
                })

            # Collect all upstream data
            all_data = []
            for asset_name_key, df in upstream_assets.items():
                if isinstance(df, pd.DataFrame) and len(df) > 0:
                    context.log.info(f"Processing {asset_name_key}: {len(df)} rows")
                    all_data.append(df)

            if not all_data:
                context.log.warning("No valid DataFrames in upstream assets")
                return pd.DataFrame()

            # Combine all upstream data
            combined = pd.concat(all_data, ignore_index=True)
            context.log.info(f"Combined {len(combined)} total rows from {len(all_data)} sources")

            # Try to find customer identifiers
            customer_cols = []
            for col in combined.columns:
                col_lower = col.lower()
                if any(id_term in col_lower for id_term in ['customer', 'user', 'email', 'id']):
                    customer_cols.append(col)

            if not customer_cols:
                context.log.warning("No customer identifier columns found. Using index.")
                # Create basic aggregation
                customer_360 = pd.DataFrame({
                    'customer_id': range(1, len(combined) + 1),
                    'total_records': [1] * len(combined),
                    'data_sources': [len(all_data)] * len(combined)
                })
            else:
                # Use first customer identifier for grouping
                group_col = customer_cols[0]
                context.log.info(f"Using '{group_col}' as primary customer identifier")

                # Aggregate by customer
                numeric_cols = combined.select_dtypes(include=['number']).columns.tolist()

                agg_dict = {}
                for col in numeric_cols:
                    if col != group_col:
                        agg_dict[col] = 'sum'

                if agg_dict:
                    customer_360 = combined.groupby(group_col).agg(agg_dict).reset_index()
                else:
                    # No numeric columns to aggregate, just count
                    customer_360 = combined.groupby(group_col).size().reset_index(name='total_records')

                customer_360.rename(columns={group_col: 'customer_id'}, inplace=True)

                # Add derived metrics
                customer_360['data_sources'] = len(all_data)
                customer_360['is_active'] = True  # Default to active

            context.log.info(f"Created {len(customer_360)} unified customer profiles")

            # Return with metadata
            return Output(
                value=customer_360,
                metadata={
                    "total_customers": len(customer_360),
                    "data_sources": len(all_data),
                    "attributes": len(customer_360.columns),
                    "columns": list(customer_360.columns),
                    "preview": MetadataValue.md(customer_360.head(10).to_markdown())
                }
            )

        return Definitions(assets=[customer_360_asset])
