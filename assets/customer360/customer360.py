from dagster import AssetExecutionContext
from dagster_components import Component, component_type
from dagster_components.core.component import ComponentLoadContext
from pydantic import BaseModel
import pandas as pd


class Customer360ComponentModel(BaseModel):
    """Model for Customer360Component component configuration."""
    asset_name: str
    description: str = ""


@component_type(name="dagster_component_templates.Customer360Component")
class Customer360Component(Component):
    """
    Create unified customer profile from multiple data sources

    This is a stub component. Implement the actual logic as needed.
    """

    params_schema = Customer360ComponentModel

    def __init__(self, dirpath, context: ComponentLoadContext):
        super().__init__(dirpath, context)

    def build_defs(self, load_context):
        from dagster import asset, Definitions, Output, AssetSpec

        params = self.params

        @asset(
            name=params.asset_name,
            description=params.description or "Create unified customer profile from multiple data sources",
        )
        def customer360_asset(context: AssetExecutionContext):
            """Stub implementation - returns empty DataFrame."""
            context.log.warning("Using stub implementation of Customer360Component")
            return pd.DataFrame()

        return Definitions(assets=[customer360_asset])
