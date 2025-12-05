from dagster import AssetExecutionContext
from dagster_components import Component, component_type
from dagster_components.core.component import ComponentLoadContext
from pydantic import BaseModel
import pandas as pd


class HubSpotIngestionComponentModel(BaseModel):
    """Model for HubSpotIngestionComponent component configuration."""
    asset_name: str
    description: str = ""


@component_type(name="dagster_component_templates.HubSpotIngestionComponent")
class HubSpotIngestionComponent(Component):
    """
    Ingest CRM data from HubSpot

    This is a stub component. Implement the actual logic as needed.
    """

    params_schema = HubSpotIngestionComponentModel

    def __init__(self, dirpath, context: ComponentLoadContext):
        super().__init__(dirpath, context)

    def build_defs(self, load_context):
        from dagster import asset, Definitions, Output, AssetSpec

        params = self.params

        @asset(
            name=params.asset_name,
            description=params.description or "Ingest CRM data from HubSpot",
        )
        def hub_spot_ingestion_asset(context: AssetExecutionContext):
            """Stub implementation - returns empty DataFrame."""
            context.log.warning("Using stub implementation of HubSpotIngestionComponent")
            return pd.DataFrame()

        return Definitions(assets=[hub_spot_ingestion_asset])
