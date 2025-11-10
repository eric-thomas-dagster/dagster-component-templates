"""AssetReferenceComponent for including existing assets in pipeline definitions."""

from typing import Optional
from pydantic import Field
from dagster import Definitions, Resolvable, Model
from dagster._core.definitions.component import Component, ComponentLoadContext


class AssetReferenceComponent(Component, Model, Resolvable):
    """Reference an existing asset (dbt, python, etc.) in a pipeline.

    Use this component to include existing assets from your codebase in a
    pipeline definition without creating new assets. This is useful when you want to:

    1. Mix new component-generated assets with existing hand-written assets
    2. Include dbt models in a pipeline with other components
    3. Reference Python assets defined elsewhere
    4. Create dependencies between new and existing assets

    This component doesn't create any new assets - it just provides metadata
    about an existing asset so it can be:
    - Displayed in the UI graph
    - Included in DependencyGraphComponent edges
    - Selected in pipeline/schedule asset_selection

    Example - Mix dbt and API assets:
        # Reference existing dbt model
        type: dagster_component_templates.AssetReferenceComponent
        attributes:
          asset_name: dbt_staging_customers
          module: my_project.dbt_assets
          description: "Staging customers from raw data"

        ---

        # New API asset
        type: dagster_component_templates.RestApiFetcherComponent
        attributes:
          asset_name: api_enrichment_data
          api_url: https://api.example.com/enrichment

        ---

        # Wire them together
        type: dagster_component_templates.DependencyGraphComponent
        attributes:
          edges:
            - source: dbt_staging_customers
              target: api_enrichment_data

    Example - Reference custom Python asset:
        # In your code: my_project/assets.py
        @asset
        def my_custom_calculation():
            return expensive_computation()

        # In defs.yaml:
        type: dagster_component_templates.AssetReferenceComponent
        attributes:
          asset_name: my_custom_calculation
          module: my_project.assets
          description: "Custom calculation logic"

    The code generator will NOT create a new asset - it will import and reference
    the existing one from the specified module.
    """

    asset_name: str = Field(
        description="Name of the existing asset to reference. "
        "Must match the asset name defined in your codebase."
    )

    module: Optional[str] = Field(
        default=None,
        description="Python module where the asset is defined (e.g., 'my_project.dbt_assets'). "
        "If not specified, assumes the asset is available in the global Definitions."
    )

    description: Optional[str] = Field(
        default=None,
        description="Description of what this asset does (for documentation)"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization in the UI"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions.

        This component doesn't generate any new assets - it just provides
        metadata about an existing asset. The actual asset must be defined
        elsewhere in your codebase.

        The code generator (template_service.py) will:
        1. Skip code generation for AssetReferenceComponent
        2. Import the asset from the specified module (if provided)
        3. Include it in the Definitions so it appears in the graph
        """
        context.log.info(
            f"AssetReferenceComponent referencing existing asset: {self.asset_name}"
        )
        if self.module:
            context.log.info(f"  Module: {self.module}")

        # Return empty Definitions - this component doesn't create assets
        # The actual asset must exist elsewhere in the codebase
        return Definitions(assets=[])
