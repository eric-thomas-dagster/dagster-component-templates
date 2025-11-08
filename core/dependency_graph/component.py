"""DependencyGraphComponent for defining asset dependencies without modifying component schemas."""

from typing import List, Dict, Any
from pydantic import Field
from dagster import Definitions
from dagster._core.definitions.component import Component, ComponentLoadContext


class DependencyGraphComponent(Component):
    """Defines the dependency graph between assets in a pipeline.

    This component doesn't create any assets - it only specifies how
    existing assets depend on each other. This allows you to wire up
    dependencies between assets from different components without
    requiring those components to have a 'deps' field.

    The dependency graph is used by:
    1. The UI to draw edges between asset nodes
    2. The code generator to create implicit dependencies (parameter names matching asset names)
    3. Dagster's IO manager pattern to pass data between assets

    Example:
        # Define three assets using different components
        type: dagster_component_templates.RestApiFetcherComponent
        attributes:
          asset_name: api_data
          api_url: https://api.example.com/data
          output_format: dataframe

        ---

        type: dagster_component_templates.DataFrameTransformerComponent
        attributes:
          asset_name: cleaned_data
          drop_duplicates: true

        ---

        # Wire them together with DependencyGraphComponent
        type: dagster_component_templates.DependencyGraphComponent
        attributes:
          edges:
            - source: api_data
              target: cleaned_data

    The code generator will create:
        @asset
        def api_data():
            ...

        @asset
        def cleaned_data(api_data):  # Implicit dependency via parameter name
            ...

    And Dagster's IO manager will automatically pass the api_data DataFrame
    to cleaned_data.
    """

    edges: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of edges defining dependencies between assets. "
        "Each edge must have 'source' and 'target' keys with asset names."
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions.

        This component doesn't generate any assets - it just stores metadata
        about how assets depend on each other. The actual wiring happens via:
        1. Code generation (template_service.py reads these edges)
        2. Implicit dependencies (function parameter names match asset names)
        """
        # Validate edges
        for i, edge in enumerate(self.edges):
            if "source" not in edge or "target" not in edge:
                raise ValueError(
                    f"Edge {i} must have both 'source' and 'target' keys. Got: {edge}"
                )
            if not isinstance(edge["source"], str) or not isinstance(edge["target"], str):
                raise ValueError(
                    f"Edge {i} source and target must be strings. Got: {edge}"
                )

        # Log the dependency graph for visibility
        context.log.info(f"DependencyGraphComponent loaded with {len(self.edges)} edges:")
        for edge in self.edges:
            context.log.info(f"  {edge['source']} → {edge['target']}")

        # Return empty Definitions - this component doesn't create assets
        return Definitions(assets=[])
