"""DependencyGraphComponent for defining asset dependencies without modifying component schemas."""

from typing import Any, Dict, List, Optional, Union
from pydantic import Field
from dagster import Definitions, Resolvable, Model
from dagster import Component, ComponentLoadContext


class DependencyGraphComponent(Component, Model, Resolvable):
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


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Dagster asset group name.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
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
