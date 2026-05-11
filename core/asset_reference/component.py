"""AssetReferenceComponent for including existing assets in pipeline definitions."""

from typing import Dict, List, Optional
from pydantic import Field
from dagster import Definitions, Resolvable, Model
from dagster import Component, ComponentLoadContext


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

    partition_date_column: Optional[str] = Field(
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

    partition_static_column: Optional[str] = Field(
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
