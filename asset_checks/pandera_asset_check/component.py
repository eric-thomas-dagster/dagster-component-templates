"""PanderaAssetCheckComponent.

Wraps `dagster-pandera` so a Pandera DataFrameSchema becomes a Dagster asset check. Define schemas in a Python file the component imports, point at the upstream asset, and the check runs the schema against the DataFrame on every materialization.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class PanderaAssetCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Validate a DataFrame asset against a Pandera schema as a Dagster asset check."""

    asset_key: str = Field(description="Asset key the schema validates.")
    schema_module: str = Field(description="Importable module containing the Pandera schema, e.g. 'mypkg.schemas'.")
    schema_name: str = Field(description="Name of the DataFrameSchema variable in that module.")
    blocking: bool = Field(default=True, description="If True, schema failures block downstream materializations.")
    description: Optional[str] = Field(default=None, description="Asset-check description.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_pandera import pandera_schema_to_dagster_type
        import importlib
        module = importlib.import_module(self.schema_module)
        schema = getattr(module, self.schema_name)
        asset_key = dg.AssetKey.from_user_string(self.asset_key)

        @dg.asset_check(
            asset=asset_key,
            blocking=self.blocking,
            description=self.description or f"Pandera validation: {self.schema_module}.{self.schema_name}",
        )
        def _pandera_check(context: dg.AssetCheckExecutionContext, upstream) -> dg.AssetCheckResult:
            try:
                schema.validate(upstream, lazy=True)
                return dg.AssetCheckResult(passed=True)
            except Exception as e:
                return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.ERROR, metadata={"error": str(e)[:2000]})
        return dg.Definitions(asset_checks=[_pandera_check])

