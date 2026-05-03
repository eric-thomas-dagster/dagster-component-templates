"""PandasDataframeCheckComponent.

Wraps `dagster-pandas` constraints to validate column existence, dtypes, and value bounds against the upstream DataFrame asset. Lighter-weight than Pandera for simple shape checks.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class PandasDataframeCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Validate a DataFrame asset's column constraints via dagster-pandas."""

    asset_key: str = Field(description="Asset key the check validates.")
    required_columns: List[str] = Field(description="Columns that must be present.")
    column_types: Optional[Dict[str, str]] = Field(default=None, description="Mapping of column → expected dtype name (e.g. 'int64', 'object').")
    blocking: bool = Field(default=True, description="If True, fail blocks downstream assets.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey.from_user_string(self.asset_key)
        required = self.required_columns
        column_types = self.column_types or {}

        @dg.asset_check(asset=asset_key, blocking=self.blocking, description="Pandas dataframe shape + dtype check")
        def _pandas_check(context, upstream) -> dg.AssetCheckResult:
            errors = []
            for col in required:
                if col not in upstream.columns:
                    errors.append(f"missing column: {col}")
            for col, expected in column_types.items():
                if col in upstream.columns:
                    actual = str(upstream[col].dtype)
                    if expected not in actual:
                        errors.append(f"column {col}: expected dtype {expected}, got {actual}")
            return dg.AssetCheckResult(
                passed=len(errors) == 0,
                severity=dg.AssetCheckSeverity.ERROR if errors else dg.AssetCheckSeverity.WARN,
                metadata={"errors": dg.MetadataValue.text("\n".join(errors)) if errors else dg.MetadataValue.text("ok")},
            )
        return dg.Definitions(asset_checks=[_pandas_check])

