"""Great Expectations Check Component.

Runs a Great Expectations expectation suite against an asset and
surfaces the results as a Dagster asset check.
"""
from typing import Optional
import dagster as dg
from pydantic import Field


class GreatExpectationsCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Great Expectations expectation suite as a Dagster asset check.

    Example:
        ```yaml
        type: dagster_component_templates.GreatExpectationsCheckComponent
        attributes:
          asset_key: warehouse/orders
          ge_context_root_dir: ./great_expectations
          datasource_name: my_postgres
          data_asset_name: orders
          expectation_suite_name: orders.critical
        ```
    """

    asset_key: str = Field(description="Asset key to attach this check to")
    ge_context_root_dir: str = Field(description="Path to the Great Expectations project root")
    datasource_name: str = Field(description="GE datasource name")
    data_asset_name: str = Field(description="GE data asset name")
    expectation_suite_name: str = Field(description="GE expectation suite name")
    severity: str = Field(default="ERROR", description="WARN or ERROR")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

        asset_key = dg.AssetKey(self.asset_key.split("/"))
        severity = (
            AssetCheckSeverity.ERROR if self.severity.upper() == "ERROR"
            else AssetCheckSeverity.WARN
        )
        _self = self

        @asset_check(asset=asset_key, description=f"Run GE suite: {self.expectation_suite_name}")
        def ge_check(context):
            try:
                import great_expectations as gx
            except ImportError:
                return AssetCheckResult(passed=False, severity=severity,
                    metadata={"error": "great-expectations not installed"})

            try:
                ge_context = gx.get_context(context_root_dir=_self.ge_context_root_dir)
                validator = ge_context.get_validator(
                    datasource_name=_self.datasource_name,
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name=_self.data_asset_name,
                    expectation_suite_name=_self.expectation_suite_name,
                )
                result = validator.validate()
            except Exception as e:
                return AssetCheckResult(passed=False, severity=severity, metadata={"error": str(e)})

            stats = result.statistics
            return AssetCheckResult(
                passed=result.success,
                severity=severity,
                metadata={
                    "evaluated_expectations": stats.get("evaluated_expectations", 0),
                    "successful_expectations": stats.get("successful_expectations", 0),
                    "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
                    "suite_name": _self.expectation_suite_name,
                },
            )

        return dg.Definitions(asset_checks=[ge_check])
