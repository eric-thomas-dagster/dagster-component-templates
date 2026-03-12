"""Soda Check Component.

Runs a Soda scan against a data source and surfaces results as
a Dagster asset check.
"""
from typing import Optional
import dagster as dg
from pydantic import Field


class SodaCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Soda scan as a Dagster asset check.

    Example:
        ```yaml
        type: dagster_component_templates.SodaCheckComponent
        attributes:
          asset_key: warehouse/orders
          data_source_name: my_postgres
          checks_yaml_path: ./soda/orders_checks.yaml
          soda_configuration_path: ./soda/configuration.yaml
        ```
    """

    asset_key: str = Field(description="Asset key to attach this check to")
    data_source_name: str = Field(description="Soda data source name (defined in configuration.yaml)")
    checks_yaml_path: str = Field(description="Path to the Soda checks YAML file")
    soda_configuration_path: str = Field(
        default="./soda/configuration.yaml",
        description="Path to the Soda configuration YAML file",
    )
    severity: str = Field(default="ERROR", description="WARN or ERROR")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

        asset_key = dg.AssetKey(self.asset_key.split("/"))
        severity = (
            AssetCheckSeverity.ERROR if self.severity.upper() == "ERROR"
            else AssetCheckSeverity.WARN
        )
        _self = self

        @asset_check(asset=asset_key, description=f"Run Soda scan: {self.checks_yaml_path}")
        def soda_check(context):
            try:
                from soda.scan import Scan
            except ImportError:
                return AssetCheckResult(passed=False, severity=severity,
                    metadata={"error": "soda-core not installed. Run: pip install soda-core"})

            try:
                scan = Scan()
                scan.set_data_source_name(_self.data_source_name)
                scan.add_configuration_yaml_file(_self.soda_configuration_path)
                scan.add_sodacl_yaml_file(_self.checks_yaml_path)
                scan.execute()
                has_errors = scan.has_check_fails() or scan.has_errors()
                checks_passed = len(scan.get_checks_pass())
                checks_failed = len(scan.get_checks_fail())
                checks_warned = len(scan.get_checks_warn())
            except Exception as e:
                return AssetCheckResult(passed=False, severity=severity, metadata={"error": str(e)})

            return AssetCheckResult(
                passed=not has_errors,
                severity=severity,
                metadata={
                    "checks_passed": checks_passed,
                    "checks_failed": checks_failed,
                    "checks_warned": checks_warned,
                    "checks_yaml_path": _self.checks_yaml_path,
                },
            )

        return dg.Definitions(asset_checks=[soda_check])
