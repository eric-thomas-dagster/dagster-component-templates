"""Cognos Report Data Ingestion Component.

Run a Cognos report with output_format=CSV or JSON, parse the result set
as a DataFrame. Useful for landing report data into a warehouse or
chaining downstream Dagster transforms.
"""
import io
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class CognosReportDataIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a Cognos report's output as a DataFrame.

    Example:

        ```yaml
        type: dagster_community_components.CognosReportDataIngestionComponent
        attributes:
          asset_key: monthly_pnl_report
          report_id: i8B1A56A56789ABCDEF01234567890AB
          output_format: CSV
          parameters:
            Month: "2026-07"
          resource_key: cognos_resource
        ```
    """

    asset_key: str = Field(description="Asset key for the emitted DataFrame.")
    report_id: str = Field(description="Cognos report ID.")
    output_format: str = Field(default="CSV", description="CSV | JSON — parseable formats.")
    parameters: Optional[Dict[str, Any]] = Field(default=None)
    timeout_seconds: int = Field(default=600)
    group_name: Optional[str] = Field(default=None)
    resource_key: str = Field(default="cognos_resource")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(
            name=_self.asset_key,
            group_name=_self.group_name,
            required_resource_keys=required_resource_keys,
            compute_kind="cognos",
        )
        def _the_asset(context: dg.AssetExecutionContext):
            try:
                import pandas as pd
                import requests
            except ImportError as e:
                raise Exception("pandas or requests library not installed") from e

            resource = getattr(context.resources, _self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            login_body = resource.login_body()
            if login_body is not None:
                r = session.post(f"{resource.api_base}/session", json=login_body, timeout=30)
                if r.status_code >= 300:
                    raise Exception(f"Cognos login failed: {r.status_code}")

            fmt = _self.output_format.upper()
            if fmt not in ("CSV", "JSON"):
                raise Exception(f"Only CSV or JSON output_format supported for ingestion; got {fmt}")

            body: dict = {"format": fmt}
            if _self.parameters:
                body["parameters"] = [{"name": k, "value": str(v)} for k, v in _self.parameters.items()]

            run_url = f"{resource.report_url(_self.report_id)}/data"
            r = session.post(run_url, json=body, headers=headers, timeout=_self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(f"Cognos report run failed: {r.status_code} {r.text[:200]}")

            if fmt == "CSV":
                df = pd.read_csv(io.StringIO(r.text))
            else:  # JSON
                payload = r.json() or {}
                rows = payload.get("data") or payload.get("rows") or payload
                if isinstance(rows, dict):
                    # Nested — walk one level to find a list.
                    for v in rows.values():
                        if isinstance(v, list):
                            rows = v
                            break
                df = pd.DataFrame(rows if isinstance(rows, list) else [])

            context.add_output_metadata({
                "row_count": len(df),
                "report_id": _self.report_id,
                "output_format": fmt,
            })
            return df

        return dg.Definitions(assets=[_the_asset])
