"""JDE Orchestration Output Ingestion Component.

Execute a JDE orchestration and materialize its result set as a DataFrame.
Common pattern: orchestrations that wrap JDE Data Services (table reads)
or Business Function calls returning tabular results.
"""
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class JDEOrchestrationOutputIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute a JDE orchestration and emit its output rows as a DataFrame.

    Example:

        ```yaml
        type: dagster_community_components.JDEOrchestrationOutputIngestionComponent
        attributes:
          asset_key: open_ap_invoices
          orchestration: JDE_Fetch_Open_APs
          inputs:
            CompanyCode: "00001"
            AsOfDate: "2026-07-10"
          output_field: RowSet         # key in the response JSON that holds rows
          group_name: jde_finance
          resource_key: jde_orchestrator_resource
        ```
    """

    asset_key: str = Field(description="Asset key for the emitted DataFrame.")
    orchestration: str = Field(description="Orchestration name.")
    inputs: Optional[Dict[str, Any]] = Field(default=None, description="Orchestration input parameter overrides.")
    output_field: Optional[str] = Field(
        default=None,
        description=(
            "Key in the response JSON that holds the row list. Common values: "
            "'RowSet', 'ServiceRequest1.RowSet', 'result'. Auto-detects flat "
            "top-level lists if unset."
        ),
    )
    timeout_seconds: int = Field(default=300)
    group_name: Optional[str] = Field(default=None)
    resource_key: str = Field(default="jde_orchestrator_resource")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(
            name=_self.asset_key,
            group_name=_self.group_name,
            required_resource_keys=required_resource_keys,
            compute_kind="jde",
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

            url = resource.orchestration_url(_self.orchestration)
            body = _self.inputs.copy() if _self.inputs else {}
            r = session.post(url, json=body, headers=headers, timeout=_self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"JDE orchestration output ingest failed: {r.status_code} {r.text[:200]}"
                )
            payload = r.json() or {}

            # Extract rows.
            rows = None
            if _self.output_field:
                # Walk dot-separated path.
                cur: Any = payload
                for part in _self.output_field.split("."):
                    if isinstance(cur, dict):
                        cur = cur.get(part)
                    else:
                        cur = None
                        break
                rows = cur
            else:
                # Autodetect: look for the first list-of-dicts we find.
                for v in payload.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        rows = v
                        break
                    if isinstance(v, dict):
                        for w in v.values():
                            if isinstance(w, list) and w and isinstance(w[0], dict):
                                rows = w
                                break
                        if rows is not None:
                            break

            if rows is None:
                context.log.warning("No row-list found in orchestration response — returning empty DataFrame.")
                rows = []

            df = pd.DataFrame(rows)
            context.add_output_metadata({
                "row_count": len(df),
                "orchestration": _self.orchestration,
                "output_field_used": _self.output_field or "(autodetected)",
            })
            return df

        return dg.Definitions(assets=[_the_asset])
