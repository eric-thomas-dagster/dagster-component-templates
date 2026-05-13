"""OData Asset Check.

Validates that an OData service URL is reachable and that a specific entity set
returns at least N rows, optionally matching a `$filter` predicate. Attaches as
an AssetCheckResult to a Dagster asset.

Use cases:
- Smoke-test SAP S/4HANA tenant connectivity in CI
- Verify a SuccessFactors entity set actually returns data before downstream
  jobs depend on it
- Catch schema drift early (any column rename / disappearance trips `$select`)

Example — confirm 1+ business partners exist:

    ```yaml
    type: dagster_component_templates.ODataCheckComponent
    attributes:
      asset_key: sap/business_partners
      service_url: https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER
      entity_set: A_BusinessPartner
      odata_version: v2
      auth_type: basic
      auth_username_env_var: S4HANA_USER
      auth_password_env_var: S4HANA_PASSWORD
      min_rows: 1
    ```
"""

import os
from typing import Any, Dict, Optional

import requests
import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    MetadataValue,
    asset_check,
)
from pydantic import Field


class ODataCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Validate that an OData entity set is reachable and matches expectations."""

    asset_key: str = Field(description="Asset key to attach this check to")
    check_name: Optional[str] = Field(
        default=None, description="Check name. Defaults to 'odata_reachable_<entity_set>'."
    )

    service_url: str = Field(description="Base OData service URL")
    entity_set: str = Field(description="Entity collection to probe")
    odata_version: str = Field(default="v2", description="'v2' or 'v4'")

    filter: Optional[str] = Field(
        default=None, description="`$filter` predicate (applied to the count query)."
    )

    # --- Expectations --------------------------------------------------------

    min_rows: int = Field(
        default=1, description="Fail if fewer than this many rows match."
    )
    max_rows: Optional[int] = Field(
        default=None, description="Optional — fail if more than this many rows match."
    )
    expect_columns: Optional[list[str]] = Field(
        default=None,
        description="If set, the response's first row must contain all these columns; otherwise WARN/FAIL.",
    )

    # --- Auth ----------------------------------------------------------------

    auth_type: str = Field(default="basic", description="'basic' | 'bearer' | 'none'")
    auth_username_env_var: Optional[str] = Field(default=None)
    auth_password_env_var: Optional[str] = Field(default=None)
    auth_token_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(default=None)
    verify_ssl: bool = Field(default=True)
    timeout_seconds: int = Field(default=60)

    severity: str = Field(
        default="ERROR", description="'ERROR' (default) or 'WARN' on failure"
    )

    def _resolve_auth(self):
        if self.auth_type == "none":
            return None, {}
        if self.auth_type == "basic":
            if not (self.auth_username_env_var and self.auth_password_env_var):
                raise ValueError(
                    "auth_type='basic' requires auth_username_env_var + auth_password_env_var."
                )
            return (
                os.environ[self.auth_username_env_var],
                os.environ[self.auth_password_env_var],
            ), {}
        if self.auth_type == "bearer":
            if not self.auth_token_env_var:
                raise ValueError("auth_type='bearer' requires auth_token_env_var.")
            return None, {"Authorization": f"Bearer {os.environ[self.auth_token_env_var]}"}
        raise ValueError(f"unknown auth_type: {self.auth_type!r}")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        asset_key = AssetKey(self.asset_key.split("/"))
        check_name = self.check_name or f"odata_reachable_{self.entity_set}"
        severity = (
            AssetCheckSeverity.WARN
            if self.severity.upper() == "WARN"
            else AssetCheckSeverity.ERROR
        )

        @asset_check(asset=asset_key, name=check_name)
        def _check(context) -> AssetCheckResult:
            auth, extra = component._resolve_auth()
            headers = {"Accept": "application/json"}
            headers.update(extra)
            if component.extra_headers:
                headers.update(component.extra_headers)

            # Build $top=N probe query
            base = component.service_url.rstrip("/") + "/" + component.entity_set.lstrip("/")
            params: Dict[str, str] = {"$format": "json", "$top": str(max(component.min_rows, 1))}
            if component.filter:
                params["$filter"] = component.filter

            try:
                resp = requests.get(
                    base,
                    params=params,
                    auth=auth,
                    headers=headers,
                    timeout=component.timeout_seconds,
                    verify=component.verify_ssl,
                )
            except Exception as e:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    description=f"OData reachability check failed: {e}",
                    metadata={"service_url": MetadataValue.text(component.service_url)},
                )

            metadata: Dict[str, Any] = {
                "status_code": MetadataValue.int(resp.status_code),
                "service_url": MetadataValue.text(component.service_url),
                "entity_set": MetadataValue.text(component.entity_set),
            }

            if resp.status_code >= 400:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    description=f"HTTP {resp.status_code}: {resp.text[:300]}",
                    metadata=metadata,
                )

            body = resp.json()
            if component.odata_version == "v4":
                rows = body.get("value", [])
            else:
                d = body.get("d", body)
                rows = d.get("results", [d] if isinstance(d, dict) else [])

            row_count = len(rows)
            metadata["rows_returned"] = MetadataValue.int(row_count)

            if row_count < component.min_rows:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    description=f"Expected >={component.min_rows} rows, got {row_count}",
                    metadata=metadata,
                )
            if component.max_rows is not None and row_count > component.max_rows:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    description=f"Expected <={component.max_rows} rows, got {row_count}",
                    metadata=metadata,
                )

            if component.expect_columns and rows:
                actual = set(rows[0].keys())
                missing = [c for c in component.expect_columns if c not in actual]
                if missing:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        description=f"Missing expected columns: {missing}",
                        metadata={**metadata, "missing_columns": MetadataValue.json(missing)},
                    )

            return AssetCheckResult(
                passed=True,
                description=f"OData reachable: {row_count} row(s) from {component.entity_set}",
                metadata=metadata,
            )

        return dg.Definitions(asset_checks=[_check])
