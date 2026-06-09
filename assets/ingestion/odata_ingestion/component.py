"""OData Ingestion Component.

Generic OData v2 / v4 client → pandas DataFrame. OData is a much wider protocol
than SAP — this single component covers a long list of enterprise systems:

SAP family (OData v2 mostly; v4 on newer S/4HANA APIs):
- S/4HANA Cloud + on-prem (`/sap/opu/odata/sap/<service>/<entity>`)
- SuccessFactors (`api<dc>.successfactors.com/odata/v2`)
- Datasphere / Data Warehouse Cloud (consumption + catalog APIs)
- Commerce Cloud (Hybris) OCC APIs
- Marketing Cloud / Service Cloud / Customer Data Cloud
- Concur + Ariba (newer endpoints expose OData alongside REST)
- Any SAP NetWeaver Gateway-exposed service

Microsoft family (mostly OData v4):
- Dynamics 365 / Dataverse (Sales, Customer Service, Field Service, Marketing)
- Microsoft Graph (Outlook, Teams, OneDrive, Azure AD)
- SharePoint Online REST APIs
- Business Central
- Project Server / Project Online
- Power BI REST API

Other ERP / line-of-business:
- Oracle Fusion Cloud Applications (Financials, HCM, SCM)
- Epicor ERP (v10+ ships OData v4)
- IFS Cloud
- JD Edwards (newer Tools releases)
- Infor M3 / Infor ION

Public test endpoints (for trying the component without credentials):
- `services.odata.org/V4/Northwind/Northwind.svc/` (v4)
- `services.odata.org/V2/Northwind/Northwind.svc/` (v2)

What the component handles:
- Pagination (v2 `__next` / v4 `@odata.nextLink`)
- Auth: basic / bearer / none (with passthrough headers for CSRF / cookies)
- OData query options: `$filter`, `$select`, `$expand`, `$orderby`, `$top`, custom `$<key>`
- `{partition_key}` substitution into `$filter` for partitioned pulls

Output: pandas DataFrame from the flattened `results` (v2) / `value` (v4) collection.

See the SAP-product walkthroughs for product-specific auth + service URL patterns:
- examples/sap_s4hana_pipeline.md
- examples/sap_successfactors_pipeline.md
- examples/sap_datasphere_pipeline.md
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None

    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [
            v.strip()
            for v in (str(partition_values) if partition_values else "").split(",")
            if v.strip()
        ]

    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ODataIngestionComponent(Component, Model, Resolvable):
    """Pull data from an OData v2/v4 endpoint into a pandas DataFrame.

    The OData protocol is everywhere SAP exposes data programmatically: S/4HANA's
    `/sap/opu/odata/sap/<service>/<entity>` endpoints, SuccessFactors's
    `api<dc>.successfactors.com/odata/v2`, Datasphere's consumption APIs, plus
    Microsoft Dynamics 365 and MS Graph.

    Example — SAP S/4HANA business partners:

        ```yaml
        type: dagster_component_templates.ODataIngestionComponent
        attributes:
          asset_name: business_partners
          service_url: https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER
          entity_set: A_BusinessPartner
          odata_version: v2
          auth_type: basic
          auth_username_env_var: S4HANA_USER
          auth_password_env_var: S4HANA_PASSWORD
          select: BusinessPartner,BusinessPartnerName,Country
          filter: CreationDate ge datetime'{partition_key}T00:00:00'
          partition_type: daily
          partition_start: '2024-01-01'
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    # --- Endpoint -------------------------------------------------------------

    service_url: str = Field(
        description=(
            "Base service URL, e.g. "
            "'https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER' or "
            "'https://api.successfactors.com/odata/v2'."
        ),
    )
    entity_set: str = Field(
        description="Entity set / collection name appended to service_url, e.g. 'A_BusinessPartner', 'User', 'PerPerson'."
    )
    odata_version: str = Field(
        default="v2",
        description="'v2' (default — most SAP products) or 'v4' (S/4HANA newer APIs, Microsoft Dynamics).",
    )

    # --- Query options --------------------------------------------------------

    select: Optional[str] = Field(
        default=None, description="`$select` columns, comma-separated."
    )
    filter: Optional[str] = Field(
        default=None,
        description="`$filter` expression. Supports `{partition_key}` substitution.",
    )
    expand: Optional[str] = Field(
        default=None, description="`$expand` — comma-separated nested collections."
    )
    orderby: Optional[str] = Field(default=None, description="`$orderby` expression.")
    top: Optional[int] = Field(
        default=None,
        description="`$top` — page size (server-driven paging usually overrides this; useful for testing).",
    )
    extra_query: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional `$<key>` query options, e.g. {`$inlinecount`: `allpages`}.",
    )

    # --- Auth -----------------------------------------------------------------

    auth_type: str = Field(
        default="basic",
        description="'basic' | 'bearer' | 'none'. Basic auth is the SAP default; bearer for OAuth-token flows.",
    )
    auth_username_env_var: Optional[str] = Field(
        default=None, description="Env var with username (auth_type=basic)."
    )
    auth_password_env_var: Optional[str] = Field(
        default=None, description="Env var with password (auth_type=basic)."
    )
    auth_token_env_var: Optional[str] = Field(
        default=None, description="Env var with bearer token (auth_type=bearer)."
    )
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Extra HTTP headers. Useful for SAP CSRF tokens, sap-client routing "
            "(`sap-client: 100`), or Datasphere `x-sap-cf-token`."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify the server's TLS certificate. Set false for on-prem self-signed.",
    )

    # --- Standard fields ------------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="odata")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None, description="Defaults to ['odata']."
    )
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    max_pages: int = Field(
        default=1000,
        description="Safety cap on pagination follow-throughs (prevents runaway pulls).",
    )
    timeout_seconds: int = Field(default=120, description="Per-request timeout.")

    # -------------------------------------------------------------------------

    def _resolve_auth(self):
        if self.auth_type == "none":
            return None, {}
        if self.auth_type == "basic":
            if not (self.auth_username_env_var and self.auth_password_env_var):
                raise ValueError(
                    "auth_type='basic' requires auth_username_env_var + auth_password_env_var."
                )
            user = os.environ.get(self.auth_username_env_var)
            pwd = os.environ.get(self.auth_password_env_var)
            if not (user and pwd):
                raise ValueError(
                    "Basic auth env vars are set but empty. Check your shell environment."
                )
            return (user, pwd), {}
        if self.auth_type == "bearer":
            if not self.auth_token_env_var:
                raise ValueError("auth_type='bearer' requires auth_token_env_var.")
            tok = os.environ.get(self.auth_token_env_var)
            if not tok:
                raise ValueError(
                    f"auth_token_env_var={self.auth_token_env_var!r} is set but empty."
                )
            return None, {"Authorization": f"Bearer {tok}"}
        raise ValueError(f"unknown auth_type: {self.auth_type!r}")

    def _build_initial_url(self, partition_key: Optional[str]) -> str:
        base = self.service_url.rstrip("/") + "/" + self.entity_set.lstrip("/")
        params: Dict[str, str] = {"$format": "json"}
        if self.select:
            params["$select"] = self.select
        if self.filter:
            f = self.filter
            if partition_key is not None and "{partition_key}" in f:
                f = f.replace("{partition_key}", partition_key)
            params["$filter"] = f
        if self.expand:
            params["$expand"] = self.expand
        if self.orderby:
            params["$orderby"] = self.orderby
        if self.top is not None:
            params["$top"] = str(self.top)
        if self.extra_query:
            params.update(self.extra_query)
        prepared = requests.Request("GET", base, params=params).prepare()
        return prepared.url or base

    def _extract_records(self, body: Any) -> tuple[list, Optional[str]]:
        """Return (records, next_link) for v2 or v4 responses."""
        if not isinstance(body, dict):
            return [], None
        if self.odata_version == "v4":
            return list(body.get("value", [])), body.get("@odata.nextLink")
        # v2 has either {d: {results: [...], __next: '...'}} or {d: {single object}}
        d = body.get("d", body)
        if isinstance(d, dict) and "results" in d:
            return list(d["results"]), d.get("__next")
        return ([d] if isinstance(d, dict) else []), None

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or f"OData ingestion ({self.entity_set})"
        component = self

        kinds = list(self.kinds or []) or ["odata"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
        )
        def odata_ingestion_asset(context: AssetExecutionContext):
            partition_key = (
                context.partition_key if context.has_partition_key else None
            )
            auth, extra = component._resolve_auth()
            headers = {"Accept": "application/json"}
            headers.update(extra)
            if component.extra_headers:
                headers.update(component.extra_headers)

            url: Optional[str] = component._build_initial_url(partition_key)
            all_records: list = []
            pages = 0

            while url and pages < component.max_pages:
                context.log.info(f"OData GET page {pages + 1}: {url[:200]}")
                resp = requests.get(
                    url,
                    auth=auth,
                    headers=headers,
                    timeout=component.timeout_seconds,
                    verify=component.verify_ssl,
                )
                resp.raise_for_status()
                body = resp.json()
                records, next_link = component._extract_records(body)
                all_records.extend(records)
                url = next_link
                pages += 1

            if pages >= component.max_pages and url:
                context.log.warning(
                    f"Hit max_pages={component.max_pages} with more results available. "
                    f"Increase max_pages or add a tighter $filter."
                )

            df = pd.json_normalize(all_records, sep="_") if all_records else pd.DataFrame()
            context.log.info(f"OData ingestion: {len(df)} rows × {len(df.columns)} cols in {pages} pages")

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "pages_fetched": MetadataValue.int(pages),
                "service_url": MetadataValue.text(component.service_url),
                "entity_set": MetadataValue.text(component.entity_set),
                "odata_version": MetadataValue.text(component.odata_version),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            if component.include_preview_metadata and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(component.preview_rows, len(df)))
                        if len(df) > component.preview_rows * 10
                        else df.head(component.preview_rows)
                    )
                    metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[odata_ingestion_asset])
