"""Vanta Controls Ingestion Component.

Fetches Vanta compliance controls (SOC 2, ISO 27001, HIPAA, PCI, etc.) plus
their status, framework, description, and evidence counts via `GET /v1/controls`.
Returns a flattened pandas DataFrame — one row per control.

Pairs with `vanta_resource` (OAuth2 client-credentials, cached tokens). Vanta
paginates via a `pageCursor` in the response (opaque cursor) with `pageSize`
query param; this component walks the cursor to completion or up to `limit` rows.

Downstream: feed the DataFrame into `vanta_evidence_response_agent` to auto-draft
control narratives, or into any warehouse sink for compliance reporting.
"""

from typing import Any, Dict, List, Optional

import pandas as pd
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


class VantaControlsIngestionComponent(Component, Model, Resolvable):
    """Ingest Vanta compliance controls into a pandas DataFrame.

    Requires a registered `vanta_resource` (see `resources/vanta_resource/`).

    Example:

        ```yaml
        type: dagster_community_components.VantaControlsIngestionComponent
        attributes:
          asset_name: vanta_soc2_controls
          resource_name: vanta_resource
          framework: soc2
          limit: 1000
        ```
    """

    asset_name: str = Field(description="Name of the Dagster asset to create")

    resource_name: str = Field(
        default="vanta_resource",
        description="Resource key of a registered `vanta_resource` (VantaResource instance)",
    )

    framework: Optional[str] = Field(
        default=None,
        description="Filter controls by compliance framework, e.g. 'soc2', 'iso27001', 'hipaa', 'pci'. Omit for all.",
    )

    status: Optional[str] = Field(
        default=None,
        description="Filter controls by status, e.g. 'OK', 'NEEDS_ATTENTION', 'DEACTIVATED'. Omit for all.",
    )

    limit: int = Field(
        default=1000,
        description="Maximum number of controls to fetch (safety cap on pagination).",
    )

    page_size: int = Field(
        default=100,
        description="Per-page size for the paginated GET /v1/controls call.",
    )

    api_path: str = Field(
        default="/v1/controls",
        description="Vanta API endpoint path. Override for API version bumps.",
    )

    # --- Standard asset metadata --------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="vanta", description="Asset group for organization"
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:security', 'compliance@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Default: ['vanta', 'rest'].",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy.",
    )

    include_preview_metadata: bool = Field(
        default=True, description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows to include in the preview metadata.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        resource_name = self.resource_name
        description = self.description or (
            f"Vanta compliance controls"
            + (f" ({self.framework})" if self.framework else "")
        )

        _kinds = list(self.kinds or []) or ["vanta", "rest"]
        _all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            _all_tags[f"dagster/kind/{k}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            required_resource_keys={resource_name},
        )
        def vanta_controls_ingestion_asset(context: AssetExecutionContext):
            vanta = getattr(context.resources, resource_name)
            session = vanta.get_client()
            base_url = vanta.api_base_url.rstrip("/")
            url = f"{base_url}{component.api_path}"

            params: Dict[str, Any] = {"pageSize": component.page_size}
            if component.framework:
                params["framework"] = component.framework
            if component.status:
                params["status"] = component.status

            all_records: List[Dict[str, Any]] = []
            pages = 0
            cursor: Optional[str] = None

            while True:
                page_params = dict(params)
                if cursor:
                    page_params["pageCursor"] = cursor

                context.log.info(
                    f"Vanta GET {url} page={pages + 1} cursor={'yes' if cursor else 'no'}"
                )
                resp = session.get(url, params=page_params, timeout=vanta.timeout_seconds)
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"Vanta API returned HTTP {resp.status_code}: {resp.text[:500]}"
                    )
                body = resp.json()

                # Vanta responses commonly nest records under `results` or `data`.
                page_records = (
                    body.get("results")
                    or body.get("data")
                    or (body if isinstance(body, list) else [])
                )
                if not isinstance(page_records, list):
                    page_records = [page_records]
                all_records.extend(page_records)
                pages += 1

                if len(all_records) >= component.limit:
                    all_records = all_records[: component.limit]
                    break

                # Cursor lives at top-level under a few names depending on API version.
                cursor = (
                    body.get("pageCursor")
                    or body.get("nextPageCursor")
                    or body.get("next_cursor")
                )
                if not cursor:
                    break

            context.log.info(
                f"Fetched {len(all_records)} controls across {pages} page(s) from Vanta"
            )

            df = pd.json_normalize(all_records, sep="_") if all_records else pd.DataFrame()

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "pages_fetched": MetadataValue.int(pages),
                "api_url": MetadataValue.text(url),
            }
            if component.framework:
                metadata["framework"] = MetadataValue.text(component.framework)
            if component.status:
                metadata["status_filter"] = MetadataValue.text(component.status)
            if component.include_preview_metadata and len(df) > 0:
                sample = (
                    df.sample(min(component.preview_rows, len(df)))
                    if len(df) > component.preview_rows * 10
                    else df.head(component.preview_rows)
                )
                metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))

            return Output(value=df, metadata=metadata)

        return Definitions(assets=[vanta_controls_ingestion_asset])
