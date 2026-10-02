"""Metronome Invoices Ingestion Component.

Pull-side sibling of `metronome_usage_event_send`: fetches customers and
their invoices from Metronome's billing API for finance reporting, and
materializes the result as a pandas DataFrame (one row per invoice).

Verified against docs.metronome.com (2026-10):
  - `GET /v1/customers` -- paginated list of customers. Response shape
    `{"data": [...], "next_page": "<cursor-or-null>"}`; paginate by
    following `next_page` until it's null/absent.
  - `GET /v1/customers/{customer_id}/invoices` -- paginated list of a
    single customer's invoices (same `{"data": [...], "next_page": ...}`
    envelope). Supports `status`, `starting_on`, `ending_before` filters.
  - Invoice objects carry header-level fields (id, status, total, subtotal,
    issued_at, start_timestamp, end_timestamp, contract_id, customer_id)
    plus a `line_items` array of per-line detail (name, type, quantity,
    unit_price, total, product_id, ...).

Uses the ``metronome_resource`` component for authentication (static
Bearer API key).
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


def _list_customers_page(client, base_url: str, limit: int, next_page: Optional[str]) -> Dict[str, Any]:
    """Isolates the real external-API boundary for `GET /customers` so it
    can be monkeypatched wholesale in tests. One page per call -- the
    component's own loop follows `next_page` until exhausted."""
    params: Dict[str, Any] = {"limit": limit}
    if next_page:
        params["next_page"] = next_page
    resp = client.get(f"{base_url}/customers", params=params)
    resp.raise_for_status()
    return resp.json()


def _list_invoices_page(
    client,
    base_url: str,
    customer_id: str,
    limit: int,
    next_page: Optional[str],
    status: Optional[str],
    starting_on: Optional[str],
    ending_before: Optional[str],
) -> Dict[str, Any]:
    """Isolates the real external-API boundary for
    `GET /customers/{customer_id}/invoices` so it can be monkeypatched
    wholesale in tests. One page per call -- the component's own loop
    follows `next_page` until exhausted."""
    params: Dict[str, Any] = {"limit": limit}
    if next_page:
        params["next_page"] = next_page
    if status:
        params["status"] = status
    if starting_on:
        params["starting_on"] = starting_on
    if ending_before:
        params["ending_before"] = ending_before
    resp = client.get(f"{base_url}/customers/{customer_id}/invoices", params=params)
    resp.raise_for_status()
    return resp.json()


class MetronomeInvoicesIngestionComponent(Component, Model, Resolvable):
    """Ingest Metronome customers + invoices as a pandas DataFrame (one row
    per invoice) for finance reporting.

    Example:

        ```yaml
        type: dagster_component_templates.MetronomeInvoicesIngestionComponent
        attributes:
          asset_name: metronome_invoices
          resource_name: metronome_resource
          invoice_status: FINALIZED
          starting_on: "2026-01-01T00:00:00.000Z"
          ending_before: "2026-07-01T00:00:00.000Z"
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="metronome_resource",
        description="Key of the MetronomeResource this asset depends on for authentication",
    )

    customer_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "Specific Metronome customer IDs to fetch invoices for. If unset, "
            "all customers are listed via GET /customers and invoices are "
            "fetched for each."
        ),
    )

    invoice_status: Optional[str] = Field(
        default=None,
        description="Filter invoices by status (e.g. 'DRAFT', 'FINALIZED', 'VOID'). Unset fetches all statuses.",
    )
    starting_on: Optional[str] = Field(
        default=None,
        description="Only include invoices starting on/after this ISO-8601 timestamp.",
    )
    ending_before: Optional[str] = Field(
        default=None,
        description="Only include invoices ending before this ISO-8601 timestamp.",
    )

    customers_page_limit: int = Field(
        default=100,
        description="Page size for the GET /customers listing (only used when customer_ids is unset).",
    )
    invoices_page_limit: int = Field(
        default=100,
        description="Page size for the GET /customers/{id}/invoices listing, per customer.",
    )
    max_customers: Optional[int] = Field(
        default=None,
        description=(
            "Safety cap on the number of distinct customers to fetch invoices "
            "for in a single run (only applies when customer_ids is unset and "
            "the account has many customers)."
        ),
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="metronome",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:finance', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['metronome', 'python'].",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata",
    )

    preview_rows: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Rows to include in the preview metadata",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime)",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_name = self.resource_name
        customer_ids = self.customer_ids
        invoice_status = self.invoice_status
        starting_on = self.starting_on
        ending_before = self.ending_before
        customers_page_limit = self.customers_page_limit
        invoices_page_limit = self.invoices_page_limit
        max_customers = self.max_customers
        description = self.description or "Metronome customer invoices for finance reporting"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        _kinds = list(self.kinds or ["metronome", "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            required_resource_keys={resource_name},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def metronome_invoices_ingestion_asset(context: AssetExecutionContext):
            resource = getattr(context.resources, resource_name)
            client = resource.get_client()
            base_url = getattr(resource, "base_url", None) or "https://api.metronome.com/v1"

            # --- 1. Resolve the set of customer IDs to pull invoices for ----
            if customer_ids:
                resolved_customer_ids = list(customer_ids)
                context.log.info(f"Using {len(resolved_customer_ids)} explicitly configured customer_ids")
            else:
                resolved_customer_ids = []
                next_page = None
                while True:
                    page = _list_customers_page(client, base_url, customers_page_limit, next_page)
                    for c in page.get("data", []):
                        cid = c.get("id")
                        if cid:
                            resolved_customer_ids.append(cid)
                    next_page = page.get("next_page")
                    if not next_page:
                        break
                    if max_customers and len(resolved_customer_ids) >= max_customers:
                        break
                if max_customers:
                    resolved_customer_ids = resolved_customer_ids[:max_customers]
                context.log.info(f"Listed {len(resolved_customer_ids)} customers from Metronome")

            # --- 2. Fetch invoices per customer, paginated -------------------
            invoices: List[Dict[str, Any]] = []
            for cid in resolved_customer_ids:
                next_page = None
                while True:
                    page = _list_invoices_page(
                        client, base_url, cid, invoices_page_limit, next_page,
                        invoice_status, starting_on, ending_before,
                    )
                    for inv in page.get("data", []):
                        inv = dict(inv)
                        inv.setdefault("customer_id", cid)
                        invoices.append(inv)
                    next_page = page.get("next_page")
                    if not next_page:
                        break

            context.log.info(f"Fetched {len(invoices)} invoices across {len(resolved_customer_ids)} customers")

            if not invoices:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "customer_count": MetadataValue.int(len(resolved_customer_ids)),
                    },
                )

            df = pd.DataFrame(invoices)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "customer_count": MetadataValue.int(len(resolved_customer_ids)),
            }
            if invoice_status:
                metadata["invoice_status_filter"] = MetadataValue.text(invoice_status)
            if "total" in df.columns:
                try:
                    metadata["total_amount_sum"] = MetadataValue.float(float(pd.to_numeric(df["total"], errors="coerce").sum()))
                except Exception:  # noqa: BLE001
                    pass
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[metronome_invoices_ingestion_asset])
