"""Totango Accounts Ingestion Component.

Fetches Account records (via `POST /api/v1/search/accounts`) and
materializes the result as a pandas DataFrame. Uses the `totango_resource`
component for authentication.

Totango's Search API is POST-based: the request body carries `terms`
(filter conditions), `count` (page size, max 1000), `offset` (page number
-- NOT a record offset: page 0 is the first page of `count` records), and
`fields` (which account attributes to return). The response nests hits at
`response.accounts.hits`. Pagination walks `offset` by 1 each page until a
short page is returned or `limit` is reached.
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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
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
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class TotangoAccountsIngestionComponent(Component, Model, Resolvable):
    """Ingest Totango Account records as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.TotangoAccountsIngestionComponent
        attributes:
          asset_name: totango_accounts
          resource_name: totango_resource
          fields: ["name", "display_name", "health", "last_activity_time"]
          limit: 2000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="totango_resource",
        description="Key of the TotangoResource this asset depends on for authentication",
    )

    terms: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Raw Totango Search API 'terms' filter conditions. Combined with any date-window "
                    "term (on date_term_name) derived from a time-based partition.",
    )

    fields: List[str] = Field(
        default_factory=lambda: ["name", "display_name", "health", "last_activity_time", "status"],
        description="Account fields to return. Field names (especially custom attributes like "
                    "health scores) are configured per Totango org -- confirm yours before relying "
                    "on this default.",
    )

    date_term_name: str = Field(
        default="last_updated",
        description="Attribute name used to build the optional modified-date range term (static or partition-derived)",
    )

    modified_after: Optional[str] = Field(
        default=None,
        description="Static lower bound (ISO-8601) for date_term_name. Ignored when a time-based partition_type is set.",
    )

    modified_before: Optional[str] = Field(
        default=None,
        description="Static upper bound (ISO-8601) for date_term_name. Ignored when a time-based partition_type is set.",
    )

    sort_by: Optional[str] = Field(default=None, description="Field to sort results by")
    sort_order: Optional[str] = Field(default=None, description="'asc' or 'desc'")
    scope: Optional[str] = Field(default=None, description="Totango search scope, if applicable to your org")

    limit: int = Field(
        default=5000,
        description="Maximum number of accounts to fetch across all pages",
    )

    count: int = Field(
        default=1000,
        ge=1,
        le=1000,
        description="Accounts requested per page. Totango caps this at 1000.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, the partition window is translated into a range term on date_term_name.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="totango",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:customer-success', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['totango', 'python'].",
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
        base_terms = list(self.terms) if self.terms else []
        fields = list(self.fields)
        date_term_name = self.date_term_name
        modified_after = self.modified_after
        modified_before = self.modified_before
        sort_by = self.sort_by
        sort_order = self.sort_order
        scope = self.scope
        limit = self.limit
        count = min(self.count, 1000)
        description = self.description or "Totango accounts"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        _kinds = list(self.kinds or ["totango", "python"])
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
            partitions_def=partitions_def,
        )
        def totango_accounts_ingestion_asset(context: AssetExecutionContext):
            _after, _before = modified_after, modified_before
            if context.has_partition_key:
                try:
                    _window = context.partition_time_window
                    _after = _window.start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    _before = _window.end.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            terms = list(base_terms)
            if _after or _before:
                range_params: Dict[str, Any] = {}
                if _after:
                    range_params["gte"] = _after
                if _before:
                    range_params["lte"] = _before
                terms.append({"type": "range", "term": date_term_name, "params": range_params})

            client = getattr(context.resources, resource_name)
            context.log.info(f"Fetching Totango accounts: terms={terms}, limit={limit}")

            accounts: List[Dict[str, Any]] = []
            offset = 0
            while len(accounts) < limit:
                body: Dict[str, Any] = {
                    "terms": terms,
                    "count": count,
                    "offset": offset,
                    "fields": fields,
                }
                if sort_by:
                    body["sort_by"] = sort_by
                if sort_order:
                    body["sort_order"] = sort_order
                if scope:
                    body["scope"] = scope
                resp = client.post("api/v1/search/accounts", json=body)
                page = (resp.get("response", {}) or {}).get("accounts", {}).get("hits", []) or []
                accounts.extend(page)
                if len(page) < count:
                    break  # short page -- no more records
                offset += 1

            accounts = accounts[:limit]
            context.log.info(f"Fetched {len(accounts)} Totango accounts")

            if not accounts:
                return Output(
                    value=pd.DataFrame(),
                    metadata={"row_count": MetadataValue.int(0)},
                )

            df = pd.DataFrame(accounts)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "fields": MetadataValue.text(", ".join(fields)),
            }
            if _after:
                metadata["modified_after"] = MetadataValue.text(_after)
            if _before:
                metadata["modified_before"] = MetadataValue.text(_before)
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[totango_accounts_ingestion_asset])
