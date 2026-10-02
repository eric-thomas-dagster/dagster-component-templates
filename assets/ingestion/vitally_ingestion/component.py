"""Vitally Accounts Ingestion Component.

Fetches Accounts (via `GET /resources/accounts`) and materializes the result
as a pandas DataFrame. Uses the `vitally_resource` component for
authentication.

Vitally's Accounts endpoint uses cursor-based pagination: each response
carries a `next` cursor which is passed back as the `from` query param until
either the cursor is empty, a short page is returned, or `limit` is reached.

NOTE: Vitally's Accounts list endpoint has no server-side "modified between"
filter -- there is a `sortBy` (createdAt/updatedAt) but no date-range query
param. This component still exposes the standard partition_type/* fields
(for structural consistency with every other ingestion component in this
repo, and so a time-based partition can still be used to *tag* each run's
metadata / drive scheduling), but the partition window is NOT sent to
Vitally as a filter -- every run re-fetches the full account list (subject
to `status`/`limit`). If you need true incremental sync, track Vitally's
`updatedAt` field downstream instead.
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


class VitallyAccountsIngestionComponent(Component, Model, Resolvable):
    """Ingest Vitally Accounts (with health/NPS data) as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.VitallyAccountsIngestionComponent
        attributes:
          asset_name: vitally_accounts
          resource_name: vitally_resource
          status: active
          limit: 2000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="vitally_resource",
        description="Key of the VitallyResource this asset depends on for authentication",
    )

    status: str = Field(
        default="active",
        description="Account state filter: 'active' (default), 'churned', or 'activeOrChurned'",
    )

    sort_by: str = Field(
        default="updatedAt",
        description="Sort field for pagination ordering: 'createdAt' or 'updatedAt'",
    )

    limit: int = Field(
        default=1000,
        description="Maximum number of accounts to fetch across all pages",
    )

    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Accounts requested per page (Vitally caps this at 100)",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. Vitally's Accounts endpoint has no server-side date filter, so a time-based partition only tags run metadata -- it does not narrow the fetched accounts.",
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
        default="vitally",
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
        description="Asset kinds for the Dagster catalog. Defaults to ['vitally', 'python'].",
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
        status = self.status
        sort_by = self.sort_by
        limit = self.limit
        page_size = min(self.page_size, 100)
        description = self.description or "Vitally accounts with health/NPS data"
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

        _kinds = list(self.kinds or ["vitally", "python"])
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
        def vitally_accounts_ingestion_asset(context: AssetExecutionContext):
            _partition_window = None
            if context.has_partition_key:
                try:
                    _window = context.partition_time_window
                    _partition_window = (
                        _window.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        _window.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            client = getattr(context.resources, resource_name)
            context.log.info(f"Fetching Vitally accounts: status={status}, limit={limit}")

            accounts: List[Dict[str, Any]] = []
            cursor: Optional[str] = None
            while len(accounts) < limit:
                page_request_size = min(page_size, limit - len(accounts))
                params: Dict[str, Any] = {
                    "status": status,
                    "sortBy": sort_by,
                    "limit": page_request_size,
                }
                if cursor:
                    params["from"] = cursor
                resp = client.get("resources/accounts", params=params)
                page = resp.get("results", []) or []
                accounts.extend(page)
                cursor = resp.get("next")
                if not cursor or not page:
                    break

            accounts = accounts[:limit]
            context.log.info(f"Fetched {len(accounts)} Vitally accounts")

            if not accounts:
                return Output(
                    value=pd.DataFrame(),
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "status": MetadataValue.text(status),
                    },
                )

            df = pd.DataFrame(accounts)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "status": MetadataValue.text(status),
                "sort_by": MetadataValue.text(sort_by),
            }
            if _partition_window:
                metadata["partition_window_start"] = MetadataValue.text(_partition_window[0])
                metadata["partition_window_end"] = MetadataValue.text(_partition_window[1])
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[vitally_accounts_ingestion_asset])
