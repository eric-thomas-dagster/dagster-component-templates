"""Kustomer Conversations Ingestion Component.

Fetches Kustomer conversations and materializes the result as a pandas
DataFrame. Uses the `kustomer_resource` component for authentication.

Kustomer note: there is no single well-documented "list all conversations"
GET endpoint with a documented date-range filter. The best-documented way to
bulk list/filter Conversations is the **Search API**
(`POST /v1/customers/search` with `"queryContext": "conversation"`), which
returns a JSON:API-ish shape: a top-level `data` array of
`{id, type, attributes, relationships}` objects. This component flattens
each object's `attributes` (plus its `id`) into a DataFrame row.

Pagination on the search endpoint is integer `page` / `pageSize` (not a
cursor) -- this component walks `page` upward until a page comes back empty,
`limit` is reached, or `max_pages` is hit, whichever comes first.
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


class KustomerConversationsIngestionComponent(Component, Model, Resolvable):
    """Ingest Kustomer conversations (via the Search API) as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.KustomerConversationsIngestionComponent
        attributes:
          asset_name: kustomer_conversations
          resource_name: kustomer_resource
          from_date_time: "2026-06-01T00:00:00.000Z"
          to_date_time: "2026-07-01T00:00:00.000Z"
          limit: 500
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="kustomer_resource",
        description="Key of the KustomerResource this asset depends on for authentication",
    )

    from_date_time: Optional[str] = Field(
        default=None,
        description=(
            "Start of the conversation window (ISO-8601). Passed as a server-side "
            "conversation_created_at >= filter on the Search API."
        ),
    )

    to_date_time: Optional[str] = Field(
        default=None,
        description=(
            "End of the conversation window (ISO-8601). Passed as a server-side "
            "conversation_created_at < filter on the Search API."
        ),
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, from_date_time/to_date_time are derived from the partition window instead of the static config.",
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

    limit: int = Field(
        default=100,
        description="Maximum number of conversations to fetch across all pages",
    )

    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Conversations requested per page (Kustomer's documented max page size)",
    )

    max_pages: int = Field(
        default=100,
        ge=1,
        description="Safety cap on the number of search pages walked, regardless of limit",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="kustomer",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:support', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['kustomer', 'python'].",
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
        from_dt = self.from_date_time
        to_dt = self.to_date_time
        limit = self.limit
        page_size = min(self.page_size, 100)
        max_pages = self.max_pages
        description = self.description or (
            f"Kustomer conversations between {from_dt} and {to_dt}" if from_dt and to_dt
            else "Kustomer conversations for the materialized partition"
        )
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

        _kinds = list(self.kinds or ["kustomer", "python"])
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
        def kustomer_conversations_ingestion_asset(context: AssetExecutionContext):
            _from_dt, _to_dt = from_dt, to_dt
            if context.has_partition_key:
                # A time-based partition means this run should fetch exactly that
                # slice, not the static from_date_time/to_date_time config.
                try:
                    _window = context.partition_time_window
                    _from_dt = _window.start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    _to_dt = _window.end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            resource = getattr(context.resources, resource_name)
            context.log.info(
                f"Fetching Kustomer conversations: limit={limit}"
                + (f", window={_from_dt} -> {_to_dt}" if _from_dt or _to_dt else "")
            )

            # --- Build the search filter -----------------------------------------
            # queryContext=conversation means every filter field name must be
            # prefixed with "conversation_" per Kustomer's documented search
            # semantics (e.g. conversation_created_at instead of createdAt).
            and_filters: List[Dict[str, Any]] = []
            if _from_dt or _to_dt:
                range_filter: Dict[str, Any] = {}
                if _from_dt:
                    range_filter["gte"] = _from_dt
                if _to_dt:
                    range_filter["lt"] = _to_dt
                and_filters.append({"conversation_created_at": range_filter})

            # --- Walk pages (integer page/pageSize, not a cursor) ----------------
            records: List[Dict[str, Any]] = []
            page = 1
            while len(records) < limit and page <= max_pages:
                body = {
                    "queryContext": "conversation",
                    "page": page,
                    "pageSize": page_size,
                    "sort": [{"conversation_created_at": "desc"}],
                }
                if and_filters:
                    body["and"] = and_filters
                resp = resource.post("v1/customers/search", json=body)
                page_data = resp.get("data", [])
                if not page_data:
                    break
                for obj in page_data:
                    row = dict(obj.get("attributes") or {})
                    row["id"] = obj.get("id")
                    row["type"] = obj.get("type")
                    records.append(row)
                page += 1

            records = records[:limit]
            context.log.info(f"Fetched {len(records)} conversation records across {page - 1} page(s)")

            if not records:
                empty_df = pd.DataFrame()
                metadata: Dict[str, Any] = {"row_count": MetadataValue.int(0)}
                if _from_dt:
                    metadata["from_date_time"] = MetadataValue.text(_from_dt)
                if _to_dt:
                    metadata["to_date_time"] = MetadataValue.text(_to_dt)
                return Output(value=empty_df, metadata=metadata)

            df = pd.DataFrame(records)

            metadata = {"row_count": MetadataValue.int(len(df))}
            if _from_dt:
                metadata["from_date_time"] = MetadataValue.text(_from_dt)
            if _to_dt:
                metadata["to_date_time"] = MetadataValue.text(_to_dt)
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[kustomer_conversations_ingestion_asset])
