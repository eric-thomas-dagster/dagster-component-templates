"""Outreach Prospects Ingestion Component.

Fetches Outreach Prospects (via `GET /api/v2/prospects`) and materializes the
result as a pandas DataFrame. Uses the `outreach_resource` component for
authentication.

Outreach's API is JSON:API-shaped: each record is a `{id, type, attributes,
relationships}` object. This component flattens `data[].attributes` (plus
`id`/`type`) into DataFrame columns rather than leaving them nested.

Pagination is cursor-based: the response's `links.next` is a fully-formed URL
(carrying its own `page[cursor]` querystring) -- this component simply keeps
following it until it's absent or `limit` is reached, rather than
constructing page params itself.
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


def _flatten_json_api(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten a JSON:API `data` array into a flat pandas DataFrame.

    Each record looks like::

        {"id": "123", "type": "prospect", "attributes": {"firstName": "Jane", ...}, "relationships": {...}}

    `id` and `type` are kept as columns; `attributes` is spread into
    top-level columns. `relationships` is dropped -- this component only
    ingests Prospect fields, not related-record linkage.
    """
    rows = []
    for item in records:
        row: Dict[str, Any] = {"id": item.get("id"), "type": item.get("type")}
        row.update(item.get("attributes") or {})
        rows.append(row)
    return pd.DataFrame(rows)


class OutreachProspectsIngestionComponent(Component, Model, Resolvable):
    """Ingest Outreach Prospects as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.OutreachProspectsIngestionComponent
        attributes:
          asset_name: outreach_prospects
          resource_name: outreach_resource
          updated_after: "2026-06-01T00:00:00Z"
          updated_before: "2026-07-01T00:00:00Z"
          limit: 1000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="outreach_resource",
        description="Key of the OutreachResource this asset depends on for authentication",
    )

    updated_after: Optional[str] = Field(
        default=None,
        description="Only fetch prospects updated at/after this ISO-8601 timestamp (e.g. '2026-06-01T00:00:00Z'). Combined with a time-based partition_type, the partition window overrides this.",
    )

    updated_before: Optional[str] = Field(
        default=None,
        description="Only fetch prospects updated before this ISO-8601 timestamp.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, updated_after/updated_before are derived from the partition window instead of the static config.",
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

    page_size: int = Field(
        default=1000,
        ge=1,
        le=1000,
        description="Records per page (page[size]). Outreach caps this at 1000.",
    )

    limit: int = Field(
        default=10000,
        description="Maximum number of prospects to fetch across all pages",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="outreach",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:sdr', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['outreach', 'python'].",
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
        updated_after = self.updated_after
        updated_before = self.updated_before
        page_size = self.page_size
        limit = self.limit
        description = self.description or "Outreach prospects for the materialized partition"
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

        _kinds = list(self.kinds or ["outreach", "python"])
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
        def outreach_prospects_ingestion_asset(context: AssetExecutionContext):
            _updated_after, _updated_before = updated_after, updated_before
            if context.has_partition_key:
                # A time-based partition means this run should fetch exactly
                # that slice, not the static updated_after/updated_before config.
                try:
                    _window = context.partition_time_window
                    _updated_after = _window.start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    _updated_before = _window.end.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            resource = getattr(context.resources, resource_name)
            context.log.info(
                f"Fetching Outreach prospects: updated {_updated_after or '-inf'} → {_updated_before or '+inf'}, limit={limit}"
            )

            params: Dict[str, Any] = {"page[size]": page_size, "count": "false"}
            if _updated_after and _updated_before:
                params["filter[updatedAt][gt]"] = _updated_after
                params["filter[updatedAt][lt]"] = _updated_before
            elif _updated_after:
                params["filter[updatedAt][gt]"] = _updated_after
            elif _updated_before:
                params["filter[updatedAt][lt]"] = _updated_before

            prospects: List[Dict[str, Any]] = []
            next_url: Optional[str] = None
            while len(prospects) < limit:
                if next_url:
                    body = resource.get(next_url)
                else:
                    body = resource.get("prospects", params=params)
                page = body.get("data", [])
                prospects.extend(page)
                next_url = (body.get("links") or {}).get("next")
                if not next_url or not page:
                    break

            prospects = prospects[:limit]
            context.log.info(f"Fetched {len(prospects)} prospects")

            if not prospects:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "updated_after": MetadataValue.text(_updated_after or ""),
                        "updated_before": MetadataValue.text(_updated_before or ""),
                    },
                )

            df = _flatten_json_api(prospects)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "updated_after": MetadataValue.text(_updated_after or ""),
                "updated_before": MetadataValue.text(_updated_before or ""),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[outreach_prospects_ingestion_asset])
