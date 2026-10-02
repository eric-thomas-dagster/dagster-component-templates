"""Gainsight Company Ingestion Component.

Fetches Company records (via `POST /v1/data/objects/query/Company`) and
materializes the result as a pandas DataFrame. Uses the `gainsight_resource`
component for authentication.

Gainsight's Company query endpoint is POST-based (not GET-with-querystring):
the request body describes `select` (fields), `where` (filter), `orderBy`,
and `limit`/`offset`. Pagination walks `offset` by the number of records
actually returned until a short page (fewer than requested) signals the end,
or `limit` is reached -- whichever comes first. Gainsight caps each page at
5000 records.

NOTE: "health score" is not a standard Gainsight field name -- it is almost
always an org-specific custom field configured via Gainsight's Scorecard /
Rules Engine (e.g. `Health_Score__c`). `select_fields` defaults to a common
set of standard Company fields and should be adjusted per-tenant.
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


class GainsightCompanyIngestionComponent(Component, Model, Resolvable):
    """Ingest Gainsight Company records as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.GainsightCompanyIngestionComponent
        attributes:
          asset_name: gainsight_companies
          resource_name: gainsight_resource
          select_fields: ["Gsid", "Name", "Csm", "Status", "Stage", "Health_Score"]
          limit: 2000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="gainsight_resource",
        description="Key of the GainsightResource this asset depends on for authentication",
    )

    object_name: str = Field(
        default="Company",
        description="Gainsight object to query (e.g. 'Company', 'Relationship', or a custom object API name)",
    )

    select_fields: List[str] = Field(
        default_factory=lambda: ["Gsid", "Name", "Csm", "Status", "Stage", "Renewal_Date", "Health_Score"],
        description="Fields to select from the object. Health score / scorecard field names are "
                    "org-specific custom fields in most Gainsight tenants -- confirm your actual "
                    "field name (e.g. 'Health_Score__c') before relying on the default.",
    )

    where_filter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw Gainsight 'where' clause (conditions/expression) merged with any date-window filter",
    )

    date_field: str = Field(
        default="Last_Modified_Date",
        description="Field used for the optional modified-date window filter (static or partition-derived)",
    )

    modified_after: Optional[str] = Field(
        default=None,
        description="Static lower bound (ISO-8601) for date_field. Ignored when a time-based partition_type is set.",
    )

    modified_before: Optional[str] = Field(
        default=None,
        description="Static upper bound (ISO-8601) for date_field. Ignored when a time-based partition_type is set.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, modified_after/modified_before are derived from the partition window instead of the static config.",
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
        default=1000,
        description="Maximum number of records to fetch across all pages",
    )

    page_size: int = Field(
        default=500,
        ge=1,
        le=5000,
        description="Records requested per page (Gainsight caps this at 5000)",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="gainsight",
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
        description="Asset kinds for the Dagster catalog. Defaults to ['gainsight', 'python'].",
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
        object_name = self.object_name
        select_fields = list(self.select_fields)
        base_where = dict(self.where_filter) if self.where_filter else None
        date_field = self.date_field
        modified_after = self.modified_after
        modified_before = self.modified_before
        limit = self.limit
        page_size = min(self.page_size, 5000)
        description = self.description or f"Gainsight {object_name} records"
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

        _kinds = list(self.kinds or ["gainsight", "python"])
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
        def gainsight_company_ingestion_asset(context: AssetExecutionContext):
            _after, _before = modified_after, modified_before
            if context.has_partition_key:
                try:
                    _window = context.partition_time_window
                    _after = _window.start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    _before = _window.end.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            where_conditions = list((base_where or {}).get("conditions", []))
            where_expression = (base_where or {}).get("expression")
            if _after:
                where_conditions.append({"field": date_field, "operator": "GTE", "value": _after})
            if _before:
                where_conditions.append({"field": date_field, "operator": "LTE", "value": _before})
            where_clause = None
            if where_conditions:
                where_clause = {"conditions": where_conditions}
                if where_expression:
                    where_clause["expression"] = where_expression

            client = getattr(context.resources, resource_name)
            context.log.info(
                f"Fetching Gainsight {object_name} records (fields={select_fields}), limit={limit}"
            )

            records: List[Dict[str, Any]] = []
            offset = 0
            while len(records) < limit:
                page_request_size = min(page_size, limit - len(records))
                body: Dict[str, Any] = {
                    "select": select_fields,
                    "limit": page_request_size,
                    "offset": offset,
                }
                if where_clause:
                    body["where"] = where_clause
                resp = client.post(f"v1/data/objects/query/{object_name}", json=body)
                page = resp.get("data", []) or []
                records.extend(page)
                if len(page) < page_request_size:
                    break  # short page -- no more records
                offset += len(page)

            records = records[:limit]
            context.log.info(f"Fetched {len(records)} {object_name} records")

            if not records:
                return Output(
                    value=pd.DataFrame(),
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "object_name": MetadataValue.text(object_name),
                    },
                )

            df = pd.DataFrame(records)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "object_name": MetadataValue.text(object_name),
                "select_fields": MetadataValue.text(", ".join(select_fields)),
            }
            if _after:
                metadata["modified_after"] = MetadataValue.text(_after)
            if _before:
                metadata["modified_before"] = MetadataValue.text(_before)
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[gainsight_company_ingestion_asset])
