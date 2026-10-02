"""Sigma Workbook Ingestion Component.

Pulls a catalog of Sigma workbooks + their elements (via `GET /v2/workbooks`
and `GET /v2/workbooks/{workbookId}/elements`) and materializes the result
as a pandas DataFrame, one row per (workbook, element) pair. Uses the
`sigma_resource` component for authentication.

Verified against Sigma's own REST API reference:
  - https://help.sigmacomputing.com/reference/list-workbooks
  - https://help.sigmacomputing.com/reference/list-workbook-elements

Each element row carries `element_type` (Sigma's generic type string for a
workbook element, e.g. `"table"`, `"viz"`, `"input-table"`, ...) and a
derived `is_input_table` boolean -- this is exactly the identification
approach documented in Sigma's own recipe, ["Workbooks: List all Input
Tables"](https://help.sigmacomputing.com/recipes/workbooks-list-all-input-tables-javascript),
which filters `elements[].type === 'input-table'`. Set
`input_tables_only: true` to return just the Input Table catalog (e.g. to
feed a `sigma_input_table_upsert` target-discovery step).

Usage/audit data gotcha -- verified, not guessed: Sigma's REST API does
**not** expose per-row audit history (who changed which cell, when) --
that's a UI-only feature (["View input table audit
history"](https://help.sigmacomputing.com/docs/view-input-table-audit-history)).
The closest API-exposed proxy is workbook-level `createdBy` / `updatedBy` /
`createdAt` / `updatedAt` / `latestVersion`, which this component surfaces
on every row. Don't mistake these workbook-level fields for row-level
input-table audit trail -- that data isn't reachable via the public API as
of this writing.
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

    NOTE: Sigma's `List workbooks` API documents no updated-at / time-range
    filter param, so a time-based partition_type here only controls *run
    cadence* -- every run still does a full workbook catalog pass. Prefer
    `static` or `dynamic` partition types for anything that should actually
    scope which workbooks get cataloged.
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


def _paginate(resource, path: str, page_limit: int, cap: int) -> List[Dict[str, Any]]:
    """Walk Sigma's `entries` / `nextPage` pagination shape (shared by both
    `GET /v2/workbooks` and `GET /v2/workbooks/{id}/elements`) until
    exhausted or `cap` is reached, whichever comes first."""
    out: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {"limit": page_limit}
    while len(out) < cap:
        body = resource.get(path, params=params)
        entries = body.get("entries") or []
        out.extend(entries)
        next_page = body.get("nextPage")
        if not next_page or not entries:
            break
        params = {"limit": page_limit, "page": next_page}
    return out[:cap]


_WORKBOOK_FIELDS = {
    "workbook_id": "workbookId",
    "workbook_name": "name",
    "workbook_path": "path",
    "workbook_url": "url",
    "owner_id": "ownerId",
    "created_by": "createdBy",
    "updated_by": "updatedBy",
    "created_at": "createdAt",
    "updated_at": "updatedAt",
    "latest_version": "latestVersion",
    "is_archived": "isArchived",
}


def _workbook_base_row(wb: Dict[str, Any]) -> Dict[str, Any]:
    return {out_key: wb.get(in_key) for out_key, in_key in _WORKBOOK_FIELDS.items()}


class SigmaWorkbookIngestionComponent(Component, Model, Resolvable):
    """Ingest a catalog of Sigma workbooks + their elements as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.SigmaWorkbookIngestionComponent
        attributes:
          asset_name: sigma_workbook_catalog
          resource_name: sigma_resource
          workbook_limit: 500
          include_elements: true
          input_tables_only: false
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="sigma_resource",
        description="Key of the SigmaResource this asset depends on for authentication",
    )

    workbook_limit: int = Field(
        default=500,
        description="Maximum number of workbooks to catalog across all pages",
    )

    page_limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Page size for both the workbooks and elements list calls (Sigma caps `limit` at 1,000).",
    )

    include_elements: bool = Field(
        default=True,
        description=(
            "If true (default), fetch each workbook's elements via "
            "`GET /v2/workbooks/{id}/elements` (one extra paginated call per "
            "workbook) and emit one row per (workbook, element). If false, "
            "emit one row per workbook with element fields left null -- "
            "much cheaper for a pure workbook-inventory pass."
        ),
    )

    input_tables_only: bool = Field(
        default=False,
        description=(
            "Only meaningful when include_elements=true. If true, drop every "
            "element row whose type isn't 'input-table' -- and drop "
            "workbooks with zero input tables entirely, rather than emitting "
            "a null placeholder row for them. Mirrors Sigma's own 'Workbooks: "
            "List all Input Tables' recipe, which filters elements[].type === "
            "'input-table'."
        ),
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. Sigma's List Workbooks API has no time-range filter, so a time-based type only controls run cadence, not which workbooks are fetched.",
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
        default="sigma",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics-eng', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['sigma', 'python'].",
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
        workbook_limit = self.workbook_limit
        page_limit = self.page_limit
        include_elements = self.include_elements
        input_tables_only = self.input_tables_only
        description = self.description or "Sigma workbook + element catalog"
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

        _kinds = list(self.kinds or ["sigma", "python"])
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
        def sigma_workbook_ingestion_asset(context: AssetExecutionContext):
            if context.has_partition_key:
                context.log.info(
                    f"Partition key={context.partition_key!r} -- Sigma's List "
                    f"Workbooks API has no time-range filter, so this run still "
                    f"does a full catalog pass (the partition only affects "
                    f"scheduling cadence)."
                )

            resource = getattr(context.resources, resource_name)
            context.log.info(f"Fetching Sigma workbook catalog (limit={workbook_limit})")

            workbooks = _paginate(resource, "v2/workbooks", page_limit, workbook_limit)
            context.log.info(f"Fetched {len(workbooks)} workbooks")

            rows: List[Dict[str, Any]] = []
            elements_fetched = 0
            input_table_count = 0

            for wb in workbooks:
                wb_id = wb.get("workbookId")
                base_row = _workbook_base_row(wb)

                if not include_elements:
                    rows.append({
                        **base_row,
                        "element_id": None,
                        "element_name": None,
                        "element_type": None,
                        "is_input_table": None,
                        "element_columns": None,
                        "visualization_type": None,
                    })
                    continue

                if not wb_id:
                    continue

                elements = _paginate(
                    resource, f"v2/workbooks/{wb_id}/elements", page_limit, cap=10_000
                )
                elements_fetched += len(elements)

                if input_tables_only:
                    elements = [e for e in elements if e.get("type") == "input-table"]
                    if not elements:
                        # Mirrors Sigma's own "List all Input Tables" recipe --
                        # a workbook with zero input tables contributes no rows
                        # at all, rather than a null placeholder.
                        continue

                if not elements:
                    rows.append({
                        **base_row,
                        "element_id": None,
                        "element_name": None,
                        "element_type": None,
                        "is_input_table": None,
                        "element_columns": None,
                        "visualization_type": None,
                    })
                    continue

                for el in elements:
                    is_input_table = el.get("type") == "input-table"
                    if is_input_table:
                        input_table_count += 1
                    rows.append({
                        **base_row,
                        "element_id": el.get("elementId"),
                        "element_name": el.get("name"),
                        "element_type": el.get("type"),
                        "is_input_table": is_input_table,
                        "element_columns": el.get("columns"),
                        "visualization_type": el.get("vizualizationType"),
                    })

            if not rows:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "workbook_count": MetadataValue.int(len(workbooks)),
                    },
                )

            df = pd.DataFrame(rows)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "workbook_count": MetadataValue.int(len(workbooks)),
                "elements_fetched": MetadataValue.int(elements_fetched),
                "input_table_count": MetadataValue.int(input_table_count),
                "include_elements": MetadataValue.bool(include_elements),
                "input_tables_only": MetadataValue.bool(input_tables_only),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[sigma_workbook_ingestion_asset])
