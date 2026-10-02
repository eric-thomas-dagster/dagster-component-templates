"""Monday.com Board Items Ingestion Component.

Fetches Items from a given monday.com board via the GraphQL API
(`boards(ids: [...]) { items_page { ... } }`) and materializes the result as
a pandas DataFrame. Uses the ``monday_resource`` component for
authentication.

monday.com's API is GraphQL-only -- there is no REST path-based pagination.
The first page comes from `boards(ids: [$boardId]) { items_page(limit: ...) }`;
subsequent pages come from the *separate* top-level `next_items_page(cursor:
..., limit: ...)` field, not a repeat of the `boards` query. This component
walks that cursor until it comes back empty/null or `limit` is reached,
whichever comes first.
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


def _column_values_selection(column_ids: Optional[List[str]]) -> str:
    """Build the `column_values { ... }` GraphQL sub-selection, optionally
    filtered to a specific set of column ids via the `ids` argument."""
    if column_ids:
        ids_literal = ", ".join(f'"{c}"' for c in column_ids)
        return f"column_values(ids: [{ids_literal}]) {{ id text value type }}"
    return "column_values { id text value type }"


def _build_items_fields(column_ids: Optional[List[str]]) -> str:
    return f"""
        id
        name
        state
        created_at
        updated_at
        group {{ id title }}
        {_column_values_selection(column_ids)}
    """


def _build_initial_query(column_ids: Optional[List[str]]) -> str:
    return f"""
    query ($boardId: ID!, $limit: Int!) {{
      boards(ids: [$boardId]) {{
        items_page(limit: $limit) {{
          cursor
          items {{{_build_items_fields(column_ids)}}}
        }}
      }}
    }}
    """


def _build_next_page_query(column_ids: Optional[List[str]]) -> str:
    return f"""
    query ($cursor: String!, $limit: Int!) {{
      next_items_page(cursor: $cursor, limit: $limit) {{
        cursor
        items {{{_build_items_fields(column_ids)}}}
      }}
    }}
    """


def _flatten_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one monday.com Item (with its nested column_values list) into
    a single flat dict suitable for a DataFrame row. Each column becomes two
    columns: `column_<id>` (human-readable `text`) and `column_<id>_raw`
    (the raw JSON-encoded `value`, for columns like dates/numbers/people
    where callers need the structured form)."""
    group = item.get("group") or {}
    row: Dict[str, Any] = {
        "id": item.get("id"),
        "name": item.get("name"),
        "state": item.get("state"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "group_id": group.get("id"),
        "group_title": group.get("title"),
    }
    for cv in item.get("column_values") or []:
        col_id = cv.get("id")
        if not col_id:
            continue
        row[f"column_{col_id}"] = cv.get("text")
        row[f"column_{col_id}_raw"] = cv.get("value")
    return row


class MondayBoardItemsIngestionComponent(Component, Model, Resolvable):
    """Ingest Items from a monday.com board as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.MondayBoardItemsIngestionComponent
        attributes:
          asset_name: monday_board_items
          resource_name: monday_resource
          board_id: "1234567890"
          limit: 2000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="monday_resource",
        description="Key of the MondayResource this asset depends on for authentication",
    )

    board_id: str = Field(description="monday.com board ID to pull items from")

    column_ids: Optional[List[str]] = Field(
        default=None,
        description="Specific column IDs to fetch via column_values(ids: [...]). Fetches all columns if unset.",
    )

    page_size: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Items per items_page/next_items_page call (monday.com max is 500)",
    )

    limit: int = Field(
        default=1000,
        description="Maximum number of items to fetch across all pages",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned.",
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
        default="productivity",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:ops', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['monday', 'python'].",
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
        board_id = self.board_id
        column_ids = list(self.column_ids) if self.column_ids else None
        page_size = self.page_size
        limit = self.limit
        description = self.description or f"monday.com Items for board {board_id}"
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

        _kinds = list(self.kinds or ["monday", "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        initial_query = _build_initial_query(column_ids)
        next_page_query = _build_next_page_query(column_ids)

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
        def monday_board_items_ingestion_asset(context: AssetExecutionContext):
            client = getattr(context.resources, resource_name)
            context.log.info(f"Fetching monday.com items for board {board_id}, limit={limit}")

            # --- Walk items_page -> next_items_page until cursor is exhausted ---
            items: List[Dict[str, Any]] = []
            cursor: Optional[str] = None
            is_first_page = True

            while len(items) < limit:
                remaining = limit - len(items)
                this_page_size = min(page_size, remaining)

                if is_first_page:
                    data = client.execute(
                        initial_query,
                        {"boardId": str(board_id), "limit": this_page_size},
                    )
                    boards = data.get("boards") or []
                    items_page = (boards[0] if boards else {}).get("items_page") or {}
                    is_first_page = False
                else:
                    data = client.execute(
                        next_page_query,
                        {"cursor": cursor, "limit": this_page_size},
                    )
                    items_page = data.get("next_items_page") or {}

                page_items = items_page.get("items") or []
                items.extend(page_items)
                cursor = items_page.get("cursor")
                if not cursor or not page_items:
                    break

            items = items[:limit]
            context.log.info(f"Fetched {len(items)} monday.com items from board {board_id}")

            if not items:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "board_id": MetadataValue.text(str(board_id)),
                    },
                )

            df = pd.DataFrame([_flatten_item(item) for item in items])

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "board_id": MetadataValue.text(str(board_id)),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[monday_board_items_ingestion_asset])
