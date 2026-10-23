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

from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    BackfillPolicy,
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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class KustomerConversationsIngestionComponent(Component, Model, Resolvable):
    """Ingest Kustomer conversations (via the Search API) as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.KustomerConversationsIngestionComponent
        attributes:
          asset_name: kustomer_conversations
          resource_key: kustomer_resource
          from_date_time: "2026-06-01T00:00:00.000Z"
          to_date_time: "2026-07-01T00:00:00.000Z"
          limit: 500
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_key: str = Field(
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

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the date-window "
            "filter below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
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

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_key = self.resource_key
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
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
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
            required_resource_keys={resource_key},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
        )
        def kustomer_conversations_ingestion_asset(context: AssetExecutionContext):
            _from_dt, _to_dt = from_dt, to_dt
            if context.has_partition_key or context.has_partition_key_range:
                # A time-based partition means this run should fetch exactly that
                # slice, not the static from_date_time/to_date_time config.
                try:
                    _window = context.partition_time_window
                    _from_dt = _window.start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    _to_dt = _window.end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            resource = getattr(context.resources, resource_key)
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
