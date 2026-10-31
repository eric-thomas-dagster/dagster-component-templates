"""Firestore Collection Ingestion component.

Materializable asset that reads a Firestore collection (or subcollection) into
a pandas DataFrame. Uses a `firebase_resource` for authenticated access.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


# Firestore FieldFilter operators.
_VALID_OPS = {"==", "!=", "<", "<=", ">", ">=", "in", "not-in", "array-contains", "array-contains-any"}



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


class FirestoreCollectionIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read a Firestore collection into a pandas DataFrame.

    `collection_path` accepts a top-level collection (`users`) or a
    subcollection path (`orders/{order_id}/items` — segments alternate
    collection/document). `where_clauses` accepts a list of
    `{field, op, value}` dicts; `order_by` accepts `{field, direction}`
    where direction is `"asc"` (default) or `"desc"`.

    The document id lands in a `_id` column.
    """

    asset_name: str = Field(description="Name of the asset to create.")
    resource_key: str = Field(
        default="firebase_resource",
        description="Name of the FirebaseResource this asset uses.",
    )
    collection_path: str = Field(
        description="Firestore collection path, e.g. 'users' or 'orders/{order_id}/items'.",
    )
    where_clauses: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional filters: list of {field, op, value} dicts. "
                    "Ops: ==, !=, <, <=, >, >=, in, not-in, array-contains, array-contains-any.",
    )
    limit: Optional[int] = Field(
        default=None,
        description="Optional maximum number of documents to read.",
    )
    order_by: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional ordering: {field: <name>, direction: 'asc'|'desc'} (default 'asc').",
    )
    group_name: Optional[str] = Field(
        default="firebase",
        description="Asset group for organization.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners (team names or emails).",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags for the asset.",
    )
    include_preview_metadata: bool = Field(
        default=True,
        description="Include a sample preview in the run metadata.",
    )
    preview_rows: int = Field(
        default=25, ge=1, le=500,
        description="Rows to include in the preview.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=(), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        asset_name = self.asset_name
        resource_key = self.resource_key
        collection_path = self.collection_path
        where_clauses = self.where_clauses or []
        limit = self.limit
        order_by = self.order_by
        description = self.description or f"Firestore collection: {collection_path}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Validate where clauses at load time.
        for i, clause in enumerate(where_clauses):
            if not isinstance(clause, dict):
                raise ValueError(f"where_clauses[{i}] must be a dict, got {type(clause).__name__}")
            missing = {"field", "op", "value"} - clause.keys()
            if missing:
                raise ValueError(f"where_clauses[{i}] missing keys: {sorted(missing)}")
            if clause["op"] not in _VALID_OPS:
                raise ValueError(
                    f"where_clauses[{i}].op={clause['op']!r} — must be one of {sorted(_VALID_OPS)}"
                )

        if order_by is not None:
            if "field" not in order_by:
                raise ValueError("order_by requires a 'field' key")
            direction = order_by.get("direction", "asc").lower()
            if direction not in ("asc", "desc"):
                raise ValueError(f"order_by.direction must be 'asc' or 'desc', got {direction!r}")

        tags = dict(self.asset_tags or {})
        tags["dagster/kind/firestore"] = ""

        @dg.asset(partitions_def=partitions_def, 
            key=dg.AssetKey.from_user_string(asset_name),
            description=description,
            group_name=group_name,
            owners=self.owners or [],
            tags=tags,
            required_resource_keys={resource_key},
        )
        def firestore_collection_asset(context: dg.AssetExecutionContext) -> dg.Output:
            from google.cloud.firestore_v1.base_query import FieldFilter  # type: ignore

            fb = getattr(context.resources, resource_key)
            client = fb.get_firestore()

            context.log.info(f"Reading Firestore collection: {collection_path}")

            # Firestore treats a path with an odd number of segments as a
            # collection and even as a document — the client's .collection()
            # accepts a slash-delimited path directly.
            query = client.collection(collection_path)
            for clause in where_clauses:
                query = query.where(filter=FieldFilter(clause["field"], clause["op"], clause["value"]))

            if order_by is not None:
                from google.cloud.firestore_v1 import Query  # type: ignore
                direction = order_by.get("direction", "asc").lower()
                query = query.order_by(
                    order_by["field"],
                    direction=Query.DESCENDING if direction == "desc" else Query.ASCENDING,
                )

            if limit is not None:
                query = query.limit(limit)

            docs = list(query.stream())
            rows = []
            for doc in docs:
                data = doc.to_dict() or {}
                data["_id"] = doc.id
                rows.append(data)

            df = pd.DataFrame(rows)
            df = _filter_df_to_partition(df, context, self.partition_date_column, self.partition_static_column)
            context.log.info(f"Read {len(df)} documents from {collection_path}")

            metadata = {
                "collection_path": dg.MetadataValue.text(collection_path),
                "row_count": dg.MetadataValue.int(len(df)),
                "where_clauses": dg.MetadataValue.json(where_clauses),
            }
            if limit is not None:
                metadata["limit"] = dg.MetadataValue.int(limit)
            if include_preview and len(df) > 0:
                try:
                    prev = df.head(preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")

            return dg.Output(value=df, metadata=metadata)

        return dg.Definitions(assets=[firestore_collection_asset])


# BEGIN reverse-etl partition helpers (inlined per component; keep copies identical, see tests/test_partition_port_reverse_etl.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run or a non-DataFrame input.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the input is returned unchanged."""
    import pandas as pd

    if not hasattr(df, "columns") or not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END reverse-etl partition helpers
