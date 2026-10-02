"""Drift Conversations Ingestion Component.

Fetches Drift conversations (via `GET /conversations/list`) and optionally
each conversation's messages (via `GET /conversations/{id}/messages`), then
materializes the result as a pandas DataFrame. Uses the `drift_resource`
component for authentication.

The conversations list endpoint is paginated via a `page_token` param (taken
from the previous response's `pagination.next` field) -- this component walks
it until exhausted or `limit` is reached, whichever comes first.

Drift note: `/conversations/list` has no documented date-range filter param
(only `statusId`). When `from_date_time` / `to_date_time` are set, they are
applied as a client-side filter on each conversation's `createdAt` epoch-ms
timestamp after fetching -- the API itself is not asked to narrow the window.
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


class DriftConversationsIngestionComponent(Component, Model, Resolvable):
    """Ingest Drift conversations (and optionally their messages) as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.DriftConversationsIngestionComponent
        attributes:
          asset_name: drift_conversations
          resource_name: drift_resource
          from_date_time: "2026-06-01T00:00:00Z"
          to_date_time: "2026-07-01T00:00:00Z"
          include_messages: true
          limit: 200
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="drift_resource",
        description="Key of the DriftResource this asset depends on for authentication",
    )

    from_date_time: Optional[str] = Field(
        default=None,
        description=(
            "Start of the conversation window (ISO-8601, e.g. '2026-06-01T00:00:00Z'). Drift's "
            "/conversations/list endpoint has no server-side date filter, so this is applied as "
            "a client-side filter on each conversation's createdAt timestamp after fetching."
        ),
    )

    to_date_time: Optional[str] = Field(
        default=None,
        description=(
            "End of the conversation window (ISO-8601, e.g. '2026-07-01T00:00:00Z'). Applied "
            "client-side on createdAt, same caveat as from_date_time."
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

    status_id: Optional[int] = Field(
        default=None,
        description="Filter by Drift conversation status: 1=OPEN, 2=CLOSED, 3=PENDING. Omit for all statuses.",
    )

    include_messages: bool = Field(
        default=False,
        description="If true, fetch each conversation's messages via GET /conversations/{id}/messages and merge a flattened transcript into the DataFrame",
    )

    limit: int = Field(
        default=100,
        description="Maximum number of conversations to fetch across all pages",
    )

    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Conversations requested per page (Drift caps this at 100)",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="drift",
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
        description="Asset kinds for the Dagster catalog. Defaults to ['drift', 'python'].",
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
        status_id = self.status_id
        include_messages = self.include_messages
        limit = self.limit
        page_size = min(self.page_size, 100)
        description = self.description or (
            f"Drift conversations between {from_dt} and {to_dt}" if from_dt and to_dt
            else "Drift conversations for the materialized partition"
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

        _kinds = list(self.kinds or ["drift", "python"])
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
        def drift_conversations_ingestion_asset(context: AssetExecutionContext):
            _from_dt, _to_dt = from_dt, to_dt
            if context.has_partition_key:
                # A time-based partition means this run should fetch exactly that
                # slice, not the static from_date_time/to_date_time config.
                try:
                    _window = context.partition_time_window
                    _from_dt = _window.start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    _to_dt = _window.end.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            resource = getattr(context.resources, resource_name)
            context.log.info(
                f"Fetching Drift conversations: limit={limit}"
                + (f", status_id={status_id}" if status_id is not None else "")
                + (f", window={_from_dt} -> {_to_dt} (client-side filter)" if _from_dt or _to_dt else "")
            )

            # --- 1. List conversations (page_token-paginated) -------------------
            conversations: List[Dict[str, Any]] = []
            params: Dict[str, Any] = {"limit": page_size}
            if status_id is not None:
                params["statusId"] = status_id
            while len(conversations) < limit:
                body = resource.get("conversations/list", params=params)
                page = body.get("data", [])
                conversations.extend(page)
                next_token = (body.get("pagination") or {}).get("next")
                if not next_token or not page:
                    break
                params["page_token"] = next_token

            conversations = conversations[:limit]
            context.log.info(f"Fetched {len(conversations)} conversation records")

            # --- 2. Client-side date-window filter ------------------------------
            # Drift's list endpoint has no server-side date filter param, so we
            # can only narrow the window after the fact. createdAt is epoch ms.
            if (_from_dt or _to_dt) and conversations:
                import datetime

                def _to_epoch_ms(iso_str: str) -> float:
                    dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=datetime.timezone.utc
                    )
                    return dt.timestamp() * 1000

                from_ms = _to_epoch_ms(_from_dt) if _from_dt else None
                to_ms = _to_epoch_ms(_to_dt) if _to_dt else None

                def _in_window(c: Dict[str, Any]) -> bool:
                    created = c.get("createdAt")
                    if created is None:
                        return True
                    if from_ms is not None and created < from_ms:
                        return False
                    if to_ms is not None and created >= to_ms:
                        return False
                    return True

                before = len(conversations)
                conversations = [c for c in conversations if _in_window(c)]
                context.log.info(
                    f"Client-side date filter kept {len(conversations)}/{before} conversations"
                )

            if not conversations:
                empty_df = pd.DataFrame()
                metadata: Dict[str, Any] = {"row_count": MetadataValue.int(0)}
                if _from_dt:
                    metadata["from_date_time"] = MetadataValue.text(_from_dt)
                if _to_dt:
                    metadata["to_date_time"] = MetadataValue.text(_to_dt)
                return Output(value=empty_df, metadata=metadata)

            df = pd.DataFrame(conversations)

            # --- 3. Per-conversation message fetch -------------------------------
            if include_messages:
                def _flatten(messages):
                    if not messages:
                        return ""
                    lines = []
                    for m in messages:
                        author = m.get("author") or m.get("authorId") or ""
                        text = m.get("body") or m.get("message") or ""
                        if text:
                            lines.append(f"{author}: {text}" if author else text)
                    return "\n".join(lines)

                transcripts_raw: Dict[Any, Any] = {}
                for conv_id in df["id"]:
                    all_messages: List[Dict[str, Any]] = []
                    msg_params: Dict[str, Any] = {}
                    # Safety cap on pages fetched per conversation so a pathological
                    # conversation can't make this loop run forever.
                    for _ in range(20):
                        mbody = resource.get(f"conversations/{conv_id}/messages", params=msg_params)
                        page_messages = mbody.get("messages", [])
                        all_messages.extend(page_messages)
                        next_cursor = (mbody.get("pagination") or {}).get("next")
                        if not next_cursor or not page_messages:
                            break
                        msg_params["next"] = next_cursor
                    transcripts_raw[conv_id] = all_messages

                df["messages_raw"] = df["id"].map(lambda i: transcripts_raw.get(i, []))
                df["transcript"] = df["messages_raw"].apply(_flatten)
                context.log.info(f"Merged messages for {len(transcripts_raw)} conversations")

            metadata = {
                "row_count": MetadataValue.int(len(df)),
                "include_messages": MetadataValue.bool(include_messages),
            }
            if _from_dt:
                metadata["from_date_time"] = MetadataValue.text(_from_dt)
            if _to_dt:
                metadata["to_date_time"] = MetadataValue.text(_to_dt)
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[drift_conversations_ingestion_asset])
