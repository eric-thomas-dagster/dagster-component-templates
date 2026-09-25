"""Gong Calls Ingestion Component.

Fetches Gong call metadata (via `GET /v2/calls`) and optionally bulk transcripts
(via `POST /v2/calls/transcript`) and materializes the result as a pandas
DataFrame. Uses the ``gong_resource`` component for authentication.

The endpoint returns paginated results — this component walks the cursor
until exhausted or ``limit`` is reached, whichever comes first.
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


class GongCallsIngestionComponent(Component, Model, Resolvable):
    """Ingest Gong call metadata + transcripts as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.GongCallsIngestionComponent
        attributes:
          asset_name: gong_calls
          resource_name: gong_resource
          from_date_time: "2026-06-01T00:00:00Z"
          to_date_time: "2026-07-01T00:00:00Z"
          include_transcript: true
          limit: 500
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="gong_resource",
        description="Key of the GongResource this asset depends on for authentication",
    )

    from_date_time: Optional[str] = Field(
        default=None,
        description="Start of the call window (ISO-8601, e.g. '2026-06-01T00:00:00Z'). Required unless a time-based partition_type is set.",
    )

    to_date_time: Optional[str] = Field(
        default=None,
        description="End of the call window (ISO-8601, e.g. '2026-07-01T00:00:00Z'). Required unless a time-based partition_type is set.",
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

    include_transcript: bool = Field(
        default=True,
        description="If true, fetch transcripts via POST /v2/calls/transcript and merge into the DataFrame",
    )

    limit: int = Field(
        default=100,
        description="Maximum number of calls to fetch across all pages",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="gong",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:revops', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['gong', 'python'].",
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
        include_transcript = self.include_transcript
        limit = self.limit
        description = self.description or (
            f"Gong calls between {from_dt} and {to_dt}" if from_dt and to_dt
            else "Gong calls for the materialized partition"
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

        _kinds = list(self.kinds or ["gong", "python"])
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
        def gong_calls_ingestion_asset(context: AssetExecutionContext):
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
            if not _from_dt or not _to_dt:
                raise ValueError(
                    "Set from_date_time/to_date_time, or a time-based partition_type, to define the call window."
                )

            client = getattr(context.resources, resource_name).get_client()
            context.log.info(f"Fetching Gong calls: {_from_dt} → {_to_dt}, limit={limit}")

            # --- 1. List calls (paginated) --------------------------------------
            calls: List[Dict[str, Any]] = []
            params: Dict[str, Any] = {
                "fromDateTime": _from_dt,
                "toDateTime": _to_dt,
            }
            while len(calls) < limit:
                resp = client.get("v2/calls", params=params)
                resp.raise_for_status()
                body = resp.json()
                page = body.get("calls", [])
                calls.extend(page)
                cursor = (body.get("records") or {}).get("cursor")
                if not cursor or not page:
                    break
                params["cursor"] = cursor

            calls = calls[:limit]
            context.log.info(f"Fetched {len(calls)} call metadata records")

            if not calls:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "from_date_time": MetadataValue.text(_from_dt),
                        "to_date_time": MetadataValue.text(_to_dt),
                    },
                )

            df = pd.DataFrame(calls)

            # --- 2. Bulk transcript fetch ---------------------------------------
            if include_transcript:
                call_ids = [c["id"] for c in calls if "id" in c]
                transcripts_by_id: Dict[str, Any] = {}

                # POST /v2/calls/transcript accepts up to 100 IDs per request.
                # Loop in chunks so `limit` above 100 still works.
                batch_size = 100
                for i in range(0, len(call_ids), batch_size):
                    batch = call_ids[i : i + batch_size]
                    payload = {"filter": {"callIds": batch}}
                    tresp = client.post("v2/calls/transcript", json=payload)
                    tresp.raise_for_status()
                    tbody = tresp.json()
                    for entry in tbody.get("callTranscripts", []):
                        cid = entry.get("callId")
                        if cid:
                            transcripts_by_id[cid] = entry.get("transcript", [])

                # Flatten each call's transcript into a single string of
                # "SPEAKER: text" joined by newlines. Store the raw structured
                # transcript in a parallel column for consumers who need it.
                def _flatten(t):
                    if not t:
                        return ""
                    lines = []
                    for turn in t:
                        speaker = turn.get("speakerId") or turn.get("speakerName") or ""
                        sentences = turn.get("sentences") or []
                        text = " ".join(s.get("text", "") for s in sentences)
                        if text:
                            lines.append(f"{speaker}: {text}" if speaker else text)
                    return "\n".join(lines)

                df["transcript_raw"] = df["id"].map(lambda i: transcripts_by_id.get(i, []))
                df["transcript"] = df["transcript_raw"].apply(_flatten)
                context.log.info(f"Merged transcripts for {len(transcripts_by_id)} calls")

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "from_date_time": MetadataValue.text(_from_dt),
                "to_date_time": MetadataValue.text(_to_dt),
                "include_transcript": MetadataValue.bool(include_transcript),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[gong_calls_ingestion_asset])
