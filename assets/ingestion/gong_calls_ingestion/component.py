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

    from_date_time: str = Field(
        description="Start of the call window (ISO-8601, e.g. '2026-06-01T00:00:00Z')",
    )

    to_date_time: str = Field(
        description="End of the call window (ISO-8601, e.g. '2026-07-01T00:00:00Z')",
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
        description = self.description or f"Gong calls between {from_dt} and {to_dt}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

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
        )
        def gong_calls_ingestion_asset(context: AssetExecutionContext):
            client = getattr(context.resources, resource_name).get_client()
            context.log.info(f"Fetching Gong calls: {from_dt} → {to_dt}, limit={limit}")

            # --- 1. List calls (paginated) --------------------------------------
            calls: List[Dict[str, Any]] = []
            params: Dict[str, Any] = {
                "fromDateTime": from_dt,
                "toDateTime": to_dt,
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
                        "from_date_time": MetadataValue.text(from_dt),
                        "to_date_time": MetadataValue.text(to_dt),
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
                "from_date_time": MetadataValue.text(from_dt),
                "to_date_time": MetadataValue.text(to_dt),
                "include_transcript": MetadataValue.bool(include_transcript),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[gong_calls_ingestion_asset])
