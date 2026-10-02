"""Zocdoc Appointments Ingestion Component.

Fetches booked appointments for a provider/practice via Zocdoc's
`GET /v1/appointments` endpoint and materializes the result as a pandas
DataFrame, for operational/analytics reporting. Uses the ``zocdoc_resource``
component for authentication.

Verified request/response shape (api-docs.zocdoc.com, 2026):
  - Query params: `page` (0-indexed), `page_size` (1-100), `statuses`
    (comma-delimited), `practice_ids` / `provider_ids` / `location_ids`
    (comma-delimited), `start_time_utc_min`/`start_time_utc_max`,
    `created_time_utc_min`/`created_time_utc_max`,
    `last_modified_time_utc_min`/`last_modified_time_utc_max`,
    `sort_by` (start_time_utc | created_time_utc | last_modified_time_utc),
    `sort_direction` (ascending | descending).
  - Response shape: `{request_id, page, page_size, total_count, next_url,
    data: [...]}`.

================================================================================
PHI SAFETY -- READ BEFORE USE
================================================================================
Zocdoc's appointment records are Protected Health Information: patient
names, contact details, date of birth, visit reasons, and provider notes
can all appear in the raw API response. This component is built so that
**none of that ever lands in Dagster's event log**, which is a
less-trusted surface than Zocdoc itself (broader internal audience,
longer retention, not covered by the same access controls as the source
system):

  1. **No data preview, anywhere, under any configuration.** Unlike most
     DataFrame-producing components in this repo (which optionally render
     a markdown head() of the data into materialization metadata), this
     component has **no `include_preview_metadata` field at all** --
     the capability does not exist in this component's code, so it cannot
     be misconfigured on. Metadata is limited to counts and the *filter
     values the caller already configured* (date window, status/practice/
     provider/location id filters) -- never anything read back from the
     API response body.
  2. **Row counts only.** Materialization metadata exposes `rows_fetched`,
     `pages_fetched`, and `total_count_reported` -- integers only. No
     column names, no sample values, no appointment IDs.
  3. **Sanitized errors.** The one external HTTP call this component makes
     is isolated in `_zocdoc_list_appointments` below. On a non-2xx
     response it raises `ZocdocAPIError`, which carries only the HTTP
     status code and a generic, fixed description string -- never
     `response.text`, `response.json()`, or the request body.
  4. **The DataFrame itself is the asset's materialized *value*, not its
     metadata.** This is unavoidable and by design -- the whole point of
     this component is to deliver appointment data to a warehouse for
     reporting. The safety guarantee here is specifically about Dagster's
     event log / UI (materialization metadata, logs, error messages),
     which is a different, less access-controlled surface than wherever
     the DataFrame itself is persisted downstream.

See the "PHI Safety" section of this component's README for the full
design rationale.
================================================================================

Pairs with:
  - ``zocdoc_resource`` -- OAuth2 client_credentials connection (required)
"""
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Failure,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class ZocdocAPIError(Exception):
    """Raised when a Zocdoc API call fails. Carries ONLY the HTTP status
    code and a fixed, generic description -- never the request or
    response body, which may contain PHI (patient name/email/phone,
    appointment notes, provider schedule detail)."""

    def __init__(self, status_code: Optional[int], message: str):
        self.status_code = status_code
        super().__init__(
            f"{message} (status_code={status_code})" if status_code is not None else message
        )


def _zocdoc_list_appointments(session, base_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Isolates the one external API call this component makes
    (`GET /v1/appointments`) so it can be monkeypatched wholesale in tests.

    On failure, raises `ZocdocAPIError` carrying only the HTTP status code
    -- never `response.text`/`response.json()`, which may contain PHI.
    """
    try:
        resp = session.get(f"{base_url}v1/appointments", params=params, timeout=30)
    except Exception:
        # Deliberately do not interpolate the raw exception string here --
        # some HTTP client errors embed the request (including query
        # params / body) in their __str__.
        raise ZocdocAPIError(None, "Zocdoc appointments request failed (network/connection error)") from None

    if resp.status_code >= 400:
        raise ZocdocAPIError(resp.status_code, "Zocdoc appointments request failed")

    try:
        return resp.json()
    except Exception:
        raise ZocdocAPIError(resp.status_code, "Zocdoc appointments response was not valid JSON") from None


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


class ZocdocAppointmentsIngestionComponent(Component, Model, Resolvable):
    """Ingest booked Zocdoc appointments as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.ZocdocAppointmentsIngestionComponent
        attributes:
          asset_name: zocdoc_appointments
          resource_key: zocdoc_resource
          practice_ids: "prac_123"
          from_date_time: "2026-06-01T00:00:00Z"
          to_date_time: "2026-07-01T00:00:00Z"
          limit: 1000
        ```

    See this component's README "PHI Safety" section before pointing it at
    a real, patient-connected Zocdoc practice.
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_key: str = Field(
        default="zocdoc_resource",
        description="Key of the ZocdocResource this asset depends on for authentication",
    )

    from_date_time: Optional[str] = Field(
        default=None,
        description="Start of the appointment start-time window (ISO-8601, e.g. '2026-06-01T00:00:00Z') -> start_time_utc_min. Required unless a time-based partition_type is set.",
    )
    to_date_time: Optional[str] = Field(
        default=None,
        description="End of the appointment start-time window (ISO-8601) -> start_time_utc_max. Required unless a time-based partition_type is set.",
    )

    practice_ids: Optional[str] = Field(
        default=None,
        description="Comma-separated Zocdoc practice ids to filter on.",
    )
    provider_ids: Optional[str] = Field(
        default=None,
        description="Comma-separated Zocdoc provider ids to filter on.",
    )
    location_ids: Optional[str] = Field(
        default=None,
        description="Comma-separated Zocdoc location ids to filter on.",
    )
    statuses: Optional[str] = Field(
        default=None,
        description="Comma-separated appointment statuses to filter on (e.g. 'booked,confirmed').",
    )
    sort_by: Optional[str] = Field(
        default=None,
        description="One of 'start_time_utc' / 'created_time_utc' / 'last_modified_time_utc'.",
    )
    sort_direction: Optional[str] = Field(
        default=None,
        description="One of 'ascending' / 'descending'.",
    )

    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Results per page (Zocdoc max is 100).",
    )
    limit: int = Field(
        default=1000,
        description="Maximum number of appointments to fetch across all pages (safety cap).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, from_date_time/to_date_time are derived from the partition window.",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based partition types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static/multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default="zocdoc", description="Asset group for organization")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags to apply to the asset")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Defaults to ['zocdoc', 'python'].")

    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime)")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_key = self.resource_key
        from_dt = self.from_date_time
        to_dt = self.to_date_time
        limit = self.limit
        page_size = self.page_size
        practice_ids = self.practice_ids
        provider_ids = self.provider_ids
        location_ids = self.location_ids
        statuses = self.statuses
        sort_by = self.sort_by
        sort_direction = self.sort_direction
        description = self.description or "Booked Zocdoc appointments for the configured filters/window."
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        _kinds = list(self.kinds or ["zocdoc", "python"])
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
        )
        def zocdoc_appointments_ingestion_asset(context: AssetExecutionContext):
            _from_dt, _to_dt = from_dt, to_dt
            if context.has_partition_key:
                try:
                    _window = context.partition_time_window
                    _from_dt = _window.start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    _to_dt = _window.end.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            if not _from_dt or not _to_dt:
                raise ValueError(
                    "Set from_date_time/to_date_time, or a time-based partition_type, to define the appointment window."
                )

            resource = getattr(context.resources, resource_key)
            session = resource.get_client()
            base_url = resource.get_base_url()

            context.log.info(
                f"Fetching Zocdoc appointments: {_from_dt} → {_to_dt} "
                f"(practice_ids={'set' if practice_ids else 'unset'}, "
                f"provider_ids={'set' if provider_ids else 'unset'}, "
                f"location_ids={'set' if location_ids else 'unset'}, "
                f"statuses={'set' if statuses else 'unset'}), limit={limit}"
            )

            params: Dict[str, Any] = {
                "start_time_utc_min": _from_dt,
                "start_time_utc_max": _to_dt,
                "page_size": min(page_size, 100),
                "page": 0,
            }
            if practice_ids:
                params["practice_ids"] = practice_ids
            if provider_ids:
                params["provider_ids"] = provider_ids
            if location_ids:
                params["location_ids"] = location_ids
            if statuses:
                params["statuses"] = statuses
            if sort_by:
                params["sort_by"] = sort_by
            if sort_direction:
                params["sort_direction"] = sort_direction

            rows: List[Dict[str, Any]] = []
            pages_fetched = 0
            total_count_reported = 0

            while len(rows) < limit:
                try:
                    body = _zocdoc_list_appointments(session, base_url, dict(params))
                except ZocdocAPIError as e:
                    # Status code + generic description only -- never the
                    # request/response body (may contain PHI).
                    raise Failure(
                        f"Zocdoc appointments fetch failed: {e}"
                    ) from None

                pages_fetched += 1
                page_rows = body.get("data") or []
                rows.extend(page_rows)
                total_count_reported = body.get("total_count") or total_count_reported

                has_next = bool(body.get("next_url"))
                if not has_next or not page_rows:
                    break
                params["page"] = params["page"] + 1

            rows = rows[:limit]
            context.log.info(f"Fetched {len(rows)} appointment record(s) across {pages_fetched} page(s).")

            df = pd.DataFrame(rows) if rows else pd.DataFrame()

            # --- PHI SAFETY: metadata is counts + configured filters ONLY ---
            # Never anything read back from the response body (no column
            # names, no sample values, no appointment IDs).
            metadata: Dict[str, Any] = {
                "rows_fetched": MetadataValue.int(len(df)),
                "pages_fetched": MetadataValue.int(pages_fetched),
                "total_count_reported": MetadataValue.int(int(total_count_reported or 0)),
                "from_date_time": MetadataValue.text(_from_dt),
                "to_date_time": MetadataValue.text(_to_dt),
            }
            if practice_ids:
                metadata["practice_ids"] = MetadataValue.text(practice_ids)
            if provider_ids:
                metadata["provider_ids"] = MetadataValue.text(provider_ids)
            if location_ids:
                metadata["location_ids"] = MetadataValue.text(location_ids)
            if statuses:
                metadata["statuses"] = MetadataValue.text(statuses)

            return Output(value=df, metadata=metadata)

        return Definitions(assets=[zocdoc_appointments_ingestion_asset])
