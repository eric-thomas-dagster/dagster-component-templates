"""GoogleCalendarIngestionComponent — list Google Calendar events.

Service-account-authenticated reader for Google Calendar. Returns a
pandas DataFrame, one row per event in the configured time window.

The SA must be shared on the target calendar (open Google Calendar →
calendar settings → "Share with specific people or groups" → paste
the SA email → "See all event details").
"""

import datetime as _dt
import json
import os
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
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _flatten_event(ev: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a Calendar API event resource into a row-friendly dict."""
    start = ev.get("start", {})
    end = ev.get("end", {})
    attendees = ev.get("attendees", []) or []
    organizer = ev.get("organizer", {}) or {}
    return {
        "id":            ev.get("id"),
        "summary":       ev.get("summary"),
        "description":   ev.get("description"),
        "status":        ev.get("status"),
        "start":         start.get("dateTime") or start.get("date"),
        "end":           end.get("dateTime") or end.get("date"),
        "all_day":       "date" in start and "dateTime" not in start,
        "location":      ev.get("location"),
        "organizer":     organizer.get("email"),
        "attendees":     [a.get("email") for a in attendees if a.get("email")],
        "attendee_count": len(attendees),
        "html_link":     ev.get("htmlLink"),
        "hangout_link":  ev.get("hangoutLink"),
        "created":       ev.get("created"),
        "updated":       ev.get("updated"),
    }


class GoogleCalendarIngestionComponent(Component, Model, Resolvable):
    """List events from a Google Calendar via a service account.

    Returns a pandas DataFrame, one row per event in the time window. Default
    window is the next 30 days from now; override via `time_min` and `time_max`
    (ISO 8601 datetime strings, RFC3339 — Calendar API requirement).
    """

    asset_name: str = Field(description="Output asset name.")

    # Auth
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to service-account JSON. Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    # What to fetch
    calendar_id: str = Field(
        default="primary",
        description=(
            "Calendar ID. Use 'primary' for the SA's own (empty) calendar, OR a "
            "user's email address (the SA must be shared on that user's calendar). "
            "Note: the SA can't see a user's `primary` calendar without delegation; "
            "use the user's email or share a specific named calendar instead."
        ),
    )
    time_min: Optional[str] = Field(
        default=None,
        description="RFC3339 lower bound (e.g. '2026-05-01T00:00:00Z'). Defaults to now.",
    )
    time_max: Optional[str] = Field(
        default=None,
        description="RFC3339 upper bound. Defaults to now + 30 days.",
    )
    days_ahead: int = Field(
        default=30,
        description="Used when time_max is unset: now → now+days_ahead.",
    )
    max_results: int = Field(
        default=250,
        description="Cap on returned events. Calendar API hard-caps at 2500/page.",
    )
    single_events: bool = Field(
        default=True,
        description="Expand recurring events into individual instances. Recommended True.",
    )
    show_deleted: bool = Field(default=False)

    # Standard Dagster attrs
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "GoogleCalendarIngestionComponent: provide one of `credentials`, "
                "`credentials_path`, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        calendar_id = self.calendar_id
        time_min = self.time_min
        time_max = self.time_max
        days_ahead = self.days_ahead
        max_results = self.max_results
        single_events = self.single_events
        show_deleted = self.show_deleted

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Google Calendar events from {calendar_id}.",
            group_name=self.group_name,
            kinds={"google", "calendar"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.oauth2 import service_account
                from googleapiclient.discovery import build
            except ImportError:
                raise ImportError(
                    "Install google-auth and google-api-python-client: "
                    "pip install google-auth google-api-python-client"
                )

            scopes = ["https://www.googleapis.com/auth/calendar.readonly"]
            sa_creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scopes,
            )
            svc = build("calendar", "v3", credentials=sa_creds, cache_discovery=False)

            now = _dt.datetime.now(_dt.timezone.utc)
            tmin = time_min or now.isoformat().replace("+00:00", "Z")
            if time_max:
                tmax = time_max
            else:
                tmax = (now + _dt.timedelta(days=days_ahead)).isoformat().replace("+00:00", "Z")

            context.log.info(
                f"Calendar list: calendar_id={calendar_id!r}, "
                f"time_min={tmin}, time_max={tmax}, max_results={max_results}"
            )

            events: List[Dict[str, Any]] = []
            page_token = None
            while len(events) < max_results:
                try:
                    resp = svc.events().list(
                        calendarId=calendar_id,
                        timeMin=tmin,
                        timeMax=tmax,
                        maxResults=min(2500, max_results - len(events)),
                        singleEvents=single_events,
                        showDeleted=show_deleted,
                        orderBy="startTime" if single_events else None,
                        pageToken=page_token,
                    ).execute()
                except Exception as e:
                    err_str = str(e)
                    if "403" in err_str and "SERVICE_DISABLED" in err_str:
                        context.log.error(
                            "Calendar API not enabled on the SA's project. The "
                            "error message above includes the activation URL."
                        )
                    elif "404" in err_str:
                        context.log.error(
                            f"Calendar {calendar_id!r}: 404. Either the calendar "
                            f"doesn't exist or hasn't been shared with the SA. "
                            f"Open Google Calendar → the calendar's three-dot "
                            f"menu → Settings & sharing → Share with specific "
                            f"people or groups → paste {creds_dict.get('client_email','<sa-email>')!r} → "
                            f"'See all event details'."
                        )
                    elif "403" in err_str:
                        context.log.error(
                            f"Calendar {calendar_id!r}: 403. SA lacks access. "
                            f"Share the calendar with {creds_dict.get('client_email','<sa-email>')!r}."
                        )
                    raise
                events.extend(resp.get("items", []))
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            events = events[:max_results]
            context.log.info(f"Calendar returned {len(events)} events")

            rows = [_flatten_event(e) for e in events]
            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame(columns=[
                    "id", "summary", "description", "status", "start", "end",
                    "all_day", "location", "organizer", "attendees", "attendee_count",
                    "html_link", "hangout_link", "created", "updated",
                ])

            preview_md = df.head(10).to_markdown(index=False) if not df.empty else "(no events)"
            md = {
                "calendar_id":   MetadataValue.text(calendar_id),
                "time_min":      MetadataValue.text(tmin),
                "time_max":      MetadataValue.text(tmax),
                "event_count":   MetadataValue.int(len(df)),
                "preview":       MetadataValue.md(preview_md or ""),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])
