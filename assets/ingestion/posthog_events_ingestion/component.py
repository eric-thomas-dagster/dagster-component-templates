"""PostHog Events Ingestion Component.

Pulls events from PostHog's Query API (or the older /api/projects/{id}/events
endpoint) and returns a DataFrame. Handles cursor pagination automatically.
Supports personal API keys (POSTHOG_API_KEY env var by default) or a
per-project write key.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
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


class PostHogEventsIngestionComponent(Component, Model, Resolvable):
    """Ingest events from a PostHog project.

    Requires a personal API key (POSTHOG_API_KEY by default) — generate one
    at Account Settings → Personal API Keys with the `query:read` scope.
    """

    asset_name: str = Field(description="Output asset name.")

    project_id: str = Field(
        description="PostHog project ID (numeric). Find in your project settings."
    )

    api_host: str = Field(
        default="https://us.i.posthog.com",
        description=(
            "PostHog API host. Use 'https://us.i.posthog.com' (default) for US Cloud, "
            "'https://eu.i.posthog.com' for EU Cloud, or your self-hosted URL."
        ),
    )

    api_key_env_var: str = Field(
        default="POSTHOG_API_KEY",
        description="Env var holding a PostHog personal API key with `query:read` scope.",
    )

    event: Optional[str] = Field(
        default=None,
        description="Filter by event name (e.g. '$pageview'). None = all events.",
    )

    since_days: int = Field(
        default=1,
        ge=1,
        le=90,
        description="Fetch events from the last N days (max 90). Ignored on a partitioned time run, which fetches exactly its partition's window.",
    )

    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Total row cap. Paginates until this limit or all events fetched.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="posthog")

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
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        _self = self

        @asset(partitions_def=partitions_def, 
            key=_self.asset_name,
            description=_self.description or (
                f"PostHog events from project {_self.project_id}"
                + (f" (event='{_self.event}')" if _self.event else "")
            ),
            group_name=_self.group_name,
            kinds={"python", "posthog"},
        )
        def _events_asset(context: AssetExecutionContext):
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(
                    f"{_self.api_key_env_var!r} env var not set. Generate a personal "
                    f"API key at https://app.posthog.com/settings/user-api-keys"
                )
            headers = {"Authorization": f"Bearer {api_key}"}
            after = (datetime.utcnow() - timedelta(days=_self.since_days)).isoformat() + "Z"

            all_rows: List[Dict[str, Any]] = []
            url = f"{_self.api_host.rstrip('/')}/api/projects/{_self.project_id}/events/"
            params: Dict[str, Any] = {"after": after, "limit": 100}
            if context.has_partition_key or context.has_partition_key_range:
                # a time partition fetches exactly its own window (since_days only applies to unpartitioned runs)
                try:
                    _window = context.partition_time_window
                except Exception:
                    _window = None
                if _window is not None:
                    from datetime import timezone as _tz

                    params["after"] = _window.start.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    params["before"] = _window.end.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if _self.event:
                params["event"] = _self.event

            page = 0
            while url and len(all_rows) < _self.limit:
                page += 1
                resp = requests.get(url, headers=headers, params=params if page == 1 else None, timeout=30)
                if resp.status_code == 401:
                    raise RuntimeError(
                        "PostHog returned 401 Unauthorized. Check that "
                        f"{_self.api_key_env_var} holds a valid personal API key with "
                        "`query:read` scope."
                    )
                resp.raise_for_status()
                data = resp.json()
                _results = data.get("results") or []
                all_rows.extend(_results)
                url = data.get("next")  # PostHog cursor pagination
                if not _results:
                    break

            df = pd.json_normalize(all_rows[: _self.limit], sep=".")
            context.log.info(
                f"Fetched {len(df)} PostHog events across {page} page(s) "
                f"from project {_self.project_id}"
            )

            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "posthog_project_id": MetadataValue.text(str(_self.project_id)),
                "posthog_api_host":   MetadataValue.text(_self.api_host),
                "pages_fetched":      MetadataValue.int(page),
            }
            if _self.event:
                metadata["event_filter"] = MetadataValue.text(_self.event)
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    _sample = df.head(_self.preview_rows)
                    metadata["preview"] = MetadataValue.md(_sample.to_markdown(index=False))
                except Exception:  # noqa: BLE001
                    pass
            return Output(df, metadata=metadata)

        return Definitions(assets=[_events_asset])


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
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
# END scaffold partitions-def builder
