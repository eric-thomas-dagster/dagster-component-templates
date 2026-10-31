"""Sentry Issues Ingestion Component.

Pulls issues from Sentry's REST API (`/api/0/projects/{org}/{project}/issues/`)
and returns a DataFrame. Handles cursor pagination via the `Link` header.
"""

import os
import re
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


_NEXT_LINK_RE = re.compile(
    r"<(?P<url>[^>]+)>;\s*rel=\"next\";\s*results=\"true\"",
    re.IGNORECASE,
)


class SentryIssuesIngestionComponent(Component, Model, Resolvable):
    """Ingest issues from a Sentry project.

    Sentry API tokens: create at Organization Settings → Auth Tokens with
    `project:read` scope. Set the token in SENTRY_AUTH_TOKEN by default.
    """

    asset_name: str = Field(description="Output asset name.")

    organization: str = Field(
        description="Sentry organization slug (from the URL, e.g. 'acme-inc')."
    )
    project: str = Field(
        description="Sentry project slug (from the URL, e.g. 'frontend-web')."
    )

    api_host: str = Field(
        default="https://sentry.io",
        description=(
            "Sentry host. Use 'https://sentry.io' (default) for SaaS or your "
            "self-hosted URL."
        ),
    )
    api_token_env_var: str = Field(
        default="SENTRY_AUTH_TOKEN",
        description="Env var holding the Sentry auth token (needs `project:read` scope).",
    )

    query: str = Field(
        default="is:unresolved",
        description=(
            "Sentry search query. Examples: 'is:unresolved', 'is:resolved age:-24h', 'level:error'. See https://docs.sentry.io/product/sentry-basics/search/ On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
        ),
    )
    stats_period: Optional[str] = Field(
        default="24h",
        description="Time range for the query (e.g. '24h', '7d', '30d'). None = default.",
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Total row cap across paginated pages.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="sentry")

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
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        _self = self

        @asset(partitions_def=partitions_def, 
            key=_self.asset_name,
            description=_self.description or (
                f"Sentry issues from {_self.organization}/{_self.project} "
                f"({_self.query!r} over {_self.stats_period or 'default'})"
            ),
            group_name=_self.group_name,
            kinds={"python", "sentry"},
        )
        def _issues_asset(context: AssetExecutionContext):
            token = os.environ.get(_self.api_token_env_var)
            if not token:
                raise RuntimeError(
                    f"{_self.api_token_env_var!r} env var not set. Create an auth "
                    f"token at https://sentry.io/settings/account/api/auth-tokens/"
                )
            headers = {"Authorization": f"Bearer {token}"}
            url = (
                f"{_self.api_host.rstrip('/')}/api/0/projects/"
                f"{_self.organization}/{_self.project}/issues/"
            )
            params: Dict[str, Any] = {"query": _substitute_partition(_self.query, _partition_vars(context)), "limit": 100}
            if _self.stats_period:
                params["statsPeriod"] = _self.stats_period

            all_rows: List[Dict[str, Any]] = []
            page = 0
            _current_url = url
            _current_params = params
            while _current_url and len(all_rows) < _self.limit:
                page += 1
                resp = requests.get(
                    _current_url,
                    headers=headers,
                    params=_current_params if page == 1 else None,
                    timeout=30,
                )
                if resp.status_code == 401:
                    raise RuntimeError(
                        "Sentry returned 401 Unauthorized. Check that "
                        f"{_self.api_token_env_var} holds a valid token with "
                        "`project:read` scope."
                    )
                resp.raise_for_status()
                _batch = resp.json() or []
                all_rows.extend(_batch)
                # Parse next cursor from Link header.
                _link = resp.headers.get("Link", "") or ""
                _m = _NEXT_LINK_RE.search(_link)
                _current_url = _m.group("url") if _m else None
                _current_params = None
                if not _batch:
                    break

            df = pd.json_normalize(all_rows[: _self.limit], sep=".")
            context.log.info(
                f"Fetched {len(df)} Sentry issues across {page} page(s) from "
                f"{_self.organization}/{_self.project}"
            )

            metadata: Dict[str, Any] = {
                "dagster/row_count":    MetadataValue.int(len(df)),
                "sentry_organization":  MetadataValue.text(_self.organization),
                "sentry_project":       MetadataValue.text(_self.project),
                "sentry_query":         MetadataValue.text(_substitute_partition(_self.query, _partition_vars(context))),
                "pages_fetched":        MetadataValue.int(page),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    _sample = df.head(_self.preview_rows)
                    metadata["preview"] = MetadataValue.md(_sample.to_markdown(index=False))
                except Exception:  # noqa: BLE001
                    pass
            return Output(df, metadata=metadata)

        return Definitions(assets=[_issues_asset])


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
# END scaffold partitions-def builder

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
