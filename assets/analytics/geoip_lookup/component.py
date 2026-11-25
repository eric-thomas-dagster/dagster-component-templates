"""GeoIP Lookup Component.

Enriches a DataFrame with geolocation data (city, country, lat/lng, postal
code, subdivision, ASN) looked up from an IP address column, using either a
local MaxMind `.mmdb` database file (fast, no per-row network call) or
MaxMind's hosted GeoIP2 web service (no local file to maintain, but a
network call per row).

LICENSING -- READ BEFORE USE: MaxMind GeoLite2/GeoIP2 databases require a
MaxMind account and EULA acceptance (required for GeoLite2 since
2019-12-30); this component does NOT bundle, redistribute, or
auto-download that database for you. In `database` mode (the default),
you must point `database_path` (and optionally `asn_database_path`) at a
`.mmdb` file YOU already hold a MaxMind license for and are responsible
for refreshing -- pair this component with `GeoIpDatabaseUpdaterComponent`
in this same registry if you want that refresh automated, or run
MaxMind's own `geoipupdate` tool yourself. In `web_service` mode, MaxMind
itself holds and serves the data per request, but you still need your own
MaxMind account_id/license_key.

IPv6: both the local `.mmdb` format and the web service cover IPv4 and
IPv6 natively -- no separate configuration is needed.
"""

import os
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
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


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


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


# --- backfill_policy support -------------------------------------------------
# A single_run backfill hands the asset a whole partition RANGE, on which
# `context.partition_key` raises. The compute below was written against one
# key at a time, so a range is run key-by-key through a thin context proxy and
# the per-partition frames are concatenated.
from typing import Literal  # noqa: E402,F401


class _SinglePartitionContext:
    """Presents ONE partition key of a range run to compute code that reads
    ``context.partition_key``; everything else delegates to the real context.
    ``add_output_metadata`` is captured (the real context only accepts one call
    per output) and merged by ``_run_per_partition``."""

    def __init__(self, context, partition_key):
        self._ctx = context
        self._key = partition_key
        self.captured_metadata = {}

    @property
    def has_partition_key(self):
        return self._key is not None

    @property
    def has_partition_key_range(self):
        return False

    @property
    def partition_key(self):
        if self._key is None:
            raise AttributeError("no partition key on this run")
        return self._key

    @property
    def partition_keys(self):
        return [] if self._key is None else [self._key]

    def add_output_metadata(self, metadata, output_name=None, mapping_key=None):
        self.captured_metadata.update(metadata or {})

    def __getattr__(self, name):
        return getattr(self._ctx, name)


def _run_per_partition(context, compute, partitions_def, loop, args, kwargs):
    """Run ``compute(context, *args, **kwargs)``; for a multi-partition range
    run it once per key (when ``loop``) and concatenate the DataFrames."""
    import pandas as pd
    from dagster import MetadataValue, Output

    if not (context.has_partition_key or context.has_partition_key_range):
        return compute(context, *args, **kwargs)
    keys = list(context.partition_keys)
    if len(keys) <= 1 and context.has_partition_key:
        return compute(context, *args, **kwargs)  # ordinary single-partition run

    if not loop:
        # Partition key does not change the computation: run once.
        proxy = _SinglePartitionContext(context, None)
        result = compute(proxy, *args, **kwargs)
        if isinstance(result, Output):
            proxy.captured_metadata.update(result.metadata or {})
            result = result.value
        if proxy.captured_metadata:
            context.add_output_metadata(proxy.captured_metadata)
        return result

    frames, merged = [], {}
    for key in keys:
        if hasattr(partitions_def, "partitions_defs") and not hasattr(key, "keys_by_dimension"):
            key = partitions_def.get_partition_key_from_str(key)
        proxy = _SinglePartitionContext(context, key)
        result = compute(proxy, *args, **kwargs)
        merged.update(proxy.captured_metadata)
        if isinstance(result, Output):
            merged.update(result.metadata or {})
            result = result.value
        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                f"backfill_policy='single_run' needs a DataFrame result per partition; "
                f"got {type(result).__name__} for partition {key!r}"
            )
        result = result.copy()
        if "_partition_key" not in result.columns:
            result["_partition_key"] = str(key)
        frames.append(result)
    merged["partition_keys"] = MetadataValue.json([str(k) for k in keys])
    context.add_output_metadata(merged)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class GeoIpLookupComponent(Component, Model, Resolvable):
    """Component for enriching a DataFrame with geolocation data looked up
    from an IP address column (MaxMind GeoLite2/GeoIP2 -- see the module
    docstring for the licensing requirements before using `database` mode).

    Example:
        ```yaml
        type: dagster_community_components.GeoIpLookupComponent
        attributes:
          asset_name: enriched_web_logs
          upstream_asset_key: web_logs
          ip_column: client_ip
          database_path: /var/lib/geoip/GeoLite2-City.mmdb
          asn_database_path: /var/lib/geoip/GeoLite2-ASN.mmdb
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame with an IP address column"
    )

    ip_column: Union[str, int] = Field(
        description="Column containing the IPv4 or IPv6 address string to look up"
    )

    lookup_mode: str = Field(
        default="database",
        description=(
            "'database' (default) -- reads a local MaxMind .mmdb file via database_path/"
            "asn_database_path, no per-row network call, YOU must supply and refresh the "
            "file (see module docstring). 'web_service' -- calls MaxMind's hosted GeoIP2 "
            "web service per row (account_id_env_var/license_key_env_var required); no "
            "local file to maintain, but one network call per row, and ASN enrichment is "
            "NOT available in this mode (MaxMind's web service has no ASN endpoint)."
        ),
    )

    database_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to a local MaxMind City/Country .mmdb file (e.g. GeoLite2-City.mmdb). "
            "Required for city/country/lat-lng/postal/subdivision fields in 'database' mode. "
            "NOT bundled or downloaded by this component -- point this at a file you "
            "already hold a MaxMind license for, e.g. one maintained by "
            "GeoIpDatabaseUpdaterComponent in this same registry."
        ),
    )

    asn_database_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to a separate local MaxMind ASN .mmdb file (e.g. GeoLite2-ASN.mmdb). "
            "ASN is always a SEPARATE MaxMind database from City/Country. Only used in "
            "'database' mode; set this to populate asn_column/asn_org_column."
        ),
    )

    account_id_env_var: str = Field(
        default="MAXMIND_ACCOUNT_ID",
        description="Environment variable holding your MaxMind account ID. Only used in 'web_service' mode.",
    )

    license_key_env_var: str = Field(
        default="MAXMIND_LICENSE_KEY",
        description="Environment variable holding your MaxMind license key. Only used in 'web_service' mode.",
    )

    web_service_host: str = Field(
        default="geoip.maxmind.com",
        description="MaxMind web service host. Only used in 'web_service' mode.",
    )

    batch_delay: float = Field(
        default=0.0,
        description="Seconds to sleep between rows in 'web_service' mode, to respect rate limits. Ignored in 'database' mode.",
    )

    city_column: Optional[Union[str, int]] = Field(default="geoip_city", description="Output column for the city name, or null to omit.")
    country_column: Optional[Union[str, int]] = Field(default="geoip_country", description="Output column for the country name, or null to omit.")
    country_iso_code_column: Optional[Union[str, int]] = Field(default="geoip_country_iso_code", description="Output column for the ISO country code, or null to omit.")
    latitude_column: Optional[Union[str, int]] = Field(default="geoip_latitude", description="Output column for latitude, or null to omit.")
    longitude_column: Optional[Union[str, int]] = Field(default="geoip_longitude", description="Output column for longitude, or null to omit.")
    continent_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the continent name, or null (default) to omit.")
    subdivision_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the most-specific subdivision (state/region) name, or null (default) to omit.")
    postal_code_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the postal code, or null (default) to omit.")
    accuracy_radius_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the location accuracy radius (km), or null (default) to omit.")
    asn_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the autonomous system number, or null (default) to omit. Requires asn_database_path (database mode only).")
    asn_org_column: Optional[Union[str, int]] = Field(default=None, description="Output column for the autonomous system organization name, or null (default) to omit. Requires asn_database_path (database mode only).")

    match_status_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional output column recording per-row lookup status: 'matched', 'not_found' (valid IP, no DB entry -- private/reserved ranges land here), or 'invalid' (unparseable IP string). Null (default) to omit.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run: the analysis runs once per partition in the range, each on the upstream rows filtered to that partition (via partition_date_column / partition_static_column), and the per-partition outputs are concatenated with a '_partition_key' column added. 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, uncapped). Ignored when partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="GeoIP Lookup", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        ip_column = self.ip_column
        lookup_mode = (self.lookup_mode or "database").lower()
        database_path = self.database_path
        asn_database_path = self.asn_database_path
        account_id_env_var = self.account_id_env_var
        license_key_env_var = self.license_key_env_var
        web_service_host = self.web_service_host
        batch_delay = self.batch_delay
        group_name = self.group_name

        city_column = self.city_column
        country_column = self.country_column
        country_iso_code_column = self.country_iso_code_column
        latitude_column = self.latitude_column
        longitude_column = self.longitude_column
        continent_column = self.continent_column
        subdivision_column = self.subdivision_column
        postal_code_column = self.postal_code_column
        accuracy_radius_column = self.accuracy_radius_column
        asn_column = self.asn_column
        asn_org_column = self.asn_org_column
        match_status_column = self.match_status_column

        if lookup_mode not in ("database", "web_service"):
            raise ValueError(f"lookup_mode must be 'database' or 'web_service', got {lookup_mode!r}")
        if lookup_mode == "database" and not database_path and not asn_database_path:
            raise ValueError("lookup_mode='database' requires database_path and/or asn_database_path.")
        if lookup_mode == "web_service" and (asn_column or asn_org_column):
            raise ValueError(
                "ASN enrichment (asn_column/asn_org_column) is only available in "
                "lookup_mode='database' with asn_database_path set -- MaxMind's "
                "hosted web service has no ASN endpoint."
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _inferred_kinds = self.kinds or ["python"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy  # noqa: F401
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _deco_geoip_asset = asset(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"GeoIP-enriched '{ip_column}' ({lookup_mode} mode)",
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )

        def geoip_asset_compute(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]

            df = upstream.copy()
            n = len(df)
            cities = [None] * n
            countries = [None] * n
            country_isos = [None] * n
            continents = [None] * n
            subdivisions = [None] * n
            postal_codes = [None] * n
            lats = [None] * n
            lngs = [None] * n
            accuracy_radii = [None] * n
            asns = [None] * n
            asn_orgs = [None] * n
            statuses = [None] * n

            matched = 0
            unmatched_not_found = 0
            unmatched_invalid = 0

            if lookup_mode == "database":
                try:
                    import geoip2.database
                    import geoip2.errors
                except ImportError:
                    raise ImportError("geoip2 is required: pip install geoip2")

                city_reader = geoip2.database.Reader(database_path) if database_path else None
                asn_reader = geoip2.database.Reader(asn_database_path) if asn_database_path else None
                try:
                    context.log.info(f"Looking up {n} IP(s) against local .mmdb file(s)")
                    for i, ip in enumerate(df[ip_column]):
                        ip_str = str(ip)
                        row_status = "matched"
                        row_any_lookup_failed = False
                        if city_reader is not None:
                            try:
                                resp = city_reader.city(ip_str)
                                cities[i] = resp.city.name
                                countries[i] = resp.country.name
                                country_isos[i] = resp.country.iso_code
                                continents[i] = resp.continent.name
                                subdivisions[i] = resp.subdivisions.most_specific.name
                                postal_codes[i] = resp.postal.code
                                lats[i] = resp.location.latitude
                                lngs[i] = resp.location.longitude
                                accuracy_radii[i] = resp.location.accuracy_radius
                            except geoip2.errors.AddressNotFoundError:
                                row_status = "not_found"
                                row_any_lookup_failed = True
                            except ValueError:
                                row_status = "invalid"
                                row_any_lookup_failed = True
                        if asn_reader is not None:
                            try:
                                asn_resp = asn_reader.asn(ip_str)
                                asns[i] = asn_resp.autonomous_system_number
                                asn_orgs[i] = asn_resp.autonomous_system_organization
                            except geoip2.errors.AddressNotFoundError:
                                if row_status == "matched":
                                    row_status = "not_found"
                                row_any_lookup_failed = True
                            except ValueError:
                                row_status = "invalid"
                                row_any_lookup_failed = True
                        statuses[i] = row_status
                        if row_status == "matched":
                            matched += 1
                        elif row_status == "not_found":
                            unmatched_not_found += 1
                        else:
                            unmatched_invalid += 1
                        if row_any_lookup_failed and row_status == "invalid":
                            context.log.warning(f"Skipping invalid IP address '{ip}'")
                        if (i + 1) % 1000 == 0:
                            context.log.info(f"Progress: {i + 1}/{n} IPs processed")
                finally:
                    if city_reader is not None:
                        city_reader.close()
                    if asn_reader is not None:
                        asn_reader.close()
            else:  # web_service
                try:
                    import geoip2.webservice
                    import geoip2.errors
                except ImportError:
                    raise ImportError("geoip2 is required: pip install geoip2")

                account_id = os.environ.get(account_id_env_var)
                license_key = os.environ.get(license_key_env_var)
                if not account_id or not license_key:
                    raise ValueError(
                        f"lookup_mode='web_service' requires {account_id_env_var} and "
                        f"{license_key_env_var} to be set with your MaxMind credentials."
                    )
                client = geoip2.webservice.Client(int(account_id), license_key, host=web_service_host)
                try:
                    context.log.info(f"Looking up {n} IP(s) against MaxMind's web service")
                    for i, ip in enumerate(df[ip_column]):
                        ip_str = str(ip)
                        try:
                            resp = client.city(ip_str)
                            cities[i] = resp.city.name
                            countries[i] = resp.country.name
                            country_isos[i] = resp.country.iso_code
                            continents[i] = resp.continent.name
                            subdivisions[i] = resp.subdivisions.most_specific.name
                            postal_codes[i] = resp.postal.code
                            lats[i] = resp.location.latitude
                            lngs[i] = resp.location.longitude
                            accuracy_radii[i] = resp.location.accuracy_radius
                            statuses[i] = "matched"
                            matched += 1
                        except geoip2.errors.AddressNotFoundError:
                            statuses[i] = "not_found"
                            unmatched_not_found += 1
                        except (geoip2.errors.InvalidRequestError, ValueError):
                            statuses[i] = "invalid"
                            unmatched_invalid += 1
                            context.log.warning(f"Skipping invalid IP address '{ip}'")
                        if batch_delay:
                            time.sleep(batch_delay)
                        if (i + 1) % 100 == 0:
                            context.log.info(f"Progress: {i + 1}/{n} IPs processed")
                finally:
                    client.close()

            if city_column:
                df[city_column] = cities
            if country_column:
                df[country_column] = countries
            if country_iso_code_column:
                df[country_iso_code_column] = country_isos
            if continent_column:
                df[continent_column] = continents
            if subdivision_column:
                df[subdivision_column] = subdivisions
            if postal_code_column:
                df[postal_code_column] = postal_codes
            if latitude_column:
                df[latitude_column] = lats
            if longitude_column:
                df[longitude_column] = lngs
            if accuracy_radius_column:
                df[accuracy_radius_column] = accuracy_radii
            if asn_column:
                df[asn_column] = asns
            if asn_org_column:
                df[asn_org_column] = asn_orgs
            if match_status_column:
                df[match_status_column] = statuses

            match_rate = matched / n * 100 if n > 0 else 0
            context.log.info(
                f"GeoIP lookup complete: {matched}/{n} matched ({match_rate:.1f}%), "
                f"{unmatched_not_found} not found, {unmatched_invalid} invalid"
            )

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "rows_matched": MetadataValue.int(matched),
                "rows_unmatched_not_found": MetadataValue.int(unmatched_not_found),
                "rows_unmatched_invalid": MetadataValue.int(unmatched_invalid),
            }
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        @_deco_geoip_asset
        def geoip_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            return _run_per_partition(context, geoip_asset_compute, partitions_def, bool(partition_date_column or partition_static_column), [upstream], {})

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[geoip_asset])

        return Definitions(assets=[geoip_asset], asset_checks=list(_schema_checks))


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
