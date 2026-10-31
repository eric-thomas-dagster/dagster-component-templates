"""DataFrame -> Zocdoc provider availability/timeslots upsert.

Pushes warehouse-computed provider availability into Zocdoc's
provider-scheduling surface via:

  `PUT /v1/providers/{provider_id}/calendar/timeslots?date=YYYY-MM-DD`

Verified request shape (api-docs.zocdoc.com's "Create Timeslots" guide, 2026):

  ```json
  {
    "timeslots": [
      {
        "provider_id": "...",
        "location_id": "...",
        "start_time": "2026-06-01T09:00:00Z",
        "time_zone": "America/New_York",
        "allowed_visit_reason_ids": ["..."],
        "excluded_visit_reason_ids": ["..."],
        "patient_type": "new" | "established" | "all"
      },
      ...
    ]
  }
  ```

CRITICAL semantics (from the same guide -- this shapes this component's
whole design, not just a footnote):
  - This endpoint is a **full replace for one (provider_id, date) pair per
    call** -- "Subsequent requests for a single date will override
    previous data" and "All open timeslots must be included in each
    request per date." It is NOT additive.
  - "Submitting an empty request body will remove all slots for the given
    date" -- so an empty upstream DataFrame for a given (provider_id,
    date) group is never silently skipped; see `zocdoc_availability_upsert`
    README for how this component handles that.
  - Zocdoc accepts 0-1500 timeslot items per request. Because the call is
    a full replace, a (provider_id, date) group that exceeds 1500 rows
    CANNOT be safely split across multiple PUT calls (the second call
    would wipe the first's slots) -- this component refuses to call the
    API for any such group and reports it as an error instead of silently
    corrupting that day's calendar.

This component groups upstream rows by (provider_id, date) -- both
upstream columns, mapped via `fields_map` -- and issues exactly one PUT
per group.

================================================================================
PHI SAFETY -- READ BEFORE USE
================================================================================
Zocdoc's provider-scheduling surface touches real patient-facing calendars.
This component writes *availability* (open timeslots), not patient data --
but the write payload still includes operational detail (visit reason ids,
patient-type restrictions) that this repo treats conservatively:

  1. **No data preview, anywhere, under any configuration.** There is no
     `include_preview_metadata`/preview field in this component at all --
     the capability does not exist in the code.
  2. **Row counts only.** Metadata exposes `rows_processed`,
     `groups_processed`, `rows_upserted`, `rows_skipped_no_key`,
     `rows_errored`, and `groups_oversized` -- integers (plus bare
     provider_id/date identifiers for failed groups, which are
     operational/provider-schedule identifiers, never patient data).
  3. **Sanitized errors.** The one external HTTP call this component makes
     is isolated in `_zocdoc_put_timeslots` below. On a non-2xx response
     it raises `ZocdocAPIError`, carrying only the HTTP status code and a
     generic description -- never the request body (the actual timeslot
     list) or response body.

See this component's README "PHI Safety" section for the full rationale.
================================================================================

Pairs with:
  - ``zocdoc_resource`` -- OAuth2 client_credentials connection (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class ZocdocAPIError(Exception):
    """Raised when a Zocdoc API call fails. Carries ONLY the HTTP status
    code and a fixed, generic description -- never the request or
    response body, which may contain operational schedule detail."""

    def __init__(self, status_code: Optional[int], message: str):
        self.status_code = status_code
        super().__init__(
            f"{message} (status_code={status_code})" if status_code is not None else message
        )


def _zocdoc_put_timeslots(
    session, base_url: str, provider_id: str, date_str: str, timeslots: List[dict]
) -> Dict[str, Any]:
    """Isolates the one external API call this component makes
    (`PUT /v1/providers/{provider_id}/calendar/timeslots?date=...`) so it
    can be monkeypatched wholesale in tests.

    On failure, raises `ZocdocAPIError` carrying only the HTTP status code
    -- never the request body (the timeslot list) or response body.
    """
    url = f"{base_url}v1/providers/{provider_id}/calendar/timeslots"
    try:
        resp = session.put(url, params={"date": date_str}, json={"timeslots": timeslots}, timeout=30)
    except Exception:
        raise ZocdocAPIError(None, "Zocdoc timeslots request failed (network/connection error)") from None

    if resp.status_code >= 400:
        raise ZocdocAPIError(resp.status_code, "Zocdoc timeslots request failed")

    try:
        return resp.json() if resp.content else {}
    except Exception:
        return {}


_REQUIRED_TARGETS = {"provider_id", "date", "location_id", "start_time", "time_zone"}
_OPTIONAL_TARGETS = {"allowed_visit_reason_ids", "excluded_visit_reason_ids", "patient_type"}
_LIST_TARGETS = {"allowed_visit_reason_ids", "excluded_visit_reason_ids"}

_MAX_SLOTS_PER_REQUEST = 1500



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


class ZocdocAvailabilityUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert warehouse-computed provider availability into Zocdoc timeslots.

    Example:
        ```yaml
        type: dagster_community_components.ZocdocAvailabilityUpsertComponent
        attributes:
          asset_name: zocdoc_availability_sync
          upstream_asset_key: dbt_marts_provider_open_slots
          resource_key: zocdoc_resource
          fields_map:
            zocdoc_provider_id: provider_id
            slot_date: date
            zocdoc_location_id: location_id
            slot_start_time: start_time
            slot_time_zone: time_zone
            visit_reason_ids: allowed_visit_reason_ids
            patient_type: patient_type
          batch_size: 5000
        ```

    Every upstream row maps (via `fields_map`) to one Zocdoc timeslot. Rows
    are grouped by their mapped `(provider_id, date)` pair -- exactly one
    `PUT .../calendar/timeslots?date=...` call is issued per group, since
    Zocdoc's endpoint fully replaces that day's open slots per call.

    See this component's README "PHI Safety" section before pointing it at
    a real, patient-connected Zocdoc practice.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="zocdoc_resource",
        description="Resource key registered by ZocdocResourceComponent.",
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Zocdoc timeslot field. Required target values: "
            "'provider_id', 'date' (YYYY-MM-DD, used for grouping -- one PUT per "
            "(provider_id, date) pair), 'location_id', 'start_time' (ISO-8601), "
            "'time_zone' (IANA, e.g. 'America/New_York'). Optional target values: "
            "'allowed_visit_reason_ids', 'excluded_visit_reason_ids' (comma-separated "
            "string or list per row), 'patient_type' ('new' | 'established' | 'all')."
        ),
    )

    batch_size: int = Field(
        default=5000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="zocdoc", description="Dagster asset group name.")

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
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'zocdoc').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("zocdoc")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ZocdocAvailabilityUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        mapped_targets = set(self.fields_map.values())
        missing_required = _REQUIRED_TARGETS - mapped_targets
        if missing_required:
            raise ValueError(
                f"ZocdocAvailabilityUpsertComponent: fields_map is missing required "
                f"target field(s) {sorted(missing_required)}. fields_map must map "
                f"upstream columns onto all of {sorted(_REQUIRED_TARGETS)} (plus "
                f"optionally {sorted(_OPTIONAL_TARGETS)})."
            )
        unknown_targets = mapped_targets - _REQUIRED_TARGETS - _OPTIONAL_TARGETS
        if unknown_targets:
            raise ValueError(
                f"ZocdocAvailabilityUpsertComponent: fields_map targets unknown "
                f"Zocdoc timeslot field(s) {sorted(unknown_targets)}. Allowed targets: "
                f"{sorted(_REQUIRED_TARGETS | _OPTIONAL_TARGETS)}."
            )

        # Upstream-column -> target-field, inverted for row-building.
        col_by_target = {v: k for k, v in self.fields_map.items()}

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"ZocdocAvailabilityUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _row_value(v):
            import pandas as pd
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return v

        def _coerce_list(v):
            if v is None:
                return None
            if isinstance(v, (list, tuple)):
                return [str(x).strip() for x in v if str(x).strip()]
            return [x.strip() for x in str(v).split(",") if x.strip()]

        def _run_upsert(context, upstream):
            import pandas as pd
            resource = getattr(context.resources, _self.resource_key)
            session = resource.get_client()
            base_url = resource.get_base_url()

            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}.")
                df = df.head(_self.batch_size)

            required_cols = {col_by_target[t] for t in _REQUIRED_TARGETS}
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            rows_processed = 0
            rows_skipped_no_key = 0
            groups: Dict[tuple, List[dict]] = {}

            for _, row in df.iterrows():
                rows_processed += 1
                provider_id = _row_value(row[col_by_target["provider_id"]])
                date_str = _row_value(row[col_by_target["date"]])
                location_id = _row_value(row[col_by_target["location_id"]])
                start_time = _row_value(row[col_by_target["start_time"]])
                time_zone = _row_value(row[col_by_target["time_zone"]])

                if not provider_id or not date_str or not location_id or not start_time or not time_zone:
                    rows_skipped_no_key += 1
                    continue

                slot: Dict[str, Any] = {
                    "provider_id": str(provider_id),
                    "location_id": str(location_id),
                    "start_time": str(start_time),
                    "time_zone": str(time_zone),
                }
                for target in _OPTIONAL_TARGETS:
                    col = col_by_target.get(target)
                    if col is None or col not in df.columns:
                        continue
                    v = _row_value(row[col])
                    if v is None:
                        continue
                    if target in _LIST_TARGETS:
                        slot[target] = _coerce_list(v)
                    else:
                        slot[target] = v

                key = (str(provider_id), str(date_str))
                groups.setdefault(key, []).append(slot)

            groups_processed = 0
            groups_oversized = 0
            rows_upserted = 0
            rows_errored = 0
            errors: List[str] = []

            for (provider_id, date_str), slots in groups.items():
                if len(slots) > _MAX_SLOTS_PER_REQUEST:
                    # A full-replace PUT can't safely be split across
                    # multiple calls for the same (provider_id, date) --
                    # the second call would wipe the first's slots. Refuse
                    # rather than silently corrupting that day's calendar.
                    groups_oversized += 1
                    errors.append(
                        f"provider_id={provider_id} date={date_str}: "
                        f"{len(slots)} slots exceeds Zocdoc's {_MAX_SLOTS_PER_REQUEST}-per-request "
                        f"limit; a full-replace call cannot be safely chunked -- skipped."
                    )
                    continue
                try:
                    _zocdoc_put_timeslots(session, base_url, provider_id, date_str, slots)
                except ZocdocAPIError as e:
                    rows_errored += len(slots)
                    errors.append(f"provider_id={provider_id} date={date_str}: {e}")
                    continue
                groups_processed += 1
                rows_upserted += len(slots)

            context.log.info(
                f"Zocdoc availability upsert: {groups_processed} group(s) "
                f"({rows_upserted} slot(s)) upserted, {groups_oversized} group(s) "
                f"oversized/skipped, {rows_errored} row(s) errored, "
                f"{rows_skipped_no_key} row(s) skipped (missing required field)."
            )
            if errors:
                context.log.error("First few group errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "rows_processed": dg.MetadataValue.int(rows_processed),
                "groups_processed": dg.MetadataValue.int(groups_processed),
                "rows_upserted": dg.MetadataValue.int(rows_upserted),
                "rows_skipped_no_key": dg.MetadataValue.int(rows_skipped_no_key),
                "rows_errored": dg.MetadataValue.int(rows_errored),
                "groups_oversized": dg.MetadataValue.int(groups_oversized),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            partitions_def=partitions_def,
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                "Upsert DataFrame rows into Zocdoc provider timeslots "
                "(grouped by provider_id + date; full replace per group)."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

        return dg.Definitions(assets=[_asset])


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
