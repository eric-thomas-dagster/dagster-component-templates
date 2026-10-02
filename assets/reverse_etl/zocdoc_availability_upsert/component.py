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


class ZocdocAvailabilityUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert warehouse-computed provider availability into Zocdoc timeslots.

    Example:
        ```yaml
        type: dagster_component_templates.ZocdocAvailabilityUpsertComponent
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

    upstream_asset_key: Optional[str] = Field(
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
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'zocdoc').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
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
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
