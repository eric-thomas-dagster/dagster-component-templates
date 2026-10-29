"""DataFrame -> X (Twitter) Ads Tailored Audience upsert.

Mirrors an upstream DataFrame into an existing X Ads Tailored (Custom)
Audience via the single users endpoint:

  `POST /accounts/:account_id/custom_audiences/:custom_audience_id/users`

The request body is a JSON array of operation objects, each combining an
`operation_type` ("Update" to add, "Delete" to remove) with a `params.users`
array -- one object per upstream row, each row contributing one or more
identifier-type keys (`email`, `phone_number`), each mapped to a *list* of
hashed values (confirmed against docs.x.com/x-ads-api/audiences and
docs.x.com/x-ads-api/audiences/reference). Unlike TikTok's one-file-per-type
upload, a single user row here can carry multiple identifier types at once
-- the same row-aligned shape Google Ads/Meta use.

This component only uploads members -- it does not create audiences.

Identifiers MUST be hashed before leaving your infrastructure -- X never
sees plaintext PII. Supported identifier types: `email`, `phone`.
Normalization follows X's documented rules (confirmed against
docs.x.com/x-ads-api/audiences/reference, and cross-checked against a
worked example there: "+11234567890" hashes to
"1fa6b8d986d9b9cd01bf36951815158bbde9f520c0567c835dfe34783d0a4231" -- which
only reproduces if the '+' is kept before hashing):
  - email: trim whitespace, lowercase, then SHA-256 (no salt)
  - phone: E.164 WITH a leading '+' (whitespace/punctuation stripped, country
    code required), then SHA-256 (no salt) -- same convention as Google Ads
    and TikTok, NOT Meta's digits-only one.

The API call itself is synchronous (it returns `success_count`/`total_count`
immediately), but X states that "audience changes are processed in batches
that run every 6-8 hours" -- so a successful response here means X accepted
the upload, not that matching against the audience has completed yet.

X caps each request at 2,500 user operations and 5,000,000 bytes -- this
component chunks the upstream rows accordingly, issuing one API call per
chunk and summing `success_count`/`total_count` across all of them.

Pairs with:
  - ``twitter_ads_resource`` -- OAuth 1.0a credentials + account ID (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone"}
_USER_FIELD_BY_IDENTIFIER = {"email": "email", "phone": "phone_number"}
_MAX_USERS_PER_REQUEST = 2500
_OPERATION_TYPE_BY_OPERATION = {"add": "Update", "remove": "Delete"}


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _normalize_phone(phone: str) -> str:
    """X requires E.164 (leading '+' + country code), whitespace/punctuation
    stripped, before hashing -- confirmed via a worked example in X's own
    Ads API docs. Same convention as Google Ads/TikTok, NOT Meta's
    digits-only one."""
    stripped = re.sub(r"[^\d+]", "", phone.strip())
    digits_only = stripped.lstrip("+")
    return "+" + digits_only


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _hash_phone(phone: str) -> str:
    return _sha256_hex(_normalize_phone(phone))


def _build_users_list(
    df_records: List[Dict[str, Any]], fields_map: Dict[str, str]
) -> tuple:
    """Pure, SDK-independent extraction: rows -> (users, rows_skipped_no_identifier).

    Each output `users` entry is a dict like {"email": ["<hash>"], "phone_number": ["<hash>"]}
    -- X's user-identifier values are always lists (a single user can carry
    multiple emails/phones), so even our one-value-per-column case is wrapped
    in a singleton list. A row with no valid identifier in any mapped column
    is skipped entirely (not sent as an empty user object)."""
    users: List[Dict[str, List[str]]] = []
    rows_skipped_no_identifier = 0
    for row in df_records:
        user: Dict[str, List[str]] = {}
        for col, id_type in fields_map.items():
            raw = row.get(col)
            if raw is None:
                continue
            try:
                import math
                if isinstance(raw, float) and math.isnan(raw):
                    continue
            except Exception:  # noqa: BLE001
                pass
            raw_str = str(raw).strip()
            if not raw_str:
                continue
            hashed = _hash_email(raw_str) if id_type == "email" else _hash_phone(raw_str)
            field_name = _USER_FIELD_BY_IDENTIFIER[id_type]
            user.setdefault(field_name, []).append(hashed)
        if user:
            users.append(user)
        else:
            rows_skipped_no_identifier += 1
    return users, rows_skipped_no_identifier


def _chunk_list(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start:start + chunk_size]


def _call_twitter_api(resource, custom_audience_id: str, operation_type: str, users: List[Dict[str, List[str]]]) -> dict:
    """Isolates the external Tailored Audience users-upload call so it can
    be monkeypatched wholesale in tests without `requests`/network access."""
    import requests

    url = f"{resource.api_base_url}/accounts/{resource.account_id}/custom_audiences/{custom_audience_id}/users"
    body = [
        {
            "operation_type": operation_type,
            "params": {"users": users},
        }
    ]
    headers = resource.get_headers("POST", url)
    response = requests.post(url, headers=headers, json=body, timeout=60)
    response.raise_for_status()
    return response.json()



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


class TwitterAdsTailoredAudienceUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into an X
    (Twitter) Ads Tailored Audience.

    Example:
        ```yaml
        type: dagster_community_components.TwitterAdsTailoredAudienceUpsertComponent
        attributes:
          asset_name: twitter_high_ltv_customers_audience
          upstream_asset_key: dbt_marts_high_ltv_customers
          resource_key: twitter_ads_resource
          custom_audience_id: "ztbh"
          fields_map:
            email: email
            phone_e164: phone
          operation: add
        ```

    `fields_map` maps upstream column -> identifier type (`email` or
    `phone`). Unlike TikTok (one file per identifier type), a single row
    here can carry multiple identifier types in one user object -- X
    matches on ANY identifier present, so supplying more improves match rate.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
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
        default="twitter_ads_resource",
        description="Resource key registered by TwitterAdsResourceComponent.",
    )

    custom_audience_id: str = Field(
        description=(
            "Target X Ads Tailored Audience ID. Must already exist -- create it "
            "once via Ads Manager (Tools -> Audiences -> Create Audience -> "
            "Create your own) or the Ads API's `custom_audience` endpoint; this "
            "component only uploads members, it does not create audiences."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Supported types: 'email', "
            "'phone'. Values are hashed (SHA-256) before upload -- never sent "
            "to X in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) adds matching users to the audience (X "
            "operation_type=Update) -- the normal activation case. 'remove' "
            "removes them (operation_type=Delete) -- useful for suppression."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="twitter_ads", description="Dagster asset group name.")

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
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'twitter_ads')."
    )

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
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("twitter_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "TwitterAdsTailoredAudienceUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"TwitterAdsTailoredAudienceUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"TwitterAdsTailoredAudienceUpsertComponent: fields_map has "
                f"unsupported identifier type(s) {sorted(bad_types)}. "
                f"Supported: {sorted(_SUPPORTED_IDENTIFIER_TYPES)}."
            )

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
            raise ValueError(f"TwitterAdsTailoredAudienceUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upload.")
                return dg.MaterializeResult(metadata={"rows_submitted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            users, rows_skipped_no_identifier = _build_users_list(df.to_dict("records"), _self.fields_map)

            if not users:
                context.log.warning(
                    "No rows had a valid identifier (email/phone) — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_identifier": dg.MetadataValue.int(rows_skipped_no_identifier),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            operation_type = _OPERATION_TYPE_BY_OPERATION[_self.operation]

            total_success = 0
            total_count = 0
            requests_sent = 0
            last_response: dict = {}
            for chunk in _chunk_list(users, _MAX_USERS_PER_REQUEST):
                response = _call_twitter_api(resource, _self.custom_audience_id, operation_type, chunk)
                requests_sent += 1
                last_response = response
                data = response.get("data") or {}
                total_success += int(data.get("success_count") or 0)
                total_count += int(data.get("total_count") or 0)

            context.log.info(
                f"X Ads Tailored Audience {_self.operation}: audience={_self.custom_audience_id} "
                f"requests_sent={requests_sent} rows_submitted={len(users)} "
                f"rows_skipped_no_identifier={rows_skipped_no_identifier} operation_type={operation_type}. "
                f"X processes audience changes in batches every 6-8 hours -- this reports upload "
                f"acceptance, not match results."
            )

            return dg.MaterializeResult(
                metadata={
                    "custom_audience_id": dg.MetadataValue.text(_self.custom_audience_id),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "operation_type": dg.MetadataValue.text(operation_type),
                    "requests_sent": dg.MetadataValue.int(requests_sent),
                    "rows_submitted": dg.MetadataValue.int(len(users)),
                    "rows_skipped_no_identifier": dg.MetadataValue.int(rows_skipped_no_identifier),
                    "success_count": dg.MetadataValue.int(total_success),
                    "total_count": dg.MetadataValue.int(total_count),
                    "last_response": dg.MetadataValue.json(last_response),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            partitions_def=partitions_def,
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into X Ads Tailored Audience "
                f"{_self.custom_audience_id} (operation={_self.operation})."
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
