"""Dataframe → HTTP sink component.

Write a pandas DataFrame to any HTTP/REST endpoint via POST / PUT. Three
batch shapes:

- `per_record` (default) — one request per row, body = that row as a JSON
  object. `url` may template row values via `{column_name}` (e.g.
  `https://api.example.com/customers/{customer_id}`).
- `batch_array` — chunks of `batch_size` rows, body = a JSON array of row
  objects, one request per chunk.
- `ndjson` — chunks of `batch_size` rows, body = newline-delimited JSON
  (one row object per line, `Content-Type: application/x-ndjson`), one
  request per chunk.

Auth: `none` | `basic` | `bearer` | `oauth2_client_credentials` (token
fetched once per run and cached across runs in-process, re-fetched a
little before expiry).

Retries: exponential backoff on configurable status codes (429/5xx by
default) and on connection/timeout errors, up to `max_retries`.

Set `write_response_to_record` to append the HTTP status/body/parsed-JSON
back onto each row and materialize the enriched DataFrame as this asset's
own output, instead of a plain sink with no output value — useful when a
downstream step needs the endpoint's generated ID or confirmation payload.
"""

import math
import time
from typing import Any, Dict, List, Literal, Optional, Union

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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _coerce_for_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """Clean a pandas row dict for JSON serialization (NaN→None, Timestamps→ISO)."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "item") and not isinstance(v, (str, bytes, dict, list)):
            try:
                out[k] = v.item()
            except (ValueError, AttributeError):
                out[k] = v
        else:
            out[k] = v
    return out


# Module-level (not instance/class-attribute) OAuth2 token cache, keyed by
# (token_url, client_id_env_var) -- deliberately NOT a pydantic class/instance
# attribute (zocdoc_resource's own history: a class-vs-instance private-attr
# footgun was found and fixed there). A component instance isn't a long-lived
# singleton across runs the way a resource is, so a plain module dict keyed
# on the config that actually identifies the token is the simplest correct
# cache here.
_OAUTH2_TOKEN_CACHE: Dict[str, Dict[str, Any]] = {}


def _fetch_oauth2_client_credentials_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: Optional[str],
    audience: Optional[str],
) -> Dict[str, Any]:
    """Isolates the one external token-request call so it can be monkeypatched
    wholesale in tests. Returns the raw token response dict ({'access_token',
    'expires_in', ...}); caller is responsible for caching."""
    import requests

    body: Dict[str, Any] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        body["scope"] = scope
    if audience:
        body["audience"] = audience
    resp = requests.post(token_url, data=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _get_oauth2_access_token(
    token_url: str,
    client_id_env_var: str,
    client_secret_env_var: str,
    scope: Optional[str],
    audience: Optional[str],
) -> str:
    cache_key = f"{token_url}:{client_id_env_var}"
    cached = _OAUTH2_TOKEN_CACHE.get(cache_key) or {}
    if cached.get("expires", 0) > time.time() + 60:
        return cached["access_token"]

    client_id = _require_env(client_id_env_var)
    client_secret = _require_env(client_secret_env_var)
    data = _fetch_oauth2_client_credentials_token(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        audience=audience,
    )
    _OAUTH2_TOKEN_CACHE[cache_key] = {
        "access_token": data["access_token"],
        # Refresh a little early rather than racing expiry.
        "expires": time.time() + data.get("expires_in", 3600) - 60,
    }
    return data["access_token"]



def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
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


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


class DataframeToHttpComponent(Component, Model, Resolvable):
    """Sink — POST/PUT each row (or batch of rows) of a DataFrame to an HTTP endpoint.

    Example — POST each row individually to a REST API with bearer auth:

        ```yaml
        type: dagster_community_components.DataframeToHttpComponent
        attributes:
          asset_name: orders_posted
          upstream_asset_key: transformed_orders
          url: https://api.example.com/v1/orders
          method: POST
          batch_mode: per_record
          auth_type: bearer
          auth_token_env_var: ORDERS_API_TOKEN
          max_retries: 5
          retry_on_status_codes: [429, 500, 502, 503, 504]
        ```

    Example — batch 200 rows per request as a JSON array, OAuth2 client_credentials auth:

        ```yaml
        type: dagster_community_components.DataframeToHttpComponent
        attributes:
          asset_name: events_posted
          upstream_asset_key: enriched_events
          url: https://ingest.example.com/v2/events/bulk
          method: POST
          batch_mode: batch_array
          batch_size: 200
          auth_type: oauth2_client_credentials
          oauth2_token_url: https://auth.example.com/oauth/token
          oauth2_client_id_env_var: EVENTS_CLIENT_ID
          oauth2_client_secret_env_var: EVENTS_CLIENT_SECRET
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset producing the DataFrame to write"
    )

    url: str = Field(
        description=(
            "Target URL. In `per_record` mode, may template row values via "
            "`{column_name}` (e.g. `https://api.example.com/customers/{customer_id}`) — "
            "templated fields are applied per row before the request is sent. "
            "`batch_array`/`ndjson` modes send to this URL literally (no per-row templating, "
            "since one request carries many rows)."
        )
    )
    method: Literal["POST", "PUT"] = Field(default="POST", description="HTTP method.")
    batch_mode: Literal["per_record", "batch_array", "ndjson"] = Field(
        default="per_record",
        description=(
            "'per_record' — one request per row, body = that row as a JSON object. "
            "'batch_array' — chunks of `batch_size` rows, body = a JSON array of row objects. "
            "'ndjson' — chunks of `batch_size` rows, body = newline-delimited JSON "
            "(Content-Type: application/x-ndjson)."
        ),
    )
    batch_size: int = Field(
        default=100, ge=1,
        description="Rows per request for `batch_array` / `ndjson` modes. Ignored for `per_record`.",
    )

    # --- Auth ------------------------------------------------------------

    auth_type: Literal["none", "basic", "bearer", "oauth2_client_credentials"] = Field(
        default="none",
        description="'none' | 'basic' | 'bearer' | 'oauth2_client_credentials'.",
    )
    auth_username_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_password_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_token_env_var: Optional[str] = Field(default=None, description="Required for auth_type='bearer'.")
    oauth2_token_url: Optional[str] = Field(
        default=None, description="Token endpoint. Required for auth_type='oauth2_client_credentials'."
    )
    oauth2_client_id_env_var: Optional[str] = Field(
        default=None, description="Env var holding the OAuth2 client ID. Required for auth_type='oauth2_client_credentials'."
    )
    oauth2_client_secret_env_var: Optional[str] = Field(
        default=None, description="Env var holding the OAuth2 client secret. Required for auth_type='oauth2_client_credentials'."
    )
    oauth2_scope: Optional[str] = Field(default=None, description="Optional OAuth2 scope to request.")
    oauth2_audience: Optional[str] = Field(default=None, description="Optional OAuth2 audience parameter.")

    headers: Optional[Dict[str, str]] = Field(
        default=None, description="Extra static HTTP headers sent with every request."
    )
    verify_ssl: bool = Field(default=True)

    # --- Behavior ----------------------------------------------------------

    skip_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to drop before serializing the row body (e.g. internal pandas columns).",
    )
    write_response_to_record: bool = Field(
        default=False,
        description=(
            "When true, append `http_response_status` / `http_response_body` / "
            "`http_response_json` columns to each row and materialize the enriched "
            "DataFrame as this asset's output, instead of a plain sink with no output "
            "value. In batch modes (`batch_array`/`ndjson`), every row in a chunk gets "
            "that chunk's single response recorded (requests aren't per-row, so the "
            "response can't be attributed more granularly than its batch)."
        ),
    )
    response_json_path: Optional[str] = Field(
        default=None,
        description=(
            "Only used with write_response_to_record. Dotted path into the parsed JSON "
            "response to extract into `http_response_value` (e.g. 'data.id' for "
            "{'data': {'id': 123}}). Leave unset to only get the raw body/status/full JSON."
        ),
    )

    timeout_seconds: int = Field(default=30, ge=1, le=600, description="Per-request timeout in seconds.")
    max_retries: int = Field(default=3, ge=0, le=10, description="Retries per request before giving up on it.")
    retry_backoff_seconds: float = Field(
        default=1.0, description="Base delay for exponential backoff between retries (delay = base * 2**attempt)."
    )
    retry_on_status_codes: List[int] = Field(
        default_factory=lambda: [429, 500, 502, 503, 504],
        description="Response status codes that trigger a retry (in addition to connection/timeout errors, which always retry).",
    )
    fail_after_n_failures: int = Field(
        default=10,
        description="Abort the whole materialization after this many failed requests (sampled in `failures_sample` metadata).",
    )

    # --- Standard fields ------------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="http_sink")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
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
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    dynamic_partition_name: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset FAILURE (the whole run step), separate from max_retries (per-HTTP-request retries above).",
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run. 'multi_run' "
            "launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data, lists=('owners', 'kinds', 'deps', 'retry_on_status_codes'),
            csv=('partition_values',), tags=('asset_tags',),
        )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    # -------------------------------------------------------------------------

    def _build_session(self):
        import requests

        s = requests.Session()
        s.verify = self.verify_ssl
        s.headers.update({"Content-Type": "application/json"})
        if self.headers:
            s.headers.update(self.headers)

        if self.auth_type == "basic":
            if not (self.auth_username_env_var and self.auth_password_env_var):
                raise ValueError("auth_type='basic' requires auth_username_env_var + auth_password_env_var.")
            s.auth = (_require_env(self.auth_username_env_var), _require_env(self.auth_password_env_var))
        elif self.auth_type == "bearer":
            if not self.auth_token_env_var:
                raise ValueError("auth_type='bearer' requires auth_token_env_var.")
            s.headers["Authorization"] = f"Bearer {_require_env(self.auth_token_env_var)}"
        elif self.auth_type == "oauth2_client_credentials":
            if not (self.oauth2_token_url and self.oauth2_client_id_env_var and self.oauth2_client_secret_env_var):
                raise ValueError(
                    "auth_type='oauth2_client_credentials' requires oauth2_token_url, "
                    "oauth2_client_id_env_var, and oauth2_client_secret_env_var."
                )
            token = _get_oauth2_access_token(
                token_url=self.oauth2_token_url,
                client_id_env_var=self.oauth2_client_id_env_var,
                client_secret_env_var=self.oauth2_client_secret_env_var,
                scope=self.oauth2_scope,
                audience=self.oauth2_audience,
            )
            s.headers["Authorization"] = f"Bearer {token}"
        elif self.auth_type != "none":
            raise ValueError(f"unknown auth_type: {self.auth_type!r}")
        return s

    def _request_with_retry(self, session, method: str, url: str, **kwargs):
        import requests

        retry_statuses = set(int(c) for c in (self.retry_on_status_codes or []))
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = session.request(method, url, timeout=self.timeout_seconds, **kwargs)
            except requests.RequestException as e:
                last_exc = e
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                    continue
                raise
            if resp.status_code in retry_statuses and attempt < self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else self.retry_backoff_seconds * (2 ** attempt)
                time.sleep(delay)
                continue
            return resp
        if last_exc is not None:
            raise last_exc
        return resp  # pragma: no cover — unreachable (loop always returns or raises)

    @staticmethod
    def _extract_json_path(data: Any, path: Optional[str]) -> Any:
        if not path or data is None:
            return None
        cur = data
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dataframe To Http', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        component = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        description = self.description or f"HTTP {self.method} ({self.batch_mode}) → {self.url}"

        kinds = list(self.kinds or []) or ["http", "api"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            backfill_policy=backfill_policy,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            freshness_policy=freshness_policy,
        )
        def dataframe_to_http_asset(context: AssetExecutionContext, upstream):
            # Defensive Output/MaterializeResult unwrap, matching every other sink in this repo.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            # Filter to current partition if partitioned
            if hasattr(upstream, "columns") and (context.has_partition_key or context.has_partition_key_range):
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
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
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(f"Upstream must produce a pandas DataFrame, got {type(upstream).__name__}")
            df = upstream
            if component.skip_columns:
                df = df.drop(columns=[c for c in component.skip_columns if c in df.columns])

            session = component._build_session()
            method = component.method
            batch_mode = component.batch_mode
            write_back = component.write_response_to_record

            sent = 0
            failed = 0
            failures: List[Dict[str, Any]] = []
            response_cols: List[Dict[str, Any]] = []  # one entry per row, aligned to df's row order

            def _record_response(resp, n_rows: int):
                status = resp.status_code
                body_text = resp.text
                parsed = None
                ctype = (resp.headers.get("content-type", "") or "").lower()
                if "json" in ctype:
                    try:
                        parsed = resp.json()
                    except ValueError:
                        parsed = None
                extracted = component._extract_json_path(parsed, component.response_json_path)
                entry = {
                    "http_response_status": status,
                    "http_response_body": body_text[:4000],
                    "http_response_json": parsed,
                    "http_response_value": extracted,
                }
                for _ in range(n_rows):
                    response_cols.append(entry)

            df_reset = df.reset_index(drop=True)

            if batch_mode == "per_record":
                for _, row in df_reset.iterrows():
                    row_dict = _coerce_for_json(row.to_dict())
                    try:
                        url = component.url.format(**row_dict) if "{" in component.url else component.url
                        resp = component._request_with_retry(session, method, url, json=row_dict)
                        if write_back:
                            _record_response(resp, 1)
                        if resp.status_code >= 400:
                            failed += 1
                            failures.append({"status": resp.status_code, "body": resp.text[:500]})
                            if failed >= component.fail_after_n_failures:
                                context.log.error(f"Aborting after {failed} failures. Sample: {failures[:3]}")
                                break
                        else:
                            sent += 1
                    except Exception as e:
                        failed += 1
                        failures.append({"error": str(e)})
                        if write_back:
                            response_cols.append({
                                "http_response_status": -1, "http_response_body": str(e)[:4000],
                                "http_response_json": None, "http_response_value": None,
                            })
                        if failed >= component.fail_after_n_failures:
                            break
            else:  # batch_array | ndjson
                batch_size = component.batch_size
                url = component.url
                rows = [_coerce_for_json(r) for r in df_reset.to_dict(orient="records")]
                for start in range(0, len(rows), batch_size):
                    chunk = rows[start:start + batch_size]
                    try:
                        if batch_mode == "batch_array":
                            resp = component._request_with_retry(session, method, url, json=chunk)
                        else:  # ndjson
                            import json as _json
                            body = "\n".join(_json.dumps(r) for r in chunk)
                            headers = {"Content-Type": "application/x-ndjson"}
                            resp = component._request_with_retry(session, method, url, data=body, headers=headers)
                        if write_back:
                            _record_response(resp, len(chunk))
                        if resp.status_code >= 400:
                            failed += len(chunk)
                            failures.append({"status": resp.status_code, "body": resp.text[:500], "chunk_rows": len(chunk)})
                            if failed >= component.fail_after_n_failures:
                                context.log.error(f"Aborting after {failed} failures. Sample: {failures[:3]}")
                                break
                        else:
                            sent += len(chunk)
                    except Exception as e:
                        failed += len(chunk)
                        failures.append({"error": str(e), "chunk_rows": len(chunk)})
                        if write_back:
                            for _ in range(len(chunk)):
                                response_cols.append({
                                    "http_response_status": -1, "http_response_body": str(e)[:4000],
                                    "http_response_json": None, "http_response_value": None,
                                })
                        if failed >= component.fail_after_n_failures:
                            break

            context.log.info(
                f"HTTP {method} ({batch_mode}) complete: sent={sent}, failed={failed}, total_rows={len(df_reset)}"
            )

            metadata = {
                "dagster/row_count": MetadataValue.int(len(df_reset)),
                "requests_sent": MetadataValue.int(sent),
                "requests_failed": MetadataValue.int(failed),
                "batch_mode": MetadataValue.text(batch_mode),
                "url": MetadataValue.text(component.url),
            }
            if failures:
                metadata["failures_sample"] = MetadataValue.json(failures[:10])
            if failed > 0:
                raise RuntimeError(
                    f"HTTP {method}: {failed} of {len(df_reset)} row(s) failed. See `failures_sample` metadata."
                )

            if write_back:
                out_df = df_reset.copy()
                if len(response_cols) == len(out_df):
                    out_df["http_response_status"] = [r["http_response_status"] for r in response_cols]
                    out_df["http_response_body"] = [r["http_response_body"] for r in response_cols]
                    out_df["http_response_json"] = [r["http_response_json"] for r in response_cols]
                    out_df["http_response_value"] = [r["http_response_value"] for r in response_cols]
                return Output(value=out_df, metadata=metadata)
            return Output(value=None, metadata=metadata)

        return Definitions(assets=[dataframe_to_http_asset])


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
