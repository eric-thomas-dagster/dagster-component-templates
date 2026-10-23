"""AddressStandardize.

Parse messy address strings into structured components (house_number,
road, unit, city, state, postcode, postcode_plus4, country) — a free /
open-source address parser.

Providers:

  - `libpostal` — local, offline, MIT license. Best parser quality.
                   Requires the libpostal C library + `pypostal` Python bindings.
  - `geoapify`  — commercial API (free tier ~3k requests/day).
                   Set GEOAPIFY_API_KEY.
  - `nominatim` — OSM / Nominatim free geocode + parse. Slower (1 req/sec
                   rate limit). Set NOMINATIM_USER_AGENT.
  - `regex`     — naive US street-address regex (no dep, no network).
                   ZIP+4-aware.

For USPS CASS-certified DPV / ZIP+4 validation, use a licensed commercial
vendor — none of the free providers above are CASS-certified.
"""
import os
import re
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


_VALID_PROVIDERS = ("libpostal", "geoapify", "nominatim", "regex")

_FIELDS = (
    "house_number",
    "road",
    "unit",
    "city",
    "state",
    "postcode",
    "postcode_plus4",
    "country",
)


_REGEX_PATTERN = re.compile(
    r"^\s*(?P<house_number>\d+[A-Z]?)\s+"
    r"(?P<road>[^,]+?)"
    r"(?:,\s*(?P<unit>(?:apt|unit|suite|ste|#)\s*\S+))?"
    r"\s*,?\s*(?P<city>[A-Za-z\.\s]+?)\s*,\s*"
    r"(?P<state>[A-Z]{2})\s*"
    r"(?P<postcode>\d{5})"
    r"(?:-(?P<postcode_plus4>\d{4}))?\s*$",
    re.IGNORECASE,
)


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")



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


class AddressStandardizeComponent(Component, Model, Resolvable):
    """Parse messy address strings into structured components (street, city, state, postcode, +4).

    Free / open-source address parsing via libpostal, Geoapify, Nominatim,
    or a built-in US regex fallback.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key (DataFrame). Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    address_column: Union[str, int] = Field(
        description="Column containing the raw address string (one address per row).",
    )
    provider: str = Field(
        default="regex",
        description=f"Parser provider. One of: {list(_VALID_PROVIDERS)}.",
    )
    api_key_env_var: str = Field(
        default="GEOAPIFY_API_KEY",
        description="Env var for the provider's API key (geoapify). Ignored for libpostal / regex.",
    )
    user_agent: str = Field(
        default="dagster-address-standardize",
        description="User-Agent header sent to Nominatim. Must be unique per app per ToS.",
    )
    output_prefix: str = Field(
        default="addr_",
        description="Prefix for emitted component columns (addr_house_number, addr_road, ...).",
    )
    keep_original: bool = Field(
        default=True,
        description="Keep the original address column in the output.",
    )
    rate_limit_seconds: float = Field(
        default=1.0,
        description="Per-row delay for API-rate-limited providers (Nominatim defaults to 1.0).",
    )
    timeout_seconds: int = Field(
        default=10,
        description="HTTP timeout per request for API providers.",
    )

    group_name: Optional[str] = Field(default=None)

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
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("AddressStandardizeComponent: set exactly one of `upstream_asset_key` or `source`.")
        _self = self
        if self.provider not in _VALID_PROVIDERS:
            raise ValueError(
                f"address_standardize: provider must be one of {list(_VALID_PROVIDERS)}; "
                f"got {self.provider!r}."
            )

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "geo"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(self.asset_name),
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))} if self.upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            group_name=self.group_name,
            description=self.description or f"Standardize addresses in {self.address_column!r} via {self.provider!r}.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(self.source, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
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
            df = upstream.copy()
            col = _self.address_column
            prefix = _self.output_prefix
            if col not in df.columns:
                raise KeyError(
                    f"address_standardize: address_column {col!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )

            for fld in _FIELDS:
                df[f"{prefix}{fld}"] = None

            parser = _self._build_parser(context)

            n_ok = 0
            n_fail = 0
            for i, raw in enumerate(df[col]):
                if raw is None or (isinstance(raw, float) and pd.isna(raw)) or str(raw).strip() == "":
                    n_fail += 1
                    continue
                try:
                    parsed = parser(str(raw))
                except Exception as e:
                    context.log.warning(f"address_standardize row {i}: {type(e).__name__}: {e}")
                    n_fail += 1
                    continue
                if not parsed:
                    n_fail += 1
                    continue
                for fld in _FIELDS:
                    if fld in parsed and parsed[fld] is not None:
                        df.at[i, f"{prefix}{fld}"] = parsed[fld]
                n_ok += 1

            if not _self.keep_original:
                df = df.drop(columns=[col])

            context.log.info(
                f"address_standardize ({_self.provider}): parsed {n_ok}/{len(df)} rows ({n_fail} failed)."
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "provider": MetadataValue.text(_self.provider),
                "address_column": MetadataValue.text(col),
                "parsed_ok": MetadataValue.int(n_ok),
                "parsed_failed": MetadataValue.int(n_fail),
            })
            return df

        return Definitions(assets=[_asset])

    def _build_parser(self, context):
        """Return a `parser(raw_address) -> dict` callable for the configured provider."""
        provider = self.provider

        if provider == "libpostal":
            try:
                from postal.parser import parse_address as _libpostal_parse
            except ImportError as e:
                raise ImportError(
                    "libpostal provider requires the libpostal C library + "
                    "Python bindings: `brew install libpostal` (or apt-get "
                    "install libpostal-dev) + `pip install pypostal`."
                ) from e

            _LP_MAP = {
                "house_number": "house_number",
                "road": "road",
                "unit": "unit",
                "po_box": "unit",
                "level": "unit",
                "city": "city",
                "city_district": "city",
                "state": "state",
                "state_district": "state",
                "postcode": "postcode",
                "country": "country",
                "country_region": "country",
            }

            def _parse(raw: str) -> Dict[str, Any]:
                out: Dict[str, Any] = {}
                for value, label in _libpostal_parse(raw):
                    key = _LP_MAP.get(label)
                    if key and key not in out:
                        out[key] = value
                pc = out.get("postcode")
                if pc and "-" in pc:
                    z, p4 = pc.split("-", 1)
                    out["postcode"] = z.strip()
                    out["postcode_plus4"] = p4.strip()
                return out

            return _parse

        if provider == "geoapify":
            api_key = os.environ.get(self.api_key_env_var)
            if not api_key:
                raise OSError(
                    f"geoapify provider needs env var {self.api_key_env_var!r}. "
                    "Free tier ~3k requests/day at https://www.geoapify.com/."
                )
            import time
            import requests

            def _parse(raw: str) -> Dict[str, Any]:
                resp = requests.get(
                    "https://api.geoapify.com/v1/geocode/search",
                    params={"text": raw, "format": "json", "apiKey": api_key},
                    timeout=self.timeout_seconds,
                )
                if resp.status_code == 429:
                    time.sleep(2.0)
                    resp = requests.get(
                        "https://api.geoapify.com/v1/geocode/search",
                        params={"text": raw, "format": "json", "apiKey": api_key},
                        timeout=self.timeout_seconds,
                    )
                resp.raise_for_status()
                results = (resp.json() or {}).get("results") or []
                if not results:
                    return {}
                r = results[0]
                pc = r.get("postcode") or ""
                p4 = ""
                if pc and "-" in pc:
                    pc, p4 = pc.split("-", 1)
                return {
                    "house_number": r.get("housenumber"),
                    "road": r.get("street"),
                    "unit": r.get("unit"),
                    "city": r.get("city") or r.get("town") or r.get("village"),
                    "state": r.get("state_code") or r.get("state"),
                    "postcode": pc,
                    "postcode_plus4": p4 or None,
                    "country": r.get("country_code") or r.get("country"),
                }

            return _parse

        if provider == "nominatim":
            ua = os.environ.get("NOMINATIM_USER_AGENT") or self.user_agent
            import time
            import requests

            def _parse(raw: str) -> Dict[str, Any]:
                resp = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": raw, "format": "json", "addressdetails": 1, "limit": 1},
                    headers={"User-Agent": ua},
                    timeout=self.timeout_seconds,
                )
                resp.raise_for_status()
                results = resp.json() or []
                time.sleep(max(self.rate_limit_seconds, 0.0))
                if not results:
                    return {}
                addr = results[0].get("address") or {}
                pc = addr.get("postcode") or ""
                p4 = ""
                if pc and "-" in pc:
                    pc, p4 = pc.split("-", 1)
                return {
                    "house_number": addr.get("house_number"),
                    "road": addr.get("road"),
                    "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet"),
                    "state": addr.get("state"),
                    "postcode": pc,
                    "postcode_plus4": p4 or None,
                    "country": addr.get("country_code") or addr.get("country"),
                }

            return _parse

        def _parse(raw: str) -> Dict[str, Any]:
            m = _REGEX_PATTERN.match(raw)
            if not m:
                return {}
            d = {k: (v.strip() if isinstance(v, str) else v) for k, v in m.groupdict().items()}
            return d

        return _parse

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


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
