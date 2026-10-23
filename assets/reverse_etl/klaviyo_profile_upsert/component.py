"""Klaviyo Profile Upsert.

Reverse-ETL sink — POSTs profile updates to Klaviyo via the async bulk
profile-import job endpoint. Delegates all wire concerns (auth, batch
limits, response parsing) to ``KlaviyoResource``; this module only
shapes DataFrame rows into Klaviyo profile dicts.

Source shapes (pick one): ``upstream_asset_key`` OR
``source: {kind: sql|csv|inline, ...}``.

Auth: ``resource_key`` (recommended, references a
``KlaviyoResourceComponent``) OR inline ``api_key_env_var`` +
``api_revision`` fields for one-off use.
"""
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetIn, AssetKey, Backoff, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable,
    RetryPolicy, asset,
)
from pydantic import Field

_RESERVED_KLAVIYO_ATTRS = {
    "email", "phone_number", "external_id", "first_name", "last_name",
    "organization", "title", "image", "location",
}



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


class KlaviyoProfileUpsertComponent(Component, Model, Resolvable):
    """Push DataFrame rows as Klaviyo profile updates via bulk-import jobs."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with source:.")
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config. {kind: sql|csv|inline, ...}.")

    resource_key: Optional[str] = Field(default=None,
        description="KlaviyoResourceComponent key (preferred).")
    api_key_env_var: Optional[str] = Field(default=None,
        description="Inline auth fallback.")
    api_revision: Optional[str] = Field(default=None,
        description="Inline auth: API revision date (Klaviyo pins behavior per date).")

    list_id: Optional[str] = Field(default=None,
        description="Optional Klaviyo list id to add all profiles to.")

    fields_map: Optional[Dict[str, str]] = Field(default=None,
        description=(
            "Explicit source_col -> klaviyo_field mapping. Reserved fields "
            "(email, phone_number, external_id, first_name, last_name, "
            "organization, title, image, location) land at the top level; "
            "everything else is nested under `properties`."
        ),
    )
    email_column: Optional[str] = Field(default="email",
        description="Column with the user's email address (Klaviyo's primary identifier for most workflows).")
    phone_column: Optional[str] = Field(default=None,
        description="Column with the user's phone number (E.164 format).")
    external_id_column: Optional[str] = Field(default=None,
        description="Column with your customer id (Klaviyo secondary identifier).")

    property_columns: Optional[List[str]] = Field(default=None,
        description=(
            "For legacy pass-through (no fields_map): which columns become "
            "Klaviyo custom properties. Default: all columns except the "
            "identifier columns."
        ),
    )

    batch_size: Optional[int] = Field(default=None,
        description="Profiles per bulk-import job. Default 10,000 (Klaviyo max).")
    dry_run: bool = Field(default=False)
    request_timeout_seconds: int = Field(default=30)

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
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
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("Supply exactly ONE of upstream_asset_key OR source:")
        if not self.resource_key and not self.api_key_env_var:
            raise ValueError("Supply resource_key OR api_key_env_var.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else Backoff.LINEAR,
            )

        cfg = self
        rrks: set = set()
        if cfg.resource_key:
            rrks.add(cfg.resource_key)
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk:
                rrks.add(sql_rk)

        def _run(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")

            df = _filter_df_to_partition(df, context, cfg.partition_date_column, cfg.partition_static_column)
            total = len(df)
            klav = _get_resource(cfg, context)
            records = df.to_dict(orient="records")
            profiles = _build_profiles(records, cfg)
            summary = klav.upsert_profiles_bulk(
                profiles, list_id=cfg.list_id,
                batch_size=cfg.batch_size or 10000, dry_run=cfg.dry_run,
                logger=context.log,
            )
            context.log.info(
                f"Klaviyo bulk-import: {summary['sent']}/{total} profiles in "
                f"{summary['batches']} jobs ({len(summary['job_ids'])} job ids)"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "profiles_sent": MetadataValue.int(summary["sent"]),
                "profiles_failed": MetadataValue.int(summary["failed"]),
                "batches": MetadataValue.int(summary["batches"]),
                "job_ids": MetadataValue.json(summary["job_ids"]),
                "list_id": MetadataValue.text(cfg.list_id or ""),
                "dry_run": MetadataValue.bool(cfg.dry_run),
            })

        if cfg.upstream_asset_key:
            uk = AssetKey(cfg.upstream_asset_key.split("/"))

            @asset(freshness_policy=_freshness_policy_obj, name=cfg.asset_name, ins={"upstream": AssetIn(key=uk)},
                   group_name=cfg.group_name, description=cfg.description,
                   deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                   owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
                   retry_policy=retry_policy, partitions_def=partitions_def,
                   required_resource_keys=rrks or None)
            def klaviyo_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[klaviyo_export_upstream])

        @asset(freshness_policy=_freshness_policy_obj, name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def klaviyo_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[klaviyo_export_inline])


# Self-contained: `KlaviyoResource` is inlined from resources/klaviyo_resource (a component.py must not import
# another component -- the CLI copies only this file). Keep in sync with that resource.
from typing import Any, Dict, List, Optional  # noqa: E402,F401
import dagster as dg  # noqa: E402
from pydantic import Field  # noqa: E402,F401


_DEFAULT_REVISION = "2024-10-15"
_MAX_PROFILES_PER_JOB = 10000


class KlaviyoResource(dg.ConfigurableResource):
    """Klaviyo REST API workhorse — auth + async bulk operations."""

    api_key_env_var: str = Field(
        default="KLAVIYO_API_KEY",
        description="Env var holding the Klaviyo private API key (starts with `pk_`).",
    )
    api_revision: str = Field(
        default=_DEFAULT_REVISION,
        description=(
            "Klaviyo API revision date sent in the `revision` header. "
            "Klaviyo pins API behavior per date; bump this when new "
            "revisions add fields you need."
        ),
    )
    base_url: str = Field(
        default="https://a.klaviyo.com",
        description="Klaviyo API base URL (rarely overridden).",
    )
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        token = os.environ.get(self.api_key_env_var)
        if not token:
            raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Klaviyo API key.")
        return token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Klaviyo-API-Key {self._token()}",
            "revision": self.api_revision,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Klaviyo POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def upsert_profiles_bulk(
        self,
        profiles: List[Dict[str, Any]],
        list_id: Optional[str] = None,
        batch_size: int = _MAX_PROFILES_PER_JOB,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-create or update Klaviyo profiles via async bulk-import jobs.

        Args:
            profiles: List of profile dicts. Each MUST carry at least one of
                ``email``, ``phone_number``, or ``external_id`` at the top
                level. Any other keys become custom properties (unless they
                match Klaviyo's reserved attribute names).
            list_id: Optional Klaviyo list id to add all profiles to.
            batch_size: Profiles per job (default = 10,000 = Klaviyo max).
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger.

        Returns:
            {sent, failed, batches, job_ids} — Klaviyo processes jobs
            asynchronously; job_ids can be polled via
            ``GET /api/profile-bulk-import-jobs/{id}`` if you need to
            confirm completion.
        """
        total = len(profiles)
        sent = failed = batches = 0
        job_ids: List[str] = []
        if total == 0:
            if logger is not None:
                logger.warning("Klaviyo profile-bulk-import: no rows.")
            return {"sent": 0, "failed": 0, "batches": 0, "job_ids": []}
        batch_size = min(batch_size, _MAX_PROFILES_PER_JOB)

        for start in range(0, total, batch_size):
            chunk = profiles[start : start + batch_size]
            data_list = [{"type": "profile", "attributes": p} for p in chunk]
            body: Dict[str, Any] = {
                "data": {
                    "type": "profile-bulk-import-job",
                    "attributes": {"profiles": {"data": data_list}},
                }
            }
            if list_id:
                body["data"]["relationships"] = {
                    "lists": {"data": [{"type": "list", "id": list_id}]}
                }
            if dry_run:
                if logger is not None:
                    logger.info(f"[dry_run] Would POST {len(chunk)} profiles to Klaviyo bulk-import")
                sent += len(chunk); batches += 1
                continue
            resp = self._post_raw("/api/profile-bulk-import-jobs", body)
            batches += 1
            if resp.status_code in (200, 201, 202):
                sent += len(chunk)
                try:
                    j = resp.json()
                    jid = (j.get("data") or {}).get("id")
                    if jid:
                        job_ids.append(jid)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"Klaviyo POST failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")

        return {"sent": sent, "failed": failed, "batches": batches, "job_ids": job_ids}



def _get_resource(cfg: KlaviyoProfileUpsertComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    return KlaviyoResource(
        api_key_env_var=cfg.api_key_env_var,
        api_revision=cfg.api_revision or "2024-10-15",
        request_timeout_seconds=cfg.request_timeout_seconds,
    )


def _coerce(v: Any) -> Any:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None:
            v = v.tz_localize("UTC")
        return v.isoformat()
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _build_profiles(records: List[Dict[str, Any]], cfg: KlaviyoProfileUpsertComponent) -> List[Dict[str, Any]]:
    """Shape records into Klaviyo profile-attribute dicts. Reserved fields
    land at top level; anything else nests under `properties`."""
    profiles: List[Dict[str, Any]] = []
    for row in records:
        attrs: Dict[str, Any] = {}
        props: Dict[str, Any] = {}

        # Identifiers — pull from configured columns
        for col_field, target in [
            (cfg.email_column, "email"),
            (cfg.phone_column, "phone_number"),
            (cfg.external_id_column, "external_id"),
        ]:
            if col_field:
                v = _coerce(row.get(col_field))
                if v is not None:
                    attrs[target] = v

        # Field mapping
        if cfg.fields_map:
            for src, dst in cfg.fields_map.items():
                if src in (cfg.email_column, cfg.phone_column, cfg.external_id_column):
                    continue  # already handled above
                v = _coerce(row.get(src))
                if v is None:
                    continue
                if dst in _RESERVED_KLAVIYO_ATTRS:
                    attrs[dst] = v
                else:
                    props[dst] = v
        else:
            # Legacy pass-through
            skip_cols = {cfg.email_column, cfg.phone_column, cfg.external_id_column}
            prop_cols = cfg.property_columns
            if prop_cols is None:
                prop_cols = [c for c in row.keys() if c not in skip_cols]
            for c in prop_cols:
                v = _coerce(row.get(c))
                if v is None:
                    continue
                if c in _RESERVED_KLAVIYO_ATTRS:
                    attrs[c] = v
                else:
                    props[c] = v

        # Must have at least one identifier
        if not any(k in attrs for k in ("email", "phone_number", "external_id")):
            continue
        if props:
            attrs["properties"] = props
        profiles.append(attrs)
    return profiles


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    if not partition_type:
        return None
    from dagster import (DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                         MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                         StaticPartitionsDefinition, DynamicPartitionsDefinition)
    _pt = partition_type
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if _pt in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={_pt!r} requires partition_start.")
    if _pt == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if _pt == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if _pt == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if _pt == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if _pt == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if _pt == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name: raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


def _resolve_source_df(cfg: KlaviyoProfileUpsertComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
    src = cfg.source or {}
    kind = (src.get("kind") or "").lower()
    if kind == "sql":
        query = src.get("query")
        if not query: raise ValueError("source kind=sql requires 'query'")
        rk = src.get("resource_key")
        if rk:
            r = getattr(exec_ctx.resources, rk)
            if hasattr(r, "get_engine"): return pd.read_sql(query, r.get_engine())
            if hasattr(r, "get_connection"):
                conn = r.get_connection()
                if hasattr(conn, "execute") and hasattr(conn, "df"):
                    return conn.execute(query).df()
                return pd.read_sql(query, conn)
            raise ValueError(f"source resource {rk!r} needs .get_engine() or .get_connection()")
        env = src.get("database_url_env_var")
        if env:
            from sqlalchemy import create_engine
            url = os.environ.get(env, "")
            if not url: raise ValueError(f"env var {env!r} unset")
            return pd.read_sql(query, create_engine(url))
        raise ValueError("source kind=sql requires resource_key OR database_url_env_var")
    if kind == "csv":
        p = src.get("path")
        if not p: raise ValueError("source kind=csv requires 'path'")
        return pd.read_csv(p, **(src.get("read_csv_kwargs") or {}))
    if kind == "inline":
        return pd.DataFrame(src.get("rows") or [])
    raise ValueError(f"source kind={kind!r} not supported")


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


# BEGIN partition row-filter helper (inlined per component; keep copies identical, see tests/test_partition_port_ingestion.py)
from typing import Literal  # noqa: E402,F401


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the frame is returned unchanged."""
    import pandas as pd

    if not (context.has_partition_key or context.has_partition_key_range):
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
# END partition row-filter helper
