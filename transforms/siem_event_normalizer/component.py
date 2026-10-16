"""SiemEventNormalizerComponent.

Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM.
"""

import json
import os
from typing import Any, Optional, Union, Dict

import dagster as dg
import pandas as pd
from pydantic import ConfigDict, Field



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
        import os
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


class SiemEventNormalizerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM."""

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream DataFrame to normalize. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    schema_name: str = Field(
        alias="schema",
        default="ocsf", description="Target schema: 'ocsf' (Open Cybersecurity Schema Framework) | 'ecs' (Elastic Common Schema)")
    source_kind: str = Field(default="generic", description="Source hint: 'cloudtrail' | 'okta' | 'github' | 'azure' | 'generic'")
    event_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "If set, treat this column as a JSON-encoded event payload and parse it into "
            "the source columns before normalization. Use when the upstream stores raw "
            "events as JSON strings in a single column."
        ),
    )
    timestamp_column: Optional[Union[str, int]] = Field(default=None, description="Source column to map to event timestamp (auto-detect if None)")
    actor_column: Optional[Union[str, int]] = Field(default=None, description="Source column for actor/user (auto-detect if None)")
    action_column: Optional[Union[str, int]] = Field(default=None, description="Source column for action/event name")
    drop_extras: bool = Field(default=False, description="Drop source columns that don't map to the target schema")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_normalize")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)

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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the normalized rows in the materialization metadata.",
    )
    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("SiemEventNormalizerComponent: set exactly one of `upstream_asset_key` or `source`.")
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

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

        _self = self

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['security', 'ocsf', 'ecs']),
            ins=({"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))} if self.upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
            df = kwargs.get("df")
            if df is None:
                df = _ingest_warehouse_query(_self.source, context)
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(df, dict):
                _frames = [v for v in df.values() if isinstance(v, pd.DataFrame)]
                df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # If event_column is set, parse JSON payloads out into individual columns first.
            if _self.event_column and _self.event_column in df.columns:
                parsed = df[_self.event_column].apply(
                    lambda v: json.loads(v) if isinstance(v, str) and v.startswith("{") else {}
                )
                expanded = pd.json_normalize(parsed.tolist())
                # Concat parsed columns alongside the originals so auto-detect can find them.
                df = pd.concat([df.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)
            # Auto-detect commonly-named fields from each source
            auto_ts = {
                "cloudtrail": "EventTime",
                "okta": "published",
                "github": "@timestamp",
                "azure": "event_timestamp",
                "generic": next((c for c in df.columns if c.lower() in ("timestamp", "@timestamp", "time", "event_time", "eventtime", "occurredat")), None),
            }
            auto_actor = {
                "cloudtrail": "Username",
                "okta": "actor.alternateId",
                "github": "actor",
                "azure": "caller",
                "generic": next((c for c in df.columns if c.lower() in ("user", "username", "actor", "principal", "email")), None),
            }
            auto_action = {
                "cloudtrail": "EventName",
                "okta": "eventType",
                "github": "action",
                "azure": "operation_name",
                "generic": next((c for c in df.columns if c.lower() in ("action", "event_name", "eventname", "eventtype", "operation_name")), None),
            }
            ts_col = _self.timestamp_column or auto_ts.get(_self.source_kind)
            actor_col = _self.actor_column or auto_actor.get(_self.source_kind)
            action_col = _self.action_column or auto_action.get(_self.source_kind)

            out = pd.DataFrame()
            if _self.schema_name == "ocsf":
                # OCSF Activity event: time, actor.user.name, activity_name, raw_data
                out["time"] = df[ts_col] if ts_col and ts_col in df.columns else None
                out["activity_name"] = df[action_col] if action_col and action_col in df.columns else None
                out["actor.user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
                out["metadata.product.vendor_name"] = _self.source_kind
                out["raw_data"] = df.apply(lambda r: r.to_json(), axis=1)
            elif _self.schema_name == "ecs":
                # ECS: @timestamp, event.action, user.name, source
                out["@timestamp"] = df[ts_col] if ts_col and ts_col in df.columns else None
                out["event.action"] = df[action_col] if action_col and action_col in df.columns else None
                out["user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
                out["event.dataset"] = _self.source_kind
                out["event.original"] = df.apply(lambda r: r.to_json(), axis=1)
            else:
                raise ValueError(f"Unknown schema: {_self.schema_name}")

            if not _self.drop_extras:
                # Pass-through original fields (prefixed) for debuggability
                for c in df.columns:
                    if c not in out.columns:
                        out[f"original.{c}"] = df[c]

            df = out

            _preview_metadata = {}
            if _self.include_preview_metadata:
                _prev_df = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                _cols = list(_prev_df.columns)
                _preview_metadata["preview"] = dg.MetadataValue.md(
                    "| " + " | ".join(_cols) + " |\n"
                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +
                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))
                )

            context.add_output_metadata({
                **_preview_metadata,
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "schema": dg.MetadataValue.text(_self.schema_name),
                "source_kind": dg.MetadataValue.text(_self.source_kind),
            })
            return df

        return dg.Definitions(assets=[_asset])
