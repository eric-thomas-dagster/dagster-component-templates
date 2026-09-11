"""DataFrame to Klaviyo.

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
    AssetExecutionContext, AssetIn, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable,
    RetryPolicy, asset,
)
from pydantic import Field

_RESERVED_KLAVIYO_ATTRS = {
    "email", "phone_number", "external_id", "first_name", "last_name",
    "organization", "title", "image", "location",
}


class DataframeToKlaviyoComponent(Component, Model, Resolvable):
    """Push DataFrame rows as Klaviyo profile updates via bulk-import jobs."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None,
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
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("Supply exactly ONE of upstream_asset_key OR source:")
        if not self.resource_key and not self.api_key_env_var:
            raise ValueError("Supply resource_key OR api_key_env_var.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        )
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=self.retry_policy_backoff,  # type: ignore[arg-type]
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

            @asset(name=cfg.asset_name, ins={"upstream": AssetIn(key=uk)},
                   group_name=cfg.group_name, description=cfg.description,
                   deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                   owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
                   retry_policy=retry_policy, partitions_def=partitions_def,
                   required_resource_keys=rrks or None)
            def klaviyo_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[klaviyo_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def klaviyo_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[klaviyo_export_inline])


def _get_resource(cfg: DataframeToKlaviyoComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    from dagster_community_components import KlaviyoResource
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


def _build_profiles(records: List[Dict[str, Any]], cfg: DataframeToKlaviyoComponent) -> List[Dict[str, Any]]:
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


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
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
    if _pt == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name: raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


def _resolve_source_df(cfg: DataframeToKlaviyoComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
