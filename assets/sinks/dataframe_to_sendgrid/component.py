"""DataFrame to SendGrid.

Reverse-ETL sink — batch-PUT DataFrame rows to SendGrid Marketing as
contact upserts via the async bulk contacts endpoint
(``PUT /v3/marketing/contacts``, up to 30,000 contacts per request).

Delegates all wire concerns (auth, batching, response parsing) to
``SendGridResource``; this module only shapes DataFrame rows into
SendGrid contact dicts.

Source shapes (pick one): ``upstream_asset_key`` OR
``source: {kind: sql|csv|inline, ...}``.

Auth: ``resource_key`` (recommended) OR inline ``api_key_env_var``.
"""
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetIn, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable,
    RetryPolicy, asset,
)
from pydantic import Field

_RESERVED_SENDGRID_FIELDS = {
    "email", "first_name", "last_name", "address_line_1", "address_line_2",
    "city", "state_province_region", "country", "postal_code",
    "phone_number_id", "whatsapp", "line", "facebook", "unique_name",
    "anonymous_id", "external_id",
}


class DataframeToSendGridComponent(Component, Model, Resolvable):
    """Push DataFrame rows as SendGrid Marketing contact upserts."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with source:.")
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config. {kind: sql|csv|inline, ...}.")

    resource_key: Optional[str] = Field(default=None,
        description="SendGridResourceComponent key (preferred).")
    api_key_env_var: Optional[str] = Field(default=None,
        description="Inline auth fallback.")

    list_ids: Optional[List[str]] = Field(default=None,
        description="Optional SendGrid marketing list ids to add contacts to.")

    email_column: Optional[str] = Field(default="email",
        description="Column with the contact's email address (required — SendGrid's primary identifier).")

    fields_map: Optional[Dict[str, str]] = Field(default=None,
        description=(
            "Explicit source_col -> sendgrid_field mapping. Reserved "
            "SendGrid fields (first_name, last_name, city, ...) land at "
            "the top level; everything else nests under `custom_fields` "
            "keyed by field ID (see `custom_field_ids` for id mapping)."
        ),
    )
    custom_field_ids: Optional[Dict[str, str]] = Field(default=None,
        description=(
            "Mapping of destination_key -> SendGrid custom field ID. "
            "Required for any fields_map target that isn't a SendGrid "
            "reserved field. Look these up via `GET /v3/marketing/field_definitions`."
        ),
    )

    reserved_field_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns are already named after SendGrid reserved fields.")

    batch_size: Optional[int] = Field(default=None,
        description="Contacts per bulk-import request. Default 30,000 (SendGrid max).")
    dry_run: bool = Field(default=False)
    request_timeout_seconds: int = Field(default=60)

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
        if not self.email_column:
            raise ValueError("email_column is required (SendGrid contacts must carry email).")

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
            sg = _get_resource(cfg, context)
            records = df.to_dict(orient="records")
            contacts = _build_contacts(records, cfg)
            summary = sg.upsert_contacts_bulk(
                contacts, list_ids=cfg.list_ids,
                batch_size=cfg.batch_size or 30000, dry_run=cfg.dry_run,
                logger=context.log,
            )
            context.log.info(
                f"SendGrid contacts: {summary['sent']}/{total} in "
                f"{summary['batches']} imports ({len(summary['job_ids'])} job ids)"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "contacts_sent": MetadataValue.int(summary["sent"]),
                "contacts_failed": MetadataValue.int(summary["failed"]),
                "batches": MetadataValue.int(summary["batches"]),
                "job_ids": MetadataValue.json(summary["job_ids"]),
                "list_ids": MetadataValue.json(cfg.list_ids or []),
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
            def sendgrid_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[sendgrid_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def sendgrid_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[sendgrid_export_inline])


def _get_resource(cfg: DataframeToSendGridComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    from dagster_community_components import SendGridResource
    return SendGridResource(
        api_key_env_var=cfg.api_key_env_var,
        request_timeout_seconds=cfg.request_timeout_seconds,
    )


def _coerce(v: Any) -> Any:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None: v = v.tz_localize("UTC")
        return v.isoformat()
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _build_contacts(records: List[Dict[str, Any]], cfg: DataframeToSendGridComponent) -> List[Dict[str, Any]]:
    """Shape records into SendGrid contact dicts. Reserved SendGrid fields
    (first_name/last_name/city/...) land at top level; everything else
    goes under `custom_fields` keyed by SendGrid field ID."""
    contacts: List[Dict[str, Any]] = []
    custom_ids = cfg.custom_field_ids or {}
    for row in records:
        email = _coerce(row.get(cfg.email_column))
        if email is None:
            continue
        c: Dict[str, Any] = {"email": str(email)}
        cf: Dict[str, Any] = {}

        if cfg.fields_map:
            for src, dst in cfg.fields_map.items():
                if src == cfg.email_column: continue
                v = _coerce(row.get(src))
                if v is None: continue
                if dst in _RESERVED_SENDGRID_FIELDS:
                    c[dst] = v
                elif dst in custom_ids:
                    cf[custom_ids[dst]] = v
                else:
                    # No id mapping — put it in custom_fields keyed by name
                    # (SendGrid will 400 unless the id is valid; user should
                    # supply custom_field_ids).
                    cf[dst] = v
        else:
            # Legacy pass-through: any column named after a SendGrid
            # reserved field lands at top level; user must supply
            # reserved_field_columns to opt-in per-column.
            cols = cfg.reserved_field_columns or []
            for col in cols:
                if col == cfg.email_column: continue
                v = _coerce(row.get(col))
                if v is not None and col in _RESERVED_SENDGRID_FIELDS:
                    c[col] = v
        if cf:
            c["custom_fields"] = cf
        contacts.append(c)
    return contacts


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


def _resolve_source_df(cfg: DataframeToSendGridComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
    src = cfg.source or {}
    kind = (src.get("kind") or "").lower()
    if kind == "sql":
        q = src.get("query")
        if not q: raise ValueError("source kind=sql requires 'query'")
        rk = src.get("resource_key")
        if rk:
            r = getattr(exec_ctx.resources, rk)
            if hasattr(r, "get_engine"): return pd.read_sql(q, r.get_engine())
            if hasattr(r, "get_connection"):
                c = r.get_connection()
                if hasattr(c, "execute") and hasattr(c, "df"): return c.execute(q).df()
                return pd.read_sql(q, c)
            raise ValueError(f"source resource {rk!r} needs .get_engine() or .get_connection()")
        env = src.get("database_url_env_var")
        if env:
            from sqlalchemy import create_engine
            u = os.environ.get(env, "")
            if not u: raise ValueError(f"env var {env!r} unset")
            return pd.read_sql(q, create_engine(u))
        raise ValueError("source kind=sql requires resource_key OR database_url_env_var")
    if kind == "csv":
        p = src.get("path")
        if not p: raise ValueError("source kind=csv requires 'path'")
        return pd.read_csv(p, **(src.get("read_csv_kwargs") or {}))
    if kind == "inline":
        return pd.DataFrame(src.get("rows") or [])
    raise ValueError(f"source kind={kind!r} not supported")
