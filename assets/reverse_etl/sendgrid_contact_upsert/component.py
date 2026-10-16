"""SendGrid Contact Upsert.

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
    AssetExecutionContext, AssetIn, AssetKey, Backoff, Component, ComponentLoadContext,
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


class SendGridContactUpsertComponent(Component, Model, Resolvable):
    """Push DataFrame rows as SendGrid Marketing contact upserts."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(default=None,
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

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


# Self-contained: `SendGridResource` is inlined from resources/sendgrid_resource (a component.py must not import
# another component -- the CLI copies only this file). Keep in sync with that resource.
from typing import Any, Dict, List, Optional  # noqa: E402,F401
import dagster as dg  # noqa: E402
from pydantic import Field  # noqa: E402,F401


_MAX_CONTACTS_PER_JOB = 30000


class SendGridResource(dg.ConfigurableResource):
    """SendGrid Marketing REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="SENDGRID_API_KEY",
        description="Env var holding the SendGrid API key (starts with `SG.`).")
    base_url: str = Field(default="https://api.sendgrid.com",
        description="SendGrid API base URL. Use https://api.eu.sendgrid.com for the EU regional subuser.")
    request_timeout_seconds: int = Field(default=60)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var, "")
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your SendGrid API key.")
        return t

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _put_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.put(url, json=json_body, headers=self._headers(),
                            timeout=self.request_timeout_seconds)

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"SendGrid POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def upsert_contacts_bulk(
        self, contacts: List[Dict[str, Any]],
        list_ids: Optional[List[str]] = None,
        batch_size: int = _MAX_CONTACTS_PER_JOB,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-upsert SendGrid contacts via PUT /v3/marketing/contacts.

        Args:
            contacts: List of contact dicts. Each MUST have ``email``.
                Optional reserved fields: first_name, last_name,
                address_line_1, city, state_province_region, country,
                postal_code, phone_number_id, whatsapp, line, facebook,
                unique_name. Custom fields go under ``custom_fields:
                {field_id: value}`` (SendGrid uses numeric field IDs).
            list_ids: Optional SendGrid marketing list ids to add contacts to.
            batch_size: Max 30,000 per SendGrid docs.

        Returns: {sent, failed, batches, job_ids}
            SendGrid processes imports asynchronously; poll via
            ``GET /v3/marketing/contacts/imports/{job_id}``.
        """
        total = len(contacts)
        sent = failed = batches = 0
        job_ids: List[str] = []
        if total == 0:
            if logger is not None: logger.warning("SendGrid /v3/marketing/contacts: no contacts.")
            return {"sent": 0, "failed": 0, "batches": 0, "job_ids": []}
        batch_size = min(batch_size, _MAX_CONTACTS_PER_JOB)

        for start in range(0, total, batch_size):
            chunk = contacts[start : start + batch_size]
            body: Dict[str, Any] = {"contacts": chunk}
            if list_ids:
                body["list_ids"] = list_ids
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would PUT {len(chunk)} contacts to SendGrid /v3/marketing/contacts")
                sent += len(chunk); batches += 1; continue
            resp = self._put_raw("/v3/marketing/contacts", body)
            batches += 1
            if resp.status_code in (200, 201, 202):
                sent += len(chunk)
                try:
                    j = resp.json()
                    jid = j.get("job_id") if isinstance(j, dict) else None
                    if jid:
                        job_ids.append(jid)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"SendGrid PUT failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")

        return {"sent": sent, "failed": failed, "batches": batches, "job_ids": job_ids}



def _get_resource(cfg: SendGridContactUpsertComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
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


def _build_contacts(records: List[Dict[str, Any]], cfg: SendGridContactUpsertComponent) -> List[Dict[str, Any]]:
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


def _resolve_source_df(cfg: SendGridContactUpsertComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
