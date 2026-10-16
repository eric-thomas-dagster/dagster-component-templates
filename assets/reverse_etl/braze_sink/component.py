"""Braze Sink.

Reverse-ETL sink — takes a DataFrame (or inline source), shapes each row
into a Braze user attribute update or catalog item, and delegates to a
``BrazeResource`` for the actual wire POST. All batching, response
parsing, and error accounting live on the resource.

Source shapes (pick one):

- ``upstream_asset_key`` — read a DataFrame from another Dagster asset.
- ``source: {kind: sql | csv | inline, ...}`` — inline source config.

Endpoints:

- ``users_track`` (default) — user attribute + custom_attribute updates.
- ``catalogs`` — custom catalog item upsert (requires ``catalog_name``).

Auth:

- ``resource_key`` (recommended) — reference a ``BrazeResourceComponent``.
- Inline ``api_key_env_var`` + ``rest_endpoint`` — sink instantiates an
  ad-hoc ``BrazeResource`` under the hood. Same wire behavior.
"""
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Backoff,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    RetryPolicy,
    asset,
)
from pydantic import Field



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


class BrazeSinkComponent(Component, Model, Resolvable):
    """Push a DataFrame (or inline source) to Braze via the REST API."""

    asset_name: str = Field(description="Dagster asset name")

    # ── Source (exactly one of these) ─────────────────────────────
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually "
            "exclusive with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with "
            "`upstream_asset_key`. Shapes: "
            "{kind: sql, resource_key|database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    # ── Auth (prefer resource_key; inline fallback) ───────────────
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key registered by BrazeResourceComponent — the "
            "recommended way to configure Braze auth. Mutually exclusive "
            "with the inline `api_key_env_var` + `rest_endpoint` fields."
        ),
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Inline fallback: env var holding the Braze REST API key. "
            "Only used when `resource_key` is unset."
        ),
    )
    rest_endpoint: Optional[str] = Field(
        default=None,
        description=(
            "Inline fallback: region-specific Braze REST endpoint URL. "
            "Only used when `resource_key` is unset."
        ),
    )

    # ── Endpoint + column mapping ─────────────────────────────────
    endpoint: Literal["users_track", "catalogs"] = Field(
        default="users_track",
        description="Which Braze endpoint to POST to.",
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description="Required when endpoint='catalogs'.",
    )

    fields_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Explicit source_col -> braze_field_name mapping. When set, "
            "columns are renamed on the way to Braze (e.g. "
            "{db_email: email}). Preferred over `attribute_columns`."
        ),
    )
    user_id_column: Optional[str] = Field(
        default="external_id",
        description=(
            "For endpoint='users_track': column holding the user's primary "
            "identifier. Set to `None` for secondary-identifier-only mode "
            "(rows identified by `email` or `phone` alone via fields_map)."
        ),
    )
    id_type: Literal["external_id", "braze_id", "user_alias"] = Field(
        default="external_id",
        description=(
            "Which Braze primary identifier the user_id_column value maps "
            "to: `external_id` (your customer id — most common), `braze_id` "
            "(Braze's opaque id), or `user_alias` (namespaced — value should "
            "be a `{alias_name, alias_label}` dict)."
        ),
    )
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track' (legacy pass-through mode when "
            "fields_map isn't set): which columns become top-level user "
            "attributes. Default: all columns except user_id_column and "
            "custom_attribute_columns."
        ),
    )
    custom_attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track': columns to send nested under "
            "'custom_attributes' on the user's Braze profile."
        ),
    )
    item_id_column: str = Field(
        default="id",
        description=(
            "For endpoint='catalogs': column holding each catalog item's "
            "unique id."
        ),
    )

    batch_size: Optional[int] = Field(
        default=None,
        description=(
            "Rows per HTTP request. Default = 75 for users_track / 50 "
            "for catalogs (Braze's maxima). Lower values reduce rate-"
            "limit pressure."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (only used with inline auth path).",
    )
    dry_run: bool = Field(
        default=False,
        description="Build payloads + log without POSTing.",
    )

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
            raise ValueError(
                "BrazeSinkComponent: supply exactly ONE of "
                "`upstream_asset_key` OR `source:`."
            )
        if self.endpoint == "catalogs" and not self.catalog_name:
            raise ValueError("endpoint='catalogs' requires catalog_name.")

        use_resource = bool(self.resource_key)
        if not use_resource and not (self.api_key_env_var and self.rest_endpoint):
            raise ValueError(
                "BrazeSinkComponent: supply `resource_key` OR both "
                "`api_key_env_var` + `rest_endpoint`."
            )

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
        required_resource_keys: set = set()
        if use_resource:
            required_resource_keys.add(cfg.resource_key)  # type: ignore[arg-type]
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk:
                required_resource_keys.add(sql_rk)

        # ── Body — data shaping + one resource-method call ──
        def _run(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")

            total = len(df)
            braze = _get_braze_resource(cfg, context)
            records = df.to_dict(orient="records")

            if cfg.endpoint == "users_track":
                users = _build_users_list(records, cfg)
                summary = braze.track_users(
                    users,
                    batch_size=cfg.batch_size or 75,
                    dry_run=cfg.dry_run,
                    logger=context.log,
                )
            else:
                items = _build_items_list(records, cfg)
                summary = braze.upsert_catalog_items(
                    cfg.catalog_name,
                    items,
                    batch_size=cfg.batch_size or 50,
                    dry_run=cfg.dry_run,
                    logger=context.log,
                )

            soft = summary["soft_errors"]
            soft_note = f" (soft errors: {soft})" if soft else ""
            dry_note = " — dry_run" if cfg.dry_run else ""
            context.log.info(
                f"Braze export ({cfg.endpoint}): "
                f"{summary['sent']}/{total} rows in {summary['batches']} batches"
                f"{soft_note}{dry_note}"
            )
            return MaterializeResult(
                metadata={
                    "rows_total": MetadataValue.int(total),
                    "rows_sent": MetadataValue.int(summary["sent"]),
                    "rows_failed": MetadataValue.int(summary["failed"]),
                    "soft_errors": MetadataValue.int(summary["soft_errors"]),
                    "batches": MetadataValue.int(summary["batches"]),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "batch_size": MetadataValue.int(cfg.batch_size or (75 if cfg.endpoint == "users_track" else 50)),
                    "dry_run": MetadataValue.bool(cfg.dry_run),
                }
            )

        # ── Two asset shapes based on source ──
        if cfg.upstream_asset_key:
            upstream_key = AssetKey(cfg.upstream_asset_key.split("/"))

            @asset(
                name=cfg.asset_name,
                ins={"upstream": AssetIn(key=upstream_key)},
                group_name=cfg.group_name,
                description=cfg.description,
                deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                owners=cfg.owners,
                tags=cfg.asset_tags,
                kinds=cfg.kinds,
                retry_policy=retry_policy,
                partitions_def=partitions_def,
                required_resource_keys=required_resource_keys or None,
            )
            def braze_export_upstream(
                context: AssetExecutionContext, upstream: pd.DataFrame
            ) -> MaterializeResult:
                return _run(context, upstream)

            return Definitions(assets=[braze_export_upstream])

        @asset(
            name=cfg.asset_name,
            group_name=cfg.group_name,
            description=cfg.description,
            deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
            owners=cfg.owners,
            tags=cfg.asset_tags,
            kinds=cfg.kinds,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys or None,
        )
        def braze_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            df = _resolve_source_df(cfg, context)
            return _run(context, df)

        return Definitions(assets=[braze_export_inline])


# ─── Helpers ──────────────────────────────────────────────────────


# Self-contained: `BrazeResource` is inlined from resources/braze_resource (a component.py must not import
# another component -- the CLI copies only this file). Keep in sync with that resource.
from typing import Any, Dict, List, Optional  # noqa: E402,F401
import dagster as dg  # noqa: E402
from pydantic import Field  # noqa: E402,F401


_CATALOG_ID_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
def _validate_catalog_id(raw_id: str) -> Optional[str]:
    """Braze catalog item IDs: strings, max 250 chars, [A-Za-z0-9_-] only.
    Returns None (skip) for invalid IDs."""
    if not raw_id or len(raw_id) > 250:
        return None
    for ch in raw_id:
        if ch not in _CATALOG_ID_ALLOWED:
            return None
    return raw_id


class BrazeResource(dg.ConfigurableResource):
    """Braze REST API workhorse — holds auth + endpoint, exposes typed
    batch operations for downstream sinks."""

    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding the Braze REST API key.",
    )
    rest_endpoint: str = Field(
        description=(
            "Region-specific Braze REST endpoint URL "
            "(e.g. https://rest.iad-01.braze.com). Look up in the Braze "
            "dashboard under Settings → REST API Keys."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (seconds).",
    )

    # ─── Wire internals ────────────────────────────────────────────

    def _token(self) -> str:
        import os
        token = os.environ.get(self.api_key_env_var)
        if not token:
            raise dg.Failure(
                f"env var {self.api_key_env_var!r} is empty or unset — "
                f"set your Braze REST API key."
            )
        return token

    def _base_url(self) -> str:
        return self.rest_endpoint.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    def _post_raw(self, path: str, json_body: Any) -> Any:
        """Low-level POST helper — returns the requests.Response object.
        Callers handle status + parsing."""
        import requests
        path = "/" + path.lstrip("/")
        url = self._base_url() + path
        return requests.post(
            url, json=json_body, headers=self._headers(),
            timeout=self.request_timeout_seconds,
        )

    # ─── Public: raw escape hatch ─────────────────────────────────

    def post(self, path: str, json_body: Any) -> Any:
        """POST ``json_body`` to ``{rest_endpoint}{path}``. Returns the
        parsed JSON response. Raises ``dg.Failure`` on non-2xx.

        Escape hatch for endpoints not covered by the typed methods
        below (campaigns.trigger, canvas.trigger, subscriptions.status,
        etc.). Auto-batching / error accounting NOT applied — that's
        what ``track_users`` and ``upsert_catalog_items`` are for.
        """
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            body = (resp.text or "")[:500]
            raise dg.Failure(
                f"Braze POST {path} failed: HTTP {resp.status_code} body={body}"
            )
        try:
            return resp.json()
        except Exception:
            return None

    # ─── Public: typed batch operations ──────────────────────────

    def track_users(
        self,
        attributes: List[Dict[str, Any]],
        batch_size: int = 75,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch POST to ``/users/track``.

        Auto-batches to Braze's max of 75 attribute objects per call
        (documented at https://www.braze.com/docs/api/endpoints/user_data/post_user_track).
        Handles Braze's 200-with-``errors`` non-fatal error shape.

        Args:
            attributes: List of user attribute dicts. Each must carry a
                primary identifier (``external_id`` / ``braze_id`` /
                ``user_alias``) OR a secondary identifier (``email`` /
                ``phone``). Callers are responsible for shaping.
            batch_size: Rows per POST (default 75, Braze's max).
                Lower reduces rate-limit pressure at the cost of more
                requests.
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger for warnings.

        Returns:
            {sent, failed, soft_errors, batches, error_samples} — send
            counts (both hard 4xx/5xx failures AND soft 200-with-errors
            counted separately), batch count, and up to 3 sampled soft-
            error dicts for debugging.
        """
        return self._run_batches(
            path="/users/track",
            key_name="attributes",
            all_rows=attributes,
            batch_size=min(batch_size, 75),
            dry_run=dry_run,
            logger=logger,
        )

    def upsert_catalog_items(
        self,
        catalog_name: str,
        items: List[Dict[str, Any]],
        batch_size: int = 50,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch POST to ``/catalogs/{catalog_name}/items``.

        Auto-batches to Braze's max of 50 items per call. Validates each
        item's ``id`` field (must exist, be a string ≤250 chars matching
        ``[A-Za-z0-9_-]+``) and drops invalid items with a warning —
        Braze would otherwise reject the whole batch on one bad id.

        Args:
            catalog_name: Braze catalog name.
            items: List of item dicts. Each must carry an ``id`` field
                (any other fields become the item's data).
            batch_size: Items per POST (default 50, Braze's max).
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger for warnings.

        Returns:
            Same shape as track_users.
        """
        # Pre-validate item ids; drop invalid and warn.
        valid_items: List[Dict[str, Any]] = []
        dropped_ids: List[str] = []
        for item in items:
            raw_id = item.get("id")
            if raw_id is None:
                continue
            clean_id = _validate_catalog_id(str(raw_id))
            if clean_id is None:
                dropped_ids.append(str(raw_id)[:60])
                continue
            item["id"] = clean_id
            valid_items.append(item)
        if dropped_ids and logger is not None:
            logger.warning(
                f"Braze catalog: dropped {len(dropped_ids)} items with invalid ids "
                f"(sample: {dropped_ids[:3]}). IDs must be ≤250 chars, letters/digits/_-  only."
            )

        return self._run_batches(
            path=f"/catalogs/{catalog_name}/items",
            key_name="items",
            all_rows=valid_items,
            batch_size=min(batch_size, 50),
            dry_run=dry_run,
            logger=logger,
        )

    # ─── Shared batch runner ─────────────────────────────────────

    def _run_batches(
        self,
        path: str,
        key_name: str,
        all_rows: List[Dict[str, Any]],
        batch_size: int,
        dry_run: bool,
        logger: Any,
    ) -> Dict[str, Any]:
        """Shared POST loop for track_users + upsert_catalog_items.
        Returns {sent, failed, soft_errors, batches, error_samples}."""
        total = len(all_rows)
        sent = failed = batches = soft_errors = 0
        error_samples: List[Any] = []

        if total == 0:
            if logger is not None:
                logger.warning(f"Braze {path}: no rows to send.")
            return {"sent": 0, "failed": 0, "soft_errors": 0, "batches": 0, "error_samples": []}

        for start in range(0, total, batch_size):
            chunk = all_rows[start : start + batch_size]
            payload = {key_name: chunk}

            if dry_run:
                if logger is not None:
                    logger.info(f"[dry_run] Would POST {len(chunk)} rows to {path}")
                sent += len(chunk)
                batches += 1
                continue

            resp = self._post_raw(path, payload)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                # Braze can return 2xx with a non-fatal `errors` array.
                # Batch succeeded overall — un-affected rows applied —
                # but affected rows didn't. Surface as warnings.
                try:
                    payload_json = resp.json()
                except Exception:
                    payload_json = None
                if isinstance(payload_json, dict):
                    errs = payload_json.get("errors")
                    if isinstance(errs, list) and errs:
                        soft_errors += len(errs)
                        for e in errs[:3]:
                            if logger is not None:
                                logger.warning(f"Braze soft error: {e}")
                        # Save up to 3 samples for the returned metadata.
                        error_samples.extend(errs[: max(0, 3 - len(error_samples))])
            else:
                failed += len(chunk)
                body = (resp.text or "")[:400]
                if logger is not None:
                    logger.warning(
                        f"Braze POST {path} failed: HTTP {resp.status_code} body={body}"
                    )

        return {
            "sent": sent,
            "failed": failed,
            "soft_errors": soft_errors,
            "batches": batches,
            "error_samples": error_samples,
        }



def _get_braze_resource(
    cfg: BrazeSinkComponent, context: AssetExecutionContext
) -> Any:
    """Return a BrazeResource — either from context.resources[<key>] or
    an ad-hoc instance built from inline auth fields."""
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    # Ad-hoc — import locally to keep the sink module light when the
    # resource component isn't installed alongside.
    return BrazeResource(
        api_key_env_var=cfg.api_key_env_var,
        rest_endpoint=cfg.rest_endpoint,
        request_timeout_seconds=cfg.request_timeout_seconds,
    )


def _build_partitions_def(
    partition_type: Optional[str],
    partition_start: Optional[str],
    partition_values: Optional[str],
    dynamic_partition_name: Optional[str],
):
    if not partition_type:
        return None
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    _pt = partition_type
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if _pt in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
    if _pt == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if _pt == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if _pt == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if _pt == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if _pt == "static":
        if not _values:
            raise ValueError("partition_type='static' requires non-empty partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


def _resolve_source_df(
    cfg: BrazeSinkComponent, exec_ctx: AssetExecutionContext
) -> pd.DataFrame:
    """Materialize the inline `source:` config into a DataFrame."""
    src = cfg.source or {}
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
            raise ValueError(
                f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()"
            )
        env = src.get("database_url_env_var")
        if env:
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
    raise ValueError(f"source kind={kind!r} not supported (sql / csv / inline)")


def _coerce_value_for_braze(v: Any) -> Any:
    """Braze requires ISO 8601 with timezone for datetimes. Coerce
    pandas Timestamps + naive datetimes to ISO strings (assuming UTC)."""
    from datetime import date, datetime, timezone
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
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


def _apply_fields_map(row: Dict[str, Any], fields_map: Dict[str, str]) -> Dict[str, Any]:
    """Rename row keys per fields_map (source_col -> braze_field). Drops
    columns not in the map. Coerces datetimes."""
    out: Dict[str, Any] = {}
    for src_col, dst_field in fields_map.items():
        if src_col not in row:
            continue
        val = _coerce_value_for_braze(row[src_col])
        if val is None:
            continue
        out[dst_field] = val
    return out


def _pick_user_id(row: Dict[str, Any], cfg: BrazeSinkComponent) -> Optional[Any]:
    if not cfg.user_id_column:
        return None
    val = row.get(cfg.user_id_column)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if cfg.id_type == "user_alias":
        return val if isinstance(val, dict) else None
    return str(val)


def _build_users_list(
    records: List[Dict[str, Any]], cfg: BrazeSinkComponent
) -> List[Dict[str, Any]]:
    """Shape records into the `attributes` array for /users/track.

    Identifier rules:
      - `id_type` picks which primary key the user_id_column value maps to
        (`external_id` | `braze_id` | `user_alias`).
      - Rows without a primary can still be sent if their mapped fields
        include `email` or `phone` (Braze's secondary identifiers).
      - Rows with NEITHER primary NOR any secondary identifier are dropped.
    """
    id_key = cfg.id_type
    custom_set = set(cfg.custom_attribute_columns or [])

    all_cols: set = set()
    for r in records:
        all_cols.update(r.keys())

    users: List[Dict[str, Any]] = []

    if cfg.fields_map:
        for row in records:
            uid = _pick_user_id(row, cfg)
            entry: Dict[str, Any] = {}
            if uid is not None:
                entry[id_key] = uid
            mapped = _apply_fields_map(row, cfg.fields_map)
            if custom_set:
                custom: Dict[str, Any] = {}
                for src_col in custom_set:
                    dst = cfg.fields_map.get(src_col, src_col)
                    if dst in mapped:
                        custom[dst] = mapped.pop(dst)
                if custom:
                    entry["custom_attributes"] = custom
            entry.update(mapped)
            if not (id_key in entry or "email" in entry or "phone" in entry):
                continue
            users.append(entry)
        return users

    # Legacy pass-through
    if cfg.attribute_columns is not None:
        attr_cols = list(cfg.attribute_columns)
    else:
        attr_cols = [
            c for c in all_cols
            if c != cfg.user_id_column and c not in custom_set
        ]
    for row in records:
        uid = _pick_user_id(row, cfg)
        entry = {}
        if uid is not None:
            entry[id_key] = uid
        for c in attr_cols:
            val = _coerce_value_for_braze(row.get(c))
            if val is None:
                continue
            entry[c] = val
        if custom_set:
            custom = {}
            for c in custom_set:
                val = _coerce_value_for_braze(row.get(c))
                if val is None:
                    continue
                custom[c] = val
            if custom:
                entry["custom_attributes"] = custom
        if not (id_key in entry or "email" in entry or "phone" in entry):
            continue
        users.append(entry)
    return users


def _build_items_list(
    records: List[Dict[str, Any]], cfg: BrazeSinkComponent
) -> List[Dict[str, Any]]:
    """Shape records into the `items` array for /catalogs/{name}/items.

    Item id is taken from ``cfg.item_id_column`` and copied into each
    item as ``id`` (Braze's required field name). The resource
    additionally validates the id charset and drops invalid ids.
    """
    items: List[Dict[str, Any]] = []
    for row in records:
        if cfg.fields_map:
            item = _apply_fields_map(row, cfg.fields_map)
            id_dst = cfg.fields_map.get(cfg.item_id_column, cfg.item_id_column)
            raw_id = item.pop(id_dst, None) or row.get(cfg.item_id_column)
        else:
            raw_id = row.get(cfg.item_id_column)
            item = {}
            for c, v in row.items():
                if c == cfg.item_id_column:
                    continue
                coerced = _coerce_value_for_braze(v)
                if coerced is not None:
                    item[c] = coerced
        if raw_id is None or (isinstance(raw_id, float) and pd.isna(raw_id)):
            continue
        item["id"] = str(raw_id)
        items.append(item)
    return items
