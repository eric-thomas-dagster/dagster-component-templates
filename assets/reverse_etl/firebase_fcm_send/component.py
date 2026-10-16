"""DataFrame -> Firebase Cloud Messaging (FCM) push send (reverse-ETL
message activation).

!! SAFETY: materializing this asset sends REAL push notifications to REAL
devices via Firebase Cloud Messaging. There is no "upload for later
matching" indirection here like the ad-audience reverse-ETL components in
this repo (Meta/Google/TikTok/LinkedIn custom-audience upserts) -- every
row that resolves a device token results in an immediate, real push the
moment this asset materializes. ALWAYS test against a small list of your
own test-device tokens (`batch_size` set low, or a
`source: {kind: inline, rows: [...]}` with 1-2 rows) before pointing this
at a real warehouse segment.

For every upstream row: `firebase_admin.messaging.send(message, app=...)`
via the `firebase_resource`'s `.get_app()` (a Firebase Admin `App`). A
single `messaging.send()` per row (not a bulk `send_each`) is used
deliberately: it keeps per-row failure isolation simple (one bad token
raises on exactly that row, caught and counted, never aborting the run)
and sidesteps the Firebase Admin Python SDK's bulk-API churn --
`send_all`/`send_multicast` were removed entirely in Python SDK 7.0.0
(2025-07-17) in favor of `send_each`/`send_each_for_multicast`, but a
simple per-row `send()` was never deprecated and needs no migration.

`title_template`/`message_template` support simple `{column_name}`
substitution from other row columns for personalization.

Pairs with:
  - ``firebase_resource`` -- Firebase Admin SDK app auth (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class _SafeFormatDict(dict):
    """format_map() helper -- a missing `{column}` in a template renders as
    empty string rather than raising KeyError (a malformed template
    shouldn't crash a real-push-sending loop)."""

    def __missing__(self, key):  # noqa: D105
        return ""


def _render_template(template: str, row: Dict[str, Any]) -> str:
    safe_row = {k: ("" if v is None else v) for k, v in row.items()}
    return template.format_map(_SafeFormatDict(safe_row))


def _call_firebase_api(resource, token: str, title: Optional[str], body: str):
    """Isolates the one real external-API boundary (`firebase_admin.messaging`
    bound to the resource's Firebase Admin `App`) so it can be monkeypatched
    wholesale in tests without the `firebase-admin` package being installed
    -- mirrors this repo's "mock only the paid/external call" test
    convention."""
    from firebase_admin import messaging  # type: ignore

    notification = messaging.Notification(title=title, body=body) if (title or body) else None
    message = messaging.Message(notification=notification, token=token)
    return messaging.send(message, app=resource.get_app())



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


class FirebaseFcmSendComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a personalized push notification (via Firebase Cloud Messaging)
    to each row of an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.FirebaseFcmSendComponent
        attributes:
          asset_name: firebase_cart_abandoned_push
          upstream_asset_key: dbt_marts_cart_abandoned_today
          resource_key: firebase_resource
          recipient_column: fcm_device_token
          title_template: "You left something behind!"
          message_template: "{first_name}, your cart has {item_count} item(s) waiting."
          batch_size: 500
        ```

    !! Every materialization sends real push notifications -- test with a
    small/test device-token list first.
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
        default="firebase_resource",
        description="Resource key registered by FirebaseResourceComponent.",
    )

    recipient_column: str = Field(
        description="Upstream column holding the target FCM device registration token.",
    )
    message_template: str = Field(
        description=(
            "Push notification body template. Supports `{column_name}` "
            "substitution from other upstream row columns, e.g. "
            "'{first_name}, your cart has {item_count} item(s) waiting.'. A "
            "missing column renders as empty string rather than failing the row."
        ),
    )
    title_template: Optional[str] = Field(
        default=None,
        description=(
            "Push notification title template (same `{column_name}` "
            "substitution as message_template). Optional -- omit for a "
            "data-only/body-only notification."
        ),
    )
    batch_size: int = Field(
        default=1000,
        description=(
            "Max upstream rows per run (safety cap). Each row is one real "
            "push send -- this is NOT a throughput/rate-limit control, it's "
            "a blast-radius cap. Configure it deliberately; this component "
            "does not add its own rate-limiting beyond this cap."
        ),
    )

    group_name: Optional[str] = Field(default="firebase", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'firebase')."
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
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("firebase")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "FirebaseFcmSendComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.message_template:
            raise ValueError("FirebaseFcmSendComponent: message_template must be non-empty.")

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
            raise ValueError(f"FirebaseFcmSendComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_send(context, upstream):
            resource = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to send.")
                return dg.MaterializeResult(
                    metadata={
                        "messages_sent": dg.MetadataValue.int(0),
                        "messages_failed": dg.MetadataValue.int(0),
                    }
                )

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            if _self.recipient_column not in df.columns:
                raise dg.Failure(
                    f"recipient_column={_self.recipient_column!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )

            def _is_blank(v) -> bool:
                if v is None:
                    return True
                try:
                    if isinstance(v, float) and pd.isna(v):
                        return True
                except Exception:  # noqa: BLE001
                    pass
                return str(v).strip() == ""

            sent = 0
            failed = 0
            skipped_no_recipient = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                token = row_dict.get(_self.recipient_column)
                if _is_blank(token):
                    skipped_no_recipient += 1
                    continue

                body = _render_template(_self.message_template, row_dict)
                title = _render_template(_self.title_template, row_dict) if _self.title_template else None
                try:
                    _call_firebase_api(resource, token=str(token).strip(), title=title, body=body)
                    sent += 1
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    errors.append(f"row {i} (token={token}): {type(e).__name__}: {e}")

            context.log.info(
                f"Firebase FCM push send: sent={sent} failed={failed} "
                f"skipped_no_recipient={skipped_no_recipient}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "messages_sent": dg.MetadataValue.int(sent),
                "messages_failed": dg.MetadataValue.int(failed),
                "rows_skipped_no_recipient": dg.MetadataValue.int(skipped_no_recipient),
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
                f"Send a personalized push notification via Firebase Cloud "
                f"Messaging to each upstream row (recipient column "
                f"{_self.recipient_column!r}). Sends REAL push notifications."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_send(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_send(context, upstream)

        return dg.Definitions(assets=[_asset])
