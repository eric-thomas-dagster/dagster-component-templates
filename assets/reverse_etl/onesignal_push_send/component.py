"""DataFrame -> OneSignal push send (reverse-ETL message activation).

!! SAFETY: materializing this asset sends REAL push notifications to REAL
devices via the OneSignal REST API. There is no "upload for later
matching" indirection here like the ad-audience reverse-ETL components in
this repo (Meta/Google/TikTok/LinkedIn custom-audience upserts) -- every
row that resolves a subscription id results in an immediate, real push
the moment this asset materializes. ALWAYS test against a small list of
your own test-device subscription ids (`batch_size` set low, or a
`source: {kind: inline, rows: [...]}` with 1-2 rows) before pointing this
at a real warehouse segment.

For every upstream row: `POST https://api.onesignal.com/notifications`
with `Authorization: Key <REST_API_KEY>` (current host + auth scheme,
confirmed 2026) via the `onesignal_resource`. Targeting uses
`include_subscription_ids` -- OneSignal's current device-level targeting
field, which replaced the deprecated `include_player_ids` (legacy
`onesignal.com/api/v1/notifications` host) as part of OneSignal's
device-centric -> user-centric data model migration.

`heading_template`/`message_template` support simple `{column_name}`
substitution from other row columns for personalization.

Pairs with:
  - ``onesignal_resource`` -- App ID + REST API key auth (required)
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


def _call_onesignal_api(resource, subscription_id: str, heading: Optional[str], contents: str):
    """Isolates the one real external-API boundary (the OneSignal REST
    `POST /notifications` call) so it can be monkeypatched wholesale in
    tests without real network access -- mirrors this repo's "mock only
    the paid/external call" test convention."""
    import requests

    payload: Dict[str, Any] = {
        "app_id": resource.app_id,
        "target_channel": "push",
        "include_subscription_ids": [subscription_id],
        "contents": {"en": contents},
    }
    if heading:
        payload["headings"] = {"en": heading}

    resp = requests.post(
        f"{resource.api_base_url}/notifications",
        json=payload,
        headers=resource.get_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


class OneSignalPushSendComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a personalized push notification (via OneSignal) to each row of
    an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.OneSignalPushSendComponent
        attributes:
          asset_name: onesignal_cart_abandoned_push
          upstream_asset_key: dbt_marts_cart_abandoned_today
          resource_key: onesignal_resource
          recipient_column: onesignal_subscription_id
          heading_template: "You left something behind!"
          message_template: "{first_name}, your cart has {item_count} item(s) waiting."
          batch_size: 500
        ```

    !! Every materialization sends real push notifications -- test with a
    small/test subscription-id list first.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
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
        default="onesignal_resource",
        description="Resource key registered by OneSignalResourceComponent.",
    )

    recipient_column: str = Field(
        description=(
            "Upstream column holding the target OneSignal Subscription ID "
            "(device-level id; current API field -- replaces the deprecated "
            "'Player ID')."
        ),
    )
    message_template: str = Field(
        description=(
            "Push notification body ('contents') template. Supports "
            "`{column_name}` substitution from other upstream row columns, "
            "e.g. '{first_name}, your cart has {item_count} item(s) "
            "waiting.'. A missing column renders as empty string rather "
            "than failing the row."
        ),
    )
    heading_template: Optional[str] = Field(
        default=None,
        description=(
            "Push notification title ('headings') template (same "
            "`{column_name}` substitution as message_template). Optional."
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

    group_name: Optional[str] = Field(default="onesignal", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'onesignal')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("onesignal")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "OneSignalPushSendComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.message_template:
            raise ValueError("OneSignalPushSendComponent: message_template must be non-empty.")

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
            raise ValueError(f"OneSignalPushSendComponent source kind={kind!r} not supported (sql / csv / inline)")

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
                subscription_id = row_dict.get(_self.recipient_column)
                if _is_blank(subscription_id):
                    skipped_no_recipient += 1
                    continue

                contents = _render_template(_self.message_template, row_dict)
                heading = _render_template(_self.heading_template, row_dict) if _self.heading_template else None
                try:
                    _call_onesignal_api(
                        resource,
                        subscription_id=str(subscription_id).strip(),
                        heading=heading,
                        contents=contents,
                    )
                    sent += 1
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    errors.append(f"row {i} (subscription_id={subscription_id}): {type(e).__name__}: {e}")

            context.log.info(
                f"OneSignal push send: sent={sent} failed={failed} "
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
                f"Send a personalized push notification via OneSignal to "
                f"each upstream row (recipient column "
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
