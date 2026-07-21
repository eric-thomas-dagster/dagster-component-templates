"""DataFrame → Sentry events sink.

Turns each row of a DataFrame into a Sentry event and sends it via
the ingestion envelope endpoint. Uses the official `sentry-sdk` client
under the hood — supports levels, tags, extras, and grouping fingerprints.

Typical use case: emit "operational events" from a Dagster pipeline into
Sentry so on-call sees data-pipeline health alongside application errors.
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeToSentryComponent(Component, Model, Resolvable):
    """Send each row of an upstream DataFrame to Sentry as an event.

    Requires `sentry-sdk` installed. DSN is provided via env var (default
    SENTRY_DSN). Common shape: DataFrame with a `message` column plus any
    number of tag/extra columns.
    """

    asset_name: str = Field(description="Output asset name (this asset is a sink and returns None).")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset.")

    dsn_env_var: str = Field(
        default="SENTRY_DSN",
        description="Env var holding the Sentry DSN. Format: 'https://<key>@sentry.io/<project_id>'.",
    )
    environment: Optional[str] = Field(
        default=None,
        description="Sentry environment tag (e.g. 'prod', 'staging'). Defaults to whatever the SDK detects.",
    )
    release: Optional[str] = Field(
        default=None,
        description="Sentry release tag. Common: your app version, git SHA, or Dagster asset partition.",
    )

    message_column: str = Field(
        default="message",
        description="Name of the column whose value becomes the event's message text.",
    )
    level_column: Optional[str] = Field(
        default=None,
        description=(
            "Column supplying the Sentry level per row (one of debug|info|warning|error|fatal). "
            "Rows with an unknown value get default_level."
        ),
    )
    default_level: str = Field(
        default="info",
        description="Level applied when level_column is None or the row's value is not a valid level.",
    )
    tag_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to attach to each event as searchable tags in Sentry.",
    )
    extra_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to attach as extra (non-indexed) context in each event.",
    )
    fingerprint_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to combine into the event fingerprint (drives Sentry grouping). "
            "Default is Sentry's automatic grouping."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="sentry")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        _upstream_key = AssetKey.from_user_string(_self.upstream_asset_key)

        @asset(
            key=_self.asset_name,
            deps=[_upstream_key],
            description=_self.description or (
                f"Send {_self.upstream_asset_key} rows to Sentry as events"
            ),
            group_name=_self.group_name,
            kinds={"python", "sentry"},
        )
        def _sink_asset(context: AssetExecutionContext, upstream: pd.DataFrame):
            try:
                import sentry_sdk
            except ImportError as e:
                raise ImportError(
                    "sentry-sdk not installed. Run `uv add sentry-sdk` (or "
                    "`pip install sentry-sdk`) before using dataframe_to_sentry."
                ) from e

            dsn = os.environ.get(_self.dsn_env_var)
            if not dsn:
                raise RuntimeError(
                    f"{_self.dsn_env_var!r} env var not set. Get your DSN at "
                    f"Sentry → Project Settings → Client Keys (DSN)."
                )

            init_kwargs: Dict[str, Any] = {"dsn": dsn}
            if _self.environment:
                init_kwargs["environment"] = _self.environment
            if _self.release:
                init_kwargs["release"] = _self.release
            client = sentry_sdk.Client(**init_kwargs)
            hub = sentry_sdk.Hub(client)

            _valid_levels = {"debug", "info", "warning", "error", "fatal"}
            _default_level = (_self.default_level or "info").lower()
            if _default_level not in _valid_levels:
                _default_level = "info"

            df = upstream.copy()
            sent = 0
            skipped_no_msg = 0

            for _i, row in df.iterrows():
                _msg = row.get(_self.message_column)
                if not _msg or (isinstance(_msg, float) and pd.isna(_msg)):
                    skipped_no_msg += 1
                    continue
                _level = _default_level
                if _self.level_column and _self.level_column in df.columns:
                    _row_lvl = str(row.get(_self.level_column, "")).strip().lower()
                    if _row_lvl in _valid_levels:
                        _level = _row_lvl

                with hub.push_scope() as scope:
                    scope.level = _level
                    if _self.tag_columns:
                        for _t in _self.tag_columns:
                            if _t in df.columns:
                                _v = row.get(_t)
                                if _v is not None and not (isinstance(_v, float) and pd.isna(_v)):
                                    scope.set_tag(_t, str(_v))
                    if _self.extra_columns:
                        for _x in _self.extra_columns:
                            if _x in df.columns:
                                _v = row.get(_x)
                                if _v is not None and not (isinstance(_v, float) and pd.isna(_v)):
                                    scope.set_extra(_x, _v)
                    if _self.fingerprint_columns:
                        _fp = [
                            str(row.get(_c))
                            for _c in _self.fingerprint_columns
                            if _c in df.columns and row.get(_c) is not None
                        ]
                        if _fp:
                            scope.fingerprint = _fp
                    hub.capture_message(str(_msg), level=_level)
                sent += 1

            client.flush(timeout=5)

            metadata = {
                "dagster/row_count":  MetadataValue.int(sent),
                "rows_upstream":      MetadataValue.int(len(df)),
                "skipped_no_message": MetadataValue.int(skipped_no_msg),
                "sentry_dsn_prefix":  MetadataValue.text(dsn.split("@")[-1][:60] if "@" in dsn else "?"),
            }
            if _self.environment:
                metadata["environment"] = MetadataValue.text(_self.environment)
            if _self.release:
                metadata["release"] = MetadataValue.text(_self.release)

            context.log.info(
                f"Sent {sent} events to Sentry (skipped {skipped_no_msg} rows with empty {_self.message_column!r})"
            )
            return Output(None, metadata=metadata)

        return Definitions(assets=[_sink_asset])
