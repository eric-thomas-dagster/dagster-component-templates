"""PostHog Events Ingestion Component.

Pulls events from PostHog's Query API (or the older /api/projects/{id}/events
endpoint) and returns a DataFrame. Handles cursor pagination automatically.
Supports personal API keys (POSTHOG_API_KEY env var by default) or a
per-project write key.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class PostHogEventsIngestionComponent(Component, Model, Resolvable):
    """Ingest events from a PostHog project.

    Requires a personal API key (POSTHOG_API_KEY by default) — generate one
    at Account Settings → Personal API Keys with the `query:read` scope.
    """

    asset_name: str = Field(description="Output asset name.")

    project_id: str = Field(
        description="PostHog project ID (numeric). Find in your project settings."
    )

    api_host: str = Field(
        default="https://us.i.posthog.com",
        description=(
            "PostHog API host. Use 'https://us.i.posthog.com' (default) for US Cloud, "
            "'https://eu.i.posthog.com' for EU Cloud, or your self-hosted URL."
        ),
    )

    api_key_env_var: str = Field(
        default="POSTHOG_API_KEY",
        description="Env var holding a PostHog personal API key with `query:read` scope.",
    )

    event: Optional[str] = Field(
        default=None,
        description="Filter by event name (e.g. '$pageview'). None = all events.",
    )

    since_days: int = Field(
        default=1,
        ge=1,
        le=90,
        description="Fetch events from the last N days (max 90).",
    )

    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Total row cap. Paginates until this limit or all events fetched.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="posthog")
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self

        @asset(
            key=_self.asset_name,
            description=_self.description or (
                f"PostHog events from project {_self.project_id}"
                + (f" (event='{_self.event}')" if _self.event else "")
            ),
            group_name=_self.group_name,
            kinds={"python", "posthog"},
        )
        def _events_asset(context: AssetExecutionContext):
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(
                    f"{_self.api_key_env_var!r} env var not set. Generate a personal "
                    f"API key at https://app.posthog.com/settings/user-api-keys"
                )
            headers = {"Authorization": f"Bearer {api_key}"}
            after = (datetime.utcnow() - timedelta(days=_self.since_days)).isoformat() + "Z"

            all_rows: List[Dict[str, Any]] = []
            url = f"{_self.api_host.rstrip('/')}/api/projects/{_self.project_id}/events/"
            params: Dict[str, Any] = {"after": after, "limit": 100}
            if _self.event:
                params["event"] = _self.event

            page = 0
            while url and len(all_rows) < _self.limit:
                page += 1
                resp = requests.get(url, headers=headers, params=params if page == 1 else None, timeout=30)
                if resp.status_code == 401:
                    raise RuntimeError(
                        "PostHog returned 401 Unauthorized. Check that "
                        f"{_self.api_key_env_var} holds a valid personal API key with "
                        "`query:read` scope."
                    )
                resp.raise_for_status()
                data = resp.json()
                _results = data.get("results") or []
                all_rows.extend(_results)
                url = data.get("next")  # PostHog cursor pagination
                if not _results:
                    break

            df = pd.json_normalize(all_rows[: _self.limit], sep=".")
            context.log.info(
                f"Fetched {len(df)} PostHog events across {page} page(s) "
                f"from project {_self.project_id}"
            )

            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "posthog_project_id": MetadataValue.text(str(_self.project_id)),
                "posthog_api_host":   MetadataValue.text(_self.api_host),
                "pages_fetched":      MetadataValue.int(page),
            }
            if _self.event:
                metadata["event_filter"] = MetadataValue.text(_self.event)
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    _sample = df.head(_self.preview_rows)
                    metadata["preview"] = MetadataValue.md(_sample.to_markdown(index=False))
                except Exception:  # noqa: BLE001
                    pass
            return Output(df, metadata=metadata)

        return Definitions(assets=[_events_asset])
