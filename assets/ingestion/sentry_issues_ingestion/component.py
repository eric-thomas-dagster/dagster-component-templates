"""Sentry Issues Ingestion Component.

Pulls issues from Sentry's REST API (`/api/0/projects/{org}/{project}/issues/`)
and returns a DataFrame. Handles cursor pagination via the `Link` header.
"""

import os
import re
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


_NEXT_LINK_RE = re.compile(
    r"<(?P<url>[^>]+)>;\s*rel=\"next\";\s*results=\"true\"",
    re.IGNORECASE,
)


class SentryIssuesIngestionComponent(Component, Model, Resolvable):
    """Ingest issues from a Sentry project.

    Sentry API tokens: create at Organization Settings → Auth Tokens with
    `project:read` scope. Set the token in SENTRY_AUTH_TOKEN by default.
    """

    asset_name: str = Field(description="Output asset name.")

    organization: str = Field(
        description="Sentry organization slug (from the URL, e.g. 'acme-inc')."
    )
    project: str = Field(
        description="Sentry project slug (from the URL, e.g. 'frontend-web')."
    )

    api_host: str = Field(
        default="https://sentry.io",
        description=(
            "Sentry host. Use 'https://sentry.io' (default) for SaaS or your "
            "self-hosted URL."
        ),
    )
    api_token_env_var: str = Field(
        default="SENTRY_AUTH_TOKEN",
        description="Env var holding the Sentry auth token (needs `project:read` scope).",
    )

    query: str = Field(
        default="is:unresolved",
        description=(
            "Sentry search query. Examples: 'is:unresolved', 'is:resolved age:-24h', "
            "'level:error'. See https://docs.sentry.io/product/sentry-basics/search/"
        ),
    )
    stats_period: Optional[str] = Field(
        default="24h",
        description="Time range for the query (e.g. '24h', '7d', '30d'). None = default.",
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Total row cap across paginated pages.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="sentry")
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self

        @asset(
            key=_self.asset_name,
            description=_self.description or (
                f"Sentry issues from {_self.organization}/{_self.project} "
                f"({_self.query!r} over {_self.stats_period or 'default'})"
            ),
            group_name=_self.group_name,
            kinds={"python", "sentry"},
        )
        def _issues_asset(context: AssetExecutionContext):
            token = os.environ.get(_self.api_token_env_var)
            if not token:
                raise RuntimeError(
                    f"{_self.api_token_env_var!r} env var not set. Create an auth "
                    f"token at https://sentry.io/settings/account/api/auth-tokens/"
                )
            headers = {"Authorization": f"Bearer {token}"}
            url = (
                f"{_self.api_host.rstrip('/')}/api/0/projects/"
                f"{_self.organization}/{_self.project}/issues/"
            )
            params: Dict[str, Any] = {"query": _self.query, "limit": 100}
            if _self.stats_period:
                params["statsPeriod"] = _self.stats_period

            all_rows: List[Dict[str, Any]] = []
            page = 0
            _current_url = url
            _current_params = params
            while _current_url and len(all_rows) < _self.limit:
                page += 1
                resp = requests.get(
                    _current_url,
                    headers=headers,
                    params=_current_params if page == 1 else None,
                    timeout=30,
                )
                if resp.status_code == 401:
                    raise RuntimeError(
                        "Sentry returned 401 Unauthorized. Check that "
                        f"{_self.api_token_env_var} holds a valid token with "
                        "`project:read` scope."
                    )
                resp.raise_for_status()
                _batch = resp.json() or []
                all_rows.extend(_batch)
                # Parse next cursor from Link header.
                _link = resp.headers.get("Link", "") or ""
                _m = _NEXT_LINK_RE.search(_link)
                _current_url = _m.group("url") if _m else None
                _current_params = None
                if not _batch:
                    break

            df = pd.json_normalize(all_rows[: _self.limit], sep=".")
            context.log.info(
                f"Fetched {len(df)} Sentry issues across {page} page(s) from "
                f"{_self.organization}/{_self.project}"
            )

            metadata: Dict[str, Any] = {
                "dagster/row_count":    MetadataValue.int(len(df)),
                "sentry_organization":  MetadataValue.text(_self.organization),
                "sentry_project":       MetadataValue.text(_self.project),
                "sentry_query":         MetadataValue.text(_self.query),
                "pages_fetched":        MetadataValue.int(page),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    _sample = df.head(_self.preview_rows)
                    metadata["preview"] = MetadataValue.md(_sample.to_markdown(index=False))
                except Exception:  # noqa: BLE001
                    pass
            return Output(df, metadata=metadata)

        return Definitions(assets=[_issues_asset])
