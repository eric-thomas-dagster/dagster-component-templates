"""PerRowHttpFetcher.

For each row in an input DataFrame, fetch the URL in `url_column` via HTTP
and append the response. (which
also does per-row HTTP).

Adds three output columns:
  - `<output_prefix>_body` (str)        — response text
  - `<output_prefix>_status` (int)      — HTTP status code (-1 on error)
  - `<output_prefix>_json` (dict | None) — parsed JSON when content-type looks like JSON, else None

Concurrent fetches via `concurrent.futures.ThreadPoolExecutor` (good
default for I/O-bound work). For very large URL lists (10k+) prefer
async/httpx — out of scope for v1.
"""
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class PerRowHttpFetcherComponent(Component, Model, Resolvable):
    """Per-row HTTP GET that appends response body + status to each row."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    url_column: str = Field(description="Column whose value is the URL to fetch for each row.")
    method: str = Field(default="GET", description="HTTP method: GET / POST / PUT / DELETE / HEAD.")
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Static headers sent with every request (e.g. {'Authorization': 'Bearer …'}).",
    )
    timeout_seconds: int = Field(
        default=30, ge=1, le=600,
        description="Per-request timeout in seconds.",
    )
    max_workers: int = Field(
        default=10, ge=1, le=128,
        description="Max concurrent requests (ThreadPoolExecutor workers).",
    )
    retries: int = Field(
        default=0, ge=0, le=10,
        description="Retry count per request on 5xx / connection errors (with exponential backoff).",
    )
    output_prefix: str = Field(
        default="response",
        description="Prefix for the three output columns: <prefix>_body, <prefix>_status, <prefix>_json.",
    )
    parse_json: bool = Field(
        default=True,
        description="Parse the response body as JSON when content-type looks like JSON (saves a downstream step).",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        url_column = self.url_column
        method = self.method.upper()
        headers = dict(self.headers or {})
        timeout = self.timeout_seconds
        max_workers = self.max_workers
        retries = self.retries
        prefix = self.output_prefix
        parse_json = self.parse_json

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "http", "api"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Per-row HTTP {method} against `{url_column}`.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                import requests
            except ImportError as e:
                raise ImportError("requests required: pip install requests") from e
            from concurrent.futures import ThreadPoolExecutor
            import json as _json
            import time as _time

            session = requests.Session()
            session.headers.update(headers)

            def _one(url: str):
                last_exc = None
                for attempt in range(retries + 1):
                    try:
                        r = session.request(method, url, timeout=timeout)
                        if r.status_code >= 500 and attempt < retries:
                            _time.sleep(0.5 * (2 ** attempt))
                            continue
                        body = r.text
                        status = r.status_code
                        js = None
                        if parse_json and "json" in (r.headers.get("content-type", "") or "").lower():
                            try:
                                js = r.json()
                            except _json.JSONDecodeError:
                                js = None
                        return body, status, js
                    except requests.RequestException as e:
                        last_exc = e
                        if attempt < retries:
                            _time.sleep(0.5 * (2 ** attempt))
                            continue
                        break
                return f"ERROR: {last_exc}", -1, None

            if url_column not in upstream.columns:
                context.log.warning(
                    f"per_row_http_fetcher: url_column {url_column!r} not present in upstream "
                    f"(have {list(upstream.columns)[:10]}). Skipping HTTP requests; "
                    f"emitting empty {prefix} / {prefix}_body / {prefix}_status columns."
                )
                _df = upstream.copy()
                _df[prefix] = ""
                _df[f"{prefix}_body"] = ""
                _df[f"{prefix}_status"] = -1
                if parse_json:
                    _df[f"{prefix}_json"] = None
                return _df
            urls = upstream[url_column].astype(str).tolist()
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                results = list(pool.map(_one, urls))

            df = upstream.copy().reset_index(drop=True)
            _bodies = [r[0] for r in results]
            # Emit the body twice: once under the bare prefix (matches # Download tool's single `DownloadData` column so downstream
            # workflows that reference the prefix-only name still work), and
            # once with `_body` suffix (preserves the dagster naming
            # convention for multi-output side-channel cols).
            df[prefix] = _bodies
            df[f"{prefix}_body"] = _bodies
            df[f"{prefix}_status"] = [r[1] for r in results]
            if parse_json:
                df[f"{prefix}_json"] = [r[2] for r in results]

            ok = int(sum(1 for r in results if 200 <= r[1] < 300))
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "requests_2xx": MetadataValue.int(ok),
                "requests_failed": MetadataValue.int(len(df) - ok),
                "url_column": MetadataValue.text(url_column),
            })
            return df

        return Definitions(assets=[_asset])
