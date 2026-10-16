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
from typing import Any, Dict, List, Optional, Union

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


class PerRowHttpFetcherComponent(Component, Model, Resolvable):
    """Per-row HTTP GET that appends response body + status to each row."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame")
    url_column: Union[str, int] = Field(description="Column whose value is the URL to fetch for each row.")
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
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
