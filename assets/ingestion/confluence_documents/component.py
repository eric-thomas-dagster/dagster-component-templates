"""ConfluenceDocumentsComponent -- Extract full Confluence page BODY TEXT for RAG pipelines.

A structural component (`confluence_ingestion` elsewhere in this repo) already syncs
Confluence pages/blog-posts as metadata rows via dlt -- it does NOT fetch page body
content. This component's whole purpose is the opposite: fetch each page's actual
body text (storage/XHTML markup by default), strip it to plain text, and emit rows
matching the document-source contract every RAG-source component in this repo shares
(doc_id/content/title/url/source_type/updated_at/content_hash -- see
firecrawl_ingestion/component.py's `_row_from_page` and document_corpus/component.py's
`_extract_text`).

Verified against Atlassian's own Confluence Cloud REST API docs on 2026-11-11:
- Auth: Confluence Cloud uses HTTP Basic auth with `base64(email:api_token)` (an
  Atlassian account email + API token pair, NOT a bearer token) --
  https://developer.atlassian.com/cloud/confluence/basic-auth-for-rest-apis/, whose
  own worked example is `Authorization: Basic base64("email@domain.com:api_token")`.
- Body fetch: `GET /wiki/rest/api/content/{id}` (and the list/search endpoints below)
  with `expand=body.storage` (or `body.view`) is real and current -- confirmed against
  the v1 Content API's OpenAPI spec (https://developer.atlassian.com/cloud/confluence/
  swagger.v3.json): the `contentExpand` parameter's own description states
  "`body.storage` returns the body of content in storage format", and `ContentBody`'s
  `representation` enum lists `storage`/`view`/`export_view`/`styled_view` as sibling
  representations of the same body object -- so the JSON path is `body.<format>.value`
  (e.g. `body.storage.value`). Storage format is Confluence's own XHTML-like markup
  (may contain `<ac:structured-macro>` macro tags); view format is server-rendered
  HTML. Stripped to plain text via BeautifulSoup (`html.parser` + `get_text(separator=
  "\\n")`), the exact pattern `document_corpus/component.py`'s `_extract_text` already
  uses for its own `.html` extraction.
- Listing pages by space: `GET /wiki/rest/api/content?spaceKey={key}&type=page`,
  paginated via `start` (zero-based offset) + `limit` (page size, default 25,
  "restricted by system limits") -- confirmed from the v1 API's own docs page. This
  endpoint's response has no documented `_links.next`/cursor in what's publicly
  rendered, so this component paginates the classic way: increment `start` by `limit`
  and stop once a page returns fewer than `limit` results.
  Source: https://developer.atlassian.com/cloud/confluence/rest/v1/intro/
- Listing descendants of a page: CQL search, `GET /wiki/rest/api/content/search?cql=
  ancestor={page_id}` -- confirmed real and current from Atlassian's own CQL docs
  (https://developer.atlassian.com/cloud/confluence/advanced-searching-using-cql/),
  which gives the exact worked example `GET https://your-domain.atlassian.net/wiki/
  rest/api/content/search?cql=ancestor=123456` and states "`ancestor` returns the
  page's direct children and all deeper descendants." This search endpoint IS
  cursor-paginated (confirmed from the OpenAPI spec: its `cql`/`cursor`/`limit`
  params, and a response `_links.next` containing the next cursor) -- this component
  follows `_links.next` until absent, same `next`-following pattern firecrawl_
  ingestion already uses for its own crawl-status pagination.
- Field names: `title` is a real top-level Content field (confirmed in the OpenAPI
  schema, `maxLength: 255`); the user-facing page URL is `_links.webui` (a
  site-relative path) joined onto `base_url` -- `_links.self` is the REST API
  resource URL instead, not what a human should click. Last-modified time is
  `version.when` (requires `expand=...,version`, which this component always
  requests alongside the body expand) -- `history.lastUpdated.when` is the real
  alternative Atlassian's own `contentExpand` docs mention ("`history.lastUpdated`
  returns information about the most recent update"), but `version.when` is the
  simpler single-field path and is what this component uses.

This connector is hand-written with a plain `@dlt.resource` generator wrapping the
already-fetched list of page-body rows (same approach as firecrawl_ingestion /
servicenow_ingestion's REST-but-not-rest_api_source-shaped siblings) wrapped into a
`dlt.pipeline(...).run(...)` -- not `rest_api_source`, since this is a two-step
flow (list pages, then read each one's already-included body via the `expand`
param) rather than one paginated resource. Confluence has no multi-table concept
here -- one run produces one flat set of page-body rows -- so there is no
`asset_shape`/multi_asset option.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import base64
import hashlib
import os
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
import requests
import dlt
from dagster import (
    BackfillPolicy,
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


# BEGIN column-hashing helper (inlined per component; keep copies identical, see tests/test_column_hashing_ingestion.py)
def _resolve_hash_salt(hash_salt, hash_salt_env_var):
    if hash_salt:
        return hash_salt
    if hash_salt_env_var:
        val = os.environ.get(hash_salt_env_var)
        if val is None:
            raise ValueError(
                f"hash_salt_env_var={hash_salt_env_var!r} references an environment "
                "variable that is not set."
            )
        return val
    return None


def _make_hash_columns_map(columns, salt):
    import hashlib

    def _hash_map(doc):
        for col in columns:
            value = doc.get(col)
            if value is not None:
                doc[col] = hashlib.sha256((str(value) + salt).encode()).hexdigest()
        return doc

    return _hash_map


def _apply_hash_columns(resources, hash_columns, hash_salt, hash_salt_env_var):
    """Pseudonymizes `hash_columns` (salted SHA-256, hex-encoded) on every resource in
    `resources` (an iterable of DltResource objects -- e.g. source.resources.values()
    or a selected_resources list) before the data reaches the destination -- dlt's own
    documented deterministic-hashing pattern for PII (https://dlthub.com/docs/
    general-usage/customising-pipelines/pseudonymizing_columns), via
    DltResource.add_map(), wired up as a config option instead of requiring a custom
    Python module."""
    if not hash_columns:
        return
    salt = _resolve_hash_salt(hash_salt, hash_salt_env_var)
    if not salt:
        raise ValueError(
            "hash_columns is set but neither hash_salt nor hash_salt_env_var is -- a "
            "salt is required (a hardcoded/shared salt would let the hash be reversed "
            "by dictionary attack on common values, e.g. emails)."
        )
    hash_map = _make_hash_columns_map(hash_columns, salt)
    for resource in resources:
        resource.add_map(hash_map)
# END column-hashing helper

# BEGIN dlt-advanced-options helper (inlined per component; keep copies identical, see tests/test_dlt_advanced_options_ingestion.py)
_SCHEMA_CONTRACT_MAP = {
    "allow_all": {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
    "allow_new_columns": {"tables": "evolve", "columns": "evolve", "data_type": "freeze"},
    "block_all": {"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
}


def _resolve_schema_contract(on_schema_change):
    contract = _SCHEMA_CONTRACT_MAP.get(on_schema_change)
    if contract is None:
        raise ValueError(
            f"on_schema_change={on_schema_change!r} must be one of {sorted(_SCHEMA_CONTRACT_MAP)}."
        )
    return dict(contract)


def _make_exclude_columns_map(columns):
    def _exclude_map(doc):
        for col in columns:
            doc.pop(col, None)
        return doc

    return _exclude_map


def _apply_exclude_columns(resources, exclude_columns):
    """Drops `exclude_columns` from every row in every resource before the data
    reaches the destination -- same per-resource add_map mechanism as
    _apply_hash_columns, applied first (see call sites) so a column in both ends up
    excluded, not hashed-then-excluded."""
    if not exclude_columns:
        return
    exclude_map = _make_exclude_columns_map(exclude_columns)
    for resource in resources:
        resource.add_map(exclude_map)


def _apply_max_rows_per_resource(resources, max_rows_per_resource):
    """Caps how many rows each resource loads -- a real cap on real rows landed in the
    real destination, never a dry run. DltResource.add_limit(count_rows=True) counts
    actual rows, not pages/yields (verified against dlt 1.30.0)."""
    if not max_rows_per_resource:
        return
    for resource in resources:
        resource.add_limit(max_items=max_rows_per_resource, count_rows=True)
# END dlt-advanced-options helper


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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


def _html_to_text(html: str) -> str:
    """Strip Confluence's storage (XHTML) or view (rendered HTML) markup down to plain
    text -- same BeautifulSoup pattern document_corpus/component.py's `_extract_text`
    uses for its own `.html` extraction (html.parser + get_text(separator="\\n"))."""
    if not html:
        return ""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator="\n")


def _build_auth_header(email: str, api_token: str) -> Dict[str, str]:
    """Confluence Cloud REST API auth: HTTP Basic with base64(email:api_token) --
    verified against https://developer.atlassian.com/cloud/confluence/
    basic-auth-for-rest-apis/ (NOT a bearer token)."""
    token = base64.b64encode(f"{email}:{api_token}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}", "Accept": "application/json"}


def _row_from_page(base_url: str, body_format: str, page: Dict[str, Any]) -> Dict[str, Any]:
    """Document source contract (shared by every document-source ingestion component
    feeding document_chunker): doc_id/content/title/url/source_type/updated_at/
    content_hash. `url` joins base_url with `_links.webui` (the user-facing wiki
    path) -- `_links.self` is the REST API resource URL, not what a human clicks."""
    page_id = str(page.get("id") or "")
    title = page.get("title") or ""
    body_obj = ((page.get("body") or {}).get(body_format) or {})
    raw_markup = body_obj.get("value") or ""
    content = _html_to_text(raw_markup)
    links = page.get("_links") or {}
    webui = links.get("webui") or ""
    url = f"{base_url.rstrip('/')}{webui}" if webui else (links.get("self") or "")
    updated_at = ((page.get("version") or {}).get("when")) or ""
    return {
        "doc_id": page_id,
        "content": content,
        "title": title,
        "url": url,
        "source_type": "confluence",
        "updated_at": updated_at,
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


def _dedupe_by_doc_id(df: "pd.DataFrame") -> Tuple["pd.DataFrame", int]:
    """Safety net against duplicate doc_id rows in the returned DataFrame, regardless of
    write_disposition -- keeps the row with the latest `updated_at` per doc_id (ties broken by
    original row order, pandas' stable sort). Returns (deduped_df, rows_removed)."""
    if "doc_id" not in df.columns or df.empty:
        return df, 0
    before = len(df)
    if "updated_at" in df.columns:
        df = df.sort_values("updated_at", kind="stable").drop_duplicates(subset="doc_id", keep="last")
    else:
        df = df.drop_duplicates(subset="doc_id", keep="last")
    return df.reset_index(drop=True), before - len(df)


def _fetch_space_pages(base_url, headers, space_key, body_format, page_size, max_pages, context):
    """GET /wiki/rest/api/content?spaceKey=...&type=page, paginated via start/limit --
    confirmed real and current against Atlassian's own v1 API intro docs. There is no
    documented `_links.next`/cursor on this endpoint's public docs, so this follows the
    classic pattern: increment `start` by `limit`, stop once a page returns fewer than
    `limit` results (or empty)."""
    pages: List[Dict[str, Any]] = []
    start = 0
    while True:
        resp = requests.get(
            f"{base_url.rstrip('/')}/wiki/rest/api/content",
            params={
                "spaceKey": space_key,
                "type": "page",
                "start": start,
                "limit": page_size,
                "expand": f"body.{body_format},version",
            },
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        results = body.get("results") or []
        pages.extend(results)
        context.log.info(f"Confluence space {space_key!r}: fetched {len(results)} page(s) at start={start} ({len(pages)} total).")
        if max_pages and len(pages) >= max_pages:
            return pages[:max_pages]
        if len(results) < page_size:
            return pages
        start += page_size


def _fetch_descendant_pages(base_url, headers, ancestor_page_id, body_format, page_size, max_pages, context):
    """CQL search, `GET /wiki/rest/api/content/search?cql=ancestor={id}` -- confirmed
    real and current against Atlassian's own CQL docs (worked example:
    `GET .../wiki/rest/api/content/search?cql=ancestor=123456`; "ancestor returns the
    page's direct children and all deeper descendants"). This endpoint IS
    cursor-paginated (confirmed via its OpenAPI spec's cursor/limit params and a
    response `_links.next`) -- follows `next` until absent, same pattern
    firecrawl_ingestion already uses for its own crawl-status pagination."""
    pages: List[Dict[str, Any]] = []
    url = f"{base_url.rstrip('/')}/wiki/rest/api/content/search"
    params = {
        "cql": f"ancestor={ancestor_page_id}",
        "limit": page_size,
        "expand": f"body.{body_format},version",
    }
    while url:
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        results = body.get("results") or []
        pages.extend(results)
        context.log.info(f"Confluence ancestor={ancestor_page_id!r}: fetched {len(results)} page(s) ({len(pages)} total).")
        if max_pages and len(pages) >= max_pages:
            return pages[:max_pages]
        next_link = (body.get("_links") or {}).get("next")
        if not next_link:
            return pages
        url = next_link if next_link.startswith("http") else f"{base_url.rstrip('/')}{next_link}"
        params = None  # next_link already carries the full query string (including the cursor)


class ConfluenceDocumentsComponent(Component, Model, Resolvable):
    """Extract full Confluence page BODY TEXT (not just page metadata) for RAG pipelines.

    List every page in a space (`space_key`) or every descendant of a page
    (`ancestor_page_id`, via Confluence's CQL `ancestor` search), fetch each page's
    body (storage or view format), strip it to plain text via BeautifulSoup, and emit
    one row per page matching the document-source contract every RAG-source component
    in this repo shares: `doc_id`, `content`, `title`, `url`, `source_type`
    (`"confluence"`), `updated_at`, `content_hash`. Point `document_chunker`'s
    `source_column` at `content` and `upstream_asset_key` at this asset for a RAG
    chunking -> embedding pipeline.

    This is NOT the same component as `confluence_ingestion` (which syncs Confluence
    pages/blog-posts as metadata rows via dlt -- no body text). Use this component
    specifically to get at page body content.

    Example (by space):

        ```yaml
        type: dagster_community_components.ConfluenceDocumentsComponent
        attributes:
          asset_name: confluence_docs
          base_url: "https://yoursite.atlassian.net"
          email_env_var: CONFLUENCE_EMAIL
          api_token_env_var: CONFLUENCE_API_TOKEN
          space_key: ENG
        ```

    Example (by ancestor page, i.e. every descendant of a root doc):

        ```yaml
        type: dagster_community_components.ConfluenceDocumentsComponent
        attributes:
          asset_name: runbook_docs
          base_url: "https://yoursite.atlassian.net"
          email_env_var: CONFLUENCE_EMAIL
          api_token_env_var: CONFLUENCE_API_TOKEN
          ancestor_page_id: "123456"
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    base_url: str = Field(
        description="Confluence Cloud site URL, e.g. 'https://yoursite.atlassian.net' (no trailing path)."
    )
    email_env_var: str = Field(
        description=(
            "Env var holding the Atlassian account email for Basic auth "
            "(Confluence Cloud: base64(email:api_token), NOT a bearer token)."
        ),
    )
    api_token_env_var: str = Field(
        description="Env var holding the Confluence Cloud API token (paired with email_env_var for Basic auth).",
    )

    space_key: Optional[str] = Field(
        default=None,
        description=(
            "Fetch every page in this Confluence space (GET /wiki/rest/api/content?"
            "spaceKey=...&type=page). Exactly one of space_key/ancestor_page_id is required."
        ),
    )
    ancestor_page_id: Optional[str] = Field(
        default=None,
        description=(
            "Fetch every descendant (direct children and deeper) of this page id, via "
            "Confluence's CQL search (cql=ancestor={id}). Exactly one of "
            "space_key/ancestor_page_id is required."
        ),
    )
    body_format: Literal["storage", "view"] = Field(
        default="storage",
        description=(
            "Which body representation to fetch and expand (Confluence's `body.storage` "
            "vs `body.view`). 'storage' (default): Confluence's own XHTML-like markup -- "
            "Atlassian's documented default representation, but may contain "
            "<ac:structured-macro> macro tags that extract as noise. 'view': "
            "server-rendered HTML -- usually cleaner plain-text extraction for RAG, at "
            "the cost of being a step removed from the authored source."
        ),
    )
    page_size: int = Field(
        default=25,
        ge=1,
        le=100,
        description="Pages fetched per API call (Confluence's own documented default/max for this endpoint is 25).",
    )
    max_pages: Optional[int] = Field(
        default=None,
        description="Stop paginating after this many Confluence pages are listed. Unset (default): fetch all.",
    )

    hash_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Column names to pseudonymize (salted SHA-256, hex-encoded) before the data "
            "reaches the destination -- same deterministic-hashing pattern dlt's own docs "
            "recommend for PII (https://dlthub.com/docs/general-usage/customising-pipelines/"
            "pseudonymizing_columns). A column missing from a given row is skipped, not an "
            "error. Requires hash_salt or hash_salt_env_var."
        ),
    )
    hash_salt: Optional[str] = Field(
        default=None,
        description=(
            "Salt for hash_columns. Required (this or hash_salt_env_var) when hash_columns "
            "is set -- a hardcoded/shared salt would let the hash be reversed by dictionary "
            "attack on common values. Use \"{{ env.VAR_NAME }}\" for the inline form. Keep it "
            "stable across runs: changing it changes every hash."
        ),
    )
    hash_salt_env_var: Optional[str] = Field(
        default=None,
        description="Alternative to hash_salt: name of an env var holding the salt. Resolved at run-time.",
    )

    on_schema_change: Literal["allow_all", "allow_new_columns", "block_all"] = Field(
        default="allow_all",
        description=(
            "What to do when incoming data's shape differs from the destination table's "
            "established schema (new columns, or a column's data type changing). "
            "'allow_all' (default, today's behavior): accept any change. "
            "'allow_new_columns': accept new columns, but reject (fail the run) a column "
            "whose data type would change. 'block_all': reject both new columns and data "
            "type changes. The first materialization can always create its initial schema "
            "in every mode -- 'block_all' only locks down an ALREADY-established table's "
            "columns/types. A rejected change fails the run with a clear error naming the "
            "table and column -- it is never silently dropped."
        ),
    )
    exclude_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Column names to drop from every row before it reaches the destination. "
            "Applied before hash_columns (so a column in both ends up excluded, not "
            "hashed-then-excluded). A column missing from a given row is skipped, not an "
            "error, matching hash_columns."
        ),
    )
    max_rows_per_resource: Optional[int] = Field(
        default=None,
        description=(
            "Caps how many page-body rows load into the destination. Unset (default): "
            "no cap. This still loads real rows into the real destination -- it is not a "
            "dry run or a preview -- meant for a first run to check a connection and the "
            "shape of the data cheaply, not for routine use. Never applied unless you set it."
        ),
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description=(
            "dlt write disposition for the destination table. 'replace' (default): each run "
            "fully replaces the table. dlt's own default is 'append', which has no notion of a "
            "previous run's pages becoming stale -- every re-run keeps adding another full copy "
            "of every page on top of the last (a real, previously-shipped bug: row_count doubled "
            "on every rerun against a persistent destination, including this component's own "
            "default duckdb-file destination -- a real on-disk file, not in-memory). 'merge': "
            "upserts keyed on `doc_id` (the Confluence page_id) -- keeps pages from a prior run "
            "that this run didn't revisit (e.g. max_pages capped this run short), unlike "
            "'replace' which drops them. 'append': the old behavior, kept as an explicit opt-in "
            "for anyone who deliberately wants a historical log of every run rather than "
            "current-state pages."
        ),
    )

    # --- Destination fields (see ../DESTINATIONS.md) --------------------------

    destination: Optional[str] = Field(
        default=None,
        description=(
            "dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', "
            "'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', "
            "'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode."
        ),
    )
    dataset_name: Optional[str] = Field(
        default=None,
        description="Target dataset/schema in the destination. Defaults to the asset name.",
    )
    persist_only: bool = Field(
        default=False,
        description=(
            "If True with destination set: emit a MaterializeResult and skip DataFrame return. "
            "If False: query the destination back into a DataFrame (only meaningful for SQL "
            "destinations -- non-SQL destinations always emit MaterializeResult)."
        ),
    )
    destination_credentials_url: Optional[str] = Field(
        default=None,
        description=(
            "Inline connection string passed to dlt's destination factory. Useful when one "
            "Dagster project ingests into multiple accounts of the same destination type. "
            "If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md."
        ),
    )
    destination_credentials_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Alternative to destination_credentials_url: name of an env var holding the "
            "connection string. Resolved at run-time."
        ),
    )
    bucket_url: Optional[str] = Field(
        default=None,
        description=(
            "Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', "
            "'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). "
            "Required when destination='filesystem' (the final write target). Also used "
            "as the staging area when destination='databricks' or 'athena', both of which "
            "require an intermediate filesystem stage before the warehouse-side load -- "
            "dlt resolves the bucket's credentials from the destination-appropriate "
            "standard environment variables (e.g. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) "
            "unless destination_credentials_url/destination_credentials_env_var is set."
        ),
    )
    athena_query_result_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional S3 path where Athena writes query results (e.g. "
            "'s3://my-bucket/results/'). Only used when destination='athena'. May be "
            "omitted to use Athena-managed query results instead."
        ),
    )

    # --- Standard asset metadata -----------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default="confluence_documents", description="Asset group for organization")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'docs', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Auto-inferred from destination and asset name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )
    include_preview_metadata: bool = Field(
        default=True, description="Include sample data preview in metadata"
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when `include_preview_metadata` is True. "
            "For long DataFrames (>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run. 'multi_run' launches one "
            "run per partition (optionally capped via backfill_policy_max_partitions_per_run). "
            "Leave unset for Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'eng,product,support'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'spaces'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'chunk_text': ['content']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="after")
    def _check_space_or_ancestor(self):
        if bool(self.space_key) == bool(self.ancestor_page_id):
            raise ValueError(
                "ConfluenceDocumentsComponent requires exactly one of 'space_key' (list every "
                "page in a space) or 'ancestor_page_id' (list every descendant of a page) -- "
                "set exactly one, not both and not neither."
            )
        return self

    def _resolve_destination(self):
        """Build the dlt `destination` argument."""
        if not self.destination:
            return "duckdb"
        creds = None
        if self.destination_credentials_url:
            creds = self.destination_credentials_url
        elif self.destination_credentials_env_var:
            creds = EnvVar(self.destination_credentials_env_var).get_value()
        if creds:
            factory = getattr(dlt.destinations, self.destination, None)
            if factory is not None:
                return factory(credentials=creds)
        return self.destination

    def _resolve_destination_with_bucket(self):
        """Wraps _resolve_destination() to inject `bucket_url` (filesystem)
        or `query_result_bucket` (athena) -- dlt needs these to know where
        to write/query, separately from the staging area wired by
        _resolve_staging() for destinations that go through one."""
        if self.destination == "filesystem" and self.bucket_url:
            creds = None
            if self.destination_credentials_url:
                creds = self.destination_credentials_url
            elif self.destination_credentials_env_var:
                creds = EnvVar(self.destination_credentials_env_var).get_value()
            return dlt.destinations.filesystem(bucket_url=self.bucket_url, credentials=creds)
        if self.destination == "athena" and self.athena_query_result_bucket:
            creds = None
            if self.destination_credentials_url:
                creds = self.destination_credentials_url
            elif self.destination_credentials_env_var:
                creds = EnvVar(self.destination_credentials_env_var).get_value()
            athena_kwargs = {"query_result_bucket": self.athena_query_result_bucket}
            if creds:
                athena_kwargs["credentials"] = creds
            return dlt.destinations.athena(**athena_kwargs)
        return self._resolve_destination()

    def _resolve_staging(self):
        """Build the dlt `staging` argument. Databricks and Athena both load
        via an intermediate filesystem stage (files copied to a bucket, then
        loaded into the warehouse from there) -- dlt requires this staging
        destination to be activated explicitly, it is never auto-enabled.
        Unconditionally activates staging for these two (there's no direct-load
        mode for them in dlt); if `bucket_url` is set inline, build the
        filesystem destination with it explicitly, otherwise fall back to the
        bare 'filesystem' string so dlt resolves bucket_url/credentials from
        DESTINATION__FILESYSTEM__* env vars, consistent with how every other
        destination in this component resolves credentials by default."""
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Confluence Documents', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        component = self
        asset_name = self.asset_name
        description = self.description or (
            "Extract full Confluence page body text (storage/view format, stripped to plain "
            "text) for RAG pipelines -- not just page metadata."
        )
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        hash_columns = self.hash_columns
        hash_salt = self.hash_salt
        hash_salt_env_var = self.hash_salt_env_var
        on_schema_change = self.on_schema_change
        exclude_columns = self.exclude_columns
        max_rows_per_resource = self.max_rows_per_resource
        write_disposition = self.write_disposition
        base_url = self.base_url
        email_env_var = self.email_env_var
        api_token_env_var = self.api_token_env_var
        space_key = self.space_key
        ancestor_page_id = self.ancestor_page_id
        body_format = self.body_format
        page_size = self.page_size
        max_pages = self.max_pages

        _inferred_kinds = list(self.kinds or [])
        if destination:
            _kind_map = {
                "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
                "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
                "mssql": "mssql", "clickhouse": "clickhouse", "duckdb": "duckdb",
                "motherduck": "duckdb", "databricks": "databricks", "athena": "athena",
                "filesystem": "filesystem", "delta": "delta", "iceberg": "iceberg",
            }
            if destination in _kind_map:
                _inferred_kinds.append(_kind_map[destination])
        if not _inferred_kinds:
            _inferred_kinds = ["confluence", "python"]
        if "rag" not in _inferred_kinds:
            _inferred_kinds.append("rag")
        if "documents" not in _inferred_kinds:
            _inferred_kinds.append("documents")
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(retry_policy=_retry_policy, partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def confluence_documents_asset(context: AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            context.log.info(f"Starting Confluence document extraction, destination={destination or 'duckdb (in-memory)'}")

            email = _require_env(email_env_var)
            api_token = _require_env(api_token_env_var)
            headers = _build_auth_header(email, api_token)

            if space_key:
                context.log.info(f"Listing pages in Confluence space {space_key!r}...")
                raw_pages = _fetch_space_pages(base_url, headers, space_key, body_format, page_size, max_pages, context)
            else:
                context.log.info(f"Listing descendants of Confluence page {ancestor_page_id!r}...")
                raw_pages = _fetch_descendant_pages(base_url, headers, ancestor_page_id, body_format, page_size, max_pages, context)

            rows = [_row_from_page(base_url, body_format, p) for p in raw_pages]
            context.log.info(f"Confluence document extraction: {len(rows)} page(s) fetched.")

            @dlt.resource(name="pages")
            def _pages_resource():
                yield from rows

            dlt_resources = [_pages_resource()]
            hint_kwargs: Dict[str, Any] = {"write_disposition": write_disposition}
            if write_disposition == "merge":
                hint_kwargs["primary_key"] = "doc_id"
            for _r in dlt_resources:
                _r.apply_hints(**hint_kwargs)

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )

            if on_schema_change != "allow_all":
                for _r in dlt_resources:
                    _r.apply_hints(schema_contract=_resolve_schema_contract(on_schema_change))
            if exclude_columns:
                _apply_exclude_columns(dlt_resources, exclude_columns)
            if hash_columns:
                _apply_hash_columns(dlt_resources, hash_columns, hash_salt, hash_salt_env_var)
            if max_rows_per_resource:
                _apply_max_rows_per_resource(dlt_resources, max_rows_per_resource)

            context.log.info("Loading Confluence page-body rows into destination...")
            load_info = pipeline.run(dlt_resources)
            context.log.info(f"Confluence data loaded: {load_info}")
            load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "pages_fetched": MetadataValue.int(len(rows)),
                "write_disposition": MetadataValue.text(write_disposition),
                "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
            }

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            is_non_sql = destination in non_sql_destinations
            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination={destination!r} is not SQL-backed; cannot return DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                return MaterializeResult(metadata=base_metadata)

            with pipeline.sql_client() as client:
                try:
                    with client.execute_query(f"SELECT * FROM {dataset_name}.pages") as cur:
                        df = cur.df()
                except Exception as e:
                    context.log.warning(f"Could not read back pages table: {e}")
                    df = pd.DataFrame(rows)

            df, duplicate_rows_removed = _dedupe_by_doc_id(df)
            if duplicate_rows_removed:
                context.log.warning(
                    f"Dropped {duplicate_rows_removed} duplicate doc_id row(s) from the returned "
                    f"DataFrame (kept the latest updated_at per doc_id)."
                )

            from dagster import TableColumn, TableSchema
            metadata = {
                **base_metadata,
                "row_count": MetadataValue.int(len(df)),
                "rows_returned": MetadataValue.int(len(df)),
                "duplicate_rows_removed": MetadataValue.int(duplicate_rows_removed),
                "column_count": MetadataValue.int(len(df.columns)),
                "dagster/column_schema": MetadataValue.table_schema(
                    TableSchema(columns=[TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns])
                ),
            }
            if include_preview and len(df) > 0:
                try:
                    _preview_cols = [c for c in ("doc_id", "title", "url", "source_type", "updated_at", "content_hash") if c in df.columns]
                    _prev = df[_preview_cols] if _preview_cols else df
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[confluence_documents_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
