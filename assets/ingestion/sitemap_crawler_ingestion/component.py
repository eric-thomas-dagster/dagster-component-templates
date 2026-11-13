"""SitemapCrawlerIngestionComponent -- plain, free, no-paid-API HTTP/sitemap-based web
crawler for RAG pipelines, rounding out the "web crawl" family alongside
firecrawl_ingestion/jina_reader_ingestion/tavily_ingestion (all three hit a paid
third-party crawl API; this one only ever makes direct HTTP requests with the
standard library + requests + BeautifulSoup -- no API key, no account, no cost).

Two discovery modes (mutually exclusive, enforced at config-resolution time):

1. `sitemap_url` -- fetch a `sitemap.xml`, parsed with the standard library's
   `xml.etree.ElementTree` (no third-party XML lib needed for this simple,
   well-documented format). Handles both shapes the sitemap protocol defines:
   a leaf sitemap (`<urlset><url><loc>...</loc><lastmod>...</lastmod></url></urlset>`)
   and a sitemap index (`<sitemapindex><sitemap><loc>...</loc></sitemap></sitemapindex>`)
   that itself lists child sitemap URLs. A sitemap index is followed one level deep
   (each child sitemap is fetched and its <url> entries collected); a child that turns
   out to itself be another index is logged and skipped rather than recursed into
   further -- real-world sitemap indexes are not nested more than one level deep in
   practice, and unbounded recursion here would have no natural stopping point.
   `max_pages`/`max_depth` do not apply in this mode (sitemaps declare their own
   complete URL list) -- a log warning is emitted if either is left non-default
   while `sitemap_url` is set, so a misconfigured component.yaml doesn't fail
   silently.
2. `start_urls` -- one or more seed URLs, crawled breadth-first by parsing same-domain
   `<a href>` links out of each fetched page (via BeautifulSoup), up to `max_pages`
   total pages and `max_depth` hops from the nearest seed URL.

`include_patterns`/`exclude_patterns` (comma-separated) are applied to every
candidate URL in BOTH modes before it is fetched (a sitemap can easily list
thousands of URLs across an entire site, so scoping is just as useful there as
in plain-crawl mode) -- as plain substring matches, not regexes. Substring
matching was chosen over regex because the primary use case this field exists
for ("scope a crawl to /docs/ only") is a path-prefix/substring check, and a
substring field never needs escaping (a user writing `/docs/` as a "regex"
would get a harmless no-op match on the literal characters; the reverse --
a user writing an intended substring that happens to contain regex
metacharacters like `?`/`.`/`+`, e.g. a query-string filter -- would silently
behave differently under regex semantics). A URL is kept if it contains at
least one `include_patterns` entry (when set) and contains none of the
`exclude_patterns` entries.

`respect_robots_txt` (default True) is enforced via the standard library's
`urllib.robotparser.RobotFileParser`, one per crawled domain (cached). Its
`robots.txt` fetch deliberately goes through `requests.get` (not
`RobotFileParser.read()`, which uses `urllib.request` directly and would be a
second, un-mockable network surface) so this component's entire network
footprint is `requests.get`, matching this repo's test convention of mocking
only `requests.get`/`requests.post`. A missing or unreachable robots.txt
(404, timeout, connection error, 5xx) is treated as allow-all with a logged
warning -- the common convention for "no robots.txt published" -- rather than
failing the run or silently refusing to crawl a site that simply has no
robots.txt at all.

Every fetched page's content is extracted the same way document_corpus's own
`.html` branch of `_extract_text` does: strip `<script>`/`<style>` tags, then
`BeautifulSoup(...).get_text(separator="\\n")` for the body text and
`<title>` for the page title.

Output rows follow the document source contract exactly (see firecrawl_ingestion's
`_row_from_page`): `doc_id`, `content`, `title`, `url`, `source_type` (literal
`"sitemap_crawler"`), `updated_at` (the sitemap's `<lastmod>` when available, else
the fetch timestamp), `content_hash` (sha256 of `content`). Point
`document_chunker`'s `source_column` at `content` and `upstream_asset_key` at this
asset for a RAG chunking -> embedding pipeline.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set
`destination` to persist directly to any dlt-supported destination (snowflake,
bigquery, postgres, filesystem, etc.). See `assets/ingestion/DESTINATIONS.md` for
the full configuration reference.
"""

import datetime as dt
import hashlib
import json
import os
import time
from collections import deque
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse

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


_DEFAULT_MAX_PAGES = 100
_DEFAULT_MAX_DEPTH = 3
_DEFAULT_USER_AGENT = "dagster-community-components-sitemap-crawler/1.0"


# --- Sitemap XML parsing (stdlib xml.etree.ElementTree) ---------------------------------

def _entries_from_urlset(root) -> List[Tuple[str, Optional[str]]]:
    """Extract (url, lastmod) pairs from a leaf <urlset> sitemap. `{*}tag` is
    ElementTree's namespace-wildcard child lookup (stdlib since Python 3.8) --
    sitemaps almost always declare the `http://www.sitemaps.org/schemas/sitemap/0.9`
    namespace, but this matches with or without one."""
    entries = []
    for url_el in root:
        loc_el = url_el.find("{*}loc")
        if loc_el is None or not loc_el.text:
            continue
        lastmod_el = url_el.find("{*}lastmod")
        lastmod = lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None
        entries.append((loc_el.text.strip(), lastmod))
    return entries


def _parse_sitemap(sitemap_url: str, headers: Dict[str, str], context) -> List[Tuple[str, Optional[str]]]:
    """Fetch and parse `sitemap_url`. Handles both sitemap-protocol shapes:
    a leaf <urlset> (returned directly) and a <sitemapindex> (each listed child
    sitemap is fetched and its <urlset> entries collected -- followed exactly one
    level deep; a child that is itself a <sitemapindex> is logged and skipped
    rather than recursed into further)."""
    import xml.etree.ElementTree as ET

    resp = requests.get(sitemap_url, headers=headers, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    local_tag = root.tag.rsplit("}", 1)[-1].lower()

    if local_tag == "urlset":
        return _entries_from_urlset(root)

    if local_tag == "sitemapindex":
        child_urls = []
        for sitemap_el in root:
            loc_el = sitemap_el.find("{*}loc")
            if loc_el is not None and loc_el.text:
                child_urls.append(loc_el.text.strip())
        context.log.info(
            f"Sitemap index at {sitemap_url!r} lists {len(child_urls)} child sitemap(s); "
            "fetching each (one level deep)..."
        )
        entries: List[Tuple[str, Optional[str]]] = []
        for child_url in child_urls:
            child_resp = requests.get(child_url, headers=headers, timeout=30)
            child_resp.raise_for_status()
            child_root = ET.fromstring(child_resp.content)
            child_tag = child_root.tag.rsplit("}", 1)[-1].lower()
            if child_tag == "urlset":
                entries.extend(_entries_from_urlset(child_root))
            else:
                context.log.warning(
                    f"Child sitemap {child_url!r} (listed by index {sitemap_url!r}) is itself a "
                    "sitemap index; nested indexes beyond one level are not followed, skipping."
                )
        return entries

    raise ValueError(
        f"Unrecognized sitemap root element <{root.tag}> at {sitemap_url!r} "
        "(expected <urlset> or <sitemapindex>)."
    )


# --- robots.txt --------------------------------------------------------------------------

class _RobotsCache:
    """One `urllib.robotparser.RobotFileParser` per domain, cached for the run.
    Fetches robots.txt via `requests.get` (not `RobotFileParser.read()`, which uses
    `urllib.request` directly) so this component's entire network footprint stays on
    `requests` -- this repo's test convention mocks only `requests.get`/`requests.post`.
    A missing (404) or unreachable (timeout/connection error/5xx) robots.txt is treated
    as allow-all, with a logged warning -- the common convention for "no robots.txt
    published", not a reason to fail the run or refuse to crawl."""

    def __init__(self, user_agent: str, headers: Dict[str, str], context, enabled: bool = True):
        self.user_agent = user_agent
        self.headers = headers
        self.context = context
        self.enabled = enabled
        self._parsers: Dict[str, Any] = {}

    def allowed(self, url: str) -> bool:
        if not self.enabled:
            return True
        from urllib import robotparser

        parsed = urlparse(url)
        netloc = parsed.netloc
        if netloc not in self._parsers:
            robots_url = f"{parsed.scheme}://{netloc}/robots.txt"
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            try:
                resp = requests.get(robots_url, headers=self.headers, timeout=15)
                if resp.status_code == 200:
                    rp.parse(resp.text.splitlines())
                else:
                    rp.parse([])
            except Exception as e:
                self.context.log.warning(
                    f"Could not fetch robots.txt for {netloc!r} ({e}) -- treating as allow-all."
                )
                rp.parse([])
            self._parsers[netloc] = rp
        return self._parsers[netloc].can_fetch(self.user_agent, url)


# --- page content extraction (same BeautifulSoup pattern as document_corpus's _extract_text) --

def _extract_page_content(html_text: str) -> Tuple[Optional[str], str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None
    content = soup.get_text(separator="\n")
    return title, content


def _row_from_page(url: str, title: Optional[str], content: str, lastmod: Optional[str] = None) -> Dict[str, Any]:
    """Document source contract (shared by every document-source ingestion component
    feeding document_chunker): doc_id/content/title/url/source_type/updated_at/content_hash."""
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    return {
        "doc_id": url,
        "content": content,
        "title": title,
        "url": url,
        "source_type": "sitemap_crawler",
        "updated_at": lastmod or fetched_at,
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


def _dedupe_by_doc_id(df: "pd.DataFrame") -> Tuple["pd.DataFrame", int]:
    """Safety net against duplicate doc_id/url rows in the returned DataFrame, regardless of
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


def _matches_patterns(url: str, include_list: List[str], exclude_list: List[str]) -> bool:
    """Plain substring matching (see module docstring for why substring, not regex,
    was chosen). Kept if it contains at least one include_list entry (when set) and
    none of the exclude_list entries."""
    if include_list and not any(p in url for p in include_list):
        return False
    if exclude_list and any(p in url for p in exclude_list):
        return False
    return True


def _normalize_link(base_url: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.split("#", 1)[0].strip()
    if not href:
        return None
    joined = urljoin(base_url, href)
    parsed = urlparse(joined)
    if parsed.scheme not in ("http", "https"):
        return None
    return joined


class SitemapCrawlerIngestionComponent(Component, Model, Resolvable):
    """Plain, free, no-paid-API HTTP/sitemap-based web crawler for RAG pipelines.

    Set `sitemap_url` to follow a sitemap.xml (or sitemap index) exhaustively, or
    `start_urls` to breadth-first crawl from one or more seed pages following
    same-domain links up to `max_pages`/`max_depth`. Exactly one of the two is
    required. Every fetched page becomes one row following the document source
    contract (`doc_id`/`content`/`title`/`url`/`source_type`/`updated_at`/
    `content_hash`) -- point `document_chunker`'s `source_column` at `content` and
    `upstream_asset_key` at this asset for a RAG chunking -> embedding pipeline.

    Example:

        ```yaml
        type: dagster_community_components.SitemapCrawlerIngestionComponent
        attributes:
          asset_name: docs_pages
          sitemap_url: "https://docs.example.com/sitemap.xml"
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    sitemap_url: Optional[str] = Field(
        default=None,
        description=(
            "URL of a sitemap.xml (leaf <urlset>, or a <sitemapindex> listing child "
            "sitemaps -- followed one level deep) to crawl exhaustively. Every <loc> "
            "URL is fetched; max_pages/max_depth do not apply. Required unless "
            "start_urls is set (exactly one of the two, not both, not neither)."
        ),
    )
    start_urls: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated seed URL(s) to crawl breadth-first, following same-domain "
            "<a href> links up to max_pages/max_depth. Required unless sitemap_url is set."
        ),
    )
    max_pages: int = Field(
        default=100,
        gt=0,
        description=(
            "Max total pages to fetch in plain-crawl mode (start_urls). Ignored (with a "
            "logged warning if left non-default) when sitemap_url is set -- a sitemap "
            "declares its own complete URL list."
        ),
    )
    max_depth: int = Field(
        default=3,
        ge=0,
        description=(
            "Max link-following hops from the nearest seed URL in plain-crawl mode "
            "(start_urls). Ignored (with a logged warning if left non-default) when "
            "sitemap_url is set."
        ),
    )
    include_patterns: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated substrings (not regexes -- see component docstring for why) "
            "a candidate URL must contain at least one of, to be fetched. Applied in both "
            "modes, e.g. '/docs/' to scope a crawl or sitemap to one section of a site."
        ),
    )
    exclude_patterns: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated substrings; a candidate URL containing any of these is "
            "skipped. Applied after include_patterns, in both modes."
        ),
    )
    respect_robots_txt: bool = Field(
        default=True,
        description=(
            "Check each domain's robots.txt (via urllib.robotparser) before fetching any "
            "page, skipping disallowed URLs. Set False only when you have explicit "
            "permission to crawl a site regardless of its robots.txt."
        ),
    )
    request_delay_seconds: float = Field(
        default=0.5,
        ge=0,
        description="Seconds to sleep between successive page fetches -- politeness, avoid hammering a site.",
    )
    user_agent: str = Field(
        default="dagster-community-components-sitemap-crawler/1.0",
        description=(
            "User-Agent header sent with every request. Defaults to a string identifying "
            "this project (not a browser impersonation) -- change it to identify your own "
            "deployment if a site's robots.txt or access logs need that."
        ),
    )

    skip_unchanged: bool = Field(
        default=False,
        description=(
            "Persist a per-URL {etag, last_modified, content_hash} fingerprint to state_path "
            "between runs. On the next run, each known URL is fetched with If-None-Match/"
            "If-Modified-Since from its stored etag/last_modified; a 304 response (or, failing "
            "that, an unchanged content_hash after a real fetch) reuses the cached row instead "
            "of reparsing -- the output still covers every page every run, just without the "
            "redundant parse work for pages that haven't changed. Off by default: every page is "
            "refetched and reparsed unconditionally, matching this component's original behavior."
        ),
    )
    state_path: Optional[str] = Field(
        default=None,
        description=(
            "Where skip_unchanged persists its per-URL fingerprint state (a JSON file). Defaults "
            "to './{asset_name}_crawler_state.json'. Ignored when skip_unchanged=False."
        ),
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
            "Caps how many crawled-page rows load into the destination. Unset (default): "
            "no cap. This still loads real rows into the real destination -- it is not a "
            "dry run or a preview -- meant for a first run to check a connection and the "
            "shape of the data cheaply, not for routine use. Never applied unless you set it."
        ),
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description=(
            "dlt write disposition for the destination table. 'replace' (default): each run's "
            "crawl fully replaces the table. dlt's own default is 'append', which has no "
            "notion of a previous crawl's pages becoming stale -- every re-run keeps adding "
            "another full copy of every page on top of the last (a real, previously-shipped "
            "bug: row_count doubled on every rerun of an unchanged site, with no way to tell "
            "the loads apart downstream). 'merge': upserts keyed on `doc_id` (the page URL) -- "
            "keeps pages from a prior run that this run's crawl didn't revisit (e.g. "
            "max_pages capped this run short), unlike 'replace' which drops them. 'append': "
            "the old behavior, kept as an explicit opt-in for anyone who deliberately wants a "
            "historical log of every crawl rather than current-state pages."
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
    group_name: Optional[str] = Field(default="sitemap_crawler_ingestion", description="Asset group for organization")
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
        description="Comma-separated values for static or multi partitioning, e.g. 'docs,blog,changelog'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'sites'.",
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
    def _check_exactly_one_source(self):
        has_sitemap = bool(self.sitemap_url)
        has_start = bool(self.start_urls)
        if has_sitemap == has_start:
            raise ValueError(
                "SitemapCrawlerIngestionComponent requires exactly one of 'sitemap_url' "
                "(sitemap-driven mode) or 'start_urls' (plain breadth-first crawl mode) -- "
                "not both, and not neither."
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
        return ComponentFormConfig(label='Sitemap Crawler Ingestion', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        component = self
        asset_name = self.asset_name
        description = self.description or (
            "Crawl a site via its sitemap.xml or by following same-domain links from one "
            "or more seed URLs, ready to feed into document_chunker for RAG."
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
        sitemap_url = self.sitemap_url
        start_urls = self.start_urls
        max_pages = self.max_pages
        max_depth = self.max_depth
        include_patterns = self.include_patterns
        exclude_patterns = self.exclude_patterns
        respect_robots_txt = self.respect_robots_txt
        request_delay_seconds = self.request_delay_seconds
        user_agent = self.user_agent
        skip_unchanged = self.skip_unchanged
        state_path = self.state_path or f"./{asset_name}_crawler_state.json"

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
            _inferred_kinds = ["sitemap_crawler", "python"]
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
        def sitemap_crawler_ingestion_asset(context: AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            context.log.info(
                f"Starting sitemap/crawler ingestion, destination={destination or 'duckdb (in-memory)'}"
            )

            headers = {"User-Agent": user_agent}
            robots = _RobotsCache(user_agent, headers, context, enabled=respect_robots_txt)
            include_list = [p.strip() for p in (include_patterns or "").split(",") if p.strip()]
            exclude_list = [p.strip() for p in (exclude_patterns or "").split(",") if p.strip()]

            rows: List[Dict[str, Any]] = []
            pages_fetched = pages_skipped = pages_failed = 0

            fingerprints: Dict[str, Dict[str, Any]] = {}
            if skip_unchanged and os.path.exists(state_path):
                try:
                    with open(state_path) as f:
                        fingerprints = json.load(f)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"skip_unchanged: could not read state_path={state_path!r}, starting fresh: {e}")
                    fingerprints = {}

            def _fetch_one(url: str, lastmod: Optional[str]) -> Optional[str]:
                """Fetches `url`, appends its row to `rows`, sleeps request_delay_seconds,
                and returns the raw HTML (for link discovery in plain-crawl mode) or None
                if the fetch was skipped/failed. With skip_unchanged, sends If-None-Match/
                If-Modified-Since from the prior run's fingerprint and reuses the cached row
                on a 304 (or an unchanged content_hash after a real fetch) instead of
                reparsing -- the output still covers every page every run."""
                nonlocal pages_fetched, pages_skipped, pages_failed
                if not _matches_patterns(url, include_list, exclude_list):
                    context.log.info(f"Skipping {url} (does not match include/exclude patterns)")
                    return None
                if not robots.allowed(url):
                    context.log.info(f"Skipping {url} (disallowed by robots.txt)")
                    return None
                fetch_headers = dict(headers)
                prior = fingerprints.get(url) if skip_unchanged else None
                if prior:
                    if prior.get("etag"):
                        fetch_headers["If-None-Match"] = prior["etag"]
                    if prior.get("last_modified"):
                        fetch_headers["If-Modified-Since"] = prior["last_modified"]
                try:
                    resp = requests.get(url, headers=fetch_headers, timeout=30)
                    if resp.status_code == 304 and prior and prior.get("row"):
                        pages_skipped += 1
                        rows.append(prior["row"])
                        if request_delay_seconds:
                            time.sleep(request_delay_seconds)
                        return None
                    resp.raise_for_status()
                except Exception as e:
                    pages_failed += 1
                    context.log.warning(f"Failed to fetch {url}: {e}")
                    return None
                title, content = _extract_page_content(resp.text)
                row = _row_from_page(url, title, content, lastmod=lastmod)
                if skip_unchanged and prior and prior.get("content_hash") == row["content_hash"]:
                    pages_skipped += 1
                else:
                    pages_fetched += 1
                rows.append(row)
                if skip_unchanged:
                    fingerprints[url] = {
                        "etag": resp.headers.get("ETag"),
                        "last_modified": resp.headers.get("Last-Modified"),
                        "content_hash": row["content_hash"],
                        "row": row,
                    }
                if request_delay_seconds:
                    time.sleep(request_delay_seconds)
                return resp.text

            if sitemap_url:
                if max_pages != _DEFAULT_MAX_PAGES or max_depth != _DEFAULT_MAX_DEPTH:
                    context.log.warning(
                        "max_pages/max_depth only apply in plain-crawl mode (start_urls) and "
                        "are ignored when sitemap_url is set -- every URL in the sitemap is fetched."
                    )
                context.log.info(f"Fetching sitemap {sitemap_url!r}...")
                entries = _parse_sitemap(sitemap_url, headers, context)
                context.log.info(f"Sitemap lists {len(entries)} URL(s); fetching each...")
                for page_url, lastmod in entries:
                    _fetch_one(page_url, lastmod)

            if start_urls:
                seed_list = [u.strip() for u in start_urls.split(",") if u.strip()]
                root_netlocs = {urlparse(u).netloc for u in seed_list}
                context.log.info(
                    f"Crawling {len(seed_list)} seed URL(s), max_pages={max_pages}, "
                    f"max_depth={max_depth}..."
                )
                queue = deque((u, 0) for u in seed_list)
                visited = set()
                while queue and len(rows) < max_pages:
                    page_url, depth = queue.popleft()
                    if page_url in visited:
                        continue
                    visited.add(page_url)
                    html_text = _fetch_one(page_url, None)
                    if html_text is None:
                        continue
                    if depth < max_depth:
                        from bs4 import BeautifulSoup
                        link_soup = BeautifulSoup(html_text, "html.parser")
                        for a_tag in link_soup.find_all("a", href=True):
                            link = _normalize_link(page_url, a_tag["href"])
                            if (
                                link
                                and link not in visited
                                and urlparse(link).netloc in root_netlocs
                            ):
                                queue.append((link, depth + 1))

            if skip_unchanged:
                try:
                    with open(state_path, "w") as f:
                        json.dump(fingerprints, f)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"skip_unchanged: could not write state_path={state_path!r}: {e}")

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

            context.log.info("Loading crawled pages into destination...")
            try:
                load_info = pipeline.run(dlt_resources)
            except ImportError as e:
                if not destination:
                    raise ImportError(
                        "sitemap_crawler_ingestion: no `destination` is configured, so dlt loads "
                        "into an in-memory duckdb pipeline by default, which requires: pip install "
                        "duckdb (or `pip install 'dagster-community-components[sitemap_crawler]'`). "
                        f"Original error: {e}"
                    ) from e
                raise ImportError(
                    f"sitemap_crawler_ingestion: destination={destination!r} requires its dlt "
                    f"destination package to be installed. Original error: {e}"
                ) from e
            context.log.info(f"Sitemap crawler data loaded: {load_info}")
            load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "pages_crawled": MetadataValue.int(len(rows)),
                "write_disposition": MetadataValue.text(write_disposition),
                "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
            }
            if skip_unchanged:
                base_metadata["pages_fetched"] = MetadataValue.int(pages_fetched)
                base_metadata["pages_skipped"] = MetadataValue.int(pages_skipped)
                base_metadata["pages_failed"] = MetadataValue.int(pages_failed)

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
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[sitemap_crawler_ingestion_asset])


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
