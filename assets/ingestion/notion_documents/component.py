"""NotionDocumentsComponent -- Extract full Notion page BODY TEXT for RAG pipelines.

`notion_ingestion` (a separate, pre-existing component) syncs Notion database
rows/page *metadata* via dlt's verified `notion` source -- it never fetches a
page's body content. This component's whole purpose is the opposite: walk a
page's actual block tree (`GET /v1/blocks/{block_id}/children`, recursively
for any block with `has_children: true`) and render it to markdown, so a RAG
pipeline has real page text to chunk/embed, not just database properties.

Verified against Notion's own API reference (https://developers.notion.com/
reference) on 2026-11-11:
  - Auth: `Authorization: Bearer <NOTION_API_KEY>` + required `Notion-Version`
    header -- current value `2026-03-11` (the latest, and the only value the
    docs describe for the endpoints this component calls).
  - `GET /v1/blocks/{block_id}/children`: paginated via `start_cursor` (request)
    / `has_more` + `next_cursor` (response). Each returned block has a `type`
    field naming the block type; type-named-key blocks that carry text store it
    under `<type>.rich_text`, an array of `{plain_text, ...}` items (join
    `plain_text` to get the block's text). Confirmed `rich_text`-bearing types:
    paragraph, heading_1/2/3/4, bulleted_list_item, numbered_list_item, quote,
    to_do, toggle, template, callout, code (code also has `language`+`caption`).
    Confirmed NON-rich_text types (skipped for content, but still walked for
    `has_children`/discovery): divider, child_page/child_database (`title`
    string, not `rich_text`), table/table_row/column/column_list, image/video/
    pdf/audio/file/embed/bookmark (captions/URLs), equation, synced_block,
    link_to_page/link_preview, table_of_contents, breadcrumb, meeting_notes,
    unsupported.
  - `POST /v1/databases/{database_id}/query`: paginated the same way
    (`start_cursor`/`has_more`/`next_cursor`); returns full Page objects
    (`id`, `url`, `last_edited_time`, `properties`) for every page in the
    database -- the same shape `GET /v1/pages/{page_id}` returns, so no extra
    per-page GET is needed in database mode.
  - `GET /v1/pages/{page_id}`: used in page-tree mode (no database query
    already handed us the page object). `last_edited_time` and `url` are
    top-level fields; a page's title lives in `properties`, under whichever
    key has `type == "title"` (its value is a `rich_text`-shaped array; join
    `plain_text`).
  - Page-tree walking: there is no documented "list all descendant pages of a
    page" endpoint. The only real, confirmed mechanism is recursively
    following `child_page` blocks discovered while walking block children --
    that's what `root_page_ids` does here. `include_child_databases` (default
    False, since it changes scope significantly) additionally recurses into
    any `child_database` block found the same way, querying it like
    `database_id` mode and walking each of its pages too.
  - Notion's docs point to a request-limits page for rate limiting but (as
    fetched) do not state a numeric average-requests-per-second figure from
    this component's source pages; `rate_limit_delay` (same pattern as
    `google_docs_extractor`) is still exposed so a real deployment can throttle
    itself, default 0.34s (~3 req/s, a conservative value in the ballpark
    commonly cited for Notion's integration rate limit).

Every row follows the document-source contract shared by every document-
source ingestion component in this repo (see `firecrawl_ingestion`'s
`_row_from_page` and `document_corpus`): `doc_id`, `content` (markdown,
rendered from the walked blocks), `title`, `url`, `source_type` (`"notion"`),
`updated_at` (`last_edited_time`), `content_hash` (sha256 of `content`).

This connector is hand-written with a plain `@dlt.resource` generator (same
pattern as firecrawl_ingestion/dropbox_ingestion/monday_ingestion for APIs
dlt's rest_api_source doesn't fit -- recursive block-walking isn't a
declarative REST-pagination shape), so hash_columns/on_schema_change/
exclude_columns/max_rows_per_resource and every dlt destination this repo
supports still work identically to every other ingestion component. There is
no multi-table concept here -- one run produces a flat set of page-body rows
-- so there is no `asset_shape`/multi_asset option, matching firecrawl_ingestion.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import os
import time
from typing import Any, Dict, List, Literal, Optional, Union

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


NOTION_BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2026-03-11"

# Block types whose `<type>.rich_text` holds renderable text (verified against
# Notion's "Retrieve block children" reference, see module docstring). Blocks
# of any other type (divider, child_page, child_database, table, image, ...)
# are still walked for `has_children`/discovery, but contribute no markdown line.
_RICH_TEXT_BLOCK_TYPES = {
    "paragraph", "heading_1", "heading_2", "heading_3", "heading_4",
    "bulleted_list_item", "numbered_list_item", "quote", "to_do", "toggle",
    "template", "callout", "code",
}


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _notion_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _extract_title(properties: Optional[Dict[str, Any]]) -> str:
    """A page's title lives in `properties`, under whichever key has `type == 'title'`
    (its value is a rich_text-shaped array) -- the key itself varies by database schema."""
    for prop in (properties or {}).values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            parts = prop.get("title") or []
            return "".join(p.get("plain_text", "") for p in parts)
    return ""


def _block_to_markdown(block: Dict[str, Any]) -> Optional[str]:
    """Translate one Notion block to a markdown line, or None for a block type that
    carries no renderable rich_text (divider, child_page, child_database, table, ...)."""
    btype = block.get("type")
    payload = block.get(btype) or {}
    if btype not in _RICH_TEXT_BLOCK_TYPES:
        return None
    rich_text = payload.get("rich_text") or []
    text = "".join(rt.get("plain_text", "") for rt in rich_text)
    if btype == "heading_1":
        return f"# {text}"
    if btype == "heading_2":
        return f"## {text}"
    if btype == "heading_3":
        return f"### {text}"
    if btype == "heading_4":
        return f"#### {text}"
    if btype == "bulleted_list_item":
        return f"- {text}"
    if btype == "numbered_list_item":
        return f"1. {text}"
    if btype == "to_do":
        return f"- [x] {text}" if payload.get("checked") else f"- [ ] {text}"
    if btype in ("quote", "callout"):
        return f"> {text}"
    if btype == "code":
        language = payload.get("language") or ""
        return f"```{language}\n{text}\n```"
    # paragraph, toggle, template: plain text as-is
    return text


def _query_database_pages(database_id: str, headers: Dict[str, str], page_size: int = 100) -> List[Dict[str, Any]]:
    """POST /v1/databases/{id}/query, paginated via start_cursor/has_more/next_cursor.
    Returns every page object in the database -- full Page objects (id/url/
    last_edited_time/properties), same shape GET /v1/pages/{id} returns."""
    pages: List[Dict[str, Any]] = []
    start_cursor: Optional[str] = None
    while True:
        body: Dict[str, Any] = {"page_size": page_size}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(
            f"{NOTION_BASE_URL}/databases/{database_id}/query",
            json=body, headers=headers, timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if not start_cursor:
            break
    return pages


def _get_page(page_id: str, headers: Dict[str, str]) -> Dict[str, Any]:
    resp = requests.get(f"{NOTION_BASE_URL}/pages/{page_id}", headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _get_block_children(block_id: str, headers: Dict[str, str], page_size: int = 100) -> List[Dict[str, Any]]:
    """GET /v1/blocks/{block_id}/children, paginated via start_cursor/has_more/next_cursor.
    Returns every DIRECT child block of block_id (one level -- recursion into a child's
    own children, if has_children, is the caller's job)."""
    children: List[Dict[str, Any]] = []
    start_cursor: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"page_size": page_size}
        if start_cursor:
            params["start_cursor"] = start_cursor
        resp = requests.get(
            f"{NOTION_BASE_URL}/blocks/{block_id}/children",
            params=params, headers=headers, timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        children.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if not start_cursor:
            break
    return children


def _walk_blocks(block_id: str, headers: Dict[str, str]):
    """Recursively walk block_id's children (a page's top-level blocks on the first
    call), rendering markdown lines and separately collecting any child_page/
    child_database block ids discovered along the way -- those are documents in
    their own right (page-tree mode), not inlined into this page's content."""
    lines: List[str] = []
    child_page_ids: List[str] = []
    child_database_ids: List[str] = []
    children = _get_block_children(block_id, headers)
    for block in children:
        btype = block.get("type")
        if btype == "child_page":
            child_page_ids.append(block["id"])
            continue
        if btype == "child_database":
            child_database_ids.append(block["id"])
            continue
        md_line = _block_to_markdown(block)
        if md_line:
            lines.append(md_line)
        if block.get("has_children"):
            nested_lines, nested_pages, nested_dbs = _walk_blocks(block["id"], headers)
            lines.extend(nested_lines)
            child_page_ids.extend(nested_pages)
            child_database_ids.extend(nested_dbs)
    return lines, child_page_ids, child_database_ids


def _render_page_content(page_id: str, headers: Dict[str, str]):
    lines, child_pages, child_dbs = _walk_blocks(page_id, headers)
    return "\n\n".join(lines), child_pages, child_dbs


def _row_from_notion_page(page_obj: Dict[str, Any], content: str) -> Dict[str, Any]:
    """Document-source contract shared by every document-source ingestion component
    in this repo (see firecrawl_ingestion._row_from_page / document_corpus)."""
    import hashlib

    return {
        "doc_id": page_obj["id"],
        "content": content,
        "title": _extract_title(page_obj.get("properties")),
        "url": page_obj.get("url"),
        "source_type": "notion",
        "updated_at": page_obj.get("last_edited_time"),
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


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


class NotionDocumentsComponent(Component, Model, Resolvable):
    """Extract full Notion page BODY TEXT (not just page metadata) for RAG pipelines.

    `notion_ingestion` syncs Notion database rows/page metadata via dlt -- it does
    NOT fetch a page's body content. This component walks a page's actual block
    tree (`GET /v1/blocks/{id}/children`, recursively) and renders it to markdown.
    Set `database_id` to extract every page in a Notion database, or
    `root_page_ids` to walk one or more page trees by recursively following
    `child_page` blocks (exactly one of the two is required). Output rows follow
    the document-source contract (`doc_id`/`content`/`title`/`url`/`source_type`/
    `updated_at`/`content_hash`) shared by every document-source ingestion
    component in this repo -- point `document_chunker`'s `source_column` at
    `content` and `upstream_asset_key` at this asset for a RAG chunking pipeline.

    Example:

        ```yaml
        type: dagster_community_components.NotionDocumentsComponent
        attributes:
          asset_name: notion_documents
          database_id: "a1b2c3d4e5f6..."
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    api_key_env_var: str = Field(
        default="NOTION_API_KEY",
        description="Env var holding your Notion integration token, sent as 'Authorization: Bearer <key>'.",
    )

    database_id: Optional[str] = Field(
        default=None,
        description=(
            "Notion database ID to query (POST /v1/databases/{id}/query) -- every page "
            "in the database becomes one output row. Required unless root_page_ids is set "
            "(exactly one of the two must be set)."
        ),
    )
    root_page_ids: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated Notion page IDs to walk as page trees: each root page becomes "
            "a row, and any child_page block discovered while walking its content is "
            "recursively walked and emitted as its own row too. Required unless database_id "
            "is set (exactly one of the two must be set)."
        ),
    )
    include_child_databases: bool = Field(
        default=False,
        description=(
            "Page-tree mode only. When a child_database block is discovered while walking "
            "a page's content, also query that database (same as database_id mode) and walk "
            "every one of its pages too, recursively. Default False: a safe default since this "
            "can significantly widen the scope of what gets extracted (whole linked databases, "
            "not just the page tree you named)."
        ),
    )
    rate_limit_delay: float = Field(
        default=0.34,
        ge=0,
        description=(
            "Seconds to sleep after fully processing each top-level document (page or "
            "database row) -- same pattern as google_docs_extractor's rate_limit_delay. "
            "Default ~3 requests/second, a conservative value for Notion's per-integration "
            "rate limit."
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
            "Caps how many page-body rows load into the destination. Unset (default): "
            "no cap. This still loads real rows into the real destination -- it is not a "
            "dry run or a preview -- meant for a first run to check a connection and the "
            "shape of the data cheaply, not for routine use. Never applied unless you set it."
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
    group_name: Optional[str] = Field(default="notion_documents", description="Asset group for organization")
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
        description="Comma-separated values for static or multi partitioning, e.g. 'docs,wiki,handbook'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'workspaces'.",
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
    def _check_database_id_or_root_page_ids(self):
        if bool(self.database_id) == bool(self.root_page_ids):
            raise ValueError(
                "NotionDocumentsComponent requires exactly one of 'database_id' (query a "
                "database) or 'root_page_ids' (comma-separated page IDs to walk as page "
                "trees) to be set -- not both, not neither."
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
        return ComponentFormConfig(label='Notion Documents', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        component = self
        asset_name = self.asset_name
        description = self.description or (
            "Extract full Notion page body text (markdown-rendered blocks) for RAG "
            "pipelines -- distinct from notion_ingestion's database-row/metadata sync."
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
        api_key_env_var = self.api_key_env_var
        database_id = self.database_id
        root_page_ids = self.root_page_ids
        include_child_databases = self.include_child_databases
        rate_limit_delay = self.rate_limit_delay

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
            _inferred_kinds = ["notion", "python"]
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
        def notion_documents_asset(context: AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            context.log.info(f"Starting Notion document extraction, destination={destination or 'duckdb (in-memory)'}")

            api_key = _require_env(api_key_env_var)
            headers = _notion_headers(api_key)

            rows: List[Dict[str, Any]] = []

            if database_id:
                context.log.info(f"Querying Notion database {database_id!r}...")
                page_objs = _query_database_pages(database_id, headers)
                context.log.info(f"Database {database_id!r} has {len(page_objs)} page(s); walking block content...")
                for page_obj in page_objs:
                    content, _child_pages, _child_dbs = _render_page_content(page_obj["id"], headers)
                    rows.append(_row_from_notion_page(page_obj, content))
                    time.sleep(rate_limit_delay)
            else:
                root_ids = [p.strip() for p in root_page_ids.split(",") if p.strip()]
                context.log.info(f"Walking {len(root_ids)} root page tree(s): {root_ids!r}")
                seen: set = set()
                queue: List[str] = list(root_ids)
                while queue:
                    page_id = queue.pop(0)
                    if page_id in seen:
                        continue
                    seen.add(page_id)
                    page_obj = _get_page(page_id, headers)
                    content, child_pages, child_dbs = _render_page_content(page_id, headers)
                    rows.append(_row_from_notion_page(page_obj, content))
                    queue.extend(cp for cp in child_pages if cp not in seen)
                    if include_child_databases:
                        for db_id in child_dbs:
                            if db_id in seen:
                                continue
                            seen.add(db_id)
                            db_pages = _query_database_pages(db_id, headers)
                            queue.extend(dp["id"] for dp in db_pages if dp["id"] not in seen)
                    time.sleep(rate_limit_delay)

            @dlt.resource(name="pages")
            def _pages_resource():
                yield from rows

            dlt_resources = [_pages_resource()]

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

            context.log.info(f"Loading {len(rows)} Notion page(s) into destination...")
            load_info = pipeline.run(dlt_resources)
            context.log.info(f"Notion data loaded: {load_info}")

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "pages_extracted": MetadataValue.int(len(rows)),
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

            from dagster import TableColumn, TableSchema
            metadata = {
                **base_metadata,
                "row_count": MetadataValue.int(len(df)),
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

        return Definitions(assets=[notion_documents_asset])


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
