"""GithubDocumentsComponent -- Extract repo FILE CONTENT + issue/PR text bodies for RAG pipelines.

`github_ingestion` (a separate, pre-existing component in this repo) syncs
GitHub repository metadata/issues/PRs as structured dlt rows -- it never
fetches the actual CONTENT of files in the repo tree, and it emits issues/PRs
in GitHub's own raw shape, not this repo's document-source contract. This
component's whole purpose is the opposite: walk the repo's file tree, fetch
real file content (README/markdown/text), and reuse issue/PR text bodies,
all normalized into the document-source contract every RAG-source component
in this repo shares -- so `document_chunker` can treat a GitHub repo exactly
like Confluence/Notion/Firecrawl/a local corpus.

Verified against GitHub's own REST API reference (https://docs.github.com/
en/rest) on 2026-11-11:
  - `GET /repos/{owner}/{repo}/git/trees/{ref}?recursive=1`: returns a
    top-level `tree` array of `{path, mode, type, sha, size, url}`; `type`
    is `"blob"` for a regular file (`"tree"` for a directory, `"commit"` for
    a submodule -- confirmed from the request-side enum, since the docs page
    doesn't spell out response values explicitly). A top-level `truncated`
    boolean is `true` when the repo exceeds the hard array limit (GitHub
    documents 100,000 entries / 7MB for a recursive listing) -- logged as a
    warning here (the walk still proceeds over whatever was returned; there
    is no generic "fetch the rest" recovery for a truncated recursive tree
    short of walking one subtree at a time without `recursive`, which this
    component does not do).
  - `GET /repos/{owner}/{repo}/contents/{path}?ref=...`: returns `content`
    (base64-encoded) + `encoding` (`"base64"`), plus `html_url`/`sha`/`size`.
    A file over the Contents API's size ceiling omits `content` entirely
    (GitHub's docs point to the Git Blobs API instead for such files) --
    skipped here with a warning rather than a second API call per oversized
    file. `ref` pins the branch/tag/commit read, defaulting to the repo's
    default branch when unset.
  - `GET /repos/{owner}/{repo}/issues?state=all`: GitHub's own docs state
    "every pull request is an issue" -- this one endpoint returns both, and
    a PR-backed issue carries a `pull_request` key that a plain issue never
    does. Rows are tagged `source_type="github_pr"` vs `"github_issue"`
    using that key, both drawn from ONE paginated fetch (not two) when both
    resources are selected. Each item has `body` (markdown, nullable),
    `title`, `html_url`, `updated_at`. Pagination: a standard RFC-5988
    `Link` response header with `rel="next"` -- `requests` parses this into
    `response.links` natively, used here as the pagination mechanism
    (functionally identical to gitlab_ingestion/component.py's
    `HeaderLinkPaginator`, which drives the very same Link-header shape; GitHub
    and GitLab are the two REST APIs in this repo built directly on RFC 5988).
  - Auth: `Authorization: Bearer <token>` -- GitHub's own current docs
    examples use the Bearer scheme throughout (the legacy `Authorization:
    token <token>` form still works per the same docs but is not what new
    code should use). Optional: public repos are reachable unauthenticated,
    just far more rate-limited.

Every row follows the document-source contract shared by every document-
source ingestion component in this repo (see `firecrawl_ingestion`'s
`_row_from_page` and `document_corpus`): `doc_id`, `content`, `title`,
`url`, `source_type`, `updated_at`, `content_hash` (sha256 of `content`).
One caveat specific to the `github_file` source_type: GitHub's Contents API
does not return a per-file last-modified timestamp (that requires a
separate `GET .../commits?path=...` call PER FILE, multiplying API calls by
file count for a value most RAG use cases don't need) -- `updated_at` for
file rows is therefore the extraction run's fetch time, not the file's real
git history. `github_issue`/`github_pr` rows use the real `updated_at` GitHub
returns.

`resources` (`files`, `issues`, `prs`) is a genuine multi-resource concept
here -- unlike firecrawl_ingestion/notion_documents (one flat set of pages),
selecting just `files` skips issue/PR pagination entirely, and vice versa.
`asset_shape='multi_asset'` (opt-in, matching gitlab_ingestion/
servicenow_ingestion/quickbooks_ingestion's precedent for this repo's
multi-resource ingestion components) gives each resource its own
independently-materializable asset; `asset_shape='single'` (default) unions
them into one DataFrame tagged by a `_resource` column. No `column_lineage`
field, matching gitlab_ingestion/servicenow_ingestion: file rows and
issue/PR rows carry different extra columns (`path` vs `number`/`state`),
so this component can produce several independently-schemaed table-assets
from one instance -- the same reasoning those two components document for
omitting it.

This connector is hand-written with plain `@dlt.resource` generators (same
pattern as firecrawl_ingestion/notion_documents for APIs dlt's
`rest_api_source` doesn't fit -- a tree-walk-then-per-file-fetch isn't a
declarative REST-pagination shape), so hash_columns/on_schema_change/
exclude_columns/max_rows_per_resource and every dlt destination this repo
supports still work identically to every other ingestion component.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import datetime as dt
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


GITHUB_API_VERSION = "2022-11-28"

_KNOWN_RESOURCES = {"files", "issues", "prs"}


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _github_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _github_tree_paths(session, api_base, owner, repo, ref, headers, path_prefix, extensions):
    """GET /repos/{owner}/{repo}/git/trees/{ref}?recursive=1 -- returns the matched file
    paths (type == 'blob' entries only, filtered by path_prefix/extensions) and whether
    the response was truncated (repo too large for one recursive listing)."""
    resp = session.get(
        f"{api_base}/repos/{owner}/{repo}/git/trees/{ref}",
        params={"recursive": "1"},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    paths = []
    for entry in body.get("tree") or []:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path") or ""
        if path_prefix and not path.startswith(path_prefix):
            continue
        if extensions and not any(path.endswith(ext) for ext in extensions):
            continue
        paths.append(path)
    return paths, bool(body.get("truncated"))


def _github_fetch_file_content(session, api_base, owner, repo, ref, path, headers):
    """GET /repos/{owner}/{repo}/contents/{path}?ref=... -- returns {content, html_url, sha,
    size} with content base64-decoded to text, or None if the path isn't a plain file with
    inline content (a directory matched by mistake, or a file too large for this endpoint)."""
    resp = session.get(
        f"{api_base}/repos/{owner}/{repo}/contents/{path}",
        params={"ref": ref},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if isinstance(body, list):
        return None  # path resolved to a directory, not a file
    content_b64 = body.get("content")
    if not content_b64:
        return None  # too large for the Contents API's inline content (needs the Blobs API)
    import base64

    try:
        text = base64.b64decode(content_b64.replace("\n", "")).decode("utf-8", errors="replace")
    except Exception:
        return None
    return {"content": text, "html_url": body.get("html_url"), "sha": body.get("sha"), "size": body.get("size")}


def _github_fetch_issues(session, api_base, owner, repo, headers, state, since, per_page, max_records):
    """GET /repos/{owner}/{repo}/issues?state=...&per_page=...&since=... -- returns raw
    issue/PR JSON items (both share this one endpoint; caller splits by the `pull_request`
    key). Paginated by parsing the raw `Link` header via `requests.utils.parse_header_links`
    directly (the same parser `requests.Response.links` wraps) rather than relying on that
    property -- a minimal Response-like test double (this repo's shared partition-port
    drift test fixture among others) implements `.headers` but not `.links`, so depending
    on the convenience property breaks under any such fake. Same RFC-5988 mechanism
    gitlab_ingestion.py's HeaderLinkPaginator drives."""
    import requests.utils

    items: List[Dict[str, Any]] = []
    url = f"{api_base}/repos/{owner}/{repo}/issues"
    params: Optional[Dict[str, Any]] = {"state": state, "per_page": per_page}
    if since:
        params["since"] = since
    while url:
        resp = session.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        items.extend(resp.json())
        if max_records is not None and len(items) >= max_records:
            return items[:max_records]
        link_header = resp.headers.get("Link") or resp.headers.get("link")
        next_url = None
        if link_header:
            for link in requests.utils.parse_header_links(link_header):
                if link.get("rel") == "next":
                    next_url = link.get("url")
                    break
        url = next_url
        params = None  # the next URL from the Link header already carries every query param
    return items


def _row_from_github_file(path, file_data, fetched_at):
    content = file_data["content"]
    return {
        "doc_id": path,
        "content": content,
        "title": path.rsplit("/", 1)[-1],
        "url": file_data.get("html_url"),
        "source_type": "github_file",
        "updated_at": fetched_at,
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "path": path,
        "sha": file_data.get("sha"),
    }


def _row_from_github_issue(item):
    """Returns (row, is_pr) -- is_pr is True iff GitHub's `pull_request` key is present."""
    is_pr = "pull_request" in item
    content = item.get("body") or ""
    row = {
        "doc_id": str(item.get("id")),
        "content": content,
        "title": item.get("title"),
        "url": item.get("html_url"),
        "source_type": "github_pr" if is_pr else "github_issue",
        "updated_at": item.get("updated_at"),
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "number": item.get("number"),
        "state": item.get("state"),
    }
    return row, is_pr


# BEGIN partition row-filter helper (inlined per component; keep copies identical, see tests/test_partition_port_ingestion.py)
from typing import Literal  # noqa: E402,F401


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the frame is returned unchanged."""
    import pandas as pd

    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END partition row-filter helper


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


def _dedupe_by_doc_id(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Collapse duplicate 'doc_id' rows in a table readback to one row per doc_id, keeping
    the row with the latest 'updated_at' when that column is present. A safety net against
    row duplication independent of write_disposition -- e.g. an 'append'-mode table, or
    stale rows accumulated before 'replace'/'merge' was adopted. No-op if 'doc_id' is
    missing or the frame is empty. Returns (deduped_df, rows_removed)."""
    if df is None or len(df) == 0 or "doc_id" not in df.columns:
        return df, 0
    before = len(df)
    if "updated_at" in df.columns:
        df = df.sort_values("updated_at", kind="stable")
    deduped = df.drop_duplicates(subset="doc_id", keep="last").reset_index(drop=True)
    return deduped, before - len(deduped)


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


class GithubDocumentsComponent(Component, Model, Resolvable):
    """Extract GitHub repo FILE CONTENT (README/markdown/text) + issue/PR text bodies for RAG pipelines.

    `github_ingestion` syncs repo metadata/issues/PRs as structured rows -- it does NOT
    fetch file content from the repo tree. This component walks the repo's file tree
    (`GET git/trees/{ref}?recursive=1`), fetches matching files' content
    (`GET contents/{path}`), and reuses issue/PR text bodies (`GET issues?state=all`,
    which GitHub's API returns from one endpoint, tagged by the presence of a
    `pull_request` key) -- normalizing all of it into the document-source contract
    (`doc_id`/`content`/`title`/`url`/`source_type`/`updated_at`/`content_hash`) shared
    by every document-source ingestion component in this repo. Point
    `document_chunker`'s `source_column` at `content` and `upstream_asset_key` at this
    asset for a RAG chunking pipeline.

    `resources` (`files`,`issues`,`prs`) is a genuine multi-resource concept: set
    `asset_shape='multi_asset'` to give each its own independently-materializable asset
    (selecting just `files` then skips issue/PR pagination entirely, and vice versa);
    the default `asset_shape='single'` unions them into one DataFrame tagged by a
    `_resource` column.

    Example:

        ```yaml
        type: dagster_community_components.GithubDocumentsComponent
        attributes:
          asset_name: dagster_docs
          owner: dagster-io
          repo: dagster
          ref: master
          file_extensions: ".md,.mdx"
          path_prefix: docs/
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    owner: str = Field(description="Repository owner (user or organization), e.g. 'dagster-io'.")
    repo: str = Field(description="Repository name, e.g. 'dagster'.")

    access_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var with a GitHub personal access token or GitHub App installation "
            "token. Sent as 'Authorization: Bearer <token>' (GitHub's current "
            "documented scheme) on every request. Optional for a public repo -- "
            "GitHub rate-limits unauthenticated requests much more aggressively, and "
            "a private repo requires it."
        ),
    )
    api_base_url: str = Field(
        default="https://api.github.com",
        description=(
            "GitHub REST API base URL. Defaults to github.com. For GitHub Enterprise "
            "Server, set this to 'https://<your-host>/api/v3'."
        ),
    )
    ref: str = Field(
        default="main",
        description=(
            "Branch, tag, or commit SHA the file tree and file contents are read from "
            "(GitHub's 'git/trees/{ref}' and 'contents/{path}?ref=...'). Does not affect "
            "'issues'/'prs' -- GitHub's issue tracker isn't branch-scoped."
        ),
    )
    resources: str = Field(
        default="files,issues,prs",
        description=(
            "Comma-separated list of resources to extract: 'files' (repo tree walk + "
            "content fetch), 'issues' (issue text bodies), 'prs' (pull request text "
            "bodies -- GitHub returns PRs from the same /issues endpoint as issues, "
            "tagged via a 'pull_request' key; selecting both 'issues' and 'prs' still "
            "fetches that endpoint only once)."
        ),
    )
    asset_shape: Literal["single", "multi_asset"] = Field(
        default="single",
        description=(
            "'single' (default): one asset, keyed `asset_name`, covering every resource in "
            "`resources` as a single unit -- rows from every resource are unioned into one "
            "returned DataFrame tagged by a `_resource` column. 'multi_asset': each resource "
            "in `resources` is instead its own independently-materializable asset, keyed "
            "`{asset_name}_{resource}` -- e.g. selecting just the 'files' asset skips "
            "issue/PR pagination entirely. Materializing several together still batches them "
            "through one shared dlt pipeline call (one Dagster step), not parallel execution."
        ),
    )
    table_name_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Override the destination table name for specific resources, e.g. "
            "{'files': 'raw_docs'}. Resources in `resources` not listed here keep their "
            "resource name ('files'/'issues'/'prs') as the destination table name. Every "
            "key must also appear in `resources`."
        ),
    )
    file_extensions: str = Field(
        default=".md,.txt,.rst",
        description=(
            "Comma-separated file-extension allowlist for the 'files' resource -- a "
            "repo's full tree can include thousands of irrelevant entries (source code, "
            "binaries, node_modules, lockfiles, ...); only paths ending in one of these "
            "suffixes are fetched. Matched with a plain suffix check, e.g. '.md' also "
            "matches 'CHANGELOG.md'."
        ),
    )
    path_prefix: Optional[str] = Field(
        default=None,
        description=(
            "Scope the 'files' resource to paths starting with this prefix, e.g. 'docs/' "
            "to only fetch files under a docs directory. Unset (default): the whole tree."
        ),
    )
    issue_state: Literal["open", "closed", "all"] = Field(
        default="all",
        description="Filter 'issues'/'prs' by state (GitHub's 'state' param on GET .../issues).",
    )
    issues_since: Optional[str] = Field(
        default=None,
        description=(
            "Filter 'issues'/'prs' to items updated on or after this ISO 8601 timestamp "
            "(GitHub's 'since' param), e.g. '2024-01-01T00:00:00Z'. Unset: every item."
        ),
    )
    per_page: int = Field(
        default=100,
        description="Records fetched per API call for paginated resources ('issues'/'prs'). Default 100, GitHub's documented maximum.",
    )
    max_records: Optional[int] = Field(
        default=None,
        description="Cap on total records returned per resource ('files' matched paths, or 'issues'/'prs' items). None (default) = no cap.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS verification. Set false only for a GitHub Enterprise Server instance with a self-signed cert.",
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
            "type changes. A resource's own first materialization can always create its "
            "initial schema in every mode -- 'block_all' only locks down an ALREADY-"
            "established table's columns/types. A rejected change fails the run with a "
            "clear error naming the table and column -- it is never silently dropped."
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
            "Caps how many rows each resource loads into the destination. Unset (default): "
            "no cap. This still loads real rows into the real destination -- it is not a "
            "dry run or a preview -- meant for a first run to check a connection and the "
            "shape of the data cheaply, not for routine use. Never applied unless you set it."
        ),
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description=(
            "How each run writes into every destination table. 'replace' (default): each "
            "run fully replaces every table's rows with this run's fetch -- fixes a real "
            "duplication bug, since dlt's own default ('append') has no notion of a "
            "previous run's rows becoming stale, so every rerun against a persistent "
            "destination (including this component's own default duckdb-file destination, "
            "which is a real on-disk file, not in-memory) silently doubled/tripled/etc. "
            "every table's row_count forever. 'merge': upserts keyed on 'doc_id' (this "
            "component's row identity for both file rows and issue/PR rows), per table -- "
            "a rerun updates existing rows and adds new ones without dropping anything "
            "removed upstream. 'append': the old behavior, an explicit opt-in for a "
            "deliberate historical log of every fetch."
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
    group_name: Optional[str] = Field(default="github_documents", description="Asset group for organization")
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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current date partition key (e.g. 'updated_at').",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'docs,changelog'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'repos'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current static/dynamic partition key.",
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

    # No column_lineage field: 'files' rows and 'issues'/'prs' rows carry different
    # extra columns (path/sha vs number/state) -- this component can produce several
    # independently-schemaed table-assets from one instance, same reasoning
    # gitlab_ingestion/servicenow_ingestion document for omitting it.

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='GitHub Documents', editable=True)

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
        """Wraps _resolve_destination() to inject `bucket_url` (filesystem) or `query_result_bucket`
        (athena) -- dlt needs these to know where to write/query, separately from the staging area
        wired by _resolve_staging() for destinations that go through one."""
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
        """Databricks and Athena both load via an intermediate filesystem stage -- dlt requires this
        staging destination to be activated explicitly, it is never auto-enabled."""
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or (
            f"GitHub repo documents: {self.owner}/{self.repo} (files + issues/PRs), "
            "normalized to the document-source contract for RAG."
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

        owner = self.owner
        repo = self.repo
        api_base_url = self.api_base_url.rstrip("/")
        ref = self.ref
        access_token_env_var = self.access_token_env_var
        file_extensions = tuple(e.strip() for e in self.file_extensions.split(",") if e.strip())
        path_prefix = self.path_prefix
        issue_state = self.issue_state
        issues_since = self.issues_since
        per_page = self.per_page
        max_records = self.max_records
        verify_ssl = self.verify_ssl

        resources_list = [r.strip() for r in self.resources.split(",") if r.strip()]
        if not resources_list:
            raise ValueError("GithubDocumentsComponent: 'resources' must list at least one resource.")
        _unknown = set(resources_list) - _KNOWN_RESOURCES
        if _unknown:
            raise ValueError(
                f"GithubDocumentsComponent: 'resources' has unknown entries {sorted(_unknown)}; "
                f"must be one of {sorted(_KNOWN_RESOURCES)}."
            )
        output_names = {r: f"{asset_name}_{r}" for r in resources_list}
        _table_name_map = self.table_name_map or {}
        _unknown_overrides = set(_table_name_map) - set(resources_list)
        if _unknown_overrides:
            raise ValueError(
                f"GithubDocumentsComponent: table_name_map has key(s) not in 'resources': "
                f"{sorted(_unknown_overrides)}."
            )
        dest_table_names = {r: _table_name_map.get(r, r) for r in resources_list}

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
            _inferred_kinds = ["github", "python"]
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
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column

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

        def _fetch_selected(context, selected):
            """Fetches rows for every resource in `selected` ('files'/'issues'/'prs'). The
            issues endpoint is fetched at most ONCE even when both 'issues' and 'prs' are
            selected (GitHub returns both from one endpoint)."""
            token = _require_env(access_token_env_var) if access_token_env_var else None
            headers = _github_headers(token)
            session = requests.Session()
            session.verify = verify_ssl
            fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
            out: Dict[str, List[Dict[str, Any]]] = {}

            if "files" in selected:
                paths, truncated = _github_tree_paths(
                    session, api_base_url, owner, repo, ref, headers, path_prefix, file_extensions
                )
                if truncated:
                    context.log.warning(
                        f"GitHub tree for {owner}/{repo}@{ref} was truncated (repo exceeds the "
                        "recursive-listing size limit); some files may be missing from this run."
                    )
                if max_records is not None:
                    paths = paths[:max_records]
                context.log.info(f"Fetching content for {len(paths)} matched file(s)...")
                file_rows = []
                for path in paths:
                    file_data = _github_fetch_file_content(session, api_base_url, owner, repo, ref, path, headers)
                    if file_data is None:
                        context.log.warning(f"Skipping {path!r}: no inline content available.")
                        continue
                    file_rows.append(_row_from_github_file(path, file_data, fetched_at))
                out["files"] = file_rows

            if "issues" in selected or "prs" in selected:
                context.log.info(f"Fetching issues/PRs for {owner}/{repo} (state={issue_state!r})...")
                items = _github_fetch_issues(
                    session, api_base_url, owner, repo, headers, issue_state, issues_since, per_page, max_records
                )
                issue_rows, pr_rows = [], []
                for item in items:
                    row, is_pr = _row_from_github_issue(item)
                    (pr_rows if is_pr else issue_rows).append(row)
                if "issues" in selected:
                    out["issues"] = issue_rows[:max_records] if max_records is not None else issue_rows
                if "prs" in selected:
                    out["prs"] = pr_rows[:max_records] if max_records is not None else pr_rows

            return out

        def _make_dlt_resources(fetched):
            dlt_resources = []
            for name, rows in fetched.items():
                table_name = dest_table_names[name]

                def _factory(rows=rows, name=name, table_name=table_name):
                    @dlt.resource(name=name, table_name=table_name)
                    def _r():
                        yield from rows
                    return _r()

                dlt_resources.append(_factory())

            # Every table's row identity is 'doc_id' (both the file-row and issue/pr-row
            # shapes set it). 'replace' (default) fully replaces each table every run --
            # fixes the duplication bug, since dlt's own default ('append', which applies
            # when write_disposition is never set) has no notion of a previous run's rows
            # becoming stale. 'merge' upserts on 'doc_id'. 'append' is the old behavior,
            # an explicit opt-in.
            for _r in dlt_resources:
                if write_disposition == "merge":
                    _r.apply_hints(write_disposition=write_disposition, primary_key="doc_id")
                else:
                    _r.apply_hints(write_disposition=write_disposition)
            return dlt_resources

        if self.asset_shape == "single":
            @asset(retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=backfill_policy,
                key=AssetKey.from_user_string(asset_name),
                description=description,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def github_documents_asset(context: AssetExecutionContext):
                if context.has_partition_key:
                    context.log.info(f"Running for partition: {context.partition_key}")

                context.log.info(f"Starting GitHub document extraction, destination={destination or 'duckdb (in-memory)'}")
                fetched = _fetch_selected(context, resources_list)
                dlt_resources = _make_dlt_resources(fetched)

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

                total_rows = sum(len(v) for v in fetched.values())
                context.log.info(f"Loading {total_rows} document row(s) into destination...")
                load_info = pipeline.run(dlt_resources)
                context.log.info(f"GitHub documents loaded: {load_info}")
                load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

                base_metadata = {
                    "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                    "dataset_name": MetadataValue.text(dataset_name),
                    "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                    "resources_requested": MetadataValue.json(resources_list),
                    "destination_table_names": MetadataValue.json(dest_table_names),
                    "write_disposition": MetadataValue.text(write_disposition),
                    "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
                }

                non_sql_destinations = {"filesystem", "delta", "iceberg"}
                is_non_sql = destination in non_sql_destinations
                if persist_only or is_non_sql:
                    if is_non_sql and not persist_only:
                        context.log.warning(
                            f"destination={destination!r} is not SQL-backed; cannot return a DataFrame. "
                            f"Set persist_only=true to silence this warning."
                        )
                    return MaterializeResult(metadata=base_metadata)

                all_data = []
                resource_metadata = {}
                duplicate_rows_removed = {}
                with pipeline.sql_client() as client:
                    for r in resources_list:
                        try:
                            with client.execute_query(f"SELECT * FROM {dataset_name}.{dest_table_names[r]}") as cur:
                                df = cur.df()
                            df, removed = _dedupe_by_doc_id(df)
                            if removed > 0:
                                context.log.warning(
                                    f"{r}: removed {removed} duplicate row(s) by 'doc_id' (keeping the latest "
                                    "'updated_at' per id) when reading back table "
                                    f"{dest_table_names[r]!r}."
                                )
                            duplicate_rows_removed[r] = removed
                            if len(df) > 0:
                                df["_resource"] = r
                                all_data.append(df)
                                resource_metadata[r] = len(df)
                        except Exception as e:
                            context.log.warning(f"Could not load {r} (destination table {dest_table_names[r]!r}): {e}")

                if not all_data:
                    context.log.warning("No documents extracted from GitHub.")
                    return Output(value=pd.DataFrame(), metadata=base_metadata)

                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = _filter_df_to_partition(combined_df, context, partition_date_column, partition_static_column)

                metadata = {
                    **base_metadata,
                    "row_count": MetadataValue.int(len(combined_df)),
                    "column_count": MetadataValue.int(len(combined_df.columns)),
                    "resources_loaded": MetadataValue.json(list(resource_metadata.keys())),
                    "duplicate_rows_removed": MetadataValue.json(duplicate_rows_removed),
                    "duplicate_rows_removed_total": MetadataValue.int(sum(duplicate_rows_removed.values())),
                }
                for resource, rows in resource_metadata.items():
                    metadata[f"rows_{resource}"] = MetadataValue.int(rows)
                if include_preview and len(combined_df) > 0:
                    try:
                        _prev = combined_df.sample(min(preview_rows, len(combined_df))) if len(combined_df) > preview_rows * 10 else combined_df.head(preview_rows)
                        metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                    except Exception as _e:
                        context.log.warning(f"preview emission failed: {_e}")
                return Output(value=combined_df, metadata=metadata)

            return Definitions(assets=[github_documents_asset])

        from dagster import AssetOut, multi_asset

        outs = {
            output_names[r]: AssetOut(
                description=f"GitHub document reader: {r}",
                is_required=False,
                group_name=group_name,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
            )
            for r in resources_list
        }

        @multi_asset(
            outs=outs,
            name=asset_name,
            can_subset=True,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def github_documents_assets(context: AssetExecutionContext):
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")

            try:
                selected_output_names = set(context.op_execution_context.selected_output_names)
            except Exception:  # noqa: BLE001
                selected_output_names = set(output_names.values())
            selected = [r for r in resources_list if output_names[r] in selected_output_names]
            if not selected:
                context.log.info("github_documents: no resources selected; skipping.")
                return

            context.log.info(
                f"Starting GitHub document extraction for {selected!r}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )
            fetched = _fetch_selected(context, selected)
            dlt_resources = _make_dlt_resources(fetched)

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

            load_info = pipeline.run(dlt_resources)
            context.log.info(f"GitHub documents loaded: {load_info}")
            load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            is_non_sql = destination in non_sql_destinations

            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination={destination!r} is not SQL-backed; cannot return a DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                for r in selected:
                    yield MaterializeResult(
                        asset_key=AssetKey.from_user_string(output_names[r]),
                        metadata={
                            "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                            "dataset_name": MetadataValue.text(dataset_name),
                            "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                            "resource": MetadataValue.text(r),
                            "destination_table_name": MetadataValue.text(dest_table_names[r]),
                            "write_disposition": MetadataValue.text(write_disposition),
                            "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
                        },
                    )
                return

            with pipeline.sql_client() as client:
                for r in selected:
                    try:
                        with client.execute_query(f"SELECT * FROM {dataset_name}.{dest_table_names[r]}") as cur:
                            df = cur.df()
                    except Exception as e:
                        context.log.warning(f"Could not load {r} (destination table {dest_table_names[r]!r}): {e}")
                        df = pd.DataFrame()

                    df, duplicate_rows_removed = _dedupe_by_doc_id(df)
                    if duplicate_rows_removed > 0:
                        context.log.warning(
                            f"{r}: removed {duplicate_rows_removed} duplicate row(s) by 'doc_id' (keeping the "
                            f"latest 'updated_at' per id) when reading back table {dest_table_names[r]!r}."
                        )

                    df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)

                    metadata = {
                        "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                        "dataset_name": MetadataValue.text(dataset_name),
                        "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                        "resource": MetadataValue.text(r),
                        "destination_table_name": MetadataValue.text(dest_table_names[r]),
                        "row_count": MetadataValue.int(len(df)),
                        "write_disposition": MetadataValue.text(write_disposition),
                        "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
                        "duplicate_rows_removed": MetadataValue.int(duplicate_rows_removed),
                    }
                    if include_preview and len(df) > 0:
                        try:
                            _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                            metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                        except Exception as _e:
                            context.log.warning(f"preview emission failed: {_e}")

                    yield Output(value=df, output_name=output_names[r], metadata=metadata)

        return Definitions(assets=[github_documents_assets])


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
