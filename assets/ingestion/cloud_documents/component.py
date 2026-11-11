"""CloudDocumentsComponent -- Extract document file CONTENT from cloud storage / file-sharing
providers for RAG pipelines.

This spans two genuinely different access patterns, dispatched by the `provider` field:

1. Generic bucket storage (s3 / gcs / azure_blob / webdav): a real `fsspec`-based file-walk +
   fetch over any fsspec-backed URI, the same pattern `document_ingestion`'s
   `_list_files`/`_read_bytes`/`_is_source_available` already use in this repo -- reused here
   directly, including the same extraction libraries (pdfplumber / python-docx / BeautifulSoup,
   see `_extract_content_from_bytes` below). `webdav` support rides on fsspec's OWN built-in
   knowledge of the `webdav://` protocol: this environment's installed fsspec responds to
   `fsspec.get_filesystem_class('webdav')` with "Install webdav4 to access WebDAV" (verified
   2026-11-11 -- NOT "unknown protocol"), confirming fsspec ships a `webdav` ->
   `webdav4.WebdavFileSystem` registry entry. `webdav4` itself (a real, pip-installable package,
   https://pypi.org/project/webdav4/) is NOT installed in this environment, so the webdav path
   could not be exercised end-to-end here -- it is documented as best-effort in the field
   descriptions below, not asserted to work.

2. OAuth-protected vendor APIs (onedrive / sharepoint / box / dropbox): this component does NOT
   reimplement `onedrive_ingestion`/`sharepoint_ingestion`/`box_ingestion`/`dropbox_ingestion`'s
   auth+listing from scratch -- each branch mirrors that sibling component's own verified token
   exchange and listing endpoint (same URLs, same request shapes, same field names for the
   fields that carry over), then adds the one thing those components don't do: fetch each
   file's raw bytes and run them through the same extraction dispatch as (1). Verified against:
     - onedrive / sharepoint: Microsoft Graph driveItem via `GET /drives/{id}/root/children`
       (exactly the endpoint `onedrive_ingestion`/`sharepoint_ingestion` already call in this
       repo) -- the default driveItem response includes `id`, `name`, `lastModifiedDateTime`,
       `webUrl`, and (for files) `@microsoft.graph.downloadUrl`, a pre-authenticated, time-
       limited direct-download URL Graph returns inline on every file item (documented at
       https://learn.microsoft.com/en-us/graph/api/driveitem-get-content) -- no extra per-file
       API call needed to fetch bytes. Root children only, matching the non-recursive scope
       `onedrive_ingestion`/`sharepoint_ingestion` already have; subfolder recursion is a known
       gap, not built here.
     - box: Box v2.0 folder items via `GET /folders/{id}/items` (same endpoint `box_ingestion`
       calls), with `fields=id,name,modified_at,size,sha1,type` added explicitly -- Box's
       default "mini" representation omits `modified_at` unless requested via `fields`
       (documented Box API behavior; not re-verified live in this session). Content is fetched
       via `GET /files/{id}/content` with the same Bearer token; Box replies with a 302 to a
       signed download URL, which `requests` follows automatically.
     - dropbox: Dropbox API v2 `files/list_folder` (+ `/continue` pagination), same calls
       `dropbox_ingestion` already makes, reading `id`, `name`, `path_lower`, `path_display`,
       `server_modified` off each `.tag == "file"` entry. Content is fetched via
       `POST https://content.dropboxapi.com/2/files/download` with a `Dropbox-API-Arg` header
       carrying `{"path": ...}` -- Dropbox's separate content-transfer domain, distinct from the
       `api.dropboxapi.com` RPC domain the listing call uses.

Document source contract (every row, every provider): `doc_id`, `content` (real extracted
text), `title` (filename), `url`, `source_type` (the literal provider name), `updated_at`,
`content_hash` (sha256 of content) -- matching `document_corpus`/`document_ingestion`/
`firecrawl_ingestion` so a knowledge-base UI / `document_chunker` can treat every document
source identically regardless of vendor. `url` is whatever each provider actually gives: the
fsspec URI for bucket storage, Graph's `webUrl` (falling back to the download URL) for
onedrive/sharepoint, a constructed Box web-viewer URL (`https://app.box.com/file/{id}` -- a
stable, documented URL pattern, not a field literally returned by the folder-items listing) for
box, and the file's own `path_display` (NOT a clickable browser URL -- a real share link needs
a separate `sharing/create_shared_link_with_settings` call per file, not made here to avoid N
extra API calls per listing) for dropbox.

No multi-table concept here -- one flat set of document rows regardless of provider -- so
there is no `asset_shape`/multi_asset option, unlike `onedrive_ingestion`/
`sharepoint_ingestion`/`box_ingestion`, which have several resource types.

Design judgment call on OAuth field names: `onedrive_ingestion`/`box_ingestion`/
`dropbox_ingestion` all already use PLAIN `client_id`/`client_secret` fields (filled via
Dagster's own `${ENV_VAR}` YAML substitution at the component.yaml level); only
`sharepoint_ingestion` uses the `*_env_var` + `_require_env` indirection for the same
Microsoft Graph app-only credentials `onedrive_ingestion` takes plain. Since this component
shares ONE `tenant_id`/`client_id`/`client_secret` field set across its onedrive AND
sharepoint branches (identical auth mechanism), it follows the majority (3 of 4) precedent and
keeps them plain, rather than adding a second `_env_var` triplet for sharepoint alone. Fields
unique to this component that ARE `_env_var`-suffixed (`webdav_password_env_var`,
`webdav_token_env_var`) are resolved via `_require_env`, per this repo's convention for new
`*_env_var` fields.

This component's OWN `destination`/`bucket_url`/`dataset_name` fields are for where dlt LOADS
the resulting document rows to (its output) -- completely separate from the SOURCE cloud
location being read from (`source_path` / the OAuth provider's own folder/drive/site fields).
By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame; set `destination`
to persist directly to any dlt-supported destination. See `assets/ingestion/DESTINATIONS.md`
for the full destination configuration reference.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import dlt
import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
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


def _extract_content_from_bytes(raw: bytes, ext: str, encoding: str = "utf-8") -> str:
    """Real text extraction by extension, given raw bytes directly -- works identically whether
    the bytes came from an fsspec read (s3/gcs/azure_blob/webdav) or a vendor API's file-download
    response (onedrive/sharepoint/box/dropbox). Same libraries as document_ingestion/
    document_corpus: pdfplumber (.pdf), python-docx (.docx), BeautifulSoup (.html/.htm);
    .txt/.md/anything else is decoded as plain text. Raises on a genuine extraction failure --
    the caller decides whether to skip or fail the run. `.doc` (legacy binary Word) has no
    extraction library available here; raises NotImplementedError so the caller skips it rather
    than reading raw bytes as if they were text."""
    import io

    if ext == ".pdf":
        import pdfplumber

        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            return "\n\n".join(page.extract_text() or "" for page in pdf.pages)
    if ext == ".docx":
        import docx

        d = docx.Document(io.BytesIO(raw))
        return "\n".join(p.text for p in d.paragraphs)
    if ext in (".html", ".htm"):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw.decode(encoding, errors="replace"), "html.parser")
        return soup.get_text(separator="\n")
    if ext in (".pptx", ".xlsx", ".xls", ".epub", ".mp3", ".wav", ".m4a"):
        return _extract_with_markitdown(raw, ext)
    if ext == ".doc":
        raise NotImplementedError("legacy .doc format has no extraction library available")
    return raw.decode(encoding)


def _extract_with_markitdown(raw: bytes, ext: str) -> str:
    """Formats pdfplumber/python-docx/BeautifulSoup don't cover: PowerPoint, Excel, EPub and
    audio. Uses Microsoft's `markitdown` library (verified real API:
    `MarkItDown(enable_plugins=False).convert_stream(stream, file_extension=...).markdown`) --
    no LLM required for any of these formats."""
    import io

    try:
        from markitdown import MarkItDown
    except ImportError:
        raise ImportError(
            f"cloud_documents: extracting {ext!r} files requires: pip install "
            f"'markitdown[pptx,xlsx,docx,pdf]' (or markitdown[all])."
        )
    result = MarkItDown(enable_plugins=False).convert_stream(io.BytesIO(raw), file_extension=ext)
    return result.markdown


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


# BEGIN partition row-filter helper (inlined per component; keep copies identical, see tests/test_partition_port_ingestion.py)
def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the frame is returned unchanged."""
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


class CloudDocumentsComponent(Component, Model, Resolvable):
    """Extract document file CONTENT (real text extraction) from cloud storage / file-sharing
    providers, for RAG pipelines. See the module docstring for the full design and the real
    API details verified for each provider.

    Example:

        ```yaml
        type: dagster_community_components.CloudDocumentsComponent
        attributes:
          asset_name: s3_documents
          provider: s3
          source_path: "s3://my-bucket/docs/"
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    provider: Literal["s3", "gcs", "azure_blob", "webdav", "onedrive", "sharepoint", "box", "dropbox"] = Field(
        description=(
            "Which cloud storage / file-sharing provider to read from. 's3'/'gcs'/'azure_blob'/"
            "'webdav' walk a bucket/share via fsspec (source_path); 'onedrive'/'sharepoint'/"
            "'box'/'dropbox' authenticate to that vendor's own API, mirroring this repo's "
            "onedrive_ingestion/sharepoint_ingestion/box_ingestion/dropbox_ingestion components."
        ),
    )

    source_path: Optional[str] = Field(
        default=None,
        description=(
            "Required for provider='s3'/'gcs'/'azure_blob': an fsspec URI to walk recursively "
            "(e.g. 's3://bucket/docs/', 'gs://bucket/docs/', 'az://container/docs/') -- install "
            "the matching backend (s3fs / gcsfs / adlfs). Credentials are resolved by that "
            "backend from its own standard environment variables (e.g. "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY for s3fs) -- there is no separate "
            "credentials field here, matching document_ingestion's own source_path. For "
            "provider='webdav', this is instead a path RELATIVE to webdav_base_url (e.g. "
            "'/Documents/'); defaults to '/' (the share root) if unset. Ignored for "
            "onedrive/sharepoint/box/dropbox, which use their own listing fields below."
        ),
    )

    storage_options_env_vars: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Only used for provider='s3'/'gcs'/'azure_blob'. Maps an fsspec storage_option key "
            "to the env var name holding its value, e.g. {'endpoint_url': 'S3_ENDPOINT_URL'} "
            "for an S3-compatible endpoint (Cloudflare R2, MinIO, Rustfs, ...), or "
            "{'key': 'AWS_ACCESS_KEY_ID', 'secret': 'AWS_SECRET_ACCESS_KEY'} to override s3fs's "
            "own ambient credential chain explicitly. Resolved via dg.EnvVar at materialization "
            "time only -- never baked into the code location at load time. Omit to keep using "
            "fsspec's ambient auth (the cloud SDK's own default credential chain), which already "
            "works for real S3/GCS/Azure without this field."
        ),
    )

    # --- webdav-specific (best-effort/unverified -- see module docstring) -----------------

    webdav_base_url: Optional[str] = Field(
        default=None,
        description=(
            "Required when provider='webdav': the WebDAV server/share root, e.g. "
            "'https://dav.example.com/remote.php/dav/files/user/'. Routes through fsspec's "
            "'webdav' protocol, which this environment's fsspec already knows how to resolve "
            "to the webdav4 package (`fsspec.get_filesystem_class('webdav')` reports 'Install "
            "webdav4 to access WebDAV' rather than an unknown-protocol error) -- but webdav4 "
            "itself (pip install webdav4) is NOT installed here, so this path is best-effort / "
            "unverified end-to-end in this repo; install webdav4 and test against your own "
            "WebDAV server before relying on it in production."
        ),
    )
    webdav_username: Optional[str] = Field(
        default=None,
        description="WebDAV basic-auth username. Pairs with webdav_password_env_var. Optional if your WebDAV server allows anonymous/token-only access.",
    )
    webdav_password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the WebDAV basic-auth password, resolved via dagster.EnvVar. Pairs with webdav_username.",
    )
    webdav_token_env_var: Optional[str] = Field(
        default=None,
        description="Alternative to username/password: env var holding a bearer token, sent as 'Authorization: Bearer <token>', for WebDAV servers that support token auth.",
    )

    # --- Microsoft Graph (onedrive + sharepoint share this auth) ---------------------------

    tenant_id: Optional[str] = Field(
        default=None,
        description="Required for provider='onedrive'/'sharepoint': Azure AD (Entra ID) tenant ID. Mirrors onedrive_ingestion's own `tenant_id` field name.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description=(
            "Required for provider='onedrive'/'sharepoint'/'box'/'dropbox': the OAuth app's "
            "client ID. Shared across these 4 branches (each is a different vendor's app "
            "registration, but all 4 already name this field `client_id` in "
            "onedrive_ingestion/box_ingestion/dropbox_ingestion). For onedrive/sharepoint: "
            "an Azure AD app registration (Files.Read.All or Sites.Read.All application "
            "permission, admin-consented). For box: a Box app configured for Client "
            "Credentials Grant. For dropbox: a Dropbox app (used for the refresh-token "
            "exchange)."
        ),
    )
    client_secret: Optional[str] = Field(
        default=None,
        description="Required alongside client_id for onedrive/sharepoint/box/dropbox -- the matching OAuth app client secret.",
    )

    # --- onedrive-specific -------------------------------------------------------------------

    drive_id: Optional[str] = Field(
        default=None,
        description="Required for provider='onedrive': target drive ID (a user's OneDrive or a SharePoint document library's drive ID). Mirrors onedrive_ingestion's own `drive_id` field exactly.",
    )

    # --- sharepoint-specific -----------------------------------------------------------------

    site_hostname: Optional[str] = Field(
        default=None,
        description="Required for provider='sharepoint': SharePoint site collection hostname, e.g. 'contoso.sharepoint.com'. Mirrors sharepoint_ingestion's own field name.",
    )
    site_path: Optional[str] = Field(
        default=None,
        description="Required for provider='sharepoint': server-relative path to the site, e.g. '/sites/TeamSite'. Combined with site_hostname to resolve the site, then its default document library drive, via two Graph calls (GET /sites/{hostname}:/{path} then GET /sites/{site-id}/drive) -- same two-call resolution sharepoint_ingestion already does.",
    )

    # --- box-specific ------------------------------------------------------------------------

    box_subject_type: Optional[str] = Field(
        default=None,
        description="Required for provider='box': Box CCG subject type, 'enterprise' or 'user'. Mirrors box_ingestion's own field name.",
    )
    box_subject_id: Optional[str] = Field(
        default=None,
        description="Required for provider='box': Box enterprise ID (if subject_type=enterprise) or user ID (if subject_type=user). Mirrors box_ingestion's own field name.",
    )
    folder_id: Optional[str] = Field(
        default=None,
        description="provider='box' only: Box folder ID to list children of. Defaults to '0' (root) if unset. Root children only -- no recursion into subfolders.",
    )

    # --- dropbox-specific --------------------------------------------------------------------

    refresh_token: Optional[str] = Field(
        default=None,
        description="Required for provider='dropbox': OAuth2 refresh token obtained once via Dropbox's OAuth consent flow (token_access_type=offline) -- Dropbox deprecated long-lived static access tokens. Mirrors dropbox_ingestion's own field name.",
    )
    folder_path: Optional[str] = Field(
        default="",
        description="provider='dropbox' only: Dropbox folder path to list, e.g. '/Reports'. Empty string (default) lists the root.",
    )
    recursive: bool = Field(
        default=True,
        description="provider='dropbox' only: list all nested subfolders' contents too (Dropbox's own `recursive` list_folder parameter), not just the immediate folder. Unlike onedrive/sharepoint/box, Dropbox's list_folder API supports this natively.",
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
            "Caps how many document rows load into the destination. Unset (default): no "
            "cap. This still loads real rows into the real destination -- it is not a dry "
            "run or a preview -- meant for a first run to check a connection and the shape "
            "of the data cheaply, not for routine use. Never applied unless you set it."
        ),
    )

    # --- Destination fields (see ../DESTINATIONS.md) --------------------------

    destination: Optional[str] = Field(
        default=None,
        description=(
            "dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', "
            "'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', "
            "'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode. "
            "This is where the extracted document ROWS this component produces get loaded -- "
            "entirely separate from the cloud source being read from (source_path / the "
            "OAuth provider's own folder/drive/site fields above)."
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
            "Bucket/path URL for filesystem-shaped DESTINATION storage (e.g. "
            "'s3://my-bucket/path', 'gs://my-bucket/path', 'az://my-container/path', or "
            "'file:///local/path'). Required when destination='filesystem' (the final write "
            "target). Also used as the staging area when destination='databricks' or "
            "'athena'. This is NOT the same as source_path -- bucket_url is where dlt writes "
            "the output document rows; source_path is where this component reads documents "
            "FROM. dlt resolves the bucket's credentials from the destination-appropriate "
            "standard environment variables unless destination_credentials_url/"
            "destination_credentials_env_var is set."
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
    group_name: Optional[str] = Field(default="cloud_documents", description="Asset group for organization")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'knowledge_base', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to [provider, 'rag', 'documents'] (plus the destination's kind if destination is set) when unset.",
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
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current static/dynamic partition key.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill strategy for a partitioned asset. 'single_run' materializes a whole backfill range in one run "
            "(rows are filtered to the whole range); 'multi_run' launches one run per partition, optionally capped by "
            "backfill_policy_max_partitions_per_run. Unset keeps Dagster's default. Needs a partition-range-aware IO "
            "manager for single_run."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run (default 1).",
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
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags', 'storage_options_env_vars'))

    @model_validator(mode="after")
    def _check_provider_requirements(self):
        p = self.provider
        if p in ("s3", "gcs", "azure_blob") and not self.source_path:
            raise ValueError(f"CloudDocumentsComponent: source_path is required when provider={p!r}.")
        if p == "webdav" and not self.webdav_base_url:
            raise ValueError("CloudDocumentsComponent: webdav_base_url is required when provider='webdav'.")
        if p in ("onedrive", "sharepoint") and not (self.tenant_id and self.client_id and self.client_secret):
            raise ValueError(f"CloudDocumentsComponent: tenant_id/client_id/client_secret are required when provider={p!r}.")
        if p == "onedrive" and not self.drive_id:
            raise ValueError("CloudDocumentsComponent: drive_id is required when provider='onedrive'.")
        if p == "sharepoint" and not (self.site_hostname and self.site_path):
            raise ValueError("CloudDocumentsComponent: site_hostname/site_path are required when provider='sharepoint'.")
        if p == "box" and not (self.client_id and self.client_secret and self.box_subject_type and self.box_subject_id):
            raise ValueError("CloudDocumentsComponent: client_id/client_secret/box_subject_type/box_subject_id are required when provider='box'.")
        if p == "dropbox" and not (self.client_id and self.client_secret and self.refresh_token):
            raise ValueError("CloudDocumentsComponent: client_id/client_secret/refresh_token are required when provider='dropbox'.")
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
        return ComponentFormConfig(label='Cloud Documents', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        component = self
        asset_name = self.asset_name
        provider = self.provider
        description = self.description or f"Extract document file content from {provider} for RAG pipelines."
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

        source_path = self.source_path
        webdav_base_url = self.webdav_base_url
        webdav_username = self.webdav_username
        webdav_password_env_var = self.webdav_password_env_var
        webdav_token_env_var = self.webdav_token_env_var
        tenant_id = self.tenant_id
        client_id = self.client_id
        client_secret = self.client_secret
        drive_id = self.drive_id
        site_hostname = self.site_hostname
        site_path = self.site_path
        box_subject_type = self.box_subject_type
        box_subject_id = self.box_subject_id
        folder_id = self.folder_id
        refresh_token = self.refresh_token
        folder_path = self.folder_path
        recursive = self.recursive

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "mssql": "mssql", "clickhouse": "clickhouse", "duckdb": "duckdb",
            "motherduck": "duckdb", "databricks": "databricks", "athena": "athena",
            "filesystem": "filesystem", "delta": "delta", "iceberg": "iceberg",
        }
        _inferred_kinds = list(self.kinds or [])
        if destination and destination in _kind_map:
            _inferred_kinds.append(_kind_map[destination])
        if not _inferred_kinds:
            _inferred_kinds = [provider, "python"]
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

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
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

        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def cloud_documents_asset(context: AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            import hashlib

            context.log.info(f"Starting cloud_documents ingestion, provider={provider}, destination={destination or 'duckdb (in-memory)'}")

            extensions = ['.txt', '.md', '.pdf', '.doc', '.docx', '.html', '.htm', '.pptx', '.xlsx', '.xls', '.epub', '.mp3', '.wav', '.m4a']
            rows: List[Dict[str, Any]] = []
            skipped_files: List[Dict[str, str]] = []

            def _add_row(doc_id, raw_bytes, filename, ext, url, updated_at):
                try:
                    content = _extract_content_from_bytes(raw_bytes, ext)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"Skipping {filename} ({e})")
                    skipped_files.append({"filename": filename, "doc_id": str(doc_id), "reason": str(e)})
                    return
                rows.append({
                    # Document source contract (shared by every document-source ingestion
                    # component feeding document_chunker): doc_id/content/title/url/
                    # source_type/updated_at/content_hash.
                    "doc_id": str(doc_id),
                    "content": content,
                    "title": Path(filename).stem,
                    "url": url,
                    "source_type": provider,
                    "updated_at": updated_at,
                    "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                })

            if provider in ("s3", "gcs", "azure_blob"):
                import fsspec

                _backend_hint = {"s3": "s3fs", "gcs": "gcsfs", "azure_blob": "adlfs"}[provider]
                _storage_options: Dict[str, Any] = {}
                for _key, _env_var in (self.storage_options_env_vars or {}).items():
                    _val = EnvVar(_env_var).get_value()
                    if _val is None:
                        raise ValueError(
                            f"cloud_documents: storage_options_env_vars[{_key!r}] names env var "
                            f"{_env_var!r}, which is not set."
                        )
                    _storage_options[_key] = _val
                try:
                    fs, root = fsspec.core.url_to_fs(source_path, **_storage_options)
                except ImportError as e:
                    raise ImportError(
                        f"cloud_documents: provider={provider!r} requires the matching fsspec "
                        f"backend installed (pip install {_backend_hint}): {e}"
                    )
                if not fs.exists(root):
                    raise ValueError(f"cloud_documents: source_path does not exist or is not reachable: {source_path!r}")
                _proto = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
                for dirpath, _dirs, files in fs.walk(root):
                    for fname in files:
                        ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                        if ext not in extensions:
                            continue
                        file_path = f"{dirpath}/{fname}"
                        uri = f"{_proto}://{file_path}"
                        try:
                            raw = fs.cat_file(file_path)
                        except Exception as e:  # noqa: BLE001
                            context.log.warning(f"Skipping {fname} (read failed: {e})")
                            skipped_files.append({"filename": fname, "doc_id": uri, "reason": str(e)})
                            continue
                        try:
                            _mtime = fs.modified(file_path)
                            updated_at = _mtime.isoformat() if hasattr(_mtime, "isoformat") else str(_mtime)
                        except Exception:  # noqa: BLE001
                            updated_at = pd.Timestamp.now(tz="UTC").isoformat()
                        _add_row(uri, raw, fname, ext, uri, updated_at)

            elif provider == "webdav":
                try:
                    import fsspec
                except ImportError as e:
                    raise ImportError(f"cloud_documents: provider='webdav' requires fsspec: {e}")
                auth = None
                if webdav_username and webdav_password_env_var:
                    auth = (webdav_username, _require_env(webdav_password_env_var))
                extra_kwargs: Dict[str, Any] = {}
                if webdav_token_env_var:
                    extra_kwargs["headers"] = {"Authorization": f"Bearer {_require_env(webdav_token_env_var)}"}
                try:
                    fs = fsspec.filesystem("webdav", base_url=webdav_base_url, auth=auth, **extra_kwargs)
                except ImportError as e:
                    raise ImportError(
                        "cloud_documents: provider='webdav' requires the 'webdav4' package "
                        f"(pip install webdav4) -- fsspec knows the 'webdav' protocol but has "
                        f"no backend installed here: {e}"
                    )
                root = source_path or "/"
                for dirpath, _dirs, files in fs.walk(root):
                    for fname in files:
                        ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                        if ext not in extensions:
                            continue
                        file_path = f"{dirpath.rstrip('/')}/{fname}"
                        try:
                            raw = fs.cat_file(file_path)
                        except Exception as e:  # noqa: BLE001
                            context.log.warning(f"Skipping {fname} (read failed: {e})")
                            skipped_files.append({"filename": fname, "doc_id": file_path, "reason": str(e)})
                            continue
                        try:
                            _mtime = fs.modified(file_path)
                            updated_at = _mtime.isoformat() if hasattr(_mtime, "isoformat") else str(_mtime)
                        except Exception:  # noqa: BLE001
                            updated_at = pd.Timestamp.now(tz="UTC").isoformat()
                        _add_row(file_path, raw, fname, ext, f"{webdav_base_url.rstrip('/')}{file_path}", updated_at)

            elif provider in ("onedrive", "sharepoint"):
                _token_resp = requests.post(
                    f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": "https://graph.microsoft.com/.default",
                    },
                )
                _token_resp.raise_for_status()
                access_token = _token_resp.json()["access_token"]
                headers = {"Authorization": f"Bearer {access_token}"}

                if provider == "onedrive":
                    _drive_id = drive_id
                else:
                    _site_path = site_path.lstrip("/")
                    _site_resp = requests.get(
                        f"https://graph.microsoft.com/v1.0/sites/{site_hostname}:/{_site_path}",
                        headers=headers,
                    )
                    _site_resp.raise_for_status()
                    _site_id = _site_resp.json()["id"]
                    _drive_resp = requests.get(
                        f"https://graph.microsoft.com/v1.0/sites/{_site_id}/drive",
                        headers=headers,
                    )
                    _drive_resp.raise_for_status()
                    _drive_id = _drive_resp.json()["id"]

                _list_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/drives/{_drive_id}/root/children",
                    headers=headers,
                )
                _list_resp.raise_for_status()
                items = _list_resp.json().get("value", [])
                for item in items:
                    if "file" not in item:
                        continue  # a folder -- root children only, no recursion (see module docstring)
                    fname = item.get("name") or item.get("id")
                    ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                    if ext not in extensions:
                        continue
                    download_url = item.get("@microsoft.graph.downloadUrl")
                    try:
                        if download_url:
                            _content_resp = requests.get(download_url)
                        else:
                            _content_resp = requests.get(
                                f"https://graph.microsoft.com/v1.0/drives/{_drive_id}/items/{item['id']}/content",
                                headers=headers,
                            )
                        _content_resp.raise_for_status()
                        raw = _content_resp.content
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(f"Skipping {fname} (download failed: {e})")
                        skipped_files.append({"filename": fname, "doc_id": str(item.get("id")), "reason": str(e)})
                        continue
                    _add_row(
                        item["id"], raw, fname, ext,
                        item.get("webUrl") or download_url or "",
                        item.get("lastModifiedDateTime") or pd.Timestamp.now(tz="UTC").isoformat(),
                    )

            elif provider == "box":
                _token_resp = requests.post(
                    "https://api.box.com/oauth2/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "box_subject_type": box_subject_type,
                        "box_subject_id": box_subject_id,
                    },
                )
                _token_resp.raise_for_status()
                access_token = _token_resp.json()["access_token"]
                headers = {"Authorization": f"Bearer {access_token}"}
                _list_resp = requests.get(
                    f"https://api.box.com/2.0/folders/{folder_id or '0'}/items",
                    params={"fields": "id,name,modified_at,size,sha1,type"},
                    headers=headers,
                )
                _list_resp.raise_for_status()
                entries = _list_resp.json().get("entries", [])
                for item in entries:
                    if item.get("type") != "file":
                        continue  # root children only, no recursion into subfolders
                    fname = item.get("name") or item.get("id")
                    ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                    if ext not in extensions:
                        continue
                    try:
                        _content_resp = requests.get(
                            f"https://api.box.com/2.0/files/{item['id']}/content",
                            headers=headers,
                        )
                        _content_resp.raise_for_status()
                        raw = _content_resp.content
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(f"Skipping {fname} (download failed: {e})")
                        skipped_files.append({"filename": fname, "doc_id": str(item.get("id")), "reason": str(e)})
                        continue
                    _add_row(
                        item["id"], raw, fname, ext,
                        f"https://app.box.com/file/{item['id']}",
                        item.get("modified_at") or pd.Timestamp.now(tz="UTC").isoformat(),
                    )

            elif provider == "dropbox":
                _token_resp = requests.post(
                    "https://api.dropboxapi.com/oauth2/token",
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": client_id,
                        "client_secret": client_secret,
                    },
                    timeout=60,
                )
                _token_resp.raise_for_status()
                access_token = _token_resp.json()["access_token"]
                headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                _resp = requests.post(
                    "https://api.dropboxapi.com/2/files/list_folder",
                    json={"path": folder_path or "", "recursive": recursive},
                    headers=headers,
                    timeout=60,
                )
                _resp.raise_for_status()
                _payload = _resp.json()
                entries = list(_payload.get("entries", []))
                _cursor = _payload.get("cursor")
                _has_more = _payload.get("has_more", False)
                while _has_more:
                    _resp = requests.post(
                        "https://api.dropboxapi.com/2/files/list_folder/continue",
                        json={"cursor": _cursor},
                        headers=headers,
                        timeout=60,
                    )
                    _resp.raise_for_status()
                    _payload = _resp.json()
                    entries.extend(_payload.get("entries", []))
                    _cursor = _payload.get("cursor")
                    _has_more = _payload.get("has_more", False)

                for item in entries:
                    if item.get(".tag") != "file":
                        continue
                    fname = item.get("name") or item.get("id")
                    ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                    if ext not in extensions:
                        continue
                    path_lower = item.get("path_lower")
                    try:
                        _content_resp = requests.post(
                            "https://content.dropboxapi.com/2/files/download",
                            headers={
                                "Authorization": f"Bearer {access_token}",
                                "Dropbox-API-Arg": json.dumps({"path": path_lower}),
                            },
                            timeout=120,
                        )
                        _content_resp.raise_for_status()
                        raw = _content_resp.content
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(f"Skipping {fname} (download failed: {e})")
                        skipped_files.append({"filename": fname, "doc_id": str(item.get("id")), "reason": str(e)})
                        continue
                    _add_row(
                        item.get("id") or path_lower, raw, fname, ext,
                        item.get("path_display") or path_lower or "",
                        item.get("server_modified") or pd.Timestamp.now(tz="UTC").isoformat(),
                    )

            else:
                raise ValueError(f"cloud_documents: unsupported provider {provider!r}")

            if not rows:
                context.log.warning(f"No extractable documents found for provider={provider!r}.")

            @dlt.resource(name="documents")
            def _documents_resource():
                yield from rows

            dlt_resources = [_documents_resource()]

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

            context.log.info(f"Loading {len(rows)} document row(s) into destination...")
            load_info = pipeline.run(dlt_resources)
            context.log.info(f"cloud_documents data loaded: {load_info}")

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "provider": MetadataValue.text(provider),
                "documents_extracted": MetadataValue.int(len(rows)),
                "skipped_files": MetadataValue.int(len(skipped_files)),
                "skipped_files_detail": MetadataValue.json(skipped_files[:200]),
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
                    with client.execute_query(f"SELECT * FROM {dataset_name}.documents") as cur:
                        df = cur.df()
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"Could not read back documents table: {e}")
                    df = pd.DataFrame(rows)

            df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)

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
                except Exception as _e:  # noqa: BLE001
                    context.log.warning(f"preview emission failed: {_e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[cloud_documents_asset])


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
