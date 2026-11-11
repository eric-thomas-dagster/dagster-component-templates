"""GoogleDriveDocumentsComponent — extract RAG-ready document rows from EVERY file in a Google
Drive folder, any type, via a service account.

This repo already has two narrower Google Drive components:

  - `google_docs_extractor` -- native Google Docs ONLY (its own `drive_folder_id` field
    filters to `mimeType='application/vnd.google-apps.document'`).
  - `google_drive_ingestion` -- lists any file type, optionally downloads it, and (with
    `inline_text_content=True`) extracts a *subset* of formats (.pdf/.docx/.xlsx via
    pdfplumber/python-docx/openpyxl) into a `content` column -- but it explicitly SKIPS
    native Google formats (Docs/Sheets/Slides): "use a dedicated component" (its own
    comment). Its output is a raw file-listing shape (id/name/mimeType/size/modifiedTime/
    owner_email/path/content/download_skipped), not the document-source contract.

Neither handles "give me RAG-ready rows for every file in this folder, whatever it is."
`GoogleDriveDocumentsComponent` fills that gap specifically:

  - Lists EVERY file in the folder (any mimeType), optionally recursing into subfolders.
  - Native Google formats (Docs/Sheets/Slides -- mimeType starts with
    `application/vnd.google-apps.`) are EXPORTED via `files().export(fileId=..., mimeType=...)`,
    not downloaded raw (Drive API has no raw byte representation for them). Verified-documented
    export mimeTypes used here: Docs -> `text/plain`, Sheets -> `text/csv` (a real, documented
    Drive API limitation: CSV export of a multi-sheet Spreadsheet only exports the FIRST sheet --
    not re-verified live in this session, carried over from Drive API v3 docs), Slides ->
    `text/plain`. Drive API cap: `files().export` is limited to files under 10MB of exported
    content (a real, documented Drive API constraint, not something this component works around).
  - Everything else (uploaded PDFs, .docx, .pptx, .xlsx, images, plain files, ...) downloads via
    `files().get_media(fileId=...)` (chunked via `MediaIoBaseDownload`, same pattern
    `google_drive_ingestion` already uses) and is routed through the SAME extraction dispatcher
    `document_corpus`/`cloud_documents`/`document_ingestion` use this session: pdfplumber (.pdf),
    python-docx (.docx), BeautifulSoup (.html/.htm), and Microsoft's `markitdown` library for
    .pptx/.xlsx/.xls/.epub/audio (verified real API:
    `MarkItDown(enable_plugins=False).convert_stream(stream, file_extension=...).markdown`).
  - `updated_at` comes directly from `modifiedTime` on the `files().list()` response -- no
    second per-file `files().get()` call needed (unlike `google_docs_extractor`, which needs one
    because the Docs API itself exposes no modifiedTime). `url` is `webViewLink` from the same
    listing response.
  - Recursive subfolders: optional, OFF by default (`recursive: false`) -- the simplest/safest
    behavior, matching this repo's convention of starting non-recursive and making recursion
    opt-in (see `onedrive_ingestion`/`sharepoint_ingestion`/`box_ingestion`'s own root-children-
    only default in `cloud_documents`'s module docstring). When true, subfolders are discovered
    from `mimeType='application/vnd.google-apps.folder'` entries in the SAME listing call and
    walked breadth-first via repeated `'<id>' in parents` queries -- no separate recursive-search
    API needed.

Document source contract (every row): `doc_id`, `content` (real extracted text), `title`
(filename/doc title, minus extension), `url`, `source_type` (literal `"google_drive"`),
`updated_at`, `content_hash` (sha256 of content) -- matching `document_corpus`/
`document_ingestion`/`cloud_documents` so a knowledge-base UI / `document_chunker` can treat this
source identically to every other document source regardless of vendor.

Auth: identical convention to `google_docs_extractor` -- `from google.oauth2 import
service_account; service_account.Credentials.from_service_account_info(...)`, built via
`googleapiclient.discovery.build("drive", "v3", credentials=sa_creds, cache_discovery=False)`.
Only needs the `drive.readonly` scope (covers listing, export, AND get_media -- no separate Docs
API scope needed, unlike `google_docs_extractor`, since export happens through the Drive API
itself for all three native formats).
"""

import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
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


# Native Google Workspace mimeTypes that must be EXPORTED (not downloaded raw), mapped to the
# export mimeType + resulting extension this component asks the Drive API for. Verified-documented
# Drive API v3 export targets (https://developers.google.com/drive/api/guides/ref-export-formats):
# Docs and Slides both support `text/plain`; Sheets supports `text/csv` (first sheet only -- a
# real, documented Drive API limitation).
_NATIVE_EXPORT = {
    "application/vnd.google-apps.document": ("text/plain", ".txt"),
    "application/vnd.google-apps.presentation": ("text/plain", ".txt"),
    "application/vnd.google-apps.spreadsheet": ("text/csv", ".csv"),
}
_NATIVE_PREFIX = "application/vnd.google-apps."
_FOLDER_MIME = "application/vnd.google-apps.folder"

# Fallback extension lookup for uploaded files whose `name` carries no extension -- the Drive API
# itself does not require an uploaded file to have an extension in its name.
_MIME_TO_EXT = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "application/epub+zip": ".epub",
    "text/plain": ".txt",
    "text/html": ".html",
    "text/csv": ".csv",
    "text/markdown": ".md",
    "application/json": ".json",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "audio/mpeg": ".mp3",
    "audio/wav": ".wav",
}


def _extract_text_from_bytes(data: bytes, suffix: str, encoding: str = "utf-8") -> str:
    """Dispatches by file extension -- same libraries/contract as document_corpus/cloud_documents/
    document_ingestion: pdfplumber (.pdf), python-docx (.docx), BeautifulSoup (.html/.htm),
    markitdown (.pptx/.xlsx/.xls/.epub/audio), everything else decoded as plain text. Raises on a
    real extraction failure -- the caller decides whether to skip or fail the run."""
    suffix = suffix.lower()
    if suffix == ".pdf":
        import pdfplumber

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(page.extract_text() or "" for page in pdf.pages)
    if suffix == ".docx":
        import docx

        d = docx.Document(io.BytesIO(data))
        return "\n".join(p.text for p in d.paragraphs)
    if suffix in (".html", ".htm"):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(data.decode(encoding, errors="replace"), "html.parser")
        return soup.get_text(separator="\n")
    if suffix in (".pptx", ".xlsx", ".xls", ".epub", ".mp3", ".wav", ".m4a"):
        return _extract_with_markitdown(data, suffix)
    if suffix == ".doc":
        raise NotImplementedError("legacy .doc format has no extraction library available")
    return data.decode(encoding)


def _extract_with_markitdown(data: bytes, suffix: str) -> str:
    """Formats pdfplumber/python-docx/BeautifulSoup don't cover: PowerPoint, Excel, EPub, audio.
    Verified real API: `MarkItDown(enable_plugins=False).convert_stream(stream,
    file_extension=...).markdown` -- no LLM required for any of these formats."""
    try:
        from markitdown import MarkItDown
    except ImportError:
        raise ImportError(
            f"google_drive_documents: extracting {suffix!r} files requires: pip install "
            f"'markitdown[pptx,xlsx,docx,pdf]' (or markitdown[all])."
        )
    result = MarkItDown(enable_plugins=False).convert_stream(io.BytesIO(data), file_extension=suffix)
    return result.markdown


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
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
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
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


class GoogleDriveDocumentsComponent(Component, Model, Resolvable):
    """Extract RAG-ready document rows from EVERY file in a Google Drive folder, any type, via a
    service account. See the module docstring for the full design (native-format export vs raw
    download, the document source contract, and verified Drive API behavior).

    Example:

        ```yaml
        type: dagster_community_components.GoogleDriveDocumentsComponent
        attributes:
          asset_name: drive_documents
          credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
          drive_folder_id: "1AbCdEfGhIjKlMnOpQrStUvWxYz0123456"
        ```
    """

    asset_name: str = Field(description="Output asset name.")

    # Auth
    credentials: Optional[Dict[str, Any]] = Field(default=None, description="Service account credentials as a dict. Set this OR credentials_path OR rely on GOOGLE_APPLICATION_CREDENTIALS.")
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to service-account JSON. Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    # Source
    drive_folder_id: str = Field(
        description=(
            "Google Drive folder ID to read every file from (any type -- PDFs, Google Sheets, "
            "Google Slides, uploaded .docx/.pptx/.xlsx/images/etc). The folder itself IS the "
            "source (like document_corpus/document_ingestion's own source_dir) -- there is no "
            "upstream_asset_key mode here."
        ),
    )
    recursive: bool = Field(
        default=False,
        description=(
            "Also walk subfolders (discovered via mimeType='application/vnd.google-apps.folder' "
            "entries in the same listing call) and extract files found inside them. Default "
            "False: only the immediate contents of drive_folder_id, matching this repo's "
            "non-recursive-by-default convention for Drive/cloud folder listings."
        ),
    )
    include_trashed: bool = Field(default=False, description="Include trashed files in the listing.")
    max_files: int = Field(
        default=500,
        description="Stop after this many files total (across all subfolders if recursive) -- avoids accidental full-Drive scans.",
    )
    download_max_bytes: int = Field(
        default=10 * 1024 * 1024,
        description="Skip non-native (uploaded) files larger than this before downloading. Default 10 MB. Native Google-format exports have no `size` field and are instead bounded by the Drive API's own ~10MB export cap.",
    )
    rate_limit_delay: float = Field(default=0.0, description="Seconds to sleep between per-file export/download calls.")

    # Standard Dagster attrs
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['google', 'drive', 'rag', 'documents'] when unset.",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}. The upstream asset is the only entry of `deps` (there is no upstream_asset_key here). Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_type: Optional[str] = Field(default=None)
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
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current date partition (time partitions). E.g. 'updated_at'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current static/dynamic partition key.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
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
        default=True,
        description="Include a markdown preview of the fetched rows in the materialization metadata.",
    )
    preview_rows: int = Field(
        default=10,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Google Drive Documents', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        # Resolve credentials.
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "GoogleDriveDocumentsComponent: provide one of `credentials`, "
                "`credentials_path`, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column

        asset_name = self.asset_name
        drive_folder_id = self.drive_folder_id
        recursive = self.recursive
        include_trashed = self.include_trashed
        max_files = self.max_files
        download_max_bytes = self.download_max_bytes
        rate_limit_delay = self.rate_limit_delay

        _kinds = set(self.kinds) if self.kinds else {"google", "drive", "rag", "documents"}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Document rows extracted from every file in a Google Drive folder.",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)

            try:
                from google.oauth2 import service_account
                from googleapiclient.discovery import build
                from googleapiclient.http import MediaIoBaseDownload
            except ImportError:
                raise ImportError(
                    "Install google-auth and google-api-python-client: "
                    "pip install google-auth google-api-python-client"
                )

            sa_creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"],
            )
            drive_svc = build("drive", "v3", credentials=sa_creds, cache_discovery=False)

            # ---- Phase 1: list every file in the folder (optionally recursive). ----
            trashed_clause = "" if include_trashed else " and trashed=false"
            list_fields = "nextPageToken, files(id, name, mimeType, modifiedTime, webViewLink, size)"

            all_files: List[Dict[str, Any]] = []
            queue: List[str] = [drive_folder_id]
            seen_folders = set()
            while queue and len(all_files) < max_files:
                folder_id = queue.pop(0)
                if folder_id in seen_folders:
                    continue
                seen_folders.add(folder_id)
                page_token = None
                while True:
                    resp = drive_svc.files().list(
                        q=f"'{folder_id}' in parents{trashed_clause}",
                        fields=list_fields,
                        pageSize=min(100, max(1, max_files - len(all_files))),
                        pageToken=page_token,
                    ).execute()
                    for f in resp.get("files", []):
                        if f.get("mimeType") == _FOLDER_MIME:
                            if recursive:
                                queue.append(f["id"])
                            continue
                        all_files.append(f)
                        if len(all_files) >= max_files:
                            break
                    page_token = resp.get("nextPageToken")
                    if not page_token or len(all_files) >= max_files:
                        break

            context.log.info(
                f"google_drive_documents: found {len(all_files)} file(s) in folder {drive_folder_id!r} "
                f"(recursive={recursive})"
            )

            # ---- Phase 2: export (native) or download (everything else) + extract text. ----
            rows: List[Dict[str, Any]] = []
            skipped_files: List[Dict[str, str]] = []
            import time as _time

            for f in all_files:
                file_id = f["id"]
                name = f.get("name") or file_id
                mime = f.get("mimeType") or ""

                try:
                    if mime.startswith(_NATIVE_PREFIX):
                        export_mime, ext = _NATIVE_EXPORT.get(mime, (None, None))
                        if export_mime is None:
                            skipped_files.append({"filename": name, "doc_id": file_id, "reason": f"unsupported native Google mimeType: {mime}"})
                            continue
                        raw = drive_svc.files().export(fileId=file_id, mimeType=export_mime).execute()
                    else:
                        raw_size = f.get("size")
                        try:
                            size = int(raw_size) if raw_size else None
                        except (TypeError, ValueError):
                            size = None
                        if size is not None and size > download_max_bytes:
                            skipped_files.append({"filename": name, "doc_id": file_id, "reason": f"size {size} > download_max_bytes {download_max_bytes}"})
                            continue
                        ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else _MIME_TO_EXT.get(mime)
                        if not ext:
                            skipped_files.append({"filename": name, "doc_id": file_id, "reason": f"no extension in name and no mimeType mapping for {mime!r}"})
                            continue
                        request = drive_svc.files().get_media(fileId=file_id)
                        buf = io.BytesIO()
                        downloader = MediaIoBaseDownload(buf, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                        raw = buf.getvalue()

                    if isinstance(raw, str):
                        raw = raw.encode("utf-8")
                    content = _extract_text_from_bytes(raw, ext)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"Skipping {name!r} ({e})")
                    skipped_files.append({"filename": name, "doc_id": file_id, "reason": str(e)})
                    continue

                if not content or not content.strip():
                    context.log.warning(f"{name!r}: empty extracted content, dropping (not emitted as a null-content row)")
                    skipped_files.append({"filename": name, "doc_id": file_id, "reason": "empty extracted content"})
                    continue

                rows.append({
                    # Document source contract (shared by every document-source ingestion
                    # component feeding document_chunker): doc_id/content/title/url/
                    # source_type/updated_at/content_hash.
                    "doc_id": file_id,
                    "content": content,
                    "title": Path(name).stem,
                    "url": f.get("webViewLink") or "",
                    "source_type": "google_drive",
                    "updated_at": f.get("modifiedTime"),
                    "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                })
                if rate_limit_delay:
                    _time.sleep(rate_limit_delay)

            if not rows:
                context.log.warning("No extractable documents found.")

            df = pd.DataFrame(
                rows,
                columns=["doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"],
            )
            df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)

            from dagster import TableColumn, TableSchema

            _preview_rows = self.preview_rows
            preview_md = (
                (df.sample(min(_preview_rows, len(df))) if len(df) > _preview_rows * 10 else df.head(_preview_rows)).to_markdown(index=False)
                if self.include_preview_metadata and not df.empty else "(empty)"
            )
            md = {
                "row_count": MetadataValue.int(len(df)),
                "files_found": MetadataValue.int(len(all_files)),
                "skipped_files": MetadataValue.int(len(skipped_files)),
                "skipped_files_detail": MetadataValue.json(skipped_files[:200]),
                "dagster/column_schema": MetadataValue.table_schema(
                    TableSchema(columns=[TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns])
                ),
                "preview": MetadataValue.md(preview_md or ""),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])


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
