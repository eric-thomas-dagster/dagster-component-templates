"""GoogleDriveIngestionComponent — list (and optionally download) Google Drive files.

Service-account-authenticated reader for Google Drive. Returns a pandas
DataFrame, one row per file matching the query/folder/MIME filter, with
optional inline file content (bytes) for small files.

For Google Sheets specifically, prefer `google_sheets_ingestion`.
For Google Docs text extraction, prefer `google_docs_extractor`.
This component is the right pick for everything else: PDFs, CSVs, images,
arbitrary uploaded files.
"""

import io
import json
import os
from typing import Any, Dict, List, Optional

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
        raise ValueError(
            "Set either partition_type or partition_dimensions, not both."
        )

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


# BEGIN binary-document text extraction (inlined per component; keep copies identical if duplicated elsewhere)
_MIME_PDF = "application/pdf"
_MIME_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _extract_pdf_text(data: bytes, log=None) -> Optional[str]:
    """Extract text from PDF bytes via pdfplumber. Returns None on any failure -- including a missing
    optional dependency -- never raises: one unparseable file shouldn't fail the whole Drive listing,
    matching the rest of this extraction path's existing None-on-failure contract."""
    import io as _io

    try:
        import pdfplumber
    except ImportError:
        if log:
            log.warning("pdfplumber not installed; skipping PDF text extraction. pip install pdfplumber")
        return None
    try:
        with pdfplumber.open(_io.BytesIO(data)) as pdf:
            parts = [page.extract_text() or "" for page in pdf.pages]
        return "\n\n".join(parts)
    except Exception as e:
        if log:
            log.warning(f"PDF text extraction failed: {e}")
        return None


def _extract_docx_text(data: bytes, log=None) -> Optional[str]:
    """Extract text from .docx bytes via python-docx. Same None-on-any-failure contract as _extract_pdf_text."""
    import io as _io

    try:
        import docx
    except ImportError:
        if log:
            log.warning("python-docx not installed; skipping .docx text extraction. pip install python-docx")
        return None
    try:
        document = docx.Document(_io.BytesIO(data))
        return "\n".join(p.text for p in document.paragraphs)
    except Exception as e:
        if log:
            log.warning(f".docx text extraction failed: {e}")
        return None


def _extract_xlsx_text(data: bytes, log=None) -> Optional[str]:
    """Render .xlsx bytes as a simple text blob (one line per row, sheets separated by a header) via
    openpyxl, so spreadsheet content is visible to a downstream LLM reading `content`. Same
    None-on-any-failure contract as _extract_pdf_text."""
    import io as _io

    try:
        import openpyxl
    except ImportError:
        if log:
            log.warning("openpyxl not installed; skipping .xlsx text extraction. pip install openpyxl")
        return None
    try:
        wb = openpyxl.load_workbook(_io.BytesIO(data), data_only=True, read_only=True)
        parts = []
        for sheet in wb.worksheets:
            parts.append(f"=== {sheet.title} ===")
            for row in sheet.iter_rows(values_only=True):
                cells = [str(c) for c in row if c is not None]
                if cells:
                    parts.append(", ".join(cells))
        return "\n".join(parts)
    except Exception as e:
        if log:
            log.warning(f".xlsx text extraction failed: {e}")
        return None
# END binary-document text extraction


class GoogleDriveIngestionComponent(Component, Model, Resolvable):
    """List (and optionally download) Google Drive files via a service account.

    Returns a pandas DataFrame, one row per file matching the configured
    folder / query / MIME filters. Optionally embeds file contents for
    small files; for large files, downloads to disk and writes the path.
    """

    asset_name: str = Field(description="Output asset name.")

    # ── Auth ─────────────────────────────────────────────────────────────
    credentials: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Service account credentials as a dict. Set this OR credentials_path OR rely on GOOGLE_APPLICATION_CREDENTIALS.",
    )
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to the service-account JSON file.",
    )

    # ── What to fetch ───────────────────────────────────────────────────
    folder_id: Optional[str] = Field(
        default=None,
        description="Drive folder ID to list. Mutually exclusive with `query`.",
    )
    query: Optional[str] = Field(
        default=None,
        description=(
            "Drive search query (Drive's `q` syntax). Examples: `mimeType='application/pdf'`, `name contains 'report'`. Mutually exclusive with `folder_id`. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
        ),
    )
    mime_type_filter: Optional[List[str]] = Field(
        default=None,
        description="Optional list of MIME types to keep (e.g. ['application/pdf']).",
    )
    include_trashed: bool = Field(
        default=False,
        description="Include trashed files in the listing.",
    )
    max_files: int = Field(
        default=100,
        description="Stop after this many files (avoids accidental full-Drive scans).",
    )

    # ── Download behavior ───────────────────────────────────────────────
    download: bool = Field(
        default=False,
        description="If True, also download file contents.",
    )
    download_dir: str = Field(
        default="/tmp/google_drive_ingestion",
        description="Where to write downloaded files. Created on first use.",
    )
    download_max_bytes: int = Field(
        default=10 * 1024 * 1024,
        description="Skip files larger than this. Default 10 MB.",
    )
    inline_text_content: bool = Field(
        default=False,
        description=(
            "When `download=True`, also write extracted text into a `content` column on the "
            "DataFrame: plain-text-ish files (text/*, json, csv) are UTF-8 decoded; PDF, .docx and "
            ".xlsx files have their text extracted via pdfplumber / python-docx / openpyxl "
            "(optional deps -- falls back to `content=None` with a logged warning if not installed, "
            "or if a given file fails to parse). Every downloaded file always gets a `path` column too."
        ),
    )

    # ── Standard Dagster attrs ──────────────────────────────────────────
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

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
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=('partition_values',), tags=('tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        if self.folder_id and self.query:
            raise ValueError(
                "GoogleDriveIngestionComponent: set folder_id OR query, not both."
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
                "GoogleDriveIngestionComponent: provide one of `credentials` (dict), "
                "`credentials_path`, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        asset_name = self.asset_name
        folder_id = self.folder_id
        query = self.query
        mime_type_filter = self.mime_type_filter
        include_trashed = self.include_trashed
        max_files = self.max_files
        download_flag = self.download
        download_dir = self.download_dir
        download_max_bytes = self.download_max_bytes
        inline_text_content = self.inline_text_content

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Google Drive listing ({folder_id or query or 'all'}).",
            group_name=self.group_name,
            kinds={"google", "drive"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.oauth2 import service_account
                from googleapiclient.discovery import build
                from googleapiclient.http import MediaIoBaseDownload
            except ImportError:
                raise ImportError(
                    "Install google-auth and google-api-python-client: "
                    "pip install google-auth google-api-python-client"
                )

            scopes = ["https://www.googleapis.com/auth/drive.readonly"]
            sa_creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scopes,
            )
            svc = build("drive", "v3", credentials=sa_creds, cache_discovery=False)

            # Build the q parameter.
            qparts = []
            if folder_id:
                qparts.append(f"'{folder_id}' in parents")
            if _substitute_partition(query, _partition_vars(context)):
                qparts.append(_substitute_partition(query, _partition_vars(context)))
            if not include_trashed:
                qparts.append("trashed=false")
            if mime_type_filter:
                mime_clauses = " or ".join(f"mimeType='{m}'" for m in mime_type_filter)
                qparts.append(f"({mime_clauses})")
            q_str = " and ".join(qparts) if qparts else None

            context.log.info(f"Drive list query: q={q_str!r}, max_files={max_files}")

            files = []
            page_token = None
            while len(files) < max_files:
                try:
                    resp = svc.files().list(
                        q=q_str,
                        fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, owners(emailAddress), webViewLink, parents)",
                        pageSize=min(100, max_files - len(files)),
                        pageToken=page_token,
                    ).execute()
                except Exception as e:
                    err_str = str(e)
                    if "403" in err_str and "SERVICE_DISABLED" in err_str:
                        context.log.error(
                            "Drive API not enabled on the service account's project. "
                            "The error message above contains the activation URL — "
                            "click it, hit Enable, wait ~30s."
                        )
                    elif "403" in err_str:
                        context.log.error(
                            "403 PERMISSION_DENIED. The service account either "
                            "lacks Drive scope or hasn't been shared on the "
                            "folder/files. Check the SA email in your JSON's "
                            "client_email field and share the relevant Drive "
                            "items with it (Viewer is sufficient)."
                        )
                    raise
                files.extend(resp.get("files", []))
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            files = files[:max_files]
            context.log.info(f"Drive returned {len(files)} files")

            df = pd.DataFrame(files)
            if df.empty:
                context.log.warning("No files matched the query.")
                df = pd.DataFrame(columns=["id", "name", "mimeType", "size", "modifiedTime"])

            # Flatten the owners column if present.
            if "owners" in df.columns:
                df["owner_email"] = df["owners"].apply(
                    lambda o: o[0]["emailAddress"] if isinstance(o, list) and o else None
                )
                df = df.drop(columns=["owners"])

            # Optional download phase.
            if download_flag and not df.empty:
                os.makedirs(download_dir, exist_ok=True)
                paths: List[Optional[str]] = []
                contents: List[Optional[str]] = []
                skipped: List[Optional[str]] = []

                for _, row in df.iterrows():
                    file_id = row["id"]
                    file_name = row.get("name") or file_id
                    mime = row.get("mimeType") or ""
                    raw_size = row.get("size")
                    try:
                        size = int(raw_size) if raw_size else None
                    except (TypeError, ValueError):
                        size = None

                    if size is not None and size > download_max_bytes:
                        paths.append(None)
                        contents.append(None)
                        skipped.append(f"size {size} > {download_max_bytes}")
                        context.log.info(f"skip {file_name!r}: {size}B > {download_max_bytes}B")
                        continue

                    # Native Google Docs / Sheets / Slides need export, not direct download.
                    is_native_google = mime.startswith("application/vnd.google-apps.")
                    if is_native_google:
                        # Skip native Google formats here — those are the job of
                        # google_docs_extractor / google_sheets_ingestion etc.
                        paths.append(None)
                        contents.append(None)
                        skipped.append(f"native Google MIME {mime} — use a dedicated component")
                        continue

                    try:
                        request = svc.files().get_media(fileId=file_id)
                        buf = io.BytesIO()
                        downloader = MediaIoBaseDownload(buf, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                        data = buf.getvalue()

                        # Sanitize filename for filesystem.
                        safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in file_name)
                        out_path = os.path.join(download_dir, f"{file_id}_{safe_name}")
                        with open(out_path, "wb") as fh:
                            fh.write(data)
                        paths.append(out_path)
                        skipped.append(None)

                        if not inline_text_content:
                            contents.append(None)
                        elif mime.startswith("text/") or mime in (
                            "application/json", "application/xml", "application/csv",
                        ):
                            try:
                                contents.append(data.decode("utf-8"))
                            except UnicodeDecodeError:
                                contents.append(None)
                        elif mime == _MIME_PDF:
                            contents.append(_extract_pdf_text(data, context.log))
                        elif mime == _MIME_DOCX:
                            contents.append(_extract_docx_text(data, context.log))
                        elif mime == _MIME_XLSX:
                            contents.append(_extract_xlsx_text(data, context.log))
                        else:
                            contents.append(None)
                    except Exception as e:
                        paths.append(None)
                        contents.append(None)
                        skipped.append(str(e))
                        context.log.warning(f"download {file_name!r} failed: {e}")

                df["path"] = paths
                df["download_skipped"] = skipped
                if inline_text_content:
                    df["content"] = contents

            _preview_rows = self.preview_rows
            preview_md = (
                (df.sample(min(_preview_rows, len(df))) if len(df) > _preview_rows * 10 else df.head(_preview_rows)).to_markdown(index=False)
                if self.include_preview_metadata and not df.empty else "(empty)"
            )
            md = {
                "row_count": MetadataValue.int(len(df)),
                "query":     MetadataValue.text(q_str or ""),
                "preview":   MetadataValue.md(preview_md or ""),
            }
            if download_flag:
                md["download_dir"] = MetadataValue.path(download_dir)
                md["downloaded_count"] = MetadataValue.int(
                    int(df.get("path", pd.Series(dtype=object)).notna().sum())
                )
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

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
