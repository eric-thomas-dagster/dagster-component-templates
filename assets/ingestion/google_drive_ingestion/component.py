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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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
            "Drive search query (Drive's `q` syntax). Examples: "
            "`mimeType='application/pdf'`, `name contains 'report'`. "
            "Mutually exclusive with `folder_id`."
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
            "When `download=True` and the file is plain-text-ish (text/*, json, csv), "
            "also write the decoded content into a `content` column on the DataFrame. "
            "Binary files always get a `path` column instead."
        ),
    )

    # ── Standard Dagster attrs ──────────────────────────────────────────
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

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
            name=asset_name,
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
        def _asset(context: AssetExecutionContext) -> Output:
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
            if query:
                qparts.append(query)
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

                        if inline_text_content and (
                            mime.startswith("text/")
                            or mime in ("application/json", "application/xml", "application/csv")
                        ):
                            try:
                                contents.append(data.decode("utf-8"))
                            except UnicodeDecodeError:
                                contents.append(None)
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

            preview_md = df.head(10).to_markdown(index=False) if not df.empty else "(empty)"
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
