"""GoogleDocsExtractorComponent — extract plain text from Google Docs.

Service-account-authenticated reader for Google Docs. Two input modes:

  - explicit `doc_ids: [...]` — fetch each Doc by ID.
  - upstream DataFrame via `upstream_asset_key` + `id_column` — typical
    pairing with `google_drive_ingestion` (filter Drive to
    application/vnd.google-apps.document, then feed the IDs here).

Returns a pandas DataFrame with one row per Doc: id, title, plain text,
optional headings list, word count.
"""

import json
import os
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
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

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


def _extract_doc_text(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Walk a Google Docs API document body and pull plain text + headings."""
    title = doc.get("title", "")
    body = doc.get("body", {})
    elements = body.get("content", [])

    text_parts: List[str] = []
    headings: List[str] = []

    def _walk(elements: List[Any]) -> None:
        for el in elements:
            para = el.get("paragraph")
            if para:
                style_type = para.get("paragraphStyle", {}).get("namedStyleType", "")
                pieces = []
                for sub in para.get("elements", []):
                    tr = sub.get("textRun")
                    if tr:
                        pieces.append(tr.get("content", ""))
                line = "".join(pieces)
                text_parts.append(line)
                if style_type.startswith("HEADING_"):
                    headings.append(line.strip())
                continue
            tbl = el.get("table")
            if tbl:
                for row in tbl.get("tableRows", []):
                    for cell in row.get("tableCells", []):
                        _walk(cell.get("content", []))
                continue
            toc = el.get("tableOfContents")
            if toc:
                _walk(toc.get("content", []))

    _walk(elements)
    text = "".join(text_parts)
    return {
        "title":    title,
        "text":     text,
        "headings": headings,
        "word_count": len(text.split()),
    }


class GoogleDocsExtractorComponent(Component, Model, Resolvable):
    """Extract plain text from Google Docs by ID via a service account.

    Use either an explicit list of doc_ids OR pair with an upstream
    DataFrame (typically google_drive_ingestion filtered to
    application/vnd.google-apps.document).
    """

    asset_name: str = Field(description="Output asset name.")

    # Auth
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to service-account JSON. Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    # Source: explicit list OR upstream DataFrame
    doc_ids: Optional[List[str]] = Field(
        default=None,
        description="Explicit list of Google Doc IDs. Mutually exclusive with upstream_asset_key.",
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream DataFrame asset key. Each row's id_column value is treated as a Doc ID.",
    )
    id_column: str = Field(
        default="id",
        description="When upstream_asset_key is set, the column with the Doc IDs.",
    )

    include_headings: bool = Field(default=True, description="Include `headings` list column.")
    include_text: bool = Field(default=True, description="Include the full plain-text `text` column.")

    rate_limit_delay: float = Field(default=0.2, description="Seconds between Doc fetches.")

    # Standard Dagster attrs
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

        if not self.doc_ids and not self.upstream_asset_key:
            raise ValueError(
                "GoogleDocsExtractorComponent: set either doc_ids or upstream_asset_key."
            )
        if self.doc_ids and self.upstream_asset_key:
            raise ValueError(
                "GoogleDocsExtractorComponent: set doc_ids OR upstream_asset_key, not both."
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
                "GoogleDocsExtractorComponent: provide one of `credentials`, "
                "`credentials_path`, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        explicit_doc_ids = list(self.doc_ids) if self.doc_ids else None
        upstream_asset_key = self.upstream_asset_key
        id_column = self.id_column
        include_headings = self.include_headings
        include_text = self.include_text
        rate_limit_delay = self.rate_limit_delay

        ins_kwargs: Dict[str, Any] = {}
        if upstream_asset_key:
            ins_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Google Docs text extraction.",
            group_name=self.group_name,
            kinds={"google", "docs"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            **ins_kwargs,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> Output:
            try:
                from google.oauth2 import service_account
                from googleapiclient.discovery import build
            except ImportError:
                raise ImportError(
                    "Install google-auth and google-api-python-client: "
                    "pip install google-auth google-api-python-client"
                )

            scopes = ["https://www.googleapis.com/auth/documents.readonly"]
            sa_creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scopes,
            )
            svc = build("docs", "v1", credentials=sa_creds, cache_discovery=False)

            # Build the doc-id list.
            if explicit_doc_ids is not None:
                ids: List[str] = list(explicit_doc_ids)
            else:
                upstream = kwargs.get("upstream")
                if upstream is None or not isinstance(upstream, pd.DataFrame):
                    raise RuntimeError("upstream input was missing or not a DataFrame")
                if id_column not in upstream.columns:
                    raise ValueError(
                        f"id_column={id_column!r} not in upstream columns: {list(upstream.columns)}"
                    )
                ids = [str(x) for x in upstream[id_column].dropna().tolist()]

            context.log.info(f"Extracting {len(ids)} Google Docs")

            rows = []
            errors = []
            import time as _time
            for i, doc_id in enumerate(ids):
                try:
                    doc = svc.documents().get(documentId=doc_id).execute()
                    parsed = _extract_doc_text(doc)
                    row = {
                        "id":         doc_id,
                        "title":      parsed["title"],
                        "word_count": parsed["word_count"],
                    }
                    if include_text:
                        row["text"] = parsed["text"]
                    if include_headings:
                        row["headings"] = parsed["headings"]
                    rows.append(row)
                    errors.append(None)
                except Exception as e:
                    err_str = str(e)
                    if "403" in err_str and "SERVICE_DISABLED" in err_str:
                        context.log.error(
                            "Docs API not enabled on the service account's project. "
                            "The error message above contains the activation URL."
                        )
                    elif "403" in err_str:
                        context.log.error(
                            f"doc {doc_id}: 403 PERMISSION_DENIED. Share the doc with "
                            f"the SA email (in your JSON's client_email field) — "
                            f"open https://docs.google.com/document/d/{doc_id} → Share → "
                            f"paste SA email → Viewer."
                        )
                    elif "404" in err_str:
                        context.log.error(f"doc {doc_id}: 404. ID may be wrong.")
                    else:
                        context.log.error(f"doc {doc_id}: {e}")
                    fallback = {"id": doc_id, "title": None, "word_count": 0}
                    if include_text:
                        fallback["text"] = None
                    if include_headings:
                        fallback["headings"] = None
                    rows.append(fallback)
                    errors.append(err_str)
                _time.sleep(rate_limit_delay)

            df = pd.DataFrame(rows)
            if any(errors):
                df["_error"] = errors

            preview_md = df.head(10).to_markdown(index=False) if not df.empty else "(empty)"
            md = {
                "rows":         MetadataValue.int(len(df)),
                "successful":   MetadataValue.int(int(df.get("title", pd.Series(dtype=object)).notna().sum())),
                "preview":      MetadataValue.md(preview_md or ""),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])
