"""DataFrame -> Vanta evidence upload (reverse-ETL, write path).

Vanta's "Manage Vanta" API documents evidence as a three-step sequence
(confirmed against https://developer.vanta.com/docs/guides/upload-a-document
and https://developer.vanta.com/reference/createdocument /
https://developer.vanta.com/reference/uploadfilefordocument):

  1. ``POST /v1/documents``                    -- create a document record
  2. ``POST /v1/documents/{documentId}/uploads`` -- attach the evidence file
     (``multipart/form-data``, file in the ``file`` field)
  3. ``POST /v1/documents/{documentId}/submit``  -- submit for review (flips
     ``uploadStatus`` to ``OK`` and makes it visible to auditors)

This component drives that sequence once per upstream row: each row becomes
one document (or reuses an existing one via ``document_id_column``), gets
one file attached, and is optionally submitted.

This is the WRITE-path sibling of ``vanta_controls_ingestion`` (read) and
``vanta_evidence_response_agent`` (LLM narrative drafting) -- it pushes
externally-produced evidence files INTO Vanta, it does not read control
state or draft text.

A separate write path exists -- creating a *custom evidence request* on an
audit (``POST /audits/{auditId}/evidence/custom-evidence-requests``,
confirmed via https://developer.vanta.com/api-reference/audits/
create-a-custom-evidence-request-for-an-audit) -- but that endpoint asks an
auditor/operator to supply evidence later; it has no file-upload step and
doesn't fit "push this file into Vanta now", so it's out of scope here.

Auth note: the ``vanta_resource`` this component consumes is shared with
``vanta_controls_ingestion`` / ``vanta_evidence_response_agent``, both of
which only need read scopes. Writing evidence requires the OAuth token to
additionally carry ``vanta-api.documents:upload`` (plus ``vanta-api.all:write``
for the create/submit calls) -- set this on the ``VantaResourceComponent``'s
``scope:`` field, e.g.:

    scope: "vanta-api.all:read vanta-api.all:write vanta-api.documents:upload"

Pairs with:
  - ``vanta_resource`` -- OAuth2 client-credentials auth (required; needs
    write/upload scopes, see above)
"""
import mimetypes
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

# RecurrenceDuration values accepted by POST /v1/documents' `cadence` field
# (confirmed via developer.vanta.com/reference/createdocument).
_CADENCE_VALUES = {"P0D", "P1D", "P1W", "P1M", "P3M", "P6M", "P1Y", "P2Y"}
# `reminderWindow` documents a narrower subset -- no P6M/P1Y/P2Y.
_REMINDER_WINDOW_VALUES = {"P0D", "P1D", "P1W", "P1M", "P3M"}
_TIME_SENSITIVITY_VALUES = {"MOST_RECENT", "DURING_AUDIT_WINDOW"}


def _create_document(
    session,
    api_base_url: str,
    title: str,
    description: str,
    time_sensitivity: str,
    cadence: str,
    reminder_window: str,
    is_sensitive: bool,
    timeout: int,
) -> str:
    """POST /v1/documents -> new document id. Isolated so it can be
    monkeypatched wholesale in tests without real network access."""
    resp = session.post(
        f"{api_base_url.rstrip('/')}/v1/documents",
        json={
            "title": title,
            "description": description or "",
            "timeSensitivity": time_sensitivity,
            "cadence": cadence,
            "reminderWindow": reminder_window,
            "isSensitive": bool(is_sensitive),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json() if resp.content else {}
    document_id = body.get("id")
    if not document_id:
        raise RuntimeError(f"Vanta create-document response had no 'id': {body}")
    return str(document_id)


def _upload_file(
    session,
    api_base_url: str,
    document_id: str,
    file_bytes: bytes,
    file_name: str,
    mime_type: str,
    description: Optional[str],
    effective_at_date: Optional[str],
    timeout: int,
) -> str:
    """POST /v1/documents/{documentId}/uploads (multipart/form-data) ->
    upload id. The upload stays in draft status until submitted."""
    files = {"file": (file_name, file_bytes, mime_type or "application/octet-stream")}
    data: Dict[str, str] = {}
    if description:
        data["description"] = description
    if effective_at_date:
        data["effectiveAtDate"] = effective_at_date
    resp = session.post(
        f"{api_base_url.rstrip('/')}/v1/documents/{document_id}/uploads",
        files=files,
        data=data,
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json() if resp.content else {}
    return str(body.get("id", ""))


def _submit_document(session, api_base_url: str, document_id: str, timeout: int) -> None:
    """POST /v1/documents/{documentId}/submit (empty body) -- flips
    uploadStatus to OK and makes the evidence visible to auditors."""
    resp = session.post(
        f"{api_base_url.rstrip('/')}/v1/documents/{document_id}/submit",
        timeout=timeout,
    )
    resp.raise_for_status()


def _row_value(row: Dict[str, Any], col: Optional[str]):
    """Pure helper: pull a column's value out of a row dict, treating
    None/NaN/empty-string as absent."""
    if not col or col not in row:
        return None
    v = row.get(col)
    if v is None:
        return None
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:  # noqa: BLE001
        pass
    if isinstance(v, str) and not v.strip():
        return None
    return v


def _guess_mime_type(file_name: str, override: Optional[str]) -> str:
    """Pure helper: explicit override wins, else guess from filename,
    else a generic binary fallback."""
    if override:
        return override
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "application/octet-stream"


def _decode_inline_content(value) -> bytes:
    """Pure helper: `file_content_column` values are base64-encoded text by
    convention (DataFrames round-trip binary poorly); fall back to raw
    utf-8 bytes if the value isn't valid base64 (e.g. a plain-text note)."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    import base64
    import binascii
    text = str(value)
    try:
        return base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        return text.encode("utf-8")


class VantaEvidenceUploadComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upload one evidence file into Vanta per row of an upstream DataFrame.

    Each row drives the create-document -> upload-file -> submit sequence
    (the submit step is skipped per-row when ``auto_submit`` is false, or
    globally when uploading to an already-submitted document isn't desired
    yet). Supply exactly one of ``file_path_column`` (reads a local file per
    row) or ``file_content_column`` (inline bytes, base64-encoded by
    convention).

    Example:
        ```yaml
        type: dagster_component_templates.VantaEvidenceUploadComponent
        attributes:
          asset_name: vanta_access_review_evidence
          upstream_asset_key: quarterly_access_review_exports
          resource_key: vanta_resource
          document_name_column: control_title
          file_path_column: evidence_file_path
          auto_submit: true
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually "
            "exclusive with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="vanta_resource",
        description=(
            "Resource key registered by VantaResourceComponent. Must be "
            "configured with write/upload scopes -- see component docstring."
        ),
    )

    # --- document identity ---------------------------------------------

    document_id_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an EXISTING Vanta document id. When "
            "present and non-empty for a row, document creation is skipped "
            "and the file is uploaded straight to that document."
        ),
    )
    document_name_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding the document title (used as `title` "
            "when creating a new document). Ignored for rows resolved via "
            "`document_id_column`."
        ),
    )
    document_name: Optional[str] = Field(
        default=None,
        description=(
            "Static fallback document title, used when `document_name_column` "
            "is unset or a row's value is empty."
        ),
    )

    # --- evidence text ----------------------------------------------------

    evidence_description_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a human-readable description of the "
            "evidence (sent as both the document's `description` on create "
            "and the upload's `description`)."
        ),
    )
    evidence_description: Optional[str] = Field(
        default=None,
        description="Static fallback evidence description.",
    )

    # --- file source (exactly one of the next two) -------------------------

    file_path_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a local filesystem path to the evidence "
            "file. Mutually exclusive with `file_content_column`."
        ),
    )
    file_content_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding inline file content -- base64-encoded "
            "text by convention (falls back to raw utf-8 bytes if not valid "
            "base64). Mutually exclusive with `file_path_column`."
        ),
    )
    file_name_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding the filename to upload as. Defaults to "
            "the basename of `file_path_column`'s path, or a generated name "
            "when using `file_content_column`."
        ),
    )
    mime_type_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding an explicit MIME type override.",
    )
    mime_type: Optional[str] = Field(
        default=None,
        description=(
            "Static MIME type override, used when `mime_type_column` is unset "
            "or empty for a row. Falls back to guessing from the filename, "
            "then `application/octet-stream`."
        ),
    )
    effective_at_date_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an ISO-8601 date for the upload's "
            "`effectiveAtDate` (when the document is effective from)."
        ),
    )

    # --- document-creation knobs (only used when creating new documents) ---

    time_sensitivity: str = Field(
        default="MOST_RECENT",
        description="Document `timeSensitivity`: 'MOST_RECENT' or 'DURING_AUDIT_WINDOW'.",
    )
    cadence: str = Field(
        default="P1Y",
        description=(
            "Document renewal cadence (ISO-8601 duration). One of "
            "P0D/P1D/P1W/P1M/P3M/P6M/P1Y/P2Y."
        ),
    )
    reminder_window: str = Field(
        default="P1M",
        description=(
            "Notification lead time before renewal (ISO-8601 duration). One "
            "of P0D/P1D/P1W/P1M/P3M (narrower than `cadence`'s range)."
        ),
    )
    is_sensitive: bool = Field(
        default=False,
        description="Whether the document holds sensitive data requiring restricted access.",
    )

    # --- behavior ------------------------------------------------------

    auto_submit: bool = Field(
        default=True,
        description=(
            "Whether to call POST /v1/documents/{id}/submit after a "
            "successful upload. When false, uploads stay in draft status."
        ),
    )
    timeout_seconds: int = Field(default=60, description="HTTP timeout for each Vanta API call.")
    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="vanta", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'vanta').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("vanta")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "VantaEvidenceUploadComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if bool(self.file_path_column) == bool(self.file_content_column):
            raise ValueError(
                "VantaEvidenceUploadComponent: supply exactly one of "
                "`file_path_column` OR `file_content_column` (got both or neither)."
            )

        if not self.document_id_column and not (self.document_name_column or self.document_name):
            raise ValueError(
                "VantaEvidenceUploadComponent: must supply `document_id_column` "
                "(to reuse existing documents) and/or `document_name_column` / "
                "`document_name` (to create new ones)."
            )

        if self.time_sensitivity not in _TIME_SENSITIVITY_VALUES:
            raise ValueError(
                f"VantaEvidenceUploadComponent: time_sensitivity must be one of "
                f"{sorted(_TIME_SENSITIVITY_VALUES)}, got {self.time_sensitivity!r}."
            )
        if self.cadence not in _CADENCE_VALUES:
            raise ValueError(
                f"VantaEvidenceUploadComponent: cadence must be one of "
                f"{sorted(_CADENCE_VALUES)}, got {self.cadence!r}."
            )
        if self.reminder_window not in _REMINDER_WINDOW_VALUES:
            raise ValueError(
                f"VantaEvidenceUploadComponent: reminder_window must be one of "
                f"{sorted(_REMINDER_WINDOW_VALUES)}, got {self.reminder_window!r}."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"VantaEvidenceUploadComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _resolve_file(row: Dict[str, Any], fallback_title: str):
            """Returns (file_bytes, file_name, mime_type) or (None, None, None) if absent."""
            if _self.file_path_column:
                raw_path = _row_value(row, _self.file_path_column)
                if raw_path is None:
                    return None, None, None
                path = Path(str(raw_path))
                file_bytes = path.read_bytes()
                file_name = _row_value(row, _self.file_name_column) or path.name
            else:
                content = _row_value(row, _self.file_content_column)
                if content is None:
                    return None, None, None
                file_bytes = _decode_inline_content(content)
                file_name = _row_value(row, _self.file_name_column) or f"{fallback_title or 'evidence'}.bin"
            mime_override = _row_value(row, _self.mime_type_column) or _self.mime_type
            mime_type = _guess_mime_type(str(file_name), mime_override)
            return file_bytes, str(file_name), mime_type

        def _run_write(context, upstream):
            svc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={"rows_processed": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = set()
            for col in (
                _self.document_id_column,
                _self.document_name_column,
                _self.evidence_description_column,
                _self.file_path_column,
                _self.file_content_column,
                _self.file_name_column,
                _self.mime_type_column,
                _self.effective_at_date_column,
            ):
                if col:
                    required_cols.add(col)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            documents_created = 0
            uploads_succeeded = 0
            documents_submitted = 0
            skipped_no_file = 0
            skipped_no_identifier = 0
            errors: List[str] = []

            for _, row in df.iterrows():
                row_dict = row.to_dict()

                document_id = _row_value(row_dict, _self.document_id_column)
                title = _row_value(row_dict, _self.document_name_column) or _self.document_name

                if not document_id and not title:
                    skipped_no_identifier += 1
                    continue

                file_bytes, file_name, mime_type = _resolve_file(row_dict, title or str(document_id))
                if file_bytes is None:
                    skipped_no_file += 1
                    continue

                evidence_description = (
                    _row_value(row_dict, _self.evidence_description_column)
                    or _self.evidence_description
                )
                effective_at_date = _row_value(row_dict, _self.effective_at_date_column)

                label = title or document_id
                try:
                    session = svc.get_client()
                    if not document_id:
                        document_id = _create_document(
                            session,
                            svc.api_base_url,
                            str(title),
                            str(evidence_description) if evidence_description else "",
                            _self.time_sensitivity,
                            _self.cadence,
                            _self.reminder_window,
                            _self.is_sensitive,
                            _self.timeout_seconds,
                        )
                        # Counted the moment the document exists in Vanta --
                        # independent of whether the upload below succeeds,
                        # since a failed upload still leaves a real (empty)
                        # document behind, not a no-op.
                        documents_created += 1

                    _upload_file(
                        session,
                        svc.api_base_url,
                        str(document_id),
                        file_bytes,
                        file_name,
                        mime_type,
                        str(evidence_description) if evidence_description else None,
                        str(effective_at_date) if effective_at_date else None,
                        _self.timeout_seconds,
                    )
                    uploads_succeeded += 1

                    if _self.auto_submit:
                        _submit_document(session, svc.api_base_url, str(document_id), _self.timeout_seconds)
                        documents_submitted += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{label}: {type(e).__name__}: {e}")

            context.log.info(
                f"Vanta evidence upload: {uploads_succeeded} uploaded "
                f"({documents_created} new documents created), "
                f"{documents_submitted} submitted, {len(errors)} errors, "
                f"{skipped_no_file} skipped (no file), "
                f"{skipped_no_identifier} skipped (no document id/title)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "rows_processed": dg.MetadataValue.int(len(df)),
                "documents_created": dg.MetadataValue.int(documents_created),
                "uploads_succeeded": dg.MetadataValue.int(uploads_succeeded),
                "documents_submitted": dg.MetadataValue.int(documents_submitted),
                "rows_skipped_no_file": dg.MetadataValue.int(skipped_no_file),
                "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                "rows_errored": dg.MetadataValue.int(len(errors)),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])
            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                "Upload one evidence file into Vanta per upstream row "
                "(create document -> upload file -> submit)."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, upstream)

        return dg.Definitions(assets=[_asset])
