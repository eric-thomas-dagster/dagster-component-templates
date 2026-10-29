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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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
        type: dagster_community_components.VantaEvidenceUploadComponent
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

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'vanta').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
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
            partitions_def=partitions_def,
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
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

        return dg.Definitions(assets=[_asset])


# BEGIN reverse-etl partition helpers (inlined per component; keep copies identical, see tests/test_partition_port_reverse_etl.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run or a non-DataFrame input.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the input is returned unchanged."""
    import pandas as pd

    if not hasattr(df, "columns") or not (context.has_partition_key or context.has_partition_key_range):
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
# END reverse-etl partition helpers
