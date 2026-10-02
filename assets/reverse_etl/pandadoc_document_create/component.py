"""DataFrame -> PandaDoc document create + send (reverse-ETL e-signature
activation).

For every upstream row: creates a PandaDoc document from a template --
`POST /documents` with `template_uuid` + `recipients` (email/first_name/
last_name/role) and, optionally, `tokens` prefilled from other row
columns -- then (when `auto_send: true`, the default) polls the document
until it reaches PandaDoc's sendable `document.draft` status and calls
`POST /documents/{id}/send`. All three calls go through the
`pandadoc_resource` component's `PandaDocResource`, which itself wraps
real HTTP calls to PandaDoc's REST API.

This is NOT a record upsert like `salesforce_record_upsert` -- there's no
match key, no create-or-update semantics. Every row that resolves a
recipient email creates a brand-new document (and, with `auto_send:
true`, immediately emails that recipient a signature request).
`name_template` and `tokens_map` support simple `{column_name}`
substitution / row-value prefilling for personalization.

Pairs with:
  - ``pandadoc_resource`` -- static API Key auth (required). New resource
    built for this component -- no PandaDoc resource existed in this repo
    before, only the read-only `pandadoc_ingestion` asset component.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class _SafeFormatDict(dict):
    """format_map() helper -- a missing `{column}` in a template renders
    as empty string rather than raising KeyError."""

    def __missing__(self, key):  # noqa: D105
        return ""


def _render_template(template: str, row: Dict[str, Any]) -> str:
    safe_row = {k: ("" if v is None else v) for k, v in row.items()}
    return template.format_map(_SafeFormatDict(safe_row))


def _call_pandadoc_api(
    resource,
    name: str,
    template_uuid: str,
    recipients: List[Dict[str, Any]],
    tokens: Optional[List[Dict[str, str]]],
    auto_send: bool,
    send_message: Optional[str],
    poll_interval_seconds: float,
    poll_timeout_seconds: float,
) -> dict:
    """Isolates the real external-API boundary (PandaDocResource -> real
    HTTP calls to PandaDoc's REST API: create, poll status, send) so it
    can be monkeypatched wholesale in tests without the `requests`
    network path being exercised. Returns the create_document response,
    merged with the send response's fields when auto_send is True."""
    created = resource.create_document(
        name=name,
        template_uuid=template_uuid,
        recipients=recipients,
        tokens=tokens,
    )
    if not auto_send:
        return created

    document_id = created["id"]
    resource.wait_until_draft(
        document_id,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=poll_timeout_seconds,
    )
    send_result = resource.send_document(document_id, message=send_message)
    return {**created, "send_result": send_result}


class PandaDocDocumentCreateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create (and, by default, send) a PandaDoc document from a template
    for each row of an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.PandaDocDocumentCreateComponent
        attributes:
          asset_name: pandadoc_contracts_sent
          upstream_asset_key: dbt_marts_contracts_ready_for_signature
          resource_key: pandadoc_resource
          template_uuid: "AbCdEfGhIjKlMnOpQrStUv"
          name_template: "{contract_type} Agreement - {customer_name}"
          recipient_email_column: signer_email
          recipient_first_name_column: signer_first_name
          recipient_last_name_column: signer_last_name
          recipient_role: Client
          tokens_map:
            contract_value: contract.value
            effective_date: contract.effective_date
          batch_size: 200
        ```

    !! Every materialization creates a real document and, with the default
    `auto_send: true`, immediately emails the recipient a signature
    request -- test with a `source: {kind: inline, rows: [...]}` against
    your own test email addresses first.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
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
        default="pandadoc_resource",
        description="Resource key registered by PandaDocResourceComponent.",
    )

    template_uuid: str = Field(
        description="PandaDoc template UUID the document is built from.",
    )
    name_template: str = Field(
        default="Document for {recipient_email_column}",
        description=(
            "Document name template. Supports `{column_name}` substitution "
            "from other upstream row columns."
        ),
    )
    recipient_email_column: str = Field(
        description="Upstream column holding the recipient's email address.",
    )
    recipient_first_name_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding the recipient's first name.",
    )
    recipient_last_name_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding the recipient's last name.",
    )
    recipient_role: Optional[str] = Field(
        default=None,
        description=(
            "Template recipient role name to assign this recipient to (must "
            "match a role defined on the PandaDoc template). Required when "
            "the template defines named roles."
        ),
    )
    tokens_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Upstream column -> PandaDoc template token name. Prefills the "
            "named tokens with the row's column values (e.g. "
            "{contract_value: 'contract.value'}). Optional -- omit to rely "
            "solely on the template's own default token values."
        ),
    )
    auto_send: bool = Field(
        default=True,
        description=(
            "If true (default): poll the created document until PandaDoc "
            "marks it sendable (status 'document.draft'), then send it "
            "immediately. If false: only create the document (status stays "
            "as PandaDoc leaves it, e.g. for manual review/send in the "
            "PandaDoc UI)."
        ),
    )
    send_message: Optional[str] = Field(
        default=None,
        description="Optional message included in the send-notification email (auto_send mode only).",
    )
    poll_interval_seconds: float = Field(
        default=2.0,
        description="Seconds between document-status polls while waiting for 'document.draft' (auto_send mode only).",
    )
    poll_timeout_seconds: float = Field(
        default=120.0,
        description="Max seconds to wait for the document to reach 'document.draft' before failing that row (auto_send mode only).",
    )
    batch_size: int = Field(
        default=200,
        description=(
            "Max upstream rows per run (safety cap). Each row creates one "
            "real document -- this is NOT a throughput/rate-limit control, "
            "it's a blast-radius cap."
        ),
    )

    group_name: Optional[str] = Field(default="pandadoc", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'pandadoc')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("pandadoc")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "PandaDocDocumentCreateComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.template_uuid:
            raise ValueError("PandaDocDocumentCreateComponent: template_uuid must be non-empty.")
        if not self.recipient_email_column:
            raise ValueError("PandaDocDocumentCreateComponent: recipient_email_column must be non-empty.")

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # -- Source resolver (self-contained per no-shared-code rule) -----
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
            raise ValueError(f"PandaDocDocumentCreateComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_create(context, upstream):
            resource = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to create.")
                return dg.MaterializeResult(
                    metadata={
                        "documents_created": dg.MetadataValue.int(0),
                        "documents_failed": dg.MetadataValue.int(0),
                    }
                )

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = [_self.recipient_email_column]
            for col in (_self.recipient_first_name_column, _self.recipient_last_name_column):
                if col:
                    required_cols.append(col)
            if _self.tokens_map:
                required_cols.extend(_self.tokens_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            def _is_blank(v) -> bool:
                if v is None:
                    return True
                try:
                    if isinstance(v, float) and pd.isna(v):
                        return True
                except Exception:  # noqa: BLE001
                    pass
                return str(v).strip() == ""

            created_count = 0
            failed = 0
            skipped_no_recipient = 0
            errors: List[str] = []
            document_ids: List[str] = []

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                email = row_dict.get(_self.recipient_email_column)
                if _is_blank(email):
                    skipped_no_recipient += 1
                    continue

                recipient: Dict[str, Any] = {"email": str(email).strip()}
                if _self.recipient_first_name_column:
                    first_name = row_dict.get(_self.recipient_first_name_column)
                    if not _is_blank(first_name):
                        recipient["first_name"] = str(first_name)
                if _self.recipient_last_name_column:
                    last_name = row_dict.get(_self.recipient_last_name_column)
                    if not _is_blank(last_name):
                        recipient["last_name"] = str(last_name)
                if _self.recipient_role:
                    recipient["role"] = _self.recipient_role

                tokens = None
                if _self.tokens_map:
                    tokens = [
                        {"name": token_name, "value": "" if _is_blank(row_dict.get(col)) else str(row_dict.get(col))}
                        for col, token_name in _self.tokens_map.items()
                    ]

                doc_name = _render_template(_self.name_template, row_dict)
                try:
                    result = _call_pandadoc_api(
                        resource,
                        name=doc_name,
                        template_uuid=_self.template_uuid,
                        recipients=[recipient],
                        tokens=tokens,
                        auto_send=_self.auto_send,
                        send_message=_self.send_message,
                        poll_interval_seconds=_self.poll_interval_seconds,
                        poll_timeout_seconds=_self.poll_timeout_seconds,
                    )
                    created_count += 1
                    doc_id = (result or {}).get("id")
                    if doc_id:
                        document_ids.append(doc_id)
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    errors.append(f"row {i} (email={email}): {type(e).__name__}: {e}")

            context.log.info(
                f"PandaDoc document create: created={created_count} failed={failed} "
                f"skipped_no_recipient={skipped_no_recipient} auto_send={_self.auto_send}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "template_uuid": dg.MetadataValue.text(_self.template_uuid),
                "auto_send": dg.MetadataValue.bool(_self.auto_send),
                "documents_created": dg.MetadataValue.int(created_count),
                "documents_failed": dg.MetadataValue.int(failed),
                "rows_skipped_no_recipient": dg.MetadataValue.int(skipped_no_recipient),
            }
            if document_ids:
                metadata["first_document_ids"] = dg.MetadataValue.json(document_ids[:5])
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
                f"Create and send a PandaDoc document from template "
                f"{_self.template_uuid!r} for each upstream row."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_create(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_create(context, upstream)

        return dg.Definitions(assets=[_asset])
