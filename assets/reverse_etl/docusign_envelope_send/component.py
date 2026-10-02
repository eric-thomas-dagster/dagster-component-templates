"""DataFrame -> DocuSign envelope send (reverse-ETL e-signature
activation).

For every upstream row: creates (and, by default, immediately sends) a
DocuSign envelope built from a template --
`POST /restapi/v2.1/accounts/{accountId}/envelopes` with `templateId` +
`templateRoles` (recipient `roleName`/`name`/`email`, optionally
prefilling the template's text tabs via `tabs_map`) -- via the
`docusign_resource` component's `DocuSignResource.create_envelope()`,
which itself wraps a real DocuSign JWT-Grant-authenticated HTTP call.

This is NOT a record upsert like `salesforce_record_upsert` -- there's no
match key, no create-or-update semantics. Every row that resolves a
recipient email creates a brand-new envelope (and, with the default
`status: sent`, immediately emails that recipient a signature request).
`email_subject_template` and `tabs_map` support simple `{column_name}`
substitution / row-value prefilling for personalization.

Pairs with:
  - ``docusign_resource`` -- JWT Grant OAuth2 auth (required). New
    resource built for this component (and any future DocuSign write-side
    components) -- no DocuSign resource existed in this repo before,
    only the read-only `docusign_ingestion` asset component.
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


def _call_docusign_api(
    resource,
    template_id: str,
    template_roles: List[Dict[str, Any]],
    email_subject: str,
    status: str,
):
    """Isolates the one real external-API boundary (DocuSignResource ->
    real JWT-Grant-authenticated HTTP call to DocuSign's eSignature REST
    API) so it can be monkeypatched wholesale in tests without the
    `pyjwt` / `cryptography` / `requests` network path being exercised."""
    return resource.create_envelope(
        template_id=template_id,
        template_roles=template_roles,
        email_subject=email_subject,
        status=status,
    )


class DocuSignEnvelopeSendComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create and send a DocuSign envelope for each row of an upstream
    DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.DocuSignEnvelopeSendComponent
        attributes:
          asset_name: docusign_contracts_sent
          upstream_asset_key: dbt_marts_contracts_ready_for_signature
          resource_key: docusign_resource
          template_id: "55A80182-xxxx-xxxx-xxxx-FD1E1C0F9D74"
          role_name: Signer1
          recipient_email_column: signer_email
          recipient_name_column: signer_name
          email_subject_template: "Please sign your {contract_type} agreement"
          tabs_map:
            contract_value: ContractValue
            effective_date: EffectiveDate
          batch_size: 200
        ```

    !! Every materialization creates a real envelope and, with the default
    `status: sent`, immediately emails the recipient a signature request --
    test with a `source: {kind: inline, rows: [...]}` against your own
    test email addresses (and/or `use_demo_env: true` on `docusign_resource`)
    first.
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
        default="docusign_resource",
        description="Resource key registered by DocuSignResourceComponent.",
    )

    template_id: str = Field(
        description="DocuSign template ID (GUID) the envelope is built from.",
    )
    role_name: str = Field(
        description=(
            "Template recipient role name to assign the resolved email/name to "
            "(must match a roleName defined on the DocuSign template, e.g. "
            "'Signer1')."
        ),
    )
    recipient_email_column: str = Field(
        description="Upstream column holding the recipient's email address.",
    )
    recipient_name_column: str = Field(
        description="Upstream column holding the recipient's full name.",
    )
    email_subject_template: str = Field(
        default="Please sign this document",
        description=(
            "Envelope email subject template. Supports `{column_name}` "
            "substitution from other upstream row columns."
        ),
    )
    status: str = Field(
        default="sent",
        description=(
            "Envelope status to create with: 'sent' emails the recipient "
            "immediately, 'created' leaves it as a draft in the sender's "
            "DocuSign account for manual review before sending."
        ),
    )
    tabs_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Upstream column -> template text-tab tabLabel. Prefills the "
            "named text tabs on the assigned role with the row's column "
            "values (e.g. {contract_value: ContractValue}). Optional -- "
            "omit to rely solely on the template's own default tab values."
        ),
    )
    batch_size: int = Field(
        default=500,
        description=(
            "Max upstream rows per run (safety cap). Each row creates one "
            "real envelope -- this is NOT a throughput/rate-limit control, "
            "it's a blast-radius cap."
        ),
    )

    group_name: Optional[str] = Field(default="docusign", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'docusign')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("docusign")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "DocuSignEnvelopeSendComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.template_id:
            raise ValueError("DocuSignEnvelopeSendComponent: template_id must be non-empty.")
        if not self.role_name:
            raise ValueError("DocuSignEnvelopeSendComponent: role_name must be non-empty.")
        if self.status not in ("sent", "created"):
            raise ValueError(
                f"DocuSignEnvelopeSendComponent: status must be 'sent' or "
                f"'created' (got {self.status!r})."
            )

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
            raise ValueError(f"DocuSignEnvelopeSendComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_send(context, upstream):
            resource = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to send.")
                return dg.MaterializeResult(
                    metadata={
                        "envelopes_sent": dg.MetadataValue.int(0),
                        "envelopes_failed": dg.MetadataValue.int(0),
                    }
                )

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            missing_cols = [
                c for c in (_self.recipient_email_column, _self.recipient_name_column)
                if c not in df.columns
            ]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )
            if _self.tabs_map:
                missing_tab_cols = [c for c in _self.tabs_map if c not in df.columns]
                if missing_tab_cols:
                    raise dg.Failure(
                        f"tabs_map columns not in upstream: {missing_tab_cols}. "
                        f"Available: {list(df.columns)}"
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

            sent = 0
            failed = 0
            skipped_no_recipient = 0
            errors: List[str] = []
            envelope_ids: List[str] = []

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                email = row_dict.get(_self.recipient_email_column)
                name = row_dict.get(_self.recipient_name_column)
                if _is_blank(email) or _is_blank(name):
                    skipped_no_recipient += 1
                    continue

                role: Dict[str, Any] = {
                    "roleName": _self.role_name,
                    "name": str(name).strip(),
                    "email": str(email).strip(),
                }
                if _self.tabs_map:
                    text_tabs = [
                        {"tabLabel": label, "value": "" if _is_blank(row_dict.get(col)) else str(row_dict.get(col))}
                        for col, label in _self.tabs_map.items()
                    ]
                    role["tabs"] = {"textTabs": text_tabs}

                subject = _render_template(_self.email_subject_template, row_dict)
                try:
                    result = _call_docusign_api(
                        resource,
                        template_id=_self.template_id,
                        template_roles=[role],
                        email_subject=subject,
                        status=_self.status,
                    )
                    sent += 1
                    envelope_id = (result or {}).get("envelopeId")
                    if envelope_id:
                        envelope_ids.append(envelope_id)
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    errors.append(f"row {i} (email={email}): {type(e).__name__}: {e}")

            context.log.info(
                f"DocuSign envelope send: sent={sent} failed={failed} "
                f"skipped_no_recipient={skipped_no_recipient} status={_self.status}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "template_id": dg.MetadataValue.text(_self.template_id),
                "envelope_status": dg.MetadataValue.text(_self.status),
                "envelopes_sent": dg.MetadataValue.int(sent),
                "envelopes_failed": dg.MetadataValue.int(failed),
                "rows_skipped_no_recipient": dg.MetadataValue.int(skipped_no_recipient),
            }
            if envelope_ids:
                metadata["first_envelope_ids"] = dg.MetadataValue.json(envelope_ids[:5])
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
                f"Create and send a DocuSign envelope from template "
                f"{_self.template_id!r} for each upstream row."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_send(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_send(context, upstream)

        return dg.Definitions(assets=[_asset])
