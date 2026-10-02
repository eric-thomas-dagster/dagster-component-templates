"""DataFrame -> Help Scout conversation upsert (update-if-key-present-else-create).

Help Scout conversations don't support a clean external-key search the way
ServiceNow/Freshdesk/Freshservice tickets do, so this sink uses an explicit
`conversation_id_column` as the match key (same shape as
`greenhouse_candidate_update`'s update-only pattern elsewhere in this repo)
-- but unlike Greenhouse, Help Scout DOES support creating brand-new
conversations, so this is a genuine create-or-update, not update-only:
  - row's `conversation_id_column` value is present -> UPDATE the existing
    conversation (tags/note/status).
  - value is absent -> CREATE a new conversation.

Two source shapes:
  1. `upstream_asset_key:` -- chain from an upstream Dagster asset that produces
     a pandas DataFrame. Standard Dagster lineage pattern.
  2. `source:` block -- read the DataFrame inline at run time, no upstream asset
     required. Supports:
       - kind: sql -- query a database via a Dagster resource (`resource_key`
         with `.get_engine()` / `.get_connection()`) OR a raw
         `database_url_env_var`.
       - kind: csv -- read a CSV file at `path`.
       - kind: inline -- literal rows in YAML.

Pairs with:
  - ``help_scout_resource`` -- OAuth2 client_credentials auth (required)

Real Help Scout API facts this sink relies on:
  - Creating a conversation by `customer_email` natively auto-creates (or
    reuses) the underlying Customer record by that email -- this IS Help
    Scout's documented customer-upsert-by-email mechanism. There is no
    separate "customer upsert" step in this component.
  - `tags_column` on UPDATE triggers a FULL REPLACE of the conversation's tag
    list via `update_tags` -- not additive like Lever's `addTags`. Any
    existing tag you don't include is removed.
  - A row with `conversation_id_column` set but none of
    `tags_column`/`note_column`/`status_column` configured is a true no-op
    (nothing to update) -- counted separately as `rows_skipped_noop` rather
    than silently doing nothing uncounted.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class HelpScoutConversationUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create-or-update rows from an upstream DataFrame into Help Scout conversations.

    Example:
        ```yaml
        type: dagster_component_templates.HelpScoutConversationUpsertComponent
        attributes:
          asset_name: help_scout_conversations_mirror
          upstream_asset_key: warehouse_support_events
          resource_key: help_scout_resource
          mailbox_id: 85
          conversation_id_column: conversation_id
          customer_email_column: email
          subject_column: subject
          body_column: body
          tags_column: tags
          note_column: internal_note
          status_column: status
          group_name: reverse_etl
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
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
        default="help_scout_resource",
        description="Resource key registered by HelpScoutResourceComponent.",
    )

    mailbox_id: int = Field(description="Target Help Scout mailbox ID for new conversations.")

    conversation_id_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an existing Help Scout conversation ID. If the row's "
            "value is present/non-null, this row UPDATES that conversation (tags/note/status); "
            "if absent (or this field is unset entirely), the row CREATES a new conversation."
        ),
    )
    customer_email_column: str = Field(
        description=(
            "Upstream column holding the customer's email. Used on CREATE as the customer "
            "identifier -- Help Scout auto-upserts the underlying Customer record by this email."
        ),
    )
    customer_first_name_column: Optional[str] = Field(
        default=None, description="Upstream column holding the customer's first name. CREATE only."
    )
    customer_last_name_column: Optional[str] = Field(
        default=None, description="Upstream column holding the customer's last name. CREATE only."
    )
    subject_column: Optional[str] = Field(
        default=None, description="Upstream column holding the conversation subject. Required for CREATE."
    )
    body_column: Optional[str] = Field(
        default=None, description="Upstream column holding the initial thread text. Required for CREATE."
    )
    tags_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding tags (comma-separated string or list). On CREATE: included "
            "in the create body. On UPDATE: triggers update_tags -- a FULL REPLACE of the "
            "conversation's tag list, not additive."
        ),
    )
    note_column: Optional[str] = Field(
        default=None, description="Upstream column holding note text. UPDATE only: posts via add_note."
    )
    status_column: Optional[str] = Field(
        default=None, description="Upstream column holding a status value. UPDATE only: calls patch_status."
    )
    conversation_type: str = Field(default="email", description="Help Scout conversation type for new conversations.")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="help_scout", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'helpscout').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("helpscout")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "HelpScoutConversationUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if bool(self.subject_column) != bool(self.body_column):
            raise ValueError(
                "HelpScoutConversationUpsertComponent: `subject_column` and `body_column` must "
                "both be set or both be unset -- every CREATE needs both, and any row could need "
                "to create (when conversation_id_column is unset or the row's value is absent)."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
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
            raise ValueError(f"HelpScoutConversationUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _row_value(v):
            import pandas as pd
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            return v

        def _parse_tags(v) -> List[str]:
            if v is None:
                return []
            if isinstance(v, (list, tuple, set)):
                return [str(t).strip() for t in v if str(t).strip()]
            return [t.strip() for t in str(v).split(",") if t.strip()]

        def _run_write(exec_ctx, upstream):
            svc = getattr(exec_ctx.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            empty_metadata = {
                "rows_created": dg.MetadataValue.int(0),
                "rows_updated": dg.MetadataValue.int(0),
                "rows_errored": dg.MetadataValue.int(0),
                "rows_skipped_no_key": dg.MetadataValue.int(0),
                "rows_skipped_noop": dg.MetadataValue.int(0),
            }
            if len(df) == 0:
                exec_ctx.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata=empty_metadata)

            if len(df) > _self.max_rows:
                exec_ctx.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = {_self.customer_email_column}
            for optional_col in (
                _self.conversation_id_column,
                _self.customer_first_name_column,
                _self.customer_last_name_column,
                _self.subject_column,
                _self.body_column,
                _self.tags_column,
                _self.note_column,
                _self.status_column,
            ):
                if optional_col:
                    required_cols.add(optional_col)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            rows_created = 0
            rows_updated = 0
            rows_skipped_no_key = 0
            rows_skipped_noop = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                conv_id_value = (
                    _row_value(row[_self.conversation_id_column]) if _self.conversation_id_column else None
                )

                if conv_id_value is not None:
                    # ── UPDATE path ──────────────────────────────────────
                    tags_value = _row_value(row[_self.tags_column]) if _self.tags_column else None
                    note_value = _row_value(row[_self.note_column]) if _self.note_column else None
                    status_value = _row_value(row[_self.status_column]) if _self.status_column else None

                    if tags_value is None and note_value is None and status_value is None:
                        exec_ctx.log.warning(
                            f"Row {i}: conversation_id={conv_id_value} present but none of "
                            "tags/note/status is configured/present for this row -- no-op."
                        )
                        rows_skipped_noop += 1
                        continue

                    try:
                        did_action = False
                        if tags_value is not None:
                            svc.update_tags(int(conv_id_value), _parse_tags(tags_value))
                            did_action = True
                        if note_value is not None:
                            svc.add_note(int(conv_id_value), str(note_value))
                            did_action = True
                        if status_value is not None:
                            svc.patch_status(int(conv_id_value), str(status_value))
                            did_action = True
                        if did_action:
                            rows_updated += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"row {i} (conversation_id={conv_id_value}): {type(e).__name__}: {e}")
                    continue

                # ── CREATE path ──────────────────────────────────────────
                email = _row_value(row[_self.customer_email_column])
                if email is None:
                    rows_skipped_no_key += 1
                    continue

                subject_value = _row_value(row[_self.subject_column]) if _self.subject_column else None
                body_value = _row_value(row[_self.body_column]) if _self.body_column else None
                if subject_value is None or body_value is None:
                    errors.append(
                        f"row {i} (email={email}): no conversation_id and missing subject/body for CREATE"
                    )
                    continue

                first_name = (
                    _row_value(row[_self.customer_first_name_column]) if _self.customer_first_name_column else None
                )
                last_name = (
                    _row_value(row[_self.customer_last_name_column]) if _self.customer_last_name_column else None
                )
                tags_value = _row_value(row[_self.tags_column]) if _self.tags_column else None

                try:
                    svc.create_conversation(
                        mailbox_id=_self.mailbox_id,
                        customer_email=str(email),
                        subject=str(subject_value),
                        body_text=str(body_value),
                        type_=_self.conversation_type,
                        tags=_parse_tags(tags_value) if tags_value is not None else None,
                        customer_first_name=str(first_name) if first_name is not None else None,
                        customer_last_name=str(last_name) if last_name is not None else None,
                    )
                    rows_created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (email={email}): {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"Help Scout conversation upsert: {rows_created} created, {rows_updated} updated, "
                f"{len(errors)} errors, {rows_skipped_no_key} skipped (missing email), "
                f"{rows_skipped_noop} skipped (no-op update)."
            )
            if errors:
                exec_ctx.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "rows_created": dg.MetadataValue.int(rows_created),
                "rows_updated": dg.MetadataValue.int(rows_updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(rows_skipped_no_key),
                "rows_skipped_noop": dg.MetadataValue.int(rows_skipped_noop),
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
                "Create-or-update DataFrame rows into Help Scout conversations (match on an "
                "optional conversation_id column; create auto-upserts the Customer by email)."
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
