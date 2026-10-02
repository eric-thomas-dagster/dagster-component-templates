"""DataFrame -> Freshservice ticket upsert (search-then-write).

Freshservice has no native upsert endpoint (same situation as ServiceNow /
Freshdesk) -- this sink searches `/tickets/filter` by a `key_field`, then
PUTs the match or POSTs a new ticket.

Freshservice is a DIFFERENT Freshworks product from Freshdesk: it deals in
tickets (not contacts), has no `unique_external_id` contact-filter
shortcut, and its custom fields live in a nested `custom_fields` object on
both create/update payloads -- but the FILTER query uses the bare field
name (no `custom_fields.` prefix, no `cf_` prefix).

Two source shapes:
  1. `upstream_asset_key:` -- chain from an upstream Dagster asset that
     produces a pandas DataFrame.
  2. `source:` block -- read the DataFrame inline at run time, no upstream
     asset required. Supports kind: sql / csv / inline.

`fields_map` prefix convention:
  - Values prefixed `"custom_fields."` (e.g. `"custom_fields.unique_external_id"`)
    go into the nested `custom_fields` sub-object on create/update.
  - Unprefixed values (e.g. `"subject"`, `"description"`, `"email"`,
    `"priority"`, `"status"`) are top-level ticket fields.

`key_field` convention:
  - `key_field` must appear in `fields_map` values (validated in
    `build_defs`), same as ServiceNow's key_field check.
  - If `key_field` itself carries the `custom_fields.` prefix, it is
    stripped before being used in the Filter Tickets API query, since
    Freshservice's filter syntax addresses custom fields by their bare
    name, not a nested path.

Pairs with:
  - ``freshservice_resource`` -- HTTP Basic auth (api_key, "X") (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class FreshserviceTicketUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from a source DataFrame into Freshservice tickets.

    Example -- upstream asset:
        ```yaml
        type: dagster_component_templates.FreshserviceTicketUpsertComponent
        attributes:
          asset_name: freshservice_tickets_from_alerts
          upstream_asset_key: alerts_to_tickets
          resource_key: freshservice_resource
          key_field: custom_fields.unique_external_id
          fields_map:
            alert_id: custom_fields.unique_external_id
            title: subject
            details: description
            requester_email: email
        ```

    Example -- inline SQL source (no upstream asset):
        ```yaml
        attributes:
          asset_name: freshservice_tickets_from_alerts
          source:
            kind: sql
            resource_key: analytics_postgres
            query: |
              SELECT alert_id, title, details, requester_email
              FROM analytics.open_alerts
              WHERE created_at > now() - interval '1 day'
          resource_key: freshservice_resource
          key_field: custom_fields.unique_external_id
          fields_map: {...}
        ```

    For every row in the source DataFrame:
      1. `GET /api/v2/tickets/filter` by the bare `key_field` (stripped of
         any `custom_fields.` prefix) == row's key value.
      2. If found, `PUT /api/v2/tickets/{id}` with the mapped fields.
      3. If not, `POST /api/v2/tickets` with the mapped fields.
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
            "Shapes:\n"
            "  {kind: sql, resource_key: <name>, query: <sql>}\n"
            "  {kind: sql, database_url_env_var: <env>, query: <sql>}\n"
            "  {kind: csv, path: <path>, read_csv_kwargs: {...}}\n"
            "  {kind: inline, rows: [{...}, ...]}"
        ),
    )

    resource_key: str = Field(
        default="freshservice_resource",
        description="Resource key registered by FreshserviceResourceComponent.",
    )

    key_field: str = Field(
        description=(
            "Freshservice field (or custom field, with a 'custom_fields.' "
            "prefix) used to match existing tickets via the Filter Tickets "
            "API. Must be present in `fields_map` values."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Source column -> Freshservice field. Values prefixed "
            "'custom_fields.' (e.g. 'custom_fields.unique_external_id') go "
            "into the nested custom_fields sub-object on create/update; "
            "unprefixed values (e.g. 'subject', 'description', 'email', "
            "'priority', 'status') are top-level ticket fields."
        ),
    )
    batch_size: int = Field(
        default=500,
        description="Max rows per run (safety cap). Each row is one search-then-write pair (2 REST calls when the ticket exists, 1 when new).",
    )

    group_name: Optional[str] = Field(
        default="freshservice", description="Dagster asset group name."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'freshservice').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("freshservice")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "FreshserviceTicketUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate key_field is in fields_map values -- otherwise the upsert
        # can't match and every row will be created as new.
        mapped_fields = set(self.fields_map.values())
        if self.key_field not in mapped_fields:
            raise ValueError(
                f"FreshserviceTicketUpsertComponent: key_field={self.key_field!r} not "
                f"present in fields_map values. key_field must be a Freshservice "
                f"field you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None

        # Extra required_resource_keys when source: kind=sql uses a resource.
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            source_rk = self.source.get("resource_key")
            if source_rk:
                extra_rks.add(source_rk)

        _CUSTOM_PREFIX = "custom_fields."

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            """Resolve DataFrame from `source:` config (called when use_source=True)."""
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
                        engine = resource.get_engine()
                        return pd.read_sql(query, engine)
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            # DuckDB fast path
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(
                        f"source kind=sql: resource {rk!r} must expose "
                        ".get_engine() or .get_connection()"
                    )
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError(
                    "source kind=sql requires 'resource_key' OR 'database_url_env_var'"
                )

            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))

            if kind == "inline":
                rows = src.get("rows") or []
                return pd.DataFrame(rows)

            raise ValueError(
                f"FreshserviceTicketUpsertComponent source kind={kind!r} not supported "
                "(expected: sql / csv / inline)"
            )

        # ── Shared upsert body ─────────────────────────────────────────
        def _run_upsert(exec_ctx, df):
            fs = getattr(exec_ctx.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame([df]) if isinstance(df, dict) else pd.DataFrame(df)

            if len(df) == 0:
                exec_ctx.log.warning("Source DataFrame is empty -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                exec_ctx.log.warning(
                    f"Source has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in source: {missing_cols}. Available: {list(df.columns)}"
                )

            key_col = next(
                (col for col, fs_field in _self.fields_map.items() if fs_field == _self.key_field),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to key_field={_self.key_field!r}. "
                    f"fields_map: {_self.fields_map}"
                )

            # Bare field name used in the Filter Tickets API query -- strip
            # the custom_fields. prefix since the filter syntax addresses
            # custom fields by their raw name, not the nested payload path.
            bare_key_field = (
                _self.key_field[len(_CUSTOM_PREFIX):]
                if _self.key_field.startswith(_CUSTOM_PREFIX)
                else _self.key_field
            )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            def _split_body(row) -> dict:
                """Build the ticket payload, splitting fields_map into
                top-level fields vs the nested custom_fields sub-object."""
                body: dict = {}
                custom: dict = {}
                for col, fs_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is None:
                        continue
                    if fs_field.startswith(_CUSTOM_PREFIX):
                        custom[fs_field[len(_CUSTOM_PREFIX):]] = v
                    else:
                        body[fs_field] = v
                if custom:
                    body["custom_fields"] = custom
                return body

            just_created_ids: Dict[str, Any] = {}
            created = 0
            updated = 0
            skipped_no_key = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
                    skipped_no_key += 1
                    continue

                body = _split_body(row)
                if not body:
                    continue

                cached_id = just_created_ids.get(str(key_value))
                try:
                    if cached_id is not None:
                        fs.update_ticket(cached_id, body)
                        updated += 1
                        continue
                    matches = fs.filter_tickets(f"{bare_key_field}:'{key_value}'")
                    if matches:
                        ticket_id = matches[0]["id"]
                        fs.update_ticket(ticket_id, body)
                        updated += 1
                    else:
                        new_ticket = fs.create_ticket(body)
                        created += 1
                        if new_ticket.get("id") is not None:
                            just_created_ids[str(key_value)] = new_ticket["id"]
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (key={key_value}): {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"Freshservice ticket upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.key_field})."
            )
            if errors:
                exec_ctx.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "key_field": dg.MetadataValue.text(_self.key_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        # ── Two asset shapes based on source configuration ─────────────
        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Freshservice tickets "
                f"(match on {_self.key_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(
                required_resource_keys=required_rks,
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
