"""DataFrame -> Close CRM Lead upsert (emulated: search-then-write).

Mirrors an upstream DataFrame into Close CRM Leads. Close has **no native
atomic upsert endpoint** (unlike Salesforce's External-ID upsert or
HubSpot's batch/upsert) -- this sink emulates one via the resource's
``upsert_lead``, which does:

  1. ``POST /api/v1/data/search/`` (Close's "Advanced Filtering" API) to
     look up an existing Lead by `dedupe_field`.
  2. ``PUT /api/v1/lead/{id}/`` if found, else ``POST /api/v1/lead/``.

**This is NOT race-condition-safe.** Two concurrent writers upserting the
same dedupe value can both miss each other's in-flight create and produce
duplicate Leads -- Close's REST API has no idempotency key or
conditional-write mechanism to close this window. Do not run concurrent
materializations of this asset (or multiple pipelines) against overlapping
dedupe values without an external lock. See ``close_crm_resource``'s
README for the full discussion, and prefer a genuinely-unique dedupe field
(a contact email, or a custom field holding a warehouse-side external id)
over `name`, which is not guaranteed unique in Close and will silently
create duplicates even without concurrency (the search only inspects the
*first* match).

`fields_map` uses a flat Close-field vocabulary (not Close's native nested
`contacts[].emails[]` JSON shape) so upstream columns map directly to
one of:
  - `"name"`          -> Lead's own `name` field.
  - `"contact_name"`  -> primary contact's `name`.
  - `"email"`         -> primary contact's first email (`type: office`).
  - `"phone"`         -> primary contact's first phone (`type: office`).
  - `"custom.cf_xxx"` -> a Lead custom field (flat top-level key on the
    Close Lead body, verified against
    developer.close.com/api/resources/leads/create).

Pairs with:
  - ``close_crm_resource`` -- HTTP Basic (API-key) auth + find/create/update
    + the ``upsert_lead`` orchestration this sink calls (required).
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_KNOWN_FLAT_FIELDS = {"name", "contact_name", "email", "phone"}


class CloseCrmLeadUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Close CRM Leads
    (emulated search-then-write upsert -- Close has no native upsert).

    Example:
        ```yaml
        type: dagster_component_templates.CloseCrmLeadUpsertComponent
        attributes:
          asset_name: close_crm_leads_mirror
          upstream_asset_key: dbt_marts_leads
          resource_key: close_crm
          dedupe_field: email
          fields_map:
            company_name: name
            contact_full_name: contact_name
            email: email
            phone: phone
            warehouse_id: custom.cf_FSYEbxYJFsnY9tN1OTAPIF33j7Sw5Lb7Eawll7JzoNh
          batch_size: 5000
        ```

    For every upstream row:
      - Builds a Close Lead body from `fields_map` (name / contact_name /
        email / phone / custom.cf_xxx).
      - Skips rows missing the `dedupe_field` value (counted in
        `rows_skipped_no_key`).
      - Calls `resource.upsert_lead(dedupe_field, dedupe_value, body)` --
        search-then-write, NOT atomic. See module docstring.
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
        default="close_crm",
        description="Resource key registered by CloseCrmResourceComponent.",
    )

    dedupe_field: str = Field(
        description=(
            "Close field used to search for an existing Lead before writing "
            "(there is no native upsert -- this is emulated via "
            "search-then-write and is therefore NOT race-condition-safe "
            "under concurrent writers; see module docstring / README). MUST "
            "be present in fields_map values. One of 'email' (matches a "
            "contact's email -- recommended), 'phone' (matches a contact's "
            "phone), 'name' (matches the Lead name -- not guaranteed unique), "
            "or 'custom.cf_xxx' (matches a Lead custom field by id -- the "
            "safest option for a warehouse-side external id)."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Close field. Values must be one of 'name', "
            "'contact_name', 'email', 'phone', or 'custom.cf_xxx' (a Lead "
            "custom field id)."
        ),
    )
    batch_size: int = Field(
        default=5000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(
        default="close_crm", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'close').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("close")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "CloseCrmLeadUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate every fields_map value is a recognized Close field shape.
        for col, close_field in self.fields_map.items():
            if close_field not in _KNOWN_FLAT_FIELDS and not close_field.startswith("custom."):
                raise ValueError(
                    f"CloseCrmLeadUpsertComponent: fields_map[{col!r}]={close_field!r} "
                    f"is not a recognized Close field. Must be one of "
                    f"{sorted(_KNOWN_FLAT_FIELDS)} or 'custom.cf_xxx'."
                )

        # Validate dedupe_field is in fields_map values AND is a supported
        # dedupe shape (the resource only knows how to search on these).
        mapped_fields = set(self.fields_map.values())
        if self.dedupe_field not in mapped_fields:
            raise ValueError(
                f"CloseCrmLeadUpsertComponent: dedupe_field={self.dedupe_field!r} "
                f"not in fields_map values. dedupe_field must be a Close field "
                f"you're upserting. fields_map values: {sorted(mapped_fields)}"
            )
        if self.dedupe_field not in ("email", "phone", "name") and not self.dedupe_field.startswith("custom."):
            raise ValueError(
                f"CloseCrmLeadUpsertComponent: dedupe_field={self.dedupe_field!r} "
                f"is not searchable. Must be 'email', 'phone', 'name', or "
                f"'custom.cf_xxx' (dedupe_field cannot be 'contact_name' -- "
                f"Close has no search field for a contact's display name)."
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
            raise ValueError(f"CloseCrmLeadUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _build_lead_body(row, fields_map) -> Dict[str, Any]:
            """Translate one upstream row into a Close Lead body shape:
            {'name', 'contacts': [{'name', 'emails': [...], 'phones': [...]}],
            'custom.cf_xxx': ...}."""
            name_val = None
            contact_name_val = None
            email_val = None
            phone_val = None
            custom_fields: Dict[str, Any] = {}
            for col, close_field in fields_map.items():
                v = row.get(col)
                if v is None:
                    continue
                if close_field == "name":
                    name_val = v
                elif close_field == "contact_name":
                    contact_name_val = v
                elif close_field == "email":
                    email_val = v
                elif close_field == "phone":
                    phone_val = v
                elif close_field.startswith("custom."):
                    custom_fields[close_field] = v

            body: Dict[str, Any] = {}
            if name_val is not None:
                body["name"] = name_val
            contact: Dict[str, Any] = {}
            if contact_name_val is not None:
                contact["name"] = contact_name_val
            if email_val is not None:
                contact["emails"] = [{"email": email_val, "type": "office"}]
            if phone_val is not None:
                contact["phones"] = [{"phone": phone_val, "type": "office"}]
            if contact:
                body["contacts"] = [contact]
            body.update(custom_fields)
            return body, {"name": name_val, "contact_name": contact_name_val, "email": email_val, "phone": phone_val, **custom_fields}

        def _dedupe_value(field_values: Dict[str, Any]):
            return field_values.get(_self.dedupe_field)

        def _run_upsert(context, upstream):
            close = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            created = 0
            updated = 0
            skipped_no_key = 0
            errors: List[str] = []

            for i, (_, row) in enumerate(df.iterrows()):
                row_dict = {col: _row_value(row[col]) for col in _self.fields_map}
                body, field_values = _build_lead_body(row_dict, _self.fields_map)
                dedupe_value = _dedupe_value(field_values)
                if dedupe_value is None:
                    skipped_no_key += 1
                    continue
                if not body:
                    skipped_no_key += 1
                    continue
                try:
                    result = close.upsert_lead(_self.dedupe_field, dedupe_value, body)
                    if result.get("action") == "created":
                        created += 1
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row {i} ({_self.dedupe_field}={dedupe_value!r}): "
                        f"{type(e).__name__}: {e}"
                    )

            context.log.info(
                f"Close CRM Lead upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing "
                f"{_self.dedupe_field}) — matched on {_self.dedupe_field}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "close_lead_dedupe_field": dg.MetadataValue.text(_self.dedupe_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
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
                f"Upsert DataFrame rows into Close CRM Leads "
                f"(emulated search-then-write, match on {_self.dedupe_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
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
