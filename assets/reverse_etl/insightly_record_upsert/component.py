"""DataFrame → Insightly CRM record upsert (emulated — search-then-write).

Mirrors an upstream DataFrame into Insightly Contacts / Leads /
Organisations. **Insightly's v3.1 REST API has no native upsert** — unlike
Salesforce's External-ID `PATCH` or HubSpot's `/batch/upsert`, Insightly
only offers plain create (`POST /{ObjectType}`) and update-by-numeric-id
(`PUT /{ObjectType}/{id}`), plus a generic field-based search
(`GET /{ObjectType}/Search?field_name=X&field_value=Y`).

This sink emulates upsert the same way `pipedrive_person_upsert` does:
search for an existing record by `dedupe_field`, then PUT the match or
POST a new one — via `InsightlyResource.upsert()`.

**Race condition (read this before relying on this for high-concurrency
writes)**: the search and the create-or-update are two separate HTTP
calls, not one atomic server-side operation. Two concurrent upserts for
the same dedupe key can both see "no match" and both POST, producing a
duplicate Insightly record. Safe for the common case (a sequential or
single-writer nightly/hourly reverse-ETL run); NOT safe if multiple
writers can race on the same key at the same time.

Contacts store email/phone in a nested `CONTACTINFOS` array rather than
flat fields. This sink special-cases `fields_map` values `"email"` /
`"phone"` (only when `object_type: Contacts`) to build that array
automatically — everything else in `fields_map` maps straight to a flat
top-level Insightly field name. Leads and Organisations are flatter (e.g.
Leads expose a flat `EMAIL` field) — map those directly, don't use the
`"email"` / `"phone"` sentinel for non-Contacts object types.

Pairs with:
  - ``insightly_resource`` — HTTP Basic auth + workhorse HTTP + search/upsert (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_EMAIL_SENTINEL = "email"
_PHONE_SENTINEL = "phone"

# Default Insightly search field_name for the email/phone sentinels, used
# when the caller doesn't override via `dedupe_search_field_name`.
# EMAIL_ADDRESS is Insightly's own documented example field_name for
# Contacts/Search; PHONE is this repo's best-effort default for phone-based
# dedupe (verify against your own instance — Insightly's public docs don't
# enumerate every virtual search field name for CONTACTINFOS-backed data).
_DEFAULT_SEARCH_FIELD_NAME = {
    _EMAIL_SENTINEL: "EMAIL_ADDRESS",
    _PHONE_SENTINEL: "PHONE",
}


def _row_value(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v


def _build_body(row, fields_map: Dict[str, str], object_type: str):
    """Shape one upstream row into an Insightly request body.

    For `object_type == "Contacts"`, fields_map values `"email"` / `"phone"`
    are nested into a `CONTACTINFOS` array instead of landing as flat keys.
    Every other value is a flat top-level Insightly field name.
    """
    body: Dict[str, Any] = {}
    contactinfos: List[Dict[str, str]] = []
    for col, target in fields_map.items():
        v = _row_value(row[col])
        if v is None:
            continue
        if object_type == "Contacts" and target == _EMAIL_SENTINEL:
            contactinfos.append({"TYPE": "EMAIL", "LABEL": "Work", "DETAIL": str(v)})
        elif object_type == "Contacts" and target == _PHONE_SENTINEL:
            contactinfos.append({"TYPE": "PHONE", "LABEL": "Work", "DETAIL": str(v)})
        else:
            body[target] = v
    if contactinfos:
        body["CONTACTINFOS"] = contactinfos
    return body


class InsightlyRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (emulated) rows from an upstream DataFrame into Insightly CRM.

    Example:
        ```yaml
        type: dagster_component_templates.InsightlyRecordUpsertComponent
        attributes:
          asset_name: insightly_contacts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: insightly
          object_type: Contacts
          dedupe_field: email
          fields_map:
            work_email: email
            first_name: FIRST_NAME
            last_name: LAST_NAME
          batch_size: 5000
        ```

    Insightly has **no native upsert**. For every upstream row this sink:
      1. Searches `GET /{object_type}/Search?field_name=X&field_value=Y`
         (via `InsightlyResource.search()`) using the value mapped to
         `dedupe_field`.
      2. `PUT /{object_type}/{id}` if a match was found, else
         `POST /{object_type}` to create.

    This is **search-then-write, not atomic** — see the module docstring
    for the race-condition caveat.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
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
        default="insightly",
        description="Resource key registered by InsightlyResourceComponent.",
    )

    object_type: str = Field(
        description=(
            "Target Insightly object type: 'Contacts', 'Leads', or "
            "'Organisations'. Only 'Contacts' gets the CONTACTINFOS "
            "email/phone nesting — Leads/Organisations use flat fields."
        ),
    )
    dedupe_field: str = Field(
        description=(
            "fields_map VALUE used to find an existing record before "
            "create-or-update — e.g. 'email' (the CONTACTINFOS sentinel, "
            "Contacts only) or a flat field name like 'LAST_NAME'. MUST be "
            "present in fields_map values, exactly like external_id_field "
            "in salesforce_record_upsert / key_property in hubspot_object_upsert."
        ),
    )
    dedupe_search_field_name: Optional[str] = Field(
        default=None,
        description=(
            "Insightly field_name passed to GET /{object_type}/Search. "
            "Defaults to 'EMAIL_ADDRESS' when dedupe_field='email', 'PHONE' "
            "when dedupe_field='phone', else dedupe_field itself verbatim "
            "(for flat fields, where the Insightly field name and the "
            "fields_map value are the same string). Override if your "
            "instance's searchable field name differs."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column → Insightly field name. Values 'email' / "
            "'phone' are special-cased (Contacts only) into the "
            "CONTACTINFOS array; every other value is a flat top-level "
            "Insightly field name (e.g. 'FIRST_NAME', 'LAST_NAME')."
        ),
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Insightly has no batch "
            "upsert endpoint — every row is one search + one create/update "
            "HTTP round trip, so keep this modest relative to your plan's "
            "daily rate limit."
        ),
    )

    group_name: Optional[str] = Field(default="insightly", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'insightly').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("insightly")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "InsightlyRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        valid_object_types = {"Contacts", "Leads", "Organisations"}
        if self.object_type not in valid_object_types:
            raise ValueError(
                f"InsightlyRecordUpsertComponent: object_type={self.object_type!r} "
                f"not supported. Must be one of {sorted(valid_object_types)}."
            )

        # Validate dedupe_field is in fields_map values.
        mapped_values = set(self.fields_map.values())
        if self.dedupe_field not in mapped_values:
            raise ValueError(
                f"InsightlyRecordUpsertComponent: dedupe_field="
                f"{self.dedupe_field!r} not in fields_map values. "
                f"dedupe_field must be a value you're mapping to in "
                f"fields_map. fields_map values: {sorted(mapped_values)}"
            )

        search_field_name = self.dedupe_search_field_name or _DEFAULT_SEARCH_FIELD_NAME.get(
            self.dedupe_field, self.dedupe_field
        )

        # Upstream column mapped to dedupe_field.
        dedupe_col = next(
            (col for col, target in self.fields_map.items() if target == self.dedupe_field),
            None,
        )
        if dedupe_col is None:
            raise ValueError(
                f"InsightlyRecordUpsertComponent: fields_map has no column "
                f"mapping to dedupe_field={self.dedupe_field!r}. "
                f"fields_map: {self.fields_map}"
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
            raise ValueError(f"InsightlyRecordUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            insightly = getattr(context.resources, _self.resource_key)

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

            created = 0
            updated = 0
            skipped_no_key = 0
            errors: List[str] = []

            for _, row in df.iterrows():
                dedupe_value = _row_value(row[dedupe_col])
                if dedupe_value is None or dedupe_value == "":
                    skipped_no_key += 1
                    continue
                body = _build_body(row, _self.fields_map, _self.object_type)
                if not body:
                    skipped_no_key += 1
                    continue
                try:
                    result = insightly.upsert(
                        _self.object_type,
                        search_field_name,
                        dedupe_value,
                        body,
                    )
                    if result.get("action") == "created":
                        created += 1
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row ({_self.dedupe_field}={dedupe_value!r}): {type(e).__name__}: {e}")

            context.log.info(
                f"Insightly upsert into {_self.object_type}: {created} created, "
                f"{updated} updated, {len(errors)} errors, {skipped_no_key} skipped "
                f"(missing {_self.dedupe_field}) — matched on "
                f"{search_field_name} (search-then-write, not atomic)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "insightly_object_type": dg.MetadataValue.text(_self.object_type),
                "dedupe_field": dg.MetadataValue.text(_self.dedupe_field),
                "dedupe_search_field_name": dg.MetadataValue.text(search_field_name),
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
                f"Upsert (emulated — search-then-write) DataFrame rows into "
                f"Insightly {_self.object_type} (match on {_self.dedupe_field})."
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
