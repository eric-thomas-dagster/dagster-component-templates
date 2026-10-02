"""DataFrame -> Copper CRM record upsert (search-then-write).

Copper has NO native atomic upsert endpoint for People / Leads /
Companies -- unlike Salesforce's External-ID PATCH, there is no single
call that creates-or-updates. This sink emulates one per row:

    1. POST /{object_type}/search with a filter shaped for `dedupe_field`
       (e.g. `{"emails": ["a@b.com"]}` for People).
    2. If a match comes back, PUT /{object_type}/{id}.
    3. If not, POST /{object_type} to create.

*** This is NOT race-condition-safe. *** Between step 1 and step 2/3,
another concurrent writer (another Dagster run, a human in the Copper UI,
a different integration) can create a record matching the same search
filter without this method seeing it -- the result is a duplicate
record, not data corruption, but a duplicate nonetheless. Safe for
single-writer / sequential reverse-ETL jobs; NOT safe if multiple
processes may upsert the same `object_type` concurrently against
overlapping key sets.

Copper's write-body shape genuinely differs across object types
(verified against developer.copper.com):

  - People:    `emails: [{"email": ..., "category": "work"}]` (array)
  - Leads:     `email: {"email": ..., "category": "work"}` (SINGULAR object,
               not an array -- a real Copper inconsistency vs. People)
  - Companies: no emails field at all -- only a singular `email_domain`
               string. Map a column to the literal Copper field name
               `email_domain` for Companies rather than the logical `email`.

All three object types accept `phone_numbers: [{"number": ..., "category":
"work"}]` (array) and a plain top-level `name` string.

Search-filter shapes ALSO differ (another verified Copper inconsistency):
  - People:    `{"emails": [value]}`     -- array
  - Leads:     `{"emails": value}`        -- bare string, not an array
  - Companies: `{"email_domains": value}` -- plural key, even though the
               create/update body field is the singular `email_domain`.

Pairs with:
  - ``copper_resource`` -- header API-key auth + generic get/post/put/search/upsert (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_VALID_OBJECT_TYPES = {"people", "leads", "companies"}

# Logical fields_map values that get special shaping instead of a flat
# top-level passthrough. Anything else in fields_map.values() is written
# as a plain top-level scalar field (e.g. 'name', 'email_domain', a
# custom field slug, 'details', etc.).
_SPECIAL_FIELDS = {"email", "phone"}


def _build_write_body(object_type: str, mapped: Dict[str, Any]) -> Dict[str, Any]:
    """Shape a per-row write body for `object_type` from {copper_field: value}.

    `mapped` already has None/NaN values dropped by the caller.
    """
    body: Dict[str, Any] = {}
    email_value = mapped.pop("email", None)
    phone_value = mapped.pop("phone", None)

    # Everything left over is a flat top-level field (name, email_domain,
    # details, custom field slugs, ...).
    body.update(mapped)

    if phone_value is not None:
        body["phone_numbers"] = [{"number": phone_value, "category": "work"}]

    if email_value is not None:
        if object_type == "people":
            body["emails"] = [{"email": email_value, "category": "work"}]
        elif object_type == "leads":
            body["email"] = {"email": email_value, "category": "work"}
        elif object_type == "companies":
            # Companies have no `emails`/`email` field -- only `email_domain`.
            # If the caller mapped a column to the logical 'email' field for
            # a companies sync, treat the value as an email domain rather
            # than silently dropping it.
            body["email_domain"] = email_value

    return body


def _build_search_filter(object_type: str, dedupe_field: str, value: Any) -> Dict[str, Any]:
    """Shape the POST /{object_type}/search filter body for `dedupe_field`.

    Copper's search filter shapes are NOT uniform with the write-body
    shapes or even with each other -- verified against Copper's own docs:
      - people:    emails filter is an ARRAY:  {"emails": [value]}
      - leads:     emails filter is a STRING:  {"emails": value}
      - companies: domain filter key is PLURAL and different from the
                   write-body field name: {"email_domains": value}
    Any other dedupe_field is passed through as a flat top-level filter
    key (e.g. "name": value), which Copper's search endpoints also accept.
    """
    if dedupe_field == "email":
        if object_type == "people":
            return {"emails": [value]}
        if object_type == "leads":
            return {"emails": value}
        if object_type == "companies":
            return {"email_domains": value}
    if dedupe_field == "email_domain" and object_type == "companies":
        return {"email_domains": value}
    return {dedupe_field: value}


class CopperRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Copper CRM People / Leads / Companies.

    Copper has no atomic upsert endpoint, so this is emulated via
    search-then-write (search by `dedupe_field`, then PUT the match or
    POST a new record). **This is not race-condition-safe** under
    concurrent writers targeting overlapping key sets -- see the module
    docstring and README for the full discussion.

    Example:
        ```yaml
        type: dagster_component_templates.CopperRecordUpsertComponent
        attributes:
          asset_name: copper_people_mirror
          upstream_asset_key: dbt_marts_contacts
          resource_key: copper
          object_type: people
          dedupe_field: email
          fields_map:
            full_name: name
            email_address: email
            phone: phone
          batch_size: 2000
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
        default="copper",
        description="Resource key registered by CopperResourceComponent.",
    )

    object_type: str = Field(
        description="Target Copper object type: 'people', 'leads', or 'companies'.",
    )

    dedupe_field: str = Field(
        description=(
            "Copper field used to search for an existing record before writing "
            "(typically 'email'; MUST be present in fields_map values). Copper "
            "has no atomic upsert, so this match is emulated via search-then-write "
            "and is therefore NOT race-condition-safe under concurrent writers -- "
            "see the component README."
        ),
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Copper field name. Special logical names "
            "'email' and 'phone' are shaped into Copper's nested "
            "emails/email/phone_numbers structures (shape differs by "
            "object_type -- see module docstring). Any other value is "
            "written as a flat top-level scalar field (e.g. 'name', "
            "'email_domain' for companies, a custom field slug)."
        ),
    )

    batch_size: int = Field(
        default=2000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="copper", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'copper')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("copper")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "CopperRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        object_type = (self.object_type or "").lower()
        if object_type not in _VALID_OBJECT_TYPES:
            raise ValueError(
                f"CopperRecordUpsertComponent: object_type={self.object_type!r} not "
                f"supported. Use one of {sorted(_VALID_OBJECT_TYPES)}."
            )

        if self.dedupe_field not in self.fields_map.values():
            raise ValueError(
                f"CopperRecordUpsertComponent: dedupe_field={self.dedupe_field!r} not in "
                f"fields_map values. fields_map values: {sorted(set(self.fields_map.values()))}"
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
            raise ValueError(f"CopperRecordUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            copper = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to upsert.")
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

            # Upstream column mapped to dedupe_field.
            dedupe_col = next(
                (col for col, copper_field in _self.fields_map.items()
                 if copper_field == _self.dedupe_field),
                None,
            )
            if dedupe_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to dedupe_field="
                    f"{_self.dedupe_field!r}. fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            created = 0
            updated = 0
            skipped_no_key = 0
            errors: List[str] = []

            for _, row in df.iterrows():
                dedupe_value = _row_value(row[dedupe_col])
                if dedupe_value is None:
                    skipped_no_key += 1
                    continue

                mapped: Dict[str, Any] = {}
                for col, copper_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        mapped[copper_field] = v

                write_body = _build_write_body(object_type, dict(mapped))
                search_filter = _build_search_filter(object_type, _self.dedupe_field, dedupe_value)

                try:
                    result = copper.upsert(object_type, search_filter, write_body)
                    if result.get("action") == "created":
                        created += 1
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row ({_self.dedupe_field}={dedupe_value!r}): {type(e).__name__}: {e}"
                    )

            context.log.info(
                f"Copper {object_type} upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.dedupe_field}) "
                f"-- matched via search-then-write on {_self.dedupe_field}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "copper_object_type": dg.MetadataValue.text(object_type),
                "dedupe_field": dg.MetadataValue.text(_self.dedupe_field),
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
                f"Upsert DataFrame rows into Copper {object_type} "
                f"(search-then-write match on {_self.dedupe_field})."
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
