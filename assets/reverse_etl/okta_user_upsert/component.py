"""DataFrame -> Okta user sync (create-if-absent / update-if-present),
with an explicit, separate `operation: deactivate` mode.

**Safety is the point of this component -- read this before configuring it.**

Default behavior (`operation: sync`, the default -- no config needed):
  - look up each row's user via `GET /api/v1/users/{login}` (Okta's Get
    User endpoint accepts id, login, or email directly).
  - if found: `POST /api/v1/users/{id}` -- a profile MERGE, not a replace;
    only the row's mapped attributes change, nothing else on the user is
    touched. The user's lifecycle status is never touched in this mode,
    no matter what columns the upstream row has (Okta's profile object
    has no "status"/"active" field to even put there).
  - if not found: `POST /api/v1/users` to create a new user.
  - This mode can NEVER deactivate or delete anyone.

Deactivation (`operation: deactivate`, must be set explicitly per run/asset):
  - look up each row's user via login.
  - if NOT found: skip (counted, logged) -- deactivate mode never creates
    a user.
  - if already DEPROVISIONED (already deactivated): skip (counted, logged)
    as a no-op -- Okta's deactivate endpoint can only be called on a user
    that is not already DEPROVISIONED.
  - otherwise: `POST /api/v1/users/{id}/lifecycle/deactivate` -- Okta's
    distinct lifecycle endpoint. This is a real, structurally separate
    call from update_user/create_user, not a profile-field flip.

There is NO delete operation, anywhere, under any configuration. Okta's
Users API does expose `DELETE /api/v1/users/{id}` (only callable on an
already-DEPROVISIONED user -- a genuine, unrecoverable hard delete that
Okta's own docs recommend against for audit/compliance reasons), but
neither this component nor `okta_resource` ever calls it. See README.md's
"Safety" section.

`operation` is validated at build_defs time against an explicit allow-list
(`sync`, `deactivate`) -- anything else raises immediately rather than
silently no-op'ing or guessing intent.

Pairs with:
  - ``okta_resource`` -- OAuth2 client_credentials connection (required)
"""
import math
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_OPERATIONS = {"sync", "deactivate"}
# Okta's profile object has no field that controls lifecycle status, so
# there's no analogous "blocked"-style footgun to guard fields_map against
# -- deactivation structurally cannot happen through a profile attribute.
# `status` is reserved anyway, defense-in-depth, in case a custom user
# schema attribute is ever named that.
_RESERVED_TARGET_FIELDS = {"status"}


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


class OktaUserUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Sync an upstream DataFrame of user records into Okta (create-if-absent
    / update-if-present by default; explicit `operation: deactivate` to
    deactivate matched users). See README.md "Safety" for the full
    guarantees.

    Example:
        ```yaml
        type: dagster_component_templates.OktaUserUpsertComponent
        attributes:
          asset_name: okta_employee_sync
          upstream_asset_key: dbt_marts_active_employees
          resource_key: okta_resource
          fields_map:
            work_email: login
            personal_email: secondEmail
            first_name: firstName
            last_name: lastName
          operation: sync
        ```
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
        default="okta_resource",
        description="Resource key registered by OktaResourceComponent.",
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Okta profile attribute (flat names: 'login', "
            "'email', 'firstName', 'lastName', 'mobilePhone', 'secondEmail', "
            "'displayName', or a custom schema attribute). Exactly one value "
            "MUST be 'login' -- it is the match key used to look up existing "
            "users (Okta's Get User endpoint accepts login directly). The "
            "value 'status' is forbidden here -- Okta has no profile field "
            "that controls lifecycle status, and deactivation is controlled "
            "ONLY by the `operation` field below."
        ),
    )
    operation: str = Field(
        default="sync",
        description=(
            "'sync' (default, SAFE) -- create-if-absent, update-profile-if-"
            "present (merge semantics); never deactivates or deletes anyone. "
            "'deactivate' (EXPLICIT) -- deactivates users matched by login "
            "via Okta's lifecycle endpoint; rows with no matching user are "
            "skipped, never created. No other value is accepted -- there is "
            "no 'delete' operation."
        ),
    )
    activate_on_create: bool = Field(
        default=True,
        description=(
            "Only used in 'sync' mode when a row has no existing Okta match: "
            "passed as Okta's own `activate` query param on user creation "
            "(Okta's documented default is also true)."
        ),
    )
    send_deactivate_email: bool = Field(
        default=False,
        description="Only used in 'deactivate' mode: Okta's `sendEmail` lifecycle param.",
    )
    batch_size: int = Field(
        default=5000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="okta", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'okta')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("okta")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "OktaUserUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in _SUPPORTED_OPERATIONS:
            raise ValueError(
                f"OktaUserUpsertComponent: operation must be one of "
                f"{sorted(_SUPPORTED_OPERATIONS)} -- got {self.operation!r}. "
                f"There is no 'delete' operation; this component can never "
                f"permanently remove an Okta user."
            )

        target_fields = list(self.fields_map.values())
        login_targets = [t for t in target_fields if t == "login"]
        if len(login_targets) != 1:
            raise ValueError(
                "OktaUserUpsertComponent: fields_map must map exactly one "
                "upstream column to the Okta field 'login' (used as the "
                f"match key for find-or-create). Got {len(login_targets)} "
                f"such mapping(s) in fields_map={self.fields_map!r}."
            )
        reserved_hit = _RESERVED_TARGET_FIELDS & set(target_fields)
        if reserved_hit:
            raise ValueError(
                f"OktaUserUpsertComponent: fields_map may not target "
                f"{sorted(reserved_hit)} -- lifecycle status is controlled "
                f"ONLY by the `operation` field (operation: deactivate), "
                f"never by row data."
            )

        login_col = next(col for col, target in self.fields_map.items() if target == "login")

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
            raise ValueError(f"OktaUserUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _build_profile(row: Dict[str, Any]) -> Dict[str, Any]:
            profile: Dict[str, Any] = {}
            for col, target in _self.fields_map.items():
                raw = row.get(col)
                if _is_blank(raw):
                    continue
                profile[target] = raw
            return profile

        def _run_upsert(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to sync.")
                return dg.MaterializeResult(metadata={"rows_total": dg.MetadataValue.int(0)})

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

            resource = getattr(context.resources, _self.resource_key)

            created = 0
            updated = 0
            deactivated = 0
            skipped_no_login = 0
            skipped_not_found = 0
            skipped_already_deactivated = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                login = row_dict.get(login_col)
                if _is_blank(login):
                    skipped_no_login += 1
                    continue
                login = str(login).strip()

                try:
                    existing = resource.find_user(login)
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (login={login}): lookup failed: {type(e).__name__}: {e}")
                    continue

                if _self.operation == "deactivate":
                    if existing is None:
                        skipped_not_found += 1
                        continue
                    if existing.get("status") == "DEPROVISIONED":
                        skipped_already_deactivated += 1
                        continue
                    user_id = existing["id"]
                    try:
                        resource.deactivate_user(user_id, send_email=_self.send_deactivate_email)
                        deactivated += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"row {i} (user_id={user_id}): deactivate failed: {type(e).__name__}: {e}")
                    continue

                # operation == "sync"
                profile = _build_profile(row_dict)
                if existing is None:
                    try:
                        resource.create_user(profile, credentials=None, activate=_self.activate_on_create)
                        created += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"row {i} (login={login}): create failed: {type(e).__name__}: {e}")
                else:
                    user_id = existing["id"]
                    try:
                        resource.update_user(user_id, profile)
                        updated += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"row {i} (user_id={user_id}): update failed: {type(e).__name__}: {e}")

            context.log.info(
                f"Okta {_self.operation}: created={created} updated={updated} "
                f"deactivated={deactivated} skipped_no_login={skipped_no_login} "
                f"skipped_not_found={skipped_not_found} "
                f"skipped_already_deactivated={skipped_already_deactivated} "
                f"errors={len(errors)}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "operation": dg.MetadataValue.text(_self.operation),
                "rows_total": dg.MetadataValue.int(len(df)),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_deactivated": dg.MetadataValue.int(deactivated),
                "rows_skipped_no_login": dg.MetadataValue.int(skipped_no_login),
                "rows_skipped_not_found": dg.MetadataValue.int(skipped_not_found),
                "rows_skipped_already_deactivated": dg.MetadataValue.int(skipped_already_deactivated),
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
                f"Sync DataFrame rows into Okta users (operation={_self.operation}, "
                f"match on login)."
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
