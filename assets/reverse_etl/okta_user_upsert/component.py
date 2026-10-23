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
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'okta')."
    )

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
            partitions_def=partitions_def,
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
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

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
