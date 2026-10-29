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


class CloseCrmLeadUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Close CRM Leads
    (emulated search-then-write upsert -- Close has no native upsert).

    Example:
        ```yaml
        type: dagster_community_components.CloseCrmLeadUpsertComponent
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
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'close').",
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
            partitions_def=partitions_def,
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
