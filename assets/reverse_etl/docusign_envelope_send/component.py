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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
        default=None, description="Asset kinds (auto-includes 'docusign')."
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
        )
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
            partitions_def=partitions_def,
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
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_send(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_send(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
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
