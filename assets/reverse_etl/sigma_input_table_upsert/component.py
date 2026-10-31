"""DataFrame -> Sigma Input Table writeback, via a webhook-triggered Action
Sequence.

Sigma's Input Tables are a genuine reverse-ETL writeback target -- external
systems can push rows into a workbook that Sigma users then build
dashboards/models against. But (verified against Sigma's own REST API
reference, not guessed): **there is no direct "write rows to an Input
Table" REST endpoint.** The only documented API-level categories are
Account types / Agents / Connections / Data models / Datasets (deprecated)
/ Files / Members / Query / Reports / Teams / Templates / Webhooks /
Workbooks, etc. -- no "Input Tables" category exists, and `POST /v2/files`
("Create a file") only creates empty containers, it does not accept CSV
content.

The real, documented write path is Sigma's **Webhook trigger (Beta)**
feature for workbook Action Sequences:
  - https://help.sigmacomputing.com/docs/configure-action-sequences-to-run-automatically
  - https://help.sigmacomputing.com/reference/send-to-webhook
  - https://help.sigmacomputing.com/reference/get-webhook-schema

A human first builds, in the Sigma UI, a published Action Sequence on the
target workbook containing an "Insert row(s)" (or "Update row(s)") action
that targets the specific Input Table, with a **Webhook trigger** exposing
one or more parameters (Text/Number/Date/Logical, or array variants).
This component is the caller side: it POSTs the upstream DataFrame's rows,
chunked at Sigma's documented 2,000-rows-per-action cap, to that
already-configured webhook:

    POST {base_url}/v2/webhooks/{workbookId}/{sequenceId}
    Authorization: Bearer <token>        (same OAuth2 client_credentials
                                           token as every other Sigma call)
    { "<rows_parameter_name>": [ {col: val, ...}, ... ], ...static_parameters }

Response is HTTP 202 `{"traceId": "..."}` -- **the trigger is asynchronous
and fire-and-forget**. This component can confirm the webhook *accepted*
each chunk (and surfaces each `traceId` for later audit), but it cannot
confirm the Input Table rows were actually written -- Sigma gives no
synchronous write-confirmation API. Don't rely on this component's success
as proof-of-write; verify in the workbook (or via `sigma_workbook_ingestion`
+ `is_input_table`) if you need that guarantee.

Pairs with:
  - ``sigma_resource`` -- OAuth2 client_credentials bearer token (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MAX_ROWS_PER_REQUEST = 2000  # Sigma's documented Insert row(s) cap per action.



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


class SigmaInputTableUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write rows from an upstream DataFrame into an existing Sigma Input
    Table, via a pre-published webhook-triggered Action Sequence.

    Example:
        ```yaml
        type: dagster_community_components.SigmaInputTableUpsertComponent
        attributes:
          asset_name: sigma_forecast_inputs_write
          upstream_asset_key: dbt_marts_forecast_inputs
          resource_key: sigma_resource
          workbook_id: "abc123-workbook-id"
          sequence_id: "def456-sequence-id"
          rows_parameter_name: rows
          fields_map:
            region: Region
            forecast_month: ForecastMonth
            forecast_value: ForecastValue
        ```

    This component does **not** create the Input Table, the workbook, or
    the Action Sequence -- all three must already exist and be published,
    with a Webhook trigger configured on the sequence (build that once in
    the Sigma UI: Workbooks -> Automation -> Action Sequences -> Webhook
    trigger). This component only calls that already-configured webhook.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
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
        default="sigma_resource",
        description="Resource key registered by SigmaResourceComponent.",
    )

    workbook_id: str = Field(
        description=(
            "Workbook ID containing the target Input Table's Action Sequence. "
            "Find it via `sigma_workbook_ingestion`, or the workbook's URL in "
            "the Sigma UI."
        ),
    )
    sequence_id: str = Field(
        description=(
            "ID of the already-published Action Sequence (with a Webhook "
            "trigger configured) that performs the Insert/Update row(s) "
            "action on the target Input Table. Create this once in the "
            "Sigma UI -- this component only calls it."
        ),
    )
    rows_parameter_name: str = Field(
        default="rows",
        description=(
            "JSON key of the array-type webhook parameter that receives the "
            "row objects -- must match the parameter name configured on the "
            "webhook trigger in Sigma exactly (webhook parameter names are "
            "user-defined at configuration time)."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Input Table column name (as the Insert/Update row(s) action expects it).",
    )
    static_parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Extra scalar webhook parameters sent on every call alongside "
            "rows_parameter_name (e.g. a batch marker), if your webhook "
            "trigger defines any beyond the rows array."
        ),
    )
    write_mode: str = Field(
        default="insert",
        description=(
            "Documentation/metadata only -- describes what the bound Action "
            "Sequence is expected to do ('insert' = Insert row(s), 'update' = "
            "Update row(s) keyed by row ID). This component always sends the "
            "same request shape; the actual behavior is whatever action the "
            "Sigma-side sequence runs, configured independently in the UI."
        ),
    )
    rows_per_request: int = Field(
        default=2000,
        ge=1,
        le=_MAX_ROWS_PER_REQUEST,
        description=(
            "Rows per webhook call. Capped at 2,000 -- Sigma's documented "
            "limit for an Insert row(s) action fed from a single multi-value "
            "source."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )
    validate_schema_before_write: bool = Field(
        default=True,
        description=(
            "Before sending any data, call `GET /v2/webhooks/{workbookId}/"
            "{sequenceId}/schema` and fail fast with a clear error if "
            "rows_parameter_name (or any static_parameters key) isn't among "
            "the webhook's declared parameters -- cheaper than discovering a "
            "silent misconfiguration via a 2am on-call page, since the "
            "webhook POST itself returns 202 for a lot of shapes it doesn't "
            "actually apply."
        ),
    )

    group_name: Optional[str] = Field(default="sigma", description="Dagster asset group name.")

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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
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
        default=None, description="Asset kinds (auto-includes 'sigma')."
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
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("sigma")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "SigmaInputTableUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.write_mode not in ("insert", "update"):
            raise ValueError(
                f"SigmaInputTableUpsertComponent: write_mode must be 'insert' "
                f"or 'update', got {self.write_mode!r}."
            )

        if not self.fields_map:
            raise ValueError(
                "SigmaInputTableUpsertComponent: fields_map must map at least "
                "one upstream column to an Input Table column."
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
            raise ValueError(f"SigmaInputTableUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _row_value(v):
            import pandas as pd
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return v

        def _run_upsert(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to write.")
                return dg.MaterializeResult(metadata={"rows_submitted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            missing_cols = [c for c in _self.fields_map if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            resource = getattr(context.resources, _self.resource_key)

            if _self.validate_schema_before_write:
                schema = resource.get(f"v2/webhooks/{_self.workbook_id}/{_self.sequence_id}/schema")
                declared = set((schema or {}).get("variables") or {})
                expected = {_self.rows_parameter_name} | set((_self.static_parameters or {}).keys())
                unknown = expected - declared
                if unknown:
                    raise dg.Failure(
                        f"Webhook schema for workbook={_self.workbook_id} "
                        f"sequence={_self.sequence_id} doesn't declare "
                        f"parameter(s) {sorted(unknown)}. Declared parameters: "
                        f"{sorted(declared)}. Check rows_parameter_name/"
                        f"static_parameters against the webhook trigger's "
                        f"configuration in Sigma."
                    )

            rows: List[dict] = []
            skipped_empty = 0
            for _, row in df.iterrows():
                rec: dict = {}
                for col, table_col in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        rec[table_col] = v
                if not rec:
                    skipped_empty += 1
                    continue
                rows.append(rec)

            if not rows:
                context.log.warning("No non-empty rows to write — nothing sent.")
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_empty": dg.MetadataValue.int(skipped_empty),
                    }
                )

            trace_ids: List[str] = []
            api_requests = 0
            for chunk_start in range(0, len(rows), _self.rows_per_request):
                chunk = rows[chunk_start:chunk_start + _self.rows_per_request]
                payload: Dict[str, Any] = {_self.rows_parameter_name: chunk}
                if _self.static_parameters:
                    payload.update(_self.static_parameters)
                result = resource.post(
                    f"v2/webhooks/{_self.workbook_id}/{_self.sequence_id}",
                    json_body=payload,
                )
                api_requests += 1
                trace_id = (result or {}).get("traceId")
                if trace_id:
                    trace_ids.append(trace_id)

            context.log.info(
                f"Sigma Input Table webhook write: workbook={_self.workbook_id} "
                f"sequence={_self.sequence_id} write_mode={_self.write_mode} "
                f"rows_submitted={len(rows)} rows_skipped_empty={skipped_empty} "
                f"api_requests={api_requests}. NOTE: the trigger is async -- "
                f"this confirms acceptance, not that the Input Table rows "
                f"were actually applied."
            )

            metadata = {
                "workbook_id": dg.MetadataValue.text(_self.workbook_id),
                "sequence_id": dg.MetadataValue.text(_self.sequence_id),
                "write_mode": dg.MetadataValue.text(_self.write_mode),
                "rows_submitted": dg.MetadataValue.int(len(rows)),
                "rows_skipped_empty": dg.MetadataValue.int(skipped_empty),
                "api_requests": dg.MetadataValue.int(api_requests),
            }
            if trace_ids:
                metadata["trace_ids"] = dg.MetadataValue.json(trace_ids)
            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            partitions_def=partitions_def,
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Write DataFrame rows into Sigma Input Table via workbook "
                f"{_self.workbook_id} sequence {_self.sequence_id} "
                f"(write_mode={_self.write_mode})."
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
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
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
