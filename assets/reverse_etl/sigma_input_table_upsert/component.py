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


class SigmaInputTableUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write rows from an upstream DataFrame into an existing Sigma Input
    Table, via a pre-published webhook-triggered Action Sequence.

    Example:
        ```yaml
        type: dagster_component_templates.SigmaInputTableUpsertComponent
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
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'sigma')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
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
