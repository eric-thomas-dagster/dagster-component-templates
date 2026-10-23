"""SAP RFC Ingestion Component.

Execute an SAP RFC (Remote Function Call) — typically a BAPI or `RFC_READ_TABLE`
— and materialize the result as a Dagster asset (pandas DataFrame).

Two common patterns:

1. **`RFC_READ_TABLE`**: read any SAP table (MARA, KNA1, BSEG, …). Limited to
   tables with row widths ≤ 512 chars and result size ≤ ~50K rows per call
   (server limit). For wider/larger tables, use the `paginated` mode which
   chunks via `ROWCOUNT` + `ROWSKIPS`. Or use a BW Open Hub or custom Z-RFC.

2. **BAPI call**: invoke a Business API like `BAPI_USER_GETLIST`,
   `BAPI_MATERIAL_GET_DETAIL`, custom Z-BAPIs. Configure the RFC name +
   import parameters; the returned table/structure is flattened to DataFrame.

Connection comes from `sap_rfc_resource_key`. Install pyrfc + the SAP NW RFC
SDK first — see `sap_rfc_resource` README.
"""

from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _parse_rfc_read_table(result: dict) -> pd.DataFrame:
    """Parse the result of RFC_READ_TABLE into a typed DataFrame.

    RFC_READ_TABLE returns:
      FIELDS: [{FIELDNAME, OFFSET, LENGTH, TYPE, FIELDTEXT}, ...]
      DATA: [{WA: "value1|value2|value3"}, ...]   (delimiter-separated string)

    We split on the configured delimiter (default '|') and align to FIELDS.
    """
    fields = result.get("FIELDS", [])
    data = result.get("DATA", [])
    if not fields:
        return pd.DataFrame()
    columns = [f["FIELDNAME"] for f in fields]
    rows = []
    for row in data:
        wa = row.get("WA", "")
        parts = [p.strip() for p in wa.split("|")]
        # Pad/truncate to column count
        if len(parts) < len(columns):
            parts += [""] * (len(columns) - len(parts))
        rows.append(parts[: len(columns)])
    return pd.DataFrame(rows, columns=columns)



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


class SapRFCIngestionComponent(Component, Model, Resolvable):
    """Execute an SAP RFC / BAPI and materialize the result as a DataFrame.

    Example — read MARA (material master) via RFC_READ_TABLE:

        ```yaml
        type: dagster_component_templates.SapRFCIngestionComponent
        attributes:
          asset_name: material_master
          sap_rfc_resource_key: sap_ecc
          mode: read_table
          table_name: MARA
          fields: [MATNR, MTART, MATKL, MEINS, ERSDA]
          where_clause: "MTART = 'FERT' AND ERSDA >= '{partition_key}'"
          partition_type: daily
          partition_start: '2024-01-01'
        ```

    Example — call BAPI_MATERIAL_GET_LIST:

        ```yaml
        attributes:
          asset_name: material_list
          sap_rfc_resource_key: sap_ecc
          mode: bapi
          rfc_name: BAPI_MATERIAL_GET_LIST
          import_params:
            MAXROWS: 1000
          result_table: MATNRLIST
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    sap_rfc_resource_key: str = Field(
        description="Key of a registered sap_rfc_resource (provides the connection)"
    )

    mode: str = Field(
        default="read_table",
        description="'read_table' (RFC_READ_TABLE wrapper) | 'bapi' (generic RFC/BAPI call)",
    )

    # --- read_table mode ---------------------------------------------------

    table_name: Optional[str] = Field(
        default=None, description="SAP table to read (read_table mode)"
    )
    fields: Optional[List[str]] = Field(
        default=None,
        description="Column projection. If unset, returns all columns. Use this — SAP tables are wide.",
    )
    where_clause: Optional[str] = Field(
        default=None,
        description="WHERE clause (SAP-dialect SQL). Supports `{partition_key}`. Max 75 chars per item, see SAP docs.",
    )
    delimiter: str = Field(default="|", description="Delimiter for RFC_READ_TABLE result rows.")
    rowcount: int = Field(default=0, description="Row limit (0 = all)")
    rowskips: int = Field(default=0, description="Rows to skip (for manual pagination)")

    # --- bapi mode ----------------------------------------------------------

    rfc_name: Optional[str] = Field(
        default=None,
        description="RFC/BAPI function name (e.g. 'BAPI_USER_GETLIST', 'Z_CUSTOM_RFC')",
    )
    import_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Import parameters for the RFC call. Values support `{partition_key}` substitution in strings.",
    )
    result_table: Optional[str] = Field(
        default=None,
        description="Name of the result table parameter to materialize as DataFrame. If unset, the entire result dict becomes a 1-row DataFrame.",
    )

    # --- Standard fields ---------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="sap_rfc")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)

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
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (one RFC call "
            "per partition in the range, each with its own "
            "where_clause/import_params {partition_key} substitution, "
            "concatenated together). 'multi_run' launches one run per "
            "partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a "
            "partition-range-aware IO manager (e.g. a DB-table IO manager "
            "with partition_expr set) -- a generic filesystem/default IO "
            "manager cannot persist a single_run output spanning multiple "
            "partitions and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or f"SAP RFC ingestion ({asset_name})"

        kinds = list(self.kinds or []) or ["sap", "rfc"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        rk = self.sap_rfc_resource_key

        backfill_policy_obj = None
        if partitions_def is not None and component.backfill_policy == "single_run":
            backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and component.backfill_policy == "multi_run":
            backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=component.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            backfill_policy=backfill_policy_obj,
            retry_policy=retry_policy,
            required_resource_keys={rk},
        )
        def sap_rfc_ingestion_asset(context: AssetExecutionContext):
            resource = getattr(context.resources, rk)

            # Partition key(s) this call must process: a normal tick gives
            # exactly one; a single_run backfill gives the whole range.
            partition_keys: List[Optional[str]] = [None]
            if context.has_partition_key or context.has_partition_key_range:
                partition_keys = list(context.partition_keys)

            mode = component.mode.lower()

            def _call_one(conn, partition_key: Optional[str]) -> pd.DataFrame:
                if mode == "read_table":
                    if not component.table_name:
                        raise ValueError("mode='read_table' requires table_name.")
                    where = component.where_clause or ""
                    if partition_key is not None and "{partition_key}" in where:
                        where = where.replace("{partition_key}", partition_key)
                    # WHERE comes in as a sequence of <=75-char strings
                    options = []
                    while where:
                        options.append({"TEXT": where[:75]})
                        where = where[75:]

                    kwargs: Dict[str, Any] = {
                        "QUERY_TABLE": component.table_name,
                        "DELIMITER": component.delimiter,
                    }
                    if component.rowcount:
                        kwargs["ROWCOUNT"] = component.rowcount
                    if component.rowskips:
                        kwargs["ROWSKIPS"] = component.rowskips
                    if component.fields:
                        kwargs["FIELDS"] = [{"FIELDNAME": f} for f in component.fields]
                    if options:
                        kwargs["OPTIONS"] = options

                    context.log.info(
                        f"RFC_READ_TABLE QUERY_TABLE={component.table_name} "
                        f"fields={component.fields or 'all'} where={component.where_clause or 'none'}"
                    )
                    result = conn.call("RFC_READ_TABLE", **kwargs)
                    return _parse_rfc_read_table(result)

                elif mode == "bapi":
                    if not component.rfc_name:
                        raise ValueError("mode='bapi' requires rfc_name.")
                    params: Dict[str, Any] = {}
                    if component.import_params:
                        for k, v in component.import_params.items():
                            if (
                                partition_key is not None
                                and isinstance(v, str)
                                and "{partition_key}" in v
                            ):
                                v = v.replace("{partition_key}", partition_key)
                            params[k] = v
                    context.log.info(
                        f"RFC call {component.rfc_name} params={list(params.keys())}"
                    )
                    result = conn.call(component.rfc_name, **params)

                    if component.result_table:
                        table = result.get(component.result_table)
                        if table is None:
                            available = list(result.keys())
                            raise ValueError(
                                f"result_table={component.result_table!r} not in response. "
                                f"Available: {available}"
                            )
                        return pd.DataFrame(table)
                    else:
                        # Whole result as a 1-row DataFrame of scalar values
                        return pd.DataFrame([{k: v for k, v in result.items() if not isinstance(v, (list, dict))}])

                else:
                    raise ValueError(f"unknown mode: {mode!r} (expected 'read_table' or 'bapi')")

            with resource.connection() as conn:
                dfs = [_call_one(conn, pk) for pk in partition_keys]
            df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

            context.log.info(f"SAP RFC: {len(df)} rows × {len(df.columns)} cols")

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "mode": MetadataValue.text(mode),
            }
            if mode == "read_table":
                metadata["table"] = MetadataValue.text(component.table_name or "")
            else:
                metadata["rfc_name"] = MetadataValue.text(component.rfc_name or "")
            if partition_keys != [None]:
                metadata["partition_keys"] = MetadataValue.json(partition_keys)
            if component.include_preview_metadata and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(component.preview_rows, len(df)))
                        if len(df) > component.preview_rows * 10
                        else df.head(component.preview_rows)
                    )
                    metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[sap_rfc_ingestion_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
