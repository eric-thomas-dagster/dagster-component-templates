"""BigQueryCreateTableFromQueryAssetComponent — CTAS as a Dagster asset.

Materializes a SQL query as a real BigQuery table (CREATE OR REPLACE
TABLE, or CREATE OR REPLACE MATERIALIZED VIEW). The transform layer
of any BQ-native ELT pipeline — same shape as a dbt model but run
directly without dbt.

Asset's stored value is a small DataFrame with the row count + table
metadata; downstream components reference the BQ table by ID, not the
DataFrame contents.
"""

import json
import os
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


class BigQueryCreateTableFromQueryAssetComponent(Component, Model, Resolvable):
    """Run a CREATE OR REPLACE TABLE / VIEW / MATERIALIZED VIEW from a SQL query.

    The destination_table_id is materialized in BigQuery at run time. The
    Dagster asset's stored value is a one-row DataFrame with the resulting
    row count + bytes_billed + slot_millis, useful for downstream conditional
    checks and lineage display. Downstream components reference the BQ
    table directly by id.
    """

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="Billing project. Defaults to the SA's project.")
    location: Optional[str] = Field(default=None, description="BQ location (`US`, `EU`, etc.). Default: auto-detect.")

    destination_table_id: str = Field(
        description="Fully-qualified destination, e.g. `my-project.analytics.orders_clean`.",
    )
    materialization: Literal["table", "view", "materialized_view"] = Field(
        default="table",
        description="`table` = CREATE OR REPLACE TABLE; `view` = CREATE OR REPLACE VIEW; `materialized_view` = CREATE OR REPLACE MATERIALIZED VIEW.",
    )
    query: str = Field(
        description="SELECT statement that defines the table contents. Supports {placeholder} substitution from query_params.",
    )
    query_params: Dict[str, Any] = Field(default_factory=dict)

    # Table-level options applied via CREATE TABLE ... OPTIONS
    table_options: Optional[Dict[str, Any]] = Field(
        default=None,
        description="OPTIONS clause k/v map (e.g. `{description: 'cleaned orders', labels: {tier: gold}}`).",
    )
    partition_field: Optional[str] = Field(
        default=None,
        description="Column to partition the destination table by (e.g. 'order_date'). Adds PARTITION BY.",
    )
    cluster_fields: Optional[List[str]] = Field(
        default=None,
        description="Columns to cluster on (max 4). Adds CLUSTER BY.",
    )

    deps: Optional[List[str]] = Field(default=None)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=("Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run: the query is formatted ONCE with partition_key_range_start/partition_key_range_end and partition_time_window_start/partition_time_window_end spanning the whole range (write the query against the window, not {partition_key}, which is deliberately left undefined for a multi-partition run so a query that depends on it fails loudly instead of silently using one partition). 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, uncapped). Ignored when partition_type/partition_dimensions is not set. The asset's returned value is persisted by the IO manager, so 'single_run' needs a partition-range-aware IO manager (the default filesystem IO manager cannot persist one output spanning multiple partitions and will raise a clear error at materialization time)."),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=('partition_values',), tags=('tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "BigQueryCreateTableFromQueryAssetComponent: provide credentials, "
                "credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        destination_table_id = self.destination_table_id
        materialization = self.materialization
        query_template = self.query
        query_params = dict(self.query_params or {})
        table_options = self.table_options
        partition_field = self.partition_field
        cluster_fields = self.cluster_fields

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"BigQuery {materialization}: {destination_table_id}",
            group_name=self.group_name,
            kinds={"bigquery", "sql", materialization},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(credentials=sa_creds, project=project_id, location=location)

            params = dict(query_params)
            if context.has_partition_key:
                params.setdefault("partition_key", context.partition_key)
            if context.has_partition_key or context.has_partition_key_range:
                # Range placeholders (a single_run backfill spans many
                # partitions, so `partition_key` itself is left undefined).
                _pkr = context.partition_key_range
                params.setdefault("partition_key_range_start", _pkr.start)
                params.setdefault("partition_key_range_end", _pkr.end)
                try:
                    _tw = context.partition_time_window
                    params.setdefault("partition_time_window_start", _tw.start.isoformat())
                    params.setdefault("partition_time_window_end", _tw.end.isoformat())
                except Exception:
                    pass  # non-time-window partitions have no window
            try:
                rendered_select = query_template.format(**params) if params else query_template
            except KeyError as e:
                raise ValueError(f"query references missing placeholder {e}; available: {list(params.keys())}")

            # Build the DDL.
            kw = {
                "table":              "TABLE",
                "view":               "VIEW",
                "materialized_view":  "MATERIALIZED VIEW",
            }[materialization]

            ddl_parts = [f"CREATE OR REPLACE {kw} `{destination_table_id}`"]
            if materialization in ("table", "materialized_view"):
                if partition_field:
                    ddl_parts.append(f"PARTITION BY {partition_field}")
                if cluster_fields:
                    ddl_parts.append(f"CLUSTER BY {', '.join(cluster_fields)}")
            if table_options:
                opts = []
                for k, v in table_options.items():
                    if isinstance(v, str):
                        opts.append(f"{k}={json.dumps(v)}")
                    elif isinstance(v, dict):
                        # labels = [("key1", "value1"), ...]
                        if k == "labels":
                            label_pairs = ", ".join(f'("{lk}", "{lv}")' for lk, lv in v.items())
                            opts.append(f"labels=[{label_pairs}]")
                        else:
                            opts.append(f"{k}={json.dumps(v)}")
                    else:
                        opts.append(f"{k}={v}")
                ddl_parts.append(f"OPTIONS({', '.join(opts)})")
            ddl_parts.append(f"AS\n{rendered_select}")
            ddl = "\n".join(ddl_parts)

            context.log.info(f"BQ {materialization} → {destination_table_id}")
            context.log.info(f"DDL preview: {ddl[:300]!r}...")

            try:
                job = client.query(ddl)
                job.result()
            except Exception as e:
                err_str = str(e)
                if "403" in err_str or "PERMISSION_DENIED" in err_str:
                    sa_email = creds_dict.get("client_email", "<sa>")
                    context.log.error(
                        f"BQ {materialization} create failed: SA {sa_email!r} needs "
                        f"roles/bigquery.dataEditor on the destination dataset "
                        f"AND roles/bigquery.jobUser on the project. "
                        f"Or grant roles/bigquery.admin / roles/owner."
                    )
                raise

            # Fetch resulting table metadata for downstream introspection / lineage.
            rows_out: Dict[str, Any] = {
                "destination_table_id": destination_table_id,
                "materialization":      materialization,
                "bytes_billed":         int(job.total_bytes_billed or 0),
                "bytes_processed":      int(job.total_bytes_processed or 0),
                "slot_millis":          int(job.slot_millis or 0),
                "cache_hit":            bool(job.cache_hit),
            }
            if materialization in ("table", "materialized_view"):
                try:
                    t = client.get_table(destination_table_id)
                    rows_out["row_count"]       = int(t.num_rows or 0)
                    rows_out["table_size_bytes"] = int(t.num_bytes or 0)
                    rows_out["created"]         = t.created.isoformat() if t.created else None
                    rows_out["modified"]        = t.modified.isoformat() if t.modified else None
                    rows_out["columns"]         = [s.name for s in (t.schema or [])]
                except Exception as e:
                    context.log.warning(f"could not fetch destination table metadata: {e}")

            df = pd.DataFrame([rows_out])
            return Output(
                value=df,
                metadata={
                    "destination_table_id":  MetadataValue.text(destination_table_id),
                    "materialization":       MetadataValue.text(materialization),
                    "row_count":             MetadataValue.int(int(rows_out.get("row_count", 0))),
                    "table_size_bytes":      MetadataValue.int(int(rows_out.get("table_size_bytes", 0))),
                    "bytes_billed":          MetadataValue.int(rows_out["bytes_billed"]),
                    "rendered_ddl":          MetadataValue.text(ddl),
                    "preview":               MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
