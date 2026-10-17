"""BigQueryLoadFromGcsAssetComponent — load GCS files into a BigQuery table.

Wraps BigQuery's load-job API for the lake → warehouse step at the
start of an ELT pipeline. Supports parquet, CSV, JSONL, AVRO, ORC.

Auto-detects schema by default (BQ does it natively for parquet/avro/orc;
detects column types from headers for CSV/JSON). Provide an explicit
schema for production use to lock down types and ordering.

Returns a small DataFrame with the destination table's resulting size,
row count, and bytes-loaded for downstream lineage / cost reporting.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
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


class BigQueryLoadFromGcsAssetComponent(Component, Model, Resolvable):
    """Load one or more GCS objects into a BigQuery table — the lake → warehouse step."""

    asset_name: str = Field(description="Output asset name.")
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="Billing project. Defaults to the SA's project.")
    location: Optional[str] = Field(default=None, description="BQ location. Default: auto-detect.")

    source_uris: List[str] = Field(
        description="One or more GCS URIs (glob/wildcard supported), e.g. ['gs://bucket/landing/*.parquet'].",
    )
    destination_table_id: str = Field(
        description="Fully-qualified destination, e.g. `my-project.raw.events`.",
    )
    format: Literal["parquet", "csv", "json", "newline_delimited_json", "avro", "orc"] = Field(
        default="parquet",
    )
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"] = Field(
        default="WRITE_TRUNCATE",
        description="WRITE_TRUNCATE replaces table contents; WRITE_APPEND adds rows; WRITE_EMPTY fails if table has rows.",
    )
    create_disposition: Literal["CREATE_IF_NEEDED", "CREATE_NEVER"] = Field(
        default="CREATE_IF_NEEDED",
    )

    autodetect: bool = Field(
        default=True,
        description="Let BQ auto-detect the schema. Set False + provide `schema` for production-strength loads.",
    )
    table_schema: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Explicit schema as a list of {name, type, mode?} dicts. Only used when autodetect=false. (Field is named `table_schema` rather than `schema` to avoid a Pydantic name collision.)",
    )

    # CSV-only options
    skip_leading_rows: Optional[int] = Field(default=None, description="CSV-only: skip N header rows.")
    csv_field_delimiter: Optional[str] = Field(default=None, description="CSV-only: field delimiter.")
    csv_quote: Optional[str] = Field(default=None, description="CSV-only: quote char.")
    csv_allow_quoted_newlines: bool = Field(default=False)

    # Partitioning + clustering on the destination TABLE (BigQuery time partitioning; not Dagster partitions)
    table_partition_field: Optional[str] = Field(
        default=None,
        description="BigQuery table partitioning: the date/timestamp column to partition the destination TABLE by. Unrelated to Dagster partitions.",
    )
    table_partition_type: Optional[Literal["DAY", "HOUR", "MONTH", "YEAR"]] = Field(
        default=None,
        description="BigQuery table partitioning: the granularity (DAY, HOUR, MONTH, YEAR) for table_partition_field.",
    )
    cluster_fields: Optional[List[str]] = Field(default=None, description="Columns to cluster on (max 4).")

    deps: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

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
    def _renamed_table_partition_fields(cls, data):
        """`partition_field` / `partition_type` were renamed `table_partition_*`: they configure the BigQuery TABLE's own
        partitioning, not a Dagster partition. The old names still work, with a DeprecationWarning."""
        if not isinstance(data, dict):
            return data
        data = dict(data)
        for old, new in (("partition_field", "table_partition_field"), ("partition_type", "table_partition_type")):
            if old in data:
                if new in data:
                    raise ValueError(f"set {new}, not both {old} and {new} ({old} is its old name)")
                import warnings

                warnings.warn(
                    f"`{old}` was renamed `{new}`: it configures the BigQuery table's own partitioning, not a Dagster "
                    "partition. The old name still works for now.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                data[new] = data.pop(old)
        return data

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=(), tags=('tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        source_uris = list(self.source_uris)
        destination_table_id = self.destination_table_id
        in_format = self.format
        write_disposition = self.write_disposition
        create_disposition = self.create_disposition
        autodetect = self.autodetect
        schema_def = self.schema
        skip_leading_rows = self.skip_leading_rows
        csv_field_delimiter = self.csv_field_delimiter
        csv_quote = self.csv_quote
        csv_allow_quoted_newlines = self.csv_allow_quoted_newlines
        table_partition_field = self.table_partition_field
        table_partition_type = self.table_partition_type
        cluster_fields = self.cluster_fields

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"GCS → BQ load: {destination_table_id}",
            group_name=self.group_name,
            kinds={"bigquery", "gcs", "load"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
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
            bq = bigquery.Client(credentials=sa_creds, project=project_id, location=location)

            bq_format_map = {
                "parquet":                 bigquery.SourceFormat.PARQUET,
                "csv":                     bigquery.SourceFormat.CSV,
                "json":                    bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                "newline_delimited_json":  bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                "avro":                    bigquery.SourceFormat.AVRO,
                "orc":                     bigquery.SourceFormat.ORC,
            }
            cfg = bigquery.LoadJobConfig(
                source_format=bq_format_map[in_format],
                write_disposition=write_disposition,
                create_disposition=create_disposition,
                autodetect=autodetect,
            )
            if schema_def is not None and not autodetect:
                cfg.schema = [
                    bigquery.SchemaField(
                        s["name"], s["type"], mode=s.get("mode", "NULLABLE"),
                    )
                    for s in schema_def
                ]
            if in_format == "csv":
                if skip_leading_rows is not None:
                    cfg.skip_leading_rows = skip_leading_rows
                if csv_field_delimiter:
                    cfg.field_delimiter = csv_field_delimiter
                if csv_quote is not None:
                    cfg.quote_character = csv_quote
                if csv_allow_quoted_newlines:
                    cfg.allow_quoted_newlines = True
            if table_partition_field and table_partition_type:
                cfg.time_partitioning = bigquery.TimePartitioning(
                    type_=table_partition_type,
                    field=table_partition_field,
                )
            if cluster_fields:
                cfg.clustering_fields = cluster_fields

            context.log.info(f"BQ load {source_uris} → {destination_table_id} ({in_format})")
            try:
                job = bq.load_table_from_uri(
                    source_uris=source_uris,
                    destination=destination_table_id,
                    job_config=cfg,
                )
                job.result()
            except Exception as e:
                err_str = str(e)
                if "403" in err_str and "does not have" in err_str.lower():
                    sa = creds_dict.get("client_email", "<sa>")
                    context.log.error(
                        f"Load failed: SA {sa!r} needs roles/bigquery.dataEditor on the "
                        f"destination dataset + roles/bigquery.jobUser on the project + "
                        f"roles/storage.objectViewer on the source bucket."
                    )
                raise

            # Inspect resulting table.
            t = bq.get_table(destination_table_id)
            row = {
                "destination_table_id": destination_table_id,
                "source_uris":          source_uris,
                "format":               in_format,
                "rows_loaded":          int(job.output_rows or 0),
                "table_row_count":      int(t.num_rows or 0),
                "table_size_bytes":     int(t.num_bytes or 0),
                "input_files":          int(job.input_files or 0),
                "bytes_loaded":         int(job.input_file_bytes or 0),
            }
            df = pd.DataFrame([row])
            md = {
                "destination_table_id": MetadataValue.text(destination_table_id),
                "source_uris":          MetadataValue.json(source_uris),
                "rows_loaded":          MetadataValue.int(row["rows_loaded"]),
                "table_row_count":      MetadataValue.int(row["table_row_count"]),
                "table_size_bytes":     MetadataValue.int(row["table_size_bytes"]),
                "input_files":          MetadataValue.int(row["input_files"]),
                "bytes_loaded":         MetadataValue.int(row["bytes_loaded"]),
                "preview":              MetadataValue.md(df.to_markdown(index=False) or ""),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])


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
