"""GCS to Database Asset Component.

Reads a file from Google Cloud Storage and writes it to a database table
via SQLAlchemy. Designed to be triggered by gcs_monitor.

Mirrors s3_to_database_asset — same format support, same destination config.
"""
from typing import Any, Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field



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

    # Both shapes set: ambiguous. Pick one.
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


class GCSToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read a file from GCS and write it to a database table.

    Triggered by gcs_monitor passing bucket/object_name via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.GCSToDatabaseAssetComponent
        attributes:
          asset_name: gcs_events_ingest
          credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          file_format: parquet
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to GCP service account JSON path. If unset, uses Application Default Credentials."
    )
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    file_format: str = Field(default="auto", description="csv, json, parquet, or auto")
    csv_delimiter: str = Field(default=",", description="CSV delimiter")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, monthly, hourly, static, multi, or dynamic")
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the written rows in the materialization metadata.",
    )

    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = _build_partitions_def(
            _self.partition_type if _self.partition_type != "none" else None,
            _self.partition_start or _self.partition_start_date,
            _self.partition_values,
            _self.dynamic_partition_name,
            _self.partition_dimensions,
        )

        class GCSObjectConfig(Config):
            bucket_name: str
            object_name: str
            object_size: Optional[int] = None

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"GCS → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"gcs", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def gcs_to_database_asset(context: AssetExecutionContext, config: GCSObjectConfig):
            import os
            from io import BytesIO
            import pandas as pd
            from google.cloud import storage
            from sqlalchemy import create_engine

            if _self.credentials_env_var:
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", os.environ[_self.credentials_env_var])

            db_url = os.environ[_self.database_url_env_var]

            object_name = config.object_name
            if context.has_partition_key:
                object_name = object_name.replace("{partition_key}", context.partition_key)

            context.log.info(f"Downloading gs://{config.bucket_name}/{object_name}")
            gcs_client = storage.Client()
            blob = gcs_client.bucket(config.bucket_name).blob(object_name)
            content = blob.download_as_bytes()

            fmt = _self.file_format
            if fmt == "auto":
                name = object_name.lower()
                fmt = "parquet" if name.endswith(".parquet") else "json" if name.endswith(".json") else "csv"

            if fmt == "parquet":
                df = pd.read_parquet(BytesIO(content))
            elif fmt == "json":
                df = pd.read_json(BytesIO(content), orient="records")
            else:
                df = pd.read_csv(BytesIO(content), delimiter=_self.csv_delimiter)

            context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            if _self.column_mapping:
                df = df.rename(columns=_self.column_mapping)

            table_name = _self.table_name
            if context.has_partition_key:
                table_name = table_name.replace("{partition_key}", context.partition_key)

            engine = create_engine(db_url)
            df.to_sql(table_name, con=engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=1000)

            _preview_metadata = {}
            if _self.include_preview_metadata:
                _prev_df = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                _cols = list(_prev_df.columns)
                _preview_metadata["preview"] = dg.MetadataValue.md(
                    "| " + " | ".join(_cols) + " |\n"
                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +
                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))
                )


            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={**_preview_metadata, 
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "bucket": config.bucket_name,
                "object": object_name,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
                "file_format": fmt,
            })

        return dg.Definitions(assets=[gcs_to_database_asset])
