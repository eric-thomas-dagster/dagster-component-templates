"""DataframeToSecurityLakeComponent.

Writes an OCSF DataFrame to AWS Security Lake — Parquet with snappy compression,
canonical Hive partitioning (`region={r}/accountId={a}/eventDay={YYYYMMDD}`),
optional Glue catalog registration.

Security Lake is OCSF-native: any row that conforms to OCSF v1.x lands cleanly
in a custom source. Use `ocsf_normalizer` upstream to map source events to
OCSF; this component writes the result.
"""

import os
import datetime as dt
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DataframeToSecurityLakeComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write an OCSF DataFrame to AWS Security Lake as partitioned Parquet."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="OCSF DataFrame to write")

    bucket: str = Field(description="Security Lake S3 bucket (e.g. 'aws-security-data-lake-us-east-1-<id>')")
    source_location: str = Field(
        description="Custom-source location/name (e.g. 'ext-dagster-plus-audit'). Becomes the top-level S3 prefix.",
    )
    region_name: str = Field(default="us-east-1", description="AWS region (also used in partition key)")
    account_id: Optional[str] = Field(default=None, description="AWS account ID — falls back to AWS_ACCOUNT_ID env if unset")
    event_day_field: str = Field(default="time", description="Field used to derive eventDay partition (epoch ms expected)")
    aws_profile: Optional[str] = Field(default=None)

    # Optional Glue catalog registration
    register_with_glue: bool = Field(default=False)
    glue_database: Optional[str] = Field(default=None)
    glue_table: Optional[str] = Field(default=None)

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_lake")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        _self = self
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Write OCSF events to AWS Security Lake",
            group_name=self.group_name,
            kinds=set(self.kinds or ["aws", "security-lake", "parquet", "ocsf"]),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            import boto3, io
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
            except ImportError:
                raise ImportError("dataframe_to_security_lake needs pyarrow — `uv add pyarrow`")

            account_id = _self.account_id or os.environ.get("AWS_ACCOUNT_ID")
            if not account_id:
                raise ValueError("account_id is required (set field or AWS_ACCOUNT_ID env var)")

            sess = boto3.Session(profile_name=_self.aws_profile, region_name=_self.region_name)
            s3 = sess.client("s3")

            # Bucket eventDay from the OCSF `time` field (epoch ms)
            if _self.event_day_field not in df.columns:
                raise ValueError(f"event_day_field '{_self.event_day_field}' not in DataFrame columns")
            df = df.copy()
            df["_event_day"] = pd.to_datetime(df[_self.event_day_field], unit="ms", errors="coerce").dt.strftime("%Y%m%d")

            written = []
            for day, chunk in df.groupby("_event_day"):
                if pd.isna(day) or day == "NaT":
                    continue
                chunk = chunk.drop(columns=["_event_day"])
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                buf = io.BytesIO()
                pq.write_table(table, buf, compression="snappy")
                buf.seek(0)
                key = (
                    f"{_self.source_location}/region={_self.region_name}/"
                    f"accountId={account_id}/eventDay={day}/"
                    f"{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.parquet"
                )
                s3.upload_fileobj(buf, _self.bucket, key)
                written.append({"key": key, "rows": int(len(chunk)), "bytes": int(buf.getbuffer().nbytes)})
                context.log.info(f"wrote s3://{_self.bucket}/{key} ({len(chunk)} rows)")

            if _self.register_with_glue and written and _self.glue_database and _self.glue_table:
                glue = sess.client("glue")
                # Best-effort table presence check; users typically pre-create the table.
                try:
                    glue.get_table(DatabaseName=_self.glue_database, Name=_self.glue_table)
                    context.log.info(f"glue table {_self.glue_database}.{_self.glue_table} exists — partitions auto-discovered by crawler")
                except glue.exceptions.EntityNotFoundException:
                    context.log.warning(
                        f"glue table {_self.glue_database}.{_self.glue_table} not found — "
                        "create it via the Security Lake console or Lake Formation"
                    )

            return dg.MaterializeResult(metadata={
                "files_written": dg.MetadataValue.int(len(written)),
                "rows_written": dg.MetadataValue.int(int(sum(w["rows"] for w in written))),
                "bytes_written": dg.MetadataValue.int(int(sum(w["bytes"] for w in written))),
                "bucket": dg.MetadataValue.text(_self.bucket),
                "source_location": dg.MetadataValue.text(_self.source_location),
                "files": dg.MetadataValue.json([w["key"] for w in written]),
            })

        return dg.Definitions(assets=[_asset])
