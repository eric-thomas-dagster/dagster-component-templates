"""Dataframe → Avro sink component.

Companion to `dataframe_to_parquet`. Writes a pandas DataFrame as Avro to
a local path or any fsspec-supported URI (s3://, gs://, abfss://, ...).
Schema is inferred from pandas dtypes; nullable types become Avro unions
with `null`. For producers that need explicit schema control, pass
`avro_schema:` as YAML and we'll skip inference and use what you give us.
"""
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey, MetadataValue, Output, asset
from pydantic import Field


# Pandas dtype → Avro type (nullable variants use union with "null").
_PANDAS_TO_AVRO = {
    "int8":    "int",
    "int16":   "int",
    "int32":   "int",
    "int64":   "long",
    "uint8":   "int",
    "uint16":  "int",
    "uint32":  "long",
    "uint64":  "long",
    "float32": "float",
    "float64": "double",
    "bool":    "boolean",
    "object":  "string",
    "string":  "string",
    "category": "string",
}


def _infer_avro_schema(df: pd.DataFrame, record_name: str) -> Dict[str, Any]:
    """Build a record-type Avro schema by mapping each column's dtype.

    All fields are emitted as nullable Avro unions (`["null", T]`) so the
    schema works even when pandas has NaNs. Caller can override with an
    explicit `avro_schema:` if they need stricter typing or named records.
    """
    fields: List[Dict[str, Any]] = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype.startswith("datetime64"):
            # Emit as ISO-8601 strings for cross-engine portability.
            avro_type = "string"
        else:
            avro_type = _PANDAS_TO_AVRO.get(dtype, "string")
        fields.append({"name": str(col), "type": ["null", avro_type], "default": None})
    return {
        "type": "record",
        "name": record_name,
        "fields": fields,
    }


def _coerce_for_avro(records: List[Dict[str, Any]]) -> None:
    """In-place: convert pandas Timestamp / NaT / numpy types to plain Python
    types so fastavro can serialize."""
    import math
    for row in records:
        for k, v in list(row.items()):
            # NaN → None for nullable unions
            if isinstance(v, float) and math.isnan(v):
                row[k] = None
                continue
            # pandas.Timestamp / numpy.datetime64 → ISO string
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
                continue
            # numpy scalars → native Python
            if hasattr(v, "item") and not isinstance(v, (str, bytes, dict, list)):
                try:
                    row[k] = v.item()
                except (ValueError, AttributeError):
                    pass


def _resolve_path(file_path: str, context: AssetExecutionContext) -> str:
    """Substitute env vars + {partition_key} into the destination path."""
    out = os.path.expandvars(file_path)
    if context.has_partition_key:
        out = out.replace("{partition_key}", str(context.partition_key))
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    if not partition_type:
        return None
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01T00:00:00")
    if partition_type == "static":
        vals = partition_values or []
        if isinstance(vals, str):
            vals = [v.strip() for v in vals.split(",") if v.strip()]
        return StaticPartitionsDefinition(list(vals))
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type=dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DataframeToAvroComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write a pandas DataFrame as Avro to a local path or fsspec URI.

    Schema is inferred from pandas dtypes by default (all columns emitted
    as nullable Avro unions). Pass `avro_schema:` as a YAML dict to
    override inference with an explicit producer schema.
    """

    asset_name: str = Field(description="Output asset name")
    upstream_asset_key: str = Field(description="Upstream asset key (must materialize a pandas DataFrame)")

    file_path: str = Field(
        description=(
            "Destination path or fsspec URI. Local (`/tmp/out.avro`) and remote "
            "URIs (`s3://...`, `gs://...`, `abfss://...`) both work via fsspec. "
            "`{partition_key}` is substituted from the asset's partition key when "
            "the asset is partitioned. Supports `${ENV_VAR}` substitution."
        )
    )
    codec: str = Field(
        default="null",
        description="Avro compression codec: null | snappy | deflate | bzip2 | xz | zstandard",
    )
    record_name: str = Field(
        default="Record",
        description="Name embedded in the inferred Avro schema's `name` field. Ignored if `avro_schema:` is set.",
    )
    avro_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional explicit Avro schema dict (overrides inference). Use when "
            "the downstream consumer needs a specific schema name / namespace / "
            "non-nullable typing. Example: "
            "`{type: record, name: Event, namespace: com.acme, fields: [...]}`"
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None, description="daily | weekly | monthly | hourly | static | dynamic | None")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        file_path = self.file_path
        codec = self.codec
        record_name = self.record_name
        explicit_schema = self.avro_schema
        description = self.description or f"Write upstream DataFrame as Avro to {file_path}"
        group_name = self.group_name
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        )

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=deps_keys,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            import fastavro
            import fsspec

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(f"Expected pandas DataFrame, got {type(upstream).__name__}")

            resolved = _resolve_path(file_path, context)
            schema = explicit_schema or _infer_avro_schema(upstream, record_name)
            records = upstream.to_dict(orient="records")
            _coerce_for_avro(records)

            with fsspec.open(resolved, "wb") as f:
                fastavro.writer(f, schema, records, codec=codec)

            context.log.info(f"Wrote {len(records)} rows to {resolved} (Avro, codec={codec})")

            return Output(
                value=None,
                metadata={
                    "destination":   MetadataValue.text(resolved),
                    "row_count":     MetadataValue.int(len(records)),
                    "column_count":  MetadataValue.int(len(upstream.columns)),
                    "codec":         MetadataValue.text(codec),
                    "schema_inferred": MetadataValue.bool(explicit_schema is None),
                },
            )

        return dg.Definitions(assets=[_asset])
