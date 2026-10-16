"""S3CsvIOManagerComponent.

YAML/Component wrapper around `S3CsvIOManager`.
"""
from typing import Optional, Any
import dagster as dg
from pydantic import Field
import pandas as pd
import boto3
from io import StringIO
from collections.abc import Mapping


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


def _is_partition_range(context) -> bool:
    """True when the context covers a partition RANGE (BackfillPolicy.single_run):
    it has asset partitions but no single partition_key."""
    return bool(context.has_asset_partitions) and not context.has_partition_key


def _range_label(context) -> str:
    keys = list(context.asset_partition_keys)
    return f"{keys[0]}...{keys[-1]}" if len(keys) > 1 else str(keys[0])


def _range_items(context, obj, who: str):
    """Validate a range write: obj must be {partition_key: DataFrame} for exactly the range's keys."""
    keys = list(context.asset_partition_keys)
    if not isinstance(obj, Mapping):
        raise TypeError(
            f"{who}: this output covers a partition range ({_range_label(context)}), e.g. a "
            f"BackfillPolicy.single_run() backfill. A multi-partition output must be a dict "
            f"{{partition_key: DataFrame}} with one entry per partition key {keys}; got {type(obj).__name__}."
        )
    missing = [k for k in keys if k not in obj]
    extra = [k for k in obj if k not in keys]
    if missing or extra:
        raise ValueError(
            f"{who}: dict keys must equal the partition keys of the range. "
            f"Missing: {missing}; unexpected: {extra}."
        )
    for k in keys:
        if not isinstance(obj[k], pd.DataFrame):
            raise TypeError(f"{who}: value for partition {k!r} must be a DataFrame; got {type(obj[k]).__name__}.")
    return [(k, obj[k]) for k in keys]


class S3CsvIOManager(dg.ConfigurableIOManager):
    """Store DataFrames as CSV files on Amazon S3 (or any S3-compatible endpoint).

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition object. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    bucket: str = Field(description="S3 bucket.")
    prefix: str = Field(default="dagster/assets", description="Key prefix.")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    aws_access_key_id: Optional[str] = Field(default=None, description="Access key (resolved from env via dg.EnvVar).")
    aws_secret_access_key: Optional[str] = Field(default=None, description="Secret key.")
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint (MinIO, LocalStack).")

    def _client(self):
        return boto3.client("s3", region_name=self.region_name, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, endpoint_url=self.endpoint_url)

    def _key(self, context, partition_key=None) -> str:
        parts = list(context.asset_key.path)
        if partition_key is not None:
            parts.append(partition_key)
        elif context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return f"{self.prefix}/" + "/".join(_sanitize(str(p)) for p in parts) + ".csv"

    def _put(self, s3, key: str, df: pd.DataFrame) -> None:
        buf = StringIO()
        df.to_csv(buf, index=False)
        s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue().encode())

    def _get(self, s3, key: str) -> pd.DataFrame:
        body = s3.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()
        return pd.read_csv(StringIO(body))

    def handle_output(self, context, obj) -> None:
        if _is_partition_range(context):
            items = _range_items(context, obj, "S3CsvIOManager")
            s3 = self._client()
            uris = []
            for pkey, df in items:
                key = self._key(context, pkey)
                self._put(s3, key, df)
                uris.append(f"s3://{self.bucket}/{key}")
            context.add_output_metadata({
                "s3_uris": dg.MetadataValue.json(uris),
                "partition_key": dg.MetadataValue.text(_range_label(context)),
                "row_count": dg.MetadataValue.int(sum(len(df) for _, df in items)),
            })
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"S3CsvIOManager only handles DataFrames; got {type(obj)}")
        s3 = self._client()
        key = self._key(context)
        self._put(s3, key, obj)
        context.add_output_metadata({"s3_uri": dg.MetadataValue.text(f"s3://{self.bucket}/{key}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        s3 = self._client()
        up = context.upstream_output
        if _is_partition_range(up):
            return {pkey: self._get(s3, self._key(up, pkey)) for pkey in up.asset_partition_keys}
        return self._get(s3, self._key(up))


class S3CsvIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as CSV files on Amazon S3 (or any S3-compatible endpoint).

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition object. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    bucket: str = Field(description="S3 bucket.")
    prefix: str = Field(default="dagster/assets", description="Key prefix.")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Env var for access key.")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Env var for secret key.")
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = S3CsvIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            region_name=self.region_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_env_var).get_value() if self.aws_access_key_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_key_env_var).get_value() if self.aws_secret_key_env_var else None,
            endpoint_url=self.endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
