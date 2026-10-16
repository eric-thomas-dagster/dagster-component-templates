"""AzureBlobParquetIOManagerComponent.

YAML/Component wrapper around `AzureBlobParquetIOManager`.
"""
from typing import Optional, Any
import dagster as dg
from pydantic import Field
import pandas as pd
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # optional dependency (see requirements.txt); a clear error is raised on first use
    BlobServiceClient = None
from io import BytesIO
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


class AzureBlobParquetIOManager(dg.ConfigurableIOManager):
    """Store DataFrames as Parquet on Azure Blob Storage.

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition object. Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    account_name: str = Field(description="Azure storage account name.")
    container: str = Field(description="Blob container.")
    prefix: str = Field(default="dagster/assets", description="Blob key prefix.")
    connection_string: Optional[str] = Field(default=None, description="Optional connection string (else uses DefaultAzureCredential).")

    def _client(self):
        if BlobServiceClient is None:
            raise ImportError("azure-storage-blob is required for AzureBlobParquetIOManager: pip install azure-storage-blob")
        return (BlobServiceClient.from_connection_string(self.connection_string) if self.connection_string
                else BlobServiceClient(f"https://{self.account_name}.blob.core.windows.net"))

    def _blob_key(self, context, partition_key=None) -> str:
        """Blob key: <prefix>/<asset path parts>[/<partition_key>].parquet (same layout for write and read)."""
        parts = list(context.asset_key.path)
        if partition_key is not None:
            parts.append(partition_key)
        elif context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return f"{self.prefix}/" + "/".join(_sanitize(str(p)) for p in parts) + ".parquet"

    def _upload(self, client, key: str, df: pd.DataFrame) -> None:
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        client.get_blob_client(container=self.container, blob=key).upload_blob(buf.getvalue(), overwrite=True)

    def _download(self, client, key: str) -> pd.DataFrame:
        stream = client.get_blob_client(container=self.container, blob=key).download_blob()
        return pd.read_parquet(BytesIO(stream.readall()))

    def handle_output(self, context, obj) -> None:
        if _is_partition_range(context):
            items = _range_items(context, obj, "AzureBlobParquetIOManager")
            client = self._client()
            blobs = []
            for pkey, df in items:
                key = self._blob_key(context, pkey)
                self._upload(client, key, df)
                blobs.append(f"{self.container}/{key}")
            context.add_output_metadata({
                "blobs": dg.MetadataValue.json(blobs),
                "partition_key": dg.MetadataValue.text(_range_label(context)),
                "row_count": dg.MetadataValue.int(sum(len(df) for _, df in items)),
            })
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"AzureBlobParquetIOManager only handles DataFrames; got {type(obj)}")
        client = self._client()
        key = self._blob_key(context)
        self._upload(client, key, obj)
        context.add_output_metadata({"blob": dg.MetadataValue.text(f"{self.container}/{key}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        client = self._client()
        up = context.upstream_output
        if _is_partition_range(up):
            return {pkey: self._download(client, self._blob_key(up, pkey)) for pkey in up.asset_partition_keys}
        return self._download(client, self._blob_key(up))


class AzureBlobParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Store DataFrames as Parquet on Azure Blob Storage.

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
    account_name: str = Field(description="Azure storage account name.")
    container: str = Field(description="Blob container.")
    prefix: str = Field(default="dagster/assets", description="Blob key prefix.")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with the connection string (else uses DefaultAzureCredential).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = AzureBlobParquetIOManager(
            account_name=self.account_name,
            container=self.container,
            prefix=self.prefix,
            connection_string=dg.EnvVar(self.connection_string_env_var).get_value() if self.connection_string_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
