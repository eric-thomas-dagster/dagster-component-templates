"""AzureBlobParquetIOManager.

ConfigurableIOManager that writes pandas DataFrames as Parquet to an Azure Blob Storage container. Asset path becomes the blob path; supports partitioned assets.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from azure.storage.blob import BlobServiceClient
from io import BytesIO


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class AzureBlobParquetIOManager(dg.ConfigurableIOManager):
    """Store DataFrames as Parquet on Azure Blob Storage."""

    account_name: str = Field(description="Azure storage account name.")
    container: str = Field(description="Blob container.")
    prefix: str = Field(default="dagster/assets", description="Blob key prefix.")
    connection_string: Optional[str] = Field(default=None, description="Optional connection string (else uses DefaultAzureCredential).")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"AzureBlobParquetIOManager only handles DataFrames; got {type(obj)}")
        client = (BlobServiceClient.from_connection_string(self.connection_string) if self.connection_string
                  else BlobServiceClient(f"https://{self.account_name}.blob.core.windows.net"))
        key = f"{self.prefix}/{self._table_name(context).replace('_', '/')}.parquet"
        buf = BytesIO()
        obj.to_parquet(buf, index=False)
        buf.seek(0)
        client.get_blob_client(container=self.container, blob=key).upload_blob(buf.getvalue(), overwrite=True)
        context.add_output_metadata({"blob": dg.MetadataValue.text(f"{self.container}/{key}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        client = (BlobServiceClient.from_connection_string(self.connection_string) if self.connection_string
                  else BlobServiceClient(f"https://{self.account_name}.blob.core.windows.net"))
        up = context.upstream_output
        # Reconstruct the same key the writer produced
        parts = list(up.asset_key.path) + ([up.asset_partition_key] if up.has_asset_partitions else [])
        key = f"{self.prefix}/" + "/".join(_sanitize(p) for p in parts) + ".parquet"
        stream = client.get_blob_client(container=self.container, blob=key).download_blob()
        return pd.read_parquet(BytesIO(stream.readall()))
