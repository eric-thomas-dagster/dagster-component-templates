"""GcsObjectResourceComponent -- object-level GCS operations (upload, download, list, delete,
exists) as a reusable resource, injectable into any op or asset. `gcs_resource` only registers
the official `dagster-gcp` `GCSResource` (a raw client); `gcs_bucket_resource` only manages
bucket lifecycle. This fills the gap: working with individual objects inside a bucket.
"""
import os
from typing import List, Optional, Union

import dagster as dg
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


class GcsObjectResource(dg.ConfigurableResource):
    """A GCS object-level client, injectable into any op or asset.

    ```python
    @dg.asset
    def my_asset(gcs_object_resource: GcsObjectResource):
        gcs_object_resource.upload("my-bucket", "path/to/obj.json", b'{"a": 1}')
        data = gcs_object_resource.download("my-bucket", "path/to/obj.json")
    ```
    """

    credentials_env_var: Optional[str] = None

    def _client(self):
        from google.cloud import storage

        if self.credentials_env_var:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(self.credentials_env_var))
        return storage.Client()

    def upload(
        self,
        bucket_name: str,
        blob_name: str,
        data: Union[str, bytes],
        content_type: Optional[str] = None,
    ) -> None:
        """Upload raw string/bytes content to an object (overwrites if it already exists)."""
        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.upload_from_string(data, content_type=content_type or "application/octet-stream")

    def upload_file(
        self,
        bucket_name: str,
        blob_name: str,
        local_path: str,
        content_type: Optional[str] = None,
    ) -> None:
        """Upload a local file's contents to an object (overwrites if it already exists)."""
        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.upload_from_filename(local_path, content_type=content_type)

    def download(self, bucket_name: str, blob_name: str) -> bytes:
        """Download an object's content as bytes."""
        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        return blob.download_as_bytes()

    def download_to_file(self, bucket_name: str, blob_name: str, local_path: str) -> None:
        """Download an object's content directly to a local file path."""
        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.download_to_filename(local_path)

    def list_objects(self, bucket_name: str, prefix: Optional[str] = None) -> List[str]:
        """List object names in a bucket, optionally filtered by prefix."""
        client = self._client()
        return [blob.name for blob in client.list_blobs(bucket_name, prefix=prefix)]

    def delete(self, bucket_name: str, blob_name: str, missing_ok: bool = True) -> bool:
        """Delete an object. Idempotent by default: deleting an already-absent object is a
        no-op (returns False) rather than raising, unless missing_ok=False.

        Returns True if an object was actually deleted, False if it didn't exist.
        """
        from google.api_core.exceptions import NotFound

        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        try:
            blob.delete()
            return True
        except NotFound:
            if missing_ok:
                return False
            raise

    def exists(self, bucket_name: str, blob_name: str) -> bool:
        """Whether an object exists."""
        client = self._client()
        blob = client.bucket(bucket_name).blob(blob_name)
        return blob.exists()


class GcsObjectResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GcsObjectResource (upload/download/list/delete/exists for objects within a
    bucket) for use by other components.

    ```yaml
    type: dagster_community_components.GcsObjectResourceComponent
    attributes:
      resource_key: gcs_object_resource
      credentials_env_var: GCP_SERVICE_ACCOUNT_JSON_PATH
    ```
    """

    resource_key: str = Field(default="gcs_object_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a path to a GCP service account JSON key file. Unset uses application-default credentials (same convention as gcs_bucket_resource's credentials_env_var).",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="GCS Object Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GcsObjectResource(credentials_env_var=self.credentials_env_var)
        return dg.Definitions(resources={self.resource_key: resource})
