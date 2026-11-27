"""GcsBucketResourceComponent -- Airflow-equivalent bucket lifecycle management (the
`GCSCreateBucketOperator`/`GCSDeleteBucketOperator` pair) as a reusable resource other
components' ops/assets can call, rather than a dedicated asset.

ensure_bucket() creates the bucket if missing and is a no-op if it already exists and is
owned by the caller. create_bucket() expects the bucket NOT to already exist and raises
(google.api_core.exceptions.Conflict) if it does. delete_bucket() is idempotent -- deleting
an already-absent bucket succeeds; deleting a non-empty bucket requires force=True
(destructive, off by default).
"""
from typing import Optional

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


class GcsBucketResource(dg.ConfigurableResource):
    """A GCS bucket-lifecycle client, injectable into any op or asset.

    ```python
    @dg.asset
    def my_asset(gcs_bucket_resource: GcsBucketResource):
        gcs_bucket_resource.ensure_bucket("my-bucket", location="US")
    ```
    """

    credentials_env_var: Optional[str] = None

    def _client(self):
        import os

        from google.cloud import storage

        if self.credentials_env_var:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(self.credentials_env_var))
        return storage.Client()

    def create_bucket(self, bucket_name: str, location: str = "US", storage_class: str = "STANDARD", project: Optional[str] = None) -> None:
        """Create a bucket. Raises google.api_core.exceptions.Conflict if it already exists.

        project overrides which GCP project owns/bills the bucket -- useful when the
        client's default project (from credentials) differs from where the bucket should
        live. Omit to use the client's default project.
        """
        client = self._client()
        bucket = client.bucket(bucket_name)
        bucket.storage_class = storage_class
        client.create_bucket(bucket, location=location, project=project)

    def ensure_bucket(self, bucket_name: str, location: str = "US", storage_class: str = "STANDARD", project: Optional[str] = None) -> dict:
        """Idempotent create: a no-op if the bucket already exists and is owned by the caller.

        project overrides which GCP project owns/bills the bucket (see create_bucket).
        Returns {"already_existed": bool}.
        """
        from google.api_core.exceptions import Conflict

        client = self._client()
        bucket = client.bucket(bucket_name)
        bucket.storage_class = storage_class
        try:
            client.create_bucket(bucket, location=location, project=project)
            return {"already_existed": False}
        except Conflict:
            # GCS's 409 Conflict doesn't distinguish "exists and it's yours" from "someone
            # else owns this globally-unique name" -- get_bucket() does (raises Forbidden/
            # NotFound if it isn't actually ours), so this only swallows the former.
            client.get_bucket(bucket_name)
            return {"already_existed": True}

    def delete_bucket(self, bucket_name: str, force: bool = False) -> dict:
        """Idempotent delete: deleting an already-absent bucket succeeds.

        force=True empties the bucket first (not recommended by Google for buckets with
        more than ~256 objects; for larger buckets, empty it out-of-band first).

        Returns {"existed": bool, "objects_deleted": int}.
        """
        from google.api_core.exceptions import NotFound

        client = self._client()
        deleted_objects = 0
        try:
            bucket = client.get_bucket(bucket_name)
            if force:
                blobs = list(client.list_blobs(bucket_name))
                deleted_objects = len(blobs)
                for blob in blobs:
                    blob.delete()
            bucket.delete(force=False)
            return {"existed": True, "objects_deleted": deleted_objects}
        except NotFound:
            return {"existed": False, "objects_deleted": 0}


class GcsBucketResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GcsBucketResource (bucket create/delete/ensure-exists) for use by other components.

    ```yaml
    type: dagster_community_components.GcsBucketResourceComponent
    attributes:
      resource_key: gcs_bucket_resource
      credentials_env_var: GCP_SERVICE_ACCOUNT_JSON_PATH
    ```
    """

    resource_key: str = Field(default="gcs_bucket_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a path to a GCP service account JSON key file. Unset uses application-default credentials (same convention as gcs_to_database_asset's credentials_env_var).",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="GCS Bucket Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GcsBucketResource(credentials_env_var=self.credentials_env_var)
        return dg.Definitions(resources={self.resource_key: resource})
