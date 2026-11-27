"""S3BucketResourceComponent -- Airflow-equivalent bucket lifecycle management (the
`S3CreateBucketOperator`/`S3DeleteBucketOperator` pair) as a reusable resource other
components' ops/assets can call, rather than a dedicated asset.

ensure_bucket() creates the bucket if missing and is a no-op if it already exists and is
owned by the caller. create_bucket() expects the bucket NOT to already exist and raises
(botocore.exceptions.ClientError) if it does. delete_bucket() is idempotent -- deleting an
already-absent bucket succeeds; deleting a non-empty bucket requires force=True
(destructive, off by default).
"""
from typing import Optional

import dagster as dg
from pydantic import Field


class S3BucketResource(dg.ConfigurableResource):
    """An S3 bucket-lifecycle client, injectable into any op or asset.

    ```python
    @dg.asset
    def my_asset(s3_bucket_resource: S3BucketResource):
        s3_bucket_resource.ensure_bucket("my-bucket", region="us-west-2")
    ```
    """

    region_name: Optional[str] = None
    profile_name: Optional[str] = None
    aws_access_key_id_env_var: Optional[str] = None
    aws_secret_access_key_env_var: Optional[str] = None
    aws_session_token_env_var: Optional[str] = None
    endpoint_url: Optional[str] = None

    def _client(self, region_override: Optional[str] = None):
        import boto3

        kwargs: dict = {}
        region = region_override or self.region_name
        if region:
            kwargs["region_name"] = region
        if self.profile_name:
            kwargs["profile_name"] = self.profile_name
        if self.aws_access_key_id_env_var:
            kwargs["aws_access_key_id"] = dg.EnvVar(self.aws_access_key_id_env_var).get_value()
        if self.aws_secret_access_key_env_var:
            kwargs["aws_secret_access_key"] = dg.EnvVar(self.aws_secret_access_key_env_var).get_value()
        if self.aws_session_token_env_var:
            kwargs["aws_session_token"] = dg.EnvVar(self.aws_session_token_env_var).get_value()
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return boto3.client("s3", **kwargs)

    def _exists(self, client, bucket_name: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket"):
                return False
            raise

    def _create(self, client, bucket_name: str, region: Optional[str]) -> None:
        """boto3's create_bucket is the one API where us-east-1 is special-cased: passing a
        LocationConstraint for it is a real, commonly-hit error (IllegalLocationConstraintException)."""
        if region and region != "us-east-1":
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            client.create_bucket(Bucket=bucket_name)

    def _empty_bucket(self, client, bucket_name: str) -> int:
        """Delete every object (and, if versioning is/was ever enabled, every version and delete
        marker) so delete_bucket won't fail with BucketNotEmpty. Used only when force=True."""
        deleted = 0
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            to_delete = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", [])]
            to_delete += [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("DeleteMarkers", [])]
            if to_delete:
                client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
                deleted += len(to_delete)
        return deleted

    def create_bucket(self, bucket_name: str, region: Optional[str] = None) -> None:
        """Create a bucket. Raises botocore.exceptions.ClientError if it already exists."""
        client = self._client(region)
        self._create(client, bucket_name, region or self.region_name)

    def ensure_bucket(self, bucket_name: str, region: Optional[str] = None) -> dict:
        """Idempotent create: a no-op if the bucket already exists and is owned by the caller.

        Returns {"already_existed": bool}.
        """
        from botocore.exceptions import ClientError

        client = self._client(region)
        effective_region = region or self.region_name
        try:
            self._create(client, bucket_name, effective_region)
            return {"already_existed": False}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "BucketAlreadyOwnedByYou":
                return {"already_existed": True}
            if code == "BucketAlreadyExists" and self._exists(client, bucket_name):
                # us-east-1 legacy behavior: re-creating a bucket you own there can surface this
                # code instead of BucketAlreadyOwnedByYou. head_bucket confirms it's actually ours.
                return {"already_existed": True}
            raise

    def delete_bucket(self, bucket_name: str, force: bool = False, region: Optional[str] = None) -> dict:
        """Idempotent delete: deleting an already-absent bucket succeeds.

        force=True empties every object/version/delete-marker first -- destructive and irreversible.

        Returns {"existed": bool, "objects_deleted": int}.
        """
        client = self._client(region)
        existed = self._exists(client, bucket_name)
        deleted_objects = 0
        if existed:
            if force:
                deleted_objects = self._empty_bucket(client, bucket_name)
            client.delete_bucket(Bucket=bucket_name)
        return {"existed": existed, "objects_deleted": deleted_objects}


class S3BucketResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an S3BucketResource (bucket create/delete/ensure-exists) for use by other components.

    ```yaml
    type: dagster_community_components.S3BucketResourceComponent
    attributes:
      resource_key: s3_bucket_resource
      region_name: us-west-2
    ```
    """

    resource_key: str = Field(default="s3_bucket_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'. Can be overridden per-call.")
    profile_name: Optional[str] = Field(default=None, description="Named AWS CLI/SDK profile (from ~/.aws/credentials) to source credentials from.")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS access key ID")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS secret access key")
    aws_session_token_env_var: Optional[str] = Field(default=None, description="Environment variable holding a temporary AWS session token")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL for MinIO or LocalStack, e.g. 'http://localhost:9000'")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="S3 Bucket Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = S3BucketResource(
            region_name=self.region_name,
            profile_name=self.profile_name,
            aws_access_key_id_env_var=self.aws_access_key_id_env_var,
            aws_secret_access_key_env_var=self.aws_secret_access_key_env_var,
            aws_session_token_env_var=self.aws_session_token_env_var,
            endpoint_url=self.endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
