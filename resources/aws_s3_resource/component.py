"""AWS S3 Resource component."""
from typing import Optional
import dagster as dg
from pydantic import Field


class S3ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-aws S3Resource for use by other components."""

    resource_key: str = Field(default="s3_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    profile_name: Optional[str] = Field(default=None, description="Named AWS CLI/SDK profile (from ~/.aws/credentials) to source credentials from. Mutually exclusive in practice with the access-key-id/secret env vars and role_arn -- whichever credential source is set, boto3's own resolution order applies.")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS access key ID")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS secret access key")
    aws_session_token_env_var: Optional[str] = Field(default=None, description="Environment variable holding a temporary AWS session token (paired with the access key/secret above for STS-issued temporary credentials you already have -- for assuming a role yourself, use role_arn instead).")
    role_arn: Optional[str] = Field(
        default=None,
        description=(
            "ARN of an IAM role to assume via STS (dagster-aws's S3Resource has no native "
            "assume-role support -- this component calls sts:AssumeRole itself and passes the "
            "resulting temporary credentials to S3Resource). IMPORTANT: the role is assumed ONCE, "
            "when this code location loads, not per-materialization -- the temporary credentials "
            "are good for role_duration_seconds (default 3600s/1h). A long-lived code server "
            "process may need to reload before they expire. Not a continuously-refreshing "
            "credential provider; for that, assume the role outside Dagster and set the access "
            "key/secret/session-token env vars above instead."
        ),
    )
    role_session_name: str = Field(default="dagster-community-components", description="RoleSessionName for the assumed-role session. Only used when role_arn is set.")
    role_external_id: Optional[str] = Field(default=None, description="ExternalId for the assumed-role call, if the role's trust policy requires one. Only used when role_arn is set.")
    role_duration_seconds: int = Field(default=3600, description="Requested STS session duration in seconds for role_arn (default 3600/1h; up to 43200/12h if the role's maximum session duration allows it).")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL for MinIO or LocalStack, e.g. 'http://localhost:9000'")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='S3 Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_aws.s3 import S3Resource
        kwargs: dict = {}
        if self.region_name:
            kwargs["region_name"] = self.region_name
        if self.profile_name:
            kwargs["profile_name"] = self.profile_name
        if self.role_arn:
            import boto3
            sts = boto3.client("sts", region_name=self.region_name)
            assume_kwargs = {
                "RoleArn": self.role_arn,
                "RoleSessionName": self.role_session_name,
                "DurationSeconds": self.role_duration_seconds,
            }
            if self.role_external_id:
                assume_kwargs["ExternalId"] = self.role_external_id
            creds = sts.assume_role(**assume_kwargs)["Credentials"]
            kwargs["aws_access_key_id"] = creds["AccessKeyId"]
            kwargs["aws_secret_access_key"] = creds["SecretAccessKey"]
            kwargs["aws_session_token"] = creds["SessionToken"]
        else:
            if self.aws_access_key_id_env_var:
                kwargs["aws_access_key_id"] = dg.EnvVar(self.aws_access_key_id_env_var)
            if self.aws_secret_access_key_env_var:
                kwargs["aws_secret_access_key"] = dg.EnvVar(self.aws_secret_access_key_env_var)
            if self.aws_session_token_env_var:
                kwargs["aws_session_token"] = dg.EnvVar(self.aws_session_token_env_var)
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        resource = S3Resource(**kwargs)
        return dg.Definitions(resources={self.resource_key: resource})
