"""LambdaResourceComponent.

Register a reusable `LambdaResource` exposing `invoke(...)` -- the AWS
Lambda invoke action as an injectable resource method, callable from any
op/asset you write, rather than baked into a single-purpose asset
component (see `assets/infrastructure/lambda_invoke_asset` for the
asset-shaped equivalent, which wraps the same real logic).

Real boto3 Lambda API, verified against the botocore reference:
`lambda.invoke(FunctionName=..., InvocationType=..., Payload=<bytes>,
Qualifier=...)` -> `{'StatusCode', 'Payload': StreamingBody, 'FunctionError':
'Unhandled'|'Handled' (absent on success), 'LogResult': base64, ...}`.

`region_name` is optional here (unlike the older asset's required field) --
omit it to fall back to boto3's own default credential/region chain
(env var, profile config, instance metadata), matching `emr_resource`'s
convention.
"""
import base64
import json
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class LambdaResource(dg.ConfigurableResource):
    """Thin wrapper around boto3's Lambda client exposing invoke(...).

    Injectable into any op or asset you write -- not tied to a single
    pre-built asset shape.
    """

    region_name: Optional[str] = None
    profile_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None

    def _client(self):
        import boto3
        kwargs: Dict[str, Any] = {}
        if self.profile_name:
            kwargs["profile_name"] = self.profile_name
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token
        if self.region_name:
            kwargs["region_name"] = self.region_name
        session = boto3.Session(**kwargs)
        return session.client("lambda")

    def invoke(
        self,
        function_name: str,
        payload: Optional[Dict[str, Any]] = None,
        invocation_type: str = "RequestResponse",
        qualifier: Optional[str] = None,
        include_logs: bool = False,
        fail_on_function_error: bool = True,
        log=None,
    ) -> Dict[str, Any]:
        """Invoke a Lambda function and parse the response.

        `RequestResponse` (the default) invokes synchronously and parses the
        function's JSON response into `payload`. `Event` invokes
        asynchronously and fire-and-forgets -- no payload to parse.

        Raises RuntimeError on a Lambda-side error (`FunctionError` in the
        real boto3 response -- the function raised/returned an error, not a
        transport failure) unless `fail_on_function_error=False`, in which
        case the error is returned in the result dict instead.
        """
        if invocation_type not in ("RequestResponse", "Event"):
            raise ValueError(f"invocation_type must be 'RequestResponse' or 'Event', got {invocation_type!r}")

        client = self._client()
        kwargs: Dict[str, Any] = {
            "FunctionName": function_name,
            "InvocationType": invocation_type,
            "Payload": json.dumps(payload or {}).encode("utf-8"),
        }
        if qualifier:
            kwargs["Qualifier"] = qualifier
        if include_logs:
            kwargs["LogType"] = "Tail"

        response = client.invoke(**kwargs)
        status_code = response["StatusCode"]
        function_error = response.get("FunctionError")  # 'Unhandled' | 'Handled' | absent

        raw = response["Payload"].read() if "Payload" in response else b""
        response_payload: Any = None
        if raw:
            try:
                response_payload = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                response_payload = raw.decode("utf-8", errors="replace")

        log_tail = None
        if include_logs and response.get("LogResult"):
            log_tail = base64.b64decode(response["LogResult"]).decode("utf-8", errors="replace")

        if log:
            log.info(f"lambda_resource: invoked {function_name!r} (status={status_code}, function_error={function_error!r})")

        if function_error and fail_on_function_error:
            err = response_payload or {}
            message = err.get("errorMessage") if isinstance(err, dict) else str(err)
            raise RuntimeError(
                f"Lambda function {function_name!r} invocation errored ({function_error}): {message}"
            )

        return {
            "status_code": status_code,
            "function_error": function_error,
            "payload": response_payload,
            "log_tail": log_tail,
        }


class LambdaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LambdaResource (invoke an AWS Lambda function) for use by other components."""

    resource_key: str = Field(default="lambda_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'. Omit to use the default boto3 credential/region chain (env vars, profile config, instance role).")
    profile_name: Optional[str] = Field(default=None, description="Named AWS CLI/SDK profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Env var holding the AWS access key ID.")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Env var holding the AWS secret access key.")
    aws_session_token_env_var: Optional[str] = Field(default=None, description="Env var holding a temporary AWS session token, paired with the access key/secret above.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Lambda Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = LambdaResource(
            region_name=self.region_name,
            profile_name=self.profile_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_id_env_var) if self.aws_access_key_id_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_access_key_env_var) if self.aws_secret_access_key_env_var else None,
            aws_session_token=dg.EnvVar(self.aws_session_token_env_var) if self.aws_session_token_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})
