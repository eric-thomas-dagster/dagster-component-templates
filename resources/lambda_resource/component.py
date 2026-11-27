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
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


def _parse_connection_json(value: str) -> Dict[str, Optional[str]]:
    """Parse a JSON blob ('{"region_name": ..., "aws_access_key_id": ..., ...}') for AWS
    credentials. AWS access keys/secrets routinely contain characters needing care to
    URL-encode correctly, and boto3's own credential surface (region, profile, key id,
    secret, session token) doesn't map onto a single scheme://login:password@host URI
    the way a database or HTTP connection does, so this resource accepts a JSON blob
    instead of the Airflow-style connection URI the other resources in this repo use."""
    data = json.loads(value)
    return {
        "region_name": data.get("region_name"),
        "profile_name": data.get("profile_name"),
        "aws_access_key_id": data.get("aws_access_key_id"),
        "aws_secret_access_key": data.get("aws_secret_access_key"),
        "aws_session_token": data.get("aws_session_token"),
    }


def _encode_payload(payload: Optional[Union[Dict[str, Any], List[Any], str, bytes, bytearray]]) -> bytes:
    """Encode an invoke() payload for the real boto3 `Payload` (blob) argument.

    dict/list -> JSON-encoded, matching the common "event payload" shape. str
    is sent as-is (utf-8 encoded) without JSON-wrapping, since some Lambda
    functions expect a raw string body rather than a JSON-encoded string.
    bytes/bytearray are passed through unchanged.
    """
    if payload is None:
        return json.dumps({}).encode("utf-8")
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload).encode("utf-8")


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
        payload: Optional[Union[Dict[str, Any], List[Any], str, bytes, bytearray]] = None,
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
            "Payload": _encode_payload(payload),
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
    connection_uri_env_var: Optional[str] = Field(default=None, description="Env var holding a JSON blob ('{\"region_name\": ..., \"aws_access_key_id\": ..., \"aws_secret_access_key\": ..., \"aws_session_token\": ..., \"profile_name\": ...}') as an alternative to the individual fields above. AWS credentials don't map onto an Airflow-style connection URI the way other resources' do, so this is JSON rather than a URI. Explicit fields, if also set, win over the blob's corresponding value.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Lambda Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        region_name = self.region_name
        profile_name = self.profile_name
        access_key_id: Any = dg.EnvVar(self.aws_access_key_id_env_var) if self.aws_access_key_id_env_var else None
        secret_access_key: Any = dg.EnvVar(self.aws_secret_access_key_env_var) if self.aws_secret_access_key_env_var else None
        session_token: Any = dg.EnvVar(self.aws_session_token_env_var) if self.aws_session_token_env_var else None
        if self.connection_uri_env_var:
            blob_value = dg.EnvVar(self.connection_uri_env_var).get_value()
            if blob_value:
                parsed = _parse_connection_json(blob_value)
                if not region_name and parsed["region_name"]:
                    region_name = parsed["region_name"]
                if not profile_name and parsed["profile_name"]:
                    profile_name = parsed["profile_name"]
                if access_key_id is None and parsed["aws_access_key_id"]:
                    access_key_id = parsed["aws_access_key_id"]
                if secret_access_key is None and parsed["aws_secret_access_key"]:
                    secret_access_key = parsed["aws_secret_access_key"]
                if session_token is None and parsed["aws_session_token"]:
                    session_token = parsed["aws_session_token"]

        resource = LambdaResource(
            region_name=region_name,
            profile_name=profile_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )
        return dg.Definitions(resources={self.resource_key: resource})
