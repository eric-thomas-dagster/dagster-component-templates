"""Real, committed tests for S3ResourceComponent -- builds the real dagster_aws.s3.S3Resource
throughout. The one external call (sts:AssumeRole) is mocked via monkeypatch, matching this
repo's "mock only the paid/external call" convention.
"""
import pytest


def _resource(mod, **attrs):
    component = mod.S3ResourceComponent(**attrs)
    defs = component.build_defs(context=None)
    return defs.resources[component.resource_key]


def test_profile_name_passes_through(mod):
    resource = _resource(mod, profile_name="my-profile")
    assert resource.profile_name == "my-profile"


def test_session_token_env_var_resolves(mod, monkeypatch):
    monkeypatch.setenv("MY_SESSION_TOKEN", "tok-abc")
    resource = _resource(mod, aws_session_token_env_var="MY_SESSION_TOKEN")
    assert resource.aws_session_token.get_value() == "tok-abc"


def test_role_arn_calls_sts_assume_role_and_wires_temporary_credentials(mod, monkeypatch):
    calls = []

    class _FakeSTS:
        def assume_role(self, **kwargs):
            calls.append(kwargs)
            return {"Credentials": {"AccessKeyId": "AKIA-TEMP", "SecretAccessKey": "sek-temp", "SessionToken": "tok-temp"}}

    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeSTS())

    resource = _resource(
        mod, role_arn="arn:aws:iam::123456789012:role/my-role", role_session_name="my-session",
        role_external_id="ext-id", role_duration_seconds=1800,
    )
    assert resource.aws_access_key_id == "AKIA-TEMP"
    assert resource.aws_secret_access_key == "sek-temp"
    assert resource.aws_session_token == "tok-temp"
    assert calls[0] == {
        "RoleArn": "arn:aws:iam::123456789012:role/my-role",
        "RoleSessionName": "my-session",
        "DurationSeconds": 1800,
        "ExternalId": "ext-id",
    }


def test_role_arn_takes_precedence_over_env_var_credentials(mod, monkeypatch):
    monkeypatch.setenv("MY_ACCESS_KEY", "should-not-be-used")

    class _FakeSTS:
        def assume_role(self, **kwargs):
            return {"Credentials": {"AccessKeyId": "AKIA-ROLE", "SecretAccessKey": "sek-role", "SessionToken": "tok-role"}}

    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeSTS())

    resource = _resource(mod, role_arn="arn:aws:iam::123456789012:role/my-role", aws_access_key_id_env_var="MY_ACCESS_KEY")
    assert resource.aws_access_key_id == "AKIA-ROLE"


def test_endpoint_url_for_minio_passes_through(mod):
    resource = _resource(mod, endpoint_url="http://localhost:9000")
    assert resource.endpoint_url == "http://localhost:9000"


def test_default_resource_key(mod):
    component = mod.S3ResourceComponent()
    defs = component.build_defs(context=None)
    assert "s3_resource" in defs.resources
