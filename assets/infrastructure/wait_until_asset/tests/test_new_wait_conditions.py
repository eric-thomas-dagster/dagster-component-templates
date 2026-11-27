"""Real, committed tests for WaitUntilAssetComponent's sftp_file_exists/s3_key_exists/
gcs_object_exists/http_ok conditions -- real dg.materialize() throughout, small
poll_seconds/timeout_seconds so the suite stays fast. Mocks only the unavoidable
external network/SDK boundary (paramiko's SSH client, boto3's S3 client, the GCS
storage client, requests' HTTP call), never the condition-check logic itself.
"""
from unittest.mock import MagicMock, patch

import pytest

from .conftest import materialize


def test_requires_fields_for_new_conditions(mod):
    with pytest.raises(Exception, match="requires `sftp_host_env_var`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="sftp_file_exists")
    with pytest.raises(Exception, match="requires `s3_bucket`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="s3_key_exists")
    with pytest.raises(Exception, match="requires `gcs_bucket`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="gcs_object_exists")
    with pytest.raises(Exception, match="requires `http_url`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="http_ok")


def test_sftp_file_exists_requires_password_or_key(mod):
    with pytest.raises(Exception, match="requires `sftp_password_env_var` or `sftp_private_key_env_var`"):
        mod.WaitUntilAssetComponent(
            asset_name="t", condition="sftp_file_exists",
            sftp_host_env_var="H", sftp_username_env_var="U", sftp_path="/x",
        )


def test_http_ok_basic_requires_username_and_password(mod):
    with pytest.raises(Exception, match="http_auth_type='basic' requires"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="http_ok", http_url="http://x", http_auth_type="basic")


def test_http_ok_bearer_requires_token(mod):
    with pytest.raises(Exception, match="http_auth_type='bearer' requires"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="http_ok", http_url="http://x", http_auth_type="bearer")


# --- condition='sftp_file_exists' ------------------------------------------------------

def test_sftp_file_exists_met_when_stat_succeeds(mod, monkeypatch):
    monkeypatch.setenv("SFTP_HOST", "host.example.com")
    monkeypatch.setenv("SFTP_USER", "alice")
    monkeypatch.setenv("SFTP_PASS", "secret")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="sftp_file_exists",
        sftp_host_env_var="SFTP_HOST", sftp_username_env_var="SFTP_USER",
        sftp_password_env_var="SFTP_PASS", sftp_path="/inbox/ready.csv",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_sftp = MagicMock()
    mock_sftp.stat.return_value = MagicMock()
    mock_ssh = MagicMock()
    mock_ssh.open_sftp.return_value = mock_sftp
    with patch("paramiko.SSHClient", return_value=mock_ssh), patch("paramiko.AutoAddPolicy"):
        result = materialize(component)
    assert result.success
    mock_ssh.connect.assert_called_once()
    assert mock_ssh.connect.call_args.kwargs["password"] == "secret"


def test_sftp_file_exists_times_out_when_stat_raises_not_found(mod, monkeypatch):
    monkeypatch.setenv("SFTP_HOST", "host.example.com")
    monkeypatch.setenv("SFTP_USER", "alice")
    monkeypatch.setenv("SFTP_PASS", "secret")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="sftp_file_exists",
        sftp_host_env_var="SFTP_HOST", sftp_username_env_var="SFTP_USER",
        sftp_password_env_var="SFTP_PASS", sftp_path="/inbox/missing.csv",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_sftp = MagicMock()
    mock_sftp.stat.side_effect = FileNotFoundError()
    mock_ssh = MagicMock()
    mock_ssh.open_sftp.return_value = mock_sftp
    with patch("paramiko.SSHClient", return_value=mock_ssh), patch("paramiko.AutoAddPolicy"):
        result = materialize(component, raise_on_error=False)
    assert not result.success


# --- condition='s3_key_exists' ----------------------------------------------------------

def test_s3_key_exists_met_when_head_object_succeeds(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists",
        s3_bucket="my-bucket", s3_key="path/ready.csv",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_client = MagicMock()
    mock_client.head_object.return_value = {"ContentLength": 10}
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_client.head_object.assert_called_with(Bucket="my-bucket", Key="path/ready.csv")


def test_s3_key_exists_times_out_on_404(mod):
    from botocore.exceptions import ClientError

    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists",
        s3_bucket="my-bucket", s3_key="path/missing.csv",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component, raise_on_error=False)
    assert not result.success


def test_s3_key_exists_hard_fails_on_non_404_client_error(mod):
    from botocore.exceptions import ClientError

    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists",
        s3_bucket="my-bucket", s3_key="path/x.csv",
        poll_seconds=5, timeout_seconds=30,
    )
    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadObject")
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component, raise_on_error=False)
    assert not result.success


# --- condition='gcs_object_exists' ------------------------------------------------------

def test_gcs_object_exists_met_when_blob_exists(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="gcs_object_exists",
        gcs_bucket="my-bucket", gcs_blob_name="path/ready.csv",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    with patch("google.cloud.storage.Client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_bucket.blob.assert_called_with("path/ready.csv")


def test_gcs_object_exists_times_out_when_blob_absent(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="gcs_object_exists",
        gcs_bucket="my-bucket", gcs_blob_name="path/missing.csv",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    with patch("google.cloud.storage.Client", return_value=mock_client):
        result = materialize(component, raise_on_error=False)
    assert not result.success


# --- condition='http_ok' ----------------------------------------------------------------

def test_http_ok_met_on_2xx(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_resp = MagicMock(status_code=200)
    with patch("requests.Session.request", return_value=mock_resp) as mock_request:
        result = materialize(component)
    assert result.success
    mock_request.assert_called_with("GET", "http://service.example.com/healthz", headers=None, timeout=10)


def test_http_ok_times_out_on_non_2xx(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_resp = MagicMock(status_code=503)
    with patch("requests.Session.request", return_value=mock_resp):
        result = materialize(component, raise_on_error=False)
    assert not result.success


def test_http_ok_times_out_on_connection_error_not_hard_fail(mod):
    import requests

    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    with patch("requests.Session.request", side_effect=requests.ConnectionError("refused")):
        result = materialize(component, raise_on_error=False)
    assert not result.success


def test_http_ok_bearer_auth_sets_header(mod, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "tok123")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        http_auth_type="bearer", http_auth_token_env_var="API_TOKEN",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_resp = MagicMock(status_code=200)
    captured = {}

    def _fake_request(self, method, url, headers=None, timeout=None):
        captured["auth_header"] = self.headers.get("Authorization")
        return mock_resp

    with patch("requests.Session.request", _fake_request):
        result = materialize(component)
    assert result.success
    assert captured["auth_header"] == "Bearer tok123"


def test_http_ok_custom_method_and_headers(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        http_method="HEAD", http_headers={"X-Probe": "1"},
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_resp = MagicMock(status_code=200)
    with patch("requests.Session.request", return_value=mock_resp) as mock_request:
        result = materialize(component)
    assert result.success
    mock_request.assert_called_with("HEAD", "http://service.example.com/healthz", headers={"X-Probe": "1"}, timeout=10)


def test_http_ok_success_status_codes_narrows_default_2xx(mod):
    """202 is a 2xx status but not in the explicit allow-list -- should count as not-yet-met,
    not success, unlike the old hardcoded 200<=code<300 check."""
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        http_success_status_codes=[200], poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_resp = MagicMock(status_code=202)
    with patch("requests.Session.request", return_value=mock_resp):
        result = materialize(component, raise_on_error=False)
    assert not result.success


def test_http_ok_success_status_codes_allows_non_2xx(mod):
    """A caller can explicitly treat a 404 as success (e.g. 'this endpoint being gone IS the
    signal we're waiting for')."""
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="http_ok", http_url="http://service.example.com/healthz",
        http_success_status_codes=[404], poll_seconds=0.01, timeout_seconds=5,
    )
    mock_resp = MagicMock(status_code=404)
    with patch("requests.Session.request", return_value=mock_resp):
        result = materialize(component)
    assert result.success


# --- match_mode: prefix/wildcard (s3/gcs/sftp) -------------------------------------------

def test_s3_key_exists_prefix_mode_succeeds_when_any_object_under_prefix(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists", s3_match_mode="prefix",
        s3_bucket="my-bucket", s3_key="incoming/2026-01-01/",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = {"KeyCount": 2}
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_client.list_objects_v2.assert_called_with(Bucket="my-bucket", Prefix="incoming/2026-01-01/", MaxKeys=1)


def test_s3_key_exists_wildcard_mode_matches_via_fnmatch(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists", s3_match_mode="wildcard",
        s3_bucket="my-bucket", s3_key="incoming/*/ready.csv",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": "incoming/2026-01-01/ready.csv"}]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_paginator.paginate.assert_called_with(Bucket="my-bucket", Prefix="incoming/")


def test_s3_key_exists_wildcard_mode_times_out_when_no_key_matches(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="s3_key_exists", s3_match_mode="wildcard",
        s3_bucket="my-bucket", s3_key="incoming/*/ready.csv",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": "incoming/2026-01-01/other.csv"}]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    with patch("boto3.client", return_value=mock_client):
        result = materialize(component, raise_on_error=False)
    assert not result.success


def test_gcs_object_exists_prefix_mode_succeeds_when_any_blob_under_prefix(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="gcs_object_exists", gcs_match_mode="prefix",
        gcs_bucket="my-bucket", gcs_blob_name="incoming/2026-01-01/",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.list_blobs.return_value = iter([MagicMock()])
    with patch("google.cloud.storage.Client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_client.list_blobs.assert_called_with(mock_bucket, prefix="incoming/2026-01-01/", max_results=1)


def test_gcs_object_exists_wildcard_mode_matches_via_fnmatch(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="gcs_object_exists", gcs_match_mode="wildcard",
        gcs_bucket="my-bucket", gcs_blob_name="incoming/*/ready.csv",
        poll_seconds=0.01, timeout_seconds=5,
    )
    matching_blob = MagicMock(name="incoming/2026-01-01/ready.csv")
    matching_blob.name = "incoming/2026-01-01/ready.csv"
    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.list_blobs.return_value = iter([matching_blob])
    with patch("google.cloud.storage.Client", return_value=mock_client):
        result = materialize(component)
    assert result.success
    mock_client.list_blobs.assert_called_with(mock_bucket, prefix="incoming/")


def test_sftp_file_exists_prefix_mode_lists_directory(mod, monkeypatch):
    monkeypatch.setenv("SFTP_HOST", "host.example.com")
    monkeypatch.setenv("SFTP_USER", "alice")
    monkeypatch.setenv("SFTP_PASS", "secret")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="sftp_file_exists", sftp_match_mode="prefix",
        sftp_host_env_var="SFTP_HOST", sftp_username_env_var="SFTP_USER",
        sftp_password_env_var="SFTP_PASS", sftp_path="/inbox/ready",
        poll_seconds=0.01, timeout_seconds=5,
    )
    mock_sftp = MagicMock()
    mock_sftp.listdir.return_value = ["ready_20260101.csv", "other.txt"]
    mock_ssh = MagicMock()
    mock_ssh.open_sftp.return_value = mock_sftp
    with patch("paramiko.SSHClient", return_value=mock_ssh), patch("paramiko.AutoAddPolicy"):
        result = materialize(component)
    assert result.success
    mock_sftp.listdir.assert_called_with("/inbox")


def test_sftp_file_exists_wildcard_mode_matches_via_fnmatch(mod, monkeypatch):
    monkeypatch.setenv("SFTP_HOST", "host.example.com")
    monkeypatch.setenv("SFTP_USER", "alice")
    monkeypatch.setenv("SFTP_PASS", "secret")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="sftp_file_exists", sftp_match_mode="wildcard",
        sftp_host_env_var="SFTP_HOST", sftp_username_env_var="SFTP_USER",
        sftp_password_env_var="SFTP_PASS", sftp_path="/inbox/*.csv",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    mock_sftp = MagicMock()
    mock_sftp.listdir.return_value = ["notes.txt"]
    mock_ssh = MagicMock()
    mock_ssh.open_sftp.return_value = mock_sftp
    with patch("paramiko.SSHClient", return_value=mock_ssh), patch("paramiko.AutoAddPolicy"):
        result = materialize(component, raise_on_error=False)
    assert not result.success
