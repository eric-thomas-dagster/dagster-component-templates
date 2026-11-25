"""GeoIpDatabaseUpdaterComponent tests.

Mocks ONLY the external HTTP boundary (requests.Session.get) -- everything
else (MD5 compare/skip logic, tar.gz extraction, atomic write, hash
verification, credential/error handling) is the real component code,
exercised through a real dg.materialize() run against real temp files.
"""
import gzip
import hashlib
import io
import os
import tarfile

import dagster as dg
import pytest

from .conftest import load_component_module


MMDB_CONTENT = b"fake-mmdb-binary-content-for-GeoLite2-City"
MMDB_CONTENT_V2 = b"fake-mmdb-binary-content-for-GeoLite2-City-v2-newer-build"


def _make_tar_gz(edition_id: str, content: bytes) -> bytes:
    """Build a tar.gz matching MaxMind's real archive layout: a top-level
    '{edition_id}_{date}/{edition_id}.mmdb' member."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            data = io.BytesIO(content)
            info = tarfile.TarInfo(name=f"{edition_id}_20240101/{edition_id}.mmdb")
            info.size = len(content)
            tar.addfile(info, data)
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, content_bytes=b"", headers=None):
        self.status_code = status_code
        self._json_data = json_data
        self._content_bytes = content_bytes
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self._content_bytes), chunk_size):
            yield self._content_bytes[i:i + chunk_size]


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def maxmind_env(monkeypatch):
    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", "12345")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test-license-key")


def _component(mod, tmp_path, **overrides):
    kwargs = dict(
        asset_name="geoip_databases",
        editions=["GeoLite2-City"],
        database_directory=str(tmp_path),
    )
    kwargs.update(overrides)
    return mod.GeoIpDatabaseUpdaterComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _install_fake_server(monkeypatch, *, server_md5, server_date="2024-01-01", archive_bytes=None, calls=None):
    calls = calls if calls is not None else []

    def fake_get(self, url, params=None, auth=None, timeout=None, stream=False):
        calls.append({"url": url, "params": params, "stream": stream})
        if url.endswith("/geoip/updates/metadata"):
            return _FakeResponse(json_data={
                "databases": [{"edition_id": params["edition_id"], "date": server_date, "md5": server_md5}]
            })
        if "/geoip/databases/" in url and url.endswith("/download"):
            return _FakeResponse(content_bytes=archive_bytes, headers={"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"})
        raise AssertionError(f"unexpected URL: {url}")

    import requests
    monkeypatch.setattr(requests.Session, "get", fake_get)
    return calls


def test_first_run_downloads_and_writes_mmdb(mod, tmp_path, monkeypatch):
    server_md5 = hashlib.md5(MMDB_CONTENT).hexdigest()
    archive = _make_tar_gz("GeoLite2-City", MMDB_CONTENT)
    calls = _install_fake_server(monkeypatch, server_md5=server_md5, archive_bytes=archive)

    result = dg.materialize([_asset(_component(mod, tmp_path))])
    assert result.success

    dest = tmp_path / "GeoLite2-City.mmdb"
    assert dest.exists()
    assert dest.read_bytes() == MMDB_CONTENT

    download_calls = [c for c in calls if "/download" in c["url"]]
    assert len(download_calls) == 1


def test_second_run_skips_download_when_md5_unchanged(mod, tmp_path, monkeypatch):
    server_md5 = hashlib.md5(MMDB_CONTENT).hexdigest()
    archive = _make_tar_gz("GeoLite2-City", MMDB_CONTENT)

    # First run: populates the local file.
    calls = _install_fake_server(monkeypatch, server_md5=server_md5, archive_bytes=archive)
    dg.materialize([_asset(_component(mod, tmp_path))])
    assert len([c for c in calls if "/download" in c["url"]]) == 1

    # Second run: server reports the SAME md5 -- must not re-download.
    calls.clear()
    result = dg.materialize([_asset(_component(mod, tmp_path))])
    assert result.success
    assert len([c for c in calls if "/download" in c["url"]]) == 0
    assert len([c for c in calls if "/metadata" in c["url"]]) == 1


def test_run_downloads_again_when_server_md5_changes(mod, tmp_path, monkeypatch):
    old_md5 = hashlib.md5(MMDB_CONTENT).hexdigest()
    new_md5 = hashlib.md5(MMDB_CONTENT_V2).hexdigest()

    calls = _install_fake_server(monkeypatch, server_md5=old_md5, archive_bytes=_make_tar_gz("GeoLite2-City", MMDB_CONTENT))
    dg.materialize([_asset(_component(mod, tmp_path))])

    calls.clear()
    _install_fake_server(monkeypatch, server_md5=new_md5, archive_bytes=_make_tar_gz("GeoLite2-City", MMDB_CONTENT_V2), calls=calls)
    result = dg.materialize([_asset(_component(mod, tmp_path))])
    assert result.success
    assert len([c for c in calls if "/download" in c["url"]]) == 1
    assert (tmp_path / "GeoLite2-City.mmdb").read_bytes() == MMDB_CONTENT_V2


def test_md5_mismatch_raises_and_does_not_write(mod, tmp_path, monkeypatch):
    server_md5 = "0" * 32  # deliberately wrong vs. the real extracted content's md5
    archive = _make_tar_gz("GeoLite2-City", MMDB_CONTENT)
    _install_fake_server(monkeypatch, server_md5=server_md5, archive_bytes=archive)

    with pytest.raises(Exception, match="MD5 mismatch"):
        dg.materialize([_asset(_component(mod, tmp_path))])
    assert not (tmp_path / "GeoLite2-City.mmdb").exists()


def test_missing_credentials_raises_clear_error(mod, tmp_path, monkeypatch):
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)
    with pytest.raises(Exception, match="MAXMIND_ACCOUNT_ID"):
        dg.materialize([_asset(_component(mod, tmp_path))])


def test_invalid_edition_id_rejected(mod, tmp_path):
    with pytest.raises(Exception, match="Invalid edition id"):
        dg.materialize([_asset(_component(mod, tmp_path, editions=["../../etc/passwd"]))])


def test_multiple_editions_each_tracked_independently(mod, tmp_path, monkeypatch):
    city_md5 = hashlib.md5(MMDB_CONTENT).hexdigest()
    asn_md5 = hashlib.md5(MMDB_CONTENT_V2).hexdigest()
    calls = []

    def fake_get(self, url, params=None, auth=None, timeout=None, stream=False):
        calls.append(url)
        edition_id = params["edition_id"] if params and "edition_id" in params else None
        if url.endswith("/geoip/updates/metadata"):
            md5 = city_md5 if edition_id == "GeoLite2-City" else asn_md5
            return _FakeResponse(json_data={"databases": [{"edition_id": edition_id, "date": "2024-01-01", "md5": md5}]})
        if "/geoip/databases/GeoLite2-City/" in url:
            return _FakeResponse(content_bytes=_make_tar_gz("GeoLite2-City", MMDB_CONTENT))
        if "/geoip/databases/GeoLite2-ASN/" in url:
            return _FakeResponse(content_bytes=_make_tar_gz("GeoLite2-ASN", MMDB_CONTENT_V2))
        raise AssertionError(url)

    import requests
    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = dg.materialize([_asset(_component(mod, tmp_path, editions=["GeoLite2-City", "GeoLite2-ASN"]))])
    assert result.success
    assert (tmp_path / "GeoLite2-City.mmdb").read_bytes() == MMDB_CONTENT
    assert (tmp_path / "GeoLite2-ASN.mmdb").read_bytes() == MMDB_CONTENT_V2
