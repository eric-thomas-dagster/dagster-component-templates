"""GeoIpLookupComponent tests.

geoip2 is not installed in this environment, so the real `geoip2.database`,
`geoip2.errors`, and `geoip2.webservice` modules are faked wholesale (same
pattern this repo's geocoder tests use for `geopy`) -- everything else
(validation, row-by-row dispatch, status/metadata counting, column
population, partition filtering via the shared boilerplate) is the real
component code, exercised through a real dg.materialize() run.
"""
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

NOT_FOUND_IP = "203.0.113.1"   # TEST-NET-3, deliberately treated as "valid but not in DB"
INVALID_IP = "not-an-ip"


def _expected_city(ip: str) -> str:
    return f"City-{sum(ord(c) for c in ip) % 50}"


def _rec(**kw):
    ns = types.SimpleNamespace(**kw)
    return ns


@pytest.fixture(autouse=True)
def fake_geoip2(monkeypatch):
    class AddressNotFoundError(Exception):
        pass

    class InvalidRequestError(Exception):
        pass

    def _city_response(ip):
        n = sum(ord(c) for c in ip)
        return _rec(
            city=_rec(name=f"City-{n % 50}"),
            country=_rec(name=f"Country-{n % 10}", iso_code=f"C{n % 10}"),
            continent=_rec(name=f"Continent-{n % 5}"),
            subdivisions=_rec(most_specific=_rec(name=f"Region-{n % 20}", iso_code=f"R{n % 20}")),
            postal=_rec(code=f"{10000 + n % 90000}"),
            location=_rec(latitude=30.0 + n / 1000.0, longitude=-90.0 - n / 1000.0, accuracy_radius=n % 100),
        )

    def _asn_response(ip):
        n = sum(ord(c) for c in ip)
        return _rec(autonomous_system_number=10000 + n, autonomous_system_organization=f"ASN-Org-{n % 7}")

    def _lookup(ip, kind):
        if ip == INVALID_IP:
            raise ValueError(f"invalid IP address: {ip}")
        if ip == NOT_FOUND_IP:
            raise AddressNotFoundError(f"The address {ip} is not in the database.")
        return _city_response(ip) if kind == "city" else _asn_response(ip)

    class FakeReader:
        def __init__(self, path):
            self.path = path
            self.closed = False

        def city(self, ip):
            return _lookup(ip, "city")

        def asn(self, ip):
            return _lookup(ip, "asn")

        def close(self):
            self.closed = True

    class FakeWebserviceClient:
        def __init__(self, account_id, license_key, host="geoip.maxmind.com"):
            self.account_id = account_id
            self.license_key = license_key
            self.host = host
            self.closed = False

        def city(self, ip):
            return _lookup(ip, "city")

        def close(self):
            self.closed = True

    database_mod = types.ModuleType("geoip2.database")
    database_mod.Reader = FakeReader
    errors_mod = types.ModuleType("geoip2.errors")
    errors_mod.AddressNotFoundError = AddressNotFoundError
    errors_mod.InvalidRequestError = InvalidRequestError
    webservice_mod = types.ModuleType("geoip2.webservice")
    webservice_mod.Client = FakeWebserviceClient
    pkg = types.ModuleType("geoip2")
    pkg.database, pkg.errors, pkg.webservice = database_mod, errors_mod, webservice_mod

    monkeypatch.setitem(sys.modules, "geoip2", pkg)
    monkeypatch.setitem(sys.modules, "geoip2.database", database_mod)
    monkeypatch.setitem(sys.modules, "geoip2.errors", errors_mod)
    monkeypatch.setitem(sys.modules, "geoip2.webservice", webservice_mod)


@pytest.fixture()
def mod():
    return load_component_module()


class _FrameIO(dg.InMemoryIOManager):
    def __init__(self, frames):
        super().__init__()
        self._frames = frames

    def load_input(self, context):
        name = context.asset_key.to_user_string()
        if name in self._frames:
            return self._frames[name]
        return super().load_input(context)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="enriched",
        upstream_asset_key="web_logs",
        ip_column="ip",
        database_path="/fake/GeoLite2-City.mmdb",
    )
    kwargs.update(overrides)
    return mod.GeoIpLookupComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frame):
    return dg.materialize(
        [_asset(component), dg.SourceAsset("web_logs")],
        resources={"io_manager": _FrameIO({"web_logs": frame})},
    )


def test_database_mode_matched_not_found_invalid(mod):
    frame = pd.DataFrame({"ip": ["8.8.8.8", NOT_FOUND_IP, INVALID_IP]})
    component = _component(mod, match_status_column="status")
    result = _run(component, frame)
    assert result.success
    df = result.output_for_node("enriched")
    assert list(df["status"]) == ["matched", "not_found", "invalid"]
    assert df.loc[0, "geoip_city"] == _expected_city("8.8.8.8")
    assert pd.isna(df.loc[1, "geoip_city"])
    assert pd.isna(df.loc[2, "geoip_city"])

    mats = list(result.get_asset_materialization_events())
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["rows_matched"].value == 1
    assert meta["rows_unmatched_not_found"].value == 1
    assert meta["rows_unmatched_invalid"].value == 1


def test_asn_enrichment_requires_separate_database(mod):
    frame = pd.DataFrame({"ip": ["8.8.8.8"]})
    component = _component(
        mod,
        asn_database_path="/fake/GeoLite2-ASN.mmdb",
        asn_column="asn",
        asn_org_column="asn_org",
    )
    result = _run(component, frame)
    df = result.output_for_node("enriched")
    assert df.loc[0, "asn"] == 10000 + sum(ord(c) for c in "8.8.8.8")
    assert df.loc[0, "asn_org"].startswith("ASN-Org-")
    # City fields still populated from the separate city database_path.
    assert df.loc[0, "geoip_city"] == _expected_city("8.8.8.8")


def test_optional_columns_off_by_default(mod):
    frame = pd.DataFrame({"ip": ["8.8.8.8"]})
    result = _run(_component(mod), frame)
    df = result.output_for_node("enriched")
    for col in ("geoip_continent", "geoip_subdivision", "geoip_postal_code", "geoip_accuracy_radius"):
        assert col not in df.columns


def test_web_service_mode_uses_hosted_client(mod, monkeypatch):
    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", "12345")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test-key")
    frame = pd.DataFrame({"ip": ["8.8.8.8", INVALID_IP]})
    component = _component(mod, lookup_mode="web_service", database_path=None, match_status_column="status")
    result = _run(component, frame)
    assert result.success
    df = result.output_for_node("enriched")
    assert list(df["status"]) == ["matched", "invalid"]
    assert df.loc[0, "geoip_city"] == _expected_city("8.8.8.8")


def test_web_service_mode_requires_credentials(mod, monkeypatch):
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)
    frame = pd.DataFrame({"ip": ["8.8.8.8"]})
    component = _component(mod, lookup_mode="web_service", database_path=None)
    with pytest.raises(Exception, match="MAXMIND_ACCOUNT_ID"):
        _run(component, frame)


def test_web_service_mode_rejects_asn_fields(mod):
    with pytest.raises(ValueError, match="ASN enrichment"):
        mod.GeoIpLookupComponent(
            asset_name="enriched", upstream_asset_key="web_logs", ip_column="ip",
            lookup_mode="web_service", asn_column="asn",
        ).build_defs(None)


def test_database_mode_requires_a_database_path(mod):
    with pytest.raises(ValueError, match="requires database_path"):
        mod.GeoIpLookupComponent(
            asset_name="enriched", upstream_asset_key="web_logs", ip_column="ip",
        ).build_defs(None)


def test_unknown_lookup_mode_rejected(mod):
    with pytest.raises(ValueError, match="lookup_mode"):
        mod.GeoIpLookupComponent(
            asset_name="enriched", upstream_asset_key="web_logs", ip_column="ip",
            lookup_mode="carrier-pigeon",
        ).build_defs(None)
