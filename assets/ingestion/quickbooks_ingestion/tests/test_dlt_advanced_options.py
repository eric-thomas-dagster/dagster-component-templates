"""on_schema_change/exclude_columns/max_rows_per_resource for QuickBooksIngestionComponent.

No conftest.py exists for this component. The fake `rest_api_source` returns a bare
`object()` capturing `config`, so on_schema_change (injected into
`config["resource_defaults"]` BEFORE rest_api_source(config) is called) is fully
verifiable, but exclude_columns/max_rows_per_resource (applied to
`source.resources.values()` AFTER) are not reachable through this fake -- covered at
the unit level instead (same shared helpers every component in this rollout uses).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("quickbooks_ingestion_component_adv", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

QuickBooksIngestionComponent = _mod.QuickBooksIngestionComponent


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        client_id="cid",
        client_secret="csecret",
        refresh_token="rtoken",
        realm_id="12345",
        resources="Customer,Invoice",
    )
    defaults.update(overrides)
    return QuickBooksIngestionComponent(**defaults)


class _Pipeline:
    def __init__(self, **kw):
        pass

    def run(self, *a, **k):
        return "load-info"

    def sql_client(self):
        import contextlib

        class _Cursor:
            def df(self):
                import pandas as pd

                return pd.DataFrame({"id": []})

        @contextlib.contextmanager
        def _cm():
            yield _Cursor()

        class _Sql:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute_query(self, sql, *a, **k):
                return _cm()

        return _Sql()


def _fake_token_response(monkeypatch):
    """`_fetch_access_token` does `import requests as _requests; _requests.post(...)` locally --
    that import resolves to the same module object already in sys.modules, so patching the real
    `requests.post` here reaches it."""
    import requests

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "fake-token"}

    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp())


def _patch_dlt(monkeypatch, captured_configs):
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline(**kw))
    import dlt.sources.rest_api as _rest_api

    def _fake_rest_api_source(config):
        captured_configs.append(config)
        return object()

    monkeypatch.setattr(_rest_api, "rest_api_source", _fake_rest_api_source)


def test_on_schema_change_default_does_not_set_resource_defaults(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, captured)
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])
    assert "resource_defaults" not in captured[0]


def test_on_schema_change_block_all_sets_resource_defaults(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, captured)
    component = _build_component(on_schema_change="block_all")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])
    assert captured[0]["resource_defaults"] == {
        "schema_contract": {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    }


def test_on_schema_change_rejects_invalid_value():
    with pytest.raises(Exception):
        _build_component(on_schema_change="bogus")


def test_resolve_schema_contract_maps_each_value():
    assert _mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert _mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert _mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        _mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing():
    fn = _mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}


def test_exclude_columns_applied_before_hash_columns():
    exclude_map = _mod._make_exclude_columns_map(["email"])
    hash_map = _mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}
