"""mode='assets' (the default): one real Dagster asset per allow-listed operation. Only the
outbound HTTP call (requests.request) is mocked; spec parsing, operation extraction, args
templating, partition handling, and dg.materialize() are all real."""
import dagster as dg
import pytest

from .conftest import SAMPLE_SPEC, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeResponse:
    def __init__(self, json_body=None, text="", status_code=200):
        self._json = json_body
        self.text = text or ""
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self._json is None:
            raise ValueError("not json")
        return self._json


@pytest.fixture()
def calls(monkeypatch):
    seen = []

    def fake_request(method, url, **kwargs):
        seen.append({"method": method, "url": url, **kwargs})
        return _FakeResponse(json_body={"ok": True, "id": kwargs.get("params", {}).get("id")})

    import requests
    monkeypatch.setattr(requests, "request", fake_request)
    return seen


def _component(mod, **kw):
    return mod.OpenApiToolServerComponent(
        spec_url="http://fake-spec", operation_ids=["getPetById", "listPets"], **kw,
    )


@pytest.fixture(autouse=True)
def _patch_fetch_spec(mod, monkeypatch):
    monkeypatch.setattr(mod, "_fetch_spec", lambda *a, **k: SAMPLE_SPEC)


def _run(component, resources=None, partition_key=None):
    return dg.materialize(
        component.build_defs(None).assets, partition_key=partition_key,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


def test_one_asset_per_allowlisted_operation(mod):
    defs = _component(mod).build_defs(None)
    keys = {next(iter(a.keys)).to_user_string() for a in defs.assets}
    assert keys == {"getPetById", "listPets"}


def test_excluded_operation_never_becomes_an_asset(mod):
    defs = mod.OpenApiToolServerComponent(spec_url="x", operation_ids=["getPetById"]).build_defs(None)
    keys = {next(iter(a.keys)).to_user_string() for a in defs.assets}
    assert keys == {"getPetById"}
    assert "createPet" not in keys and "listPets" not in keys


def test_materialize_calls_the_right_url_with_path_param_substitution(mod, calls):
    component = _component(mod, api_key_env_var=None, operation_args={"getPetById": {"id": "42"}})
    r = _run(component)
    assert r.success
    call = next(c for c in calls if "/pets/42" in c["url"])
    assert call["method"] == "GET"
    assert call["url"] == "https://api.example.com/pets/42"


def test_query_param_goes_to_params_not_path(mod, calls):
    component = _component(mod, operation_args={"listPets": {"status": "available"}})
    _run(component)
    call = next(c for c in calls if c["url"].endswith("/pets"))
    assert call["params"] == {"status": "available"}


def test_tags_mark_tool_source_and_operation(mod):
    defs = _component(mod).build_defs(None)
    asset_def = next(a for a in defs.assets if next(iter(a.keys)).to_user_string() == "getPetById")
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/tool-source"] == "openapi"
    assert tags["dagster-community/openapi-operation"] == "getPetById"


def test_kinds_always_include_api_and_openapi(mod):
    """mode='assets' always adds both 'api' and 'openapi' kinds, the way mcp_tool_call always
    adds 'mcp' -- so a UI can find every operation asset this component emits by kind alone,
    even if the user sets their own kinds list."""
    defs = _component(mod, kinds=["custom"]).build_defs(None)
    asset_def = next(a for a in defs.assets if next(iter(a.keys)).to_user_string() == "getPetById")
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert "dagster/kind/api" in tags
    assert "dagster/kind/openapi" in tags
    assert "dagster/kind/custom" in tags


def test_kinds_default_to_api_and_openapi_only(mod):
    defs = _component(mod).build_defs(None)
    asset_def = next(a for a in defs.assets if next(iter(a.keys)).to_user_string() == "getPetById")
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    kind_tags = {t for t in tags if t.startswith("dagster/kind/")}
    assert kind_tags == {"dagster/kind/api", "dagster/kind/openapi"}


def test_asset_name_prefix_applied(mod):
    defs = _component(mod, asset_name_prefix="petstore").build_defs(None)
    keys = {next(iter(a.keys)).to_user_string() for a in defs.assets}
    assert keys == {"petstore_getPetById", "petstore_listPets"}


def test_metadata_includes_analogous_keys(mod, calls):
    component = _component(mod, operation_args={"getPetById": {"id": "1"}})
    r = _run(component)
    events = [e for e in r.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("getPetById")]
    md = events[-1].materialization.metadata
    assert md["operation_id"].value == "getPetById"
    assert md["method"].value == "GET"
    assert md["path"].value == "/pets/{id}"
    assert md["transport"].value == "http"
    assert md["status_code"].value == 200
    assert md["parsed_as"].value == "json"
    assert isinstance(md["latency_seconds"].value, float)
    assert md["response_bytes"].value >= 0


def test_args_templating_with_partition_key(mod, calls):
    component = _component(
        mod, operation_args={"getPetById": {"id": "{partition_key}"}},
        partition_type="daily", partition_start="2024-01-01",
    )
    r = _run(component, partition_key="2024-01-03")
    assert r.success
    call = next(c for c in calls if "/pets/2024-01-03" in c["url"])
    assert call is not None


def test_auth_header_applied(mod, calls, monkeypatch):
    monkeypatch.setenv("PET_API_KEY", "secret-token")
    component = _component(mod, api_key_env_var="PET_API_KEY", operation_args={"getPetById": {"id": "1"}})
    _run(component)
    call = next(c for c in calls if "/pets/1" in c["url"])
    assert call["headers"]["Authorization"] == "Bearer secret-token"
