import pytest

from .conftest import execute_with_checks, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


# ---------------------------------------------------------------------------
# models_offered_for_provider -- pure function
# ---------------------------------------------------------------------------


def test_models_offered_declared_override_wins(mod):
    assert mod.models_offered_for_provider("openai", ["gpt-4o-mini", "gpt-4o"]) == ["gpt-4o-mini", "gpt-4o"]


def test_models_offered_falls_back_to_litellm_catalog(mod):
    models = mod.models_offered_for_provider("openai", None)
    assert isinstance(models, list)
    assert len(models) > 0


def test_models_offered_unknown_provider_returns_empty_list_without_raising(mod):
    assert mod.models_offered_for_provider("not_a_real_provider_xyz", None) == []


# ---------------------------------------------------------------------------
# run_live_connectivity_check -- pure function (network calls faked out)
# ---------------------------------------------------------------------------


def test_live_check_stub_provider_is_not_supported(mod):
    result = mod.run_live_connectivity_check("bedrock", "any-key", None)
    assert result == {"supported": False, "ok": True, "models": [], "error": None}


def test_live_check_unknown_provider_is_not_supported(mod):
    result = mod.run_live_connectivity_check("not_a_real_provider_xyz", "any-key", None)
    assert result["supported"] is False
    assert result["ok"] is True


def test_live_check_provider_without_endpoint_is_not_supported(mod, monkeypatch):
    from litellm.utils import ProviderConfigManager

    monkeypatch.setattr(ProviderConfigManager, "get_provider_model_info", lambda model, provider: None)
    result = mod.run_live_connectivity_check("openai", "any-key", None)
    assert result == {"supported": False, "ok": True, "models": [], "error": None}


def test_live_check_success_surfaces_models(mod, monkeypatch):
    from litellm.utils import ProviderConfigManager

    class _FakeConfig:
        def get_models(self, api_key=None, api_base=None):
            assert api_key == "sk-real"
            return ["gpt-4o", "gpt-4o-mini"]

    monkeypatch.setattr(ProviderConfigManager, "get_provider_model_info", lambda model, provider: _FakeConfig())
    result = mod.run_live_connectivity_check("openai", "sk-real", None)
    assert result == {"supported": True, "ok": True, "models": ["gpt-4o", "gpt-4o-mini"], "error": None}


def test_live_check_failure_surfaces_real_error(mod, monkeypatch):
    from litellm.utils import ProviderConfigManager

    class _FakeConfig:
        def get_models(self, api_key=None, api_base=None):
            raise Exception("Incorrect API key provided")

    monkeypatch.setattr(ProviderConfigManager, "get_provider_model_info", lambda model, provider: _FakeConfig())
    result = mod.run_live_connectivity_check("openai", "sk-bogus", None)
    assert result["supported"] is True
    assert result["ok"] is False
    assert result["error"] == "Incorrect API key provided"


def test_live_check_not_implemented_is_not_supported(mod, monkeypatch):
    from litellm.utils import ProviderConfigManager

    class _FakeConfig:
        def get_models(self, api_key=None, api_base=None):
            raise NotImplementedError

    monkeypatch.setattr(ProviderConfigManager, "get_provider_model_info", lambda model, provider: _FakeConfig())
    result = mod.run_live_connectivity_check("vertex_ai", "any-key", None)
    assert result == {"supported": False, "ok": True, "models": [], "error": None}


# ---------------------------------------------------------------------------
# Component: build_defs + asset/check execution
# ---------------------------------------------------------------------------


def _component(mod, **overrides):
    kwargs = dict(asset_name="openai_provider", provider="openai", api_key_env_var="OPENAI_API_KEY_TEST_VAR")
    kwargs.update(overrides)
    return mod.ModelProviderComponent(**kwargs)


def test_build_defs_does_not_raise_without_env_var(mod, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY_TEST_VAR", raising=False)
    component = _component(mod)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1
    assert len(defs.asset_checks) == 1  # presence check only, no test_call


def test_test_call_false_means_no_live_connectivity_check(mod):
    component = _component(mod, test_call=False)
    defs = component.build_defs(context=None)
    assert len(defs.asset_checks) == 1


def test_test_call_true_adds_live_connectivity_check(mod):
    component = _component(mod, test_call=True)
    defs = component.build_defs(context=None)
    assert len(defs.asset_checks) == 2


def test_api_key_present_check_fails_when_unset(mod, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY_TEST_VAR", raising=False)
    component = _component(mod)
    result = execute_with_checks(component.build_defs(context=None))
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evaluations["openai_provider_api_key_present"].passed is False


def test_api_key_present_check_passes_when_set(mod, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_TEST_VAR", "sk-real-value")
    component = _component(mod)
    result = execute_with_checks(component.build_defs(context=None))
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evaluations["openai_provider_api_key_present"].passed is True


def test_asset_observes_models_offered_and_key_presence(mod, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_TEST_VAR", "sk-real-value")
    component = _component(mod, models=["gpt-4o-mini"])
    result = execute_with_checks(component.build_defs(context=None))
    materialization = result.get_asset_observation_events()[0].event_specific_data.asset_observation
    metadata = materialization.metadata
    assert metadata["api_key_present"].value is True
    assert metadata["models_offered"].data == ["gpt-4o-mini"]


def test_live_connectivity_check_runs_and_reports_metadata(mod, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_TEST_VAR", "sk-real-value")

    def _fake_live_check(provider, api_key, api_base):
        assert provider == "openai"
        assert api_key == "sk-real-value"
        return {"supported": True, "ok": True, "models": ["gpt-4o"], "error": None}

    monkeypatch.setattr(mod, "run_live_connectivity_check", _fake_live_check)
    component = _component(mod, test_call=True)
    result = execute_with_checks(component.build_defs(context=None))
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}
    live = evaluations["openai_provider_live_connectivity"]
    assert live.passed is True
    assert live.metadata["models_found"].value == 1
