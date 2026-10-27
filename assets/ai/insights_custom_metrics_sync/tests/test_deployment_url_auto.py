"""deployment_url: auto -- derived only from confirmed sources (DAGSTER_CLOUD_DEPLOYMENT_NAME + instance.dagit_url), else a clear error."""
import pytest

from .conftest import load_component_module

M = load_component_module()


class Inst:
    def __init__(self, dagit_url):
        self._u = dagit_url

    @property
    def dagit_url(self):
        if isinstance(self._u, Exception):
            raise self._u
        return self._u


def test_explicit_value_always_wins_even_if_env_and_instance_disagree():
    env = {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "other"}
    assert M.resolve_deployment_url("https://acme.dagster.cloud/prod/", Inst("https://zzz.dagster.cloud/"), env) == "https://acme.dagster.cloud/prod"


@pytest.mark.parametrize("dagit, dep, want", [
    ("https://acme.dagster.cloud/", "prod", "https://acme.dagster.cloud/prod"),                          # US
    ("https://acme.eu.dagster.cloud/", "prod", "https://acme.eu.dagster.cloud/prod"),                    # EU region
    ("https://acme.dagster.cloud/prod/", "staging", "https://acme.dagster.cloud/staging"),               # instance-config deployment ignored
    ("https://acme.dagster.cloud/", "feature-branch-12", "https://acme.dagster.cloud/feature-branch-12"),  # branch deployment name
])
def test_auto_shapes(dagit, dep, want):
    assert M.resolve_deployment_url("auto", Inst(dagit), {"DAGSTER_CLOUD_DEPLOYMENT_NAME": dep}) == want


def test_auto_reads_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    assert M.resolve_deployment_url(" AUTO ", Inst("https://acme.dagster.cloud/")) == "https://acme.dagster.cloud/prod"


def test_unresolvable_cases_raise_and_say_to_set_it_explicitly():
    with pytest.raises(ValueError, match="DAGSTER_CLOUD_DEPLOYMENT_NAME is not set.*set deployment_url explicitly"):
        M.resolve_deployment_url("auto", Inst("https://acme.dagster.cloud/"), {})
    with pytest.raises(ValueError, match="does not expose the Dagster\\+ organization.*set deployment_url explicitly"):
        M.resolve_deployment_url("auto", object(), {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"})          # plain OSS instance
    with pytest.raises(ValueError, match="legacy|Could not derive"):
        M.resolve_deployment_url("auto", Inst(Exception("Could not derive Dagster Cloud URL from agent token (legacy)")),
                                 {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"})
    with pytest.raises(ValueError, match="instance.dagit_url is 'http://x/'"):
        M.resolve_deployment_url("auto", Inst("http://x/"), {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"})


def test_organization_env_vars_are_deliberately_not_used(monkeypatch):
    """DAGSTER_CLOUD_ORGANIZATION is a CLI/CI variable, not set in run workers: it must never be trusted to build the URL."""
    with pytest.raises(ValueError):
        M.resolve_deployment_url("auto", object(), {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod", "DAGSTER_CLOUD_ORGANIZATION": "acme"})


def test_component_accepts_auto_at_build_time_and_still_rejects_garbage():
    M.InsightsCustomMetricsSyncComponent(deployment_url="auto", presets=["agentic_pipeline"]).build_defs(None)
    with pytest.raises(ValueError):
        M.InsightsCustomMetricsSyncComponent(deployment_url="acme", presets=["agentic_pipeline"]).build_defs(None)


def test_asset_run_with_auto_fails_clearly_without_the_env(monkeypatch):
    import dagster as dg
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    defs = M.InsightsCustomMetricsSyncComponent(deployment_url="auto", presets=["agentic_pipeline"]).build_defs(None)
    res = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not res.success
    assert any("DAGSTER_CLOUD_DEPLOYMENT_NAME is not set" in str(e.event_specific_data) for e in res.all_events if e.is_step_failure)
