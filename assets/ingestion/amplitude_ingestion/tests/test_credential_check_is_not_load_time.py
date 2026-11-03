"""Regression test for a real production bug: the "supply resource_key OR
credentials" validation(s) used to run at the top of `build_defs`, i.e. at
CODE LOCATION LOAD TIME, not when the asset actually runs. If the
credential was an environment variable that hadn't been set yet, this
raised while Dagster was constructing `Definitions`, which failed the
WHOLE code location -- every other asset in it went offline too, not just
this one. (Documented in a wizard-development write-up: "this broke a
real code location while building the wizard.")

Fixed by moving both checks (resource_key-or-api_key/secret_key, and the
"secret_key must always be supplied inline" check) from `build_defs` into
the asset function body, so a missing credential only fails that asset's
own runs.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_build_defs_does_not_raise_with_no_credentials(mod):
    component = mod.AmplitudeIngestionComponent(asset_name="test_asset")
    # Must not raise -- this is the whole point of the fix.
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_materializing_with_no_credentials_fails_the_run_not_the_location(mod):
    component = mod.AmplitudeIngestionComponent(asset_name="test_asset")
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "supply resource_key" in str(exc_info.value)
    assert "api_key/secret_key" in str(exc_info.value)


def test_materializing_with_resource_key_but_no_secret_key_still_fails_the_run_not_the_location(mod):
    """The second, narrower check: secret_key is never sourced from
    resource_key, so it must always be supplied inline even when
    resource_key covers api_key (which satisfies the first check)."""
    import dagster as _dg

    @_dg.resource
    def _fake_amplitude_resource(_):
        return object()

    component = mod.AmplitudeIngestionComponent(asset_name="test_asset", resource_key="amplitude")
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize(
            [asset_def],
            resources={"io_manager": dg.InMemoryIOManager(), "amplitude": _fake_amplitude_resource},
        )

    assert "secret_key must always be supplied inline" in str(exc_info.value)
