"""Regression test for a real production bug: the credential-presence check
used to live at the top of `build_defs`, so a code location with a
JiraIngestionComponent configured via a not-yet-set environment variable
failed to LOAD at all -- taking every other asset in that code location
offline, not just this one. (Documented in a wizard-development write-up:
"this broke a real code location while building the wizard".)

Fixed by moving the check into the asset function body, so `build_defs`
always succeeds and only materializing the asset without a credential fails
-- and only that asset's run, not the whole code location.
"""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component_with_no_credentials(mod):
    return mod.JiraIngestionComponent(
        asset_name="test_asset",
        resources=["issues"],
        # domain/email/api_token and resource_key all deliberately omitted
    )


def test_build_defs_does_not_raise_with_no_credentials(mod):
    component = _make_component_with_no_credentials(mod)
    # Must NOT raise -- this is the code-location-load path.
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_materializing_with_no_credentials_still_fails_the_run(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component_with_no_credentials(mod)
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="supply resource_key"):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
