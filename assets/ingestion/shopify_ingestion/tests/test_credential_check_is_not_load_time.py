"""Regression test for a real production bug: the "supply resource_key OR
shop_url/private_app_password" validation used to run at the top of
`build_defs`, i.e. at CODE LOCATION LOAD TIME, not when the asset actually
runs. If the credential was an environment variable that hadn't been set
yet, this raised while Dagster was constructing `Definitions`, which
failed the WHOLE code location -- every other asset in it went offline
too, not just this one. (Documented in a wizard-development write-up:
"this broke a real code location while building the wizard.")

Fixed by moving the check from `build_defs` into the asset function body,
so a missing credential only fails that asset's own runs.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"


@pytest.fixture()
def mod():
    spec = importlib.util.spec_from_file_location("shopify_ingestion_component", _COMPONENT_PY)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_build_defs_does_not_raise_with_no_credentials(mod):
    component = mod.ShopifyIngestionComponent(asset_name="test_asset")
    # Must not raise -- this is the whole point of the fix.
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_materializing_with_no_credentials_fails_the_run_not_the_location(mod):
    component = mod.ShopifyIngestionComponent(asset_name="test_asset")
    asset_def = component.build_defs(context=None).assets[0]

    with pytest.raises(Exception) as exc_info:
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "supply resource_key" in str(exc_info.value)
    assert "shop_url/private_app_password" in str(exc_info.value)
