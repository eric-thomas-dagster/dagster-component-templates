"""Real, committed tests for SapRFCResourceComponent -- mocks nothing; `pyrfc` is never
imported anywhere in this component except inside `SapRFCResource.connection()`, which none
of these tests call.

The core bug this guards against: Dagster's own component-YAML resolution runs EVERY string
attribute value through Jinja2's NativeTemplate rendering (confirmed directly against the
installed `dagster`/`jinja2` packages, not assumed) -- even a plain value with no `{{ }}`
syntax at all. A numeric-looking string comes back out as a Python int regardless of how it
was quoted in the source YAML: '00' -> the int 0 (the leading zero is gone before this
component ever sees it), '100' -> the int 100. `client`/`sysnr` are SAP's own fixed-width
numeric codes (three digits / two digits), so this used to crash SapRFCResourceComponent's own
registry example with a real pydantic.ValidationError ("Input should be a valid string").
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **overrides):
    defaults = dict(
        ashost="sapecc01.acme.com", sysnr="00", client="100", user="DAGSTER_RFC",
        passwd_env_var="SAP_RFC_PASSWORD",
    )
    defaults.update(overrides)
    return mod.SapRFCResourceComponent(**defaults)


def test_quoted_string_inputs_are_unaffected(mod):
    component = _component(mod)
    assert component.sysnr == "00"
    assert component.client == "100"


def test_int_client_is_zero_padded_to_three_digits(mod):
    # Simulates exactly what Dagster's own NativeTemplate resolution hands this component
    # when the saved YAML had client: "100" -- the int 100, not the string "100".
    component = _component(mod, client=100)
    assert component.client == "100"
    assert isinstance(component.client, str)


def test_int_sysnr_is_zero_padded_to_two_digits(mod):
    # '00' -> NativeTemplate -> the int 0 -- the leading zero is gone by the time any Python
    # code (ours or Dagster's) sees it; re-zero-padding to SAP's fixed 2-digit width is how
    # it's recovered.
    component = _component(mod, sysnr=0)
    assert component.sysnr == "00"
    assert isinstance(component.sysnr, str)


def test_int_sysnr_single_digit_is_zero_padded(mod):
    component = _component(mod, sysnr=5)
    assert component.sysnr == "05"


def test_int_client_already_three_digits_is_not_altered(mod):
    component = _component(mod, client=999)
    assert component.client == "999"


def test_sysnr_unset_stays_none_not_coerced(mod):
    component = mod.SapRFCResourceComponent(
        ashost="sapecc01.acme.com", client="100", user="DAGSTER_RFC", passwd_env_var="SAP_RFC_PASSWORD",
    )
    assert component.sysnr is None


def test_conn_params_uses_the_corrected_string_values(mod, monkeypatch):
    monkeypatch.setenv("SAP_RFC_PASSWORD", "secret")
    component = _component(mod, sysnr=0, client=100)
    defs = component.build_defs(context=None)
    resource = defs.resources[component.resource_key]
    params = resource.conn_params()
    assert params["sysnr"] == "00"
    assert params["client"] == "100"


# --- real end-to-end reproduction of the reported bug, through Dagster's actual component- ---
# --- YAML resolution path (not just this component's own validator in isolation) ----------

def test_real_dagster_yaml_resolution_no_longer_crashes(mod, monkeypatch, tmp_path):
    """The exact failure a user hit: loading this component's own registry example
    (`client: "100"`, `sysnr: "00"`, both quoted) through a real `dg`-shaped component tree
    used to raise `pydantic.ValidationError: Input should be a valid string [type=string_type,
    input_value=0, input_type=int]` -- confirmed by reproducing it before this fix existed."""
    import yaml
    from dagster.components.testing.utils import create_defs_folder_sandbox

    import dagster_community_components as dcc
    monkeypatch.setattr(dcc, "SapRFCResourceComponent", mod.SapRFCResourceComponent, raising=False)

    attrs = {
        "type": "dagster_community_components.SapRFCResourceComponent",
        "attributes": {
            "resource_key": "sap_ecc",
            "ashost": "sapecc01.acme.com",
            "sysnr": "00",
            "client": "100",
            "user": "DAGSTER_RFC",
            "passwd_env_var": "SAP_RFC_PASSWORD",
            "lang": "EN",
        },
    }

    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "sap_test"
        defs_path.mkdir(parents=True, exist_ok=True)
        (defs_path / "defs.yaml").write_text(yaml.safe_dump(attrs))
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (component, defs):
            assert component.sysnr == "00"
            assert component.client == "100"
