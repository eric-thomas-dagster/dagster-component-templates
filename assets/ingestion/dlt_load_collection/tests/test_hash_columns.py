"""hash_columns: deterministic salted-SHA-256 pseudonymization, matching dlt's own
documented pattern (https://dlthub.com/docs/general-usage/customising-pipelines/
pseudonymizing_columns) but wired up as a config option instead of requiring a custom
Python module -- the whole premise of this component.

Exercised against a real local file (no network/paid call involved) for the end-to-end
check; the hashing/override logic itself is tested directly as a unit, matching how
write_disposition/schema_contract are tested elsewhere in this directory.
"""
import hashlib

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_make_hash_columns_map_is_deterministic_and_skips_missing_or_none(mod):
    fn = mod._make_hash_columns_map(["email", "ssn"], "salt123")
    row = {"id": 1, "email": "a@x.com", "name": "unhashed"}
    out = fn(dict(row))
    assert out["email"] == hashlib.sha256(("a@x.com" + "salt123").encode()).hexdigest()
    assert out["name"] == "unhashed"
    assert "ssn" not in out  # missing column: no KeyError, left absent

    assert fn({"email": "a@x.com"})["email"] == fn({"email": "a@x.com"})["email"]  # deterministic

    row_with_none = {"email": None}
    assert fn(row_with_none)["email"] is None  # None is left alone, not hashed to a string


def test_effective_hash_columns_component_default_and_per_resource_override(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        hash_columns=["email"], hash_salt="s",
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}}],
    )
    assert component._effective_hash_columns({"name": "r"}) == ["email"]
    assert component._effective_hash_columns({"name": "r", "hash_columns": ["ssn"]}) == ["ssn"]
    assert component._effective_hash_columns({"name": "r", "hash_columns": []}) == []  # explicit opt-out


def test_hash_salt_required_when_hash_columns_set(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        hash_columns=["email"],
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}}],
    )
    with pytest.raises(ValueError, match="hash_salt"):
        component.build_defs(context=None)


def test_hash_salt_required_when_only_a_per_resource_hash_columns_is_set(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "hash_columns": ["email"]}],
    )
    with pytest.raises(ValueError, match="hash_salt"):
        component.build_defs(context=None)


def test_hash_salt_env_var_unset_raises_clearly(mod, monkeypatch):
    monkeypatch.delenv("DLT_HASH_SALT_TEST_UNSET", raising=False)
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        hash_columns=["email"], hash_salt_env_var="DLT_HASH_SALT_TEST_UNSET",
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}}],
    )
    with pytest.raises(ValueError, match="DLT_HASH_SALT_TEST_UNSET"):
        component._build_resources()


def test_hash_columns_wired_into_rest_api_resource(mod):
    """rest_api resources aren't individually accessible objects before
    rest_api_source() builds the DltSource -- hashing is applied afterward via
    source.resources[name].add_map(...). No real HTTP call is made here (the
    resource is only built, never iterated), so this checks the pipe gained the
    extra step rather than fetching real data."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        hash_salt="s",
        rest_api_resources=[
            {"name": "hashed", "endpoint": {"path": "hashed"}, "hash_columns": ["email"]},
            {"name": "plain", "endpoint": {"path": "plain"}},
        ],
    )
    source, names = component._build_resources()
    assert sorted(names) == ["hashed", "plain"]
    hashed_steps = len(source.resources["hashed"]._pipe._steps)
    plain_steps = len(source.resources["plain"]._pipe._steps)
    assert hashed_steps == plain_steps + 1  # only "hashed" got the extra add_map step


def test_hash_columns_filesystem_end_to_end(mod, tmp_path):
    (tmp_path / "a.csv").write_text("id,email\n1,a@x.com\n2,b@x.com\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        hash_columns=["email"], hash_salt="pepper",
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    resources, names = component._build_resources()
    assert names == ["records"]
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    by_id = {r["id"]: r["email"] for r in rows}
    assert by_id[1] == hashlib.sha256(("a@x.com" + "pepper").encode()).hexdigest()
    assert by_id[2] == hashlib.sha256(("b@x.com" + "pepper").encode()).hexdigest()
    assert by_id[1] != "a@x.com"
