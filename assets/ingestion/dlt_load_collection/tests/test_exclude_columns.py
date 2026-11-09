"""'exclude_columns': drops named columns from every row before it reaches the
destination, via the same per-resource `add_map` mechanism as `hash_columns` (see
test_hash_columns.py) -- applied first in the chain, so a column in both
exclude_columns and hash_columns ends up excluded, not hashed-then-excluded.
"""
import sqlite3

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    row = {"id": 1, "ssn": "123-45-6789", "name": "kept"}
    out = fn(dict(row))
    assert "ssn" not in out
    assert out == {"id": 1, "name": "kept"}


def test_effective_exclude_columns_component_default_and_per_resource_override(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        exclude_columns=["ssn"],
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}}],
    )
    assert component._effective_exclude_columns({"name": "r"}) == ["ssn"]
    assert component._effective_exclude_columns({"name": "r", "exclude_columns": ["other"]}) == ["other"]
    assert component._effective_exclude_columns({"name": "r", "exclude_columns": []}) == []  # explicit opt-out


def test_exclude_columns_wired_into_rest_api_resource(mod):
    """No real HTTP call here (the resource is only built, never iterated) -- checks
    the pipe gained the extra add_map step, matching how hash_columns's own rest_api
    test checks pipe step counts."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[
            {"name": "excluded", "endpoint": {"path": "excluded"}, "exclude_columns": ["ssn"]},
            {"name": "plain", "endpoint": {"path": "plain"}},
        ],
    )
    source, names = component._build_resources()
    excluded_steps = len(source.resources["excluded"]._pipe._steps)
    plain_steps = len(source.resources["plain"]._pipe._steps)
    assert excluded_steps == plain_steps + 1


def test_exclude_columns_filesystem_end_to_end(mod, tmp_path):
    (tmp_path / "a.csv").write_text("id,ssn,name\n1,123-45-6789,alice\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        exclude_columns=["ssn"],
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    resources, names = component._build_resources()
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    assert "ssn" not in rows[0]
    assert rows[0]["name"] == "alice"


def test_exclude_columns_sql_database_end_to_end(mod, tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, ssn TEXT, name TEXT)")
    conn.execute("INSERT INTO items VALUES (1, '123-45-6789', 'alice')")
    conn.commit()
    conn.close()

    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database",
        connection_string=f"sqlite:///{db_path}",
        exclude_columns=["ssn"],
        sql_resources=[{"name": "items"}],
    )
    resources, names = component._build_resources()
    rows = list(resources[0])
    assert "ssn" not in rows[0]
    assert rows[0]["name"] == "alice"


def test_a_column_in_both_exclude_columns_and_hash_columns_ends_up_excluded_not_hashed(mod, tmp_path):
    (tmp_path / "a.csv").write_text("id,email\n1,a@x.com\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        exclude_columns=["email"], hash_columns=["email"], hash_salt="s",
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    resources, names = component._build_resources()
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    assert "email" not in rows[0]  # excluded, not a hash digest


def test_exclude_columns_rejects_non_list(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "exclude_columns": "ssn"}],
    )
    with pytest.raises(ValueError, match="exclude_columns"):
        component.build_defs(context=None)


def test_exclude_columns_requires_no_salt_unlike_hash_columns(mod, tmp_path):
    """exclude_columns has no secondary salt/key requirement -- it's pure removal, not
    deterministic hashing, so there's nothing to validate beyond the list shape."""
    (tmp_path / "a.csv").write_text("id,ssn\n1,x\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        exclude_columns=["ssn"],
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    component.build_defs(context=None)  # does not raise
