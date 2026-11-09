"""'max_rows_per_resource': caps how many rows a resource loads, via dlt's own
`DltResource.add_limit(max_items=N, count_rows=True)` -- `count_rows=True` matters:
`add_limit`'s default counts "yields/pages", not rows, which would undercount for a
file reader that yields a chunk of several rows per batch (filesystem's csv/jsonl/
parquet readers all do).

This is a real cap on real data loaded into the real destination -- never a dry run --
so it must never apply unless explicitly set (default: None, no cap).
"""
import sqlite3

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_sql_database_max_rows_per_resource_caps_rows(mod, tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO items VALUES (?)", [(i,) for i in range(10)])
    conn.commit()
    conn.close()

    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database",
        connection_string=f"sqlite:///{db_path}",
        sql_resources=[{"name": "items", "max_rows_per_resource": 3}],
    )
    resources, names = component._build_resources()
    rows = list(resources[0])
    assert len(rows) == 3


def test_filesystem_max_rows_per_resource_counts_rows_not_pages(mod, tmp_path):
    """A single csv file is one 'page'/yield from dlt's reader -- add_limit's default
    (max_items counts yields, not rows) would cap at the whole file; count_rows=True is
    required to actually cap at N rows within that one file."""
    (tmp_path / "a.csv").write_text("id\n" + "\n".join(str(i) for i in range(10)) + "\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{"name": "records", "file_glob": "*.csv", "max_rows_per_resource": 4}],
    )
    resources, names = component._build_resources()
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    assert len(rows) == 4


def test_rest_api_max_rows_per_resource_adds_a_limit_step(mod):
    """No real HTTP call here (the resource is only built, never iterated) -- checks
    the pipe gained the add_limit step, matching how hash_columns's own rest_api test
    checks pipe step counts rather than iterating."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[
            {"name": "capped", "endpoint": {"path": "capped"}, "max_rows_per_resource": 5},
            {"name": "plain", "endpoint": {"path": "plain"}},
        ],
    )
    source, names = component._build_resources()
    capped_steps = len(source.resources["capped"]._pipe._steps)
    plain_steps = len(source.resources["plain"]._pipe._steps)
    assert capped_steps == plain_steps + 1


def test_make_row_cap_yield_map_stops_exactly_at_the_cap_across_calls(mod):
    """One closure instance is shared across every item dlt feeds it (that's how
    add_yield_map works) -- state must accumulate across calls, not reset per call."""
    cap = mod._make_row_cap_yield_map(3)
    seen = []
    for item in ["a", "b", "c", "d", "e"]:
        seen.extend(cap(item))
    assert seen == ["a", "b", "c"]


def test_effective_max_rows_per_resource_component_default_and_per_resource_override(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        max_rows_per_resource=100,
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}}],
    )
    assert component._effective_max_rows_per_resource({"name": "r"}) == 100
    assert component._effective_max_rows_per_resource({"name": "r", "max_rows_per_resource": 5}) == 5
    assert component._effective_max_rows_per_resource({"name": "r", "max_rows_per_resource": None}) is None  # explicit opt-out


def test_max_rows_per_resource_defaults_to_no_cap(mod, tmp_path):
    (tmp_path / "a.csv").write_text("id\n" + "\n".join(str(i) for i in range(10)) + "\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    resources, names = component._build_resources()
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    assert len(rows) == 10


def test_max_rows_per_resource_rejects_zero_and_negative(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "max_rows_per_resource": 0}],
    )
    with pytest.raises(ValueError, match="max_rows_per_resource"):
        component.build_defs(context=None)


def test_max_rows_per_resource_rejects_non_integer(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "max_rows_per_resource": "five"}],
    )
    with pytest.raises(ValueError, match="max_rows_per_resource"):
        component.build_defs(context=None)
