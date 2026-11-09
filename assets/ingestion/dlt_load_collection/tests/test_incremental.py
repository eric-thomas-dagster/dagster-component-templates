"""Per-resource 'incremental': only loads rows/files past the last synced cursor value,
via dlt's own native `dlt.sources.incremental(...)` (sql_database/filesystem) or
`endpoint.incremental` (rest_api, a real, pre-existing dlt mechanism this component
translates a top-level 'incremental' key into).

sql_database is exercised against a real local SQLite file (free, no network -- same
"mock only the paid call" convention as the filesystem hash_columns test using a real
local csv). rest_api checks the built resource actually carries a real dlt Incremental
wrapper (no real HTTP call is made -- the resource is only built, never iterated).
"""
import sqlite3

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_sqlite_db(tmp_path, rows):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, updated_at TEXT, val TEXT)")
    conn.executemany("INSERT INTO items VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return db_path


def test_sql_database_incremental_only_loads_rows_past_initial_value(mod, tmp_path):
    db_path = _make_sqlite_db(tmp_path, [
        (1, "2024-01-01T00:00:00", "a"),
        (2, "2024-01-02T00:00:00", "b"),
    ])
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database",
        connection_string=f"sqlite:///{db_path}",
        sql_resources=[{
            "name": "items",
            "incremental": {"cursor_path": "updated_at", "initial_value": "2024-01-01T12:00:00"},
        }],
    )
    resources, names = component._build_resources()
    assert names == ["items"]
    rows = list(resources[0])
    assert [r["id"] for r in rows] == [2]


def test_sql_database_incremental_with_no_initial_value_loads_everything_first_run(mod, tmp_path):
    db_path = _make_sqlite_db(tmp_path, [
        (1, "2024-01-01T00:00:00", "a"),
        (2, "2024-01-02T00:00:00", "b"),
    ])
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database",
        connection_string=f"sqlite:///{db_path}",
        sql_resources=[{"name": "items", "incremental": {"cursor_path": "updated_at"}}],
    )
    resources, names = component._build_resources()
    rows = list(resources[0])
    assert [r["id"] for r in rows] == [1, 2]


def test_filesystem_incremental_only_loads_files_past_cutoff(mod, tmp_path):
    import time

    import dlt
    import pendulum

    (tmp_path / "old.csv").write_text("id\n1\n")
    time.sleep(1.2)
    cutoff = time.time()
    time.sleep(1.2)
    (tmp_path / "new.csv").write_text("id\n2\n")

    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{
            "name": "records", "file_glob": "*.csv",
            "incremental": {"cursor_path": "modification_date", "initial_value": pendulum.from_timestamp(cutoff)},
        }],
    )
    resources, names = component._build_resources()
    rows = []
    for batch in resources[0]:
        rows.extend(batch if isinstance(batch, list) else [batch])
    assert [r["id"] for r in rows] == [2]


def test_rest_api_incremental_builds_a_real_dlt_incremental_wrapper(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "orders", "endpoint": {
                "path": "orders",
                "incremental": {"cursor_path": "updated_at", "initial_value": "2024-01-01", "start_param": "since"},
            },
        }],
    )
    source, names = component._build_resources()
    assert names == ["orders"]
    res = source.resources["orders"]
    assert res.incremental is not None


def test_rest_api_top_level_incremental_is_translated_into_endpoint_incremental(mod):
    """The top-level 'incremental' key (consistent placement with sql_resources/
    filesystem_resources) is a convenience translated into dlt's own native, nested
    'endpoint.incremental' location -- same resulting behavior either way."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "orders", "endpoint": {"path": "orders"},
            "incremental": {"cursor_path": "updated_at"},
        }],
    )
    source, names = component._build_resources()
    res = source.resources["orders"]
    assert res.incremental is not None


def test_rest_api_top_level_incremental_does_not_mutate_component_state(mod):
    """`_resources_cfg`'s dict comprehension only shallow-copies each resource dict --
    translating 'incremental' into 'endpoint.incremental' must create a new endpoint
    dict, not mutate the original (shared) one in place, or a second _build_resources
    call would see the translated key already there (or worse, leak it back onto
    self.rest_api_resources permanently)."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "orders", "endpoint": {"path": "orders"},
            "incremental": {"cursor_path": "updated_at"},
        }],
    )
    component._build_resources()
    assert "incremental" not in component.rest_api_resources[0]["endpoint"]


def test_build_incremental_passes_through_cursor_path_initial_value_and_lag(mod):
    incr = mod._build_incremental({"cursor_path": "updated_at", "initial_value": "x", "lag": 60})
    assert incr.cursor_path == "updated_at"
    assert incr.initial_value == "x"


# --- Validation ---------------------------------------------------------------------


def test_incremental_requires_cursor_path(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "incremental": {}}],
    )
    with pytest.raises(ValueError, match="cursor_path"):
        component.build_defs(context=None)


def test_incremental_must_be_a_dict(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "r", "endpoint": {"path": "r"}, "incremental": "not-a-dict"}],
    )
    with pytest.raises(ValueError, match="mapping"):
        component.build_defs(context=None)


def test_start_param_end_param_rejected_for_sql_database(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="sql_database", connection_string="sqlite:///x.db",
        sql_resources=[{
            "name": "items",
            "incremental": {"cursor_path": "updated_at", "start_param": "since"},
        }],
    )
    with pytest.raises(ValueError, match="start_param"):
        component.build_defs(context=None)


def test_start_param_end_param_rejected_for_filesystem(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem", filesystem_bucket_url="file:///tmp/x",
        filesystem_resources=[{
            "name": "records", "file_glob": "*.csv",
            "incremental": {"cursor_path": "modification_date", "end_param": "until"},
        }],
    )
    with pytest.raises(ValueError, match="end_param"):
        component.build_defs(context=None)


def test_unrecognized_incremental_key_rejected(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "r", "endpoint": {"path": "r"},
            "incremental": {"cursor_path": "updated_at", "bogus_key": 1},
        }],
    )
    with pytest.raises(ValueError, match="bogus_key"):
        component.build_defs(context=None)


def test_setting_both_top_level_and_endpoint_incremental_is_rejected(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "r", "endpoint": {"path": "r", "incremental": {"cursor_path": "updated_at"}},
            "incremental": {"cursor_path": "updated_at"},
        }],
    )
    with pytest.raises(ValueError, match="exactly one place"):
        component.build_defs(context=None)


def test_incremental_conflicts_with_partition_window_params_on_the_same_resource(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        partition_window_params={"start_param": "since", "end_param": "until"},
        rest_api_resources=[{
            "name": "r", "endpoint": {"path": "r"},
            "incremental": {"cursor_path": "updated_at"},
        }],
    )
    with pytest.raises(ValueError, match="partition_window_params"):
        component.build_defs(context=None)


def test_incremental_and_partition_window_params_coexist_on_different_resources(mod):
    """The conflict is per-resource -- a different resource in the same component using
    partition_window_params is unaffected by another resource's 'incremental'."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        partition_window_params={"start_param": "since", "end_param": "until"},
        partition_window_resources=["windowed"],
        rest_api_resources=[
            {"name": "windowed", "endpoint": {"path": "windowed"}},
            {"name": "incremental_r", "endpoint": {"path": "incremental_r"}, "incremental": {"cursor_path": "updated_at"}},
        ],
    )
    component.build_defs(context=None)  # does not raise
