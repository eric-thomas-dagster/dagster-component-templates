"""on_schema_change maps to dlt's native schema_contract. Exercised against a REAL
local DuckDB pipeline (no network, no credentials) -- there's no way to meaningfully
test schema-contract enforcement without actually running dlt's normalize step,
and `filesystem()` + a local bucket_url + unset `destination` (in-memory/local
DuckDB) involves no "paid call" to mock. Each test uses a fresh, random
pipeline_name and redirects dlt's own state/data dirs into tmp_path so nothing
touches the real machine's ~/.dlt or working directory.

VERIFIED (dlt 1.30.0) before writing the mapping: `schema_contract={"tables":
"freeze", ...}` rejects creating ANY table dlt's local pipeline state doesn't
already know about -- including a brand new resource's very first materialization
ever. So `tables` is 'evolve' in every on_schema_change mode here, never 'freeze'
(see the comment on _SCHEMA_CONTRACT_MAP in component.py)."""
import uuid

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _isolate_dlt_state(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / ".dlt_data"))


def _run(mod, asset_name, tmp_path, glob, **component_kwargs):
    """Builds and runs a pipeline the same way build_defs's asset body would
    (pipeline_name = f"{asset_name}_pipeline") -- callers that later materialize a
    second component with the same asset_name land in the SAME dlt pipeline/schema,
    which is the whole point: establish a schema here, then challenge it there."""
    component = mod.DltYamlSourceComponent(
        asset_name=asset_name, source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{"name": "t", "file_glob": glob}],
        **component_kwargs,
    )
    run_arg, _ = component._build_resources()
    pipeline = component._build_pipeline(f"{asset_name}_pipeline", "d")
    return pipeline.run(run_arg)


def _materialize(component):
    """Goes through the real asset body (unlike `_run`, which calls pipeline.run()
    directly) -- this is what actually exercises build_defs's try/except around
    PipelineStepFailed that turns it into a clear, named ValueError."""
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_new_column_allowed_under_default_allow_all(tmp_path):
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    _run(mod_, name, tmp_path, "a.csv")
    (tmp_path / "b.csv").write_text("id,amount,new_col\n2,20,x\n")
    info = _run(mod_, name, tmp_path, "b.csv")  # should not raise
    assert not info.has_failed_jobs


def test_new_column_blocked_under_block_all(tmp_path):
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    _run(mod_, name, tmp_path, "a.csv", on_schema_change="block_all")
    (tmp_path / "b.csv").write_text("id,amount,new_col\n2,20,x\n")
    component = mod_.DltYamlSourceComponent(
        asset_name=name, source_type="filesystem", dataset_name="d",
        filesystem_bucket_url=str(tmp_path), on_schema_change="block_all",
        filesystem_resources=[{"name": "t", "file_glob": "b.csv"}],
    )
    with pytest.raises(Exception, match="schema change rejected") as exc_info:
        _materialize(component)
    msg = str(exc_info.value)
    assert "table 't'" in msg
    assert "new_col" in msg
    assert "columns" in msg


def test_type_change_blocked_under_allow_new_columns(tmp_path):
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    _run(mod_, name, tmp_path, "a.csv", on_schema_change="allow_new_columns")
    (tmp_path / "b.csv").write_text("id,amount\n2,not_a_number\n")
    component = mod_.DltYamlSourceComponent(
        asset_name=name, source_type="filesystem", dataset_name="d",
        filesystem_bucket_url=str(tmp_path), on_schema_change="allow_new_columns",
        filesystem_resources=[{"name": "t", "file_glob": "b.csv"}],
    )
    with pytest.raises(Exception, match="schema change rejected") as exc_info:
        _materialize(component)
    assert "data_type" in str(exc_info.value)


def test_new_column_allowed_under_allow_new_columns(tmp_path):
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    _run(mod_, name, tmp_path, "a.csv", on_schema_change="allow_new_columns")
    (tmp_path / "b.csv").write_text("id,amount,new_col\n2,20,x\n")
    info = _run(mod_, name, tmp_path, "b.csv", on_schema_change="allow_new_columns")
    assert not info.has_failed_jobs


def test_first_ever_run_succeeds_under_block_all(tmp_path):
    """The bug the suggested mapping had: a brand new resource's very first run must
    always be able to create its own table, even under the strictest mode."""
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    info = _run(mod_, name, tmp_path, "a.csv", on_schema_change="block_all")
    assert not info.has_failed_jobs


def test_per_resource_on_schema_change_overrides_the_component_default(tmp_path):
    mod_ = load_component_module()
    name = f"schema_test_{uuid.uuid4().hex}"
    (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
    component = mod_.DltYamlSourceComponent(
        asset_name=name, source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        on_schema_change="allow_all",  # component-level default: permissive
        filesystem_resources=[{"name": "t", "file_glob": "a.csv", "on_schema_change": "block_all"}],
    )
    run_arg, _ = component._build_resources()
    pipeline = component._build_pipeline(f"{name}_pipeline", "d")
    pipeline.run(run_arg)

    (tmp_path / "b.csv").write_text("id,amount,new_col\n2,20,x\n")
    component2 = mod_.DltYamlSourceComponent(
        asset_name=name, source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        on_schema_change="allow_all",
        filesystem_resources=[{"name": "t", "file_glob": "b.csv", "on_schema_change": "block_all"}],
    )
    run_arg2, _ = component2._build_resources()
    pipeline2 = component2._build_pipeline(f"{name}_pipeline", "d")
    # Calling pipeline.run() directly here (not through build_defs/_materialize), so
    # this sees dlt's own raw PipelineStepFailed, not this component's cleaned-up
    # ValueError -- the point of this test is the per-resource override taking
    # effect at all, which the other tests already cover the error-message cleanup for.
    with pytest.raises(Exception, match="contract_mode=freeze"):
        pipeline2.run(run_arg2)


def test_invalid_on_schema_change_value_raises_clearly(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url="/tmp/doesnt_matter",
        filesystem_resources=[{"name": "t", "file_glob": "*.csv", "on_schema_change": "sometimes"}],
    )
    with pytest.raises(ValueError, match="on_schema_change"):
        component.build_defs(context=None)
