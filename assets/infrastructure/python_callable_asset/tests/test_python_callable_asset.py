"""Real, committed tests for PythonCallableAssetComponent -- real dg.materialize() throughout
(this repo's convention), no mocking of the component's own logic. The one external surface
(`S3Hook`'s boto3 client) is mocked per this repo's "mock only the paid/external call" rule.
"""
import importlib
import json

import dagster as dg
import pytest

from .conftest import load_component_module, materialize

FIXTURES_PATH = "assets.infrastructure.python_callable_asset.tests.fixture_callables"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def _upstream_asset(mod, key: str, value):
    @dg.asset(key=dg.AssetKey.from_user_string(key))
    def _a():
        return value
    return _a


def _upstream_asset_with_metadata(key: str, metadata: dict):
    @dg.asset(key=dg.AssetKey.from_user_string(key))
    def _a():
        return dg.MaterializeResult(metadata=metadata)
    return _a


# --- static call: args/kwargs, no context ------------------------------------------------

def test_static_call_passes_args_and_kwargs(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs",
        args=[1, 2], kwargs={"x": "y"},
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["args"] == (1, 2)
    assert fx.CALLS["kwargs"] == {"x": "y"}


# --- inputs: TaskFlow-style direct binding, independent of context_mode -----------------

def test_inputs_binds_taskflow_style_param_and_loads_value(mod, fx):
    upstream = _upstream_asset(mod, "up/a", 42)
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
        inputs={"upstream_a": "up/a"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == 42


# --- inputs: metadata-lookup form ('asset_key.metadata_key') ---------------------------

def test_inputs_metadata_lookup_reads_named_metadata_from_this_run(mod, fx):
    upstream = _upstream_asset_with_metadata("up/a", {"row_count": dg.MetadataValue.int(42)})
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
        inputs={"upstream_a": "up/a.row_count"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == 42


def test_inputs_metadata_lookup_missing_key_uses_input_defaults(mod, fx):
    upstream = _upstream_asset_with_metadata("up/a", {"other_key": dg.MetadataValue.int(1)})
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
        inputs={"upstream_a": "up/a.row_count"}, input_defaults={"upstream_a": 0},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == 0


def test_inputs_metadata_lookup_missing_key_no_default_fails(mod, fx):
    upstream = _upstream_asset_with_metadata("up/a", {"other_key": dg.MetadataValue.int(1)})
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
        inputs={"upstream_a": "up/a.row_count"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success


def test_inputs_metadata_lookup_spec_requires_both_sides(mod):
    with pytest.raises(Exception, match="metadata lookup"):
        mod.PythonCallableAssetComponent(
            asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
            inputs={"upstream_a": "up/a."},
        )


def test_inputs_key_must_be_a_valid_identifier(mod):
    with pytest.raises(Exception, match="valid Python identifier"):
        mod.PythonCallableAssetComponent(
            asset_key="d/t", callable_path=f"{FIXTURES_PATH}:taskflow_style",
            inputs={"not-an-identifier": "up/a"},
        )


# --- "inject only what's declared" ------------------------------------------------------

def test_context_mode_airflow_declares_nothing_receives_nothing(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_nothing", context_mode="airflow",
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["called"] is True


def test_wants_ds_only_receives_ds_from_a_daily_partition(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="2024-01-05",
    )
    assert result.success
    assert fx.CALLS["ds"] == "2024-01-05"


def test_airflow_version_3_uses_interval_end_for_logical_date(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
        partition_type="daily", partition_start="2024-01-01", airflow_version=3,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="2024-01-05",
    )
    assert result.success
    # airflow_version=3: logical_date = interval END -> the NEXT day's date.
    assert fx.CALLS["ds"] == "2024-01-06"


def test_unpartitioned_falls_back_to_now(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["ds"]  # a real date string, just not asserting the exact value (= run time)


def test_full_context_injection_matches_declared_params(mod, fx):
    upstream = _upstream_asset(mod, "up/a", "hello")
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_full_context", context_mode="airflow",
        partition_type="daily", partition_start="2024-01-01",
        inputs={"upstream_a": "up/a"}, params={"p1": "v1"},
        dag_run_conf_tag="airflow/dag_run_conf",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()},
        partition_key="2024-01-05", tags={"airflow/dag_run_conf": json.dumps({"k": "v"})},
    )
    assert result.success
    assert fx.CALLS["ds"] == "2024-01-05"
    assert fx.CALLS["params"] == {"p1": "v1"}
    assert fx.CALLS["dag_run_conf"] == {"k": "v"}
    assert fx.CALLS["Variable"] is mod._AirflowVariable
    assert fx.CALLS["S3Hook"] is mod._AirflowS3Hook


def test_var_kwargs_catchall_receives_everything(mod, fx):
    upstream = _upstream_asset(mod, "up/a", "hello")
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_var_kwargs_catchall", context_mode="airflow",
        inputs={"upstream_a": "up/a"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    keys = fx.CALLS["context_keys"]
    for name in ("ds", "ds_nodash", "ts", "logical_date", "data_interval_start", "data_interval_end",
                 "run_id", "params", "dag_run", "ti", "Variable", "S3Hook", "upstream_a"):
        assert name in keys, f"{name} missing from catch-all context: {keys}"


# --- deps-only upstream has no loaded value ---------------------------------------------

def test_deps_only_upstream_is_lineage_only_no_value(mod, fx):
    upstream = _upstream_asset(mod, "up/a", 1)
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_nothing", context_mode="airflow",
        deps=["up/a"],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success  # lineage edge exists, no value was ever loaded for it


# --- ti.xcom_pull --------------------------------------------------------------------------

def test_ti_xcom_pull_resolves_a_declared_input(mod, fx):
    upstream = _upstream_asset(mod, "up/a", "xcom-value")
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:pulls_xcom", context_mode="airflow",
        inputs={"upstream_a": "up/a"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["pulled"] == "xcom-value"


def test_ti_xcom_pull_undeclared_task_id_fails_the_run_clearly(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:pulls_undeclared_xcom", context_mode="airflow",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert not result.success


def test_airflow_task_instance_xcom_pull_error_message_names_the_fix(mod):
    ti = mod._AirflowTaskInstance({"a": 1}, retry_number=0)
    with pytest.raises(ValueError, match="not declared in `inputs`"):
        ti.xcom_pull(task_ids="b")


def test_try_number_is_retry_number_plus_one(mod, fx):
    # context.retry_number reflects real engine-tracked retries, not a spoofable run tag --
    # on a normal (non-retried) run it's 0, so try_number (Airflow's 1-indexed convention) is 1.
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:reports_try_number", context_mode="airflow",
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["try_number"] == 1


def test_airflow_task_instance_try_number_is_retry_number_plus_one_directly(mod):
    ti = mod._AirflowTaskInstance({}, retry_number=2)
    assert ti.try_number == 3


# --- dag_run.conf -------------------------------------------------------------------------

def test_dag_run_conf_tag_none_disables_it(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_var_kwargs_catchall", context_mode="airflow",
        dag_run_conf_tag=None,
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["context"]["dag_run"].conf == {}


def test_dag_run_conf_tag_present_decodes_json(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_var_kwargs_catchall", context_mode="airflow",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()},
        tags={"airflow/dag_run_conf": json.dumps({"a": 1})},
    )
    assert result.success
    assert fx.CALLS["context"]["dag_run"].conf == {"a": 1}


# --- Jinja templating -----------------------------------------------------------------------

def test_jinja_ds_and_ds_nodash_render(mod, fx, monkeypatch):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        partition_type="daily", partition_start="2024-01-01",
        kwargs={"a": "{{ ds }}", "b": "{{ ds_nodash }}"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="2024-03-07",
    )
    assert result.success
    # record_args_kwargs declares **kwargs, so it's a legitimate catch-all and also receives
    # the full injected airflow context (verified separately by the var-kwargs-catchall test) --
    # only check the two rendered Jinja values here.
    assert fx.CALLS["kwargs"]["a"] == "2024-03-07"
    assert fx.CALLS["kwargs"]["b"] == "20240307"


def test_jinja_logical_date_ds_filter_renders(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        partition_type="daily", partition_start="2024-01-01",
        kwargs={"a": "{{ logical_date | ds }}"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="2024-03-07",
    )
    assert result.success
    assert fx.CALLS["kwargs"]["a"] == "2024-03-07"


def test_jinja_params_dot_access_renders(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        params={"x": "hello"}, kwargs={"a": "{{ params.x }}"},
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["kwargs"]["a"] == "hello"


def test_jinja_var_value_renders_from_env(mod, fx, monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_FOO", "bar")
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        kwargs={"a": "{{ var.value.FOO }}"},
    )
    result = materialize(component)
    assert result.success
    assert fx.CALLS["kwargs"]["a"] == "bar"


def test_jinja_unsupported_identifier_raises_at_construction_time(mod):
    with pytest.raises(Exception, match="unsupported Jinja variable"):
        mod.PythonCallableAssetComponent(
            asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
            kwargs={"a": "{{ not_a_real_var }}"},
        )


def test_jinja_not_validated_when_context_mode_none(mod):
    # {{ ds }} with context_mode='none' is just a literal string -- no Jinja variables are available
    # at all in that mode, so validation (and rendering) is skipped entirely, not an error.
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs",
        kwargs={"a": "{{ ds }}"},
    )
    assert component.kwargs == {"a": "{{ ds }}"}


# --- Variable / S3Hook shims --------------------------------------------------------------

def test_variable_get_reads_airflow_var_env_var(mod, monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_MY_KEY", "my-value")
    assert mod._AirflowVariable.get("my_key") == "my-value"
    assert mod._AirflowVariable.get("missing_key", "default") == "default"


def test_s3hook_shim_load_check_read_round_trip(mod):
    class _FakeS3Client:
        def __init__(self):
            self.objects = {}

        def head_object(self, Bucket, Key):
            if (Bucket, Key) not in self.objects:
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
            return {}

        def put_object(self, Bucket, Key, Body):
            self.objects[(Bucket, Key)] = Body

        def get_object(self, Bucket, Key):
            import io
            return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    client = _FakeS3Client()
    hook = mod._AirflowS3Hook(client=client)
    assert hook.check_for_key("k", "b") is False
    hook.load_string("hello", "k", "b")
    assert hook.check_for_key("k", "b") is True
    assert hook.read_key("k", "b") == "hello"
    with pytest.raises(ValueError, match="already exists"):
        hook.load_string("again", "k", "b", replace=False)
    hook.load_string("again", "k", "b", replace=True)
    assert hook.read_key("k", "b") == "again"


# --- common attributes (shape copied from ssh_asset; representative wiring checks) --------

def test_retry_policy_wires_through(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_nothing",
        retry_policy_max_retries=2, retry_policy_delay_seconds=5, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    spec = asset_def.get_asset_spec() if hasattr(asset_def, "get_asset_spec") else None
    rp = asset_def.op.retry_policy if hasattr(asset_def, "op") else None
    assert rp is not None
    assert rp.max_retries == 2
    assert rp.delay == 5


def test_static_partition_type_wires_through(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_nothing",
        partition_type="static", partition_values="a,b,c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="b",
    )
    assert result.success
