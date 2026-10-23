"""Real, committed test for a gap a demo-builder hit: a fresh Iceberg
warehouse/namespace failed on every asset's FIRST write with
pyiceberg.exceptions.NoSuchNamespaceError -- PyArrowIcebergIOManager checks
the namespace exists (`catalog.list_namespaces`) but never creates it.

Reproduced for real against a real local PyIceberg SqlCatalog (sqlite:///
+ a file:// warehouse) before writing the fix, and this test confirms both
directions: bootstrap_namespace=True (default) actually materializes
against a brand-new namespace, and bootstrap_namespace=False reproduces the
original crash (so the flag's own behavior is pinned down, not just the
happy path).

Requires `dagster-iceberg` + `pyiceberg` installed; skipped otherwise (an
optional backend, same convention as this repo's other optional-dependency
component tests).
"""
import importlib.util
import pathlib

import pytest

pytest.importorskip("dagster_iceberg")
pytest.importorskip("pyiceberg")

import dagster as dg
import pyarrow as pa


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("iceberg_io_manager_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
IcebergIOManagerComponent = mod.IcebergIOManagerComponent


@pytest.fixture
def fresh_catalog_properties(tmp_path):
    db_path = tmp_path / "catalog.db"
    warehouse = tmp_path / "warehouse"
    return {"catalog_uri": f"sqlite:///{db_path}", "warehouse": f"file://{warehouse}"}


def _materialize_one_table(io_manager, namespace="analytics_test"):
    @dg.asset(io_manager_def=io_manager)
    def my_table() -> pa.Table:
        return pa.table({"a": [1, 2, 3]})

    with dg.instance_for_test() as instance:
        return dg.materialize([my_table], instance=instance, raise_on_error=False)


def test_fresh_namespace_write_succeeds_with_bootstrap_enabled(fresh_catalog_properties):
    comp = IcebergIOManagerComponent(
        namespace="analytics_test", bootstrap_namespace=True, **fresh_catalog_properties,
    )
    io_manager = comp.build_defs(None).resources["io_manager"]
    result = _materialize_one_table(io_manager, namespace="analytics_test")
    assert result.success


def test_fresh_namespace_write_fails_with_bootstrap_disabled(fresh_catalog_properties):
    """Confirms this is a real, reproducible crash in the first place -- not a
    hypothetical -- and that bootstrap_namespace=False genuinely opts out
    rather than silently bootstrapping anyway."""
    comp = IcebergIOManagerComponent(
        namespace="analytics_test", bootstrap_namespace=False, **fresh_catalog_properties,
    )
    io_manager = comp.build_defs(None).resources["io_manager"]
    result = _materialize_one_table(io_manager, namespace="analytics_test")
    assert result.success is False
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "NoSuchNamespaceError" in failure.event_specific_data.error.to_string()


def test_bootstrap_is_idempotent_across_repeated_defs_loads(fresh_catalog_properties):
    """create_namespace_if_not_exists must not raise on a namespace that
    already exists -- e.g. a second `dg dev` reload against the same catalog."""
    comp = IcebergIOManagerComponent(namespace="analytics_test", **fresh_catalog_properties)
    comp.build_defs(None)  # first load creates the namespace
    defs2 = comp.build_defs(None)  # second load must not raise
    result = _materialize_one_table(defs2.resources["io_manager"], namespace="analytics_test")
    assert result.success
