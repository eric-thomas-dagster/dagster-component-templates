"""Real, committed tests for a gap found in review feedback: one
SqlTransformComponent instance covered exactly one transform asset, so N
transforms meant N component instances. `transforms:` lets one instance
produce N assets, each a per-entry override on top of this instance's own
shared defaults (connection_url_env_var, group_name, etc.) -- same
mutually-exclusive single-vs-collection convention as external_sftp_path's
asset_key/remote_path vs paths:.

Real SQL against a real SQLite file through the component's own SQLAlchemy
path (return_dataframe=true), same convention as test_backfill_policy.py.
"""
import dagster as dg
import pandas as pd
import pytest
import sqlalchemy

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_url(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'warehouse.db'}"
    eng = sqlalchemy.create_engine(url)
    pd.DataFrame({"id": [1, 2, 3], "status": ["open", "closed", "open"]}).to_sql("orders", eng, index=False)
    pd.DataFrame({"id": [1, 2], "amount": [100, 200]}).to_sql("payments", eng, index=False)
    eng.dispose()
    monkeypatch.setenv("TEST_SQL_TRANSFORM_COLLECTION_URL", url)
    return url


def _comp(mod, **kw):
    args = dict(connection_url_env_var="TEST_SQL_TRANSFORM_COLLECTION_URL", return_dataframe=True)
    args.update(kw)
    return mod.SqlTransformComponent(**args)


def test_both_single_and_collection_set_is_rejected(mod, db_url):
    comp = _comp(mod, asset_name="orders_out", sql="SELECT * FROM orders", transforms=[
        {"asset_name": "a", "sql": "SELECT 1"},
    ])
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_single_nor_collection_set_is_rejected(mod, db_url):
    comp = _comp(mod)
    with pytest.raises(ValueError, match="asset_name/sql"):
        comp.build_defs(None)


def test_transform_entry_missing_asset_name_is_rejected(mod, db_url):
    comp = _comp(mod, transforms=[{"sql": "SELECT 1"}])
    with pytest.raises(ValueError, match="asset_name"):
        comp.build_defs(None)


def test_transforms_produce_one_asset_each_sharing_the_connection(mod, db_url):
    comp = _comp(mod, transforms=[
        {"asset_name": "open_orders", "sql": "SELECT * FROM orders WHERE status = 'open'"},
        {"asset_name": "payments_out", "sql": "SELECT * FROM payments"},
    ])
    defs = comp.build_defs(None)
    assets = list(defs.assets)
    assert len(assets) == 2

    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
    open_orders = result.asset_value(dg.AssetKey(["open_orders"]))
    payments_out = result.asset_value(dg.AssetKey(["payments_out"]))
    assert len(open_orders) == 2  # 2 open orders
    assert len(payments_out) == 2


def test_per_transform_overrides_apply_independently(mod, db_url):
    comp = _comp(mod, group_name="default_group", transforms=[
        {"asset_name": "a", "sql": "SELECT * FROM orders", "group_name": "group_a"},
        {"asset_name": "b", "sql": "SELECT * FROM payments"},  # inherits default_group
    ])
    defs = comp.build_defs(None)
    specs_by_key = {s.key: s for s in defs.resolve_all_asset_specs()}
    assert specs_by_key[dg.AssetKey(["a"])].group_name == "group_a"
    assert specs_by_key[dg.AssetKey(["b"])].group_name == "default_group"


def test_transform_entry_malformed_type_raises_a_clear_error(mod, db_url):
    """Pydantic's own type validation on transforms: List[Dict[str, Any]]
    already rejects a non-dict entry at construction time, before build_defs
    ever runs."""
    with pytest.raises(Exception, match="dict"):
        _comp(mod, transforms=["not-a-dict"])
