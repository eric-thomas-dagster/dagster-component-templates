"""`column_lineage` becomes real `dagster/column_lineage` metadata on the asset's materialization."""
import importlib.util
import re
import warnings
from pathlib import Path

import pandas as pd
import pytest

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
BEGIN, END = "# BEGIN column-lineage helper", "# END column-lineage helper"


def _load(rel):
    p = ROOT / rel / "component.py"
    spec = importlib.util.spec_from_file_location("lin_" + p.parent.name, p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _users():
    return [p for p in sorted(ROOT.glob("assets/*/*/component.py")) if BEGIN in p.read_text()]


def test_helper_copies_are_identical_and_wired_into_the_asset():
    blocks = {}
    for p in _users():
        s = p.read_text()
        blocks.setdefault(re.search(rf"{BEGIN}.*?{END}", s, re.S).group(0), []).append(p)
        assert "_column_lineage_md = _column_lineage_metadata(" in s, p
        assert re.search(r"if _column_lineage_md:\n\s+\w+\.add_output_metadata\(_column_lineage_md\)", s), p
    assert len(blocks) == 1 and len(_users()) == 255


def test_every_declaring_component_either_applies_or_never_declared_it():
    unwired = [str(p.parent) for p in ROOT.glob("assets/*/*/component.py")
               if re.search(r"^\s+column_lineage\s*:", p.read_text(), re.M)
               and not re.search(r"TableColumnLineage|column_lineage\(", p.read_text())]
    assert unwired == [], unwired


def _helper():
    return _load("assets/transforms/select_records")._column_lineage_metadata


class C:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _cols(md):
    return {k: [(d.asset_key.to_user_string(), d.column_name) for d in v] for k, v in md["dagster/column_lineage"].column_lineage.deps_by_column.items()}


def test_single_upstream_from_upstream_asset_key_or_the_only_dep():
    h = _helper()
    assert _cols(h(C(column_lineage={"total": ["price", "qty"]}, upstream_asset_key="orders"))) == {"total": [("orders", "price"), ("orders", "qty")]}
    assert _cols(h(C(column_lineage={"id": "id"}, deps=["raw/customers"]))) == {"id": [("raw/customers", "id")]}
    assert h(C(column_lineage=None, upstream_asset_key="x")) == {}


def test_several_upstream_assets_need_explicit_keys_and_an_unresolvable_mapping_warns():
    h = _helper()
    c = C(column_lineage={"v": ["a:price", "b:qty"]}, upstream_asset_key=["a", "b"])
    assert _cols(h(c)) == {"v": [("a", "price"), ("b", "qty")]}
    with pytest.warns(UserWarning, match="column_lineage ignored"):
        assert h(C(column_lineage={"v": ["price"]}, upstream_asset_key=["a", "b"])) == {}
    with pytest.warns(UserWarning, match="column_lineage ignored"):
        assert h(C(column_lineage={"v": ["price"]})) == {}  # a source with no upstream asset


def test_materialization_carries_the_lineage():
    mod = _load("assets/transforms/select_records")

    @dg.asset(name="ranked_customers")
    def src():
        return pd.DataFrame({"id": range(5), "score": range(5)})

    c = mod.SelectRecordsComponent(asset_name="top", upstream_asset_key="ranked_customers", mode="head", n=2,
                                   column_lineage={"id": ["id"], "score": ["score"]})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = dg.materialize([src, next(a for a in c.build_defs(None).assets)])
    md = r.asset_materializations_for_node("top")[0].metadata
    assert set(md["dagster/column_lineage"].value.deps_by_column) >= {"id", "score"}
    assert _cols({"dagster/column_lineage": md["dagster/column_lineage"]})["id"] == [("ranked_customers", "id")]


def test_source_to_target_lineage_survives_the_event_log(tmp_path):
    """External source asset -> real ingestion component. The lineage names the source asset and its columns, the
    source is a real parent in the asset graph, and the metadata is what a persisted instance (what the UI reads) returns."""
    from dagster._serdes import deserialize_value, serialize_value

    csv = tmp_path / "customers.csv"
    csv.write_text("id,full_name,spend\n1,Ann,10\n2,Bo,20\n")
    mod = _load("assets/ingestion/file_ingestion")
    comp = mod.FileIngestionComponent(
        asset_name="customers", format="csv", file_path=str(csv), deps=["raw/customers_export"],
        column_lineage={"id": ["id"], "full_name": ["full_name"], "spend": ["spend"]})
    source = dg.AssetSpec("raw/customers_export", description="the system the file was exported from")
    defs = dg.Definitions(assets=[source, *comp.build_defs(None).assets])
    graph = defs.resolve_asset_graph()
    assert dg.AssetKey(["raw", "customers_export"]) in graph.get(dg.AssetKey("customers")).parent_keys

    (tmp_path / "home").mkdir()
    with dg.instance_for_test(temp_dir=str(tmp_path / "home")) as instance:
        assert dg.materialize([a for a in defs.assets if not isinstance(a, dg.AssetSpec)], instance=instance).success
        event = instance.get_latest_materialization_event(dg.AssetKey("customers"))
        raw = event.asset_materialization.metadata["dagster/column_lineage"]
        lineage = deserialize_value(serialize_value(raw)).value
    deps = lineage.deps_by_column
    assert set(deps) == {"id", "full_name", "spend"}
    assert [(d.asset_key.to_user_string(), d.column_name) for d in deps["full_name"]] == [("raw/customers_export", "full_name")]
