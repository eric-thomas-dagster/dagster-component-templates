"""partition_type='multi' builds its static axis as 'static_dim'; the row filter must look the
static key up under that name (it used to look under `partition_static_dim or "segment"`, which never
matched, so the static filter was silently skipped and every segment's rows came back)."""
import importlib.util
from pathlib import Path

import dagster as dg
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OLD = '_pk.keys_by_dimension.get(partition_static_dim or "segment", "")'


def test_old_lookup_is_gone_everywhere():
    bad = [
        str(p.relative_to(ROOT)) for p in ROOT.glob("**/component.py")
        if ".venv" not in p.parts and "site-packages" not in p.parts and p.relative_to(ROOT).parts[0] != "dist"
        and OLD in p.read_text()
    ]
    assert not bad, "static-axis lookup regressed in:\n  " + "\n  ".join(bad)


def test_filter_component_applies_static_filter_with_default_multi_config():
    spec = importlib.util.spec_from_file_location("_msa_filter", ROOT / "assets/transforms/filter/component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    @dg.asset(name="orders_src")
    def src() -> pd.DataFrame:
        return pd.DataFrame([
            {"id": 1, "d": "2024-01-02 10:00:00", "seg": "a", "v": 5},
            {"id": 2, "d": "2024-01-02 11:00:00", "seg": "b", "v": 5},
            {"id": 3, "d": "2024-01-03 10:00:00", "seg": "a", "v": 5},
        ])

    comp = mod.FilterComponent(
        asset_name="out", upstream_asset_key="orders_src", condition="v > 0",
        partition_type="multi", partition_start="2024-01-01", partition_values="a,b",
        partition_date_column="d", partition_static_column="seg",
    )
    asset = comp.build_defs(None).assets[0]
    captured = {}

    class Capture(dg.InMemoryIOManager):
        def handle_output(self, context, obj):
            if context.asset_key.to_user_string() == "out":
                captured["df"] = obj
            super().handle_output(context, obj)

    r = dg.materialize(
        [src, asset], resources={"io_manager": Capture()},
        partition_key=dg.MultiPartitionKey({"date": "2024-01-02", "static_dim": "a"}),
    )
    assert r.success
    assert list(captured["df"]["id"]) == [1]  # date 01-02 AND segment a; not id 2 (segment b)
