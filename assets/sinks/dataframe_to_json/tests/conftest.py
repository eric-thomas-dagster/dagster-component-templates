"""Test helpers for DataframeToJsonComponent.

Loads component.py via importlib (no package install needed). Everything the
component owns runs for real under dg.materialize(); nothing is faked -- real files are written to tmp_path.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_json_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class PartitionedMemoryIOManager(dg.IOManager):
    """In-memory IO manager that survives across dg.materialize() calls (dg.InMemoryIOManager
    is per-run) and behaves like a partition-range-aware IO manager: an asset consuming
    several partitions is handed ONE DataFrame spanning them (``multi="concat"``) or, like
    dagster's filesystem IO manager, a ``{partition_key: DataFrame}`` dict (``multi="dict"``)."""

    def __init__(self, multi="concat"):
        self.store = {}
        self.multi = multi

    def handle_output(self, context, obj):
        pk = context.asset_partition_key if context.has_asset_partitions else None
        self.store[(context.asset_key, pk)] = obj

    def load_input(self, context):
        if context.has_asset_partitions:
            keys = list(context.asset_partition_keys)
            if len(keys) == 1:
                return self.store[(context.asset_key, keys[0])]
            parts = {k: self.store[(context.asset_key, k)] for k in keys}
            return parts if self.multi == "dict" else pd.concat(parts.values(), ignore_index=True)
        return self.store[(context.asset_key, None)]


def daily_upstream(name="orders_src", start="2024-01-01"):
    """A daily-partitioned upstream asset returning that day's rows (id, order_date, total)."""
    @dg.asset(name=name, partitions_def=dg.DailyPartitionsDefinition(start_date=start))
    def _up(context: dg.AssetExecutionContext) -> pd.DataFrame:
        day = context.partition_key
        base = {"2024-01-02": 100, "2024-01-03": 200, "2024-01-04": 300}[day]
        return pd.DataFrame({
            "id": [base + 1, base + 2],
            "order_date": [f"{day} 08:00:00", f"{day} 17:30:00"],
            "total": [1.5, 2.5],
        })
    return _up


def all_days_upstream(name="orders_src"):
    """UNPARTITIONED upstream that always hands over all 3 days of rows, so only the
    component's own logic can narrow/split them."""
    @dg.asset(name=name)
    def _up() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [(d - 1) * 100 + 1, (d - 1) * 100 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)
    return _up


def materialize_upstream_days(upstream_def, instance, io_manager, days):
    for d in days:
        r = dg.materialize([upstream_def], partition_key=d, instance=instance,
                           resources={"io_manager": io_manager})
        assert r.success

