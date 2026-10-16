"""Committed tests for the backfill_policy / partition-range support of this
component's "filter the upstream DataFrame to the current partition" step.

The real component asset is materialized with dg.materialize() against a real
(real-data) upstream asset and dg.InMemoryIOManager(), using the run tags a
real single_run backfill sets (dagster/asset_partition_range_start/_end).

The upstream DataFrame is a thin pandas subclass that records every
boolean-mask selection made on it. That lets us assert exactly which rows the
component's partition filter kept, for ANY of this family of components,
without mocking its (paid / heavyweight) model call at all -- the model call
happens after the filter and may legitimately fail in a test env; the
assertions only concern the filter, which must have really run.
"""
import inspect
import os

import dagster as dg
import numpy as np
import pandas as pd
import pytest

from .conftest import load_component_module

EXTRA = {'text_column': 'text'}

SEEN = []


class _SpyFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return _SpyFrame

    def __getitem__(self, key):
        out = super().__getitem__(key)
        if isinstance(key, np.ndarray) and key.dtype == bool:
            SEEN.append(out)
        return out


def _rows():
    return _SpyFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "event_date": [
            "2024-01-01", "2024-01-02", "2024-01-02",
            "2024-01-03", "2024-01-04", "2024-01-05",
        ],
        "seg": ["a", "b", "a", "b", "a", "b"],
        "text": ["t1", "t2", "t3", "t4", "t5", "t6"],
    })


@pytest.fixture()
def mod():
    return load_component_module()


def _make(mod, tmp_path, **kw):
    cls = next(
        c for c in vars(mod).values()
        if inspect.isclass(c) and issubclass(c, dg.Component)
        and c is not dg.Component and c.__module__ == mod.__name__
    )
    extra = {k: (v.replace("__TMP__", str(tmp_path)) if isinstance(v, str) else v)
             for k, v in EXTRA.items()}
    return cls(asset_name="out_asset", upstream_asset_key="src", **extra, **kw)


def _run(component, **kw):
    @dg.asset(name="src")
    def src():
        return _rows()

    asset_def = component.build_defs(None).assets[0]
    return asset_def, dg.materialize(
        [src, asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        raise_on_error=False,
        **kw,
    )


def _filtered_ids():
    # The partition filter is the first boolean-mask selection on the frame.
    assert SEEN, "the component's partition filter never ran on the upstream frame"
    return sorted(SEEN[0]["id"].tolist())


@pytest.fixture(autouse=True)
def _clear():
    SEEN.clear()
    yield
    SEEN.clear()


def test_backfill_policy_wiring(mod, tmp_path):
    kw = dict(partition_type="daily", partition_start="2024-01-01")
    assert _make(mod, tmp_path, **kw).build_defs(None).assets[0].backfill_policy is None
    single = _make(mod, tmp_path, backfill_policy="single_run", **kw)
    assert single.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    multi = _make(mod, tmp_path, backfill_policy="multi_run",
                  backfill_policy_max_partitions_per_run=3, **kw)
    assert multi.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.multi_run(3)


def test_backfill_policy_ignored_when_unpartitioned(mod, tmp_path):
    comp = _make(mod, tmp_path, backfill_policy="single_run")
    assert comp.build_defs(None).assets[0].backfill_policy is None


def test_single_partition_tick_keeps_only_that_days_rows(mod, tmp_path):
    comp = _make(mod, tmp_path, partition_type="daily", partition_start="2024-01-01",
                 partition_date_column="event_date")
    _run(comp, partition_key="2024-01-02")
    assert _filtered_ids() == [2, 3]


def test_single_run_backfill_range_keeps_union_of_days(mod, tmp_path):
    """Before the fix the filter was gated on has_partition_key (False for a
    range) and read context.partition_key (raises for a range), so a
    single_run backfill either crashed or processed every partition's rows."""
    comp = _make(mod, tmp_path, partition_type="daily", partition_start="2024-01-01",
                 partition_date_column="event_date", backfill_policy="single_run")
    asset_def, result = _run(comp, tags={
        "dagster/asset_partition_range_start": "2024-01-02",
        "dagster/asset_partition_range_end": "2024-01-04",
    })
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()
    # 3 days in range (01-02, 01-03, 01-04), rows for 01-01 and 01-05 excluded.
    assert _filtered_ids() == [2, 3, 4, 5]


def test_static_partition_range_filters_on_static_column(mod, tmp_path):
    comp = _make(mod, tmp_path, partition_type="static", partition_values="a,b,c",
                 partition_static_column="seg", backfill_policy="single_run")
    _run(comp, tags={
        "dagster/asset_partition_range_start": "a",
        "dagster/asset_partition_range_end": "b",
    })
    assert _filtered_ids() == [1, 2, 3, 4, 5, 6]
    SEEN.clear()
    _run(comp, partition_key="a")
    assert _filtered_ids() == [1, 3, 5]
