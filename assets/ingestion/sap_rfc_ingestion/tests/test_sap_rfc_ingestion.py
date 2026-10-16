"""Committed tests for SapRFCIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Mocks ONLY the sap_rfc_resource's `.connection()` context manager + `.call()`
(a fake RFC_READ_TABLE responder keyed by the resolved WHERE clause) -- every
other behavior (partitions_def construction, where_clause templating,
RFC_READ_TABLE response parsing, DataFrame building) is exercised for real
through dg.materialize().

This asset returns Output(value=df) -- it IS routed through Dagster's IO
manager, so a single_run backfill here has the multi-partition-output IO
manager constraint documented on the field -- tests use
dg.InMemoryIOManager() + the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets.
"""
import contextlib

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeRFCConnection:
    """Maps the resolved WHERE clause to canned RFC_READ_TABLE FIELDS/DATA;
    records every call."""

    def __init__(self, where_to_rows: dict):
        self.where_to_rows = where_to_rows
        self.calls: list = []

    def call(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if name != "RFC_READ_TABLE":
            raise ValueError(f"unexpected RFC {name!r}")
        options = kwargs.get("OPTIONS", [])
        where = "".join(o["TEXT"] for o in options)
        rows = self.where_to_rows.get(where, [])
        return {
            "FIELDS": [{"FIELDNAME": "MATNR"}, {"FIELDNAME": "ERSDA"}],
            "DATA": [{"WA": "|".join(row)} for row in rows],
        }


class FakeRFCResource:
    def __init__(self, conn):
        self._conn = conn

    @contextlib.contextmanager
    def connection(self):
        yield self._conn


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="material_master",
        sap_rfc_resource_key="sap_ecc",
        mode="read_table",
        table_name="MARA",
        where_clause="ERSDA = '{partition_key}'",
    )
    kwargs.update(overrides)
    return mod.SapRFCIngestionComponent(**kwargs)


def _materialize(component, fake_resource, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"sap_ecc": fake_resource, "io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_reads_one_days_rows(mod):
    conn = FakeRFCConnection({"ERSDA = '2024-01-02'": [["M1", "2024-01-02"], ["M2", "2024-01-02"]]})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, FakeRFCResource(conn), partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("material_master")
    assert len(df) == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod):
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod):
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_calls_once_per_day_and_concatenates(mod):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into
    where_clause, which doesn't exist for a multi-partition single_run
    backfill -- has_partition_key is False for a range, so partition_key
    silently became None and the unresolved '{partition_key}' literal was
    sent as the literal WHERE text, matching nothing. Confirmed fixed: a
    3-day single_run backfill now issues one RFC_READ_TABLE call per day in
    the range and concatenates exactly those rows."""
    conn = FakeRFCConnection({
        "ERSDA = '2024-01-02'": [["M1", "2024-01-02"], ["M2", "2024-01-02"]],
        "ERSDA = '2024-01-03'": [["M3", "2024-01-03"]],
        "ERSDA = '2024-01-04'": [["M4", "2024-01-04"], ["M5", "2024-01-04"], ["M6", "2024-01-04"]],
    })
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, FakeRFCResource(conn),
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-04",
        },
    )
    assert result.success
    df = result.output_for_node("material_master")
    assert len(df) == 6
    assert sorted(df["MATNR"].tolist()) == ["M1", "M2", "M3", "M4", "M5", "M6"]
    assert len(conn.calls) == 3
