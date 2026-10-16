"""backfill_policy tests for SupportTicketStandardizerComponent.

The component loads its raw tickets with context.load_asset_value and filters
them to the current partition (partition_date_column = created_at, compared at
day granularity). A single_run backfill hands it a partition RANGE, so the
standardisation runs once per partition and the frames are concatenated with a
'_partition_key' column.
Real dg.materialize() with a real Zendesk-shaped DataFrame; nothing is mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "zendesk_tickets"
DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")


@pytest.fixture()
def mod():
    return load_component_module()


class _FrameIO(dg.InMemoryIOManager):
    def __init__(self, frame):
        super().__init__()
        self._frame = frame

    def load_input(self, context):
        if context.asset_key == dg.AssetKey(UPSTREAM):
            return self._frame
        return super().load_input(context)


def _frame():
    rows = []
    n = 100
    for d, day in enumerate(DAYS):
        for i in range(d + 2):
            n += 1
            created = pd.Timestamp(f"{day} 0{9 + i}:30:00")
            rows.append({
                "id": n,
                "subject": f"Ticket {n}",
                "status": "solved" if i % 2 == 0 else "open",
                "priority": "high" if d == 1 else "normal",
                "created_at": created.isoformat(),
                "updated_at": (created + pd.Timedelta(hours=3)).isoformat(),
                "solved_at": (created + pd.Timedelta(hours=2 + d)).isoformat(),
            })
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="std_tickets",
        platform="zendesk",
        upstream_asset_key=UPSTREAM,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    kwargs.update(overrides)
    return mod.SupportTicketStandardizerComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frame, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component), dg.SourceAsset(UPSTREAM)],
        resources={"io_manager": _FrameIO(frame)},
        partition_key=partition_key,
        tags=tags or {},
    )


def _range_tags(start, end):
    return {
        "dagster/asset_partition_range_start": start,
        "dagster/asset_partition_range_end": end,
    }


def test_backfill_policy_is_wired_onto_the_asset(mod):
    assert _asset(_component(mod, backfill_policy="single_run")).backfill_policy == dg.BackfillPolicy.single_run()
    assert _asset(
        _component(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3)
    ).backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    assert _asset(_component(mod)).backfill_policy is None


def test_backfill_policy_ignored_when_not_partitioned(mod):
    asset_def = _asset(_component(mod, partition_type=None, partition_start=None, backfill_policy="single_run"))
    assert asset_def.partitions_def is None
    assert asset_def.backfill_policy is None


def test_single_partition_still_standardises_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("std_tickets")
    assert "_partition_key" not in out.columns
    assert len(out) == 3
    assert set(out["priority"]) == {"high"}
    assert (out["created_date"].dt.strftime("%Y-%m-%d") == "2024-01-03").all()


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("std_tickets")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node("std_tickets")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
    assert out.groupby("_partition_key").size().tolist() == [2, 3, 4]


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("std_tickets")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}
    assert len(out) == 3 + 4


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("std_tickets")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frame()
    ).output_for_node("std_tickets")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
    assert len(out) == 2 + 3 + 4
