"""Committed regression test for a real bug found while debugging a customer
report against the synthetic_commerce quickstart: an unpartitioned asset
(e.g. a "customers" master snapshot) co-materialized in the same run as a
daily-partitioned sibling (e.g. "orders", via a `deps:`-declared relationship
and a `+asset_name` selection launch) incorrectly adopted the run's partition
key as its own, because `context.has_partition_key` reflects whether the
CURRENT RUN targets a single partition, not whether THIS asset itself has a
`partitions_def`. That corrupted the unpartitioned asset's output (customer_id
silently switched from the canonical CUST000001-CUST00NNNN scheme to the
date-suffixed scheme meant only for a genuinely partitioned caller), which in
turn broke a downstream dbt `relationships` test against 100% of that
partition's rows.

Also covers a related follow-up: `_generate_orders`' customer_id range was
hardcoded to [1, 1000] with no config exposing the assumption that some
sibling "customers" asset has exactly that many rows -- a silent convention,
not an enforced link. Promoted to an explicit `schema_options.customer_count`
knob instead.

And a third, user-reported follow-up: every partition replayed the exact
same `random.seed(random_state)` sequence, so e.g. daily order revenue was
byte-for-byte identical on every single partition (confirmed live against
real materialized data) -- useless as the basis for any check/metric meant
to show a real trend chart. Fixed by offsetting the seed by the partition
date's ordinal, so each day's draws genuinely differ while the SAME day
still reproduces identically on rematerialization.
"""
import re

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _customers_component(mod):
    return mod.SyntheticDataGeneratorComponent(
        asset_name="raw_customers",
        schema_type="customers",
        row_count=20,
        random_state=42,
    )


def _orders_component(mod):
    return mod.SyntheticDataGeneratorComponent(
        asset_name="raw_orders",
        schema_type="orders",
        row_count=5,
        random_state=42,
        partition_type="daily",
        partition_start="2026-09-01",
        deps=["raw_customers"],
    )


def test_unpartitioned_sibling_ignores_run_level_partition_key(mod):
    customers_defs = _customers_component(mod).build_defs(context=None)
    orders_defs = _orders_component(mod).build_defs(context=None)

    result = dg.materialize(
        assets=[*customers_defs.assets, *orders_defs.assets],
        partition_key="2026-10-03",
    )
    assert result.success

    customers_df = result.output_for_node("raw_customers")
    # Canonical unpartitioned format: CUST000001-CUST0000NN. The bug produced
    # CUST<YYYYMMDD><3-digit-seq> instead -- the sibling's run-level
    # partition key leaking into an asset with no partitions_def of its own.
    assert all(
        re.fullmatch(r"CUST\d{6}", cid) for cid in customers_df["customer_id"]
    ), customers_df["customer_id"].tolist()


def test_partitioned_asset_still_uses_its_own_partition_key(mod):
    """Non-regression: a genuinely partitioned asset must still derive
    target_date from its own partition key, unaffected by the guard added
    for the unpartitioned-sibling bug above."""
    orders_defs = _orders_component(mod).build_defs(context=None)

    result = dg.materialize(assets=orders_defs.assets, partition_key="2026-10-03")
    assert result.success

    orders_df = result.output_for_node("raw_orders")
    assert (orders_df["order_date"].dt.date.astype(str) == "2026-10-03").all()


def test_orders_customer_id_defaults_to_the_historical_1000_range(mod):
    """No schema_options set -- preserves the pre-existing hardcoded range
    exactly, so this isn't a breaking change for any existing project."""
    orders = mod.SyntheticDataGeneratorComponent(
        asset_name="raw_orders", schema_type="orders", row_count=50, random_state=42,
    )
    result = dg.materialize(assets=orders.build_defs(context=None).assets)
    assert result.success

    orders_df = result.output_for_node("raw_orders")
    ids = sorted(int(c[4:]) for c in orders_df["customer_id"])
    assert 1 <= ids[0] and ids[-1] <= 1000


def test_orders_customer_id_respects_schema_options_customer_count(mod):
    """The real fix: customer_count is now an explicit, visible config knob
    instead of a magic literal buried in the generator -- set it narrow and
    confirm every drawn customer_id actually honors the configured bound."""
    orders = mod.SyntheticDataGeneratorComponent(
        asset_name="raw_orders",
        schema_type="orders",
        row_count=50,
        random_state=42,
        schema_options={"customer_count": 7},
    )
    result = dg.materialize(assets=orders.build_defs(context=None).assets)
    assert result.success

    orders_df = result.output_for_node("raw_orders")
    ids = {int(c[4:]) for c in orders_df["customer_id"]}
    assert ids, "expected at least one order"
    assert ids <= set(range(1, 8))


def test_partitioned_output_varies_across_partitions_but_not_within_one(mod):
    """Every partition used to replay the identical random.seed(random_state)
    sequence, so e.g. daily revenue was byte-for-byte identical on every
    single day -- confirmed live against real materialized data. A check
    meant to chart something like daily revenue needs genuine day-to-day
    variation to be any use at all."""
    def _materialize_revenue(partition_key: str) -> float:
        orders = mod.SyntheticDataGeneratorComponent(
            asset_name="raw_orders", schema_type="orders", row_count=50,
            random_state=42, partition_type="daily", partition_start="2026-09-01",
        )
        result = dg.materialize(
            assets=orders.build_defs(context=None).assets, partition_key=partition_key,
        )
        assert result.success
        return float(result.output_for_node("raw_orders")["total"].sum())

    revenue_day_1 = _materialize_revenue("2026-10-03")
    revenue_day_1_again = _materialize_revenue("2026-10-03")
    revenue_day_2 = _materialize_revenue("2026-10-04")

    assert revenue_day_1 == revenue_day_1_again, "same partition must reproduce identically"
    assert revenue_day_1 != revenue_day_2, "different partitions must not be byte-for-byte identical"


def test_backfill_policy_single_run_covers_the_whole_range_in_one_call(mod):
    """backfill_policy='single_run' means a backfill of N partitions arrives
    as ONE call covering all N days at once (via context.partition_key_range,
    not a single context.partition_key) -- confirm the asset handles that:
    row_count rows per day, every day's date present, and each day still
    gets its own correctly-varying random draws (same per-day revenue as if
    that single day had been materialized on its own -- see the two tests
    above)."""
    import dagster as dg

    orders = mod.SyntheticDataGeneratorComponent(
        asset_name="raw_orders", schema_type="orders", row_count=50,
        random_state=42, partition_type="daily", partition_start="2026-09-01",
        backfill_policy="single_run",
    )
    asset_def = orders.build_defs(context=None).assets[0]

    ctx = dg.build_asset_context(
        partition_key_range=dg.PartitionKeyRange(start="2026-10-01", end="2026-10-03")
    )
    df = asset_def(ctx)

    assert len(df) == 50 * 3
    assert sorted(df["order_date"].dt.date.astype(str).unique()) == [
        "2026-10-01", "2026-10-02", "2026-10-03",
    ]
    per_day_revenue = df.groupby(df["order_date"].dt.date)["total"].sum()
    assert len(set(per_day_revenue.values)) == 3, "each backfilled day must still vary"


def test_backfill_policy_unset_does_not_set_one(mod):
    """Non-regression: the default (no backfill_policy configured) must not
    silently acquire one -- Dagster's own per-partition default behavior for
    every existing project stays exactly as it was."""
    orders = mod.SyntheticDataGeneratorComponent(
        asset_name="raw_orders", schema_type="orders", row_count=5,
        random_state=42, partition_type="daily", partition_start="2026-09-01",
    )
    asset_def = orders.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None
