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
