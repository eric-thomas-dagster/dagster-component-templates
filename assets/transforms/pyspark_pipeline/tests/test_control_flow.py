"""Real tests for the multi-step control-flow step types (condition / checkpoint /
for_each) against a real local Spark session -- see the module docstring in
component.py for the full step-type reference these mirror."""
import json

import dagster as dg
import pandas as pd
import pytest


@dg.asset
def raw_orders():
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "amount": [100.0, 50.0, 200.0, 10.0, 75.0],
        "status": ["paid", "paid", "unpaid", "paid", "paid"],
    })


def _materialize_single(comp, upstream_assets=()):
    defs = dg.Definitions.merge(
        dg.Definitions(assets=list(upstream_assets)) if upstream_assets else dg.Definitions(),
        comp.build_defs(None),
    )
    asset_key = dg.AssetKey(comp.asset_name)
    result = dg.materialize(list(upstream_assets) + [defs.get_assets_def(asset_key)])
    return result.output_for_node(comp.asset_name)


def test_condition_true_continues(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="condition_passes",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "guard", "type": "condition", "when": "orders.row_count > 0", "on_false": "skip_rest"},
            {"id": "filtered", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "filter", "predicate": "status = 'paid'"}]},
        ],
        sinks=[{"from": "filtered", "kind": "none"}],
    )
    df = _materialize_single(comp, [raw_orders])
    assert sorted(df["customer_id"].tolist()) == [1, 2, 4, 5]


def test_condition_false_skip_rest_halts_remaining_steps(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="condition_skips",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "guard", "type": "condition", "when": "orders.row_count > 100", "on_false": "skip_rest"},
            {"id": "filtered", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "filter", "predicate": "status = 'paid'"}]},
        ],
        # sink reads from "orders" (produced before the halted step), not "filtered"
        # (which never ran) -- proves skip_rest actually stopped step execution.
        sinks=[{"from": "orders", "kind": "none"}],
    )
    df = _materialize_single(comp, [raw_orders])
    assert len(df) == 5  # unfiltered -- "filtered" step never executed


def test_condition_false_fail_raises(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="condition_fails_hard",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "guard", "type": "condition", "when": "orders.row_count > 100", "on_false": "fail"},
        ],
        sinks=[{"from": "orders", "kind": "none"}],
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[raw_orders]), comp.build_defs(None))
    with pytest.raises(dg.Failure, match="row_count > 100"):
        dg.materialize([raw_orders, defs.get_assets_def(dg.AssetKey(comp.asset_name))])


def test_checkpoint_persists_and_reloads_next_run(mod, spark, tmp_path):
    ckpt_dir = tmp_path / "checkpoints"
    comp = mod.PySparkPipelineComponent(
        asset_name="with_checkpoint",
        checkpoint_dir=str(ckpt_dir),
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "snap", "type": "checkpoint", "keys": ["orders.row_count"]},
        ],
        sinks=[{"from": "orders", "kind": "none"}],
    )
    _materialize_single(comp, [raw_orders])

    ckpt_file = ckpt_dir / "with_checkpoint.json"
    assert ckpt_file.exists()
    saved = json.loads(ckpt_file.read_text())
    assert saved["orders.row_count"] == 5

    # Second run: a condition gated on the checkpoint value from the prior run
    # must see it via `checkpoint.<key>`.
    comp2 = mod.PySparkPipelineComponent(
        asset_name="with_checkpoint",
        checkpoint_dir=str(ckpt_dir),
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "guard", "type": "condition", "when": "checkpoint.get('orders.row_count') == 5", "on_false": "fail"},
        ],
        sinks=[{"from": "orders", "kind": "none"}],
    )
    df = _materialize_single(comp2, [raw_orders])
    assert len(df) == 5


def test_for_each_loops_over_variable_and_substitutes_loop_tokens(mod, spark, tmp_path):
    # for_each's documented usage is a templated FILE path per iteration (not a
    # per-iteration upstream asset key -- Dagster's asset dependency graph is
    # static, declared at code-location-load time, before any loop token is
    # known, so `source: {kind: upstream, ...}` isn't meaningful inside a
    # for_each body; see the component's own module docstring example).
    for region, amount in [("a", 10.0), ("b", 20.0)]:
        spark.createDataFrame([(region, amount)], ["region", "amount"]) \
            .write.mode("overwrite").parquet(str(tmp_path / region))

    comp = mod.PySparkPipelineComponent(
        asset_name="for_each_demo",
        variables={"regions": ["a", "b"]},
        steps=[
            {"id": "loop", "type": "for_each", "over": "vars.regions", "as": "r",
             "steps": [
                 {"id": "region_slice",
                  "source": {"kind": "parquet", "path": str(tmp_path) + "/{{ loop.r }}"},
                  "operations": [{"op": "with_columns", "expressions": {"iter_index": "{{ loop.index }}"}}]},
             ]},
        ],
        sinks=[{"from": "region_slice", "kind": "none"}],
    )
    df = _materialize_single(comp)
    # the outer scope only ever sees the LAST iteration's step_outputs entry
    # (region "b", index 1) -- the component's own documented behavior.
    assert df["region"].iloc[0] == "b"
    assert df["amount"].iloc[0] == 20.0
    assert int(df["iter_index"].iloc[0]) == 1
