"""Real, committed tests for ControlMIntegrationComponent.

The gap this covers: `upstream_deps` used to be ONE list shared across
every job declared in `jobs:`, forcing jobs with different upstreams into
separate component instances. `ControlMJobSpec` already had a per-job
collection (the `jobs:` list itself) that `upstream_deps` failed to key
into -- each `ControlMJobSpec` now has its own optional `deps:` list that
overrides the component-level `upstream_deps` for that job only, falling
back to `upstream_deps` when left empty.

Runs real dg.materialize()/inspects real AssetsDefinition.asset_deps rather
than asserting on shallow field shapes; the only thing mocked is time.sleep
free -- demo_mode already avoids any real Control-M HTTP call.
"""
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
ControlMIntegrationComponent = mod.ControlMIntegrationComponent


def _asset_by_name(defs: dg.Definitions, name: str) -> dg.AssetsDefinition:
    for a in defs.assets:
        if isinstance(a, dg.AssetsDefinition) and name in {k.path[-1] for k in a.keys}:
            return a
    raise AssertionError(f"no asset named {name!r} in {[a for a in defs.assets]}")


def test_jobs_with_no_per_job_deps_fall_back_to_component_level_upstream_deps():
    comp = ControlMIntegrationComponent(
        upstream_deps=["shared/upstream"],
        jobs=[
            {"job_name": "JOB_A", "asset_name": "job_a"},
            {"job_name": "JOB_B", "asset_name": "job_b"},
        ],
    )
    defs = comp.build_defs(None)
    job_a = _asset_by_name(defs, "job_a")
    job_b = _asset_by_name(defs, "job_b")
    assert job_a.asset_deps[dg.AssetKey("job_a")] == {dg.AssetKey(["shared", "upstream"])}
    assert job_b.asset_deps[dg.AssetKey("job_b")] == {dg.AssetKey(["shared", "upstream"])}


def test_per_job_deps_overrides_the_shared_upstream_deps_list():
    """The actual gap: two jobs in the same component instance now get
    DIFFERENT upstreams, which used to require two separate component
    instances."""
    comp = ControlMIntegrationComponent(
        upstream_deps=["shared/upstream"],
        jobs=[
            {"job_name": "JOB_A", "asset_name": "job_a", "deps": ["only_for_a"]},
            {"job_name": "JOB_B", "asset_name": "job_b"},
        ],
    )
    defs = comp.build_defs(None)
    job_a = _asset_by_name(defs, "job_a")
    job_b = _asset_by_name(defs, "job_b")
    assert job_a.asset_deps[dg.AssetKey("job_a")] == {dg.AssetKey(["only_for_a"])}
    assert job_b.asset_deps[dg.AssetKey("job_b")] == {dg.AssetKey(["shared", "upstream"])}


def test_per_job_deps_supports_nested_slash_separated_keys():
    comp = ControlMIntegrationComponent(
        jobs=[{"job_name": "JOB_A", "asset_name": "job_a", "deps": ["raw/settlement_ledger"]}],
    )
    defs = comp.build_defs(None)
    job_a = _asset_by_name(defs, "job_a")
    assert job_a.asset_deps[dg.AssetKey("job_a")] == {dg.AssetKey(["raw", "settlement_ledger"])}


def test_job_with_empty_deps_and_no_component_level_upstream_deps_has_no_deps():
    comp = ControlMIntegrationComponent(
        jobs=[{"job_name": "JOB_A", "asset_name": "job_a"}],
    )
    defs = comp.build_defs(None)
    job_a = _asset_by_name(defs, "job_a")
    assert job_a.asset_deps[dg.AssetKey("job_a")] == set()


def test_demo_mode_materializes_each_job_with_its_own_deps_intact():
    """End-to-end sanity: the per-job deps field doesn't break materialization
    in demo_mode (no real Control-M endpoint required)."""
    comp = ControlMIntegrationComponent(
        demo_mode=True,
        jobs=[
            {"job_name": "JOB_A", "asset_name": "job_a"},
            {"job_name": "JOB_B", "asset_name": "job_b", "deps": ["job_a"]},
        ],
    )
    defs = comp.build_defs(None)
    job_b = _asset_by_name(defs, "job_b")
    assert job_b.asset_deps[dg.AssetKey("job_b")] == {dg.AssetKey("job_a")}

    result = dg.materialize([_asset_by_name(defs, "job_a"), job_b], partition_key="2024-01-02")
    assert result.success
