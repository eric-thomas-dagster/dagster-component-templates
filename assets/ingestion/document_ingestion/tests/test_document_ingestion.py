"""Committed tests for DocumentIngestionComponent's cosmetic-partitioning
fix + backfill_policy support (see airtable_ingestion's tests for the
full rationale -- same _filter_df_to_partition() fix). Uses real local
files on disk -- no network/API to mock for this component at all.

This component has no per-row date column in its real output (every row
gets the same `ingested_at` timestamp), so the meaningful partitioning
axis here is static (file_type), not time-based -- tested accordingly.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def docs_dir(tmp_path):
    (tmp_path / "a.md").write_text("markdown doc")
    (tmp_path / "b.txt").write_text("text doc")
    (tmp_path / "c.html").write_text("<p>html doc</p>")
    return tmp_path


def _make_component(mod, source_path, **overrides):
    kwargs = dict(asset_name="docs", source_path=str(source_path))
    kwargs.update(overrides)
    return mod.DocumentIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_filters_by_file_type(mod, docs_dir):
    component = _make_component(
        mod, docs_dir,
        partition_type="static", partition_values=".md,.txt,.html",
        partition_static_column="file_type",
    )
    result = _materialize(component, partition_key=".md")
    assert result.success
    df = result.output_for_node("docs")
    assert df["file_type"].tolist() == [".md"]


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, docs_dir):
    component = _make_component(
        mod, docs_dir,
        partition_type="static", partition_values=".md,.txt,.html",
        partition_static_column="file_type", backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, docs_dir):
    component = _make_component(
        mod, docs_dir,
        partition_type="static", partition_values=".md,.txt,.html",
        partition_static_column="file_type",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_filters_to_the_requested_keys_only(mod, docs_dir):
    """Real bug before the fix: partition_static_column was declared and
    documented but never used -- every partition (and every backfill
    range) silently returned the IDENTICAL full set of documents.
    Confirmed fixed: a single_run backfill over [.md, .txt] now excludes
    the .html document that falls outside the requested range."""
    component = _make_component(
        mod, docs_dir,
        partition_type="static", partition_values=".md,.txt,.html",
        partition_static_column="file_type", backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": ".md",
            "dagster/asset_partition_range_end": ".txt",
        },
    )
    assert result.success
    df = result.output_for_node("docs")
    assert sorted(df["file_type"].tolist()) == [".md", ".txt"]


def test_partitioned_without_filter_column_configured_is_unfiltered(mod, docs_dir):
    component = _make_component(
        mod, docs_dir,
        partition_type="static", partition_values=".md,.txt,.html",
    )
    result = _materialize(component, partition_key=".md")
    assert result.success
    df = result.output_for_node("docs")
    assert len(df) == 3
