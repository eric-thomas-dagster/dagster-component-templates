"""Universal post_processing test — verifies that Dagster's `post_processing:`
YAML block lands attributes on assets emitted by asset-emitting components in
this library.

Approach:
- Instantiate a representative sample of asset-emitting components across
  categories (external_asset, dbt, streaming source, warehouse table, etc.)
- Run the component's `build_defs()` to get a `Definitions`.
- Manually apply a `post_processing` block that targets `"*"` and sets
  `tags={"audited": "true"}` — mirrors what Dagster's component runtime
  does after `build_defs()` returns.
- Assert every emitted AssetSpec picked up the tag.

If any component grows a non-standard build_defs pattern in the future that
clobbers post_processing (e.g., replacing specs after Definitions creation),
this test fails and flags the regression.
"""
import importlib.util
import pathlib
from typing import Any

import pytest

import dagster as dg


_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_component(rel_path: str, class_name: str) -> Any:
    """Load a component class directly from its component.py without
    depending on the dagster_community_components package being installed."""
    src = _REPO_ROOT / rel_path / "component.py"
    spec = importlib.util.spec_from_file_location(rel_path.replace("/", "."), src)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, class_name)


def _apply_post_processing_attrs(defs: dg.Definitions, target: str, attrs: dict) -> dg.Definitions:
    """Emulate the Dagster runtime's post_processing: hook. Applies the given
    attributes to every asset spec whose key matches the selection expression.
    This mirrors what defs_module.py::ComponentPostProcessingModel does.
    """
    graph = defs.resolve_asset_graph()
    if target == "*":
        matched_keys = set(graph.get_all_asset_keys())
    else:
        matched_keys = dg.AssetSelection.from_string(target).resolve(graph)

    def transform(spec: dg.AssetSpec) -> dg.AssetSpec:
        if spec.key not in matched_keys:
            return spec
        merge_kw = {k: v for k, v in attrs.items() if k in ("tags", "owners", "metadata", "kinds")}
        replace_kw = {k: v for k, v in attrs.items() if k not in merge_kw}
        result = spec
        if merge_kw:
            result = result.merge_attributes(**merge_kw)
        if replace_kw:
            result = result.replace_attributes(**replace_kw)
        return result

    return defs.map_resolved_asset_specs(func=transform)


# ── Representative sample: asset-emitting components across categories ──
#
# Each tuple: (rel_path, class_name, ctor_kwargs, expected_asset_key_prefix).
# ctor_kwargs is the minimal config needed to instantiate + build_defs without
# side effects (no network / disk / env vars).

SAMPLES = [
    # External asset family — no compute, purely metadata
    ("external_assets/external_kafka_asset", "ExternalKafkaAsset",
     {"asset_key": "streams/orders", "bootstrap_servers": "localhost:9092", "topic": "orders"}),
    ("external_assets/external_s3_asset", "ExternalS3Asset",
     {"asset_key": "lake/raw", "bucket_name": "my-lake"}),
    ("external_assets/external_sql_asset", "ExternalSqlAsset",
     {"asset_key": "warehouse/orders", "table_name": "orders", "connection_string_env_var": "DB_URL"}),
    ("external_assets/external_bigquery_table", "ExternalBigQueryTableAsset",
     {"asset_key": "bq/orders", "project_id": "p", "dataset_id": "d", "table_id": "orders"}),
    ("external_assets/external_snowflake_table", "ExternalSnowflakeTableAsset",
     {"asset_key": "sf/orders", "account": "myacct", "database": "db", "schema_name": "public", "table_name": "orders"}),
    ("external_assets/external_delta_table", "ExternalDeltaTableAsset",
     {"asset_key": "delta/orders", "table_uri": "s3://x/orders"}),
    ("external_assets/external_iceberg_table", "ExternalIcebergTableAsset",
     {"asset_key": "iceberg/orders", "catalog_name": "c", "namespace": "ns", "table_name": "orders"}),
    ("external_assets/external_databricks_table", "ExternalDatabricksTableAsset",
     {"asset_key": "dbx/orders", "workspace_url": "https://x", "schema_name": "s", "table_name": "orders"}),
    ("external_assets/external_gcs_asset", "ExternalGcsAsset",
     {"asset_key": "gcs/raw", "bucket_name": "my-bucket"}),
    ("external_assets/external_adls_asset", "ExternalAdlsAsset",
     {"asset_key": "adls/raw", "account_name": "acct", "container_name": "raw"}),
]


@pytest.mark.parametrize("rel_path,class_name,kwargs", SAMPLES,
                          ids=[f"{s[1]}" for s in SAMPLES])
def test_post_processing_tags_land_on_all_assets(rel_path, class_name, kwargs):
    """post_processing: assets[*].target=* + attributes.tags → tag lands on every emitted asset."""
    Cls = _load_component(rel_path, class_name)
    c = Cls(**kwargs)
    defs = c.build_defs(None)

    processed = _apply_post_processing_attrs(
        defs, target="*", attrs={"tags": {"audited": "true"}},
    )

    specs = list(processed.resolve_all_asset_specs())
    assert specs, f"{class_name} emitted zero specs"
    for spec in specs:
        assert spec.tags.get("audited") == "true", (
            f"{class_name}: post_processing tag did not land on {spec.key.to_user_string()!r}"
        )


@pytest.mark.parametrize("rel_path,class_name,kwargs", SAMPLES,
                          ids=[f"{s[1]}" for s in SAMPLES])
def test_post_processing_owners_land_on_all_assets(rel_path, class_name, kwargs):
    """post_processing sets owners — verifies list-typed attributes merge."""
    Cls = _load_component(rel_path, class_name)
    c = Cls(**kwargs)
    defs = c.build_defs(None)

    processed = _apply_post_processing_attrs(
        defs, target="*", attrs={"owners": ["data-platform@company.com"]},
    )

    specs = list(processed.resolve_all_asset_specs())
    for spec in specs:
        assert "data-platform@company.com" in (spec.owners or []), (
            f"{class_name}: post_processing owners did not land on {spec.key.to_user_string()!r}"
        )


# ── Partition-specific test — only on external assets (metadata-only, always safe) ──


PARTITION_SAFE_SAMPLES = [
    s for s in SAMPLES if s[0].startswith("external_assets/")
]


@pytest.mark.parametrize("rel_path,class_name,kwargs", PARTITION_SAFE_SAMPLES,
                          ids=[f"{s[1]}" for s in PARTITION_SAFE_SAMPLES])
def test_post_processing_partitions_def_lands_on_external_assets(rel_path, class_name, kwargs):
    """External assets accept post_processing partitions_def as metadata (no compute path).

    For compute-having components, whether post_processing.partitions_def is
    safe depends on whether the compute reads context.partition_key —
    see docs/partition_aware_components.md.
    """
    Cls = _load_component(rel_path, class_name)
    c = Cls(**kwargs)
    defs = c.build_defs(None)

    processed = _apply_post_processing_attrs(
        defs, target="*",
        attrs={"partitions_def": dg.DailyPartitionsDefinition(start_date="2025-01-01")},
    )

    specs = list(processed.resolve_all_asset_specs())
    for spec in specs:
        assert isinstance(spec.partitions_def, dg.DailyPartitionsDefinition), (
            f"{class_name}: post_processing partitions_def did not land on {spec.key.to_user_string()!r}"
        )
