"""`partition_field` / `partition_type` on the BigQuery components configure the destination TABLE's own partitioning,
which is unrelated to Dagster partitions, so they were renamed `table_partition_*`. The old names keep working (with a
DeprecationWarning) so existing YAML does not break."""
import importlib.util
import warnings
from pathlib import Path

import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
LOAD = ROOT / "assets/ingestion/bigquery_load_from_gcs_asset"
CREATE = ROOT / "assets/transforms/bigquery_create_table_from_query_asset"
KEY_PATH = ""


@pytest.fixture(autouse=True)
def _credentials(tmp_path, monkeypatch):
    """Building the defs reads the credentials file; any JSON will do because no BigQuery client is created here."""
    key = tmp_path / "sa.json"
    key.write_text("{}")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setattr(__import__(__name__), "KEY_PATH", str(key), raising=False)


def _cls(folder):
    spec = importlib.util.spec_from_file_location(f"bq_{folder.name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    attrs = dict(doc["attributes"])
    if "credentials_path" in attrs:  # the example's ${ENV} placeholder is only resolved by Dagster's YAML loader
        attrs["credentials_path"] = KEY_PATH
    return getattr(mod, doc["type"].rsplit(".", 1)[-1]), attrs


def test_load_component_uses_the_new_names_and_builds():
    cls, base = _cls(LOAD)
    comp = cls(**{**base, "table_partition_field": "order_date", "table_partition_type": "DAY"})
    assert (comp.table_partition_field, comp.table_partition_type) == ("order_date", "DAY")
    assert next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def")).partitions_def is None
    assert "partition_field" not in cls.model_fields and "partition_type" not in cls.model_fields


def test_load_component_old_names_still_work_with_a_deprecation_warning():
    cls, base = _cls(LOAD)
    with pytest.warns(DeprecationWarning, match="partition_field.*table_partition_field"):
        with pytest.warns(DeprecationWarning, match="partition_type.*table_partition_type"):
            comp = cls(**{**base, "partition_field": "order_date", "partition_type": "HOUR"})
    assert (comp.table_partition_field, comp.table_partition_type) == ("order_date", "HOUR")


def test_both_old_and_new_names_is_an_error():
    cls, base = _cls(LOAD)
    with pytest.raises(ValueError, match="not both"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cls(**{**base, "partition_field": "a", "table_partition_field": "b"})


def test_create_table_keeps_dagster_partition_type_separate_from_the_table_partition_column():
    cls, base = _cls(CREATE)
    comp = cls(**{**base, "table_partition_field": "order_date", "partition_type": "daily", "partition_start": "2026-01-01"})
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    assert isinstance(asset.partitions_def, dg.DailyPartitionsDefinition)  # Dagster partitions: partition_type
    assert comp.table_partition_field == "order_date"                      # the table's PARTITION BY column
    with pytest.warns(DeprecationWarning, match="partition_field.*table_partition_field"):
        old = cls(**{**{k: v for k, v in base.items() if k != "table_partition_field"}, "partition_field": "order_date"})
    assert old.table_partition_field == "order_date" and old.partition_type is None


def test_examples_and_schemas_use_the_new_names():
    for folder in (LOAD, CREATE):
        schema = (folder / "schema.json").read_text()
        assert '"table_partition_field"' in schema and '"partition_field"' not in schema
        assert "table_partition_field" in (folder / "README.md").read_text()
    assert '"table_partition_type"' in (LOAD / "schema.json").read_text() and '"partition_type"' not in (LOAD / "schema.json").read_text()
