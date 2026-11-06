"""file_format='json' (plain, non-newline-delimited JSON) and 'auto' (per-file
extension dispatch for a mixed-format glob) -- dlt's own filesystem readers only
cover csv/parquet/jsonl. Exercised against real local files (no network/paid
call involved for a local bucket_url, matching this repo's "mock only the paid
call" convention -- nothing here needs faking)."""
from .conftest import load_component_module


def _rows(resource):
    out = []
    for batch in resource:
        out.extend(batch if isinstance(batch, list) else [batch])
    return out


def test_json_format_reads_array_and_single_object_files(tmp_path):
    mod = load_component_module()
    (tmp_path / "a.json").write_text('[{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]')
    (tmp_path / "b.json").write_text('{"id": 3, "name": "z"}')
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        file_format="json",
        filesystem_resources=[{"name": "records", "file_glob": "*.json"}],
    )
    resources, names = component._build_resources()
    assert names == ["records"]
    rows = _rows(resources[0])
    assert sorted(r["id"] for r in rows) == [1, 2, 3]


def test_auto_format_dispatches_csv_and_jsonl_by_extension(tmp_path):
    mod = load_component_module()
    (tmp_path / "x.csv").write_text("id,name\n1,a\n2,b\n")
    (tmp_path / "y.jsonl").write_text('{"id": 3, "name": "c"}\n{"id": 4, "name": "d"}\n')
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        file_format="auto",
        filesystem_resources=[{"name": "records", "file_glob": "*"}],
    )
    resources, names = component._build_resources()
    rows = _rows(resources[0])
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4]


def test_invalid_file_format_still_raises_a_clear_error(tmp_path):
    mod = load_component_module()
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{"name": "records", "file_glob": "*.xyz", "file_format": "xyz"}],
    )
    import pytest
    with pytest.raises(ValueError, match="csv/parquet/jsonl/json/auto"):
        component._build_resources()
