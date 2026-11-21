"""Committed tests for DagsterApiCheckTools / DagsterApiCheckToolsComponent.

Nothing is mocked -- every check runs against the REAL installed `dagster` package (there is no
external/paid call to fake here; this component's whole job is introspecting the real thing).
Fixtures use names confirmed, in this session, to actually be deprecated/superseded in the
installed dagster version (`AutoMaterializePolicy` deprecated, `MultiAssetSensorDefinition`
superseded, `AssetOut(auto_materialize_policy=...)` a deprecated parameter) -- not guessed.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_check_dagster_api_finds_a_real_name_with_a_signature(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_check_dagster_api(["dagster.AssetKey"])
    assert result["names"]["dagster.AssetKey"]["exists"] is True
    assert result["names"]["dagster.AssetKey"]["signature"]


def test_check_dagster_api_aliases_dg_prefix_to_dagster(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_check_dagster_api(["dg.AssetKey"])
    assert result["names"]["dg.AssetKey"]["exists"] is True


def test_check_dagster_api_reports_a_fictional_name_as_missing(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_check_dagster_api(["dagster.AssetAutoMaterializePolicy"])
    assert result["names"]["dagster.AssetAutoMaterializePolicy"]["exists"] is False


def test_scan_source_flags_a_removed_import(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "from dagster import ThisNameDoesNotExist123\n")
    [f] = result["findings"]
    assert f["status"] == "removed" and "ThisNameDoesNotExist123" in f["name"]


def test_scan_source_flags_a_deprecated_import(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "from dagster import AutoMaterializePolicy\n")
    [f] = result["findings"]
    assert f["status"] == "deprecated" and f["name"] == "dagster.AutoMaterializePolicy"
    assert f["breaking_version"]


def test_scan_source_flags_a_superseded_import(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "from dagster import MultiAssetSensorDefinition\n")
    [f] = result["findings"]
    assert f["status"] == "superseded" and f["name"] == "dagster.MultiAssetSensorDefinition"


def test_scan_source_flags_a_deprecated_import_via_attribute_chain(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "import dagster\nx = dagster.AutoMaterializePolicy\n")
    assert any(f["status"] == "deprecated" and f["name"] == "dagster.AutoMaterializePolicy" for f in result["findings"])


def test_scan_source_flags_a_deprecated_call_keyword_argument(mod):
    tools = mod.DagsterApiCheckTools()
    source = "from dagster import AssetOut\no = AssetOut(auto_materialize_policy=None)\n"
    result = tools.t_scan_source("a.py", source)
    param_findings = [f for f in result["findings"] if f["status"] == "deprecated parameter"]
    assert any("auto_materialize_policy" in f["name"] for f in param_findings)


def test_scan_source_reports_the_installed_dagster_version(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "")
    assert result["checked_against_dagster"] == dg.__version__
    assert result["findings"] == []


def test_scan_source_handles_a_syntax_error_without_crashing(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.t_scan_source("a.py", "def broken(:\n")
    assert result["findings"] == []


def test_scan_sources_dedups_and_sorts_across_files(mod):
    tools = mod.DagsterApiCheckTools()
    files = [
        {"path": "b.py", "source": "from dagster import AutoMaterializePolicy\n"},
        {"path": "a.py", "source": "from dagster import AutoMaterializePolicy\nfrom dagster import MultiAssetSensorDefinition\n"},
    ]
    result = tools.t_scan_sources(files)
    assert result["files_scanned"] == 2
    assert [f["path"] for f in result["findings"]] == ["a.py", "a.py", "b.py"]


def test_call_dispatch_unknown_tool(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.call("delete_everything", {})
    assert "error" in result and "unknown tool" in result["error"]


def test_call_dispatch_tool_not_in_allowlist(mod):
    tools = mod.DagsterApiCheckTools(allow=["check_dagster_api"])
    result = tools.call("scan_source", {"path": "a.py", "source": ""})
    assert "error" in result and "not enabled" in result["error"]


def test_call_dispatch_bad_arguments(mod):
    tools = mod.DagsterApiCheckTools()
    result = tools.call("check_dagster_api", {"not_a_real_arg": 1})
    assert "error" in result and "bad arguments" in result["error"]


def test_max_result_chars_truncates_a_large_result(mod):
    tools = mod.DagsterApiCheckTools(max_result_chars=2000)
    files = [{"path": f"f{i}.py", "source": "from dagster import AutoMaterializePolicy\n"} for i in range(50)]
    result = tools.call("scan_sources", {"files": files})
    assert result.get("truncated") is True
