"""Committed tests for RegexParser's `log_format` (Apache Common/Combined log presets).

Sample lines are Apache's own documented examples (httpd.apache.org/docs/2.4/logs.html),
not invented — real fixtures, not mocks. Routed through the real asset graph via
dg.materialize(), nothing mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

COMMON_LINE = '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326'
COMBINED_LINE = (
    '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 '
    '"http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"'
)
MALFORMED_LINE = '127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "not a real request line" 400 -'


@pytest.fixture()
def mod():
    return load_component_module()


def _build_asset(mod, **kwargs):
    comp = mod.RegexParser(
        asset_name="parsed",
        column="line",
        mode="extract",
        **kwargs,
    )
    defs = comp.build_defs(load_context=None)
    return next(iter(defs.assets))


def test_common_log_format_fields(mod):
    df = pd.DataFrame({"line": [COMMON_LINE]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(mod, log_format="common", upstream_asset_key="src")
    result = dg.materialize(
        [src, parsed],
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    out = result.output_for_node("parsed")

    assert out.loc[0, "clientip"] == "127.0.0.1"
    assert out.loc[0, "ident"] == "-"
    assert out.loc[0, "auth"] == "frank"
    assert out.loc[0, "timestamp"] == "10/Oct/2000:13:55:36 -0700"
    assert out.loc[0, "verb"] == "GET"
    assert out.loc[0, "request"] == "/apache_pb.gif"
    assert out.loc[0, "httpversion"] == "1.0"
    assert out.loc[0, "response"] == "200"
    assert out.loc[0, "bytes"] == "2326"
    # combined-only fields must not appear for the common preset
    assert "referrer" not in out.columns
    assert "agent" not in out.columns


def test_combined_log_format_fields(mod):
    df = pd.DataFrame({"line": [COMBINED_LINE]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(mod, log_format="combined", upstream_asset_key="src")
    result = dg.materialize(
        [src, parsed],
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    out = result.output_for_node("parsed")

    assert out.loc[0, "verb"] == "GET"
    assert out.loc[0, "response"] == "200"
    assert out.loc[0, "referrer"] == "http://www.example.com/start.html"
    assert out.loc[0, "agent"] == "Mozilla/4.08 [en] (Win98; I ;Nav)"


def test_malformed_request_line_falls_back_to_rawrequest(mod):
    df = pd.DataFrame({"line": [MALFORMED_LINE]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(mod, log_format="common", upstream_asset_key="src")
    result = dg.materialize(
        [src, parsed],
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    out = result.output_for_node("parsed")

    assert pd.isna(out.loc[0, "verb"])
    assert out.loc[0, "rawrequest"] == "not a real request line"
    assert out.loc[0, "response"] == "400"
    assert out.loc[0, "bytes"] == "-"


def test_log_format_requires_extract_mode(mod):
    with pytest.raises(ValueError, match="requires mode='extract'"):
        mod.RegexParser(
            asset_name="parsed",
            column="line",
            mode="match",
            log_format="common",
            upstream_asset_key="src",
        ).build_defs(load_context=None)


def test_missing_pattern_and_log_format_raises(mod):
    with pytest.raises(ValueError, match="set `pattern`, or `log_format`"):
        mod.RegexParser(
            asset_name="parsed",
            column="line",
            mode="extract",
            upstream_asset_key="src",
        ).build_defs(load_context=None)
