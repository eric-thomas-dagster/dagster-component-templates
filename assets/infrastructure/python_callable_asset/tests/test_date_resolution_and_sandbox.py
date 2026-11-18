"""Tests for the date-resolution priority fix and the Jinja sandbox.

Real gap found via external feedback: context_mode='airflow' on an un-partitioned asset
silently fell back to datetime.now(utc) with no trace, and used a plain (non-sandboxed)
jinja2.Environment. Fixed: run tags (set by cron_schedule's/interval_schedule's own
add_logical_date_tag/add_interval_tags) take priority over the partition window, which takes
priority over a now() fallback that's loud by default (on_missing_date='warn', logs a clear
warning) and can be made to fail outright (on_missing_date='fail').
"""
import importlib
from datetime import datetime, timezone

import dagster as dg
import pytest

from .conftest import load_component_module, materialize

FIXTURES_PATH = "assets.infrastructure.python_callable_asset.tests.fixture_callables"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


# ---- run-tag priority ----

def test_logical_date_run_tag_takes_priority_over_now(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
    )
    result = materialize(component, tags={"logical_date": "2026-03-05T00:00:00+00:00"})
    assert result.success
    assert fx.CALLS["ds"] == "2026-03-05"


def test_interval_tags_resolve_full_context_without_a_partition(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_full_context", context_mode="airflow",
    )
    result = materialize(component, tags={
        "data_interval_start": "2026-03-05T00:00:00+00:00",
        "data_interval_end": "2026-03-06T00:00:00+00:00",
    })
    assert result.success
    assert fx.CALLS["data_interval_start"] == datetime(2026, 3, 5, tzinfo=timezone.utc)
    assert fx.CALLS["data_interval_end"] == datetime(2026, 3, 6, tzinfo=timezone.utc)
    # airflow_version defaults to 2 -> logical_date = interval start
    assert fx.CALLS["logical_date"] == datetime(2026, 3, 5, tzinfo=timezone.utc)


def test_logical_date_tag_used_verbatim_even_with_interval_tags_present(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_full_context", context_mode="airflow",
    )
    result = materialize(component, tags={
        "logical_date": "2026-03-05T12:00:00+00:00",
        "data_interval_start": "2026-03-05T00:00:00+00:00",
        "data_interval_end": "2026-03-06T00:00:00+00:00",
    })
    assert result.success
    assert fx.CALLS["logical_date"] == datetime(2026, 3, 5, 12, tzinfo=timezone.utc)


# ---- on_missing_date: no tags, no partition ----

def test_on_missing_date_warn_falls_back_to_now_with_a_warning(mod, fx, caplog):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
        on_missing_date="warn",
    )
    before = datetime.now(timezone.utc)
    result = materialize(component)
    assert result.success
    resolved = datetime.strptime(fx.CALLS["ds"], "%Y-%m-%d")
    assert resolved.date() == before.date()


def test_on_missing_date_fail_raises(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
        on_missing_date="fail",
    )
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_on_missing_date_defaults_to_warn_not_fail(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
    )
    assert component.on_missing_date == "warn"
    result = materialize(component)
    assert result.success  # old behavior preserved by default -- still falls back, just loudly


# ---- partition window still works, and still beats the now() fallback ----

def test_partition_window_used_when_no_run_tags_present(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:wants_ds_only", context_mode="airflow",
        partition_type="daily", partition_start="2020-01-01",
    )
    result = materialize(component, partition_key="2026-03-05")
    assert result.success
    assert fx.CALLS["ds"] == "2026-03-05"


# ---- Jinja sandbox ----

def test_sandboxed_jinja_blocks_unsafe_attribute_access_on_a_whitelisted_identifier(mod, fx):
    # "my_param" is NOT one of the auto-injected airflow context names, so it survives
    # through to Jinja rendering untouched -- unlike "ds"/"params" themselves, which the
    # airflow-context injection step would overwrite before Jinja ever saw the template.
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        kwargs={"my_param": "{{ params.__class__.__mro__ }}"},
    )
    result = materialize(component, tags={"logical_date": "2026-03-05T00:00:00+00:00"}, raise_on_error=False)
    assert not result.success


def test_sandboxed_jinja_still_renders_whitelisted_templates_normally(mod, fx):
    component = mod.PythonCallableAssetComponent(
        asset_key="d/t", callable_path=f"{FIXTURES_PATH}:record_args_kwargs", context_mode="airflow",
        kwargs={"my_param": "day-{{ ds }}"},
    )
    result = materialize(component, tags={"logical_date": "2026-03-05T00:00:00+00:00"})
    assert result.success
    assert fx.CALLS["kwargs"]["my_param"] == "day-2026-03-05"
