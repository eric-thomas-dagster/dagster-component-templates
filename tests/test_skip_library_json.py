"""The DAGSTER_COMMUNITY_COMPONENTS_SKIP_LIBRARY_JSON opt-in must hide the
library from Dagster's `dir()`-based entry-point discovery (so local tooling
skips the all-component library-JSON build) without breaking by-name lookup
(`type: dagster_community_components.X` in a defs.yaml)."""
import importlib

import pytest

import dagster_community_components as pkg

ENV = pkg.SKIP_LIBRARY_JSON_ENV


def test_dir_lists_components_by_default(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    names = dir(pkg)
    assert len(names) > 1000
    assert "CronScheduleComponent" in names


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_dir_is_empty_when_opted_in(monkeypatch, value):
    monkeypatch.setenv(ENV, value)
    assert dir(pkg) == []


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_falsey_values_do_not_opt_in(monkeypatch, value):
    monkeypatch.setenv(ENV, value)
    assert "CronScheduleComponent" in dir(pkg)


def test_lookup_by_name_still_works_when_opted_in(monkeypatch):
    monkeypatch.setenv(ENV, "1")
    cls = getattr(pkg, "CronScheduleComponent")
    assert cls.__name__ == "CronScheduleComponent"
    assert importlib.import_module("dagster_community_components") is pkg
