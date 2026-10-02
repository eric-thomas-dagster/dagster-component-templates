"""Committed regression tests for PayscaleCompensationEnrichmentComponent.

Monkeypatches the component module's one external-call boundary,
`_fetch_compensation_report(resource, answers)` -- per-row answer building,
Pay-report flattening, per-row error isolation, and the full asset
materialization path are all exercised for real. No real `payscale_resource`
or HTTP call is ever needed.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeResource, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


# --- _build_answers -----------------------------------------------------------

def test_build_answers_required_and_default_country(mod):
    row = {"title": "Software Developer"}
    answers = mod._build_answers(
        row, "title", None, None, None, "United States", None, None, None
    )
    assert answers == {"JobTitle": "Software Developer", "Country": "United States"}


def test_build_answers_includes_optional_fields_when_present(mod):
    row = {
        "title": "Software Developer",
        "city": "Seattle",
        "state": "Washington",
        "country": "United States",
        "yoe": 5,
        "degree": "Bachelor's Degree",
    }
    answers = mod._build_answers(
        row, "title", "city", "state", "country", "United States", "yoe", "degree", None
    )
    assert answers == {
        "JobTitle": "Software Developer",
        "City": "Seattle",
        "State": "Washington",
        "Country": "United States",
        "YearsExperience": 5,
        "HighestDegreeEarned": "Bachelor's Degree",
    }


def test_build_answers_skips_blank_optional_fields_and_falls_back_country(mod):
    row = {"title": "Analyst", "city": "", "state": None, "country": "  "}
    answers = mod._build_answers(
        row, "title", "city", "state", "country", "United States", None, None, None
    )
    assert answers == {"JobTitle": "Analyst", "Country": "United States"}


def test_build_answers_skills_from_comma_string(mod):
    row = {"title": "Engineer", "skills": "Python, JavaScript,  SQL "}
    answers = mod._build_answers(
        row, "title", None, None, None, "United States", None, None, "skills"
    )
    assert answers["Skills"] == ["Python", "JavaScript", "SQL"]


def test_build_answers_skills_from_list(mod):
    row = {"title": "Engineer", "skills": ["Python", "Go"]}
    answers = mod._build_answers(
        row, "title", None, None, None, "United States", None, None, "skills"
    )
    assert answers["Skills"] == ["Python", "Go"]


def test_build_answers_years_experience_coerced_to_int(mod):
    row = {"title": "Engineer", "yoe": "7.0"}
    answers = mod._build_answers(
        row, "title", None, None, None, "United States", "yoe", None, None
    )
    assert answers["YearsExperience"] == 7
    assert isinstance(answers["YearsExperience"], int)


# --- _flatten_pay_report -------------------------------------------------------

_SAMPLE_REPORT = {
    "BasePayReport": {
        "Percentile10": 70000,
        "Percentile25": 80000,
        "Percentile50": 95000,
        "Percentile75": 110000,
        "Percentile90": 130000,
        "Average": 97000,
        "CurrencyName": "USD",
    },
    "TotalPayReport": {
        "Percentile10": 75000,
        "Percentile50": 100000,
        "Percentile90": 140000,
    },
    "ReportRating": 0.9,
    "TotalProfilesAnalyzed": 42,
    "Context": {"MatchedJobTitle": "Software Developer I", "JobTitleRating": 0.95},
}


def test_flatten_pay_report_base_pay_fields(mod):
    flat = mod._flatten_pay_report(_SAMPLE_REPORT, include_total_pay=False)
    assert flat["median_base_pay"] == 95000
    assert flat["base_pay_p10"] == 70000
    assert flat["base_pay_p90"] == 130000
    assert flat["currency"] == "USD"
    assert flat["report_rating"] == 0.9
    assert flat["matched_job_title"] == "Software Developer I"
    assert "median_total_pay" not in flat


def test_flatten_pay_report_includes_total_pay_when_enabled(mod):
    flat = mod._flatten_pay_report(_SAMPLE_REPORT, include_total_pay=True)
    assert flat["median_total_pay"] == 100000
    assert flat["total_pay_p10"] == 75000
    assert flat["total_pay_p90"] == 140000


def test_flatten_pay_report_handles_missing_subreports(mod):
    flat = mod._flatten_pay_report({}, include_total_pay=True)
    assert flat["median_base_pay"] is None
    assert flat["median_total_pay"] is None


# --- _fetch_compensation_report ------------------------------------------------

def test_fetch_compensation_report_delegates_to_resource(mod):
    resource = FakeResource()
    answers = {"JobTitle": "Engineer", "Country": "United States"}
    result = mod._fetch_compensation_report(resource, answers)
    assert resource.calls == [answers]
    assert result == {"BasePayReport": {"Percentile50": 90000}}


# --- full asset materialization -------------------------------------------------

def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="compensation_benchmarked_roles",
        upstream_asset_key="open_roles",
        resource_key="payscale_resource",
        job_title_column="job_title",
        city_column="city",
        country_column="country",
    )
    kwargs.update(overrides)
    return mod.PayscaleCompensationEnrichmentComponent(**kwargs)


def test_asset_appends_columns_and_metadata(mod, monkeypatch):
    component = _make_component(mod)
    defs = component.build_defs(context=None)

    def fake_fetch(resource, answers):
        return _SAMPLE_REPORT

    monkeypatch.setattr(mod, "_fetch_compensation_report", fake_fetch)

    upstream_df = pd.DataFrame(
        {
            "job_title": ["Software Developer", "Data Analyst"],
            "city": ["Seattle", "Austin"],
            "country": ["United States", "United States"],
        }
    )

    @dg.asset(name="open_roles")
    def open_roles():
        return upstream_df

    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def, open_roles],
        resources={"payscale_resource": object()},
    )
    assert result.success

    df = result.output_for_node("compensation_benchmarked_roles")
    assert len(df) == 2
    assert df["payscale_median_base_pay"].tolist() == [95000, 95000]
    assert df["payscale_median_total_pay"].tolist() == [100000, 100000]
    assert df["payscale_matched_job_title"].tolist() == ["Software Developer I"] * 2
    assert df["payscale_error"].isna().all()
    # Original upstream columns preserved
    assert "job_title" in df.columns and "city" in df.columns


def test_asset_continue_on_error_nulls_row_and_records_error(mod, monkeypatch):
    component = _make_component(mod, continue_on_error=True)
    defs = component.build_defs(context=None)

    def fake_fetch(resource, answers):
        if answers["JobTitle"] == "Bad Title":
            raise RuntimeError("PayScale rejected the report request: ['Invalid JobTitle']")
        return _SAMPLE_REPORT

    monkeypatch.setattr(mod, "_fetch_compensation_report", fake_fetch)

    upstream_df = pd.DataFrame(
        {
            "job_title": ["Software Developer", "Bad Title"],
            "city": ["Seattle", "Nowhere"],
            "country": ["United States", "United States"],
        }
    )

    @dg.asset(name="open_roles")
    def open_roles():
        return upstream_df

    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def, open_roles],
        resources={"payscale_resource": object()},
    )
    assert result.success

    df = result.output_for_node("compensation_benchmarked_roles")
    assert df["payscale_median_base_pay"].iloc[0] == 95000
    assert pd.isna(df["payscale_median_base_pay"].iloc[1])
    assert pd.isna(df["payscale_error"].iloc[0])
    assert "Invalid JobTitle" in df["payscale_error"].iloc[1]


def test_asset_continue_on_error_false_raises(mod, monkeypatch):
    component = _make_component(mod, continue_on_error=False)
    defs = component.build_defs(context=None)

    def fake_fetch(resource, answers):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_fetch_compensation_report", fake_fetch)

    upstream_df = pd.DataFrame({"job_title": ["Engineer"], "city": ["Seattle"], "country": ["United States"]})

    @dg.asset(name="open_roles")
    def open_roles():
        return upstream_df

    asset_def = list(defs.assets)[0]
    with pytest.raises(Exception):
        dg.materialize(
            [asset_def, open_roles],
            resources={"payscale_resource": object()},
        )


def test_asset_requires_payscale_resource_key(mod, monkeypatch):
    component = _make_component(mod, resource_key="custom_payscale")
    defs = component.build_defs(context=None)

    def fake_fetch(resource, answers):
        return _SAMPLE_REPORT

    monkeypatch.setattr(mod, "_fetch_compensation_report", fake_fetch)

    upstream_df = pd.DataFrame({"job_title": ["Engineer"], "city": ["Seattle"], "country": ["United States"]})

    @dg.asset(name="open_roles")
    def open_roles():
        return upstream_df

    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def, open_roles],
        resources={"custom_payscale": object()},
    )
    assert result.success
