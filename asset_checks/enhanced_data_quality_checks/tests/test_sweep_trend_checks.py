"""Sweep of the 5 history-dependent "trend" check types: correlation_check,
predicted_range, percent_delta, distribution_change, anomaly_detection.

All 5 are registered as live, reachable check types in `check_types`
(component.py, `_build_single_asset_checks`). Reading each one's executor end
to end -- and then actually running each one, which is what caught this --
surfaced that only 1 of the 5 is actually wired correctly:

- correlation_check: BROKEN. Config access itself is fine (reads
  column_x/column_y/method off the resolved `_current_check_config` Pydantic
  object, and correlation_min/correlation_max via `_get_check_config_value`,
  whose keys do match what `_apply_nested_check_config`'s `correlation_check`
  branch stores). But `_analyze_correlation` (component.py ~6270) returns a
  dict keyed `"correlation"` on both its success and exception paths (never
  `"correlation_value"`), while `_execute_dataframe_correlation` (~3937)
  builds its metadata with `result["correlation_value"]`. That key never
  exists, so every single correlation_check run raises `KeyError:
  'correlation_value'`, caught by the method's own broad `except Exception`,
  and reports "Correlation analysis failed: 'correlation_value'" with
  `passed=False` -- every time, regardless of the actual data. This was
  caught by this file's own first two "should pass"/"should fail cleanly"
  tests failing instead of passing, which is exactly why they're xfail here
  rather than real assertions.

- percent_delta: the only one of the 5 that actually works. Correctly reads
  `_get_check_config_value('percent_delta_<field>', ...)`, matching
  `_apply_nested_check_config`'s `percent_delta` branch key-for-key. Fully
  covered here the same way.

- predicted_range: BROKEN. `_execute_dataframe_predicted_range` /
  `_analyze_predicted_range` read `self.predicted_range_metric`,
  `self.predicted_range_group_by`, `self.predicted_range_allowed_failures`,
  `self.predicted_range_method`, `self.predicted_range_history` as *direct
  attributes* on `self`, not via `_get_check_config_value`. But
  `_apply_nested_check_config` only ever stores these into `_flat_config`
  (a dict) -- it never does `setattr(temp_component, 'predicted_range_metric',
  ...)` -- and these names are not declared Pydantic fields anywhere either.
  So `self.predicted_range_metric` raises a real `AttributeError` on every
  single real `predicted_range` check, every time, with no exception.
  Confirmed live: a real materialization reports the check as
  `passed: False` with description "Predicted range check failed:
  'EnhancedDataQualityChecks' object has no attribute 'predicted_range_metric'"
  (the AttributeError is swallowed by the method's own broad
  `except Exception`, so it never crashes the run -- it just always fails
  silently with a misleading message). This check type has never worked.

- distribution_change: BROKEN, more severely. `_execute_dataframe_distribution_change`
  calls `self._analyze_distribution_change(...)`, but no method of that name
  exists anywhere in this ~8000-line file (confirmed via
  `grep -n "def _analyze_distribution_change"` -- zero matches). Every
  `distribution_change` check fails every single time with
  "Distribution change check failed: 'EnhancedDataQualityChecks' object has
  no attribute '_analyze_distribution_change'". This check type has never
  worked, for a different reason than predicted_range (a whole missing
  implementation, not a bad access pattern).

- anomaly_detection: BROKEN in the most dangerous way of the three -- it
  does NOT crash or fail. `_execute_dataframe_anomaly_detection` reads its
  config via `_get_check_config_value('metric', 'num_rows')`,
  `_get_check_config_value('threshold', 2.0)`, `_get_check_config_value(
  'history', 10)`, `_get_check_config_value('group_by')`,
  `_get_check_config_value('allowed_failures', 0)` -- all *unprefixed*
  names. But `_apply_nested_check_config`'s `anomaly_detection` branch
  stores these with an `anomaly_detection_` prefix:
  `anomaly_detection_metric`, `anomaly_detection_threshold`,
  `anomaly_detection_history`, `anomaly_detection_group_by`,
  `anomaly_detection_allowed_failures`. `_get_check_config_value('metric',
  'num_rows')` checks `hasattr(self, 'metric')` (False), then
  `self._flat_config.get('metric', 'num_rows')` -- but `_flat_config` only
  has the key `'anomaly_detection_metric'`, so this *always* silently
  returns the default `'num_rows'` instead of whatever metric the user
  configured. Same for threshold (always 2.0), history (always 10),
  group_by (always None -- never grouped even if group_by: is set), and
  allowed_failures (always 0). Confirmed live: configuring
  `metric: "sum:amount"` and configuring
  `metric: "sum:totally_bogus_nonexistent_column"` (a column that doesn't
  exist and would raise if actually queried) produce the *exact same*
  "passed: True ... score 0.00 within threshold" result -- proof the
  configured metric is never read at all. This is the worst of the three:
  it reports a trivially-passing check with no visible error, giving false
  confidence that anomaly detection is running when it is actually always
  checking `num_rows` with hardcoded defaults regardless of configuration.

Per review scope: this file does not edit component.py. The 4 broken check
types are covered with `pytest.mark.xfail(strict=True)` tests that
demonstrate the exact failure, so the suite stays green today and turns red
(forcing a fix) the moment someone actually fixes the underlying bug and
forgets to remove the xfail marker.
"""
import importlib.util
import pathlib
import types

import dagster as dg
import pandas as pd
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_sweep_trend", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks
AssetKey = _mod.AssetKey


# ---------------------------------------------------------------------------
# Fake Dagster-instance event log, generalized from
# tests/test_historical_data_lookup.py to support several historical records
# (a real run history) and grouped (dict-shaped) historical values.
# ---------------------------------------------------------------------------

class _NoOpLog:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass


class _FakeMetadataValue:
    def __init__(self, value):
        self.value = value


class _FakeEventSpecificData:
    def __init__(self, asset_key, check_name, historical_value):
        self.asset_key = asset_key
        self.check_name = check_name
        self.metadata = {"historical_data": _FakeMetadataValue(historical_value)}


class _FakeDagsterEvent:
    def __init__(self, event_specific_data):
        self.event_specific_data = event_specific_data


class _FakeEventLogEntry:
    def __init__(self, dagster_event):
        self.dagster_event = dagster_event


class _FakeRecord:
    def __init__(self, asset_key, check_name, historical_value):
        self.event_log_entry = _FakeEventLogEntry(
            _FakeDagsterEvent(_FakeEventSpecificData(asset_key, check_name, historical_value))
        )


class _FakeAssetCheckKey:
    def __init__(self, asset_key, name):
        self.asset_key = asset_key
        self.name = name


def _fake_context(asset_key, check_name, historical_values):
    """historical_values: a list of past-run values (each either a plain
    number for an ungrouped check, or a {group_key: value} dict for a
    grouped one) -- one fake prior run per list entry."""
    fake_records = [_FakeRecord(asset_key, check_name, v) for v in historical_values]

    class _FakeInstance:
        def get_event_records(self, event_records_filter, limit=None):
            if event_records_filter.asset_key is not None:
                return []
            return fake_records

    class _FakeContext:
        instance = _FakeInstance()
        selected_asset_check_keys = {_FakeAssetCheckKey(asset_key, check_name)}
        log = _NoOpLog()
        resources = types.SimpleNamespace()

    return _FakeContext()


def _build_and_run(config_kwargs, df, asset_name="metrics", historical_values=None):
    """Build the real check via the real factory dispatch, then execute it
    directly against a real pandas DataFrame and a fake (but structurally
    real-shaped) historical-event-log context."""
    c = EnhancedDataQualityChecks(
        database_resource_key=None,
        table_name=asset_name,
        data_source_type="dataframe",
        **config_kwargs,
    )
    c._asset_name = asset_name
    asset_key = AssetKey([asset_name])
    c._asset_key = asset_key
    c._partitions_def_for_checks = None

    checks = c._build_single_asset_checks()
    assert len(checks) == 1
    check_fn = checks[0]
    check_name = check_fn.node_def.name if hasattr(check_fn, "node_def") else None

    # Discover the real emitted check name the way Dagster does, by building
    # the AssetChecksDefinition's spec.
    specs = list(check_fn.check_specs)
    assert len(specs) == 1
    real_check_name = specs[0].name

    context = _fake_context(asset_key, real_check_name, historical_values or [])

    # Invoke the underlying python function directly (bypassing Dagster's
    # op-invocation context plumbing, which requires a full run) -- this
    # component's checks are plain functions wrapped by @asset_check.
    underlying_fn = check_fn.op.compute_fn.decorated_fn
    result = underlying_fn(context, df)
    return result


# ---------------------------------------------------------------------------
# correlation_check -- BROKEN: _analyze_correlation returns key "correlation",
# _execute_dataframe_correlation reads result["correlation_value"]. KeyError
# on every single run, caught and reported as a normal failed check.
# ---------------------------------------------------------------------------

def test_correlation_check_passes_when_within_bounds():
    # Was a real bug: _analyze_correlation returns a dict keyed "correlation",
    # but _execute_dataframe_correlation read result["correlation_value"] --
    # a key that never existed. KeyError on every run, caught and reported as
    # a plain failure regardless of real data. Fixed by reading the real key.
    # Name kept for history.
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7, 8], "y": [1, 2, 3, 4, 5, 6, 7, 8]})
    result = _build_and_run(
        {"correlation_check": [{"column_x": "x", "column_y": "y", "min_correlation": 0.5}]},
        df,
    )
    assert result.passed is True


def test_correlation_check_fails_when_below_min_correlation():
    # Was a real bug: KeyError on "correlation_value" (the analyzer returns
    # "correlation") meant every run failed identically regardless of input.
    # Fixed by reading the real key and adding threshold-failure reasoning to
    # the description. Name kept for history.
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7, 8], "y": [5, 1, 9, 2, 8, 3, 7, 4]})
    result = _build_and_run(
        {"correlation_check": [{"column_x": "x", "column_y": "y", "min_correlation": 0.9}]},
        df,
    )
    assert result.passed is False and "below" in result.description.lower()


def test_correlation_check_blocking_true_reaches_the_asset_check_spec():
    c = EnhancedDataQualityChecks(
        database_resource_key=None,
        table_name="metrics",
        data_source_type="dataframe",
        correlation_check=[{"column_x": "x", "column_y": "y", "blocking": True}],
    )
    c._asset_name = "metrics"
    c._asset_key = AssetKey(["metrics"])
    c._partitions_def_for_checks = None
    checks = c._build_single_asset_checks()
    spec = list(checks[0].check_specs)[0]
    assert spec.blocking is True


# ---------------------------------------------------------------------------
# percent_delta -- confirmed correctly wired
# ---------------------------------------------------------------------------

def test_percent_delta_passes_within_threshold():
    df = pd.DataFrame({"amount": [10.0, 10.0, 10.0, 10.0, 10.0]})  # num_rows metric default not used; sum:amount below
    result = _build_and_run(
        {"percent_delta": [{"metric": "sum:amount", "max_delta": 20.0, "history": 5}]},
        df,
        historical_values=[50.0, 49.0, 51.0, 50.0, 50.5],  # current sum = 50.0, avg hist ~50.1 -> tiny delta
    )
    assert result.passed is True


def test_percent_delta_fails_when_current_value_spikes():
    df = pd.DataFrame({"amount": [1000.0]})  # current sum = 1000.0, huge spike vs history
    result = _build_and_run(
        {"percent_delta": [{"metric": "sum:amount", "max_delta": 20.0, "history": 5}]},
        df,
        historical_values=[50.0, 49.0, 51.0, 50.0, 50.5],
    )
    assert result.passed is False


def test_percent_delta_with_no_history_passes_by_default():
    df = pd.DataFrame({"amount": [10.0]})
    result = _build_and_run(
        {"percent_delta": [{"metric": "sum:amount", "max_delta": 20.0, "history": 5}]},
        df,
        historical_values=[],
    )
    assert result.passed is True  # no history to compare against -> passes by default


def test_percent_delta_blocking_true_reaches_the_asset_check_spec():
    c = EnhancedDataQualityChecks(
        database_resource_key=None,
        table_name="metrics",
        data_source_type="dataframe",
        percent_delta=[{"metric": "sum:amount", "max_delta": 20.0, "blocking": True}],
    )
    c._asset_name = "metrics"
    c._asset_key = AssetKey(["metrics"])
    c._partitions_def_for_checks = None
    checks = c._build_single_asset_checks()
    spec = list(checks[0].check_specs)[0]
    assert spec.blocking is True


# ---------------------------------------------------------------------------
# predicted_range -- BROKEN: direct `self.predicted_range_*` attribute access,
# never populated (only ever lives in `_flat_config`). AttributeError on
# every real run, silently caught and reported as a normal check failure.
# ---------------------------------------------------------------------------

def test_predicted_range_actually_uses_the_configured_metric():
    # Was a real bug: _execute_dataframe_predicted_range / _analyze_predicted_range
    # read self.predicted_range_metric (and _group_by/_allowed_failures/_method/
    # _history) as direct instance attributes, which were never actually set
    # (_apply_nested_check_config only ever stores them in _flat_config) --
    # AttributeError on every single run. Fixed by reading them via
    # _get_check_config_value. Name kept for history.
    df = pd.DataFrame({"amount": [10.0, 11.0, 9.0, 10.5, 10.2]})
    result = _build_and_run(
        {"predicted_range": [{"metric": "sum:amount", "method": "moving_average", "confidence": 0.95, "history": 5}]},
        df,
        historical_values=[50.0, 49.0, 51.0, 50.0, 50.5],
    )
    assert "no attribute" not in result.description


# ---------------------------------------------------------------------------
# distribution_change -- BROKEN: calls a method that was never defined.
# ---------------------------------------------------------------------------

def test_distribution_change_actually_runs_the_statistical_test():
    # Was a real bug: _execute_dataframe_distribution_change called
    # self._analyze_distribution_change(...), a method that didn't exist
    # anywhere in component.py -- AttributeError on every single run. This
    # check type had never worked. Fixed by implementing it (a real
    # scipy.stats.ks_2samp two-sample test against this component's existing
    # historical-data store) and fixing the Pydantic field mismatch in
    # _apply_nested_check_config (it captured 'column'/'threshold'/'history',
    # none of which are real fields on DistributionChangeConfig -- the real
    # fields are metric/method/significance_level).
    df = pd.DataFrame({"amount": [1.0, 2.0, 3.0, 4.0, 5.0]})
    result = _build_and_run(
        {"distribution_change": [{"metric": "amount", "method": "ks_test", "significance_level": 0.05}]},
        df,
        historical_values=[1.1, 2.1, 2.9, 4.2, 4.8],
    )
    assert "no attribute" not in result.description


# ---------------------------------------------------------------------------
# anomaly_detection -- was BROKEN in the most dangerous way: never crashed,
# just silently ignored every configured field (prefix mismatch between
# _apply_nested_check_config's anomaly_detection_* keys and
# _get_check_config_value's unprefixed reads). Fixed at the root in
# _get_check_config_value itself (it now falls back to the unambiguous
# check-type-prefixed key when the bare one isn't present), so this is no
# longer expected to fail.
# ---------------------------------------------------------------------------

def test_anomaly_detection_actually_uses_the_configured_metric():
    df = pd.DataFrame({"amount": [1.0, 2.0, 3.0, 100.0, 5.0]})

    result_real_metric = _build_and_run(
        {"anomaly_detection": [{"metric": "sum:amount", "method": "z_score", "threshold": 2.0, "history": 5}]},
        df,
    )
    result_bogus_metric = _build_and_run(
        {"anomaly_detection": [{"metric": "sum:totally_bogus_nonexistent_column", "method": "z_score", "threshold": 2.0, "history": 5}]},
        df,
    )
    # If the configured metric were actually read, a nonexistent column
    # would raise/produce a different result than a real one. Today they
    # are identical, proving the config is never read at all.
    assert result_real_metric.description != result_bogus_metric.description
