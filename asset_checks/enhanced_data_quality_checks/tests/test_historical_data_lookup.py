"""Regression test for a real bug found while adding a percent_delta check
with a custom `name:` to a quickstart: `_load_historical_data`'s Strategy 3
fallback (the only working path, since Strategy 1/2's asset_key-filtered
query is separately broken -- see that code's own comments) reconstructed
the expected check_name as f"{asset}_{check_type}" (e.g.
"raw_orders_percent_delta") instead of using the REAL, already-resolved
check name off the current check spec. A check configured with a custom
`name:` (BaseCheckConfig.name) does not emit that reconstructed name, so
the match never succeeded and historical lookup silently returned {} every
single run -- the check always reported "0.00%" instead of a real,
varying percent-delta, with no error to flag why. Affects every
history-dependent check type that funnels through _load_historical_data
with a custom name: percent_delta, anomaly_detection, correlation_check,
predicted_range.

Confirmed live against a real, persistent Dagster instance before writing
this test: 4 sequential materializations of a percent_delta check named
"raw_orders_revenue_trend" each logged "Historical data: {}" despite the
prior runs' evaluations being genuinely present in the event log (found by
querying get_event_records with no asset_key filter at all).

Caught a second real issue fixing the first: the initial attempt read the
real check name off `context.asset_check_spec.name` -- which does not
exist on AssetCheckExecutionContext in the installed dagster version, so
it silently fell through the fix's own except clause back to the broken
reconstruction, with no visible error (confirmed by rerunning live after
the "fix" and seeing the exact same "raw_orders_percent_delta" wrong name
logged). The real, per-execution attribute is `selected_asset_check_keys`
-- the check(s) targeted by the current step; this component always runs
one check per step, so it's a singleton here.
"""
import importlib.util
import pathlib

import dagster as dg

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_historical", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


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


def _fake_context_and_instance(asset_key, real_check_name, historical_value):
    """A fake AssetCheckExecutionContext whose instance mirrors the real,
    documented Dagster asset_key-filter bug this component already works
    around: an asset_key-filtered query returns nothing, but the
    unfiltered (Strategy 3) query returns the real historical record."""
    fake_record = _FakeRecord(asset_key, real_check_name, historical_value)

    class _FakeInstance:
        def get_event_records(self, event_records_filter, limit=None):
            if event_records_filter.asset_key is not None:
                return []
            return [fake_record]

    class _FakeContext:
        instance = _FakeInstance()
        selected_asset_check_keys = {_FakeAssetCheckKey(asset_key, real_check_name)}
        log = _NoOpLog()

    return _FakeContext()


def test_historical_lookup_uses_the_real_check_name_not_a_reconstructed_one():
    asset_key = dg.AssetKey(["raw_orders"])
    custom_name = "raw_orders_revenue_trend"  # NOT "raw_orders_percent_delta"

    c = EnhancedDataQualityChecks.__new__(EnhancedDataQualityChecks)
    c._asset_key = asset_key  # matches how the real component resolves this
    context = _fake_context_and_instance(asset_key, custom_name, 111040.93)

    historical = c._load_historical_data(
        context, "percent_delta", "sum:total", group_by=None, history=5
    )

    assert historical == {"__total__": [111040.93]}


def test_historical_lookup_still_works_with_the_default_generated_name():
    """Non-regression: the common case (no custom `name:` set, so the real
    check name happens to equal the reconstructed "{asset}_{check_type}"
    convention) must keep working too."""
    asset_key = dg.AssetKey(["raw_orders"])
    default_name = "raw_orders_percent_delta"

    c = EnhancedDataQualityChecks.__new__(EnhancedDataQualityChecks)
    c._asset_key = asset_key
    context = _fake_context_and_instance(asset_key, default_name, 555.2)

    historical = c._load_historical_data(
        context, "percent_delta", "sum:total", group_by=None, history=5
    )

    assert historical == {"__total__": [555.2]}
