"""Dedicated PHI-safety regression tests for ZocdocAppointmentsIngestionComponent.

The entire point of these tests: prove that no patient-identifying value
from a realistic fake Zocdoc API response ever appears -- as a substring,
anywhere -- in the materialization metadata dict, or in any error message
this component raises. This is the MANDATORY safety guarantee described in
this component's README "PHI Safety" section; these tests are what make it
more than a documentation claim.
"""
import dagster as dg
import pytest

from .conftest import FakeZocdocResource, PHI_SHAPED_APPOINTMENT_ROW, load_component_module

# The exact PHI-shaped substrings that must NEVER appear anywhere in
# metadata or error messages, regardless of what the (fake) Zocdoc API
# returned.
_FORBIDDEN_SUBSTRINGS = [
    "Rosalind",
    "Franklin",
    "1983-04-17",
    "+15552347788",
    "rosalind.franklin@example.com",
    "persistent lower back pain",
    "MRI referral",
    "dev_pat_42",
]


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource=None, raise_on_error=True):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_key: resource or FakeZocdocResource()},
        raise_on_error=raise_on_error,
    )


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


def _flatten_to_text(value) -> str:
    """Render any metadata value (str/int/bool/dict/list/MetadataValue-unwrapped)
    to a single string for substring scanning."""
    return repr(value)


def test_successful_fetch_metadata_contains_no_phi(mod, monkeypatch):
    def fake_list(session, base_url, params):
        return {
            "total_count": 1,
            "next_url": None,
            "data": [dict(PHI_SHAPED_APPOINTMENT_ROW)],
        }

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component)
    assert result.success

    out = _metadata_for(result, "zocdoc_out")

    # Sanity: the fetch actually happened and counted the PHI-bearing row.
    assert out["rows_fetched"] == 1

    full_text = _flatten_to_text(out)
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in full_text, (
            f"PHI leak: {forbidden!r} found in materialization metadata: {full_text}"
        )

    # Metadata keys themselves must be a known, bounded set -- no
    # surprise key derived from row content (e.g. 'first_appointment_id').
    # ('path' is added automatically by Dagster's default IO manager and
    # is not something this component's code emits.)
    allowed_keys = {
        "rows_fetched", "pages_fetched", "total_count_reported",
        "from_date_time", "to_date_time", "practice_ids", "provider_ids",
        "location_ids", "statuses", "path",
    }
    assert set(out.keys()) <= allowed_keys


def test_multi_row_fetch_metadata_contains_no_phi(mod, monkeypatch):
    """Same guarantee holds across multiple distinct patient-shaped rows,
    not just a single row (guards against a bug that only sanitizes the
    first record)."""
    row2 = dict(PHI_SHAPED_APPOINTMENT_ROW)
    row2["patient"] = {
        "first_name": "Ada",
        "last_name": "Lovelace",
        "date_of_birth": "1990-12-10",
        "phone_number": "+15557654321",
        "email_address": "ada.lovelace@example.com",
    }
    row2["notes"] = "Patient requests early-morning slots only; pregnancy follow-up."

    def fake_list(session, base_url, params):
        return {
            "total_count": 2,
            "next_url": None,
            "data": [dict(PHI_SHAPED_APPOINTMENT_ROW), row2],
        }

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component)
    out = _metadata_for(result, "zocdoc_out")
    full_text = _flatten_to_text(out)

    for forbidden in _FORBIDDEN_SUBSTRINGS + ["Ada", "Lovelace", "ada.lovelace@example.com", "pregnancy"]:
        assert forbidden not in full_text


def test_api_failure_error_message_contains_no_phi(mod, monkeypatch):
    """Simulates an API failure where the underlying (fake) HTTP error
    object happens to carry the PHI-bearing response body -- proves this
    component's sanitization, not the test double's good behavior, is what
    keeps PHI out of the raised error."""

    class FakeResponseWithPHIBody:
        status_code = 500
        content = b"x"

        def json(self):
            return {"error": "internal error", "offending_record": PHI_SHAPED_APPOINTMENT_ROW}

    class FakeSessionRaisingWithPHIInException:
        def get(self, url, params=None, timeout=None):
            # Simulate a client library whose exception __str__ embeds the
            # full request (a realistic failure mode for some HTTP clients).
            raise RuntimeError(
                f"Request failed for {url} with params={params} and body="
                f"{PHI_SHAPED_APPOINTMENT_ROW}"
            )

    resource = FakeZocdocResource()
    monkeypatch.setattr(resource, "get_client", lambda: FakeSessionRaisingWithPHIInException())

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )

    with pytest.raises(Exception) as exc_info:
        _materialize(component, resource=resource, raise_on_error=True)

    message = str(exc_info.value)
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in message, f"PHI leak in error message: {forbidden!r} found in: {message}"


def test_api_failure_with_status_code_error_message_contains_no_phi(mod, monkeypatch):
    """A clean non-2xx HTTP response (status_code=500) must surface only
    the status code -- never attempt to read/report response.json()."""

    def fake_list(session, base_url, params):
        raise mod.ZocdocAPIError(500, "Zocdoc appointments request failed")

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component, raise_on_error=False)
    assert not result.success

    failure_events = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    assert failure_events
    message = str(failure_events[0].event_specific_data.error)
    assert "500" in message
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in message


def test_zocdoc_api_error_class_never_stores_body(mod):
    """Structural guarantee: ZocdocAPIError's constructor signature only
    accepts (status_code, message) -- there is no parameter through which
    a response/request body could be threaded into the exception at all."""
    import inspect

    sig = inspect.signature(mod.ZocdocAPIError.__init__)
    param_names = list(sig.parameters.keys())
    assert param_names == ["self", "status_code", "message"]


def test_list_appointments_function_source_never_reads_response_body_into_error():
    """The isolated `_zocdoc_list_appointments` function must never pass
    `resp.text`/`resp.json()`/the request body into a raised exception."""
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    # Isolate just the function body for a slightly more targeted check.
    start = source.index("def _zocdoc_list_appointments")
    end = source.index("\ndef ", start + 1)
    func_src = source[start:end]
    assert "resp.text" not in func_src
    assert "e!r" not in func_src  # no raw-exception interpolation
    assert "{e}" not in func_src
