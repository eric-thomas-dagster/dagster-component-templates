"""Shared test helpers for ZocdocAppointmentsIngestionComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeZocdocResource` stands in for the
real ZocdocResource -- its `get_client()`/`get_base_url()` are the only
surface the component touches directly; the one external HTTP call
(`_zocdoc_list_appointments`) is monkeypatched wholesale in tests.

`PHI_SHAPED_APPOINTMENT_ROW` is a realistic fake Zocdoc appointment record
-- modeled on the fields Zocdoc's create-appointment/patient object
documents (first_name, last_name, date_of_birth, phone_number,
email_address, free-text notes) -- used by test_phi_safety.py to prove
none of these values ever leak into materialization metadata.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "zocdoc_appointments_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeZocdocResource:
    """Stands in for the real ZocdocResource. `.get_client()` returns a
    bare object (never a real requests.Session) since the actual HTTP call
    is monkeypatched wholesale at the `_zocdoc_list_appointments` level in
    every test -- this session object is never used for a real network
    call."""

    def __init__(self, base_url="https://api-developer-sandbox.zocdoc.com/"):
        self._base_url = base_url

    def get_client(self):
        return object()

    def get_base_url(self):
        return self._base_url


# A realistic, PHI-shaped fake Zocdoc appointment record. These exact
# string values are what test_phi_safety.py scans for in the metadata.
PHI_SHAPED_APPOINTMENT_ROW = {
    "appointment_id": "appt_abc123",
    "appointment_status": "booked",
    "provider_id": "prov_999",
    "location_id": "loc_888",
    "start_time": "2026-06-15T09:30:00Z",
    "visit_reason_id": "vr_annual_physical",
    "developer_patient_id": "dev_pat_42",
    "patient": {
        "first_name": "Rosalind",
        "last_name": "Franklin",
        "date_of_birth": "1983-04-17",
        "phone_number": "+15552347788",
        "email_address": "rosalind.franklin@example.com",
    },
    "notes": "Patient reports persistent lower back pain radiating to left leg; follow-up MRI referral discussed.",
}
