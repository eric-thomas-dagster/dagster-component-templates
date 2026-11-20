"""Committed tests for SapJobTriggerAssetComponent.

Mocks ONLY the sap_rfc_resource's `.connection()` context manager + `.call()`
(a fake RFC responder keyed by RFC/BAPI name, same pattern as
sap_rfc_ingestion's own tests) -- every other behavior (XMI logon/close
sequencing, BAPI RETURN-table error checking, the poll loop, run_window,
dry_run, metadata) is exercised for real through dg.materialize().

`FakeRFCConnection` is strict for the XBP (`BAPI_XMI_*`/`BAPI_XBP_*`) family: it
only accepts the function names and parameters SAP's own XBP 6.10 interface
documentation actually has (`DOCUMENTED`/`REQUIRED` below), the same way a real
RFC connection would reject an undocumented function module or parameter. This
is deliberate -- an earlier version of this component called function modules
and parameters the XBP interface doesn't have at all (e.g. `BAPI_XBP_JOB_ADD_STEP`,
a `START_IMMEDIATELY` flag on `_CLOSE`) and a lenient mock let that ship
unnoticed. `RSPC_API_*` (process_chain mode) isn't a documented external
interface the way XBP is, so it isn't checked this strictly here.
"""
import contextlib

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


# Function name -> allowed parameter names, per SAP's XBP 6.10 interface documentation.
XBP_DOCUMENTED = {
    "BAPI_XMI_LOGON": {"EXTCOMPANY", "EXTPRODUCT", "INTERFACE", "VERSION"},
    "BAPI_XMI_CLOSE": {"INTERFACE"},
    "BAPI_XBP_JOB_OPEN": {"JOBNAME", "EXTERNAL_USER_NAME", "JOBCLASS"},
    "BAPI_XBP_JOB_ADD_ABAP_STEP": {
        "JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME", "ABAP_PROGRAM_NAME", "ABAP_VARIANT_NAME",
        "SAP_USER_NAME", "LANGUAGE",
    },
    "BAPI_XBP_JOB_CLOSE": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME", "TARGET_SERVER"},
    "BAPI_XBP_JOB_START_IMMEDIATELY": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME", "TARGET_SERVER"},
    "BAPI_XBP_JOB_STATUS_GET": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME"},
    "BAPI_XBP_JOB_JOBLOG_READ": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME"},
}
XBP_REQUIRED = {
    "BAPI_XMI_LOGON": {"EXTCOMPANY", "EXTPRODUCT"},
    "BAPI_XBP_JOB_OPEN": {"JOBNAME", "EXTERNAL_USER_NAME"},
    "BAPI_XBP_JOB_ADD_ABAP_STEP": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME", "ABAP_PROGRAM_NAME"},
    "BAPI_XBP_JOB_CLOSE": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME"},
    "BAPI_XBP_JOB_START_IMMEDIATELY": {"JOBNAME", "JOBCOUNT", "EXTERNAL_USER_NAME"},
    "BAPI_XBP_JOB_STATUS_GET": {"JOBNAME", "JOBCOUNT"},
}


class FakeRFCConnection:
    """Maps an RFC/BAPI name to a canned response. A list is consumed one value
    per call (the last value repeats once exhausted) -- used to simulate a
    status poll going RUNNING -> FINISHED/ABORTED across repeated calls.

    For any `BAPI_XMI_*`/`BAPI_XBP_*` call, also enforces SAP's documented XBP
    interface shape (function exists, no undocumented parameters, no missing
    required ones) -- an RFC call to an undocumented function module or
    parameter would fail the same way on a real system."""

    def __init__(self, responses: dict):
        self.responses = {k: (list(v) if isinstance(v, list) else v) for k, v in responses.items()}
        self.calls: list = []

    def call(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if name in XBP_DOCUMENTED:
            bad = set(kwargs) - XBP_DOCUMENTED[name]
            if bad:
                raise AssertionError(f"{name}: parameter(s) not in SAP's documented XBP interface: {sorted(bad)}")
            missing = XBP_REQUIRED.get(name, set()) - set(kwargs)
            if missing:
                raise AssertionError(f"{name}: missing required parameter(s): {sorted(missing)}")
        elif name.startswith("BAPI_XMI_") or name.startswith("BAPI_XBP_"):
            raise AssertionError(f"{name}: not a function module in SAP's documented XBP interface")
        resp = self.responses.get(name)
        if resp is None:
            raise AssertionError(f"FakeRFCConnection: no canned response for {name!r}; kwargs={kwargs}")
        if isinstance(resp, list):
            return resp.pop(0) if len(resp) > 1 else resp[0]
        return resp


class FakeRFCResource:
    def __init__(self, conn: FakeRFCConnection):
        self._conn = conn

    @contextlib.contextmanager
    def connection(self):
        yield self._conn


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_key="sap/nightly_close",
        sap_rfc_resource_key="sap_ecc",
        target_type="background_job",
        job_name="ZNIGHTLY_CLOSE",
        program_name="ZNIGHTLY_CLOSE_PROG",
        poll_interval_seconds=0.001,
        timeout_seconds=1,
    )
    kwargs.update(overrides)
    return mod.SapJobTriggerAssetComponent(**kwargs)


def _materialize(component, resource=None):
    asset_def = component.build_defs(context=None).assets[0]
    resources = {"sap_ecc": resource} if resource is not None else {}
    result = dg.materialize([asset_def], resources=resources)
    return result, asset_def


# --- background_job: create + start a new one-step job -----------------------------------------

def test_background_job_create_and_start_success(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_OPEN": {"JOBCOUNT": "12345678", "RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_ADD_ABAP_STEP": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_CLOSE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": [{"STATUS": "R"}, {"STATUS": "F"}],
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, variant_name="PROD_VARIANT")
    result, asset_def = _materialize(component, FakeRFCResource(conn))

    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["sap/run_id"].value == "12345678"
    assert md["sap/status"].value == "F"
    assert md["dry_run"].value is False

    names = [c[0] for c in conn.calls]
    assert names == [
        "BAPI_XMI_LOGON", "BAPI_XBP_JOB_OPEN", "BAPI_XBP_JOB_ADD_ABAP_STEP", "BAPI_XBP_JOB_CLOSE",
        "BAPI_XBP_JOB_START_IMMEDIATELY", "BAPI_XBP_JOB_STATUS_GET", "BAPI_XBP_JOB_STATUS_GET", "BAPI_XMI_CLOSE",
    ]
    logon_kwargs = conn.calls[0][1]
    assert logon_kwargs["EXTCOMPANY"] and logon_kwargs["EXTPRODUCT"]
    add_step_kwargs = conn.calls[2][1]
    assert add_step_kwargs["ABAP_PROGRAM_NAME"] == "ZNIGHTLY_CLOSE_PROG"
    assert add_step_kwargs["ABAP_VARIANT_NAME"] == "PROD_VARIANT"
    assert add_step_kwargs["JOBCOUNT"] == "12345678"
    assert add_step_kwargs["EXTERNAL_USER_NAME"] == "DAGSTER"
    close_kwargs = conn.calls[3][1]
    assert "START_IMMEDIATELY" not in close_kwargs  # BAPI_XBP_JOB_CLOSE has no such parameter


def test_background_job_start_existing_job_count_skips_open_add_step_close(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "F"},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, program_name=None, job_count="99999999")
    result, asset_def = _materialize(component, FakeRFCResource(conn))

    assert result.success
    names = [c[0] for c in conn.calls]
    assert names == ["BAPI_XMI_LOGON", "BAPI_XBP_JOB_START_IMMEDIATELY", "BAPI_XBP_JOB_STATUS_GET", "BAPI_XMI_CLOSE"]
    assert conn.calls[1][1] == {
        "JOBNAME": "ZNIGHTLY_CLOSE", "JOBCOUNT": "99999999", "EXTERNAL_USER_NAME": "DAGSTER",
    }


def test_background_job_failure_raises_with_joblog(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_OPEN": {"JOBCOUNT": "1", "RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_ADD_ABAP_STEP": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_CLOSE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "A"},
        "BAPI_XBP_JOB_JOBLOG_READ": {"JOB_PROTOCOL": [{"MESSAGE": "RABAX dump in ZNIGHTLY_CLOSE_PROG"}]},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod)
    with pytest.raises(Exception, match="RABAX dump in ZNIGHTLY_CLOSE_PROG"):
        _materialize(component, FakeRFCResource(conn))
    # BAPI_XMI_CLOSE still runs even after a raised failure (finally-block logoff).
    assert conn.calls[-1][0] == "BAPI_XMI_CLOSE"


def test_background_job_xmi_logon_error_raises_before_any_job_call(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "E", "MESSAGE": "no XBP license"}},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod)
    with pytest.raises(Exception, match="no XBP license"):
        _materialize(component, FakeRFCResource(conn))
    assert [c[0] for c in conn.calls if c[0] == "BAPI_XBP_JOB_OPEN"] == []


def test_background_job_timeout_raises(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "R"},  # never finishes
        "BAPI_XBP_JOB_JOBLOG_READ": {"JOB_PROTOCOL": []},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(
        mod, program_name=None, job_count="1", poll_interval_seconds=0.001, timeout_seconds=1,
    )
    with pytest.raises(Exception, match="timed out"):
        _materialize(component, FakeRFCResource(conn))


def test_poll_error_is_retried_not_treated_as_failure(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    class _FlakyThenFinished(FakeRFCConnection):
        def call(self, name, **kwargs):
            if name == "BAPI_XBP_JOB_STATUS_GET" and len(self.calls) < 3:
                self.calls.append((name, kwargs))
                raise ConnectionError("transient RFC comm error")
            return super().call(name, **kwargs)

    conn = _FlakyThenFinished({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "F"},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, program_name=None, job_count="1", timeout_seconds=1.0)
    result, _ = _materialize(component, FakeRFCResource(conn))
    assert result.success


def test_external_user_name_and_target_server_are_configurable(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATELY": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "F"},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(
        mod, program_name=None, job_count="1",
        external_user_name="SVC_DAGSTER", target_server="APPSRV01",
    )
    _materialize(component, FakeRFCResource(conn))
    start_kwargs = conn.calls[1][1]
    assert start_kwargs["EXTERNAL_USER_NAME"] == "SVC_DAGSTER"
    assert start_kwargs["TARGET_SERVER"] == "APPSRV01"


def test_success_and_failure_statuses_default_to_the_documented_one_character_codes(mod):
    component = _make_component(mod, program_name=None, job_count="1")
    assert component.success_statuses == ["F"]
    assert component.failure_statuses == ["A"]


# --- process_chain -----------------------------------------------------------------------------

def test_process_chain_success(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "RSPC_API_CHAIN_START": {"LOG_ID": "LOGID001"},
        "RSPC_API_CHAIN_STATUS": [{"STATUS": "RUNNING"}, {"STATUS": "GREEN"}],
    })
    component = _make_component(
        mod, target_type="process_chain", job_name=None, program_name=None, process_chain_id="ZDAILY_LOAD_CHAIN",
    )
    result, asset_def = _materialize(component, FakeRFCResource(conn))

    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["sap/run_id"].value == "LOGID001"
    assert md["sap/status"].value == "GREEN"
    # No XMI logon/close for process_chain mode.
    assert [c[0] for c in conn.calls if c[0].startswith("BAPI_XMI")] == []


def test_process_chain_failure_raises_with_log(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "RSPC_API_CHAIN_START": {"LOG_ID": "LOGID002"},
        "RSPC_API_CHAIN_STATUS": {"STATUS": "RED"},
        "RSPC_API_CHAIN_VIEW_LOG": {"MESSAGES": [{"MESSAGE": "process DTP_LOAD failed"}]},
    })
    component = _make_component(
        mod, target_type="process_chain", job_name=None, program_name=None, process_chain_id="ZDAILY_LOAD_CHAIN",
    )
    with pytest.raises(Exception, match="process DTP_LOAD failed"):
        _materialize(component, FakeRFCResource(conn))


def test_process_chain_statuses_keep_the_unverified_guessed_defaults(mod):
    component = _make_component(
        mod, target_type="process_chain", job_name=None, program_name=None, process_chain_id="X",
    )
    assert component.success_statuses == ["FINISHED", "COMPLETED", "GREEN"]
    assert component.failure_statuses == ["ABORTED", "CANCELLED", "RED", "ERROR", "FAILED"]


# --- config validation ---------------------------------------------------------------------------

def test_background_job_requires_job_name(mod):
    with pytest.raises(Exception, match="requires job_name"):
        mod.SapJobTriggerAssetComponent(asset_key="t", sap_rfc_resource_key="sap_ecc", target_type="background_job")


def test_background_job_requires_job_count_or_program_name(mod):
    with pytest.raises(Exception, match="requires job_count"):
        mod.SapJobTriggerAssetComponent(
            asset_key="t", sap_rfc_resource_key="sap_ecc", target_type="background_job", job_name="ZJOB",
        )


def test_process_chain_requires_process_chain_id(mod):
    with pytest.raises(Exception, match="requires process_chain_id"):
        mod.SapJobTriggerAssetComponent(
            asset_key="t", sap_rfc_resource_key="sap_ecc", target_type="process_chain",
        )


# --- dry_run / run_window -------------------------------------------------------------------------

def test_dry_run_never_opens_a_connection(mod):
    component = _make_component(mod, dry_run=True)
    result, asset_def = _materialize(component, resource=None)  # no resources at all
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is True


def test_run_window_outside_the_window_skips_cleanly(mod):
    component = _make_component(mod, dry_run=True, run_window="00:00-00:01 UTC")
    result, asset_def = _materialize(component, resource=None)
    assert result.success  # skip, not fail
    assert result.asset_materializations_for_node(asset_def.op.name) == []


def test_run_window_covering_all_day_always_runs(mod):
    component = _make_component(mod, dry_run=True, run_window="00:00-23:59 UTC")
    result, asset_def = _materialize(component, resource=None)
    assert result.success
    assert len(result.asset_materializations_for_node(asset_def.op.name)) == 1
