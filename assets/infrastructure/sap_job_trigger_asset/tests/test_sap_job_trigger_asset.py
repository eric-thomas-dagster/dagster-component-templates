"""Committed tests for SapJobTriggerAssetComponent.

Mocks ONLY the sap_rfc_resource's `.connection()` context manager + `.call()`
(a fake RFC responder keyed by RFC/BAPI name, same pattern as
sap_rfc_ingestion's own tests) -- every other behavior (XMI logon/close
sequencing, BAPI RETURN-table error checking, the poll loop, run_window,
dry_run, metadata) is exercised for real through dg.materialize().
"""
import contextlib

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeRFCConnection:
    """Maps an RFC/BAPI name to a canned response. A list is consumed one value
    per call (the last value repeats once exhausted) -- used to simulate a
    status poll going RUNNING -> FINISHED/ABORTED across repeated calls."""

    def __init__(self, responses: dict):
        self.responses = {k: (list(v) if isinstance(v, list) else v) for k, v in responses.items()}
        self.calls: list = []

    def call(self, name, **kwargs):
        self.calls.append((name, kwargs))
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
        "BAPI_XBP_JOB_ADD_STEP": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_CLOSE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": [{"STATUS": "RUNNING"}, {"STATUS": "FINISHED"}],
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, variant_name="PROD_VARIANT")
    result, asset_def = _materialize(component, FakeRFCResource(conn))

    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["sap/run_id"].value == "12345678"
    assert md["sap/status"].value == "FINISHED"
    assert md["dry_run"].value is False

    names = [c[0] for c in conn.calls]
    assert names == [
        "BAPI_XMI_LOGON", "BAPI_XBP_JOB_OPEN", "BAPI_XBP_JOB_ADD_STEP", "BAPI_XBP_JOB_CLOSE",
        "BAPI_XBP_JOB_STATUS_GET", "BAPI_XBP_JOB_STATUS_GET", "BAPI_XMI_CLOSE",
    ]
    add_step_kwargs = conn.calls[2][1]
    assert add_step_kwargs["PROGRAM"] == "ZNIGHTLY_CLOSE_PROG"
    assert add_step_kwargs["VARIANT"] == "PROD_VARIANT"
    assert add_step_kwargs["JOBCOUNT"] == "12345678"


def test_background_job_start_existing_job_count_skips_open_add_step_close(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_START_IMMEDIATE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "FINISHED"},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, program_name=None, job_count="99999999")
    result, asset_def = _materialize(component, FakeRFCResource(conn))

    assert result.success
    names = [c[0] for c in conn.calls]
    assert names == ["BAPI_XMI_LOGON", "BAPI_XBP_JOB_START_IMMEDIATE", "BAPI_XBP_JOB_STATUS_GET", "BAPI_XMI_CLOSE"]
    assert conn.calls[1][1] == {"JOBNAME": "ZNIGHTLY_CLOSE", "JOBCOUNT": "99999999"}


def test_background_job_failure_raises_with_joblog(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    conn = FakeRFCConnection({
        "BAPI_XMI_LOGON": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_OPEN": {"JOBCOUNT": "1", "RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_ADD_STEP": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_CLOSE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "ABORTED"},
        "BAPI_XBP_JOB_READ_JOBLOG": {"JOBLOG": [{"MSGTXT": "RABAX dump in ZNIGHTLY_CLOSE_PROG"}]},
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
        "BAPI_XBP_JOB_START_IMMEDIATE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "RUNNING"},  # never finishes
        "BAPI_XBP_JOB_READ_JOBLOG": {"JOBLOG": []},
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
        "BAPI_XBP_JOB_START_IMMEDIATE": {"RETURN": {"TYPE": "S"}},
        "BAPI_XBP_JOB_STATUS_GET": {"STATUS": "FINISHED"},
        "BAPI_XMI_CLOSE": {"RETURN": {"TYPE": "S"}},
    })
    component = _make_component(mod, program_name=None, job_count="1", timeout_seconds=1.0)
    result, _ = _materialize(component, FakeRFCResource(conn))
    assert result.success


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
