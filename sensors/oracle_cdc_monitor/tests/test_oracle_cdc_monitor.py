"""OracleCDCMonitorSensorComponent: mocks oracledb (the only real external surface --
there is no real Oracle server reachable in CI, same situation other forks faced for
untestable live-server libraries) so these tests exercise table-pattern resolution,
SCN-range resolution (start_now / from_beginning), insert/update/delete classification
via MINE_VALUE/COLUMN_PRESENT, the archived-log-gap failure path, and the
max_events_per_tick cap in isolation."""
import json
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


class _FakeCursor:
    """Dispatches on recognizable substrings of the SQL/PLSQL text -- the component
    issues several distinct queries through the same cursor object sequentially."""

    def __init__(self, scenario):
        self.scenario = scenario
        self._last_rows = None
        self._iter_rows = None

    def execute(self, sql, params=None):
        params = params or {}
        s = sql.upper()
        if "FROM ALL_TABLES" in s:
            self._last_rows = [(t,) for t in self.scenario["all_tables"]]
        elif "FROM ALL_TAB_COLUMNS" in s:
            tname = params["tname"]
            self._last_rows = [(c,) for c in self.scenario["table_columns"][tname]]
        elif "CURRENT_SCN FROM V$DATABASE" in s:
            self._last_rows = [(self.scenario["end_scn"],)]
        elif "MIN(FIRST_CHANGE#)" in s:
            logs = self.scenario["archived_logs"]
            self._last_rows = [(min(l[1] for l in logs) if logs else None,)]
        elif "FROM V$ARCHIVED_LOG" in s:
            start_scn, end_scn = params["start_scn"], params["end_scn"]
            self._last_rows = [
                (name, first, next_)
                for name, first, next_ in self.scenario["archived_logs"]
                if first < end_scn and next_ > start_scn
            ]
        elif "DBMS_LOGMNR.ADD_LOGFILE" in s:
            self.scenario.setdefault("added_logfiles", []).append(params["name"])
        elif "DBMS_LOGMNR.START_LOGMNR" in s:
            self.scenario["start_logmnr_called_with"] = (params["s"], params["e"])
        elif "DBMS_LOGMNR.END_LOGMNR" in s:
            self.scenario["end_logmnr_called"] = True
        elif "FROM V$LOGMNR_CONTENTS" in s:
            self._iter_rows = iter(self.scenario["logmnr_rows"])
        else:
            raise AssertionError(f"Unexpected SQL in fake cursor: {sql}")

    def fetchall(self):
        return self._last_rows or []

    def fetchone(self):
        rows = self._last_rows or []
        return rows[0] if rows else None

    def callfunc(self, name, return_type, params):
        raw_value, qualified = params
        table_val_map = self.scenario["mine_values"].get(raw_value, {})
        col = qualified.split(".")[-1]
        if name == "DBMS_LOGMNR.COLUMN_PRESENT":
            return 1 if col in table_val_map else 0
        if name == "DBMS_LOGMNR.MINE_VALUE":
            return table_val_map.get(col)
        raise AssertionError(f"Unexpected callfunc: {name}")

    def __iter__(self):
        return self._iter_rows


class _FakeConnection:
    def __init__(self, scenario):
        self.scenario = scenario

    def cursor(self):
        return _FakeCursor(self.scenario)

    def close(self):
        pass


def _install_fake_oracledb(monkeypatch, scenario):
    fake_mod = types.ModuleType("oracledb")
    fake_mod.makedsn = lambda host, port, service_name=None, sid=None: f"{host}:{port}/{service_name or sid}"
    fake_mod.connect = lambda user, password, dsn: _FakeConnection(scenario)
    monkeypatch.setitem(sys.modules, "oracledb", fake_mod)
    return fake_mod


def _run_sensor(mod, instance, attrs, cursor=None):
    comp = mod.OracleCDCMonitorSensorComponent(**attrs)
    defs = comp.build_defs(context=None)
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor, sensor_name=sensor_def.name)
    return sensor_def(ctx), ctx


def _base_attrs(**overrides):
    attrs = dict(
        sensor_name="orders_cdc",
        host="db.internal",
        port=1521,
        service_name="ORCLPDB1",
        username="cdc_user",
        password_env_var=None,
        schema_name="SALES",
        table_patterns=["ORDERS"],
        job_name="apply_cdc_job",
        on_first_run="start_now",
    )
    attrs.update(overrides)
    return attrs


def test_no_table_pattern_match_skips(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["CUSTOMERS"],
        "table_columns": {},
        "end_scn": 1000,
        "archived_logs": [],
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(table_patterns=["ORDERS"]))
    assert "No tables" in result.skip_reason.skip_message


def test_first_run_start_now_skips_with_no_prior_activity(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID", "STATUS", "AMOUNT"]},
        "end_scn": 5000,
        "archived_logs": [],
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs())
    # start_now -> start_scn == end_scn on first run -> no backlog to mine
    assert json.loads(result.cursor)["scn"] == 5000
    assert "No new Oracle CDC events" in result.skip_reason.skip_message


def test_from_beginning_uses_earliest_archived_log(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID", "STATUS", "AMOUNT"]},
        "end_scn": 5000,
        "archived_logs": [("/arch/log1.arc", 100, 2000), ("/arch/log2.arc", 2000, 6000)],
        "logmnr_rows": [],
        "mine_values": {},
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(on_first_run="from_beginning"))
    assert scenario["start_logmnr_called_with"] == (100, 5000)
    assert scenario["added_logfiles"] == ["/arch/log1.arc", "/arch/log2.arc"]


def test_insert_update_delete_classification(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID", "STATUS", "AMOUNT"]},
        "end_scn": 2000,
        "archived_logs": [("/arch/log1.arc", 500, 2500)],
        "logmnr_rows": [
            (1001, "SALES", "ORDERS", "INSERT", "REDO1", None),
            (1002, "SALES", "ORDERS", "UPDATE", "REDO2", "UNDO2"),
            (1003, "SALES", "ORDERS", "DELETE", None, "UNDO3"),
        ],
        "mine_values": {
            "REDO1": {"ID": "1", "STATUS": "NEW", "AMOUNT": "9.99"},
            "REDO2": {"STATUS": "SHIPPED"},  # only the changed column present
            "UNDO2": {"STATUS": "NEW"},
            "UNDO3": {"ID": "1", "STATUS": "SHIPPED", "AMOUNT": "9.99"},
        },
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(), cursor=json.dumps({"scn": 1000}))

    assert scenario["end_logmnr_called"] is True
    run_config = result.run_requests[0].run_config
    events = run_config["ops"]["config"]["config"]["events"]
    assert len(events) == 3
    assert events[0]["_cdc_op"] == "insert" and events[0]["after"] == {"ID": "1", "STATUS": "NEW", "AMOUNT": "9.99"} and events[0]["before"] is None
    assert events[1]["_cdc_op"] == "update" and events[1]["before"] == {"STATUS": "NEW"} and events[1]["after"] == {"STATUS": "SHIPPED"}
    assert events[2]["_cdc_op"] == "delete" and events[2]["before"] == {"ID": "1", "STATUS": "SHIPPED", "AMOUNT": "9.99"} and events[2]["after"] is None
    assert json.loads(result.cursor)["scn"] == 2000


def test_archived_log_gap_fails_loudly(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID"]},
        "end_scn": 5000,
        # Stored position (1000) predates every available archived log (earliest starts at 3000) -- a real gap.
        "archived_logs": [("/arch/log3.arc", 3000, 6000)],
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(), cursor=json.dumps({"scn": 1000}))
    assert "Gap detected" in result.skip_reason.skip_message
    assert "re-snapshot" in result.skip_reason.skip_message.lower()


def test_no_archived_log_covers_range_fails_loudly(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID"]},
        "end_scn": 5000,
        "archived_logs": [],  # nothing at all covers [1000, 5000)
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(), cursor=json.dumps({"scn": 1000}))
    assert "No archived logs cover" in result.skip_reason.skip_message


def test_max_events_per_tick_caps_and_advances_cursor_partially(monkeypatch, instance):
    mod = load_component_module()
    scenario = {
        "all_tables": ["ORDERS"],
        "table_columns": {"ORDERS": ["ID"]},
        "end_scn": 2000,
        "archived_logs": [("/arch/log1.arc", 500, 2500)],
        "logmnr_rows": [
            (1001, "SALES", "ORDERS", "INSERT", "R1", None),
            (1002, "SALES", "ORDERS", "INSERT", "R2", None),
            (1003, "SALES", "ORDERS", "INSERT", "R3", None),
        ],
        "mine_values": {"R1": {"ID": "1"}, "R2": {"ID": "2"}, "R3": {"ID": "3"}},
    }
    _install_fake_oracledb(monkeypatch, scenario)
    result, _ = _run_sensor(mod, instance, _base_attrs(max_events_per_tick=2), cursor=json.dumps({"scn": 1000}))

    run_config = result.run_requests[0].run_config
    events = run_config["ops"]["config"]["config"]["events"]
    assert len(events) == 2
    # Cursor only advances to the SCN of the last event actually processed (1002), not the full end_scn (2000).
    assert json.loads(result.cursor)["scn"] == 1002


def test_missing_oracledb_dependency_skips_gracefully(monkeypatch, instance):
    mod = load_component_module()
    monkeypatch.setitem(sys.modules, "oracledb", None)
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "oracledb":
            raise ImportError("no module named oracledb")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    result, _ = _run_sensor(mod, instance, _base_attrs())
    assert "oracledb is not installed" in result.skip_reason.skip_message


def test_requires_service_name_or_sid():
    mod = load_component_module()
    with pytest.raises(ValueError, match="service_name or sid"):
        comp = mod.OracleCDCMonitorSensorComponent(**_base_attrs(service_name=None, sid=None))
        comp.build_defs(context=None)


def test_requires_at_least_one_table_pattern():
    mod = load_component_module()
    with pytest.raises(ValueError, match="table_patterns"):
        comp = mod.OracleCDCMonitorSensorComponent(**_base_attrs(table_patterns=[]))
        comp.build_defs(context=None)
