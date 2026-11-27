"""Real, committed tests for RedshiftDataApiAssetComponent.

Mocks only the external boundary (a fake boto3 redshift-data client's
`.execute_statement()`/`.describe_statement()`, matching the real response
shape verified against the botocore reference) -- everything else (Jinja
templating, connection-mode validation, polling loop, metadata extraction,
failure handling, Dagster asset wiring) is real.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class FakeRedshiftDataClient:
    def __init__(self, statuses, result_rows=42, result_size=1024, duration_ns=5_000_000, redshift_query_id=777, error=None):
        self.execute_calls = []
        self.poll_calls = 0
        self._statuses = list(statuses)
        self._result_rows = result_rows
        self._result_size = result_size
        self._duration_ns = duration_ns
        self._redshift_query_id = redshift_query_id
        self._error = error

    def execute_statement(self, **kwargs):
        self.execute_calls.append(kwargs)
        return {"Id": "stmt-123"}

    def describe_statement(self, Id):
        status = self._statuses[min(self.poll_calls, len(self._statuses) - 1)]
        self.poll_calls += 1
        return {
            "Id": Id,
            "Status": status,
            "Error": self._error,
            "ResultRows": self._result_rows,
            "ResultSize": self._result_size,
            "Duration": self._duration_ns,
            "RedshiftQueryId": self._redshift_query_id,
            "HasResultSet": True,
        }


def _patch_client(monkeypatch, client, secret_value=None):
    monkeypatch.setattr(mod, "_make_redshift_data_client", lambda region_name, aws_profile: client)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    if secret_value is not None:
        monkeypatch.setattr(mod.dg, "EnvVar", lambda name: type("_E", (), {"get_value": staticmethod(lambda: secret_value)})())


def _comp(**kw):
    args = dict(asset_name="out", sql="SELECT 1", database="mydb", region_name="us-east-1", workgroup_name="my-wg")
    args.update(kw)
    return mod.RedshiftDataApiAssetComponent(**args)


def test_finished_statement_materializes_with_metadata(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["STARTED", "FINISHED"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mat = result.asset_materializations_for_node("out")[0]
    assert mat.metadata["statement_id"].text == "stmt-123"
    assert mat.metadata["status"].text == "FINISHED"
    assert mat.metadata["result_rows"].value == 42
    assert mat.metadata["result_size_bytes"].value == 1024
    assert mat.metadata["duration_ms"].value == 5
    assert mat.metadata["redshift_query_id"].value == 777

    assert client.execute_calls[0]["Database"] == "mydb"
    assert client.execute_calls[0]["Sql"] == "SELECT 1"
    assert client.execute_calls[0]["WorkgroupName"] == "my-wg"
    assert "ClusterIdentifier" not in client.execute_calls[0]
    assert client.poll_calls == 2


def test_failed_statement_raises_with_real_error(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["FAILED"], error="syntax error at or near 'SELEC'")
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="syntax error"):
        dg.materialize(list(defs.assets))


def test_aborted_statement_raises(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["ABORTED"], error="cancelled")
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="ABORTED"):
        dg.materialize(list(defs.assets))


def test_cluster_identifier_with_db_user_passed_through(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["FINISHED"])
    _patch_client(monkeypatch, client)
    comp = _comp(workgroup_name=None, cluster_identifier="my-cluster", db_user="admin")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.execute_calls[0]["ClusterIdentifier"] == "my-cluster"
    assert client.execute_calls[0]["DbUser"] == "admin"
    assert "WorkgroupName" not in client.execute_calls[0]


def test_secret_arn_env_var_resolved_and_passed(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["FINISHED"])
    _patch_client(monkeypatch, client, secret_value="arn:aws:secretsmanager:us-east-1:123:secret:my-secret")
    comp = _comp(cluster_identifier="my-cluster", workgroup_name=None, secret_arn_env_var="REDSHIFT_SECRET_ARN")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.execute_calls[0]["SecretArn"] == "arn:aws:secretsmanager:us-east-1:123:secret:my-secret"


def test_both_cluster_identifier_and_workgroup_name_rejected():
    comp = _comp(cluster_identifier="my-cluster", workgroup_name="my-wg", db_user="admin")
    with pytest.raises(ValueError, match="exactly one of"):
        comp.build_defs(None)


def test_neither_cluster_identifier_nor_workgroup_name_rejected():
    comp = _comp(workgroup_name=None)
    with pytest.raises(ValueError, match="exactly one of"):
        comp.build_defs(None)


def test_cluster_identifier_without_auth_rejected():
    comp = _comp(cluster_identifier="my-cluster", workgroup_name=None)
    with pytest.raises(ValueError, match="db_user"):
        comp.build_defs(None)


def test_sql_is_jinja_rendered_with_partition_date(monkeypatch):
    client = FakeRedshiftDataClient(statuses=["FINISHED"])
    _patch_client(monkeypatch, client)
    comp = _comp(
        sql="CALL refresh('{{ partition_date }}');",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), partition_key="2024-01-02")
    assert result.success
    assert client.execute_calls[0]["Sql"] == "CALL refresh('2024-01-02');"
