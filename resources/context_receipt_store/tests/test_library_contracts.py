"""Contract tests against the REAL client libraries (BigQuery, Snowflake, Databricks, psycopg2, pymysql, SQLAlchemy).

Our fakes (bq_fake.py, the sqlite-behind-a-snowflake-shaped-connection in test_dbapi_executor.py) accept whatever we hand them, so they
cannot catch "we call the library differently from how the library works". These tests import the real packages and check what we send
against them: real method signatures (``inspect.signature(...).bind``), real parameter / config classes serialized with their real
``to_api_repr()``, the vendors' own client-side SQL rendering, and the real SQL compilers. Each section skips cleanly when its library
is not importable; no network and no credentials are used. They do NOT prove behaviour on the real services (no server is contacted)."""
import datetime as dt
import inspect
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import load_module  # noqa: E402
from .test_bigquery_backend import item, scenario  # noqa: E402

AWARE = dt.datetime(2026, 10, 19, 12, 30, 5, 123456, tzinfo=dt.timezone.utc)


try:                                    # imported at COLLECTION time: before any test installs a fake google.cloud.bigquery
    from . import real_bq
except BaseException:  # noqa: BLE001  (pytest.skip at module level raises a BaseException subclass)
    real_bq = None


@pytest.fixture(scope="module")
def mod():
    return load_module("context_receipt_store_library_contracts")


# =====================================================================================================================
# google-cloud-bigquery
# =====================================================================================================================
class TestBigQuery:
    @pytest.fixture()
    def real_bq(self, monkeypatch):
        if real_bq is None:
            pytest.skip("google-cloud-bigquery is not installed")
        real_bq.install_real_bigquery(monkeypatch)
        return real_bq

    @pytest.fixture()
    def client(self, real_bq):
        c = real_bq.ContractClient()
        c.create_dataset("proj.ds")
        return c

    @pytest.fixture()
    def store(self, mod, client):
        return mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")

    def test_the_client_is_a_context_manager_that_returns_itself_and_closes(self, store, client):
        # the real bigquery.Client has __enter__/__exit__ (exit closes the transport; it stays usable): the store enters it per operation
        store.ensure_tables()
        assert client.closed >= 1
        assert store.get_receipt("nope") is None

    def test_scenario_runs_through_real_signatures_and_real_api_objects(self, store, client):
        # every query()/load_table_from_json() call is bound against the real signature and its config serialized with to_api_repr()
        ids = scenario(store)
        store.refresh_item_scores(3.0)
        assert store.get_receipt(ids[0])["items"]
        assert store.get_item_scores(["a", "b"])
        assert client.loads() and client.sql()

    def test_selftest_through_real_signatures(self, store):
        results = store.selftest()
        assert not [r for r in results if not r["ok"]], results

    def test_parameter_types_on_the_wire(self, store, client):
        store.record_retrieval(context_name="c", receipt_id="rid-1", items=[item("a")], caller_id="u")
        store.delete_receipts(["rid-1", "rid-2"])
        seen = [p for call in client.calls if call[0] == "query" for p in call[2]]
        kinds = {(p["parameterType"]["type"], p["parameterType"].get("arrayType", {}).get("type")) for p in seen}
        assert ("STRING", None) in kinds                     # @r = receipt id
        assert ("ARRAY", "STRING") in kinds                  # IN UNNEST(@ids)
        assert all("name" in p for p in seen)

    def test_load_configs_on_the_wire(self, store, client):
        store.record_retrieval(context_name="c", items=[item("a")])
        loads = client.loads()
        assert loads
        for _, table, rows, cfg in loads:
            assert table.count(".") == 2 and "`" not in table      # project.dataset.table
            assert cfg["load"]["writeDisposition"] == "WRITE_APPEND"
            assert "schema" not in cfg["load"]                      # existing tables keep their own (REQUIRED) schema

    def test_staging_load_sends_an_explicit_all_nullable_schema(self, mod, store, client):
        scenario(store)
        store.refresh_item_scores(3.0)
        staged = [c for c in client.loads() if c[3] and "schema" in c[3]["load"]]
        assert staged, "refresh_item_scores should stage its rows with an explicit schema"
        fields = staged[0][3]["load"]["schema"]["fields"]
        assert [f["name"] for f in fields] == ["item_id", "retrievals", "outcome_weight", "outcome_sum", "prior", "updated_at"]
        assert {f["type"] for f in fields} <= {"STRING", "INTEGER", "FLOAT", "TIMESTAMP"}
        assert all(f.get("mode", "NULLABLE") == "NULLABLE" for f in fields)
        # our logical -> BigQuery type table only produces types the API accepts
        from google.cloud import bigquery

        for bq_type in mod._BQ_TYPES.values():
            assert bigquery.SchemaField("c", bq_type).to_api_repr()["type"] in {"STRING", "INTEGER", "FLOAT", "TIMESTAMP"}

    def test_params_helper_matches_real_parameter_classes(self, mod, real_bq):
        from google.cloud import bigquery

        out = mod.BigQueryContextReceiptStore._params(bigquery, {"s": "x", "i": 3, "f": 0.5, "b": True, "l": ["a", 1], "t": AWARE})
        by = {p.name: p.to_api_repr() for p in out}
        assert by["s"]["parameterType"]["type"] == "STRING" and by["s"]["parameterValue"]["value"] == "x"
        assert by["i"]["parameterType"]["type"] == "INT64"
        assert by["f"]["parameterType"]["type"] == "FLOAT64"
        assert by["b"]["parameterType"]["type"] == "BOOL" and by["b"]["parameterValue"]["value"] == "true"   # bool is not sent as INT64
        assert by["l"]["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "STRING"}}
        assert by["l"]["parameterValue"]["arrayValues"] == [{"value": "a"}, {"value": "1"}]
        assert by["t"]["parameterType"]["type"] == "STRING"                                            # datetimes travel as strings
        with pytest.raises(ValueError, match="cannot be NULL"):
            mod.BigQueryContextReceiptStore._params(bigquery, {"n": None})

    def test_contract_client_rejects_what_the_real_signature_rejects(self, client):
        # guards the harness itself: a wrong keyword / missing argument is a TypeError, a bare context manager has no .query
        with pytest.raises(TypeError):
            client.query("SELECT 1", job_configuration=None)
        with pytest.raises(TypeError):
            client.load_table_from_json([{"a": 1}])
        import contextlib

        @contextlib.contextmanager
        def cm():
            yield client

        assert not hasattr(cm(), "query")

    def test_dagster_gcp_resource_get_client_is_a_context_manager(self, mod, real_bq, monkeypatch):
        # the original bug class, against the REAL dagster-gcp resource: its get_client() is a @contextmanager around bigquery.Client(...)
        pytest.importorskip("dagster_gcp")
        import dagster_gcp
        from google.cloud import bigquery

        backend = real_bq.FakeBigQueryClient("proj")
        backend.create_dataset("proj.ds")
        made = []
        monkeypatch.setattr(bigquery, "Client", lambda **kw: made.append(kw) or real_bq.ContractClient(backend))
        resource = dagster_gcp.BigQueryResource(project="proj", location="EU")
        cm = resource.get_client()
        assert not hasattr(cm, "query") and hasattr(cm, "__enter__")
        store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=resource.get_client)
        rid = store.record_retrieval(context_name="c", items=[item("a")])
        assert store.get_receipt(rid)["items"][0]["item_id"] == "a"
        assert made and made[0] == {"project": "proj", "location": "EU"}


# =====================================================================================================================
# the statements the store really issues, per dialect (captured while it runs on sqlite, whose permissive SQL accepts every dialect's
# quoting/type names for these statements), then pushed through each vendor library's own client-side machinery
# =====================================================================================================================
import sqlite3  # noqa: E402

DIALECTS = ["snowflake", "postgres", "mysql", "databricks", "sqlite"]


def _capture(mod, dialect, tmp_path, monkeypatch):
    """-> [(sql, params)]: every ``_convert_sql_params`` call (``:name`` SQL + the value dict) made while the store runs ``scenario`` and
    ``selftest`` as ``dialect``. A step that sqlite cannot run for that dialect (e.g. Databricks MERGE) stops the capture, not the test."""
    seen: list = []
    real = mod._convert_sql_params

    def spy(sql, params, style):
        seen.append((sql, dict(params or {})))
        return real(sql, params, style)

    monkeypatch.setattr(mod, "_convert_sql_params", spy)
    path = str(tmp_path / f"cap-{dialect}.sqlite3")
    store = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path), dialect=dialect, paramstyle="qmark", table_prefix="p_")
    for step in (lambda: scenario(store), lambda: store.refresh_item_scores(3.0), store.selftest):
        try:
            step()
        except Exception:  # noqa: BLE001
            pass
    return seen


@pytest.fixture(scope="module")
def captured(mod, tmp_path_factory):
    out = {}
    for d in DIALECTS:
        mp = pytest.MonkeyPatch()          # one per dialect: spies must not chain into each other's lists
        try:
            out[d] = _capture(mod, d, tmp_path_factory.mktemp(d), mp)
        finally:
            mp.undo()
    return out


def test_capture_is_substantial(captured):
    for d in ("snowflake", "postgres", "mysql", "sqlite"):
        assert len(captured[d]) > 40, (d, len(captured[d]))
    assert any(" LIMIT " in sql.upper() for sql, _ in captured["snowflake"])
    assert any("IN :" in sql for sql, _ in captured["snowflake"])
    assert any(sql.lstrip().upper().startswith("INSERT INTO") and "context_receipts" in sql for sql, _ in captured["snowflake"])


# =====================================================================================================================
# snowflake-connector-python: the client-side pyformat substitution
# =====================================================================================================================
def _snowflake_renderer():
    """The connector's own ``query % params`` rendering (4.x: SnowflakeCursor._preprocess_pyformat_query; 5.x: ClientSideBindingConverter)."""
    pytest.importorskip("snowflake.connector")
    try:
        from snowflake.connector.converter import SnowflakeConverter
        from snowflake.connector.connection import SnowflakeConnection
        from snowflake.connector.cursor import SnowflakeCursor

        conn = object.__new__(SnowflakeConnection)
        conn._interpolate_empty_sequences = False
        conn.converter = SnowflakeConverter(use_numpy=False, support_negative_year=True)
        cur = object.__new__(SnowflakeCursor)
        cur._connection = conn
        return lambda sql, params: cur._preprocess_pyformat_query(sql, params)
    except ImportError:
        pass
    try:
        from snowflake.connector._internal.binding_converters import ClientSideBindingConverter

        return lambda sql, params: ClientSideBindingConverter.interpolate_query(sql, params)
    except ImportError:
        pytest.skip("this snowflake-connector-python version has no client-side binding entry point this test knows")


class TestSnowflake:
    @pytest.fixture()
    def render(self, mod):
        raw = _snowflake_renderer()

        def render(sql, params):
            if params:
                sql, bound = mod._convert_sql_params(sql, params, "pyformat")
            else:
                bound = None
            return raw(sql, bound)

        return render

    def test_the_driver_module_declares_pyformat(self, mod):
        import snowflake.connector as sf

        assert sf.paramstyle == "pyformat" and sf.paramstyle in mod._PARAMSTYLES
        from snowflake.connector.connection import SnowflakeConnection

        assert mod._driver_paramstyle(object.__new__(SnowflakeConnection)) == "pyformat"

    @pytest.mark.parametrize("value, literal", [
        (AWARE, "'2026-10-19 12:30:05.123456+00:00'"),
        (dt.datetime(2026, 10, 19, 12, 30, 5, 123456), "'2026-10-19 12:30:05.123456'"),
        (None, "NULL"),
        (7, "7"),
        (0.1, "0.1"),
        (True, "TRUE"),
        (float("nan"), "NULL"),                      # our converter's NaN -> NULL (a bare nan would be read as a column)
        (float("inf"), "NULL"),
        (float("-inf"), "NULL"),
        ("plain", "'plain'"),
        ("it's", "'it\\'s'"),
        ("back\\slash", "'back\\\\slash'"),
        ("100% %s %(x)s", "'100% %s %(x)s'"),         # percent signs in a VALUE are never re-interpreted
    ])
    def test_scalar_literals(self, render, value, literal):
        assert render("SELECT :v AS v", {"v": value}) == f"SELECT {literal} AS v"

    def test_record_retrieval_insert(self, mod, render):
        cols = mod._BQ_RECEIPT_COLS
        sql = f"INSERT INTO t ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"
        vals = ("rid", AWARE, "ctx", None, None, "caller", None, "h", 3, 0, "a/b", None, None, "sess", None, "2026-01-01")
        out = render(sql, dict(zip(cols, vals)))
        assert out.endswith("VALUES ('rid', '2026-10-19 12:30:05.123456+00:00', 'ctx', NULL, NULL, 'caller', NULL, 'h', 3, 0, 'a/b', "
                            "NULL, NULL, 'sess', NULL, '2026-01-01')")

    def test_in_list_since_limit_and_empty_list(self, render):
        assert render("SELECT * FROM t WHERE receipt_id IN :ids AND x LIKE '50%' ", {"ids": ["a", "b'c", "d%e"]}) == (
            "SELECT * FROM t WHERE receipt_id IN ('a', 'b\\'c', 'd%e') AND x LIKE '50%' ")
        assert render("SELECT * FROM t WHERE created_at >= :since", {"since": AWARE}) == "SELECT * FROM t WHERE created_at >= '2026-10-19 12:30:05.123456+00:00'"
        assert render("SELECT * FROM t WHERE created_at > :c ORDER BY created_at LIMIT 1000", {"c": AWARE}).endswith("ORDER BY created_at LIMIT 1000")
        assert render("SELECT * FROM t WHERE ids IN :ids", {"ids": []}) == "SELECT * FROM t WHERE ids IN (NULL)"
        assert render("SELECT 1 WHERE a LIKE '50%'", None) == "SELECT 1 WHERE a LIKE '50%'"      # no params: untouched, % is not doubled
        assert render("SELECT :n AS n LIMIT :k", {"n": float("nan"), "k": 5}) == "SELECT NULL AS n LIMIT 5"

    def test_cast_and_path_syntax_survive(self, render):
        assert render("SELECT payload:field::string AS f, :v::int AS v FROM t", {"v": 3}) == "SELECT payload:field::string AS f, 3::int AS v FROM t"

    def test_every_statement_the_store_sends_renders(self, mod, captured, render):
        # no leftover pyformat marker, no leftover :name, and every scalar parameter value appears as the connector's own literal for it
        raw = _snowflake_renderer()
        checked = 0
        for sql, params in captured["snowflake"]:
            out = render(sql, params)
            assert "%(" not in out.replace("%(x)s", ""), out
            for name, value in params.items():
                if isinstance(value, (str, int)) and not isinstance(value, bool) and f":{name}" in sql and not isinstance(value, list):
                    literal = raw("SELECT %(v)s", {"v": value})[len("SELECT "):]
                    assert literal in out, (name, value, out)
                    checked += 1
        assert checked > 50

    def test_executemany_bulk_insert_is_recognised_by_the_connector(self, mod):
        # snowflake-connector-python (4.x) rewrites a recognised INSERT ... VALUES (...) executemany into ONE multi-row statement
        try:
            from snowflake.connector.cursor import SnowflakeCursor
        except ImportError:
            pytest.skip("no SnowflakeCursor")
        regex = getattr(SnowflakeCursor, "INSERT_SQL_RE", None)
        if regex is None:
            pytest.skip("this connector version has no executemany rewrite regex")
        sql = ("INSERT INTO DB.SCH.p_items (receipt_id, item_id, source_asset_key, rank, score, selection) "
               "VALUES (:receipt_id, :item_id, :source_asset_key, :rank, :score, :selection)")
        converted, _ = mod._convert_sql_params(sql, dict(receipt_id="r", item_id="i", source_asset_key=None, rank=1, score=0.5, selection="s"), "pyformat")
        assert regex.match(converted)

    def test_connect_kwargs_are_real_snowflake_configuration_names(self, mod, monkeypatch):
        import snowflake.connector as sf
        from snowflake.connector.connection import DEFAULT_CONFIGURATION

        seen = []
        monkeypatch.setattr(sf, "connect", lambda **kw: seen.append(kw) or object())
        monkeypatch.setenv("SF_PW", "pw")
        factory, dialect = mod._build_dbapi_connection({
            "kind": "snowflake", "account": "a-b", "user": "u", "password_env_var": "SF_PW", "warehouse": "WH", "database": "DB", "schema": "S",
            "role": "R", "authenticator": "snowflake"})
        factory()
        assert dialect == "snowflake" and seen
        if not DEFAULT_CONFIGURATION:
            pytest.skip("this connector version no longer publishes DEFAULT_CONFIGURATION")
        assert set(seen[0]) <= set(DEFAULT_CONFIGURATION), sorted(set(seen[0]) - set(DEFAULT_CONFIGURATION))
        assert inspect.signature(sf.connect).parameters      # connect() exists with a signature we can introspect


# =====================================================================================================================
# databricks-sql-connector: native named parameters
# =====================================================================================================================
class TestDatabricks:
    @pytest.fixture()
    def native(self, mod):
        pytest.importorskip("databricks.sql")
        from databricks.sql.client import Cursor
        from databricks.sql.utils import transform_paramstyle

        cur = object.__new__(Cursor)

        def native(sql, params):
            converted, bound = mod._convert_sql_params(sql, params, "named")
            norm = cur._normalize_tparametercollection(bound)
            structure = cur._determine_parameter_structure(norm)
            stmt, tparams = cur._prepare_native_parameters(transform_paramstyle(converted, norm, structure), norm, structure)
            return stmt, structure, tparams

        return native

    def test_driver_declares_named_paramstyle(self, mod):
        pytest.importorskip("databricks.sql")
        import databricks.sql as dbsql
        from databricks.sql.client import Connection

        assert dbsql.paramstyle == "named" and "named" in mod._PARAMSTYLES
        assert mod._driver_paramstyle(object.__new__(Connection)) == "named"

    def test_python_values_map_to_native_types(self, native):
        _, structure, tp = native("SELECT :t, :n, :i, :big, :f, :s, :b", {
            "t": AWARE, "n": None, "i": 7, "big": 2 ** 40, "f": 0.5, "s": "x", "b": True})
        assert structure.name == "NAMED"
        by = {p.name: p for p in tp}
        assert {k: v.type for k, v in by.items()} == {"t": "TIMESTAMP", "n": "VOID", "i": "INT", "big": "BIGINT", "f": "DOUBLE", "s": "STRING", "b": "BOOLEAN"}
        assert by["n"].value is None
        assert by["t"].value.stringValue == "2026-10-19 12:30:05.123456+00:00"
        assert by["b"].value.stringValue == "True" and by["i"].value.stringValue == "7"
        assert all(p.ordinal is False for p in tp)

    def test_nan_and_inf_become_void_not_double(self, native):
        _, _, tp = native("SELECT :a, :b", {"a": float("nan"), "b": float("inf")})
        assert [p.type for p in tp] == ["VOID", "VOID"]

    def test_statement_keeps_named_markers_and_expands_lists(self, native):
        stmt, _, tp = native("SELECT * FROM t WHERE id IN :ids AND c >= :since LIMIT :n", {"ids": ["a", "b"], "since": AWARE, "n": 5})
        assert stmt == "SELECT * FROM t WHERE id IN (:ids_0, :ids_1) AND c >= :since LIMIT :n"
        assert [p.name for p in tp] == ["ids_0", "ids_1", "since", "n"]
        assert tp[-1].type == "INT"                              # LIMIT :n must be INT, not BIGINT/DOUBLE

    def test_percent_literals_are_left_alone(self, native):
        stmt, _, _ = native("SELECT '50%' AS a, :v AS c", {"v": 1})
        assert stmt == "SELECT '50%' AS a, :v AS c"

    def test_every_statement_the_store_sends_binds(self, captured, native):
        from collections import Counter

        types_seen = Counter()
        for sql, params in captured["databricks"] + captured["snowflake"]:
            stmt, structure, tp = native(sql, params)
            assert "%(" not in stmt
            for p in tp:
                assert p.type in {"STRING", "INT", "BIGINT", "DOUBLE", "BOOLEAN", "TIMESTAMP", "VOID", "DATE"}, p
                types_seen[p.type] += 1
        assert {"STRING", "VOID", "INT"} <= set(types_seen)

    def test_connect_kwargs_bind_to_the_real_connection_signature(self, mod, monkeypatch):
        pytest.importorskip("databricks.sql")
        import databricks.sql as dbsql
        from databricks.sql.client import Connection

        seen = []

        def fake_connect(*a, **kw):
            inspect.signature(dbsql.connect).bind(*a, **kw)
            inspect.signature(Connection.__init__).bind(None, *a, **kw)      # what connect() forwards to
            seen.append(kw)
            return object()

        monkeypatch.setattr(dbsql, "connect", fake_connect)
        monkeypatch.setenv("DBX_TOKEN", "tok")
        factory, dialect = mod._build_dbapi_connection({
            "kind": "databricks", "server_hostname": "h.cloud.databricks.com", "http_path": "/sql/1.0/warehouses/x", "access_token_env_var": "DBX_TOKEN",
            "catalog": "main", "schema": "ctx"})
        factory()
        assert dialect == "databricks"
        assert seen == [{"server_hostname": "h.cloud.databricks.com", "http_path": "/sql/1.0/warehouses/x", "access_token": "tok", "catalog": "main", "schema": "ctx"}]


# =====================================================================================================================
# psycopg2 / pymysql
# =====================================================================================================================
class TestPostgresAndMysql:
    def test_psycopg2_connect_kwargs_are_valid_libpq_options(self, mod, monkeypatch):
        pytest.importorskip("psycopg2")
        import psycopg2
        from psycopg2.extensions import make_dsn

        seen = []
        monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: seen.append(kw) or object())
        monkeypatch.setenv("PG_PW", "pw")
        factory, dialect = mod._build_dbapi_connection({"kind": "postgres", "host": "h", "database": "d", "user": "u", "port": "5433", "password_env_var": "PG_PW",
                                                        "connect_args": {"sslmode": "require", "connect_timeout": 5}})
        factory()
        assert dialect == "postgres" and seen[0]["port"] == 5433
        dsn = make_dsn(**seen[0])                       # raises ProgrammingError on an option libpq does not know
        assert "dbname=d" in dsn and "host=h" in dsn and "port=5433" in dsn
        inspect.signature(psycopg2.connect).bind(**seen[0])

    def test_psycopg2_declares_pyformat_and_adapts_our_values(self, mod, captured):
        pytest.importorskip("psycopg2")
        import psycopg2
        from psycopg2.extensions import adapt

        assert psycopg2.paramstyle == "pyformat"
        rendered = 0
        for sql, params in captured["postgres"]:
            converted, bound = mod._convert_sql_params(sql, params, "pyformat")
            quoted = {k: adapt(v).getquoted().decode() for k, v in bound.items()}     # psycopg2 renders client-side with %-formatting
            out = converted % quoted
            assert "%(" not in out
            rendered += 1
        assert rendered > 40
        assert adapt("it's").getquoted() == b"'it''s'" and adapt(None).getquoted() == b"NULL" and adapt(True).getquoted() == b"true"

    def test_pymysql_connect_kwargs_bind_to_the_real_signature(self, mod, monkeypatch):
        pytest.importorskip("pymysql")
        import pymysql
        from pymysql.connections import Connection

        seen = []

        def fake_connect(*a, **kw):
            inspect.signature(Connection.__init__).bind(None, *a, **kw)
            seen.append(kw)
            return object()

        monkeypatch.setattr(pymysql, "connect", fake_connect)
        monkeypatch.setenv("MY_PW", "pw")
        factory, dialect = mod._build_dbapi_connection({"kind": "mysql", "host": "h", "database": "d", "user": "u", "port": 3307, "password_env_var": "MY_PW",
                                                        "connect_args": {"charset": "utf8mb4"}})
        factory()
        assert dialect == "mysql" and seen == [{"host": "h", "user": "u", "port": 3307, "password": "pw", "database": "d", "charset": "utf8mb4"}]

    @pytest.fixture()
    def cursor(self):
        pytest.importorskip("pymysql")
        import pymysql

        conn = pymysql.connections.Connection(defer_connect=True)      # no socket: escape/mogrify are pure client-side
        conn.server_status = 0
        return conn.cursor()

    def test_pymysql_rendering(self, mod, cursor):
        import pymysql

        assert pymysql.paramstyle == "pyformat"

        def r(sql, params):
            s, b = mod._convert_sql_params(sql, params, "pyformat")
            return cursor.mogrify(s, b)

        out = r("SELECT * FROM `d`.t WHERE a IN :ids AND b LIKE '50%' AND c > :c AND d = :n LIMIT 5",
                {"ids": ["a", "b'c", "%s"], "c": dt.datetime(2026, 1, 1, 1, 2, 3, 4), "n": None})
        assert out == "SELECT * FROM `d`.t WHERE a IN ('a', 'b\\'c', '%s') AND b LIKE '50%' AND c > '2026-01-01 01:02:03.000004' AND d = NULL LIMIT 5"
        assert r("INSERT INTO t (`rank`, score, ok) VALUES (:rank, :score, :ok)", {"rank": 1, "score": 0.5, "ok": True}) == (
            "INSERT INTO t (`rank`, score, ok) VALUES (1, 0.5e0, 1)")
        assert r("SELECT :v", {"v": float("nan")}) == "SELECT NULL"

    def test_pymysql_every_statement_renders_and_bulk_insert_is_recognised(self, mod, cursor, captured):
        import pymysql.cursors as pc

        for sql, params in captured["mysql"]:
            s, b = mod._convert_sql_params(sql, params, "pyformat")
            out = cursor.mogrify(s, b)
            assert "%(" not in out
        inserts = [mod._convert_sql_params(s, p, "pyformat")[0] for s, p in captured["mysql"] if s.lstrip().upper().startswith("INSERT INTO") and "VALUES" in s.upper()]
        assert inserts and all(pc.RE_INSERT_VALUES.match(s) for s in inserts), "executemany would fall back to one round trip per row"


# =====================================================================================================================
# SQLAlchemy 2.x: our ``:name`` statements compiled for dialects (no driver needed)
# =====================================================================================================================
class TestSqlAlchemyCompile:
    @pytest.fixture(scope="class")
    def sa(self):
        sa = pytest.importorskip("sqlalchemy")
        assert int(sa.__version__.split(".")[0]) >= 2
        return sa

    @staticmethod
    def dialect(name):
        from sqlalchemy.dialects import mysql, postgresql, sqlite

        return {"postgres": postgresql.dialect(), "mysql": mysql.dialect(), "sqlite": sqlite.dialect()}[name]

    def compile(self, sa, sql, params, name):
        lists = {k: v for k, v in params.items() if isinstance(v, (list, tuple))}
        scalars = {k: v for k, v in params.items() if k not in lists}
        stmt = sa.text(sql).bindparams(*[sa.bindparam(k, value=list(v), expanding=True) for k, v in lists.items()], **scalars)
        return stmt.compile(dialect=self.dialect(name), compile_kwargs={"render_postcompile": True})

    @pytest.mark.parametrize("name, marker", [("postgres", "%(ids_1)s"), ("mysql", "%s"), ("sqlite", "?")])
    def test_expanding_in_per_dialect_paramstyle(self, sa, name, marker):
        c = self.compile(sa, "SELECT * FROM t WHERE a IN :ids AND c > :c LIMIT 5", {"ids": ["a", "b"], "c": 1}, name)
        assert marker in str(c) and str(c).rstrip().endswith("LIMIT 5")
        assert len(c.params) == 3

    @pytest.mark.parametrize("name", ["postgres", "mysql"])
    def test_percent_in_sql_text_is_doubled_for_pyformat_dialects(self, sa, name):
        c = self.compile(sa, "SELECT * FROM t WHERE x LIKE '50%' AND y = :y", {"y": "100%"}, name)
        assert "LIKE '50%%'" in str(c) and c.params["y"] == "100%"       # the VALUE is a bound parameter: never doubled

    def test_sqlite_leaves_percent_alone(self, sa):
        assert "LIKE '50%'" in str(self.compile(sa, "SELECT * FROM t WHERE x LIKE '50%' AND y = :y", {"y": 1}, "sqlite"))

    @pytest.mark.parametrize("name", ["postgres", "mysql", "sqlite"])
    def test_casts_and_digit_colons_are_not_binds(self, sa, name):
        c = self.compile(sa, "SELECT a::text AS x, '2026-01-01T00:00:00+00:00' AS y, :v AS z FROM t", {"v": 1}, name)
        assert list(c.params.values()) == [1] and "::text" in str(c) and "'2026-01-01T00:00:00+00:00'" in str(c)

    @pytest.mark.parametrize("name", ["postgres", "mysql", "sqlite"])
    def test_sqlalchemy_would_bind_exactly_the_names_our_converter_binds(self, mod, sa, captured, name):
        # SQLAlchemy's text() has no notion of quotes (':x' inside a literal IS a bind for it); our own paramstyle converter does. The store's
        # SA path hands the SQL to text() unchanged, so the two must agree on every statement the store sends.
        for sql, params in captured[name]:
            names = set(sa.text(sql).compile(dialect=self.dialect(name)).params)
            _, bound = mod._convert_sql_params(sql, params, "pyformat")
            expanded = {k for k in bound}
            used = {n for n in names}
            assert used <= set(params), (sql, used - set(params))
            assert {k.rsplit("_", 1)[0] if k not in params else k for k in expanded} == used, sql

    @pytest.mark.parametrize("name", ["postgres", "mysql", "sqlite"])
    def test_every_statement_the_store_sends_compiles_with_quoting_intact(self, mod, sa, captured, name):
        n = 0
        for sql, params in captured[name]:
            c = self.compile(sa, sql, params, name)
            text = str(c)
            assert ":" not in text.replace("::", "").replace("':notabind'", "") or "'" in text, text      # no unrendered :name outside literals
            if name == "mysql" and " rank" in sql.lower() and "ORDER BY" in sql:
                assert "`rank`" in text, text                     # RANK is reserved in MySQL 8: must stay quoted through compilation
            if name == "postgres":
                assert '"p_' in text or "context_" not in text, text   # quoted table identifiers survive
            n += 1
        assert n > 40
