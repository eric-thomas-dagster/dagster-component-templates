"""SqlContextReceiptStore on a plain PEP-249 connection (no SQLAlchemy).

Real: the `:name` -> paramstyle converter on all five paramstyles, and the whole store on real sqlite3 and real duckdb connections
(the shared suites in test_sql_backend / test_selftest / test_privacy / ... also run on them via tests/backends.py).
Fake: Snowflake, Databricks, Postgres and MySQL. There is no such server here, so a connection shaped like snowflake.connector
(pyformat, UPPERCASE column names, autocommit) wraps sqlite to prove the conversion and name handling end to end, and the vendor
modules are stand-ins that record how they were called. Nothing here proves behaviour against the real services."""
import re
import sqlite3
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import load_module  # noqa: E402
from .test_bigquery_backend import item, scenario  # noqa: E402


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_dbapi_under_test")


# ------------------------------------------------------------------ the converter

SQL = "SELECT a FROM t WHERE x = :x AND y = :y AND x2 = :x"


def test_qmark_numeric_format_named_pyformat(mod):
    p = {"x": 1, "y": "b"}
    assert mod._convert_sql_params(SQL, p, "qmark") == ("SELECT a FROM t WHERE x = ? AND y = ? AND x2 = ?", [1, "b", 1])
    assert mod._convert_sql_params(SQL, p, "numeric") == ("SELECT a FROM t WHERE x = :1 AND y = :2 AND x2 = :3", [1, "b", 1])
    assert mod._convert_sql_params(SQL, p, "format") == ("SELECT a FROM t WHERE x = %s AND y = %s AND x2 = %s", [1, "b", 1])
    assert mod._convert_sql_params(SQL, p, "named") == (SQL, {"x": 1, "y": "b"})
    assert mod._convert_sql_params(SQL, p, "pyformat") == (
        "SELECT a FROM t WHERE x = %(x)s AND y = %(y)s AND x2 = %(x)s", {"x": 1, "y": "b"})


@pytest.mark.parametrize("style,expect_sql,expect_params", [
    ("qmark", "id IN (?, ?, ?) AND k = ?", ["a", "b", "c", 9]),
    ("numeric", "id IN (:1, :2, :3) AND k = :4", ["a", "b", "c", 9]),
    ("format", "id IN (%s, %s, %s) AND k = %s", ["a", "b", "c", 9]),
    ("named", "id IN (:ids_0, :ids_1, :ids_2) AND k = :k", {"ids_0": "a", "ids_1": "b", "ids_2": "c", "k": 9}),
    ("pyformat", "id IN (%(ids_0)s, %(ids_1)s, %(ids_2)s) AND k = %(k)s", {"ids_0": "a", "ids_1": "b", "ids_2": "c", "k": 9}),
])
def test_list_params_expand_in_every_style(mod, style, expect_sql, expect_params):
    assert mod._convert_sql_params("id IN :ids AND k = :k", {"ids": ["a", "b", "c"], "k": 9}, style) == (expect_sql, expect_params)


def test_empty_list_matches_nothing_and_tuple_works(mod):
    assert mod._convert_sql_params("id IN :ids", {"ids": []}, "qmark") == ("id IN (NULL)", [])
    assert mod._convert_sql_params("id IN :ids", {"ids": ("a",)}, "qmark") == ("id IN (?)", ["a"])


@pytest.mark.parametrize("style", ["qmark", "numeric", "named", "format", "pyformat"])
def test_colons_in_literals_identifiers_comments_and_casts_are_untouched(mod, style):
    sql = ("SELECT 'a:b :c ''quoted :d''' AS s, \"col:name\" AS q, `tick:name` AS b, x::int AS cast, y :: text AS cast2, "
           "doc:field AS path, '12:30:00' AS t -- trailing :comment\n"
           "/* block :c */ FROM t WHERE k = :k")
    out, params = mod._convert_sql_params(sql, {"k": 1}, style)
    for kept in ("'a:b :c ''quoted :d'''", '"col:name"', "`tick:name`", "x::int", "y :: text", "doc:field", "'12:30:00'",
                 "-- trailing :comment", "/* block :c */"):
        assert kept in out, (style, kept, out)
    assert len(params) == 1


def test_percent_signs_are_doubled_only_for_format_and_pyformat(mod):
    sql = "SELECT '100%' AS p FROM t WHERE n LIKE 'a%' AND k = :k -- 5% off"
    assert "'100%%'" in mod._convert_sql_params(sql, {"k": 1}, "pyformat")[0]
    assert "'a%%'" in mod._convert_sql_params(sql, {"k": 1}, "format")[0] and "-- 5%% off" in mod._convert_sql_params(sql, {"k": 1}, "format")[0]
    assert "'100%'" in mod._convert_sql_params(sql, {"k": 1}, "qmark")[0]
    assert "'100%'" in mod._convert_sql_params(sql, {"k": 1}, "named")[0]


def test_missing_param_and_bad_style_are_errors_and_extras_are_ignored(mod):
    with pytest.raises(ValueError, match=r":y has no value"):
        mod._convert_sql_params(SQL, {"x": 1}, "qmark")
    with pytest.raises(ValueError, match="unsupported paramstyle"):
        mod._convert_sql_params(SQL, {"x": 1, "y": 2}, "dollar")
    assert mod._convert_sql_params("a = :x", {"x": 1, "unused": 2}, "qmark") == ("a = ?", [1])
    assert mod._convert_sql_params("a = :x", {"x": 1, "unused": 2}, "pyformat")[1] == {"x": 1}


def test_expanded_name_collision_and_unterminated_quote(mod):
    with pytest.raises(ValueError, match="collide"):
        mod._convert_sql_params("a IN :ids AND b = :ids_0", {"ids": [1, 2], "ids_0": 5}, "pyformat")
    assert mod._convert_sql_params("a = :x AND b = 'open :x", {"x": 1}, "qmark") == ("a = ? AND b = 'open :x", [1])


def test_every_real_statement_the_store_sends_converts_in_every_style(mod):
    """Record each statement the store would send; every `:name` it contains must be convertible (no unknown token) in all five styles."""
    class Recorder(mod.SqlContextReceiptStore):
        def __init__(self):
            super().__init__(connection_factory=lambda: None, dialect="snowflake", schema="db.sch", table_prefix="p_", paramstyle="pyformat")
            self.seen = []

        def _query(self, sql, **params):
            self.seen.append((sql, params))
            return []

        def ensure_tables(self):
            pass

    r = Recorder()
    r.get_receipt("x")
    r.item_feedback("kb", since="2026-01-01", code_version="cv")
    r.asset_usage_since("kb", "2026-01-01")
    r.outcomes_by_code_version("kb")
    r.find_by_assets({"kb": ["p"]})
    r.find_by_item("a")
    r.derive_implicit_outcomes()
    r.get_item_scores(["a", "b"])
    assert len(r.seen) > 8
    for sql, params in r.seen:
        for style in mod._PARAMSTYLES:
            mod._convert_sql_params(sql, params, style)


# ------------------------------------------------------------------ the DBAPI store on sqlite3 / constructor rules

def test_constructor_rules(mod, tmp_path):
    with pytest.raises(ValueError, match="exactly one"):
        mod.SqlContextReceiptStore()
    with pytest.raises(ValueError, match="exactly one"):
        mod.SqlContextReceiptStore(f"sqlite:///{tmp_path / 'a.db'}", connection_factory=lambda: None, dialect="sqlite")
    with pytest.raises(ValueError, match="needs dialect"):
        mod.SqlContextReceiptStore(connection_factory=lambda: None)
    with pytest.raises(ValueError, match="unsupported SQL dialect"):
        mod.SqlContextReceiptStore(connection_factory=lambda: None, dialect="oracle")
    with pytest.raises(ValueError, match="unsupported paramstyle"):
        mod.SqlContextReceiptStore(connection_factory=lambda: None, dialect="sqlite", paramstyle="dollar")
    with pytest.raises(ValueError, match="only to connection_factory"):
        mod.SqlContextReceiptStore(f"sqlite:///{tmp_path / 'a.db'}", paramstyle="qmark")


def test_paramstyle_is_read_from_the_driver_module_or_demanded(mod, tmp_path):
    path = str(tmp_path / "a.db")
    store = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path), dialect="sqlite")
    store.record_retrieval(context_name="kb", receipt_id="r", items=[item("a")])
    assert store._dbapi._style == "qmark"                                  # sqlite3.paramstyle

    class Conn:        # a connection whose module has no paramstyle
        def cursor(self):
            raise AssertionError("unreachable")
    unknown = mod.SqlContextReceiptStore(connection_factory=Conn, dialect="postgres")
    with pytest.raises(ValueError, match="pass paramstyle="):
        unknown.get_receipt("x")


def test_transaction_rolls_back_and_connection_is_per_thread(mod, tmp_path):
    path = str(tmp_path / "t.db")
    store = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path), dialect="sqlite", table_prefix="t_")
    store.ensure_tables()
    with pytest.raises(RuntimeError):
        with store._exec.transaction() as tx:
            tx.execute(f"INSERT INTO {store._t('context_item_scores')} (item_id, retrievals, outcome_weight, outcome_sum, prior, updated_at) "
                       "VALUES ('z', 1, 1.0, 1.0, 0.1, '2026-01-01')")
            raise RuntimeError("boom")
    assert store._query(f"SELECT COUNT(1) AS n FROM {store._t('context_item_scores')}")[0]["n"] == 0
    import threading
    seen = []
    t = threading.Thread(target=lambda: seen.append(store._exec._conn()))
    t.start()
    t.join()
    assert seen[0] is not store._exec._conn()


# ------------------------------------------------------------------ a fake snowflake.connector-shaped connection over sqlite

class _FakeSnowflake:
    """Module-shaped stand-in for snowflake.connector: pyformat, UPPERCASE column names, autocommit=True. Under the hood sqlite3
    (isolation_level=None = autocommit, BEGIN/COMMIT/ROLLBACK as statements). Refuses any non-pyformat placeholder."""
    paramstyle = "pyformat"

    def __init__(self, path):
        self.path, self.executed, self.connect_kwargs = path, [], []
        outer = self

        class SnowflakeConnection:
            __module__ = "snowflake.connector.connection"

            def __init__(self):
                self._c = sqlite3.connect(outer.path, isolation_level=None)

            def cursor(self):
                return _FakeCursor(self._c, outer)

            def close(self):
                self._c.close()

            def commit(self):
                raise AssertionError("an autocommit connection is never commit()ed by the store")

            rollback = commit

        self.SnowflakeConnection = SnowflakeConnection

    def connect(self, **kwargs):
        self.connect_kwargs.append(kwargs)
        return self.SnowflakeConnection()

    def install(self, monkeypatch):
        pkg = types.ModuleType("snowflake")
        pkg.__path__ = []
        pkg.connector = self
        self.__name__ = "snowflake.connector"
        self.__path__ = []
        sub = types.ModuleType("snowflake.connector.connection")
        monkeypatch.setitem(sys.modules, "snowflake", pkg)
        monkeypatch.setitem(sys.modules, "snowflake.connector", self)
        monkeypatch.setitem(sys.modules, "snowflake.connector.connection", sub)
        return self


class _FakeCursor:
    def __init__(self, conn, owner):
        self._cur, self._owner = conn.cursor(), owner

    @staticmethod
    def _translate(sql, params):
        assert not re.search(r"(?<![:\w]):[A-Za-z_]\w*", re.sub(r"'[^']*'", "''", sql)) or "::" in sql, f"left a :name placeholder: {sql}"
        assert "?" not in re.sub(r"'[^']*'", "''", sql), f"qmark placeholder in a pyformat connection: {sql}"
        assert params is None or isinstance(params, dict), f"pyformat wants a dict, got {type(params)}"
        sqlite_sql = re.sub(r"%\((\w+)\)s", r":\1", sql) if params is not None else sql
        return sqlite_sql.replace("%%", "%") if params is not None else sqlite_sql

    def execute(self, sql, params=None):
        self._owner.executed.append(sql)
        self._cur.execute(self._translate(sql, params), params or ())
        return self

    def executemany(self, sql, rows):
        self._owner.executed.append(sql)
        self._cur.executemany(self._translate(sql, rows[0]), rows)

    @property
    def description(self):
        d = self._cur.description
        return None if d is None else [(c[0].upper(),) + tuple(c[1:]) for c in d]

    def fetchall(self):
        return self._cur.fetchall()

    def close(self):
        self._cur.close()


@pytest.fixture()
def snow(monkeypatch, tmp_path):
    return _FakeSnowflake(str(tmp_path / "snow.db")).install(monkeypatch)


def test_snowflake_shaped_connection_end_to_end(mod, snow):
    store = mod.SqlContextReceiptStore(connection_factory=snow.connect, dialect="snowflake", schema="", table_prefix="s_")
    lite_path = Path(snow.path).with_name("lite.db")
    lite = mod.ContextReceiptStore(str(lite_path))
    scenario(store)
    scenario(lite)
    assert store._dbapi._style == "pyformat"                              # read from the snowflake.connector module
    since = "2000-01-01T00:00:00+00:00"
    got, want = store.item_feedback("kb", since=since), lite.item_feedback("kb", since=since)
    for d in (got, want):
        for e in d.values():
            e.pop("last_retrieved_at")                                       # formatted per backend
    assert got == want and got
    assert store.refresh_item_scores(3.0) == lite.refresh_item_scores(3.0)
    assert store.find_by_item("a") == lite.find_by_item("a")
    assert all(k == k.lower() for k in store.get_receipt("r1"))            # UPPERCASE names came back lowercased
    assert any("%(" in q for q in snow.executed)                          # the SQL really was pyformat
    # transactions: multi-statement writes are BEGIN ... COMMIT (autocommit connection); a lone read is not wrapped
    assert "BEGIN" in snow.executed and "COMMIT" in snow.executed
    before = len(snow.executed)
    store.get_receipt("r1")
    assert "BEGIN" not in snow.executed[before:]


def test_snowflake_shaped_failure_issues_rollback(mod, snow):
    store = mod.SqlContextReceiptStore(connection_factory=snow.connect, dialect="snowflake", table_prefix="s_")
    store.ensure_tables()
    store._query(f"DROP TABLE {store._t('context_receipts')}")
    with pytest.raises(Exception):
        store.record_retrieval(context_name="kb", receipt_id="tx", items=[item("a")])
    assert "ROLLBACK" in snow.executed
    assert store._query(f"SELECT COUNT(1) AS n FROM {store._t('context_receipt_items')} WHERE receipt_id = 'tx'")[0]["n"] == 0


def test_selftest_passes_on_the_snowflake_shaped_connection(mod, snow):
    store = mod.SqlContextReceiptStore(connection_factory=snow.connect, dialect="snowflake", table_prefix="s_")
    results = store.selftest()
    assert all(r["ok"] for r in results), [r for r in results if not r["ok"]]


def test_databricks_mode_never_wraps_in_transactions(mod, tmp_path):
    seen, path = [], str(tmp_path / "db.sqlite3")

    class Conn:
        def __init__(self):
            self._c = sqlite3.connect(path, isolation_level=None)

        def cursor(self):
            outer, cur = seen, self._c.cursor()

            class Cur:
                description = property(lambda self: cur.description)

                def execute(self, sql, params=()):
                    outer.append(sql)
                    return cur.execute(sql, params)

                def __getattr__(self, name):
                    return getattr(cur, name)
            return Cur()

        def commit(self):
            raise AssertionError("no transactions on Databricks")

        rollback = commit

        def close(self):
            self._c.close()

    store = mod.SqlContextReceiptStore(connection_factory=Conn, dialect="databricks", schema="", table_prefix="d_", paramstyle="qmark")
    # sqlite stands in for the warehouse; only the transaction behaviour is under test, so skip table creation
    store._ready = True
    assert store._exec._mode == "none"
    with store._exec.transaction() as tx:
        tx.execute("CREATE TABLE x (a INT)")
    assert not [q for q in seen if q.strip().upper() in ("BEGIN", "COMMIT", "ROLLBACK")]
    with pytest.raises(KeyError):
        with store._exec.transaction() as tx:
            raise KeyError("fails: nothing to roll back, and rollback() must not be called")


# ------------------------------------------------------------------ component wiring

def _comp(mod, **kw):
    return mod.ContextReceiptStoreComponent(backend="sql", async_writes=False, **kw)


def _store(comp):
    return comp.build_defs(None).resources["context_receipts"].resource_fn(None)


def test_component_validation(mod):
    with pytest.raises(ValueError, match="exactly one of sql_url_env_var"):
        _comp(mod).build_defs(None)
    with pytest.raises(ValueError, match="exactly one of sql_url_env_var"):
        _comp(mod, sql_url_env_var="U", sql_connection={"kind": "postgres", "host": "h", "database": "d", "user": "u"}).build_defs(None)
    with pytest.raises(ValueError, match="kind must be one of"):
        _comp(mod, sql_connection={"kind": "oracle"}).build_defs(None)
    with pytest.raises(ValueError, match="unknown keys"):
        _comp(mod, sql_connection={"kind": "postgres", "host": "h", "database": "d", "user": "u", "passwd": "x"}).build_defs(None)
    with pytest.raises(ValueError, match="requires"):
        _comp(mod, sql_connection={"kind": "databricks", "server_hostname": "h"}).build_defs(None)
    with pytest.raises(ValueError, match="exactly one of password_env_var or private_key_env_var"):
        _comp(mod, sql_connection={"kind": "snowflake", "account": "a", "user": "u"}).build_defs(None)
    with pytest.raises(ValueError, match="exactly one of password_env_var or private_key_env_var"):
        _comp(mod, sql_connection={"kind": "snowflake", "account": "a", "user": "u", "password_env_var": "P", "private_key_env_var": "K"}).build_defs(None)


def test_component_builds_a_dbapi_store_lazily_and_snowflake_kwargs_are_mapped(mod, snow, monkeypatch):
    monkeypatch.setenv("SNOW_PW", "s3cret")
    comp = _comp(mod, sql_table_prefix="c_", selftest_asset=False,
                 sql_connection={"kind": "snowflake", "account": "acme-xy1", "user": "RCPT", "warehouse": "WH", "database": "DB",
                                 "schema": "CTX", "role": "R", "password_env_var": "SNOW_PW", "connect_args": {"client_session_keep_alive": True}})
    store = _store(comp)
    assert isinstance(store, mod.SqlContextReceiptStore) and store.dialect == "snowflake" and snow.connect_kwargs == []   # not connected at load time
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.record_outcome(rid, signal="thumbs_up")
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    assert snow.connect_kwargs[0] == {"account": "acme-xy1", "user": "RCPT", "warehouse": "WH", "database": "DB", "schema": "CTX", "role": "R",
                                      "password": "s3cret", "client_session_keep_alive": True}
    assert all(r["ok"] for r in store.selftest())


def test_snowflake_key_pair_auth_passes_der_bytes(mod, snow, monkeypatch):
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    from cryptography.hazmat.primitives import serialization
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.BestAvailableEncryption(b"pp")).decode()
    monkeypatch.setenv("SNOW_KEY", pem)
    monkeypatch.setenv("SNOW_PP", "pp")
    store = _store(_comp(mod, sql_connection={"kind": "snowflake", "account": "a", "user": "u", "private_key_env_var": "SNOW_KEY",
                                              "private_key_passphrase_env_var": "SNOW_PP"}))
    store.get_receipt("x")
    kwargs = snow.connect_kwargs[0]
    assert "password" not in kwargs and isinstance(kwargs["private_key"], bytes)
    assert serialization.load_der_private_key(kwargs["private_key"], password=None).key_size == 2048


def test_unset_secret_env_var_is_a_clear_error(mod, snow, monkeypatch):
    monkeypatch.delenv("NOPE_PW", raising=False)
    store = _store(_comp(mod, sql_connection={"kind": "snowflake", "account": "a", "user": "u", "password_env_var": "NOPE_PW"}))
    with pytest.raises(ValueError, match="NOPE_PW"):
        store.get_receipt("x")


@pytest.mark.parametrize("kind,spec,module,pip", [
    ("snowflake", {"account": "a", "user": "u", "password_env_var": "P"}, "snowflake.connector", "snowflake-connector-python"),
    ("databricks", {"server_hostname": "h", "http_path": "/p", "access_token_env_var": "P"}, "databricks.sql", "databricks-sql-connector"),
    ("postgres", {"host": "h", "database": "d", "user": "u", "password_env_var": "P"}, "psycopg2", "psycopg2-binary"),
    ("mysql", {"host": "h", "database": "d", "user": "u", "password_env_var": "P"}, "pymysql", "pymysql"),
])
def test_missing_vendor_connector_names_the_exact_pip_package(mod, monkeypatch, kind, spec, module, pip):
    monkeypatch.setenv("P", "x")
    monkeypatch.setitem(sys.modules, module, None)          # makes `import module` raise ImportError
    store = _store(_comp(mod, sql_connection={"kind": kind, **spec}))      # loading never imports the connector
    with pytest.raises(ImportError, match=f"pip install {re.escape(pip)}"):
        store.get_receipt("x")


def test_vendor_kwargs_for_databricks_postgres_and_mysql(mod, monkeypatch):
    calls = {}

    def fake(name, key):
        m = types.ModuleType(name)
        m.paramstyle = "pyformat"
        m.connect = lambda **kw: calls.setdefault(key, kw) or (_ for _ in ()).throw(RuntimeError("stop"))
        return m
    for k, v in {"DBX": "tok", "PG": "pgpw", "MY": "mypw"}.items():
        monkeypatch.setenv(k, v)
    databricks = types.ModuleType("databricks")
    databricks.__path__ = []
    sql_mod = fake("databricks.sql", "dbx")
    databricks.sql = sql_mod
    monkeypatch.setitem(sys.modules, "databricks", databricks)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "psycopg2", fake("psycopg2", "pg"))
    monkeypatch.setitem(sys.modules, "pymysql", fake("pymysql", "my"))
    for spec in ({"kind": "databricks", "server_hostname": "h.cloud", "http_path": "/sql/1", "access_token_env_var": "DBX", "catalog": "c", "schema": "s"},
                 {"kind": "postgres", "host": "pg", "port": "5433", "database": "d", "user": "u", "password_env_var": "PG"},
                 {"kind": "mysql", "host": "my", "database": "d", "user": "u", "password_env_var": "MY"}):
        with pytest.raises(Exception):
            _store(_comp(mod, sql_connection=spec)).get_receipt("x")
    assert calls["dbx"] == {"server_hostname": "h.cloud", "http_path": "/sql/1", "access_token": "tok", "catalog": "c", "schema": "s"}
    assert calls["pg"] == {"host": "pg", "user": "u", "port": 5433, "password": "pgpw", "dbname": "d"}
    assert calls["my"] == {"host": "my", "user": "u", "password": "mypw", "database": "d"}


def test_sqlalchemy_url_form_still_works_alongside(mod, monkeypatch, tmp_path):
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("RECEIPTS_URL", f"sqlite:///{tmp_path / 'c.sqlite3'}")
    store = _store(_comp(mod, sql_url_env_var="RECEIPTS_URL"))
    assert store._dbapi is None and store._engine_obj is None
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    assert store.get_receipt(rid)["receipt_id"] == rid


def test_nan_and_inf_become_null_not_a_bare_literal():
    """Snowflake's client would render a float NaN as bare `nan` (read as a column name); store NULL instead."""
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location("crs_nan", Path(__file__).resolve().parent.parent / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sql, params = mod._convert_sql_params("INSERT INTO t (a, b, c, d) VALUES (:a, :b, :c, :d)",
                                          {"a": float("nan"), "b": float("inf"), "c": float("-inf"), "d": 0.5}, "pyformat")
    assert params == {"a": None, "b": None, "c": None, "d": 0.5}


def test_bare_postgresql_url_falls_back_to_psycopg2_when_psycopg3_is_missing(monkeypatch):
    import importlib.util
    from pathlib import Path
    import sqlalchemy
    spec = importlib.util.spec_from_file_location("crs_pg", Path(__file__).resolve().parent.parent / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen = []

    def fake_create(url, **kw):
        seen.append(url)
        if url.startswith("postgresql://"):
            raise ModuleNotFoundError("No module named 'psycopg'", name="psycopg")
        return "engine"

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create)
    assert mod._create_sql_engine("postgresql://u:p@h/db") == "engine"
    assert seen == ["postgresql://u:p@h/db", "postgresql+psycopg2://u:p@h/db"]
