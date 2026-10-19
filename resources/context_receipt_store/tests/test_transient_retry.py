"""Transient-failure handling of the warehouse-backed receipt stores, by failure injection. Nothing sleeps (a fake ``retry_sleep`` records the waits).

Real: the retry/classification code, the stores, sqlite3 / DuckDB-through-the-BigQuery-fake underneath.
Fake: the failures. The exception classes below are stand-ins NAMED like the real vendor ones (google.api_core.exceptions.ServiceUnavailable,
snowflake.connector.errors.OperationalError, pymysql's OperationalError(1213, ...), SQLAlchemy's DBAPIError ...), because the code classifies by
class name / attributes and never imports the vendor libraries. Nothing here proves behaviour against the real services."""
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import load_module  # noqa: E402
from .bq_fake import FakeBigQueryClient, install_fake_bigquery  # noqa: E402
from .test_bigquery_backend import item  # noqa: E402


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_transient_retry_under_test")


# ------------------------------------------------------------------ stand-in vendor exceptions (named like the real ones)

class GoogleAPICallError(Exception):
    code = None

    def __init__(self, message="boom", errors=()):
        super().__init__(message)
        self.errors = list(errors)


def _gae(name, code):
    return type(name, (GoogleAPICallError,), {"code": code})


ServiceUnavailable, InternalServerError, TooManyRequests = _gae("ServiceUnavailable", 503), _gae("InternalServerError", 500), _gae("TooManyRequests", 429)
DeadlineExceeded, GatewayTimeout, BadGateway = _gae("DeadlineExceeded", 504), _gae("GatewayTimeout", 504), _gae("BadGateway", 502)
Forbidden, BadRequest, NotFound = _gae("Forbidden", 403), _gae("BadRequest", 400), _gae("NotFound", 404)


class OperationalError(Exception):          # snowflake.connector.errors / pymysql / psycopg2 / sqlite3 all name theirs this
    def __init__(self, msg="", errno=None, pgcode=None, sqlstate=None):
        super().__init__(msg)
        self.errno, self.pgcode, self.sqlstate = errno, pgcode, sqlstate


class ProgrammingError(OperationalError):   # snowflake.connector.errors.ProgrammingError shape
    pass


class DBAPIError(Exception):                # sqlalchemy.exc.DBAPIError: wraps the driver exception in .orig
    def __init__(self, orig, connection_invalidated=False):
        super().__init__(str(orig))
        self.orig, self.connection_invalidated = orig, connection_invalidated


class UpperRng:
    """Deterministic 'jitter': always the ceiling, so the exponential schedule itself is visible."""
    def uniform(self, a, b):
        return b


# ------------------------------------------------------------------ failure plan shared by both fakes

class Plan:
    def __init__(self):
        self.rules, self.calls = [], []

    def add(self, op, match, exc, times=1, after=False):
        self.rules.append({"op": op, "match": match, "exc": exc, "times": times, "after": after})

    def hit(self, op, text=""):
        self.calls.append((op, text))
        for r in self.rules:
            if r["op"] == op and (r["match"] is None or r["match"] in text) and (r["times"] is None or r["times"] > 0):
                if r["times"] is not None:
                    r["times"] -= 1
                return r
        return None

    def count(self, op, match=None):
        return sum(1 for o, t in self.calls if o == op and (match is None or match in t))


# ------------------------------------------------------------------ BigQuery

class FlakyBQClient:
    def __init__(self, inner, plan):
        self._inner, self._plan = inner, plan

    def query(self, sql, job_config=None, **kw):
        rule = self._plan.hit("query", sql)
        if rule and not rule["after"]:
            raise rule["exc"]()
        job = self._inner.query(sql, job_config=job_config, **kw)
        if rule:
            raise rule["exc"]()
        return job

    def load_table_from_json(self, rows, destination, job_config=None, **kw):
        rule = self._plan.hit("load", str(destination))
        if rule and not rule["after"]:
            raise rule["exc"]()
        job = self._inner.load_table_from_json(rows, destination, job_config=job_config, **kw)
        if rule:                                   # the load committed; the acknowledgement is what got lost
            raise rule["exc"]()
        return job

    def __getattr__(self, name):
        return getattr(self._inner, name)


@pytest.fixture()
def bq(mod, monkeypatch):
    plan, sleeps = Plan(), []
    inner = FakeBigQueryClient("proj")
    inner.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(inner))
    client = FlakyBQClient(inner, plan)
    store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_",
                                            retry_sleep=sleeps.append, retry_rng=UpperRng())
    store.ensure_tables()
    plan.calls.clear()
    store.plan, store.sleeps = plan, sleeps
    return store


def bq_count(store, table, where="1 = 1"):
    return store._query(f"SELECT COUNT(1) AS n FROM {store._t(table)} WHERE {where}")[0]["n"]


@pytest.mark.parametrize("exc", [ServiceUnavailable, InternalServerError, TooManyRequests, DeadlineExceeded, GatewayTimeout, BadGateway,
                                 lambda: Forbidden("Exceeded rate limits: too many table update operations", [{"reason": "rateLimitExceeded"}]),
                                 lambda: Forbidden("quota", [{"reason": "quotaExceeded"}])])
def test_bigquery_transient_errors_are_retried_then_succeed(bq, exc):
    rid = bq.record_retrieval(context_name="kb", items=[item("a")])
    bq.plan.add("query", "SELECT * FROM", exc, times=2)
    assert bq.get_receipt(rid)["context_name"] == "kb"
    assert len(bq.sleeps) == 2
    assert bq.plan.count("query", "SELECT * FROM") == 3


@pytest.mark.parametrize("exc", [BadRequest, NotFound, lambda: Forbidden("denied", [{"reason": "accessDenied"}]), lambda: ValueError("x"),
                                 lambda: TypeError("x"), lambda: KeyError("x")])
def test_bigquery_permanent_errors_are_not_retried(bq, exc):
    bq.plan.add("query", None, exc, times=None)
    with pytest.raises(Exception) as info:
        bq.get_receipt("r")
    assert bq.sleeps == [] and bq.plan.count("query") == 1
    assert not getattr(info.value, "__notes__", None)       # a first-attempt permanent error is raised untouched


def test_attempt_cap_is_respected_and_the_original_exception_is_raised(bq):
    bq.plan.add("query", None, ServiceUnavailable, times=None)
    with pytest.raises(ServiceUnavailable) as info:
        bq.get_receipt("r")
    assert bq.plan.count("query") == 4 and len(bq.sleeps) == 3
    assert any("4 attempt" in n for n in info.value.__notes__)


def test_backoff_is_exponential_with_a_ceiling_and_full_jitter_bounds(mod):
    sleeps = []
    r = mod._Retrier(6, 3.0, sleep=sleeps.append, rng=UpperRng())

    def always():
        raise ServiceUnavailable()
    with pytest.raises(ServiceUnavailable):
        r.run(always)
    assert sleeps == [0.5, 1.0, 2.0, 3.0, 3.0]                        # min(cap, 0.5 * 2^(n-1)); the ceiling holds
    import random
    real = mod._Retrier(2, 20.0, sleep=lambda s: None, rng=random.Random(7))
    for attempt in range(1, 12):
        for _ in range(50):
            assert 0.0 <= real.delay(attempt) <= min(20.0, 0.5 * 2 ** (attempt - 1))
    assert len({round(real.delay(5), 6) for _ in range(20)}) > 1       # jittered, not constant


def test_retry_settings_are_validated(mod):
    for bad in (0, -1, 1.5, True, None):
        with pytest.raises(ValueError, match="retry_max_attempts"):
            mod._Retrier(bad, 1.0)
    for bad in (-1, float("nan"), "x"):
        with pytest.raises(ValueError, match="retry_max_backoff_seconds"):
            mod._Retrier(2, bad)
    with pytest.raises(ValueError, match="connection_max_age_seconds"):
        mod.SqlContextReceiptStore(connection_factory=lambda: None, dialect="sqlite", connection_max_age_seconds=0)
    assert mod._Retrier(1, 0).max_attempts == 1


def test_retry_logs_class_and_attempt_but_never_the_parameters(bq, caplog):
    rid = bq.record_retrieval(context_name="kb", receipt_id="SECRET-RECEIPT-ID-123", items=[item("a")])
    bq.plan.add("query", "SELECT * FROM", lambda: ServiceUnavailable("503 for SELECT ... SECRET-RECEIPT-ID-123"), times=1)
    with caplog.at_level(logging.WARNING, logger=mod_logger_name(bq)):
        bq.get_receipt(rid)
    lines = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(lines) == 1 and "ServiceUnavailable" in lines[0] and "attempt 1 of 4" in lines[0]
    assert "SECRET" not in lines[0] and "SELECT" not in lines[0]


def mod_logger_name(store):
    return sys.modules[type(store).__module__].logger.name if type(store).__module__ in sys.modules else ""


def test_nested_calls_do_not_multiply_attempts(bq):
    """record_retrieval is one retried unit: its inner queries/loads call through, so the budget is 4 attempts total, not 4 x 4."""
    bq.plan.add("load", "context_receipt_items", ServiceUnavailable, times=None)
    with pytest.raises(ServiceUnavailable):
        bq.record_retrieval(context_name="kb", items=[item("a")])
    assert bq.plan.count("load", "context_receipt_items") == 4


def test_a_retried_unit_calls_its_inner_queries_through_instead_of_retrying_each(bq):
    bq.plan.add("query", "COUNT(1)", ServiceUnavailable, times=None)       # the duplicate check inside record_retrieval
    with pytest.raises(ServiceUnavailable):
        bq.record_retrieval(context_name="kb", items=[item("a")])
    assert bq.plan.count("query", "COUNT(1)") == 4                          # 4 attempts of the unit, not 4 x 4


def test_record_retrieval_retried_after_a_committed_but_unacknowledged_write_does_not_duplicate_or_raise(bq):
    bq.plan.add("load", "t_context_receipts", ServiceUnavailable, times=1, after=True)      # the receipt row committed, the ack was lost
    rid = bq.record_retrieval(context_name="kb", receipt_id="r1", query_hash="h", items=[item("a"), item("b")])
    assert rid == "r1"
    assert bq_count(bq, "context_receipts") == 1 and bq_count(bq, "context_receipt_items") == 2


def test_record_retrieval_retried_after_an_unacknowledged_items_load_leaves_no_duplicate_items(bq):
    bq.plan.add("load", "context_receipt_items", ServiceUnavailable, times=1, after=True)
    bq.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a"), item("b")])
    assert bq_count(bq, "context_receipts") == 1 and bq_count(bq, "context_receipt_items") == 2


def test_a_retry_that_finds_someone_elses_receipt_with_that_id_still_raises(bq, mod):
    bq.record_retrieval(context_name="theirs", receipt_id="r1", query_hash="h", items=[item("a")])
    bq.plan.add("query", "COUNT(1)", ServiceUnavailable, times=1)                     # attempt 1 fails, attempt 2 finds r1
    with pytest.raises(mod.DuplicateReceiptError):
        bq.record_retrieval(context_name="ours", receipt_id="r1", query_hash="h", items=[item("a")])
    bq.plan.add("query", "COUNT(1)", ServiceUnavailable, times=1)
    with pytest.raises(mod.DuplicateReceiptError):                                    # same context, different query_hash: also not ours
        bq.record_retrieval(context_name="theirs", receipt_id="r1", query_hash="other", items=[item("a")])
    assert bq_count(bq, "context_receipts") == 1


def test_a_first_attempt_duplicate_is_still_an_error(bq, mod):
    bq.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a")])
    with pytest.raises(mod.DuplicateReceiptError):
        bq.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a")])
    assert bq.sleeps == []


def test_append_event_retried_after_an_unacknowledged_load_never_duplicates(bq):
    rid = bq.record_retrieval(context_name="kb", items=[item("a")])
    bq.plan.add("load", "context_receipt_events", ServiceUnavailable, times=2, after=True)    # committed twice over, acked never
    bq.record_outcome(rid, signal="thumbs_up")
    assert bq_count(bq, "context_receipt_events") == 1
    bq.plan.add("query", "context_receipt_events", ServiceUnavailable, times=1)               # failure in the existence pre-check
    bq.record_outcome(rid, signal="thumbs_down")
    assert bq_count(bq, "context_receipt_events") == 2


def test_unknown_receipt_event_is_a_permanent_keyerror_not_retried(bq):
    with pytest.raises(KeyError):
        bq.record_outcome("ghost", signal="thumbs_up")
    assert bq.sleeps == []


def test_refresh_item_scores_retried_in_the_merge_leaves_one_correct_row(bq):
    rid = bq.record_retrieval(context_name="kb", items=[item("a")])
    bq.record_outcome(rid, signal="thumbs_up")
    bq.plan.add("query", "MERGE", ServiceUnavailable, times=2)
    assert bq.refresh_item_scores() == 1
    assert bq_count(bq, "context_item_scores", "item_id = 'a'") == 1
    assert len(bq.sleeps) == 2


def test_enforce_retention_and_delete_are_retried_whole(bq):
    rid = bq.record_retrieval(context_name="kb", items=[item("a")])
    bq.plan.add("query", "DELETE", ServiceUnavailable, times=1, after=True)               # the DELETE ran; its ack was lost
    assert bq.delete_receipts([rid]) == 1
    assert bq_count(bq, "context_receipts") == 0


def test_a_transient_probe_failure_is_not_mistaken_for_a_missing_column(mod, monkeypatch):
    plan, sleeps = Plan(), []
    inner = FakeBigQueryClient("proj")
    inner.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(inner))
    plan.add("query", "dagster_partition_key FROM", ServiceUnavailable, times=None)
    store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: FlakyBQClient(inner, plan), retry_sleep=sleeps.append)
    with pytest.raises(ServiceUnavailable):
        store.ensure_tables()                    # NOT 'ALTER TABLE ... ADD COLUMN'
    assert not any("ALTER" in t for _, t in plan.calls)


def test_bigquery_selftest_passes_through_transient_failures(bq):
    bq.plan.add("query", None, ServiceUnavailable, times=1)
    bq.plan.add("load", None, TooManyRequests, times=1)
    steps = bq.selftest()
    assert steps and all(s.get("ok", True) for s in steps if isinstance(s, dict))


# ------------------------------------------------------------------ classification table

@pytest.mark.parametrize("exc,expected", [
    (ServiceUnavailable(), "transient"), (TooManyRequests(), "transient"), (BadGateway(), "transient"),
    (Forbidden("x", [{"reason": "rateLimitExceeded"}]), "transient"), (Forbidden("x", [{"reason": "accessDenied"}]), "permanent"),
    (BadRequest(), "permanent"), (NotFound(), "permanent"),
    # Snowflake
    (OperationalError("Failed to connect", errno=250001), "connection_lost"), (OperationalError("net", errno=250003), "connection_lost"),
    (ProgrammingError("Authentication token has expired", errno=390114), "connection_lost"),
    (OperationalError("SQL execution canceled", errno=604), "timeout"), (ProgrammingError("SQL compilation error", errno=2003 + 1000), "permanent"),
    # Databricks
    (type("RequestError", (Exception,), {})("Error during request to server"), "connection_lost"),
    (OperationalError("Temporarily unavailable"), "transient"), (OperationalError("HTTP 503"), "permanent"),
    (type("HTTPError", (Exception,), {"status_code": 503})("x"), "transient"),
    # Postgres / MySQL
    (OperationalError("could not serialize access", pgcode="40001"), "transient"), (OperationalError("deadlock detected", pgcode="40P01"), "transient"),
    (OperationalError("server closed the connection unexpectedly"), "connection_lost"),
    (OperationalError("SSL SYSCALL error: EOF detected"), "connection_lost"),
    (OperationalError("password authentication failed for user"), "permanent"),
    (OperationalError("Deadlock found", errno=1213), "transient"), (OperationalError("lock wait", errno=1205), "transient"),
    (OperationalError("gone away", errno=2006), "connection_lost"), (OperationalError("Access denied", errno=1045), "permanent"),
    (OperationalError("connection exception", sqlstate="08006"), "connection_lost"),
    (OperationalError("statement timeout", pgcode="57014"), "timeout"),
    (sqlite3.OperationalError("database is locked"), "transient"),
    (sqlite3.IntegrityError("UNIQUE constraint failed"), "permanent"), (sqlite3.ProgrammingError("syntax"), "permanent"),
    # SQLAlchemy wrappers
    (DBAPIError(OperationalError("x"), connection_invalidated=True), "connection_lost"),
    (DBAPIError(OperationalError("Deadlock found", errno=1213)), "transient"),
    (DBAPIError(sqlite3.IntegrityError("dup")), "permanent"),
    # generic
    (ConnectionResetError(), "connection_lost"), (BrokenPipeError(), "connection_lost"), (TimeoutError(), "transient"),
    (OSError(__import__("errno").ECONNRESET, "reset"), "connection_lost"), (FileNotFoundError(), "permanent"), (PermissionError(), "permanent"),
    (type("Unavailable", (Exception,), {})("grpc"), "transient"), (type("ResourceExhausted", (Exception,), {})("grpc"), "transient"),
    (type("DeadlockDetected", (Exception,), {})("pg"), "transient"), (type("Aborted", (Exception,), {})("409"), "transient"),
    (type("ServiceUnavailable", (ValueError,), {})("never retried: a ValueError is a caller bug"), "permanent"),
    (ValueError("bad"), "permanent"), (TypeError("bad"), "permanent"), (KeyError("k"), "permanent"), (RuntimeError("?"), "permanent"),
])
def test_classification_table(mod, exc, expected):
    assert mod._classify_error(exc) == expected


def test_duplicate_receipt_error_is_permanent_and_an_unknown_error_is_what_the_caller_says(mod):
    assert mod._classify_error(mod.DuplicateReceiptError("x")) == "permanent"
    assert mod._classify_error(RuntimeError("?"), unknown="transient") == "transient"


def test_a_timeout_is_retried_only_when_the_work_is_idempotent(mod):
    calls = []

    def stmt():
        calls.append(1)
        raise OperationalError("SQL execution canceled", errno=604)
    r = mod._Retrier(3, 1.0, sleep=lambda s: None)
    with pytest.raises(OperationalError):
        r.run(stmt, idempotent=False)
    assert len(calls) == 1
    calls.clear()
    with pytest.raises(OperationalError):
        r.run(stmt, idempotent=True)
    assert len(calls) == 3


# ------------------------------------------------------------------ SQL on a DBAPI connection factory

class FlakyCursor:
    def __init__(self, real, plan, conn):
        self._real, self._plan, self._conn = real, plan, conn

    description = property(lambda self: self._real.description)

    def _gate(self, op, sql):
        if self._conn.dead:
            raise OperationalError("server closed the connection unexpectedly")
        return self._plan.hit(op, sql)

    def execute(self, sql, params=None):
        rule = self._gate("execute", sql)
        if rule and not rule["after"]:
            raise rule["exc"]()
        self._real.execute(sql, params) if params is not None else self._real.execute(sql)
        if rule:
            raise rule["exc"]()

    def executemany(self, sql, rows):
        rule = self._gate("execute", sql)
        if rule and not rule["after"]:
            raise rule["exc"]()
        self._real.executemany(sql, rows)
        if rule:
            raise rule["exc"]()

    def fetchall(self):
        return self._real.fetchall()

    def close(self):
        self._real.close()


class FlakyConn:
    def __init__(self, path, plan, registry):
        self._real, self._plan = sqlite3.connect(path), plan
        self.dead = self.closed = False
        registry.append(self)

    def cursor(self):
        return FlakyCursor(self._real.cursor(), self._plan, self)

    def commit(self):
        if self.dead:
            raise OperationalError("server closed the connection unexpectedly")
        rule = self._plan.hit("commit")
        if rule and not rule["after"]:
            raise rule["exc"]()
        self._real.commit()
        if rule:
            raise rule["exc"]()

    def rollback(self):
        if self.dead:
            raise OperationalError("server closed the connection unexpectedly")
        self._real.rollback()

    def close(self):
        self.closed = True
        self._real.close()


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def make_dbapi(mod, tmp_path, cls=None, **kw):
    plan, conns, sleeps = Plan(), [], []
    cls = cls or mod.SqlContextReceiptStore
    path = str(tmp_path / "f.sqlite3")
    store = cls(connection_factory=lambda: FlakyConn(path, plan, conns), dialect="sqlite", paramstyle="qmark", table_prefix="g_",
                retry_sleep=sleeps.append, retry_rng=UpperRng(), **kw)
    store.plan, store.conns, store.sleeps = plan, conns, sleeps
    return store


@pytest.fixture()
def dbapi(mod, tmp_path):
    store = make_dbapi(mod, tmp_path)
    store.ensure_tables()
    store.plan.calls.clear()
    yield store
    store.close()


def sql_count(store, table, where="1 = 1"):
    return store._query(f"SELECT COUNT(1) AS n FROM {store._t(table)} WHERE {where}")[0]["n"]


def test_dbapi_transient_errors_retried_then_succeed(dbapi):
    dbapi.plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=2)
    rid = dbapi.record_retrieval(context_name="kb", items=[item("a")])
    assert sql_count(dbapi, "context_receipts", f"receipt_id = '{rid}'") == 1 and sql_count(dbapi, "context_receipt_items") == 1
    assert len(dbapi.sleeps) == 2


@pytest.mark.parametrize("exc", [sqlite3.IntegrityError, sqlite3.ProgrammingError, lambda: ValueError("x")])
def test_dbapi_permanent_errors_not_retried(dbapi, exc):
    dbapi.plan.add("execute", "SELECT", exc, times=None)
    with pytest.raises(Exception):
        dbapi.get_receipt("r")
    assert dbapi.sleeps == [] and dbapi.plan.count("execute", "SELECT") == 1


def test_dbapi_attempt_cap_and_no_multiplication(dbapi):
    dbapi.plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=None)
    with pytest.raises(OperationalError) as info:
        dbapi.record_retrieval(context_name="kb", items=[item("a")])
    assert dbapi.plan.count("execute", "INSERT INTO") == 4         # one INSERT per attempt (the first INSERT fails), 4 attempts total
    assert any("4 attempt" in n for n in info.value.__notes__)
    assert sql_count(dbapi, "context_receipts") == 0 and sql_count(dbapi, "context_receipt_items") == 0   # each attempt rolled back


def test_dbapi_record_retrieval_after_commit_whose_ack_was_lost_does_not_duplicate_or_raise(dbapi):
    dbapi.plan.add("commit", None, ConnectionResetError, times=1, after=True)
    rid = dbapi.record_retrieval(context_name="kb", receipt_id="r1", query_hash="h", items=[item("a"), item("b")])
    assert rid == "r1"
    assert sql_count(dbapi, "context_receipts") == 1 and sql_count(dbapi, "context_receipt_items") == 2
    assert len(dbapi.conns) == 2 and dbapi.conns[0].closed            # the connection whose commit was ambiguous was discarded


def test_dbapi_retry_that_finds_a_different_receipt_raises_duplicate(dbapi, mod):
    dbapi.record_retrieval(context_name="theirs", receipt_id="r1", query_hash="h", items=[item("a")])
    dbapi.plan.add("execute", "COUNT(1)", lambda: OperationalError("database is locked"), times=1)
    with pytest.raises(mod.DuplicateReceiptError):
        dbapi.record_retrieval(context_name="ours", receipt_id="r1", query_hash="h", items=[item("a")])
    assert sql_count(dbapi, "context_receipts") == 1


def test_dbapi_append_event_after_lost_commit_ack_never_duplicates(dbapi):
    rid = dbapi.record_retrieval(context_name="kb", items=[item("a")])
    dbapi.plan.add("commit", None, ConnectionResetError, times=2, after=True)
    dbapi.record_outcome(rid, signal="thumbs_up")
    assert sql_count(dbapi, "context_receipt_events") == 1
    dbapi.plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=1)
    dbapi.record_outcome(rid, signal="thumbs_down")
    assert sql_count(dbapi, "context_receipt_events") == 2


def test_dbapi_refresh_delete_and_retention_are_retried_whole(dbapi):
    rid = dbapi.record_retrieval(context_name="kb", items=[item("a")])
    dbapi.record_outcome(rid, signal="thumbs_up")
    dbapi.plan.add("commit", None, ConnectionResetError, times=1, after=True)
    assert dbapi.refresh_item_scores() == 1
    assert sql_count(dbapi, "context_item_scores") == 1
    dbapi.plan.add("commit", None, ConnectionResetError, times=1, after=True)
    assert dbapi.delete_receipts([rid]) == 1
    assert sql_count(dbapi, "context_receipts") == 0


def test_dead_connection_is_replaced_once_and_the_call_succeeds(dbapi):
    rid = dbapi.record_retrieval(context_name="kb", items=[item("a")])
    first = dbapi.conns[-1]
    first.dead = True                                  # idle kill / session expiry: every use now fails
    assert dbapi.get_receipt(rid)["context_name"] == "kb"
    assert len(dbapi.conns) == 2 and first.closed and not dbapi.conns[1].closed
    assert len(dbapi.sleeps) == 1                      # exactly one retry


def test_a_connection_that_cannot_be_replaced_exhausts_the_budget_and_raises_the_original_error(mod, tmp_path):
    store = make_dbapi(mod, tmp_path)
    store.ensure_tables()
    store.conns[-1].dead = True
    store._exec._connect = lambda: (_ for _ in ()).throw(ConnectionRefusedError(111, "refused"))
    with pytest.raises(ConnectionRefusedError):
        store.get_receipt("r")
    assert len(store.sleeps) == 3


def test_connection_is_recycled_by_max_age(mod, tmp_path):
    clock = Clock()
    store = make_dbapi(mod, tmp_path, connection_max_age_seconds=60, connection_clock=clock)
    store.ensure_tables()
    n = len(store.conns)
    store.get_receipt("r")
    clock.now = 59
    store.get_receipt("r")
    assert len(store.conns) == n                      # not old enough
    clock.now = 61
    store.get_receipt("r")
    assert len(store.conns) == n + 1 and store.conns[n - 1].closed and not store.conns[n].closed
    store.close()
    assert store.conns[n].closed


def test_no_max_age_means_the_connection_is_kept(dbapi):
    dbapi.get_receipt("r")
    dbapi.get_receipt("r")
    assert len(dbapi.conns) == 1


def test_close_releases_every_connection_and_the_store_can_be_used_again(mod, tmp_path):
    store = make_dbapi(mod, tmp_path)
    store.ensure_tables()
    store.close()
    store.close()                                     # idempotent
    assert all(c.closed for c in store.conns)
    store.get_receipt("r")                            # reconnects
    assert len(store.conns) == 2
    store.close()


def test_a_failed_paramstyle_probe_does_not_leak_the_connection(mod, tmp_path):
    conns = []
    path = str(tmp_path / "x.sqlite3")
    store = mod.SqlContextReceiptStore(connection_factory=lambda: FlakyConn(path, Plan(), conns), dialect="sqlite")   # paramstyle undetectable
    with pytest.raises(ValueError, match="paramstyle"):
        store.get_receipt("r")
    assert conns and all(c.closed for c in conns)


def test_dbapi_selftest_passes_through_transient_failures(dbapi):
    dbapi.plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=1)
    dbapi.plan.add("commit", None, ConnectionResetError, times=1, after=True)
    steps = dbapi.selftest()
    assert steps


def test_a_transient_probe_failure_is_not_mistaken_for_a_missing_column_in_sql(mod, tmp_path):
    store = make_dbapi(mod, tmp_path)
    store.plan.add("execute", "dagster_partition_key FROM", lambda: OperationalError("database is locked"), times=None)
    with pytest.raises(OperationalError):
        store.ensure_tables()
    assert not any("ALTER" in t for _, t in store.plan.calls)
    store.close()


# ------------------------------------------------------------------ SQL on a SQLAlchemy engine

def test_sqlalchemy_engine_path_retries_transient_errors(mod, tmp_path):
    sa = pytest.importorskip("sqlalchemy")
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'e.sqlite3'}")
    sleeps, seen = [], {"n": 0}
    store = mod.SqlContextReceiptStore(engine, table_prefix="g_", retry_sleep=sleeps.append, retry_rng=UpperRng())
    store.ensure_tables()

    @sa.event.listens_for(engine, "before_cursor_execute")
    def fail(conn, cursor, statement, parameters, context, executemany):
        if "g_context_receipts" in statement and statement.startswith("INSERT") and seen["n"] < 2:
            seen["n"] += 1
            raise sqlite3.OperationalError("database is locked")

    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    assert len(sleeps) == 2 and store.get_receipt(rid) is not None
    assert sql_count(store, "context_receipt_items") == 1
    seen["n"] = -99
    with pytest.raises(sa.exc.OperationalError) as info:          # SQLAlchemy wraps the driver error: classified through .orig
        store.record_retrieval(context_name="kb", items=[item("a")])
    assert len(sleeps) == 2 + 3 and any("4 attempt" in n for n in info.value.__notes__)
    engine.dispose()


# ------------------------------------------------------------------ async stores rely on the store-level retry

def test_async_sql_transient_errors_do_not_dead_letter_and_are_not_multiplied(mod, tmp_path):
    store = make_dbapi(mod, tmp_path, cls=mod.AsyncSqlContextReceiptStore)
    store.flush()
    inner_plan = store.plan
    inner_plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=2)
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.flush()
    assert store.dead_letters == [] and store.get_receipt(rid) is not None
    # an error that outlasts the store's whole budget dead-letters ONCE, after exactly retry_max_attempts tries (the writer adds none)
    inner_plan.calls.clear()
    inner_plan.add("execute", "INSERT INTO", lambda: OperationalError("database is locked"), times=None)
    store.record_retrieval(context_name="kb", items=[item("b")])
    store.flush()
    assert len(store.dead_letters) == 1 and inner_plan.count("execute", "INSERT INTO") == 4
    store.close()


def test_async_sql_permanent_errors_dead_letter_immediately(mod, tmp_path):
    store = make_dbapi(mod, tmp_path, cls=mod.AsyncSqlContextReceiptStore)
    store.flush()
    store.plan.add("execute", "INSERT INTO", sqlite3.ProgrammingError, times=None)
    store.record_retrieval(context_name="kb", items=[item("a")])
    store.flush()
    assert len(store.dead_letters) == 1 and store.plan.count("execute", "INSERT INTO") == 1 and store.sleeps == []
    store.close()


def test_async_sql_ack_lost_commit_does_not_duplicate(mod, tmp_path):
    store = make_dbapi(mod, tmp_path, cls=mod.AsyncSqlContextReceiptStore)
    store.flush()
    store.plan.add("commit", None, ConnectionResetError, times=1, after=True)
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.flush()
    assert store.dead_letters == []
    assert sql_count(store, "context_receipts", f"receipt_id = '{rid}'") == 1
    store.close()


def test_async_bigquery_transient_vs_permanent(mod, monkeypatch):
    plan, sleeps = Plan(), []
    inner = FakeBigQueryClient("proj")
    inner.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(inner))
    client = FlakyBQClient(inner, plan)
    store = mod.AsyncBigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_",
                                                 retry_sleep=sleeps.append, retry_rng=UpperRng())
    plan.add("load", "t_context_receipts", ServiceUnavailable, times=1, after=True)
    plan.add("query", "COUNT(1)", TooManyRequests, times=1)
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.flush()
    assert store.dead_letters == [] and bq_count(store, "context_receipts", f"receipt_id = '{rid}'") == 1
    plan.add("load", None, BadRequest, times=None)
    store.record_retrieval(context_name="kb", items=[item("b")])
    store.flush()
    assert len(store.dead_letters) == 1
    store.close()


def test_async_writer_defaults_zero_retries_over_a_retrying_store_and_five_over_sqlite(mod, tmp_path):
    sqlite_store = mod.ContextReceiptStore(str(tmp_path / "s.sqlite3"))
    assert mod.AsyncReceiptWriter(sqlite_store).max_retries == 5
    retrying = make_dbapi(mod, tmp_path)
    assert mod.AsyncReceiptWriter(retrying).max_retries == 0
    assert mod.AsyncReceiptWriter(retrying, max_retries=2).max_retries == 2


def test_async_writer_dead_letters_known_permanent_errors_without_retrying_and_retries_unknown_ones(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "s.sqlite3"))
    calls = {"n": 0}

    def perm(**kw):
        calls["n"] += 1
        raise sqlite3.ProgrammingError("syntax")
    store.record_retrieval = perm
    sleeps = []
    w = mod.AsyncReceiptWriter(store, max_retries=5, sleep=sleeps.append)
    w.record_retrieval(context_name="c", items=[])
    w.flush()
    assert calls["n"] == 1 and len(w.dead_letters) == 1 and sleeps == []
    calls["n"] = 0

    def unknown(**kw):
        calls["n"] += 1
        raise RuntimeError("hiccup")
    store.record_retrieval = unknown
    w.record_retrieval(context_name="c", items=[])
    w.flush()
    assert calls["n"] == 6 and len(w.dead_letters) == 2 and len(sleeps) == 5


def test_async_close_flushes_and_closes_the_worker_connection(mod, tmp_path):
    store = make_dbapi(mod, tmp_path, cls=mod.AsyncSqlContextReceiptStore)
    store.record_retrieval(context_name="kb", items=[item("a")])
    store.close()
    assert store.dead_letters == [] and all(c.closed for c in store.conns)
    assert not store._writer._worker.is_alive()


# ------------------------------------------------------------------ component surface

def test_component_exposes_and_validates_the_retry_fields(mod):
    c = mod.ContextReceiptStoreComponent(backend="bigquery", bigquery_dataset="p.d", retry_max_attempts=2, retry_max_backoff_seconds=3.0,
                                         async_writes=False)
    assert c.retry_max_attempts == 2
    for bad in ({"retry_max_attempts": 0}, {"retry_max_backoff_seconds": -1.0}, {"connection_max_age_seconds": 0}):
        with pytest.raises(ValueError):
            mod.ContextReceiptStoreComponent(backend="bigquery", bigquery_dataset="p.d", async_writes=False, **bad).build_defs(None)
