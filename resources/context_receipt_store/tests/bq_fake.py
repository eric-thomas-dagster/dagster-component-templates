"""A fake `google.cloud.bigquery` backed by DuckDB, for tests that cannot reach BigQuery.

It EXECUTES the real BigQuery SQL the code under test sends: the SQL is parsed as the BigQuery dialect with sqlglot (so SQL that is not valid
BigQuery fails the test), transpiled to DuckDB and run. That checks syntax and semantics of the logic; it is NOT BigQuery itself -- dialect
corners (type coercion, NULL ordering, JSON functions) can differ, so passing here never proves it runs on real BigQuery.

Usage:
    client = FakeBigQueryClient(project="proj")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(client))   # also registers google / google.cloud
    # a dagster-gcp style resource: FakeBigQueryResource(client) with .get_client() as a context manager
"""
import contextlib
import re
import sys
import types
from typing import Any, Dict, List

import duckdb
import sqlglot
from sqlglot import exp


class Row(dict):
    """google.cloud.bigquery.table.Row: access by key, attribute, or index."""

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as e:
            raise AttributeError(k) from e

    def __getitem__(self, k):
        return list(self.values())[k] if isinstance(k, int) else super().__getitem__(k)

    def values(self):  # noqa: D102
        return super().values()


class ScalarQueryParameter:
    def __init__(self, name, type_, value):
        self.name, self.type_, self.value = name, type_, value


class ArrayQueryParameter:
    def __init__(self, name, array_type, values):
        self.name, self.array_type, self.value = name, array_type, list(values)


class QueryJobConfig:
    def __init__(self, query_parameters=None, **kw):
        self.query_parameters = list(query_parameters or [])
        self.__dict__.update(kw)


class LoadJobConfig:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class SchemaField:
    def __init__(self, name, field_type, mode="NULLABLE", **kw):
        self.name, self.field_type, self.mode = name, field_type, mode


class _Job:
    def __init__(self, rows=None, affected=None):
        self._rows, self.num_dml_affected_rows, self.errors = rows or [], affected, None

    def result(self, *a, **k):
        return iter(self._rows)

    def to_dataframe(self, *a, **k):
        import pandas as pd
        return pd.DataFrame([dict(r) for r in self._rows])


def _strip_project(sql: str, project: str) -> str:
    # `proj.ds.tbl` / proj.ds.tbl -> ds.tbl (DuckDB schemas stand in for datasets)
    sql = re.sub(r"`([^`.]+)\.([^`.]+)\.([^`.]+)`", lambda m: f"{m.group(2)}.{m.group(3)}", sql)
    sql = re.sub(r"`([^`.]+)\.([^`.]+)`", lambda m: f"{m.group(1)}.{m.group(2)}", sql)
    return re.sub(rf"\b{re.escape(project)}\.(\w+)\.(\w+)", r"\1.\2", sql)


def _zero_based_offsets(stmt):
    """BigQuery's `UNNEST(..) WITH OFFSET AS i` is 0-based; sqlglot renders DuckDB's `WITH ORDINALITY`, which is 1-based. Shift the
    offset column back in the SELECT that owns the UNNEST so the executed result matches BigQuery."""
    for select in stmt.find_all(exp.Select):
        for unnest in [u for u in select.find_all(exp.Unnest) if u.args.get("offset") is not None and u.find_ancestor(exp.Select) is select]:
            name = unnest.args["offset"].name
            for i, proj in enumerate(select.expressions):
                if isinstance(proj, exp.Column) and proj.name == name and not proj.table:
                    select.expressions[i] = exp.alias_(exp.Paren(this=exp.Sub(this=proj.copy(), expression=exp.Literal.number(1))), name)
    return stmt


def _sha2_is_already_hex(stmt):
    """BigQuery's SHA256() returns BYTES (so TO_HEX(SHA256(x)) is the hex digest); DuckDB's sha256() already returns hex, and sqlglot's
    BigQuery->DuckDB rendering of the TO_HEX would hex-encode it a second time. Drop the redundant TO_HEX around SHA2."""
    for node in list(stmt.find_all(exp.LowerHex)):
        if isinstance(node.this, exp.SHA2):
            node.replace(node.this)
    return stmt


class FakeBigQueryClient:
    def __init__(self, project: str = "proj", location=None):
        self.project, self.location = project, location
        self.con = duckdb.connect()
        self.queries: List[str] = []

    # -- SQL ----------------------------------------------------------------------------------------------
    def _to_duckdb(self, sql: str) -> str:
        sql = _strip_project(sql, self.project)
        out = []
        for stmt in sqlglot.parse(sql, read="bigquery"):       # raises ParseError for SQL that is not valid BigQuery
            if stmt is not None:
                out.append(_sha2_is_already_hex(_zero_based_offsets(stmt)).sql(dialect="duckdb"))
        return "; ".join(out)

    def query(self, sql: str, job_config=None, **kw) -> _Job:
        self.queries.append(sql)
        params: Dict[str, Any] = {p.name: p.value for p in (getattr(job_config, "query_parameters", None) or [])}
        translated = self._to_duckdb(sql)
        for name in params:                                     # sqlglot renders @name as :name or $name depending on version
            translated = re.sub(rf":{name}\b", f"${name}", translated)
        for stmt in [s for s in translated.split("; ") if s.strip()] or [translated]:
            used = {n: v for n, v in params.items() if f"${n}" in stmt}
            cur = self.con.execute(stmt, used) if used else self.con.execute(stmt)
        rows = []
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = [Row(zip(cols, r)) for r in cur.fetchall()]
        return _Job(rows)

    # -- writes -------------------------------------------------------------------------------------------
    def _norm_table(self, table) -> str:
        t = str(getattr(table, "full_table_id", table)).replace(":", ".").strip("`")
        parts = t.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else t

    def insert_rows_json(self, table, rows, **kw) -> list:
        self._insert(self._norm_table(table), rows)
        return []

    def load_table_from_json(self, rows, destination, job_config=None, **kw) -> _Job:
        table = self._norm_table(destination)
        # Like BigQuery: a load job whose explicit schema says NULLABLE for a column the table has as REQUIRED (NOT NULL) would relax the mode,
        # which needs schema_update_options=ALLOW_FIELD_RELAXATION; without it the job fails. (Without schema=, the table's own schema applies.)
        schema = getattr(job_config, "schema", None)
        if schema and "ALLOW_FIELD_RELAXATION" not in (getattr(job_config, "schema_update_options", None) or []):
            ds, _, name = table.rpartition(".")
            required = {r[0] for r in self.con.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND is_nullable = 'NO'"
                + (" AND table_schema = ?" if ds else ""), [name, ds] if ds else [name]).fetchall()}
            relaxed = sorted(f.name for f in schema if f.mode == "NULLABLE" and f.name in required)
            if relaxed:
                raise RuntimeError(f"400 Provided Schema does not match Table {destination}: field(s) {relaxed} changed mode from REQUIRED to NULLABLE")
        self._insert(table, rows)
        return _Job()

    def _insert(self, table: str, rows: List[dict]) -> None:
        for row in rows:
            cols = list(row)
            self.con.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})", [row[c] for c in cols])

    def create_dataset(self, dataset, exists_ok=False, **kw):
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {str(dataset).split('.')[-1]}")

    def close(self):
        pass


class FakeBigQueryResource:
    """Stands in for dagster-gcp's BigQueryResource: `with resource.get_client() as client`."""

    def __init__(self, client: FakeBigQueryClient):
        self._client = client

    @contextlib.contextmanager
    def get_client(self):
        yield self._client


def install_fake_bigquery(client: FakeBigQueryClient) -> types.ModuleType:
    """Return a module usable as `google.cloud.bigquery`; also makes `from google.cloud import bigquery` work."""
    mod = types.ModuleType("google.cloud.bigquery")
    mod.Client = lambda *a, **k: client
    for cls in (QueryJobConfig, LoadJobConfig, ScalarQueryParameter, ArrayQueryParameter, SchemaField):
        setattr(mod, cls.__name__, cls)
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    cloud = sys.modules.setdefault("google.cloud", types.ModuleType("google.cloud"))
    google.cloud, cloud.bigquery = cloud, mod
    sys.modules["google.cloud.bigquery"] = mod
    return mod
