"""A google.cloud.bigquery.Client subclass for CONTRACT tests: the REAL library classes and the REAL method signatures, no network.

Why: our DuckDB-backed fake (bq_fake.py) accepts whatever the code under test hands it, so it cannot catch "we called the client the way
the fake allows but the real library does not" (the real bug that motivated this file: dagster-gcp's ``BigQueryResource.get_client()``
is a @contextmanager, and a helper called ``.query`` on it). Here:

  * ``query`` / ``load_table_from_json`` / ``create_dataset`` BIND their arguments against ``inspect.signature`` of the real
    ``bigquery.Client`` methods, so a wrong keyword or a missing argument raises TypeError exactly as the real client would;
  * QueryJobConfig / LoadJobConfig / ScalarQueryParameter / ArrayQueryParameter / SchemaField must be the real classes, and are
    serialized with their real ``to_api_repr()`` -- the JSON that would go over the wire -- and checked against the REST API's rules
    (parameter types, modes, referenced @names, JSON-serializable load rows, table reference parses);
  * the statement is then EXECUTED by the DuckDB fake underneath (statements are parsed as BigQuery by sqlglot), so a whole store /
    sync run can be driven end to end through the real-signature surface.

Skips the importing module when google-cloud-bigquery is not installed. Identical copies live in the three test dirs that use it."""
import inspect
import json
import re
import sys
import types
from pathlib import Path

import pytest

bigquery = pytest.importorskip("google.cloud.bigquery")
if not inspect.isclass(getattr(bigquery, "Client", None)):      # an earlier test's fake module (bq_fake.install_fake_bigquery) is not the library
    pytest.skip("google.cloud.bigquery here is a test fake, not the real library", allow_module_level=True)
import google.cloud  # noqa: E402  (real package, imported now so a later fake install cannot replace it)

try:                                    # imported as part of a tests package (context_engineering_pipeline) ...
    from .bq_fake import FakeBigQueryClient  # noqa: E402
except ImportError:                     # ... or as a top-level module (the other test dirs put their own directory on sys.path)
    sys.path.insert(0, str(Path(__file__).parent))
    from .bq_fake import FakeBigQueryClient  # noqa: E402

REAL_BIGQUERY = bigquery

QUERY_SIG = inspect.signature(bigquery.Client.query)
LOAD_SIG = inspect.signature(bigquery.Client.load_table_from_json)
CREATE_DATASET_SIG = inspect.signature(bigquery.Client.create_dataset)

# REST API standard-SQL parameter types (jobs.query QueryParameterType); legacy aliases are deliberately NOT accepted
SCALAR_TYPES = {"STRING", "INT64", "FLOAT64", "BOOL", "TIMESTAMP", "DATE", "DATETIME", "TIME", "NUMERIC", "BIGNUMERIC", "BYTES", "JSON", "GEOGRAPHY"}
# TableFieldSchema.type accepts both spellings
SCHEMA_TYPES = SCALAR_TYPES | {"INTEGER", "FLOAT", "BOOLEAN", "RECORD", "STRUCT"}
SCHEMA_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}
WRITE_DISPOSITIONS = {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"}


def referenced_names(sql: str) -> set:
    """``@name`` parameters referenced by ``sql`` outside string literals, quoted identifiers and comments."""
    stripped = re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`[^`]*`|--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S)
    return set(re.findall(r"(?<![@\w])@([A-Za-z_]\w*)", stripped))


def check_query_config(sql: str, cfg) -> list:
    """Validate a QueryJobConfig the way the API would; returns the serialized parameters."""
    if cfg is None:
        assert not referenced_names(sql), f"SQL references {sorted(referenced_names(sql))} but no job_config with parameters was sent: {sql}"
        return []
    assert isinstance(cfg, bigquery.QueryJobConfig), f"job_config must be the real QueryJobConfig, got {type(cfg)!r}"
    repr_ = cfg.to_api_repr()
    json.dumps(repr_)
    params = repr_.get("query", {}).get("queryParameters", [])
    if params:
        assert repr_["query"].get("parameterMode") == "NAMED", repr_
    names = [p.get("name") for p in params]
    assert all(names) and len(names) == len(set(names)), f"parameters must be uniquely named: {names}"
    for p in params:
        t = p["parameterType"]
        if t["type"] == "ARRAY":
            assert t["arrayType"]["type"] in SCALAR_TYPES, p
            assert isinstance(p["parameterValue"]["arrayValues"], list), p
            for v in p["parameterValue"]["arrayValues"]:
                assert v.get("value") is not None, f"NULL array element in {p}"
        else:
            assert t["type"] in SCALAR_TYPES, f"{t['type']!r} is not a BigQuery parameter type: {p}"
            assert p["parameterValue"].get("value") is not None, f"NULL scalar parameter: {p}"
            if t["type"] in ("INT64", "FLOAT64"):
                float(p["parameterValue"]["value"])
            if t["type"] == "BOOL":
                assert str(p["parameterValue"]["value"]).lower() in ("true", "false"), p
    missing = referenced_names(sql) - set(names)
    assert not missing, f"SQL references @{sorted(missing)} with no parameter (sent: {names}): {sql}"
    return params


def check_load_config(cfg) -> None:
    assert cfg is None or isinstance(cfg, bigquery.LoadJobConfig), f"job_config must be the real LoadJobConfig, got {type(cfg)!r}"
    if cfg is None:
        return
    repr_ = cfg.to_api_repr()
    json.dumps(repr_)
    load = repr_.get("load", {})
    if "writeDisposition" in load:
        assert load["writeDisposition"] in WRITE_DISPOSITIONS, load
    for f in (load.get("schema") or {}).get("fields", []):
        assert f["type"] in SCHEMA_TYPES, f"{f['type']!r} is not a BigQuery column type: {f}"
        assert f.get("mode", "NULLABLE") in SCHEMA_MODES, f


class _Params:
    """What the DuckDB fake reads from a job config (``.query_parameters[i].name/.value``), built from the REAL objects."""

    def __init__(self, cfg):
        self.query_parameters = [types.SimpleNamespace(name=p.name, value=(list(p.values) if isinstance(p, bigquery.ArrayQueryParameter) else p.value))
                                 for p in (cfg.query_parameters if cfg is not None else [])]


class ContractClient(bigquery.Client):
    """The real Client class (isinstance/signature/__enter__ behave as in production) whose network methods are validated, then run on DuckDB."""

    def __init__(self, backend: FakeBigQueryClient | None = None, project: str = "proj"):
        self._backend = backend or FakeBigQueryClient(project)
        self._project = project          # skip Client.__init__: it wants credentials
        self.calls: list = []
        self.closed = 0

    @property
    def project(self):
        return self._project

    def query(self, *args, **kwargs):
        bound = QUERY_SIG.bind(self, *args, **kwargs)
        sql, cfg = bound.arguments["query"], bound.arguments.get("job_config")
        assert isinstance(sql, str) and sql.strip()
        params = check_query_config(sql, cfg)
        self.calls.append(("query", sql, params))
        return self._backend.query(sql, job_config=_Params(cfg))

    def load_table_from_json(self, *args, **kwargs):
        bound = LOAD_SIG.bind(self, *args, **kwargs)
        rows, dest, cfg = bound.arguments["json_rows"], bound.arguments["destination"], bound.arguments.get("job_config")
        rows = list(rows)
        for r in rows:
            assert isinstance(r, dict)
            json.dumps(r, ensure_ascii=False)         # the real client json.dumps each row into NDJSON
        ref = bigquery.TableReference.from_string(dest, default_project=self.project) if isinstance(dest, str) else dest
        assert ref.dataset_id and ref.table_id, ref
        assert "`" not in str(dest), f"destination must not carry backticks: {dest!r}"
        check_load_config(cfg)
        self.calls.append(("load", str(ref), rows, cfg.to_api_repr() if cfg is not None else None))
        shim = None if cfg is None else types.SimpleNamespace(schema=cfg.schema, schema_update_options=cfg.schema_update_options)
        return self._backend.load_table_from_json(rows, dest, job_config=shim)

    def create_dataset(self, *args, **kwargs):
        CREATE_DATASET_SIG.bind(self, *args, **kwargs)
        return self._backend.create_dataset(args[0] if args else kwargs["dataset"], exists_ok=kwargs.get("exists_ok", False))

    def close(self):
        self.closed += 1

    # queries / loads, in order, for assertions
    def sql(self) -> list:
        return [c[1] for c in self.calls if c[0] == "query"]

    def loads(self) -> list:
        return [c for c in self.calls if c[0] == "load"]


def install_real_bigquery(monkeypatch) -> None:
    """Make ``from google.cloud import bigquery`` resolve to the REAL module again, whatever fake an earlier test left on the package."""
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", REAL_BIGQUERY)
    monkeypatch.setattr(google.cloud, "bigquery", REAL_BIGQUERY, raising=False)
