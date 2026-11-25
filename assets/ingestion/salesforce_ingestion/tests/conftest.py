"""Shared test helpers for SalesforceIngestionComponent.

The real `simple_salesforce` package isn't installed in this test environment, and this
component does a local `from simple_salesforce import Salesforce` inside the asset body --
a fake module is injected into sys.modules (same lazy-optional-dependency convention as
azure-storage-blob/google-cloud-storage/paramiko elsewhere in this repo) via
monkeypatch.setitem, not unittest.mock.patch.

The dlt pipeline run + sql_client() query-back are faked wholesale (this repo's "mock only
the paid call" convention) -- everything this component actually owns (SOQL construction,
incremental predicate + quoting, destination/staging resolution, resource dispatch,
DataFrame concatenation, partition filtering, preview metadata) is exercised for real
through dg.materialize().

Previously this faked `dlt.sources.salesforce` (dlt's verified source) instead -- removed
along with the component's own real bug of calling that source with the wrong keyword
arguments; see component.py's "inline Salesforce dlt source" comment block for why.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, List, Optional

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "salesforce_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Must be registered in sys.modules before exec -- dlt's real @dlt.resource decorator
    # (now actually exercised, since it's no longer faked away entirely) introspects the
    # decorated function's module via sys.modules.get(f.__module__) when building its spec,
    # and raises AttributeError on a bare None if this module was never registered there.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Permissive:
    """Generic duck-typed stand-in -- every attribute access or call returns another
    permissive object, so code that doesn't matter for a given test succeeds without
    needing to replicate real shape."""

    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter([self])


class FakeSFType:
    """Stands in for `getattr(Salesforce_instance, sobject)` -- an SFType with a
    describe()-shaped payload. `fields` is a list of {"name", "type", "compoundFieldName"}
    dicts (describe()'s real shape, trimmed to what this component reads)."""

    def __init__(self, fields: List[Dict[str, Any]]):
        self._fields = fields

    def describe(self):
        return {"fields": self._fields}


class FakeBulkHandler:
    """Stands in for `sf.bulk` -- `getattr(bulk, sobject)` returns a FakeBulkObject whose
    query_all(soql, lazy_operation=True) yields the configured pages (or raises to exercise
    the standard-SOQL fallback). Every issued SOQL string is appended to `queries` (shared
    with the owning FakeSalesforce instance) so tests can assert on exactly what was built."""

    def __init__(
        self,
        pages_by_object: Dict[str, List[List[Dict[str, Any]]]],
        queries: List[str],
        raise_async_disabled: bool = False,
    ):
        self._pages_by_object = pages_by_object
        self._queries = queries
        self._raise_async_disabled = raise_async_disabled

    def __getattr__(self, sobject):
        pages = self._pages_by_object.get(sobject, [])
        raise_flag = self._raise_async_disabled
        queries = self._queries

        class _BulkObject:
            def query_all(self_inner, soql, lazy_operation=True):
                queries.append(soql)
                if raise_flag:
                    from simple_salesforce.exceptions import SalesforceMalformedRequest
                    raise SalesforceMalformedRequest(
                        url="fake://bulk", status=400,
                        resource_name=sobject,
                        content=[{"message": "Async API not enabled for this org."}],
                    )
                return iter(pages)

        return _BulkObject()


class FakeSalesforce:
    """Stands in for `simple_salesforce.Salesforce(username=..., password=..., security_token=...)`.

    `describe_by_object`: {sobject: [field dicts]} -- drives FakeSFType.describe().
    `bulk_pages_by_object`: {sobject: [[record dict, ...], ...]} -- pages yielded by the Bulk
    API path (each inner list is one "page" of records, each record carrying an "attributes"
    key the way real Salesforce responses do, stripped by the component).
    `standard_records_by_object`: {sobject: [record dict, ...]} -- used only when
    `raise_async_disabled=True`, to exercise the standard-SOQL + query_more fallback path.
    Every issued SOQL string is recorded in `.queries` (both describe-driven bulk calls and
    `.query()` fallback calls) so tests can assert on exactly what was generated."""

    def __init__(
        self,
        describe_by_object: Dict[str, List[Dict[str, Any]]],
        bulk_pages_by_object: Optional[Dict[str, List[List[Dict[str, Any]]]]] = None,
        standard_records_by_object: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        raise_async_disabled: bool = False,
    ):
        self.queries: List[str] = []
        self._describe_by_object = describe_by_object
        self._standard_records_by_object = standard_records_by_object or {}
        self.bulk = FakeBulkHandler(
            bulk_pages_by_object or {}, self.queries, raise_async_disabled=raise_async_disabled,
        )
        self._raise_async_disabled = raise_async_disabled

    def __getattr__(self, sobject):
        return FakeSFType(self._describe_by_object.get(sobject, []))

    def query(self, soql):
        self.queries.append(soql)
        sobject = soql.split("FROM", 1)[1].strip().split()[0]
        records = self._standard_records_by_object.get(sobject, [])
        return {"records": list(records), "done": True, "nextRecordsUrl": ""}

    def query_more(self, url):
        return {"records": [], "done": True, "nextRecordsUrl": ""}


def install_fake_simple_salesforce(monkeypatch, fake_client: "FakeSalesforce"):
    """Installs a fake `simple_salesforce` (+ `.exceptions`) module whose `Salesforce(...)`
    constructor always returns `fake_client`, regardless of the credentials passed."""
    exceptions_mod = types.ModuleType("simple_salesforce.exceptions")

    class SalesforceMalformedRequest(Exception):
        def __init__(self, url, status, resource_name, content):
            self.url = url
            self.status = status
            self.resource_name = resource_name
            self.content = content
            super().__init__(f"Malformed request {url} status={status}: {content}")

    exceptions_mod.SalesforceMalformedRequest = SalesforceMalformedRequest

    fake_mod = types.ModuleType("simple_salesforce")
    fake_mod.Salesforce = lambda **kwargs: fake_client
    fake_mod.exceptions = exceptions_mod
    monkeypatch.setitem(sys.modules, "simple_salesforce", fake_mod)
    monkeypatch.setitem(sys.modules, "simple_salesforce.exceptions", exceptions_mod)


# Standard describe() payload used by most tests: an "Id" (string-like) and a
# "SystemModstamp" (datetime) field, enough to exercise both SOQL-quoting branches.
DEFAULT_DESCRIBE_FIELDS = [
    {"name": "Id", "type": "id", "compoundFieldName": None},
    {"name": "Name", "type": "string", "compoundFieldName": None},
    {"name": "SystemModstamp", "type": "datetime", "compoundFieldName": None},
    {"name": "BillingAddress", "type": "address", "compoundFieldName": None},
    {"name": "BillingStreet", "type": "string", "compoundFieldName": "BillingAddress"},
]


class FakeCursor:
    """Stands in for the DBAPI cursor `pipeline.sql_client().execute_query()`
    yields -- `.description` (column name tuples) and `.fetchall()` (row
    tuples) are the only surface this component touches."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def description(self):
        return [(col,) for col in self._df.columns]

    def fetchall(self):
        return list(self._df.itertuples(index=False, name=None))


class FakeSqlClient:
    def __init__(self, table_rows: Dict[str, pd.DataFrame]):
        self.table_rows = table_rows
        self.queries: List[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def execute_query(self, sql: str):
        self.queries.append(sql)
        for table_name, df in self.table_rows.items():
            if sql.strip().endswith(f".{table_name}"):
                yield FakeCursor(df)
                return
        yield FakeCursor(pd.DataFrame())


class FakePipeline:
    def __init__(self, table_rows: Optional[Dict[str, pd.DataFrame]] = None, **pipeline_kwargs):
        self.pipeline_kwargs = pipeline_kwargs
        self.run_calls: List[Any] = []
        self._table_rows = table_rows or {}
        self.sql_client_instance = FakeSqlClient(self._table_rows)

    def run(self, source):
        self.run_calls.append(source)
        return f"fake-load-info(resources={len(self.run_calls)})"

    def sql_client(self):
        return self.sql_client_instance


def install_fake_dlt(
    monkeypatch,
    mod,
    table_rows: Optional[Dict[str, pd.DataFrame]] = None,
    fake_sf_client: Optional["FakeSalesforce"] = None,
):
    """Fakes dlt.pipeline(...) so the asset body never touches a real DuckDB file, AND
    installs a fake simple_salesforce client (default: DEFAULT_DESCRIBE_FIELDS, zero rows --
    sufficient for every test that doesn't care what SOQL got generated or what rows flowed
    through, since `pipeline.run(...)` is also faked here and never actually executes the
    dlt resource generators -- but resource construction itself still calls describe()
    eagerly, so every test needs SOME working fake client). Pass `fake_sf_client` (an
    `install_fake_simple_salesforce`-ready `FakeSalesforce`) for a test that needs to assert
    on generated SOQL or exercise the bulk/standard-API fallback."""
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    install_fake_simple_salesforce(
        monkeypatch,
        fake_sf_client or FakeSalesforce(describe_by_object={}),
    )
    return fake_pipeline
