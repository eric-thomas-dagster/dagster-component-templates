"""Test helpers for x: loads component.py via importlib; only the external service is faked."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("firestore_writer_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeBatch:
    def __init__(self, client):
        self.client = client
        self.ops = []

    def set(self, ref, body, merge=False):
        self.ops.append(("merge" if merge else "set", ref.path, dict(body)))

    def create(self, ref, body):
        self.ops.append(("create", ref.path, dict(body)))

    def commit(self):
        self.client.committed.extend(self.ops)
        self.client.commit_count += 1
        self.ops = []


class _DocRef:
    def __init__(self, path):
        self.path = path


class _Col:
    def __init__(self, name):
        self.name = name
        self.auto = 0

    def document(self, doc_id=None):
        if doc_id is None:
            self.auto += 1
            doc_id = f"auto{self.auto}"
        return _DocRef(f"{self.name}/{doc_id}")


class FakeFirestoreClient:
    """In-memory stand-in for google.cloud.firestore.Client (collection/document/batch)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.committed = []
        self.commit_count = 0

    def collection(self, name):
        return _Col(name)

    def batch(self):
        return FakeBatch(self)


class FakeFirestoreResource:
    """Same surface as FirestoreResource: get_client()."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


def install_fake_google_firestore(monkeypatch):
    """Inject fakes for exactly the google.* modules the component imports (restored at teardown).
    The real top-level `google` namespace package is left alone."""
    clients = []
    creds_calls = []

    firestore = types.ModuleType("google.cloud.firestore")

    def Client(**kwargs):
        c = FakeFirestoreClient(**kwargs)
        clients.append(c)
        return c

    firestore.Client = Client

    cloud = types.ModuleType("google.cloud")
    cloud.__path__ = []
    cloud.firestore = firestore

    service_account = types.ModuleType("google.oauth2.service_account")

    class Credentials:
        @staticmethod
        def from_service_account_info(info):
            creds_calls.append(info)
            return ("creds", info.get("client_email"))

    service_account.Credentials = Credentials
    oauth2 = types.ModuleType("google.oauth2")
    oauth2.__path__ = []
    oauth2.service_account = service_account

    class AlreadyExists(Exception):
        pass

    exceptions = types.ModuleType("google.api_core.exceptions")
    exceptions.AlreadyExists = AlreadyExists
    api_core = types.ModuleType("google.api_core")
    api_core.__path__ = []
    api_core.exceptions = exceptions

    for name, m in (("google.cloud", cloud), ("google.cloud.firestore", firestore),
                    ("google.oauth2", oauth2), ("google.oauth2.service_account", service_account),
                    ("google.api_core", api_core), ("google.api_core.exceptions", exceptions)):
        monkeypatch.setitem(sys.modules, name, m)
    return clients, creds_calls
