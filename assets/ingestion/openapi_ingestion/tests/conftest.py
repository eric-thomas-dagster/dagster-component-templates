"""Shared test helpers for OpenAPIIngestionComponent.

Mocks ONLY requests.get -- everything this component actually owns
(endpoint grouping, pagination, real SQL write, partition filtering) is
exercised for real through dg.materialize() + a real temp-file SQLite
database.
"""
import importlib.util
import pathlib
from types import ModuleType
from unittest.mock import MagicMock


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "openapi_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_grouped(endpoint_path="/pets"):
    return {
        "pets": [{
            "path": endpoint_path,
            "full_path": endpoint_path,
            "operation_id": "listPets",
            "summary": "",
            "response_array_key": None,
            "has_page_param": False,
            "has_offset_param": False,
            "has_cursor_param": False,
            "params": [],
        }],
    }


class FakeOpenAPIServer:
    def __init__(self, records: list):
        self.records = records
        self.calls: list = []

    def handle_get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        resp.json.return_value = self.records
        return resp
