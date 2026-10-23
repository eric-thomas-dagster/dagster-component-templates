"""Loads the real component.py by path (repo convention) and provides a local fake Dagster+ GraphQL server."""
import copy
import importlib.util
import json
import pathlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import ModuleType

import pytest

TOKEN = "tok-SECRET-1234567890"


def load_component_module() -> ModuleType:
    path = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("insights_custom_metrics_sync_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Declarative description of the (fake) Dagster+ schema, used BOTH to answer introspection in the fake server and by validator tests.
# Mirrors exactly the shapes cli/sync_custom_metrics selects. Type strings use GraphQL notation.
SCHEMA = {
    "query": "Query", "mutation": "Mutation",
    "scalars": ["String", "Boolean", "Int"],
    "enums": {"CustomMetricUnitType": ["INTEGER", "FLOAT", "TIME_MS", "TIME_SECONDS", "BYTES"]},
    "inputs": {
        "CustomMetricInput": {"metadataKey": "String!", "displayName": "String", "description": "String", "unitType": "CustomMetricUnitType"},
        "CustomMetricUpdateInput": {"displayName": "String", "description": "String", "unitType": "CustomMetricUnitType"},
    },
    "objects": {
        "Query": {"customMetrics": ("[CustomMetric!]!", {})},
        "Mutation": {
            "createCustomMetric": ("CreateCustomMetricResult!", {"customMetricInput": "CustomMetricInput!"}),
            "updateCustomMetric": ("UpdateCustomMetricResult!", {"customMetricId": "String!", "customMetricUpdateInput": "CustomMetricUpdateInput!"}),
            "deleteCustomMetric": ("DeleteCustomMetricResult!", {"customMetricId": "String!"}),
        },
        "CustomMetric": {"id": ("String!", {}), "metadataKey": ("String!", {}), "displayName": ("String", {}),
                         "description": ("String", {}), "unitType": ("CustomMetricUnitType", {})},
        "CreateCustomMetricSuccess": {"customMetric": ("CustomMetric!", {})},
        "UpdateCustomMetricSuccess": {"customMetric": ("CustomMetric!", {})},
        "DeleteCustomMetricSuccess": {"customMetricId": ("String!", {})},
        "CustomMetricError": {"message": ("String!", {})},
        "PythonError": {"message": ("String!", {}), "stack": ("[String!]!", {})},
        "UnauthorizedError": {"message": ("String!", {})},
    },
    "unions": {
        "CreateCustomMetricResult": ["CreateCustomMetricSuccess", "CustomMetricError", "PythonError", "UnauthorizedError"],
        "UpdateCustomMetricResult": ["UpdateCustomMetricSuccess", "CustomMetricError", "PythonError", "UnauthorizedError"],
        "DeleteCustomMetricResult": ["DeleteCustomMetricSuccess", "CustomMetricError", "PythonError", "UnauthorizedError"],
    },
}


def _ref(s):
    if s.endswith("!"):
        return {"kind": "NON_NULL", "name": None, "ofType": _ref(s[:-1])}
    if s.startswith("["):
        return {"kind": "LIST", "name": None, "ofType": _ref(s[1:-1])}
    return {"kind": "NAMED", "name": s, "ofType": None}


def build_introspection(schema):
    """Real-shaped introspection `data` ({"__schema": ...}) from the declarative SCHEMA."""
    kinds = {n: "SCALAR" for n in schema["scalars"]}
    kinds.update({n: "ENUM" for n in schema["enums"]})
    kinds.update({n: "INPUT_OBJECT" for n in schema["inputs"]})
    kinds.update({n: "OBJECT" for n in schema["objects"]})
    kinds.update({n: "UNION" for n in schema["unions"]})

    def ref(s):  # resolve NAMED to the real kind
        r = _ref(s)
        leaf = r
        while leaf["ofType"]:
            leaf = leaf["ofType"]
        leaf["kind"] = kinds[leaf["name"]]
        return r

    def iv(name, t):
        return {"name": name, "type": ref(t), "defaultValue": None}

    types = [{"kind": "SCALAR", "name": n, "fields": None, "inputFields": None, "interfaces": None, "enumValues": None, "possibleTypes": None} for n in schema["scalars"]]
    types += [{"kind": "ENUM", "name": n, "fields": None, "inputFields": None, "interfaces": None, "possibleTypes": None,
               "enumValues": [{"name": v} for v in vals]} for n, vals in schema["enums"].items()]
    types += [{"kind": "INPUT_OBJECT", "name": n, "fields": None, "interfaces": None, "enumValues": None, "possibleTypes": None,
               "inputFields": [iv(k, t) for k, t in f.items()]} for n, f in schema["inputs"].items()]
    types += [{"kind": "OBJECT", "name": n, "inputFields": None, "interfaces": [], "enumValues": None, "possibleTypes": None,
               "fields": [{"name": fn, "args": [iv(a, at) for a, at in args.items()], "type": ref(ft)} for fn, (ft, args) in f.items()]}
              for n, f in schema["objects"].items()]
    types += [{"kind": "UNION", "name": n, "fields": None, "inputFields": None, "interfaces": None, "enumValues": None,
               "possibleTypes": [ref(m) for m in members]} for n, members in schema["unions"].items()]
    return {"__schema": {"queryType": {"name": schema["query"]}, "mutationType": {"name": schema["mutation"]}, "types": types}}


class FakeDagsterPlus:
    """Mirrors the response shapes in cli/sync_custom_metrics (ListCustomMetrics / Create / Update / Delete)."""

    def __init__(self):
        self.metrics = {}  # id -> dict(id, metadataKey, displayName, description, unitType)
        self.ops = []  # (op_name, variables)
        self.queue = []  # scripted (status, headers, body) responses consumed before normal handling
        self.token = TOKEN
        self.error_for_key = {}  # metadataKey -> message => CustomMetricError on create
        self._n = 0
        self.schema = copy.deepcopy(SCHEMA)
        self.introspection_enabled = True

    def seed(self, key, unit="INTEGER", display=None, description=None):
        self._n += 1
        mid = f"m{self._n}"
        self.metrics[mid] = {"id": mid, "metadataKey": key, "displayName": display, "description": description, "unitType": unit}
        return mid

    @property
    def mutations(self):
        return [o for o in self.ops if o[0] not in ("ListCustomMetrics", "IntrospectionQuery")]

    def handle(self, query, variables):
        if "__schema" in query:
            self.ops.append(("IntrospectionQuery", variables))
            if not self.introspection_enabled:
                raise PermissionError("introspection disabled")
            return build_introspection(self.schema)
        if "query ListCustomMetrics" in query:
            self.ops.append(("ListCustomMetrics", variables))
            return {"customMetrics": list(self.metrics.values())}
        if "mutation CreateCustomMetric" in query:
            self.ops.append(("CreateCustomMetric", variables))
            inp = variables["customMetricInput"]
            if inp["metadataKey"] in self.error_for_key:
                return {"createCustomMetric": {"__typename": "CustomMetricError", "message": self.error_for_key[inp["metadataKey"]]}}
            if any(m["metadataKey"] == inp["metadataKey"] for m in self.metrics.values()):
                return {"createCustomMetric": {"__typename": "CustomMetricError", "message": "already exists"}}
            self._n += 1
            mid = f"m{self._n}"
            self.metrics[mid] = {"id": mid, "metadataKey": inp["metadataKey"], "displayName": inp.get("displayName"),
                                 "description": inp.get("description"), "unitType": inp.get("unitType")}
            return {"createCustomMetric": {"__typename": "CreateCustomMetricSuccess", "customMetric": {"id": mid, "metadataKey": inp["metadataKey"]}}}
        if "mutation UpdateCustomMetric" in query:
            self.ops.append(("UpdateCustomMetric", variables))
            mid, inp = variables["customMetricId"], variables["customMetricUpdateInput"]
            if mid not in self.metrics:
                return {"updateCustomMetric": {"__typename": "CustomMetricError", "message": "not found"}}
            self.metrics[mid].update(inp)
            return {"updateCustomMetric": {"__typename": "UpdateCustomMetricSuccess", "customMetric": {"id": mid, "metadataKey": self.metrics[mid]["metadataKey"]}}}
        if "mutation DeleteCustomMetric" in query:
            self.ops.append(("DeleteCustomMetric", variables))
            mid = variables["customMetricId"]
            self.metrics.pop(mid, None)
            return {"deleteCustomMetric": {"__typename": "DeleteCustomMetricSuccess", "customMetricId": mid}}
        raise AssertionError(f"unexpected query {query[:60]}")


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def fake():
    state = FakeDagsterPlus()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path != "/prod/graphql":
                self.send_response(404); self.end_headers(); return
            if self.headers.get("Dagster-Cloud-Api-Token") != state.token:
                self.send_response(401); self.end_headers(); self.wfile.write(b'{"error":"bad token"}'); return
            if state.queue:
                status, headers, payload = state.queue.pop(0)
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(payload.encode())
                return
            try:
                out = json.dumps({"data": state.handle(body["query"], body.get("variables") or {})}).encode()
            except PermissionError as e:
                out = json.dumps({"errors": [{"message": str(e)}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    state.url = f"http://127.0.0.1:{srv.server_address[1]}/prod"
    yield state
    srv.shutdown()
    srv.server_close()
