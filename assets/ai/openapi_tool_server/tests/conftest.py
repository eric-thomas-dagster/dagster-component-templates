import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("openapi_tool_server_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SAMPLE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Sample Pet API", "version": "1.0"},
    "servers": [{"url": "https://api.example.com"}],
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "summary": "List all pets",
                "parameters": [{"name": "status", "in": "query", "required": False, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "ok"}},
            },
            "post": {
                "operationId": "createPet",
                "summary": "Create a pet",
                "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
                "responses": {"200": {"description": "ok"}},
            },
        },
        "/pets/{id}": {
            "get": {
                "operationId": "getPetById",
                "summary": "Get a pet by id",
                "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "ok"}},
            },
        },
    },
}

# Same shape as the real Swagger Petstore v3 spec (https://petstore3.swagger.io/api/v3/openapi.json)
# -- servers[].url is relative ('/api/v3'), resolved against the host that served the spec.
SAMPLE_SPEC_RELATIVE_SERVER = {
    "openapi": "3.0.0",
    "info": {"title": "Sample Pet API", "version": "1.0"},
    "servers": [{"url": "/api/v3"}],
    "paths": {
        "/pet/{id}": {
            "get": {
                "operationId": "getPetById",
                "summary": "Get a pet by id",
                "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "ok"}},
            },
        },
    },
}
