"""`_extract_operations` reads every HTTP method (not just GET, unlike openapi_ingestion's
own `_extract_endpoints`) and enforces the operation_ids allow-list by operationId."""
import pytest

from .conftest import SAMPLE_SPEC, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_extracts_every_method_when_no_allowlist(mod):
    ops = mod._extract_operations(SAMPLE_SPEC, None)
    ids = {o["operation_id"] for o in ops}
    assert ids == {"listPets", "createPet", "getPetById"}


def test_allowlist_filters_by_operation_id(mod):
    ops = mod._extract_operations(SAMPLE_SPEC, ["getPetById"])
    assert [o["operation_id"] for o in ops] == ["getPetById"]


def test_allowlist_excludes_everything_not_listed(mod):
    ops = mod._extract_operations(SAMPLE_SPEC, ["doesNotExist"])
    assert ops == []


def test_method_and_param_location_captured(mod):
    ops = {o["operation_id"]: o for o in mod._extract_operations(SAMPLE_SPEC, None)}
    assert ops["getPetById"]["method"] == "GET"
    assert ops["getPetById"]["parameters"] == [{"name": "id", "in": "path", "required": True}]
    assert ops["listPets"]["parameters"] == [{"name": "status", "in": "query", "required": False}]
    assert ops["createPet"]["method"] == "POST"
    assert ops["createPet"]["has_request_body"] is True
    assert ops["getPetById"]["has_request_body"] is False


@pytest.mark.parametrize("op_id,expected", [
    ("getPetById", "getPetById"),
    ("weird-id!!", "weird_id"),
    ("123abc", "op_123abc"),
])
def test_safe_asset_name(mod, op_id, expected):
    assert mod._safe_asset_name(op_id) == expected
