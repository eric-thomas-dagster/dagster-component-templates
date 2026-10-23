import copy
import logging

import dagster as dg
import pytest

from .conftest import SCHEMA, TOKEN, build_introspection, load_component_module

M = load_component_module()
ENTRY = {"metadata_key": "my_cost", "unit_type": "FLOAT", "display_name": "My cost", "description": "d"}


def problems(schema):
    return M.verify_operations_against_schema(build_introspection(schema))


def mutated(fn):
    s = copy.deepcopy(SCHEMA)
    fn(s)
    return s


def test_current_shapes_pass():
    assert problems(SCHEMA) == []
    assert M.verify_operations_against_schema({"data": build_introspection(SCHEMA)}) == []


def test_all_four_operations_are_covered():
    assert set(M.OPERATIONS) == {"ListCustomMetrics", "CreateCustomMetric", "UpdateCustomMetric", "DeleteCustomMetric"}


def test_renamed_field():
    def f(s):
        s["objects"]["CustomMetric"]["key"] = s["objects"]["CustomMetric"].pop("metadataKey")
    p = problems(mutated(f))
    assert any(x.startswith("ListCustomMetrics: query.customMetrics.metadataKey: field not on CustomMetric") and "'key'" in x for x in p)
    assert any(x.startswith("CreateCustomMetric: mutation.createCustomMetric.<CreateCustomMetricSuccess>.customMetric.metadataKey") for x in p)


def test_removed_error_field():
    p = problems(mutated(lambda s: s["objects"]["CustomMetricError"].pop("message")))
    assert any("CreateCustomMetric" in x and "<CustomMetricError>.message: field not on CustomMetricError" in x for x in p)
    p = problems(mutated(lambda s: s["objects"]["PythonError"].pop("stack")))
    assert any("<PythonError>.stack" in x for x in p) and not any("DeleteCustomMetric" in x and "stack" in x for x in p)


def test_missing_union_member():
    p = problems(mutated(lambda s: s["unions"]["UpdateCustomMetricResult"].remove("CustomMetricError")))
    assert len(p) == 1
    assert "UpdateCustomMetric" in p[0] and "'... on CustomMetricError' is not a member of union UpdateCustomMetricResult" in p[0]
    assert "UpdateCustomMetricSuccess" in p[0]  # shows what the server offers


def test_input_field_made_required():
    def f(s):
        s["inputs"]["CustomMetricUpdateInput"]["displayName"] = "String!"
        s["inputs"]["CustomMetricInput"]["displayName"] = "String!"
    p = problems(mutated(f))
    assert any("input CustomMetricInput.displayName: server requires String! but we do not always send it" in x for x in p)
    assert not any("CustomMetricUpdateInput.displayName" in x for x in p)  # we always send it there


def test_unknown_input_key():
    def f(s):
        s["inputs"]["CustomMetricInput"].pop("description")
    p = problems(mutated(f))
    assert p == ["input CustomMetricInput.description: key we send is not accepted; server offers ['displayName', 'metadataKey', 'unitType']"]


def test_enum_value_missing_and_argument_type_changes_and_root_removed():
    p = problems(mutated(lambda s: s["enums"]["CustomMetricUnitType"].remove("BYTES")))
    assert any("unitType: enum CustomMetricUnitType lacks ['BYTES']" in x for x in p)
    def f(s):
        s["objects"]["Mutation"]["deleteCustomMetric"] = ("DeleteCustomMetricResult!", {"customMetricId": "Int!"})
        s["objects"]["Query"].pop("customMetrics"); s["objects"]["Query"]["metrics"] = ("[CustomMetric!]!", {})
    p = problems(mutated(f))
    assert any("DeleteCustomMetric" in x and "declared as String! but the server argument is Int!" in x for x in p)
    assert any("ListCustomMetrics: query.customMetrics: field not on Query; server offers ['metrics']" in x for x in p)


def test_missing_required_argument_and_removed_type():
    def f(s):
        s["objects"]["Mutation"]["deleteCustomMetric"] = ("DeleteCustomMetricResult!", {"customMetricId": "String!", "reason": "String!"})
        del s["inputs"]["CustomMetricUpdateInput"]
        s["objects"]["Mutation"]["updateCustomMetric"] = ("UpdateCustomMetricResult!", {"customMetricId": "String!"})
    p = problems(mutated(f))
    assert any("required argument reason: String! is not provided" in x for x in p)
    assert any("variable $customMetricUpdateInput: type 'CustomMetricUpdateInput' does not exist" in x for x in p)
    assert any("input CustomMetricUpdateInput: input type does not exist" in x for x in p)


def test_validator_is_not_vacuous():
    """Mutation check: a validator that always passes must fail the drift tests above."""
    real = M.verify_operations_against_schema
    s = mutated(lambda s: s["objects"]["CustomMetric"].pop("metadataKey"))
    assert real(build_introspection(s))
    M.verify_operations_against_schema = lambda *a, **k: []
    try:
        assert M.verify_operations_against_schema(build_introspection(s)) == []  # what a broken validator would claim
        with pytest.raises(AssertionError):
            test_renamed_field()
    finally:
        M.verify_operations_against_schema = real


def test_fake_server_answers_introspection_and_check_schema(fake):
    c = M._Client(fake.url, TOKEN, sleep=lambda s: None)
    assert M.check_schema(c) == []
    fake.schema["objects"]["CustomMetric"].pop("unitType")
    assert any("unitType" in x for x in M.check_schema(c))
    fake.introspection_enabled = False
    assert M.check_schema(c) is None


# ---- component behaviour ----
def _comp(fake, monkeypatch, **kw):
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", TOKEN)
    a = dict(deployment_url=fake.url, metrics=[ENTRY], retry_backoff_seconds=0.0, dry_run=False)
    a.update(kw)
    return M.InsightsCustomMetricsSyncComponent(**a).build_defs(None)


def _md(r):
    return r.get_asset_materialization_events()[0].materialization.metadata


def test_default_on_and_clean_schema_syncs(fake, monkeypatch):
    assert M.InsightsCustomMetricsSyncComponent(deployment_url=fake.url, metrics=[ENTRY]).verify_schema == "warn"
    C = M.InsightsCustomMetricsSyncComponent
    assert C(deployment_url=fake.url, metrics=[ENTRY], verify_schema=True).verify_schema == "strict"
    assert C(deployment_url=fake.url, metrics=[ENTRY], verify_schema=False).verify_schema == "off"
    with pytest.raises(ValueError, match="verify_schema"):
        C(deployment_url=fake.url, metrics=[ENTRY], verify_schema="maybe").build_defs(None)
    r = dg.materialize(_comp(fake, monkeypatch).assets)
    assert _md(r)["schema_check"].value == "pass" and _md(r)["created"].value == 1
    assert [o[0] for o in fake.ops].count("IntrospectionQuery") == 1


def test_warn_default_reports_drift_but_still_syncs(fake, monkeypatch, capfd):
    fake.schema["objects"]["CustomMetric"]["key"] = fake.schema["objects"]["CustomMetric"].pop("metadataKey")
    r = dg.materialize(_comp(fake, monkeypatch).assets)
    md = _md(r)
    assert r.success and md["status"].value == "ok" and md["schema_check"].value == "drift" and md["created"].value == 1
    assert any("metadataKey" in x for x in md["schema_problems"].value) and "schema drift detected" in capfd.readouterr().err


def test_strict_drift_skips_all_writes_and_reports(fake, monkeypatch, caplog):
    fake.schema["objects"]["CustomMetric"]["key"] = fake.schema["objects"]["CustomMetric"].pop("metadataKey")
    r = dg.materialize(_comp(fake, monkeypatch, verify_schema="strict").assets)
    assert r.success and fake.mutations == [] and not any(o[0] == "ListCustomMetrics" for o in fake.ops)
    md = _md(r)
    assert md["status"].value == "schema_drift" and md["schema_check"].value == "drift"
    assert any("metadataKey" in x for x in md["schema_problems"].value)
    assert TOKEN not in repr({k: v.value for k, v in md.items()})
    assert not fake.metrics


def test_drift_with_on_error_fail_raises(fake, monkeypatch):
    fake.schema["unions"]["CreateCustomMetricResult"].remove("CustomMetricError")
    r = dg.materialize(_comp(fake, monkeypatch, on_error="fail", verify_schema="strict").assets, raise_on_error=False)
    assert not r.success and fake.mutations == []


def test_introspection_unavailable_does_not_block(fake, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    fake.introspection_enabled = False
    r = dg.materialize(_comp(fake, monkeypatch).assets)
    assert r.success and _md(r)["schema_check"].value == "unavailable" and _md(r)["created"].value == 1


def test_verify_schema_false_skips_introspection(fake, monkeypatch):
    fake.schema["objects"]["CustomMetric"].pop("unitType")
    r = dg.materialize(_comp(fake, monkeypatch, verify_schema="off").assets)
    assert _md(r)["schema_check"].value == "skipped" and _md(r)["created"].value == 1
    assert not any(o[0] == "IntrospectionQuery" for o in fake.ops)


# ---- CLI ----
def test_cli_verify_schema_pass_and_drift(fake, monkeypatch, capsys):
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", TOKEN)
    argv = ["--verify-schema", "--deployment-url", fake.url]
    assert M._cli(argv) == 0 and capsys.readouterr().out.startswith("PASS")
    fake.schema["objects"]["CustomMetric"].pop("displayName")
    assert M._cli(argv) == 1
    out = capsys.readouterr().out
    assert out.startswith("DRIFT") and "displayName" in out and TOKEN not in out
    fake.introspection_enabled = False
    assert M._cli(argv) == 2
    monkeypatch.delenv("DAGSTER_CLOUD_API_TOKEN")
    assert M._cli(argv) == 2


def test_graphql_core_cross_check_when_available():
    gc = pytest.importorskip("graphql")
    schema = gc.build_client_schema(build_introspection(SCHEMA))
    for name, text in M.OPERATIONS.items():
        assert gc.validate(schema, gc.parse(text)) == [], name
