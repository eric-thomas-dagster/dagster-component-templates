import logging

import dagster as dg
import pytest

from .conftest import TOKEN, load_component_module

M = load_component_module()
ENTRY = {"metadata_key": "my_cost", "unit_type": "FLOAT", "display_name": "My cost", "description": "d"}


def client(fake, token=TOKEN, sleeps=None, **kw):
    sleeps = sleeps if sleeps is not None else []
    return M._Client(fake.url, token, sleep=sleeps.append, backoff_seconds=1.0, **kw)


def desired(*entries):
    return M.resolve_metrics([], list(entries))


def test_create_new(fake):
    r = M.sync_metrics(client(fake), desired(ENTRY), dry_run=False)
    assert r["created"] == ["my_cost"]
    m = list(fake.metrics.values())[0]
    assert (m["metadataKey"], m["unitType"], m["displayName"]) == ("my_cost", "FLOAT", "My cost")
    create = [o for o in fake.ops if o[0] == "CreateCustomMetric"][0][1]
    assert create == {"customMetricInput": {"metadataKey": "my_cost", "unitType": "FLOAT", "displayName": "My cost", "description": "d"}}


def test_noop_when_unchanged_zero_mutations(fake):
    fake.seed("my_cost", "FLOAT", "My cost", "d")
    r = M.sync_metrics(client(fake), desired(ENTRY), dry_run=False)
    assert r["unchanged"] == ["my_cost"] and not r["created"] and not r["updated"]
    assert fake.mutations == []


def test_unspecified_optionals_are_not_a_diff_and_not_nulled(fake):
    mid = fake.seed("my_cost", "FLOAT", "Hand-written name", "hand description")
    bare = {"metadata_key": "my_cost", "unit_type": "FLOAT"}
    M.sync_metrics(client(fake), desired(bare), dry_run=False)
    assert fake.mutations == []
    # a unit change updates, but keeps the server's name/description rather than sending null
    M.sync_metrics(client(fake), desired({**bare, "unit_type": "INTEGER"}), dry_run=False)
    assert fake.metrics[mid]["unitType"] == "INTEGER"
    assert fake.metrics[mid]["displayName"] == "Hand-written name" and fake.metrics[mid]["description"] == "hand description"


def test_update_when_changed(fake):
    mid = fake.seed("my_cost", "INTEGER", "Old", "d")
    r = M.sync_metrics(client(fake), desired(ENTRY), dry_run=False)
    assert r["updated"] == ["my_cost"]
    assert fake.metrics[mid]["unitType"] == "FLOAT" and fake.metrics[mid]["displayName"] == "My cost"
    upd = [o for o in fake.ops if o[0] == "UpdateCustomMetric"][0][1]
    assert upd["customMetricId"] == mid
    assert set(upd["customMetricUpdateInput"]) == {"displayName", "description", "unitType"}


def test_prune_only_prefixed_and_never_ui_created(fake):
    fake.seed("my_cost", "FLOAT", "My cost", "d")
    fake.seed("agentic_old_metric")
    fake.seed("ui_created_by_hand")
    r = M.sync_metrics(client(fake), desired(ENTRY), dry_run=False, prune=True, prune_prefixes=["agentic_"])
    assert r["deleted"] == ["agentic_old_metric"]
    assert sorted(m["metadataKey"] for m in fake.metrics.values()) == ["my_cost", "ui_created_by_hand"]


def test_prune_without_safelist_deletes_nothing(fake):
    fake.seed("stray")
    M.sync_metrics(client(fake), desired(ENTRY), dry_run=False, prune=True, prune_prefixes=[])
    assert not [o for o in fake.ops if o[0] == "DeleteCustomMetric"]


def test_prune_never_deletes_desired_keys(fake):
    fake.seed("agentic_cost_usd", "FLOAT")
    M.sync_metrics(client(fake), desired({"metadata_key": "agentic_cost_usd", "unit_type": "FLOAT"}), dry_run=False,
                   prune=True, prune_prefixes=["agentic_"])
    assert not [o for o in fake.ops if o[0] == "DeleteCustomMetric"]


def test_dry_run_zero_mutations_reports_plan(fake):
    fake.seed("agentic_stale")
    fake.seed("changed", "INTEGER", "x", "y")
    r = M.sync_metrics(client(fake), desired(ENTRY, {"metadata_key": "changed", "unit_type": "FLOAT"}), dry_run=True,
                       prune=True, prune_prefixes=["agentic_"])
    assert fake.mutations == []
    assert r["planned"] == {"create": ["my_cost"], "update": ["changed"], "delete": ["agentic_stale"]}


def test_custom_metric_error_clear_message_and_others_continue(fake):
    fake.error_for_key["bad_one"] = "unit not allowed for this key"
    r = M.sync_metrics(client(fake), desired({"metadata_key": "bad_one", "unit_type": "BYTES"}, ENTRY), dry_run=False)
    assert r["created"] == ["my_cost"]
    assert len(r["errors"]) == 1 and "bad_one" in r["errors"][0] and "unit not allowed" in r["errors"][0] and "CustomMetricError" in r["errors"][0]


def test_401_clear_error_without_token(fake, caplog):
    with pytest.raises(M.InsightsSyncError) as ei:
        M.sync_metrics(client(fake, token="WRONG-TOKEN-VALUE"), desired(ENTRY), dry_run=False)
    msg = str(ei.value)
    assert "401" in msg and "DAGSTER_CLOUD_API_TOKEN" in msg and "WRONG-TOKEN-VALUE" not in msg
    assert fake.ops == []


def test_retry_429_with_retry_after_then_success(fake):
    fake.queue = [(429, {"Retry-After": "3"}, "slow down")]
    sleeps = []
    r = M.sync_metrics(client(fake, sleeps=sleeps), desired(ENTRY), dry_run=False)
    assert r["created"] == ["my_cost"] and sleeps == [3.0]


def test_retry_503_backoff_jitter_then_success(fake):
    fake.queue = [(503, {}, "x"), (503, {}, "x")]
    sleeps = []
    r = M.sync_metrics(client(fake, sleeps=sleeps), desired(ENTRY), dry_run=False)
    assert r["created"] == ["my_cost"] and len(sleeps) == 2
    assert 0.5 <= sleeps[0] <= 1.0 and 1.0 <= sleeps[1] <= 2.0  # exponential with jitter


def test_retries_exhausted(fake):
    fake.queue = [(503, {}, "x")] * 10
    sleeps = []
    with pytest.raises(M.InsightsSyncError, match="after 3 attempts"):
        M.sync_metrics(client(fake, sleeps=sleeps, max_retries=2), desired(ENTRY), dry_run=False)
    assert len(sleeps) == 2


def test_400_not_retried(fake):
    fake.queue = [(400, {}, "bad request")]
    sleeps = []
    with pytest.raises(M.InsightsSyncError, match="HTTP 400"):
        M.sync_metrics(client(fake, sleeps=sleeps), desired(ENTRY), dry_run=False)
    assert sleeps == []


def test_graphql_errors_payload(fake):
    fake.queue = [(200, {}, '{"errors":[{"message":"Cannot query field customMetrics"}]}')]
    with pytest.raises(M.InsightsSyncError, match="GraphQL errors"):
        M.sync_metrics(client(fake), desired(ENTRY), dry_run=False)


@pytest.mark.parametrize("bad", [
    {"metadata_key": "k", "unit_type": "PERCENT"}, {"metadata_key": "k"}, {"metadata_key": "has space", "unit_type": "INTEGER"},
    {"metadata_key": "", "unit_type": "INTEGER"}, {"metadata_key": "k", "unit_type": "integer"}, {"metadata_key": "k", "unit_type": "INTEGER", "x": 1},
])
def test_invalid_entries_rejected(bad):
    with pytest.raises(ValueError):
        M.validate_metric(bad)


def test_unknown_preset_and_url_validation():
    with pytest.raises(ValueError, match="unknown preset"):
        M.resolve_metrics(["nope"], [])
    for u in ("http://acme.dagster.cloud/prod", "acme.dagster.cloud", ""):
        with pytest.raises(ValueError):
            M.validate_deployment_url(u)
    assert M.validate_deployment_url("https://acme.dagster.cloud/prod/") == "https://acme.dagster.cloud/prod"


def test_explicit_overrides_preset():
    out = M.resolve_metrics(["agentic_pipeline"], [{"metadata_key": "agentic_cost_usd", "unit_type": "FLOAT", "display_name": "Mine"}])
    assert [m["display_name"] for m in out if m["metadata_key"] == "agentic_cost_usd"] == ["Mine"]


def test_manifest_cli_output(capsys):
    assert M._cli(["--manifest-only", "--preset", "agentic_pipeline"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("metrics:\n") and "metadata_key: agentic_latency_ms" in out and "unit_type: TIME_MS" in out
    assert M._cli(["--manifest-only", "--preset", "zzz"]) == 2


# ---- the real component, materialized ----
def _defs(fake, monkeypatch, **kw):
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", TOKEN)
    attrs = dict(deployment_url=fake.url, metrics=[ENTRY], retry_backoff_seconds=0.0, dry_run=False)
    attrs.update(kw)
    return M.InsightsCustomMetricsSyncComponent(**attrs).build_defs(None)


def _run(defs):
    return dg.materialize(defs.assets, raise_on_error=False)


def _md(result):
    return result.get_asset_materialization_events()[0].materialization.metadata


def test_materialize_real_asset_and_idempotent(fake, monkeypatch, caplog, capfd):
    caplog.set_level(logging.DEBUG)
    defs = _defs(fake, monkeypatch)
    r = _run(defs)
    assert r.success and _md(r)["created"].value == 1 and _md(r)["status"].value == "ok"
    r2 = _run(defs)
    assert r2.success and _md(r2)["unchanged"].value == 1 and len(fake.mutations) == 1
    assert TOKEN not in caplog.text + capfd.readouterr().err  # dagster's run logger writes to stderr
    assert TOKEN not in repr({k: v.value for k, v in _md(r2).items()})


def test_materialize_dry_run_default_is_true(fake, monkeypatch):
    defs = _defs(fake, monkeypatch, dry_run=True)
    r = _run(defs)
    assert r.success and fake.mutations == [] and _md(r)["planned"].value["create"] == ["my_cost"]
    assert M.InsightsCustomMetricsSyncComponent(deployment_url=fake.url, metrics=[ENTRY]).dry_run is True


def test_on_error_warn_vs_fail_and_token_never_leaks(fake, monkeypatch, caplog, capfd):
    caplog.set_level(logging.DEBUG)
    fake.token = "different-token"
    r = _run(_defs(fake, monkeypatch))
    assert r.success and _md(r)["status"].value == "failed"
    errs = _md(r)["errors"].value
    assert "401" in errs[0] and TOKEN not in repr(errs) and TOKEN not in caplog.text
    r = _run(_defs(fake, monkeypatch, on_error="fail"))
    assert not r.success
    assert TOKEN not in caplog.text + capfd.readouterr().err


def test_missing_token_env(fake, monkeypatch):
    defs = _defs(fake, monkeypatch, token_env_var="NOPE_NOT_SET")
    r = _run(defs)
    assert r.success and "NOPE_NOT_SET" in _md(r)["errors"].value[0]


def test_build_defs_validation(fake):
    C = M.InsightsCustomMetricsSyncComponent
    with pytest.raises(ValueError, match="prune_prefixes"):
        C(deployment_url=fake.url, metrics=[ENTRY], prune=True).build_defs(None)
    with pytest.raises(ValueError, match="unit_type"):
        C(deployment_url=fake.url, metrics=[{"metadata_key": "a", "unit_type": "NOPE"}]).build_defs(None)
    with pytest.raises(ValueError, match="nothing to sync"):
        C(deployment_url=fake.url).build_defs(None)
    with pytest.raises(ValueError, match="on_error"):
        C(deployment_url=fake.url, metrics=[ENTRY], on_error="x").build_defs(None)
    with pytest.raises(ValueError, match="deployment_url"):
        C(deployment_url="http://evil.example/prod", metrics=[ENTRY]).build_defs(None)


def test_schedule_optional(fake):
    d = M.InsightsCustomMetricsSyncComponent(deployment_url=fake.url, metrics=[ENTRY], cron_schedule="0 * * * *").build_defs(None)
    assert [s.name for s in d.schedules] == ["insights_custom_metrics_sync_schedule"]


# ---- presets: auto ----
def _tagged(name, preset):
    @dg.asset(name=name, tags={"insights/metrics": preset} if preset else {})
    def _a():
        return 1
    return _a


def _auto_defs(fake, monkeypatch, tagged, **kw):
    d = _defs(fake, monkeypatch, presets=["auto"], metrics=kw.pop("metrics", []), **kw)
    return d, [*tagged, *d.assets]


def _keys():
    return lambda fake: sorted(m["metadataKey"] for m in fake.metrics.values())


def test_auto_only_tagged_presets_upserted(fake, monkeypatch):
    d, assets = _auto_defs(fake, monkeypatch, [_tagged("x", "receipt_outcome_ingestion"), _tagged("y", "agentic_pipeline"), _tagged("z", None)])
    r = dg.materialize(assets)
    assert r.success
    md = _md(r)
    assert md["auto_discovered"].value == ["agentic_pipeline", "receipt_outcome_ingestion"]
    assert md["auto_scope"].value == "job"  # in-process execution has no repository_def
    want = {m["metadata_key"] for p in ("agentic_pipeline", "receipt_outcome_ingestion") for m in M.PRESETS[p]}
    assert set(_keys()(fake)) == want and md["created"].value == len(want)
    assert "total_tokens" not in want  # rag_pipeline preset was not tagged


def test_auto_none_tagged_is_clear_noop(fake, monkeypatch):
    d, assets = _auto_defs(fake, monkeypatch, [_tagged("z", None)])
    r = dg.materialize(assets)
    assert r.success and fake.ops == []
    assert _md(r)["auto_discovered"].value == [] and "no assets tagged 'insights/metrics'" in _md(r)["message"].value


def test_auto_combined_with_explicit_metrics_and_unknown_tag(fake, monkeypatch):
    d, assets = _auto_defs(fake, monkeypatch, [_tagged("x", "rag_pipeline"), _tagged("w", "nonsense")], metrics=[ENTRY])
    r = dg.materialize(assets)
    keys = set(_keys()(fake))
    assert "my_cost" in keys and "total_cost_usd" in keys
    assert _md(r)["auto_unknown_tag_values"].value == ["nonsense"]


def test_auto_dry_run_default_still_applies(fake, monkeypatch):
    d, assets = _auto_defs(fake, monkeypatch, [_tagged("x", "agentic_pipeline")], dry_run=True)
    r = dg.materialize(assets)
    assert r.success and fake.mutations == []
    assert len(_md(r)["planned"].value["create"]) == len(M.PRESETS["agentic_pipeline"])


def test_presets_string_auto_accepted(fake):
    c = M.InsightsCustomMetricsSyncComponent(deployment_url=fake.url, presets="auto")
    assert c.presets == ["auto"]
    c.build_defs(None)


def test_auto_full_repository_scope_in_a_code_location_style_run(fake, monkeypatch, tmp_path):
    """A reconstructable (code-location-style) run: context.repository_def is set, so the WHOLE location is scanned even though the
    run only selects the sync asset."""
    import tempfile
    import pathlib
    mod_path = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    f = tmp_path / "loc.py"
    f.write_text(f'''
import importlib.util, dagster as dg
spec = importlib.util.spec_from_file_location("ics_loc", {str(mod_path)!r}); M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)
@dg.asset(tags={{"insights/metrics": "receipt_outcome_ingestion"}})
def tagged_a(): ...
@dg.asset(tags={{"insights/metrics": "agentic_pipeline"}})
def tagged_b(): ...
sync = M.InsightsCustomMetricsSyncComponent(deployment_url={fake.url!r}, presets=["auto"], dry_run=False).build_defs(None)
defs = dg.Definitions(assets=[tagged_a, tagged_b, *sync.assets], executor=dg.in_process_executor)
''')
    from dagster._core.definitions.reconstruct import ReconstructableRepository
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", TOKEN)
    inst = dg.DagsterInstance.local_temp(tempfile.mkdtemp())
    job = ReconstructableRepository.for_file(str(f), "defs", working_directory=str(tmp_path)).get_reconstructable_job("__ASSET_JOB")
    res = dg.execute_job(job, inst, asset_selection=[dg.AssetKey("insights_custom_metrics_sync")])
    assert res.success
    md = res.get_asset_materialization_events()[0].materialization.metadata
    assert md["auto_scope"].value == "repository"
    assert md["auto_discovered"].value == ["agentic_pipeline", "receipt_outcome_ingestion"]
    assert "agentic_cost_usd" in _keys()(fake) and "outcomes_recorded" in _keys()(fake)
