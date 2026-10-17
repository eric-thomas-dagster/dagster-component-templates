"""Infrastructure components (Terraform, Pulumi, Helm, Ansible, CDK, CloudFormation, Ab Initio, Matillion, Modal) have no
rows to filter, so a partition means: the run is parameterized by it. {partition_key}, {partition_date}, {partition_start}
and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted into the component's variables and
identity fields, and subprocess tools also get DAGSTER_PARTITION_* environment variables.

Each test runs the component's real code path against a recording fake of the external tool/API and asserts what it was
actually called with."""
import importlib.util
import subprocess
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "assets/infrastructure"
KEY = "2026-01-03"
PARTITION = dict(partition_type="daily", partition_start="2026-01-01")


def _load(name, extra):
    folder = INFRA / name
    spec = importlib.util.spec_from_file_location(f"infra_{name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    defs = cls(**{**doc["attributes"], **extra}).build_defs(None)
    return mod, next(a for a in defs.assets if hasattr(a, "node_def"))


def _materialize(asset, partitioned=True):
    kwargs = {"partition_key": KEY} if partitioned else {}
    return dg.materialize([asset], raise_on_error=False, **kwargs)


class _Recorder:
    """Stands in for subprocess.run: records every command with its environment, always succeeds."""
    def __init__(self):
        self.calls = []

    def __call__(self, cmd, *a, **kw):
        self.calls.append((cmd if isinstance(cmd, str) else list(cmd), kw.get("env") or {}))
        return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")


def _flat(recorder):
    return [" ".join(map(str, cmd)) for cmd, _ in recorder.calls]


def _envs(recorder):
    return [env for _, env in recorder.calls]


# ── subprocess tools ────────────────────────────────────────────────────────

def test_terraform_substitutes_the_partition_into_workspace_and_vars(monkeypatch):
    mod, asset = _load("terraform_asset", {**PARTITION, "workspace": "tenant-{partition_key}",
                                           "vars": {"run_date": "{partition_date}", "label": "plain {not_a_token}"},
                                           "env_vars": {"AWS_PROFILE": "x"}})
    rec = _Recorder()
    monkeypatch.setattr(mod.subprocess, "run", rec)
    assert _materialize(asset).success
    text = "\n".join(_flat(rec))
    assert "workspace select tenant-2026-01-03" in text
    assert "run_date=2026-01-03" in text and "label=plain {not_a_token}" in text    # only partition tokens are replaced
    env = _envs(rec)[0]
    assert env["DAGSTER_PARTITION_KEY"] == KEY and env["DAGSTER_PARTITION_DATE"] == KEY and env["AWS_PROFILE"] == "x"
    assert env["DAGSTER_PARTITION_START"] == "2026-01-03T00:00:00" and env["DAGSTER_PARTITION_END"] == "2026-01-04T00:00:00"


def test_terraform_unpartitioned_run_is_untouched(monkeypatch):
    mod, asset = _load("terraform_asset", {"workspace": "tenant-{partition_key}", "vars": {"run_date": "{partition_date}"}})
    rec = _Recorder()
    monkeypatch.setattr(mod.subprocess, "run", rec)
    assert asset.partitions_def is None and _materialize(asset, partitioned=False).success
    assert "workspace select tenant-{partition_key}" in "\n".join(_flat(rec))                # literal: no partition, no substitution
    assert all("DAGSTER_PARTITION_KEY" not in env for env in _envs(rec))


def test_pulumi_substitutes_stack_and_config(monkeypatch):
    mod, asset = _load("pulumi_asset", {**PARTITION, "stack_name": "acme/{partition_key}",
                                        "config": {"window": "{partition_start}/{partition_end}"}})
    rec = _Recorder()
    monkeypatch.setattr(mod.subprocess, "run", rec)
    _materialize(asset)
    text = "\n".join(_flat(rec))
    assert "--stack acme/2026-01-03" in text and "window 2026-01-03T00:00:00/2026-01-04T00:00:00" in text.replace("=", " ")
    assert _envs(rec)[0]["DAGSTER_PARTITION_KEY"] == KEY


def test_helm_substitutes_release_namespace_and_values(monkeypatch):
    mod, asset = _load("helm_deploy", {**PARTITION, "release_name": "app-{partition_key}", "namespace": "ns-{partition_key}",
                                       "set_values": {"image.tag": "{partition_date}"}, "set_string_values": {"id": "{partition_key}"}})
    rec = _Recorder()
    monkeypatch.setattr(mod.subprocess, "run", rec)
    _materialize(asset)
    text = "\n".join(_flat(rec))
    assert "app-2026-01-03" in text and "--namespace ns-2026-01-03" in text
    assert "image.tag=2026-01-03" in text and "id=2026-01-03" in text
    assert _envs(rec)[0]["DAGSTER_PARTITION_KEY"] == KEY


class _FakePopen:
    """Stands in for subprocess.Popen: streams a couple of lines, exits 0, records command and environment."""
    calls = []

    def __init__(self, cmd, *a, **kw):
        type(self).calls.append((list(cmd), kw.get("env") or {}))
        self.stdout = types.SimpleNamespace(readline=iter(["ok\n", ""]).__next__)
        self.returncode = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def communicate(self):
        return "", ""

    def wait(self):
        return 0


def _popen(monkeypatch, mod):
    _FakePopen.calls = []
    monkeypatch.setattr(mod.subprocess, "Popen", _FakePopen)
    return _FakePopen


def test_ansible_substitutes_extra_vars_and_exports_the_partition(monkeypatch):
    mod, asset = _load("ansible_asset", {**PARTITION, "extra_vars": {"run_date": "{partition_date}", "env": "prod"}})
    fake = _popen(monkeypatch, mod)
    _materialize(asset)
    cmd, env = fake.calls[0]
    assert "run_date=2026-01-03 env=prod" in " ".join(cmd) and env["DAGSTER_PARTITION_KEY"] == KEY


def test_aws_cdk_substitutes_context_and_parameters(monkeypatch):
    mod, asset = _load("aws_cdk_asset", {**PARTITION, "context": {"day": "{partition_date}"}, "parameters": {"Window": "{partition_start}"}})
    fake = _popen(monkeypatch, mod)
    _materialize(asset)
    cmd, env = fake.calls[0]
    joined = " ".join(cmd)
    assert "-c day=2026-01-03" in joined and "--parameters Window=2026-01-03T00:00:00" in joined
    assert env["DAGSTER_PARTITION_START"] == "2026-01-03T00:00:00"


# ── cloud APIs ──────────────────────────────────────────────────────────────

def test_cloudformation_substitutes_stack_name_and_parameters(monkeypatch, tmp_path):
    import boto3
    template = tmp_path / "stack.yaml"
    template.write_text("Resources: {}\n")
    created = {}

    class FakeCF:
        def __init__(self):
            self.exists = False

        def describe_stacks(self, StackName):
            if not self.exists:
                raise Exception("Stack does not exist")
            return {"Stacks": [{"StackStatus": "CREATE_COMPLETE", "StackStatusReason": "", "Outputs": []}]}

        def create_stack(self, **kwargs):
            created.update(kwargs)
            self.exists = True

        def __getattr__(self, name):  # any other call a flow may make
            return lambda *a, **k: {}

    cf = FakeCF()
    monkeypatch.setattr(boto3, "Session", lambda **kw: types.SimpleNamespace(client=lambda *a, **k: cf))
    mod, asset = _load("cloudformation_asset", {**PARTITION, "stack_name": "lake-{partition_key}", "template_file": str(template),
                                                "parameters": {"RunDate": "{partition_date}", "Env": "prod"}})
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=False)
    assert _materialize(asset).success
    assert created["StackName"] == "lake-2026-01-03"
    assert {"ParameterKey": "RunDate", "ParameterValue": "2026-01-03"} in created["Parameters"]
    assert {"ParameterKey": "Env", "ParameterValue": "prod"} in created["Parameters"]


def _requests(monkeypatch, status):
    import requests
    posted = []

    def post(url, **kw):
        posted.append((url, kw.get("json")))
        return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"runId": "r1", "id": "r1"})

    def get(url, **kw):
        return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: status)

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(requests, "get", get)
    return posted


def test_abinitio_substitutes_job_parameters(monkeypatch):
    posted = _requests(monkeypatch, {"status": "ok"})
    monkeypatch.setenv("ABINITIO_EME_URL", "https://eme.example")
    mod, asset = _load("abinitio_run_asset", {**PARTITION, "poll_interval_seconds": 0, "timeout_seconds": 5,
                                              "parameters": {"AI_START_DATE": "{partition_start}", "AI_DAY": "{partition_date}", "STATIC": "x"}})
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=False)
    _materialize(asset)
    assert posted[0][1] == {"AI_START_DATE": "2026-01-03T00:00:00", "AI_DAY": "2026-01-03", "STATIC": "x"}


def test_matillion_substitutes_job_variables(monkeypatch):
    posted = _requests(monkeypatch, {"state": "SUCCESS"})
    monkeypatch.setenv("MATILLION_INSTANCE_URL", "https://matillion.example")
    mod, asset = _load("matillion_run_asset", {**PARTITION, "poll_interval_seconds": 0, "timeout_seconds": 5,
                                               "variables": {"load_date": "{partition_date}"}})
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=False)
    _materialize(asset)
    assert posted[0][1] == {"variables": {"load_date": "2026-01-03"}}


def test_modal_substitutes_call_arguments_on_the_sdk_path(monkeypatch):
    calls = {}

    class Fn:
        def remote(self, *a, **k):
            calls["args"], calls["kwargs"] = a, k
            return "done"

    modal = types.ModuleType("modal")
    modal.Function = types.SimpleNamespace(lookup=lambda *a, **k: Fn())
    modal.config = types.SimpleNamespace(_override_config=lambda **k: None)
    monkeypatch.setitem(sys.modules, "modal", modal)
    mod, asset = _load("modal_asset", {**PARTITION, "args": ["{partition_key}"], "kwargs": {"day": "{partition_date}", "n": 5}})
    assert _materialize(asset).success
    assert calls["args"] == (KEY,) and calls["kwargs"] == {"day": "2026-01-03", "n": 5}


def test_modal_substitutes_on_the_cli_fallback_path(monkeypatch):
    monkeypatch.setitem(sys.modules, "modal", None)  # makes `import modal` raise ImportError -> CLI path
    mod, asset = _load("modal_asset", {**PARTITION, "kwargs": {"day": "{partition_date}"}, "app_file": "app.py"})
    fake = _popen(monkeypatch, mod)
    _materialize(asset)
    cmd, env = fake.calls[0]
    assert cmd[-1] == '{"day": "2026-01-03"}' and env["DAGSTER_PARTITION_KEY"] == KEY


# ── secrets and data versioning ─────────────────────────────────────────────

def test_vault_substitutes_secret_paths_and_commands(monkeypatch):
    mod, asset = _load("hashicorp_vault", {
        **PARTITION,
        "secrets": [{"path": "secret/data/{partition_key}/db", "key": "password", "env_var": "DB_PASSWORD"}],
        "commands": ["echo run-for-{partition_date}"]})
    fetched = []
    monkeypatch.setattr(mod.VaultResource, "get_secret", lambda self, path, key: fetched.append((path, key)) or "s3cret")
    rec = _Recorder()
    monkeypatch.setattr(mod.subprocess, "run", lambda cmd, **kw: rec(cmd, **kw) and subprocess.CompletedProcess(cmd, 0, stdout="", stderr=""))
    assert _materialize(asset).success
    assert fetched == [("secret/data/2026-01-03/db", "password")]
    assert rec.calls[0][0] == "echo run-for-2026-01-03"
    env = rec.calls[0][1]
    assert env["DB_PASSWORD"] == "s3cret" and env["DAGSTER_PARTITION_KEY"] == KEY


def test_lakefs_substitutes_branch_and_commit_message(monkeypatch):
    seen = {}

    class Branch:
        def __init__(self, name):
            seen["branch"] = name

        def commit(self, message, metadata):
            seen["message"], seen["metadata"] = message, metadata
            return types.SimpleNamespace(get_commit=lambda: types.SimpleNamespace(id="c1"))

    class Repo:
        def __init__(self, *a, **k):
            pass

        def branch(self, name):
            return Branch(name)

    pkg = types.ModuleType("lakefs")
    pkg.Repository = Repo
    client = types.ModuleType("lakefs.client")
    client.Client = lambda **kw: object()
    pkg.client = client
    monkeypatch.setitem(sys.modules, "lakefs", pkg)
    monkeypatch.setitem(sys.modules, "lakefs.client", client)
    monkeypatch.setenv("LAKEFS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("LAKEFS_SECRET_ACCESS_KEY", "s")
    spec = importlib.util.spec_from_file_location("lakefs_asset_part", ROOT / "assets/analytics/lakefs_asset/component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((ROOT / "assets/analytics/lakefs_asset/example.yaml").read_text()) if d and "attributes" in d)
    attrs = {**doc["attributes"], **PARTITION, "branch": "load-{partition_date}", "commit_message": "ETL {partition_key}",
             "metadata": {"day": "{partition_date}"}}
    asset = next(a for a in getattr(mod, doc["type"].rsplit(".", 1)[-1])(**attrs).build_defs(None).assets if hasattr(a, "node_def"))
    assert _materialize(asset).success
    assert seen == {"branch": "load-2026-01-03", "message": "ETL 2026-01-03", "metadata": {"day": "2026-01-03"}}


# ── the shared helper ───────────────────────────────────────────────────────

def test_partition_vars_helper_copies_are_identical_and_behave():
    begin, end = "# BEGIN partition-vars helper", "# END partition-vars helper"
    blocks = {}
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        text = p.read_text()
        if begin in text:
            blocks.setdefault(text[text.index(begin): text.index(end) + len(end)], []).append(str(p.relative_to(ROOT)))
    assert len(blocks) == 1 and sum(len(v) for v in blocks.values()) == 15, {k[:40]: v for k, v in blocks.items()}
    ns = {}
    exec(next(iter(blocks)), ns)
    sub = ns["_substitute_partition"]
    tokens = {"partition_key": "k", "partition_date": "2026-01-03"}
    assert sub({"a": ["{partition_key}", 5, None], "b": "{partition_date} {unknown} {{partition_key}}"}, tokens) == {
        "a": ["k", 5, None], "b": "2026-01-03 {unknown} {k}"}
    assert sub("{partition_key}", {}) == "{partition_key}" and sub(None, tokens) is None
    assert ns["_partition_env"]({}) == {} and ns["_partition_env"](tokens) == {"DAGSTER_PARTITION_KEY": "k", "DAGSTER_PARTITION_DATE": "2026-01-03"}
    ctx = types.SimpleNamespace(has_partition_key=False, has_partition_key_range=False)
    assert ns["_partition_vars"](ctx) == {}


def test_rivery_has_nothing_to_parameterize_so_it_says_so_instead_of_pretending():
    spec = importlib.util.spec_from_file_location("rivery_part", INFRA / "rivery_run_asset/component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((INFRA / "rivery_run_asset/example.yaml").read_text()) if d and "attributes" in d)
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    with pytest.warns(UserWarning, match="ignores partition_type"):
        defs = cls(**{**doc["attributes"], **PARTITION}).build_defs(None)
    assert next(a for a in defs.assets if hasattr(a, "node_def")).partitions_def is None
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # unpartitioned use stays silent
        cls(**doc["attributes"]).build_defs(None)
