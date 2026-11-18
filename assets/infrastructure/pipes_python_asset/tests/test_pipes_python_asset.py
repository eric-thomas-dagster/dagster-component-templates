"""Real, committed tests for PipesPythonAssetComponent.

external_python mode tests run a REAL dagster_pipes-aware script under the CURRENT interpreter
(sys.executable already has dagster-pipes installed) -- a real, fast, full round trip, no
mocking of Dagster Pipes itself.

virtualenv mode's real venv-creation path is exercised once end-to-end (slower: a real `venv.create`
+ `pip install dagster-pipes`) to prove the mechanism genuinely works; the hash-based reuse/rebuild
decision is also tested in isolation against `_ensure_venv` directly, without real venv/pip calls,
since that's pure logic.
"""
import sys

import dagster as dg
import pytest

from .conftest import FIXTURE_SCRIPT


def _materialize(mod, **attrs):
    component = mod.PipesPythonAssetComponent(asset_name="t", script_path=FIXTURE_SCRIPT, **attrs)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


# --- external_python mode (real, fast) ---------------------------------------------------

def test_external_python_runs_the_real_script_under_the_given_interpreter(mod):
    result = _materialize(mod, mode="external_python", python_executable=sys.executable)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["python_path"].value == sys.executable


def test_external_python_passes_args_and_extras(mod):
    result = _materialize(
        mod, mode="external_python", python_executable=sys.executable,
        args=["--flag", "value"], extras={"echo": "hello-extras"},
    )
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["args"].value == ["--flag", "value"]
    assert md["extras_echo"].value == "hello-extras"


def test_external_python_requires_python_executable(mod):
    with pytest.raises(Exception, match="python_executable"):
        mod.PipesPythonAssetComponent(asset_name="t", script_path=FIXTURE_SCRIPT, mode="external_python")


def test_external_python_rejects_requirements(mod):
    with pytest.raises(Exception, match="requirements"):
        mod.PipesPythonAssetComponent(
            asset_name="t", script_path=FIXTURE_SCRIPT, mode="external_python",
            python_executable=sys.executable, requirements=["requests"],
        )


def test_virtualenv_mode_rejects_python_executable(mod):
    with pytest.raises(Exception, match="python_executable"):
        mod.PipesPythonAssetComponent(
            asset_name="t", script_path=FIXTURE_SCRIPT, mode="virtualenv",
            python_executable=sys.executable,
        )


# --- virtualenv mode: _ensure_venv's reuse/rebuild logic, in isolation --------------------

class _FakeLog:
    def info(self, *a, **k):
        pass


def test_ensure_venv_rebuilds_when_no_prior_venv_exists(mod, tmp_path, monkeypatch):
    import os
    created = []
    monkeypatch.setattr(mod, "subprocess", type("S", (), {"run": staticmethod(lambda *a, **k: created.append(a))})())

    def _fake_create(path, **k):
        os.makedirs(path, exist_ok=True)
        created.append(("venv.create", path))

    import venv as real_venv
    monkeypatch.setattr(real_venv, "create", _fake_create)
    venv_path = str(tmp_path / "myvenv")
    python_bin = mod._ensure_venv(venv_path, ["requests"], reuse=True, log=_FakeLog())
    assert any(c[0] == "venv.create" for c in created if isinstance(c, tuple))
    assert python_bin == mod._venv_python_path(venv_path)


def test_ensure_venv_reuses_when_requirements_hash_matches(mod, tmp_path, monkeypatch):
    venv_path = tmp_path / "myvenv"
    python_bin = mod._venv_python_path(str(venv_path))
    import os
    os.makedirs(os.path.dirname(python_bin), exist_ok=True)
    open(python_bin, "w").close()
    req_hash = mod.hashlib.sha256("\n".join(sorted(["requests"])).encode("utf-8")).hexdigest()
    with open(venv_path / ".requirements_hash", "w") as f:
        f.write(req_hash)

    rebuilt = []
    monkeypatch.setattr(mod.subprocess, "run", lambda *a, **k: rebuilt.append(a))
    import venv as real_venv
    monkeypatch.setattr(real_venv, "create", lambda path, **k: rebuilt.append(("venv.create", path)))

    result_bin = mod._ensure_venv(str(venv_path), ["requests"], reuse=True, log=_FakeLog())
    assert result_bin == python_bin
    assert rebuilt == []  # reused, nothing rebuilt


def test_ensure_venv_rebuilds_when_requirements_hash_changes(mod, tmp_path, monkeypatch):
    venv_path = tmp_path / "myvenv"
    python_bin = mod._venv_python_path(str(venv_path))
    import os
    os.makedirs(os.path.dirname(python_bin), exist_ok=True)
    open(python_bin, "w").close()
    with open(venv_path / ".requirements_hash", "w") as f:
        f.write("stale-hash-from-different-requirements")

    rebuilt = []
    monkeypatch.setattr(mod.subprocess, "run", lambda *a, **k: rebuilt.append(a))
    import venv as real_venv
    monkeypatch.setattr(real_venv, "create", lambda path, **k: rebuilt.append(("venv.create", path)))

    mod._ensure_venv(str(venv_path), ["requests"], reuse=True, log=_FakeLog())
    assert any(c[0] == "venv.create" for c in rebuilt if isinstance(c, tuple))


def test_ensure_venv_always_rebuilds_when_reuse_is_false(mod, tmp_path, monkeypatch):
    venv_path = tmp_path / "myvenv"
    python_bin = mod._venv_python_path(str(venv_path))
    import os
    os.makedirs(os.path.dirname(python_bin), exist_ok=True)
    open(python_bin, "w").close()
    req_hash = mod.hashlib.sha256("\n".join(sorted(["requests"])).encode("utf-8")).hexdigest()
    with open(venv_path / ".requirements_hash", "w") as f:
        f.write(req_hash)

    rebuilt = []
    monkeypatch.setattr(mod.subprocess, "run", lambda *a, **k: rebuilt.append(a))
    import venv as real_venv
    monkeypatch.setattr(real_venv, "create", lambda path, **k: rebuilt.append(("venv.create", path)))

    mod._ensure_venv(str(venv_path), ["requests"], reuse=False, log=_FakeLog())
    assert any(c[0] == "venv.create" for c in rebuilt if isinstance(c, tuple))


# --- virtualenv mode: one real end-to-end venv build (slow, but real) ---------------------

def test_virtualenv_mode_real_end_to_end(mod, tmp_path):
    result = _materialize(
        mod, mode="virtualenv", venv_path=str(tmp_path / "venv"), requirements=[],
    )
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["python_path"].value.startswith(str(tmp_path / "venv"))
