"""local_path mode is exercised for real (plain filesystem I/O, no mocking needed). git_url
mode mocks only the `git clone` subprocess call -- the one external-process boundary."""
import pytest

from .conftest import SAMPLE_SKILL_MD


def _write_local_skill(tmp_path, content=SAMPLE_SKILL_MD, dirname="pdf-report"):
    skill_dir = tmp_path / dirname
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(content)
    return str(skill_dir)


def test_requires_exactly_one_source_when_neither_set(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.SkillResourceComponent(resource_key="skill")


def test_requires_exactly_one_source_when_both_set(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    with pytest.raises(ValueError, match="exactly one of"):
        mod.SkillResourceComponent(resource_key="skill", git_url="https://example.com/x.git", local_path=local_path)


def test_name_and_description_resolved_from_front_matter(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    component = mod.SkillResourceComponent(resource_key="pdf_skill", local_path=local_path)
    defs = component.build_defs(None)
    resource = defs.resources["pdf_skill"]
    assert resource.describe() == {
        "key": "pdf_skill", "name": "pdf-report", "description": "Generate a formatted PDF report from a DataFrame.",
    }


def test_explicit_name_and_description_override_front_matter(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    component = mod.SkillResourceComponent(
        resource_key="pdf_skill", local_path=local_path, name="custom-name", description="custom description",
    )
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.name == "custom-name"
    assert resource.description == "custom description"


def test_load_returns_full_text_and_bundled_file_list(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    (tmp_path / "pdf-report" / "reference.md").write_text("extra reference material")
    (tmp_path / "pdf-report" / "scripts").mkdir()
    (tmp_path / "pdf-report" / "scripts" / "render.py").write_text("print('hi')")

    component = mod.SkillResourceComponent(resource_key="pdf_skill", local_path=local_path)
    resource = component.build_defs(None).resources["pdf_skill"]
    loaded = resource.load()
    assert "## Instructions" in loaded["content"]
    assert set(loaded["files"]) == {"reference.md", "scripts/render.py"}


def test_read_file_returns_bundled_file_content(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    (tmp_path / "pdf-report" / "reference.md").write_text("extra reference material")
    component = mod.SkillResourceComponent(resource_key="pdf_skill", local_path=local_path)
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.read_file("reference.md") == "extra reference material"


def test_read_file_rejects_path_escaping_the_folder(mod, tmp_path):
    local_path = _write_local_skill(tmp_path)
    (tmp_path / "secret.txt").write_text("nope")
    component = mod.SkillResourceComponent(resource_key="pdf_skill", local_path=local_path)
    resource = component.build_defs(None).resources["pdf_skill"]
    with pytest.raises(ValueError, match="escapes the skill folder"):
        resource.read_file("../secret.txt")


def test_missing_skill_md_fails_at_load_naming_the_path(mod, tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    component = mod.SkillResourceComponent(resource_key="broken_skill", local_path=str(empty_dir))
    with pytest.raises(ValueError, match=str(empty_dir)):
        component.build_defs(None)


def test_no_description_anywhere_fails_at_load(mod, tmp_path):
    local_path = _write_local_skill(tmp_path, content="---\nname: pdf-report\n---\nbody only")
    component = mod.SkillResourceComponent(resource_key="pdf_skill", local_path=local_path)
    with pytest.raises(ValueError, match="no `description`"):
        component.build_defs(None)


def test_git_clone_invoked_with_depth_1_and_ref(mod, monkeypatch):
    """Mocks only subprocess.run (the one external-process boundary) -- the fake `git clone`
    writes the skill dir for real into the dir git_clone was asked to target, so the rest of
    the real Path/file-reading logic runs unmodified."""
    import subprocess as real_subprocess
    from pathlib import Path

    calls = []

    class _FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        dest = Path(cmd[-1])
        (dest / "skills" / "pdf-report").mkdir(parents=True)
        (dest / "skills" / "pdf-report" / "SKILL.md").write_text(SAMPLE_SKILL_MD)
        return _FakeResult()

    monkeypatch.setattr(real_subprocess, "run", fake_run)

    component = mod.SkillResourceComponent(
        resource_key="pdf_skill", git_url="https://example.com/skills-unique-1.git", git_path="skills/pdf-report", git_ref="v2",
    )
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.name == "pdf-report"
    assert any("--depth" in c and "1" in c for c in calls)
    assert any("v2" in c for c in calls)


def test_git_checkout_is_cached_across_components_for_same_url_and_ref(mod, monkeypatch):
    import subprocess as real_subprocess
    from pathlib import Path

    calls = []

    class _FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        dest = Path(cmd[-1])
        (dest / "pdf-report").mkdir(parents=True)
        (dest / "pdf-report" / "SKILL.md").write_text(SAMPLE_SKILL_MD)
        return _FakeResult()

    monkeypatch.setattr(real_subprocess, "run", fake_run)

    url = "https://example.com/skills-unique-2.git"
    mod.SkillResourceComponent(resource_key="a", git_url=url, git_path="pdf-report", git_ref="main").build_defs(None)
    mod.SkillResourceComponent(resource_key="b", git_url=url, git_path="pdf-report", git_ref="main").build_defs(None)
    assert len(calls) == 1


def test_git_token_env_var_sent_as_auth_header(mod, monkeypatch):
    import subprocess as real_subprocess
    from pathlib import Path

    calls = []

    class _FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        dest = Path(cmd[-1])
        dest.mkdir(parents=True)
        (dest / "SKILL.md").write_text(SAMPLE_SKILL_MD)
        return _FakeResult()

    monkeypatch.setattr(real_subprocess, "run", fake_run)
    monkeypatch.setenv("MY_GIT_TOKEN", "secret-token")

    component = mod.SkillResourceComponent(
        resource_key="pdf_skill", git_url="https://example.com/skills-unique-3.git", git_ref="main",
        git_token_env_var="MY_GIT_TOKEN",
    )
    component.build_defs(None)
    assert any("secret-token" in c for c in calls[0])


def test_unreachable_git_source_fails_clearly(mod, monkeypatch):
    import subprocess as real_subprocess

    class _FakeResult:
        returncode = 128
        stderr = "fatal: repository not found"

    monkeypatch.setattr(real_subprocess, "run", lambda cmd, **kwargs: _FakeResult())

    component = mod.SkillResourceComponent(resource_key="broken_skill", git_url="https://example.com/nope-unique.git")
    with pytest.raises(RuntimeError, match="git clone"):
        component.build_defs(None)
