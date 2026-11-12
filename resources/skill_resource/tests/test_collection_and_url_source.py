"""mode='collection' (local_path exercised for real, git_url mocks only subprocess.run)
and the url download source (requests.get mocked -- the one network boundary)."""
import io
import json
import zipfile

import pytest

from .conftest import SAMPLE_SKILL_MD


def _write_skill_folder(root, dirname, name, description, body="body"):
    d = root / dirname
    d.mkdir(parents=True)
    (d / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {description}\n---\n{body}")
    return d


# ── mode="collection", local_path ───────────────────────────────────────────────────

def test_collection_registers_one_resource_per_skill_subfolder(mod, tmp_path):
    _write_skill_folder(tmp_path, "pdf", "pdf-report", "Generate a PDF report.")
    _write_skill_folder(tmp_path, "docx", "docx-report", "Generate a DOCX report.")
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path))
    defs = component.build_defs(None)
    assert set(defs.resources) == {"pdf", "docx"}
    assert defs.resources["pdf"].describe() == {"key": "pdf", "name": "pdf-report", "description": "Generate a PDF report."}
    assert defs.resources["docx"].describe()["name"] == "docx-report"


def test_collection_silently_skips_non_skill_subfolders(mod, tmp_path):
    _write_skill_folder(tmp_path, "pdf", "pdf-report", "Generate a PDF report.")
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github" / "workflows.yml").write_text("x")
    (tmp_path / "README.md").write_text("not a dir")  # a file, not a dir -- also skipped
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path))
    defs = component.build_defs(None)
    assert set(defs.resources) == {"pdf"}


def test_collection_include_filters_to_named_folders(mod, tmp_path):
    _write_skill_folder(tmp_path, "pdf", "pdf-report", "Generate a PDF report.")
    _write_skill_folder(tmp_path, "docx", "docx-report", "Generate a DOCX report.")
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path), include=["pdf"])
    defs = component.build_defs(None)
    assert set(defs.resources) == {"pdf"}


def test_collection_exclude_removes_named_folders(mod, tmp_path):
    _write_skill_folder(tmp_path, "pdf", "pdf-report", "Generate a PDF report.")
    _write_skill_folder(tmp_path, "docx", "docx-report", "Generate a DOCX report.")
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path), exclude=["docx"])
    defs = component.build_defs(None)
    assert set(defs.resources) == {"pdf"}


def test_collection_skill_with_no_description_fails_naming_the_folder(mod, tmp_path):
    d = tmp_path / "broken"
    d.mkdir()
    (d / "SKILL.md").write_text("---\nname: broken\n---\nbody only")
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path))
    with pytest.raises(ValueError, match="broken"):
        component.build_defs(None)


def test_collection_with_no_skills_found_raises(mod, tmp_path):
    (tmp_path / "not-a-skill").mkdir()
    component = mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path))
    with pytest.raises(ValueError, match="found no skill subfolders"):
        component.build_defs(None)


def test_collection_rejects_url_source(mod):
    with pytest.raises(ValueError, match="mode='collection'"):
        mod.SkillResourceComponent(mode="collection", url="https://example.com/skills.zip")


def test_collection_rejects_name_or_description_override(mod, tmp_path):
    with pytest.raises(ValueError, match="mode='collection'"):
        mod.SkillResourceComponent(mode="collection", local_path=str(tmp_path), name="x")


def test_single_mode_rejects_include_exclude(mod, tmp_path):
    local_path = tmp_path / "pdf-report"
    local_path.mkdir()
    (local_path / "SKILL.md").write_text(SAMPLE_SKILL_MD)
    with pytest.raises(ValueError, match="mode='collection'"):
        mod.SkillResourceComponent(local_path=str(local_path), include=["pdf"])


def test_collection_via_git_builds_per_skill_git_path(mod, monkeypatch):
    """Each registered skill's resource keeps git_url/git_ref so it can lazily re-fetch if
    the shared checkout cache is ever evicted -- git_path becomes <collection path>/<folder>."""
    import subprocess as real_subprocess
    from pathlib import Path

    def fake_run(cmd, **kwargs):
        dest = Path(cmd[-1])
        _write_skill_folder(dest, "pdf", "pdf-report", "Generate a PDF report.")
        _write_skill_folder(dest, "docx", "docx-report", "Generate a DOCX report.")

        class _FakeResult:
            returncode = 0
            stderr = ""
        return _FakeResult()

    monkeypatch.setattr(real_subprocess, "run", fake_run)

    component = mod.SkillResourceComponent(mode="collection", git_url="https://example.com/skills-collection.git", git_ref="main")
    defs = component.build_defs(None)
    pdf = defs.resources["pdf"]
    assert pdf.git_url == "https://example.com/skills-collection.git"
    assert pdf.git_path == "pdf"
    assert pdf.name == "pdf-report"


# ── url download source ──────────────────────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, content, headers=None):
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self):
        pass


def test_url_source_raw_skill_md(mod, monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append((url, headers))
        return _FakeResponse(SAMPLE_SKILL_MD.encode())

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    component = mod.SkillResourceComponent(resource_key="pdf_skill", url="https://example.com/skills/SKILL.md")
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.name == "pdf-report"
    assert calls[0][0] == "https://example.com/skills/SKILL.md"
    assert calls[0][1] is None


def test_url_source_zip_archive(mod, monkeypatch):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("SKILL.md", SAMPLE_SKILL_MD)
        zf.writestr("reference.md", "extra material")
    zip_bytes = buf.getvalue()

    def fake_get(url, headers=None, timeout=None):
        return _FakeResponse(zip_bytes)

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    component = mod.SkillResourceComponent(resource_key="pdf_skill", url="https://example.com/skills/pdf-report-unique1.zip")
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.name == "pdf-report"
    loaded = resource.load()
    assert loaded["files"] == ["reference.md"]


def test_url_source_zip_with_single_wrapping_directory(mod, monkeypatch):
    """A typical single-skill zip export (e.g. a marketplace download, or a GitHub
    "download this folder as a zip" tool) wraps its contents in one top-level folder
    matching the skill's own name -- the real SKILL.md ends up one level down, not at
    the zip root."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("pdf-report/SKILL.md", SAMPLE_SKILL_MD)
        zf.writestr("pdf-report/reference.md", "extra material")
    zip_bytes = buf.getvalue()

    def fake_get(url, headers=None, timeout=None):
        return _FakeResponse(zip_bytes)

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    component = mod.SkillResourceComponent(resource_key="pdf_skill", url="https://example.com/skills/pdf-report-unique2.zip")
    resource = component.build_defs(None).resources["pdf_skill"]
    assert resource.name == "pdf-report"


def test_url_headers_env_parsed_as_json_and_sent(mod, monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(headers)
        return _FakeResponse(SAMPLE_SKILL_MD.encode())

    import requests
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setenv("MY_DOWNLOAD_HEADERS", json.dumps({"Authorization": "Bearer secret-token"}))

    component = mod.SkillResourceComponent(
        resource_key="pdf_skill", url="https://example.com/skills/SKILL-unique3.md", url_headers_env="MY_DOWNLOAD_HEADERS",
    )
    component.build_defs(None)
    assert calls[0] == {"Authorization": "Bearer secret-token"}


def test_url_headers_env_non_json_fails_clearly(mod, monkeypatch):
    monkeypatch.setenv("BAD_HEADERS", "not json")
    component = mod.SkillResourceComponent(
        resource_key="pdf_skill", url="https://example.com/skills/SKILL-unique4.md", url_headers_env="BAD_HEADERS",
    )
    with pytest.raises(ValueError, match="JSON object"):
        component.build_defs(None)


def test_url_download_is_cached_across_components_for_same_url(mod, monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        return _FakeResponse(SAMPLE_SKILL_MD.encode())

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://example.com/skills/SKILL-unique5.md"
    mod.SkillResourceComponent(resource_key="a", url=url).build_defs(None)
    mod.SkillResourceComponent(resource_key="b", url=url).build_defs(None)
    assert len(calls) == 1
