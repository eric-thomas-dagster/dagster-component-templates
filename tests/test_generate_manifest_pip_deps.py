"""Regression test: generate_manifest.py's requirements.txt parsing stripped whole-line
comments but not trailing inline ones (e.g. "mcp>=1.30,<3   # verified with..."), so a
dependency's own explanatory comment ended up baked into manifest.json's dependencies.pip
as part of the pip spec string -- a UI reading that to say "what's missing" would see
garbage instead of a clean requirement."""
import importlib.util
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_generate_manifest():
    spec = importlib.util.spec_from_file_location("generate_manifest_under_test", REPO_ROOT / "generate_manifest.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod(monkeypatch, tmp_path):
    """`build_derivable_fields` computes its component path relative to REPO_ROOT and
    requires a sibling schema.json -- monkeypatch REPO_ROOT to tmp_path itself so a bare
    fixture dir (with just a requirements.txt + minimal schema.json) is a valid 'component
    dir' for this test, without needing a real repo-rooted component."""
    m = _load_generate_manifest()
    monkeypatch.setattr(m, "REPO_ROOT", tmp_path)
    (tmp_path / "schema.json").write_text('{"name": "x", "category": "other", "description": ""}')
    return m


def test_trailing_inline_comment_is_stripped_from_pip_dep(mod, tmp_path):
    (tmp_path / "requirements.txt").write_text(
        "litellm>=1.30.0\n"
        "mcp>=1.30,<3   # verified with mcp 1.30.0 and 2.3.0 (client API differs between majors)\n"
    )
    derived = mod.build_derivable_fields(tmp_path)
    assert derived["_pip_deps"] == ["litellm>=1.30.0", "mcp>=1.30,<3"]


def test_whole_line_comment_still_excluded(mod, tmp_path):
    (tmp_path / "requirements.txt").write_text(
        "# this whole line is a comment\n"
        "requests>=2.28\n"
    )
    derived = mod.build_derivable_fields(tmp_path)
    assert derived["_pip_deps"] == ["requests>=2.28"]


def test_blank_lines_and_comment_only_lines_produce_no_empty_entries(mod, tmp_path):
    (tmp_path / "requirements.txt").write_text(
        "pandas>=1.5.0  # only needed when return_dataframe=true\n"
        "\n"
        "   # another full-line comment\n"
        "boto3>=1.34\n"
    )
    derived = mod.build_derivable_fields(tmp_path)
    assert derived["_pip_deps"] == ["pandas>=1.5.0", "boto3>=1.34"]


def test_no_requirements_file_yields_empty_deps(mod, tmp_path):
    derived = mod.build_derivable_fields(tmp_path)
    assert derived["_pip_deps"] == []


# ─── unavailable_reason ─────────────────────────────────────────────────────
# Real bug this guards: a first version blindly guessed every pip spec's import name (splitting
# on "-"), which would have wrongly reported plenty of installed hyphenated packages (their own
# real import name differs, e.g. beautifulsoup4 -> bs4) as "unavailable" -- worse than not
# checking them at all. Only PIP_TO_IMPORT_NAME's explicit entries, or a simple single-word name
# that trivially equals its own import name, are ever checked.

def test_unavailable_reason_is_none_when_every_hard_dep_is_installed(mod, tmp_path):
    (tmp_path / "requirements.txt").write_text("pandas>=1.5.0\n")  # pandas is installed in this test env
    derived = mod.build_derivable_fields(tmp_path)
    assert "unavailable_reason" not in derived


def test_unavailable_reason_flags_a_genuinely_missing_package(mod, tmp_path):
    (tmp_path / "requirements.txt").write_text("this_package_definitely_does_not_exist_anywhere>=1.0\n")
    derived = mod.build_derivable_fields(tmp_path)
    assert "unavailable_reason" in derived
    assert "this_package_definitely_does_not_exist_anywhere" in derived["unavailable_reason"]


def test_unavailable_reason_uses_the_explicit_import_name_override(mod, tmp_path):
    # beautifulsoup4's real import name is bs4, not beautifulsoup4 -- if bs4 isn't installed in
    # this test env, the reason must name the override's import, never a naive guess.
    (tmp_path / "requirements.txt").write_text("beautifulsoup4>=4.12.0\n")
    derived = mod.build_derivable_fields(tmp_path)
    if importlib_util_find_spec_missing("bs4"):
        assert "unavailable_reason" in derived
    else:
        assert "unavailable_reason" not in derived


def test_unavailable_reason_never_guesses_an_unmapped_hyphenated_package(mod, tmp_path):
    # Not in PIP_TO_IMPORT_NAME, and hyphenated -- must never be guessed/flagged, regardless of
    # whether some made-up import name would or wouldn't resolve.
    (tmp_path / "requirements.txt").write_text("some-made-up-package-not-in-the-override-table>=1.0\n")
    derived = mod.build_derivable_fields(tmp_path)
    assert "unavailable_reason" not in derived


def importlib_util_find_spec_missing(name: str) -> bool:
    import importlib.util
    try:
        return importlib.util.find_spec(name) is None
    except (ImportError, ModuleNotFoundError, ValueError):
        return True
