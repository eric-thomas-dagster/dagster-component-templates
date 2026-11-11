"""source_dir accepts a remote fsspec URI (s3://, gs://, az://, abfs://, sftp://, ...) with
storage options resolved from named env vars at materialization time -- never baked into the
code location at load time. Exercised here against fsspec's built-in in-memory filesystem
(memory://) so the test needs no real cloud account or extra backend package.
"""
import uuid

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
DocumentCorpusComponent = mod.DocumentCorpusComponent


def _materialize(comp, **kwargs):
    kwargs.setdefault("raise_on_error", True)
    defs = comp.build_defs(None)
    return dg.materialize([*defs.assets, *(defs.asset_checks or [])], **kwargs)


@pytest.fixture
def memory_root():
    import fsspec
    root = f"memory://corpus-{uuid.uuid4().hex[:8]}"
    fs, path = fsspec.core.url_to_fs(root)
    fs.makedirs(path, exist_ok=True)
    yield root, fs, path
    fs.rm(path, recursive=True)


def test_remote_uri_is_scanned_and_extracted(memory_root):
    root, fs, path = memory_root
    with fs.open(f"{path}/a.md", "wb") as f:
        f.write(b"# Hello\n\nWorld")
    with fs.open(f"{path}/b.md", "wb") as f:
        f.write(b"# Second\n\nDoc")

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=root, file_glob="*.md")
    result = _materialize(comp)
    df = result.output_for_node("corpus")

    assert sorted(df["doc_id"]) == ["a.md", "b.md"]
    assert sorted(df["content"]) == ["# Hello\n\nWorld", "# Second\n\nDoc"]
    assert (df["source_type"] == "memory").all()
    assert df["url"].str.startswith("memory://").all()


def test_remote_uri_default_glob_recurses_into_subdirectories(memory_root):
    root, fs, path = memory_root
    fs.makedirs(f"{path}/sub", exist_ok=True)
    with fs.open(f"{path}/top.md", "wb") as f:
        f.write(b"top level")
    with fs.open(f"{path}/sub/nested.md", "wb") as f:
        f.write(b"nested")

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=root, file_glob="**/*.md")
    result = _materialize(comp)
    df = result.output_for_node("corpus")

    assert sorted(df["doc_id"]) == ["sub/nested.md", "top.md"]


def test_missing_remote_root_fails_loudly(memory_root):
    root, fs, path = memory_root
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=f"{root}/does-not-exist", file_glob="*.md")
    with pytest.raises(Exception, match="does not exist or is not reachable"):
        _materialize(comp)


def test_storage_options_env_vars_missing_env_var_raises_clearly(memory_root):
    root, fs, path = memory_root
    with fs.open(f"{path}/a.md", "wb") as f:
        f.write(b"content")

    comp = DocumentCorpusComponent(
        asset_name="corpus",
        source_dir=root,
        file_glob="*.md",
        storage_options_env_vars={"key": "CORPUS_TEST_UNSET_ENV_VAR"},
    )
    with pytest.raises(Exception, match="CORPUS_TEST_UNSET_ENV_VAR"):
        _materialize(comp)


def test_storage_options_env_vars_accepts_ergonomic_kv_list_shape(memory_root):
    """`coerce_field_shapes` lets storage_options_env_vars be written as ["key=VAR"] in YAML."""
    root, fs, path = memory_root
    comp = DocumentCorpusComponent(
        asset_name="corpus",
        source_dir=root,
        file_glob="*.md",
        storage_options_env_vars=["key=CORPUS_TEST_UNSET_ENV_VAR"],
    )
    assert comp.storage_options_env_vars == {"key": "CORPUS_TEST_UNSET_ENV_VAR"}
