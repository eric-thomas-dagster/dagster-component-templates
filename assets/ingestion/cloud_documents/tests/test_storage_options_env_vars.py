"""storage_options_env_vars: named fsspec storage options (e.g. endpoint_url for an
S3-compatible store like Cloudflare R2/MinIO/Rustfs), resolved via dg.EnvVar at materialization
time only -- never baked into the code location at load time, same pattern as
document_corpus's own storage_options_env_vars added this session.
"""
import fsspec
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_module(mod, component):
    import dagster as dg
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)


def test_missing_env_var_raises_a_clear_error(mod, monkeypatch, tmp_path):
    monkeypatch.delenv("CLOUD_DOCS_TEST_UNSET_ENV_VAR", raising=False)
    install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        storage_options_env_vars={"endpoint_url": "CLOUD_DOCS_TEST_UNSET_ENV_VAR"},
    )
    result = _materialize_module(mod, component)
    assert not result.success


def test_resolved_storage_options_are_forwarded_to_fsspec(mod, monkeypatch, tmp_path):
    (tmp_path / "a.md").write_text("hello world", encoding="utf-8")
    monkeypatch.setenv("CLOUD_DOCS_TEST_ENDPOINT", "http://localhost:9000")
    install_fake_dlt(monkeypatch, mod)

    captured = {}
    real_url_to_fs = fsspec.core.url_to_fs

    def _spy(path, **kwargs):
        captured.update(kwargs)
        return real_url_to_fs(path)  # file:// ignores the extra kwargs, still resolves for real

    monkeypatch.setattr(fsspec.core, "url_to_fs", _spy)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        storage_options_env_vars={"endpoint_url": "CLOUD_DOCS_TEST_ENDPOINT"},
        include_preview_metadata=False,
    )
    result = _materialize_module(mod, component)
    assert result.success
    assert captured.get("endpoint_url") == "http://localhost:9000"
