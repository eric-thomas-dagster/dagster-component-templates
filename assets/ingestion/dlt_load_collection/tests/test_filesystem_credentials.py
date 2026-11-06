"""filesystem_credentials: inline SOURCE-bucket credentials (SFTP/Azure/...),
passed straight to dlt's filesystem(). Mocks only the real filesystem() call
(the boundary where a real SFTP/Azure connection would otherwise be attempted)
-- everything else is real component logic."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _patch_filesystem(monkeypatch, captured):
    import dlt.sources.filesystem as fsmod
    real_filesystem = fsmod.filesystem

    def fake_filesystem(**kwargs):
        captured.update(kwargs)
        return real_filesystem(**kwargs)

    monkeypatch.setattr(fsmod, "filesystem", fake_filesystem)


def test_filesystem_credentials_reach_the_real_filesystem_call(mod, tmp_path, monkeypatch):
    captured = {}
    _patch_filesystem(monkeypatch, captured)
    (tmp_path / "a.csv").write_text("id\n1\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_credentials={"sftp_username": "u", "sftp_password": "p"},
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    component._build_resources()
    assert captured.get("credentials") == {"sftp_username": "u", "sftp_password": "p"}


def test_filesystem_credentials_unset_omits_the_kwarg_entirely(mod, tmp_path, monkeypatch):
    """Must NOT pass credentials=None -- that would replace dlt's own
    dlt.secrets.value injection sentinel with "no credentials", breaking the
    existing, working env-var-only flow for every provider that doesn't set
    this field."""
    captured = {}
    _patch_filesystem(monkeypatch, captured)
    (tmp_path / "a.csv").write_text("id\n1\n")
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    component._build_resources()
    assert "credentials" not in captured


def test_filesystem_credentials_unresolved_env_var_raises_clearly(mod, tmp_path):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="filesystem",
        filesystem_bucket_url=str(tmp_path),
        filesystem_credentials={"sftp_username": "u", "sftp_password": None},
        filesystem_resources=[{"name": "records", "file_glob": "*.csv"}],
    )
    with pytest.raises(ValueError, match="filesystem_credentials.sftp_password"):
        component._build_resources()
