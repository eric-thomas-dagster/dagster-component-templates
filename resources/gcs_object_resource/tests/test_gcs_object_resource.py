from unittest.mock import MagicMock, patch

import pytest


def _resource(mod, **kw):
    return mod.GcsObjectResource(**kw)


def test_upload_calls_upload_from_string(mod):
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        _resource(mod).upload("my-bucket", "path/obj.json", b'{"a": 1}', content_type="application/json")
    client.bucket.assert_called_once_with("my-bucket")
    client.bucket.return_value.blob.assert_called_once_with("path/obj.json")
    blob.upload_from_string.assert_called_once_with(b'{"a": 1}', content_type="application/json")


def test_upload_defaults_content_type(mod):
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        _resource(mod).upload("my-bucket", "path/obj.bin", b"raw")
    blob.upload_from_string.assert_called_once_with(b"raw", content_type="application/octet-stream")


def test_upload_file_calls_upload_from_filename(mod):
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        _resource(mod).upload_file("my-bucket", "path/obj.bin", "/tmp/local.bin", content_type="text/plain")
    blob.upload_from_filename.assert_called_once_with("/tmp/local.bin", content_type="text/plain")


def test_download_returns_bytes(mod):
    client = MagicMock()
    blob = MagicMock()
    blob.download_as_bytes.return_value = b"hello"
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        result = _resource(mod).download("my-bucket", "path/obj.txt")
    assert result == b"hello"
    blob.download_as_bytes.assert_called_once()


def test_download_to_file_calls_download_to_filename(mod):
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        _resource(mod).download_to_file("my-bucket", "path/obj.txt", "/tmp/out.txt")
    blob.download_to_filename.assert_called_once_with("/tmp/out.txt")


def test_list_objects_no_prefix(mod):
    client = MagicMock()
    b1, b2 = MagicMock(name="a"), MagicMock(name="b")
    b1.name, b2.name = "a/one.json", "a/two.json"
    client.list_blobs.return_value = [b1, b2]
    with patch("google.cloud.storage.Client", return_value=client):
        result = _resource(mod).list_objects("my-bucket")
    assert result == ["a/one.json", "a/two.json"]
    client.list_blobs.assert_called_once_with("my-bucket", prefix=None)


def test_list_objects_with_prefix(mod):
    client = MagicMock()
    client.list_blobs.return_value = []
    with patch("google.cloud.storage.Client", return_value=client):
        _resource(mod).list_objects("my-bucket", prefix="a/")
    client.list_blobs.assert_called_once_with("my-bucket", prefix="a/")


def test_delete_existing_object_returns_true(mod):
    client = MagicMock()
    blob = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        result = _resource(mod).delete("my-bucket", "path/obj.json")
    assert result is True
    blob.delete.assert_called_once()


def test_delete_missing_object_is_idempotent_by_default(mod):
    from google.api_core.exceptions import NotFound

    client = MagicMock()
    blob = MagicMock()
    blob.delete.side_effect = NotFound("gone")
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        result = _resource(mod).delete("my-bucket", "path/obj.json")
    assert result is False


def test_delete_missing_object_raises_when_missing_ok_false(mod):
    from google.api_core.exceptions import NotFound

    client = MagicMock()
    blob = MagicMock()
    blob.delete.side_effect = NotFound("gone")
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        with pytest.raises(NotFound):
            _resource(mod).delete("my-bucket", "path/obj.json", missing_ok=False)


def test_exists_true(mod):
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = True
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        assert _resource(mod).exists("my-bucket", "path/obj.json") is True


def test_exists_false(mod):
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = False
    client.bucket.return_value.blob.return_value = blob
    with patch("google.cloud.storage.Client", return_value=client):
        assert _resource(mod).exists("my-bucket", "path/obj.json") is False


def test_component_registers_resource(mod):
    component = mod.GcsObjectResourceComponent(resource_key="my_gcs_objects")
    defs = component.build_defs(context=None)
    resource = defs.resources["my_gcs_objects"]
    assert isinstance(resource, mod.GcsObjectResource)
