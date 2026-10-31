"""Regression test for `_ApprovalFS.move()` on plain (non-URI) local paths.

Real bug: the non-URI branch called `Path(dst).rsplit("/", 1)` as a
dead-code no-op expression (its result was never used -- the next line
already does the real work via `shutil.move`). `rsplit` is a `str` method,
not `pathlib.Path`'s, so this raised `AttributeError` on EVERY call whose
destination path contained a "/" -- i.e. virtually every real call, since
`dst` is always a path with at least one path separator. Found while live-
testing a CLI demo chaining this component with a real rejection-token
file move (uncovered by any prior test -- this component had no tests
before this file).
"""
from .conftest import load_component_module

mod = load_component_module()
_ApprovalFS = mod._ApprovalFS


def test_move_plain_local_path_does_not_raise(tmp_path):
    src = tmp_path / "pending" / "token.json"
    src.parent.mkdir(parents=True)
    src.write_text('{"approved": false}')

    fs = _ApprovalFS(str(tmp_path))
    dst = str(tmp_path / "consumed" / "token.json")

    fs.move(str(src), dst)  # must not raise AttributeError

    assert not src.exists()
    assert (tmp_path / "consumed" / "token.json").exists()
    assert (tmp_path / "consumed" / "token.json").read_text() == '{"approved": false}'


def test_move_creates_destination_parent_dir(tmp_path):
    src = tmp_path / "token.json"
    src.write_text("{}")
    fs = _ApprovalFS(str(tmp_path))
    dst = str(tmp_path / "a" / "b" / "c" / "token.json")

    fs.move(str(src), dst)

    assert (tmp_path / "a" / "b" / "c" / "token.json").exists()
