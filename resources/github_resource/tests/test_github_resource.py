"""Committed regression tests for GitHubResource's Contents/Git/Pulls methods.

Monkeypatches `requests.Session.get/post/put/delete` (the one external
boundary all of GitHubResource's convenience methods go through via
`get_client()`) -- URL building, base64 encode/decode, optimistic-concurrency
`sha` handling, and the real "push rejected -> branch -> commit -> PR ->
merge" fallback sequence are all exercised for real.
"""
import base64

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def resource(mod):
    return mod.GitHubResource(token="ghp_test123")


# --- get_file_contents -------------------------------------------------------

def test_get_file_contents_decodes_base64(resource, monkeypatch):
    import requests

    def fake_get(self, url, params=None, timeout=None):
        assert url == "https://api.github.com/repos/acme/widgets/contents/README.md"
        assert params == {"ref": "main"}
        encoded = base64.b64encode(b"hello world").decode("ascii")
        return FakeResponse({"content": encoded, "sha": "abc123", "path": "README.md"})

    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = resource.get_file_contents("acme/widgets", "README.md")

    assert result == {"content": "hello world", "sha": "abc123"}


def test_get_file_contents_returns_none_on_404(resource, monkeypatch):
    import requests

    def fake_get(self, url, params=None, timeout=None):
        return FakeResponse({"message": "Not Found"}, status_code=404)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    assert resource.get_file_contents("acme/widgets", "missing.txt") is None


def test_get_file_contents_raises_on_other_errors(resource, monkeypatch):
    import requests

    def fake_get(self, url, params=None, timeout=None):
        return FakeResponse({"message": "Server Error"}, status_code=500)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        resource.get_file_contents("acme/widgets", "README.md")


def test_get_file_contents_uses_given_ref(resource, monkeypatch):
    import requests

    get_calls = []

    def fake_get(self, url, params=None, timeout=None):
        get_calls.append(params)
        encoded = base64.b64encode(b"data").decode("ascii")
        return FakeResponse({"content": encoded, "sha": "sha-on-branch"})

    monkeypatch.setattr(requests.Session, "get", fake_get)

    resource.get_file_contents("acme/widgets", "README.md", ref="feature-x")

    assert get_calls[0] == {"ref": "feature-x"}


# --- list_directory -----------------------------------------------------------

def test_list_directory_parses_entries(resource, monkeypatch):
    import requests

    def fake_get(self, url, params=None, timeout=None):
        assert url == "https://api.github.com/repos/acme/widgets/contents/src"
        assert params == {"ref": "main"}
        return FakeResponse([
            {"name": "main.py", "path": "src/main.py", "type": "file", "sha": "x", "size": 10},
            {"name": "utils", "path": "src/utils", "type": "dir", "sha": "y", "size": 0},
        ])

    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = resource.list_directory("acme/widgets", "src")

    assert result == [
        {"name": "main.py", "path": "src/main.py", "type": "file"},
        {"name": "utils", "path": "src/utils", "type": "dir"},
    ]


# --- create_or_update_file_contents --------------------------------------------

def test_create_or_update_file_contents_create_omits_sha(resource, monkeypatch):
    import requests

    put_calls = []

    def fake_put(self, url, json=None, timeout=None):
        put_calls.append(json)
        return FakeResponse({"content": {"sha": "new-sha"}, "commit": {"sha": "commit-sha"}})

    monkeypatch.setattr(requests.Session, "put", fake_put)

    result = resource.create_or_update_file_contents("acme/widgets", "new.txt", "hello", "Add new.txt")

    assert "sha" not in put_calls[0]
    assert put_calls[0]["message"] == "Add new.txt"
    assert put_calls[0]["branch"] == "main"
    assert base64.b64decode(put_calls[0]["content"]).decode("utf-8") == "hello"
    assert result["content"]["sha"] == "new-sha"


def test_create_or_update_file_contents_update_includes_sha(resource, monkeypatch):
    import requests

    put_calls = []

    def fake_put(self, url, json=None, timeout=None):
        put_calls.append(json)
        return FakeResponse({"content": {"sha": "updated-sha"}})

    monkeypatch.setattr(requests.Session, "put", fake_put)

    resource.create_or_update_file_contents(
        "acme/widgets", "existing.txt", "updated content", "Update existing.txt",
        branch="automation/update", sha="old-sha",
    )

    assert put_calls[0]["sha"] == "old-sha"
    assert put_calls[0]["branch"] == "automation/update"
    assert base64.b64decode(put_calls[0]["content"]).decode("utf-8") == "updated content"


# --- delete_file_contents -------------------------------------------------------

def test_delete_file_contents_sends_sha_and_message(resource, monkeypatch):
    import requests

    delete_calls = []

    def fake_delete(self, url, json=None, timeout=None):
        delete_calls.append({"url": url, "json": json})
        return FakeResponse({"commit": {"sha": "delete-commit-sha"}})

    monkeypatch.setattr(requests.Session, "delete", fake_delete)

    result = resource.delete_file_contents("acme/widgets", "old.txt", "old-sha", "Remove old.txt")

    assert delete_calls[0]["url"] == "https://api.github.com/repos/acme/widgets/contents/old.txt"
    assert delete_calls[0]["json"] == {"message": "Remove old.txt", "sha": "old-sha", "branch": "main"}
    assert result["commit"]["sha"] == "delete-commit-sha"


# --- create_branch -----------------------------------------------------------

def test_create_branch_gets_tip_then_creates_ref(resource, monkeypatch):
    import requests

    get_calls = []
    post_calls = []

    def fake_get(self, url, params=None, timeout=None):
        get_calls.append(url)
        assert url == "https://api.github.com/repos/acme/widgets/git/ref/heads/main"
        return FakeResponse({"ref": "refs/heads/main", "object": {"sha": "tip-sha"}})

    def fake_post(self, url, json=None, timeout=None):
        post_calls.append({"url": url, "json": json})
        return FakeResponse({"ref": "refs/heads/feature-x", "object": {"sha": "tip-sha"}})

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(requests.Session, "post", fake_post)

    resource.create_branch("acme/widgets", "feature-x")

    assert get_calls == ["https://api.github.com/repos/acme/widgets/git/ref/heads/main"]
    assert post_calls[0]["url"] == "https://api.github.com/repos/acme/widgets/git/refs"
    assert post_calls[0]["json"] == {"ref": "refs/heads/feature-x", "sha": "tip-sha"}


def test_create_branch_uses_given_from_branch(resource, monkeypatch):
    import requests

    get_calls = []

    def fake_get(self, url, params=None, timeout=None):
        get_calls.append(url)
        return FakeResponse({"object": {"sha": "release-tip-sha"}})

    def fake_post(self, url, json=None, timeout=None):
        return FakeResponse({"ref": f"refs/heads/{json['ref'].split('/')[-1]}", "object": {"sha": json["sha"]}})

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(requests.Session, "post", fake_post)

    resource.create_branch("acme/widgets", "hotfix-1", from_branch="release")

    assert get_calls == ["https://api.github.com/repos/acme/widgets/git/ref/heads/release"]


# --- create_pull_request -------------------------------------------------------

def test_create_pull_request_posts_payload(resource, monkeypatch):
    import requests

    post_calls = []

    def fake_post(self, url, json=None, timeout=None):
        post_calls.append({"url": url, "json": json})
        return FakeResponse({"number": 42, "html_url": "https://github.com/acme/widgets/pull/42"})

    monkeypatch.setattr(requests.Session, "post", fake_post)

    result = resource.create_pull_request("acme/widgets", "Add feature", head="feature-x", base="main", body="desc")

    assert result["number"] == 42
    assert post_calls[0]["url"] == "https://api.github.com/repos/acme/widgets/pulls"
    assert post_calls[0]["json"] == {"title": "Add feature", "head": "feature-x", "base": "main", "body": "desc"}


# --- merge_pull_request -------------------------------------------------------

def test_merge_pull_request_puts_merge_method(resource, monkeypatch):
    import requests

    put_calls = []

    def fake_put(self, url, json=None, timeout=None):
        put_calls.append({"url": url, "json": json})
        return FakeResponse({"merged": True, "sha": "merge-sha"})

    monkeypatch.setattr(requests.Session, "put", fake_put)

    result = resource.merge_pull_request("acme/widgets", 42, merge_method="rebase")

    assert result["merged"] is True
    assert put_calls[0]["url"] == "https://api.github.com/repos/acme/widgets/pulls/42/merge"
    assert put_calls[0]["json"] == {"merge_method": "rebase"}


def test_merge_pull_request_defaults_to_squash(resource, monkeypatch):
    import requests

    put_calls = []

    def fake_put(self, url, json=None, timeout=None):
        put_calls.append(json)
        return FakeResponse({"merged": True})

    monkeypatch.setattr(requests.Session, "put", fake_put)

    resource.merge_pull_request("acme/widgets", 7)

    assert put_calls[0] == {"merge_method": "squash"}


# --- integration: push rejected -> branch -> commit -> PR -> merge -------------

def test_direct_push_rejected_falls_back_to_branch_commit_pr_merge(resource, monkeypatch):
    """Mirrors how a caller elsewhere in this codebase is expected to use these
    methods: try a direct commit to `main`, and when GitHub rejects it (e.g.
    branch protection requiring PRs, modeled here as a 422), fall back to
    branch + commit + PR + merge."""
    import requests

    repo = "acme/widgets"
    calls = {"get": [], "post": [], "put": []}

    def fake_get(self, url, params=None, timeout=None):
        calls["get"].append(url)
        assert url == f"https://api.github.com/repos/{repo}/git/ref/heads/main"
        return FakeResponse({"object": {"sha": "main-tip-sha"}})

    def fake_put(self, url, json=None, timeout=None):
        calls["put"].append({"url": url, "json": json})
        if url == f"https://api.github.com/repos/{repo}/contents/config.yaml":
            if json.get("branch") == "main":
                # branch protection rejects the direct push
                return FakeResponse({"message": "Changes must be made through a pull request"}, status_code=422)
            if json.get("branch") == "automation/update-config":
                return FakeResponse({"content": {"sha": "new-file-sha"}, "commit": {"sha": "commit-sha"}})
        if url == f"https://api.github.com/repos/{repo}/pulls/7/merge":
            return FakeResponse({"merged": True, "sha": "merge-sha"})
        raise AssertionError(f"unexpected PUT {url} {json}")

    def fake_post(self, url, json=None, timeout=None):
        calls["post"].append({"url": url, "json": json})
        if url == f"https://api.github.com/repos/{repo}/git/refs":
            assert json == {"ref": "refs/heads/automation/update-config", "sha": "main-tip-sha"}
            return FakeResponse({"ref": "refs/heads/automation/update-config", "object": {"sha": "main-tip-sha"}})
        if url == f"https://api.github.com/repos/{repo}/pulls":
            return FakeResponse({"number": 7, "html_url": f"https://github.com/{repo}/pull/7"})
        raise AssertionError(f"unexpected POST {url} {json}")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(requests.Session, "put", fake_put)
    monkeypatch.setattr(requests.Session, "post", fake_post)

    # 1. Try a direct push to main -- rejected.
    with pytest.raises(RuntimeError, match="HTTP 422"):
        resource.create_or_update_file_contents(repo, "config.yaml", "new: contents\n", "Update config", branch="main")

    # 2. Branch off main.
    resource.create_branch(repo, "automation/update-config", from_branch="main")

    # 3. Commit to the new branch -- succeeds.
    commit_result = resource.create_or_update_file_contents(
        repo, "config.yaml", "new: contents\n", "Update config", branch="automation/update-config",
    )
    assert commit_result["content"]["sha"] == "new-file-sha"

    # 4. Open a PR.
    pr = resource.create_pull_request(repo, "Update config", head="automation/update-config", base="main")
    assert pr["number"] == 7

    # 5. Merge it.
    merge_result = resource.merge_pull_request(repo, pr["number"], merge_method="squash")
    assert merge_result["merged"] is True

    assert calls["get"] == [f"https://api.github.com/repos/{repo}/git/ref/heads/main"]
    assert len(calls["put"]) == 3  # rejected direct push + successful branch commit + merge
    assert len(calls["post"]) == 2  # create ref + open PR


# --- component registration ----------------------------------------------------

def test_component_registers_resource_under_resource_key(mod, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_abc")
    component = mod.GithubResourceComponent(resource_key="github_prod", token_env_var="GITHUB_TOKEN")
    defs = component.build_defs(context=None)
    assert "github_prod" in defs.resources
    assert isinstance(defs.resources["github_prod"], mod.GitHubResource)
