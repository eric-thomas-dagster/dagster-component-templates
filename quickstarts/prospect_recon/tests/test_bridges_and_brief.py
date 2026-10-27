"""Lighter coverage for the bridge assets, the live-template-driven brief
extraction, and the commit-to-dagster-demo-factory logic. Mocks only the
network boundary (github methods, litellm.completion)."""
import json
import sys
import types

import dagster as dg
import pandas as pd
import pytest


class _FakeGithub:
    def __init__(self, files=None):
        self.files = dict(files or {})
        self.writes = []
        self.deletes = []
        self.branches_created = []
        self.prs_created = []
        self.prs_merged = []
        self.fail_write_on_branch = None

    def get_file_contents(self, repo, path, ref="main"):
        return self.files.get(path)

    def create_or_update_file_contents(self, repo, path, content, message, branch="main", sha=None):
        if branch == self.fail_write_on_branch:
            raise RuntimeError("422 conflict (simulated)")
        self.writes.append({"path": path, "content": content, "branch": branch, "sha": sha})
        self.files[path] = {"content": content, "sha": f"sha-{len(self.writes)}"}
        return {"content": {"sha": f"sha-{len(self.writes)}"}}

    def delete_file_contents(self, repo, path, sha, message, branch="main"):
        self.deletes.append({"path": path, "sha": sha, "branch": branch})
        self.files.pop(path, None)

    def create_branch(self, repo, new_branch, from_branch="main"):
        self.branches_created.append(new_branch)

    def create_pull_request(self, repo, title, head, base="main", body=""):
        pr = {"number": len(self.prs_created) + 1}
        self.prs_created.append(pr)
        return pr

    def merge_pull_request(self, repo, pr_number, merge_method="squash"):
        self.prs_merged.append(pr_number)


# ── prospect_recon_evidence_df ──────────────────────────────────────────────

def test_evidence_df_builds_rows_from_tool_calls_and_final_text(mod):
    candidate = {"slug": "acme"}
    research = {
        "text": "Final summary of findings.",
        "tool_call_trace": [
            {"tool": "search_drive", "result": [{"id": "f1", "name": "Acme Notes.pdf"}]},
            {"tool": "read_drive_file", "result": "Acme uses Airflow and dbt."},
            {"tool": "noop", "result": ""},  # empty result dropped
        ],
    }
    df = mod.prospect_recon_evidence_df(dg.build_asset_context(), candidate, research)
    assert len(df) == 3  # 2 real tool calls + final summary
    assert (df["opportunity_slug"] == "acme").all()
    assert "research:final" in df["item_id"].tolist()


def test_evidence_df_falls_back_when_no_trace(mod):
    candidate = {"slug": "acme", "text": "Company: Acme"}
    research = {"text": "", "tool_call_trace": []}
    df = mod.prospect_recon_evidence_df(dg.build_asset_context(), candidate, research)
    assert len(df) == 1
    assert df.iloc[0]["source"] == "candidate"


# ── prospect_recon_items_json ───────────────────────────────────────────────

def test_items_json_drops_embedding_column(mod):
    kb = pd.DataFrame([{"chunk_id": "c1", "chunk_text": "hello", "embedding": [0.1, 0.2]}])
    result = mod.prospect_recon_items_json(dg.build_asset_context(), kb)
    parsed = json.loads(result["text"])
    assert "embedding" not in parsed[0]
    assert result["n_items"] == 1


def test_items_json_empty_kb(mod):
    result = mod.prospect_recon_items_json(dg.build_asset_context(), pd.DataFrame())
    assert result["text"] == "[]"
    assert result["n_items"] == 0


# ── derive_schema_from_template ─────────────────────────────────────────────

def test_derive_schema_from_headings(mod):
    template = "# Brief\n\n## Use Case\n\ntext\n\n## Pain Points\n\ntext"
    schema = mod.derive_schema_from_template(template)
    assert "use_case" in schema["properties"]
    assert "pain_points" in schema["properties"]
    assert "company" in schema["properties"] and "markdown_body" in schema["properties"]


def test_derive_schema_falls_back_when_no_headings(mod):
    assert mod.derive_schema_from_template("just prose, no headings") == mod.FALLBACK_BRIEF_SCHEMA


def test_derive_schema_falls_back_when_empty(mod):
    assert mod.derive_schema_from_template("") == mod.FALLBACK_BRIEF_SCHEMA
    assert mod.derive_schema_from_template(None) == mod.FALLBACK_BRIEF_SCHEMA


# ── prospect_recon_brief (mocked litellm) ───────────────────────────────────

def _install_fake_litellm(monkeypatch, response_args_json):
    fake_litellm = types.ModuleType("litellm")

    class _Call:
        def __init__(self, args):
            self.function = types.SimpleNamespace(arguments=args)

    class _Message:
        def __init__(self, args):
            self.tool_calls = [_Call(args)]

    class _Choice:
        def __init__(self, args):
            self.message = _Message(args)

    class _Resp:
        def __init__(self, args):
            self.choices = [_Choice(args)]

    def completion(**kwargs):
        return _Resp(response_args_json)

    fake_litellm.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)


def test_brief_no_candidate_short_circuits(mod):
    candidate = {"has_candidate": False, "text": "NO_CANDIDATE: nothing found"}
    # required_resource_keys={"github"} is validated on direct invocation even though
    # the no-candidate path never calls it -- a dummy resource satisfies the check.
    result = mod.prospect_recon_brief(dg.build_asset_context(resources={"github": object()}), {"text": "[]"}, candidate)
    assert result["schema_source"] == "no_candidate"
    assert result["extracted"]["company"] == "none"


def test_brief_uses_live_template_schema(mod, monkeypatch):
    gh = _FakeGithub(files={"briefs/_TEMPLATE.md": {"content": "## Use Case\n\n## Risks\n", "sha": "s1"}})
    args = json.dumps({"company": "Acme", "use_case": "lineage", "risks": "none", "markdown_body": "# Brief"})
    _install_fake_litellm(monkeypatch, args)
    monkeypatch.setattr(mod, "_call_governance_tool", lambda *a, **k: {"receipt_id": "r1"})
    candidate = {"has_candidate": True, "company": "Acme", "domain": "acme.com", "demo_date": "2026-11-05"}
    result = mod.prospect_recon_brief(dg.build_asset_context(resources={"github": gh}), {"text": "[]"}, candidate)
    assert result["schema_source"] == "live_template"
    assert result["extracted"]["company"] == "Acme"


def test_brief_falls_back_when_template_missing(mod, monkeypatch):
    gh = _FakeGithub(files={})
    args = json.dumps({"company": "Acme", "use_case": "x", "next_steps": [], "markdown_body": "body"})
    _install_fake_litellm(monkeypatch, args)
    monkeypatch.setattr(mod, "_call_governance_tool", lambda *a, **k: {"receipt_id": "r1"})
    candidate = {"has_candidate": True, "company": "Acme", "domain": "acme.com", "demo_date": "TBD"}
    result = mod.prospect_recon_brief(dg.build_asset_context(resources={"github": gh}), {"text": "[]"}, candidate)
    assert result["schema_source"] == "fallback"


# ── prospect_recon_brief: FastMCP context-governance receipt call ──────────
# (mocked at the _call_governance_tool boundary -- this function is the one spot
# that spawns a real MCP server subprocess, so it's the mock boundary, not litellm)

def test_brief_records_governance_receipt_on_success(mod, monkeypatch):
    gh = _FakeGithub(files={})
    args = json.dumps({"company": "Acme", "use_case": "x", "next_steps": [], "markdown_body": "body"})
    _install_fake_litellm(monkeypatch, args)
    calls = []

    def fake_governance(tool_name, tool_args, **kwargs):
        calls.append((tool_name, tool_args))
        return {"receipt_id": "receipt-123"}

    monkeypatch.setattr(mod, "_call_governance_tool", fake_governance)
    candidate = {"has_candidate": True, "company": "Acme", "domain": "acme.com", "demo_date": "TBD", "slug": "acme"}
    result = mod.prospect_recon_brief(dg.build_asset_context(resources={"github": gh}), {"text": "[]", "n_items": 3}, candidate)
    assert result["governance_receipt_id"] == "receipt-123"
    assert calls[0][0] == "write_receipt"
    assert calls[0][1]["context_name"] == "prospect_recon:acme"
    assert len(calls[0][1]["item_ids"]) == 3


def test_brief_governance_failure_is_best_effort_not_fatal(mod, monkeypatch):
    gh = _FakeGithub(files={})
    args = json.dumps({"company": "Acme", "use_case": "x", "next_steps": [], "markdown_body": "body"})
    _install_fake_litellm(monkeypatch, args)

    def broken_governance(*a, **k):
        raise RuntimeError("governance server unreachable")

    monkeypatch.setattr(mod, "_call_governance_tool", broken_governance)
    candidate = {"has_candidate": True, "company": "Acme", "domain": "acme.com", "demo_date": "TBD", "slug": "acme"}
    result = mod.prospect_recon_brief(dg.build_asset_context(resources={"github": gh}), {"text": "[]"}, candidate)
    assert result["governance_receipt_id"] is None
    assert result["extracted"]["company"] == "Acme"  # brief itself still succeeded


# ── brief markdown / path rendering ─────────────────────────────────────────

def test_brief_path_uses_demo_date_when_dated(mod):
    candidate = {"demo_date": "2026-11-05T10:00:00Z", "slug": "acme"}
    assert mod.brief_path_for(candidate) == "briefs/2026-11-05-acme.md"


def test_brief_path_uses_today_when_tbd(mod):
    import datetime as dt
    candidate = {"demo_date": "TBD", "slug": "acme"}
    expected = f"briefs/{dt.date.today().isoformat()}-acme.md"
    assert mod.brief_path_for(candidate) == expected


def test_render_brief_markdown_has_frontmatter(mod):
    extracted = {"company": "Acme", "markdown_body": "# Demo Thesis\n\ntext"}
    candidate = {"demo_date": "2026-11-05", "slug": "acme"}
    rendered = mod.render_brief_markdown(extracted, candidate)
    # Real frontmatter shape, confirmed against a live brief + _TEMPLATE.md: quoted
    # string fields, company/slug/domain/demo_date/demo_time/attendees/ae_doc/
    # ae_doc_modified/overall_confidence/generated.
    assert rendered.startswith('---\ncompany: "Acme"\n')
    assert 'slug: "acme"' in rendered
    assert "attendees: []" in rendered  # no candidate["candidate"]["attendees"] given
    assert "# Demo Thesis" in rendered


def test_render_brief_markdown_includes_attendees_block(mod):
    extracted = {"company": "Acme", "markdown_body": "body"}
    candidate = {"demo_date": "2026-11-05", "slug": "acme", "candidate": {"attendees": ["a@acme.com", "b@acme.com"]}}
    rendered = mod.render_brief_markdown(extracted, candidate)
    assert "attendees:\n" in rendered
    assert '  - "a@acme.com"' in rendered
    assert '  - "b@acme.com"' in rendered


# ── commit_brief_and_ledger: direct push + conflict fallback ───────────────

def test_commit_direct_push_succeeds(mod):
    gh = _FakeGithub()
    candidate = {"slug": "acme", "company": "Acme", "demo_date": "2026-11-05", "request_file": None}
    extracted = {"company": "Acme", "markdown_body": "body"}
    result = mod.commit_brief_and_ledger(gh, candidate, extracted)
    assert result["branch"] == "main"
    assert result["pr_number"] is None
    assert any(w["path"] == "briefs/2026-11-05-acme.md" for w in gh.writes)
    # Real ledger.json shape: two buckets, each a list of entries keyed by slug/company.
    ledger = json.loads(gh.files["state/ledger.json"]["content"])
    assert ledger["brief-ready"][0]["status"] == "brief-ready"
    assert ledger["brief-ready"][0]["slug"] == "acme"
    assert ledger["built"] == []


def test_commit_moves_slug_out_of_built_bucket_on_rebuild(mod):
    """A rebuild-brief request for a previously-deployed prospect resets its ledger
    entry back to brief-ready, removed from `built`."""
    gh = _FakeGithub()
    gh.files["state/ledger.json"] = {
        "content": json.dumps({"brief-ready": [], "built": [{"slug": "acme", "company": "Acme", "status": "deployed"}]}),
        "sha": "ls1",
    }
    candidate = {"slug": "acme", "company": "Acme", "demo_date": "2026-11-05", "request_file": None}
    extracted = {"company": "Acme", "markdown_body": "body"}
    mod.commit_brief_and_ledger(gh, candidate, extracted)
    ledger = json.loads(gh.files["state/ledger.json"]["content"])
    assert ledger["built"] == []
    assert len(ledger["brief-ready"]) == 1
    assert ledger["brief-ready"][0]["slug"] == "acme"


def test_commit_falls_back_to_branch_pr_merge_on_conflict(mod):
    gh = _FakeGithub()
    gh.fail_write_on_branch = "main"
    candidate = {"slug": "acme", "company": "Acme", "demo_date": "2026-11-05", "request_file": None}
    extracted = {"company": "Acme", "markdown_body": "body"}
    result = mod.commit_brief_and_ledger(gh, candidate, extracted)
    assert result["branch"] != "main"
    assert result["pr_number"] == 1
    assert gh.branches_created == [result["branch"]]
    assert gh.prs_merged == [1]


def test_commit_archives_consumed_request_file(mod):
    gh = _FakeGithub()
    request_file = {"path": "requests/acme.md", "sha": "rs1", "content": "---\naction: rebuild-brief\n---\n"}
    candidate = {"slug": "acme", "company": "Acme", "demo_date": "2026-11-05", "request_file": request_file}
    extracted = {"company": "Acme", "markdown_body": "body"}
    mod.commit_brief_and_ledger(gh, candidate, extracted)
    assert gh.deletes and gh.deletes[0]["path"] == "requests/acme.md"
    assert any("requests/done/" in w["path"] for w in gh.writes)


# ── prospect_recon_commit_brief asset: no-candidate short-circuit ──────────

def test_commit_brief_asset_short_circuits_on_no_candidate(mod):
    candidate = {"has_candidate": False, "reason": "no candidates from either signal"}
    df = mod.prospect_recon_commit_brief(dg.build_asset_context(resources={"github": _FakeGithub()}), candidate, {})
    assert df.iloc[0]["company"] == "none"
    assert "no candidates" in df.iloc[0]["thesis"]
