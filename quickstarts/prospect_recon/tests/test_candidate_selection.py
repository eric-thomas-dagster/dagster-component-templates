"""Deepest test coverage in this quickstart, per plan: one test per exclusion
rule, the dangling-ledger fallback, request-file precedence, and the full
tie-break ordering -- this is where a subtle bug would silently pick the
wrong prospect."""
import datetime as dt

import pandas as pd
import pytest


OWN_DOMAIN = "dagsterlabs.com"


def _event(summary, attendee_domain="acme.com", recurring=False, start="2026-11-05T10:00:00Z",
           end="2026-11-05T10:30:00Z", description=""):
    return {
        "id": f"ev-{summary}", "summary": summary, "description": description,
        "status": "confirmed", "start": start, "end": end,
        "attendees": [f"me@{OWN_DOMAIN}", f"them@{attendee_domain}"] if attendee_domain else [f"me@{OWN_DOMAIN}"],
        "recurringEventId": "rec-1" if recurring else None,
        "html_link": f"https://cal/{summary}",
    }


def _calendar_df(*events):
    return pd.DataFrame(list(events))


def _drive_row(fid, name, modified_time="2026-11-01T00:00:00Z"):
    return {"id": fid, "name": name, "mimeType": "application/pdf", "modifiedTime": modified_time,
            "webViewLink": f"https://drive/{fid}"}


def _drive_df(*rows):
    return pd.DataFrame(list(rows))


# ── Signal A: calendar exclusion rules ──────────────────────────────────────

def test_calendar_recurring_event_excluded(mod):
    df = _calendar_df(_event("Weekly Architecture Review", recurring=True))
    candidates, skipped = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger={})
    assert candidates == []
    assert skipped[0]["reason"] == "recurring"


@pytest.mark.parametrize("title", [
    "POC Weekly Check-In", "Acme Sync", "Standup", "Q3 QBR", "Renewal discussion",
    "Onboarding session", "Training", "Implementation kickoff", "Office hours",
])
def test_calendar_keyword_excluded(mod, title):
    df = _calendar_df(_event(title))
    candidates, skipped = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger={})
    assert candidates == []
    assert skipped[0]["reason"].startswith("keyword:")


def test_calendar_no_external_attendee_excluded(mod):
    df = _calendar_df(_event("Internal planning", attendee_domain=None))
    candidates, skipped = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger={})
    assert candidates == []
    assert skipped[0]["reason"] == "no_external_attendee"


def test_calendar_already_deployed_excluded(mod):
    df = _calendar_df(_event("Acme Demo", attendee_domain="acme.com"))
    # Real ledger.json shape: two buckets, entries keyed by slug/company (no domain field) --
    # "built" bucket entries are the ones already deployed, matched by slug.
    ledger = {"built": [{"slug": "acme", "company": "Acme", "status": "deployed"}]}
    candidates, skipped = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger=ledger)
    assert candidates == []
    assert skipped[0]["reason"] == "already_deployed"


def test_calendar_valid_candidate_kept(mod):
    df = _calendar_df(_event("Acme Corp Technical Deep-Dive", attendee_domain="acme.com"))
    candidates, skipped = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger={})
    assert skipped == []
    assert len(candidates) == 1
    c = candidates[0]
    assert c["domain"] == "acme.com"
    assert c["company"] == "acme"
    assert c["dated"] is True
    assert c["source"] == "calendar"


def test_calendar_company_derived_from_domain_not_title(mod):
    df = _calendar_df(_event("Totally Unrelated Title", attendee_domain="globex.io"))
    candidates, _ = mod.filter_calendar_candidates(df, own_domain=OWN_DOMAIN, ledger={})
    assert candidates[0]["domain"] == "globex.io"
    assert candidates[0]["company"] == "globex"


def test_calendar_empty_df(mod):
    candidates, skipped = mod.filter_calendar_candidates(pd.DataFrame(), own_domain=OWN_DOMAIN, ledger={})
    assert candidates == [] and skipped == []
    candidates, skipped = mod.filter_calendar_candidates(None, own_domain=OWN_DOMAIN, ledger={})
    assert candidates == [] and skipped == []


# ── Signal B: drive dedup / company guess / deployed exclusion ─────────────

def test_drive_dedup_by_file_id_across_frames(mod):
    recent = _drive_df(_drive_row("f1", "Acme Discovery Notes.pdf"))
    search = _drive_df(_drive_row("f1", "Acme Discovery Notes.pdf"), _drive_row("f2", "Globex Prep.pdf"))
    candidates = mod.filter_drive_candidates(recent, search, ledger={})
    assert {c["file_id"] for c in candidates} == {"f1", "f2"}


def test_drive_company_guess_from_filename(mod):
    """No `content` column at all (as real non-text-MIME Drive rows have) --
    falls straight to the filename heuristic, no LLM call attempted."""
    df = _drive_df(_drive_row("f1", "Acme Discovery Notes.pdf"))
    candidates = mod.filter_drive_candidates(df, None, ledger={})
    assert candidates[0]["company"] == "Acme"
    assert candidates[0]["domain"] == "acme.com"
    assert candidates[0]["dated"] is False
    assert candidates[0]["demo_date"] == "TBD"
    assert candidates[0]["company_attribution_confidence"] == "low"


def test_drive_no_company_guess_skipped(mod):
    df = _drive_df(_drive_row("f1", "notes draft final v2.pdf"))
    candidates = mod.filter_drive_candidates(df, None, ledger={})
    assert candidates == []


def test_drive_already_deployed_excluded(mod):
    df = _drive_df(_drive_row("f1", "Acme Discovery Notes.pdf"))
    ledger = {"built": [{"slug": "acme", "company": "Acme", "status": "deployed"}]}
    candidates = mod.filter_drive_candidates(df, None, ledger=ledger)
    assert candidates == []


# ── Signal B: LLM-driven company attribution from Drive file content ───────

def _install_fake_litellm_classify(monkeypatch, company, confidence, domain=None):
    import json as _json
    import sys
    import types

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

    args = _json.dumps({"company": company, "confidence": confidence, **({"domain": domain} if domain else {})})

    def completion(**kwargs):
        return _Resp(args)

    fake_litellm.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)


def test_classify_drive_company_high_confidence_content_wins_over_filename(mod, monkeypatch):
    _install_fake_litellm_classify(monkeypatch, "Globex Corporation", "high", domain="globex.io")
    company, domain, confidence = mod._classify_drive_company(
        "Prepared for Globex Corporation discovery call.", "notes draft final v2.pdf", ["globex.io"],
    )
    assert company == "Globex Corporation"
    assert domain == "globex.io"
    assert confidence == "high"


def test_classify_drive_company_low_confidence_falls_back_to_filename(mod, monkeypatch):
    _install_fake_litellm_classify(monkeypatch, "", "none")
    company, domain, confidence = mod._classify_drive_company(
        "Some ambiguous internal memo with no company name.", "Acme Discovery Notes.pdf", [],
    )
    assert company == "Acme"
    assert confidence == "low"


def test_classify_drive_company_llm_failure_falls_back_to_filename(mod, monkeypatch):
    import sys
    import types

    def _broken_completion(**kwargs):
        raise RuntimeError("network down")

    fake_litellm = types.ModuleType("litellm")
    fake_litellm.completion = _broken_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)

    company, domain, confidence = mod._classify_drive_company("some content", "Acme Discovery Notes.pdf", [])
    assert company == "Acme"
    assert confidence == "low"


def test_classify_drive_company_no_content_skips_llm_entirely(mod):
    company, domain, confidence = mod._classify_drive_company(None, "Acme Discovery Notes.pdf", [])
    assert company == "Acme"
    assert confidence == "low"
    company, domain, confidence = mod._classify_drive_company("   ", "Acme Discovery Notes.pdf", [])
    assert company == "Acme"
    assert confidence == "low"


def test_drive_candidates_uses_content_based_classification_when_present(mod, monkeypatch):
    _install_fake_litellm_classify(monkeypatch, "Globex Corporation", "high", domain="globex.io")
    df = _drive_df({
        "id": "f1", "name": "notes draft final v2.pdf", "mimeType": "text/plain",
        "modifiedTime": "2026-11-01T00:00:00Z", "webViewLink": "https://drive/f1",
        "content": "Prepared for Globex Corporation discovery call.",
    })
    candidates = mod.filter_drive_candidates(df, None, ledger={}, known_domains=["globex.io"])
    assert candidates[0]["company"] == "Globex Corporation"
    assert candidates[0]["domain"] == "globex.io"
    assert candidates[0]["company_attribution_confidence"] == "high"


def test_drive_empty_frames(mod):
    assert mod.filter_drive_candidates(None, None, ledger={}) == []
    assert mod.filter_drive_candidates(pd.DataFrame(), pd.DataFrame(), ledger={}) == []


# ── dedup + tie-break ────────────────────────────────────────────────────────

def test_select_dated_upcoming_wins_over_undated(mod):
    now = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    cal = [{"source": "calendar", "domain": "acme.com", "company": "acme", "demo_date": "2026-11-10T10:00:00Z",
            "dated": True, "duration_minutes": 30, "external_attendee_count": 2}]
    drive = [{"source": "drive", "domain": "globex.io", "company": "globex", "demo_date": "TBD", "dated": False,
              "modified_time": "2026-10-31T00:00:00Z"}]
    picked = mod.select_candidate(cal, drive, now=now)
    assert picked["domain"] == "acme.com"


def test_select_undated_wins_over_dated_passed(mod):
    now = dt.datetime(2026, 11, 15, tzinfo=dt.timezone.utc)
    cal = [{"source": "calendar", "domain": "acme.com", "company": "acme", "demo_date": "2026-11-01T10:00:00Z",
            "dated": True, "duration_minutes": 30, "external_attendee_count": 2}]
    drive = [{"source": "drive", "domain": "globex.io", "company": "globex", "demo_date": "TBD", "dated": False,
              "modified_time": "2026-11-10T00:00:00Z"}]
    picked = mod.select_candidate(cal, drive, now=now)
    assert picked["domain"] == "globex.io"


def test_select_dated_upcoming_soonest_first(mod):
    now = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    cal = [
        {"source": "calendar", "domain": "later.com", "company": "later", "demo_date": "2026-11-20T10:00:00Z",
         "dated": True, "duration_minutes": 30, "external_attendee_count": 1},
        {"source": "calendar", "domain": "sooner.com", "company": "sooner", "demo_date": "2026-11-05T10:00:00Z",
         "dated": True, "duration_minutes": 30, "external_attendee_count": 1},
    ]
    picked = mod.select_candidate(cal, [], now=now)
    assert picked["domain"] == "sooner.com"


def test_select_undated_most_recently_shared_first(mod):
    drive = [
        {"source": "drive", "domain": "old.com", "company": "old", "demo_date": "TBD", "dated": False,
         "modified_time": "2026-10-01T00:00:00Z"},
        {"source": "drive", "domain": "new.com", "company": "new", "demo_date": "TBD", "dated": False,
         "modified_time": "2026-10-20T00:00:00Z"},
    ]
    picked = mod.select_candidate([], drive, now=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc))
    assert picked["domain"] == "new.com"


def test_select_tiebreak_by_duration_then_attendees(mod):
    now = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    cal = [
        {"source": "calendar", "domain": "short.com", "company": "short", "demo_date": "2026-11-05T10:00:00Z",
         "dated": True, "duration_minutes": 15, "external_attendee_count": 5},
        {"source": "calendar", "domain": "long.com", "company": "long", "demo_date": "2026-11-05T10:00:00Z",
         "dated": True, "duration_minutes": 60, "external_attendee_count": 1},
    ]
    picked = mod.select_candidate(cal, [], now=now)
    assert picked["domain"] == "long.com"  # same demo_date -> longer meeting wins


def test_select_dedup_prefers_calendar_over_drive_same_domain(mod):
    now = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    cal = [{"source": "calendar", "domain": "acme.com", "company": "acme", "demo_date": "2026-11-05T10:00:00Z",
            "dated": True, "duration_minutes": 30, "external_attendee_count": 2}]
    drive = [{"source": "drive", "domain": "acme.com", "company": "acme", "demo_date": "TBD", "dated": False,
              "modified_time": "2026-11-01T00:00:00Z"}]
    picked = mod.select_candidate(cal, drive, now=now)
    assert picked["source"] == "calendar"


def test_select_no_candidates_returns_none(mod):
    assert mod.select_candidate([], [], now=dt.datetime.now(dt.timezone.utc)) is None


# ── request-file / dangling-ledger precedence ──────────────────────────────

def test_parse_frontmatter_valid(mod):
    text = "---\naction: rebuild-brief\ncompany: Acme\ndemo_date: 2026-11-05\n---\n\nRewrite notes here."
    fm = mod.parse_frontmatter(text)
    assert fm == {"action": "rebuild-brief", "company": "Acme", "demo_date": "2026-11-05"}


def test_parse_frontmatter_missing_returns_empty(mod):
    assert mod.parse_frontmatter("just a plain markdown file, no frontmatter") == {}
    assert mod.parse_frontmatter("") == {}


class _FakeGithub:
    def __init__(self, files=None, directories=None):
        self.files = files or {}  # path -> {"content": str, "sha": str}
        self.directories = directories or {}  # dir path -> [{"name","path","type"}]
        self.writes = []
        self.deletes = []
        self.branches_created = []
        self.prs_created = []
        self.prs_merged = []
        self.fail_write_on_branch = None  # branch name on which writes should raise

    def get_file_contents(self, repo, path, ref="main"):
        return self.files.get(path)

    def list_directory(self, repo, path, ref="main"):
        return self.directories.get(path, [])

    def create_or_update_file_contents(self, repo, path, content, message, branch="main", sha=None):
        if branch == self.fail_write_on_branch:
            raise RuntimeError("409 conflict (simulated)")
        self.writes.append({"path": path, "content": content, "branch": branch, "sha": sha})
        new_sha = f"sha-{len(self.writes)}"
        self.files[path] = {"content": content, "sha": new_sha}
        return {"content": {"sha": new_sha}}

    def delete_file_contents(self, repo, path, sha, message, branch="main"):
        self.deletes.append({"path": path, "sha": sha, "branch": branch})
        self.files.pop(path, None)

    def create_branch(self, repo, new_branch, from_branch="main"):
        self.branches_created.append(new_branch)
        return {"ref": f"refs/heads/{new_branch}"}

    def create_pull_request(self, repo, title, head, base="main", body=""):
        pr = {"number": len(self.prs_created) + 1, "title": title, "head": head, "base": base}
        self.prs_created.append(pr)
        return pr

    def merge_pull_request(self, repo, pr_number, merge_method="squash"):
        self.prs_merged.append(pr_number)
        return {"merged": True}


def test_find_request_candidate_ignores_readme_and_done(mod):
    gh = _FakeGithub(
        directories={"requests": [
            {"name": "README.md", "path": "requests/README.md", "type": "file"},
            {"name": "acme.md", "path": "requests/acme.md", "type": "file"},
        ]},
        files={"requests/acme.md": {"content": "---\naction: rebuild-brief\ncompany: Acme\ndemo_date: 2026-11-05\n---\nnotes", "sha": "s1"}},
    )
    req = mod.find_request_candidate(gh)
    assert req is not None
    assert req["path"] == "requests/acme.md"
    assert req["frontmatter"]["company"] == "Acme"


def test_find_request_candidate_picks_nearest_demo_date(mod):
    gh = _FakeGithub(
        directories={"requests": [
            {"name": "far.md", "path": "requests/far.md", "type": "file"},
            {"name": "near.md", "path": "requests/near.md", "type": "file"},
        ]},
        files={
            "requests/far.md": {"content": "---\naction: rebuild-brief\ncompany: Far\ndemo_date: 2026-12-01\n---\n", "sha": "s1"},
            "requests/near.md": {"content": "---\naction: rebuild-brief\ncompany: Near\ndemo_date: 2026-11-02\n---\n", "sha": "s2"},
        },
    )
    req = mod.find_request_candidate(gh)
    assert req["frontmatter"]["company"] == "Near"


def test_find_request_candidate_ignores_non_rebuild_action(mod):
    gh = _FakeGithub(
        directories={"requests": [{"name": "x.md", "path": "requests/x.md", "type": "file"}]},
        files={"requests/x.md": {"content": "---\naction: deploy\ncompany: X\n---\n", "sha": "s1"}},
    )
    assert mod.find_request_candidate(gh) is None


def test_find_dangling_ledger_entry_detects_missing_brief(mod):
    gh = _FakeGithub(files={})  # brief file does not exist
    # Real shape: "brief-ready" bucket is a LIST of entries, not a flat dict keyed by slug.
    ledger = {"brief-ready": [{"slug": "acme", "status": "brief-ready", "brief_path": "briefs/2026-11-05-acme.md", "company": "Acme"}]}
    dangling = mod.find_dangling_ledger_entry(ledger, gh)
    assert dangling is not None
    assert dangling["slug"] == "acme"


def test_find_dangling_ledger_entry_none_when_brief_exists(mod):
    gh = _FakeGithub(files={"briefs/2026-11-05-acme.md": {"content": "x", "sha": "s1"}})
    ledger = {"brief-ready": [{"slug": "acme", "status": "brief-ready", "brief_path": "briefs/2026-11-05-acme.md"}]}
    assert mod.find_dangling_ledger_entry(ledger, gh) is None


def test_find_dangling_ledger_entry_ignores_built_bucket(mod):
    """A "built" (deployed) entry is never dangling -- find_dangling_ledger_entry only
    ever reads the "brief-ready" bucket, regardless of any per-entry status string."""
    gh = _FakeGithub(files={})
    ledger = {"built": [{"slug": "acme", "status": "deployed", "brief_path": "briefs/x.md"}]}
    assert mod.find_dangling_ledger_entry(ledger, gh) is None


# ── prospect_recon_candidate asset: direct invocation ───────────────────────

def _ctx(github):
    import dagster as dg

    return dg.build_asset_context(resources={"github": github})


def test_asset_honors_request_file_over_signals(mod):
    gh = _FakeGithub(
        directories={"requests": [{"name": "acme.md", "path": "requests/acme.md", "type": "file"}]},
        files={"requests/acme.md": {"content": "---\naction: rebuild-brief\ncompany: Acme\nslug: acme\ndemo_date: 2026-11-05\n---\nnotes", "sha": "s1"}},
    )
    calendar = _calendar_df(_event("Globex Demo", attendee_domain="globex.io"))
    result = mod.prospect_recon_candidate(_ctx(gh), calendar, pd.DataFrame(), pd.DataFrame())
    assert result["has_candidate"] is True
    assert result["source_kind"] == "request"
    assert result["company"] == "Acme"


def test_asset_honors_dangling_ledger_when_no_request(mod):
    gh = _FakeGithub(directories={"requests": []}, files={})
    ledger_json = (
        '{"brief-ready": [{"slug": "acme", "status": "brief-ready", "brief_path": "briefs/missing.md", '
        '"company": "Acme", "demo_date": "TBD"}], "built": []}'
    )
    gh.files["state/ledger.json"] = {"content": ledger_json, "sha": "ls1"}
    result = mod.prospect_recon_candidate(_ctx(gh), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert result["has_candidate"] is True
    assert result["source_kind"] == "dangling_ledger"
    assert result["slug"] == "acme"


def test_asset_falls_through_to_signals_and_picks_best(mod):
    gh = _FakeGithub(directories={"requests": []}, files={})
    calendar = _calendar_df(_event("Acme Corp Demo", attendee_domain="acme.com"))
    result = mod.prospect_recon_candidate(_ctx(gh), calendar, pd.DataFrame(), pd.DataFrame())
    assert result["has_candidate"] is True
    assert result["domain"] == "acme.com"
    assert result["slug"] == "acme"


def test_asset_no_candidate_when_everything_excluded(mod):
    gh = _FakeGithub(directories={"requests": []}, files={})
    calendar = _calendar_df(_event("Weekly Standup", recurring=True))
    result = mod.prospect_recon_candidate(_ctx(gh), calendar, pd.DataFrame(), pd.DataFrame())
    assert result["has_candidate"] is False
    assert "reason" in result
    assert result["text"].startswith("NO_CANDIDATE")
