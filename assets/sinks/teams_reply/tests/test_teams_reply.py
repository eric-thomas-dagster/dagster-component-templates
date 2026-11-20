"""Real, committed tests for TeamsReplyComponent -- real dg.materialize() for the success
path, a real failed run (not mocked) feeding a real RunStatusSensorContext for the failure
path. Mocks only the unavoidable external calls (requests.get/requests.post for Graph + OAuth)."""
import json
import sqlite3

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


mod_cache = {}


@pytest.fixture(autouse=True)
def _cache_mod(mod):
    mod_cache["mod"] = mod
    yield


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def _install_fakes(monkeypatch, *, post_fail=None, reactions_by_reply=None, files_folder_fail=None, upload_fail=None, patch_fail=None):
    import requests

    reactions_by_reply = reactions_by_reply or {}
    calls = {"post": [], "get": [], "put": [], "patch": []}
    counter = {"n": 0}

    def fake_patch(url, **kwargs):
        calls["patch"].append((url, kwargs))
        if patch_fail:
            raise RuntimeError(patch_fail)
        return _Resp({})

    def fake_post(url, **kwargs):
        calls["post"].append((url, kwargs))
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        if url.endswith("/replies"):
            if post_fail:
                raise RuntimeError(post_fail)
            counter["n"] += 1
            return _Resp({"id": f"reply{counter['n']}"})
        return _Resp({})

    def fake_get(url, **kwargs):
        calls["get"].append((url, kwargs))
        if "/users/" in url:
            return _Resp({"mail": None, "userPrincipalName": None})
        if url.endswith("/filesFolder"):
            if files_folder_fail:
                raise RuntimeError(files_folder_fail)
            return _Resp({"id": "folder1", "parentReference": {"driveId": "drive1"}})
        if url.endswith("/replies"):
            replies = [
                {"id": rid, "reactions": reactions}
                for rid, reactions in reactions_by_reply.items()
            ]
            return _Resp({"value": replies})
        return _Resp({"value": []})

    def fake_put(url, **kwargs):
        calls["put"].append((url, kwargs))
        if upload_fail:
            raise RuntimeError(upload_fail)
        return _Resp({"id": "item1", "webUrl": "https://sharepoint.example/chart.png"})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "put", fake_put)
    monkeypatch.setattr(requests, "patch", fake_patch)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")
    return calls


def _upstream_asset(output):
    @dg.asset(key=dg.AssetKey(["chat_agent"]))
    def chat_agent():
        return output
    return chat_agent


def _component(tmp_path, **overrides):
    defaults = dict(asset_name="chat_reply", upstream_asset_key="chat_agent", oauth_state_dir=str(tmp_path / "state"))
    defaults.update(overrides)
    return mod_cache["mod"].TeamsReplyComponent(**defaults)


def _materialize(agent_output, component, monkeypatch, tmp_path, *, tags=None, post_fail=None, reactions_by_reply=None, files_folder_fail=None, upload_fail=None, patch_fail=None):
    calls = _install_fakes(
        monkeypatch, post_fail=post_fail, reactions_by_reply=reactions_by_reply,
        files_folder_fail=files_folder_fail, upload_fail=upload_fail, patch_fail=patch_fail,
    )
    upstream = _upstream_asset(agent_output)
    defs = component.build_defs(None)
    result = dg.materialize(
        [upstream, *defs.assets], resources={"io_manager": dg.InMemoryIOManager()}, tags=tags or {},
        raise_on_error=False,
    )
    return result, calls


TEAMS_TAGS = {"teams/team_id": "T1", "teams/channel_id": "Ch1", "teams/thread_id": "root1"}


# --- markdown stripping ------------------------------------------------------------------

def test_strip_markdown_bold_and_underscore_bold(mod):
    assert mod._strip_markdown("**bold**") == "bold"
    assert mod._strip_markdown("__also bold__") == "also bold"


def test_strip_markdown_links_keep_both_text_and_url(mod):
    assert mod._strip_markdown("[Dagster](https://dagster.io)") == "Dagster (https://dagster.io)"


def test_strip_markdown_headings(mod):
    assert mod._strip_markdown("# Big Heading") == "Big Heading"
    assert mod._strip_markdown("### Smaller Heading") == "Smaller Heading"


def test_strip_markdown_bullet_lists(mod):
    text = "* first\n- second"
    assert mod._strip_markdown(text) == "• first\n• second"


def test_strip_markdown_leaves_code_fences_and_inline_code_untouched(mod):
    text = "Use **bold** text. Code:\n```python\n**not bold** [not](a-link)\n```\nand `inline **code**` too."
    stripped = mod._strip_markdown(text)
    assert "```python\n**not bold** [not](a-link)\n```" in stripped
    assert "`inline **code**`" in stripped
    assert "Use bold text" in stripped


def test_strip_markdown_empty_text_is_a_no_op(mod):
    assert mod._strip_markdown("") == ""


# --- success path ----------------------------------------------------------------------

def test_posts_final_answer_to_the_tagged_thread(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize(
        {"final_answer": "The nightly etl failed twice this week."}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS,
    )
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert len(reply_posts) == 1
    assert reply_posts[0][0] == "https://graph.microsoft.com/v1.0/teams/T1/channels/Ch1/messages/root1/replies"
    assert reply_posts[0][1]["json"]["body"]["content"] == "The nightly etl failed twice this week."


def test_markdown_is_stripped_before_posting_by_default(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize(
        {"final_answer": "**Status**: done. See [the run](https://example.com)."},
        component, monkeypatch, tmp_path, tags=TEAMS_TAGS,
    )
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts[0][1]["json"]["body"]["content"] == "Status: done. See the run (https://example.com)."


def test_convert_markdown_false_posts_the_answer_verbatim(monkeypatch, tmp_path):
    component = _component(tmp_path, convert_markdown=False)
    result, calls = _materialize(
        {"final_answer": "**Status**: done."}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS,
    )
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts[0][1]["json"]["body"]["content"] == "**Status**: done."


def test_missing_teams_tags_fails_the_run_clearly(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags={})
    assert not result.success
    assert not any(c[0].endswith("/replies") for c in calls["post"])


def test_answer_field_override(monkeypatch, tmp_path):
    component = _component(tmp_path, answer_field="summary")
    result, calls = _materialize(
        {"summary": "custom field text", "final_answer": "ignored"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS,
    )
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts[0][1]["json"]["body"]["content"] == "custom field text"


def test_plain_string_upstream_output_is_used_directly(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize("just a plain string answer", component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts[0][1]["json"]["body"]["content"] == "just a plain string answer"


def test_long_answer_is_split_into_multiple_messages(monkeypatch, tmp_path):
    component = _component(tmp_path, max_chars_per_message=10, on_long_answer="split")
    long_text = "0123456789" * 3  # 30 chars
    result, calls = _materialize({"final_answer": long_text}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert len(reply_posts) == 3
    assert "".join(c[1]["json"]["body"]["content"].split("<br>")[0] for c in reply_posts) == long_text


def test_long_answer_truncate_mode_posts_one_message(monkeypatch, tmp_path):
    component = _component(tmp_path, max_chars_per_message=10, on_long_answer="truncate")
    result, calls = _materialize({"final_answer": "0123456789" * 3}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert len(reply_posts) == 1
    assert "truncated" in reply_posts[0][1]["json"]["body"]["content"]


def test_run_link_appended_when_webserver_base_url_set(monkeypatch, tmp_path):
    component = _component(tmp_path, webserver_base_url="http://localhost:3000")
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    content = reply_posts[0][1]["json"]["body"]["content"]
    assert '<a href="http://localhost:3000/runs/' in content and "View run" in content


def test_no_run_link_when_webserver_base_url_unset(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts[0][1]["json"]["body"]["content"] == "hi"


def test_secrets_in_the_answer_are_redacted(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize(
        {"final_answer": "here's a token: xoxb-1234567890-abcdefghij"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS,
    )
    assert result.success
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    content = reply_posts[0][1]["json"]["body"]["content"]
    assert "xoxb-1234567890" not in content
    assert "[REDACTED]" in content


def test_teams_post_failure_fails_the_run(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS, post_fail="channel_not_found",
    )
    assert not result.success


# --- failure-path run-status sensor -----------------------------------------------------

def test_component_emits_a_failure_watcher_sensor_by_default(mod, tmp_path):
    component = _component(tmp_path)
    defs = component.build_defs(None)
    assert [s.name for s in defs.sensors] == ["chat_reply_failure_watcher"]


def test_monitor_failures_false_emits_no_sensor(mod, tmp_path):
    component = _component(tmp_path, monitor_failures=False)
    defs = component.build_defs(None)
    assert not defs.sensors


def _real_failed_run_context(tags):
    @dg.asset
    def boom():
        raise ValueError("deliberate failure for a test")

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([boom], instance=instance, raise_on_error=False, tags=tags)
    run = instance.get_run_by_id(result.run_id)
    rec = instance.get_records_for_run(run.run_id, of_type=dg.DagsterEventType.RUN_FAILURE).records[0]
    ev = rec.event_log_entry.dagster_event
    return dg.build_run_status_sensor_context(sensor_name="chat_reply_failure_watcher", dagster_event=ev, dagster_instance=instance, dagster_run=run)


def test_failure_watcher_posts_an_error_reply_for_a_tagged_failed_run(monkeypatch, mod, tmp_path):
    component = _component(tmp_path)
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    calls = _install_fakes(monkeypatch)

    ctx = _real_failed_run_context(TEAMS_TAGS)
    sensor_def(ctx)
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert len(reply_posts) == 1
    assert reply_posts[0][0] == "https://graph.microsoft.com/v1.0/teams/T1/channels/Ch1/messages/root1/replies"
    assert "Something went wrong" in reply_posts[0][1]["json"]["body"]["content"]


def test_failure_watcher_ignores_a_failed_run_with_no_teams_tags(monkeypatch, mod, tmp_path):
    component = _component(tmp_path)
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    calls = _install_fakes(monkeypatch)

    ctx = _real_failed_run_context({})  # not a chat-agent run
    sensor_def(ctx)
    assert not any(c[0].endswith("/replies") for c in calls["post"])


# --- receipt-message-links: pure functions (real sqlite, no mocking) -------------------

def _seed_context_receipts(db_path, rows):
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE context_receipts (receipt_id TEXT, dagster_run_id TEXT)")
    conn.executemany("INSERT INTO context_receipts VALUES (?, ?)", rows)
    conn.execute("CREATE TABLE context_receipt_events (event_id TEXT, receipt_id TEXT, event_type TEXT, occurred_at TEXT, payload_json TEXT)")
    conn.commit()
    conn.close()


def test_record_and_look_up_message_links(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, ["r1", "r2"], platform="teams", channel="T1|Ch1", message_ts="root1|reply1")
    assert set(mod.receipt_ids_for_message(db_path, platform="teams", channel="T1|Ch1", message_ts="root1|reply1")) == {"r1", "r2"}
    assert mod.receipt_ids_for_message(db_path, platform="slack", channel="T1|Ch1", message_ts="root1|reply1") == []


def test_receipt_ids_for_run_reads_context_receipts(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a"), ("r2", "run-a"), ("r3", "run-b")])
    assert set(mod.receipt_ids_for_run(db_path, "run-a")) == {"r1", "r2"}
    assert mod.receipt_ids_for_run(db_path, "run-c") == []


def test_receipt_ids_for_run_returns_empty_when_table_does_not_exist(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    assert mod.receipt_ids_for_run(db_path, "run-a") == []


def test_record_outcome_writes_an_event_for_a_known_receipt(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_outcome(db_path, "r1", signal="thumbs_up", outcome_ref="jane@co.com")
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT payload_json FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()
    assert '"outcome_ref":"jane@co.com"' in row[0] and '"signal":"thumbs_up"' in row[0]


def test_record_outcome_unknown_signal_raises(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    with pytest.raises(ValueError, match="unknown signal"):
        mod.record_outcome(db_path, "r1", signal="made_up_signal")


# --- receipts_db_path wiring on the success-path asset ----------------------------------

def test_receipts_db_path_links_the_posted_reply_to_this_runs_receipts(monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(tmp_path, receipts_db_path=db_path)
    captured = {}

    def fake_receipt_ids_for_run(path, run_id):
        captured["lookup"] = (path, run_id)
        return ["r1", "r2"]

    def fake_record_message_links(path, receipt_ids, **kw):
        captured["linked"] = (path, receipt_ids, kw)

    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", fake_receipt_ids_for_run)
    monkeypatch.setattr(mod_cache["mod"], "record_message_links", fake_record_message_links)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    assert captured["lookup"][0] == db_path
    path, receipt_ids, kw = captured["linked"]
    assert path == db_path and receipt_ids == ["r1", "r2"]
    assert kw == {"platform": "teams", "channel": "T1|Ch1", "message_ts": "root1|reply1"}


def test_receipts_db_path_lookup_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(tmp_path, receipts_db_path=db_path)

    def boom(*a, **k):
        raise RuntimeError("db is locked")

    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", boom)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    assert len([c for c in calls["post"] if c[0].endswith("/replies")]) == 1


def test_receipts_db_path_unset_never_touches_the_db(monkeypatch, tmp_path):
    component = _component(tmp_path)
    called = []
    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", lambda *a, **k: called.append(1))
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success and called == []


# --- watch_reactions / rating_watcher sensor --------------------------------------------

def test_watch_reactions_requires_receipts_db_path(tmp_path):
    component = _component(tmp_path, watch_reactions=True)
    with pytest.raises(ValueError, match="watch_reactions requires receipts_db_path"):
        component.build_defs(None)


def test_watch_reactions_emits_a_rating_watcher_sensor_alongside_the_failure_watcher(tmp_path):
    component = _component(tmp_path, receipts_db_path=str(tmp_path / "r.sqlite3"), watch_reactions=True)
    defs = component.build_defs(None)
    assert sorted(s.name for s in defs.sensors) == ["chat_reply_failure_watcher", "chat_reply_rating_watcher"]


def _rating_sensor(component):
    defs = component.build_defs(None)
    return next(s for s in defs.sensors if s.name.endswith("_rating_watcher"))


def test_rating_watcher_records_thumbs_up_for_a_linked_reply(monkeypatch, mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_message_links(db_path, ["r1"], platform="teams", channel="T1|Ch1", message_ts="root1|reply1")

    component = _component(tmp_path, receipts_db_path=db_path, watch_reactions=True)
    sensor_def = _rating_sensor(component)
    _install_fakes(monkeypatch, reactions_by_reply={"reply1": [{"reactionType": "👍", "user": {"user": {"id": "U1"}}}]})
    result = sensor_def(dg.build_sensor_context(cursor=None))

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT payload_json FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()
    assert row is not None and '"signal":"thumbs_up"' in row[0]
    assert result.skip_reason is None


def test_rating_watcher_does_not_recount_an_unchanged_reaction(monkeypatch, mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_message_links(db_path, ["r1"], platform="teams", channel="T1|Ch1", message_ts="root1|reply1")

    component = _component(tmp_path, receipts_db_path=db_path, watch_reactions=True, resolve_rater_email=False)
    sensor_def = _rating_sensor(component)
    _install_fakes(monkeypatch, reactions_by_reply={"reply1": [{"reactionType": "👍", "user": {"user": {"id": "U1"}}}]})

    first = sensor_def(dg.build_sensor_context(cursor=None))
    second = sensor_def(dg.build_sensor_context(cursor=first.cursor))

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()[0]
    assert count == 1
    assert second.skip_reason is not None


def test_rating_watcher_skips_cleanly_when_no_links_yet(monkeypatch, mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(tmp_path, receipts_db_path=db_path, watch_reactions=True)
    sensor_def = _rating_sensor(component)
    _install_fakes(monkeypatch)
    result = sensor_def(dg.build_sensor_context(cursor=None))
    assert isinstance(result, dg.SkipReason)


# --- report_files_dir: uploading to the channel's Files folder -------------------------

def test_report_files_dir_uploads_and_links_every_generated_file(monkeypatch, tmp_path):
    component = _component(tmp_path, report_files_dir=str(tmp_path))
    (tmp_path / "chart.png").write_bytes(b"x")
    fake_manifest = [
        {"file_id": "f1", "name": "chart.png", "media_type": "image/png", "path": str(tmp_path / "chart.png")},
    ]
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: fake_manifest)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    assert any(c[0].endswith("/filesFolder") for c in calls["get"])
    uploads = [c for c in calls["put"] if c[0].endswith(":/chart.png:/content")]
    assert len(uploads) == 1
    assert uploads[0][0] == "https://graph.microsoft.com/v1.0/drives/drive1/items/folder1:/chart.png:/content"
    link_posts = [c for c in calls["post"] if c[0].endswith("/replies") and "sharepoint.example" in c[1]["json"]["body"]["content"]]
    assert len(link_posts) == 1


def test_report_files_dir_unset_never_touches_the_files_folder(monkeypatch, tmp_path):
    component = _component(tmp_path)
    called = []
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: called.append(1) or [])
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success and not called
    assert not any(c[0].endswith("/filesFolder") for c in calls["get"])


def test_report_files_dir_files_folder_lookup_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    component = _component(tmp_path, report_files_dir=str(tmp_path))
    fake_manifest = [{"file_id": "f1", "name": "chart.png", "media_type": "image/png", "path": str(tmp_path / "chart.png")}]
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: fake_manifest)
    result, calls = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS, files_folder_fail="boom",
    )
    assert result.success


def test_report_files_dir_upload_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    component = _component(tmp_path, report_files_dir=str(tmp_path))
    fake_manifest = [{"file_id": "f1", "name": "chart.png", "media_type": "image/png", "path": str(tmp_path / "chart.png")}]
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: fake_manifest)
    result, calls = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS, upload_fail="boom",
    )
    assert result.success


def test_report_manifest_read_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    component = _component(tmp_path, report_files_dir=str(tmp_path))

    def boom(*a, **k):
        raise RuntimeError("disk error")

    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", boom)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success


def test_read_run_manifest_reads_a_real_manifest_file(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}]')
    assert mod.read_run_manifest(str(tmp_path), "run_abc") == [{"file_id": "f1", "name": "x.png"}]


def test_lookup_report_file_finds_by_id(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}, {"file_id": "f2", "name": "y.pdf"}]')
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "f2")["name"] == "y.pdf"
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "nope") is None


# --- update_reaction_on_completion / placeholder_id -------------------------------------

def test_success_swaps_ack_reaction_for_done_reaction(monkeypatch, tmp_path):
    component = _component(tmp_path)
    tags = {**TEAMS_TAGS, "teams/message_id": "m1"}
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=tags)
    assert result.success
    unset = [c for c in calls["post"] if c[0].endswith("/messages/m1/unsetReaction")]
    setr = [c for c in calls["post"] if c[0].endswith("/messages/m1/setReaction")]
    assert len(unset) == 1 and unset[0][1]["json"] == {"reactionType": "👀"}
    assert len(setr) == 1 and setr[0][1]["json"] == {"reactionType": "✅"}


def test_update_reaction_on_completion_false_skips_reactions(monkeypatch, tmp_path):
    component = _component(tmp_path, update_reaction_on_completion=False)
    tags = {**TEAMS_TAGS, "teams/message_id": "m1"}
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=tags)
    assert result.success
    assert not [c for c in calls["post"] if "Reaction" in c[0]]


def test_no_message_id_tag_skips_reactions_without_erroring(monkeypatch, tmp_path):
    component = _component(tmp_path)
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=TEAMS_TAGS)
    assert result.success
    assert not [c for c in calls["post"] if "Reaction" in c[0]]


def test_placeholder_id_edits_the_existing_reply_instead_of_posting_a_new_one(monkeypatch, tmp_path):
    component = _component(tmp_path)
    tags = {**TEAMS_TAGS, "teams/placeholder_id": "ph1"}
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=tags)
    assert result.success
    assert len(calls["patch"]) == 1
    assert calls["patch"][0][0] == "https://graph.microsoft.com/v1.0/teams/T1/channels/Ch1/messages/root1/replies/ph1"
    assert calls["patch"][0][1]["json"]["body"]["content"] == "hi"
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts == []  # no NEW reply posted


def test_placeholder_id_falls_back_to_a_new_post_when_edit_fails(monkeypatch, tmp_path):
    component = _component(tmp_path)
    tags = {**TEAMS_TAGS, "teams/placeholder_id": "ph1"}
    result, calls = _materialize({"final_answer": "hi"}, component, monkeypatch, tmp_path, tags=tags, patch_fail="boom")
    assert result.success
    assert len(calls["patch"]) == 1
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert len(reply_posts) == 1  # fell back to a fresh reply


def test_failure_watcher_edits_placeholder_and_swaps_reaction(monkeypatch, tmp_path):
    component = _component(tmp_path)
    calls = _install_fakes(monkeypatch)
    import dagster as dg

    @dg.asset
    def boom():
        raise ValueError("deliberate failure for a test")

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([boom], instance=instance, raise_on_error=False, tags={**TEAMS_TAGS, "teams/message_id": "m1", "teams/placeholder_id": "ph1"})
    run = instance.get_run_by_id(result.run_id)
    rec = instance.get_records_for_run(run.run_id, of_type=dg.DagsterEventType.RUN_FAILURE).records[0]
    ev = rec.event_log_entry.dagster_event
    ctx = dg.build_run_status_sensor_context(sensor_name="chat_reply_failure_watcher", dagster_event=ev, dagster_instance=instance, dagster_run=run)
    defs = component.build_defs(None)
    (sensor_def,) = [s for s in defs.sensors if s.name.endswith("_failure_watcher")]
    sensor_def(ctx)
    assert len(calls["patch"]) == 1
    reply_posts = [c for c in calls["post"] if c[0].endswith("/replies")]
    assert reply_posts == []
    unset = [c for c in calls["post"] if c[0].endswith("/messages/m1/unsetReaction")]
    setr = [c for c in calls["post"] if c[0].endswith("/messages/m1/setReaction")]
    assert len(unset) == 1 and len(setr) == 1 and setr[0][1]["json"] == {"reactionType": "⚠️"}
