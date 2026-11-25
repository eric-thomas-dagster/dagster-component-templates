"""Real, committed tests for SlackReplyComponent -- real dg.materialize() for the success
path, a real failed run (not mocked) feeding a real RunStatusSensorContext for the failure
path. Mocks only `_get_slack_client`."""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeClient:
    def __init__(self, ok=True, error=None, upload_ok=True, update_ok=True):
        self.calls = []
        self.uploaded = []
        self.updates = []
        self.reactions_added = []
        self.reactions_removed = []
        self._ok = ok
        self._error = error
        self._upload_ok = upload_ok
        self._update_ok = update_ok

    def chat_postMessage(self, **kwargs):
        self.calls.append(kwargs)
        if not self._ok:
            return {"ok": False, "error": self._error}
        return {"ok": True, "ts": "999.0001"}

    def chat_update(self, **kwargs):
        self.updates.append(kwargs)
        return {"ok": self._update_ok}

    def reactions_add(self, **kwargs):
        self.reactions_added.append(kwargs)
        return {"ok": True}

    def reactions_remove(self, **kwargs):
        self.reactions_removed.append(kwargs)
        return {"ok": True}

    def files_upload_v2(self, **kwargs):
        if not self._upload_ok:
            raise RuntimeError("upload failed")
        self.uploaded.append(kwargs)
        return {"ok": True}


def _upstream_asset(output):
    @dg.asset(key=dg.AssetKey(["chat_agent"]))
    def chat_agent():
        return output
    return chat_agent


def _materialize(agent_output, component, monkeypatch, *, tags=None, client=None):
    client = client or _FakeClient()
    monkeypatch.setattr(mod_cache["mod"], "_get_slack_client", lambda env: client)
    upstream = _upstream_asset(agent_output)
    defs = component.build_defs(None)
    result = dg.materialize(
        [upstream, *defs.assets], resources={"io_manager": dg.InMemoryIOManager()}, tags=tags or {},
        raise_on_error=False,
    )
    return result, client


mod_cache = {}


@pytest.fixture(autouse=True)
def _cache_mod(mod):
    mod_cache["mod"] = mod
    yield


def _component(**overrides):
    defaults = dict(asset_name="chat_reply", upstream_asset_key="chat_agent")
    defaults.update(overrides)
    return mod_cache["mod"].SlackReplyComponent(**defaults)


# --- markdown -> mrkdwn conversion ------------------------------------------------------

def test_convert_markdown_bold_and_underscore_bold(mod):
    assert mod._convert_markdown_to_mrkdwn("**bold**") == "*bold*"
    assert mod._convert_markdown_to_mrkdwn("__also bold__") == "*also bold*"


def test_convert_markdown_links(mod):
    assert mod._convert_markdown_to_mrkdwn("[Dagster](https://dagster.io)") == "<https://dagster.io|Dagster>"


def test_convert_markdown_headings(mod):
    assert mod._convert_markdown_to_mrkdwn("# Big Heading") == "*Big Heading*"
    assert mod._convert_markdown_to_mrkdwn("### Smaller Heading") == "*Smaller Heading*"


def test_convert_markdown_bullet_lists(mod):
    text = "* first\n- second\n  * nested"
    assert mod._convert_markdown_to_mrkdwn(text) == "• first\n• second\n  • nested"


def test_convert_markdown_table_becomes_aligned_code_block(mod):
    text = "| Name | Count |\n|---|---|\n| apples | 3 |\n| bananas | 10 |"
    converted = mod._convert_markdown_to_mrkdwn(text)
    assert converted.startswith("```\n")
    assert converted.endswith("\n```")
    assert "Name" in converted and "Count" in converted
    assert "---" not in converted  # separator row dropped, not rendered


def test_convert_markdown_leaves_code_fences_and_inline_code_untouched(mod):
    text = "Use **bold** text. Here's code:\n```python\n**not bold** [not](a-link)\n```\nand `inline **code**` too."
    converted = mod._convert_markdown_to_mrkdwn(text)
    assert "```python\n**not bold** [not](a-link)\n```" in converted
    assert "`inline **code**`" in converted
    assert "Use *bold* text" in converted


def test_convert_markdown_empty_text_is_a_no_op(mod):
    assert mod._convert_markdown_to_mrkdwn("") == ""


# --- success path ----------------------------------------------------------------------

def test_posts_final_answer_to_the_tagged_thread(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "The nightly etl failed twice this week.", "iterations": 1, "tool_calls_made": 0, "stopped_reason": "end_turn", "tool_call_details": []},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls == [{"channel": "C1", "thread_ts": "100.001", "text": "The nightly etl failed twice this week."}]


def test_markdown_is_converted_before_posting_by_default(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "**Status**: done. See [the run](https://example.com)."},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls[0]["text"] == "*Status*: done. See <https://example.com|the run>."


def test_convert_markdown_false_posts_the_answer_verbatim(monkeypatch):
    component = _component(convert_markdown=False)
    result, client = _materialize(
        {"final_answer": "**Status**: done."},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls[0]["text"] == "**Status**: done."


def test_missing_slack_tags_fails_the_run_clearly(monkeypatch):
    component = _component()
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={})
    assert not result.success
    assert client.calls == []


def test_answer_field_override(monkeypatch):
    component = _component(answer_field="summary")
    result, client = _materialize(
        {"summary": "custom field text", "final_answer": "ignored"},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls[0]["text"] == "custom field text"


@pytest.mark.parametrize("op", ["tool_use_loop", "mcp_call", "llm_call"])
def test_agentic_pipeline_step_output_shape_posts_clean_text(monkeypatch, op):
    """An agentic_pipeline step's own output dict is {text, op, ...op-specific fields} -- never
    final_answer. Without this, upstream_asset_key pointed at a pipeline's last step (any of its
    tool_use_loop/mcp_call/llm_call final-step shapes) would post the whole dict's str()
    representation instead of just the answer -- confirmed as a real bug and fixed by recognizing
    `text` as a second default extraction key, same priority tier as final_answer."""
    component = _component()
    pipeline_step_output = {
        "text": "the real answer text", "op": op, "model_fingerprint": "gpt-4o-mini@t0.0",
        "n_iterations": 3, "cost_usd": 0.01, "tool_call_trace": [{"phase": "tool_call"}],
    }
    result, client = _materialize(
        pipeline_step_output, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls[0]["text"] == "the real answer text"
    # not the stringified whole dict -- the bug this guards against
    assert "tool_call_trace" not in client.calls[0]["text"]


def test_plain_string_upstream_output_is_used_directly(monkeypatch):
    component = _component()
    result, client = _materialize("just a plain string answer", component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert client.calls[0]["text"] == "just a plain string answer"


def test_long_answer_is_split_into_multiple_messages(monkeypatch):
    component = _component(max_chars_per_message=10, on_long_answer="split")
    long_text = "0123456789" * 3  # 30 chars
    result, client = _materialize({"final_answer": long_text}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert len(client.calls) == 3
    assert "".join(c["text"].split("\n\n")[0] for c in client.calls) == long_text


def test_long_answer_truncate_mode_posts_one_message(monkeypatch):
    component = _component(max_chars_per_message=10, on_long_answer="truncate")
    result, client = _materialize({"final_answer": "0123456789" * 3}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert len(client.calls) == 1
    assert "truncated" in client.calls[0]["text"]


def test_run_link_appended_when_webserver_base_url_set(monkeypatch):
    component = _component(webserver_base_url="http://localhost:3000")
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert "http://localhost:3000/runs/" in client.calls[0]["text"]
    assert "View run" in client.calls[0]["text"]


def test_no_run_link_when_webserver_base_url_unset(monkeypatch):
    component = _component()
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert client.calls[0]["text"] == "hi"


def test_secrets_in_the_answer_are_redacted(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "here's a token: xoxb-1234567890-abcdefghij"},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert "xoxb-1234567890" not in client.calls[0]["text"]
    assert "[REDACTED]" in client.calls[0]["text"]


def test_slack_post_failure_fails_the_run(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
        client=_FakeClient(ok=False, error="channel_not_found"),
    )
    assert not result.success


# --- failure-path run-status sensor -----------------------------------------------------

def test_component_emits_a_failure_watcher_sensor_by_default(mod):
    component = _component()
    defs = component.build_defs(None)
    assert [s.name for s in defs.sensors] == ["chat_reply_failure_watcher"]


def test_monitor_failures_false_emits_no_sensor(mod):
    component = _component(monitor_failures=False)
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


def test_failure_watcher_posts_an_error_reply_for_a_tagged_failed_run(monkeypatch, mod):
    component = _component()
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    ctx = _real_failed_run_context({"slack/channel": "C1", "slack/thread_ts": "100.001"})
    sensor_def(ctx)
    assert len(client.calls) == 1
    assert client.calls[0]["channel"] == "C1" and client.calls[0]["thread_ts"] == "100.001"
    assert "Something went wrong" in client.calls[0]["text"]


def test_failure_watcher_ignores_a_failed_run_with_no_slack_tags(monkeypatch, mod):
    component = _component()
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    ctx = _real_failed_run_context({})  # not a chat-agent run
    sensor_def(ctx)
    assert client.calls == []


# --- receipt-message-links: pure functions (real sqlite, no mocking) -------------------

def _seed_context_receipts(db_path, rows):
    """rows: [(receipt_id, dagster_run_id), ...]. Minimal columns -- only what the component's
    own queries touch."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE context_receipts (receipt_id TEXT, dagster_run_id TEXT)")
    conn.executemany("INSERT INTO context_receipts VALUES (?, ?)", rows)
    conn.execute("CREATE TABLE context_receipt_events (event_id TEXT, receipt_id TEXT, event_type TEXT, occurred_at TEXT, payload_json TEXT)")
    conn.commit()
    conn.close()


def test_record_and_look_up_message_links(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, ["r1", "r2"], platform="slack", channel="C1", message_ts="100.001")
    assert set(mod.receipt_ids_for_message(db_path, platform="slack", channel="C1", message_ts="100.001")) == {"r1", "r2"}
    # Different platform/channel/ts never matches.
    assert mod.receipt_ids_for_message(db_path, platform="teams", channel="C1", message_ts="100.001") == []
    assert mod.receipt_ids_for_message(db_path, platform="slack", channel="C2", message_ts="100.001") == []


def test_record_message_links_is_a_no_op_for_an_empty_list(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, [], platform="slack", channel="C1", message_ts="100.001")
    import os
    assert not os.path.exists(db_path)  # never even opened/created the file


def test_recent_message_links_orders_most_recent_first_and_is_bounded(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, ["r1"], platform="slack", channel="C1", message_ts="100.001")
    mod.record_message_links(db_path, ["r2"], platform="slack", channel="C1", message_ts="200.001")
    links = mod.recent_message_links(db_path, platform="slack", limit=1)
    assert links == [{"channel": "C1", "message_ts": "200.001"}]


def test_receipt_ids_for_run_reads_context_receipts(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a"), ("r2", "run-a"), ("r3", "run-b")])
    assert set(mod.receipt_ids_for_run(db_path, "run-a")) == {"r1", "r2"}
    assert mod.receipt_ids_for_run(db_path, "run-c") == []


def test_receipt_ids_for_run_returns_empty_when_table_does_not_exist(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    assert mod.receipt_ids_for_run(db_path, "run-a") == []


def test_record_outcome_writes_an_event_for_a_known_receipt(mod, tmp_path):
    import sqlite3
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_outcome(db_path, "r1", signal="thumbs_up", outcome_ref="jane@co.com")
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT payload_json FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()
    assert '"outcome_ref":"jane@co.com"' in row[0] and '"signal":"thumbs_up"' in row[0]


def test_record_outcome_unknown_receipt_is_a_no_op(mod, tmp_path):
    import sqlite3
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [])
    mod.record_outcome(db_path, "does-not-exist", signal="thumbs_up")
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM context_receipt_events").fetchone()[0] == 0


def test_record_outcome_missing_tables_is_a_no_op(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_outcome(db_path, "r1", signal="thumbs_up")  # must not raise


def test_record_outcome_unknown_signal_raises(mod, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    with pytest.raises(ValueError, match="unknown signal"):
        mod.record_outcome(db_path, "r1", signal="made_up_signal")


# --- receipts_db_path wiring on the success-path asset ----------------------------------

def test_receipts_db_path_links_the_posted_message_to_this_runs_receipts(monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(receipts_db_path=db_path)
    captured = {}

    def fake_receipt_ids_for_run(path, run_id):
        captured["lookup"] = (path, run_id)
        return ["r1", "r2"]

    def fake_record_message_links(path, receipt_ids, **kw):
        captured["linked"] = (path, receipt_ids, kw)

    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", fake_receipt_ids_for_run)
    monkeypatch.setattr(mod_cache["mod"], "record_message_links", fake_record_message_links)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert captured["lookup"][0] == db_path
    path, receipt_ids, kw = captured["linked"]
    assert path == db_path and receipt_ids == ["r1", "r2"]
    assert kw == {"platform": "slack", "channel": "C1", "message_ts": "999.0001"}


def test_receipts_db_path_lookup_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(receipts_db_path=db_path)

    def boom(*a, **k):
        raise RuntimeError("db is locked")

    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", boom)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success  # the reply itself was already posted successfully
    assert len(client.calls) == 1


def test_receipts_db_path_unset_never_touches_the_db(monkeypatch, tmp_path):
    component = _component()
    called = []
    monkeypatch.setattr(mod_cache["mod"], "receipt_ids_for_run", lambda *a, **k: called.append(1))
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success and called == []


# --- watch_reactions / rating_watcher sensor --------------------------------------------

def test_watch_reactions_requires_receipts_db_path():
    component = _component(watch_reactions=True)
    with pytest.raises(ValueError, match="watch_reactions requires receipts_db_path"):
        component.build_defs(None)


def test_watch_reactions_emits_a_rating_watcher_sensor_alongside_the_failure_watcher(tmp_path):
    component = _component(receipts_db_path=str(tmp_path / "r.sqlite3"), watch_reactions=True)
    defs = component.build_defs(None)
    assert sorted(s.name for s in defs.sensors) == ["chat_reply_failure_watcher", "chat_reply_rating_watcher"]


class _RatingFakeClient:
    def __init__(self, reactions_by_key=None, email_by_user=None):
        self.reactions_by_key = reactions_by_key or {}
        self.email_by_user = email_by_user or {}
        self.calls = []

    def reactions_get(self, *, channel, timestamp):
        self.calls.append((channel, timestamp))
        return {"ok": True, "message": {"reactions": self.reactions_by_key.get(f"{channel}:{timestamp}", [])}}

    def users_info(self, *, user):
        email = self.email_by_user.get(user)
        return {"ok": True, "user": {"profile": {"email": email}}} if email else {"ok": False}


def _rating_sensor(component):
    defs = component.build_defs(None)
    return next(s for s in defs.sensors if s.name.endswith("_rating_watcher"))


def test_rating_watcher_records_thumbs_up_for_a_linked_receipt(monkeypatch, tmp_path, mod):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_message_links(db_path, ["r1"], platform="slack", channel="C1", message_ts="100.001")

    component = _component(receipts_db_path=db_path, watch_reactions=True)
    sensor_def = _rating_sensor(component)
    client = _RatingFakeClient(
        reactions_by_key={"C1:100.001": [{"name": "+1", "users": ["U1"]}]},
        email_by_user={"U1": "jane@co.com"},
    )
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    ctx = dg.build_sensor_context(cursor=None)
    result = sensor_def(ctx)

    import sqlite3
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT payload_json FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()
    assert row is not None and '"signal":"thumbs_up"' in row[0] and '"outcome_ref":"jane@co.com"' in row[0]
    assert result.skip_reason is None  # something changed this tick


def test_rating_watcher_does_not_recount_an_unchanged_reaction(monkeypatch, tmp_path, mod):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_message_links(db_path, ["r1"], platform="slack", channel="C1", message_ts="100.001")

    component = _component(receipts_db_path=db_path, watch_reactions=True, resolve_rater_email=False)
    sensor_def = _rating_sensor(component)
    client = _RatingFakeClient(reactions_by_key={"C1:100.001": [{"name": "+1", "users": ["U1"]}]})
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    first = sensor_def(dg.build_sensor_context(cursor=None))
    second = sensor_def(dg.build_sensor_context(cursor=first.cursor))

    import sqlite3
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM context_receipt_events WHERE receipt_id = 'r1'").fetchone()[0]
    assert count == 1  # not double-recorded on the second tick
    assert second.skip_reason is not None


def test_rating_watcher_changing_reaction_supersedes_the_earlier_one(monkeypatch, tmp_path, mod):
    db_path = str(tmp_path / "r.sqlite3")
    _seed_context_receipts(db_path, [("r1", "run-a")])
    mod.record_message_links(db_path, ["r1"], platform="slack", channel="C1", message_ts="100.001")

    component = _component(receipts_db_path=db_path, watch_reactions=True, resolve_rater_email=False)
    sensor_def = _rating_sensor(component)
    client = _RatingFakeClient(reactions_by_key={"C1:100.001": [{"name": "+1", "users": ["U1"]}]})
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    first = sensor_def(dg.build_sensor_context(cursor=None))

    client.reactions_by_key["C1:100.001"] = [{"name": "-1", "users": ["U1"]}]
    second = sensor_def(dg.build_sensor_context(cursor=first.cursor))

    import sqlite3
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT payload_json FROM context_receipt_events WHERE receipt_id = 'r1' ORDER BY rowid").fetchall()
    assert len(rows) == 2
    assert '"signal":"thumbs_up"' in rows[0][0] and '"signal":"thumbs_down"' in rows[1][0]
    assert second.skip_reason is None  # a genuine change, not a repeat


def test_rating_watcher_skips_cleanly_when_no_links_yet(monkeypatch, tmp_path, mod):
    db_path = str(tmp_path / "r.sqlite3")
    component = _component(receipts_db_path=db_path, watch_reactions=True)
    sensor_def = _rating_sensor(component)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: _RatingFakeClient())
    result = sensor_def(dg.build_sensor_context(cursor=None))
    assert isinstance(result, dg.SkipReason)


# --- report_files_dir: posting report_tools_server's generated files -------------------

def test_report_files_dir_posts_every_generated_file(monkeypatch, tmp_path):
    component = _component(report_files_dir=str(tmp_path))
    (tmp_path / "chart.png").write_bytes(b"x")
    fake_manifest = [
        {"file_id": "f1", "name": "chart.png", "media_type": "image/png", "path": str(tmp_path / "chart.png"), "size_bytes": 1},
    ]
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: fake_manifest)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert len(client.uploaded) == 1
    assert client.uploaded[0]["filename"] == "chart.png"
    assert client.uploaded[0]["channel"] == "C1" and client.uploaded[0]["thread_ts"] == "100.001"


def test_report_files_dir_unset_never_reads_a_manifest(monkeypatch, tmp_path):
    component = _component()
    called = []
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: called.append(1) or [])
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success and not called and not client.uploaded


def test_report_files_dir_upload_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    component = _component(report_files_dir=str(tmp_path))
    fake_manifest = [{"file_id": "f1", "name": "chart.png", "media_type": "image/png", "path": str(tmp_path / "chart.png")}]
    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", lambda *a, **k: fake_manifest)
    client = _FakeClient(upload_ok=False)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"}, client=client,
    )
    assert result.success  # upload failure must never fail an already-posted reply


def test_report_manifest_read_failure_does_not_fail_the_reply(monkeypatch, tmp_path):
    component = _component(report_files_dir=str(tmp_path))

    def boom(*a, **k):
        raise RuntimeError("disk error")

    monkeypatch.setattr(mod_cache["mod"], "read_run_manifest", boom)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success


def test_read_run_manifest_reads_a_real_manifest_file(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}]')
    assert mod.read_run_manifest(str(tmp_path), "run_abc") == [{"file_id": "f1", "name": "x.png"}]


def test_read_run_manifest_missing_file_returns_empty(mod, tmp_path):
    assert mod.read_run_manifest(str(tmp_path), "no_such_run") == []


def test_lookup_report_file_finds_by_id(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}, {"file_id": "f2", "name": "y.pdf"}]')
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "f2")["name"] == "y.pdf"
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "nope") is None


# --- update_reaction_on_completion / placeholder_ts ------------------------------------

def test_success_swaps_ack_reaction_for_done_reaction(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/message_ts": "100.001"},
    )
    assert result.success
    assert client.reactions_removed == [{"channel": "C1", "timestamp": "100.001", "name": "eyes"}]
    assert client.reactions_added == [{"channel": "C1", "timestamp": "100.001", "name": "white_check_mark"}]


def test_update_reaction_on_completion_false_skips_reactions(monkeypatch):
    component = _component(update_reaction_on_completion=False)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/message_ts": "100.001"},
    )
    assert result.success and not client.reactions_removed and not client.reactions_added


def test_no_message_ts_tag_skips_reactions_without_erroring(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success and not client.reactions_removed and not client.reactions_added


def test_custom_reaction_names_are_used(monkeypatch):
    component = _component(ack_reaction="thinking_face", done_reaction="tada")
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/message_ts": "100.001"},
    )
    assert client.reactions_removed[0]["name"] == "thinking_face"
    assert client.reactions_added[0]["name"] == "tada"


def test_placeholder_ts_updates_the_existing_message_instead_of_posting_a_new_one(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/placeholder_ts": "ph.001"},
    )
    assert result.success
    assert client.updates == [{"channel": "C1", "ts": "ph.001", "text": "hi"}]
    assert client.calls == []  # no NEW message posted


def test_placeholder_ts_falls_back_to_a_new_post_when_update_fails(monkeypatch):
    component = _component()
    client = _FakeClient(update_ok=False)
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/placeholder_ts": "ph.001"}, client=client,
    )
    assert result.success
    assert len(client.updates) == 1
    assert len(client.calls) == 1  # fell back to chat_postMessage


def test_only_the_first_part_of_a_split_answer_uses_the_placeholder(monkeypatch):
    component = _component(max_chars_per_message=5, on_long_answer="split")
    result, client = _materialize(
        {"final_answer": "0123456789"}, component, monkeypatch,
        tags={"slack/channel": "C1", "slack/thread_ts": "100.001", "slack/placeholder_ts": "ph.001"},
    )
    assert result.success
    assert len(client.updates) == 1
    assert len(client.calls) == 1  # the second part is a real new post


def test_failure_watcher_updates_placeholder_and_swaps_reaction(monkeypatch, mod):
    component = _component()
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    ctx = _real_failed_run_context({
        "slack/channel": "C1", "slack/thread_ts": "100.001", "slack/message_ts": "100.001", "slack/placeholder_ts": "ph.001",
    })
    sensor_def(ctx)
    assert client.updates and client.updates[0]["ts"] == "ph.001"
    assert client.calls == []  # updated the placeholder, never posted a new message
    assert client.reactions_removed == [{"channel": "C1", "timestamp": "100.001", "name": "eyes"}]
    assert client.reactions_added == [{"channel": "C1", "timestamp": "100.001", "name": "warning"}]
