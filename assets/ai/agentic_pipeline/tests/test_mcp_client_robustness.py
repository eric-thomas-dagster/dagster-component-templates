"""Robustness of agentic_pipeline's MCP client against REAL servers (a hand-rolled stdio JSON-RPC server for
protocol-level misbehaviour -- fixtures/raw_mcp_server.py -- and the SDK server for http). Runs under whichever `mcp`
major is installed (1.x / 2.x); nothing here is mocked except the paid LLM call in the tool_use_loop tests."""
import asyncio
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import threading
import time

import pytest

pytest.importorskip("mcp")

from .conftest import new_state, tool_call  # noqa: E402

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
LOG = logging.getLogger("mcp_robust")


def _run(coro, timeout=90):
    return asyncio.run(asyncio.wait_for(coro, timeout))


@pytest.fixture(scope="module")
def srv_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("mcp_fix")  # clean dir: this repo's path contains ':' (PYTHONPATH separator)
    for f in ("raw_mcp_server.py", "mini_mcp_server.py"):
        shutil.copy(FIXTURES / f, d / f)
    return d


@pytest.fixture
def raw(srv_dir, tmp_path):
    """raw(*flags, name='raw', **cfg_extra) -> server cfg for the hand-rolled stdio server; its pids land in raw.pids."""
    pidfile = tmp_path / "pids.txt"

    def make(*flags, name="raw", env=None, **extra):
        cfg = {"name": name, "type": "stdio",
               "command": [sys.executable, str(srv_dir / "raw_mcp_server.py"), "--name", name, *flags],
               "env": {"RAW_PID_FILE": str(pidfile), **(env or {})}}
        cfg.update(extra)
        return cfg

    make.pids = lambda: [int(x) for x in pidfile.read_text().split()] if pidfile.exists() else []
    return make


def _alive(pid):
    out = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True).stdout.strip()
    return bool(out)  # a zombie ("Z") still lists, so "no leaked process / zombie" == nothing listed


def _wait_gone(pids, secs=25):
    deadline = time.time() + secs
    while time.time() < deadline and any(_alive(p) for p in pids):
        time.sleep(0.1)
    return [p for p in pids if _alive(p)]


def _call(mod, cfg, tool, args=None, parse_as="text", **kw):
    return _run(mod._call_mcp_tool_async(log=LOG, server_cfg=cfg, tool_name=tool, tool_args=args or {}, parse_as=parse_as, **kw))


def _discover(mod, cfgs):
    return _run(mod._discover_mcp_tools_async(LOG, cfgs))


# ── failures: crash, hung tool, hung / slow handshake ────────────────────────────────────────────────────────────

def test_server_crash_mid_call_names_the_server_and_leaves_no_process(mod, raw):
    cfg = raw(name="flaky")
    t0 = time.time()
    with pytest.raises(mod.MCPServerError) as ei:
        _call(mod, cfg, "crash")
    assert time.time() - t0 < 40  # no hang
    msg = str(ei.value)
    assert "'flaky'" in msg and "'crash'" in msg and "crashed" in msg
    assert not isinstance(ei.value, BaseExceptionGroup if sys.version_info >= (3, 11) else ())  # a clean leaf, not nested groups
    assert _wait_gone(raw.pids()) == []


def test_timeout_seconds_bounds_a_hung_tool_and_reaps_the_process(mod, raw):
    cfg = raw(name="hung", timeout_seconds=2)
    t0 = time.time()
    with pytest.raises(mod.MCPServerError) as ei:
        _call(mod, cfg, "sleep", {"seconds": 120})
    took = time.time() - t0
    assert took < 40, took  # bounded by timeout_seconds (+ teardown), not by the 120s tool
    assert "'hung'" in str(ei.value) and "timed out after 2s" in str(ei.value)
    assert _wait_gone(raw.pids()) == []  # subprocess terminated + reaped (no zombie)


def test_timeout_does_not_cut_a_call_that_finishes_in_time(mod, raw):
    res = _call(mod, raw(timeout_seconds=20), "sleep", {"seconds": 1})
    assert res["raw"] == "slept" and res["is_error"] is False


def test_startup_timeout_bounds_a_hung_handshake(mod, raw):
    cfg = raw("--no-initialize-reply", name="mute", startup_timeout_seconds=1.5)
    t0 = time.time()
    with pytest.raises(mod.MCPServerError) as ei:
        _call(mod, cfg, "echo", {"text": "x"})
    assert time.time() - t0 < 40
    assert "'mute'" in str(ei.value) and "initialize" in str(ei.value) and "startup_timeout_seconds" in str(ei.value)
    assert _wait_gone(raw.pids()) == []


def test_slow_starting_server_within_the_startup_budget_works(mod, raw):
    assert _call(mod, raw("--slow-start", "2"), "echo", {"text": "late"})["raw"] == "late"


def test_slow_starting_server_beyond_the_startup_budget_fails_clearly(mod, raw):
    with pytest.raises(mod.MCPServerError, match="startup_timeout_seconds"):
        _call(mod, raw("--slow-start", "6", startup_timeout_seconds=1.5), "echo", {"text": "x"})
    assert _wait_gone(raw.pids()) == []


@pytest.mark.parametrize("bad", [0, -3, "soon"])
def test_bad_timeout_values_are_rejected_not_ignored(mod, raw, bad):
    with pytest.raises(ValueError, match="timeout_seconds"):
        _call(mod, raw(timeout_seconds=bad), "echo", {"text": "x"})


def test_missing_command_is_a_clear_error(mod):
    cfg = {"name": "ghost", "type": "stdio", "command": ["/nonexistent/definitely-not-a-server"]}
    with pytest.raises(mod.MCPServerError, match="'ghost'"):
        _call(mod, cfg, "echo", {"text": "x"})


def test_junk_on_stdout_before_the_protocol_starts_is_tolerated(mod, raw):
    cfg = raw("--junk")
    assert _call(mod, cfg, "echo", {"text": "hi"})["raw"] == "hi"
    assert {t["name"] for t in _discover(mod, [cfg])} >= {"echo", "sleep"}


def test_unknown_tool_is_a_named_protocol_error_not_a_result(mod, raw):
    with pytest.raises(mod.MCPServerError, match="'nope'"):
        _call(mod, raw(), "nope")


# ── result shapes ────────────────────────────────────────────────────────────────────────────────────────────────

def test_very_large_text_is_returned_whole_by_the_client_call(mod, raw):
    res = _call(mod, raw(), "big", {"n": 400_000})
    assert len(res["raw"]) == 400_000 and res["content_parts"] == [{"type": "text", "chars": 400_000, "omitted": False}]


def test_unicode_and_emoji_round_trip(mod, raw):
    res = _call(mod, raw(), "unicode")
    assert res["raw"] == "héllo 🌍🔥 日本語 עברית   end"
    sent = "ключ 🔑 ☃ é"
    assert _call(mod, raw(), "echo", {"text": sent})["raw"] == sent


def test_multi_part_result_is_flattened_in_order_with_explicit_placeholders(mod, raw):
    res = _call(mod, raw(), "multi", parse_as="auto")
    lines = res["raw"].split("\n")
    assert lines[0] == "part one" and lines[-1] == "part last"
    assert 'embedded text body' in res["raw"]  # embedded TEXT resource is real content
    assert "[image omitted: image/png, ~3009 bytes]" in res["raw"]
    assert "[audio omitted: audio/wav" in res["raw"]
    assert "[binary resource omitted: file:///blob.bin (application/octet-stream" in res["raw"]
    assert "[resource_link: file:///linked.txt (linked)]" in res["raw"]
    assert "iVBOR" not in res["raw"] and "AAAA" not in res["raw"]  # no base64 payload leaks into the text an LLM reads
    assert [p["type"] for p in res["content_parts"]] == ["text", "text", "image", "audio", "resource", "resource", "resource_link", "text"]
    assert [p["omitted"] for p in res["content_parts"]] == [False, False, True, True, False, True, True, False]
    assert res["kind"] == "text"  # multi-part text is not valid JSON as a whole


def test_structured_only_result_becomes_its_json(mod, raw):
    res = _call(mod, raw(), "structured_only", parse_as="auto")
    assert res["value"] == {"answer": 42, "items": [1, 2]} and res["kind"] == "json"
    assert res["structured_content"] == {"answer": 42, "items": [1, 2]}


def test_isError_result_vs_raised_tool_are_both_is_error_true_on_the_sdk_server(mod, srv_dir):
    cfg = {"name": "mini", "type": "stdio", "command": [sys.executable, str(srv_dir / "mini_mcp_server.py"), "stdio"]}
    returned = _call(mod, cfg, "boom", {"reason": "r"})  # SDK server turns the raised ValueError into an isError result
    assert returned["is_error"] is True and "boom" in returned["raw"]


def test_isError_result_from_a_hand_rolled_server(mod, raw):
    res = _call(mod, raw(), "error_result")
    assert res["is_error"] is True and res["raw"] == "it failed politely"


# ── pagination, collisions ───────────────────────────────────────────────────────────────────────────────────────

def test_discovery_follows_every_tools_list_page(mod, raw):
    tools = _discover(mod, [raw("--page-size", "4", "--n-tools", "23", "--only-extra")])
    assert [t["name"] for t in tools] == [f"tool_{i:03d}" for i in range(23)]  # 6 pages, none dropped or repeated


def test_delegate_schema_validation_finds_a_tool_on_a_later_page(mod, raw):
    cfg = raw("--page-size", "3", "--n-tools", "10")  # echo is on page 1, tool_009 on the last page
    assert _call(mod, cfg, "tool_009", {}, validate_input_schema=True)["raw"] == "called tool_009"


def test_two_servers_with_the_same_tool_name_are_namespaced_and_route_correctly(mod, raw, fake_litellm, ctx):
    a, b = raw(name="alpha"), raw(name="beta")
    tools = _discover(mod, [a, b])
    names = [(t["server"], t["name"]) for t in tools if t["name"] == "echo"]
    assert names == [("alpha", "echo"), ("beta", "echo")]
    assert {s["function"]["name"] for s in mod._mcp_tools_to_openai_schema(tools, None) if s["function"]["name"].endswith("__echo")} == {"alpha__echo", "beta__echo"}
    fake_litellm.reply("", tool_calls=[tool_call("beta__pid", {}, id="1"), tool_call("alpha__pid", {}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "ok"}, id="f")])
    state = new_state()
    step = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "max_iterations": 4, "mcp_servers": [a, b]}
    mod._run_step(step, state, ctx)
    trace = [t for t in state["agent"]["tool_call_trace"] if t["phase"] == "tool_call"]
    assert [t["tool_name"] for t in trace] == ["beta__pid", "alpha__pid"]
    assert trace[0]["tool_result_preview"] != trace[1]["tool_result_preview"]  # two different server processes answered


def test_duplicate_server_names_are_rejected_instead_of_misrouting(mod, raw):
    with pytest.raises(ValueError, match="duplicate MCP server name"):
        _discover(mod, [raw(name="same"), raw(name="same")])


def test_namespaced_name_collision_across_servers_is_rejected(mod, raw):
    # server 'a' + tool 'b__echo'  vs  server 'a__b' + tool 'echo'  both namespace to 'a__b__echo'
    with pytest.raises(ValueError, match="collision"):
        _discover(mod, [raw("--rename-echo", "b__echo", name="a"), raw(name="a__b")])


# ── concurrency / fds ────────────────────────────────────────────────────────────────────────────────────────────

def _open_fds():
    return len(os.listdir("/dev/fd"))


def test_fifty_concurrent_stdio_calls_do_not_cross_talk_or_leak_fds(mod, raw):
    cfg = raw()
    _call(mod, cfg, "echo", {"text": "warm"})  # warm imports / lazy fds
    before = _open_fds()
    out, errs = {}, []

    def worker(i):
        try:
            out[i] = asyncio.run(mod._call_mcp_tool_async(
                log=LOG, server_cfg=cfg, tool_name="echo", tool_args={"text": f"msg-{i}-{'y' * i}"}, parse_as="text"))["raw"]
        except BaseException as e:  # noqa: BLE001
            errs.append((i, repr(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert errs == []
    assert out == {i: f"msg-{i}-{'y' * i}" for i in range(50)}  # every caller got exactly its own answer
    assert len(set(raw.pids())) >= 50
    assert _wait_gone(raw.pids()) == []
    assert _open_fds() <= before + 2, (before, _open_fds())  # no leaked pipes


def test_fifty_concurrent_asyncio_gather_calls_in_one_loop(mod, raw):
    cfg = raw()

    async def go():
        return await asyncio.gather(*[
            mod._call_mcp_tool_async(log=LOG, server_cfg=cfg, tool_name="echo", tool_args={"text": str(i)}, parse_as="text")
            for i in range(50)])

    res = _run(go(), 120)
    assert [r["raw"] for r in res] == [str(i) for i in range(50)]
    assert _wait_gone(raw.pids()) == []


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_concurrent_http_calls_share_one_server_without_cross_talk_or_fd_leak(mod, srv_dir):
    port = _free_port()
    p = subprocess.Popen([sys.executable, str(srv_dir / "mini_mcp_server.py"), "http", str(port)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                break
            except OSError:
                time.sleep(0.2)
        os.environ["MINI_T"] = "Bearer t0k"
        cfg = {"name": "web", "type": "http", "url": f"http://127.0.0.1:{port}/mcp", "headers_env": {"Authorization": "MINI_T"}}
        _call(mod, cfg, "echo", {"text": "warm"}, parse_as="auto")
        before = _open_fds()
        out, errs = {}, []

        def worker(i):
            try:
                out[i] = asyncio.run(mod._call_mcp_tool_async(
                    log=LOG, server_cfg=cfg, tool_name="echo", tool_args={"text": f"v{i}"}, parse_as="auto"))["value"]
            except BaseException as e:  # noqa: BLE001
                errs.append((i, repr(e)))

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(120)
        assert errs == []
        assert out == {i: {"echo": f"v{i}"} for i in range(50)}
        time.sleep(0.5)
        assert _open_fds() <= before + 2, (before, _open_fds())
    finally:
        os.environ.pop("MINI_T", None)
        p.terminate()
        p.wait(10)


# ── env passing + secrets ────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def activate(mod):
    tokens = []

    def _go(cfg):
        tokens.append(mod._ACTIVE_POLICY.set(mod._Policy(cfg, {})))

    yield _go
    for t in reversed(tokens):
        mod._ACTIVE_POLICY.reset(t)


def test_env_without_policy_inherits_the_process_env_and_yaml_env_overrides(mod, raw, monkeypatch):
    monkeypatch.setenv("INHERITED_VAR", "from-parent")
    monkeypatch.setenv("OVERRIDDEN_VAR", "parent")
    cfg = raw(env={"OVERRIDDEN_VAR": "yaml", "YAML_ONLY": "y"})
    got = {n: _call(mod, cfg, "env", {"name": n})["raw"] for n in ("INHERITED_VAR", "OVERRIDDEN_VAR", "YAML_ONLY", "NOPE")}
    assert got == {"INHERITED_VAR": "from-parent", "OVERRIDDEN_VAR": "yaml", "YAML_ONLY": "y", "NOPE": "<unset>"}


def test_policy_env_allow_filters_what_the_stdio_server_can_see(mod, raw, monkeypatch, activate):
    monkeypatch.setenv("MCP_OK_TOKEN", "allowed-value")
    monkeypatch.setenv("OTHER_SECRET", "must-not-reach-server")
    activate({"env": {"allow": ["MCP_*"]}})
    cfg = raw(env={"YAML_ONLY": "y"})
    got = {n: _call(mod, cfg, "env", {"name": n})["raw"] for n in ("MCP_OK_TOKEN", "OTHER_SECRET", "YAML_ONLY", "PATH")}
    assert got["MCP_OK_TOKEN"] == "allowed-value"
    assert got["OTHER_SECRET"] == "<unset>"  # not allow-listed -> never handed to the subprocess
    assert got["YAML_ONLY"] == "y"          # the server's own YAML env always applies
    assert got["PATH"] != "<unset>"         # minimal baseline kept


def test_secrets_never_appear_in_logs_or_error_messages(mod, raw, monkeypatch, capfd, caplog):
    secret = "s3cr3t-TOKEN-value-8841"
    monkeypatch.setenv("MY_SECRET_ENV", secret)
    monkeypatch.setenv("HDR_SECRET", f"Bearer {secret}")
    caplog.set_level(logging.DEBUG)
    cfg = raw(env={"YAML_SECRET": secret})
    # successful discovery + call, a tool-level error, a crash, an unknown tool, and an http connect failure with a secret header
    _discover(mod, [cfg])
    _call(mod, cfg, "echo", {"text": "hi"})
    _call(mod, cfg, "error_result")
    errors = []
    for tool in ("crash", "nope"):
        with pytest.raises(Exception) as ei:
            _call(mod, cfg, tool)
        errors.append(repr(ei.value))
    dead = {"name": "dead", "type": "http", "url": "http://127.0.0.1:9/mcp", "headers_env": {"Authorization": "HDR_SECRET"}, "timeout_seconds": 10}
    with pytest.raises(Exception) as ei:
        _call(mod, dead, "echo", {"text": "x"})
    errors.append(repr(ei.value))
    cap = capfd.readouterr()
    blob = cap.out + cap.err + caplog.text + "\n".join(errors)
    assert secret not in blob, "a secret leaked into logs/errors"


def test_command_line_arguments_are_not_logged(mod, raw, caplog):
    caplog.set_level(logging.INFO)
    cfg = raw()
    cfg["command"] = cfg["command"] + ["--n-tools", "0", "--only-extra", "--rename-echo", "echo"]
    _discover(mod, [cfg])
    started = [r.getMessage() for r in caplog.records if "starting stdio server" in r.getMessage()]
    assert started and all("--rename-echo" not in m and "--only-extra" not in m and "(+" in m for m in started)


# ── tool_use_loop: reconnect per call, truncation ───────────────────────────────────────────────────────────────

def _loop_step(servers, **kw):
    return {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "max_iterations": 8, "mcp_servers": servers, **kw}


def test_loop_reconnects_for_each_call_and_leaves_nothing_running(mod, raw, fake_litellm, ctx):
    cfg = raw(name="svc")
    fake_litellm.reply("", tool_calls=[tool_call("svc__pid", {}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("svc__pid", {}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("svc__echo", {"text": "again"}, id="3")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
    state = new_state()
    mod._run_step(_loop_step([cfg]), state, ctx)
    out = state["agent"]
    assert out["stop_reason"] == "finalize_called" and out["n_tool_calls"] == 3
    pids = [t["tool_result_preview"] for t in out["tool_call_trace"] if t["tool_name"] == "svc__pid"]
    assert pids[0] != pids[1]  # documented: a fresh stdio server per call (a stateful server loses state between calls)
    assert len(raw.pids()) == 4  # 1 discovery + 3 calls
    assert _wait_gone(raw.pids()) == []


def test_loop_survives_a_crashing_tool_and_tells_the_model(mod, raw, fake_litellm, ctx):
    cfg = raw(name="svc")
    fake_litellm.reply("", tool_calls=[tool_call("svc__crash", {}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("svc__echo", {"text": "recovered"}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
    state = new_state()
    mod._run_step(_loop_step([cfg]), state, ctx)
    phases = [t["phase"] for t in state["agent"]["tool_call_trace"]]
    assert phases == ["tool_call_error", "tool_call", "finalize"]
    err_msg = [m["content"] for m in fake_litellm.calls[1]["messages"] if m["role"] == "tool"][0]
    assert err_msg.startswith("ERROR:") and "'svc'" in err_msg and "crashed" in err_msg


def test_loop_marks_a_tool_reported_failure_as_an_error_for_the_model(mod, raw, fake_litellm, ctx):
    fake_litellm.reply("", tool_calls=[tool_call("svc__error_result", {}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
    state = new_state()
    mod._run_step(_loop_step([raw(name="svc")]), state, ctx)
    msg = [m["content"] for m in fake_litellm.calls[1]["messages"] if m["role"] == "tool"][0]
    assert msg.startswith("ERROR: tool reported failure: it failed politely")
    assert state["agent"]["tool_call_trace"][0]["tool_result_is_error"] is True


def test_loop_truncates_huge_results_visibly_and_the_cap_is_configurable(mod, raw, fake_litellm, ctx):
    for cap, expect_len, expect_trunc in ((None, 20000, True), (500, 500, True), (0, 100_000, False)):
        fake_litellm.calls.clear()
        fake_litellm.reply("", tool_calls=[tool_call("svc__big", {"n": 100_000}, id="1")])
        fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
        state = new_state()
        kw = {} if cap is None else {"tool_result_max_chars": cap}
        mod._run_step(_loop_step([raw(name="svc")], **kw), state, ctx)
        shown = [m["content"] for m in fake_litellm.calls[1]["messages"] if m["role"] == "tool"][0]
        tr = state["agent"]["tool_call_trace"][0]
        assert tr["tool_result_chars"] == 100_000 and tr["tool_result_truncated"] is expect_trunc
        if expect_trunc:
            assert shown.startswith("x" * expect_len) and shown[expect_len:].startswith("\n...[truncated: ")
            assert f"{100_000 - expect_len} more chars" in shown
        else:
            assert shown == "x" * 100_000


def test_loop_multipart_result_reaches_the_model_without_base64(mod, raw, fake_litellm, ctx):
    fake_litellm.reply("", tool_calls=[tool_call("svc__multi", {}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="f")])
    state = new_state()
    mod._run_step(_loop_step([raw(name="svc")]), state, ctx)
    shown = [m["content"] for m in fake_litellm.calls[1]["messages"] if m["role"] == "tool"][0]
    assert "[image omitted" in shown and "embedded text body" in shown and "iVBOR" not in shown


def test_mcp_call_op_surfaces_content_parts_and_is_error(mod, raw, ctx):
    step = {"id": "grab", "op": "mcp_call", "server": raw(name="svc"), "mcp_tool_name": "multi"}
    out = mod._do_mcp_call(step, new_state(), ctx)
    assert out["is_error"] is False and out["content_parts"][2]["type"] == "image" and "iVBOR" not in out["text"]
    step2 = {"id": "bad", "op": "mcp_call", "server": raw(name="svc"), "mcp_tool_name": "crash"}
    with pytest.raises(mod.MCPServerError, match="'svc'"):
        mod._do_mcp_call(step2, new_state(), ctx)
