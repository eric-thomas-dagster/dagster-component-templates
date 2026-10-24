"""Hand-rolled stdio MCP server (plain JSON-RPC over stdin/stdout, no SDK) so tests can produce protocol-level behaviours
that the SDK servers will not (paged tools/list, junk before the protocol, hard crashes, hung tools, image/resource parts,
structured-only results). Version independent: it speaks the wire format, so the same server exercises mcp 1.x, mcp 2.x
and fastmcp clients.

  python raw_mcp_server.py [--page-size N] [--junk] [--slow-start SECONDS] [--name NAME] [--n-tools N]
Env RAW_PID_FILE: append own pid to that file at startup (tests prove reaping with it).
Tools: echo, sleep, crash, big, unicode, multi, structured_only, error_result, env, pid, secret_args, tool_NNN (paging)
"""
import argparse
import base64
import json
import os
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--page-size", type=int, default=0)
ap.add_argument("--junk", action="store_true")
ap.add_argument("--slow-start", type=float, default=0)
ap.add_argument("--name", default="raw")
ap.add_argument("--n-tools", type=int, default=0, help="extra tool_000.. tools (for paging)")
ap.add_argument("--only-extra", action="store_true", help="expose ONLY the tool_NNN tools")
ap.add_argument("--rename-echo", default="echo", help="expose the echo tool under this name")
ap.add_argument("--no-initialize-reply", action="store_true", help="never answer initialize (hung handshake)")
args, _ = ap.parse_known_args()

OBJ = {"type": "object", "properties": {}}
STATIC = [
    ("echo", "Echo text.", {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}),
    ("sleep", "Sleep N seconds then return.", {"type": "object", "properties": {"seconds": {"type": "number"}}}),
    ("crash", "Exit the process abruptly mid-call.", OBJ),
    ("big", "Return N characters of text.", {"type": "object", "properties": {"n": {"type": "integer"}}}),
    ("unicode", "Return emoji + CJK + RTL text.", OBJ),
    ("multi", "Return text, image, audio, embedded resources, resource_link.", OBJ),
    ("structured_only", "Return structuredContent only (empty content).", OBJ),
    ("error_result", "Return isError result.", OBJ),
    ("env", "Return an env var value (or '<unset>').", {"type": "object", "properties": {"name": {"type": "string"}}}),
    ("pid", "Return this server's pid.", OBJ),
]
PNG = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 3000).decode()


def tools():
    out = [] if args.only_extra else [{"name": args.rename_echo if n == "echo" else n, "description": d, "inputSchema": s} for n, d, s in STATIC]
    out += [{"name": f"tool_{i:03d}", "description": f"extra {i}", "inputSchema": OBJ} for i in range(args.n_tools)]
    return out


def text(t):
    return {"type": "text", "text": t}


def call(name, a):
    if name == args.rename_echo:
        name = "echo"
    if name == "echo":
        return {"content": [text(a.get("text", ""))]}
    if name == "sleep":
        time.sleep(float(a.get("seconds", 1)))
        return {"content": [text("slept")]}
    if name == "crash":
        os._exit(3)
    if name == "big":
        return {"content": [text("x" * int(a.get("n", 1000)))]}
    if name == "unicode":
        return {"content": [text("héllo 🌍🔥 日本語 עברית   end")]}
    if name == "multi":
        return {"content": [
            text("part one"),
            text('{"k": 1}'),
            {"type": "image", "data": PNG, "mimeType": "image/png"},
            {"type": "audio", "data": PNG, "mimeType": "audio/wav"},
            {"type": "resource", "resource": {"uri": "file:///notes.txt", "mimeType": "text/plain", "text": "embedded text body"}},
            {"type": "resource", "resource": {"uri": "file:///blob.bin", "mimeType": "application/octet-stream", "blob": PNG}},
            {"type": "resource_link", "uri": "file:///linked.txt", "name": "linked"},
            text("part last"),
        ]}
    if name == "structured_only":
        return {"content": [], "structuredContent": {"answer": 42, "items": [1, 2]}}
    if name == "error_result":
        return {"content": [text("it failed politely")], "isError": True}
    if name == "env":
        return {"content": [text(os.environ.get(a.get("name", ""), "<unset>"))]}
    if name == "pid":
        return {"content": [text(str(os.getpid()))]}
    if name.startswith("tool_"):
        return {"content": [text(f"called {name}")]}
    return None


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    if os.environ.get("RAW_PID_FILE"):  # lets tests prove the process was reaped after a crash / timeout
        with open(os.environ["RAW_PID_FILE"], "a") as f:
            f.write(f"{os.getpid()}\n")
    if args.slow_start:
        time.sleep(args.slow_start)
    if args.junk:
        sys.stdout.write("WARNING: starting up, this is not json\n[also not a message\n{broken json\n")
        sys.stdout.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        m, rid = msg.get("method"), msg.get("id")
        if rid is None:
            continue  # notification
        if m == "initialize":
            if args.no_initialize_reply:
                time.sleep(3600)
            send({"jsonrpc": "2.0", "id": rid, "result": {
                "protocolVersion": msg["params"]["protocolVersion"], "capabilities": {"tools": {}},
                "serverInfo": {"name": args.name, "version": "0"}}})
        elif m == "tools/list":
            cursor = (msg.get("params") or {}).get("cursor")
            allt = tools()
            if args.page_size:
                start = int(cursor) if cursor else 0
                page = allt[start:start + args.page_size]
                res = {"tools": page}
                if start + args.page_size < len(allt):
                    res["nextCursor"] = str(start + args.page_size)
            else:
                res = {"tools": allt}
            send({"jsonrpc": "2.0", "id": rid, "result": res})
        elif m == "tools/call":
            p = msg["params"]
            r = call(p["name"], p.get("arguments") or {})
            if r is None:
                send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32602, "message": f"unknown tool {p['name']}"}})
            else:
                send({"jsonrpc": "2.0", "id": rid, "result": r})
        elif m == "ping":
            send({"jsonrpc": "2.0", "id": rid, "result": {}})
        else:
            send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "no such method"}})


main()
