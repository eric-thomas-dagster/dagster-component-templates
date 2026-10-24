"""Tiny REAL fastmcp server (the `fastmcp` package, not the mcp SDK's FastMCP) for the `type: fastmcp` client tests.

  python fastmcp_server.py stdio
  python fastmcp_server.py http <port>   # streamable-http at /mcp; requires `Authorization: Bearer t0k` (401 otherwise)
  python fastmcp_server.py sse <port>    # sse at /sse; same bearer requirement
"""
import os
import sys

from fastmcp import FastMCP

server = FastMCP("fm")


@server.tool
def echo(text: str, times: int = 1) -> dict:
    """Echo text back `times` times."""
    return {"echo": text * times}


@server.tool
def boom(reason: str) -> dict:
    """Always fails."""
    raise ValueError(f"boom: {reason}")


@server.tool
def plain(text: str) -> str:
    """Return a plain string."""
    return f"plain:{text}"


@server.tool
def multi():  # no return annotation: fastmcp 3.x derives an outputSchema from `-> list` and then rejects mixed content
    """Text + image parts."""
    from fastmcp.utilities.types import Image
    return ["part one", Image(data=b"\x89PNG\r\n\x1a\n" + b"\x00" * 3000, format="png"), "part last"]


@server.tool
def big(n: int) -> str:
    """n characters."""
    return "x" * n


@server.tool
async def sleep(seconds: float) -> str:
    """Sleep."""
    import asyncio
    await asyncio.sleep(seconds)
    return "slept"


@server.tool
def seen_headers() -> dict:
    """The http request headers this call arrived with (authorization included)."""
    try:
        from fastmcp.server.dependencies import get_http_headers
        h = get_http_headers(include_all=True)
    except Exception:  # noqa: BLE001
        h = {}
    return {"authorization": h.get("authorization"), "x-extra": h.get("x-extra")}


@server.tool
def env(name: str) -> str:
    """An env var of the server process."""
    return os.environ.get(name, "<unset>")


@server.tool
def pid() -> str:
    """Server pid."""
    return str(os.getpid())


def _bearer_app(app):
    async def wrapped(scope, receive, send):
        if scope["type"] == "http":
            hdrs = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            if hdrs.get("authorization") != "Bearer t0k":
                await send({"type": "http.response.start", "status": 401, "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await app(scope, receive, send)
    return wrapped


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "stdio":
        server.run(transport="stdio", show_banner=False)
    else:
        import uvicorn
        app = server.http_app(transport="streamable-http" if mode == "http" else "sse")
        uvicorn.run(_bearer_app(app), host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
