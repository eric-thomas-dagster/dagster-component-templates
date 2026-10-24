"""Tiny REAL MCP server for the client contract tests. Runs on mcp 1.x (FastMCP) or 2.x (MCPServer).

  python mini_mcp_server.py stdio
  python mini_mcp_server.py http <port>     # streamable-http; requires `Authorization: Bearer t0k` (401 otherwise)
  python mini_mcp_server.py sse <port>      # same bearer requirement
"""
import sys

try:
    from mcp.server.mcpserver import MCPServer as Server  # mcp 2.x
except ImportError:
    from mcp.server.fastmcp import FastMCP as Server  # mcp 1.x

server = Server("mini")


@server.tool()
def echo(text: str, times: int = 1) -> dict:
    """Echo text back `times` times."""
    return {"echo": text * times}


@server.tool()
def boom(reason: str) -> dict:
    """Always fails."""
    raise ValueError(f"boom: {reason}")


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
        server.run(transport="stdio")
    else:
        import uvicorn

        app = server.streamable_http_app() if mode == "http" else server.sse_app()
        uvicorn.run(_bearer_app(app), host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
