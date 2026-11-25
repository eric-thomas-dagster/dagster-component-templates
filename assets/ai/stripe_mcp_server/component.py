"""StripeMcpServerComponent -- local stdio proxy to Stripe's hosted MCP server.

Confirmed directly against Stripe's own docs (docs.stripe.com/mcp): the remote endpoint is
`https://mcp.stripe.com`, and the required auth is a STATIC bearer token -- specifically an
"Agent"-badged Stripe API key (visible in the Dashboard by the "Agent" badge), sent as
`Authorization: Bearer <key>`. Plain secret/restricted keys are explicitly NOT accepted for this
endpoint. An optional `Stripe-Account` header selects a connected account.

Unlike `notion_mcp_server` (whose hosted server exposes exactly two fixed tools and needs an
OAuth-refresh dance because the signed-in person's token expires in ~8h), Stripe's MCP surface is
large (many Stripe API operations) and the auth token here never expires on its own -- so this
component is simpler on both axes: no OAuth sign-in CLI, and the proxy exposes two generic tools
(`list_remote_tools`, `call_remote_tool`) rather than guessing or hardcoding Stripe's full tool
catalog, which can change server-side without this component needing an update.
"""
import os
import sys
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator


# BEGIN stripe mcp proxy (inlined per component; keep copies conceptually aligned with
# cloudflare_mcp_server/atlassian_mcp_server's own static-bearer-token proxy -- NOT byte-identical
# since each vendor's URL/headers differ, so no drift test across vendors; see each component's
# own module docstring)
import json as _smo_json  # noqa: E402,F401
import threading as _smo_threading  # noqa: E402,F401
import time as _smo_time  # noqa: E402,F401

STRIPE_MCP_URL = "https://mcp.stripe.com"


class StripeMcpAuthError(Exception):
    """The configured API key env var is missing, empty, or Stripe rejected it."""


class StripeMcpRateLimiter:
    """ONE instance shared by every worker -- a per-worker sleep still exceeds Stripe's real
    aggregate limit when several workers each pace independently; only a single shared gate
    enforces the combined rate across all of them."""

    def __init__(self, max_requests_per_second: float):
        self._interval = 1.0 / max(float(max_requests_per_second), 0.01)
        self._lock = _smo_threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = _smo_time.time()
            wait = self._next_allowed - now
            if wait > 0:
                _smo_time.sleep(wait)
                now = _smo_time.time()
            self._next_allowed = now + self._interval


def _smo_headers(api_key: str, stripe_account: Optional[str]) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {api_key}"}
    if stripe_account:
        headers["Stripe-Account"] = stripe_account
    return headers


async def _smo_open_mcp_session(stack, api_key: str, stripe_account: Optional[str]):
    """Streamable-HTTP MCP client against STRIPE_MCP_URL with a static bearer token. Works on mcp
    1.x and 2.x -- mcp 2.x moved header support onto an httpx-like client object instead of a
    `headers=` kwarg (same shim this repo's notion_mcp_server/mcp_server_resource/agentic_pipeline
    components already use for their own streamable-HTTP connections, duplicated here per this
    repo's self-contained-component convention rather than cross-importing)."""
    headers = _smo_headers(api_key, stripe_account)
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=headers))
        streams = await stack.enter_async_context(streamable_http_client(STRIPE_MCP_URL, http_client=http_client))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client
        streams = await stack.enter_async_context(streamablehttp_client(STRIPE_MCP_URL, headers=headers))
    read, write = streams[0], streams[1]
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _smo_list_remote_tools_async(api_key: str, stripe_account: Optional[str], timeout_seconds: float = 60.0) -> List[Dict[str, Any]]:
    import anyio
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        session = await _smo_open_mcp_session(stack, api_key, stripe_account)
        with anyio.fail_after(timeout_seconds):
            result = await session.list_tools()
        return [{"name": t.name, "description": t.description or ""} for t in (result.tools or [])]


async def _smo_call_tool_async(
    api_key: str, stripe_account: Optional[str], tool_name: str, arguments: Optional[Dict[str, Any]], timeout_seconds: float = 60.0,
) -> str:
    import anyio
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        session = await _smo_open_mcp_session(stack, api_key, stripe_account)
        with anyio.fail_after(timeout_seconds):
            result = await session.call_tool(tool_name, arguments or {})
        parts = [getattr(item, "text", None) for item in (result.content or [])]
        return "\n".join(p for p in parts if p)
# END stripe mcp proxy


class StripeMcpProxyTools:
    """`list_tools()`/`call(tool, args)` are the safe entry points an in-process caller (or the
    stdio server body below) uses -- auth-check + rate limiting + retry + error shaping, never a
    raw exception escaping to an agent loop."""

    def __init__(
        self, api_key_env_var: str, *, stripe_account: Optional[str] = None,
        allow: Optional[List[str]] = None, max_requests_per_second: float = 5.0, max_retries: int = 3,
    ):
        self.api_key_env_var = api_key_env_var
        self.stripe_account = stripe_account
        self.allow = frozenset(allow) if allow else None
        self.max_retries = max_retries
        self.rate_limiter = StripeMcpRateLimiter(max_requests_per_second)

    def _get_api_key(self) -> str:
        key = os.environ.get(self.api_key_env_var)
        if not key:
            raise StripeMcpAuthError(
                f"Env var {self.api_key_env_var!r} is not set. Create an 'Agent'-badged Stripe API "
                "key (Dashboard > Developers > API keys) and set it in that env var -- plain "
                "secret/restricted keys are rejected by Stripe's hosted MCP server."
            )
        return key

    def list_tools(self) -> Dict[str, Any]:
        import asyncio

        try:
            api_key = self._get_api_key()
        except StripeMcpAuthError as e:
            return {"error": str(e)}
        attempt = 0
        while True:
            self.rate_limiter.acquire()
            try:
                tools = asyncio.run(_smo_list_remote_tools_async(api_key, self.stripe_account))
                if self.allow:
                    tools = [t for t in tools if t["name"] in self.allow]
                return {"tools": tools}
            except Exception as e:  # noqa: BLE001
                attempt += 1
                if attempt > self.max_retries:
                    return {"error": f"list_remote_tools failed after {self.max_retries} retries: {e}"}
                _smo_time.sleep(min(2 ** attempt, 30))

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import asyncio

        args = dict(args or {})
        if self.allow is not None and tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {sorted(self.allow)})"}
        try:
            api_key = self._get_api_key()
        except StripeMcpAuthError as e:
            return {"error": str(e)}
        attempt = 0
        while True:
            self.rate_limiter.acquire()
            try:
                text = asyncio.run(_smo_call_tool_async(api_key, self.stripe_account, tool, args))
                return {"text": text}
            except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
                attempt += 1
                if attempt > self.max_retries:
                    return {"error": f"{tool} failed after {self.max_retries} retries: {type(e).__name__}: {e}"}
                _smo_time.sleep(min(2 ** attempt, 30))


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- `__name__` is literally
    `"__main__"` for both a bare `python component.py` and `python -m dotted.path`, so
    `__spec__.name` is used instead (same fix as notion_mcp_server's own `_this_module_name`,
    duplicated here per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "stripe_mcp_server's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>`, not `python component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined rather than imported, same shape as
    notion_mcp_server's own copy."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    instructions: Optional[str] = None


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class StripeMcpServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a local stdio MCP server proxying arbitrary Stripe MCP tools (via
    `list_remote_tools`/`call_remote_tool`) to Stripe's hosted MCP server
    (`https://mcp.stripe.com`), authenticated with a static Agent API key -- no OAuth, no token
    refresh. Any mcp_servers-aware agent component references it via `server_resource_key`."""

    resource_key: str = Field(default="stripe_mcp", description="Dagster resource key other components reference via server_resource_key.")
    api_key_env_var: str = Field(
        default="STRIPE_AGENT_API_KEY",
        description=(
            "Env var holding an 'Agent'-badged Stripe API key (Dashboard > Developers > API keys, "
            "identified by the Agent badge). Plain secret/restricted keys are rejected by Stripe's "
            "hosted MCP server."
        ),
    )
    stripe_account_env_var: Optional[str] = Field(
        default=None, description="Optional env var holding a connected account id (acct_...) sent as the Stripe-Account header, for acting on behalf of a connected account."
    )
    allow: Optional[List[str]] = Field(default=None, description="Optional hard allow-list of Stripe MCP tool names. Default: every tool Stripe's hosted server exposes.")
    max_requests_per_second: float = Field(default=5.0, description="Shared by every tool call this server process makes.")
    max_retries: int = Field(default=3, description="Retries on a transient error before returning {'error': ...} to the agent.")
    instructions: Optional[str] = Field(
        default=None,
        description=(
            "Free-text usage guidance for this server -- read by the litellm/openai/anthropic/"
            "gemini agent components and appended to their system prompt under a heading with "
            "this resource's name, so every agent referencing it via server_resource_key gets "
            "the same guidance with no copy-paste."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow",), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Stripe MCP Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        mcp_env: Dict[str, str] = {
            "STRIPE_MCP_API_KEY_ENV_VAR": self.api_key_env_var,
            "STRIPE_MCP_MAX_REQUESTS_PER_SECOND": str(self.max_requests_per_second),
            "STRIPE_MCP_MAX_RETRIES": str(self.max_retries),
        }
        if self.stripe_account_env_var:
            mcp_env["STRIPE_MCP_ACCOUNT_ENV_VAR"] = self.stripe_account_env_var
        if self.allow:
            mcp_env["STRIPE_MCP_ALLOW"] = ",".join(self.allow)

        resource = _McpServerShapeResource(
            name=self.resource_key,
            type="stdio",
            command=[sys.executable, "-m", _this_module_name()],
            env=mcp_env,
            instructions=self.instructions,
        )
        return dg.Definitions(resources={self.resource_key: resource})

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────
def build_mcp_server(tools: "StripeMcpProxyTools"):
    """MCP server (official `mcp` package) exposing two generic passthrough tools. Works on mcp
    1.x and 2.x. Generic (not per-Stripe-operation) because Stripe's MCP tool catalog is large and
    can change server-side; this proxy never has to be updated to track it."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("stripe-mcp-proxy")

    @mcp.tool(name="list_remote_tools")
    def list_remote_tools() -> dict:
        """List the Stripe MCP tools currently available through this proxy (name + description
        for each), so the agent can discover what's callable before using call_remote_tool."""
        return tools.list_tools()

    @mcp.tool(name="call_remote_tool")
    def call_remote_tool(tool_name: str, arguments: Optional[dict] = None) -> dict:
        """Call one Stripe MCP tool by name (see list_remote_tools), with a JSON-object of
        arguments matching that tool's own schema. Returns {"text": ...} or {"error": ...} --
        never raises."""
        return tools.call(tool_name, arguments)

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: STRIPE_MCP_API_KEY_ENV_VAR (required -- names the env var that itself holds the key),
    STRIPE_MCP_ACCOUNT_ENV_VAR (optional, same indirection), STRIPE_MCP_ALLOW (csv),
    _MAX_REQUESTS_PER_SECOND, _MAX_RETRIES."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    api_key_env_var = os.environ.get("STRIPE_MCP_API_KEY_ENV_VAR")
    if not api_key_env_var:
        raise SystemExit("STRIPE_MCP_API_KEY_ENV_VAR must be set")
    account_env_var = os.environ.get("STRIPE_MCP_ACCOUNT_ENV_VAR")
    allow_env = os.environ.get("STRIPE_MCP_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None

    tools = StripeMcpProxyTools(
        api_key_env_var,
        stripe_account=(os.environ.get(account_env_var) if account_env_var else None),
        allow=allow,
        max_requests_per_second=float(os.environ.get("STRIPE_MCP_MAX_REQUESTS_PER_SECOND", "5.0")),
        max_retries=int(os.environ.get("STRIPE_MCP_MAX_RETRIES", "3")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
