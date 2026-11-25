"""AtlassianMcpServerComponent -- local stdio proxy to Atlassian's hosted MCP server (Jira,
Confluence, Jira Service Management).

Confirmed directly against Atlassian's own docs (support.atlassian.com/atlassian-ai-gateway): the
remote endpoint is `https://mcp.atlassian.com/v2/mcp`. Atlassian MCP's PRIMARY auth mechanism is
OAuth 2.1 (interactive, browser consent) -- but Atlassian also documents a token-based auth path
explicitly recommended for "backend services or CI/CD pipelines" (their own words), which an org
admin must first explicitly enable (it is off by default). Two token shapes, both confirmed real
and both granting the SAME tool access as an interactive session (not a reduced subset):

  - Personal API token: `Authorization: Basic <base64(email:api_token)>`
  - Service-account API key: `Authorization: Bearer <key>`

This component supports ONLY these two token modes, not OAuth. That is a deliberate, documented
scope cut, not an oversight: unlike Notion's hosted MCP server (which supports OAuth via RFC 8414
dynamic client registration -- no pre-registered app needed), Atlassian's OAuth 2.1 flow requires
a pre-registered OAuth app created through Atlassian's developer console, with no dynamic
registration and no confirmed scope string for the Atlassian-MCP-specific consent grant. Since the
token path already grants full access and is Atlassian's own recommended automation story, this
component does not attempt to replicate that heavier, less-verifiable OAuth flow. If your
organization has not enabled token-based MCP auth, this component cannot be used until an admin
does (see README for the exact steps, linked from Atlassian's own docs) -- use the OAuth-based
sign-in flow in your interactive MCP client instead.
"""
import base64
import os
import sys
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator


# BEGIN atlassian mcp proxy (inlined per component; keep copies conceptually aligned with
# stripe_mcp_server/cloudflare_mcp_server's own static-bearer-token proxy -- NOT byte-identical
# since each vendor's URL/headers/auth-mode differ, so no drift test across vendors; see each
# component's own module docstring)
import threading as _amo_threading  # noqa: E402,F401
import time as _amo_time  # noqa: E402,F401

ATLASSIAN_MCP_URL = "https://mcp.atlassian.com/v2/mcp"


class AtlassianMcpAuthError(Exception):
    """A required env var for the configured auth_mode is missing, empty, or Atlassian rejected
    the credential (e.g. token-based auth isn't enabled for this org)."""


class AtlassianMcpRateLimiter:
    """ONE instance shared by every worker -- a per-worker sleep still exceeds Atlassian's real
    aggregate limit when several workers each pace independently; only a single shared gate
    enforces the combined rate across all of them."""

    def __init__(self, max_requests_per_second: float):
        self._interval = 1.0 / max(float(max_requests_per_second), 0.01)
        self._lock = _amo_threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = _amo_time.time()
            wait = self._next_allowed - now
            if wait > 0:
                _amo_time.sleep(wait)
                now = _amo_time.time()
            self._next_allowed = now + self._interval


async def _amo_open_mcp_session(stack, auth_header: str):
    """Streamable-HTTP MCP client against ATLASSIAN_MCP_URL with a static Basic or Bearer
    Authorization header. Works on mcp 1.x and 2.x -- mcp 2.x moved header support onto an
    httpx-like client object instead of a `headers=` kwarg (same shim this repo's
    notion_mcp_server/mcp_server_resource/agentic_pipeline components already use for their own
    streamable-HTTP connections, duplicated here per this repo's self-contained-component
    convention rather than cross-importing)."""
    headers = {"Authorization": auth_header}
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=headers))
        streams = await stack.enter_async_context(streamable_http_client(ATLASSIAN_MCP_URL, http_client=http_client))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client
        streams = await stack.enter_async_context(streamablehttp_client(ATLASSIAN_MCP_URL, headers=headers))
    read, write = streams[0], streams[1]
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _amo_list_remote_tools_async(auth_header: str, timeout_seconds: float = 60.0) -> List[Dict[str, Any]]:
    import anyio
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        session = await _amo_open_mcp_session(stack, auth_header)
        with anyio.fail_after(timeout_seconds):
            result = await session.list_tools()
        return [{"name": t.name, "description": t.description or ""} for t in (result.tools or [])]


async def _amo_call_tool_async(auth_header: str, tool_name: str, arguments: Optional[Dict[str, Any]], timeout_seconds: float = 60.0) -> str:
    import anyio
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        session = await _amo_open_mcp_session(stack, auth_header)
        with anyio.fail_after(timeout_seconds):
            result = await session.call_tool(tool_name, arguments or {})
        parts = [getattr(item, "text", None) for item in (result.content or [])]
        return "\n".join(p for p in parts if p)
# END atlassian mcp proxy


class AtlassianMcpProxyTools:
    """`list_tools()`/`call(tool, args)` are the safe entry points an in-process caller (or the
    stdio server body below) uses -- auth-header construction + rate limiting + retry + error
    shaping, never a raw exception escaping to an agent loop."""

    def __init__(
        self, auth_mode: str, *, email_env_var: Optional[str] = None, api_token_env_var: Optional[str] = None,
        service_account_key_env_var: Optional[str] = None, allow: Optional[List[str]] = None,
        max_requests_per_second: float = 5.0, max_retries: int = 3,
    ):
        if auth_mode not in ("personal_api_token", "service_account"):
            raise ValueError(f"unknown auth_mode {auth_mode!r}; valid: personal_api_token, service_account")
        self.auth_mode = auth_mode
        self.email_env_var = email_env_var
        self.api_token_env_var = api_token_env_var
        self.service_account_key_env_var = service_account_key_env_var
        self.allow = frozenset(allow) if allow else None
        self.max_retries = max_retries
        self.rate_limiter = AtlassianMcpRateLimiter(max_requests_per_second)

    def _get_auth_header(self) -> str:
        if self.auth_mode == "personal_api_token":
            email = os.environ.get(self.email_env_var or "")
            token = os.environ.get(self.api_token_env_var or "")
            if not email or not token:
                raise AtlassianMcpAuthError(
                    f"auth_mode='personal_api_token' requires both {self.email_env_var!r} and "
                    f"{self.api_token_env_var!r} to be set. Create a personal API token at "
                    "id.atlassian.com/manage-profile/security/api-tokens -- requires your org "
                    "admin to have enabled token-based auth for Atlassian MCP first."
                )
            encoded = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
            return f"Basic {encoded}"
        key = os.environ.get(self.service_account_key_env_var or "")
        if not key:
            raise AtlassianMcpAuthError(
                f"auth_mode='service_account' requires {self.service_account_key_env_var!r} to be "
                "set to a service-account API key -- requires your org admin to have enabled "
                "token-based auth for Atlassian MCP first."
            )
        return f"Bearer {key}"

    def list_tools(self) -> Dict[str, Any]:
        import asyncio

        try:
            auth_header = self._get_auth_header()
        except AtlassianMcpAuthError as e:
            return {"error": str(e)}
        attempt = 0
        while True:
            self.rate_limiter.acquire()
            try:
                tools = asyncio.run(_amo_list_remote_tools_async(auth_header))
                if self.allow:
                    tools = [t for t in tools if t["name"] in self.allow]
                return {"tools": tools}
            except Exception as e:  # noqa: BLE001
                attempt += 1
                if attempt > self.max_retries:
                    return {"error": f"list_remote_tools failed after {self.max_retries} retries: {e}"}
                _amo_time.sleep(min(2 ** attempt, 30))

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import asyncio

        args = dict(args or {})
        if self.allow is not None and tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {sorted(self.allow)})"}
        try:
            auth_header = self._get_auth_header()
        except AtlassianMcpAuthError as e:
            return {"error": str(e)}
        attempt = 0
        while True:
            self.rate_limiter.acquire()
            try:
                text = asyncio.run(_amo_call_tool_async(auth_header, tool, args))
                return {"text": text}
            except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
                attempt += 1
                if attempt > self.max_retries:
                    return {"error": f"{tool} failed after {self.max_retries} retries: {type(e).__name__}: {e}"}
                _amo_time.sleep(min(2 ** attempt, 30))


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- `__name__` is literally
    `"__main__"` for both a bare `python component.py` and `python -m dotted.path`, so
    `__spec__.name` is used instead (same fix as notion_mcp_server's own `_this_module_name`,
    duplicated here per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "atlassian_mcp_server's mcp_server_resource_key path must be run as `python -m "
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


class AtlassianMcpServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a local stdio MCP server proxying arbitrary Jira/Confluence/JSM MCP tools (via
    `list_remote_tools`/`call_remote_tool`) to Atlassian's hosted MCP server
    (`https://mcp.atlassian.com/v2/mcp`), authenticated with a static personal API token or
    service-account key -- no OAuth. Requires your org admin to have enabled token-based MCP auth
    first (see module docstring and README). Any mcp_servers-aware agent component references it
    via `server_resource_key`."""

    resource_key: str = Field(default="atlassian_mcp", description="Dagster resource key other components reference via server_resource_key.")
    auth_mode: Literal["personal_api_token", "service_account"] = Field(
        default="personal_api_token",
        description=(
            "'personal_api_token': Authorization: Basic base64(email:api_token), acting as that "
            "person. 'service_account': Authorization: Bearer <key>, acting as a service account. "
            "Both require an org admin to have enabled token-based MCP auth."
        ),
    )
    atlassian_email_env_var: str = Field(default="ATLASSIAN_EMAIL", description="auth_mode='personal_api_token': env var holding the token owner's Atlassian account email.")
    atlassian_api_token_env_var: str = Field(default="ATLASSIAN_API_TOKEN", description="auth_mode='personal_api_token': env var holding the personal API token (id.atlassian.com/manage-profile/security/api-tokens).")
    atlassian_service_account_key_env_var: str = Field(default="ATLASSIAN_SERVICE_ACCOUNT_KEY", description="auth_mode='service_account': env var holding the service-account API key.")
    allow: Optional[List[str]] = Field(default=None, description="Optional hard allow-list of Atlassian MCP tool names. Default: every tool your granted apps/scopes expose.")
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
        return ComponentFormConfig(label='Atlassian MCP Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        mcp_env: Dict[str, str] = {
            "ATLASSIAN_MCP_AUTH_MODE": self.auth_mode,
            "ATLASSIAN_MCP_EMAIL_ENV_VAR": self.atlassian_email_env_var,
            "ATLASSIAN_MCP_API_TOKEN_ENV_VAR": self.atlassian_api_token_env_var,
            "ATLASSIAN_MCP_SERVICE_ACCOUNT_KEY_ENV_VAR": self.atlassian_service_account_key_env_var,
            "ATLASSIAN_MCP_MAX_REQUESTS_PER_SECOND": str(self.max_requests_per_second),
            "ATLASSIAN_MCP_MAX_RETRIES": str(self.max_retries),
        }
        if self.allow:
            mcp_env["ATLASSIAN_MCP_ALLOW"] = ",".join(self.allow)

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
def build_mcp_server(tools: "AtlassianMcpProxyTools"):
    """MCP server (official `mcp` package) exposing two generic passthrough tools. Works on mcp
    1.x and 2.x. Generic (not per-Jira/Confluence-operation) because Atlassian MCP's own tool
    surface depends on which apps/scopes were granted; this proxy just forwards to whatever
    Atlassian exposes."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("atlassian-mcp-proxy")

    @mcp.tool(name="list_remote_tools")
    def list_remote_tools() -> dict:
        """List the Atlassian MCP tools currently available through this proxy (name +
        description for each -- depends on which Jira/Confluence/JSM scopes this credential has),
        so the agent can discover what's callable before using call_remote_tool."""
        return tools.list_tools()

    @mcp.tool(name="call_remote_tool")
    def call_remote_tool(tool_name: str, arguments: Optional[dict] = None) -> dict:
        """Call one Atlassian MCP tool by name (see list_remote_tools), with a JSON-object of
        arguments matching that tool's own schema. Returns {"text": ...} or {"error": ...} --
        never raises."""
        return tools.call(tool_name, arguments)

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: ATLASSIAN_MCP_AUTH_MODE (personal_api_token|service_account), plus whichever of
    ATLASSIAN_MCP_EMAIL_ENV_VAR/_API_TOKEN_ENV_VAR/_SERVICE_ACCOUNT_KEY_ENV_VAR that mode needs
    (each names ANOTHER env var that itself holds the real secret), ATLASSIAN_MCP_ALLOW (csv),
    _MAX_REQUESTS_PER_SECOND, _MAX_RETRIES."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    auth_mode = os.environ.get("ATLASSIAN_MCP_AUTH_MODE", "personal_api_token")
    allow_env = os.environ.get("ATLASSIAN_MCP_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None

    tools = AtlassianMcpProxyTools(
        auth_mode,
        email_env_var=os.environ.get("ATLASSIAN_MCP_EMAIL_ENV_VAR"),
        api_token_env_var=os.environ.get("ATLASSIAN_MCP_API_TOKEN_ENV_VAR"),
        service_account_key_env_var=os.environ.get("ATLASSIAN_MCP_SERVICE_ACCOUNT_KEY_ENV_VAR"),
        allow=allow,
        max_requests_per_second=float(os.environ.get("ATLASSIAN_MCP_MAX_REQUESTS_PER_SECOND", "5.0")),
        max_retries=int(os.environ.get("ATLASSIAN_MCP_MAX_RETRIES", "3")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
