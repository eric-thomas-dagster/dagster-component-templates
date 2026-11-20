# HostedMcpServer

A local stdio MCP server proxying an allow-listed subset of tools from **any hosted MCP server
that uses OAuth** — generalizes [`NotionMcpServerComponent`](../notion_mcp_server/README.md)
(which could become a preset of this component; kept separate for now rather than refactored, to
avoid risking a working integration). Built for a gallery of ready-made servers (Linear, GitHub,
Pylon, Salesforce, Sentry, …) where some use OAuth and some take a static token.

## Sign in first

```
python -m dagster_community_components.resources.mcp_oauth_login --url https://mcp.example.com/mcp --oauth-dir ~/.mcp_oauth/example
```

Runs the OAuth Authorization Code + PKCE flow, discovering the server's real endpoints via the
MCP authorization spec's own two-step flow: RFC 9728 (Protected Resource Metadata, published by
the MCP server itself) names the authorization server(s); RFC 8414 (Authorization Server
Metadata) is then fetched from the first one. If a server doesn't publish RFC 9728 metadata
(confirmed real — Notion's own hosted server is exactly this case), it falls back to RFC 8414
directly against the MCP server's own origin. Writes `tokens.json` + `client_info.json` under
`--oauth-dir` (mode `0600`) — point `oauth_dir` at the same directory below.

**No dynamic client registration?** Some servers (Salesforce is the confirmed example — it
requires an admin-created External Client App with the `mcp_api` scope) don't support
self-service client registration. Pass `--client-id` (and `--client-secret-env`, naming an env
var holding the secret, if it issues one) instead:

```
python -m dagster_community_components.resources.mcp_oauth_login \
  --url https://mycompany.my.salesforce.com/services/mcp \
  --oauth-dir ~/.mcp_oauth/salesforce \
  --client-id 3MVG9... --client-secret-env SALESFORCE_MCP_CLIENT_SECRET
```

The CLI also tries to confirm **who** signed in, calling the first tool whose name looks
identity-shaped (`whoami`, `get_self`, `current_user`, `viewer`, …) with no arguments — a
best-effort courtesy; not every server has one.

## Servers that take a STATIC token instead — skip all of this

| Server | Hosted MCP URL | Header |
|---|---|---|
| GitHub | `https://api.githubcopilot.com/mcp/` | `Authorization: Bearer <personal access token>` |
| Linear | `https://mcp.linear.app/mcp` | `Authorization: Bearer <API key>` — **note:** this differs from Linear's own REST API, which takes the raw key with **no** `Bearer` prefix; don't mix the two up |

Register either directly via
[`MCPServerResourceComponent`](../../../resources/mcp_server_resource/README.md)'s
`headers`/`headers_env` fields (`http` transport) — no sign-in CLI, no `oauth_dir`, no token
refresh needed, since the token never expires.

## Registering the proxy

```yaml
type: dagster_community_components.HostedMcpServerComponent
attributes:
  resource_key: linear
  url: https://mcp.linear.app/mcp
  oauth_dir: /home/dagster/.mcp_oauth/linear
  allow:
    - get_issue
    - list_issues
    - create_comment
  max_requests_per_second: 2
```

Then, in any MCP-aware component (anthropic_agent / openai_agent / litellm_agent / gemini_agent /
agentic_pipeline) instead of an inline `server:` spec:

```yaml
server_resource_key: linear
```

## `allow` defaults to EMPTY

Unlike `NotionMcpServerComponent` (which defaults to exposing its only two, known-safe, read-only
tools), `allow` here defaults to an empty list — **nothing is exposed until you name it**. An
arbitrary hosted server's tool surface isn't known ahead of time and may include destructive
write/delete tools; you choose explicitly.

## Tool interface shape — stated plainly

Each name in `allow` is exposed as **`<tool_name>(arguments: dict)`** — a single nested object
parameter carrying the upstream tool's own real argument names (e.g.
`get_issue(arguments={"id": "ABC-123"})`), **not** flattened kwargs matching that tool's exact
JSON schema. Mirroring the upstream server's per-tool schema exactly would require MCP's
low-level `Server` API, whose `list_tools`/`call_tool` registration is a genuinely different,
incompatible shape between the `mcp` SDK's 1.x (decorator-based) and 2.x (constructor-callback)
majors — confirmed directly, not assumed — and isn't something this repo can verify against a
live third-party server in this environment. The `arguments: dict` wrapper is simple, identical
across both SDK majors (confirmed: a `Dict[str, Any]`-typed parameter gets a free-form
`additionalProperties: true` schema on both), and fully testable without a live server. A
deliberate, documented tradeoff — not an oversight.

## Rate limiting

```yaml
max_requests_per_second: 2
max_retries: 5
soft_rate_limit_pattern: null   # optional regex for a server that signals rate limits as text
```

Handles a real HTTP 429 (if the transport surfaces one as an exception — the common case for
most servers) and, optionally, a soft TEXT-based rate-limit signal via a configurable regex
(`soft_rate_limit_pattern`) — some servers (Notion's hosted server is the confirmed example, see
[`notion_mcp_documents`](../../ingestion/notion_mcp_documents/README.md)) never return HTTP 429
at all, just ordinary-looking tool-result text. Unset, a generic default pattern (`rate limit`,
`too many requests`, `retry...after`) is used, which may not match every server's own wording —
set it explicitly if you see a server's own rate-limit text slip through unmatched.

## Token health

`check_token_health(oauth_dir)` (importable from this component's `component.py`) refreshes the
token without making a real tool call, returning `{"healthy": bool, "message": str}` — a rejected
refresh token reports `"sign in again"` in the message, rather than a raw auth-library exception,
so a caller (a health-check asset, a CLI, a dashboard) can give an actionable message.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `url` | `str` | The hosted MCP server's URL (e.g. 'https://mcp.example.com/mcp'). |
| `oauth_dir` | `str` | Directory holding tokens.json + client_info.json from `python -m dagster_community_components.resources.mcp_oauth_login`. Treat as a secret. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"mcp"` | Dagster resource key other components reference via server_resource_key. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `int` | `5` | Retries on a real HTTP 429, a soft text-based rate-limit signal, or a transient error before returning {'error': ...} to the agent. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `allow` | `List[str]` | `list()` | Tool allow-list -- default EMPTY, so no tools are exposed until you explicitly choose some (unlike notion_mcp_server's 'both tools by default': an arbitrary hosted server's tool surface is unknown ahead of time and may i… _(full docs in schema.json + component README)_ |
| `max_requests_per_second` | `float` | `2.0` | Shared by every tool call this server process makes -- see HostedMcpRateLimiter. |
| `soft_rate_limit_pattern` | `str` | — | Regex matched against a tool result's TEXT to detect a server that signals rate limits as ordinary text instead of HTTP 429 (Notion's hosted server is the confirmed example -- see notion_mcp_documents). Unset: a generic… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
requests
mcp
anyio
```
