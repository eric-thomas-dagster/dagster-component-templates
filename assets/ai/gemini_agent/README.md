# Gemini Agent

> **🔑 Gemini key required.** Set `GEMINI_API_KEY` (or override `api_key_env_var`). Uses the new `google-genai` SDK (not the deprecated `google-generativeai`).

Single-shot LLM agent with Model Context Protocol (MCP) tool support, using the **Google Gen AI SDK directly** — no LiteLLM dependency. Pick this when you're a Gemini-only shop and want one less Python dep.

## How it works

Standard tool-calling loop adapted to Gemini's content/parts shape:

1. Connect to every MCP server in `mcp_servers` (stdio / streamable-http / sse).
2. List tools from each, wrap them in a single `Tool(function_declarations=[…])` (Gemini's convention — one Tool per registry).
3. Send the prompt with `system_instruction` + `tools` via `client.aio.models.generate_content`.
4. Response is a list of `Part`s — each part has either `text` or `function_call`. Dispatch every `function_call` to its MCP session.
5. Tool results go back as a single `user` Content with `Part.from_function_response` blocks (Gemini's convention).
6. Loop until the model returns text without function calls, or `max_iterations`.

## `thinking_budget` — read this if outputs are getting cut off

On Gemini 2.5+, thinking tokens come out of `max_output_tokens`. If the model decides to think for 1500 tokens and your `max_output_tokens` is 2048, you have only ~550 tokens left for the answer + tool calls — and the output silently truncates.

This component defaults `thinking_budget=0` (no thinking) because the **iteration loop already gives the model multi-step reasoning** — each tool round is itself a planning step. You rarely need both.

If your task really needs deeper reasoning per turn (e.g. complex SQL synthesis), raise it explicitly:

```yaml
thinking_budget: 1024     # ¼ of max_output_tokens is a safe ratio
max_output_tokens: 4096   # bump this too if you do
```

## Output

Same dict shape as `litellm_agent` / `openai_agent` / `anthropic_agent`:

```python
{
    "final_answer": "...",
    "iterations": 3,
    "tool_calls_made": 2,
    "tool_call_details": [...],
    "transcript": [...],
    "stopped_reason": "final_answer" | "max_iterations",
    "model": "gemini-2.5-flash",
    "mcp_servers": ["dgp"],
}
```

## MCP transports

Identical config to the other native agents:

```yaml
mcp_servers:
  - name: fs
    type: stdio
    command: [npx, -y, "@modelcontextprotocol/server-filesystem", /tmp]

  - name: dgp
    type: http
    url: https://mcp.agent.dagster.cloud/mcp/
    headers:
      Dagster-Cloud-Organization: my-org
    headers_env:
      Authorization: DAGSTER_PLUS_BEARER
```

## When to use vs `openai_agent` / `anthropic_agent` / `litellm_agent`

- **`gemini_agent`** — Gemini-only. `google-genai`+`mcp` deps only. Cheapest tier (gemini-2.5-flash) has a generous free quota.
- **`openai_agent`** — OpenAI / Azure OpenAI. `openai`+`mcp`.
- **`anthropic_agent`** — Claude. `anthropic`+`mcp`.
- **`litellm_agent`** — Multi-vendor + model routing + fallbacks. `litellm`+`mcp`.

## When NOT to use

| Task | Right component |
|---|---|
| Per-row DataFrame enrichment via chat completion | `gemini_llm` |
| Per-row single tool/function call (no loop) | `litellm_function_calling` |
| RAG over a corpus | `rag_pipeline` |
