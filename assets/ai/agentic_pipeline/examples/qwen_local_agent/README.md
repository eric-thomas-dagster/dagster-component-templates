# A tool-using agent on local Qwen3, confined by `policy:`

One `tool_use_loop` step on `ollama/qwen3:8b` that reads files through a filesystem MCP server -- and **cannot** read
outside `./data`, write anything, see any env var you didn't list, or call any host but your local Ollama. The
interesting part is not the model, it's that those limits hold *regardless of what the model decides to do*.

> **Status: not yet run against a live Ollama.** Everything below that says "enforced" is covered by the component's
> own tests (`assets/ai/agentic_pipeline/tests/test_policy.py`, with the model call faked), and `pipeline.yaml` is
> validated against the real component. The end-to-end run with a real Qwen3 has not been done -- the "What to
> expect" section describes what the tests prove and what to look for, not a recorded run.

## Run it

```bash
ollama pull qwen3:8b && ollama serve
export OLLAMA_API_BASE=http://localhost:11434
# drop pipeline.yaml into a defs/ folder of a Dagster project with dagster_community_components installed, then
dg launch --assets 'qwen_review*'
```

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent`: one `tool_use_loop` step + the `policy:` block. |
| `data/deploy.sh`, `data/notes.md` | Sample files with deliberately planted security risks (a hard-coded secret, `curl -k`). |

## What the policy does here

| Limit | Mechanism | What happens on a violation |
|---|---|---|
| Read only under `data/` | `arg_rules: {tool: fs__*, arg: path, under: [data]}` -- paths are resolved first, so `data/../secret` and symlinks are refused | The call is not run; the model receives `ERROR: blocked by policy: …` and can adapt. Recorded in `…__policy_violation_detail`. |
| Never write/delete/move | `tools.deny: [fs__write_*, …]` | Those tools are **not shown to the model at all**; a hallucinated call is refused the same way. |
| Only listed env vars | `env.allow: [OLLAMA_API_BASE]` | The pipeline refuses to read any other variable, and the tool subprocess is started with *only* that variable plus a minimal `PATH`/`HOME` baseline -- not your whole environment (cloud keys, tokens). |
| Only local Ollama | `network.allow_hosts: [localhost, 127.0.0.1]` | A different `api_base` is refused **before** any request is made. |
| Bounded spend | `budget.max_llm_calls`, `tools.max_calls_per_run`, `max_seconds` | The next call raises `PolicyViolation`; usage so far is on the step's metadata. |

**What this is not:** an OS sandbox. The MCP server is a subprocess running with your user's OS permissions; the policy
decides which tools the model may use, with which arguments, and which environment that subprocess receives. For hard
isolation, run the code location in a container.

## Qwen-specific behaviour this relies on

- **`<think>…</think>` is stripped** from the answer and kept as `qwen_review_review__reasoning` metadata, so
  structured steps never choke on the model's chain of thought (an unclosed block from a truncated response is handled).
- **`ollama/` is routed to `ollama_chat/`** when tools are in play -- the Ollama route that supports tool calling -- and
  `tool_choice="required"` (which Ollama's chat API lacks) is downgraded to `auto`.
- **`<tool_call>{…}</tool_call>` text is recovered** when the model emits a tool call as plain text instead of a
  structured one, so a format slip doesn't end the loop.

## What to look for when you run it

- `…__tool_call_trace` -- every tool call, and any `tool_call_blocked` entries with the reason.
- `…__reasoning` -- Qwen's thinking, per call.
- `…__policy_usage` -- calls/tokens/cost used against the budget; `…__policy_violations` -- count this step.
