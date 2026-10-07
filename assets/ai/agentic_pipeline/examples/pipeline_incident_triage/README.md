# Pipeline incident triage — one agent, three real tools, a live decision

Inspired by a real cross-stack pipeline-incident-triage tool: when something breaks, synthesize technical signals (vendor status, institutional knowledge, recent deploys) into a root cause and a decision — critically, **"wait, don't debug" vs. actually troubleshoot**, since debugging a vendor outage wastes engineering time the vendor is already spending.

This is the first example in this set built on `tool_use_loop` instead of `delegate`: **one** agent, **three** real tools, iterating freely until it has enough evidence — not a single forced call.

```
incident report
       │
       ▼
┌──────────────┐        iter 1: check_vendor_status(snowflake)  -- REAL, LIVE
│  investigate  │        iter 1: get_recent_commits(5)            -- REAL git log
│ op: tool_use_ │        iter 2: search_runbooks(...)             -- REAL search
│     loop      │        iter 3: finalize(ROOT_CAUSE / ACTION / REASONING)
└──────┬───────┘
       ▼
┌──────────────┐
│  diagnosis    │  op: extract -- turns the labeled free text into
│              │  clean {root_cause, action, reasoning} fields
└──────────────┘
```

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` — `tool_use_loop` then `extract`. |
| `dispatch_mcp_server.py` | Three REAL tools: a live vendor-status check, a real runbook search, a real `git log`. |
| `runbooks.json` | The "institutional knowledge" the agent searches — a handful of realistic pipeline-incident playbooks. |

## The tools are real, not simulated

- **`check_vendor_status(vendor)`** — a live HTTP call to the vendor's actual public status API (the same Statuspage.io-backed endpoints real uptime-monitoring integrations use): `githubstatus.com`, `status.fivetran.com`, `status.getdbt.com`, `status.snowflake.com`.
- **`get_recent_commits(n)`** — a real `git log -n --oneline` against this repo. Not fabricated history.
- **`search_runbooks(query)`** — real keyword search over `runbooks.json`. Institutional knowledge is inherently local (there's no public API for your team's own playbooks), but the search itself is genuine, not canned.

## What you'll actually see

Run for real against `gpt-4o-mini`, on the incident *"Asset `stg_orders` failed to materialize: the dbt model's query against Snowflake timed out / connection reset... No recent code changes are suspected, but please confirm."* — at the time this was run, Snowflake's real status page happened to show **"Partially Degraded Service"**, live:

```
iter 1: check_vendor_status(vendor=snowflake)
   → {"status": "minor", "description": "Partially Degraded Service", "source_url": "https://status.snowflake.com/api/v2/status.json"}
iter 1: get_recent_commits(n=5)
   → real commits from this repo's actual git log
iter 2: search_runbooks(query="dbt model query timeout connection reset")
   → matched "vendor-degradation-wait" AND "dbt-compilation-error" runbooks
iter 3: finalize(...)
```

Final diagnosis (`extract` step, clean JSON):

```json
{
  "root_cause": "Snowflake is experiencing a minor degraded service affecting connections, which is causing the dbt model to timeout.",
  "action": "wait",
  "reasoning": "The Snowflake status page indicates a partially degraded service, which aligns with the connection reset issue reported. Additionally, there are no recent code changes that could have caused this."
}
```

The agent correctly (a) checked the one vendor actually implicated by the error text, (b) pulled real recent commits and correctly ruled them out as the cause (the incident report said so, and the agent didn't just take that on faith — it actually checked), (c) found the matching runbook, and (d) landed on `action: wait` — genuinely synthesized from three real, independently-verifiable sources, not a canned response. If you run this again after Snowflake's status returns to fully operational, expect a different (correctly different) diagnosis — that's the point.

## Why `tool_use_loop` instead of three `delegate` calls

You could model this as three registered specialist agents (a vendor-checker, a runbook-searcher, a git-checker) and three `delegate` calls — `route_to_specialist/` shows that pattern. `tool_use_loop` is a better fit here because the *order and number* of checks genuinely depends on what the agent finds: it might skip the runbook search if vendor status alone is conclusive, or check two vendors if the incident is ambiguous. `delegate` is for "pick which registered specialist handles this" (one decision); `tool_use_loop` is for "figure out, as you go, which combination of tools you need" (an open-ended investigation). Incident triage is the latter.

## Running it for real

Drop `pipeline.yaml`, `dispatch_mcp_server.py`, and `runbooks.json` into the same defs folder of a real `dg` project, set `OPENAI_API_KEY`, and materialize `incident_triage_diagnosis` (pulls in `incident_triage_investigate` automatically). Edit `pipeline.yaml`'s `source.text` to try a different incident, or point `get_recent_commits` at a different repo via `repo_path`.
