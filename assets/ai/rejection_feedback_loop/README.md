# rejection_feedback_loop

Sensor that closes the loop between human rejection and agent revision.
Watches an approval-token directory for `approved: false` tokens
carrying a `feedback: "..."` field, and re-triggers the upstream pipeline
with that feedback captured as new input.

## Type

```
dagster_community_components.RejectionFeedbackLoopComponent
```

## The loop

```
1. Agent generates draft ──▶ HumanApprovalGate emits asset
                                        │
                              ┌─────────┴─────────┐
                              ▼                   ▼
                        (approved: true)   (approved: false + feedback: "...")
                              │                   │
                              ▼                   ▼
                        (ship it)     RejectionFeedbackLoop sees rejection:
                                        - writes {approval_dir}/.feedback/{key}.txt
                                        - bumps iteration counter
                                        - moves token to .consumed/
                                        - triggers target_job re-run for partition
                                                       │
                                                       ▼
                                        Agent re-runs, reads feedback,
                                        emits revised draft
                                                       │
                                                       ▼
                                        Loop until approved OR max_iterations
```

## Minimal example

```yaml
type: dagster_community_components.RejectionFeedbackLoopComponent
attributes:
  sensor_name: mir_feedback_loop
  approval_dir: /tmp/mir_approvals
  target_job_name: launch_mir_triage
  max_iterations: 3
```

## Wiring the feedback into the pipeline

The sensor writes `{approval_dir}/.feedback/{partition_key}.txt` when
a rejection with feedback lands. Your `AgenticPipeline` reads that file
alongside the original source — the `synthesize` op is the natural fit:

```yaml
steps:
  - id: original_source
    op: llm_call
    source: source
    prompt_template: "{text}"
    model: gpt-4o-mini
    api_key_env_var: OPENAI_API_KEY

  # Try to read the feedback file — if it doesn't exist, mcp_call fails
  # gracefully or emits empty text; wrap in a specialist that tolerates
  # absence. Simpler: use `synthesize` with typed inputs that DEFAULT
  # to empty when a source step returns nothing.
  - id: feedback
    op: mcp_call
    server:
      name: local_fs
      type: stdio
      command: ["npx", "-y", "@modelcontextprotocol/server-filesystem",
                "/tmp/mir_approvals/.feedback"]
    mcp_tool_name: read_file
    tool_args:
      path: "/tmp/mir_approvals/.feedback/{partition_key}.txt"

  - id: revised
    op: synthesize
    model: gpt-4o
    api_key_env_var: OPENAI_API_KEY
    inputs:
      original: {from: original_source}
      human_feedback: {from: feedback}
    prompt_template: |
      Original task and prior draft:
      {original}

      Human reviewer feedback (may be empty on first iteration):
      {human_feedback}

      Revise to address the feedback. Output only the revised draft.
```

## Fields

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `sensor_name` | string | yes | | Unique sensor name. |
| `approval_dir` | string | yes | | Shared directory approval-gate components write into. |
| `target_job_name` | string | yes | | Job to re-materialize when a rejection-with-feedback token is detected. |
| `max_iterations` | integer | | `3` | Cap on revise cycles per partition. |
| `minimum_interval_seconds` | integer | | `30` | Sensor cadence. |
| `default_status` | enum | | `running` | `running` or `stopped`. |
| `dynamic_partitions_name` | string | | | Required if target assets are dynamic-partitioned. |
| `feedback_subdir` | string | | `.feedback` | |
| `consumed_subdir` | string | | `.consumed` | |
| `state_subdir` | string | | `.state` | |
| `emit_state_asset` | boolean | | `false` | Emit a DataFrame asset with one row per partition mid-loop. |

## Token format

Reads any JSON token in `approval_dir` that matches:

```json
{
  "approved": false,
  "approver": "alice@acme.com",
  "reason": "too vague",
  "feedback": "Cover the API-stability implication explicitly, and cite the specific line numbers you're basing the classification on."
}
```

`approved: true` tokens are ignored. `approved: false` without `feedback` is ignored (that's a plain rejection — nothing to loop on).

## What gets written where

```
approval_dir/
├── dagster-io_dagster#30000.json      ← IN: rejection token (moved once processed)
├── .feedback/
│   └── dagster-io_dagster#30000.txt   ← OUT: human feedback for next iteration
├── .state/
│   └── dagster-io_dagster#30000.json  ← OUT: iteration counter + history
└── .consumed/
    ├── dagster-io_dagster#30000.iter1.json    ← moved consumed tokens
    └── dagster-io_dagster#30000.iter2.json
```

Once `iterations` reaches `max_iterations`, subsequent rejections write
`.consumed/{key}.exhausted.json` markers and are logged but NOT retried.

## What it doesn't do

- **Doesn't judge the feedback quality.** If the human writes garbage, the agent gets garbage. Wire an LLM-judge upstream if you want feedback-quality gating.
- **Doesn't clean up `.feedback/` files.** Retained for audit; rotate manually if disk grows.
- **Doesn't cancel in-flight runs.** If a rejection lands while a re-run is already going, the new RunRequest queues normally — Dagster's concurrency controls handle the rest.
