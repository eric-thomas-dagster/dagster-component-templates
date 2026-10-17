# Governed invoice matching — agents decide *how*, the workflow decides what must be *true*

A five-step pipeline where every judgment call is a recorded, auditable asset and the risky action is gated:

| Step | Op | What it adds |
|---|---|---|
| `facts` | `cascade` | Cheap local model first (`ollama/qwen3:8b`); escalates to a stronger model **only** when the JSON is unusable. `escalations` records which model answered and why earlier ones were skipped — cost control with evidence. |
| `decision` | `decide` | One of three **named outcomes** + the reason + quoted evidence + confidence. An off-menu answer falls back to `needs_review`, never to `auto_pay`. |
| `grounded` | `verify` | Is the decision's reason actually supported by the extracted facts? Verdict, score, and the unsupported claims. |
| `gate` | `approve` + `when` | A human is asked **only for the exceptions**. A pending record lands in `./approvals/pending/` (the review queue); the reviewer writes `./approvals/INV-2291.json`. No answer ⇒ the safe default (**do not pay**). |
| `record` | `llm_call` + `when` | Writes the audit line; skipped unless the invoice was cleared. |

Governance, declared once: the pipeline `policy:` (env allowlist, spend/time budget) and a **per-step** `policy:` on `gate`
(it may touch only `./approvals` and make no network calls — a step can only be *more* restricted than the pipeline, never less).
Every step carries `…__pipeline_version` metadata and a per-step `code_version`, so editing one step's prompt marks exactly that
step (and what depends on it) stale.

## Approving

```bash
# the reviewer (or a Slack bot / Retool form / another asset) approves the pending record AS WRITTEN: the token copies the
# record's fingerprint, so it is bound to exactly what was shown (an edited summary needs a fresh approval):
jq '.approve_by_writing.json + {approver: "eric@dagsterlabs.com", reason: "PO confirmed by phone", approved_at: (now | todate)}' \
  approvals/pending/INV-2291.json > approvals/INV-2291.json
dg launch --assets 'invoice*'
```

`reuse: once` spends the approval when a run passes the gate; `max_age_hours: 72` expires an unused one. Same token shape as the standalone [`human_approval_gate`](../../../human_approval_gate/README.md).

## What is verified

`assets/ai/agentic_pipeline/tests/test_example_governed_invoice.py` loads **this exact `pipeline.yaml`** and materializes it through
a real Dagster run with only the model call scripted, covering: cascade escalation, the exception path (pending record, rejected
by default, approved with a token), and the routine path (gate skipped, no human, no queue entry). **It has not been run against
a live Ollama/OpenAI** — set `OLLAMA_API_BASE` and `OPENAI_API_KEY` to try it.
