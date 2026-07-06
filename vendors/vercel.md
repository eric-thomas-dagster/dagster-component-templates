# Vercel

[Vercel](https://vercel.com/) does two things Dagster customers care about: **frontend deploys** (Next.js / Vite / Astro / Nuxt / etc. via git-push CI) and, via **AI Gateway**, unified multi-provider LLM routing (OpenAI, Anthropic, Google, xAI, Groq, etc. through a single Vercel API key).

Vercel is not a data orchestrator — it's a deploy + edge platform. Dagster's role in a Vercel-adopting stack: gate downstream data work on production deployments going green, put the deploy stream in the catalog for lineage, and use the AI Gateway as an OpenAI-compatible endpoint for gateway-routed agents.

The community registry ships **3 Vercel components** today.

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`vercel_deployment_sensor`](https://dagster-component-ui.vercel.app/c/vercel_deployment_sensor) | sensor | Polls Vercel `/v6/deployments` for a project (`production` / `preview` / `development`). On terminal `READY`, emits `AssetMaterialization` / `AssetObservation` with commit SHA + branch + deployment URL + commit message, and fires a downstream Dagster job. Optionally fires on `ERROR` too for alert / rollback flows. | `live` |
| [`external_vercel_deployment`](https://dagster-component-ui.vercel.app/c/external_vercel_deployment) | external asset | Declare-only `AssetSpec` — surfaces a Vercel project's deployment stream in the Dagster catalog with clickable Vercel dashboard + production URLs. Downstream `deps:` work, lineage shows up. No execution. | `live` |
| [`vercel_ai_gateway_agent`](https://dagster-component-ui.vercel.app/c/vercel_ai_gateway_agent) | ai | Single-shot LLM agent via `https://ai-gateway.vercel.sh/v1` — OpenAI-compatible under the hood, so one Vercel key routes to any provider (`openai/gpt-4o`, `anthropic/claude-sonnet-4-6`, `google/gemini-2.5-flash`, `xai/grok-4`, `groq/llama-3.3-70b`, …). Optional `fallback_models:` chain for automatic retry across providers on rate-limit / outage. Full MCP tool-calling support. | `live` |

## Two independent surfaces, one vendor

Vercel Deploy and Vercel AI Gateway are separate products under the same account, each with its own auth model:

- **Deployments API** (used by `vercel_deployment_sensor` + `external_vercel_deployment`): standard Vercel API tokens (`vcp_...`) — create at https://vercel.com/account/tokens with **Read Access** scope.
- **AI Gateway** (used by `vercel_ai_gateway_agent`): AI-Gateway-scoped keys (`vck_...`) with a positive credit balance — create in the Vercel dashboard's AI Gateway section. Requires `temporalio`-style separate scoping; the deployment token does NOT grant AI Gateway access.

Both use `Authorization: Bearer <token>` — just different tokens for different endpoints.

## Pairing pattern (deployments)

Vercel owns the deploy schedule (git push, deploy hook, cron); Dagster observes. Pair `external_vercel_deployment` + `vercel_deployment_sensor` on the same `asset_key`. Downstream Dagster assets add `deps: ["vercel/site/production"]` to gate on production being live.

Same one-owner rule as the Temporal / Argo patterns: don't try to "trigger" Vercel deploys from Dagster — the natural source is git.

## Connection / auth — quick reference

| Surface | Setting | Notes |
|---|---|---|
| Vercel API base | `api_base_url: https://api.vercel.com` | Default. |
| Deployment API version | `/v6/deployments` (baked in) | Stable current version — `/v13/*` is not a valid Vercel API version. |
| Deployment token | `api_token_env_var: VERCEL_API_TOKEN` | `vcp_...` — Read Access sufficient. |
| Team scope | `team_id: team_...` | Required for team-scoped projects. |
| AI Gateway base | `api_base_url: https://ai-gateway.vercel.sh/v1` | Default on `vercel_ai_gateway_agent`. |
| AI Gateway token | `api_key_env_var: VERCEL_AI_TOKEN` (or override) | `vck_...` — requires a positive credit balance. |

## Walkthroughs

- [Vercel Deployment](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/vercel_deployment.md) — sensor + external asset live-validated against a real Vercel project. Observed the `dagster-component-ui` production deployment (commit SHA + branch + URL) in one tick.
- [Vercel AI Gateway Agent](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/vercel_ai_gateway_agent.md) — three-provider demo: same Vercel key routes `openai/gpt-4o-mini`, `anthropic/claude-haiku-4-5`, and `google/gemini-2.5-flash` in one project.

## Where Dagster adds value that Vercel alone doesn't

- **Downstream gating.** Vercel says "deploy is READY." Dagster fires the post-deploy Playwright suite, the CDN warmer, the search-index rebuild — and marks any of those as blocked if the deploy fails.
- **Lineage across the stack.** The production deploy shows up in the same catalog as your Snowflake tables + dbt models + dashboards. Downstream data assets can express "depends on prod being green" as a first-class dep.
- **Multi-provider agents in a broader DAG.** Vercel AI Gateway routes to any LLM; Dagster asset materializations wire the LLM calls into a full data pipeline (partitions, retries, freshness, checks, downstream `deps:`) that the gateway alone doesn't offer.
- **Preview environment analytics.** Second sensor with `target: preview` + a different `asset_key` feeds a preview-analytics ETL for QA reporting.

## Gotchas — most now handled by the components

- **~~Deployments API version.~~** The sensor uses `/v6` (stable). Fixed.
- **Two credential classes.** A Vercel API token (`vcp_...`) does NOT grant AI Gateway access. AI Gateway needs a separate `vck_...` credit-backed token. **The components now detect the wrong prefix and fail fast with an actionable message** — no silent 401s.
- **AI Gateway credit balance.** Zero-balance accounts get `402 insufficient_funds`. **`vercel_ai_gateway_agent` now catches this** and raises a friendly error pointing at the top-up URL.
- **`max_output_tokens` minimum.** Vercel AI Gateway enforces a minimum of 16 output tokens per call. **`vercel_ai_gateway_agent` now auto-clamps** `max_tokens < 16` up to 16 with a warning log — silently-works instead of a cryptic 400.
- **AI Gateway model strings.** Use `<provider>/<model>` format (e.g. `openai/gpt-4o-mini`, `anthropic/claude-sonnet-4-6`). See https://vercel.com/ai-gateway/models for the current catalog.
