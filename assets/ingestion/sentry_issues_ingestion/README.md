# sentry_issues_ingestion

Pull issues from a Sentry project via the REST API into a pandas DataFrame. Uses `Link`-header cursor pagination.

## Prereqs

- Sentry auth token with `project:read` scope. Create at Organization Settings → Auth Tokens.
- Set the token in the env var referenced by `api_token_env_var` (default `SENTRY_AUTH_TOKEN`).

## Fields

- `asset_name` — output asset name.
- `organization` / `project` — the slugs from your Sentry URL (`sentry.io/organizations/<org>/issues/?project=<num>`).
- `api_host` — `https://sentry.io` (default) or self-hosted URL.
- `api_token_env_var` — env var holding the token. Default `SENTRY_AUTH_TOKEN`.
- `query` — Sentry search query (default `is:unresolved`). See [Sentry search docs](https://docs.sentry.io/product/sentry-basics/search/).
- `stats_period` — time range (`24h`, `7d`, `30d`, or `None`).
- `limit` — total row cap across pages.

## Output

DataFrame with columns like `id, shortId, title, culprit, level, status, count, userCount, firstSeen, lastSeen, metadata.*, project.*, permalink`. Nested fields flattened via `pd.json_normalize`.

## Related

- `dataframe_to_sentry` — send events / metrics TO Sentry
- `posthog_events_ingestion` — pair with PostHog for error-vs-behavior correlation
- `dataframe_to_snowflake` / `dataframe_to_bigquery` — persist issues to your warehouse for long-term trends
