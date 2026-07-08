# posthog_events_ingestion

Pull events from a PostHog project into a pandas DataFrame. Handles cursor pagination automatically.

## Prereqs

- Personal API key with `query:read` scope. Generate at Account Settings → Personal API Keys.
- Set the token in the env var referenced by `api_key_env_var` (default `POSTHOG_API_KEY`).

## Fields

- `asset_name` — output asset name.
- `project_id` — PostHog project ID (numeric — check your project settings).
- `api_host` — `https://us.i.posthog.com` (default) / `https://eu.i.posthog.com` / self-hosted URL.
- `api_key_env_var` — env var holding the API key. Default `POSTHOG_API_KEY`.
- `event` — optional event name filter (e.g. `$pageview`, `checkout_completed`).
- `since_days` — how many days back to pull (1–90).
- `limit` — max total rows fetched across paginated pages.

## Output

DataFrame with columns from PostHog events (id, event, distinct_id, timestamp, properties.*, person.*, etc.). Nested fields flattened with `.` separator via `pd.json_normalize`.

## Related

- `posthog_persons_ingestion` — persons (identified users + traits)
- `sentry_issues_ingestion` — pair with Sentry for error-vs-behavior correlation
- `dataframe_to_snowflake` / `dataframe_to_bigquery` — persist events to your warehouse
