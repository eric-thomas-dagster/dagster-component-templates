# `PayscaleResourceComponent`

Registers a `PayscaleResource` (OAuth2 `client_credentials` grant) wrapping PayScale's **Jobalyzer** compensation-benchmarking API for other components to use via `resource_key`.

## ⚠️ PayScale credentials are NOT self-serve

**There is no signup form for Jobalyzer API access.** Unlike most integrations in this repo, you cannot create a developer account, generate a key, and start calling this API on your own. `client_id`, `client_secret`, and `customer_id` are only issued once your organization has a **direct commercial agreement with PayScale** — the same account team that sells PayScale's compensation-data subscriptions provisions API access as part of that deal. developers.payscale.com documents the API in full technical detail (it's a real, well-documented REST API), but it does not expose any "create an application" or "get an API key" flow. If you don't already have credentials from a PayScale account rep, nothing in this component will get you any — don't spend time looking for a public signup page.

## What is Jobalyzer?

Jobalyzer is **not** a bulk/paginated data feed. It is a per-lookup REST service: you submit "compensable factors" for a single job (title, location, years of experience, education, skills, certifications, ...) and PayScale returns a compensation benchmark report built from its salary-survey database. Each lookup is a billable "report" (PayScale charges per report via consumable "report credits"; see `report_charges` in PayScale's docs). This is why `payscale_compensation_enrichment` calls it once per DataFrame row, the same shape as this repo's `geocoder`/`reverse_geocoder` components, rather than pulling a bulk export.

## Auth: client_credentials, verified against developers.payscale.com

Token endpoint: `POST https://accounts.payscale.com/connect/token`

```
Content-Type: application/x-www-form-urlencoded

client_id=...&client_secret=...&grant_type=client_credentials&scope=jobalyzer
```

Response:

```json
{"access_token": "...", "token_type": "Bearer", "expires_in": 600}
```

Tokens are short-lived (~10 minutes per PayScale's documentation) and are cached in-memory by this resource, refreshed a little before expiry.

## Report flow: submit, then poll

PayScale's `/reports` endpoint is **asynchronous** — the POST does not return the report itself, it returns links to poll:

```
POST https://jobalyzer.payscale.com/jobalyzer/v1/reports
Authorization: Bearer <token>
Content-Type: application/json

{
  "customerId": "<your PayScale customer id>",
  "user": "<your PayScale customer id>",
  "AutoResolveJobTitle": true,
  "requestedReports": ["pay"],
  "answers": {
    "JobTitle": "Software Developer",
    "City": "Seattle",
    "State": "Washington",
    "Country": "United States",
    "YearsExperience": 5,
    "HighestDegreeEarned": "Bachelor's Degree",
    "Skills": ["Python", "JavaScript"]
  }
}
```

```json
{
  "Links": {
    "Self": "https://jobalyzer.payscale.com/jobalyzer/v2/reports/<id>",
    "PayReport": "https://jobalyzer.payscale.com/jobalyzer/v1/reports/<id>/pay",
    "YearsExperienceReport": "https://jobalyzer.payscale.com/jobalyzer/v2/reports/<id>/yoe"
  },
  "Warnings": null,
  "Errors": null
}
```

The caller then `GET`s the relevant link (e.g. `Links.PayReport`) repeatedly until PayScale returns HTTP 200 — PayScale's own docs describe this as polling "until status code 200 is received." This resource's `poll_until_ready()` does that loop for you (treating `202`/`204` as "still processing" and anything else as a real error), bounded by `poll_timeout_seconds`.

`get_pay_report(answers)` is the one-call convenience that does submit + poll and returns the finished Pay report body.

## Pay report shape (verified field names)

The Pay report contains multiple sub-reports — `BasePayReport`, `TotalPayReport`, `HourlyPayReport`, `Bonus`, `Commission`, `ProfitShare` — each with:

| Field | Meaning |
|---|---|
| `Percentile10` / `Percentile25` / `Percentile50` / `Percentile75` / `Percentile90` | Pay distribution percentiles (median = `Percentile50`) |
| `Average` | Mean pay |
| `Count` | Number of profiles the figure is based on (PayScale caps this at 45) |
| `Percent` | % of matched profiles reporting this pay component |
| `CurrencyName` / `CurrencyFormat` | Currency of the figures |

Report-level fields: `ReportRating` (0–1 data-quality score), `TotalProfilesAnalyzed`, `TotalCompanies`, `TotalIncumbents`, `LevelUsed` (`Metro`/`State`/`Country` — how specific a geography PayScale could match), and `Context.MatchedJobTitle` / `Context.JobTitleRating` (how well your `JobTitle` input matched a standardized PayScale title, especially relevant when `AutoResolveJobTitle: true`).

## What this resource exposes

| Method | PayScale call | Purpose |
|---|---|---|
| `submit_report_request(answers, requested_reports, auto_resolve_job_title)` | `POST /reports` | Submit compensable factors; returns the `Links`/`Warnings`/`Errors` envelope (not the report). |
| `poll_until_ready(report_url)` | `GET <link>` (repeated) | Poll a report link to completion. |
| `get_pay_report(answers, requested_reports, auto_resolve_job_title)` | submit + poll | Convenience: returns the finished Pay report body directly. |

Every actual HTTP call — token fetch, submit, and poll — routes through one module-level function, `_payscale_http_request`, so tests only ever need to monkeypatch that one function.

## Pairs with

- **`payscale_compensation_enrichment`** — per-row DataFrame enrichment asset built on top of this resource (the intended consumer).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `payscale_resource`) | Key used to register this resource. |
| `customer_id` | **required** | PayScale-issued customer/client identifier (not self-serve). |
| `client_id_env_var` | optional (default `PAYSCALE_CLIENT_ID`) | Env var holding the OAuth2 client ID. |
| `client_secret_env_var` | optional (default `PAYSCALE_CLIENT_SECRET`) | Env var holding the OAuth2 client secret. |
| `token_url` | optional | OAuth2 token endpoint. |
| `api_base_url` | optional | Jobalyzer REST API base URL. |
| `scope` | optional (default `jobalyzer`) | OAuth2 scope. |
| `request_timeout_seconds` | optional (default `30`) | Per-HTTP-call timeout. |
| `poll_interval_seconds` | optional (default `2.0`) | Delay between poll attempts. |
| `poll_timeout_seconds` | optional (default `60.0`) | Max time to wait for a report before raising `TimeoutError`. |

## Example

```yaml
type: dagster_component_templates.PayscaleResourceComponent
attributes:
  resource_key: payscale_resource
  customer_id: "123456"
  client_id_env_var: PAYSCALE_CLIENT_ID
  client_secret_env_var: PAYSCALE_CLIENT_SECRET
```

## Sources

Verified directly against PayScale's own documentation at developers.payscale.com: `jobalyzer/index.html`, `jobalyzer/patterns.html` (auth + request/response shape), `jobalyzer/report_charges.html` (billing model), `jobalyzer/reports.html` (Pay report field names), `jobalyzer/definitions.html` (compensable-factor / `answers` field names), `jobalyzer/troubleshooting.html` (error shapes).
