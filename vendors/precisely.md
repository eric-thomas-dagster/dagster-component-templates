# Precisely

[Precisely](https://www.precisely.com/) is a data-integrity vendor with products spanning ETL, address verification, data quality, catalog/governance, and mainframe-to-cloud data movement. The community registry covers the Connect ETL + Data Integrity Suite (DIS) + Data360 surfaces with **6 components** today.

## Components

### Connect ETL — schedule observation + triggering

The mainframe-era ETL product (formerly DMExpress / Syncsort DMX). Connect ETL publishes a thin REST surface that the registry models in two complementary ways depending on **who owns the schedule**:

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`external_precisely_job`](https://dagster-component-ui.vercel.app/c/external_precisely_job) | external asset | Declare-only `AssetSpec`. Surfaces a Connect ETL job as a real catalog asset — downstream `deps:` work, lineage shows up, materialization history is real. No execution. | `code` |
| [`precisely_job_sensor`](https://dagster-component-ui.vercel.app/c/precisely_job_sensor) | sensor | Polls the documented `GET /projects/{jobRunId}/status` endpoint. On terminal `COMPLETED` / `COMPLETED_WITH_WARNINGS`, emits `AssetMaterialization` (or `AssetObservation` — see `asset_event_type`) for the paired asset key, AND fires a `RunRequest` for any downstream Dagster job. | `code` |
| [`precisely_connect_job_trigger`](https://dagster-component-ui.vercel.app/c/precisely_connect_job_trigger) | infrastructure | Materializable asset that POSTs to `/runtask` to trigger a Connect ETL job. Captures `jobRunId` in metadata so a paired sensor can watch it. | `code` |

**Two valid pairings — pick ONE per `asset_key`:**

| Case | Schedule owner | Components | Materialization timeline |
|---|---|---|---|
| **A. Observe-only** | Precisely | `external_precisely_job` + `precisely_job_sensor` (`asset_event_type=materialization`) | Sensor emits AssetMaterialization on terminal SUCCESS |
| **B. Dagster-triggered** | Dagster | `precisely_connect_job_trigger` + optional `precisely_job_sensor` (`asset_event_type=observation`) | Trigger emits Materialization on `/runtask` submit; sensor emits Observation on terminal SUCCESS |

**Do NOT mix** `external_precisely_job` and `precisely_connect_job_trigger` on the same `asset_key` — they're alternatives.

### Data Integrity Suite (DIS) — Data Quality

The modern cloud-native DQ surface, OAuth2 client-credentials auth against `api.data.precisely.com`.

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`precisely_dis_dq_check`](https://dagster-component-ui.vercel.app/c/precisely_dis_dq_check) | asset_check | Wraps a DIS rule set as N Dagster asset checks. Pre-resolves rule_ids at build_defs time so each becomes its own check — individually alertable, retriable, visible in the catalog UI. Per-rule pass/fail + score lands as check metadata. | `code` |

### DIS — Address Verify / Geo Addressing

CASS-certified address standardization. Distinct from the generic `geocoder` (Nominatim/Google/HERE) — this returns Precisely's normalized address + match-quality scoring, what mailing-list deliverability + USPS Move Update workflows actually need.

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`precisely_address_verify`](https://dagster-component-ui.vercel.app/c/precisely_address_verify) | transformation | Verify + standardize an address column in an upstream pandas DataFrame. Writes CASS-certified columns alongside the source — `{prefix}status` / `{prefix}standardized` / `{prefix}match_score` / `{prefix}postal_code` / `{prefix}latitude` / `{prefix}longitude`. | `code` (trial-key tier may enable `live` validation) |

### Data360 Govern — catalog / lineage

The Precisely data catalog (formerly Infogix). OAuth2 client-credentials (same flow as DIS).

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`lineage_to_data360`](https://dagster-component-ui.vercel.app/c/lineage_to_data360) | sink | Push Dagster `lineage_graph` to Data360 Govern. Upserts assets via `POST /objects`, edges via `POST /lineage`. `only_push_on_change` dedups against payload_hash across runs. | `code` |

## Walkthrough

**[`examples/precisely_validation.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/precisely_validation.md)** — Two-component scaffold (external asset + sensor) for the Observe-only case. `setup_precisely_validation_demo.sh` scaffolds a working Dagster project in one command; flip `default_status: running` + set a real `job_run_id` once you have one.

## Connection / auth — quick reference

| Surface | Auth | Endpoint base |
|---|---|---|
| Connect ETL | Bearer token (server-issued) | `https://<your-connect-host>` |
| DIS (DQ + Address Verify) | OAuth2 client-credentials | `https://api.data.precisely.com` |
| Data360 Govern | OAuth2 client-credentials | `https://api.data.precisely.com/data360/catalog` |

Recommended env-var convention:
- `PRECISELY_HOST` — Connect ETL host
- `PRECISELY_API_TOKEN` — Connect ETL bearer
- `PRECISELY_DIS_CLIENT_ID` / `PRECISELY_DIS_CLIENT_SECRET` — DIS + Data360 OAuth2

## Gotchas

- **Connect ETL has no public list-runs endpoint.** The sensor polls one specific `job_run_id`. For "watch the latest run" semantics, either (a) use the trigger component (Dagster knows the run_id it just submitted) or (b) configure Precisely to write run IDs to a known location and monitor that with `s3_monitor` / equivalent. We do not attempt to call undocumented Precisely endpoints.
- **Job Status returns plain text**, not JSON. The sensor handles this; if you write a custom op against the same endpoint, don't `resp.json()`.
- **DIS rule_ids commonly have hyphens** (`rule-not-null-order_id`). Dagster check names must match `^[A-Za-z0-9_]+$`; the component sanitizes for the check name but passes the original rule_id to the DIS API.
- **Connect ETL 4.x vs 5.x:** `/runtask` returns plain text in 4.x and JSON `{"jobRunId": "..."}` in 5.x. The trigger component handles both.
- **Address Verify trial key** exists for small-volume testing — see [docs](https://docs.precisely.com/docs/sftw/precisely-apis/). Sustained traffic needs a paid tenant.

## Roadmap — what's NOT shipped yet (per customer demand)

- **Precisely Spectrum dataflow asset + sensor** — on-prem Spectrum Technology Platform integration. Same shape as the Connect ETL pair, different REST surface. Open if a customer needs it.
- **`precisely_job_sensor` enhancement: `job_run_id_from_asset_key`** — auto-discover the run_id from the latest materialization metadata of a paired trigger asset. Closes the Case B loop fully (no static `job_run_id` config). Open in the registry's follow-ups list.

## See also

- [Precisely documentation](https://help.precisely.com/r/)
- [DIS REST API docs](https://docs.precisely.com/docs/sftw/precisely-apis/)
- [Data360 Govern docs](https://help.precisely.com/r/Data360-Govern)
