# SAP Job Trigger Asset Component

Start an SAP background job or BW process chain from Dagster, then wait for it to finish — the same "trigger external job, poll, surface the vendor's own error" shape as [`abinitio_run_asset`](../abinitio_run_asset/), just over SAP's RFC protocol instead of REST. Dagster owns the schedule; SAP executes the work.

Pairs with [`sap_rfc_resource`](../../../resources/sap_rfc_resource/) which holds the connection. See its README for the SAP NetWeaver RFC SDK install steps. See [`sap_rfc_ingestion`](../../ingestion/sap_rfc_ingestion/) for reading SAP tables/BAPIs into a DataFrame instead of triggering a job.

## Not verified against a live SAP system

No pyrfc/SAP sandbox is available in this repo's development environment — the same constraint `sap_rfc_ingestion` already documents. `BAPI_XMI_LOGON`/`BAPI_XMI_CLOSE` and the `BAPI_XBP_*` family are SAP's own publicly documented **XBP (External Background Processing) interface** — the same one third-party schedulers (RunMyJobs, Control-M, Automic, Redwood) use to trigger and monitor SAP background jobs — not a guess at an undocumented API. Each BAPI's `RETURN` table is checked generically against the standard BAPI convention (`{TYPE, ID, NUMBER, MESSAGE}` structs, `TYPE` in `S`/`W`/`E`/`A`/`I`). The `RSPC_API_*` process-chain function modules are widely used in practice (BW consulting references and third-party function-module catalogs such as se80.co.uk/sapdatasheet.org) but, unlike XBP, are **not** an SAP-supported external interface — confirm their exact signature on your BW release before relying on `process_chain` mode. Their parameter names below (`I_CHAIN`/`I_LOGID`/`E_LOGID`/`E_STATUS`/`E_MANUAL_ABORT`/`E_T_LOG`) come from those community/catalog sources, not SAP's own documentation, so they're a real improvement over guessing but still worth checking in SE37 on your system. `status_field`/`success_statuses`/`failure_statuses` are deliberately configurable rather than hardcoded, since the exact status vocabulary a given system returns can vary by release — `process_chain`'s defaults (`G`/`F` success, `R`/`X`/`J` failure) are themselves forum-sourced, not SAP-documented.

The exact XBP call signatures (`BAPI_XMI_LOGON` requiring `EXTCOMPANY`/`EXTPRODUCT`, `EXTERNAL_USER_NAME` on every job call, `BAPI_XBP_JOB_ADD_ABAP_STEP`/`_START_IMMEDIATELY`/`_JOBLOG_READ`'s real names and parameters, and `BAPI_XBP_JOB_STATUS_GET`'s one-character `STATUS` vocabulary) were checked against SAP's XBP 6.10 interface documentation and corrected to match it. Still confirm against a current ABAP system before relying on this in production — XBP 2.0 has been stable for a long time, but the documentation used to check this is old.

## `target_type: background_job`

Logs on via the XBP interface, then either starts an already-scheduled job immediately, or creates and starts a new one-step job:

```yaml
type: dagster_community_components.SapJobTriggerAssetComponent
attributes:
  asset_key: sap/nightly_close
  sap_rfc_resource_key: sap_ecc
  target_type: background_job
  job_name: ZNIGHTLY_CLOSE
  program_name: ZNIGHTLY_CLOSE_PROG   # create+start a new one-step job
  variant_name: PROD_VARIANT
  poll_interval_seconds: 30
  timeout_seconds: 7200
```

To instead start a job that's already scheduled in SM36/SM37 (common when the job's steps are configured outside Dagster), set `job_count` and leave `program_name` unset:

```yaml
  job_name: ZNIGHTLY_CLOSE
  job_count: "12345678"   # BAPI_XBP_JOB_START_IMMEDIATELY
```

On failure, the asset raises with the job's own log (`BAPI_XBP_JOB_JOBLOG_READ`) appended.

`xmi_ext_company`/`xmi_ext_product` identify the calling external system to `BAPI_XMI_LOGON` (both required by the XBP interface); `external_user_name` is sent on every XBP call (required by `_OPEN`/`_ADD_ABAP_STEP`/`_CLOSE`/`_START_IMMEDIATELY`). `target_server`/`job_class` are optional. `success_statuses`/`failure_statuses` default to `['F']`/`['A']` for `background_job` mode -- `BAPI_XBP_JOB_STATUS_GET`'s documented one-character `STATUS` code (`P` scheduled, `S` released, `Y` ready, `R` active, `F` finished, `A` cancelled).

## `target_type: process_chain`

Starts a BW process chain (`RSPC_API_CHAIN_START`, `I_CHAIN=process_chain_id`) and polls its status (`RSPC_API_CHAIN_GET_STATUS`, `I_CHAIN`/`I_LOGID`):

```yaml
type: dagster_community_components.SapJobTriggerAssetComponent
attributes:
  asset_key: sap/bw_daily_chain
  sap_rfc_resource_key: sap_bw
  target_type: process_chain
  process_chain_id: ZDAILY_LOAD_CHAIN
```

On failure, the asset raises with the chain's own log (`RSPC_API_CHAIN_GET_LOG`) appended. If `RSPC_API_CHAIN_GET_STATUS`'s `E_MANUAL_ABORT` flag comes back set, the run is treated as failed immediately, even if `E_STATUS` hasn't yet settled into one of `failure_statuses`.

## Polling and status vocabulary

`poll_interval_seconds` / `timeout_seconds` bound the wait the same way as `abinitio_run_asset` — the poll loop's own `while elapsed < timeout_seconds` enforces the timeout, not a background-thread wrapper. A poll call that itself errors (a transient RFC communication blip) is logged and retried, not treated as job failure.

`status_field` names the field read off each poll response; `success_statuses`/`failure_statuses` classify its value (case-insensitive). Anything not in either list keeps polling. Defaults are per `target_type`: `background_job` reads `STATUS` from `BAPI_XBP_JOB_STATUS_GET` (`['F']` success, `['A']` failure); `process_chain` reads `E_STATUS` from `RSPC_API_CHAIN_GET_STATUS` (`['G','F']` success, `['R','X','J']` failure — forum-sourced, confirm against your system).

## `run_window`, `pool`, `dry_run`

Same conventions as `sql_asset`/`task_chain_asset`: `run_window` skips cleanly outside a configured time window (e.g. don't trigger a nightly job outside its real maintenance window), `pool` caps concurrent triggers via Dagster's named concurrency pools, and `dry_run` logs what would be triggered without opening a connection at all. To share one `run_window` value across several assets instead of repeating the same literal string in each component instance, see `task_chain_asset`'s README ("Sharing one `run_window` across several assets") for a `template_vars_module` example.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | Dagster asset key (e.g. 'sap/nightly_close'). |
| `sap_rfc_resource_key` | `str` | Key of a registered sap_rfc_resource (provides the RFC connection). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `float` | `15.0` | Seconds between status polls. |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for the job/chain to finish before raising a timeout error. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"sap"` | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `deps` | `List[str]` | — | Upstream Dagster asset keys for lineage. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. Auto-includes 'sap' if empty. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | — | ABAP background job name (background_job mode; required then). |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `target_type` | `Literal['background_job', 'process_chain']` | `"background_job"` | 'background_job': start an ABAP background job via the XBP (External Background Processing) interface. 'process_chain': start a BW process chain. |
| `target_server` | `str` | — | Optional application server to run the job on (BAPI_XBP_JOB_START_IMMEDIATELY / _CLOSE's TARGET_SERVER). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `job_count` | `str` | — | If set, start this already-scheduled job (job_name + job_count) immediately via BAPI_XBP_JOB_START_IMMEDIATE, instead of creating a new one-step job. Takes precedence over program_name/variant_name when both are set. |
| `program_name` | `str` | — | ABAP program to run as the job's one step, used when job_count is unset -- a new job is opened (BAPI_XBP_JOB_OPEN), this program added as its only step (BAPI_XBP_JOB_ADD_STEP), then closed and started immediately (BAPI_XBP_JOB_CLOSE). |
| `variant_name` | `str` | — | ABAP program variant (optional; program_name's own default variant is used when unset). |
| `process_chain_id` | `str` | — | BW process chain technical ID (process_chain mode; required then). |
| `xmi_ext_company` | `str` | `"Dagster"` | BAPI_XMI_LOGON EXTCOMPANY value -- required by the XBP interface; identifies the calling external company/organization. |
| `xmi_ext_product` | `str` | `"dagster-community-components"` | BAPI_XMI_LOGON EXTPRODUCT value -- required by the XBP interface; identifies the calling external product. |
| `xmi_interface` | `str` | `"XBP"` | BAPI_XMI_LOGON INTERFACE value -- the external interface this component identifies itself as. |
| `xmi_version` | `str` | `"3.0"` | BAPI_XMI_LOGON VERSION value. |
| `external_user_name` | `str` | `"DAGSTER"` | EXTERNAL_USER_NAME sent on every XBP call -- required by BAPI_XBP_JOB_OPEN / _ADD_ABAP_STEP / _CLOSE / _START_IMMEDIATELY; optional on _STATUS_GET / _JOBLOG_READ (sent there too, for consistency). |
| `job_class` | `str` | — | Optional ABAP job class (BAPI_XBP_JOB_OPEN's JOBCLASS, e.g. 'A'/'B'/'C'). |
| `status_field` | `str` | — | Field name read off the status-poll response to determine job/chain state. Defaults by target_type when unset: background_job -> 'STATUS' (BAPI_XBP_JOB_STATUS_GET's documented one-character STATUS code); process_chain ->… _(full docs in schema.json + component README)_ |
| `success_statuses` | `List[str]` | — | status_field values (case-insensitive) treated as success. Defaults by target_type when unset: background_job -> ['F'] (BAPI_XBP_JOB_STATUS_GET's documented one-character STATUS code for 'finished'); process_chain -> ['G… _(full docs in schema.json + component README)_ |
| `failure_statuses` | `List[str]` | — | status_field values (case-insensitive) treated as failure. Any other value keeps polling. Defaults by target_type when unset: background_job -> ['A'] ('cancelled'); process_chain -> ['R','X','J'] (forum-sourced 'ended wi… _(full docs in schema.json + component README)_ |
| `trigger_params` | `Dict[str, Any]` | — | Extra kwargs merged into the trigger call (e.g. job-step print parameters not covered by variant_name). |
| `dry_run` | `bool` | `false` | Log what would be triggered and skip opening a connection entirely -- no resource is looked up, no RFC call is made. Output metadata always includes dry_run so a materialization history shows which runs were real. |
| `run_window` | `str` | — | 'HH:MM-HH:MM tz [day,day,...]' (e.g. '22:00-05:00 Europe/Berlin mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of triggering or failing -- no SAP job/chain is started, and real downstream dep… _(full docs in schema.json + component README)_ |
| `pool` | `str` | — | Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number of slots shared across every asset/op using the same pool name, e.g. a cap on concurrent SAP background-job triggers). The pool's own slot… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## See also

- [`sap_rfc_resource`](../../../resources/sap_rfc_resource/) — connection registration + SDK install instructions
- [`sap_rfc_ingestion`](../../ingestion/sap_rfc_ingestion/) — read SAP tables/BAPIs into a DataFrame
- [`abinitio_run_asset`](../abinitio_run_asset/) — the same trigger-and-poll shape over REST
