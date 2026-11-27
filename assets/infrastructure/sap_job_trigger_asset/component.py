"""SAP Job Trigger Asset Component.

Starts an SAP background job (via the SAP-documented XBP -- External Background
Processing -- interface used by third-party schedulers such as RunMyJobs,
Control-M, Automic, Redwood) or a BW process chain, then polls until it
finishes, raising with the SAP job/chain log on failure. Dagster owns the
schedule; SAP executes the work -- the same "trigger external job, poll,
surface the vendor's own error" shape as abinitio_run_asset, just over RFC
instead of REST.

Connection comes from `sap_rfc_resource_key` (a registered sap_rfc_resource).
Install pyrfc + the SAP NW RFC SDK first -- see `sap_rfc_resource`'s README.

NOT verified against a live SAP system -- no pyrfc/SAP sandbox is available in
this repo's development environment (the same constraint sap_rfc_ingestion
already documents). BAPI_XMI_LOGON/BAPI_XMI_CLOSE and the BAPI_XBP_* family are
SAP's own publicly documented External Interface for job scheduling (XBP),
not a guess at an undocumented API. The exact shape of each BAPI's RETURN
table (TYPE/ID/NUMBER/MESSAGE) follows SAP's standard BAPI convention and is
checked generically.

The RSPC_API_* process-chain function modules are widely used in practice
(BW consulting references and third-party function-module catalogs, not an
SAP-supported external interface the way XBP is) -- confirm their exact
signature on your BW release before relying on process_chain mode. Their
importing/exporting parameter names below (I_CHAIN/I_LOGID/E_LOGID/E_STATUS/
E_T_LOG) are taken from those community/catalog sources, not SAP's own
documentation, so they are more trustworthy than a guess but still worth
checking against SE37 on your system: RSPC_API_CHAIN_START takes I_CHAIN
(+ optional I_T_VARIABLES/I_SYNCHRONOUS/I_SIMULATE/I_NOPLAN), returns
E_LOGID, and raises the ABAP exception FAILED on error (surfaced by the RFC
layer itself, not a RETURN table). RSPC_API_CHAIN_GET_STATUS (not
RSPC_API_CHAIN_STATUS, which does not exist) takes I_CHAIN/I_LOGID and
returns E_STATUS/E_MANUAL_ABORT/E_MESSAGE. RSPC_API_CHAIN_GET_LOG (not
RSPC_API_CHAIN_VIEW_LOG, which does not exist) takes I_CHAIN/I_LOGID and
returns a table, E_T_LOG. `status_field`/`success_statuses`/
`failure_statuses` are deliberately configurable rather than hardcoded,
since the exact status vocabulary a given system returns can vary --
process_chain's defaults (G/F success, R/X/J failure) are themselves
forum-sourced, not SAP-documented.
"""
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# BEGIN run_window (inlined per component; keep copies identical — ported verbatim from
# sensors/event_automation's own business_hours_only parser/checker, same format and same
# fail-open-on-malformed-input behavior, just renamed for this field's own name)
_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_run_window(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays). Raises
    ValueError on malformed input."""
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"run_window needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
    window, tz_name = parts[0], parts[1]
    start_str, _, end_str = window.partition("-")
    sh, _, sm = start_str.partition(":")
    eh, _, em = end_str.partition(":")
    start_min = int(sh) * 60 + int(sm or 0)
    end_min = int(eh) * 60 + int(em or 0)
    weekdays: Optional[List[int]] = None
    if len(parts) >= 3:
        weekdays = []
        for day in parts[2].split(","):
            key = day.strip().lower()[:3]
            if key in _WEEKDAY_MAP:
                weekdays.append(_WEEKDAY_MAP[key])
    return start_min, end_min, tz_name, weekdays


def _is_in_run_window(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list). Fails open
    (returns True -- never blocks) on a malformed spec or any unexpected error."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_run_window(spec)
    except Exception:
        return True
    try:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name) if tz_name.upper() != "UTC" else timezone.utc
        except Exception:
            tz = timezone.utc
        local = datetime.fromtimestamp(now_ts, tz=tz)
        cur_min = local.hour * 60 + local.minute
        if weekdays is not None and local.weekday() not in weekdays:
            return False
        if start_min <= end_min:
            return start_min <= cur_min < end_min
        return cur_min >= start_min or cur_min < end_min  # overnight window, e.g. 22:00-06:00
    except Exception:
        return True
# END run_window


def _check_bapi_return(result: dict, call_name: str) -> None:
    """Raise if a BAPI's RETURN parameter signals an error -- the standard BAPI RETURN-table
    convention ({TYPE, ID, NUMBER, MESSAGE, ...} structs; TYPE one of
    S(uccess)/W(arning)/E(rror)/A(bort)/I(nfo)). A BAPI with no RETURN parameter at all is a
    no-op here."""
    ret = result.get("RETURN") if isinstance(result, dict) else None
    if not ret:
        return
    rows = ret if isinstance(ret, list) else [ret]
    errors = [r for r in rows if isinstance(r, dict) and str(r.get("TYPE", "")).strip().upper() in ("E", "A")]
    if errors:
        messages = "; ".join(str(r.get("MESSAGE", r)) for r in errors)
        raise Exception(f"{call_name} failed: {messages}")


class SapJobTriggerAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Start an SAP background job or BW process chain and wait for it to finish.

    Two target types:

    **`target_type: background_job`** -- logs on via the XBP external interface
    (`BAPI_XMI_LOGON`), then either starts an already-scheduled job immediately
    (`job_count` set -- `BAPI_XBP_JOB_START_IMMEDIATELY`) or creates and starts a
    new one-step job (`program_name` set -- `BAPI_XBP_JOB_OPEN` /
    `BAPI_XBP_JOB_ADD_ABAP_STEP` / `BAPI_XBP_JOB_CLOSE` / `BAPI_XBP_JOB_START_IMMEDIATELY`),
    polls `BAPI_XBP_JOB_STATUS_GET` (one-character STATUS: P/S/Y/R/F/A), and on
    failure reads `BAPI_XBP_JOB_JOBLOG_READ`.

    ```yaml
    type: dagster_community_components.SapJobTriggerAssetComponent
    attributes:
      asset_key: sap/nightly_close
      sap_rfc_resource_key: sap_ecc
      target_type: background_job
      job_name: ZNIGHTLY_CLOSE
      program_name: ZNIGHTLY_CLOSE_PROG
      variant_name: PROD_VARIANT
      poll_interval_seconds: 30
      timeout_seconds: 7200
    ```

    **`target_type: process_chain`** -- starts a BW process chain
    (`RSPC_API_CHAIN_START`) and polls its status
    (`RSPC_API_CHAIN_GET_STATUS`), reading `RSPC_API_CHAIN_GET_LOG` on failure.

    ```yaml
    type: dagster_community_components.SapJobTriggerAssetComponent
    attributes:
      asset_key: sap/bw_daily_chain
      sap_rfc_resource_key: sap_bw
      target_type: process_chain
      process_chain_id: ZDAILY_LOAD_CHAIN
    ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'sap/nightly_close').")
    sap_rfc_resource_key: str = Field(
        description="Key of a registered sap_rfc_resource (provides the RFC connection)."
    )

    target_type: Literal["background_job", "process_chain"] = Field(
        default="background_job",
        description=(
            "'background_job': start an ABAP background job via the XBP (External Background "
            "Processing) interface. 'process_chain': start a BW process chain."
        ),
    )

    # --- background_job mode -------------------------------------------------
    job_name: Optional[str] = Field(
        default=None,
        description="ABAP background job name (background_job mode; required then).",
    )
    job_count: Optional[str] = Field(
        default=None,
        description=(
            "If set, start this already-scheduled job (job_name + job_count) immediately via "
            "BAPI_XBP_JOB_START_IMMEDIATE, instead of creating a new one-step job. Takes "
            "precedence over program_name/variant_name when both are set."
        ),
    )
    program_name: Optional[str] = Field(
        default=None,
        description=(
            "ABAP program to run as the job's one step, used when job_count is unset -- a new "
            "job is opened (BAPI_XBP_JOB_OPEN), this program added as its only step "
            "(BAPI_XBP_JOB_ADD_STEP), then closed and started immediately (BAPI_XBP_JOB_CLOSE)."
        ),
    )
    variant_name: Optional[str] = Field(
        default=None,
        description="ABAP program variant (optional; program_name's own default variant is used when unset).",
    )

    # --- process_chain mode --------------------------------------------------
    process_chain_id: Optional[str] = Field(
        default=None,
        description="BW process chain technical ID (process_chain mode; required then).",
    )

    # --- XBP external-interface logon (background_job mode only) ------------
    xmi_ext_company: str = Field(
        default="Dagster",
        description="BAPI_XMI_LOGON EXTCOMPANY value -- required by the XBP interface; identifies the calling external company/organization.",
    )
    xmi_ext_product: str = Field(
        default="dagster-community-components",
        description="BAPI_XMI_LOGON EXTPRODUCT value -- required by the XBP interface; identifies the calling external product.",
    )
    xmi_interface: str = Field(
        default="XBP",
        description="BAPI_XMI_LOGON INTERFACE value -- the external interface this component identifies itself as.",
    )
    xmi_version: str = Field(default="3.0", description="BAPI_XMI_LOGON VERSION value.")
    external_user_name: str = Field(
        default="DAGSTER",
        description=(
            "EXTERNAL_USER_NAME sent on every XBP call -- required by BAPI_XBP_JOB_OPEN / "
            "_ADD_ABAP_STEP / _CLOSE / _START_IMMEDIATELY; optional on _STATUS_GET / "
            "_JOBLOG_READ (sent there too, for consistency)."
        ),
    )
    target_server: Optional[str] = Field(
        default=None,
        description="Optional application server to run the job on (BAPI_XBP_JOB_START_IMMEDIATELY / _CLOSE's TARGET_SERVER).",
    )
    job_class: Optional[str] = Field(
        default=None,
        description="Optional ABAP job class (BAPI_XBP_JOB_OPEN's JOBCLASS, e.g. 'A'/'B'/'C').",
    )

    # --- Polling --------------------------------------------------------------
    poll_interval_seconds: float = Field(default=15.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(
        default=3600,
        description="Max seconds to wait for the job/chain to finish before raising a timeout error.",
    )
    status_field: Optional[str] = Field(
        default=None,
        description=(
            "Field name read off the status-poll response to determine job/chain state. Defaults "
            "by target_type when unset: background_job -> 'STATUS' (BAPI_XBP_JOB_STATUS_GET's "
            "documented one-character STATUS code); process_chain -> 'E_STATUS' "
            "(RSPC_API_CHAIN_GET_STATUS's exporting parameter)."
        ),
    )
    success_statuses: Optional[List[str]] = Field(
        default=None,
        description=(
            "status_field values (case-insensitive) treated as success. Defaults by target_type "
            "when unset: background_job -> ['F'] (BAPI_XBP_JOB_STATUS_GET's documented "
            "one-character STATUS code for 'finished'); process_chain -> ['G','F'] (forum-sourced "
            "RSPCLOGCHAIN/RSPC_API_CHAIN_GET_STATUS codes for 'completed' -- not SAP-documented; "
            "confirm on your BW system)."
        ),
    )
    failure_statuses: Optional[List[str]] = Field(
        default=None,
        description=(
            "status_field values (case-insensitive) treated as failure. Any other value keeps "
            "polling. Defaults by target_type when unset: background_job -> ['A'] ('cancelled'); "
            "process_chain -> ['R','X','J'] (forum-sourced 'ended with errors'/'cancelled' codes "
            "-- not SAP-documented)."
        ),
    )
    trigger_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Extra kwargs merged into the trigger call (e.g. job-step print parameters not covered by variant_name).",
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "Log what would be triggered and skip opening a connection entirely -- no resource "
            "is looked up, no RFC call is made. Output metadata always includes dry_run so a "
            "materialization history shows which runs were real."
        ),
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '22:00-05:00 Europe/Berlin "
            "mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of "
            "triggering or failing -- no SAP job/chain is started, and real downstream "
            "dependents skip too (not fail). Checked once per materialization, before any RFC "
            "call. A malformed spec fails open (never blocks). Unset (default): always runs."
        ),
    )
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number "
            "of slots shared across every asset/op using the same pool name, e.g. a cap on "
            "concurrent SAP background-job triggers). The pool's own slot count is configured "
            "at the deployment level (dagster.yaml's concurrency: pools:, or the Dagster+ UI), "
            "not here."
        ),
    )

    group_name: Optional[str] = Field(default="sap", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners -- team names ('team:analytics') or email addresses."
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None, description="Additional key-value tags applied to the asset in the Dagster catalog."
    )
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds for the catalog. Auto-includes 'sap' if empty."
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None, description="Max retries on asset failure. Defines a RetryPolicy."
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data,
            lists=("deps", "owners", "kinds", "success_statuses", "failure_statuses"),
            tags=("asset_tags",),
        )

    @model_validator(mode="after")
    def _validate_target(self):
        if self.target_type == "background_job":
            if not self.job_name:
                raise ValueError("SapJobTriggerAssetComponent: target_type='background_job' requires job_name.")
            if not self.job_count and not self.program_name:
                raise ValueError(
                    "SapJobTriggerAssetComponent: target_type='background_job' requires job_count "
                    "(start an already-scheduled job) or program_name (create+start a new one-step job)."
                )
        else:
            if not self.process_chain_id:
                raise ValueError("SapJobTriggerAssetComponent: target_type='process_chain' requires process_chain_id.")
        return self

    @model_validator(mode="after")
    def _apply_status_defaults(self):
        if self.status_field is None:
            self.status_field = "STATUS" if self.target_type == "background_job" else "E_STATUS"
        if self.success_statuses is None:
            self.success_statuses = ["F"] if self.target_type == "background_job" else ["G", "F"]
        if self.failure_statuses is None:
            self.failure_statuses = ["A"] if self.target_type == "background_job" else ["R", "X", "J"]
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Sap Job Trigger Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"sap"}
        dry_run = component.dry_run
        run_window = component.run_window
        rk = component.sap_rfc_resource_key
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        outs = {
            "result": dg.AssetOut(
                key=dg.AssetKey(component.asset_key.split("/")),
                description=component.description
                or f"Trigger and wait for SAP {component.target_type} ({component.job_name or component.process_chain_id})",
                is_required=False,
                kinds=kinds,
                owners=component.owners or None,
                tags=component.asset_tags or None,
            ),
        }

        @dg.multi_asset(
            name=dg.AssetKey(component.asset_key.split("/")).to_python_identifier(),
            outs=outs,
            group_name=component.group_name,
            deps=deps_keys,
            required_resource_keys={rk} if not dry_run else None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _sap_job_trigger_asset(context: dg.AssetExecutionContext):
            if run_window and not _is_in_run_window(time.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return
            if dry_run:
                yield _dry_run_preview(context)
                return
            yield _execute(context)

        def _dry_run_preview(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if component.target_type == "background_job":
                action = (
                    f"start job_count={component.job_count} immediately"
                    if component.job_count
                    else f"create+start program={component.program_name} variant={component.variant_name or '(default)'}"
                )
                context.log.info(f"[dry_run] would XMI-logon then {action} for job_name={component.job_name}")
            else:
                context.log.info(f"[dry_run] would start process_chain_id={component.process_chain_id}")
            return dg.MaterializeResult(metadata={
                "dry_run": dg.MetadataValue.bool(True),
                "sap/target_type": dg.MetadataValue.text(component.target_type),
            })

        def _execute(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            resource = getattr(context.resources, rk)
            with resource.connection() as conn:
                run_id = None
                try:
                    if component.target_type == "background_job":
                        logon = conn.call(
                            "BAPI_XMI_LOGON",
                            EXTCOMPANY=component.xmi_ext_company,
                            EXTPRODUCT=component.xmi_ext_product,
                            INTERFACE=component.xmi_interface,
                            VERSION=component.xmi_version,
                        )
                        _check_bapi_return(logon, "BAPI_XMI_LOGON")

                        if component.job_count:
                            jobcount = component.job_count
                            params: Dict[str, Any] = {
                                "JOBNAME": component.job_name,
                                "JOBCOUNT": jobcount,
                                "EXTERNAL_USER_NAME": component.external_user_name,
                            }
                            if component.target_server:
                                params["TARGET_SERVER"] = component.target_server
                            params.update(component.trigger_params or {})
                            start_result = conn.call("BAPI_XBP_JOB_START_IMMEDIATELY", **params)
                            _check_bapi_return(start_result, "BAPI_XBP_JOB_START_IMMEDIATELY")
                        else:
                            open_params: Dict[str, Any] = {
                                "JOBNAME": component.job_name, "EXTERNAL_USER_NAME": component.external_user_name,
                            }
                            if component.job_class:
                                open_params["JOBCLASS"] = component.job_class
                            open_result = conn.call("BAPI_XBP_JOB_OPEN", **open_params)
                            _check_bapi_return(open_result, "BAPI_XBP_JOB_OPEN")
                            jobcount = open_result.get("JOBCOUNT")
                            step_params: Dict[str, Any] = {
                                "JOBNAME": component.job_name,
                                "JOBCOUNT": jobcount,
                                "EXTERNAL_USER_NAME": component.external_user_name,
                                "ABAP_PROGRAM_NAME": component.program_name,
                            }
                            if component.variant_name:
                                step_params["ABAP_VARIANT_NAME"] = component.variant_name
                            step_params.update(component.trigger_params or {})
                            step_result = conn.call("BAPI_XBP_JOB_ADD_ABAP_STEP", **step_params)
                            _check_bapi_return(step_result, "BAPI_XBP_JOB_ADD_ABAP_STEP")

                            close_params: Dict[str, Any] = {
                                "JOBNAME": component.job_name,
                                "JOBCOUNT": jobcount,
                                "EXTERNAL_USER_NAME": component.external_user_name,
                            }
                            if component.target_server:
                                close_params["TARGET_SERVER"] = component.target_server
                            close_result = conn.call("BAPI_XBP_JOB_CLOSE", **close_params)
                            _check_bapi_return(close_result, "BAPI_XBP_JOB_CLOSE")

                            start_params: Dict[str, Any] = {
                                "JOBNAME": component.job_name,
                                "JOBCOUNT": jobcount,
                                "EXTERNAL_USER_NAME": component.external_user_name,
                            }
                            if component.target_server:
                                start_params["TARGET_SERVER"] = component.target_server
                            start_result = conn.call("BAPI_XBP_JOB_START_IMMEDIATELY", **start_params)
                            _check_bapi_return(start_result, "BAPI_XBP_JOB_START_IMMEDIATELY")
                        run_id = str(jobcount)
                        context.log.info(f"SAP background job triggered. job_name={component.job_name} jobcount={run_id}")

                        def _poll():
                            return conn.call(
                                "BAPI_XBP_JOB_STATUS_GET",
                                JOBNAME=component.job_name,
                                JOBCOUNT=jobcount,
                                EXTERNAL_USER_NAME=component.external_user_name,
                            )

                        def _joblog():
                            try:
                                log_result = conn.call(
                                    "BAPI_XBP_JOB_JOBLOG_READ",
                                    JOBNAME=component.job_name,
                                    JOBCOUNT=jobcount,
                                    EXTERNAL_USER_NAME=component.external_user_name,
                                )
                                lines = log_result.get("JOB_PROTOCOL") or log_result.get("JOB_PROTOCOL_NEW") or []
                                return "\n".join(str(l.get("MESSAGE", l)) if isinstance(l, dict) else str(l) for l in lines)
                            except Exception as e:
                                return f"(could not read job log: {e})"
                    else:
                        params = {"I_CHAIN": component.process_chain_id}
                        params.update(component.trigger_params or {})
                        start_result = conn.call("RSPC_API_CHAIN_START", **params)
                        run_id = str(start_result.get("E_LOGID") or component.process_chain_id)
                        context.log.info(
                            f"SAP process chain triggered. process_chain_id={component.process_chain_id} run_id={run_id}"
                        )

                        def _poll():
                            return conn.call(
                                "RSPC_API_CHAIN_GET_STATUS", I_CHAIN=component.process_chain_id, I_LOGID=run_id,
                            )

                        def _joblog():
                            try:
                                log_result = conn.call(
                                    "RSPC_API_CHAIN_GET_LOG", I_CHAIN=component.process_chain_id, I_LOGID=run_id,
                                )
                                lines = log_result.get("E_T_LOG") or []
                                def _line_text(l):
                                    if not isinstance(l, dict):
                                        return str(l)
                                    for key in ("MESSAGE", "MSGTEXT", "TEXT", "VALUE"):
                                        if key in l:
                                            return str(l[key])
                                    return str(l)
                                return "\n".join(_line_text(l) for l in lines)
                            except Exception as e:
                                return f"(could not read process chain log: {e})"

                    elapsed = 0.0
                    while elapsed < component.timeout_seconds:
                        time.sleep(component.poll_interval_seconds)
                        elapsed += component.poll_interval_seconds
                        try:
                            status_data = _poll()
                        except Exception as e:
                            context.log.warning(f"Poll error: {e}")
                            continue
                        raw_status = str(status_data.get(component.status_field, "")).strip().upper()
                        context.log.info(f"SAP {component.target_type} {run_id} status: {raw_status or '(empty)'}")
                        if component.target_type == "process_chain" and isinstance(status_data, dict) and status_data.get("E_MANUAL_ABORT"):
                            # RSPC_API_CHAIN_GET_STATUS's manual-abort flag -- treat as failure even
                            # if E_STATUS hasn't settled into one of failure_statuses yet.
                            log_text = _joblog()
                            raise Exception(
                                f"SAP process_chain {run_id} manually aborted (E_MANUAL_ABORT). {log_text}"
                            )
                        if raw_status in {s.upper() for s in component.success_statuses}:
                            return dg.MaterializeResult(metadata={
                                "sap/target_type": dg.MetadataValue.text(component.target_type),
                                "sap/run_id": dg.MetadataValue.text(run_id),
                                "sap/status": dg.MetadataValue.text(raw_status),
                                "sap/job_name": dg.MetadataValue.text(component.job_name or ""),
                                "sap/process_chain_id": dg.MetadataValue.text(component.process_chain_id or ""),
                                "dry_run": dg.MetadataValue.bool(False),
                            })
                        if raw_status in {s.upper() for s in component.failure_statuses}:
                            log_text = _joblog()
                            raise Exception(
                                f"SAP {component.target_type} {run_id} status={raw_status}. {log_text}"
                            )
                        # anything else (scheduled/released/ready/running/queued...) -- keep polling

                    log_text = _joblog()
                    raise Exception(
                        f"SAP {component.target_type} {run_id} timed out after {component.timeout_seconds}s. "
                        f"Last log:\n{log_text}"
                    )
                finally:
                    if component.target_type == "background_job":
                        try:
                            conn.call("BAPI_XMI_CLOSE", INTERFACE=component.xmi_interface)
                        except Exception as e:
                            context.log.warning(f"BAPI_XMI_CLOSE failed (non-fatal): {e}")

        return dg.Definitions(assets=[_sap_job_trigger_asset])
