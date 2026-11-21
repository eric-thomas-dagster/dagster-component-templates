"""Semantic Model Refresh Asset Component.

Triggers a Power BI / Fabric semantic model (dataset) refresh via the Power BI REST API, then
polls until it finishes, raising with Fabric's own error detail on failure. The missing piece on
the Fabric side of an ADF-style pipeline: ADF's `PBISemanticModelRefresh` activity has no
Dagster-component equivalent today, and `fabric_workspace` only lists `SemanticModel` items as
external/observable (no trigger-and-wait).

Verified against Microsoft's own REST API reference (learn.microsoft.com/rest/api/power-bi/
datasets/refresh-dataset-in-group and .../get-refresh-execution-details-in-group), not guessed:

    POST https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes
    GET  https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{refreshId}

An "enhanced refresh" (type/commitMode/objects/maxParallelism/retryCount/effectiveDate/timeout)
triggers only when a request payload OTHER than `notifyOption` is set -- Microsoft's own
documented rule, not an assumption. `notifyOption` and the enhanced-refresh fields are mutually
exclusive on the wire (the API requires notifyOption be EXCLUDED from an enhanced-refresh body).

The polled `status` field is "Unknown" while in progress; terminal values are "Completed",
"Failed", "Disabled" (a selective/table-scoped refresh Fabric declined to run) -- all three
confirmed from Microsoft's own response schema (DatasetRefreshDetailStatus), not inferred.

Auth: the OAuth2 scope for the Power BI REST API (`https://analysis.windows.net/powerbi/api/
.default`) is NOT documented on a single canonical Microsoft Learn page the way the endpoint
paths above are -- it is the scope Microsoft's own sample code and multiple independent
community sources consistently use for service-principal calls to api.powerbi.com, distinct
from the Fabric API's own scope (`https://api.fabric.microsoft.com/.default`) that
`fabric_workspace`/`fabric_pipeline_trigger_job` use. The service principal also needs to be
added as a member/admin of the target workspace, and a Power BI admin must enable "Allow service
principals to use Power BI APIs" -- tenant-level settings outside this component's control.
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator

_POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
_POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# DatasetRefreshDetailStatus (confirmed via the real API reference, not guessed).
_TERMINAL_STATUSES = {"Completed", "Failed", "Disabled"}


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
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


class SemanticModelRefreshAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Power BI / Fabric semantic model refresh and wait for it to finish.

    ```yaml
    type: dagster_community_components.SemanticModelRefreshAssetComponent
    attributes:
      asset_key: fabric/sales_semantic_model
      workspace_id: "{{ env.FABRIC_WORKSPACE_ID }}"
      dataset_id: "{{ env.SALES_DATASET_ID }}"
      tenant_id_env_var: AZURE_TENANT_ID
      client_id_env_var: AZURE_CLIENT_ID
      client_secret_env_var: AZURE_CLIENT_SECRET
      refresh_type: Full
      commit_mode: transactional
    ```

    Basic (non-enhanced) refresh — only `notify_option`, no other refresh fields set:

    ```yaml
    type: dagster_community_components.SemanticModelRefreshAssetComponent
    attributes:
      asset_key: fabric/sales_semantic_model
      workspace_id: ...
      dataset_id: ...
      notify_option: MailOnFailure
    ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'fabric/sales_semantic_model').")
    workspace_id: str = Field(description="Power BI / Fabric workspace ID (GUID) -- the 'groupId' path segment.")
    dataset_id: str = Field(description="Power BI / Fabric semantic model (dataset) ID (GUID).")

    tenant_id_env_var: Optional[str] = Field(default=None, description="Env var with the Azure AD tenant ID (service principal auth). Falls back to DefaultAzureCredential when unset.")
    client_id_env_var: Optional[str] = Field(default=None, description="Env var with the Azure AD app (client) ID.")
    client_secret_env_var: Optional[str] = Field(default=None, description="Env var with the Azure AD app client secret.")

    notify_option: Optional[Literal["NoNotification", "MailOnFailure", "MailOnCompletion"]] = Field(
        default=None,
        description=(
            "Mail notification option for a BASIC (non-enhanced) refresh. Mutually exclusive with "
            "every enhanced-refresh field below -- Power BI's own documented rule requires "
            "notifyOption be excluded from an enhanced-refresh request entirely. Not applicable to "
            "service-principal auth anyway (Power BI ignores it then); set it only for "
            "delegated/user auth."
        ),
    )
    refresh_type: Optional[Literal["Full", "ClearValues", "Calculate", "DataOnly", "Automatic", "Defragment"]] = Field(
        default=None, description="Enhanced refresh only. The type of processing to perform (Power BI's DatasetRefreshType).",
    )
    commit_mode: Optional[Literal["transactional", "partialBatch"]] = Field(
        default=None,
        description="Enhanced refresh only. 'transactional' commits the whole refresh as one transaction; 'partialBatch' commits in batches (no transaction -- a failure can leave a partial/empty table).",
    )
    max_parallelism: Optional[int] = Field(default=None, description="Enhanced refresh only. Max threads for parallel processing commands.")
    retry_count: Optional[int] = Field(default=None, description="Enhanced refresh only. Retries before failing (Power BI may still retry transient internal errors even at 0).")
    objects: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description="Enhanced refresh only. Selective refresh targets: list of {table, partition?} dicts. Omit to refresh the whole model.",
    )
    apply_refresh_policy: Optional[bool] = Field(default=None, description="Enhanced refresh only. Whether an incremental refresh policy is applied.")
    effective_date: Optional[str] = Field(default=None, description="Enhanced refresh only. ISO date-time overriding 'today' for an incremental refresh policy's effectiveDate.")
    request_timeout: Optional[str] = Field(
        default=None,
        description="Enhanced refresh only. Per-attempt timeout as 'HH:MM:SS' (Power BI's own `timeout` field -- caps each refresh ATTEMPT, distinct from this component's own max_wait_seconds, which caps the whole Dagster-side wait across retries).",
    )

    wait_for_completion: bool = Field(default=True, description="If false, trigger the refresh and return without polling for completion.")
    poll_interval_seconds: float = Field(default=20.0, description="Seconds between status polls.")
    max_wait_seconds: int = Field(default=3600, description="Max seconds to wait for the refresh to finish before raising a timeout error (Dagster-side; distinct from request_timeout).")

    dry_run: bool = Field(
        default=False,
        description="Log what would be triggered and skip opening a connection entirely -- no token is acquired, no REST call is made.",
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '22:00-05:00 Europe/Berlin mon,tue,wed,thu,fri'). "
            "Outside this window, the asset SKIPS cleanly instead of triggering -- no refresh is "
            "started, and real downstream dependents skip too. A malformed spec fails open "
            "(never blocks). Unset (default): always runs."
        ),
    )
    pool: Optional[str] = Field(default=None, description="Dagster's native named concurrency pool -- a cap on concurrent refreshes sharing the same pool name.")

    group_name: Optional[str] = Field(default="fabric", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'powerbi' if empty.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), tags=("asset_tags",))

    @model_validator(mode="after")
    def _validate_notify_option_vs_enhanced(self):
        enhanced_fields = (
            self.refresh_type, self.commit_mode, self.max_parallelism, self.retry_count,
            self.objects, self.apply_refresh_policy, self.effective_date, self.request_timeout,
        )
        if self.notify_option is not None and any(f is not None for f in enhanced_fields):
            raise ValueError(
                "SemanticModelRefreshAssetComponent: notify_option is mutually exclusive with "
                "every enhanced-refresh field (refresh_type/commit_mode/max_parallelism/"
                "retry_count/objects/apply_refresh_policy/effective_date/request_timeout) -- "
                "Power BI's own API requires notifyOption be excluded from an enhanced-refresh "
                "request body entirely."
            )
        return self

    def _is_enhanced_refresh(self) -> bool:
        return any(
            f is not None for f in (
                self.refresh_type, self.commit_mode, self.max_parallelism, self.retry_count,
                self.objects, self.apply_refresh_policy, self.effective_date, self.request_timeout,
            )
        )

    def _refresh_body(self) -> Optional[Dict[str, Any]]:
        if self._is_enhanced_refresh():
            body: Dict[str, Any] = {}
            if self.refresh_type is not None:
                body["type"] = self.refresh_type
            if self.commit_mode is not None:
                body["commitMode"] = self.commit_mode
            if self.max_parallelism is not None:
                body["maxParallelism"] = self.max_parallelism
            if self.retry_count is not None:
                body["retryCount"] = self.retry_count
            if self.objects is not None:
                body["objects"] = self.objects
            if self.apply_refresh_policy is not None:
                body["applyRefreshPolicy"] = self.apply_refresh_policy
            if self.effective_date is not None:
                body["effectiveDate"] = self.effective_date
            if self.request_timeout is not None:
                body["timeout"] = self.request_timeout
            return body
        if self.notify_option is not None:
            return {"notifyOption": self.notify_option}
        return None

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Semantic Model Refresh Asset", editable=True)

    def _get_credential(self):
        """Lazy-imported (azure-identity may not be installed everywhere this component.py is
        loaded) -- never invoked at all when dry_run=true."""
        from azure.identity import ClientSecretCredential, DefaultAzureCredential

        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        return DefaultAzureCredential()

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"powerbi"}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_key),
            description=component.description or f"Refresh Power BI / Fabric semantic model {component.dataset_id}",
            group_name=component.group_name,
            kinds=kinds,
            deps=deps_keys,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _semantic_model_refresh_asset(context: dg.AssetExecutionContext):
            if component.run_window and not _is_in_run_window(time.time(), component.run_window):
                context.log.info(f"run_window={component.run_window!r}: outside the configured window -- skipping (not failing).")
                return None

            body = component._refresh_body()
            if component.dry_run:
                context.log.info(
                    f"[DRY RUN] would refresh dataset_id={component.dataset_id!r} in workspace "
                    f"{component.workspace_id!r} with body={body}"
                )
                return dg.MaterializeResult(metadata={
                    "powerbi/dataset_id": component.dataset_id,
                    "powerbi/enhanced_refresh": component._is_enhanced_refresh(),
                    "status": "dry_run — not executed",
                })

            import requests

            try:
                token = component._get_credential().get_token(_POWERBI_SCOPE).token
            except Exception as e:
                raise dg.Failure(description=f"Power BI authentication failed: {e}") from e

            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            refresh_url = f"{_POWERBI_API_BASE}/groups/{component.workspace_id}/datasets/{component.dataset_id}/refreshes"
            resp = requests.post(refresh_url, json=body, headers=headers, timeout=60)
            if resp.status_code >= 300:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text[:300]
                raise dg.Failure(
                    description=f"Power BI refresh trigger failed ({resp.status_code}): {detail}",
                    metadata={"powerbi_status_code": resp.status_code},
                )

            status_url = resp.headers.get("Location")
            request_id = resp.headers.get("x-ms-request-id")
            if not status_url and request_id:
                status_url = f"{refresh_url}/{request_id}"
            context.log.info(f"Power BI refresh triggered. request_id={request_id} status_url={status_url}")

            if not component.wait_for_completion or not status_url:
                return dg.MaterializeResult(metadata={
                    "powerbi/dataset_id": component.dataset_id,
                    "powerbi/request_id": request_id or "",
                    "powerbi/status": "not polled" if status_url else "no status URL available",
                })

            elapsed = 0.0
            last_detail: Dict[str, Any] = {}
            while elapsed < component.max_wait_seconds:
                state_resp = requests.get(status_url, headers=headers, timeout=60)
                state_resp.raise_for_status()
                last_detail = state_resp.json()
                status = last_detail.get("status", "Unknown")
                context.log.info(f"Power BI refresh status={status} elapsed={elapsed:.0f}s")

                if status in _TERMINAL_STATUSES:
                    if status != "Completed":
                        messages = last_detail.get("messages") or []
                        message_text = "; ".join(str(m.get("message", m)) for m in messages) or last_detail.get("serviceExceptionJson") or status
                        raise dg.Failure(
                            description=f"Power BI refresh of dataset {component.dataset_id} ended with status={status!r}: {message_text}",
                            metadata={
                                "powerbi/status": status,
                                "powerbi/request_id": request_id or "",
                            },
                        )
                    return dg.MaterializeResult(metadata={
                        "powerbi/dataset_id": component.dataset_id,
                        "powerbi/request_id": request_id or "",
                        "powerbi/status": status,
                        "powerbi/start_time": last_detail.get("startTime", ""),
                        "powerbi/end_time": last_detail.get("endTime", ""),
                        "powerbi/number_of_attempts": last_detail.get("numberOfAttempts", 0),
                    })

                time.sleep(component.poll_interval_seconds)
                elapsed += component.poll_interval_seconds

            raise dg.Failure(
                description=f"Power BI refresh of dataset {component.dataset_id} timed out after {component.max_wait_seconds}s (last status={last_detail.get('status', 'Unknown')!r}).",
                metadata={"powerbi/request_id": request_id or ""},
            )

        return dg.Definitions(assets=[_semantic_model_refresh_asset])
