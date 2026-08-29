"""InformaticaPowerCenterWorkspaceComponent.

Wrap Informatica **PowerCenter** (legacy on-prem, distinct from IDMC) behind a
Dagster workspace-shape component. Discovers workflows in the PowerCenter
repository via `pmrep listobjects` and emits one AssetSpec per workflow. On
materialize, invokes `pmcmd startworkflow` and polls `pmcmd getworkflowdetails`
until the workflow reaches a terminal state.

Full workspace-pattern shape (parity with informatica_workspace / ssis_workspace):
  - `@public` class annotation
  - `@record` props class
  - `translation:` callable for per-asset customization
  - `StateBackedComponent` inheritance — discovery cached to disk
  - `workflow_selector:` folder / include / exclude filtering
  - `workflow_overrides:` runtime parameter files + variable overrides
  - `polling_sensor:` + `freshness_lag_threshold_seconds:` observation

Requirements at runtime:
  - `pmcmd` and `pmrep` binaries on PATH (or absolute paths set via
    `workspace.pmcmd_path` / `workspace.pmrep_path`).
  - Network access from the Dagster runtime to the PowerCenter domain
    server (default port 6005).

Companion of `informatica_workspace` (IDMC). This component covers the on-prem
PowerCenter product; that one covers the cloud IDMC / IICS platform.
"""

import fnmatch
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    TranslationFn,
    TranslationFnResolver,
)
from dagster_shared.record import record
from pydantic import Field


# ── Terminal PowerCenter workflow statuses ──────────────────────────
# From pmcmd getworkflowdetails output ("Status:" line):
#   "Succeeded", "Failed", "Aborted", "Stopped", "Terminated", "Unknown",
#   "Running", "Suspended", "Suspending"
PC_TERMINAL: set = {"SUCCEEDED", "FAILED", "ABORTED", "STOPPED", "TERMINATED", "UNKNOWN"}
PC_SUCCESS: set = {"SUCCEEDED"}


# ── Props (@record) for translator callable ─────────────────────────
@record
class PowerCenterWorkflowProps:
    """Data passed to `translation:` callables for each PowerCenter workflow.

    Mirrors HvrObjectProps / InformaticaTaskProps.

    Attributes:
        workflow_name: Workflow name (matches WF_ prefix convention).
        folder_name: PowerCenter folder containing the workflow.
        repository: PowerCenter repository name.
        description: Workflow description (may be None).
        server_host: PowerCenter domain host (from workspace.domain_host).
    """

    workflow_name: str
    folder_name: str
    repository: str
    description: Optional[str] = None
    server_host: str = ""

    @property
    def qualified_name(self) -> str:
        return f"{self.repository}/{self.folder_name}/{self.workflow_name}"


# ── Workspace config nested block ───────────────────────────────────
class PowerCenterWorkspaceConfig(dg.Model):
    """Connection to a PowerCenter domain + repository.

    All commands go through pmcmd / pmrep subprocess. Auth is domain-level
    (username/password) — repository connect happens lazily inside each
    subprocess invocation.
    """

    domain_name: str = Field(
        description="Informatica domain name (matches the PowerCenter domain config)."
    )
    domain_host: Optional[str] = Field(
        default=None,
        description="Optional domain gateway hostname (informational — pmcmd reads "
        "domain config from $INFA_DOMAINS_FILE).",
    )
    integration_service: str = Field(
        description="Integration Service name to run workflows against."
    )
    repository: str = Field(
        description="PowerCenter repository name (target of pmrep connect + pmcmd -sv)."
    )
    username: Optional[str] = Field(
        default=None, description="PowerCenter username. Prefer `username_env_var` for secrets."
    )
    username_env_var: Optional[str] = Field(
        default=None, description="Env var holding the PowerCenter username."
    )
    password: Optional[str] = Field(
        default=None, description="PowerCenter password. Prefer `password_env_var` for secrets."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var holding the PowerCenter password."
    )
    security_domain: str = Field(
        default="Native",
        description="Informatica security domain — `Native`, `LDAP`, or a "
        "custom-configured name.",
    )
    pmcmd_path: str = Field(
        default="pmcmd", description="Path to the pmcmd binary. Default assumes it's on PATH."
    )
    pmrep_path: str = Field(
        default="pmrep", description="Path to the pmrep binary. Default assumes it's on PATH."
    )
    command_timeout_seconds: int = Field(
        default=120, description="Timeout for a single pmcmd / pmrep invocation."
    )


# ── Per-workflow runtime override ───────────────────────────────────
class PowerCenterWorkflowOverride(dg.Model):
    """Runtime parameter file + variable overrides for workflows matching an
    fnmatch pattern against `<repository>/<folder>/<workflow>`.

    PowerCenter accepts two parameter mechanisms at startworkflow time:
      - `-paramfile <path>`  — a parameter file with $$Var=value lines
      - Inline `-parameter` flags — direct override of individual variables

    Values can reference `{partition_key}` — substituted at run time.
    """

    match: str = Field(
        description="fnmatch pattern against `<repository>/<folder>/<workflow_name>` (case-insensitive)."
    )
    parameter_file: Optional[str] = Field(
        default=None,
        description="Path to a PowerCenter parameter file passed via `pmcmd -paramfile`. "
        "May contain `{partition_key}` for run-time substitution.",
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Session/workflow variable name → value. Rendered as "
        "`-parameter NAME=VALUE` on the pmcmd command line. Values may "
        "contain `{partition_key}`.",
    )
    session_log_file: Optional[str] = Field(
        default=None,
        description="Optional path for `pmcmd -sessionlog`.",
    )


# ── Selector block ──────────────────────────────────────────────────
class PowerCenterWorkflowSelector(dg.Model):
    """Filter which PowerCenter workflows become Dagster assets."""

    by_folder: Optional[List[str]] = Field(
        default=None,
        description="Restrict to these PowerCenter folders (exact match).",
    )
    include: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns against `<folder>/<workflow_name>` (case-insensitive).",
    )
    exclude: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns to EXCLUDE. Applied last.",
    )


# ── Base translator ─────────────────────────────────────────────────
class PowerCenterComponentTranslator:
    """Base translator: PowerCenterWorkflowProps → AssetSpec."""

    def __init__(self, component: "InformaticaPowerCenterWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: PowerCenterWorkflowProps) -> AssetSpec:
        prefix = self._component.asset_key_prefix or [
            "informatica",
            "powercenter",
            (props.server_host or props.repository).split(".")[0].lower(),
        ]
        return AssetSpec(
            key=AssetKey([*prefix, props.folder_name, props.workflow_name]),
            description=(
                props.description
                or f"PowerCenter workflow `{props.qualified_name}`"
            ),
            group_name=self._component.group_name,
            # Kinds — text-only badges (no first-class Dagster icon).
            kinds=set(self._component.kinds or ["informatica", "powercenter", "etl"]),
            tags=dict(self._component.tags or {}),
            owners=list(self._component.owners or []),
            metadata={
                "informatica/repository": props.repository,
                "informatica/folder": props.folder_name,
                "informatica/workflow": props.workflow_name,
            },
        )


# ── Component ───────────────────────────────────────────────────────
@public
class InformaticaPowerCenterWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Informatica PowerCenter workflows as Dagster assets — full workspace shape.

    Example:

    ```yaml
    type: dagster_community_components.InformaticaPowerCenterWorkspaceComponent
    attributes:
      workspace:
        domain_name:          "{{ env.PC_DOMAIN }}"
        integration_service:  IS_PROD
        repository:           REPO_PROD
        username_env_var:     PC_USER
        password_env_var:     PC_PASSWORD
        security_domain:      Native
      workflow_selector:
        by_folder: [Sales, Finance]
        include:   ["wf_load_*"]
        exclude:   ["wf_*_test"]
      # translation: |
      #   {{ load_python_module_attr('my_project.pc.translate.by_folder') }}
      workflow_overrides:
        - match: "REPO_PROD/Sales/wf_load_customers"
          parameter_file: "/etc/infa/params/customers_{partition_key}.par"
          parameters:
            $$TargetSchema: "raw_sales"
            $$BatchDate:    "{partition_key}"
      action: execute
      wait_for_completion: true
      poll_interval_seconds: 30
      timeout_seconds: 3600
      polling_sensor: false
      observation_interval_seconds: 300
      freshness_lag_threshold_seconds: 3600
      group_name: powercenter_prod
      kinds: [informatica, powercenter, etl]
    ```
    """

    workspace: PowerCenterWorkspaceConfig = Field(
        description="Connection to the PowerCenter domain + repository."
    )
    workflow_selector: Optional[PowerCenterWorkflowSelector] = Field(
        default=None,
        description="Filter which workflows become assets. Default = every workflow "
        "in the repository.",
    )
    workflow_overrides: Optional[List[PowerCenterWorkflowOverride]] = Field(
        default=None,
        description="Parameter file + variable overrides applied at execute time via "
        "pmcmd startworkflow. Only used when `action: execute`. See "
        "`PowerCenterWorkflowOverride` for shape.",
    )
    translation: Annotated[
        Optional[TranslationFn[PowerCenterWorkflowProps]],
        TranslationFnResolver(
            template_vars_for_translation_fn=lambda data: {"props": data}
        ),
    ] = Field(
        default=None,
        description=(
            "Optional per-asset translation callable. Receives a "
            "PowerCenterWorkflowProps and returns a dict of AssetSpec kwargs "
            "(key / group_name / tags / owners / kinds / metadata / "
            "description)."
        ),
    )
    action: str = Field(
        default="noop",
        description=(
            "What materialize() does. `noop` = external asset (declare-only). "
            "`execute` = shell out to `pmcmd startworkflow`."
        ),
    )
    wait_for_completion: bool = Field(
        default=True,
        description="Only used when `action: execute`. If true, poll "
        "getworkflowdetails until a terminal status; if false, return once "
        "pmcmd accepts the trigger.",
    )
    poll_interval_seconds: int = Field(
        default=30,
        description="Seconds between `pmcmd getworkflowdetails` polls.",
    )
    timeout_seconds: int = Field(
        default=3600,
        description="Overall timeout for a synchronous workflow. Set 0 for no timeout.",
    )
    polling_sensor: bool = Field(
        default=False,
        description="If true, emit a sensor that polls the last-run status "
        "for each workflow and emits AssetObservation on new terminal states.",
    )
    observation_interval_seconds: int = Field(
        default=300,
        description="Sensor tick interval. Only used when `polling_sensor: true`.",
    )
    freshness_lag_threshold_seconds: Optional[int] = Field(
        default=None,
        description="If set, attach an asset check per workflow that fails when the "
        "last successful run is older than this many seconds.",
    )
    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description="Prefix for every emitted asset key. Default: "
        "`['informatica', 'powercenter', <repository>]`.",
    )
    group_name: Optional[str] = Field(
        default="informatica_powercenter", description="Dagster asset group."
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds badge (Dagster catalog UI). Default: "
        "`['informatica', 'powercenter', 'etl']`.",
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None, description="Additional asset tags."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners (team names / email addresses)."
    )
    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description="StateBackedComponent state config. Default: local "
        "filesystem cache keyed on repository+integration_service.",
    )

    # ── Base translator (cached per instance) ─────────────────────
    @property
    def _base_translator(self) -> PowerCenterComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = PowerCenterComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @public
    def get_asset_spec(self, props: PowerCenterWorkflowProps) -> AssetSpec:
        """Public hook — user's `translation:` callable wraps this."""
        base_spec = self._base_translator.get_asset_spec(props)
        if self.translation is None:
            return base_spec
        overrides = self.translation(base_spec, props) or {}
        if isinstance(overrides, AssetSpec):
            return overrides
        return base_spec._replace(**overrides) if hasattr(base_spec, "_replace") else base_spec

    @property
    def defs_state_config(self) -> DefsStateConfig:
        composite = f"{self.workspace.repository}::{self.workspace.integration_service}"
        state_hash = hashlib.sha256(composite.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{state_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    # ── Auth ──────────────────────────────────────────────────────
    def _resolve_creds(self) -> Dict[str, str]:
        import os
        cfg = self.workspace
        user = cfg.username or (
            os.environ.get(cfg.username_env_var, "") if cfg.username_env_var else ""
        )
        password = cfg.password or (
            os.environ.get(cfg.password_env_var, "") if cfg.password_env_var else ""
        )
        if not (user and password):
            raise ValueError(
                "InformaticaPowerCenterWorkspaceComponent.workspace: supply "
                "username/password OR username_env_var/password_env_var."
            )
        return {
            "user": user,
            "password": password,
            "security_domain": cfg.security_domain,
        }

    def _run_subprocess(self, argv: List[str], stdin_input: Optional[str] = None) -> Dict[str, str]:
        """Run a pmcmd/pmrep invocation. Returns {'stdout', 'stderr', 'returncode'}.

        pmcmd exits with 0 on success. Anything non-zero is a hard failure.
        pmrep exits with 0 on success; per-command status is inline in stdout.
        """
        cfg = self.workspace
        try:
            proc = subprocess.run(
                argv,
                input=stdin_input,
                capture_output=True,
                text=True,
                timeout=cfg.command_timeout_seconds,
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                f"PowerCenter binary not found: {argv[0]!r}. "
                f"Set workspace.pmcmd_path / pmrep_path or add to PATH."
            ) from e
        return {
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
            "returncode": str(proc.returncode),
        }

    # ── pmrep listobjects → workflow discovery ────────────────────
    def _discover_workflows(self) -> List[Dict[str, Any]]:
        """List every workflow in the target repository via pmrep.

        pmrep session:
            connect -r <repo> -d <domain> -n <user> -x <password>
            listobjects -o workflow -f <folder>   # per folder
            OR
            listobjects -o workflow               # all folders (slower)

        Output shape (one line per object):
            workflow  <folder>  <workflow_name>
        """
        cfg = self.workspace
        creds = self._resolve_creds()

        connect_cmd = (
            f"connect -r {cfg.repository} -d {cfg.domain_name} "
            f"-n {creds['user']} -x {creds['password']} -s {creds['security_domain']}"
        )

        # Ask for every folder first — pmrep listfolders is one line per folder.
        folder_result = self._run_subprocess(
            [cfg.pmrep_path],
            stdin_input=f"{connect_cmd}\nlistfolders\nexit\n",
        )
        if folder_result["returncode"] != "0":
            raise RuntimeError(
                f"pmrep listfolders failed (rc={folder_result['returncode']}): "
                f"{folder_result['stderr'] or folder_result['stdout']}"
            )
        folders = [
            ln.strip()
            for ln in folder_result["stdout"].splitlines()
            if ln.strip() and not ln.startswith(("Connect", "listfolders", "pmrep>"))
        ]
        # Apply by_folder filter early to skip needless listobjects calls.
        sel = self.workflow_selector
        if sel and sel.by_folder:
            folders = [f for f in folders if f in sel.by_folder]

        workflows: List[Dict[str, Any]] = []
        for folder in folders:
            r = self._run_subprocess(
                [cfg.pmrep_path],
                stdin_input=(
                    f"{connect_cmd}\n"
                    f"listobjects -o workflow -f {folder}\n"
                    "exit\n"
                ),
            )
            if r["returncode"] != "0":
                # Skip unreadable folders but keep discovery going.
                continue
            for line in r["stdout"].splitlines():
                m = re.match(r"^\s*workflow\s+\S+\s+(\S+)\s*$", line, re.IGNORECASE)
                if not m:
                    continue
                workflows.append({
                    "workflow_name": m.group(1),
                    "folder_name": folder,
                    "repository": cfg.repository,
                    "description": None,
                })

        # Apply include / exclude fnmatch.
        if not sel:
            return workflows

        def _match(wf: Dict[str, Any]) -> bool:
            qname_lower = f"{wf['folder_name']}/{wf['workflow_name']}".lower()
            if sel.include and not any(
                fnmatch.fnmatch(qname_lower, pat.lower()) for pat in sel.include
            ):
                return False
            if sel.exclude and any(
                fnmatch.fnmatch(qname_lower, pat.lower()) for pat in sel.exclude
            ):
                return False
            return True

        return [w for w in workflows if _match(w)]

    # ── pmcmd startworkflow / getworkflowdetails ──────────────────
    def _resolve_wf_overrides(
        self, qualified_name: str, context
    ) -> Dict[str, Any]:
        merged_params: Dict[str, str] = {}
        param_file: Optional[str] = None
        session_log: Optional[str] = None

        if not self.workflow_overrides:
            return {"parameter_file": None, "parameters": {}, "session_log_file": None}

        partition_key = getattr(context, "partition_key", None)

        def _sub(v):
            if isinstance(v, str) and partition_key is not None and "{partition_key}" in v:
                return v.format(partition_key=partition_key)
            return v

        for ov in self.workflow_overrides:
            if not fnmatch.fnmatch(qualified_name.lower(), ov.match.lower()):
                continue
            if ov.parameter_file:
                param_file = _sub(ov.parameter_file)
            if ov.parameters:
                for k, v in ov.parameters.items():
                    merged_params[k] = _sub(str(v))
            if ov.session_log_file:
                session_log = _sub(ov.session_log_file)

        return {
            "parameter_file": param_file,
            "parameters": merged_params,
            "session_log_file": session_log,
        }

    def _execute_workflow(
        self, wf: Dict[str, Any], context
    ) -> Dict[str, Any]:
        """pmcmd startworkflow + optional polling.

        pmcmd startworkflow expects:
            pmcmd startworkflow -sv <IS> -d <domain> -u <user> -p <password>
                -f <folder> [-paramfile <file>] [-parameter NAME=VALUE ...]
                [-wait] <workflow_name>

        Using `-wait` blocks pmcmd itself until the workflow finishes — an
        alternative to our poll loop. We prefer polling because `-wait` gives
        up on socket disconnects and returns no structured status.
        """
        cfg = self.workspace
        creds = self._resolve_creds()
        override = self._resolve_wf_overrides(
            qualified_name=f"{wf['repository']}/{wf['folder_name']}/{wf['workflow_name']}",
            context=context,
        )

        argv = [
            cfg.pmcmd_path, "startworkflow",
            "-sv", cfg.integration_service,
            "-d", cfg.domain_name,
            "-u", creds["user"],
            "-p", creds["password"],
            "-f", wf["folder_name"],
        ]
        if override["parameter_file"]:
            argv += ["-paramfile", override["parameter_file"]]
        for k, v in (override["parameters"] or {}).items():
            argv += ["-parameter", f"{k}={v}"]
        if override["session_log_file"]:
            argv += ["-sessionlog", override["session_log_file"]]
        argv += [wf["workflow_name"]]

        r = self._run_subprocess(argv)
        if r["returncode"] != "0":
            raise RuntimeError(
                f"pmcmd startworkflow failed (rc={r['returncode']}) for "
                f"{wf['folder_name']}/{wf['workflow_name']}: "
                f"{r['stderr'] or r['stdout']}"
            )
        context.log.info(
            f"PowerCenter workflow {wf['folder_name']}/{wf['workflow_name']} started"
        )

        if not self.wait_for_completion:
            return {"status": None, "status_text": "async — not polled", "error_message": None}

        start = time.time()
        while True:
            details = self._run_subprocess([
                cfg.pmcmd_path, "getworkflowdetails",
                "-sv", cfg.integration_service,
                "-d", cfg.domain_name,
                "-u", creds["user"],
                "-p", creds["password"],
                "-f", wf["folder_name"],
                wf["workflow_name"],
            ])
            status = _extract_status(details["stdout"])
            if status and status.upper() in PC_TERMINAL:
                error_message = None
                if status.upper() not in PC_SUCCESS:
                    error_message = _extract_error(details["stdout"])
                return {
                    "status": status.upper(),
                    "status_text": status,
                    "error_message": error_message,
                }
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise TimeoutError(
                    f"PowerCenter workflow {wf['folder_name']}/{wf['workflow_name']} "
                    f"exceeded timeout of {self.timeout_seconds}s (last status={status})"
                )
            time.sleep(self.poll_interval_seconds)

    # ── StateBackedComponent contract ─────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        try:
            workflows = self._discover_workflows()
        except Exception:  # noqa: BLE001
            workflows = []
        snapshot = {
            "repository": self.workspace.repository,
            "domain_host": self.workspace.domain_host or "",
            "workflows": workflows,
            "polled_at": time.time(),
        }
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        if state_path is None or not state_path.exists():
            return Definitions()

        state = json.loads(state_path.read_text())
        workflows = state.get("workflows", [])
        server_host = state.get("domain_host", self.workspace.domain_host or "")

        specs: List[AssetSpec] = []
        for wf in workflows:
            props = PowerCenterWorkflowProps(
                workflow_name=wf["workflow_name"],
                folder_name=wf["folder_name"],
                repository=wf["repository"],
                description=wf.get("description"),
                server_host=server_host,
            )
            specs.append(self.get_asset_spec(props))

        assets: List[Any] = []
        action = (self.action or "noop").lower()

        if action == "noop":
            assets = list(specs)
        elif action == "execute":
            _self = self

            @dg.multi_asset(specs=specs)
            def _powercenter_execute(context: dg.AssetExecutionContext):
                for spec in specs:
                    wf = {
                        "workflow_name": spec.metadata["informatica/workflow"],
                        "folder_name":   spec.metadata["informatica/folder"],
                        "repository":    spec.metadata["informatica/repository"],
                    }
                    result = _self._execute_workflow(wf, context)
                    if _self.wait_for_completion and result.get("status") not in PC_SUCCESS.union({None}):
                        raise dg.Failure(
                            description=(
                                f"PowerCenter workflow {wf['folder_name']}/{wf['workflow_name']} "
                                f"finished with status={result.get('status_text')!r}: "
                                f"{result.get('error_message')}"
                            )
                        )
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "informatica/pc/status": result.get("status_text") or "async",
                        },
                    )

            assets = [_powercenter_execute]
        else:
            raise ValueError(
                f"InformaticaPowerCenterWorkspaceComponent.action={action!r} not "
                f"supported. Use 'noop' or 'execute'."
            )

        sensors: List[Any] = []
        if self.polling_sensor and specs:
            sensors.append(self._build_observation_sensor(specs))

        checks: List[Any] = []
        if self.freshness_lag_threshold_seconds is not None and specs:
            checks.extend(self._build_freshness_checks(specs))

        return Definitions(assets=assets, sensors=sensors, asset_checks=checks)

    def _build_observation_sensor(self, specs: List[AssetSpec]):
        _self = self
        cfg = self.workspace

        @dg.sensor(
            name="powercenter_workspace_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            creds = _self._resolve_creds()
            observations = []
            for spec in specs:
                r = _self._run_subprocess([
                    cfg.pmcmd_path, "getworkflowdetails",
                    "-sv", cfg.integration_service,
                    "-d", cfg.domain_name,
                    "-u", creds["user"],
                    "-p", creds["password"],
                    "-f", spec.metadata["informatica/folder"],
                    spec.metadata["informatica/workflow"],
                ])
                status = _extract_status(r["stdout"])
                if not status or status.upper() not in PC_TERMINAL:
                    continue
                observations.append(
                    dg.AssetObservation(
                        asset_key=spec.key,
                        metadata={
                            "informatica/pc/status": status,
                        },
                    )
                )
            return dg.SensorResult(asset_events=observations)

        return _observation_sensor

    def _build_freshness_checks(self, specs: List[AssetSpec]) -> List[Any]:
        # PowerCenter run history requires `pmrep gettaskdetails` or a
        # repository query — the MVP shape defers this. The check exists
        # so the field validates; a runtime issues an INFO if invoked.
        _self = self
        threshold = self.freshness_lag_threshold_seconds

        checks = []
        for spec in specs:
            @dg.asset_check(
                asset=spec.key,
                name="powercenter_freshness_lag",
                description=(
                    f"Fails when the last successful PowerCenter run is older "
                    f"than {threshold}s. (Placeholder — needs pmrep gettaskdetails.)"
                ),
            )
            def _check(_spec=spec):
                return dg.AssetCheckResult(
                    passed=True,
                    description="freshness lag check not yet implemented; run pmrep gettaskdetails externally.",
                )
            checks.append(_check)
        return checks


# ── pmcmd output parsers ────────────────────────────────────────────
def _extract_status(pmcmd_output: str) -> Optional[str]:
    """Pull the `Status: <value>` field out of pmcmd getworkflowdetails."""
    m = re.search(r"^Status:\s*(\S+)\s*$", pmcmd_output, re.MULTILINE)
    return m.group(1) if m else None


def _extract_error(pmcmd_output: str) -> Optional[str]:
    """Pull the first meaningful error line out of pmcmd output."""
    for line in pmcmd_output.splitlines():
        if "ERROR" in line.upper() or "Failed" in line:
            return line.strip()
    return None
