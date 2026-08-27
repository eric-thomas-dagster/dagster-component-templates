"""SsisWorkspaceComponent.

Wrap SQL Server Integration Services (SSIS) behind a Dagster workspace-shape
component. Discovers every package deployed to SSISDB and emits one AssetSpec
per package. On materialize, EXECs `SSISDB.catalog.create_execution` +
`start_execution` and polls `SSISDB.catalog.executions.status` until the
package finishes.

Full workspace-pattern shape (parity with hvr_hub_workspace /
snowflake_workspace / qlik_replicate_workspace):
  - `@public` class annotation
  - `@record` props class (replaces @dataclass)
  - `translation:` callable field for per-asset customization
  - `StateBackedComponent` inheritance — discovery cached to disk via
    `write_state_to_path`; code-location reloads read from JSON cache.
    Refresh via `dg utils refresh-defs-state`.

Backing SQL (all from SSISDB system database):

    SELECT * FROM SSISDB.catalog.packages     -- enumeration
    SELECT * FROM SSISDB.catalog.folders      -- folder scope
    SELECT * FROM SSISDB.catalog.projects     -- project scope
    EXEC SSISDB.catalog.create_execution ...  -- trigger (returns execution_id)
    EXEC SSISDB.catalog.start_execution ...
    SELECT status FROM SSISDB.catalog.executions   -- poll
    SELECT * FROM SSISDB.catalog.operation_messages  -- log surfacing

Status codes (SSISDB.catalog.executions.status):
    1  Created         2  Running          3  Canceled
    4  Failed          5  Pending          6  Ended unexpectedly
    7  Succeeded       8  Stopping         9  Completed

Auth: connect to SQL Server hosting SSISDB. Either supply a full SQLAlchemy
connection string via `connection_string_env_var` OR the flat fields
(server / database / user / password / driver) — component builds the URL.

The connecting login needs at minimum:
  - VIEW ANY DATABASE (for enumeration) OR the ssis_admin role on SSISDB
  - ssis_admin (or DBO on SSISDB) if you set `action: execute`
"""

import fnmatch
import hashlib
import json
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


# ── Status code → human string ──────────────────────────────────────
SSIS_STATUS: Dict[int, str] = {
    1: "Created",
    2: "Running",
    3: "Canceled",
    4: "Failed",
    5: "Pending",
    6: "Ended unexpectedly",
    7: "Succeeded",
    8: "Stopping",
    9: "Completed",
}
SSIS_TERMINAL: set = {3, 4, 6, 7, 9}  # any of these = stop polling


# ── Props (@record) for translator callable ─────────────────────────
@record
class SsisPackageProps:
    """Data passed to `translation:` callables for each imported SSIS package.

    Mirrors `HvrObjectProps` / `SnowflakeObjectProps` / `QlikReplicateObjectProps` —
    a single record describing the package so `translation:` callables can
    filter, rename, add tags, etc.

    Attributes:
        folder_name: SSISDB folder name.
        project_name: SSISDB project name.
        project_id: Numeric project_id from SSISDB.catalog.projects.
        package_name: Package name (without the `.dtsx` suffix).
        description: The package's own description string (may be None).
        server_host: The SQL Server host (from workspace.server).
    """

    folder_name: str
    project_name: str
    project_id: int
    package_name: str
    description: Optional[str] = None
    server_host: str = ""

    @property
    def qualified_name(self) -> str:
        return f"{self.folder_name}/{self.project_name}/{self.package_name}"


# ── Workspace config nested block ───────────────────────────────────
class SsisWorkspaceConfig(dg.Model):
    """Connection to the SQL Server hosting SSISDB.

    Supply EITHER `connection_string_env_var` (opaque SQLAlchemy URL) OR
    the flat fields — not both. Flat fields build a pyodbc URL:
    `mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}`.
    """

    connection_string_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing a full SQLAlchemy URL. Takes precedence over "
            "the flat fields. Example value: "
            "`mssql+pyodbc://svc:pw@sql-01/SSISDB?driver=ODBC+Driver+18+for+SQL+Server`."
        ),
    )
    server: Optional[str] = Field(
        default=None, description="SQL Server host / instance (e.g. `sql-01.corp` or `sql-01\\INST1`)."
    )
    database: str = Field(
        default="SSISDB",
        description="SSIS catalog database. Almost always `SSISDB`.",
    )
    user: Optional[str] = Field(
        default=None, description="SQL login username (basic auth)."
    )
    password: Optional[str] = Field(
        default=None, description="SQL login password (basic auth)."
    )
    driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="ODBC driver name for the mssql+pyodbc dialect.",
    )
    trust_server_certificate: bool = Field(
        default=False,
        description="Passes `TrustServerCertificate=yes` on the connection — "
        "needed for lab SQL Servers with self-signed TLS.",
    )


# ── Selector block ──────────────────────────────────────────────────
class SsisPackageSelector(dg.Model):
    """Filter which SSISDB packages become Dagster assets.

    All four filters are ANDed. Wildcards use fnmatch (case-insensitive)."""

    by_folder: Optional[List[str]] = Field(
        default=None,
        description="Restrict to these SSISDB folders (exact match, case-sensitive).",
    )
    by_project: Optional[List[str]] = Field(
        default=None,
        description="Restrict to these SSISDB projects (exact match, case-sensitive).",
    )
    include: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns against `<folder>/<project>/<package>`. "
        "Case-insensitive. Applied AFTER `by_folder`/`by_project`.",
    )
    exclude: Optional[List[str]] = Field(
        default=None,
        description="fnmatch patterns to EXCLUDE. Applied last.",
    )


# ── Base translator ─────────────────────────────────────────────────
class SsisComponentTranslator:
    """Base translator: SsisPackageProps → AssetSpec.

    User's `translation:` callable wraps this via TranslationFn. Mirrors
    HvrHubComponentTranslator / SnowflakeComponentTranslator / etc.
    """

    def __init__(self, component: "SsisWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: SsisPackageProps) -> AssetSpec:
        prefix = self._component.asset_key_prefix or [
            "ssis",
            (props.server_host or "hub").split(".")[0].split("\\")[0].lower(),
        ]
        return AssetSpec(
            key=AssetKey([*prefix, props.folder_name, props.project_name, props.package_name]),
            description=(
                props.description
                or f"SSIS package `{props.qualified_name}` deployed to SSISDB."
            ),
            group_name=self._component.group_name,
            # Dagster kinds — `mssql` has a first-class icon; `ssis` + `etl`
            # render as text-only badges (still useful for filtering the
            # catalog by class-of-tool + vendor).
            kinds=set(self._component.kinds or ["ssis", "mssql", "etl"]),
            tags=dict(self._component.tags or {}),
            owners=list(self._component.owners or []),
            metadata={
                "ssis/folder": props.folder_name,
                "ssis/project": props.project_name,
                "ssis/package": props.package_name,
                "ssis/project_id": props.project_id,
            },
        )


# ── Component ───────────────────────────────────────────────────────
@public
class SsisWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """SQL Server Integration Services (SSIS) as a Dagster workspace.

    Discovers packages deployed to SSISDB, emits one AssetSpec per
    package, and (optionally) triggers `create_execution + start_execution`
    on materialize with completion polling.

    Full workspace-pattern shape:
      - Fivetran-shape `workspace:` block for auth
      - `package_selector:` for filtering discovered packages
      - `translation:` callable for per-asset customization
      - `StateBackedComponent` caching — refresh via
        `dg utils refresh-defs-state`
      - `action: noop | execute`
      - `polling_sensor:` opt-in observation sensor
      - `freshness_lag_threshold_seconds:` asset check per package

    Example:

    ```yaml
    type: dagster_community_components.SsisWorkspaceComponent
    attributes:
      workspace:
        server:   "{{ env.SSIS_SERVER }}"
        database: SSISDB
        user:     "{{ env.SSIS_USER }}"
        password: "{{ env.SSIS_PASSWORD }}"
        trust_server_certificate: true
      package_selector:
        by_folder: [Sales, Finance]
        include:   ["*sales*"]
        exclude:   ["*_test*"]
      # translation: |
      #   {{ load_python_module_attr('my_project.ssis.translate.by_domain') }}
      action: execute
      wait_for_completion: true
      poll_interval_seconds: 30
      timeout_seconds: 3600
      polling_sensor: true
      observation_interval_seconds: 300
      freshness_lag_threshold_seconds: 3600
      group_name: ssis_prod
      kinds: [ssis, mssql, etl]
    ```
    """

    workspace: SsisWorkspaceConfig = Field(
        description="Connection to the SQL Server hosting SSISDB."
    )
    package_selector: Optional[SsisPackageSelector] = Field(
        default=None,
        description="Filter which packages become assets. Default = every "
        "package in SSISDB.",
    )
    translation: Annotated[
        Optional[TranslationFn[SsisPackageProps]],
        TranslationFnResolver(
            template_vars_for_translation_fn=lambda data: {"props": data}
        ),
    ] = Field(
        default=None,
        description=(
            "Optional per-asset translation callable. Receives a "
            "SsisPackageProps and returns a dict of AssetSpec kwargs "
            "(key / group_name / tags / owners / kinds / metadata / "
            "description). Use for per-folder / per-project customization "
            "beyond the uniform group/kinds/tags. Follows the same "
            "convention as dagster-fivetran / dagster-snowflake."
        ),
    )
    action: str = Field(
        default="noop",
        description=(
            "What materialize() does. `noop` = external asset (declare-only). "
            "`execute` = call `SSISDB.catalog.create_execution` + `start_execution`."
        ),
    )
    wait_for_completion: bool = Field(
        default=True,
        description="Only used when `action: execute`. If true, poll executions "
        "until a terminal status; if false, return once SSIS accepts the trigger.",
    )
    poll_interval_seconds: int = Field(
        default=30,
        description="Seconds between polls of `SSISDB.catalog.executions.status`.",
    )
    timeout_seconds: int = Field(
        default=3600,
        description="Overall timeout for a synchronous execution. Set 0 for no timeout.",
    )
    polling_sensor: bool = Field(
        default=False,
        description="If true, emit a sensor that polls SSISDB.catalog.executions "
        "for completions since watermark and emits AssetObservation per package.",
    )
    observation_interval_seconds: int = Field(
        default=300,
        description="Sensor tick interval. Only used when `polling_sensor: true`.",
    )
    freshness_lag_threshold_seconds: Optional[int] = Field(
        default=None,
        description="If set, attach an asset check to every package that fails "
        "when the last successful execution is older than this many seconds.",
    )
    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description="Prefix for every emitted asset key. Default: "
        "`['ssis', <ssis_server_hostname>]`.",
    )
    group_name: Optional[str] = Field(
        default="ssis", description="Dagster asset group."
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds badge (Dagster catalog UI). Default: `['ssis', 'mssql']`.",
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
        "filesystem cache keyed on server+database hash so multiple SSIS "
        "servers don't collide.",
    )

    # ── Base translator (cached per instance) ─────────────────────
    @property
    def _base_translator(self) -> SsisComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = SsisComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @public
    def get_asset_spec(self, props: SsisPackageProps) -> AssetSpec:
        """Public hook — user's `translation:` callable wraps this."""
        base_spec = self._base_translator.get_asset_spec(props)
        if self.translation is None:
            return base_spec
        # user callable → dict of overrides → merge onto base
        overrides = self.translation(base_spec, props) or {}
        if isinstance(overrides, AssetSpec):
            return overrides
        # Dict shape — merge onto base_spec
        return base_spec._replace(**overrides) if hasattr(base_spec, "_replace") else base_spec

    @property
    def defs_state_config(self) -> DefsStateConfig:
        composite = f"{self.workspace.server or 'ssisdb'}::{self.workspace.database}"
        state_hash = hashlib.sha256(composite.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{state_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    # ── Runtime helpers ────────────────────────────────────────────
    def _build_engine(self):
        """Return a SQLAlchemy engine against SSISDB."""
        from sqlalchemy import create_engine
        import os
        cfg = self.workspace

        if cfg.connection_string_env_var:
            url = os.environ.get(cfg.connection_string_env_var, "")
            if not url:
                raise ValueError(
                    f"env var {cfg.connection_string_env_var!r} is empty or unset"
                )
            return create_engine(url)

        if not (cfg.server and cfg.user and cfg.password):
            raise ValueError(
                "SsisWorkspaceComponent.workspace: supply either "
                "connection_string_env_var OR the {server, user, password} flat fields."
            )
        from urllib.parse import quote_plus
        driver = quote_plus(cfg.driver)
        params = f"driver={driver}"
        if cfg.trust_server_certificate:
            params += "&TrustServerCertificate=yes"
        return create_engine(
            f"mssql+pyodbc://{cfg.user}:{quote_plus(cfg.password)}"
            f"@{cfg.server}/{cfg.database}?{params}"
        )

    def _discover_packages(self, engine) -> List[Dict[str, Any]]:
        """Enumerate SSISDB.catalog.packages. Apply the selector.
        Returns dicts (not props objects) for JSON serialization to state."""
        from sqlalchemy import text as sa_text
        sql = sa_text(
            """
            SELECT
                f.name          AS folder_name,
                p.name          AS project_name,
                p.project_id    AS project_id,
                pkg.name        AS package_name,
                pkg.description AS description
            FROM SSISDB.catalog.packages pkg
            JOIN SSISDB.catalog.projects p ON pkg.project_id = p.project_id
            JOIN SSISDB.catalog.folders  f ON p.folder_id   = f.folder_id
            ORDER BY f.name, p.name, pkg.name
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()

        packages: List[Dict[str, Any]] = []
        for r in rows:
            pkg_name = r["package_name"]
            if pkg_name.lower().endswith(".dtsx"):
                pkg_name = pkg_name[:-5]
            packages.append({
                "folder_name": r["folder_name"],
                "project_name": r["project_name"],
                "project_id": int(r["project_id"]),
                "package_name": pkg_name,
                "description": r.get("description"),
            })

        sel = self.package_selector
        if not sel:
            return packages

        def _match(p: Dict[str, Any]) -> bool:
            if sel.by_folder and p["folder_name"] not in sel.by_folder:
                return False
            if sel.by_project and p["project_name"] not in sel.by_project:
                return False
            qname_lower = f"{p['folder_name']}/{p['project_name']}/{p['package_name']}".lower()
            if sel.include and not any(
                fnmatch.fnmatch(qname_lower, pat.lower()) for pat in sel.include
            ):
                return False
            if sel.exclude and any(
                fnmatch.fnmatch(qname_lower, pat.lower()) for pat in sel.exclude
            ):
                return False
            return True

        return [p for p in packages if _match(p)]

    def _execute_package(self, engine, props: SsisPackageProps, context) -> Dict[str, Any]:
        """EXEC create_execution + start_execution + poll to completion."""
        from sqlalchemy import text as sa_text
        pkg_dtsx = f"{props.package_name}.dtsx"

        with engine.connect() as conn:
            row = conn.execute(
                sa_text(
                    """
                    DECLARE @exec_id BIGINT;
                    EXEC SSISDB.catalog.create_execution
                        @package_name  = :pkg,
                        @folder_name   = :folder,
                        @project_name  = :project,
                        @use32bitruntime = 0,
                        @reference_id  = NULL,
                        @execution_id  = @exec_id OUTPUT;
                    SELECT @exec_id AS execution_id;
                    """
                ),
                {"pkg": pkg_dtsx, "folder": props.folder_name, "project": props.project_name},
            ).mappings().first()
            execution_id = row["execution_id"] if row else None
            if execution_id is None:
                raise RuntimeError(f"create_execution returned no execution_id for {props.qualified_name}")

            conn.execute(
                sa_text("EXEC SSISDB.catalog.start_execution :exec_id"),
                {"exec_id": execution_id},
            )
            if hasattr(conn, "commit"):
                conn.commit()

        context.log.info(
            f"SSIS package {props.qualified_name} started — execution_id={execution_id}"
        )

        if not self.wait_for_completion:
            return {
                "execution_id": execution_id,
                "status": None,
                "status_text": "async — not polled",
                "error_message": None,
            }

        start_time = time.time()
        while True:
            with engine.connect() as conn:
                row = conn.execute(
                    sa_text(
                        "SELECT status FROM SSISDB.catalog.executions WHERE execution_id = :id"
                    ),
                    {"id": execution_id},
                ).mappings().first()
            status = int(row["status"]) if row and row.get("status") is not None else None
            if status in SSIS_TERMINAL:
                status_text = SSIS_STATUS.get(status, str(status)) if status is not None else "unknown"
                error_message = None
                if status != 7:
                    with engine.connect() as conn:
                        msgs = conn.execute(
                            sa_text(
                                """
                                SELECT TOP 5 message
                                FROM SSISDB.catalog.operation_messages
                                WHERE operation_id = :id AND message_type = 120
                                ORDER BY message_time DESC
                                """
                            ),
                            {"id": execution_id},
                        ).mappings().all()
                    error_message = " | ".join(m["message"] for m in msgs) or None
                return {
                    "execution_id": execution_id,
                    "status": status,
                    "status_text": status_text,
                    "error_message": error_message,
                }
            if self.timeout_seconds and (time.time() - start_time) > self.timeout_seconds:
                raise TimeoutError(
                    f"SSIS package {props.qualified_name} exceeded timeout of {self.timeout_seconds}s"
                    f" (last status={SSIS_STATUS.get(status, str(status)) if status is not None else 'unknown'})"
                )
            time.sleep(self.poll_interval_seconds)

    # ── StateBackedComponent contract ─────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        """Discover packages via SSISDB query + write JSON snapshot to state_path."""
        try:
            engine = self._build_engine()
            packages = self._discover_packages(engine)
        except Exception:  # noqa: BLE001
            packages = []
        snapshot = {
            "server_host": self.workspace.server or "",
            "packages": packages,
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
        packages = state.get("packages", [])
        server_host = state.get("server_host", self.workspace.server or "")

        specs: List[AssetSpec] = []
        for p in packages:
            props = SsisPackageProps(
                folder_name=p["folder_name"],
                project_name=p["project_name"],
                project_id=int(p["project_id"]),
                package_name=p["package_name"],
                description=p.get("description"),
                server_host=server_host,
            )
            specs.append(self.get_asset_spec(props))

        # Build compute (external asset or executable) per action.
        assets: List[Any] = []
        action = (self.action or "noop").lower()

        if action == "noop":
            assets = list(specs)
        elif action == "execute":
            _self = self

            @dg.multi_asset(specs=specs)
            def _ssis_execute(context: dg.AssetExecutionContext):
                exec_engine = _self._build_engine()
                for spec in specs:
                    props = SsisPackageProps(
                        folder_name=spec.metadata["ssis/folder"],
                        project_name=spec.metadata["ssis/project"],
                        project_id=int(spec.metadata["ssis/project_id"]),
                        package_name=spec.metadata["ssis/package"],
                        description=None,
                        server_host=server_host,
                    )
                    result = _self._execute_package(exec_engine, props, context)
                    if result.get("status") not in (None, 7):
                        raise dg.Failure(
                            description=(
                                f"SSIS package {props.qualified_name} finished with "
                                f"status={result['status_text']!r}. "
                                f"error={result.get('error_message')!r}"
                            )
                        )
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "ssis/execution_id": result["execution_id"],
                            "ssis/status": result["status_text"],
                        },
                    )

            assets = [_ssis_execute]
        else:
            raise ValueError(
                f"SsisWorkspaceComponent.action={action!r} not supported. "
                f"Use 'noop' or 'execute'."
            )

        # Polling sensor + freshness checks.
        sensors: List[Any] = []
        if self.polling_sensor and specs:
            sensors.append(self._build_observation_sensor(specs, server_host))

        checks: List[Any] = []
        if self.freshness_lag_threshold_seconds is not None and specs:
            checks.extend(self._build_freshness_checks(specs))

        return Definitions(assets=assets, sensors=sensors, asset_checks=checks)

    def _build_observation_sensor(self, specs: List[AssetSpec], server_host: str):
        _self = self
        # Rebuild the prefix from workspace so sensor + specs agree.
        prefix = self.asset_key_prefix or [
            "ssis",
            (server_host or "hub").split(".")[0].split("\\")[0].lower(),
        ]

        @dg.sensor(
            name="ssis_workspace_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            cursor_val = int(context.cursor) if context.cursor and context.cursor.isdigit() else 0
            engine = _self._build_engine()
            from sqlalchemy import text as sa_text
            with engine.connect() as conn:
                rows = conn.execute(
                    sa_text(
                        """
                        SELECT e.execution_id, e.folder_name, e.project_name,
                               e.package_name, e.status, e.end_time
                        FROM SSISDB.catalog.executions e
                        WHERE e.execution_id > :cursor
                          AND e.end_time IS NOT NULL
                        ORDER BY e.execution_id
                        """
                    ),
                    {"cursor": cursor_val},
                ).mappings().all()

            observations = []
            new_cursor = cursor_val
            for r in rows:
                exec_id = int(r["execution_id"])
                new_cursor = max(new_cursor, exec_id)
                pkg_name = r["package_name"] or ""
                if pkg_name.lower().endswith(".dtsx"):
                    pkg_name = pkg_name[:-5]
                key_path = [*prefix, r["folder_name"], r["project_name"], pkg_name]
                observations.append(
                    dg.AssetObservation(
                        asset_key=dg.AssetKey(key_path),
                        metadata={
                            "ssis/execution_id": exec_id,
                            "ssis/status": SSIS_STATUS.get(int(r["status"]), str(r["status"])),
                            "ssis/end_time": str(r["end_time"]),
                        },
                    )
                )
            return dg.SensorResult(
                asset_events=observations,
                cursor=str(new_cursor),
            )

        return _observation_sensor

    def _build_freshness_checks(self, specs: List[AssetSpec]) -> List[Any]:
        from datetime import datetime, timezone
        _self = self
        threshold = self.freshness_lag_threshold_seconds

        checks = []
        for spec in specs:
            @dg.asset_check(
                asset=spec.key,
                name="ssis_freshness_lag",
                description=(
                    f"Fails when the last successful SSIS execution of "
                    f"this package is older than {threshold}s."
                ),
            )
            def _check(_spec_key=spec.key):
                pkg_meta = next(
                    s.metadata for s in specs if s.key == _spec_key
                )
                from sqlalchemy import text as sa_text
                engine = _self._build_engine()
                with engine.connect() as conn:
                    row = conn.execute(
                        sa_text(
                            """
                            SELECT TOP 1 end_time
                            FROM SSISDB.catalog.executions
                            WHERE folder_name = :folder
                              AND project_name = :project
                              AND package_name = :package
                              AND status = 7
                            ORDER BY end_time DESC
                            """
                        ),
                        {
                            "folder": pkg_meta["ssis/folder"],
                            "project": pkg_meta["ssis/project"],
                            "package": pkg_meta["ssis/package"] + ".dtsx",
                        },
                    ).mappings().first()
                if not row or row["end_time"] is None:
                    return dg.AssetCheckResult(
                        passed=False,
                        description="No successful executions found.",
                    )
                end_time = row["end_time"]
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                lag = (datetime.now(timezone.utc) - end_time).total_seconds()
                return dg.AssetCheckResult(
                    passed=lag <= threshold,
                    description=f"lag={int(lag)}s (threshold={threshold}s)",
                    metadata={"ssis/last_success_at": str(end_time), "ssis/lag_seconds": int(lag)},
                )
            checks.append(_check)
        return checks
