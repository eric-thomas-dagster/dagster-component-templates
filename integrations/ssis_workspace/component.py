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


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Construct a Dagster partitions_def from the canonical flat partition fields.

    Strict: raises ValueError on misconfigured combinations rather than silently
    picking a default (time-based types need partition_start; static needs
    partition_values; dynamic needs dynamic_partition_name).
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if isinstance(partition_values, (list, tuple)):
            values = [str(v).strip() for v in partition_values if str(v).strip()]
        else:
            values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
        if not values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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


def _topological_order(specs: List[AssetSpec], selected_keys) -> List[AssetSpec]:
    """Order `specs` (already filtered to the selected subset) so that a
    package depending on another SELECTED package (via `assets_by_package`'s
    `deps:`) always runs after it. Deps pointing outside the selected subset
    (including outside SSIS entirely) are ignored here -- Dagster's own
    execution ordering already guarantees those completed before this step
    runs at all; this function only orders execution WITHIN one multi_asset
    invocation, where Dagster has no visibility into the loop."""
    by_key = {s.key: s for s in specs}
    order: List[AssetSpec] = []
    visited: set = set()
    in_progress: set = set()

    def visit(spec: AssetSpec) -> None:
        if spec.key in visited:
            return
        if spec.key in in_progress:
            raise ValueError(
                f"circular dependency detected among selected SSIS packages involving {spec.key.to_user_string()!r} "
                "-- check assets_by_package deps: for a cycle."
            )
        in_progress.add(spec.key)
        for dep in (spec.deps or []):
            dep_key = getattr(dep, "asset_key", dep)
            dep_spec = by_key.get(dep_key)
            if dep_spec is not None:
                visit(dep_spec)
        in_progress.discard(spec.key)
        visited.add(spec.key)
        order.append(spec)

    for s in specs:
        visit(s)
    return order


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


# ── Per-package parameter override ──────────────────────────────────
class SsisPackageOverride(dg.Model):
    """Override runtime parameters + environment reference for packages
    matching an fnmatch pattern against `<folder>/<project>/<package>`.

    Applied at `create_execution` time via
    `SSISDB.catalog.set_execution_parameter_value`. Values can reference
    partitions with `{partition_key}` — the string is `.format()`-substituted
    at run time. Environments (`environment_reference:` field) map to
    `SSISDB.catalog.create_environment_reference`, letting the SSISDB
    environment's variable bundle override matching parameters.

    Object type mapping (SQL Server docs):
      - `object_type=20` → project parameter (project-scoped)
      - `object_type=30` → package parameter (package-scoped)
      - `object_type=50` → property override (rarely needed)

    Parameter scope defaults to `package` (30). Set `scope: project` to
    write project-level parameters instead.
    """

    match: str = Field(
        description="fnmatch pattern against `<folder>/<project>/<package>` "
        "(case-insensitive). Example: 'Sales/*/LoadCustomers'."
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Parameter name → value. Value can be a Jinja-templated "
        "string with `{partition_key}` for partitioned runs.",
    )
    scope: str = Field(
        default="package",
        description="'package' (object_type=30, default) or 'project' (object_type=20).",
    )
    environment_reference: Optional[str] = Field(
        default=None,
        description="Name of an SSISDB environment (in the same folder as the "
        "project) to reference for this execution. Environment variables "
        "override parameter values.",
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
        kwargs: Dict[str, Any] = dict(
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
            deps=[],
            metadata={
                "ssis/folder": props.folder_name,
                "ssis/project": props.project_name,
                "ssis/package": props.package_name,
                "ssis/project_id": props.project_id,
            },
            partitions_def=self._component._partitions_def,
        )
        kwargs = self._component._apply_asset_overrides(props.qualified_name, kwargs)
        return AssetSpec(**kwargs)


# ── Component ───────────────────────────────────────────────────────

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
    demo_mode: bool = Field(
        default=False,
        description=(
            "Skip the real SQL Server connection entirely. Discovery returns a small synthetic "
            "package catalog (2 packages across 1 folder/project); execute/poll, the observation "
            "sensor, and freshness checks all log a simulated SSIS lifecycle instead of running "
            "real T-SQL against SSISDB. Use to smoke-test the whole component shape -- can_subset, "
            "assets_by_package overrides, the sensor, checks -- with no real server available."
        ),
    )
    package_selector: Optional[SsisPackageSelector] = Field(
        default=None,
        description="Filter which packages become assets. Default = every "
        "package in SSISDB.",
    )
    package_overrides: Optional[List[SsisPackageOverride]] = Field(
        default=None,
        description=(
            "Runtime parameters + environment references applied at execute "
            "time via `SSISDB.catalog.set_execution_parameter_value` (and, "
            "when set, `create_environment_reference`). Only used when "
            "`action: execute`. Multiple overrides match in order — every "
            "matching entry's parameters merge (later entries win on conflict). "
            "See `SsisPackageOverride` for shape."
        ),
    )
    assets_by_package: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec for specific packages, keyed by qualified name "
            "'<folder>/<project>/<package>'. Mirrors the official dagster-databricks "
            "`DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention "
            "`snowflake_workspace.assets_by_name` uses in this registry).\n\n"
            "Per-package keys (all optional):\n"
            "  key:          renames the Dagster asset key (slash-separated -> AssetKey)\n"
            "  group_name:   overrides the group\n"
            "  description:  overrides the description\n"
            "  deps:         list of upstream asset keys (slash-separated), including other SSIS "
            "                package asset keys -- merged with whatever this component infers. "
            "                When `action: execute`, a dependency that is ALSO a selected SSIS "
            "                package asset runs before the package that depends on it.\n"
            "  metadata:     dict merged into the auto-emitted metadata\n"
            "  tags:         dict merged into asset tags\n"
            "  kinds:        list of kind strings (overrides inference)\n"
            "  owners:       list of owners\n\n"
            "Example — declare a real run-order dependency between two packages:\n"
            "  assets_by_package:\n"
            "    Sales/SalesETL/LoadOrders:\n"
            "      deps: [ssis/sql-01/Sales/SalesETL/ExtractCustomers]\n"
        ),
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
    max_execution_minutes: Optional[int] = Field(
        default=None,
        description=(
            "If set, attach a `ssis_completed_within_window` asset check to every "
            "package that fails when the package's LATEST successful execution took "
            "longer than this many minutes (DATEDIFF(MINUTE, start_time, end_time) "
            "from SSISDB.catalog.executions) -- a runtime/SLA check, distinct from "
            "`freshness_lag_threshold_seconds` (which checks staleness, i.e. how "
            "long ago it last succeeded, not how long that run took)."
        ),
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
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Applies the SAME partitions_def to every imported package: "
            "'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic', or None for "
            "unpartitioned (default). Real per-package execution history ("
            "`SSISDB.catalog.executions`) has no native partition concept, so the "
            "observation sensor can only derive a partition key for time-based "
            "types (from each execution's end_time) -- static/dynamic partition "
            "types attach the partitions_def to the asset (for lineage/backfill "
            "UI purposes) but the sensor cannot auto-tag observations against "
            "them; emit AssetObservation(partition=...) yourself via a subclass "
            "if you need that."
        ),
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01').",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for partition_type='static'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners'), csv=(), tags=('tags',))

    @property
    def _partitions_def(self):
        return _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

    def _apply_asset_overrides(self, qualified_name: str, base_kwargs: dict) -> dict:
        """Merge per-package overrides from `assets_by_package` into AssetSpec kwargs.

        Mirrors the official DatabricksWorkspaceComponent.assets_by_task_key
        pattern (same convention snowflake_workspace.assets_by_name already
        uses in this registry). Keyed by the package's qualified name
        ('<folder>/<project>/<package>'). Each override is itself a dict;
        supported keys: key, group_name, description, deps, metadata, tags,
        kinds, owners.

        - `key` replaces the inferred AssetKey with an explicit one.
        - `deps` is merged with any deps already set (none, by default --
          SSISDB.catalog has no native concept of inter-package dependency,
          so this is the only way to declare real run-order between packages).
        - `metadata` and `tags` are dict-merged (override wins on conflicts).
        - `group_name` / `description` / `kinds` / `owners` straight-replace.
        """
        if not self.assets_by_package:
            return base_kwargs
        override = self.assets_by_package.get(qualified_name)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result["key"] = AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(AssetKey.from_user_string(d) for d in override["deps"])
            result["deps"] = existing
        if "metadata" in override:
            existing = dict(result.get("metadata") or {})
            existing.update(override["metadata"])
            result["metadata"] = existing
        if "tags" in override:
            existing = dict(result.get("tags") or {})
            existing.update(override["tags"])
            result["tags"] = existing
        if "kinds" in override:
            result["kinds"] = set(override["kinds"])
        if "owners" in override:
            result["owners"] = override["owners"]
        return result

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
        """Return a SQLAlchemy engine against SSISDB. Every real caller already
        guards on demo_mode before reaching here (discovery/execute/sensor/checks
        all branch to a simulated path first) -- this is a defensive backstop,
        not the primary demo_mode seam, so a future caller that forgets the
        guard fails loudly instead of trying a real SQL Server connection."""
        if self.demo_mode:
            raise RuntimeError(
                "_build_engine() called while demo_mode=true -- this is a bug in the "
                "component (a real SQL path was reached that should have branched to "
                "a simulated one first)."
            )
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

    def _demo_packages(self) -> List[Dict[str, Any]]:
        """Synthetic 2-package catalog for demo_mode -- same shape _discover_packages
        returns from a real SSISDB query, so build_defs_from_state and everything
        downstream of it (specs, execute, sensor, checks) is identical code."""
        return [
            {
                "folder_name": "Demo", "project_name": "DemoETL", "project_id": 1,
                "package_name": "ExtractCustomers", "description": "Demo package: extract customers.",
            },
            {
                "folder_name": "Demo", "project_name": "DemoETL", "project_id": 1,
                "package_name": "LoadOrders", "description": "Demo package: load orders (depends on ExtractCustomers).",
            },
        ]

    def _apply_selector(self, packages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    def _discover_packages(self, engine) -> List[Dict[str, Any]]:
        """Enumerate SSISDB.catalog.packages. Apply the selector.
        Returns dicts (not props objects) for JSON serialization to state.
        demo_mode skips the real SQL entirely and uses a synthetic catalog."""
        if self.demo_mode:
            return self._apply_selector(self._demo_packages())

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

        return self._apply_selector(packages)

    def _resolve_overrides(self, props: SsisPackageProps, context) -> Dict[str, Any]:
        """Merge all matching package_overrides into one {parameters, scope, environment_reference}.

        `parameters` values pass through `str.format(partition_key=...)` when
        the run is partitioned, so `"{partition_key}"` substitutes at run time.
        """
        merged_params: Dict[str, Any] = {}
        env_ref: Optional[str] = None
        # SSIS uses object_type integers on set_execution_parameter_value —
        # 30 = package param (default), 20 = project param. Track the last
        # explicit scope declaration per key so users can pin a specific one.
        param_scopes: Dict[str, str] = {}

        if not self.package_overrides:
            return {"parameters": {}, "param_scopes": {}, "environment_reference": None}

        qname_lower = props.qualified_name.lower()
        partition_key = getattr(context, "partition_key", None)

        for ov in self.package_overrides:
            if not fnmatch.fnmatch(qname_lower, ov.match.lower()):
                continue
            if ov.parameters:
                for k, v in ov.parameters.items():
                    if isinstance(v, str) and partition_key is not None and "{partition_key}" in v:
                        v = v.format(partition_key=partition_key)
                    merged_params[k] = v
                    param_scopes[k] = ov.scope
            if ov.environment_reference:
                env_ref = ov.environment_reference

        return {
            "parameters": merged_params,
            "param_scopes": param_scopes,
            "environment_reference": env_ref,
        }

    def _execute_demo_package(self, props: SsisPackageProps, context) -> Dict[str, Any]:
        """Simulate the create_execution/start_execution/poll lifecycle on stdout --
        no SQL Server connection at all. Deterministic (seeded on the package's own
        qualified name), always Succeeded, so demo_mode is useful to exercise
        can_subset ordering and the sensor/checks without a real SSIS instance."""
        import random
        rng = random.Random(props.qualified_name)
        execution_id = rng.randint(100000, 999999)
        context.log.info(f"[SSIS DEMO] create_execution + start_execution for {props.qualified_name} -> execution_id={execution_id}")
        context.log.info(f"[SSIS DEMO] {props.qualified_name} -> Succeeded")
        return {
            "execution_id": execution_id,
            "status": 7,
            "status_text": "Succeeded",
            "error_message": None,
        }

    def _execute_package(self, engine, props: SsisPackageProps, context) -> Dict[str, Any]:
        """EXEC create_execution + optional set_parameter_value + start_execution + poll."""
        if self.demo_mode:
            return self._execute_demo_package(props, context)

        from sqlalchemy import text as sa_text
        pkg_dtsx = f"{props.package_name}.dtsx"

        override = self._resolve_overrides(props, context)
        env_ref_name = override.get("environment_reference")
        parameters = override.get("parameters") or {}
        param_scopes = override.get("param_scopes") or {}

        with engine.connect() as conn:
            # Environment reference (optional). SSIS environments live under
            # a folder; the reference has to already exist against the target
            # project (created via SSMS or `catalog.create_environment_reference`).
            reference_id = None
            if env_ref_name:
                ref_row = conn.execute(
                    sa_text(
                        """
                        SELECT er.reference_id
                        FROM SSISDB.catalog.environment_references er
                        WHERE er.project_id = :pid AND er.environment_name = :env
                        """
                    ),
                    {"pid": props.project_id, "env": env_ref_name},
                ).mappings().first()
                if not ref_row:
                    raise RuntimeError(
                        f"SSIS environment_reference {env_ref_name!r} not found for "
                        f"project_id={props.project_id}. Create it via SSMS or "
                        f"`SSISDB.catalog.create_environment_reference` first."
                    )
                reference_id = int(ref_row["reference_id"])

            row = conn.execute(
                sa_text(
                    """
                    DECLARE @exec_id BIGINT;
                    EXEC SSISDB.catalog.create_execution
                        @package_name  = :pkg,
                        @folder_name   = :folder,
                        @project_name  = :project,
                        @use32bitruntime = 0,
                        @reference_id  = :ref,
                        @execution_id  = @exec_id OUTPUT;
                    SELECT @exec_id AS execution_id;
                    """
                ),
                {
                    "pkg": pkg_dtsx,
                    "folder": props.folder_name,
                    "project": props.project_name,
                    "ref": reference_id,
                },
            ).mappings().first()
            execution_id = row["execution_id"] if row else None
            if execution_id is None:
                raise RuntimeError(f"create_execution returned no execution_id for {props.qualified_name}")

            # Runtime parameter overrides — one call per parameter.
            #   object_type 20 = project param, 30 = package param
            for pname, pvalue in parameters.items():
                scope = param_scopes.get(pname, "package")
                object_type = 20 if scope == "project" else 30
                conn.execute(
                    sa_text(
                        "EXEC SSISDB.catalog.set_execution_parameter_value "
                        "@execution_id = :eid, "
                        "@object_type  = :otype, "
                        "@parameter_name = :pname, "
                        "@parameter_value = :pval"
                    ),
                    {
                        "eid": execution_id,
                        "otype": object_type,
                        "pname": pname,
                        "pval": pvalue,
                    },
                )

            conn.execute(
                sa_text("EXEC SSISDB.catalog.start_execution :exec_id"),
                {"exec_id": execution_id},
            )
            if hasattr(conn, "commit"):
                conn.commit()

        if parameters or env_ref_name:
            context.log.info(
                f"SSIS package {props.qualified_name} started — execution_id={execution_id}, "
                f"parameters={list(parameters.keys())}, environment_reference={env_ref_name}"
            )
        else:
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
    async def refresh_state(self, project_root: Path) -> str:
        """Refresh LOCAL_FILESYSTEM state atomically.

        The stock ``StateBackedComponent.refresh_state`` does ``shutil.rmtree(state_path)`` and only then
        runs ``write_state_to_path`` (network calls, seconds long), so for the whole fetch -- and again
        while the file is half-written -- a concurrent reader (run worker, step subprocess, ``dg list defs``;
        every ``dg`` command refreshes in dev mode) sees missing/partial state: empty Definitions or a
        JSONDecodeError. Here the new state is built beside the old and swapped in with a rename, so the
        previous state stays readable until the new one is complete (and survives a failed refresh)."""
        import os
        import shutil
        import uuid

        from dagster._core.storage.defs_state.base import DefsStateStorage
        from dagster.components.utils.project_paths import get_local_state_path
        from dagster_shared.serdes.objects.models.defs_state_info import (
            LOCAL_STATE_VERSION,
            DefsStateManagementType,
        )

        config = self.defs_state_config
        if config.management_type != DefsStateManagementType.LOCAL_FILESYSTEM:
            return await super().refresh_state(project_root)
        state_path = get_local_state_path(config.key, project_root)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        token = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        work = state_path.parent / f".{state_path.name}.refresh-{token}"
        old = state_path.parent / f".{state_path.name}.old-{token}"
        try:
            await self._write_state_to_path_async(work)
            if work.exists():
                if work.is_dir() and state_path.is_dir():
                    try:
                        os.rename(state_path, old)
                        os.rename(work, state_path)
                    except OSError:  # e.g. Windows: directory held open by another process
                        shutil.rmtree(state_path, ignore_errors=True)
                        os.rename(work, state_path)
                else:
                    if state_path.is_dir():
                        shutil.rmtree(state_path, ignore_errors=True)
                    elif work.is_dir() and state_path.exists():
                        state_path.unlink()
                    os.replace(work, state_path)
        finally:
            for _p in (work, old):
                if _p.is_dir():
                    shutil.rmtree(_p, ignore_errors=True)
                else:
                    _p.unlink(missing_ok=True)
        state_storage = DefsStateStorage.get()
        if state_storage:
            state_storage.set_latest_version(config.key, LOCAL_STATE_VERSION)
        return LOCAL_STATE_VERSION

    async def write_state_to_path(self, state_path: Path) -> None:
        """Discover packages via SSISDB query (or the synthetic demo_mode catalog)
        + write JSON snapshot to state_path."""
        if self.demo_mode:
            packages = self._discover_packages(None)
        else:
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
            # A genuine no-op executable (not a bare AssetSpec): SSISDB.catalog's
            # packages are observe-only here -- Dagster never triggers them -- but
            # `freshness_lag_threshold_seconds` checks still need SOME executable
            # node to attach to. Dagster can't build a job (nor does `dg launch`
            # offer anything to click) for a check-only selection over a bare
            # AssetSpec with zero executable nodes
            # (`DagsterInvalidDefinitionError: Selected keys must be a subset of
            # existing executable asset keys`) -- this was invisible until a check
            # was actually run through a job, since an external AssetSpec still
            # shows up in the catalog fine on its own. The real activity signal
            # still comes from the observation sensor; materializing this just
            # logs and does nothing to SSIS.
            if specs:
                @dg.multi_asset(specs=specs)
                def _ssis_noop(context: dg.AssetExecutionContext):
                    context.log.info(
                        f"SSIS workspace action=noop: {len(specs)} package(s) are "
                        "observe-only; materializing here does not trigger SSIS."
                    )

                assets = [_ssis_noop]
            else:
                assets = []
        elif action == "execute":
            _self = self

            # can_subset=True: materializing ONE package must execute only that
            # package (plus any SELECTED assets_by_package dependency ahead of
            # it), not every discovered package. Before this fix, a bare
            # @multi_asset(specs=specs) with no can_subset ran the full loop
            # below regardless of what was actually selected -- selecting one
            # asset in the UI still triggered every SSIS package.
            @dg.multi_asset(specs=specs, can_subset=True)
            def _ssis_execute(context: dg.AssetExecutionContext):
                exec_engine = None if _self.demo_mode else _self._build_engine()
                selected = context.selected_asset_keys
                selected_specs = [s for s in specs if s.key in selected]
                for spec in _topological_order(selected_specs, selected):
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
        if self.max_execution_minutes is not None and specs:
            checks.extend(self._build_duration_checks(specs))

        return Definitions(assets=assets, sensors=sensors, asset_checks=checks)

    def _build_observation_sensor(self, specs: List[AssetSpec], server_host: str):
        _self = self

        @dg.sensor(
            name="ssis_workspace_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            from datetime import timezone
            cursor_val = int(context.cursor) if context.cursor and context.cursor.isdigit() else 0

            if _self.demo_mode:
                import random
                tick = cursor_val + 1
                spec = specs[(tick - 1) % len(specs)]
                exec_id = tick
                rng = random.Random(f"{spec.key.to_user_string()}:{tick}")
                status = rng.choice([7, 7, 7, 4])  # mostly Succeeded, sometimes Failed
                context.log.info(f"[SSIS DEMO] observed execution_id={exec_id} for {spec.key.to_user_string()} -> {SSIS_STATUS.get(status, status)}")
                return dg.SensorResult(
                    asset_events=[dg.AssetObservation(
                        asset_key=spec.key,
                        metadata={
                            "ssis/execution_id": exec_id,
                            "ssis/status": SSIS_STATUS.get(status, str(status)),
                            "ssis/end_time": "demo",
                        },
                    )],
                    cursor=str(tick),
                )

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
                # Resolve the REAL asset key via get_asset_spec -- the documented
                # override point this component's own docstring calls out -- instead
                # of independently reconstructing [*prefix, folder, project, pkg].
                # A subclass overriding get_asset_spec (e.g. to pin asset keys to the
                # source system's own naming, per the demo_mode_pattern convention)
                # used to silently split sensor identity from asset identity: the
                # observation landed on a key nothing in the graph has, and the UI
                # showed no activity on the actual asset.
                props = SsisPackageProps(
                    folder_name=r["folder_name"],
                    project_name=r["project_name"],
                    # SSISDB.catalog.executions doesn't carry project_id (would need an
                    # extra join to catalog.projects) -- 0 is a placeholder. Only matters
                    # if a user's own `translation:` callable keys off project_id, which
                    # the default translator does not (key is folder/project/package name
                    # only; project_id is metadata-only).
                    project_id=0,
                    package_name=pkg_name,
                    description=None,
                    server_host=server_host,
                )
                resolved_key = _self.get_asset_spec(props).key

                # Best-effort partition tag: SSISDB.catalog.executions has no native
                # partition concept, but for a TIME-based partitions_def we can derive
                # the partition key from the execution's own end_time. Not possible for
                # static/dynamic partition types (no timestamp-to-key mapping exists) --
                # those stay unpartitioned observations even if the asset itself is
                # partitioned (documented on the partition_type field).
                partition_kwargs: Dict[str, Any] = {}
                pdef = _self._partitions_def
                end_time = r["end_time"]
                if pdef is not None and hasattr(pdef, "get_partition_key_for_timestamp") and end_time is not None:
                    try:
                        ts = end_time.replace(tzinfo=end_time.tzinfo or timezone.utc).timestamp()
                        partition_kwargs["partition"] = pdef.get_partition_key_for_timestamp(ts)
                    except Exception:
                        pass  # static/dynamic defs, or an out-of-range timestamp -- leave unpartitioned

                observations.append(
                    dg.AssetObservation(
                        asset_key=resolved_key,
                        metadata={
                            "ssis/execution_id": exec_id,
                            "ssis/status": SSIS_STATUS.get(int(r["status"]), str(r["status"])),
                            "ssis/end_time": str(r["end_time"]),
                        },
                        **partition_kwargs,
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

        # Factory function, not a default-argument trick (`def _check(_spec_key=spec.key)`):
        # Dagster's asset-check machinery treats any non-`context` parameter on an
        # `@asset_check` function as an input to load from the checked asset via the
        # project's default IO manager. The old default-argument trick avoided Python's
        # closure late-binding bug (every `_check` would otherwise see the LAST loop
        # iteration's `spec`) but, since `_spec_key` isn't named `context`, Dagster tried
        # to load it as a real input -- invisible under `action: noop` (the check never
        # ran through a job at all -- see the noop-executable-stub fix below), but a real
        # crash under `action: execute` the moment the project's default io_manager is
        # type-specific (e.g. an Iceberg IO manager for other assets in the same
        # project): `CheckError: <IOManager> does not have a handler for type
        # 'typing.Any'`, for a check with nothing to do with that IO manager at all.
        def _make_check(spec):
            pkg_meta = spec.metadata

            @dg.asset_check(
                asset=spec.key,
                name="ssis_freshness_lag",
                description=(
                    f"Fails when the last successful SSIS execution of "
                    f"this package is older than {threshold}s."
                ),
            )
            def _check(context):
                if _self.demo_mode:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"[SSIS DEMO] lag=0s (threshold={threshold}s)",
                        metadata={"ssis/last_success_at": "demo", "ssis/lag_seconds": 0},
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
            return _check

        checks = []
        for spec in specs:
            checks.append(_make_check(spec))
        return checks

    def _build_duration_checks(self, specs: List[AssetSpec]) -> List[Any]:
        """Execution-DURATION SLA check -- distinct from _build_freshness_checks'
        staleness check. A package can be perfectly fresh (ran 5 minutes ago)
        and still have badly blown its runtime budget; neither check catches
        the other's failure mode."""
        _self = self
        max_minutes = self.max_execution_minutes

        # Factory function for the same closure/IO-manager-input reason
        # documented on _build_freshness_checks' _make_check above.
        def _make_check(spec):
            pkg_meta = spec.metadata

            @dg.asset_check(
                asset=spec.key,
                name="ssis_completed_within_window",
                description=f"Fails when the package's latest successful execution took longer than {max_minutes} minutes.",
            )
            def _check(context):
                if _self.demo_mode:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"[SSIS DEMO] minutes_taken=0 (max={max_minutes})",
                        metadata={"ssis/minutes_taken": 0, "ssis/max_minutes": max_minutes},
                    )
                from sqlalchemy import text as sa_text
                engine = _self._build_engine()
                with engine.connect() as conn:
                    row = conn.execute(
                        sa_text(
                            """
                            SELECT TOP 1 start_time, end_time
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
                if not row or row["start_time"] is None or row["end_time"] is None:
                    return dg.AssetCheckResult(passed=False, description="No successful executions found.")
                minutes_taken = (row["end_time"] - row["start_time"]).total_seconds() / 60
                return dg.AssetCheckResult(
                    passed=minutes_taken <= max_minutes,
                    description=f"minutes_taken={minutes_taken:.1f} (max={max_minutes})",
                    metadata={"ssis/minutes_taken": round(minutes_taken, 1), "ssis/max_minutes": max_minutes},
                )

            return _check

        return [_make_check(spec) for spec in specs]
