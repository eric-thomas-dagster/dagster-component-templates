"""SQL Server Stored Procedure Workspace Component.

Wraps plain T-SQL stored procedures on an on-prem SQL Server estate as
Dagster assets -- the T-SQL equivalent of `ssis_workspace` (SSISDB packages)
for shops that run ETL directly out of stored procedures, not SSIS. Many
SQL Server estates have both; this component and `ssis_workspace` are
siblings, not alternatives.

Verified shape, mirroring `ssis_workspace`'s own conventions in this
registry (StateBackedComponent caching, `can_subset` multi-asset,
`assets_by_*` overrides, `demo_mode`):

    SELECT s.name, p.name FROM sys.procedures p
      JOIN sys.schemas s ON p.schema_id = s.schema_id      -- discovery
    EXEC [schema].[procedure] @Param = :value               -- execute (parameterized)
    SELECT ... FROM msdb.dbo.sysjobhistory jh
      JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id        -- observe SQL-Agent-triggered runs
      WHERE j.name = :job_name AND jh.step_id = 0

A stored procedure has no catalog of its own run history the way SSISDB
does -- the only way to see a run SQL Agent (not Dagster) triggered is via
the job that wraps it, hence `sql_agent_job_name` on `assets_by_procedure`:
without it, a procedure's polling-sensor coverage is simply skipped (no
error -- most procedures aren't wrapped in a SQL Agent job at all).
"""
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


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


try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = dg.Component
    _HAS_STATE_BACKED = False


def _topological_order(specs: List[dg.AssetSpec], selected_keys) -> List[dg.AssetSpec]:
    """Order `specs` (already filtered to the selected subset) so a procedure
    depending on another SELECTED procedure (via assets_by_procedure's deps:)
    always runs after it. Deps pointing outside the selected subset are left
    to Dagster's own execution ordering -- this only orders execution WITHIN
    one multi_asset invocation. Identical helper to ssis_workspace's own
    (duplicated, not imported -- components in this registry are
    self-contained, see tests/test_self_contained.py)."""
    by_key = {s.key: s for s in specs}
    order: List[dg.AssetSpec] = []
    visited: set = set()
    in_progress: set = set()

    def visit(spec: dg.AssetSpec) -> None:
        if spec.key in visited:
            return
        if spec.key in in_progress:
            raise ValueError(
                f"circular dependency detected among selected procedures involving "
                f"{spec.key.to_user_string()!r} -- check assets_by_procedure deps: for a cycle."
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


class SqlServerConnectionConfig(dg.Model):
    """Connection to the SQL Server instance. Supply EITHER
    `connection_string_env_var` (opaque SQLAlchemy URL) OR the flat fields --
    not both. Flat fields build a pyodbc URL."""

    connection_string_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing a full SQLAlchemy URL. Takes precedence over the flat fields. "
            "Example value: `mssql+pyodbc://svc:pw@sql-01/Canvas?driver=ODBC+Driver+18+for+SQL+Server`."
        ),
    )
    server: Optional[str] = Field(default=None, description="SQL Server host / instance (e.g. `sql-01.corp` or `sql-01\\INST1`).")
    database: Optional[str] = Field(default=None, description="Database hosting the stored procedures.")
    user: Optional[str] = Field(default=None, description="SQL login username (basic auth).")
    password: Optional[str] = Field(default=None, description="SQL login password (basic auth).")
    driver: str = Field(default="ODBC Driver 18 for SQL Server", description="ODBC driver name for the mssql+pyodbc dialect.")
    trust_server_certificate: bool = Field(
        default=False,
        description="Passes `TrustServerCertificate=yes` on the connection -- needed for lab SQL Servers with self-signed TLS.",
    )


class SqlServerProcedureSelector(dg.Model):
    """Filter which stored procedures become Dagster assets. Omit for every procedure in every schema."""

    include_schemas: Optional[List[str]] = Field(default=None, description="Only import procedures from these schemas. Omit for all schemas.")
    exclude_schemas: Optional[List[str]] = Field(default=None, description="Never import procedures from these schemas (e.g. ['sys']).")
    include: Optional[List[str]] = Field(default=None, description="fnmatch pattern(s) a procedure name must match (e.g. ['Load*', 'Sync*']).")
    exclude: Optional[List[str]] = Field(default=None, description="fnmatch pattern(s) that veto a procedure regardless of include.")


class SqlServerStoredProcedureWorkspaceComponent(StateBackedComponent, dg.Model, dg.Resolvable):
    """Plain T-SQL stored procedures on SQL Server as Dagster assets.

    Example:
        ```yaml
        type: dagster_community_components.SqlServerStoredProcedureWorkspaceComponent
        attributes:
          connection:
            server: "{{ env.SQL_SERVER }}"
            database: Canvas
            user: "{{ env.SQL_USER }}"
            password: "{{ env.SQL_PASSWORD }}"
          action: execute
          assets_by_procedure:
            dbo.LoadSilverClaims:
              key: silver/claims
              params:
                BusinessDate: "{partition_key}"
              sql_agent_job_name: "Nightly - Load Silver Claims"
            dbo.LoadGoldClaims:
              key: gold/claims
              deps: [silver/claims]
          polling_sensor: true
          partition_type: daily
          partition_start: "2024-01-01"
        ```
    """

    connection: SqlServerConnectionConfig = Field(description="Connection to the SQL Server instance.")
    demo_mode: bool = Field(
        default=False,
        description=(
            "Skip the real SQL Server connection entirely. Discovery returns a small synthetic "
            "procedure catalog (2 procedures); execute, the polling sensor, all log a simulated "
            "lifecycle instead of running real T-SQL. Use to smoke-test the whole shape -- "
            "can_subset, assets_by_procedure, the sensor -- with no real SQL Server available."
        ),
    )
    procedure_selector: Optional[SqlServerProcedureSelector] = Field(default=None, description="Filter which procedures become assets. Omit for every procedure.")
    assets_by_procedure: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-procedure overrides keyed by qualified name ('<schema>.<procedure>'). Mirrors "
            "the official dagster-databricks DatabricksWorkspaceComponent.assets_by_task_key "
            "shape. Per-procedure keys (all optional): key, group_name, description, deps "
            "(merged), metadata (merged), tags (merged), kinds (replaces), owners (replaces), "
            "params (dict of EXEC parameter name -> value; `{partition_key}`/`{run_id}` are "
            "substituted at execute time), sql_agent_job_name (the SQL Agent job wrapping this "
            "procedure, if any -- required for polling_sensor coverage of this procedure; "
            "procedures without it are simply skipped by the sensor, not an error)."
        ),
    )
    action: str = Field(
        default="noop",
        description=(
            "noop (default) = external asset, declare-only, materialize does nothing. execute = "
            "materialize runs EXEC [schema].[procedure] (with assets_by_procedure.params bound as "
            "parameters, never string-interpolated). Subsettable (can_subset=true): selecting one "
            "procedure runs only that procedure, in assets_by_procedure dependency order, not "
            "every discovered procedure."
        ),
    )
    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds ONE sensor (not one per procedure) that polls msdb.dbo.sysjobhistory "
            "for completions of each procedure's sql_agent_job_name (set via assets_by_procedure) "
            "and emits AssetObservation for runs Dagster did not itself trigger -- how a SQL-Agent"
            "-triggered run shows up in the same asset graph. Procedures with no sql_agent_job_name "
            "set are skipped (not every stored procedure is wrapped in a SQL Agent job)."
        ),
    )
    observation_interval_seconds: int = Field(default=300, description="Seconds between sensor polls. Only used when polling_sensor=True.")
    group_name: Optional[str] = Field(default="sql_server", description="Dagster asset group.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds badge. Default: ['sqlserver', 'tsql'].")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional asset tags.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners (team names / email addresses).")
    asset_key_prefix: Optional[List[str]] = Field(default=None, description="Prefix for every emitted asset key. Default: ['sqlserver', <server-host>].")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'cron'|'static'|'dynamic', or None for unpartitioned. Applies the SAME partitions_def to every imported procedure.")
    partition_start: Optional[str] = Field(default=None, description="ISO start date for time-based partition types (e.g. '2024-01-01').")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for partition_type='static'.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').")

    defs_state: ResolvedDefsStateConfig = Field(default_factory=DefsStateConfigArgs.local_filesystem)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("tags",))

    @property
    def defs_state_config(self) -> "DefsStateConfig":
        return DefsStateConfig.from_args(self.defs_state, default_key=f"SqlServerStoredProcedureWorkspaceComponent[{self.connection.server or 'conn'}]")

    def _build_engine(self):
        """Return a SQLAlchemy engine. Every real caller already guards on
        demo_mode before reaching here -- defensive backstop, not the primary
        demo_mode seam, same shape as ssis_workspace's own."""
        if self.demo_mode:
            raise RuntimeError(
                "_build_engine() called while demo_mode=true -- this is a bug in the component "
                "(a real SQL path was reached that should have branched to a simulated one first)."
            )
        from sqlalchemy import create_engine
        import os

        cfg = self.connection
        if cfg.connection_string_env_var:
            url = os.environ.get(cfg.connection_string_env_var, "")
            if not url:
                raise ValueError(f"env var {cfg.connection_string_env_var!r} is empty or unset")
            return create_engine(url)
        if not (cfg.server and cfg.user and cfg.password):
            raise ValueError("connection: supply either connection_string_env_var OR the {server, user, password} flat fields.")
        from urllib.parse import quote_plus

        driver = quote_plus(cfg.driver)
        params = f"driver={driver}"
        if cfg.trust_server_certificate:
            params += "&TrustServerCertificate=yes"
        return create_engine(f"mssql+pyodbc://{cfg.user}:{quote_plus(cfg.password)}@{cfg.server}/{cfg.database}?{params}")

    def _demo_procedures(self) -> List[Dict[str, Any]]:
        return [
            {"schema_name": "dbo", "procedure_name": "LoadSilverClaims"},
            {"schema_name": "dbo", "procedure_name": "LoadGoldClaims"},
        ]

    def _apply_selector(self, procedures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sel = self.procedure_selector
        if not sel:
            return procedures
        out = []
        for p in procedures:
            schema, name = p["schema_name"], p["procedure_name"]
            if sel.include_schemas and schema not in sel.include_schemas:
                continue
            if sel.exclude_schemas and schema in sel.exclude_schemas:
                continue
            if sel.exclude and any(re_fnmatch(name, pat) for pat in sel.exclude):
                continue
            if sel.include and not any(re_fnmatch(name, pat) for pat in sel.include):
                continue
            out.append(p)
        return out

    def _discover_procedures(self, engine) -> List[Dict[str, Any]]:
        if self.demo_mode:
            return self._apply_selector(self._demo_procedures())
        from sqlalchemy import text

        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT s.name AS schema_name, p.name AS procedure_name "
                "FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id "
                "ORDER BY s.name, p.name"
            )).mappings().all()
        return self._apply_selector([dict(r) for r in rows])

    async def write_state_to_path(self, state_path: Path) -> None:
        if self.demo_mode:
            procedures = self._discover_procedures(None)
        else:
            try:
                engine = self._build_engine()
                procedures = self._discover_procedures(engine)
            except Exception:
                procedures = []
        import json

        state_path.write_text(json.dumps({"server_host": self.connection.server or "", "procedures": procedures}))

    def _apply_asset_overrides(self, qualified_name: str, base_kwargs: dict) -> dict:
        """Merge per-procedure overrides from `assets_by_procedure`. Mirrors
        snowflake_workspace / ssis_workspace's own `_apply_asset_overrides` exactly."""
        if not self.assets_by_procedure:
            return base_kwargs
        override = self.assets_by_procedure.get(qualified_name)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result.pop("key", None)
            result["key"] = dg.AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(dg.AssetKey.from_user_string(d) for d in override["deps"])
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

    def _get_asset_spec(self, proc: Dict[str, Any], partitions_def) -> dg.AssetSpec:
        qualified_name = f"{proc['schema_name']}.{proc['procedure_name']}"
        prefix = self.asset_key_prefix or ["sqlserver", self.connection.server or "demo"]
        base_kwargs: Dict[str, Any] = dict(
            key=dg.AssetKey([*prefix, proc["schema_name"], proc["procedure_name"]]),
            group_name=self.group_name,
            description=f"SQL Server stored procedure {qualified_name}",
            deps=[],
            metadata={"sqlserver/schema": proc["schema_name"], "sqlserver/procedure": proc["procedure_name"]},
            tags=dict(self.tags or {}) if isinstance(self.tags, dict) else {},
            owners=self.owners,
            kinds=set(self.kinds) if self.kinds else {"sqlserver", "tsql"},
            partitions_def=partitions_def,
        )
        return dg.AssetSpec(**self._apply_asset_overrides(qualified_name, base_kwargs))

    def _execute_procedure_demo(self, qualified_name: str, context) -> Dict[str, Any]:
        import random

        rng = random.Random(qualified_name)
        context.log.info(f"[SQL SERVER DEMO] EXEC {qualified_name} -> Succeeded")
        return {"status": "Succeeded", "rows_affected": rng.randint(10, 5000)}

    def _execute_procedure(self, engine, proc: Dict[str, Any], params: Dict[str, Any], context) -> Dict[str, Any]:
        if self.demo_mode:
            return self._execute_procedure_demo(f"{proc['schema_name']}.{proc['procedure_name']}", context)
        from sqlalchemy import text

        qualified_name = f"[{proc['schema_name']}].[{proc['procedure_name']}]"
        param_sql = ", ".join(f"@{name} = :{name}" for name in params)
        exec_sql = f"EXEC {qualified_name}" + (f" {param_sql}" if param_sql else "")
        with engine.connect() as conn:
            conn.execute(text(exec_sql), params)
            conn.commit()
        return {"status": "Succeeded", "rows_affected": None}

    def build_defs_from_state(self, context: dg.ComponentLoadContext, state_path: Optional[Path]) -> dg.Definitions:
        if state_path is None or not state_path.exists():
            return dg.Definitions()
        import json

        state = json.loads(state_path.read_text())
        procedures = state.get("procedures", [])
        server_host = state.get("server_host", "")
        if not procedures:
            return dg.Definitions()

        partitions_def = _build_sql_server_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_cron_schedule, self.partition_cron_fmt or "%Y-%m-%d",
        )
        specs = [self._get_asset_spec(p, partitions_def) for p in procedures]
        procs_by_qualified_name = {f"{p['schema_name']}.{p['procedure_name']}": p for p in procedures}
        specs_by_qualified_name = {f"{p['schema_name']}.{p['procedure_name']}": s for p, s in zip(procedures, specs)}

        action = self.action
        if action == "noop":
            @dg.multi_asset(specs=specs, name="sql_server_procedures")
            def _noop(context: dg.AssetExecutionContext):
                for spec in specs:
                    yield dg.MaterializeResult(asset_key=spec.key)

            assets = [_noop]
        elif action == "execute":
            _self = self

            @dg.multi_asset(specs=specs, can_subset=True, name="sql_server_procedures")
            def _execute(context: dg.AssetExecutionContext):
                exec_engine = None if _self.demo_mode else _self._build_engine()
                selected = context.selected_asset_keys
                selected_specs = [s for s in specs if s.key in selected]
                key_to_qualified = {s.key: qn for qn, s in specs_by_qualified_name.items()}
                for spec in _topological_order(selected_specs, selected):
                    qualified_name = key_to_qualified[spec.key]
                    proc = procs_by_qualified_name[qualified_name]
                    override = (_self.assets_by_procedure or {}).get(qualified_name) or {}
                    raw_params = dict(override.get("params") or {})
                    resolved_params = {
                        k: (str(v).replace("{partition_key}", str(context.partition_key) if context.has_partition_key else "")
                               .replace("{run_id}", context.run.run_id))
                        for k, v in raw_params.items()
                    }
                    result = _self._execute_procedure(exec_engine, proc, resolved_params, context)
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={"sqlserver/status": result["status"], "sqlserver/rows_affected": result.get("rows_affected")},
                    )

            assets = [_execute]
        else:
            raise ValueError(f"SqlServerStoredProcedureWorkspaceComponent.action={action!r} not supported. Use 'noop' or 'execute'.")

        sensors: List[Any] = []
        if self.polling_sensor:
            sensors.append(self._build_observation_sensor(specs_by_qualified_name))

        return dg.Definitions(assets=assets, sensors=sensors)

    def _build_observation_sensor(self, specs_by_qualified_name: Dict[str, dg.AssetSpec]):
        _self = self
        watched = {
            qn: (self.assets_by_procedure or {}).get(qn, {}).get("sql_agent_job_name")
            for qn in specs_by_qualified_name
        }
        watched = {qn: job for qn, job in watched.items() if job}
        asset_keys = [specs_by_qualified_name[qn].key for qn in watched]

        @dg.sensor(
            name="sql_server_stored_procedure_sql_agent_observer",
            minimum_interval_seconds=self.observation_interval_seconds,
            asset_selection=dg.AssetSelection.assets(*asset_keys) if asset_keys else dg.AssetSelection.assets(),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            import json as _json

            if not watched:
                return dg.SensorResult(skip_reason="no procedure has an assets_by_procedure.sql_agent_job_name set")

            last_seen: Dict[str, int] = _json.loads(context.cursor) if context.cursor else {}
            new_seen = dict(last_seen)
            events = []

            if _self.demo_mode:
                import random

                tick = int(last_seen.get("__tick", 0)) + 1
                for qn, job_name in watched.items():
                    instance_id = tick * 1000 + abs(hash(qn)) % 1000
                    key_str = f"{qn}::{job_name}"
                    if last_seen.get(key_str) == instance_id:
                        continue
                    new_seen[key_str] = instance_id
                    events.append(dg.AssetObservation(
                        asset_key=specs_by_qualified_name[qn].key,
                        metadata={"sqlserver/sql_agent_job": job_name, "sqlserver/instance_id": instance_id},
                    ))
                new_seen["__tick"] = tick
                cursor = _json.dumps(new_seen)
                return dg.SensorResult(asset_events=events, cursor=cursor) if events else dg.SensorResult(cursor=cursor, skip_reason="no new demo run")

            from sqlalchemy import text

            engine = _self._build_engine()
            with engine.connect() as conn:
                for qn, job_name in watched.items():
                    key_str = f"{qn}::{job_name}"
                    last_instance_id = last_seen.get(key_str, 0)
                    try:
                        rows = conn.execute(
                            text(
                                "SELECT TOP 5 jh.instance_id, jh.run_status "
                                "FROM msdb.dbo.sysjobhistory jh "
                                "JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id "
                                "WHERE j.name = :job_name AND jh.step_id = 0 AND jh.instance_id > :last_id "
                                "ORDER BY jh.instance_id DESC"
                            ),
                            {"job_name": job_name, "last_id": last_instance_id},
                        ).mappings().all()
                    except Exception as e:
                        context.log.error(f"sysjobhistory query failed for job {job_name!r}: {e}")
                        continue
                    for row in rows:
                        new_seen[key_str] = max(new_seen.get(key_str, 0), row["instance_id"])
                        if row["run_status"] != 1:  # 1 = Succeeded
                            continue
                        events.append(dg.AssetObservation(
                            asset_key=specs_by_qualified_name[qn].key,
                            metadata={"sqlserver/sql_agent_job": job_name, "sqlserver/instance_id": row["instance_id"]},
                        ))

            cursor = _json.dumps(new_seen)
            if events:
                return dg.SensorResult(asset_events=events, cursor=cursor)
            return dg.SensorResult(cursor=cursor, skip_reason="no new SQL Agent job completions since last tick")

        return _observation_sensor


def re_fnmatch(name: str, pattern: str) -> bool:
    import fnmatch

    return fnmatch.fnmatch(name, pattern)


def _build_sql_server_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name, cron_schedule, cron_fmt):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition, StaticPartitionsDefinition, DynamicPartitionsDefinition,
        TimeWindowPartitionsDefinition,
    )

    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires partition_cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
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
