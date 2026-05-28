"""Snowflake Workspace Component.

Import Snowflake tasks, stored procedures, dynamic tables, and streams
as Dagster assets with automatic observation and orchestration.
"""

import json
import logging
import re
from typing import Optional, List, Dict, Any
from datetime import datetime

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from dagster import (
    AssetKey,
    AssetSpec,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    observable_source_asset,
    sensor,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    AssetMaterialization,
    DataVersion,
    MaterializeResult,
    ObserveResult,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import ConfigDict, Field

_logger = logging.getLogger(__name__)


_CONFIG_SCHEMA_TYPE_MAP = {
    "int": int,
    "str": str,
    "string": str,
    "float": float,
    "bool": bool,
    "boolean": bool,
}


def _build_task_config_class(task_name: str, schema: Dict[str, Any]):
    """Build a ``dg.Config`` subclass from a YAML config_schema dict.

    Each field is a dict of ``{type, default, description}``. The supported
    type strings are int / str / float / bool — the minimum-viable set for
    the `EXECUTE TASK ... USING CONFIG = '<json>'` pattern. Anything else
    raises ValueError at component build_defs time (loudly, not silently).

    Defaults are required when the YAML supplies a `default:` key, optional
    otherwise — in which case the field is required at launchpad time.
    """
    from pydantic import create_model
    import dagster as _dg

    sane_name = re.sub(r"[^A-Za-z0-9]+", "_", task_name).strip("_") or "Task"
    fields: Dict[str, Any] = {}
    for field_name, spec in schema.items():
        if not isinstance(spec, dict):
            raise ValueError(
                f"config_schema['{field_name}'] must be a dict with "
                f"`type:` (int/str/float/bool); got {type(spec).__name__}."
            )
        type_str = str(spec.get("type", "")).lower()
        if type_str not in _CONFIG_SCHEMA_TYPE_MAP:
            raise ValueError(
                f"config_schema['{field_name}'].type must be one of "
                f"{sorted(_CONFIG_SCHEMA_TYPE_MAP)}; got {spec.get('type')!r}."
            )
        py_type = _CONFIG_SCHEMA_TYPE_MAP[type_str]
        if "default" in spec:
            fields[field_name] = (
                py_type,
                Field(default=spec["default"], description=spec.get("description")),
            )
        else:
            fields[field_name] = (
                py_type,
                Field(..., description=spec.get("description")),
            )
    return create_model(f"{sane_name}Config", __base__=_dg.Config, **fields)


def _serialize_proc_arg(a):
    """Convert a Python value to a SQL literal for CALL <proc>(args...).

    NULL / TRUE / FALSE / numbers inlined as-is; strings single-quoted with
    embedded-quote escaping. Used by the stored-procedure asset path so
    customers can wire literal args (or per-instance args under
    ``assets_by_name.<proc>.instances[].args``) without writing SQL.
    """
    if a is None:
        return "NULL"
    if isinstance(a, bool):
        return "TRUE" if a else "FALSE"
    if isinstance(a, (int, float)):
        return str(a)
    return "'" + str(a).replace("'", "''") + "'"


def _emit_query_perf(cursor, query_id) -> dict:
    """Return Dagster MetadataValue.int/float fields from QUERY_HISTORY for a
    Snowflake query, keyed by a stable ``snowflake/*`` namespace.

    Dagster's catalog auto-plots ``MetadataValue.int`` / ``MetadataValue.float``
    fields over time on each asset's ``Plots`` tab — so wiring this after
    every ``cursor.execute()`` turns each materialization into a per-run
    perf trace (duration, rows produced, bytes scanned, credits used,
    partition pruning ratio, spill bytes).

    ``cursor.sfqid`` exposed by snowflake-connector returns the most-recent
    query id; pass that as ``query_id``. Returns an empty dict on any
    failure so callers can blindly ``metadata.update(...)`` without
    risking the materialization itself.
    """
    if not query_id:
        return {}
    try:
        cursor.execute(
            "SELECT total_elapsed_time, rows_produced, bytes_scanned, "
            "       bytes_spilled_to_local_storage, "
            "       credits_used_cloud_services, "
            "       partitions_scanned, partitions_total "
            f"FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_QUERY_ID('{query_id}'))"
        )
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "snowflake/query_duration_ms":   MetadataValue.int(int(row[0] or 0)),
            "snowflake/rows_produced":       MetadataValue.int(int(row[1] or 0)),
            "snowflake/bytes_scanned":       MetadataValue.int(int(row[2] or 0)),
            "snowflake/bytes_spilled_local": MetadataValue.int(int(row[3] or 0)),
            "snowflake/credits_used":        MetadataValue.float(float(row[4] or 0.0)),
            "snowflake/partitions_scanned":  MetadataValue.int(int(row[5] or 0)),
            "snowflake/partitions_total":    MetadataValue.int(int(row[6] or 0)),
        }
    except Exception:
        return {}


class SnowflakeWorkspaceComponent(Component, Model, Resolvable):
    """Component for importing Snowflake workspace entities as Dagster assets.

    Supports importing:
    - Tasks (scheduled SQL/stored procedure executions)
    - Stored Procedures (callable routines)
    - Dynamic Tables (materialized views with automatic refresh)
    - Materialized Views (traditional MVs with manual refresh)
    - Streams (change data capture)
    - Snowpipe (continuous data ingestion pipes)
    - Stages (internal/external file stages)
    - External Tables (monitor external data sources)
    - Alerts (Snowflake native alerts on conditions)
    - OpenFlow Flows (data integration flows via Apache NiFi)

    Example:
        ```yaml
        type: dagster_component_templates.SnowflakeWorkspaceComponent
        attributes:
          account: xy12345.us-east-1
          user: dagster_user
          password: "{{ env('SNOWFLAKE_PASSWORD') }}"
          warehouse: COMPUTE_WH
          database: ANALYTICS
          schema: PUBLIC
          import_tasks: true
          import_dynamic_tables: true
        ```
    """

    # Internal field is `schema_name` (avoids shadowing BaseModel.schema()).
    # YAML still accepts the Snowflake-native `schema:` key via the alias +
    # populate_by_name. Both names work; emit either in defs.yaml.
    model_config = ConfigDict(populate_by_name=True)

    account: str = Field(
        description="Snowflake account identifier (e.g., xy12345.us-east-1)"
    )

    user: str = Field(
        description="Snowflake username"
    )

    # ── Auth: pick ONE of password / authenticator(+keypair) / token ──
    # Pure-Snowflake shops with password auth disabled (most enterprise
    # accounts in 2025+) must use SSO (`authenticator: externalbrowser`)
    # or keypair (`authenticator: SNOWFLAKE_JWT` + `private_key_file`).
    password: Optional[str] = Field(
        default=None,
        description="Snowflake password. Leave unset if using SSO or keypair."
    )

    authenticator: Optional[str] = Field(
        default=None,
        description=(
            "Snowflake authenticator. Common values: 'SNOWFLAKE_JWT' (keypair, "
            "recommended for headless / production), 'externalbrowser' (SSO, "
            "good for local dev — pops a browser to auth), 'oauth', "
            "'PROGRAMMATIC_ACCESS_TOKEN'. Leave unset if using password."
        ),
    )

    private_key_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to a PEM-formatted RSA private key file. Used when "
            "`authenticator: SNOWFLAKE_JWT`. Pair with `private_key_file_pwd` "
            "if the key is encrypted."
        ),
    )

    private_key_file_pwd: Optional[str] = Field(
        default=None,
        description="Passphrase for the encrypted private_key_file (omit if unencrypted).",
    )

    token: Optional[str] = Field(
        default=None,
        description="Auth token (PAT / OAuth). Used with `authenticator: oauth` or PAT.",
    )

    warehouse: str = Field(
        description="Snowflake warehouse to use for queries"
    )

    database: str = Field(
        description="Snowflake database to connect to"
    )

    schema_name: str = Field(
        default="PUBLIC",
        alias="schema",
        description="Snowflake schema to use"
    )

    role: Optional[str] = Field(
        default=None,
        description="Snowflake role to use (optional)"
    )

    import_tasks: bool = Field(
        default=True,
        description="Import Snowflake tasks as materializable assets"
    )

    import_stored_procedures: bool = Field(
        default=False,
        description="Import stored procedures as materializable assets"
    )

    import_dynamic_tables: bool = Field(
        default=False,
        description="Import dynamic tables as observable assets"
    )

    dt_modeling: str = Field(
        default="external",
        description=(
            "How imported dynamic tables are emitted: 'external' (default; "
            "AssetSpec — no manual refresh from Dagster, sensor owns "
            "materialization events) or 'asset' (legacy @asset with manual "
            "ALTER ... REFRESH — sensor still runs, so manual materializations "
            "and auto-refreshes can both emit materialization events for the "
            "same DT)."
        ),
    )

    dt_refresh_sensor_interval_seconds: int = Field(
        default=60,
        description=(
            "Polling interval (seconds) for the DT-refresh detection sensor. "
            "The sensor runs whenever import_dynamic_tables=True regardless of "
            "dt_modeling, so Snowflake's TARGET_LAG-driven auto-refreshes "
            "propagate to downstream Dagster consumers."
        ),
    )

    import_streams: bool = Field(
        default=False,
        description="Import streams as observable assets"
    )

    import_snowpipes: bool = Field(
        default=False,
        description="Import Snowpipe continuous ingestion pipes as materializable assets"
    )

    import_stages: bool = Field(
        default=False,
        description="Import internal and external stages as observable assets"
    )

    import_materialized_views: bool = Field(
        default=False,
        description="Import materialized views as materializable assets (trigger refresh)"
    )

    import_external_tables: bool = Field(
        default=False,
        description="Import external tables as materializable assets (trigger refresh)"
    )

    import_alerts: bool = Field(
        default=False,
        description="Import Snowflake alerts as observable assets (monitor alert status)"
    )

    import_openflow_flows: bool = Field(
        default=False,
        description="Import OpenFlow data integration flows as observable assets (monitor via telemetry)"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    task_filter_by_state: Optional[str] = Field(
        default=None,
        description="Filter tasks by state (STARTED, SUSPENDED). If not specified, imports all tasks."
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="How often (in seconds) the sensor should check for completed runs"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Create a sensor to observe task runs and dynamic table refreshes"
    )

    group_name: Optional[str] = Field(
        default="snowflake",
        description="Group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Snowflake workspace component"
    )

    def _create_connection(self) -> SnowflakeConnection:
        """Create and return a Snowflake connection.

        Auth methods (in priority order — first matching wins):
          1. authenticator='SNOWFLAKE_JWT' + private_key_file (keypair)
          2. authenticator='externalbrowser' (SSO — pops a browser)
          3. authenticator + token (PAT / OAuth)
          4. password (only if account allows it)
        """
        conn_params = {
            'account': self.account,
            'user': self.user,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema_name,
        }

        if self.role:
            conn_params['role'] = self.role

        if self.authenticator:
            conn_params['authenticator'] = self.authenticator
            if self.private_key_file:
                conn_params['private_key_file'] = self.private_key_file
                if self.private_key_file_pwd:
                    conn_params['private_key_file_pwd'] = self.private_key_file_pwd
            elif self.token:
                conn_params['token'] = self.token
            # externalbrowser needs neither — connector pops a browser
        elif self.password:
            conn_params['password'] = self.password
        else:
            raise ValueError(
                "snowflake_workspace: must set either `password`, OR "
                "`authenticator` (+ `private_key_file` for SNOWFLAKE_JWT, or "
                "+ `token` for oauth/PAT, or none for externalbrowser SSO)."
            )

        return snowflake.connector.connect(**conn_params)

    def _should_include_entity(self, name: str) -> bool:
        """Check if an entity should be included based on filters."""
        # Check name exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name, re.IGNORECASE):
                return False

        # Check name inclusion pattern
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name, re.IGNORECASE):
                return False

        return True

    def _execute_query(self, conn: SnowflakeConnection, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            # Uppercase column names — lets the rest of the code use dict['NAME']
            # whether the source is INFORMATION_SCHEMA (uppercase) or SHOW (lowercase).
            columns = [col[0].upper() for col in cursor.description] if cursor.description else []
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            return results
        finally:
            cursor.close()

    def _apply_asset_overrides(self, entity_name: str, base_kwargs: dict) -> dict:
        """Merge per-entity overrides from `assets_by_name` into @asset kwargs.

        Mirrors the official DatabricksWorkspaceComponent.assets_by_task_key
        pattern. Keyed by the Snowflake entity name (task / dynamic table /
        stored procedure / etc.). Each override is itself a dict; supported
        keys: key, group_name, description, deps, metadata, tags, kinds, owners.

        - `key` replaces `name=<asset_key>` with `key=AssetKey(...)` (slash-separated
          paths become AssetKey prefixes).
        - `deps` is merged with any deps already set on the base.
        - `metadata` and `tags` are dict-merged (override wins on conflicts).
        - `group_name` / `description` / `kinds` / `owners` straight-replace.
        """
        if not self.assets_by_name:
            return base_kwargs
        override = self.assets_by_name.get(entity_name)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result.pop("name", None)
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

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    assets_by_name: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the AssetSpec for specific imported entities (Tasks, "
            "Dynamic Tables, Stored Procedures, Streams, Snowpipes, etc.) "
            "by their Snowflake name. Mirrors the official dagster-databricks "
            "`DatabricksWorkspaceComponent.assets_by_task_key` shape.\n\n"
            "Per-entity keys (all optional):\n"
            "  key:          renames the Dagster asset key (slash-separated → AssetKey)\n"
            "  group_name:   overrides the group\n"
            "  description:  overrides the description\n"
            "  deps:         list of upstream asset keys (slash-separated). "
            "                Merged with whatever the component infers.\n"
            "  metadata:     dict merged into the auto-emitted metadata\n"
            "  tags:         dict merged into asset tags\n"
            "  kinds:        list of kind strings (overrides inference)\n"
            "  owners:       list of owners\n\n"
            "Example — wire cross-engine lineage from the Snowflake side:\n"
            "  assets_by_name:\n"
            "    CUSTOMER_METRICS:\n"
            "      key: silver/customer_metrics\n"
            "      group_name: silver\n"
            "      deps: [raw/orders, raw/customers]\n"
            "      metadata:\n"
            "        domain: analytics\n"
        ),
    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from Snowflake workspace entities."""
        conn = self._create_connection()

        assets_list = []
        sensors_list = []

        # Track task and snowpipe metadata for the legacy observation sensor.
        # Dynamic-table refreshes are owned by the dedicated DT-refresh sensor
        # below (wired in unconditionally when import_dynamic_tables=True).
        task_metadata = {}
        snowpipe_metadata = {}

        # Collected during the DT import loop and consumed by the dedicated
        # DT-refresh sensor below. Hoisted out of the try-block so the sensor
        # block can read them even if an earlier import path raised.
        dt_asset_keys: list = []
        dt_name_to_asset_key: Dict[str, str] = {}

        try:
            # Import Tasks
            if self.import_tasks:
                try:
                    # INFORMATION_SCHEMA.TASKS doesn't exist as a view in Snowflake;
                    # SHOW TASKS works universally. _execute_query uppercases
                    # column names so downstream task['NAME'] / ['STATE'] / etc.
                    # continue to work.
                    query = f"SHOW TASKS IN SCHEMA {self.database}.{self.schema_name}"
                    tasks = self._execute_query(conn, query)
                    if self.task_filter_by_state:
                        tasks = [t for t in tasks if t.get('STATE') == self.task_filter_by_state]

                    # Factory pattern: capture loop variables in a closure
                    # WITHOUT using default args. Dagster's @asset decorator
                    # treats non-context function parameters as upstream
                    # asset inputs; default args here caused
                    # DagsterInvalidDefinitionError: Input asset "['task_name']"
                    # Defined once before the per-task loop so both
                    # multi-instance and single-instance paths can call it.
                    #
                    # Two execution-shape variants:
                    #   - No config_schema → @asset(context) with hardcoded
                    #     `config_v` baked into the EXECUTE TASK USING CONFIG
                    #     (current behavior; backwards-compatible).
                    #   - config_schema set → @asset(context, config:
                    #     <generated dg.Config subclass>) so Dagster renders
                    #     the launchpad form. Values resolved at materialize
                    #     time; `config_v` is ignored on this path.
                    def _run_task_body(context, self_v, task_name_v, db_v, schema_v, config_dict):
                        """Shared body: execute the task, gather metadata, return it."""
                        conn = self_v._create_connection()
                        cursor = conn.cursor()
                        try:
                            if config_dict:
                                import json as _json
                                config_json = _json.dumps(config_dict).replace("'", "''")
                                # NB: `USING CONFIG = '<json>'` (equals) — NOT
                                # `WITH CONFIG => '<json>'` (arrow). The arrow
                                # syntax is for table-function args; EXECUTE
                                # TASK uses the SQL clause syntax. Snowflake
                                # compile-errors on the arrow form with
                                # "unexpected 'WITH'".
                                execute_query = (
                                    f"EXECUTE TASK {db_v}.{schema_v}.{task_name_v} "
                                    f"USING CONFIG = '{config_json}'"
                                )
                            else:
                                execute_query = f"EXECUTE TASK {db_v}.{schema_v}.{task_name_v}"
                            exec_sfqid = None
                            try:
                                cursor.execute(execute_query)
                                exec_sfqid = cursor.sfqid
                                context.log.info(
                                    f"Executed Snowflake task: {task_name_v}"
                                    + (f" USING CONFIG = {config_dict}" if config_dict else "")
                                )
                            except Exception as exc:
                                if "non-root task" in str(exc).lower():
                                    context.log.info(
                                        f"{task_name_v} is a child task — skipping direct EXECUTE TASK "
                                        f"(parent triggers it). Run the parent task asset (or wait for "
                                        f"its schedule) to actually exercise this asset."
                                    )
                                else:
                                    raise

                            metadata = {
                                "task_name": task_name_v,
                                "database": db_v,
                                "schema": schema_v,
                                "snowflake_task_config": config_dict or {},
                            }
                            # Per-run numeric perf trace (auto-plots on Plots tab).
                            metadata.update(_emit_query_perf(cursor, exec_sfqid))

                            # SHOW TASKS first — works for least-privilege roles where
                            # INFORMATION_SCHEMA may be invisible. Pulls schedule + state.
                            try:
                                cursor.execute(
                                    f"SHOW TASKS LIKE '{task_name_v}' "
                                    f"IN SCHEMA {db_v}.{schema_v}"
                                )
                                info = cursor.fetchone()
                                if info:
                                    columns = [col[0].lower() for col in cursor.description]
                                    info_dict = dict(zip(columns, info))
                                    metadata.update({
                                        "task_state": info_dict.get("state"),
                                        "task_schedule": info_dict.get("schedule"),
                                        "warehouse": info_dict.get("warehouse"),
                                        "owner": info_dict.get("owner"),
                                    })
                            except Exception as exc:
                                context.log.warning(
                                    f"Could not read task metadata for {task_name_v}: {exc}. "
                                    f"Execute succeeded; emitting asset without enriched metadata."
                                )

                            # TASK_HISTORY is a table function (not a view) — Snowflake
                            # serves it via the ACCOUNT_USAGE / INFORMATION_SCHEMA function
                            # surface. Wrap in try/except so EXECUTE TASK still wins even
                            # if the role can't read history.
                            history_state = None
                            history_return_value = None
                            try:
                                history_query = f"""
                                SELECT
                                    query_id,
                                    state,
                                    scheduled_time,
                                    query_start_time,
                                    completed_time,
                                    return_value,
                                    error_message
                                FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                    TASK_NAME => '{task_name_v}',
                                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                ))
                                ORDER BY scheduled_time DESC
                                LIMIT 1
                                """
                                cursor.execute(history_query)
                                history = cursor.fetchone()
                                if history:
                                    columns = [col[0] for col in cursor.description]
                                    history_dict = dict(zip(columns, history))
                                    history_state = history_dict.get('STATE')
                                    history_return_value = history_dict.get('RETURN_VALUE')
                                    metadata.update({
                                        "query_id": history_dict.get('QUERY_ID'),
                                        "state": history_state,
                                        "scheduled_time": str(history_dict.get('SCHEDULED_TIME')) if history_dict.get('SCHEDULED_TIME') else None,
                                        "return_value": history_return_value,
                                    })
                            except Exception as exc:
                                context.log.warning(
                                    f"Could not read TASK_HISTORY for {task_name_v}: {exc}. "
                                    f"Execute succeeded; emitting asset without history metadata."
                                )

                            # Stable signature: state + return_value. Tasks whose
                            # body is idempotent (proc returns same string when input
                            # data hasn't changed — e.g. "Recomputed 0 tiers") get
                            # the same data_version across runs, so downstream
                            # AutomationCondition.eager() treats them as no-ops.
                            signature = f"{history_state}:{history_return_value}"
                            return MaterializeResult(
                                data_version=DataVersion(signature),
                                metadata=metadata,
                            )
                        finally:
                            cursor.close()
                            conn.close()

                    def _make_task_asset(task_name_v, db_v, schema_v, task_kwargs_v, self_v, config_v, config_schema_v=None):
                        if config_schema_v:
                            # Launchpad-overridable path: build a dg.Config
                            # subclass from the YAML config_schema dict.
                            # Dagster's @asset introspects the function
                            # signature; a parameter typed as a dg.Config
                            # subclass triggers automatic form rendering.
                            ConfigClass = _build_task_config_class(task_name_v, config_schema_v)

                            @asset(**task_kwargs_v)
                            def _task_asset(context: AssetExecutionContext, config: ConfigClass):  # type: ignore[valid-type]
                                """Materialize by executing Snowflake task (config from launchpad)."""
                                return _run_task_body(
                                    context, self_v, task_name_v, db_v, schema_v,
                                    config.model_dump(),
                                )
                            return _task_asset

                        # Hardcoded-config (or no-config) path: legacy behavior.
                        @asset(**task_kwargs_v)
                        def _task_asset(context: AssetExecutionContext):
                            """Materialize by executing Snowflake task (with optional config)."""
                            return _run_task_body(
                                context, self_v, task_name_v, db_v, schema_v,
                                config_v or {},
                            )
                        return _task_asset

                    for task in tasks:
                        task_name = task['NAME']

                        if not self._should_include_entity(task_name):
                            continue

                        override = (self.assets_by_name or {}).get(task_name) or {}

                        # Build retry policy once per task (shared across single + multi-instance).
                        _retry_policy = None
                        if self.retry_policy_max_retries is not None:
                            from dagster import Backoff, RetryPolicy
                            _retry_policy = RetryPolicy(
                                max_retries=self.retry_policy_max_retries,
                                delay=self.retry_policy_delay_seconds or 1,
                                backoff=Backoff[self.retry_policy_backoff.upper()],
                            )

                        # Multi-instance mode: one task → N Dagster assets, each
                        # with its own EXECUTE TASK USING CONFIG = '<json>' + optional
                        # key / deps / kinds / tags / owners overrides.
                        if isinstance(override, dict) and override.get("instances"):
                            for inst in override["instances"]:
                                inst_name = inst.get("asset_name") or inst.get("key")
                                if not inst_name:
                                    _logger.warning(
                                        f"{task_name}.instances entry missing "
                                        f"'asset_name' or 'key'; skipping"
                                    )
                                    continue
                                inst_config = inst.get("config", {})
                                inst_config_schema = inst.get("config_schema")
                                if inst_config and inst_config_schema:
                                    raise ValueError(
                                        f"assets_by_name.{task_name}.instances[{inst_name}]: "
                                        f"can't mix hardcoded `config:` with overridable "
                                        f"`config_schema:` on the same asset — pick one. "
                                        f"`config_schema:` exposes a launchpad form so "
                                        f"customers override at materialize time; `config:` "
                                        f"bakes the values in."
                                    )
                                inst_kwargs: dict = dict(
                                    retry_policy=_retry_policy,
                                    name=re.sub(r'[^a-zA-Z0-9_]', '_', inst_name.lower()),
                                    group_name=inst.get("group_name", self.group_name),
                                    description=inst.get(
                                        "description",
                                        f"Snowflake task: {task_name}"
                                        + (f" (config={inst_config})" if inst_config else "")
                                        + (" (launchpad config_schema)" if inst_config_schema else ""),
                                    ),
                                    metadata={
                                        "snowflake_task_name": task_name,
                                        "snowflake_database": task['DATABASE_NAME'],
                                        "snowflake_schema": task['SCHEMA_NAME'],
                                        "snowflake_state": task['STATE'],
                                        "snowflake_schedule": task.get('SCHEDULE'),
                                        "snowflake_task_config": inst_config,
                                        "snowflake_task_config_schema": list((inst_config_schema or {}).keys()),
                                        "entity_type": "task",
                                    },
                                )
                                if "key" in inst:
                                    inst_kwargs.pop("name", None)
                                    inst_kwargs["key"] = AssetKey.from_user_string(str(inst["key"]))
                                if "deps" in inst:
                                    inst_kwargs["deps"] = [
                                        AssetKey.from_user_string(d) for d in inst["deps"]
                                    ]
                                if "kinds" in inst:
                                    inst_kwargs["kinds"] = set(inst["kinds"])
                                if "tags" in inst:
                                    inst_kwargs["tags"] = dict(inst["tags"])
                                if "owners" in inst:
                                    inst_kwargs["owners"] = inst["owners"]
                                # Per-instance sensor metadata bucket so the
                                # observation sensor can match against either
                                # the proc-style asset_key or the explicit key.
                                inst_asset_key_str = inst_kwargs.get("name") or str(inst_kwargs.get("key"))
                                task_metadata[inst_asset_key_str] = {
                                    'task_name': task_name,
                                    'database': task['DATABASE_NAME'],
                                    'schema': task['SCHEMA_NAME'],
                                    'state': task['STATE'],
                                }
                                assets_list.append(_make_task_asset(
                                    task_name, task['DATABASE_NAME'], task['SCHEMA_NAME'],
                                    inst_kwargs, self, inst_config, inst_config_schema,
                                ))
                            continue  # skip single-instance path

                        # Single-instance mode (default): one Dagster asset per
                        # task, with optional `config:` (hardcoded) OR
                        # `config_schema:` (launchpad-overridable) override.
                        task_config = override.get("config", {}) if isinstance(override, dict) else {}
                        task_config_schema = override.get("config_schema") if isinstance(override, dict) else None
                        if task_config and task_config_schema:
                            raise ValueError(
                                f"assets_by_name.{task_name}: can't mix hardcoded "
                                f"`config:` with overridable `config_schema:` on "
                                f"the same asset — pick one. `config_schema:` "
                                f"exposes a launchpad form so customers override at "
                                f"materialize time; `config:` bakes the values in."
                            )
                        asset_key = f"task_{re.sub(r'[^a-zA-Z0-9_]', '_', task_name.lower())}"
                        task_metadata[asset_key] = {
                            'task_name': task_name,
                            'database': task['DATABASE_NAME'],
                            'schema': task['SCHEMA_NAME'],
                            'state': task['STATE'],
                        }
                        _task_kwargs = self._apply_asset_overrides(task_name, dict(
                            retry_policy=_retry_policy,
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake task: {task_name}",
                            metadata={
                                "snowflake_task_name": task_name,
                                "snowflake_database": task['DATABASE_NAME'],
                                "snowflake_schema": task['SCHEMA_NAME'],
                                "snowflake_state": task['STATE'],
                                "snowflake_schedule": task.get('SCHEDULE'),
                                "snowflake_task_config": task_config,
                                "snowflake_task_config_schema": list((task_config_schema or {}).keys()),
                                "entity_type": "task",
                            },
                        ))
                        assets_list.append(_make_task_asset(
                            task_name, task['DATABASE_NAME'], task['SCHEMA_NAME'],
                            _task_kwargs, self, task_config, task_config_schema,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake tasks: {e}")

            # Import Stored Procedures
            if self.import_stored_procedures:
                try:
                    # SHOW PROCEDURES (not INFORMATION_SCHEMA.PROCEDURES) —
                    # SHOW only needs USAGE on the schema + any privilege on
                    # the proc; INFORMATION_SCHEMA can be invisible to
                    # least-privilege roles.
                    query = f"SHOW PROCEDURES IN SCHEMA {self.database}.{self.schema_name}"
                    procedures = self._execute_query(conn, query)

                    # Dedupe overloaded procedure signatures (Snowflake returns
                    # one SHOW row per (name, argument_signature) pair, so a
                    # proc like SYSTEM$SEND_EMAIL with 1-arg / 2-arg / 3-arg
                    # variants shows up 3 times — all hashing to the same
                    # asset_key. Keep the first signature; log the rest.
                    _seen_proc_names: set[str] = set()

                    for proc in procedures:
                        # SHOW PROCEDURES returns NAME (no PROCEDURE_NAME) and
                        # has no CATALOG column — we already know the database
                        # from self.database.
                        proc_name = proc['NAME']

                        # SYSTEM$* procedures are Snowflake-managed (e.g.
                        # SYSTEM$SEND_EMAIL, SYSTEM$WAIT). They're not
                        # user-managed assets and shouldn't be auto-imported
                        # into the workspace asset graph.
                        if proc_name.startswith("SYSTEM$"):
                            continue

                        if proc_name in _seen_proc_names:
                            _logger.info(
                                f"Skipping overloaded signature for procedure "
                                f"{proc_name} (signature={proc.get('ARGUMENTS')!r}) — "
                                f"first signature already imported."
                            )
                            continue
                        _seen_proc_names.add(proc_name)

                        if not self._should_include_entity(proc_name):
                            continue

                        # Factory pattern: capture loop variables in a closure
                        # WITHOUT using default args. Dagster's @asset decorator
                        # treats non-context function parameters as upstream
                        # asset inputs; default args here caused
                        # DagsterInvalidDefinitionError: Input asset "['proc_name']"
                        # Defined here (before the dispatch) so both multi-instance
                        # and single-instance paths can call it.
                        def _make_proc_asset(proc_name_v, db_v, schema_v, proc_kwargs_v, self_v, args_v):
                            @asset(**proc_kwargs_v)
                            def _procedure_asset(context: AssetExecutionContext):
                                """Materialize by calling stored procedure (with optional args)."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                try:
                                    args_sql = ", ".join(
                                        _serialize_proc_arg(a) for a in (args_v or [])
                                    )
                                    call_query = f"CALL {db_v}.{schema_v}.{proc_name_v}({args_sql})"
                                    cursor.execute(call_query)
                                    call_sfqid = cursor.sfqid
                                    result = cursor.fetchone()
                                    context.log.info(
                                        f"Called {proc_name_v}({args_sql}) → {result}"
                                    )
                                    metadata = {
                                        "procedure_name": proc_name_v,
                                        "database": db_v,
                                        "schema": schema_v,
                                        "args": list(args_v or []),
                                        "result": str(result) if result else None,
                                    }
                                    # Per-run numeric perf trace (auto-plots).
                                    metadata.update(_emit_query_perf(cursor, call_sfqid))
                                    return metadata
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _procedure_asset

                        override = (self.assets_by_name or {}).get(proc_name) or {}

                        # Multi-instance mode: one proc → N Dagster assets, each
                        # with its own arg set + (optional) key / deps / kinds /
                        # tags / owners overrides.
                        if isinstance(override, dict) and override.get("instances"):
                            for inst in override["instances"]:
                                inst_name = inst.get("asset_name") or inst.get("key")
                                if not inst_name:
                                    _logger.warning(
                                        f"{proc_name}.instances entry missing "
                                        f"'asset_name' or 'key'; skipping"
                                    )
                                    continue
                                inst_args = inst.get("args", [])
                                inst_kwargs: dict = dict(
                                    name=re.sub(r'[^a-zA-Z0-9_]', '_', inst_name.lower()),
                                    group_name=inst.get("group_name", self.group_name),
                                    description=inst.get(
                                        "description",
                                        f"Snowflake stored procedure: "
                                        f"{proc_name}({', '.join(map(str, inst_args))})",
                                    ),
                                    metadata={
                                        "snowflake_procedure_name": proc_name,
                                        "snowflake_database": self.database,
                                        "snowflake_schema": proc.get("SCHEMA_NAME", self.schema_name),
                                        "snowflake_signature": proc.get("ARGUMENTS"),
                                        "snowflake_call_args": inst_args,
                                        "entity_type": "stored_procedure",
                                    },
                                )
                                if "key" in inst:
                                    inst_kwargs.pop("name", None)
                                    inst_kwargs["key"] = AssetKey.from_user_string(str(inst["key"]))
                                if "deps" in inst:
                                    inst_kwargs["deps"] = [
                                        AssetKey.from_user_string(d) for d in inst["deps"]
                                    ]
                                if "kinds" in inst:
                                    inst_kwargs["kinds"] = set(inst["kinds"])
                                if "tags" in inst:
                                    inst_kwargs["tags"] = dict(inst["tags"])
                                if "owners" in inst:
                                    inst_kwargs["owners"] = inst["owners"]
                                assets_list.append(_make_proc_asset(
                                    proc_name, self.database,
                                    proc.get("SCHEMA_NAME", self.schema_name),
                                    inst_kwargs, self, inst_args,
                                ))
                            continue  # skip the single-instance path

                        # Single-instance mode (default): one Dagster asset per
                        # proc, with optional `args:` override.
                        proc_args = override.get("args", []) if isinstance(override, dict) else []
                        asset_key = f"proc_{re.sub(r'[^a-zA-Z0-9_]', '_', proc_name.lower())}"
                        _proc_kwargs = self._apply_asset_overrides(proc_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stored procedure: {proc_name}",
                            metadata={
                                "snowflake_procedure_name": proc_name,
                                "snowflake_database": self.database,
                                "snowflake_schema": proc.get('SCHEMA_NAME', self.schema_name),
                                "snowflake_signature": proc.get('ARGUMENTS'),
                                "snowflake_call_args": proc_args,
                                "entity_type": "stored_procedure",
                            },
                        ))
                        assets_list.append(_make_proc_asset(
                            proc_name, self.database,
                            proc.get('SCHEMA_NAME', self.schema_name),
                            _proc_kwargs, self, proc_args,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake stored procedures: {e}")

            # Import Dynamic Tables
            # All imported DTs land at asset_key dynamic_table_<lower> regardless
            # of dt_modeling, so downstream `deps:` references stay stable across
            # mode flips. dt_asset_keys + dt_name_to_asset_key are hoisted to
            # the top of build_defs so the dedicated DT-refresh sensor can
            # read them even if an earlier import path raised.
            if self.import_dynamic_tables:
                try:
                    query = f"SHOW DYNAMIC TABLES IN SCHEMA {self.database}.{self.schema_name}"
                    dynamic_tables = self._execute_query(conn, query)

                    # Factory for the legacy @asset (dt_modeling="asset") path.
                    def _make_dynamic_table_asset(dt_name_v, db_v, schema_v, dt_kwargs_v, self_v):
                        @asset(**dt_kwargs_v)
                        def _dynamic_table_asset(context: AssetExecutionContext):
                            """Materialize by triggering dynamic table refresh."""
                            conn = self_v._create_connection()
                            cursor = conn.cursor()
                            try:
                                refresh_query = f"ALTER DYNAMIC TABLE {db_v}.{schema_v}.{dt_name_v} REFRESH"
                                cursor.execute(refresh_query)
                                refresh_sfqid = cursor.sfqid
                                context.log.info(f"Triggered refresh for dynamic table: {dt_name_v}")

                                metadata = {
                                    "table_name": dt_name_v,
                                    "database": db_v,
                                    "schema": schema_v,
                                }
                                metadata.update(_emit_query_perf(cursor, refresh_sfqid))

                                try:
                                    cursor.execute(
                                        f"SHOW DYNAMIC TABLES LIKE '{dt_name_v}' "
                                        f"IN SCHEMA {db_v}.{schema_v}"
                                    )
                                    info = cursor.fetchone()
                                    if info:
                                        columns = [col[0].lower() for col in cursor.description]
                                        info_dict = dict(zip(columns, info))
                                        metadata.update({
                                            "refresh_mode": info_dict.get("refresh_mode"),
                                            "scheduling_state": info_dict.get("scheduling_state"),
                                            "target_lag": info_dict.get("target_lag"),
                                        })
                                        if info_dict.get("rows") is not None:
                                            metadata["snowflake/rows"] = MetadataValue.int(int(info_dict["rows"]))
                                        if info_dict.get("bytes") is not None:
                                            metadata["snowflake/bytes"] = MetadataValue.int(int(info_dict["bytes"]))
                                except Exception as exc:
                                    context.log.warning(
                                        f"Could not read DT metadata for {dt_name_v}: {exc}. "
                                        f"Refresh succeeded; emitting asset without enriched metadata."
                                    )
                                return metadata
                            finally:
                                cursor.close()
                                conn.close()
                        return _dynamic_table_asset

                    for dt in dynamic_tables:
                        dt_name = dt['NAME']

                        if not self._should_include_entity(dt_name):
                            continue

                        # Asset key shape is identical across both modes so
                        # downstream `deps:` references don't break on flip.
                        asset_key = f"dynamic_table_{re.sub(r'[^a-zA-Z0-9_]', '_', dt_name.lower())}"
                        dt_asset_keys.append(asset_key)
                        dt_name_to_asset_key[dt_name] = asset_key

                        base_metadata = {
                            "snowflake_table_name": dt_name,
                            "snowflake_database": dt['DATABASE_NAME'],
                            "snowflake_schema": dt['SCHEMA_NAME'],
                            "snowflake_target_lag": dt.get('TARGET_LAG'),
                            "snowflake_refresh_mode": dt.get('REFRESH_MODE'),
                            "entity_type": "dynamic_table",
                        }

                        if self.dt_modeling == "external":
                            # Declare-only AssetSpec — no compute. Materialization
                            # events come exclusively from the DT-refresh sensor
                            # below, which catches Snowflake's TARGET_LAG-driven
                            # auto-refreshes too.
                            base_metadata["dagster.observability_type"] = "external"
                            assets_list.append(AssetSpec(
                                key=AssetKey([asset_key]),
                                group_name=self.group_name,
                                description=f"Snowflake dynamic table: {dt_name}",
                                kinds={"snowflake", "dynamic_table"},
                                metadata=base_metadata,
                            ))
                        else:
                            # Legacy @asset path: manual REFRESH from Dagster.
                            # NOTE: the DT-refresh sensor still runs, so manual
                            # materializations and auto-refreshes can both emit
                            # materialization events for the same DT (accepted
                            # double-count — see component docstring).
                            _dt_kwargs = self._apply_asset_overrides(dt_name, dict(
                                name=asset_key,
                                group_name=self.group_name,
                                description=f"Snowflake dynamic table: {dt_name}",
                                metadata=base_metadata,
                            ))
                            assets_list.append(_make_dynamic_table_asset(
                                dt_name, dt['DATABASE_NAME'], dt['SCHEMA_NAME'], _dt_kwargs, self,
                            ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake dynamic tables: {e}")

            # Import Streams
            if self.import_streams:
                try:
                    # INFORMATION_SCHEMA.STREAMS isn't a queryable view.
                    query = f"SHOW STREAMS IN SCHEMA {self.database}.{self.schema_name}"

                    streams = self._execute_query(conn, query)

                    for stream in streams:
                        stream_name = stream['NAME']

                        if not self._should_include_entity(stream_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"stream_{re.sub(r'[^a-zA-Z0-9_]', '_', stream_name.lower())}"

                        # Streams are observable (CDC)
                        _stream_kwargs = self._apply_asset_overrides(stream_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stream: {stream_name}",
                            metadata={
                                "snowflake_stream_name": stream_name,
                                "snowflake_database": stream['DATABASE_NAME'],
                                "snowflake_schema": stream['SCHEMA_NAME'],
                                "snowflake_table": stream.get('TABLE_NAME'),
                                "snowflake_type": stream.get('TYPE'),
                                "entity_type": "stream",
                            },
                        ))
                        def _make_stream_asset(stream_name_v, db_v, schema_v, stream_kwargs_v, self_v):
                            @observable_source_asset(**stream_kwargs_v)
                            def _stream_asset(context):
                                """Observable stream asset — emits has_data + pending_rows metrics."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                metadata: dict = {
                                    "stream_name": stream_name_v,
                                    "database": db_v,
                                    "schema": schema_v,
                                }
                                try:
                                    cursor.execute(
                                        f"SELECT SYSTEM$STREAM_HAS_DATA('{db_v}.{schema_v}.{stream_name_v}')"
                                    )
                                    has_data_raw = cursor.fetchone()[0]
                                    # Normalize to 0/1 int (SYSTEM$STREAM_HAS_DATA returns 'true'/'false' string).
                                    has_data_bool = str(has_data_raw).lower() == "true"
                                    metadata["snowflake/has_data"] = MetadataValue.int(1 if has_data_bool else 0)
                                    context.log.info(f"Stream {stream_name_v} has data: {has_data_bool}")

                                    # Plottable pending rows — only query if HAS_DATA to avoid
                                    # an unnecessary full scan on quiet streams.
                                    if has_data_bool:
                                        try:
                                            cursor.execute(
                                                f"SELECT COUNT(*) FROM {db_v}.{schema_v}.{stream_name_v}"
                                            )
                                            pending = cursor.fetchone()[0] or 0
                                            metadata["snowflake/pending_rows"] = MetadataValue.int(int(pending))
                                        except Exception as exc:
                                            context.log.warning(
                                                f"Could not read pending row count for {stream_name_v}: {exc}."
                                            )
                                except Exception as exc:
                                    context.log.warning(
                                        f"Could not observe stream {stream_name_v}: {exc}."
                                    )
                                finally:
                                    cursor.close()
                                    conn.close()
                                # data_version: change-sensitive signature so
                                # downstream AutomationCondition.eager() doesn't
                                # re-fire on every observation tick when the
                                # stream's state hasn't moved.
                                _has = metadata.get("snowflake/has_data")
                                _pending = metadata.get("snowflake/pending_rows")
                                signature = (
                                    f"{getattr(_has, 'value', _has)}:"
                                    f"{getattr(_pending, 'value', _pending)}"
                                )
                                return ObserveResult(
                                    data_version=DataVersion(signature),
                                    metadata=metadata,
                                )
                            return _stream_asset

                        assets_list.append(_make_stream_asset(
                            stream_name, stream['DATABASE_NAME'], stream['SCHEMA_NAME'], _stream_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake streams: {e}")

            # Import Snowpipes
            if self.import_snowpipes:
                try:
                    # INFORMATION_SCHEMA.PIPES uses pipe_name (not name) and is
                    # restrictive about visibility. SHOW PIPES is universal.
                    query = f"SHOW PIPES IN SCHEMA {self.database}.{self.schema_name}"
                    pipes = self._execute_query(conn, query)

                    for pipe in pipes:
                        pipe_name = pipe['NAME']

                        if not self._should_include_entity(pipe_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"snowpipe_{re.sub(r'[^a-zA-Z0-9_]', '_', pipe_name.lower())}"

                        # Parse the COPY target table out of the pipe DEFINITION
                        # so the sensor can query COPY_HISTORY against the right
                        # table. COPY_HISTORY's TABLE_NAME parameter wants the
                        # COPY target — passing pipe_name produces a 002003
                        # "Table … does not exist or not authorized" error
                        # spammed once per pipe per sensor tick.
                        # Target can be 1-/2-/3-part; default missing parts to
                        # the pipe's own database/schema.
                        _defn = pipe.get('DEFINITION') or ''
                        _m = re.search(r'COPY\s+INTO\s+([A-Za-z0-9_."]+)', _defn, re.IGNORECASE)
                        if _m:
                            _parts = [p.strip('"') for p in _m.group(1).split('.')]
                            if len(_parts) == 1:
                                _parts = [pipe['DATABASE_NAME'], pipe['SCHEMA_NAME'], _parts[0]]
                            elif len(_parts) == 2:
                                _parts = [pipe['DATABASE_NAME'], _parts[0], _parts[1]]
                            target_table = '.'.join(_parts)
                        else:
                            target_table = None

                        # Store metadata for sensor
                        snowpipe_metadata[asset_key] = {
                            'pipe_name': pipe_name,
                            'database': pipe['DATABASE_NAME'],
                            'schema': pipe['SCHEMA_NAME'],
                            'target_table': target_table,
                        }

                        # Snowpipes are materializable - can trigger refresh
                        _pipe_kwargs = self._apply_asset_overrides(pipe_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake pipe: {pipe_name}",
                            metadata={
                                "snowflake_pipe_name": pipe_name,
                                "snowflake_database": pipe['DATABASE_NAME'],
                                "snowflake_schema": pipe['SCHEMA_NAME'],
                                "snowflake_notification_channel": pipe.get('NOTIFICATION_CHANNEL'),
                                "entity_type": "snowpipe",
                            },
                        ))
                        def _make_snowpipe_asset(pipe_name_v, db_v, schema_v, target_table_v, pipe_kwargs_v, self_v):
                            @asset(**pipe_kwargs_v)
                            def _snowpipe_asset(context: AssetExecutionContext):
                                """Materialize by refreshing Snowpipe (loading pending files)."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                try:
                                    refresh_query = f"ALTER PIPE {db_v}.{schema_v}.{pipe_name_v} REFRESH"
                                    cursor.execute(refresh_query)
                                    refresh_sfqid = cursor.sfqid
                                    context.log.info(f"Refreshed Snowpipe: {pipe_name_v}")

                                    metadata = {
                                        "pipe_name": pipe_name_v,
                                        "database": db_v,
                                        "schema": schema_v,
                                    }
                                    # Per-run numeric perf trace for the REFRESH query.
                                    metadata.update(_emit_query_perf(cursor, refresh_sfqid))

                                    # SYSTEM$PIPE_STATUS — works for any role with USAGE on the pipe.
                                    # Returns a JSON string with numeric fields worth plotting:
                                    # pendingFileCount, lastReceivedMessageTimestamp, executionState, etc.
                                    try:
                                        cursor.execute(
                                            f"SELECT SYSTEM$PIPE_STATUS('{db_v}.{schema_v}.{pipe_name_v}')"
                                        )
                                        status = cursor.fetchone()
                                        if status and status[0]:
                                            status_raw = status[0]
                                            metadata["pipe_status"] = str(status_raw)
                                            # Parse the JSON for numeric plottable fields.
                                            try:
                                                import json as _json
                                                status_json = _json.loads(status_raw) if isinstance(status_raw, str) else status_raw
                                                if isinstance(status_json, dict):
                                                    if "pendingFileCount" in status_json:
                                                        metadata["snowflake/pending_file_count"] = MetadataValue.int(
                                                            int(status_json["pendingFileCount"] or 0)
                                                        )
                                                    if "executionState" in status_json:
                                                        metadata["pipe_execution_state"] = status_json["executionState"]
                                                    if "lastReceivedMessageTimestamp" in status_json:
                                                        metadata["last_received_message_timestamp"] = (
                                                            status_json["lastReceivedMessageTimestamp"]
                                                        )
                                            except Exception:
                                                pass
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read pipe status for {pipe_name_v}: {exc}."
                                        )

                                    # SHOW PIPES picks up notification_channel + owner, which the
                                    # INFORMATION_SCHEMA path doesn't expose. Works for
                                    # least-privilege roles.
                                    try:
                                        cursor.execute(
                                            f"SHOW PIPES LIKE '{pipe_name_v}' "
                                            f"IN SCHEMA {db_v}.{schema_v}"
                                        )
                                        info = cursor.fetchone()
                                        if info:
                                            columns = [col[0].lower() for col in cursor.description]
                                            info_dict = dict(zip(columns, info))
                                            metadata.update({
                                                "owner": info_dict.get("owner"),
                                                "notification_channel": info_dict.get("notification_channel"),
                                                "definition": info_dict.get("definition"),
                                            })
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read pipe metadata for {pipe_name_v}: {exc}."
                                        )

                                    # COPY_HISTORY's TABLE_NAME parameter takes the COPY
                                    # *target* table, NOT the pipe name. The target was
                                    # parsed out of the pipe DEFINITION at import-time and
                                    # captured in this closure as target_table_v; if it's
                                    # None (couldn't parse), skip the query entirely rather
                                    # than running one guaranteed to fail. The
                                    # `pipe_name = ...` filter scopes the result to this
                                    # pipe in case multiple pipes COPY into the same target.
                                    if target_table_v:
                                        try:
                                            qualified_pipe = f"{db_v}.{schema_v}.{pipe_name_v}"
                                            # COPY_HISTORY returns `status` as
                                            # 'Loaded' (mixed-case) and `pipe_name`
                                            # either qualified or unqualified
                                            # depending on the role's schema
                                            # context — so compare case-insensitively
                                            # and accept both forms. TABLE_NAME
                                            # already scopes to one target table,
                                            # so allowing the unqualified pipe
                                            # name can't cross-fire between
                                            # schemas.
                                            history_query = f"""
                                            SELECT
                                                file_name,
                                                stage_location,
                                                last_load_time,
                                                row_count,
                                                row_parsed,
                                                file_size,
                                                first_error_message,
                                                pipe_name
                                            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                                                TABLE_NAME => '{target_table_v}',
                                                START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                            ))
                                            WHERE UPPER(status) = 'LOADED'
                                              AND UPPER(pipe_name) IN (
                                                UPPER('{pipe_name_v}'),
                                                UPPER('{qualified_pipe}')
                                              )
                                            ORDER BY last_load_time DESC
                                            LIMIT 5
                                            """
                                            cursor.execute(history_query)
                                            recent_loads = cursor.fetchall()
                                            metadata["snowflake/recent_loads"] = MetadataValue.int(
                                                len(recent_loads) if recent_loads else 0
                                            )
                                        except Exception as exc:
                                            context.log.warning(
                                                f"Could not read COPY_HISTORY for {pipe_name_v}: {exc}. "
                                                f"Refresh succeeded; emitting asset without load-count metadata."
                                            )
                                    else:
                                        context.log.warning(
                                            f"Skipping COPY_HISTORY for {pipe_name_v}: could "
                                            f"not determine COPY target table from pipe "
                                            f"definition."
                                        )

                                    return metadata
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _snowpipe_asset

                        assets_list.append(_make_snowpipe_asset(
                            pipe_name, pipe['DATABASE_NAME'], pipe['SCHEMA_NAME'],
                            target_table, _pipe_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake pipes: {e}")

            # Import Stages
            if self.import_stages:
                try:
                    # SHOW STAGES (not INFORMATION_SCHEMA.STAGES) — SHOW only
                    # needs USAGE on the schema; INFORMATION_SCHEMA can be
                    # invisible to least-privilege roles.
                    query = f"SHOW STAGES IN SCHEMA {self.database}.{self.schema_name}"
                    stages = self._execute_query(conn, query)

                    for stage in stages:
                        # SHOW STAGES returns NAME (not STAGE_NAME), DATABASE_NAME,
                        # SCHEMA_NAME, URL, TYPE, OWNER — different from
                        # INFORMATION_SCHEMA.STAGES column names.
                        stage_name = stage['NAME']

                        if not self._should_include_entity(stage_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"stage_{re.sub(r'[^a-zA-Z0-9_]', '_', stage_name.lower())}"

                        # Stages are observable (monitor files)
                        _stage_kwargs = self._apply_asset_overrides(stage_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stage: {stage_name}",
                            metadata={
                                "snowflake_stage_name": stage_name,
                                "snowflake_database": stage.get('DATABASE_NAME', self.database),
                                "snowflake_schema": stage.get('SCHEMA_NAME', self.schema_name),
                                "snowflake_url": stage.get('URL'),
                                "snowflake_type": stage.get('TYPE'),
                                "entity_type": "stage",
                            },
                        ))
                        def _make_stage_asset(stage_name_v, db_v, schema_v, stage_kwargs_v, self_v):
                            @observable_source_asset(**stage_kwargs_v)
                            def _stage_asset(context):
                                """Observable stage asset — emits file_count + total_bytes metrics."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                metadata: dict = {
                                    "stage_name": stage_name_v,
                                    "database": db_v,
                                    "schema": schema_v,
                                }
                                try:
                                    cursor.execute(f"LIST @{db_v}.{schema_v}.{stage_name_v}")
                                    files = cursor.fetchall() or []
                                    file_count = len(files)
                                    total_bytes = 0
                                    for file_row in files:
                                        if len(file_row) > 2 and file_row[2] is not None:
                                            try:
                                                total_bytes += int(file_row[2])
                                            except (TypeError, ValueError):
                                                pass
                                    metadata["snowflake/file_count"] = MetadataValue.int(file_count)
                                    metadata["snowflake/total_bytes"] = MetadataValue.int(total_bytes)
                                    context.log.info(
                                        f"Stage {stage_name_v} has {file_count} files, "
                                        f"total size: {total_bytes} bytes"
                                    )
                                except Exception as exc:
                                    context.log.warning(
                                        f"Could not LIST @{stage_name_v}: {exc}."
                                    )
                                finally:
                                    cursor.close()
                                    conn.close()
                                # data_version: file_count + total_bytes — only
                                # changes when stage contents actually move,
                                # so downstream eager doesn't cascade on
                                # every observation tick.
                                _fc = metadata.get("snowflake/file_count")
                                _tb = metadata.get("snowflake/total_bytes")
                                signature = (
                                    f"{getattr(_fc, 'value', _fc)}:"
                                    f"{getattr(_tb, 'value', _tb)}"
                                )
                                return ObserveResult(
                                    data_version=DataVersion(signature),
                                    metadata=metadata,
                                )
                            return _stage_asset

                        assets_list.append(_make_stage_asset(
                            stage_name,
                            stage.get('DATABASE_NAME', self.database),
                            stage.get('SCHEMA_NAME', self.schema_name),
                            _stage_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake stages: {e}")

            # Import Materialized Views
            if self.import_materialized_views:
                try:
                    # The original SELECT FROM INFORMATION_SCHEMA.VIEWS WHERE
                    # table_type='MATERIALIZED VIEW' didn't work (VIEWS uses
                    # `name`/`table_name` not `table_type`). SHOW MATERIALIZED
                    # VIEWS is the canonical query (Enterprise+ only —
                    # fails non-fatal on Standard edition).
                    query = f"SHOW MATERIALIZED VIEWS IN SCHEMA {self.database}.{self.schema_name}"

                    mv_list = self._execute_query(conn, query)

                    for mv in mv_list:
                        mv_name = mv['NAME']

                        if not self._should_include_entity(mv_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"mv_{re.sub(r'[^a-zA-Z0-9_]', '_', mv_name.lower())}"

                        # Materialized views are materializable
                        _mv_kwargs = self._apply_asset_overrides(mv_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake materialized view: {mv_name}",
                            metadata={
                                "snowflake_view_name": mv_name,
                                "snowflake_database": mv['DATABASE_NAME'],
                                "snowflake_schema": mv['SCHEMA_NAME'],
                                "snowflake_cluster_by": mv.get('CLUSTER_BY'),
                                "entity_type": "materialized_view",
                            },
                        ))
                        def _make_mv_asset(mv_name_v, db_v, schema_v, mv_kwargs_v, self_v):
                            @asset(**mv_kwargs_v)
                            def _mv_asset(context: AssetExecutionContext):
                                """Materialize by refreshing materialized view."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                try:
                                    cursor.execute(f"ALTER MATERIALIZED VIEW {db_v}.{schema_v}.{mv_name_v} SUSPEND")
                                    cursor.execute(f"ALTER MATERIALIZED VIEW {db_v}.{schema_v}.{mv_name_v} RESUME")
                                    resume_sfqid = cursor.sfqid
                                    context.log.info(f"Refreshed materialized view: {mv_name_v}")

                                    metadata = {
                                        "view_name": mv_name_v,
                                        "database": db_v,
                                        "schema": schema_v,
                                    }
                                    # Per-run numeric perf trace for the RESUME query.
                                    metadata.update(_emit_query_perf(cursor, resume_sfqid))

                                    # SHOW MATERIALIZED VIEWS — works for least-privilege roles.
                                    # Provides rows + bytes + cluster_by + behind_by + invalid + owner.
                                    try:
                                        cursor.execute(
                                            f"SHOW MATERIALIZED VIEWS LIKE '{mv_name_v}' "
                                            f"IN SCHEMA {db_v}.{schema_v}"
                                        )
                                        info = cursor.fetchone()
                                        if info:
                                            columns = [col[0].lower() for col in cursor.description]
                                            info_dict = dict(zip(columns, info))
                                            metadata.update({
                                                "cluster_by": info_dict.get("cluster_by"),
                                                "behind_by": info_dict.get("behind_by"),
                                                "invalid": info_dict.get("invalid"),
                                                "owner": info_dict.get("owner"),
                                            })
                                            # Numeric fields → plottable.
                                            if info_dict.get("rows") is not None:
                                                metadata["snowflake/rows"] = MetadataValue.int(int(info_dict["rows"]))
                                            if info_dict.get("bytes") is not None:
                                                metadata["snowflake/bytes"] = MetadataValue.int(int(info_dict["bytes"]))
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read MV metadata for {mv_name_v}: {exc}. "
                                            f"Refresh succeeded; emitting asset without enriched metadata."
                                        )
                                    return metadata
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _mv_asset

                        assets_list.append(_make_mv_asset(
                            mv_name, mv['DATABASE_NAME'], mv['SCHEMA_NAME'], _mv_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake materialized views: {e}")

            # Import External Tables
            if self.import_external_tables:
                try:
                    # SHOW EXTERNAL TABLES (not INFORMATION_SCHEMA.TABLES) —
                    # SHOW only needs USAGE on the schema; INFORMATION_SCHEMA
                    # can be invisible to least-privilege roles.
                    query = f"SHOW EXTERNAL TABLES IN SCHEMA {self.database}.{self.schema_name}"
                    ext_tables = self._execute_query(conn, query)

                    for ext_table in ext_tables:
                        # SHOW EXTERNAL TABLES returns NAME (not TABLE_NAME),
                        # DATABASE_NAME (not TABLE_CATALOG), SCHEMA_NAME (not
                        # TABLE_SCHEMA), plus richer fields (location, owner,
                        # last_refreshed_on, file_format_name, etc.).
                        table_name = ext_table['NAME']

                        if not self._should_include_entity(table_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"external_table_{re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())}"

                        # External tables can be refreshed
                        _ext_kwargs = self._apply_asset_overrides(table_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake external table: {table_name}",
                            metadata={
                                "snowflake_table_name": table_name,
                                "snowflake_database": ext_table.get('DATABASE_NAME', self.database),
                                "snowflake_schema": ext_table.get('SCHEMA_NAME', self.schema_name),
                                "entity_type": "external_table",
                            },
                        ))
                        def _make_external_table_asset(table_name_v, db_v, schema_v, ext_kwargs_v, self_v):
                            @asset(**ext_kwargs_v)
                            def _external_table_asset(context: AssetExecutionContext):
                                """Materialize by refreshing external table metadata."""
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                try:
                                    cursor.execute(f"ALTER EXTERNAL TABLE {db_v}.{schema_v}.{table_name_v} REFRESH")
                                    refresh_sfqid = cursor.sfqid
                                    context.log.info(f"Refreshed external table: {table_name_v}")

                                    metadata = {
                                        "table_name": table_name_v,
                                        "database": db_v,
                                        "schema": schema_v,
                                    }
                                    # Per-run numeric perf trace for the REFRESH query.
                                    metadata.update(_emit_query_perf(cursor, refresh_sfqid))

                                    # SHOW EXTERNAL TABLES — works for least-privilege roles.
                                    # Provides rows + location + file_format_name + last_refreshed.
                                    try:
                                        cursor.execute(
                                            f"SHOW EXTERNAL TABLES LIKE '{table_name_v}' "
                                            f"IN SCHEMA {db_v}.{schema_v}"
                                        )
                                        info = cursor.fetchone()
                                        if info:
                                            columns = [col[0].lower() for col in cursor.description]
                                            info_dict = dict(zip(columns, info))
                                            metadata.update({
                                                "location": info_dict.get("location"),
                                                "file_format_name": info_dict.get("file_format_name"),
                                                "last_refreshed_on": str(info_dict.get("last_refreshed_on"))
                                                    if info_dict.get("last_refreshed_on") else None,
                                                "owner": info_dict.get("owner"),
                                            })
                                            if info_dict.get("rows") is not None:
                                                metadata["snowflake/rows"] = MetadataValue.int(int(info_dict["rows"]))
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read external table metadata for {table_name_v}: {exc}. "
                                            f"Refresh succeeded; emitting asset without enriched metadata."
                                        )
                                    return metadata
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _external_table_asset

                        assets_list.append(_make_external_table_asset(
                            table_name, ext_table.get('DATABASE_NAME', self.database), ext_table.get('SCHEMA_NAME', self.schema_name), _ext_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake external tables: {e}")

            # Import Alerts
            if self.import_alerts:
                try:
                    # INFORMATION_SCHEMA.ALERTS isn't a queryable view.
                    query = f"SHOW ALERTS IN SCHEMA {self.database}.{self.schema_name}"
                    alerts = self._execute_query(conn, query)

                    for alert in alerts:
                        alert_name = alert['NAME']

                        if not self._should_include_entity(alert_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"alert_{re.sub(r'[^a-zA-Z0-9_]', '_', alert_name.lower())}"

                        # Alerts are observable
                        _alert_kwargs = self._apply_asset_overrides(alert_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake alert: {alert_name}",
                            metadata={
                                "snowflake_alert_name": alert_name,
                                "snowflake_database": alert['DATABASE_NAME'],
                                "snowflake_schema": alert['SCHEMA_NAME'],
                                "snowflake_condition": alert.get('CONDITION'),
                                "entity_type": "alert",
                            },
                        ))
                        def _make_alert_asset(alert_name_v, db_v, schema_v, alert_kwargs_v, self_v):
                            @observable_source_asset(**alert_kwargs_v)
                            def _alert_asset(context: AssetExecutionContext):
                                """Observable alert asset - monitor alert status.

                                Returns ObserveResult with state / schedule / last-run
                                metadata. Newer Dagster rejects None returns from
                                @observable_source_asset bodies.
                                """
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                metadata: Dict[str, Any] = {
                                    "alert_name": alert_name_v,
                                    "database": db_v,
                                    "schema": schema_v,
                                }
                                try:
                                    # SHOW ALERTS first — works for least-privilege roles where
                                    # INFORMATION_SCHEMA may be invisible. Exposes state +
                                    # condition + action + schedule.
                                    try:
                                        cursor.execute(
                                            f"SHOW ALERTS LIKE '{alert_name_v}' "
                                            f"IN SCHEMA {db_v}.{schema_v}"
                                        )
                                        info = cursor.fetchone()
                                        if info:
                                            columns = [col[0].lower() for col in cursor.description]
                                            info_dict = dict(zip(columns, info))
                                            metadata.update({
                                                "alert_state": info_dict.get("state"),
                                                "alert_schedule": info_dict.get("schedule"),
                                                "alert_condition": info_dict.get("condition"),
                                                "alert_action": info_dict.get("action"),
                                                "alert_owner": info_dict.get("owner"),
                                            })
                                            context.log.info(
                                                f"Alert {alert_name_v} state={info_dict.get('state')} "
                                                f"schedule={info_dict.get('schedule')}"
                                            )
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read alert metadata for {alert_name_v}: {exc}."
                                        )

                                    # ALERT_HISTORY is a table function — wrap in try/except
                                    # so the observation still emits even if the role can't
                                    # read history. Column list per INFORMATION_SCHEMA.ALERT_HISTORY
                                    # docs: name, database_name, schema_name, condition, action,
                                    # scheduled_time, state, query_id, error_code, error_message,
                                    # duration. The old query included `error` and
                                    # `completed_time` — neither exist; Snowflake compile-errored
                                    # on every observation tick.
                                    try:
                                        history_query = f"""
                                        SELECT
                                            name,
                                            state,
                                            scheduled_time,
                                            query_id,
                                            error_code,
                                            error_message,
                                            duration
                                        FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
                                            SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
                                        ))
                                        WHERE name = '{alert_name_v}'
                                        ORDER BY scheduled_time DESC
                                        LIMIT 1
                                        """
                                        cursor.execute(history_query)
                                        history = cursor.fetchone()
                                        if history:
                                            columns = [col[0] for col in cursor.description]
                                            history_dict = dict(zip(columns, history))
                                            metadata.update({
                                                "last_run_state": history_dict.get("STATE"),
                                                "last_run_query_id": history_dict.get("QUERY_ID"),
                                                "last_run_scheduled_time": str(history_dict.get("SCHEDULED_TIME")) if history_dict.get("SCHEDULED_TIME") else None,
                                                "last_run_error_code": history_dict.get("ERROR_CODE"),
                                                "last_run_error_message": history_dict.get("ERROR_MESSAGE"),
                                            })
                                            if history_dict.get("DURATION") is not None:
                                                try:
                                                    metadata["snowflake/alert_duration_ms"] = MetadataValue.int(int(history_dict["DURATION"]))
                                                except (TypeError, ValueError):
                                                    pass
                                            context.log.info(
                                                f"Alert {alert_name_v} last run state: "
                                                f"{history_dict.get('STATE')}"
                                            )
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not read ALERT_HISTORY for {alert_name_v}: {exc}."
                                        )
                                    # data_version: last-scheduled-time +
                                    # last-state + query_id signature so
                                    # downstream eager only re-fires when the
                                    # alert actually ran a new evaluation.
                                    signature = (
                                        f"{metadata.get('last_run_scheduled_time')}:"
                                        f"{metadata.get('last_run_state')}:"
                                        f"{metadata.get('last_run_query_id')}"
                                    )
                                    return ObserveResult(
                                        data_version=DataVersion(signature),
                                        metadata=metadata,
                                    )
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _alert_asset

                        assets_list.append(_make_alert_asset(
                            alert_name, alert['DATABASE_NAME'], alert['SCHEMA_NAME'], _alert_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing Snowflake alerts: {e}")

            # Import OpenFlow Flows
            if self.import_openflow_flows:
                try:
                    # Query OpenFlow telemetry to discover flows
                    # Get unique process group names from recent telemetry
                    query = f"""
                    SELECT DISTINCT
                        RECORD['process_group_name']::STRING AS flow_name,
                        RECORD['runtime_id']::STRING AS runtime_id
                    FROM SNOWFLAKE.TELEMETRY.EVENTS
                    WHERE RECORD_TYPE = 'openflow_metric'
                    AND RECORD['metric_name']::STRING = 'process_group_input_bytes'
                    AND TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                    ORDER BY flow_name
                    """

                    flows = self._execute_query(conn, query)

                    for flow in flows:
                        flow_name = flow['FLOW_NAME']
                        runtime_id = flow.get('RUNTIME_ID')

                        if not flow_name or not self._should_include_entity(flow_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"openflow_{re.sub(r'[^a-zA-Z0-9_]', '_', flow_name.lower())}"

                        # OpenFlow flows are observable
                        _flow_kwargs = self._apply_asset_overrides(flow_name, dict(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"OpenFlow data integration flow: {flow_name}",
                            metadata={
                                "openflow_flow_name": flow_name,
                                "openflow_runtime_id": runtime_id,
                                "entity_type": "openflow_flow",
                            },
                        ))
                        def _make_openflow_asset(flow_name_v, runtime_id_v, flow_kwargs_v, self_v):
                            @observable_source_asset(**flow_kwargs_v)
                            def _openflow_asset(context: AssetExecutionContext):
                                """Observable OpenFlow flow — monitor via telemetry.

                                Returns ObserveResult with recent-metric count.
                                Newer Dagster rejects None returns from
                                @observable_source_asset bodies.
                                """
                                conn = self_v._create_connection()
                                cursor = conn.cursor()
                                metadata: Dict[str, Any] = {
                                    "openflow_flow_name": flow_name_v,
                                    "openflow_runtime_id": runtime_id_v,
                                }
                                try:
                                    metrics_query = f"""
                                    SELECT
                                        TIMESTAMP,
                                        RECORD['metric_name']::STRING AS metric_name,
                                        RECORD['metric_value']::NUMBER AS metric_value,
                                        RECORD['component_name']::STRING AS component_name
                                    FROM SNOWFLAKE.TELEMETRY.EVENTS
                                    WHERE RECORD_TYPE = 'openflow_metric'
                                    AND RECORD['process_group_name']::STRING = '{flow_name_v}'
                                    AND TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                    ORDER BY TIMESTAMP DESC
                                    LIMIT 100
                                    """
                                    try:
                                        cursor.execute(metrics_query)
                                        metrics = cursor.fetchall()
                                    except Exception as exc:
                                        context.log.warning(
                                            f"Could not query OpenFlow telemetry for {flow_name_v}: {exc}. "
                                            f"Common cause: role lacks IMPORT SHARE on SNOWFLAKE database "
                                            f"or telemetry is disabled on this runtime."
                                        )
                                        metrics = []
                                    metadata["snowflake/openflow_recent_metric_count"] = MetadataValue.int(len(metrics))
                                    # Pull the most-recent metric timestamp
                                    # for the data_version signature so
                                    # downstream eager re-fires only when new
                                    # metrics actually land in TELEMETRY.EVENTS.
                                    latest_ts = str(metrics[0][0]) if metrics else "none"
                                    metadata["snowflake/openflow_latest_metric_ts"] = latest_ts
                                    if metrics:
                                        context.log.info(f"OpenFlow flow {flow_name_v} (runtime {runtime_id_v}) has {len(metrics)} recent metrics")
                                    else:
                                        context.log.info(f"OpenFlow flow {flow_name_v} (runtime {runtime_id_v}) has no recent activity")
                                    signature = f"{len(metrics)}:{latest_ts}"
                                    return ObserveResult(
                                        data_version=DataVersion(signature),
                                        metadata=metadata,
                                    )
                                finally:
                                    cursor.close()
                                    conn.close()
                            return _openflow_asset

                        assets_list.append(_make_openflow_asset(
                            flow_name, runtime_id, _flow_kwargs, self,
                        ))

                except Exception as e:
                    _logger.error(f"Error importing OpenFlow flows: {e}")

        finally:
            conn.close()

        # Create observation sensor if requested
        if self.generate_sensor and (task_metadata or snowpipe_metadata):
            @sensor(
                name=f"{self.group_name}_observation_sensor",
                minimum_interval_seconds=self.poll_interval_seconds
            )
            def snowflake_observation_sensor(context: SensorEvaluationContext):
                """Sensor to observe Snowflake task runs and Snowpipe loads.

                Dynamic-table refreshes are handled by the dedicated
                ``<group>_dt_refresh_sensor`` (see DT-refresh sensor block).

                Returns a single ``SensorResult(asset_events=[...])`` —
                Dagster's sensor framework rejects ``yield
                AssetMaterialization(...)`` (list-member-type check
                expects SkipReason/RunRequest/DagsterRunReaction).
                """
                conn = self._create_connection()
                cursor = conn.cursor()
                events: list = []

                try:
                    # Check for completed task runs
                    for asset_key, metadata in task_metadata.items():
                        task_name = metadata['task_name']
                        db = metadata['database']
                        schema_name = metadata['schema']

                        try:
                            # Get recent task history
                            history_query = f"""
                            SELECT
                                query_id,
                                state,
                                scheduled_time,
                                query_start_time,
                                completed_time
                            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                TASK_NAME => '{task_name}',
                                SCHEDULED_TIME_RANGE_START => DATEADD('minute', -{self.poll_interval_seconds / 60}, CURRENT_TIMESTAMP())
                            ))
                            WHERE state = 'SUCCEEDED'
                            ORDER BY scheduled_time DESC
                            LIMIT 5
                            """

                            cursor.execute(history_query)

                            for run in cursor:
                                columns = [col[0] for col in cursor.description]
                                run_dict = dict(zip(columns, run))

                                # Stable signature so eager downstream doesn't
                                # re-fire when the sensor's lookback window catches
                                # the same TASK_HISTORY row twice.
                                _sig = f"{run_dict.get('QUERY_ID')}:{run_dict.get('STATE')}"
                                events.append(AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "query_id": run_dict.get('QUERY_ID'),
                                        "state": run_dict.get('STATE'),
                                        "scheduled_time": str(run_dict.get('SCHEDULED_TIME')) if run_dict.get('SCHEDULED_TIME') else None,
                                        "completed_time": str(run_dict.get('COMPLETED_TIME')) if run_dict.get('COMPLETED_TIME') else None,
                                        "source": "snowflake_observation_sensor",
                                        "entity_type": "task",
                                    },
                                    tags={"dagster/data_version": _sig},
                                ))
                        except Exception as e:
                            _logger.error(f"Error checking runs for task {task_name}: {e}")

                    # Check for Snowpipe loads.
                    # COPY_HISTORY's TABLE_NAME parameter takes the COPY
                    # *target* table, NOT the pipe name (passing the pipe
                    # name yields a 002003 "Table does not exist" error
                    # every tick). The target was parsed out of the pipe
                    # DEFINITION at import-time; if parsing failed, skip
                    # the pipe with a one-line warning instead of running
                    # a broken query.
                    #
                    # The pipe_name filter prevents cross-pipe interference
                    # when multiple pipes COPY into the same target table.
                    # Snowflake returns `status` as 'Loaded' (mixed-case)
                    # and `pipe_name` either qualified or unqualified
                    # depending on the role's schema context — so compare
                    # case-insensitively and accept both forms.
                    for asset_key, metadata in snowpipe_metadata.items():
                        pipe_name = metadata['pipe_name']
                        target_table = metadata.get('target_table')
                        if not target_table:
                            _logger.warning(
                                f"Skipping Snowpipe {pipe_name}: could not "
                                f"determine COPY target table from pipe "
                                f"definition."
                            )
                            continue

                        qualified_pipe = (
                            f"{metadata['database']}.{metadata['schema']}.{pipe_name}"
                        )

                        try:
                            history_query = f"""
                            SELECT
                                file_name,
                                stage_location,
                                last_load_time,
                                row_count,
                                row_parsed,
                                file_size,
                                status,
                                first_error_message,
                                pipe_name
                            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                                TABLE_NAME => '{target_table}',
                                START_TIME => DATEADD('minute',
                                                      -{self.poll_interval_seconds / 60},
                                                      CURRENT_TIMESTAMP())
                            ))
                            WHERE UPPER(status) = 'LOADED'
                              AND UPPER(pipe_name) IN (
                                UPPER('{pipe_name}'),
                                UPPER('{qualified_pipe}')
                              )
                            ORDER BY last_load_time DESC
                            LIMIT 10
                            """

                            cursor.execute(history_query)

                            for load in cursor:
                                columns = [col[0] for col in cursor.description]
                                load_dict = dict(zip(columns, load))

                                # Stable signature — same loaded file + load time
                                # means same materialization. Prevents double-emit
                                # when the sensor's lookback window catches the
                                # same COPY_HISTORY row twice across ticks.
                                _sig = f"{load_dict.get('FILE_NAME')}:{load_dict.get('LAST_LOAD_TIME')}"
                                events.append(AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "pipe_name": pipe_name,
                                        "file_name": load_dict.get('FILE_NAME'),
                                        "last_load_time": str(load_dict.get('LAST_LOAD_TIME')) if load_dict.get('LAST_LOAD_TIME') else None,
                                        "row_count": load_dict.get('ROW_COUNT'),
                                        "file_size": load_dict.get('FILE_SIZE'),
                                        "source": "snowflake_observation_sensor",
                                        "entity_type": "snowpipe",
                                    },
                                    tags={"dagster/data_version": _sig},
                                ))
                        except Exception as e:
                            _logger.error(f"Error checking loads for Snowpipe {pipe_name}: {e}")

                finally:
                    cursor.close()
                    conn.close()

                if events:
                    return SensorResult(asset_events=events)
                return SkipReason(
                    "No new task completions or pipe loads in this tick window."
                )

            sensors_list.append(snowflake_observation_sensor)

        # Dedicated DT-refresh sensor.
        # Always wired in when import_dynamic_tables=True (regardless of
        # dt_modeling), so Snowflake's TARGET_LAG-driven auto-refreshes
        # propagate as Dagster materialization events for every imported DT.
        # One SHOW DYNAMIC TABLES IN SCHEMA <db>.<schema> per tick + a JSON
        # cursor keyed by DT name stores each DT's previous
        # (last_refresh_action, last_refresh_status, last_refresh_status_timestamp);
        # we emit AssetMaterialization only when that tuple changes AND status
        # is SUCCEEDED. Skips ticks with no change (de-dupe).
        if self.import_dynamic_tables and dt_asset_keys:
            _dt_name_to_asset_key = dict(dt_name_to_asset_key)
            _dt_asset_selection = [AssetKey([k]) for k in dt_asset_keys]
            _self_for_sensor = self

            @sensor(
                name=f"{self.group_name}_dt_refresh_sensor",
                minimum_interval_seconds=self.dt_refresh_sensor_interval_seconds,
                asset_selection=_dt_asset_selection,
            )
            def snowflake_dt_refresh_sensor(context: SensorEvaluationContext):
                """Emit AssetMaterialization for any DT whose latest refresh tuple changed.

                One SHOW DYNAMIC TABLES per tick; per-DT diff against a JSON
                cursor. Catches Snowflake's TARGET_LAG-driven auto-refreshes
                that Dagster never triggered itself, so external assets
                (``dt_modeling='external'``) still get materialization events.
                """
                try:
                    prev_state: Dict[str, list] = json.loads(context.cursor) if context.cursor else {}
                except Exception:
                    prev_state = {}

                new_state: Dict[str, list] = dict(prev_state)
                asset_events: list = []

                conn = _self_for_sensor._create_connection()
                cursor = conn.cursor()
                try:
                    try:
                        cursor.execute(
                            f"SHOW DYNAMIC TABLES IN SCHEMA "
                            f"{_self_for_sensor.database}.{_self_for_sensor.schema_name}"
                        )
                        rows = cursor.fetchall()
                        columns = [c[0].lower() for c in cursor.description]
                    except Exception as exc:
                        context.log.warning(
                            f"snowflake_dt_refresh_sensor: SHOW DYNAMIC TABLES failed: {exc}"
                        )
                        return SensorResult(asset_events=[], cursor=context.cursor or "{}")

                    for row in rows:
                        rd = dict(zip(columns, row))
                        dt_name = rd.get("name")
                        if not dt_name or dt_name not in _dt_name_to_asset_key:
                            continue

                        # Snowflake renamed the SHOW DYNAMIC TABLES refresh
                        # columns to `last_completed_refresh_*` in 2024;
                        # legacy accounts still expose `last_refresh_*`
                        # (and older still `last_successful_refresh_*` /
                        # `last_refresh_state`). Probe current names
                        # first, fall back to legacy. `data_timestamp` is
                        # populated even when refresh-status fields are
                        # null (initial materialization, accounts where
                        # refresh tracking lags), so include it in the
                        # timestamp fallback chain.
                        action = (
                            rd.get("last_completed_refresh_action")
                            or rd.get("last_refresh_action")
                            or rd.get("last_successful_refresh_action")
                        )
                        status = (
                            rd.get("last_completed_refresh_status")
                            or rd.get("last_refresh_status")
                            or rd.get("last_refresh_state")
                        )
                        ts = (
                            rd.get("last_completed_refresh_status_timestamp")
                            or rd.get("last_refresh_status_timestamp")
                            or rd.get("data_timestamp")
                            or rd.get("last_suspended_on")
                            or rd.get("last_refreshed_on")
                        )
                        cur_tuple = [action, status, str(ts) if ts is not None else None]
                        prev_tuple = prev_state.get(dt_name)

                        new_state[dt_name] = cur_tuple

                        if prev_tuple == cur_tuple:
                            continue
                        # Some Snowflake versions/regions don't expose any
                        # refresh-status column on SHOW DYNAMIC TABLES —
                        # only `data_timestamp`. In that case status is
                        # None and we use the advancing `data_timestamp`
                        # itself as the success signal (Snowflake does
                        # not advance data_timestamp on failed refreshes).
                        # Accounts that DO expose status keep the strict
                        # 'SUCCEEDED' check.
                        # TODO: refactor to INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY
                        # (stable cross-version columns: state,
                        # refresh_action, data_timestamp).
                        if status is not None and status != "SUCCEEDED":
                            continue

                        asset_key_str = _dt_name_to_asset_key[dt_name]
                        metadata: Dict[str, Any] = {
                            "last_refresh_status": status,
                            "refresh_mode": rd.get("refresh_mode"),
                            "target_lag": rd.get("target_lag"),
                        }
                        if rd.get("rows") is not None:
                            try:
                                metadata["rows"] = int(rd["rows"])
                            except (TypeError, ValueError):
                                pass
                        if rd.get("bytes") is not None:
                            try:
                                metadata["bytes"] = int(rd["bytes"])
                            except (TypeError, ValueError):
                                pass

                        # Big-impact site: DTs auto-refresh on TARGET_LAG even
                        # when their inputs haven't moved. Same row count + same
                        # bytes = no material change in the DT's output, so
                        # downstream AutomationCondition.eager() treats the
                        # emission as a no-op instead of cascading every TARGET_LAG.
                        _sig = f"{metadata.get('rows')}:{metadata.get('bytes')}"
                        asset_events.append(AssetMaterialization(
                            asset_key=AssetKey([asset_key_str]),
                            metadata=metadata,
                            tags={"dagster/data_version": _sig},
                        ))
                finally:
                    cursor.close()
                    conn.close()

                return SensorResult(
                    asset_events=asset_events,
                    cursor=json.dumps(new_state),
                )

            sensors_list.append(snowflake_dt_refresh_sensor)

        return Definitions(
            assets=assets_list,
            sensors=sensors_list if sensors_list else None,
        )
