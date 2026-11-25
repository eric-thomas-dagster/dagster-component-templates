"""SQL Server CDC Monitor Sensor Component.

Micro-batch change-data-capture source using SQL Server's full **Change Data
Capture** feature (`cdc.<capture_instance>_CT` system tables + the
`cdc.fn_cdc_get_all_changes_<capture_instance>` table function) -- NOT the
lighter-weight **Change Tracking** feature this registry's existing
`sql_server_change_tracking_asset` already covers. Change Tracking only
exposes an I/U/D flag plus each row's CURRENT values (joined back from the
base table); it cannot give you an update's BEFORE values at all. Full CDC
reads the transaction log itself, so both before- and after- row images are
available for updates -- the same `_cdc_op`/`before`/`after` contract this
registry's `mysql_cdc_monitor` already established, so the same downstream
CDC-apply sinks (the `delete_column`-based modes on `dataframe_to_delta_table`
/ `sql_to_database_asset` / `dataframe_to_databricks`) can consume either
source's output identically.

Requires CDC already enabled server-side -- this component only reads:
    ALTER DATABASE <db> SET ... (enable_broker not required)
    EXEC sys.sp_cdc_enable_db;
    EXEC sys.sp_cdc_enable_table @source_schema = N'<schema>', @source_name = N'<table>',
         @role_name = NULL, @supports_net_changes = 0;

Real, verified T-SQL (Microsoft Learn, `cdc.fn_cdc_get_all_changes_<capture_
instance>`, checked 2026-11-25):
  - `cdc.fn_cdc_get_all_changes_<capture_instance>(from_lsn, to_lsn, 'all update old')`
    returns one row per change; with the `'all update old'` row-filter option,
    an UPDATE is represented as TWO rows (before + after) rather than one.
  - `__$operation`: 1 = delete, 2 = insert, 3 = update (before-image, only
    with 'all update old'), 4 = update (after-image).
  - `sys.fn_cdc_get_min_lsn('<capture_instance>')` -- the oldest LSN this
    capture instance's cleanup job still retains. If the stored cursor is
    older than this, history needed to resume cleanly has already been
    purged -- this fails the tick loudly (same as MySQL's binlog-purged
    error 1236 handling) rather than silently skipping the gap.
  - `sys.fn_cdc_get_max_lsn()` -- the server's current max LSN (database-
    wide, not per-table) -- the "catch up to" boundary each tick.
  - `sys.fn_cdc_increment_lsn(lsn)` -- the next LSN strictly after a given
    one; used to build the next tick's `from_lsn` from the previous tick's
    `to_lsn` so the boundary row is never reprocessed.

Capture instances are discovered (not declared) each tick by querying
`cdc.change_tables` joined to `sys.schemas`/`sys.tables` -- a table can have
more than one capture instance (e.g. during a column-list migration), so a
pattern can resolve to more than one instance for the same table; all
matching instances are queried.
"""

import fnmatch
import json
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    Component,
    ComponentLoadContext,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


def _jsonable(value):
    """SQL Server column values can be datetime/date/Decimal/bytes/memoryview
    -- none of those round-trip through Dagster's run_config JSON
    serialization, so coerce them to plain str/hex, matching
    mysql_cdc_monitor's own `_jsonable` convention exactly so a downstream
    consumer handles both sources' events identically."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b.hex()
    return str(value)


def _jsonable_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    return {k: _jsonable(v) for k, v in row.items()}


def _lsn_to_hex(lsn) -> Optional[str]:
    if lsn is None:
        return None
    return bytes(lsn).hex()


def _lsn_from_hex(hex_str: Optional[str]):
    if hex_str is None:
        return None
    return bytes.fromhex(hex_str)


class SQLServerCDCMonitorSensorComponent(Component, Model, Resolvable):
    """Component for micro-batch SQL Server CDC via `cdc.fn_cdc_get_all_changes_*`.

    Reads SQL Server's Change Data Capture change tables from a stored LSN
    position, classifies each row change as insert/update/delete (with real
    before/after values for updates), batches everything from one tick into
    a single RunRequest, and stops once it has caught up to
    `sys.fn_cdc_get_max_lsn()` -- it never blocks waiting for new events.

    Example:
        ```yaml
        type: dagster_community_components.SQLServerCDCMonitorSensorComponent
        attributes:
          sensor_name: orders_cdc_sensor
          host: db.internal
          username: cdc_reader
          password_env_var: MSSQL_CDC_PASSWORD
          database: sales
          table_patterns:
            - dbo.orders
            - dbo.order_items_*
          job_name: apply_sales_cdc_job
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    host: str = Field(description="SQL Server host")
    port: int = Field(default=1433, description="SQL Server port")
    username: str = Field(description="SQL Server login with db_owner (or equivalent SELECT + cdc-role) on `database`")
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding the SQL Server password",
    )
    driver: str = Field(
        default="pymssql",
        description="'pymssql' (pure Python, default) or 'pyodbc' (requires ODBC Driver 18 installed) -- matches mssql_resource's convention.",
    )
    encrypt: bool = Field(default=True, description="Enable connection encryption. Required for Azure SQL.")
    trust_server_certificate: bool = Field(
        default=False,
        description="When True, skip server-cert verification. Use only for self-signed local SQL Server.",
    )

    database: str = Field(
        description="Which SQL Server database to track. CDC must already be enabled on it (sys.sp_cdc_enable_db).",
    )

    table_patterns: List[str] = Field(
        description=(
            "fnmatch-style glob patterns matched against 'schema.table' "
            "(e.g. 'dbo.orders', 'dbo.order_items_*'). Resolved against "
            "cdc.change_tables + sys.schemas/sys.tables at the start of "
            "every tick (not cached) -- a newly CDC-enabled table matching "
            "a wildcard is picked up automatically on its next tick, no "
            "restart required. A table can resolve to more than one "
            "capture instance (e.g. mid-migration); all matching instances "
            "for a matched table are queried."
        ),
    )

    job_name: str = Field(description="Name of the job to trigger when changes are detected")

    minimum_interval_seconds: int = Field(default=30, description="Minimum time (in seconds) between sensor evaluations")

    max_events_per_tick: Optional[int] = Field(
        default=None,
        description=(
            "Caps how many row-change events this sensor processes in one "
            "tick, even if more remain before it would otherwise report "
            "'caught up'. Unset (default): no cap. When the cap is hit, "
            "position is persisted only through the last event actually "
            "processed -- the remainder is picked up on the next tick."
        ),
    )

    on_first_run: str = Field(
        default="start_now",
        description=(
            "'start_now' (default) -- skip all historical CDC data and start "
            "capturing changes from the server's CURRENT max LSN on the "
            "very first tick. Pair with a separate one-time snapshot/full-"
            "load of existing rows -- this sensor is the ongoing-changes "
            "half of a CDC pipeline, not an initial-load mechanism. "
            "'from_beginning' -- start from the OLDEST LSN any tracked "
            "capture instance still retains (the minimum of each matched "
            "instance's own sys.fn_cdc_get_min_lsn). Rarely useful outside "
            "small/test databases with long CDC retention."
        ),
    )

    op_name: Optional[str] = Field(
        default="config",
        description=(
            "Op name to key the run_config under. Emitted as "
            "`{'ops': {op_name: {'config': {...}}}}`. Defaults to 'config' "
            "for backward compat with other sensors in this registry; set "
            "to the real op/asset name if it's not literally 'config'."
        ),
    )

    source_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional asset key to receive an AssetObservation on every "
            "tick that yields changes. Metadata includes database, "
            "tables, event_count, inserts, updates, deletes, from_lsn, to_lsn."
        ),
    )

    default_status: str = Field(default="running", description="Default status of the sensor (running or stopped)")

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. External assets show healthy/green in the Dagster "
            "UI and downstream AutomationCondition.eager() fires naturally "
            "on parent updates. When False, emit AssetObservation -- free "
            "of Dagster+ credit charges, but downstream conditions that "
            "gate on ~any_deps_missing() (including eager()) will not fire."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SQL Server CDC Monitor Sensor", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        host = self.host
        port = self.port
        username = self.username
        password_env_var = self.password_env_var
        driver = (self.driver or "pymssql").lower()
        encrypt = self.encrypt
        trust_server_certificate = self.trust_server_certificate
        database = self.database
        table_patterns = list(self.table_patterns or [])
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_events_per_tick = self.max_events_per_tick
        on_first_run = (self.on_first_run or "start_now").lower()
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key
        default_status_str = self.default_status

        if driver not in ("pymssql", "pyodbc"):
            raise ValueError(f"driver must be 'pymssql' | 'pyodbc', got {driver!r}")
        if on_first_run not in ("start_now", "from_beginning"):
            raise ValueError(f"on_first_run must be 'start_now' | 'from_beginning', got {on_first_run!r}")
        if not table_patterns:
            raise ValueError("table_patterns must list at least one pattern")

        default_status = (
            DefaultSensorStatus.RUNNING if default_status_str == "running" else DefaultSensorStatus.STOPPED
        )

        _event_cls = AssetMaterialization if self.emit_materialization else AssetObservation
        placeholder = "%s" if driver == "pymssql" else "?"

        def _connect(password: str):
            if driver == "pyodbc":
                import pyodbc
                return pyodbc.connect(
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={host},{port};DATABASE={database};"
                    f"UID={username};PWD={password};"
                    f"Encrypt={'yes' if encrypt else 'no'};"
                    f"TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"
                )
            import pymssql
            return pymssql.connect(
                server=host, port=port, database=database, user=username, password=password,
            )

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
        )
        def sqlserver_cdc_sensor(context: SensorEvaluationContext):
            import os

            try:
                if driver == "pyodbc":
                    import pyodbc  # noqa: F401
                else:
                    import pymssql  # noqa: F401
            except ImportError:
                pkg = "pyodbc" if driver == "pyodbc" else "pymssql"
                return SensorResult(skip_reason=f"{pkg} is not installed. Run: pip install {pkg}")

            password = os.environ.get(password_env_var, "") if password_env_var else ""

            try:
                conn = _connect(password)
            except Exception as e:
                return SensorResult(skip_reason=f"Failed to connect to SQL Server: {e}")

            try:
                cur = conn.cursor()

                # Resolve table_patterns -> (schema, table, capture_instance) every tick
                # (not cached) -- a capture instance can be re-created with a new name
                # mid-migration, and cdc.change_tables is the authoritative source for
                # which instances currently exist.
                cur.execute(
                    "SELECT s.name AS schema_name, t.name AS table_name, ct.capture_instance "
                    "FROM cdc.change_tables ct "
                    "JOIN sys.tables t ON ct.source_object_id = t.object_id "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id"
                )
                all_instances = [
                    {"schema": row[0], "table": row[1], "capture_instance": row[2]}
                    for row in cur.fetchall()
                ]
                resolved = [
                    inst for inst in all_instances
                    if any(fnmatch.fnmatch(f"{inst['schema']}.{inst['table']}", p) for p in table_patterns)
                ]
                if not resolved:
                    cur.close()
                    conn.close()
                    return SensorResult(
                        skip_reason=f"No CDC capture instances in database '{database}' matched table_patterns {table_patterns}"
                    )

                cursor_data = {}
                if context.cursor:
                    try:
                        cursor_data = json.loads(context.cursor)
                    except json.JSONDecodeError:
                        context.log.warning("Could not parse cursor, treating as first run")

                stored_from_lsn_hex = cursor_data.get("from_lsn")

                cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
                to_lsn = cur.fetchone()[0]
                if to_lsn is None:
                    cur.close()
                    conn.close()
                    return SensorResult(skip_reason="sys.fn_cdc_get_max_lsn() returned NULL -- is CDC enabled on this database?")

                if stored_from_lsn_hex:
                    from_lsn = _lsn_from_hex(stored_from_lsn_hex)
                    # Check every resolved instance's own retention floor against the
                    # stored position -- one instance's history can be cleaned up
                    # while another's isn't, so this is checked per-instance, not once.
                    purged = []
                    for inst in resolved:
                        cur.execute(f"SELECT sys.fn_cdc_get_min_lsn({placeholder})", (inst["capture_instance"],))
                        min_lsn = cur.fetchone()[0]
                        if min_lsn is not None and from_lsn < bytes(min_lsn):
                            purged.append(f"{inst['schema']}.{inst['table']} (capture_instance={inst['capture_instance']})")
                    if purged:
                        cur.close()
                        conn.close()
                        return SensorResult(
                            skip_reason=(
                                f"Stored CDC position has aged out of retention for: {', '.join(purged)}. "
                                f"This sensor's stored position is too old -- re-snapshot the tracked "
                                f"tables and reset this sensor's cursor to resume cleanly; it will NOT "
                                f"silently skip the gap."
                            )
                        )
                elif on_first_run == "from_beginning":
                    min_lsns = []
                    for inst in resolved:
                        cur.execute(f"SELECT sys.fn_cdc_get_min_lsn({placeholder})", (inst["capture_instance"],))
                        row = cur.fetchone()
                        if row and row[0] is not None:
                            min_lsns.append(bytes(row[0]))
                    if not min_lsns:
                        cur.close()
                        conn.close()
                        return SensorResult(skip_reason="No minimum LSN available for any matched capture instance")
                    from_lsn = min(min_lsns)
                else:  # start_now (default)
                    from_lsn = to_lsn

                events = []
                inserts = updates = deletes = 0
                tables_seen = set()
                new_from_lsn = from_lsn
                last_processed_lsn = None
                cap_hit = False

                try:
                    for inst in resolved:
                        fn_name = f"cdc.fn_cdc_get_all_changes_{inst['capture_instance']}"
                        cur.execute(
                            f"SELECT * FROM {fn_name}({placeholder}, {placeholder}, N'all update old') ORDER BY __$start_lsn, __$seqval, __$operation",
                            (from_lsn, to_lsn),
                        )
                        col_names = [d[0] for d in cur.description]
                        rows = cur.fetchall()
                        table_label = f"{inst['schema']}.{inst['table']}"

                        pending_before = None
                        for raw_row in rows:
                            row = dict(zip(col_names, raw_row))
                            op_code = row["__$operation"]
                            meta_keys = {"__$start_lsn", "__$seqval", "__$operation", "__$update_mask"}
                            values = {k: v for k, v in row.items() if k not in meta_keys}

                            if op_code == 1:  # delete
                                events.append({
                                    "table": table_label, "_cdc_op": "delete",
                                    "before": _jsonable_row(values), "after": None,
                                })
                                deletes += 1
                                last_processed_lsn = row["__$start_lsn"]
                            elif op_code == 2:  # insert
                                events.append({
                                    "table": table_label, "_cdc_op": "insert",
                                    "before": None, "after": _jsonable_row(values),
                                })
                                inserts += 1
                                last_processed_lsn = row["__$start_lsn"]
                            elif op_code == 3:  # update, before-image -- pair with the next row (op 4)
                                pending_before = values
                                continue
                            elif op_code == 4:  # update, after-image
                                events.append({
                                    "table": table_label, "_cdc_op": "update",
                                    "before": _jsonable_row(pending_before), "after": _jsonable_row(values),
                                })
                                pending_before = None
                                updates += 1
                                last_processed_lsn = row["__$start_lsn"]
                            tables_seen.add(table_label)

                            # The cap is only ever checked here -- after a COMPLETE event
                            # (insert/delete immediately, or an update only once its
                            # op_code==4 pairs with the stored pending_before via `continue`
                            # above) -- so an update's before/after pair is never split
                            # across a cap boundary.
                            if max_events_per_tick and len(events) >= max_events_per_tick:
                                cap_hit = True
                                break
                        if cap_hit:
                            break
                    # The next tick must start strictly AFTER the last position actually
                    # consumed, or that row would be reprocessed (the range query above is
                    # inclusive on both ends) -- sys.fn_cdc_increment_lsn is the server's
                    # own LSN-arithmetic function for this, not hand-rolled byte math.
                    # When max_events_per_tick capped this tick early, that boundary is
                    # last_processed_lsn, NOT the originally-fetched to_lsn -- advancing to
                    # the full to_lsn regardless of the cap would silently skip every event
                    # between the cap and to_lsn that this tick never actually processed.
                    advance_from = last_processed_lsn if (cap_hit and last_processed_lsn is not None) else to_lsn
                    cur.execute(f"SELECT sys.fn_cdc_increment_lsn({placeholder})", (advance_from,))
                    new_from_lsn = cur.fetchone()[0]
                except Exception as e:
                    cur.close()
                    conn.close()
                    return SensorResult(skip_reason=f"Error reading SQL Server CDC changes: {e}")

                cur.close()
                conn.close()
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                raise

            # Next tick's from_lsn starts strictly after this tick's to_lsn -- increment
            # it NOW (not next tick) using the server's own LSN-arithmetic function so a
            # retried tick (before this cursor commit lands) still re-derives the exact
            # same boundary deterministically.
            # (Done inline rather than a second connection: reuse the already-open
            # increment result computed by the server is cheap and avoids a stale offset.)
            next_from_lsn_hex = _lsn_to_hex(new_from_lsn)

            new_cursor_data = dict(cursor_data)
            new_cursor_data["from_lsn"] = next_from_lsn_hex

            if not events:
                return SensorResult(cursor=json.dumps(new_cursor_data), skip_reason="No new SQL Server CDC events found")

            context.log.info(
                f"SQL Server CDC: {len(events)} change(s) across {len(tables_seen)} table(s) "
                f"in '{database}' (insert={inserts} update={updates} delete={deletes})"
            )

            run_config = {
                "ops": {
                    op_name: {
                        "config": {
                            "database": database,
                            "tables": sorted(tables_seen),
                            "events": events,
                            "event_count": len(events),
                        }
                    }
                }
            }

            asset_events = []
            if source_asset_key:
                asset_events = [
                    _event_cls(
                        asset_key=AssetKey.from_user_string(source_asset_key),
                        metadata={
                            "database": database,
                            "tables": sorted(tables_seen),
                            "event_count": len(events),
                            "inserts": inserts,
                            "updates": updates,
                            "deletes": deletes,
                            "from_lsn": _lsn_to_hex(from_lsn) or "",
                            "to_lsn": _lsn_to_hex(to_lsn) or "",
                        },
                    )
                ]

            start_marker = _lsn_to_hex(from_lsn) or ""
            end_marker = _lsn_to_hex(to_lsn) or ""
            run_key = f"{sensor_name}-{start_marker}->{end_marker}"

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=run_key,
                        run_config=run_config,
                    )
                ],
                cursor=json.dumps(new_cursor_data),
                asset_events=asset_events,
            )

        return Definitions(sensors=[sqlserver_cdc_sensor])
