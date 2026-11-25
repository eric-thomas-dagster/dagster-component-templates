"""Oracle CDC (LogMiner) Monitor Sensor Component.

Micro-batch change-data-capture source: mines Oracle's redo/archive logs from
a stored SCN (System Change Number) position, classifies each row change as
insert/update/delete with structured column-level before/after values, and
stops once it has caught up to the current SCN -- this is NOT a long-running
streaming process, it's a poll-catch-up-stop sensor like
mysql_cdc_monitor/s3_monitor/kafka_monitor.

VERIFIED against Oracle's own current (19c) `DBMS_LOGMNR` PL/SQL Packages and
Types Reference (docs.oracle.com/en/database/oracle/oracle-database/19/arpls/
DBMS_LOGMNR.html):
  - `DBMS_LOGMNR.MINE_VALUE(sql_redo_undo, column_name) RETURN VARCHAR2` and
    `DBMS_LOGMNR.COLUMN_PRESENT(sql_redo_undo, column_name) RETURN NUMBER`
    read a SPECIFIC column's value out of `V$LOGMNR_CONTENTS.REDO_VALUE` /
    `.UNDO_VALUE` -- this is the real, structured mechanism for before/after
    column values, NOT parsing the `SQL_REDO`/`SQL_UNDO` reconstructed-SQL
    text (which this component deliberately does not do -- SQL_REDO's own
    format has real documented escaping/quoting quirks that make it a much
    less reliable source of truth than the two mining functions built for
    exactly this). `column_name` must be `schema.table.column` (fully
    qualified). Neither function supports LONG, LOB, ADT, or COLLECTION
    datatypes -- a column of one of those types always reads as absent
    (`COLUMN_PRESENT` = 0), a real, documented limitation, not a bug here.
  - `DBMS_LOGMNR.CONTINUOUS_MINE` is CONFIRMED DESUPPORTED as of 19c ("no
    replacement functionality") -- this component does NOT use it. Instead:
    `DBMS_LOGMNR.ADD_LOGFILE` is called once per archived-log file covering
    the needed SCN range (resolved via `V$ARCHIVED_LOG.FIRST_CHANGE#`/
    `.NEXT_CHANGE#`, confirmed stable columns on that view for decades),
    then `DBMS_LOGMNR.START_LOGMNR(startScn=..., endScn=...)` mines exactly
    that range.
  - `V$ARCHIVED_LOG`'s `FIRST_CHANGE#`/`NEXT_CHANGE#`/`NAME`/`SEQUENCE#`
    columns are confirmed from an Oracle 10.2-era reference page (a current
    19c/21c page for this specific view could not be located) -- these
    column names are long-stable across Oracle versions; flagged honestly
    rather than silently assumed current.

KNOWN LIMITATION (deliberate, not an oversight): this sensor mines ARCHIVED
redo logs only, not the current online redo log -- changes become visible
once their containing log is archived (a log switch), not the instant they
commit. This trades a small latency tail for using the stable, documented
ADD_LOGFILE/archived-log path instead of the more fragile online-redo-log
member-tracking the desupported CONTINUOUS_MINE used to paper over.
"""

import fnmatch
import json
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


class OracleCDCMonitorSensorComponent(Component, Model, Resolvable):
    """Component for micro-batch Oracle CDC via LogMiner.

    Reads Oracle's archived redo logs from a stored SCN, mining each table's
    changed rows via `DBMS_LOGMNR.MINE_VALUE`/`COLUMN_PRESENT` for structured
    column-level before/after values (not SQL-text parsing), batches
    everything from one tick into a single RunRequest, and stops once it has
    caught up to the database's current SCN. Tracks exactly one schema per
    sensor instance -- declare one sensor per schema if you need more than
    one.

    Example:
        ```yaml
        type: dagster_community_components.OracleCDCMonitorSensorComponent
        attributes:
          sensor_name: orders_cdc_sensor
          host: db.internal
          service_name: ORCLPDB1
          username: cdc_user
          password_env_var: ORACLE_CDC_PASSWORD
          schema_name: SALES
          table_patterns:
            - ORDERS
            - ORDER_ITEMS_*
          job_name: apply_sales_cdc_job
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    host: str = Field(description="Oracle host")
    port: int = Field(default=1521, description="Oracle listener port")
    service_name: Optional[str] = Field(default=None, description="Oracle service name. Set this OR sid.")
    sid: Optional[str] = Field(default=None, description="Oracle SID. Set this OR service_name.")
    username: str = Field(
        description=(
            "Oracle username. Needs SELECT_CATALOG_ROLE (or explicit SELECT on "
            "V$LOGMNR_CONTENTS/V$ARCHIVED_LOG/V$DATABASE) and EXECUTE on "
            "DBMS_LOGMNR, plus LogMiner access to the tracked schema's "
            "supplementally-logged columns."
        ),
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Environment variable holding the Oracle password",
    )

    schema_name: str = Field(
        description=(
            "Which Oracle schema (SEG_OWNER) to track. This sensor tracks "
            "exactly ONE schema -- declare one sensor per schema if you need "
            "more than one."
        ),
    )

    table_patterns: List[str] = Field(
        description=(
            "fnmatch-style glob patterns (e.g. 'ORDERS', 'ORDER_ITEMS_*') "
            "matched against table names within `schema_name`. Resolved "
            "against ALL_TABLES at the start of every tick (not cached)."
        ),
    )

    job_name: str = Field(description="Name of the job to trigger when changes are detected")

    minimum_interval_seconds: int = Field(default=60, description="Minimum time (in seconds) between sensor evaluations")

    max_events_per_tick: Optional[int] = Field(
        default=None,
        description=(
            "Caps how many row-change events this sensor processes in one "
            "tick. Unset (default): no cap. When the cap is hit, the stored "
            "SCN only advances to the last event actually processed."
        ),
    )

    on_first_run: str = Field(
        default="start_now",
        description=(
            "'start_now' (default) -- start capturing changes from the "
            "database's CURRENT SCN on the very first tick; pair with a "
            "separate one-time snapshot/full-load of existing rows. "
            "'from_beginning' -- start from the EARLIEST SCN still covered "
            "by an archived log in V$ARCHIVED_LOG. Fails loudly via "
            "skip_reason if no archived logs are available at all."
        ),
    )

    op_name: Optional[str] = Field(
        default="config",
        description="Op name to key the run_config under. Emitted as `{'ops': {op_name: {'config': {...}}}}`.",
    )

    source_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional asset key to receive an AssetObservation on every tick "
            "that yields changes. Metadata includes schema_name, tables, "
            "event_count, inserts, updates, deletes, start_scn, end_scn."
        ),
    )

    default_status: str = Field(default="running", description="Default status of the sensor (running or stopped)")

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. When False, emit AssetObservation instead -- free of "
            "Dagster+ credit charges, but downstream conditions gating on "
            "~any_deps_missing() (including eager()) will not fire."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Oracle CDC Monitor Sensor", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        host = self.host
        port = self.port
        service_name = self.service_name
        sid = self.sid
        username = self.username
        password_env_var = self.password_env_var
        schema_name = self.schema_name
        table_patterns = list(self.table_patterns or [])
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_events_per_tick = self.max_events_per_tick
        on_first_run = (self.on_first_run or "start_now").lower()
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key
        default_status_str = self.default_status

        if on_first_run not in ("start_now", "from_beginning"):
            raise ValueError(f"on_first_run must be 'start_now' | 'from_beginning', got {on_first_run!r}")
        if not table_patterns:
            raise ValueError("table_patterns must list at least one pattern")
        if not service_name and not sid:
            raise ValueError("Either service_name or sid must be set.")

        default_status = (
            DefaultSensorStatus.RUNNING if default_status_str == "running" else DefaultSensorStatus.STOPPED
        )

        _event_cls = AssetMaterialization if self.emit_materialization else AssetObservation

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
        )
        def oracle_cdc_sensor(context: SensorEvaluationContext):
            import os

            try:
                import oracledb
            except ImportError:
                return SensorResult(skip_reason="oracledb is not installed. Run: pip install oracledb")

            password = os.environ.get(password_env_var, "") if password_env_var else ""

            try:
                if service_name:
                    dsn = oracledb.makedsn(host, port, service_name=service_name)
                else:
                    dsn = oracledb.makedsn(host, port, sid=sid)
                conn = oracledb.connect(user=username, password=password, dsn=dsn)
            except Exception as e:
                return SensorResult(skip_reason=f"Failed to connect to Oracle: {e}")

            try:
                cur = conn.cursor()

                # ── Resolve tracked tables (every tick, not cached) ──────
                cur.execute(
                    "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :owner",
                    {"owner": schema_name},
                )
                all_tables = [r[0] for r in cur.fetchall()]
                resolved_tables = sorted({
                    t for t in all_tables if any(fnmatch.fnmatch(t, p) for p in table_patterns)
                })
                if not resolved_tables:
                    return SensorResult(
                        skip_reason=f"No tables in schema '{schema_name}' matched table_patterns {table_patterns}"
                    )

                # ── Column lists per table (needed for MINE_VALUE calls) ──
                table_columns = {}
                for t in resolved_tables:
                    cur.execute(
                        "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = :owner AND TABLE_NAME = :tname "
                        "ORDER BY COLUMN_ID",
                        {"owner": schema_name, "tname": t},
                    )
                    table_columns[t] = [r[0] for r in cur.fetchall()]

                # ── Current SCN (end of range for this tick) ─────────────
                cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                end_scn = int(cur.fetchone()[0])

                # ── Stored position (start of range) ─────────────────────
                cursor_data = {}
                if context.cursor:
                    try:
                        cursor_data = json.loads(context.cursor)
                    except json.JSONDecodeError:
                        context.log.warning("Could not parse cursor, treating as first run")

                start_scn = cursor_data.get("scn")
                if start_scn is None:
                    if on_first_run == "start_now":
                        start_scn = end_scn
                    else:  # from_beginning
                        cur.execute(
                            "SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL"
                        )
                        row = cur.fetchone()
                        if not row or row[0] is None:
                            return SensorResult(skip_reason="No archived logs available to start from_beginning")
                        start_scn = int(row[0])

                if start_scn >= end_scn:
                    return SensorResult(
                        cursor=json.dumps({"scn": start_scn}),
                        skip_reason="No new Oracle CDC events found",
                    )

                # ── Resolve archived logs covering [start_scn, end_scn) ──
                cur.execute(
                    "SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE# FROM V$ARCHIVED_LOG "
                    "WHERE NAME IS NOT NULL AND FIRST_CHANGE# < :end_scn AND NEXT_CHANGE# > :start_scn "
                    "ORDER BY FIRST_CHANGE#",
                    {"start_scn": start_scn, "end_scn": end_scn},
                )
                log_rows = cur.fetchall()
                if not log_rows:
                    return SensorResult(
                        skip_reason=(
                            f"No archived logs cover SCN range [{start_scn}, {end_scn}). This sensor's "
                            f"stored position is likely too old -- the covering archived log has been "
                            f"deleted/expired. Re-snapshot the tracked tables and reset this sensor's "
                            f"cursor to resume cleanly; it will NOT silently skip the gap."
                        )
                    )
                earliest_first_change = min(r[1] for r in log_rows)
                if earliest_first_change > start_scn:
                    return SensorResult(
                        skip_reason=(
                            f"Gap detected: no archived log covers SCN {start_scn} (earliest available "
                            f"starts at {earliest_first_change}). The log covering this sensor's stored "
                            f"position has been deleted/expired -- re-snapshot the tracked tables and "
                            f"reset this sensor's cursor; it will NOT silently skip the gap."
                        )
                    )

                # ── Mine the range ────────────────────────────────────────
                events = []
                inserts = updates = deletes = 0
                tables_seen = set()
                last_scn_processed = start_scn

                try:
                    for i, (name, _first, _next) in enumerate(log_rows):
                        # Named PL/SQL constants, not guessed bitmask integers --
                        # DBMS_LOGMNR.NEW resets any prior log-file list (first file
                        # only), DBMS_LOGMNR.ADDFILE appends (every file after).
                        opt_name = "DBMS_LOGMNR.NEW" if i == 0 else "DBMS_LOGMNR.ADDFILE"
                        cur.execute(
                            f"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LogFileName => :name, options => {opt_name}); END;",
                            {"name": name},
                        )

                    # Named constants: DICT_FROM_ONLINE_CATALOG (resolve column
                    # names against this session's own catalog) + COMMITTED_DATA_ONLY
                    # (only fully-committed transactions, in commit order).
                    cur.execute(
                        "BEGIN DBMS_LOGMNR.START_LOGMNR(startScn => :s, endScn => :e, "
                        "options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;",
                        {"s": start_scn, "e": end_scn},
                    )

                    mine_cur = conn.cursor()
                    mine_cur.execute(
                        "SELECT SCN, SEG_OWNER, TABLE_NAME, OPERATION, REDO_VALUE, UNDO_VALUE "
                        "FROM V$LOGMNR_CONTENTS "
                        "WHERE SEG_OWNER = :owner AND TABLE_NAME IN ({}) "
                        "AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') AND SCN > :start_scn "
                        "ORDER BY SCN".format(",".join(f"'{t}'" for t in resolved_tables)),
                        {"owner": schema_name, "start_scn": start_scn},
                    )

                    for scn, seg_owner, table_name, operation, redo_value, undo_value in mine_cur:
                        columns = table_columns.get(table_name, [])

                        def _mine(raw_value, cols):
                            if raw_value is None:
                                return None
                            out = {}
                            for col in cols:
                                qualified = f"{seg_owner}.{table_name}.{col}"
                                present = mine_cur.callfunc(
                                    "DBMS_LOGMNR.COLUMN_PRESENT", int, [raw_value, qualified]
                                )
                                if present:
                                    out[col] = mine_cur.callfunc(
                                        "DBMS_LOGMNR.MINE_VALUE", str, [raw_value, qualified]
                                    )
                            return out or None

                        if operation == "INSERT":
                            op, before, after = "insert", None, _mine(redo_value, columns)
                            inserts += 1
                        elif operation == "DELETE":
                            op, before, after = "delete", _mine(undo_value, columns), None
                            deletes += 1
                        else:  # UPDATE
                            op, before, after = "update", _mine(undo_value, columns), _mine(redo_value, columns)
                            updates += 1

                        events.append({
                            "table": table_name,
                            "_cdc_op": op,
                            "before": before,
                            "after": after,
                            "scn": int(scn),
                        })
                        tables_seen.add(table_name)
                        last_scn_processed = int(scn)

                        if max_events_per_tick and len(events) >= max_events_per_tick:
                            break
                finally:
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception:
                        pass
            except Exception as e:
                return SensorResult(skip_reason=f"Error reading Oracle LogMiner contents: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            new_scn = last_scn_processed if (max_events_per_tick and len(events) >= max_events_per_tick) else end_scn

            if not events:
                return SensorResult(cursor=json.dumps({"scn": end_scn}), skip_reason="No new Oracle CDC events found")

            context.log.info(
                f"Oracle CDC: {len(events)} change(s) across {len(tables_seen)} table(s) "
                f"in '{schema_name}' (insert={inserts} update={updates} delete={deletes})"
            )

            run_config = {
                "ops": {
                    op_name: {
                        "config": {
                            "schema_name": schema_name,
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
                            "schema_name": schema_name,
                            "tables": sorted(tables_seen),
                            "event_count": len(events),
                            "inserts": inserts,
                            "updates": updates,
                            "deletes": deletes,
                            "start_scn": start_scn,
                            "end_scn": new_scn,
                        },
                    )
                ]

            run_key = f"{sensor_name}-{start_scn}->{new_scn}"

            return SensorResult(
                run_requests=[RunRequest(run_key=run_key, run_config=run_config)],
                cursor=json.dumps({"scn": new_scn}),
                asset_events=asset_events,
            )

        return Definitions(sensors=[oracle_cdc_sensor])
