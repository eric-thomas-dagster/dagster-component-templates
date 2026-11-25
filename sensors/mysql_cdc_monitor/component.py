"""MySQL CDC (binlog) Monitor Sensor Component.

Micro-batch change-data-capture source: reads MySQL's binary log from a stored
position, classifies each row change as insert/update/delete, batches them up,
and stops once it has caught up to the server's current binlog position --
this is NOT a long-running streaming process, it's a poll-catch-up-stop
sensor like s3_monitor/kafka_monitor.
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
    """MySQL column values can be datetime/date/Decimal/bytes -- none of
    those round-trip through Dagster's run_config JSON serialization, so
    coerce them to plain str. Downstream assets that need the original
    type back must re-parse the specific columns they care about -- this
    is a real, documented limitation, not a cosmetic one.

    JSON columns decode to native dict/list in pymysqlreplication -- those
    are already fully JSON-native (MySQL's JSON type can only ever contain
    JSON-native scalars/objects/arrays, never a datetime/Decimal/bytes), so
    they pass through untouched rather than falling into the str(value)
    fallback below, which would otherwise produce a Python repr (single
    quotes) that isn't valid JSON for a downstream asset to re-parse."""
    if value is None or isinstance(value, (str, int, float, bool, dict, list)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)


def _jsonable_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    return {k: _jsonable(v) for k, v in row.items()}


class MySQLCDCMonitorSensorComponent(Component, Model, Resolvable):
    """Component for micro-batch MySQL CDC via the binary log (binlog).

    Reads MySQL's replication stream from a stored file+position (or GTID
    set), classifies each row change as insert/update/delete, batches
    everything from one tick into a single RunRequest, and stops once the
    reader reports it has caught up to the server's current binlog
    position -- it never blocks waiting for new events. Tracks exactly one
    schema per sensor instance; declare one sensor per schema if you need
    more than one, since MySQL's own replication-protocol table/schema
    filters match on bare names, not schema-qualified ones, and a single
    `database` field keeps that limitation from becoming a cross-schema
    collision bug.

    Example:
        ```yaml
        type: dagster_community_components.MySQLCDCMonitorSensorComponent
        attributes:
          sensor_name: orders_cdc_sensor
          host: db.internal
          username: repl_user
          password_env_var: MYSQL_REPL_PASSWORD
          server_id: 1001
          database: sales
          table_patterns:
            - orders
            - order_items_*
          job_name: apply_sales_cdc_job
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    host: str = Field(description="MySQL host")
    port: int = Field(default=3306, description="MySQL port")
    username: str = Field(description="MySQL username with REPLICATION SLAVE and REPLICATION CLIENT privileges")
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding the MySQL password",
    )
    ssl_disabled: bool = Field(default=False, description="Disable TLS for the binlog connection. Default False (TLS required); set True only for trusted local networks.")

    server_id: int = Field(
        description=(
            "Unique integer (1 to 2^32-1) this sensor presents to the MySQL "
            "server as a replication client id. Must not collide with any "
            "other replica or binlog consumer connected to this server -- "
            "including another instance of this same sensor, if you ever "
            "run more than one against the same database."
        ),
    )

    database: str = Field(
        description=(
            "Which MySQL schema/database to track. This sensor tracks "
            "exactly ONE schema -- declare one sensor per schema if you "
            "need more than one. (MySQL's own binlog-protocol table/schema "
            "filters match on bare names, not schema-qualified ones, so "
            "tracking several schemas from one sensor risks a table-name "
            "collision silently matching the wrong schema's table.)"
        ),
    )

    table_patterns: List[str] = Field(
        description=(
            "fnmatch-style glob patterns (e.g. 'orders', 'order_items_*') "
            "matched against bare table names within `database`. Resolved "
            "against INFORMATION_SCHEMA.TABLES at the start of every tick "
            "(not cached), so a new table matching a wildcard is picked up "
            "automatically on its next tick -- no restart required."
        ),
    )

    job_name: str = Field(description="Name of the job to trigger when changes are detected")

    minimum_interval_seconds: int = Field(default=30, description="Minimum time (in seconds) between sensor evaluations")

    max_events_per_tick: Optional[int] = Field(
        default=None,
        description=(
            "Caps how many row-change events this sensor processes in one "
            "tick, even if more remain before it would otherwise report "
            "'caught up'. Protects a single run from an enormous backlog "
            "(e.g. first run against a server with days of retained "
            "binlog). Unset (default): no cap, processes everything "
            "available until caught up. When the cap is hit, position is "
            "persisted only through the last event actually processed -- "
            "the remainder is picked up on the next tick."
        ),
    )

    position_mode: str = Field(
        default="binlog",
        description=(
            "'binlog' (default) -- resume from a stored binlog file+position. "
            "'gtid' -- resume from a stored GTID set via the server's "
            "auto-position mechanism. Requires gtid_mode=ON and "
            "enforce_gtid_consistency=ON on the MySQL server -- this "
            "sensor does NOT auto-detect whether GTID mode is enabled; "
            "selecting 'gtid' against a server without it fails loudly "
            "via skip_reason instead of silently falling back."
        ),
    )

    on_first_run: str = Field(
        default="start_now",
        description=(
            "'start_now' (default) -- skip all historical binlog and start "
            "capturing changes from the server's CURRENT position on the "
            "very first tick. Pair with a separate one-time snapshot/full-"
            "load of existing rows (e.g. this registry's own full-table "
            "ingestion components) -- this sensor is the ongoing-changes "
            "half of a CDC pipeline, not an initial-load mechanism. "
            "'from_beginning' -- start from the EARLIEST binlog file the "
            "server still retains. Fails loudly if that file has already "
            "aged out under the server's binlog retention settings -- "
            "rarely useful outside small/test servers with long retention."
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
            "tables, event_count, inserts, updates, deletes, log_file, "
            "log_pos."
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
        return ComponentFormConfig(label="MySQL CDC Monitor Sensor", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        host = self.host
        port = self.port
        username = self.username
        password_env_var = self.password_env_var
        ssl_disabled = self.ssl_disabled
        server_id = self.server_id
        database = self.database
        table_patterns = list(self.table_patterns or [])
        job_name = self.job_name
        minimum_interval_seconds = self.minimum_interval_seconds
        max_events_per_tick = self.max_events_per_tick
        position_mode = (self.position_mode or "binlog").lower()
        on_first_run = (self.on_first_run or "start_now").lower()
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key
        default_status_str = self.default_status

        if position_mode not in ("binlog", "gtid"):
            raise ValueError(f"position_mode must be 'binlog' | 'gtid', got {position_mode!r}")
        if on_first_run not in ("start_now", "from_beginning"):
            raise ValueError(f"on_first_run must be 'start_now' | 'from_beginning', got {on_first_run!r}")
        if not table_patterns:
            raise ValueError("table_patterns must list at least one pattern")

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
        def mysql_cdc_sensor(context: SensorEvaluationContext):
            import os

            try:
                import pymysql
                from pymysqlreplication import BinLogStreamReader
                from pymysqlreplication.event import GtidEvent
                from pymysqlreplication.gtid import Gtid, GtidSet
                from pymysqlreplication.row_event import (
                    DeleteRowsEvent,
                    PartialUpdateRowsEvent,
                    UpdateRowsEvent,
                    WriteRowsEvent,
                )
            except ImportError:
                return SensorResult(
                    skip_reason="mysql-replication is not installed. Run: pip install mysql-replication"
                )

            password = os.environ.get(password_env_var, "") if password_env_var else ""

            connection_settings = {
                "host": host,
                "port": port,
                "user": username,
                "password": password,
                "ssl_disabled": ssl_disabled,
            }

            # Resolve table_patterns -> a concrete, exact-name list every tick (not
            # cached) by querying INFORMATION_SCHEMA directly -- the replication
            # protocol's own only_tables/only_schemas filters are exact-match only,
            # they don't understand globs.
            try:
                info_conn = pymysql.connect(
                    host=host, port=port, user=username, password=password,
                    database="information_schema", ssl_disabled=ssl_disabled,
                )
                try:
                    with info_conn.cursor() as cur:
                        cur.execute(
                            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s",
                            (database,),
                        )
                        all_tables = [row[0] for row in cur.fetchall()]
                finally:
                    info_conn.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Failed to resolve tables in schema '{database}': {e}")

            resolved_tables = sorted({
                t for t in all_tables if any(fnmatch.fnmatch(t, p) for p in table_patterns)
            })
            if not resolved_tables:
                return SensorResult(
                    skip_reason=f"No tables in schema '{database}' matched table_patterns {table_patterns}"
                )

            # ── Stored position ──────────────────────────────────────────
            cursor_data = {}
            if context.cursor:
                try:
                    cursor_data = json.loads(context.cursor)
                except json.JSONDecodeError:
                    context.log.warning("Could not parse cursor, treating as first run")

            # GtidEvent precedes every transaction and is only needed in gtid
            # mode, to let us advance the stored GTID set ourselves -- the
            # reader's own `auto_position` attribute is write-once (the value
            # it was constructed with) and is NEVER updated as transactions are
            # consumed, unlike log_file/log_pos. Tracking GtidEvents and
            # merging them via GtidSet.merge_gtid (pymysqlreplication's own
            # range-merging logic, not hand-rolled) is what makes gtid mode
            # correct instead of silently reprocessing the same transactions
            # forever.
            # PartialUpdateRowsEvent is a DISTINCT wire event type (MySQL
            # 8.0.18+'s binlog_row_value_options=PARTIAL_JSON optimization for
            # JSON-column updates) dispatched to its own class -- the
            # reader's only_events filter matches by exact __class__, not
            # isinstance/subclass, so omitting it here would silently DROP
            # every partial-JSON update on a server with that option enabled,
            # even though our classification below (which checks
            # isinstance(..., UpdateRowsEvent)) already handles it correctly
            # once it's actually let through.
            only_events = [WriteRowsEvent, UpdateRowsEvent, PartialUpdateRowsEvent, DeleteRowsEvent]
            if position_mode == "gtid":
                only_events.append(GtidEvent)

            reader_kwargs = dict(
                connection_settings=connection_settings,
                server_id=server_id,
                only_schemas=[database],
                only_tables=resolved_tables,
                only_events=only_events,
                blocking=False,  # catch-up-and-stop, not a long-running stream
                resume_stream=True,
            )

            gtid_set = None
            if position_mode == "gtid":
                stored_gtid = cursor_data.get("gtid")
                gtid_set = GtidSet(stored_gtid) if stored_gtid else GtidSet(None)
                if stored_gtid:
                    reader_kwargs["auto_position"] = stored_gtid
                # else: first run -- pymysqlreplication/the server resolve the
                # starting GTID set itself when auto_position is omitted.
            else:
                stored_log_file = cursor_data.get("log_file")
                stored_log_pos = cursor_data.get("log_pos")
                if stored_log_file and stored_log_pos:
                    reader_kwargs["log_file"] = stored_log_file
                    reader_kwargs["log_pos"] = stored_log_pos
                elif on_first_run == "from_beginning":
                    try:
                        ctl_conn = pymysql.connect(
                            host=host, port=port, user=username, password=password,
                            ssl_disabled=ssl_disabled,
                        )
                        try:
                            with ctl_conn.cursor() as cur:
                                cur.execute("SHOW BINARY LOGS")
                                logs = cur.fetchall()
                        finally:
                            ctl_conn.close()
                    except Exception as e:
                        return SensorResult(skip_reason=f"Failed to list binary logs: {e}")
                    if not logs:
                        return SensorResult(skip_reason="Server reports no binary logs available")
                    reader_kwargs["log_file"] = logs[0][0]
                    reader_kwargs["log_pos"] = 4  # first event in any binlog file follows the 4-byte magic number
                # else: on_first_run == "start_now" (default) -- leave log_file/
                # log_pos unset; the reader resolves to the server's CURRENT
                # position itself (SHOW BINARY LOG STATUS / SHOW MASTER STATUS).

            # Constructing BinLogStreamReader never actually connects --
            # pymysqlreplication connects lazily on the first fetchone() call
            # (inside the iteration below), so the purged-position error
            # (MySQL 1236, "Could not find first log file name in binary log
            # index file") can only ever surface from the loop, never from
            # this constructor call. It has no dedicated exception class --
            # it's a generic pymysql.OperationalError -- so we inspect the
            # structured error code ourselves (same idiom
            # pymysqlreplication's own __show_binary_log_status uses for its
            # ER_PARSE_ERROR check) rather than string-matching the message.
            try:
                stream = BinLogStreamReader(**reader_kwargs)
            except Exception as e:
                return SensorResult(skip_reason=f"Failed to construct MySQL binlog stream reader: {e}")

            events = []
            inserts = updates = deletes = 0
            tables_seen = set()
            starting_log_file = reader_kwargs.get("log_file")
            starting_log_pos = reader_kwargs.get("log_pos")
            new_log_file = starting_log_file
            new_log_pos = starting_log_pos

            try:
                for row_event in stream:
                    if isinstance(row_event, GtidEvent):
                        gtid_set.merge_gtid(Gtid(row_event.gtid))
                        continue

                    for row in row_event.rows:
                        if isinstance(row_event, WriteRowsEvent):
                            op, before, after = "insert", None, row.get("values")
                            inserts += 1
                        elif isinstance(row_event, UpdateRowsEvent):
                            op, before, after = "update", row.get("before_values"), row.get("after_values")
                            updates += 1
                        else:  # DeleteRowsEvent
                            op, before, after = "delete", row.get("values"), None
                            deletes += 1

                        events.append({
                            "table": row_event.table,
                            "_cdc_op": op,
                            "before": _jsonable_row(before),
                            "after": _jsonable_row(after),
                            "log_file": stream.log_file,
                            "log_pos": stream.log_pos,
                        })
                        tables_seen.add(row_event.table)

                    new_log_file = stream.log_file
                    new_log_pos = stream.log_pos

                    if max_events_per_tick and len(events) >= max_events_per_tick:
                        break
            except pymysql.err.OperationalError as e:
                stream.close()
                if e.args and e.args[0] == 1236:
                    return SensorResult(
                        skip_reason=(
                            f"Stored binlog position has been purged from the server "
                            f"(MySQL error 1236: {e}). This sensor's stored position is "
                            f"too old -- re-snapshot the tracked tables and reset this "
                            f"sensor's cursor to resume cleanly; it will NOT silently "
                            f"skip the gap."
                        )
                    )
                return SensorResult(skip_reason=f"Error reading MySQL binlog stream: {e}")
            except Exception as e:
                stream.close()
                return SensorResult(skip_reason=f"Error reading MySQL binlog stream: {e}")

            stream.close()

            new_cursor_data = dict(cursor_data)
            if position_mode == "gtid":
                # Every GtidEvent seen this tick was merged into gtid_set above
                # via GtidSet.merge_gtid (pymysqlreplication's own range-merging
                # logic) -- this is the advancing position, unlike the
                # reader's own `auto_position` attribute, which never changes
                # from what it was constructed with.
                new_cursor_data["gtid"] = str(gtid_set) if gtid_set and gtid_set.gtids else cursor_data.get("gtid")
            else:
                new_cursor_data["log_file"] = new_log_file
                new_cursor_data["log_pos"] = new_log_pos

            if not events:
                return SensorResult(cursor=json.dumps(new_cursor_data), skip_reason="No new MySQL CDC events found")

            context.log.info(
                f"MySQL CDC: {len(events)} change(s) across {len(tables_seen)} table(s) "
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
                            "log_file": new_log_file or "",
                            "log_pos": new_log_pos or 0,
                        },
                    )
                ]

            # Deterministic on the tick's START->END position range (not wall
            # time) so a sensor-evaluation retry of the SAME tick -- before its
            # cursor commit lands -- dedupes against the original instead of
            # double-launching a run for changes already queued.
            start_marker = reader_kwargs.get("auto_position") or f"{starting_log_file}:{starting_log_pos}"
            end_marker = new_cursor_data.get("gtid") or f"{new_log_file}:{new_log_pos}"
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

        return Definitions(sensors=[mysql_cdc_sensor])
