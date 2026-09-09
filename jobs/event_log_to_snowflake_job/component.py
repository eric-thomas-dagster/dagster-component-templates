"""EventLogToSnowflakeJobComponent.

Op-shaped job that streams new Dagster event log entries into a
Snowflake table via `snowflake-connector-python` INSERT-batches.

Target table DDL (create once, manually):
    CREATE TABLE dagster_events (
      storage_id NUMBER,
      timestamp NUMBER,
      timestamp_ts TIMESTAMP_NTZ,
      run_id STRING,
      job_name STRING,
      event_type STRING,
      step_key STRING,
      asset_key STRING,
      message STRING
    );

Auth: username + password (from env) OR key-pair (private_key_env). Users
who need SSO / OAuth should extend this or use the sink asset alternative.
"""

import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "event_log_to_snowflake/last_storage_id"

DEFAULT_EVENT_TYPES = [
    dg.DagsterEventType.PIPELINE_START,
    dg.DagsterEventType.PIPELINE_SUCCESS,
    dg.DagsterEventType.PIPELINE_FAILURE,
    dg.DagsterEventType.STEP_FAILURE,
    dg.DagsterEventType.ASSET_MATERIALIZATION,
    dg.DagsterEventType.ASSET_OBSERVATION,
    dg.DagsterEventType.ASSET_CHECK_EVALUATION,
]


def _row(rec) -> tuple:
    e = rec.event_log_entry
    de = e.dagster_event
    return (
        rec.storage_id,
        e.timestamp,
        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(e.timestamp)),
        e.run_id,
        (getattr(de, "job_name", None) if de else None) or e.job_name,
        de.event_type_value if de else None,
        e.step_key,
        e.asset_key.to_user_string() if e.asset_key else None,
        e.message[:2000] if e.message else None,
    )


class EventLogToSnowflakeJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that streams new Dagster event log entries into a
    Snowflake table via INSERT batches."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/15 * * * *", description="Cron schedule; default every 15min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    account: str = Field(description="Snowflake account identifier (e.g. abc12345.us-east-1).")
    user: str = Field(description="Snowflake username.")
    password_env: str = Field(default="SNOWFLAKE_PASSWORD", description="Env var with password.")
    warehouse: str = Field(description="Snowflake warehouse.")
    database: str = Field(description="Snowflake database.")
    snowflake_schema: str = Field(default="PUBLIC", description="Snowflake schema.")
    table: str = Field(default="DAGSTER_EVENTS", description="Target table name.")
    role: Optional[str] = Field(default=None, description="Optional role.")

    initial_lookback_hours: int = Field(default=24, ge=1, description="On first run, pull events from last N hours.")
    batch_limit: int = Field(default=5000, ge=1, description="Max events per tick.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            import os
            import snowflake.connector as sc

            password = os.environ.get(_self.password_env)
            if not password:
                raise RuntimeError(f"Missing {_self.password_env} env var")

            conn_kwargs = dict(
                account=_self.account, user=_self.user, password=password,
                warehouse=_self.warehouse, database=_self.database, schema=_self.snowflake_schema,
            )
            if _self.role:
                conn_kwargs["role"] = _self.role
            conn = sc.connect(**conn_kwargs)

            try:
                # Cursor from prior run.
                last_storage_id = None
                prior = context.instance.get_runs(
                    filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                    limit=2,
                )
                for r in prior:
                    if r.run_id == context.run_id:
                        continue
                    v = (r.tags or {}).get(CURSOR_TAG)
                    if v:
                        last_storage_id = int(v)
                        break

                filter_kwargs: Dict[str, Any] = {}
                if last_storage_id is not None:
                    filter_kwargs["after_cursor"] = last_storage_id
                else:
                    filter_kwargs["after_timestamp"] = time.time() - _self.initial_lookback_hours * 3600.0

                records: List[Any] = []
                for et in DEFAULT_EVENT_TYPES:
                    try:
                        batch = context.instance.get_event_records(
                            event_records_filter=dg.EventRecordsFilter(event_type=et, **filter_kwargs),
                            limit=_self.batch_limit,
                            ascending=True,
                        )
                        records.extend(batch)
                    except Exception as exc:  # noqa: BLE001
                        context.log.warning(f"Failed to query {et}: {exc}")

                records.sort(key=lambda r: r.storage_id)
                records = records[:_self.batch_limit]

                if not records:
                    context.log.info("No new events since last run.")
                    context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(last_storage_id or 0)})
                    return {"inserted": 0, "last_storage_id": last_storage_id}

                rows = [_row(r) for r in records]
                cur = conn.cursor()
                cur.executemany(
                    f"INSERT INTO {_self.table} "
                    "(storage_id, timestamp, timestamp_ts, run_id, job_name, event_type, step_key, asset_key, message) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    rows,
                )
                conn.commit()

                new_cursor = records[-1].storage_id
                context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(new_cursor)})
                context.log.info(f"Inserted {len(rows)} rows into {_self.database}.{_self.snowflake_schema}.{_self.table}")
                return {"inserted": len(rows), "last_storage_id": new_cursor}
            finally:
                conn.close()

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _export()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)
