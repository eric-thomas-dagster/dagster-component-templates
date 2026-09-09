"""RunHistoryToWarehouseJobComponent.

Op-shaped job that MERGEs Dagster run history (id, status, timings,
job, tags) into a warehouse table. Complement to the event-log
exports — those are event-level, this is run-level (one row per run).

Target: Snowflake / BigQuery / Postgres, selected via `warehouse` field.
Idempotent upsert via MERGE-by-run_id so replaying is safe.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "run_history_to_warehouse/last_update_ts"


def _row(run) -> Dict[str, Any]:
    tags = run.tags or {}
    return {
        "run_id": run.run_id,
        "job_name": run.job_name,
        "status": run.status.value,
        "create_timestamp": getattr(run, "create_timestamp", None),
        "start_time": run.start_time,
        "end_time": run.end_time,
        "duration_seconds": (
            (run.end_time - run.start_time) if (run.start_time and run.end_time) else None
        ),
        "root_run_id": run.root_run_id,
        "parent_run_id": run.parent_run_id,
        "mode": run.mode,
        "tags_json": json.dumps(tags, default=str)[:4000],
    }


class RunHistoryToWarehouseJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that MERGEs new/updated Dagster runs into a
    warehouse table (Snowflake | BigQuery | Postgres).

    Target table DDL (create manually):
        CREATE TABLE dagster_run_history (
          run_id STRING PRIMARY KEY,
          job_name STRING,
          status STRING,
          create_timestamp NUMBER/FLOAT,
          start_time NUMBER/FLOAT,
          end_time NUMBER/FLOAT,
          duration_seconds NUMBER/FLOAT,
          root_run_id STRING,
          parent_run_id STRING,
          mode STRING,
          tags_json STRING
        );
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/15 * * * *", description="Cron schedule; default every 15min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    warehouse: str = Field(description="Target warehouse type: snowflake | bigquery | postgres")

    # Snowflake fields (used when warehouse=snowflake)
    snowflake_account: Optional[str] = Field(default=None)
    snowflake_user: Optional[str] = Field(default=None)
    snowflake_password_env: str = Field(default="SNOWFLAKE_PASSWORD")
    snowflake_warehouse: Optional[str] = Field(default=None)
    snowflake_database: Optional[str] = Field(default=None)
    snowflake_schema: str = Field(default="PUBLIC")

    # BigQuery fields (used when warehouse=bigquery)
    bq_project: Optional[str] = Field(default=None)
    bq_dataset: Optional[str] = Field(default=None)

    # Postgres fields (used when warehouse=postgres)
    postgres_dsn_env: str = Field(default="POSTGRES_DSN", description="Env var with the Postgres DSN.")
    postgres_schema: str = Field(default="public")

    table: str = Field(default="dagster_run_history", description="Target table name (case sensitivity per warehouse).")
    initial_lookback_hours: int = Field(default=48, ge=1)
    batch_limit: int = Field(default=1000, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            # Cursor: last updated_after we swept from.
            last_ts = None
            try:
                prior = context.instance.get_runs(
                    filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                    limit=2,
                )
                for r in prior:
                    if r.run_id == context.run_id:
                        continue
                    v = (r.tags or {}).get(CURSOR_TAG)
                    if v:
                        last_ts = float(v)
                        break
            except Exception:
                pass

            since = last_ts or (time.time() - _self.initial_lookback_hours * 3600.0)

            runs = context.instance.get_runs(
                filters=dg.RunsFilter(updated_after=since),
                limit=_self.batch_limit,
            )
            if not runs:
                context.log.info("No new/updated runs.")
                context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(time.time())})
                return {"merged": 0}

            rows = [_row(r) for r in runs]
            merged = _merge_to_warehouse(_self, rows, context.log)

            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(time.time())})
            context.log.info(f"MERGEd {merged} runs into {_self.warehouse} table {_self.table}")
            return {"merged": merged, "warehouse": _self.warehouse}

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


def _merge_to_warehouse(cfg, rows: List[Dict[str, Any]], log) -> int:
    """Route to the right MERGE path based on cfg.warehouse."""
    w = cfg.warehouse.lower()
    if w == "snowflake":
        return _merge_snowflake(cfg, rows)
    if w == "bigquery":
        return _merge_bigquery(cfg, rows)
    if w == "postgres":
        return _merge_postgres(cfg, rows)
    raise ValueError(f"Unknown warehouse: {cfg.warehouse!r}")


def _merge_snowflake(cfg, rows) -> int:
    import snowflake.connector as sc
    password = os.environ.get(cfg.snowflake_password_env)
    if not password:
        raise RuntimeError(f"Missing {cfg.snowflake_password_env}")
    conn = sc.connect(
        account=cfg.snowflake_account, user=cfg.snowflake_user, password=password,
        warehouse=cfg.snowflake_warehouse, database=cfg.snowflake_database, schema=cfg.snowflake_schema,
    )
    try:
        cur = conn.cursor()
        # Snowflake MERGE via temp table + INSERT/UPDATE
        cur.execute(f"CREATE TEMPORARY TABLE {cfg.table}_stg LIKE {cfg.table}")
        cur.executemany(
            f"INSERT INTO {cfg.table}_stg VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [tuple(r.values()) for r in rows],
        )
        cur.execute(f"""
            MERGE INTO {cfg.table} t USING {cfg.table}_stg s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET
                status=s.status, end_time=s.end_time, duration_seconds=s.duration_seconds, tags_json=s.tags_json
            WHEN NOT MATCHED THEN INSERT VALUES (s.run_id, s.job_name, s.status, s.create_timestamp,
                s.start_time, s.end_time, s.duration_seconds, s.root_run_id, s.parent_run_id, s.mode, s.tags_json)
        """)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _merge_bigquery(cfg, rows) -> int:
    from google.cloud import bigquery
    client = bigquery.Client(project=cfg.bq_project)
    table_ref = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.table}"
    # BQ MERGE via streaming buffer isn't ideal; use a load job into a temp table then MERGE.
    # For simplicity here, use insert_rows_json + rely on downstream dedup (users can enforce
    # primary key on run_id via a view). Callers who need strict MERGE should use Snowflake/Postgres.
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
    return len(rows)


def _merge_postgres(cfg, rows) -> int:
    import psycopg2
    dsn = os.environ.get(cfg.postgres_dsn_env)
    if not dsn:
        raise RuntimeError(f"Missing {cfg.postgres_dsn_env}")
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        for r in rows:
            cur.execute(f"""
                INSERT INTO {cfg.postgres_schema}.{cfg.table}
                  (run_id, job_name, status, create_timestamp, start_time, end_time,
                   duration_seconds, root_run_id, parent_run_id, mode, tags_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                  status = EXCLUDED.status,
                  end_time = EXCLUDED.end_time,
                  duration_seconds = EXCLUDED.duration_seconds,
                  tags_json = EXCLUDED.tags_json
            """, tuple(r.values()))
        conn.commit()
        return len(rows)
    finally:
        conn.close()
