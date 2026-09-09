"""EventLogToBigQueryJobComponent.

Op-shaped job that streams new Dagster event log entries into a
BigQuery table via `insert_rows_json`. Ideal for BI on Dagster
reliability (Looker/Tableau on top of BQ).

Target table schema (create it manually with `bq mk` or via DDL):
  storage_id INT64
  timestamp FLOAT64
  timestamp_ts TIMESTAMP
  run_id STRING
  job_name STRING
  event_type STRING
  step_key STRING
  asset_key STRING
  message STRING
"""

import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "event_log_to_bigquery/last_storage_id"

DEFAULT_EVENT_TYPES = [
    dg.DagsterEventType.PIPELINE_START,
    dg.DagsterEventType.PIPELINE_SUCCESS,
    dg.DagsterEventType.PIPELINE_FAILURE,
    dg.DagsterEventType.STEP_FAILURE,
    dg.DagsterEventType.ASSET_MATERIALIZATION,
    dg.DagsterEventType.ASSET_OBSERVATION,
    dg.DagsterEventType.ASSET_CHECK_EVALUATION,
]


def _row(rec) -> Dict[str, Any]:
    e = rec.event_log_entry
    de = e.dagster_event
    return {
        "storage_id": rec.storage_id,
        "timestamp": e.timestamp,
        "timestamp_ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(e.timestamp)),
        "run_id": e.run_id,
        "job_name": (getattr(de, "job_name", None) if de else None) or e.job_name,
        "event_type": de.event_type_value if de else None,
        "step_key": e.step_key,
        "asset_key": e.asset_key.to_user_string() if e.asset_key else None,
        "message": e.message[:2000] if e.message else None,
    }


class EventLogToBigQueryJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that streams new Dagster event log entries into a
    BigQuery table via `insert_rows_json`."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/15 * * * *", description="Cron schedule; default every 15min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    gcp_project: str = Field(description="GCP project ID.")
    dataset: str = Field(description="BigQuery dataset name.")
    table: str = Field(default="dagster_events", description="BigQuery table name.")
    initial_lookback_hours: int = Field(default=24, ge=1, description="On first run, pull events from last N hours.")
    batch_limit: int = Field(default=5000, ge=1, description="Max events per tick.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            from google.cloud import bigquery
            client = bigquery.Client(project=_self.gcp_project)
            table_ref = f"{_self.gcp_project}.{_self.dataset}.{_self.table}"

            last_storage_id = None
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
                        last_storage_id = int(v)
                        break
            except Exception:
                pass

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
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")

            new_cursor = records[-1].storage_id
            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(new_cursor)})
            context.log.info(f"Inserted {len(rows)} rows into {table_ref}")
            return {"inserted": len(rows), "last_storage_id": new_cursor, "table": table_ref}

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
