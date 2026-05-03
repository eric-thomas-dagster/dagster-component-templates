"""DagsterPlusToSiemJobComponent.

Compound op job: pull Dagster+ events (audit-log/runs/asset-events) → optionally normalize → ship to SIEM (Splunk/Sentinel/Datadog/Sumo/S3) on cron.

Compound op job — pull Dagster+ → optional OCSF/ECS normalize → ship to SIEM,
all from one YAML config. No assets materialized; clean lineage graph.

Pull stage uses the same GraphQL pattern as the standalone Dagster+ ingestion
components. The default query is a best-guess — validate against your live
schema.
"""

import json as _json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


DEFAULT_QUERIES = {
    "audit_log": """query DagsterPlusAuditLogs($limit: Int, $cursor: String, $filters: AuditLogFilters) {
  auditLogs(limit: $limit, cursor: $cursor, filters: $filters) {
    entries {
      timestamp
      userEmail
      eventType
      targetType
      targetIdentifier
      metadata
    }
    cursor
    hasMore
  }
}""",
    "runs": """query RunsList($cursor: String, $limit: Int, $startTime: Float, $endTime: Float) {
  runsOrError(filter: {createdBefore: $endTime, createdAfter: $startTime}, cursor: $cursor, limit: $limit) {
    ... on Runs {
      results {
        runId
        status
        startTime
        endTime
        creationTime
        pipelineName
        jobName
        mode
        runConfigYaml
        tags { key value }
      }
    }
  }
}""",
    "asset_events": """query AssetEvents($cursor: String, $limit: Int) {
  assetsOrError(cursor: $cursor, limit: $limit) {
    ... on AssetConnection {
      nodes {
        key { path }
        latestEventSortKey
        latestRunForPartition(partition: null) { runId status }
      }
    }
  }
}""",
}

DEFAULT_RESULT_PATHS = {
    "audit_log": "auditLogs.entries",
    "runs": "runsOrError.results",
    "asset_events": "assetsOrError.nodes",
}


class DagsterPlusToSiemJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Compound op job: pull Dagster+ events (audit-log/runs/asset-events) → optionally normalize → ship to SIEM (Splunk/Sentinel/Datadog/Sumo/S3) on cron."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")

    # Source — Dagster+
    endpoint_url: str = Field(
        description="Dagster+ GraphQL endpoint — US: 'https://<org>.dagster.cloud/<deployment>/graphql', EU: 'https://<org>.eu.dagster.cloud/<deployment>/graphql'"
    )
    user_token_env: str = Field(default="DAGSTER_PLUS_USER_TOKEN")
    event_type: str = Field(default="audit_log", description="audit_log | runs | asset_events")
    query: Optional[str] = Field(default=None, description="Custom GraphQL query — overrides the default for the chosen event_type")
    result_path: Optional[str] = Field(default=None, description="Override JSON path to records (defaults match the event_type)")
    page_size: int = Field(default=100, description="Per-page limit (max 100 for Dagster+ audit log)")
    lookback_minutes: int = Field(default=15, description="Stop pagination once entry timestamps fall outside this window")

    # Audit-log filters (only used when event_type=audit_log)
    event_types: Optional[list[str]] = Field(default=None, description="AuditLog event-type codes (e.g. ['LOG_IN'])")
    user_emails: Optional[list[str]] = Field(default=None, description="Filter to events by these user emails")
    deployment_names: Optional[list[str]] = Field(default=None, description="Filter to events from these deployment names")
    timestamp_field: str = Field(default="timestamp", description="Entry field used for the lookback cutoff")

    # Normalize
    normalize_to: str = Field(default="ocsf", description="ocsf | ecs | none")

    # Sink
    sink: str = Field(description="splunk | sentinel | datadog_logs | sumo_logic | s3")
    sink_config: dict = Field(default_factory=dict, description="Sink-specific config — see README")

    # Failure handling
    fail_on_zero_events: bool = Field(default=False)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        import datetime as dt

        def _get(obj, path):
            if not path: return None
            for key in path.split("."):
                if isinstance(obj, dict): obj = obj.get(key)
                else: return None
                if obj is None: return None
            return obj

        def _entry_epoch(entry, field):
            ts = entry.get(field)
            if ts is None:
                return None
            if isinstance(ts, (int, float)):
                return ts / 1000.0 if ts > 1e12 else float(ts)
            try:
                parsed = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                return parsed.timestamp()
            except Exception:
                return None

        def _fetch(context):
            import requests
            token = os.environ[_self.user_token_env]
            headers = {"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"}
            query = _self.query or DEFAULT_QUERIES.get(_self.event_type)
            if not query:
                raise ValueError(f"unknown event_type: {_self.event_type}")
            result_path = _self.result_path or DEFAULT_RESULT_PATHS.get(_self.event_type)

            # Build the per-event-type variables payload.
            base_vars: dict = {"limit": _self.page_size}
            if _self.event_type == "audit_log":
                filters: dict = {}
                if _self.event_types: filters["eventTypes"] = _self.event_types
                if _self.user_emails: filters["userEmails"] = _self.user_emails
                if _self.deployment_names: filters["deploymentNames"] = _self.deployment_names
                base_vars["filters"] = filters
            else:
                # For runs / asset_events the schema is best-guess — keep startTime/endTime
                # as a hint that downstream queries may want a time window.
                end = dt.datetime.utcnow()
                start = end - dt.timedelta(minutes=_self.lookback_minutes)
                base_vars["startTime"] = start.timestamp()
                base_vars["endTime"] = end.timestamp()

            cutoff_epoch = (
                dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=_self.lookback_minutes)
            ).timestamp()

            all_records, cursor, page_count, stopped_early = [], None, 0, False
            while True:
                page_count += 1
                vars_payload = dict(base_vars)
                vars_payload["cursor"] = cursor
                r = requests.post(_self.endpoint_url, json={"query": query, "variables": vars_payload}, headers=headers, timeout=120)
                r.raise_for_status()
                body = r.json()
                if "errors" in body:
                    raise Exception(f"Dagster+ GraphQL errors: {body['errors']}")
                data = body.get("data") or {}
                page_records = _get(data, result_path) or []
                for entry in page_records:
                    if _self.event_type == "audit_log" and isinstance(entry, dict):
                        e = _entry_epoch(entry, _self.timestamp_field)
                        if e is not None and e < cutoff_epoch:
                            stopped_early = True
                            break
                    all_records.append(entry)
                next_cursor = _get(data, "auditLogs.cursor") if _self.event_type == "audit_log" else None
                has_more = _get(data, "auditLogs.hasMore") if _self.event_type == "audit_log" else False
                if stopped_early or not next_cursor or not has_more:
                    break
                if page_count >= 100:
                    context.log.warning("hit 100-page safety limit; stopping")
                    break
                cursor = next_cursor

            df = pd.DataFrame(all_records)
            context.log.info(f"fetched {len(df)} {_self.event_type} from Dagster+ in {page_count} page(s)")
            if len(df) == 0 and _self.fail_on_zero_events:
                raise Exception("no events; fail_on_zero_events=True")
            return df

        def _normalize_ocsf(df):
            # Best-effort mapping for Dagster+ events
            out = pd.DataFrame()
            ts_col = next((c for c in df.columns if c.lower() in ("timestamp", "creationtime", "starttime", "occurredat")), None)
            actor_col = next((c for c in df.columns if c.lower() in ("useremail", "actor", "createdby")), None)
            action_col = next((c for c in df.columns if c.lower() in ("eventtype", "status", "operationname")), None)
            out["time"] = df[ts_col] if ts_col and ts_col in df.columns else None
            out["activity_name"] = df[action_col] if action_col and action_col in df.columns else None
            out["actor.user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
            out["metadata.product.vendor_name"] = "Dagster"
            out["metadata.product.name"] = "Dagster+"
            out["raw_data"] = df.apply(lambda r: r.to_json(), axis=1)
            return out

        def _normalize_ecs(df):
            out = pd.DataFrame()
            ts_col = next((c for c in df.columns if c.lower() in ("timestamp", "creationtime", "starttime")), None)
            actor_col = next((c for c in df.columns if c.lower() in ("useremail", "actor")), None)
            action_col = next((c for c in df.columns if c.lower() in ("eventtype", "status")), None)
            out["@timestamp"] = df[ts_col] if ts_col and ts_col in df.columns else None
            out["event.action"] = df[action_col] if action_col and action_col in df.columns else None
            out["user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
            out["event.dataset"] = "dagster_plus"
            out["event.original"] = df.apply(lambda r: r.to_json(), axis=1)
            return out

        def _ship_splunk(df, cfg, log):
            import requests
            url = cfg["hec_url"]
            token = os.environ[cfg.get("hec_token_env", "SPLUNK_HEC_TOKEN")]
            payload = "\n".join(_json.dumps({
                "event": _json.loads(row.to_json()),
                **({"index": cfg["index"]} if cfg.get("index") else {}),
                **({"sourcetype": cfg["sourcetype"]} if cfg.get("sourcetype") else {}),
                "source": cfg.get("source", "dagster+"),
            }) for _, row in df.iterrows())
            r = requests.post(url, data=payload, headers={"Authorization": f"Splunk {token}"},
                              verify=cfg.get("verify_ssl", True), timeout=60)
            if r.status_code >= 300:
                raise Exception(f"splunk: {r.status_code} {r.text[:200]}")

        def _ship_sentinel(df, cfg, log):
            import requests, hashlib, hmac, base64
            workspace_id = cfg["workspace_id"]
            key = os.environ[cfg.get("workspace_key_env", "SENTINEL_WORKSPACE_KEY")]
            log_type = cfg.get("log_type", "DagsterPlusAudit")
            body = df.to_json(orient="records")
            now = dt.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            content_length = len(body)
            string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{now}\n/api/logs"
            decoded = base64.b64decode(key)
            sig = base64.b64encode(hmac.new(decoded, string_to_hash.encode(), hashlib.sha256).digest()).decode()
            auth = f"SharedKey {workspace_id}:{sig}"
            url = f"https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
            r = requests.post(url, data=body, headers={
                "content-type": "application/json", "Authorization": auth,
                "Log-Type": log_type, "x-ms-date": now,
            }, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"sentinel: {r.status_code} {r.text[:200]}")

        def _ship_datadog(df, cfg, log):
            import requests
            api_key = os.environ[cfg.get("api_key_env", "DD_API_KEY")]
            site = cfg.get("site", "datadoghq.com")
            url = f"https://http-intake.logs.{site}/api/v2/logs"
            payload = [{"message": row.to_json(), "service": cfg.get("service", "dagster-plus"), "ddsource": "dagster"} for _, row in df.iterrows()]
            r = requests.post(url, json=payload, headers={"DD-API-KEY": api_key}, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"datadog: {r.status_code} {r.text[:200]}")

        def _ship_sumo_logic(df, cfg, log):
            import requests
            url = os.environ[cfg.get("collector_url_env", "SUMO_HTTP_URL")]
            body = "\n".join(row.to_json() for _, row in df.iterrows())
            r = requests.post(url, data=body, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"sumo: {r.status_code} {r.text[:200]}")

        def _ship_s3(df, cfg, log):
            import boto3, io
            sess = boto3.Session(profile_name=cfg.get("aws_profile"), region_name=cfg.get("region_name", "us-east-1"))
            s3 = sess.client("s3")
            buf = io.BytesIO(df.to_json(orient="records").encode())
            stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            s3.upload_fileobj(buf, cfg["bucket"], f"{cfg.get('prefix', '')}{stamp}.json")

        SINK_FNS = {
            "splunk": _ship_splunk, "sentinel": _ship_sentinel,
            "datadog_logs": _ship_datadog, "sumo_logic": _ship_sumo_logic,
            "s3": _ship_s3,
        }

        @dg.op
        def _fetch_op(context):
            return _fetch(context)

        @dg.op
        def _normalize_op(context, df: pd.DataFrame) -> pd.DataFrame:
            if _self.normalize_to == "none" or len(df) == 0:
                return df
            if _self.normalize_to == "ocsf":
                return _normalize_ocsf(df)
            if _self.normalize_to == "ecs":
                return _normalize_ecs(df)
            return df

        @dg.op
        def _ship_op(context, df: pd.DataFrame):
            if len(df) == 0:
                context.log.info("nothing to ship — skipping")
                return
            fn = SINK_FNS.get(_self.sink)
            if not fn:
                raise ValueError(f"unknown sink: {_self.sink}")
            fn(df, _self.sink_config, context.log)
            context.log.info(f"shipped {len(df)} events to {_self.sink}")

        @dg.job(name=self.job_name, tags={"compound": "dagster_plus_to_siem"})
        def _the_job():
            _ship_op(_normalize_op(_fetch_op()))

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
