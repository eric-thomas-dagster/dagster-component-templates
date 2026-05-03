"""SiemAuditExportJobComponent.

Compound op job: pull audit logs from a SaaS source, normalize to OCSF/ECS, ship to a SIEM — all in one YAML config.

Compound op-job — wires multiple ops + an optional schedule from one YAML.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class SiemAuditExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Compound op job: pull audit logs from a SaaS source, normalize to OCSF/ECS, ship to a SIEM — all in one YAML config."""


    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (e.g. '*/15 * * * *') — None = no schedule")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")

    # Source
    source: str = Field(description="okta | aws_cloudtrail | github | azure_activity | slack | auth0 | snowflake | generic_http")
    source_config: dict = Field(default_factory=dict, description="Source-specific config (see README)")
    lookback_minutes: int = Field(default=15, description="How far back to fetch on each run")

    # Normalize
    normalize_to: str = Field(default="ocsf", description="ocsf | ecs | none")

    # Sink
    sink: str = Field(description="splunk | sentinel | datadog_logs | sumo_logic | elastic | qradar | chronicle | s3")
    sink_config: dict = Field(default_factory=dict, description="Sink-specific config (see README)")

    # Failure
    fail_on_zero_events: bool = Field(default=False, description="Fail the job when 0 events were returned")


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # The compound is built inline — three @op stages wired into one @job.
        # Source stage produces a DataFrame; normalize transforms it; sink ships it.
        # No assets materialized — pure procedural pipeline.
        import datetime as dt
        import os
        import json as _json

        def _fetch_okta(cfg, end, start):
            import requests
            domain = cfg["okta_domain"]
            token = os.environ[cfg.get("api_token_env", "OKTA_API_TOKEN")]
            r = requests.get(f"https://{domain}/api/v1/logs",
                             params={"since": start.isoformat() + "Z", "until": end.isoformat() + "Z", "limit": 1000},
                             headers={"Authorization": f"SSWS {token}"}, timeout=60)
            r.raise_for_status()
            return r.json()

        def _fetch_cloudtrail(cfg, end, start):
            import boto3
            sess = boto3.Session(profile_name=cfg.get("aws_profile"), region_name=cfg.get("region_name", "us-east-1"))
            client = sess.client("cloudtrail")
            paginator = client.get_paginator("lookup_events")
            events = []
            for page in paginator.paginate(StartTime=start, EndTime=end):
                events.extend(page.get("Events", []))
            return events

        def _fetch_github(cfg, end, start):
            import requests
            org = cfg["org"]
            token = os.environ[cfg.get("token_env", "GITHUB_TOKEN")]
            r = requests.get(f"https://api.github.com/orgs/{org}/audit-log",
                             params={"phrase": f"created:>{start.isoformat()}Z", "per_page": 100},
                             headers={"Authorization": f"Bearer {token}"}, timeout=60)
            r.raise_for_status()
            return r.json()

        def _fetch_generic_http(cfg, end, start):
            import requests
            r = requests.get(cfg["url"], headers=cfg.get("headers", {}), params=cfg.get("params", {}), timeout=60)
            r.raise_for_status()
            return r.json() if r.headers.get("content-type", "").startswith("application/json") else []

        SOURCE_FNS = {
            "okta": _fetch_okta,
            "aws_cloudtrail": _fetch_cloudtrail,
            "github": _fetch_github,
            "generic_http": _fetch_generic_http,
        }

        def _normalize_ocsf(df, source):
            ts_keys = {"okta": "published", "aws_cloudtrail": "EventTime", "github": "@timestamp"}
            actor_keys = {"okta": "actor.alternateId", "aws_cloudtrail": "Username", "github": "actor"}
            action_keys = {"okta": "eventType", "aws_cloudtrail": "EventName", "github": "action"}
            ts = ts_keys.get(source)
            actor = actor_keys.get(source)
            action = action_keys.get(source)
            out = pd.DataFrame()
            out["time"] = df[ts] if ts and ts in df.columns else None
            out["activity_name"] = df[action] if action and action in df.columns else None
            out["actor.user.name"] = df[actor] if actor and actor in df.columns else None
            out["metadata.product.vendor_name"] = source
            out["raw_data"] = df.apply(lambda r: r.to_json(), axis=1)
            return out

        def _ship_splunk(df, cfg, log):
            import requests
            url = cfg["hec_url"]
            token = os.environ[cfg.get("hec_token_env", "SPLUNK_HEC_TOKEN")]
            payload = "\n".join(_json.dumps({"event": _json.loads(row.to_json())}) for _, row in df.iterrows())
            r = requests.post(url, data=payload, headers={"Authorization": f"Splunk {token}"},
                              verify=cfg.get("verify_ssl", True), timeout=60)
            if r.status_code >= 300:
                raise Exception(f"splunk: {r.status_code} {r.text[:200]}")

        def _ship_datadog(df, cfg, log):
            import requests
            api_key = os.environ[cfg.get("api_key_env", "DD_API_KEY")]
            site = cfg.get("site", "datadoghq.com")
            url = f"https://http-intake.logs.{site}/api/v2/logs"
            payload = [{"message": row.to_json(), "service": cfg.get("service", "dagster-audit"), "ddsource": "dagster"}
                       for _, row in df.iterrows()]
            r = requests.post(url, json=payload, headers={"DD-API-KEY": api_key}, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"datadog: {r.status_code} {r.text[:200]}")

        def _ship_s3(df, cfg, log):
            import boto3, io
            sess = boto3.Session(profile_name=cfg.get("aws_profile"), region_name=cfg.get("region_name", "us-east-1"))
            s3 = sess.client("s3")
            buf = io.BytesIO(df.to_json(orient="records").encode())
            stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            s3.upload_fileobj(buf, cfg["bucket"], f"{cfg.get('prefix', '')}{stamp}.json")

        def _ship_sumo_logic(df, cfg, log):
            import requests
            url = os.environ[cfg.get("collector_url_env", "SUMO_HTTP_URL")]
            body = "\n".join(row.to_json() for _, row in df.iterrows())
            r = requests.post(url, data=body, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"sumo: {r.status_code} {r.text[:200]}")

        SINK_FNS = {
            "splunk": _ship_splunk,
            "datadog_logs": _ship_datadog,
            "s3": _ship_s3,
            "sumo_logic": _ship_sumo_logic,
        }

        # Build the ops + job
        _self = self

        @dg.op
        def _fetch(context):
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(minutes=_self.lookback_minutes)
            fn = SOURCE_FNS.get(_self.source)
            if not fn:
                raise ValueError(f"unknown source: {_self.source}")
            events = fn(_self.source_config, end, start)
            df = pd.DataFrame(events)
            context.log.info(f"{_self.source}: {len(df)} events")
            if len(df) == 0 and _self.fail_on_zero_events:
                raise Exception("no events returned and fail_on_zero_events=True")
            return df

        @dg.op
        def _normalize(context, df: pd.DataFrame) -> pd.DataFrame:
            if _self.normalize_to == "none":
                return df
            if _self.normalize_to == "ocsf":
                return _normalize_ocsf(df, _self.source)
            return df  # ECS support stub — extend as needed

        @dg.op
        def _ship(context, df: pd.DataFrame):
            if len(df) == 0:
                context.log.info("nothing to ship — skipping")
                return
            fn = SINK_FNS.get(_self.sink)
            if not fn:
                raise ValueError(f"unknown sink: {_self.sink}")
            fn(df, _self.sink_config, context.log)
            context.log.info(f"shipped {len(df)} events to {_self.sink}")

        @dg.job(name=self.job_name, tags={"compound": "siem_audit_export"})
        def _the_job():
            _ship(_normalize(_fetch()))

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
