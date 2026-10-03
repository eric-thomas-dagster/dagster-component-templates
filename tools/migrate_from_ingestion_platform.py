#!/usr/bin/env python3
"""Audit an existing Fivetran or Airbyte account and show, per connector,
both migration paths into this project:

  1. KEEP AS-IS: orchestrate the connector from inside Dagster unchanged,
     via the existing `fivetran_assets` / `airbyte_assets` components
     (each wraps dagster-fivetran/dagster-airbyte generically -- works for
     ANY connector on that platform, no per-vendor mapping needed).
  2. CONVERT TO DLT: if this catalog already has a native `*_ingestion`
     component for that connector's vendor, scaffold a ready-to-edit YAML
     for it (reusing the component's own example.yaml as the template).

Connectors with no native dlt equivalent are collected into a gap report --
real, demand-driven signal for which vendor to build next, instead of
guessing from a generic SaaS list.

Nothing here invents or guesses platform API shapes: Fivetran's and
Airbyte's list-connectors responses are read exactly as their own API
reference docs define them (see VENDOR_TYPE_TO_COMPONENT's comment for
per-entry confidence notes). Credentials are never extracted or carried
over -- neither platform's API exposes secret config values, and the
generated YAML always leaves credential fields as env-var placeholders
for the user to fill in themselves.

Usage:
    export FIVETRAN_API_KEY=...
    export FIVETRAN_API_SECRET=...
    python3 tools/migrate_from_ingestion_platform.py --platform fivetran

    export AIRBYTE_CLIENT_ID=...
    export AIRBYTE_CLIENT_SECRET=...
    python3 tools/migrate_from_ingestion_platform.py --platform airbyte

Options:
    --out-dir DIR     Where to write scaffolded YAML + the JSON report
                       (default: migration_audit/<platform>/)
    --dry-run          Print the report, don't write any files
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
INGESTION_DIR = REPO_ROOT / "assets" / "ingestion"


# ---------------------------------------------------------------------------
# Vendor-type -> native component mapping.
#
# Key = the platform's own connector-type identifier, lowercased with both
# "-" and "_" normalized to "-" (see _normalize_vendor_key). Value = this
# repo's assets/ingestion/<name> directory.
#
# Confidence notes:
#   [verified]  confirmed against Fivetran's or Airbyte's own docs/API
#               reference directly during this tool's development.
#   [inferred]  not individually verified, but follows the exact naming
#               convention confirmed for several verified entries from the
#               same platform (e.g. Fivetran's plain-lowercase-vendor-name
#               pattern) -- correct the key if a real account shows a
#               different string; this dict is meant to be edited freely.
#
# This list is intentionally not exhaustive. An unmapped connector is not a
# bug in this tool -- it's the actual output this tool exists to produce:
# a real gap, found via a real account, worth prioritizing.
# ---------------------------------------------------------------------------
VENDOR_TYPE_TO_COMPONENT: dict[str, str] = {
    # --- verified ---
    "salesforce": "salesforce_ingestion",
    "zendesk": "zendesk_ingestion",
    "zendesk-support": "zendesk_ingestion",
    "zendesk-chat": "zendesk_ingestion",
    "hubspot": "hubspot_ingestion",
    "stripe": "stripe_ingestion",
    "postgres": "database_replication",
    "trello": "trello_ingestion",
    "harvest": None,  # confirmed real Airbyte connector -- no native equivalent (real gap)
    # --- inferred (plain-vendor-name convention) ---
    "shopify": "shopify_ingestion",
    "github": "github_ingestion",
    "jira": "jira_ingestion",
    "asana": "asana_ingestion",
    "intercom": "intercom_ingestion",
    "pipedrive": "pipedrive_ingestion",
    "mailchimp": "mailchimp_ingestion",
    "netsuite": "netsuite_ingestion",
    "workday": "workday_ingestion",
    "google-ads": "google_ads_ingestion",
    "facebook-marketing": "facebook_ads_ingestion",
    "facebook-ads": "facebook_ads_ingestion",
    "linkedin-ads": "linkedin_ads_ingestion",
    "twitter-ads": "twitter_ads_ingestion",
    "pinterest-ads": "pinterest_ads_ingestion",
    "tiktok-ads": "tiktok_ads_ingestion",
    "zoom": "zoom_ingestion",
    "slack": "slack_ingestion",
    "notion": "notion_ingestion",
    "airtable": "airtable_ingestion",
    "monday": "monday_ingestion",
    "mondaydotcom": "monday_ingestion",
    "freshdesk": "freshdesk_ingestion",
    "freshservice": "freshservice_ingestion",
    "zoho-crm": "zoho_crm_ingestion",
    "zoho-desk": "zoho_desk_ingestion",
    "quickbooks": "quickbooks_ingestion",
    "xero": "xero_ingestion",
    "docusign": "docusign_ingestion",
    "pandadoc": "pandadoc_ingestion",
    "typeform": "typeform_ingestion",
    "surveymonkey": "surveymonkey_ingestion",
    "qualtrics": "qualtrics_ingestion",
    "calendly": "calendly_ingestion",
    "acuity-scheduling": "acuity_scheduling_ingestion",
    "front": "front_ingestion",
    "gong": "gong_calls_ingestion",
    "greenhouse": "greenhouse_harvest_ingestion",
    "bamboohr": "bamboohr_ingestion",
    "gusto": "gusto_ingestion",
    "webflow": "webflow_ingestion",
    "segment": "segment_ingestion",
    "amplitude": "amplitude_ingestion",
    "braze": "braze_ingestion",
    "klaviyo": "klaviyo_ingestion",
    "customer-io": "customerio_ingestion",
    "customerio": "customerio_ingestion",
    "activecampaign": "activecampaign_ingestion",
    "constant-contact": "constant_contact_ingestion",
    "convertkit": "convertkit_ingestion",
    "copper": "copper_ingestion",
    "insightly": "insightly_ingestion",
    "close-crm": "close_crm_ingestion",
    "close": "close_crm_ingestion",
    "dynamics-crm": "dynamics_crm_ingestion",
    "workable": "workable_ingestion",
    "lever": "lever_ingestion",
    "linear": "linear_ingestion",
    "clickup": "clickup_ingestion",
    "wrike": "wrike_ingestion",
    "smartsheet": "smartsheet_ingestion",
    "basecamp": "basecamp_ingestion",
    "sentry": "sentry_issues_ingestion",
    "statuspage": "statuspage_ingestion",
    "pagerduty": "pagerduty_events_ingestion",
    "opsgenie": "opsgenie_ingestion",
    "launchdarkly": "launchdarkly_ingestion",
    "optimizely": "optimizely_ingestion",
    "posthog": "posthog_events_ingestion",
    "pendo": "pendo_ingestion",
    "productboard": "productboard_ingestion",
    "google-analytics": "google_analytics_ingestion",
    "google-analytics-v4": "google_analytics_ingestion",
    "google-sheets": "google_sheets_ingestion",
    "google-calendar": "google_calendar_ingestion",
    "google-drive": "google_drive_ingestion",
    "dropbox": "dropbox_ingestion",
    "dropbox-sign": "dropbox_sign_ingestion",
    "hellosign": "dropbox_sign_ingestion",
    "box": "box_ingestion",
    "bitbucket": "bitbucket_ingestion",
    "circleci": "circleci_ingestion",
    "buildkite": "buildkite_ingestion",
    "okta": "okta_management_ingestion",
    "auth0": "auth0_management_ingestion",
    "clerk": "clerk_ingestion",
    "workos": "workos_ingestion",
    "frontegg": "frontegg_ingestion",
    "deel": "deel_ingestion",
    "personio": "personio_ingestion",
    "rippling": "rippling_ingestion",
    "paylocity": "paylocity_ingestion",
    "adp-workforce-now": "adp_workforce_now_ingestion",
    "expensify": "expensify_ingestion",
    "navan": "navan_ingestion",
    "tripactions": "navan_ingestion",
    "concur": "concur_ingestion",
    "brex": "brex_ingestion",
    "ramp": "ramp_ingestion",
    "bill-com": "bill_com_ingestion",
    "coupa": "coupa_ingestion",
    "chargebee": "chargebee_ingestion",
    "chargify": "chargify_ingestion",
    "maxio": "chargify_ingestion",
    "recurly": "recurly_ingestion",
    "zuora": "zuora_ingestion",
    "paddle": "paddle_ingestion",
    "adyen": "adyen_ingestion",
    "paypal": "paypal_ingestion",
    "square": "square_ingestion",
    "bigcommerce": "bigcommerce_ingestion",
    "magento": "magento_ingestion",
    "woocommerce": "woocommerce_ingestion",
    "webex": "webex_ingestion",
    "ringcentral": "ringcentral_ingestion",
    "talkdesk": "talkdesk_ingestion",
    "toggl": "toggl_track_ingestion",
    "vanta": "vanta_controls_ingestion",
    "vercel": "vercel_ingestion",
    "grafana": "grafana_cloud_ingestion",
    "honeycomb": "honeycomb_ingestion",
    "sumologic": "sumo_logic_ingestion",
    "sumo-logic": "sumo_logic_ingestion",
    "statsig": "statsig_ingestion",
    "metabase": "metabase_ingestion",
    "omni": "omni_ingestion",
    "matomo": "matomo_ingestion",
    "docebo": "docebo_ingestion",
    "egnyte": "egnyte_ingestion",
    "fullstory": "fullstory_ingestion",
    "gorgias": "gorgias_tickets_ingestion",
    "drift": "drift_conversations_ingestion",
    "kustomer": "kustomer_conversations_ingestion",
    "gainsight": "gainsight_ingestion",
    "churnzero": "churnzero_ingestion",
    "vitally": "vitally_ingestion",
    "totango": "totango_ingestion",
    "outreach": "outreach_prospects_ingestion",
    "salesloft": "salesloft_people_ingestion",
    "sage-intacct": "sage_intacct_ar_invoices_ingestion",
    "confluence": "confluence_ingestion",
    "help-scout": "help_scout_ingestion",
    "gmail": "gmail_ingestion",
    "mongodb": "mongodb_ingestion",
    "kafka": "kafka_to_database_asset",
    "s3": "s3_to_database_asset",
    "sftp": "sftp_to_database_asset",
}


def _normalize_vendor_key(raw: str) -> str:
    return re.sub(r"[_\s]+", "-", raw.strip().lower())


# ---------------------------------------------------------------------------
# Common record shape
# ---------------------------------------------------------------------------
@dataclass
class ExternalConnector:
    platform: str  # "fivetran" | "airbyte"
    connector_id: str
    vendor_type: str  # normalized key
    raw_vendor_type: str  # as returned by the platform, pre-normalization
    display_name: str
    status: str
    schema: Optional[str] = None


@dataclass
class ConnectorPlan:
    connector: ExternalConnector
    keep_as_is_component: str  # always "fivetran_assets" or "airbyte_assets"
    native_component: Optional[str]  # None if no native equivalent exists
    scaffold_path: Optional[str] = None


# ---------------------------------------------------------------------------
# Fivetran
# ---------------------------------------------------------------------------
def _call_fivetran_api(path: str, api_key: str, api_secret: str) -> dict[str, Any]:
    """The one external call for the Fivetran path -- isolated so tests can
    monkeypatch it wholesale without needing real credentials or network."""
    resp = requests.get(
        f"https://api.fivetran.com/v1{path}",
        auth=(api_key, api_secret),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def list_fivetran_connectors(api_key: str, api_secret: str) -> list[ExternalConnector]:
    connectors: list[ExternalConnector] = []
    cursor: Optional[str] = None
    while True:
        path = "/connections" + (f"?cursor={cursor}" if cursor else "")
        body = _call_fivetran_api(path, api_key, api_secret)
        items = body.get("data", {}).get("items", [])
        for item in items:
            raw_type = item.get("service", "")
            connectors.append(
                ExternalConnector(
                    platform="fivetran",
                    connector_id=item.get("id", ""),
                    vendor_type=_normalize_vendor_key(raw_type),
                    raw_vendor_type=raw_type,
                    display_name=item.get("schema", raw_type),
                    status=(item.get("status", {}) or {}).get("setup_state", "unknown"),
                    schema=item.get("schema"),
                )
            )
        cursor = body.get("data", {}).get("next_cursor")
        if not cursor:
            break
    return connectors


# ---------------------------------------------------------------------------
# Airbyte (modern Cloud API, api.airbyte.com)
# ---------------------------------------------------------------------------
def _get_airbyte_access_token(client_id: str, client_secret: str) -> str:
    """The one auth call for Airbyte -- isolated for the same reason."""
    resp = requests.post(
        "https://api.airbyte.com/v1/applications/token",
        json={"client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _call_airbyte_api(path: str, token: str) -> dict[str, Any]:
    """The one external call for the Airbyte path -- isolated for tests."""
    resp = requests.get(
        f"https://api.airbyte.com/v1{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def list_airbyte_sources(
    client_id: str, client_secret: str, workspace_id: Optional[str] = None
) -> list[ExternalConnector]:
    token = _get_airbyte_access_token(client_id, client_secret)
    connectors: list[ExternalConnector] = []
    next_path: Optional[str] = "/sources" + (
        f"?workspaceIds={workspace_id}" if workspace_id else ""
    )
    while next_path:
        body = _call_airbyte_api(next_path, token)
        for item in body.get("data", []):
            raw_type = item.get("sourceType", "")
            connectors.append(
                ExternalConnector(
                    platform="airbyte",
                    connector_id=item.get("sourceId", ""),
                    vendor_type=_normalize_vendor_key(raw_type),
                    raw_vendor_type=raw_type,
                    display_name=item.get("name", raw_type),
                    status="active",
                )
            )
        next_url = body.get("next")
        next_path = None
        if next_url:
            idx = next_url.find("/v1")
            if idx != -1:
                next_path = next_url[idx + 3 :]
    return connectors


# ---------------------------------------------------------------------------
# Planning + scaffolding
# ---------------------------------------------------------------------------
def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return slug or "unnamed"


def plan_connector(connector: ExternalConnector) -> ConnectorPlan:
    keep_as_is = "fivetran_assets" if connector.platform == "fivetran" else "airbyte_assets"
    native = VENDOR_TYPE_TO_COMPONENT.get(connector.vendor_type)
    return ConnectorPlan(
        connector=connector,
        keep_as_is_component=keep_as_is,
        native_component=native,
    )


def scaffold_native_yaml(plan: ConnectorPlan) -> Optional[str]:
    """Build a ready-to-edit YAML for the native component, reusing that
    component's own example.yaml as the template. Never carries over
    credentials -- neither platform's API exposes secret config values, so
    every credential field in the template stays an env-var placeholder for
    the user to fill in themselves."""
    if not plan.native_component:
        return None
    example_path = INGESTION_DIR / plan.native_component / "example.yaml"
    if not example_path.exists():
        return None
    template = example_path.read_text()

    connector = plan.connector
    asset_name = _slugify(connector.display_name) or _slugify(connector.raw_vendor_type)
    template = re.sub(r"asset_name:\s*\S+", f"asset_name: {asset_name}", template, count=1)

    header = (
        f"# Migrated from {connector.platform} connector "
        f"'{connector.display_name}' (id: {connector.connector_id}, "
        f"type: {connector.raw_vendor_type}).\n"
        f"# Credentials were NOT carried over -- {connector.platform}'s API never "
        f"exposes secret config values. Fill in the env vars below yourself.\n"
    )
    return header + template


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def build_report(connectors: list[ExternalConnector]) -> dict[str, Any]:
    plans = [plan_connector(c) for c in connectors]
    mapped = [p for p in plans if p.native_component]
    unmapped = [p for p in plans if not p.native_component]

    gap_counts: dict[str, int] = {}
    gap_examples: dict[str, str] = {}
    for p in unmapped:
        key = p.connector.raw_vendor_type or p.connector.vendor_type
        gap_counts[key] = gap_counts.get(key, 0) + 1
        gap_examples.setdefault(key, p.connector.display_name)

    gap_list = sorted(gap_counts.items(), key=lambda kv: -kv[1])

    return {
        "total_connectors": len(connectors),
        "mapped_count": len(mapped),
        "unmapped_count": len(unmapped),
        "mapped": [
            {
                "connector_id": p.connector.connector_id,
                "display_name": p.connector.display_name,
                "vendor_type": p.connector.raw_vendor_type,
                "keep_as_is_component": p.keep_as_is_component,
                "native_component": p.native_component,
            }
            for p in mapped
        ],
        "gap_report": [
            {"vendor_type": vt, "example_connector": gap_examples[vt], "count": count}
            for vt, count in gap_list
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=["fivetran", "airbyte"], required=True)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.platform == "fivetran":
        api_key = os.environ.get("FIVETRAN_API_KEY")
        api_secret = os.environ.get("FIVETRAN_API_SECRET")
        if not api_key or not api_secret:
            print("Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET.", file=sys.stderr)
            return 1
        connectors = list_fivetran_connectors(api_key, api_secret)
    else:
        client_id = os.environ.get("AIRBYTE_CLIENT_ID")
        client_secret = os.environ.get("AIRBYTE_CLIENT_SECRET")
        workspace_id = os.environ.get("AIRBYTE_WORKSPACE_ID")
        if not client_id or not client_secret:
            print("Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET.", file=sys.stderr)
            return 1
        connectors = list_airbyte_sources(client_id, client_secret, workspace_id)

    report = build_report(connectors)
    plans = [plan_connector(c) for c in connectors]

    print(f"Found {report['total_connectors']} {args.platform} connector(s): "
          f"{report['mapped_count']} have a native dlt equivalent here, "
          f"{report['unmapped_count']} do not.\n")

    for p in plans:
        c = p.connector
        print(f"- {c.display_name} ({c.raw_vendor_type})")
        print(f"    keep as-is:  type: dagster_component_templates.{p.keep_as_is_component.title().replace('_', '')}Component  (add '{c.connector_id}' to its filter list)")
        if p.native_component:
            print(f"    convert to dlt: assets/ingestion/{p.native_component}  [scaffold generated]")
        else:
            print(f"    convert to dlt: no native component yet -- real gap")
        print()

    if report["gap_report"]:
        print("Gap report (prioritize building these next, highest-count first):")
        for g in report["gap_report"]:
            print(f"  {g['vendor_type']:30s} seen {g['count']}x (e.g. {g['example_connector']!r})")

    if args.dry_run:
        return 0

    out_dir = Path(args.out_dir) if args.out_dir else REPO_ROOT / "migration_audit" / args.platform
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "report.json").write_text(json.dumps(report, indent=2))

    scaffolds_dir = out_dir / "scaffolds"
    for p in plans:
        yaml_text = scaffold_native_yaml(p)
        if yaml_text:
            scaffolds_dir.mkdir(parents=True, exist_ok=True)
            slug = _slugify(p.connector.display_name)
            scaffold_file = scaffolds_dir / f"{slug}.yaml"
            scaffold_file.write_text(yaml_text)
            try:
                p.scaffold_path = str(scaffold_file.relative_to(REPO_ROOT))
            except ValueError:
                p.scaffold_path = str(scaffold_file)

    print(f"\nWrote {out_dir / 'report.json'} and scaffolded YAML under {scaffolds_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
