#!/usr/bin/env python3
"""migrate_asset_metadata.py — carry an asset's historical metadata over to a NEW asset key
within the same Dagster+ deployment, for the common "I renamed an asset in code and it now
shows up as brand new, with no history" situation.

Why this is needed at all: an asset's materialization history, metadata, and catalog
identity are all keyed by its asset key. Renaming the key in code (or changing a
`key_prefix`, `group_name`-driven key, etc.) does not move or merge that history onto the
new key — Dagster has no "rename" operation, by design: the event log is append-only and
keyed. The old key's history stays exactly where it is, under the old key, forever; the new
key starts with none.

What this script actually does (and does NOT do):
  - It reads the SOURCE asset key's materialization history via Dagster+'s real GraphQL API
    (`assetNodeOrError(assetKey).assetMaterializations`), including every metadata entry
    Dagster+ recorded for each event.
  - It REPORTS NEW materialization events under the DESTINATION asset key, carrying that
    metadata forward, via Dagster+'s real `report_asset_materialization/` REST endpoint (the
    same endpoint Dagster's own `examples/oss-metadata-to-plus` example uses:
    https://github.com/dagster-io/dagster/tree/master/examples/oss-metadata-to-plus).
  - By default, one event per PARTITION (or one single event for an unpartitioned asset),
    carrying that partition's MOST RECENT metadata — enough to populate the new asset's
    catalog page and partition status grid the way the old one looked. Pass --all-events to
    instead replay every historical event for every partition (see the caveat below on why
    this usually isn't more useful, just noisier).
  - It does NOT preserve the original timestamps. Every event this script reports lands with
    `report_asset_materialization/`'s OWN notion of "now" — there is no field to backdate it.
    The new asset's materialization timeline will show a burst of events dated whenever you
    ran this script, not the original historical dates. If your goal is "the new asset looks
    like it has always existed with this history," that is NOT achievable via any public API
    today — only "the new asset has this metadata and shows as materialized."
  - It is NOT reversible via this script. Dagster's event log is append-only; there is no
    "delete a materialization" API. Once reported, these events are permanent (an admin could
    still purge via Dagster+ support channels, but this tool has no undo). `--dry-run` (the
    default) never writes anything; you must pass --yes to actually report events.
  - The REST write endpoint, per Dagster's own example, requires an AGENT token (not a
    regular user/service API token) — unverified beyond that example's own README claim. If
    you get an auth error with a user/service token, this is almost certainly why.
  - `Dict`/structured metadata (TableMetadataEntry, TableSchemaMetadataEntry, and similar
    non-scalar entry types) is skipped, not guessed at — this script only introspects and
    forwards metadata entries whose GraphQL type exposes a plain scalar value. A skipped
    entry is reported, never silently dropped.

GraphQL field names for metadata entry types (TextMetadataEntry.text, JsonMetadataEntry.jsonString,
etc.) are NOT hardcoded: this script introspects your OWN deployment's schema at runtime (one
extra GraphQL call) and only ever requests fields that your deployment's schema actually has.
This makes it self-correcting across Dagster+ schema versions, instead of guessing field names
from a point-in-time schema snapshot that could drift.

Usage:
    export DAGSTER_CLOUD_API_TOKEN=agent:xxxxxx   # must be an AGENT token -- see caveat above

    # Preview (default, never writes anything):
    ./migrate_asset_metadata.py migrate \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --source-asset-key "old/asset/key" \\
        --dest-asset-key "new/asset/key"

    # Actually report the events:
    ./migrate_asset_metadata.py migrate \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --source-asset-key "old/asset/key" \\
        --dest-asset-key "new/asset/key" \\
        --yes

    # Batch, via a YAML manifest of {source_asset_key, dest_asset_key} pairs:
    ./migrate_asset_metadata.py migrate manifest.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN --yes

    # Inspect a source asset's history before deciding anything (read-only, no manifest needed):
    ./migrate_asset_metadata.py list-materializations \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --asset-key "old/asset/key"

Manifest shape (YAML), for the batch form of `migrate`:
    migrations:
      - source_asset_key: old/path/to/asset
        dest_asset_key: new/path/to/asset
      - source_asset_key: another_old_key
        dest_asset_key: another_new_key

Requires: Python 3.8+, PyYAML (only for the manifest form of `migrate`).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


# --------------------------------------------------------------------------
# GraphQL + REST clients
# --------------------------------------------------------------------------
def graphql(deployment_url: str, token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    endpoint = deployment_url.rstrip("/") + "/graphql"
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Dagster-Cloud-Api-Token": token,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:500]}") from e

    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {json.dumps(payload['errors'])[:1500]}")
    return payload.get("data") or {}


def report_asset_materialization_rest(
    deployment_url: str, token: str, asset_key_path: List[str],
    metadata: Dict[str, Any], partition: Optional[str],
) -> Dict[str, Any]:
    """POST to Dagster+'s `report_asset_materialization/` REST endpoint -- the same one used by
    Dagster's own `examples/oss-metadata-to-plus`. Per that example's README, this requires an
    AGENT token, not a regular user/service token."""
    endpoint = deployment_url.rstrip("/") + "/report_asset_materialization/"
    body: Dict[str, Any] = {"asset_key": asset_key_path, "metadata": metadata}
    if partition is not None:
        body["partition"] = partition
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Dagster-Cloud-Api-Token": token,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:500]
        hint = ""
        if e.code in (401, 403):
            hint = " (hint: this endpoint requires an AGENT token, not a user/service token)"
        raise RuntimeError(f"HTTP {e.code}{hint}: {detail}") from e


# --------------------------------------------------------------------------
# Metadata-entry introspection -- never hardcode GraphQL field names for the
# ~19 MetadataEntry subtypes; ask the deployment's own schema instead, every run.
# --------------------------------------------------------------------------
Q_INTROSPECT_METADATA_ENTRY_TYPES = """
  query IntrospectMetadataEntryTypes {
    __schema {
      types {
        name
        kind
        fields {
          name
          type { kind ofType { kind name } }
        }
      }
    }
  }
"""


def _is_scalar_field(field: Dict[str, Any]) -> bool:
    t = field["type"]
    kind = t.get("kind")
    if kind == "NON_NULL":
        kind = (t.get("ofType") or {}).get("kind")
    return kind in ("SCALAR", "ENUM")


def introspect_metadata_entry_scalar_fields(deployment_url: str, token: str) -> Dict[str, List[str]]:
    """Returns {TypeName: [scalar_field_name, ...]} for every GraphQL type whose name ends in
    'MetadataEntry' and exposes at least one scalar/enum field we can select without a further
    sub-selection. Table/TableSchema/PythonArtifact/AssetLineage-shaped entries are skipped here
    (no scalar fields of their own at the top level) -- reported as skipped at migrate time,
    never silently dropped."""
    data = graphql(deployment_url, token, Q_INTROSPECT_METADATA_ENTRY_TYPES, {})
    result: Dict[str, List[str]] = {}
    for t in data.get("__schema", {}).get("types") or []:
        name = t.get("name") or ""
        if not name.endswith("MetadataEntry") or t.get("kind") != "OBJECT":
            continue
        scalar_fields = [f["name"] for f in (t.get("fields") or []) if _is_scalar_field(f) and f["name"] != "label"]
        if scalar_fields:
            result[name] = scalar_fields
    return result


def build_metadata_entry_fragment(entry_type_fields: Dict[str, List[str]]) -> str:
    parts = ["__typename", "label"]
    for type_name, fields in sorted(entry_type_fields.items()):
        parts.append("... on %s { %s }" % (type_name, " ".join(fields)))
    return "\n".join(parts)


# Field names tried in this priority order, per entry type, when picking the one value to
# forward -- all names here must still exist in whatever `build_metadata_entry_fragment`
# actually selected (itself driven by live introspection), so an outdated guess here is
# harmless: it just falls through to the generic "first present scalar field" fallback below.
_VALUE_FIELD_PRIORITY = [
    "text", "url", "path", "jsonString", "mdStr", "md", "floatValue", "intValue",
    "boolValue", "timestamp", "pool",
]


def extract_metadata_value(entry: Dict[str, Any]) -> Optional[Any]:
    for name in _VALUE_FIELD_PRIORITY:
        if entry.get(name) is not None:
            return entry[name]
    for key, value in entry.items():
        if key in ("__typename", "label") or value is None:
            continue
        return value
    return None


# --------------------------------------------------------------------------
# Reading materialization history
# --------------------------------------------------------------------------
def _asset_materializations_query(entry_fragment: str) -> str:
    return """
      query AssetMaterializations($assetKey: AssetKeyInput!, $limit: Int!, $beforeTimestampMillis: String) {
        assetNodeOrError(assetKey: $assetKey) {
          __typename
          ... on AssetNode {
            assetMaterializations(limit: $limit, beforeTimestampMillis: $beforeTimestampMillis) {
              timestamp
              partition
              runId
              metadataEntries {
                %s
              }
            }
          }
          ... on AssetNotFoundError { message }
        }
      }
    """ % entry_fragment


def parse_asset_key(s: str) -> List[str]:
    return [seg for seg in s.strip().split("/") if seg]


def fetch_all_materializations(
    deployment_url: str, token: str, asset_key_path: List[str], entry_fragment: str,
    page_size: int = 100, overall_limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Walks the full history oldest-ward using `beforeTimestampMillis` (there is no cursor field
    on `assetMaterializations` -- the oldest timestamp seen so far becomes the next page's
    cutoff). Stops when a page returns fewer than `page_size` events, or `overall_limit` is hit."""
    query = _asset_materializations_query(entry_fragment)
    all_events: List[Dict[str, Any]] = []
    before: Optional[str] = None
    while True:
        remaining = page_size if overall_limit is None else min(page_size, overall_limit - len(all_events))
        if remaining <= 0:
            break
        data = graphql(deployment_url, token, query, {
            "assetKey": {"path": asset_key_path}, "limit": remaining, "beforeTimestampMillis": before,
        })
        node = data.get("assetNodeOrError") or {}
        if node.get("__typename") == "AssetNotFoundError":
            raise RuntimeError(f"asset key {'/'.join(asset_key_path)!r} not found: {node.get('message')}")
        page = node.get("assetMaterializations") or []
        if not page:
            break
        all_events.extend(page)
        if len(page) < remaining:
            break
        oldest_ts_ms = int(min(float(e["timestamp"]) for e in page) * 1000)
        before = str(oldest_ts_ms)
    return all_events


def group_latest_per_partition(events: List[Dict[str, Any]]) -> Dict[Optional[str], Dict[str, Any]]:
    """One event per partition key (None for an unpartitioned asset), keeping whichever has the
    highest timestamp -- the API's own ordering isn't assumed/relied on."""
    latest: Dict[Optional[str], Dict[str, Any]] = {}
    for e in events:
        key = e.get("partition")
        if key not in latest or float(e["timestamp"]) > float(latest[key]["timestamp"]):
            latest[key] = e
    return latest


# --------------------------------------------------------------------------
# Migration
# --------------------------------------------------------------------------
def _events_to_report(args: argparse.Namespace, deployment_url: str, token: str, source_path: List[str],
                       entry_fragment: str) -> List[Dict[str, Any]]:
    events = fetch_all_materializations(
        deployment_url, token, source_path, entry_fragment,
        page_size=100, overall_limit=args.limit,
    )
    if not events:
        return []
    return events if args.all_events else list(group_latest_per_partition(events).values())


def _migrate_one(args: argparse.Namespace, deployment_url: str, token: str, entry_fragment: str,
                  source_asset_key: str, dest_asset_key: str) -> Tuple[int, int]:
    source_path = parse_asset_key(source_asset_key)
    dest_path = parse_asset_key(dest_asset_key)
    if source_path == dest_path:
        sys.exit(f"ERROR: source and destination asset keys are identical ({source_asset_key!r}) -- refusing.")

    print(f"\n== {source_asset_key}  ->  {dest_asset_key} ==", file=sys.stderr)
    to_report = _events_to_report(args, deployment_url, token, source_path, entry_fragment)
    if not to_report:
        print("  (no materializations found for the source asset key -- nothing to migrate)", file=sys.stderr)
        return 0, 0

    reported = 0
    skipped_entries = 0
    for event in to_report:
        metadata: Dict[str, Any] = {}
        for entry in event.get("metadataEntries") or []:
            value = extract_metadata_value(entry)
            if value is None:
                skipped_entries += 1
                print(f"  skipping metadata entry {entry.get('label')!r} ({entry.get('__typename')}) "
                      f"-- no forwardable scalar value in this deployment's schema", file=sys.stderr)
                continue
            metadata[entry["label"]] = value
        partition = event.get("partition")
        label = f"partition={partition!r}" if partition else "(unpartitioned)"
        if args.dry_run:
            print(f"  [DRY RUN] would report {label}: {len(metadata)} metadata field(s)", file=sys.stderr)
        else:
            report_asset_materialization_rest(deployment_url, token, dest_path, metadata, partition)
            print(f"  reported {label}: {len(metadata)} metadata field(s)", file=sys.stderr)
        reported += 1
    return reported, skipped_entries


def cmd_migrate(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    if not args.dry_run and not args.yes:
        sys.exit("ERROR: this reports REAL, IRREVERSIBLE materialization events. "
                  "Run with --dry-run first, then re-run with --yes to actually write.")

    if args.manifest:
        try:
            import yaml
        except ImportError:
            sys.exit("ERROR: PyYAML not installed. Install with: pip install pyyaml")
        with open(args.manifest) as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict) or not isinstance(doc.get("migrations"), list):
            sys.exit(f"ERROR: manifest {args.manifest!r} must be a mapping with a top-level `migrations:` list")
        pairs = []
        for i, m in enumerate(doc["migrations"]):
            if not isinstance(m, dict) or not m.get("source_asset_key") or not m.get("dest_asset_key"):
                sys.exit(f"ERROR: migrations[{i}] needs both `source_asset_key` and `dest_asset_key`")
            pairs.append((m["source_asset_key"], m["dest_asset_key"]))
    else:
        if not args.source_asset_key or not args.dest_asset_key:
            sys.exit("ERROR: pass a manifest file, or both --source-asset-key and --dest-asset-key")
        pairs = [(args.source_asset_key, args.dest_asset_key)]

    print("Introspecting this deployment's MetadataEntry schema...", file=sys.stderr)
    entry_type_fields = introspect_metadata_entry_scalar_fields(args.deployment_url, token)
    entry_fragment = build_metadata_entry_fragment(entry_type_fields)
    print(f"  found {len(entry_type_fields)} forwardable metadata entry type(s)", file=sys.stderr)

    if args.dry_run:
        print("\n[DRY RUN] -- nothing will be written. Re-run with --yes to actually report events.", file=sys.stderr)

    total_reported = total_skipped = 0
    for source_asset_key, dest_asset_key in pairs:
        reported, skipped = _migrate_one(args, args.deployment_url, token, entry_fragment, source_asset_key, dest_asset_key)
        total_reported += reported
        total_skipped += skipped

    verb = "Would report" if args.dry_run else "Reported"
    print(f"\n{verb} {total_reported} event(s) across {len(pairs)} asset(s) "
          f"({total_skipped} metadata entries skipped as non-forwardable)", file=sys.stderr)
    return 0


def cmd_list_materializations(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    entry_type_fields = introspect_metadata_entry_scalar_fields(args.deployment_url, token)
    entry_fragment = build_metadata_entry_fragment(entry_type_fields)
    events = fetch_all_materializations(
        args.deployment_url, token, parse_asset_key(args.asset_key), entry_fragment,
        page_size=100, overall_limit=args.limit,
    )
    if not events:
        print("(no materializations found for this asset key)", file=sys.stderr)
        return 0
    for e in sorted(events, key=lambda e: float(e["timestamp"]), reverse=True):
        fields = {entry["label"]: extract_metadata_value(entry) for entry in (e.get("metadataEntries") or [])}
        print(json.dumps({
            "timestamp": e["timestamp"], "partition": e.get("partition"), "run_id": e.get("runId"),
            "metadata": fields,
        }))
    print(f"\n{len(events)} materialization(s) total", file=sys.stderr)
    return 0


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="migrate_asset_metadata",
        description="Carry an asset's historical metadata over to a new asset key within the same Dagster+ deployment.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_migrate = sub.add_parser("migrate", help="Report source asset's metadata under a destination asset key.")
    p_migrate.add_argument("manifest", nargs="?", default=None, help="YAML manifest of {source_asset_key, dest_asset_key} pairs. Omit to use --source-asset-key/--dest-asset-key instead.")
    p_migrate.add_argument("--deployment-url", required=True, help="e.g. https://acme.dagster.cloud/prod")
    p_migrate.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_migrate.add_argument("--source-asset-key", help="e.g. 'old/asset/key' (ignored if a manifest is given)")
    p_migrate.add_argument("--dest-asset-key", help="e.g. 'new/asset/key' (ignored if a manifest is given)")
    p_migrate.add_argument("--all-events", action="store_true",
                            help="Replay every historical event per partition instead of just the latest. "
                                 "Rarely more useful: every reported event gets today's timestamp regardless, "
                                 "so this mostly produces several same-looking events instead of one.")
    p_migrate.add_argument("--limit", type=int, default=None, help="Cap the number of source events read per asset (debugging / chunked runs).")
    p_migrate.add_argument("--dry-run", action="store_true", default=True, help="Preview only, write nothing (default).")
    p_migrate.add_argument("--yes", action="store_true", help="Actually report events. Required to write anything.")
    p_migrate.set_defaults(func=cmd_migrate)

    p_list = sub.add_parser("list-materializations", help="Dump an asset's materialization history + metadata (read-only).")
    p_list.add_argument("--deployment-url", required=True)
    p_list.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_list.add_argument("--asset-key", required=True)
    p_list.add_argument("--limit", type=int, default=None)
    p_list.set_defaults(func=cmd_list_materializations)

    args = parser.parse_args()
    if getattr(args, "yes", False):
        args.dry_run = False
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
