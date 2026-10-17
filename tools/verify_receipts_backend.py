#!/usr/bin/env python3
"""Verify a context-receipt backend against YOUR database/warehouse: runs the store's built-in self-test (13 steps, synthetic
receipts, cleans up after itself) and prints a pass/fail table. Exit code 0 only if every step passed.

The warehouse backends were developed without access to real BigQuery/Snowflake/Databricks/Postgres/MySQL (they were exercised
against DuckDB/SQLite and parsed with sqlglot), so run this once against yours before relying on it.

    # BigQuery (Application Default Credentials, or GCP_SA_JSON_B64 = base64 service-account JSON)
    python3 tools/verify_receipts_backend.py bigquery --dataset my-project.context [--prefix verify_] [--creds-env GCP_SA_JSON_B64]

    # Any SQLAlchemy database (URL read from an environment variable, never from argv)
    RECEIPTS_URL='snowflake://user:pass@account/DB/SCHEMA?warehouse=WH' \\
    python3 tools/verify_receipts_backend.py sql --url-env RECEIPTS_URL --schema ANALYTICS.CONTEXT [--prefix verify_]

    # Local sanity check, no warehouse needed
    python3 tools/verify_receipts_backend.py sqlite --path /tmp/receipts.sqlite3

Use --prefix to point at throw-away table names (created if missing, left in place; drop them afterwards if you like). The
self-test only writes and deletes its own synthetic rows, but note it also recomputes item scores store-wide (see README).
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

STORE = Path(__file__).resolve().parent.parent / "resources" / "context_receipt_store" / "component.py"


def _load_store_module():
    if STORE.exists():
        spec = importlib.util.spec_from_file_location("context_receipt_store_verify", STORE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    import dagster_community_components as pkg  # installed-package fallback

    return sys.modules[pkg.ContextReceiptStore.__module__]


def build(mod, args):
    import dagster as dg

    if args.backend == "sqlite":
        return mod.ContextReceiptStore(args.path)
    if args.backend == "bigquery":
        project = args.dataset.split(".")[0]

        def factory():
            from dagster_gcp import BigQueryResource

            creds = dg.EnvVar(args.creds_env).get_value() if args.creds_env else None
            return BigQueryResource(project=project, location=args.location, gcp_credentials=creds).get_client()

        return mod.BigQueryContextReceiptStore(args.dataset, client_factory=factory, table_prefix=args.prefix)
    engine = lambda: mod._create_sql_engine(dg.EnvVar(args.url_env).get_value())  # noqa: E731
    return mod.SqlContextReceiptStore(engine, schema=args.schema, table_prefix=args.prefix)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="backend", required=True)
    b = sub.add_parser("bigquery")
    b.add_argument("--dataset", required=True, help="project.dataset")
    b.add_argument("--prefix", default="")
    b.add_argument("--location")
    b.add_argument("--creds-env", help="env var with base64 service-account JSON (default: ADC)")
    s = sub.add_parser("sql")
    s.add_argument("--url-env", required=True, help="env var holding the SQLAlchemy URL")
    s.add_argument("--schema", default="", help="database.schema (Snowflake), catalog.schema (Databricks), schema (Postgres), database (MySQL)")
    s.add_argument("--prefix", default="")
    q = sub.add_parser("sqlite")
    q.add_argument("--path", required=True)
    args = ap.parse_args(argv)

    mod = _load_store_module()
    try:
        store = build(mod, args)
        steps = store.selftest()
    except Exception as exc:  # noqa: BLE001  (connection/driver problems are the most likely first failure)
        print(f"could not run the self-test: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    width = max(len(s["step"]) for s in steps)
    for s in steps:
        print(f"{'PASS' if s['ok'] else 'FAIL'}  {s['step']:<{width}}  {s['detail']}")
    failed = [s for s in steps if not s["ok"]]
    print(f"\n{len(steps) - len(failed)}/{len(steps)} steps passed" + ("" if not failed else f"; failing: {', '.join(s['step'] for s in failed)}"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
