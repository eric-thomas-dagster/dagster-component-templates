#!/usr/bin/env python3
"""Validate every component in manifest.json loads + builds defs.

Strategy:

  For each component, import its component.py module directly, instantiate
  the class from its example.yaml attributes, and call build_defs(ctx).

  - L1 (this script): catches import errors, Pydantic field warnings,
    schema mismatches, broken closures, parent-attribute shadow warnings,
    and any exception thrown during Definitions construction.
  - L2 (NOT in this script): full `dg add` + `dg check defs` against the
    registry-installed copy — catches autoload glue + manifest URL drift.

Status categories:

  PASS   — component loads + builds_defs returns Definitions cleanly
  WARN   — same as PASS but issued a warning (shadow attr, deprecation, …)
  FAIL   — real defect: import / instantiate / build_defs threw
  SKIP_*  — categorized failures the harness cannot evaluate:
    SKIP_OPTIONAL_DEP   — missing third-party SDK (boto3, dlt, google.cloud, …)
    SKIP_RELATIVE_IMPORT — component.py uses relative imports the harness can't resolve
    SKIP_EXAMPLE_MULTI_DOC — example.yaml has no parseable single document with a `type:` field
    SKIP_CONTEXT_MISSING_ATTR — component needs a real ComponentLoadContext attr the stub doesn't expose
    SKIP_CREDENTIALS — component validates credentials at instantiation time (eager-creds anti-pattern; flagged but not a build_defs bug)

Output:

  - validate_manifest_report.json — per-component status + warnings + error trace
  - stdout summary table

Usage:

  python tools/validate_manifest.py                  # run all
  python tools/validate_manifest.py --ids foo,bar    # subset
  python tools/validate_manifest.py --jobs 8         # parallelism
  python tools/validate_manifest.py --fail-on-warn   # exit nonzero if any WARN

Exit code is 0 on no FAIL, 1 if any FAIL, 2 if --fail-on-warn and any WARN.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import importlib.util
import io
import json
import sys
import traceback
import warnings
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "manifest.json"
REPORT_PATH = REPO_ROOT / "validate_manifest_report.json"

# Modules whose absence is a "no SDK installed" skip, not a component bug.
# This is the universe of optional / paid-vendor SDKs that components in
# the registry depend on. If component.py does `import boto3` and boto3
# isn't installed in the harness venv, that's not a bug in the component.
_OPTIONAL_DEPS = {
    # Cloud SDKs
    "boto3", "botocore",
    "google", "google.cloud", "google.api_core", "google.oauth2",
    "azure", "azure.identity", "azure.storage", "azure.mgmt",
    # Data tools
    "dlt", "duckdb", "databricks", "polars", "pyarrow", "scipy",
    "snowflake", "snowflake.connector", "snowflake.snowpark",
    "trino", "presto", "clickhouse_driver", "clickhouse_connect",
    "cassandra", "couchdb", "neo4j", "redis", "pymongo", "pulsar", "nats",
    "kafka", "confluent_kafka",
    "mysql", "pymssql", "psycopg2", "psycopg", "pymysql", "oracledb", "cx_Oracle",
    "ibm_db", "ibm_db_sa", "ibm_db_dbi",
    "elasticsearch", "opensearchpy",
    # Cloud SDK Dagster libs (all dagster_<vendor> are optional)
    "dagster_aws", "dagster_gcp", "dagster_azure", "dagster_databricks",
    "dagster_snowflake", "dagster_snowflake_pandas",
    "dagster_snowflake_polars", "dagster_snowflake_pyspark",
    "dagster_dbt", "dagster_fivetran", "dagster_airbyte",
    "dagster_airflow", "dagster_airlift", "dagster_anthropic",
    "dagster_gcp_pandas", "dagster_duckdb_pandas",
    "dagster_duckdb", "dagster_duckdb_polars", "dagster_duckdb_pyspark",
    "dagster_census", "dagster_chroma", "dagster_weaviate",
    "dagster_twilio", "dagster_postgres", "dagster_mysql",
    "dagster_msteams", "dagster_pagerduty", "dagster_slack", "dagster_papertrail",
    "dagster_pyspark", "dagster_spark",
    "dagster_datadog", "dagster_deltalake_pandas", "dagster_deltalake_polars",
    "dagster_dlt", "dagster_gemini", "dagster_github", "dagster_hex",
    "dagster_iceberg", "dagster_looker", "dagster_openai", "dagster_polars",
    "dagster_qdrant", "dagster_salesforce", "dagster_sigma", "dagster_sling",
    # AI / NLP heavies
    "openai", "anthropic", "cohere", "litellm", "instructor", "dspy",
    "transformers", "torch", "sentence_transformers", "tensorflow",
    "huggingface_hub",
    "langchain", "llama_index", "chromadb", "weaviate", "pinecone",
    "spacy", "nltk", "sklearn", "scikit_learn",
    "Pillow", "PIL",
    # Misc SaaS / chat
    "twilio", "stripe", "sendgrid", "slack_sdk",
    "requests_oauthlib", "msal",
    "sshtunnel", "paramiko",
    "papermill", "jupyter",
}


class _StubLogger:
    """No-op logger used in place of the real Dagster logger inside the
    harness. Components that call context.log.info(...) inside build_defs
    (rare, since build_defs is graph-construction not execution) silently
    succeed against this stub."""
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def debug(self, *args, **kwargs): pass


class _StubComponentLoadContext:
    """Best-effort stand-in for dg.ComponentLoadContext.

    Exposes the attributes load-time component code commonly reads:
    log, path, project_root, defs_root, resources. Everything else
    yields a recursive default that doesn't throw — so an unknown
    attribute access just returns another stub instead of AttributeError.

    This is sufficient because build_defs() is graph CONSTRUCTION; it
    builds `dg.Definitions(...)` from config but does not actually run
    anything. Real ComponentLoadContext attrs matter at run time, not
    construction time.
    """
    log = _StubLogger()
    path = Path(".")
    project_root = Path(".")
    defs_root = Path(".")
    resources = {}

    def __getattr__(self, name):
        # Anything we didn't explicitly define returns another stub —
        # never raises AttributeError. Class-level attrs above still
        # take precedence; this fires only for missing names.
        return _StubComponentLoadContext()

    def __call__(self, *args, **kwargs):
        return _StubComponentLoadContext()


def _classify_module_not_found(exc: ModuleNotFoundError) -> str | None:
    """Return SKIP_OPTIONAL_DEP if the missing module is a known optional
    dep we don't expect installed in the harness venv. Returns None
    otherwise (real defect)."""
    name = (exc.name or "").lower()
    if not name:
        return None
    # Check name and parent components against the optional-deps registry.
    parts = name.split(".")
    for i in range(len(parts), 0, -1):
        candidate = ".".join(parts[:i])
        if candidate in _OPTIONAL_DEPS or candidate.lower() in _OPTIONAL_DEPS:
            return "SKIP_OPTIONAL_DEP"
    # Common case: module name starts with a known optional prefix
    for opt in _OPTIONAL_DEPS:
        if name.startswith(opt.lower() + "."):
            return "SKIP_OPTIONAL_DEP"
    return None


def _classify_credentials_error(exc: Exception) -> str | None:
    """ValueError or FileNotFoundError that complain about credentials
    are the eager-credentials-in-init anti-pattern. Flag as SKIP_CREDENTIALS
    so the report distinguishes 'component validates creds too eagerly' from
    'component is broken'."""
    msg = str(exc).lower()
    if isinstance(exc, FileNotFoundError):
        path = str(getattr(exc, "filename", "")).lower()
        if "credential" in path or "credentials" in path or "${" in path:
            return "SKIP_CREDENTIALS"
    if "credential" in msg or "credentials_path" in msg or \
       "google_application_credentials" in msg or \
       "api_key" in msg and "must" in msg:
        return "SKIP_CREDENTIALS"
    return None


def _import_module_from_path(module_path: Path):
    """Import a Python module from an arbitrary file path.

    Sets __package__ heuristically to allow relative imports inside
    component.py to resolve. The convention is: registry components live
    at <category>/<id>/component.py with a sibling __init__.py. We use
    `_component_under_test_<id>` as the package name so relative imports
    resolve to other files in the same dir.
    """
    parent = module_path.parent
    pkg_name = f"_component_under_test_{parent.name}"
    # First ensure the package itself is importable (so relative imports work).
    pkg_init = parent / "__init__.py"
    if pkg_init.exists():
        pkg_spec = importlib.util.spec_from_file_location(
            pkg_name, pkg_init, submodule_search_locations=[str(parent)],
        )
        if pkg_spec is None or pkg_spec.loader is None:
            raise ImportError(f"could not spec parent package at {pkg_init}")
        pkg_mod = importlib.util.module_from_spec(pkg_spec)
        sys.modules[pkg_name] = pkg_mod
        try:
            pkg_spec.loader.exec_module(pkg_mod)
        except Exception:
            # If the __init__.py itself can't load (e.g. because it imports
            # the component class which has a top-level dep we don't have),
            # we still try to load component.py directly without the package.
            sys.modules.pop(pkg_name, None)
            pkg_name = ""

    full_name = f"{pkg_name}.component" if pkg_name else f"_loose_{parent.name}_component"
    spec = importlib.util.spec_from_file_location(full_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not spec module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    if pkg_name:
        sys.modules[full_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _pick_primary_yaml_doc(docs: list) -> dict | None:
    """Pick the first non-empty YAML document that has both a `type:` field
    and an `attributes:` field. Some example.yaml files have multiple docs
    separated by `---` (one verified, one speculative) — we want the first
    fully-specified one."""
    for d in docs:
        if isinstance(d, dict) and d.get("type") and "attributes" in d:
            return d
    return None


def _validate_one(entry: dict) -> dict:
    cid = entry.get("id", "(unknown)")
    result: dict = {
        "id": cid,
        "path": entry.get("path"),
        "status": "PASS",
        "warnings": [],
        "error": None,
        "error_class": None,
    }

    try:
        path = REPO_ROOT / entry["path"]
        component_py = path / "component.py"
        example_yaml = path / "example.yaml"
        if not component_py.exists():
            raise FileNotFoundError(f"component.py missing at {component_py}")
        if not example_yaml.exists():
            raise FileNotFoundError(f"example.yaml missing at {example_yaml}")

        with example_yaml.open() as f:
            docs = list(yaml.safe_load_all(f))
        primary = _pick_primary_yaml_doc(docs)
        if primary is None:
            result["status"] = "SKIP_EXAMPLE_MULTI_DOC"
            result["error"] = "example.yaml has no document with both `type:` and `attributes:`"
            return result

        attributes = primary.get("attributes", {}) or {}

        warnings_caught: list[str] = []
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                mod = _import_module_from_path(component_py)
            except ModuleNotFoundError as mnf:
                skip = _classify_module_not_found(mnf)
                if skip:
                    result["status"] = skip
                    result["error"] = f"{type(mnf).__name__}: {mnf}"
                    return result
                raise
            for entry_w in w:
                warnings_caught.append(f"{entry_w.category.__name__}: {entry_w.message}")

        # Resolve class name from example yaml's `type:` field.
        type_str = primary.get("type", "")
        cls_name = type_str.rsplit(".", 1)[-1]
        cls = getattr(mod, cls_name, None)
        if cls is None:
            raise AttributeError(f"class {cls_name!r} not found in {component_py}")

        # Instantiate.
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                instance = cls(**attributes)
            except TypeError:
                # Class may use Pydantic alias-by-name; try model_validate.
                if hasattr(cls, "model_validate"):
                    instance = cls.model_validate(attributes)
                else:
                    raise
            for entry_w in w:
                warnings_caught.append(f"{entry_w.category.__name__}: {entry_w.message}")

        # Call build_defs with our stub context.
        with warnings.catch_warnings(record=True) as w, \
             contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            warnings.simplefilter("always")
            ctx = _StubComponentLoadContext()
            defs = instance.build_defs(ctx)
            for entry_w in w:
                warnings_caught.append(f"{entry_w.category.__name__}: {entry_w.message}")

        if defs is None:
            raise ValueError("build_defs() returned None")

        result["warnings"] = warnings_caught
        if any(
            ("shadows an attribute in parent" in w_str) or
            ("Field name " in w_str and "conflict" in w_str)
            for w_str in warnings_caught
        ):
            result["status"] = "WARN"

    except ModuleNotFoundError as exc:
        # In case a deeper import (not from _import_module_from_path)
        # raised it during instantiation / build_defs.
        skip = _classify_module_not_found(exc)
        result["status"] = skip or "FAIL"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["error_class"] = type(exc).__name__
        if not skip:
            result["traceback"] = traceback.format_exc().splitlines()[-5:]
    except (ValueError, FileNotFoundError) as exc:
        cred_skip = _classify_credentials_error(exc)
        result["status"] = cred_skip or "FAIL"
        result["error"] = str(exc)
        result["error_class"] = type(exc).__name__
        if not cred_skip:
            result["traceback"] = traceback.format_exc().splitlines()[-5:]
    except ImportError as exc:
        msg = str(exc)
        if "relative import" in msg.lower() or "no known parent package" in msg.lower():
            result["status"] = "SKIP_RELATIVE_IMPORT"
            result["error"] = msg
        elif "pip install" in msg.lower() or "required: pip install" in msg.lower():
            # Component raises ImportError with explicit install hint —
            # the dep is genuinely optional; categorize as skip.
            result["status"] = "SKIP_OPTIONAL_DEP"
            result["error"] = msg
        else:
            result["status"] = "FAIL"
            result["error"] = msg
            result["error_class"] = type(exc).__name__
            result["traceback"] = traceback.format_exc().splitlines()[-5:]
    except AttributeError as exc:
        msg = str(exc)
        # Stub context missing attr — classify as SKIP_CONTEXT_MISSING_ATTR
        # so we can improve the stub on the next pass; not a component bug.
        if "_StubComponentLoadContext" in msg or "no attribute" in msg.lower():
            # Heuristic: if the missing attr is something we'd expect on
            # ComponentLoadContext, mark it as a stub gap. Otherwise it's
            # a real AttributeError inside the component.
            result["status"] = "SKIP_CONTEXT_MISSING_ATTR" if "_StubComponentLoadContext" in msg else "FAIL"
        else:
            result["status"] = "FAIL"
        result["error"] = msg
        result["error_class"] = type(exc).__name__
        if result["status"] == "FAIL":
            result["traceback"] = traceback.format_exc().splitlines()[-5:]
    except Exception as exc:
        cls_name = type(exc).__name__
        msg = str(exc)
        # Some components instantiate Dagster project-aware classes (e.g.
        # DbtCli, DbtProject, dg.PythonComponent) inside build_defs that
        # walk up looking for pyproject.toml. That's a real-project
        # dependency the harness can't satisfy from a tmp dir; categorize
        # as SKIP_PROJECT_REQUIRED.
        if "dg.toml" in msg or "pyproject.toml" in msg:
            result["status"] = "SKIP_PROJECT_REQUIRED"
            result["error"] = msg
        # Components may raise their own ImportError-ish errors as plain
        # exceptions with install hints.
        elif ("pip install" in msg.lower()
              and ("required" in msg.lower() or "install with" in msg.lower())):
            result["status"] = "SKIP_OPTIONAL_DEP"
            result["error"] = msg
        else:
            result["status"] = "FAIL"
            result["error"] = msg
            result["error_class"] = cls_name
            result["traceback"] = traceback.format_exc().splitlines()[-5:]

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ids", default=None, help="Comma-separated ids to validate")
    parser.add_argument("--jobs", type=int, default=4, help="Parallel workers (default 4)")
    parser.add_argument("--report", default=str(REPORT_PATH), help="JSON report output path")
    parser.add_argument("--fail-on-warn", action="store_true", help="Exit nonzero if any WARN")
    args = parser.parse_args()

    m = json.loads(MANIFEST.read_text())
    items = m["components"]

    if args.ids:
        wanted = {s.strip() for s in args.ids.split(",") if s.strip()}
        items = [c for c in items if c.get("id") in wanted]
        print(f"Validating {len(items)} components matching --ids", flush=True)
    else:
        print(f"Validating {len(items)} components from manifest", flush=True)

    results: list[dict] = []
    if args.jobs <= 1:
        for c in items:
            results.append(_validate_one(c))
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
            future_to_id = {ex.submit(_validate_one, c): c.get("id") for c in items}
            for fut in concurrent.futures.as_completed(future_to_id):
                results.append(fut.result())

    results.sort(key=lambda r: r["id"])

    # Summarize by status category.
    from collections import Counter
    counts = Counter(r["status"] for r in results)
    print()
    print("=== L1 Validation Summary ===")
    print(f"  Total:                       {len(results)}")
    print(f"  ✓ PASS:                      {counts.get('PASS', 0)}")
    print(f"  ! WARN:                      {counts.get('WARN', 0)}")
    print(f"  ✗ FAIL:                      {counts.get('FAIL', 0)}")
    for skip_cat in sorted(k for k in counts if k.startswith('SKIP_')):
        print(f"  - {skip_cat}: {counts[skip_cat]}")
    print()

    fails = [r for r in results if r["status"] == "FAIL"]
    if fails:
        print(f"FAILURES ({len(fails)}) — these are real defects:")
        # Group by error class for triage
        by_cls = {}
        for r in fails:
            by_cls.setdefault(r.get("error_class", "Unknown"), []).append(r)
        for cls, group in sorted(by_cls.items()):
            print(f"  [{cls}] ({len(group)}):")
            for r in group[:10]:
                err = r["error"][:100] + ("..." if len(r["error"]) > 100 else "")
                print(f"    - {r['id']}: {err}")
            if len(group) > 10:
                print(f"    ... and {len(group) - 10} more")
        print()

    warns = [r for r in results if r["status"] == "WARN"]
    if warns:
        print(f"WARNINGS ({len(warns)}) — load OK but emitted a warning:")
        for r in warns[:15]:
            w0 = r["warnings"][0] if r["warnings"] else "(unrecorded)"
            print(f"  - {r['id']}: {w0[:120]}")
        if len(warns) > 15:
            print(f"  ... and {len(warns) - 15} more")
        print()

    Path(args.report).write_text(
        json.dumps({"summary": dict(counts), "results": results}, indent=2, default=str)
    )
    print(f"Report written to: {args.report}")

    if counts.get("FAIL", 0) > 0:
        return 1
    if args.fail_on_warn and counts.get("WARN", 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
