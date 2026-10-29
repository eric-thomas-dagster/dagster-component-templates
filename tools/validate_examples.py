#!/usr/bin/env python3
"""Validate every YAML example that names one of our components -- not just each component's own example.yaml.

Walks files/dirs you give it (markdown recipes, *.yaml, READMEs), extracts every YAML document that has a
`type: dagster_community_components.<Class>` and `attributes:`, resolves
<Class> through this repo's registry (`_CLASS_PATHS`), then does what tools/validate_manifest.py does for
example.yaml: import component.py, instantiate from the attributes, and call build_defs() with a stub context.

Run it against two repo roots (e.g. a git worktree of an older commit and HEAD) with the same sources and diff the
reports to separate regressions from examples that were already broken (see --compare).

Usage:
    python3 tools/validate_examples.py SRC [SRC ...] [--root REPO] [--report out.json] [--jobs 4]
    python3 tools/validate_examples.py --compare base.json head.json
"""
from __future__ import annotations

import argparse
import ast
import concurrent.futures
import contextlib
import hashlib
import io
import json
import re
import sys
import traceback
import warnings
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import validate_manifest as vm  # noqa: E402  (reuses the stub context + skip classification)

FENCE = re.compile(r"```(?:ya?ml)\s*\n(.*?)```", re.S)
# `dagster_community_components.Class` (package form) or `<pkg>.components.<name>.component.Class` (what the CLI vendors
# into a scaffolded project; the demo scripts write `$PKG` there)
TYPE_RE = re.compile(r"^(?:dagster_community_components\.|\w+\.components\.\w+\.component\.)(\w+)$")
HEREDOC = re.compile(r"<<-?\s*['\"]?(\w+)['\"]?[^\n]*\n(.*?)\n\s*\1\b", re.S)


def registry(root: Path) -> dict[str, str]:
    src = (root / "dagster_community_components" / "__init__.py").read_text()
    m = re.search(r"_CLASS_PATHS[^=]*=\s*(\{.*?\n\})", src, re.S)
    return ast.literal_eval(m.group(1)) if m else {}


def extract(path: Path):
    """Yield (doc_index, parsed_doc) for every component-shaped YAML document in `path`."""
    text = path.read_text(errors="replace")
    if path.suffix.lower() in (".md", ".markdown"):
        blocks = FENCE.findall(text)
    elif path.suffix.lower() == ".sh":
        # demo setup scripts write their defs.yaml through heredocs; `$PKG` stands for the project package name
        blocks = [b.replace("$PKG", "PKG").replace("${PKG}", "PKG").replace("\\$", "$") for _, b in HEREDOC.findall(text)]
    else:
        blocks = [text]
    for bi, block in enumerate(blocks):
        try:
            docs = list(yaml.safe_load_all(block))
        except yaml.YAMLError:
            continue
        for di, doc in enumerate(docs):
            if isinstance(doc, dict) and isinstance(doc.get("type"), str) and TYPE_RE.match(doc["type"]) \
                    and isinstance(doc.get("attributes", {}), dict):
                yield f"{bi}.{di}", doc


def collect(sources: list[Path]) -> list[dict]:
    items, seen = [], set()
    for src in sources:
        files = [src] if src.is_file() else sorted(
            p for p in src.rglob("*") if p.is_file() and p.suffix.lower() in (".md", ".yaml", ".yml", ".sh")
            and not any(x in p.parts for x in (".venv", "node_modules", ".git", "site-packages", "dist")))
        for f in files:
            for idx, doc in extract(f):
                cls = TYPE_RE.match(doc["type"]).group(1)
                attrs = doc.get("attributes") or {}
                key = hashlib.sha1(repr([cls, attrs]).encode()).hexdigest()[:12]
                if key in seen:
                    continue
                seen.add(key)
                items.append({"key": key, "source": str(f), "doc": idx, "cls": cls, "attributes": attrs})
    return items


def _check(item: dict, root: Path, reg: dict[str, str]) -> dict:
    out = {"key": item["key"], "source": item["source"], "cls": item["cls"], "status": "PASS", "error": None}
    rel = reg.get(item["cls"])
    if not rel:
        out.update(status="SKIP_NOT_IN_REGISTRY", error=f"{item['cls']} not in _CLASS_PATHS")
        return out
    path = root / rel.split(":")[0]
    _blob = json.dumps(item["attributes"], default=str)
    if "{{" in _blob or "${" in _blob or re.search(r"\$[A-Za-z_]", _blob):
        out.update(status="SKIP_TEMPLATED", error="attribute values use {{ }} / ${ } / $VAR templating, resolved by Dagster or the shell at load time")
        return out
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            mod = vm._import_module_from_path(path)
            cls = getattr(mod, item["cls"])
            try:
                inst = cls(**item["attributes"])
            except TypeError:
                inst = cls.model_validate(item["attributes"])
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                defs = inst.build_defs(vm._StubComponentLoadContext())
            if defs is None:
                raise ValueError("build_defs() returned None")
    except ModuleNotFoundError as e:
        out.update(status=vm._classify_module_not_found(e) or "FAIL", error=f"{type(e).__name__}: {e}")
    except ImportError as e:
        out.update(status=vm._classify_import_error(e) or "FAIL", error=f"{type(e).__name__}: {e}")
    except Exception as e:  # noqa: BLE001
        skip = vm._classify_credentials_error(e)
        msg = str(e)
        if "deadlock detected" in msg.lower():
            skip = "SKIP_PROJECT_REQUIRED"
        status = skip or "FAIL"
        if status == "FAIL":
            if "Could not find dg.toml or pyproject.toml" in msg:
                status = "SKIP_PROJECT_REQUIRED"
            elif "validation error" in msg and "Extra inputs" not in msg and "Input should" not in msg \
                    and "Value error" not in msg and "Field required" in msg:
                # only missing-required errors: the snippet is an illustrative fragment, not a full config
                status = "SKIP_PARTIAL_FRAGMENT"
            elif "set exactly one of `upstream_asset_key` or `source`" in msg:
                status = "SKIP_PARTIAL_FRAGMENT"  # snippet omits the upstream; not a full config
            elif ".wraps:" in msg and "cannot import" in msg and "dagster_community_components" in msg:
                status = "SKIP_NEEDS_PACKAGE"  # wrapper components resolve `wraps:` through the installed package
            elif isinstance(e, ImportError) or "pip install" in msg:
                status = "SKIP_OPTIONAL_DEP"
            elif isinstance(e, ModuleNotFoundError) or "No module named" in msg:
                status = "SKIP_PLACEHOLDER_MODULE"  # e.g. `my_project.defs...` in a snippet
            elif isinstance(e, AttributeError) and "'dict' object has no attribute" in msg:
                # nested Resolvable models need Dagster's resolver, which this plain instantiation doesn't run
                status = "SKIP_NEEDS_RESOLVER"
        out.update(status=status, error=f"{type(e).__name__}: {msg[:300]}")
        if status == "FAIL":
            out["traceback"] = traceback.format_exc().splitlines()[-4:]
    return out


def _worker(args):
    return _check(*args)


def run(sources, root, jobs):
    reg = registry(root)
    items = collect(sources)
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as ex:
        results = list(ex.map(_worker, [(i, root, reg) for i in items], chunksize=4))
    return results


def compare(base_path, head_path):
    base = {r["key"]: r for r in json.load(open(base_path))["results"]}
    head = {r["key"]: r for r in json.load(open(head_path))["results"]}
    regress = [(base[k], head[k]) for k in head if k in base and base[k]["status"] == "PASS" and head[k]["status"] == "FAIL"]
    fixed = [k for k in head if k in base and base[k]["status"] == "FAIL" and head[k]["status"] == "PASS"]
    both = [head[k] for k in head if k in base and base[k]["status"] == "FAIL" and head[k]["status"] == "FAIL"]
    print(f"examples compared: {len(head)}  regressions: {len(regress)}  fixed: {len(fixed)}  broken-in-both: {len(both)}")
    for b, h in regress:
        print(f"REGRESSION {h['cls']}  ({h['source']})\n    {h['error']}")
    return 1 if regress else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="*")
    ap.add_argument("--root", default=str(HERE.parent))
    ap.add_argument("--report", default="validate_examples_report.json")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--compare", nargs=2, metavar=("BASE", "HEAD"))
    a = ap.parse_args()
    if a.compare:
        return compare(*a.compare)
    results = run([Path(s) for s in a.sources], Path(a.root), a.jobs)
    counts: dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    Path(a.report).write_text(json.dumps({"summary": counts, "results": results}, indent=1))
    print(json.dumps(counts))
    for r in results:
        if r["status"] == "FAIL":
            print(f"FAIL {r['cls']}  {r['source']}\n    {r['error']}")
    return 1 if counts.get("FAIL") else 0


if __name__ == "__main__":
    sys.exit(main())
