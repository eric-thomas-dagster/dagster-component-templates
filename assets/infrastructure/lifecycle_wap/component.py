"""LifecycleWapComponent — Write-Audit-Publish lifecycle for assets.

Wraps an asset's compute in the classic **Write → Audit → Publish** (WAP)
pattern popularized by Iceberg + Netflix data platforms:

1. **Write** — compute produces data; wrapper writes to a STAGING location
   (staging file, staging table, or branch).
2. **Audit** — run configurable quality checks against the staging data.
   Each check emits an `AssetCheckResult` visible in Dagster's asset-check
   panel.
3. **Publish** — on ALL checks pass, promote staging to prod (atomic
   rename, branch fast-forward, staging→prod swap). On ANY check fail,
   apply the `on_fail` policy: quarantine (move to quarantine location),
   discard (delete staging), or tag_and_keep (keep staging with metadata).

Emits ONE Dagster asset with N `AssetCheckSpec`s (one per audit check) so
downstream `AutomationCondition`s can block on failed checks.

## Two shapes — both use the same audit + publish engine

### 1. Component (`LifecycleWapComponent`) — YAML-defined asset

Emit a NEW asset with compute referenced by `compute.python: 'mod:fn'`.
Function returns a DataFrame; the component handles write/audit/publish.

### 2. Decorator (`@lifecycle`) — wraps an EXISTING @dg.asset

Applied BEFORE `@dg.asset` — same engine, no YAML wrangling, keeps the
asset defined in place. Use for assets that live in Python code, not YAML.

## Write backends in v1

- **`kind: filesystem`** — parquet or csv on local FS or fsspec URI
  (`s3://`, `gs://`, `abfs://`). Staging file → audit → atomic move on
  publish.

- **`kind: sql`** — SQLAlchemy-compatible warehouse (Postgres / Snowflake
  / BigQuery / MySQL / DuckDB). Staging table (`{prod}_staging_wap`) →
  audit → transactional swap (RENAME) on publish.

- **`kind: iceberg`** — via `pyiceberg`. Writes to a branch
  (`wap_staging_<timestamp>`) → audit → fast_forward on publish. Requires
  `pyiceberg` installed; skipped with clear error if missing.

## Audit checks in v1

- `row_count_min` — `{kind: row_count_min, min: 1000}`
- `row_count_max` — `{kind: row_count_max, max: 1000000}`
- `col_null_ratio_max` — `{kind: col_null_ratio_max, col: user_id, max: 0.0}`
- `col_unique` — `{kind: col_unique, col: order_id}`
- `col_range_min` — `{kind: col_range_min, col: amount, min: 0}`
- `col_range_max` — `{kind: col_range_max, col: amount, max: 1_000_000}`
- `python` — `{kind: python, python: 'mod.audits:my_check', name: 'my_check'}`
  User function receives the DataFrame and returns `{'passed': bool,
  'description': str, 'metadata': dict}`.

## Publish policies

- `on_pass: publish` (default) — atomic promote to prod.
- `on_pass: discard` — write happened for the audit alone; drop after (dry-run).
- `on_fail: quarantine` (default) — move staging to `quarantine_path` /
  `quarantine_table`. Data preserved for manual inspection.
- `on_fail: discard` — delete staging. Fastest cleanup.
- `on_fail: tag_and_keep` — keep staging in place; add `wap_status=failed`
  metadata for followup jobs.
"""

import functools
import importlib
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Audit-check engine (shared between decorator + component)
# --------------------------------------------------------------------------


def _run_audit_check(check: Dict[str, Any], df) -> Dict[str, Any]:
    """Run one audit check against a pandas DataFrame.

    Returns {passed: bool, name: str, description: str, metadata: dict}.
    """
    kind = check.get("kind")
    name = check.get("name") or (
        f"{kind}_{check.get('col')}" if check.get("col") else str(kind)
    )
    try:
        if kind == "row_count_min":
            threshold = int(check["min"])
            actual = int(len(df))
            passed = actual >= threshold
            return {
                "passed": passed,
                "name": name,
                "description": (
                    f"row count {actual} >= {threshold}" if passed
                    else f"FAIL: row count {actual} < required min {threshold}"
                ),
                "metadata": {"threshold_min": threshold, "actual_rows": actual},
            }
        if kind == "row_count_max":
            threshold = int(check["max"])
            actual = int(len(df))
            passed = actual <= threshold
            return {
                "passed": passed,
                "name": name,
                "description": (
                    f"row count {actual} <= {threshold}" if passed
                    else f"FAIL: row count {actual} > allowed max {threshold}"
                ),
                "metadata": {"threshold_max": threshold, "actual_rows": actual},
            }
        if kind == "col_null_ratio_max":
            col = check["col"]
            threshold = float(check["max"])
            if col not in df.columns:
                return {
                    "passed": False, "name": name,
                    "description": f"FAIL: column {col!r} not present",
                    "metadata": {"col": col},
                }
            n_total = int(len(df))
            n_null = int(df[col].isna().sum())
            ratio = (n_null / n_total) if n_total else 0.0
            passed = ratio <= threshold
            return {
                "passed": passed, "name": name,
                "description": (
                    f"null ratio {ratio:.4f} <= {threshold}" if passed
                    else f"FAIL: null ratio {ratio:.4f} > allowed {threshold}"
                ),
                "metadata": {"col": col, "null_ratio": round(ratio, 6),
                             "null_count": n_null, "row_count": n_total,
                             "threshold_max": threshold},
            }
        if kind == "col_unique":
            col = check["col"]
            if col not in df.columns:
                return {"passed": False, "name": name,
                        "description": f"FAIL: column {col!r} not present",
                        "metadata": {"col": col}}
            n_total = int(len(df))
            n_distinct = int(df[col].nunique(dropna=False))
            passed = n_distinct == n_total
            return {
                "passed": passed, "name": name,
                "description": (
                    f"unique: {n_distinct}/{n_total} distinct" if passed
                    else f"FAIL: duplicates found — {n_distinct}/{n_total} distinct"
                ),
                "metadata": {"col": col, "distinct": n_distinct,
                             "rows": n_total, "duplicates": n_total - n_distinct},
            }
        if kind == "col_range_min":
            col = check["col"]
            threshold = check["min"]
            if col not in df.columns:
                return {"passed": False, "name": name,
                        "description": f"FAIL: column {col!r} not present",
                        "metadata": {"col": col}}
            actual_min = df[col].min()
            passed = actual_min >= threshold
            return {
                "passed": passed, "name": name,
                "description": (
                    f"min({col})={actual_min} >= {threshold}" if passed
                    else f"FAIL: min({col})={actual_min} < required {threshold}"
                ),
                "metadata": {"col": col, "actual_min": str(actual_min),
                             "threshold_min": str(threshold)},
            }
        if kind == "col_range_max":
            col = check["col"]
            threshold = check["max"]
            if col not in df.columns:
                return {"passed": False, "name": name,
                        "description": f"FAIL: column {col!r} not present",
                        "metadata": {"col": col}}
            actual_max = df[col].max()
            passed = actual_max <= threshold
            return {
                "passed": passed, "name": name,
                "description": (
                    f"max({col})={actual_max} <= {threshold}" if passed
                    else f"FAIL: max({col})={actual_max} > allowed {threshold}"
                ),
                "metadata": {"col": col, "actual_max": str(actual_max),
                             "threshold_max": str(threshold)},
            }
        if kind == "python":
            ref = check.get("python")
            if not ref or ":" not in ref:
                return {"passed": False, "name": name,
                        "description": "FAIL: python check missing 'python: mod:fn' ref",
                        "metadata": {}}
            mod_path, fn_name = ref.rsplit(":", 1)
            mod = importlib.import_module(mod_path.strip())
            fn = getattr(mod, fn_name.strip(), None)
            if not callable(fn):
                return {"passed": False, "name": name,
                        "description": f"FAIL: {ref} is not callable",
                        "metadata": {}}
            result = fn(df)
            # Normalize
            if isinstance(result, bool):
                return {"passed": result, "name": name,
                        "description": ("passed" if result else "FAIL: user check returned False"),
                        "metadata": {}}
            if isinstance(result, dict):
                return {
                    "passed": bool(result.get("passed")),
                    "name": name,
                    "description": str(result.get("description", "")),
                    "metadata": dict(result.get("metadata") or {}),
                }
            return {"passed": False, "name": name,
                    "description": f"FAIL: user check returned {type(result).__name__} (expected bool or dict)",
                    "metadata": {}}
        return {"passed": False, "name": name,
                "description": f"FAIL: unknown check kind {kind!r}",
                "metadata": {"kind": str(kind)}}
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "name": name,
                "description": f"FAIL: check raised {type(exc).__name__}: {exc}",
                "metadata": {"error_class": type(exc).__name__}}


def _run_all_audits(checks: List[Dict[str, Any]], df) -> List[Dict[str, Any]]:
    return [_run_audit_check(c, df) for c in checks]


# --------------------------------------------------------------------------
# Write / Publish backends
# --------------------------------------------------------------------------


def _write_staging(df, write_cfg: Dict[str, Any], context) -> Dict[str, Any]:
    """Write DataFrame to staging. Returns backend-specific handle."""
    kind = (write_cfg.get("kind") or "filesystem").lower()
    if kind == "filesystem":
        return _fs_write_staging(df, write_cfg, context)
    if kind == "sql":
        return _sql_write_staging(df, write_cfg, context)
    if kind == "iceberg":
        return _iceberg_write_staging(df, write_cfg, context)
    raise ValueError(f"lifecycle_wap: write kind={kind!r} not supported")


def _publish_or_cleanup(
    staging_handle: Dict[str, Any],
    write_cfg: Dict[str, Any],
    all_passed: bool,
    on_pass: str,
    on_fail: str,
    quarantine_cfg: Optional[Dict[str, Any]],
    context,
) -> Dict[str, Any]:
    """Apply publish policy. Returns outcome metadata."""
    kind = (write_cfg.get("kind") or "filesystem").lower()
    policy = on_pass if all_passed else on_fail
    if kind == "filesystem":
        return _fs_publish(staging_handle, write_cfg, policy, quarantine_cfg, context)
    if kind == "sql":
        return _sql_publish(staging_handle, write_cfg, policy, quarantine_cfg, context)
    if kind == "iceberg":
        return _iceberg_publish(staging_handle, write_cfg, policy, quarantine_cfg, context)
    raise ValueError(f"lifecycle_wap: publish kind={kind!r} not supported")


# ── Filesystem backend ────────────────────────────────────────────────

def _fs_write_staging(df, cfg: Dict[str, Any], context) -> Dict[str, Any]:
    prod = cfg["prod_path"]
    staging = cfg.get("staging_path") or _derive_staging_fs(prod)
    fmt = (cfg.get("format") or _infer_fmt(prod)).lower()
    if "://" in staging:
        import fsspec
        fs, root = fsspec.core.url_to_fs(staging)
        parent = "/".join(root.split("/")[:-1])
        try: fs.makedirs(parent, exist_ok=True)
        except Exception: pass
        # Write via fsspec — pandas can consume storage_options via URL
        if fmt == "parquet":
            df.to_parquet(staging)
        elif fmt == "csv":
            df.to_csv(staging, index=False)
        elif fmt == "json":
            df.to_json(staging, orient="records", lines=True)
        else:
            raise ValueError(f"lifecycle_wap: format={fmt!r} not supported")
    else:
        Path(staging).parent.mkdir(parents=True, exist_ok=True)
        if fmt == "parquet":
            df.to_parquet(staging)
        elif fmt == "csv":
            df.to_csv(staging, index=False)
        elif fmt == "json":
            df.to_json(staging, orient="records", lines=True)
        else:
            raise ValueError(f"lifecycle_wap: format={fmt!r} not supported")
    context.log.info(f"[lifecycle_wap] wrote staging: {staging}")
    return {"staging_path": staging, "prod_path": prod, "format": fmt}


def _fs_publish(handle, cfg, policy: str, quarantine_cfg, context) -> Dict[str, Any]:
    staging = handle["staging_path"]
    prod = handle["prod_path"]
    if policy == "publish":
        if "://" in staging:
            import fsspec
            fs, root = fsspec.core.url_to_fs(staging)
            fs_prod, prod_path = fsspec.core.url_to_fs(prod)
            # If same fs, use mv; otherwise copy+delete
            if fs.protocol == fs_prod.protocol:
                try: fs_prod.makedirs("/".join(prod_path.split("/")[:-1]), exist_ok=True)
                except Exception: pass
                fs.mv(root, prod_path)
            else:
                with fs.open(root, "rb") as src, fs_prod.open(prod_path, "wb") as dst:
                    dst.write(src.read())
                fs.rm(root)
            context.log.info(f"[lifecycle_wap] PUBLISHED {staging} → {prod}")
        else:
            Path(prod).parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, prod)
            context.log.info(f"[lifecycle_wap] PUBLISHED {staging} → {prod}")
        return {"outcome": "published", "prod_path": prod}
    if policy == "quarantine":
        q_path = (quarantine_cfg or {}).get("quarantine_path")
        if not q_path:
            # Derive one alongside prod
            q_path = _derive_quarantine_fs(prod)
        if "://" in staging:
            import fsspec
            fs, root = fsspec.core.url_to_fs(staging)
            fs_q, q_path_x = fsspec.core.url_to_fs(q_path)
            try: fs_q.makedirs("/".join(q_path_x.split("/")[:-1]), exist_ok=True)
            except Exception: pass
            if fs.protocol == fs_q.protocol:
                fs.mv(root, q_path_x)
            else:
                with fs.open(root, "rb") as src, fs_q.open(q_path_x, "wb") as dst:
                    dst.write(src.read())
                fs.rm(root)
        else:
            Path(q_path).parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, q_path)
        context.log.warning(f"[lifecycle_wap] QUARANTINED {staging} → {q_path}")
        return {"outcome": "quarantined", "quarantine_path": q_path}
    if policy == "discard":
        try:
            if "://" in staging:
                import fsspec
                fs, root = fsspec.core.url_to_fs(staging)
                fs.rm(root)
            else:
                Path(staging).unlink(missing_ok=True)
        except Exception as e:  # noqa: BLE001
            context.log.warning(f"[lifecycle_wap] discard cleanup failed: {e}")
        context.log.info(f"[lifecycle_wap] DISCARDED staging")
        return {"outcome": "discarded"}
    if policy == "tag_and_keep":
        context.log.info(f"[lifecycle_wap] kept staging at {staging} (tag_and_keep)")
        return {"outcome": "kept", "staging_path": staging}
    raise ValueError(f"lifecycle_wap: policy={policy!r} not supported")


def _derive_staging_fs(prod: str) -> str:
    """Derive a `.staging` sibling path."""
    if "://" in prod:
        head, _, tail = prod.rpartition("/")
        return f"{head}/.staging/{tail}"
    p = Path(prod)
    return str(p.parent / ".staging" / p.name)


def _derive_quarantine_fs(prod: str) -> str:
    if "://" in prod:
        head, _, tail = prod.rpartition("/")
        return f"{head}/.quarantine/{tail}.{int(time.time())}"
    p = Path(prod)
    return str(p.parent / ".quarantine" / f"{p.name}.{int(time.time())}")


def _infer_fmt(path: str) -> str:
    p = path.lower()
    if p.endswith(".parquet") or p.endswith(".pq"): return "parquet"
    if p.endswith(".csv"): return "csv"
    if p.endswith(".json") or p.endswith(".jsonl"): return "json"
    return "parquet"


# ── SQL backend ───────────────────────────────────────────────────────

def _sql_write_staging(df, cfg: Dict[str, Any], context) -> Dict[str, Any]:
    engine = _sql_get_engine(cfg, context)
    prod_table = cfg["prod_table"]
    schema = cfg.get("schema")
    staging_table = cfg.get("staging_table") or f"{prod_table}_staging_wap"
    df.to_sql(staging_table, engine, schema=schema, if_exists="replace", index=False)
    context.log.info(f"[lifecycle_wap] wrote staging: {schema+'.' if schema else ''}{staging_table} ({len(df)} rows)")
    return {"engine": engine, "staging_table": staging_table,
            "prod_table": prod_table, "schema": schema}


def _sql_publish(handle, cfg, policy: str, quarantine_cfg, context) -> Dict[str, Any]:
    from sqlalchemy import text
    engine = handle["engine"]
    schema = handle["schema"]
    schema_p = f"{schema}." if schema else ""
    staging = handle["staging_table"]
    prod = handle["prod_table"]
    if policy == "publish":
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema_p}{prod}"))
            conn.execute(text(f"ALTER TABLE {schema_p}{staging} RENAME TO {prod}"))
        context.log.info(f"[lifecycle_wap] PUBLISHED {schema_p}{staging} → {schema_p}{prod}")
        return {"outcome": "published", "prod_table": prod}
    if policy == "quarantine":
        q_table = (quarantine_cfg or {}).get("quarantine_table") or f"{prod}_quarantine_{int(time.time())}"
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {schema_p}{staging} RENAME TO {q_table}"))
        context.log.warning(f"[lifecycle_wap] QUARANTINED {schema_p}{staging} → {schema_p}{q_table}")
        return {"outcome": "quarantined", "quarantine_table": q_table}
    if policy == "discard":
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema_p}{staging}"))
        context.log.info(f"[lifecycle_wap] DISCARDED staging table {schema_p}{staging}")
        return {"outcome": "discarded"}
    if policy == "tag_and_keep":
        context.log.info(f"[lifecycle_wap] kept staging at {schema_p}{staging}")
        return {"outcome": "kept", "staging_table": staging}
    raise ValueError(f"lifecycle_wap: policy={policy!r} not supported")


def _sql_get_engine(cfg: Dict[str, Any], context):
    """Return a sqlalchemy engine from `database_url_env_var` or a Dagster resource."""
    env = cfg.get("database_url_env_var")
    if env:
        from sqlalchemy import create_engine
        url = os.environ.get(env, "")
        if not url:
            raise ValueError(f"lifecycle_wap: database_url_env_var {env!r} unset")
        return create_engine(url)
    rk = cfg.get("resource_key")
    if rk:
        resource = getattr(context.resources, rk)
        if hasattr(resource, "get_engine"):
            return resource.get_engine()
        if hasattr(resource, "get_connection"):
            return resource.get_connection()
        raise ValueError(f"lifecycle_wap: resource {rk!r} must expose get_engine() or get_connection()")
    raise ValueError("lifecycle_wap: sql write requires database_url_env_var OR resource_key")


# ── Iceberg backend ───────────────────────────────────────────────────

def _iceberg_write_staging(df, cfg: Dict[str, Any], context) -> Dict[str, Any]:
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError as e:
        raise ImportError(
            "lifecycle_wap: iceberg backend requires `pyiceberg`. "
            "Install with `pip install 'pyiceberg[pyarrow]'`."
        ) from e
    catalog_name = cfg.get("catalog") or "default"
    table_id = cfg["table"]
    branch = cfg.get("staging_branch") or f"wap_staging_{int(time.time())}"
    catalog = load_catalog(catalog_name)
    table = catalog.load_table(table_id)
    # Create branch off main + append rows to branch.
    try:
        table.manage_snapshots().create_branch(branch, ref="main").commit()
    except Exception as e:  # noqa: BLE001
        # Branch may already exist — that's fine for retries.
        context.log.warning(f"[lifecycle_wap] iceberg create_branch: {e}")
    import pyarrow as pa
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    table.append(tbl, snapshot_properties={"wap.branch": branch})
    context.log.info(f"[lifecycle_wap] wrote iceberg staging branch {branch!r} on {table_id}")
    return {"table_id": table_id, "branch": branch, "catalog": catalog_name}


def _iceberg_publish(handle, cfg, policy: str, quarantine_cfg, context) -> Dict[str, Any]:
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog(handle["catalog"])
    table = catalog.load_table(handle["table_id"])
    branch = handle["branch"]
    if policy == "publish":
        table.manage_snapshots().fast_forward("main", branch).commit()
        context.log.info(f"[lifecycle_wap] PUBLISHED iceberg branch {branch!r} → main")
        return {"outcome": "published", "iceberg_branch": branch}
    if policy in ("discard", "quarantine", "tag_and_keep"):
        # For iceberg, quarantine == keep branch (with tag), discard == drop branch.
        if policy == "discard":
            try:
                table.manage_snapshots().remove_branch(branch).commit()
                context.log.info(f"[lifecycle_wap] DISCARDED iceberg branch {branch!r}")
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"[lifecycle_wap] iceberg branch discard failed: {e}")
            return {"outcome": "discarded"}
        if policy == "quarantine":
            q_tag = (quarantine_cfg or {}).get("quarantine_tag") or f"wap_quarantine_{int(time.time())}"
            table.manage_snapshots().create_tag(q_tag, table.refs()[branch].snapshot_id).commit()
            context.log.warning(f"[lifecycle_wap] QUARANTINED iceberg — tagged {q_tag} on {branch!r}")
            return {"outcome": "quarantined", "iceberg_tag": q_tag}
        # tag_and_keep
        return {"outcome": "kept", "iceberg_branch": branch}
    raise ValueError(f"lifecycle_wap: policy={policy!r} not supported")


# --------------------------------------------------------------------------
# @lifecycle decorator — wraps any @dg.asset compute
# --------------------------------------------------------------------------


def lifecycle(
    write: Dict[str, Any],
    audit: List[Dict[str, Any]],
    *,
    on_pass: str = "publish",
    on_fail: str = "quarantine",
    quarantine: Optional[Dict[str, Any]] = None,
    raise_on_fail: bool = True,
) -> Callable:
    """Wrap a compute function with Write-Audit-Publish (WAP) lifecycle.

    Applied BEFORE `@dg.asset` — the wrapped function's return value must
    be a pandas DataFrame. The decorator writes it to staging, runs audits,
    then publishes / quarantines / discards based on results.

    ```python
    from dagster_community_components import lifecycle

    @dg.asset(check_specs=[
        dg.AssetCheckSpec(name="rows_gte_1000", asset="daily_orders"),
        dg.AssetCheckSpec(name="user_id_no_nulls", asset="daily_orders"),
    ])
    @lifecycle(
        write={"kind": "filesystem",
               "prod_path": "data/orders.parquet",
               "format": "parquet"},
        audit=[
            {"kind": "row_count_min", "min": 1000, "name": "rows_gte_1000"},
            {"kind": "col_null_ratio_max", "col": "user_id", "max": 0.0,
             "name": "user_id_no_nulls"},
        ],
        on_pass="publish",
        on_fail="quarantine",
    )
    def daily_orders(context):
        return build_dataset()   # returns a pandas DataFrame
    ```

    Args match `LifecycleWapComponent`. `raise_on_fail=True` (default)
    raises `dg.Failure` when any check fails; downstream sees the failure
    and blocks. Set `False` to always materialize + rely on AssetCheckResult
    signals downstream (via `AutomationCondition`).
    """
    if on_pass not in ("publish", "discard"):
        raise ValueError(f"on_pass must be publish|discard; got {on_pass!r}")
    if on_fail not in ("quarantine", "discard", "tag_and_keep"):
        raise ValueError(f"on_fail must be quarantine|discard|tag_and_keep; got {on_fail!r}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            import pandas as pd
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError(
                    "@lifecycle requires a Dagster context — decorator "
                    "must wrap a Dagster asset/op compute function."
                )
            df = fn(*args, **kwargs)
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"@lifecycle: compute must return a pandas DataFrame; got {type(df).__name__}."
                )
            handle = _write_staging(df, write, context)
            audit_results = _run_all_audits(audit, df)
            all_passed = all(r["passed"] for r in audit_results)
            publish_outcome = _publish_or_cleanup(
                handle, write, all_passed, on_pass, on_fail, quarantine, context,
            )

            # Yield AssetCheckResult per audit + then the primary output.
            for r in audit_results:
                yield dg.AssetCheckResult(
                    check_name=r["name"],
                    passed=r["passed"],
                    severity=dg.AssetCheckSeverity.ERROR if not r["passed"] else dg.AssetCheckSeverity.WARN,
                    description=r["description"],
                    metadata={k: dg.MetadataValue.text(str(v)) for k, v in r["metadata"].items()},
                )
            _md = {
                "wap_write_kind": write.get("kind", "filesystem"),
                "wap_all_passed": all_passed,
                "wap_publish_outcome": publish_outcome.get("outcome", ""),
                "wap_row_count": len(df),
                "wap_check_summary": (
                    f"{sum(1 for r in audit_results if r['passed'])}/{len(audit_results)} passed"
                ),
            }
            for k, v in publish_outcome.items():
                _md[f"wap_{k}"] = v
            yield dg.Output(df, metadata={k: dg.MetadataValue.text(str(v)) for k, v in _md.items()})

            if raise_on_fail and not all_passed:
                failed_names = ", ".join(r["name"] for r in audit_results if not r["passed"])
                raise dg.Failure(
                    description=f"@lifecycle audit FAILED: {failed_names}",
                    metadata={
                        "failed_checks": dg.MetadataValue.json(
                            [r for r in audit_results if not r["passed"]]
                        ),
                    },
                )

        return _wrapped

    return _decorator


# --------------------------------------------------------------------------
# LifecycleWapComponent — YAML-defined new asset
# --------------------------------------------------------------------------


class LifecycleWapComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the WAP lifecycle. Defines a new asset whose compute is
    referenced by `compute.python: 'mod:fn'`.

    For an EXISTING asset defined in Python, use the `@lifecycle` decorator
    instead — same audit + publish engine, no YAML wrangling.
    """

    asset_name: str = Field(description="Dagster asset name emitted by this component.")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Optional upstream asset passed as second arg to compute.python.",
    )
    compute: Dict[str, Any] = Field(
        description=(
            "Compute config. Shape: `{kind: python, python: 'mod:fn'}`. Function "
            "returns a pandas DataFrame; the component handles write/audit/publish."
        ),
    )

    write: Dict[str, Any] = Field(
        description=(
            "Write backend. Shape (filesystem): `{kind: filesystem, prod_path, "
            "staging_path?, format?: parquet|csv|json}`. Shape (sql): `{kind: sql, "
            "resource_key|database_url_env_var, prod_table, staging_table?, schema?}`. "
            "Shape (iceberg): `{kind: iceberg, catalog, table, staging_branch?}`."
        ),
    )
    audit: List[Dict[str, Any]] = Field(
        description=(
            "Ordered audit checks. Each: `{kind: row_count_min|row_count_max|"
            "col_null_ratio_max|col_unique|col_range_min|col_range_max|python, "
            "name?, ...kind-specific fields}`. Every check becomes an AssetCheckSpec."
        ),
    )
    on_pass: str = Field(
        default="publish",
        description="Publish policy on ALL checks pass: `publish` (default) or `discard`.",
    )
    on_fail: str = Field(
        default="quarantine",
        description="Publish policy on ANY check fail: `quarantine` (default), `discard`, `tag_and_keep`.",
    )
    quarantine: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Quarantine target. Filesystem: `{quarantine_path}`. SQL: "
            "`{quarantine_table}`. Iceberg: `{quarantine_tag}`. Omit to auto-derive."
        ),
    )
    raise_on_fail: bool = Field(
        default=True,
        description=(
            "If true (default), raise `dg.Failure` when any audit check fails. "
            "Set false to always materialize the asset + rely on AssetCheckResult "
            "signals downstream via AutomationCondition."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'wap', 'lifecycle'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Lifecycle (WAP)", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        write_cfg = dict(self.write)
        audit_cfg = list(self.audit)
        on_pass = self.on_pass
        on_fail = self.on_fail
        quarantine_cfg = dict(self.quarantine) if self.quarantine else None
        raise_on_fail = self.raise_on_fail

        if on_pass not in ("publish", "discard"):
            raise ValueError(f"on_pass must be publish|discard; got {on_pass!r}")
        if on_fail not in ("quarantine", "discard", "tag_and_keep"):
            raise ValueError(f"on_fail must be quarantine|discard|tag_and_keep; got {on_fail!r}")

        kinds_set = set(self.kinds or []) | {"python", "wap", "lifecycle"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        # Build AssetCheckSpec per audit check — Dagster UI shows each on the asset.
        check_specs = []
        for i, check in enumerate(audit_cfg):
            _name = check.get("name") or (
                f"{check.get('kind')}_{check.get('col')}" if check.get("col")
                else f"{check.get('kind')}_{i}"
            )
            check_specs.append(dg.AssetCheckSpec(
                name=_name,
                asset=dg.AssetKey.from_user_string(asset_name),
                description=f"WAP audit: {check.get('kind')}",
            ))

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        # Auto-detect required resources.
        required_rks = set()
        if (write_cfg.get("kind") or "").lower() == "sql" and write_cfg.get("resource_key"):
            required_rks.add(write_cfg["resource_key"])

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"WAP lifecycle-wrapped asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            check_specs=check_specs,
            ins=ins,
            required_resource_keys=required_rks or None,
        )
        def _wap_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd
            # Resolve compute.
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"LifecycleWapComponent v1 supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                df = fn()
            elif n_positional == 1:
                df = fn(context)
            else:
                df = fn(context, kwargs.get("upstream"))

            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"compute must return a DataFrame; got {type(df).__name__}")

            # Staging → audit → publish.
            handle = _write_staging(df, write_cfg, context)
            audit_results = _run_all_audits(audit_cfg, df)
            all_passed = all(r["passed"] for r in audit_results)
            publish_outcome = _publish_or_cleanup(
                handle, write_cfg, all_passed, on_pass, on_fail, quarantine_cfg, context,
            )

            # Emit AssetCheckResult per audit.
            for r in audit_results:
                yield dg.AssetCheckResult(
                    check_name=r["name"],
                    passed=r["passed"],
                    severity=dg.AssetCheckSeverity.ERROR if not r["passed"] else dg.AssetCheckSeverity.WARN,
                    description=r["description"],
                    metadata={k: dg.MetadataValue.text(str(v)) for k, v in r["metadata"].items()},
                )

            _md = {
                "wap_write_kind": write_cfg.get("kind", "filesystem"),
                "wap_all_passed": all_passed,
                "wap_publish_outcome": publish_outcome.get("outcome", ""),
                "wap_row_count": len(df),
                "wap_check_summary": (
                    f"{sum(1 for r in audit_results if r['passed'])}/{len(audit_results)} passed"
                ),
            }
            for k, v in publish_outcome.items():
                _md[f"wap_{k}"] = v
            yield dg.Output(df, metadata={k: dg.MetadataValue.text(str(v)) for k, v in _md.items()})

            if raise_on_fail and not all_passed:
                failed_names = ", ".join(r["name"] for r in audit_results if not r["passed"])
                raise dg.Failure(
                    description=f"WAP audit FAILED: {failed_names}",
                    metadata={
                        "failed_checks": dg.MetadataValue.json(
                            [r for r in audit_results if not r["passed"]]
                        ),
                    },
                )

        return dg.Definitions(assets=[_wap_asset])
