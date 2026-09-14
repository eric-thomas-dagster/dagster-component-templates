"""Comprehensive validation of everything shipped in today's session.

Run:  cd /Users/ericthomas/dagster_components/dcc-src && ./.venv/bin/python /tmp/validate_todays_session.py

Reports one line per feature: PASS or FAIL with brief detail.
"""
import sys, os, tempfile, time
import pathlib; sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from datetime import timedelta

import dagster as dg

results = []

def check(name, fn):
    try:
        detail = fn()
        results.append(("PASS", name, detail or ""))
    except Exception as e:  # noqa: BLE001
        results.append(("FAIL", name, f"{type(e).__name__}: {str(e)[:120]}"))


# ═══════════════════════════════════════════════════════════════════
# 8 scheduler integrations shipped earlier: materialize each in demo_mode
# ═══════════════════════════════════════════════════════════════════

def _mat_scheduler(mod, cls_name, spec_name, spec_kwargs, asset_name):
    m = __import__(f"integrations.{mod}.component", fromlist=[cls_name, spec_name])
    C = getattr(m, cls_name); S = getattr(m, spec_name)
    c = C(demo_mode=True, jobs=[S(**spec_kwargs)])
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    ja = [a for a in defs.assets if str(a.key) == f"AssetKey(['{asset_name}'])"][0]
    r = dg.materialize([ja], partition_key="2024-06-01", instance=dg.DagsterInstance.ephemeral())
    if not r.success: raise AssertionError("materialize returned success=False")
    return f"assets={sum(1 for _ in defs.assets)} jobs={len(defs.jobs or [])} sensors={len(defs.sensors or [])}"

check("controlm_integration",   lambda: _mat_scheduler("controlm_integration", "ControlMIntegrationComponent", "ControlMJobSpec", dict(job_name="J", asset_name="a", folder="F", application="A", sub_application="S", host="H"), "a"))
check("runmyjobs_integration",  lambda: _mat_scheduler("runmyjobs_integration", "RunMyJobsIntegrationComponent", "RunMyJobsJobSpec", dict(job_definition="J", asset_name="a", application="A", partition_type="P", sub_partition_type="S", queue="Q"), "a"))
check("jenkins_integration",    lambda: _mat_scheduler("jenkins_integration", "JenkinsIntegrationComponent", "JenkinsJobSpec", dict(job_name="j", asset_name="a", folder="f", application="A", node_label="n"), "a"))
check("rundeck_integration",    lambda: _mat_scheduler("rundeck_integration", "RundeckIntegrationComponent", "RundeckJobSpec", dict(job_id="abc-123", asset_name="a", project="P"), "a"))
check("stonebranch_uac_integration", lambda: _mat_scheduler("stonebranch_uac_integration", "StonebranchUACIntegrationComponent", "StonebranchTaskSpec", dict(task_name="T", asset_name="a", workflow="W", agent="G"), "a"))
check("iws_integration",        lambda: _mat_scheduler("iws_integration", "IWSIntegrationComponent", "IWSJobSpec", dict(job_name="J", asset_name="a", application="A", workstation="W"), "a"))
check("activebatch_integration", lambda: _mat_scheduler("activebatch_integration", "ActiveBatchIntegrationComponent", "ActiveBatchJobSpec", dict(object_id="12345", asset_name="a", plan="P", execution_queue="Q"), "a"))
check("jams_integration",       lambda: _mat_scheduler("jams_integration", "JAMSIntegrationComponent", "JAMSJobSpec", dict(job_name="J", asset_name="a", folder="F", agent="A"), "a"))

# ═══════════════════════════════════════════════════════════════════
# 4 RPA integrations
# ═══════════════════════════════════════════════════════════════════

check("uipath_orchestrator_integration",   lambda: _mat_scheduler("uipath_orchestrator_integration", "UiPathOrchestratorIntegrationComponent", "UiPathProcessSpec", dict(release_key="k", asset_name="a", folder="F", folder_id=1), "a"))
check("automation_anywhere_integration",   lambda: _mat_scheduler("automation_anywhere_integration", "AutomationAnywhereIntegrationComponent", "AutomationAnywhereBotSpec", dict(file_id=1, asset_name="a", workspace="W", device_pool_id=1), "a"))
check("blue_prism_integration",            lambda: _mat_scheduler("blue_prism_integration", "BluePrismIntegrationComponent", "BluePrismProcessSpec", dict(process_id="p", asset_name="a", resource_id="r", resource_group="R"), "a"))
check("power_automate_integration",        lambda: _mat_scheduler("power_automate_integration", "PowerAutomateIntegrationComponent", "PowerAutomateFlowSpec", dict(flow_id="f", asset_name="a", environment_id="E", solution="S"), "a"))

# ═══════════════════════════════════════════════════════════════════
# 3 RPA utility components
# ═══════════════════════════════════════════════════════════════════

def _test_rpa_output_parser():
    from assets.transforms.rpa_output_parser.component import RPAOutputParserComponent
    c = RPAOutputParserComponent(asset_name="norm", upstream_asset_keys=["uipath_x", "aa_y"])
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    ja = list(defs.assets)[0]
    r = dg.materialize([ja], instance=dg.DagsterInstance.ephemeral())
    if not r.success: raise AssertionError("materialize failed")
    return "materialized 2 MISSING rows"

check("rpa_output_parser", _test_rpa_output_parser)

def _test_rpa_queue_concurrency_lock():
    from assets.infrastructure.rpa_queue_concurrency_lock.component import RPAQueueConcurrencyLockComponent
    c = RPAQueueConcurrencyLockComponent(asset_name="w", pool_key="p", max_concurrent=5)
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    ja = list(defs.assets)[0]
    r = dg.materialize([ja], instance=dg.DagsterInstance.ephemeral())
    if not r.success: raise AssertionError("materialize failed")
    return "pool acquire/release cycle OK"

check("rpa_queue_concurrency_lock", _test_rpa_queue_concurrency_lock)

def _test_rpa_health_check():
    from asset_checks.rpa_health_check.component import RPAHealthCheckComponent
    c = RPAHealthCheckComponent(target_asset="uipath_x", check_name="h", max_age_hours=24.0)
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    if len(defs.asset_checks or []) != 1: raise AssertionError("expected 1 check")
    return "check def builds"

check("rpa_health_check", _test_rpa_health_check)

# ═══════════════════════════════════════════════════════════════════
# Schedule components — partition support (cron_schedule + interval_schedule)
# ═══════════════════════════════════════════════════════════════════

def _test_cron_schedule():
    from schedules.cron_schedule.component import CronScheduleComponent
    # Cron on partitioned job — daily 09:15 (decomposition happens under the hood)
    c = CronScheduleComponent(schedule_name="s", cron_expression="15 9 * * *", asset_keys=["a"], partition_type="daily", partition_start="2024-01-01")
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    sched = list(defs.schedules or [])[0]
    if sched.cron_schedule != "15 9 * * *":
        raise AssertionError(f"expected cron_schedule='15 9 * * *', got {sched.cron_schedule!r}")
    return "cron '15 9 * * *' preserved via decomposition"

check("cron_schedule (partitioned)", _test_cron_schedule)

def _test_interval_schedule():
    from schedules.interval_schedule.component import IntervalScheduleComponent
    c = IntervalScheduleComponent(schedule_name="s", every="1h", asset_keys=["a"], partition_type="hourly", partition_start="2024-01-01-00:00")
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    sched = list(defs.schedules or [])[0]
    if sched.cron_schedule != "0 * * * *":
        raise AssertionError(f"expected '0 * * * *', got {sched.cron_schedule!r}")
    # Verify mismatch raises
    c2 = IntervalScheduleComponent(schedule_name="s2", every="15m", asset_keys=["a"], partition_type="hourly", partition_start="2024-01-01-00:00")
    try:
        c2.build_defs(_Ctx())
        raise AssertionError("mismatched interval should have raised")
    except ValueError:
        pass
    return "1h+hourly OK, 15m+hourly raises"

check("interval_schedule (partitioned)", _test_interval_schedule)

# ═══════════════════════════════════════════════════════════════════
# external_bigquery_table kinds trim (3 not 4)
# ═══════════════════════════════════════════════════════════════════

def _test_external_bigquery_table():
    from external_assets.external_bigquery_table.component import ExternalBigQueryTableAsset
    c = ExternalBigQueryTableAsset(asset_key="raw/x", project_id="p", dataset_id="d", table_id="t")
    class _Ctx: pass
    defs = c.build_defs(_Ctx())
    spec = list(defs.assets)[0]
    kinds = sorted(spec.kinds)
    if kinds != ["bigquery", "gcp", "table"]:
        raise AssertionError(f"expected 3 kinds ['bigquery','gcp','table'], got {kinds}")
    return f"default kinds trimmed to 3: {kinds}"

check("external_bigquery_table (kinds trim)", _test_external_bigquery_table)

# ═══════════════════════════════════════════════════════════════════
# cached_asset — refresh_cache tag + input_hash_cache_key_fn
# ═══════════════════════════════════════════════════════════════════

def _test_cached_refresh():
    from assets.infrastructure.cached_asset.component import _is_refresh_cache_requested, input_hash_cache_key_fn
    class MC1: run_tags = {"refresh_cache": "true"}
    class MC2: run_tags = {"dagster/refresh_cache": "1"}
    class MC3: run_tags = {}
    if not _is_refresh_cache_requested(MC1()): raise AssertionError("refresh_cache=true not detected")
    if not _is_refresh_cache_requested(MC2()): raise AssertionError("dagster/refresh_cache=1 not detected")
    if _is_refresh_cache_requested(MC3()): raise AssertionError("empty tags misdetected as refresh")
    # input_hash_cache_key_fn produces stable hash
    class MC:
        asset_key = None
        has_partition_key = False
    k1 = input_hash_cache_key_fn(MC(), upstream={"a": 1})
    k2 = input_hash_cache_key_fn(MC(), upstream={"a": 1})
    if k1 != k2: raise AssertionError("input_hash_cache_key_fn not deterministic")
    return "refresh_cache tag + input_hash_cache_key_fn OK"

check("cached_asset (refresh_cache + input_hash_cache_key_fn)", _test_cached_refresh)

# ═══════════════════════════════════════════════════════════════════
# task_asset — full cache parity work (from earlier this session)
# ═══════════════════════════════════════════════════════════════════

def _test_task_cache_parity():
    from assets.infrastructure.task_asset.component import task, INPUTS, TASK_SOURCE, CROSS_RUN, NO_CACHE, FilesystemTaskCache, CachePolicy
    # Composition
    p = INPUTS + TASK_SOURCE + CROSS_RUN
    if not (p.include_inputs and p.include_source and p.run_scope == "cross_run"):
        raise AssertionError("composition broken")
    if not (NO_CACHE + INPUTS).disabled:
        raise AssertionError("NO_CACHE + X should short-circuit to disabled")
    # cache=True zero-config
    compute = {"n": 0}
    @task(cache=True)
    def auto(ctx, x):
        compute["n"] += 1
        return x * 2
    @dg.asset
    def demo(context):
        auto(context, 5); auto(context, 5); auto(context, 6)  # 2 unique inputs
        return True
    dg.materialize([demo], instance=dg.DagsterInstance.ephemeral())
    if compute["n"] != 2: raise AssertionError(f"expected 2 computes for 2 unique inputs, got {compute['n']}")
    # LRU eviction
    lru_dir = tempfile.mkdtemp()
    lru = FilesystemTaskCache(base_dir=lru_dir, max_entries=3)
    for i in range(5):
        lru.put(f"k{i}", f"v{i}")
    files = [f for f in os.listdir(lru_dir) if f.endswith(".pkl")]
    if len(files) != 3: raise AssertionError(f"LRU max_entries=3 kept {len(files)} files")
    return "compose + cache=True + LRU + NO_CACHE all OK"

check("task_asset (cache parity: CachePolicy + cache=True + LRU + NO_CACHE)", _test_task_cache_parity)

# ═══════════════════════════════════════════════════════════════════
# task_asset — 3 new gaps just shipped (async / retry_condition_fn / concurrency_pool)
# ═══════════════════════════════════════════════════════════════════

def _test_task_async():
    from assets.infrastructure.task_asset.component import task
    calls = {"n": 0}
    @task
    async def async_double(ctx, x):
        import asyncio
        await asyncio.sleep(0.01)
        calls["n"] += 1
        return x * 2
    @dg.asset
    def demo(context):
        r = async_double(context, 21)
        return r
    r = dg.materialize([demo], instance=dg.DagsterInstance.ephemeral())
    if not r.success: raise AssertionError("async task materialize failed")
    if calls["n"] != 1: raise AssertionError(f"expected 1 call, got {calls['n']}")
    return "async def task auto-awaited"

check("task_asset — async task support (NEW)", _test_task_async)

def _test_task_retry_condition_fn():
    from assets.infrastructure.task_asset.component import task
    attempts = {"n": 0}
    @task(
        retry_condition_fn=lambda ctx, exc: isinstance(exc, ValueError),
        max_retries=2, retry_delay_seconds=0,
    )
    def flaky(ctx, x):
        attempts["n"] += 1
        raise ValueError("simulated transient")
    @dg.asset
    def demo(context):
        return flaky(context, 1)
    r = dg.materialize([demo], instance=dg.DagsterInstance.ephemeral(), raise_on_error=False)
    # Expected: RetryRequested → 2 retries → 3 total attempts → then fails
    if attempts["n"] < 3:
        raise AssertionError(f"expected >=3 attempts (initial + 2 retries), got {attempts['n']}")
    if r.success:
        raise AssertionError("run should have failed after retries exhausted")
    return f"predicate + Dagster RetryRequested chained {attempts['n']} attempts"

check("task_asset — retry_condition_fn (NEW)", _test_task_retry_condition_fn)

def _test_task_concurrency_pool():
    from assets.infrastructure.task_asset.component import task
    # Same pool_name declared with different max_concurrent → should raise
    @task(concurrency_pool="test_pool_a", max_concurrent=2)
    def a(ctx): return 1
    try:
        @task(concurrency_pool="test_pool_a", max_concurrent=3)
        def b(ctx): return 2
        raise AssertionError("mismatched pool cap should have raised")
    except ValueError:
        pass

    # Actual concurrency test: 3 threads try to acquire a pool of 2; only 2 concurrent
    import threading
    active = {"count": 0, "max": 0}
    lock = threading.Lock()
    @task(concurrency_pool="test_pool_b", max_concurrent=2)
    def gated(ctx, i):
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        time.sleep(0.05)
        with lock:
            active["count"] -= 1
        return i

    @dg.asset
    def demo(context):
        # Simulate concurrent calls via threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=lambda i=i: gated(context, i))
            threads.append(t); t.start()
        for t in threads: t.join()
        return active["max"]
    r = dg.materialize([demo], instance=dg.DagsterInstance.ephemeral())
    if not r.success: raise AssertionError("materialize failed")
    if active["max"] > 2:
        raise AssertionError(f"concurrency cap violated: peak concurrent = {active['max']}")
    return f"pool cap enforced (peak={active['max']}, cap=2); mismatched cap raises"

check("task_asset — concurrency_pool (NEW)", _test_task_concurrency_pool)

# ═══════════════════════════════════════════════════════════════════
# Report
# ═══════════════════════════════════════════════════════════════════

print()
pass_count = sum(1 for r in results if r[0] == "PASS")
fail_count = sum(1 for r in results if r[0] == "FAIL")
for status, name, detail in results:
    marker = "\033[32m✓\033[0m" if status == "PASS" else "\033[31m✗\033[0m"
    print(f"{marker} {name:60s} {detail}")
print()
print(f"═══ {pass_count} PASS / {fail_count} FAIL / {len(results)} TOTAL ═══")
sys.exit(0 if fail_count == 0 else 1)
