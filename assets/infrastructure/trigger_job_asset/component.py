"""TriggerJobAssetComponent.

An asset that launches another Dagster job (or an ad-hoc asset-selection job) with config, and
waits for it to finish -- the Airflow `TriggerDagRunOperator` equivalent, but Dagster-to-Dagster
rather than Dagster-to-Airflow (`jobs/airflow_trigger_job` already covers triggering an external
Airflow DAG; `sensors/asset_to_job_trigger_sensor` already covers the REACTIVE case -- a sensor
firing a job when an asset materializes. Neither covers "an asset that synchronously launches
another job", which is the gap this component fills).

Verified against this repo's own `assets/ai/dagster_workflow_tools` component before writing
anything: `dagster.execute_job` (a real, documented, synchronous, out-of-process execution
entrypoint) with a `ReconstructableJob` built via `dagster._core.definitions.reconstruct.
build_reconstructable_job` is the correct mechanism -- `instance.submit_run` (the only other
synchronous-looking candidate) needs a live `BaseWorkspaceRequestContext` that a running op does
not have; that's a confirmed architectural gap in dagster itself, documented in
dagster_workflow_tools's own docstring, not something this component works around.

`wait=False` (fire-and-forget, Airflow's `TriggerDagRunOperator(wait_for_completion=False)`):
confirmed live, not guessed, that `execute_job` itself is just
`instance.create_run_for_job(...)` (synchronous, returns a real run_id immediately) followed by
`dagster._core.execution.api.execute_run(...)` (the actual, slower execution). Splitting those
two steps -- create synchronously, execute in a background thread -- gives a real run_id back
immediately without needing `instance.submit_run`'s workspace context at all.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


def load_definitions(spec: str) -> "dg.Definitions":
    """'pkg.module' or 'pkg.module:attr' (default attr 'defs'; a callable is called). Same
    convention as dagster_workflow_tools' own load_definitions (duplicated, not imported --
    components are self-contained, no shared helper modules)."""
    import importlib
    mod_name, _, attr = spec.partition(":")
    obj = getattr(importlib.import_module(mod_name), attr or "defs")
    if callable(obj) and not isinstance(obj, dg.Definitions):
        obj = obj()
    if not isinstance(obj, dg.Definitions):
        raise ValueError(f"{spec!r} did not resolve to dagster.Definitions (got {type(obj).__name__})")
    return obj


def _reconstruct_job(definitions_module: str, job_name: Optional[str], asset_keys: Optional[List[str]]) -> "dg.JobDefinition":
    """The reconstruction target for `build_reconstructable_job` -- re-imported fresh in the
    executing process, never called directly by this component's own code. Exactly one of
    job_name/asset_keys is set (validated by the caller)."""
    defs = load_definitions(definitions_module)
    if job_name:
        return defs.resolve_job_def(job_name)
    keys = [dg.AssetKey.from_user_string(k) for k in (asset_keys or [])]
    # NOT Definitions.resolve_implicit_job_def_def_for_assets: that returns a job named
    # __ASSET_JOB, dagster's reserved name for the implicit per-repo asset job, which raises
    # DagsterInvalidDefinitionError ("... reserved name") when wrapped standalone like this.
    job_def = dg.define_asset_job(name="trigger_job_asset_ad_hoc", selection=dg.AssetSelection.assets(*keys))
    return job_def.resolve(asset_graph=defs.resolve_asset_graph())


def _this_module_name() -> str:
    """The module name `build_reconstructable_job` needs to re-import this file in the
    executing process. `__spec__.name` holds the real dotted path under a normal import --
    `dagster_community_components.assets.infrastructure.trigger_job_asset.component` in a real
    pip install (confirmed: this is exactly the dotted path dagster_community_components'
    own lazy __init__.py uses to import every component) -- and under this repo's own test
    convention (`importlib.util.spec_from_file_location` + `module_from_spec` also sets
    `__spec__.name` to the loader's synthetic name). Only a truly bare `python component.py`
    run leaves `__spec__` None, which doesn't apply here (components are always loaded via
    import, never executed as a script)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "TriggerJobAssetComponent could not determine its own importable module name "
        "(__spec__ is None) -- this should not happen for a component loaded via Dagster's "
        "normal component-loading machinery."
    )


class TriggerJobAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """An asset that launches another Dagster job (or ad-hoc asset selection) with config --
    the Airflow `TriggerDagRunOperator` equivalent. `wait=True` (default) waits for it to
    finish; `wait=False` fires it in the background and returns immediately."""

    asset_key: str = Field(description="Dagster asset key for this trigger asset, slash-delimited (e.g. 'pipeline/trigger_downstream').")
    definitions_module: str = Field(description="'pkg.module' or 'pkg.module:attr' (default attr 'defs') pointing at the target Definitions containing the job/assets to run.")
    job_name: Optional[str] = Field(default=None, description="Name of an existing job in definitions_module to run. Set this or asset_keys, not both.")
    asset_keys: Optional[List[str]] = Field(default=None, description="Asset keys in definitions_module to run as an ad-hoc selection job. Set this or job_name, not both.")
    run_config: Optional[Dict[str, Any]] = Field(default=None, description="Run config passed to the triggered run.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra tags on the triggered run (in addition to dagster-community/triggered-by).")
    partition_key: Optional[str] = Field(default=None, description="Partition key for the triggered run, if the target job/assets are partitioned.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for this trigger asset's own lineage (in this component's own pipeline, separate from definitions_module's graph).")
    wait: bool = Field(
        default=True,
        description=(
            "True (default): wait synchronously for the triggered job to finish -- Airflow's "
            "TriggerDagRunOperator(wait_for_completion=True). False: fire-and-forget -- create "
            "the run (a real run_id is returned immediately) and return, without waiting for "
            "it -- Airflow's wait_for_completion=False. The triggered run keeps executing in a "
            "background thread; this asset has no way to report its eventual success/failure "
            "(look it up by `triggered_run_id` in the Dagster UI/API). Mutually exclusive with "
            "`timeout_seconds` (nothing to time out if not waiting)."
        ),
    )

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog (distinct from `tags`, which land on the triggered RUN).")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on THIS trigger asset's own step if it fails. Defines a RetryPolicy -- re-triggers the downstream job again on retry (dagster.execute_job has no built-in retry of its own).")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name). The "
            "pool's own slot count is configured at the deployment level (dagster.yaml's "
            "concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Fail this trigger asset if the triggered job hasn't finished after this many seconds. Enforced via a background-thread wrapper -- dagster.execute_job has no native timeout, and the triggered run is NOT terminated on timeout (Python threads can't be forcibly killed, and execute_job gives no run_id back until it returns); this only bounds how long the TRIGGERING step waits.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        for k in ("deps", "owners", "kinds", "asset_keys"):
            v = out.get(k)
            if isinstance(v, str):
                out[k] = [x.strip() for x in v.split(",") if x.strip()]
        for k in ("tags", "asset_tags"):
            v = out.get(k)
            if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
                out[k] = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        return out

    @model_validator(mode="after")
    def _validate_target(self):
        if bool(self.job_name) == bool(self.asset_keys):
            raise ValueError("TriggerJobAssetComponent: set exactly one of `job_name` or `asset_keys`.")
        if not self.wait and self.timeout_seconds:
            raise ValueError("TriggerJobAssetComponent: `timeout_seconds` has nothing to bound when `wait=False` (fire-and-forget) -- unset one of them.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Trigger Job Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey(d.split("/")) for d in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            deps=deps_keys,
            group_name=component.group_name,
            description=component.description,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _trigger(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            from dagster._core.definitions.reconstruct import build_reconstructable_job

            reconstructable_job = build_reconstructable_job(
                _this_module_name(),
                "_reconstruct_job",
                reconstructable_kwargs={
                    "definitions_module": component.definitions_module,
                    "job_name": component.job_name,
                    "asset_keys": component.asset_keys,
                },
            )
            run_tags = {"dagster-community/triggered-by": str(context.asset_key), **(component.tags or {})}
            if component.partition_key:
                run_tags["dagster/partition"] = component.partition_key
            context.log.info(
                f"Triggering {'job ' + component.job_name if component.job_name else 'asset selection ' + str(component.asset_keys)} "
                f"from {component.definitions_module!r} (wait={component.wait})..."
            )

            if not component.wait:
                import threading

                from dagster._core.execution.api import execute_run

                job_def = reconstructable_job.get_definition()
                dagster_run = context.instance.create_run_for_job(
                    job_def=job_def,
                    run_config=component.run_config,
                    tags=run_tags,
                    job_code_origin=reconstructable_job.get_python_origin(),
                )
                threading.Thread(
                    target=execute_run,
                    args=(reconstructable_job, dagster_run, context.instance),
                    kwargs={"raise_on_error": False},
                    daemon=True,
                ).start()
                context.log.info(f"Fired run {dagster_run.run_id} in the background (wait=False) -- not waiting for it to finish.")
                return dg.MaterializeResult(metadata={
                    "triggered_run_id": dg.MetadataValue.text(dagster_run.run_id),
                    "triggered_job_name": dg.MetadataValue.text(component.job_name or "trigger_job_asset_ad_hoc"),
                    "waited": dg.MetadataValue.bool(False),
                })

            def _do_trigger():
                return dg.execute_job(reconstructable_job, instance=context.instance, run_config=component.run_config, tags=run_tags)

            result = _run_with_timeout(_do_trigger, component.timeout_seconds)
            if not result.success:
                raise dg.Failure(
                    description=f"Triggered job failed (run_id={result.run_id}).",
                    metadata={"triggered_run_id": dg.MetadataValue.text(result.run_id)},
                )
            return dg.MaterializeResult(metadata={
                "triggered_run_id": dg.MetadataValue.text(result.run_id),
                "triggered_job_name": dg.MetadataValue.text(component.job_name or "trigger_job_asset_ad_hoc"),
                "success": dg.MetadataValue.bool(result.success),
                "waited": dg.MetadataValue.bool(True),
            })

        return dg.Definitions(assets=[_trigger])
