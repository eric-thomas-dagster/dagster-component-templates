"""Catalog Agent — planner picks real components from the live registry, iteratively, with schema discovery.

The most sophisticated agentic primitive. At each step, the planner sees the
ACTUAL columns + preview of every prior step's real materialized output — so
it picks the next component from the live 900-component manifest with real
schema knowledge. Works on customer data with schemas unknown at pipeline-
write time (Snowflake tables, uploaded CSVs, anything). Step 1 executes → we
inspect the real DataFrame → step 2's planner sees the real column names →
picks the next component with real knowledge.

Combines two primitives that used to be separate: per-step iteration (like
IterativeSupervisorAgentComponent) with live-registry component picking
(reflection-based, real in-process materialization).

Assets emitted (`max_iterations + 1` per YAML block):
  1. `<step_asset_prefix>_1` … `<step_asset_prefix>_<max_iterations>`
     — each step's output is a dict:
        {plan: {iteration, done, component_id, config, reason, asset_name},
         df: <the DataFrame from the picked component's materialization, or None>}
  2. `<synthesis_asset_name>` — reads all step outputs, writes final answer.

Whichever step declares `done` short-circuits all downstream steps. Static DAG
shape at YAML load; dynamic termination at runtime.

Chaining across steps works because each step's picked component reads its
upstream via `upstream_asset_key` matching a prior step's `asset_name`. When
executing, we construct an ad-hoc source asset seeded from the prior step's
stored DataFrame, then dg.materialize both together.

Limitations (v1):
  • Only source-style + upstream_asset_key chained transforms. Multi-asset
    components run but only the first asset's output is captured.
  • Components requiring runtime resources (Snowflake, S3, Slack, ...) fail
    at materialize; `fail_on_execution_error=false` lets sibling steps continue.
"""
import json
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

import dagster as dg
from pydantic import Field


class CatalogAgentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Catalog agent — per-step planner + real component materialization from the live registry, with schema discovery.

    Example — schema-agnostic pipeline (planner discovers real columns after step 1):

        ```yaml
        type: dagster_community_components.CatalogAgentComponent
        attributes:
          step_asset_prefix: catalog_step
          synthesis_asset_name: catalog_final_answer
          task: |
            Generate 100 synthetic support tickets. Then figure out which
            columns exist and use them to filter to tickets from a
            specific channel — pick any channel that appears in the data.
            Then summarize by that channel-picked column.
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          include_ids: [synthetic_data_generator, filter, summarize]
          max_iterations: 5
        ```

    The planner picks step 1 (`synthetic_data_generator`), executes it,
    inspects the real columns (`ticket_id, channel, priority, ticket_text, ...`),
    then picks step 2 (`filter`) with knowledge of the real column names.
    """

    step_asset_prefix: str = Field(
        default="catalog_step",
        description="Prefix for the N step assets. Assets are named <prefix>_1, <prefix>_2, ...",
    )
    synthesis_asset_name: str = Field(description="Final synthesis asset name.")
    task: str = Field(description="The task the agent iterates against.")

    manifest_url: Optional[str] = Field(
        default="https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/manifest.json",
        description="URL of manifest.json to fetch at runtime.",
    )
    manifest_path: Optional[str] = Field(
        default=None,
        description="Local filesystem path to manifest.json (takes precedence over manifest_url).",
    )
    include_categories: Optional[List[str]] = Field(default=None)
    include_tags: Optional[List[str]] = Field(default=None)
    include_ids: Optional[List[str]] = Field(default=None)
    max_catalog_entries: int = Field(default=40)

    max_iterations: int = Field(default=5, ge=1, le=15)

    model: str = Field(default="gpt-4o-mini")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.1)
    planner_max_tokens: int = Field(default=600)
    synthesis_max_tokens: int = Field(default=800)
    fail_on_execution_error: bool = Field(default=False)
    synthesis_system_message: Optional[str] = Field(default=None)

    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)

    # Standard fields — partitions / freshness / retries.
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "agent", "iterative", "catalog"})

        # Build partitions / freshness / retry policies.
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _vals = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _vals:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_vals)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            if self.freshness_cron:
                freshness_policy = FreshnessPolicy.cron(
                    deadline_cron=self.freshness_cron,
                    lower_bound_delta=timedelta(minutes=self.freshness_max_lag_minutes),
                )
            else:
                freshness_policy = FreshnessPolicy.time_window(
                    fail_window=timedelta(minutes=self.freshness_max_lag_minutes),
                )
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        std_kwargs: Dict[str, Any] = {}
        if partitions_def is not None: std_kwargs["partitions_def"] = partitions_def
        if freshness_policy is not None: std_kwargs["freshness_policy"] = freshness_policy
        if retry_policy is not None: std_kwargs["retry_policy"] = retry_policy
        if self.owners: std_kwargs["owners"] = list(self.owners)
        if self.tags: std_kwargs["tags"] = dict(self.tags)

        def _client():
            import os
            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("catalog_agent requires openai>=1.0.0") from e
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    kwargs["base_url"] = base_url
            return OpenAI(**kwargs)

        def _resolve_task(ctx) -> str:
            _t = _self.task
            if not isinstance(_t, str) or "{" not in _t:
                return _t
            out = _t
            out = out.replace("{run_id}", str(getattr(ctx, "run_id", "") or ""))
            has_pk = False
            try: has_pk = ctx.has_partition_key
            except Exception: pass
            if has_pk:
                try: pk = ctx.partition_key
                except Exception: pk = ""
                if hasattr(pk, "keys_by_dimension"):
                    out = out.replace("{partition_key}", str(pk))
                    for dim, val in pk.keys_by_dimension.items():
                        out = out.replace("{partition_keys." + dim + "}", str(val))
                else:
                    out = out.replace("{partition_key}", str(pk or ""))
            else:
                out = out.replace("{partition_key}", "")
            return out

        def _load_and_filter_catalog():
            import importlib
            _dcc = importlib.import_module("dagster_community_components")
            if _self.manifest_path:
                with open(_self.manifest_path, "r") as fh:
                    manifest = json.load(fh)
            else:
                if not _self.manifest_url:
                    raise RuntimeError("Set manifest_url or manifest_path.")
                with urlopen(_self.manifest_url, timeout=30) as resp:
                    manifest = json.load(resp)
            comps = manifest.get("components") or manifest.get("templates") or []
            filtered = []
            if _self.include_ids:
                _iset = set(_self.include_ids)
                filtered = [c for c in comps if c.get("id") in _iset]
            else:
                for c in comps:
                    if _self.include_categories and c.get("category") not in _self.include_categories:
                        continue
                    if _self.include_tags:
                        _ct = set(c.get("tags") or [])
                        if not (_ct & set(_self.include_tags)):
                            continue
                    filtered.append(c)
            resolved = []
            for c in filtered:
                raw_type = c.get("component_type") or c.get("type") or ""
                if raw_type:
                    _cls_name = raw_type.rsplit(".", 1)[-1]
                else:
                    _id = c.get("id") or ""
                    _pascal = "".join(p.capitalize() for p in _id.split("_"))
                    # Most classes end in Component, but a handful don't (e.g. DataframeJoin).
                    # Try Component suffix first; fall back to bare PascalCase.
                    _cls_name = _pascal + "Component" if hasattr(_dcc, _pascal + "Component") else _pascal
                if hasattr(_dcc, _cls_name):
                    resolved.append({**c, "component_type": f"dagster_community_components.{_cls_name}"})
            return resolved[: _self.max_catalog_entries], _dcc

        def _short_type(annotation) -> str:
            s = str(annotation).replace("typing.", "").replace("<class '", "").replace("'>", "")
            if s.startswith("Optional["):
                s = s[len("Optional["):-1]
            return s[:60]

        def _catalog_lines(catalog, dcc):
            lines = []
            for c in catalog:
                _cls_name = c["component_type"].rsplit(".", 1)[-1]
                _cls = getattr(dcc, _cls_name, None)
                if _cls is not None and hasattr(_cls, "model_fields"):
                    _req_lines = []
                    _opt_lines = []
                    for _n, _f in _cls.model_fields.items():
                        _ts = _short_type(_f.annotation) if _f.annotation else "any"
                        (_req_lines if _f.is_required() else _opt_lines).append(f"{_n}: {_ts}")
                    _fs = (
                        f"required=[{', '.join(_req_lines) or '(none)'}]"
                        f"\n      optional=[{', '.join(_opt_lines[:8]) or '(none)'}]"
                    )
                else:
                    _fs = "fields=(unknown)"
                lines.append(
                    f"  - id: {c['id']}  |  {c.get('description', '')[:100]}\n"
                    f"      {_fs}"
                )
            return lines

        # ── One planner+executor step. Called from each step_N asset. ─────
        def _plan_and_execute_step(context, iteration: int, prior_step_outputs: List[Any]):
            import importlib
            import pandas as pd

            # Short-circuit if any prior step said done.
            for prior in prior_step_outputs:
                if isinstance(prior, dict) and prior.get("plan", {}).get("done"):
                    context.log.info(f"[step {iteration}] short-circuit — prior step done")
                    context.add_output_metadata({
                        "skipped": dg.MetadataValue.bool(True),
                        "reason": dg.MetadataValue.text("prior step declared done"),
                    })
                    return {
                        "plan": {
                            "iteration": iteration, "done": True, "component_id": None,
                            "component_type": None, "config": None, "reason": "short-circuit",
                            "asset_name": None, "output_columns": [], "output_preview": "",
                            "status": "no_op", "error": "",
                        },
                        "df": None,
                    }

            catalog, _dcc = _load_and_filter_catalog()
            if not catalog:
                raise RuntimeError("Catalog filter returned zero components; loosen filters.")

            # Summarize prior work for the planner — including REAL column names + previews.
            prior_summary_parts = []
            prior_dfs: Dict[str, Any] = {}  # asset_name → DataFrame
            for i, prior in enumerate(prior_step_outputs, start=1):
                if not isinstance(prior, dict):
                    continue
                _p = prior.get("plan") or {}
                if not _p.get("component_id"):
                    continue
                _asn = _p.get("asset_name") or ""
                _cols = _p.get("output_columns") or []
                _prev = _p.get("output_preview") or ""
                _status = _p.get("status") or "success"
                _err = _p.get("error") or ""
                if _status == "success":
                    prior_summary_parts.append(
                        f"Step {i}: [SUCCESS]\n"
                        f"  picked: {_p['component_id']}\n"
                        f"  config: {_p.get('config', '')}\n"
                        f"  produced asset: {_asn}\n"
                        f"  columns: {_cols}\n"
                        f"  preview:\n{_prev[:500]}\n"
                    )
                else:
                    prior_summary_parts.append(
                        f"Step {i}: [FAILED — do not repeat this mistake]\n"
                        f"  picked: {_p['component_id']}\n"
                        f"  config: {_p.get('config', '')}\n"
                        f"  error: {_err[:400]}\n"
                        f"  NOTE: this step did NOT produce {_asn!r} — don't reference it.\n"
                    )
                _df = prior.get("df")
                if _df is not None and _asn:
                    prior_dfs[_asn] = _df

            prior_summary = "\n".join(prior_summary_parts) or "(no prior steps — this is step 1)"

            lines = _catalog_lines(catalog, _dcc)
            valid_ids = ", ".join(f'"{c["id"]}"' for c in catalog)
            _task = _resolve_task(context)

            planner_prompt = (
                f"You are an iterative pipeline agent. You've done the work below; "
                f"decide the NEXT step, or declare done.\n\n"
                f"Task:\n{_task}\n\n"
                f"Prior steps (with REAL output columns and previews):\n{prior_summary}\n\n"
                f"Available components ({len(catalog)} shown; with their Pydantic field TYPES):\n"
                + "\n".join(lines) + "\n\n"
                f"Output ONLY a JSON object (no markdown fences).\n"
                f"If more work is needed:\n"
                f'  {{"done": false,'
                f' "id": "<one of: {valid_ids}>",'
                f' "config": <object with asset_name (snake_case, unique) + all required fields;'
                f' wire ANY field ending in _asset_key (or _asset_keys for lists) to a PRIOR step\'s asset_name to chain>,'
                f' "reason": "<one sentence — why this step now>"}}\n'
                f"If the task is complete:\n"
                f'  {{"done": true, "reason": "<one sentence — why we\'re done>"}}\n\n'
                f"CRITICAL rules:\n"
                f"  • NEVER reference an asset name that hasn't been produced by a PRIOR "
                f"    step in the Prior steps section above. Every upstream_asset_key / "
                f"    left_asset_key / right_asset_key / etc. MUST point to a specific "
                f"    asset_name listed in Prior steps. If the upstream you need doesn't "
                f"    exist yet, pick THAT component this step instead — don't skip ahead.\n"
                f"  • Use REAL column names from prior steps\' `columns` field.\n"
                f"  • For summarize/filter, group_by must be List[str] and aggregations must be Dict[str, Any] "
                f"like {{'row_count': {{'col': '<real_col>', 'agg': 'count'}}}}.\n"
                f"  • For MULTI-SOURCE components like dataframe_join, set BOTH `left_asset_key` and "
                f"`right_asset_key` to PRIOR asset_names.\n"
                f"  • Sources (like synthetic_data_generator) that produce independent inputs can be picked "
                f"in EARLY steps back-to-back — each becomes its own asset that later steps reference by name."
            )

            context.log.info(f"[step {iteration}] planner deciding next action (catalog size={len(catalog)})")

            client = _client()
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are an iterative agent that picks real Dagster components step by step."},
                    {"role": "user", "content": planner_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                plan = json.loads(raw)
            except json.JSONDecodeError as e:
                context.log.warning(f"[step {iteration}] plan JSON parse failed: {e}; raw={raw[:200]}")
                plan = {"done": True, "reason": f"plan parse failed: {e}"}

            done = bool(plan.get("done", False))
            cid = plan.get("id")
            cfg = plan.get("config") or {}
            reason = plan.get("reason", "")

            if done or not cid:
                context.log.info(f"[step {iteration}] planner reports done: {reason}")
                context.add_output_metadata({
                    "done": dg.MetadataValue.bool(True),
                    "reason": dg.MetadataValue.text(reason),
                })
                return {
                    "plan": {
                        "iteration": iteration, "done": True, "component_id": None,
                        "component_type": None, "config": None, "reason": reason,
                        "asset_name": None, "output_columns": [], "output_preview": "",
                        "status": "done_signal", "error": "",
                    },
                    "df": None,
                }

            # Look up the catalog entry for the picked id
            entry = next((c for c in catalog if c["id"] == cid), None)
            if entry is None:
                # Recoverable — let the next step's planner retry with a
                # different pick. `done: False` so we don't short-circuit.
                context.log.warning(f"[step {iteration}] invalid id {cid!r}; letting next step retry")
                return {
                    "plan": {
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": None, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": None, "output_columns": [], "output_preview": "",
                        "status": "invalid_pick", "error": f"unknown id {cid!r}",
                    },
                    "df": None,
                }

            ctype = entry["component_type"]
            asset_name = cfg.get("asset_name") or f"{cid}_step{iteration}"

            # Validate upstream references BEFORE materializing. If the pick
            # references an asset that no prior step produced, it's a planning
            # error — mark the pick failed so the next step's planner sees
            # the mistake and can course-correct.
            _dangling: List[str] = []
            for _cfg_key, _cfg_val in cfg.items():
                if _cfg_key.endswith("_asset_key") or _cfg_key == "asset_key":
                    if isinstance(_cfg_val, str) and _cfg_val and _cfg_val not in prior_dfs:
                        _dangling.append(f"{_cfg_key}={_cfg_val!r}")
                elif _cfg_key.endswith("_asset_keys") or _cfg_key == "asset_keys":
                    if isinstance(_cfg_val, list):
                        for _v in _cfg_val:
                            if isinstance(_v, str) and _v and _v not in prior_dfs:
                                _dangling.append(f"{_cfg_key}={_v!r}")
            if _dangling:
                _err = (
                    f"pick references non-existent upstream(s): {', '.join(_dangling)}. "
                    f"Available prior asset_names: {sorted(prior_dfs.keys()) or '(none)'}"
                )
                context.log.warning(f"[step {iteration}] {_err}")
                return {
                    "plan": {
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [], "output_preview": "",
                        "status": "failed", "error": _err,
                    },
                    "df": None,
                }

            context.log.info(f"[step {iteration}] running {cid} → asset {asset_name}, config keys={sorted(cfg.keys())}")

            try:
                _mod_name, _cls_name = ctype.rsplit(".", 1)
                _module = importlib.import_module(_mod_name)
                _cls = getattr(_module, _cls_name)
                _inst = _cls(**cfg)
                _defs = _inst.build_defs(context)
                _assets = list(_defs.assets or [])
                if not _assets:
                    raise RuntimeError(f"component {cid} produced no assets")

                # Wire prior-step DataFrames as source assets for any field
                # in the pick's config that looks like an upstream reference.
                # Supports:
                #   • single-upstream (e.g. `upstream_asset_key: "foo"`)
                #   • multi-source (e.g. dataframe_join's `left_asset_key`
                #     + `right_asset_key`)
                #   • N-way (`additional_asset_keys: ["a", "b", ...]`)
                # We match any config key ending in `_asset_key` (singular) or
                # `_asset_keys` / `asset_keys` (list). Each matching VALUE
                # that names a prior step's asset_name gets wired.
                _extra_assets = []
                _wired: set = set()

                def _make_upstream_source(name, df_value):
                    @dg.asset(key=dg.AssetKey.from_user_string(name))
                    def _upstream_source():
                        return df_value
                    return _upstream_source

                def _try_wire(candidate_name):
                    if isinstance(candidate_name, str) and candidate_name in prior_dfs and candidate_name not in _wired:
                        _extra_assets.append(_make_upstream_source(candidate_name, prior_dfs[candidate_name]))
                        _wired.add(candidate_name)

                for _cfg_key, _cfg_val in cfg.items():
                    if _cfg_key.endswith("_asset_key") or _cfg_key == "asset_key":
                        _try_wire(_cfg_val)
                    elif _cfg_key.endswith("_asset_keys") or _cfg_key == "asset_keys":
                        if isinstance(_cfg_val, list):
                            for _v in _cfg_val:
                                _try_wire(_v)

                if _wired:
                    context.log.info(f"[step {iteration}] wired upstream sources: {sorted(_wired)}")

                _mat_result = dg.materialize(_extra_assets + _assets, raise_on_error=False)
                if not _mat_result.success:
                    # Find the specific step failure message
                    _err = ""
                    for _evt in _mat_result.all_events:
                        _et = getattr(_evt, "event_type_value", "") or ""
                        if "FAILURE" in _et:
                            _err = str(getattr(_evt, "message", "") or _evt)[:400]
                            break
                    raise RuntimeError(_err or "materialize returned success=false")

                # Pull the output value for the primary asset.
                _val = None
                for _a in _assets:
                    for _k in _a.keys:
                        _name = _k.to_user_string()
                        try:
                            _val = _mat_result.output_for_node(_name)
                            break
                        except Exception:
                            try:
                                _val = _mat_result.output_for_node(_name.split("/")[-1])
                                break
                            except Exception:
                                continue
                    if _val is not None:
                        break

                # Extract schema + preview from the real DataFrame.
                if hasattr(_val, "columns"):
                    _cols = list(_val.columns)
                    _preview = _val.head(5).to_markdown(index=False) if hasattr(_val, "to_markdown") else str(_val)[:600]
                elif _val is not None:
                    _cols = []
                    _preview = str(_val)[:600]
                else:
                    _cols = []
                    _preview = "(no value)"

                context.add_output_metadata({
                    "iteration": dg.MetadataValue.int(iteration),
                    "done": dg.MetadataValue.bool(False),
                    "component_id": dg.MetadataValue.text(cid),
                    "asset_name": dg.MetadataValue.text(asset_name),
                    "reason": dg.MetadataValue.text(reason),
                    "output_columns": dg.MetadataValue.text(str(_cols)),
                    "output_preview": dg.MetadataValue.md(_preview),
                })
                return {
                    "plan": {
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": _cols, "output_preview": _preview,
                        "status": "success", "error": "",
                    },
                    "df": _val,
                }
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"[step {iteration}] execution failed for {cid}: {e}")
                if _self.fail_on_execution_error:
                    raise
                return {
                    "plan": {
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [], "output_preview": "",
                        "status": "failed", "error": str(e)[:400],
                    },
                    "df": None,
                }

        # ── Emit N pre-declared step assets ────────────────────────────
        assets: list = []
        step_keys: List[str] = []

        def _make_step_asset(iteration: int):
            step_name = f"{_self.step_asset_prefix}_{iteration}"
            prior_names = [f"{_self.step_asset_prefix}_{i}" for i in range(1, iteration)]
            _ins = {n: dg.AssetIn(key=dg.AssetKey.from_user_string(n)) for n in prior_names}

            @dg.asset(
                key=dg.AssetKey.from_user_string(step_name),
                group_name=_self.group_name,
                kinds=_kinds | {"step"},
                description=f"Iterative catalog agent step {iteration}/{_self.max_iterations}.",
                ins=_ins if _ins else None,
                **std_kwargs,
            )
            def _step_asset(context: dg.AssetExecutionContext, **kwargs):
                prior = [kwargs.get(n) for n in prior_names]
                return _plan_and_execute_step(context, iteration, prior)

            return _step_asset, step_name

        for i in range(1, _self.max_iterations + 1):
            _a, _n = _make_step_asset(i)
            assets.append(_a)
            step_keys.append(_n)

        # ── Synthesizer ────────────────────────────────────────────────
        _syn_default = _self.synthesis_system_message or (
            "You are a research synthesizer. Given the original task and the "
            "iterative agent's trajectory (each step's picked component, "
            "config, output columns, and preview), write the final answer. "
            "Cite each step by its number + component_id."
        )
        _syn_ins = {n: dg.AssetIn(key=dg.AssetKey.from_user_string(n)) for n in step_keys}

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.synthesis_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"synthesizer"},
            description="Final answer synthesized from the iterative trajectory.",
            ins=_syn_ins,
            **std_kwargs,
        )
        def _synthesis_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            _task = _resolve_task(context)
            steps_desc = []
            n_ok = 0
            for _n in step_keys:
                _o = kwargs.get(_n)
                if not isinstance(_o, dict):
                    continue
                _p = _o.get("plan") or {}
                if not _p.get("component_id"):
                    continue
                if _p.get("status") == "success":
                    n_ok += 1
                steps_desc.append(
                    f"### step {_p['iteration']}\n"
                    f"picked: {_p['component_id']}\n"
                    f"asset: {_p.get('asset_name')}\n"
                    f"reason: {_p.get('reason')}\n"
                    f"status: {_p.get('status')}\n"
                    f"columns: {_p.get('output_columns')}\n"
                    f"preview:\n{_p.get('output_preview', '')[:800]}\n"
                )

            if not steps_desc:
                answer = "(no components executed — the agent declared done immediately)"
            else:
                client = _client()
                user_msg = (
                    f"Task:\n{_task}\n\nTrajectory ({n_ok} successful step(s)):\n\n"
                    + "\n".join(steps_desc)
                )
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.synthesis_max_tokens,
                    messages=[
                        {"role": "system", "content": _syn_default},
                        {"role": "user", "content": user_msg},
                    ],
                )
                answer = resp.choices[0].message.content or ""

            df = pd.DataFrame([{"task": _task, "n_success": n_ok, "answer": answer}])
            context.add_output_metadata({
                "task": dg.MetadataValue.text(_task),
                "n_success": dg.MetadataValue.int(n_ok),
                "answer": dg.MetadataValue.md(answer),
            })
            return df

        assets.append(_synthesis_asset)
        return dg.Definitions(assets=assets)
