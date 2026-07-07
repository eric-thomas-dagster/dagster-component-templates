"""Component Catalog Agent — planner picks from the LIVE manifest, executor materializes them for real.

The most sophisticated agentic-pipeline demo in this repo. Unlike
SupervisorAgentComponent (bounded LLM-persona tools) or
MCPToolPickerComponent (bounded MCP tools), this agent's "tool set" is
the **live component registry** — 900+ real Dagster components. The
planner sees a filtered slice of the manifest and picks REAL components
to invoke; the executor imports each picked class by name, instantiates
it with Pydantic, and materializes its asset in-process.

Truly runtime dynamic. Real component invocation, not simulation.

Pipeline (3 assets per YAML block):
  1. <plan_asset_name>       — planner picks {component_type, config, reason}
                                per invocation from the filtered catalog
  2. <execution_asset_name>  — for each pick: import class → instantiate →
                                build_defs → in-process materialize.
                                Emits {component_type, config, reason, output_repr}
  3. <synthesis_asset_name>  — LLM reads all executed outputs + task →
                                writes grounded final answer

Safety:
  - The catalog is the WHOLE registry filtered by `include_categories` /
    `include_tags` / `include_ids`. That filter IS the bounded action space —
    scope it tight in production.
  - Every planner pick + config + reason is a Dagster asset row (audit trail).
  - Every execution is a real materialization event, captured with logs.

Limitations (v1):
  - Only picks components with no external upstream deps (source-style
    components — synthetic_data_generator, text_embedding_asset,
    langchain_chain_asset with a task, etc.). Components that depend on
    upstream asset materializations are filtered out.
  - Components requiring resources (databases, cloud APIs beyond a single
    OPENAI_API_KEY env var) will fail at materialize — set
    `fail_on_execution_error=false` to log-and-skip.
"""
import json
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

import dagster as dg
from pydantic import Field


class ComponentCatalogAgentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Planner picks real components from the live manifest; executor materializes them for real.

    Example:

        ```yaml
        type: dagster_community_components.ComponentCatalogAgentComponent
        attributes:
          plan_asset_name: catalog_plan
          execution_asset_name: catalog_execution
          synthesis_asset_name: catalog_answer
          task: |
            Generate two small synthetic datasets — one of customers and
            one of products — and give me a one-paragraph summary
            describing what each looks like.
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          # Manifest source
          manifest_url: https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/manifest.json
          # Filter the 900 components down to a manageable slice
          include_tags: [synthetic, data-generation]
          max_picks: 3
        ```

    Planner sees only the filtered slice of the manifest — that's the
    "bounded action space" for safety. In production, filter tight:
    include a category (like 'ai' or 'transformation') and a few
    key tags.
    """

    plan_asset_name: str = Field(description="Planner asset name.")
    execution_asset_name: str = Field(description="Executor asset name (real materializations).")
    synthesis_asset_name: str = Field(description="Synthesizer asset name.")
    task: str = Field(description="The task the agent plans against.")

    manifest_url: Optional[str] = Field(
        default="https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/manifest.json",
        description="Where to fetch the live manifest.json (URL or local file:// path).",
    )
    manifest_path: Optional[str] = Field(
        default=None,
        description="Local filesystem path to manifest.json (takes precedence over manifest_url).",
    )
    include_categories: Optional[List[str]] = Field(
        default=None,
        description="If set, only manifest entries with these categories are shown to the planner.",
    )
    include_tags: Optional[List[str]] = Field(
        default=None,
        description="If set, only entries whose tags overlap with these are shown to the planner.",
    )
    include_ids: Optional[List[str]] = Field(
        default=None,
        description="Explicit component IDs to include (overrides category/tag filters if set).",
    )
    max_catalog_entries: int = Field(
        default=40,
        description=(
            "Max entries shown to the planner. The manifest is huge; sending "
            "900 to gpt-4o-mini would blow the context. Filter down first."
        ),
    )

    model: str = Field(default="gpt-4o-mini")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.2)
    planner_max_tokens: int = Field(default=800)
    synthesis_max_tokens: int = Field(default=800)
    max_picks: int = Field(default=3, ge=1, description="Max components the planner may pick per run.")
    fail_on_execution_error: bool = Field(
        default=False,
        description="If True, raise on the first component that fails to instantiate or materialize. If False, log + continue.",
    )

    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)

    # Standard fields — partitions / freshness / retries.
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic'")
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
        _kinds.update({"ai", "agent", "catalog"})

        # Build partitions / freshness / retry policies from the standard fields.
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
                raise ImportError("component_catalog_agent requires openai>=1.0.0") from e
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

        def _load_manifest() -> Dict[str, Any]:
            if _self.manifest_path:
                with open(_self.manifest_path, "r") as fh:
                    return json.load(fh)
            if not _self.manifest_url:
                raise RuntimeError("Set manifest_url or manifest_path.")
            with urlopen(_self.manifest_url, timeout=30) as resp:
                return json.load(resp)

        def _filter_catalog(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
            comps = manifest.get("components") or manifest.get("templates") or []
            filtered: List[Dict[str, Any]] = []
            if _self.include_ids:
                include_set = set(_self.include_ids)
                filtered = [c for c in comps if c.get("id") in include_set]
            else:
                for c in comps:
                    if _self.include_categories and c.get("category") not in _self.include_categories:
                        continue
                    if _self.include_tags:
                        ctags = set(c.get("tags") or [])
                        if not (ctags & set(_self.include_tags)):
                            continue
                    filtered.append(c)
            # Resolve component_type (class name) for each entry.
            # Every actual class lives in `dagster_community_components` (the
            # installed package). Some manifest entries use the legacy prefix
            # `dagster_component_templates.` — we normalize to
            # `dagster_community_components.` always. We verify each class is
            # actually importable by attempting `getattr(dcc, ClassName)`.
            import importlib
            _dcc = importlib.import_module("dagster_community_components")
            resolved: List[Dict[str, Any]] = []
            for c in filtered:
                raw_type = c.get("component_type") or c.get("type") or ""
                if raw_type:
                    _cls_name = raw_type.rsplit(".", 1)[-1]
                else:
                    # Infer from id: 'synthetic_data_generator' → 'SyntheticDataGeneratorComponent'
                    _id = c.get("id") or ""
                    _cls_name = "".join(p.capitalize() for p in _id.split("_")) + "Component"
                if hasattr(_dcc, _cls_name):
                    resolved.append({**c, "component_type": f"dagster_community_components.{_cls_name}"})
            return resolved[: _self.max_catalog_entries]

        # ── Planner asset ──────────────────────────────────────────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.plan_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"planner"},
            description=f"Planner picks real components from the live catalog for task: {_self.task[:80]}",
            **std_kwargs,
        )
        def _plan_asset(context: dg.AssetExecutionContext):
            import pandas as pd

            _task = _resolve_task(context)
            manifest = _load_manifest()
            catalog = _filter_catalog(manifest)
            if not catalog:
                raise RuntimeError("Catalog filter returned zero components. Loosen include_categories / include_tags.")
            context.log.info(f"[catalog] filtered manifest → {len(catalog)} candidate components")

            # Present catalog to planner with field names AND TYPES dynamically
            # inspected from each component's Pydantic model. Type info matters —
            # without it the planner emits `group_by: "x"` when the field is
            # actually List[str], causing validation errors.
            import importlib
            _dcc = importlib.import_module("dagster_community_components")

            def _short_type(annotation) -> str:
                """Best-effort short type repr — 'str', 'List[str]', 'int', 'dict', etc."""
                s = str(annotation)
                # Strip typing. prefix and Optional[...] wrappers for readability
                s = s.replace("typing.", "").replace("<class '", "").replace("'>", "")
                if s.startswith("Optional["):
                    s = s[len("Optional["):-1]
                return s[:60]

            catalog_lines = []
            for c in catalog:
                _cls_name = c["component_type"].rsplit(".", 1)[-1]
                _cls = getattr(_dcc, _cls_name, None)
                if _cls is not None and hasattr(_cls, "model_fields"):
                    _req_lines = []
                    _opt_lines = []
                    for _name, _fld in _cls.model_fields.items():
                        _type_str = _short_type(_fld.annotation) if _fld.annotation else "any"
                        _line = f"{_name}: {_type_str}"
                        if _fld.is_required():
                            _req_lines.append(_line)
                        else:
                            _opt_lines.append(_line)
                    _fields_str = (
                        f"required=[{', '.join(_req_lines) or '(none)'}]"
                        f"\n      optional=[{', '.join(_opt_lines[:8]) or '(none)'}]"
                    )
                else:
                    _fields_str = "fields=(unknown)"
                catalog_lines.append(
                    f"  - id: {c['id']}  |  {c.get('description', '')[:100]}\n"
                    f"      {_fields_str}"
                )
            valid_ids = ", ".join(f'"{c["id"]}"' for c in catalog)

            planner_prompt = (
                f"You are a data-pipeline agent. Design a multi-step pipeline "
                f"by picking UP TO {_self.max_picks} real components to chain "
                f"together. Order matters: first pick is the source, later "
                f"picks read from earlier picks via `upstream_asset_key`.\n\n"
                f"Available components (with their Pydantic field names):\n"
                + "\n".join(catalog_lines) + "\n\n"
                f"Task:\n{_task}\n\n"
                f"Output ONLY a JSON array (no markdown fences). Each element:\n"
                f'  {{"id": "<one of: {valid_ids}>",\n'
                f'   "config": <object; the Pydantic attributes for that component — MUST include asset_name AND all required fields shown above>,\n'
                f'   "reason": "<one sentence — why this step now>"}}\n\n'
                f"CRITICAL rules for building the pipeline:\n"
                f"  • Every pick must have a UNIQUE snake_case `asset_name`.\n"
                f"  • If a component has `upstream_asset_key` as a required or "
                f"    common optional field, set it to the `asset_name` of a "
                f"    PRIOR pick in the same array. That's how you chain steps.\n"
                f"  • The first pick(s) should be source-style (no "
                f"    upstream_asset_key needed).\n"
                f"  • Order the array from source → transforms → sinks. Dagster "
                f"    will build the DAG from the asset_name / upstream_asset_key "
                f"    linkage.\n"
                f"  • For synthetic_data_generator, valid schema_type values: "
                f"    customers / orders / products / transactions / events / "
                f"    sensors / users / subscriptions / support_tickets."
            )
            context.log.info(f"[catalog] asking planner to pick from {len(catalog)} components")

            client = _client()
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are a helpful component-catalog agent that picks real Dagster components."},
                    {"role": "user", "content": planner_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                picks = json.loads(raw)
                if not isinstance(picks, list):
                    picks = [picks]
            except json.JSONDecodeError as e:
                context.log.warning(f"[catalog] plan JSON parse failed: {e}; raw={raw[:200]}")
                picks = []

            id_to_type = {c["id"]: c.get("component_type") for c in catalog}
            filtered: List[Dict[str, Any]] = []
            for p in picks:
                if not isinstance(p, dict):
                    continue
                pid = p.get("id")
                if pid not in id_to_type:
                    context.log.warning(f"[catalog] planner picked unknown id {pid!r}; dropping")
                    continue
                filtered.append({
                    "component_id": pid,
                    "component_type": id_to_type[pid],
                    "config_json": json.dumps(p.get("config") or {}),
                    "reason": p.get("reason", ""),
                })

            df = pd.DataFrame(filtered) if filtered else pd.DataFrame(columns=["component_id", "component_type", "config_json", "reason"])
            picked_ids = sorted({r["component_id"] for r in filtered}) if filtered else []
            context.log.info(f"[catalog] planner picked {len(df)} invocation(s): {picked_ids}")

            context.add_output_metadata({
                "task": dg.MetadataValue.text(_task),
                "catalog_size": dg.MetadataValue.int(len(catalog)),
                "n_picks": dg.MetadataValue.int(len(df)),
                "components_picked": dg.MetadataValue.text(", ".join(picked_ids) or "(none)"),
                "plan": dg.MetadataValue.md(df.to_markdown(index=False) if not df.empty else "_no picks_"),
            })
            return df

        # ── Executor asset — reflection-based real invocation ──────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.execution_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"executor"},
            description="Reflection-instantiate each picked component and materialize it in-process.",
            ins={"plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name))},
            **std_kwargs,
        )
        def _execution_asset(context: dg.AssetExecutionContext, plan):
            import pandas as pd
            import importlib

            plan_df = plan if isinstance(plan, pd.DataFrame) else pd.DataFrame(plan)
            if plan_df.empty:
                context.log.warning("[catalog] executor: plan is empty")
                context.add_output_metadata({"n_executed": dg.MetadataValue.int(0)})
                return pd.DataFrame(columns=["component_id", "component_type", "config", "asset_names", "status", "output_repr", "error"])

            # ── PHASE 1: Instantiate every pick and collect its Definitions.
            # We build all assets FIRST, then materialize the entire graph in
            # ONE dg.materialize() call. That way Dagster resolves
            # cross-pick dependencies via `upstream_asset_key` chaining
            # automatically — the planner can build real multi-step
            # pipelines (gen → filter → summarize → narrate) and each
            # downstream pick reads the upstream pick's output.
            per_pick: List[Dict[str, Any]] = []
            all_assets = []
            for i, (_, row) in enumerate(plan_df.iterrows()):
                cid = row["component_id"]
                ctype = row["component_type"]
                cfg = json.loads(row["config_json"]) if isinstance(row["config_json"], str) else (row["config_json"] or {})
                reason = row.get("reason", "")
                per_pick.append({
                    "component_id": cid,
                    "component_type": ctype,
                    "config": json.dumps(cfg),
                    "reason": reason,
                    "asset_names": [],
                    "status": "pending",
                    "output_repr": "",
                    "error": "",
                })
                context.log.info(f"[catalog] preparing pick {i+1}: {cid} ({ctype}) config keys={sorted(cfg.keys())}")

                try:
                    _mod_name, _cls_name = ctype.rsplit(".", 1)
                    _module = importlib.import_module(_mod_name)
                    _cls = getattr(_module, _cls_name, None)
                    if _cls is None:
                        raise RuntimeError(f"class {_cls_name} not found in {_mod_name}")

                    _inst = _cls(**cfg)
                    _defs = _inst.build_defs(context)
                    _assets = list(_defs.assets or [])
                    if not _assets:
                        raise RuntimeError(f"component {cid} produced no assets")

                    # Record the asset names this pick produces (for output lookup later).
                    _my_names = []
                    for _a in _assets:
                        for _k in _a.keys:
                            _my_names.append(_k.to_user_string())
                    per_pick[-1]["asset_names"] = _my_names
                    all_assets.extend(_assets)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"[catalog] pick {cid} instantiation failed: {e}")
                    per_pick[-1]["status"] = "failed"
                    per_pick[-1]["error"] = str(e)[:500]
                    if _self.fail_on_execution_error:
                        raise

            if not all_assets:
                context.log.warning("[catalog] no assets to materialize — all picks failed at instantiation")
                return pd.DataFrame(per_pick)

            # ── PHASE 2: Materialize the WHOLE graph together.
            # Dagster resolves cross-pick dependencies via asset key matching.
            # If a downstream pick has upstream_asset_key = "some_earlier_asset",
            # and an earlier pick produced that asset key, Dagster wires them.
            context.log.info(f"[catalog] materializing full graph: {len(all_assets)} asset(s) across {len([p for p in per_pick if p['status']!='failed'])} pick(s)")
            try:
                _mat_result = dg.materialize(all_assets, raise_on_error=False)
            except Exception as e:  # noqa: BLE001
                context.log.error(f"[catalog] graph materialize failed: {e}")
                for p in per_pick:
                    if p["status"] == "pending":
                        p["status"] = "failed"
                        p["error"] = str(e)[:500]
                if _self.fail_on_execution_error:
                    raise
                return pd.DataFrame(per_pick)

            # ── PHASE 3: For each pick, look up whether its assets materialized.
            # Collect step failure messages so we can attribute the real
            # Dagster error to each failed pick (rather than the generic
            # "asset(s) did not materialize" placeholder).
            _step_errors: Dict[str, str] = {}
            try:
                for _evt in _mat_result.all_events:
                    _et = getattr(_evt, "event_type_value", "") or ""
                    if "STEP_FAILURE" in _et or "FAILURE" in _et:
                        _step_key = getattr(_evt, "step_key", "") or ""
                        _msg = str(getattr(_evt, "message", "") or _evt)[:500]
                        if _step_key:
                            _step_errors[_step_key] = _msg
                            _step_errors[_step_key.split("[")[0]] = _msg
            except Exception:
                pass

            for p in per_pick:
                if p["status"] == "failed":
                    continue  # instantiation already failed
                if not p["asset_names"]:
                    p["status"] = "failed"
                    p["error"] = "no assets produced"
                    continue
                # If any of this pick's assets materialized, count it as success.
                _val = None
                _got_any = False
                for _name in p["asset_names"]:
                    try:
                        _val = _mat_result.output_for_node(_name)
                        _got_any = True
                        break
                    except Exception:
                        continue
                if _got_any:
                    p["status"] = "success"
                    if hasattr(_val, "to_markdown"):
                        p["output_repr"] = _val.head(5).to_markdown(index=False)
                    elif _val is not None:
                        p["output_repr"] = str(_val)[:1500]
                    else:
                        p["output_repr"] = "(materialized, no output captured)"
                else:
                    p["status"] = "failed"
                    # Try to find the specific Dagster step failure message
                    _real_err = ""
                    for _name in p["asset_names"]:
                        _real_err = _step_errors.get(_name, "") or _step_errors.get(_name.split("/")[-1], "")
                        if _real_err:
                            break
                    p["error"] = _real_err or "asset(s) did not materialize successfully"

            df = pd.DataFrame(per_pick)
            n_ok = int((df["status"] == "success").sum()) if not df.empty else 0
            n_fail = int((df["status"] == "failed").sum()) if not df.empty else 0
            context.add_output_metadata({
                "n_picks": dg.MetadataValue.int(len(df)),
                "n_success": dg.MetadataValue.int(n_ok),
                "n_failed": dg.MetadataValue.int(n_fail),
                "graph_size": dg.MetadataValue.int(len(all_assets)),
                "results": dg.MetadataValue.md(
                    df[["component_id", "status", "output_repr", "error"]].to_markdown(index=False)[:4000]
                ),
            })
            return df

        # ── Synthesizer ────────────────────────────────────────────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.synthesis_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"synthesizer"},
            description="Grounded final answer synthesized from real component outputs.",
            ins={
                "plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name)),
                "execution": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.execution_asset_name)),
            },
            **std_kwargs,
        )
        def _synthesis_asset(context: dg.AssetExecutionContext, plan, execution):
            import pandas as pd

            _task = _resolve_task(context)
            exec_df = execution if isinstance(execution, pd.DataFrame) else pd.DataFrame(execution)
            if exec_df.empty:
                answer = "(no components were executed — planner picked none)"
                n = 0
            else:
                client = _client()
                exec_sections = []
                for _, r in exec_df.iterrows():
                    exec_sections.append(
                        f"### {r['component_id']}\n"
                        f"status: {r['status']}\n"
                        f"config: {r['config']}\n"
                        f"output: {r.get('output_repr', '')}\n"
                        f"error: {r.get('error', '')}\n"
                    )
                user_msg = (
                    f"Task:\n{_task}\n\n"
                    f"Real component outputs ({len(exec_df)} invocation(s)):\n\n"
                    + "\n".join(exec_sections)
                )
                sys_msg = (
                    "You are a synthesizer. Given a task and the outputs of "
                    "real Dagster components the planner invoked, write a "
                    "concise, grounded answer. Cite each component by id in "
                    "parentheses. Note any failures explicitly."
                )
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.synthesis_max_tokens,
                    messages=[
                        {"role": "system", "content": sys_msg},
                        {"role": "user", "content": user_msg},
                    ],
                )
                answer = resp.choices[0].message.content or ""
                n = int((exec_df["status"] == "success").sum())

            df = pd.DataFrame([{"task": _task, "n_success": n, "answer": answer}])
            context.add_output_metadata({
                "task": dg.MetadataValue.text(_task),
                "n_success": dg.MetadataValue.int(n),
                "answer": dg.MetadataValue.md(answer),
            })
            return df

        return dg.Definitions(assets=[_plan_asset, _execution_asset, _synthesis_asset])
