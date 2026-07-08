"""Planned Catalog Agent — StateBackedComponent variant of CatalogAgentComponent.

Runs the LLM planner ONCE at prepare time (write_state_to_path) and caches
the full plan to disk via Dagster's state-backed component mechanism. On
every subsequent load, build_defs_from_state reads the cached plan and
emits REAL Dagster assets — one per pick, wired via upstream_asset_key.
Zero LLM cost on load. No `step_N` wrappers.

Prep with `dg utils refresh-defs-state` or `dagster dev` (auto). To
re-plan: change the task in defs.yaml and refresh state.

Compared to CatalogAgentComponent (the exploration variant):
  - No iterative step assets. The DAG shows CONCRETE assets — synthetic_orders,
    dataframe_join, formula, etc. — with their real upstream/downstream deps.
  - LLM only runs at prepare time, not per materialization.
  - Perfect for "input a task in the Dagster+ UI, real assets appear in
    the graph, run them forever without paying tokens."
  - For exploration + transparent trajectory, use CatalogAgentComponent.
"""
import hashlib
import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


# Fields that appear on nearly every component — skip in catalog prompt.
_SKIP_COMMON_FIELDS = {
    "description", "group_name", "owners", "tags", "asset_tags", "kinds", "deps",
    "partition_type", "partition_start", "partition_values", "partition_date_column",
    "partition_static_column", "partition_static_dim", "dynamic_partition_name",
    "partition_dimensions", "freshness_max_lag_minutes", "freshness_cron",
    "retry_policy_max_retries", "retry_policy_delay_seconds", "retry_policy_backoff",
    "include_preview_metadata", "preview_rows", "column_lineage",
}


def _short_type(annotation) -> str:
    s = str(annotation).replace("typing.", "").replace("<class '", "").replace("'>", "")
    if s.startswith("Optional["):
        s = s[len("Optional["):-1]
    return s[:60]


if _HAS_STATE_BACKED:

    @dataclass
    class PlannedCatalogAgentComponent(StateBackedComponent, dg.Resolvable):
        """StateBackedComponent — runs the LLM planner at prepare time, emits real assets.

        Example (Dagster+ UI or defs.yaml):

            ```yaml
            type: dagster_community_components.PlannedCatalogAgentComponent
            attributes:
              task: |
                Generate synthetic orders + customers, join them, group by
                first_name, email, and month, sum total and count orders,
                then store to /tmp/orders_by_customer_month.csv.
              include_categories: [source, ingestion, transformation, sink]
              include_ids: [synthetic_data_generator]
              llm_model: gpt-4o-mini
              api_key_env_var: OPENAI_API_KEY
              max_iterations: 8
            ```

        Load flow:
          1. `dagster dev` (auto) or `dg utils refresh-defs-state` triggers
             `write_state_to_path`.
          2. Component fetches manifest, iterates planner + real
             dg.materialize per pick to discover columns, records the final
             plan (list of picks + configs) to a state file.
          3. `build_defs_from_state` reads the state and instantiates each
             picked component, returning real Dagster assets.
          4. Assets appear in `dg dev` / Dagster+ UI with their real names
             (synthetic_orders, orders_with_customers, ...) and lineage.
          5. Materialize normally — zero LLM cost from this point.

        To re-plan: edit the task, save the YAML, refresh state.
        """

        task: str

        include_ids: Optional[List[str]] = None
        include_categories: Optional[List[str]] = None
        include_tags: Optional[List[str]] = None
        max_catalog_entries: int = 300
        max_iterations: int = 8

        manifest_url: Optional[str] = "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/manifest.json"
        manifest_path: Optional[str] = None

        llm_model: str = "gpt-4o-mini"
        api_key_env_var: str = "OPENAI_API_KEY"
        api_base_env_var: Optional[str] = None
        temperature: float = 0.1
        planner_max_tokens: int = 600
        fail_on_execution_error: bool = False

        group_name: Optional[str] = None
        kinds: Optional[List[str]] = None
        owners: Optional[List[str]] = None
        tags: Optional[Dict[str, str]] = None

        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        # ── StateBackedComponent interface ──────────────────────────────

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            # Key on task hash so different tasks get different state files.
            _task_hash = hashlib.sha256(self.task.encode()).hexdigest()[:12]
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"PlannedCatalogAgent[{_task_hash}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Run the full LLM planning trajectory + cache the plan."""
            plan = self._run_full_trajectory()
            state_path.write_text(json.dumps({
                "task": self.task,
                "plan": plan,
                "task_hash": hashlib.sha256(self.task.encode()).hexdigest()[:12],
            }, indent=2))

        def build_defs_from_state(
            self,
            context: dg.ComponentLoadContext,
            state_path: Optional[Path],
        ) -> dg.Definitions:
            """Instantiate each cached pick and union its Definitions."""
            if state_path is None or not state_path.exists():
                return dg.Definitions()

            state = json.loads(state_path.read_text())
            plan = state.get("plan") or []
            if not plan:
                return dg.Definitions()

            _dcc = importlib.import_module("dagster_community_components")
            all_assets = []
            for pick in plan:
                if pick.get("status") != "success":
                    continue
                _cls_name = pick["component_type"].rsplit(".", 1)[-1]
                _cls = getattr(_dcc, _cls_name, None)
                if _cls is None:
                    continue
                try:
                    _cfg = json.loads(pick["config"]) if isinstance(pick.get("config"), str) else pick.get("config", {})
                    _inst = _cls(**_cfg)
                    _defs = _inst.build_defs(context)
                    all_assets.extend(_defs.assets or [])
                except Exception:  # noqa: BLE001
                    continue

            return dg.Definitions(assets=all_assets)

        # ── Trajectory logic (adapted from CatalogAgentComponent) ───────

        def _client(self):
            import os
            from openai import OpenAI
            api_key = os.environ.get(self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{self.api_key_env_var!r} env var not set.")
            kwargs: Dict[str, Any] = {"api_key": api_key}
            if self.api_base_env_var:
                base_url = os.environ.get(self.api_base_env_var)
                if base_url:
                    kwargs["base_url"] = base_url
            return OpenAI(**kwargs)

        def _load_manifest_and_filter(self):
            _dcc = importlib.import_module("dagster_community_components")
            if self.manifest_path:
                with open(self.manifest_path, "r") as fh:
                    manifest = json.load(fh)
            else:
                with urlopen(self.manifest_url, timeout=30) as resp:
                    manifest = json.load(resp)
            comps = manifest.get("components") or []

            _ids = set(self.include_ids or [])
            _cats = set(self.include_categories or [])
            _tags = set(self.include_tags or [])
            _any = bool(_ids or _cats or _tags)
            filtered = []
            for c in comps:
                if not _any:
                    filtered.append(c); continue
                if c.get("id") in _ids or c.get("category") in _cats:
                    filtered.append(c); continue
                if _tags and (set(c.get("tags") or []) & _tags):
                    filtered.append(c); continue

            resolved = []
            for c in filtered:
                raw_type = c.get("component_type") or c.get("type") or ""
                if raw_type:
                    _cls_name = raw_type.rsplit(".", 1)[-1]
                else:
                    _pascal = "".join(p.capitalize() for p in (c.get("id") or "").split("_"))
                    _cls_name = _pascal + "Component" if hasattr(_dcc, _pascal + "Component") else _pascal
                if hasattr(_dcc, _cls_name):
                    resolved.append({**c, "component_type": f"dagster_community_components.{_cls_name}"})
            return resolved[: self.max_catalog_entries], _dcc

        def _catalog_lines(self, catalog, dcc):
            lines = []
            for c in catalog:
                _cls_name = c["component_type"].rsplit(".", 1)[-1]
                _cls = getattr(dcc, _cls_name, None)
                if _cls is not None and hasattr(_cls, "model_fields"):
                    _req, _opt = [], []
                    for _n, _f in _cls.model_fields.items():
                        if _n in _SKIP_COMMON_FIELDS:
                            continue
                        _ts = _short_type(_f.annotation) if _f.annotation else "any"
                        (_req if _f.is_required() else _opt).append(f"{_n}: {_ts}")
                    _parts = []
                    if _req: _parts.append(f"req=[{', '.join(_req)}]")
                    if _opt: _parts.append(f"opt=[{', '.join(_opt[:4])}{', ...' if len(_opt) > 4 else ''}]")
                    _parts.append("+ std asset_name/upstream_asset_key/etc")
                    _fs = " | ".join(_parts)
                else:
                    _fs = "fields=(unknown)"
                lines.append(f"  - {c['id']}: {c.get('description', '')[:150]}  ({_fs})")
            return lines

        def _run_full_trajectory(self) -> List[Dict[str, Any]]:
            """Iterate: planner LLM → materialize picked component → discover columns → repeat."""
            import pandas as pd

            catalog, _dcc = self._load_manifest_and_filter()
            if not catalog:
                raise RuntimeError(
                    "Catalog filter returned zero components — loosen include_ids / "
                    "include_categories / include_tags."
                )

            lines = self._catalog_lines(catalog, _dcc)
            valid_ids = ", ".join(f'"{c["id"]}"' for c in catalog)

            picks: List[Dict[str, Any]] = []
            prior_dfs: Dict[str, Any] = {}
            client = self._client()

            for iteration in range(1, self.max_iterations + 1):
                # Build prior_summary from picks so far (like CatalogAgentComponent).
                _prior_lines = []
                for p in picks:
                    _lbl = "SUCCESS" if p.get("status") == "success" else "FAILED"
                    _lines_p = [
                        f"Step {p['iteration']}: [{_lbl}]",
                        f"  picked: {p['component_id']}",
                        f"  config: {p.get('config', '')}",
                        f"  produced asset: {p.get('asset_name')}",
                        f"  columns: {p.get('output_columns')}",
                    ]
                    if p.get("status") != "success":
                        _lines_p.append(f"  error: {p.get('error', '')[:400]}")
                    _prior_lines.append("\n".join(_lines_p))
                prior_summary = "\n\n".join(_prior_lines) or "(no prior steps — this is step 1)"

                planner_prompt = (
                    f"You are an iterative pipeline agent. Decide the NEXT step, or declare done.\n\n"
                    f"Task:\n{self.task}\n\n"
                    f"Prior steps:\n{prior_summary}\n\n"
                    f"Available components ({len(catalog)} shown):\n"
                    + "\n".join(lines) + "\n\n"
                    f"Output ONLY a JSON object.\nIf more work is needed:\n"
                    f'  {{"done": false, "id": "<one of: {valid_ids}>", '
                    f'"config": <object; asset_name (unique, snake_case) + required fields; '
                    f'wire _asset_key fields to a prior asset_name to chain>, '
                    f'"reason": "<why this step now>"}}\n'
                    f"If the task is complete:\n"
                    f'  {{"done": true, "reason": "<why done>"}}\n\n'
                    f"CRITICAL:\n"
                    f"  • asset_name MUST be new, unique, snake_case; NEVER equal to an upstream_asset_key.\n"
                    f"  • NEVER reference an asset that wasn\'t produced by a prior step.\n"
                    f"  • FOLLOW the task literally. If it says 'join', you MUST use dataframe_join.\n"
                    f"  • If task mentions 'month'/'year'/'quarter'/'week' and data has a raw timestamp, "
                    f"insert a formula step FIRST to derive it.\n"
                    f"  • For summarize/filter, group_by is List[str], aggregations is Dict[str, Any] "
                    f"like {{'total': {{'col': '<real_col>', 'agg': 'sum'}}}}.\n"
                    f"  • For MULTI-SOURCE (dataframe_join), set BOTH left_asset_key and right_asset_key,\n"
                    f"    and they MUST reference DIFFERENT prior assets — never join an asset with itself.\n"
                    f"  • synthetic_data_generator produces ONE schema per call. If the task mentions\n"
                    f"    multiple distinct datasets (e.g. 'orders and customers'), use SEPARATE calls\n"
                    f"    with different schema_type values (e.g. schema_type: 'orders', then schema_type: 'customers').\n"
                    f"  • NEVER declare done until EVERY part of the task is complete. If the task says\n"
                    f"    'store to a csv' / 'write to file' / 'save' — a sink step (e.g. dataframe_to_csv)\n"
                    f"    MUST have run successfully first."
                )

                resp = client.chat.completions.create(
                    model=self.llm_model,
                    temperature=self.temperature,
                    max_tokens=self.planner_max_tokens,
                    messages=[
                        {"role": "system", "content": "You are an iterative agent picking real Dagster components."},
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
                except json.JSONDecodeError:
                    picks.append({
                        "iteration": iteration, "done": True, "component_id": None,
                        "component_type": None, "config": None, "reason": "planner JSON parse failed",
                        "asset_name": None, "output_columns": [], "status": "failed", "error": "parse fail",
                    })
                    break

                if plan.get("done"):
                    break

                cid = plan.get("id")
                cfg = plan.get("config") or {}
                reason = plan.get("reason", "")
                entry = next((c for c in catalog if c["id"] == cid), None)
                if entry is None:
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": None, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": None, "output_columns": [], "status": "failed",
                        "error": f"unknown id {cid!r}",
                    })
                    continue

                ctype = entry["component_type"]
                asset_name = cfg.get("asset_name") or f"{cid}_step{iteration}"

                # Validate: no self-dep, no dangling upstream
                _dangling, _self_dep = [], []
                for _k, _v in cfg.items():
                    if _k.endswith("_asset_key") or _k == "asset_key":
                        if isinstance(_v, str) and _v:
                            if _v == asset_name: _self_dep.append(f"{_k}={_v!r}")
                            elif _v not in prior_dfs: _dangling.append(f"{_k}={_v!r}")
                    elif _k.endswith("_asset_keys") or _k == "asset_keys":
                        if isinstance(_v, list):
                            for _vv in _v:
                                if isinstance(_vv, str) and _vv:
                                    if _vv == asset_name: _self_dep.append(f"{_k}={_vv!r}")
                                    elif _vv not in prior_dfs: _dangling.append(f"{_k}={_vv!r}")
                # Reject degenerate self-joins where left_asset_key == right_asset_key.
                _left = cfg.get("left_asset_key")
                _right = cfg.get("right_asset_key")
                _self_join = (
                    isinstance(_left, str) and isinstance(_right, str) and _left == _right
                )
                if _self_join:
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [],
                        "status": "failed",
                        "error": (
                            f"degenerate self-join: left_asset_key == right_asset_key == {_left!r}. "
                            f"Join requires two DIFFERENT upstream datasets. If the task needs multiple "
                            f"datasets, generate them via separate synthetic_data_generator calls first "
                            f"(with different schema_type values)."
                        ),
                    })
                    continue

                if _self_dep or _dangling:
                    _err = (
                        f"self-dep: {_self_dep}" if _self_dep
                        else f"dangling upstream: {_dangling}; available: {sorted(prior_dfs.keys())}"
                    )
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [],
                        "status": "failed", "error": _err,
                    })
                    continue

                # Materialize this pick in-process to discover its output columns.
                try:
                    _mod_name, _cls_name = ctype.rsplit(".", 1)
                    _module = importlib.import_module(_mod_name)
                    _cls = getattr(_module, _cls_name)
                    _inst = _cls(**cfg)
                    _defs = _inst.build_defs(None)
                    _assets = list(_defs.assets or [])

                    _extra_assets = []
                    _wired = set()
                    for _k, _v in cfg.items():
                        if _k.endswith("_asset_key") or _k == "asset_key":
                            if isinstance(_v, str) and _v in prior_dfs and _v not in _wired:
                                _extra_assets.append(_make_source_asset(_v, prior_dfs[_v]))
                                _wired.add(_v)
                        elif _k.endswith("_asset_keys") or _k == "asset_keys":
                            if isinstance(_v, list):
                                for _vv in _v:
                                    if isinstance(_vv, str) and _vv in prior_dfs and _vv not in _wired:
                                        _extra_assets.append(_make_source_asset(_vv, prior_dfs[_vv]))
                                        _wired.add(_vv)

                    _mat = dg.materialize(_extra_assets + _assets, raise_on_error=False)
                    if not _mat.success:
                        _err_msg = ""
                        for _evt in _mat.all_events:
                            if "FAILURE" in getattr(_evt, "event_type_value", "") or "":
                                _err_msg = str(getattr(_evt, "message", "") or _evt)[:300]
                                break
                        raise RuntimeError(_err_msg or "materialize failed")

                    _val = None
                    for _a in _assets:
                        for _k in _a.keys:
                            try:
                                _val = _mat.output_for_node(_k.to_user_string())
                                break
                            except Exception:
                                continue
                        if _val is not None: break

                    _cols = list(_val.columns) if hasattr(_val, "columns") else []
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": _cols,
                        "status": "success", "error": "",
                    })
                    if _val is not None and asset_name:
                        prior_dfs[asset_name] = _val
                except Exception as e:  # noqa: BLE001
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [],
                        "status": "failed", "error": str(e)[:400],
                    })
                    if self.fail_on_execution_error:
                        raise

            return picks


def _make_source_asset(name: str, df_value):
    @dg.asset(key=dg.AssetKey.from_user_string(name))
    def _upstream_source():
        return df_value
    return _upstream_source


if not _HAS_STATE_BACKED:
    # Older Dagster without StateBackedComponent — export a stub that
    # raises so users get a clear error message.
    class PlannedCatalogAgentComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError(
                "PlannedCatalogAgentComponent requires Dagster with "
                "StateBackedComponent support (post-2026 dagster>=1.11 or later). "
                "For older Dagster, use CatalogAgentComponent instead."
            )
