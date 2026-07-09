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


# OpenAI $ per 1M tokens (July 2026, as of Tier 1 published pricing).
# `cached` is the discount rate applied to prompt_tokens_details.cached_tokens.
_OPENAI_PRICING = {
    "gpt-4o":        {"input": 2.50, "cached": 1.25, "output": 10.00},
    "gpt-4o-mini":   {"input": 0.15, "cached": 0.075, "output": 0.60},
    "gpt-4.1":       {"input": 2.00, "cached": 0.50, "output": 8.00},
    "gpt-4.1-mini":  {"input": 0.40, "cached": 0.10, "output": 1.60},
    "gpt-5":         {"input": 1.25, "cached": 0.125, "output": 10.00},
    "gpt-5-mini":    {"input": 0.25, "cached": 0.025, "output": 2.00},
}


def _price_for(model: str) -> Dict[str, float]:
    """Return per-1M pricing dict for the closest-matching model name."""
    if model in _OPENAI_PRICING:
        return _OPENAI_PRICING[model]
    # Match on prefix (e.g. "gpt-4o-2024-08-06" → gpt-4o).
    for _k, _v in _OPENAI_PRICING.items():
        if model.startswith(_k):
            return _v
    # Unknown model → fall back to gpt-4o-mini pricing (conservative).
    return _OPENAI_PRICING["gpt-4o-mini"]


def _short_type(annotation) -> str:
    s = str(annotation).replace("typing.", "").replace("<class '", "").replace("'>", "")
    if s.startswith("Optional["):
        s = s[len("Optional["):-1]
    return s[:60]


def _enum_values(annotation) -> Optional[List[str]]:
    """Extract literal string choices from `Literal[...]` or `Optional[Literal[...]]`."""
    try:
        import typing as _t
        _ann = annotation
        # Unwrap Optional[X] / Union[X, None] to get X.
        _origin = _t.get_origin(_ann)
        if _origin is _t.Union:
            _args = [a for a in _t.get_args(_ann) if a is not type(None)]
            if len(_args) == 1:
                _ann = _args[0]
                _origin = _t.get_origin(_ann)
        if _origin is _t.Literal:
            _vals = [a for a in _t.get_args(_ann) if isinstance(a, str)]
            return _vals or None
    except Exception:  # noqa: BLE001
        pass
    return None


def _extract_choices_from_description(desc: str) -> Optional[List[str]]:
    """Heuristic: scrape single-quoted / double-quoted tokens out of a Field description
    when the description reads like 'one of A, B, C' — helps for enums the author wrote
    with str (not Literal). Only returns 2-6 tokens; wider returns None."""
    import re as _re
    if not desc:
        return None
    # Look for patterns like: 'foo' | "foo" -- collect single/double-quoted values.
    _tokens = _re.findall(r"['\"]([a-zA-Z][a-zA-Z0-9_]{1,25})['\"]", desc)
    _tokens = list(dict.fromkeys(_tokens))  # dedupe, preserve order
    if 2 <= len(_tokens) <= 6:
        return _tokens
    return None


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

        # OpenAI TPM (tokens-per-minute) budget for a single request. When set,
        # the component trims the catalog line contents progressively (drops
        # anti_uses hints → trims descriptions → cuts entries) until the estimated
        # prompt fits `tpm_budget - 2000` (safety margin for planner output +
        # prior_summary growth). Trade-offs:
        #   • Lower budgets force smaller catalog / weaker hints → LLM has less
        #     context → more picking errors → more iterations wasted → SLOWER
        #     end-to-end despite fewer tokens per call. The right knob when your
        #     Tier can't handle the full catalog is `include_ids` (narrow the
        #     universe of picks); `tpm_budget` is a fallback safety net.
        #   • OpenAI Tier reference: Tier1=30000, Tier2=500000, Tier3=5000000.
        #   • None (default) = no trimming; trust the user's include_* filters.
        tpm_budget: Optional[int] = None

        # Pre-filter the catalog with ONE cheap gpt-4o-mini call before the
        # iterative planner runs. The pre-filter sees (id + description) for
        # every filtered component and returns ~30 IDs it thinks the task
        # actually needs. Effective at semantic matching that keyword scoring
        # can't handle (e.g. task says "predict survival" → pre-filter picks
        # logistic_regression_model even though the words don't overlap).
        # Trade-offs:
        #   • Adds one ~2-5s LLM call at trajectory start.
        #   • Costs ~$0.001 (mini pricing).
        #   • If the pre-filter misses a component the planner needs, that
        #     component simply isn't available → planner may fail or
        #     pick something suboptimal. Set include_ids explicitly to
        #     guarantee availability.
        #   • Best combined with tpm_budget: the pre-filter shrinks the
        #     catalog, tpm_budget catches overflow if the shrunk catalog
        #     is still too big.
        prefilter_llm: bool = False
        prefilter_max_entries: int = 40  # how many components the pre-filter keeps

        # Which external resources / credentials are actually configured in this
        # environment. Feeds into the planner prompt so the LLM avoids components
        # that need resources you don't have. Any component with
        # `agent_hints.requires_resources` containing a resource NOT in this list
        # will be filtered out of the catalog before the planner sees it.
        #
        # Example — user has Snowflake creds but not BQ:
        #   available_resources: [snowflake_resource]
        # → the planner won't see bigquery_query_asset, dataframe_to_bigquery, etc.
        #
        # Default None = no filtering. Components authored WITHOUT
        # requires_resources are always available (safe default).
        available_resources: Optional[List[str]] = None

        # Domain-specific hints injected into the planner prompt just after the
        # task. Give per-task context the manifest can't know: actual column
        # value casings, size-dependent choices, business rules. E.g.:
        #   task_hints:
        #     - "Survived column has values 'yes'/'no' (already lowercased)."
        #     - "For row-count over 1M prefer dataframe_to_snowflake_bulk."
        task_hints: Optional[List[str]] = None
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
            _timing = getattr(self, "_last_timing_summary", None) or {}
            state_path.write_text(json.dumps({
                "task": self.task,
                "plan": plan,
                "task_hash": hashlib.sha256(self.task.encode()).hexdigest()[:12],
                "timing": _timing,
            }, indent=2))

        def build_defs_from_state(
            self,
            context: dg.ComponentLoadContext,
            state_path: Optional[Path],
        ) -> dg.Definitions:
            """Instantiate each cached pick and union its Definitions.

            If the trajectory produced NO successful picks, emit a soft-signal
            placeholder asset instead of empty defs — so the user sees SOMETHING
            in the graph with metadata explaining why the pipeline is empty.
            """
            if state_path is None or not state_path.exists():
                return dg.Definitions(assets=[self._make_placeholder(
                    reason="No cached state file found. Refresh state via `dg utils refresh-defs-state` or `dagster dev` to run the LLM planner.",
                    state=None,
                )])

            state = json.loads(state_path.read_text())
            plan = state.get("plan") or []
            if not plan:
                return dg.Definitions(assets=[self._make_placeholder(
                    reason="Trajectory ran but produced no picks (empty plan). Check state file for errors.",
                    state=state,
                )])

            _dcc = importlib.import_module("dagster_community_components")
            all_assets = []
            _built_asset_names: List[str] = []
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

                    # Attach planner-decision metadata to every asset from
                    # this pick. Metadata surfaces in `dg dev` on the asset
                    # detail page — no more `cat state` archaeology.
                    _pick_meta = {
                        "planner_component_id": dg.MetadataValue.text(pick.get("component_id") or ""),
                        "planner_iteration": dg.MetadataValue.int(pick.get("iteration") or 0),
                        "planner_reason": dg.MetadataValue.text(str(pick.get("reason") or "")[:1000]),
                        "planner_output_columns": dg.MetadataValue.json(pick.get("output_columns") or []),
                        "planner_config": dg.MetadataValue.md(
                            f"```json\n{json.dumps((json.loads(pick['config']) if isinstance(pick.get('config'), str) else pick.get('config')) or {}, indent=2)[:1500]}\n```"
                        ),
                    }
                    _decorated_assets = []
                    for _ad in (_defs.assets or []):
                        try:
                            _mbk = {_k: _pick_meta for _k in _ad.keys}
                            _ad = _ad.with_attributes(metadata_by_key=_mbk)
                        except Exception:  # noqa: BLE001
                            # Older Dagster or non-standard AssetsDefinition
                            # → skip decoration, keep the raw asset.
                            pass
                        _decorated_assets.append(_ad)
                    all_assets.extend(_decorated_assets)
                    _built_asset_names.append(pick.get("asset_name") or "")
                except Exception:  # noqa: BLE001
                    continue

            # If nothing built (all picks failed OR every class instantiation
            # errored), emit only the placeholder.
            if not all_assets:
                return dg.Definitions(assets=[self._make_placeholder(
                    reason="Trajectory produced picks but zero assets built successfully. Every pick either failed or its class couldn't be instantiated.",
                    state=state,
                )])

            # Partial success: include a companion diagnostics asset iff any
            # picks failed. Real assets always ship.
            _n_failed = sum(1 for p in plan if p.get("status") != "success")
            if _n_failed:
                all_assets.append(self._make_placeholder(
                    reason=f"Partial success — {len(_built_asset_names)} of {len(plan)} picks succeeded. See `attempts` metadata for what failed.",
                    state=state,
                    partial=True,
                ))

            return dg.Definitions(assets=all_assets)

        def _make_placeholder(self, reason: str, state: Optional[Dict[str, Any]], partial: bool = False):
            """Build a soft-signal placeholder asset that succeeds on materialize
            but signals a problem by its very presence. All diagnostics live in
            the asset's metadata."""
            _task_hash = hashlib.sha256((self.task or "").encode()).hexdigest()[:12]
            _key_suffix = "diagnostics" if partial else "no_plan"
            _asset_key = dg.AssetKey(f"_planned_agent_{_key_suffix}_{_task_hash}")

            _plan = (state or {}).get("plan") or []
            _timing = (state or {}).get("timing") or {}
            _picks_ok = sum(1 for p in _plan if p.get("status") == "success")
            _picks_bad = len(_plan) - _picks_ok
            _last_err = next(
                (str(p.get("error"))[:400] for p in reversed(_plan) if p.get("error")),
                "(no errors recorded)",
            )
            _attempts_md: List[str] = []
            for i, p in enumerate(_plan, 1):
                _lbl = "✓" if p.get("status") == "success" else "✗"
                _row = f"{i:2d}. {_lbl} {(p.get('component_id') or '?'):30s} → {p.get('asset_name') or '(none)'}"
                if p.get("status") != "success":
                    _row += f"    ERR: {str(p.get('error') or '')[:200]}"
                _attempts_md.append(_row)
            _attempts_str = "\n".join(_attempts_md) or "(no picks)"

            _next_steps = (
                "1. Inspect the full state file: cat "
                "`src/<project>/defs/.local_defs_state/PlannedCatalogAgent__*/state`\n"
                "2. Narrow `include_ids` to the components your task actually needs.\n"
                "3. Try `llm_model: gpt-4o` (gpt-4o-mini often fails multi-hop / branching reasoning).\n"
                "4. Set `available_resources` to exclude components needing credentials you don't have.\n"
                "5. Set `tpm_budget` if you're hitting OpenAI rate limits.\n"
                "6. Enable `prefilter_llm: true` for wide catalogs.\n"
                "7. Re-run `dg utils refresh-defs-state` after any config change."
            )

            _task_snip = (self.task or "")[:500] + ("..." if self.task and len(self.task) > 500 else "")
            _reason = reason
            _kinds = ["planner_diagnostics"]

            @dg.asset(
                key=_asset_key,
                description=f"PlannedCatalogAgent diagnostics — {reason}",
                kinds=_kinds,
                group_name=(self.group_name or "planned_agent_diagnostics"),
                metadata={
                    "reason": dg.MetadataValue.text(_reason),
                    "task": dg.MetadataValue.text(_task_snip),
                    "picks_total": dg.MetadataValue.int(len(_plan)),
                    "picks_succeeded": dg.MetadataValue.int(_picks_ok),
                    "picks_failed": dg.MetadataValue.int(_picks_bad),
                    "last_error": dg.MetadataValue.text(_last_err),
                    "trajectory_total_s": dg.MetadataValue.float(float(_timing.get("trajectory_total_s") or 0)),
                    "llm_total_s": dg.MetadataValue.float(float(_timing.get("llm_total_s") or 0)),
                    "materialize_total_s": dg.MetadataValue.float(float(_timing.get("materialize_total_s") or 0)),
                    "attempts": dg.MetadataValue.md(f"```\n{_attempts_str}\n```"),
                    "next_steps": dg.MetadataValue.md(_next_steps),
                },
            )
            def _placeholder():
                # Soft signal: succeed on materialize. Its presence in the
                # graph is the signal that something needs attention.
                return dg.MaterializeResult(
                    metadata={
                        "reason": dg.MetadataValue.text(_reason),
                        "last_error": dg.MetadataValue.text(_last_err),
                    }
                )

            return _placeholder

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

            # If `available_resources` is set, drop components whose
            # agent_hints.requires_resources includes a resource NOT in
            # the allowed set. Components without requires_resources are
            # always kept (safe default — assume no external requirement).
            if self.available_resources is not None:
                _allowed = set(self.available_resources)
                _filtered_2 = []
                for c in filtered:
                    _req = (c.get("agent_hints") or {}).get("requires_resources") or []
                    if isinstance(_req, str):
                        _req = [_req]
                    _missing = [r for r in _req if r not in _allowed]
                    if not _missing:
                        _filtered_2.append(c)
                filtered = _filtered_2

            # Always: drop components whose `agent_hints.requires_pip` includes
            # a package not importable in this Python environment. Auto-detected
            # via importlib.util.find_spec (no import side-effects). This lets
            # analytics/ML components declare their real dependencies once, and
            # the planner automatically avoids them when the packages are absent
            # — no "ModuleNotFoundError" iterations wasted.
            import importlib.util as _ilu
            _pip_ok_cache: Dict[str, bool] = {}
            def _pip_available(_pkg: str) -> bool:
                if _pkg not in _pip_ok_cache:
                    try:
                        _pip_ok_cache[_pkg] = _ilu.find_spec(_pkg) is not None
                    except Exception:  # noqa: BLE001
                        _pip_ok_cache[_pkg] = False
                return _pip_ok_cache[_pkg]

            _filtered_3 = []
            for c in filtered:
                _pip = (c.get("agent_hints") or {}).get("requires_pip") or []
                if isinstance(_pip, str):
                    _pip = [_pip]
                _missing_pip = [p for p in _pip if not _pip_available(p)]
                if not _missing_pip:
                    _filtered_3.append(c)
            filtered = _filtered_3

            # Build a reverse-index: snake_case(class_name) → class_name. Covers
            # acronyms (RFMSegmentationComponent → rfm_segmentation) which the
            # naive pascalize pass misses.
            import re as _re
            def _snake(_name: str) -> str:
                # Insert underscores before uppercase runs, then lowercase.
                _s = _re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', _name)
                _s = _re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', _s)
                _s = _s.lower()
                # Strip a trailing _component if present.
                if _s.endswith("_component"):
                    _s = _s[:-len("_component")]
                return _s
            _snake_index = {}
            for _attr in dir(_dcc):
                if not _attr[:1].isupper():
                    continue
                _snake_index.setdefault(_snake(_attr), _attr)

            resolved = []
            for c in filtered:
                raw_type = c.get("component_type") or c.get("type") or ""
                _cls_name = None
                if raw_type:
                    _cand = raw_type.rsplit(".", 1)[-1]
                    if hasattr(_dcc, _cand):
                        _cls_name = _cand
                if _cls_name is None:
                    # Try naive pascalize.
                    _pascal = "".join(p.capitalize() for p in (c.get("id") or "").split("_"))
                    for _cand in (_pascal + "Component", _pascal):
                        if hasattr(_dcc, _cand):
                            _cls_name = _cand
                            break
                if _cls_name is None:
                    # Reverse-lookup: snake_case(class_name) → class.
                    _cls_name = _snake_index.get(c.get("id"))
                if _cls_name is not None and hasattr(_dcc, _cls_name):
                    resolved.append({**c, "component_type": f"dagster_community_components.{_cls_name}"})
            return resolved[: self.max_catalog_entries], _dcc

        def _catalog_lines(self, catalog, dcc, hint_fields=("side_effects", "anti_uses"), description_max=300):
            """Render catalog lines with configurable trim level.
            hint_fields: which agent_hints keys to include per line (empty tuple = no hints).
            description_max: char cap on component description.
            """
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
                        _choices = _enum_values(_f.annotation)
                        if _choices is None:
                            _desc = getattr(_f, "description", "") or ""
                            _choices = _extract_choices_from_description(_desc)
                        if _choices:
                            _ts = f"{_ts}={'|'.join(_choices)}"
                        (_req if _f.is_required() else _opt).append(f"{_n}: {_ts}")
                    _parts = []
                    if _req: _parts.append(f"req=[{', '.join(_req)}]")
                    if _opt: _parts.append(f"opt=[{', '.join(_opt[:4])}{', ...' if len(_opt) > 4 else ''}]")
                    _parts.append("+ std asset_name/upstream_asset_key/etc")
                    _fs = " | ".join(_parts)
                else:
                    _fs = "fields=(unknown)"

                _hints = c.get("agent_hints") or {}
                _hint_bits = []
                for _hk in hint_fields:
                    _hv = _hints.get(_hk)
                    if _hv:
                        # anti_uses is where authors document config shape +
                        # enum values — allow more room. side_effects stays
                        # tighter to keep total line length bounded.
                        _cap = 400 if _hk == "anti_uses" else 200
                        _hint_bits.append(f"{_hk}={str(_hv)[:_cap]}")
                _hint_str = f"  [{' | '.join(_hint_bits)}]" if _hint_bits else ""

                lines.append(f"  - {c['id']}: {c.get('description', '')[:description_max]}  ({_fs}){_hint_str}")
            return lines

        def _llm_prefilter(self, catalog):
            """Cheap gpt-4o-mini call to shortlist the catalog for the task.

            Returns (filtered_catalog, notes). If prefilter_llm is False,
            returns the input catalog unchanged.
            """
            if not self.prefilter_llm:
                return catalog, []

            if len(catalog) <= self.prefilter_max_entries:
                return catalog, [f"prefilter_llm: skipped (catalog {len(catalog)} <= {self.prefilter_max_entries})"]

            # Minimal per-component line: just id + short description.
            _lines = [
                f"- {c['id']} ({c.get('category','?')}): {c.get('description','')[:100]}"
                for c in catalog
            ]
            _catalog_str = "\n".join(_lines)
            _prompt = (
                f"You are shortlisting Dagster component IDs from a catalog for the task below. "
                f"A separate planner LLM will pick components from your shortlist to build a real "
                f"data pipeline — if you exclude a component it needs, the pipeline will FAIL.\n\n"
                f"Task:\n{self.task}\n\n"
                f"Return {self.prefilter_max_entries}+ component IDs (never fewer than 20). "
                f"Err on the side of INCLUDING components — extra picks are cheap; missing picks are fatal.\n\n"
                f"Include ALL of these categories that apply:\n"
                f"  1. INGESTION components matching the input format the task describes "
                f"(csv → file_ingestion; API → rest_api_fetcher; DB → database_query; etc.).\n"
                f"  2. Every TRANSFORMATION the task calls for by name or intent (dedup, cleanse, join, "
                f"filter, group-by/summarize, encode, bin, impute, coerce, sort, pivot/unpivot, formula, etc.).\n"
                f"  3. Any ANALYTICS/MODEL the task calls for (predict → logistic_regression_model / "
                f"linear_regression_model; forecast → arima_forecast; anomaly → anomaly_detection; etc.).\n"
                f"  4. Every SINK implied by the task (write to CSV → dataframe_to_csv; parquet → "
                f"dataframe_to_parquet; Snowflake → dataframe_to_snowflake; write ONE csv → one instance, "
                f"write THREE csvs → still ONE component id, just used 3 times).\n"
                f"  5. LIKELY BRIDGE components: formula (compute derived columns), select_columns, "
                f"type_coercer, imputation, outlier_clipper — include these even if the task doesn't "
                f"explicitly name them; the planner often needs them.\n\n"
                f"Available components ({len(catalog)}):\n{_catalog_str}\n\n"
                f'Reply ONLY with: {{"selected": ["file_ingestion", "unique_dedup", ...]}}. '
                f"IDs must be EXACT matches from the catalog above."
            )

            client = self._client()
            import time as _t
            _start = _t.time()
            resp = client.chat.completions.create(
                model="gpt-4o-mini",   # always mini for the pre-filter
                temperature=0.0,
                max_tokens=2000,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": _prompt}],
            )
            _elapsed = round(_t.time() - _start, 2)

            _raw = resp.choices[0].message.content or "{}"
            try:
                _sel = json.loads(_raw).get("selected") or []
            except Exception:  # noqa: BLE001
                _sel = []
            _sel_set = {str(x) for x in _sel}

            # ALWAYS include the user's explicit include_ids in the shortlist —
            # they asked for them by name, the prefilter must not drop them.
            if self.include_ids:
                _forced = set(self.include_ids)
                _sel_set |= _forced

            _filtered = [c for c in catalog if c["id"] in _sel_set]

            _notes = [
                f"prefilter_llm: catalog {len(catalog)} → {len(_filtered)} in {_elapsed}s",
                f"prefilter_llm: kept={sorted(_sel_set)[:20]}{'...' if len(_sel_set) > 20 else ''}",
                f"prefilter_llm: raw_response_head={_raw[:200]}",
            ]

            # Safety: if the pre-filter returned nothing or something ridiculously
            # small, fall back to the original catalog so the trajectory still has
            # something to pick from.
            if len(_filtered) < 5:
                _notes.append(f"prefilter_llm: returned <5 picks; falling back to full catalog")
                return catalog, _notes

            return _filtered, _notes

        def _fit_to_budget(self, catalog, dcc):
            """Return (lines, catalog, trim_notes) fit to self.tpm_budget.

            If tpm_budget is None → no trimming, return the full catalog + default hints.
            Otherwise progressively trim in this order until we fit
            `tpm_budget - 2000` (safety margin for planner output + prior_summary growth):
              1. Drop anti_uses hints (keep side_effects only)
              2. Trim description 300 → 150 chars
              3. Drop all hints
              4. Cut catalog entries from the tail
            """
            default_hints = ("side_effects", "anti_uses")
            if self.tpm_budget is None:
                return self._catalog_lines(catalog, dcc, default_hints, 300), catalog, []

            _safety = 2000
            _budget = max(self.tpm_budget - _safety, 1500)  # minimum sane floor
            _trim_notes: List[str] = []

            def _estimate(_lines):
                # 4 chars ≈ 1 token (rough — good enough for budget decisions).
                # +1250 tokens (5000 chars / 4) covers rules boilerplate.
                _static = "\n".join(_lines) + (self.task or "")
                return (len(_static) // 4) + 1250

            # Level 1: full hints + full description
            _lines = self._catalog_lines(catalog, dcc, default_hints, 300)
            _est = _estimate(_lines)
            if _est <= _budget:
                _trim_notes.append(f"level=0 (full hints+desc); est={_est}; budget={_budget}")
                return _lines, catalog, _trim_notes

            # Level 2: drop anti_uses; keep side_effects; full description
            _lines = self._catalog_lines(catalog, dcc, ("side_effects",), 300)
            _est = _estimate(_lines)
            _trim_notes.append(f"level=1 (dropped anti_uses); est={_est}")
            if _est <= _budget:
                return _lines, catalog, _trim_notes

            # Level 3: still side_effects; trim description
            _lines = self._catalog_lines(catalog, dcc, ("side_effects",), 150)
            _est = _estimate(_lines)
            _trim_notes.append(f"level=2 (desc→150); est={_est}")
            if _est <= _budget:
                return _lines, catalog, _trim_notes

            # Level 4: drop hints entirely
            _lines = self._catalog_lines(catalog, dcc, (), 150)
            _est = _estimate(_lines)
            _trim_notes.append(f"level=3 (dropped all hints); est={_est}")
            if _est <= _budget:
                return _lines, catalog, _trim_notes

            # Level 5: cut entries from the tail until we fit
            _trimmed = list(catalog)
            while _trimmed and _est > _budget:
                _trimmed.pop()
                _lines = self._catalog_lines(_trimmed, dcc, (), 150)
                _est = _estimate(_lines)
            _trim_notes.append(
                f"level=4 (dropped hints+desc→150+catalog {len(catalog)}→{len(_trimmed)}); est={_est}"
            )
            _trim_notes.append(
                "WARNING: tpm_budget forced aggressive catalog trim. Consider "
                "narrowing include_ids / include_tags — a smaller filter with rich hints "
                "beats a huge filter with no hints for pick quality."
            )
            return _lines, _trimmed, _trim_notes

        def _run_full_trajectory(self) -> List[Dict[str, Any]]:
            """Iterate: planner LLM → materialize picked component → discover columns → repeat."""
            import pandas as pd

            catalog, _dcc = self._load_manifest_and_filter()
            if not catalog:
                raise RuntimeError(
                    "Catalog filter returned zero components — loosen include_ids / "
                    "include_categories / include_tags."
                )

            # Pre-filter the catalog via cheap LLM call (no-op if prefilter_llm=False).
            catalog, _prefilter_notes = self._llm_prefilter(catalog)

            # Then trim catalog contents to fit `tpm_budget` (no-op if unset).
            lines, catalog, _trim_notes = self._fit_to_budget(catalog, _dcc)
            _trim_notes = _prefilter_notes + _trim_notes
            valid_ids = ", ".join(f'"{c["id"]}"' for c in catalog)

            # STATIC prefix — identical across all iterations of THIS trajectory.
            # OpenAI auto-caches prompt prefixes ≥1024 tokens for 5-10 minutes, so
            # putting task + catalog + rules FIRST means iterations 2..N get a 50%
            # discount on those tokens. Only the DYNAMIC suffix (prior_summary +
            # iteration hint) changes across calls.
            _hints_block = ""
            if self.task_hints:
                _hb = "\n".join(f"  - {h}" for h in self.task_hints)
                _hints_block = f"\nAdditional task hints (domain knowledge):\n{_hb}\n"

            static_prompt = (
                f"You are an iterative pipeline agent. Decide the NEXT step, or declare done.\n\n"
                f"Task:\n{self.task}\n"
                f"{_hints_block}\n"
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
                f"  • NEVER reference an asset that wasn't produced by a prior step.\n"
                f"  • FOLLOW the task literally. If it says 'join', you MUST use dataframe_join.\n"
                f"  • Field values shown as `name: type=A|B|C` are ENUMS — you MUST pick one of the listed\n"
                f"    values verbatim. Do not invent similar-sounding alternatives (e.g. 'quantile' when\n"
                f"    the enum is 'equal_freq').\n"
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
                f"    MUST have run successfully first.\n"
                f"  • INGESTION routing: for tabular files at a URL or local path (.csv / .tsv /\n"
                f"    .parquet / .json / .xlsx), use file_ingestion or dataframe_from_csv — NOT\n"
                f"    rest_api_fetcher. rest_api_fetcher is for paginated JSON APIs, not files.\n"
                f"  • If a prior step's output has ZERO columns, its output is not a usable\n"
                f"    DataFrame — the upstream picked is wrong. Do NOT chain further transforms\n"
                f"    off that asset; pick a different ingestion component and try again.\n"
                f"  • BRANCHING: when the task describes MULTIPLE independent outputs (e.g. 'also\n"
                f"    produce a summary', 'also write survivors to a CSV'), each branch picks its\n"
                f"    OWN upstream_asset_key from EARLIER in the pipeline — pick the earliest step\n"
                f"    that has the columns you need. Do NOT feed every branch off the final step.\n"
                f"  • If a step fails with KeyError on a column, LOOK at every prior step's\n"
                f"    output_columns. Find the LATEST prior step whose columns contain the missing\n"
                f"    key. Use THAT asset_name as upstream_asset_key. Do NOT re-pick the same\n"
                f"    upstream that just failed — that will fail again.\n\n"
                f"=== END OF STATIC RULES; DYNAMIC PRIOR-STEPS SUMMARY FOLLOWS ==="
            )

            picks: List[Dict[str, Any]] = []
            prior_dfs: Dict[str, Any] = {}
            client = self._client()

            import time as _time
            _trajectory_start = _time.time()
            _total_llm_seconds = 0.0
            _total_mat_seconds = 0.0
            _total_in_tokens = 0
            _total_out_tokens = 0
            _total_cached_tokens = 0
            _total_cost = 0.0
            _iter_timings: List[Dict[str, Any]] = []
            print(f"[planned_agent] trajectory START: model={self.llm_model} catalog={len(catalog)} tpm_budget={self.tpm_budget}")

            for iteration in range(1, self.max_iterations + 1):
                _iter_start = _time.time()
                _llm_seconds = 0.0
                _mat_seconds = 0.0
                # DYNAMIC suffix — prior_summary changes every iteration.
                _prior_lines = []
                for p in picks:
                    _lbl = "SUCCESS" if p.get("status") == "success" else "FAILED"
                    _emitted = p.get("emitted_asset_names") or []
                    # For multi-asset components (router, train_test_splitter, etc.)
                    # show every emitted asset name so downstream picks can
                    # reference them by their real key, not the placeholder.
                    if len(_emitted) > 1:
                        _produced = f"{p.get('asset_name')}  (multi-asset — emits: {_emitted})"
                    else:
                        _produced = str(p.get('asset_name'))
                    _lines_p = [
                        f"Step {p['iteration']}: [{_lbl}]",
                        f"  picked: {p['component_id']}",
                        f"  config: {p.get('config', '')}",
                        f"  produced asset: {_produced}",
                        f"  columns: {p.get('output_columns')}",
                    ]
                    if p.get("status") != "success":
                        _lines_p.append(f"  error: {p.get('error', '')[:400]}")
                    _prior_lines.append("\n".join(_lines_p))
                prior_summary = "\n\n".join(_prior_lines) or "(no prior steps — this is step 1)"

                planner_prompt = f"{static_prompt}\n\nPrior steps:\n{prior_summary}"

                _llm_start = _time.time()
                _attempts = 0
                while True:
                    try:
                        resp = client.chat.completions.create(
                            model=self.llm_model,
                            temperature=self.temperature,
                            max_tokens=self.planner_max_tokens,
                            response_format={"type": "json_object"},
                            messages=[
                                {"role": "system", "content": "You are an iterative agent picking real Dagster components. Reply ONLY with a single JSON object."},
                                {"role": "user", "content": planner_prompt},
                            ],
                        )
                        break
                    except Exception as _e:  # noqa: BLE001
                        _attempts += 1
                        _msg = str(_e)
                        # Retry on rate-limit / TPM errors up to 3 times, sleeping
                        # 20/40/60s. Any other error propagates immediately.
                        if _attempts < 3 and ("rate_limit" in _msg.lower() or "429" in _msg or "tpm" in _msg.lower()):
                            _wait = 20 * _attempts
                            print(f"[planner] rate-limit hit, sleeping {_wait}s (attempt {_attempts}/3)")
                            _time.sleep(_wait)
                            continue
                        raise
                _llm_seconds = _time.time() - _llm_start

                # Token usage + cost tracking. Cached-prompt discount kicks in on
                # iters ≥2 for gpt-4o family when the static prefix matches.
                _usage = getattr(resp, "usage", None)
                _in_tok = int(getattr(_usage, "prompt_tokens", 0) or 0)
                _out_tok = int(getattr(_usage, "completion_tokens", 0) or 0)
                _cached_tok = 0
                _details = getattr(_usage, "prompt_tokens_details", None)
                if _details is not None:
                    _cached_tok = int(getattr(_details, "cached_tokens", 0) or 0)
                _p = _price_for(self.llm_model)
                _in_cost = ((_in_tok - _cached_tok) * _p["input"] + _cached_tok * _p["cached"]) / 1_000_000
                _out_cost = _out_tok * _p["output"] / 1_000_000
                _iter_cost = round(_in_cost + _out_cost, 6)
                _total_in_tokens += _in_tok
                _total_out_tokens += _out_tok
                _total_cached_tokens += _cached_tok
                _total_cost += _iter_cost

                raw = (resp.choices[0].message.content or "").strip()
                if raw.startswith("```"):
                    raw = raw.strip("`").split("\n", 1)[-1]
                    if raw.endswith("```"):
                        raw = raw.rsplit("```", 1)[0]
                try:
                    plan = json.loads(raw)
                except json.JSONDecodeError as _e:
                    # Don't kill the whole trajectory — the LLM sometimes emits
                    # near-JSON. Record the failure, feed it into prior_summary,
                    # and give the LLM another chance.
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": None,
                        "component_type": None, "config": None, "reason": "planner JSON parse failed",
                        "asset_name": None, "output_columns": [],
                        "status": "failed",
                        "error": f"json parse failed: {str(_e)[:100]}. Output MUST be a single JSON object.",
                    })
                    continue

                if plan.get("done"):
                    # Done-validation: if the task text mentions writing a file
                    # (csv / parquet / json / xlsx / a /path/to/file) but no sink
                    # component has succeeded, override "done" — the LLM is
                    # skipping the required sinks.
                    _sink_ids = {"dataframe_to_csv", "dataframe_to_parquet",
                                 "dataframe_to_json", "dataframe_to_excel",
                                 "dataframe_to_avro"}
                    _task_lower = self.task.lower()
                    _wants_sink = any(
                        _kw in _task_lower for _kw in
                        ["csv", "parquet", "json", "xlsx", "excel", "write to",
                         "store to", "save to", "output to", "to disk", "to a file"]
                    )
                    _succeeded_sinks = [
                        p for p in picks
                        if p.get("component_id") in _sink_ids and p.get("status") == "success"
                    ]
                    # Count distinct file paths in the task — if the task
                    # asks for N CSVs and we've only written M<N, reject done.
                    import re as _re
                    _paths = set(_re.findall(
                        r"/?[\w\-\./]+\.(?:csv|parquet|json|xlsx|avro|xls)", self.task or ""
                    ))
                    _expected_sinks = len(_paths)
                    if _wants_sink and len(_succeeded_sinks) < max(1, _expected_sinks):
                        _missing = (_expected_sinks - len(_succeeded_sinks)) if _expected_sinks else 1
                        picks.append({
                            "iteration": iteration, "done": False, "component_id": None,
                            "component_type": None, "config": None,
                            "reason": plan.get("reason", ""), "asset_name": None,
                            "output_columns": [], "status": "failed",
                            "error": (
                                f"REJECTED 'done': task mentions {_expected_sinks or 'at least one'} "
                                f"output file path(s) — {sorted(_paths) if _paths else 'unspecified'}. "
                                f"Only {len(_succeeded_sinks)} sink(s) have succeeded. Pick "
                                f"{_missing} more sink(s), each with a DIFFERENT upstream_asset_key "
                                f"and DIFFERENT file_path."
                            ),
                        })
                        continue
                    break

                cid = plan.get("id")
                cfg = plan.get("config") or {}
                reason = plan.get("reason", "")

                # Loop guard FIRST: if the planner has picked the SAME component
                # id and failed 3 times in a row, break out. Runs BEFORE the
                # exact-repeat guard so we don't waste iterations on infinite
                # rejections when the LLM ignores the hints.
                _recent_same = [
                    p for p in picks[-3:]
                    if p.get("component_id") == cid and p.get("status") == "failed"
                ]
                if len(_recent_same) >= 3:
                    picks.append({
                        "iteration": iteration, "done": True, "component_id": cid,
                        "component_type": None, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": None, "output_columns": [], "status": "failed",
                        "error": (
                            f"loop guard: {cid!r} failed 3 times in a row — planner is stuck. "
                            f"Ending trajectory to preserve prior successful picks."
                        ),
                    })
                    break

                # Exact-repeat guard: if this pick is a TRUE exact repeat of the
                # last failed one (same component_id + same upstream key + same
                # FULL config), reject it immediately. Config changes = the LLM
                # is legitimately retrying with a different approach — let it
                # through.
                _upstream_of = lambda _c: (_c or {}).get("upstream_asset_key") or (
                    (_c or {}).get("left_asset_key"), (_c or {}).get("right_asset_key")
                )
                _cfg_fingerprint = lambda _c: json.dumps(
                    {k: v for k, v in (_c or {}).items() if k != "asset_name"},
                    sort_keys=True,
                )
                _last_failed = next(
                    (p for p in reversed(picks) if p.get("status") == "failed"), None
                )
                if _last_failed:
                    _last_cfg = _last_failed.get("config")
                    if isinstance(_last_cfg, str):
                        try: _last_cfg = json.loads(_last_cfg)
                        except: _last_cfg = {}
                    if (
                        _last_failed.get("component_id") == cid
                        and _cfg_fingerprint(_last_cfg) == _cfg_fingerprint(cfg)
                    ):
                        # If the last error was a KeyError, extract the missing
                        # column name and point the LLM at the prior asset that
                        # DOES have that column.
                        _hint = ""
                        _last_err = str(_last_failed.get("error") or "")
                        import re as _re
                        _m = _re.search(r"KeyError:?\s*['\"]?([^'\"\)]+?)['\"]?\s*$", _last_err)
                        if _m:
                            _missing = _m.group(1).strip("'\" ")
                            _has_col = [
                                p.get("asset_name") for p in picks
                                if p.get("status") == "success"
                                and _missing in (p.get("output_columns") or [])
                            ]
                            if _has_col:
                                _hint = (
                                    f" HINT: Missing column {_missing!r}. Prior asset(s) that HAVE "
                                    f"this column: {_has_col}. Set upstream_asset_key to one of these."
                                )
                        picks.append({
                            "iteration": iteration, "done": False, "component_id": cid,
                            "component_type": None, "config": json.dumps(cfg), "reason": reason,
                            "asset_name": cfg.get("asset_name"), "output_columns": [],
                            "status": "failed",
                            "error": (
                                f"EXACT REPEAT of the last failed pick ({cid} with same upstream) — "
                                f"prior error was {_last_err[:200]}. "
                                f"You MUST change either the component_id OR the upstream_asset_key "
                                f"OR the config.{_hint}"
                            ),
                        })
                        continue

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

                # Duplicate-asset-key guard: if the LLM re-uses an asset_name
                # that a prior successful pick already produced, dg.materialize
                # will fail with "Duplicate asset key". Reject early with a
                # clear message so the LLM renames.
                _existing_names = {
                    p.get("asset_name") for p in picks
                    if p.get("status") == "success" and p.get("asset_name")
                }
                if asset_name in _existing_names:
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [],
                        "status": "failed",
                        "error": (
                            f"Duplicate asset_name {asset_name!r}: a prior successful pick "
                            f"already produced that asset. Choose a UNIQUE asset_name "
                            f"(e.g. add a suffix like _v2 / _final / _clean). Prior successful "
                            f"asset_names: {sorted(_existing_names)[:10]}"
                        ),
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

                    # Wire upstream asset references. Matches the standard
                    # naming (`upstream_asset_key`, `left_asset_key`, etc.)
                    # but also opportunistically wires any string field whose
                    # value happens to match a prior_df key (catches things
                    # like `upstream_prior_key`, `baseline_asset`, etc.).
                    _extra_assets = []
                    _wired = set()
                    def _wire_str(_v):
                        if isinstance(_v, str) and _v in prior_dfs and _v not in _wired:
                            _extra_assets.append(_make_source_asset(_v, prior_dfs[_v]))
                            _wired.add(_v)
                    for _k, _v in cfg.items():
                        if _k.endswith("_asset_key") or _k == "asset_key" or _k.endswith("_key"):
                            if isinstance(_v, str):
                                _wire_str(_v)
                            elif isinstance(_v, list):
                                for _vv in _v:
                                    _wire_str(_vv)
                        elif _k.endswith("_asset_keys") or _k == "asset_keys" or _k.endswith("_keys"):
                            if isinstance(_v, list):
                                for _vv in _v:
                                    _wire_str(_vv)
                        elif isinstance(_v, str) and _v in prior_dfs:
                            # Opportunistic: any string value that IS a prior
                            # asset name gets wired. Covers custom naming.
                            _wire_str(_v)

                    _mat_start = _time.time()
                    _mat = dg.materialize(_extra_assets + _assets, raise_on_error=False)
                    _mat_seconds = _time.time() - _mat_start
                    if not _mat.success:
                        # Extract the underlying step exception — the LLM needs a
                        # real error to course-correct, not the DagsterExecutionStepExecutionError
                        # wrapper. Walk `.cause` down to the innermost SerializableErrorInfo.
                        _err_msg = ""
                        for _evt in _mat.all_events:
                            if "STEP_FAILURE" in getattr(_evt, "event_type_value", ""):
                                _esd = getattr(_evt, "event_specific_data", None)
                                _err_info = getattr(_esd, "error", None) if _esd else None
                                # Chase .cause down to the root exception.
                                while _err_info is not None and getattr(_err_info, "cause", None):
                                    _err_info = _err_info.cause
                                if _err_info is not None:
                                    _cls = getattr(_err_info, "cls_name", "") or ""
                                    _msg = getattr(_err_info, "message", "") or ""
                                    _err_msg = f"{_cls}: {_msg}".strip(": ")[:400]
                                if not _err_msg:
                                    _err_msg = str(getattr(_evt, "message", "") or _evt)[:400]
                                break
                        raise RuntimeError(_err_msg or "materialize failed")

                    # Capture ALL outputs. For single-asset components this is
                    # just one value under the LLM's asset_name. For multi-asset
                    # components (e.g. router with N routes, train_test_splitter
                    # with 2 outputs), capture each under its actual asset key.
                    _val = None
                    _cols = []
                    _emitted_names: List[str] = []
                    for _a in _assets:
                        for _k in _a.keys:
                            try:
                                _v = _mat.output_for_node(_k.to_user_string())
                            except Exception:
                                continue
                            _name = _k.to_user_string()
                            if _v is not None and _name:
                                prior_dfs[_name] = _v
                                _emitted_names.append(_name)
                                if _val is None:
                                    _val = _v
                                    _cols = list(_v.columns) if hasattr(_v, "columns") else []
                    # Preserve LLM-picked asset_name mapping too (for
                    # single-asset picks where key doesn't match).
                    if _val is not None and asset_name and asset_name not in prior_dfs:
                        prior_dfs[asset_name] = _val

                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": _cols,
                        "emitted_asset_names": _emitted_names,
                        "status": "success", "error": "",
                    })
                except Exception as e:  # noqa: BLE001
                    picks.append({
                        "iteration": iteration, "done": False, "component_id": cid,
                        "component_type": ctype, "config": json.dumps(cfg), "reason": reason,
                        "asset_name": asset_name, "output_columns": [],
                        "status": "failed", "error": str(e)[:400],
                    })
                    if self.fail_on_execution_error:
                        raise

                # Record iteration timing regardless of pick outcome.
                _total_llm_seconds += _llm_seconds
                _total_mat_seconds += _mat_seconds
                _last_pick = picks[-1] if picks else {}
                _status = _last_pick.get("status", "?")
                _asset_name = _last_pick.get("asset_name") or "?"
                _iter_timings.append({
                    "iteration": iteration,
                    "component_id": cid,
                    "asset_name": _asset_name,
                    "status": _status,
                    "llm_s": round(_llm_seconds, 2),
                    "mat_s": round(_mat_seconds, 2),
                    "total_s": round(_time.time() - _iter_start, 2),
                    "in_tokens": _in_tok,
                    "cached_tokens": _cached_tok,
                    "out_tokens": _out_tok,
                    "cost_usd": _iter_cost,
                })
                _mark = "✓" if _status == "success" else "✗"
                _cache_pct = (
                    f" cache={100*_cached_tok//_in_tok}%" if _in_tok else ""
                )
                print(
                    f"[planned_agent] iter {iteration:2d}/{self.max_iterations} {_mark} "
                    f"{cid or '?':30s} → {_asset_name:34s} "
                    f"llm={_llm_seconds:5.1f}s mat={_mat_seconds:5.1f}s "
                    f"tok={_in_tok+_out_tok:5d}{_cache_pct} ${_iter_cost:.4f}"
                )

            # Attach summary timing to the return value via a wrapper structure
            # so write_state_to_path can persist it alongside `plan`.
            _cache_pct = (100 * _total_cached_tokens // _total_in_tokens) if _total_in_tokens else 0
            self._last_timing_summary = {
                "trajectory_total_s": round(_time.time() - _trajectory_start, 2),
                "llm_total_s": round(_total_llm_seconds, 2),
                "materialize_total_s": round(_total_mat_seconds, 2),
                "total_input_tokens": _total_in_tokens,
                "total_output_tokens": _total_out_tokens,
                "total_cached_tokens": _total_cached_tokens,
                "cache_hit_pct": _cache_pct,
                "total_cost_usd": round(_total_cost, 4),
                "model": self.llm_model,
                "per_iteration": _iter_timings,
                "tpm_budget": self.tpm_budget,
                "catalog_trim_notes": _trim_notes,
                "catalog_final_size": len(catalog),
            }
            print(
                f"[planned_agent] trajectory END: {sum(1 for p in picks if p.get('status')=='success')}/{len(picks)} picks "
                f"in {round(_time.time() - _trajectory_start,1)}s "
                f"(llm={round(_total_llm_seconds,1)}s mat={round(_total_mat_seconds,1)}s) "
                f"tokens={_total_in_tokens+_total_out_tokens} cache={_cache_pct}% "
                f"total_cost=${round(_total_cost, 4)}"
            )
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
