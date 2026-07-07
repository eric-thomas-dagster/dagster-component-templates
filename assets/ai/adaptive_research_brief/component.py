"""Adaptive Research Brief — planner LLM decides N sub-topics at runtime.

Where DynamicOutput fits. The planner LLM reads a topic and decides how
many sub-topics to research — could be 3, could be 12. Each sub-topic
becomes a row that a downstream LLM researches. A final synthesizer
combines the notes into a grounded brief.

Assets emitted (`3` per YAML block):
  1. <plan_asset_name>       — planner's list of N subtopics (variable N)
  2. <notes_asset_name>      — row-wise LLM: one research note per subtopic
  3. <brief_asset_name>      — synthesizer LLM: final markdown brief

The N sub-topics are truly runtime-decided. For per-subtopic UI
visibility (each subtopic as its own asset materialization), pair the
notes asset with a dynamic partitions definition and let the planner
emit partition keys. This component keeps a single-asset iteration
shape for demoability.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class AdaptiveResearchBriefComponent(dg.Component, dg.Model, dg.Resolvable):
    """Planner LLM decides N sub-topics; row-wise researcher writes a note per subtopic; synthesizer combines.

    Example:

        ```yaml
        type: dagster_community_components.AdaptiveResearchBriefComponent
        attributes:
          plan_asset_name: research_plan
          notes_asset_name: subtopic_notes
          brief_asset_name: research_brief
          topic: |
            Prepare a competitive brief on Anthropic. Cover product,
            pricing, safety approach, and recent research directions.
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_subtopics: 8
        ```

    The planner outputs a JSON array — one entry per subtopic with an
    `angle` (short heading) and a `focus` (what the researcher should
    focus on). N is decided by the LLM, capped at `max_subtopics`.
    """

    plan_asset_name: str = Field(description="Planner asset name.")
    notes_asset_name: str = Field(description="Subtopic-notes asset name.")
    brief_asset_name: str = Field(description="Final brief asset name.")
    topic: str = Field(description="The topic to research.")
    model: str = Field(default="gpt-4o-mini")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.3)
    planner_max_tokens: int = Field(default=500)
    researcher_max_tokens: int = Field(default=350)
    brief_max_tokens: int = Field(default=1200)
    max_subtopics: int = Field(
        default=8,
        ge=1,
        le=30,
        description="Upper bound the planner is told about. The LLM picks up to this many at runtime.",
    )
    researcher_system_message: Optional[str] = Field(
        default=None,
        description=(
            "Optional override for the researcher's system prompt. Default is a "
            "generic 'be concise, cite plausible sources' persona."
        ),
    )
    brief_system_message: Optional[str] = Field(
        default=None,
        description="Optional override for the synthesizer's system prompt.",
    )
    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")

    # Standard fields — partitions / freshness / retries.
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic'",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitions.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "agent", "research"})

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
                raise ImportError("adaptive_research_brief requires openai>=1.0.0") from e
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    kwargs["base_url"] = base_url
            return OpenAI(**kwargs)

        # ── Planner: variable N sub-topics ─────────────────────────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.plan_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"planner"},
            description=f"Adaptive research plan: {_self.topic[:80]}",
            **std_kwargs,
        )
        def _plan_asset(context: dg.AssetExecutionContext):
            import json
            import pandas as pd

            # Template substitution for {partition_key} / {run_id}.
            _topic = _self.topic
            if isinstance(_topic, str) and "{" in _topic:
                _rid = getattr(context, "run_id", "") or ""
                _topic = _topic.replace("{run_id}", str(_rid))
                _has_pk = False
                try: _has_pk = context.has_partition_key
                except Exception: pass
                if _has_pk:
                    try: _pk = context.partition_key
                    except Exception: _pk = ""
                    if hasattr(_pk, "keys_by_dimension"):
                        _topic = _topic.replace("{partition_key}", str(_pk))
                        for dim, val in _pk.keys_by_dimension.items():
                            _topic = _topic.replace("{partition_keys." + dim + "}", str(val))
                    else:
                        _topic = _topic.replace("{partition_key}", str(_pk or ""))
                else:
                    _topic = _topic.replace("{partition_key}", "")

            client = _client()
            prompt = (
                f"You are a research planner. Given a topic, decide how many "
                f"sub-topics to research (1 to {_self.max_subtopics}) and what "
                f"each should focus on. Pick the N that best covers the topic — "
                f"more is not always better.\n\n"
                f"Topic: {_topic}\n\n"
                f"Output ONLY a JSON array (no markdown fences). Each element:\n"
                f"  {{\"angle\": \"<short 3-5 word heading>\","
                f" \"focus\": \"<one sentence describing what to research>\"}}"
            )
            context.log.info(f"[planner] planning sub-topics for: {_topic[:80]}")
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are a research planner. Output JSON array only."},
                    {"role": "user", "content": prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                items = json.loads(raw)
                if not isinstance(items, list):
                    items = [items]
            except json.JSONDecodeError as e:
                context.log.warning(f"[planner] JSON parse failed: {e}; raw={raw[:200]}")
                items = []

            rows = []
            for i, it in enumerate(items[: _self.max_subtopics]):
                if not isinstance(it, dict):
                    continue
                rows.append({
                    "subtopic_id": i + 1,
                    "angle": str(it.get("angle", f"subtopic {i+1}")),
                    "focus": str(it.get("focus", "")),
                })
            df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["subtopic_id", "angle", "focus"])
            context.log.info(f"[planner] emitted {len(df)} subtopic(s)")
            context.add_output_metadata({
                "topic": dg.MetadataValue.text(_topic),
                "n_subtopics": dg.MetadataValue.int(len(df)),
                "plan": dg.MetadataValue.md(df.to_markdown(index=False) if not df.empty else "_no subtopics_"),
            })
            return df

        # ── Researcher: row-wise LLM per subtopic ──────────────────────
        _res_sys = _self.researcher_system_message or (
            "You are a research assistant. Given a subtopic (angle + focus) "
            "and the overall research topic, write a concise 3-5 sentence note "
            "with plausible fact-shaped observations. Cite plausible source "
            "names in parentheses (e.g., 'Anthropic blog', 'TechCrunch 2026') "
            "even if fabricated — this is a demo. Do NOT include a heading; "
            "just the note text."
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.notes_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"researcher"},
            description="One research note per planner-emitted subtopic.",
            ins={"plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name))},
            **std_kwargs,
        )
        def _notes_asset(context: dg.AssetExecutionContext, plan):
            import pandas as pd

            # Template substitution for {partition_key} / {run_id}.
            _topic = _self.topic
            if isinstance(_topic, str) and "{" in _topic:
                _rid = getattr(context, "run_id", "") or ""
                _topic = _topic.replace("{run_id}", str(_rid))
                _has_pk = False
                try: _has_pk = context.has_partition_key
                except Exception: pass
                if _has_pk:
                    try: _pk = context.partition_key
                    except Exception: _pk = ""
                    if hasattr(_pk, "keys_by_dimension"):
                        _topic = _topic.replace("{partition_key}", str(_pk))
                        for dim, val in _pk.keys_by_dimension.items():
                            _topic = _topic.replace("{partition_keys." + dim + "}", str(val))
                    else:
                        _topic = _topic.replace("{partition_key}", str(_pk or ""))
                else:
                    _topic = _topic.replace("{partition_key}", "")

            plan_df = plan if isinstance(plan, pd.DataFrame) else pd.DataFrame(plan)
            if plan_df.empty:
                context.log.warning("[researcher] no subtopics — planner emitted empty plan")
                context.add_output_metadata({"n_notes": dg.MetadataValue.int(0)})
                return pd.DataFrame(columns=["subtopic_id", "angle", "focus", "note"])

            client = _client()
            notes = []
            for _, row in plan_df.iterrows():
                context.log.info(f"[researcher] researching #{row['subtopic_id']}: {row['angle']}")
                user_msg = (
                    f"Overall research topic:\n{_topic}\n\n"
                    f"Subtopic angle: {row['angle']}\n"
                    f"Focus: {row['focus']}\n\n"
                    "Write the research note now (3-5 sentences)."
                )
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.researcher_max_tokens,
                    messages=[
                        {"role": "system", "content": _res_sys},
                        {"role": "user", "content": user_msg},
                    ],
                )
                notes.append({
                    "subtopic_id": row["subtopic_id"],
                    "angle": row["angle"],
                    "focus": row["focus"],
                    "note": resp.choices[0].message.content or "",
                })
            df = pd.DataFrame(notes)
            context.add_output_metadata({
                "n_notes": dg.MetadataValue.int(len(df)),
                "preview": dg.MetadataValue.md(df[["angle", "note"]].head(3).to_markdown(index=False)),
            })
            return df

        # ── Synthesizer: reduce all notes into one brief ────────────────
        _brief_sys = _self.brief_system_message or (
            "You are a research writer. Given a topic and a set of subtopic "
            "notes, write a well-structured markdown brief. Use headings for "
            "each subtopic, keep prose tight, preserve the source citations "
            "the researcher added. End with a 1-paragraph executive summary."
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.brief_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"synthesizer"},
            description="Final markdown research brief.",
            ins={"notes": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.notes_asset_name))},
            **std_kwargs,
        )
        def _brief_asset(context: dg.AssetExecutionContext, notes):
            import pandas as pd

            # Template substitution for {partition_key} / {run_id}.
            _topic = _self.topic
            if isinstance(_topic, str) and "{" in _topic:
                _rid = getattr(context, "run_id", "") or ""
                _topic = _topic.replace("{run_id}", str(_rid))
                _has_pk = False
                try: _has_pk = context.has_partition_key
                except Exception: pass
                if _has_pk:
                    try: _pk = context.partition_key
                    except Exception: _pk = ""
                    if hasattr(_pk, "keys_by_dimension"):
                        _topic = _topic.replace("{partition_key}", str(_pk))
                        for dim, val in _pk.keys_by_dimension.items():
                            _topic = _topic.replace("{partition_keys." + dim + "}", str(val))
                    else:
                        _topic = _topic.replace("{partition_key}", str(_pk or ""))
                else:
                    _topic = _topic.replace("{partition_key}", "")

            notes_df = notes if isinstance(notes, pd.DataFrame) else pd.DataFrame(notes)
            if notes_df.empty:
                brief = "(no research notes — planner produced no subtopics)"
            else:
                client = _client()
                bundle = "\n\n".join(
                    f"### {row['angle']}\n{row['focus']}\n\n{row['note']}"
                    for _, row in notes_df.iterrows()
                )
                user_msg = (
                    f"Topic:\n{_topic}\n\n"
                    f"Sub-topic notes ({len(notes_df)} total):\n\n{bundle}\n\n"
                    "Write the brief now."
                )
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.brief_max_tokens,
                    messages=[
                        {"role": "system", "content": _brief_sys},
                        {"role": "user", "content": user_msg},
                    ],
                )
                brief = resp.choices[0].message.content or ""

            df = pd.DataFrame([{
                "topic": _topic,
                "n_subtopics": len(notes_df),
                "brief": brief,
            }])
            context.add_output_metadata({
                "topic": dg.MetadataValue.text(_topic),
                "n_subtopics": dg.MetadataValue.int(len(notes_df)),
                "brief": dg.MetadataValue.md(brief),
            })
            return df

        return dg.Definitions(assets=[_plan_asset, _notes_asset, _brief_asset])
