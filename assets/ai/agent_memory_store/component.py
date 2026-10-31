"""AgentMemoryStoreComponent -- form long-term agent memory from conversation
turns using Mem0 (`pip install mem0ai`), distinct from simple chat-history
storage.

## Why this exists

This registry's existing `conversation_memory` component is a sliding-window
chat-turn JSON file -- it appends/trims raw messages, with no extraction,
consolidation, semantic search, or per-user scoping. "Agent memory" as an
architectural pattern (Mem0 being the most-cited dedicated implementation) is
a genuinely different thing: an LLM extracts durable facts from a
conversation and stores them in a vector store for later semantic recall,
consolidating/updating what's already known about a user rather than keeping
a raw transcript. This component wraps Mem0's real, confirmed self-hosted
API (`Memory.from_config({"vector_store":..., "llm":..., "embedder":...})`,
`add()`, `search()`) to form memories from a DataFrame of conversation turns,
grouped per user/session.

## Scope

Deliberately limited to `add()` (write) and an optional `search()` smoke
check -- Mem0's `get_all()`/`update()`/`delete()` signatures were not
confirmed against primary docs for the self-hosted OSS class at the time
this was written, and this repo's convention is to not guess API shapes.
`agent_id`/`run_id`/`metadata`/`infer` on `add()` are confirmed on Mem0's
*cloud* API reference and referenced in the OSS README, but not spelled out
for the OSS `Memory` class's own docs page -- they're only sent when the
corresponding column is actually configured, so a mismatch with an installed
mem0ai version surfaces as a clear TypeError rather than a silent no-op.

## Relationship to this repo's receipt-store concept

`resources/context_receipt_store` is a separate, SQLite-backed,
schema-versioned audit trail of retrieval/output/evidence/outcome events --
retrieval-provenance-shaped (what was fetched, from where, did it work out),
not semantic-fact-shaped (what should be remembered about this user). They
are complementary, not overlapping. Logging a memory write as a new receipt
event type is a plausible future idea but is a deliberate schema-version
decision for whoever owns `context_receipt_store` -- not implemented here.

## Demo mode

`demo_mode: true` (the default) never calls Mem0 or any LLM/embedder
provider. `_build_memory_client` is swapped for an internal
`_DemoMemoryClient` that persists to a local JSON file and returns
real-shaped `add()`/`search()` responses, so a demo pipeline round-trips
without any API keys or paid calls.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


def _build_memory_client(config: Optional[Dict[str, Any]]) -> Any:
    """Isolated client construction -- tests monkeypatch
    `agent_memory_store_component._build_memory_client` to return a fake
    client instead of calling the real (paid) Mem0/LLM/embedder APIs."""
    from mem0 import Memory

    if config:
        return Memory.from_config(config)
    return Memory()


class _DemoMemoryClient:
    """Deterministic, zero-cost stand-in for a real Mem0 `Memory` instance.
    Persists to a local JSON file so writes are retrievable within a demo
    run (and across a writer run followed by a separate query, mirroring
    this session's other demo-mode components)."""

    def __init__(self, path: str):
        self._path = path

    def _load(self) -> List[Dict[str, Any]]:
        if os.path.exists(self._path):
            with open(self._path) as f:
                return json.load(f)
        return []

    def _save(self, memories: List[Dict[str, Any]]) -> None:
        with open(self._path, "w") as f:
            json.dump(memories, f)

    def add(self, messages, user_id=None, agent_id=None, run_id=None, metadata=None, infer=True) -> Dict[str, Any]:
        memories = self._load()
        last_user_msg = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), None)
        text = last_user_msg if infer and last_user_msg else json.dumps(messages)
        entry = {
            "id": f"mem_{len(memories)}",
            "memory": text[:200],
            "user_id": user_id,
            "agent_id": agent_id,
            "run_id": run_id,
            "metadata": metadata or {},
            "categories": [],
            "created_at": "demo",
            "score": 1.0,
        }
        memories.append(entry)
        self._save(memories)
        return {"results": [entry]}

    def search(self, query: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        memories = self._load()
        filters = filters or {}
        matches = [m for m in memories if all(m.get(k) == v for k, v in filters.items())]
        return {"results": matches}


class AgentMemoryStoreComponent(dg.Component, dg.Model, dg.Resolvable):
    """Form long-term agent memory from a DataFrame of conversation turns via
    Mem0, grouped per user/session.

    Example:
        ```yaml
        type: dagster_community_components.AgentMemoryStoreComponent
        attributes:
          asset_name: customer_agent_memory
          upstream_asset_key: support_chat_turns
          user_id_column: customer_id
          role_column: role
          content_column: content
          validate_search: true
          validate_search_query: "What does this customer care about?"
          demo_mode: true
        ```
    """

    asset_name: str = Field(description="Dagster asset key emitted by this component.")
    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame of conversation turns -- one row per message."
    )
    user_id_column: str = Field(default="user_id", description="Column identifying who the conversation is with. Rows are grouped by this (and agent_id_column/run_id_column, if set) before calling Mem0's add() once per group.")
    agent_id_column: Optional[str] = Field(default=None, description="Column identifying which agent participated, if memories should be scoped per-agent as well as per-user.")
    run_id_column: Optional[str] = Field(default=None, description="Column identifying the conversation/session, if memories should be scoped per-run rather than accumulated across a user's entire history.")
    role_column: str = Field(default="role", description="Column holding each turn's role ('user'/'assistant'/etc).")
    content_column: str = Field(default="content", description="Column holding each turn's message text.")
    metadata_column: Optional[str] = Field(default=None, description="Optional column holding a JSON object (or JSON string) of extra metadata to attach -- the first non-null value in each group is used.")
    infer: bool = Field(default=True, description="Passed to Mem0's add(infer=...). true (default) runs fact extraction over the messages; false stores the raw messages verbatim with no extraction LLM call.")

    vector_store: Optional[Dict[str, Any]] = Field(default=None, description="Mem0 vector_store config, e.g. {provider: qdrant, config: {host: localhost, port: 6333}}. Unset = Mem0's own on-disk default.")
    llm: Optional[Dict[str, Any]] = Field(default=None, description="Mem0 llm config (used for fact extraction when infer=true), e.g. {provider: openai, config: {model: gpt-5-mini}}. Unset = Mem0's own default -- requires OPENAI_API_KEY.")
    embedder: Optional[Dict[str, Any]] = Field(default=None, description="Mem0 embedder config, e.g. {provider: openai, config: {model: text-embedding-3-small}}. Unset = Mem0's own default.")

    validate_search: bool = Field(default=False, description="After writing, run one Mem0 search() as a smoke check -- catches a silently-broken vector-store/embedder config. Emits a non-blocking (WARN) asset check.")
    validate_search_query: Optional[str] = Field(default=None, description="Query text for the validate_search smoke check. Required when validate_search=true.")

    demo_mode: bool = Field(
        default=True,
        description="Form memories against a local JSON-file-backed fake instead of a real Mem0/LLM/embedder stack. Set false for a real configuration.",
    )
    demo_store_path: Optional[str] = Field(default=None, description="Path to the local JSON file demo_mode reads/writes. Unset = './{asset_name}_agent_memory_demo.json'.")

    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['mem0', 'memory', 'llm'].")

    partition_type: Optional[str] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None)
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None)
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @model_validator(mode="after")
    def _validate_search_config(self):
        if self.validate_search and not self.validate_search_query:
            raise ValueError("AgentMemoryStoreComponent: validate_search_query is required when validate_search=true.")
        return self

    def _get_client(self):
        if self.demo_mode:
            path = self.demo_store_path or f"./{self.asset_name}_agent_memory_demo.json"
            return _DemoMemoryClient(path)
        config: Dict[str, Any] = {}
        if self.vector_store:
            config["vector_store"] = self.vector_store
        if self.llm:
            config["llm"] = self.llm
        if self.embedder:
            config["embedder"] = self.embedder
        return _build_memory_client(config or None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Agent Memory Store", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        kinds_set = set(self.kinds or []) | {"mem0", "memory", "llm"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        check_specs = []
        if self.validate_search:
            check_specs.append(dg.AssetCheckSpec(name="memory_search_smoke_check", asset=asset_key, blocking=False))

        @dg.asset(
            key=asset_key,
            description=self.description or "Long-term agent memory formed via Mem0",
            group_name=self.group_name, owners=self.owners or [], tags=tag_map, kinds=kinds_set,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            check_specs=check_specs,
            ins={"upstream": dg.AssetIn(key=upstream_key)},
        )
        def _agent_memory_store_asset(context: dg.AssetExecutionContext, upstream: Any):
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            df = upstream

            client = component._get_client()
            group_cols = [component.user_id_column]
            if component.agent_id_column:
                group_cols.append(component.agent_id_column)
            if component.run_id_column:
                group_cols.append(component.run_id_column)

            groups_written = 0
            memories_written = 0
            last_user_id = None
            for _, group in df.groupby(group_cols, sort=False):
                messages = [
                    {"role": row[component.role_column], "content": row[component.content_column]}
                    for _, row in group.iterrows()
                ]
                first_row = group.iloc[0]
                user_id = first_row[component.user_id_column]
                last_user_id = user_id
                kwargs: Dict[str, Any] = {"user_id": user_id, "infer": component.infer}
                if component.agent_id_column:
                    kwargs["agent_id"] = first_row[component.agent_id_column]
                if component.run_id_column:
                    kwargs["run_id"] = first_row[component.run_id_column]
                if component.metadata_column and component.metadata_column in group.columns:
                    raw_meta = next((m for m in group[component.metadata_column] if m is not None and not (isinstance(m, float))), None)
                    if raw_meta is not None:
                        kwargs["metadata"] = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta

                response = client.add(messages, **kwargs)
                groups_written += 1
                # Defensive: Mem0's exact add() return shape isn't fully pinned down for the
                # self-hosted class, so this only counts entries best-effort rather than
                # depending on any specific nested field.
                if isinstance(response, dict):
                    memories_written += len(response.get("results", []) or [])
                elif isinstance(response, list):
                    memories_written += len(response)

            metadata = {
                "groups_written": dg.MetadataValue.int(groups_written),
                "memories_written": dg.MetadataValue.int(memories_written),
            }
            yield dg.Output(None, metadata=metadata)

            if component.validate_search:
                search_response = client.search(component.validate_search_query, filters={"user_id": last_user_id} if last_user_id is not None else None)
                results = search_response.get("results", []) if isinstance(search_response, dict) else (search_response or [])
                yield dg.AssetCheckResult(
                    check_name="memory_search_smoke_check", passed=len(results) > 0,
                    severity=dg.AssetCheckSeverity.WARN,
                    metadata={"query": component.validate_search_query, "results_returned": len(results)},
                )

        return dg.Definitions(assets=[_agent_memory_store_asset])


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
