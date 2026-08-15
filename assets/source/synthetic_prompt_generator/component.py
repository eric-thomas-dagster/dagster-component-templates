"""SyntheticPromptGeneratorComponent — emit prompt text strings for LLM demos.

Sibling of `synthetic_image_generator`, `synthetic_audio_generator`,
`synthetic_pdf_generator`, `synthetic_video_generator` — completes the
"synthetic <modality> generator" family with a prompts primitive that
emits plain text.

── Mode selection ─────────────────────────────────────────────────────

Set exactly ONE of these shapes:

  A. LITERAL       — `prompts: {key: text}`
                     Each key = a static partition.

  B. TEMPLATE      — `topics: [...]` + `template: "..."`
                     Each topic = a static partition; template rendered.

  C. FIXED         — `prompt: "..."`
                     Unpartitioned single string.

  D. COMPOSED      — `topics: [...]` + any of the v1.5 levers
                     (persona/style/length/task_type/format_hint/depth) +
                     optional `count_per_topic:` for N variants per topic.
                     Deterministic template composition — no LLM, no
                     network. Levers give ~150 variants per topic (5^6)
                     depending on which are set.

  E. LLM           — `topics: [...]` + `paraphrase_model: "..."` +
                     optional levers (interpreted as natural-language
                     hints, not enum lookups).
                     LiteLLM generates paraphrased variants at
                     materialization time. Optional
                     `include_wrong_variants:` sprinkles adversarial /
                     under-specified prompts for eval + robustness.

Modes D and E use partition-key convention `{topic}` (when
count_per_topic=1) or `{topic}__v{n}` (when >1). Every partition
independently materializes → one str per partition, ready for
`AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}`.

── Design of the systematic composer (Mode D) ─────────────────────────

Composition happens in a fixed order:
  1) OPENER — persona-based ("As a student trying to learn…"), or
     task-type based ("Compare and contrast…"), or bare ("Explain X.")
  2) STYLE hint    — precision / register (e.g. "Use precise technical
     vocabulary.")
  3) LENGTH hint   — target word count
  4) FORMAT hint   — bullets / paragraphs / table / code
  5) DEPTH hint    — assumed background

Each hint is picked deterministically (seeded per (topic, variant_idx))
from a small set of templates. Variants within a topic differ because
the seed changes; across topics the same lever produces predictable
sibling structure.

── Design of the LLM elevation (Mode E) ───────────────────────────────

Same lever surface, but the LLM interprets each lever as a
natural-language hint and paraphrases. Meta-prompt structure:

  "Generate one clear, natural-sounding prompt on: <topic>.
   The prompt should sound like it comes from a <persona>.
   Style: <style>. Length: <length>. Frame it as a <task_type>.
   Ask for the answer in <format_hint>. Assume the audience is
   <depth>-level. This is variant N. Make it distinct from others.
   Return ONLY the prompt text, no preamble."

`include_wrong_variants: true` peppers in adversarial variants (short,
ambiguous, jargon-heavy) — useful when the downstream is a robustness
eval or a defensive-prompting test.
"""
import os
from typing import Any, Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    StaticPartitionsDefinition,
    asset,
)
from pydantic import Field


# ── Systematic composition templates (Mode D) ─────────────────────────

_PERSONA_OPENERS: Dict[str, List[str]] = {
    "student": [
        "As a student trying to learn, help me understand {topic}.",
        "I'm a student new to this — walk me through {topic}.",
        "Help me build intuition for {topic}, from the basics up.",
    ],
    "engineer": [
        "As a senior engineer, give a rigorous treatment of {topic}.",
        "Cover {topic} for someone who ships production systems.",
        "What are the load-bearing details of {topic} in practice?",
    ],
    "executive": [
        "For a busy executive, briefly explain {topic} and why it matters.",
        "Give me the executive summary of {topic}.",
        "In business terms, what is {topic} and where does it apply?",
    ],
    "novice": [
        "In plain language, explain {topic} to someone with no background.",
        "I know nothing about this — explain {topic} simply.",
        "Give me a beginner-friendly explanation of {topic}.",
    ],
    "expert": [
        "For an expert audience, cover {topic} in depth. Skip introductions.",
        "Give an expert-level treatment of {topic}, including edge cases.",
        "Discuss {topic} at the level of a domain specialist.",
    ],
}

_TASK_VERBS: Dict[str, List[str]] = {
    "question": [
        "How does {topic} work?",
        "What is {topic} and why does it matter?",
        "Can you explain what {topic} is?",
    ],
    "instruction": [
        "Explain {topic}.",
        "Describe {topic}.",
        "Walk through {topic}.",
    ],
    "comparison": [
        "Compare and contrast the leading approaches to {topic}.",
        "What are the trade-offs between the main variants of {topic}?",
        "How does {topic} compare against its alternatives?",
    ],
    "analysis": [
        "Analyze the trade-offs in {topic}.",
        "What are the strengths and weaknesses of {topic}?",
        "Give a critical analysis of {topic}.",
    ],
    "explanation": [
        "Give a thorough explanation of {topic}.",
        "Explain the underlying mechanics of {topic}.",
        "Explain how {topic} works and why it works that way.",
    ],
    "debate": [
        "Debate the strengths and weaknesses of {topic}.",
        "Argue both sides of {topic}.",
        "Present a debate on {topic}, then pick a side.",
    ],
}

_STYLE_HINTS: Dict[str, List[str]] = {
    "formal": ["Use precise, academic language."],
    "casual": ["Use conversational, friendly language."],
    "technical": [
        "Use precise technical vocabulary and include equations where helpful.",
        "Include the actual technical detail — variable names, complexity, edge cases.",
    ],
    "journalistic": [
        "Structure it like a news article with a lede and supporting details.",
    ],
}

_LENGTH_HINTS: Dict[str, List[str]] = {
    "short": ["Keep it under 100 words.", "Be concise — 100 words max."],
    "medium": ["Aim for ~250 words.", "Around 250 words is right."],
    "long": [
        "Aim for ~500 words with sub-sections.",
        "Go long — around 500 words, structured with headings.",
    ],
}

_FORMAT_HINTS: Dict[str, List[str]] = {
    "bullets": ["Use bullet points.", "Present the answer as a bulleted list."],
    "paragraphs": ["Use flowing paragraphs.", "Write it as prose."],
    "table": ["Present the answer as a table where applicable."],
    "code": ["Include code examples.", "Use code blocks for illustration."],
}

_DEPTH_HINTS: Dict[str, List[str]] = {
    "intro": ["Assume the reader has no prior background."],
    "intermediate": ["Assume the reader has basic familiarity with the field."],
    "advanced": ["Assume the reader is an expert; skip introductions."],
}

_VALID_PERSONAS = set(_PERSONA_OPENERS)
_VALID_STYLES = set(_STYLE_HINTS)
_VALID_LENGTHS = set(_LENGTH_HINTS)
_VALID_TASK_TYPES = set(_TASK_VERBS)
_VALID_FORMAT_HINTS = set(_FORMAT_HINTS)
_VALID_DEPTHS = set(_DEPTH_HINTS)


def _pick(rng, options: List[str]) -> str:
    return options[rng.randrange(len(options))]


def _compose_systematic(
    topic: str,
    variant_idx: int,
    persona: Optional[str],
    style: Optional[str],
    length: Optional[str],
    task_type: Optional[str],
    format_hint: Optional[str],
    depth: Optional[str],
    seed: int,
) -> str:
    """Mode D: build a prompt deterministically from lever configs.

    Seeded per (topic, variant_idx) so re-materializing a partition
    produces the same prompt bit-for-bit — matters for CI + auditing.
    """
    import random

    seed_val = seed + (hash(topic) & 0xFFFFFFFF) + variant_idx * 997
    rng = random.Random(seed_val)

    parts: List[str] = []

    # 1) Opener — persona OR task_type, or bare fallback
    if persona:
        opener = _pick(rng, _PERSONA_OPENERS[persona])
    elif task_type:
        opener = _pick(rng, _TASK_VERBS[task_type])
    else:
        opener = "Explain {topic}."
    parts.append(opener.replace("{topic}", topic))

    # 2-5) Optional hints in fixed order
    for lever, table in (
        (style, _STYLE_HINTS),
        (length, _LENGTH_HINTS),
        (format_hint, _FORMAT_HINTS),
        (depth, _DEPTH_HINTS),
    ):
        if lever:
            parts.append(_pick(rng, table[lever]))

    return " ".join(parts)


def _compose_llm(
    topic: str,
    variant_idx: int,
    count_per_topic: int,
    paraphrase_model: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    system_prompt: Optional[str],
    temperature: float,
    max_tokens: int,
    persona: Optional[str],
    style: Optional[str],
    length: Optional[str],
    task_type: Optional[str],
    format_hint: Optional[str],
    depth: Optional[str],
    include_wrong_variants: bool,
    seed: int,
) -> str:
    """Mode E: ask LiteLLM to generate one paraphrased prompt variant.

    Same lever surface as Mode D, but the LLM interprets each as a
    natural-language hint instead of picking from an enum table.

    include_wrong_variants=True peppers adversarial / under-specified
    prompts in the mix (approximately every 3rd variant) — useful when
    the downstream is a robustness eval.
    """
    try:
        import litellm  # type: ignore
    except ImportError:
        raise ImportError(
            "Mode E (paraphrase_model:) requires litellm. "
            "Install with: pip install litellm"
        )

    # Deterministic-ish adversarial slot every 3rd variant when opted in.
    is_wrong_variant = include_wrong_variants and variant_idx > 0 and variant_idx % 3 == 0

    hints: List[str] = [f"Topic: {topic}."]
    if persona:
        hints.append(f"Sound like a {persona}.")
    if style:
        hints.append(f"Style: {style}.")
    if length:
        hints.append(f"The requested answer should be {length}.")
    if task_type:
        hints.append(f"Frame it as a {task_type}.")
    if format_hint:
        hints.append(f"Ask for the answer in {format_hint} format.")
    if depth:
        hints.append(f"Assume {depth}-level audience.")

    if is_wrong_variant:
        hints.append(
            "Make it deliberately ambiguous, under-specified, or over-jargoned — "
            "a bad prompt useful for testing robustness."
        )

    hints.append(f"This is variant {variant_idx + 1} of {count_per_topic}. Make it distinct.")
    hints.append("Return ONLY the prompt text, no preamble, no quotes.")

    meta_prompt = " ".join(hints)

    completion_kwargs: Dict[str, Any] = {
        "model": paraphrase_model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt or (
                    "You are a prompt-writing assistant. Emit ONE prompt per response."
                ),
            },
            {"role": "user", "content": meta_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        # Best-effort determinism — some providers honor seed, others ignore it
        "seed": seed + variant_idx,
    }
    if api_key_env_var:
        api_key = os.environ.get(api_key_env_var, None)
        if api_key:
            completion_kwargs["api_key"] = api_key
    if api_base_env_var:
        api_base = os.environ.get(api_base_env_var, None)
        if api_base:
            completion_kwargs["api_base"] = api_base

    resp = litellm.completion(**completion_kwargs)
    text = resp.choices[0].message.content.strip()
    # Strip surrounding quotes the LLM sometimes adds
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1]
    return text


class SyntheticPromptGeneratorComponent(Component, Model, Resolvable):
    """Emit deterministic or LLM-elevated prompt text strings for LLM /
    agent demos.

    Five modes total (v1 → v2). See the module docstring for the mode-
    selection tree and lever taxonomy.

    Emit contract: `str` per materialization — snaps into
    `AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}`
    with no glue asset required.
    """

    asset_name: str = Field(description="Dagster asset name.")

    # ── Mode A: literal per-key mapping ────────────────────────────────
    prompts: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Mode A: literal per-key mapping. Keys become static partitions; "
            "the mapped string is the partition's output."
        ),
    )

    # ── Mode B: template ──────────────────────────────────────────────
    template: Optional[str] = Field(
        default=None,
        description=(
            "Mode B: template rendered per topic. Substitutes `{topic}` and "
            "`{partition_key}` with the topic value. Requires `topics`."
        ),
    )

    # ── Mode C: fixed single ──────────────────────────────────────────
    prompt: Optional[str] = Field(
        default=None,
        description="Mode C: unpartitioned single-string prompt.",
    )

    # ── Shared: topics list (used by modes B / D / E) ─────────────────
    topics: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of topics. In mode B, template is rendered per topic. In mode "
            "D (composed) and mode E (LLM), each topic is expanded into "
            "`count_per_topic` variants. Partition keys are `{topic}` when "
            "count=1, `{topic}__v{n}` when >1."
        ),
    )

    # ── v1.5 systematic levers (Mode D) ───────────────────────────────
    persona: Optional[str] = Field(
        default=None,
        description="student | engineer | executive | novice | expert",
    )
    style: Optional[str] = Field(
        default=None,
        description="formal | casual | technical | journalistic",
    )
    length: Optional[str] = Field(
        default=None, description="short | medium | long",
    )
    task_type: Optional[str] = Field(
        default=None,
        description="question | instruction | comparison | analysis | explanation | debate",
    )
    format_hint: Optional[str] = Field(
        default=None, description="bullets | paragraphs | table | code",
    )
    depth: Optional[str] = Field(
        default=None, description="intro | intermediate | advanced",
    )
    count_per_topic: int = Field(
        default=1,
        description=(
            "Modes D + E: number of prompt variants per topic. Partition keys "
            "are `{topic}` when 1, `{topic}__v{n}` (0-indexed) when >1."
        ),
    )
    seed: int = Field(
        default=42,
        description="Reproducibility seed for mode D (composed picking) and mode E (LLM seed).",
    )

    # ── v2 LLM elevation (Mode E) ─────────────────────────────────────
    paraphrase_model: Optional[str] = Field(
        default=None,
        description=(
            "Mode E: LiteLLM model string (e.g. 'gpt-4o-mini', "
            "'claude-haiku-4-5-20251001'). Presence triggers LLM mode — the "
            "same v1.5 levers become natural-language hints instead of enum "
            "lookups."
        ),
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Mode E: env var holding the LLM provider's API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Mode E: env var holding a custom API base URL (self-hosted / proxies).",
    )
    system_prompt: Optional[str] = Field(
        default=None,
        description="Mode E: override the default paraphraser system prompt.",
    )
    temperature: float = Field(default=0.7)
    max_tokens: int = Field(default=300)
    include_wrong_variants: bool = Field(
        default=False,
        description=(
            "Mode E: pepper adversarial / under-specified variants (every 3rd) "
            "into the output — useful for downstream robustness / eval flows."
        ),
    )

    # ── Standard catalog metadata ────────────────────────────────────
    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="prompts")
    kinds: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def _validate_mode(self) -> str:
        """Return the resolved mode, or raise for ambiguous / empty configs."""
        modes: List[str] = []

        if self.prompts:
            modes.append("prompts")

        if self.topics:
            # Which sub-mode within topics-based configuration?
            if self.template:
                modes.append("template")
            elif self.paraphrase_model:
                modes.append("llm")
            elif any(
                [self.persona, self.style, self.length, self.task_type, self.format_hint, self.depth]
            ):
                modes.append("composed")
            else:
                raise ValueError(
                    "`topics:` requires one of: `template:` (mode B), "
                    "`paraphrase_model:` (mode E), or at least one lever "
                    "(`persona:` / `style:` / `length:` / `task_type:` / "
                    "`format_hint:` / `depth:`) for mode D."
                )

        if self.prompt is not None:
            modes.append("prompt")

        if not modes:
            raise ValueError(
                "Must set exactly one of: `prompts:`, `prompt:`, or `topics:` "
                "(with `template:` / `paraphrase_model:` / any composed lever)."
            )
        if len(modes) > 1:
            raise ValueError(
                f"Multiple modes set ({modes}); pick exactly one."
            )

        # Validate lever enum values
        for name, value, valid in (
            ("persona", self.persona, _VALID_PERSONAS),
            ("style", self.style, _VALID_STYLES),
            ("length", self.length, _VALID_LENGTHS),
            ("task_type", self.task_type, _VALID_TASK_TYPES),
            ("format_hint", self.format_hint, _VALID_FORMAT_HINTS),
            ("depth", self.depth, _VALID_DEPTHS),
        ):
            if value is not None and value not in valid:
                raise ValueError(
                    f"Invalid `{name}: {value!r}`. Valid: {sorted(valid)}"
                )
        if self.count_per_topic < 1:
            raise ValueError("count_per_topic must be >= 1")

        return modes[0]

    def _partition_keys_for_topics_mode(self) -> List[str]:
        """Modes D + E build partition keys as topic__v{n} when count > 1."""
        assert self.topics is not None
        if self.count_per_topic == 1:
            return list(self.topics)
        return [f"{t}__v{i}" for t in self.topics for i in range(self.count_per_topic)]

    def _split_partition_key(self, partition_key: str) -> "tuple[str, int]":
        """Undo topic__v{n} → (topic, variant_idx). Returns (topic, 0) for
        count_per_topic==1."""
        if "__v" in partition_key and self.count_per_topic > 1:
            topic, _, v = partition_key.rpartition("__v")
            try:
                return topic, int(v)
            except ValueError:
                return partition_key, 0
        return partition_key, 0

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        mode = self._validate_mode()

        partitions_def = None
        prompts_dict: Optional[Dict[str, str]] = None
        template_str: Optional[str] = None
        single_prompt: Optional[str] = None

        if mode == "prompts":
            assert self.prompts is not None
            partitions_def = StaticPartitionsDefinition(list(self.prompts.keys()))
            prompts_dict = dict(self.prompts)
        elif mode == "template":
            assert self.topics is not None
            partitions_def = StaticPartitionsDefinition(list(self.topics))
            template_str = self.template
        elif mode == "prompt":
            single_prompt = self.prompt
        elif mode in ("composed", "llm"):
            partitions_def = StaticPartitionsDefinition(self._partition_keys_for_topics_mode())
        else:  # unreachable per _validate_mode
            raise AssertionError(f"unknown mode {mode!r}")

        # Snapshot the lever values so closures don't reference `self` at
        # materialization time (Dagster serializes tasks).
        persona = self.persona
        style = self.style
        length = self.length
        task_type = self.task_type
        format_hint = self.format_hint
        depth = self.depth
        seed = self.seed
        count_per_topic = self.count_per_topic
        paraphrase_model = self.paraphrase_model
        api_key_env_var = self.api_key_env_var
        api_base_env_var = self.api_base_env_var
        system_prompt = self.system_prompt
        temperature = self.temperature
        max_tokens = self.max_tokens
        include_wrong_variants = self.include_wrong_variants

        asset_name = self.asset_name
        description = (
            self.description
            or f"Synthetic prompt asset (mode={mode}) — one string per materialization."
        )
        kinds_list = self.kinds or ["prompt", "synthetic"]

        def _resolve(partition_key: Optional[str]) -> str:
            if mode == "prompts":
                if partition_key is None:
                    raise ValueError(
                        f"{asset_name}: mode=prompts requires a partition_key."
                    )
                assert prompts_dict is not None
                if partition_key not in prompts_dict:
                    raise KeyError(
                        f"{asset_name}: no prompt for partition {partition_key!r}."
                    )
                return prompts_dict[partition_key]

            if mode == "template":
                if partition_key is None:
                    raise ValueError(f"{asset_name}: mode=template requires a partition_key.")
                assert template_str is not None
                return template_str.replace("{topic}", partition_key).replace(
                    "{partition_key}", partition_key
                )

            if mode == "prompt":
                assert single_prompt is not None
                return single_prompt

            if mode == "composed":
                if partition_key is None:
                    raise ValueError(f"{asset_name}: mode=composed requires a partition_key.")
                topic, v_idx = self._split_partition_key(partition_key)
                return _compose_systematic(
                    topic=topic,
                    variant_idx=v_idx,
                    persona=persona,
                    style=style,
                    length=length,
                    task_type=task_type,
                    format_hint=format_hint,
                    depth=depth,
                    seed=seed,
                )

            # mode == "llm"
            if partition_key is None:
                raise ValueError(f"{asset_name}: mode=llm requires a partition_key.")
            topic, v_idx = self._split_partition_key(partition_key)
            assert paraphrase_model is not None
            return _compose_llm(
                topic=topic,
                variant_idx=v_idx,
                count_per_topic=count_per_topic,
                paraphrase_model=paraphrase_model,
                api_key_env_var=api_key_env_var,
                api_base_env_var=api_base_env_var,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                persona=persona,
                style=style,
                length=length,
                task_type=task_type,
                format_hint=format_hint,
                depth=depth,
                include_wrong_variants=include_wrong_variants,
                seed=seed,
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            group_name=self.group_name,
            kinds=set(kinds_list),
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            partition_key = context.partition_key if context.has_partition_key else None
            text = _resolve(partition_key)
            metadata: Dict[str, Any] = {
                "mode": MetadataValue.text(mode),
                "char_count": MetadataValue.int(len(text)),
                "preview": MetadataValue.md(f"> {text[:400]}"),
            }
            if partition_key is not None:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            return Output(value=text, metadata=metadata)

        return Definitions(assets=[_asset])
