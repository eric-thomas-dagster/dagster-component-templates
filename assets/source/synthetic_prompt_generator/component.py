"""SyntheticPromptGeneratorComponent — emit prompt text strings for LLM demos.

Sibling of `synthetic_image_generator`, `synthetic_audio_generator`,
`synthetic_pdf_generator`, `synthetic_video_generator` — completes the
"synthetic <modality> generator" set with a prompts primitive that emits
plain text.

v1 is deterministic / systematic — three mutually exclusive modes:

  1. Literal per-key mapping: `prompts: {key: text, ...}`
     Each key becomes a static partition; the mapped text is the
     partition's output.

  2. Templated: `topics: [t1, t2, ...]` + `template: "{topic} ..."`
     Each topic becomes a static partition; the resolved template
     (with {topic} + standard {partition_key} substitution) is the
     partition's output.

  3. Fixed single prompt: `prompt: "..."`
     Unpartitioned; the single string is the output.

Output type per materialization: `str` — snaps directly into
`AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}` contract.

v2 (future): LLM- or local-ML-driven prompt synthesis (e.g. take a topic
pool + a persona description and have a small LLM generate paraphrased
prompt variants). v1 covers the systematic case cleanly; v2 layers on
top without changing the emit contract.
"""
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


class SyntheticPromptGeneratorComponent(Component, Model, Resolvable):
    """Emit deterministic prompt text strings for LLM / agent demos.

    v1 is systematic — three mutually exclusive modes for how the prompt
    is resolved per materialization. All three emit `str`, which
    `AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}`
    consumes directly (no glue asset required).

    Choose ONE of:

    - `prompts: {key: text}` — key becomes a static partition,
      text is that partition's output.
    - `topics: [...]` + `template: "..."` — topics become partitions,
      template rendered per partition (`{topic}` / `{partition_key}`).
    - `prompt: "..."` — unpartitioned single-string output.
    """

    asset_name: str = Field(description="Dagster asset name.")

    # Mode A: literal per-key mapping (each key → static partition → its text)
    prompts: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Mode A: literal per-key mapping. Keys become static partitions; "
            "the mapped string is the partition's output. Mutually exclusive "
            "with `topics`+`template` and `prompt`."
        ),
    )

    # Mode B: template + topics
    topics: Optional[List[str]] = Field(
        default=None,
        description=(
            "Mode B: list of topics. Each becomes a static partition. Requires "
            "`template` to be set. Mutually exclusive with `prompts` and `prompt`."
        ),
    )
    template: Optional[str] = Field(
        default=None,
        description=(
            "Mode B: template rendered per topic. Substitutes `{topic}` (the "
            "topic value) and `{partition_key}` (same as topic in this mode). "
            "Requires `topics` to be set."
        ),
    )

    # Mode C: fixed single unpartitioned prompt
    prompt: Optional[str] = Field(
        default=None,
        description=(
            "Mode C: unpartitioned single-string prompt. Mutually exclusive "
            "with `prompts` and `topics`+`template`."
        ),
    )

    # Standard catalog metadata
    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="prompts")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['prompt', 'synthetic'].",
    )
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def _validate_mode(self) -> str:
        """Return the resolved mode ('prompts' / 'template' / 'prompt')
        and raise if the config is ambiguous or empty."""
        modes_set = []
        if self.prompts:
            modes_set.append("prompts")
        if self.topics or self.template:
            if not (self.topics and self.template):
                raise ValueError(
                    "Mode B requires BOTH `topics` and `template` to be set."
                )
            modes_set.append("template")
        if self.prompt is not None:
            modes_set.append("prompt")
        if not modes_set:
            raise ValueError(
                "Must set exactly one of: `prompts:`, `topics:`+`template:`, or `prompt:`."
            )
        if len(modes_set) > 1:
            raise ValueError(
                f"Multiple modes set ({modes_set}); pick exactly one of "
                "`prompts:`, `topics:`+`template:`, or `prompt:`."
            )
        return modes_set[0]

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        mode = self._validate_mode()

        # Build the partition set + resolver
        partitions_def = None
        if mode == "prompts":
            partition_keys = list(self.prompts.keys())
            partitions_def = StaticPartitionsDefinition(partition_keys)
            prompts_dict = dict(self.prompts)

            def _resolve(partition_key: Optional[str]) -> str:
                if partition_key is None:
                    raise ValueError(
                        f"asset {self.asset_name}: mode=prompts requires a partition_key; "
                        "materialize with `--partition <key>` or backfill."
                    )
                if partition_key not in prompts_dict:
                    raise KeyError(
                        f"asset {self.asset_name}: no prompt for partition {partition_key!r}. "
                        f"Known keys: {list(prompts_dict)}"
                    )
                return prompts_dict[partition_key]

        elif mode == "template":
            partitions_def = StaticPartitionsDefinition(list(self.topics))
            template = self.template
            topics_set = set(self.topics)

            def _resolve(partition_key: Optional[str]) -> str:
                if partition_key is None:
                    raise ValueError(
                        f"asset {self.asset_name}: mode=template requires a partition_key."
                    )
                if partition_key not in topics_set:
                    raise KeyError(
                        f"asset {self.asset_name}: partition {partition_key!r} not in topics."
                    )
                return template.replace("{topic}", partition_key).replace(
                    "{partition_key}", partition_key
                )

        else:  # mode == "prompt"
            single = self.prompt

            def _resolve(partition_key: Optional[str]) -> str:
                return single

        asset_name = self.asset_name
        description = (
            self.description
            or f"Synthetic prompt asset ({mode} mode) — one string per materialization."
        )
        kinds_list = self.kinds or ["prompt", "synthetic"]

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
