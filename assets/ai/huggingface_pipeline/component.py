"""HuggingFace pipeline component — single-input, no-DataFrame.

Run any ``transformers.pipeline()`` task on a list of inputs and surface
each result in Dagster's asset catalog. Two execution modes:

  - ``mode: local`` — runs ``transformers.pipeline(task, model)`` in-process.
    Heavy first run (model download); fast after.
  - ``mode: inference_api`` — calls the HuggingFace Inference API via
    ``huggingface_hub.InferenceClient``. Zero local compute; needs a HF
    token. Some models are pay-as-you-go on the HF side.

Designed for the "demo HuggingFace in 5 minutes" path: no upstream
DataFrame asset, no pandas, just a list of strings / URLs / image paths
in the ``inputs:`` field.

For batch inference over a DataFrame column, use the task-specific
components instead (``image_object_detector``, ``text_classifier``,
``sentiment_analyzer``, etc.).
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


class HuggingfacePipelineComponent(Component, Model, Resolvable):
    # `model` is a Pydantic / Dagster Resolvable-reserved name. We expose
    # it in YAML via Field(alias="model") + populate_by_name so users keep
    # writing `model: facebook/detr-resnet-50` and the attribute is
    # `model_id` internally — no parent-attribute shadow warning.
    model_config = ConfigDict(populate_by_name=True)
    """Run a HuggingFace pipeline task on a list of inputs.

    Example (local mode — text classification):
        ```yaml
        type: dagster_community_components.HuggingfacePipelineComponent
        attributes:
          asset_key: hf/sentiment
          task: text-classification
          model: cardiffnlp/twitter-roberta-base-sentiment-latest
          mode: local
          inputs:
            - "I love this product!"
            - "Terrible experience, would not recommend."
            - "It's fine. Nothing special."
        ```

    Example (Inference API mode — object detection):
        ```yaml
        type: dagster_community_components.HuggingfacePipelineComponent
        attributes:
          asset_key: hf/detect_objects
          task: object-detection
          model: facebook/detr-resnet-50
          mode: inference_api
          inputs:
            - https://huggingface.co/datasets/mishig/sample_images/resolve/main/cats.jpg
          hf_token_env_var: HF_TOKEN
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/sentiment' or 'ml/detect_objects').")
    task: str = Field(
        description=(
            "HuggingFace pipeline task name. Any task supported by "
            "transformers.pipeline() — text-classification, object-detection, "
            "image-classification, zero-shot-classification, summarization, "
            "translation, audio-classification, fill-mask, etc."
        ),
    )
    model_id: str = Field(
        alias="model",
        description=(
            "HuggingFace Hub model id (e.g. 'facebook/detr-resnet-50', "
            "'cardiffnlp/twitter-roberta-base-sentiment-latest'). Any public "
            "or gated model on the Hub. Aliased to `model:` in YAML."
        ),
    )
    inputs: List[str] = Field(
        description=(
            "List of inputs to run inference on. Each item is passed to the "
            "pipeline as-is — strings for NLP tasks, URLs or file paths for "
            "image / audio tasks. No DataFrame needed."
        ),
    )
    mode: str = Field(
        default="local",
        description=(
            "'local' to run via transformers.pipeline() in-process, or "
            "'inference_api' to call the HuggingFace Inference API "
            "(huggingface_hub.InferenceClient)."
        ),
    )
    hf_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding the HuggingFace token. Required for "
            "mode=inference_api or for gated models in local mode."
        ),
    )
    pipeline_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Extra kwargs passed to transformers.pipeline() — e.g. "
            "{'device': 0} for GPU, {'use_fast': True}, etc."
        ),
    )
    call_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Extra kwargs passed to each pipeline call — e.g. "
            "{'top_k': 5}, {'candidate_labels': ['positive', 'negative']}."
        ),
    )

    group_name: Optional[str] = Field(default="huggingface", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — team names ('team:ml') or emails.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Key-value tags applied in the catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")

        @asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"HuggingFace {self.task} via {self.model_id}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_pipeline_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os
            import json

            token = os.environ.get(_self.hf_token_env_var or "", "") or None
            inputs = list(_self.inputs)
            pkw = dict(_self.pipeline_kwargs or {})
            ckw = dict(_self.call_kwargs or {})

            results: List[Any] = []

            if _self.mode == "local":
                try:
                    from transformers import pipeline as hf_pipeline
                except ImportError:
                    raise ImportError(
                        "huggingface_pipeline 'local' mode requires the 'transformers' "
                        "package. Install with: pip install 'transformers[torch]'"
                    )
                if token and "token" not in pkw:
                    pkw["token"] = token
                context.log.info(f"Loading {_self.task} pipeline with model={_self.model_id}")
                pipe = hf_pipeline(task=_self.task, model=_self.model_id, **pkw)
                for i, item in enumerate(inputs):
                    try:
                        out = pipe(item, **ckw)
                        results.append(out)
                        context.log.info(f"[{i+1}/{len(inputs)}] {item[:60]!r} → {str(out)[:120]}")
                    except Exception as e:
                        context.log.warning(f"[{i+1}/{len(inputs)}] {item!r} failed: {e}")
                        results.append({"error": str(e)})

            elif _self.mode == "inference_api":
                try:
                    from huggingface_hub import InferenceClient
                except ImportError:
                    raise ImportError(
                        "huggingface_pipeline 'inference_api' mode requires the "
                        "'huggingface_hub' package. Install with: pip install huggingface-hub"
                    )
                client = InferenceClient(model=_self.model_id, token=token)
                _method_map = {
                    "text-classification": client.text_classification,
                    "token-classification": client.token_classification,
                    "image-classification": client.image_classification,
                    "object-detection": client.object_detection,
                    "image-segmentation": client.image_segmentation,
                    "zero-shot-classification": client.zero_shot_classification,
                    "zero-shot-image-classification": client.zero_shot_image_classification,
                    "summarization": client.summarization,
                    "translation": client.translation,
                    "feature-extraction": client.feature_extraction,
                    "fill-mask": client.fill_mask,
                    "sentence-similarity": client.sentence_similarity,
                    "automatic-speech-recognition": client.automatic_speech_recognition,
                    "audio-classification": client.audio_classification,
                }
                fn = _method_map.get(_self.task)
                if fn is None:
                    context.log.warning(
                        f"Task {_self.task!r} not in the explicit method map — falling "
                        f"back to InferenceClient.post(). Pass call_kwargs as needed."
                    )
                for i, item in enumerate(inputs):
                    try:
                        if fn is not None:
                            out = fn(item, **ckw)
                        else:
                            out = client.post(json={"inputs": item, **ckw})
                        results.append(_normalize_inference_output(out))
                        context.log.info(f"[{i+1}/{len(inputs)}] {item[:60]!r} → ok")
                    except Exception as e:
                        context.log.warning(f"[{i+1}/{len(inputs)}] {item!r} failed: {e}")
                        results.append({"error": str(e)})
            else:
                raise ValueError(f"mode must be 'local' or 'inference_api', got: {_self.mode!r}")

            success_count = sum(1 for r in results if not (isinstance(r, dict) and "error" in r))
            preview = json.dumps(results[: min(5, len(results))], default=str, indent=2)

            return MaterializeResult(
                metadata={
                    "task": _self.task,
                    "model": _self.model_id,
                    "mode": _self.mode,
                    "input_count": len(inputs),
                    "success_count": success_count,
                    "failure_count": len(inputs) - success_count,
                    "preview": MetadataValue.md(f"```json\n{preview}\n```"),
                }
            )

        return Definitions(assets=[huggingface_pipeline_asset])


def _normalize_inference_output(out: Any) -> Any:
    """Convert HF InferenceClient return objects into JSON-friendly shapes."""
    if hasattr(out, "model_dump"):
        try:
            return out.model_dump()
        except Exception:
            pass
    if isinstance(out, list):
        return [_normalize_inference_output(x) for x in out]
    if isinstance(out, dict):
        return {k: _normalize_inference_output(v) for k, v in out.items()}
    return out
