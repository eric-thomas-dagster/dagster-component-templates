"""HuggingFace Inference Endpoints — dedicated-endpoint client.

Calls a [dedicated HuggingFace Inference Endpoint](https://huggingface.co/inference-endpoints)
that **you** deployed (paid, scaled, dedicated to one model). Different
from ``huggingface_pipeline`` ``mode: inference_api`` which uses the
shared public Inference API.

Use this when:

  - The customer has provisioned a dedicated endpoint (auto-scale,
    private VPC option, predictable latency) and wants Dagster to call it.
  - The model is too large for the shared Inference API.
  - SLA / availability matters (the dedicated endpoint has uptime
    guarantees the shared API doesn't).
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
from pydantic import Field


class HuggingfaceInferenceEndpointComponent(Component, Model, Resolvable):
    """Call a dedicated HuggingFace Inference Endpoint.

    Example (named endpoint):
        ```yaml
        type: dagster_community_components.HuggingfaceInferenceEndpointComponent
        attributes:
          asset_key: hf/endpoint/sentiment
          endpoint_name: my-sentiment-endpoint  # named endpoint in your HF account
          task: text-classification
          inputs:
            - "I love this product!"
            - "Terrible experience."
          hf_token_env_var: HF_TOKEN
        ```

    Example (endpoint URL):
        ```yaml
        type: dagster_community_components.HuggingfaceInferenceEndpointComponent
        attributes:
          asset_key: hf/endpoint/sentiment
          endpoint_url: https://abc123.us-east-1.aws.endpoints.huggingface.cloud
          task: text-classification
          inputs:
            - "I love this product!"
          hf_token_env_var: HF_TOKEN
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/endpoint/sentiment').")
    task: str = Field(
        description=(
            "Inference task — text-classification, object-detection, "
            "image-classification, summarization, etc."
        ),
    )
    inputs: List[str] = Field(description="List of inputs to send to the endpoint.")
    endpoint_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of a dedicated endpoint in your HF account. Looked up via "
            "huggingface_hub.HfApi().get_inference_endpoint(). Mutually "
            "exclusive with endpoint_url."
        ),
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description=(
            "Direct endpoint URL (e.g. https://xxxx.aws.endpoints.huggingface.cloud). "
            "Use this if you already know the URL. Mutually exclusive with endpoint_name."
        ),
    )
    namespace: Optional[str] = Field(
        default=None,
        description="HF namespace (username / org) — required if endpoint_name is in another account.",
    )
    hf_token_env_var: str = Field(
        description="Env var with HF token. Required to call the endpoint.",
        json_schema_extra={"ui:widget": "env_var"},
    )
    call_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Extra kwargs passed to each inference call (e.g. {'candidate_labels': [...]}).",
    )

    group_name: Optional[str] = Field(default="huggingface", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Key-value catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")

        @asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"HF Inference Endpoint: {self.endpoint_name or self.endpoint_url}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_inference_endpoint_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os
            import json

            token = os.environ.get(_self.hf_token_env_var, "")
            if not token:
                raise ValueError(
                    f"HuggingFace token not found in env var {_self.hf_token_env_var!r}. "
                    f"Dedicated endpoints require authentication."
                )

            try:
                from huggingface_hub import InferenceClient, HfApi
            except ImportError:
                raise ImportError(
                    "huggingface_inference_endpoint requires the 'huggingface_hub' "
                    "package. Install with: pip install huggingface-hub"
                )

            if _self.endpoint_url and _self.endpoint_name:
                raise ValueError("Set endpoint_name OR endpoint_url, not both.")
            if not _self.endpoint_url and not _self.endpoint_name:
                raise ValueError("Set endpoint_name OR endpoint_url.")

            if _self.endpoint_url:
                model_url = _self.endpoint_url
                endpoint_status = "(url-provided)"
            else:
                api = HfApi(token=token)
                ep = api.get_inference_endpoint(name=_self.endpoint_name, namespace=_self.namespace)
                model_url = ep.url
                endpoint_status = str(ep.status) if hasattr(ep, "status") else "(unknown)"
                context.log.info(
                    f"Resolved endpoint {_self.endpoint_name!r} → {model_url} (status={endpoint_status})"
                )

            client = InferenceClient(model=model_url, token=token)
            ckw = dict(_self.call_kwargs or {})

            _method_map = {
                "text-classification": client.text_classification,
                "image-classification": client.image_classification,
                "object-detection": client.object_detection,
                "zero-shot-classification": client.zero_shot_classification,
                "summarization": client.summarization,
                "translation": client.translation,
                "feature-extraction": client.feature_extraction,
                "fill-mask": client.fill_mask,
                "automatic-speech-recognition": client.automatic_speech_recognition,
                "audio-classification": client.audio_classification,
            }
            fn = _method_map.get(_self.task)

            results: List[Any] = []
            for i, item in enumerate(_self.inputs):
                try:
                    if fn is not None:
                        out = fn(item, **ckw)
                    else:
                        out = client.post(json={"inputs": item, **ckw})
                    if hasattr(out, "model_dump"):
                        out = out.model_dump()
                    results.append(out)
                    context.log.info(f"[{i+1}/{len(_self.inputs)}] {str(item)[:60]!r} → ok")
                except Exception as e:
                    context.log.warning(f"[{i+1}/{len(_self.inputs)}] {item!r} failed: {e}")
                    results.append({"error": str(e)})

            success_count = sum(1 for r in results if not (isinstance(r, dict) and "error" in r))
            preview = json.dumps(results[: min(5, len(results))], default=str, indent=2)

            return MaterializeResult(
                metadata={
                    "endpoint_name": _self.endpoint_name or "(direct URL)",
                    "endpoint_url": MetadataValue.url(model_url),
                    "endpoint_status": endpoint_status,
                    "task": _self.task,
                    "input_count": len(_self.inputs),
                    "success_count": success_count,
                    "failure_count": len(_self.inputs) - success_count,
                    "preview": MetadataValue.md(f"```json\n{preview}\n```"),
                }
            )

        return Definitions(assets=[huggingface_inference_endpoint_asset])
