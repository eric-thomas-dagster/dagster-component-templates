"""HuggingFace router chat-completion component.

Calls the HuggingFace router (``https://router.huggingface.co/v1``) using
the standard OpenAI SDK. The router exposes hundreds of hosted chat
models (Kimi-K2, Llama-3.3, Mistral, Qwen, DeepSeek, etc.) through a
single OpenAI-compatible endpoint — so this component is a drop-in
chat-completion runner that doesn't care about per-vendor SDKs.

Different from ``huggingface_pipeline``:

  - ``huggingface_pipeline`` runs ``transformers.pipeline()`` tasks
    (text-classification, object-detection, summarization, …) either
    locally or via the *task-specific* Inference API endpoints.
  - This component runs **chat completions** against the router. Output
    is an assistant message + token usage. Same shape as OpenAI /
    Anthropic chat-completion components.

For pure-OpenAI customers there are dedicated ``openai_*`` components.
This component is for **HF-routed** chat: any model on the HF Hub that
exposes a chat-completion endpoint.
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


HF_ROUTER_BASE_URL = "https://router.huggingface.co/v1"


class HuggingfaceChatCompletionComponent(Component, Model, Resolvable):
    # `model` shadows Pydantic / Dagster Resolvable parent attr; alias in YAML.
    model_config = ConfigDict(populate_by_name=True)
    """Call a chat-completion model via the HuggingFace router (OpenAI-compatible).

    Example (simple prompt, Kimi-K2):
        ```yaml
        type: dagster_community_components.HuggingfaceChatCompletionComponent
        attributes:
          asset_key: hf/chat/photosynthesis
          model: moonshotai/Kimi-K2-Instruct-0905
          prompt: "Describe the process of photosynthesis."
          hf_token_env_var: HF_TOKEN
        ```

    Example (full messages array + temperature):
        ```yaml
        type: dagster_community_components.HuggingfaceChatCompletionComponent
        attributes:
          asset_key: hf/chat/scientist
          model: meta-llama/Llama-3.3-70B-Instruct
          messages:
            - role: system
              content: "You are an expert physicist. Be precise."
            - role: user
              content: "Explain Hawking radiation in two sentences."
          temperature: 0.3
          max_tokens: 500
          hf_token_env_var: HF_TOKEN
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/chat/summary').")
    model_id: str = Field(
        alias="model",
        description=(
            "HF Hub model id available on the router (e.g. "
            "'moonshotai/Kimi-K2-Instruct-0905', "
            "'meta-llama/Llama-3.3-70B-Instruct', 'mistralai/Mistral-7B-Instruct-v0.3'). "
            "Aliased to `model:` in YAML."
        ),
    )
    prompt: Optional[str] = Field(
        default=None,
        description=(
            "Simple single-user-message prompt. Mutually exclusive with "
            "`messages`. Use this for the common 'one question, one answer' shape."
        ),
    )
    messages: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description=(
            "OpenAI-shaped chat messages: [{role, content}, ...]. "
            "Mutually exclusive with `prompt`."
        ),
    )
    hf_token_env_var: str = Field(
        default="HF_TOKEN",
        description="Env var with HuggingFace token (default 'HF_TOKEN').",
        json_schema_extra={"ui:widget": "env_var"},
    )
    temperature: Optional[float] = Field(default=None, description="Sampling temperature (0.0–2.0).")
    max_tokens: Optional[int] = Field(default=None, description="Max output tokens.")
    top_p: Optional[float] = Field(default=None, description="Nucleus sampling top_p.")
    base_url: str = Field(
        default=HF_ROUTER_BASE_URL,
        description="OpenAI-compatible base URL. Default is the HF router.",
    )
    extra_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional kwargs forwarded to chat.completions.create() (e.g. stop, response_format).",
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
            description=self.description or f"HF chat-completion via {self.model_id}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_chat_completion_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os
            try:
                from openai import OpenAI
            except ImportError:
                raise ImportError(
                    "huggingface_chat_completion requires the 'openai' package. "
                    "Install with: pip install openai"
                )

            token = os.environ.get(_self.hf_token_env_var, "")
            if not token:
                raise ValueError(
                    f"HuggingFace token not found in env var {_self.hf_token_env_var!r}. "
                    f"The router requires authentication."
                )

            if _self.prompt is not None and _self.messages is not None:
                raise ValueError("Set either `prompt` or `messages`, not both.")
            if _self.prompt is None and _self.messages is None:
                raise ValueError("Set one of `prompt` or `messages`.")

            if _self.prompt is not None:
                messages = [{"role": "user", "content": _self.prompt}]
            else:
                assert _self.messages is not None  # narrowed by the validation above
                messages = list(_self.messages)

            client = OpenAI(base_url=_self.base_url, api_key=token)

            kwargs: Dict[str, Any] = {"model": _self.model_id, "messages": messages}
            if _self.temperature is not None:
                kwargs["temperature"] = _self.temperature
            if _self.max_tokens is not None:
                kwargs["max_tokens"] = _self.max_tokens
            if _self.top_p is not None:
                kwargs["top_p"] = _self.top_p
            if _self.extra_kwargs:
                kwargs.update(_self.extra_kwargs)

            context.log.info(
                f"HF chat: model={_self.model_id} messages={len(messages)} "
                f"temperature={_self.temperature} max_tokens={_self.max_tokens}"
            )

            completion = client.chat.completions.create(**kwargs)
            choice = completion.choices[0]
            content = choice.message.content or ""
            usage = getattr(completion, "usage", None)

            context.log.info(f"HF chat response: {content[:200]}{'...' if len(content) > 200 else ''}")

            metadata: Dict[str, Any] = {
                "model": _self.model_id,
                "base_url": _self.base_url,
                "finish_reason": choice.finish_reason or "(none)",
                "response": MetadataValue.md(content),
                "response_length_chars": len(content),
                "message_count": len(messages),
            }
            if usage is not None:
                metadata.update({
                    "prompt_tokens": getattr(usage, "prompt_tokens", None),
                    "completion_tokens": getattr(usage, "completion_tokens", None),
                    "total_tokens": getattr(usage, "total_tokens", None),
                })

            return MaterializeResult(metadata=metadata)

        return Definitions(assets=[huggingface_chat_completion_asset])
