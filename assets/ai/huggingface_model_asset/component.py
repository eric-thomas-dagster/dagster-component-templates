"""HuggingFace Hub model — observable source asset.

Surfaces a [HuggingFace Hub model](https://huggingface.co/models) as a
Dagster ``observable_source_asset``. On each observation, polls the Hub
API via ``huggingface_hub.HfApi().model_info()`` and emits an
``ObserveResult`` with the model's current downloads, likes,
pipeline tag, library_name, last-modified timestamp, and file count.

Use it to track the version of a model that downstream inference assets
depend on — e.g. trigger a re-eval pipeline whenever the model's
last_modified advances.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    ObserveResult,
    Resolvable,
    observable_source_asset,
)
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


class HuggingfaceModelAssetComponent(Component, Model, Resolvable):
    """Observe a HuggingFace Hub model's metadata as a Dagster source asset.

    Example:
        ```yaml
        type: dagster_community_components.HuggingfaceModelAssetComponent
        attributes:
          asset_key: hf/models/sentiment_model
          model_id: cardiffnlp/twitter-roberta-base-sentiment-latest
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/models/bert-large').")
    model_id: str = Field(
        description=(
            "HuggingFace Hub model id (e.g. 'facebook/detr-resnet-50', "
            "'cardiffnlp/twitter-roberta-base-sentiment-latest')."
        ),
    )
    revision: Optional[str] = Field(
        default=None,
        description="Optional revision (branch / tag / commit hash) to inspect.",
    )
    hf_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with HF token (required for gated / private models).",
        json_schema_extra={"ui:widget": "env_var"},
    )

    group_name: Optional[str] = Field(default="huggingface", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Key-value catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")

        @observable_source_asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"HuggingFace Hub model: {self.model_id}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_model_asset(context) -> ObserveResult:
            import os
            try:
                from huggingface_hub import HfApi
            except ImportError:
                raise ImportError(
                    "huggingface_model_asset requires the 'huggingface_hub' "
                    "package. Install with: pip install huggingface-hub"
                )

            token = os.environ.get(_self.hf_token_env_var or "", "") or None
            api = HfApi(token=token)

            try:
                info = api.model_info(_self.model_id, revision=_self.revision)
            except Exception as e:
                context.log.error(f"HfApi.model_info({_self.model_id!r}) failed: {e}")
                return ObserveResult(
                    metadata={
                        "model_id": _self.model_id,
                        "error": str(e),
                        "hub_url": f"https://huggingface.co/{_self.model_id}",
                    }
                )

            file_count = len(info.siblings or [])
            tags = list(info.tags or [])
            cards: Dict[str, Any] = {}
            card_data = getattr(info, "card_data", None)
            if card_data is not None:
                if hasattr(card_data, "to_dict"):
                    try:
                        cards = card_data.to_dict()
                    except Exception:
                        cards = {}
                elif isinstance(card_data, dict):
                    cards = card_data

            context.log.info(
                f"hf:model/{_self.model_id} — "
                f"downloads={info.downloads} likes={info.likes} "
                f"task={info.pipeline_tag} files={file_count}"
            )

            return ObserveResult(
                metadata={
                    "model_id": _self.model_id,
                    "downloads": int(info.downloads or 0),
                    "likes": int(info.likes or 0),
                    "pipeline_tag": info.pipeline_tag or "(unspecified)",
                    "library_name": getattr(info, "library_name", None) or "(unspecified)",
                    "last_modified": str(info.last_modified) if info.last_modified else None,
                    "private": bool(getattr(info, "private", False)),
                    "gated": getattr(info, "gated", False) if hasattr(info, "gated") else None,
                    "file_count": file_count,
                    "tags": ", ".join(tags[:20]),
                    "license": cards.get("license") or "(unspecified)",
                    "base_model": ", ".join(cards.get("base_model", []) or [])
                    if isinstance(cards.get("base_model"), list)
                    else (cards.get("base_model") or ""),
                    "hub_url": MetadataValue.url(f"https://huggingface.co/{_self.model_id}"),
                }
            )

        return Definitions(assets=[huggingface_model_asset])
