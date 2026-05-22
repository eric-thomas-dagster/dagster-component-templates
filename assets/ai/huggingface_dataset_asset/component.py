"""HuggingFace Hub dataset — observable source asset.

Surfaces a [HuggingFace Hub dataset](https://huggingface.co/datasets) as a
Dagster ``observable_source_asset``. On each observation, it polls the Hub
API via ``huggingface_hub.HfApi().dataset_info()`` and emits an
``ObserveResult`` with the dataset's current downloads, likes, last-modified
timestamp, configs, and file count.

The dataset itself lives on the HF Hub — Dagster surfaces its **metadata
in the catalog**, not its contents. Pair with ``huggingface_pipeline``
(``call_kwargs: {dataset_url: ...}``) or your own loader to actually
consume the data downstream.
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


class HuggingfaceDatasetAssetComponent(Component, Model, Resolvable):
    """Observe a HuggingFace Hub dataset's metadata as a Dagster source asset.

    Example:
        ```yaml
        type: dagster_community_components.HuggingfaceDatasetAssetComponent
        attributes:
          asset_key: hf/datasets/imdb
          dataset_id: imdb
          hf_token_env_var: HF_TOKEN   # optional, for gated/private datasets
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/datasets/imdb').")
    dataset_id: str = Field(
        description=(
            "HuggingFace Hub dataset id (e.g. 'imdb', 'glue', or "
            "'mozilla-foundation/common_voice_16_0' for namespaced datasets)."
        ),
    )
    revision: Optional[str] = Field(
        default=None,
        description="Optional revision (branch / tag / commit hash) to inspect.",
    )
    hf_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with HF token (required for gated / private datasets).",
        json_schema_extra={"ui:widget": "env_var"},
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

        @observable_source_asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"HuggingFace Hub dataset: {self.dataset_id}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_dataset_asset(context) -> ObserveResult:
            import os
            try:
                from huggingface_hub import HfApi
            except ImportError:
                raise ImportError(
                    "huggingface_dataset_asset requires the 'huggingface_hub' "
                    "package. Install with: pip install huggingface-hub"
                )

            token = os.environ.get(_self.hf_token_env_var or "", "") or None
            api = HfApi(token=token)

            try:
                info = api.dataset_info(_self.dataset_id, revision=_self.revision)
            except Exception as e:
                context.log.error(f"HfApi.dataset_info({_self.dataset_id!r}) failed: {e}")
                return ObserveResult(
                    metadata={
                        "dataset_id": _self.dataset_id,
                        "error": str(e),
                        "hub_url": f"https://huggingface.co/datasets/{_self.dataset_id}",
                    }
                )

            file_count = len(info.siblings or [])
            configs: List[str] = []
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
            try:
                cfg = cards.get("configs", [])
                if isinstance(cfg, list):
                    for c in cfg:
                        if isinstance(c, dict) and "config_name" in c:
                            configs.append(c["config_name"])
                        elif isinstance(c, str):
                            configs.append(c)
            except Exception:
                pass

            context.log.info(
                f"hf:dataset/{_self.dataset_id} — "
                f"downloads={info.downloads} likes={info.likes} files={file_count}"
            )

            return ObserveResult(
                metadata={
                    "dataset_id": _self.dataset_id,
                    "downloads": int(info.downloads or 0),
                    "likes": int(info.likes or 0),
                    "last_modified": str(info.last_modified) if info.last_modified else None,
                    "private": bool(getattr(info, "private", False)),
                    "gated": getattr(info, "gated", False) if hasattr(info, "gated") else None,
                    "file_count": file_count,
                    "configs": ", ".join(configs) if configs else "(default)",
                    "task_categories": ", ".join(cards.get("task_categories", []) or []),
                    "license": cards.get("license") or "(unspecified)",
                    "hub_url": MetadataValue.url(f"https://huggingface.co/datasets/{_self.dataset_id}"),
                }
            )

        return Definitions(assets=[huggingface_dataset_asset])
