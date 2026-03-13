"""Image Classifier Component.

Classify images using a pre-trained model (CLIP or torchvision models).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class ImageClassifierComponent(Component, Model, Resolvable):
    """Component for classifying images using pre-trained models.

    Supports CLIP zero-shot classification and standard torchvision models.
    Returns top-N predicted labels and confidence scores.

    Features:
    - CLIP zero-shot classification against custom candidate labels
    - torchvision model support (resnet50, efficientnet) with ImageNet labels
    - Configurable top-k predictions
    - Optional full predictions column with all label/score pairs
    - CPU, CUDA, and MPS device support

    Use Cases:
    - Product image categorization
    - Content moderation classification
    - Visual search indexing
    - Image dataset labeling
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with image file paths or URLs")
    output_column: str = Field(
        default="predicted_class", description="Top predicted label column"
    )
    score_column: str = Field(
        default="confidence_score", description="Confidence score for top prediction"
    )
    top_k: int = Field(default=3, description="Return top-k predictions")
    all_predictions_column: Optional[str] = Field(
        default=None,
        description="If set, write list of [{label, score}] dicts to this column",
    )
    model_name: str = Field(
        default="openai/clip-vit-base-patch32",
        description="HuggingFace model ID or 'resnet50'/'efficientnet'",
    )
    candidate_labels: Optional[List[str]] = Field(
        default=None,
        description="For CLIP zero-shot, restrict to these labels",
    )
    device: str = Field(
        default="cpu",
        description="Compute device: cpu, cuda, or mps",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        image_column = self.image_column
        output_column = self.output_column
        score_column = self.score_column
        top_k = self.top_k
        all_predictions_column = self.all_predictions_column
        model_name = self.model_name
        candidate_labels = self.candidate_labels
        device = self.device

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "image_classifier"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=self.group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                from PIL import Image
            except ImportError:
                raise ImportError("Pillow required: pip install Pillow")
            try:
                import torch
            except ImportError:
                raise ImportError("torch required: pip install torch")

            df = upstream.copy()
            top_labels = []
            top_scores = []
            all_preds = []

            use_clip = model_name not in ("resnet50", "efficientnet")

            if use_clip:
                try:
                    from transformers import CLIPProcessor, CLIPModel
                except ImportError:
                    raise ImportError("transformers required: pip install transformers")

                context.log.info(f"Loading CLIP model {model_name}")
                model = CLIPModel.from_pretrained(model_name).to(device)
                processor = CLIPProcessor.from_pretrained(model_name)

                labels = candidate_labels or [
                    "cat", "dog", "car", "person", "building", "food",
                    "nature", "technology", "sport", "animal",
                ]

                for path in df[image_column]:
                    try:
                        img = Image.open(str(path)).convert("RGB")
                        inputs = processor(
                            text=labels, images=img, return_tensors="pt", padding=True
                        )
                        inputs = {k: v.to(device) for k, v in inputs.items()}
                        with torch.no_grad():
                            outputs = model(**inputs)
                        logits = outputs.logits_per_image[0]
                        probs = logits.softmax(dim=-1).cpu().tolist()
                        pairs = sorted(
                            zip(labels, probs), key=lambda x: x[1], reverse=True
                        )
                        top_labels.append(pairs[0][0])
                        top_scores.append(round(pairs[0][1], 4))
                        all_preds.append(
                            [{"label": l, "score": round(s, 4)} for l, s in pairs[:top_k]]
                        )
                    except Exception as e:
                        context.log.warning(f"Classification failed for {path}: {e}")
                        top_labels.append(None)
                        top_scores.append(None)
                        all_preds.append(None)
            else:
                try:
                    import torchvision.models as tv_models
                    import torchvision.transforms as transforms
                    import urllib.request
                    import json as _json
                except ImportError:
                    raise ImportError("torchvision required: pip install torchvision")

                context.log.info(f"Loading torchvision model {model_name}")
                if model_name == "resnet50":
                    model = tv_models.resnet50(pretrained=True).to(device).eval()
                else:
                    model = tv_models.efficientnet_b0(pretrained=True).to(device).eval()

                transform = transforms.Compose([
                    transforms.Resize(256),
                    transforms.CenterCrop(224),
                    transforms.ToTensor(),
                    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
                ])

                # Load ImageNet labels
                imagenet_url = (
                    "https://raw.githubusercontent.com/pytorch/hub/master/imagenet_classes.txt"
                )
                try:
                    with urllib.request.urlopen(imagenet_url, timeout=5) as f:
                        imagenet_labels = [line.strip().decode() for line in f.readlines()]
                except Exception:
                    imagenet_labels = [str(i) for i in range(1000)]

                for path in df[image_column]:
                    try:
                        img = Image.open(str(path)).convert("RGB")
                        inp = transform(img).unsqueeze(0).to(device)
                        with torch.no_grad():
                            output = model(inp)
                        probs = torch.nn.functional.softmax(output[0], dim=0).cpu()
                        top_indices = probs.topk(top_k).indices.tolist()
                        top_probs = probs.topk(top_k).values.tolist()
                        top_labels.append(imagenet_labels[top_indices[0]])
                        top_scores.append(round(top_probs[0], 4))
                        all_preds.append(
                            [
                                {"label": imagenet_labels[i], "score": round(p, 4)}
                                for i, p in zip(top_indices, top_probs)
                            ]
                        )
                    except Exception as e:
                        context.log.warning(f"Classification failed for {path}: {e}")
                        top_labels.append(None)
                        top_scores.append(None)
                        all_preds.append(None)

            df[output_column] = top_labels
            df[score_column] = top_scores
            if all_predictions_column:
                df[all_predictions_column] = all_preds

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "model": model_name,
                    "device": device,
                    "preview": MetadataValue.md(df[[image_column, output_column, score_column]].head(5).to_markdown()),
                }
            )
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
