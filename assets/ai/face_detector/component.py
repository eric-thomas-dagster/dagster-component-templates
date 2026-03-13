"""Face Detector Component.

Detect faces in images using OpenCV or MediaPipe backends.
"""

from dataclasses import dataclass
from typing import Optional
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
class FaceDetectorComponent(Component, Model, Resolvable):
    """Component for detecting faces in images.

    Supports OpenCV Haar cascades and MediaPipe face detection backends.
    Returns face count and optional bounding boxes and landmark positions.

    Features:
    - OpenCV Haar cascade detection (fast, CPU-only)
    - MediaPipe face detection (accurate, supports confidence threshold)
    - Optional bounding box column
    - Optional landmarks column
    - Configurable minimum confidence

    Use Cases:
    - Headcount estimation from images
    - Privacy compliance (detect faces for blurring)
    - Photo quality assessment
    - Audience measurement
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with image file paths")
    count_column: str = Field(
        default="face_count", description="Column to write detected face count"
    )
    boxes_column: Optional[str] = Field(
        default=None,
        description="If set, write list of bounding box dicts to this column",
    )
    landmarks_column: Optional[str] = Field(
        default=None,
        description="If set, write facial landmarks to this column",
    )
    backend: str = Field(
        default="opencv",
        description="Detection backend: opencv, mediapipe, or retinaface",
    )
    min_confidence: float = Field(
        default=0.5, description="Minimum detection confidence (used by mediapipe)"
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        image_column = self.image_column
        count_column = self.count_column
        boxes_column = self.boxes_column
        landmarks_column = self.landmarks_column
        backend = self.backend
        min_confidence = self.min_confidence

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

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
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
            df = upstream.copy()
            counts = []
            all_boxes = []
            all_landmarks = []

            if backend == "mediapipe":
                try:
                    import mediapipe as mp
                except ImportError:
                    raise ImportError("mediapipe required: pip install mediapipe")
                try:
                    import cv2
                except ImportError:
                    raise ImportError("opencv-python required: pip install opencv-python")

                mp_face = mp.solutions.face_detection
                face_detection = mp_face.FaceDetection(
                    model_selection=1, min_detection_confidence=min_confidence
                )

                for path in df[image_column]:
                    try:
                        img_bgr = cv2.imread(str(path))
                        if img_bgr is None:
                            raise ValueError(f"Could not load image: {path}")
                        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
                        results = face_detection.process(img_rgb)
                        h, w = img_rgb.shape[:2]
                        boxes = []
                        lmarks = []

                        if results.detections:
                            for det in results.detections:
                                bb = det.location_data.relative_bounding_box
                                boxes.append(
                                    {
                                        "x": round(bb.xmin * w, 2),
                                        "y": round(bb.ymin * h, 2),
                                        "width": round(bb.width * w, 2),
                                        "height": round(bb.height * h, 2),
                                        "confidence": round(det.score[0], 4) if det.score else None,
                                    }
                                )
                                kp_list = []
                                for kp in det.location_data.relative_keypoints:
                                    kp_list.append(
                                        {"x": round(kp.x * w, 2), "y": round(kp.y * h, 2)}
                                    )
                                lmarks.append(kp_list)

                        counts.append(len(boxes))
                        all_boxes.append(boxes)
                        all_landmarks.append(lmarks)
                    except Exception as e:
                        context.log.warning(f"Face detection failed for {path}: {e}")
                        counts.append(0)
                        all_boxes.append([])
                        all_landmarks.append([])

                face_detection.close()

            else:
                # opencv backend
                try:
                    import cv2
                except ImportError:
                    raise ImportError("opencv-python required: pip install opencv-python")

                cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
                face_cascade = cv2.CascadeClassifier(cascade_path)

                for path in df[image_column]:
                    try:
                        img = cv2.imread(str(path))
                        if img is None:
                            raise ValueError(f"Could not load image: {path}")
                        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                        detected = face_cascade.detectMultiScale(
                            gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30)
                        )
                        boxes = []
                        if len(detected) > 0:
                            for x, y, w, h in detected:
                                boxes.append(
                                    {
                                        "x": int(x), "y": int(y),
                                        "width": int(w), "height": int(h),
                                    }
                                )
                        counts.append(len(boxes))
                        all_boxes.append(boxes)
                        all_landmarks.append([])
                    except Exception as e:
                        context.log.warning(f"Face detection failed for {path}: {e}")
                        counts.append(0)
                        all_boxes.append([])
                        all_landmarks.append([])

            df[count_column] = counts
            if boxes_column:
                df[boxes_column] = all_boxes
            if landmarks_column:
                df[landmarks_column] = all_landmarks

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "backend": backend,
                    "total_faces": MetadataValue.int(sum(counts)),
                    "preview": MetadataValue.md(df[[image_column, count_column]].head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
