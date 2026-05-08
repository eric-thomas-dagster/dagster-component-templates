"""Document Text Extractor Asset Component.

Extract text content from various document formats (PDF, DOCX, TXT, HTML, etc.).
Supports multiple extraction methods and formats for downstream processing.
"""

import os
from typing import Any, Dict, List, Optional
from pathlib import Path

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DocumentTextExtractorComponent(Component, Model, Resolvable):
    """Component for extracting text from documents.

    This asset extracts text from various document formats including PDF, DOCX,
    TXT, HTML, Markdown, and more. Useful as the first step in document processing pipelines.

    File paths should be provided via:
    - RunConfig (ideal for sensors detecting new files)
    - Upstream assets producing file paths

    Example with RunConfig:
        ```yaml
        type: dagster_component_templates.DocumentTextExtractorComponent
        attributes:
          asset_name: extracted_text
          extraction_method: auto
        ```

        RunConfig (passed by sensor):
        ```python
        RunRequest(
            run_config={"ops": {"extracted_text": {"config": {"file_path": "/path/to/document.pdf"}}}}
        )
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    extraction_method: str = Field(
        default="auto",
        description="Extraction method: 'auto', 'pypdf', 'pdfplumber', 'pytesseract' (OCR), 'docx', 'markdown'"
    )

    ocr_enabled: bool = Field(
        default=False,
        description="Enable OCR for scanned PDFs or images"
    )

    preserve_formatting: bool = Field(
        default=False,
        description="Attempt to preserve document formatting (paragraphs, whitespace)"
    )

    extract_metadata: bool = Field(
        default=True,
        description="Extract document metadata (author, creation date, etc.)"
    )

    page_range: Optional[str] = Field(
        default=None,
        description="Page range to extract (e.g., '1-5', '1,3,5'). Only for PDFs"
    )

    output_format: str = Field(
        default="text",
        description="Output format: 'text', 'json' (with metadata), 'markdown'"
    )

    save_to_file: bool = Field(
        default=False,
        description="Save extracted text to a file"
    )

    output_path: Optional[str] = Field(
        default=None,
        description="Path to save extracted text (required if save_to_file is True)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
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
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        extraction_method = self.extraction_method
        ocr_enabled = self.ocr_enabled
        preserve_formatting = self.preserve_formatting
        extract_metadata = self.extract_metadata
        page_range = self.page_range
        output_format = self.output_format
        save_to_file = self.save_to_file
        output_path = self.output_path
        description = self.description or "Extract text from documents"
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "document_text_extractor"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def document_text_extractor_asset(context: AssetExecutionContext, config: Optional[dict] = None, **kwargs):
            """Asset that extracts text from documents.

            File path can be provided via:
            - RunConfig: context.op_execution_context.op_config['file_path']
            - Upstream assets: via **kwargs (string or dict with 'file_path' key)

            Ideal for use with sensors that detect new files and pass them via RunRequest.
            """

            # Get file_path from config (RunConfig) or upstream assets
            file_path = None

            # Priority 1: Check op config (from RunConfig)
            if config and 'file_path' in config:
                file_path = config['file_path']
                context.log.info(f"Using file_path from RunConfig: {file_path}")

            # Priority 2: Check upstream assets
            if not file_path and kwargs:
                upstream_assets = {k: v for k, v in kwargs.items()}
                for key, value in upstream_assets.items():
                    if isinstance(value, str):
                        file_path = value
                        context.log.info(f"Using file_path from upstream asset '{key}': {file_path}")
                        break
                    elif isinstance(value, dict) and 'file_path' in value:
                        file_path = value['file_path']
                        context.log.info(f"Using file_path from upstream asset '{key}' dict: {file_path}")
                        break

            if not file_path:
                raise ValueError(
                    f"Document Text Extractor '{asset_name}' requires file_path. "
                    "Provide via RunConfig (ideal for sensors) or upstream asset. "
                    "Example RunConfig: {\"ops\": {\"" + asset_name + "\": {\"config\": {\"file_path\": \"/path/to/file.pdf\"}}}}"
                )

            # Verify file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            file_extension = Path(file_path).suffix.lower()
            context.log.info(f"Extracting text from {file_path} (format: {file_extension})")

            extracted_text = ""
            metadata = {}

            # Auto-detect extraction method
            method = extraction_method
            if method == "auto":
                if file_extension == ".pdf":
                    method = "pypdf"
                elif file_extension in [".docx", ".doc"]:
                    method = "docx"
                elif file_extension in [".txt", ".text"]:
                    method = "text"
                elif file_extension in [".html", ".htm"]:
                    method = "html"
                elif file_extension in [".md", ".markdown"]:
                    method = "markdown"
                else:
                    method = "text"

            context.log.info(f"Using extraction method: {method}")

            # Extract based on method
            if method in ["pypdf", "pdfplumber"] and file_extension == ".pdf":
                if method == "pypdf":
                    try:
                        import pypdf
                        with open(file_path, 'rb') as f:
                            reader = pypdf.PdfReader(f)

                            # Extract metadata
                            if extract_metadata and reader.metadata:
                                metadata = {
                                    "title": reader.metadata.get("/Title", ""),
                                    "author": reader.metadata.get("/Author", ""),
                                    "subject": reader.metadata.get("/Subject", ""),
                                    "creator": reader.metadata.get("/Creator", ""),
                                    "producer": reader.metadata.get("/Producer", ""),
                                }

                            # Parse page range
                            pages_to_extract = range(len(reader.pages))
                            if page_range:
                                pages_to_extract = self._parse_page_range(page_range, len(reader.pages))

                            # Extract text
                            text_parts = []
                            for page_num in pages_to_extract:
                                page = reader.pages[page_num]
                                text_parts.append(page.extract_text())

                            extracted_text = "\n\n".join(text_parts)
                    except ImportError:
                        raise ImportError("pypdf not installed. Install with: pip install pypdf")

                elif method == "pdfplumber":
                    try:
                        import pdfplumber
                        with pdfplumber.open(file_path) as pdf:
                            # Parse page range
                            pages_to_extract = range(len(pdf.pages))
                            if page_range:
                                pages_to_extract = self._parse_page_range(page_range, len(pdf.pages))

                            text_parts = []
                            for page_num in pages_to_extract:
                                page = pdf.pages[page_num]
                                text = page.extract_text()
                                if text:
                                    text_parts.append(text)

                            extracted_text = "\n\n".join(text_parts)
                    except ImportError:
                        raise ImportError("pdfplumber not installed. Install with: pip install pdfplumber")

            elif method == "pytesseract" or (ocr_enabled and file_extension == ".pdf"):
                try:
                    import pytesseract
                    from pdf2image import convert_from_path
                    from PIL import Image

                    if file_extension == ".pdf":
                        images = convert_from_path(file_path)
                        text_parts = []
                        for i, image in enumerate(images):
                            context.log.info(f"OCR processing page {i+1}")
                            text = pytesseract.image_to_string(image)
                            text_parts.append(text)
                        extracted_text = "\n\n".join(text_parts)
                    else:
                        # Image file
                        image = Image.open(file_path)
                        extracted_text = pytesseract.image_to_string(image)
                except ImportError:
                    raise ImportError("OCR libraries not installed. Install with: pip install pytesseract pdf2image pillow")

            elif method == "docx" and file_extension in [".docx", ".doc"]:
                try:
                    import docx
                    doc = docx.Document(file_path)

                    # Extract metadata
                    if extract_metadata:
                        core_props = doc.core_properties
                        metadata = {
                            "title": core_props.title or "",
                            "author": core_props.author or "",
                            "subject": core_props.subject or "",
                            "created": str(core_props.created) if core_props.created else "",
                            "modified": str(core_props.modified) if core_props.modified else "",
                        }

                    # Extract text
                    paragraphs = [para.text for para in doc.paragraphs]
                    if preserve_formatting:
                        extracted_text = "\n\n".join(paragraphs)
                    else:
                        extracted_text = " ".join(paragraphs)
                except ImportError:
                    raise ImportError("python-docx not installed. Install with: pip install python-docx")

            elif method == "html" and file_extension in [".html", ".htm"]:
                try:
                    from bs4 import BeautifulSoup
                    with open(file_path, 'r', encoding='utf-8') as f:
                        soup = BeautifulSoup(f.read(), 'html.parser')

                        # Extract metadata
                        if extract_metadata:
                            metadata = {
                                "title": soup.title.string if soup.title else "",
                            }

                        # Remove script and style elements
                        for script in soup(["script", "style"]):
                            script.decompose()

                        extracted_text = soup.get_text(separator="\n\n" if preserve_formatting else " ")
                except ImportError:
                    raise ImportError("beautifulsoup4 not installed. Install with: pip install beautifulsoup4")

            elif method in ["text", "markdown"]:
                with open(file_path, 'r', encoding='utf-8') as f:
                    extracted_text = f.read()

            else:
                raise ValueError(f"Unsupported extraction method '{method}' for file type '{file_extension}'")

            # Clean up text
            if not preserve_formatting:
                # Remove extra whitespace
                extracted_text = " ".join(extracted_text.split())

            context.log.info(f"Extracted {len(extracted_text)} characters")

            # Format output
            result = extracted_text
            if output_format == "json":
                result = {
                    "text": extracted_text,
                    "metadata": metadata,
                    "file_path": file_path,
                    "extraction_method": method,
                    "character_count": len(extracted_text),
                }
            elif output_format == "markdown":
                result = f"# Extracted from {Path(file_path).name}\n\n{extracted_text}"

            # Save to file if requested
            if save_to_file and output_path:
                context.log.info(f"Saving extracted text to {output_path}")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w', encoding='utf-8') as f:
                    if isinstance(result, dict):
                        import json
                        json.dump(result, f, indent=2)
                    else:
                        f.write(result)

            # Add metadata
            output_metadata = {
                "file_path": file_path,
                "extraction_method": method,
                "character_count": len(extracted_text),
                "word_count": len(extracted_text.split()),
            }
            if metadata:
                output_metadata["document_metadata"] = metadata

            context.add_output_metadata(output_metadata)

            return result

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[document_text_extractor_asset])


        return Definitions(assets=[document_text_extractor_asset], asset_checks=list(_schema_checks))

    def _parse_page_range(self, page_range_str: str, total_pages: int):
        """Parse page range string into list of page numbers."""
        pages = set()
        for part in page_range_str.split(','):
            if '-' in part:
                start, end = part.split('-')
                pages.update(range(int(start) - 1, int(end)))
            else:
                pages.add(int(part) - 1)
        return sorted([p for p in pages if 0 <= p < total_pages])
