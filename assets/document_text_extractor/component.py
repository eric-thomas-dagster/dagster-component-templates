"""Document Text Extractor Asset Component.

Extract text content from various document formats (PDF, DOCX, TXT, HTML, etc.).
Supports multiple extraction methods and formats for downstream processing.
"""

import os
from typing import Optional
from pathlib import Path

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetSpec,
    multi_asset,
    Resolvable,
    Model,
)
from pydantic import Field


class DocumentTextExtractorComponent(Component, Model, Resolvable):
    """Component for extracting text from documents.

    This asset extracts text from various document formats including PDF, DOCX,
    TXT, HTML, Markdown, and more. Useful as the first step in document processing pipelines.

    Example:
        ```yaml
        type: dagster_component_templates.DocumentTextExtractorComponent
        attributes:
          asset_name: extracted_text
          file_path: /path/to/document.pdf
          extraction_method: auto
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    file_path: str = Field(
        description="Path to the document file to extract text from"
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        file_path = self.file_path
        extraction_method = self.extraction_method
        ocr_enabled = self.ocr_enabled
        preserve_formatting = self.preserve_formatting
        extract_metadata = self.extract_metadata
        page_range = self.page_range
        output_format = self.output_format
        save_to_file = self.save_to_file
        output_path = self.output_path
        description = self.description or f"Extract text from {file_path}"
        group_name = self.group_name

        @multi_asset(
            name=f"{asset_name}_asset",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                )
            ],
        )
        def document_text_extractor_asset(context: AssetExecutionContext):
            """Asset that extracts text from documents."""

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

        return Definitions(assets=[document_text_extractor_asset])

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
