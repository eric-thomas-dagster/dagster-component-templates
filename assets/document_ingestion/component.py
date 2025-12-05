"""Document Ingestion Component.

Ingest documents for RAG/Q&A systems.
Supports various document sources including files, URLs, and directories.
"""

from typing import Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class DocumentIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting documents for RAG/Q&A systems.

    This component ingests documents from various sources and prepares them
    for embedding and vector storage for retrieval-augmented generation.

    Example:
        ```yaml
        type: dagster_component_templates.DocumentIngestionComponent
        attributes:
          asset_name: knowledge_base_docs
          source_path: "/path/to/documents"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_path: Optional[str] = Field(
        default=None,
        description="Path to documents directory or file"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_path = self.source_path
        description = self.description or "Documents for RAG/Q&A system"

        @asset(
            name=asset_name,
            description=description,
            group_name="knowledge_base",
        )
        def document_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests documents for RAG systems."""
            import os
            from pathlib import Path

            context.log.info("Ingesting documents for RAG/Q&A system")

            documents = []

            if source_path and os.path.exists(source_path):
                context.log.info(f"Reading documents from: {source_path}")

                path = Path(source_path)

                # Handle directory
                if path.is_dir():
                    # Look for common document types
                    extensions = ['.txt', '.md', '.pdf', '.doc', '.docx', '.html']
                    for ext in extensions:
                        for file_path in path.rglob(f'*{ext}'):
                            try:
                                # Read text files
                                if ext in ['.txt', '.md']:
                                    with open(file_path, 'r', encoding='utf-8') as f:
                                        content = f.read()
                                        documents.append({
                                            'doc_id': str(file_path),
                                            'filename': file_path.name,
                                            'file_type': ext,
                                            'content': content,
                                            'content_length': len(content),
                                            'source_path': str(file_path)
                                        })
                                        context.log.info(f"Ingested: {file_path.name}")
                                else:
                                    # For other types, add placeholder
                                    documents.append({
                                        'doc_id': str(file_path),
                                        'filename': file_path.name,
                                        'file_type': ext,
                                        'content': f'[Document: {file_path.name}]',
                                        'content_length': 0,
                                        'source_path': str(file_path),
                                        'needs_extraction': True
                                    })
                            except Exception as e:
                                context.log.warning(f"Could not read {file_path}: {e}")

                # Handle single file
                elif path.is_file():
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            documents.append({
                                'doc_id': str(path),
                                'filename': path.name,
                                'file_type': path.suffix,
                                'content': content,
                                'content_length': len(content),
                                'source_path': str(path)
                            })
                            context.log.info(f"Ingested single document: {path.name}")
                    except Exception as e:
                        context.log.warning(f"Could not read {path}: {e}")

            # If no documents found, create sample documents
            if not documents:
                context.log.info("No documents found at source path. Creating sample knowledge base.")
                documents = [
                    {
                        'doc_id': 'doc_1',
                        'filename': 'product_guide.md',
                        'file_type': '.md',
                        'content': 'Product Guide: Our product helps teams collaborate effectively. Features include real-time chat, file sharing, and video conferencing.',
                        'content_length': 150,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_2',
                        'filename': 'faq.md',
                        'file_type': '.md',
                        'content': 'FAQ: Q: How do I reset my password? A: Click on "Forgot Password" on the login page. Q: What payment methods do you accept? A: We accept credit cards and PayPal.',
                        'content_length': 180,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_3',
                        'filename': 'api_docs.md',
                        'file_type': '.md',
                        'content': 'API Documentation: Our REST API provides endpoints for user management, data access, and webhooks. Authentication uses API keys.',
                        'content_length': 140,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_4',
                        'filename': 'pricing.md',
                        'file_type': '.md',
                        'content': 'Pricing: Starter plan is $10/month for up to 5 users. Pro plan is $50/month for unlimited users with advanced features.',
                        'content_length': 130,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_5',
                        'filename': 'support.md',
                        'file_type': '.md',
                        'content': 'Support: Contact our support team via email at support@company.com or chat with us during business hours 9am-5pm EST.',
                        'content_length': 125,
                        'source_path': 'sample'
                    },
                ]

            df = pd.DataFrame(documents)

            # Add metadata
            df['ingested_at'] = pd.Timestamp.now()

            context.log.info(f"Successfully ingested {len(df)} documents")

            # Calculate statistics
            total_chars = df['content_length'].sum() if 'content_length' in df.columns else 0
            file_types = df['file_type'].value_counts().to_dict() if 'file_type' in df.columns else {}

            return Output(
                value=df,
                metadata={
                    "document_count": len(df),
                    "total_characters": int(total_chars),
                    "file_types": file_types,
                    "columns": list(df.columns),
                    "preview": MetadataValue.md(df.head(5).to_markdown())
                }
            )

        return Definitions(assets=[document_ingestion_asset])
