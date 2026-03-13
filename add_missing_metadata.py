#!/usr/bin/env python3
"""Add missing name, type, category, and description fields to component schemas."""

import json
from pathlib import Path

# Components needing updates with their friendly names and categories
COMPONENT_METADATA = {
    "conversation_memory": {
        "name": "Conversation Memory",
        "category": "ai",
        "description": "Store and retrieve conversation history for AI agents and chatbots",
        "icon": "MessageSquare"
    },
    "database_query": {
        "name": "Database Query",
        "category": "extraction",
        "description": "Execute SQL queries against databases and return results as DataFrames",
        "icon": "Database"
    },
    "dataframe_transformer": {
        "name": "DataFrame Transformer",
        "category": "transformation",
        "description": "Transform pandas DataFrames using Python code, filters, aggregations, and joins",
        "icon": "Table"
    },
    "document_summarizer": {
        "name": "Document Summarizer",
        "category": "ai",
        "description": "Summarize documents using LLMs with customizable prompts and chunking strategies",
        "icon": "FileText"
    },
    "document_text_extractor": {
        "name": "Document Text Extractor",
        "category": "transformation",
        "description": "Extract text content from PDF, Word, HTML, and other document formats",
        "icon": "FileSearch"
    },
    "duckdb_query_reader": {
        "name": "DuckDB Query",
        "category": "extraction",
        "description": "Query DuckDB databases and return results as DataFrames",
        "icon": "Database"
    },
    "duckdb_table_writer": {
        "name": "DuckDB Writer",
        "category": "loading",
        "description": "Write DataFrames to DuckDB tables with create, append, or replace modes",
        "icon": "Database"
    },
    "embedding_generator": {
        "name": "Embedding Generator",
        "category": "ai",
        "description": "Generate vector embeddings for text using OpenAI, Anthropic, or HuggingFace models",
        "icon": "Sparkles"
    },
    "entity_extractor": {
        "name": "Entity Extractor",
        "category": "ai",
        "description": "Extract named entities (people, organizations, locations) from text using NLP",
        "icon": "Tag"
    },
    "file_transformer": {
        "name": "File Transformer",
        "category": "transformation",
        "description": "Transform files using custom Python code or built-in operations",
        "icon": "FileCode"
    },
    "llm_chain_executor": {
        "name": "LLM Chain",
        "category": "ai",
        "description": "Execute multi-step LLM chains with memory and context passing",
        "icon": "Workflow"
    },
    "llm_output_parser": {
        "name": "LLM Output Parser",
        "category": "ai",
        "description": "Parse and structure LLM outputs into JSON, CSV, lists, or DataFrames",
        "icon": "Code"
    },
    "llm_prompt_executor": {
        "name": "LLM Prompt",
        "category": "ai",
        "description": "Execute LLM prompts using OpenAI, Anthropic, or other providers",
        "icon": "MessageCircle"
    },
    "priority_scorer": {
        "name": "Priority Scorer",
        "category": "ml",
        "description": "Score and prioritize items using custom rules or ML models",
        "icon": "Star"
    },
    "rag_pipeline": {
        "name": "RAG Pipeline",
        "category": "ai",
        "description": "Retrieval-augmented generation pipeline for question answering with vector stores",
        "icon": "BookOpen"
    },
    "rest_api_fetcher": {
        "name": "REST API Fetcher",
        "category": "extraction",
        "description": "Fetch data from REST APIs with pagination, authentication, and retry logic",
        "icon": "Cloud"
    },
    "synthetic_data_generator": {
        "name": "Synthetic Data Generator",
        "category": "transformation",
        "description": "Generate realistic synthetic data for testing and development",
        "icon": "Wand"
    },
    "text_chunker": {
        "name": "Text Chunker",
        "category": "ai",
        "description": "Split text into chunks using fixed size, sentence, paragraph, or semantic strategies",
        "icon": "Scissors"
    },
    "text_classifier": {
        "name": "Text Classifier",
        "category": "ai",
        "description": "Classify text into categories using LLMs or ML models",
        "icon": "Tag"
    },
    "text_moderator": {
        "name": "Text Moderator",
        "category": "ai",
        "description": "Moderate text content for inappropriate or harmful content",
        "icon": "Shield"
    },
    "ticket_classifier": {
        "name": "Ticket Classifier",
        "category": "ai",
        "description": "Classify support tickets by category, priority, and sentiment",
        "icon": "Ticket"
    },
    "time_series_generator": {
        "name": "Time Series Generator",
        "category": "transformation",
        "description": "Generate time series data with trends, seasonality, and noise",
        "icon": "TrendingUp"
    },
    "vector_store_query": {
        "name": "Vector Store Query",
        "category": "ai",
        "description": "Query vector stores for semantic search and similarity matching",
        "icon": "Search"
    },
    "vector_store_writer": {
        "name": "Vector Store Writer",
        "category": "ai",
        "description": "Write embeddings to vector stores (Pinecone, Weaviate, ChromaDB, Qdrant)",
        "icon": "Database"
    }
}

def get_component_class_name(component_id):
    """Get the component class name from component.py."""
    component_file = Path("assets") / component_id / "component.py"
    if not component_file.exists():
        return None

    # Read component.py and look for class definition
    with open(component_file) as f:
        for line in f:
            if line.strip().startswith("class ") and "Component" in line and "(" in line:
                # Extract class name
                class_name = line.strip().split("class ")[1].split("(")[0].strip()
                return f"dagster_component_templates.{class_name}"

    return None

def update_schema(component_id):
    """Update a schema file to add missing metadata."""
    schema_file = Path("assets") / component_id / "schema.json"

    if not schema_file.exists():
        print(f"Skipping {component_id}: no schema.json")
        return False

    # Read existing schema
    with open(schema_file) as f:
        schema = json.load(f)

    # Check if already has the fields
    if "name" in schema and "type" in schema and "dagster_component" in schema.get("type", ""):
        print(f"Skipping {component_id}: already has metadata")
        return False

    # Get metadata
    metadata = COMPONENT_METADATA.get(component_id)
    if not metadata:
        print(f"Skipping {component_id}: no metadata defined")
        return False

    # Get component type
    component_type = get_component_class_name(component_id)
    if not component_type:
        print(f"Warning: Could not determine component type for {component_id}")
        return False

    # Convert old format to new format
    new_schema = {
        "type": component_type,
        "name": metadata["name"],
        "category": metadata["category"],
        "description": metadata["description"]
    }

    # Add icon if present
    if "icon" in metadata:
        new_schema["icon"] = metadata["icon"]

    # Copy x-dagster-io section if present
    if "x-dagster-io" in schema:
        new_schema["x-dagster-io"] = schema["x-dagster-io"]

    # Convert properties to attributes
    if "properties" in schema:
        new_schema["attributes"] = {}
        for prop_name, prop_def in schema["properties"].items():
            # Convert property format
            attr_def = {}

            # Map type
            if "type" in prop_def:
                attr_def["type"] = prop_def["type"]

            # Map other fields
            if "title" in prop_def:
                attr_def["label"] = prop_def["title"]
            if "description" in prop_def:
                attr_def["description"] = prop_def["description"]
            if "default" in prop_def:
                attr_def["default"] = prop_def["default"]
            if "enum" in prop_def:
                attr_def["enum"] = prop_def["enum"]
            if "items" in prop_def:
                attr_def["items"] = prop_def["items"]
            if "minimum" in prop_def:
                attr_def["minimum"] = prop_def["minimum"]
            if "maximum" in prop_def:
                attr_def["maximum"] = prop_def["maximum"]

            # Determine if required
            attr_def["required"] = prop_name in schema.get("required", [])

            new_schema["attributes"][prop_name] = attr_def

    # Add dependencies if present
    if "dependencies" in schema:
        new_schema["dependencies"] = schema["dependencies"]

    # Add tags based on category
    tags = [metadata["category"]]
    if metadata["category"] == "ai":
        tags.extend(["llm", "nlp"])
    elif metadata["category"] == "transformation":
        tags.append("data-processing")
    elif metadata["category"] == "extraction":
        tags.extend(["ingestion", "data-source"])

    new_schema["tags"] = tags

    # Write updated schema
    with open(schema_file, "w") as f:
        json.dump(new_schema, f, indent=2)

    print(f"Updated {component_id}")
    return True

def main():
    print("Updating component schemas with missing metadata...")

    updated = 0
    for component_id in COMPONENT_METADATA.keys():
        if update_schema(component_id):
            updated += 1

    print(f"\n✓ Updated {updated} component schemas")

if __name__ == "__main__":
    main()
