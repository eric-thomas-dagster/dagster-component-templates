#!/bin/bash

# This script creates the remaining support files for agentic components

# Array of components that need files
components=(
  "vector_store_writer"
  "vector_store_query"
  "llm_chain_executor"
  "document_summarizer"
  "text_classifier"
  "conversation_memory"
  "rag_pipeline"
  "llm_output_parser"
)

for comp in "${components[@]}"; do
  echo "Creating files for $comp..."
  touch "/Users/ericthomas/dagster_components/dagster-component-templates/assets/$comp/schema.json"
  touch "/Users/ericthomas/dagster_components/dagster-component-templates/assets/$comp/requirements.txt"
  touch "/Users/ericthomas/dagster_components/dagster-component-templates/assets/$comp/example.yaml"
  touch "/Users/ericthomas/dagster_components/dagster-component-templates/assets/$comp/README.md"
done

echo "Done!"
