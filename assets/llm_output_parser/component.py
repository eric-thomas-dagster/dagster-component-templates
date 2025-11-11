"""LLM Output Parser Asset Component.

Parse and validate LLM outputs into structured formats with schema validation.
"""

import json
from typing import Optional
import re

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field
import pandas as pd


class LLMOutputParserComponent(Component, Model, Resolvable):
    """Component for parsing LLM outputs.

    Example:
        ```yaml
        type: dagster_component_templates.LLMOutputParserComponent
        attributes:
          asset_name: parsed_output
          parser_type: json
          validation_schema: '{"type": "object", "properties": {"name": {"type": "string"}}}'
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    parser_type: str = Field(description="Parser: 'json', 'csv', 'list', 'key_value', 'markdown_table', 'custom'")
    validation_schema: Optional[str] = Field(default=None, description="JSON schema for validation")
    custom_regex: Optional[str] = Field(default=None, description="Custom regex pattern")
    extract_code_blocks: bool = Field(default=False, description="Extract code blocks from markdown")
    strip_markdown: bool = Field(default=True, description="Strip markdown formatting")
    output_format: str = Field(default="dict", description="Output: 'dict', 'list', 'dataframe'")
    strict_validation: bool = Field(default=False, description="Raise error on validation failure")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        parser_type = self.parser_type
        validation_schema_str = self.validation_schema
        custom_regex = self.custom_regex
        extract_code_blocks = self.extract_code_blocks
        strip_markdown = self.strip_markdown
        output_format = self.output_format
        strict_validation = self.strict_validation
        description = self.description or "Parse LLM output"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def llm_output_parser_asset(ctx: AssetExecutionContext, **kwargs):
            """Parse LLM output.

            Accepts LLM output from upstream assets via IO manager pattern.
            Upstream data is available in **kwargs.
            """

            # Get input from upstream
            if not kwargs:
                raise ValueError("No upstream data provided")

            raw_output = next(iter(kwargs.values()))
            if isinstance(raw_output, dict):
                raw_output = raw_output.get('text', str(raw_output))
            else:
                raw_output = str(raw_output)

            ctx.log.info(f"Parsing output with {parser_type} parser")

            # Extract code blocks if requested
            if extract_code_blocks:
                code_blocks = re.findall(r'```(?:\w+)?\n(.*?)\n```', raw_output, re.DOTALL)
                if code_blocks:
                    raw_output = code_blocks[0]  # Use first code block

            # Strip markdown
            if strip_markdown:
                raw_output = re.sub(r'[*_`#]', '', raw_output)

            # Parse based on type
            parsed_result = None

            if parser_type == "json":
                # Extract JSON from output
                json_match = re.search(r'(\{.*\}|\[.*\])', raw_output, re.DOTALL)
                if json_match:
                    try:
                        parsed_result = json.loads(json_match.group(1))
                    except json.JSONDecodeError as e:
                        if strict_validation:
                            raise
                        ctx.log.warning(f"JSON parse error: {e}")
                        parsed_result = {"raw": raw_output, "error": str(e)}

            elif parser_type == "csv":
                # Parse CSV
                lines = raw_output.strip().split('\n')
                if lines:
                    import csv
                    from io import StringIO
                    reader = csv.DictReader(StringIO('\n'.join(lines)))
                    parsed_result = list(reader)

            elif parser_type == "list":
                # Parse bullet points or numbered lists
                items = re.findall(r'(?:^|\n)(?:[-*•]|\d+\.)\s+(.+)', raw_output)
                parsed_result = items if items else raw_output.strip().split('\n')

            elif parser_type == "key_value":
                # Parse key: value pairs
                pairs = re.findall(r'(\w+):\s*(.+?)(?:\n|$)', raw_output)
                parsed_result = dict(pairs)

            elif parser_type == "markdown_table":
                # Parse markdown tables
                lines = [l.strip() for l in raw_output.split('\n') if '|' in l]
                if len(lines) >= 2:
                    headers = [h.strip() for h in lines[0].split('|')[1:-1]]
                    rows = []
                    for line in lines[2:]:  # Skip separator
                        values = [v.strip() for v in line.split('|')[1:-1]]
                        rows.append(dict(zip(headers, values)))
                    parsed_result = rows

            elif parser_type == "custom" and custom_regex:
                # Use custom regex
                matches = re.findall(custom_regex, raw_output, re.DOTALL)
                parsed_result = matches

            else:
                parsed_result = {"raw": raw_output}

            # Validate schema if provided
            if validation_schema_str and parser_type == "json":
                try:
                    import jsonschema
                    schema = json.loads(validation_schema_str)
                    jsonschema.validate(parsed_result, schema)
                    ctx.log.info("Schema validation passed")
                except ImportError:
                    ctx.log.warning("jsonschema not installed, skipping validation")
                except Exception as e:
                    if strict_validation:
                        raise
                    ctx.log.warning(f"Schema validation failed: {e}")

            # Format output
            result = parsed_result

            if output_format == "dataframe" and isinstance(parsed_result, list):
                result = pd.DataFrame(parsed_result)
            elif output_format == "list" and isinstance(parsed_result, dict):
                result = [parsed_result]

            ctx.log.info(f"Parsed output successfully")

            ctx.add_output_metadata({
                "parser_type": parser_type,
                "output_format": output_format
            })

            return result

        return Definitions(assets=[llm_output_parser_asset])
